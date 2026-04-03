#include "query_condition_cache_optimizer.hpp"

#include "predicate_key_utils.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
	optimize_function = OptimizeFunction;
}

bool QueryConditionCacheOptimizer::IsSettingEnabled(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("use_query_condition_cache", val);
	if (!result) {
		return false;
	}
	return val.GetValue<bool>();
}

void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}

	auto query_state =
	    input.context.registered_state->GetOrCreate<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	query_state->cache_apply_pending.clear();

	try {
		PreOptimizeWalk(input.context, plan, false, *query_state);
	} catch (...) {
		// Let the query continue without cache optimization if planning fails here.
		query_state->cache_apply_pending.clear();
	}
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, CacheOptimizerQueryState &state) {
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml, state);
	}

	if (inside_dml) {
		return;
	}
	if (plan->type != LogicalOperatorType::LOGICAL_FILTER || plan->children.empty()) {
		return;
	}
	if (plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &filter = plan->Cast<LogicalFilter>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || filter.expressions.empty()) {
		return;
	}

	auto &duck_table = table->Cast<DuckTableEntry>();
	if (duck_table.GetStorage().HasIndexes()) {
		return;
	}

	auto key = ComputePredicateKey(context, table->oid, filter.expressions, get);
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);
	if (!entry) {
		entry = BuildCacheForPredicate(context, filter.expressions, get);
		if (entry) {
			store->Upsert(context, key, entry);
		}
	}

	if (entry) {
		state.cache_apply_pending[get.table_index] = std::move(entry);
	}
}

string QueryConditionCacheOptimizer::ReconstructPredicateSQL(const vector<unique_ptr<Expression>> &expressions) {
	vector<string> parts;
	parts.reserve(expressions.size());
	for (const auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	sort(parts.begin(), parts.end());
	return StringUtil::Join(parts, " AND ");
}

CacheKey QueryConditionCacheOptimizer::ComputePredicateKey(ClientContext &context, idx_t table_oid,
                                                           const vector<unique_ptr<Expression>> &expressions,
                                                           LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return CacheKey {table_oid, ""};
	}

	auto predicate_sql = ReconstructPredicateSQL(expressions);
	auto canonical = ComputeCanonicalPredicateKey(context, table_ptr->Cast<DuckTableEntry>(), predicate_sql);
	return CacheKey {table_oid, std::move(canonical)};
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}

	auto &table_entry = table_ptr->Cast<DuckTableEntry>();
	auto predicate_sql = ReconstructPredicateSQL(expressions);
	auto bound_expr = BindPredicate(context, table_entry, predicate_sql);
	if (!bound_expr) {
		return nullptr;
	}

	bound_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});
	return BuildCacheEntry(context, table_entry, *bound_expr);
}

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto query_state = input.context.registered_state->Get<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	if (!query_state || query_state->cache_apply_pending.empty()) {
		return;
	}

	PostOptimizeWalk(plan, *query_state);
	query_state->cache_apply_pending.clear();
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(unique_ptr<LogicalOperator> &plan,
                                                    CacheOptimizerQueryState &state) {
	for (auto &child : plan->children) {
		PostOptimizeWalk(child, state);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = plan->Cast<LogicalGet>();
	auto it = state.cache_apply_pending.find(get.table_index);
	if (it == state.cache_apply_pending.end()) {
		return;
	}

	InjectCacheFilter(plan, it->second);
	state.cache_apply_pending.erase(it);
}

void QueryConditionCacheOptimizer::InjectCacheFilter(unique_ptr<LogicalOperator> &get_plan,
                                                     const shared_ptr<ConditionCacheEntry> &entry) {
	auto &get = get_plan->Cast<LogicalGet>();

	ScalarFunction func("__condition_cache_filter", {LogicalType {LogicalTypeId::BIGINT}},
	                    LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFn, ConditionCacheFilterBind, nullptr,
	                    nullptr, ConditionCacheFilterInit);

	auto bind_data = make_uniq<ConditionCacheFilterBindData>(entry);

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0));

	auto func_expr = make_uniq<BoundFunctionExpression>(LogicalType {LogicalTypeId::BOOLEAN}, func, std::move(children),
	                                                    std::move(bind_data));

	get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID),
	                             make_uniq<CacheExpressionFilter>(std::move(func_expr), entry));

	bool has_rowid = false;
	for (auto &col : get.GetColumnIds()) {
		if (col.IsRowIdColumn()) {
			has_rowid = true;
			break;
		}
	}
	if (!has_rowid) {
		if (get.projection_ids.empty()) {
			for (idx_t i = 0; i < get.GetColumnIds().size(); ++i) {
				get.projection_ids.push_back(i);
			}
		}
		get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
	}
}

} // namespace duckdb
