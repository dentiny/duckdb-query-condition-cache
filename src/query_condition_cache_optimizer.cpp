#include "query_condition_cache_optimizer.hpp"

#include "query_condition_cache_filter.hpp"
#include "predicate_key_utils.hpp"
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

// Pre-optimize runs BEFORE built-in passes (including FilterPushdown).
// The full WHERE clause is still in LogicalFilter.expressions, so we can
// compute a canonical key that covers the entire predicate.
void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}
	auto query_state =
	    input.context.registered_state->GetOrCreate<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	query_state->cache_apply_pending.clear();
	PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, *query_state);
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, CacheOptimizerQueryState &state) {
	// Skip cache building inside DML subplans
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

	// Only handle direct table scans with a filter (LogicalFilter -> LogicalGet).
	// Joins, subqueries, and other patterns are not supported currently.
	if (plan->type != LogicalOperatorType::LOGICAL_FILTER || plan->children.empty()) {
		return;
	}
	if (plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &filter = plan->Cast<LogicalFilter>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		return; // not a DuckDB table (e.g. system table, external)
	}
	if (filter.expressions.empty()) {
		return;
	}

	// TODO: Use std::optional when DuckDB upgrades to C++17 in v2.0
	auto key = ComputePredicateKey(context, table->oid, filter.expressions, get);
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);

	if (!entry) {
		// TODO: Consider building cache in the background and syncing later
		// to avoid blocking the first query.
		entry = BuildCacheForPredicate(context, filter.expressions, get);
		if (entry) {
			store->Upsert(context, key, entry);
		}
	}

	if (entry) {
		state.cache_apply_pending[get.table_index] = std::move(entry);
	}
}

// Reconstruct SQL from filter expressions, sort for alphabetical ordering.
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
	string predicate_sql = ReconstructPredicateSQL(expressions);
	string canonical = ComputeCanonicalPredicateKey(context, table_ptr->Cast<DuckTableEntry>(), predicate_sql);
	return CacheKey {table_oid, std::move(canonical)};
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();
	string predicate_sql = ReconstructPredicateSQL(expressions);
	auto bound_expr = BindPredicate(context, table_entry, predicate_sql);
	if (!bound_expr) {
		return nullptr;
	}

	// CheckBinder sets target_type = INTEGER, re-cast to BOOLEAN
	bound_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

	return BuildCacheEntry(context, table_entry, *bound_expr);
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                    CacheOptimizerQueryState &state) {
	for (auto &child : plan->children) {
		PostOptimizeWalk(context, child, state);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = plan->Cast<LogicalGet>();
	auto entry = state.cache_apply_pending.find(get.table_index);
	if (entry == state.cache_apply_pending.end()) {
		return;
	}

	InjectCacheFilter(context, get, entry->second);
	state.cache_apply_pending.erase(entry);
}

void QueryConditionCacheOptimizer::InjectCacheFilter(ClientContext &context, LogicalGet &get,
                                                     const shared_ptr<ConditionCacheEntry> &entry) {
	auto &column_ids = get.GetMutableColumnIds();
	bool has_row_id = false;
	for (const auto &column_id : column_ids) {
		if (column_id.IsRowIdColumn()) {
			has_row_id = true;
			break;
		}
	}
	if (!has_row_id) {
		if (get.projection_ids.empty() && !column_ids.empty()) {
			get.projection_ids.reserve(column_ids.size());
			for (idx_t i = 0; i < column_ids.size(); i++) {
				get.projection_ids.push_back(i);
			}
		}
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0U));

	auto filter_expr =
	    make_uniq<BoundFunctionExpression>(LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFunction(),
	                                       std::move(children), make_uniq<ConditionCacheFilterBindData>(entry));

	get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID),
	                             make_uniq<CacheExpressionFilter>(std::move(filter_expr), entry));
}

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}

	auto query_state = input.context.registered_state->Get<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	if (!query_state || query_state->cache_apply_pending.empty()) {
		return;
	}

	PostOptimizeWalk(input.context, plan, *query_state);
}

} // namespace duckdb
