#include "query_condition_cache_optimizer.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
	optimize_function = OptimizeFunction;
}

// ============================================================================
// Setting check
// ============================================================================

bool QueryConditionCacheOptimizer::IsSettingEnabled(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("use_query_condition_cache", val);
	if (!result) {
		return false;
	}
	return val.GetValue<bool>();
}

// ============================================================================
// Pre-optimize: runs BEFORE built-in passes (including FilterPushdown).
// The full WHERE clause is still in LogicalFilter.expressions, so we can
// compute a canonical key that covers the entire predicate.
// On cache miss, builds cache inline so the first query benefits immediately.
// ============================================================================

void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}
	auto query_state =
	    input.context.registered_state->GetOrCreate<CacheOptimizerQueryState>("qcc_optimizer_state");
	query_state->cache_apply_pending.clear();
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, *query_state);
	} catch (...) {
		// If anything goes wrong during pre-optimization, clear pending entries
		// and let the query proceed without cache optimization
		query_state->cache_apply_pending.clear();
	}
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, CacheOptimizerQueryState &state) {
	// Check if this node is a DML operator — skip cache building in DML subplans
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml, state);
	}

	if (inside_dml) {
		return; // Don't build caches for predicates inside DML subplans
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
	if (!table) {
		return; // Not a DuckDB table (e.g. system table, external), skip gracefully
	}
	if (filter.expressions.empty()) {
		return;
	}

	auto &duck_table = table->Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Skip caching for tables with indexes — index scans are typically more efficient
	if (storage.HasIndexes()) {
		return;
	}

	auto key = ComputePredicateKey(table->oid, filter.expressions);
	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);

	if (!entry) {
		// Cache miss: build cache inline.
		entry = BuildCacheForPredicate(context, filter.expressions, get);
		if (entry) {
			store->Upsert(context, key, entry);
		}
	}

	if (entry) {
		state.cache_apply_pending[get.table_index] = std::move(entry);
	}
}

CacheKey QueryConditionCacheOptimizer::ComputePredicateKey(idx_t table_oid,
                                                           const vector<unique_ptr<Expression>> &expressions) {
	vector<string> parts;
	parts.reserve(expressions.size());
	for (auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	std::sort(parts.begin(), parts.end());
	return CacheKey {table_oid, StringUtil::Join(parts, ";")};
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}

	// We need a DuckTableEntry to use BuildCacheEntry
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Reconstruct predicate SQL from the bound expressions.
	// Join multiple filter expressions with AND.
	vector<string> parts;
	parts.reserve(expressions.size());
	for (auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	string predicate_sql = StringUtil::Join(parts, " AND ");

	// Parse, bind, and build cache entry (same flow as condition_cache_build table function)
	auto parsed_exprs = Parser::ParseExpressionList(predicate_sql);
	if (parsed_exprs.empty()) {
		return nullptr;
	}

	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, table_entry.name, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);

	// CheckBinder sets target_type = INTEGER, re-cast to BOOLEAN
	bound_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

	return BuildCacheEntry(context, table_entry, *bound_expr);
}

// ============================================================================
// Post-optimize: runs AFTER all built-in passes.
// FilterPushdown has already split predicates; we don't care about the split
// because the key was computed pre-pushdown. We just find the LogicalGet
// nodes whose table_index was matched and inject the cache filter.
// ============================================================================

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto query_state = input.context.registered_state->Get<CacheOptimizerQueryState>("qcc_optimizer_state");
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

// ============================================================================
// InjectCacheFilter: add __condition_cache_filter on ROW_ID
// ============================================================================

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

	// Ensure ROW_ID column is in the scan so the filter can access it
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
