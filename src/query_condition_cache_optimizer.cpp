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

namespace {
// Thread-local: table_index -> cache entry to inject in the post-optimize pass.
// Populated by PreOptimizeFunction, consumed and cleared by OptimizeFunction.
thread_local unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> tl_cache_apply_pending;
} // namespace

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
	tl_cache_apply_pending.clear();
	PreOptimizeWalk(input.context, plan);
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child);
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

	auto key = ComputePredicateKey(table->oid, filter.expressions);
	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(key);

	if (!entry) {
		// Cache miss: build cache inline
		entry = BuildCacheForPredicate(context, filter.expressions, get);
		if (entry) {
			store->Upsert(key, entry);
		}
	}

	if (entry) {
		tl_cache_apply_pending[get.table_index] = std::move(entry);
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
	if (tl_cache_apply_pending.empty()) {
		return;
	}
	PostOptimizeWalk(plan);
	tl_cache_apply_pending.clear();
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		PostOptimizeWalk(child);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = plan->Cast<LogicalGet>();
	auto it = tl_cache_apply_pending.find(get.table_index);
	if (it == tl_cache_apply_pending.end()) {
		return;
	}

	InjectCacheFilter(plan, std::move(it->second));
	tl_cache_apply_pending.erase(it);
}

// ============================================================================
// InjectCacheFilter: add __condition_cache_filter on ROW_ID
// ============================================================================

void QueryConditionCacheOptimizer::InjectCacheFilter(unique_ptr<LogicalOperator> &get_plan,
                                                     shared_ptr<ConditionCacheEntry> entry) {
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

	// Ensure ROW_ID column is in the scan
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
