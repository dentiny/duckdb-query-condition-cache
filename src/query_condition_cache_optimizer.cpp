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
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
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
	PreOptimizeWalk(input.context, plan, /*inside_dml=*/false);
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml) {
	// Check if this node is a DML operator — skip cache building in DML subplans
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml);
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
		InjectCacheExpression(filter, get, entry);
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
// InjectCacheExpression: add __condition_cache_filter on ROW_ID
// ============================================================================

void QueryConditionCacheOptimizer::InjectCacheExpression(LogicalFilter &filter, LogicalGet &get,
                                                         const shared_ptr<ConditionCacheEntry> &entry) {
	// Ensure ROW_ID column is in the scan so the filter can access it
	bool has_rowid = false;
	idx_t row_id_col_idx = 0;
	for (idx_t i = 0; i < get.GetColumnIds().size(); ++i) {
		if (get.GetColumnIds()[i].IsRowIdColumn()) {
			has_rowid = true;
			row_id_col_idx = i;
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
		row_id_col_idx = get.GetColumnIds().size() - 1;
	}

	ScalarFunction func("__condition_cache_filter", {LogicalType {LogicalTypeId::BIGINT}},
	                    LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFn, ConditionCacheFilterBind, nullptr,
	                    nullptr, ConditionCacheFilterInit);

	auto bind_data = make_uniq<ConditionCacheFilterBindData>(entry);

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundColumnRefExpression>("rowid", LogicalType::BIGINT,
	                                                       ColumnBinding(get.table_index, row_id_col_idx)));

	auto func_expr = make_uniq<BoundFunctionExpression>(LogicalType {LogicalTypeId::BOOLEAN}, func, std::move(children),
	                                                    std::move(bind_data));

	filter.expressions.push_back(std::move(func_expr));
}

} // namespace duckdb
