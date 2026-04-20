#include "query_condition_cache_optimizer.hpp"

#include "query_condition_cache_filter.hpp"
#include "predicate_key_utils.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
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
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, *query_state);
	} catch (...) {
		// Defense in depth: skip cache optimization rather than failing the query.
		query_state->cache_apply_pending.clear();
	}
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

	auto &duck_table = table->Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Skip caching on indexed tables: our extra ROW_ID filter would force
	// filter_set.filters.size() > 1, disabling the ART index scan path.
	if (storage.HasIndexes()) {
		return;
	}

	CacheKey key {table->oid, ComputeCanonicalPredicateKey(filter.expressions)};
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);
	store->RecordAccess(entry != nullptr);

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

namespace {

// Rewrite BoundColumnRefExpression -> BoundReferenceExpression(storage OID),
// the shape BuildCacheEntry expects. The source column_index is a position
// into LogicalGet::column_ids, which maps to the storage OID.
void ConvertColumnRefsToScanRefs(unique_ptr<Expression> &expr, const LogicalGet &get) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		auto &column_ids = get.GetColumnIds();
		auto col_idx = colref.binding.column_index;
		ALWAYS_ASSERT(col_idx < column_ids.size());
		auto storage_oid = column_ids[col_idx].GetPrimaryIndex();
		expr = make_uniq<BoundReferenceExpression>(colref.alias, colref.return_type, storage_oid);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ConvertColumnRefsToScanRefs(child, get); });
}

} // namespace

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Clone the plan's already-bound filter expressions and remap column refs
	// to storage OIDs.
	vector<unique_ptr<Expression>> cloned;
	cloned.reserve(expressions.size());
	for (const auto &expr : expressions) {
		auto copy = expr->Copy();
		ConvertColumnRefsToScanRefs(copy, get);
		cloned.push_back(std::move(copy));
	}

	auto predicate = CombineWithAnd(std::move(cloned));
	predicate = BoundCastExpression::AddCastToType(context, std::move(predicate), LogicalType {LogicalTypeId::BOOLEAN});

	return BuildCacheEntry(context, table_entry, *predicate);
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
