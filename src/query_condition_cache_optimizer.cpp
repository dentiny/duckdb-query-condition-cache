#include "query_condition_cache_optimizer.hpp"

#include "logical_cache_recorder.hpp"
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
	query_state->cache_recorder_pending.clear();
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, *query_state);
	} catch (...) {
		// Defense in depth: skip cache optimization rather than failing the query.
		query_state->cache_apply_pending.clear();
		query_state->cache_recorder_pending.clear();
	}
}

namespace {

// BoundColumnRef.column_index == chunk position at runtime, so we can rewrite directly
// to BoundReference(column_index).
void ConvertColumnRefsToChunkRefs(unique_ptr<Expression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		expr = make_uniq<BoundReferenceExpression>(colref.alias, colref.return_type, colref.binding.column_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { ConvertColumnRefsToChunkRefs(child); });
}

} // namespace

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

	if (!entry) {
		// Upsert the empty entry up front so the cache filter's bind_data and the recorder's
		// Finalize Lookup resolve to the same shared_ptr.
		entry = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, key, entry);

		// TODO: Also inject the recorder on a partial cache hit so the watermark can
		// advance on later queries and DML-dropped rgs get re-observed. Must skip under
		// LogicalLimit: a truncated scan cannot distinguish "unobserved" from "no match".
		vector<unique_ptr<Expression>> cloned;
		cloned.reserve(filter.expressions.size());
		for (const auto &expr : filter.expressions) {
			auto copy = expr->Copy();
			ConvertColumnRefsToChunkRefs(copy);
			cloned.push_back(std::move(copy));
		}
		auto predicate = CombineWithAnd(std::move(cloned));
		predicate =
		    BoundCastExpression::AddCastToType(context, std::move(predicate), LogicalType {LogicalTypeId::BOOLEAN});

		state.cache_recorder_pending[get.table_index] =
		    RecorderInjectionInfo {table->oid, key.filter_key, std::move(predicate)};
	}

	state.cache_apply_pending[get.table_index] = std::move(entry);
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

	auto recorder_it = state.cache_recorder_pending.find(get.table_index);
	if (recorder_it != state.cache_recorder_pending.end()) {
		auto info = std::move(recorder_it->second);
		state.cache_recorder_pending.erase(recorder_it);
		InjectCacheRecorder(context, plan, std::move(info));
	}
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

void QueryConditionCacheOptimizer::InjectCacheRecorder(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                       RecorderInjectionInfo &&info) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = plan->Cast<LogicalGet>();
	auto &column_ids = get.GetMutableColumnIds();

	// ROW_ID was added to column_ids by InjectCacheFilter; we also surface it through
	// projection_ids so it reaches the recorder's input chunk.
	idx_t rowid_column_ids_pos = column_ids.size();
	for (idx_t ii = 0; ii < column_ids.size(); ++ii) {
		if (column_ids[ii].IsRowIdColumn()) {
			rowid_column_ids_pos = ii;
			break;
		}
	}
	D_ASSERT(rowid_column_ids_pos < column_ids.size());

	// Chunk-level position = column_ids position if no projection, otherwise the matching
	// (or newly appended) projection_ids slot.
	idx_t rowid_chunk_idx;
	if (get.projection_ids.empty()) {
		rowid_chunk_idx = rowid_column_ids_pos;
	} else {
		idx_t found = get.projection_ids.size();
		for (idx_t ii = 0; ii < get.projection_ids.size(); ++ii) {
			if (get.projection_ids[ii] == rowid_column_ids_pos) {
				found = ii;
				break;
			}
		}
		if (found < get.projection_ids.size()) {
			rowid_chunk_idx = found;
		} else {
			get.projection_ids.push_back(rowid_column_ids_pos);
			rowid_chunk_idx = get.projection_ids.size() - 1;
		}
	}

	auto recorder = make_uniq<LogicalCacheRecorder>(info.table_oid, std::move(info.canonical_key),
	                                                std::move(info.predicate), rowid_chunk_idx);
	recorder->children.push_back(std::move(plan));
	plan = std::move(recorder);
}

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}

	auto query_state = input.context.registered_state->Get<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	if (!query_state || (query_state->cache_apply_pending.empty() && query_state->cache_recorder_pending.empty())) {
		return;
	}

	PostOptimizeWalk(input.context, plan, *query_state);
}

} // namespace duckdb
