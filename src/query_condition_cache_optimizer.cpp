#include "query_condition_cache_optimizer.hpp"

#include "logical_cache_recorder.hpp"
#include "predicate_key_utils.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

bool ConvertColumnRefsToStorageRefs(unique_ptr<Expression> &expr, const LogicalGet &get) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		auto &column_ids = get.GetColumnIds();
		if (colref.binding.column_index >= column_ids.size()) {
			return false;
		}
		StorageIndex storage_index;
		if (!get.TryGetStorageIndex(column_ids[colref.binding.column_index], storage_index)) {
			return false;
		}
		expr = make_uniq<BoundReferenceExpression>(colref.alias, colref.return_type, storage_index.GetPrimaryIndex());
		return true;
	}

	bool success = true;
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { success = ConvertColumnRefsToStorageRefs(child, get) && success; });
	return success;
}

} // namespace

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
	query_state->cache_recorder_pending.clear();
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, /*inside_truncating=*/false, *query_state);
	} catch (...) {
		query_state->cache_apply_pending.clear();
		query_state->cache_recorder_pending.clear();
	}
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, bool inside_truncating,
                                                   CacheOptimizerQueryState &state) {
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool is_truncating = plan->type == LogicalOperatorType::LOGICAL_LIMIT ||
	                     plan->type == LogicalOperatorType::LOGICAL_TOP_N ||
	                     plan->type == LogicalOperatorType::LOGICAL_SAMPLE;
	bool child_inside_dml = inside_dml || is_dml;
	bool child_inside_truncating = inside_truncating || is_truncating;

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml, child_inside_truncating, state);
	}

	if (inside_dml || inside_truncating) {
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
	auto &storage = duck_table.GetStorage();
	if (storage.HasIndexes()) {
		return;
	}
	idx_t total_rows = storage.GetTotalRows();
	idx_t table_index = get.table_index;

	CacheKey key {table->oid, ComputeCanonicalPredicateKey(filter.expressions)};
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);
	store->RecordAccess(entry != nullptr);

	bool should_inject_recorder = false;
	if (!entry) {
		entry = make_shared_ptr<ConditionCacheEntry>();
		should_inject_recorder = true;
	} else if (entry->NeedsObservation(total_rows)) {
		should_inject_recorder = true;
	}

	if (should_inject_recorder) {
		vector<unique_ptr<Expression>> cloned;
		cloned.reserve(filter.expressions.size());
		vector<unique_ptr<Expression>> backfill_cloned;
		backfill_cloned.reserve(filter.expressions.size());
		bool can_backfill = true;
		for (const auto &expr : filter.expressions) {
			cloned.push_back(expr->Copy());
			auto backfill_copy = expr->Copy();
			can_backfill = ConvertColumnRefsToStorageRefs(backfill_copy, get) && can_backfill;
			backfill_cloned.push_back(std::move(backfill_copy));
		}
		auto predicate = CombineWithAnd(std::move(cloned));
		predicate =
		    BoundCastExpression::AddCastToType(context, std::move(predicate), LogicalType {LogicalTypeId::BOOLEAN});
		unique_ptr<Expression> backfill_predicate;
		if (can_backfill) {
			backfill_predicate = CombineWithAnd(std::move(backfill_cloned));
			backfill_predicate = BoundCastExpression::AddCastToType(context, std::move(backfill_predicate),
			                                                        LogicalType {LogicalTypeId::BOOLEAN});
		}
		state.cache_recorder_pending[table_index] = RecorderInjectionInfo {
		    table->oid,
		    table->ParentCatalog().GetName(),
		    table->ParentSchema().name,
		    table->name,
		    key.filter_key,
		    std::move(predicate),
		    std::move(backfill_predicate),
		    entry,
		    nullptr,
		};
	}

	state.cache_apply_pending[table_index] = entry;
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                    CacheOptimizerQueryState &state) {
	for (auto &child : plan->children) {
		PostOptimizeWalk(context, child, state);
	}

	if (plan->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan->Cast<LogicalGet>();
		auto entry = state.cache_apply_pending.find(get.table_index);
		if (entry == state.cache_apply_pending.end()) {
			return;
		}

		InjectCacheFilter(context, get, entry->second);
		state.cache_apply_pending.erase(entry);
		auto recorder_it = state.cache_recorder_pending.find(get.table_index);
		if (recorder_it == state.cache_recorder_pending.end()) {
			return;
		}

		auto info = std::move(recorder_it->second);
		state.cache_recorder_pending.erase(recorder_it);
		info.metadata_entry = GetPrunedRowGroupsFromTableFilters(context, get);
		InjectCacheRecorder(plan, info.table_oid, std::move(info.canonical_key), std::move(info.table_catalog),
		                    std::move(info.table_schema), std::move(info.table_name), std::move(info.predicate),
		                    std::move(info.backfill_predicate), info.entry, info.metadata_entry);
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

void QueryConditionCacheOptimizer::InjectCacheRecorder(unique_ptr<LogicalOperator> &plan, idx_t table_oid,
                                                       string canonical_key, string table_catalog, string table_schema,
                                                       string table_name, unique_ptr<Expression> predicate,
                                                       unique_ptr<Expression> backfill_predicate,
                                                       const shared_ptr<ConditionCacheEntry> &entry,
                                                       const shared_ptr<ConditionCacheEntry> &metadata_entry) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = plan->Cast<LogicalGet>();
	idx_t rowid_chunk_idx = EnsureRowIdChunkIndex(get);
	auto recorder = make_uniq<LogicalCacheRecorder>(
	    table_oid, std::move(canonical_key), std::move(predicate), rowid_chunk_idx, std::move(table_catalog),
	    std::move(table_schema), std::move(table_name), std::move(backfill_predicate), entry, metadata_entry);
	recorder->children.push_back(std::move(plan));
	plan = std::move(recorder);
}

shared_ptr<ConditionCacheEntry>
QueryConditionCacheOptimizer::GetPrunedRowGroupsFromTableFilters(ClientContext &context, const LogicalGet &get) {
	if (get.table_filters.filters.empty()) {
		return nullptr;
	}

	auto table = get.GetTable();
	if (!table) {
		return nullptr;
	}

	vector<pair<StorageIndex, const TableFilter *>> pushed_filters;
	pushed_filters.reserve(get.table_filters.filters.size());
	for (const auto &filter_entry : get.table_filters.filters) {
		StorageIndex storage_index;
		if (!get.TryGetStorageIndex(ColumnIndex(filter_entry.first), storage_index)) {
			continue;
		}
		pushed_filters.emplace_back(storage_index, filter_entry.second.get());
	}
	if (pushed_filters.empty()) {
		return nullptr;
	}

	auto &duck_table = table->Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	auto metadata_entry = make_shared_ptr<ConditionCacheEntry>();
	auto partition_stats = storage.GetPartitionStats(context);
	for (auto &partition : partition_stats) {
		if (!partition.row_start.IsValid() || !partition.partition_row_group) {
			continue;
		}
		auto row_start = partition.row_start.GetIndex();
		if (row_start >= NumericCast<idx_t>(MAX_ROW_ID)) {
			continue;
		}

		bool row_group_pruned = false;
		for (const auto &filter : pushed_filters) {
			auto column_stats = partition.partition_row_group->GetColumnStatistics(filter.first);
			if (!column_stats) {
				continue;
			}
			if (filter.second->CheckStatistics(*column_stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				row_group_pruned = true;
				break;
			}
		}
		if (row_group_pruned) {
			metadata_entry->MarkRowGroupFullyObserved(row_start / DEFAULT_ROW_GROUP_SIZE);
		}
	}

	if (metadata_entry->RowGroupCount() == 0) {
		return nullptr;
	}
	return metadata_entry;
}

idx_t QueryConditionCacheOptimizer::EnsureRowIdChunkIndex(LogicalGet &get) {
	auto &column_ids = get.GetMutableColumnIds();
	idx_t rowid_column_ids_pos = column_ids.size();
	for (idx_t i = 0; i < column_ids.size(); ++i) {
		if (column_ids[i].IsRowIdColumn()) {
			rowid_column_ids_pos = i;
			break;
		}
	}
	if (rowid_column_ids_pos == column_ids.size()) {
		if (get.projection_ids.empty() && !column_ids.empty()) {
			get.projection_ids.reserve(column_ids.size() + 1);
			for (idx_t i = 0; i < column_ids.size(); i++) {
				get.projection_ids.push_back(i);
			}
		}
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
		rowid_column_ids_pos = column_ids.size() - 1;
	}

	if (get.projection_ids.empty()) {
		get.projection_ids.reserve(column_ids.size());
		for (idx_t i = 0; i < column_ids.size(); ++i) {
			get.projection_ids.push_back(i);
		}
	}
	for (idx_t i = 0; i < get.projection_ids.size(); ++i) {
		if (get.projection_ids[i] == rowid_column_ids_pos) {
			return i;
		}
	}
	get.projection_ids.push_back(rowid_column_ids_pos);
	return get.projection_ids.size() - 1;
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
	query_state->cache_apply_pending.clear();
	query_state->cache_recorder_pending.clear();
}

} // namespace duckdb
