#include "physical_cache_building_filter.hpp"

#include "query_condition_cache_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

namespace {

unique_ptr<Expression> CombineFilterExpressions(vector<unique_ptr<Expression>> select_list) {
	D_ASSERT(!select_list.empty());
	if (select_list.size() == 1) {
		return std::move(select_list[0]);
	}

	auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	conjunction->children.reserve(select_list.size());
	for (auto &expr : select_list) {
		conjunction->children.push_back(std::move(expr));
	}
	return std::move(conjunction);
}

class CacheBuildingFilterState : public OperatorState {
public:
	CacheBuildingFilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

bool GetPersistentVectorKey(UnifiedVectorFormat &rowid_data, const row_t *rowids, idx_t row_idx, idx_t &key,
                            idx_t &row_id_idx) {
	auto rowid_idx = rowid_data.sel->get_index(row_idx);
	if (!rowid_data.validity.RowIsValid(rowid_idx)) {
		return false;
	}

	auto row_id = rowids[rowid_idx];
	if (row_id < 0 || row_id >= MAX_ROW_ID) {
		return false;
	}

	row_id_idx = NumericCast<idx_t>(row_id);
	idx_t row_group_idx = row_id_idx / DEFAULT_ROW_GROUP_SIZE;
	idx_t vector_idx = (row_id_idx % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
	key = row_group_idx * VECTORS_PER_ROW_GROUP + vector_idx;
	return true;
}

void RecordVectors(const shared_ptr<ConditionCacheEntry> &entry, Vector &rowid_vec, const SelectionVector &match_sel,
                   idx_t input_count, idx_t match_count) {
	if (!entry || input_count == 0) {
		return;
	}

	UnifiedVectorFormat rowid_data;
	rowid_vec.ToUnifiedFormat(input_count, rowid_data);
	auto rowids = UnifiedVectorFormat::GetData<row_t>(rowid_data);

	unordered_map<idx_t, idx_t> observed_max_row_ids;
	observed_max_row_ids.reserve(1);
	for (idx_t row_idx = 0; row_idx < input_count; ++row_idx) {
		idx_t key;
		idx_t row_id_idx;
		if (!GetPersistentVectorKey(rowid_data, rowids, row_idx, key, row_id_idx)) {
			continue;
		}
		auto it = observed_max_row_ids.find(key);
		if (it == observed_max_row_ids.end() || row_id_idx > it->second) {
			observed_max_row_ids[key] = row_id_idx;
		}
	}

	unordered_set<idx_t> matching_keys;
	matching_keys.reserve(1);
	for (idx_t match_idx = 0; match_idx < match_count; ++match_idx) {
		idx_t key;
		idx_t row_id_idx;
		if (GetPersistentVectorKey(rowid_data, rowids, match_sel.get_index(match_idx), key, row_id_idx)) {
			matching_keys.insert(key);
		}
	}

	for (const auto &[key, max_row_id] : observed_max_row_ids) {
		idx_t row_group_idx = key / VECTORS_PER_ROW_GROUP;
		idx_t vector_idx = key % VECTORS_PER_ROW_GROUP;
		entry->RecordVector(row_group_idx, vector_idx, matching_keys.count(key) > 0, max_row_id);
	}
}

} // namespace

PhysicalCacheBuildingFilter::PhysicalCacheBuildingFilter(PhysicalPlan &physical_plan, vector<LogicalType> visible_types,
                                                         vector<unique_ptr<Expression>> select_list,
                                                         idx_t row_id_column_index_p,
                                                         bool hide_row_id_column_p,
                                                         shared_ptr<ConditionCacheEntry> cache_entry_p,
                                                         string table_catalog_p, string table_schema_p,
                                                         string table_name_p,
                                                         unique_ptr<Expression> backfill_predicate_p,
                                                         bool allow_finalize_backfill_p,
                                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(visible_types), estimated_cardinality),
      row_id_column_index(row_id_column_index_p), hide_row_id_column(hide_row_id_column_p),
      cache_entry(std::move(cache_entry_p)), table_catalog(std::move(table_catalog_p)),
      table_schema(std::move(table_schema_p)), table_name(std::move(table_name_p)),
      backfill_predicate(std::move(backfill_predicate_p)), allow_finalize_backfill(allow_finalize_backfill_p) {
	expression_list.push_back(CombineFilterExpressions(std::move(select_list)));
}

unique_ptr<OperatorState> PhysicalCacheBuildingFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CacheBuildingFilterState>(context, *expression_list[0]);
}

OperatorResultType PhysicalCacheBuildingFilter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                        GlobalOperatorState &gstate, OperatorState &state_p) const {
	D_ASSERT(input.ColumnCount() > row_id_column_index);
	D_ASSERT(chunk.ColumnCount() == types.size());
	D_ASSERT(input.ColumnCount() >= chunk.ColumnCount());
	D_ASSERT(!hide_row_id_column || input.ColumnCount() == chunk.ColumnCount() + 1);

	auto &state = state_p.Cast<CacheBuildingFilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	RecordVectors(cache_entry, input.data[row_id_column_index], state.sel, input.size(), result_count);

	if (result_count == input.size()) {
		idx_t output_col_idx = 0;
		for (idx_t input_col_idx = 0; input_col_idx < input.ColumnCount(); ++input_col_idx) {
			if (hide_row_id_column && input_col_idx == row_id_column_index) {
				continue;
			}
			D_ASSERT(output_col_idx < chunk.ColumnCount());
			chunk.data[output_col_idx++].Reference(input.data[input_col_idx]);
		}
		D_ASSERT(output_col_idx == chunk.ColumnCount());
		chunk.SetCardinality(input.size());
		return OperatorResultType::NEED_MORE_INPUT;
	}

	idx_t output_col_idx = 0;
	for (idx_t input_col_idx = 0; input_col_idx < input.ColumnCount(); ++input_col_idx) {
		if (hide_row_id_column && input_col_idx == row_id_column_index) {
			continue;
		}
		D_ASSERT(output_col_idx < chunk.ColumnCount());
		chunk.data[output_col_idx++].Slice(input.data[input_col_idx], state.sel, result_count);
	}
	D_ASSERT(output_col_idx == chunk.ColumnCount());
	chunk.SetCardinality(result_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalResultType PhysicalCacheBuildingFilter::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                      ClientContext &context,
                                                                      OperatorFinalizeInput &input) const {
	if (!cache_entry || !backfill_predicate || table_catalog.empty() || table_schema.empty() || table_name.empty()) {
		return OperatorFinalResultType::FINISHED;
	}
	if (!allow_finalize_backfill) {
		return OperatorFinalResultType::FINISHED;
	}

	try {
		auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, table_catalog, table_schema, table_name);
		auto ranges = cache_entry->GetUnobservedVectorRanges(table_entry.GetStorage().GetTotalRows());
		if (!ranges.empty()) {
			auto backfill_entry = BuildCacheEntryForRanges(context, table_entry, *backfill_predicate, ranges);
			cache_entry->MergeFrom(*backfill_entry);
		}
	} catch (...) {
		// Backfill is only a cache side effect. The query result has already been produced by DuckDB's normal filter.
	}
	return OperatorFinalResultType::FINISHED;
}

bool PhysicalCacheBuildingFilter::RequiresOperatorFinalize() const {
	return allow_finalize_backfill && cache_entry != nullptr && backfill_predicate != nullptr;
}

string PhysicalCacheBuildingFilter::GetName() const {
	return "CACHE_BUILDING_FILTER";
}

InsertionOrderPreservingMap<string> PhysicalCacheBuildingFilter::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["__expression__"] = expression_list[0]->GetName();
	result["Row ID Column"] = to_string(row_id_column_index);
	result["Hidden Row ID"] = hide_row_id_column ? "true" : "false";
	result["Recording"] = cache_entry ? "true" : "false";
	result["Finalize Backfill"] = allow_finalize_backfill ? "true" : "false";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
