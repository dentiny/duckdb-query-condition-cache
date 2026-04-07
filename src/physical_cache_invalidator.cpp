#include "physical_cache_invalidator.hpp"

#include "concurrency/annotated_lock.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

PhysicalCacheInvalidator::PhysicalCacheInvalidator(PhysicalPlan &physical_plan, idx_t table_oid_p,
                                                   CacheInvalidatorMode mode_p, idx_t row_id_column_index_p,
                                                   idx_t pre_insert_row_count_p, vector<LogicalType> types,
                                                   idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_oid(table_oid_p), mode(mode_p), row_id_column_index(row_id_column_index_p),
      pre_insert_row_count(pre_insert_row_count_p) {
}

unique_ptr<GlobalOperatorState> PhysicalCacheInvalidator::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<CacheInvalidatorGlobalState>();
}

namespace {

// Collect row group indices from valid row IDs in the given vector.
// For rows with NULL row IDs (unmatched inserts in MERGE), increment inserted_row_count instead.
void CollectRowGroups(Vector &row_id_vec, idx_t count, CacheInvalidatorGlobalState &state, bool track_nulls) {
	UnifiedVectorFormat row_id_data;
	row_id_vec.ToUnifiedFormat(count, row_id_data);
	auto row_ids = UnifiedVectorFormat::GetData<row_t>(row_id_data);

	concurrency::lock_guard<concurrency::mutex> guard(state.lock);
	for (idx_t ii = 0; ii < count; ++ii) {
		auto sel_idx = row_id_data.sel->get_index(ii);
		if (!row_id_data.validity.RowIsValid(sel_idx)) {
			if (track_nulls) {
				++state.inserted_row_count;
			}
			continue;
		}
		auto row_id = NumericCast<idx_t>(row_ids[sel_idx]);
		state.affected_row_groups.insert(row_id / DEFAULT_ROW_GROUP_SIZE);
	}
}

} // namespace

// Passthrough operator: forwards input unchanged while collecting affected row groups.
// Called once per chunk by potentially multiple threads.
OperatorResultType PhysicalCacheInvalidator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state) const {
	chunk.Reference(input);
	auto &invalidator_state = gstate.Cast<CacheInvalidatorGlobalState>();

	switch (mode) {
	case CacheInvalidatorMode::DELETE:
	case CacheInvalidatorMode::UPDATE:
		CollectRowGroups(input.data[row_id_column_index], input.size(), invalidator_state, /*track_nulls=*/false);
		break;
	case CacheInvalidatorMode::INSERT: {
		concurrency::lock_guard<concurrency::mutex> guard(invalidator_state.lock);
		invalidator_state.inserted_row_count += input.size();
		break;
	}
	case CacheInvalidatorMode::MERGE:
		CollectRowGroups(input.data[row_id_column_index], input.size(), invalidator_state, /*track_nulls=*/true);
		break;
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

// Called once after all Execute() calls complete. Converts insert counts into
// row group indices, then removes affected row groups from the cache store.
OperatorFinalResultType PhysicalCacheInvalidator::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                   ClientContext &context,
                                                                   OperatorFinalizeInput &input) const {
	auto &invalidator_state = input.global_state.Cast<CacheInvalidatorGlobalState>();

	concurrency::lock_guard<concurrency::mutex> guard(invalidator_state.lock);
	// For INSERT/MERGE modes: compute affected row groups from the inserted row range.
	// New rows start at pre_insert_row_count and span inserted_row_count rows.
	if (invalidator_state.inserted_row_count > 0) {
		idx_t first_rg = pre_insert_row_count / DEFAULT_ROW_GROUP_SIZE;
		idx_t last_rg = (pre_insert_row_count + invalidator_state.inserted_row_count - 1) / DEFAULT_ROW_GROUP_SIZE;
		for (idx_t rg = first_rg; rg <= last_rg; ++rg) {
			invalidator_state.affected_row_groups.insert(rg);
		}
	}

	if (!invalidator_state.affected_row_groups.empty()) {
		auto store = ConditionCacheStore::GetOrCreate(context);
		store->RemoveRowGroupsForTable(context, table_oid, invalidator_state.affected_row_groups);
	}
	return OperatorFinalResultType::FINISHED;
}

bool PhysicalCacheInvalidator::RequiresOperatorFinalize() const {
	return true;
}

bool PhysicalCacheInvalidator::ParallelOperator() const {
	return true;
}

string PhysicalCacheInvalidator::GetName() const {
	return "CACHE_INVALIDATOR";
}

// Returns operator parameters for EXPLAIN output.
InsertionOrderPreservingMap<string> PhysicalCacheInvalidator::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table OID"] = to_string(table_oid);
	switch (mode) {
	case CacheInvalidatorMode::DELETE:
		result["Mode"] = "DELETE";
		result["Row ID Column"] = to_string(row_id_column_index);
		break;
	case CacheInvalidatorMode::UPDATE:
		result["Mode"] = "UPDATE";
		result["Row ID Column"] = to_string(row_id_column_index);
		break;
	case CacheInvalidatorMode::INSERT:
		result["Mode"] = "INSERT";
		result["Pre-insert Rows"] = to_string(pre_insert_row_count);
		break;
	case CacheInvalidatorMode::MERGE:
		result["Mode"] = "MERGE";
		result["Row ID Column"] = to_string(row_id_column_index);
		result["Pre-insert Rows"] = to_string(pre_insert_row_count);
		break;
	}
	return result;
}

} // namespace duckdb
