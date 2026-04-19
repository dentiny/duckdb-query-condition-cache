#include "physical_cache_recorder.hpp"

#include "concurrency/annotated_lock.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CacheRecorderLocalState::CacheRecorderLocalState(ClientContext &context, const Expression &bound_predicate)
    : expr_executor(context, bound_predicate) {
}

PhysicalCacheRecorder::PhysicalCacheRecorder(PhysicalPlan &physical_plan, idx_t table_oid_p, string canonical_key_p,
                                             unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p,
                                             vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_oid(table_oid_p), canonical_key(std::move(canonical_key_p)), bound_predicate(std::move(bound_predicate_p)),
      rowid_column_index(rowid_column_index_p) {
}

unique_ptr<GlobalOperatorState> PhysicalCacheRecorder::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<CacheRecorderGlobalState>();
}

unique_ptr<OperatorState> PhysicalCacheRecorder::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CacheRecorderLocalState>(context.client, *bound_predicate);
}

namespace {

// Register so the entry outlives this OperatorState (destroyed before OperatorFinalize).
void RegisterLocalIfNeeded(CacheRecorderLocalState &local_state, CacheRecorderGlobalState &global_state) {
	if (local_state.local_entry) {
		return;
	}
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	{
		concurrency::lock_guard<concurrency::mutex> guard(global_state.lock);
		global_state.task_local_entries.push_back(entry);
	}
	local_state.local_entry = std::move(entry);
}

} // namespace

OperatorResultType PhysicalCacheRecorder::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state) const {
	D_ASSERT(rowid_column_index < input.ColumnCount());

	chunk.Reference(input);

	auto &local_state = state.Cast<CacheRecorderLocalState>();
	auto &global_state = gstate.Cast<CacheRecorderGlobalState>();
	RegisterLocalIfNeeded(local_state, global_state);

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	auto &rowid_vec = input.data[rowid_column_index];
	UnifiedVectorFormat rowid_data;
	rowid_vec.ToUnifiedFormat(input.size(), rowid_data);
	auto rowids = UnifiedVectorFormat::GetData<row_t>(rowid_data);

	auto first_idx = rowid_data.sel->get_index(0);
	if (!rowid_data.validity.RowIsValid(first_idx)) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	auto first_row_id = NumericCast<idx_t>(rowids[first_idx]);
	if (first_row_id >= NumericCast<idx_t>(MAX_ROW_ID)) {
		// Transaction-local storage rows have no stable cache identity.
		return OperatorResultType::NEED_MORE_INPUT;
	}

	idx_t rg_idx = first_row_id / DEFAULT_ROW_GROUP_SIZE;
	idx_t vec_idx = (first_row_id % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;

	SelectionVector sel(input.size());
	idx_t match_count = local_state.expr_executor.SelectExpression(input, sel);

	RecordChunkObservation(*local_state.local_entry, local_state.max_vec_per_rg, local_state.current_rg, rg_idx,
	                       vec_idx, /*has_qualifying=*/match_count > 0);

	return OperatorResultType::NEED_MORE_INPUT;
}

// TODO: advance the watermark aggressively on vec_idx jumps (intermediate vecs filtered
// out by column filters) and on rg transitions (close rg to its true vec count from
// storage) instead of the current +1-per-transition policy.
void PhysicalCacheRecorder::RecordChunkObservation(ConditionCacheEntry &local_entry,
                                                   unordered_map<idx_t, idx_t> &max_vec_per_rg,
                                                   optional_idx &current_rg, idx_t rg_idx, idx_t vec_idx,
                                                   bool has_qualifying) {
	if (current_rg.IsValid() && current_rg.GetIndex() != rg_idx) {
		idx_t prev_rg = current_rg.GetIndex();
		local_entry.SetRowGroupWatermark(prev_rg, max_vec_per_rg[prev_rg] + 1);
	}

	auto it = max_vec_per_rg.find(rg_idx);
	bool first_chunk_for_rg = it == max_vec_per_rg.end();
	if (first_chunk_for_rg) {
		local_entry.EnsureRowGroup(rg_idx);
	} else if (vec_idx > it->second) {
		local_entry.SetRowGroupWatermark(rg_idx, it->second + 1);
	}
	max_vec_per_rg[rg_idx] = vec_idx;
	current_rg = optional_idx(rg_idx);

	if (has_qualifying) {
		local_entry.SetQualifyingVector(rg_idx, vec_idx);
	}
}

OperatorFinalResultType PhysicalCacheRecorder::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                ClientContext &context,
                                                                OperatorFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<CacheRecorderGlobalState>();

	// TODO: backfill rgs scan fully skipped (zone-map or column-filter prune) by consulting
	// storage for the rg count and keying missing entries with bit=0 + watermark=FULL. Only
	// safe when scan ran to completion; pair with LogicalLimit detection in the optimizer.
	auto store = ConditionCacheStore::GetOrCreate(context);
	CacheKey key {table_oid, canonical_key};
	auto destination = store->Lookup(context, key);
	if (!destination) {
		destination = make_shared_ptr<ConditionCacheEntry>();
	}

	{
		concurrency::lock_guard<concurrency::mutex> guard(global_state.lock);
		for (const auto &task_entry : global_state.task_local_entries) {
			destination->MergeFrom(*task_entry);
		}
	}

	store->Upsert(context, key, std::move(destination));
	return OperatorFinalResultType::FINISHED;
}

bool PhysicalCacheRecorder::RequiresOperatorFinalize() const {
	return true;
}

bool PhysicalCacheRecorder::ParallelOperator() const {
	return true;
}

string PhysicalCacheRecorder::GetName() const {
	return "CACHE_RECORDER";
}

InsertionOrderPreservingMap<string> PhysicalCacheRecorder::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table OID"] = to_string(table_oid);
	result["Filter Key"] = canonical_key;
	result["Row ID Column"] = to_string(rowid_column_index);
	return result;
}

} // namespace duckdb
