#include "physical_cache_recorder.hpp"

#include "concurrency/annotated_lock.hpp"
#include "query_condition_cache_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

CacheRecorderLocalState::CacheRecorderLocalState(ClientContext &context, const Expression &bound_predicate)
    : expr_executor(context, bound_predicate) {
}

PhysicalCacheRecorder::PhysicalCacheRecorder(PhysicalPlan &physical_plan, idx_t table_oid_p, string canonical_key_p,
                                             unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p,
                                             string table_catalog_p, string table_schema_p, string table_name_p,
                                             unique_ptr<Expression> backfill_predicate_p,
                                             shared_ptr<ConditionCacheEntry> cache_entry_p,
                                             shared_ptr<ConditionCacheEntry> metadata_entry_p,
                                             vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_oid(table_oid_p), canonical_key(std::move(canonical_key_p)), table_catalog(std::move(table_catalog_p)),
      table_schema(std::move(table_schema_p)), table_name(std::move(table_name_p)),
      bound_predicate(std::move(bound_predicate_p)), backfill_predicate(std::move(backfill_predicate_p)),
      rowid_column_index(rowid_column_index_p), cache_entry(std::move(cache_entry_p)),
      metadata_entry(std::move(metadata_entry_p)) {
}

unique_ptr<GlobalOperatorState> PhysicalCacheRecorder::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<CacheRecorderGlobalState>();
}

unique_ptr<OperatorState> PhysicalCacheRecorder::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CacheRecorderLocalState>(context.client, *bound_predicate);
}

namespace {

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

bool TryEncodeVectorKey(row_t row_id, idx_t &key) {
	if (row_id < 0 || row_id >= MAX_ROW_ID) {
		return false;
	}
	auto unsigned_row_id = NumericCast<idx_t>(row_id);
	auto rg_idx = unsigned_row_id / DEFAULT_ROW_GROUP_SIZE;
	auto vec_idx = (unsigned_row_id % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
	key = rg_idx * VECTORS_PER_ROW_GROUP + vec_idx;
	return true;
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

	unordered_set<idx_t> observed_vectors;
	observed_vectors.reserve(input.size());
	for (idx_t input_idx = 0; input_idx < input.size(); ++input_idx) {
		auto rowid_idx = rowid_data.sel->get_index(input_idx);
		if (!rowid_data.validity.RowIsValid(rowid_idx)) {
			continue;
		}
		idx_t key;
		if (TryEncodeVectorKey(rowids[rowid_idx], key)) {
			observed_vectors.insert(key);
		}
	}

	if (observed_vectors.empty()) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	SelectionVector sel(input.size());
	idx_t match_count = local_state.expr_executor.SelectExpression(input, sel);
	unordered_set<idx_t> matching_vectors;
	matching_vectors.reserve(match_count);
	for (idx_t match_idx = 0; match_idx < match_count; ++match_idx) {
		auto input_idx = sel.get_index(match_idx);
		auto rowid_idx = rowid_data.sel->get_index(input_idx);
		if (!rowid_data.validity.RowIsValid(rowid_idx)) {
			continue;
		}
		idx_t key;
		if (TryEncodeVectorKey(rowids[rowid_idx], key)) {
			matching_vectors.insert(key);
		}
	}

	for (const auto &key : observed_vectors) {
		auto rg_idx = key / VECTORS_PER_ROW_GROUP;
		auto vec_idx = key % VECTORS_PER_ROW_GROUP;
		RecordChunkObservation(*local_state.local_entry, rg_idx, vec_idx, matching_vectors.count(key) > 0);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalCacheRecorder::RecordChunkObservation(ConditionCacheEntry &local_entry, idx_t rg_idx, idx_t vec_idx,
                                                   bool has_qualifying) {
	local_entry.EnsureRowGroup(rg_idx);
	local_entry.SetObservedVector(rg_idx, vec_idx);
	if (has_qualifying) {
		local_entry.SetQualifyingVector(rg_idx, vec_idx);
	}
}

namespace {

void BackfillMissingObservations(ClientContext &context, ConditionCacheEntry &destination, const string &table_catalog,
                                 const string &table_schema, const string &table_name, Expression &predicate) {
	if (table_catalog.empty() || table_schema.empty() || table_name.empty()) {
		return;
	}
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, table_catalog, table_schema, table_name);
	auto ranges = destination.GetUnobservedVectorRanges(table_entry.GetStorage().GetTotalRows());
	if (ranges.empty()) {
		return;
	}
	auto backfill_entry = BuildCacheEntryForRanges(context, table_entry, predicate, ranges);
	destination.MergeFrom(*backfill_entry);
}

} // namespace

OperatorFinalResultType PhysicalCacheRecorder::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                ClientContext &context,
                                                                OperatorFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<CacheRecorderGlobalState>();
	auto store = ConditionCacheStore::GetOrCreate(context);
	CacheKey key {table_oid, canonical_key};

	auto destination = cache_entry ? cache_entry : make_shared_ptr<ConditionCacheEntry>();
	if (metadata_entry) {
		destination->MergeFrom(*metadata_entry);
	}
	{
		concurrency::lock_guard<concurrency::mutex> guard(global_state.lock);
		for (const auto &task_entry : global_state.task_local_entries) {
			destination->MergeFrom(*task_entry);
		}
	}

	auto existing = store->Lookup(context, key);
	if (existing && existing.get() != destination.get()) {
		destination->MergeFrom(*existing);
	}

	if (backfill_predicate) {
		try {
			BackfillMissingObservations(context, *destination, table_catalog, table_schema, table_name,
			                            *backfill_predicate);
		} catch (...) {
			// Backfill is an optimization side effect. Preserve query correctness if it cannot run.
		}
	}

	store->Upsert(context, key, destination);
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
