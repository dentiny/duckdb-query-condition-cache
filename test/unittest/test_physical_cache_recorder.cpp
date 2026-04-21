#include "catch/catch.hpp"

#include "logical_cache_recorder.hpp"
#include "physical_cache_recorder.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

namespace {

// Two idx_t fields — designated initializers prevent arg-swap mistakes.
struct RecorderObservation {
	idx_t rg_idx;
	idx_t vec_idx;
	bool has_qualifying;
};

void RecordSequence(ConditionCacheEntry &local_entry, const vector<RecorderObservation> &observations) {
	for (const auto &obs : observations) {
		PhysicalCacheRecorder::RecordChunkObservation(local_entry, obs.rg_idx, obs.vec_idx, obs.has_qualifying);
	}
}

} // namespace

TEST_CASE("PhysicalCacheRecorder - marks observed bit and qualifying bit per chunk", "[physical_recorder]") {
	ConditionCacheEntry local_entry;

	RecordSequence(local_entry, {{.rg_idx = 5, .vec_idx = 0, .has_qualifying = true},
	                             {.rg_idx = 5, .vec_idx = 1, .has_qualifying = false},
	                             {.rg_idx = 5, .vec_idx = 2, .has_qualifying = true}});

	REQUIRE(local_entry.HasRowGroup(5));
	REQUIRE(local_entry.GetObservedVectorCount(5) == 3);
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(5, 0));
	REQUIRE_FALSE(local_entry.RowGroupVectorHasQualifyingRows(5, 1));
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(5, 2));
}

TEST_CASE("PhysicalCacheRecorder - non-contiguous vecs set bits exactly where observed", "[physical_recorder]") {
	ConditionCacheEntry local_entry;

	// Simulating column-filter-induced gaps: scan emits chunks only for vecs 0, 3, 7.
	RecordSequence(local_entry, {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                             {.rg_idx = 0, .vec_idx = 3, .has_qualifying = false},
	                             {.rg_idx = 0, .vec_idx = 7, .has_qualifying = true}});

	REQUIRE(local_entry.GetObservedVectorCount(0) == 3);
	// Observed bits only where chunks arrived; intermediate vecs are NOT claimed observed.
	REQUIRE(local_entry.VectorPassesFilter(0, 0) == true);  // observed + qualifying -> pass
	REQUIRE(local_entry.VectorPassesFilter(0, 1) == true);  // unobserved -> pass-through
	REQUIRE(local_entry.VectorPassesFilter(0, 3) == false); // observed + non-qualifying -> prune
	REQUIRE(local_entry.VectorPassesFilter(0, 4) == true);  // unobserved
	REQUIRE(local_entry.VectorPassesFilter(0, 7) == true);  // observed + qualifying -> pass
}

TEST_CASE("PhysicalCacheRecorder - single chunk keys rg and sets one bit", "[physical_recorder]") {
	ConditionCacheEntry local_entry;
	RecordSequence(local_entry, {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true}});

	REQUIRE(local_entry.HasRowGroup(0));
	REQUIRE(local_entry.GetObservedVectorCount(0) == 1);
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(0, 0));
}

TEST_CASE("PhysicalCacheRecorder - rg with no qualifying rows is keyed with observed bits set", "[physical_recorder]") {
	ConditionCacheEntry local_entry;

	RecordSequence(local_entry, {{.rg_idx = 2, .vec_idx = 0, .has_qualifying = false},
	                             {.rg_idx = 2, .vec_idx = 1, .has_qualifying = false},
	                             {.rg_idx = 2, .vec_idx = 2, .has_qualifying = false}});

	REQUIRE(local_entry.HasRowGroup(2));
	REQUIRE(local_entry.GetObservedVectorCount(2) == 3);
	REQUIRE(local_entry.RowGroupIsCompletelyEmpty(2));
}

TEST_CASE("PhysicalCacheRecorder - merging two task-local entries ORs observed and qualifying bits",
          "[physical_recorder]") {
	ConditionCacheEntry task_a;
	RecordSequence(task_a, {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                        {.rg_idx = 0, .vec_idx = 2, .has_qualifying = true},
	                        {.rg_idx = 1, .vec_idx = 1, .has_qualifying = true}});

	ConditionCacheEntry task_b;
	RecordSequence(task_b, {{.rg_idx = 0, .vec_idx = 1, .has_qualifying = false},
	                        {.rg_idx = 2, .vec_idx = 0, .has_qualifying = true}});

	auto destination = make_shared_ptr<ConditionCacheEntry>();
	destination->MergeFrom(task_a);
	destination->MergeFrom(task_b);

	// observed bits: union of tasks' observations.
	REQUIRE(destination->GetObservedVectorCount(0) == 3); // vecs 0, 1, 2
	REQUIRE(destination->GetObservedVectorCount(1) == 1); // vec 1
	REQUIRE(destination->GetObservedVectorCount(2) == 1); // vec 0

	REQUIRE(destination->RowGroupVectorHasQualifyingRows(0, 0));
	REQUIRE_FALSE(destination->RowGroupVectorHasQualifyingRows(0, 1));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(0, 2));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(1, 1));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(2, 0));
}

TEST_CASE("PhysicalCacheRecorder - second pass accumulates observations across queries", "[physical_recorder]") {
	ConditionCacheEntry pass1;
	RecordSequence(pass1, {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                       {.rg_idx = 0, .vec_idx = 1, .has_qualifying = false}});

	ConditionCacheEntry pass2;
	RecordSequence(pass2, {{.rg_idx = 0, .vec_idx = 2, .has_qualifying = true},
	                       {.rg_idx = 0, .vec_idx = 3, .has_qualifying = false}});

	auto store = make_shared_ptr<ConditionCacheEntry>();
	store->MergeFrom(pass1);
	REQUIRE(store->GetObservedVectorCount(0) == 2);
	store->MergeFrom(pass2);
	REQUIRE(store->GetObservedVectorCount(0) == 4);

	REQUIRE(store->RowGroupVectorHasQualifyingRows(0, 0));
	REQUIRE_FALSE(store->RowGroupVectorHasQualifyingRows(0, 1));
	REQUIRE(store->RowGroupVectorHasQualifyingRows(0, 2));
	REQUIRE_FALSE(store->RowGroupVectorHasQualifyingRows(0, 3));
}

TEST_CASE("CacheRecorderOperatorExtension - GetName matches recorder's GetExtensionName", "[physical_recorder]") {
	CacheRecorderOperatorExtension ext;
	REQUIRE(ext.GetName() == "query_condition_cache_recorder");
}

} // namespace duckdb
