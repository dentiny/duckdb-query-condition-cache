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

void RecordSequence(ConditionCacheEntry &local_entry, unordered_map<idx_t, idx_t> &max_vec_per_rg,
                    optional_idx &current_rg, const vector<RecorderObservation> &observations) {
	for (const auto &obs : observations) {
		PhysicalCacheRecorder::RecordChunkObservation(local_entry, max_vec_per_rg, current_rg, obs.rg_idx, obs.vec_idx,
		                                              obs.has_qualifying);
	}
}

} // namespace

TEST_CASE("PhysicalCacheRecorder - watermark advances on within-rg vec transition", "[physical_recorder]") {
	ConditionCacheEntry local_entry;
	unordered_map<idx_t, idx_t> max_vec_per_rg;
	optional_idx current_rg;

	// Observe vecs 0, 1, 2 of rg 5 in order. After each transition, the previous vec is "confirmed".
	RecordSequence(local_entry, max_vec_per_rg, current_rg,
	               {{.rg_idx = 5, .vec_idx = 0, .has_qualifying = true},
	                {.rg_idx = 5, .vec_idx = 1, .has_qualifying = false},
	                {.rg_idx = 5, .vec_idx = 2, .has_qualifying = true}});

	// Watermark should be 2: vecs [0, 2) confirmed observed (vecs 0 and 1).
	// Vec 2 is the last seen and stays uncommitted (might be partial).
	REQUIRE(local_entry.GetObservedVectors(5) == 2);
	REQUIRE(local_entry.HasRowGroup(5));
	// Bits for vec 0 (qualifying), vec 1 (not), vec 2 (qualifying) should all be recorded.
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(5, 0));
	REQUIRE_FALSE(local_entry.RowGroupVectorHasQualifyingRows(5, 1));
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(5, 2));
}

TEST_CASE("PhysicalCacheRecorder - rg transition advances previous rg's watermark to max + 1", "[physical_recorder]") {
	ConditionCacheEntry local_entry;
	unordered_map<idx_t, idx_t> max_vec_per_rg;
	optional_idx current_rg;

	// Process rg 3 with vecs 0..4, then transition to rg 7 with vec 0.
	RecordSequence(local_entry, max_vec_per_rg, current_rg,
	               {{.rg_idx = 3, .vec_idx = 0, .has_qualifying = false},
	                {.rg_idx = 3, .vec_idx = 1, .has_qualifying = true},
	                {.rg_idx = 3, .vec_idx = 2, .has_qualifying = false},
	                {.rg_idx = 3, .vec_idx = 3, .has_qualifying = true},
	                {.rg_idx = 3, .vec_idx = 4, .has_qualifying = false},
	                {.rg_idx = 7, .vec_idx = 0, .has_qualifying = true}});

	// rg 3 fully observed in this task: watermark = max + 1 = 4 + 1 = 5.
	REQUIRE(local_entry.GetObservedVectors(3) == 5);
	// rg 7 is the new "last" rg: watermark stays at 0 (its vec 0 is the last seen, uncommitted).
	REQUIRE(local_entry.GetObservedVectors(7) == 0);
	REQUIRE(local_entry.HasRowGroup(7));
}

TEST_CASE("PhysicalCacheRecorder - single chunk leaves last vec uncommitted", "[physical_recorder]") {
	ConditionCacheEntry local_entry;
	unordered_map<idx_t, idx_t> max_vec_per_rg;
	optional_idx current_rg;

	RecordSequence(local_entry, max_vec_per_rg, current_rg, {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true}});

	// Only one chunk seen for rg 0: watermark stays at 0 (vec 0 uncommitted).
	// The qualifying bit is recorded so future merges retain the observation.
	REQUIRE(local_entry.GetObservedVectors(0) == 0);
	REQUIRE(local_entry.HasRowGroup(0));
	REQUIRE(local_entry.RowGroupVectorHasQualifyingRows(0, 0));
}

TEST_CASE("PhysicalCacheRecorder - empty rg keyed even with no qualifying rows", "[physical_recorder]") {
	ConditionCacheEntry local_entry;
	unordered_map<idx_t, idx_t> max_vec_per_rg;
	optional_idx current_rg;

	// rg 2 observed with no qualifying rows in any vec.
	RecordSequence(local_entry, max_vec_per_rg, current_rg,
	               {{.rg_idx = 2, .vec_idx = 0, .has_qualifying = false},
	                {.rg_idx = 2, .vec_idx = 1, .has_qualifying = false},
	                {.rg_idx = 2, .vec_idx = 2, .has_qualifying = false},
	                {.rg_idx = 9, .vec_idx = 0, .has_qualifying = false}});

	REQUIRE(local_entry.HasRowGroup(2));
	// rg 2 transitioned to rg 9, so rg 2's watermark = max + 1 = 3.
	REQUIRE(local_entry.GetObservedVectors(2) == 3);
	// Bitvector for rg 2 should be empty (no qualifying rows).
	REQUIRE(local_entry.RowGroupIsCompletelyEmpty(2));
}

TEST_CASE("PhysicalCacheRecorder - merging two task-local entries takes max watermark and OR of bits",
          "[physical_recorder]") {
	// Simulate two tasks each scanning a different subset of rgs, then merge into a destination.
	ConditionCacheEntry task_a;
	unordered_map<idx_t, idx_t> max_a;
	optional_idx cur_a;
	RecordSequence(task_a, max_a, cur_a,
	               {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                {.rg_idx = 0, .vec_idx = 1, .has_qualifying = false},
	                {.rg_idx = 0, .vec_idx = 2, .has_qualifying = true},
	                {.rg_idx = 1, .vec_idx = 0, .has_qualifying = false},
	                {.rg_idx = 1, .vec_idx = 1, .has_qualifying = true},
	                {.rg_idx = 1, .vec_idx = 2, .has_qualifying = false}});
	// task_a finishes on rg 1: rg 0 watermark = 3 (closed), rg 1 watermark = 2 (last vec uncommitted).

	ConditionCacheEntry task_b;
	unordered_map<idx_t, idx_t> max_b;
	optional_idx cur_b;
	RecordSequence(task_b, max_b, cur_b,
	               {{.rg_idx = 2, .vec_idx = 0, .has_qualifying = true},
	                {.rg_idx = 2, .vec_idx = 1, .has_qualifying = true},
	                {.rg_idx = 3, .vec_idx = 0, .has_qualifying = false}});
	// task_b finishes on rg 3: rg 2 watermark = 2 (closed), rg 3 watermark = 0.

	auto destination = make_shared_ptr<ConditionCacheEntry>();
	destination->MergeFrom(task_a);
	destination->MergeFrom(task_b);

	REQUIRE(destination->GetObservedVectors(0) == 3);
	REQUIRE(destination->GetObservedVectors(1) == 2);
	REQUIRE(destination->GetObservedVectors(2) == 2);
	REQUIRE(destination->GetObservedVectors(3) == 0);

	REQUIRE(destination->RowGroupVectorHasQualifyingRows(0, 0));
	REQUIRE_FALSE(destination->RowGroupVectorHasQualifyingRows(0, 1));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(0, 2));
	REQUIRE_FALSE(destination->RowGroupVectorHasQualifyingRows(1, 0));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(1, 1));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(2, 0));
	REQUIRE(destination->RowGroupVectorHasQualifyingRows(2, 1));
	REQUIRE(destination->RowGroupIsCompletelyEmpty(3));
}

TEST_CASE("PhysicalCacheRecorder - second pass over same rg advances watermark via max merge", "[physical_recorder]") {
	// Two passes (e.g. two queries) observing different prefixes of rg 0 should merge into the
	// higher watermark, demonstrating that incremental observations accumulate over time.
	ConditionCacheEntry pass1;
	unordered_map<idx_t, idx_t> max1;
	optional_idx cur1;
	RecordSequence(pass1, max1, cur1,
	               {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                {.rg_idx = 0, .vec_idx = 1, .has_qualifying = false},
	                {.rg_idx = 0, .vec_idx = 2, .has_qualifying = true}});
	// pass1 is the only rg this task; watermark stays at 2.
	REQUIRE(pass1.GetObservedVectors(0) == 2);

	ConditionCacheEntry pass2;
	unordered_map<idx_t, idx_t> max2;
	optional_idx cur2;
	RecordSequence(pass2, max2, cur2,
	               {{.rg_idx = 0, .vec_idx = 0, .has_qualifying = true},
	                {.rg_idx = 0, .vec_idx = 1, .has_qualifying = false},
	                {.rg_idx = 0, .vec_idx = 2, .has_qualifying = true},
	                {.rg_idx = 0, .vec_idx = 3, .has_qualifying = false},
	                {.rg_idx = 0, .vec_idx = 4, .has_qualifying = false}});
	REQUIRE(pass2.GetObservedVectors(0) == 4);

	auto store = make_shared_ptr<ConditionCacheEntry>();
	store->MergeFrom(pass1);
	REQUIRE(store->GetObservedVectors(0) == 2);
	store->MergeFrom(pass2);
	REQUIRE(store->GetObservedVectors(0) == 4);

	// Bit-wise OR: vecs 0 and 2 (qualifying in both passes), 4 (not qualifying), 1 and 3 not set.
	REQUIRE(store->RowGroupVectorHasQualifyingRows(0, 0));
	REQUIRE_FALSE(store->RowGroupVectorHasQualifyingRows(0, 1));
	REQUIRE(store->RowGroupVectorHasQualifyingRows(0, 2));
	REQUIRE_FALSE(store->RowGroupVectorHasQualifyingRows(0, 3));
	REQUIRE_FALSE(store->RowGroupVectorHasQualifyingRows(0, 4));
}

TEST_CASE("CacheRecorderOperatorExtension - GetName matches recorder's GetExtensionName", "[physical_recorder]") {
	// Ensures the deserialization dispatch picks up recorder plans correctly.
	CacheRecorderOperatorExtension ext;
	REQUIRE(ext.GetName() == "query_condition_cache_recorder");
}

} // namespace duckdb
