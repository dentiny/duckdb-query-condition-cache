#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

TEST_CASE("RowGroupFilter - basic operations", "[bitvector]") {
	SECTION("default constructor is empty") {
		RowGroupFilter bv;
		REQUIRE(bv.IsEmpty());
		REQUIRE_FALSE(bv.HasObservedVectors());
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			REQUIRE_FALSE(bv.VectorIsObserved(i));
			REQUIRE_FALSE(bv.VectorHasRows(i));
		}
	}

	SECTION("construct with single vector") {
		RowGroupFilter bv({0});
		REQUIRE_FALSE(bv.IsEmpty());
		REQUIRE(bv.HasObservedVectors());
		REQUIRE(bv.VectorIsObserved(0));
		REQUIRE(bv.VectorHasRows(0));
		REQUIRE_FALSE(bv.VectorIsObserved(1));
		REQUIRE_FALSE(bv.VectorHasRows(1));
	}

	SECTION("construct with multiple vectors") {
		RowGroupFilter bv({0, 10, 59});
		REQUIRE(bv.VectorHasRows(0));
		REQUIRE(bv.VectorHasRows(10));
		REQUIRE(bv.VectorHasRows(59));
		REQUIRE_FALSE(bv.VectorHasRows(1));
	}

	SECTION("construct with all vectors") {
		vector<idx_t> all;
		all.reserve(VECTORS_PER_ROW_GROUP);
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			all.push_back(i);
		}
		RowGroupFilter bv(all);
		REQUIRE_FALSE(bv.IsEmpty());
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			REQUIRE(bv.VectorHasRows(i));
		}
	}

	SECTION("duplicate indices are handled correctly") {
		RowGroupFilter bv({5, 5});
		REQUIRE(bv.VectorHasRows(5));
		REQUIRE_FALSE(bv.VectorHasRows(6));
	}

	SECTION("MergeFrom combines two filters") {
		RowGroupFilter a({1, 3});
		RowGroupFilter b({2, 3});
		b.SetObserved(4);
		a.MergeFrom(b);
		REQUIRE(a.VectorHasRows(1));
		REQUIRE(a.VectorHasRows(2));
		REQUIRE(a.VectorHasRows(3));
		REQUIRE(a.VectorIsObserved(4));
		REQUIRE_FALSE(a.VectorHasRows(4));
		REQUIRE_FALSE(a.VectorHasRows(0));
	}

	SECTION("observed-empty vector is distinct from unobserved vector") {
		RowGroupFilter bv;
		bv.SetObserved(7);
		REQUIRE(bv.HasObservedVectors());
		REQUIRE(bv.VectorIsObserved(7));
		REQUIRE_FALSE(bv.VectorHasRows(7));
		REQUIRE_FALSE(bv.VectorIsObserved(8));
		REQUIRE_FALSE(bv.AllVectorsObserved(VECTORS_PER_ROW_GROUP));
	}
}

TEST_CASE("ConditionCacheEntry - partial entries pass unknown vectors", "[bitvector]") {
	ConditionCacheEntry entry;

	REQUIRE(entry.VectorPassesFilter(0, 0));

	entry.EnsureRowGroup(0);
	REQUIRE(entry.VectorPassesFilter(0, 0));

	entry.SetObservedVector(0, 0);
	REQUIRE_FALSE(entry.VectorPassesFilter(0, 0));
	REQUIRE(entry.VectorPassesFilter(0, 1));

	entry.SetQualifyingVector(0, 1);
	REQUIRE(entry.VectorPassesFilter(0, 1));
	REQUIRE(entry.RowGroupVectorIsObserved(0, 1));
	REQUIRE(entry.RowGroupVectorHasQualifyingRows(0, 1));
}

TEST_CASE("ConditionCacheEntry - row id filter respects persistent high watermark", "[bitvector]") {
	ConditionCacheEntry entry;

	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/0, /*has_match=*/false, /*max_row_id=*/STANDARD_VECTOR_SIZE - 1);

	REQUIRE_FALSE(entry.RowIdPassesFilter(0));
	REQUIRE_FALSE(entry.RowIdPassesFilter(STANDARD_VECTOR_SIZE - 1));
	REQUIRE(entry.RowIdPassesFilter(STANDARD_VECTOR_SIZE));
	REQUIRE(entry.RowIdPassesFilter(MAX_ROW_ID));
	REQUIRE(entry.RowIdPassesFilter(-1));
}

TEST_CASE("ConditionCacheEntry - unobserved vector ranges coalesce by persistent row range", "[bitvector]") {
	ConditionCacheEntry entry;
	idx_t total_rows = STANDARD_VECTOR_SIZE * 4 + 17;

	REQUIRE(entry.NeedsObservation(total_rows));
	auto ranges = entry.GetUnobservedVectorRanges(total_rows);
	REQUIRE(ranges.size() == 1);
	REQUIRE(ranges[0].start_row == 0);
	REQUIRE(ranges[0].count == total_rows);

	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/0, /*has_match=*/false, /*max_row_id=*/STANDARD_VECTOR_SIZE - 1);
	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/2, /*has_match=*/true, /*max_row_id=*/STANDARD_VECTOR_SIZE * 3 - 1);

	ranges = entry.GetUnobservedVectorRanges(total_rows);
	REQUIRE(ranges.size() == 2);
	REQUIRE(ranges[0].start_row == STANDARD_VECTOR_SIZE);
	REQUIRE(ranges[0].count == STANDARD_VECTOR_SIZE);
	REQUIRE(ranges[1].start_row == STANDARD_VECTOR_SIZE * 3);
	REQUIRE(ranges[1].count == STANDARD_VECTOR_SIZE + 17);

	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/1, /*has_match=*/false, /*max_row_id=*/STANDARD_VECTOR_SIZE * 2 - 1);
	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/3, /*has_match=*/false, /*max_row_id=*/STANDARD_VECTOR_SIZE * 4 - 1);
	entry.RecordVector(/*rg_idx=*/0, /*vec_idx=*/4, /*has_match=*/false, /*max_row_id=*/total_rows - 1);

	REQUIRE_FALSE(entry.NeedsObservation(total_rows));
	REQUIRE(entry.GetUnobservedVectorRanges(total_rows).empty());
}

} // namespace duckdb
