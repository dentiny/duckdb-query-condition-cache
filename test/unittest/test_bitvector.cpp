#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

TEST_CASE("RowGroupFilter - basic operations", "[bitvector]") {
	SECTION("default constructor is empty") {
		RowGroupFilter bv;
		REQUIRE(bv.IsEmpty());
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			REQUIRE_FALSE(bv.VectorHasRows(i));
		}
	}

	SECTION("construct with single vector") {
		RowGroupFilter bv({0});
		REQUIRE_FALSE(bv.IsEmpty());
		REQUIRE(bv.VectorHasRows(0));
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
		a.MergeFrom(b);
		REQUIRE(a.VectorHasRows(1));
		REQUIRE(a.VectorHasRows(2));
		REQUIRE(a.VectorHasRows(3));
		REQUIRE_FALSE(a.VectorHasRows(0));
	}
}

TEST_CASE("RowGroupFilter - observed_vectors watermark", "[bitvector]") {
	SECTION("default constructor leaves observed_vectors at 0") {
		RowGroupFilter bv;
		REQUIRE(bv.observed_vectors == 0);
	}

	SECTION("vector-of-indices constructor leaves observed_vectors at 0") {
		RowGroupFilter bv({0, 5});
		REQUIRE(bv.observed_vectors == 0);
	}

	SECTION("MergeFrom takes max of watermarks") {
		RowGroupFilter a;
		a.observed_vectors = 3;
		RowGroupFilter b;
		b.observed_vectors = 7;
		a.MergeFrom(b);
		REQUIRE(a.observed_vectors == 7);
	}

	SECTION("MergeFrom keeps higher watermark when other is lower") {
		RowGroupFilter a;
		a.observed_vectors = 10;
		RowGroupFilter b;
		b.observed_vectors = 4;
		a.MergeFrom(b);
		REQUIRE(a.observed_vectors == 10);
	}

	SECTION("MergeFrom from a default-watermark filter preserves existing watermark") {
		RowGroupFilter a;
		a.observed_vectors = 5;
		RowGroupFilter empty;
		a.MergeFrom(empty);
		REQUIRE(a.observed_vectors == 5);
	}
}
} // namespace duckdb
