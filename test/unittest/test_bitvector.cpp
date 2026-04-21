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

TEST_CASE("RowGroupFilter - observed bitmask", "[bitvector]") {
	SECTION("default constructor leaves observed bits all zero") {
		RowGroupFilter bv;
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			REQUIRE_FALSE(bv.IsObserved(i));
		}
		REQUIRE_FALSE(bv.IsFullyObserved());
	}

	SECTION("vector-of-indices constructor leaves observed bits all zero") {
		RowGroupFilter bv({0, 5});
		REQUIRE_FALSE(bv.IsObserved(0));
		REQUIRE_FALSE(bv.IsObserved(5));
	}

	SECTION("SetObserved sets the bit") {
		RowGroupFilter bv;
		bv.SetObserved(3);
		bv.SetObserved(7);
		REQUIRE(bv.IsObserved(3));
		REQUIRE(bv.IsObserved(7));
		REQUIRE_FALSE(bv.IsObserved(4));
	}

	SECTION("IsFullyObserved true only when every vec bit set") {
		RowGroupFilter bv;
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			bv.SetObserved(i);
		}
		REQUIRE(bv.IsFullyObserved());
	}

	SECTION("MergeFrom ORs observed bits") {
		RowGroupFilter a;
		a.SetObserved(1);
		a.SetObserved(3);
		RowGroupFilter b;
		b.SetObserved(2);
		b.SetObserved(3);
		a.MergeFrom(b);
		REQUIRE(a.IsObserved(1));
		REQUIRE(a.IsObserved(2));
		REQUIRE(a.IsObserved(3));
		REQUIRE_FALSE(a.IsObserved(0));
	}

	SECTION("MergeFrom from an empty filter preserves existing observed bits") {
		RowGroupFilter a;
		a.SetObserved(5);
		RowGroupFilter empty;
		a.MergeFrom(empty);
		REQUIRE(a.IsObserved(5));
	}
}
} // namespace duckdb
