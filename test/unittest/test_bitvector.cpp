#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

using namespace duckdb;

TEST_CASE("RowGroupBitvector - basic operations", "[bitvector]") {
	RowGroupBitvector bv;

	SECTION("initially empty") {
		REQUIRE(bv.IsEmpty());
		REQUIRE(bv.CountSetBits() == 0);
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			REQUIRE_FALSE(bv.VectorHasRows(i));
		}
	}

	SECTION("set and query single vector") {
		bv.SetVector(0);
		REQUIRE_FALSE(bv.IsEmpty());
		REQUIRE(bv.VectorHasRows(0));
		REQUIRE_FALSE(bv.VectorHasRows(1));
		REQUIRE(bv.CountSetBits() == 1);
	}

	SECTION("set multiple vectors") {
		bv.SetVector(0);
		bv.SetVector(10);
		bv.SetVector(59);
		REQUIRE(bv.CountSetBits() == 3);
		REQUIRE(bv.VectorHasRows(0));
		REQUIRE(bv.VectorHasRows(10));
		REQUIRE(bv.VectorHasRows(59));
		REQUIRE_FALSE(bv.VectorHasRows(1));
	}

	SECTION("set all vectors") {
		for (idx_t i = 0; i < VECTORS_PER_ROW_GROUP; ++i) {
			bv.SetVector(i);
		}
		REQUIRE(bv.CountSetBits() == VECTORS_PER_ROW_GROUP);
		REQUIRE_FALSE(bv.IsEmpty());
	}

	SECTION("SetVector is idempotent") {
		bv.SetVector(5);
		bv.SetVector(5);
		REQUIRE(bv.CountSetBits() == 1);
	}
}
