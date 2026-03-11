#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

TEST_CASE("RemoveRowGroupsForTable - basic operations", "[invalidation]") {
	ConditionCacheStore store;

	SECTION("empty store returns 0") {
		unordered_set<idx_t> rg_indices = {0, 1, 2};
		REQUIRE(store.RemoveRowGroupsForTable(1, rg_indices) == 0);
	}

	SECTION("removes specified row groups from matching table entries") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->bitvectors[0].SetVector(0);
		entry->bitvectors[1].SetVector(0);
		entry->bitvectors[2].SetVector(0);
		store.Upsert({1, "val > 5"}, entry);

		unordered_set<idx_t> rg_indices = {0, 2};
		auto removed = store.RemoveRowGroupsForTable(1, rg_indices);
		REQUIRE(removed == 2);

		// Entry still exists with RG 1
		auto found = store.Lookup({1, "val > 5"});
		REQUIRE(found != nullptr);
		REQUIRE(found->bitvectors.count(0) == 0);
		REQUIRE(found->bitvectors.count(1) == 1);
		REQUIRE(found->bitvectors.count(2) == 0);
	}

	SECTION("removes entire entry when all row groups are removed") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->bitvectors[0].SetVector(0);
		store.Upsert({1, "val > 5"}, entry);

		unordered_set<idx_t> rg_indices = {0};
		store.RemoveRowGroupsForTable(1, rg_indices);

		REQUIRE(store.Lookup({1, "val > 5"}) == nullptr);
	}

	SECTION("does not affect entries for other tables") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->bitvectors[0].SetVector(0);
		store.Upsert({1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->bitvectors[0].SetVector(0);
		store.Upsert({2, "val > 5"}, entry2);

		unordered_set<idx_t> rg_indices = {0};
		store.RemoveRowGroupsForTable(1, rg_indices);

		REQUIRE(store.Lookup({1, "val > 5"}) == nullptr);
		auto found2 = store.Lookup({2, "val > 5"});
		REQUIRE(found2 != nullptr);
		REQUIRE(found2->bitvectors.count(0) == 1);
	}

	SECTION("removes from multiple entries for the same table") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->bitvectors[0].SetVector(0);
		entry1->bitvectors[1].SetVector(0);
		store.Upsert({1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->bitvectors[0].SetVector(0);
		entry2->bitvectors[1].SetVector(0);
		store.Upsert({1, "val < 10"}, entry2);

		unordered_set<idx_t> rg_indices = {0};
		auto removed = store.RemoveRowGroupsForTable(1, rg_indices);
		REQUIRE(removed == 2); // one from each entry

		auto found1 = store.Lookup({1, "val > 5"});
		REQUIRE(found1 != nullptr);
		REQUIRE(found1->bitvectors.count(0) == 0);
		REQUIRE(found1->bitvectors.count(1) == 1);

		auto found2 = store.Lookup({1, "val < 10"});
		REQUIRE(found2 != nullptr);
		REQUIRE(found2->bitvectors.count(0) == 0);
		REQUIRE(found2->bitvectors.count(1) == 1);
	}
}

TEST_CASE("RemoveByTable - basic operations", "[invalidation]") {
	ConditionCacheStore store;

	SECTION("empty store returns 0") {
		REQUIRE(store.RemoveByTable(1) == 0);
	}

	SECTION("removes all entries for the specified table") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->bitvectors[0].SetVector(0);
		store.Upsert({1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->bitvectors[0].SetVector(0);
		store.Upsert({1, "val < 10"}, entry2);

		auto removed = store.RemoveByTable(1);
		REQUIRE(removed == 2);

		REQUIRE(store.Lookup({1, "val > 5"}) == nullptr);
		REQUIRE(store.Lookup({1, "val < 10"}) == nullptr);
	}

	SECTION("does not affect entries for other tables") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({2, "val > 5"}, entry2);

		store.RemoveByTable(1);

		REQUIRE(store.Lookup({1, "val > 5"}) == nullptr);
		REQUIRE(store.Lookup({2, "val > 5"}) != nullptr);
	}
}
} // namespace duckdb
