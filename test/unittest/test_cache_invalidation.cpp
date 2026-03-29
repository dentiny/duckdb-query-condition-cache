#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

TEST_CASE("RemoveRowGroupsForTable - basic operations", "[invalidation]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	SECTION("empty store returns 0") {
		unordered_set<idx_t> rg_indices = {0, 1, 2};
		REQUIRE(store->RemoveRowGroupsForTable(context, 1, rg_indices) == 0);
	}

	SECTION("removes specified row groups from matching table entries") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->bitvectors[0].SetVector(0);
		entry->bitvectors[1].SetVector(0);
		entry->bitvectors[2].SetVector(0);
		store->Upsert(context, {1, "val > 5"}, entry);

		unordered_set<idx_t> rg_indices = {0, 2};
		auto removed = store->RemoveRowGroupsForTable(context, 1, rg_indices);
		REQUIRE(removed == 2);

		auto found = store->Lookup(context, {1, "val > 5"});
		REQUIRE(found != nullptr);
		REQUIRE(found->bitvectors.count(0) == 0);
		REQUIRE(found->bitvectors.count(1) == 1);
		REQUIRE(found->bitvectors.count(2) == 0);
	}

	SECTION("removes entire entry when all row groups are removed") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->bitvectors[0].SetVector(0);
		store->Upsert(context, {1, "val > 5"}, entry);

		unordered_set<idx_t> rg_indices = {0};
		store->RemoveRowGroupsForTable(context, 1, rg_indices);

		REQUIRE(store->Lookup(context, {1, "val > 5"}) == nullptr);
	}

	SECTION("does not affect entries for other tables") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->bitvectors[0].SetVector(0);
		store->Upsert(context, {1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->bitvectors[0].SetVector(0);
		store->Upsert(context, {2, "val > 5"}, entry2);

		unordered_set<idx_t> rg_indices = {0};
		store->RemoveRowGroupsForTable(context, 1, rg_indices);

		REQUIRE(store->Lookup(context, {1, "val > 5"}) == nullptr);
		auto found2 = store->Lookup(context, {2, "val > 5"});
		REQUIRE(found2 != nullptr);
		REQUIRE(found2->bitvectors.count(0) == 1);
	}

	SECTION("removes from multiple entries for the same table") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->bitvectors[0].SetVector(0);
		entry1->bitvectors[1].SetVector(0);
		store->Upsert(context, {1, "val > 5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->bitvectors[0].SetVector(0);
		entry2->bitvectors[1].SetVector(0);
		store->Upsert(context, {1, "val < 10"}, entry2);

		unordered_set<idx_t> rg_indices = {0};
		auto removed = store->RemoveRowGroupsForTable(context, 1, rg_indices);
		REQUIRE(removed == 2); // one from each entry

		auto found1 = store->Lookup(context, {1, "val > 5"});
		REQUIRE(found1 != nullptr);
		REQUIRE(found1->bitvectors.count(0) == 0);
		REQUIRE(found1->bitvectors.count(1) == 1);

		auto found2 = store->Lookup(context, {1, "val < 10"});
		REQUIRE(found2 != nullptr);
		REQUIRE(found2->bitvectors.count(0) == 0);
		REQUIRE(found2->bitvectors.count(1) == 1);
	}
}
} // namespace duckdb
