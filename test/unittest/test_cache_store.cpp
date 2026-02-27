#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

using namespace duckdb;

TEST_CASE("ConditionCacheStore - basic operations", "[cache_store]") {
	ConditionCacheStore store;

	SECTION("lookup empty store returns nullptr") {
		auto result = store.Lookup("nonexistent");
		REQUIRE(result == nullptr);
	}

	SECTION("insert and lookup by filter key") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->table_oid = 1;
		store.Insert("col:>5", entry);

		auto found = store.Lookup("col:>5");
		REQUIRE(found != nullptr);
		REQUIRE(found->table_oid == 1);

		auto not_found = store.Lookup("col:<10");
		REQUIRE(not_found == nullptr);
	}

	SECTION("insert and lookup by table OID") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->table_oid = 42;
		store.Insert("col:>5", entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->table_oid = 42;
		store.Insert("col:<10", entry2);

		auto entry3 = make_shared_ptr<ConditionCacheEntry>();
		entry3->table_oid = 99;
		store.Insert("val:=1", entry3);

		auto results = store.LookupByTable(42);
		REQUIRE(results.size() == 2);

		auto results_other = store.LookupByTable(99);
		REQUIRE(results_other.size() == 1);
		REQUIRE(results_other[0]->table_oid == 99);

		auto results_empty = store.LookupByTable(0);
		REQUIRE(results_empty.empty());
	}

	SECTION("insert duplicate filter key updates entry") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->table_oid = 1;
		store.Insert("col:>5", entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->table_oid = 2;
		store.Insert("col:>5", entry2);

		auto found = store.Lookup("col:>5");
		REQUIRE(found != nullptr);
		REQUIRE(found->table_oid == 2);
	}

	SECTION("clear removes all entries") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		entry->table_oid = 1;
		store.Insert("col:>5", entry);

		store.Clear();

		REQUIRE(store.Lookup("col:>5") == nullptr);
		REQUIRE(store.LookupByTable(1).empty());
		REQUIRE(store.GetAll().empty());
	}

	SECTION("get all returns all entries") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		entry1->table_oid = 1;
		store.Insert("col:>5", entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		entry2->table_oid = 2;
		store.Insert("col:<10", entry2);

		auto all = store.GetAll();
		REQUIRE(all.size() == 2);
	}
}
