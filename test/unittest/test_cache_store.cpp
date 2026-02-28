#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/exception.hpp"

using namespace duckdb;

TEST_CASE("ConditionCacheStore - basic operations", "[cache_store]") {
	ConditionCacheStore store;

	SECTION("lookup empty store returns nullptr") {
		auto result = store.Lookup({1, "nonexistent"});
		REQUIRE(result == nullptr);
	}

	SECTION("upsert and lookup by cache key") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry);

		auto found = store.Lookup({1, "col:>5"});
		REQUIRE(found != nullptr);

		auto not_found = store.Lookup({1, "col:<10"});
		REQUIRE(not_found == nullptr);

		auto wrong_oid = store.Lookup({2, "col:>5"});
		REQUIRE(wrong_oid == nullptr);
	}

	SECTION("upsert duplicate key updates entry") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry2);

		auto found = store.Lookup({1, "col:>5"});
		REQUIRE(found != nullptr);
		REQUIRE(found == entry2);
	}

	SECTION("different oid same filter_key are separate entries") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({2, "col:>5"}, entry2);

		REQUIRE(store.Lookup({1, "col:>5"}) == entry1);
		REQUIRE(store.Lookup({2, "col:>5"}) == entry2);
	}

	SECTION("clear removes all entries") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry);

		store.Clear();

		REQUIRE(store.Lookup({1, "col:>5"}) == nullptr);
		REQUIRE(store.GetAll().empty());
	}

	SECTION("get all returns all entries") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({1, "col:>5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store.Upsert({2, "col:<10"}, entry2);

		auto all = store.GetAll();
		REQUIRE(all.size() == 2);
	}

	SECTION("upsert null entry throws") {
		REQUIRE_THROWS_AS(store.Upsert({1, "col:>5"}, nullptr), InvalidInputException);
	}
}
