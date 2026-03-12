#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/exception.hpp"
#include "test_helpers.hpp"

namespace duckdb {

TEST_CASE("ConditionCacheStore - basic operations", "[cache_store]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	SECTION("lookup empty store returns nullptr") {
		auto result = store->Lookup(context, {1, "nonexistent"});
		REQUIRE(result == nullptr);
	}

	SECTION("upsert and lookup by cache key") {
		auto entry = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, {1, "col:>5"}, entry);

		auto found = store->Lookup(context, {1, "col:>5"});
		REQUIRE(found != nullptr);

		auto not_found = store->Lookup(context, {1, "col:<10"});
		REQUIRE(not_found == nullptr);

		auto wrong_oid = store->Lookup(context, {2, "col:>5"});
		REQUIRE(wrong_oid == nullptr);
	}

	SECTION("upsert duplicate key updates entry") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, {1, "col:>5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, {1, "col:>5"}, entry2);

		auto found = store->Lookup(context, {1, "col:>5"});
		REQUIRE(found != nullptr);
		REQUIRE(found == entry2);
	}

	SECTION("different oid same filter_key are separate entries") {
		auto entry1 = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, {1, "col:>5"}, entry1);

		auto entry2 = make_shared_ptr<ConditionCacheEntry>();
		store->Upsert(context, {2, "col:>5"}, entry2);

		REQUIRE(store->Lookup(context, {1, "col:>5"}) == entry1);
		REQUIRE(store->Lookup(context, {2, "col:>5"}) == entry2);
	}

	SECTION("upsert null entry throws") {
		REQUIRE_THROWS_AS(store->Upsert(context, {1, "col:>5"}, nullptr), InvalidInputException);
	}
}
} // namespace duckdb
