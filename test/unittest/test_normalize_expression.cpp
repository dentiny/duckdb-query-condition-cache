#include "catch/catch.hpp"
#include "predicate_key_utils.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

TEST_CASE("NormalizeExpressionForCacheKey - comparison operand order", "[normalize]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t (id INTEGER, val INTEGER, name VARCHAR)");

	auto &context = *con.context;
	con.BeginTransaction();
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, "t");

	SECTION("equality: constant on left is swapped to right") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "42 = val");
		REQUIRE(key1 == key2);
	}

	SECTION("less-than / greater-than flip") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "id < 100");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "100 > id");
		REQUIRE(key1 == key2);
	}

	SECTION("less-than-or-equal / greater-than-or-equal flip") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val <= 50");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "50 >= val");
		REQUIRE(key1 == key2);
	}

	SECTION("greater-than / less-than flip") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val > 10");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "10 < val");
		REQUIRE(key1 == key2);
	}

	SECTION("greater-than-or-equal / less-than-or-equal flip") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val >= 10");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "10 <= val");
		REQUIRE(key1 == key2);
	}

	SECTION("not-equal is order-independent") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val != 0");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "0 != val");
		REQUIRE(key1 == key2);
	}

	SECTION("no swap when column is already on left") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42");
		REQUIRE(key1 == key2);
	}

	SECTION("both non-foldable: sorted by ToString") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "id = val");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "val = id");
		REQUIRE(key1 == key2);
	}

	SECTION("nested comparison in AND is normalized") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 AND id < 100");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "42 = val AND 100 > id");
		REQUIRE(key1 == key2);
	}

	SECTION("nested comparison in OR is normalized") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 OR id > 100");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "42 = val OR 100 < id");
		REQUIRE(key1 == key2);
	}

	SECTION("string constant is normalized") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "name = 'hello'");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "'hello' = name");
		REQUIRE(key1 == key2);
	}

	SECTION("OR children are sorted") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 OR val = 99");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "val = 99 OR val = 42");
		REQUIRE(key1 == key2);
	}

	SECTION("AND children are sorted") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 AND id < 100");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "id < 100 AND val = 42");
		REQUIRE(key1 == key2);
	}

	SECTION("nested AND inside OR is normalized") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "(val = 42 AND id < 100) OR val = 99");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "val = 99 OR (id < 100 AND val = 42)");
		REQUIRE(key1 == key2);
	}

	SECTION("reversed comparisons inside OR are normalized") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 OR val = 99");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "99 = val OR 42 = val");
		REQUIRE(key1 == key2);
	}

	SECTION("whitespace variations produce same key") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "val  =   42");
		REQUIRE(key1 == key2);
	}

	SECTION("three conjunction children are sorted") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "val = 42 AND id < 100 AND name = 'hello'");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "name = 'hello' AND val = 42 AND id < 100");
		REQUIRE(key1 == key2);
	}

	SECTION("both sides foldable: equality") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "1 = 2");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "2 = 1");
		REQUIRE(key1 == key2);
	}

	SECTION("both sides foldable: less-than") {
		auto key1 = ComputeCanonicalPredicateKey(context, table_entry, "1 < 2");
		auto key2 = ComputeCanonicalPredicateKey(context, table_entry, "2 > 1");
		REQUIRE(key1 == key2);
	}

	SECTION("empty predicate returns empty key") {
		REQUIRE(ComputeCanonicalPredicateKey(context, table_entry, "").empty());
	}

	SECTION("whitespace-only predicate returns empty key") {
		REQUIRE(ComputeCanonicalPredicateKey(context, table_entry, "   ").empty());
	}
}

} // namespace duckdb
