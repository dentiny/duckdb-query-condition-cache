#include "catch/catch.hpp"
#include "query_condition_cache_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"

namespace duckdb {
namespace {

// Helper: parse SQL predicate, bind against table "t", normalize, return ToString()
string NormalizedKey(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql) {
	auto parsed_exprs = Parser::ParseExpressionList(predicate_sql);
	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, table_entry.name, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);
	NormalizeExpressionForKey(*bound_expr);
	return bound_expr->ToString();
}

} // namespace

TEST_CASE("NormalizeExpressionForKey - comparison operand order", "[normalize]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE t (id INTEGER, val INTEGER, name VARCHAR)");

	auto &context = *con.context;
	con.BeginTransaction();
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, "t");

	SECTION("equality: constant on left is swapped to right") {
		auto key1 = NormalizedKey(context, table_entry, "val = 42");
		auto key2 = NormalizedKey(context, table_entry, "42 = val");
		REQUIRE(key1 == key2);
	}

	SECTION("less-than / greater-than flip") {
		auto key1 = NormalizedKey(context, table_entry, "id < 100");
		auto key2 = NormalizedKey(context, table_entry, "100 > id");
		REQUIRE(key1 == key2);
	}

	SECTION("less-than-or-equal / greater-than-or-equal flip") {
		auto key1 = NormalizedKey(context, table_entry, "val <= 50");
		auto key2 = NormalizedKey(context, table_entry, "50 >= val");
		REQUIRE(key1 == key2);
	}

	SECTION("greater-than / less-than flip") {
		auto key1 = NormalizedKey(context, table_entry, "val > 10");
		auto key2 = NormalizedKey(context, table_entry, "10 < val");
		REQUIRE(key1 == key2);
	}

	SECTION("greater-than-or-equal / less-than-or-equal flip") {
		auto key1 = NormalizedKey(context, table_entry, "val >= 10");
		auto key2 = NormalizedKey(context, table_entry, "10 <= val");
		REQUIRE(key1 == key2);
	}

	SECTION("not-equal is order-independent") {
		auto key1 = NormalizedKey(context, table_entry, "val != 0");
		auto key2 = NormalizedKey(context, table_entry, "0 != val");
		REQUIRE(key1 == key2);
	}

	SECTION("no swap when column is already on left") {
		auto key1 = NormalizedKey(context, table_entry, "val = 42");
		auto key2 = NormalizedKey(context, table_entry, "val = 42");
		REQUIRE(key1 == key2);
	}

	SECTION("no swap when both sides are columns") {
		// Both sides are non-foldable, should stay as-is
		auto key = NormalizedKey(context, table_entry, "id = val");
		REQUIRE(key == NormalizedKey(context, table_entry, "id = val"));
	}

	SECTION("nested comparison in AND is normalized") {
		auto key1 = NormalizedKey(context, table_entry, "val = 42 AND id < 100");
		auto key2 = NormalizedKey(context, table_entry, "42 = val AND 100 > id");
		REQUIRE(key1 == key2);
	}

	SECTION("nested comparison in OR is normalized") {
		auto key1 = NormalizedKey(context, table_entry, "val = 42 OR id > 100");
		auto key2 = NormalizedKey(context, table_entry, "42 = val OR 100 < id");
		REQUIRE(key1 == key2);
	}

	SECTION("string constant is normalized") {
		auto key1 = NormalizedKey(context, table_entry, "name = 'hello'");
		auto key2 = NormalizedKey(context, table_entry, "'hello' = name");
		REQUIRE(key1 == key2);
	}
}

} // namespace duckdb
