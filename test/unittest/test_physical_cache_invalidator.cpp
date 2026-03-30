#include "catch/catch.hpp"
#include "test_helpers_invalidation.hpp"
#include "query_condition_cache_extension.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

TEST_CASE("PhysicalCacheInvalidator - GetName returns CACHE_INVALIDATOR", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");

	auto prepared = con.Prepare("DELETE FROM t WHERE id < 10");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->GetName() == "CACHE_INVALIDATOR");
}

TEST_CASE("PhysicalCacheInvalidator - ParamsToString contains mode and table OID", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("DELETE FROM t WHERE id < 10");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	auto params = invalidator->ParamsToString();
	REQUIRE(params["Table OID"] == to_string(table_oid));
	REQUIRE(params["Mode"] == "CLEAR_TABLE");
}

TEST_CASE("PhysicalCacheInvalidator - ParallelOperator returns true", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");

	auto prepared = con.Prepare("DELETE FROM t WHERE id < 10");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->ParallelOperator());
	REQUIRE(invalidator->RequiresOperatorFinalize());
}

TEST_CASE("PhysicalCacheInvalidator - passthrough preserves data", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");

	auto del_result = con.Query("DELETE FROM t WHERE id < 50");
	REQUIRE_FALSE(del_result->HasError());

	auto count_result = con.Query("SELECT count(*) FROM t");
	REQUIRE_FALSE(count_result->HasError());
	auto chunk = count_result->Fetch();
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 50);
}

TEST_CASE("PhysicalCacheInvalidator - parallel execution preserves correctness", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");

	BuildCache(con, "t", "val = 42");

	auto table_oid = GetTableOid(con, "t");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	auto del_result = con.Query("DELETE FROM t WHERE val = 42");
	REQUIRE_FALSE(del_result->HasError());

	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after == nullptr);
}

} // namespace duckdb
