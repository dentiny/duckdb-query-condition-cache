#include "catch/catch.hpp"
#include "physical_cache_invalidator.hpp"
#include "query_condition_cache_extension.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

static PhysicalCacheInvalidator *FindInvalidator(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::EXTENSION && op.GetName() == "CACHE_INVALIDATOR") {
		return &op.Cast<PhysicalCacheInvalidator>();
	}
	for (auto &child : op.children) {
		auto *result = FindInvalidator(child.get());
		if (result) {
			return result;
		}
	}
	return nullptr;
}

static idx_t GetTableOid(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto &context = *con.context;
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
	auto oid = table_entry.oid;
	con.Commit();
	return oid;
}

TEST_CASE("PhysicalCacheInvalidator - DELETE has ROW_ID mode with correct fields", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("DELETE FROM t WHERE id < 10");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::ROW_ID);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 0);
}

TEST_CASE("PhysicalCacheInvalidator - UPDATE has ROW_ID mode with correct fields", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i AS val FROM range(100) t(i)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("UPDATE t SET val = 999 WHERE id = 5");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::ROW_ID);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 0);
}

TEST_CASE("PhysicalCacheInvalidator - INSERT has INSERT mode with correct pre_insert_row_count",
          "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("INSERT INTO t VALUES (999)");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::INSERT);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 100);
}

TEST_CASE("PhysicalCacheInvalidator - INSERT into empty table has zero pre_insert_row_count",
          "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t (id INTEGER)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("INSERT INTO t VALUES (1)");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::INSERT);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 0);
}

TEST_CASE("PhysicalCacheInvalidator - MERGE has MERGE mode with correct fields", "[physical_invalidator]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE dst AS SELECT i AS id, i AS val FROM range(100) t(i)");
	con.Query("CREATE TABLE src AS SELECT i AS id, i * 10 AS val FROM range(50, 150) t(i)");
	auto table_oid = GetTableOid(con, "dst");

	auto prepared = con.Prepare("MERGE INTO dst USING src ON dst.id = src.id "
	                            "WHEN MATCHED THEN UPDATE SET val = src.val "
	                            "WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.val)");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::MERGE);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 100);
}

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
	REQUIRE(params["Mode"] == "ROW_ID");
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

	auto build_result = con.Query("SELECT * FROM condition_cache_build('t', 'val = 42')");
	REQUIRE_FALSE(build_result->HasError());

	auto table_oid = GetTableOid(con, "t");
	auto store = ConditionCacheStore::GetOrCreate(*con.context);
	auto entry = store->Lookup(*con.context, {table_oid, "val = 42"});
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	auto del_result = con.Query("DELETE FROM t WHERE val = 42");
	REQUIRE_FALSE(del_result->HasError());

	auto after = store->Lookup(*con.context, {table_oid, "val = 42"});
	REQUIRE(after == nullptr);
}

} // namespace duckdb
