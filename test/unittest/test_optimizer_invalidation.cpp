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

// Helper: build cache for (table_name, predicate) via the SQL table function.
static void BuildCache(Connection &con, const string &table_name, const string &predicate) {
	auto result = con.Query("SELECT * FROM condition_cache_build('" + table_name + "', '" + predicate + "')");
	REQUIRE(result->GetError() == "");
}

// Helper: get the cache store from a connection's context.
static shared_ptr<ConditionCacheStore> GetStore(Connection &con) {
	return ConditionCacheStore::GetOrCreate(*con.context);
}

// Helper: look up a cache entry by table OID and predicate.
static shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate) {
	auto store = GetStore(con);
	return store->Lookup(*con.context, {table_oid, predicate});
}

// Helper: get the OID of a table via the catalog API.
static idx_t GetTableOid(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto &context = *con.context;
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
	auto oid = table_entry.oid;
	con.Commit();
	return oid;
}

TEST_CASE("Optimizer invalidation - DELETE removes affected row groups from cache", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	// Create table spanning multiple row groups (122880 rows per RG → 5 RGs for 500000 rows)
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	// Build cache with a selective predicate (only hits RG0: ids 0..2999)
	BuildCache(con, "t", "id < 3000");
	auto entry = LookupEntry(con, table_oid, "id < 3000");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 1);
	REQUIRE(entry->bitvectors.count(0) == 1); // only RG0

	// Also build a broad predicate that hits all RGs
	BuildCache(con, "t", "val = 42");
	auto broad_entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(broad_entry != nullptr);
	REQUIRE(broad_entry->bitvectors.size() == 5);

	// DELETE rows in RG0 only
	auto del_result = con.Query("DELETE FROM t WHERE id < 100");
	REQUIRE_FALSE(del_result->HasError());

	// The selective entry (only had RG0) should be fully removed
	auto after_del = LookupEntry(con, table_oid, "id < 3000");
	REQUIRE(after_del == nullptr);

	// The broad entry should have RG0 removed but RGs 1-4 preserved
	auto broad_after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(broad_after != nullptr);
	REQUIRE(broad_after->bitvectors.size() == 4);
	REQUIRE(broad_after->bitvectors.count(0) == 0);
	REQUIRE(broad_after->bitvectors.count(1) == 1);
	REQUIRE(broad_after->bitvectors.count(2) == 1);
	REQUIRE(broad_after->bitvectors.count(3) == 1);
	REQUIRE(broad_after->bitvectors.count(4) == 1);
}

TEST_CASE("Optimizer invalidation - UPDATE removes affected row groups from cache", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	// UPDATE a row in RG0 (id=200 is in the first row group)
	auto upd_result = con.Query("UPDATE t SET val = 999 WHERE id = 200");
	REQUIRE_FALSE(upd_result->HasError());

	// RG0 should be invalidated, others preserved
	auto after_upd = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after_upd != nullptr);
	REQUIRE(after_upd->bitvectors.size() == 4);
	REQUIRE(after_upd->bitvectors.count(0) == 0);
}

TEST_CASE("Optimizer invalidation - INSERT only invalidates affected row groups", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	// 500000 rows = 5 row groups (122880 per RG). Last RG (RG4) starts at row 491520.
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	// Build cache with a selective predicate (only RG0)
	BuildCache(con, "t", "id < 3000");
	auto selective = LookupEntry(con, table_oid, "id < 3000");
	REQUIRE(selective != nullptr);
	REQUIRE(selective->bitvectors.size() == 1);
	REQUIRE(selective->bitvectors.count(0) == 1);

	// Build cache with a broad predicate (all RGs)
	BuildCache(con, "t", "val = 42");
	auto broad = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(broad != nullptr);
	REQUIRE(broad->bitvectors.size() == 5);

	// INSERT a single row — appended at the end, affects only the last RG (RG4)
	auto ins_result = con.Query("INSERT INTO t VALUES (999999, 0)");
	REQUIRE_FALSE(ins_result->HasError());

	// Selective entry (RG0 only) should be preserved — INSERT didn't touch RG0
	auto selective_after = LookupEntry(con, table_oid, "id < 3000");
	REQUIRE(selective_after != nullptr);
	REQUIRE(selective_after->bitvectors.size() == 1);
	REQUIRE(selective_after->bitvectors.count(0) == 1);

	// Broad entry should have RG4 invalidated, RGs 0-3 preserved
	auto broad_after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(broad_after != nullptr);
	REQUIRE(broad_after->bitvectors.size() == 4);
	REQUIRE(broad_after->bitvectors.count(0) == 1);
	REQUIRE(broad_after->bitvectors.count(1) == 1);
	REQUIRE(broad_after->bitvectors.count(2) == 1);
	REQUIRE(broad_after->bitvectors.count(3) == 1);
	REQUIRE(broad_after->bitvectors.count(4) == 0);
}

TEST_CASE("Optimizer invalidation - INSERT spanning multiple new row groups", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	// Start with a small table (1 row group)
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(100000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 1); // 1 RG
	REQUIRE(entry->bitvectors.count(0) == 1);

	// INSERT enough rows to create new row groups (add 300000 rows → spans RG0 tail + RG1 + RG2)
	auto ins_result = con.Query("INSERT INTO t SELECT i AS id, i % 100 AS val FROM range(300000) t(i)");
	REQUIRE_FALSE(ins_result->HasError());

	// RG0 should be invalidated (new rows appended into it), original entry gone
	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after == nullptr); // only had RG0, which got invalidated
}

TEST_CASE("Optimizer invalidation - INSERT ON CONFLICT DO UPDATE invalidates per row group",
          "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
	con.Query("INSERT INTO t SELECT i, i % 100 FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	// ON CONFLICT DO UPDATE: row 200 is in RG0 (200 / 122880 = 0)
	auto ins_result = con.Query("INSERT INTO t VALUES (200, 999) ON CONFLICT (id) DO UPDATE SET val = 999");
	REQUIRE_FALSE(ins_result->HasError());

	// Only RG0 should be invalidated; RGs 1-4 remain
	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after != nullptr);
	REQUIRE(after->bitvectors.size() == 4);
	REQUIRE(after->bitvectors.count(0) == 0); // RG0 invalidated
	REQUIRE(after->bitvectors.count(1) == 1);
	REQUIRE(after->bitvectors.count(2) == 1);
	REQUIRE(after->bitvectors.count(3) == 1);
	REQUIRE(after->bitvectors.count(4) == 1);
}

TEST_CASE("Optimizer invalidation - cross-table isolation", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t1 AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	con.Query("CREATE TABLE t2 AS SELECT i AS id, i % 10 AS val FROM range(1000) t(i)");
	auto t1_oid = GetTableOid(con, "t1");

	// Build cache for t1
	BuildCache(con, "t1", "val = 42");
	auto t1_entry = LookupEntry(con, t1_oid, "val = 42");
	REQUIRE(t1_entry != nullptr);
	REQUIRE(t1_entry->bitvectors.size() == 5);

	// DML on t2 should not affect t1's cache
	con.Query("DELETE FROM t2 WHERE id < 100");
	con.Query("UPDATE t2 SET val = 0 WHERE id = 500");
	con.Query("INSERT INTO t2 VALUES (9999, 0)");

	auto t1_after = LookupEntry(con, t1_oid, "val = 42");
	REQUIRE(t1_after != nullptr);
	REQUIRE(t1_after->bitvectors.size() == 5);
}

TEST_CASE("Optimizer invalidation - DELETE across multiple row groups", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	// DELETE rows spanning RG0 and RG2 (RG size = 122880)
	// RG0: ids 0..122879, RG2: ids 245760..368639
	auto del_result = con.Query("DELETE FROM t WHERE id < 100 OR (id >= 250000 AND id < 250100)");
	REQUIRE_FALSE(del_result->HasError());

	// RG0 and RG2 should be invalidated
	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after != nullptr);
	REQUIRE(after->bitvectors.size() == 3);
	REQUIRE(after->bitvectors.count(0) == 0);
	REQUIRE(after->bitvectors.count(1) == 1);
	REQUIRE(after->bitvectors.count(2) == 0);
	REQUIRE(after->bitvectors.count(3) == 1);
	REQUIRE(after->bitvectors.count(4) == 1);
}

TEST_CASE("Optimizer invalidation - MERGE invalidates matched and inserted row groups", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	// Create target table spanning multiple row groups
	con.Query("CREATE TABLE dst AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "dst");

	BuildCache(con, "dst", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	// Create source table with:
	// - id=200 matches an existing row in RG0 (update path)
	// - id=600000 does not exist (insert path, appended after last RG)
	con.Query("CREATE TABLE src (id INTEGER, val INTEGER)");
	con.Query("INSERT INTO src VALUES (200, 999), (600000, 42)");

	auto merge_result = con.Query("MERGE INTO dst USING src ON dst.id = src.id "
	                              "WHEN MATCHED THEN UPDATE SET val = src.val "
	                              "WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.val)");
	REQUIRE_FALSE(merge_result->HasError());

	// RG0 should be invalidated (matched update of id=200)
	// Last RG (RG4) should be invalidated (insert of id=600000 appended there)
	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after != nullptr);
	// RG0 and RG4 invalidated, RGs 1-3 remain
	REQUIRE(after->bitvectors.count(0) == 0);
	REQUIRE(after->bitvectors.count(1) == 1);
	REQUIRE(after->bitvectors.count(2) == 1);
	REQUIRE(after->bitvectors.count(3) == 1);
	REQUIRE(after->bitvectors.count(4) == 0);
}

TEST_CASE("Optimizer invalidation - TRUNCATE does not crash with stale cache", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 5);

	// TRUNCATE removes all rows — stale cache entries remain but are harmless
	auto trunc_result = con.Query("TRUNCATE t");
	REQUIRE_FALSE(trunc_result->HasError());

	// Query on empty table should return 0 rows regardless of stale cache
	auto count_result = con.Query("SELECT count(*) FROM t WHERE val = 42");
	REQUIRE_FALSE(count_result->HasError());
	auto chunk = count_result->Fetch();
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("Optimizer invalidation - DROP TABLE does not crash with stale cache", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);

	// DROP TABLE — stale cache entries become orphaned
	auto drop_result = con.Query("DROP TABLE t");
	REQUIRE_FALSE(drop_result->HasError());

	// Recreate table (gets new OID) and query — must work correctly
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 10 AS val FROM range(1000) t(i)");

	auto count_result = con.Query("SELECT count(*) FROM t WHERE val = 5");
	REQUIRE_FALSE(count_result->HasError());
	auto chunk = count_result->Fetch();
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 100);
}

TEST_CASE("Optimizer invalidation - INSERT into partial tail row group", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	// 130000 rows = 1 full RG (122880) + partial RG with 7120 rows
	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(130000) t(i)");
	auto table_oid = GetTableOid(con, "t");

	BuildCache(con, "t", "val = 42");
	auto entry = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(entry != nullptr);
	REQUIRE(entry->bitvectors.size() == 2); // RG0 (full) + RG1 (partial)

	// INSERT a single row — goes into the partial tail RG (RG1)
	auto ins_result = con.Query("INSERT INTO t VALUES (999999, 42)");
	REQUIRE_FALSE(ins_result->HasError());

	// RG1 (partial tail) should be invalidated, RG0 preserved
	auto after = LookupEntry(con, table_oid, "val = 42");
	REQUIRE(after != nullptr);
	REQUIRE(after->bitvectors.size() == 1);
	REQUIRE(after->bitvectors.count(0) == 1);
	REQUIRE(after->bitvectors.count(1) == 0);

	// Query must still return correct results including the new row
	auto count_result = con.Query("SELECT count(*) FROM t WHERE val = 42");
	REQUIRE_FALSE(count_result->HasError());
	auto chunk = count_result->Fetch();
	// 130000 / 100 = 1300 rows with val=42, plus 1 inserted
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 1301);
}

// --- Plan injection tests (verify optimizer injects PhysicalCacheInvalidator with correct fields) ---

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

TEST_CASE("Optimizer invalidation - injects ROW_ID invalidator for DELETE", "[invalidation][optimizer]") {
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
}

TEST_CASE("Optimizer invalidation - injects ROW_ID invalidator for UPDATE", "[invalidation][optimizer]") {
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
}

TEST_CASE("Optimizer invalidation - injects INSERT invalidator for INSERT", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t (id INTEGER, val INTEGER)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("INSERT INTO t VALUES (1, 2)");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::INSERT);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 0);
}

TEST_CASE("Optimizer invalidation - no invalidator for SELECT", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(100) t(i)");

	auto prepared = con.Prepare("SELECT * FROM t WHERE id < 10");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE_FALSE(invalidator);
}

TEST_CASE("Optimizer invalidation - injects MERGE invalidator for INSERT ON CONFLICT",
          "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");
	con.Query("INSERT INTO t VALUES (1, 10)");
	auto table_oid = GetTableOid(con, "t");

	auto prepared = con.Prepare("INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = 20");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->table_oid == table_oid);
}

TEST_CASE("Optimizer invalidation - injects MERGE invalidator for MERGE", "[invalidation][optimizer]") {
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

TEST_CASE("Optimizer invalidation - injects INSERT invalidator for INSERT SELECT", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE src AS SELECT i AS id FROM range(100) t(i)");
	con.Query("CREATE TABLE dst (id INTEGER)");
	auto table_oid = GetTableOid(con, "dst");

	auto prepared = con.Prepare("INSERT INTO dst SELECT * FROM src WHERE id < 50");
	REQUIRE_FALSE(prepared->HasError());
	auto *invalidator = FindInvalidator(prepared->data->physical_plan->Root());
	REQUIRE(invalidator);
	REQUIRE(invalidator->mode == CacheInvalidatorMode::INSERT);
	REQUIRE(invalidator->table_oid == table_oid);
	REQUIRE(invalidator->pre_insert_row_count == 0);
}

TEST_CASE("Optimizer invalidation - DML on empty cache is no-op", "[invalidation][optimizer]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<QueryConditionCacheExtension>();
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id FROM range(1000) t(i)");

	// DML without any cache built should not crash
	auto del_result = con.Query("DELETE FROM t WHERE id < 10");
	REQUIRE_FALSE(del_result->HasError());

	auto upd_result = con.Query("UPDATE t SET id = id + 1 WHERE id < 10");
	REQUIRE_FALSE(upd_result->HasError());

	auto ins_result = con.Query("INSERT INTO t VALUES (9999)");
	REQUIRE_FALSE(ins_result->HasError());
}
} // namespace duckdb
