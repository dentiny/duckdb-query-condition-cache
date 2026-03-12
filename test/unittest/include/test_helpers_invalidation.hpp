#pragma once

#include "catch/catch.hpp"
#include "physical_cache_invalidator.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

// Helper: get the OID of a table via the catalog API.
inline idx_t GetTableOid(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto &context = *con.context;
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
	auto oid = table_entry.oid;
	con.Commit();
	return oid;
}

// Helper: get the cache store from a connection's context.
inline shared_ptr<ConditionCacheStore> GetStore(Connection &con) {
	return ConditionCacheStore::GetOrCreate(*con.context);
}

// Helper: look up a cache entry by table OID and predicate.
inline shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate) {
	auto store = GetStore(con);
	return store->Lookup(*con.context, {table_oid, predicate});
}

// Helper: build cache for (table_name, predicate) via the SQL table function.
inline void BuildCache(Connection &con, const string &table_name, const string &predicate) {
	auto result = con.Query("SELECT * FROM condition_cache_build('" + table_name + "', '" + predicate + "')");
	REQUIRE(result->GetError() == "");
}

// Helper: find a PhysicalCacheInvalidator in a physical plan tree.
inline PhysicalCacheInvalidator *FindInvalidator(PhysicalOperator &op) {
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

} // namespace duckdb
