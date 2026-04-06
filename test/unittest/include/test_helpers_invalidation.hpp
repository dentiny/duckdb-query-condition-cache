#pragma once

#include "catch/catch.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

inline idx_t GetTableOid(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto &context = *con.context;
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
	auto oid = table_entry.oid;
	con.Commit();
	return oid;
}

inline shared_ptr<ConditionCacheStore> GetStore(Connection &con) {
	return ConditionCacheStore::GetOrCreate(*con.context);
}

inline shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate) {
	auto store = GetStore(con);
	return store->Lookup(*con.context, {table_oid, predicate});
}

inline void BuildCache(Connection &con, const string &table_name, const string &predicate) {
	auto sql = StringUtil::Format("SELECT * FROM condition_cache_build('%s', '%s')", table_name, predicate);
	auto result = con.Query(sql);
	REQUIRE(result->GetError() == "");
}

} // namespace duckdb
