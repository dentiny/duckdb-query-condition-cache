#include "test_helpers_invalidation.hpp"

#include "predicate_key_utils.hpp"

#include "catch/catch.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace {

DuckTableEntry &GetDuckTableEntry(ClientContext &context, const string &table_name) {
	return Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
}

} // namespace

idx_t GetTableOid(Connection &con, const string &table_name) {
	con.BeginTransaction();
	auto oid = GetDuckTableEntry(*con.context, table_name).oid;
	con.Commit();
	return oid;
}

shared_ptr<ConditionCacheStore> GetStore(Connection &con) {
	return ConditionCacheStore::GetOrCreate(*con.context);
}

shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate) {
	auto store = GetStore(con);
	return store->Lookup(*con.context, {table_oid, predicate});
}

shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, const string &table_name, const string &predicate) {
	auto &context = *con.context;
	con.BeginTransaction();
	auto &table_entry = GetDuckTableEntry(context, table_name);
	auto table_oid = table_entry.oid;
	auto canonical_key = ComputeCanonicalPredicateKey(context, table_entry, predicate);
	auto store = GetStore(con);
	auto entry = store->Lookup(context, table_entry, {table_oid, canonical_key});
	con.Commit();
	return entry;
}

void BuildCache(Connection &con, const string &table_name, const string &predicate) {
	auto sql = StringUtil::Format("SELECT * FROM condition_cache_build('%s', '%s')", table_name, predicate);
	auto result = con.Query(sql);
	REQUIRE_FALSE(result->HasError());
}

} // namespace duckdb
