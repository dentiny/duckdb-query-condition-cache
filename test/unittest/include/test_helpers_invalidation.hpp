#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"

namespace duckdb {

idx_t GetTableOid(Connection &con, const string &table_name);
shared_ptr<ConditionCacheStore> GetStore(Connection &con);
shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate);
void BuildCache(Connection &con, const string &table_name, const string &predicate);

} // namespace duckdb
