#pragma once

#include "physical_cache_invalidator.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"

namespace duckdb {

idx_t GetTableOid(Connection &con, const string &table_name);
shared_ptr<ConditionCacheStore> GetStore(Connection &con);
shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, idx_t table_oid, const string &predicate);
shared_ptr<ConditionCacheEntry> LookupEntry(Connection &con, const string &table_name, const string &predicate);
void BuildCache(Connection &con, const string &table_name, const string &predicate);

// Recursively search the physical plan tree for a PhysicalCacheInvalidator node.
inline PhysicalCacheInvalidator *FindInvalidator(PhysicalOperator &op) {
	if (op.GetName() == "CACHE_INVALIDATOR") {
		return dynamic_cast<PhysicalCacheInvalidator *>(&op);
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
