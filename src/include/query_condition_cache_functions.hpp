#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DuckTableEntry;
class Expression;

// TODO: Exposed for future reuse and C++ unit testing.
// Scan all rows in the table, evaluate bound_expr, and build a ConditionCacheEntry
// recording which vectors contain qualifying rows.
// Modifies bound_expr in-place (remaps column indices to scan positions).
shared_ptr<ConditionCacheEntry> BuildCacheEntry(ClientContext &context, DuckTableEntry &table_entry,
                                                Expression &bound_expr);

struct CacheEntryStats {
	idx_t qualifying_vectors;
	idx_t total_vectors;
	idx_t qualifying_row_groups;
	idx_t total_row_groups;
};

// TODO: Exposed for future reuse.
CacheEntryStats ComputeCacheEntryStats(const ConditionCacheEntry &entry, idx_t total_rows);

TableFunction ConditionCacheBuildFunction();
TableFunction ConditionCacheInfoFunction();

} // namespace duckdb
