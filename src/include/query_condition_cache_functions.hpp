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

TableFunction ConditionCacheBuildFunction();

} // namespace duckdb
