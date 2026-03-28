#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DuckTableEntry;
class Expression;

// Normalize a bound expression tree in-place for canonical cache key generation.
// Ensures equivalent predicates produce the same ToString() output:
//   - Comparison operands: constants moved to the right ("42 = val" → "val = 42")
//   - Conjunction children (AND/OR): sorted by ToString() ("b = 2 OR a = 1" -> "a = 1 OR b = 2")
void NormalizeExpressionForKey(Expression &expr);

// TODO: Exposed for future reuse and C++ unit testing.
// Scan all rows in the table, evaluate bound_expr, and build a ConditionCacheEntry
// recording which vectors contain qualifying rows.
// Modifies bound_expr in-place (remaps column indices to scan positions).
shared_ptr<ConditionCacheEntry> BuildCacheEntry(ClientContext &context, DuckTableEntry &table_entry,
                                                Expression &bound_expr);

TableFunction ConditionCacheBuildFunction();
TableFunction ConditionCacheInfoFunction();

} // namespace duckdb
