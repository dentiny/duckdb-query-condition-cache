#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DuckTableEntry;
class Expression;

// Normalize comparison operand order in-place: put foldable constants on the right
// so that "42 = val" and "val = 42" produce the same ToString() for cache key.
// Reference: duckdb/src/optimizer/rule/comparison_simplification.cpp
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
