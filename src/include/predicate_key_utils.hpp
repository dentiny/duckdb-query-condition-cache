#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class DuckTableEntry;

// Parse a predicate SQL string, bind it against a table using CheckBinder,
// and return the bound expression. Returns nullptr if predicate_sql is empty
// or parsing fails.
unique_ptr<Expression> BindPredicate(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql);

// Compute a canonical cache key string from a predicate SQL.
// Parses, binds, normalizes (comparison operand order + conjunction sort),
// and returns the normalized ToString(). Returns empty string if predicate_sql
// is empty or parsing fails.
string ComputeCanonicalPredicateKey(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql);

// Normalize a bound expression tree in-place for canonical cache key generation.
// Ensures equivalent predicates produce the same ToString() output:
//   - Comparison operands: constants moved to the right ("42 = val" -> "val = 42")
//   - Conjunction children (AND/OR): sorted by ToString() ("b = 2 OR a = 1" -> "a = 1 OR b = 2")
void NormalizeExpressionForCacheKey(Expression &expr);

} // namespace duckdb
