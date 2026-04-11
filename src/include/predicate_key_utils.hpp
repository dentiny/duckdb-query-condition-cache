#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class DuckTableEntry;

// Parse + CheckBinder-bind a predicate SQL against a table. Returns nullptr on
// empty input or parse failure.
unique_ptr<Expression> BindPredicate(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql);

// Compute a canonical cache key from a predicate SQL string. Use for the table
// function path where input starts as SQL text.
string ComputeCanonicalPredicateKey(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql);

// Compute a canonical cache key from already-bound filter expressions. Use for
// the optimizer path (filter.expressions) to avoid re-binding via CheckBinder.
string ComputeCanonicalPredicateKey(const vector<unique_ptr<Expression>> &expressions);

// Combine expressions under AND, taking ownership. Single input is returned
// unchanged.
unique_ptr<Expression> CombineWithAnd(vector<unique_ptr<Expression>> children);

// Normalize a bound expression tree in-place so equivalent predicates produce
// the same ToString(): comparison constants moved to the right, conjunction
// children sorted by ToString().
void NormalizeExpressionForCacheKey(Expression &expr);

} // namespace duckdb
