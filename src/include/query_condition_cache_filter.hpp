#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

// Holds a shared_ptr to the ConditionCacheEntry so the filter function
// and CheckStatistics can query the cache via ConditionCacheEntry's thread-safe API.
struct ConditionCacheFilterBindData : public FunctionData {
	shared_ptr<ConditionCacheEntry> cache_entry;

	explicit ConditionCacheFilterBindData(shared_ptr<ConditionCacheEntry> entry);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

// Called during plan deserialization/verification when the optimizer's bind data
// is not available. Returns an empty cache entry that passes all rows through.
unique_ptr<FunctionData> ConditionCacheFilterBind(BindScalarFunctionInput &input);

unique_ptr<FunctionLocalState> ConditionCacheFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data);

// Factory for the internal scalar filter ConditionCacheFilterFn used by optimizer-injected cache checks.
ScalarFunction ConditionCacheFilterFunction();

// Vector-level filter: takes a ROW_ID column as input, looks up the bitvector
// for that row group + vector index, and returns a constant BOOLEAN for the
// entire vector. Defaults to true for row groups not in cache.
void ConditionCacheFilterFn(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace duckdb
