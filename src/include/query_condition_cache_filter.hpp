#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

// Holds a shared_ptr to the ConditionCacheEntry so the filter function
// and CheckStatistics can access the bitvectors during execution.
struct ConditionCacheFilterBindData : public FunctionData {
	shared_ptr<ConditionCacheEntry> cache_entry;

	explicit ConditionCacheFilterBindData(shared_ptr<ConditionCacheEntry> entry);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

// Called during plan deserialization/verification when the optimizer's bind data
// is not available. Returns an empty cache entry that passes all rows through.
unique_ptr<FunctionData> ConditionCacheFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments);

unique_ptr<FunctionLocalState> ConditionCacheFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data);

// Vector-level filter: takes a ROW_ID column as input, looks up the bitvector
// for that row group + vector index, and returns a constant BOOLEAN for the
// entire vector. Defaults to true for row groups not in cache.
void ConditionCacheFilterFn(DataChunk &args, ExpressionState &state, Vector &result);

// Row-group level pruning via CheckStatistics. If all row groups covered by
// the stats range are cached and empty, returns FILTER_ALWAYS_FALSE to skip
// the entire row group. Otherwise returns NO_PRUNING_POSSIBLE.
class CacheExpressionFilter : public ExpressionFilter {
public:
	shared_ptr<ConditionCacheEntry> cache_entry;

	CacheExpressionFilter(unique_ptr<Expression> expr, shared_ptr<ConditionCacheEntry> entry);

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	unique_ptr<TableFilter> Copy() const override;
};

} // namespace duckdb
