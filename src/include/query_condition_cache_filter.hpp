#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

// Bind data containing the cached bitvectors
struct ConditionCacheFilterBindData : public FunctionData {
	shared_ptr<ConditionCacheEntry> cache_entry;

	explicit ConditionCacheFilterBindData(shared_ptr<ConditionCacheEntry> entry);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

// Bind callback (should never be called directly — bind data is created by the optimizer)
unique_ptr<FunctionData> ConditionCacheFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments);

// Per-thread local state (placeholder)
struct ConditionCacheFilterState : public FunctionLocalState {};

unique_ptr<FunctionLocalState> ConditionCacheFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data);

// The filter function: for each vector, check the bitvector and return constant boolean
void ConditionCacheFilterFn(DataChunk &args, ExpressionState &state, Vector &result);

// ExpressionFilter subclass that overrides CheckStatistics for row-group level pruning
class CacheExpressionFilter : public ExpressionFilter {
public:
	CacheKey cache_key;
	shared_ptr<ConditionCacheEntry> cache_entry;

	CacheExpressionFilter(unique_ptr<Expression> expr, CacheKey key, shared_ptr<ConditionCacheEntry> entry);

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	unique_ptr<TableFilter> Copy() const override;
	string ToString(const string &column_name) const override;
};

} // namespace duckdb
