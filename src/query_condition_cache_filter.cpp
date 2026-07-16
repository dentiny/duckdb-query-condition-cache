#include "query_condition_cache_filter.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

ConditionCacheFilterBindData::ConditionCacheFilterBindData(shared_ptr<ConditionCacheEntry> entry)
    : cache_entry(std::move(entry)) {
}

unique_ptr<FunctionData> ConditionCacheFilterBindData::Copy() const {
	return make_uniq<ConditionCacheFilterBindData>(cache_entry);
}

bool ConditionCacheFilterBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ConditionCacheFilterBindData>();
	return cache_entry == other.cache_entry;
}

unique_ptr<FunctionData> ConditionCacheFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto empty_entry = make_shared_ptr<ConditionCacheEntry>();
	return make_uniq<ConditionCacheFilterBindData>(std::move(empty_entry));
}

struct ConditionCacheFilterState : public FunctionLocalState {};

unique_ptr<FunctionLocalState> ConditionCacheFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data) {
	return make_uniq<ConditionCacheFilterState>();
}

ScalarFunction ConditionCacheFilterFunction() {
	return ScalarFunction("__condition_cache_filter", {LogicalType {LogicalTypeId::BIGINT}},
	                      LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFn, ConditionCacheFilterBind,
	                      nullptr, nullptr, ConditionCacheFilterInit);
}

void ConditionCacheFilterFn(DataChunk &args, ExpressionState &state, Vector &result) {
	ALWAYS_ASSERT(args.size() > 0);
	ALWAYS_ASSERT(args.ColumnCount() > 0);
	const auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ConditionCacheFilterBindData>();
	const auto &entry = bind_data.cache_entry;

	auto &input_vec = args.data[0];

	UnifiedVectorFormat vdata;
	input_vec.ToUnifiedFormat(args.size(), vdata);
	const auto row_ids = UnifiedVectorFormat::GetData<int64_t>(vdata);

	const auto first_idx = vdata.sel->get_index(0);
	const int64_t first_row_id = row_ids[first_idx];

	const auto row_id_passes = [&](int64_t row_id) {
		const auto numeric_row_id = NumericCast<idx_t>(row_id);
		const auto rg_idx = numeric_row_id / DEFAULT_ROW_GROUP_SIZE;
		const auto vec_idx = (numeric_row_id % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
		return entry->VectorPassesFilter(rg_idx, vec_idx);
	};

	const bool passes = row_id_passes(first_row_id);

	// DuckDB scan vectors are aligned relative to their physical row group. A
	// physical row group can contain fewer than DEFAULT_ROW_GROUP_SIZE rows, so
	// one scan vector can overlap two cache vectors derived from absolute ROW_IDs.
	const auto first_cache_vector = NumericCast<idx_t>(first_row_id) / STANDARD_VECTOR_SIZE;
	const auto last_idx = vdata.sel->get_index(args.size() - 1);
	const auto last_row_id = row_ids[last_idx];
	const auto last_cache_vector = NumericCast<idx_t>(last_row_id) / STANDARD_VECTOR_SIZE;

	if (first_cache_vector == last_cache_vector) {
		result.Reference(Value::BOOLEAN(passes));
		return;
	}

	// A native table-filter invocation covers at most one physical scan vector,
	// and its selection remains ordered. It can therefore overlap at most two
	// adjacent cache vectors. Look up both decisions once, then classify rows by
	// their absolute cache-vector index without taking the entry lock per row.
	if (last_cache_vector == first_cache_vector + 1) {
		const auto last_passes = row_id_passes(last_row_id);
		if (last_passes == passes) {
			result.Reference(Value::BOOLEAN(passes));
			return;
		}
		UnaryExecutor::Execute<int64_t, bool>(input_vec, result, args.size(), [&](int64_t row_id) {
			return NumericCast<idx_t>(row_id) / STANDARD_VECTOR_SIZE == first_cache_vector ? passes : last_passes;
		});
		return;
	}

	// Conservative fallback for unexpected, non-contiguous input.
	UnaryExecutor::Execute<int64_t, bool>(input_vec, result, args.size(), row_id_passes);
}

CacheExpressionFilter::CacheExpressionFilter(unique_ptr<Expression> expr_p, shared_ptr<ConditionCacheEntry> entry)
    : ExpressionFilter(std::move(expr_p)), cache_entry(std::move(entry)) {
}

FilterPropagateResult CacheExpressionFilter::CheckStatistics(BaseStatistics &stats) const {
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	auto min_val = NumericStats::GetMin<int64_t>(stats);
	auto max_val = NumericStats::GetMax<int64_t>(stats);

	idx_t min_rg = NumericCast<idx_t>(min_val) / DEFAULT_ROW_GROUP_SIZE;
	idx_t max_rg = NumericCast<idx_t>(max_val) / DEFAULT_ROW_GROUP_SIZE;

	if (!cache_entry->StatisticsRangeIsAllEmptyCached(min_rg, max_rg)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

unique_ptr<TableFilter> CacheExpressionFilter::Copy() const {
	return make_uniq<CacheExpressionFilter>(expr->Copy(), cache_entry);
}

} // namespace duckdb
