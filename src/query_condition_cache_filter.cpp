#include "query_condition_cache_filter.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/vector.hpp"
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
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ConditionCacheFilterBindData>();
	auto &entry = bind_data.cache_entry;

	auto &input_vec = args.data[0];

	UnifiedVectorFormat vdata;
	input_vec.ToUnifiedFormat(args.size(), vdata);
	auto row_ids = UnifiedVectorFormat::GetData<int64_t>(vdata);

	auto first_idx = vdata.sel->get_index(0);
	bool first_valid = vdata.validity.RowIsValid(first_idx);
	row_t first_row_id = first_valid ? row_ids[first_idx] : -1;
	bool first_passes = first_valid ? entry->RowIdPassesFilter(first_row_id) : true;
	idx_t rg_idx = 0;
	idx_t vec_idx = 0;
	if (first_valid && first_row_id >= 0 && first_row_id < MAX_ROW_ID) {
		auto first_row_id_idx = NumericCast<idx_t>(first_row_id);
		rg_idx = first_row_id_idx / DEFAULT_ROW_GROUP_SIZE;
		vec_idx = (first_row_id_idx % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
	}

	bool same_pass_result = true;
	bool same_storage_vector = true;
	for (idx_t i = 1; i < args.size(); ++i) {
		auto row_idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(row_idx)) {
			same_storage_vector = false;
			if (!first_passes) {
				same_pass_result = false;
			}
			break;
		}
		auto row_id = row_ids[row_idx];
		if (entry->RowIdPassesFilter(row_id) != first_passes) {
			same_pass_result = false;
		}
		if (row_id < 0 || row_id >= MAX_ROW_ID) {
			same_storage_vector = false;
			continue;
		}
		auto row_id_idx = NumericCast<idx_t>(row_id);
		if (!first_valid || first_row_id < 0 || first_row_id >= MAX_ROW_ID || row_id_idx / DEFAULT_ROW_GROUP_SIZE != rg_idx ||
		    (row_id_idx % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE != vec_idx) {
			same_storage_vector = false;
		}
	}

	if (same_storage_vector && same_pass_result) {
		result.Reference(Value::BOOLEAN(first_passes));
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto output = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < args.size(); ++i) {
		auto row_idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(row_idx)) {
			output[i] = true;
			continue;
		}
		auto row_id = row_ids[row_idx];
		output[i] = entry->RowIdPassesFilter(row_id);
	}
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
