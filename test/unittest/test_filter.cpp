#include "catch/catch.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

TEST_CASE("CacheExpressionFilter - CheckStatistics", "[query_condition_cache]") {
	// Build a cache entry with qualifying vectors in row group 0 and 2,
	// and an explicitly cached empty row group 1.
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/5);
	entry->EnsureRowGroup(/*rg_idx=*/1);
	entry->SetQualifyingVector(/*rg_idx=*/2, /*vec_idx=*/10);

	// Create a dummy expression for the filter (won't be evaluated in stats check)
	auto dummy_expr = make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0);
	CacheExpressionFilter filter(std::move(dummy_expr), entry);

	SECTION("row group with qualifying vectors: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(0));
		NumericStats::SetMax(stats, Value::BIGINT(100));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("known-empty row group: prune") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(122880));
		NumericStats::SetMax(stats, Value::BIGINT(200000));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE);
	}

	SECTION("row group 2 with qualifying vectors: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(245760));
		NumericStats::SetMax(stats, Value::BIGINT(300000));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("stats without min/max: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}
}
} // namespace duckdb
