#include "catch/catch.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

using namespace duckdb;

TEST_CASE("CacheExpressionFilter - CheckStatistics", "[query_condition_cache]") {
	// Build a cache entry with qualifying vectors in row group 0 and 2
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->bitvectors[0].SetVector(0);
	entry->bitvectors[0].SetVector(5);
	entry->bitvectors[2].SetVector(10);

	// Create a dummy expression for the filter (won't be evaluated in stats check)
	auto dummy_expr = make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0);
	CacheExpressionFilter filter(std::move(dummy_expr), CacheKey {0, "val > 5"}, entry);

	SECTION("row group with qualifying vectors: no pruning") {
		// Stats covering row group 0 (row_ids 0..122879)
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(0));
		NumericStats::SetMax(stats, Value::BIGINT(100));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("row group without qualifying vectors: no pruning") {
		// Stats covering row group 1 (row_ids 122880..245759) - no entry in cache
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(122880));
		NumericStats::SetMax(stats, Value::BIGINT(200000));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("row group 2 with qualifying vectors: no pruning") {
		// Stats covering row group 2 (row_ids 245760..368639)
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
