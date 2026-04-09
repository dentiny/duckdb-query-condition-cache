#include "catch/catch.hpp"

#include "logical_cache_invalidator.hpp"
#include "test_helpers_invalidation.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/planner.hpp"
#include "test_helpers.hpp"

namespace duckdb {
namespace {

struct InvalidatorExpectation {
	CacheInvalidatorMode mode;
	idx_t table_oid;
	idx_t pre_insert_row_count;
	idx_t row_id_column_index;
	idx_t expression_count;
};

unique_ptr<LogicalOperator> OptimizeQuery(Connection &con, const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	REQUIRE(parser.statements.size() == 1);

	con.context->transaction.BeginTransaction();

	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));

	Optimizer optimizer(*planner.binder, *con.context);
	return optimizer.Optimize(std::move(planner.plan));
}

LogicalCacheInvalidator &GetOnlyInvalidatorChild(LogicalOperator &op) {
	REQUIRE(op.children.size() == 1);
	REQUIRE(op.children[0]->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR);
	return op.children[0]->Cast<LogicalCacheInvalidator>();
}

void VerifyCopiedInvalidator(ClientContext &context, LogicalOperator &plan, const InvalidatorExpectation &expected) {
	auto copied = plan.Copy(context);
	auto &copied_invalidator = GetOnlyInvalidatorChild(*copied);
	REQUIRE(copied_invalidator.table_oid == expected.table_oid);
	REQUIRE(copied_invalidator.mode == expected.mode);
	REQUIRE(copied_invalidator.pre_insert_row_count == expected.pre_insert_row_count);
	REQUIRE(copied_invalidator.row_id_column_index == expected.row_id_column_index);
	REQUIRE(copied_invalidator.expressions.size() == expected.expression_count);
}

} // namespace

TEST_CASE("CacheInvalidationOptimizer injects logical invalidators for DML", "[invalidation_optimizer]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("LOAD query_condition_cache"));
	REQUIRE_NO_FAIL(con.Query("SET use_query_condition_cache = false"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE target (id INTEGER, val INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE source (id INTEGER, val INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO source VALUES (2, 200), (5, 500)"));

	const auto table_oid = GetTableOid(con, "target");

	SECTION("DELETE plans inject INVALIDATE mode with a bound row-id expression") {
		auto plan = OptimizeQuery(con, "DELETE FROM target WHERE id = 1");
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_DELETE);

		auto &invalidator = GetOnlyInvalidatorChild(*plan);
		REQUIRE(invalidator.table_oid == table_oid);
		REQUIRE(invalidator.mode == CacheInvalidatorMode::INVALIDATE);
		REQUIRE(invalidator.pre_insert_row_count == 0);
		REQUIRE(invalidator.row_id_column_index == 0);
		REQUIRE(invalidator.expressions.size() == 1);
		REQUIRE_FALSE(invalidator.expressions[0]->ToString().empty());

		VerifyCopiedInvalidator(*con.context, *plan, {CacheInvalidatorMode::INVALIDATE, table_oid, 0, 0, 1});
		con.context->transaction.Commit();
	}

	SECTION("UPDATE plans inject INVALIDATE mode and point at the trailing row-id column") {
		auto plan = OptimizeQuery(con, "UPDATE target SET val = val + 1 WHERE id = 2");
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_UPDATE);

		auto &invalidator = GetOnlyInvalidatorChild(*plan);
		auto &row_id_ref = invalidator.expressions[0]->Cast<BoundReferenceExpression>();

		REQUIRE(invalidator.table_oid == table_oid);
		REQUIRE(invalidator.mode == CacheInvalidatorMode::INVALIDATE);
		REQUIRE(invalidator.pre_insert_row_count == 0);
		REQUIRE(invalidator.expressions.size() == 1);
		REQUIRE(row_id_ref.index + 1 == invalidator.children[0]->GetColumnBindings().size());

		VerifyCopiedInvalidator(*con.context, *plan, {CacheInvalidatorMode::INVALIDATE, table_oid, 0, 0, 1});
		con.context->transaction.Commit();
	}

	SECTION("INSERT plans inject INSERT mode and capture the pre-insert row count") {
		auto plan = OptimizeQuery(con, "INSERT INTO target VALUES (4, 40)");
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_INSERT);

		auto &invalidator = GetOnlyInvalidatorChild(*plan);
		REQUIRE(invalidator.table_oid == table_oid);
		REQUIRE(invalidator.mode == CacheInvalidatorMode::INSERT);
		REQUIRE(invalidator.pre_insert_row_count == 3);
		REQUIRE(invalidator.row_id_column_index == 0);
		REQUIRE(invalidator.expressions.empty());

		VerifyCopiedInvalidator(*con.context, *plan, {CacheInvalidatorMode::INSERT, table_oid, 3, 0, 0});
		con.context->transaction.Commit();
	}

	SECTION("MERGE plans inject MERGE mode with row-id offset and pre-insert row count") {
		auto plan = OptimizeQuery(con, "MERGE INTO target AS target USING source ON target.id = source.id "
		                               "WHEN MATCHED THEN UPDATE SET val = source.val "
		                               "WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.val)");
		REQUIRE(plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO);

		auto &merge = plan->Cast<LogicalMergeInto>();
		auto &invalidator = GetOnlyInvalidatorChild(*plan);
		REQUIRE(invalidator.table_oid == table_oid);
		REQUIRE(invalidator.mode == CacheInvalidatorMode::MERGE);
		REQUIRE(invalidator.row_id_column_index == merge.row_id_start);
		REQUIRE(invalidator.pre_insert_row_count == 3);
		REQUIRE(invalidator.expressions.empty());

		VerifyCopiedInvalidator(*con.context, *plan,
		                        {CacheInvalidatorMode::MERGE, table_oid, 3, merge.row_id_start, 0});
		con.context->transaction.Commit();
	}
}

} // namespace duckdb
