#include "logical_cache_recorder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

namespace {

void ConvertColumnRefsToChunkRefs(unique_ptr<Expression> &expr, const vector<ColumnBinding> &bindings) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		for (idx_t i = 0; i < bindings.size(); ++i) {
			if (bindings[i] == colref.binding) {
				expr = make_uniq<BoundReferenceExpression>(colref.alias, colref.return_type, i);
				return;
			}
		}
		throw InternalException("Failed to bind cache recorder predicate column reference");
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ConvertColumnRefsToChunkRefs(child, bindings); });
}

} // namespace

LogicalCacheRecorder::LogicalCacheRecorder(idx_t table_oid_p, string canonical_key_p,
                                           unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p,
                                           string table_catalog_p, string table_schema_p, string table_name_p,
                                           unique_ptr<Expression> backfill_predicate_p,
                                           shared_ptr<ConditionCacheEntry> cache_entry_p,
                                           shared_ptr<ConditionCacheEntry> metadata_entry_p)
    : table_oid(table_oid_p), table_catalog(std::move(table_catalog_p)), table_schema(std::move(table_schema_p)),
      table_name(std::move(table_name_p)), canonical_key(std::move(canonical_key_p)),
      rowid_column_index(rowid_column_index_p), cache_entry(std::move(cache_entry_p)),
      metadata_entry(std::move(metadata_entry_p)) {
	expressions.push_back(std::move(bound_predicate_p));
	if (backfill_predicate_p) {
		expressions.push_back(std::move(backfill_predicate_p));
	}
}

PhysicalOperator &LogicalCacheRecorder::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);
	auto bound_predicate = std::move(expressions[0]);
	ConvertColumnRefsToChunkRefs(bound_predicate, children[0]->GetColumnBindings());
	unique_ptr<Expression> backfill_predicate;
	if (expressions.size() > 1) {
		backfill_predicate = std::move(expressions[1]);
	}
	auto &op =
	    planner.Make<PhysicalCacheRecorder>(table_oid, canonical_key, std::move(bound_predicate), rowid_column_index,
	                                        table_catalog, table_schema, table_name, std::move(backfill_predicate),
	                                        cache_entry, metadata_entry, child_plan.types, estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

vector<ColumnBinding> LogicalCacheRecorder::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCacheRecorder::ResolveTypes() {
	types = children[0]->types;
}

string LogicalCacheRecorder::GetExtensionName() const {
	return "query_condition_cache_recorder";
}

void LogicalCacheRecorder::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(400, "table_oid", table_oid);
	serializer.WriteProperty(401, "canonical_key", canonical_key);
	serializer.WriteProperty(402, "rowid_column_index", rowid_column_index);
	serializer.WritePropertyWithDefault(403, "expressions", expressions);
	serializer.WritePropertyWithDefault(404, "table_catalog", table_catalog);
	serializer.WritePropertyWithDefault(405, "table_schema", table_schema);
	serializer.WritePropertyWithDefault(406, "table_name", table_name);
}

namespace {

BoundStatement CacheRecorderBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                 SQLStatement &statement) {
	return BoundStatement();
}

} // namespace

CacheRecorderOperatorExtension::CacheRecorderOperatorExtension() {
	Bind = CacheRecorderBind;
}

string CacheRecorderOperatorExtension::GetName() {
	return "query_condition_cache_recorder";
}

unique_ptr<LogicalExtensionOperator> CacheRecorderOperatorExtension::Deserialize(Deserializer &deserializer) {
	auto oid = deserializer.ReadProperty<idx_t>(400, "table_oid");
	auto key = deserializer.ReadProperty<string>(401, "canonical_key");
	auto rowid_col = deserializer.ReadProperty<idx_t>(402, "rowid_column_index");
	auto exprs = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(403, "expressions");
	auto table_catalog = deserializer.ReadPropertyWithDefault<string>(404, "table_catalog");
	auto table_schema = deserializer.ReadPropertyWithDefault<string>(405, "table_schema");
	auto table_name = deserializer.ReadPropertyWithDefault<string>(406, "table_name");

	unique_ptr<Expression> bound_predicate;
	if (!exprs.empty()) {
		bound_predicate = std::move(exprs[0]);
	} else {
		bound_predicate = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	}
	unique_ptr<Expression> backfill_predicate;
	if (exprs.size() > 1) {
		backfill_predicate = std::move(exprs[1]);
	}
	return make_uniq<LogicalCacheRecorder>(oid, std::move(key), std::move(bound_predicate), rowid_col,
	                                       std::move(table_catalog), std::move(table_schema), std::move(table_name),
	                                       std::move(backfill_predicate));
}

} // namespace duckdb
