#include "query_condition_cache_optimizer.hpp"

#include "logical_cache_building_filter.hpp"
#include "query_condition_cache_filter.hpp"
#include "predicate_key_utils.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
	optimize_function = OptimizeFunction;
}

namespace {

// Rewrite BoundColumnRefExpression -> BoundReferenceExpression(storage OID),
// the stable shape shared with the table-function build path. The source
// column_index is a position into LogicalGet::column_ids.
void ConvertColumnRefsToScanRefs(unique_ptr<Expression> &expr, const LogicalGet &get) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		auto &column_ids = get.GetColumnIds();
		auto col_idx = colref.binding.column_index;
		ALWAYS_ASSERT(col_idx < column_ids.size());
		auto storage_oid = column_ids[col_idx].GetPrimaryIndex();
		expr = make_uniq<BoundReferenceExpression>(colref.alias, colref.return_type, storage_oid);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ConvertColumnRefsToScanRefs(child, get); });
}

string ComputeCanonicalPredicateKeyForGet(const vector<unique_ptr<Expression>> &expressions, const LogicalGet &get) {
	if (expressions.empty()) {
		return "";
	}

	vector<unique_ptr<Expression>> cloned;
	cloned.reserve(expressions.size());
	for (const auto &expr : expressions) {
		auto copy = expr->Copy();
		ConvertColumnRefsToScanRefs(copy, get);
		cloned.push_back(std::move(copy));
	}

	return ComputeCanonicalPredicateKey(cloned);
}

bool ContainsParameter(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_PARAMETER) {
		return true;
	}

	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (ContainsParameter(child)) {
			found = true;
		}
	});
	return found;
}

bool ContainsParameter(const vector<unique_ptr<Expression>> &expressions) {
	for (const auto &expr : expressions) {
		if (ContainsParameter(*expr)) {
			return true;
		}
	}
	return false;
}

bool CanAnyExpressionThrow(const vector<unique_ptr<Expression>> &expressions) {
	for (const auto &expr : expressions) {
		if (expr->CanThrow()) {
			return true;
		}
	}
	return false;
}

bool IsNestedType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::VARIANT:
		return true;
	default:
		return false;
	}
}

bool ReferencesNestedColumn(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return IsNestedType(expr.Cast<BoundColumnRefExpression>().return_type);
	}

	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (ReferencesNestedColumn(child)) {
			found = true;
		}
	});
	return found;
}

bool ReferencesNestedColumn(const vector<unique_ptr<Expression>> &expressions) {
	for (const auto &expr : expressions) {
		if (ReferencesNestedColumn(*expr)) {
			return true;
		}
	}
	return false;
}

bool IsTruncatingOperator(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_LIMIT || type == LogicalOperatorType::LOGICAL_TOP_N ||
	       type == LogicalOperatorType::LOGICAL_SAMPLE;
}

} // namespace

bool QueryConditionCacheOptimizer::IsSettingEnabled(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("use_query_condition_cache", val);
	if (!result) {
		return false;
	}
	return val.GetValue<bool>();
}

// Pre-optimize runs BEFORE built-in passes (including FilterPushdown).
// The full WHERE clause is still in LogicalFilter.expressions, so we can
// compute a canonical key that covers the entire predicate.
void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}
	auto query_state =
	    input.context.registered_state->GetOrCreate<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	query_state->cache_candidates.clear();
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, /*inside_truncating=*/false, *query_state);
	} catch (...) {
		// Defense in depth: skip cache optimization rather than failing the query.
		query_state->cache_candidates.clear();
	}
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, bool inside_truncating,
                                                   CacheOptimizerQueryState &state) {
	if (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		return;
	}

	// Skip cache building inside DML subplans
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;
	bool child_inside_truncating = inside_truncating || IsTruncatingOperator(plan->type);

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml, child_inside_truncating, state);
	}

	if (inside_dml) {
		return;
	}

	// Only handle direct table scans with a filter (LogicalFilter -> LogicalGet).
	// Joins, subqueries, and other patterns are not supported currently.
	if (plan->type != LogicalOperatorType::LOGICAL_FILTER || plan->children.empty()) {
		return;
	}
	if (plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &filter = plan->Cast<LogicalFilter>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		return; // not a DuckDB table (e.g. system table, external)
	}
	if (filter.expressions.empty()) {
		return;
	}
	if (ContainsParameter(filter.expressions)) {
		return;
	}
	if (CanAnyExpressionThrow(filter.expressions)) {
		return;
	}
	if (ReferencesNestedColumn(filter.expressions)) {
		return;
	}

	auto &duck_table = table->Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Skip caching on indexed tables: our extra ROW_ID filter would force
	// filter_set.filters.size() > 1, disabling the ART index scan path.
	if (storage.HasIndexes()) {
		return;
	}

	CacheKey key {table->oid, ComputeCanonicalPredicateKeyForGet(filter.expressions, get)};
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);
	store->RecordAccess(entry != nullptr);

	CacheOptimizerCandidate candidate {std::move(key), std::move(entry), /*cache_hit=*/entry != nullptr,
	                                   /*allow_finalize_backfill=*/!inside_truncating};
	if (candidate.cache_hit && candidate.entry && !candidate.entry->NeedsObservation(storage.GetTotalRows())) {
		state.cache_candidates[get.table_index] = std::move(candidate);
		return;
	}
	if (TryInstallFusedRecorder(context, plan, candidate)) {
		return;
	}

	if (candidate.cache_hit && candidate.entry) {
		state.cache_candidates[get.table_index] = std::move(candidate);
	}
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Clone the plan's already-bound filter expressions and remap column refs
	// to storage OIDs.
	vector<unique_ptr<Expression>> cloned;
	cloned.reserve(expressions.size());
	for (const auto &expr : expressions) {
		auto copy = expr->Copy();
		ConvertColumnRefsToScanRefs(copy, get);
		cloned.push_back(std::move(copy));
	}

	auto predicate = CombineWithAnd(std::move(cloned));
	predicate = BoundCastExpression::AddCastToType(context, std::move(predicate), LogicalType {LogicalTypeId::BOOLEAN});

	return BuildCacheEntry(context, table_entry, *predicate);
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                    CacheOptimizerQueryState &state) {
	if (plan->type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		return;
	}

	for (auto &child : plan->children) {
		PostOptimizeWalk(context, child, state);
	}

	if (plan->type == LogicalOperatorType::LOGICAL_FILTER && !plan->children.empty() &&
	    plan->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = plan->children[0]->Cast<LogicalGet>();
		auto candidate = state.cache_candidates.find(get.table_index);
		if (candidate != state.cache_candidates.end()) {
			if (candidate->second.cache_hit && candidate->second.entry) {
				InjectCacheFilter(context, get, candidate->second.entry);
				state.cache_candidates.erase(candidate);
			} else if (TryInstallFusedRecorder(context, plan, candidate->second)) {
				state.cache_candidates.erase(candidate);
			}
			return;
		}
	}

	if (plan->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = plan->Cast<LogicalGet>();
	auto entry = state.cache_candidates.find(get.table_index);
	if (entry == state.cache_candidates.end()) {
		return;
	}

	if (entry->second.cache_hit && entry->second.entry) {
		InjectCacheFilter(context, get, entry->second.entry);
	}
	state.cache_candidates.erase(entry);
}

void QueryConditionCacheOptimizer::InjectCacheFilter(ClientContext &context, LogicalGet &get,
                                                     const shared_ptr<ConditionCacheEntry> &entry) {
	auto &column_ids = get.GetMutableColumnIds();
	bool has_row_id = false;
	for (const auto &column_id : column_ids) {
		if (column_id.IsRowIdColumn()) {
			has_row_id = true;
			break;
		}
	}
	if (!has_row_id) {
		if (get.projection_ids.empty() && !column_ids.empty()) {
			get.projection_ids.reserve(column_ids.size());
			for (idx_t i = 0; i < column_ids.size(); i++) {
				get.projection_ids.push_back(i);
			}
		}
		column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0U));

	auto filter_expr =
	    make_uniq<BoundFunctionExpression>(LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFunction(),
	                                       std::move(children), make_uniq<ConditionCacheFilterBindData>(entry));

	get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID),
	                             make_uniq<CacheExpressionFilter>(std::move(filter_expr), entry));
}

namespace {

bool TryGetProjectedRowIdIndex(const LogicalGet &get, idx_t &row_id_column_index) {
	auto &column_ids = get.GetColumnIds();
	if (get.projection_ids.empty()) {
		for (idx_t column_idx = 0; column_idx < column_ids.size(); ++column_idx) {
			if (column_ids[column_idx].IsRowIdColumn()) {
				row_id_column_index = column_idx;
				return true;
			}
		}
		return false;
	}
	for (idx_t projected_idx = 0; projected_idx < get.projection_ids.size(); ++projected_idx) {
		auto projection_id = get.projection_ids[projected_idx];
		if (column_ids[projection_id].IsRowIdColumn()) {
			row_id_column_index = projected_idx;
			return true;
		}
	}
	return false;
}

idx_t EnsureHiddenRowIdProjected(LogicalGet &get) {
	auto &column_ids = get.GetMutableColumnIds();
	get.virtual_columns.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE)));

	const idx_t original_column_count = column_ids.size();
	if (get.projection_ids.empty()) {
		get.projection_ids.reserve(original_column_count + 1);
		for (idx_t i = 0; i < original_column_count; ++i) {
			get.projection_ids.push_back(i);
		}
	}

	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	const idx_t row_id_column_index = column_ids.size() - 1;
	get.projection_ids.push_back(row_id_column_index);
	return get.projection_ids.size() - 1;
}

} // namespace

bool QueryConditionCacheOptimizer::TryInstallFusedRecorder(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                           CacheOptimizerCandidate &candidate) {
	auto &filter = plan->Cast<LogicalFilter>();
	auto &get = filter.children[0]->Cast<LogicalGet>();

	if (filter.expressions.empty() || filter.HasProjectionMap()) {
		return false;
	}
	if (!get.table_filters.filters.empty()) {
		return false;
	}
	auto table = get.GetTable();
	if (!table) {
		return false;
	}
	auto &duck_table = table->Cast<DuckTableEntry>();
	if (duck_table.GetStorage().HasIndexes()) {
		return false;
	}

	unique_ptr<Expression> backfill_predicate;
	if (candidate.allow_finalize_backfill) {
		vector<unique_ptr<Expression>> backfill_expressions;
		backfill_expressions.reserve(filter.expressions.size());
		for (const auto &expr : filter.expressions) {
			auto copy = expr->Copy();
			ConvertColumnRefsToScanRefs(copy, get);
			backfill_expressions.push_back(std::move(copy));
		}
		backfill_predicate = CombineWithAnd(std::move(backfill_expressions));
		backfill_predicate = BoundCastExpression::AddCastToType(context, std::move(backfill_predicate),
		                                                        LogicalType {LogicalTypeId::BOOLEAN});
	}

	auto entry = candidate.entry;
	if (!entry) {
		entry = make_shared_ptr<ConditionCacheEntry>();
		auto store = ConditionCacheStore::GetOrCreate(context);
		store->Upsert(context, candidate.key, entry);
		candidate.entry = entry;
	}

	idx_t row_id_column_index;
	bool hide_row_id_column = false;
	if (!TryGetProjectedRowIdIndex(get, row_id_column_index)) {
		row_id_column_index = EnsureHiddenRowIdProjected(get);
		hide_row_id_column = true;
	}
	InjectCacheFilter(context, get, entry);
	auto row_id_binding_index = get.projection_ids.empty() ? row_id_column_index : get.projection_ids[row_id_column_index];
	auto row_id_binding = ColumnBinding(get.table_index, row_id_binding_index);
	auto row_id_reference = make_uniq<BoundColumnRefExpression>("rowid", LogicalType::ROW_TYPE, row_id_binding);
	auto child = std::move(filter.children[0]);
	auto expressions = std::move(filter.expressions);
	auto fused = make_uniq<LogicalCacheBuildingFilter>(std::move(expressions), std::move(row_id_reference),
	                                                   row_id_column_index, hide_row_id_column, entry,
	                                                   table->ParentCatalog().GetName(), table->ParentSchema().name,
	                                                   table->name, std::move(backfill_predicate),
	                                                   candidate.allow_finalize_backfill);
	fused->estimated_cardinality = filter.estimated_cardinality;
	fused->children.push_back(std::move(child));
	plan = std::move(fused);
	return true;
}

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}

	auto query_state = input.context.registered_state->Get<CacheOptimizerQueryState>(CacheOptimizerQueryState::NAME);
	if (!query_state || query_state->cache_candidates.empty()) {
		return;
	}

	PostOptimizeWalk(input.context, plan, *query_state);
}

} // namespace duckdb
