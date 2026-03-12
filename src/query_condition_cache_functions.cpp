#include "query_condition_cache_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {
namespace {

// ------- condition_cache_build(table_name VARCHAR, predicate VARCHAR) -------

struct ConditionCacheBuildBindData : public TableFunctionData {
	string catalog;
	string schema;
	string table;
	string predicate_sql;
	idx_t table_oid;
	idx_t total_rows; // total number of rows in the table
};

struct ConditionCacheBuildState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> ConditionCacheBuildBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ConditionCacheBuildBindData>();
	result->predicate_sql = input.inputs[1].GetValue<string>();

	// Parse qualified table name (supports "table", "schema.table", "catalog.schema.table")
	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
	result->catalog = std::move(qname.catalog);
	result->schema = std::move(qname.schema);
	result->table = std::move(qname.name);

	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, result->catalog, result->schema, result->table);
	result->table_oid = table_entry.oid;
	result->total_rows = table_entry.GetStorage().GetTotalRows();

	names.emplace_back("status");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	return result;
}

unique_ptr<GlobalTableFunctionState> ConditionCacheBuildInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ConditionCacheBuildState>();
}

// Collect all column OIDs referenced by bound expressions (BoundReferenceExpression).
void CollectReferencedColumns(Expression &expr, unordered_set<column_t> &column_oids) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		column_oids.insert(ref.index);
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { CollectReferencedColumns(child, column_oids); });
}

// After binding, column indices refer to StorageOid which may not match the scan order.
// Remap them to the actual DataChunk column positions.
void RemapColumnIndices(Expression &expr, const unordered_map<column_t, idx_t> &mapping) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		auto it = mapping.find(ref.index);
		D_ASSERT(it != mapping.end());
		ref.index = it->second;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { RemapColumnIndices(child, mapping); });
}

struct ScanColumn {
	StorageIndex index;
	LogicalType type;
};

} // namespace

// Scan all rows, evaluate predicate, and build a ConditionCacheEntry.
// Modifies bound_expr in-place (remaps column indices to scan positions).
shared_ptr<ConditionCacheEntry> BuildCacheEntry(ClientContext &context, DuckTableEntry &table_entry,
                                                Expression &bound_expr) {
	auto &storage = table_entry.GetStorage();
	auto &columns = table_entry.GetColumns();

	// Only scan columns referenced by the predicate, plus rowid
	unordered_set<column_t> referenced_oids;
	CollectReferencedColumns(bound_expr, referenced_oids);

	vector<ScanColumn> scan_columns;
	// Maps storage column OID to scan position in DataChunk
	unordered_map<column_t, idx_t> storage_to_scan_idx;

	scan_columns.reserve(referenced_oids.size() + 1);

	idx_t scan_pos = 0;
	for (const auto &col : columns.Physical()) {
		if (referenced_oids.count(col.Oid()) == 0) {
			continue;
		}
		scan_columns.push_back({StorageIndex(col.Oid()), col.Type()});
		storage_to_scan_idx[col.Oid()] = scan_pos++;
	}
	const idx_t rowid_col_idx = scan_pos;
	scan_columns.push_back({StorageIndex(COLUMN_IDENTIFIER_ROW_ID), LogicalType {LogicalType::ROW_TYPE}});

	vector<StorageIndex> column_ids;
	vector<LogicalType> scan_types;
	column_ids.reserve(scan_columns.size());
	scan_types.reserve(scan_columns.size());
	for (const auto &sc : scan_columns) {
		column_ids.push_back(sc.index);
		scan_types.push_back(sc.type);
	}

	RemapColumnIndices(bound_expr, storage_to_scan_idx);

	// Direct table scan with predicate evaluation via ExpressionExecutor
	auto &transaction = DuckTransaction::Get(context, table_entry.ParentCatalog().GetAttached());
	TableScanState scan_state;
	storage.InitializeScan(context, transaction, scan_state, column_ids);

	// Scan and evaluate predicate
	ExpressionExecutor executor(context, bound_expr);

	auto entry = make_shared_ptr<ConditionCacheEntry>();

	while (true) {
		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), scan_types);
		storage.Scan(transaction, chunk, scan_state);
		if (chunk.size() == 0) {
			break;
		}

		SelectionVector sel(chunk.size());
		idx_t match_count = executor.SelectExpression(chunk, sel);

		auto &rowid_vec = chunk.data[rowid_col_idx];
		UnifiedVectorFormat rowid_data;
		rowid_vec.ToUnifiedFormat(chunk.size(), rowid_data);
		auto rowids = UnifiedVectorFormat::GetData<row_t>(rowid_data);

		for (idx_t idx = 0; idx < match_count; ++idx) {
			auto rowid_entry = rowid_data.sel->get_index(sel.get_index(idx));
			if (!rowid_data.validity.RowIsValid(rowid_entry)) {
				continue;
			}
			auto row_id = NumericCast<idx_t>(rowids[rowid_entry]);
			idx_t row_group_idx = row_id / DEFAULT_ROW_GROUP_SIZE;
			idx_t vector_idx = (row_id % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
			entry->bitvectors[row_group_idx].SetVector(vector_idx);
		}
	}

	return entry;
}

CacheEntryStats ComputeCacheEntryStats(const ConditionCacheEntry &entry, idx_t total_rows) {
	constexpr idx_t vectors_per_row_group = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;

	idx_t qualifying_vectors = 0;
	for (const auto &bitvector_pair : entry.bitvectors) {
		for (idx_t v = 0; v < vectors_per_row_group; ++v) {
			if (bitvector_pair.second.VectorHasRows(v)) {
				++qualifying_vectors;
			}
		}
	}

	idx_t full_row_groups = total_rows / DEFAULT_ROW_GROUP_SIZE;
	idx_t remaining_rows = total_rows % DEFAULT_ROW_GROUP_SIZE;
	idx_t total_vectors = full_row_groups * vectors_per_row_group;
	if (remaining_rows > 0) {
		total_vectors += (remaining_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	}

	idx_t qualifying_row_groups = entry.bitvectors.size();
	idx_t total_row_groups = (total_rows + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;

	CacheEntryStats stats;
	stats.qualifying_vectors = qualifying_vectors;
	stats.total_vectors = total_vectors;
	stats.qualifying_row_groups = qualifying_row_groups;
	stats.total_row_groups = total_row_groups;
	return stats;
}

void ConditionCacheBuildExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<ConditionCacheBuildState>();
	if (gstate.done) {
		return;
	}
	gstate.done = true;

	const auto &bind_data = data_p.bind_data->Cast<ConditionCacheBuildBindData>();

	auto &table_entry =
	    Catalog::GetEntry<DuckTableEntry>(context, bind_data.catalog, bind_data.schema, bind_data.table);

	// Parse predicate SQL and bind column references to table columns
	auto parsed_exprs = Parser::ParseExpressionList(bind_data.predicate_sql);
	if (parsed_exprs.empty()) {
		throw InvalidInputException("condition_cache_build: failed to parse predicate");
	}
	// Reject bare column references (e.g. predicate = 'val')
	if (parsed_exprs[0]->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		throw InvalidInputException(
		    "condition_cache_build: predicate must be a boolean expression, not a column reference");
	}

	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, bind_data.table, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);

	// CheckBinder always sets target_type = INTEGER, so re-cast to BOOLEAN for SelectExpression.
	bound_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

	auto entry = BuildCacheEntry(context, table_entry, *bound_expr);
	auto stats = ComputeCacheEntryStats(*entry, bind_data.total_rows);

	// Store in cache
	// TODO: Use canonical predicate key (sorted expressions) so that "a AND b" and "b AND a" hit same entry
	// TODO: Invalidate cache on table modification (INSERT/DELETE/UPDATE/VACUUM)
	CacheKey key {bind_data.table_oid, bind_data.predicate_sql};
	auto store = ConditionCacheStore::GetOrCreate(context);
	store->Upsert(key, std::move(entry));

	output.SetCardinality(1);
	output.data[0].SetValue(0, StringUtil::Format("Cache Built: %llu/%llu vectors, %llu/%llu row groups",
	                                              stats.qualifying_vectors, stats.total_vectors,
	                                              stats.qualifying_row_groups, stats.total_row_groups));
}

TableFunction ConditionCacheBuildFunction() {
	TableFunction func(
	    "condition_cache_build",
	    {LogicalType {LogicalTypeId::VARCHAR} /*table*/, LogicalType {LogicalTypeId::VARCHAR} /*predicate_sql*/},
	    ConditionCacheBuildExecute, ConditionCacheBuildBind, ConditionCacheBuildInit);
	return func;
}

// ------- condition_cache_info(table_name VARCHAR, predicate VARCHAR) -------
// Returns the number of cached row groups for a given (table, predicate) pair.
// Returns 0 if no cache entry exists.

struct ConditionCacheInfoBindData : public TableFunctionData {
	idx_t table_oid;
	string predicate_sql;
};

struct ConditionCacheInfoState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> ConditionCacheInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ConditionCacheInfoBindData>();
	result->predicate_sql = input.inputs[1].GetValue<string>();

	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);

	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, qname.catalog, qname.schema, qname.name);
	result->table_oid = table_entry.oid;

	names.emplace_back("cached_row_groups");
	return_types.emplace_back(LogicalType::INTEGER);

	return result;
}

unique_ptr<GlobalTableFunctionState> ConditionCacheInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ConditionCacheInfoState>();
}

void ConditionCacheInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<ConditionCacheInfoState>();
	if (gstate.done) {
		return;
	}
	gstate.done = true;

	const auto &bind_data = data_p.bind_data->Cast<ConditionCacheInfoBindData>();
	CacheKey key {bind_data.table_oid, bind_data.predicate_sql};
	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(key);

	output.SetCardinality(1);
	if (entry) {
		output.data[0].SetValue(0, Value::INTEGER(static_cast<int32_t>(entry->bitvectors.size())));
	} else {
		output.data[0].SetValue(0, Value::INTEGER(0));
	}
}

TableFunction ConditionCacheInfoFunction() {
	TableFunction func(
	    "condition_cache_info",
	    {LogicalType {LogicalTypeId::VARCHAR} /*table*/, LogicalType {LogicalTypeId::VARCHAR} /*predicate_sql*/},
	    ConditionCacheInfoExecute, ConditionCacheInfoBind, ConditionCacheInfoInit);
	return func;
}

} // namespace duckdb
