#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

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

namespace duckdb {
namespace {

// ------- condition_cache_build(table_name VARCHAR, predicate VARCHAR) -------

struct ConditionCacheBuildBindData : public TableFunctionData {
	string catalog;
	string schema;
	string table_name;
	string predicate_sql;
	idx_t table_oid;
	idx_t total_rows;
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
	result->catalog = qname.catalog;
	result->schema = qname.schema;
	result->table_name = qname.name;

	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, result->catalog, result->schema, result->table_name);
	result->table_oid = table_entry.oid;
	result->total_rows = table_entry.GetStorage().GetTotalRows();

	names.emplace_back("status");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	return result;
}

unique_ptr<GlobalTableFunctionState> ConditionCacheBuildInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ConditionCacheBuildState>();
}

// After binding, column indices refer to StorageOid which may not match the scan order.
// Remap them to the actual DataChunk column positions.
void RemapColumnIndices(Expression &expr, const unordered_map<idx_t, idx_t> &mapping) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		auto it = mapping.find(ref.index);
		if (it != mapping.end()) {
			ref.index = it->second;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		RemapColumnIndices(child, mapping);
	});
}

} // namespace

// Scan all rows, evaluate predicate, and build a ConditionCacheEntry.
// Modifies bound_expr in-place (remaps column indices to scan positions).
shared_ptr<ConditionCacheEntry> BuildCacheEntry(ClientContext &context, DuckTableEntry &table_entry,
                                                Expression &bound_expr, idx_t &total_qualifying_rows) {
	auto &storage = table_entry.GetStorage();
	auto &columns = table_entry.GetColumns();

	// Scan all physical columns (needed by the predicate expression) plus rowid
	vector<StorageIndex> column_ids;
	vector<LogicalType> scan_types;
	unordered_map<idx_t, idx_t> storage_to_scan_idx;

	idx_t scan_pos = 0;
	for (auto &col : columns.Physical()) {
		column_ids.push_back(StorageIndex(col.Oid()));
		scan_types.push_back(col.Type());
		storage_to_scan_idx[col.Oid()] = scan_pos++;
	}
	idx_t rowid_col_idx = scan_pos;
	column_ids.push_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
	scan_types.push_back(LogicalType::ROW_TYPE);

	RemapColumnIndices(bound_expr, storage_to_scan_idx);

	// Direct table scan with predicate evaluation via ExpressionExecutor
	auto &transaction = DuckTransaction::Get(context, table_entry.ParentCatalog().GetAttached());
	TableScanState scan_state;
	storage.InitializeScan(context, transaction, scan_state, column_ids);

	// Scan and evaluate predicate
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), scan_types);
	ExpressionExecutor executor(context, bound_expr);

	unordered_map<idx_t, unordered_set<idx_t>> row_group_vec_indices;
	total_qualifying_rows = 0;

	while (true) {
		chunk.Reset();
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

		for (idx_t i = 0; i < match_count; ++i) {
			auto rid_idx = rowid_data.sel->get_index(sel.get_index(i));
			if (!rowid_data.validity.RowIsValid(rid_idx)) {
				continue;
			}
			auto row_id = NumericCast<idx_t>(rowids[rid_idx]);
			idx_t row_group_idx = row_id / DEFAULT_ROW_GROUP_SIZE;
			idx_t vector_idx = (row_id % DEFAULT_ROW_GROUP_SIZE) / STANDARD_VECTOR_SIZE;
			row_group_vec_indices[row_group_idx].insert(vector_idx);
			++total_qualifying_rows;
		}
	}

	// Build cache entry from collected indices
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	for (const auto &pair : row_group_vec_indices) {
		vector<idx_t> indices(pair.second.begin(), pair.second.end());
		entry->bitvectors.emplace(pair.first, RowGroupFilter(indices));
	}
	return entry;
}

void ConditionCacheBuildExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<ConditionCacheBuildState>();
	if (gstate.done) {
		return;
	}
	gstate.done = true;

	const auto &bind_data = data_p.bind_data->Cast<ConditionCacheBuildBindData>();

	auto &table_entry =
	    Catalog::GetEntry<DuckTableEntry>(context, bind_data.catalog, bind_data.schema, bind_data.table_name);

	// Parse predicate SQL and bind column references to table columns
	auto parsed_exprs = Parser::ParseExpressionList(bind_data.predicate_sql);
	if (parsed_exprs.empty()) {
		throw InvalidInputException("condition_cache_build: failed to parse predicate");
	}
	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, bind_data.table_name, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);

	// SelectExpression requires BOOLEAN return type
	if (bound_expr->return_type.id() != LogicalTypeId::BOOLEAN) {
		bound_expr = BoundCastExpression::AddCastToType(context, std::move(bound_expr),
		                                               LogicalType {LogicalTypeId::BOOLEAN});
	}

	idx_t total_qualifying_rows = 0;
	auto entry = BuildCacheEntry(context, table_entry, *bound_expr, total_qualifying_rows);

	// Compute statistics
	constexpr idx_t vectors_per_row_group = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
	idx_t qualifying_row_groups = entry->bitvectors.size();
	idx_t total_row_groups = (bind_data.total_rows + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;

	idx_t qualifying_vectors = 0;
	for (const auto &bitvector_pair : entry->bitvectors) {
		for (idx_t v = 0; v < vectors_per_row_group; ++v) {
			if (bitvector_pair.second.VectorHasRows(v)) {
				++qualifying_vectors;
			}
		}
	}

	idx_t full_row_groups = bind_data.total_rows / DEFAULT_ROW_GROUP_SIZE;
	idx_t remaining_rows = bind_data.total_rows % DEFAULT_ROW_GROUP_SIZE;
	idx_t total_vectors = full_row_groups * vectors_per_row_group;
	if (remaining_rows > 0) {
		total_vectors += (remaining_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	}

	// Store in cache
	// TODO: Use canonical predicate key (sorted expressions) so that "a AND b" and "b AND a" hit same entry
	// TODO: Invalidate cache on table modification (INSERT/DELETE/UPDATE/VACUUM)
	CacheKey key {bind_data.table_oid, bind_data.predicate_sql};
	auto store = ConditionCacheStore::GetOrCreate(context);
	store->Upsert(key, std::move(entry));

	output.SetCardinality(1);
	output.data[0].SetValue(
	    0, StringUtil::Format("Cache Built: %llu qualifying rows, %llu/%llu vectors, %llu/%llu row groups",
	                          total_qualifying_rows, qualifying_vectors, total_vectors, qualifying_row_groups,
	                          total_row_groups));
}

TableFunction ConditionCacheBuildFunction() {
	TableFunction func("condition_cache_build",
	                   {LogicalType {LogicalTypeId::VARCHAR} /*table_name*/,
	                    LogicalType {LogicalTypeId::VARCHAR} /*predicate_sql*/},
	                   ConditionCacheBuildExecute, ConditionCacheBuildBind, ConditionCacheBuildInit);
	return func;
}

} // namespace duckdb
