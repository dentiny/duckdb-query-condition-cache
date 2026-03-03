#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {
namespace {

// ------- condition_cache_build(table_name VARCHAR, predicate VARCHAR) -------

struct ConditionCacheBuildBindData : public TableFunctionData {
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
	result->table_name = input.inputs[0].GetValue<string>();
	result->predicate_sql = input.inputs[1].GetValue<string>();

	// Resolve table to get OID and total rows; throws if table doesn't exist
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, result->table_name);
	result->table_oid = table_entry.oid;
	result->total_rows = table_entry.GetStorage().GetTotalRows();

	names.emplace_back("status");
	return_types.emplace_back(LogicalType {LogicalTypeId::VARCHAR});

	return result;
}

unique_ptr<GlobalTableFunctionState> ConditionCacheBuildInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ConditionCacheBuildState>();
}

void ConditionCacheBuildExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<ConditionCacheBuildState>();
	if (gstate.done) {
		return;
	}
	gstate.done = true;

	const auto &bind_data = data_p.bind_data->Cast<ConditionCacheBuildBindData>();

	// Run SELECT rowid on a new connection
	string quoted_table = KeywordHelper::WriteQuoted(bind_data.table_name, '"');
	string query = StringUtil::Format("SELECT rowid FROM %s WHERE %s", quoted_table, bind_data.predicate_sql);
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	auto result = con.Query(query);

	if (result->HasError()) {
		throw InvalidInputException("condition_cache_build: query failed: %s", result->GetError());
	}

	// Collect qualifying vector indices per row group
	unordered_map<idx_t, unordered_set<idx_t>> row_group_vec_indices;
	idx_t total_qualifying_rows = 0;

	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}

		auto &rowid_vec = chunk->data[0];
		UnifiedVectorFormat vector_data;
		rowid_vec.ToUnifiedFormat(chunk->size(), vector_data);
		auto rowids = UnifiedVectorFormat::GetData<int64_t>(vector_data);

		for (idx_t i = 0; i < chunk->size(); ++i) {
			auto idx = vector_data.sel->get_index(i);
			if (!vector_data.validity.RowIsValid(idx)) {
				continue;
			}
			auto row_id = NumericCast<idx_t>(rowids[idx]);
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

	// Compute statistics
	constexpr idx_t vectors_per_row_group = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
	idx_t qualifying_row_groups = row_group_vec_indices.size();
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
	// TODO: Invalidate cache on table modification (INSERT/DELETE/UPDATE/VACUUM), need to implement invalidation
	// mechanism
	CacheKey key {bind_data.table_oid, bind_data.predicate_sql};
	auto store = ConditionCacheStore::GetOrCreate(context);
	store->Upsert(key, std::move(entry));

	output.SetCardinality(1);
	output.data[0].SetValue(
	    0, StringUtil::Format("Cache Built: %llu qualifying rows, %llu/%llu vectors, %llu/%llu row groups",
	                          total_qualifying_rows, qualifying_vectors, total_vectors, qualifying_row_groups,
	                          total_row_groups));
}

} // namespace

TableFunction ConditionCacheBuildFunction() {
	TableFunction func("condition_cache_build",
	                   {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                   ConditionCacheBuildExecute, ConditionCacheBuildBind, ConditionCacheBuildInit);
	return func;
}

} // namespace duckdb
