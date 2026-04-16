#include "query_condition_cache_txn_state.hpp"

#include "query_condition_cache_state.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

void AddAppendRange(unordered_map<idx_t, unordered_set<idx_t>> &affected_row_groups, idx_t table_oid, idx_t start_row,
                    idx_t count) {
	if (count == 0) {
		return;
	}

	auto first_rg = start_row / DEFAULT_ROW_GROUP_SIZE;
	auto last_rg = (start_row + count - 1) / DEFAULT_ROW_GROUP_SIZE;
	auto &table_row_groups = affected_row_groups[table_oid];
	for (idx_t rg = first_rg; rg <= last_rg; ++rg) {
		table_row_groups.insert(rg);
	}
}

} // namespace

void ConditionCacheTxnState::RecordRowGroups(idx_t table_oid, const unordered_set<idx_t> &row_groups) {
	if (row_groups.empty()) {
		return;
	}

	lock_guard<mutex> guard(lock);
	auto &table_row_groups = affected_row_groups[table_oid];
	table_row_groups.insert(row_groups.begin(), row_groups.end());
}

void ConditionCacheTxnState::RegisterAppendTable(idx_t table_oid, const string &catalog, const string &schema,
                                                 const string &table) {
	lock_guard<mutex> guard(lock);
	append_table_oids[TableIdentity {catalog, schema, table}] = table_oid;
}

void ConditionCacheTxnState::TransactionBegin(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void ConditionCacheTxnState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	unordered_map<idx_t, unordered_set<idx_t>> pending_row_groups;
	unordered_map<TableIdentity, idx_t, TableIdentityHash> pending_append_table_oids;
	{
		lock_guard<mutex> guard(lock);
		pending_row_groups = std::move(affected_row_groups);
		pending_append_table_oids = std::move(append_table_oids);
		affected_row_groups.clear();
		append_table_oids.clear();
	}

	if (!pending_append_table_oids.empty()) {
		transaction.ScanCommittedAppends([&](const CommittedAppendInfo &append_info) {
			auto table_lookup = pending_append_table_oids.find(
			    TableIdentity {append_info.catalog, append_info.schema, append_info.table});
			if (table_lookup == pending_append_table_oids.end()) {
				return;
			}
			AddAppendRange(pending_row_groups, table_lookup->second, append_info.start_row, append_info.count);
		});
	}

	if (pending_row_groups.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	for (auto &entry : pending_row_groups) {
		if (entry.second.empty()) {
			continue;
		}
		store->RemoveRowGroupsForTable(context, entry.first, entry.second);
	}
}

void ConditionCacheTxnState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void ConditionCacheTxnState::TransactionRollback(MetaTransaction &transaction, ClientContext &context,
                                                 optional_ptr<ErrorData> error) {
	Clear();
}

void ConditionCacheTxnState::Clear() {
	lock_guard<mutex> guard(lock);
	affected_row_groups.clear();
	append_table_oids.clear();
}

} // namespace duckdb
