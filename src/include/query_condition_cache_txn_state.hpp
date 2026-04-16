#pragma once

#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

// Transaction-scoped replay of cache invalidation facts.
// Physical invalidators record affected row groups during statement execution,
// and TransactionCommit replays them together with exact append ranges from
// DuckTransaction::AppendInfo so caches built after a statement but before
// COMMIT are still invalidated correctly.
class ConditionCacheTxnState : public ClientContextState {
public:
	static constexpr const char *NAME = "query_condition_cache_txn_state";

	struct TableIdentity {
		string catalog;
		string schema;
		string table;

		bool operator==(const TableIdentity &other) const {
			return catalog == other.catalog && schema == other.schema && table == other.table;
		}
	};

	struct TableIdentityHash {
		uint64_t operator()(const TableIdentity &identity) const {
			auto hash = Hash(identity.catalog.c_str());
			hash = CombineHash(hash, Hash(identity.schema.c_str()));
			return CombineHash(hash, Hash(identity.table.c_str()));
		}
	};

	void RecordRowGroups(idx_t table_oid, const unordered_set<idx_t> &row_groups);
	void RegisterAppendTable(idx_t table_oid, const string &catalog, const string &schema, const string &table);

	void TransactionBegin(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                         optional_ptr<ErrorData> error) override;

private:
	void Clear();

	mutex lock;
	unordered_map<idx_t, unordered_set<idx_t>> affected_row_groups;
	unordered_map<TableIdentity, idx_t, TableIdentityHash> append_table_oids;
};

} // namespace duckdb
