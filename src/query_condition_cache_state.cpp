#include "query_condition_cache_state.hpp"

namespace duckdb {

// ------- ROW_GROUP_BITVECTOR -------

bool RowGroupBitvector::VectorHasRows(idx_t vector_index) const {
	return (matching_vectors.at(vector_index / 64) >> (vector_index % 64)) & 1ULL;
}

void RowGroupBitvector::SetVector(idx_t vector_index) {
	matching_vectors.at(vector_index / 64) |= (1ULL << (vector_index % 64));
}

bool RowGroupBitvector::IsEmpty() const {
	for (const auto &w : matching_vectors) {
		if (w != 0) {
			return false;
		}
	}
	return true;
}

idx_t RowGroupBitvector::CountSetBits() const {
	idx_t count = 0;
	for (const auto &w : matching_vectors) {
		uint64_t v = w;
		while (v) {
			v &= v - 1;
			++count;
		}
	}
	return count;
}

// ------- CONDITION_CACHE_STORE -------

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(const string &filter_key) {
	lock_guard<mutex> guard(cache_lock);
	auto it = entries.find(filter_key);
	if (it != entries.end()) {
		return it->second;
	}
	return nullptr;
}

vector<shared_ptr<ConditionCacheEntry>> ConditionCacheStore::LookupByTable(idx_t table_oid) {
	lock_guard<mutex> guard(cache_lock);
	vector<shared_ptr<ConditionCacheEntry>> result;
	auto it = entries_by_table.find(table_oid);
	if (it != entries_by_table.end()) {
		result.reserve(it->second.size());
		for (const auto &key : it->second) {
			auto entry_it = entries.find(key);
			if (entry_it != entries.end()) {
				result.push_back(entry_it->second);
			}
		}
	}
	return result;
}

void ConditionCacheStore::Insert(const string &filter_key, shared_ptr<ConditionCacheEntry> entry) {
	lock_guard<mutex> guard(cache_lock);
	const auto table_oid = entry->table_oid;
	auto result = entries.emplace(filter_key, entry);
	if (!result.second) {
		result.first->second = std::move(entry);
	} else {
		entries_by_table[table_oid].push_back(filter_key);
	}
}

void ConditionCacheStore::Clear() {
	lock_guard<mutex> guard(cache_lock);
	entries.clear();
	entries_by_table.clear();
}

vector<shared_ptr<ConditionCacheEntry>> ConditionCacheStore::GetAll() {
	vector<shared_ptr<ConditionCacheEntry>> result;
	{
		lock_guard<mutex> guard(cache_lock);
		result.reserve(entries.size());
		for (const auto &pair : entries) {
			result.push_back(pair.second);
		}
	}
	return result;
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb
