#include "query_condition_cache_state.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

// ------- ROW_GROUP_FILTER -------

RowGroupFilter::RowGroupFilter(const vector<idx_t> &qualifying_vectors) {
	for (const auto &vec_idx : qualifying_vectors) {
		matching_vectors.at(vec_idx / 64) |= (1ULL << (vec_idx % 64));
	}
}

void RowGroupFilter::SetVector(idx_t vector_index) {
	matching_vectors.at(vector_index / 64) |= (1ULL << (vector_index % 64));
}

bool RowGroupFilter::VectorHasRows(idx_t vector_index) const {
	return (matching_vectors.at(vector_index / 64) >> (vector_index % 64)) & 1ULL;
}

bool RowGroupFilter::IsEmpty() const {
	for (const auto &w : matching_vectors) {
		if (w != 0) {
			return false;
		}
	}
	return true;
}

// ------- CONDITION_CACHE_STORE -------

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(const CacheKey &key) {
	lock_guard<mutex> guard(cache_lock);
	auto it = entries.find(key);
	if (it != entries.end()) {
		return it->second;
	}
	return nullptr;
}

void ConditionCacheStore::Upsert(const CacheKey &key, shared_ptr<ConditionCacheEntry> entry) {
	if (!entry) {
		throw InvalidInputException("ConditionCacheStore::Upsert: entry must not be null");
	}
	lock_guard<mutex> guard(cache_lock);
	auto result = entries.emplace(key, entry);
	if (!result.second) {
		result.first->second = std::move(entry);
	}
}

void ConditionCacheStore::Clear() {
	lock_guard<mutex> guard(cache_lock);
	entries.clear();
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

idx_t ConditionCacheStore::RemoveRowGroupsForTable(idx_t table_oid, const unordered_set<idx_t> &row_group_indices) {
	lock_guard<mutex> guard(cache_lock);
	idx_t removed_count = 0;
	vector<CacheKey> keys_to_remove;
	for (auto &pair : entries) {
		if (pair.first.table_oid != table_oid) {
			continue;
		}
		auto &entry = pair.second;
		for (auto rg_idx : row_group_indices) {
			if (entry->bitvectors.erase(rg_idx) > 0) {
				++removed_count;
			}
		}
		if (entry->bitvectors.empty()) {
			keys_to_remove.push_back(pair.first);
		}
	}
	for (auto &key : keys_to_remove) {
		entries.erase(key);
	}
	return removed_count;
}

idx_t ConditionCacheStore::RemoveByTable(idx_t table_oid) {
	lock_guard<mutex> guard(cache_lock);
	idx_t removed_count = 0;
	vector<CacheKey> keys_to_remove;
	for (auto &pair : entries) {
		if (pair.first.table_oid == table_oid) {
			keys_to_remove.push_back(pair.first);
		}
	}
	for (auto &key : keys_to_remove) {
		entries.erase(key);
		++removed_count;
	}
	return removed_count;
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb
