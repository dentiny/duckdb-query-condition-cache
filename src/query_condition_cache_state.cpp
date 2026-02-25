#include "query_condition_cache_state.hpp"

namespace duckdb {

// ------- ROW_GROUP_BITVECTOR -------

bool RowGroupBitvector::VectorHasRows(idx_t vector_index) const {
	D_ASSERT(vector_index < VECTORS_PER_ROW_GROUP);
	return (words[vector_index / 64] >> (vector_index % 64)) & 1ULL;
}

void RowGroupBitvector::SetVector(idx_t vector_index) {
	D_ASSERT(vector_index < VECTORS_PER_ROW_GROUP);
	words[vector_index / 64] |= (1ULL << (vector_index % 64));
}

bool RowGroupBitvector::IsEmpty() const {
	for (auto &w : words) {
		if (w != 0) {
			return false;
		}
	}
	return true;
}

idx_t RowGroupBitvector::CountSetBits() const {
	idx_t count = 0;
	for (auto &w : words) {
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
	for (auto &pair : entries) {
		if (pair.second->table_oid == table_oid) {
			result.push_back(pair.second);
		}
	}
	return result;
}

void ConditionCacheStore::Insert(shared_ptr<ConditionCacheEntry> entry) {
	lock_guard<mutex> guard(cache_lock);
	auto result = entries.emplace(entry->filter_key, entry);
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
		for (auto &pair : entries) {
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
