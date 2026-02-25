#include "query_condition_cache_state.hpp"

namespace duckdb {

// Thread-local build capture state
thread_local BuildCaptureState tl_build_capture;

// Thread-local pending cache injections (pre-optimize → post-optimize handoff)
thread_local unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> tl_lookup_pending;

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(const string &filter_key) {
	lock_guard<mutex> guard(cache_lock);
	auto it = entries.find(filter_key);
	if (it != entries.end()) {
		return it->second;
	}
	return nullptr;
}

vector<shared_ptr<ConditionCacheEntry>> ConditionCacheStore::LookupByTable(const string &table_name) {
	lock_guard<mutex> guard(cache_lock);
	vector<shared_ptr<ConditionCacheEntry>> result;
	for (auto &pair : entries) {
		if (pair.second->table_name == table_name) {
			result.push_back(pair.second);
		}
	}
	return result;
}

void ConditionCacheStore::Insert(shared_ptr<ConditionCacheEntry> entry) {
	lock_guard<mutex> guard(cache_lock);
	entries[entry->filter_key] = std::move(entry);
}

void ConditionCacheStore::Clear() {
	lock_guard<mutex> guard(cache_lock);
	entries.clear();
}

vector<shared_ptr<ConditionCacheEntry>> ConditionCacheStore::GetAll() {
	lock_guard<mutex> guard(cache_lock);
	vector<shared_ptr<ConditionCacheEntry>> result;
	result.reserve(entries.size());
	for (auto &pair : entries) {
		result.push_back(pair.second);
	}
	return result;
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb
