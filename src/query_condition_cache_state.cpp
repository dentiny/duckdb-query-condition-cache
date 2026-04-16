#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

namespace {

vector<idx_t> GetRowGroupCounts(ClientContext &context, DuckTableEntry &table_entry) {
	auto partitions = table_entry.GetStorage().GetPartitionStats(context);
	vector<idx_t> counts;
	counts.reserve(partitions.size());
	for (const auto &partition : partitions) {
		counts.push_back(partition.count);
	}
	return counts;
}

bool LayoutIsAppendCompatible(const vector<idx_t> &old_counts, const vector<idx_t> &new_counts) {
	if (old_counts.empty()) {
		return true;
	}
	if (new_counts.size() < old_counts.size()) {
		return false;
	}
	if (old_counts.size() == 1) {
		return new_counts[0] >= old_counts[0];
	}
	for (idx_t i = 0; i + 1 < old_counts.size(); ++i) {
		if (new_counts[i] != old_counts[i]) {
			return false;
		}
	}
	return new_counts[old_counts.size() - 1] >= old_counts[old_counts.size() - 1];
}

} // namespace

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

void RowGroupFilter::MergeFrom(const RowGroupFilter &other) {
	for (idx_t i = 0; i < BITVECTOR_ARRAY_SIZE; ++i) {
		matching_vectors[i] |= other.matching_vectors[i];
	}
}

// ------- CONDITION_CACHE_ENTRY -------

optional_idx ConditionCacheEntry::GetEstimatedCacheMemory() const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	idx_t estimated_size = sizeof(ConditionCacheEntry);
	estimated_size += bitvectors.size() * (sizeof(idx_t) + sizeof(RowGroupFilter) + 32);
	return optional_idx(estimated_size);
}

CacheEntryStats ConditionCacheEntry::ComputeStats(idx_t total_rows) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	constexpr idx_t vectors_per_row_group = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;

	idx_t qualifying_vectors = 0;
	idx_t qualifying_row_groups = 0;
	for (const auto &[rg_idx, filter] : bitvectors) {
		if (!filter.IsEmpty()) {
			++qualifying_row_groups;
		}
		for (idx_t v = 0; v < vectors_per_row_group; ++v) {
			if (filter.VectorHasRows(v)) {
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
	idx_t total_row_groups = (total_rows + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;

	return CacheEntryStats {
	    .qualifying_vectors = qualifying_vectors,
	    .total_vectors = total_vectors,
	    .qualifying_row_groups = qualifying_row_groups,
	    .total_row_groups = total_row_groups,
	};
}

void ConditionCacheEntry::EnsureRowGroup(idx_t rg_idx) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	(void)bitvectors[rg_idx];
}

void ConditionCacheEntry::SetQualifyingVector(idx_t rg_idx, idx_t vec_idx) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	bitvectors[rg_idx].SetVector(vec_idx);
}

void ConditionCacheEntry::MergeFrom(const ConditionCacheEntry &other) {
	if (this == &other) {
		return;
	}
	vector<pair<idx_t, RowGroupFilter>> snapshot;
	{
		concurrency::lock_guard<concurrency::mutex> guard(other.lock);
		snapshot.reserve(other.bitvectors.size());
		for (const auto &kv : other.bitvectors) {
			snapshot.push_back(kv);
		}
	}
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (const auto &[rg_idx, filter] : snapshot) {
		bitvectors[rg_idx].MergeFrom(filter);
	}
}

bool ConditionCacheEntry::VectorPassesFilter(idx_t rg_idx, idx_t vec_idx) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	auto it = bitvectors.find(rg_idx);
	if (it == bitvectors.end()) {
		return true;
	}
	return it->second.VectorHasRows(vec_idx);
}

bool ConditionCacheEntry::StatisticsRangeIsAllEmptyCached(idx_t min_rg, idx_t max_rg) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (idx_t rg = min_rg; rg <= max_rg; ++rg) {
		auto it = bitvectors.find(rg);
		if (it == bitvectors.end() || !it->second.IsEmpty()) {
			return false;
		}
	}
	return true;
}

idx_t ConditionCacheEntry::RowGroupCount() const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	return bitvectors.size();
}

bool ConditionCacheEntry::HasRowGroup(idx_t rg_idx) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	return bitvectors.find(rg_idx) != bitvectors.end();
}

bool ConditionCacheEntry::RowGroupVectorHasQualifyingRows(idx_t rg_idx, idx_t vec_idx) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	auto it = bitvectors.find(rg_idx);
	if (it == bitvectors.end()) {
		return false;
	}
	return it->second.VectorHasRows(vec_idx);
}

bool ConditionCacheEntry::RowGroupIsCompletelyEmpty(idx_t rg_idx) const {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	auto it = bitvectors.find(rg_idx);
	if (it == bitvectors.end()) {
		return false;
	}
	return it->second.IsEmpty();
}

pair<idx_t, bool> ConditionCacheEntry::EraseRowGroups(const unordered_set<idx_t> &row_group_indices) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	idx_t removed = 0;
	for (auto rg_idx : row_group_indices) {
		removed += bitvectors.erase(rg_idx);
	}
	return {removed, bitvectors.empty()};
}

// ------- TABLE_FILTER_KEY_INDEX -------

void TableFilterKeyIndex::Add(const string &filter_key) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	filter_keys.insert(filter_key);
}

void TableFilterKeyIndex::Remove(const string &filter_key) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	ALWAYS_ASSERT(filter_keys.count(filter_key) > 0);
	filter_keys.erase(filter_key);
}

bool TableFilterKeyIndex::IsEmpty() {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	return filter_keys.empty();
}

unordered_set<string> TableFilterKeyIndex::Take() {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	return std::move(filter_keys);
}

// ------- CONDITION_CACHE_STORE -------

string ConditionCacheStore::MakeCacheKeyString(const CacheKey &key) {
	return StringUtil::Format("query_condition_cache_entry:%llu:%s", key.table_oid, key.filter_key);
}

string ConditionCacheStore::MakeFilterKeyIndexKey(idx_t table_oid) {
	return StringUtil::Format("query_condition_cache_filter_key_index:%llu", table_oid);
}

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(ClientContext &context, const CacheKey &key) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.Get<ConditionCacheEntry>(MakeCacheKeyString(key));
}

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(ClientContext &context, DuckTableEntry &table_entry,
                                                            const CacheKey &key) {
	auto current_layout_epoch = GetLayoutEpoch(context, table_entry);
	auto entry = Lookup(context, key);
	if (!entry || entry->layout_epoch == current_layout_epoch) {
		return entry;
	}

	auto &cache = ObjectCache::GetObjectCache(context);
	cache.Delete(MakeCacheKeyString(key));

	auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(key.table_oid));
	if (index) {
		index->Remove(key.filter_key);
		if (index->IsEmpty()) {
			cache.Delete(MakeFilterKeyIndexKey(key.table_oid));
			concurrency::lock_guard<concurrency::mutex> guard(lock);
			cached_table_oids.erase(key.table_oid);
		}
	}
	return nullptr;
}

void ConditionCacheStore::Upsert(ClientContext &context, const CacheKey &key, shared_ptr<ConditionCacheEntry> entry) {
	if (!entry) {
		throw InvalidInputException("ConditionCacheStore::Upsert: entry must not be null");
	}
	auto &cache = ObjectCache::GetObjectCache(context);
	cache.Put(MakeCacheKeyString(key), std::move(entry));

	auto index = cache.GetOrCreate<TableFilterKeyIndex>(MakeFilterKeyIndexKey(key.table_oid));
	index->Add(key.filter_key);

	concurrency::lock_guard<concurrency::mutex> guard(lock);
	cached_table_oids.insert(key.table_oid);
}

void ConditionCacheStore::Upsert(ClientContext &context, DuckTableEntry &table_entry, const CacheKey &key,
                                 shared_ptr<ConditionCacheEntry> entry) {
	entry->layout_epoch = GetLayoutEpoch(context, table_entry);
	Upsert(context, key, std::move(entry));
}

idx_t ConditionCacheStore::GetLayoutEpoch(ClientContext &context, DuckTableEntry &table_entry) {
	auto current_row_group_counts = GetRowGroupCounts(context, table_entry);

	concurrency::lock_guard<concurrency::mutex> guard(lock);
	auto &layout_state = table_layouts[table_entry.oid];
	if (!LayoutIsAppendCompatible(layout_state.row_group_counts, current_row_group_counts)) {
		layout_state.layout_epoch++;
	}
	layout_state.row_group_counts = std::move(current_row_group_counts);
	return layout_state.layout_epoch;
}

idx_t ConditionCacheStore::RemoveRowGroupsForTable(ClientContext &context, idx_t table_oid,
                                                   const unordered_set<idx_t> &row_group_indices) {
	auto &cache = ObjectCache::GetObjectCache(context);

	auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
	if (!index) {
		return 0;
	}

	auto filter_keys = index->Take();
	idx_t removed_count = 0;

	for (const auto &filter_key : filter_keys) {
		CacheKey key {table_oid, filter_key};
		string cache_key = MakeCacheKeyString(key);
		auto entry = cache.Get<ConditionCacheEntry>(cache_key);
		if (!entry) {
			continue;
		}
		auto erased = entry->EraseRowGroups(row_group_indices);
		removed_count += erased.first;
		if (erased.second) {
			cache.Delete(cache_key);
		} else {
			index->Add(filter_key);
		}
	}

	if (index->IsEmpty()) {
		cache.Delete(MakeFilterKeyIndexKey(table_oid));
		concurrency::lock_guard<concurrency::mutex> guard(lock);
		cached_table_oids.erase(table_oid);
	}

	return removed_count;
}

bool ConditionCacheStore::HasEntriesForTable(ClientContext &context, idx_t table_oid) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
	return index && !index->IsEmpty();
}

void ConditionCacheStore::ClearAll(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);

	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (auto table_oid : cached_table_oids) {
		auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
		if (!index) {
			continue;
		}
		auto filter_keys = index->Take();
		for (const auto &filter_key : filter_keys) {
			cache.Delete(MakeCacheKeyString(CacheKey {table_oid, filter_key}));
		}
		cache.Delete(MakeFilterKeyIndexKey(table_oid));
	}
	cached_table_oids.clear();
	table_layouts.clear();
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb
