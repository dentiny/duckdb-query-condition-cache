# Changelog

## 0.1.2

### Fixed

- Fix cache filter alignment across row groups ([#87]).

[#87]: https://github.com/dentiny/duckdb-query-condition-cache/pull/87

### Updated

- Upgrade DuckDB to v1.5.5.

- Rework HDFS benchmark suite with separate cold/warm baseline, build, and cache-hit modes ([#88]).

[#88]: https://github.com/dentiny/duckdb-query-condition-cache/pull/88

## 0.1.1

### Updated

- Upgrade DuckDB to v1.5.4

## v0.1.0

### Added

- Cache stats exposed via `condition_cache_stats` ([#79]).

[#79]: https://github.com/dentiny/duckdb-query-condition-cache/pull/79

### Updated

- Upgrade DuckDB to v1.5.3
