# Benchmark Scripts

Benchmark harnesses for the QueryConditionCache DuckDB extension. All scripts measure query execution time with and without the condition cache and output results as Markdown tables + PNG charts.

## Prerequisites

```bash
uv sync
```

The extension must be built first (`GEN=ninja make`). Scripts auto-detect the built extension at `build/release/extension/query_condition_cache/query_condition_cache.duckdb_extension`.

## Python Benchmarks

### `run_clickbench_benchmark.py`

Runs 43 ClickBench queries (Q00–Q42) on the `hits` table. Downloads ~15 GB of parquet data from ClickHouse on first run and caches it in `clickbench.duckdb`.

```bash
uv run python benchmark/run_clickbench_benchmark.py --repeat 5 --queries 1,3,5,10
```

Key flags: `--repeat N`, `--queries 1,3,5`, `--threads N`, `--memory-limit 4GB`, `--regenerate`, `--no-chart`

### `run_hdfs_log_benchmark.py`

Downloads the HDFS_v2 dataset from Zenodo (~71M rows from a 32-node cluster). Same story-based protocol as Spark but on a larger dataset:

- **Story 1** — Block Health Dashboard: `LIKE '%blk_%'` queries
- **Story 2** — SRE Replication Drill-Down: `addStoredBlock` analysis
- **Story 3** — Error Investigation: WARN/ERROR + Exception patterns

The HDFS harness requires a valid cached sudo credential to purge the OS page
cache before each sample. Refresh it before starting:

```bash
sudo -v
```

The HDFS harness runs exactly one page-cache/condition-cache mode per
invocation:

```text
cold-baseline
cold-build
cold-cache-hit
warm-baseline
warm-build
warm-cache-hit
```

For example:

```bash
uv run python benchmark/run_hdfs_log_benchmark.py \
  --mode warm-cache-hit \
  --repeat 5 \
  --stories 1
```

Every measured sample begins with an OS page-cache purge and a fresh DuckDB
connection. A warm mode additionally runs one untimed normal query with the
condition cache disabled. Build modes time the complete first query with the
condition cache enabled and no matching entry, including automatic cache
construction and execution of the original query. Cache-hit modes build the
entry outside the measured interval.

## Output

- Markdown results: `benchmark/<name>_r<repeat>.md`
- Charts: `benchmark/<name>_r<repeat>.png`
- Cached data: `benchmark/*.duckdb`
- Downloaded datasets: `benchmark/spark_data/`, `benchmark/hdfs_data/`
