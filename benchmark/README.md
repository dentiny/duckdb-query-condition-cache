# Benchmark Scripts

Benchmark harnesses for the QueryConditionCache DuckDB extension. All scripts measure query execution time with and without the condition cache and output results as Markdown tables + PNG charts.

## Prerequisites

```bash
pip install duckdb matplotlib numpy
```

The extension must be built first (`GEN=ninja make`). Scripts auto-detect the built extension at `build/release/extension/query_condition_cache/query_condition_cache.duckdb_extension`.

## Python Benchmarks

All Python scripts share the same measurement protocol:

1. **Cold run** (cache OFF) — warms OS page cache
2. **Warm baseline** (cache OFF, N repeats) — true baseline
3. **Cache build** (cache ON, first run) — builds the condition cache
4. **Cache hit** (cache ON, N repeats) — measures cached performance

**Speedup = avg(baseline) / avg(cache hit)**

OS page caches are dropped between phases (requires `sudo` for `purge`/`drop_caches`).

### `run_clickbench_benchmark.py`

Runs 43 ClickBench queries (Q00–Q42) on the `hits` table. Downloads ~15 GB of parquet data from ClickHouse on first run and caches it in `clickbench.duckdb`.

```bash
python benchmark/run_clickbench_benchmark.py --repeat 5 --queries 1,3,5,10
```

Key flags: `--repeat N`, `--queries 1,3,5`, `--threads N`, `--memory-limit 4GB`, `--regenerate`, `--no-chart`

### `run_tpch_benchmark.py`

Runs the 22 standard TPC-H queries plus 8 dashboard-style queries (LIKE, OR, mixed predicates) on `lineitem`. Generates TPC-H data via `CALL dbgen(sf=N)`.

```bash
python benchmark/run_tpch_benchmark.py --sf 10 --repeat 3
```

Key flags: `--sf N`, `--no-tpch`, `--no-dashboard`, `--threads N`, `--memory-limit 4GB`

### `run_spark_log_benchmark.py`

Downloads the Spark log dataset from Zenodo (loghub-2.0, ~33M rows) and benchmarks three "story" scenarios where queries within a story share cached predicates:

- **Story 1** — Multi-Widget Dashboard: 4 queries on `Content LIKE '%memory%'`
- **Story 2** — SRE Drill-Down: iterative refinement with LIKE on broadcast/memory
- **Story 3** — Pattern Matching: SecurityManager log analysis

```bash
python benchmark/run_spark_log_benchmark.py --repeat 5 --stories 1,2
```

Key flags: `--stories 1,2,3`, `--individual` (also run each query with a fresh connection)

### `run_hdfs_log_benchmark.py`

Downloads the HDFS_v2 dataset from Zenodo (~71M rows from a 32-node cluster). Same story-based protocol as Spark but on a larger dataset:

- **Story 1** — Block Health Dashboard: `LIKE '%blk_%'` queries
- **Story 2** — SRE Replication Drill-Down: `addStoredBlock` analysis
- **Story 3** — Error Investigation: WARN/ERROR + Exception patterns

```bash
python benchmark/run_hdfs_log_benchmark.py --repeat 5 --stories 1,2,3
```

## SQL Benchmarks

Run directly with the DuckDB CLI (extension must be pre-loaded):

```bash
./build/release/duckdb < benchmark/benchmark.sql
```

### `benchmark.sql`

Four synthetic benchmarks on 30M-row tables with 9 columns each:

1. **Compound LIKE OR** — two LIKE predicates joined by OR (non-pushable)
2. **Simple Integer OR** — integer comparisons under OR
3. **Mixed AND** — integer + LIKE combined with AND
4. **Single LIKE** — standalone LIKE (pushable predicate)

### `benchmark_like.sql`

Isolated single-LIKE benchmark (30M rows, pushable predicate). Targets the scenario where cache provides vector-skip on top of filter pushdown.

### `benchmark_like_or.sql`

Single LIKE with a dummy OR clause to prevent pushdown, forcing full decompression. Demonstrates maximum cache benefit.

## Output

- Markdown results: `benchmark/<name>_r<repeat>.md`
- Charts: `benchmark/<name>_r<repeat>.png`
- Cached data: `benchmark/*.duckdb`
- Downloaded datasets: `benchmark/spark_data/`, `benchmark/hdfs_data/`
