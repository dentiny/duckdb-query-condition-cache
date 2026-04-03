# Benchmark Scripts

Benchmark harnesses for the QueryConditionCache DuckDB extension. All scripts measure query execution time with and without the condition cache and output results as Markdown tables + PNG charts.

## Prerequisites

```bash
uv sync
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

## Output

- Markdown results: `benchmark/<name>_r<repeat>.md`
- Charts: `benchmark/<name>_r<repeat>.png`
- Cached data: `benchmark/*.duckdb`
- Downloaded datasets: `benchmark/spark_data/`, `benchmark/hdfs_data/`
