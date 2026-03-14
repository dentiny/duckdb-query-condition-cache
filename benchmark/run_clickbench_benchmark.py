#!/usr/bin/env python3
"""
ClickBench Benchmark Harness for QueryConditionCache extension.

Measures query execution time with and without the condition cache, using the
same protocol as run_tpch_benchmark.py.

Per-query protocol (all runs in the same DuckDB session):
  Run 1 – cache OFF, OS cold  : cold baseline (reference only)
  Run 2 – cache OFF, OS warm  : true baseline (warm OS, no condition cache)
  Run 3 – cache ON,  first run : cache build (first query with cache enabled)
  Run 4 – cache ON,  repeated  : cache hit   (warm OS + warm condition cache)

  Speedup = Run 2 / Run 4

Usage:
    python benchmark/run_clickbench_benchmark.py [--repeat N] [--out FILE] [--queries 1,3,5]
"""

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

WORKSPACE = Path(__file__).parent.parent

# The duckdb/ directory (C++ submodule) shadows the duckdb Python package.
_submodule = str(WORKSPACE / "duckdb")
_root = str(WORKSPACE)
sys.path = [p for p in sys.path if p not in (_submodule, _root)]

EXT_PATH = (
    WORKSPACE
    / "build"
    / "release"
    / "extension"
    / "query_condition_cache"
    / "query_condition_cache.duckdb_extension"
)

CLICKBENCH_QUERIES_DIR = WORKSPACE / "duckdb" / "benchmark" / "clickbench" / "queries"
LOAD_SQL = CLICKBENCH_QUERIES_DIR / "load.sql"
NUM_QUERIES = 43  # q00 through q42


_PARQUET_URL_TEMPLATE = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet"
_NUM_PARQUET_FILES = 100
_MAX_RETRIES = 3

# CREATE TABLE portion of load.sql (without the INSERT)
_CREATE_HITS_SQL = LOAD_SQL.read_text().split(";")[0] + ";"

_INSERT_SINGLE_SQL = """
INSERT INTO hits BY NAME
SELECT *
    REPLACE (
        make_date(EventDate) AS EventDate,
        epoch_ms(EventTime * 1000) AS EventTime,
        epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
FROM read_parquet('{url}', binary_as_string=True);
"""


def _load_clickbench_data(con):
    """Load ClickBench data one parquet file at a time with retries."""
    con.execute(_CREATE_HITS_SQL)
    for i in range(_NUM_PARQUET_FILES):
        url = _PARQUET_URL_TEMPLATE.format(i)
        sql = _INSERT_SINGLE_SQL.format(url=url)
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                con.execute(sql)
                print(f"  [{i + 1}/{_NUM_PARQUET_FILES}] hits_{i}.parquet loaded", flush=True)
                break
            except Exception as e:
                if attempt < _MAX_RETRIES:
                    wait = attempt * 5
                    print(f"  [{i + 1}/{_NUM_PARQUET_FILES}] hits_{i}.parquet attempt {attempt} failed: {e}. Retrying in {wait}s...", flush=True)
                    time.sleep(wait)
                else:
                    print(f"  [{i + 1}/{_NUM_PARQUET_FILES}] hits_{i}.parquet FAILED after {_MAX_RETRIES} attempts", flush=True)
                    raise


def load_queries() -> list[tuple[str, str]]:
    """Load all ClickBench queries from .sql files. Returns list of (label, sql)."""
    queries = []
    for i in range(NUM_QUERIES):
        path = CLICKBENCH_QUERIES_DIR / f"q{i:02d}.sql"
        if path.exists():
            sql = path.read_text().strip()
            queries.append((f"Q{i:02d}", sql))
    return queries


def get_duckdb_connection(args):
    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb Python package not found. Run via: uv run benchmark/run_clickbench_benchmark.py")
        sys.exit(1)

    cfg: dict = {"allow_unsigned_extensions": True}
    if args.threads:
        cfg["threads"] = args.threads
    if args.memory_limit:
        cfg["memory_limit"] = args.memory_limit

    db_path = WORKSPACE / "benchmark" / "clickbench.duckdb"
    if args.regenerate and db_path.exists():
        db_path.unlink()
        wal = db_path.with_suffix(".duckdb.wal")
        if wal.exists():
            wal.unlink()
    needs_generate = not db_path.exists()

    if needs_generate:
        print(f"Loading ClickBench data into {db_path}...")
        print("This downloads ~15 GB of parquet data from the ClickHouse dataset. May take a while.", flush=True)
        gen_con = duckdb.connect(str(db_path), config={"allow_unsigned_extensions": True})
        if EXT_PATH.exists():
            gen_con.execute(f"LOAD '{EXT_PATH}';")
        else:
            gen_con.execute("LOAD query_condition_cache;")

        try:
            _load_clickbench_data(gen_con)
            gen_con.execute("CHECKPOINT;")
            gen_con.close()
            print("Data loaded and cached on disk.", flush=True)
        except Exception:
            gen_con.close()
            # Remove the partial/corrupt database so next run retries cleanly
            if db_path.exists():
                db_path.unlink()
            wal = db_path.with_suffix(".duckdb.wal")
            if wal.exists():
                wal.unlink()
            raise
    else:
        print(f"Reusing cached ClickBench data from {db_path}", flush=True)

    con = duckdb.connect(str(db_path), config=cfg)

    if EXT_PATH.exists():
        con.execute(f"LOAD '{EXT_PATH}';")
    else:
        print(f"WARNING: Extension not found at {EXT_PATH}. Build with: GEN=ninja make")
        con.execute("LOAD query_condition_cache;")

    print("Data ready.", flush=True)
    return con


def time_query(con, sql: str, repeat: int) -> list[float]:
    """Run sql `repeat` times and return list of elapsed times in ms."""
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    return times


def benchmark_query(con, label: str, sql: str, repeat: int) -> dict:
    """Run the 4-step protocol and return timing results."""
    # Step 1 & 2: cache OFF
    con.execute("SET use_query_condition_cache = false;")
    t_cold = time_query(con, sql, 1)[0]
    t_warm_no_cache = time_query(con, sql, repeat)

    # Step 3 & 4: cache ON
    con.execute("SET use_query_condition_cache = true;")
    t_build = time_query(con, sql, 1)[0]
    t_cache_hit = time_query(con, sql, repeat)

    con.execute("SET use_query_condition_cache = false;")

    baseline = statistics.median(t_warm_no_cache)
    cache_hit = statistics.median(t_cache_hit)
    speedup = baseline / cache_hit if cache_hit > 0 else float("inf")

    return {
        "label": label,
        "cold_ms": t_cold,
        "baseline_ms": baseline,
        "build_ms": t_build,
        "cache_hit_ms": cache_hit,
        "speedup": speedup,
    }


def format_table(title: str, results: list[dict], repeat: int) -> str:
    lines = [
        f"## {title} (median of {repeat} warm runs)\n",
        "| Query | Cold OS (ms) | Warm OS, no cache (ms) | Cache Build (ms) | Cache Hit (ms) | Speedup |",
        "|-------|--------------|------------------------|------------------|----------------|---------|",
    ]
    for r in results:
        lines.append(
            f"| {r['label']:<5} "
            f"| {r['cold_ms']:>12.0f} "
            f"| {r['baseline_ms']:>22.0f} "
            f"| {r['build_ms']:>16.0f} "
            f"| {r['cache_hit_ms']:>14.0f} "
            f"| {r['speedup']:>6.2f}x |"
        )
    lines.append("")
    return "\n".join(lines)


def plot_results(results: list[dict], out_path: Path):
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib/numpy not available, skipping chart generation.")
        return

    fig, ax = plt.subplots(figsize=(16, 6))
    fig.suptitle("QueryConditionCache Speedup — ClickBench", fontsize=14, fontweight="bold")

    labels = [r["label"] for r in results]
    speedups = [r["speedup"] for r in results]
    colors = ["#2196F3" if s >= 1.05 else "#9E9E9E" for s in speedups]

    x = np.arange(len(labels))
    bars = ax.bar(x, speedups, color=colors)
    ax.axhline(y=1.0, color="red", linestyle="--", linewidth=1, alpha=0.7, label="baseline (1x)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Speedup (higher is better)")
    ax.set_title("ClickBench Queries (Q00–Q42)")
    ax.legend()

    for bar, speed in zip(bars, speedups):
        if speed >= 1.05:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.05,
                f"{speed:.1f}x",
                ha="center",
                va="bottom",
                fontsize=7,
                fontweight="bold",
            )

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {out_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="QueryConditionCache ClickBench benchmark")
    parser.add_argument(
        "--repeat", type=int, default=3,
        help="Number of warm runs per phase for median (default: 3)",
    )
    parser.add_argument(
        "--out", type=str, default=None,
        help="Output file for results (default: benchmark/results_clickbench.md)",
    )
    parser.add_argument(
        "--queries", type=str, default=None,
        help="Comma-separated query numbers to run (e.g. '1,3,5,10'). Default: all.",
    )
    parser.add_argument("--no-chart", action="store_true", help="Skip chart generation")
    parser.add_argument(
        "--regenerate", action="store_true",
        help="Force re-download/reload of ClickBench data",
    )
    parser.add_argument(
        "--threads", type=int, default=None,
        help="Number of DuckDB threads (default: auto)",
    )
    parser.add_argument(
        "--memory-limit", type=str, default=None,
        help="DuckDB memory limit (e.g. '4GB')",
    )
    args = parser.parse_args()

    out_file = args.out or str(WORKSPACE / "benchmark" / "results_clickbench.md")

    con = get_duckdb_connection(args)

    all_queries = load_queries()

    # Filter to selected queries if specified
    if args.queries:
        selected = {int(q.strip()) for q in args.queries.split(",")}
        all_queries = [(label, sql) for label, sql in all_queries if int(label[1:]) in selected]

    print(f"\nRunning {len(all_queries)} ClickBench queries...", flush=True)
    results = []
    for label, sql in all_queries:
        print(f"  {label}...", end=" ", flush=True)
        try:
            result = benchmark_query(con, label, sql, args.repeat)
            results.append(result)
            print(
                f"baseline={result['baseline_ms']:.0f}ms  hit={result['cache_hit_ms']:.0f}ms  {result['speedup']:.2f}x",
                flush=True,
            )
        except Exception as e:
            print(f"FAILED: {e}", flush=True)

    mem_note = f", memory_limit={args.memory_limit}" if args.memory_limit else ""
    threads_note = f", threads={args.threads}" if args.threads else ""

    lines = [
        "# QueryConditionCache Benchmark Results — ClickBench\n",
        f"**Settings:**{threads_note}{mem_note}\n",
        f"**Methodology:** Each query runs {args.repeat + 2} times in one session.",
        "Run 1 (cold OS, cache off) warms the OS page cache and DuckDB buffer pool.",
        "Runs 2–N (warm OS, cache off) establish the true baseline.",
        "Cache is then enabled; first run builds the cache entry, subsequent runs measure cache hits.",
        "**Speedup = median(warm OS+buffer, no cache) / median(warm OS+buffer, cache hit)**\n",
    ]

    if results:
        lines.append(format_table("ClickBench Queries", results, args.repeat))

    output = "\n".join(lines)
    print("\n" + output)

    with open(out_file, "w") as f:
        f.write(output)
    print(f"\nResults written to {out_file}")

    if not args.no_chart and results:
        chart_path = Path(out_file).with_suffix(".png")
        plot_results(results, chart_path)

    con.close()


if __name__ == "__main__":
    main()
