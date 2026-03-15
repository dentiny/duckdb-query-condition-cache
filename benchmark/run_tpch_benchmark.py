#!/usr/bin/env python3
"""
TPC-H Benchmark Harness for QueryConditionCache extension.

Measures query execution time with and without the condition cache, using a
protocol that isolates cache benefit from OS page cache effects.

Per-query protocol (all runs in the same DuckDB session):
  Run 1 – cache OFF, OS cold  : cold baseline (reference only)
  Run 2 – cache OFF, OS warm  : true baseline (warm OS, no condition cache)
  Run 3 – cache ON,  OS warm  : cache build (first query with cache enabled)
  Run 4 – cache ON,  OS warm  : cache hit   (warm OS + warm condition cache)

  Speedup = Run 2 / Run 4

Runs 2 and 4 are each repeated REPEAT times; the median is used.

Usage:
    python benchmark/run_tpch_benchmark.py [--sf SF] [--repeat N] [--out FILE]

Requirements:
    pip install duckdb  (or use the duckdb package bundled with this project)
"""

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

WORKSPACE = Path(__file__).parent.parent

# The duckdb/ directory (C++ submodule) shadows the duckdb Python package.
# Remove it from sys.path so the installed package is found instead.
_submodule = str(WORKSPACE / "duckdb")
if _submodule in sys.path:
    sys.path.remove(_submodule)
# Also remove the workspace root if it causes the same issue
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

DASHBOARD_QUERIES = [
    (
        "D01: LIKE '%unusual%'",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_comment LIKE '%unusual%'",
    ),
    (
        "D02: OR shipmode IN ('AIR','AIR REG')",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_shipmode = 'AIR' OR l_shipmode = 'AIR REG'",
    ),
    (
        "D03: OR returnflag='R' OR linestatus='F'",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_returnflag = 'R' OR l_linestatus = 'F'",
    ),
    (
        "D04: discount>0.08 AND LIKE '%special%'",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_discount > 0.08 AND l_comment LIKE '%special%'",
    ),
    (
        "D05: OR LIKE '%unusual%' OR LIKE '%special%'",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_comment LIKE '%unusual%' OR l_comment LIKE '%special%'",
    ),
    (
        "D06: OR quantity<2 OR discount>0.09",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_quantity < 2 OR l_discount > 0.09",
    ),
    (
        "D07: LIKE '%PERSON%' on shipinstruct",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_shipinstruct LIKE '%PERSON%'",
    ),
    (
        "D08: OR 3-clause cross-column",
        "SELECT count(*), sum(l_extendedprice), sum(l_extendedprice*(1-l_discount)), sum(l_quantity) "
        "FROM lineitem WHERE l_shipmode = 'TRUCK' OR l_returnflag = 'A' OR l_linestatus = 'O'",
    ),
]


def get_duckdb_connection(sf: int, args=None):
    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb Python package not found. Run via: uv run benchmark/run_tpch_benchmark.py")
        sys.exit(1)

    # allow_unsigned_extensions is needed to load the locally-built .duckdb_extension binary
    cfg: dict = {"allow_unsigned_extensions": True}
    if args and args.threads:
        cfg["threads"] = args.threads
    if args and args.memory_limit:
        cfg["memory_limit"] = args.memory_limit

    # Cache TPC-H data on disk to avoid regenerating it each run
    db_path = WORKSPACE / "benchmark" / f"tpch_sf{sf}.duckdb"
    regenerate = args.regenerate if args else False
    if regenerate and db_path.exists():
        db_path.unlink()
        # Also remove WAL file if present
        wal = db_path.with_suffix(".duckdb.wal")
        if wal.exists():
            wal.unlink()
    needs_generate = not db_path.exists()

    if needs_generate:
        print(f"Generating TPC-H data (SF={sf}) into {db_path}...", flush=True)
        gen_con = duckdb.connect(str(db_path), config={"allow_unsigned_extensions": True})
        # Load extension in generator connection so tpch extension is available
        if EXT_PATH.exists():
            gen_con.execute(f"LOAD '{EXT_PATH}';")
        else:
            gen_con.execute("LOAD query_condition_cache;")
        gen_con.execute("CALL dbgen(sf=%d);" % sf)
        gen_con.execute("CHECKPOINT;")
        gen_con.close()
        print("Data generated and cached on disk.", flush=True)
    else:
        print(f"Reusing cached TPC-H data (SF={sf}) from {db_path}", flush=True)

    con = duckdb.connect(str(db_path), config=cfg)

    # Load the locally-built extension if available, otherwise fall back to installed
    if EXT_PATH.exists():
        con.execute(f"LOAD '{EXT_PATH}';")
    else:
        print(f"WARNING: Extension not found at {EXT_PATH}. Build with: GEN=ninja make")
        print("Attempting to LOAD from installed extensions...")
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
    """
    Run the 4-step protocol and return timing results.
    Steps:
      1. cache OFF, first run  (cold OS baseline — reference)
      2. cache OFF, repeated   (warm OS baseline — true baseline)
      3. cache ON,  first run  (cache build)
      4. cache ON,  repeated   (cache hit)
    """
    # Step 1 & 2: cache OFF
    con.execute("SET use_query_condition_cache = false;")
    t_cold = time_query(con, sql, 1)[0]
    t_warm_no_cache = time_query(con, sql, repeat)

    # Step 3 & 4: cache ON
    con.execute("SET use_query_condition_cache = true;")
    t_build = time_query(con, sql, 1)[0]
    t_cache_hit = time_query(con, sql, repeat)

    # Always turn cache back off so queries don't interfere
    con.execute("SET use_query_condition_cache = false;")

    baseline_avg = statistics.mean(t_warm_no_cache)
    baseline_std = statistics.stdev(t_warm_no_cache) if len(t_warm_no_cache) > 1 else 0.0
    cache_hit_avg = statistics.mean(t_cache_hit)
    cache_hit_std = statistics.stdev(t_cache_hit) if len(t_cache_hit) > 1 else 0.0
    speedup = baseline_avg / cache_hit_avg if cache_hit_avg > 0 else float("inf")

    return {
        "label": label,
        "cold_ms": t_cold,
        "baseline_avg": baseline_avg,
        "baseline_std": baseline_std,
        "baseline_runs": t_warm_no_cache,
        "build_ms": t_build,
        "cache_hit_avg": cache_hit_avg,
        "cache_hit_std": cache_hit_std,
        "cache_hit_runs": t_cache_hit,
        "speedup": speedup,
    }


def run_tpch_suite(con, repeat: int) -> list[dict]:
    """Run all 22 TPC-H queries."""
    print("\nRunning TPC-H suite (22 queries)...", flush=True)
    results = []

    rows = con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr;").fetchall()
    for query_nr, query_sql in rows:
        label = f"Q{query_nr:02d}"
        print(f"  {label}...", end=" ", flush=True)
        result = benchmark_query(con, label, query_sql.strip(), repeat)
        results.append(result)
        print(
            f"baseline={result['baseline_avg']:.0f}±{result['baseline_std']:.0f}ms  "
            f"hit={result['cache_hit_avg']:.0f}±{result['cache_hit_std']:.0f}ms  {result['speedup']:.2f}x",
            flush=True,
        )
    return results


def run_dashboard_suite(con, repeat: int) -> list[dict]:
    """Run dashboard-style queries on lineitem."""
    print("\nRunning dashboard query suite...", flush=True)
    results = []

    for label, sql in DASHBOARD_QUERIES:
        short = label.split(":")[0]
        print(f"  {short}...", end=" ", flush=True)
        result = benchmark_query(con, label, sql, repeat)
        results.append(result)
        print(
            f"baseline={result['baseline_avg']:.0f}±{result['baseline_std']:.0f}ms  "
            f"hit={result['cache_hit_avg']:.0f}±{result['cache_hit_std']:.0f}ms  {result['speedup']:.2f}x",
            flush=True,
        )
    return results


def format_table(title: str, results: list[dict], sf: int, repeat: int) -> str:
    lines = [
        f"## {title} (SF={sf}, avg±std of {repeat} warm runs)\n",
        "| Query | Cold (ms) | Baseline avg±std (ms) | Cache Build (ms) | Cache Hit avg±std (ms) | Speedup |",
        "|-------|-----------|-----------------------|------------------|------------------------|---------|",
    ]
    for r in results:
        lines.append(
            f"| {r['label']:<40} "
            f"| {r['cold_ms']:>9.0f} "
            f"| {r['baseline_avg']:>10.0f} ± {r['baseline_std']:<8.0f} "
            f"| {r['build_ms']:>16.0f} "
            f"| {r['cache_hit_avg']:>11.0f} ± {r['cache_hit_std']:<8.0f} "
            f"| {r['speedup']:>6.2f}x |"
        )
    lines.append("")
    return "\n".join(lines)


def plot_results(tpch_results: list[dict], dash_results: list[dict], sf: int, out_path: Path):
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib/numpy not available, skipping chart generation.")
        return

    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle(f"QueryConditionCache — TPC-H SF={sf} (avg ± std)", fontsize=14, fontweight="bold")

    bar_width = 0.35
    for ax, results, title in [
        (axes[0], tpch_results, "TPC-H Queries (Q01–Q22)"),
        (axes[1], dash_results, "Dashboard Queries on lineitem"),
    ]:
        if not results:
            ax.set_visible(False)
            continue
        labels = [r["label"].split(":")[0] for r in results]
        baseline_avgs = [r["baseline_avg"] for r in results]
        baseline_stds = [r["baseline_std"] for r in results]
        cached_avgs = [r["cache_hit_avg"] for r in results]
        cached_stds = [r["cache_hit_std"] for r in results]

        x = np.arange(len(labels))
        ax.bar(x - bar_width / 2, baseline_avgs, bar_width, yerr=baseline_stds,
               capsize=3, label="Baseline (no cache)", color="#9E9E9E", edgecolor="white")
        ax.bar(x + bar_width / 2, cached_avgs, bar_width, yerr=cached_stds,
               capsize=3, label="Cached", color="#2196F3", edgecolor="white")

        # Annotate speedup above each pair
        for i, r in enumerate(results):
            top = max(baseline_avgs[i] + baseline_stds[i], cached_avgs[i] + cached_stds[i])
            if r["speedup"] >= 1.05:
                ax.text(x[i], top * 1.02, f"{r['speedup']:.1f}x",
                        ha="center", va="bottom", fontsize=7, fontweight="bold", color="#1565C0")

        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("Time (ms)")
        ax.set_title(title)
        ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {out_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="QueryConditionCache TPC-H benchmark")
    parser.add_argument("--sf", type=int, default=1, help="TPC-H scale factor (default: 1)")
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of warm runs per phase for median (default: 3)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output file for results (default: benchmark/results_sf<N>.md)",
    )
    parser.add_argument(
        "--no-tpch",
        action="store_true",
        help="Skip the 22 standard TPC-H queries, run dashboard only",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Skip dashboard queries, run standard TPC-H only",
    )
    parser.add_argument("--no-chart", action="store_true", help="Skip chart generation")
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of TPC-H data even if cached on disk",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of DuckDB threads (default: auto). Use 1 for single-threaded benchmark.",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default=None,
        help="DuckDB memory limit (e.g. '4GB'). Lowering this forces more reads from storage, "
        "amplifying cache benefit.",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default=None,
        help="Name prefix for output files (e.g. 'baseline_v2'). "
        "Default auto-generates from arguments.",
    )
    args = parser.parse_args()

    # Build a descriptive default filename from arguments
    if args.out:
        out_file = args.out
    else:
        parts = [f"tpch_sf{args.sf}"]
        if args.threads:
            parts.append(f"t{args.threads}")
        if args.memory_limit:
            parts.append(f"mem{args.memory_limit.replace(' ', '')}")
        parts.append(f"r{args.repeat}")
        if args.experiment_name:
            parts.append(args.experiment_name)
        out_file = str(WORKSPACE / "benchmark" / f"{'_'.join(parts)}.md")

    con = get_duckdb_connection(args.sf, args)

    tpch_results = []
    dash_results = []

    if not args.no_tpch:
        tpch_results = run_tpch_suite(con, args.repeat)

    if not args.no_dashboard:
        dash_results = run_dashboard_suite(con, args.repeat)

    mem_note = f", memory_limit={args.memory_limit}" if args.memory_limit else ""
    threads_note = f", threads={args.threads}" if args.threads else ""

    # Build markdown output
    lines = [
        f"# QueryConditionCache Benchmark Results — TPC-H SF={args.sf}\n",
        f"**Settings:** SF={args.sf}{threads_note}{mem_note}\n",
        f"**Methodology:** Each query runs {args.repeat + 2} times in one session.",
        "Run 1 (cold OS, cache off) warms the OS page cache and DuckDB buffer pool.",
        "Runs 2–N (warm OS, cache off) establish the true baseline — warm buffer pool, no condition cache.",
        "Cache is then enabled; first run builds the cache entry, subsequent runs measure cache hits.",
        "**Speedup = avg(warm OS+buffer, no cache) / avg(warm OS+buffer, cache hit)**",
        "",
        "> Note: When data fits in the DuckDB buffer pool (small SF or large RAM), the baseline is already",
        "> serving from memory and speedups will be modest. Use `--memory-limit` to constrain the buffer",
        "> pool below the dataset size to see larger speedups representative of cold-data scenarios.\n",
    ]

    if tpch_results:
        lines.append(format_table("TPC-H Standard Queries", tpch_results, args.sf, args.repeat))

    if dash_results:
        lines.append(
            format_table("Dashboard Queries on lineitem", dash_results, args.sf, args.repeat)
        )

    output = "\n".join(lines)
    print("\n" + output)

    with open(out_file, "w") as f:
        f.write(output)
    print(f"\nResults written to {out_file}")

    if not args.no_chart:
        chart_path = Path(out_file).with_suffix(".png")
        plot_results(tpch_results, dash_results, args.sf, chart_path)

    con.close()


if __name__ == "__main__":
    main()
