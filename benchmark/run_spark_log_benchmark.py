#!/usr/bin/env python3
"""
Spark Log Analytics Benchmark Harness for QueryConditionCache extension.

Downloads the full Spark log dataset from Zenodo (loghub-2.0), loads it into
DuckDB, and benchmarks three real-world observability stories:

  Story 1 – Multi-Widget Dashboard:  4 queries sharing Content LIKE '%memory%'
  Story 2 – SRE Interactive Drill-Down: iterative refinement with LIKE filters on broadcast/memory
  Story 3 – Pattern Matching:  SecurityManager log analysis with LIKE filters

Per-query protocol (fresh connection + OS cache drop per query):
  Run 1 – cache OFF, OS cold  : cold baseline (reference only)
  Run 2 – cache OFF, OS warm  : true baseline (warm OS, no condition cache)
  Run 3 – cache ON,  first run : cache build (first query with cache enabled)
  Run 4 – cache ON,  repeated  : cache hit   (warm OS + warm condition cache)

  Speedup = Run 2 / Run 4

For stories, the "story speedup" measures the total wall-clock time across all
queries in the story, since the cache is built on the first query and reused by
subsequent ones.

Usage:
    python benchmark/run_spark_log_benchmark.py [--repeat N] [--out FILE] [--stories 1,2,3]
"""

import argparse
import platform
import statistics
import subprocess
import sys
import time
import urllib.request
import zipfile
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

SPARK_ZIP_URL = "https://zenodo.org/records/8275861/files/Spark.zip?download=1"
SPARK_DATA_DIR = WORKSPACE / "benchmark" / "spark_data"
SPARK_DB_PATH = WORKSPACE / "benchmark" / "spark_logs.duckdb"

# ---------------------------------------------------------------------------
# Story definitions
# ---------------------------------------------------------------------------

STORY_1_QUERIES = [
    (
        "S1-W1: Memory Event Count",
        "SELECT COUNT(*) FROM logs WHERE Content LIKE '%memory%';",
    ),
    (
        "S1-W2: Memory Event Templates",
        "SELECT EventTemplate, COUNT(*) FROM logs WHERE Content LIKE '%memory%' GROUP BY EventTemplate;",
    ),
    (
        "S1-W3: Memory Event IDs",
        "SELECT EventId, COUNT(*) FROM logs WHERE Content LIKE '%memory%' GROUP BY EventId ORDER BY 2 DESC;",
    ),
    (
        "S1-W4: Memory + TID",
        "SELECT EventId, COUNT(*) FROM logs WHERE Content LIKE '%memory%' AND Content LIKE '%TID%' GROUP BY EventId;",
    ),
]

STORY_2_QUERIES = [
    (
        "S2-Q1: Broadcast events",
        "SELECT Content FROM logs WHERE Content LIKE '%broadcast%';",
    ),
    (
        "S2-Q2: Broadcast + memory",
        "SELECT Content FROM logs WHERE Content LIKE '%broadcast%' AND Content LIKE '%memory%';",
    ),
    (
        "S2-Q3: Broadcast + memory templates",
        "SELECT EventTemplate, COUNT(*) FROM logs WHERE Content LIKE '%broadcast%' AND Content LIKE '%memory%' GROUP BY EventTemplate ORDER BY 2 DESC;",
    ),
]

STORY_3_QUERIES = [
    (
        "S3-W1: SecurityManager acls",
        "SELECT Content FROM logs WHERE Content LIKE '%SecurityManager%' AND Content LIKE '%acls%';",
    ),
    (
        "S3-W2: SecurityManager permissions",
        "SELECT Content FROM logs WHERE Content LIKE '%SecurityManager%' AND Content LIKE '%permissions%';",
    ),
]

STORIES = {
    1: ("Story 1: Multi-Widget Auto-Refreshing Dashboard", STORY_1_QUERIES),
    2: ("Story 2: SRE Interactive Drill-Down", STORY_2_QUERIES),
    3: ("Story 3: Sliding Time Window", STORY_3_QUERIES),
}


# ---------------------------------------------------------------------------
# Data download & loading
# ---------------------------------------------------------------------------

def download_spark_data():
    """Download and extract the Spark structured log dataset from Zenodo (loghub-2.0)."""
    zip_path = SPARK_DATA_DIR / "Spark.zip"
    SPARK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not zip_path.exists():
        print("Downloading Spark structured log data from Zenodo...", flush=True)
        urllib.request.urlretrieve(SPARK_ZIP_URL, str(zip_path))
        print(f"Downloaded to {zip_path}", flush=True)
    else:
        print(f"Reusing cached zip at {zip_path}", flush=True)

    # Extract
    csv_candidates = list(SPARK_DATA_DIR.glob("**/Spark_full.log_structured.csv"))
    if not csv_candidates:
        print("Extracting zip...", flush=True)
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            zf.extractall(str(SPARK_DATA_DIR))
        csv_candidates = list(SPARK_DATA_DIR.glob("**/Spark_full.log_structured.csv"))

    if not csv_candidates:
        raise FileNotFoundError(
            "Could not find Spark.log_structured.csv in the extracted archive. "
            "Contents: " + str(list(SPARK_DATA_DIR.rglob("*")))
        )

    csv_path = csv_candidates[0]
    print(f"Found structured CSV: {csv_path}", flush=True)
    return csv_path


def ensure_spark_data(args):
    """Download/load Spark log data into DuckDB if not already cached. Returns db_path."""
    import duckdb

    db_path = SPARK_DB_PATH
    if args.regenerate and db_path.exists():
        db_path.unlink()
        wal = db_path.with_suffix(".duckdb.wal")
        if wal.exists():
            wal.unlink()

    if not db_path.exists():
        csv_path = download_spark_data()
        row_count = sum(1 for _ in open(csv_path)) - 1  # exclude header
        print(f"CSV has {row_count:,} rows. Loading into DuckDB...", flush=True)

        con = duckdb.connect(str(db_path), config={"allow_unsigned_extensions": True})
        if EXT_PATH.exists():
            con.execute(f"LOAD '{EXT_PATH}';")
        else:
            con.execute("LOAD query_condition_cache;")

        con.execute("""
            CREATE TABLE logs AS
            SELECT
                LineId::INTEGER AS LineId,
                Content::VARCHAR AS Content,
                EventId::VARCHAR AS EventId,
                EventTemplate::VARCHAR AS EventTemplate
            FROM read_csv_auto(?, header=true, all_varchar=true);
        """, [str(csv_path)])
        count = con.execute("SELECT COUNT(*) FROM logs;").fetchone()[0]
        print(f"Loaded {count:,} rows into 'logs' table.", flush=True)

        # Print some stats for verification
        templates = con.execute(
            "SELECT EventTemplate, COUNT(*) AS cnt FROM logs GROUP BY EventTemplate ORDER BY cnt DESC LIMIT 10;"
        ).fetchall()
        print("Top 10 event templates:", flush=True)
        for tmpl, cnt in templates:
            print(f"  {tmpl}: {cnt:,}", flush=True)

        con.execute("CHECKPOINT;")
        con.close()
        print(f"Data cached at {db_path}", flush=True)
    else:
        print(f"Reusing cached Spark log data from {db_path}", flush=True)

    return db_path


# ---------------------------------------------------------------------------
# Benchmark infrastructure (matches clickbench/tpch pattern)
# ---------------------------------------------------------------------------

def drop_os_caches():
    """Best-effort OS page cache drop. Requires sudo on macOS, root on Linux."""
    try:
        if platform.system() == "Darwin":
            subprocess.run(["sudo", "-n", "purge"], capture_output=True, timeout=10)
        else:
            subprocess.run(
                ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
                capture_output=True, timeout=10,
            )
    except Exception:
        pass  # best-effort; not fatal if unavailable


def open_connection(db_path: Path, args):
    """Open a fresh DuckDB connection with the extension loaded."""
    import duckdb

    cfg: dict = {"allow_unsigned_extensions": True}
    if args.threads:
        cfg["threads"] = args.threads
    if args.memory_limit:
        cfg["memory_limit"] = args.memory_limit

    con = duckdb.connect(str(db_path), config=cfg)
    if EXT_PATH.exists():
        con.execute(f"LOAD '{EXT_PATH}';")
    else:
        con.execute("LOAD query_condition_cache;")
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


def benchmark_story(db_path: Path, args, story_name: str, queries: list[tuple[str, str]], repeat: int) -> list[dict]:
    """
    Run a story's queries sequentially on a single connection.

    For story benchmarking, we keep the same connection across queries within
    a story, because the point is that the cache built by query N benefits
    query N+1.
    """
    print(f"\n### {story_name}", flush=True)
    results = []

    # --- Baseline pass: cache OFF, all queries ---
    drop_os_caches()
    con = open_connection(db_path, args)
    con.execute("SET use_query_condition_cache = false;")

    # Cold run (warms OS cache)
    for label, sql in queries:
        time_query(con, sql, 1)

    # Warm baseline runs
    baseline_times = {}
    for label, sql in queries:
        baseline_times[label] = time_query(con, sql, repeat)
    con.close()

    # --- Cached pass: cache ON, all queries (cache builds on first, reuses on rest) ---
    drop_os_caches()
    con = open_connection(db_path, args)
    con.execute("SET use_query_condition_cache = true;")

    # Cold + cache build run
    build_times = {}
    for label, sql in queries:
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        build_times[label] = (time.perf_counter() - t0) * 1000

    # Warm cache hit runs
    cache_hit_times = {}
    for label, sql in queries:
        cache_hit_times[label] = time_query(con, sql, repeat)
    con.close()

    # Assemble results
    for label, sql in queries:
        short = label.split(":")[0]
        bl = baseline_times[label]
        ch = cache_hit_times[label]
        baseline_avg = statistics.mean(bl)
        baseline_std = statistics.stdev(bl) if len(bl) > 1 else 0.0
        cache_hit_avg = statistics.mean(ch)
        cache_hit_std = statistics.stdev(ch) if len(ch) > 1 else 0.0
        speedup = baseline_avg / cache_hit_avg if cache_hit_avg > 0 else float("inf")

        r = {
            "label": label,
            "cold_ms": 0,  # not measured individually in story mode
            "baseline_avg": baseline_avg,
            "baseline_std": baseline_std,
            "baseline_runs": bl,
            "build_ms": build_times[label],
            "cache_hit_avg": cache_hit_avg,
            "cache_hit_std": cache_hit_std,
            "cache_hit_runs": ch,
            "speedup": speedup,
        }
        results.append(r)
        print(
            f"  {short}:  baseline={baseline_avg:.0f}±{baseline_std:.0f}ms  "
            f"hit={cache_hit_avg:.0f}±{cache_hit_std:.0f}ms  {speedup:.2f}x",
            flush=True,
        )

    # Compute story-level totals
    total_baseline = sum(statistics.mean(baseline_times[l]) for l, _ in queries)
    total_cached = sum(statistics.mean(cache_hit_times[l]) for l, _ in queries)
    story_speedup = total_baseline / total_cached if total_cached > 0 else float("inf")
    print(
        f"  ** Story total: baseline={total_baseline:.0f}ms  cached={total_cached:.0f}ms  "
        f"speedup={story_speedup:.2f}x **",
        flush=True,
    )

    return results


# ---------------------------------------------------------------------------
# Individual query benchmark (for completeness, like clickbench pattern)
# ---------------------------------------------------------------------------

def run_individual_queries(db_path: Path, args, repeat: int) -> list[dict]:
    """Run each story query individually with full 4-step protocol (fresh connection per query)."""
    print("\nRunning all queries individually (fresh connection per query)...", flush=True)
    results = []
    all_queries = []
    for story_id in sorted(STORIES.keys()):
        _, queries = STORIES[story_id]
        all_queries.extend(queries)

    for label, sql in all_queries:
        short = label.split(":")[0]
        print(f"  {short}...", end=" ", flush=True)
        try:
            drop_os_caches()
            con = open_connection(db_path, args)
            result = benchmark_query(con, label, sql, repeat)
            con.close()
            results.append(result)
            print(
                f"baseline={result['baseline_avg']:.0f}±{result['baseline_std']:.0f}ms  "
                f"hit={result['cache_hit_avg']:.0f}±{result['cache_hit_std']:.0f}ms  {result['speedup']:.2f}x",
                flush=True,
            )
        except Exception as e:
            print(f"FAILED: {e}", flush=True)
    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def format_table(title: str, results: list[dict], repeat: int) -> str:
    lines = [
        f"## {title} (avg±std of {repeat} warm runs)\n",
        "| Query | Baseline avg±std (ms) | Cache Build (ms) | Cache Hit avg±std (ms) | Speedup |",
        "|-------|-----------------------|------------------|------------------------|---------|",
    ]
    for r in results:
        lines.append(
            f"| {r['label']:<45} "
            f"| {r['baseline_avg']:>10.1f} ± {r['baseline_std']:<8.1f} "
            f"| {r['build_ms']:>16.1f} "
            f"| {r['cache_hit_avg']:>11.1f} ± {r['cache_hit_std']:<8.1f} "
            f"| {r['speedup']:>6.2f}x |"
        )

    # Story total row
    if results:
        total_bl = sum(r["baseline_avg"] for r in results)
        total_ch = sum(r["cache_hit_avg"] for r in results)
        total_build = sum(r["build_ms"] for r in results)
        total_speedup = total_bl / total_ch if total_ch > 0 else float("inf")
        lines.append(
            f"| {'**TOTAL**':<45} "
            f"| {total_bl:>10.1f} {'':>11} "
            f"| {total_build:>16.1f} "
            f"| {total_ch:>11.1f} {'':>11} "
            f"| **{total_speedup:.2f}x** |"
        )

    lines.append("")
    return "\n".join(lines)


def plot_results(all_story_results: dict, out_path: Path):
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib/numpy not available, skipping chart generation.")
        return

    n_stories = len(all_story_results)
    fig, axes = plt.subplots(1, n_stories, figsize=(7 * n_stories, 7))
    if n_stories == 1:
        axes = [axes]
    fig.suptitle("QueryConditionCache — Spark Log Analytics (avg ± std)", fontsize=14, fontweight="bold")

    bar_width = 0.35
    for ax, (story_id, (story_name, results)) in zip(axes, all_story_results.items()):
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

        # Log scale when range is too large
        all_vals = [v for v in baseline_avgs + cached_avgs if v > 0]
        if all_vals and max(all_vals) / min(all_vals) > 50:
            ax.set_yscale("log")

        # Annotate speedup
        for i, r in enumerate(results):
            top = max(baseline_avgs[i] + baseline_stds[i], cached_avgs[i] + cached_stds[i])
            if r["speedup"] >= 1.05:
                ax.text(x[i], top * 1.05, f"{r['speedup']:.1f}x",
                        ha="center", va="bottom", fontsize=8, fontweight="bold", color="#1565C0")

        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=8)
        ax.set_ylabel("Time (ms)")
        ax.set_title(story_name, fontsize=10)
        ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {out_path}")
    plt.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="QueryConditionCache Spark Log Analytics benchmark")
    parser.add_argument(
        "--repeat", type=int, default=3,
        help="Number of warm runs per phase (default: 3)",
    )
    parser.add_argument(
        "--out", type=str, default=None,
        help="Output file for results (default: auto-generated)",
    )
    parser.add_argument(
        "--stories", type=str, default=None,
        help="Comma-separated story numbers to run (e.g. '1,2'). Default: all.",
    )
    parser.add_argument(
        "--individual", action="store_true",
        help="Also run each query individually with fresh connection (like clickbench mode)",
    )
    parser.add_argument("--no-chart", action="store_true", help="Skip chart generation")
    parser.add_argument(
        "--regenerate", action="store_true",
        help="Force re-download/reload of Spark log data",
    )
    parser.add_argument(
        "--threads", type=int, default=None,
        help="Number of DuckDB threads (default: auto)",
    )
    parser.add_argument(
        "--memory-limit", type=str, default=None,
        help="DuckDB memory limit (e.g. '4GB')",
    )
    parser.add_argument(
        "--experiment-name", type=str, default=None,
        help="Name prefix for output files",
    )
    args = parser.parse_args()

    # Build output filename
    if args.out:
        out_file = args.out
    else:
        parts = ["spark_logs"]
        if args.threads:
            parts.append(f"t{args.threads}")
        if args.memory_limit:
            parts.append(f"mem{args.memory_limit.replace(' ', '')}")
        parts.append(f"r{args.repeat}")
        if args.stories:
            parts.append(f"s{args.stories.replace(',', '-')}")
        if args.experiment_name:
            parts.append(args.experiment_name)
        out_file = str(WORKSPACE / "benchmark" / f"{'_'.join(parts)}.md")

    try:
        import duckdb  # noqa: F401
    except ImportError:
        print("ERROR: duckdb Python package not found. Run via: uv run benchmark/run_spark_log_benchmark.py")
        sys.exit(1)

    db_path = ensure_spark_data(args)
    print("Data ready.", flush=True)

    # Select stories
    if args.stories:
        selected = [int(s.strip()) for s in args.stories.split(",")]
    else:
        selected = sorted(STORIES.keys())

    # Run story-mode benchmarks
    all_story_results = {}
    for story_id in selected:
        story_name, queries = STORIES[story_id]
        results = benchmark_story(db_path, args, story_name, queries, args.repeat)
        all_story_results[story_id] = (story_name, results)

    # Optionally run individual-query mode
    individual_results = []
    if args.individual:
        individual_results = run_individual_queries(db_path, args, args.repeat)

    # Build markdown output
    mem_note = f", memory_limit={args.memory_limit}" if args.memory_limit else ""
    threads_note = f", threads={args.threads}" if args.threads else ""

    lines = [
        "# QueryConditionCache Benchmark Results — Spark Log Analytics\n",
        f"**Settings:**{threads_note}{mem_note}\n",
        f"**Methodology:** Each story runs on a single connection (cache persists across queries).",
        f"Baseline: {args.repeat} warm runs with cache OFF. Cached: {args.repeat} warm runs with cache ON.",
        "OS page caches are dropped between baseline and cached passes.",
        "**Speedup = avg(baseline) / avg(cache hit)**\n",
        "**Dataset:** Spark executor logs from LogHub-2.0 (Zenodo record 8196385)\n",
    ]

    for story_id in selected:
        story_name, results = all_story_results[story_id]
        lines.append(format_table(story_name, results, args.repeat))

    if individual_results:
        lines.append(format_table("Individual Queries (fresh connection each)", individual_results, args.repeat))

    output = "\n".join(lines)
    print("\n" + output)

    with open(out_file, "w") as f:
        f.write(output)
    print(f"\nResults written to {out_file}")

    if not args.no_chart and all_story_results:
        chart_path = Path(out_file).with_suffix(".png")
        plot_results(all_story_results, chart_path)


if __name__ == "__main__":
    main()
