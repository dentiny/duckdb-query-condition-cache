#!/usr/bin/env python3
"""
HDFS Log Analytics Benchmark Harness for QueryConditionCache extension.

Downloads the HDFS_v2 dataset from Zenodo (71M log lines, ~16GB raw), parses
the raw Hadoop log4j format into structured columns, loads into DuckDB, and
benchmarks three real-world observability stories.

  Story 1 – Block Health Dashboard:  4 queries sharing Content LIKE '%blk_%'
  Story 2 – SRE Replication Drill-Down: iterative refinement on block replication
  Story 3 – Error Investigation:  WARN/ERROR pattern matching with LIKE

The dataset is significantly larger than Spark (~71M rows vs ~33M), making the
performance gap between cached and uncached queries more pronounced.

Per-story protocol:
  Baseline: N warm runs with cache OFF on a single connection.
  Cached:   cache ON, first run builds cache, N warm runs measure hits.
  OS page caches are dropped between baseline and cached passes.

  Speedup = avg(baseline) / avg(cache hit)

Usage:
    python benchmark/run_hdfs_log_benchmark.py [--repeat N] [--out FILE] [--stories 1,2,3]
"""

import argparse
import glob as glob_mod
import platform
import re
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

HDFS_ZIP_URL = "https://zenodo.org/records/8196385/files/HDFS_v2.zip?download=1"
HDFS_DATA_DIR = WORKSPACE / "benchmark" / "hdfs_data"
HDFS_DB_PATH = WORKSPACE / "benchmark" / "hdfs_logs.duckdb"

# Regex to parse Hadoop log4j lines:
#   2016-10-22 13:28:13,003 INFO org.apache.hadoop.hdfs.StateChange: DIR* completeFile: ...
_LOG_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})\s+"      # date
    r"(\d{2}:\d{2}:\d{2}),\d{3}\s+" # time (drop millis for grouping)
    r"(\w+)\s+"                      # level (INFO, WARN, ERROR, etc.)
    r"([^:]+):\s+"                   # component (java class / short name)
    r"(.*)$"                         # message content
)

# ---------------------------------------------------------------------------
# Story definitions
# ---------------------------------------------------------------------------

STORY_1_QUERIES = [
    (
        "S1-W1: Block Event Count",
        "SELECT COUNT(*) FROM logs WHERE Content LIKE '%blk_%';",
    ),
    (
        "S1-W2: Block Events by Level",
        "SELECT Level, COUNT(*) FROM logs WHERE Content LIKE '%blk_%' GROUP BY Level;",
    ),
    (
        "S1-W3: Block Events by Component",
        "SELECT Component, COUNT(*) FROM logs WHERE Content LIKE '%blk_%' GROUP BY Component ORDER BY 2 DESC;",
    ),
    (
        "S1-W4: Block + Replication",
        "SELECT Component, COUNT(*) FROM logs WHERE Content LIKE '%blk_%' AND Content LIKE '%replicas%' GROUP BY Component;",
    ),
]

STORY_2_QUERIES = [
    (
        "S2-Q1: addStoredBlock events",
        "SELECT Content FROM logs WHERE Content LIKE '%addStoredBlock%' LIMIT 1000;",
    ),
    (
        "S2-Q2: addStoredBlock + specific IP",
        "SELECT Content FROM logs WHERE Content LIKE '%addStoredBlock%' AND Content LIKE '%10.10.34.11%' LIMIT 1000;",
    ),
    (
        "S2-Q3: addStoredBlock aggregate by date",
        "SELECT Date, COUNT(*) FROM logs WHERE Content LIKE '%addStoredBlock%' AND Content LIKE '%10.10.34.11%' GROUP BY Date ORDER BY Date;",
    ),
]

STORY_3_QUERIES = [
    (
        "S3-Q1: WARN + ERROR logs",
        "SELECT COUNT(*) FROM logs WHERE Level = 'WARN' OR Level = 'ERROR';",
    ),
    (
        "S3-Q2: Exceptions in content",
        "SELECT Content FROM logs WHERE Content LIKE '%Exception%' LIMIT 1000;",
    ),
    (
        "S3-Q3: Exception + block correlation",
        "SELECT Date, COUNT(*) FROM logs WHERE Content LIKE '%Exception%' AND Content LIKE '%blk_%' GROUP BY Date ORDER BY Date;",
    ),
]

STORIES = {
    1: ("Story 1: Block Health Dashboard (71M rows)", STORY_1_QUERIES),
    2: ("Story 2: SRE Replication Drill-Down", STORY_2_QUERIES),
    3: ("Story 3: Error & Exception Investigation", STORY_3_QUERIES),
}


# ---------------------------------------------------------------------------
# Data download, parsing & loading
# ---------------------------------------------------------------------------

def download_hdfs_data():
    """Download and extract the HDFS_v2 dataset from Zenodo."""
    zip_path = HDFS_DATA_DIR / "HDFS_v2.zip"
    HDFS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not zip_path.exists():
        print("Downloading HDFS_v2 dataset from Zenodo (~786 MB)...", flush=True)
        urllib.request.urlretrieve(HDFS_ZIP_URL, str(zip_path))
        print(f"Downloaded to {zip_path}", flush=True)
    else:
        print(f"Reusing cached zip at {zip_path}", flush=True)

    # Extract if needed
    log_dir = HDFS_DATA_DIR / "node_logs"
    if not log_dir.exists() or not list(log_dir.glob("*.log")):
        print("Extracting zip...", flush=True)
        with zipfile.ZipFile(str(zip_path), "r") as zf:
            zf.extractall(str(HDFS_DATA_DIR))
        print("Extraction complete.", flush=True)

    log_files = sorted(log_dir.glob("*.log"))
    if not log_files:
        raise FileNotFoundError(f"No .log files found in {log_dir}")

    print(f"Found {len(log_files)} log files in {log_dir}", flush=True)
    return log_files


def parse_and_load_logs(log_files: list[Path], db_path: Path):
    """Parse raw HDFS logs and load into DuckDB.

    Uses a two-phase approach: first writes a temporary CSV, then bulk-loads
    into DuckDB for efficiency with 71M+ rows.
    """
    import duckdb

    csv_path = HDFS_DATA_DIR / "hdfs_parsed.csv"

    if not csv_path.exists():
        print("Parsing raw HDFS logs into CSV (this may take a few minutes)...", flush=True)
        total_lines = 0
        parsed_lines = 0
        skipped_lines = 0

        with open(csv_path, "w", encoding="utf-8") as out:
            out.write("Date,Time,Level,Component,Content,SourceFile\n")

            for log_file in log_files:
                source = log_file.name
                file_lines = 0
                file_parsed = 0
                print(f"  Parsing {source}...", end=" ", flush=True)

                with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        file_lines += 1
                        m = _LOG_RE.match(line.rstrip())
                        if m:
                            date, time_str, level, component, content = m.groups()
                            # CSV-escape: double any quotes in content
                            content = content.replace('"', '""')
                            component = component.strip()
                            out.write(f'{date},{time_str},{level},"{component}","{content}","{source}"\n')
                            file_parsed += 1

                total_lines += file_lines
                parsed_lines += file_parsed
                skipped_lines += file_lines - file_parsed
                print(f"{file_parsed:,}/{file_lines:,} lines parsed", flush=True)

        print(
            f"\nTotal: {parsed_lines:,} parsed, {skipped_lines:,} skipped "
            f"(multi-line/startup messages) out of {total_lines:,} raw lines.",
            flush=True,
        )
    else:
        print(f"Reusing parsed CSV at {csv_path}", flush=True)

    # Load into DuckDB
    print(f"Loading parsed CSV into DuckDB at {db_path}...", flush=True)
    con = duckdb.connect(str(db_path), config={"allow_unsigned_extensions": True})
    if EXT_PATH.exists():
        con.execute(f"LOAD '{EXT_PATH}';")
    else:
        con.execute("LOAD query_condition_cache;")

    con.execute(f"""
        CREATE TABLE logs AS
        SELECT
            Date::VARCHAR AS Date,
            Time::VARCHAR AS Time,
            Level::VARCHAR AS Level,
            Component::VARCHAR AS Component,
            Content::VARCHAR AS Content,
            SourceFile::VARCHAR AS SourceFile
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true,
                           max_line_size=1048576, ignore_errors=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM logs;").fetchone()[0]
    print(f"Loaded {count:,} rows into 'logs' table.", flush=True)

    # Print stats
    print("\nLevel distribution:", flush=True)
    for level, cnt in con.execute(
        "SELECT Level, COUNT(*) AS cnt FROM logs GROUP BY Level ORDER BY cnt DESC;"
    ).fetchall():
        print(f"  {level}: {cnt:,}", flush=True)

    print("\nTop 10 components:", flush=True)
    for comp, cnt in con.execute(
        "SELECT Component, COUNT(*) AS cnt FROM logs GROUP BY Component ORDER BY cnt DESC LIMIT 10;"
    ).fetchall():
        print(f"  {comp}: {cnt:,}", flush=True)

    con.execute("CHECKPOINT;")
    con.close()
    print(f"\nData cached at {db_path}", flush=True)


def ensure_hdfs_data(args):
    """Download/parse/load HDFS log data into DuckDB if not already cached. Returns db_path."""
    db_path = HDFS_DB_PATH
    if args.regenerate and db_path.exists():
        db_path.unlink()
        wal = db_path.with_suffix(".duckdb.wal")
        if wal.exists():
            wal.unlink()
        # Also remove parsed CSV to force re-parse
        csv_path = HDFS_DATA_DIR / "hdfs_parsed.csv"
        if csv_path.exists():
            csv_path.unlink()

    if not db_path.exists():
        log_files = download_hdfs_data()
        parse_and_load_logs(log_files, db_path)
    else:
        print(f"Reusing cached HDFS log data from {db_path}", flush=True)

    return db_path


# ---------------------------------------------------------------------------
# Benchmark infrastructure (matches clickbench/tpch/spark pattern)
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

    Cache built by query N benefits query N+1 within the same story.
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
            "cold_ms": 0,
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
    fig.suptitle("QueryConditionCache — HDFS Log Analytics (avg ± std)", fontsize=14, fontweight="bold")

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
               capsize=3, label="Cached", color="#4CAF50", edgecolor="white")

        # Log scale when range is too large
        all_vals = [v for v in baseline_avgs + cached_avgs if v > 0]
        if all_vals and max(all_vals) / min(all_vals) > 50:
            ax.set_yscale("log")

        # Annotate speedup
        for i, r in enumerate(results):
            top = max(baseline_avgs[i] + baseline_stds[i], cached_avgs[i] + cached_stds[i])
            if r["speedup"] >= 1.05:
                ax.text(x[i], top * 1.05, f"{r['speedup']:.1f}x",
                        ha="center", va="bottom", fontsize=8, fontweight="bold", color="#2E7D32")

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
    parser = argparse.ArgumentParser(description="QueryConditionCache HDFS Log Analytics benchmark")
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
        help="Force re-download/reload of HDFS log data",
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
        parts = ["hdfs_logs"]
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
        print("ERROR: duckdb Python package not found. Run via: uv run benchmark/run_hdfs_log_benchmark.py")
        sys.exit(1)

    db_path = ensure_hdfs_data(args)
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
        "# QueryConditionCache Benchmark Results — HDFS Log Analytics\n",
        f"**Settings:**{threads_note}{mem_note}\n",
        "**Dataset:** HDFS_v2 from Zenodo record 8196385 — 31 Hadoop log files,",
        "~71M log lines from a 32-node HDFS cluster (1 namenode + 31 datanodes).\n",
        f"**Methodology:** Each story runs on a single connection (cache persists across queries).",
        f"Baseline: {args.repeat} warm runs with cache OFF. Cached: {args.repeat} warm runs with cache ON.",
        "OS page caches are dropped between baseline and cached passes.",
        "**Speedup = avg(baseline) / avg(cache hit)**\n",
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
