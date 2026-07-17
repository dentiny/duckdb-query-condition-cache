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

Each invocation runs exactly one measurement mode:
  cold-baseline   -- purge page cache, then run with condition cache disabled
  cold-build      -- purge, then time a cache-miss query that auto-builds
  cold-cache-hit  -- build the condition cache, purge page cache, then run
  warm-baseline   -- purge, run one normal warm-up query, then run cache-off
  warm-build      -- purge, warm once cache-off, then time an auto-build query
  warm-cache-hit  -- purge, run one normal warm-up query, build, then run cached

Usage:
    python benchmark/run_hdfs_log_benchmark.py --mode MODE [--repeat N] [--stories 1,2,3]
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
    r"^(\d{4}-\d{2}-\d{2})\s+"  # date
    r"(\d{2}:\d{2}:\d{2}),\d{3}\s+"  # time (drop millis for grouping)
    r"(\w+)\s+"  # level (INFO, WARN, ERROR, etc.)
    r"([^:]+):\s+"  # component (java class / short name)
    r"(.*)$"  # message content
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

QUERY_PREDICATES = {
    "S1-W1: Block Event Count": "Content LIKE '%blk_%'",
    "S1-W2: Block Events by Level": "Content LIKE '%blk_%'",
    "S1-W3: Block Events by Component": "Content LIKE '%blk_%'",
    "S1-W4: Block + Replication": "Content LIKE '%blk_%' AND Content LIKE '%replicas%'",
    "S2-Q1: addStoredBlock events": "Content LIKE '%addStoredBlock%'",
    "S2-Q2: addStoredBlock + specific IP": (
        "Content LIKE '%addStoredBlock%' AND Content LIKE '%10.10.34.11%'"
    ),
    "S2-Q3: addStoredBlock aggregate by date": (
        "Content LIKE '%addStoredBlock%' AND Content LIKE '%10.10.34.11%'"
    ),
    "S3-Q1: WARN + ERROR logs": "Level = 'WARN' OR Level = 'ERROR'",
    "S3-Q2: Exceptions in content": "Content LIKE '%Exception%'",
    "S3-Q3: Exception + block correlation": (
        "Content LIKE '%Exception%' AND Content LIKE '%blk_%'"
    ),
}

MODES = (
    "cold-baseline",
    "cold-build",
    "cold-cache-hit",
    "warm-baseline",
    "warm-build",
    "warm-cache-hit",
)


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
        print(
            "Parsing raw HDFS logs into CSV (this may take a few minutes)...",
            flush=True,
        )
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
                            out.write(
                                f'{date},{time_str},{level},"{component}","{content}","{source}"\n'
                            )
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
    con = duckdb.connect(str(db_path))

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


def require_local_extension() -> Path:
    """Return the local extension path or fail before running the benchmark."""
    if not EXT_PATH.is_file():
        raise FileNotFoundError(
            f"Local QueryConditionCache extension not found at {EXT_PATH}. "
            "Build it first with `GEN=ninja make`."
        )
    return EXT_PATH


def drop_os_caches():
    """Drop the OS page cache or fail rather than silently mislabeling a run."""
    if platform.system() == "Darwin":
        command = ["sudo", "-n", "purge"]
    elif platform.system() == "Linux":
        command = [
            "sudo",
            "-n",
            "sh",
            "-c",
            "sync && echo 3 > /proc/sys/vm/drop_caches",
        ]
    else:
        raise RuntimeError(
            f"Page-cache purge is not implemented for {platform.system()}"
        )

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Timed out while purging the OS page cache") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise RuntimeError(
            "Failed to purge the OS page cache. Run `sudo -v` before the benchmark. "
            f"Command output: {detail}"
        )


def open_connection(db_path: Path, args):
    """Open a fresh DuckDB connection with the extension loaded."""
    import duckdb

    extension_path = require_local_extension()
    cfg: dict = {"allow_unsigned_extensions": True}
    if args.threads:
        cfg["threads"] = args.threads
    if args.memory_limit:
        cfg["memory_limit"] = args.memory_limit

    con = duckdb.connect(str(db_path), config=cfg)
    con.execute(f"LOAD '{extension_path}';")
    return con


def time_query_once(con, sql: str) -> float:
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return (time.perf_counter() - t0) * 1000


def verify_cache_entry(con, predicate: str):
    total_row_groups = con.execute(
        "SELECT total_row_groups FROM condition_cache_info(?, ?)",
        ["logs", predicate],
    ).fetchone()[0]
    if total_row_groups == 0:
        raise RuntimeError(
            f"Cache build produced no cached row groups for: {predicate}"
        )


def build_cache(con, predicate: str):
    con.execute(
        "SELECT * FROM condition_cache_build(?, ?)",
        ["logs", predicate],
    ).fetchall()
    verify_cache_entry(con, predicate)


def selected_query_cases(story_ids: list[int]) -> list[tuple[str, str, str]]:
    cases = []
    for story_id in story_ids:
        _, queries = STORIES[story_id]
        for label, sql in queries:
            cases.append((label, sql, QUERY_PREDICATES[label]))
    return cases


def measure_once(db_path: Path, args, mode: str, sql: str, predicate: str) -> float:
    # Every sample starts from a purged OS page cache and a fresh connection.
    drop_os_caches()
    con = open_connection(db_path, args)
    try:
        con.execute("SET use_query_condition_cache = false;")

        if mode == "cold-baseline":
            return time_query_once(con, sql)

        if mode == "cold-build":
            con.execute("SET use_query_condition_cache = true;")
            elapsed_ms = time_query_once(con, sql)
            verify_cache_entry(con, predicate)
            return elapsed_ms

        if mode == "cold-cache-hit":
            build_cache(con, predicate)
            con.execute("SET use_query_condition_cache = true;")
            drop_os_caches()
            return time_query_once(con, sql)

        # The warm-state definition is shared by all warm modes: after the
        # purge, run one normal query with the condition cache disabled.
        time_query_once(con, sql)

        if mode == "warm-baseline":
            return time_query_once(con, sql)

        if mode == "warm-build":
            con.execute("SET use_query_condition_cache = true;")
            elapsed_ms = time_query_once(con, sql)
            verify_cache_entry(con, predicate)
            return elapsed_ms

        if mode == "warm-cache-hit":
            build_cache(con, predicate)
            con.execute("SET use_query_condition_cache = true;")
            return time_query_once(con, sql)

        raise ValueError(f"Unknown benchmark mode: {mode}")
    finally:
        con.close()


def run_mode(db_path: Path, args, story_ids: list[int]) -> list[dict]:
    cases = selected_query_cases(story_ids)
    results = []
    print(f"\nRunning mode: {args.mode}", flush=True)
    for label, sql, predicate in cases:
        short = label.split(":")[0]
        times = []
        print(f"  {short}", flush=True)
        for sample in range(args.repeat):
            elapsed_ms = measure_once(db_path, args, args.mode, sql, predicate)
            times.append(elapsed_ms)
            print(
                f"    sample {sample + 1}/{args.repeat}: {elapsed_ms:.1f} ms",
                flush=True,
            )
        avg_ms = statistics.mean(times)
        std_ms = statistics.stdev(times) if len(times) > 1 else 0.0
        results.append(
            {
                "label": label,
                "predicate": predicate,
                "avg_ms": avg_ms,
                "std_ms": std_ms,
                "runs_ms": times,
            }
        )
        print(f"    average: {avg_ms:.1f} ± {std_ms:.1f} ms", flush=True)
    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def format_table(mode: str, results: list[dict], repeat: int) -> str:
    lines = [
        f"## {mode} (avg ± std of {repeat} independent samples)\n",
        "| Query | Predicate | Time (ms) | Samples (ms) |",
        "|-------|-----------|----------:|--------------|",
    ]
    for r in results:
        samples = ", ".join(f"{value:.1f}" for value in r["runs_ms"])
        lines.append(
            f"| {r['label']} | `{r['predicate']}` "
            f"| {r['avg_ms']:.1f} ± {r['std_ms']:.1f} | {samples} |"
        )
    lines.append("")
    return "\n".join(lines)


def plot_results(results: list[dict], mode: str, out_path: Path):
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib/numpy not available, skipping chart generation.")
        return

    if not results:
        return
    labels = [r["label"].split(":")[0] for r in results]
    averages = [r["avg_ms"] for r in results]
    deviations = [r["std_ms"] for r in results]
    x = np.arange(len(labels))

    _, ax = plt.subplots(figsize=(max(7, len(labels) * 1.2), 6))
    ax.bar(x, averages, yerr=deviations, capsize=4, color="#4C78A8", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Time (ms)")
    ax.set_title(f"QueryConditionCache — HDFS {mode} (avg ± std)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {out_path}")
    plt.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="QueryConditionCache HDFS Log Analytics benchmark"
    )
    parser.add_argument(
        "--mode",
        choices=MODES,
        required=True,
        help="Run exactly one cache/page-state measurement mode.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Number of independent measured samples (default: 3)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output file for results (default: auto-generated)",
    )
    parser.add_argument(
        "--stories",
        type=str,
        default=None,
        help="Comma-separated story numbers to run (e.g. '1,2'). Default: all.",
    )
    parser.add_argument("--no-chart", action="store_true", help="Skip chart generation")
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force re-download/reload of HDFS log data",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of DuckDB threads (default: auto)",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default=None,
        help="DuckDB memory limit (e.g. '4GB')",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default=None,
        help="Name prefix for output files",
    )
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")

    # Build output filename
    if args.out:
        out_file = args.out
    else:
        parts = ["hdfs_logs", args.mode]
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
        print(
            "ERROR: duckdb Python package not found. Run via: uv run benchmark/run_hdfs_log_benchmark.py"
        )
        sys.exit(1)

    require_local_extension()
    db_path = ensure_hdfs_data(args)
    print("Data ready.", flush=True)

    # Select stories
    if args.stories:
        selected = [int(s.strip()) for s in args.stories.split(",")]
    else:
        selected = sorted(STORIES.keys())
    invalid_stories = [story_id for story_id in selected if story_id not in STORIES]
    if invalid_stories:
        parser.error(f"Unknown story IDs: {invalid_stories}")

    results = run_mode(db_path, args, selected)

    # Build markdown output
    settings = ", ".join(
        item
        for item in [
            f"threads={args.threads}" if args.threads else "threads=auto",
            (
                f"memory_limit={args.memory_limit}"
                if args.memory_limit
                else "memory_limit=auto"
            ),
        ]
    )
    lines = [
        f"# QueryConditionCache HDFS Benchmark — {args.mode}\n",
        f"**Settings:** {settings}\n",
        "**Dataset:** HDFS_v2 from Zenodo record 8196385 — 31 Hadoop log files,",
        "~71M log lines from a 32-node HDFS cluster (1 namenode + 31 datanodes).\n",
        f"**Methodology:** Mode `{args.mode}`; {args.repeat} independent samples per query.",
        "Every sample starts with an OS page-cache purge and a fresh DuckDB connection.",
    ]
    if args.mode.startswith("warm-"):
        lines.append(
            "Warm state is established by one untimed normal query with the condition cache disabled.\n"
        )
    elif args.mode == "cold-cache-hit":
        lines.append(
            "The condition cache is built first, then the OS page cache is purged before measurement.\n"
        )
    if args.mode in ("cold-build", "warm-build"):
        lines.append(
            "The measured query starts with the condition cache enabled and no matching entry; "
            "its latency includes automatic cache construction and execution of the original query.\n"
        )
    lines.append(format_table(args.mode, results, args.repeat))

    output = "\n".join(lines)
    print("\n" + output)

    with open(out_file, "w") as f:
        f.write(output)
    print(f"\nResults written to {out_file}")

    if not args.no_chart and results:
        chart_path = Path(out_file).with_suffix(".png")
        plot_results(results, args.mode, chart_path)


if __name__ == "__main__":
    main()
