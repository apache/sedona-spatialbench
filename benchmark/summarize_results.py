#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""
Summarize benchmark results from multiple engines into a markdown report.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def load_results(results_dir: str, expected_engines: list[str] | None = None) -> dict:
    """Load all JSON result files from a directory.

    Supports two layouts:
    1. One file per engine (e.g., duckdb_results.json with all queries)
    2. One file per query (e.g., duckdb_q1_results.json with a single query)

    Per-query files are merged into a single suite per engine. If multiple files
    contain results for the same engine, their query results are combined.

    If expected_engines is provided, engines that were expected to run but have
    no results file will be included with all queries marked as 'not_started'.
    This handles the case where a runner was OOM-killed before uploading results.
    """
    results = {}
    results_path = Path(results_dir)

    for json_file in results_path.glob("*_results.json"):
        with open(json_file) as f:
            data = json.load(f)
            for suite in data.get("results", []):
                engine = suite["engine"]
                if engine not in results:
                    results[engine] = suite
                else:
                    # Merge query results from multiple files for the same engine
                    existing_queries = {r["query"] for r in results[engine].get("results", [])}
                    for r in suite.get("results", []):
                        if r["query"] not in existing_queries:
                            results[engine]["results"].append(r)
                            existing_queries.add(r["query"])
                        elif r.get("status") != "not_started":
                            # Replace not_started placeholder with actual result
                            results[engine]["results"] = [
                                r if existing["query"] == r["query"] else existing
                                for existing in results[engine]["results"]
                            ]

    # For expected engines with no results, create placeholder entries
    if expected_engines:
        # Determine the full query list from engines that did report results
        all_queries = set()
        scale_factor = None
        for engine_data in results.values():
            if scale_factor is None:
                scale_factor = engine_data.get("scale_factor", 1)
            for r in engine_data.get("results", []):
                all_queries.add(r["query"])

        # Default to q1-q12 if no engine reported any results
        if not all_queries:
            all_queries = {f"q{i}" for i in range(1, 13)}

        for engine in expected_engines:
            if engine not in results:
                results[engine] = {
                    "engine": engine,
                    "version": "unknown",
                    "scale_factor": scale_factor or 1,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "results": [
                        {
                            "query": q,
                            "status": "not_started",
                            "time_seconds": None,
                            "row_count": None,
                            "error_message": "Runner was killed before completing this query (likely OOM)",
                        }
                        for q in sorted(all_queries, key=lambda x: int(x[1:]))
                    ],
                }

    return results


def format_time(seconds: float | None) -> str:
    """Format time in seconds to a readable string."""
    if seconds is None:
        return "N/A"
    if seconds < 0.01:
        return "<0.01s"
    return f"{seconds:.2f}s"


def get_winner(query: str, data: dict, engines: list) -> str | None:
    """Get the fastest engine for a query."""
    times = {}
    for engine in engines:
        result = data.get(engine, {}).get(query, {})
        if result.get("status") == "success" and result.get("time_seconds") is not None:
            times[engine] = result["time_seconds"]

    if not times:
        return None
    return min(times, key=times.get)


def generate_markdown_summary(results: dict, output_file: str, query_timeout: int | None = None, runs: int | None = None) -> str:
    """Generate a markdown summary of benchmark results for GitHub Actions."""
    engines = sorted(results.keys())

    if not engines:
        markdown = "# 📊 SpatialBench Benchmark Results\n\n⚠️ No results found."
        with open(output_file, "w") as f:
            f.write(markdown)
        return markdown

    # Get scale factor from first result
    scale_factor = results[engines[0]].get("scale_factor", 1)
    timestamp = results[engines[0]].get("timestamp", datetime.now(timezone.utc).isoformat())

    # Collect all queries
    all_queries = set()
    for engine_data in results.values():
        for r in engine_data.get("results", []):
            all_queries.add(r["query"])
    all_queries = sorted(all_queries, key=lambda x: int(x[1:]))

    # Build result lookup
    data = {}
    for engine, engine_data in results.items():
        data[engine] = {}
        for r in engine_data.get("results", []):
            data[engine][r["query"]] = r

    # Get version info
    versions = {engine: results[engine].get("version", "unknown") for engine in engines}

    # Engine display names with icons
    engine_icons = {
        "sedonadb": "🌵 SedonaDB",
        "duckdb": "🦆 DuckDB",
        "geopandas": "🐼 GeoPandas",
        "spatial_polars": "🐻‍❄️ Spatial Polars",
        "pycanopy": "🌴 PyCanopy",
    }

    # Generate markdown
    lines = [
        "# 📊 SpatialBench Benchmark Results",
        "",
        "| Parameter | Value |",
        "|-----------|-------|",
        f"| **Scale Factor** | {scale_factor} |",
        f"| **Query Timeout** | {query_timeout}s |",
        f"| **Runs per Query** | {runs} |",
        f"| **Timestamp** | {timestamp} |",
        f"| **Queries** | {len(all_queries)} |",
        "",
        "## 🔧 Software Versions",
        "",
        "| Engine | Version |",
        "|--------|---------|",
    ]

    for engine in engines:
        icon_name = engine_icons.get(engine, engine.title())
        lines.append(f"| {icon_name} | `{versions[engine]}` |")

    # Main results table
    lines.extend([
        "",
        "## 🏁 Results Comparison",
        "",
        "| Query | " + " | ".join(engine_icons.get(e, e.title()) for e in engines) + " |",
        "|:------|" + "|".join(":---:" for _ in engines) + "|",
    ])

    # Add rows for each query with winner highlighting
    for query in all_queries:
        winner = get_winner(query, data, engines)
        row = f"| **{query.upper()}** |"
        for engine in engines:
            result = data.get(engine, {}).get(query, {})
            status = result.get("status", "N/A")
            if status == "success":
                time_val = result.get("time_seconds")
                time_str = format_time(time_val)
                if engine == winner:
                    row += f" **{time_str}** |"
                else:
                    row += f" {time_str} |"
            elif status == "timeout":
                row += " ⏱️ TIMEOUT |"
            elif status == "error":
                row += " ❌ ERROR |"
            elif status == "not_started":
                row += " 💀 OOM |"
            else:
                row += " — |"
        lines.append(row)

    # Win count and completion summary
    win_counts = {engine: 0 for engine in engines}
    completed_counts = {engine: 0 for engine in engines}
    total_queries = len(all_queries)
    for query in all_queries:
        winner = get_winner(query, data, engines)
        if winner:
            win_counts[winner] += 1
        for engine in engines:
            result = data.get(engine, {}).get(query, {})
            if result.get("status") == "success":
                completed_counts[engine] += 1

    lines.extend([
        "",
        "## 🥇 Performance Summary",
        "",
        "| Engine | Completed | Wins |",
        "|--------|:---------:|:----:|",
    ])

    for engine in sorted(engines, key=lambda e: win_counts[e], reverse=True):
        icon_name = engine_icons.get(engine, engine.title())
        wins = win_counts[engine]
        completed = completed_counts[engine]
        lines.append(f"| {icon_name} | {completed}/{total_queries} | {wins} |")

    # Detailed results section (collapsible)
    lines.extend([
        "",
        "## 📋 Detailed Results",
        "",
    ])

    for engine in engines:
        icon_name = engine_icons.get(engine, engine.title())
        lines.extend([
            f"<details>",
            f"<summary><b>{icon_name}</b> - Click to expand</summary>",
            "",
            "| Query | Time | Status | Rows |",
            "|:------|-----:|:------:|-----:|",
        ])

        for query in all_queries:
            result = data.get(engine, {}).get(query, {})
            time_str = format_time(result.get("time_seconds"))
            status = result.get("status", "N/A")
            rows = result.get("row_count")
            row_str = f"{rows:,}" if rows is not None else "—"

            status_emoji = {
                "success": "✅",
                "error": "❌",
                "timeout": "⏱️",
                "not_started": "💀",
            }.get(status, "❓")

            lines.append(f"| {query.upper()} | {time_str} | {status_emoji} | {row_str} |")

        lines.extend([
            "",
            "</details>",
            "",
        ])

    # Add error details if any
    has_errors = False
    error_lines = ["## ⚠️ Errors and Timeouts", ""]

    for engine in engines:
        engine_errors = []
        not_started_queries = []
        for query in all_queries:
            result = data.get(engine, {}).get(query, {})
            status = result.get("status")
            if status in ("error", "timeout"):
                error_msg = result.get("error_message", "No details available")
                # Truncate long error messages
                if len(error_msg) > 200:
                    error_msg = error_msg[:200] + "..."
                engine_errors.append(f"- **{query.upper()}**: `{error_msg}`")
            elif status == "not_started":
                not_started_queries.append(query.upper())

        if not_started_queries:
            engine_errors.append(
                f"- **{', '.join(not_started_queries)}**: "
                f"`Could not complete these queries, likely due to OOM (runner was killed)`"
            )

        if engine_errors:
            has_errors = True
            icon_name = engine_icons.get(engine, engine.title())
            error_lines.append(f"### {icon_name}")
            error_lines.append("")
            error_lines.extend(engine_errors)
            error_lines.append("")

    if has_errors:
        lines.extend(error_lines)

    # Footer
    lines.extend([
        "---",
        "",
        "| Legend | Meaning |",
        "|--------|---------|",
        "| **bold** | Fastest for this query |",
        "| ⏱️ TIMEOUT | Query exceeded timeout |",
        "| ❌ ERROR | Query failed |",
        "| 💀 OOM | Could not run, likely due to out-of-memory (runner killed) |",
        "",
        f"*Generated by [SpatialBench](https://github.com/apache/sedona-spatialbench) on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*",
    ])

    markdown = "\n".join(lines)

    # Write to file
    with open(output_file, "w") as f:
        f.write(markdown)

    return markdown


def main():
    parser = argparse.ArgumentParser(
        description="Summarize SpatialBench benchmark results"
    )
    parser.add_argument(
        "--results-dir",
        type=str,
        required=True,
        help="Directory containing *_results.json files",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmark_summary.md",
        help="Output markdown file",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Query timeout in seconds (for reporting)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per query (for reporting)",
    )
    parser.add_argument(
        "--engines",
        type=str,
        default=None,
        help="Comma-separated list of expected engines (e.g., 'duckdb,geopandas,sedonadb,spatial_polars,pycanopy'). "
        "Engines that were expected but have no results will be shown as OOM/runner-killed.",
    )

    args = parser.parse_args()

    expected_engines = [e.strip() for e in args.engines.split(",")] if args.engines else None
    results = load_results(args.results_dir, expected_engines=expected_engines)

    if not results:
        print(f"No results found in {args.results_dir}")
        # Write empty summary
        with open(args.output, "w") as f:
            f.write("# SpatialBench Benchmark Results\n\nNo results found.")
        return

    markdown = generate_markdown_summary(results, args.output, args.timeout, args.runs)
    print(f"Summary written to {args.output}")
    print("\nPreview:")
    print("-" * 60)
    print(markdown[:2000])
    if len(markdown) > 2000:
        print("...")


if __name__ == "__main__":
    main()
