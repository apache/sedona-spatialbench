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
Verify SpatialBench correctness by comparing the benchmark run's result dumps
against the committed ground-truth answers, and render a summary table.

This runs as a downstream CI job that *needs* the benchmark jobs and reuses their
output: the benchmark run writes each query's normalized result to
``<engine>_<query>_result.csv`` (see run_benchmark.py --result-dir); this script
reads those dumps plus the committed answers in ``benchmark/answers/sf<sf>`` and
compares them. It is engine-free (only pandas/numpy/pyarrow), so none of the
SedonaDB / pyarrow import-order pitfalls apply here.

Comparison semantics come from the canonical answer schema — the committed
``q<n>.parquet``, which is type-faithful:
  * columns compared by position (engines name e.g. avg_duration differently)
  * integer keys / counts: exact
  * timestamps: exact (parsed, format-independent)
  * strings: exact
  * floating-point metrics (distances, areas, IoU, seconds): rtol=1e-6, atol=1e-9
  * a within-tolerance tie confined to the final row of a *capped* LIMIT query is
    tolerated (float ordering metrics can tie across the LIMIT boundary and swap
    the last row's identity); this exception never applies to non-LIMIT queries,
    to results below the cap, or when a numeric metric actually differs.

Exit status: non-zero if any engine's result MISMATCHES its committed answer, or a
result that should exist is missing — so a regression fails CI. An engine that
explicitly could not compute a query (timeout / error / OOM, e.g. DuckDB's
lateral-join Q12 at scale) is reported but does not fail the job.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

QUERY_COUNT = 12
ENGINES = ["duckdb", "geopandas", "sedonadb", "spatial_polars", "pycanopy"]
ENGINE_ICONS = {
    "sedonadb": "🌵 SedonaDB",
    "duckdb": "🦆 DuckDB",
    "geopandas": "🐼 GeoPandas",
    "spatial_polars": "🐻‍❄️ Spatial Polars",
    "pycanopy": "🌴 PyCanopy",
}

# Queries eligible for the LIMIT-boundary-tie tolerance (see #124): bounded by
# ORDER BY <float metric> ... LIMIT, so two rows can tie on the float metric within
# tolerance and swap the final row's identity. Q8 is deliberately excluded — it
# orders by an integer count (nearby_pickup_count) with an integer-key tiebreaker,
# so its ordering is fully deterministic and must match exactly.
LIMIT_QUERIES = {"q1", "q5", "q7", "q9", "q10", "q12"}
LIMIT_CAP = 100

# Duration columns are named with this suffix (see run_benchmark.normalize). SedonaDB
# truncates interval averages to milliseconds, so allow a ~1 ms absolute tolerance on
# them; every other float metric uses the tight default.
DURATION_SUFFIX = "_seconds"
DURATION_ATOL = 1e-3

# Verdicts that must fail CI: a wrong value, a result that reported success but is
# missing, or a result file that exists but cannot be read/compared.
FAILING = {"fail", "missing", "verify_error"}

VERDICT_SYMBOL = {
    "pass": "✅",
    "fail": "❌",
    "missing": "🚫",
    "timeout": "⏱️",
    "oom": "💀",
    "run_error": "⚠️",
    "verify_error": "❌",
    "no_data": "❔",
    "no_answer": "—",
}


def column_kind(dtype) -> str:
    """Classify a canonical (answer parquet) column dtype for comparison."""
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    return "exact"


def compare(answer: pd.DataFrame, result: pd.DataFrame, rtol: float, atol: float) -> list[dict]:
    """Positional comparison. ``answer`` is the type-faithful parquet frame, whose
    per-column dtype decides the semantics; ``result`` is the engine's csv dump.

    Returns a list of structured issues (kind / first_row / count / message). Only
    floating-point columns use tolerance — integer keys, timestamps and strings are
    matched exactly, so e.g. key 1451371 vs 1451372.0 is a mismatch, not a tie.
    """
    if answer.shape[1] != result.shape[1]:
        return [{
            "kind": "shape", "first_row": -1, "count": 0,
            "message": (f"column count differs: answer {answer.shape[1]} "
                        f"({list(answer.columns)}) vs engine {result.shape[1]} ({list(result.columns)})"),
        }]
    issues = []
    if len(answer) != len(result):
        issues.append({
            "kind": "shape", "first_row": -1, "count": abs(len(answer) - len(result)),
            "message": f"row count differs: answer {len(answer)} vs engine {len(result)}",
        })
    n = min(len(answer), len(result))
    for i in range(answer.shape[1]):
        name = answer.columns[i]
        kind = column_kind(answer.dtypes.iloc[i])
        a = answer.iloc[:n, i].reset_index(drop=True)
        b = result.iloc[:n, i].reset_index(drop=True)
        if kind == "float":
            # SedonaDB truncates interval averages to milliseconds, so a duration
            # column (named *_seconds) can differ from a full-precision engine by up
            # to ~1 ms. Allow that as an absolute tolerance on duration columns; other
            # float metrics keep the tight default.
            col_atol = max(atol, DURATION_ATOL) if str(name).endswith(DURATION_SUFFIX) else atol
            va = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
            vb = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)
            bad = ~np.isclose(va, vb, rtol=rtol, atol=col_atol, equal_nan=True)
        elif kind == "int":
            va = pd.to_numeric(a, errors="coerce")
            vb = pd.to_numeric(b, errors="coerce")
            bad = ((va != vb) & ~(va.isna() & vb.isna())).to_numpy()
        elif kind == "datetime":
            va = pd.to_datetime(a, errors="coerce")
            vb = pd.to_datetime(b, errors="coerce")
            bad = ((va != vb) & ~(va.isna() & vb.isna())).to_numpy()
        else:  # exact string match
            sa = a.astype("string").fillna("<NA>")
            sb = b.astype("string").fillna("<NA>")
            bad = (sa != sb).to_numpy()
        count = int(bad.sum())
        if count:
            j = int(np.where(bad)[0][0])
            issues.append({
                "kind": kind, "first_row": j, "count": count, "col": i, "name": str(name),
                "message": (f"col {i} ('{name}', {kind}): {count}/{n} mismatch "
                            f"(first at row {j}: {a.iloc[j]!r} vs {b.iloc[j]!r})"),
            })
    return issues


def is_boundary_tie(query: str, issues: list[dict], answer: pd.DataFrame) -> bool:
    """True only for a legitimate LIMIT-boundary tie: an eligible LIMIT query, at the
    cap, whose only discrepancy is the final row's identity (int key / string), with
    every floating-point ordering metric still tying within tolerance.
    """
    if not issues:
        return False
    if query not in LIMIT_QUERIES:
        return False
    if len(answer) != LIMIT_CAP:
        return False
    last = len(answer) - 1
    for iss in issues:
        if iss["kind"] == "shape":
            return False
        # every discrepancy must be exactly one differing value, on the final row
        if iss["first_row"] != last or iss["count"] != 1:
            return False
        # a numeric ordering metric that differs is not a tie — it is a wrong answer
        if iss["kind"] == "float":
            return False
    return True


def load_statuses(results_dir: Path) -> dict:
    """Map (engine, query) -> benchmark run status, from the timing JSONs, so a
    timeout/error/OOM (couldn't run) is distinguished from a missing dump."""
    statuses = {}
    for jf in results_dir.glob("*_results.json"):
        try:
            data = json.loads(jf.read_text())
        except Exception:
            continue
        for suite in data.get("results", []):
            engine = suite.get("engine")
            for r in suite.get("results", []):
                statuses[(engine, r.get("query"))] = r.get("status")
    return statuses


def verify_one(results_dir: Path, answers_dir: Path, engine: str, query: str,
               status: str | None, rtol: float, atol: float) -> tuple[str, str | None]:
    """Return (verdict, detail). Verdicts in FAILING ('fail', 'missing') fail CI."""
    answer_pq = answers_dir / f"{query}.parquet"
    result_csv = results_dir / f"{engine}_{query}_result.csv"
    if not answer_pq.exists():
        return "no_answer", None
    if not result_csv.exists():
        # No result to check.
        #  - explicit runtime failure (timeout/error/OOM): tolerated, the engine
        #    could not compute the query;
        #  - the engine reported SUCCESS but produced no dump: the dump silently
        #    failed, which must fail CI (otherwise the gate passes without checking);
        #  - no record at all (the framework's whole benchmark job failed, e.g. an
        #    install error): reported but not a correctness failure — the framework's
        #    own red job surfaces it, and it left no answer that could be "wrong".
        if status == "timeout":
            return "timeout", None
        if status == "not_started":
            return "oom", None
        if status == "error":
            return "run_error", None
        if status == "success":
            return "missing", "benchmark reported success but produced no result (dump failed)"
        return "no_data", "framework produced no result (its benchmark job failed)"
    # Reading/comparing one engine's dump must never take down the whole summary:
    # a bad or unreadable result becomes a ⚠️ cell (not a crash, not a hard failure —
    # the engine's own benchmark job surfaces the underlying problem).
    try:
        answer = pd.read_parquet(answer_pq)
        result = pd.read_csv(result_csv)
        issues = compare(answer, result, rtol, atol)
    except Exception as e:
        return "verify_error", f"could not read/compare result: {e}"
    if not issues:
        return "pass", None
    if is_boundary_tie(query, issues, answer):
        return "pass", "boundary tie tolerated"
    return "fail", "; ".join(i["message"] for i in issues[:3])


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify engine correctness against committed answers")
    parser.add_argument("--results-dir", required=True,
                        help="Directory of result dumps (<engine>_<query>_result.csv) and timing JSONs")
    parser.add_argument("--answers-dir", default=None,
                        help="Committed answers directory (default: benchmark/answers/sf<sf>)")
    parser.add_argument("--scale-factor", required=True, help="Scale factor being verified")
    parser.add_argument("--engines", default=",".join(ENGINES))
    parser.add_argument("--queries", default=None, help="Comma list, e.g. q1,q6 (default all 12)")
    parser.add_argument("--rtol", type=float, default=1e-6)
    parser.add_argument("--atol", type=float, default=1e-9)
    parser.add_argument("--output", default="correctness_summary.md")
    args = parser.parse_args()

    sf = args.scale_factor
    sf_tag = f"sf{int(float(sf)) if float(sf).is_integer() else sf}"
    sf_display = int(float(sf)) if float(sf).is_integer() else sf
    results_dir = Path(args.results_dir)
    answers_dir = Path(args.answers_dir) if args.answers_dir else Path(__file__).parent / "answers" / sf_tag

    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    queries = ([q.strip() for q in args.queries.split(",")] if args.queries
               else [f"q{i}" for i in range(1, QUERY_COUNT + 1)])

    # No committed answers for this scale factor -> nothing to verify, nothing to gate.
    if not answers_dir.is_dir():
        msg = (f"# ✅ SpatialBench Correctness (SF{sf_display})\n\n"
               f"No committed answers at `{answers_dir}`, so correctness was not checked "
               f"for scale factor {sf_display}.\n")
        Path(args.output).write_text(msg)
        print(msg)
        return 0

    statuses = load_statuses(results_dir)

    # verdicts[engine][query] = (verdict, detail)
    verdicts = {}
    for engine in engines:
        verdicts[engine] = {}
        for query in queries:
            verdicts[engine][query] = verify_one(
                results_dir, answers_dir, engine, query,
                statuses.get((engine, query)), args.rtol, args.atol,
            )

    failures = [
        (engine, query, verdict, detail)
        for engine in engines
        for query, (verdict, detail) in verdicts[engine].items()
        if verdict in FAILING
    ]

    # Number of (engine, query) pairs actually compared against an answer. If this is
    # zero the gate verified nothing — a total artifact-download failure or a fully
    # broken benchmark — and must not pass silently.
    compared = sum(
        1
        for engine in engines
        for _q, (verdict, _d) in verdicts[engine].items()
        if verdict in ("pass", "fail")
    )
    verified_nothing = compared == 0

    # ── Build the markdown summary ──
    lines = [
        f"# ✅ SpatialBench Correctness (SF{sf_display})",
        "",
        f"Each engine's result compared against the committed ground-truth answers in "
        f"[`benchmark/answers/{sf_tag}`](../tree/main/benchmark/answers/{sf_tag}). "
        "A mismatch (❌), a missing result (🚫), or an unreadable result (⚠️) fails this job.",
        "",
    ]
    if verified_nothing:
        lines += [
            "> ❌ **No engine results were verified** — the benchmark artifacts are "
            "missing or unreadable (likely a download failure). Failing the job rather "
            "than passing without checking anything.",
            "",
        ]
    lines += [
        "| Query | " + " | ".join(ENGINE_ICONS.get(e, e.title()) for e in engines) + " |",
        "|:------|" + "|".join(":---:" for _ in engines) + "|",
    ]
    for query in queries:
        row = f"| **{query.upper()}** |"
        for engine in engines:
            verdict, _ = verdicts[engine][query]
            row += f" {VERDICT_SYMBOL.get(verdict, '❓')} |"
        lines.append(row)

    # Per-engine tally
    lines.extend([
        "",
        "| Engine | ✅ Correct | ❌ Failed | Not verified |",
        "|--------|:---------:|:---------:|:------------:|",
    ])
    for engine in engines:
        correct = sum(1 for v, _ in verdicts[engine].values() if v == "pass")
        failed = sum(1 for v, _ in verdicts[engine].values() if v in FAILING)
        not_verified = len(queries) - correct - failed
        lines.append(f"| {ENGINE_ICONS.get(engine, engine.title())} | {correct} | {failed} | {not_verified} |")

    if failures:
        lines.extend(["", "## ❌ Failures", ""])
        for engine, query, verdict, detail in failures:
            sym = VERDICT_SYMBOL.get(verdict, "❌")
            lines.append(f"- {sym} **{ENGINE_ICONS.get(engine, engine.title())} {query.upper()}**: {detail}")

    lines.extend([
        "",
        "| Legend | Meaning |",
        "|--------|---------|",
        "| ✅ | Matches the committed answer |",
        "| ❌ | Does not match, or the result exists but could not be read/compared — fails CI |",
        "| 🚫 | Reported success but produced no result (dump failed) — fails CI |",
        "| ⏱️ | Engine could not compute the query in time (not a failure) |",
        "| 💀 | Runner killed before this query ran, likely OOM (not a failure) |",
        "| ⚠️ | Engine errored running the query (not a failure) |",
        "| ❔ | Framework produced no result — its benchmark job failed; see the benchmark summary (not a failure) |",
        "| — | No committed answer for this query |",
        "",
        f"*Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*",
    ])

    markdown = "\n".join(lines)
    Path(args.output).write_text(markdown)
    print(markdown)

    if verified_nothing:
        print("\nFAILED: no engine results were verified (artifacts missing/unreadable — "
              "possible download failure).", file=sys.stderr)
        return 1
    if failures:
        print(f"\nFAILED: {len(failures)} (engine, query) result(s) did not match, were "
              f"missing, or could not be read.", file=sys.stderr)
        return 1
    print("\nAll engine results match the committed answers (where a result was produced).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
