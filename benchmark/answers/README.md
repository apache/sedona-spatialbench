<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# SpatialBench ground-truth answers

Reference results for the SpatialBench queries. A downstream CI job
(`verify_results.py`) reuses the benchmark run's result dumps — instead of
re-executing any query — and compares each engine's result against these answers.
A mismatch **fails CI**.

## Layout

```
benchmark/answers/
  sf1/                          # scale factor 1
    q1.parquet   q1.csv
    q2.parquet   q2.csv
    ...
    q12.parquet  q12.csv
  sf10/                         # scale factor 10
    q1.parquet   q1.csv
    ...
    q12.parquet  q12.csv
```

Each query's expected result is committed in two formats, written from the same
normalized frame:

- **`q<n>.parquet`** — the type-faithful canonical answer (timestamps stay
  timestamps, ints stay ints). The verify job reads this to know each column's type,
  so comparison semantics come from the schema: integer keys/counts, timestamps and
  strings are matched exactly, and only floating-point metrics use tolerance.
- **`q<n>.csv`** — a review companion: GitHub renders it as a table and diffs are
  readable when an answer changes.

The benchmark run dumps each engine's result to a csv (never parquet), so no pyarrow
filesystem is touched inside a SedonaDB worker; the engine-free verify job then reads
the answer parquet (types) and that csv (values) and compares them.

Most queries are bounded to at most 100 rows (see #124), so the fixtures are tiny. The one
exception is **Q4**, which groups the top-1000 tipped trips by zone and returns one row per
zone (~260 at SF1, ~240 at SF10); it is inherently small and bounded (never more than the
number of zones touched by 1000 trips) but is not capped at 100.

## How they are generated

- **SedonaDB is the reference oracle** — the answers are the output of the canonical
  SedonaDB dialect on the SpatialBench dataset, produced by the benchmark run in CI (the
  `--result-dir` dumps), not on a local machine.
- **SF1** was generated with SedonaDB 0.4.0.
- **SF10** was generated with SedonaDB 0.3.0. SedonaDB 0.4.0 cannot compute Q5 at SF10 —
  its grouped convex-hull aggregation spills >100 GB and aborts
  ([apache/sedona-db#1077](https://github.com/apache/sedona-db/issues/1077)) — while 0.3.0
  computes it in modest space. The two versions produce identical results for the other 11
  queries at SF10, so the values are version-independent; only Q5's feasibility differs.
- **Cross-check:** the correctness verify job runs every participating engine against these
  answers, so each committed answer is independently validated by the other engines (e.g.
  DuckDB) wherever they can compute the query.

## Canonical, engine-neutral form

Engines represent some types differently, so answers are normalized before writing:

- **Durations/intervals → total seconds** (float), with a `_seconds` column suffix
  (e.g. `avg_duration` → `avg_duration_seconds`).
- **Decimals → float**.
- **Timestamps → `datetime`** (preserved as timestamps in parquet; ISO-8601 in csv).

Row order is significant and preserved: every query has a deterministic `ORDER BY`
(with key tiebreakers) followed by `LIMIT`, so the expected rows and their order are
well defined.

## Comparison semantics

Columns are compared by position (engines name columns differently, e.g.
`avg_duration` vs `avg_duration_seconds`), with:

- Integer keys, strings, and timestamps: exact match.
- Floats (distances, areas, IoU, seconds): relative + absolute tolerance
  (`rtol=1e-6`, `atol=1e-9`) to absorb cross-engine floating-point differences.
- The final row may legitimately differ across engines when a float metric ties near
  the `LIMIT` boundary; a within-tolerance boundary difference is treated as a pass.

An engine that cannot compute a query (timeout / error / OOM, e.g. DuckDB's Q12
below) is reported but does **not** fail the job — that is a runtime issue surfaced
by the benchmark summary, not a wrong answer. Only an actual mismatch fails CI.

## Caveat: Q12 at SF1

DuckDB has no KNN operator, so its Q12 uses a lateral cross-join that is infeasible at
SF1 (it does not finish in reasonable time). Q12 is therefore **not** cross-checked by
DuckDB here — it is validated by the KNN-capable engines (SedonaDB, Spatial Polars,
PyCanopy). In the verify job, DuckDB's Q12 shows as "could not compute" rather than a
failure.
