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

Reference results for the SpatialBench queries, used by the correctness harness to
verify that every participating engine returns the same answer for the same query.

## Layout

```
benchmark/answers/
  sf1/                          # scale factor 1  (SF10 to follow)
    q1.parquet   q1.csv
    q2.parquet   q2.csv
    ...
    q12.parquet  q12.csv
```

Each query's expected result is committed in two formats, written from the same
normalized frame:

- **`q<n>.parquet`** — the type-faithful canonical answer (timestamps stay
  timestamps, ints stay ints). The correctness harness compares against this.
- **`q<n>.csv`** — a review companion: GitHub renders it as a table and diffs are
  readable when an answer changes.

Most queries are bounded to at most 100 rows (see #124), so the fixtures are tiny. The one
exception is **Q4**, which groups the top-1000 tipped trips by zone and returns one row per
zone (~260 at SF1); it is inherently small and bounded (never more than the number of zones
touched by 1000 trips) but is not capped at 100.

## How they are generated

- **SedonaDB is the reference oracle** — the answers are the output of the canonical
  SedonaDB dialect on the SF1 dataset.
- **DuckDB independently cross-checks** every query it can run; an answer is only
  blessed when DuckDB agrees with SedonaDB within a small float tolerance
  (`rtol=1e-6`), so the committed answers are validated by two independent engines.

## Canonical, engine-neutral form

Engines represent some types differently, so answers are normalized before writing:

- **Durations/intervals → total seconds** (float), with a `_seconds` column suffix
  (e.g. `avg_duration` → `avg_duration_seconds`).
- **Decimals → float**.
- **Timestamps → `datetime`** (preserved as timestamps in parquet; ISO-8601 in csv).

Row order is significant and preserved: every query has a deterministic `ORDER BY`
(with key tiebreakers) followed by `LIMIT`, so the expected rows and their order are
well defined.

## Comparison semantics (for the harness)

- Integer keys, strings, and timestamps: exact match.
- Floats (distances, areas, IoU, seconds): relative + absolute tolerance
  (`rtol=1e-6`, `atol=1e-9`) to absorb cross-engine floating-point differences.
- The final row may legitimately differ across engines when a float metric ties near
  the `LIMIT` boundary; the harness treats a within-tolerance boundary difference as a
  pass.

## Caveat: Q12 at SF1

DuckDB has no KNN operator, so its Q12 uses a lateral cross-join that is infeasible at
SF1 (it does not finish in reasonable time). Q12 is therefore **not** cross-checked by
DuckDB here — it is validated by the KNN-capable engines (SedonaDB, Spatial Polars,
PyCanopy) in the correctness harness.
