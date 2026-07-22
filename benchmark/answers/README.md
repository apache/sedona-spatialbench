# SpatialBench ground-truth answers

Reference results for the SpatialBench queries, used by the correctness harness to
verify that every participating engine returns the same answer for the same query.

## Layout

```
answers/
  sf1/q1.parquet  q1.csv  ...  q12.parquet  q12.csv     # scale factor 1  (SF10 to follow)
```

Each query's expected result is committed in two formats, written from the same
normalized frame:

- **`q<n>.parquet`** — the type-faithful canonical answer (timestamps stay
  timestamps, ints stay ints). The correctness harness compares against this.
- **`q<n>.csv`** — a review companion: GitHub renders it as a table and diffs are
  readable when an answer changes.

Every query is bounded to at most 100 rows (see #124), so the fixtures are tiny.

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
