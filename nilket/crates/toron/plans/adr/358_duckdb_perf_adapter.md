# ADR 358 - the DuckDB perf adapter (external reference: data parity + engine-aware summary)

## Status
Proposed.

## Context

The next step on the 356 perf-rail arc - a **live DuckDB adapter** so the rail carries an external reference before we
interpret sf=10 walls. Steer: **Parquet** export / **CLI shell-out, not_attempted-if-absent** / **row-count sanity now,
full cross-engine digest later**. Three parts, in order: **(A) make the perf summary engine-aware, (B) export the exact
Toron data with a manifest, (C) the DuckDB CLI adapter.** No optimization, no Toron engine change, no full cross-engine
oracle.

## Decision

### A. Engine-aware perf summary (FIRST - the prerequisite)
`PerfAttempt.engine` exists, but the summary doesn't use it: `summarize_query_rows` **hardcodes `"engine":"toron"`**
(main.rs:612), `summarize_perf_rows` groups by query, and the baseline lookup is query-keyed. Adding DuckDB rows without
fixing this **collides Toron and DuckDB**. So first: **key summaries and baseline lookup by `(engine, query)`**,
un-hardcode the engine, and render a board that holds both engines per query. This lands before any DuckDB row exists.

### B. Data parity - export the exact Toron tables (the crux)
DuckDB must read the **exact data Toron ran**, never its own `dbgen` (different rows -> invalid comparison).
- Export the gauge's generated tables - **same query table helper, same mode/sf** Toron used - to **Parquet** under
  `CACHE_ROOT` (e.g. `target/tpch/parquet/<mode>/sf<N>/<table>.parquet`). Adds a **Parquet dependency to `toron-tpch`
  only** (the repo has none today).
- Write an **export manifest**: table names, row counts, **schema fingerprint**, **file hashes**, mode, sf - so data
  parity is *verifiable*, not assumed.
- **Type-parity is a Phase 0 gate:** confirm DuckDB sees Decimal / Date / Utf8 as intended via `DESCRIBE` / `typeof` +
  row counts **before trusting any timing**. A timing over mis-typed data is worthless.

### C. The DuckDB CLI adapter
- **CLI shell-out** to the `duckdb` binary (no Rust DuckDB crate); record `duckdb --version` in each row.
- **Optional:** if `duckdb` is absent, emit clean `status=not_attempted` rows with `reason=duckdb_absent` (not a
  failure); the Toron rail stays independent. Do not add a new status enum just for skipped external tools.
- **Query parity:** the canonical TPC-H SQL, same queries Toron runs, with only the minimal documented DuckDB dialect
  adjustments.
- **Execute-only timing:** export/load/prep are **outside** measured runs. Each measured DuckDB run is a **fresh CLI
  process over already-exported Parquet**; any `read_parquet` view/table setup is outside `execute_ms`, and only the
  query is timed - mirroring the fresh-process Toron rail. Same **N-runs / warmup / median / min / spread / stability
  gate**.
- **Row-count + schema sanity now**, recorded for both engines as `output_row_count` and
  `output_schema_fingerprint`; DuckDB rows are flagged if they disagree with the matching Toron row. **No full
  cross-engine digest** (that's its own correctness adapter, a follow-on).
- **DuckDB plan fingerprint:** populate the existing `plan_fingerprint` field with a DuckDB-side fingerprint of the
  emitted SQL + exported-data manifest schema/table fingerprint (not Toron's bound-plan fingerprint). Record the
  DuckDB version separately.

## Acceptance
- **Engine-aware summary + baseline** keyed by `(engine, query)`; the `"toron"` hardcode removed; no Toron/DuckDB
  collision (lands before any DuckDB row).
- **Parquet export of the exact Toron tables + a verifiable manifest** (names, row counts, schema fingerprint, hashes,
  mode, sf); **type parity confirmed** (Decimal/Date/Utf8 via DESCRIBE/typeof + row counts).
- DuckDB runs the **same queries on the same exported data**, **execute-only**, recorded as `engine = duckdb` rows in
  the JSONL + an extended board showing **Toron vs DuckDB** per query (execute_ms median/min/spread), same noise
  methodology, `duckdb --version` recorded.
- **duckdb absent -> `not_attempted(duckdb_absent)`**, not a failure; **row-count/schema sanity** per row via
  `output_row_count` + `output_schema_fingerprint` (full digest deferred).
- **No Toron engine/behavior change**; sf=1 board 22/22 unchanged; the recorded Toron baseline untouched.

## Guardrails
- **Engine-aware first** - summary/baseline keyed by `(engine, query)` before any DuckDB row; no collision, no
  `"toron"` hardcode.
- **Data parity or nothing** - DuckDB loads the exact exported gauge data (manifest-verified); never `dbgen`.
- **Type-parity gate** - Decimal/Date/Utf8 confirmed in DuckDB before any timing is trusted.
- **Execute-only** - export/load/prep outside measured runs; fresh CLI process per measured run; same noise model as
  Toron.
- **Optional dependency** - CLI shell-out, version recorded, `not_attempted(duckdb_absent)` if absent; no Rust DuckDB
  crate.
- **No status schema churn** - absent DuckDB uses existing `not_attempted`; no new `skipped` status.
- **No full cross-engine digest in 358** - row-count/schema sanity only; the canonical cross-engine oracle is a
  follow-on.
- **No Toron change** - additive only; the Toron rail + baseline untouched.

## Risks
- **Summary collision** - the `(engine, query)` keying + un-hardcode prevent it; lands first.
- **Silent data mismatch** - the export manifest (row counts, schema fingerprint, file hashes) makes parity verifiable;
  the type-parity gate catches Decimal/Date drift.
- **Apples-to-oranges timing** - same data, same queries, execute-only, same noise model; DuckDB version recorded for
  reproducibility.
- **DuckDB dependency creep** - optional CLI shell-out, `not_attempted(duckdb_absent)` if absent; no crate.
- **Over-scoping** - no optimization, no Toron change, no full digest (deferred to its own adapter).

## Forward arc
358 (DuckDB reference: data parity + engine-aware summary) -> **perf-optimization ADRs** (gated by the rail, now with an
external target to close) -> the **sf=10 atlas** (Toron absolute + regression bands + DuckDB comparison on one artifact
shape) -> **Rune-dataframe TPC-H** -> (follow-on) the **cross-engine correctness oracle** (canonical DuckDB result
normalization vs Toron digest) + the matrix-hygiene fix. No central roadmap; each a Toron-side ADR.

## Why toron-home
The gauge harness (`crates/toron-tpch`) - the perf summary, the data export, the DuckDB adapter - measuring
`crates/toron`. No Wendelon / cross-ring surface; no Toron engine change.

## Council provenance
Codex + Claude. DuckDB adapter on the 356 rail. Operator/Codex steer: **Parquet / shell-out-not-attempted-if-absent /
row-count-now-full-digest-later**. Codex's tightenings (adopted): (1) **engine-aware summary FIRST** -
`summarize_query_rows` hardcodes `"engine":"toron"` (main.rs:612) and keys by query; key by `(engine, query)` before
adding DuckDB or rows collide; (2) **export the exact Toron tables** (same helper + mode/sf) with an **export manifest**
(names, row counts, schema fingerprint, file hashes, mode, sf); (3) **Parquet type-parity is a Phase 0 gate**
(DESCRIBE/typeof + row counts; toron-tpch-only Parquet dep); (4) **DuckDB optional** (CLI, version recorded,
`not_attempted(duckdb_absent)`, no crate); (5) **no full cross-engine digest** in 358 (row-count/schema sanity; full
digest is its own adapter); (6) **execute-only** (export/prep outside measured runs; fresh CLI process per run); (7)
DuckDB's `plan_fingerprint` is a DuckDB SQL + manifest fingerprint, not a Toron bound-plan fingerprint. 358 = adapter +
data parity + engine-aware summary; no optimization, no Toron engine change, no full oracle.
