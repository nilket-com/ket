# ADR 358 - Implementation plan: the DuckDB perf adapter (engine-aware summary + data parity + adapter)

Pairs with `plans/adr/358_duckdb_perf_adapter.md`. A `crates/toron-tpch`-only cut in three ordered parts: (A) make the
perf summary engine-aware, (B) export the exact Toron tables to Parquet with a manifest + type-parity gate, (C) the
DuckDB CLI adapter (optional, execute-only, row-count sanity). No optimization, no Toron engine change, no full
cross-engine digest.

## Phase 0 - grounding (before building)
- **Summary keying:** confirm `summarize_perf_rows` groups by query and `summarize_query_rows` hardcodes
  `"engine":"toron"` (main.rs:612); confirm the baseline lookup is query-keyed. These are the collision sites.
- **Data export seam:** identify the gauge's table generators + the query table helper Toron uses per (mode, sf); plan
  the Parquet export reusing them (no separate data path).
- **Parquet dep:** add a Parquet writer dependency to `toron-tpch` only (the repo has none); pick the arrow-parquet
  writer matching the workspace arrow version.
- **Type-parity probe:** export one table, load in DuckDB, `DESCRIBE`/`typeof` + row count - confirm Decimal/Date/Utf8
  map as intended before building the full adapter.
- **DuckDB invocation:** confirm `duckdb` CLI presence detection + `--version` capture + execute-only timing over
  exported Parquet, with view/table setup outside the measured query.

## A - engine-aware perf summary (lands first; no DuckDB row yet)
- Key `summarize_perf_rows` + the summary records by **`(engine, query)`**; remove the `"engine":"toron"` hardcode
  (carry the row's real engine).
- Key the **baseline lookup** by `(engine, query)`.
- Board renders both engines per query (Toron rows still render identically when DuckDB is absent).
- **Behavior-neutral for Toron:** the existing Toron-only baseline + board render the same (a test: Toron-only input ->
  same board as before).

## B - data parity: Parquet export + manifest + type gate
- Export the gauge's generated tables (same helper, same mode/sf) to `target/tpch/parquet/<mode>/sf<N>/<table>.parquet`.
- Write `target/tpch/parquet/<mode>/sf<N>/manifest.json`: table names, row counts, schema fingerprint, file hashes,
  mode, sf.
- **Type-parity gate:** a check (DuckDB `DESCRIBE`/`typeof` + row count vs the manifest) that must pass before any
  DuckDB timing is recorded; a Decimal/Date/Utf8 mismatch fails the gate (no timing on mis-typed data).

## C - the DuckDB CLI adapter
- Detect `duckdb`; if absent, emit `engine=duckdb, status=not_attempted, reason=duckdb_absent` rows (clean) - no
  failure and no new status enum.
- Record `duckdb --version` per row.
- Load/register the exported Parquet in DuckDB outside `execute_ms`; run the canonical TPC-H SQL (documented dialect
  deltas).
- **Execute-only, fresh process per measured run:** each measured run is a fresh `duckdb` CLI invocation over the
  already-exported Parquet, `.timer` on the query; same `--runs`/`--warmup`/median/min/spread/stability as Toron.
- Record `engine=duckdb` rows (execute_ms, `output_row_count`, `output_schema_fingerprint`, duckdb_version) in the
  JSONL; add the same output row-count/schema fields to Toron perf rows so DuckDB sanity has a concrete comparator.
  The board shows Toron vs DuckDB.
- Populate DuckDB `plan_fingerprint` with a stable DuckDB-side fingerprint of emitted SQL + exported-data manifest
  schema/table fingerprint; keep `duckdb_version` separate. Do not compare DuckDB rows against Toron's bound-plan
  fingerprint.
- **Row-count + schema sanity only** (no full digest); a DuckDB row mismatch against the matching Toron row flags the
  row (`row_count_mismatch` / `schema_mismatch`).

## Tests / safety net
- **Engine-aware summary:** a test that Toron-only rows render the same board as before (behavior-neutral for Toron);
  a test that `(toron, qNN)` and `(duckdb, qNN)` rows do **not** collide.
- **Manifest round-trip:** export -> manifest row counts + schema fingerprint match the in-memory tables.
- **Type-parity gate:** a fixture proving the gate fails on a deliberately mis-typed export (Decimal -> Float drift).
- **DuckDB optional:** with `duckdb` absent, the adapter emits `not_attempted(duckdb_absent)` rows + the Toron rail
  still runs/records.
- **DuckDB fingerprint:** DuckDB rows carry a SQL+manifest `plan_fingerprint`; baseline comparisons are engine-local.
- **No Toron change:** `correctness-matrix` 22/22 + `matrix` unchanged; the Toron baseline untouched; sf=1 board green.
- `--lib`, `--test architecture_test`, `--no-fail-fast`, `toron-tpch` green; `check -p toron-tpch --all-targets`
  (the Parquet dep compiles).

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix        # 22/22 unchanged (no Toron change)
scripts/cargo-cgroup run -p toron-tpch -- matrix                    # unchanged
# DuckDB present: export + type-gate + a perf run shows engine=toron AND engine=duckdb rows on the board
scripts/cargo-cgroup run -p toron-tpch --release -- perf --sf 1 --mode strict --runs 7 --warmup 1 --query all --engines toron,duckdb
git diff --check
#   engine-aware board (no collision); manifest-verified Parquet parity; type-gate passed; execute-only DuckDB rows;
#   duckdb-absent maps to not_attempted(duckdb_absent); Toron baseline/board unchanged
```

## Results doc
`crates/toron/plans/impl/358_duckdb_perf_adapter_results.md`: the engine-aware summary change (collision test); the
Parquet export + manifest (row counts, schema fingerprint, hashes) + the type-parity gate evidence (DuckDB
DESCRIBE/typeof); the DuckDB adapter (CLI, version, execute-only, not_attempted-if-absent); a sample
**Toron-vs-DuckDB board** at sf=1 (execute_ms median/min/spread per query) with `output_row_count` +
`output_schema_fingerprint` sanity; DuckDB SQL+manifest `plan_fingerprint`; confirmation no Toron change (22/22
unchanged, baseline untouched); the deferred full cross-engine digest oracle named.

## Risk / blast radius
- **Collision** - `(engine, query)` keying lands first; Toron-only render unchanged (tested).
- **Data mismatch** - manifest (counts/fingerprint/hashes) + type-parity gate make parity verifiable.
- **Apples-to-oranges** - same data/queries/execute-only/noise-model; duckdb version recorded.
- **Dependency** - Parquet dep is toron-tpch-only; DuckDB optional (CLI, `not_attempted(duckdb_absent)`, no crate).
- **No Toron change / no full digest / no optimization** - scoped out; the cross-engine oracle is a follow-on.

## Seal
`impl: adr 358 duckdb perf adapter`. Do not commit wip drafts.
