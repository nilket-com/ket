# ADR 360 - persisted/streaming TPC-H data surface (make sf=10 measurable)

## Status
Proposed.

## Context

The 359 unblocker. The sf=10 first wall is the gauge's **in-memory data surface**: `generate_strict_lineitem`
(lib.rs:713) drains `LineItemGenerator` into full `Vec`s -> one `RecordBatch`, and `HarnessTable` holds the full
batches - the `lineitem` fixture alone OOMs at 4 GB before any query runs (both engines). ADR 360 makes sf=10
**measurable**: a **shared persisted dataset** (streamed-generated Parquet), a **gauge-local Parquet-backed
`StorageScan`** with projection/PBC pushdown, and a **q06/q14/q21 unblock pilot** - **surface only, not the full
359-redux atlas.** Gauge-local, behavior-neutral.

## Decision - manifest + streamed Parquet generator + ParquetHarnessScan

### The key design artifact (gauge-local, behavior-neutral)
A new persisted data surface in `crates/toron-tpch`: a **manifest + a streamed Parquet generator + a
`ParquetHarnessScan`**. No Wendelon/production storage ingest; no external dbgen.

### 1. Shared persisted dataset, not per-query export (Codex)
358's export is **query-shaped** and still starts from in-memory fixtures (the thing that OOMs). 360 writes **table
files once** under `target/tpch/data/<mode>/sf<N>/{lineitem,orders,...}.parquet` with a **manifest** (table names, row
counts, schema, file hashes, mode, sf). Query specs reference **table sources from the manifest** - one shared dataset,
not a re-export per query.

The manifest is also the catalog/facts source for the persisted path: table descriptor metadata, row count, schema
fingerprint/signature, file bytes, and a conservative optimizer byte upper bound must be available **without loading the
table**. The persisted path must not rebuild `HarnessTable { batch: RecordBatch, ... }`; introduce a table-source
metadata layer (for example, in-memory fixture vs persisted Parquet source) so catalog registration and
`TableOptimizerFacts` are seeded from metadata while the scan storage reads the file.

### 2. Streamed generation (the existing tpchgen generators)
Drive `LineItemGenerator` (and the other table generators) **incrementally**: generate a chunk, `ArrowWriter.write`
it, drop it - never accumulate full `Vec`s/`RecordBatch`. Reuses the 358 `ArrowWriter`. Bounded RSS on write. **Same
generators** Toron's oracles expect (not external dbgen), so the data is identical to today's, just persisted.

### 3. ParquetHarnessScan with projection/PBC pushdown (load-bearing - Codex)
The current `HarnessScan` (lib.rs:1275) applies **PBC before projection**, then returns projected batches. The Parquet
scan **must preserve that**, and projection pushdown is **probably the difference between q06 fitting and moving the OOM
to scan materialization**:
- read **only `projection ∪ pbc_columns`** from Parquet (columnar projection - Parquet's strength),
- apply **PBC per row group**,
- emit **exactly the projected schema**,
- **streamed** (row-group by row-group, read-yield-drop) - never load the full table.

If PBC column dependencies cannot be extracted safely in Phase 0, the first cut may restrict persisted mode to queries
whose scan predicates do not require hidden PBC columns, but it must record that limitation. Do not silently read all
columns as a fallback; that reopens the q06-style memory wall.

### 4. DuckDB reads the same shared Parquet
DuckDB `read_parquet`s the **same shared table files** (the manifest sources) - now the primary surface, not an export
derived from an in-memory fixture. Data parity = identical on-disk files for both engines.

## Acceptance (sf=10 measurable; honest bar - emit a row, not necessarily green)
- **Shared persisted dataset:** sf=10 tables written once (streamed, bounded RSS << 4 GB) + a manifest; query specs
  reference manifest sources; catalog descriptors/facts are seeded from metadata, not from loaded `RecordBatch` values.
- **ParquetHarnessScan:** streamed, **projection-pushed** (reads only projection ∪ pbc_columns), **PBC-before-projection
  preserved**, emits the projected schema; bounded scan RSS.
- **Pilot proof (the honest bar - Codex #3):** q06/q14/q21 at sf=10 under fixed 4 GB **emit a perf row** - **pass OR a
  named wall** (332), **not necessarily green**. `execute_bound` still calls `materialize_scan_stream`, so 360 proves
  *"no full in-memory fixture before scan"*; remaining scan/materialize/engine walls are the **359-redux** landscape.
- **Correctness parity:** the persisted-scan path yields the **same answers** as the in-memory path at sf=0.01/sf=1
  (the oracle) - a scan-path divergence is a bug, not a perf detail.
- **No engine behavior change; gauge-local; sf=1 board unchanged.**

## Guardrails
- **Shared dataset, not per-query** - tables written once + a manifest; specs reference it.
- **Projection pushdown is load-bearing** - read only projection ∪ pbc_columns; without it the OOM just moves to scan
  materialization (q06 is the canary).
- **PBC-before-projection parity** - the Parquet scan matches `HarnessScan`'s order/semantics exactly.
- **Bounded both ways** - streamed generation + streamed projected scan; peak RSS verified at sf=10.
- **Don't overpromise "real scan path"** - `execute_bound` materializes the scan stream; the bar is emit-a-row
  (pass/named-wall), not green; materialize/engine walls are 359-redux.
- **Correctness parity** - same answers as the in-memory path (oracle-gated).
- **Gauge-local + behavior-neutral** - no Wendelon/production ingest, no engine change.

## Risks
- **Projection not pushed -> OOM moves to scan** - the q06 pilot is the canary; projection ∪ pbc only.
- **PBC/projection semantic drift** - the Parquet scan must mirror `HarnessScan` (PBC before projection, projected
  emit); the correctness-parity oracle catches divergence.
- **Overpromising green** - rejected; the bar is emit-a-row; materialize walls are named (359-redux), not 360 failures.
- **Generation memory** - streamed write, bounded buffer; verified by sf=10 generation RSS.
- **Scope creep into 359-redux** - 360 is surface + the 3-query pilot only; the full atlas is separate.

## Forward arc
360 (persisted/streaming surface; q06/q14/q21 emit rows under 4 GB) -> **359-redux** (the full sf=10 atlas on the real
scan path: Toron absolute + bands + DuckDB; the streaming substrate's constant-memory verdict; the scan/materialize +
upstream-child-materialization walls, ADR 349) -> sf=10 fix ADRs -> sf=100 / Rune. Plus the cross-engine digest oracle +
matrix-hygiene. No central roadmap.

## Why toron-home
Gauge-local data surface in `crates/toron-tpch` (manifest + streamed Parquet generator + ParquetHarnessScan) measuring
`crates/toron`. No Wendelon / cross-ring surface; no engine behavior change.

## Council provenance
Codex + Claude. The 359 unblocker. Operator/Codex steer: **surface + q06/q14/q21 unblock pilot only** (no full
359-redux); **gauge-local Parquet-backed `StorageScan`** (not Wendelon/production); **stream the existing tpchgen
generators** (not external dbgen). Codex's tightenings (adopted): (1) **shared persisted dataset** (table files written
once under `target/tpch/data/<mode>/sf<N>/` + a manifest; specs reference it), not 358's per-query/in-memory export;
(2) **projection/PBC pushdown is load-bearing** - read only projection ∪ pbc_columns, PBC before projection, emit
projected; probably the difference between q06 fitting and moving the OOM to scan materialization; (3) **don't
overpromise "real scan path"** - `execute_bound` materializes the scan stream, so the bar is q06/q14/q21 **emit a perf
row under 4 GB (pass or named wall), not necessarily green**. The key artifact is the manifest + streamed generator +
ParquetHarnessScan; gauge-local, behavior-neutral; 359-redux measures the engine once the surface is unblocked.
