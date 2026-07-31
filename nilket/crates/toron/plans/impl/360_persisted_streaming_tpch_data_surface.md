# ADR 360 - Implementation plan: persisted/streaming TPC-H data surface

Pairs with `plans/adr/360_persisted_streaming_tpch_data_surface.md`. A gauge-local `crates/toron-tpch` cut: a shared
persisted dataset (manifest + streamed Parquet generator) + a `ParquetHarnessScan` (projection/PBC pushdown, streamed)
+ a q06/q14/q21 sf=10 unblock pilot. Surface only (not 359-redux). Behavior-neutral; no engine change. Honest bar:
the pilot **emits a perf row under 4 GB (pass or named wall), not necessarily green**.

## Phase 0 - grounding
- **Generators:** confirm `LineItemGenerator` (+ the other table generators) can be driven incrementally (yield
  chunks) rather than only drained into full `Vec`s (the lib.rs:713 pattern).
- **Scan contract:** read `HarnessScan` (lib.rs:1275) - confirm PBC-before-projection, the projected emit, and the
  `ScanStream`/`ScanRequest` shape (projection + pushed predicate columns) the `ParquetHarnessScan` must mirror.
- **Metadata seam:** map the current `HarnessTable { descriptor, batch, facts }` use. Persisted mode must seed catalog
  descriptors and `TableOptimizerFacts` from manifest metadata (row count, schema, file bytes / conservative logical
  byte bound) without loading a `RecordBatch`; if the design still constructs `HarnessTable { batch }`, the cut has
  failed its own premise.
- **Parquet I/O:** the 358 `ArrowWriter` is the writer; identify the reader (`ParquetRecordBatchReaderBuilder` with
  column projection + row-group iteration) for the streamed projected scan.
- **Manifest shape:** table name -> file path, row count, schema/signature/fingerprint, file hash, mode, sf, and
  optimizer fact inputs.

## D1 - shared persisted dataset (streamed generation)
- A `prepare`-style path that writes sf=N tables **once** to `target/tpch/data/<mode>/sf<N>/<table>.parquet`:
  drive each generator in chunks, `ArrowWriter.write(chunk)`, drop - bounded RSS, no full `Vec`/`RecordBatch`.
- Write `target/tpch/data/<mode>/sf<N>/manifest.json` (table -> path, row count, schema, hash, mode, sf,
  descriptor/fact metadata).
- Idempotent: skip regeneration if the manifest + hashes match.

## D2 - ParquetHarnessScan (projection/PBC pushdown, streamed)
- A `StorageScan` impl over a manifest table source: on `scan(request)`, compute `projection ∪ pbc_columns`; build a
  `ParquetRecordBatchReader` projected to those columns; iterate **row group by row group**; apply **PBC per row group**
  (matching `HarnessScan`'s pre-projection order); emit **exactly the projected schema** as a `ScanStream`
  (read-yield-drop, bounded).
- Add a persisted table-source path for query specs instead of forcing everything through `HarnessTable`'s full-batch
  shape. The catalog/load phase gets descriptors/facts from the manifest; the storage object maps `TableId` to Parquet
  table sources. The in-memory path stays unchanged for sf=0.01/sf=1 parity.
- If PBC column dependencies cannot be extracted safely in Phase 0, restrict persisted mode to the pilot/query shapes
  that do not require hidden PBC columns and record the limitation. Do not fall back to reading all columns silently.

## D3 - the q06/q14/q21 sf=10 unblock pilot
- Generate the sf=10 shared dataset (D1); run q06/q14/q21 at sf=10 under fixed 4 GB via the persisted surface.
- **Honest bar:** each emits a perf row - **pass or a named wall (332)** - confirming the data stage no longer OOMs
  before execution. q06 (projected scan-agg) is the canary that projection pushdown works (else the OOM moves to scan
  materialization). Record peak RSS for generation and scan separately.

## Tests / safety net
- **Correctness parity (load-bearing):** a query (e.g. q06 + one join query) over the **ParquetHarnessScan** yields the
  **same answer** as the in-memory `HarnessScan` at sf=0.01/sf=1 (the oracle / a direct batch-equality test).
- **Projection pushdown:** a test that the ParquetHarnessScan reads only projection ∪ pbc_columns (e.g. a wide table,
  narrow projection -> only the projected columns materialized; assert via the reader's projected schema / a byte or
  column-count probe).
- **PBC-before-projection parity:** a fixture where PBC drops rows -> the Parquet scan emits the same filtered+projected
  rows as `HarnessScan`.
- **Streamed/bounded:** generation + scan of a multi-row-group table keep peak RSS bounded (no full-table residency).
- **Manifest round-trip:** generate -> manifest row counts/schema/hash match; idempotent skip on re-run.
- **No engine/behavior change:** focused live correctness/parity on the pilot queries at oracle scale; `check`/`test -p
  toron-tpch` green. Do not use cached `matrix`/`correctness-matrix` as sf=10 evidence.

## Verification (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q06 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q14 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q21 --sf 0.01
# generate sf=10 shared dataset (streamed, bounded), then the pilot under fixed 4 GB:
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --mode strict \
#     --runs N --warmup K --query q06 --engines toron,duckdb --output-dir target/tpch/perf_adr360_pilot_q06
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --mode strict \
#     --runs N --warmup K --query q14 --engines toron,duckdb --output-dir target/tpch/perf_adr360_pilot_q14
#   systemd-run --user --property=MemoryMax=4G ... target/release/toron-tpch perf --sf 10 --mode strict \
#     --runs N --warmup K --query q21 --engines toron,duckdb --output-dir target/tpch/perf_adr360_pilot_q21
git diff --check
#   shared persisted dataset + manifest; ParquetHarnessScan projection/PBC-pushed; q06/q14/q21 emit rows under 4 GB
#   (pass or named wall); correctness parity holds; no engine change
```

## Results doc
`crates/toron/plans/impl/360_persisted_streaming_tpch_data_surface_results.md`: the shared dataset layout + manifest;
the streamed-generation peak RSS (<< 4 GB); the ParquetHarnessScan (projection/PBC pushdown, streamed) + the
correctness-parity evidence; the q06/q14/q21 sf=10 pilot result (each emits a row under 4 GB - pass or named wall, with
producer/bytes), generation vs scan peak RSS; confirmation projection pushdown is engaged (q06 didn't move the OOM to
scan); explicit note this is surface-only (359-redux measures the engine landscape next).

## Risk / blast radius
- **Projection not pushed** - the q06 pilot canary + the projection test catch it.
- **PBC/projection drift** - mirror `HarnessScan`; correctness-parity oracle gates it.
- **Overpromising green** - the bar is emit-a-row (pass/named-wall); materialize/engine walls are 359-redux.
- **Generation/scan memory** - streamed + bounded; peak RSS verified at sf=10.
- **Gauge-local + behavior-neutral** - no Wendelon ingest, no engine/cost change; sf=1 board unchanged.

## Seal
`impl: adr 360 persisted streaming tpch data surface`. Do not commit wip drafts.
