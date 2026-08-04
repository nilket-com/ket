# ADR 396 - Implementation plan: q03 materialized allocation/copy precision strike

Pairs with `plans/adr/0396_q03_materialized_alloc_copy_strike.md`. First substrate strike: Phase 0 cardinality + per-stage
allocation evidence -> strike the evidenced dominant Toron-owned producer -> re-measure Rune absolute median. Engine cut
if it touches shared join helpers; behavior-neutral; not q03-shaped in core.

## Phase 0 - evidence before the target (temporary/env-gated)
- **Join-stage stats** (env-gated, e.g. under the ADR 395 profile path): per join stage of the Rune q03 plan record
  stage label / build-side lineage if available, key plan, build rows, unique keys, duplicate keys, overflow rows
  (BuildRowList that allocated a Vec), max rows/key, and which stage is hot. **This gates the join-build strike:**
  mostly-unique -> the Vec-overflow theory is falsified, though HashMap/key/guard work may still be measured join-build
  cost.
- **Per-stage allocation counts + bytes** attributed to the Toron-owned caller: scan/filter output, join build (HashMap +
  BuildRowList), join probe/output, aggregate/order rebatch. A scoped allocation counter (temporary/off-by-default) or
  `dhat`-style attribution; record the dominant producer with %.
- Record all of this in the results doc **before** the code cut.

## Phase 1 - the evidenced strike
Pick exactly one dominant producer from Phase 0:
- **join-build:** choose the measured sub-cause. Duplicate overflow -> duplicate-aware overflow sizing or a flat
  row-index arena preserving per-key order. Mostly-unique HashMap/key work -> only a narrow behavior-neutral lever, or
  name it as residual. `join_build_memory_fits` -> lift to O(1) incremental if it re-derives array bytes per row.
- **Arrow filter / Parquet materialization (likely for unique-key q03):** remove a redundant decode/filter/dictionary
  rematerialization or copy at the scan->batch boundary.
- **aggregate/order rebatch:** cut the `execute_bound -> Vec` copy.
Keep it narrow. **Behavior-neutral** - identical results.

## Tests (behavior-neutral, shared-helper if touched)
- if `build_in_memory_join_rows` / `insert_join_build_row` change: parity across **Int32 / Int64 / generic keys**,
  **null keys**, **unique and duplicate keys**, **multiple rows per key ordering**, and output-order - the join result +
  order identical before/after;
- if the scan/filter or aggregate boundary changes: batch/digest/schema/column-order parity on a synthetic q03-shaped
  plan;
- Rune q03 value gate (395) still passes;
- prior greens (q04/q06/q14/q19) + carried unchanged; sf=3 streaming path unchanged.

## Phase 2 - re-measure (absolute primary, ratio context)
```bash
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch \
  --features polars-sql-reference --release
systemd-run ... target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
  --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
  --output-dir target/tpch/perf_adr396_q03_sf1
```
- **Primary:** Rune execute_ms median drop **beyond the runs=7 stability band** (395 baseline 1339.8 ms). Within-noise ->
  not banked; name the residual.
- **Secondary:** `Rune/Polars` (contextual; Polars sf=1 noisy). Re-profile (381) to confirm the struck producer's share
  fell.

## Verification
```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib                     # join-build parity if touched
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast            # REQUIRED (engine cut if core touched)
scripts/cargo-cgroup test -p toron-tpch --no-fail-fast       # REQUIRED
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01   # strict pass@compare
# Phase 2 sf=1 re-measure: Rune absolute median beyond noise; re-profile the struck site
git diff --check
```

## Deliverable (results doc)
`crates/toron/plans/impl/0396_q03_materialized_alloc_copy_strike_results.md`:
- Phase 0: join-stage cardinality (unique/duplicate/overflow/max-per-key/hot-stage) + per-stage allocation counts/bytes;
  the named dominant producer;
- the strike (which producer, why, the narrow change);
- Phase 2: Rune absolute median before/after (beyond-noise verdict) + `Rune/Polars` context + the re-profile showing the
  struck share fell;
- behavior-neutral parity (key plans/nulls/duplicates/order if a shared helper) + q03 value gate;
- confirmation: no cap bump, no qNN, full gates green, prior greens + sf=3 unchanged, instrumentation off-by-default.

## Risk / blast radius
- **Join-build theory falsified** -> redirect to Arrow/Parquet or rebatch; no wasted join rewrite.
- **Too small to move median** -> name the residual, re-target.
- **Shared-helper regression** -> parity tests + `--no-fail-fast`.
- **Polars-noise false win** -> absolute-median-primary.
- **Instrumentation left on** -> temporary/off-by-default.

## Seal
`plan: adr 396 q03 materialized alloc copy strike` (the dual). Implementation (strike + evidence + re-measure):
`impl: adr 396 q03 materialized alloc copy strike`. Do not commit `.perf.data`.
