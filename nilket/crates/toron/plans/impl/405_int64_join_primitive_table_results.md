# ADR 405 results - Int64 join build/probe primitive table

## Summary

Implemented the fenced `Int64` in-memory join table. The old generic representation:

```text
HashMap<JoinKey, BuildRowList, ahash::RandomState>
```

now remains the fallback for `Int32`, `Generic`, and null-safe joins. `JoinKeyPlan::Int64 && !null_safe_keys` uses an
open-addressing table over raw `i64` keys, with the existing `BuildRowList` value kept intact. That preserves duplicate
insertion order, probe-row output order, and the existing outer/semi/anti matched-build bitmap semantics.

The cut banked, but modestly: the conservative same-machine q03 sf=1 row moved Toron SQL from **533.281 ms** to
**462.434 ms** (`-13.3%`), and Rune moved with it from **538.358 ms** to **466.393 ms** (`-13.4%`). The plan fingerprint
stayed `666e0ac028036f70`, spill stayed zero, and SQL/Rune stayed ~1.00x. This is a real Int64 substrate win, not a
planner or one-path artifact.

## Implementation

- `JoinBuildRows` is now a small wrapper over either:
  - `Hash(HashMap<JoinKey, BuildRowList, RandomState>)`, or
  - `Int64(Int64BuildTable)`.
- `Int64BuildTable` stores:
  - `buckets: Vec<usize>` with `usize::MAX` as the empty marker;
  - `entries: Vec<Int64BuildEntry>`, each entry holding the raw `i64` key plus `BuildRowList`;
  - a per-table `ahash::RandomState`.
- Bucket capacity is `2 * rows` rounded to a power of two, so absent probes always terminate at an empty bucket. A
  regression test locks this spare-bucket invariant.
- Selection is fenced in `build_in_memory_join_rows`:

```text
JoinKeyPlan::Int64 && !null_safe_keys -> Int64BuildTable
otherwise                           -> existing HashMap path
```

The central Int64 probe path now uses `get_int64(i64)`, and the direct streaming left-outer path was updated the same
way. `get(&JoinKey)` remains available for the generic/existence callers.

## Phase 0 census

The in-memory build/probe seam is shared:

- `join.rs`: plain in-memory joins call `build_in_memory_join_rows` and `probe_in_memory_join_rows`.
- `streaming.rs`: the reoriented/streaming paths also call `build_in_memory_join_rows` at the per-partition/in-memory
  build points.
- Direct `build_rows.get(...)` callers were checked; the only direct Int64 streaming probe was moved to `get_int64`.

So this is a generic Int64 in-memory build/probe representation change, not a q03-branded branch.

## Correctness gates

Focused join table tests:

```text
CARGO_CGROUP_JOBS=12 scripts/cargo-cgroup test -p toron join_build_rows --lib -- --nocapture
```

Result: 8/8 passed.

New locks:

- `int64_join_build_rows_are_fenced_to_non_null_safe_int64_plan`
- `int64_join_build_rows_preserve_duplicate_probe_order_and_null_routing`
- `int64_join_build_rows_match_hash_fallback_for_join_kinds`
- `int64_join_build_rows_preserve_memory_guard_decision`
- `int64_join_build_rows_keep_spare_bucket_for_absent_probe`

Existing locks for unique rows, duplicate overflow, Int32 null/order, generic key order, and preallocation also pass.

End-to-end q03 correctness:

```text
target/release/toron-tpch correctness --query q03 --sf 1 --mode strict \
  --data-source parquet --data-dir target/tpch/data
```

Result: `pass`.

## q03 sf=1 four-way

Baseline was run from a detached worktree at `be16a4e` (the ADR 405 plan commit), using the same target data and command
shape. The post-cut row is from the current worktree after force-rebuilding the current release binary.

Baseline artifact:

```text
target/tpch/perf_adr405_q03_sf1_baseline_worktree/latest_summary.jsonl
```

Post-cut artifact:

```text
target/tpch/perf_adr405_q03_sf1_post/latest_summary.jsonl
```

| engine | baseline median | post median | delta | spread post | fingerprint | flags |
|---|---:|---:|---:|---:|---|---|
| toron | 533.281 ms | 462.434 ms | -13.3% | 0.091 | `666e0ac028036f70` | none |
| rune | 538.358 ms | 466.393 ms | -13.4% | 0.059 | `666e0ac028036f70` | none |
| polars_sql | 128.450 ms | 126.122 ms | -1.8% | 0.117 | `fnv1a64:a3b5c15847833ac3` | none |
| duckdb | 48.000 ms | 45.000 ms | context | 0.244 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

Ratios:

| ratio | baseline | post |
|---|---:|---:|
| SQL/Rune | 0.99x | 0.99x |
| Toron/Polars | 4.15x | 3.67x |
| Rune/Polars | 4.19x | 3.70x |
| Toron/DuckDB | 11.11x | 10.28x |

DuckDB remains context only for sf=1 because the row is flagged unstable/schema_mismatch. The Toron/Rune/Polars rows are
the banked comparison.

An earlier current-row artifact in the same session (`target/tpch/perf_adr405_q03_sf1_current_duckdb`) measured Toron at
371.675 ms and Rune at 373.234 ms. I did not use it as the primary row because it was collected before the baseline
worktree artifact swap/rebuild cycle; the conservative post-cut row above is the sealed evidence.

## Current join-build telemetry

Lightweight profile loop:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_PROFILE_LOOP=16 \
  --setenv=TORON_JOIN_BUILD_PROFILE=1 \
  target/release/toron-tpch perf-run-one \
    --engine rune \
    --query q03 \
    --mode strict \
    --sf 1 \
    --run-index 0 \
    --data-source parquet \
    --data-dir target/tpch/data \
  > target/tpch/adr405_q03_rune_profile_loop_current.out
```

Current q03 build profile:

```text
key_plan=int64
build_rows=3,241,776
unique_keys=829,958
duplicate_keys=700,918
overflow_rows=2,411,818
max_rows_per_key=7
overflow_heap_capacity_bytes=0
build_micros=142,064
```

Sampled perf note: I also captured `target/tpch/adr405_q03_loop_perf_current.perf.data`, but the release binary is
stripped in the normal profile, so it did not provide a useful resolved Toron frame table. The frame-shrink claim is
therefore not overclaimed as a sampled table here. Mechanistically, the Int64 path no longer calls `hashbrown::rustc_entry`
or `HashMap::get`; the banked evidence is the same-machine q03 median/RSS movement plus the fenced code path and tests.

## sf=10 controls

Artifacts:

```text
target/tpch/perf_adr405_q04_sf10_current/latest_summary.jsonl
target/tpch/perf_adr405_q19_sf10_current/latest_summary.jsonl
target/tpch/perf_adr405_q08_sf10_current/latest_summary.jsonl
```

| query | Toron status | Toron median | spill | DuckDB median | notes |
|---|---|---:|---:|---:|---|
| q04 | pass | 5,321.337 ms | 0 | 132.000 ms | green control |
| q19 | pass | 88,999.795 ms | 51,471,552 | 157.000 ms | spill-path control, DuckDB schema_mismatch |
| q08 | pass | 20,478.861 ms | 0 | 222.000 ms | 403 path control, Toron flagged unstable |

All three controls still pass. q08 is noisy (`spread=0.227`) but remains correct and completes.

## Hygiene

- No cap bump.
- Plan fingerprint unchanged for q03: `666e0ac028036f70`.
- `Int32`, `Generic`, and null-safe joins stay on the existing hash-map representation.
- `.perf.data` artifacts are under `target/` and are not staged.
- The detached baseline worktree was used only to produce the same-machine baseline row.

## Verdict

405 lands as the fenced Int64 primitive-table implementation, not a stop-line diagnostic. It removes the enum-key/hashbrown
representation from the Int64 in-memory build/probe path, preserves the join contract, and banks a conservative **13%**
q03 sf=1 improvement with SQL and Rune moving together.

This does not close the Polars gap. The post-cut q03 path is still **3.67x Polars**, with decode/materialization and the
remaining join-table representation work still ahead. `Int32` and radix/partitioned table variants remain follow-ons.
