# ADR 416 results - q07/q09 compact spilled-index extension

## Verdict

ADR 416 extends ADR 414's compact resident spilled-build index to the reoriented materialized-build seam used by
q07 and q09. Both sf=10 cells move from the same resident-index wall to persisted-correct greens under the unchanged
4 GiB scope:

| query | pre-416 status | post-416 status | execute_ms | correctness | spill bytes |
|---|---|---|---:|---|---:|
| q07 | fail: resident index | pass | 16,180.396 | pass@compare | 134,732,480 |
| q09 | fail: resident index | pass | 37,528.912 | pass@compare | 108,992,256 |

The compact index is selected before the first build-row append. A temporary finish-time profile measured each real
driver's completed index at 536,870,912 bytes, down from 1,074,377,480 bytes on the legacy path: 2.001x headroom on
q07 and independently 2.001x on q09. No cap, multiplier, cgroup scope, or rescue window changed.

## Conditions And Artifacts

All reach and correctness runs used persisted strict sf=10 Parquet under `MemoryMax=4G`, `MemoryHigh=4G`, and
`TasksMax=512`. Spill scratch was cleared between scoped runs.

Artifact root: `target/tpch/adr416/`.

Key artifacts:

- `q07_pre.json`, `q09_pre.json`: fresh pre-cut walls;
- `q07_site_trace.err`, `q09_site_trace.err`: temporary creation-site traces;
- `q07_index_profile.jsonl`, `q09_index_profile.jsonl`: temporary completed-index profiles;
- `q07_final.json`, `q09_final.json`: final reach rows from the hook-free release binary;
- `q07_final_correctness.out`, `q09_final_correctness.out`: persisted correctness;
- `*_control_perf.json`, `*_control_correctness.out`: spill-bearing controls;
- `full_toron.out`, `full_tpch.out`: full no-fail-fast gates.

The temporary site and index-profile hooks were removed before the final release build and final measurements.

## Phase 0 Classification

Fresh baseline runs reproduced the byte-identical ADR 415 wall:

| query | execute_ms to wall | peak RSS | requested | available | resident index | fingerprint |
|---|---:|---:|---:|---:|---:|---|
| q07 | 3,909.202 | 1,704,564 KiB | 1,074,443,016 | 1,073,741,824 | 1,074,377,480 | `f0bf3fb0ae7aef26` |
| q09 | 7,095.672 | 3,540,468 KiB | 1,074,443,016 | 1,073,741,824 | 1,074,377,480 | `5fc5d571c280b8df` |

Both queries route through
`Executor::prepare_reoriented_inner_join_runtime_from_build_batches`, with `JoinKeyPlan::Int64`. That helper created
`SpilledInnerJoinBuild` with the default legacy index and began appending immediately. The missing compact opt-in was
an omission, not a semantic fence.

The bounded rescue mechanism was not the production path: the 701,192-byte overshoot is below its 1 MiB bound, but
rescue opt-in is test-only and disabled for production builds. ADR 416 enables compact storage from construction;
it does not widen or promote the rescue path.

## Creation-Path Census

The pre-cut census classified every production `SpilledInnerJoinBuild::create` site:

| state | creation paths | verdict |
|---|---|---|
| already compact | streamed-spilled left-semi; streamed-spilled inner build | retained |
| newly compact | reoriented materialized build batches | ADR 416 cut |
| direct indexed, legacy | partitioned spilled left-outer; streamed scan-build inner; grouped left-outer; projected left-semi aggregate; reoriented scan build | compact safety not yet proven |
| initially unindexed partition/reservoir/load | partitioned streamed-inner visitors, resident group loader, reservoir builder, active-group probe | different lifecycle; compact safety not yet proven |

The runtime flag and legacy index are therefore not retired in ADR 416. Multiple creation paths remain unopted or
have different index lifecycles; retirement requires a separately reviewed all-path conversion.

## Implementation

The production behavior change is one line at the classified helper: call `use_compact_resident_index()` before
writing any already-materialized build batch. ADR 414's existing key-plan fence remains authoritative:

| key plan | index |
|---|---|
| `Int32` or `Int64`, non-null-safe | compact primitive index |
| `Generic` or null-safe | legacy hash fallback |

A test-only monotonic `legacy_index_insert_count` was added to both legacy append locations. The production-helper
test now asserts all of the following:

- two input batches are spilled batch-by-batch rather than concatenated first;
- the completed spill uses the compact Int64 index;
- `legacy_index_insert_count == 0`, proving selection before the first append rather than rescue after legacy writes;
- accounted resident-index bytes equal the compact structure's computed bytes;
- output remains row-for-row equal to the in-memory path with probe-first column order.

The counter is compiled only under `cfg(test)` and adds no production state or runtime work.

## Per-Driver Headroom

The temporary finish-time profile observed each completed real build, not merely its first low-cap crossing:

| query | index kind | indexed rows | pages | legacy bytes | compact bytes | reduction | spill bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| q07 | Int64 | 15,000,000 | 1,832 | 1,074,377,480 | 536,870,912 | 2.001x | 134,732,480 |
| q09 | Int64 | 15,000,000 | 1,832 | 1,074,377,480 | 536,870,912 | 2.001x | 108,992,256 |

This is measured headroom on both ADR 416 drivers. It is not inherited from q12's ADR 414 result.

## Driver Results

| query | status | execute_ms | peak RSS | spill bytes | output rows | fingerprint | correctness |
|---|---|---:|---:|---:|---:|---|---|
| q07 | pass | 16,180.396 | 983,740 KiB | 134,732,480 | 4 | `f0bf3fb0ae7aef26` | pass@compare |
| q09 | pass | 37,528.912 | 2,817,592 KiB | 108,992,256 | 175 | `5fc5d571c280b8df` | pass@compare |

The fingerprints are unchanged from the failing baseline rows. q09 does not expose the pre-registered OOM-behind-
the-wall outcome; its peak falls by about 706 MiB, though 2.75 GiB remains high enough to keep it a residency watch.

## Controls

The spill-bearing green controls all re-proved persisted correctness:

| query | status | execute_ms | peak RSS | spill bytes | forensic verdict |
|---|---|---:|---:|---:|---|
| q03 | pass | 14,491.159 | 530,956 KiB | 233,338,304 | exact prior signature |
| q10 | pass | 11,914.645 | 1,700,408 KiB | 215,703,848 | exact prior signature |
| q12 | pass | 6,452.417 | 1,432,196 KiB | 159,510,976 | exact prior signature |
| q14 | pass | 5,274.276 | 287,144 KiB | 36,464,000 | retained green |
| q16 | pass | 4,063.162 | 762,664 KiB | 60,078,400 | retained green |
| q19 | pass | 81,704.073 | 271,884 KiB | 51,471,552 | exact prior signature |
| q20 | pass | 21,828.483 | 3,361,696 KiB | 256,104,392 | retained; residency watch unchanged |

The exact q03/q10/q12/q19 spill signatures match ADR 415. Execution times are single-session reach observations,
not cross-session speed claims. q10's large execute-time drop is plausibly a compact-index probe-speed side effect
on this shared seam; the speed thread should measure that hypothesis under a controlled same-session baseline before
banking it.

## Verification

The following gates passed after removing the temporary profile hooks and formatting:

- `scripts/cargo-cgroup fmt -- --check`;
- `scripts/cargo-cgroup check -p toron --all-targets`;
- filtered spill suite: 42 passed;
- filtered join suite: 194 passed;
- architecture test: 2,079 passed, 0 failed;
- `cargo test -p toron --no-fail-fast` through the cgroup wrapper with four jobs;
- `cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast` through the wrapper with four jobs.

## Board And Scope Honesty

ADR 416 moves the live sf=10 board from 12/22 to **14/22 persisted-correct greens**. q07 and q09 are representation
wins, not cap adjustments. The remaining non-green cells and their named work remain:

- q13/q17/q18/q21: scan-output streaming family;
- q02: join-output streaming;
- q22: spill-writer residency;
- q05: peak-residency/OOM diagnosis;
- q11: oracle/query reconciliation.

No cap, multiplier, task budget, cgroup property, or rescue threshold changed. No `.perf.data` belongs to this
change. Compact-index flag retirement remains a separately scoped cleanup informed by the creation-path census.
