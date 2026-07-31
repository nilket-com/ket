# ADR 410 - Results: q10 reoriented build accumulator completion

ADR 410 executed the bounded fork from the plan: route over-cap **already-materialized**
reoriented inner build batches into the existing `SpilledInnerJoinBuild` writer instead of
failing at the in-memory hash accumulator. q12's 67,728-byte resident-state edge was
re-probed and left as a separate named residual; it was not folded into this change.

## Implementation

Changed:
- `crates/toron/src/exec/join/streaming.rs`

## Phase 0 Classification

q10's pre-410 wall was the materialized-fallback accumulator seam, not ADR 409's terminal
scan flip:
- the failing producer was `reoriented join build accumulator`, requested
  1,776,981,960 / available 1,073,741,824;
- the wall is at `prepare_reoriented_inner_join_runtime_from_build_batches`, after
  `stage.build` has already been executed into a `Vec<RecordBatch>`;
- therefore the build side is derived/materialized, not a direct scan-shaped build handled by
  `prepare_reoriented_inner_join_runtime_with_streamed_scan_build`;
- ADR 409 did not absorb it because 409 only flipped the terminal catastrophically-over-cap
  scan side. q10's sibling probe after ADR 409 already showed lineitem at only 2 scans, so
  this was not the q03-style terminal lineitem re-scan loop.

The bounded path was therefore to spill those already-materialized build batches through the
existing spilled-build runtime. The one-pass reservoir remains unnecessary for q10's
accumulator seam and stays reserved for q05/non-terminal repeated-scan work.

Mechanism:
- `prepare_reoriented_inner_join_runtime_from_build_batches` now estimates build hash bytes
  across the incoming batch vector before `concat_or_empty`.
- If the estimate exceeds the materialization cap, it writes each existing `RecordBatch` to
  `SpilledInnerJoinBuild::write_batch_chunks`, calls `finish`, and returns the existing
  `ReorientedInnerJoinBuild::Spilled` runtime.
- Below-cap builds keep the previous concat + in-memory `JoinBuildRows` path.

Scope guard:
- No q10/table branch.
- No cap bump.
- No second spill path.
- No concat-first spill; the over-cap branch writes the materialized batches directly.

## Discriminating Test

Added:
- `reoriented_materialized_build_over_cap_spills_batches_without_concat`

The test forces the materialized-batch hash estimate over the accumulator cap while keeping
the join output tiny. It asserts:
- low-budget runtime becomes `ReorientedInnerJoinBuild::Spilled`;
- `input_batches_written() == 2`, proving the two materialized batches are written
  separately rather than concatenated before spill;
- high-budget runtime remains `InMemory`;
- spilled output equals the high-budget in-memory output and restores probe-first column
  order.

Targeted run:

```text
CARGO_CGROUP_JOBS=12 cargo test -p toron \
  reoriented_materialized_build_over_cap_spills_batches_without_concat --lib -- --nocapture

test exec::join::streaming::tests::reoriented_materialized_build_over_cap_spills_batches_without_concat ... ok
```

## q10 Driver

Command:

```bash
rm -f target/tpch/adr410_q10_sf10_scan_progress_post.jsonl \
  target/tpch/adr410_q10_sf10_join_profiles_post.jsonl \
  target/tpch/adr410_q10_sf10_post.json \
  target/tpch/adr410_q10_sf10_post.err
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr410_q10_sf10_scan_progress_post.jsonl \
  --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr410_q10_sf10_join_profiles_post.jsonl \
  target/release/toron-tpch perf-run-one --engine toron --query q10 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr410_q10_sf10_post.json 2> target/tpch/adr410_q10_sf10_post.err || true
```

Result:

| query | pre-410 class | post-410 status | post-410 producer | execute_ms | spill_bytes | verdict |
|---|---|---|---|---:|---:|---|
| q10 | `reoriented join build accumulator`, 1,776,981,960 / 1,073,741,824, 7.633 s | fail | `sort materialization`, 76,797,659 / 67,108,864 | 114,625.123 | 215,703,848 | accumulator cleared; downstream wall named |
| q10 repro | same | fail | `sort materialization`, 76,797,659 / 67,108,864 | 110,671.178 | 215,703,848 | downstream wall reproduced |

The primary is a wall-move, not a green claim. q10 no longer fails at the reoriented build
accumulator; it now reaches a downstream sort materialization cap. A second post-fix probe
reproduced the same next wall and the same spill byte count.

## q10 I/O Sanity

Aggregated from `target/tpch/adr410_q10_sf10_scan_progress_post.jsonl`:

| table | scan starts | batch events | raw rows | raw bytes | output bytes |
|---|---:|---:|---:|---:|---:|
| customer | 2 | 2,930 | 3,000,000 | 594,512,574 | 594,512,574 |
| lineitem | 2 | 117,162 | 119,972,104 | 5,447,037,080 | 1,218,397,296 |
| nation | 2 | 2 | 50 | 1,552 | 1,552 |
| orders | 3 | 43,947 | 45,000,000 | 912,691,296 | 912,691,296 |

The spill route did not introduce a q03-style runaway re-scan. The lineitem input remains a
two-scan pattern, not dozens of passes.

The join-build profile hook recorded only the small in-memory nation build:

```json
{"replay_id":2,"strategy":"HashPartitioned","key_plan":"int64","build_rows":25,"unique_keys":25,"duplicate_keys":0,"overflow_rows":0,"max_rows_per_key":1,"row_list_value_bytes":1000,"overflow_heap_capacity_bytes":0,"build_micros":11}
```

That is expected: the new over-cap materialized branch goes to the spilled-build runtime,
not `JoinBuildRows`.

## q12 Fold-In Assessment

Command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=360s \
  target/release/toron-tpch perf-run-one --engine toron --query q12 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr410_q12_sf10_post.json 2> target/tpch/adr410_q12_sf10_post.err || true
```

Result:

| query | status | producer | requested | available | execute_ms | verdict |
|---|---|---|---:|---:|---:|---|
| q12 | fail | `streamed inner join build spill resident state` | 1,073,809,552 | 1,073,741,824 | 7,394.609 | not folded |

The edge remains the spill resident-state guard itself, 67,728 bytes over the retained
footprint budget. ADR 410 did not relax the guard or change spill accounting; q12 remains a
separate named residual.

q12 classification: genuine retained-state shortfall, not a small accounting or eviction
ordering bug for ADR 410. The failing check is `SpilledInnerJoinBuild::guard_resident_state`,
which sums resident index bytes, writer peak, one page reserve, and pinned decoded bytes
against `join_spill_retained_footprint_memory_cap`. The guard is intentionally still firing;
changing it would be a spill resident-footprint policy cut, not part of q10's materialized
build routing.

## q03 sf=1 Four-Way Control

Command:

```bash
rm -rf target/tpch/perf_adr410_q03_sf1_control
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr410_q03_sf1_control \
  > target/tpch/adr410_q03_sf1_control.out 2> target/tpch/adr410_q03_sf1_control.err
```

Artifact:
`target/tpch/perf_adr410_q03_sf1_control/latest_summary.jsonl`

| engine | status | median_ms | spread | fingerprint | flags |
|---|---|---:|---:|---|---|
| toron | pass | 375.271 | 0.023 | `666e0ac028036f70` | none |
| rune | pass | 372.865 | 0.055 | `666e0ac028036f70` | none |
| polars_sql | pass | 104.758 | 0.479 | `fnv1a64:a3b5c15847833ac3` | `unstable` |
| duckdb | pass | 48.000 | 0.583 | `fnv1a64:73b142150e59739f` | `unstable,schema_mismatch` |

SQL/Rune remains closed at sf=1. Polars/DuckDB are context rows; both are unstable at this
small scale.

## sf=10 Correctness Controls

Command shape:

```bash
for q in q03 q04 q08 q19; do
  systemd-run --user --same-dir --wait --collect --quiet --pipe \
    --property=MemoryMax=4G \
    --property=MemoryHigh=4G \
    --property=TasksMax=512 \
    --property=RuntimeMaxSec=360s \
    --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
    target/release/toron-tpch correctness --query "$q" --sf 10 --mode strict \
      --data-source parquet --data-dir target/tpch/data
done
```

Artifact:
`target/tpch/adr410_correctness_controls.out`

| query | status | elapsed_ms |
|---|---|---:|
| q03 | pass@compare | 24,441.890 |
| q04 | pass@compare | 15,521.399 |
| q08 | pass@compare | 29,662.489 |
| q19 | pass@compare | 100,955.483 |

The won ground holds: q03's one-pass completion, q08's 403 path, q19's spill path, and q04's
green control all remain correct under the fixed 4 GiB scope.

## Verification

Completed:

```text
CARGO_CGROUP_JOBS=12 cargo check -p toron --lib
CARGO_CGROUP_JOBS=12 cargo test -p toron reoriented_materialized_build_over_cap_spills_batches_without_concat --lib -- --nocapture
cargo fmt --all
CARGO_CGROUP_JOBS=12 cargo test -p toron reoriented_ --lib
CARGO_CGROUP_JOBS=12 cargo test -p toron --test architecture_test
CARGO_CGROUP_JOBS=12 cargo build -p toron-tpch --release --features polars-sql-reference
CARGO_CGROUP_JOBS=4 cargo test -p toron --no-fail-fast
CARGO_CGROUP_JOBS=4 cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

Results:
- reoriented join family: 12 passed / 0 failed;
- architecture: 2079 passed / 0 failed;
- `toron --no-fail-fast`: passed;
- `toron-tpch --features polars-sql-reference --no-fail-fast`: passed;
- whitespace clean;
- no tracked `.perf.data`.

## Verdict

ADR 410 is an honest q10 wall-move:
- the `reoriented join build accumulator` failure is gone;
- the existing spilled-build writer/probe path handles the over-cap materialized build;
- q10 advances to a downstream `sort materialization` cap;
- q12 is explicitly not folded;
- q03/q04/q08/q19 controls remain correct.

Next producers:
- q10: sort materialization cap, 76,797,659 / 67,108,864;
- q12: streamed inner join build spill resident-state edge, 67,728 bytes over;
- q05: deferred to ADR 411's reservoir/non-terminal completion work.
