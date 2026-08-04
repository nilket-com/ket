# ADR 411 - Results: sort materialization cap alignment

ADR 411 aligns sort materialization with the existing materialization-family policy instead of
leaving sort on the raw 64 MiB task budget. The 4 GB scope, cgroup, `memory_per_task`, join
materialization cap, and spill retained-footprint cap are unchanged.

## Implementation

Production diff:

- `SORT_MATERIALIZATION_MEMORY_CAP_MULTIPLIER = 16`
- `sort_materialization_memory_cap(task_memory)` beside `execute_sort`
- `execute_sort` compares its materialization estimate against that named cap
- producer string remains `sort materialization`

The policy is aligned, not unbounded. Sorts over the family cap still fail with
`sort materialization`.

## Discriminating Tests

Targeted test command:

```bash
CARGO_BUILD_JOBS=12 cargo test -p toron --test sort_ordering_test sort_materialization -- --nocapture
```

Result:

- `sort_materialization_allows_raw_overage_within_family_cap` pass
- `sort_materialization_still_fails_over_family_cap` pass

The first fixture exceeds the raw per-task cap but fits the aligned family cap; it fails on
pre-411 code. The second fixture exceeds the aligned family cap and still fails with producer
`sort materialization`, locking that this is not an unbounded cap bump.

## Phase 0 Wall

Pre-fix command used the clean ADR 411 plan worktree and a freshly checked release binary:

```bash
CARGO_BUILD_JOBS=12 cargo build -p toron-tpch --release --features polars-sql-reference
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=240s \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr411_q10_sf10_scan_progress_pre.jsonl \
  --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr411_q10_sf10_join_profiles_pre.jsonl \
  target/release/toron-tpch perf-run-one --engine toron --query q10 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q10_sf10_pre.json 2> target/tpch/adr411_q10_sf10_pre.err
```

Artifact: `target/tpch/adr411_q10_sf10_pre.json`.

| query | status | producer | requested | available | execute_ms | spill_bytes |
|---|---|---|---:|---:|---:|---:|
| q10 pre | fail | `sort materialization` | 76,797,659 | 67,108,864 | 116,877.642 | 215,703,848 |

Policy comparison:

- raw task budget: 67,108,864
- aligned family cap: 1,073,741,824
- q10 request: 76,797,659

The request is over the raw cap and well under the existing materialization-family cap.

## q10 Result

Post-fix command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=300s \
  --setenv=TORON_TPCH_PARQUET_SCAN_PROGRESS_PATH=target/tpch/adr411_q10_sf10_scan_progress_post.jsonl \
  --setenv=TORON_JOIN_BUILD_PROFILE_PATH=target/tpch/adr411_q10_sf10_join_profiles_post.jsonl \
  target/release/toron-tpch perf-run-one --engine toron --query q10 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q10_sf10_post.json 2> target/tpch/adr411_q10_sf10_post.err
```

Artifact: `target/tpch/adr411_q10_sf10_post.json`.

| query | pre | post | execute_ms | output_rows | spill_bytes | verdict |
|---|---|---|---:|---:|---:|---|
| q10 | `sort materialization` fail | pass | 119,431.618 | 381,105 | 215,703,848 | sort wall cleared |

Persisted correctness:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=300s \
  target/release/toron-tpch correctness --query q10 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q10_sf10_correctness.out 2> target/tpch/adr411_q10_sf10_correctness.err
```

Result: `tpch_correctness query=q10 sf=10 mode=strict phase=compare status=pass reason=none
elapsed_ms=127364.1`.

The scan-progress ledger is byte-identical pre/post for q10:

| table | scan events | rows | bytes |
|---|---:|---:|---:|
| customer | 2,932 | 3,000,000 | 594,512,574 |
| lineitem | 117,164 | 119,972,104 | 5,447,037,080 |
| nation | 4 | 50 | 1,552 |
| orders | 43,950 | 45,000,000 | 912,691,296 |

ADR 411 clears the final sort wall; it does not change q10's upstream re-scan profile.

## q12 Fence

q12 remains on its distinct resident-state producer:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=60s \
  target/release/toron-tpch perf-run-one --engine toron --query q12 --mode strict --sf 10 \
    --data-source parquet --data-dir target/tpch/data \
  > target/tpch/adr411_q12_sf10_fence.json 2> target/tpch/adr411_q12_sf10_fence.err
```

| query | status | producer | requested | available | execute_ms |
|---|---|---|---:|---:|---:|
| q12 | fail | `streamed inner join build spill resident state` | 1,073,809,552 | 1,073,741,824 | 7,825.894 |

This confirms ADR 411's blast radius is sort-only. The q12 resident-footprint policy cut remains
separate.

## Controls

sf=10 persisted correctness controls:

| query | result |
|---|---:|
| q03 | `pass@compare`, 26,966.3 ms |
| q04 | `pass@compare`, 16,727.2 ms |
| q08 | `pass@compare`, 31,736.8 ms |
| q19 | `pass@compare`, 110,806.3 ms |

q03 sf=1 four-way control:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --property=RuntimeMaxSec=180s \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/1.5.4/duckdb \
  target/release/toron-tpch perf --sf 1 --mode strict --runs 7 --warmup 1 --query q03 \
    --engines toron,rune,polars_sql,duckdb --data-source parquet --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr411_q03_sf1_fourway \
  > target/tpch/adr411_q03_sf1_fourway.out 2> target/tpch/adr411_q03_sf1_fourway.err
```

Artifact: `target/tpch/perf_adr411_q03_sf1_fourway/latest_summary.jsonl`.

| engine | status | median_ms | spread | fingerprint | spill_bytes | flags |
|---|---|---:|---:|---|---:|---|
| toron | pass | 387.017 | 0.040 | `666e0ac028036f70` | 0 | none |
| rune | pass | 388.371 | 0.033 | `666e0ac028036f70` | 0 | none |
| polars_sql | pass | 108.166 | 0.304 | `fnv1a64:a3b5c15847833ac3` | 0 | `unstable` |
| duckdb | pass | 48.000 | 0.562 | `fnv1a64:73b142150e59739f` | 0 | `unstable,schema_mismatch` |

Toron/Rune shape closure holds (`666e0ac...`, no spill). DuckDB and Polars remain context rows
for this short sf=1 run because both are flagged unstable.

## Verification

Commands run:

```bash
cargo fmt --all --check
CARGO_BUILD_JOBS=12 cargo test -p toron --test sort_ordering_test sort_materialization -- --nocapture
CARGO_BUILD_JOBS=12 cargo test -p toron --test architecture_test
CARGO_BUILD_JOBS=12 cargo test -p toron --no-fail-fast
CARGO_BUILD_JOBS=12 cargo test -p toron-tpch --features polars-sql-reference --no-fail-fast
```

Results:

- `sort_ordering_test sort_materialization`: 2 passed
- `architecture_test`: 2,079 passed, 0 failed
- `toron --no-fail-fast`: pass
- `toron-tpch --features polars-sql-reference --no-fail-fast`: pass

## Verdict

ADR 411 is a bounded policy-alignment cut:

- q10 sf=10 moved from a deterministic `sort materialization` wall to `pass@compare`;
- the request was over the raw 64 MiB cap but far under the already-established x16
  materialization-family cap;
- over-family sorts still fail with `sort materialization`;
- q12's distinct resident-state producer is untouched;
- q03/q04/q08/q19 controls remain correct.

Forward:

- q12: resident-footprint shedding/accounting policy;
- q05: non-terminal reservoir / completion cut;
- external sort remains the named mechanism for future sorts that exceed the family cap.
