# ADR 403 results - q08 reoriented build accumulator streaming

## Summary

q08's sf=10 `reoriented join build accumulator` wall is gone. The query now completes under the fixed 4 GB scope and
passes the persisted correctness oracle.

The Phase 0 fork refined the initial A1/A2/B split: the failing build was scan-decomposable and reached the reoriented
scan-build decision, but the decision guarded only the raw materialized scan-output bytes. The raw batches fit, so the
stage chose `MaterializedBatches`; the downstream hash-build accumulator then estimated ~1.8 GiB against the ~1 GiB
materialization cap and failed at `prepare_reoriented_inner_join_runtime_from_build_batches`. ADR 403 now checks both
the raw scan-output estimate and the estimated hash-build accumulator footprint before choosing the materialized path.

This is a generic routing/decision fix, not a q08 branch and not a budget increase.

## Phase 0 classification

| item | finding |
|---|---|
| pre-403 wall | q08 sf=10 failed at `memory_budget_exhausted`: `reoriented join build accumulator`, requested ~1.8 GiB / available ~1 GiB (ADR 398) |
| runtime entry | reoriented scan-build decision path, ending in `MaterializedBatches` and then `prepare_reoriented_inner_join_runtime_from_build_batches` |
| decomposer verdict | scan-decomposable enough to materialize through `materialize_streaming_scan_output_for_cap`; not a derived-build stop-line |
| miss class | decision miss: raw scan-output bytes fit, estimated hash-build accumulator bytes did not |
| orientation | no planner orientation change; the fix is execution-side routing to the existing streamed scan-build runtime when the hash accumulator would exceed the cap |
| fence change | none; `decompose_streaming_scan_probe_input` was not loosened |

## Implementation

- Added `reoriented_stage_scan_build_decision_for_materialized_batches`, a small shared decision helper.
- `reoriented_inner_stage_scan_build_decision` now computes `estimated_build_hash_bytes_for_batches(&batches, &stage.build_keys)` after materializing a scan-shaped build candidate.
- The stage streams if either:
  - raw scan-output bytes exceed `join_output_materialization_memory_cap(task_budget.memory)`, or
  - estimated hash-build accumulator bytes exceed that same cap.
- If both estimates fit, it preserves the existing `MaterializedBatches` path.

No q08/table/query branch was added, no spill path was invented, and no cap was bumped.

## Tests

Targeted discriminating test:

```text
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron reoriented_scan_build_decision_checks_hash_accumulator_bytes --lib
```

Result:

```text
test exec::join::streaming::tests::reoriented_scan_build_decision_checks_hash_accumulator_bytes ... ok
```

The test locks the three decision cases:

- scan fits + hash accumulator exceeds cap -> `Stream`
- scan exceeds cap + hash fits -> `Stream`
- both fit -> `MaterializedBatches`

## q08 result

Stable q08 sf=10 perf command:

```bash
rm -rf target/tpch/perf_adr403_q08_sf10_final && \
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  --setenv=TORON_TPCH_DUCKDB_BIN=/home/me/.duckdb/cli/latest/duckdb \
  target/release/toron-tpch perf \
    --sf 10 \
    --mode strict \
    --runs 3 \
    --warmup 1 \
    --query q08 \
    --engines toron,duckdb \
    --data-source parquet \
    --data-dir target/tpch/data \
    --output-dir target/tpch/perf_adr403_q08_sf10_final \
  > target/tpch/adr403_q08_sf10_perf_final.out
```

Artifact: `target/tpch/perf_adr403_q08_sf10_final/latest_summary.jsonl`.

| engine | status | execute_ms_median | spread | output rows | peak RSS | spill bytes | flags |
|---|---|---:|---:|---:|---:|---:|---|
| toron | pass | 17,747.712 | 0.048 | 2 | 1,441,700 KiB | 0 | none |
| duckdb | pass | 224.000 | 0.049 | 2 | 409,728 KiB | 0 | `schema_mismatch` |

q08 moved from:

```text
fail(memory_budget_exhausted): reoriented join build accumulator, requested ~1.8 GiB / available ~1 GiB
```

to:

```text
pass, 2 rows, no spill, persisted shape fingerprint cb686ae2c1202c97
```

Correctness command:

```bash
systemd-run --user --same-dir --wait --collect --quiet --pipe \
  --property=MemoryMax=4G \
  --property=MemoryHigh=4G \
  --property=TasksMax=512 \
  target/release/toron-tpch correctness \
    --query q08 \
    --sf 10 \
    --mode strict \
    --data-source parquet \
    --data-dir target/tpch/data \
  > target/tpch/adr403_q08_sf10_correctness.out
```

Result:

```text
tpch_correctness query=q08 sf=10 mode=strict phase=compare status=pass reason=none elapsed_ms=27769.6
```

## Controls

q04 control:

| engine | status | execute_ms | output rows | peak RSS | spill bytes | flags |
|---|---|---:|---:|---:|---:|---|
| toron | pass | 5,380.478 | 5 | 1,229,184 KiB | 0 | none |
| duckdb | pass | 132.000 | 5 | 293,080 KiB | 0 | none |

Artifact: `target/tpch/perf_adr403_q04_sf10/latest_raw.jsonl`.

q19 control:

| engine | status | execute_ms | output rows | peak RSS | spill bytes | flags |
|---|---|---:|---:|---:|---:|---|
| toron | pass | 89,413.791 | 1 | 290,316 KiB | 51,471,552 | none |
| duckdb | pass | 221.000 | 1 | 417,592 KiB | 0 | `schema_mismatch` |

Artifact: `target/tpch/perf_adr403_q19_sf10/latest_raw.jsonl`.

The q19 control still uses the existing spill path and remains green, so ADR 403 did not regress the working spill
reference.

## Verification

Completed:

```text
scripts/cargo-cgroup fmt -- --check
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup check -p toron --all-targets
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron reoriented_scan_build_decision_checks_hash_accumulator_bytes --lib
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron --test architecture_test
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup build -p toron-tpch --release
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron --no-fail-fast
CARGO_CGROUP_JOBS=4 scripts/cargo-cgroup test -p toron-tpch --no-fail-fast
git diff --check
git ls-files '*.perf.data'
```

The architecture ratchet remains at `2079 passed; 0 failed`. `git ls-files '*.perf.data'` returned no tracked perf
data. A de-branding grep over `crates/toron/src/exec/join/streaming.rs` found no q08/table-specific branches.

## Verdict

ADR 403 is an A-class extension, but more precisely a **hash-accumulator decision miss** rather than a decomposer/fence
miss. q08 is now green at sf=10 with live persisted correctness. The first concrete Thread 3 brick landed without a
new spill subsystem, without a cap bump, and without a query-specific branch.
