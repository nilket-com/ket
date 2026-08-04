# ADR 378 results - bounded partition/group spill

## Summary

ADR 378 makes the streamed inner-build partitioning spill-budget-aware. It sizes both:

- `P`, the writer partition count, so one partition writer is not allowed to own the full projected build spill; and
- `G`, the ordered live partition-group count, so only one bounded group of partition spill files is live before probing.

This is a correct wall-mover, not a green result. The immediate ADR 377 `streamed inner join build spill writer` walls
for q03/q05/q10 are gone, and those cells now run into the time/no-row class. q12 does not ship an unbounded grouping
plan: it stops at the named `streamed inner join build partition group sizing` residual when the measured build size
would require a degenerate group plan.

No spill cap was raised.

## Phase 0 classification

ADR 377 showed q03/q05/q10/q12 were already writing projected build columns. The wall was not another projection gap.
The first implementation probe also found that the q03/q05/q10 path was not the same materialized-build path that q07
had exercised; it was a scan-build/scan-probe streamed inner path still using the single-writer helper. ADR 378 therefore
extends the bounded partition/group machinery to all three streamed inner-build shapes:

- scan build + scan probe;
- materialized build + scan probe;
- materialized build + materialized probe.

The old debug producer labels used to identify those paths were removed before seal; the core producer names remain
shape-named and de-branded.

| cell | ADR 377 wall | classification | ADR 378 outcome |
| --- | --- | --- | --- |
| q03 | `streamed inner join build spill writer`, 536,871,224 / 536,870,912 | projected scan-build/scan-probe writer spill | old writer wall gone; time/no-row in sample window |
| q05 | `streamed inner join build spill writer`, 536,871,224 / 536,870,912 | projected scan-build/scan-probe writer spill | old writer wall gone; time/no-row in sample window |
| q10 | `streamed inner join build spill writer`, 536,871,224 / 536,870,912 | projected scan-build/scan-probe writer spill | old writer wall gone; time/no-row in sample window |
| q12 | `streamed inner join build spill writer`, 536,876,880 / 536,870,912 | projected build spill, but required grouping is unbounded | named stop-line: `streamed inner join build partition group sizing`, 8,396,405,047 / 536,870,912 |

## Implementation

- `streamed_inner_build_partition_count` now sizes by both resident-memory footprint and spill writer target. The spill
  side uses a target below the full per-query spill budget, then rounds to the existing power-of-two partition policy.
- `streamed_inner_build_group_count` sizes live partition groups from the measured build spill against a group target
  below the full spill budget. If the group count would become the old per-partition re-scan in disguise, it returns the
  named sizing residual rather than forcing a fragile plan.
- Partition groups are contiguous ascending partition ranges, preserving the visible partition order from ADR 377.
- Each group scans the build once, writes only rows whose bucket falls in that group, rebuilds/probes its partitions, and
  cleans the group's spill files before moving to the next group.
- Cumulative `spill_bytes` telemetry still records total bytes written across groups. The live budget decision is made on
  the active writer/group, not the cumulative counter.

## Tests

- `streamed_inner_build_partitioning_uses_spill_budget_and_ordered_groups`
  - proves the spill budget can raise `P` even when memory alone would not;
  - proves `G` is ordered and bounded for the just-over-spill-budget case;
  - checks the group ranges.
- `partitioned_reoriented_inner_runtime_matches_single_spill_projected_path`
  - keeps Float64 and Decimal parity against the single-spill projected path;
  - now accepts `1 + group_count` build scans: one estimate scan plus one build scan per live group, not once per
    writer partition.
- Existing scan-probe tests were updated for the explicit build-estimation scan and still prove the probe scan is not
  materialized.

## sf=10 family re-measure

Fixed cap, persisted data, release perf, Toron-only:

| cell | command shape | result |
| --- | --- | --- |
| q03 | perf sf=10, strict, persisted | advanced past old spill writer wall; manually stopped after more than 5 minutes with no row |
| q05 | perf sf=10, strict, persisted, `timeout 180s` | exit 124, no row; time/no-row |
| q10 | perf sf=10, strict, persisted, `timeout 180s` | exit 124, no row; time/no-row |
| q12 | perf sf=10, strict, persisted | fail, `spill_budget_exhausted`, producer `streamed inner join build partition group sizing`, 8,396,405,047 / 536,870,912 |

q03/q05/q10 are not green. ADR 378 removed their immediate spill writer wall and exposed the next time-bound behavior.
q12 is not green and correctly refuses the unbounded grouping branch as a named residual.

## Correctness

Persisted sf=0.01 strict correctness passes after the change:

| query | result |
| --- | --- |
| q03 | pass@compare |
| q05 | pass@compare |
| q10 | pass@compare |
| q12 | pass@compare |

No sf=10 correctness claim is made for q03/q05/q10 because they did not produce a row in the bounded measurement
window. q12 fails before producing a result.

## Verification

| command | result |
| --- | --- |
| `cargo fmt --check` | pass |
| `git diff --check` | pass |
| no debug/no-qNN executor scan | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron streamed_inner_build_partitioning_uses_spill_budget_and_ordered_groups --lib` | pass |
| `scripts/cargo-cgroup test -p toron partitioned_reoriented_inner_runtime_matches_single_spill_projected_path --lib` | pass |
| `scripts/cargo-cgroup test -p toron --lib` | pass, 1182/1182 |
| `scripts/cargo-cgroup test -p toron --test architecture_test` | pass, 2079/2079 |
| `scripts/cargo-cgroup test -p toron-tpch` | pass |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01 --data-source parquet` | pass@compare |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q05 --sf 0.01 --data-source parquet` | pass@compare |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q10 --sf 0.01 --data-source parquet` | pass@compare |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01 --data-source parquet` | pass@compare |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |

## Seal notes

- No cap bump.
- No projection overclaim; projected writes were already present.
- No broad spill abstraction.
- No qNN references in `crates/toron/src/exec.rs` or `crates/toron/src/exec/`.
- Outcome: wall-mover to time/no-row for q03/q05/q10, named group-sizing residual for q12.
