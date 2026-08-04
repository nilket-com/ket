# ADR 388 results - scan-output streaming for the multi-table inner-build family

## Summary

ADR 388 targeted the q03/q05/q10 scan-output accumulator family left by ADR 387. The miss was not q08's bare-base
shape. q03/q05/q10 enter the reoriented inner-chain grouped path through a multi-table join whose grouped aggregate input
has no pre-aggregate `Project`; the old path required `Project` and returned before constructing the streamed stage
runtimes.

The implementation extends the existing reoriented inner-chain grouped path to handle that identity aggregate input. It
computes the required output columns directly from the group expression, aggregate arguments, and outer predicate when no
project expression exists, then reuses the existing streamed stage-build machinery and the ADR 387 memory-aware gate.

Outcome:

- q03/q05/q10 no longer hit the `scan output accumulator`.
- All three advance to the same named downstream producer: `streamed inner join build partition group sizing`.
- No sf=10 green row is claimed. The new producer is a pre-write partition-group sizing stop-line, so the old 1.43 GiB
  written-spill observation changes shape rather than becoming a reduced spill-volume result.
- sf=1 stays on the materialized path; the streamed path is selected only when the estimated scan output exceeds the
  existing scan-output cap.
- No cap changed, and the full `toron` and `toron-tpch` no-fail-fast gates remain green.

## Phase 0 Producer Table

The q03 persisted plan shape is:

```text
customer/orders base Join(0) -> lineitem stage Join(1) -> grouped aggregate
```

The q05 and q10 plans share the same identity-input reoriented grouped aggregate miss, with more stage joins.

| query | before producer | measured miss class | implementation coverage | after |
| --- | --- | --- | --- | --- |
| q03 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | multi-table reoriented inner-chain grouped aggregate; aggregate input is identity/no `Project`, so the old path returned before streaming the lineitem stage build | covered by identity-input grouped required-column analysis plus the memory-aware stage scan-build gate | advanced to `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |
| q05 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | same identity-input multi-stage reoriented grouped aggregate miss | covered | advanced to `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |
| q10 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | same identity-input multi-stage reoriented grouped aggregate miss | covered | advanced to `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |

`ScanRequest` construction stays on the shared `bound_scan_request` builder. The miss was not a dropped projection, PBC,
runtime-filter, row-identity, profile-hint, morsel, row-limit, trace, or cancellation field. It was the path's
over-specific requirement that the grouped aggregate input be a `Project`.

## Implementation

The executor changes are limited to `crates/toron/src/exec/join/streaming.rs`.

Key points:

- Added `reoriented_grouped_required_output_columns`.
- Projected grouped inputs keep the previous behavior: required columns come from the projection program.
- Identity grouped inputs collect required columns from the group expression and aggregate arguments, plus any outer
  predicate.
- `try_streaming_reoriented_inner_chain_grouped_aggregate` now accepts either `Project(input)` or an identity input.
- The existing filtered-base projected path keeps its prior always-streamed behavior.
- Identity inputs and bare bases use `reoriented_inner_stage_scan_build_exceeds_scan_output_cap`, preserving the ADR 387
  small-scale memory-aware selector.
- The grouped fold now goes through a helper that optionally filters, optionally projects, and then folds the grouped
  aggregate.

The new unit test
`reoriented_grouped_required_columns_cover_identity_and_projected_inputs` locks the required-column split:

- identity/no-project input retains group, aggregate, and outer-predicate columns;
- projected input retains only the projection program's input columns, matching the previous path.

## Memory-Aware Selector

The q03 selector was traced before removing local instrumentation:

| run | replay id | estimated scan-output bytes | cap | selected path |
| --- | --- | ---: | ---: | --- |
| q03 sf=10 strict persisted | `JoinReplayId(1)` | 8,683,765,360 | 1,073,741,824 | stream |
| q03 sf=1 strict persisted | `JoinReplayId(1)` | 869,291,056 | 1,073,741,824 | materialize |

The sf=1 control then passed on the trace-free code:

| query | sf | status | execute ms | peak RSS KiB | spill bytes | plan fingerprint |
| --- | ---: | --- | ---: | ---: | ---: | --- |
| q03 | 1 | pass | 2,890.0 | 2,698,384 | 0 | `dd78431fc3e271b1` |

This is the load-bearing small-scale guard: ADR 388 does not force the streamed path where the existing materialized path
fits under the scan-output cap.

## Live sf=10 Remeasure

Fixed-cap persisted runs used the existing 4 GiB scope and the sf=10 Parquet surface.

| query | status | class after ADR 388 | execute ms | peak RSS KiB | spill bytes | evidence |
| --- | --- | --- | ---: | ---: | ---: | --- |
| q03 | fail | advanced to partition-group sizing | 8,420.9 | 27,516 | 0 | `spill_budget_exhausted`, producer `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |
| q05 | fail | advanced to partition-group sizing | 12,512.6 | 27,480 | 0 | `spill_budget_exhausted`, producer `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |
| q10 | fail | advanced to partition-group sizing | 12,489.7 | 27,404 | 0 | `spill_budget_exhausted`, producer `streamed inner join build partition group sizing`, 7,198,326,240 / 536,870,912 |

ADR 388 therefore claims **3/3 coverage** of the measured q03/q05/q10 scan-output family:

- the scan-output accumulator producer is gone for all three;
- all three share the same downstream producer;
- none is green, so no new sf=10 correctness-green claim is made.

## Spill Before/After Characterization

| query | before producer | before spill bytes | after producer | after spill bytes | characterization |
| --- | --- | ---: | --- | ---: | --- |
| q03 | `scan output accumulator` | 1,434,880,384 | `streamed inner join build partition group sizing` | 0 | changed-shape: sizing fails before partition-group spill pages are written |
| q05 | `scan output accumulator` | 1,434,880,384 | `streamed inner join build partition group sizing` | 0 | changed-shape: sizing fails before partition-group spill pages are written |
| q10 | `scan output accumulator` | 1,434,880,384 | `streamed inner join build partition group sizing` | 0 | changed-shape: sizing fails before partition-group spill pages are written |

This is not a spill-volume reduction claim. ADR 388 removes the scan-output materialization and exposes the partition
grouping feasibility check. The next cut is the partition-group sizing/q12 family, not another projection-only
spill-volume pass.

## Correctness And Controls

Small-scale correctness and reachability:

| command | result |
| --- | --- |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01 --data-source memory` | pass@compare, 5,174.4 ms |
| `scripts/cargo-cgroup run -p toron-tpch -- run --query q03 --mode dev --sf 0.01` | pass, 158.3 ms |
| `scripts/cargo-cgroup run -p toron-tpch -- run --query q03 --mode strict --sf 0.01` | pass, 94.3 ms |

No persisted sf=10 correctness command is reported for q03/q05/q10 because none produced a pass row. The ADR 388 bar for
a green row was therefore not reached.

## Tests And Verification

Focused executor tests:

| command | result |
| --- | --- |
| `scripts/cargo-cgroup test -p toron reoriented_grouped_required_columns_cover_identity_and_projected_inputs -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_chain_grouped_base_classifies_filtered_and_bare_scan_probe -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_chain_handles_right_growing_project_join_shape -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron bound_scan_request_carries_every_bound_scan_field -- --nocapture` | pass |

Package and integration gates:

| command | result |
| --- | --- |
| `cargo fmt -p toron` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast` | pass |

The full `toron --no-fail-fast` run includes the CTAS/delete/distributed regression family that caught the earlier
empty-batch/schema regression.

## Outcome

ADR 388 is an honest 3/3 wall-mover:

- q03/q05/q10 no longer die at the scan-output accumulator;
- all three advance to a shared partition-group sizing stop-line;
- sf=1 stays materialized through the memory-aware gate;
- no cap changed and no sf=10 green is overclaimed.

Fresh ranking after this cut:

1. `streamed inner join build partition group sizing`: q03/q05/q10 now share this with the q12 family.
2. q21's tiny existence-set resident wall.
3. q08/q07 time cluster and the remaining carried-forward memory tail.

The next reach cut should start from the partition-group sizing producer, because ADR 388 turns q03/q05/q10 into the
same family rather than leaving them as scan-output accumulator cells.
