# ADR 387 results - scan-output streaming for the inner/join family

## Summary

ADR 387 targeted the current sf=10 scan-output accumulator family from ADR 386:

```text
q03 q05 q08 q10
```

The cut was diagnostic-first and shape-fenced. It found that q08's persisted sf=10 plan reaches the reoriented inner
chain grouped path, but its base shape is a bare `Exchange(Join(0))` instead of the older filtered
`Exchange(Filter(Join(0)))` base. The existing path streamed some scans, but q08 still materialized a later stage scan
build through the ordinary build path and hit the scan-output accumulator.

The implementation extends the existing reoriented inner-chain grouped path to recognize the bare base, preserve the
optional base filter for the filtered shape, and stream only stage scan builds whose estimated scan output would exceed
the existing scan-output cap. That last memory-aware gate is load-bearing: an initial always-streamed bare-base version
removed the q08 accumulator at sf=10 but regressed q08 sf=1 into a timeout. The final version keeps sf=1 on the fast
materialized path and uses streaming only for measured large scan outputs.

Outcome:

- q08 no longer emits the prior scan-output accumulator wall under a 420s sf=10 run, but it does not green; it advances
  to a bounded time/no-row classification.
- q03/q05/q10 do not share the q08 miss and remain live scan-output accumulator walls.
- No cap changed.
- The full `toron` and `toron-tpch` no-fail-fast suites remain green.

## Phase 0 Producer Table

| query | before producer | measured miss class | implementation coverage | after |
| --- | --- | --- | --- | --- |
| q08 | `scan output accumulator`, 1,073,762,000 / 1,073,741,824, spill 0 | bare `Exchange(Join(0))` base in the reoriented inner-chain grouped path; a later scan build was prepared by the ordinary materialized build path | covered by bare-base recognition plus memory-aware stage scan-build streaming | no accumulator row surfaced in 420s; advanced to time/no-row, not green |
| q03 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | not the q08 bare-base miss | not covered | unchanged scan-output wall |
| q05 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | not the q08 bare-base miss | not covered | unchanged scan-output wall |
| q10 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824, spill 1,434,880,384 | not the q08 bare-base miss | not covered | unchanged scan-output wall |

`ScanRequest` construction stays on the shared `bound_scan_request` builder. The q08 miss was not a dropped
projection/PBC/runtime-filter/row-identity/profile/morsel/row-limit/cancellation field; it was the consumer edge routing
the scan build through materialization.

## Implementation

The executor changes are limited to `crates/toron/src/exec/join/streaming.rs`.

Key points:

- Replaced the boolean base check with `ReorientedInnerChainGroupedBaseScanProbeMode::{FilteredJoin0, BareJoin0}`.
- `FilteredJoin0` preserves the prior behavior for the older filtered base.
- `BareJoin0` is accepted for q08's persisted shape.
- `try_streaming_reoriented_inner_chain_scan_probe_base_grouped` now handles both `Exchange(Filter(Join(0)))` and
  `Exchange(Join(0))`, applying the optional base filter only when it exists.
- Added `estimate_streaming_scan_output_bytes`, which mirrors the materialized scan-output accounting path by reading
  the shared scan request, applying row limits and scan filters, and summing projected batch array memory.
- Bare-base stage scan builds stream only when the estimate exceeds `join_output_materialization_memory_cap`.

That gate keeps the new path from taking over small-scale plans where materialization is cheaper.

## Live sf=10 Remeasure

Fixed-cap persisted runs used the same 4 GiB cap and the existing sf=10 Parquet surface.

| query | status | class after ADR 387 | execute ms / timeout | peak RSS KiB | spill bytes | evidence |
| --- | --- | --- | ---: | ---: | ---: | --- |
| q08 | no row | time/no-row after q08 accumulator removal | `timeout 420s` | n/a | n/a | no JSON row emitted; no scan-output accumulator failure surfaced under the bound |
| q03 | fail | unchanged scan-output accumulator | 172,103.6 | 1,744,840 | 1,434,880,384 | `BudgetExhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824 |
| q05 | fail | unchanged scan-output accumulator | 198,675.7 | 2,234,808 | 1,434,880,384 | `BudgetExhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824 |
| q10 | fail | unchanged scan-output accumulator | 168,056.2 | 1,259,680 | 1,434,880,384 | `BudgetExhausted`, producer `scan output accumulator`, 1,073,790,848 / 1,073,741,824 |

ADR 387 therefore claims **1/4 coverage** of the measured family:

- q08 advanced from scan-output memory wall to time/no-row;
- q03/q05/q10 stayed scan-output and need a separate consumer-edge cut.

No new sf=10 green row was produced, so there is no new sf=10 correctness-green claim in this ADR.

## Small-Scale Controls

The small-scale perf guard caught and shaped the final implementation.

| command | result |
| --- | --- |
| q08 sf=0.01 strict persisted correctness | pass@compare, 11,569.0 ms |
| q08 sf=0.01 dev reachability | pass, 2,296.9 ms |
| q08 sf=0.01 strict reachability | pass, 2,149.5 ms |
| q08 sf=1 strict persisted perf, Toron-only | pass, execute 4,856.3 ms, peak RSS 615,268 KiB, spill 0 |

The initial always-streamed bare-base version timed out at q08 sf=1. The final memory-aware gate avoids that regression
by keeping small scan builds on the existing materialized path.

## Tests and Verification

Focused executor tests:

| command | result |
| --- | --- |
| `scripts/cargo-cgroup test -p toron reoriented_inner_chain_handles_right_growing_project_join_shape -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron bound_scan_request_carries_every_bound_scan_field -- --nocapture` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_chain_grouped_base_classifies_filtered_and_bare_scan_probe -- --nocapture` | pass |

Package and integration gates:

| command | result |
| --- | --- |
| `scripts/cargo-cgroup check -p toron` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast` | pass |
| `cargo fmt --check -p toron` | pass |
| `git diff --check` | pass |
| `rg -n "\\b[qQ][0-9]{1,2}\\b" crates/toron/src/exec.rs crates/toron/src/exec` | no matches |

The full `toron --no-fail-fast` run includes the CTAS/delete/distributed regression family that caught the earlier
empty-batch/schema regression.

## Outcome

ADR 387 is an honest wall-mover:

- q08's measured scan-output edge is removed without a small-scale perf regression;
- q08 is not green and is now part of the time/perf cluster;
- q03/q05/q10 remain a separate scan-output accumulator family;
- the next memory/reach cut should start from q03/q05/q10 rather than assuming the q08 edge generalizes.
