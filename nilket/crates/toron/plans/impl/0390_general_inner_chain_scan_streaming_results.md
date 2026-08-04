# ADR 390 results - general inner-chain stage-build scan streaming

## Summary

ADR 390 removes the base-mode dependency that stranded eligible reoriented-inner-chain stage-build scans behind the
generic base traversal. The per-stage memory-aware gate now decides each stage independently, and the generic base path
consumes non-in-memory stage runtimes through `probe_reoriented_inner_join_streaming` instead of the old synchronous
in-memory-only probe.

No cap was raised. The sf=1 control stays on the materialized path with zero spill.

## Phase 0 Diagnosis

The q03/q05/q10 scan-output wall was not caused by a missing stage loop. The stage loop and scan-output estimator already
existed. The miss was the combination of a declined global base mode and a synchronous generic-base consumer.

| query | prior producer | base mode | stage-build eligibility | old consumer path | verdict |
| --- | --- | --- | --- | --- | --- |
| q03 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824 bytes after 185,893.9 ms | `None` | stage build decomposes and exceeds the scan-output cap | generic base traversal used the synchronous in-memory probe | stream eligible stage builds and route generic base through the streaming probe |
| q05 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824 bytes after 183,718.3 ms | `None` | stage build decomposes and exceeds the scan-output cap | generic base traversal used the synchronous in-memory probe | same shared miss |
| q10 | `scan output accumulator`, 1,073,790,848 / 1,073,741,824 bytes after 105,210.2 ms | `None` | stage build decomposes and exceeds the scan-output cap | generic base traversal used the synchronous in-memory probe | same shared miss |

The probe/base scan was not pulled into this cut. The measured miss was on stage-build scans plus the generic-base
consumer path.

## Implementation

- The stage-build streaming decision no longer depends on
  `reoriented_inner_chain_grouped_base_scan_probe_mode` except for the existing forced filtered-base case.
- The generic base fallback is now asynchronous and calls `probe_reoriented_inner_join_streaming`, so a streamed
  stage-build runtime is not prepared and then rejected by an in-memory-only probe.
- The generic base fallback has its own named guard, `reoriented inner chain base accumulator`, for any residual base
  materialization.
- The old synchronous `probe_reoriented_inner_join` helper was removed; local parity tests use the streaming probe for
  both in-memory and spilled/runtime cases.
- Added `reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime`, which forces a generic base mode
  (`None`), forces a streamed stage runtime under low budget, and asserts digest parity against the high-budget
  materialized baseline.

## SF=10 Results

| query | result | evidence |
| --- | --- | --- |
| q03 | advanced to time/no-row | A persisted release sf=10 run continued far past the previous 185.9s scan-output wall without emitting that named failure, then was stopped manually after about 35 minutes. No sf=10 correctness is claimed. |
| q05 | advanced to time/no-row | A persisted release sf=10 run continued far past the previous 183.7s scan-output wall without emitting that named failure, then was stopped manually after about 28 minutes. No sf=10 correctness is claimed. |
| q10 | advanced to time/no-row | A persisted release sf=10 run continued far past the previous 105.2s scan-output wall without emitting that named failure, then was stopped manually after about 21 minutes. No sf=10 correctness is claimed. |

This is an honest wall move. The repeated scan-output cycle for the q03/q05/q10 inner-chain stage-build edges is gone,
but the cells are not green.

## Small-Scale Correctness And Controls

- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01` passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q05 --sf 0.01` passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q10 --sf 0.01` passed.
- Dev reach at sf=0.01 passed for q03/q05/q10.
- Release sf=1 persisted controls passed with zero spill:
  - q03: 8,808.7 ms, peak RSS 2,698,512 KiB, `spill_bytes=0`.
  - q05: 9,600.0 ms, peak RSS 2,649,920 KiB, `spill_bytes=0`.
  - q10: 8,923.0 ms, peak RSS 2,721,956 KiB, `spill_bytes=0`.

The sf=1 controls prove the memory-aware gate did not force the streamed path where materialization still fits.

## Verification

- `cargo fmt -p toron`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup test -p toron --lib reoriented_inner_chain_generic_base_consumes_streamed_stage_runtime -- --nocapture`
- `scripts/cargo-cgroup test -p toron --lib reoriented_inner -- --nocapture`
- `scripts/cargo-cgroup test -p toron --no-fail-fast`
- `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast`

Final seal checks also covered no qNN references under `src/exec`, formatting, and `git diff --check`.

## Follow-Up

q03/q05/q10 join the time/no-row cluster. The next cut should be chosen from a fresh board/ranking rather than assuming
these cells still have a scan-output producer. If the time cluster remains the priority, profile q03/q05/q10 with the
same symbol-retaining method used for q19 before cutting.
