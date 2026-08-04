# ADR 389 results - partition group spill family

## Summary

ADR 389 replaces total-spill partition-group sizing with max-active-group sizing for the streamed inner-build partition path. The old 378 stop-line treated the projected build's total spill volume as if it had to fit under the per-query spill budget at once. The new path estimates projected spill bytes per partition, chooses the smallest deterministic ordered group count whose maximum active group fits the group spill budget, and increases the partition count up to the bounded ceiling when skew leaves an individual group too large.

No budget cap was raised. `spill_bytes_reserved_per_query` remains `512 * 1024 * 1024` in `crates/toron/src/distributed/budget.rs`.

## Mechanism

- Added per-partition projected spill-byte estimation for materialized and streamed scan builds.
- Group sizing now tests `max(group_projected_spill_bytes) <= group_spill_budget`, not average or total alone.
- `group_spill_budget = spill_budget * 3 / 4`; with the unchanged 512 MiB budget, the active group payload budget is 402,653,184 bytes and the reserve is 134,217,728 bytes.
- If a single partition still exceeds the group budget, partition count doubles up to `STREAMED_INNER_BUILD_MAX_PARTITIONS = 1024`; only then is `streamed inner join build partition group sizing` reported as a named floor.
- The active group writer uses the group spill budget, so the runtime guard matches the sizing proof.

Projection was measured first by inspection of the existing path. The streamed inner build already routes through the projected build key/retained-column path added by the earlier projection cuts, so ADR 389 did not add another projection lever.

## Correctness And Guards

- `streamed_inner_build_group_count_uses_max_group_spill_not_average` proves group sizing is based on maximum group payload, not average payload.
- `streamed_inner_build_group_count_names_skew_floor_when_single_partition_exceeds_budget` proves skew produces the named producer instead of silently exceeding the group budget.
- `streamed_inner_build_partitioning_uses_spill_budget_and_ordered_groups` covers spill-budget-aware group sizing and ordered groups.
- `partitioned_reoriented_inner_runtime_matches_single_spill_projected_path` proves the partitioned path remains equivalent to the single-spill projected path.
- `reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order` now documents the extra bounded per-partition sizing scan: total estimate, per-partition spill sizing, live group, and materialized comparison path.

## SF=10 Results

| query | result | evidence |
| --- | --- | --- |
| q12 | advanced to time/no-row | The previous immediate partition-group-sizing failure is gone. A release persisted sf=10 run under fixed cap timed out at 600s before emitting a raw harness row, so there is no sf=10 correctness claim. |
| q03 | advanced to downstream scan-output wall | Release persisted sf=10 now runs 185,893.9 ms and fails at `scan output accumulator` with requested 1,073,790,848 / available 1,073,741,824 bytes, peak RSS 1,923,968 KiB, spill bytes 1,434,880,384. |
| q05 | advanced to downstream scan-output wall | Release persisted sf=10 now runs 183,718.3 ms and fails at `scan output accumulator` with requested 1,073,790,848 / available 1,073,741,824 bytes, peak RSS 2,356,708 KiB, spill bytes 1,434,880,384. |
| q10 | advanced to downstream scan-output wall | Release persisted sf=10 now runs 105,210.2 ms and fails at `scan output accumulator` with requested 1,073,790,848 / available 1,073,741,824 bytes, peak RSS 1,445,292 KiB, spill bytes 1,434,880,384. |

The q03/q05/q10 result is an honest wall move. These cells no longer stop at the partition-group-sizing producer; once that producer is removed, they proceed far enough to reveal a later scan-output accumulator wall. They are not green.

## Correctness Runs

- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12 --sf 0.01` passed.
- `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01` passed.

No sf=10 correctness was claimed for q12/q03/q05/q10 because none produced a pass row at sf=10.

## Verification

- `cargo fmt -p toron`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order`
- `scripts/cargo-cgroup test -p toron --no-fail-fast`
- `scripts/cargo-cgroup test -p toron-tpch --no-fail-fast`

Final formatting, diff, and architecture hygiene checks are run as part of the seal.

## Follow-Up

The next ranked producer is the downstream scan-output accumulator for q03/q05/q10. q12 is now a time/no-row cell and needs a separate time-profile if it remains strategically important. The partition-group-sizing family is no longer the blocking producer for the measured coverage set.
