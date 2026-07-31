# ADR 383 results - active-group partition take

## Summary

ADR 383 shipped lever 1 only: the partitioned streamed-inner build path now materializes only the active partition
group for the current 378 group pass. The old path computed buckets for a scanned build batch, built/took all
partitions, and then discarded partitions outside the active group. The new helper skips rows outside the active group
before `take`, so inactive partitions are never materialized.

This is a correct partial perf cut, not a green claim. q07 remains in the time cluster, but the named `take_bytes` and
`__memmove` shares dropped in the bounded profile. The residual bucket work is still visible and is named as the lever-2
stop-line rather than forced into this ADR.

## Phase 0 counts

The runtime path is the 378 grouped partitioned build path:

- `G` = `group_count`
- `P` = `partition_count`
- active group range = `partition_group_bounds(P, G, group)`

The executor does not currently emit q07's concrete `P`/`G` values in perf JSON. Rather than add a new telemetry surface
for this perf peel, the count evidence is recorded from the exact runtime variables and code path that ADR 383 changes:

| term | before ADR 383 | after ADR 383 |
| --- | ---: | ---: |
| build rows scanned | `G x rows` | `G x rows` |
| bucket computations | `G x rows` | `G x rows` |
| partition batches considered for `take` | up to `G x P` | up to `P` total across the ordered groups |
| partitions materialized per group | all non-empty partitions in `[0, P)` | only non-empty partitions in that group's `[start, end)` |

The bucket-cache lever would be needed to reduce `G x rows -> rows`; that is not shipped here because it needs a
resident budget and scan-order proof. Holding the build remains the 378 stop-line because it would reintroduce the spill
peak that 378 removed.

## Implementation

- Added `partition_streaming_join_batch_active_group_with_buckets`.
  - It validates bucket count and active range.
  - It still errors on buckets outside `partition_count`.
  - It skips buckets outside `[partition_start, partition_end)` before building the `UInt32Array` indices.
  - It preserves ascending partition order and per-partition row order.
- Changed only `write_partitioned_streaming_scan_inner_build_batch` to use the active-group helper.
- Left `partition_streaming_join_batch_with_buckets` unchanged for probe partitioning and other all-partition callers.

Diff footprint:

| file | change |
| --- | --- |
| `crates/toron/src/exec/join/streaming.rs` | active-group helper + one call-site change + focused parity test |

No budget files or cap constants changed.

## Tests

New focused test:

- `active_group_partitioning_matches_all_partitions_filtered`
  - compares active-group output against the old all-partitions output filtered to the same active range;
  - includes an unkeyed row (`None` bucket);
  - checks partition order and batch equality.

Existing parity tests rerun:

- `partitioned_reoriented_inner_runtime_matches_single_spill_projected_path`
- `reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order`
- `parallel_partitioned_scan_build_matches_sequential_ordered_emit`

All passed.

## Correctness

| command | result |
| --- | --- |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q07 --sf 0.01` | pass@compare, 2193.1 ms |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q07 --sf 1` | pass@compare, 745772.9 ms |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q03 --sf 0.01` | pass@compare |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q05 --sf 0.01` | pass@compare |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q10 --sf 0.01` | pass@compare |

q07 sf=10 did not produce a row in the bounded profile window, so there is no persisted sf=10 correctness claim.

## sf=10 controls

q03/q05/q10 were rerun as bounded persisted sf=10 controls under the fixed 4 GiB scope. Each was interrupted after 60s
with no JSON row and no stderr, matching the existing time/no-row class rather than surfacing a new budget wall.

| query | fixed-cap control |
| --- | --- |
| q03 | timeout/no row, no stderr |
| q05 | timeout/no row, no stderr |
| q10 | timeout/no row, no stderr |

## Profile

Method: symbol-retaining release build, fixed 4 GiB `systemd-run --user --scope`, persisted sf=10 q07,
`perf record -e task-clock -F 99 --call-graph dwarf -D 60000`, 180s timeout. Timeout exit `124` is expected because
the query is sampled, not required to complete.

Artifact:

| artifact | path |
| --- | --- |
| perf data | `target/tpch/profiles/adr383_q07.perf.data` |
| perf report | `target/tpch/profiles/adr383_q07_report.txt` |

The q07 sample captured 11,608 samples and lost 13.15%, so the conclusion is limited to large frame movements.

| frame | ADR 381 q07 | ADR 383 q07 | read |
| --- | ---: | ---: | --- |
| `__memmove_avx_unaligned_erms` | 16.50% | 13.77% | lower, but still dominant copy pressure |
| `arrow_select::take::take_bytes` | 5.83% | 4.31% | active-group take reduced the named take work |
| `partition_bucket_with_scratch` + `partition_bucket_if_non_null_with_scratch` | 12.01% | 13.98% | unchanged/high residual; lever 2 remains |
| `encode_partition_key_value` | 1.58% | 1.96% | unchanged residual |
| `RecordBatchDecoder::next_buffer` | 0.62% | 0.72% | not the q07 dominant frame |

Interpretation:

- Lever 1 worked on the targeted `take_bytes`/copy component.
- The bucket precompute remains `G x rows` and did not drop because lever 2 was not shipped.
- q07 is still dominated by partition/copy/data movement, so this is a wall-mover/perf peel rather than a green.

## Lever 2 decision

The cross-pass bucket cache is not shipped. It still needs:

- a measured `rows x bucket_repr_bytes` resident budget;
- a compact representation;
- a scan-order-stability proof across the `G` build re-scans;
- a sidecar/disk design if the resident cache does not fit.

The residual is named as `partition-bucket-cache` / sidecar work. Holding the build remains out of scope because it would
reintroduce the 378 spill peak.

## Verification

| command | result |
| --- | --- |
| `scripts/cargo-cgroup fmt -- --check` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron active_group_partitioning_matches_all_partitions_filtered --lib` | pass |
| `scripts/cargo-cgroup test -p toron partitioned_reoriented_inner_runtime_matches_single_spill_projected_path --lib` | pass |
| `scripts/cargo-cgroup test -p toron reoriented_inner_runtime_streams_scan_build_batches_and_preserves_order --lib` | pass |
| `scripts/cargo-cgroup test -p toron parallel_partitioned_scan_build_matches_sequential_ordered_emit --lib` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass, 1187 lib tests plus integration tests |
| `scripts/cargo-cgroup test -p toron-tpch` | pass |
| `git diff --check` | pass |
| `rg -n "\\b[qQ][0-9]{1,2}\\b" crates/toron/src/exec crates/toron/src/exec.rs` | no matches |

## Outcome

ADR 383 is a correct partial perf cut:

- active-group-only take shipped;
- q07 correctness remains green at sf=0.01 and sf=1;
- q03/q05/q10 controls remain correct and in the same sf=10 wall class;
- no spill peak, resident guard, or budget cap was raised;
- the remaining bucket-cache sidecar is named for a future cut.
