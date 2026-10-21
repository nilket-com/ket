# ADR 376 results - partition-bucket churn

## Summary

ADR 376 removed the per-call hash scratch allocation and hoisted bucket computation for the held-batch partitioned
paths. The q07 profile did not show the hoped-for material drop in total partition/key share because the dominant q07
path is the streamed scan build re-read: each join partition re-scans the build input and must still compute buckets for
that partition. Caching that side would require reshaping the scan/spill read model or binding cached buckets to stable
row identity, so it is recorded as the ADR stop-line residual.

This is a correct partial perf cut, not a green or a broad win. The next q07 decision should be made from the cleaned
profile: either attack the remaining scan re-read partitioning with a larger substrate cut, or pivot to the parallelism
reassessment named by ADR 373.

## Phase 0 fork

| item | verdict | evidence |
| --- | --- | --- |
| A: scratch reuse | shipped | `partition_bucket_with_scratch` clears and reuses caller-owned `Vec<u8>`; exchange call sites and join selectors pass scratch instead of allocating inside the hash helper |
| B: held-batch bucket hoist | shipped for held batches | materialized partitioned left-outer path precomputes left/right `Vec<Option<u32>>` once per batch before the partition loop; streamed inner probe batch precomputes once before its partition loop |
| B: scan re-read cache | residual | streaming scan build still reads batches per partition; caching buckets across those reads would require row-identity/spill-read reshaping beyond the ADR stop line |
| null-key discriminator | shipped | non-null-safe precompute uses `partition_bucket_if_non_null_with_scratch`, producing one `Option<u32>` per row instead of retaining a separate `join_key_bytes` eligibility encode beside a bucket encode |

The exchange partitioner remains one-pass and out of scope. Its call sites were updated only because the shared helper
now has a scratch-capable variant.

## Implementation

- Moved the partition hash helper into the join spill substrate and added:
  - `partition_bucket_with_scratch`
  - `partition_bucket_if_non_null_with_scratch`
  - scratch-backed private hash helpers
- Kept `partition_bucket` as the fresh-buffer compatibility wrapper for tests and non-hot callers.
- Added `precompute_join_partition_buckets` plus `select_join_partition_batch_with_buckets`.
- Hoisted partition buckets for:
  - materialized partitioned left-outer left and right held batches
  - streamed inner probe batches
- Left the streaming scan build re-read path on the single-pass selector with scratch reuse only.

No cap, spill-budget, producer-name, or partition-count semantics changed.

## Tests

New focused tests:

- `exec::join::spill::tests::partition_bucket_with_scratch_matches_fresh_encoding`
- `exec::join::spill::tests::precomputed_partition_buckets_match_recompute_and_selector`

The parity test covers nulls, decimal, string, date-ish keys, multiple partition counts, null-safe joins,
non-null-safe joins, and retained unkeyed rows through the real selector path.

Verification run:

| command | result |
| --- | --- |
| `scripts/cargo-cgroup fmt -- --check` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron --lib partition_ -- --nocapture` | pass, 55/55 |
| `scripts/cargo-cgroup test -p toron --lib` | pass, 1181/1181 |
| `scripts/cargo-cgroup test -p toron --test architecture_test` | pass, 2079/2079 |
| `scripts/cargo-cgroup test -p toron-tpch` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01 --mode strict` | pass@compare |
| `git diff --check` | pass |

The full post-cleanup rerun used the same commands; the no-qNN executor scan remained clean.

## Profile

Method: symbolized release build, fixed 4 GB `systemd-run --user --scope`, persisted sf=10 q07, `perf record -F 99
--call-graph dwarf -D 60000`, 120s controlled timeout. Timeout exit `124` is expected: the query is sampled, not
required to complete.

Artifacts are uncommitted under:

| artifact | path |
| --- | --- |
| perf data | `target/tpch/profiles/adr376_q07.perf.data` |
| perf script | `target/tpch/profiles/adr376_q07.perf.script` |
| folded stacks | `target/tpch/profiles/adr376_q07.folded` |
| SVG | `target/tpch/profiles/adr376_q07.svg` |

Comparable `cpu_core/cycles/P` profile summary:

| stack / symbol | ADR 375 q07 | ADR 376 q07 |
| --- | ---: | ---: |
| `partition_bucket` / scratch variants | 7.21% | 5.15% `partition_bucket_with_scratch` + 5.01% `partition_bucket_if_non_null_with_scratch` |
| `encode_partition_key_value` | 4.55% | 2.83% |
| `SpilledInnerJoinBuild::write_page_batch` | 7.31% | 7.90% |
| `__memmove_avx_unaligned_erms` | 6.80% | 8.73% |
| `arrow_select::take::take_bytes` | 4.73% | 5.09% |
| `_int_malloc` | 3.04% | 3.32% |
| `shmem_file_write_iter` | visible residual | 11.05% in call graph |

Interpretation:

- The old fresh-buffer `partition_bucket` symbol is gone from the hot path; scratch-backed helpers replaced it.
- `encode_partition_key_value` drops on the dominant core stream, but the overall partition/key family does not drop
  materially because the scan re-read path still computes buckets per partition.
- The held-batch B hoist is structurally in place, but q07's sampled hot path is the residual scan build side, not the
  held-probe side.
- The next hot families are real spill/write/Arrow-copy work plus the residual scan re-read partitioning.

This means ADR 376 reached the stop line rather than the optimistic profile outcome. The residual is named and bounded:
reducing it requires a broader change to avoid re-reading/repartitioning the build scan per join partition.

## Parallelism reassessment

The cleaned q07 profile is now mostly real one-thread work:

- spill page write / tmpfs write
- Arrow take / memmove
- allocator churn around Arrow and spill structures
- residual per-partition scan re-read bucket computation

There is no longer an obvious small O(1) accounting bug like ADR 374, nor a clean local buffer reuse like ADR 375. The
next q07 cut should be selected deliberately:

- a larger scan-build partitioning substrate cut if we want to remove the residual O(rows x partitions) re-read bucket
  computation; or
- the parallelism arc if we decide the remaining work should first be spread across cores.

## Seal notes

- No cap bump.
- No qNN references in `crates/toron/src/exec*`.
- Partition assignment and output semantics are unchanged by test.
- q07 did not green; green was not the ADR's success metric.
