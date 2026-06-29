# ADR 377 results - per-partition build spill

## Summary

ADR 377 removes the whole-build-per-partition re-scan from the partitioned streamed inner-build path. The build side is
now scanned once, routed into per-partition spill pages, and each active partition rebuilds its in-memory index only when
that partition is probed.

This is a correct wall-mover/perf cut, not a green claim. q07's sampled profile no longer shows the pre-377
partition/key re-scan symbols as the dominant residual; the next visible stack is tmpfs spill read/write plus
memcpy/allocator/syscall work. The q03/q05/q10/q12 family now reaches the shared `streamed inner join build spill
writer` spill-byte wall quickly instead of running for minutes on the old re-scan path.

## Phase 0 map

| item | verdict |
| --- | --- |
| Build side | Confirmed re-scanned. `probe_partitioned_streamed_inner_scan_build` looped over active partitions and called the streaming build writer inside the loop, so the build source was read once per active partition. |
| Probe side | Held once. The function receives a materialized `probe_batch`; the new path precomputes probe buckets once and partitions that held batch once. No probe scan is introduced. |
| Partition/file handles | Bounded by active probe partitions and the existing `streamed_spill_partition_count` cap (`<= 1024`). Writers are opened only for active probe partitions, not every theoretical partition. |
| Resident memory | Pages are written unindexed. Each active partition rebuilds its index immediately before probing and is cleaned up immediately after probing, preserving one-partition-resident behavior. |
| Spill-byte peak | All active partition page payloads may exist before probing, so the router tracks aggregate partition spill bytes and checks the unchanged per-query spill budget. Exceeding it returns `spill_budget_exhausted` from `partitioned streamed inner join build spill writer`; no cap was raised. |
| Routing surface | Routing happens after scan filtering and exchange repartitioning, before retained-column projection. The router buckets with original `partition_build_keys`; page writes use projected `write_build_keys`. |

## Implementation

- Added `partition_streaming_join_batch_with_buckets`, a local streaming-join helper that turns precomputed buckets into
  stable per-partition batches.
- Added unindexed `SpilledInnerJoinBuild` writes plus `rebuild_index_from_pages`. Partition pages are written once
  without retaining every partition's index; index rebuild is per active partition at probe time.
- Added `write_partitioned_streaming_scan_inner_build_batches`, which drives the scan/filter/exchange stream once and
  routes rows into active partition spills.
- Replaced the old build re-scan loop in `probe_partitioned_streamed_inner_scan_build` with:
  1. partition the held probe batch once;
  2. open spills for active probe partitions;
  3. scan the build once and route post-filter/post-exchange rows to active partition pages;
  4. finish and release write scratch;
  5. rebuild/probe/cleanup one active partition at a time.

No parallelism, cap bump, or broad spill abstraction was added.

## Tests

- `partitioned_reoriented_inner_runtime_matches_single_spill_projected_path`
  - covers Float64 and Decimal;
  - uses different routing and projected write key ordinals;
  - compares partitioned output to the single-spill projected path;
  - asserts the partitioned path scans the build source exactly twice total: once for estimation and once for the
    one-pass route, not once per partition.
- `precomputed_partition_buckets_match_recompute_and_selector`
  - keeps the bucket/precompute selector parity gate over nulls, decimal/string/date-ish keys, and multiple partition
    counts.

## Verification

| command | result |
| --- | --- |
| `scripts/cargo-cgroup fmt -- --check` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron partitioned_reoriented_inner_runtime_matches_single_spill_projected_path --lib` | pass |
| `scripts/cargo-cgroup test -p toron precomputed_partition_buckets_match_recompute_and_selector --lib` | pass |
| `scripts/cargo-cgroup test -p toron --lib` | pass, 1181/1181 |
| `scripts/cargo-cgroup test -p toron --test architecture_test` | pass, 2079/2079 |
| `scripts/cargo-cgroup test -p toron-tpch` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01` | pass@compare |
| `git diff --check` | pass |
| no-qNN executor scan | pass |

## Profile

Method: fixed 4 GB `systemd-run --user --scope`, persisted sf=10 q07, `perf record -F 99 --call-graph dwarf -D 60000`,
180s controlled timeout. Timeout exit `124` is expected: this samples the hot path, it does not require the query to
finish.

Artifacts are uncommitted under:

| artifact | path |
| --- | --- |
| perf data | `target/tpch/profiles/adr377_q07.perf.data` |
| perf script | `target/tpch/profiles/adr377_q07.perf.script` |
| folded stacks | `target/tpch/profiles/adr377_q07.folded` |
| SVG | `target/tpch/profiles/adr377_q07.svg` |

The repository release profile strips symbols, and the attempted environment override did not rebuild a symbolized
binary, so the ADR 377 sample is less Rust-symbol-rich than the ADR 376 sample. Still, the visible before/after shape is
clear:

| stack / symbol | ADR 376 q07 | ADR 377 q07 |
| --- | ---: | ---: |
| `partition_bucket_with_scratch` | 5.15% | not visible in top report/folded output |
| `partition_bucket_if_non_null_with_scratch` | 5.01% | not visible in top report/folded output |
| `encode_partition_key_value` | 2.83% | not visible in top report/folded output |
| `select_join_partition_batch` | 1.05% | not visible in top report/folded output |
| tmpfs spill write (`pwrite64` / `shmem_file_write_iter`) | visible residual | ~13.8% children in the top call graph |
| tmpfs spill read (`pread64` / `shmem_file_read_iter`) | visible residual | ~6.9% children in the top call graph |
| `__memmove_avx_unaligned_erms` | 8.73% | 5.12% self in the top report |
| allocator (`_int_malloc` / `malloc` / `realloc`) | visible residual | ~4.4% combined visible self |

Interpretation: the avoidable O(build x partitions) scan/bucket residual is gone from the visible hot stacks. The next
hot family is real spill I/O plus copy/allocation/syscall overhead, which is the expected input to the parallelism or
spill-I/O follow-up rather than another local bucket/re-scan peel.

## Family impact

One-run persisted sf=10 perf rows under the fixed 4 GB cap:

| query | ADR 366 toron status/time | ADR 377 toron status/time | producer after 377 |
| --- | ---: | ---: | --- |
| q03 | fail after 163.2s | fail after 4.68s | `spill_budget_exhausted`, `streamed inner join build spill writer`, 536,871,224 / 536,870,912 |
| q05 | fail after 164.3s | fail after 4.69s | `spill_budget_exhausted`, `streamed inner join build spill writer`, 536,871,224 / 536,870,912 |
| q10 | fail after 168.4s | fail after 4.90s | `spill_budget_exhausted`, `streamed inner join build spill writer`, 536,871,224 / 536,870,912 |
| q12 | fail after 120.0s | fail after 3.19s | `spill_budget_exhausted`, `streamed inner join build spill writer`, 536,876,880 / 536,870,912 |

Coverage verdict: the per-partition build-spill cut is systemic for the inner-build-spill family, but it is a
wall-mover. The common next wall is spill-byte volume, not memory residency or partition re-scan CPU.

## Parallelism reassessment

After ADR 377, the cleaned q07 profile is no longer dominated by the avoidable build re-scan/bucket computation. The
remaining visible cost is spill read/write, Arrow/copy, allocator, and syscall/futex overhead in a still single-threaded
execution model. That makes parallelism a reasonable next systemic lever, but the family measurement also identifies a
separate high-leverage memory/wall cut: reduce the `streamed inner join build spill writer` spill volume for
q03/q05/q10/q12.

## Seal notes

- No cap bump.
- No parallelism.
- No broad abstraction.
- No qNN references in `crates/toron/src/exec.rs` or `crates/toron/src/exec/`.
- q07 did not green; green was not the ADR's success metric.
