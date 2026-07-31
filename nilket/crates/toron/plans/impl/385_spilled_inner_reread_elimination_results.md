# ADR 385 results - streamed spilled-inner reread elimination

## Summary

ADR 385 ships a bounded reread-elimination cut for streamed spilled-inner materialization. The q19 canary no longer
loops over the same few thousand spill pages until timeout: persisted sf=10 q19 now completes and passes correctness.

The implementation has two layers:

- per-probe-chunk page de-dup for streamed spilled-inner materialization, so each output chunk no longer starts a fresh
  physical page-read set;
- budgeted decoded-page pinning for `SpilledInnerJoinBuild`, so resident-safe spilled-inner builds can read/decode their
  page set once and serve later materialization from decoded pages.

This is not a global cache cap bump. Pinned decoded bytes are charged through the existing spill resident guard, and the
path falls back if the decoded-page set would exceed the retained-footprint budget.

## Phase 0

ADR 384 proved the q19 pathology with a temporary per-page counter:

| query | physical reads | unique pages | max reads/page | interpretation |
| --- | ---: | ---: | ---: | --- |
| q19 | 4,095,000 | 3,908 | 1,273 | severe reread over a small hot page set |
| q03 | 3,684,000 | 58,596 | 99 | broader moderate reread |
| q07 | 688,500 | 688,500 | 1 | unique-once negative control |

The existing 8 MiB decoded-page LRU was not the right fix: the counter observed `cache_hits=0`, and q19 reused pages
across materialization/work-unit boundaries rather than inside one small output chunk. The lever was to widen the
dedupe scope and, only when budget-safe, pin the decoded page set locally on the spill build.

## Implementation

Changed code:

| file | change |
| --- | --- |
| `crates/toron/src/exec/join.rs` | generic streamed spilled-inner materialization loads unique match pages once per probe chunk |
| `crates/toron/src/exec/join/streaming.rs` | projected streamed spilled-inner path uses the same page loader; partitioned scan-build path can preload resident-safe partition builds |
| `crates/toron/src/exec/join/spill.rs` | `SpilledInnerJoinBuild::try_pin_decoded_pages` and pinned decoded-page accounting |

Budgeting details:

- `try_pin_decoded_pages` first checks
  `resident_index_bytes + writer_peak + JOIN_SPILL_PAGE_SIZE + loaded_page_bytes_estimate()` against
  `join_spill_retained_footprint_memory_cap(memory_cap)`;
- it then reads pages once, sums `record_batch_memory_bytes`, and stops without pinning if actual resident bytes would
  exceed the same cap;
- when pinning succeeds, the normal LRU is cleared and `pinned_decoded_bytes` is included in `guard_resident_state`;
- no task memory, spill-byte, or resident-footprint cap constants changed.

The partitioned scan-build path also has a conservative resident preload branch. It estimates the build footprint before
attempting a load and falls back to the sequential spill path if the resident-safe check fails. This is the q19-enabling
branch for the small hot page set; it is not used to claim q03/q07 greens.

## Correctness

| command | result |
| --- | --- |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19 --sf 0.01` | pass@compare, 7670.4 ms |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q19 --sf 1` | pass@compare, 5635.6 ms |
| `CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q19 --sf 10 --data-source parquet --data-dir target/tpch/data` | pass@compare, 166717.8 ms |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01` | pass@compare, 10316.3 ms |
| `scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07 --sf 0.01` | pass@compare, 10573.6 ms |

Focused regression tests:

- `sequential_partitioned_scan_build_loads_spill_pages_once_for_repeated_matches`
- `partitioned_reoriented_inner_runtime_matches_single_spill_projected_path`
- `parallel_partitioned_scan_build_matches_sequential_ordered_emit`
- `loaded_partition_work_is_send_static`

The read-once regression uses a counting `AsyncFileOps` wrapper and proves the resident-safe sequential branch reads two
spilled pages once despite repeated matches across multiple output chunks.

## sf=10 outcome

| query | before | after ADR 385 |
| --- | --- | --- |
| q19 | time/no-row; ADR 384 counted 4.095M physical reads over 3,908 pages | `status=pass`, `execute_ms=107188.371984`, `output_row_count=1`, `spill_bytes=134225536` |
| q03 | moderate reread control | `memory_budget_exhausted` at `scan output accumulator`, `execute_ms=83635.89498`, `spill_bytes=1434880384` |
| q07 | unique-once negative control | 180s timeout/no row, no new green claim |

q19 now reaches a real row at sf=10 and passes persisted correctness. q03 benefits are not claimed as a green because
the measured run advanced to a separate scan-output accumulator wall. q07 remains a time-wall control.

## Profile

Post-fix q19 was re-profiled with the symbol-retaining 381 method:

```text
systemd-run --user --scope --same-dir --collect --quiet \
  --property=MemoryMax=4G --property=MemoryHigh=4G --property=TasksMax=512 \
  timeout --signal=INT 150s perf record -F 99 -g --call-graph dwarf \
    -o target/tpch/profiles/adr385_q19_pinned.perf.data -- \
    target/release/toron-tpch perf-run-one \
      --query q19 --mode strict --sf 10 --run-index 0 --data-source parquet --data-dir target/tpch/data
```

The profile window completed with q19 already finishing. The old q19 signature, repeated `pread64`/`_copy_to_iter` plus
Arrow IPC decode under `SpilledInnerJoinBuild::read_batch_page`, is no longer the dominant wall. Top local frames in the
saved report include:

| frame | sample % | interpretation |
| --- | ---: | --- |
| `__memmove_avx_unaligned_erms` | 11.83% | remaining materialization/copy pressure |
| `arrow_ipc::reader::RecordBatchDecoder::next_buffer` | 9.35% | one-time decoded-page pin / IPC decode |
| `SpilledInnerJoinBuild::read_batch_page` | 7.45% | pinned page clone / page access path |
| `encode_partition_key_value` | 4.80% | remaining partition/key work |
| `arrow_select::interleave::interleave_bytes` | 4.58% | output materialization |
| `_copy_to_iter` | 3.10% | residual kernel read copy, no longer dominant |

The exact post-fix physical reads/page count was not recorded with committed instrumentation. The 384 counter was
temporary and removed before seal. ADR 385 instead keeps a focused read-once regression test plus the post-fix completion
and profile evidence. The result is still sufficient to classify q19 as fixed for reach/correctness; it should not be
reported as an exact `1,273x -> 1x` counter measurement.

## Verification

| command | result |
| --- | --- |
| `scripts/cargo-cgroup fmt -- --check` | pass |
| `scripts/cargo-cgroup check -p toron --all-targets` | pass |
| `scripts/cargo-cgroup test -p toron sequential_partitioned_scan_build_loads_spill_pages_once_for_repeated_matches --lib` | pass |
| `scripts/cargo-cgroup test -p toron partitioned_reoriented_inner_runtime_matches_single_spill_projected_path --lib` | pass |
| `scripts/cargo-cgroup test -p toron parallel_partitioned_scan_build_matches_sequential_ordered_emit --lib` | pass |
| `scripts/cargo-cgroup test -p toron loaded_partition_work_is_send_static --lib` | pass |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `scripts/cargo-cgroup test -p toron-tpch` | pass |

The `exec/join.rs` root remains at the ADR 352 line-count ratchet after moving the shared page loader into
`exec/join/streaming.rs`.

## Outcome

ADR 385 is a real wall-mover:

- q19 repeated spill-page rereads are eliminated enough for sf=10 completion;
- q19 is correctness-proven at sf=0.01, sf=1, and persisted sf=10;
- q03 and q07 are measured controls, not overclaimed;
- no cap was raised and pinned decoded pages are budget-accounted;
- the next q19 profile is materialization/copy/key work, not the 384 repeated physical read pathology.
