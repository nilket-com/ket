# ADR 381 results - symbol-retaining spill read/copy profile

## Summary

ADR 381 is complete as a diagnostic ADR. No executor or harness behavior changed.

The symbol-retaining profile fixed the ADR 380 attribution gap. The q03 and q07 samples do not point to one shared
spill-read frame:

- q03 is dominated by spill-page decode/decompression/allocation work, with
  `arrow_ipc::reader::RecordBatchDecoder::next_buffer` as the named dominant read/decode frame.
- q07 is dominated by partition/repartition/take/copy work, especially `__memmove`,
  `partition_bucket_with_scratch`, `partition_bucket_if_non_null_with_scratch`, and
  `arrow_select::take::take_bytes`. Spill read is visible but not dominant.

So ADR 382 should not force a single q03/q07 read-path fix. The next read-path cut is best scoped to the q03-style
spill-decode path; q07 should split to the partition/take/copy profile family.

## Symbol gate

The first build with `CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false` did not override the root
release profile's `debug = 0` / `strip = true`. The symbol gate stopped the profiling path correctly. The build was
rerun with explicit values:

```bash
CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_STRIP=none \
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
```

The hard gate then passed:

```text
target/release/toron-tpch: ELF 64-bit LSB pie executable, x86-64, ... with debug_info, not stripped
```

`readelf -S target/release/toron-tpch | rg 'debug|symtab'` reported `.debug_abbrev`, `.debug_aranges`,
`.debug_info`, `.debug_line`, `.debug_loc`, `.debug_ranges`, `.debug_str`, and `.symtab`.

`nm -C target/release/toron-tpch | rg 'decode_record_batch_ipc|SpilledInnerJoinBuild::read_batch_page|read_plain_at|read_plain_exact'`
resolved the spill-read symbols:

```text
<wendelon::storage::file_ops::BlockingFileOps as wendelon::storage::file_ops::AsyncFileOps>::read_plain_at
toron::exec::join::spill::read_plain_exact::{{closure}}
toron::exec::join::spill::SpilledInnerJoinBuild::read_batch_page::{{closure}}
toron::exec::join::spill::decode_record_batch_ipc
```

No coarse no-symbol profile was accepted.

## Sampling method

All accepted profiles used an explicit software event:

```text
perf record -e task-clock -F 99 --call-graph dwarf ...
```

The reported percentages are therefore over `task-clock` samples, not a hybrid `cpu_atom/cycles` vs
`cpu_core/cycles` split. q03 and q07 used `-D 60000` to skip startup and sample the engine window.

| artifact | event | sample window | samples | lost samples | note |
| --- | --- | ---: | ---: | ---: | --- |
| `target/tpch/profiles/adr381_q03.perf.data` | `task-clock` | 60.016 s | 5,760 | 0 | accepted |
| `target/tpch/profiles/adr381_q07.perf.data` | `task-clock` | 60.042 s | 5,830 | 2,176 / 27.18% | accepted with caveat |
| `target/tpch/profiles/adr381_q07_lowfreq.perf.data` | `task-clock` | bounded retry | 2,871 | 39.87% | worse retry, not used |
| `target/tpch/profiles/adr381_q06_control.perf.data` | `task-clock` | 4.712 s | 465 | 0 | non-spill control |

The q07 conclusion is limited to large, repeated signals because that sample lost 27.18% of samples. The lower-frequency
retry lost more, so the original q07 sample is the least-bad artifact and is labeled accordingly.

SVGs were rendered for visual inspection:

- `target/tpch/profiles/adr381_q03.svg`
- `target/tpch/profiles/adr381_q07.svg`
- `target/tpch/profiles/adr381_q06_control.svg`

The numeric conclusions below come from `perf report --stdio -g`, not from reading the SVG by eye.

## q03 profile

q03 timed out as expected under the bounded 120 s profile window. The accepted 60 s engine-window sample names the
spill-decode path:

| frame | sample % | interpretation |
| --- | ---: | --- |
| `__memset_avx2_unaligned_erms` | 17.08% | allocation/zero-fill pressure |
| `arrow_ipc::reader::RecordBatchDecoder::next_buffer` | 10.54% | dominant spill-page decode/decompress/copy frame |
| `_int_malloc` | 9.98% | allocation pressure under decode/materialization |
| futex wait/wake stacks | about 7% | worker coordination/wait |
| `__x64_sys_pread64` | 4.13% | spill file read syscall path |
| `__memmove_avx_unaligned_erms` | 3.72% | copy pressure |
| `SpilledInnerJoinBuild::touch_cache_entry` | 2.66% | spill cache bookkeeping |
| `_copy_to_iter` | 2.33% | kernel copy from shmem read |
| `decode_record_batch_ipc` | 0.09% self | Toron wrapper is visible but inlined/dependency work dominates |
| `SpilledInnerJoinBuild::read_batch_page::{{closure}}` | 0.36% self | Toron caller visible; downstream work dominates |
| `read_plain_at::{{closure}}` | 0.09% self | file-op wrapper visible; syscall/copy is below it |

Named q03 dominant frame: `arrow_ipc::reader::RecordBatchDecoder::next_buffer`.

ADR 382 read-path candidate: reduce IPC decode/decompression allocation/copy in the spill read path, or avoid repeatedly
decoding spill pages into fresh Arrow buffers. The `.to_vec()` decrypt-copy candidate is not the dominant q03 frame in
this sample.

## q07 profile

q07 also timed out under the bounded profile window. The sample is weaker because of lost samples, but its dominant
signals are not the q03 spill-decode stack:

| frame | sample % | interpretation |
| --- | ---: | --- |
| `__memmove_avx_unaligned_erms` | 16.50% | dominant copy pressure |
| `partition_bucket_with_scratch` | 6.04% | partition bucket work |
| `partition_bucket_if_non_null_with_scratch` | 5.97% | partition bucket work |
| `arrow_select::take::take_bytes` | 5.83% | Arrow take/string copy work |
| `_int_malloc` | 4.56% | allocation pressure |
| `_copy_to_iter` | 4.34% | kernel copy from shmem read |
| `encode_partition_key_value` | 1.58% | key encoding |
| `Executor::repartition_exchange_batch` | 1.56% | repartition/copy path |
| `SpilledInnerJoinBuild::touch_cache_entry` | 1.13% | spill cache bookkeeping |
| `RecordBatchDecoder::next_buffer` | 0.62% | spill decode visible but not dominant |

Named q07 dominant family: partition/repartition/take/copy, not spill IPC decode. This should split from the q03
read-path lever unless a later profile contradicts it.

## Non-spill control

q06 was profiled with the same symbol-retaining build and `task-clock` event. It passed:

```text
query=q06 status=pass execute_ms=1096.097838 peak_rss_kib=23256 spill_bytes=0 flags=none
```

The q06 control is dominated by harness file hashing rather than spill reads:

| frame | sample % | interpretation |
| --- | ---: | --- |
| `stable_file_hash` / `fnv1a_update` | about 70% | persisted dataset hash validation |
| `_copy_to_iter` | 5.16% | file read/copy under hashing |

No spill-read/decode frame dominates q06, and q06 recorded `spill_bytes=0`. This validates that the q03 spill-decode
signal is not merely a profiling/harness artifact.

q14 was also sampled, but it recorded `spill_bytes=134225536`, so it is not used as the primary non-spill control.

## Parallelism side table

The side table was run with a 90 s wall window for default partition parallelism and forced sequential
`TORON_PARTITION_PARALLELISM=1`.

| query | setting | result | wall window |
| --- | --- | --- | ---: |
| q03 | default | timed out before row | 91 s |
| q03 | `TORON_PARTITION_PARALLELISM=1` | timed out before row | 90 s |
| q07 | default | timed out before row | 90 s |
| q07 | `TORON_PARTITION_PARALLELISM=1` | timed out before row | 90 s |

No speedup or slowdown is banked from this table because no run produced a row inside the side-table window.

The exact computed K is not surfaced by the current CLI or telemetry. Thread-level report rows showed worker engagement,
but not the semaphore K value. If a future engagement-policy cut depends on K, first add off-by-default reporting of
computed K and active-group resident peak. ADR 381 does not add that instrumentation.

## Named follow-ups

1. q03 read-path cut: reduce spill IPC decode/decompression allocation/copy, starting from
   `RecordBatchDecoder::next_buffer`.
2. q07 partition/take/copy cut: investigate `partition_bucket_*`, `arrow_select::take::take_bytes`,
   `repartition_exchange_batch`, and `__memmove` as a separate profile family.
3. Parallelism policy follow-up: expose computed K and active-group resident peak before using side-table data for a
   default-engagement decision.
4. Harness follow-up: persisted file hashing dominates short q06 profiles. It is outside execute-heavy q03/q07 delayed
   windows, but it is real wall overhead for short control runs.

## Verification

Profile commands ran inside `systemd-run --user --scope` with:

```text
MemoryMax=4G
MemoryHigh=4G
TasksMax=512
```

No source files changed. Large `.perf.data` artifacts remain under `target/tpch/profiles` and are not committed.

Final seal checks:

```text
scripts/cargo-cgroup test -p toron --no-fail-fast    # pass
git diff --check                                     # pass
executor no-qNN source scan                          # pass
```

## Verdict

ADR 381 fixes the ADR 380 symbol gap and names the next frame with enough precision to avoid a blind read-path rewrite.
q03 is a spill decode/decompression/allocation problem. q07 is a partition/repartition/take/copy problem. The next ADR
should choose one of those explicitly rather than treating them as a single time-cluster stack.
