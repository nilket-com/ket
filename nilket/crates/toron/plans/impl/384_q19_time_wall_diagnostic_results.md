# ADR 384 results - q19 time-wall diagnostic

## Summary

q19 is a proven repeated spill-page read pathology. The symbol-retaining profile names the hot execution path as
streamed spilled-inner materialization:

- dependency hot frame: `arrow_ipc::reader::RecordBatchDecoder::next_buffer` at 21.61%;
- Toron path visible in the report: `SpilledInnerJoinBuild::read_batch_page`,
  `materialize_streamed_spilled_inner_matches`, and `visit_streaming_spilled_inner_probe_batch`;
- supporting costs: LZ4 decode, AES-GCM-SIV decrypt, `pread64`/tmpfs copy, memset/memmove, allocation.

The counted page-read artifact proves this is not a flamegraph-shape inference. In a 180s bounded q19 window, the
temporary counter observed 4,095,000 physical page reads over only 3,908 unique `(spill_id, page_index)` keys; the
hottest page was physically read 1,273 times. q07, measured with the same counter, read each page exactly once in its
120s window.

Classification: **repeated-read pathology**. ADR 385 should be a re-read elimination cut for the streamed spilled-inner
materialization path, not a broad parallelism cut and not a generic "same floor, larger volume" spill-format decision.

## Symbol gate

The profile build used the ADR 381 hard symbol gate:

```text
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_PROFILE_RELEASE_STRIP=false \
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup build -p toron-tpch --release
```

Gate evidence:

```text
target/release/toron-tpch: ... with debug_info, not stripped
readelf -S target/release/toron-tpch -> .debug_* sections and .symtab present
nm -C target/release/toron-tpch -> read_plain_at, SpilledInnerJoinBuild::read_batch_page,
decode_record_batch_ipc, probe_partitioned_streamed_inner_scan_build symbols resolved
```

The symbol gate passed before sampling.

## q19 profile

Method: fixed 4 GiB `systemd-run --user --scope`, persisted sf=10 q19, `perf record -e task-clock -F 99
--call-graph dwarf -D 60000`, 180s timeout. The timeout exit is expected; this is an engine-window sample, not a
completion run.

Artifacts are uncommitted under `target/tpch/profiles/`:

| artifact | path |
| --- | --- |
| perf data | `adr384_q19.perf.data` |
| perf report | `adr384_q19_report.txt` |
| caller report | `adr384_q19_caller_report.txt` |
| perf script | `adr384_q19.perf.script` |
| folded stacks | `adr384_q19.folded` |
| SVG | `adr384_q19.svg` |

Sampling quality:

```text
Samples: 11K task-clock
Total Lost Samples: 0
```

Top frames:

| frame | sample % | interpretation |
| --- | ---: | --- |
| `arrow_ipc::reader::RecordBatchDecoder::next_buffer` | 21.61% | IPC page decode / LZ4 decompress |
| `__memset_avx2_unaligned_erms` | 14.62% | buffer initialization / decode materialization |
| `__memmove_avx_unaligned_erms` | 6.57% | copy pressure |
| `aws_lc_0_40_0_aes256gcmsiv_dec` | 5.39% | spill page decrypt |
| `_int_malloc` | 4.61% | allocation |
| `_copy_to_iter` under `__x64_sys_pread64` | 4.34% | tmpfs read copy |
| `arrow_data::data::ArrayData::validate_values` | 3.68% | Arrow UTF8 validation |

The dominant dependency frame is Arrow IPC decode, but the Toron executor path is the streamed spilled-inner probe /
materialization path (`SpilledInnerJoinBuild::read_batch_page` ->
`materialize_streamed_spilled_inner_matches` -> `visit_streaming_spilled_inner_probe_batch`).

## Read-count artifact

The read-count artifact used a temporary env-gated counter keyed by `(spill kind, spill_id, page_index)`. It recorded:

- logical `read_page` requests;
- cache hits;
- physical cache-miss reads immediately before `read_plain_exact`.

The instrumentation was removed before seal; no engine source change remains.

Counter runs used fixed 4 GiB `systemd-run --user --scope`, persisted sf=10, and `timeout --signal=INT`. stdout was
empty because no sampled query completed.

| query | window | physical reads | unique pages | physical repeats | max reads/page | read pattern |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| q19 | 180s | 4,095,000 | 3,908 | 4,091,092 | 1,273 | severe repeated physical reads over a tiny page set |
| q03 | 120s | 3,684,000 | 58,596 | 3,625,404 | 99 | repeated reads, but spread across a much larger page set |
| q07 | 120s | 688,500 | 688,500 | 0 | 1 | unique-once physical reads |

Representative final q19 counter line:

```text
spill_read_counter logical_reads=4095000 logical_unique=3908 logical_repeats=4091092 cache_hits=0 \
physical_reads=4095000 physical_unique=3908 physical_repeats=4091092 \
max_logical_reads_per_page=1273 max_physical_reads_per_page=1273
```

This proves q19 re-reads the same pages physically. The result is not inferred from flamegraph shape.

## Control comparison

| query | dominant hot frame | RSS / spill bytes | read pattern | verdict |
| --- | --- | --- | --- | --- |
| q19 | `RecordBatchDecoder::next_buffer` under streamed spilled-inner materialization | no JSON row in sample; prior atlas showed stable RSS around 406 MiB | 4.095M physical reads / 3,908 unique pages, max 1,273 reads/page | repeated-read pathology |
| q03 | ADR 381/382: `RecordBatchDecoder::next_buffer` / spill decode floor | no JSON row in 120s counter window | 3.684M physical reads / 58,596 unique pages, max 99 reads/page | repeated reads exist, but broader page set; not q19's tiny hot-page loop |
| q07 | ADR 383: partition/take/copy, `take_bytes` / bucket / memmove | no JSON row in 120s counter window | 688.5K physical reads / 688.5K unique pages, max 1 read/page | not a repeated-read case |

The q19 vs q07 split is decisive. q19's read pattern is not the partition/take/copy floor. q19 and q03 both repeat
physical reads, but q19 is much more concentrated: a few thousand pages are reread hundreds to more than one thousand
times inside the sample window.

## Classification

Classification: **repeated-read pathology**, specifically in the streamed spilled-inner materialization path.

Rejected branches:

- **Same floor, larger volume:** rejected. q07 is unique-once, and q19's tiny unique-page set with 1,273 reads/page is
  not just more data.
- **Spill-read dominant, repetition unproven:** rejected. The per-page counter proves repetition.
- **Harness artifact:** rejected. The hot stack is inside spill decode/materialization and the read counter is keyed to
  executor spill pages.
- **Parallelism first:** rejected for this branch. Parallelism would distribute repeated reads; it would not remove the
  reread loop.

ADR 385 branch: **re-read elimination for streamed spilled-inner materialization**. The first cut should make the probe
materialization path read/decode each needed spill page once per relevant work unit, likely by grouping refs by page or
preloading unique pages before materializing matches, while preserving output order and budget semantics.

q03 should be kept as a control for ADR 385 because it also shows repeated physical reads, though with a broader page
distribution. q07 should remain the negative control for unique-once partitioned spill reads.

## Verification

No source code change is retained. The temporary counter was removed after collecting the read-count artifacts.

| command | result |
| --- | --- |
| `git diff -- crates/toron/src/exec/join/spill.rs` | empty after instrumentation removal |
| `perf report --stdio -g --no-children -i target/tpch/profiles/adr384_q19.perf.data` | exact q19 hot frames named |
| `perf script ... | inferno-collapse-perf | inferno-flamegraph` | `adr384_q19.svg` generated |
| q19/q03/q07 counter windows | read-count table above |
| `scripts/cargo-cgroup test -p toron --no-fail-fast` | pass |
| `git diff --check` | pass |

## Outcome

ADR 384 succeeded as a diagnostic:

- symbol-gated q19 profile collected;
- exact q19 hot frame and Toron path named;
- read repetition proven with a counted per-page artifact;
- q03/q07 controls compared using the same read counter;
- 385 branch named as streamed spilled-inner re-read elimination;
- no engine change retained.
