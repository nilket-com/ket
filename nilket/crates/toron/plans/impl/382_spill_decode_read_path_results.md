# ADR 382 results - q03 spill-decode read path

## Summary

ADR 382 implemented a narrow read-path cut for the q03-style inner spill pages: `SpilledInnerJoinBuild::read_batch_page`
now caches decoded `RecordBatch` pages under the existing join-spill cache budget. A decoded cache hit returns the
`RecordBatch` directly instead of cloning cached plaintext bytes and paying Arrow IPC decode again.

The spill page format remains Arrow IPC + LZ4 + encryption. No spill cap, memory cap, producer name, or page format
changed.

The cut is intentionally modest. The post-cut profile moves `RecordBatchDecoder::next_buffer` from 10.54% to 8.89% in
the delayed q03 engine-window profile, but the frame remains visible. The remaining q03 cost is mostly unique-page
IPC/LZ4 decode/decompression/allocation, not fully eliminated cache-hit redecode.

## Phase 0 findings

### Once-vs-repeated decode

The structural premise in the ADR was too optimistic. The partitioned path does preload partition work in
`LoadedPartitionWork.build_pages`, but the measured q03 run also exercises repeated `SpilledInnerJoinBuild::read_batch_page`
calls through other inner-spill access paths. Before the cut, the cache stored decrypted plaintext bytes only, so a cache
hit still decoded Arrow IPC again.

Temporary page-level diagnostics on q03 sf=10 showed the repeated-decode shape:

| variant | writes | reads | avg rows/page | avg decode us/page | spill bytes for one finished inner build | writer peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| IPC+LZ4, 8192-row ceiling | 29,298 | 1,300,197 | 64.0 | 13.01 | 89,529,944 | 7,944 |

So the first safe lever was not a raw spill format rewrite. It was to cache the decoded page representation already
produced by the current reader.

### Candidate table

The page-size and codec candidates were measured with temporary, uncommitted instrumentation. The runs used persisted
q03 sf=10 under the fixed 4 GiB scope and were interrupted after enough write/read diagnostics were captured.

| variant | page count | rows/page | compressed bytes/page | decode ms/page | `next_buffer` % | decompress % | alloc/copy % | spill bytes | writer peak | guard outcome |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| IPC+LZ4, 8192-row ceiling | 14,649 | 64.0 | 6,122 | 0.0130 | 10.54 | ~5.45 | malloc 9.98 / memmove 3.72 | 89,529,944 | 7,944 | pass |
| IPC+LZ4, 32768-row ceiling | 14,649 | 64.0 | 6,122 | 0.0130 | n/a | n/a | n/a | 89,529,944 | 7,944 | pass |
| IPC uncompressed, 8192-row ceiling | 14,649 | 64.0 | 9,055 | 0.0044 | n/a | n/a | n/a | 132,738,200 | 13,128 | q03 pass |

Interpretation:

- Page-size is not the lever. The nominal chunk ceiling changed from 8192 to 32768 rows, but actual partition-routed
  pages stayed around 64 rows. The page unit is constrained by partition routing, not the chunk ceiling.
- Uncompressed IPC is a real q03-local decode win, but it is not shipped in ADR 382. The shared encoder also serves
  budget-sensitive spill shapes where prior ADRs relied on compression to stay under the 512 MiB spill budget. A scoped
  codec policy would need its own budget-aware design.
- The chosen ADR 382 lever is decoded-page caching, which changes only the read cache and leaves spill bytes and
  writer peak unchanged.

## Implementation

`CachedJoinSpillPage` now can hold an optional decoded `RecordBatch` and its accounted memory bytes.

`SpilledInnerJoinBuild::read_batch_page` now:

1. returns the cached decoded `RecordBatch` if present;
2. otherwise reads/decrypts the page as before;
3. decodes the Arrow IPC page as before;
4. stores the decoded batch in the existing cache;
5. drops the plaintext bytes for that cache entry so the cache budget is spent on the decoded representation.

The shared cache eviction now accounts `bytes + decoded_bytes`, so decoded batches are not hidden resident state. The
cache still uses the existing `JOIN_SPILL_CACHE_TARGET_BYTES` target and the task memory cap; no cap was raised.

## Correctness and profile evidence

Focused tests:

```text
inner_spill_read_batch_page_reuses_decoded_cache ... ok
inner_spill_page_write_reuses_scratch_and_round_trips ... ok
```

The new cache test proves the second `read_batch_page` uses the decoded cache by clearing the cached plaintext bytes
after the first read and verifying the second read still returns the correct batch.

q03 oracle:

```text
tpch_correctness query=q03 sf=0.01 mode=strict phase=compare status=pass reason=none
```

Post-cut q03 profile, using the ADR 381 symbol-retaining method:

| profile | samples | lost samples | `next_buffer` | `_int_malloc` | `__memmove` | note |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| ADR 381 baseline | 5,760 | 0 | 10.54% | 9.98% | 3.72% | byte cache only |
| ADR 382 decoded cache | 5,703 | 0 | 8.89% | 8.54% | 4.26% | decoded cache under existing cap |

The profile movement is real but not a full q03 time fix. ADR 382 removes repeated cache-hit decode where the small cache
has locality; the remaining `next_buffer` stack is the cost of decoding unique projected spill pages.

## Verification

Run so far:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib inner_spill_read_batch_page_reuses_decoded_cache -- --nocapture
scripts/cargo-cgroup test -p toron --lib inner_spill_page_write_reuses_scratch_and_round_trips -- --nocapture
CARGO_CGROUP_BACKEND=affinity scripts/cargo-cgroup run -p toron-tpch -- correctness --query q03 --sf 0.01
scripts/cargo-cgroup test -p toron --no-fail-fast
git diff --check
executor no-qNN source scan
```

`scripts/cargo-cgroup test -p toron --no-fail-fast` passed with 1186 lib tests, 2079 architecture tests, and the
integration suites green.

Large profiling artifacts remain under `target/tpch/profiles` and are not committed.

## Verdict

ADR 382 is a behavior-preserving read-cache improvement, not a spill-format rewrite. It corrects the false
once-decode premise, removes one repeated-decode source under the existing cache budget, and records the measured codec
and page-size candidates without broadening the spill format.

The next q03 read-path lever is a separate design: a budget-aware internal spill codec/page policy, or a larger locality
strategy for partition-routed tiny pages. q07 remains outside this ADR and stays in the partition/take/copy family.
