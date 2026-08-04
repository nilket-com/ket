# ADR 433 results - q13 projected streamed-spilled LeftOuter throughput diagnosis

## Verdict

ADR 433 is complete as a diagnosis-only cut. The q13 slowdown and its 77-million-context-switch
parent signature are owned by the **decoded spill-page cache eviction/reread loop** on the projected
streamed-spilled LeftOuter path:

- q13's 5,860 projected spill pages decode to a 12,562,560-byte working set, **1.498x** the
  8,388,608-byte cache target;
- 13,627,398 materialization page requests produce 4,823,849 decoded-cache misses, 4,819,936
  decoded evictions, 4,823,849 page decodes, and 4,823,849 actual spill-file reads;
- 4,823,757 of those final-run reads return `Pending`, while the process records 9,660,675
  voluntary context switches - **2.003 switches per pending read**;
- the independent syscall census records 4,823,851 `pread64` calls, only two more than the
  ledger's spill reads, plus 21,402,675 `futex` calls;
- the zero-lost-sample flamegraph names the linear LRU bookkeeping in
  `SpilledInnerJoinBuild::touch_cache_entry` as the largest self frame at **30.82%**.

The selected ADR 434 candidate is a shape-fenced **decoded-page preload/pin** for this projected
streamed-spilled LeftOuter path, using the existing guarded `try_pin_decoded_pages` substrate. That
cut should eliminate the reread/decode/LRU churn before considering a separate O(1) LRU rewrite.
The grouped fold is the measured next CPU wall at 10.81%, but it is not the first mechanism.

No throughput mechanism lands in ADR 433. The board remains 22/22 and the ADR 356 throughput
baseline remains the comparison authority.

## Conditions and provenance

| item | value |
|---|---|
| engine seal | `7488ddff01b6dd9c04f2dd9eec0223b65f022529` |
| committed ADR 433 plan | `296ba440c954a939f2210958ba16b9691d7128ba` |
| measurement date | 2026-07-13 CDT |
| sealed stripped binary SHA-256 | `cca277b41ca6bf87dc401e0bb97cbdaff896d7cf751bd4859ac5539eda7a02ac` |
| symbolized flamegraph binary SHA-256 | `63538c73bcc8452b96e957772b1f3beaa184aad8884c222e36ed47c97ea17b7c` |
| source-matching final ledger binary SHA-256 | `4dd35a3cf74db4a298bd4d25b25556b6f792909e82e6798b799695ae1278cd38` |
| sf=10 manifest SHA-256 | `c04dd8dc81df705578abda1839f7e2f8ca39d2e6b4c43bcfef2aed5f77c9aa8f` |
| data | persisted strict sf=10 Parquet at `target/tpch/data` |
| scope | `MemoryHigh=infinity`, `MemoryMax=infinity`, `AllowedCPUs=0-1`, `CPUQuota=150%`, `TasksMax=512`, `RuntimeMaxSec=3600` |
| host | 12th Gen Intel Core i9-12900K; CPUs 0-1 are SMT siblings on P-core 0; 61 GiB RAM; no swap |
| kernel / perf | Linux `7.0.0-27-generic`; perf `7.0.6`; `perf_event_paranoid=1`; `kptr_restrict=1` |
| evidence root | `target/tpch/adr433/` |
| join-spill scratch | `target/tpch/join_spill`; empty before and after every accepted child |

The sealed stripped binary was copied to the evidence root before any diagnostic build. The
symbol-retaining build used `CARGO_PROFILE_RELEASE_DEBUG=2` and
`CARGO_PROFILE_RELEASE_STRIP=none`; `file`, `readelf`, and `nm -C` confirmed debug information,
`.symtab`, and resolved symbols for the projected probe, `read_batch_page`, `read_plain_at`, and the
ledger. Profiling and ledger timings are diagnostic and are not compared to or substituted for the
unprofiled ADR 356 baseline.

## Phase 0 - shape and path reconfirmation

The separate sealed-binary warmup passed:

| field | value |
|---|---:|
| plan fingerprint | `ac3784c060c3c226` |
| digest | `ArrowIpcDigest([251, 176, 197, 49, 42, 86, 184, 154, 199, 15, 104, 119, 68, 150, 145, 253, 171, 193, 51, 22, 250, 94, 192, 98, 213, 87, 64, 235, 161, 3, 128, 29])` |
| output rows | 46 |
| schema fingerprint | `fnv1a64:06933e705bc02cb3` |
| spill bytes | 9,152,992 |
| execute time | 128,445.192 ms, diagnostic warmup only |
| voluntary context switches | 9,659,251 |

The current recognizer admits q13 through the `match_filter.is_none()` branch at
`streaming.rs:7118`. The probe scan applies the pushed `Filter(EvaluateInToron)` residual at
`streaming.rs:7423-7431`, so the `NOT LIKE` is not a join `match_filter`. The accepted branch then
executes:

1. `visit_streaming_spilled_left_outer_scan_probe_chunks` (`streaming.rs:7385`);
2. `visit_streaming_spilled_left_outer_probe_batch` (`streaming.rs:7489`), with the 8,192-row
   selection chunk and per-row spill-index lookup;
3. `materialize_projected_streamed_spilled_left_outer_matches` (`streaming.rs:2879`) and
   `_unmatched` (`streaming.rs:2988`);
4. grouped folding and finalization.

The profile snapshot is published only from this admitted branch, so the live ledger is also a
positive path receipt. The branch still does not call `try_pin_decoded_pages`; its existing use at
`streaming.rs:6496` belongs to a different streaming shape.

The serialized spill exceeds the cache target by 764,384 bytes, but that did not establish decoded
geometry. The ledger measures the distinct decoded domain: 12,562,560 bytes, 4,173,952 bytes over
the target.

## Phase 1 - external single-child profile

The cache was warmed in a separate scoped child. One fresh sealed-binary child then ran under
`perf stat`; a second fresh child ran the bounded syscall census. Neither timing is a speed result.

### Hardware and software counters

| counter | single-child value |
|---|---:|
| task clock | 154,050.29 ms |
| context switches | 9,663,436 |
| CPU migrations | 28,824 |
| page faults | 262,690 |
| core cycles | 657,714,645,051 |
| instructions | 1,647,040,786,831 |
| IPC | 2.504 |
| cache references | 7,782,711,755 |
| cache misses | 3,556,398,275 (45.70%) |

`/usr/bin/time` independently records 9,659,319 voluntary and 4,130 involuntary switches for the
same counter child. The sealed warmup and counter child both record zero filesystem-input blocks;
the spill file was page-cache hot, but its async blocking-file operations still crossed the runtime
handoff millions of times.

### Syscall census

| syscall | calls | traced time |
|---|---:|---:|
| `futex` | 21,402,675 | 316.066 s |
| `pread64` | 4,823,851 | 7.312 s |
| `close` | 4,836,841 | 3.402 s |
| `fcntl` | 4,836,809 | 3.122 s |
| `read` | 83,091 | 0.126 s |

The syscall run took 402,975.650 ms execute time under interposition and is deliberately excluded
from performance comparison. The paired `fcntl`/`pread64`/`close` shape matches
`BlockingFileOps::read_plain_at`, which clones the file handle and sends each positional read
through `spawn_blocking_io` (`crates/wendelon/src/storage/file_ops.rs:363-376`). External evidence
therefore localized the context-switch family to spill reads, but it could not distinguish the two
cache layers or measure decoded residency. That triggered the ADR's required ledger.

## Phase 2 - parity-locked operation ledger

The query-neutral ledger is gated by
`TORON_STREAMED_SPILLED_LEFT_OUTER_PROFILE_PATH`. When unset it constructs no counter state or sink,
does not collect eviction statistics, and does not take diagnostic decode/fold clock readings. The
enabled path writes one atomic JSON snapshot; publication errors are returned as `ExecutionError`.

### Final structural snapshot

| component | count / bytes |
|---|---:|
| build input | 1,500,000 rows; 5,860 pages; 9,152,992 serialized spill bytes |
| decoded working set | 5,860 unique pages; 12,562,560 bytes; maximum page 2,584 bytes |
| probe | 14,837,583 rows / lookups / matched references; 58,596 probe chunks |
| probe unique-page requests | 13,621,480 total; maximum 289 in one chunk |
| decoded cache | 8,803,549 hits; 4,823,849 misses; 4,819,936 evictions |
| decoded cache residency | 8,388,008 final bytes; 8,389,456 transient pre-eviction peak bytes |
| plaintext cache | 0 hits; 4,823,849 misses; 4,823,849 promotions to decoded; 0 capacity evictions |
| actual spill reads | 4,823,849 calls / completions; 4,823,757 returned `Pending` |
| spill-read bytes | 7,532,826,200 requested and returned - **822.99x** serialized-spill amplification |
| page decode | 4,823,849 decodes |
| materialization | 58,658 batches; 15,337,604 rows; 13,627,398 unique-page requests |
| grouped fold | 58,658 batches; 15,337,604 rows |
| counter state | no overflow |

The independent receipts reconcile exactly where they should:

- ledger file reads = decoded misses = page decodes = plaintext misses = plaintext promotions =
  4,823,849;
- strace `pread64` minus ledger spill reads = 2 process-level reads;
- materialized batches/rows = grouped-fold batches/rows;
- requested read bytes = returned read bytes;
- lookup calls = matched references = probe rows for q13's unique customer build key.

The source-matching final ledger run recorded 275.843 ms in build and 125,348.867 ms in the
probe/fold phase. Within that diagnostic phase, actual spill reads accounted for 24,008.502 ms, IPC
decode for 11,657.180 ms, and grouped fold for 13,275.448 ms. These timers overlap broader work and
do not cross-sum to a
complete CPU decomposition.

Three separate enabled runs produced identical structural counts; only elapsed micros and the number
of file futures observed returning `Pending` varied slightly. After the profiler state was boxed to
keep the disabled join-build footprint pointer-sized, the exact source-matching final run retained
the expected fingerprint, digest, schema, 46 rows, and 9,152,992 spill bytes. It recorded
126,339.758 ms execute time, 9,660,675 voluntary context switches, and 957,468 KiB maximum RSS;
scratch was empty after exit.

### Disabled parity

The same source-matching binary was also run with the environment variable absent. It created no
sink and retained exactly the same fingerprint, digest, schema, row count, and spill telemetry as
the enabled diagnostic and sealed baseline. The disabled run recorded 123,753.810 ms execute time,
9,659,336 voluntary context switches, and 958,128 KiB maximum RSS; scratch was empty after exit.
Unit coverage locks both no-state/no-sink construction when disabled and exact enabled counter
serialization.

## Symbolized task-clock profile

The accepted sample used:

```text
perf record -e task-clock -F 99 --call-graph dwarf -m 1024
```

It captured 15,434 samples with **zero lost samples**. Restricted kernel symbols leave kernel frames
unresolved; conclusions about Toron/Rust frames use resolved userspace symbols only.

| userspace self frame | samples |
|---|---:|
| `SpilledInnerJoinBuild::touch_cache_entry` | 30.82% |
| `fold_bound_grouped_aggregate_batch` | 10.81% |
| `RecordBatchDecoder::next_buffer` | 4.30% |
| `SpilledInnerJoinBuild::read_batch_page` closure | 3.82% |
| `hashbrown::HashMap::get` | 2.91% |
| `visit_streaming_spilled_left_outer_probe_batch` closure | 2.42% |
| `stable_file_hash` harness work | 2.26% |

`touch_cache_entry` is a `VecDeque::retain` followed by `push_back`
(`spill.rs:2193-2195`), so each of the millions of cache touches linearly scans the retained LRU
queue. The flamegraph therefore names the largest CPU frame, while the ledger and syscall census
show why it executes millions of times.

The retained local artifacts are:

- `target/tpch/adr433/flamegraph/q13.perf.data`;
- `target/tpch/adr433/flamegraph/q13.svg`;
- `target/tpch/adr433/flamegraph/perf-report.txt` and `perf-report-flat.txt`;
- `target/tpch/adr433/ledger/q13_source_final.json`;
- warmup, perf-stat, strace, parity, and scope stdout/stderr receipts under the same evidence root.

No `.perf.data` file is tracked.

## Attribution and ADR 434 handoff

The evidence closes the causal chain:

1. The projected build's decoded working set is 1.498x cache capacity.
2. Probe/materialization revisits pages across 58,596 chunks; the LRU evicts nearly every decoded
   miss under that access geometry.
3. A decoded miss cannot reuse plaintext here: promotion clears plaintext, so a later miss performs
   another file read and another IPC decode.
4. `BlockingFileOps` sends the read through a blocking worker. The ledger's pending-read count and
   approximately two voluntary switches per pending read account for the prior parent signature.
5. Each cache hit, promotion, and eviction also exercises a linear `VecDeque::retain`, producing the
   30.82% dominant userspace frame.

ADR 434 should first extend the existing guarded `try_pin_decoded_pages` mechanism to the exact
projected streamed-spilled LeftOuter branch after `finish` and before probe. Acceptance should
require:

- a durable receipt that pinning was admitted and records decoded bytes/index bytes/page reads;
- no fallback regression when the retained-footprint guard rejects pinning;
- q13 output/digest/fingerprint/spill parity;
- an unprofiled ADR 356-shaped repeated-run comparison before any speed claim;
- a counter receipt showing the 4.82M rereads/decodes and 9.66M context switches collapse.

If pinning does not eliminate the expected time, the next named wall is the grouped fold. An O(1)
LRU structure is a separate general follow-up, not the first q13 cut: pinning removes both the linear
touch cost and the much larger read/decode amplification for this measured shape.

## Verification

All source and live gates are green:

- `scripts/cargo-cgroup fmt -- --check`;
- `scripts/cargo-cgroup check -p toron --all-targets` and
  `scripts/cargo-cgroup check -p toron-tpch --all-targets`;
- the two new profile unit tests: 2 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --lib`: 1,287 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --test architecture_test`: 2,079 passed, 0 failed;
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: every unit, integration, and doc-test binary
  green;
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`: every
  unit, integration, and doc-test binary green (the two pre-existing ADR fixture ignores remain
  labeled);
- source-matching enabled and disabled live q13 runs: fingerprint, digest, schema, row count, and
  spill parity; scratch empty after each;
- `git diff --check`: clean;
- `git ls-files '*.perf.data'`: empty.

During verification, the first full-lib run exposed a real disabled-path regression: storing the
ledger inline enlarged the join-build async state enough to overflow the stack in
`reoriented_inner_chain_grouped_aggregate_matches_materialized_float64_and_decimal_order`. The
ledger was changed to `Option<Box<_>>`; the isolated reproducer then passed, followed by the clean
1,287-test full-lib and whole-package gates above. The final live receipts and binary hash are from
the boxed source.

The implementation and this results document remain uncommitted for independent review.
