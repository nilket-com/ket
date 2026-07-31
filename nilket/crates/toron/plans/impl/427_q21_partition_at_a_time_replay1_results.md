# ADR 427 results - q21 partition-at-a-time replay 1

## Outcome

ADR 427 is implemented. The q21 chain owner no longer re-scans and re-indexes the orders build
for every replay-1 probe call. It writes the orders build and replay-0 probe side once, then
drains eight partitions one at a time: one compact-index build, one build-page decode pass, and
streamed probe pages per partition. The final instrumented release run completed under the
unchanged 4 GiB / 600-second engine envelope in **86.40 seconds wall / 82.00 seconds execute**.
ADR 426's replay-1 timeout is removed.

The canonical in-process q21 comparison still cannot complete under 4 GiB. The unchanged dense
q21 oracle retains the 15-million-order map and the complete 59,986,052-row
`lineitems_by_order` map after the engine result; the 4 GiB process is OOM-killed while building
that reference. The same final engine answer passes strict comparison at 6 GiB in 120,499.1 ms,
with 5,045,848 KiB process RSS. This is supplemental semantic evidence only.

The calibrated result is therefore **engine-green / reach-only** with the named
`q21_sf10_oracle_residency_gap`. The board remains **18/22 green**, not 19/22. A frugal q21
oracle is the bounded follow-on that can retire the instrument gap without further engine work.

## Phase 0 and before evidence

ADR 426 supplied the required before profile at the same persisted sf=10 input and board
envelope. Its periodically durable receipt showed replay 1 consuming 582,443,597 of
582,492,546 measured microseconds (99.991597%) before the 600-second kill. The fallback had
made 232 replay-1 calls in the partial run; each call re-streamed the partitioned orders scan
and rebuilt partition indexes. An 1,800-second diagnostic reproduced the same attribution.

Grounding the proposed reservoir exposed the second pathology: the existing reservoir prober
either builds `LoadedInnerJoinRows` from decoded pages or invokes `rebuild_index_from_pages` for
every probe call. Persisting only the files would remove repeated source scans but retain
O(probe calls x index build) work, the same scan-once/re-index-per-batch failure class ADR 413
had already exposed. This changed the cut from a global reservoir wire-in to an owner-scoped
partition-at-a-time grace-hash schedule.

The completed sf=10 measurements are:

| quantity | measured value |
| --- | ---: |
| replay-0 rows buffered | 37,929,348 |
| replay-1 partitions | 8 |
| completed recognized-Neq summary pair | 809,500,672 bytes |
| orders build reservoir spill | 93,680,256 bytes |
| probe reservoir spill | 2,192,875,264 bytes |
| aggregate reservoir spill | 2,286,555,520 bytes |
| largest compact partition index | 33,554,432 bytes |
| largest pinned decoded build partition | 8,719,952 bytes |
| total decoded build bytes | 69,723,904 bytes |

The largest active partition therefore fits under the live summary baseline with material
headroom. All eight individual writers remain under the unchanged 512 MiB writer quota. The
aggregate reservoir volume is intentionally larger: ADR 413's established contract is
per-writer, not a new 512 MiB cap summed across independent partition files. An initial
implementation accidentally imposed that aggregate cap and failed exactly at 536,874,784
bytes; the policy was removed and the promoted ADR/implementation wording was corrected before
seal.

## The cut

### Exact owner admission and commit

The deferred path is selected only for the chain-owned q21 shape:

- exactly two inner suffix runtimes, replay 1 and replay 2;
- recognized-Neq semi replay 3 and anti replay 4 summaries;
- an unattempted multi-partition Int64 `PartitionedScan` replay-1 build;
- no already-loaded replay-1 partition state.

Before I/O, a conservative active-partition estimate must fit under the completed runtime
baseline. A decline leaves the old path untouched. The first nonempty replay-0 output is the
commit point: after the orders scan or either reservoir starts, any failure is labeled and all
created files are cleaned; there is no late `Ok(None)` and no silent return to repeated scans.

### Both sides once, then partition at a time

At commit the owner scans the orders build once into eight unindexed build reservoirs and
creates eight probe reservoirs. Every replay-0 output is partitioned by the replay-1 probe key
and written once. After the base scan finishes, the owner:

1. takes one build partition and its corresponding probe partition;
2. rebuilds the compact Int64 index while decoding and pinning each build page in the same pass;
3. reads one probe page, probes pinned build rows, and immediately drives replay 2, semi,
   anti, and the grouped fold;
4. releases the probe-page cache, active index, decoded pages, and both files before advancing.

No drain call uses `read_all_batch_pages`, `concat_or_empty`, or a partition-sized probe vector.
Build probing reads the pinned batches, so the build pages are not decoded again after index
construction.

### Full owner accounting

The owner guard sums and names every simultaneously live component:

- completed summary and other-runtime baseline;
- remaining build-reservoir state;
- remaining probe-reservoir state;
- active build metadata, index, and pinned pages;
- active probe metadata and one decoded probe page;
- bounded current output batches;
- grouped-fold state.

Reservoir state is capacity-accounted, including writer/page metadata, handles, caches,
scratch, and path capacity. The active spill receives the owner's current baseline before each
combined rebuild-and-pin pass; it does not rely on the older baseline captured at file
creation. Guards run at reservoir growth, each pinned build page/index growth, probe-page and
output materialization, and after fold updates. Decoded pages move from cache to pinned
ownership before duplicate residency is credited away.

### Durable receipt

ADR 426's seven public stages remain unchanged. The receipt adds deferred replay metadata:
partition count and progress, buffered rows, index builds, build/probe page reads, decoded and
spill bytes, and maximum index/pinned-page residency. Buffering counts replay-1 logical input;
draining counts logical output, so rows are not doubled.

Atomic temp-then-rename snapshots are written at commit, each index/partition boundary, every
1,024 probe pages, and every 256 base batches once deferred replay has committed. The bounded
cadence produced 711 snapshots in the seal run. The explicit unattributed residual retains
snapshot and owner-bookkeeping cost rather than assigning it to a public stage.

The new recognizer future is heap-boxed at the grouped-aggregate dispatch boundary. The first
full library run exposed why this matters: without that isolation, the enlarged async state
made an unrelated nested reoriented-chain parity fixture overflow its execution stack. The
exact fixture and the complete library matrix pass after the boundary fix.

## Final mechanism receipt

The final release binary ran q21 strict sf=10 from persisted parquet under `MemoryMax=4G`,
`MemoryHigh=4G`, `TasksMax=512`, and `RuntimeMaxSec=600s`:

```text
status: pass
execute: 82,001.498 ms
wall: 1:26.40
max process RSS: 841,112 KiB
output rows: 100
spill bytes: 2,286,556,056
plan fingerprint: e2e25953a6a3fefd
schema fingerprint: fnv1a64:b30f81a65ea1fe43
digest: ArrowIpcDigest([252, 25, 104, 129, 111, 192, 78, 139,
                        130, 212, 110, 80, 159, 173, 241, 177,
                        218, 7, 220, 191, 130, 53, 16, 242,
                        116, 50, 185, 26, 212, 50, 65, 151])
```

The completed deferred receipt is discriminating:

| field | value |
| --- | ---: |
| partitions completed | 8 / 8 |
| compact indexes built | 8 |
| build page reads | 117,192 |
| probe page reads | 468,648 |
| build bytes decoded | 69,723,904 |
| replay-0 rows buffered | 37,929,348 |
| snapshots | 711 |
| total drive | 73,686,294 us |
| unattributed residual | 32,398,621 us |

Each build page is decoded exactly once during its partition's combined index-and-pin pass;
probe reads do not add build-page decodes. Replay 1 now completes rather than consuming the
entire gate:

| stage | exclusive us | input rows | output rows |
| --- | ---: | ---: | ---: |
| base scan | 942,423 | 59,986,052 | 37,929,348 |
| inner replay 0 | 3,379,693 | 37,929,348 | 37,929,348 |
| inner replay 1 | 34,765,851 | 37,929,348 | 18,321,400 |
| inner replay 2 | 1,137,678 | 18,321,400 | 734,523 |
| semi replay 3 | 644,021 | 734,523 | 707,593 |
| anti replay 4 | 407,921 | 707,593 | 39,448 |
| grouped fold | 10,086 | 39,448 | 39,448 |

Replay 1 is still the largest measured stage, as expected for 468,648 streamed probe pages,
but the all-or-nothing repeated-scan pathology is gone. No stage behind it approaches the
600-second gate.

## Correctness calibration

The final 4 GiB strict comparison was OOM-killed after the engine completed and before a
terminal correctness row. Source inspection and the 6 GiB measurement identify the producer:
`strict_q21_oracle` retains supplier and nation maps, the full 15-million-order map, and a
`HashMap<i64, Vec<...>>` containing all lineitems grouped by order. This is oracle instrument
state, not retained Toron engine state.

The unchanged comparison passed at 6 GiB:

```text
phase=compare status=pass elapsed_ms=120499.1
maximum resident set size=5,045,848 KiB
```

As with the former q18 residency gap, a comparison that needs a raised memory envelope does not
earn board credit. q21 is recorded `reach_only` with `q21_sf10_oracle_residency_gap`; the 6 GiB
row proves semantic parity but does not turn the cell green.

## Discriminating locks

The landed locks cover:

- combined compact-index rebuild and page pinning decodes every build page once, while pinned
  probes add zero decode reads;
- the existing reservoir path scans its source once and matches the repeated-scan answer;
- deferred receipt flow counts logical replay rows once and leaves parseable bounded page
  progress;
- the full owner guard names all eight live components and fires when their sum crosses the
  cap although the components fit individually;
- the drain is structurally page-streamed and contains none of the forbidden whole-partition
  helpers;
- a post-commit reservoir budget failure is loud, occurs after source I/O, and leaves unique
  scratch empty;
- the pre-existing reoriented-chain mixed-type parity fixture remains green after the async
  dispatch boxing.

The q21 live receipt supplies the large-shape one-scan/one-index-per-partition lock: eight
partitions, eight index builds, and the exact one-pass build-page census.

## Controls and verification

All thirteen independent sf=10 strict comparison controls passed under separate 4 GiB /
600-second units with the receipt environment unset:

| controls | result |
| --- | --- |
| q03, q07, q09, q10, q11, q12 | 6/6 `pass@compare` |
| q13, q14, q16, q19, q20, q22 | 6/6 `pass@compare` |
| q18 membership-first adjacent control | `pass@compare` (259,689.1 ms) |

All Cargo commands used `scripts/cargo-cgroup`:

- `fmt -- --check` and `check -p toron --all-targets` passed;
- streaming passed 66/66, join 227/227, and spill 52/52;
- the full Toron library passed 1,266/1,266;
- architecture/source guards passed 2,079/2,079;
- required `toron --no-fail-fast` passed across all targets;
- `toron-tpch --features polars-sql-reference --no-fail-fast` passed across all targets,
  including library 44/44 and q14 probes 2/2;
- the final release q21 engine run, 6 GiB supplemental strict comparison, and all thirteen
  canonical controls ran as described above.

`exec.rs` remains at its 7,500-line ratchet. Spill scratch is empty, `git diff --check` is
clean, and no `.perf.data` is tracked. No debug output, temporary helper, source branding, or
profiler artifact is shipped.

## Forward arc

ADR 427 closes q21's engine throughput thread. The cheapest next board move is a toron-tpch-only
frugal q21 oracle that preserves the current dense oracle as a test reference while removing
the co-resident 15-million-order maps, following ADR 425's instrument-honesty pattern. q17's
decimal-boundary policy proof, q02's join-output C-spec, and q05's diagnosis remain separate
mechanisms.
