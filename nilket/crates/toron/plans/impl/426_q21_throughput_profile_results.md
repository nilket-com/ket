# ADR 426 results - q21 throughput profile

## Outcome

ADR 426 is implemented as a diagnosis-only seal. The reoriented semi/anti chain driver now has
an off-by-default, periodically durable seven-stage wall-time receipt. The canonical q21 sf=10
run timed out at the unchanged 600-second board gate, but left a complete atomic snapshot:
**replay 1 consumed 582,443,597 of 582,492,546 measured microseconds (99.991597%)**. The grouped
fold consumed 197 microseconds. A required 1,800-second diagnostic attempt reproduced the split:
replay 1 consumed 1,779,232,745 of 1,779,543,631 measured microseconds (99.982530%); the fold
consumed 709 microseconds.

The dominant work is therefore neither the Utf8-key `COUNT(*)` fold nor the base pull
interface. The specialized scan-base chain feeds small replay-0 outputs into replay 1, whose
sf=10 runtime cannot retain its partitioned scan build and consequently takes the existing
scan-build fallback on every runtime probe. The next mechanism is a **scan-once, persistent
partition reservoir for replay 1 under the live semi/anti-summary baseline**. Although the
reservoir writer/prober already exists, composing its lifetime, combined accounting, cleanup,
and committed fallback into this owner is not a profiling-only change and was not folded into
ADR 426.

q21 remains red and the board remains **18/22 green**. The raised-budget run is diagnostic only;
it earns no board credit. q17, q02, and q05 remain separate named threads.

## Phase 0 evidence

The promoted-plan HEAD (`47871e9`) was built before production code changed. The exact persisted
q21 sf=10 performance rail was run under `MemoryMax=4G`, `MemoryHigh=4G`, `TasksMax=512`, and
`RuntimeMaxSec=600s`:

| result | exit | CPU usage | memory peak | terminal row |
| --- | ---: | ---: | ---: | --- |
| `timeout` | 15 (`systemd-run` rc 1) | 601,445,469,000 ns | 4,048,637,952 bytes | none |

The current memory peak differs from ADR 421's 2,242,555,904-byte observation but remains below
the unchanged 4 GiB cgroup cap. No terminal task telemetry survived the external kill. Source
inspection confirmed why: `try_streaming_inner_prefix_semi_anti_scan_probe_base` started one
timer around the entire scan/probe/fold loop and added only one aggregate `probe_micros` value
after normal loop completion. It neither separated replay stages nor serialized partial state.

The bound plan still has the expected replay 0-4 topology: three inner joins, then the replay-3
semi and replay-4 anti stages, followed by the grouped `s_name` / `COUNT(*)` fold. Both
existence stages retain ADR 421's page-free recognized-Neq summaries.

## The instrument

`TORON_CHAIN_STAGE_PROFILE_PATH` enables the profiler for this specialized chain drive. The path
is resolved once. With the variable absent, the owner constructs no profiler, starts no stage
timer, allocates no receipt state, and performs no file I/O. Tests use the injected private path
constructor and never mutate process-global environment state.

The receipt owns seven exclusive accumulators:

1. base scan, row limit, and filter;
2. inner replay 0 key/probe, selection materialization, post-filter, and repartition;
3. complete inner replay 1 runtime probe and post-filter;
4. complete inner replay 2 runtime probe and post-filter;
5. complete replay 3 semi probe;
6. complete replay 4 anti probe;
7. grouped fold.

Each stage records exclusive microseconds, calls, batches, input rows, and output rows. An
independent total-drive timer includes loop/vector bookkeeping and snapshot work. Serialization
checked-adds all stage durations and checked-subtracts them from the total; a stage sum above
the total is a loud labeled error rather than saturation. The exact difference is emitted as
`unattributed_residual_micros`.

The profiler writes one complete JSON value to a sibling temporary path and atomically renames
it over the configured path at drive start, after every completed base batch, and on normal or
typed-error exit. `sequence`, completed batches, raw/post-filter base rows, `drive_completed`,
and the optional terminal error make partial progress explicit. An external kill can lose only
the currently executing base batch; it cannot expose a partial JSON record. Enabled write or
rename failures are labeled executor errors. The existing chain owner still performs runtime
cleanup on returned errors.

The pre-existing aggregate task-phase timer remains unchanged and still records only on normal
completion. The new receipt is a diagnostic wall-time split, not a CPU-time claim.

## Discriminating locks

Three direct profiler locks cover:

- all seven stage rows, exact stage-sum plus residual equality, flow/progress fields, and loud
  rejection when the supplied total is smaller than the stage sum;
- start and completed-batch snapshots plus preservation of the last renamed JSON value when a
  temporary write is interrupted;
- a missing-parent write failure that is both labeled and returned.

The locks use unique injected paths. No test toggles the production environment variable.

## sf=1 parity and overhead calibration

Separate release processes used the same persisted sf=1 input. Profiled and unprofiled runs
were repeated in reverse order to separate instrumentation cost from warm-cache/order effects.
All four executions produced:

```text
digest: ArrowIpcDigest([94, 189, 64, 185, 77, 200, 230, 42,
                        50, 106, 192, 70, 174, 205, 32, 191,
                        199, 227, 224, 135, 158, 223, 162, 207,
                        160, 204, 93, 64, 162, 97, 97, 11])
output rows: 100
schema fingerprint: fnv1a64:b30f81a65ea1fe43
plan fingerprint: e2e25953a6a3fefd
spill bytes: 6,139,576
```

| order | unprofiled execute | profiled execute | observed delta |
| --- | ---: | ---: | ---: |
| first pass | 1,973.648 ms | 2,201.289 ms | 227.641 ms |
| reverse pass | 1,912.580 ms | 2,344.162 ms | 431.582 ms |

The completed sf=1 receipt made 5,862 atomic snapshots. The observed cost is approximately
39-74 microseconds per snapshot. Projecting that measured cadence to roughly 58,000 sf=10 base
batches gives about 2.3-4.3 seconds, below 0.75% of the 600-second gate, and the actual snapshot
cost remains visible in the residual. The instrumented strict sf=1 correctness rail also passed
compare.

The completed sf=1 drive provides a useful scale-direction control:

| stage | exclusive us | share | input rows | output rows |
| --- | ---: | ---: | ---: | ---: |
| base scan | 84,511 | 6.821% | 6,001,215 | 3,793,296 |
| inner replay 0 | 267,715 | 21.607% | 3,793,296 | 3,793,296 |
| inner replay 1 | 434,921 | 35.102% | 3,793,296 | 1,828,911 |
| inner replay 2 | 73,120 | 5.901% | 1,828,911 | 75,871 |
| semi replay 3 | 42,893 | 3.462% | 75,871 | 73,089 |
| anti replay 4 | 10,864 | 0.877% | 73,089 | 4,141 |
| grouped fold | 1,369 | 0.110% | 4,141 | 4,141 |
| unattributed residual | 323,627 | 26.120% | - | - |

The drive completed in 1,239,020 microseconds. Its non-dominant, distributed sf=1 shape rules
out a profiler that mechanically assigns all work to replay 1.

## Canonical 600-second profile

The final release binary ran q21 strict sf=10 from persisted parquet under the unchanged board
envelope. The process timed out with `CPUUsageNSec=601,946,852,000` and a 4,229,275,648-byte
cgroup memory peak. The killed process emitted no terminal output, but the last atomic receipt
was parseable:

| stage | exclusive us | share | calls / batches | input rows | output rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| base scan | 5,281 | 0.000907% | 58 / 58 | 59,392 | 37,407 |
| inner replay 0 | 9,158 | 0.001572% | 58 / 232 | 37,407 | 37,407 |
| **inner replay 1** | **582,443,597** | **99.991597%** | **232 / 464** | **37,407** | **18,075** |
| inner replay 2 | 7,085 | 0.001216% | 464 / 355 | 18,075 | 756 |
| semi replay 3 | 1,599 | 0.000275% | 355 / 345 | 756 | 725 |
| anti replay 4 | 296 | 0.000051% | 345 / 36 | 725 | 37 |
| grouped fold | 197 | 0.000034% | 36 / 36 | 37 | 37 |
| unattributed residual | 25,333 | 0.004349% | - | - | - |

The exact total is 582,492,546 microseconds. Snapshot sequence and completed-base-batch count
are both 58; `drive_completed=false` and `terminal_error=null` correctly distinguish an
external kill from a returned executor error. The receipt covered 59,392 raw rows and 37,407
post-filter rows before the last rename.

## Raised-budget diagnostic attempt

The required second attempt kept the 4 GiB memory envelope and raised only
`RuntimeMaxSec=1800s`. It also timed out: `CPUUsageNSec=1,805,792,099,000`, memory peak
981,856,256 bytes, and no terminal row. Its final sequence-193 receipt covered 197,632 raw
rows / 125,140 post-filter rows:

| stage | exclusive us | share | calls / batches | input rows | output rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| base scan | 11,530 | 0.000648% | 193 / 193 | 197,632 | 125,140 |
| inner replay 0 | 29,981 | 0.001685% | 193 / 772 | 125,140 | 125,140 |
| **inner replay 1** | **1,779,232,745** | **99.982530%** | **772 / 1,544** | **125,140** | **60,448** |
| inner replay 2 | 20,817 | 0.001170% | 1,544 / 1,182 | 60,448 | 2,465 |
| semi replay 3 | 5,395 | 0.000303% | 1,182 / 1,161 | 2,465 | 2,372 |
| anti replay 4 | 1,005 | 0.000056% | 1,161 / 132 | 2,372 | 142 |
| grouped fold | 709 | 0.000040% | 132 / 132 | 142 | 142 |
| unattributed residual | 241,449 | 0.013568% | - | - | - |

The exact total is 1,779,543,631 microseconds. The stage share and row flow remain stable across
more than three times the canonical observation window. The attempt did not complete and is not
reported as completed-drive evidence.

## Diagnosis and forward mechanism

The base scan, pull bookkeeping, semi/anti summaries, and fold together are immaterial at both
sf=10 windows. Replay 1 is the orders join in the persisted replay-0-through-4 plan. At this
scale its `ReorientedInnerJoinBuild::PartitionedScan` cannot take the resident-load shortcut.
`probe_reoriented_inner_join_streaming` therefore invokes
`probe_partitioned_streamed_inner_scan_build`; that fallback constructs active partition
writers and streams the scan build for each probe call. The canonical receipt records 232 such
replay-1 runtime calls; the raised receipt records 772. This is the repeated work the durable
split exposes.

The repository already has the scan-once building block:
`build_partitioned_streaming_scan_inner_build_reservoir` writes the source once, and
`probe_partitioned_streamed_inner_scan_build_reservoir` probes retained partition files. Its
existing lock proves one source scan and row-for-row parity against the repeated-scan path.
However, q21 prepares two 404,750,336-byte recognized-Neq summaries before driving the base.
Keeping a partition reservoir across the drive therefore requires explicit combined owner
accounting, a pre-commit eligibility/failure contract, and cleanup across every post-preparation
exit. Those are production execution semantics, not a bounded timing-hook extension.

The named follow-on is therefore the q21 replay-1 persistent partition reservoir under the live
summary baseline. A Utf8-key `COUNT` primitive is not supported by the profile and is removed
from the immediate queue. The negligible residual also provides no evidence for the q02 pull
interface. This diagnosis discharges the standing “profile before speedup” requirement without
pretending that the remedy landed in the same seal.

## Scope and verification

ADR 426 changes only `exec/join/streaming.rs` plus this results document. `exec.rs` remains at
its 7,500-line ratchet. The profiling environment variable is unset for all ordinary tests and
execution, so no query decision, output, comparator, schema, budget, or guard changes.

All Cargo commands used `scripts/cargo-cgroup`:

- `fmt -- --check` and `check -p toron --all-targets` passed;
- direct receipt locks passed 3/3;
- Toron library passed 1,261/1,261, the streaming filter passed 62/62, and the join filter
  passed 222/222;
- Toron architecture/source guards passed 2,079/2,079;
- required `toron --no-fail-fast` passed across all targets;
- base `toron-tpch` passed: library 34 active / 1 intentional heavy ignore, CLI 14/14,
  architecture 12/12, correctness 176 active / 1 intentional heavy ignore, Arrow probes 5/5,
  decimal probes 12/12, harness 44/44, q14 probes 2/2, and doc tests;
- the `polars-sql-reference --no-fail-fast` matrix passed the same set plus its feature-specific
  CLI test (15/15);
- release sf=1 profiled/unprofiled parity, instrumented sf=1 strict compare, the canonical
  600-second profile, and the raised 1,800-second diagnostic profile all ran as described above;
- `git diff --check` passed, spill scratch was removed, and no `.perf.data` is tracked.

Because the seal changes no execution behavior with profiling disabled, the twelve sf=10 engine
controls were not rerun. The full engine and toron-tpch matrices instead exercise the unchanged
paths with the variable absent. No debug output, temporary source, qNN/ADR branding, or profiler
artifact is tracked.
