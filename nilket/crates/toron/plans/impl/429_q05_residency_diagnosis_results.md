# ADR 429 - q05 residency diagnosis: results

Pairs with `plans/adr/429_q05_residency_diagnosis.md` and its implementation plan. This is a
diagnosis-only cut. The board stays 19/22; no execution fix or budget change is included.

## Outcome

The current q05 kill is a **distributed cgroup peak with one named growth producer**:

- process RSS and cgroup anonymous memory are already about 2.96 GB / 2.95 GB when the generic
  grouped path admits the reservoir, and remain flat through the last durable sample;
- the reservoir writes to the default `/tmp/toron_join_spill`, and `/tmp` is `tmpfs` on this
  host;
- 1.329 GB of reservoir spill therefore becomes cgroup `shmem`, not reclaimable disk-backed
  cache. It replaces the input Parquet file cache under direct reclaim until no useful file
  cache remains, then the next small spill growth reaches the exact 4 GiB cgroup cap;
- the selected engine-owned base plus all 64 writers account for only 124.2 MB at the last
  sample. Writer index and cache payload are both zero. This is not a base-concat, resident-index,
  or writer-cache peak.

ADR 430 is consequently a **spill-placement/cache-control cut**: keep large join spill off
memory-backed `tmpfs` (or otherwise make spill placement cgroup-aware) so that spill pages are
backed by a reclaimable filesystem. It is not a cap bump and ADR 429 does not implement it.

## WIP disposition

Claude's initial implementation was tossed rather than incrementally retained. It treated RSS
as the primary truth, used 32-batch cadence, saturated accounting, omitted cgroup categories and
all-writer ownership, used a different env surface, and had no pre-growth intent. The final code
was rebuilt around the reviewed ADR. Only the source-level call-site census informed the rewrite.

## Phase 0 - pinned before-state and history

The uninstrumented promoted-parent source was built in a detached worktree at
`5a2f7be6da3cde66dda60c6d6ade11e0720ff92a` (ADR 428 implementation). Its retained binary hash
is in `target/tpch/adr429/baseline_binary.sha256`.

Three fresh-scratch sf=10 q05 strict runs under `MemoryMax=4G`, `MemoryHigh=4G`,
`TasksMax=512`, and the 600-second envelope reproduced the current state:

| run | wall to death | systemd result | cgroup peak | surviving scratch |
| --- | ---: | --- | ---: | ---: |
| 1 | 50.036 s | `oom-kill` / signal 15 | 4,294,967,296 | 1,339,527,872 |
| 2 | 49.834 s | `oom-kill` / signal 15 | 4,294,967,296 | 1,339,472,280 |
| 3 | 52.163 s | `oom-kill` / signal 15 | 4,294,967,296 | 1,339,353,864 |

The completed units had already lost their cgroup directories, so `memory.events` is explicitly
unavailable; each journal records the kernel OOM kill. No `/usr/bin/time` RSS is inferred from a
killed unit. Partial scratch is disk progress consistent with an interrupted reservoir, not by
itself proof of the phase.

The distinct historical evidence remains distinct:

- ADR 413 perf completed in 190,034 ms with 5,353,985,328 spill bytes, 3,051,064 KiB
  **process RSS**, and fingerprint `757d7c625669b21a`.
- ADR 413 correctness passed comparison, while its cgroup peak was **exactly 4 GiB**. It proved
  correctness but no residency headroom.
- ADR 414 could not reproduce the green, including from a clean detached source.
- ADR 415 reproduced three exact-cap OOM kills at about 42 seconds and about 1.3 GiB scratch.

ADR 413 did not retain cgroup categories, spill mount identity, or swap/reclaim state. Its process
RSS cannot be substituted for those facts. The exact-cap correctness result proves the old green
was knife-edge; the current receipt explains the repeatable present failure without inventing an
unmeasured historical category split or claiming an engine-semantics regression.

## Owning path and instrumented boundaries

The source path is:

`base accumulation -> writer-set creation -> reservoir write -> reservoir finish -> base concat
-> reservoir probe -> suffix -> grouped fold`.

The generic grouped path retains all base batches while the first partitioned scan runtime builds
the one-pass reservoir. `concat_or_empty` is after reservoir finish. Instrumentable high-water
sites are writer-vector creation, each scan/exchange batch before writer append, all phase
transitions, and base concat. Writer-vector bytes are exact; batch growth is a conservative
batch-buffer-plus-index-row estimate; concat intent uses accounted base buffers and labels
allocator overhead unavailable.

## Kill-surviving receipt

The off-by-default, query-neutral surface is
`TORON_GROUPED_RESERVOIR_RESIDENCY_PROFILE_PATH`. With it absent, admission constructs no receipt
state, samples nothing, and performs no receipt I/O. The implementation is the semantic join
submodule `exec/join/residency_profile.rs`; `exec.rs` is untouched.

Schema version 1 retains:

- sequence, event, phase, status, completed build batches/rows, partition/group counts, the last
  completed durable boundary, 64-batch cadence, and the 16 MiB intent threshold;
- an engine ownership ledger with base buffers/vector capacity, every writer and its component
  capacities, aggregate/max writer values, receipt bytes, checked current total, guard-only
  historical fields, and spill disk bytes kept outside memory totals;
- current and admission OS ledgers with process RSS, cgroup `memory.current`, anon, file,
  dirty/writeback, shmem, kernel/slab, unknown keys, and explicit unavailable/error markers;
- a checked `cgroup anon - engine current ownership` diagnostic residual. It is subtraction for
  attribution only; the engine and cgroup ledgers remain explicitly non-additive.

All receipt accounting uses checked arithmetic. Shared-Arrow and guard-accounting overlap is
labeled rather than silently deducted or saturated. Allocation intent is flushed before writer
creation, before each instrumentable projected high-water advance of at least 16 MiB, and before
base concat. Progress is flushed every 64 completed build batches and all phase transitions are
flushed.

Snapshots use an atomic sibling temporary write and rename, matching the ADR 426 process-kill
receipt pattern. An initial version also `fsync`ed the file and parent directory and took 48.43 s
against a 37.58 s path-bearing disabled sample (+28.9%), correctly tripping the stop line. Host
crash durability is not needed for an OOM process-kill receipt. Removing those two `fsync`s while
preserving atomic rename and the fixed attribution cadence brought the profiler inside bounds.
Serialization/write/rename failures are labeled and loud; an interrupted temp file preserves the
last parseable rename.

Eleven dedicated unit locks cover disabled construction, checked overflow, all-writer aggregate
and maximum values, intent-before-growth, 64-batch cadence, every named phase, dual-ledger
separation and residual, proc/cgroup parsers, loud enabled failure, and atomicity. The existing
reservoir parity test additionally compares enabled/disabled output, row counts, digest, scan
telemetry, and writer census.

## Calibration

The implementation plan assumed sf=1 would enter the subject path. Six fresh sf=1 samples proved
that assumption false: all six passed with the same five-row digest and fingerprint
`757d7c625669b21a`, but both the disabled and env-enabled modes wrote zero receipts because sf=1
never enters even the generic grouped-base helper. This is retained as scale-routing and disabled
parity evidence, not mislabeled as receipt calibration.

The lowest completing scale observed to enter the subject reservoir path was sf=4 (32 partitions
and 8 groups there, versus 64 and 22 in the canonical sf=10 run), so the quantitative receipt
stop line was rerun there under the same 4 GiB cgroup. Final-binary results:

| mode | run walls | median | maximum RSS | receipt |
| --- | --- | ---: | ---: | --- |
| disabled | 35.65, 38.37, 39.04 s | 38.37 s | 1,265,344 KiB | none |
| enabled | 39.93, 36.87, 36.65 s | 36.87 s | 1,265,184 KiB | one atomic file |

Median overhead is -3.91% (well below +10%); max-RSS delta is -160 KiB (below +32 MiB). Every
sample has the same status, digest, five output rows, 1,289,941,048 spill bytes, and fingerprint.
All three enabled receipts are schema 1 terminal snapshots at sequence 1,473 with the unchanged
64-batch and 16 MiB bounds. One attempt during unrelated extreme host I/O pressure hit the
180-second runtime envelope; it was discarded and rerun fresh rather than folded into profiler
cost.

## Canonical diagnosis

The refined sf=10 run used the unchanged strict cgroup envelope and fresh scratch. Systemd reports
`Result=oom-kill`, `ExecMainCode=2`, `ExecMainStatus=15`, exact
`MemoryPeak=4,294,967,296`, and 49.66 seconds outer wall. The live cgroup directory was gone after
unit teardown, so `memory.events` is marked unavailable and the OOM journal is retained.

The last atomic receipt is a `progress` event in `reservoir_write`:

- sequence 925; 59,072 completed writer batches and 15,122,432 build rows;
- previous completed boundary at 59,008 batches;
- 64 writers, 1,328,795,264 spill bytes at the receipt, and 1,329,899,280 scratch bytes at death;
- base ownership 94,495,004 bytes; writer ownership 29,733,563 bytes; total selected engine
  ownership 124,229,531 bytes;
- writer ownership is almost entirely 29,360,128 bytes of page metadata. Resident index, decoded
  cache payload, and pinned decoded bytes are all zero; aggregate write-scratch capacity is
  337,344 bytes.

Admission versus last cgroup state (file includes shmem and is not added to it):

| field | admission | last receipt | delta |
| --- | ---: | ---: | ---: |
| `memory.current` | 4,253,016,064 | 4,294,848,512 | +41,832,448 |
| process RSS | 2,961,793,024 | 2,961,784,832 | -8,192 |
| anon | 2,945,077,248 | 2,945,069,056 | -8,192 |
| file (includes shmem) | 1,289,441,280 | 1,329,565,696 | +40,124,416 |
| shmem | 0 | 1,328,922,624 | +1,328,922,624 |
| ordinary file (`file - shmem`) | 1,289,441,280 | 643,072 | -1,288,798,208 |
| kernel | 16,654,336 | 19,841,024 | +3,186,688 |
| slab | 10,046,512 | 13,219,592 | +3,173,080 |

Anon and RSS are flat. The last `cgroup anon - selected engine ownership` residual is
2,820,839,525 bytes, but it is also flat across the write interval rather than the peak-growing
producer. Direct reclaim scanned about 1.75 million pages, `pswpout=0`, and the host has no active
swap. The last receipt is only 118,784 bytes below the cgroup cap.

The filesystem and byte identity discriminate the owner:

- `/tmp` is a `tmpfs` mount;
- `shmem=1,328,922,624` at the receipt;
- engine spill progress is 1,328,795,264 bytes at the same receipt;
- surviving scratch is 1,329,899,280 bytes at death.

Thus the reservoir's tmpfs spill replaces almost all reclaimable input file cache with charged,
non-swapped shmem while static anon occupies about 2.95 GB. The last disk-progress gap to death is
1,104,016 bytes. No later completed boundary can be more than 64 batches away, and no projected
>=16 MiB high-water event can occur without a preceding intent. The kill is conclusively during
reservoir write, before finish or base concat.

## Focused disabled controls

Final release, sf=10, persisted strict mode, same cgroup, receipt env absent:

| query | status | execute | rows | spill | max RSS | scratch after |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| q03 | pass | 15.487 s | 114,003 | 233,338,304 | 527,596 KiB | 0 |
| q10 | pass | 12.701 s | 381,105 | 215,703,848 | 1,696,708 KiB | 0 |
| q19 | pass | 86.568 s | 1 | 51,471,552 | 291,288 KiB | 0 |

No control receipt exists. These are the required focused shared-family controls; no claim is made
that all thirteen historical controls were rerun.

## Verification and scope

Green final gates:

- `scripts/cargo-cgroup fmt -- --check`
- `scripts/cargo-cgroup check -p toron --all-targets`
- `scripts/cargo-cgroup check -p toron-tpch --all-targets`
- dedicated residency profiler tests: 11 passed
- `scripts/cargo-cgroup test -p toron streaming --lib`: 66 passed
- `scripts/cargo-cgroup test -p toron join --lib`: 238 passed
- `scripts/cargo-cgroup test -p toron spill --lib`: 52 passed
- `scripts/cargo-cgroup test -p toron --test architecture_test`: 2,079 passed
- `scripts/cargo-cgroup test -p toron --no-fail-fast`: all unit, integration, and doc tests
- `scripts/cargo-cgroup test -p toron-tpch --features polars-sql-reference --no-fail-fast`:
  all unit, integration, and doc tests (only pre-existing documented ignores)
- `git diff --check`

- `exec.rs` is unchanged and its ratchet is untouched.
- Engine source and the env surface contain no q05 or TPC-H branding.
- No memory cap, task budget, comparator, schema, or execution behavior changes when disabled.
- No `.perf.data` is tracked.
- Diagnosis only: q05 remains red and the board remains 19/22. ADR 430 owns the fix.
