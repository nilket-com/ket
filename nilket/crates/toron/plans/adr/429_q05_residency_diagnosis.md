# ADR 429 - q05 residency diagnosis: explain a lost green under the 4 GiB cgroup

## Context

Post-428 board: 19/22 green. Three cells remain (q05, q17, q02). This ADR takes q05 as a
**diagnosis-only lost-green / reproducibility investigation**, not as a never-named OOM:

- ADR 413 made the one-pass partition-reservoir path complete persisted sf=10. Its perf row
  finished in 190,034 ms with 5,353,985,328 spill bytes and 3,051,064 KiB process RSS; the
  canonical correctness row also passed, but its systemd unit reached the exact 4 GiB cgroup
  peak. The mechanism was green but already had no proved cgroup headroom.
- ADR 414 could not reproduce that green even at its clean plan commit.
- ADR 415 then reproduced three hard OOM kills at about 42 s. Every unit reached
  `MemoryPeak=4,294,967,296`, emitted no terminal row, and left about 1.3 GiB of spill scratch.

The precise question is:

> Why did the previously successful one-pass q05 reservoir become a repeatable 4 GiB kill, and
> which simultaneously live engine or cgroup component owns the peak?

### Narrowed hypothesis, not a conclusion

The generic grouped path fully accumulates its base batches before entering
`try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`. That helper builds the
partition reservoir while the base remains resident and calls `concat_or_empty` only after the
reservoir build finishes. The killed runs retained roughly 1.3 GiB of scratch, versus ADR 413's
5.27 GiB completed reservoir.

Those facts are **consistent with** a kill during reservoir construction under the co-resident
base baseline; they do not prove it. Scratch bytes are disk progress, not resident bytes, and a
cgroup OOM can be driven by anonymous memory, charged file cache / dirty spill pages, kernel
memory, or their overlap in time. The receipt must distinguish these possibilities. A cap bump
remains doctrine-rejected.

## Decision

Land an off-by-default, query-neutral, durable residency receipt on the generic grouped
partition-reservoir path and use the canonical q05 sf=10 kill to name ADR 430. ADR 429 ships the
diagnosis only; it does not pre-commit or half-land the fix.

The instrument follows ADR 426's atomic kill-surviving receipt pattern, but its subject is peak
residency rather than elapsed time. It lives in `exec/join/streaming.rs`, not `exec.rs` (which
remains at its 7,500-line ratchet), and carries no q05/TPC-H naming in engine source or its env
surface.

### Two non-additive measurement ledgers

Every snapshot carries two views. They are compared, never added together:

1. **Engine ownership ledger**
   - base-batch owned/accounted bytes and vector capacity before reservoir construction;
   - reservoir writer count and aggregate resident state across every writer: resident index,
     writer scratch/peak, page and path metadata, cache, handles, and collection capacity;
   - receipt-owned resident bytes;
   - build rows/batches, active phase/partition, and spill bytes as **disk progress**, explicitly
     excluded from resident-byte sums;
   - checked sum of the engine components plus a named allocator/unattributed residual when an
     applicable process-anonymous total is greater. Shared Arrow buffers or other possible
     overlaps are identified rather than silently double-counted.
2. **OS/cgroup ledger**
   - process RSS as a secondary process-only signal;
   - cgroup-v2 `memory.current` and the available `memory.stat` categories, including at least
     anonymous, file cache, dirty/writeback file pages, shmem, and kernel/slab state;
   - the postmortem systemd `Result`, `MemoryPeak`, and `memory.events` OOM counters.

The normative kill evidence is the cgroup result and `MemoryPeak`, not process RSS. The engine
ledger attributes owned structures; the cgroup ledger exposes charged memory that engine
accounting cannot see, especially spill-backed file cache. Cross-ledger fields are explicitly
non-additive.

### Bounded allocation-intent coverage

An after-batch receipt can be killed inside the decisive allocation, but flushing before every
small allocation would distort the workload. The receipt therefore persists an allocation
intent at these bounded points:

- before reservoir-writer-set creation;
- before a reservoir append/capacity change whose projected resident high-water advances by at
  least **16 MiB**, with the threshold recorded in the receipt;
- before the base concat and every phase transition;
- plus a periodic snapshot every **64 completed build batches**.

An intent records the phase, current owned bytes, projected delta/total, and whether the number
is exact or estimated, and is flushed before the corresponding operation. Smaller cumulative
growth is bounded by the periodic cadence. If Phase 0 shows a single allocation can exceed the
threshold without passing an instrumentable call boundary, the nearest owning call is bracketed
instead; the receipt never claims finer attribution than it has.

Snapshots use atomic sibling-temp write plus rename. Enabled serialization/write/rename or
cgroup-sampling failures are labeled and loud; a missing field is acceptable only when recorded
as unavailable, never silently zero.

### Calibration and disabled behavior

The env path is resolved once. When absent, the path creates no receipt state and performs no
receipt I/O; output, digest, telemetry, and plan fingerprint remain identical. This is an
instrumented branch, not a claim of identical machine code.

Before the canonical diagnostic, three completing sf=1 fresh-process runs per mode compare
disabled and enabled operation. They must preserve output/digest and keep enabled median wall
overhead at or below 10% and maximum-RSS delta at or below 32 MiB. Missing either bound requires
reducing receipt allocation/sampling cost and recalibrating before sf=10. The plan does not
silently weaken its 64-batch / 16 MiB attribution bounds to pass calibration; if those bounds
cannot be met without material perturbation, the receipt design must be re-converged.

## Diagnosis deliverable

Run persisted strict q05 sf=10 under the unchanged `MemoryMax=4G`, `MemoryHigh=4G`,
`TasksMax=512`, and 600-second envelope. The last durable state must establish:

- whether the kill occurred during writer creation, reservoir write/growth, reservoir finish,
  base concat, probe, suffix processing, or grouped fold;
- the co-resident base contribution and aggregate state of all writers;
- whether the dominant cgroup charge was anonymous engine memory, file cache/dirty spill pages,
  kernel memory, or a distributed combination;
- the bounded gap from the final completed/intent snapshot to the kill;
- why ADR 413's successful row was knife-edge or behaviorally different, using its process-RSS,
  cgroup-peak, spill, fingerprint, and phase evidence without conflating those metrics.

If the evidence does not discriminate the peak owner, ADR 429 does not seal: refine the receipt
at the last ambiguous boundary and rerun. Once discriminating, name ADR 430 from the evidence.
Possible mechanisms are examples only—stream/spill the base, change reservoir lifetime,
partition-at-a-time ownership, or control spill-cache pressure—and are not selected in advance.

## Acceptance

- **Before state:** the promoted-plan parent reproduces the current q05 OOM with fresh scratch;
  the ADR 413 successful perf and correctness rows, ADR 414 non-reproduction, and ADR 415 x3 kill
  are recorded without treating process RSS as cgroup peak.
- **Kill-surviving diagnosis:** a parseable last snapshot/intent plus cgroup postmortem names the
  phase and peak owner with a stated attribution bound; inconclusive evidence triggers a receipt
  refinement, not an asserted finding.
- **Dual-ledger honesty:** engine resident ownership, disk spill progress, process RSS, and cgroup
  categories are labeled separately; cross-ledger values are not summed; checked sums and named
  residuals never saturate or hide overlap.
- **Intent/cadence locks:** intent precedes every named >=16 MiB high-water event and base concat;
  periodic progress is no sparser than 64 completed build batches; interrupted temp writes
  preserve the previous complete snapshot.
- **Disabled and calibration locks:** disabled output/digest/telemetry parity with zero receipt
  writes; enabled sf=1 output parity, <=10% median wall overhead, and <=32 MiB max-RSS delta.
- **Honesty:** diagnosis-only; board stays 19/22; no cap/budget change; ADR 430 is named only from
  the measured owner.
- **Scope:** full engine gate; focused generic grouped-reservoir controls with the receipt
  disabled; `exec.rs` ratchet untouched; no q05 branding in engine source; no `.perf.data`.

## Guardrails

- Lost-green and exact-cgroup-peak history are both load-bearing; ADR 413 is not simplified to a
  comfortable 2.9 GiB green.
- Disk spill bytes are never presented as resident memory, and RSS is never presented as the
  cgroup kill counter.
- Receipt I/O is bounded by phase, 64-batch cadence, and 16 MiB high-water intents.
- No preselected fix, cap bump, query-number branch, or profiler artifact ships.
- Won ground remains a control with the receipt disabled.

## Risks

- **File cache owns the gap:** cgroup `memory.stat` makes that visible instead of forcing an
  engine-structure answer.
- **Kill occurs between snapshots:** high-water intents and periodic cadence bound, but do not
  overstate, the attribution gap.
- **Accounting overlaps or omits allocator state:** separate ledgers, explicit overlap labels,
  and residuals expose the uncertainty.
- **Receipt perturbs the wall:** the quantitative calibration is a stop line before sf=10.
- **Peak is distributed:** a distributed co-residency finding is valid if the ledgers prove it;
  the plan does not require a fictional single allocation.

## Forward Arc

ADR 429 converts q05 into a measured peak owner and a precise ADR 430 mechanism. q17 remains an
unvalidated oracle boundary-fidelity risk, not a proven divergence: before lifting its sf=0.02
pin, a future cut must compare an sf=10 structural boundary census (eligible parts,
contributing rows, pre-division raw sum, per-part digest, and below/equal/above-threshold counts)
as well as the final scalar. q02 remains the join-output C-spec rock and stays last.

## Grounding evidence

Lost-green history: `plans/impl/413_one_pass_partition_reservoir_results.md` (q05 result and
correctness), `plans/impl/414_q12_spilled_index_compaction_results.md` (q05 control caveat), and
`plans/impl/415_sf10_full_board_live_sweep_results.md` (q05 x3 OOM). Suspect seam:
`exec/join/streaming.rs` (`try_streaming_reoriented_inner_chain_generic_base_grouped`,
`try_process_reoriented_inner_chain_grouped_partitioned_scan_reservoir`, reservoir build, then
base concat). Existing writer accounting: `SpilledInnerJoinBuild::owner_retained_resident_bytes`
and its resident index/writer/cache/page metadata components in `exec/join/spill.rs`. Durable
receipt precedent: ADR 426/427 atomic stage receipt.
