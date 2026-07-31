# ADR 427 - q21 partition-at-a-time replay 1: one scan, one index per partition, under the live summary baseline

## Context

Post-426 board: 18/22 green. ADR 426's durable seven-stage receipt proved q21's wall to a single
stage: **replay 1 (the orders inner join) consumed 99.99% of drive time** (582 s over 232 probe
calls at the 600 s window; stable at 1800 s), with the fold (197 us) and pull interface
(immaterial residual) ruled out by measurement. 427 executes the named fix - and the iteration
grounded that the naive "persist the scan reservoir" is **not** the fix, because replay 1 has
**two** pathologies, not one:

1. **Re-scan per probe call.** The `PartitionedScan` probe is all-or-nothing
   (`exec/join/streaming.rs:12603-12683`): `try_load_resident_...` (`:12624`) rejects the
   standalone all-resident estimate at sf=10. That helper has no summary-baseline input, so the
   two live summaries do **not** cause its current decision. With `loaded_builds` empty, **every**
   probe call falls to `probe_partitioned_streamed_inner_scan_build` (`:12661`) and re-streams
   the whole build. This is what 426 measured. The summaries matter to the new persistent
   lifetime and must be newly accounted there.
2. **Re-index per probe call.** Even the existing reservoir probe
   (`probe_partitioned_streamed_inner_scan_build_reservoir`, `:12992`) rebuilds each partition's
   index on **every** call - `run_loaded_partition_work` builds it on the fly (`:13065`) or
   `rebuild_index_from_pages` (`:13070`) - and reads the whole partition via
   `read_all_batch_pages` (`:13048`). So persisting only the scan would replace re-scan with
   re-index. **ADR 413 already established that scan-once-but-reindex-per-batch can still time
   out.**

Two more grounded gaps: `create_with_resident_baseline` charges **one** writer, not N sibling
partitions plus the summaries; and `try_load_resident_...` takes `estimated_build_bytes` but no
baseline, deciding independently of the summaries. The fix is a **restructure of replay 1 in the
q21 chain owner**, reusing generic components - not a wire-in and not a global `PartitionedScan`
change.

## Decision

Restructure replay 1 as a **partition-at-a-time grace hash join** owned by the q21 chain driver,
making **both the source scan and the index one-shot**:

### The mechanism

- **Lazy admission after the completed baseline.** After the first replay-0 output, with both
  recognized-Neq summaries built and their resident bytes known, the owner admits the
  partition-at-a-time path. Before that point nothing is scanned or written (fork A).
- **Both sides reservoired once.** Write the orders build once into partition reservoirs; buffer
  the replay-0 probe output into a **spill-backed** probe reservoir (so a large replay-0 output
  is bounded, never held resident).
- **Drain partition-at-a-time, one index each.** For each orders partition: build its compact
  index and retain its decoded build pages in **one combined page pass**; **stream the probe
  reservoir's pages for that partition individually** against the pinned pages; emit; drop the
  index/pages; advance. One source scan; one index build and one build-page decode per partition.
  Calling today's `rebuild_index_from_pages` and then `try_pin_decoded_pages` does not satisfy
  this contract because that sequence reads the build pages twice; the cut owns a narrow
  combined rebuild-and-pin seam.
- **Stream pages, never materialize a probe partition.** The probe drain reads probe-reservoir pages
  **one at a time** - **no `read_all_batch_pages`, no `concat_or_empty`, no partition-sized batch
  vector** on the probe side. The active build partition is deliberately retained because Phase
  0 must prove that its compact index plus decoded pages fit under the full owner baseline. The
  drain measures build-page reads and must not re-decode them per probe page (the ADR 420
  page-amplification class must not reappear on the build side).
- **Commit contract** (fork B): a **zero-I/O pre-commit** decline falls back to today's path
  before any scan or write. Once construction starts, any failure is a **labeled, cleaned
  error** - never a silent fall-through to per-call re-scan/re-index. Reservoir files are cleaned
  on every post-commit exit (424 discipline; stale join spill has broken sf=10 runs before).
- **Owner-scoped, generic components** (fork C): the scheduling lives in the q21 chain owner
  (`try_streaming_inner_prefix_semi_anti_scan_probe_base`) reusing the generic reservoir
  writer/loader; no global `PartitionedScan` behavior change, so stages that take the resident
  shortcut cleanly are untouched.

### Full-peak owner accounting

The owner guards the **complete** resident peak against the unchanged cap, not just
"summaries + one index":

```text
peak = two 421 summaries
     + all other retained inner runtimes
     + both reservoirs' handles, page metadata/reserves, and retained writer/cache state
     + the current partition's build index + its pinned decoded build pages
     + the current streamed probe page
     + bounded current selection/output batches
     + grouped-fold state
```

Bounded by the **largest single partition**, not the whole build. The owner alone knows the
summary baseline, so the guard lives there (fixing the per-writer / no-baseline-input gaps).
The active spill object's baseline is refreshed or passed explicitly from the owner immediately
before the combined rebuild-and-pin pass; the baseline captured when reservoir files were first
created is not treated as current after the probe reservoir and fold have grown. Output batches
are consumed by the suffix before the next probe page; no cross-page output vector is retained.
The owner rechecks the same full sum at every growth boundary: reservoir writes, each pinned
build page/index-growth step, each decoded probe page/output materialization, and after each fold
update while the active index/pages remain live. A pre-partition estimate is not the only guard.

### The skew floor (named now, per ADR 413)

If the largest single orders partition's full active peak cannot fit under the cap, that
partition cannot be processed one-shot.
**Phase 0 measures the max-partition index at sf=10 under the live summary baseline** - the
decisive de-risking number. If the full active-partition peak does not fit, 427 seals as the
grounded finding naming that skew wall. Recursive sub-partitioning is the bounded follow-on, not
a second mechanism folded into this cut; 427 does not ship a path that silently re-scans.

### Receipt extension

Extend the 426 seven-stage durable receipt through the **deferred partition drain** with atomic
**partition-index and page-position milestones**, so an external kill during the drain still
leaves parseable evidence (which partition, how many probe pages) - the before/after instrument
for the replay-1 collapse.

## Acceptance

- **Primary:** q21 sf=10 reaches `pass@compare` under **both** the 4 GiB cap and the 600 s gate
  (**board 19/22**), **or** advances to a named, reproduced next wall (the skew floor, or a
  newly-dominant stage) with replay 1's collapse proven by the receipt. Over-gate completion
  earns no board credit (the standing honesty rule).
- **Mechanism proof (426 receipt, before/after):** replay 1's exclusive share collapses from
  ~99.99% to a small term; the drive shows **one** orders scan and **one** index build per
  partition; no stage-behind-the-wall or buffered-probe bookkeeping becomes the new dominant
  term.
- **Streaming-probe lock:** the probe drain uses no `read_all_batch_pages`, `concat_or_empty`, or
  partition-sized probe vector; a lock asserts per-page streaming.
- **Build-page-reread lock:** each partition's build pages are read/decoded once per drain; a
  counter proves no per-probe-page re-decode (the 420 class stays closed).
- **Full-peak guard lock:** a fixture where summaries + one-partition index + probe page +
  retained runtime/reservoir state + decoded pages + outputs + fold would cross the cap fails
  loudly (labeled), not silently; the guard names each component. A second lock proves the
  active partition uses the owner's current baseline rather than its creation-time baseline.
- **Committed-arm locks:** zero-I/O pre-commit decline falls back cleanly; post-commit failure is
  labeled and leaves no reservoir scratch.
- **Correctness parity:** the restructured replay 1 yields the same q21 answer as the pre-cut
  path (sf=1/sf=0.01 compare unchanged; full-row parity).
- **Receipt-durability lock:** a simulated kill mid-drain leaves a parseable receipt at the last
  completed partition/page.
- **Controls:** the twelve sf=10 greens + q18's 425 green untouched (owner-scoped change; other
  stages unaffected). Full engine gate after fmt; `exec.rs` ratchet untouched; no `.perf.data`.

## Guardrails

- **Both scan and index one-shot** - fixing only the scan is the 413-known-insufficient shape.
- **Stream probe pages; read build pages once** - no `read_all_batch_pages`/`concat` on probe; no
  per-probe-page build re-decode (420 class).
- **Full owner peak, all components** - summaries + index + decoded pages + probe page + outputs
  + fold; guarded where the baseline is known.
- **Skew floor named and measured** - a partition that cannot fit one-shot is a stated wall, not
  a silent re-scan.
- **Committed arm** - zero-I/O pre-commit fallback only; post-commit failure labeled + cleaned.
- **Owner-scoped** - no global `PartitionedScan` change; resident-shortcut stages untouched.
- **Won ground is a control** - twelve greens + q18 untouched; receipt proves the collapse.

## Risks

- **Skew: one huge orders partition** -> Phase 0 measures the full max-partition peak; seal with
  the named skew wall and carry recursive sub-partitioning as the next mechanism, never silently
  re-scan.
- **A stage behind replay 1** -> the receipt re-measures the full split; a newly-dominant stage
  is an accepted named outcome.
- **Buffered replay-0 output larger than expected** -> the probe side is spill-backed and
  streamed per page, so it is bounded; resident reservoir metadata/writer state is in the owner
  peak. Aggregate reservoir disk volume is measured, while every partition writer remains under
  the unchanged per-writer spill quota exactly like ADR 413's landed multi-writer reservoir; 427
  does not reinterpret that quota as a new aggregate cap.
- **Build-page re-decode amplification** -> the build-page-reread lock keeps the 420 class closed.
- **Committed-arm leak** -> post-commit cleanup on every exit; the durable receipt + scratch
  check prove it.

## Forward Arc

q21 green closes the throughput thread at 19/22 and banks a reusable owner-scoped grace-hash-join
schedule (the partition-at-a-time drain + full-peak guard) that q05's diagnosis may reuse. A
named skew wall instead feeds the same recursive-partition mechanism. Remaining: q17 (oracle
boundary-precision fidelity), q02 (join-output C-spec rock), q05 (diagnosis-first OOM). The 426
receipt remains the throughput instrument.

## Grounding evidence (file:line)

Two pathologies: `exec/join/streaming.rs:12603-12683` (all-or-nothing; re-scan else-branch
`:12661`), `:13065`/`:13070` (per-call reindex), `:13048` (`read_all_batch_pages`). Reservoir
components: `:12844` (`build_..._reservoir`, `create_with_resident_baseline`), `:12992`
(`probe_..._reservoir`). Baseline gaps: per-writer `create_with_resident_baseline`;
`try_load_resident_...` has `estimated_build_bytes` but no baseline. Chain owner:
`try_streaming_inner_prefix_semi_anti_scan_probe_base`. 421 summaries: 404,750,336 bytes each
(pair 809,500,672). 426 measured split: replay 1 582,443,597 / 582,492,546 us (99.99%), 232
probe calls; 413 one-pass-reservoir precedent (scan-once, reindex-per-group). 426 receipt: the
landed seven-stage durable instrument.
