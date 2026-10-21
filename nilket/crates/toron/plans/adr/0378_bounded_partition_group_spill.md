# ADR 378 - bounded partition/group spill (q03/q05/q10/q12 streamed inner-build spill)

## Context

ADR 377 removed the whole-build-per-partition re-scan by routing the build into per-partition spills in one pass. The
four inner-build-spill cells now fail quickly and just over the unchanged 512 MiB spill budget: q03/q05/q10 =
536,871,224 / 536,870,912 (**312 B over**), q12 = **5,968 over** (377 results).

The important correction: the recorded producer is **`streamed inner join build spill writer`**, not the aggregate
`partitioned streamed inner join build spill writer` guard. That means the first visible wall is the **writer's own spill
bytes**, not yet proven to be only the all-partition aggregate peak. Projection is already applied (page writes use
projected `write_build_keys`), so the lever is not more projection. The durable fix is to size the partitioned spill so
both levels are bounded:

- **per-partition writer spill** stays below the 512 MiB budget with headroom; and
- **live group spill** stays below the 512 MiB budget when only one partition group is present.

A byte-trim would green today but is fragile (sf=100 blows past it). Partition/group sizing is the durable fix.
Parallelism waits one cut.

## Decision

### Phase 0 - classify the spill wall + size P and G from measurements
1. Confirm q03/q05/q10/q12 all hit the **377 streamed inner-build spill writer** wall with projected columns already
   applied (not a projection gap, not residency, not re-scan CPU).
2. Discriminate the spill dimension before code:
   - **Per-writer wall:** one partition writer reaches `streamed inner join build spill writer` over budget. Grouping
     alone does not fix this; the partition count `P` must increase or the partition must be split.
   - **Aggregate-live wall:** no individual partition exceeds budget, but all active partitions live together exceed the
     aggregate live-spill budget. Grouping fixes this.
   - If both apply, fix both.
3. Measure, per cell: projected build spill, current `partition_count`, largest partition writer spill, aggregate live
   spill, and over-budget bytes.
4. Pick **partition count `P`** from the measured per-writer peak, not from memory residency alone. `P` must make each
   partition writer stay comfortably under 512 MiB with headroom.
5. Pick **group count `G`** from the measured aggregate live peak. `G` must make one group's live spill stay
   comfortably under 512 MiB with headroom. Given the overage is bytes-to-KiB, the expected outcome is small (`P` and/or
   `G` around 2), but Phase 0 decides.

### The cut - spill-budget-aware partitions + bounded groups
- Size the streamed inner-build partitioning by both resident memory and spill budget: `P = max(memory_required_P,
  spill_required_P)`, rounded consistently with the existing partition-count policy and capped by the existing hard
  maximum.
- Partition those `P` partitions into **G ordered groups**. For each group: scan the build once, route only that group's
  partitions into their spills, probe, clean up the group's spill, then the next group. Live disk spill = one group's
  payload (< 512 MiB), never the whole build.
- Preserve output order by processing groups as ascending partition ranges and partitions in ascending order within each
  group. Non-contiguous/hash group layouts are allowed only if they restore the same partition order observable in ADR
  377.
- Reuse 377's router + projected writes within each group. Topology-named; no qNN in core.

### Honesty line (the trade, stated)
This **reintroduces bounded G-pass source scanning** that 377 removed. That is acceptable because it replaces
**O(partitions)** re-scans (pre-377) with **O(G)** where **G is chosen by budget** (expected small: 2-4), while keeping
377's per-partition routing within each pass. It is the intended memory<->CPU trade for fitting the spill budget.

### Stop line
If `P` exceeds the existing hard partition cap, or `G` becomes large enough to behave like the old per-partition re-scan
rather than a small bounded pass count, the design failed - **name it as a residual, do not ship it as "bounded."**
`G == P` is acceptable only when both are small (for example two partitions/two groups). Given the bytes-to-KiB
overage, a small `P`/`G` should suffice; if it does not, that is the signal.

## Acceptance
- **q03/q05/q10/q12: green-or-next-wall** at sf=10 fixed 4 GB - either `pass@execute` + persisted correctness, or a
  *different* named wall (not the same streamed inner-build spill writer wall).
- **Spill peak bounded at both levels:** per-writer spill and live group spill stay under 512 MiB by design; **no cap
  bump**; budget producers/names unchanged.
- **P and G chosen by measured peaks**, not hardcoded; `P` stays within the existing cap and `G` stays a small bounded
  pass count (else named residual per the stop line).
- **Live vs cumulative accounting separated:** the budget guard is over live per-writer/per-group spill. Telemetry may
  still report cumulative bytes written across groups, but cumulative bytes must not be mistaken for live reservation.
- **Parity unchanged:** row order, schema, and join output identical to the 377 single-pass path - a focused test
  (multi-batch, multi-partition, null/residual keys) + q03 small-scale strict `pass@compare`; persisted sf=10
  correctness for any new green.
- **Coverage measured:** q03 canary, then q05/q10/q12 measured (green/advanced) - the family, not promised.
- **`--no-fail-fast` required**; de-branded; prior greens unchanged.

## Guardrails
- **Bound the peak, don't raise the budget** - per-writer and live group spill < 512 MiB by design; no cap bump.
- **P/G from measured peaks** - else the stop-line residual.
- **Budget live spill, not cumulative telemetry** - group cleanup releases the live reservation before the next group;
  cumulative write counters can exceed one group and remain telemetry-only.
- **Parity is the correctness gate** - G-pass output identical to 377's single-pass (row order, schema, join output).
- **Honest about the G-pass scan** - O(partitions) -> O(G), budget-chosen; not hidden.
- **Green-or-next-wall; coverage measured not promised; --no-fail-fast; de-branded; no qNN.**

## Risks
- **Misclassified wall** - grouping alone cannot fix an individual writer over budget; Phase 0 distinguishes per-writer
  and aggregate-live walls before code.
- **P/G creep** - the stop line catches it; name residual, don't ship.
- **Per-group cleanup leaves stale spill** - explicit free + a resident/peak assertion test.
- **Parity drift across groups** - the order/output parity test vs 377's single-pass.
- **A cell advances to a new wall** (not green) - honest green-or-next-wall; record it.
- **Regression** - `--no-fail-fast` required.

## Forward arc
378 (bounded partition-group spill) makes the spill-budget invariant robust across the inner-build-spill family. Then
**parallelism** (the deferred one-thread systemic, 373) - the bigger architecture lever for q07, on a cleaner board.
q17 (grouped fold + spill-read), q19 (spill-read I/O), q21 (712-byte resident-state) remain queued.
