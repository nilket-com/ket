# ADR 508 - the aggregate operator: two fronts, inner attribution before any remedy

ADR 507 measured where the composed gap actually is. Across the seven candidate cells at strict sf=8,
exclusive time by node kind: **aggregate ~76%, join ~15%, exchange ~5%, sort ~1%, scan ~1%.** That
retires the encoded-column premise the campaign had leaned on since ADR 505 — the scan is one percent
of the gap — and makes the aggregate operator the first optimization target on measured share rather
than on a presumed mechanism.

This ADR opens that cycle. It authorizes no kernel. Its job is to split the target into the two fronts
the attribution actually shows, give each its own attribution and acceptance path, and gate any remedy
on knowing what is inside the operator.

## What the ranking does and does not say

Per cell, exclusive share of the dominant node:

| cell | execute ms | dominant | spill | front |
|---|---:|---|---:|---|
| q17 | 35,693 | aggregate 100% | 623 MB | spill-replay + compute-fold (see below) |
| q21 | 17,935 | aggregate 100% | 898 MB | spill-replay |
| q09 | 13,032 | aggregate 95% | 0 | **join-probe — coordinate correction below** |
| q20 | 9,476 | aggregate 61%, join 33% | 0 | compute-fold |
| q08 | 7,982 | aggregate 96% | 0 | **reassigned — join-probe, see below** |
| q18 | 19,430 | join 65%, exchange 17% | 990 MB | join/sort (parked) |
| q02 | 2,675 | sort 41%, join 31% | 0 | join/sort (parked) |

**q09's withdrawal was caused by a coordinate mismatch, not demonstrated tree movement.** The
step-2 run that measured 83.2 MB and 14,939 ms used the harness defaults — 64 MiB per task and
512 MiB spill reserve — while this board's ladder coordinate is 2 GiB / 2 GiB. A clean full-hash
re-grounding at the ladder coordinate records q09 at zero spill (13,421.8 ms in the attribution run;
13,349.9 ms median on the first clean board attempt) and preserves the join-probe-dominated inner
shape. The 83.2 MB result remains a valid low-budget fact but cannot invalidate a 2 GiB premise.
**q09 is provisionally restored to the join-probe premise, subject to the final clean 22/22 rerun
after the q21 attribution correction.** It is not promoted to the spill front.

**q08's row is reassigned, and the trap below is why.** Step-2 inner attribution decomposed the
`reoriented_inner_chain_grouped` handler on both q08 and q09 (`impl/508_inner_attribution.md`). Inside
the aggregate node the **grouped fold is 0.7% (q08) and 1.8% (q09)**; join work is **87.7% and 86.7%**,
with suffix probing alone at ~50% on both. So q08's compute-fold assignment rested on exactly the
inference the next section forbids — "aggregate 96%" read as "the fold kernel". **q08 moves to a
join-probe premise**, and the compute-fold front's step-4 decomposition covers q20 and q17 inv 0 only.
q09 joins that same premise at the corrected memory coordinate. Whether join-probe earns its own
successor front is step 4's call against the final clean baseline, not a mechanism selection this note
makes.

That the guard below caught a real case is the point of having written it down: the per-node layer was
correct and the front *name* was one layer too coarse.

**Fronts are a property of an invocation, not a query.** The step-1 handler map
(`impl/508_handler_map.md`) measured q17's dominant node as *two* invocations on different handlers:
one `scan_grouped` at 31% with zero spill (compute-fold) and one `correlated_scalar_left_outer_global`
at 69% carrying all 623 MB (spill-replay). The cell column above is therefore a summary; the ranked
premises in steps 4 and 5 are built per invocation. A cell may contribute to both fronts.

**The trap this ADR exists to avoid.** "Aggregate" here is a *plan node*, and for most of these cells
that node is a **fused streaming operator** — a handler that internally does a join, a fold, and
sometimes a spilling replay, all under one `ExecutionPlan::Aggregate`. Reading "aggregate 96%" as "the
fold kernel is 96%" would be a category error: the join half of a fused join+aggregate is inside that
number. The per-node layer localizes the gap to an operator; it does not decompose the operator.

And the operator is decomposed for only one of the five aggregate-dominated cells. ADR 507's coverage
probe ran all seven cells at sf=8 against both existing fused-loop profilers; **only q21 emitted**.
q21's inside is therefore known — 76% of its attributed drive is `inner_replay_1`. For q17, q08, q09
and q20 the dominant operator is opaque. Any remedy chosen now would be chosen blind for four of five
cells.

The ADR 505 lesson applies directly: a kernel picked before the decomposition (batched prefetch on a
cache-resident three-entry table) was a wash, because the premise about *where* the cost sat was never
tested. The same gate applies one layer down.

## Decision

Six ordered steps. Each gates the next; a step that fails stops the cycle rather than handing a weaker
premise forward.

**1. Confirm the handler map.** For each aggregate-dominated cell, establish which fused handler owns
the dominant node. ADR 507's grounding observed three distinct families in play
(`correlated_scalar_left_outer_global`, `reoriented_inner_chain_grouped`, `scan_grouped`) plus q21's
already-instrumented `inner_prefix_semi_anti_scan_probe_base`, but that mapping came from throwaway
tracing and is not recorded in any artifact. Re-establish it as a durable receipt: this is the list of
what has to be instrumented.

**2. Inner attribution for the dominant handlers.** Extend attribution one layer beneath the node —
inside the fused operator — for the handlers step 1 names, and only those. Every profile reconciles
through the ADR 507 contract: named spans plus an explicit residual, both axes, failing closed. The
minimum each must answer: **how much of this operator is join, how much is fold, how much is spill
machinery.** Reuse before building — q21's chain profiler already answers this for its handler, and its
stage vocabulary is the starting point, not a template to copy across lifecycles it does not fit (the
ADR 507 finding: vocabulary follows lifecycle, the contract is what is shared).

**The bridge between layers is part of the contract, not an afterthought.** A handler profile that
reconciles only against itself proves nothing about the node it is supposed to explain. Each profile
must close the chain:

```
inner stages ⊆ handler span ⊆ matched Aggregate-node exclusive span ⊆ execute_bound span
```

with the residual at every link explicit, and every link reconciled — an internally tidy handler
profile sitting inside an unexplained node is the failure mode this requirement exists to catch.

**Multiple invocations must be matched or summed explicitly.** This is not hypothetical: the ADR 507
board records q17's dominant node as `aggregate, calls = 2` — its 100% is a *sum across two
invocations*. A single handler profile there would reconcile internally and still leave half the
dominant node opaque. The receipt must therefore identify which invocation each inner profile belongs
to, or sum the matched instances and show that sum closing against the node's exclusive span. A cell
whose invocations cannot be matched is recorded as such rather than reported as covered.

**3. Clean sf=8 baseline, measured across an explicit commit boundary.** The ADR 507 board is council
grounding measured from a dirty tree (`cab8ea9f-dirty`) and cannot carry a movement claim. A clean
board cannot be measured from the same commit that is still being edited — a binary built from a tree
that is later squashed or amended has no recoverable identity, which is exactly how the 507 board
acquired its `-dirty` stamp. So the baseline is **two cohesive deliverables, in order**:

- Step 1 (the handler map and its routing instrument) **may seal independently** — it is a durable
  receipt plus a reusable instrument, independently valuable and revertible, not a review pivot. Its
  own measurements are routing facts and may be taken from a working tree, provided the receipt says
  so; they are never baseline numbers.
- **Seal and push** the cumulative step-2 inner attribution, then enable q11 at sf=8 before measuring.
  Step 2 sealed at `3d48890e` without the q11 change; rewriting that pushed seal would destroy the
  immutable boundary this rule exists to create. The q11 harness support therefore lands as a
  separate, independently valuable follow-up. The first clean-board attempt then exposed that q21's
  separate chain profiler had never been bridged into the invocation-level stage contract. That
  corrective implementation also earns its own commit: it closes an acceptance red and changes the
  binary. The pushed descendant containing both q11 support and the q21 bridge — not `3d48890e` or
  the pre-correction `b6e37cf9` — is the build identity of record and is never amended afterwards.
- **Then measure** the 22/22 board from that exact pushed commit, and land the numbers as a **separate
  perf receipt** citing it.

The test for whether a commit earns its place is whether it is independently valuable and revertible —
not a fixed count. What is fixed is that the baseline's identity is a clean, pushed, unamended commit.

While touching the baseline harness, **enable q11 at sf=8** — an enumeration gap, not a spec limit
(`0.0001 / 8 = 0.0000125` is exact and the oracle integer is 800) — making the clean board 22/22.

**4. Compute-fold front — decompose and rank the premises.** q20 inv 0 and q17 inv 0. (This step was
written naming q08 inv 0 and q09 inv 0 as well; step-2 inner attribution reassigned both to a
join-probe premise — see the front table above.) Zero spill, so the
cost is compute inside a fused join+aggregate. Instrumenting
`scan_grouped` also yields a profile for q18's invocation because they share a handler — that is an
incidental by-product held for the parked front, and is **excluded** when ranking this front's premise;
q18 is join-dominated and stays parked. Split it join-half vs fold-half vs exchange and produce the
front's **ranked premise**: which sub-component holds the share, how much, and what would have to be
true for a remedy to reach it. Selecting and building that remedy is the successor ADR's decision, not
this one's. Note what is already closed: the primitive ladder took m04's grouped fold to a producer
boundary (ADR 505) and m05 is already ahead of the reference. If the composed fold cost is the same
fold, that is a finding that redirects the premise to the join half — exactly the sort of thing only
the decomposition can say.

**5. Spill-replay front — separately, and ranked separately.** q17 inv 1 (623 MB) and q21 inv 0
(898 MB): hundreds of MB inside a single invocation. Do **not** assume this front shares the compute
front's premise or its remedy; the mechanism here is spill and replay machinery, not fold arithmetic,
and the two fronts hand the successor two independent premises rather than one blended one. q21's
invocation already has inner attribution; q17's `correlated_scalar_left_outer_global` invocation has
none and is the largest single uncovered item on the board (69% of a 35.7 s cell), so it is the
front's first instrumentation target. The corrective q21 vocabulary now also closes its full
invocation-level chain; the old separate chain profile alone did not satisfy that acceptance link.
At the 2 GiB coordinate q21 splits into runtime preparation (33.1%) and a deferred replay envelope
(54.3%); inside that envelope reservoir buffering is 42.2% and replay-1 read/probe is 46.3%, with
72.7 ms residual. The task spill counter advances by 24.3 MB during buffering and by 873.4 MB at
release/accounting, so byte ownership remains a write-accounting fact rather than a timing claim.

**6. Park the join/sort front.** q18 (join 65%) and q02 (sort 41%, join 31%) are a real, measured
second front. They stay parked so this cycle stays one decision wide; re-open them from the same
attribution once the aggregate fronts land or stall.

## Acceptance

This ADR is done when the successor can be opened on measured premises — not when anything is faster.

- The handler map exists as a receipt, not a memory of a trace.
- Every dominant aggregate node in the two fronts has inner attribution that closes the full chain —
  inner stages ⊆ handler span ⊆ matched node exclusive span ⊆ `execute_bound` span — through the ADR
  507 contract, on both axes, failing closed on untrustworthy input, with the residual at each link
  explicit.
- Cells with multiple aggregate invocations have each invocation matched to a profile, or the matched
  set summed and shown to close against the node's exclusive span. Unmatched invocations are named as
  uncovered rather than folded into a coverage claim.
- The implementation is sealed and pushed as one commit, and the clean 22/22 sf=8 board is measured
  from that exact commit and landed as a separate perf receipt citing it. No `-dirty` build identity.
- Each front hands the successor its own **ranked premise** — sub-component, measured share, and the
  condition a remedy would have to satisfy. The two premises stand independently; neither inherits the
  other's justification.
- A front whose decomposition shows no reachable premise is recorded as an honest close, as m10 was.

Remedy selection, ablation pairs, and movement claims are the successor ADR's acceptance, not this
one's.

## Guardrails

- **No kernel is authorized by this ADR, and no remedy is selected by it.** It ends with a handler
  map, inner attribution, a clean baseline, and one ranked premise per front; the next ADR spends
  them. If a step reads as "pick the mechanism," the scope has slipped.
- Stage vocabulary is not copied across lifecycle boundaries without an explicit comparability rule.
- No movement claim against the dirty-tree ADR 507 board — that board grounds direction only.
- Encoded columns stay retired unless a decomposition shows scan share that the composed board does
  not currently contain anywhere.

## Risks

- **The fused operator may not decompose cleanly.** A handler that interleaves join and fold per batch
  may have no meaningful join-half/fold-half split. That is a finding: record it and attribute at the
  granularity the lifecycle actually has, rather than inventing a boundary to report against.
- **Instrumenting three handler families is most of the cycle's cost.** Mitigated by step 1 naming the
  minimum set and by reusing q21's existing decomposition rather than rebuilding it.
- **The compute front may already be closed.** If the composed fold cost is the m04 fold the ladder
  already took to its producer boundary, the front's remaining lever is the join half or nothing —
  worth knowing early, and cheaper to learn from the split than from a failed kernel.

## Forward

The cycle produces two independently ranked premises and a clean 22/22 baseline measured from a pushed
commit. The successor ADR opens on those premises and is where a mechanism is chosen, built, ablated,
and priced for movement against that baseline — or where a front is closed as having no reachable cut.
The join/sort front (q18, q02) is the measured successor either way. The sf=10 publication board on the
ADR 356 coordinate remains the last step before any external result, after the composed work lands.
