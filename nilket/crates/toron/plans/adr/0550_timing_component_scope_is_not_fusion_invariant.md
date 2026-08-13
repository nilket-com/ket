# ADR 550 - Timing component scope is not fusion-invariant

**Status:** Accepted 2026-08-12. Corrects ADR 538.

## Context

ADR 538 built a paired local/distributed timing atlas, classified 21 TPC-H cells,
and selected a forward branch by component dominance. The fork returned
**no-work**: no component led enough cells to direct substrate work.

That fork rests on a premise ADR 538 states in its own component table — that
`scan`, `operator` and `shuffle` name the same scope on both arms, so a paired
contribution is a difference in cost rather than a difference in what was
measured. **That premise is false for `scan` and `operator`, and the source says
so.**

At `cad04fc4`, the artifact's exact revision, `distributed/task.rs` documents the
contract on `scan_micros`:

> Written at that single site only: scans fused inside a streaming operator loop
> are already covered by that operator's own phase bracket, and writing them here
> as well would break the task-wall partition.

Confirmed at that revision:

- `scan_micros` has **one** write site — `exec.rs:872`, inside the bare
  `ExecutionPlan::Scan { .. }` arm at 858.
- Fused paths — `try_streaming_scan_global_aggregate`,
  `try_streaming_scan_grouped_aggregate`, and twelve `try_streaming_*` join
  variants — call `storage.scan(req)` directly and never touch it.
- The **aggregate** decomposition verified here is purely structural:
  `decompose_scan_global_aggregate_input` matches
  `Aggregate → [Filter] → [Project] → Scan` on the plan tree and consults no
  statistics. **The join routes are not** — they additionally consult runtime
  estimates (`row_upper_bound`, admission capacity targets, resident budgets), so
  their eligibility is not a pure function of plan shape. Nothing below depends on
  the join routes being structural; what matters is only that a fused read, by
  whatever route, is not written to `scan_micros`.
- `operator` is `build_micros + probe_micros + spill_micros` — join brackets only.

So identical base-relation work reaches **three different components**, selected
by execution route/shape:

| execution route | base-relation read counted as |
|---|---|
| bare `ExecutionPlan::Scan` node | `scan` |
| fused into a join build/probe bracket | `operator` |
| fused into an aggregate loop | **unattributed remainder** |

Fusion is decided per arm, and fragmentation **can** split an aggregate from its
scan, so a read fused on one arm may be a bare `Scan` node on the other. The
component is therefore not guaranteed invariant across the arms it is being
compared across. That conclusion follows from the source mapping alone; it does
not require knowing which route any particular cell took.

**The board is consistent with this.** Five cells — q01, q04, q06, q14, q19 —
pair a **zero median local `scan` counter against a nonzero distributed `scan`** of
447–1767 ms, totalling 4,643 ms, every one marked `pairing: paired`,
`eligible: true`, `additive: true`. Each component row summarizes seven pairs, so a
`median_local_ms` of 0 is a property of the median and does **not** assert that
every pair recorded zero. These cells are reported as observations, not as
diagnosed cases: this record does **not** claim their local reads were counted
under `operator` or in remainder, because the exact plans are unknown (Decision 5).
What the source mapping establishes is that a zero `scan` counter does not mean no
base relation was read.

`scan` also contributes **more than 100% of the median paired delta** in five
cells — q19 361%, q14 168%, q06 160%, q18 157%, q03 127%. This is **not** proof of
mismatched scope: a valid positive component can legitimately exceed a net delta
when another component is negative, which is the case here. It is recorded as a
measure of how much weight the fork places on this boundary.

**The fork is sensitive to exactly this boundary.** Recomputed on ADR 538's own
definitions: |S| = 15, threshold = ⌈2/3 · 15⌉ = 10. Leading counts are `scan` 6,
`operator` 5, `shuffle` 4, `transport` 1 — none reaches 10, so no component leads
and no-work wins, reproducing the original verdict. Merging `scan` and `operator`
across this scope-variant boundary gives **11 ≥ 10**, and the fork flips.

This does not license a combined-component optimization chosen after the numbers
were seen. It does mean the no-work branch cannot be described as evidence-
selected: it was selected by a partition the source contract does not support.
Nor would merging the two components repair the mapping — an aggregate-fused read
is written to neither of them, so no combination of `scan` and `operator` can
recover it. That is a property of the mapping, asserted here of no particular cell.

## Decision

### 0. The board is reproducible from the banked artifact

The evidence receipt is generated, not transcribed. `plans/impl/0550_board.sh`
(bash + jq) reads only `timing_atlas_cad04fc4.jsonl` and emits
`plans/impl/0550_timing_component_scope_is_not_fusion_invariant_evidence.md`;
the exact command is in its docstring. No engine build, dataset or timing rerun is
involved, so every figure in this record can be re-derived from the artifact alone.

**Wall, component and remainder columns are independent medians and must not be
summed row-wise.** Each is a median taken separately over the measured pairs, so a
row is not an arithmetic partition of its own wall figure. Per-child and per-path
reconciliation was enforced by the atlas replay validator at seal time — the
artifact carries `replay_violations: []`, `withheld_reasons: []`,
`timing_authority: true` — and is relied upon here rather than re-derived.

### 1. What remains authoritative

- The **correctness identities** and every result derived from them.
- The **paired elapsed measurements** and their `distinguished` /
  `indistinguishable` classification. Wall-clock pairing measures one arm against
  the other with no component partition and is untouched by this defect.
- The **complete 21-cell timing board** (q15 excluded, recorded as
  `excluded_cell`). Twenty-one cells is the board; any 15-cell presentation is the
  `distinguished`-and-slower subset S, and must be labelled as such rather than
  shown as the board.

### 2. What is withdrawn

- The **component-dominance fork** and the **no-work mechanistic verdict**. The
  arithmetic ran correctly over inputs whose scope premise does not hold.
- **"Three things, cleanly separated."**
- The **scan-led** and **operator-led** narratives, and the generalized scan-led
  mechanism and re-reading claim built on them.

Withdrawing the mechanistic verdict does not assert that substrate work *is*
warranted. It asserts that this artifact cannot decide the question.

### 3. Shuffle receipts survive, but cannot carry the fork

`shuffle` scope is exchange partitioning, publication and consumption. It is not
implicated in scan/operator fusion, and its receipts remain valid component
evidence **where their paired scope holds per cell**. They cannot independently
restore the suite-wide fork: `shuffle` leads 4 of 15 against a threshold of 10,
and a fork cannot be rebuilt from the one component that survived a partition the
rest of it failed.

### 4. No cell is a directly comparable scan observation

**No cell is certified comparable from this artifact.** Near-unity local/
distributed ratios — q10 1.01, q20 0.99, q16 0.98, q05 1.16 — prove similar
values, not equivalent measurement scope. Two arms can agree numerically while
counting different work, and nothing in the artifact distinguishes that from
genuine agreement. Treating a near-1 ratio as evidence of shared scope is the
proxy error this record exists to correct, so q12 is not retained either.

### 5. Exact plan shapes are unknown and are not claimed

Per-cell bare-`Scan` counts and fused-path identities **cannot be reconstructed**.
The sf=8 corpus is no longer on disk, and binding at `cad04fc4` is cost-model
driven — `estimate_row_source`, `filter_equality_ndv_estimated_row_count`,
`filter_range_selectivity_estimated_row_count` — so memory-mode metadata would not
reproduce sf=8 shapes.

**On the corpus identifier:** the atlas header's `dataset_manifest_fingerprint`
carries `fnv1a64:b9b384385fad4af5`, but despite the field name that value is the
per-query **export** manifest, stable across all runs. The full **dataset**
manifest was `fnv1a64:0fc5c06d19c5ed23`. A regeneration attempt must verify against
the latter; matching the header field alone would prove only that the query export
set is unchanged.

This record leaves those shapes **explicitly unknown**. Establishing the contract
violation does not require them: it follows from the single `scan_micros` write
site and the documented fused-read exclusion. No cell here is annotated with an
inferred plan shape, and the five zero-median-local-`scan` cells are cited as paired
counter observations only.

## Acceptance

1. The source contract is quoted from `distributed/task.rs` at `cad04fc4`, with the
   single `scan_micros` write site cited at that revision, the aggregate
   decomposition shown to be structural, and the join routes stated **not** to be.
2. The three destinations for identical base-relation work are stated.
3. The five zero-median-local-`scan` cells are named with values as paired counter
   observations, with no per-cell causal claim attached to any of them.
4. The fork is recomputed on ADR 538's own definitions, reproducing no-work at
   6/5/4/1 against threshold 10, and shown to flip at 11 when the boundary is
   merged.
5. The withdrawals in Decision 2 are stated without asserting the opposite verdict.
6. No cell is certified as a comparable scan observation, and no plan shape is
   claimed.
7. The scope defect is established from the source mapping, and the >100%
   contributions are presented as sensitivity rather than as proof of mismatch.

## Guardrails

- No substrate family is selected, and no scan or combined-component optimization
  is authorized, by this record.
- The merged scan+operator crossing is **sensitivity evidence only**. It must not
  be cited as a dominance result; it was computed after the board was known.
- ADR 538 is not edited. Its arithmetic stands over its stated inputs; this record
  supersedes its component-dominance conclusions.
- No 15-cell presentation may be labelled the board.

## Risks

- **The no-work outcome may well be correct.** Withdrawing it costs a verdict that
  a fusion-invariant rerun could reinstate unchanged. Retaining it on an invalid
  partition would have been worse, because the boundary that produced it is one a
  future measurement is expected to move.
- **The remainder is larger than it looks.** ADR 538 scoped `operator` to joins,
  so non-join execution is unattributed by design and lands in remainder; q01, q04
  and q06 report 100% local remainder. A successor that widens operator scope will
  change many cells without any engine change, and must not read that as a
  regression or an improvement.

## Forward

A future attribution run must measure **base-relation work inside both bare and
fused paths under one fusion-invariant scope**, and demonstrate that scope holds
per arm per cell, before any substrate family is selected. Until then the
component board bounds where cost sits, not what it is.

The instrumentation change is the prerequisite, not the analysis: as long as
`scan_micros` is written at one bare-node site while fused reads land in a join
bracket or in remainder, no reweighting of the existing artifact can recover a
comparable partition.
