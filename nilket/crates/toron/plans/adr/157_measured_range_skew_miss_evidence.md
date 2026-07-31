# ADR 157: Measured Range-Skew Miss Evidence

**Status:** Promoted. **Genuine 3-of-3 (no residual divergence).** A **Toron-local evidence
cut, no behavior change** — applies ADR 154's evidence discipline to *range* selectivity:
prove the current min/max-uniform model can miss, prove whether the miss crosses a real
broadcast decision boundary, and name the future quantile/range-distribution fact contract
without building it. The next estimate-quality frontier after the 154→155→156 equality/IN arc.

## What this measures (and what stays correct)

144/147 estimate range selectivity as **min/max-uniform**: `estimated_rows_for_integral_range`
(`binder.rs:4587`) returns `ceil(non_null_count × overlap_width / domain_width)` over
`RangeFact { min, max, non_null_count }` (real ranges: a clipped continuous fraction).
**min/max is not wrong** — it stays exactly correct for envelope pruning (out-of-range → 0,
constant column). The measured gap is the **uniform interpolation *inside* the envelope** (as
154 measured the NDV-uniform assumption, not NDV). A column whose envelope is stretched by a
few outliers but whose rows cluster in a narrow band makes the in-envelope estimate miss badly.

## Decisions

**D1 — Evidence-generation only, no behavior change.** 144/147 range math, the chooser,
139/140 replay, and Wendelon are **untouched**. Deliverable: a checked-in deterministic
measurement + a named quantile fact contract. No KLL spend, no descriptor fact, no public SQL
EXPLAIN column, no Rune API, no public trace surface.

**D2 — Scope: single-column range/`BETWEEN` over a `RangeFact` base relation; Int64 headline.**
The current consumers — `filter_range_selectivity_estimated_row_count` (`:4215`),
`estimated_rows_for_range` (`:4559`), the Integral arm — over a `PlanOrigin::SingleBaseRelation`
with a seeded `column_range`. **`Int64` is the headline fixture;** Date32/Timestamp/float range
stay 144/147 regressions and future consumer candidates (civil-time / float-bound edges are not
part of the headline proof). Equality/`IN`/NDV/join selectivity/multi-column: out; the join in
D4 exists only to exercise the chooser/replay consequence.

**D3 — Deterministic clustered fixture, direct `LogicalPlan` (3-of-3 numbers).** A build-side
`customers` dim, `amount: Int64`, seeded `RangeFact(min=0, max=10_000, non_null_count=10_000)`;
**actual** = 9_000 rows in `[0,100]`, with the rest outside the filter while stretching the
envelope to 10_000. Filter `amount BETWEEN 0 AND 100`: uniform
`ceil(10_000 × 101 / 10_001) = 101`; actual `9_000` — a
**~89× under-count**, exact and **RNG-free**. **Direct `LogicalPlan` `Filter(Scan)`** (not SQL
`WHERE` — the 143/154 pushdown-fragility trap). Reuse 154's `skew_evidence` fixture + replay-
signal runner.

**D4 — The 154 split (raw miss vs decision-boundary vs absorbed).**
- **(a) raw miss** — bind the filtered scan; `facts.estimated_row_count == 101` vs known
  `9_000`; assert ratio ≥ threshold.
- **(b) decision-boundary** — `fact ⋈ Filter(dim, amount BETWEEN 0 AND 100)`, auto strategy,
  planner threshold accepts the 101-row estimated bytes; runtime broadcast cap sits below the
  actual 9,000-row build bytes → `BroadcastEstimated` → `BudgetExhausted(BroadcastReplication)`
  → replay (`replayed == true`, digest = forced-hash).
- **(c) wrong-but-absorbed** — same miss under default budget completes as broadcast,
  `replayed == false`. The miss matters only at (b); (c) keeps the finding honest — *"wrong by
  Nx but absorbed"* is a legitimate gate-resolving outcome.

**D5 — Future fact is a distribution/quantile, NOT top-k (the ADR-158 contract; inert).**
```rust
/// ADR 157 evidence deliverable; the fact ADR 158 must produce. NOT wired in 157.
struct ColumnRangeDistributionFact {
    column_ordinal: u16, arrow_type_tag: u8, non_null_count: u64,
    family: Kll,                 // params + rank_error_ppm (integer units, 141 Eq-discipline)
    payload: RangeDistributionPayload,   // KLL state | equi-depth bucket boundaries+counts
}
```
Refines **in-envelope interpolation only** — min/max stays the envelope/zero-out fact; the
consumer interpolates within the relevant quantile bucket instead of the whole `[min,max]`.
Numeric/Date32/Timestamp; all-contributors coverage (142/156); estimate-channel only; **not** a
`HeavyHitterFact` top-k (a different consumer contract than 155/156). `KllSketch` is today an
*aggregate state* — the optimizer fact is a different, schema-owned contract. A contract, not a
live fact in 157.

**D6 — UX doctrine (brainstorm 010), as forward framing.** 157 *produces a gauge reading*
("uniform said 101; observed 9_000; crossed the broadcast boundary"). Per brainstorm 010 (one
fact spine, two UX layers): that reading belongs in the **future Rune optimizer cockpit**, NOT
SQL `EXPLAIN` — which is *why* 157 keeps it out of the public surface (it explains the
`no_public_filter_explain_surface` fence, doesn't fight it). 157 builds neither surface; it adds
a fact-spine receipt SQL will later spend automatically and Rune will later expose.

**D7 — Toron-local; fences; retarget; hygiene separate.** No Wendelon/producer/hard-bound;
estimate-channel only. Guards pin "range selectivity is min/max-uniform; no quantile/KLL fact
exists" (the 158 retarget anchor) + the EXPLAIN surface stays closed. New guards land in a
**fresh `tests/architecture/range_skew.rs`**, not the over-ceiling `optimizer.rs`. adr077/adr031
stay a **separate** do-regardless maintenance pass (+ the full-architecture-binary review-checklist
update) — not folded into the evidence cut.

## Headline gates

- `range_skew_filter_records_min_max_uniform_miss` — (a): estimate 101, actual 9_000, ratio pinned.
- `range_skew_miss_triggers_broadcast_replay` — (b): estimated broadcast over tiny cap replays;
  digest = forced-hash.

## Supporting gates

- `range_skew_miss_within_headroom_is_absorbed_no_replay` (D4c);
- `range_skew_measurement_does_not_change_estimated_rows_for_range` (observational, not a fix);
- `min_max_envelope_pruning_still_correct_out_of_range_is_zero` (D2 — min/max not "wrong");
- `range_skew_evidence_uses_range_fact_not_heavy_hitter_or_ndv` (quantile-not-top-k fence);
- architecture: `adr157_range_selectivity_is_min_max_uniform_no_quantile_fact` (158 anchor);
  `adr157_no_live_range_distribution_optimizer_fact_or_producer`;
  `adr157_public_explain_surface_stays_closed`; estimate-channel + replay fences intact;
- regressions: 144/147 range, 154 replay/evidence, 155/156 heavy-hitter.

## Consensus trail

Genuine 3-of-3, no residual divergence. The load-bearing framings converged across iters:
**in-envelope-uniformity not min/max** (the precise locus), **quantile-distribution not top-k**
(a different consumer contract than 155/156), **`KllSketch`-is-aggregate-state-not-fact**, the
exact RNG-free fixture (101 vs 9_000), the 154 three-state split, and hygiene-separate. Brainstorm
010's SQL-auto/Rune-cockpit UX framing folded 3-of-3 as forward doctrine (D6) — 157 produces a
gauge but stays private.

## Forward arc

`156 heavy-hitter producer (live) → 157 range-skew evidence → (if (b) fires) 158 KLL/range-
distribution producer+consumer [builds to D5 contract] → Theta join-key intersection → join
selectivity → reordering` · *(parallel: Linear-Dimension CONVERT; the future Rune optimizer
cockpit over the same fact spine, brainstorm 010)*. ADR 158 gated on the 157 feat's measured
outcome — (b)-fires → producer; absorbed → reprioritize.
