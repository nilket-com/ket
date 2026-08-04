# ADR 161: Join-Key Range-Overlap Refinement for NDV Containment

**Status:** Promoted. **3-of-3; both iter-2 forks closed on grounding (fraction model derived, type lane
resolved).** A **pure tightening** of 160's containment estimate, reusing the live min/max `RangeFact`s
(158/159) on the join keys: provably-disjoint key ranges → join ≈ empty; partial overlap → scaled down;
contained/identical → unchanged. Zero new substrate, estimate-channel, Toron-local, Inner-only,
single-key. Inherits every 160 discipline; no execution-side change (only sharpens the
`estimated_row_count` that 160's consumer wire already reads).

## Why this is a *tightening*, not a re-estimate — the load-bearing invariant

Containment (`|L⋈R| ≈ |L|·|R| / max(ndv_L, ndv_R)`) assumes every distinct key on the high-NDV side finds
matches. Range facts add one true constraint: **only keys inside the overlap
`[max(min_L,min_R), min(max_L,max_R)]` can possibly join.** Keys outside the overlap are *phantom*
matches containment counted but that cannot exist. So the refinement can only *remove* phantom joins —
**`refined ≤ containment`, always.** That monotonicity is the safety property: range-overlap never inflates
an estimate, so it can only fix over-counts (the direction that causes over-broadcast / mis-size); if
containment *under*-counts, the refinement is a no-op and 139/140 still backstops. We are never *more*
wrong than 160.

The clean win is the **disjoint** case: `orders.region ∈ [1,5]`, `customers.region ∈ [100,200]` → under
the optimizer fact contract, no value is in both ranges. Containment ignored this and estimated
`|L|·|R|/max(ndv)`; the refinement drives it to `0`. Partial overlap is the continuous generalization.
Because 161 stays in the estimate channel, stale or mismapped stats are still backstopped instead of
treated as a hard empty-plan proof.

## The fraction model — `overlap / min(width)`, the grounded safe coarsening

The refinement multiplies the base estimate by an overlap fraction `f`. To choose `f`, derive the exact
two-sided participation estimate under uniform key spread over each side's `[min,max]`
(`oL = overlap_w/width_L`, `oR = overlap_w/width_R`):

```
E' = (|L|·oL)·(|R|·oR) / max(ndv_L·oL, ndv_R·oR)
```

The single geometric fraction **`f = overlap_w / min(width_L, width_R) = max(oL, oR)`** is *exact* at the
cases that matter and safe everywhere:

- **disjoint** → `overlap_w = 0` → `f = 0`; ✓
- **contained** (`L ⊂ R`) → `overlap_w = width_L`, `min(width) = width_L` → `f = 1` → base unchanged,
  correct (every L key is joinable); **this is why `min`, not `max`/union — `max` would wrongly penalize
  the contained case**; ✓
- **symmetric partial** (`width_L = width_R`) → `oL = oR` → exact model `= E·f`; ✓
- **asymmetric partial** → `f` and the exact model diverge, and `f` errs in the *safe* direction
  (under-tightens, still `≤ base`). The exact two-sided model couples range with the `ndv` argmax in a
  way that can flip — fragile to ship for precision the evidence hasn't asked for.

So `f = overlap_w / min(width)` is the robust, `ndv`-independent, monotone choice. The two-sided
participation model is deferred behind a *measured* asymmetric-partial miss.

## Decisions

**D1 — Refine `join_output_ndv_containment_estimate` by the overlap fraction.** After
`ndv_containment_join_rows` yields the base estimate (binder.rs:4170/4192), multiply by `f`; return the
refined estimate. If either key lacks a clean-base `RangeFact`, return the base unchanged (fail-open to
160 behavior).

**D2 — `clean_base_column_range_for_join_input` (mirror the NDV clean-base helper).** Match root
`ExecutionPlan::Scan { projection, column_range, .. }` only (reject `Filter`/`Project`/`Join`/…), remap
projection output ordinal → descriptor ordinal, read the matching `RangeFact` (binder.rs:4140 is the
shape; `Scan.column_range` at ir/execution.rs:242). Do **not** reuse `column_range_for_input` (it recurses
`Filter`, the stale-stats trap 160 already established).

**D3 — Fraction `f = overlap_w / min(width_L, width_R)`, clamped `[0,1]`** (the grounded coarsening
above). `overlap = [max(min_L,min_R), min(max_L,max_R)]`; `min` of the two widths, never `max`/union.

**D4 — Refined estimate: disjoint → `0`; overlapping → `max(1, ⌊base · f⌋)`.** Disjoint (`overlap_w == 0`)
returns exactly `RowCount(0)` — provably empty, mirroring 160's `numerator==0 → 0` (an empty *estimate*,
**not** a hard `EmptyRelation` rewrite). Any non-empty overlap floors at `≥1` (don't claim empty when
ranges touch). `u128` intermediates; monotone `refined ≤ base` by construction (`overlap_w ≤ min_w`).

**D5 — Type lane: Integral (Int32/Int64/Date32/Timestamp); Real fails open.** Validate each range fact's
`arrow_type_tag` against the actual join key `DataType` before decoding. Descriptor range import currently
filters by ordinal, not by type, so the join consumer must defend itself: `Int32`/`Int64`/`Date32` tags
must match the corresponding key type; timestamp tags must match a UTC timestamp key and the same
`TimeUnit`. Then `decode_range_scalar` (binder.rs:4994) maps those supported types to one
`RangeScalar::Integral(i128)` path (`hi - lo + 1` inclusive width). `RangeScalar::Real` fails open to the
base estimate. SQL float join keys are already rejected by `validate_join_key_types` before estimation, so
the Real-lane gate is helper-level, not a SQL join behavior test.

**D6 — Estimate-channel only; all 160 guards inherited.** Fires only on clean `Scan ⋈ Scan`
(`SingleBaseRelation`), Inner, single-key. Refines `estimated_row_count`/`estimated_byte_size` only; never
`row_upper_bound`/`byte_upper_bound`/`EmptyRelation`. 139/140-backstopped; `adr136` markers unchanged.

**D7 — Hard disjoint-join pruning is deferred (named).** Provably-disjoint inner-join keys → the output
is *exactly* empty, which could justify a hard `EmptyRelation` rewrite (prune the join entirely). That
crosses from the estimate channel into the correctness/hard-bound channel with its own 139/140 replay
implications — a separate, bigger cut. 161 stays estimate-channel (`0` estimate, not a prune).

## Headline gate

`disjoint_join_key_ranges_estimate_empty_not_containment_product`: `orders ⋈ customers` on `region`, base
scans, seeded `column_ndv` *and* `column_range` with **disjoint** ranges (`[1,5]` vs `[100,200]`) → refined
`estimated_row_count == Some(0)`; the same plan on plain-160 NDV (no range) estimates `> 0`. Paired
`partial_overlap_join_scales_containment_down` (half-overlap → strictly between `0` and `base`).

## Behavior payoff gates

`range_disjoint_child_join_broadcasts_zero_estimate_without_replay`: a two-join query where ADR 160's
child join containment estimate is too large for the parent build threshold, but ADR 161's disjoint
range estimate is `0`. The parent join spends the child composite `estimated_byte_size` through ADR
160's existing `BroadcastEstimated` path, broadcasts without replay, and matches the forced-hash digest.

`stale_disjoint_join_range_underestimate_replays_as_hash_and_matches_digest`: the same planner facts claim
disjoint ranges, but storage actually contains overlapping keys and a large child join. The underestimated
composite build may choose broadcast; a tight broadcast budget replays as hash and matches forced hash.
This pins that 161 changed only the estimate, not the correctness channel.

## Supporting gates

- `range_overlap_refinement_never_exceeds_containment` (the monotone invariant, fuzzed over seeded ranges);
- `contained_range_scores_full_overlap` (`L ⊂ R` → `f = 1` → base unchanged — `min`-not-`max` proof);
- `integral_lane_covers_date32_and_timestamp_join_keys` (D5 — the lane is integral-broad, not Int64-only);
- `join_range_overlap_fails_open_on_fact_tag_type_mismatch` (D5 — range tag/type mismatch never refines);
- `range_overlap_real_lane_fails_open_in_helper` (D5 — `RangeScalar::Real` returns the base estimate; SQL
  float join keys remain rejected before estimation);
- `join_range_overlap_fails_open_without_range_fact` (missing `column_range` on a key → base, 160 behavior);
- `join_range_overlap_skips_filtered_input` (clean-base discipline — `Filter(Scan) ⋈ Scan` → unrefined);
- `join_range_overlap_is_estimate_channel_not_emptyrelation` (D7 — disjoint → estimate `0`, plan still a
  `Join`, no `EmptyRelation`);
- guards: `adr161_range_overlap_only_tightens_containment`, `adr161_no_emptyrelation_from_disjoint_join`;
  `adr136` markers unchanged; 160 `join_cardinality` + 158/159 + 144/147 regressions green.

## Consensus trail

iter-0: candidate **A**, reframed from the roadmap's "evidence → Theta" — containment's weakness splits
into *disjoint* (a cheap reuse fix, no evidence needed: min/max prove emptiness) and *skew-correlated* (the
half that warrants evidence + heavy-hitter/Theta). iter-1: the dual — a monotone tightening reusing live
min/max, with the `refined ≤ base` invariant as the safety property. iter-2 closed both forks on grounding:
the fraction model `overlap/min(width)` is the *derived* safe coarsening of the exact two-sided
participation estimate (exact at disjoint/contained/symmetric, safe-direction error on asymmetric-partial),
and the type lane is the broad Integral path (Int32/Int64/Date32/Timestamp) with Real failing open. Final
iteration folded both; no open questions.

## Forward arc

`160 containment baseline → **161 range-overlap tightening (disjoint/partial keys)** → heavy-hitter-join
skew refinement (top-k intersection, reuse 155/156) → measured residual miss evidence → Theta/MinHash only
for the residual` · *(parallel: CONVERT/units; Rune cockpit — overlap fraction is a join gauge)*. The cheap
reuses (161 range + heavy-hitter skew) likely shrink Theta to a small residual.
