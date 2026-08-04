# ADR 162: Heavy-Hitter Join-Skew Refinement

**Status:** Promoted. **3-of-3; all three iter-2 forks closed on grounding.** Takes the **skew** half 161
left: at `L.x = R.y`, recover the *exact* contribution of keys hot on both sides from 156's
already-produced top-k, and leave the cold keys to 160/161 containment. A **pure superset of 161**
(byte-identical when no shared hot keys, and never below the 160/161 prior when co-hot facts are present).
Toron-home (consumer reuse, no new Wendelon production — provenance test,
[[adr-placement-interior-exterior]]). Estimate-channel, Inner, single-key, clean base scans. No
execution-side change (sharpens the `estimated_row_count` 160's wire already reads).

## The decomposition — exact hot-both + cold residual (no double-count)

The naive `containment(all) + hot_key_intersection` double-counts the hot keys. The correct split
partitions keys by whether they're hot on **both** sides (`H = hot_L ∩ hot_R`, by key):

```
estimate = exact_hot_both + residual_containment
  exact_hot_both = Σ_{v ∈ H} freq_L(v) · freq_R(v)                       ← EXACT (156 exact-or-decline)
  residual_containment = containment_161(
      rows_L = |L| − Σ_{v∈H} freq_L(v),   rows_R = |R| − Σ_{v∈H} freq_R(v),
      ndv_L  = ndv_L − |H|,               ndv_R  = ndv_R − |H| )          ← cold keys only
```

**No double-count, by construction:** the `|H|` hot-both keys and their rows are *removed* from the
residual inputs before containment, and accounted for exactly in `exact_hot_both`. Frequencies are exact
because 156's producer emits a key only when it counted it exactly across all contributors. A key absent
from either Toron top-k map is **untracked, not zero** for this consumer, so it remains in the cold
residual; only keys present in both maps are spent exactly. For `v` in both top-k lists,
`freq_L(v)·freq_R(v)` is the exact join row count for that value.

**Why exact-hot-both is the strong part:** a value hot on both sides is the cross-table whale — `customer
7` with 50k orders and 50k payments produces 2.5B join rows for that one key. Uniform containment spreads
that mass across `max(ndv)` distinct values and *badly under-counts* it (154's hot-key miss, now across
two tables). The intersection recovers it to the row.

## Why this *sharpens*, not just tightens — direction honesty (distinct from 161)

161 was a pure tightening (`refined ≤ base`). **162 can move the estimate *up*** — and that is the point.
Co-hot correlation produces *more* join rows than uniform assumes, so recovering it raises the estimate
toward truth. This is the safe direction: a higher estimate → the chooser picks **hash** (no broadcast);
the *dangerous* error is the under-estimate (→ broadcast → over-cap → 139/140 replay tax), which is
exactly what 162 fixes. An over-correction (estimate too high) costs a missed broadcast opportunity, never
correctness. Estimate-channel, 139/140-backstopped throughout.

The returned estimate is still guarded by the already-computed 160/161 prior:

```
candidate_162 = exact_hot_both + residual_containment
final         = max(prior_160_161, candidate_162)
```

This is **not** the rejected `max(containment, hot_both)` floor. That floor drops the cold residual and can
under-count. The accepted shape ships the full hot+cold decomposition, then refuses to let a malformed or
pathological residual make 162 worse than the baseline estimate it wraps.

## The composition invariant — a pure superset of 161

When `H = ∅` (anti-correlated top-k, or no heavy-hitter facts), `exact_hot_both = 0` and the residual
inputs are unreduced ⇒ `residual == the exact 161 estimate` ⇒ `total == 161`. **Byte-identical to 161.**
When `H != ∅`, 162 returns `max(prior_160_161, exact_hot_both + residual)`; it can recover co-hot skew
without lowering the 160/161 answer. Disjoint ranges ⇒ no shared hot key possible ⇒ `H = ∅` ⇒ residual
range-overlap returns `0` ⇒ estimate `0` (composes with 161's disjoint win).

## Decisions

**D1 — Decompose in the join estimate seam.** Wrap `join_output_ndv_containment_estimate` (the 161-refined
helper): compute `exact_hot_both`, run the existing containment+range path on the cold residual, sum.
`H = ∅` ⇒ residual is the full estimate ⇒ 161 behavior unchanged.

**D2 — `clean_base_column_heavy_hitter_for_join_input` (mirror the NDV/range clean-base helpers).** Match
root `ExecutionPlan::Scan { projection, column_heavy_hitters, .. }` only, remap projection ordinal →
descriptor ordinal, read the `ColumnHeavyHitterFact` (catalog.rs:137). Do **not** reuse
`column_heavy_hitter_for_input:4340` (it recurses `Filter` — the stale-stats trap 160 established).

**D3 — Exact hot-both intersection.** `H = {v : v ∈ left.entries ∧ v ∈ right.entries}` (BTreeMap
intersection by `ScalarLiteralKey`). Before spending a fact, validate that every key variant matches the
join-key `DataType`, all frequencies are nonzero, and per-side emitted frequency totals do not exceed the
chosen base row estimate; invalid facts fail open to the 160/161 prior. `exact_hot_both = Σ_{v∈H}
freq_L(v)·freq_R(v)`; accumulate `|H|`, `Σ freq_L`, `Σ freq_R` alongside. **Checked `u128` math**;
overflow → fail-open to plain 161.

**D4 — Cold residual = the two existing helpers sequenced on reduced inputs.** `rows_L − Σfreq_L`,
`rows_R − Σfreq_R` (saturating ≥0); `ndv_L − |H|`, `ndv_R − |H|` (saturating). If either residual row side
is `0` → residual `0`. If rows remain but either residual NDV is `0`, decline the 162 candidate and keep
the 160/161 prior rather than fabricating a zero-tail estimate from inconsistent facts. Else
`range_refined_containment_join_rows(ndv_containment_join_rows(reduced rows/ndv)?, ranges)` — **the
residual re-applies 161 range-overlap** (cold keys are still range-constrained; hot-both keys are always
in-overlap so removing them doesn't move the residual's region; min/max looseness on a hot extreme is
second-order, estimate-channel). **No new `containment_then_range` helper** — sequence
`ndv_containment_join_rows:4203` then `range_refined_containment_join_rows:4266`.

If the 161 range-overlap helper proves a zero-width overlap (`overlap_width == 0`), return the 160/161
prior immediately. A shared hot-key sidecar contradicting a disjoint range fact is range-inconsistent
evidence, not permission for 162 to undo the disjoint-range zero.

**D5 — Estimate-channel; sharpen-not-bound; full decomposition, baseline-guarded.** Sets
`estimated_row_count`/`estimated_byte_size` only; never `row_upper_bound`/`byte_upper_bound`/
`EmptyRelation`. Build the **sum** `exact_hot_both + residual`, never the `max(containment, hot_both)`
floor — the floor under-counts the cold residual, re-introducing the under-estimate 162 exists to fix.
Return `max(prior_160_161, sum)` so this producer cannot lower the range-refined containment estimate.
139/140-backstopped; `adr136` markers unchanged.

**D6 — All 160/161 disciplines inherited; anti-correlated hot keys to the residual.** Clean `Scan ⋈ Scan`
(`SingleBaseRelation`), Inner, single-key. Fail-open: missing heavy-hitter fact on either key → `H = ∅` →
161 behavior. A key hot on only one side stays in the residual (containment-estimated) — its other-side
frequency is known only to be sub-threshold; a `freq_L·R_threshold` bound is named but deferred (one-sided
over-state risk, only worth building if a measured miss shows anti-correlated skew matters).

## Headline gate

`co_hot_join_recovers_skew_and_hashes_without_replay`: `orders ⋈ payments` on `customer_id`, base scans,
seeded `column_ndv` *and* `column_heavy_hitters` with a shared whale key (`freq_L = freq_R = large`) → 162
estimate ≈ `freq_L·freq_R + cold residual`, far above uniform containment → chooser picks **hash,
`replayed == false`**. On `main` (160/161) the uniform estimate is small → broadcasts → over-cap → replay.
162 turns a replay into a correct first-pass hash. Paired
`no_shared_hot_keys_falls_back_to_161_containment` (anti-correlated top-k → `H = ∅` → byte-identical to
161 — the pure-superset invariant).

## Supporting gates

- `hot_both_contribution_is_exact_product_of_frequencies` (`Σ freq_L·freq_R` on seeded co-hot keys);
- `residual_excludes_hot_both_rows_and_keys_no_double_count` (total = exact + residual; hot-both removed;
  residual passes through range-overlap — a partial-overlap + co-hot fixture → `exact + scaled_residual`);
- `disjoint_ranges_force_empty_hot_set_composes_with_161` (disjoint ⇒ `H=∅` ⇒ estimate `0`);
- `disjoint_range_zero_dominates_stale_shared_hot_key` (range overlap `0` → keep 161's `0` even if a
  seeded HH fixture says the same key is hot on both sides);
- `join_skew_never_lowers_161_prior` (co-hot facts whose reduced residual is lower than 161 still return
  the prior, not the smaller candidate);
- `join_skew_fails_open_on_key_type_mismatch_or_invalid_frequency` (wrong `ScalarLiteralKey` variant,
  zero frequency, over-base emitted total → 160/161 prior);
- `stale_heavy_hitter_overestimate_stays_estimate_channel_replays_or_hashes` (wrong/stale fact → higher
  estimate → hash, or if it caused a broadcast, replays byte-identical — the backstop);
- `join_skew_fails_open_without_heavy_hitter_fact`; `join_skew_skips_filtered_input` (clean-base);
- guards: `adr162_join_skew_is_estimate_channel_only`, `adr162_hot_both_removed_from_residual`,
  `adr162_requires_clean_base_heavy_hitter`, `adr162_join_skew_keeps_160_161_prior_floor`; `adr136`
  markers unchanged; 160/161 `join_cardinality` + 155/156 + 158/159 + 144/147 regressions green.

## Consensus trail

iter-0: candidate **A** (the skew half 161 named), Toron-home by provenance (consumes 156's existing
top-k; no new Wendelon production). iter-1: the dual — the `exact_hot_both + cold_residual` decomposition,
no double-count (hot-both removed from the residual), direction-honest (sharpens *up* to recover skew,
unlike 161's tightening), pure-superset-of-161 invariant. iter-2 closed three forks on grounding: the
residual **re-applies** 161 range-overlap by sequencing the two existing helpers on reduced inputs (no new
`containment_then_range` helper — verified `ndv_containment_join_rows:4203`/
`range_refined_containment_join_rows:4266` take explicit inputs); the **full sum** ships, not the hot-only
floor (which under-counts the cold residual), with a final `max(prior_160_161, sum)` guard; anti-correlated
hot keys stay in the residual (the one-sided bound deferred). Final iteration folded all; no open
questions.

## Forward arc

`160 containment → 161 range-overlap (disjoint) → **162 heavy-hitter skew (co-hot)** → measured *residual*
miss evidence → Theta/MinHash only for the residual → multi-join reordering` · *(parallel: CONVERT/units;
Rune cockpit — join skew is a gauge)*. After 162 the cheap reuses (NDV → range → heavy-hitter) are
exhausted; the residual evidence cut (163) measures what the three still miss — likely small, possibly
closing Theta out entirely.
