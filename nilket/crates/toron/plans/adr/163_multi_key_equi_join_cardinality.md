# ADR 163: Multi-Key Equi-Join Cardinality

**Status:** Promoted. **3-of-3; both iter-2 forks closed on grounding (fold correctness verified,
correlation deferred to evidence).** Fills the verified total gap: composite-key joins (`L.a=R.a AND
L.b=R.b`) get **no** estimate today (the seam matches a single-element slice, binder.rs:1479, else `None`
→ `composite_unknown`). Generalize the estimator to all key pairs under attribute independence, reusing
160 containment + 161 range-overlap **per key**. Single-key stays byte-identical to 162; heavy-hitter
(162) and correlation are deferred. Toron-local, estimate-channel, Inner, clean base scans. No
execution-side change.

## The estimate — independence over key pairs

A row joins only if it matches on **every** key. Under attribute independence the distinct key-*tuples*
factor (`ndv(a,b) ≈ ndv(a)·ndv(b)`), so the containment denominator becomes the **product** of per-key
denominators:

> **`|L ⋈ R| ≈ |L|·|R| / Π_k max(ndv(L.k), ndv(R.k))`**

Each key independently filters: two keys each with `max(ndv)=100` give a denominator of `10_000`, a join
100× more selective than either key alone. It's the textbook multi-column join estimate — the direct
generalization of 160's single-key containment (the `k=1` case).

**Range-overlap composes per key (conjunctively).** A row must match on *all* keys, and each match needs
the value in that key's overlap — so the per-key 161 fractions **multiply**: `refined = base · Π_k f_k`.
And the strong win: **any single disjoint key proves the whole join empty** (a row can't match a key whose
ranges don't overlap), so `f_k = 0` on *any* key drives the estimate to `0`. Multi-key disjoint detection
is *more* powerful than single-key — more keys = more chances to prove emptiness.

## Why it's the cheap completion — and the new risk it carries

It reuses every 160/161 helper *per key* (no new substrate); the only new code is the product denominator
and the per-key fold. But multi-key introduces a failure mode the single-key cuts didn't have:

**Attribute-independence over-states the denominator when keys are correlated.** If `(country,
country_code)` are functionally dependent, `ndv(country,country_code) ≈ ndv(country)`, not the product —
so `Π max(ndv)` is too big, the denominator too large, and the join **under-estimated** (the dangerous
direction: under-estimate → broadcast → 139/140 replay). This is the multi-key analog of 160's single-key
containment assumption, and the *future* refinement target (composite-NDV / correlation, the way 162
refined single-key skew). 163 ships the independent baseline — estimate-channel, 139/140-backstopped —
and **names** the correlation miss for the residual evidence (B) to measure before any correction is built.

## Decisions

**D1 — Generalize the seam to key slices; single-key path unchanged.** At binder.rs:1479, match the full
`(left_keys, right_keys)` slices (`bind_join` guarantees equal length, 1451–1461): `len == 1` → the
existing `join_output_ndv_containment_estimate` (160/161/162, byte-identical); `len ≥ 2` → new
`multi_key_join_cardinality`. `len == 0` impossible (equi-join has ≥1 key).

**D2 — Product denominator; all key pairs must have clean-base NDV (else fail-open).** `denom = Π_k
max(ndv_Lk, ndv_Rk)` via `clean_base_column_ndv_for_join_input` per key (root-`Scan`, projection-remapped
— the 160 discipline). If *any* key lacks NDV or `ndv == 0` → `None` (fail-open to `composite_unknown`;
fires only on full information, like 160). Factor `ndv_containment_join_rows_from_denominator(rows_L,
rows_R, denom: u128)`; the single-key `_from_points` delegates with `denom = max(ndv_L, ndv_R)` (no
behavior change). Use checked/capped `u128`: once the running denominator reaches the positive numerator,
cap it there because any larger denominator still yields the floor-1 estimate. Overflow is treated as the
same cap, never wraparound.

**D3 — Per-key range-overlap fold (reuse 161); disjoint-zero propagates [iter-2 verified].** Fold
`range_refined_containment_join_rows` over the key pairs: `refined ← range_refine(refined, range_Lk,
type_Lk, range_Rk, type_Rk)` for each key with clean-base ranges on both sides; a key lacking ranges
contributes fraction `1` (skipped). A malformed, unsupported, or tag-mismatched range pair also keeps the
current estimate for that key, matching 161's fail-open behavior. **Any disjoint key ⇒ `0`, and it stays
`0`** through later overlapping keys — verified: 161's floor `if refined == 0 && base.get() > 0 { 1 }`
only bumps a *positive* base that rounded to 0 (a sliver), so it never revives a disjoint-proven-empty join
(`base == 0` ⇒ floor skipped).
And `_from_denominator`'s `((num−1)/denom)+1` keeps `base ≥ 1` for any non-empty input regardless of how
large `Π max(ndv)` grows — so only a *truly disjoint* key yields `0`; every non-disjoint multi-key join is
`≥1`. Keep the per-key fold (it also inherits 161's tag/type validation); per-step rounding is conservative
(safe direction), no product-fraction primitive.

**D4 — Heavy-hitter (162) stays single-key; multi-key skew needs a new producer (named).** Multi-key skew
lives in the hot *tuple* `(a,b)` — the whale *combination* — which **cannot** be reconstructed from 156's
per-column marginals (that is exactly the correlation problem). So multi-key ships containment+range only.
Recovering multi-key skew needs a genuinely **new Wendelon-produced fact: a composite/tuple heavy-hitter
top-k** (a Space-Saving sketch keyed on the multi-column tuple — the direct extension of 156 from
per-column to composite). See *Deferred*.

**D5 — Estimate-channel; independence baseline.** Sets `estimated_row_count`/`estimated_byte_size` only;
never `row_upper_bound`/`byte_upper_bound`/`EmptyRelation` (disjoint → estimate `0`, not a prune — the
161-D7 discipline). 139/140-backstopped; `adr136` markers unchanged. May under-state on correlated keys
(named risk) — recoverable via replay.

**D6 — All 160/161 disciplines inherited.** Clean `Scan ⋈ Scan` (`SingleBaseRelation` both sides), Inner.
Checked math throughout. No execution-side change — sharpens the `estimated_row_count` 160's wire reads.

## Headline gate

`multi_key_join_estimates_from_product_denominator_not_composite_unknown`: `order_items ⋈ line_prices` on
`(order_id, line_no)`, base scans, seeded per-key `column_ndv` → 162-shape facts give **no** estimate on
`main` (`composite_unknown`); 163 estimates `|L|·|R| / (max(ndv_order)·max(ndv_line))` → a real
`estimated_row_count` that flips the strategy decision from the no-estimate default. Paired
`multi_key_disjoint_on_any_key_estimates_empty` (one key's ranges disjoint → estimate `0`, even though the
other key overlaps).

## Supporting gates

- `multi_key_denominator_is_product_of_per_key_max_ndv` (exact `|L|·|R|/Π max(ndv)` on seeded NDV);
- `multi_key_range_overlap_folds_per_key` (two partially-overlapping keys → `base · f_1 · f_2`);
- `multi_key_range_malformed_key_keeps_multi_key_ndv_estimate` (bad range metadata skips that key, rather
  than declining the whole multi-key estimate);
- `single_key_join_unchanged_byte_identical_to_162` (the `len==1` path is exactly 162);
- `multi_key_three_key_join_does_not_spuriously_zero` + its converse (3-key non-disjoint → `≥1`; one
  disjoint key among three → exactly `0` — pins D3's guard);
- `multi_key_fails_open_when_any_key_lacks_ndv`; `multi_key_does_not_apply_heavy_hitter`;
- `multi_key_is_estimate_channel_not_emptyrelation`;
- guards: `adr163_multi_key_denominator_is_product`, `adr163_multi_key_defers_heavy_hitter`,
  `adr163_multi_key_requires_clean_base_ndv`, `adr163_no_wendelon_tuple_top_k_surface`; `adr136` markers
  unchanged; 160/161/162 `join_cardinality` + 155/156 + 158/159 regressions green.

## Consensus trail

iter-0: candidate **A**, reframed from "evidence → Theta" on a *verified total gap* (composite-key joins
get zero CBO — the single-element slice at 1479). iter-1: the dual — product-denominator independence
estimate, per-key range-overlap fold, single-key byte-identical, HH + correlation deferred, the
correlation-under-estimate risk named. iter-2 closed both forks on grounding: the per-key fold is correct
(disjoint-`0` propagates via 161's `base > 0`-guarded floor; `_from_denominator`'s `+1` prevents
huge-denominator spurious zero — verified, not assumed), and correlation is deferred to the residual
evidence (B). The operator identified the sharp boundary: **multi-key skew is the first join-arc need that
requires new Wendelon production** (a composite top-k), folded into *Deferred*. Final iteration closed; no
open questions.

## Forward arc

`162 single-key skew → **163 multi-key containment (independence baseline)** → measured residual miss
evidence (single + multi key, + correlation) → composite/tuple top-k + Theta only for the residual →
multi-join reordering` · *(parallel: CONVERT/units; Rune cockpit)*. 163 completes the join estimator to the
full key surface; the residual evidence then measures the *complete* estimator (containment + range + skew
+ multi-key independence) — the honest gate before any new producer.

## Deferred (named)

- **Wendelon composite/tuple heavy-hitter top-k producer + multi-key-skew consumer** — the first join-arc
  need that requires *new Wendelon production* (the cheap reuses 160–163 are exhausted). A Space-Saving
  sketch keyed on the multi-column tuple `(a,b,…)`, extending 156 from per-column to composite, with the
  same exact-or-decline merge. Placement: **Toron-sequence cross-ring producer (the 156/159 pattern)** —
  Wendelon owns the maintained composite sketch, Toron consumes it in a multi-key-skew refinement (the
  162-analog for tuples). Use brand-clean Wendelon names such as `ColumnGroupTopKSpec`,
  `ColumnGroupTopKFact`, and `ColumnGroupTopKEntry`; do not add new `*V1` suffixes. This is the
  *concrete* lead beyond the cheap reuses — a proven build pattern, more so than the vague "Theta."
  Theta/MinHash remains for the **cold-key overlap** residual (a different, new substrate); the two are
  complementary halves.
- Correlation / composite-NDV refinement for the independence baseline (measure in the residual evidence
  first); multi-key over `Filter`-ed inputs (filtered-NDV decay); the Rune cockpit multi-key gauge.
