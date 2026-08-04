# ADR 165: Column-Group NDV Facts for Multi-Key Join Containment

**Status:** Promoted. **The sibling to 164 (Column-Group Top-K); operator chose to build the second group
producer while 164's machinery is fresh.** A **single Toron-sequence cross-ring ADR** (the 142/156/164
pattern; provenance — serves the CBO, Toron-home despite Wendelon producer code,
[[adr-placement-interior-exterior]]): a Wendelon group-HLL producer (142's `HllRegisters` keyed on 164's
tuple extraction) + a Toron consumer (cap 163's `Π max(ndv)` independence proxy with the real
composite-NDV) + activation, consumer **provably dark** (absent → byte-identical to the current
163/161/164 path). Estimate-channel,
**approximate** (HLL, unlike 164's exact top-k), **raising-only by construction**. Reuses 142 HLL + 164
tuple-key — almost no new substrate.

## Why a new fact — and why it's *raising-only* (the load-bearing safety)

163's multi-key containment uses `denom = Π_k max(ndv_k)` — an **attribute-independence proxy** for the
distinct key-tuples. When keys are correlated (`(order_id, line_no)`, `(country, city)`), the true
`ndv(a,b) ≈ ndv(a) ≪ Π max(ndv)`, so the proxy denominator is too large and 163 **under-estimates** (the
dangerous direction → broadcast → 139/140 replay). The fix is the *measured* composite distinct-count:

> **`denom = min( Π_k max(ndv_k) ,  max(ndv(a,b)_L, ndv(a,b)_R) )`**

The **`min` cap at the independence product is the whole safety story**, and it does double duty:

1. **Raising-only.** Distinct tuples can never exceed the product of marginals, so the real
   `ndv(a,b) ≤ Π max(ndv)` always — composite-NDV can only *shrink* the denominator, which can only *raise*
   the estimate (toward truth, the safe direction → hash).
2. **Neutralizes the approximation.** Unlike 164's *exact* top-k, this is HLL-*approximate* — an
   over-counting HLL could inflate the denominator and *lower* the estimate (dangerous). But
   `min(product, composite)` never exceeds the product, so an over-count degrades to "no worse than 163,"
   never below. **The estimate is monotone ≥ 163** regardless of HLL error.

Independent keys → `composite ≈ product` → cap picks the product → byte-identical to the current path
(also the darkness invariant: absent fact → denominator unchanged).

## Decisions

**D1 — Single cross-ring Toron ADR (142/156/164 pattern); consumer provably dark.** Wendelon owns the
group-HLL producer + register-max merge + wire; Toron owns the denominator refinement + activation. The
impl ships the consumer dark first (absent group-NDV fact → exactly the current post-164 behavior), then
the producer activates it (the 158 discipline within one ADR).

**D2 — Producer: group HLL = 142's `HllRegisters` keyed on 164's tuple encoding [HLL, not exact-or-
decline].** New `ColumnGroupNdvSpec { column_names: Vec<String>, precision: HllPrecision,
exact_max_distinct_values: SmallSetValueCap, exact_max_bytes: SmallSetByteCap }` on the existing stats
policy container (mirror the existing HLL/small-set knobs + 164's group columns; brand-clean, no new
`*V1`). **Extract 164's tuple-key construction** (`GroupTopKCollector`'s `Vec<Vec<u8>>` builder +
null-component exclusion) into a **shared helper** so NDV and Top-K extract tuples identically (no drift); a
`GroupNdvCollector` encodes each complete tuple as canonical length-delimited bytes before inserting it into
an `HllRegisters` (reuse `hll_insert`) and, while caps fit, an exact tuple set. Raw component concatenation
is forbidden (`["ab", "c"]` must not collide with `["a", "bc"]`). HLL chosen over exact-or-decline:
composite key spaces are large (exact-or-decline would mostly decline), and the `min`-cap makes
approximation safe. Build at seal; CSB encode/decode the exact set plus registers; **register-max tiered
merge** (mirror `ndv_fact_for_column:795` / `HllRegisters::merge:63`); all-contributors coverage (else
omit); opt-in budget (lowest priority). Wire `ColumnGroupNdvFact { column_ordinals: Vec<u16>, distinct:
NdvFact }` beside `column_group_top_k` — only the exact/approx count crosses, registers and tuple bytes stay
Wendelon-private (the 159/164 sketch-private discipline). Descriptor sync → Toron grouped carrier.

**D3 — Consumer: refine 163's denominator (capped → raising-only); composes orthogonally with 161.** In
`multi_key_join_cardinality:4311`, after `denom = Π_k max(ndv_k)`: if a `ColumnGroupNdvFact` matches the
join group on **both** sides (set-match + the 164 order normalization, arity validated, point estimate
`> 0`), set `composite = max(group_ndv_L.point(), group_ndv_R.point())` and `denom = denom.min(composite)`; then the existing
`ndv_containment_join_rows_from_denominator`. **The 161 range-overlap fold then applies to `base`
unchanged** — orthogonal axes (composite-NDV corrects the *distinct-count* denominator; range-overlap
scales by the *value-range* overlap fraction), the same shape as 161-on-160 with a better denominator, no
double-tighten. Absent / invalid / `composite == 0` → denominator unchanged → current behavior (fail-open).

**D4 — Provable darkness.** No `ColumnGroupNdvFact` for the group ⇒ denominator unchanged ⇒ **byte-identical
to the current post-164 path** (exact-equality gate). The single behavior-change moment is the producer
emitting group-NDV facts.

**D5 — Approximate (HLL), estimate-channel, raising-only.** HLL-approximate (no exactness claim), but the
`min(product, composite)` cap makes it monotone ≥ 163 regardless of HLL error (the safety property above).
Sets `estimated_row_count`/`estimated_byte_size` only; never hard bound / `EmptyRelation`.
139/140-backstopped; `adr136` markers unchanged. Registers, tuple bytes, and Toron scalar keys remain
private; only `NdvFact` crosses.

**D6 — Scope: containment denominator only; Int64 2-column headline.** 165 refines the *base* containment
denominator; 164's skew floor `max(prior_163, exact_hot_tuple)` benefits automatically (its `prior_163` is
now composite-NDV-refined). Headline 2-column Int64 group; N-column + wider lanes fold (the tuple key
generalizes).

## Headline gate

`correlated_multi_key_join_uses_composite_ndv_and_hashes_without_replay`: `order_items ⋈ shipments` on a
**correlated** `(order_id, line_no)` (`ndv(a,b) ≈ ndv(a) ≪ max(ndv_a)·max(ndv_b)`), base scans ingested
through the **real producer** (opt-in group-NDV policy, ≥2 sealed slabs to exercise register-max merge), no
test-injected fact → composite-NDV shrinks the denominator → estimate rises ≫ 163's product-denominator
under-estimate → chooser **hash, `replayed == false`**. On `main` (163) the inflated denominator → small
estimate → broadcast → over-cap → replay. Paired `absent_column_group_ndv_falls_back_to_164_byte_identical`
(the darkness proof for the current path; a no-range/no-top-k fixture may additionally equal 163).

## Supporting gates

- `composite_ndv_is_capped_at_independence_product_raising_only` (the `min` cap — composite never inflates
  the denominator above the product; estimate monotone ≥ 163, fuzzed incl. an over-counting HLL);
- `independent_keys_composite_ndv_matches_product_byte_identical`;
- `composite_ndv_composes_with_range_overlap_no_double_tighten` (D3 — denom-shrink then 161 fold);
- `column_group_ndv_builds_exact_small_set_when_caps_fit`;
- `column_group_ndv_merges_across_two_slabs` (exact union or register-max tiered merge, the 142/159-style
  2-slab proof);
- `column_group_ndv_declines_on_partial_coverage` → consumer falls to the current path;
- `column_group_ndv_normalizes_group_order` + `…validates_arity_and_types`;
- `column_group_ndv_registers_are_wendelon_private_only_ndv_fact_crosses_wire`;
- `column_group_ndv_wire_omits_arrow_tags_and_toron_scalar_literal_key`; `…_spec_names_are_brand_clean`;
- `group_tuple_key_builder_shared_between_ndv_and_top_k` (D2 — no encoding drift);
- guards: `adr165_group_ndv_is_estimate_channel_only`, `adr165_consumer_dark_without_group_ndv`,
  `adr165_composite_ndv_capped_raising_only`, `adr165_group_ndv_registers_wendelon_private`,
  `adr165_wendelon_wire_does_not_use_toron_scalar_literal_key`,
  `adr165_column_group_ndv_names_are_brand_clean`; `adr136`
  markers unchanged; 160–164 `join_cardinality` + 142 + 155/156 + 158/159 regressions green.

## Consensus trail

iter-0: I had ranked composite-NDV as "premature, gate on evidence (C)"; the operator pulled toward it and
grounding *strengthened* it — it's the sibling to 164 (reuses the group machinery), fixes 163's *named*
correlation risk (the denominator), and unlocks 164's deferred residual. I owned the under-rating. iter-1:
the dual — group HLL + capped denominator refinement, the `min`-cap as the raising-only/approximation-safe
property. iter-2 closed three forks: HLL over exact-or-decline (cap makes it safe; large key spaces);
extract 164's tuple-key builder as a shared helper (no drift); range-overlap composes orthogonally
(denom-shrink then fold). Build-over-measure again (like 164) on the correlation prior — named, accepted
for sibling-efficiency.

## Forward arc

`164 group top-k (skew, exact) + **165 group NDV (correlation, denominator, approximate-capped)** → 164's
exact hot+cold residual (now unlocked — combine skew + correlation, gated on this composite tuple-NDV) →
Theta cold-key overlap (residual 3, new substrate) → multi-join reordering` · *(parallel: CONVERT/units;
Rune cockpit)*. With 164 + 165 the multi-key residual is covered for **skew + correlation**; only cold-key
value-set overlap (Theta) remains — measure before building. The breadth pivot (CONVERT 009 / Rune 010) is
still the standing alternative now that the join CBO's material residuals are addressed.

## Deferred (named)

164's exact hot+cold residual (now unlocked by this composite tuple-NDV — combine 164 skew + 165
correlation residual; the next *consumer* refinement); Theta/MinHash cold-key value-set overlap (residual 3,
genuinely new substrate — measure first); N-column groups beyond 2 + wider scalar lanes (tuple key
generalizes); the measured residual-evidence cut (now that skew+correlation are done, what's left to
measure is just cold-overlap); the Rune cockpit group gauges; **dimensioned CONVERT (009) / Rune cockpit
(010)** as the breadth pivot whenever the join axis is set down.
