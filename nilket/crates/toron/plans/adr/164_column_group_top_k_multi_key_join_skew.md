# ADR 164: Column-Group Top-K Facts for Multi-Key Join Skew

**Status:** Promoted. **Operator chose to build the producer (skip the evidence cut) on the whale-tuple
prior; unified one-ADR shape with a provably-dark consumer.** A **single Toron-sequence cross-ring ADR**
(the 156/159 pattern; provenance — serves the Toron CBO, so Toron-home even though the producer code lands
in Wendelon, [[adr-placement-interior-exterior]]): a Wendelon **Column-Group Top-K** producer (extend
156's Space-Saving from per-column to *tuple* keys) + a Toron multi-key-skew consumer (the tuple version
of 162's exact hot intersection, without a cold residual) + activation. The consumer lands **provably
dark** (the 158 discipline, within one ADR) so its math is proven before the producer emits a byte.
Estimate-channel, exact-or-decline, Inner, headline 2-column Int64 lane.

## Why this needs a *new* fact — the boundary 163 named

Multi-key skew lives in the hot *tuple* `(a*,b*)` — `customer 7 in region EU` with 50k orders and 50k
payments → 2.5B join rows for that one tuple. 163's independence spreads it across `Π max(ndv)` and
under-counts; **and the per-column 156 facts cannot recover it** — the marginals `freq(a*)`, `freq(b*)`
only *bound* `freq(a*,b*)` (≤ their min), never pin it (the correlation problem). The only fact that closes
the gap is one keyed on the **tuple itself**: a composite/group top-k. This is the first join-arc need the
cheap reuses (160–163) genuinely cannot serve — where new Wendelon production re-enters.

## The estimate — exact hot tuple floor, not a fake composite residual

At `L ⋈ R on (a,b)`, with group top-k on `{a,b}` both sides:

```
estimate = max( prior_163 , exact_hot_tuple )
  exact_hot_tuple = Σ_{t ∈ H} freq_L(t)·freq_R(t)            H = hot_tuple_L ∩ hot_tuple_R  (EXACT)
```

This deliberately stops short of 162's full hot+cold residual. In the single-key case, subtracting hot
keys from NDV is meaningful because the distinct model is one-dimensional. In 163, the denominator is an
attribute-independence proxy (`Π max(ndv_i)`), not a measured composite tuple-NDV. Subtracting hot tuple
counts from that proxy is fake precision. Top-k frequencies are exact (distinct from 159's KLL), so the
whale tuple is recovered to the row; the cold residual remains 163. `H = ∅` (or no group fact) →
`exact = 0` → **byte-identical to 163** (the pure-superset / darkness invariant).

## Decisions

**D1 — Single cross-ring Toron ADR (156/159 pattern); consumer lands provably dark; unified [final].**
Wendelon owns the group sketch + exact-or-decline merge + wire; Toron owns the tuple-skew decomposition +
activation. The impl ships the consumer first, **fixture-proven dark** (absent group fact → exactly 163),
then the producer activates it — the 158 darkness discipline *within* one ADR. (If the feat sprawls at
implementation, the documented fallback is the literal 158/159 split — dark consumer 164 → producer 165;
the Phase-0-dark structure keeps that split cheap. The plan is unified.)

**D2 — Producer: extend 156's Space-Saving to a structured tuple key.** New
`ColumnGroupTopKSpec { column_names: Vec<String>, max_entries, max_bytes }` on `ColumnStatsPolicyV1`
(brand-clean per 163's reserved guard; no `*V1`). The collector inserts a tuple key as
`Vec<Vec<u8>>`: one full canonical optimizer-fact scalar byte vector per component, paired with the
group's `arrow_type_tags`. **A null in any key component excludes the row** (it cannot equi-join). Tracks
top-k tuples exact-or-decline (mirror `HeavyHitterCollector`/`finish`). Build at seal; CSB encode/decode
on the lowest group ordinal's `ColumnStatsBlockEntry`, with the payload carrying the full group ordinals
and type tags; exact-or-decline tiered merge (mirror `heavy_hitter_fact_for_column:872`); all-contributors
coverage; opt-in budget (lowest priority, like 159). Wire `ColumnGroupTopKFact { column_ordinals,
arrow_type_tags, entries: Vec<ColumnGroupTopKEntry { value_keys, frequency }> }` beside
`column_heavy_hitters`; descriptor sync maps to Toron's grouped carrier.

**D3 — Consumer: generalize 162's `hot_both_join_contribution` to tuples.** A
`clean_base_column_group_top_k_for_join_input` (mirror the clean-base HH reader, binder.rs:4208) that
matches the fact's group to the **full join-key set** of that side after scan-projection remap. If the
fact order differs, normalize tuple components into join-key order before intersection. Intersect L's and
R's tuple entries (by tuple key, iterate the smaller — the 162 shape); compute `exact_hot_tuple`; return
`max(prior_163, exact_hot_tuple)`. Validate before spending: tuple arity == join-key count, each component's
tag ↔ the key `DataType`, freq-sum ≤ base rows, nonzero (the 162/161 fact-validation discipline). Called
from `multi_key_join_cardinality:4311`. Partial and over-grouped facts decline; reorderable full-group
facts are accepted after normalization.

**D4 — Provable darkness (the gate).** No `ColumnGroupTopKFact` for the group ⇒ `H = ∅` ⇒ estimate
**byte-identical to 163** (asserted by exact equality — the 158 darkness proof). The single
behavior-change moment is when the producer emits group facts (its own coverage/budget fences).

**D5 — Exact-or-decline (156), raising-only (162), estimate-channel.** Tuple frequencies are exact (not
KLL-approximate). The estimate can only *rise* above 163 (`max(prior_163, exact_hot_tuple)`) — safe
direction (higher → hash; fixes the under-estimate → broadcast → replay). Sets `estimated_row_count`/
`estimated_byte_size` only; never `row_upper_bound`/`EmptyRelation`. 139/140-backstopped; `adr136`
markers unchanged. The group sketch's collector internals stay Wendelon-private; only schema-owned tuple
entries cross the wire (the 159 KLL-private discipline).

**D6 — Scope: headline 2-column Int64; structured key order generalizes.** Headline is one 2-column group
(`{customer_id, region_id}`), Int64 columns; N-column tuples fold because the key is structured
component-by-component. Wider scalar lanes can follow the existing storage scalar decoder support. The
policy group order defines the wire value-key order; the consumer normalizes to the join-key order (the
L↔R pairing from `bind_join`).

## Headline gate

`live_column_group_top_k_recovers_multi_key_skew_and_hashes_without_seeding`: `orders ⋈ payments` on
`(customer_id, region_id)`, base scans ingested through the **real producer** (opt-in group policy, ≥2
sealed slabs to exercise the merge), no test-injected fact, with a co-hot tuple → 164 estimate at least
`freq·freq` and above the broadcast threshold → chooser **hash, `replayed == false`**. On `main` (163) the uniform
estimate is small → broadcast → over-cap → replay. Paired
`absent_column_group_top_k_falls_back_to_163_byte_identical` (the darkness proof — exact equality to 163).

## Supporting gates

- `hot_tuple_contribution_is_exact_product_of_frequencies`; `consumer_uses_exact_hot_floor_not_fake_residual`;
- `column_group_top_k_merges_across_two_slabs` (exact-or-decline tiered merge, the 159-style 2-slab proof);
- `column_group_top_k_declines_on_partial_coverage` → consumer falls to 163 (darkness composes);
- `multi_key_skew_never_lowers_163_prior` (the 162 raising-only guard, tuple version);
- `column_group_top_k_validates_tuple_arity_and_types`; `group_must_match_full_join_key_set`;
- `column_group_top_k_normalizes_group_order_to_join_key_order`;
- `tuple_entries_are_schema_owned_and_scalar_literal_key_stays_toron_private`;
- `column_group_top_k_spec_names_are_brand_clean`; `group_top_k_csb_payload_is_anchored_not_format_bumped`;
- guards: `adr164_group_top_k_is_estimate_channel_only`, `adr164_consumer_dark_without_producer`,
  `adr164_exact_or_decline_not_approximate`, `adr164_consumer_uses_exact_hot_floor_not_fake_residual`,
  `adr164_group_top_k_sketch_is_wendelon_private`; retarget 163's
  `adr163_no_wendelon_tuple_top_k_surface` → "owned by 164, brand-clean"; `adr136` markers unchanged;
  160/161/162/163 `join_cardinality` + 155/156 + 158/159 regressions green.

## Consensus trail

iter-0: the fork measure-first (A) vs build-now (B) vs breadth, surfaced for the operator. iter-1: I drafted
the evidence cut (A) on discipline. **The operator chose B** — build the Column-Group Top-K producer on the
strong whale-tuple prior, the explicit "prior's strong enough" call I invited. iter-2: the dual — the
156/159 cross-ring pattern, producer (tuple Space-Saving) + consumer (exact hot tuple floor) + activation,
consumer provably dark within the one ADR. Final iteration: **unified** (faithful to the single-topic,
facts-named selection; phased-impl de-risking; 158/159 split documented as the implementation-time
fallback); tuple encoding locked as structured component bytes (not Toron keys), null-component exclusion,
full-group match with order normalization, and no cold residual until composite tuple-NDV exists.

## Forward arc

`163 multi-key independence → **164 column-group top-k (multi-key skew, exact)** → composite-NDV (residual
2: broad correlation) / Theta (residual 3: cold-key value-set overlap) as their misses justify → multi-join
reordering` · *(parallel: CONVERT/units; Rune cockpit — group-skew is a gauge)*. 164 closes the *skew* half
of the multi-key residual exactly; the correlation and cold-overlap halves remain (named, unmeasured —
the residual-evidence cut, if the operator wants it before those).

## Deferred (named)

Exact hot+cold residual gated on a composite tuple-NDV fact; N-column groups beyond the headline 2 (the
tuple key generalizes; confirm budget); broader scalar lanes beyond the headline Int64 fixture;
composite-NDV (residual 2) and Theta key-set overlap (residual 3) — the *other* multi-key residuals 164
doesn't touch; multi-group-per-join; the measured residual-evidence cut (A, if wanted before the further
producers); the Rune cockpit group-skew gauge.
