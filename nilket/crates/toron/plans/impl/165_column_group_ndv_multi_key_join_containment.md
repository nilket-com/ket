# Impl Plan — ADR 165: Column-Group NDV Facts for Multi-Key Join Containment

Pairs with `plans/adr/165_column_group_ndv_multi_key_join_containment.md`. The sibling to 164: a Wendelon
group-HLL producer (142's `HllRegisters` keyed on 164's tuple extraction) + a Toron consumer refining 163's
`Π max(ndv)` containment denominator with the real composite-NDV, **capped at the product → raising-only**.
Consumer provably dark. Reuses 142 HLL + 164 tuple-key. Anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| HLL machinery (reuse) | `HllRegisters` — `wendelon/src/storage/sketch.rs:11`; `merge:63` (register-max), `estimate:76`, `from_registers:25`, `hll_insert` |
| Tuple-key builder (extract & share from 164) | `GroupTopKCollector` — `wendelon/src/storage/stats.rs:2636` (`Vec<Vec<u8>>` tuple key, null-component exclusion) |
| 142 NDV tiered merge (mirror) | `ndv_fact_for_column:795` (register-max; decline if any contributor lacks); coverage `has_uncovered_local_rows_for_descriptor` |
| 142 policy (mirror) | `ColumnHllSpecV1` — `schema/table.rs:1373`; `hll_distincts:1337`; `validate_hll_stats_policy:2765` |
| Wire (add beside 164's) | `column_group_top_k` — `schema/optimizer_facts.rs` |
| Denominator seam (refine) | `multi_key_join_cardinality:4311`; `multiply_denominator_capped_at_numerator` (builds `Π max(ndv)`); `ndv_containment_join_rows_from_denominator` |
| Consumer reader (mirror 164) | `clean_base_column_group_top_k_for_join_input` (group set-match + order normalization) |
| Live-producer headline harness | `…produced_storage_…_fact` fixture + `force_seal_active_for_test`; replay-signal runner |
| Guards | `crates/toron/tests/architecture/join_cardinality.rs` (adr165_*) + Wendelon `tests/` group-ndv guards |

## Phase 0 — Consumer-dark first

Land D3 reading a `ColumnGroupNdvFact` the producer doesn't yet emit (descriptor sync empty for group-NDV).
Fixture-seed a group-NDV fact → prove the capped denominator refinement; **darkness gate**
`absent_column_group_ndv_falls_back_to_164_byte_identical` (exact equality to the current post-164 path when
absent; a no-range/no-top-k fixture may additionally equal 163).

## Phase 1 — Consumer: capped denominator refinement (Toron)

- `clean_base_column_group_ndv_for_join_input(input, group: &[ColumnOrdinal]) -> Option<DistinctCountFact>`:
  copy 164's group-reader shape — root-`Scan`, projection-remap, full-group set-match (NDV count is
  order-independent, but validate the group matches the join key set).
- In `multi_key_join_cardinality:4311`, after the existing `denom = Π max(ndv_k)`:
  ```
  if let (Some(l), Some(r)) = (clean_base_group_ndv(left, &lk_ords), clean_base_group_ndv(right, &rk_ords)) {
      let composite = l.point_estimate().max(r.point_estimate());
      if composite > 0 {
          denom = denom.min(u128::from(composite));      // CAP at the product → raising-only (D5)
      }
  }
  let base = ndv_containment_join_rows_from_denominator(left_rows, right_rows, denom)?;
  // 161 range-overlap fold on `base` UNCHANGED below (orthogonal, no double-tighten — D3)
  ```
  `denom.min(composite)` ⇒ composite can only shrink the denominator; never above the product (HLL
  over-count neutralized). Absent / invalid / `composite == 0` → denominator unchanged → current behavior.

## Phase 2 — Producer: group HLL (Wendelon, reuse 142 + 164)

- `ColumnGroupNdvSpec { column_names: Vec<String>, precision: HllPrecision, exact_max_distinct_values:
  SmallSetValueCap, exact_max_bytes: SmallSetByteCap }` on the existing stats policy container (mirror the
  existing HLL/small-set knobs, `#[serde(default)]`, brand-clean); extend stats-policy validation for groups.
- **Extract** 164's tuple-key construction (`GroupTopKCollector`'s `Vec<Vec<u8>>` builder + null-component
  exclusion) into a **shared helper**; `GroupNdvCollector` uses it, then encodes each complete tuple as
  canonical length-delimited bytes before inserting it into an `HllRegisters` (`hll_insert`) and, while caps
  fit, an exact tuple set. Raw concatenation is forbidden (`["ab", "c"]` vs `["a", "bc"]`). `finish` →
  exact tuple bytes plus registers. CSB encode/decode the exact set plus registers; opt-in budget (lowest
  priority).
- `column_group_ndv_fact_for_group` (mirror `ndv_fact_for_column:795`): register-max merge across
  contributors (`HllRegisters::merge`), exact-set union while every contributor is exact and caps still fit,
  decline if any lacks the group; all-contributors coverage; exact union → `NdvFact::Exact`, otherwise
  `estimate()` + `rel_err_ppm` → `NdvFact::Approx`. Wire into `table_optimizer_facts`.

## Phase 3 — Wire + descriptor sync + activation

- `ColumnGroupNdvFact { column_ordinals: Vec<u16>, distinct: NdvFact }` on the
  `TABLE_OPTIMIZER_FACTS_GET` response (beside `column_group_top_k`). **Registers, tuple bytes, Arrow tags,
  and Toron scalar keys stay private** — only the exact/approx NDV fact crosses. Arrow tags remain in the CSB
  payload for producer-side contributor validation; the final optimizer-fact wire mirrors single-column NDV
  because no typed values cross.
- Descriptor sync: storage group-NDV → Toron grouped carrier (validate ordinals/arity/nonzero point
  estimate; drop malformed).
  **Flip the Phase-0 dark emit** → live mapping — the single behavior-change moment.

## Phase 4 — Fixtures + gates

- Headline `correlated_multi_key_join_uses_composite_ndv_and_hashes_without_replay` (live producer, ≥2
  slabs, correlated `(order_id, line_no)` → composite-NDV shrinks denom → estimate rises → hash, no replay;
  163 on `main` → under-estimate → broadcast → replay).
- `absent_column_group_ndv_falls_back_to_164_byte_identical` (darkness, Phase 0);
- `composite_ndv_is_capped_at_independence_product_raising_only` (fuzz incl. an HLL over-count → denom never
  exceeds product → estimate ≥ 163);
- `independent_keys_composite_ndv_matches_product_byte_identical`;
- `composite_ndv_composes_with_range_overlap_no_double_tighten` (denom-shrink then 161 fold);
- `column_group_ndv_builds_exact_small_set_when_caps_fit`;
- `column_group_ndv_merges_across_two_slabs`; `column_group_ndv_declines_on_partial_coverage`;
- `column_group_ndv_normalizes_group_order`; `column_group_ndv_validates_arity_and_types`;
- `column_group_ndv_registers_are_wendelon_private_only_ndv_fact_crosses_wire`;
- `column_group_ndv_wire_omits_arrow_tags_and_toron_scalar_literal_key`; `…_spec_names_are_brand_clean`;
- `group_tuple_key_builder_shared_between_ndv_and_top_k`;
- keep 160–164 `join_cardinality` + 142 + 155/156 + 158/159 + 144/147 regressions green.

## Phase 5 — Guards

- `adr165_group_ndv_is_estimate_channel_only`; `adr165_consumer_dark_without_group_ndv` (darkness pin);
  `adr165_composite_ndv_capped_raising_only` (the `min(product, composite)` cap — source or behavioral);
  `adr165_group_ndv_registers_wendelon_private` (only `NdvFact` on the wire);
  `adr165_wendelon_wire_does_not_use_toron_scalar_literal_key`;
  `adr165_column_group_ndv_names_are_brand_clean`;
- `adr136` markers unchanged. Producer guards in `crates/wendelon/tests/`.

## Verification

```sh
scripts/cargo-cgroup check -p wendelon --all-targets       # new spec/fact/wire field ⇒ E0063 sweep
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron absent_column_group_ndv_falls_back_to_164_byte_identical
scripts/cargo-cgroup test -p toron correlated_multi_key_join_uses_composite_ndv
scripts/cargo-cgroup test -p toron composite_ndv_is_capped_at_independence_product
scripts/cargo-cgroup test -p wendelon column_group_ndv
scripts/cargo-cgroup test -p toron join_cardinality
scripts/cargo-cgroup test -p toron  --test architecture_test            # FULL binary (expect 0 failed)
git diff --check
```

## Risks

- **Raising-only violated** — the `min(product, composite)` cap is the whole safety story; an un-capped
  composite + HLL over-count → inflated denominator → under-estimate → replay. Pinned by
  `composite_ndv_is_capped_at_independence_product_raising_only` (fuzzed with an over-counting fact).
- **Darkness** — absent group-NDV must be exactly the current post-164 behavior (denominator unchanged);
  byte-identical gate.
- **Range-overlap composition** — denominator shrink composes orthogonally with the 161 fold on `base`; no
  double-tighten. Pinned by `composite_ndv_composes_with_range_overlap_no_double_tighten`.
- **Tuple-key drift from 164** — the shared builder must be the *single* source for both NDV and Top-K
  encoding (a divergence makes group facts inconsistent). Pinned by `group_tuple_key_builder_shared_…`.
- **E0063 breadth** — new policy + wire field hits Wendelon initializers; `--all-targets` both crates.

## Deferred (named)

164's exact hot+cold residual now unlocked (combine 164 skew + 165 correlation residual — the next
*consumer* refinement); Theta/MinHash cold-key overlap (residual 3, new substrate — measure first);
N-column groups + wider scalar lanes; the Rune cockpit group gauges; dimensioned CONVERT (009) / Rune
cockpit (010) as the breadth pivot.
