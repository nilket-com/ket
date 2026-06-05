# Impl Plan — ADR 161: Join-Key Range-Overlap Refinement for NDV Containment

Pairs with `plans/adr/161_join_key_range_overlap_refinement.md`. A **small Toron-local tightening** of
160's containment estimate: read the join keys' min/max the clean-base way, scale the estimate by the
overlap fraction. No new substrate (reuses live `RangeFact`s + the 158 decoders), no execution-side change
(160's consumer wire untouched — this only sharpens the `estimated_row_count` flowing into it). Anchors
verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Refine seam | `join_output_ndv_containment_estimate` — `binder.rs:4192` (returns `ndv_containment_join_rows`) |
| Base formula | `ndv_containment_join_rows` — `binder.rs:4170` (`ceil(|L|·|R| / max(ndv))`, `numerator==0 → 0`) |
| Clean-base helper (mirror) | `clean_base_column_ndv_for_join_input` — `binder.rs:4140` (root-`Scan`-only, projection remap) |
| Scan facts | `Scan.column_ndv:241`, `Scan.column_range: Vec<(ColumnOrdinal, RangeFact)>` — `ir/execution.rs:242` |
| Range fact | `RangeFact { arrow_type_tag, min: Option<Vec<u8>>, max, non_null_count }` — `catalog.rs:101` |
| Scalar decode (reuse) | `decode_range_scalar(tag, bytes) -> Option<RangeScalar>` — `binder.rs:4994`; `RangeScalar::{Integral(i128), Real(f64)}` — `binder.rs:4367` |
| Range type defense | descriptor range import filters ordinals, not tag/type compatibility — validate in the join consumer |
| Float join-key gate | `validate_join_key_types` rejects Float16/32/64 before estimation |
| Composite facts (unchanged) | `composite_with_join_estimate` — `binder.rs:229` (consumes the refined `RowCount`) |
| Guards module | `crates/toron/tests/architecture/join_cardinality.rs` (160's module, add adr161_*) |

## Phase 1 — Clean-base range reader (mirror the NDV helper)

`clean_base_column_range_for_join_input(input: &ExecutionPlan, requested_output_column: ColumnOrdinal)
-> Option<RangeFact>`: copy `clean_base_column_ndv_for_join_input:4140` in shape — match root
`ExecutionPlan::Scan { projection, column_range, .. }` only (return `None` for `Filter`/`Project`/`Join`/
others), remap `requested_output_column` through `projection` to the descriptor ordinal, then
`column_range.iter().find_map(|(c, fact)| (*c == descriptor_column).then(|| fact.clone()))`.

## Phase 2 — Overlap fraction (D3, D5)

`join_key_range_overlap_fraction(left: &RangeFact, left_key_type: &DataType, right: &RangeFact,
right_key_type: &DataType) -> Option<(u128, u128)>` returning `(overlap_width, min_width)` as an exact
rational (no float):
- validate each fact's `arrow_type_tag` against its actual join key type before decoding:
  `ARROW_STATS_TAG_INT32` ↔ `DataType::Int32`, `ARROW_STATS_TAG_INT64` ↔ `DataType::Int64`,
  `ARROW_STATS_TAG_DATE32` ↔ `DataType::Date32`, timestamp tags ↔ `DataType::Timestamp(unit,
  Some(timezone))` where `timezone == "UTC"` and `timestamp_fact_unit(tag) == Some(unit)`;
  do **not** infer fact compatibility from equal range tags alone — equi-join validation proves column
  type compatibility, not descriptor fact-tag compatibility;
- decode `min_L = decode_range_scalar(left.arrow_type_tag, left.min.as_deref()?)?`, same for `max_L`, and
  `min_R, max_R` with `right.arrow_type_tag`; if the two validated tags differ, `None` (fail-open);
- **Integral lane** (`RangeScalar::Integral` on all four): `i128` math, inclusive width `+1`:
  `lo = max(min_L,min_R)`, `hi = min(max_L,max_R)`; if `hi < lo` → `Some((0, 1))` (disjoint);
  else `overlap = (hi-lo)+1`, `width_L = (max_L-min_L)+1`, `width_R = (max_R-min_R)+1`,
  `min_w = width_L.min(width_R)`; return `Some((overlap as u128, min_w as u128))` (`min_w ≥ 1` always);
- **Real lane** (`RangeScalar::Real` on either) → `None` (fail-open to base containment, D5); this is a
  helper-level guard because SQL float join keys are rejected before this estimator runs;
- mixed Integral/Real (shouldn't happen post type-validation) → `None`.

## Phase 3 — Apply the tightening (D1, D4)

Pass the resolved join key data types into `join_output_ndv_containment_estimate` along with the ordinals.
Inside it, after `let base = ndv_containment_join_rows(...)?;`:
```
let (lk, rk) = match (
    clean_base_column_range_for_join_input(&left.plan, left_key),
    clean_base_column_range_for_join_input(&right.plan, right_key),
) {
    (Some(l), Some(r)) => (l, r),
    _ => return Some(base),                          // fail-open (D1/D5)
};
let Some((overlap_w, min_w)) =
    join_key_range_overlap_fraction(&lk, left_key_type, &rk, right_key_type)
else {
    return Some(base);                               // Real lane / tag/type mismatch → fail-open
};
if overlap_w == 0 {
    return Some(RowCount::new(0));                    // disjoint → provably empty (D4)
}
let refined = (u128::from(base.get()) * overlap_w) / min_w;   // ≤ base (overlap_w ≤ min_w)
Some(RowCount::new(u64::try_from(refined.max(1)).unwrap_or(u64::MAX)))   // floor ≥1 on overlap
```
Monotone by construction. No change to `composite_with_join_estimate`, `build_byte_candidate`, the
chooser, or fragmentation.

## Phase 4 — Fixtures + gates

- Headline `disjoint_join_key_ranges_estimate_empty_not_containment_product`: `orders ⋈ customers` on
  `region`, seeded `column_ndv` + disjoint `column_range` (`[1,5]` vs `[100,200]`) → refined
  `estimated_row_count == Some(0)`; assert the same plan without `column_range` estimates `> 0`.
- `partial_overlap_join_scales_containment_down`; `range_overlap_refinement_never_exceeds_containment`
  (fuzz seeded pairs); `contained_range_scores_full_overlap` (`L ⊂ R` → base unchanged);
- `integral_lane_covers_date32_and_timestamp_join_keys`; `range_tag_type_mismatch_fails_open_to_containment`;
- `range_overlap_real_lane_fails_open_in_helper`;
- `join_range_overlap_fails_open_without_range_fact`; `join_range_overlap_skips_filtered_input`;
- `join_range_overlap_is_estimate_channel_not_emptyrelation` (disjoint → `0`, plan still `Join`);
- `range_disjoint_child_join_broadcasts_zero_estimate_without_replay` (2-join payoff: parent spends the
  zero child estimate through 160's composite `BroadcastEstimated` path, no replay, digest = forced hash);
- `stale_disjoint_join_range_underestimate_replays_as_hash_and_matches_digest` (stale range safety:
  estimated broadcast replays as hash under a tight budget, digest = forced hash);
- keep 160 `join_cardinality` (6) + 158/159 + 144/147 regressions green.

## Phase 5 — Guards (join_cardinality.rs, adr161_*)

- `adr161_range_overlap_only_tightens_containment` — assert the refine site multiplies by `overlap_w/min_w`
  with `min` of widths and floors `≥1` (behavioral fuzz if source-matching is brittle).
- `adr161_range_overlap_validates_fact_tag_against_key_type` — assert the range-overlap helper receives key
  `DataType`s and checks tag/type compatibility before decoding.
- `adr161_range_overlap_spends_existing_composite_estimated_wire` — assert the headline payoff test exists
  and `build_byte_candidate`/`choose_auto_join_strategy` remain the ADR 160 estimated-composite shape.
- `adr161_no_emptyrelation_from_disjoint_join` — disjoint path returns `RowCount(0)`, never `EmptyRelation`.
- `adr136` markers gain no join entry (unchanged).

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron disjoint_join_key_ranges_estimate_empty
scripts/cargo-cgroup test -p toron range_disjoint_child_join_broadcasts_zero_estimate_without_replay
scripts/cargo-cgroup test -p toron stale_disjoint_join_range_underestimate_replays_as_hash
scripts/cargo-cgroup test -p toron range_overlap
scripts/cargo-cgroup test -p toron join_cardinality          # 160 regressions
scripts/cargo-cgroup test -p toron --test architecture_test adr161
scripts/cargo-cgroup test -p toron --test architecture_test  # FULL binary (expect 0 failed)
git diff --check
```

## Risks

- **Inflation bug** — must be monotone `≤ base`; `overlap_w ≤ min_w` guarantees it. A `max`-of-widths or
  union denominator would wrongly penalize contained ranges — use `min`. Pinned by the monotone +
  contained-full-overlap gates.
- **Disjoint false-positive from stale range** — only fires on clean `SingleBaseRelation` scans (exact
  min/max for the scanned table); a wrong `0` is still estimate-channel (139/140 catches an over-broadcast
  of an actually-nonempty join). Pinned by the estimate-channel guard.
- **Fact tag/type mismatch** — descriptor range import is ordinal-only, so the consumer must validate the
  range tag against the join key `DataType`. Pinned by `range_tag_type_mismatch_fails_open_to_containment`.
- **Float lane** — SQL float join keys are rejected before estimation; helper-level `RangeScalar::Real`
  cases fail open to base containment. Pinned by `range_overlap_real_lane_fails_open_in_helper`.
- **Filtered-input staleness** — clean-base helper rejects non-`Scan` roots (mirror 160-D2); pinned.

## Deferred (named)

Float32/64 key lane (two-sided rational or deliberate `f64`); two-sided participation fraction
(`overlap/w_L · overlap/w_R`, behind a measured asymmetric-partial miss); hard disjoint→`EmptyRelation`
join prune (D7, its own replay grounding); heavy-hitter-join skew refinement (top-k intersection, the 162
sibling); multi-key joins.
