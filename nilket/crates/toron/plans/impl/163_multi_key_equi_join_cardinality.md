# Impl Plan — ADR 163: Multi-Key Equi-Join Cardinality

Pairs with `plans/adr/163_multi_key_equi_join_cardinality.md`. Generalize the join estimator from one key
pair to all, under attribute independence: product denominator + per-key 161 range-overlap fold.
Single-key path unchanged (byte-identical to 162); heavy-hitter + correlation deferred. No new substrate,
no execution-side change. Anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Seam (single-element slice → generalize) | binder.rs:1479 `([left_key],[right_key]) => …, _ => None` |
| Key vecs (already collected) | `left_keys`/`right_keys` — binder.rs:1451–1461; resolved keys carry `.column.ordinal` + `.column.data_type` |
| Containment denominator (factor) | `ndv_containment_join_rows_from_points(rows_L, rows_R, ndv_L, ndv_R)` — `ceil(|L|·|R|/max(ndv))` |
| Clean-base readers (reuse per key) | `clean_base_column_ndv_for_join_input:4145`, `clean_base_column_range_for_join_input:4174` |
| Range refine (fold per key) | `range_refined_containment_join_rows(base, rangeL, typeL, rangeR, typeR)` — `binder.rs:4266` (monotone; disjoint → 0; floor guarded by `base > 0`) |
| Single-key pipeline (len==1) | `join_output_ndv_containment_estimate` (160/161/162) — unchanged |
| Composite facts (unchanged) | `composite_with_join_estimate` — `binder.rs:229` |
| Guards module | `crates/toron/tests/architecture/join_cardinality.rs` (add adr163_*) |

## Phase 1 — Factor the denominator seam (no behavior change)

- Extract `ndv_containment_join_rows_from_denominator(left_rows: RowCount, right_rows: RowCount, denom:
  u128) -> Option<RowCount>`: `if denom == 0 { None }`; `num = |L|·|R|` (u128); `est = if num==0 {0} else
  {((num-1)/denom)+1}`; `RowCount::new(try_from(est).unwrap_or(MAX))`. The `+1` keeps `est ≥ 1` for any
  non-empty input regardless of `denom` size (no spurious zero — D3).
- `ndv_containment_join_rows_from_points(rows_L, rows_R, ndv_L, ndv_R)`: `if ndv_L==0||ndv_R==0 {None}`
  else `_from_denominator(rows_L, rows_R, u128::from(ndv_L.max(ndv_R)))`. **Single-key byte-identical.**
- Add a small capped denominator multiply: for a positive numerator, once `denom >= numerator`, keep
  `denom = numerator`; if `checked_mul` overflows, also cap at `numerator`. Larger denominators all produce
  the same floor-1 estimate, so this preserves signal without wraparound or fail-open noise.

## Phase 2 — Multi-key estimator

`multi_key_join_cardinality(kind, left, right, left_keys: &[ResolvedJoinKey], right_keys:
&[ResolvedJoinKey]) -> Option<RowCount>`:
```
if kind != JoinKind::Inner
    || left.facts.origin != SingleBaseRelation || right.facts.origin != SingleBaseRelation {
    return None;
}
let left_rows  = estimate_row_source(left.facts.row_upper_bound, left.facts.estimated_row_count)?;
let right_rows = estimate_row_source(right.facts.row_upper_bound, right.facts.estimated_row_count)?;

// D2 — product denominator; ALL keys need clean-base NDV
let numerator = u128::from(left_rows.get()) * u128::from(right_rows.get());
let mut denom: u128 = 1;
for (lk, rk) in left_keys.iter().zip(right_keys) {
    let ndv_l = clean_base_column_ndv_for_join_input(&left.plan, lk.column.ordinal)?.point_estimate();
    let ndv_r = clean_base_column_ndv_for_join_input(&right.plan, rk.column.ordinal)?.point_estimate();
    if ndv_l == 0 || ndv_r == 0 { return None; }
    denom = multiply_denominator_capped_at_numerator(
        denom,
        u128::from(ndv_l.max(ndv_r)),
        numerator,
    );                                                                // overflow -> cap, never wrap
}
let base = ndv_containment_join_rows_from_denominator(left_rows, right_rows, denom)?;

// D3 — per-key range-overlap fold (reuse 161; any disjoint key → 0, propagates)
let mut refined = base;
for (lk, rk) in left_keys.iter().zip(right_keys) {
    if let (Some(lr), Some(rr)) = (
        clean_base_column_range_for_join_input(&left.plan, lk.column.ordinal),
        clean_base_column_range_for_join_input(&right.plan, rk.column.ordinal),
    ) {
        refined = range_refined_containment_join_rows(
            refined, &lr, &lk.column.data_type, &rr, &rk.column.data_type,
        ).unwrap_or(refined); // malformed/tag mismatch keeps 161 fail-open; disjoint returns 0
    }                                                                   // missing range ⇒ fraction 1 (skip)
}
Some(refined)        // multi-key: HH deferred (D4)
```

## Phase 3 — Wire the seam (D1)

binder.rs:1479:
```
let join_estimated_row_count = match (left_resolved_keys.as_slice(), right_resolved_keys.as_slice()) {
    ([left_key], [right_key]) => join_output_ndv_containment_estimate(  // 160/161/162 single-key, unchanged
        kind, &left_plan, &right_plan,
        left_key.column.ordinal, right_key.column.ordinal,
        &left_key.column.data_type, &right_key.column.data_type,
    ),
    (lks, rks) if lks.len() == rks.len() && lks.len() >= 2 =>
        multi_key_join_cardinality(kind, &left_plan, &right_plan, lks, rks),   // NEW
    _ => None,
};
```
(`bind_join` swaps `left_keys`/`right_keys` together on placement, 1547 — the pairing stays aligned.)

## Phase 4 — Fixtures + gates

- Headline `multi_key_join_estimates_from_product_denominator_not_composite_unknown`: `order_items ⋈
  line_prices` on `(order_id, line_no)`, seeded per-key `column_ndv` → 163 estimates `|L|·|R|/(max(ndv_a)·
  max(ndv_b))`; assert `main` gives `composite_unknown` (no estimate) and 163 gives `Some(_)` that flips
  the strategy decision.
- `multi_key_disjoint_on_any_key_estimates_empty` (one key disjoint → `0`, other overlaps);
- `multi_key_denominator_is_product_of_per_key_max_ndv`; `multi_key_range_overlap_folds_per_key`;
- `multi_key_range_malformed_key_keeps_multi_key_ndv_estimate`;
- `single_key_join_unchanged_byte_identical_to_162` (the critical no-regression);
- `multi_key_three_key_join_does_not_spuriously_zero` (3-key non-disjoint → `≥1`) **and** its converse
  (one disjoint key among three → `0`, propagates through the other folds — pins D3);
- `multi_key_fails_open_when_any_key_lacks_ndv`; `multi_key_does_not_apply_heavy_hitter`;
- `multi_key_is_estimate_channel_not_emptyrelation`;
- keep 160/161/162 `join_cardinality` + 155/156 + 158/159 + 144/147 regressions green.

## Phase 5 — Guards (join_cardinality.rs, adr163_*)

- `adr163_multi_key_denominator_is_product` — the multi-key path multiplies per-key `max(ndv)`.
- `adr163_multi_key_defers_heavy_hitter` — multi-key estimator does not consult `column_heavy_hitters`.
- `adr163_multi_key_requires_clean_base_ndv` — clean-base NDV reader per key, root-`Scan` only.
- `adr163_no_wendelon_tuple_top_k_surface` — ADR 163 does not add Wendelon tuple top-k policy/fact structs;
  the follow-up producer should use brand-clean names like `ColumnGroupTopKSpec`, `ColumnGroupTopKFact`,
  and `ColumnGroupTopKEntry`, with no new `*V1` suffixes.
- `adr136` markers gain no join entry.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron multi_key_join_estimates_from_product_denominator
scripts/cargo-cgroup test -p toron single_key_join_unchanged_byte_identical_to_162
scripts/cargo-cgroup test -p toron multi_key
scripts/cargo-cgroup test -p toron join_cardinality            # 160/161/162 regressions
scripts/cargo-cgroup test -p toron --test architecture_test adr163
scripts/cargo-cgroup test -p toron --test architecture_test    # FULL binary (expect 0 failed)
git diff --check
```

## Risks

- **Single-key regression** — the denominator factoring must keep `len==1` byte-identical; pinned by
  `single_key_join_unchanged_byte_identical_to_162` + the full 160/161/162 `join_cardinality` suite.
- **Spurious zero** — `_from_denominator`'s `+1` ceil + 161's `base>0`-guarded floor mean only a *truly
  disjoint* key yields `0`; denominator multiplication caps at the numerator instead of wrapping. Pinned
  by the 3-key test + its converse.
- **Correlation under-estimate** — independence over-states `denom` on correlated keys; estimate-channel,
  backstopped; named for the residual evidence, not fixed here.
- **Filtered-input staleness** — clean-base helpers reject non-`Scan` roots (160/161 discipline), per key.
- **Key pairing alignment** — `left_keys`/`right_keys` stay zipped through the placement swap (1547).

## Deferred (named)

Wendelon composite/tuple heavy-hitter top-k producer (extend 156 per-column → composite; the 156/159
cross-ring pattern) + the multi-key-skew consumer it activates — the first new-production need in the join
arc. Use brand-clean Wendelon names such as `ColumnGroupTopKSpec`, `ColumnGroupTopKFact`, and
`ColumnGroupTopKEntry`; do not introduce new `*V1` suffixes. Also deferred: correlation / composite-NDV
refinement (measure in the residual evidence first); multi-key over `Filter`-ed inputs; the Rune cockpit
multi-key gauge; Theta/MinHash for the cold-key overlap residual.
