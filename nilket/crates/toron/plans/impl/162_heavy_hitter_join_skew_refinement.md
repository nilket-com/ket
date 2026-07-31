# Impl Plan — ADR 162: Heavy-Hitter Join-Skew Refinement

Pairs with `plans/adr/162_heavy_hitter_join_skew_refinement.md`. A **Toron-local** decomposition wrapping
the 160/161 join estimate: exact hot-both intersection (reuse 156's top-k) + 160/161 containment on the
cold residual. No new substrate, no execution-side change (sharpens the `estimated_row_count` 160's wire
already reads). A pure superset of 161 — byte-identical when no shared hot keys, and never below the
160/161 prior when co-hot facts are present. Anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Estimate seam (wrap) | `join_output_ndv_containment_estimate` (161-refined) — `binder.rs` |
| Base containment (residual) | `ndv_containment_join_rows` — `binder.rs:4203` (explicit rows/ndv) |
| 161 range refine (residual) | `range_refined_containment_join_rows` — `binder.rs:4266` (explicit `base` + ranges) |
| Clean-base helpers (mirror) | `clean_base_column_ndv_for_join_input:4145`, `clean_base_column_range_for_join_input:4174` |
| HH reader (recurses Filter — don't reuse) | `column_heavy_hitter_for_input:4340` |
| HH fact | `ColumnHeavyHitterFact { entries: BTreeMap<ScalarLiteralKey, u64> }` — catalog.rs:137; `Scan.column_heavy_hitters` (ir/execution.rs) |
| NDV fact | `DistinctCountFact::point_estimate() -> u64` — catalog.rs:86 |
| Composite facts (unchanged) | `composite_with_join_estimate` — `binder.rs:229` |
| Guards module | `crates/toron/tests/architecture/join_cardinality.rs` (add adr162_*) |

## Phase 1 — Clean-base heavy-hitter reader (mirror)

`clean_base_column_heavy_hitter_for_join_input(input: &ExecutionPlan, requested_output_column:
ColumnOrdinal) -> Option<&ColumnHeavyHitterFact>`: copy the `clean_base_column_ndv_for_join_input:4145`
shape — root `ExecutionPlan::Scan { projection, column_heavy_hitters, .. }` only, projection-remap the
ordinal, `find_map` the matching fact. Borrow (no clone; `entries` can be large).

## Phase 2 — Hot-both intersection (D3)

`hot_both_join_contribution(left: &ColumnHeavyHitterFact, left_rows: RowCount, left_key_type: &DataType,
right: &ColumnHeavyHitterFact, right_rows: RowCount, right_key_type: &DataType) -> Option<HotBoth>` where
`HotBoth { rows: u128, count: u64, left_rows: u128, right_rows: u128 }` (derive `Default`):
- first validate both facts before spending either:
  - every frequency is nonzero;
  - the sum of emitted frequencies on each side is `<=` that side's chosen base row estimate;
  - every `ScalarLiteralKey` variant matches that side's join-key `DataType` (Boolean, Int32, Int64,
    Date32, UTC Timestamp with matching `TimeUnit`, Utf8; unsupported or mismatched keys fail open);
- iterate the smaller `entries` map; for each `(v, fL)` with `right.entries.get(v) == Some(fR)`:
  - `rows = rows.checked_add(u128::from(*fL).checked_mul(u128::from(*fR))?)?` (overflow → `None` →
    fail-open to 161);
  - `count += 1`; `left_rows += u128::from(*fL)`; `right_rows += u128::from(*fR)`;
- `H = ∅` ⇒ `HotBoth::default()` (all zero) — caller yields pure 161.

## Phase 3 — Cold residual + sum (D1, D4) — sequence the two existing helpers

In `join_output_ndv_containment_estimate`, after the inputs (`left_rows, right_rows, left_ndv, right_ndv,
left_range?, right_range?`) are resolved, compute the existing 160/161 estimate first and use it as the
fail-open prior:
```
let left_ndv_point = left_ndv.point_estimate();
let right_ndv_point = right_ndv.point_estimate();
let base = ndv_containment_join_rows_from_points(
    left_rows, right_rows, left_ndv_point, right_ndv_point,
)?;
let range_overlap = match (left_range, right_range) {
    (Some(lr), Some(rr)) =>
        join_key_range_overlap_fraction(lr, left_key_type, rr, right_key_type),
    _ => None,
};
let prior = match (left_range, right_range) {
    (Some(lr), Some(rr)) =>
        range_refined_containment_join_rows(base, lr, left_key_type, rr, right_key_type)
            .unwrap_or(base),
    _ => base,
};
if matches!(range_overlap, Some((0, _))) {
    return Some(prior);                                        // preserve 161 disjoint zero
}
let hb = match (
    clean_base_column_heavy_hitter_for_join_input(&left.plan, left_key),
    clean_base_column_heavy_hitter_for_join_input(&right.plan, right_key),
) {
    (Some(l), Some(r)) => match hot_both_join_contribution(
        l, left_rows, left_key_type, r, right_rows, right_key_type,
    ) {
        Some(hb) => hb,
        None => return Some(prior),                            // fail-open: 161 behavior
    },
    _ => return Some(prior),                                    // no HH facts: byte-identical to 161
};
let rows_l = u128::from(left_rows.get()).saturating_sub(hb.left_rows);
let rows_r = u128::from(right_rows.get()).saturating_sub(hb.right_rows);
let ndv_l  = left_ndv_point.saturating_sub(hb.count);
let ndv_r  = right_ndv_point.saturating_sub(hb.count);
let residual = if rows_l == 0 || rows_r == 0 {
    0u128
} else if ndv_l == 0 || ndv_r == 0 {
    return Some(prior);                                        // inconsistent residual; keep prior
} else {
    let cold_base = ndv_containment_join_rows_from_points(
        RowCount::new(u64::try_from(rows_l).unwrap_or(u64::MAX)),
        RowCount::new(u64::try_from(rows_r).unwrap_or(u64::MAX)),
        ndv_l, ndv_r,
    )?;
    let refined = match (left_range, right_range) {       // residual re-applies range-overlap (D4)
        (Some(lr), Some(rr)) =>
            range_refined_containment_join_rows(cold_base, lr, left_key_type, rr, right_key_type)
                .unwrap_or(cold_base),
        _ => cold_base,
    };
    u128::from(refined.get())
};
let total = hb.rows.checked_add(residual)?;               // exact hot-both + cold residual (the SUM)
let candidate = RowCount::new(u64::try_from(total).unwrap_or(u64::MAX));
Some(RowCount::new(prior.get().max(candidate.get())))      // never lower 160/161
```
- Factor `ndv_containment_join_rows_from_points(left_rows, right_rows, left_ndv: u64, right_ndv: u64)` out
  of the existing helper's formula, then let `ndv_containment_join_rows` call it through
  `DistinctCountFact::point_estimate()`. Do not introduce a fake `DistinctCountFact` constructor for the
  residual counts.
- `H = ∅` ⇒ `hb` all-zero ⇒ `rows_l/ndv_l` unchanged ⇒ `residual == the 161 estimate` ⇒ `total == 161`
  ⇒ **byte-identical to 161** (the pure-superset no-op — pin it).
- Disjoint ranges ⇒ `H = ∅` (no shared key in-range) ⇒ residual range step returns `0` ⇒ `total == 0`.
- If the heavy-hitter candidate is smaller than `prior` because of range looseness or tail NDV reduction,
  return `prior`; 162 is a skew recovery overlay, not a lowering pass.

## Phase 4 — Fixtures + gates

- Headline `co_hot_join_recovers_skew_and_hashes_without_replay`: `orders ⋈ payments` on `customer_id`,
  seeded `column_ndv` + `column_heavy_hitters` with a shared large-freq whale key → estimate ≫ uniform →
  **hash, `replayed == false`**; assert the same plan without HH broadcasts → replays.
- `no_shared_hot_keys_falls_back_to_161_containment` (anti-correlated top-k → byte-identical to 161);
- `hot_both_contribution_is_exact_product_of_frequencies`;
- `residual_excludes_hot_both_rows_and_keys_no_double_count` (total = exact + residual; hot-both removed;
  a partial-overlap + co-hot fixture confirms the residual passes through range-overlap);
- `disjoint_ranges_force_empty_hot_set_composes_with_161` (disjoint ⇒ estimate `0`);
- `disjoint_range_zero_dominates_stale_shared_hot_key` (shared HH fixture cannot raise a 161 zero-overlap
  estimate);
- `join_skew_never_lowers_161_prior` (candidate below prior returns prior);
- `join_skew_fails_open_on_key_type_mismatch_or_invalid_frequency` (wrong key variant, zero frequency,
  over-base emitted total → prior);
- `stale_heavy_hitter_overestimate_stays_estimate_channel_replays_or_hashes` (wrong fact → higher estimate
  → hash, or broadcast→replay byte-identical — the backstop);
- `join_skew_fails_open_without_heavy_hitter_fact`; `join_skew_skips_filtered_input`;
- keep 160/161 `join_cardinality` + 155/156 + 158/159 + 144/147 regressions green.

## Phase 5 — Guards (join_cardinality.rs, adr162_*)

- `adr162_join_skew_is_estimate_channel_only` — sets only `estimated_*`; no hard bound / `EmptyRelation`.
- `adr162_hot_both_removed_from_residual` — residual inputs subtract hot-both rows/keys (source or
  behavioral fuzz: total never double-counts a co-hot key; `H=∅ ⇒ byte-identical-to-161`).
- `adr162_requires_clean_base_heavy_hitter` — clean-base helper, root-`Scan` only.
- `adr162_join_skew_keeps_160_161_prior_floor` — source/behavior guard for `max(prior, candidate)` and
  invalid-HH fail-open.
- `adr136` markers gain no join entry.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron co_hot_join_recovers_skew_and_hashes_without_replay
scripts/cargo-cgroup test -p toron no_shared_hot_keys_falls_back_to_161
scripts/cargo-cgroup test -p toron disjoint_range_zero_dominates_stale_shared_hot_key
scripts/cargo-cgroup test -p toron join_skew_never_lowers_161_prior
scripts/cargo-cgroup test -p toron join_skew_fails_open_on_key_type_mismatch_or_invalid_frequency
scripts/cargo-cgroup test -p toron join_skew
scripts/cargo-cgroup test -p toron join_cardinality            # 160/161 regressions
scripts/cargo-cgroup test -p toron --test architecture_test adr162
scripts/cargo-cgroup test -p toron --test architecture_test    # FULL binary (expect 0 failed)
git diff --check
```

## Risks

- **Double-count** — the residual must subtract hot-both rows and keys; pinned by
  `residual_excludes_hot_both_rows_and_keys_no_double_count` and the `H=∅ ⇒ byte-identical-to-161` no-op.
- **Over-estimate from stale/over-correlated facts** — estimate may rise; safe direction (→ hash), and a
  broadcast off a wrong estimate replays byte-identical. Pinned by the stale-fact gate.
- **Overflow** — `freq_L·freq_R` and the sum use checked `u128`; overflow → fail-open to 161.
- **Bad seeded/mock facts** — mismatched key variants, zero frequencies, or per-side emitted totals above
  base rows fail open to the 160/161 prior before spending exact hot-both rows.
- **Range-inconsistent HH facts** — a 161-proven zero range overlap returns the prior before HH spend, so a
  stale shared key cannot undo disjoint-range pruning.
- **Filtered-input staleness** — clean-base helper rejects non-`Scan` roots (160/161 discipline); pinned.
- **Residual underflow** — `saturating_sub`; residual rows of `0` ⇒ residual `0`; residual rows with
  residual NDV of `0` ⇒ fail-open to the 160/161 prior.

## Deferred (named)

Anti-correlated hot keys (hot-one-side, bound by the other side's HH threshold — one-sided over-state
risk, only if measured); multi-key joins; the measured *residual* miss evidence (163) gating Theta/MinHash
(164); the Rune cockpit skew gauge.
