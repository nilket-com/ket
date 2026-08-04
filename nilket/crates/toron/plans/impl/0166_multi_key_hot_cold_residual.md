# Impl Plan — ADR 166: Multi-Key Hot+Cold Residual From Group Top-K + Group NDV

Pairs with `plans/adr/0166_multi_key_hot_cold_residual.md`. A **contained, consumer-only** cut: modify the
164 floor section of `multi_key_join_cardinality` to compute the full hot+cold sum when *both* group facts
are present, reusing 164's `hot_tuple_join_contribution` + 165's composite-NDV. **No new producer, no
Wendelon change, no wire change.** Anchors verified on `main`.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Consumer seam (modify the 164 floor) | `multi_key_join_cardinality:4450`; base `refined` (NDV denom + 165 cap + 161 range-fold), then the disjoint short-circuit `if refined.get() == 0 { return Some(refined) }`, then the group-top-k floor block |
| 164 hot contribution (reuse) | `hot_tuple_join_contribution` → `{ rows, count, left_rows, right_rows }` |
| 164 / 165 readers (reuse) | `clean_base_column_group_top_k_for_join_input:4252`, `clean_base_column_group_ndv_for_join_input` |
| Containment + range (cold residual) | `ndv_containment_join_rows_from_denominator`, `range_refined_containment_join_rows` |
| Headline harness | descriptor-mapped existing group top-k + group NDV storage facts + the replay-signal runner |
| Guards | `crates/toron/tests/architecture/join_cardinality.rs` (adr166_*) |

## Phase 1 — The hot+cold sum (the only code change)

In `multi_key_join_cardinality`, first preserve the effective group denominator that ADR165 actually
spent:

```
let mut effective_group_denominator = None;
if let (Some(left_group_ndv), Some(right_group_ndv)) = (
    clean_base_column_group_ndv_for_join_input(&left.plan, &left_key_ordinals),
    clean_base_column_group_ndv_for_join_input(&right.plan, &right_key_ordinals),
) {
    let raw_group_denominator = u128::from(
        left_group_ndv
            .point_estimate()
            .max(right_group_ndv.point_estimate()),
    );
    if raw_group_denominator != 0 {
        denominator = denominator.min(raw_group_denominator);
        effective_group_denominator = Some(denominator);
    }
}
```

This is the already-capped `min(Π max(ndv), max(group_ndv_L, group_ndv_R))`, not just the raw group-NDV
point. Then compute `refined` and keep the `refined.get() == 0` disjoint short-circuit exactly before the
hot path. After that, name the current post-165 top-k behavior as `hot_floor`:

```
let hot = match (group_top_k_left, group_top_k_right) {
    (Some(l), Some(r)) => hot_tuple_join_contribution(l, left_rows, lk_types, r, right_rows, rk_types),
    _ => None,
};
let Some(hot) = hot else { return Some(refined); };            // no top-k → base (165/161), unchanged

let hot_floor = RowCount::new(refined.get().max(u64::try_from(hot.rows).unwrap_or(u64::MAX)));
let Some(effective_group_denominator) = effective_group_denominator else {
    return Some(hot_floor);                                    // only top-k → 164 floor behavior
};

let cold_rows_l = u128::from(left_rows.get()).saturating_sub(hot.left_rows);
let cold_rows_r = u128::from(right_rows.get()).saturating_sub(hot.right_rows);
let cold_ndv = effective_group_denominator.saturating_sub(u128::from(hot.count));
let cold = if cold_rows_l == 0 || cold_rows_r == 0 {
    RowCount::new(0)
} else if cold_ndv == 0 {
    return Some(hot_floor);                                    // inconsistent residual; keep post-165
} else {
    let cold_base = ndv_containment_join_rows_from_denominator(
        RowCount::new(u64::try_from(cold_rows_l).unwrap_or(u64::MAX)),
        RowCount::new(u64::try_from(cold_rows_r).unwrap_or(u64::MAX)),
        cold_ndv,
    )?;
    range_fold_cold(cold_base, left, right, left_keys, right_keys)   // reuse 161 per-key fold
};
let Some(candidate) = hot.rows.checked_add(u128::from(cold.get())) else {
    return Some(hot_floor);
};
Some(RowCount::new(hot_floor.get().max(u64::try_from(candidate).unwrap_or(u64::MAX))))   // raising-only
```
- **Refactor:** lift the effective 165-capped denominator into a reusable `Option<u128>` binding (`Some`
  iff both group-NDV facts matched and the cap denominator was nonzero). Do not use raw group NDV for the
  cold residual.
- `range_fold_cold` reuses the existing per-key `range_refined_containment_join_rows` fold over the join
  keys (same as the base path; a key lacking ranges → fraction 1, the 161 fail-open).

## Phase 2 — Fixtures + gates

- Headline `live_column_group_hot_cold_recovers_multi_key_skew_and_correlation_without_seeding`: use
  descriptor-mapped existing storage facts for both group facts; **calibrate** so `refined_165` *and*
  `exact_hot_tuple` each land just **under** the broadcast threshold (current `max` → broadcast → replay)
  while `hot+cold` lands **above** (166 → hash, `replayed == false`) — a strategy **flip** (iter-2 Close
  3), not just a number.
- `only_one_group_fact_present_is_byte_identical_to_post_165` (top-k-only → 164 floor; NDV-only → 165 cap;
  exact equality);
- seeded arithmetic gates: `hot_plus_cold_no_double_counts_hot_tuples`;
  `cold_ndv_uses_effective_composite_not_independence_proxy_or_raw_overcount`;
  `hot_plus_cold_never_lowers_post_165`; `cold_residual_zero_when_all_tuples_hot`;
  `cold_ndv_zero_with_cold_rows_declines_to_post_165`;
- `disjoint_join_short_circuits_before_hot_plus_cold` (a disjoint join + a stale shared-tuple top-k → `0`,
  the disjoint-dominance ordering, iter-2 Close 2);
- `cold_residual_applies_range_overlap`; `hot_plus_cold_fails_open_without_either_group_fact`;
- keep 160–165 `join_cardinality` + 142 + 155/156 + 158/159 regressions green.

## Phase 3 — Guards (join_cardinality.rs, adr166_*)

- `adr166_hot_plus_cold_is_estimate_channel_only`; `adr166_requires_both_group_facts`;
  `adr166_no_new_producer_or_wire` (the diff touches no Wendelon/wire/producer code — consumer-only;
  source-scan that `crates/wendelon` and the optimizer-facts wire are unchanged by this ADR);
- `adr166_hot_plus_cold_never_lowers_prior` (raising-only `max`); `adr136` markers unchanged.
- Retarget `adr164_consumer_uses_exact_hot_floor_not_fake_residual`: the negative residual assertions are
  stale. The new ADR164 invariant is top-k-only behavior: valid top-k computes `hot_floor`, missing group
  NDV returns `hot_floor`, and the binder contains
  `only_one_group_fact_present_is_byte_identical_to_post_165`.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron live_column_group_hot_cold_recovers_multi_key_skew_and_correlation_without_seeding
scripts/cargo-cgroup test -p toron only_one_group_fact_present_is_byte_identical_to_post_165
scripts/cargo-cgroup test -p toron hot_plus_cold
scripts/cargo-cgroup test -p toron cold_ndv_zero_with_cold_rows_declines_to_post_165
scripts/cargo-cgroup test -p toron disjoint_join_short_circuits_before_hot_plus_cold
scripts/cargo-cgroup test -p toron join_cardinality          # 160–165 regressions
scripts/cargo-cgroup test -p toron --test architecture_test adr166
scripts/cargo-cgroup test -p toron --test architecture_test  # FULL binary (expect 0 failed)
git diff --check
```

## Risks

- **Double-count** — cold residual must subtract hot rows AND `hot.count` from the composite NDV; pinned by
  `hot_plus_cold_no_double_counts_hot_tuples`.
- **Fake-precision regression** — `cold_ndv` from 165's effective capped denominator, never the raw
  `Π max(ndv)` proxy or raw HLL overcount; **no second cap** (D2); pinned by
  `cold_ndv_uses_effective_composite_not_independence_proxy_or_raw_overcount`.
- **Failing open to the wrong prior** — after a valid top-k contribution, declines must return
  `hot_floor`, not raw `refined`; pinned by the top-k-only / one-fact darkness gate.
- **Residual NDV fabrication** — cold rows with `cold_ndv == 0` must decline to post-165, not clamp to one.
- **Composition drift** — only-one-fact / neither must stay byte-identical to post-165; pinned by the
  composition-darkness gate + the 160–165 regression suite.
- **Raising-only** — `max(hot_floor, candidate)` after valid top-k; never below post-165 behavior (cold
  half is HLL-approx).
- **Disjoint interaction** — the `refined == 0` short-circuit precedes this block (D4); pinned by
  `disjoint_join_short_circuits_before_hot_plus_cold`.
- **Consumer-only scope** — no producer/wire/Wendelon change; pinned by `adr166_no_new_producer_or_wire`.

## Deferred (named)

Theta/MinHash cold-key value-set overlap (residual 3, measure-first — the last, lowest-value residual);
N-column groups + wider scalar lanes; **dimensioned CONVERT (009)** — the agreed breadth pivot next; the
Rune cockpit (010); multi-join reordering.
