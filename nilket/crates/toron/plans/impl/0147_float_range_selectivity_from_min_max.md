# Impl Plan — ADR 147: Float Range Selectivity from Maintained Min/Max

**Migration for** `plans/adr/0147_float_range_selectivity_from_min_max.md`.
**Cross-ring:** Wendelon finite-float producer (min/max computation arm + encode +
tags) + Toron typed-domain consumer. Estimate-channel only. Anchors vs `main`.

## Surfaces
| Concern | Anchor |
|---|---|
| Float tag = `None` (→ OPAQUE) | `stats.rs:382` `arrow_stats_type_tag`; `:411` `is_valid_v1_arrow_stats_tag` |
| min/max per-type computation | `collect_column_stats_in_one_pass` (`stats.rs:1679` test wrapper: `min_max_for_array`) |
| Schema-owned tags + encode helpers | `optimizer_facts.rs:13` tag consts, `:111` `range_fact_tag_supported`, `:130` `encode_signed_optimizer_fact_scalar` (parallel) |
| Client re-export | `wendelon-client` (144 pattern) |
| Toron range helper (integer-only) | `binder.rs:3911` `RangeAtom{literal_value:i64}`, `:3917` `RangeInterval`, `:3960` `range_atom`, `:4011` `range_literal_value`, `:4032` `estimated_rows_for_range`, `:4064` `decode_range_i64` |

## Phase 1 — Wendelon producer
1. **Tags:** add `ARROW_STATS_TAG_FLOAT32/64` consts (`optimizer_facts.rs`),
   re-export via `wendelon-client`, add to `is_valid_v1_arrow_stats_tag`,
   `arrow_stats_type_tag` (`Float32`/`Float64` arms), `range_fact_tag_supported`.
2. **Float canonical encode/decode** beside 144's signed helpers
   (`encode/decode_float32_optimizer_fact_scalar`,
   `encode/decode_float64_optimizer_fact_scalar`, or a native-width generic):
   reject non-finite; normalize `-0.0`→`+0.0`; order-preserving big-endian over the
   unsigned raw bits (`raw ^ SIGN` for non-negative values, `!raw` for negative
   values; `u32` for `f32`, `u64` for `f64`); decode rejects bad length / non-finite.
   Round-trip + ordering tests (negatives < positives, −0=+0).
3. **min/max float arm** in `collect_column_stats_in_one_pass`: downcast
   `Float32Array`/`Float64Array`; fold min/max over non-null values via `partial_cmp`;
   **any NaN or ±∞ → emit counts-only/no-envelope stats for the column** (fail open,
   no opt-in payload built from only the finite subset, never error); normalize −0;
   encode via the new float encoder. Coverage fences unchanged; `range_fact_for_column`
   min-of-mins/max-of-maxes works once bytes exist.

## Phase 2 — Toron consumer (typed-domain refactor)
1. `RangeScalar { Integral(i128), Real(f64) }`. `range_literal_value` →
   `range_scalar_for_fact(range, value)`: int tags → `Integral`; float tags → `Real`
   (int literal → `Real` if `f64`-representable); non-finite/null → `None`.
   `decode_range_i64` → `decode_range_scalar(tag, bytes)` (int → `i128`; float →
   `f64`; non-finite → `None`).
2. `RangeInterval` → enum:
   - `Integral { lower_inclusive, upper_inclusive: Option<i128> }` — **144 verbatim**.
   - `Real { lower, upper: Option<FloatBound> }`, `FloatBound { value: f64, inclusive }`.
     `apply`: `Gt`→exclusive lower, `Gte`→inclusive lower, `Lt`→exclusive upper,
     `Lte`→inclusive upper; intersect tightest, `inclusive = a && b` at equal value.
   Domain chosen by the column's fact tag.
3. `estimated_rows_for_range` dispatch:
   - `Integral` → 144 unchanged.
   - `Real`: `min==max` → all-or-zero; clip to `[min,max]`;
     `frac=(high−low)/(max−min)`; non-empty overlap (`high>low`, or `high==low` with
     both endpoints inclusive) → `max(1, ceil(non_null·frac))`; `>= max` (incl) → 1;
     `> max`/disjoint → 0. Estimate-channel only.
4. Filter bind-arm integration unchanged (range estimate still `min`-combined with
   the equality estimate; 144/146 paths untouched).

## Phase 3 — tests
Wendelon: `table_optimizer_facts_emits_float_range_for_finite_values`,
`..._suppresses_float_range_with_nan_or_infinity`, `float_stats_normalizes_negative_zero`,
`float_canonical_encode_round_trips_and_orders`.
Toron binder (float `RangeFact` fixtures via the new encoder): headline
`float_range_filtered_dim_with_wendelon_min_max_infers_broadcast_by_estimate`,
`float_range_continuous_midpoint_estimates_half_non_null_rows`,
`float_range_endpoint_inclusive_ge_max_estimates_one_row_but_gt_max_zero`,
`float_range_constant_column_min_equals_max_no_div_by_zero`,
`float_range_outside_envelope_estimates_zero_without_hard_bound`,
`float_range_does_not_write_hard_bounds_or_empty_relation`,
`int_literal_against_float_fact_consumed_as_f64`,
`integer_range_facts_keep_discrete_interpolation` (regression),
`utf8_range_facts_still_fail_open`.
Architecture: retarget `range_utf8_and_float_facts_fail_open`; extend the scalar-tag
guard (schema-owned float tags + helpers); keep 144 estimate-channel + 146
no-elision guards; add the float-helper-no-hard-bound guard.

## Phase 4 — verification
```sh
cargo test -p wendelon
cargo test -p toron
cargo test -p toron --test architecture_test adr147
cargo test -p toron --test architecture_test adr144
cargo test -p toron --test architecture_test adr146
cargo check -p toron --all-targets
cargo check -p wendelon --all-targets
git diff --check
```
129–146 regressions green; integer range behavior unchanged (the `Integral` arm is
144 verbatim — the regression guard is load-bearing).

## Risk register
- **Float min/max NaN/∞** — producer arm suppresses per-column (not error);
  `..._suppresses_float_range_with_nan_or_infinity` guards it.
- **Inclusivity drop** — `Real` interval must carry `inclusive`, else `>= max` vs
  `> max` collapse; the endpoint test pins it.
- **Integer regression** — `Integral` must stay 144 exactly; its guard is load-bearing.
- **−0 / ordering** — encoder normalizes −0 + sorts sign-correctly; round-trip+order test.
- **Estimate-channel only** — no hard bound / `EmptyRelation` from float (144/146 +
  new float-helper guard).
