# ADR 289 — Implementation plan: q14 correctness oracle

Pairs with `plans/adr/0289_q14_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — reconcile, measure, verify non-vacuity
1. **Reconcile any in-progress draft first.** A prior q14 draft dropped the ADR 275 Decimal-join probe's
   `Err(StageError { phase, reason })` match arm. Keep that arm intact and get a clean
   `scripts/cargo-cgroup check -p toron-tpch --all-targets` before trusting any new q14 behavior.
2. **Measure and pin the live schema.** Run strict q14 at sf=0.01 and capture `batches[0].schema()`. The measured
   output is **`promo_revenue Decimal128(38, 10)`** (scale 10 — the divide widens well beyond `(38,4)`, the
   ADR 271 warning). Assert the *measured* type; never bake a paper guess.
3. **Verify non-vacuity at sf=0.01:** denominator rows > 0 **and PROMO rows > 0**. If PROMO rows are zero, stop
   and re-scope — a hollow CASE-true branch is not a valid correctness cell.

## D1 — `strict_q14_oracle` (`crates/toron-tpch/src/lib.rs`)
A `Q14Oracle { promo_revenue: DecimalRaw, denominator_rows: usize, promo_rows: usize }` (reuse ADR 288's
`DecimalRaw`). Independent traversal:
- `HashMap<i64, bool>` from `PartGenerator`: `p_partkey → p_type.starts_with("PROMO")` (the fixture populates
  `p_type` from the generator).
- One pass over `LineItemGenerator`: for each item with `l_partkey` in the map and
  `l_shipdate ∈ [day(1995-09-01), day(1995-10-01)) = [9374, 9404)` (half-open; mirror the engine via the
  generator date's epoch-day vs the bounds, as `strict_q5_oracle` does — keep the constants near a test that
  proves the day-counts):
  - `rev = discounted_price_raw(extprice_raw, discount_raw)` (raw scale 4);
  - `sum_all_raw += rev`; `denominator_rows += 1`;
  - if promo: `sum_promo_raw += rev`; `promo_rows += 1`. **(CASE trap: a non-PROMO row counts toward the
    denominator but `0` toward the numerator.)**
- `numerator = sum_promo_raw.checked_mul(10_000)` (the `100.00` literal → scale 4+2 = 6).
- `promo_revenue = decimal_division_raw(numerator, <p_num>, 6, sum_all_raw, <p_den>, 4)`. Pass the
  precisions that yield the **Phase-0-measured `(38,10)`** (`decimal_division_output_type` is the engine's; the
  live test is the cross-validator — the value is scale-driven, scale delta `10+4-6=8`). If `sum_all_raw == 0` →
  `value: None` with the measured `(p,s)`. Overflow → `Err(String)` → `fail@compare`, never panic.

## D2 — `check_strict_q14` + `compare_strict_q14` + wiring
Mirror `check_strict_q5`:
1. **Factor `q14_tables(strict, sf)`** out of `run_dev_q14`/`run_strict_q14` — the ADR 288
   `q17_tables` move; reach runners and the oracle then share one builder (no fixture drift). Pure refactor — the
   reach row stays green.
2. `execute_query_with_tables(STRICT_Q14_SQL, q14_tables(true, sf))`; `StageError` → classified row.
3. `assert_strict_q14_schema(&batches)` — `promo_revenue` = the measured `Decimal128(38,10)`; drift →
   `Compare`/`Fail`.
4. Non-vacuity: `denominator_rows >= 1 && promo_rows >= 1 && promo_revenue.value.is_some()` → else `Fail` w/ counts.
5. `compare_strict_q14(&batches, &oracle)`: exactly one row; `is_null` on `promo_revenue`; raw `i128` ==
   `oracle.promo_revenue.value`; oracle-side `(p,s)` checked.
- `main.rs`: route `"q14" => check_strict_q14(sf).await` (strict-only; **normal correctness default sf=0.01**, no
  q17-style scale policy). Refresh the command's `ADR 273/276/280/287` comment to include 289 (or de-number it).
- `correctness_matrix`: q14 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q14_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread if deep).
- `strict_q14_oracle_is_non_vacuous_at_default_scale` — `denominator_rows ≥ 1 && promo_rows ≥ 1 &&
  value.is_some()`.
- `q14_promo_branch_is_required` — an oracle with denominator rows but `promo_rows == 0` → `compare` **fails**
  (the load-bearing anti-hollow-CASE guard).
- **CASE fidelity** — a constructed case proving a non-PROMO row contributes to the denominator but `0` to the
  numerator (flipping one row's promo flag changes the expected ratio).
- `q14_ratio_uses_half_away_division` — `decimal_division_raw` on a crafted quotient where truncation differs
  (reuse the q17 truncation discipline at the q14 scale).
- `q14_correctness_mismatch_is_fail_at_compare` — off-by-one value, wrong-precision schema → fail.
- `q14_null_or_empty_result_is_fail_at_compare` — NULL output, 0-row, >1-row, oracle-NULL → fail.
- Existing correctness (q01/q03/q05/q06/q17) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q14
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Live `correctness --query q14` must emit `phase=compare status=pass`.

## Results doc
`crates/toron/plans/impl/0289_q14_correctness_oracle_results.md`: the Phase-0-measured `promo_revenue`
`Decimal128(38,10)` (wider-than-(38,4) fact); sf=0.01 non-vacuity counts (denominator + **PROMO** rows); the
`100.00 *` scaling + reused `discounted_price_raw`/`decimal_division_raw`; q14 `strict=pass@compare`;
confirmation prior rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the CASE-fidelity +
  promo-branch + injected-mismatch + truncation tests, the denominator+PROMO non-vacuity guard, and the Phase-0
  schema pin are the defenses.
- **Measure the divide schema** — `(38,10)`, the single most likely place to get it wrong.
- The `q14_tables` factor must not change reach-runner behavior (the reach row stays green) — pure refactor.
- Keep the ADR 275 `StageError` probe arm intact while reconciling any draft material.

## Seal
`impl: adr 289 q14 correctness oracle`. Do not commit wip drafts.
