# ADR 306 — Implementation plan: q22 correctness oracle

Pairs with `plans/adr/306_q22_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree. A 2-pass independent traversal verifying q22's anti-join + AVG-threshold +
SUBSTRING.

## Phase 0 — measure schema + non-vacuity (masked-empty check)
1. **Schema:** run strict q22 at sf=0.01, capture `batches[0].schema()` → `cntrycode` Utf8, `numcust` **Int64**,
   `totacctbal` **`Decimal128(p,2)`** (SUM(c_acctbal); measure `p`). Assert it.
2. **Non-vacuity:** measure the row count + per-group `numcust` + `with_orders_excluded`. The target is `sf=0.01`,
   but a hollow live result is not an acceptable correctness pass: if `selected_rows == 0`, retarget to the
   smallest tractable scale with selected rows (q17 precedent) or stop and revise the plan. Confirm
   `avg_threshold_raw > 0`. If no live row is excluded only by the anti-join, record that fact and let the
   mandatory synthetic carry anti-join discrimination.

## D1 — `strict_q22_oracle` (`crates/toron-tpch/src/lib.rs`)
```rust
pub struct Q22Oracle {
    pub groups: BTreeMap<String, (i64, i128)>,  // cntrycode -> (numcust, totacctbal raw scale-2)
    pub avg_threshold_raw: i128,                 // scale 6
    pub with_orders_excluded: usize, pub selected_rows: usize,
}
```
- `CODES: [&str; 7] = ["13","31","23","29","30","18","17"]`; `cntry(c_phone) = &c_phone[0..2]` (char-counted;
  c_phone is ASCII so byte==char, but slice by char to match ADR 305).
- **Pass 1 (threshold):** over `CustomerGenerator`, for customers with `cntry ∈ CODES` **and** `c_acctbal_raw > 0`:
  `sum += c_acctbal_raw`, `count += 1`. `avg_threshold_raw = avg_decimal_raw(sum, count, 2, 6, 19)`
  (**reuse lib.rs:1494** — the q01 AVG helper; same rounding as the engine).
- `orders`: `OrderGenerator` → `HashSet<i64>` of `o_custkey`.
- **Pass 2 (filter):** over `CustomerGenerator`, keep iff `cntry ∈ CODES` && `c_acctbal_raw * 10_000 >
  avg_threshold_raw` (scale-6 align, ADR 286) && `!orders.contains(c_custkey)` (anti-join). On a customer excluded
  *only* by the anti-join, `with_orders_excluded += 1`. Accumulate `groups[cntry] = (numcust+1, totacctbal +
  c_acctbal_raw)`; `selected_rows += 1`.

## D2 — `check_strict_q22` + `compare_strict_q22` + wiring
Mirror `check_strict_q09`/`q08`:
1. `execute_query_with_tables(STRICT_Q22_SQL, q22_tables(true, sf))` (reuse ADR 304's `q22_tables`); `StageError`
   → classified row.
2. `assert_strict_q22_schema(&batches)` — `cntrycode` Utf8, `numcust` Int64, `totacctbal` `Decimal128(p,2)`; drift
   → `Compare`/`Fail`.
3. Non-vacuity: `selected_rows >= 1 && !groups.is_empty()` at the chosen correctness scale. Record
   `with_orders_excluded`; the synthetic carries anti-join discrimination if this count is 0 live.
4. `compare_strict_q22(&batches, &oracle)`:
   - extract emitted `(cntrycode, numcust, totacctbal)`; `is_null` on every cell → `Err`;
   - **bijection by `cntrycode`**: unique; counts equal; exact `numcust` (`Int64Array`) + `totacctbal`
     (`Decimal128Array`, raw `i128`);
   - **order:** adjacent emitted `cntrycode` ascending (reject violation).
- `main.rs`: route `correctness --query q22` → `check_strict_q22(sf)` (strict-only).
- `correctness_matrix`: q22 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q22_matches_independent_oracle` — `pass@compare` at the chosen correctness scale, expected `sf=0.01`
  (64MB-stack thread; q22 is a deep derived + anti-join + scalar-subquery + group-by).
- `strict_q22_oracle_is_non_vacuous_at_default_scale` — groups present, `selected_rows > 0` at `sf=0.01` if Phase
  0 confirms the default scale; otherwise rename to the chosen scale and record why `sf=0.01` was hollow. Record
  `with_orders_excluded`.
- `strict_q22_output_schema_is_asserted` — accept `Utf8/Int64/Decimal128(p,2)`; reject drift.
- **`q22_not_exists_anti_join_excludes_customers_with_orders`** (synthetic) — same customer with vs without an
  order flips group membership (the FIRST anti-join correctness pin).
- **`q22_avg_threshold_boundary`** (synthetic) — a customer just-below avg excluded, just-above included; the
  oracle's `avg_threshold_raw` == the engine's threshold (`Decimal128(19,6)`, `avg_decimal_raw`).
- `q22_substring_prefix_and_in7_filter` (synthetic) — `cntrycode` = first 2 chars; a non-7 / wrong-prefix excluded.
- `q22_correctness_mismatch_is_fail_at_compare` (wrong numcust/totacctbal);
  `q22_missing_extra_or_duplicate_cntrycode_is_fail`; `q22_null_cell_is_fail`; `q22_order_violation_is_fail`.
- Existing correctness (q01-10/12/14/17/20) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q22
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q22` → `pass@compare` when Phase 0 keeps the default
scale; otherwise include the chosen `--sf`.

## Results doc
`crates/toron/plans/impl/306_q22_correctness_oracle_results.md`: the measured schema (`totacctbal` `Decimal128(p,2)`);
chosen-scale non-vacuity (groups, selected_rows, `with_orders_excluded`, `avg_threshold_raw`; expected
`sf=0.01`); q22 `strict=pass@compare`; boards level (15/15); confirmation prior rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (lying green): the anti-join flip, the
  threshold boundary, the substring/IN-7, the mismatch/missing/dup/null/order tests, and the Phase-0 non-vacuity +
  schema pin are the defenses.
- **The anti-join is the first of its kind** — pin both directions (with-order excluded, without-order included).
- **The AVG threshold** — reuse `avg_decimal_raw` (don't reinvent rounding); the scale-6 comparison align.
- **Masked-empty** — measure the live count; synthetics mandatory regardless.

## Seal
`impl: adr 306 q22 correctness oracle`. Do not commit wip drafts.
