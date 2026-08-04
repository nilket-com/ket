# ADR 303 — Implementation plan: q09 correctness oracle

Pairs with `plans/adr/0303_q09_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure schema + non-vacuity (incl. negative amounts)
1. **Schema:** run strict q09 at sf=0.01, capture `batches[0].schema()` → `nation` Utf8, `o_year` **Int32**,
   `sum_profit` **`Decimal128(33,4)`** (grounded; `amount` = `(32,4)−(31,4)` → `(33,4)` per ADR 302). Assert it.
2. **Non-vacuity:** many `(nation, o_year)` groups (no date filter). Measure `negative_amount_rows`. If it is
   non-zero, assert it; if it is zero at sf=0.01, record that and rely on the mandatory synthetic negative-amount
   test for sign discrimination. Do not make q09 fail solely because the live generator distribution lacks a
   negative per-row amount.

## D1 — `strict_q09_oracle` (`crates/toron-tpch/src/lib.rs`)
Reuse the q05/q07 grouped traversal.
```rust
pub struct Q09Oracle {
    pub groups: BTreeMap<(String, i32), i128>,    // (nation, o_year) -> sum_profit (raw scale-4, signed)
    pub selected_rows: usize, pub negative_amount_rows: usize,
}
```
- `green` parts: `PartGenerator` → `p_partkey` where `p_name.to_string().contains("green")`.
- `partsupp`: `PartSuppGenerator` → `HashMap<(i64,i64), i128>` `(ps_partkey, ps_suppkey) → ps_supplycost_raw`.
- `nation`: `n_nationkey → n_name`; `supplier`: `s_suppkey → s_nationkey`; `orders`: `o_orderkey → o_orderdate`.
  Add/reuse a general Date32-to-calendar-year helper matching `EXTRACT(year FROM o_orderdate)`; q07/q08's helper is
  intentionally bounded to 1995/1996 and is not valid for q09's all-year query.
- Per `LineItemGenerator`: require `l_partkey ∈ green`, `partsupp.get((l_partkey, l_suppkey))`, and
  `orders.get(l_orderkey)`:
  - `revenue_raw = discounted_price_raw(l_extendedprice.into_inner(), l_discount.into_inner())` (scale 4);
  - `cost_raw = ps_supplycost_raw * (l_quantity.into_inner() as i128)` (scale 4) — verify `l_quantity` raw scale
    is 2 (strict fixture); use the same raw shape;
  - `amount_raw = revenue_raw - cost_raw` (signed); if `< 0` → `negative_amount_rows += 1`;
  - `nation = nation[supplier[l_suppkey]]`; `o_year = q09_order_year(o_orderdate.to_unix_epoch())` (i32);
  - `*groups.entry((nation, o_year)).or_default() += amount_raw`; `selected_rows += 1`.

## D2 — `check_strict_q09` + `compare_strict_q09` + wiring
Mirror `check_strict_q05`/`q07`:
1. `execute_query_with_tables(STRICT_Q9_SQL, q09_tables(true, sf))` (reuse ADR 301's `q09_tables`); `StageError`
   → classified row.
2. `assert_strict_q09_schema(&batches)` — `nation` Utf8, `o_year` Int32, `sum_profit` `Decimal128(33,4)`; drift →
   `Compare`/`Fail`.
3. Non-vacuity: `selected_rows >= 1 && groups.len() > 1`; record `negative_amount_rows`; assert it only if Phase 0
   measured live negatives. The synthetic sign test is mandatory regardless, and is the backstop if live rows are
   all positive.
4. `compare_strict_q09(&batches, &oracle)`:
   - extract emitted `(nation, o_year, sum_profit)`; `is_null` on every cell → `Err`;
   - **bijection by `(nation, o_year)`**: unique; row counts equal; exact **signed** `i128` `sum_profit`
     (`Decimal128Array`);
   - **order:** adjacent emitted rows respect `nation ASC, o_year DESC` (reject violation).
- `main.rs`: route `correctness --query q09|q9` → `check_strict_q09(sf)` (strict-only).
- `correctness_matrix`: q09 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q09_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread; q09 is a deep 6-table
  derived + grouped aggregate).
- `strict_q09_oracle_is_non_vacuous_at_default_scale` — multiple groups, `selected_rows > 0`,
  `negative_amount_rows` recorded; assert it only when Phase 0 measures live negatives.
- `strict_q09_output_schema_is_asserted` — accept `Utf8/Int32/Decimal128(33,4)`; reject drift.
- **`q09_amount_is_revenue_minus_cost_signed`** (synthetic) — a row where `cost > revenue` → **negative** amount;
  the oracle/compare carry the sign (proves the ADR 302 subtraction + signed SUM).
- **`q09_partsupp_composite_key_picks_right_cost`** (synthetic) — two partsupp rows for the same part, different
  suppliers/costs; the lookup uses `(partkey, suppkey)`, not partkey alone.
- `q09_green_is_contains_match` — `.contains("green")`, not prefix/suffix.
- `q09_correctness_mismatch_is_fail_at_compare` (wrong sum_profit); `q09_missing_extra_or_duplicate_group_is_fail`;
  `q09_null_cell_is_fail`; `q09_order_violation_is_fail` (mixed nation-ASC/o_year-DESC).
- Existing correctness (q01-08/10/12/14/17/20) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q09
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q09` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/0303_q09_correctness_oracle_results.md`: the measured schema (`sum_profit`
`Decimal128(33,4)`); sf=0.01 non-vacuity (groups, selected_rows, **negative_amount_rows**, and whether live or
synthetic sign discrimination is carrying the negative case); q09 `strict=pass@compare`; that the boards are
level (14/14); confirmation prior rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the signed-amount +
  composite-key + contains + mismatch + missing/dup + null + order tests, the live/synthetic negative
  discrimination, and the Phase-0 schema pin are the defenses.
- **Signed subtraction** — `amount = revenue − cost` can be negative; the oracle + comparison use signed `i128`.
- **Composite partsupp key** — `(partkey, suppkey)`, not partkey alone.
- **`%green%` = contains**, not prefix/suffix.

## Seal
`impl: adr 303 q09 correctness oracle`. Do not commit wip drafts.
