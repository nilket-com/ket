# ADR 295 — Implementation plan: q10 correctness oracle

Pairs with `plans/adr/295_q10_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure schema + non-vacuity (don't bake)
1. **Measure the live schema.** Run strict q10 at sf=0.01, capture `batches[0].schema()`. Prior: `c_custkey
   Int64`, `c_name Utf8`, `revenue Decimal128(32,4)`, `c_acctbal Decimal128(15,2)`, `n_name`/`c_address`/`c_phone`/
   `c_comment` Utf8. Assert the *measured* types.
2. **Measure non-vacuity:** `selected_rows > 0` and `rows.len() > 0`; record the row count (if naturally >1, pin
   `rows.len() > 1`). `selected_rows` counts lineitems that actually contribute to a q10 group, not merely
   date-filtered orders.

## D1 — `strict_q10_oracle` (`crates/toron-tpch/src/lib.rs`)
Mirror `strict_q5_oracle`'s grouped multi-table traversal.
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q10Row {
    pub c_custkey: i64, pub c_name: String, pub revenue: i128, pub c_acctbal: i128,
    pub n_name: String, pub c_address: String, pub c_phone: String, pub c_comment: String,
}
pub struct Q10Oracle { pub rows: Vec<Q10Row>, pub selected_rows: usize }
```
- `NationGenerator` → `HashMap<i64, String>` (`n_nationkey → n_name`).
- `CustomerGenerator` → `c_custkey → (c_name, c_acctbal_raw [into_inner, scale 2], c_nationkey, c_address,
  c_phone, c_comment)`.
- `OrderGenerator` → keep `o_orderdate.to_unix_epoch() ∈ [day(1993-10-01), day(1994-01-01))`; map
  `o_orderkey → o_custkey` (date-filtered order map only; not the non-vacuity proof).
- `LineItemGenerator` → keep `l_returnflag == "R"`; resolve order→customer→nation; accumulate
  `discounted_price_raw(l_extendedprice.into_inner(), l_discount.into_inner())` (raw scale 4) into a
  `BTreeMap<i64, accum>` keyed by `c_custkey` (carry the customer/nation attributes once); increment
  `selected_rows` only after the lineitem contributes to a group.
- Emit a `Q10Row` per customer; sort by `revenue` DESC for the order check (diagnostic order only — equality is
  via the multiset/bijection).

## D2 — `check_strict_q10` + `compare_strict_q10` + wiring
Mirror `check_strict_q5`:
1. `execute_query_with_tables(STRICT_Q10_SQL, q10_tables(true, sf))` (reuse q10's existing reach table set);
   `StageError` → classified row.
2. `assert_strict_q10_schema(&batches)` — the 8 columns at the Phase-0-measured types; drift → `Compare`/`Fail`.
3. Non-vacuity: `oracle.selected_rows >= 1 && !oracle.rows.is_empty()` → else `Fail`.
4. `compare_strict_q10(&batches, &oracle)`:
   - extract emitted `Q10Row`s; `is_null` on **every** cell → `Err`;
   - **answer = bijection keyed by `c_custkey`**: each emitted custkey unique; row counts equal; every projected
     column matches exactly (raw `i128` `revenue`+`c_acctbal`, `String`/`i64` rest); **not** a key-only check —
     compare all 8 columns;
   - **order = `revenue DESC` prefix**: adjacent emitted rows satisfy `revenue[i] >= revenue[i+1]`; allow
     equal-revenue ties to permute (no hidden tie-breaker).
- `main.rs`: route `correctness --query q10` → `check_strict_q10(sf)` (strict-only).
- `correctness_matrix`: q10 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q10_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread, like q02/q05).
- `strict_q10_oracle_is_non_vacuous_at_default_scale` — `selected_rows > 0 && !rows.is_empty()`.
- `strict_q10_output_schema_is_asserted` — accept measured; reject precision/name/count drift.
- `q10_correctness_mismatch_is_fail_at_compare` — wrong `revenue`, wrong `c_acctbal`, wrong string attribute → fail.
- `q10_missing_extra_or_duplicate_row_is_fail_at_compare` — missing row, extra row, duplicate `c_custkey` → fail.
- `q10_null_cell_is_fail_at_compare` — NULL in each of the 8 columns → fail.
- `q10_order_violation_is_fail` + `q10_accepts_tied_revenue_permutation` — DESC prefix enforced; ties permute.
- Existing correctness (q01/q02/q03/q05/q06/q12/q14/q17) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q10
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q10` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/295_q10_correctness_oracle_results.md`: the measured 8-column schema (`revenue
Decimal128(32,4)`, `c_acctbal Decimal128(15,2)`, `c_custkey Int64`); sf=0.01 non-vacuity counts (selected_rows,
rows); q10 `strict=pass@compare`; confirmation prior reach + correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the per-class mismatch +
  duplicate/missing/extra + null + order-violation tests, the non-vacuity guard, and the Phase-0 schema pin are
  the defenses.
- **Key by `c_custkey` but check all 8 columns** — a key-only compare would mask a wrong attribute.
- **DESC tie-tolerant** — don't pin a hidden tie-breaker (q05's lesson).
- Exact raw Decimal equality (no float/format/tolerance) for `revenue` + `c_acctbal`.

## Seal
`impl: adr 295 q10 correctness oracle`. Do not commit wip drafts.
