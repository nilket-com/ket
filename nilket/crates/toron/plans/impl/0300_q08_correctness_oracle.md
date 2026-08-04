# ADR 300 — Implementation plan: q08 correctness oracle

Pairs with `plans/adr/0300_q08_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree. The gap-closer (rail → 13, boards level).

## Phase 0 — measure (the two ungrounded facts) + remove any probe
1. **`mkt_share` schema:** run strict q08 at sf=0.01, capture `batches[0].schema()` → `o_year` Int32, `mkt_share`
   `Decimal128(p, s)`. Also measure the Decimal type of `SUM(volume)`/`SUM(CASE ...)`. **Measure these types —
   don't bake them from prose.**
2. **`brazil_rows`:** measure whether the filtered set has any BRAZIL-supplier rows. If `brazil_rows == 0`, the
   `CASE`-true / numerator is hollow live → the synthetic ratio + CASE tests are **mandatory** (record the fact).
3. Confirm `denominator_rows > 0` and 2 year groups (1995/1996). **Leave no temp probe in the tree.**

## D1 — `strict_q08_oracle` (`crates/toron-tpch/src/lib.rs`)
Reuse q14's ratio + q07's self-join traversal.
```rust
pub struct Q08Oracle {
    pub groups: BTreeMap<i32, DecimalRaw>,      // o_year -> mkt_share (reuse ADR 288 DecimalRaw)
    pub denominator_rows: usize, pub brazil_rows: usize,
}
```
- Lookups: `NationGenerator` `n_nationkey → (n_name, n_regionkey)`; `RegionGenerator` AMERICA `r_regionkey`;
  `PartGenerator` `p_type == "ECONOMY ANODIZED STEEL"` → partkey set; `SupplierGenerator`
  `s_suppkey → s_nationkey`; `CustomerGenerator` `c_custkey → c_nationkey`; `OrderGenerator`
  `o_orderkey → (o_custkey, o_orderdate)` for `o_orderdate ∈ [day(1995-01-01), day(1996-12-31)]` inclusive.
- Per `LineItemGenerator`: require `l_partkey ∈ ECONOMY-ANODIZED-STEEL parts`, order in window; **customer's
  nation (n1) in AMERICA region** (`customer[o_custkey].c_nationkey → nation.n_regionkey ∈ AMERICA`); else skip.
  - `volume = discounted_price_raw` (raw scale 4); `o_year = year(o_orderdate)` (i32);
  - `den_raw[o_year] += volume`; `denominator_rows += 1`;
  - `n2 = nation[supplier[l_suppkey].s_nationkey].n_name`; if `n2 == "BRAZIL"` → `num_raw[o_year] += volume`;
    `brazil_rows += 1`.
- Per `o_year`: `groups[o_year] = decimal_division_raw(num_raw, measured_sum_p, 4, den_raw, measured_sum_p, 4)`;
  assert the returned `DecimalRaw { precision, scale }` matches the Phase-0 measured `mkt_share` type.

## D2 — `check_strict_q08` + `compare_strict_q08` + wiring
Mirror `check_strict_q14`/`q05`:
1. `execute_query_with_tables(STRICT_Q8_SQL, q08_tables(true, sf))` (reuse ADR 297's `q08_tables`); `StageError`
   → classified row.
2. `assert_strict_q08_schema(&batches)` — `o_year` Int32 + `mkt_share` measured `Decimal128(p,s)`; drift →
   `Compare`/`Fail`.
3. Non-vacuity: 2 year groups, `denominator_rows >= 1`; assert `brazil_rows` (record; if 0, rely on synthetics).
4. `compare_strict_q08(&batches, &oracle)`:
   - extract emitted `(o_year, mkt_share)`; `is_null` on both cells → `Err`;
   - **bijection by `o_year`**: exact `mkt_share` raw `i128` (`Decimal128Array`); row counts equal;
   - **order:** emitted `o_year` ascending (total order — reject violation).
- `main.rs`: route `correctness --query q08|q8` → `check_strict_q08(sf)` (strict-only; no `--mode` argument).
- `correctness_matrix`: q08 renders `strict=pass@compare`.
- Reachability `status_rows` already lists q08 and stays untouched.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q08_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread; q08 is deepest).
- `strict_q08_oracle_is_non_vacuous_at_default_scale` — 2 year groups, `denominator_rows > 0`, `brazil_rows`
  measured and recorded; if it is zero, the synthetic numerator tests are load-bearing.
- `strict_q08_output_schema_is_asserted` — accept measured `Int32`/`Decimal128(p,s)`; reject drift.
- **`q08_ratio_uses_engine_decimal_division`** (synthetic) — `decimal_division_raw` on a crafted num/den where
  truncation would differ (the q14/q17 division discipline).
- **`q08_self_join_customer_vs_supplier_not_conflated`** (synthetic) — data where region-filtering the supplier or
  CASE-checking the customer would change the result; the oracle distinguishes.
- **`q08_brazil_numerator_group`** (synthetic) — a group with BRAZIL volume → `mkt_share = num/den` (proves the
  CASE-true + ratio, since live is likely hollow).
- `q08_correctness_mismatch_is_fail_at_compare`; `q08_missing_extra_group_is_fail`; `q08_null_cell_is_fail`;
  `q08_order_violation_is_fail`.
- Existing correctness (q01-07/10/12/14/17/20) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q08
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q08` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/0300_q08_correctness_oracle_results.md`: the measured `mkt_share` `(p,s)`; sf=0.01
non-vacuity (year groups, denominator_rows, **brazil_rows** — hollow or not); q08 `strict=pass@compare`; that the
boards are level (13/13); confirmation prior rows unchanged; no temp probe left.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the ratio-division +
  self-join-conflation + BRAZIL-numerator + filter + mismatch + null + order tests, the discriminating
  non-vacuity, and the Phase-0 schema/brazil measurements are the defenses.
- **The nation self-join** (customer→AMERICA / supplier→BRAZIL) — the subtlest q08 point; don't conflate.
- **The ratio** — `decimal_division_raw` + the measured output type; no float/percent/row-ratio.
- **Likely-hollow BRAZIL numerator** — synthetics carry the ratio + CASE-true if live `brazil_rows == 0`.

## Seal
`impl: adr 300 q08 correctness oracle`. Do not commit wip drafts.
