# ADR 299 — Implementation plan: q07 correctness oracle

Pairs with `plans/adr/0299_q07_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree. Schema/non-vacuity already grounded (temp probe removed).

## Phase 0 — confirm grounded facts (no temp probe left behind)
- Schema (grounded): `supp_nation` Utf8, `cust_nation` Utf8, `l_year` **Int32**, `revenue` **Decimal128(32,4)**.
  Assert it (drift → fail).
- Non-vacuity (grounded): sf=0.01 → 46 contributing rows, 4 groups (both directions × both years). Confirm.
- Ensure no `tmp_q07_*` probe remains in `lib.rs` (the grounding probe was removed — keep the seal clean).

## D1 — `strict_q07_oracle` (`crates/toron-tpch/src/lib.rs`)
Reuse q05's grouped-revenue traversal.
```rust
pub struct Q07Oracle { pub groups: BTreeMap<(String, String, i32), i128>, pub selected_rows: usize }
```
- `NationGenerator` → `n_nationkey → n_name`; `SupplierGenerator` → `s_suppkey → s_nationkey`;
  `CustomerGenerator` → `c_custkey → c_nationkey`; `OrderGenerator` → `o_orderkey → o_custkey`.
- Per `LineItemGenerator`: `shipdate = l_shipdate.to_unix_epoch()`; keep `9131 <= shipdate <= 9861` (inclusive
  `BETWEEN`; equivalently `shipdate < 9862`).
  - `supp_nation = nation[supplier[l_suppkey]]`; `cust_nation = nation[customer[orders[l_orderkey]]]` (skip if any
    lookup misses).
  - keep iff `(supp_nation, cust_nation) == ("FRANCE","GERMANY") || == ("GERMANY","FRANCE")`.
  - `l_year` = the calendar year of `l_shipdate` (1995 or 1996) as `i32`.
  - `revenue += discounted_price_raw(l_extendedprice, l_discount)` (raw scale 4) into
    `groups[(supp_nation, cust_nation, l_year)]`; `selected_rows += 1`.

## D2 — `check_strict_q07` + `compare_strict_q07` + wiring
Mirror `check_strict_q05`:
1. `execute_query_with_tables(STRICT_Q7_SQL, q07_tables(true, sf))` (reuse ADR 293's `q07_tables`); `StageError`
   → classified row.
2. `assert_strict_q07_schema(&batches)` — the 4 columns at `Utf8/Utf8/Int32/Decimal128(32,4)`; drift →
   `Compare`/`Fail`.
3. Non-vacuity: `oracle.selected_rows >= 1 && oracle.groups.len() >= 1` (grounded 4 groups; assert the 4-group
   completeness in the non-vacuity test).
4. `compare_strict_q07(&batches, &oracle)`:
   - extract emitted `(supp_nation, cust_nation, l_year, revenue)`; `is_null` on every cell → `Err`;
   - **bijection by `(supp_nation, cust_nation, l_year)`**: unique; row counts equal; exact raw `i128` `revenue`
     (`Decimal128Array`), exact `String`/`i32`;
   - **order:** emitted rows ascending by `(supp_nation, cust_nation, l_year)` (total order — reject violation).
- `main.rs`: route `correctness --query q07|q7` → `check_strict_q07(sf)` (strict-only; no `--mode` argument).
- `correctness_matrix`: q07 renders `strict=pass@compare`.
- Reachability `status_rows` already lists q07 and stays untouched.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q07_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread; q07 is deep).
- `strict_q07_oracle_is_non_vacuous_at_default_scale` — `selected_rows > 0`, 4 groups (both pairs × both years).
- `strict_q07_output_schema_is_asserted` — accept `Utf8/Utf8/Int32/Decimal128(32,4)`; reject drift.
- **`q07_supp_cust_nation_not_swapped`** (synthetic self-join tripwire) — asymmetric data where swapping
  supp/cust would change the per-group revenue; the oracle/compare distinguishes them.
- **`q07_between_is_inclusive_upper_bound`** (synthetic) — a 1996-12-31 lineitem is included; a 1997-01-01 one is
  excluded.
- `q07_disjunctive_pair_filter_excludes_other_pairs` (synthetic) — a non-{FR/GER} pair is dropped.
- `q07_correctness_mismatch_is_fail_at_compare` (wrong revenue), `q07_missing_extra_or_duplicate_group_is_fail`,
  `q07_null_cell_is_fail`, `q07_order_violation_is_fail`.
- Existing correctness (q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q07
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q07` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/0299_q07_correctness_oracle_results.md`: the schema (`l_year` Int32, `revenue`
Decimal128(32,4)); sf=0.01 non-vacuity (46 rows, 4 groups, both directions/years); q07 `strict=pass@compare`;
confirmation prior reach + correctness rows unchanged; **no temp probe left in the tree**.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the swap + inclusive-
  boundary + disjunction + mismatch + missing/extra/dup + null + order tests, the non-vacuity guard, and the
  schema pin are the defenses.
- **Self-join: don't swap** supp (supplier) vs cust (customer).
- **`BETWEEN` inclusive** — `<= 9861` (1996-12-31 in).
- `revenue` exact raw `i128` at scale 4 (no float/format/tolerance).

## Seal
`impl: adr 299 q07 correctness oracle`. Do not commit wip drafts.
