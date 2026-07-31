# ADR 296 — Implementation plan: q20 correctness oracle

Pairs with `plans/adr/296_q20_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure schema + threshold scale + absent-group coverage + discrimination
1. **Schema:** run strict q20 at sf=0.01, capture `batches[0].schema()` → `s_name` Utf8, `s_address` Utf8. Assert
   measured.
2. **Threshold scale:** confirm the grounded predicate `ps_availqty * 10_000 > 50 * sum_qty_raw` (0.5 coerced to
   `Decimal128(15,2)` → `0.5*SUM` is `Decimal128(31,4)`, scale 4; ADR 286 aligns `ps_availqty` → `*10_000`). The
   live oracle-vs-engine test is the cross-validator — if the qualifying set diverges, the scale is wrong.
3. **Absent-group `SUM`:** measure whether the live data has forest partsupp rows with **no** matching 1994
   lineitems. The oracle always uses SQL-standard semantics: `SUM(∅) = NULL` → `ps_availqty > NULL` → row
   **fails**. Do not mirror a hypothetical engine-side `sum=0` bug; if live data exposes such a divergence, the
   correctness row should fail at compare.
4. **Discrimination:** record `selected_partsupp_rows`, `threshold_pass_rows`, and `threshold_fail_rows` at
   sf=0.01. If `fail_rows == 0` (every forest partsupp passes), document it and make the synthetic boundary test
   mandatory; do not let the synthetic test substitute for a non-empty live q20 answer.

## D1 — `strict_q20_oracle` (`crates/toron-tpch/src/lib.rs`)
Mirror `strict_q5_oracle` traversal.
```rust
pub struct Q20Row {
    pub s_name: String,
    pub s_address: String,
}

pub struct Q20Oracle {
    pub rows: Vec<Q20Row>,                     // sorted by s_name
    pub selected_partsupp_rows: usize,
    // Distinct ps_suppkeys passing the inner membership threshold, before the outer CANADA filter.
    pub qualifying_suppliers: usize,
    pub threshold_pass_rows: usize,
    pub threshold_fail_rows: usize,
}
```
- `PartGenerator` → forest `p_partkey` set (`p_name.starts_with("forest")`).
- `NationGenerator` → CANADA `n_nationkey`; `SupplierGenerator` → CANADA `s_suppkey → (s_name, s_address)`.
- `LineItemGenerator` → for `l_shipdate ∈ [day(1994-01-01), day(1995-01-01))`, accumulate `sum_qty_raw` (scale 2)
  by `(l_partkey, l_suppkey)`.
- `PartSuppGenerator` → for `ps_partkey ∈ forest`: increment `selected_partsupp_rows`, then look up
  `sum_qty_raw` for `(ps_partkey, ps_suppkey)`:
  - **absent group** → SQL-standard NULL → **fail**; increment `threshold_fail_rows`; do not default to
    0-and-pass;
  - else keep iff `ps_availqty as i128 * 10_000 > 50 * sum_qty_raw` (exact, strict `>`); tally
    `threshold_pass_rows`/`threshold_fail_rows`; on pass add `ps_suppkey` to the qualifying set.
- Qualifying CANADA suppliers → `(s_name, s_address)`; sort by `s_name`.

## D2 — `check_strict_q20` + `compare_strict_q20` + wiring
Mirror `check_strict_q5`:
1. `execute_query_with_tables(STRICT_Q20_SQL, q20_tables(true, sf))` (reuse ADR 285's `q20_tables`); `StageError`
   → classified row.
2. `assert_strict_q20_schema(&batches)` — `s_name`/`s_address` Utf8; drift → `Compare`/`Fail`.
3. Non-vacuity: `!rows.is_empty()` **and** `selected_partsupp_rows >= 1` **and** `threshold_pass_rows >= 1`.
   If `threshold_fail_rows == 0` live, document it and require the synthetic boundary test; if it is naturally
   non-zero, assert it in `strict_q20_oracle_is_non_vacuous_at_default_scale`.
4. `compare_strict_q20(&batches, &oracle)`:
   - extract emitted `(s_name, s_address)`; `is_null` on both cells → `Err`;
   - **bijection by `s_name`** (unique); row counts equal; no duplicate; `s_address` matches per `s_name`;
   - **order:** emitted `s_name` non-decreasing (reject violation).
- `main.rs`: route `correctness --query q20` → `check_strict_q20(sf)` (strict-only).
- `correctness_matrix`: q20 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q20_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread; q20 is deep).
- `strict_q20_oracle_is_non_vacuous_at_default_scale` — non-empty rows, `selected_partsupp_rows > 0`,
  `threshold_pass_rows > 0`, and measured `threshold_fail_rows` behavior recorded.
- `strict_q20_output_schema_is_asserted` — accept Utf8/Utf8; reject drift.
- **`q20_threshold_boundary_flip`** (synthetic) — a partsupp with `ps_availqty` just above vs at/below
  `0.5*SUM` flips inclusion (strict `>`); proves the oracle mirrors the comparison.
- **`q20_absent_lineitem_group_follows_sql_null_rule`** (synthetic) — a forest partsupp with no matching
  lineitems is excluded (NULL→fail), not included.
- `q20_correctness_mismatch_is_fail_at_compare` — extra/missing/duplicate `s_name`, wrong `s_address` → fail.
- `q20_forest_is_prefix_match` — `starts_with("forest")`, not contains.
- `q20_null_cell_is_fail` + `q20_order_violation_is_fail`.
- Existing correctness (q01/q02/q03/q05/q06/q10/q12/q14/q17) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q20
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q20` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/296_q20_correctness_oracle_results.md`: the measured schema; the threshold predicate;
absent-group coverage/semantics; sf=0.01 non-vacuity counts (rows, selected partsupp rows, pass/fail rows); q20
`strict=pass@compare`; confirmation prior reach + correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the threshold-boundary +
  absent-group + forest-prefix + set-mismatch + order tests, the discriminating non-vacuity, and the Phase-0
  schema/scale/semantics measurements are the defenses.
- **Threshold scale** — `*10_000` / `*50` (scale 4), not f64, not scale 3; cross-validated by the live test.
- **Absent-group `SUM`** — SQL NULL→fail, don't bake `sum=0` and don't mirror an engine-side 0 bug. This is the
  subtlest correctness point in q20.

## Seal
`impl: adr 296 q20 correctness oracle`. Do not commit wip drafts.
