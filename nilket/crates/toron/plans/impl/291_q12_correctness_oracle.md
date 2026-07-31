# ADR 291 — Implementation plan: q12 correctness oracle

Pairs with `plans/adr/291_q12_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure and verify (don't bake)
1. **Measure the live count schema.** Run strict q12 at sf=0.01 and capture `batches[0].schema()`. Prior:
   `l_shipmode` Utf8, `high_line_count` / `low_line_count` **`Int64`** (`CASE → Int32`, `SUM(Int32) → Int64`).
   Assert the *measured* types; never bake a guess.
2. **Verify non-vacuity at sf=0.01:** both `MAIL` and `SHIP` groups present, `selected_rows > 0`,
   `high_rows > 0`, `low_rows > 0`. Also record whether **per-group** strictly-positive holds (each mode has
   high>0 AND low>0) — if it does, strengthen the guard per group; if not, keep the global floor. Don't assume.

## D1 — `strict_q12_oracle` (`crates/toron-tpch/src/lib.rs`)
Mirror the `strict_q5_oracle` grouped traversal pattern — independent generator traversal, then a `BTreeMap`
grouped by the SQL output key.
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Q12Group { pub high_line_count: i64, pub low_line_count: i64 }
pub struct Q12Oracle {
    pub groups: BTreeMap<String, Q12Group>,
    pub selected_rows: usize, pub high_rows: usize, pub low_rows: usize,
}
```
- `orders_by_key: HashMap<i64, String>` from `OrderGenerator` (`o_orderkey → o_orderpriority`).
- Per `LineItemGenerator` row: `l_shipmode == "MAIL" || "SHIP"`; `l_commitdate < l_receiptdate`;
  `l_shipdate < l_commitdate`; `l_receiptdate ∈ [8766, 9131)` (Date32 day-counts via `to_unix_epoch`, as
  `strict_q5_oracle` does); look up `o_orderpriority` by `l_orderkey` (skip if absent).
- Group by `l_shipmode`; `high = o_orderpriority == "1-URGENT" || == "2-HIGH"`, else `low`; increment the group's
  `high_line_count`/`low_line_count` (i64) + the global `selected_rows`/`high_rows`/`low_rows`.

## D2 — `check_strict_q12` + `compare_strict_q12` + wiring
Mirror the existing correctness runners:
1. `execute_query_with_tables(STRICT_Q12_SQL, q12_tables(true, sf))` (reuse ADR 290's q12 table set);
   `StageError` → classified row.
2. `assert_strict_q12_schema(&batches)` — `l_shipmode` Utf8 + the two count columns at the **measured** `Int64`;
   drift → `Compare`/`Fail`.
3. Non-vacuity: both groups + `selected_rows >= 1 && high_rows >= 1 && low_rows >= 1` → else `Fail` w/ counts
   (per-group strictly-positive if Phase-0 confirmed).
4. `compare_strict_q12(&batches, &oracle)`:
   - extract emitted `(l_shipmode, high, low)` rows; `is_null` on every cell → `Err`;
   - row count == `oracle.groups.len()`; no duplicate `l_shipmode`;
   - exact `(high_line_count, low_line_count)` per group (bijection by `l_shipmode`);
   - **emitted order == sorted `l_shipmode` (BTreeMap order)** — reject out-of-order (total ordering, no tie
     tolerance).
- `main.rs`: route `correctness --query q12` → `check_strict_q12(sf)` (strict-only; default sf=0.01).
- `correctness_matrix`: q12 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q12_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread if deep).
- `strict_q12_oracle_is_non_vacuous_at_default_scale` — both groups, `selected_rows/high_rows/low_rows > 0`.
- `strict_q12_output_schema_is_asserted` — accept measured `Int64`; reject a drifted type.
- `q12_correctness_mismatch_is_fail_at_compare` — wrong high or low count → fail.
- `q12_high_low_swapped_is_fail_at_compare` — swapping a group's high/low (misclassification) → fail (the
  load-bearing classification tripwire).
- `q12_missing_or_duplicate_group_is_fail_at_compare`; `q12_null_cell_is_fail_at_compare`;
  `q12_out_of_order_groups_is_fail_at_compare`.
- Existing correctness (q01/q03/q05/q06/q14/q17) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q12
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q12` → `pass@compare`. The correctness rail is
strict-only, so no `--mode` flag is needed.

## Results doc
`crates/toron/plans/impl/291_q12_correctness_oracle_results.md`: the measured count schema (`Int64`); sf=0.01
non-vacuity counts (both groups, high_rows/low_rows, per-group-positive?); q12 `strict=pass@compare`;
confirmation prior reach + correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the high/low-swap +
  mismatch + out-of-order + null + both-arms-non-vacuity tests are the defenses.
- **Measure the count type** (`Int64` prior) — assert the live schema, don't bake.
- The two **column-to-column** date inequalities are q12-specific — replicate exactly (mirror every gate).

## Seal
`impl: adr 291 q12 correctness oracle`. Do not commit wip drafts.
