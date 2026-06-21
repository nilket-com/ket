# ADR 298 — Implementation plan: q04 correctness oracle

Pairs with `plans/adr/298_q04_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure + confirm non-vacuity
1. **Schema:** run strict q04 at sf=0.01, capture `batches[0].schema()` → `o_orderpriority` Utf8, `order_count`
   **Int64**. Assert measured.
2. **Non-vacuity (Codex-grounded):** at sf=0.01, `orders_pass_exists` ≈ 535, `orders_fail_exists` ≈ 47, all 5
   priority groups present. Confirm `pass>0` AND `fail>0` AND 5 groups.

## D1 — `strict_q04_oracle` (`crates/toron-tpch/src/lib.rs`)
Reuse the q12 count-grouped pattern.
```rust
pub struct Q04Oracle {
    pub groups: BTreeMap<String, i64>,
    pub orders_pass_exists: usize, pub orders_fail_exists: usize,
}
```
- **EXISTS set (one pass):** `HashSet<i64>` of `l_orderkey` from `LineItemGenerator` where
  `l_commitdate.to_unix_epoch() < l_receiptdate.to_unix_epoch()`.
- **Per `order`** in `OrderGenerator` with `o_orderdate.to_unix_epoch() ∈ [8582, 8674)`
  (`[1993-07-01, 1993-10-01)`; the `+ INTERVAL '3' MONTH` folds):
  - if `o_orderkey ∈ EXISTS set` → `*groups.entry(o_orderpriority).or_default() += 1`; `orders_pass_exists += 1`;
  - else `orders_fail_exists += 1`.
- **Count orders once** — the per-order set-membership test enforces the semijoin (an order's multiple qualifying
  lineitems collapse to one set entry).

## D2 — `check_strict_q04` + `compare_strict_q04` + wiring
Mirror `check_strict_q12`:
1. `execute_query_with_tables(STRICT_Q4_SQL, q04_tables(true, sf))` (reuse q04's reach table set); `StageError`
   → classified row.
2. `assert_strict_q04_schema(&batches)` — `o_orderpriority` Utf8 + `order_count` Int64; drift → `Compare`/`Fail`.
3. Non-vacuity: `oracle.orders_pass_exists >= 1 && oracle.orders_fail_exists >= 1 && !oracle.groups.is_empty()`
   → else `Fail` with counts.
4. `compare_strict_q04(&batches, &oracle)`:
   - extract emitted `(o_orderpriority, order_count)`; `is_null` on both cells → `Err`;
   - **bijection by `o_orderpriority`**: unique; row counts equal; exact `order_count` (`Int64Array`);
   - **order:** emitted `o_orderpriority` strictly ascending (total order — reject violation, no tie tolerance).
- `main.rs`: route `correctness --query q04|q4` → `check_strict_q04(sf)`. The correctness command is already
  strict-only; do not add or require a `--mode` flag.
- `correctness_matrix`: q04 renders `strict=pass@compare`.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q04_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread if needed).
- `strict_q04_oracle_is_non_vacuous_at_default_scale` — `orders_pass_exists > 0`, `orders_fail_exists > 0`, **all
  5 priority groups present** (each count > 0).
- `strict_q04_output_schema_is_asserted` — accept Utf8/Int64; reject drift.
- **`q04_exists_counts_order_once`** (synthetic semijoin tripwire) — one order with **two** qualifying lineitems
  → `order_count == 1`, not 2.
- **`q04_order_failing_exists_is_excluded`** (synthetic EXISTS tripwire) — a date-window order whose lineitems all
  have `l_commitdate >= l_receiptdate` (or none) is **not** counted.
- `q04_correctness_mismatch_is_fail_at_compare` — wrong count → fail.
- `q04_missing_extra_or_duplicate_group_is_fail_at_compare`; `q04_null_cell_is_fail`; `q04_order_violation_is_fail`.
- Existing correctness (q01/q02/q03/q05/q06/q10/q12/q14/q17/q20) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q04
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q04` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/298_q04_correctness_oracle_results.md`: the measured schema (`order_count` Int64); the
sf=0.01 non-vacuity counts (orders_pass/fail, the 5 group counts); q04 `strict=pass@compare`; confirmation prior
reach + correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the order-once + EXISTS-
  excludes + mismatch + missing/extra/dup + null + order tests, the discriminating non-vacuity, and the Phase-0
  schema pin are the defenses.
- **EXISTS = semijoin** — count orders once; the qualifying-*order* set enforces it (don't count lineitems).
- **EXISTS discriminating** — `pass>0` AND `fail>0` (grounded 535/47).

## Seal
`impl: adr 298 q04 correctness oracle`. Do not commit wip drafts.
