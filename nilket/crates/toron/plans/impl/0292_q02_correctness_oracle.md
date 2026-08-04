# ADR 292 — Implementation plan: q02 correctness oracle

Pairs with `plans/adr/0292_q02_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Anchors are current-tree.

## Phase 0 — measure and record (don't bake)
1. **Measure the live schema.** Run strict q02 at sf=0.01 and capture `batches[0].schema()`. Priors:
   `s_acctbal Decimal128(15,2)`, `p_partkey` **`Int64`**, the other six `Utf8`. Assert the *measured* types;
   reject the `Int32` `p_partkey` variant.
2. **Record (don't assert as invariant):** the emitted row count (small at sf=0.01) and `min_tie_parts` (parts
   with ≥2 EUROPE suppliers at the per-part min). If `min_tie_parts == 0`, the live fixture won't exercise the
   tie path — keep the oracle tie-correct and cover the policy with synthetic comparator tests (below).

## D1 — `strict_q02_oracle` (`crates/toron-tpch/src/lib.rs`)
Mirror the `strict_q5_oracle` multi-table traversal.
```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Q02Row {
    pub s_acctbal: i128, pub s_name: String, pub n_name: String, pub p_partkey: i64,
    pub p_mfgr: String, pub s_address: String, pub s_phone: String, pub s_comment: String,
}
pub struct Q02Oracle { pub rows: Vec<Q02Row>, pub matched_parts: usize, pub min_tie_parts: usize }
```
- `RegionGenerator` → EUROPE `r_regionkey` set; `NationGenerator` → `n_nationkey → n_name` for EUROPE regions.
- `SupplierGenerator` → for EUROPE-nation suppliers, `s_suppkey → (s_acctbal_raw, s_name, n_name, s_address,
  s_phone, s_comment)`.
- Add a tiny public predicate helper (`pub fn q02_type_matches_brass`) so the suffix semantics are unit-testable
  from the integration correctness test. `PartGenerator` → for `p_size == 15 && q02_type_matches_brass(p_type)`,
  `p_partkey → p_mfgr`.
- `PartSuppGenerator` → for eligible `(ps_partkey ∈ parts, ps_suppkey ∈ EUROPE suppliers)`: first pass computes
  `min_cost[ps_partkey] = MIN(ps_supplycost_raw)`; second pass emits a `Q02Row` for **every** pair with
  `ps_supplycost_raw == min_cost[ps_partkey]`. Tally `matched_parts` and `min_tie_parts`.
- Optionally sort `rows` by `ORDER BY s_acctbal DESC, n_name, s_name, p_partkey` for stable diagnostics. Do not
  use that order for answer equality; the comparator separately checks full-row multiset equality and emitted SQL
  order.

## D2 — `check_strict_q02` + `compare_strict_q02` + wiring
Mirror `check_strict_q5`:
1. `execute_query_with_tables(STRICT_Q2_SQL, q02_tables(true, sf))` (reuse ADR 284's `q02_tables`); `StageError`
   → classified row.
2. `assert_strict_q02_schema(&batches)` — the 8 columns at the Phase-0-measured types; drift → `Compare`/`Fail`.
3. Non-vacuity: `oracle.matched_parts >= 1 && !oracle.rows.is_empty()` → else `Fail`.
4. `compare_strict_q02(&batches, &oracle)`:
   - extract emitted `Q02Row`s; `is_null` on every one of the 8 cells → `Err` before reading;
   - **answer = full-row multiset**: sort emitted + oracle `rows` by a canonical all-columns key, element-wise
     equal (exact `i128`/`String`/`i64`); equal lengths. **Not** a `(p_partkey, …)` key-map (no DISTINCT).
   - **order check**: walk emitted rows; each adjacent pair must respect `s_acctbal DESC, n_name ASC, s_name ASC,
     p_partkey ASC` — reject a violation; allow a full-4-key tie to permute (no hidden tie-break).
- `main.rs`: route `correctness --query q02` and `correctness --query q2` → `check_strict_q02(sf)` (strict-only).
- `correctness_matrix`: q02 renders `strict=pass@compare` (8th cell).

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `strict_q02_matches_independent_oracle` — `pass@compare` at sf=0.01 (64MB-stack thread if the nested scalar
  bind/exec depth needs it).
- `strict_q02_oracle_is_non_vacuous_at_default_scale` — `matched_parts > 0 && !rows.is_empty()`.
- `strict_q02_output_schema_is_asserted` — accept measured schema; **reject `p_partkey Int32`**.
- `q02_brass_is_suffix_match` — `q02_type_matches_brass` is `ends_with("BRASS")`, not contains (a
  `'…BRASSX'`-style negative).
- per-class mismatch fails: wrong `s_acctbal` (Decimal), wrong string projection, wrong `p_partkey`.
- `q02_dropped_min_tie_supplier_is_fail` — **synthetic** oracle with two min-cost rows for one part; dropping one
  in the batch → fail (the MIN-tie tripwire, even if the live fixture has `min_tie_parts == 0`).
- non-min/extra row, missing row, unexpected duplicate row → fail; NULL in any of the 8 columns → fail.
- `q02_order_violation_is_fail` — including the mixed `DESC`/`ASC` directions.
- Existing correctness (q01/q03/q05/q06/q12/q14/q17) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q02
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
Plus empirical: `target/debug/toron-tpch correctness --query q02` → `pass@compare`.

## Results doc
`crates/toron/plans/impl/0292_q02_correctness_oracle_results.md`: the measured 8-column schema (`p_partkey Int64`,
`s_acctbal Decimal128(15,2)`); the emitted row count + `min_tie_parts` at sf=0.01 (measured evidence, not an
invariant); q02 `strict=pass@compare`; confirmation prior reach + correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the EUROPE-scoped-MIN +
  all-ties + full-row-multiset + dropped-min-tie + order-violation + per-class-mismatch tests are the defenses.
- **EUROPE-scoped MIN, not global** — the single easiest place to get the correlated subquery wrong.
- **Full-row multiset, not a key-map** (no DISTINCT) — a key-map would mask a duplicate row.
- **`p_partkey Int64`** — measure/assert; don't bake Int32.

## Seal
`impl: adr 292 q02 correctness oracle`. Do not commit wip drafts.
