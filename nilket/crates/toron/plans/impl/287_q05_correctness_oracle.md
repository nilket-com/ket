# ADR 287 — Implementation plan: q05 correctness oracle

Pairs with `plans/adr/287_q05_correctness_oracle.md`. Harness-only (`toron-tpch`); no `crates/toron` engine
change. Function names are current-tree anchors.

## Phase 0 — measure the live schema (don't hard-code)
Run strict q05 and **capture its actual output schema** before writing the assertion:
```
scripts/cargo-cgroup run -p toron-tpch -- run --query q05 --mode strict --sf 0.01 --allow-blocked
```
Use a temporary probe or focused helper to print/assert `batches[0].schema()`; the CLI row only proves
reachability. Record the revenue column's exact `Decimal128(p, 4)` in the results doc. The assertion in D2 must
pin *that measured type* — not `(15,2)` (the `l_extendedprice` input) and not an unmeasured `(32,4)`.

## D1 — `strict_q5_oracle` (`crates/toron-tpch/src/lib.rs`)
Mirror `strict_q3_oracle` (`:1651`) — an independent generator traversal returning grouped revenue. A `Q5Oracle`
(like `Q3Oracle`) carries `selected_rows` and `groups: BTreeMap<String, i128>` (n_name → raw scale-4 revenue).
- Build lookups from the generators: `region` (filter `r_name == "ASIA"` → region keys), `nation`
  (`n_nationkey → (n_name, n_regionkey)`), `supplier` (`s_suppkey → s_nationkey`), `customer`
  (`c_custkey → c_nationkey`), `orders` (`o_orderkey → (o_custkey, o_orderdate)`).
- Per `lineitem`: resolve `o = orders[l_orderkey]`; `cust_nat = customer[o.o_custkey]`;
  `supp_nat = supplier[l_suppkey]`; require `o.o_orderdate ∈ [day(1994-01-01), day(1995-01-01))`,
  **`cust_nat == supp_nat`**, `nation[supp_nat].regionkey ∈ ASIA region keys`. Count `selected_rows`.
- Accumulate `extprice_raw * (100 - disc_raw)` (raw scale-4 `i128`, the q03 arithmetic) into
  `groups[nation[supp_nat].n_name]`. Factor a tiny shared helper with q03 only if it keeps the arithmetic more
  auditable; q05 has no division/AVG rounding path to mirror.

## D2 — `check_strict_q5` + wiring (`crates/toron-tpch/src/lib.rs`, `main.rs`)
Mirror `check_strict_q3` (`:1863`):
1. `execute_query_with_tables(STRICT_Q5_SQL, q05_tables(true, sf))`; on `StageError` return the classified row.
2. `assert_strict_q5_schema(&batches)` — `n_name` Utf8 + revenue `Decimal128(p, 4)` (the **Phase-0-measured**
   exact type recorded in the implementation); schema drift → `Compare`/`Fail`.
3. Non-vacuity: `oracle.selected_rows >= 1 && oracle.groups.len() > 1` → else `Fail`.
4. `compare_strict_q5(&batches, &oracle.groups)`:
   - bijection: each output `(n_name, revenue)` matches the oracle exactly; keys unique; **`is_null` checked on
     every slot** before reading; counts equal.
   - **ORDER BY prefix:** assert the output revenue column is non-increasing (DESC) — do **not** assert a
     secondary order (no tie-break key in the SQL).
- `main.rs`: route `correctness --query q05`/`q5` → `check_strict_q5` (strict-only; no `--mode` branch).
- `correctness_matrix` should render `q05 strict=pass@compare` from the emitted correctness row; no separate
  reachability matrix mutation.

## Tests (`crates/toron-tpch/tests/correctness_test.rs`)
- `check_strict_q5` returns `pass@compare` at `sf=0.01` (the correctness rail's default scale).
- **Discriminating:** a focused unit asserting the oracle is non-vacuous (rows + >1 group) and that
  `compare_strict_q5` rejects a perturbed batch (wrong revenue / dropped group / wrong DESC order) — proving it's
  ≥ as strict as the engine, not a tautology.
- Existing correctness rows (q01/q03/q06) + reach board unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q05
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```
(No `-p toron` engine legs needed — this is a harness-only cut. If the oracle traversal is deep, use the
explicit-64MB-stack pattern in the test, like the q05/q10 reach tests.)

## Results doc
`crates/toron/plans/impl/287_q05_correctness_oracle_results.md`: the Phase-0-measured live revenue
`Decimal128(p,4)`; q05 `strict=pass@compare` row; the cyclic-gate + scale-4 + null + ORDER-BY-prefix disciplines
applied; confirmation prior reach/correctness rows unchanged.

## Risk / blast radius
- Harness-only — no engine surface. The risk is an **incorrect oracle** (a lying green): the discriminating
  perturbation test + the live-schema assertion + the cyclic-gate guard are the defenses.
- Don't hard-code the revenue precision — Phase 0 measures it (the whole point of this cut's schema discipline).

## Seal
`impl: adr 287 q05 correctness oracle`. Do not commit wip drafts.
