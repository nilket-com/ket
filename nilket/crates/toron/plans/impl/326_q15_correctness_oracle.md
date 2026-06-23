# ADR 326 — Implementation plan: q15 correctness oracle

Pairs with `plans/adr/326_q15_correctness_oracle.md`. Oracle-only cut in `toron-tpch` (generator traversal + the
existing precision helpers). Verify-first — no engine change expected. Canonical correctness row is **strict-only**.

## Phase 0 — verify-first + ground the precision (before pinning)
1. Run a draft q15 oracle vs q15 **strict** at sf=0.01. Expect `pass@compare`; if it mismatches, name the bug
   (don't paper over it — the q11 lesson).
2. **The MAX-equality precision (the crux):** confirm the engine's CTE `total_revenue` precision and the scalar
   `MAX(total_revenue)` precision are identical, so `total_revenue == MAX` matches exactly. Mirror that exact
   precision in the oracle (compute the SUMs and the MAX at the engine's `(p,s)`).
3. **Tie probe:** record whether sf=0.01 yields a single max supplier or a tie; the oracle must be correct either
   way, and the test documents which it is at default scale.
4. **Selectivity:** record `oracle_rows` vs `total suppliers in window` — must be a proper subset.

## D1 — the oracle (`crates/toron-tpch/tests/correctness_test.rs`)
- Independent traversal (pattern: `q04_oracle`, `q11_oracle`, `q16_oracle`; helpers `avg_decimal_raw` /
  `decimal_division_raw` for the precision math):
  - revenue CTE: per `l_suppkey`, `SUM(l_extendedprice * (1 - l_discount))` over the shipdate window, mirroring the
    engine's product + SUM Decimal precision.
  - `m = MAX(total_revenue)` at the CTE-column precision.
  - `supplier ⋈ revenue on s_suppkey = supplier_no`; keep `total_revenue == m` (exact Decimal eq); **emit all ties**.
  - `ORDER BY s_suppkey`; project `(s_suppkey, s_name, s_address, s_phone, total_revenue)`.
- Assert the strict q15 output schema first; check `is_null` on every Arrow slot.

## D2 — wiring + non-vacuity + strict-only
- Wire q15 into the strict correctness comparator (`crates/toron-tpch/src/lib.rs`) and the correctness tests
  (`crates/toron-tpch/tests/correctness_test.rs`); q15 is already present in the reachability inventory, so no CLI
  routing change is needed. The canonical row is **strict**.
- **Non-vacuity assertion (mandatory):** `0 < oracle_rows < suppliers_in_window` (proper subset — the top
  supplier(s), not all/empty). Without it the green is meaningless.
- Tie assertion: emit all suppliers whose `total_revenue == m` (document single-vs-tie at sf=0.01).

## Tests (`correctness_test.rs`)
- **q15 oracle** — the traversal; the selectivity (proper subset) assertion; schema + nullness; ORDER BY s_suppkey.
- **MAX-equality** — a fixture with a clear single max → exactly that supplier; a constructed tie → all tied
  suppliers (discriminating: a one-row assumption fails the tie case).
- **Expected pass** — q15 strict = `pass@compare` (or, if Phase 0 found a bug, an honest red asserted with a named
  reason).

## Phase 3 — the matrix
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q15
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- q15 strict = `pass@compare` (correctness **19**); other correctness cells unchanged; reach 22/22.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/326_q15_correctness_oracle_results.md`: the oracle; the Phase-0 precision finding (CTE vs
scalar MAX) + the tie/selectivity numbers at sf=0.01; q15 strict `pass@compare` (correctness 19, or a named red);
confirmation reach + other correctness cells unchanged.

## Risk / blast radius
- **MAX-equality precision** — mirror the engine's exact CTE/scalar precision; the place a silent drop hides.
- **Ties** — emit all maxes; the tie test discriminates.
- **Vacuity** — the proper-subset assertion is mandatory.
- **Product precision** — match the engine (q01/q06 helper); `1` is Int (ADR 266), unaffected by ADR 325.

## Seal
`impl: adr 326 q15 correctness oracle`. Do not commit wip drafts.
