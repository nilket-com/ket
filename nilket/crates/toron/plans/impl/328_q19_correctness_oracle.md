# ADR 328 - Implementation plan: q19 correctness oracle

Pairs with `plans/adr/328_q19_correctness_oracle.md`. Oracle (+ near-certain mandatory synthetic) in `toron-tpch`,
plus the canonical CLI wiring. Verify-first. Strict-only canonical row. q19 is a **scalar SUM** - vacuity is on
**qualifying input rows**, not output rows.

## Phase 0 - settle vacuity (on input rows) before pinning
1. **Qualifying-input count at sf=0.01:** count lineitem/part rows that match any branch (a draft oracle traversal).
   Output is always one row (scalar SUM), so this is the only meaningful vacuity measure. Record the count; ~0
   expected -> **mandatory synthetic** (D3).
2. **Precision:** confirm the engine's `revenue` is `Decimal128(32,4)` (same product as q15's `total_revenue`);
   mirror exactly.

## D1 - the oracle (`crates/toron-tpch/src/lib.rs`)
- Traverse `LineItemGenerator` + `PartGenerator`; join on `p_partkey = l_partkey`.
- Qualify a row if it matches **any** branch's full conjunction (brand, container-set, quantity-range,
  size-range, shipmode-set, shipinstruct) - the three branches from lib.rs:164.
- `revenue = SUM(l_extendedprice * (1 - l_discount))` over qualifying rows at the engine's `Decimal128(32,4)`
  precision (reuse the q15 product helper).
- Comparator `compare_strict_q19` + `check_strict_q19` (mirror `compare_strict_q15`/`check_strict_q15`): assert the
  single-column `Decimal128(32,4)` schema; compare raw value + nullness exactly.

## D2 - vacuity assertion (input rows)
- Assert **qualifying-input-row count >= 1** for the answer check (live or synthetic). Never assert on output rows
  (always 1). A NULL/0 result with zero qualifying inputs is vacuous and must not count as a pass.

## D3 - the synthetic (near-certain; proves the disjunction)
- A synthetic fixture with:
  - one qualifying lineitem/part row **per branch** (1, 2, 3) -> included, contributing known revenue;
  - **per-dimension control rows** missing by exactly one predicate (wrong brand / wrong container / quantity out of
    `[lo, lo+10]` / size out of range / non-AIR shipmode / non-DELIVER-IN-PERSON) -> each excluded.
- The test asserts the synthetic's revenue equals the sum of *only* the per-branch rows (inclusions) and that each
  control is excluded - proving the OR both ways.

## D4 - canonical wiring (mandatory)
- Add `"q19" => check_strict_q19(sf).await` to `main.rs` `correctness_command`.
- Confirm `correctness-matrix` (derives from `ALL_QUERY_IDS`) shows `q19 strict=pass@compare` once the row writes.

## Tests (`correctness_test.rs`)
- **q19 oracle** - the traversal; the `Decimal128(32,4)` schema; raw value + nullness.
- **Input-row vacuity** - assert >=1 qualifying input row (live or synthetic).
- **Disjunction both ways** - each branch's row included; each per-dimension control excluded (discriminating).
- **Expected pass** - q19 strict = `pass@compare` (or an honest red with a named reason if Phase 0 finds a bug).

## Phase 3 - the canonical board (NOT optional)
```
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
```
- `correctness --query q19` -> a real `phase=compare status=pass` row; `correctness-matrix` -> `q19 strict=pass@compare`
  (count **21**); reach 22/22; other cells unchanged.

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

## Results doc
`crates/toron/plans/impl/328_q19_correctness_oracle_results.md`: the oracle; the Phase-0 qualifying-input count
(live, and the synthetic since live is ~0); the branch inclusions + per-dimension exclusions; the `Decimal128(32,4)`
precision; q19 strict `pass@compare` (count 21) confirmed on the **canonical matrix**; reach + other cells
unchanged.

## Risk / blast radius
- **Vacuity on input rows** - assert qualifying-input >= 1, not output count.
- **Disjunction** - per-dimension controls prove the OR-key extraction; range-boundary cases included.
- **Canonical wiring** - `correctness --query q19` + `correctness-matrix` part of the seal.
- **Precision** - `Decimal128(32,4)`; empty-SUM is NULL but never the pass.

## Seal
`impl: adr 328 q19 correctness oracle`. Do not commit wip drafts.
