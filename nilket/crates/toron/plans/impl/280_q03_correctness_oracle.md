# Implementation Plan — ADR 280: strict q03 correctness oracle

## Summary
Add a strict-only q03 correctness oracle to the ADR 273/276 rail: assert strict q03's measured 4-column Arrow
schema, then verify the answer (per-group `revenue`/`o_orderdate`/`o_shippriority`, keyed by the unique
`l_orderkey`) against an independent generator-traversal oracle that mirrors the executor's raw-`i128` revenue
accumulation, plus an ORDER-BY-prefix monotonicity check. Scope: `crates/toron-tpch` only. No engine change
(q03 reachability already green).

## Phase 0 — assert the measured schema (not optional)
Before locking the comparator's schema constant, run strict q03 once and read its live output schema (a
temporary local probe is fine; do not commit debug output). Pin that schema as a constant and assert the
executor's output matches (count, names, exact `DataType`) before comparing values. Expected (confirm live, do
not re-derive):
```
l_orderkey Int64 · revenue Decimal128(32,4) · o_orderdate Date32 · o_shippriority Int32
```
`revenue` is q01's `sum_disc_price` expression, measured `(32,4)` in ADR 276 — but measure, don't assume. A
mismatch is itself `fail@compare`.

## Phase 1 — implement the independent oracle (`lib.rs`)
`strict_q3_oracle(scale_factor) -> Q3Oracle`, where `Q3Oracle` carries
`groups: BTreeMap<i64, Q3Group>` keyed by the unique `l_orderkey` and `selected_rows: usize` for the
non-vacuity guard. `Q3Group { revenue: i128 /*scale 4*/, orderdate: i32, shippriority: i32 }`.
- **`c_mktsegment == "BUILDING"`** → the custkey `HashSet`.
- Date cutoff: use the verified constant **`9204`** for `DATE '1995-03-15'` (cross-checks: q01 used `10471`
  for `1998-09-02`; the gap 1995-03-15→1998-09-02 is 1267 days, 9204+1267=10471).
  Orders: `o.o_orderdate.to_unix_epoch() < 9204` and `building_customers.contains(&o.o_custkey)` → map
  `o_orderkey → (orderdate, shippriority)`. Lineitem: `l.l_shipdate.to_unix_epoch() > 9204` and `l_orderkey`
  in the order map; increment `selected_rows` for each contributing lineitem.
- Accumulate `revenue += (l_extendedprice.into_inner() as i128) * (100 - l_discount.into_inner() as i128)`
  (scale 4 — identical to q01's `sum_disc_price`). Use a `BTreeMap` for deterministic map comparison.

## Phase 2 — comparator: answer-bijection + ORDER-BY-prefix (`lib.rs`)
Implement a spec-faithful `compare_strict_q3`:
- **Schema** (Phase 0): assert 4 columns, names, `DataType`.
- **Answer-bijection:** collect the emitted rows into a `BTreeMap<i64 /*l_orderkey*/, (revenue, orderdate,
  shippriority)>` (downcast `Int64Array`/`Decimal128Array`/`Date32Array`/`Int32Array`; reject any null slot).
  Reject a duplicate emitted `l_orderkey` before insertion, then assert the key set equals the oracle's and row
  count matches; per `l_orderkey` assert `revenue` raw `i128`, `orderdate` `i32`, `shippriority` `i32`.
  Mismatch → `Err` naming `l_orderkey` + column.
- **Order-prefix:** walk the emitted rows **in emit order**; assert `revenue[i] >= revenue[i+1]`, and where
  `revenue[i] == revenue[i+1]` assert `orderdate[i] <= orderdate[i+1]`. A violation → `Err`. (Do not assert any
  ordering on `l_orderkey`/`o_shippriority` ties — SQL leaves it unspecified.)

## Phase 3 — check + non-vacuity (`lib.rs`)
Keep `check_strict_q3(scale_factor) -> CorrectnessAttempt` (mirrors `check_strict_q1`): build tables via
`q03_tables(true, scale_factor)`, `execute_query_with_tables(STRICT_Q3_SQL, …)`; on `Ok(batches)`:
- Phase-0 schema assertion (else `fail@compare` "schema drift: …").
- **Non-vacuous (precise):** `oracle.selected_rows` contributing-row count ≥ 1 **and**
  `oracle.len() > 1`; else `fail@compare`.
- `compare_strict_q3(&batches, &oracle.groups)` → `Pass`/`Fail`.
- `Err(StageError)` → carry the deepest phase + reason (as `check_strict_q1` does).

## Phase 4 — CLI (`main.rs`)
Add `"q03" | "q3" => check_strict_q3(sf).await` in `correctness_command`. Verify the command compiles and the
existing `correctness-matrix` renderer picks up q03 rows; unsupported query stays a usage error (exit 2, no
row).

## Phase 5 — tests (`tests/correctness_test.rs`)
- `strict_q3_matches_independent_oracle` — `check_strict_q3(0.01)` → `phase=Compare status=Pass`.
- `strict_q3_output_schema_is_asserted` — the Phase-0 constant matches the 4 measured columns; a crafted
  wrong-type batch trips the assertion.
- `q3_comparator_accepts_either_tie_order` — a crafted oracle + two emitted orderings of a `(revenue,
  o_orderdate)` tie: both pass the prefix check and the bijection matches (pins that the comparator does NOT
  depend on the executor's tiebreak — the discriminating test for the design decision).
- `q3_order_prefix_violation_is_fail` — an emitted sequence with revenue increasing → `fail@compare`.
- `q3_correctness_mismatch_is_fail_at_compare` — perturb one oracle cell (+1) → `fail@compare`.
- `q3_duplicate_output_key_is_fail_at_compare` — a crafted output with the same `l_orderkey` twice rejects
  before the bijection can hide the duplicate.
- `strict_q3_oracle_selects_multiple_groups` — `selected_rows ≥ 1` and `oracle.len() > 1` at sf 0.01.
- q06/q01 correctness + q01/q03/q04/q05/q06/q10/q14 reachability stay green.

## Verification gate (harness-only, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- correctness --query q03 --sf 0.01   # -> pass
scripts/cargo-cgroup run   -p toron-tpch -- correctness-matrix                  # q03 strict=pass@compare
scripts/cargo-cgroup run   -p toron-tpch -- matrix                              # unchanged
git diff --check
```

## Seal
`impl: adr 280 q03 correctness oracle`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
Add `crates/toron/plans/impl/280_q03_correctness_oracle_results.md` (the measured schema, oracle counts, and
the pass row).

## Risk notes
- **Right query, right constants** — `BUILDING`, day `9204`, no LIMIT, and no hidden sort key.
- **Schema first** — assert before any value compare; silent scale drift is the main failure mode.
- **Order by spec, not by tiebreak** — answer-bijection by `l_orderkey` + ORDER-BY-prefix monotonicity; the
  totalized full-sort match is brittle (depends on `bind_sort_keys`'s appended-column order).
- **Designed for ties** even though the tiny fixture may not exhibit them — the tie-order test pins it.
- **No engine change** — reachability rail + digests untouched; `CorrectnessAttempt`/`tpch_correctness` reused.
