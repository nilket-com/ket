# Implementation Plan — ADR 276: strict q01 correctness oracle

## Summary
Add a strict-only q01 correctness oracle to the ADR 273 rail: assert strict q01's measured 10-column Arrow
schema, then compare every cell (N groups × 8 aggregates + 2 keys) against an independent
`LineItemGenerator`-traversal oracle that mirrors the executor's raw-`i128` arithmetic (incl.
`avg_decimal_finalize` half-away rounding). Scope: `crates/toron-tpch` only. No engine change (q01 reachability
already green).

## Phase 0 — assert the measured schema (not optional)
Pin strict q01's output schema as a constant and assert the executor's actual output matches it (column count,
names, exact `DataType`) **before** comparing values. Measured live (do not re-derive):
```
l_returnflag Utf8 · l_linestatus Utf8
sum_qty Decimal128(15,2) · sum_base_price Decimal128(15,2)
sum_disc_price Decimal128(32,4) · sum_charge Decimal128(38,6)
avg_qty Decimal128(19,6) · avg_price Decimal128(19,6) · avg_disc Decimal128(19,6)
count_order Int64
```
A schema mismatch is itself `fail@compare` (it means the oracle would compare at the wrong scale).

## Phase 1 — the independent oracle (`lib.rs`)
`strict_q1_oracle(scale_factor) -> BTreeMap<(String, String), Q1Group>` (just the sorted group map; the
non-vacuous guards derive from it — see Phase 3). Traverse `LineItemGenerator::new(sf, 1, 1)`; for each row with
`l_shipdate.to_unix_epoch() <= 10471` (verified day-count for `DATE '1998-09-02'`), accumulate into the
`(l_returnflag, l_linestatus)` entry (`BTreeMap` → sorted ASC/ASC):
- `qty_raw = l_quantity as i128 * 100` (scale 2, matches the strict fixture); `extprice_raw =
  l_extendedprice.into_inner() as i128`; `disc_raw = l_discount.into_inner() as i128`; `tax_raw =
  l_tax.into_inner() as i128`.
- `sum_qty += qty_raw`; `sum_base_price += extprice_raw`; `sum_discount += disc_raw` (avg_disc input);
  `sum_disc_price += extprice_raw * (100 - disc_raw)`;
  `sum_charge += extprice_raw * (100 - disc_raw) * (100 + tax_raw)`; `count += 1`.
`Q1Group` holds those `i128` accumulators + the `i64` count.

## Phase 2 — mirror `avg_decimal_finalize` (`lib.rs`)
`avg_decimal_raw(total: i128, count: i64, input_scale: i8, out_scale: i8) -> Result<i128, String>`,
bit-for-bit with the executor on valid q01 groups: `scaled = total * 10^(out_scale - input_scale)`;
`quotient = scaled / count128`;
`if (scaled % count128).unsigned_abs() * 2 >= count128.unsigned_abs() { quotient += scaled.signum() }`.
Return `Err` for impossible inputs (`count == 0`, `out_scale < input_scale`, checked-power/multiply overflow)
so oracle bugs become `fail@compare`, not harness panics. q01:
`avg_qty = avg_decimal_raw(sum_qty, count, 2, 6)?`, `avg_price = avg_decimal_raw(sum_base_price, count, 2, 6)?`,
`avg_disc = avg_decimal_raw(sum_discount, count, 2, 6)?` — all `Decimal128(19,6)`.

## Phase 3 — check + compare (`lib.rs`)
`check_strict_q1(scale_factor) -> CorrectnessAttempt` (mirrors `check_strict_q6`): build the strict-q01 tables
(reuse `generate_strict_lineitem` + `strict_lineitem_descriptor`), `execute_query_with_tables(STRICT_Q1_SQL,
…)`; on `Ok(batches)`:
- **Phase 0 schema assertion** (else `fail@compare` "schema drift: …").
- **Non-vacuous (precise):** `selected_rows = oracle.values().map(|g| g.count).sum()` ≥ 1, **and**
  `oracle.len() > 1` (genuinely multi-row); else `fail@compare`.
- `compare_strict_q1(batches, &oracle)`: concatenate output rows; assert row count == `oracle.len()`; per row
  read `l_returnflag`/`l_linestatus` (Utf8) to find the oracle group, then assert each column's raw value
  (`Decimal128Array::value(row)` == oracle raw — sums direct, avgs via Phase 2), `count_order` i64, and
  nullness. Mismatch → `fail@compare` naming the group + column.
- `Err(StageError)` → carry the deepest phase + reason (reachability-style), as `check_strict_q6` does.

## Phase 4 — CLI (`main.rs`)
In `correctness_command`, add `"q01" | "q1" => check_strict_q1(sf).await` beside the q06 arm. Unsupported query
stays a usage error (exit 2, no row). `correctness-matrix` already renders any `tpch_correctness` row.

## Phase 5 — tests (`tests/correctness_test.rs`)
- `strict_q1_matches_independent_oracle` — `check_strict_q1(0.01)` → `phase=Compare status=Pass`.
- `strict_q1_output_schema_is_asserted` — the Phase-0 schema constant matches the 10 measured columns (and a
  crafted wrong-type batch trips the assertion).
- `avg_decimal_raw_matches_half_away` — crafted values incl. a **negative** `total` and a `.5` tie, to pin the
  `scaled.signum()` direction; zero count / negative scale delta return `Err`.
- `strict_q1_oracle_selects_multiple_groups` — `selected_rows ≥ 1` and `oracle.len() > 1` at sf 0.01.
- `q1_correctness_mismatch_is_fail_at_compare` — feed `compare_strict_q1` a perturbed oracle (one cell +1) →
  `fail@compare`.
- q06 correctness + q01/q03/q06/q10/q14 reachability stay green.

## Verification gate (harness-only, via scripts/cargo-cgroup, AFTER fmt)
```
scripts/cargo-cgroup fmt   -p toron-tpch -- --check
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test  -p toron-tpch
scripts/cargo-cgroup run   -p toron-tpch -- correctness --query q01 --sf 0.01   # -> pass
scripts/cargo-cgroup run   -p toron-tpch -- correctness-matrix                  # q01 strict=pass@compare
scripts/cargo-cgroup run   -p toron-tpch -- matrix                              # unchanged
git diff --check
```

## Seal
`impl: adr 276 q01 correctness oracle`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
Add `crates/toron/plans/impl/276_q01_correctness_oracle_results.md` (the measured schema + the pass row).

## Risk notes
- **Schema first.** Assert the 10-column schema before any value compare; a silent scale drift is the main
  failure mode (the iteration `(31,4)`-vs-`(32,4)` miss).
- **Exact, not approximate.** Raw `i128` equality; AVG mirrors `avg_decimal_finalize` exactly on valid groups,
  with invalid helper inputs returned as compare errors — verify a negative-`total` case (direction-aware
  rounding).
- **Non-vacuous means what it says:** `selected_rows ≥ 1` (input rows accumulated) AND `oracle.len() > 1`
  (multi-row) — not a vague "matched count".
- **Row alignment by group key**, not positional — assert both the sorted order and the key values.
- **No engine change**; reachability rail + digests untouched; `CorrectnessAttempt`/`tpch_correctness` reused
  (not `Attempt`).
