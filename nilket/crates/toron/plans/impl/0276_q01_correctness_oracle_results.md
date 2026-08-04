# ADR 276 — Results: strict q01 correctness oracle (first multi-row answer-check)

## Measured row
```
Correctness:  q01 strict=pass@compare   (q06 strict=pass@compare unchanged)
Reachability: q01/q03/q06/q10/q14 dev+strict pass@execute (unchanged)
```
Verified live:
```
tpch_correctness query=q01 sf=0.01 mode=strict phase=compare status=pass reason=none
```
Toron's strict q01 output — **N groups × 10 columns** — matches an independent oracle **exactly**: the schema
is asserted first, then every cell's raw `i128` (four sums, three half-away averages) and `i64` count compare
equal, group by group. The first multi-row, multi-column answer-checked cell.

## Schema measured, not derived (the load-bearing discipline)
Phase 0 reads strict q01's actual Arrow output and asserts it against a pinned schema **before** any value
comparison; a drift is itself `fail@compare`. The measured types:
```
l_returnflag Utf8 · l_linestatus Utf8
sum_qty/sum_base_price Decimal128(15,2) · sum_disc_price Decimal128(32,4) · sum_charge Decimal128(38,6)
avg_qty/avg_price/avg_disc Decimal128(19,6) · count_order Int64
```
`sum_disc_price` is `Decimal128(32,4)` — an on-paper derivation in iteration gave `(31,4)`, wrong by one digit;
measuring (an in-crate `#[cfg(test)]` schema-dump probe) corrected it. A wrong scale would have made the oracle
false-fail at the comparison rather than flag the real cause.

## The oracle (independent, exact — `crates/toron-tpch/src/lib.rs`)
- `strict_q1_oracle(sf) -> BTreeMap<(returnflag, linestatus), Q1Group>` — traverses `LineItemGenerator`,
  filters `l_shipdate.to_unix_epoch() <= 10471` (`DATE '1998-09-02'`, verified), and accumulates raw `i128`
  per group (sorted map → matches q01's ASC/ASC `ORDER BY`): `sum_qty = Σ(l_quantity*100)`,
  `sum_base_price = Σ extprice_raw`, `sum_disc_price = Σ(extprice_raw*(100-disc_raw))` [scale 4],
  `sum_charge = Σ(extprice_raw*(100-disc_raw)*(100+tax_raw))` [scale 6], `sum_discount` (avg_disc input),
  `count`.
- `avg_decimal_raw(total, count, in_scale, out_scale, out_precision) -> Result<i128, String>` — mirrors
  `exec.rs::avg_decimal_finalize` bit-for-bit (`scaled = total*10^(out-in)`, `/count`, half-away via
  `scaled.signum()`, **then the output-precision fit** — the executor's final guard, review Finding 2), and is
  **fallible**: zero count / negative scale-delta / overflow / a result that doesn't fit `out_precision` return
  a compare failure, not a harness panic.
- `compare_strict_q1` — schema assert; then, per output row, reject any **NULL** cell (key/aggregate/count) as
  `fail@compare` (review Finding 1 — q01's non-empty groups are always non-null, so a NULL is an anomaly, not
  a value to compare through its slot); then align rows to the sorted oracle groups by key (verifying the
  order), asserting each cell's raw value + the `count`. Mismatch → `fail@compare` naming group + column.
- `check_strict_q1` — execute → schema/non-vacuous guards → compare. Non-vacuous (precise): `selected_rows ≥ 1`
  (Σ per-group counts) **and** `groups.len() > 1` (genuinely multi-row).

CLI: `correctness --query q01` (beside q06); unsupported queries stay a usage error (exit 2, no row).

## Tests (`tests/correctness_test.rs`)
- `strict_q1_matches_independent_oracle` — `check_strict_q1(0.01)` → `phase=Compare status=Pass`.
- `strict_q1_oracle_selects_multiple_groups` — `selected_rows ≥ 1` and `> 1` group (multi-row, non-vacuous).
- `avg_decimal_raw_matches_half_away_and_is_fallible` — half-away ties round away in both directions (incl. a
  negative `total`), the scale-6 rescale, the zero-count / negative-scale-delta `Err` paths, and the
  **output-precision-overflow** `Err` (Finding 2).
- `q1_correctness_mismatch_is_fail_at_compare` — a consistent (output, oracle) pair matches; a one-cell oracle
  perturbation is `fail@compare`.
- `q1_null_cell_is_fail_at_compare` — a NULL aggregate cell is rejected, naming the column (Finding 1).
- `strict_q1_schema_drift_is_detected` — a `sum_disc_price` typed `(31,4)` (the iteration miss) is caught by
  Phase 0, naming the column.
- q06 correctness + q01/q03/q06/q10/q14 reachability stay green.

## Gate (harness-only)
fmt clean · `check -p toron-tpch --all-targets` clean · `test -p toron-tpch` green (correctness 10, harness 15,
probes) · live `correctness --query q01` → `pass`, `correctness-matrix` → q01 + q06 `pass@compare`, `matrix`
unchanged · `git diff --check` clean. No `-p toron` engine change (q01 reachability already green; the oracle
found no mismatch).

## Forward arc
After q01 correctness, the cadence points back to reachability: **q05 (6-table)**. q03 (grouped, like q01) and
q14 (needs an explicit ratio/rounding policy) remain available correctness targets later.
