# ADR 276 — strict q01 correctness oracle (first multi-row answer-check)

## Status
Proposed.

## Context
The gauge has five reach cells dev+strict green (q01/q03/q06/q10/q14) and one correctness cell (q06 strict
`pass@compare`, ADR 273). ADR 276 adds the first **multi-row, multi-column** correctness cell: **q01 strict**.
It extends the ADR 273 rail (`CorrectnessAttempt`, `correctness.jsonl`, `correctness-matrix`, the
`execute_query_with_tables` substrate, the oracle-traverses-the-generator-independently pattern) from one
Decimal scalar to N groups × 10 columns. Strict-only; harness-only (`crates/toron-tpch`) — q01 reachability is
already green, so no engine surface.

q01 strict: `GROUP BY (l_returnflag, l_linestatus) ORDER BY (l_returnflag, l_linestatus)`, eight aggregates +
the two keys.

## Measured output schema (read from the executor, not derived)
The oracle compares the exact raw `i128` at each column's exact scale, so the per-column `(p,s)` is **read from
strict q01's actual Arrow output**, not from prose type-propagation. Measured:
```
l_returnflag    Utf8              l_linestatus    Utf8
sum_qty         Decimal128(15,2)  sum_base_price  Decimal128(15,2)
sum_disc_price  Decimal128(32,4)  sum_charge      Decimal128(38,6)
avg_qty         Decimal128(19,6)  avg_price       Decimal128(19,6)
avg_disc        Decimal128(19,6)  count_order     Int64
```
`sum_disc_price` is **`Decimal128(32,4)`** — an on-paper derivation in iteration produced `(31,4)`, wrong by
one digit of precision, which the measurement corrected. That near-miss is exactly why the schema is measured
and asserted (below), not derived: a wrong scale would silently compare at the wrong place.

## Decision
A strict-only q01 correctness oracle on the ADR 273 rail:

1. **Phase 0 — assert the schema.** Pin the 10-column Arrow schema above and assert strict q01's actual output
   matches it (column count, names, exact `DataType` incl. `(p,s)`) **before** comparing any value. A schema
   drift is itself `fail@compare`.
2. **Independent oracle.** Traverse `LineItemGenerator(sf)`, filter `l_shipdate.to_unix_epoch() <= 10471`
   (`DATE '1998-09-02'`, verified: 28 yrs + 7 leap + 244 days from 1970-01-01), group into a
   `BTreeMap<(returnflag, linestatus), …>` (sorted → matches q01's ASC/ASC `ORDER BY`).
3. **Mirror executor arithmetic exactly** (raw `i128`, the measured scales). `qty_raw = l_quantity * 100`
   (scale 2, matching the strict fixture's `l_quantity as i128 * 100`):
   - `sum_qty` = Σ`qty_raw`; `sum_base_price` = Σ`extendedprice_raw` [scale 2].
   - `sum_disc_price` = Σ(`extendedprice_raw * (100 - discount_raw)`) [scale 4].
   - `sum_charge` = Σ(`extendedprice_raw * (100 - discount_raw) * (100 + tax_raw)`) [scale 6].
   - `count_order` = group row count [i64].
   - `avg_qty`/`avg_price`/`avg_disc` mirror `avg_decimal_finalize` exactly, but as a fallible harness helper:
     `scaled = total * 10^(6-2)`, `quotient = scaled / count`, **half-away** — `if
     remainder.unsigned_abs()*2 >= count.unsigned_abs() { quotient += scaled.signum() }` (`avg_disc` needs
     its own Σ`discount_raw`). Impossible inputs (`count == 0`, negative scale deltas, checked-power/multiply
     overflow) return a compare failure, not a panic. No float, **no tolerance**.
4. **Exact multi-row/-column compare.** Align Toron's output rows to the oracle's sorted groups; assert row
   count, then per row: the key columns, each Decimal cell's raw `i128`, `count_order`'s `i64`, and nullness.
   Any mismatch → `fail@compare` (no "wrong answer" recognized blocker).
5. **Non-vacuous (named precisely).** From the oracle map: **selected input rows ≥ 1** (Σ per-group counts)
   *and* **`groups.len() > 1`** — the second pins that the multi-row alignment is genuinely exercised, not a
   degenerate single group.

## Watch-items (operator) → guards
- **Schema is asserted, not assumed.** Phase 0 fails loudly if any of the 10 columns' `DataType` drifts —
  prevents a silent wrong-scale comparison.
- **Exact, not approximate.** Raw `i128` equality per cell; AVG mirrors `avg_decimal_finalize` bit-for-bit via
  a fallible helper (a tolerance would let a real rounding bug pass — the honest-correctness doctrine).
- **Row alignment by group key**, not by position — assert both the sorted order and the key values.
- **Distinct rail.** Reuse `CorrectnessAttempt` (`tpch_correctness` line) + `correctness.jsonl`; reachability
  untouched. `--query` accepts q06 and now q01; unsupported = usage error (exit 2, no row).
- **Strict-only, no engine change.** q01 reachability already green; this is pure correctness measurement.

## Acceptance
- `correctness --query q01 --sf 0.01` → `pass`; `correctness-matrix` → `q01 strict=pass@compare` (q06 row +
  reachability matrix unchanged).
- Tests: `check_strict_q1(0.01)` → `phase=Compare status=Pass`; the schema assertion; the AVG half-away helper
  on crafted values incl. a negative-`total` case (`scaled.signum()` is direction-aware) plus a zero-count
  guard; an injected mismatch → `fail@compare`; the oracle selects ≥ 1 input row and produces > 1 group.
- Harness-only gate; no `-p toron` change.

## Forward arc
After q01 correctness, the cadence (a couple of reach cells, then a correctness cell) points back to
reachability: **q05 (6-table)**. q03 (grouped, like q01) and q14 (needs an explicit ratio/rounding policy)
remain available correctness targets later.

## Why toron-home
`crates/toron-tpch` — the correctness rail. No cross-ring surface.
