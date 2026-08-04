# ADR 289 - Results: q14 correctness oracle

## Outcome

Strict q14 is now answer-checked:

```text
tpch_correctness query=q14 sf=0.01 mode=strict phase=compare status=pass reason=none
```

This adds the sixth correctness cell and the first ratio query to the correctness rail. The reachability matrix is
unchanged.

## Phase-0 Schema Measurement

The strict q14 output schema was measured live and pinned:

```text
promo_revenue  Decimal128(38, 12)
```

This is the implementation-time correction to the plan's `(38,10)` prior. The numerator expression is
`100.00 * SUM(...)`; the `100.00` literal aligns with the scale-4 Decimal sum, so the numerator is treated as
scale 8 before the final Decimal division. The division output rule then yields scale 12.

## Oracle

`strict_q14_oracle` traverses the generators directly:

- `part`: build `p_partkey -> p_type.starts_with("PROMO")`;
- `lineitem`: keep rows in `[1995-09-01, 1995-10-01)` and joined by `l_partkey`;
- denominator: all matching discounted prices, `l_extendedprice_raw * (100 - l_discount_raw)`;
- numerator: only PROMO matching discounted prices, multiplied by the aligned `100.00` literal;
- quotient: `decimal_division_raw`, matching the executor's half-away Decimal division.

Measured non-vacuity at `sf=0.01`:

```text
denominator_rows = 722
promo_rows       = 109
promo_revenue    = 15486545812284  -- raw Decimal128(38,12)
```

The comparator requires both denominator rows and PROMO rows, so the CASE true branch cannot pass hollow.

## Tests

Added focused tests for:

- strict q14 end-to-end `pass@compare` at `sf=0.01`;
- non-vacuous oracle data at the default correctness scale;
- measured schema drift rejection;
- PROMO-branch non-vacuity rejection;
- a synthetic CASE-denominator bug where non-PROMO rows are wrongly excluded;
- q14-shaped half-away Decimal division;
- off-by-one raw mismatch and oracle-side type drift;
- NULL, empty, multi-row, and oracle-NULL result rejection.

## Matrix

Correctness now has six strict pass cells:

```text
q01 strict=pass@compare
q03 strict=pass@compare
q05 strict=pass@compare
q06 strict=pass@compare
q14 strict=pass@compare
q17 strict=pass@compare
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q10/q14/q17/q20: dev=pass@execute strict=pass@execute
```

## Forward

q14 closes the ratio-query correctness class for the current harness. The next cycle can return to reachability
breadth, or extend correctness to q10/q02.
