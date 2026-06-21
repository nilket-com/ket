# ADR 300 results - q08 correctness oracle

## Measured result

`q08 strict=pass@compare` at `sf=0.01`.

## Phase-0 facts

Strict q08 output schema:

```text
o_year     Int32
mkt_share  Decimal128(38,8)
```

The numerator and denominator SUM operands both measured as:

```text
Decimal128(32,4)
```

The independent oracle measured the default-scale answer as non-empty but numerator-hollow:

```text
denominator_rows=29
brazil_rows=0
groups:
  1995  0  Decimal128(38,8)
  1996  0  Decimal128(38,8)
```

So the live q08 answer proves the derived join, filters, denominator, division type, and ordering, but not a
non-zero BRAZIL numerator. The synthetic q08 tests therefore carry the CASE-true and non-zero ratio
discrimination.

## Oracle

The q08 oracle traverses `PartGenerator`, `SupplierGenerator`, `LineItemGenerator`, `OrderGenerator`,
`CustomerGenerator`, `NationGenerator`, and `RegionGenerator` directly:

- `p_type = 'ECONOMY ANODIZED STEEL'`;
- customer nation (`n1`) must be in region `AMERICA`;
- supplier nation (`n2`) controls the `CASE WHEN nation = 'BRAZIL'` numerator branch;
- `o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'` is inclusive;
- `volume = discounted_price_raw(l_extendedprice, l_discount)` at raw scale 4;
- `mkt_share = decimal_division_raw(num_raw, 32, 4, den_raw, 32, 4)`.

## Tests

The q08 suite pins:

- live `strict_q08_matches_independent_oracle`;
- default-scale non-vacuity, denominator rows, and the measured hollow BRAZIL branch;
- schema drift;
- Decimal division half-away behavior;
- customer/supplier nation-role separation;
- exact part type and date bounds;
- a synthetic BRAZIL numerator group;
- wrong ratio or wrong year;
- missing, extra, or duplicate year groups;
- NULL cells;
- `ORDER BY o_year`.

## Board

Correctness after this cut:

```text
q01/q02/q03/q04/q05/q06/q07/q08/q10/q12/q14/q17/q20  strict=pass@compare
remaining queries                                          unchecked
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q07/q08/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
q09/q11/q13/q15/q16/q18/q19/q21/q22                 not_run
```

## Forward

The current green reachability frontier is fully answer-checked. The next move can return to reachability; q09 is
the likely next query because it introduces the next string surface over a multi-table join.
