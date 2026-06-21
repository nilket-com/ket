# ADR 299 results - q07 correctness oracle

## Measured result

`q07 strict=pass@compare` at `sf=0.01`.

Live row:

```text
tpch_correctness query=q07 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=3735.2
```

Reachability is unchanged: q07 remains `pass@execute` in both modes.

## Phase-0 facts

The strict q07 output schema is pinned before comparing values:

```text
supp_nation  Utf8
cust_nation  Utf8
l_year       Int32
revenue      Decimal128(32,4)
```

At `sf=0.01`, the independent oracle is non-vacuous:

```text
selected_rows=46
groups:
  FRANCE   GERMANY  1995  2680685774
  FRANCE   GERMANY  1996  3038622980
  GERMANY  FRANCE   1995  6211594882
  GERMANY  FRANCE   1996  3790958854
```

## Oracle

The oracle traverses `SupplierGenerator`, `LineItemGenerator`, `OrderGenerator`, `CustomerGenerator`, and
`NationGenerator` directly. It resolves the nation self-join explicitly:

- `supp_nation` comes from `l_suppkey -> supplier.s_nationkey -> nation.n_name`;
- `cust_nation` comes from `l_orderkey -> orders.o_custkey -> customer.c_nationkey -> nation.n_name`;
- only `(FRANCE, GERMANY)` and `(GERMANY, FRANCE)` are admitted;
- `l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'` is inclusive, Date32 days `[9131, 9861]`;
- `l_year` is 1995 or 1996 from the shipdate;
- `revenue` is q05's raw scale-4 `discounted_price_raw`.

## Tests

The q07 suite pins:

- live `strict_q07_matches_independent_oracle`;
- default-scale non-vacuity and exact four grounded groups;
- schema drift;
- supplier/customer nation direction;
- inclusive `BETWEEN` boundaries;
- the two ordered FR/GER disjuncts only;
- revenue mismatch and wrong year;
- missing, extra, or duplicate groups;
- NULL cells;
- total `ORDER BY supp_nation, cust_nation, l_year`.

## Board

Correctness after this cut:

```text
q01/q02/q03/q04/q05/q06/q07/q10/q12/q14/q17/q20  strict=pass@compare
q08 and remaining queries                              unchecked
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q07/q08/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
q09/q11/q13/q15/q16/q18/q19/q21/q22                 not_run
```

## Forward

q07 is no longer an unchecked green reachability cell. q08 is the remaining catch-up target before the
correctness rail catches the current reachability frontier.
