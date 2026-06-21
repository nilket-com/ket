# ADR 291 - Results: q12 correctness oracle

## Outcome

Strict q12 is now answer-checked:

```text
tpch_correctness query=q12 sf=0.01 mode=strict phase=compare status=pass reason=none
```

This adds the seventh correctness cell. Reachability is unchanged.

## Phase-0 Schema Measurement

The strict q12 output schema was measured live and pinned:

```text
l_shipmode       Utf8
high_line_count  Int64
low_line_count   Int64
```

A schema drift is `fail@compare`.

## Oracle

`strict_q12_oracle` traverses the generators directly:

- `orders`: build `o_orderkey -> o_orderpriority`;
- `lineitem`: keep `l_shipmode IN ('MAIL','SHIP')`;
- apply `l_commitdate < l_receiptdate`, `l_shipdate < l_commitdate`, and
  `l_receiptdate in [1994-01-01, 1995-01-01)`;
- join by `l_orderkey`;
- group by `l_shipmode`;
- count `1-URGENT` / `2-HIGH` as high priority, all other joined priorities as low priority.

Measured non-vacuity at `sf=0.01`:

```text
selected_rows = 307
high_rows     = 125
low_rows      = 182

MAIL high=64 low=86
SHIP high=61 low=96
```

Because both groups have both CASE branches populated, the comparator enforces per-group high/low
non-vacuity rather than only a global floor.

## Tests

Added focused tests for:

- strict q12 end-to-end `pass@compare` at `sf=0.01`;
- non-vacuous oracle data at the default correctness scale;
- measured schema drift rejection;
- off-by-one high and low count mismatches;
- high/low CASE-arm swap rejection;
- missing and duplicate `l_shipmode` group rejection;
- NULL cell rejection for all output columns;
- `ORDER BY l_shipmode` rejection when rows are out of order;
- hollow oracle rejection for missing global or per-group CASE branches.

## Matrix

Correctness now has seven strict pass cells:

```text
q01 strict=pass@compare
q03 strict=pass@compare
q05 strict=pass@compare
q06 strict=pass@compare
q12 strict=pass@compare
q14 strict=pass@compare
q17 strict=pass@compare
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20: dev=pass@execute strict=pass@execute
```

## Forward

q12 closes the first correctness cell whose headline result is conditional counts over joined rows. The
remaining rail can continue through q10/q02, while reachability can return to the unattempted query set.
