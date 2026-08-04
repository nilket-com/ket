# ADR 287 - Results: q05 correctness oracle

## Outcome

Strict q05 is now answer-checked:

```text
tpch_correctness query=q05 sf=0.01 mode=strict phase=compare status=pass reason=none
```

This adds the first six-table cyclic-join correctness cell. The reachability matrix is unchanged.

## Phase-0 Schema Measurement

The strict q05 output schema was measured before the comparator was pinned:

```text
n_name   Utf8
revenue  Decimal128(32, 4)
```

The revenue type is not the input money type `Decimal128(15,2)`. q05's revenue is the same
`SUM(l_extendedprice * (1 - l_discount))` shape used by q03, and the executor emits raw scale-4 revenue.

## Oracle

`strict_q5_oracle` traverses the `tpchgen` generators directly:

- `region`: keep `ASIA`;
- `nation`: keep nations in the ASIA region;
- `supplier`: map `s_suppkey -> s_nationkey`;
- `customer`: map `c_custkey -> c_nationkey`;
- `orders`: keep `[1994-01-01, 1995-01-01)` and map `o_orderkey -> o_custkey`;
- `lineitem`: apply `l_orderkey`, `l_suppkey`, `c_nationkey = s_nationkey`, and ASIA-region gates.

For each selected row it accumulates:

```text
l_extendedprice_raw * (100 - l_discount_raw)
```

The accumulator is raw `i128` at scale 4. There is no float conversion, no formatted-string comparison, and no
tolerance.

## Comparator

`compare_strict_q5` asserts:

- measured schema exactly: `n_name Utf8`, `revenue Decimal128(32,4)`;
- non-null `n_name` and `revenue` cells before reading values;
- answer bijection keyed by unique `n_name`;
- exact raw `i128` revenue equality;
- `ORDER BY revenue DESC` only, without pinning an unstated secondary tie-break.

The oracle non-vacuity guard requires at least one contributing lineitem row and more than one output group.

## Tests

Added focused tests for:

- strict q05 end-to-end `pass@compare` at `sf=0.01` on the large-stack path;
- non-vacuous oracle row/group counts;
- schema drift detection for the measured revenue type;
- tie-order tolerance for equal revenues;
- revenue order violation;
- off-by-one revenue mismatch;
- missing and duplicate group rejection;
- NULL-cell rejection.

## Matrix

Correctness now has four strict pass cells:

```text
q01 strict=pass@compare
q03 strict=pass@compare
q05 strict=pass@compare
q06 strict=pass@compare
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q10/q14/q17/q20: dev=pass@execute strict=pass@execute
```

## Forward

The rail now covers a scalar query, single-table grouped output, a three-table grouped join, and a six-table
cyclic grouped join. The next cycle can return to reachability breadth or pick the next correctness cell with a
new comparison policy.
