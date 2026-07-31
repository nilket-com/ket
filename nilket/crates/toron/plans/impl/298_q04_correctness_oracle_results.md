# ADR 298 - Results: q04 correctness oracle

## Final row

q04 is now answer-checked on the strict correctness rail:

```text
q04 strict=pass@compare
```

Live row at `sf=0.01`:

```text
tpch_correctness query=q04 sf=0.01 mode=strict phase=compare status=pass reason=none elapsed_ms=3682.0
```

Reachability is unchanged: q04 remains `pass@execute` in both modes.

## Measured schema

Phase 0 confirmed the strict q04 output schema:

```text
o_orderpriority  Utf8
order_count      Int64
```

The schema helper pins both the names and types before any value comparison.

## Oracle

The q04 oracle traverses `OrderGenerator` and `LineItemGenerator` directly:

- order window: `[1993-07-01, 1993-10-01)`, Date32 days `[8582, 8674)`;
- EXISTS set: `l_orderkey` values with at least one lineitem where `l_commitdate < l_receiptdate`;
- group: count each qualifying order once by `o_orderpriority`.

The semijoin distinction is load-bearing: multiple qualifying lineitems for one order contribute one count, not
one count per lineitem.

## Non-vacuity

At `sf=0.01`, the independent oracle measured:

```text
orders_pass_exists=535
orders_fail_exists=47
groups:
  1-URGENT          93
  2-HIGH           103
  3-MEDIUM         109
  4-NOT SPECIFIED  102
  5-LOW            128
```

The rail therefore exercises both sides of the EXISTS predicate and all five order-priority groups.

## Tests

The q04 suite pins:

- live `strict_q04_matches_independent_oracle`;
- default-scale non-vacuity and all five priority groups;
- schema drift;
- count mismatch;
- missing, extra, or duplicate priority groups;
- NULL cells;
- order violation;
- semijoin behavior: one order with multiple qualifying lineitems counts once;
- EXISTS exclusion: date-window orders with no qualifying lineitem are not counted.

## Board

Correctness after this cut:

```text
q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20  strict=pass@compare
q07/q08 and remaining queries                 unchecked
```

Reachability remains:

```text
q01/q02/q03/q04/q05/q06/q07/q08/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
q09/q11/q13/q15/q16/q18/q19/q21/q22                 not_run
```

## Forward

q04 is no longer an unchecked green reachability cell. The remaining catch-up correctness targets are q07 and
q08, with q08 carrying the larger derived-body ratio shape.
