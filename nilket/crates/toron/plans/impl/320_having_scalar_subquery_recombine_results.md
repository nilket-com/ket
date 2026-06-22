# ADR 320 results - HAVING scalar subquery recombine

## Outcome

q11 moved off the ADR 314 HAVING-scalar wall:

```text
q11 dev    = pass@execute
q11 strict = blocked(unsupported_decimal_arithmetic@bind)
```

Live default-scale runs:

```text
tpch_reachability query=q11 sf=0.01 mode=dev    phase=execute status=pass    reason=none digest=ArrowIpcDigest([141, 153, 28, 87, 121, 125, 41, 87, 196, 108, 66, 183, 246, 64, 251, 53, 224, 42, 106, 104, 72, 158, 212, 10, 31, 9, 27, 35, 250, 79, 117, 208])
tpch_reachability query=q11 sf=0.01 mode=strict phase=bind    status=blocked reason=unsupported_decimal_arithmetic digest=none
```

The strict row is the expected next wall: `ps_supplycost * ps_availqty` is Decimal128 x Int32. That names the next
Decimal arithmetic cut; this ADR's acceptance was movement off `unsupported_having_scalar_subquery@plan`, not q11
strict green.

## Phase 0 Finding

The old exchange-placement objection was still real. Recombining the scalar with ADR 257's keyless `Cross` reaches
`BroadcastBuildSidePlacement`: the grouped aggregate side carries a terminal exchange, and the broadcast-star shape
requires a scan-linear probe.

The implementation therefore does not relax broadcast admission. HAVING scalars use a HAVING-specific recombine:

- project a side-specific hidden constant key onto the grouped aggregate output;
- project a matching hidden constant key beside the one-row scalar value;
- join them with a direct hash `Inner` join;
- apply the HAVING filter;
- project back to the aggregate's natural output names so `__scalar_N` and `__having_scalar_*` columns never leak.

This gives a legal distributed shape without changing the WHERE/projection scalar `Cross` path.

## Tests

`group_by_test::scalar_aggregate_subquery_in_having_filters` pins the recombine: each group compares against the
single global scalar and the visible schema contains no synthetic scalar/key columns.

`group_by_test::having_scalar_deferred_shapes_stay_rejected` keeps the cut narrow: non-aggregate HAVING scalars,
correlated HAVING scalars, and aggregate-query projection scalars still reject.

`architecture::scalar_subquery::adr320_having_scalar_recombines_post_aggregate` pins the planner-owned
side-specific synthetic-key hash join and the one-HAVING-scalar slice.

The q11 harness test now asserts the measured split above.

## Matrix Delta

Only q11 moved:

```text
q11 dev=pass@execute strict=blocked(unsupported_decimal_arithmetic@bind)
```

q18 and q21 remain the query-surface reach reds; q11 strict now names the Decimal arithmetic wall. q15 and q19
stay green. Correctness rows are unchanged.
