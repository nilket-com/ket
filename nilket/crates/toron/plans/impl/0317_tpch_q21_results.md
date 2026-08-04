# ADR 317 results — tpch.q21 reachability

## Measured row

q21 is now wired into the reachability harness. Both modes block at the same first wall:

```text
q21 dev    = blocked(unsupported_query_surface@plan)
q21 strict = blocked(unsupported_query_surface@plan)
```

Raw plan error, measured with a temporary probe and then removed:

```text
unsupported frontend construct: unsupported EXISTS subquery predicate
```

The row reaches the correlated `EXISTS` / `NOT EXISTS` predicate-shape wall before bind, so the `o_orderstatus`
fixture contingency does not fire in this cut. The orders fixture remains unchanged.

## Classification

The raw message proves the broad query-surface bucket, but it does not name the non-equi correlated predicate
specifically enough to justify a narrower telemetry classifier. q21 therefore stays in
`unsupported_query_surface`, with a dedicated harness test pinning that boundary.

## Atlas

ADR 317 closes the TPC-H reachability atlas. The remaining measured reach walls are:

- q11 — scalar subquery in `HAVING`
- q15 — keyless cross over a composite/aggregate CTE input
- q18 — grouped membership-subquery body
- q19 — keyless cross because disjunction hides the comma-join key
- q21 — correlated `EXISTS` / `NOT EXISTS` predicate body (`unsupported EXISTS subquery predicate`)

The prior reach rows and correctness rows are unchanged by this harness-only cut.

## Verification notes

- `q21_blocks_on_correlated_exists_predicate_shape` pins the dev+strict rows at `sf=0.01`.
- `reason_for_error_keeps_q21_exists_predicate_in_query_surface_bucket` pins the deliberate broad classifier.
- Live gate runs should show q21 in the matrix as `blocked(unsupported_query_surface@plan)` in both modes.
