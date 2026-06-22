# ADR 315 results — tpch.q18 reachability

## Measured rows

`q18` is wired into the reachability harness with the official-shaped query, including `LIMIT 100`.

```text
q18 dev    = blocked(unsupported_query_surface@plan)
q18 strict = blocked(unsupported_query_surface@plan)
```

Live gate rows at `sf=0.01`:

```text
tpch_reachability query=q18 sf=0.01 mode=dev    phase=plan status=blocked reason=unsupported_query_surface digest=none
tpch_reachability query=q18 sf=0.01 mode=strict phase=plan status=blocked reason=unsupported_query_surface digest=none
```

## First wall

The first wall is the grouped membership-subquery body:

```sql
o_orderkey IN (
  SELECT l_orderkey
  FROM lineitem
  GROUP BY l_orderkey
  HAVING SUM(l_quantity) > 300
)
```

The raw planner reject is the shared `unsupported EXISTS subquery shape` path, so the harness keeps the broad
`unsupported_query_surface` classifier rather than minting a q18-specific reason from a generic message.

This fires before bind, so the intentionally deferred `o_totalprice` fixture gap is not reached. `LIMIT 100` is not
the measured wall; it is already represented by the existing `Take` / `Sort.limit` path.

## Shared-wall finding

No q15 shared composite-join evidence yet. q18 stops before the IN-over-grouped-aggregate input can be admitted, so it
does not yet confirm or refute whether q15's composite/aggregate-CTE join wall is shared with q18.

## Forward

Next engine direction from this row: membership subqueries over grouped aggregate bodies. After that lands, q18 should
be re-measured to determine whether it next hits the q15-like composite-join wall, an `o_totalprice` fixture gap, or a
different blocker.

Prior reachability and correctness cells are unchanged; q18 is an honest red row, not a new reach cell.
