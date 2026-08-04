# ADR 293 - Results: tpch.q07 reachability

## Final row

q07 is now in the reachability gauge. It measures an honest red row in both modes:

```text
q07 dev=blocked(unsupported_derived_multi_source@plan)
q07 strict=blocked(unsupported_derived_multi_source@plan)
```

Live rows at `sf=0.01`:

```text
tpch_reachability query=q07 sf=0.01 mode=dev    phase=plan status=blocked reason=unsupported_derived_multi_source digest=none
tpch_reachability query=q07 sf=0.01 mode=strict phase=plan status=blocked reason=unsupported_derived_multi_source digest=none
```

## Measured wall

The first wall is earlier than the self-join alias probe the ADR expected to reach.

q07's official `shipping` derived table has a comma-list FROM body:

```sql
FROM supplier, lineitem, orders, customer, nation n1, nation n2
```

The current derived-table frontend still requires a derived-table subquery to have exactly one FROM source. The
raw planner wall is:

```text
derived table subquery must have exactly one FROM source
```

ADR 293 adds the narrow `unsupported_derived_multi_source` telemetry reason for that exact measured wall. It is
intentionally more precise than the generic `unsupported_query_surface` bucket, but it does not pretend q07 has
reached the later self-join question.

## What landed

- `Q7_DEV_SQL` and `STRICT_Q7_SQL`, using tpchgen-2.0.2's q07 shape with `FRANCE`/`GERMANY` inlined and no LIMIT.
- `run_dev_q07`, `run_strict_q07`, CLI route (`q07`/`q7`), static inventory entry, and matrix row.
- `q07_tables(strict, sf)` reuses the q05 table family but removes `region`.
- One physical `nation` table is registered; `nation n1` and `nation n2` remain SQL aliases. No `nation_1` /
  `nation_2` shortcut was introduced.
- Harness and architecture tests pin the measured red row and the single-physical-nation invariant.

## Board

Reachability after this cut:

```text
q01/q02/q03/q04/q05/q06/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
q07                                                dev=blocked@plan strict=blocked@plan
remaining queries                                  not_run
```

Correctness is unchanged:

```text
q01/q02/q03/q05/q06/q12/q14/q17  strict=pass@compare
```

## Forward

ADR 294 is the measured q07 prerequisite: derived-table multi-source/comma-join bodies. Once a derived table can
lower an official comma-list FROM body, q07 can re-measure and either reach the intended `nation n1`/`nation n2`
self-join probe or pass.
