# ADR 314 results — tpch.q11

## Measured row

`q11` is implemented in the reachability harness, but it does not reach bind yet:

```text
q11 dev    = blocked(unsupported_having_scalar_subquery@plan)
q11 strict = blocked(unsupported_having_scalar_subquery@plan)
```

The raw wall is the frontend reject:

```text
unsupported frontend construct: scalar subqueries in GROUP BY/HAVING are not supported
```

The blocker is mode-independent and occurs before joins, Decimal arithmetic, or result materialization are reached.

## What landed

- Official-shaped q11 SQL, with `GERMANY`, `0.0001`, and no LIMIT.
- q11 fixture assembly over existing `partsupp`, `supplier`, and `nation` generators.
- CLI routing for `run --query q11 --mode dev|strict`.
- Static inventory marks q11 implemented.
- Telemetry now classifies the measured wall as `unsupported_having_scalar_subquery`, narrower than the generic query-surface bucket.
- Harness regression `q11_blocks_on_having_scalar_subquery` pins both dev and strict rows.

## Forward arc

The next engine cut is scalar subqueries in `HAVING` / aggregate-filter contexts. Only after that wall moves can q11 measure the join-keying shape and the strict Decimal arithmetic surface (`ps_supplycost * ps_availqty`, Decimal128 × Int32).
