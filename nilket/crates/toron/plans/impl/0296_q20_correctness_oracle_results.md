# ADR 296 Results - q20 correctness oracle

## Measured row

`correctness --query q20` at `sf=0.01`:

```text
q20 strict=pass@compare
```

Reachability is unchanged; q20 was already `pass@execute` in both modes. This cut adds the strict answer check.

## Schema

Strict q20 output is schema-checked before comparing values:

| column | type |
|--------|------|
| `s_name` | `Utf8` |
| `s_address` | `Utf8` |

Any schema drift is `fail@compare`.

## Threshold

The oracle uses the exact strict predicate:

```text
ps_availqty * 10_000 > 50 * sum_qty_raw
```

`sum_qty_raw` is `SUM(l_quantity)` at scale 2 for the correlated `(l_partkey, l_suppkey)` group in the 1994
shipdate window. The `0.5` literal is coerced to `Decimal128(15,2)`, so `0.5 * SUM(l_quantity)` compares at
scale 4. There is no float conversion or rounding in the oracle.

## Non-vacuity at sf=0.01

Measured oracle counters:

| counter | value |
|---------|-------|
| emitted rows | 1 |
| selected forest partsupp rows | 64 |
| qualifying suppliers before CANADA filter | 35 |
| threshold pass rows | 45 |
| threshold fail rows | 19 |
| absent correlated groups | 19 |

The live data naturally exercises both threshold inclusion and exclusion. All measured threshold failures are
absent correlated groups, so the SQL rule `SUM(empty) = NULL` is live in the rail: those rows fail the comparison
instead of being treated as `SUM = 0`.

## Verification shape

The comparator checks:

- `s_name` / `s_address` nullness.
- answer bijection by unique `s_name`.
- exact `s_address` match.
- duplicate, missing, and extra supplier rejection.
- `ORDER BY s_name`.

The test suite also pins the threshold boundary (`>` excludes equality), absent-group NULL semantics, and
`forest%` as prefix rather than contains.
