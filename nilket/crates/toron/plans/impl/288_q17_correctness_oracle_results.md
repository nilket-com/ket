# ADR 288 - Results: q17 correctness oracle

## Outcome

Strict q17 is now answer-checked at the first measured non-empty scale:

```text
tpch_correctness query=q17 sf=0.02 mode=strict phase=compare status=pass reason=none
```

This adds the first correctness cell that exercises a correlated scalar subquery and Decimal division over
contributing rows. Reachability is unchanged.

## Scale Policy

q17 correctness deliberately does not use the rail default `sf=0.01`: that scale produces an empty result and
would let a NULL-equals-NULL comparison pass without exercising the division path. The CLI therefore resolves
`correctness --query q17` to `sf=0.02`, accepts explicit `--sf 0.02`, and rejects `--sf 0.01` as a harness usage
error with no telemetry row.

## Schema

The strict q17 output schema is asserted before comparison:

```text
avg_yearly Decimal128(21, 6)
```

A schema drift is `fail@compare`.

## Oracle

`strict_q17_oracle` traverses `tpchgen` directly:

- `part`: keep `p_brand = 'Brand#23'` and `p_container = 'MED BOX'`;
- `lineitem`: compute per-part `AVG(l_quantity)` from scale-2 quantity raws;
- threshold: `0.2 * AVG(l_quantity)` at scale 8;
- selected rows: eligible part keys with `l_quantity < threshold`;
- final value: `SUM(l_extendedprice) / 7.0`, using the same half-away-from-zero Decimal division rule as the
  executor.

The comparator checks exact raw `i128`, nullness, the measured `(p,s)`, and exactly one output row. The
non-vacuity guard requires at least one eligible part and at least one contributing lineitem.

## Tests

Added focused tests for:

- q17 end-to-end `pass@compare` at `sf=0.02`;
- non-vacuous oracle data at the correctness scale;
- Decimal division half-away rounding, including a negative quotient and divide-by-zero -> NULL;
- schema drift detection for `avg_yearly`;
- off-by-one raw mismatch;
- NULL, empty, and multi-row result rejection;
- CLI scale policy rejecting the masked-empty `sf=0.01`.

## Matrix

Correctness now has five strict pass cells:

```text
q01 strict=pass@compare
q03 strict=pass@compare
q05 strict=pass@compare
q06 strict=pass@compare
q17 strict=pass@compare
```

## Forward

q17 closes the first division-and-correlated-scalar correctness cell. The remaining correctness rail can move to
ratio queries such as q14, while the reachability frontier remains the harder correlated/nested query set.
