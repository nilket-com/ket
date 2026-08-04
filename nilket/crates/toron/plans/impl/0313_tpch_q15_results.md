# ADR 313 results — tpch.q15 reachability

## Measured row

`q15` is wired into the reachability harness, but it does not reach execute yet:

```text
q15 dev=blocked(unsupported_cross_join@bind)
q15 strict=blocked(unsupported_cross_join@bind)
```

Live runs at `sf=0.01`:

```text
tpch_reachability query=q15 sf=0.01 mode=dev phase=bind status=blocked reason=unsupported_cross_join digest=none
tpch_reachability query=q15 sf=0.01 mode=strict phase=bind status=blocked reason=unsupported_cross_join digest=none
```

The raw bind wall is the keyless CROSS JOIN admission guard:

```text
CROSS JOIN requires a broadcast-eligible build side under the broadcast cap
```

ADR 313 therefore adds `unsupported_cross_join` as a precise recognized blocker for this guard, instead of
letting the row fall through the generic `unsupported_query_surface` bucket.

## Keyed-plan assertion

The q15-specific keyed-plan probe binds the official-shaped query over `supplier` + `lineitem` and checks for an
`Inner` keyed join containing the supplier/revenue fields. It currently fails before the join shape can be proven:

```text
bind q15: UnsupportedExpression { message: "CROSS JOIN requires a broadcast-eligible build side under the broadcast cap; no eligible side fits and a keyless hash-partition would collapse the cartesian to a single bucket" }
```

That is the important ADR 313 finding: q15 is attempted honestly, but the CTE/scalar-subquery shape still leaves a
keyless cross in the bound plan. A future engine cut should inspect that shape and make the supplier/revenue
equality keyable before q15 can count as a green reach cell.

## Future oracle facts

The implementation plan asked to record q15 non-emptiness and whether multiple suppliers tie at the max revenue.
Those facts are not measurable in this cut because q15 does not execute. The future q15 correctness oracle should
record them after the keyless-cross wall is removed.

## Board impact

Reachability remains at 17 green queries. `q15` is now an implemented, measured red row:

```text
q01 dev=pass@execute strict=pass@execute
q02 dev=pass@execute strict=pass@execute
q03 dev=pass@execute strict=pass@execute
q04 dev=pass@execute strict=pass@execute
q05 dev=pass@execute strict=pass@execute
q06 dev=pass@execute strict=pass@execute
q07 dev=pass@execute strict=pass@execute
q08 dev=pass@execute strict=pass@execute
q09 dev=pass@execute strict=pass@execute
q10 dev=pass@execute strict=pass@execute
q11 dev=not_run strict=not_run
q12 dev=pass@execute strict=pass@execute
q13 dev=pass@execute strict=pass@execute
q14 dev=pass@execute strict=pass@execute
q15 dev=blocked(unsupported_cross_join@bind) strict=blocked(unsupported_cross_join@bind)
q16 dev=pass@execute strict=pass@execute
q17 dev=pass@execute strict=pass@execute
q18 dev=not_run strict=not_run
q19 dev=not_run strict=not_run
q20 dev=pass@execute strict=pass@execute
q21 dev=not_run strict=not_run
q22 dev=pass@execute strict=pass@execute
```

Correctness is unchanged; q15 is still unchecked:

```text
q15 strict=unchecked
```
