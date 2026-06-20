# ADR 284 - Results: tpch.q02 reachability

## Measured row

Before ADR 284, q02 stopped before binding the query shape:

```text
q02 dev=blocked(unsupported_query_surface@plan)
raw: non-aggregate scalar subqueries are not supported
```

After ADR 284:

```text
q02 dev=pass@execute
q02 strict=pass@execute
```

Live default-scale runs:

```text
tpch_reachability query=q02 sf=0.01 mode=dev phase=execute status=pass reason=none
digest=ArrowIpcDigest([86, 129, 173, 48, 180, 12, 240, 126, 41, 240, 24, 119, 218, 183, 111, 181, 251, 119, 169, 146, 9, 69, 198, 226, 198, 216, 153, 173, 98, 16, 218, 96])

tpch_reachability query=q02 sf=0.01 mode=strict phase=execute status=pass reason=none
digest=ArrowIpcDigest([114, 173, 180, 209, 37, 248, 12, 215, 166, 64, 71, 119, 217, 21, 144, 195, 72, 227, 235, 34, 121, 179, 247, 165, 59, 19, 181, 62, 191, 130, 109, 164])
```

## What moved

- **Multi-table inner scalar accepted.** The scalar-aggregate shape gate no longer conflates "supported scalar
  aggregate" with "single FROM source". The deferred correlated-scalar path can now carry a comma-list inner
  `FROM`; the uncorrelated and qualified-correlated scalar paths keep the single-source gate.
- **The full inner `FROM` is lowered.** `DeferredScalarSubquery.inner` now receives a left-deep comma join over
  every inner source instead of `from[0]` only.
- **Inner residual joins are keyed before bind.** `bind_deferred_scalar_subquery` derives logical scopes without
  binding the raw inner comma list, classifies the predicates, builds the inner residual filter, then reruns
  comma-join recognition before the grouped aggregate. The focused oversized-facts test fails if that inner
  residual equality remains a keyless Cross.
- **Decimal `MIN` executes.** Strict q02 exposed the next measured wall behind the scalar shape:
  `MIN(Decimal128(15,2))`. MIN/MAX now accept Decimal128 input and materialize Decimal128 output with the
  aggregate output type's precision and scale.

## Harness

q02 uses the official-shaped TPC-H text with parameters inlined (`15`, `BRASS`, `EUROPE`) and no LIMIT.

Added fixtures:

- `partsupp`: `ps_partkey`, `ps_suppkey`, `ps_availqty`, `ps_supplycost`
- appended `part`: `p_size`, `p_mfgr`
- appended `supplier`: `s_name`, `s_address`, `s_phone`, `s_acctbal`, `s_comment`

`ps_supplycost` and `s_acctbal` are `Float64` in dev mode and `Decimal128(15,2)` in strict mode. Every q02 table
is seeded with optimizer facts, matching the reachability harness's "complete fairly" rule.

## Scope held

- Official q02 SQL shape is unchanged: no pre-qualification, no LIMIT, no scalar rewrite.
- The cut is reachability only; q02 answer correctness is a later oracle.
- Explicit JOIN syntax inside scalar subqueries remains deferred.
- Qualified multi-table correlated scalars still take the older single-source path and remain out of scope.
- No cost-based join reordering claim; q02's default-scale pass proves keyed execution, not optimal join order.

## Tests

- `correlated_scalar_test::deferred_correlated_scalar_with_multi_table_inner_executes`
- `correlated_scalar_test::deferred_correlated_scalar_multi_table_inner_is_keyed`
- `correlated_scalar_test::multi_table_non_aggregate_scalar_still_rejects`
- `correlated_scalar_test::deferred_correlated_scalar_decimal_min_executes`
- `harness::q02_executes_in_both_modes`

## Forward arc

Reachability now has q01/q02/q03/q04/q05/q06/q10/q14/q17 green in both modes. Next candidates are q22
(`SUBSTRING` plus correlated predicates), q20 (nested membership), and the q05/q17/q02 correctness rail.
