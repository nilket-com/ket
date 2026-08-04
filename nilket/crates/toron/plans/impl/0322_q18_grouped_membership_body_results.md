# ADR 322 — Results: q18 grouped membership-subquery body

## Outcome

q18 moved off the ADR 315 `unsupported_query_surface@plan` wall and now reaches execute in both harness modes:

```text
q18 dev    pass@execute  ArrowIpcDigest([28, 48, 18, 202, 14, 6, 96, 104, 59, 127, 243, 219, 154, 38, 231, 39, 85, 146, 182, 89, 30, 20, 12, 135, 97, 59, 15, 38, 211, 89, 7, 103])
q18 strict pass@execute  ArrowIpcDigest([63, 211, 92, 198, 125, 214, 217, 115, 117, 24, 126, 202, 144, 247, 214, 60, 136, 143, 103, 186, 184, 77, 143, 255, 130, 163, 188, 63, 1, 220, 59, 23])
```

The reachability board advances to 21/22. q21 remains the last reach red row.

## What Landed

- `IN` membership subqueries now have a narrow grouped-body admission path for `GROUP BY` plus aggregate-vs-literal `HAVING`.
- The grouped body is lowered through the existing grouped aggregate path, wrapped in a synthetic alias, and the semi-join keys on the body's projected output column.
- Grouped `EXISTS` and grouped `NOT IN` still reject; this cut intentionally does not relax the shared EXISTS validator or the NOT-IN anti path.
- The shared orders fixture now appends `o_totalprice`, as `Float64` in dev and `Decimal128(15,2)` in strict.

## Grounding Findings

After admitting the grouped body and adding `o_totalprice`, q18 first exposed `unsupported_cross_join@bind`. The missing piece was the output-filter pushdown through `LeftSemi`/`LeftAnti`: residuals above a semi/anti join must be classified against the join's emitted left scope, not against the hidden right input. Using both scopes made q18's outer `l_orderkey` collide with the grouped body's projected `l_orderkey`.

Qualifying the grouped body output behind a synthetic alias fixed the other half of that ambiguity. The membership key now resolves to `__toron_grouped_in.l_orderkey`, not the base table's `lineitem.l_orderkey`.

ADR 318's composite-input work did consolidate here: once the grouped body was admitted and the emitted-scope ambiguity was removed, the grouped-IN membership join keyed at sf=0.01 and executed without a cartesian fallback.

## Verification

- `exists_decorrelation_test` pins grouped `IN` execution, grouped `EXISTS`/`NOT IN` stay-rejected behavior, and the deferred correlated/scalar-HAVING grouped-IN cases.
- `toron-tpch` pins q18's keyed grouped-IN semi-join in both modes before accepting the pass rows.
- q18 correctness is not checked by this ADR; this is reachability only.
