# ADR 318 results — q15 composite/aggregate CTE join keying

## Phase 0 diagnostic

Measured subcause: **A1 — equality not recognized because composite-side output ownership was opaque**.

The existing q15 helper proved the bind wall but stopped before a bound `ExecutionPlan` summary existed:

```text
bind q15: UnsupportedExpression { message: "CROSS JOIN requires a broadcast-eligible build side under the broadcast cap; no eligible side fits and a keyless hash-partition would collapse the cartesian to a single bucket" }
```

Grounding showed why the supplier/revenue equality did not key:

- `s_suppkey = supplier_no` reached a `Filter` over the supplier/revenue `Cross`; it was not solely stranded above the scalar-MAX wrapper.
- The `revenue` side is a projected grouped aggregate.
- The pre-bind cross-equi recognizer's logical scope treated `Project` and `Aggregate` as opaque, so it could not attribute `supplier_no` to the `revenue` side.
- Because the equality stayed residual, the join remained a keyless `Cross` and hit the broadcast-only Cross guard.

No A2 keyed-admission issue appeared after the equality was lifted: the existing keyed inner join path binds the composite aggregate input once it has real keys.

## Fix

Add logical-scope derivation for the safe passthrough subset the bound scope already preserves:

- `Project`: expose direct column passthrough outputs under their projected names.
- `Aggregate`: expose grouped direct column keys, plus aggregate output field names with unknown type for side ownership only.

Computed aggregate outputs are not made join-key-capable by this cut; only the grouped direct column (`l_suppkey` projected as `supplier_no`) becomes attributable for q15's key.

## Verification

Targeted proofs:

- `binder::tests::comma_join_keys_projected_aggregate_cte_group_key` passes. This is the minimal base-to-aggregate-CTE comma join shape; before the fix it hits the same keyless Cross guard.
- `harness::q15_executes_with_keyed_supplier_revenue_join` passes. The helper confirms the supplier/revenue join is keyed in both dev and strict modes, then q15 reaches execute at `sf=0.01`.

Observed row movement:

```text
q15 dev    blocked(unsupported_cross_join@bind) -> pass@execute
q15 strict blocked(unsupported_cross_join@bind) -> pass@execute
```

Live runs at `sf=0.01`:

```text
q15 dev    pass@execute 94.1 ms
q15 strict pass@execute 34.7 ms
```

## Scope boundary

This cut keys q15 only. The matrix confirmed q11/q18/q19/q21 remain red; q18 and q19 are not claimed as moved by this result:

- q18's grouped-membership body remains its own measured wall until re-run and proven otherwise.
- q19's disjunctive key extraction remains separate.

## Forward arc

Reach is now 18. The remaining reach reds stay scoped by their measured families: q19 OR-key extraction, q11 HAVING scalar, q18 grouped EXISTS body, and q21 non-equi EXISTS correlation.
