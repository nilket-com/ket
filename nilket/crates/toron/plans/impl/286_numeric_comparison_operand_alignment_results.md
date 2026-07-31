# ADR 286 - Results: numeric comparison operand alignment

## Pre-Fix Wall

ADR 285 left q20 at a recognized bind wall:

```text
q20 dev=blocked(unsupported_numeric_comparison_alignment@bind)
q20 strict=blocked(unsupported_numeric_comparison_alignment@bind)
```

Phase 0 re-measured the raw errors at `sf=0.0001` before editing behavior:

```text
dev:    comparison operands have different types: Int32 and Float64
strict: comparison operands have different types: Int32 and Decimal128(31, 4)
```

The strict RHS is `0.5 * SUM(l_quantity)`: `l_quantity` is `Decimal128(15,2)`, `SUM` carries that
scale, the Decimal literal coerces to `Decimal128(15,2)`, and `*` widens to `Decimal128(31,4)`.

## What Landed

`bind_comparison` now runs:

```text
coerce_comparison_literals(...)
reconcile_decimal_comparison(...)
reconcile_numeric_comparison(...)
```

The new helper is comparison-local. It does not change `bind_in_list`, CASE, set operations, arithmetic, or
the explicit CAST policy.

Policy:

- Primitive non-literal numeric comparisons use the existing internal widening lattice:
  `Int32 -> Int64`, `Int32 -> Float64`, `Int64 -> Float64`, `Float32 -> Float64`.
- `Int32`/`Int64` compared with `Decimal128(p,s)` casts both sides as needed to a Decimal common type with
  the Decimal side's scale and enough integer digits for both operands.
- Integer/Decimal precision overflow rejects at bind instead of silently capping beyond Decimal128's `38`
  digit limit.
- Reconciliation only applies to naked/naked operands or identical declared logical identities. Different
  logical identities remain rejected by the existing mismatch path.
- Mixed numeric literal-vs-literal comparisons are left on the pre-existing path. ADR 286 aligns non-literal
  comparison operands; literal coercion remains owned by the existing literal rewrite helper.

Executor support is limited to the binder-inserted casts this helper emits:

```text
Int32 -> Decimal128(_, _)
Int64 -> Decimal128(_, _)
```

Those arms delegate to `arrow::compute::cast`, matching ADR 283's Decimal-to-Decimal rescale cast. No
`Decimal128 -> Float64`, public `CAST`, or `TRY_CAST` surface was added.

## Final q20 Rows

Tiny gauge scale:

```text
tpch_reachability query=q20 sf=0.0001 mode=dev    phase=execute status=pass
tpch_reachability query=q20 sf=0.0001 mode=strict phase=execute status=pass
```

Default harness scale:

```text
tpch_reachability query=q20 sf=0.01 mode=dev    phase=execute status=pass
tpch_reachability query=q20 sf=0.01 mode=strict phase=execute status=pass
```

Both default-scale runs completed in about 100 ms and produced the same digest:

```text
ArrowIpcDigest([213, 55, 160, 72, 166, 249, 107, 152, 13, 9, 168, 88, 161, 27, 175, 132, 56, 254, 12, 176, 199, 77, 164, 72, 117, 181, 96, 72, 243, 244, 30, 94])
```

## Tests And Guards

Added focused tests for:

- `Int32` column vs non-literal `Float64` scalar expression;
- `Int32` column vs non-literal `Decimal128` scalar expression;
- `Int64` vs high-scale `Decimal128(38,20)` precision overflow;
- different logical identities with mixed primitive carriers still rejecting.

The q20 harness test now asserts `pass@execute` in both modes while keeping the explicit 64 MB test stack.

Architecture guard reconciliation: `adr286_int_decimal_cast_is_internal_comparison_support_only` pins the
new executor Int-to-Decimal cast arms and verifies the explicit public CAST policy still contains no Decimal
surface.

## Matrix

q20 joins the green reachability cells. The prior correctness cells are unchanged:

```text
q01/q02/q03/q04/q05/q06/q10/q14/q17/q20: dev=pass@execute strict=pass@execute
q01/q03/q06: strict=pass@compare
```

## Forward

The q20 reachability wall is closed. The next council cycle can choose between the next reachability probe
and extending the correctness rail.
