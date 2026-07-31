# ADR 321 results - Decimal x Int multiply

## Outcome

q11 moved off ADR 320's strict Decimal arithmetic wall:

```text
q11 dev    = pass@execute
q11 strict = pass@execute
```

Live default-scale runs:

```text
tpch_reachability query=q11 sf=0.01 mode=dev    phase=execute status=pass reason=none digest=ArrowIpcDigest([141, 153, 28, 87, 121, 125, 41, 87, 196, 108, 66, 183, 246, 64, 251, 53, 224, 42, 106, 104, 72, 158, 212, 10, 31, 9, 27, 35, 250, 79, 117, 208])
tpch_reachability query=q11 sf=0.01 mode=strict phase=execute status=pass reason=none digest=ArrowIpcDigest([143, 195, 243, 21, 27, 76, 161, 92, 136, 153, 5, 19, 83, 58, 186, 163, 4, 175, 4, 84, 244, 41, 132, 69, 2, 4, 38, 48, 199, 46, 87, 182])
```

The reachability matrix now has q11 green in both modes; q18 and q21 remain the reach reds.

## Engine Change

The binder now aligns a non-literal Int32/Int64 operand beside Decimal128 for multiplication only:

- Int32 -> `Decimal128(10,0)`
- Int64 -> `Decimal128(19,0)`
- `Decimal128(p,s) * Decimal128(int_prec,0)` reuses the existing ADR 263 Decimal multiply output rule:
  `Decimal128(min(38, p + int_prec + 1), s)`

The cast is an internal `append_cast`, not `cast_if_needed`; the primitive widening helper still does not admit
Int-to-Decimal. Execution uses the existing Arrow Int-to-Decimal cast and the existing Decimal x Decimal
`numeric::mul` path. No executor kernel was added.

## Tests

`group_by_test::decimal_multiply_coerces_non_literal_integer_operands` pins the core behavior:

- `Decimal128(15,2) * Int32` -> `Decimal128(26,2)`;
- `Decimal128(15,2) * Int64` -> `Decimal128(35,2)`;
- `Int32 * Decimal128(15,2)` is symmetric;
- a computed non-literal Int expression (`amount + 1`) is cast as a whole;
- wide Decimal x Int64 keeps the existing bind-time precision cap;
- Decimal x Int literal stays on ADR 266's literal path.

`harness::q11_executes_in_both_modes` pins the q11 reachability flip and both digests.

## Reach / Correctness Boundary

q11 green is a reachability result, not a correctness result. The HAVING threshold still contains
`SUM(...) * 0.0001`; today's Float-to-Decimal literal coercion rounds `0.0001` to the other operand's scale. The
future q11 correctness oracle must either account for that current behavior or follow a separate Decimal-literal
scale policy cut.

## Matrix Delta

Only q11 moved:

```text
q11 dev=pass@execute strict=pass@execute
```

All existing correctness rows are unchanged.
