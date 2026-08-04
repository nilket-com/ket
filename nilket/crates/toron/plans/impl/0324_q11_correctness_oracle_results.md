# ADR 324 results - q11 correctness oracle

Implemented the q11 strict correctness oracle as an expected-red correctness cell. This cut is oracle-only: it
does not change Toron's numeric literal representation or Decimal coercion.

## Measured row

`correctness --query q11` at `sf=0.01` reaches compare and fails with the intended reason:

```text
tpch_correctness query=q11 sf=0.01 mode=strict phase=compare status=fail reason=decimal_literal_scale
```

The command exits non-zero because correctness failures are process failures. The test suite stays green by
asserting that this red is expected and carries the exact `decimal_literal_scale` reason.

## Oracle

The oracle traverses `partsupp`, `supplier`, and `nation` directly, filters to `n_name = 'GERMANY'`, groups by
`ps_partkey`, and compares the exact TPC-H threshold without using floats:

```text
value_raw * 10000 > global_value_raw
```

At `sf=0.01`, the oracle is non-vacuous:

```text
rows=359
total_german_parts=374
positive_german_parts=374
threshold_fail_groups=15
global_value_raw=95025425444
```

The current strict engine includes the 15 positive groups that should fail the exact threshold because `0.0001`
has already lowered to `Float64` and is then rounded to Decimal scale 2 (`0.00`) during coercion. That collapses
the HAVING threshold to zero.

## Boundaries

- `decimal_literal_scale` is emitted by the q11 correctness comparator, not by reachability error classification.
- `decimal_literal_scale` is not a recognized blocker and is not added to `reason_for_error`.
- Reachability remains 22/22.
- q11 remains unchecked for correctness only in the sense that it is not a pass; the matrix now records the known
  strict failure as `q11 strict=fail(decimal_literal_scale@compare)`.

## Follow-on

ADR 325 should preserve exact numeric/decimal literal scale through lowering/binding so `0.0001` can survive as a
scale-bearing Decimal operand. That cut should flip q11 strict from `fail@compare` to `pass@compare`.

