# ADR 325 results - decimal-point expression literal preservation

## Outcome

ADR 325 closes the q11 correctness red that ADR 324 exposed:

```text
tpch_correctness query=q11 sf=0.01 mode=strict phase=compare status=pass reason=none
```

The literal `0.0001` now lowers as an exact `Decimal128(5,4)` expression literal instead of becoming `Float64` and then rounding to `0.00` at the Decimal operand's scale. Strict q11 therefore uses the real TPC-H threshold rather than `value > 0`.

## Implementation Notes

- Decimal-point expression literals lower from SQL text to exact `ScalarValue::Decimal128 { value, precision, scale }`.
- Integers keep the existing expression-literal path.
- Decimal-point literals with more than 38 significant digits reject during lowering; the implementation does not silently cap precision.
- Dev-mode Float contexts stay compatible through literal-only rewrites from Decimal128 literal to Float32/Float64 in arithmetic, comparison, CASE/COALESCE numeric widening, and explicit numeric/logical casts.
- This does not add a general user-visible Decimal-to-Float cast surface; nonliteral Decimal values still follow the existing cast rules.

## Measured Follow-On Effects

Exact decimal-point literals also changed the strict Decimal metadata for existing correctness oracles:

- q14: `100.00` is now scale 2, so `promo_revenue` is `Decimal128(38,10)` instead of the old `Decimal128(38,12)`.
- q17: `7.0` is now scale 1, so `avg_yearly` is `Decimal128(20,6)` instead of the old `Decimal128(21,6)`.

Both q14 and q17 still pass their independent raw-value correctness oracles after the schema and oracle math were retargeted to the exact literal scales.

## Verification Snapshot

- q11 strict correctness: pass@compare.
- q14 strict correctness: pass@compare with `Decimal128(38,10)`.
- q17 strict correctness: pass@compare with `Decimal128(20,6)`.
- Focused Decimal probe: `l_extendedprice * 0.0001` produces `Decimal128(21,6)` raw values `10000`, `20000`.
