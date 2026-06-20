# ADR 283 - Results: Decimal operand alignment

## Measured row

Before ADR 283:

```text
q17 dev=pass@execute   strict=blocked(unsupported_decimal_scalar@bind)
```

After ADR 283:

```text
q17 dev=pass@execute   strict=pass@execute
```

Live strict run at the default scale:

```text
tpch_reachability query=q17 sf=0.01 mode=strict phase=execute status=pass reason=none
digest=ArrowIpcDigest([82, 189, 203, 214, 236, 158, 115, 157, 109, 103, 154, 163, 100, 99, 39, 57, 119, 96, 152, 22, 170, 170, 249, 106, 147, 202, 39, 62, 200, 38, 50, 105])
```

## What moved

- **Comparison wall cleared.** The raw first wall was `Decimal128(15, 2)` compared with `Decimal128(38, 12)` in
  q17's `l_quantity < (0.2 * AVG(l_quantity))` predicate. The binder now derives a value-preserving common
  Decimal type for binary comparisons and inserts an internal Decimal cast where logical identities are naked or
  the same declared identity can be preserved.
- **Decimal cast executes.** `evaluate_cast` now handles `Decimal128 -> Decimal128` through Arrow's cast kernel.
  The Arrow probe pins exact scale widening and NULL preservation; Toron's binder owns range protection and
  rejects common comparison types that would exceed precision 38.
- **Division literal wall cleared.** `coerce_decimal_arithmetic_literal` now runs for `Div`, so
  `SUM(l_extendedprice) / 7.0` binds as Decimal/Decimal and uses the existing Decimal division kernel.

## Scope held

- Official q17 SQL is unchanged.
- No public SQL `CAST(... AS DECIMAL)` or Decimal `TRY_CAST` surface was added.
- `is_widening_cast` was deliberately not extended; CASE/UNION/IN-list/arithmetic implicit coercion is unchanged.
- IN-list Decimal alignment remains deferred; ADR 283 applies only to binary comparisons.
- This is reachability, not q17 answer correctness. A future q17 oracle owns division rounding policy.

## Tests

- `decimal_comparison_rescales_non_literal_operands`
- `decimal_comparison_precision_overflow_rejects_at_bind`
- `decimal_division_coerces_float_literal`
- `arrow_decimal128_cast_widens_scale_exactly`
- `q17_executes_in_both_modes`

## Forward arc

With strict q17 green, the next reach probes are q02 (new `partsupp` fixture plus correlated MIN scalar) and q22
(`substring` plus correlated predicates). The correctness rail can continue with q17 or q05.
