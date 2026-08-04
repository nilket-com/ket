# ADR 286 — numeric comparison operand alignment

## Status

Proposed.

## Context

ADR 285 moved q20 past the frontend membership-body wall and measured the next blocker:

```text
q20 dev=blocked(unsupported_numeric_comparison_alignment@bind)
q20 strict=blocked(unsupported_numeric_comparison_alignment@bind)
```

The failing predicate is q20's availability test:

```sql
ps_availqty > (
  SELECT 0.5 * SUM(l_quantity)
  FROM lineitem
  WHERE l_partkey = ps_partkey
    AND l_suppkey = ps_suppkey
    AND l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
)
```

`ps_availqty` is `Int32`. The scalar expression is a non-literal numeric expression: `Float64` in the dev
fixture, Decimal-derived in the strict fixture. `bind_comparison` currently handles literal coercion and
Decimal-vs-Decimal scale reconciliation, then requires exact physical type equality. That was enough for ADR 283
(`Decimal128(p,s)` vs `Decimal128(p,s')`) but not for q20's `Int32` vs non-literal scalar comparison.

This ADR adds binder-inserted, comparison-only numeric operand alignment.

## Decision

Add a sibling helper to ADR 283's Decimal comparison reconciliation:

```text
coerce_comparison_literals(...)
reconcile_decimal_comparison(...)
reconcile_numeric_comparison(...)
```

`reconcile_numeric_comparison` inserts `Cast` nodes for comparison-local numeric alignment:

1. **Primitive numeric comparisons.** Reconcile non-literal primitive numeric operands using the existing widening
   lattice already accepted internally by `is_widening_cast`/`evaluate_cast`: `Int32 -> Int64`, `Int32 ->
   Float64`, `Int64 -> Float64`, and `Float32 -> Float64`. This inherits Toron's existing primitive widening
   policy; it is not a new exactness claim for every primitive pair. It is comparison-local and does not widen
   CASE, set ops, IN lists, or arithmetic.
2. **Integer vs Decimal comparisons.** Reconcile `Int32`/`Int64` against `Decimal128(p,s)` by casting the integer
   side to a Decimal type that preserves the integer's full range and the Decimal side's scale. Use a fail-loud
   common-type derivation:
   - integer digits required: `10` for `Int32`, `19` for `Int64`;
   - scale: the Decimal side's scale;
   - decimal integer digits: `decimal_precision - decimal_scale`;
   - precision: `max(decimal_integer_digits, integer_digits) + scale`;
   - reject if precision exceeds `38`, scale is negative, or `decimal_precision < decimal_scale`.
3. **Decimal-vs-Decimal remains ADR 283.** Keep `reconcile_decimal_comparison` as the Decimal scale/precision
   helper and do not duplicate it.
4. **Logical identity remains guarded.** Reconcile only naked operands or identical declared logical identities.
   If the logical identities differ, leave the existing nominal compatibility path to reject.

The executor gains the cast support needed by the new binder casts:

- `Int32 -> Decimal128(_, _)`
- `Int64 -> Decimal128(_, _)`

These cast arms delegate to `arrow::compute::cast`, matching ADR 283's Decimal-to-Decimal cast arm.

## Non-Decisions

- No public `CAST` or `TRY_CAST` surface expansion.
- No `Decimal128 -> Float64` non-literal comparison policy in this cut. q20's strict path can stay exact by
  casting the integer side to Decimal; a Decimal-to-float comparison policy is precision-losing and should be
  nominated by a real query before it lands.
- No IN-list, CASE, UNION/set-op, or arithmetic reconciliation changes.
- No q20 correctness oracle. This is reachability only.
- No runtime date-interval arithmetic. q20 still uses ADR 285's literalized upper date bound.

## Acceptance

Target:

```text
q20 dev=pass@execute
q20 strict=pass@execute
```

Both modes must also be tractable at `sf=0.01`. If q20 advances to a deeper blocker instead of passing, classify
that wall precisely and record it as the next ADR candidate; do not broaden this cut to absorb unrelated work.

## Verification

- Focused comparison tests:
  - `Int32` column vs non-literal `Float64` expression binds and executes.
  - `Int32` column vs non-literal `Decimal128` expression binds and executes.
  - `Int64` vs high-scale Decimal precision overflow rejects cleanly.
  - logical identity mismatch still rejects.
- q20 harness test flips from `blocked(unsupported_numeric_comparison_alignment@bind)` to `pass@execute`, or to a
  measured deeper blocker if one appears.
- `q20` live runs at `sf=0.01` in both modes.
- Full engine gate, because `bind_comparison` is shared:

```text
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
git diff --check
```

## Forward Arc

If q20 turns green, the reachability board reaches ten green strict/dev cells and the next council cycle can pick
between the next reachability query and the correctness rail. If q20 reveals a deeper wall, that measured row
names ADR 287.
