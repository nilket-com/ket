# ADR 285 - Results: tpch.q20 reachability

## Measured row

Before ADR 285, q20 stopped in the frontend:

```text
q20 dev=blocked(unsupported_query_surface@plan)
raw: unsupported frontend construct: IN subquery is not supported
```

The unsupported shape was the body of the outer membership subquery:

```sql
ps_partkey IN (SELECT ...)
AND ps_availqty > (SELECT 0.5 * SUM(...) ...)
```

The old membership lowering could only classify simple residual predicates and correlation equalities in the
membership body. It could not compose the body's nested `IN` and correlated scalar subquery.

After ADR 285:

```text
q20 dev=blocked(unsupported_numeric_comparison_alignment@bind)
q20 strict=blocked(unsupported_numeric_comparison_alignment@bind)
```

Live gauge rows:

```text
tpch_reachability query=q20 sf=0.0001 mode=dev phase=bind status=blocked reason=unsupported_numeric_comparison_alignment
tpch_reachability query=q20 sf=0.0001 mode=strict phase=bind status=blocked reason=unsupported_numeric_comparison_alignment
```

Default-scale rows also stop at the same bind wall:

```text
tpch_reachability query=q20 sf=0.01 mode=dev phase=bind status=blocked reason=unsupported_numeric_comparison_alignment
tpch_reachability query=q20 sf=0.01 mode=strict phase=bind status=blocked reason=unsupported_numeric_comparison_alignment
```

That default-scale check matters because it proves the cut does not claim a tiny-scale cartesian pass. q20 does
not reach execution yet; it advances to a precise bind blocker.

## What moved

- **Uncorrelated plain `IN` bodies now use the filtering pipeline.** For the q20 shape, `lower_membership_subquery`
  lowers the membership body selection through scalar pull-up plus `lower_where`, using the body plan's
  qualifiers as the scalar-subquery outer scope.
- **Nested `IN` composes inside a membership body.** The body's `ps_partkey IN (SELECT p_partkey FROM part ...)`
  is lowered by the same path as an ordinary WHERE membership predicate.
- **A correlated aggregate scalar composes inside that same body.** The body's `SELECT 0.5 * SUM(l_quantity) ...`
  is pulled up with the `partsupp` body as the outer scope, so the scalar reaches the already-landed correlated
  scalar machinery.
- **Correlated membership and `NOT IN` remain on their existing restricted paths.** The new filtering pipeline is
  only selected for uncorrelated plain `IN` body predicates; the null-aware `NOT IN` path is unchanged.

## Harness

Added q20 to the TPC-H reachability harness:

- official-shaped q20 text with parameters inlined (`forest%`, `CANADA`);
- upper interval bound literalized to `DATE '1995-01-01'`;
- `q20_tables(strict, sf)` over supplier, nation, partsupp, part, and lineitem;
- appended `part.p_name` for the `forest%` predicate;
- `run_dev_q20`, `run_strict_q20`, CLI route, status row, and matrix row.

Every q20 table is seeded with optimizer facts through the normal harness path.

## Scope held

- Reachability only; no q20 answer oracle.
- No date-interval runtime kernel; the upper bound is literalized.
- No general numeric comparison alignment; that is the measured next wall.
- No correlated membership-body generalization beyond the uncorrelated plain `IN` q20 shape.
- No `NOT IN` behavior change.

## Tests

- `correlated_scalar_test::membership_body_composes_nested_in_and_correlated_scalar`
- `harness::q20_advances_to_numeric_comparison_wall`
- `harness::reason_for_error_classifies_numeric_comparison_alignment_precisely`

## Forward arc

ADR 286: numeric comparison alignment for non-literal scalar expressions, starting with q20's
`ps_availqty > 0.5 * SUM(l_quantity)` (`Int32` vs `Float64` in dev, `Int32` vs Decimal-derived strict).
