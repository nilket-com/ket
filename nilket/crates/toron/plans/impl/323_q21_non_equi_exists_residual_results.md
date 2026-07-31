# ADR 323 results — q21 non-equi correlated EXISTS residual

## Outcome

q21 moved from the measured ADR 317 wall to execute in both modes:

```text
q21 dev=pass@execute
q21 strict=pass@execute
```

The full reachability matrix is now green: q01 through q22 all reach execute in dev and strict modes.

## What changed

- `EXISTS` / `NOT EXISTS` predicates now classify a non-equi cross-side correlated predicate as a keyed
  semi/anti join match residual when at least one equality correlation key is present.
- The binder carries that residual as an optional `match_filter` bound against the combined left+right input
  schema for keyed semi/anti joins.
- The hash-join probe evaluates the residual inside the keyed match group. For `NOT EXISTS`, a right row
  suppresses the left row only when both the equality key and the residual are true.
- The old side-local ON residual pushdown still handles ordinary joins; the cross-side semi/anti residual is
  preserved through normalization for binding as a match filter.
- The shared orders fixture gained append-only `o_orderstatus`, unmasking q21's final order-status filter.

## Tests

- `exists_non_equi_correlated_residual_filters_matches_inside_probe` proves `EXISTS` does not ignore the
  residual.
- `not_exists_non_equi_correlated_residual_filters_matches_inside_probe` proves anti-join semantics use
  `key && residual`, not key-only matching.
- `exists_non_equi_correlated_residual_composes_with_inner_residual` pins the q21-style composition of inner
  residual plus cross-side residual.
- `non_equi_only_exists_correlation_stays_rejected` keeps the non-equi-only cartesian correlation boundary.
- `q21_executes_in_both_modes` pins the TPC-H row.

## Remaining boundary

Non-equi-only EXISTS correlation remains unsupported and is classified as
`unsupported_non_equi_exists_correlation`. A residual is executable only inside an already-keyed semi/anti
probe.

## Matrix

Reachability after this cut:

```text
q01..q22 dev=pass@execute strict=pass@execute
```

Correctness is unchanged by this reach cut. q21 remains unchecked on the correctness rail.
