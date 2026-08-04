# ADR 319 results - q19 OR-key extraction

## Outcome

q19 moved from the ADR 316 keyless-cross red row to keyed reachability:

```text
q19 dev    = pass@execute
q19 strict = pass@execute
```

Live default-scale runs:

```text
tpch_reachability query=q19 sf=0.01 mode=dev    phase=execute status=pass reason=none digest=ArrowIpcDigest([97, 19, 247, 248, 113, 199, 24, 110, 186, 115, 56, 106, 92, 251, 41, 171, 158, 20, 244, 54, 115, 223, 160, 195, 57, 99, 252, 163, 164, 184, 95, 131])
tpch_reachability query=q19 sf=0.01 mode=strict phase=execute status=pass reason=none digest=ArrowIpcDigest([178, 144, 2, 201, 111, 119, 123, 187, 42, 239, 40, 185, 90, 169, 245, 113, 124, 195, 77, 103, 178, 121, 7, 40, 176, 233, 222, 135, 47, 111, 172, 188])
```

The q19 harness still binds the official SQL first and asserts the `lineitem`/`part` join is keyed before executing,
so this is not a hidden cartesian pass.

## What changed

`recognize_cross_equi_joins` now handles an OR conjunct whose every branch contains the same bridge equality. Inside
`lift_cross_equi`, after left/right scopes are known, it:

- flattens the OR tree;
- splits each branch on AND;
- orients bridging equality candidates with the existing side classifier;
- lifts only oriented key pairs present in every branch;
- leaves the non-key branch predicates as a residual OR above the keyed inner join.

This is not a generic boolean normalizer. Common non-key predicates stay in the residual OR, and OR branches without
a common bridge key stay opaque.

## Tests

New behavior tests in `cross_join_test` cover:

- q19-shaped OR branches keying an oversized comma join;
- reversed equality spelling in one branch;
- branch-missing-key and no-common-key controls still failing as keyless Cross under the oversized-build lever;
- extracted-key results matching the explicitly factored predicate over nullable join keys.

The q19 harness test now asserts both dev and strict modes pass at `sf=0.01` after the keyed-plan guard succeeds.
An architecture guard pins the helper in the binder recognizer rather than a SQL rewrite.

## Matrix Delta

Only q19 is intended to move. q15 remains green from ADR 318's composite/aggregate CTE keying cut, and q18 remains
in its grouped-membership/EXISTS-body family until a separate engine cut measures it again. The reachability matrix
now has 19 pass@execute cells and three reds (q11, q18, q21).
