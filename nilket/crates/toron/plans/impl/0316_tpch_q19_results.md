# ADR 316 results — tpch.q19 reachability

## Measured rows

`q19` is wired into the reachability harness with the official three-branch OR predicate.

```text
q19 dev    = blocked(unsupported_cross_join@bind)
q19 strict = blocked(unsupported_cross_join@bind)
```

Live gate rows at `sf=0.01` after the q19 keying guard:

```text
tpch_reachability query=q19 sf=0.01 mode=dev    phase=bind status=blocked reason=unsupported_cross_join digest=none
tpch_reachability query=q19 sf=0.01 mode=strict phase=bind status=blocked reason=unsupported_cross_join digest=none
```

## What fired first

The first minimal measurement hit the planned fixture contingency:

```text
UnknownColumn { column: "l_shipinstruct" }
```

`l_shipinstruct` was appended to the dev and strict lineitem fixtures, preserving existing column positions.

After that fixture gap moved, q19 no longer produced a fast bind reject. The default-scale live run crossed 60s with
no telemetry before interruption, which is the q16-style fake-green danger: the disjunction hid the join key, the
keyless `lineitem x part` cross was admitted, and execution started materializing an intractable cartesian.

The harness now blocks q19 pre-execution with a q19-specific keying guard. The guard binds the official q19 SQL and
requires a keyed `Inner` join over the lineitem/part equality. Today it reports a keyless `Cross`:

```text
q19 lineitem/part equality was not a keyed Inner join
```

That blocker is reported as `unsupported_cross_join@bind`.

## q15-family finding

q19 joins the keyless-cross family, but with a different cause than q15:

- q15: the equality is against a composite/aggregate CTE input, and the build side is not broadcast-eligible.
- q19: the equality is repeated inside OR branches, so the AND-only comma-join recognizer never extracts it; the
  resulting keyless cross can be broadcast-admitted but is not tractable at `sf=0.01`.

The shared blocker reason is useful family evidence, but the fixes are cause-specific. q19's next engine direction is
disjunction-aware join-key factoring, not the q15 composite-input join cut.

## Forward

q19 is an honest red row, not a new reach cell. Prior reachability and correctness cells are unchanged.

The remaining reach map now has q21 unprobed, plus measured red rows for q11, q15, q18, and q19. After q21, the
frontier can be prioritized from measured wall families rather than predictions.
