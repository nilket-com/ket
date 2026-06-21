# ADR 294 - Results: derived-table comma-list FROM body

## Final row

q07 moved from the ADR 293 plan wall to execute in both modes:

```text
q07 dev=pass@execute
q07 strict=pass@execute
```

Live rows at `sf=0.01`:

```text
tpch_reachability query=q07 sf=0.01 mode=dev    phase=execute status=pass reason=none digest=ArrowIpcDigest([254, 32, 180, 27, 15, 205, 250, 145, 236, 119, 186, 72, 64, 25, 40, 216, 242, 249, 182, 155, 169, 63, 145, 65, 146, 174, 105, 195, 241, 50, 177, 159])
tpch_reachability query=q07 sf=0.01 mode=strict phase=execute status=pass reason=none digest=ArrowIpcDigest([136, 88, 216, 196, 133, 221, 24, 47, 66, 56, 206, 253, 199, 162, 173, 169, 234, 39, 159, 12, 251, 68, 30, 151, 74, 254, 105, 79, 128, 38, 120, 19])
```

The default-scale run is the non-cartesian check: q07's derived body joins six relations. A keyless cartesian
would not complete at `sf=0.01`; the measured sub-second pass shows the comma-list body flows into the existing
keyed join recognition.

## What landed

- `validate_derived_select_shape` now accepts a derived-table body with one or more `FROM` entries.
- The new derived-source validator checks every `TableWithJoins.relation` and explicit `join.relation`, so a
  later comma entry cannot sneak in a nested derived factor.
- Lowering reuses the existing path: `lower_derived_table` still calls `lower_select`; `lower_select` still folds
  comma-list entries into keyless `Cross` joins; the binder's ADR 270 recognition keys the equality predicates.
- No new join engine, no new IR node, no harness query rewrite.

## Tests

- `derived_table_comma_list_body_executes` proves a comma-list derived body with an equality predicate binds and
  executes.
- `derived_table_comma_list_keeps_nested_derived_fences` proves nested derived factors remain fenced in both
  later comma entries and explicit join factors.
- `q07_executes_in_both_modes` flips the q07 gauge test from blocked to pass and runs on the same large-stack
  thread pattern used by other deep TPC-H harness tests.

## Findings

The expected self-join wall did not appear. q07's `nation n1, nation n2` aliases over one physical `nation` table
compose with the existing binder, so no ADR 295 self-join cut is needed from q07.

The old `unsupported_derived_multi_source` classifier remains only for the historical raw error string and
targeted regression probes. q07 no longer emits it.

## Board

Reachability after this cut:

```text
q01/q02/q03/q04/q05/q06/q07/q10/q12/q14/q17/q20  dev=pass@execute strict=pass@execute
remaining queries                                      not_run
```

Correctness is unchanged.

## Forward

q07 is green. q08 has the same derived comma-list shape, so it is the next natural reachability probe in this
family unless the council chooses to catch up the correctness rail first.
