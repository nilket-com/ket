# ADR 308 results - ON-clause residual pushdown

## Outcome

q13 moved off the ADR 307 plan wall and now reaches execute in both modes:

```text
q13 dev=pass@execute strict=pass@execute
```

Live runs at `sf=0.01`:

```text
tpch_reachability query=q13 sf=0.01 mode=dev phase=execute status=pass reason=none elapsed_ms=67.3 digest=ArrowIpcDigest([6, 95, 246, 6, 27, 55, 213, 177, 25, 224, 9, 168, 103, 227, 170, 45, 253, 215, 34, 121, 181, 93, 71, 245, 175, 11, 37, 250, 13, 221, 8, 35])
tpch_reachability query=q13 sf=0.01 mode=strict phase=execute status=pass reason=none elapsed_ms=65.4 digest=ArrowIpcDigest([6, 95, 246, 6, 27, 55, 213, 177, 25, 224, 9, 168, 103, 227, 170, 45, 253, 215, 34, 121, 181, 93, 71, 245, 175, 11, 37, 250, 13, 221, 8, 35])
```

## What landed

- `LogicalPlan::Join` now has a transient logical-only `on_filter`.
- SQL `JOIN ... ON` lowering splits column-column equality predicates into key pairs and keeps all other
  conjuncts as an ON residual. Equality against a literal is a residual, not a join key.
- The binder normalizes `on_filter` before binding:
  - `Inner` + one-side residual -> push into that side.
  - `LeftOuter` + right-side residual -> push into the right/null-supplying input.
  - `RightOuter` + left-side residual -> push into the left/null-supplying input.
  - preserved-side outer residuals, cross-side residuals, full outer residuals, constant/no-column residuals,
    and residual-only joins reject with `unsupported ON-clause residual`.
- `ExecutionPlan::Join`, executor, and fragmenter remain key-only and unchanged.
- The shared `orders` fixture now appends `o_comment` from `tpchgen::OrderGenerator`.

## Tests

Engine tests pin the semantic boundary:

- `sql_left_outer_on_right_residual_preserves_unmatched_left_rows` proves null-extension is preserved: a left row
  whose matching right row fails the residual remains as one null-extended row.
- `sql_inner_join_on_single_side_residual_executes` proves inner single-side residual pushdown executes.
- `sql_on_residual_rejects_unsupported_semantics` covers preserved-side outer residuals, cross-side residuals,
  full outer residuals, residual-only joins, and constant residuals.
- q13 harness coverage now asserts `pass@execute` with the measured digest in both modes.

## Gate

- `cargo fmt --check`: clean.
- `cargo check -p toron --all-targets`: clean.
- `cargo test -p toron --lib`: green.
- `cargo test -p toron --test architecture_test`: green.
- `cargo test -p toron --no-fail-fast`: green.
- `cargo test -p toron-tpch`: green.
- Live `toron-tpch run --query q13 --mode dev --sf 0.01`: `pass@execute`.
- Live `toron-tpch run --query q13 --mode strict --sf 0.01`: `pass@execute`.
- `matrix`: q13 reports `dev=pass@execute strict=pass@execute`.
- `correctness-matrix`: q13 remains `unchecked`.

## Forward

q13 has no next reachability wall in this cut. The old `unsupported_non_equi_join_on` classifier remains only for
historical telemetry and targeted regression probes.
