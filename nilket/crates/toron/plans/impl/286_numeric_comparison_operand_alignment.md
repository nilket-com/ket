# ADR 286 — Implementation plan: numeric comparison operand alignment

Pairs with `plans/adr/286_numeric_comparison_operand_alignment.md`.

## Phase 0 — verify q20's exact wall

Before editing behavior, re-run q20 at the tiny gauge scale and capture the current bind error:

```text
scripts/cargo-cgroup run -p toron-tpch -- run --query q20 --mode dev --sf 0.0001 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q20 --mode strict --sf 0.0001 --allow-blocked
```

If needed, use a temporary local probe to print the raw bind error; remove it before sealing. Record the exact
strict RHS type in the results doc. Expected shape: dev is `Int32` vs `Float64`; strict is `Int32` vs a
Decimal-derived scalar expression.

## Phase 1 — binder comparison reconciliation

Files:

- `crates/toron/src/binder.rs`
- `crates/toron/src/binder/arithmetic.rs`

Steps:

1. Add `arithmetic::reconcile_numeric_comparison(builder, &mut left, &mut right)?`.
2. Wire it in `bind_comparison` after:

   ```rust
   coerce_comparison_literals(builder, left, right, ctx.session_timezone)?;
   arithmetic::reconcile_decimal_comparison(builder, &mut left, &mut right)?;
   ```

3. Keep `bind_in_list` unchanged. q20 nominates binary comparison alignment, not IN-list reconciliation.
4. Implement common-type helpers in `arithmetic.rs`:
   - primitive numeric comparison common type from the existing widening lattice;
   - integer-vs-Decimal common type using Decimal scale and integer range digits (`10` for `Int32`, `19` for
     `Int64`);
   - fail loud for precision `> 38`, negative scale, `precision < scale`, or unsupported pairs.
5. Preserve logical identity:
   - naked + naked -> naked output cast;
   - same declared identity -> preserve that identity on the inserted cast;
   - differing identities -> do not reconcile; let the existing nominal check reject.

Use a dedicated helper that appends casts directly. Do not extend `is_widening_cast`; that helper is shared by
CASE, IN-list, set-op, and arithmetic paths that this ADR does not intend to change.

## Phase 2 — executor cast arms

File:

- `crates/toron/src/exec.rs`

Add `evaluate_cast` arms:

```rust
(DataType::Int32, DataType::Decimal128(_, _))
| (DataType::Int64, DataType::Decimal128(_, _)) => {
    arrow::compute::cast(array, target)
        .map_err(|error| cast_failed(array.data_type(), target, error))
}
```

Do not add `Decimal128 -> Float64` in this cut. The strict q20 path should be handled by exact Int-to-Decimal
alignment. If Phase 0 somehow proves q20 needs Decimal-to-Float, stop and revise the ADR instead of silently
expanding this cut.

Architecture guard to expect: `architecture/sql_planner_surface.rs::adr025_cast_supports_numeric_primitive_lattice`
currently scans the `evaluate_cast` arms. Reconcile it by naming these ADR 286 internal comparison casts without
claiming the public explicit-cast policy changed.

## Phase 3 — tests

Focused engine tests should be value-discriminating and fail before the cut.

Candidate homes:

- `crates/toron/tests/correlated_scalar_test.rs` for q20-like scalar comparison shapes;
- binder or exec unit tests if a smaller fixture is clearer.

Required coverage:

1. `Int32` column compared to a non-literal `Float64` expression binds and executes.
2. `Int32` column compared to a non-literal `Decimal128` expression binds and executes.
3. `Int64` vs Decimal precision overflow rejects cleanly at bind.
4. Logical identity mismatch does not get silently erased by the inserted cast.
5. Existing literal comparison behavior still passes.

Update `crates/toron-tpch/tests/harness.rs`:

- rename/flip `q20_advances_to_numeric_comparison_wall`;
- assert q20 `Status::Pass`, `Phase::Execute`, and digest in both dev and strict modes if q20 turns green;
- keep the explicit 64MB-stack pattern from ADR 285.

If q20 instead reveals a deeper measured blocker, keep the harness assertion blocked with the new precise reason
and update telemetry only for that precise wall.

## Phase 4 — live q20 and matrix

Run:

```text
scripts/cargo-cgroup run -p toron-tpch -- run --query q20 --mode dev --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- run --query q20 --mode strict --sf 0.01 --allow-blocked
scripts/cargo-cgroup run -p toron-tpch -- matrix
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
```

If q20 is green, the matrix should render:

```text
q20 dev=pass@execute strict=pass@execute
```

Correctness rows should remain unchanged.

## Results doc

Write `crates/toron/plans/impl/286_numeric_comparison_operand_alignment_results.md` with:

- the pre-fix q20 row from ADR 285;
- the exact strict RHS type measured in Phase 0;
- the final q20 rows at `sf=0.0001` and `sf=0.01`;
- the comparison common-type policy that landed;
- any architecture guard reconciliation;
- unchanged prior reachability/correctness rows.

## Verification gate

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

## Seal

`impl: adr 286 numeric comparison operand alignment`. Do not commit wip drafts.
