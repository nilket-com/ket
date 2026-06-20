# ADR 283 — Implementation plan: Decimal operand alignment

Pairs with `plans/adr/283_decimal_operand_alignment.md`. Option L cast mechanism. Line numbers are current-tree
anchors, not promises.

## Phased (verify-first) order
Phase 0 done for the first wall: comparison `(15,2)` vs `(38,12)` is the measured raw bind error. The
`Div`-literal wall is code-grounded behind it (`coerce_decimal_arithmetic_literal` excludes `Div`), then verified
after the comparison clears. Implement, re-probing the raw bind error between phases:
1. Exec Decimal→Decimal cast arm (D1 prerequisite).
2. Binder comparison reconciliation (D1) → strict q17 should advance off the comparison wall to the `Div` wall.
3. `Div` literal gate (D2) → strict q17 should reach `pass@execute`; else capture the raw error and name ADR 284.

## Step 1 — Exec: `Decimal128 → Decimal128` cast
`crates/toron/src/exec.rs`, `evaluate_cast` (match at `:7053`): add an arm before the catch-all, delegating to
Arrow (already used in this file, e.g. `arrow::compute::take` `:658`):

```rust
(DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
    arrow::compute::cast(array, target).map_err(|error| cast_failed(array.data_type(), target, error))
}
```
`cast_failed` (`:7100`) keeps the message style consistent. Do **not** add a Decimal `evaluate_try_cast` arm in
this cut unless it implements true try semantics and has its own test. The reconciliation emits a plain `Cast`,
and public Decimal `TRY_CAST` remains binder-gated.

## Step 2 — Binder: common-type + reconciliation helpers
`crates/toron/src/binder.rs`, near `coerce_comparison_literals` (`:5463`).

**a. Common-type helper:**
```rust
/// ADR 283: value-preserving common Decimal type for comparing two Decimal128 operands. Align to the wider
/// scale; integer-digit budget is the max of both. `None` if precision would exceed 38 (caller fails loud).
fn decimal_comparison_common_type(p1: u8, s1: i8, p2: u8, s2: i8) -> Option<(u8, i8)> {
    if s1 < 0 || s2 < 0 {
        return None;
    }
    let scale = s1.max(s2);
    let int_digits = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
    if int_digits < 0 {
        return None;
    }
    let precision = int_digits + i16::from(scale);
    (1..=38).contains(&precision).then(|| (precision as u8, scale))
}
```

**b. Reconcile helper (Option L — `append_cast` directly; `is_widening_cast` untouched; logical-identity
guarded):**
```rust
fn reconcile_decimal_comparison(
    builder: &mut BoundExprProgramBuilder,
    left: &mut BoundExprId,
    right: &mut BoundExprId,
) -> Result<(), BindingError> {
    let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) =
        (builder.node_type(*left).cloned().expect("typed"), builder.node_type(*right).cloned().expect("typed"))
    else { return Ok(()) };
    if (p1, s1) == (p2, s2) { return Ok(()); }
    // Logical-identity guard: reconcile all-naked operands, or same-identity operands only if the inserted
    // cast output preserves that identity. A declared-identity mismatch is left to the existing nominal check
    // to reject (receipt doctrine: reconcile, never silently relabel).
    if !comparison_identities_are_reconcilable(builder, *left, *right) { return Ok(()); }
    let (p, s) = decimal_comparison_common_type(p1, s1, p2, s2).ok_or_else(|| BindingError::TypeMismatch {
        message: format!("Decimal comparison operands exceed precision 38: Decimal128({p1},{s1}) vs Decimal128({p2},{s2})"),
    })?;
    let common = DataType::Decimal128(p, s);
    if (p1, s1) != (p, s) { *left = append_decimal_comparison_cast(builder, *left, common.clone())?; }
    if (p2, s2) != (p, s) { *right = append_decimal_comparison_cast(builder, *right, common.clone())?; }
    Ok(())
}
```
`comparison_identities_are_reconcilable`: true when both operands' logical identities (`builder.node_logical_type`)
are `NAKED`, or when they are the same declared identity and `append_decimal_comparison_cast` preserves that
identity on the cast node via `set_node_logical_type`. If preserving identity proves noisy, restrict ADR 283 to
the all-naked case and record same-declared-identity rescale as a deferred sibling. q17's columns are naked, so
the required path fires either way. Pin the chosen behavior with the discriminating test.

## Step 3 — Binder: call the reconcile at the shared comparison sites
In `bind_comparison` (`:5160`), the `(false,false)` path: after `coerce_comparison_literals(...)?` (`:5222`) and
**before** the `left_type`/`right_type` fetch (`:5224`):
```rust
coerce_comparison_literals(builder, left, right, ctx.session_timezone)?;
reconcile_decimal_comparison(builder, &mut left, &mut right)?;   // ADR 283
let left_type = builder.node_type(left). ...
```
(`left`/`right` become `let mut`.) Do **not** apply this to the IN-list path in ADR 283. IN-list Decimal
alignment is not needed for q17 and has its own target/list-item state machine; name it as a deferred sibling if
it appears in a later query. The downstream `left_type != right_type` / `nominal_compatible` checks then pass on
the binary-comparison cast nodes.

## Step 4 — Binder: `Div` literal coercion
`crates/toron/src/binder/arithmetic.rs:1081` — extend the gate:
```rust
let (left_type, right_type) = if matches!(op,
    BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div) {
    coerce_decimal_arithmetic_literal(builder, ctx, left, right, &left_type, &right_type)?;
    ...
```
`coerce_decimal_arithmetic_literal` (`:1208`) is op-agnostic; `decimal_division_output_type` (`:1183`) + the exec
`Decimal/Decimal` division path carry the rest. Update the ADR 266/271 comment above the gate to add `Div`
(`SUM(...) / 7.0`).

## Step 5 — Tests
- **Engine-level** (a focused Decimal test home — e.g. a new `decimal_alignment_test.rs` or an existing decimal
  test file; **not** `correlated_scalar_test.rs`, which locks ADR 282 — keep concerns separate):
  - D1 executes: a `Decimal128(p1,s1)` column compared to a wider Decimal expression over matching rows → correct
    rows; revert Steps 1–3 to prove it bind-rejects pre-cut (revert→fail→restore attribution).
  - D1 fails loud: a comparison whose common precision would exceed 38 → `TypeMismatch` at bind (no panic, no cap).
  - D2: `SUM(<decimal>) / 7.0` binds + executes; pin output `(p,s)` from `decimal_division_output_type`.
- **Gauge** (`crates/toron-tpch/tests/harness.rs`): flip `q17_dev_executes_strict_hits_decimal_literal_wall`
  (rename e.g. `q17_executes_in_both_modes`): strict `Blocked`/`Bind`/`unsupported_decimal_scalar` → `Pass`/
  `Execute` with digest; dev unchanged. Correct the doc-comment (`:314–327`) — it calls the strict wall
  "`0.2`/`7.0` Float64 vs Decimal128," which this cut disproves; state the scale-reconciliation invariant.

## Step 6 — Results doc
Add `crates/toron/plans/impl/283_decimal_operand_alignment_results.md`: before/after q17 row; the raw first wall
`(15,2)` vs `(38,12)`; whether the `Div` wall appeared after the comparison fix; the exact common-type policy;
the logical-identity guard; the deferred IN-list sibling; the Option-L decision (and that `is_widening_cast` was
deliberately not extended).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test     # FULL — re-check ADR263/decimal guards
scripts/cargo-cgroup test -p toron --no-fail-fast               # whole suite (4412 baseline)
scripts/cargo-cgroup test -p toron-tpch                         # the gauge — q17 flips
git diff --check
```
Plus the empirical proof on the **built** binary (separate compile from runtime):
`target/debug/toron-tpch run --query q17 --mode strict --sf 0.0001 --allow-blocked` → `pass@execute`.

## Risk / blast radius
- `evaluate_cast` is shared (arithmetic widening at `:7035/7040` calls it) — the new arm is additive; the
  catch-all stays for genuinely-unsupported casts.
- The reconcile is at the shared comparison site, but the `(p,s)==(p,s)` / non-Decimal / non-reconcilable-identity
  early-returns make it a no-op for every existing case; the full `--no-fail-fast` leg is the regression net.
- `is_widening_cast` NOT touched → CASE/UNION/IN-list/arithmetic implicit coercion unchanged (the point of
  Option L).
- No new IR node / IR field. `--all-targets` is cheap insurance, not expected to surface anything.

## Seal
`impl: adr 283 decimal operand alignment`. Do not commit wip drafts.
