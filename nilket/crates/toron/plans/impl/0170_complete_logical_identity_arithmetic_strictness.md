# Impl Plan - ADR 170: Complete Logical-Identity Arithmetic Strictness

Pairs with `plans/adr/0170_complete_logical_identity_arithmetic_strictness.md`.

Contained Toron binder-domain change. Extends ADR 169's identity reject from `Add`
to `Sub`/`Mul`/`Div`, generalizes the binder-local naming and untyped-NULL guard,
and pins refinement-in-scope. No executor, aggregate-kernel, PBC, Wendelon,
storage, wire, or optimizer-fact change. No aggregate behavior change beyond the
diagnostic wording (169 settled `SUM`/`AVG`).

**Blast radius (audited): zero.** Crate-wide grep finds no existing identity
`-`/`*`/`/` usage, so this cut rewrites no landed query. The only edits to landed
code are the D2 naming/string travel on affected 169 tests and guards.

## Anchors

| Concern | Anchor |
|---|---|
| Arithmetic operator enum | `BoundBinaryOp` in `crates/toron/src/ir/execution.rs:58` (`Add`,`Sub`,`Mul`,`Div`; no `Mod`) |
| Scalar arithmetic seam | `bind_arithmetic` (`binder.rs:9163`); Add-only logical gate at `binder.rs:9271` |
| Untyped-NULL arms | `match (is_untyped_null(left), is_untyped_null(right))`: reject calls at `binder.rs:9183` and `9204` |
| 169 identity gate | `bind_logical_addition` (`binder.rs:9331`) -> generalize to `bind_logical_arithmetic` |
| 169 null guard | `reject_logical_addition_with_untyped_null` (`binder.rs:9371`) |
| Domain classifier | `AdditiveLogicalDomain`, `additive_logical_domain_for_node` (`9301`) / `additive_logical_domain_for_type` (`9312`) -> `LogicalArithmeticDomain`, `logical_arithmetic_domain_for_node/type` |
| Diagnostic | `non_additive_logical_type_error` (`binder.rs:9585`) -> `non_arithmetic_logical_type_error` |
| Quantity arithmetic (unchanged) | `bind_dimensioned_arithmetic` (`binder.rs:9387`; `Add|Sub`@9409, `Mul`@9427, `Div`@9453), `logical_unit_for_node` (`9529`) |
| Aggregate identity reject (caller) | `additive_unit_aggregate_logical_type` (`binder.rs:10663`; error @`10678`) |
| Rune builders | `RuneExpr::sub`/`mul`/`div` (`rune_engine.rs:916/923/930`) -> `Expr::Sub/Mul/Div` |
| Behavior tests | `crates/toron/tests/logical_type_ddl_test.rs`, `rune_relation_handles_test.rs` |
| Architecture tests | `crates/toron/tests/architecture/nominal_type_identity.rs` |

## Phase 1 - Generalize the logical-arithmetic gate

Rename the widened binder-local surface:

- `AdditiveLogicalDomain` -> `LogicalArithmeticDomain`;
- `additive_logical_domain_for_node` -> `logical_arithmetic_domain_for_node`;
- `additive_logical_domain_for_type` -> `logical_arithmetic_domain_for_type`;
- `bind_logical_addition` -> `bind_logical_arithmetic`.

Add an `op` param to the scalar gate:

```rust
fn bind_logical_arithmetic(
    builder: &mut BoundExprProgramBuilder,
    ctx: &ExprBindContext<'_>,
    op: &BoundBinaryOp,
    left: BoundExprId,
    right: BoundExprId,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<Option<BoundExprId>, BindingError>
```

Body (identity check first, so mixed `identity ∘ quantity` rejects too):

```rust
let left_logical = builder.node_logical_type(left).expect("...");
let left_domain = logical_arithmetic_domain_for_node(ctx, builder, left)?;
let right_domain = logical_arithmetic_domain_for_node(ctx, builder, right)?;
if let LogicalArithmeticDomain::Identity(spec) = &left_domain {
    return Err(non_arithmetic_logical_type_error(&spec.name, op_symbol(op)));
}
if let LogicalArithmeticDomain::Identity(spec) = &right_domain {
    return Err(non_arithmetic_logical_type_error(&spec.name, op_symbol(op)));
}
if !matches!(op, BoundBinaryOp::Add) {
    return Ok(None); // Quantity/Naked Sub/Mul/Div -> bind_dimensioned_arithmetic
}
match (left_domain, right_domain) {
    (Naked, Naked) => Ok(None),
    (Quantity(l), Quantity(r)) => { require_compatible_units(&l, &r)?;
        Ok(Some(append_dimensioned_arithmetic(builder, BoundBinaryOp::Add, left, right,
            left_type, right_type, left_logical)?)) }
    (Quantity(_), Naked) | (Naked, Quantity(_)) => Err(unit_type_mismatch(
        "unit arithmetic requires both operands to be the same unit; CONVERT explicitly before combining with a scalar")),
    _ => unreachable!("identity operands handled above"),
}
```

Add `fn op_symbol(op: &BoundBinaryOp) -> &'static str` (`Add=>"+"`, `Sub=>"-"`,
`Mul=>"*"`, `Div=>"/"`).

In `bind_arithmetic`, widen the Add-only gate (`binder.rs:9271`):

```rust
if matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Sub
              | BoundBinaryOp::Mul | BoundBinaryOp::Div)
    && let Some(output) =
        bind_logical_arithmetic(builder, ctx, &op, left, right, &left_type, &right_type)?
{
    return Ok(output);
}
```

Must stay **before** `bind_dimensioned_arithmetic` and the physical fallback.

## Phase 2 - Generalize the untyped-NULL guard

Rename `reject_logical_addition_with_untyped_null` ->
`reject_logical_arithmetic_with_untyped_null`, thread `op`. In both
`is_untyped_null` arms (reject calls at `9183`, `9204`) replace:

```rust
if matches!(op, BoundBinaryOp::Add) {
    reject_logical_addition_with_untyped_null(builder, ctx, typed_operand)?;
}
```

with the four-op match calling the renamed guard with `&op`. Identity operand ->
`non_arithmetic_logical_type_error(&spec.name, op_symbol(op))`.

Quantity operand -> reject as well:

```rust
LogicalArithmeticDomain::Quantity(_) => Err(unit_type_mismatch(
    "unit arithmetic with untyped NULL requires an explicit typed NULL or primitive CAST",
))
```

Rationale: this branch returns before `bind_dimensioned_arithmetic`, so accepting
a quantity operand with untyped NULL can silently append a naked physical binary.
Explicit typed NULL remains available where ADR 167 already allows a naked scalar
operand (`unit * scalar`, `unit / scalar`).

## Phase 3 - Generalize the diagnostic (D2)

Rename `non_additive_logical_type_error` -> `non_arithmetic_logical_type_error`;
message:

```text
logical type {type_name} is not an arithmetic quantity for {context}; use an
explicit primitive CAST to perform raw numeric arithmetic
```

Update the aggregate caller (`@10678`, `{context}` = `"SUM"`/`"AVG"`) and the two
scalar sites. Travels (in-scope, behavior-neutral): update 169's
`strict_additive_operations_reject_identity` asserted substring ("not an additive
quantity" -> "not an arithmetic quantity") and the 169 guards
`adr169_addition_gates_logical_identity_before_numeric_fallback` /
`adr169_sum_avg_reject_non_additive_identity` that reference the old fn name.
Retarget any guard that references `AdditiveLogicalDomain` or
`additive_logical_domain_*` to the new logical-arithmetic names.

## Phase 4 - Behavioral tests

In `logical_type_ddl_test.rs` (reuse `aggregate_samples`; `setup_person_id_people`
for refinement):

- `strict_identity_arithmetic_rejects_all_operators` - `+`/`-`/`*`/`/`, plus
  same-identity `customer_id - customer_id`, `* customer_id`, and
  `/ customer_id`, reject with "not an arithmetic quantity".
- `explicit_cast_erases_identity_for_all_arithmetic` - `CAST(customer_id AS BIGINT)
  + 1` / `- 1` / `* 2` / `/ 2` bind naked and execute (assert values).
- `quantity_subtraction_and_scalar_scaling_unchanged` - `distance - distance`,
  `distance * 2`, `2 * distance`, `distance / 2` keep `Mile` (assert identity +
  values); same-dimension different-unit subtraction still requires `CONVERT`.
- `refinement_identity_arithmetic_rejects` - `people.id`: `+ 0`/`- 1`/`* 2`/`/ 2`
  plus `SUM`/`AVG` reject; `CAST(id AS BIGINT) ∘ k` binds naked.
- `logical_null_arithmetic_rejects_for_all_operators` - identity operands such as
  `customer_id - NULL`, `NULL * customer_id`, and `customer_id / NULL` reject;
  quantity operands opposite untyped NULL reject; typed scalar NULL for quantity
  scaling binds where ADR 167 allows a naked scalar.
- `naked_numeric_arithmetic_unchanged` - raw numeric `+`/`-`/`*`/`/` stay naked
  and execute.

In `rune_relation_handles_test.rs`:

- `rune_identity_arithmetic_rejects_and_cast_erases_all_operators` - mirror the
  169 Rune gate for `sub`/`mul`/`div`: identity rejects; `.cast(type_i64())` first
  binds naked (assert schema + values).

Keep all 169/168/167 tests green (the `+`/`SUM`/`AVG` rejects now carry the
generalized message).

## Phase 5 - Architecture guards

In `crates/toron/tests/architecture/nominal_type_identity.rs`, add the eight
`adr170_*` guards listed in the ADR (operator-set gate + positional ordering;
identity rejects all ops; null parity for identities and quantities;
refinement-in-scope; quantity-unchanged; comparison/selection non-leak;
frontend-shared; no-runtime-surface), and retarget the affected 169 guards/tests
touched by D2. Follow 169's `source_block` + positional-index style.

## Verification

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test strict_identity_arithmetic_rejects_all_operators
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test explicit_cast_erases_identity_for_all_arithmetic
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test quantity_subtraction_and_scalar_scaling_unchanged
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test logical_null_arithmetic_rejects_for_all_operators
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test refinement_identity_arithmetic_rejects
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test naked_numeric_arithmetic_unchanged
scripts/cargo-cgroup test -p toron --test logical_type_ddl_test
scripts/cargo-cgroup test -p toron --test rune_relation_handles_test rune_identity_arithmetic_rejects_and_cast_erases_all_operators
scripts/cargo-cgroup test -p toron --test architecture_test adr170
scripts/cargo-cgroup test -p toron --test architecture_test adr169
scripts/cargo-cgroup test -p toron --test architecture_test
git diff --check
```

## Risks

- **Quantity regressions.** `Mile - Mile`, `Mile * 2`, `Mile / 2` must stay
  unchanged — `bind_logical_arithmetic` returns `Ok(None)` for quantities on
  `Sub`/`Mul`/`Div` (the `if !matches!(op, Add) { return Ok(None); }` after the
  identity check) so `bind_dimensioned_arithmetic` still runs.
- **Diagnostic-rename churn.** D2 touches 169's asserted strings and helper
  names; reconcile the affected 169 tests/guards in the same cut,
  behavior-neutral.
- **NULL parity.** Do not let untyped NULL bypass logical safety. Reject
  identity and quantity operands on the shortcut; use explicit typed NULL where
  ADR 167 already allows a naked scalar.
- **Over-reach into comparison.** Identities stay comparable/selectable; keep the
  gate out of comparison/`MIN`/`MAX`/`COUNT`.
- **New behavior, no existing fallout.** Audit found zero existing identity
  `-`/`*`/`/`; if new fixtures introduce them, CAST-wrap.

## Deferred

Affine units and temperature; Quantity point/delta subtraction; derived dimensions
and ratios; unit display formatting; converted predicate pushdown; Rune optimizer
cockpit and receipt API; `DISTINCT`/window aggregate identity policy; statistical
aggregate unit propagation; richer Rune erasure syntax.
