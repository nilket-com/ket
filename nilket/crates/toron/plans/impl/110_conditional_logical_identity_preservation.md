# Impl Plan — ADR 110: Conditional Logical-Identity Preservation

**Migration for** `plans/adr/110_conditional_logical_identity_preservation.md`. **Scope:** `binder.rs`
(`bind_case` + one helper) + behavior tests + guard reconciliation. **Non-scope:** no grammar,
`Expr`/`BoundExprNode` variant, executor, PBC, Rune API, explicit-`NULL`, Wendelon. Binder-local; all six
109 sugars inherit by construction.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| The rule | `bind_case` (`binder.rs`), after `output_type = case_common_type(...)`, **before** the `type_case_result` cast loop (`result_ids` must be pre-cast) |
| Policy | `nominal_compatible` (`:5157`); **not** `reconcile_identity` (too permissive). Precedent: `bind_comparison` (`:5064`) rejects non-literal physical mismatch |
| Accessors | `node_type` (`:508`), `node_logical_type` (`:512`), `literal` (`:524`), `set_node_logical_type` (`:516`) |
| Propagation (free) | `schema_for_exprs_with_output_names` reads `root_logical_type` (`:5385`) → 089 metadata |
| Neutral | `is_case_null_placeholder` (109) |
| Guard to retarget | `case_result_is_naked` (`sql_case_test.rs:210`) |

## Phase 1 — The identity helper (`binder.rs`)

```rust
/// CASE output logical identity. A non-placeholder branch contributes its identity only if it does not need
/// a physical widening cast (a logical type has one physical; widening would erase it). Implicit widening of
/// a *non-naked* branch rejects — the erasure escape hatch is an explicit CAST (ADR 087; consistent with
/// `bind_comparison` rejecting non-literal physical mismatch). Naked literals/placeholders are neutral;
/// mixed non-naked or non-naked-plus-naked-non-literal reject; all-naked → NAKED.
fn case_output_logical_type(
    builder: &BoundExprProgramBuilder,
    result_ids: &[BoundExprId],
    output_type: &DataType,
) -> Result<LogicalTypeId, BindingError> {
    let mut acc_logical = LogicalTypeId::NAKED;
    let mut acc_is_literal = true; // neutral start (naked-literal left is OK per nominal_compatible)
    for &id in result_ids {
        if is_case_null_placeholder(builder, id) {
            continue; // 109 untyped-NULL placeholder: identity-neutral
        }
        let branch_type = builder
            .node_type(id)
            .cloned()
            .expect("builder returns typed CASE result");
        let branch_logical = builder.node_logical_type(id).unwrap_or(LogicalTypeId::NAKED);
        // D5: reject implicit physical widening of an identity-bearing branch.
        if !branch_logical.is_naked() && branch_type != *output_type {
            return Err(BindingError::TypeMismatch {
                message: format!(
                    "CASE logical result {} has physical {branch_type:?}, but common CASE output is {output_type:?}; cast explicitly to erase identity before widening",
                    logical_type_name(branch_logical)
                ),
            });
        }
        let is_literal = builder.literal(id).is_some();
        // non-naked branches here are all at output_type → equal physicals (nominal_compatible's assert holds)
        nominal_compatible(output_type, output_type, acc_logical, branch_logical, acc_is_literal, is_literal)?;
        if !branch_logical.is_naked() {
            acc_logical = branch_logical;
            acc_is_literal = false;
        } else if !is_literal {
            acc_is_literal = false; // a real naked value/column: no longer neutral
        }
    }
    Ok(acc_logical)
}
```

Order-independent: a naked-non-literal seen before or after an identity branch sets `acc_is_literal = false`,
so the identity branch then rejects via `nominal_compatible`. D5 is a physical `TypeMismatch` because the CASE
output type no longer matches the logical type's physical representation; pin only a stable substring of the
explicit-cast diagnostic in behavior tests.

## Phase 2 — Hook `bind_case` (`binder.rs`)

After `let output_type = case_common_type(&result_types)?;`, **before** the `type_case_result` loop:

```rust
let output_logical = case_output_logical_type(builder, &result_ids, &output_type)?;

// ... unchanged: cast each branch + else via type_case_result ...

let case_id = builder.append_case(bound_when_then, else_input, output_type);
if !output_logical.is_naked() {
    builder.set_node_logical_type(case_id, output_logical);
}
Ok(case_id)
```

Entire production change: one helper + ~4 lines.

## Phase 3 — Behavior tests

Add logical-identity behavior tests in `tests/logical_type_ddl_test.rs`, where durable logical-type catalog
setup, projection-schema assertions, and refinement-elision helpers already live.

Per ADR §Behavior gates:
- `case_uniform_logical_identity_propagates_to_projection_schema` — `CASE WHEN c THEN pid ELSE pid END` →
  output field carries `PersonId` (assert via the bound/projection schema metadata).
- `case_literal_fallback_can_infer_logical_identity` — `CASE WHEN c THEN pid ELSE 7 END` → `PersonId`.
- `case_missing_else_preserves_uniform_logical_identity`, `coalesce_..._through_null_fallback`,
  `if_..._through_null_branch` — placeholder/missing-ELSE neutral.
- `case_mixed_logical_identities_reject` (`THEN customer_id ELSE product_id`),
  `case_logical_identity_plus_naked_nonliteral_rejects` (`THEN pid ELSE naked_int_col`, same physical),
  `case_identity_branch_rejects_physical_widening` (`THEN pid(Int64) ELSE 1.5` → reject, D5).
- `case_produced_identity_is_nominally_checked_downstream` (`(CASE … THEN customer_id …) = product_id` →
  reject), `case_computed_identity_does_not_refinement_elide`, `case_all_naked_result_stays_naked`,
  `case_numeric_widening_all_naked_still_works`.

Retarget the existing all-naked CASE fixture in `tests/sql_case_test.rs`:

- Rename `case_result_is_naked` to `case_all_naked_result_stays_naked`.
- Add or adapt `case_numeric_widening_all_naked_still_works` if no existing all-naked numeric widening fixture
  asserts `LogicalTypeId::NAKED`.

## Phase 4 — Architecture guards (`tests/architecture/nominal_type_identity.rs`)

- **Retarget** `case_result_is_naked` (`sql_case_test.rs:210`) → all-naked CASE stays naked (drop the
  unconditional-naked assertion).
- Add `adr110_case_preserves_compatible_logical_identity`: `bind_case`/`case_output_logical_type` reads
  `node_logical_type`, calls `nominal_compatible`, treats `is_case_null_placeholder` neutral, rejects a
  non-naked branch whose physical ≠ output type, and `bind_case` calls `set_node_logical_type`; **not**
  `reconcile_identity` for branch folding.
- Keep 108 (`adr108_case_is_residual_bound_expression`) + 109 guards green.

## Phase 5 — Verification

```sh
cargo fmt
cargo test -p toron case_
cargo test -p toron --test architecture_test adr110 && cargo test -p toron --test architecture_test adr108 && cargo test -p toron --test architecture_test adr109
cargo test -p toron --test architecture_test adr087   # nominal spine regression
cargo test -p toron
git diff --check
```

## Acceptance

A CASE / 109 sugar whose non-placeholder branches share one identity (nakeds neutral as literals) carries it
to the projection schema + downstream nominal checks; mixed identities, identity-plus-naked-non-literal, and
implicit physical widening of an identity branch all reject (explicit CAST erases); all-naked stays naked;
identity flow, not elision; binder-local, **no new IR/executor/PBC/Rune/Wendelon surface**;
`case_result_is_naked` retargeted + `adr110_*` added; 108/109 guards green; Toron suite green.
