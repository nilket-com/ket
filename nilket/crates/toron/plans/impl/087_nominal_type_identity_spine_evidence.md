# Impl 087 Evidence: Nominal Type Identity Spine

**Implementation status:** complete. ADR 087's nominal identity spine is implemented as
a Toron-internal sidecar: Arrow `DataType` remains the physical execution/storage
currency, while `LogicalTypeId` is binder metadata.

## Production Changes

- `src/ir.rs`: added `LogicalTypeId`, pilot `LogicalTypeSpec` registry
  (`CustomerId(Int64)`, `ProductId(Int64)`), and `Expr::LogicalAscription`.
- `src/ir/execution.rs`: added `BoundExprProgram.node_logical_types`,
  `NodeLogicalTypeArityMismatch`, builder getter/setter support, NAKED defaults, and
  validation of the new arity invariant.
- `src/binder.rs`: binds `LogicalAscription` into the sidecar; preserves the sidecar
  through clone/merge paths; adds one shared `nominal_compatible` gate used by
  comparison, `IN`, and equi-join binding; extends join-key resolution with logical
  identity while execution continues to store key ordinals only.
- `src/error.rs` and `src/audit.rs`: added dedicated `NominalTypeMismatch` reporting
  and audit classification.
- `src/rune_engine.rs`: added `.as_logical_type::<CustomerId>()` and
  `.as_logical_type::<ProductId>()` via `rune::Params` + marker `TypeHash` dispatch.
- `src/sql_planner.rs` and `src/exec.rs`: treat `LogicalAscription` as a transparent
  wrapper in the remaining unbound-expression utility paths.

## Sidecar Ripple

The sidecar is carried in `BoundExprProgram` rather than Arrow fields or runtime nodes.
New bound nodes default to `LogicalTypeId::NAKED`; binding an ascription updates the
existing bound root's sidecar; cast/try-cast outputs remain naked by construction.

Updated or verified ripple sites:

- `BoundExprProgramBuilder::append_node`, `build`, `empty`, `validate`
- `BoundExprProgram::node_logical_type`
- `clone_bound_subgraph`
- `merge_aggregate_args`
- direct `BoundExprProgram { ... }` test construction

`project_program_roots` and `rewrite_program_ordinals` carry whole
`BoundExprProgram` values, so the added field travels with the existing move/clone
shape.

## Rune Surface

The compiled registration shape is:

```rust
m.associated_function(
    rune::Params::new("as_logical_type", [CustomerId::HASH]),
    RuneExpr::as_customer_id,
)?;
```

and likewise for `ProductId`. There is no stringly `.as_logical_type("CustomerId")`,
no parser hook, and no source normalizer.

Implementation note: the authored surface remains the ADR's turbofish form, but the
marker carriers are uninhabited native enums instead of unit structs. Rune 0.14 rejected
unit struct markers in this generic method position with `UnsupportedGenerics`; enum
paths lower as type markers and preserve the intended static-dispatch contract.

## Behavior Fixtures

The ADR's nominal behavior gate is covered by:

- `rune_nominal_turbofish_binds_vs_literal`
- `rune_nominal_same_identity_binds`
- `rune_nominal_different_identity_rejects`
- `rune_nominal_one_side_oops_rejects`
- `rune_nominal_ascription_physical_mismatch_rejects`
- `rune_nominal_both_side_cast_binds`
- `rune_nominal_one_side_cast_still_rejects`
- `rune_nominal_in_list_same_identity_binds`
- `rune_nominal_in_list_different_identity_rejects`
- `rune_nominal_ascribed_literal_identity_is_preserved`
- `pure_join_gate_rejects_nominal_mismatch`
- `pure_join_gate_accepts_nominal_match`
- existing all-naked SQL/Rune suites, including group-by, predicate widening, Rune
  relation handles, and sandbox tests

## Architecture Guards

Added `tests/architecture/nominal_type_identity.rs` with 10 `adr087_*` guards covering:

- sidecar-not-Arrow-replacement
- `BoundExprProgram` logical arity
- no `BoundExprNode::LogicalAscription`
- sidecar preservation through clone and merge
- comparison gate after physical coercion
- `IN` gate reuse of the shared helper
- join-key gate extension
- dedicated `NominalTypeMismatch`
- Rune `Params` marker registration with no string API
- cast outputs naked identity

## Boundary Confirmation

No Arrow `DataType` replacement was introduced. No Wendelon storage/wire schema,
executor kernel, or PBC opcode change was introduced. The distributed
coordinator-worker plan payload does gain the Toron-internal `node_logical_types`
field, so ADR 087 assumes same-version Toron deploys; mixed-version serde fallback is
not part of this cut. A legacy payload would fail deserialization or, if defaulted in
the future, trip the arity invariant.

## Verification

```
cargo test -p toron --test rune_relation_handles_test rune_nominal_
cargo test -p toron --test join_kind_test pure_join_gate_
cargo test -p toron --test architecture_test adr087_
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test join_kind_test
cargo test -p toron --test group_by_test
cargo test -p toron --test predicate_widening_test
cargo test -p toron --test rune_sandbox_test
cargo test -p toron
cargo clippy -p toron -- -D warnings
git diff --check
```

All commands above passed.
