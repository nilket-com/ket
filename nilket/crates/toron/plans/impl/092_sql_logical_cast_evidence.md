# Evidence 092: SQL Logical Cast

ADR 092 is implemented as a Toron-only change. `CAST(expr AS LogicalType)` now lowers to a
transient logical-plan node, resolves the logical type name through the durable registry at bind
time, reconciles any existing logical identity, optionally reuses the existing numeric physical
cast kernel, and stamps the resulting bound expression with the durable logical type id.

## Implementation Notes

- `ir.rs`: added transient `Expr::SqlLogicalCast { expr, logical_name, try_cast }`. No execution
  IR or PBC opcode was added, and `LogicalAscription` remains unchanged.
- `sql_planner.rs`: split CAST target lowering into `LoweredCastTarget::{Physical, LogicalName}`.
  Primitive CAST targets keep the existing path. `SqlDataType::Custom` is accepted only as a
  one-part, unquoted ASCII logical type name with no modifiers and not `naked`; planner lowering
  does no catalog or seed lookup. `TRY_CAST`/`SAFE_CAST` to a logical target are represented only
  so the binder can reject them with a targeted diagnostic.
- `binder.rs`: expression binding now performs an async prepass for SQL logical CAST names and
  resolves them with `Catalog::resolve_durable_logical_type_by_name`, excluding the static seed.
  The sync expression binder receives resolved `LogicalTypeSpec` values, reconciles identity before
  appending any physical cast, uses same-physical ascription without a cast node, and only uses the
  existing numeric explicit-cast lattice for cross-physical numeric conversions.
- `exec.rs`: unresolved `Expr::SqlLogicalCast` is a defensive internal error if it ever reaches
  unbound execution.

## Tests And Guards

- Expanded `tests/logical_type_ddl_test.rs` with ADR 092 fixtures for planner lowering, invalid
  logical target names, seed exclusion, defined-type identity stamping, same-identity restate,
  conflict rejection, explicit primitive erase/relabel, `Int32 -> CustomerId(Int64)` numeric
  coercion, same-physical `Utf8` ascription without a cast kernel, unsupported `Utf8 -> Int64`
  rejection, logical `TRY_CAST` rejection, and CTAS materialization of durable logical identity.
- Reconciled the ADR 090/091 "CAST still rejects" fixture to the still-deferred typed CTAS column
  list surface.
- Added ADR 092 architecture guards for transient IR shape, planner no-resolution lowering,
  durable-only binder pre-resolution, identity-before-physical-cast ordering, and no new executor
  kernel/PBC opcode.
- Reconciled stale ADR 025/026, ADR 064, and Rune architecture string guards after the binder
  expression context became async and CAST target lowering gained the logical branch.

## Verification

Commands run:

```text
cargo fmt -p toron
cargo check -p toron --tests
cargo test -p toron --test logical_type_ddl_test
cargo test -p toron --test logical_type_definition_test
cargo test -p toron --test architecture_test
cargo test -p toron --test catalog_logical_identity_test --test rune_relation_handles_test --test ctas_test --test group_by_test
cargo test -p toron --lib
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron --test create_table_test --test insert_test --test delete_test --test update_test
cargo test -p toron
```

All completed successfully. Full `cargo test -p toron` completed with one existing ignored
Wendelon fixture.
