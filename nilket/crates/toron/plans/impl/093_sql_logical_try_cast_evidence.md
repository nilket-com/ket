# Evidence 093: SQL Logical Try Cast

ADR 093 is implemented as a Toron-only binder change. `TRY_CAST(expr AS LogicalType)` and
`SAFE_CAST(expr AS LogicalType)` now resolve durable logical type names through the same pre-bind
registry path as ADR 092 `CAST`, reconcile any existing logical identity before physical
conversion, and stamp the resulting `TryCast` node with the logical type id.

## Implementation Notes

- `binder.rs`: `collect_logical_cast_names` now collects `Expr::SqlLogicalCast` names independent
  of `try_cast`, so unknown logical targets fail through the durable-only logical type lookup.
- `binder.rs`: `bind_sql_logical_cast` no longer rejects `try_cast` up front. The strict `CAST`
  branch keeps the ADR 092 behavior, while the `TRY_CAST` branch appends the existing
  `BoundExprNode::TryCast` for same-physical casts and supported numeric conversions.
- Identity reconciliation remains before physical conversion. Conflicting logical identities still
  reject at bind time; explicit primitive erase can relabel through the existing naked-identity
  cast rule.
- No planner, IR, executor, PBC, Rune, or Wendelon changes were required. Same-physical Utf8 and
  Boolean logical `TRY_CAST` use the existing executor passthrough for equal source/target types.

## Tests And Guards

- Replaced the ADR 092 logical `TRY_CAST` rejection fixture with ADR 093 behavior coverage for
  widening, narrowing-to-null, same-physical numeric, same-physical Utf8, same-physical Boolean,
  conflicting identities, primitive erase/relabel, undefined targets, unsupported cross-physical
  casts, and `SAFE_CAST` aliasing.
- Added CTAS coverage proving logical `TRY_CAST` materializes a nullable field with durable logical
  metadata and survives descriptor resync.
- Updated the ADR 092 architecture guard that previously pinned rejection and added ADR 093 guards
  for name collection, identity-before-try-cast ordering, categorical nullable `TryCast` emission,
  reuse of the existing execution node/no PBC opcode, physical `bind_try_cast` remaining
  identity-free, and CTAS nullability propagation through the ADR 089 metadata writer.

## Verification

Commands run:

```text
cargo fmt -p toron
cargo test -p toron --test logical_type_ddl_test
cargo test -p toron --test architecture_test nominal_type_identity
cargo test -p toron
cargo clippy -p toron --all-targets -- -D warnings
```

All completed successfully. Full `cargo test -p toron` completed with one existing ignored Wendelon
fixture.
