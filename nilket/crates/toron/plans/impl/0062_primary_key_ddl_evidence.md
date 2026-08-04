# Impl Evidence 062: Primary-Key DDL

**Date:** 2026-05-21
**Base:** `3038f1c plan: adr 062 primary key ddl`

## Summary

ADR 062 is implemented in the Toron SQL/coordinator/catalog path. Schema-only
`CREATE TABLE` now accepts a single-column explicit-`NOT NULL` primary key,
threads a Toron-local `CreateTableRowIdentity` through logical and execution
IR, and maps it to Wendelon's existing `RowIdentityPolicy::PrimaryKey` only at
the catalog boundary. Tables without a primary key continue to create
`AppendOnly` descriptors.

Planner validation owns the DDL policy: exactly one primary-key column,
column-level and table-level syntax, explicit `NOT NULL`, and the narrow
`Int32`/`Int64`/`Utf8` type intersection. CTAS remains append-only and has no
row-identity surface.

`IF NOT EXISTS` compatibility now compares both schema and row identity. Same
schema plus different identity returns `SchemaMismatch`; unsupported existing
identity fails closed. The live Wendelon fixture proves a SQL-created primary
key table drives Wendelon primary-key supersession rather than creating an
inert descriptor.

Prior guards were retargeted for ADR 038 and the CREATE-TABLE-PK rejection
surface:

- ADR 038 no longer asserts every Wendelon create is append-only; it asserts
  create identity is request-driven after ADR 062.
- CREATE-TABLE tests now cover the accepted PK syntax plus the narrowed reject
  matrix: nullable/missing-`NOT NULL`, duplicate/composite PK, unsupported
  types, UNIQUE, named/options constraints, and CTAS PK.

## Verification

- `cargo check -p toron --tests` - passed.
- `cargo clippy -p toron --all-targets -- -D warnings` - passed.
- `cargo test -p toron --test architecture_test adr062 -- --test-threads=1`
  - passed: 23 passed.
- `cargo test -p toron --test architecture_test -- --test-threads=1` -
  passed: 666 passed.
- `cargo test -p toron --test create_table_test -- --test-threads=1` -
  passed: 32 passed.
- `cargo test -p toron --test ctas_test -- --test-threads=1` - passed:
  28 passed.
- `cargo test -p toron --test sql_listener_test toron_sql_complete_frame_carries_stable_row_count -- --test-threads=1`
  - passed.
- `cargo test -p toron --test drop_table_test wendelon_create_row_identity_policy_is_request_driven_after_adr062 -- --test-threads=1`
  - passed.
- `cargo test -p toron --test wendelon_storage_execution_test -- --test-threads=1`
  - passed: 10 passed, 1 ignored.
- `cargo test -p toron -- --test-threads=6` - passed; one pre-existing
  Wendelon admin fixture remains ignored.
- `git diff --check` - passed.

## Notes

- No Wendelon server, wire, worker protocol, or storage primitive change was
  added.
- No `UPDATE`, `RETURNING`, multi-column primary key, uniqueness surface, or
  DDL type widening was added.
- The live Wendelon admin schema-update fixture remains ignored as before; ADR
  062's live proof uses a SQL-created binding reconstructed as a matching
  Wendelon descriptor and validates primary-key supersession through
  `StorageWrite`/`Storage`.
