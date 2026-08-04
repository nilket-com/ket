# Evidence 091: SQL Logical Type Use-Sites — DDL Column Position

ADR 091 is implemented as a Toron-only change. `CREATE TABLE ... (customer_id CustomerId)` now
records `CustomerId` as an unresolved logical type name in the logical plan, resolves it at bind
through the durable registry, writes the Arrow field through the ADR 089 logical metadata writer,
and leaves `ExecutionPlan::CreateTable` as a resolved `SchemaRef`.

## Implementation Notes

- `ir.rs`: added `CreateTableSchema`, `CreateTableColumn`, and
  `CreateTableColumnType::{Physical, LogicalName}`. Only `LogicalPlan::CreateTable` carries this
  unresolved schema; `ExecutionPlan::CreateTable` remains unchanged. Added
  `LogicalTypeResolver::durable_only` so DDL name lookup can bypass the static seed.
- `sql_planner.rs`: `CREATE TABLE` primitive column types lower to `Physical(DataType)`;
  `SqlDataType::Custom` lowers to `LogicalName` only for one-part, unquoted ASCII identifiers with
  no modifiers and not `naked` case-insensitively. The planner does no catalog lookup.
- `binder.rs`: `bind_create_table` resolves `LogicalName` with
  `Catalog::resolve_durable_logical_type_by_name(...).await`, uses
  `field_with_logical_type_metadata`, and emits a resolved `ExecutionPlan::CreateTable`.
- `catalog.rs`: added durable-only registry lookup to the `Catalog` trait. `MockCatalog` reads its
  tenant registry without the seed. `WendelonCatalog` checks local registry, refreshes with
  `list_logical_type_definitions` on miss, then returns `CatalogError::LogicalTypeNotFound` if the
  name still is not durable. Both create paths now rebind their freshly written descriptor through
  `from_descriptor_with_logical_types(..., LogicalTypeResolver::with_registry(...))`.
- `ir.rs` decoder split: descriptor ingestion still uses
  `decode_column_logical_type_with_resolver` and remains fail-closed for undefined durable ids.
  The existing expression binder's convenience decoder now allows already-validated durable ids to
  flow through the nominal gate by id; otherwise SQL-created durable tables would bind at create
  time but fail on the first read.
- `error.rs`/`audit.rs`: added the typed `CatalogError::LogicalTypeNotFound` and audit class.

## Tests And Guards

- New behavior suite: `tests/logical_type_ddl_test.rs`
  - define then use creates `Int64` field with durable metadata.
  - static seed does not satisfy SQL DDL name lookup.
  - two durable logical ids reject bare comparison.
  - naked literal comparison still binds.
  - primitive `BIGINT` remains naked.
  - planner records `SqlDataType::Custom` names without catalog lookup.
  - `naked`, quoted, multi-part, and modifier custom type names reject.
- Updated 090 fixture: `CAST(x AS CustomerId)` still rejects; SQL column use-site no longer does.
- Added catalog unit coverage for Wendelon refresh-before-unknown name lookup.
- Added ADR 091 architecture guards for unresolved logical plan shape, resolved execution shape,
  durable-only DDL lookup, Wendelon list refresh, 089 writer reuse, resolver-aware create rebind,
  undefined-name pre-submit failure, and no CAST/typed-CTAS authoring.

## Verification

Commands run:

```text
cargo check -p toron --tests
cargo test -p toron --test logical_type_ddl_test
cargo test -p toron --test create_table_test
cargo test -p toron --test logical_type_definition_test
cargo test -p toron --lib durable_logical_type_name_lookup_refreshes_list_before_not_found
cargo test -p toron --test architecture_test
cargo test -p toron --test catalog_logical_identity_test
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test group_by_test
cargo fmt
cargo test -p toron --lib
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron
```

All completed successfully. Full `cargo test -p toron` completed with one existing ignored
Wendelon fixture.
