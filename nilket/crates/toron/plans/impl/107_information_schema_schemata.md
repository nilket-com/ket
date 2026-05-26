# Impl Plan — ADR 107: `information_schema.schemata`

**Migration for** `plans/adr/107_information_schema_schemata.md`. **Scope:** Toron `ir/execution.rs` (1 enum
variant) + `binder.rs` (constants + dispatch arm + schema/row fns) + behavior tests
(`logical_type_ddl_test.rs`) + guard reconciliation (`tests/architecture/nominal_type_identity.rs`).
**Non-scope:** no new `ExecutionPlan` variant, no walker sweep, no Wendelon RPC/wire, no
storage/PBC/executor/worker surface, no `CREATE SCHEMA`, no schema registry. The 4th finite system relation
over the existing 102 substrate.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| `SystemTableKind` | `ir/execution.rs` — 3 variants; no `SystemTableKind::` match outside `binder.rs` (grep clean) → additive |
| Dispatch switch | `bind_information_schema_relation` (`binder.rs:683`); constants `binder.rs:47-51`; unknown-relation error `:732` |
| Authoritative refresh (reuse) | `self.catalog.refreshed_table_schema_descriptors().await?` (102) |
| Schema/row pattern (mirror) | `information_schema_tables_schema` (`:3345`), `information_schema_table_rows` (`:3403`, emits `descriptor.namespace`), `system_table_plan` (`:3365`) |
| Read-only sentinel (retarget) | `information_schema_namespace_is_read_only_and_scoped` (`logical_type_ddl_test.rs:2522`); sentinel `schemata` at `:2542` |
| Storage-scan behavior test (retarget) | `system_table_executes_without_storage_scan` (`logical_type_ddl_test.rs:2584`) |
| 102 guards (retarget) | `nominal_type_identity.rs` — finite-family (`:1050`, asserts variants `:1056`), dispatch-explicit (`:1063`), coordinator-local (`:1122`, enumerates variants `:1128`/`:1135`) |

## Phase 1 — IR kind (`ir/execution.rs`)

```rust
pub enum SystemTableKind {
    InformationSchemaLogicalTypes,
    InformationSchemaTables,
    InformationSchemaColumns,
    InformationSchemaSchemata,   // ADR 107
}
```

Additive (derives carry); `SystemTable` execution is kind-agnostic → no exhaustive-match/walker sweep.

## Phase 2 — Binder constants + dispatch (`binder.rs`)

Constants (after `:51`):

```rust
const INFORMATION_SCHEMA_SCHEMATA: &str = "information_schema.schemata";
const INFORMATION_SCHEMA_SCHEMATA_TABLE: &str = "schemata";
```

Arm in `bind_information_schema_relation`, after the columns arm (mirrors `tables`):

```rust
if table
    .table
    .eq_ignore_ascii_case(INFORMATION_SCHEMA_SCHEMATA_TABLE)
{
    let descriptors = self.catalog.refreshed_table_schema_descriptors().await?;
    return system_table_plan(
        SystemTableKind::InformationSchemaSchemata,
        information_schema_schemata_schema(),
        information_schema_schemata_rows(&descriptors),
        projection,
    );
}
```

Add `{INFORMATION_SCHEMA_SCHEMATA}` to the unknown-relation error's supported-relations list (`:732`). No
registry map.

## Phase 3 — Schema + row builder (`binder.rs`, near the other `information_schema_*` fns)

```rust
fn information_schema_schemata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("schema_name", DataType::Utf8, false)]))
}

fn information_schema_schemata_rows(descriptors: &[TableSchemaDescriptor]) -> Vec<Vec<ScalarValue>> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.namespace.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|schema_name| vec![ScalarValue::Utf8(Some(schema_name))])
        .collect()
}
```

`BTreeSet` (already imported in `binder.rs`) → distinct + deterministic sorted; empty → zero rows. Do **not**
call `descriptor.arrow_schema()` / consult `LogicalTypeRegistry`, and do **not** add
`catalog_name`/`schema_owner`/tenant id. That is the entire production change.

## Phase 4 — Behavior tests + sentinel retargets (`tests/logical_type_ddl_test.rs`)

Mirror the existing 102 system-relation tests (the `RefreshingTableDescriptorCatalog` /
`latest_descriptors_for_tenant` / `catalog.scoped(TenantId(..))` helpers). Add:

- `information_schema_schemata_lists_descriptor_namespaces` — tables in `analytics.*` + `default.*`;
  `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name` → rows `analytics`, `default`.
- `information_schema_schemata_is_tenant_scoped` — bind via `.scoped(TenantId(1))` vs `(2)`; each sees only
  its namespace.
- `information_schema_schemata_is_latest_only` — old + newer generations for one table → namespace appears
  once (mirror `information_schema_tables_columns_are_latest_only`).
- `information_schema_schemata_empty_catalog_has_no_rows` — empty descriptor list → zero rows.
- `information_schema_schemata_listing_failure_fails_query_without_stale_fallback` — refresh-fail fixture →
  binding error carries the refresh message (D3).
- `information_schema_schemata_composes_with_query_ops` — `WHERE schema_name <> 'default' ORDER BY
  schema_name`; `SELECT COUNT(*) FROM information_schema.schemata`.

Retarget:
- `information_schema_namespace_is_read_only_and_scoped` — include `"schemata"` in the unqualified-normal-lookup
  loop; switch the unknown sentinel `information_schema.schemata` (`:2542`) → `information_schema.views`; add
  DDL/DML target rejects for `information_schema.schemata`.
- `system_table_executes_without_storage_scan` (`:2584`) — also query `SELECT schema_name FROM
  information_schema.schemata` against the failing-storage fixture.

## Phase 5 — Architecture guards (`tests/architecture/nominal_type_identity.rs`)

1. Retarget `adr102_system_table_kind_has_finite_introspection_family` — count `4`; assert
   `InformationSchemaSchemata`; keep no `VirtualTable`/`SystemTableRegistry`.
2. Retarget `adr102_information_schema_dispatch_is_explicit` — assert the `INFORMATION_SCHEMA_SCHEMATA_TABLE`
   constant + `eq_ignore_ascii_case(INFORMATION_SCHEMA_SCHEMATA_TABLE)`; keep unknown-relation rejection, no
   `HashMap`/registry.
3. Retarget `adr102_system_table_execution_stays_coordinator_local` — assert `execution` contains
   `InformationSchemaSchemata`; assert `exec`/`storage`/`pbc` do **not**; keep the existing
   `SystemTable { schema, rows, .. }` / `materialize_scalar_rows` assertions.
4. Add `adr107_schemata_is_descriptor_derived_not_a_registry`:

```rust
let binder = binder_source();
let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");
let schema = source_block(&binder, "fn information_schema_schemata_schema");
let rows = source_block(&binder, "fn information_schema_schemata_rows");

assert!(bind_relation.contains("INFORMATION_SCHEMA_SCHEMATA_TABLE"));
assert!(rows.contains("BTreeSet"));
assert!(rows.contains("descriptor.namespace"));
assert!(!rows.contains(".arrow_schema()"));
assert!(!rows.contains("logical_type_display_name"));
assert!(!rows.contains("LogicalTypeResolver"));
assert!(!schema.contains("catalog_name") && !schema.contains("schema_owner"));
assert!(!binder.contains("CREATE SCHEMA") && !binder.contains("SystemTableRegistry"));
```

`adr102_table_descriptor_refresh_is_authoritative` stays green (no retarget).

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron --test logical_type_ddl_test information_schema_schemata
cargo test -p toron --test logical_type_ddl_test information_schema_namespace_is_read_only_and_scoped
cargo test -p toron --test logical_type_ddl_test system_table_executes_without_storage_scan
cargo test -p toron --test architecture_test adr107
cargo test -p toron --test architecture_test adr102
cargo test -p toron --test architecture_test adr099
cargo test -p toron                                  # full suite green, no red guard
git diff --check
```

## Acceptance

`information_schema.schemata` returns the current tenant's sorted distinct latest-descriptor namespaces;
empty → zero rows; refresh failure fails binding; query operators compose; unqualified `schemata` stays a
normal table; `information_schema.views` rejects; `information_schema.schemata` mutation targets reject;
`SystemTableKind` is a finite 4-variant family reached by explicit dispatch; no `ExecutionPlan`-variant /
walker / Wendelon / storage / PBC / executor surface; the three ADR 102 guards + two behavior sentinels are
reconciled in-change; Toron suite green.
