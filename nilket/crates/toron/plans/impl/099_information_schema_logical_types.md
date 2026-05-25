# Impl Plan — ADR 099: `information_schema.logical_types`

**Migration for** `plans/adr/099_information_schema_logical_types.md`. **Toron-only.** Add the first
read-side synthetic relation: `ExecutionPlan::SystemTable` (095 `EmptyRelation` pattern) + a bind-time
interception of `ScanNamed(information_schema.logical_types)` + bind-time materialization from the
refreshed durable registry. No Wendelon contract/PBC/wire/storage change.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| FROM lowering | `lower_table_factor` `sql_planner.rs:3310` → `ScanNamed { table }` via `lower_table_ref_object_name(name, "SELECT")` |
| Interception point | binder `LogicalPlan::ScanNamed` arm `binder.rs:92-93` (`self.catalog.resolve_table(table)`) |
| Data source | `Catalog::logical_type_registry()`; `LogicalTypeRegistry { by_id, by_name }` `ir.rs:87`; `LogicalTypeSpec { id, name, physical, refinement }` `ir.rs:33` |
| Row idiom | `materialize_insert_batch` (`coordinator/write.rs:32`) — `Vec<Vec<ScalarValue>>` → `RecordBatch` |
| Node precedent | `ExecutionPlan::EmptyRelation` `ir/execution.rs:179` (coordinator-local leaf, ~15-site walker sweep) |
| Refinement AST | `LogicalTypeRefinementPredicate` (096) — render to `CHECK (…)` text |

## Phase 0 — Confirm two-part FROM is already carried

`lower_table_ref_object_name` already has a `[schema, table]` branch returning
`TableRef::with_schema(...)`; confirm `information_schema.logical_types` arrives at the binder as
`TableRef { schema: Some("information_schema"), table: "logical_types" }`. No SQL-planner change is
expected. If parser drift breaks that path, restore only the reserved `information_schema` source
shape; do not widen or reinterpret general two-part table semantics.

## Phase 1 — Add `SystemTable` variant + walker sweep

`ir/execution.rs`: `SystemTable { kind: SystemTableKind, schema: SchemaRef, rows: Vec<Vec<ScalarValue>> }`,
`enum SystemTableKind { InformationSchemaLogicalTypes }` with the same debug/clone/equality and
`Serialize`/`Deserialize` support required by `ExecutionPlan`. `schema()` arm → `schema.clone()`. Run
`rg "ExecutionPlan::"` and handle the new leaf in every exhaustive walker (EmptyRelation precedent,
~15 sites): `exec.rs` dispatch, `binder.rs` `optimize_for_required`/`terminal_exchange`, `fragment.rs`
(childless coordinator-local leaf), `scheduler.rs` `first_scan_fingerprint`→`None`, `server/mod.rs`
`operator_kind_label`→`"system_table"`. No `_` wildcard.

## Phase 2 — Executor arm

`Executor::execute_bound`: `SystemTable { schema, rows, .. } =>` materialize `Vec<Vec<ScalarValue>>` →
one `RecordBatch` (mirror `materialize_insert_batch`'s row→column build), coordinator-local; **no storage
call / Wendelon RPC / PBC**.

## Phase 3 — Bind-time interception + materialization

In the binder `ScanNamed` arm (`binder.rs:92`), **before** `catalog.resolve_table`:
```rust
if is_reserved_information_schema(table) {                 // ASCII case-insensitive
    return bind_information_schema_relation(table).await;  // unknown sub-name → reject
}
// else: existing catalog.resolve_table path
```
`bind_information_schema_relation`: match `logical_types`; call `catalog.refreshed_logical_type_registry()`
(D6); for each `specs_sorted()` entry build a row `[Int64(id), Utf8(name), Utf8(physical_keyword),
refinement_text_or_null]`; emit `SystemTable { kind: InformationSchemaLogicalTypes, schema: <D4>, rows }`.
Helpers: `physical_keyword(&DataType) -> &str`; `render_refinement(&LogicalTypeRefinementPredicate) ->
String` (AST→`CHECK (…)`, the inverse of 096's parser — `VALUE`, ops, escaped literals, `AND` in stored
order).

## Phase 4 — Catalog refresh hook + registry iteration

`Catalog` trait: `async fn refreshed_logical_type_registry(&self) -> Result<LogicalTypeRegistry,
CatalogError>` — default returns `logical_type_registry()`; Wendelon-backed scoped catalog lists durable
defs for the tenant, registers, returns the refreshed snapshot; **list failure → `Err`** (query fails,
no stale partial). Add `LogicalTypeRegistry::specs_sorted(&self) -> Vec<&LogicalTypeSpec>` (sorted by id;
no private-map exposure).

## Phase 5 — Read-only namespace (D7)

Reject mutation/DDL **targeting** `information_schema.*` at the planner/binder (CREATE TABLE / CTAS
target / DROP TABLE / INSERT / UPDATE / DELETE). `information_schema.logical_types` as a SELECT *source*
(incl. CTAS-from) is allowed. Clear "information_schema is read-only" error.

## Phase 6 — Behavior fixtures

`information_schema_logical_types_lists_current_tenant_durable_definitions`;
`refinement_column_renders_check_text_or_null`; `physical_column_renders_sql_carrier_keyword`;
`information_schema_logical_types_is_tenant_scoped`; `information_schema_logical_types_excludes_seed_and_naked`;
`information_schema_logical_types_refreshes_before_read` (fake Wendelon-backed def not yet locally
resolved appears post-refresh); `unqualified_logical_types_is_normal_table_lookup`;
`unknown_information_schema_relation_rejects`; `information_schema_namespace_is_read_only` (+ CTAS-source
valid); `select_with_where_projection_orderby_over_information_schema_composes`;
`system_table_executes_coordinator_local_without_storage_scan_or_pbc`.

## Phase 7 — Architecture guards

`adr099_information_schema_logical_types_intercepts_before_catalog_resolve_table`;
`adr099_system_table_is_coordinator_local_scalar_rows`;
`adr099_logical_types_rows_from_refreshed_durable_registry_not_seed`;
`adr099_information_schema_namespace_is_reserved_read_only`;
`adr099_system_table_walkers_handle_new_variant` (no `_` wildcard);
`adr099_no_wendelon_pbc_wire_or_storage_surface`; `adr099_scoped_to_logical_types_only`.

## Phase 8 — Verification

```
cargo test -p toron --test logical_type_ddl_test     # introspection rows + refresh + tenant scope + read-only
cargo test -p toron --test architecture_test         # adr099_* + walker sweep
cargo test -p toron                                  # full suite; 095 EmptyRelation + no regression
```

**Acceptance:** `SELECT … FROM information_schema.logical_types` lists refreshed durable definitions
(id/name/keyword/`CHECK`-text-or-null), seed excluded, tenant-scoped; unqualified `logical_types` is a
normal table; unknown `information_schema.*` + mutation targets reject (CTAS source valid);
`WHERE`/projection/`ORDER BY` compose; coordinator-local execution (no scan/RPC/PBC); the `SystemTable`
variant handled across the ~15 walkers; full suite green.

## Risks / notes

- **The walker sweep is the bulk** (new `ExecutionPlan` variant — the 095 precedent); pin with the
  variant-handling guard, no `_` wildcard.
- **Two-part FROM lowering** (Phase 0) — expected to need no planner change; if it regresses, restore
  only the reserved source shape and don't widen general two-part table support.
- **Refresh-before-read** is load-bearing (synced view); list-failure must fail the query, not show a
  stale partial (mirrors 091/092).
- **Bind-time, not executor-time** — catalog refresh + materialization at bind (the binder owns catalog
  access); the executor only emits the baked rows.
- **Narrow:** one `SystemTableKind`; no general virtual-table framework / reserved slots; durable-only
  (no seed); read-only namespace.
