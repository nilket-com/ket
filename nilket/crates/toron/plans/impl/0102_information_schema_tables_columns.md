# Impl Plan — ADR 102: `information_schema.tables` / `.columns` (authoritative)

**Migration for** `plans/adr/0102_information_schema_tables_columns.md`. Two halves: (1) a **new read-only
cross-ring RPC** — list latest tenant table descriptors (Wendelon Control-Ring server + client + wire) —
the only genuinely-new surface; (2) Toron-side **099-pattern reuse** — two `SystemTableKind` kinds + two
`bind_information_schema_relation` arms + a refresh hook, reusing the existing descriptor-decode path.
**No new `ExecutionPlan` variant → no walker sweep.** No storage/executor/PBC change.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| 099 system-relation seam | `bind_information_schema_relation` `binder.rs:675`; `SystemTableKind` `ir/execution.rs:178` |
| Refresh-before-read precedent | `refreshed_logical_type_registry` `catalog.rs:239` (default) / `:1741` (Wendelon-backed) |
| Cross-ring client trait | `DescriptorSyncClient` `catalog.rs:298` (`list_logical_type_definitions` + `get_table_schema_descriptor`; `impl … for Client` delegates to RPC `Client::*`) |
| Wendelon list/fetch RPC precedent | `LOGICAL_TYPE_LIST_*` / `TABLE_DESCRIPTOR_GET_*` MsgType pairs + `Client::list_logical_type_definitions` |
| Latest-only source | Wendelon `latest_by_name` `schema/table.rs:2061` (via `upsert` `:2065`); **not** all-generations `descriptors_for_tenant` |
| Descriptor → schema decode | `descriptor.arrow_schema()` `catalog.rs:104`; `from_descriptor_with_logical_types` `:99` |
| Column logical-type decode | `decode_column_logical_type_with_resolver(field, registry)` `catalog.rs:2093`; `LogicalTypeResolver::with_registry` |
| SQL carrier keyword | `physical_keyword(&DataType) -> Result<&'static str, _>` `binder.rs:3292` (closed carrier mapping; unsupported carriers must fail, not become row text) |
| 099 guards to retarget | `nominal_type_identity.rs`: `adr099_no_wendelon_pbc_wire_or_storage_surface`, `adr099_scoped_to_logical_types_only`, `adr099_information_schema_namespace_is_reserved_read_only` |

## Phase 1 — Wendelon: latest-only tenant descriptor listing (the cross-ring surface)

The bulk of genuinely-new code. Mirror the existing `list_logical_type_definitions` RPC end-to-end.

1. **Schema catalog (latest-only).** Add `latest_descriptors_for_tenant(tenant) -> Vec<TableSchemaDescriptor>`
   on `TableSchemaCatalogV1` (`schema/table.rs`), built from `latest_by_name` (resolve each tenant
   `TableNameCatalogKey` → its `TableSchemaCatalogKey` → the descriptor in `by_id`), returned sorted by
   `(namespace, table_name)`. **Do not** use the all-generations `descriptors_for_tenant`
   (`schema_generation: 0..=u64::MAX`). Tests: two generations → only the higher; two tenants isolated;
   deterministic sort.
2. **Wire + server.** Add a read-only Control-Ring operation beside `TABLE_DESCRIPTOR_GET_*` /
   `LOGICAL_TYPE_LIST_*`: request/response metas (`schema/core.rs`), adjacent `MsgType` constants,
   size/offset assertions + routing/known-mask updates. Handler authorizes the actor for the tenant,
   requires the Control-Ring leader, snapshots state, calls `latest_descriptors_for_tenant`, encodes a
   trailing list payload (`u32 count` + `(u32 len, bytes)*`, reusing the existing per-descriptor codec).
   Unversioned name (no `*V1`). Wire tests: multi-descriptor decode; rejected→no payload; malformed
   count/len rejects; non-leader + authz rejection; latest-only (old generation absent).
3. **Client.** `Client::list_table_schema_descriptors(tenant) -> io::Result<Vec<TableSchemaDescriptor>>`
   + a `parse_*_list_response` mirroring `parse_logical_type_list_response`.

## Phase 2 — Toron: `DescriptorSyncClient` listing method

`catalog.rs:298` — add to the trait + the `impl … for Client` delegation + the test fake
(`:2238` area):

```rust
async fn list_table_schema_descriptors(
    &self,
    tenant_id: WendelonTenantId,
) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, ClientError>;
```

## Phase 3 — Toron: `Catalog::refreshed_table_schema_descriptors` hook

Mirror `refreshed_logical_type_registry` (`:239` default, `:1741` Wendelon-backed). Trait:

```rust
async fn refreshed_table_schema_descriptors(
    &self,
) -> Result<Vec<TableSchemaDescriptor>, CatalogError>;   // reuse descriptor; no new carrier (D4)
```

- default / `MockCatalog`: return registered local descriptors (binder tests run without Wendelon);
- `WendelonCatalog` without a sync client: local snapshot (matching the `refreshed_logical_type_registry`
  no-client fallback);
- `WendelonCatalog` with a sync client: **(1)** refresh+register durable logical types; **(2)**
  `list_table_schema_descriptors`; **(3)** `CatalogError::AdminFailed` on client error (no stale partial);
  **(4)** sync/register each descriptor into the local registry (reuse the per-table sync/`upsert` path);
  **(5)** return the snapshot sorted `(namespace, table_name)`.

Tests: remote table not locally resolved appears; list failure fails despite a stale local descriptor;
logical-type refresh precedes descriptor sync; tenant scoping via `Catalog::scoped`.

## Phase 4 — Toron binder: two kinds + two arms (099-pattern reuse)

1. `ir/execution.rs:178`: add `InformationSchemaTables`, `InformationSchemaColumns` to `SystemTableKind`.
   **No walker sweep** (not a new `ExecutionPlan` variant); `rg "SystemTableKind::"` and add arms only
   where the kind is explicitly matched (expected: the binder dispatch only).
2. `bind_information_schema_relation` (`binder.rs:675`): retarget the single `logical_types` check to an
   explicit case-insensitive match over `logical_types`/`tables`/`columns`; extract a shared
   `(kind, schema, rows, projection)` projection helper (reuse `project_scalar_rows`). Add
   `information_schema_tables_schema()` / `..._columns_schema()` + row builders. Both arms call
   `catalog.refreshed_table_schema_descriptors().await?`, then read the local refreshed registry
   (`catalog.logical_type_registry()`) for name rendering (the hook already registered them — avoid a 2nd
   cross-ring list).
   - **tables row:** `[Utf8(namespace), Utf8(table_name), Utf8("BASE TABLE")]`.
   - **columns rows:** `let schema = descriptor.arrow_schema()?;` then per field (1-based `ordinal`):
     `[Utf8(namespace), Utf8(table_name), Utf8(field.name), Int64(ordinal),
     Utf8(physical_keyword(field.data_type())?), Utf8(if field.is_nullable() {"YES"} else {"NO"}),
     logical_type_name_or_null]` where the name =
     `decode_column_logical_type_with_resolver(field, LogicalTypeResolver::with_registry(&registry))?`
     → if naked, `Null`; else `LogicalTypeResolver::with_registry(&registry).spec(id).name` (**not**
     `logical_type_name(id)` fallback text, and not `registry.spec(id)` which would miss seed types).
     Order by `(namespace, table_name, ordinal)`.
3. Optional behavior-neutral cleanup that travels with the ADR: rename `physical_keyword` →
   `sql_carrier_keyword` (now serving logical-types *and* columns); keep the named unsupported-carrier
   error suitable for both call sites, and do not define debug-formatted `DataType` output as a contract.
4. The 099 read-only-namespace rejection + `is_information_schema_namespace` are unchanged (they already
   cover any reserved sub-name); unknown `information_schema.<name>` still rejects.

## Phase 5 — Behavior tests (`logical_type_ddl_test.rs`, where the 099 information-schema tests live)

The 11 ADR gates. Load-bearing three: **`..._descriptor_listing_failure_fails_query`** (fake client
returns `Err` → query errors, no rows, even with a stale local descriptor);
**`..._are_latest_only`** (feed old+new generation for one name → one current row);
**`..._renders_logical_type_names`** (durable annotation renders — proves refresh ordering). Use fake
`DescriptorSyncClient`s (incl. a table never locally resolved → authoritative-not-cache); `MockCatalog`
descriptors suffice for composition/scoping.

## Phase 6 — Architecture guards (retarget 099 + new adr102)

**Retarget (`nominal_type_identity.rs`):**
- `adr099_no_wendelon_pbc_wire_or_storage_surface` → narrow to **no execution-time storage scan / PBC /
  worker / table-query RPC** (102 adds a *bind-time control-plane* descriptor-list read — allowed).
- `adr099_scoped_to_logical_types_only` → `adr102_system_table_kind_has_finite_introspection_family`
  (exactly the three `InformationSchema*` variants).
- `adr099_information_schema_namespace_is_reserved_read_only` → keep target rejection; add `tables`/
  `columns` as valid sources.

**New `adr102_*`:** `..._dispatch_is_explicit`; `..._table_descriptor_refresh_is_authoritative`;
`..._wendelon_descriptor_list_is_latest_only_metadata` (built on `latest_by_name`, read-only, no
row/stats/PBC); `..._columns_decode_logical_types_after_refresh`; `..._system_table_execution_stays_coordinator_local`.
Wendelon-side guards (latest-only listing / read-only control-plane op) in the Wendelon architecture/wire
guard module.

## Phase 7 — Verification

```sh
cargo test -p wendelon table_schema_catalog_latest_descriptors   # latest-only listing
cargo test -p wendelon <control_ring table_descriptor_list>      # RPC roundtrip + latest-only server
cargo test -p toron --test logical_type_ddl_test information_schema   # 11 gates
cargo test -p toron --test architecture_test adr102              # new + retargeted guards
cargo test -p toron && cargo test -p wendelon                    # full suites, no regression
```

Keep **both** suites in acceptance — ADR 102 crosses the catalog boundary.

## Acceptance

`information_schema.tables`/`.columns` list the tenant's authoritative latest descriptors
(schema/name/`BASE TABLE`; column name/1-based-ordinal/SQL-type/`YES`-`NO`/logical-type-name),
refreshed-before-read, tenant-scoped, latest-only; a listing-RPC failure fails the query; unqualified
`tables`/`columns` stay normal lookups; unknown `information_schema.*` + mutation targets reject
(CTAS-from valid); composes; coordinator-local (listing at bind, not execution); the cross-ring listing is
read-only metadata built on `latest_by_name`; the three 099 guards are retargeted; both Toron and Wendelon
suites green.

## Risks / notes

- **Latest-only source** — build on `latest_by_name`, never raw `descriptors_for_tenant` (all
  generations); the `..._are_latest_only` gate catches a regression.
- **Refresh ordering** — logical types must refresh/register *before* descriptor decode, or
  `logical_type_name` resolves empty for durable annotations (the `..._renders_logical_type_names` gate).
- **Name rendering** — `logical_type_name(id)` is seed-oriented fallback text; durable display names come
  from `LogicalTypeResolver::with_registry(&registry).spec(id).name`.
- **099 guard `adr099_no_wendelon_pbc_wire_or_storage_surface` must be retargeted**, not left as-is — 102
  intentionally adds a Wendelon control-plane read; the absolute-no-Wendelon assertion would fail.
- **No new `ExecutionPlan` variant** — reuse `SystemTable`; do not re-do the 099 walker sweep.
- **Carrier rendering** — unsupported `DataType` must fail the query explicitly; no silent catch-all and no
  debug-formatted `data_type` row text.
- **Naming** — one unversioned listing operation; reuse the existing per-descriptor codec; no `*V1`/`*V2`.
