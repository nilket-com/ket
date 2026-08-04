# Impl 091: SQL Logical Type Use-Sites — DDL Column Position

**Pairs with:** `plans/adr/0091_sql_logical_type_ddl_columns.md` (Promoted, genuine 3-of-3).
**Cut:** `CREATE TABLE … customer_id CustomerId` only (CAST + typed CTAS → 092). **Core
mechanism:** resolution-timing shift — `CreateTable` carries unresolved logical-type names,
resolved at **bind** against a **durable-only (registry-only, seed-excluded)** catalog lookup;
field via the 089 writer; write-side fail-closed; the create-path self-rebind goes resolver-aware.
**Toron-only** (consumes 090's substrate; no Wendelon change).

## Invariants

- SQL DDL logical names resolve **durable-only** (registry, seed **excluded**) — distinct from
  decode (088/090 = seed ∪ registry). The seed-first
  `LogicalTypeResolver::spec_by_name`/top-level `logical_type_spec_by_name` path is **not** the
  DDL path; direct `LogicalTypeRegistry::spec_by_name` is allowed.
- Resolution happens at **bind** (async, has catalog); `SqlPlanner::plan` only records the name;
  `ExecutionPlan::CreateTable` stays a resolved `SchemaRef`.
- Write-side fail-closed: an undefined name errors **before** any descriptor is submitted.
- The resolved field is built via 089's `field_with_logical_type_metadata` (one writer).
- No Wendelon descriptor-metadata parsing; no executor/PBC/Rune change; no new nominal-gate logic.

## Phase 1 — plan shape (`ir.rs`)

```rust
pub struct CreateTableSchema { pub columns: Vec<CreateTableColumn> }
pub struct CreateTableColumn { pub name: String, pub nullable: bool, pub column_type: CreateTableColumnType }
pub enum CreateTableColumnType { Physical(DataType), LogicalName(String) }
// LogicalPlan::CreateTable { schema: SchemaRef, .. }  ->  { schema: CreateTableSchema, .. }
// ExecutionPlan::CreateTable keeps schema: SchemaRef (resolved at bind).
```
Only `LogicalPlan::CreateTable` changes. **Reconcile** the variant/shape guards + any
`CreateTable { schema: SchemaRef }` pin in the *same change* (discipline #4 — 090's review showed
the round shipped a red suite by skipping this).

## Phase 2 — durable-only registry lookup (`ir.rs` + `catalog.rs`) — the load-bearing bit

- **The trap (grounded):** `LogicalTypeResolver::spec_by_name` (`ir.rs:204`) is seed-first
  (`seed_…or_else(registry)`); `resolve_logical_type_by_name` (`catalog.rs:1116`) rides it.
  Reusing either for DDL would let the static pilot satisfy a SQL `CustomerId` → doctrine
  violation.
- **Registry-only path:** add `LogicalTypeResolver::durable_only(registry)` whose `spec_by_name`
  skips the seed branch (or call `LogicalTypeRegistry::spec_by_name(name)` directly).
- **Catalog method:** `async fn resolve_durable_logical_type_by_name(&self, name: &str) ->
  Result<LogicalTypeSpec, CatalogError>` on the `Catalog` trait. `WendelonCatalog`: check local
  tenant registry → on miss refresh via `list_logical_type_definitions` (`catalog.rs:286`) → else
  `CatalogError::LogicalTypeNotFound { name }` (transport/admin failures are bounded errors, not
  false unknowns). `MockCatalog`: local registry, durable-only.

## Phase 3 — bind resolution + field build (`binder.rs`)

`bind_create_table` iterates the `CreateTableSchema`: `Physical(dt)` → `Field::new`;
`LogicalName(name)` → `resolve_durable_logical_type_by_name(name).await?` →
`field_with_logical_type_metadata(col.name, spec.physical, col.nullable, spec.id)`. Undefined →
`CatalogError::LogicalTypeNotFound { name }` (D7) before emitting the resolved
`ExecutionPlan::CreateTable`.

## Phase 4 — create-path self-rebind resolver-aware (`catalog.rs`) — the correctness catch

`create_table_with_outcome` rebinds the new descriptor via seed-only `from_descriptor`
(`catalog.rs:636`, Mock; Wendelon analogously). Switch to
`from_descriptor_with_logical_types(local_table_id, &descriptor, with_registry(tenant registry))`
so a create that wrote a durable id doesn't reject its own output (the sync paths
`:458/:953/:1058` already do this — only the create rebind is stale).

## Phase 5 — SQL lowering (`sql_planner.rs`)

`lower_create_table_column`: primitives → `Physical`; `SqlDataType::Custom(name, mods)` →
`LogicalName(name)` iff one-part + unquoted ASCII + no modifiers + not `naked` case-insensitively
(else bounded plan-time reject), preserving the exact authored case for registry lookup. Flips the
current `lower_create_table_data_type` catch-all reject. CAST custom targets stay rejected (092).

## Phase 0 — fixtures first

`ddl_define_then_use_creates_typed_int64_descriptor` (non-null + reserved metadata);
`ddl_two_logical_types_then_bare_compare_rejects` (087/088 gate); `ddl_literal_policy_unchanged`
(`= 7`); **`ddl_undefined_logical_type_fails_closed`** + **`ddl_static_seed_does_not_satisfy_sql_name`**
(the seed-first trap — `CREATE TABLE (id CustomerId)` with only the seed, no `CREATE LOGICAL TYPE`,
must reject); `ddl_primitive_stays_naked`; `ddl_naked_modifier_quoted_multipart_reject`;
**`ddl_typed_table_survives_resync`** (090 durability ∘ real DDL);
**`ddl_type_defined_in_prior_process_resolves_via_wendelon_list`** (refresh-on-miss);
`cast_custom_target_still_rejects` (091/092 line); regression: 087/088/089/090 + all-naked
create_table/ctas/group_by green.

## Phase 6 — guards

The `adr091_*` set (ADR list), notably
`adr091_ddl_logical_type_resolution_is_registry_only_not_seed_first` (the DDL path uses the
registry-only lookup, NOT seed-first `LogicalTypeResolver::spec_by_name` or
`logical_type_spec_by_name`);
`adr091_catalog_create_rebinds_logical_descriptors_with_resolver` (no seed-only self-rebind);
`adr091_execution_create_table_stays_resolved_schema`;
`adr091_sql_planner_records_custom_type_names_without_catalog_resolution`;
`adr091_wendelon_lookup_refreshes_logical_type_list_before_unknown`;
`adr091_undefined_logical_type_fails_before_descriptor_submit`. **Reconcile** the variant/shape +
`CreateTable { schema: SchemaRef }` guards. Carry 087/088/089/090.

## Verification

```
cargo fmt
cargo test -p toron --test <ddl logical column tests>
cargo test -p toron --test architecture_test            # FULL suite — reconcile, do not leave red (090 lesson)
cargo test -p toron --test logical_type_definition_test  # 090 substrate intact
cargo test -p toron --test catalog_logical_identity_test --test rune_relation_handles_test
cargo test -p toron --lib
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron
git diff --check
```

## Evidence doc (`plans/impl/091_..._evidence.md`)

Record: files touched (`ir.rs`/`sql_planner.rs`/`binder.rs`/`catalog.rs` + tests; **no Wendelon
change**); the `CreateTableSchema` plan-shape + ExecutionPlan-stays-resolved; the durable-only
registry-only lookup (seed-first trap avoided) + refresh-on-miss; the create-path resolver-aware
rebind; write-side `LogicalTypeNotFound`; 089 writer reuse; the reconciled variant/shape guards
(suite green, not red); CAST still rejects; 087/088/089/090 + all-naked + `--all-targets` clippy
green.

## Watch items

1. **Durable-only vs seed-first** — DDL resolver MUST bypass the seed (registry-only). Do not
   reuse `resolve_logical_type_by_name` (seed-first). Guard + the `seed_does_not_satisfy` fixture.
2. **Create-path self-rebind** — `create_table_with_outcome` (`catalog.rs:636`) must rebind
   resolver-aware, or a valid create rejects its own durable-id descriptor.
3. **Refresh-on-miss** — durable lookup lists from Wendelon before `NotFound` (fresh process).
4. **Reconcile guards in the same change** — the plan-shape bumps variant/shape guards; reconcile
   them here. Run the *full* architecture suite; do not ship red (the explicit 090-review lesson).
5. **Resolution at bind, not plan** — `SqlPlanner::plan` stays sync/name-recording;
   `ExecutionPlan::CreateTable` stays resolved.

## Acceptance checklist

- [ ] `LogicalPlan::CreateTable` carries `CreateTableSchema` (Physical | LogicalName); `ExecutionPlan::CreateTable` resolved.
- [ ] Durable-only registry-only lookup (seed excluded) + refresh-on-miss via Wendelon list.
- [ ] Bind resolves + builds field via 089 writer; undefined → `LogicalTypeNotFound` before submit.
- [ ] Create-path self-rebind uses `from_descriptor_with_logical_types(with_registry)`.
- [ ] `naked`/modifiers/quoted/multi-part reject; `id BIGINT` naked; static seed does NOT satisfy a SQL name.
- [ ] CAST custom target still rejects (092); no Wendelon/executor/PBC/Rune change; no new nominal gate.
- [ ] Typed table survives resync + resolves in a fresh process; 087/088/089/090 + all-naked green.
- [ ] Variant/shape guards reconciled; FULL architecture suite + `--all-targets` clippy green (not red).
