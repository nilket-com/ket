# ADR 090 Evidence: Persisted Logical Type Definition Substrate

## Implementation Summary

Touched production files:

- `crates/wendelon/src/integration/compute.rs`
- `crates/wendelon/src/consensus/raft_storage.rs`
- `crates/wendelon/src/schema/core.rs`
- `crates/wendelon/src/network/control_server.rs`
- `crates/wendelon/src/client/core.rs`
- `crates/wendelon/crates/wendelon-client/src/lib.rs`
- `crates/toron/src/ir.rs`
- `crates/toron/src/ir/execution.rs`
- `crates/toron/src/catalog.rs`
- `crates/toron/src/storage.rs`
- `crates/toron/src/sql_planner.rs`
- `crates/toron/src/binder.rs`
- `crates/toron/src/coordinator.rs`
- enum-exhaustiveness touch points in Toron execution/fragment/server telemetry surfaces.

Touched tests:

- `crates/toron/tests/logical_type_definition_test.rs`
- `crates/toron/tests/architecture/nominal_type_identity.rs`
- `crates/toron/tests/wendelon_storage_execution_test.rs`
- `crates/wendelon/tests/compute_integration_contract.rs`

## Wendelon Authority

Wendelon now has `SchemaObjectKind::LogicalType` and a durable
`LogicalTypeCatalogV1` carrying `{logical_type_id, name, physical}` per tenant. The catalog
allocates ids above the static seed range (`LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1`), rejects
the `naked` sentinel, and treats same-name/same-physical create as idempotent while rejecting
same-name/different-physical.

`apply_schema_update` routes `SchemaObjectKind::LogicalType` to
`apply_logical_type_schema_update`. Only `Create` is accepted; `Alter`/`Drop`, refinements, and
secondary-index lifecycle mutations remain unsupported.

Wendelon exposes the definition through metadata/admin surfaces:

- `LogicalTypeDefinitionCreateRequest` submits a schema update envelope.
- `LOGICAL_TYPE_GET_REQ/RESP` reads back a durable definition by tenant/name.
- `LOGICAL_TYPE_LIST_REQ/RESP` lists tenant definitions for descriptor-sync registry refresh.
- `WendelonStorageAdmin::create_logical_type` submits the create, reads back the definition, and
  syncs the Toron registry cache.

There is no Toron-managed ordinary table or query-path `SELECT` authority for type definitions.

## Toron Registry And Decode

`ir.rs` now has:

- `LogicalTypeRegistry`
- `LogicalTypeResolver`
- `decode_column_logical_type_with_resolver`

The old seed-only `logical_type_spec` / `decode_column_logical_type` wrappers remain for Rune and
test call sites with no catalog context. Wendelon-backed descriptor registration now validates
field metadata through `TableBinding::from_descriptor_with_logical_types`, so a durable
descriptor id must resolve against `seed ∪ synced-registry` or descriptor binding fails closed.
On a WendelonCatalog descriptor miss, Toron now refreshes the tenant logical type registry through
the metadata list surface before decoding the fetched descriptor.

The dynamic registry is context-bearing and tenant-scoped through `WendelonDescriptorRegistry`;
there is no process-global mutable logical type registry.

## SQL Surface

`SqlPlanner` recognizes the ADR 090 surface:

```sql
CREATE LOGICAL TYPE <Name> AS <primitive>
```

It lowers to `LogicalPlan::CreateLogicalType` / `ExecutionPlan::CreateLogicalType` and executes
through `StorageAdmin::create_logical_type`. Physical carriers are limited to the ADR primitive
set. Multi-part names, `naked`, and unsupported physical carriers reject.

SQL use-sites remain ADR 091:

- `CREATE TABLE ... customer_id CustomerId` still rejects.
- `CAST(x AS CustomerId)` still rejects.

No executor kernel, PBC opcode, Rune API, or new `BoundExprNode` was added.

## Behavior Fixtures

`logical_type_definition_test.rs` covers:

- `planner_accepts_create_logical_type_surface`
- `planner_rejects_invalid_create_logical_type_shapes`
- `create_logical_type_executes_through_storage_admin`
- `duplicate_definition_same_physical_is_idempotent_and_different_rejects`
- `decode_resolves_durable_id_against_synced_registry`
- `logical_type_definition_survives_catalog_resync`
- `descriptor_referencing_undefined_id_fails_closed`
- `static_and_durable_ids_are_disjoint`
- `static_seed_still_resolves_for_rune_and_tests`
- `sql_column_and_cast_use_sites_still_reject`
- `binder_lowers_create_logical_type_without_engine_surface_changes`

Result:

```text
cargo test -p toron --test logical_type_definition_test
11 passed
```

`compute_integration_contract.rs` adds Wendelon-side contract coverage for the logical type
schema object payload, list payload round-trip, and idempotent durable-id allocation.

Result:

```text
cargo test -p wendelon --test compute_integration_contract
11 passed
```

`catalog.rs` adds the descriptor-miss sync regression:

- `resolve_table_bind_miss_syncs_logical_types_before_descriptor_decode`

Result:

```text
cargo test -p toron --lib resolve_table_bind_miss_syncs_logical_types_before_descriptor_decode
1 passed
```

## Architecture Guards

`nominal_type_identity.rs` adds the ADR 090 guard set:

- `adr090_logical_type_authority_is_wendelon_metadata_not_toron_query_path`
- `adr090_logical_type_id_allocation_is_wendelon_owned_and_durable`
- `adr090_decode_resolves_against_synced_registry_not_static_hardcode`
- `adr090_no_process_global_dynamic_logical_type_registry`
- `adr090_create_logical_type_keyword_not_create_domain`
- `adr090_static_pilot_seed_is_reserved_range_rune_test_bridge_only`
- `adr090_no_refinement_dropalter_or_pkfk_derivation`

Result:

```text
cargo test -p toron --test architecture_test adr090_
7 passed
```

Carried nominal guards:

```text
cargo test -p toron --test architecture_test nominal_type_identity::adr08
28 passed
```

## Verification Run So Far

```text
cargo fmt
cargo check -p toron --tests
cargo check -p wendelon --tests
cargo test -p toron --test logical_type_definition_test
cargo test -p wendelon --test compute_integration_contract
cargo test -p toron --test architecture_test adr090_
cargo test -p toron --test architecture_test nominal_type_identity::adr08
cargo test -p toron --test architecture_test
cargo test -p toron --lib
cargo test -p wendelon logical_type
cargo test -p toron --test catalog_logical_identity_test
cargo test -p toron --test create_table_test
cargo test -p toron --test ctas_test
cargo test -p toron --test group_by_test
cargo test -p toron --test rune_relation_handles_test rune_nominal_
cargo clippy -p toron --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

All commands above passed.

## Residual Scope

ADR 090 intentionally stops at the definition substrate. SQL column type use-sites,
`CAST(... AS LogicalType)`, CTAS materialization of dynamic logical ids, DROP/ALTER logical type,
refinements, PK/FK-derived identity, and read-only information-schema introspection remain future
ADR work.
