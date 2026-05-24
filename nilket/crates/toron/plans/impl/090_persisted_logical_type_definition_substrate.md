# Impl 090: Persisted Logical Type Definition Substrate

**Pairs with:** `plans/adr/090_persisted_logical_type_definition_substrate.md` (Promoted, genuine
3-of-3). **Cut B** — the durable definition substrate only (SQL use-sites → 091). **Cross-ring:**
a new Wendelon `SchemaObjectKind::LogicalType` (durable authority) + the Toron consumer surface
(`CREATE LOGICAL TYPE`, catalog sync, decode rewire). **Invariant:** a descriptor may reference
only a logical type id whose definition is durably stored in the same metadata authority.

## Invariants

- Durable `{id, name, physical}` lives in Wendelon (same authority as descriptors), tenant-scoped,
  Wendelon-allocated id, riding existing schema-object machinery.
- **Authority vs representation:** authority = Wendelon schema metadata (id alloc, persist, sync via
  admin APIs); physical representation may be a Wendelon-**internal** catalog table/object —
  invisible to Toron, never the query path.
- **Minimal awareness:** Wendelon owns the registry object + id allocation + sync; it does **not**
  parse descriptor field-metadata to validate references (preserves 088's opaque-IPC doctrine). The
  reference invariant is enforced Toron-side: write refuses undefined ids (091), read = decode
  fail-closed.
- Static pilot seed (`CustomerId=1`/`ProductId=2`) is a reserved-range Rune/test bridge; durable
  ids disjoint (start above the reserved range).
- No SQL use-sites (091); no `Expr`/`BoundExprNode` change; no executor/PBC/Rune change.

## Phase W — Wendelon side (contract; likely paired Wendelon-sequence work)

- `SchemaObjectKind::LogicalType` in `wendelon::integration::compute`; payload
  `{ logical_type_id: u32, name: String, physical: <primitive> }`, tenant-scoped.
- ADR 090 supports one-part, tenant-global logical type names. If the existing Wendelon
  `SchemaObjectRefV1` envelope requires a namespace, store logical types under a fixed/reserved
  logical-type namespace internally; schema-qualified logical type names are deferred.
- **Wendelon allocates** the durable `logical_type_id` (stable across refresh/restart), created via
  `SchemaMutationKindV1::Create` (origin `ComputeClient`), versioned by `SchemaGeneration`,
  persisted + listable for sync.
- Wendelon owns create idempotency: duplicate name + same physical returns the existing durable id;
  duplicate name + different physical rejects. Toron cache checks are advisory only.
- The council fixes this contract; storage internals are Wendelon's. Reserved id range for
  static-seed disjointness is a shared constant.

## Phase 1 — Toron registry view + resolution rewire (`ir.rs` + `catalog.rs`) — THE BULK

- **Synced per-tenant view** on the catalog (alongside descriptor sync):
  `LogicalTypeRegistry { by_id: Map<u32, LogicalTypeSpec>, by_name: Map<String, LogicalTypeId> }`,
  populated from Wendelon `LogicalType` objects on the descriptor sync path.
- **Resolution rewire (the ripple):** `logical_type_spec(id)` / `logical_type_spec_by_name(name)`
  become **layered** — static seed (reserved range, built-in; keeps Rune/087 + tests working with
  no catalog context) **∪** synced registry (durable, tenant-scoped). These are currently free fns
  over a static `match`; registry-backing means threading the synced view into the call sites that
  have tenant/catalog context — 088 `decode_column_logical_type` runs in `from_descriptor` (has the
  descriptor + tenant → thread the registry there). Rune/test sites keep the static-seed fallback.
- Use explicit resolver context / registry arguments for the dynamic layer. Do **not** introduce a
  process-global mutable registry; it would violate tenant isolation and make tests order-dependent.
- **Decode honesty + invariant guard:** `from_descriptor` resolves each field's `logical_type_id`
  against (seed ∪ registry); absent from both → bounded `CatalogError` (invariant violated → fail
  closed, loud not silent).

## Phase 2 — `CREATE LOGICAL TYPE` surface (`sql_planner.rs` + `coordinator.rs`)

- **Parser:** `CREATE LOGICAL TYPE <Name> AS <primitive>` — sqlparser lacks it (and `CreateType`
  is the wrong Postgres composite/enum shape; no `CREATE DOMAIN`), so a small Toron recognizer
  handles this statement. Lower to `LogicalPlan::CreateLogicalType { name, physical }` (sibling of
  `CreateTable`); reject `naked`, modifiers, multi-part names, non-primitive physical.
- **Coordinator:** execute `CreateLogicalType` via `admin.create_logical_type(tenant, name,
  physical)` → Wendelon allocates+persists → trigger registry sync. Idempotent same-definition ok;
  different-physical → bounded error (reuse the CTAS-IF-NOT-EXISTS idempotency/honesty shape at
  `coordinator.rs` — don't silently mutate a definition). The authoritative decision comes from
  Wendelon/admin, not from a potentially stale Toron registry cache.
- `SqlPlanner::plan` is sync + catalog-less → resolution/persistence happens at execute (async, has
  catalog/admin), exactly as `CreateTable` does today.

## Phase 0 — fixtures first

The 12 ADR gates:

1. `create_logical_type_persists_durable_tenant_scoped_entry`
2. `logical_type_definition_survives_catalog_resync` (durability — rebuild the view from Wendelon;
   the "next session" honesty proof)
3. `duplicate_definition_same_physical_is_idempotent`
4. `duplicate_definition_different_physical_rejects`
5. `create_logical_type_naked_sentinel_rejects`
6. `create_logical_type_non_primitive_physical_rejects`
7. `decode_resolves_durable_id_against_synced_registry`
8. `descriptor_referencing_undefined_id_fails_closed` (invariant guard)
9. `static_seed_still_resolves_for_rune` (087 `.as_logical_type::<CustomerId>()` green)
10. `static_and_durable_ids_are_disjoint`
11. `sql_create_table_custom_type_still_rejects` (the 090/091 line)
12. `sql_cast_custom_target_still_rejects`
+ regression: 087/088/089 + all-naked DDL/CTAS/group-by green.

## Phase 3 — guards

The `adr090_*` set (ADR list), notably
`adr090_logical_type_authority_is_wendelon_metadata_not_toron_query_path` (authority via admin/sync
as metadata; Wendelon-internal catalog table allowed; Toron-managed ordinary table read at bind
rejected); `adr090_decode_resolves_against_synced_registry_not_static_hardcode`;
`adr090_descriptor_id_definition_co_authority_invariant`;
`adr090_static_pilot_seed_is_reserved_range_rune_test_bridge_only`; `adr090_no_sql_use_sites_yet`.
Add `adr090_no_process_global_dynamic_logical_type_registry`.
Carry 087/088/089 guards.

## Verification

```
cargo fmt
cargo test -p toron --test <logical-type-definition tests>
cargo test -p toron --test architecture_test adr090_
cargo test -p toron --test architecture_test adr087_ adr088_ adr089_   # carried
cargo test -p toron --test catalog_logical_identity_test               # 088 decode honesty
cargo test -p toron --test rune_relation_handles_test rune_nominal_     # static seed intact
cargo test -p toron --lib                                              # catalog sync + distributed
cargo clippy -p toron --all-targets -- -D warnings
cargo test -p toron
# + Wendelon-side: SchemaObjectKind::LogicalType persistence/sync tests (Wendelon sequence)
```

## Evidence doc (`plans/impl/090_..._evidence.md`)

Record: files touched (Wendelon `integration::compute` + Toron `ir.rs`/`catalog.rs`/`sql_planner.rs`/
`coordinator.rs` + tests); the new `SchemaObjectKind::LogicalType` + `StorageAdmin::create_logical_type`;
the synced `LogicalTypeRegistry` view + the `logical_type_spec`/decode rewire (seed ∪ registry); the
durability fixture (survives resync); the invariant fail-closed guard; that authority is metadata
(no query-path read), Wendelon stays minimally aware (no descriptor-metadata parsing); SQL use-sites
still reject; no executor/PBC/Rune change; 087/088/089 + all-naked + `--all-targets` clippy green.

## Watch items

1. **The resolution rewire is the Toron-side bulk + the risk.** Thread the synced registry into
   `from_descriptor`/decode without breaking the static-seed (no-context) path for Rune + 087/088/089.
   Layered seed ∪ registry; static seed always resolves.
2. **Cross-ring sequencing.** The Wendelon `SchemaObjectKind::LogicalType` persistence must land (or
   be stubbed behind the `StorageAdmin` contract) before the durability fixtures run end-to-end;
   coordinate as paired Wendelon-sequence work. Until then, a mock `StorageAdmin` proves the Toron
   consumer surface.
3. **Id-range discipline.** Reserved low range for the static seed; Wendelon allocator starts above
   it; shared constant + guard prevents collision.
4. **Idempotency parity with CTAS-IF-NOT-EXISTS.** Same-definition re-create = no-op; different
   physical rejects — reuse the 089 reconcile-by-policy framing (don't silently mutate a definition).
5. **No use-site leakage.** `CREATE TABLE … CustomerId` / `CAST(… AS CustomerId)` must still reject
   (091 line); the parser resolves `CREATE LOGICAL TYPE` but column/cast custom-target paths stay
   unsupported. Guard it.
6. **Tenant scoping = the authority.** "Same metadata authority" = the tenant's Wendelon schema
   store; the registry view is per-tenant; cross-tenant id resolution must not leak.
7. **Authority not query-path.** Resolution goes through the catalog admin/sync surface, never a
   `SELECT` against a registry table; a Wendelon-internal catalog table is fine as representation.
8. **No global registry shortcut.** Thread a registry context into decode/resolution sites that have
   catalog context; keep seed-only helpers for Rune/tests. A global dynamic map would be easier but
   wrong for tenant isolation and restart semantics.

## Acceptance checklist

- [ ] Wendelon `SchemaObjectKind::LogicalType` + durable tenant-scoped `{id, name, physical}` +
      Wendelon-allocated stable id + sync (contract met / paired Wendelon work).
- [ ] `StorageAdmin::create_logical_type` RPC; coordinator executes `CreateLogicalType`.
- [ ] `CREATE LOGICAL TYPE <Name> AS <primitive>` parses + lowers; `naked`/modifiers/multipart/
      non-primitive reject; idempotent same-def, different-physical rejects.
- [ ] Catalog syncs per-tenant registry; `logical_type_spec`/`_by_name` resolve seed ∪ registry;
      decode (088) consults it; undefined id fails closed (invariant).
- [ ] Dynamic logical type resolution is context-bearing; no process-global mutable registry.
- [ ] Static seed reserved-range Rune/test bridge intact; durable ids disjoint.
- [ ] Authority is metadata (admin/sync), not a Toron query-path table; Wendelon stays minimally
      aware (no descriptor-metadata parsing).
- [ ] SQL column/CAST custom targets still reject (091 line); no `Expr`/`BoundExprNode`/executor/
      PBC/Rune change.
- [ ] Durability fixture (survives resync) + all 087/088/089 + all-naked suites green; full package
      + `--all-targets` clippy clean.
