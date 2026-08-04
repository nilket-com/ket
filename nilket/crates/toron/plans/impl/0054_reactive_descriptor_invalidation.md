# Impl Plan 054: Reactive Descriptor Invalidation

**Status:** Promoted, paired with
`plans/adr/0054_reactive_descriptor_invalidation.md`.
**Posture:** Single plan, Toron-ring only. Reuses ADR 053's bind-miss
sync + budget + single-flight wholesale — no new fetch path, no new
budget. Eleven steps; dependency-ordered.

## Invariants carried in

- `crates/nilket` runtime-free; `toron_fixture_worker.rs` unchanged;
  brand-clean / no-`*V1`.
- ADR 049 tenant source, 050 memory, 051 bootstrap, 052 concurrency,
  053 sync — behaviorally unchanged; their guards keep passing (with the
  053 evolved-path guard retargeted, ADR D-set).
- No Wendelon server / SQL wire / DDL change. No Governance Phase 3
  behavior (→ ADR 055).

## Phase 0 — Audit (verified)

- `StorageError::DescriptorEvolved` is unit-shaped (error.rs:14);
  `From<ClientError>` maps `TableDescriptorEvolved` context-free
  (error.rs:54). `TaskFailureKind::DescriptorEvolved { expected,
  current }` has fingerprints, no table id (error/distributed.rs:12).
- SELECT one-retry loop at coordinator.rs:879-894; write/delete/CTAS
  (`execute_write_plan`, ~897) have no retry loop.
- Storage `DescriptorEvolved` construction sites: storage.rs
  266/300/303/333/336/379/382/719/722, coordinator.rs:1296 — each holds
  `request.table_id`.
- ADR 053 reuse: `WendelonCatalog::resolve_table` bind-miss sync,
  `DescriptorSyncFlights`, `sync_descriptor_for_tenant`,
  `next_table_id`/`bump_next_wendelon_table_id`, registry maps; the
  `max_concurrent_descriptor_syncs` accountant. All reused unchanged.

## Phase 1 — Error identity (error.rs, error/distributed.rs)

- `StorageError::DescriptorEvolved { table_id: TableId }` (D3).
- `TaskFailureKind::DescriptorEvolved { table_id, expected, current }`
  (D4) + task-failure serialization/tests. Fingerprints stay
  diagnostic; `table_id` is the invalidation key.

## Phase 2 — Contextual storage mapping (storage.rs, server/mod.rs, exec sites)

Replace context-free `ClientError::TableDescriptorEvolved` mapping with
explicit `StorageError::DescriptorEvolved { table_id: request.table_id }`
at every construction site (D5). Keep `From<ClientError>` for non-evolved
arms if useful; the evolved arm must not erase identity.

## Phase 3 — Catalog + registry invalidation (catalog.rs)

```rust
pub struct InvalidatedDescriptor {
    pub table_id: TableId,
    pub table_object_id: TableObjectId,
    pub fingerprint: PhysicalDescriptorFingerprint,
    pub refs: Vec<TableRef>,          // needed by the D12 replacement guard
}

// Catalog trait (no silent default):
async fn invalidate_table_descriptor(&self, table_id: TableId)
    -> Result<Option<InvalidatedDescriptor>, CatalogError>;

// Registry:
pub fn invalidate_descriptor_for_tenant(&self, tenant_id, table_id)
    -> Option<InvalidatedDescriptor>;
```
Registry algorithm (D7): write lock; remove `by_id[(t,id)]` (None if
absent → `missing` metric); remove descriptor payload; remove every
`by_ref` alias whose value == id; return old object id + fingerprint +
removed refs. Never bump `next_table_id`, call sync, swap the `Arc`,
hold the lock across an await, or touch another tenant. Implement for
mock + Wendelon catalogs.

## Phase 4 — Coordinator helpers (coordinator.rs)

```rust
fn descriptor_evolved_table_id(error: &QueryError) -> Option<TableId>; // local + exec + distributed
async fn invalidate_descriptor_evolution(catalog, error) -> Result<Option<InvalidatedDescriptor>, QueryError>;
async fn reject_replacement_object(catalog, invalidated: &[InvalidatedDescriptor]) -> Result<(), QueryError>;
```
`reject_replacement_object` re-resolves each invalidated **ref** after
re-bind: resolves to the same object → proceed; resolves only to a
different object → `SchemaInstability` (D12); all refs missing → let the
bind miss surface `TableNotFound` (D13).

## Phase 5 — SELECT retry (coordinator.rs:879-894, the existing loop only)

Track `retried` + `invalidated: Vec<InvalidatedDescriptor>`. On first
`DescriptorEvolved`: invalidate (record result), set retry, loop to
re-bind (053 re-sync). Before executing a *retry* bind with non-empty
`invalidated`, run the replacement guard. Second `DescriptorEvolved` →
`SchemaInstability` (D10). The re-bind miss must use ADR 053 — no
force-refresh, no admission bypass.

## Phase 6 — Writes/deletes/CTAS (coordinator.rs)

Wrap the write result: on `DescriptorEvolved`, invalidate, then return
the original error if invalidation succeeds — **no retry** (D11). If
invalidation itself fails, surface that failure because the cache was not
repaired:
```rust
let result = self.execute_write_plan(catalog, &bound).await;
if let Err(e) = &result {
    if descriptor_evolved_table_id(e).is_some() {
        invalidate_descriptor_evolution(catalog, e).await?;
    }
}
result
```

## Phase 7 — Metrics (catalog.rs, network/observability.rs)

Aggregate sibling counter `catalog_descriptor_refresh_requests_total`
with bounded `outcome` values `invalidated`, `missing`, and
`replacement`; no tenant/table/object labels (D15). ADR 053 sync metrics
remain the fetch-outcome source.

## Phase 8 — Guards + fixtures (tests/architecture_test.rs, catalog/coordinator tests)

14 architecture + 3 convention guards (ADR list); retarget ADR 053's
`descriptor_evolved_path_stays_unchanged` guard (ADR D-set). 13 required
fixtures + 1 optional (ADR list), incl. drop-and-recreate →
`SchemaInstability`, write-invalidates-no-retry, distributed-evolved
carries table id, idempotent invalidation, single-flight reuse.

## Phase 9 — Evidence + roadmap

- `plans/impl/0054_reactive_descriptor_invalidation_evidence.md`:
  `architecture_test adr054`, full `architecture_test`, `cargo test -p
  toron`, `cargo clippy -p toron --all-targets -- -D warnings`.
- Roadmap: 054 status (completes liveness: 053 miss→sync, 054
  stale→invalidate→sync); ADR 055 candidate = Governance Phase 3, the
  ADR 050/052 hard-reject reversal.

## Watch-items

1. No registry write lock across an await (invalidation is sync; re-sync
   is 053's path, which locks only after the fetch).
2. Replacement-guard scope is the *current statement*; the new object
   stays registered for later statements.
3. Path-agnostic invalidation must not become write-retry (D11).
4. Distributed `TableId` is carried on the task failure (D4), not
   inferred from the plan. The D12 replacement guard uses the refs
   captured in `InvalidatedDescriptor`.

## Acceptance checklist

- [ ] Phase 1-2: evolved errors carry table id (local + distributed);
      Wendelon evolved mapped with request context.
- [ ] Phase 3: `InvalidatedDescriptor`; registry invalidation scoped,
      idempotent, alias-complete; returns object id + refs.
- [ ] Phase 4-5: SELECT invalidates → rebinds (053) → replacement guard
      → execute once; 2nd evolution → `SchemaInstability`.
- [ ] Phase 6: writes/deletes invalidate, no retry.
- [ ] Phase 7: aggregate bounded metrics.
- [ ] Phase 8-9: 14+3 guards, 13 fixtures, prior guards pass (053
      retarget), evidence + roadmap; no Governance Phase 3 drift.
