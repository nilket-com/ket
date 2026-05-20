# ADR 053: Governed Bind-Miss Descriptor Sync Consumer

**Status:** Promoted
**Seed:** ADR 052 §D15 / Wendelon ADR 072 §D14 — live Wendelon → Toron
descriptor sync, sequenced consumer-after-producer. The Wendelon 072
producer now exists; this is its Toron consumer.
**Council:** Claude, Codex, Gemini (iter-0 split — Codex+Gemini led the
consumer, Claude led Governance Phase 3; iter-1 3-of-3 on the consumer
once 072 was recognized as the unblocker, Claude's "sync is a governed
resource" objection made load-bearing; iter-2 dual resolved three
splits — throttle→resource-exhaustion, retry-in-`resolve_table`,
single-flight-outer; iter-3 settled the error seam as
`CatalogError::ResourceExhausted`, Claude retracting an
`ExecutionError` proposal).
**Category:** Toron compute-ring substrate ADR — catalog bind-path
liveness. Exterior Substrate → Toron home. Node-local; consumes the
Wendelon 072 exact-name descriptor producer; no Wendelon server change,
no SQL wire change.

## Summary

Make `WendelonCatalog::resolve_table` **self-healing**: on a local
descriptor-registry miss for the scoped tenant, fetch the exact
`(tenant_id, namespace, table_name)` descriptor from the Wendelon 072
producer, merge it **in place** into the shared registry, and return the
binding — so a table created in Wendelon after a Toron daemon starts
becomes queryable without a restart. The sync is a **governed resource**:
per-tenant `max_concurrent_descriptor_syncs`, **single-flight** so a miss
storm spends one reservation and one RPC, and throttle rejection surfaced
as `ResourceExhausted` (not table-not-found). The merge is **insert-only**;
staleness of an already-cached descriptor is **out of scope and
fail-closed** at the storage path via the existing `DescriptorEvolved`
(ADR 051 D8). This closes ADR 051's "startup-only" gap without turning
Toron into a descriptor-management system.

## Motivation

ADR 051 made startup descriptors tenant-explicit but **startup-only**;
ADR 052 hardened per-tenant resource isolation; Wendelon ADR 072 shipped
the authoritative read-only exact-name descriptor producer. The
remaining gap: a Wendelon table that exists but was absent from Toron's
startup snapshot fails legitimate SQL until the daemon restarts. ADR 053
consumes 072 to close exactly that gap, and only that gap — Claude's
iter-0 governance objection is honored by making the sync itself a
budgeted, duplicate-collapsed resource so "liveness" cannot become an
unbounded RPC vector against the Control Ring leader.

## Decisions (13, Promotion-Locked)

**D1: Toron-owned consumer.** ADR 053 lives in Toron and changes
`WendelonCatalog` bind behavior on a local name miss. No Wendelon
server, router, opcode, reason-code, authorization, or descriptor-write
change. The one Wendelon-side edit is **public client facade plumbing**:
expose the 072 inner method on `wendelon_client::Client` (D5) — not a
072 reopen.

**D2: Scale boundary.** One Toron daemon; node-local admission; one
exact lookup per canonical `(TenantId, namespace, table_name)` bind
miss. **Out of scope:** background sync, descriptor inventory/listing,
file watch/reload, negative cache/TTL, SQL wire/DDL change, table-id
lookup, storage-path sync, refresh on `DescriptorEvolved`, write retry
after evolution, registry `Arc` replacement, cluster-wide
semaphore/fairness/borrowing.

**D3: Trigger — `resolve_table` miss only.** Sync fires *only* from
`WendelonCatalog::resolve_table` after a `WendelonDescriptorRegistry`
miss for the scoped tenant. **Not** from `resolve_table_id`, storage
`TableNotFound`, `DescriptorEvolved`, `create_table`/`drop_table`/
`CREATE TABLE IF NOT EXISTS`, coordinator bind-retry, or any background
task.

**D4: Canonical exact-name key.** Qualified `schema.table` →
`(tenant_id, "schema", "table")`; unqualified `table` →
`(tenant_id, "default", "table")` (Toron's existing Wendelon default
namespace). `table` and `default.table` share one canonical
single-flight key. No namespace enumeration, no fallback search.

**D5: Public client facade.** Expose ADR 072 through the wrapper Toron
holds: `wendelon_client::Client::get_table_schema_descriptor(tenant_id,
namespace, table_name) -> Result<Option<TableSchemaDescriptor>,
ClientError>`. The inner client owns leader-hint retry and maps
`CONTROL_REASON_NOT_FOUND → Ok(None)`; Toron adds no `NOT_LEADER` logic.

**D6: Per-tenant descriptor-sync budget.** `TenantBudget` gains
`max_concurrent_descriptor_syncs: NonZeroU32` (named in full, beside
`max_concurrent_queries`). Validation mirrors ADR 052: explicit `0` →
`"tenant_budgets max_concurrent_descriptor_syncs must be greater than
0"`; omitted → `min(2, max_concurrent_queries)` clamped ≥ 1;
`TenantBudgetPolicy::from_query_budget` → `1`.

**D7: RAII sync accountant.** `TenantDescriptorSyncAccountant` /
`TenantDescriptorSyncReservation` / `TenantDescriptorSyncRejection`,
shaped like ADR 052's query-concurrency accountant. Reservation lifetime
is **RPC + merge only** — it releases **before** query execution and on
cancellation via `Drop` (distinct from the 052 query reservation, which
spans execution).

**D8: Single-flight outer, budget inner.** `WendelonCatalog` owns a
node-local descriptor-sync flight coordinator, shared across `scoped(...)`
clones, keyed by the canonical `(TenantId, namespace, table_name)`. The
first caller owns the flight, reserves **one** sync slot, and sends
**one** Wendelon RPC; waiters join the flight and then re-resolve the
local registry — waiters consume **no** budget and send **no** RPC.
Cancellation of the owner releases the reservation and clears the flight
so a later miss re-initiates.

**D9: In-place, insert-only registry merge.** Add
`WendelonDescriptorRegistry::sync_descriptor_for_tenant(&self, tenant_id,
requested: &TableRef, descriptor) -> Result<TableBinding, CatalogError>`.
It validates the descriptor namespace/table against the canonical
request; under the write lock allocates a **fresh Toron-local `TableId`**
from `next_table_id` (collision-free under the registry's existing
next-table-id bump discipline, including ADR 051 bootstrap ids);
preserves Wendelon's `table_object_id` / `schema_generation` / physical
fingerprint via `TableBinding::from_descriptor`; inserts the qualified
binding + descriptor maps + the unqualified alias (`or_insert`); returns
the existing binding on a same-identity race; rejects a
different-identity conflict with `AdminFailed`. Same identity means the
object id, schema generation, physical fingerprint, and schema match. It
must **not** call `from_tenant_descriptors`, swap the registry `Arc`, use
the Wendelon object id as a local `TableId`, or **overwrite** an existing
name with a different descriptor identity. (Insert-only: 053 fills a
miss; it never refreshes.)

**D10: Miss / error mapping.** Local hit is the fast path. On miss:
(1) no configured client → `TableNotFound`; (2) sync budget exhausted →
**`CatalogError::ResourceExhausted`** with `"tenant descriptor sync
limit exhausted"`, mapped at `daemon.rs::map_sql_query_error` to
`SqlErrorCode::ResourceExhausted` (a new arm before the generic
`BindingError::Catalog(_)`/`Catalog(_) → CatalogError` arms); (3)
Wendelon `Ok(None)` → `TableNotFound`; (4) `Ok(Some(descriptor))` →
validate, merge, return binding; (5) name mismatch / registry conflict →
`AdminFailed` ("descriptor sync failed" prefix); (6) Wendelon
auth/transport/invalid error → `AdminFailed`, same prefix. Authorization
failure is **not** table-not-found. The retry is the owner's direct
return / a waiter's single local re-resolve — **inside** `resolve_table`,
**no** coordinator bind-retry loop.

**D11: Execution paths unchanged, fail-closed.** Post-bind execution is
identical to an ADR 051-bootstrapped descriptor: scan/write/delete read
the shared registry by `(tenant_id, table_id)`; storage table-object /
physical-fingerprint checks stand; the SELECT `DescriptorEvolved` retry
loop is unchanged and is the **fail-closed backstop** for a cached
descriptor that Wendelon later evolved/dropped (053 does not refresh —
deferred ADR). No write retry after evolution.

**D12: Observability — aggregate only.** Add only
`catalog_descriptor_sync_requests_total` with a fixed outcome label:
`success`, `not_found`, `throttled`, or `error`. No tenant / namespace /
table / object-id / error-string labels. A later resource-family
exposure would use the ADR 052 configured-tenant-or-`"other"` bounded
rule; 053 does not need it.

**D13: Security unchanged.** Toron uses the same configured Wendelon
client identity and 072 authorization path. No new role, SQL tenant
claim, SQL wire field, SQL message, or descriptor-write privilege.

**Single-plan posture; Toron-ring only.**

## Architecture Guards (19, Promotion-Locked)

1. `adr053_descriptor_sync_lives_in_toron_consumer`
2. `adr053_wendelon_server_descriptor_get_surface_unchanged`
3. `adr053_resolve_table_miss_is_only_sync_trigger`
4. `adr053_descriptor_sync_key_is_tenant_and_exact_name`
5. `adr053_unqualified_bind_miss_uses_default_namespace`
6. `adr053_singleflight_shared_across_scoped_catalogs`
7. `adr053_singleflight_waiters_do_not_consume_sync_budget`
8. `adr053_descriptor_sync_budget_is_nonzero_configured`
9. `adr053_sync_budget_rejection_maps_resource_exhausted`
10. `adr053_descriptor_sync_uses_client_facade`
11. `adr053_sync_merges_registry_in_place_no_arc_swap`
12. `adr053_runtime_sync_does_not_call_from_tenant_descriptors`
13. `adr053_no_background_watch_reload_inventory_or_negative_cache`
14. `adr053_create_drop_do_not_trigger_descriptor_sync`
15. `adr053_resolve_table_id_and_storage_do_not_trigger_descriptor_sync`
16. `adr053_descriptor_evolved_path_stays_unchanged`
17. `adr053_catalog_liveness_metrics_have_no_tenant_or_table_labels`
18. `adr053_retry_is_internal_to_resolve_table_not_coordinator`
19. `adr053_prior_adr049_050_051_052_guards_continue_to_pass`
    (meta-guard)

**Convention guards:**
- `adr053_nilket_root_stays_runtime_free`
- `adr053_toron_fixture_worker_source_unchanged`
- `adr053_new_surface_is_brand_clean_and_has_no_v1_suffix`

## Integration Fixtures (29 required + 1 optional)

Config/budget: `tenant_budget_config_accepts_descriptor_sync_limit`,
`..._rejects_zero_descriptor_sync_limit`,
`..._derives_descriptor_sync_limit_when_omitted`,
`tenant_descriptor_sync_accountant_releases_on_drop`,
`..._isolated_by_tenant`, `..._releases_on_cancelled_flight`.
Catalog bind-miss: `resolve_table_bind_miss_fetches_descriptor_and_returns_binding`,
`resolve_table_local_hit_does_not_call_wendelon`,
`..._unqualified_miss_fetches_default_namespace`,
`..._qualified_miss_fetches_exact_namespace`,
`..._not_found_remains_table_not_found`,
`..._fetch_error_is_admin_failed`,
`..._auth_error_is_not_table_not_found`,
`..._throttled_is_resource_exhausted`.
Single-flight: `concurrent_same_table_bind_misses_issue_one_descriptor_get`,
`..._share_one_sync_reservation`,
`..._converge_to_one_local_table_id`.
Merge helper: `registry_sync_descriptor_for_tenant_rejects_name_mismatch`,
`..._allocates_fresh_local_table_id`, `..._preserves_physical_fingerprint`,
`..._returns_existing_same_identity`, `..._rejects_existing_conflict`.
Isolation/integration: `descriptor_sync_is_tenant_scoped_across_same_table_name`,
`storage_scan_uses_synced_descriptor_after_bind_miss`,
`sql_select_binds_wendelon_table_missing_from_startup_bootstrap`,
`sql_insert_binds_wendelon_table_missing_from_startup_bootstrap`,
`create_if_not_exists_local_miss_does_not_descriptor_get`,
`descriptor_evolved_retry_loop_is_not_expanded_by_bind_miss_sync`,
`catalog_descriptor_sync_metrics_are_aggregate_outcome_only`.
**Optional:** `qualified_default_and_unqualified_bind_misses_share_singleflight`.

## Out of Scope

- Descriptor **refresh** / external-DROP invalidation / `DescriptorEvolved`
  refresh / TTL — fail-closed by the runtime; a future ADR if evidence
  asks.
- Negative caching; background sync; inventory/listing; reload/watch.
- Table-id-based lookup; storage-path or create/drop-triggered sync.
- Coordinator bind-retry; registry `Arc` swap; cluster-wide
  fairness/borrowing/queueing (Governance Phase 3 — ADR 054 candidate).
- Any Wendelon server change; SQL wire/DDL change; new role/claim.

## Council History

- **Iter-0:** Codex+Gemini led the consumer; Claude led Governance
  Phase 3 and flagged the sync-storm DoS risk.
- **Iter-1:** 3-of-3 on the consumer once 072 was the clear unblocker;
  Claude's "sync is a governed resource" made load-bearing (single-flight
  + per-tenant sync budget); Claude withdrew the governance contest
  (→ ADR 054 candidate) and locked the **insert-only / fail-closed**
  scope the others left implicit.
- **Iter-2 (dual):** resolved three splits — throttle → resource
  exhaustion (not `TableNotFound`); retry inside `resolve_table` (not the
  coordinator); single-flight outer / budget inner / owner-only
  reservation.
- **Iter-3 (promoted):** error seam settled as
  **`CatalogError::ResourceExhausted`** (Claude retracted an
  `ExecutionError` proposal — `resolve_table` returns `CatalogError`);
  `max_concurrent_descriptor_syncs` naming; aggregate outcome-counter
  metrics.

**Settled:** throttle is resource exhaustion, not absence; retry is
catalog-internal; merge is insert-only; no negative cache; descriptor
refresh is a future ADR.

## Promotion criteria met

1. D-set locked at 13 (3-of-3).
2. Guards 19 + 3 convention; ADR 049-052 meta-guard.
3. Fixtures 29 required + 1 optional.
4. Toron-ring; node-local; no Wendelon server change; no SQL wire
   change; consumes 072.
5. Placement: Exterior Substrate → Toron home.
6. Brand-clean / no-`*V1`; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 049-052 invariants preserved; the merge is insert-only and
   fail-closed via the unchanged `DescriptorEvolved` path.

## Conclusion

ADR 053 makes Toron's catalog self-healing on a bind miss, consuming the
Wendelon 072 producer through a governed, single-flighted, insert-only
sync — closing the runtime descriptor-liveness gap that ADR 051 left to
startup, while keeping Wendelon authoritative (fail-closed via
`DescriptorEvolved`) and adding no descriptor-management surface.
Descriptor refresh, invalidation, and negative caching are deferred to
future ADRs if evidence asks.

The migration is materialized in
`plans/impl/053_governed_bind_miss_descriptor_sync_consumer.md`.
