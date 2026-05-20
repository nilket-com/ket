# Impl Plan 053: Governed Bind-Miss Descriptor Sync Consumer

**Status:** Promoted, paired with
`plans/adr/053_governed_bind_miss_descriptor_sync_consumer.md`.
**Posture:** Single plan, Toron-ring only. Consumes the Wendelon 072
producer. No Wendelon server change, no SQL wire change. Six phases,
dependency-ordered.

## Invariants carried in (must still hold at the end)

- `crates/nilket` stays runtime-free; `toron_fixture_worker.rs`
  source unchanged; brand-clean / no-`*V1`.
- ADR 049 tenant source, 050 memory budget, 051 startup bootstrap, 052
  query concurrency unchanged; their guards keep passing.
- Wendelon 072 server/router/opcode/authz unchanged — the only
  Wendelon-side edit is public client-facade plumbing.
- The merge is insert-only and fail-closed via the unchanged
  `DescriptorEvolved` path.

## Phase 0 — Audit (verified at promotion)

- `WendelonCatalog::resolve_table` is `async`, holds `client:
  Option<Arc<Client>>`, scoped `tenant_id`, `registry:
  Arc<WendelonDescriptorRegistry>`; `scoped()` rebuilds the struct
  (catalog.rs).
- `WendelonDescriptorRegistry`: `register_descriptor_for_tenant`,
  `next_table_id` + `bump_next_wendelon_table_id`,
  `insert_wendelon_descriptor_locked`, `TableBinding::from_descriptor`;
  `resolve_table` returns
  `CatalogError::TableNotFound` on miss.
- `TableRef { schema: Option<String>, table }` (catalog.rs) →
  canonical `(namespace, table_name)`; `schema=None` → `"default"`.
- `CatalogError` (error.rs:119) — has `AdminFailed`/`TableNotFound`;
  **no `ResourceExhausted` yet**.
- `daemon.rs::map_sql_query_error` maps
  `QueryError::Binding(BindingError::Catalog(_))` and
  `QueryError::Catalog(_)` → `SqlErrorCode::CatalogError`
  (daemon.rs:771/773).
- ADR 052 budget seam to mirror: `distributed/budget.rs` `TenantBudget`
  (`max_concurrent_queries: NonZeroU32`), `TenantConcurrencyAccountant`
  / `TenantConcurrencyReservation` (`Arc<Mutex<HashMap<TenantId,u32>>>`,
  RAII `Drop`), `derive_max_concurrent_queries`; `config.rs`
  `RawTenantBudget` + `normalize_tenant_budget` (zero-reject + derive);
  Wendelon 072 inner descriptor-get in `crates/wendelon/src/client/core.rs`.

## Phase 1 — Public client facade (`crates/wendelon/crates/wendelon-client/src/lib.rs`)

Expose the 072 inner method from `crates/wendelon/src/client/core.rs`
on the public `wendelon_client::Client` wrapper Toron holds (D5):
`get_table_schema_descriptor(tenant_id, namespace, table_name) ->
Result<Option<TableSchemaDescriptor>, ClientError>`. No Toron-specific
semantics (inner already does leader-hint retry + `NOT_FOUND → Ok(None)`).
Facade tests: `Some`, `Ok(None)`, leader-hint pass-through, transport/auth
`Err`.

## Phase 2 — Config + sync budget (`distributed/budget.rs`, `config.rs`, `error.rs`)

- `TenantBudget` gains `max_concurrent_descriptor_syncs: NonZeroU32`
  (update constructors as 052 did). Add
  `derive_max_concurrent_descriptor_syncs(mcq) = min(2, mcq).max(1)`;
  `from_query_budget` → 1.
- `RawTenantBudget`/`RawTenantBudgetEntry` gain
  `max_concurrent_descriptor_syncs: Option<u32>`;
  `normalize_tenant_budget` rejects `Some(0)` with
  `"tenant_budgets max_concurrent_descriptor_syncs must be greater than
  0"`, else derives. Update the example-config comment.
- `TenantDescriptorSyncAccountant` / `TenantDescriptorSyncReservation`
  / `TenantDescriptorSyncRejection`, near-copies of the 052 concurrency
  trio (RAII `Drop`, idempotent release).
- **error.rs:** add `CatalogError::ResourceExhausted { message }` (D10).

## Phase 3 — Single-flight coordinator (owned by `WendelonCatalog`)

A helper type constructed once at daemon build and shared through
`scoped()` (alongside `client`/`registry`): holds the
`TenantDescriptorSyncAccountant` + an in-flight map keyed by canonical
`(TenantId, String, String)`. Owner acquires one reservation + runs one
RPC; waiters await the shared completion then re-resolve locally
(consume no budget, no RPC). **Cancellation-safe:** owner-drop releases
the reservation and clears the flight entry. `WendelonCatalog` gains an
`Option<Arc<...>>` field, threaded through `scoped()`.

## Phase 4 — Insert-only merge helper (`catalog.rs`)

`WendelonDescriptorRegistry::sync_descriptor_for_tenant(&self,
tenant_id, requested: &TableRef, descriptor) -> Result<TableBinding,
CatalogError>` (D9): validate namespace/table vs canonical request
(mismatch → `AdminFailed`); under the write lock — if the qualified
`(tenant, TableRef)` is present with the same object id, schema
generation, physical fingerprint, and schema, return that binding; with
a different identity return `AdminFailed`; else allocate fresh `TableId`
from `next_table_id` (+ `bump_next_wendelon_table_id`), build
`TableBinding::from_descriptor` (preserve object-id / generation /
fingerprint), insert qualified binding + descriptor-by-id + unqualified
alias, return the binding. Never `from_tenant_descriptors`, never `Arc`
swap, never overwrite, never a Wendelon SchemaUpdate.

## Phase 5 — `resolve_table` integration + SQL mapping (`catalog.rs`, `daemon.rs`)

`resolve_table`: local lookup → on `Ok` return; on miss, if no
flights/client → `TableNotFound`; else canonicalize, join-or-own the
flight (owner: `try_reserve` → on full `CatalogError::ResourceExhausted
{ "tenant descriptor sync limit exhausted" }`; `get_table_schema_descriptor`;
`Ok(None)`→`TableNotFound`; `Ok(Some)`→`sync_descriptor_for_tenant`→
return binding; `Err`→`AdminFailed` "descriptor sync failed"); waiter:
await flight, re-resolve once. No coordinator change.

`daemon.rs::map_sql_query_error`: add an arm matching
`QueryError::Binding(BindingError::Catalog(CatalogError::ResourceExhausted{..}))`
and `QueryError::Catalog(CatalogError::ResourceExhausted{..})` →
`SqlErrorCode::ResourceExhausted` + `"tenant descriptor sync limit
exhausted"`, **before** the generic `Catalog(_)` arms (daemon.rs:771/773);
extend the unit tests.

## Phase 6 — Guards, fixtures, evidence, roadmap

- 19 architecture + 3 convention guards (ADR list) in
  `tests/architecture_test.rs`.
- 29 required + 1 optional fixtures (ADR list).
- Evidence:
  `plans/impl/053_governed_bind_miss_descriptor_sync_consumer_evidence.md`
  (targeted facade/budget/catalog tests, `architecture_test adr053`,
  full `architecture_test`, `cargo test -p toron`, `cargo clippy -p
  toron --all-targets -- -D warnings`).
- Roadmap: ADR 053 status (consumer of the 072 producer); correct any
  lingering "`from_tenant_descriptors` is the live-sync seam" wording to
  point at `sync_descriptor_for_tenant`; Governance Phase 3 → ADR 054
  candidate.

## Watch-items

1. Sync reservation covers **RPC + merge only** — released before query
   execution (unlike the 052 query reservation that spans execution).
2. `scoped()` must thread the shared flights/accountant `Arc` — else
   single-flight + budget are per-clone and inert.
3. Never hold the registry write lock across the Wendelon RPC await —
   the single-flight serializes misses; the lock is taken only inside
   `sync_descriptor_for_tenant` after the fetch returns.
4. Local `TableId` allocation is collision-free vs ADR 051 bootstrap
   ids because `bump_next_wendelon_table_id` always advances
   `next_table_id`.

## Acceptance checklist

- [ ] Phase 1 client facade + tests.
- [ ] Phase 2 config + `max_concurrent_descriptor_syncs` + sync
      accountant + `CatalogError::ResourceExhausted`.
- [ ] Phase 3 single-flight shared via `scoped()`, owner-only
      reservation, cancellation-clears-flight.
- [ ] Phase 4 insert-only `sync_descriptor_for_tenant` returning the
      binding.
- [ ] Phase 5 `resolve_table` integration + daemon ResourceExhausted
      arm; no coordinator change.
- [ ] Phase 6 19+3 guards, 29 fixtures, prior ADR guards pass, evidence
      + roadmap.
