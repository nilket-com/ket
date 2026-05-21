# ADR 053 Evidence: Governed Bind-Miss Descriptor Sync Consumer

**ADR:** `crates/toron/plans/adr/053_governed_bind_miss_descriptor_sync_consumer.md`
**Implementation plan:** `crates/toron/plans/impl/053_governed_bind_miss_descriptor_sync_consumer.md`

## Implementation Notes

- `wendelon_client::Client` now exposes
  `get_table_schema_descriptor(tenant_id, namespace, table_name)`,
  delegating to the Wendelon ADR 072 inner client and mapping I/O
  failures through the existing `ClientError` surface.
- `TenantBudget` carries
  `max_concurrent_descriptor_syncs: NonZeroU32`. Config parsing accepts
  the optional field for default and per-tenant budgets, rejects explicit
  zero with the ADR 053 diagnostic, and derives `min(2,
  max_concurrent_queries)` when omitted.
- `TenantDescriptorSyncAccountant` / `TenantDescriptorSyncReservation`
  mirror ADR 052's tenant-keyed RAII admission shape for descriptor sync
  work. A sync reservation covers the Wendelon descriptor-get RPC plus
  the local insert-only merge only; it is released before query
  execution.
- `WendelonCatalog::resolve_table` remains the only live-sync trigger.
  On local registry miss it canonicalizes the scoped tenant + table name,
  joins a shared single-flight map, and has the owner perform one
  governed descriptor-get RPC. Waiters consume no descriptor-sync budget
  and re-resolve the local registry after the owner finishes. Waiters
  register their `Notify` future before checking the stored outcome, so
  owner completion cannot be missed between the outcome check and await.
- `WendelonDescriptorRegistry::sync_descriptor_for_tenant` performs an
  in-place insert-only merge under the registry write lock. It validates
  descriptor name identity, allocates a fresh local `TableId`, preserves
  Wendelon object id / generation / physical fingerprint, returns an
  existing same-identity binding, and rejects conflicting identities.
- `CatalogError::ResourceExhausted { message }` maps to the existing
  SQL `ResourceExhausted` code for descriptor-sync throttling. The stable
  diagnostic is `tenant descriptor sync limit exhausted`.
- `MetricsRegistry` renders aggregate-only descriptor-sync counters:
  `catalog_descriptor_sync_requests_total{outcome="success|not_found|throttled|error"}`.
- `Catalog::resolve_table_local` was added as a local-only identity
  lookup seam. `WendelonCatalog` overrides it to read only the shared
  registry, and the `DROP TABLE` storage-admin helper uses it so DDL
  execution remains outside the descriptor-sync trigger set.

## Preserved Boundaries

- Tenant identity is still sourced from ADR 049 scoped catalog context:
  authenticated client SAN for remote SQL and configured tenant fallback
  for loopback SQL.
- ADR 050 memory admission and ADR 052 query concurrency admission are
  unchanged; descriptor sync is a sibling governed resource, not a query
  admission rewrite.
- Startup descriptor bootstrap remains tenant-explicit ADR 051 input;
  runtime sync does not reuse `from_tenant_descriptors` and never swaps
  the registry `Arc`.
- No Wendelon server/router/opcode/authz change; ADR 053 only consumes
  the ADR 072 producer through the public client facade.
- No SQL wire change, no new SQL error code, and no coordinator
  bind-retry loop.
- No negative cache, refresh, TTL, inventory, watch/reload, background
  sync, table-id lookup sync, storage-path sync, or create/drop-triggered
  descriptor sync.
- `crates/nilket/Cargo.toml` remains runtime-free for Toron, and
  `crates/toron/src/bin/toron_fixture_worker.rs` remains source-unchanged.
- New ADR 053 surface is brand-clean and has no `*V1` suffix.

## Verification

Commands run:

```text
cargo fmt
cargo check -p toron --tests
cargo test -p toron catalog::tests:: -- --nocapture
cargo test -p toron descriptor_sync -- --nocapture
cargo test -p toron binds_wendelon_table_missing_from_startup_bootstrap -- --nocapture
cargo test -p toron --test architecture_test adr053 -- --nocapture
cargo test -p toron --test architecture_test -- --nocapture
cargo test -p toron --test sql_listener_test toron_sql_complete_frame_carries_stable_row_count -- --nocapture
cargo test -p toron
cargo test -p wendelon-client
cargo clippy -p toron --all-targets -- -D warnings
cargo clippy -p wendelon-client --all-targets -- -D warnings
```

Observed results:

- `cargo check -p toron --tests`: passed.
- `cargo test -p toron catalog::tests:: -- --nocapture`: passed the
  catalog unit suite while the descriptor-sync fixtures were being added.
- `cargo test -p toron descriptor_sync -- --nocapture`: passed targeted
  budget, config, catalog, architecture, and observability filters for
  descriptor sync.
- `cargo test -p toron binds_wendelon_table_missing_from_startup_bootstrap -- --nocapture`:
  passed both SQL bind-miss fixtures, covering `SELECT` and `INSERT`.
- `cargo test -p toron --test architecture_test adr053 -- --nocapture`:
  22 ADR 053 guards passed.
- Full `architecture_test`: 479 passed.
- Targeted SQL listener regression:
  `toron_sql_complete_frame_carries_stable_row_count` passed after the
  DROP path was corrected to use `resolve_table_local`.
- Full `cargo test -p toron`: passed all Toron suites. The existing
  Wendelon-backed storage execution suite reported 9 passed / 1 ignored.
- Full `cargo test -p wendelon-client`: 3 passed.
- `cargo clippy -p toron --all-targets -- -D warnings`: passed.
- `cargo clippy -p wendelon-client --all-targets -- -D warnings`:
  passed.

## Fixture Notes

- Config fixtures pin explicit descriptor-sync limits, zero rejection,
  and derived defaults from `max_concurrent_queries`.
- Budget fixtures pin tenant isolation and RAII release for
  `TenantDescriptorSyncAccountant`.
- Catalog fixtures pin local-hit no-RPC behavior, unqualified default
  namespace canonicalization, qualified exact namespace lookup,
  `Ok(None)` to `TableNotFound`, transport/auth/merge errors to
  `AdminFailed`, throttling to `ResourceExhausted`, tenant scoping,
  single-flight one-RPC behavior, waiter no-budget behavior,
  cancellation release, waiter completion on owner cancellation, local
  `TableId` allocation, physical
  fingerprint preservation, existing same-identity returns, and conflict
  rejection.
- SQL fixtures prove a table missing from ADR 051 startup bootstrap can
  be bound after Wendelon descriptor lookup for both reads and writes.
- Observability fixtures pin aggregate outcome-only descriptor-sync
  metrics with no tenant or table labels.
- Architecture guards pin the trigger set, client facade consumption,
  single-flight sharing across scoped catalogs, insert-only in-place
  merge, no `from_tenant_descriptors` runtime reuse, no background
  reload/watch/cache, unchanged Wendelon producer surface, and prior ADR
  invariants.
