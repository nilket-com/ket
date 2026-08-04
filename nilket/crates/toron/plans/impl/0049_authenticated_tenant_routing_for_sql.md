# Implementation Plan: ADR 049 — Authenticated Tenant Routing for SQL

**Status:** Promoted (paired with
`plans/adr/0049_authenticated_tenant_routing_for_sql.md`).

## Overview

Eight-phase migration. Toron-ring substrate cycle; no new crate,
no Wendelon production change. The execution-layer `tenant_id`
propagation already exists; this cycle adds the **tenant source**
(SQL client cert) and **tenant-scoped catalog resolution**.

```
Phase 0: Audit + peer-identity extraction spike
Phase 1: PeerKind::Client { principal, tenant_id } + admit_san numeric tenant
Phase 2: Accepted-connection tenant extraction in the SQL accept loop
Phase 3: Tenant-key the Toron WendelonDescriptorRegistry
Phase 4: Catalog::scoped(tenant_id) + coordinator scopes before bind + DDL
Phase 5: Thread admitted tenant into sql_execution_context
Phase 6: Architecture guards
Phase 7: Integration fixtures
Phase 8: Evidence + promotion
```

## Phase 0: Audit + peer-identity extraction spike

The one real implementation risk is exposing the admitted TLS
peer identity from the QUIC accept layer. Spike it first.

**Tasks:**

**0.1** Confirm `quinn::Connection::peer_identity()` returns the
rustls peer cert chain after handshake:

```rust
// in run_sql_accept_loop, after accept_connection() -> connection
let certs = connection
    .peer_identity()
    .and_then(|id| id.downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>().ok());
```

Verify (quinn 0.11) that `peer_identity()` is `Some` for an
mTLS SQL connection (client cert required) and that the downcast
target type matches quinn's rustls backend. Record the exact
type. **If the downcast target differs, the whole accept-loop
extraction contract must adapt — do this before any other
phase.**

**0.2** Confirm the extraction → admission chain:
`tls_pki::extract_dns_sans(leaf)` → `admit_san(san,
ListenerRole::Sql, cluster_domain)` → `PeerKind::Client`. The
handshake-time `SanClientCertVerifier` already ran (chain + SAN
shape); re-extraction derives the tenant route. Confirm the
"exactly one admitted tenant SAN" rule is enforceable (a cert
with two `client-N` SANs must be rejectable).

**0.3** Confirm the SQL accept path (`daemon.rs:542-640`):
`run_sql_accept_loop(... tenant_id ...)` → `accept_connection()`
→ `accept_stream_on` → `handle_sql_connection(coordinator,
stream, tenant_id, ..)` → `sql_execution_context(tenant_id,
query_id)`. The static `tenant_id` is the per-connection seam.

**0.4** Confirm the coordinator bind site:
`execute_query_with_context(&self, plan, context)` binds with
`self.catalog.as_ref()`; the catalog is a long-lived
`Arc<dyn Catalog>`. Single site that gains `.scoped(context.tenant_id)`.

**0.5** Confirm the Toron registry shape (`catalog.rs`):
`WendelonDescriptorRegistry { by_ref: HashMap<TableRef, TableId>,
by_id, descriptors, finalized_drops }`. Confirm the current local
`TableId` allocation assumptions, but key all descriptor-visible
maps by `(TenantId, _)` regardless; tenant isolation must not
depend on tenantless ids.

**0.6** Inventory ADR 043/044 SQL/TLS fixtures using
`client-<principal>` (non-numeric) SANs — to be updated to
numeric tenant SANs in Phase 7 (intentional supersession).

**Acceptance:** `peer_identity()` extraction confirmed on a real
mTLS SQL connection (throwaway spike); accept loop + coordinator
bind site + registry shape mapped; legacy SAN fixtures
inventoried.

## Phase 1: PeerKind::Client gains tenant_id + admit_san numeric tenant

**Tasks:**

**1.1** Extend `PeerKind::Client { principal: String, tenant_id:
TenantId }` (`network/tls.rs`); `principal` is diagnostic-only
(the canonical tenant token), never used for routing.

**1.2** In `admit_san` (or a SQL-role post-check) for the
`client-` prefix on `ListenerRole::Sql`: parse the token as a
**canonical decimal `u64`** → `TenantId`. Reject
(`TlsError::SanShapeRejected`): non-numeric, `0`, leading zeros,
signed, hex, empty.

**1.3** Keep `monitor-` and `toron-` admission unchanged; daemon
+ monitor identities remain rejected for SQL.

**Acceptance:** unit tests — `admit_san("client-7.cluster.local",
Sql, ..)` → `Client { tenant_id: TenantId(7), .. }`;
`client-007` / `client-0` / `client-analytics` / wildcard /
underscore / subdomain → rejected (fixtures 3-6).

## Phase 2: Accepted-connection tenant extraction in the SQL accept loop

**Tasks:**

**2.1** Add an accept-boundary helper (brand-clean, e.g.
`accepted_sql_tenant(&connection, cluster_domain) ->
Result<TenantId, _>`) running the Phase-0 chain
(`peer_identity()` → downcast → `extract_dns_sans` →
`admit_san(Sql)` → `PeerKind::Client { tenant_id }`), requiring
**exactly one** admitted client tenant SAN (reject zero or
multiple as ambiguous).

**2.2** In `run_sql_accept_loop`: after `accept_connection()`,
for the authenticated remote SQL listener derive the
per-connection tenant via 2.1; on failure drop the connection
(no SQL frame read, no response written) + increment a
bounded-reason reject metric (no tenant label). For loopback
unauthenticated, use `[node].tenant_id` (D5).

**2.3** Thread the per-connection `TenantId` into
`handle_sql_connection` in place of the static loop `tenant_id`
(which becomes the loopback fallback only).

**Acceptance:** fixtures 1, 2, 7, 8 — per-connection tenant
observed; ambiguous multi-SAN rejected; loopback fallback.

## Phase 3: Tenant-key the Toron WendelonDescriptorRegistry

**Tasks:**

**3.1** `by_ref` → `HashMap<(TenantId, TableRef), TableId>`; key
`by_id` / `descriptors` / `finalized_drops` by `(TenantId,
TableId)` (or full `DropOperationId`).

**3.2** Update registry methods (`resolve_table`,
`resolve_table_id`, insert/create, drop-finalize) to take/thread
a `TenantId`. Storage-path descriptor lookups use the request
tenant (D10).

**3.3** Config-bootstrap descriptor insertion uses
`[node].tenant_id` (D11).

**Acceptance:** fixtures 9, 10, 14 — same name resolves
per-tenant; cross-tenant resolution fails; storage descriptor
lookup uses request tenant.

## Phase 4: Catalog::scoped + coordinator scopes before bind + DDL

**Tasks:**

**4.1** Add `fn scoped(self: Arc<Self>, tenant_id: TenantId) ->
Arc<dyn Catalog>;` to the `Catalog` trait (object-safe;
`Arc<Self>` receiver).

**4.2** `WendelonCatalog::scoped` → `Arc::new(WendelonCatalog {
client: self.client.clone(), tenant_id, registry:
self.registry.clone() })` (clone with swapped tenant over the
shared tenant-keyed registry; no separate wrapper type).
`MockCatalog::scoped` → tenant-bound view sharing in-memory
state. Implementation-owned views, not a generic decorator (D7).

**4.3** `WendelonCatalog` resolve/create/drop use `self.tenant_id`
for registry + Wendelon admin requests (D10): `create_table`
keeps `TableSchemaUpdateRequest::create(self.tenant_id, ..)`;
`drop_table` keeps `DropOperationId` with `self.tenant_id` — now
the scoped tenant.

**4.4** `DistributedQueryCoordinator::execute_query_with_context`:
`let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
let bound = bind(plan, catalog.as_ref(), &self.config).await?;`
Planner/binder stay tenant-unaware.

**Acceptance:** fixtures 11, 12, 13 — DDL under admitted tenant;
binding through scoped catalog.

## Phase 5: Thread admitted tenant into sql_execution_context

**Tasks:**

**5.1** `handle_sql_connection` builds
`sql_execution_context(admitted_tenant, frame.query_id)` from
the per-connection tenant (Phase 2), not the static parameter.

**5.2** Confirm `TaskExecutionContext.tenant_id` + storage
request `tenant_id` propagation unchanged (already carries the
context tenant) — fixture 15 verifies; no code change expected.

**Acceptance:** fixture 15 — task dispatch preserves the
admitted tenant.

## Phase 6: Architecture guards

Add the 18 guards + 3 convention guards (ADR §Architecture
Guards) to `crates/toron/tests/architecture_test.rs`.

**Notes:**
- Guard 3 (`remote_sql_never_falls_back_to_config_tenant`):
  assert the authenticated accept path does not use the loopback
  fallback tenant; the static loop `tenant_id` is used only on
  the loopback branch.
- Guard 7 (`quic_peer_identity_extracted_before_sql_frame_read`):
  assert the accept loop calls the extraction helper before
  `accept_stream_on` / `read_sql_frame`.
- Guard 10 (`descriptor_registry_keys_refs_and_descriptors_by_tenant`):
  assert `by_ref` keyed by a `(TenantId, _)` tuple.
- Guard 12 (`storage_descriptor_lookup_uses_request_tenant`):
  assert storage scan/write/delete/drop descriptor lookups pass
  the request tenant.
- Guard 18 (meta): assert ADR 040-048 guard fn names persist +
  sweep passes (ADR 047/048 convention).
- Convention guards: nilket runtime-free; `toron_fixture_worker.rs`
  source-unchanged; brand-clean/no-V1.

**Acceptance:** 18 + 3 pass; ADR 040-048 guards still pass.

## Phase 7: Integration fixtures

Implement the 15 required (+ 2 V1-evidence optional) fixtures.
Mix: `admit_san` unit tests (Phase 1), accept-loop extraction
fixtures (deterministic; constructed connection or spike
harness), registry/catalog scoping unit tests (Phases 3-4),
optional real mTLS round-trip.

**Notes:**
- Fixtures 1-7 (SAN admission + ambiguity) largely Phase-1/2
  unit tests.
- Fixtures 9-14 (registry + catalog scoping + DDL + storage
  lookup) seed `(TenantId, TableRef)` entries directly (D11 — no
  descriptor discovery needed).
- Fixture 15 + optional round-trip exercise the
  `peer_identity()` path end-to-end.
- **Legacy SAN supersession:** ADR 043/044 SQL fixtures using
  non-numeric `client-<principal>` update to numeric tenant SANs
  where ADR 049 supersedes them (Phase 0.6 inventory). Document
  which prior guard/fixture names change in the evidence record.

**Acceptance:** `cargo test -p toron` passes the required
executing fixtures; fixture 11's live Wendelon admin round-trip
is documented as an accepted code-inspection / architecture-guard
coverage substitution while the simulated fixture remains unable
to expose authenticated schema-update admin operations. Optional
round-trip passes or is documented deferred.

## Phase 8: Evidence + promotion

**Tasks:**

**8.1** Commit atomically.
**8.2** Verify promoted docs exist (this file +
`plans/adr/0049_authenticated_tenant_routing_for_sql.md`).
**8.3** Verify `crates/toron/plans/wip/roadmap.md`: Status bullet
for ADR 049; Forward sequence new `### ADR 049 promoted; ADR 050
candidates`; demote `### ADR 048 promoted; ADR 049 candidates` to
`(HISTORICAL)`.
**8.4** Evidence record (commands + pass counts + the legacy-SAN
supersession note).

**Acceptance:** ADR + impl + roadmap promoted; evidence captured.

## Risk register

| Risk | Mitigation |
|------|-----------|
| `peer_identity()` type/availability differs from assumption | Phase 0.1 spike confirms downcast target + Some-ness on a real mTLS SQL connection before any other phase |
| Legacy `client-<principal>` SQL fixtures break | Phase 0.6 inventory + Phase 7 intentional supersession to numeric SANs, documented in evidence |
| Tenantless `TableId` lookup leaks across tenants | Phase 0.5 confirms local allocation assumptions, but Phase 3 keys descriptor-visible maps by `(TenantId, _)` regardless |
| Loopback fallback leaks into authenticated path | Guard 3 asserts the fallback tenant is used only on the loopback branch; fixtures 1-2 + 8 separate the paths |
| Multi-SAN cert ambiguity | D4 / Phase 2.1 require exactly one admitted tenant SAN; fixture 7 |
| `scoped()` clone cost per query | `WendelonCatalog::scoped` clones `Arc`s + `TenantId` (cheap); registry shared via `Arc` |
| Coordinator owns one catalog | D8/Phase 4.4 use `scoped()` at the bind site (single localized change), not per-connection catalog plumbing |
| Worker task tenant divergence | Phase 5.2: worker inherits via existing `TaskExecutionContext.tenant_id`; no worker-admission change |

## Phase totals

- 8 phases.
- 0 new crates; 0 Wendelon production changes; 0 `crates/nilket`
  changes; `toron_fixture_worker.rs` unchanged.
- Touch points: `PeerKind::Client` + `admit_san`
  (network/tls.rs); accept-loop extraction + `sql_execution_context`
  source (daemon.rs); registry tenant-keying + `Catalog::scoped`
  + `WendelonCatalog::scoped` (catalog.rs); coordinator bind-site
  (1 line, coordinator).
- 18 guards + 3 convention; 15 required fixture targets + 2
  optional, with fixture 11 covered by accepted code-inspection
  evidence rather than an executing live admin fixture.

## Acceptance checklist

- [ ] Phase 0 spike confirms `peer_identity()` extraction.
- [ ] Phase 1 `PeerKind::Client { tenant_id }` + numeric admit_san.
- [ ] Phase 2 per-connection extraction; exactly-one-SAN; loopback
      fallback.
- [ ] Phase 3 tenant-keyed registry + storage request-tenant
      lookup.
- [ ] Phase 4 `Catalog::scoped` + coordinator scopes before bind
      + DDL under scoped tenant.
- [ ] Phase 5 admitted tenant into `sql_execution_context`.
- [ ] Phase 6 18 + 3 guards pass; ADR 040-048 guards pass.
- [ ] Phase 7 required fixtures pass where executable; fixture 11
      code-inspection substitution and legacy SAN supersession
      documented.
- [ ] Phase 8 ADR + impl + roadmap promoted; evidence.

Ready for implementation.
