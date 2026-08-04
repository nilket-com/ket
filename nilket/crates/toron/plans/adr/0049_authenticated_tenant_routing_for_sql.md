# ADR 049: Authenticated Tenant Routing for SQL

**Status:** Promoted
**Seed:** ADR 048 promotion §"ADR 049 candidates" (multi-tenant
routing, LEAD); the Toron ADR 049 slot reclaimed after
`nilket wendelon inspect` was promoted in the Wendelon sequence
as Wendelon ADR 071 (ring-product-placement convention).
**Council:** Claude, Codex, Gemini (four iterations: iter-0 3-of-3
convergence on multi-tenant SQL routing as the reclaimed Toron
slot; iter-1 a live-code audit resolved the load-bearing
question — Wendelon's durable catalog already enforces tenant
isolation (`TableLifecycleKey{tenant_id, table_object_id}` +
`tenant_scoped` identity hashing), so this delivers real
isolation, not just attribution, and no paired Wendelon ADR is
needed (Claude R1/R2 retractions); iter-2 dual ADR+impl
converged on `Catalog::scoped`, tenant-keyed Toron registry,
Quinn `peer_identity()` extraction, strict numeric SQL client
SANs, loopback fallback, no SQL wire change; iter-3 ratified the
`scoped()` factory as implementation-owned tenant views (not a
generic decorator; Codex+Claude 2-of-3 over Gemini's
`ScopedCatalog` decorator — echoing the ADR 047 D8
caller-owned-policy lesson), added exactly-one-tenant-SAN
ambiguity rejection, and a storage-path request-tenant
descriptor-lookup guard). Promoted from iter-3 with Codex+Claude
2-of-3 convergence.
**Category:** Toron compute-ring substrate ADR — multi-tenant
SQL admission + tenant-scoped catalog binding. Exterior Substrate
→ Toron home (ring-product-placement convention); the Leap Rule's
interior "How." Zero Wendelon production change.

## Summary

Make authenticated remote SQL **tenant-scoped by mTLS
identity.** The tenant is derived from the SQL client
certificate's DNS SAN (`client-<tenant_id>.<cluster_domain>`),
threaded through the already-existing `ExecutionContext.tenant_id`
substrate, and used to scope catalog resolution before binding.
Toron's tenant-flat local descriptor registry becomes
tenant-keyed. Wendelon's durable, already-tenant-keyed
schema/storage substrate is consumed unchanged.

This delivers **real SQL catalog isolation** (two tenants on one
daemon cannot see or mutate each other's tables through a reused
unqualified name), not merely tenant attribution — Wendelon
enforces tenant isolation at the storage layer, and ADR 049
closes the Toron-side admission + catalog gaps above it.

**No** SQL wire change, **no** new `SqlMessage` variants /
`SqlErrorCode` values, **no** `--tenant` flag, **no**
token/JWT/OIDC/SPIFFE model,
**no** per-tenant metric labels, **no** new shared crate, **no**
Wendelon production change, **no** worker-task admission change
(workers inherit the tenant via the existing
`TaskExecutionContext.tenant_id` task-frame contract).

## Motivation

Toron has the lower execution tenant substrate but not the
admission/catalog routing for a multi-tenant SQL gateway:

- `ExecutionContext`, `TaskExecutionContext`, and storage
  requests already carry `tenant_id`.
- The SQL accept loop passes the daemon's static `[node].tenant_id`
  to every connection (`daemon.rs:548,627,700-702`).
- `PeerKind::Client` carries only `principal`
  (`network/tls.rs:79`) — a free-form client identity, not a
  tenant route.
- The Toron `WendelonDescriptorRegistry` resolves table names
  through tenant-flat maps (`catalog.rs:390`,
  `by_ref: HashMap<TableRef, TableId>`), so two tenants'
  same-named tables would collide inside Toron.
- Wendelon's durable schema catalog, lifecycle, row identity,
  and storage paths are already tenant-keyed
  (`schema/lifecycle.rs:144` `TableLifecycleKey::new(tenant_id,
  table_object_id)`; `hash/identity.rs::tenant_scoped`), so no
  Wendelon production change is needed.

ADR 049 closes the Toron gap: an authenticated tenant source
plus tenant-scoped local catalog binding. ADR 071 is the
placement precedent running the other way (product-local
Wendelon behavior → Wendelon sequence); this is Toron
compute-ring substrate → Toron sequence.

## Decisions (12, Promotion-Locked)

**D1: Placement — Toron sequence.**

The changed behavior is SQL admission, Toron catalog
binding, Toron descriptor caching, and Toron execution-context
construction. Exterior Substrate → Toron home
(ring-product-placement convention). Wendelon owns the durable
tenant-keyed substrate consumed here, unchanged.

**D2: Tenant routing source — mTLS client SAN.**

Authenticated non-loopback SQL derives its tenant from the
client certificate DNS SAN:

```text
client-<tenant_id>.<cluster_domain>
```

Rules:
- `<tenant_id>` is a **canonical base-10 `u64`** mapping to
  `wendelon::schema::TenantId`.
- Client-facing tenant ids start at `1`.
- `client-0.<cluster_domain>` rejected as reserved (D9).
- Leading zeros rejected (`client-007` rejected).
- Empty, signed, hex, underscored, wildcard, subdomain-injected,
  and non-numeric forms rejected.
- Daemon (`toron-`) and monitor (`monitor-`) identities remain
  rejected for SQL.

This intentionally supersedes ADR 044's free-form SQL
`client-<principal>` acceptance: SQL clients now carry a tenant
route.

**D3: `PeerKind::Client` carries `tenant_id`.**

```rust
pub enum PeerKind {
    SiblingDaemon { worker_id: WorkerId },
    Client { principal: String, tenant_id: TenantId },
    Monitor { principal: String },
}
```

`admit_san` parses the numeric tenant token for the `client-`
prefix on `ListenerRole::Sql`; a `client-` SAN whose token is
not a canonical numeric tenant is rejected
(`TlsError::SanShapeRejected`). `principal` is retained as
diagnostic/audit text only (exactly the canonical tenant token);
it is never used for routing.

**D4: Accepted-connection tenant extraction via Quinn
`peer_identity()`.**

Remote authenticated SQL derives the tenant **after QUIC/TLS
handshake and before accepting a SQL stream** — no rustls
verifier side channel:

- `quinn::Connection::peer_identity()` → downcast to
  `Vec<rustls::pki_types::CertificateDer<'static>>`.
- Inspect the end-entity certificate;
  `tls_pki::extract_dns_sans(leaf)` → DNS SANs.
- Admit via Toron's `ListenerRole::Sql` policy (`admit_san`).
- **Require exactly one admitted client tenant SAN.**

Missing peer identity, downcast failure, no valid SQL client
tenant SAN, **or multiple valid SQL client tenant SANs**
(ambiguous) reject the connection before any SQL frame is read —
no SQL request read, no SQL response frame written. The
handshake-time verifier (`SanClientCertVerifier`) still enforces
chain trust + admissible SAN shape; the accept-loop extraction
obtains the tenant route.

**D5: Loopback fallback; strict remote.**

Loopback-only unauthenticated SQL keeps `[node].tenant_id`.
Authenticated non-loopback SQL **must never** fall back to the
daemon-configured tenant.

**D6: No SQL wire tenant claim.**

No new `SqlMessage` variant, request field,
`nilket toron sql --tenant` flag, token auth, JWT, OIDC, or
SPIFFE URI SAN model. The certificate is the tenant claim for
authenticated remote SQL.

**D7: Scoped catalog factory — implementation-owned views.**

`Catalog` gains an object-safe scoped factory:

```rust
#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog>;
    // existing async resolve/create/drop methods unchanged
}
```

This is **not** a generic default decorator over tenantless
methods. Each implementation returns a tenant-aware view:
- `WendelonCatalog::scoped` returns a new `WendelonCatalog`
  bound to the requested `tenant_id`, sharing the same client +
  registry (the `tenant_id` field already exists; the scoped
  view is a clone with a swapped tenant — no separate
  `ScopedCatalog` wrapper type).
- `MockCatalog::scoped` returns a tenant-bound view sharing the
  same in-memory state.

The `Arc<Self>` receiver lets `DistributedQueryCoordinator` keep
its existing long-lived `Arc<dyn Catalog>` and scope it at
execution time.

*(Rejected 2-of-3 Codex+Claude: Gemini's generic `ScopedCatalog`
decorator delegating over tenantless methods — the same
caller-owned-vs-wrapper trade-off settled in ADR 047 D8;
implementation-owned views are lower-surface here because
`WendelonCatalog` already carries `tenant_id`.)*

**D8: Bind through the scoped catalog.**

`DistributedQueryCoordinator::execute_query_with_context` scopes
the catalog from `context.tenant_id` before binding:

```rust
let catalog = Arc::clone(&self.catalog).scoped(context.tenant_id);
let bound = bind(plan, catalog.as_ref(), &self.config).await?;
```

The same scoped catalog serves DDL and mutation paths that
resolve table names. Planner and binder remain tenant-unaware.
**Invariant:** binding and DDL never resolve unqualified table
names through a tenant-agnostic registry.

**D9: Tenant-keyed Toron descriptor registry; `TenantId(0)`
reserved.**

`WendelonDescriptorRegistry` becomes tenant-keyed where tenant
affects visibility or lifecycle:
- table ref lookup: `(TenantId, TableRef) -> TableId`
- table id lookup: `(TenantId, TableId) -> TableBinding`
  (lookup is tenant-qualified even if local `TableId` allocation
  remains globally unique)
- descriptor lookup: `(TenantId, TableId) -> TableSchemaDescriptor`
- finalized drop markers keyed by full tenant-bearing identity.

Two tenants may use the same schema/table name on one Toron
daemon. `TenantId(0)` is reserved (Wendelon internals +
`TenantId::ZERO`); client-facing SQL tenant ids start at `1`.

**D10: DDL and storage descriptor lookups use the scoped/request
tenant.**

`CREATE TABLE` / `DROP TABLE` use the scoped catalog tenant for
Wendelon admin requests and local registry updates
(`WendelonCatalog::create_table` already uses
`TableSchemaUpdateRequest::create(self.tenant_id, ..)`;
`drop_table` builds `DropOperationId` with `self.tenant_id` —
both now sourced from the scoped view). Storage
scan/write/delete/drop paths already receive request tenant ids;
their descriptor-registry lookups must use the **request
tenant**, not a tenant-flat table id.

**D11: Bootstrap policy.**

Config-loaded descriptors are assigned `[node].tenant_id`
(bootstrap policy only). Tests and future descriptor sync may
use tenant-explicit registry constructors; live multi-tenant
descriptor *discovery* is outside this ADR.

**D12: Preserved surfaces; no Wendelon change; brand-clean.**

No new SQL wire messages or error codes; no per-tenant metric
labels (ADR 045 low-cardinality preserved; reject counters use
bounded reason labels, never tenant values); no new workspace
crate; no Wendelon production changes; `crates/nilket/Cargo.toml`
runtime-free; `crates/toron/src/bin/toron_fixture_worker.rs`
source unchanged; brand-clean names, no V1 suffix
(`ClientIdentity`, `TenantRoute`, `AcceptedSqlConnection`; not
`ToronTenantRoute` / `*V1`).

**Single-plan posture.** Toron-ring only; no paired Wendelon ADR
(Wendelon isolation already enforced). Matches ADR 042-048.

## Architecture Guards (18, Promotion-Locked)

1. `adr049_sql_tenant_source_is_authenticated_peer_identity`
2. `adr049_loopback_sql_retains_config_tenant_fallback`
3. `adr049_remote_sql_never_falls_back_to_config_tenant`
4. `adr049_sql_client_san_requires_canonical_numeric_tenant`
5. `adr049_sql_client_san_rejects_non_numeric_principal_for_tenant`
6. `adr049_sql_listener_still_rejects_sibling_daemon_identity`
7. `adr049_quic_peer_identity_extracted_before_sql_frame_read`
8. `adr049_peer_kind_client_carries_tenant_id`
9. `adr049_coordinator_scopes_catalog_before_binding`
10. `adr049_descriptor_registry_keys_refs_and_descriptors_by_tenant`
11. `adr049_create_and_drop_table_use_scoped_tenant`
12. `adr049_storage_descriptor_lookup_uses_request_tenant`
13. `adr049_task_context_and_storage_requests_preserve_tenant`
14. `adr049_no_sql_wire_tenant_claim_or_message_variant`
15. `adr049_no_new_sql_error_codes`
16. `adr049_no_per_tenant_metric_labels`
17. `adr049_no_wendelon_production_changes`
18. `adr049_prior_adr04x_guards_continue_to_pass` (meta-guard;
    ADR 040-048 guard names persist + sweep passes, per the
    ADR 047/048 convention)

**Post-promotion convention guards:**
- `adr049_nilket_root_remains_runtime_free`
- `adr049_fixture_worker_source_unchanged`
- `adr049_new_types_are_brand_clean_and_no_v1_suffix`

## Integration Fixtures (15 required + 2 V1-evidence optional)

**Required (gates promotion):**

1. `sql_client_tenant_7_observed_in_execution_context`
2. `sql_client_tenant_8_observed_proves_not_daemon_static`
3. `sql_client_non_numeric_principal_rejected_before_frame`
4. `sql_client_leading_zero_tenant_rejected`
5. `sql_client_zero_tenant_rejected_as_reserved`
6. `sql_client_attack_class_sans_remain_rejected` (daemon,
   monitor, wildcard, underscore, subdomain-injection,
   unknown-CA)
7. `sql_client_multiple_valid_tenant_sans_rejected_as_ambiguous`
8. `loopback_sql_without_tls_uses_config_tenant`
9. `same_table_name_resolves_per_tenant`
10. `tenant_8_cannot_resolve_tenant_7_only_table`
11. `create_table_submits_wendelon_request_for_admitted_tenant`
12. `drop_table_consumes_admitted_tenant_drop_marker`
13. `binding_uses_scoped_catalog_before_distributed_execution`
14. `storage_descriptor_lookup_uses_request_tenant`
15. `task_dispatch_preserves_admitted_tenant_in_task_context`

Promotion note: fixtures 1-10 and 12-15 execute as tests.
Fixture 11 is accepted by code-inspection / architecture-guard
coverage of `WendelonCatalog::create_table` using
`wendelon_tenant_id(self.tenant_id)` plus the ignored live fixture
`wendelon_catalog_create_table_submits_schema_update_request`;
the live fixture remains ignored because the simulated Wendelon
fixture does not yet expose authenticated schema-update admin
operations. This is an explicitly accepted coverage substitution,
not a silently green executing fixture.

**Optional V1-evidence (non-gating):**

- `real_mtls_sql_round_trip_tenant_7_and_8` — full mTLS SQL
  round trip for two tenants on one daemon.
- `prior_adr043_044_sql_tls_fixtures_pass_with_numeric_san` —
  ADR 043/044 SQL/TLS fixtures pass; legacy `client-<principal>`
  SQL fixtures updated to numeric tenant SANs where ADR 049
  supersedes them.

## Sub-Lift Posture

**Single-plan.** Toron-ring only; no paired Wendelon ADR
(Wendelon isolation already enforced). The one implementation
risk — exposing the admitted peer identity from the QUIC accept
layer — is grounded on Quinn `Connection::peer_identity()` and
spiked in Phase 0 of the impl plan.

## Out of Scope

- Per-tenant quotas / rate limits / memory or storage pools /
  resource governance.
- Per-tenant observability labels.
- Tenant provisioning / lifecycle / cert issuance.
- Token / JWT / OIDC / SPIFFE URI SAN admission.
- SQL wire tenant fields / new `SqlMessage` variants /
  `SqlErrorCode` values.
- Live Wendelon → Toron descriptor sync / cross-tenant
  descriptor discovery.
- Worker-task listener admission changes (workers inherit the
  tenant via the existing `TaskExecutionContext.tenant_id`).
- Any Wendelon production change.

## Council History

- **Iter-0 (3-of-3 topic):** Codex (mTLS tenant routing for
  SQL), Claude (per-connection identity + cross-ring framing),
  Gemini (scoped-catalog decorator + strict tenancy). Reclaimed
  Toron slot after Wendelon inspect → Wendelon ADR 071.
- **Iter-1 (Codex+Claude full convergence):** live-code audit
  resolved that Wendelon already enforces tenant isolation;
  Claude retracted the "attribution-not-isolation" hedge (R1)
  and the cross-ring decomposition / paired-Wendelon-ADR
  hypothesis (R2); both identified the tenant-flat Toron
  registry; Quinn `peer_identity()` as the extraction path.
- **Iter-2 (dual; Codex+Claude convergent):** `Catalog::scoped`,
  tenant-keyed Toron registry, `peer_identity()` extraction,
  strict numeric SQL client SANs, loopback fallback, no SQL wire
  change. Claude revised the iter-1 catalog-mechanism lean
  (per-connection construction → `scoped()` method) given the
  coordinator's long-lived `Arc<dyn Catalog>` ownership.
- **Iter-3 (Codex+Claude 2-of-3, promoted):** `scoped()` as
  implementation-owned tenant views, not a generic decorator
  (over Gemini's `ScopedCatalog`); added exactly-one-tenant-SAN
  ambiguity rejection (D4 / fixture 7) and the storage-path
  request-tenant descriptor-lookup guard (D10 / guard 12 /
  fixture 14).

**Rejected (2-of-3 Codex+Claude):** Gemini's generic
`ScopedCatalog` decorator delegating over tenantless methods
(implementation-owned `scoped()` views are lower-surface;
mirrors the ADR 047 D8 caller-owned-policy resolution).

## Promotion criteria met

1. D-set locked at 12 (Codex+Claude convergent; Gemini-aligned
   except the scoped-catalog mechanism, resolved 2-of-3).
2. Guard set 18 + 3 convention.
3. Fixture set 15 required + 2 V1-evidence, with fixture 11
   covered by accepted code-inspection / architecture-guard
   evidence because the live Wendelon admin fixture remains
   ignored for fixture-surface reasons.
4. Single-plan, Toron-ring; no Wendelon production change.
5. Placement: Exterior Substrate → Toron home (convention).
6. No-V1-suffix convention; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 040-048 invariants preserved (meta-guard 18).
8. Real catalog isolation (Wendelon enforcement beneath the
   tenant-keyed Toron registry), not just attribution.

## Conclusion

ADR 049 sources the SQL tenant from the authenticated client
cert, scopes catalog resolution to it via `Catalog::scoped`, and
tenant-keys Toron's local registry — turning Toron's SQL gateway
multi-tenant over the already-tenant-keyed Wendelon substrate,
with no Wendelon production change. The lone implementation risk
(exposing the admitted peer identity from the QUIC accept layer)
is grounded on Quinn `Connection::peer_identity()`.

The eight-phase migration is materialized in
`plans/impl/0049_authenticated_tenant_routing_for_sql.md`.
