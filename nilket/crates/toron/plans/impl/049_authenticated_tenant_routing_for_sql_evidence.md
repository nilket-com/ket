# ADR 049 Evidence: Authenticated Tenant Routing for SQL

**ADR:** `crates/toron/plans/adr/049_authenticated_tenant_routing_for_sql.md`
**Implementation plan:** `crates/toron/plans/impl/049_authenticated_tenant_routing_for_sql.md`

## Implementation Notes

- Remote authenticated SQL derives the execution tenant from the accepted QUIC connection peer certificate via `accepted_sql_tenant`, after TLS handshake and before SQL stream acceptance.
- Loopback SQL keeps the configured `[node].tenant_id` fallback; authenticated remote SQL does not use that fallback.
- SQL client SANs now require canonical numeric tenants (`client-<tenant_id>.<cluster_domain>`). Legacy free-form `client-<principal>` SQL SAN fixtures were intentionally superseded.
- `PeerKind::Client` carries `tenant_id`; `principal` remains diagnostic text only.
- `Catalog::scoped` was added and both coordinator paths bind through a tenant-scoped catalog view.
- `WendelonDescriptorRegistry`, `MockCatalog`, `WendelonCatalog`, and Wendelon storage descriptor lookups are tenant-keyed by the request/scoped tenant.
- The Wendelon storage integration helpers now share a tenant-keyed descriptor registry between catalog binding and storage execution, preserving the live tenant-isolation fixture under ADR 049.

## Preserved Boundaries

- No SQL wire changes: no new `SqlMessage` variants and no new `SqlErrorCode` values.
- No `nilket toron sql --tenant` flag or token/JWT/OIDC/SPIFFE tenant model.
- No per-tenant metric labels.
- No production changes under `crates/wendelon`.
- `crates/nilket/Cargo.toml` remains runtime-free for Toron.
- `crates/toron/src/bin/toron_fixture_worker.rs` remains source-unchanged.

## Verification

Commands run:

```text
cargo fmt --all
cargo test -p toron --test architecture_test
cargo test -p toron --test config_test --test sql_listener_test --test tls_listener_test
cargo test -p toron --test wendelon_storage_execution_test --quiet
cargo test -p toron --quiet
cargo clippy -p toron -- -D warnings
```

Observed results:

- `architecture_test`: 403 passed.
- `config_test`: 11 passed.
- `sql_listener_test`: 7 passed.
- `tls_listener_test`: 10 passed.
- `wendelon_storage_execution_test`: 9 passed, 1 ignored.
- Full `cargo test -p toron --quiet`: passed all Toron suites, including 214 lib tests and all integration suites.
- `cargo clippy -p toron -- -D warnings`: passed.

## Fixture Notes

- `sql_client_tenant_7_observed_in_execution_context` verifies a numeric client SAN routes execution under tenant 7.
- `sql_client_tenant_8_observed_proves_not_daemon_static` verifies authenticated remote SQL does not fall back to the daemon/config tenant.
- `remote_sql_listener_rejects_multiple_valid_client_tenant_sans` verifies ambiguous multi-tenant client SANs are rejected before SQL execution.
- `wendelon_storage_scan_keeps_live_tenants_isolated` verifies Wendelon-backed storage scans remain tenant-isolated after descriptor lookups became request-tenant keyed.
- Fixture 11 (`create_table_submits_wendelon_request_for_admitted_tenant`) is accepted by code-inspection / architecture-guard coverage of `WendelonCatalog::create_table` using `wendelon_tenant_id(self.tenant_id)`, rather than by an executing live admin fixture.
- `wendelon_catalog_create_table_submits_schema_update_request` remains ignored because the simulated fixture does not yet expose authenticated schema-update admin operations; this is pre-existing fixture-surface limitation, not an ADR 049 behavior failure.
- The de-branded test target names (`config_test`, `sql_listener_test`, `tls_listener_test`) are in scope for ADR 049; prior evidence command references were updated accordingly.
