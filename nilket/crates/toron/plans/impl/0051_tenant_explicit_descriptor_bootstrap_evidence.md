# ADR 051 Evidence: Tenant-Explicit Descriptor Bootstrap

**ADR:** `crates/toron/plans/adr/0051_tenant_explicit_descriptor_bootstrap.md`
**Implementation plan:** `crates/toron/plans/impl/0051_tenant_explicit_descriptor_bootstrap.md`

## Implementation Notes

- Descriptor bootstrap JSON is now tenant-explicit: every entry carries required, nonzero `tenant_id` and `table_id`.
- Public bootstrap types are brand-clean: `DescriptorBootstrap` and `DescriptorBootstrapEntry`; no compatibility aliases or `*V1` successors were added.
- `load_descriptor_bootstrap` parses through raw optional IDs and returns normalized `(TenantId, TableId, TableSchemaDescriptor)` triples with the ADR 051 stable missing/zero diagnostics.
- `WendelonDescriptorRegistry::from_tenant_descriptors` builds a fresh registry, rejects duplicate `(tenant_id, table_id)` and `(tenant_id, namespace, table_name)` keys with the pinned diagnostics, and allows the same local table key/name across tenants.
- `ToronDaemon::start_with_client` feeds loader triples directly into `from_tenant_descriptors`; `[node].tenant_id` remains only the loopback SQL tenant source.
- Descriptor identity fields are not rewritten; the bootstrap `table_id` remains the Toron-local registry key.

## Preserved Boundaries

- Startup-only ingestion: no file watch, reload endpoint, SQL admin message, live Wendelon descriptor sync, or descriptor-management SQL.
- No SQL wire or CLI tenant-source changes.
- No per-tenant descriptor metrics; the aggregate `descriptors_loaded` count is retained.
- No production changes under `crates/wendelon`.
- `crates/nilket` remains runtime-free for Toron.
- `crates/toron/src/bin/toron_fixture_worker.rs` remains source-unchanged.

## Verification

Commands run:

```text
cargo fmt
cargo test -p toron --test config_test descriptor_bootstrap -- --nocapture
cargo test -p toron --test architecture_test adr051 -- --nocapture
cargo test -p toron --test sql_listener_test -- --nocapture
cargo test -p toron --test architecture_test -- --nocapture
cargo test -p toron --test config_test -- --nocapture
cargo test -p toron --lib
cargo test -p toron --quiet
cargo clippy -p toron -- -D warnings
```

Observed results:

- `config_test descriptor_bootstrap`: 12 passed.
- `architecture_test adr051`: 20 passed.
- `sql_listener_test`: 10 passed.
- Full `architecture_test`: 439 passed.
- Full `config_test`: 24 passed.
- `cargo test -p toron --lib`: 224 passed.
- Full `cargo test -p toron --quiet`: passed all Toron suites; the Wendelon-backed storage execution suite reported 9 passed / 1 ignored, matching the existing ignored fixture.
- `cargo clippy -p toron -- -D warnings`: passed.

## Fixture Notes

- `descriptor_bootstrap_requires_tenant_id`, `descriptor_bootstrap_requires_table_id`, `descriptor_bootstrap_rejects_zero_tenant`, and `descriptor_bootstrap_rejects_zero_table_id` pin the loader-owned diagnostics.
- `descriptor_bootstrap_rejects_duplicate_table_id_within_tenant` and `descriptor_bootstrap_rejects_duplicate_table_name_within_tenant` pin the registry constructor diagnostics.
- `descriptor_bootstrap_allows_same_table_id_across_tenants`, `descriptor_bootstrap_allows_same_table_name_across_tenants`, and `descriptor_bootstrap_loads_two_tenants_into_one_registry` verify ADR 049 tenant-key isolation is preserved.
- `tenant_7_sql_resolves_tenant_7_bootstrapped_descriptor`, `tenant_8_sql_resolves_tenant_8_bootstrapped_descriptor`, `tenant_8_cannot_resolve_tenant_7_only_bootstrapped_descriptor`, and `loopback_sql_uses_config_tenant_for_bootstrapped_descriptor` verify tenant-explicit startup descriptors through live SQL execution.
- `descriptor_bootstrap_preserves_physical_fingerprint` verifies descriptor identity is preserved.
- `old_single_tenant_descriptor_bootstrap_shape_is_rejected` documents the intentional pre-V1 flat-shape supersession.
