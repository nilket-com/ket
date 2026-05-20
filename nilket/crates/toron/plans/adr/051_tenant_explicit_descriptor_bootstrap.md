# ADR 051: Tenant-Explicit Descriptor Bootstrap

**Status:** Promoted
**Seed:** ADR 050 promotion §"ADR 051 candidates" (tenant-explicit
descriptor ingestion, LEAD — Codex's ADR 050 iter-0 Option 1,
Toron-local).
**Council:** Claude, Codex, Gemini (iter-0 3-of-3 on the topic
with a mechanism split — Codex+Claude startup config-file
bootstrap vs. Gemini runtime SQL management-message; iter-1 Claude
resolved the mechanism 2-of-3 toward the startup bootstrap and an
audit found the flat bootstrap format is test-only; iter-2 dual
converged on the shape, Gemini escalated its runtime model to
`SqlMessage::Admin` + `Monitor`-role auth + new error codes; iter-3
two settlements — mechanism firmly 2-of-3 startup bootstrap, and
the `table_object_id` duplicate check dropped (validate registry
index keys only); promoted from iter-3 with Codex+Claude 2-of-3
convergence).
**Category:** Toron compute-ring substrate ADR — startup
descriptor-ingestion revision completing the multi-tenant arc
(ADR 049 routing + ADR 050 governance). Exterior Substrate →
Toron home. Startup-only. Zero Wendelon production change, zero
SQL wire change, zero runtime ingestion.

## Summary

Make Toron's startup descriptor bootstrap **tenant-explicit**:
each `[catalog].descriptor_bootstrap_path` JSON entry carries
`tenant_id`, loaded atomically into the ADR-049 tenant-keyed
`WendelonDescriptorRegistry`. This retires ADR 049's single-tenant
bootstrap (D11 gap) so one daemon can serve multiple tenants'
pre-existing tables. The source-not-mechanism completion of the
multi-tenant arc: fix the static single-tenant descriptor source;
add no new mechanism.

The `WendelonDescriptorRegistry::from_tenant_descriptors` atomic
constructor is the seam a future live-Wendelon-sync ADR reuses
(no speculative producer trait now).

## Motivation

ADR 042 introduced startup descriptor bootstrap as a product type
with explicit `table_id`. ADR 049 made the registry tenant-keyed
and scoped catalog resolution by the admitted SQL tenant; ADR 050
made query memory per-tenant. But daemon startup still reads a
tenant-flat descriptor file and applies every descriptor to
`config.node.tenant_id` (`daemon.rs:147`
`from_descriptors_for_tenant(config.node.tenant_id, pairs)`;
`ToronDescriptorBootstrapTable { table_id, descriptor }` has no
tenant field). So a daemon can bootstrap only one tenant's tables;
operator config cannot express "tenant 7 and tenant 8 each have
this table." ADR 051 closes that gap.

The flat single-tenant bootstrap format is used **only in tests**
(no production deployment), so making `tenant_id` required is a
contained pre-V1 test-fixture supersession — the same discipline
as ADR 049's `client-<principal>` → `client-<tenant_id>` SAN
change.

## Decisions (14, Promotion-Locked)

**D1: Placement — Toron sequence.** The changed behavior is
Toron's startup config loader + descriptor registry construction.
No Wendelon production code or API.

**D2: Startup-only ingestion.** ADR 051 changes only
`[catalog].descriptor_bootstrap_path` loading at daemon startup.
No file watch, reload endpoint, background refresh, Wendelon
descriptor-listing RPC, runtime SQL admin message, or
descriptor-management SQL.

**D3: Tenant-explicit descriptor entry.** Each entry contains
`tenant_id`, `table_id`, `descriptor`:

```json
{ "tenant_id": 7, "table_id": 1, "descriptor": { "...": "TableSchemaDescriptor" } }
```

`tenant_id` and `table_id` are **required and nonzero**. Missing
`tenant_id` is a stable validation failure (D10), **not** a
fallback to `config.node.tenant_id`.

**D4: Brand-clean bootstrap types.** Replace
`ToronDescriptorBootstrap` → `DescriptorBootstrap`,
`ToronDescriptorBootstrapTable` → `DescriptorBootstrapEntry`. No
aliases, numbered successors, or `*V1`. This rename travels with
the ADR per the **continuous-refactoring convention**
(`plans/conventions/continuous-refactoring.md`): the bootstrap
protocol shape is already changing (to add `tenant_id`), so the
adjacent brand-clean rename belongs in this cycle. All references
update in the same cycle — code, the ADR 042 name-pin guards,
fixtures, and command snippets (the 3-question test: tied to the
touched surface ✓, behavior-neutral ✓, all references updated ✓).

**D5: Loader returns explicit tenant triples.**

```rust
pub fn load_descriptor_bootstrap(
    path: impl AsRef<Path>,
) -> Result<Vec<(TenantId, TableId, wendelon_client::TableSchemaDescriptor)>, ToronConfigError>;
```

It no longer returns tenant-flat `(TableId, descriptor)` and does
not read `config.node.tenant_id`. Raw parse structs use
`Option<u64>` for `tenant_id`/`table_id` so missing fields hit the
D10 stable diagnostics (not raw serde text); the public surface
exposes normalized `TenantId`/`TableId` triples.

**D6: Production daemon uses the tenant-explicit constructor.**

```rust
WendelonDescriptorRegistry::from_tenant_descriptors(
    impl IntoIterator<Item = (TenantId, TableId, TableSchemaDescriptor)>,
) -> Result<Self, CatalogError>
```

`ToronDaemon::start` (`daemon.rs:147`) uses it directly. The
daemon bootstrap path **must not** call `from_descriptors(...)`,
`from_descriptors_for_tenant(config.node.tenant_id, ...)`, or any
helper defaulting descriptors to `TenantId(1)` /
`[node].tenant_id` (the named main implementation risk; guard 10
pins it). `config.node.tenant_id` remains the loopback SQL tenant
(ADR 049) — unchanged; it is just no longer the descriptor
-ingestion tenant.

**D7: Atomic fresh-registry construction.** All-or-nothing.
Validation + insertion target a fresh registry; daemon services
see it only after construction succeeds; startup fails on any
invalid or conflicting entry; no partially loaded registry is
exposed.

**D8: Duplicate rules are registry-key scoped.** Within one
tenant, reject duplicate:
- `(tenant_id, table_id)`
- `(tenant_id, namespace, table_name)`

Allow the same `table_id` and table name across different tenants
(preserves ADR 049 D9). **No `table_object_id` uniqueness
validation** — the loader validates only the registry index keys.
`table_object_id` is descriptor-internal (Wendelon's
tenant-scoped durable-lifecycle identity); the runtime storage
path backstops a stale mapping via `request.table_object_id !=
descriptor.table_object_id` (`DescriptorEvolved`). The duplicate
checks live in the registry constructor so programmatic bootstrap
gets the same safety as file bootstrap.

*(Known non-validated case: an operator bootstrap mapping two
local `table_id`s to one `table_object_id` within a tenant loads
successfully; queries against the stale mapping surface the
runtime `DescriptorEvolved` error. Acceptable for V1; a future
ADR may fail-fast at bootstrap if pressure surfaces.)*

**D9: Descriptor identity is preserved.** The loader + constructor
do not rewrite descriptor fields: `table_object_id`, schema
generation, physical descriptor fingerprint, namespace, table
name, schema, primary key, locality, layout, evolution policy.
The Toron-local `table_id` remains the bootstrap registry key.

**D10: Stable Toron-owned diagnostics.** Pinned strings:

```text
descriptor bootstrap tenant_id is required
descriptor bootstrap tenant_id 0 is reserved
descriptor bootstrap table_id is required
descriptor bootstrap table_id 0 is reserved
duplicate descriptor bootstrap table_id <table_id> for tenant <tenant_id>
duplicate descriptor bootstrap table <namespace>.<table_name> for tenant <tenant_id>
```

Serde descriptor parse failures keep serde's surface; ADR 051
pins only Toron's own shape + duplicate diagnostics.

**D11: SQL tenant source remains ADR 049 only.** No SQL `USE
TENANT`, CLI `--tenant`, token/JWT/OIDC/SPIFFE claim, SQL wire
tenant field, or SQL admin ingestion message. Remote SQL uses the
authenticated client-cert tenant; loopback uses
`config.node.tenant_id`.

**D12: Execution-time scoping remains ADR 049.** DDL + storage
paths keep using `Catalog::scoped(context.tenant_id)` and request-
tenant threading. ADR 051 seeds initial descriptors only.

**D13: Low-cardinality observability only.** Keep the aggregate
`descriptors_loaded` count; no per-tenant descriptor metrics or
labels.

**D14: Future sync remains future.** Live Wendelon → Toron
descriptor sync, descriptor-evolution refresh, cache
invalidation, runtime ingestion, audit-backed admin ingestion,
and multi-coordinator schema consensus are deferred to later
(cross-ring) ADRs.

**Single-plan posture; Toron-ring only.**

## Architecture Guards (17, Promotion-Locked)

1. `adr051_descriptor_bootstrap_entries_are_tenant_explicit`
2. `adr051_descriptor_bootstrap_uses_brand_clean_names`
3. `adr051_descriptor_bootstrap_rejects_missing_tenant_and_table_id`
4. `adr051_descriptor_bootstrap_rejects_zero_tenant_and_table_id`
5. `adr051_descriptor_bootstrap_rejects_duplicate_table_id_within_tenant`
6. `adr051_descriptor_bootstrap_rejects_duplicate_table_name_within_tenant`
7. `adr051_descriptor_bootstrap_allows_duplicate_keys_across_tenants`
8. `adr051_loader_returns_tenant_table_descriptor_triples`
9. `adr051_daemon_uses_tenant_explicit_registry_constructor`
10. `adr051_daemon_bootstrap_does_not_default_to_node_tenant`
11. `adr051_registry_constructor_is_atomic_for_bootstrap`
12. `adr051_descriptor_identity_fields_are_preserved`
13. `adr051_no_sql_wire_or_tenant_source_changes`
14. `adr051_no_live_descriptor_sync_or_reload`
15. `adr051_no_per_tenant_metric_labels`
16. `adr051_no_wendelon_production_changes`
17. `adr051_prior_adr04x_and_adr050_guards_continue_to_pass`
    (meta-guard; ADR 040-050 guard names persist + sweep passes)

**Convention guards:**
- `adr051_nilket_root_remains_runtime_free`
- `adr051_fixture_worker_source_unchanged`
- `adr051_new_surface_is_brand_clean_and_has_no_v1_suffix`

**Prior-guard adjustments (continuous-refactoring):** the ADR 042
descriptor-bootstrap guards keep their function names, assertions
updated to the new product types:
- `adr042_descriptor_bootstrap_is_product_type_with_table_ids` →
  assert `DescriptorBootstrap`, `DescriptorBootstrapEntry`,
  `tenant_id`, `table_id`, and the triple return type.
- `adr042_descriptor_bootstrap_has_no_v1_type_suffix` → reject
  old + new `*V1` names.
- `adr042_daemon_uses_single_descriptor_registry` → keep the
  single-shared-registry invariant, matching
  `from_tenant_descriptors`.

## Integration Fixtures (15 required + 2 V1-evidence optional)

**Required (gates promotion):**
1. `descriptor_bootstrap_requires_tenant_id`
2. `descriptor_bootstrap_requires_table_id`
3. `descriptor_bootstrap_rejects_zero_tenant`
4. `descriptor_bootstrap_rejects_zero_table_id`
5. `descriptor_bootstrap_rejects_duplicate_table_id_within_tenant`
6. `descriptor_bootstrap_rejects_duplicate_table_name_within_tenant`
7. `descriptor_bootstrap_allows_same_table_id_across_tenants`
8. `descriptor_bootstrap_allows_same_table_name_across_tenants`
9. `descriptor_bootstrap_loads_two_tenants_into_one_registry`
10. `tenant_7_sql_resolves_tenant_7_bootstrapped_descriptor`
11. `tenant_8_sql_resolves_tenant_8_bootstrapped_descriptor`
12. `tenant_8_cannot_resolve_tenant_7_only_bootstrapped_descriptor`
13. `loopback_sql_uses_config_tenant_for_bootstrapped_descriptor`
14. `descriptor_bootstrap_preserves_physical_fingerprint`
15. `old_single_tenant_descriptor_bootstrap_shape_is_rejected`

**Optional V1-evidence:**
- `descriptor_bootstrap_json_example_round_trips`
- `same_table_object_id_across_different_tenants_is_allowed`

## Out of Scope

- Runtime SQL descriptor ingestion / management-message
  (Gemini's `SqlMessage::Admin` + `Monitor`-role model) — a later
  ADR over a proper catalog-write role + audit, not `Monitor`
  (read-only observability) and not this startup-config cycle.
- Live Wendelon → Toron descriptor sync (cross-ring successor;
  needs a Wendelon producer API).
- Runtime/control-plane reload; file watches.
- Speculative `DescriptorSource` producer trait (the
  `from_tenant_descriptors` constructor is the seam).
- `table_object_id` bootstrap uniqueness validation (Wendelon
  backstops at runtime — D8).
- Descriptor-management SQL; descriptor rewriting; cache
  invalidation / schema-consensus.
- SQL wire / tenant-source change; per-tenant metrics; Wendelon
  production change.

## Council History

- **Iter-0 (3-of-3 topic):** Claude, Codex, Gemini lead
  tenant-explicit ingestion. Codex+Claude framed it as a startup
  config-file bootstrap; Gemini proposed a runtime SQL
  management-message.
- **Iter-1:** Claude resolved the mechanism 2-of-3 (startup
  bootstrap); audit confirmed the flat format is test-only
  (contained supersession). Codex tightened (brand-clean rename,
  `Option<u64>` loader, pinned diagnostics).
- **Iter-2:** Codex+Claude dual converged on the startup
  bootstrap. The `table_object_id` dup check swapped (Codex
  dropped it; Claude added it). Gemini escalated its runtime model
  to `SqlMessage::Admin` + `Monitor`-role auth + new error codes.
- **Iter-3 (promoted):** mechanism firmly 2-of-3 startup bootstrap
  (Gemini's `Monitor`-role-writes-catalog escalation noted as a
  privilege-escalation anti-pattern + further reason to defer);
  `table_object_id` check dropped (Claude retracts iter-2 S3,
  converges on Codex iter-2 — validate registry index keys only).

**Rejected (2-of-3 Codex+Claude):**
- Gemini's runtime `SqlMessage::Admin` ingestion + `Monitor`-role
  authorization + new error codes — a SQL wire surface + a
  privilege-escalation model (read-only observability role writing
  tenant catalogs) for startup config. Deferred to a future ADR
  with a proper catalog-write role + audit.
- Dual-shape (tenant-flat + tenant-explicit) backward compat —
  keeps the ambiguity; clean pre-V1 supersession instead.
- Implicit `[node].tenant_id` fallback for missing `tenant_id` —
  the old single-tenant behavior under a new name.
- `table_object_id` bootstrap validation — Wendelon's
  tenant-scoped durable identity backstops it at runtime; the
  loader validates only the keys it indexes.

## Promotion criteria met

1. D-set locked at 14 (Codex+Claude 2-of-3).
2. Guards 17 + 3 convention; ADR 042 name-pin guards updated.
3. Fixtures 15 required + 2 V1-evidence.
4. Single-plan, Toron-ring; no Wendelon production change; no SQL
   wire change; no runtime ingestion.
5. Placement: Exterior Substrate → Toron home; the brand-clean
   rename travels per the continuous-refactoring convention.
6. No-V1 convention; `toron_fixture_worker.rs` unchanged;
   `crates/nilket` runtime-free.
7. ADR 040-050 invariants preserved (meta-guard 17).

## Conclusion

ADR 051 makes the startup descriptor bootstrap tenant-explicit
and atomic, retiring ADR 049's single-tenant bootstrap so one
daemon serves multiple tenants' pre-existing tables — the
source-not-mechanism completion of the multi-tenant arc, with no
SQL wire change, no Wendelon API, and no runtime ingestion. The
`from_tenant_descriptors` atomic constructor is the seam a future
live-sync ADR reuses.

The seven-phase migration is materialized in
`plans/impl/051_tenant_explicit_descriptor_bootstrap.md`.
