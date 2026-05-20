# Impl Plan 051: Tenant-Explicit Descriptor Bootstrap

**Status:** Promoted, paired with
`plans/adr/051_tenant_explicit_descriptor_bootstrap.md`.
**Posture:** Single plan, Toron-ring only. Startup-config change
+ atomic registry constructor. No Wendelon production change, no
SQL wire change, no runtime ingestion. Seven phases.

## Invariants carried in (must still hold at the end)

- `crates/nilket` stays runtime-free.
- `crates/toron/src/bin/toron_fixture_worker.rs` source unchanged.
- New surface is brand-clean (no `Toron` prefix) and has no `*V1`
  suffix (`no-version-suffixes`, `no-project-name-in-protocols`).
- ADR 049 tenant routing + ADR 050 per-tenant budget paths
  unchanged in behavior; their guards keep passing.
- ADR 040-050 guard *names* persist (meta-guard 17) and the guard
  sweep stays green.
- Brand-clean rename travels with this cycle per
  `plans/conventions/continuous-refactoring.md` (the bootstrap
  protocol shape is already changing, so the adjacent rename is in
  scope; 3-question test passes — tied to the touched surface,
  behavior-neutral, all references updated).

## Phase 0 — Audit (no code change)

- Confirm the bootstrap path: `config.rs`
  `ToronDescriptorBootstrap` / `ToronDescriptorBootstrapTable` +
  `load_descriptor_bootstrap`, consumed at `daemon.rs:142-149`
  via `from_descriptors_for_tenant(config.node.tenant_id, pairs)`.
- Enumerate every caller of `WendelonDescriptorRegistry`
  constructors (`from_descriptors`, `from_descriptors_for_tenant`,
  `from_tenant_descriptors` if present) and classify
  production vs. test — including `storage.rs` `from_descriptors`.
  The daemon bootstrap is the only production caller that must
  move to the tenant-explicit constructor.
- Locate the ADR 042 name-pin guards
  (`adr042_descriptor_bootstrap_is_product_type_with_table_ids`,
  `adr042_descriptor_bootstrap_has_no_v1_type_suffix`,
  `adr042_daemon_uses_single_descriptor_registry`).
- Confirm the flat single-tenant bootstrap JSON shape is used
  **only in tests/fixtures** (no production deployment) →
  pre-V1 supersession is contained.

## Phase 1 — Brand-clean tenant-explicit config types + loader

- Rename in `config.rs`:
  - `ToronDescriptorBootstrap` → `DescriptorBootstrap`
  - `ToronDescriptorBootstrapTable` → `DescriptorBootstrapEntry`
  No aliases and no `*V1`. Update all references in the same edit
  (D4).
- Add the required, nonzero `tenant_id` field to
  `DescriptorBootstrapEntry` (alongside `table_id`, `descriptor`).
  Raw parse struct uses `Option<u64>` for `tenant_id`/`table_id`
  so missing fields produce the D10 stable diagnostics rather than
  raw serde text.
- `load_descriptor_bootstrap` returns
  `Vec<(TenantId, TableId, TableSchemaDescriptor)>` (D5). It no
  longer reads `config.node.tenant_id`. Normalize `Option<u64>`
  → `TenantId`/`TableId` here, emitting the four required/zero
  diagnostics:

```text
descriptor bootstrap tenant_id is required
descriptor bootstrap tenant_id 0 is reserved
descriptor bootstrap table_id is required
descriptor bootstrap table_id 0 is reserved
```

- Descriptor identity fields are passed through untouched (D9).

## Phase 2 — `from_tenant_descriptors` atomic registry constructor

- Add to `WendelonDescriptorRegistry` (`catalog.rs`):

```rust
pub fn from_tenant_descriptors(
    entries: impl IntoIterator<Item = (TenantId, TableId, TableSchemaDescriptor)>,
) -> Result<Self, CatalogError>;
```

- Build into a **fresh** registry, all-or-nothing (D7): construct
  locally, return `Err` on the first invalid/conflicting entry,
  expose only on full success. Reuse
  `insert_wendelon_descriptor_locked` (the existing path that
  already rejects `(tenant_id, table_ref)` / `(tenant_id,
  table_id)` collisions) so file and programmatic bootstrap share
  the same safety.
- **Within-tenant duplicate validation = registry index keys only**
  (D8, Settlement 2): `(tenant_id, table_id)` and
  `(tenant_id, namespace, table_name)`. Allow the same
  `table_id` / table name across tenants (preserves ADR 049 D9).
  **No `table_object_id` check** — do **not** introduce a
  transient `HashSet<(TenantId, TableObjectId)>`. Surface the two
  collisions as the D10 diagnostics:

```text
duplicate descriptor bootstrap table_id <table_id> for tenant <tenant_id>
duplicate descriptor bootstrap table <namespace>.<table_name> for tenant <tenant_id>
```

## Phase 3 — Daemon repoint

- `daemon.rs:142-149`: replace
  `from_descriptors_for_tenant(config.node.tenant_id, pairs)` with
  `WendelonDescriptorRegistry::from_tenant_descriptors(triples)?`
  fed directly by the Phase 1 loader (D6).
- The daemon bootstrap path must **not** call `from_descriptors`,
  `from_descriptors_for_tenant(config.node.tenant_id, ...)`, or any
  helper that defaults descriptors to `TenantId(1)` /
  `[node].tenant_id` (named main risk; guard 10).
- `config.node.tenant_id` remains the loopback SQL tenant
  (ADR 049) — unchanged; it is just no longer the descriptor
  ingestion tenant.
- Keep the aggregate `descriptors_loaded` count; no per-tenant
  descriptor metric labels (D13).

## Phase 4 — Guards (17 architecture + 3 convention)

Add the 17 ADR 051 architecture guards (ADR doc list), notably:
- guard 9 `adr051_daemon_uses_tenant_explicit_registry_constructor`
- guard 10 `adr051_daemon_bootstrap_does_not_default_to_node_tenant`
  (named-main-risk pin)
- guard 8 `adr051_loader_returns_tenant_table_descriptor_triples`
- **no** object_id-duplicate guard (dropped per Settlement 2)
- guard 17 meta `adr051_prior_adr04x_and_adr050_guards_continue_to_pass`

Convention guards:
`adr051_nilket_root_remains_runtime_free`,
`adr051_fixture_worker_source_unchanged`,
`adr051_new_surface_is_brand_clean_and_has_no_v1_suffix`.

**Update the ADR 042 name-pin guards in place (keep names):**
- `adr042_descriptor_bootstrap_is_product_type_with_table_ids` →
  assert `DescriptorBootstrap` / `DescriptorBootstrapEntry`,
  `tenant_id`, `table_id`, and the triple return type.
- `adr042_descriptor_bootstrap_has_no_v1_type_suffix` → reject old
  + new `*V1` names.
- `adr042_daemon_uses_single_descriptor_registry` → keep the
  single-shared-registry invariant; matches `from_tenant_descriptors`.

## Phase 5 — Integration fixtures (15 required + 2 V1-evidence)

Required (gate promotion) — ADR doc list, including:
- tenant_id / table_id required + zero rejections (1-4)
- within-tenant duplicate table_id / table name rejection (5-6)
- same table_id / table name allowed across tenants (7-8)
- two tenants into one registry (9)
- tenant-7 / tenant-8 SQL resolves own descriptor; tenant-8 cannot
  resolve tenant-7-only descriptor (10-12)
- loopback SQL uses `config.node.tenant_id` (13)
- physical fingerprint preserved (14)
- `old_single_tenant_descriptor_bootstrap_shape_is_rejected` (15)

Optional V1-evidence:
`descriptor_bootstrap_json_example_round_trips`,
`same_table_object_id_across_different_tenants_is_allowed`.

Drop (Settlement 2): the object_id-duplicate-within-tenant
required fixture and the object_id-cross-tenant-reuse *required*
fixture (the latter survives as the optional evidence fixture).

Supersede + document the old tenant-flat bootstrap fixtures (no
compat shape retained — `no-premature-backward-compat`).

## Phase 6 — Evidence + promotion

- Promote into the Toron sequence:
  `plans/adr/051_*` + `plans/impl/051_*` (Status: Promoted, paired).
- Roadmap: ADR 051 Status bullet; new `### ADR 051 promoted;
  ADR 052 candidates` (lead: **live Wendelon → Toron descriptor
  sync** over a new Wendelon descriptor-query API; secondary:
  per-tenant governance Phase 2 / runtime descriptor ingestion
  over a proper catalog-write role + audit / OTLP / SQL widening);
  demote `### ADR 050 promoted; ADR 051 candidates` to HISTORICAL.

## Acceptance checklist

- [ ] Phase 0 audit complete (callers classified; flat format
      confirmed test-only).
- [ ] Phase 1 brand-clean types + tenant_id + triple loader + 4
      pinned required/zero diagnostics.
- [ ] Phase 2 `from_tenant_descriptors` atomic; two within-tenant
      dup checks; **no object_id check**; 2 pinned dup diagnostics.
- [ ] Phase 3 daemon repointed; no implicit single-tenant default;
      loopback tenant unchanged; aggregate count retained.
- [ ] Phase 4 17 + 3 guards pass; ADR 042 name-pin guards updated;
      ADR 040-050 guards pass.
- [ ] Phase 5 15 required fixtures pass; flat-format fixtures
      superseded + documented.
- [ ] Phase 6 ADR + impl + roadmap promoted; evidence recorded.
