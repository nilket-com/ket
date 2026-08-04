# Impl Plan — ADR 136: Wendelon-Backed Hard Table-Size Facts (136-T Toron consumer)

**Migration for** `plans/adr/0136_wendelon_backed_table_size_facts.md`.
**Scope (136-T only):** `catalog.rs` (sidecar fact type + facts-aware constructors,
registry/bootstrap/sync paths, refresh-on-facts-change), `config.rs` (bootstrap JSON
optional facts block), `binder.rs` (explicit `PlannerConfig::default`), tests/guards.
**Non-scope:** the 136-W Wendelon producer; `TableSchemaDescriptor` shape; wire /
scheduler / substrate; selectivity (ADR 137); any 129–135 strategy logic.

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| production blind spot | `TableBinding::from_descriptor_with_logical_types` → `optimizer_facts: TableOptimizerFacts::default()` (`catalog.rs:204`) |
| fact type to populate | `TableOptimizerFacts { byte_upper_bound, row_upper_bound }` (`catalog.rs:79`) |
| ingestion call sites (10) | `from_descriptor_with_logical_types` (`catalog.rs:1093/1125/1180/1198/1238/1266/2048/502/768`) |
| registry/sync seam | `register_descriptor_for_tenant` (`catalog.rs:1107`), `from_tenant_descriptors` (`catalog.rs:1047`), bootstrap `config.rs:659 load_descriptor_bootstrap` |
| refresh-by-identity (D5) | descriptor sync returns existing binding on `same_descriptor_identity(...)` — must not no-op a facts change |
| flip surface | `PlannerConfig.auto_broadcast_max_build_bytes` (`binder.rs:62`); `effective_auto_broadcast_threshold` (`binder.rs:4004`); `default_auto_broadcast_cap_ceiling` (`binder.rs:4010`, 16 MiB) |
| do-not-consume (D3) | `LsmRunSummary.size_bytes` (`lsm.rs:62`, run-scoped); `estimated_size_bytes` (`lsm.rs:252`, soft) |

## Phases
0. **Failing gate first:** `star_query_infers_broadcast_without_seed_optimizer_facts`
   (SQL + Rune over descriptor+sidecar fixture) fails on `main` because
   `from_descriptor` defaults unknown. `legacy_descriptor_paths_still_default_unknown`
   green before + after.
1. **Sidecar fact type (D1/D2/D3):** add `DescriptorOptimizerFacts { schema_generation,
   physical_descriptor_fingerprint, manifest_epoch, materialized_byte_upper_bound:
   Option<ByteCount>, row_upper_bound: Option<RowCount> }` near `TableOptimizerFacts`,
   with `for_descriptor(&self, descriptor) -> TableOptimizerFacts` that returns
   `default()` unless `schema_generation` **and** `physical_descriptor_fingerprint`
   match (atomic — Gemini G1). Preserve a *known-empty* fact distinct from unknown:
   `RowCount` is zero-representable, and the consumer should normalize
   `row_upper_bound = Some(0)` to `materialized_byte_upper_bound = Some(ByteCount::ZERO)`
   when the byte bound is absent, so the existing byte-first chooser can broadcast it.
2. **Facts-aware constructors (D4):** `TableBinding::from_descriptor_with_optimizer_facts`
   + `..._with_logical_types_and_optimizer_facts`; existing `from_descriptor*` become
   wrappers passing unknown.
3. **Registry/bootstrap/sync (D4/D5):** facts-aware `from_descriptors_for_tenant_with_facts`,
   `from_tenant_descriptors_with_facts`, `register_descriptor_for_tenant_with_facts`,
   `sync_*_with_facts`; descriptor-only methods delegate with `None`. **On sync of an
   existing table, rebuild the binding when sidecar facts changed even if descriptor
   identity is unchanged** (D5 — do not let `same_descriptor_identity` no-op it).
   Extend `config.rs` bootstrap JSON with `optimizer_facts: Option<RawDescriptorOptimizerFacts>`,
   keeping the old loader as a wrapper → production-shaped fixtures before 136-W.
4. **Explicit planner default (D6):** replace `#[derive(Default)]` for `PlannerConfig`
   with an impl setting `auto_broadcast_max_build_bytes: Some(ByteCount::new(1024*1024))`
   (clamped by the 16 MiB ceiling). Keep explicit small-threshold test configs.
5. **Guards + tests:** the eight `adr136_*` guards; the behavior gates (incl.
   `planner_default_on_missing_facts_still_hashes`,
   `facts_refresh_updates_binding_without_descriptor_identity_change`,
   `known_empty_table_infers_broadcast`, `materialized_byte_bound_not_storage_size`);
   re-run 129–135.
6. **Verify:** `cargo test -p toron` · `--test architecture_test -- adr129 adr130
   adr131 adr132 adr133 adr134 adr135 adr136` · `cargo check -p toron --all-targets`
   (new fact field is widely constructed — catch bench/example E0063) · `git diff --check`.

## Risk register
- **Undercount = silent wrong broadcast (the big one):** D3 materialized-byte naming
  + `materialized_byte_bound_not_storage_size` gate + the no-`size_bytes`-mapping
  guard are the backstops. This is the one place "data data data" corrupts results.
- **Stale facts after table growth:** D5 refresh gate; facts keyed to `manifest_epoch`,
  not schema generation. Producer-known stale facts must arrive as absent/unknown
  unless 136-W later gives Toron an authoritative freshness comparison.
- **Default flip regressing seeded/legacy paths:** `planner_default_on_missing_facts_still_hashes`
  + `legacy_descriptor_paths_still_default_unknown`; all explicit test thresholds kept.
- **Known-empty vs unknown conflation:** `known_empty_table_infers_broadcast` pins the
  distinction; phase 1 normalizes zero rows to a zero materialized byte bound.
- **Wide construction of a new fact field:** `--all-targets` per
  [[feedback_verify_all_targets_on_new_ir_field]].
- **Descriptor identity drift:** `adr136_table_schema_descriptor_not_modified` — facts
  are sidecar, fingerprint/equality/upsert untouched.

## Acceptance
SQL + Rune star query over descriptor+synthetic-sidecar infers broadcast and
fragments via 135's star with no `seed_optimizer_facts`; missing/mismatched
hash; known-empty broadcasts; default enables inference yet hashes without facts;
refresh updates facts without identity change; descriptor unchanged; byte bound is
materialized; eight `adr136_*` + 129–135 guards green.
