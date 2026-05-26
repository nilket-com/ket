# ADR 107: `information_schema.schemata`

**Status:** Promoted (plan). **Category:** Toron Exterior Composition - SQL/catalog introspection surface.
**Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on the narrow, descriptor-derived 4th finite system
relation. **Toron-only** — reuses 099's `ExecutionPlan::SystemTable` leaf and 102's authoritative
`refreshed_table_schema_descriptors` snapshot; **no new `ExecutionPlan` variant, walker sweep, Wendelon
RPC/wire, storage scan, PBC, executor, or worker surface.** **Predecessors:** 099 (`SystemTable` leaf +
reserved read-only `information_schema` namespace), 102 (`tables`/`.columns` + authoritative refresh).
**Migration:** `plans/impl/107_information_schema_schemata.md`.

## Doctrine — the 4th finite system relation, descriptor-derived (not a registry)

099/102 built a finite `SystemTableKind` family (logical_types + tables + columns), each an explicit
`bind_information_schema_relation` dispatch arm over a bind-time authoritative refresh, materialized
coordinator-local. `schemata` is the **4th member, same pattern, smaller**: it lists the **distinct
namespaces present in the latest table descriptors**. It is *descriptor-derived*, not a schema registry —
Toron has table *namespaces*, not standalone schema objects, so there is no `CREATE`/`DROP SCHEMA`, no
empty/synthetic schema, no owner/catalog/privilege semantics, and no self-listing of `information_schema`.

## Scale boundary

One read-only system relation: `information_schema.schemata`, exposed as `schema_name TEXT NOT NULL`,
composing through the existing coordinator-local `SystemTable` path. **Out of scope:** `CREATE`/`DROP
SCHEMA`, schema ownership/catalog/charset, constraints / indexes / PK-FK / views (need constraint/namespace
modeling), and any Wendelon-side schema registry.

## Grounding (verified)

- **Dispatch + substrate already exist.** `bind_information_schema_relation` (`binder.rs:683`) is an explicit
  per-relation switch (`eq_ignore_ascii_case(INFORMATION_SCHEMA_X_TABLE)` → fetch → `system_table_plan(kind,
  schema_fn(), rows, projection)`); `system_table_plan` (`:3365`) already handles projection.
- **Reuse 102's authoritative snapshot.** `tables`/`.columns` both call
  `catalog.refreshed_table_schema_descriptors().await?` (latest-only, tenant-scoped); `schemata` reuses it.
  `information_schema_table_rows` (`:3403`) already emits `descriptor.namespace` — `schemata` is that field,
  distincted.
- **No walker sweep.** `SystemTableKind` has no exhaustive match outside `binder.rs` (grep clean);
  `SystemTable` execution is kind-agnostic. The 4th variant touches only the binder — the 102 2nd/3rd-variant
  additions are the precedent.
- **Non-inert.** Schema-qualified DDL `CREATE TABLE schema.t` lowers through `lower_ddl_table_name` (`:597`)
  to `TableRef::with_schema`, so non-default namespaces are user-creatable; `schemata` lists real namespaces.
- **The read-only sentinel must move.** `information_schema_namespace_is_read_only_and_scoped`
  (`logical_type_ddl_test.rs:2522`) uses `information_schema.schemata` as its *unknown-relation* sentinel
  (`:2542`); 107 makes schemata supported, so the sentinel becomes a still-unsupported relation (`views`).
- **The coordinator-local guard enumerates variants.** `adr102_system_table_execution_stays_coordinator_local`
  (`nominal_type_identity.rs:1122`) asserts each variant is present in the IR/binder (`:1128`) and absent from
  storage/PBC (`:1135`) — so it must gain `InformationSchemaSchemata` (Codex's catch; not a no-op retarget).

## Decisions (promotion-locked, 3-of-3)

**D1 — Add `SystemTableKind::InformationSchemaSchemata`** (the 4th finite variant). Additive; no registry /
trait object / `HashMap` / new `ExecutionPlan` variant / walker sweep.

**D2 — Explicit binder dispatch.** Constants `INFORMATION_SCHEMA_SCHEMATA` (`"information_schema.schemata"`)
+ `INFORMATION_SCHEMA_SCHEMATA_TABLE` (`"schemata"`); a case-insensitive arm mirroring the `tables` arm; add
`schemata` to the unknown-relation diagnostic's supported list. No registry map.

**D3 — Reuse 102's authoritative refresh; fail-closed.** The arm calls
`self.catalog.refreshed_table_schema_descriptors().await?` and bakes rows at bind; refresh/list failure
**fails the query** (no stale fallback).

**D4 — Descriptor-derived distinct rows.** `information_schema_schemata_rows` builds a `BTreeSet<String>` over
`descriptor.namespace` and emits one `ScalarValue::Utf8` row per distinct namespace (deterministic sorted).
**Empty descriptors → zero rows** (no magic `default`/`information_schema`/empty-schema rows).

**D5 — Minimal schema: `schema_name TEXT NOT NULL` only.** No `catalog_name`/`schema_owner`/charset — Toron
has no catalog object, and `tables`/`columns` already omit SQL-standard catalog columns; no tenant-id leak.

**D6 — Coordinator-local execution unchanged.** `SystemTable` receives baked scalar rows; no execution-time
descriptor listing, storage scan, worker dispatch, PBC, or Wendelon call.

**D7 — Read-only namespace preserved.** Unqualified `schemata` stays a normal table lookup;
`information_schema.schemata` is selectable; unknown relations (e.g. `information_schema.views`) still reject;
DDL/DML targeting `information_schema.*` (incl. `schemata`) still rejects as read-only.

**D8 — Reconcile the inverted guards/sentinel in-change (discipline #4).** Retarget the two ADR 102
finite-family + explicit-dispatch guards (3 → 4), the ADR 102 coordinator-local guard (+the new variant in
its present/absent assertions), the behavior test `information_schema_namespace_is_read_only_and_scoped`
(sentinel `schemata` → `views`), and `system_table_executes_without_storage_scan` (+schemata).

## Behavior gates

- `information_schema_schemata_lists_descriptor_namespaces` — multiple descriptors in one namespace → one
  row; descriptors across namespaces → sorted distinct rows.
- `information_schema_schemata_is_tenant_scoped` — scoped catalogs see only their tenant's namespaces.
- `information_schema_schemata_is_latest_only` — superseded descriptor generations produce no stale/duplicate
  namespace row.
- `information_schema_schemata_empty_catalog_has_no_rows` — no descriptors → zero rows (no magic `default`).
- `information_schema_schemata_listing_failure_fails_query_without_stale_fallback` — refresh failure fails
  binding (D3).
- `information_schema_schemata_composes_with_query_ops` — projection / `WHERE` / `ORDER BY` / `COUNT(*)`
  compose via the existing system-table path.
- Retargeted `information_schema_namespace_is_read_only_and_scoped` (schemata binds; sentinel `views`;
  mutation targets reject) and `system_table_executes_without_storage_scan` (+schemata).

## Architecture guards

- Retarget `adr102_system_table_kind_has_finite_introspection_family` (3 → 4; assert
  `InformationSchemaSchemata`; keep no-registry/no-virtual-table).
- Retarget `adr102_information_schema_dispatch_is_explicit` (+`schemata` constant + arm; keep
  unknown-relation rejection; no `HashMap`/registry).
- Retarget `adr102_system_table_execution_stays_coordinator_local` (assert `InformationSchemaSchemata`
  present in the IR/binder path, absent from storage/PBC/execution-time listing).
- Add `adr107_schemata_is_descriptor_derived_not_a_registry` — the row builder uses `BTreeSet` over
  `descriptor.namespace`, does not decode Arrow schemas / logical types, the schema helper has no
  `catalog_name`/`schema_owner`, and the binder introduces no `CREATE SCHEMA` / `SystemTableRegistry`.

`adr102_table_descriptor_refresh_is_authoritative` already covers the refresh invariant — stays green.

## Council history

Iter-0: Gemini led `schemata`; Codex recommended it with the descriptor-backed scope clarification; Claude
retracted his own iter-0 "inert" objection (non-default namespaces *are* user-creatable via two-part DDL
names) and leaned `schemata`, surfacing SQL `CASE` as the honest higher-value swing. Operator locked
`schemata`. Iters 1–2 converged on one contract (4th finite `SystemTableKind`, `schema_name`-only,
`BTreeSet` distinct descriptor namespaces, fail-closed refresh, no Wendelon/storage/PBC surface). **Codex's
iter-2 added the completeness items the others missed:** the third ADR 102 guard retarget
(coordinator-local enumerates variants), the `system_table_executes_without_storage_scan` retarget, and the
explicit refresh-failure gate. Genuine 3-of-3.

## What to do next

SQL `CASE` is the strongest capability swing (no conditional substrate today). Derived sort/limit and
joins-around-derived are the next derived-source continuations. Further introspection (constraints, indexes,
views, PK/FK) stays gated on constraint/view modeling.

## Promotion criteria

`SELECT schema_name FROM information_schema.schemata` returns the current tenant's distinct latest-descriptor
namespaces (sorted); empty tenants → zero rows; refresh failure fails binding; unqualified `schemata` stays a
normal table; `information_schema.views` rejects; read-only protections include `schemata`; query operators
compose; execution stays coordinator-local; the three ADR 102 guards + two behavior sentinels are reconciled;
no `ExecutionPlan`-variant/walker/Wendelon/storage/PBC/executor surface added; Toron suite green. Migration:
`plans/impl/107_information_schema_schemata.md`.
