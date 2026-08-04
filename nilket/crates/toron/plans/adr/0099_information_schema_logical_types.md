# ADR 099: `information_schema.logical_types` (read-only introspection)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on the first
read-side synthetic relation exposing the durable logical-type registry. Two minor points resolved to
Codex's grounded positions (reflected in Codex's dual draft): **bind-time row materialization** (catalog
refresh belongs in the binder, not the executor — Gemini's phrasing leaned executor-time) and
**`BIGINT` id / SQL-keyword `physical`** (more SQL-idiomatic than Gemini's `UInt32`/snake_case). **Topic
operator-locked** (pivot off the elision arc; pass-through-projection → ADR 100, its demotion proven).
**Toron-only.** **Predecessors:** 090–098 (the type system this makes inspectable), 095 (`EmptyRelation`
— the coordinator-local-leaf precedent).

## Doctrine — the type system becomes inspectable; a virtual table is a local frontend concern

090–098 made logical types durable, enforced, and elision-bearing — but **opaque** (no way to query
what's defined). 099 adds the first **system relation**: `information_schema.logical_types` renders the
synced durable registry. Virtual tables produce rows from local coordinator state — no storage scan, no
Wendelon RPC at execution, no worker dispatch, no PBC.

## Grounding

- **No precedent; the FROM seam.** Zero `information_schema`/virtual-table hits in `crates/toron/src/`.
  `lower_table_factor` (`sql_planner.rs:3310`) lowers a FROM name → `LogicalPlan::ScanNamed { table }`
  (via `lower_table_ref_object_name`); the binder's `ScanNamed` arm (`binder.rs:92-93`) calls
  `self.catalog.resolve_table(table)` — the interception point.
- **Data source (synced).** `Catalog::logical_type_registry()` → per-tenant `LogicalTypeRegistry { by_id,
  by_name }` (`ir.rs:87`) of durable `LogicalTypeSpec { id, name, physical, refinement }`.
- **Row idiom + node precedent.** `materialize_insert_batch` (`write.rs:32`) converts
  `Vec<Vec<ScalarValue>>` → `RecordBatch`; `ExecutionPlan::EmptyRelation` (095, `ir/execution.rs:179`)
  is the coordinator-local-leaf + ~15-site walker-sweep precedent.

## Decisions (promotion-locked)

**D1 — Reserved read-only `information_schema` namespace; one relation.** `information_schema.logical_types`
(ASCII case-insensitive) is the only relation; unqualified `logical_types` stays a normal catalog
lookup; unknown `information_schema.<other>` rejects (not treated as a Wendelon table). **D2 — Bind-time
interception; no `LogicalPlan` variant.** `lower_table_factor` keeps emitting `ScanNamed`; the binder's
`ScanNamed` arm intercepts the reserved ref **before** `catalog.resolve_table` and emits the system
relation — planner stays simple, catalog access stays in the binder. **D3 — `ExecutionPlan::SystemTable
{ kind: SystemTableKind, schema, rows: Vec<Vec<ScalarValue>> }`.** `kind = InformationSchemaLogicalTypes`
only (kind-enum, narrow — no general framework, no reserved slots). A leaf like `EmptyRelation`: rows
**baked at bind**, materialized coordinator-local via the literal-rows idiom; no scan/RPC/worker/PBC at
execution. (Bind-time, not executor-time: the executor owns execution/storage, not catalog refresh.)
**D4 — Stable schema:** `logical_type_id BIGINT NOT NULL` (the `u32` widened), `name TEXT NOT NULL`,
`physical TEXT NOT NULL` (SQL carrier keyword `INT`/`BIGINT`/`REAL`/`DOUBLE`/`TEXT`/`BOOLEAN` — matches
authoring), `refinement TEXT NULL` (normalized `CHECK (…)` or `NULL`). No tenant-id/generation/
fingerprint/seed/internals. **D5 — Durable + tenant-scoped; seed excluded.** Rows from the current
tenant's durable registry only; static seed/`naked` excluded (the 096 user contract). Add
`LogicalTypeRegistry::specs_sorted()` for deterministic ordering without private-map access. **D6 —
Refresh before materialize.** `Catalog::refreshed_logical_type_registry()` — default returns the current
registry; a Wendelon-backed scoped catalog lists durable definitions for the tenant, registers, returns
the refreshed snapshot; **a list failure fails the query** (no stale partial). Mirrors 091/092
refresh-before-unknown. **D7 — Read-only namespace.** `information_schema` is valid only as a SELECT
*source*; mutation/DDL *targeting* `information_schema.*` rejects (CREATE/CTAS-target/DROP/INSERT/UPDATE/
DELETE). `CREATE TABLE copy AS SELECT * FROM information_schema.logical_types` is **valid** (source).
**D8 — Refinement rendered from the AST.** Normalized `CHECK (<expr>)` from `LogicalTypeRefinementPredicate`
(`VALUE`, SQL comparison ops, escaped literals, `TRUE`/`FALSE`, `AND` in stored order) — introspection
text, not the user's original formatting. **D9 — Composes for free** (lowers to a normal relation:
`WHERE`/projection/`ORDER BY`/`COUNT(*)`/join/CTAS-source). **No cross-ring surface** — refreshes through
the existing catalog-sync path, then executes coordinator-local; no Wendelon contract/PBC/wire/storage
change; the new `ExecutionPlan` variant rides the ~15-site walker sweep (the bulk).

## Behavior gate

`information_schema_logical_types_lists_current_tenant_durable_definitions` (`SELECT … ORDER BY
logical_type_id` → stable rows); `refinement_column_renders_check_text_or_null`;
`physical_column_renders_sql_carrier_keyword`; `information_schema_logical_types_is_tenant_scoped`;
`information_schema_logical_types_excludes_seed_and_naked`;
**`information_schema_logical_types_refreshes_before_read`** (a Wendelon-backed def not yet locally
resolved appears after the refresh hook); `unqualified_logical_types_is_normal_table_lookup`;
`unknown_information_schema_relation_rejects`; **`information_schema_namespace_is_read_only`**
(mutation/DDL target rejects; CTAS-from-source valid);
`select_with_where_projection_orderby_over_information_schema_composes`;
`system_table_executes_coordinator_local_without_storage_scan_or_pbc`.

## Architecture guards

`adr099_information_schema_logical_types_intercepts_before_catalog_resolve_table`;
`adr099_system_table_is_coordinator_local_scalar_rows` (`SystemTable { kind, schema, rows }`, not a
Wendelon table / storage scan / SQL `VALUES` surface);
`adr099_logical_types_rows_from_refreshed_durable_registry_not_seed`;
`adr099_information_schema_namespace_is_reserved_read_only`;
`adr099_system_table_walkers_handle_new_variant` (`schema()`/optimize/fragment/scheduler/server + the
~15-site sweep, no `_` wildcard); `adr099_no_wendelon_pbc_wire_or_storage_surface`;
`adr099_scoped_to_logical_types_only` (one `SystemTableKind`; no general framework / reserved slots).

## Scale boundary & seam check

Toron-only; the **first read-side synthetic relation** — one coordinator-local `ExecutionPlan` variant
(the 095 `EmptyRelation` precedent: variant + walker sweep + literal-rows materialization). Reads the
refreshed durable registry; no Wendelon contract/wire/PBC/executor-kernel change. The `SystemTable`
node is self-contained — touches no load-bearing seam.

## What to do next

- **ADR 100 — pass-through-projection provenance** (the deferred elision follow-on; its real consumer —
  subquery-`FROM`/views — makes `Filter(Project(Scan))` SQL-reachable; revisit then, or pair).
- Further introspection (`information_schema.tables`/`columns`) once a 2nd system relation justifies
  generalizing `SystemTableKind`. Carried: `DROP`/`ALTER LOGICAL TYPE`; Rune `Like`/`ILike`; the
  Wendelon `*V1` sweep (Wendelon-home chore).

## Council history

098 closed the elision arc; 099 iter-0/1: Claude **proved** pass-through-projection has no SQL consumer
(`SELECT id FROM people WHERE id<0` already elides — `lower_select` puts Project above Filter); operator
locked the `information_schema.logical_types` pivot (and prefers message-steer over interactive prompts).
iter-2: Claude grounded the virtual-table substrate (EmptyRelation precedent + literal-rows idiom +
synced registry). iter-3 (dual): consolidated with Codex's **bind-time interception** (no `LogicalPlan`
variant), **refresh-before-materialize**, and `specs_sorted`; resolved bind-time-over-executor-time +
`BIGINT`/SQL-keyword representation; 3-of-3. Migration: `plans/impl/0099_information_schema_logical_types.md`.

## Promotion criteria

`SELECT … FROM information_schema.logical_types` lists the current tenant's durable definitions
(id/name/physical-keyword/refinement-`CHECK`-text-or-null), refreshed-before-read, seed excluded;
unqualified `logical_types` stays a normal table; unknown `information_schema.*` + mutation/DDL targets
reject (CTAS source valid); `WHERE`/projection/`ORDER BY` compose; executes coordinator-local with no
storage scan / Wendelon RPC during execution / PBC; the `SystemTable` variant handled across the ~15
walkers; no cross-ring surface. Migration: `plans/impl/0099_information_schema_logical_types.md`.
