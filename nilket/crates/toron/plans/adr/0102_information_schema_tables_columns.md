# ADR 102: `information_schema.tables` / `.columns` (authoritative)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on topic + **Fork B**
(operator-locked): authoritative refreshed tenant descriptor listing, not local-cache-only. The 2nd/3rd
read-side system relations, generalizing 099's `SystemTableKind`. **Adds one read-only cross-ring listing
RPC** — the first new cross-ring read surface since 090, and the point of the ADR (a reserved SQL
introspection relation must be truthful). **Predecessors:** 099 (`SystemTable` substrate +
refresh-before-read), 087-098 (the logical identity that `columns.logical_type_name` surfaces).
**Migration:** `plans/impl/0102_information_schema_tables_columns.md`.

## Doctrine — introspection must be authoritative, not a cache snapshot

099 made the durable type system inspectable and set the rule: a synced introspection view **refreshes
before read**, and a list-failure fails the query (no stale partial). `information_schema.tables`/`.columns`
extends that to the table catalog. Local-cache-only (a) was rejected on grounding: Toron's
`WendelonDescriptorRegistry` is populated lazily by bind-miss, so (a) would mean "tables this coordinator
touched," not "tables in this tenant" — a misleading SQL catalog surface that contradicts 099 (D6). The
authoritative listing is the only correct cut; its cost — a cross-ring read RPC — is the ADR, not scope
creep.

## Grounding (verified)

- **099 substrate.** `ExecutionPlan::SystemTable { kind, schema, rows }` (coordinator-local leaf);
  `SystemTableKind` (`ir/execution.rs:178`) has one variant; `bind_information_schema_relation`
  (`binder.rs:675`) intercepts the reserved namespace before `resolve_table` and currently accepts only
  `logical_types`; the read-only namespace guard already rejects mutation/DDL targets. **Adding kinds does
  not add an `ExecutionPlan` variant → no walker sweep** (unlike 099).
- **No authoritative source today.** `Catalog`/`DescriptorSyncClient` (`catalog.rs:298`) expose per-table
  `get_table_schema_descriptor`; no tenant listing. Wendelon's `descriptors_for_tenant`
  (`wendelon/schema/table.rs`) walks **all** generations (`schema_generation: 0..=u64::MAX`) → would
  duplicate rows; the `latest_by_name` index (`:2061`, maintained by `upsert`) is the authoritative
  latest-only source.
- **Decode is reuse.** `descriptor.arrow_schema()` (`catalog.rs:104`) decodes `arrow_schema_ipc` → `Schema`;
  `decode_column_logical_type_with_resolver(field, registry)` (`:2093`) resolves a field's logical type;
  `physical_keyword` (`binder.rs:3292`) is the closed SQL-keyword carrier mapping. `.columns` reuses all
  three, with unsupported carriers treated as binding errors rather than `data_type` row text.

## Decisions (promotion-locked, 3-of-3)

**D1 — Authoritative listing branch.** Refresh the tenant's table descriptor list before materializing
`tables`/`columns` rows. No local-cache-only variant.

**D2 — Narrow cross-ring read surface.** Add a read-only Wendelon→Toron client operation
`list_table_schema_descriptors(tenant) -> Vec<TableSchemaDescriptor>`: descriptor metadata only — no row
data, stats slabs, PBC programs, or scan results. A Control-Ring leader read, mirroring the existing
`list_logical_type_definitions` / per-table descriptor fetch (auth + tenant-scoping + error mapping the
same). Unversioned name (no `*V1`), reusing the existing per-descriptor codec.

**D3 — Latest-only.** The listing returns the **latest name-bound** descriptor per tenant table, built on
Wendelon's `latest_by_name` index (not raw all-generations `descriptors_for_tenant`). Historical
generations must not produce duplicate rows.

**D4 — Refresh before materialize (099 D6).** Add `Catalog::refreshed_table_schema_descriptors() ->
Result<Vec<TableSchemaDescriptor>, CatalogError>` (reuse the descriptor type; no gratuitous carrier). The
Wendelon-backed impl: (1) refresh+register durable logical types (so descriptor logical metadata decodes),
(2) list latest table descriptors, (3) sync/register locally, (4) return a deterministic latest-only
snapshot sorted `(namespace, table_name)`. **List failure fails the query** (`CatalogError::AdminFailed`)
— no stale partial.

**D5 — Finite `SystemTableKind`, no framework.** Add `InformationSchemaTables` + `InformationSchemaColumns`
(three total). No trait/registry virtual-table framework this cycle (099 D3's "narrow, no reserved slots";
a framework over three variants removes no real complexity). `bind_information_schema_relation` dispatch
stays an explicit case-insensitive `match`, no fall-through. **No new `ExecutionPlan` variant → no walker
sweep** (verify via `rg "SystemTableKind::"`: the kind is consumed only at the binder dispatch).

**D6 — `information_schema.tables` schema.** `table_schema TEXT NOT NULL` (namespace), `table_name TEXT NOT
NULL`, `table_type TEXT NOT NULL` (always `BASE TABLE` this cut; don't self-list `information_schema.*`).
No tenant-id/`table_object_id`/generation/fingerprint/locality/stats/row-identity/evolution fields.

**D7 — `information_schema.columns` schema.** Ordered by `(table_schema, table_name, ordinal_position)`:
`table_schema TEXT`, `table_name TEXT`, `column_name TEXT`, `ordinal_position BIGINT` (1-based), `data_type
TEXT` (SQL carrier keyword via `physical_keyword`; unsupported Arrow type → explicit binding error, not a
debug-string `data_type` contract), `is_nullable TEXT` (`YES`/`NO`), `logical_type_name TEXT NULL`. The
last is the 087-098 tie-in: `NULL` for naked columns; for annotated columns the durable name resolved through
`LogicalTypeResolver::with_registry(&registry).spec(id).name` — **not** `logical_type_name(id)` fallback
text and **not** a column-name heuristic. Columns come from decoding each descriptor's `arrow_schema_ipc`.
Defer defaults/constraints/PK-FK/collation/precision.

**D8 — Namespace semantics intact (retarget 099 guards).** `logical_types` unchanged; `tables`/`columns`
are SELECT sources; unqualified `tables`/`columns` stay normal lookups; unknown `information_schema.<name>`
rejects; mutation/DDL targets reject (CTAS-from valid).

**D9 — Coordinator-local execution.** Rows baked at bind after refresh into `ExecutionPlan::SystemTable`;
execution does no descriptor listing / storage scan / table-query RPC / PBC / worker dispatch. (The
listing RPC happens at **bind**, not execution.)

## Behavior gate

`information_schema_tables_lists_authoritative_tenant_tables` (a Wendelon table never resolved locally
appears after refresh); `information_schema_columns_lists_authoritative_columns` (1-based ordinal, SQL
carrier `data_type`, `YES`/`NO`); `information_schema_columns_renders_logical_type_names` (durable
annotation renders after the logical-type refresh — proves refresh ordering);
`information_schema_tables_columns_are_tenant_scoped`;
**`information_schema_descriptor_listing_failure_fails_query`** (RPC failure fails the query, no stale
rows); **`information_schema_tables_columns_are_latest_only`** (evolved generations don't duplicate);
`unqualified_tables_and_columns_are_normal_table_lookups`; `unknown_information_schema_relation_still_rejects`
(`information_schema.schemata` unsupported); `information_schema_namespace_remains_read_only`
(targets reject; CTAS-from valid); `information_schema_tables_columns_compose` (WHERE/projection/ORDER
BY/COUNT(*)/`.tables`⨝`.columns` join); `system_table_tables_columns_execute_without_storage_scan_or_pbc`.

## Architecture guards (new + **retargeted 099**)

**Retarget three 099 guards** (`nominal_type_identity.rs`) — else the 102 feat breaks them:
- `adr099_no_wendelon_pbc_wire_or_storage_surface` → **narrow to "no execution-time storage scan / PBC /
  worker / table-query RPC"** (102 intentionally adds a *bind-time control-plane* descriptor-list read; the
  data-plane invariant stands, the absolute-no-Wendelon one does not — inverted-guard discipline #4).
- `adr099_scoped_to_logical_types_only` → `adr102_system_table_kind_has_finite_introspection_family`
  (exactly the three `InformationSchema*` variants).
- `adr099_information_schema_namespace_is_reserved_read_only` → still asserts target rejection; valid
  sources now include `tables`/`columns`.

**New `adr102_*` guards:**
- `adr102_information_schema_dispatch_is_explicit` (binder matches all three; unknown rejects before
  `resolve_table`).
- `adr102_table_descriptor_refresh_is_authoritative` (`Catalog` exposes the refresh hook; Wendelon-backed
  calls the listing client and fails on list error).
- `adr102_wendelon_descriptor_list_is_latest_only_metadata` (Wendelon listing built on `latest_by_name`,
  latest-only, read-only, metadata-only — no row/stats/PBC/scan data).
- `adr102_columns_decode_logical_types_after_refresh` (decoded via the refreshed registry resolver, not
  seed-only / not name heuristic / not `logical_type_name(id)` fallback).
- `adr102_system_table_execution_stays_coordinator_local` (no new `ExecutionPlan` variant; executor only
  materializes baked rows; no storage scan / PBC / worker / execution-time RPC).

## Scale boundary & seam check (honest)

Adds **one read-only cross-ring RPC** (Toron `DescriptorSyncClient::list_table_schema_descriptors` +
Wendelon Control-Ring endpoint + the `latest_by_name`-backed listing + the wire message) — bounded,
read-only, metadata-only, no mutation, no admin choreography, refreshed through the existing sync
discipline. The Toron side is otherwise 099-pattern reuse: two `SystemTableKind` kinds + two binder arms +
descriptor decode reuse. **No new `ExecutionPlan` variant, so no walker sweep.** No storage/executor/PBC
change.

## What to do next

- `information_schema.schemata` / constraints / indexes / PK-FK / views — later, once constraint/namespace
  modeling justifies it.
- Carried: derived-table alias column lists (`AS p(pid)`, pure `sql_planner`); Wendelon `*V1` de-version
  sweep (Wendelon-home, audit taxonomy); `DROP`/`ALTER LOGICAL TYPE` + PK/FK identity; Rune joins / regex
  pattern families.

## Council history

101 promoted Rune `LIKE`/`ILIKE`; 102 was the pre-teed introspection follow-on. iter-0: all three picked
the topic; Claude ruled out (a) local-cache on grounding (lazy registry → violates 099 D6) and framed the
live choice as (b)-now vs defer; Codex independently rejected (a) as `toron_local.*`-only; Gemini ratified.
Operator locked (b). iter-1: Claude grounded the latest-only mechanism (`latest_by_name`, not
all-generations `descriptors_for_tenant`) and sided with Codex on finite variants (vs Gemini's framework);
Codex consolidated D1–D9. iter-2 (dual): Claude grounded the decode-reuse anchors (`descriptor.arrow_schema()`
/ `decode_column_logical_type_with_resolver` / `physical_keyword`) + the no-walker-sweep simplification;
Codex grounded the Wendelon Control-Ring wire shape + the 099 guard retargeting (esp.
`adr099_no_wendelon_pbc_wire_or_storage_surface` must narrow) + the `logical_type_name(id)`-fallback
hazard. iter-3 (final): Claude folded the guard retargeting in; 3-of-3. Migration:
`plans/impl/0102_information_schema_tables_columns.md`.

## Promotion criteria

`SELECT … FROM information_schema.tables`/`.columns` lists the tenant's authoritative latest descriptors
(schema/name/type; column name/ordinal/SQL-type/nullability/logical-type-name), refreshed-before-read,
tenant-scoped, latest-only; list-failure fails the query; unqualified `tables`/`columns` stay normal
tables; unknown `information_schema.*` + mutation targets reject (CTAS-from valid); composes; executes
coordinator-local (the listing RPC at bind, not execution); the cross-ring listing is read-only metadata
built on `latest_by_name`; the three 099 guards are retargeted (no execution-time Wendelon/storage/PBC
surface); no new `ExecutionPlan` variant; Toron + Wendelon suites green. Migration:
`plans/impl/0102_information_schema_tables_columns.md`.
