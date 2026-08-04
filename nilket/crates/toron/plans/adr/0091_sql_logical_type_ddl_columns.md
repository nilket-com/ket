# ADR 091: SQL Logical Type Use-Sites — DDL Column Position

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified against the
latest peer drafts (Codex iter-1, Gemini iter-2). **DDL column position only** is unanimous;
`CAST(x AS CustomerId)` + typed CTAS projection are 092 (different machinery — coercing-ascription).
**Back to Toron-only** — 091 *consumes* the 090 substrate (the cross-ring breach was a one-time
090 event); no Wendelon change.
**Category:** Toron SQL-frontend / catalog→binder type-system continuation.
**Predecessor:** ADR 090 (durable definition substrate, landed `89d5d4d`).
**Scope:** `ir.rs`, `sql_planner.rs`, `binder.rs`, `catalog.rs`, tests. **No Wendelon
descriptor-metadata parsing, no executor/PBC change, no new nominal-gate logic** (087/088 enforce
the resulting identity unchanged).

## The payoff

`CREATE LOGICAL TYPE CustomerId AS BIGINT; CREATE TABLE orders (customer_id CustomerId NOT NULL)`
— the first honest SQL use-site: the column is physically `Int64`, carries the durable
`LogicalTypeId` via the 089 writer, and the 087/088 gate protects it. **No hand-built Arrow
metadata, no static seed acting as SQL truth, no name heuristic** — the headline the 087→090 arc
was building toward.

## The resolution-timing plan-shape shift

Grounded: `SqlPlanner::plan` is **sync and catalog-less** — it can validate syntax but cannot
resolve a tenant-scoped durable type name; and `LogicalPlan::CreateTable` currently carries a
fully-lowered `SchemaRef`, so `CustomerId` has nowhere honest to live between parse and bind. So
091's load-bearing work is a resolution-timing shift: **the plan carries the type *name*
unresolved; the binder resolves it at bind** (async, has the catalog), then emits a resolved
`ExecutionPlan::CreateTable`. Execution stays boring — it never learns about unresolved names.

## The seed-first trap (the load-bearing finding)

`LogicalTypeResolver::spec_by_name` (`ir.rs:204`) resolves **seed first, registry second**
(`seed_logical_type_spec_by_name(name).or_else(|| registry…)`), and `resolve_logical_type_by_name`
(`catalog.rs:1116`) rides it. That is **correct for descriptor decode** (Rune/test descriptors
carry seed ids and must resolve) but **wrong for SQL DDL**: reusing it would let
`CREATE TABLE t (id CustomerId)` resolve against the static pilot (id=1) with **no `CREATE LOGICAL
TYPE`** — silently making the seed "SQL truth," the exact receipt-doctrine violation 090 forbade
([[logical-identity-receipt-doctrine]]). So **SQL DDL resolution must be a registry-only lookup
that bypasses the seed.** Two paths, two policies:

| path | resolution | why |
|---|---|---|
| descriptor decode (088/090) | seed ∪ registry (seed-first) | Rune/test descriptors carry seed ids |
| **SQL DDL name (091)** | **registry-only (seed excluded)** | a SQL name is "truth" only with a durable receipt |

## Decisions (10, promotion-locked)

**D1 DDL-only** — `CREATE TABLE` column-position logical types; no `CAST`/typed-CTAS authoring,
PK/FK derivation, refinements, or Wendelon descriptor-metadata parsing (all later). **D2
unresolved logical plan, resolved execution plan** — `LogicalPlan::CreateTable` carries
`CreateTableSchema { columns: Vec<CreateTableColumn { name, nullable, column_type:
CreateTableColumnType::{ Physical(DataType), LogicalName(String) } }> }`; `ExecutionPlan::CreateTable`
keeps a resolved `SchemaRef`. **D3 planner syntax only** — primitives → `Physical`;
`SqlDataType::Custom(name, mods)` → `LogicalName(name)` only when one-part, unquoted ASCII,
empty modifiers, not `naked` (case-insensitive), preserving the exact authored case for registry
lookup; else a bounded plan-time error. **D4 bind-time resolution** — the
binder resolves `LogicalName` via async catalog before emitting the execution plan. **D5
durable-only (registry-only) lookup** — SQL DDL name resolution **bypasses the seed**; it must
not call seed-first `LogicalTypeResolver::spec_by_name` or the top-level
`logical_type_spec_by_name`; direct `LogicalTypeRegistry::spec_by_name` or an explicit
durable-only resolver is allowed (the seed-first trap). **D6 refresh before unknown** —
`WendelonCatalog` durable lookup checks the local tenant registry,
then refreshes via 090's `list_logical_type_definitions` before returning not-found (fresh-process
usable); transport/admin failures are bounded errors, not false unknowns. **D7 write-side
fail-closed** — undefined name → `CatalogError::LogicalTypeNotFound { name }` *before* any
descriptor is submitted (the write-half of 090's co-authority invariant; a dedicated typed
variant, not stringly `AdminFailed`). **D8 field writer reuse** — resolved `(physical, id)` →
`field_with_logical_type_metadata(col.name, spec.physical, nullable, spec.id)` (089 writer; DDL
joins its guard's call-site list); primitives use `Field::new`, stay naked. **D9 create-path
self-rebind is resolver-aware** — `MockCatalog`/`WendelonCatalog::create_table_with_outcome`
rebind the fresh descriptor via `from_descriptor_with_logical_types(…, with_registry(tenant))`
(today they use seed-only `from_descriptor` at `catalog.rs:636`), so a valid create does not
reject its own durable-id output. **D10 no new nominal gate** — comparison/join/IN/projection/CTAS
keep using 087/088/089 once the table exists; no name heuristics (`id BIGINT` naked, `id
CustomerId` non-naked).

## Behavior gate

define+use (`CREATE LOGICAL TYPE CustomerId AS BIGINT; CREATE TABLE orders (customer_id CustomerId
NOT NULL)` → non-null Int64 + durable metadata, resolved from the registry); two ids
(`customer_id CustomerId, product_id ProductId` → bare `customer_id = product_id` rejects via
087/088); `= 7` literal policy holds; **undefined-type fails closed AND the static seed does not
satisfy it** (`CREATE TABLE (id CustomerId)` with only the seed present → reject — the seed-first
trap pinned); `id BIGINT` naked; `naked`/modifiers/quoted/multi-part reject at plan time; typed
table **survives resync** with identity (090 durability ∘ real DDL); **fresh-process** type
resolves via the Wendelon list refresh; **`CAST(x AS CustomerId)` still rejects** (091/092 line);
087/088/089/090 + all-naked create_table/ctas/group_by green.

## Architecture Guards

`adr091_create_table_carries_unresolved_logical_type_names_to_bind`;
`adr091_execution_create_table_stays_resolved_schema`;
`adr091_sql_planner_records_custom_type_names_without_catalog_resolution`;
`adr091_ddl_logical_type_resolution_is_registry_only_not_seed_first` (the seed-first trap —
backed by the registry-only lookup, **not** seed-first `LogicalTypeResolver::spec_by_name` or
`logical_type_spec_by_name`);
`adr091_wendelon_lookup_refreshes_logical_type_list_before_unknown`;
`adr091_ddl_logical_column_uses_089_metadata_writer`;
`adr091_catalog_create_rebinds_logical_descriptors_with_resolver` (no seed-only self-rebind);
`adr091_undefined_logical_type_fails_before_descriptor_submit`;
`adr091_no_cast_or_typed_ctas_authoring_yet`. **Reconcile** the guards that pin
`LogicalPlan::CreateTable { schema: SchemaRef }`, the seed-only create rebind, or custom
`CREATE TABLE` types as universally rejected (discipline #4 — do the reconcile *in this change*;
090's review found the round shipped a red suite by skipping it). Carry 087/088/089/090 guards.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **ADR 092 — `CAST(x AS CustomerId)` + typed CTAS projection** (the direct successor). Owns the
  *coercing-ascription*: a `coerce` flag on `Expr::LogicalAscription`, reconcile against the
  operand's identity, **reconcile-before-erase** (binds iff `T` known [via the same durable-only
  resolver] ∧ physical cast supported ∧ no conflict; `CAST(product_id AS CustomerId)` rejects;
  `CAST(CAST(product_id AS BIGINT) AS CustomerId)` is the explicit override), physical coercion via
  the existing numeric lattice; then CTAS materialization composes it with 089. CAST resolution
  reuses 091's durable-only lookup at its own bind site.
- **PK/FK-derived identity** ("magic that follows constraints, not names" — derive from `PRIMARY
  KEY`, inherit via `REFERENCES`; needs FK support + a canonical identity name).
- **`DROP`/`ALTER LOGICAL TYPE`**, `information_schema.logical_types` view, refinement contracts
  (value predicates → static elision), the optional Wendelon-validate-at-authority (090 knob).
- Carried Rune leads (Like/ILike, joins); non-Rune residual minimization (078 follow-on).

## Council History

iter-0: Claude seeds DDL-only + flags the resolution-timing challenge; Codex agrees, names the
plan-shape + the create-rebind catch + bind-time + refresh-on-miss. iter-1: Claude adopts those
and **grounds the seed-first trap** (durable-only needs a registry-only lookup, else the seed
becomes SQL truth) — adopted unanimously (Codex D5, Gemini D4). iter-2: converged. Migration:
`plans/impl/0091_sql_logical_type_ddl_columns.md`.

## Promotion criteria

`CREATE TABLE (col CustomerId)` resolves a **durably-defined** `CustomerId` at bind (registry-only,
seed excluded), builds the field via the 089 writer, and persists; the create rebinds its own
descriptor resolver-aware; undefined names fail closed before submit; a durable type resolves in a
fresh process (Wendelon list refresh); the static seed never satisfies a SQL name; `CAST` still
rejects (092); no Wendelon/executor/PBC change; the variant/shape guards are **reconciled** (suite
green, not red); 087/088/089/090 + all-naked suites green. Migration:
`plans/impl/0091_sql_logical_type_ddl_columns.md`.
