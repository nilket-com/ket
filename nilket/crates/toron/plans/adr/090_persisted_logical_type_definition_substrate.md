# ADR 090: Persisted Logical Type Definition Substrate

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified against the
latest peer drafts (Codex iter-1-revised, Gemini iter-3, Claude iter-3). **The arc's first
cross-ring ADR** — a deliberate, bounded exception for one foundational concern: *durable type
receipts*. **Operator design call (locked):** definition-before-use (**Cut B** — substrate now,
SQL use-sites → 091), **Wendelon-native authority** (not a Toron-managed data table, not session
state), keyword **`CREATE LOGICAL TYPE`** (not `CREATE DOMAIN`), PK/FK-derived identity deferred.
**Category:** cross-ring substrate (Wendelon storage authority + Toron consumer) / nominal-type
system.
**Scope:** Wendelon `integration::compute` + schema-admin/catalog surfaces (new schema-object
kind + durable object payload/sync contract), Toron `ir.rs`, `catalog.rs`, `storage.rs`,
`sql_planner.rs`, `coordinator.rs`, tests. **SQL DDL/CAST use-sites deferred to ADR 091. No
executor/PBC change, no Rune-surface change, no new `Expr`/`BoundExprNode`.**

## The invariant (headline)

> **A descriptor may reference only a logical type id whose definition is durably stored in the
> same metadata authority as the descriptor.**

088 persists a bare `LogicalTypeId` into descriptor field metadata. If the `id → name → physical`
definition were session-local or hardcoded, a persisted descriptor's id would decode against
nothing next session — a *dishonest* decode (a previously-valid table becomes unbindable, or
silently mis-typed). The fix is to co-locate the definition with the descriptor in the **same
authority** — the tenant's Wendelon schema store. This ADR builds that authority. It does **not**
add SQL use-sites (091).

## Doctrine — receipts, and definition before use

*Magic may enforce only when it has a receipt* (see [[logical-identity-receipt-doctrine]]). A
`CAST(x AS CustomerId)` or `customer_id CustomerId` is a **use-site** that presupposes a
**definition** receipt. The definition must be durable, tenant-scoped, and persisted in the same
authority as the tables that reference it. So we build the definition substrate first (090) and
expose the SQL use-sites on the proven substrate (091). The static pilot registry was silently
playing "the definition," and it isn't a real one — a SQL user must never have to ask "why is
`CustomerId` an `Int64`?"; the answer must be a durable definition, not a hardcoded pilot.

## Why cross-ring, and why bounded

Grounded (`catalog.rs`): the production `WendelonCatalog` is a **synced view of Wendelon-stored
descriptors**; Toron holds **no durable state of its own** (cross-ring discipline: Wendelon owns
storage + admin choreography, Toron makes RPCs — see [[cross-ring-admin-orchestration-boundary]]).
So a durable registry has no Toron home — it must live in Wendelon. **That is the cross-ring
exception.** It is bounded because Wendelon already has the framework: `integration::compute`
exposes `SchemaObjectKind { Namespace, Table, Index, View }`, `SchemaMutationKindV1 { Create,
Alter, Drop, … }`, `SchemaGeneration`, `SchemaIntentOrigin`, all `TenantId`-scoped. A logical-type
registry is a **new `SchemaObjectKind::LogicalType`** riding the *same* schema-intent / `Create` /
generation / persistence / sync machinery tables use — not a new storage subsystem.

**Authority vs representation (Codex's split).** Separate *who is the source of truth* from *how
the bytes are physically stored*:

- **Authority = Wendelon, as schema metadata** — allocates the id, persists `{id, name, physical}`,
  exposes it through the **admin/sync surface** (metadata plane). Toron resolves it the way it
  resolves descriptors, **never through the query/data path**.
- **Physical representation = flexible, Wendelon-internal** — Wendelon *may* store the registry in
  an internal catalog table or object store; that's an implementation detail invisible to Toron.
- **Rejected:** a **Toron-managed _ordinary_ table** as the authoritative registry, read through
  the bind/query path — it forces type resolution onto the data plane, creates descriptor/registry
  race + transaction problems, and lets descriptors be persisted referencing ids the authority
  never validated.

**Awareness precision (resolved).** 090 enforces the reference invariant **Toron-side** — at write
(091 use-sites refuse an undefined id) and read (088 decode fails closed) — because that is where
the binder / type-checker already lives and it needs **zero new Wendelon descriptor-parsing**.
This is a *where-to-enforce* choice, **not** a Wendelon-capability one: Wendelon is a storage ring
that already holds real compute (e.g. KLL approximate-quantile kernels), so "Wendelon can't/shouldn't
understand things" is the wrong axis. The honest axis is **data-plane compute** (quantiles over the
bytes it holds — squarely Wendelon's job) **vs. interpreting a Toron _metadata_ convention** (the
`nilket.toron.logical_type_id` field-metadata key, which 088 deliberately stores opaquely). Codex's
stronger **validate-at-authority** (Wendelon checks descriptor references against the registry it
owns — proper write-time referential integrity, well within a capable storage ring) is a legitimate
enhancement, kept **open for 091**; the only thing it changes is that Wendelon would start
interpreting that metadata key. 090 simply doesn't *require* it — Toron-side enforcement is the
smaller cut.

## Decisions (10, promotion-locked)

### Wendelon-owned authority (D1–D3; the storage internals may land as paired Wendelon-sequence work — the council fixes the Toron-facing contract)

**D1** add `SchemaObjectKind::LogicalType`: a tenant-scoped `{ logical_type_id: u32, name,
physical: <primitive> }`, created via the existing `Create` intent (origin `ComputeClient`),
versioned (`SchemaGeneration`), persisted + listable for sync — exactly like table descriptors.
ADR 090 accepts **one-part, tenant-global logical type names** only; if the Wendelon schema-object
envelope requires a namespace, use a fixed/reserved logical-type namespace internally. Qualified or
schema-scoped logical type names are deferred.
**D2** **Wendelon allocates the durable `logical_type_id`**, stable across descriptor refresh and
restart, unique within the tenant. **D3** the read/list/sync surface mirrors descriptor sync.

### Toron consumer surface (D4–D10)

**D4** `StorageAdmin` gains `create_logical_type(tenant, name, physical) -> logical_type_id` (one
RPC, mirroring table-create choreography). Wendelon is the idempotency authority: Toron may consult
its synced cache for diagnostics, but duplicate-name/same-physical vs different-physical is decided
by the durable metadata authority so a stale Toron cache cannot mint a conflicting definition.
**D5** `CREATE LOGICAL TYPE <Name> AS <primitive>` — a
Toron SQL extension (sqlparser has neither it nor a usable `CREATE DOMAIN`/base-alias `CreateType`)
lowering to `LogicalPlan::CreateLogicalType`, executed at the coordinator via D4; physical ∈
`{Int64, Int32, Float64, Float32, Utf8, Boolean}`. **D6** `WendelonCatalog` syncs a per-tenant
`LogicalTypeRegistry { by_id, by_name }` from the durable objects. **D7 decode honesty (the
rewire):** 088's `decode_column_logical_type` + `logical_type_spec`/`_by_name` resolve against the
**union of the static seed** (Rune/tests, reserved range) **and the synced registry** (durable,
tenant-scoped); this threads tenant context into resolution via context-bearing resolver helpers
or explicit registry arguments. Do **not** introduce a process-global mutable registry. Seed-only
wrappers may remain for no-catalog Rune/test call sites. **D8 static pilot is a reserved-range
bridge:** `CustomerId=1`/`ProductId=2` stay a Rune/test seed in a reserved low range; Wendelon
allocation starts above it; production tables rely on the synced registry; the invariant binds
*persisted* descriptors, which never reference a seed id in production (no use-site until 091; Rune
ascription is query-local). **D9** `CREATE LOGICAL TYPE` validation: duplicate name + **same**
physical is idempotent-ok; duplicate name + **different** physical rejects; the `naked` sentinel is
never definable or resolvable by name; nominal only (name+id+physical, no refinements). **D10**
fail closed: a descriptor id absent from seed ∪ registry → bounded `CatalogError` (the invariant
made loud, not silent).

## Explicitly NOT in ADR 090 (→ 091+)

SQL **use-sites**: `CREATE TABLE … customer_id CustomerId`; `CAST(x AS CustomerId)` and its
**reconcile-before-erase** semantics (no silent relabel; `CAST(CAST(product_id AS BIGINT) AS
CustomerId)` is the explicit override); CTAS materialization of typed columns. Also: `DROP`/`ALTER
LOGICAL TYPE`, tenant-policy nuances, refinements (092+), **PK/FK-derived identity** ("magic that
follows constraints, not names" — the strong long-term path, which needs this durable id substrate
first), and a **read-only `information_schema.logical_types` introspection view** (read-only → no
data-plane authority crossing).

## Behavior gate (12 — define + read-back + decode honesty)

`create_logical_type_persists_durable_tenant_scoped_entry`;
**`logical_type_definition_survives_catalog_resync`** (the next-session honesty proof — read back
from Wendelon, not memory); `duplicate_definition_same_physical_is_idempotent` /
`_different_physical_rejects`; `create_logical_type_naked_sentinel_rejects` /
`_non_primitive_physical_rejects`; `decode_resolves_durable_id_against_synced_registry`;
**`descriptor_referencing_undefined_id_fails_closed`** (invariant guard);
`static_seed_still_resolves_for_rune` (087 surface green) / `static_and_durable_ids_are_disjoint`;
**`sql_create_table_custom_type_still_rejects`** / **`sql_cast_custom_target_still_rejects`** (pin
the 090/091 line); + all 087/088/089 and all-naked DDL/CTAS/group-by suites stay green.

## Architecture Guards

`adr090_logical_type_authority_is_wendelon_metadata_not_toron_query_path` (resolved via the catalog
admin/sync surface as schema metadata, **not** a Toron-managed ordinary data table read through the
bind/query path; a Wendelon-internal catalog table is an allowed representation);
`adr090_logical_type_id_allocation_is_wendelon_owned_and_durable`;
`adr090_decode_resolves_against_synced_registry_not_static_hardcode`;
`adr090_descriptor_id_definition_co_authority_invariant`;
`adr090_static_pilot_seed_is_reserved_range_rune_test_bridge_only`;
`adr090_no_process_global_dynamic_logical_type_registry`;
`adr090_create_logical_type_keyword_not_create_domain`; `adr090_naked_sentinel_not_definable`;
`adr090_no_sql_use_sites_yet` (DDL custom type / CAST custom target still reject);
`adr090_no_refinement_dropalter_or_pkfk_derivation`. Carry all 087/088/089 guards.

## Cross-ring scope note (honest)

This ADR breaches the Toron-only boundary 087–089 held — *once*, for the foundational
durable-receipt substrate. The Wendelon half (D1–D3) is bounded (a new `SchemaObjectKind` on
existing machinery) and may be paired Wendelon-sequence work; the Toron half (D4–D10) is the
consumer surface + the `logical_type_spec`/decode **rewire** to resolve against the synced
per-tenant registry (the Toron-side bulk — layered seed ∪ registry so the static seed keeps
Rune/tests green with no catalog context). Wendelon stays minimally aware (registry object + id
allocation + sync; it does not parse descriptor metadata).

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **ADR 091 — SQL use-sites over the substrate** (the direct successor; strongest lead). On the
  proven registry: `CREATE TABLE … customer_id CustomerId` (DDL column via the 089 writer),
  `CAST(x AS CustomerId)` with **reconcile-before-erase** (no silent relabel; `CAST(x AS T)` binds
  iff `T` known ∧ physical cast supported ∧ `reconcile(x.identity, T)` doesn't conflict; explicit
  primitive erase is the override), CTAS materialization. This is the payoff — receipts become
  authorable in production SQL.
- **PK/FK-derived identity** — "magic that follows constraints, not names": derive identity from
  `PRIMARY KEY` and inherit via `REFERENCES`, durable on this id substrate (needs FK support,
  currently rejected, + a canonical `schema.table.column` identity name).
- **`DROP`/`ALTER LOGICAL TYPE`** + a read-only `information_schema.logical_types` view; tenant
  policy; **refinement contracts** (092+) — value predicates on a `LogicalTypeId` → static elision.
- Carried Rune leads (Like/ILike, joins); non-Rune residual minimization (078 follow-on).

## Promotion criteria

`CREATE LOGICAL TYPE CustomerId AS BIGINT` installs a durable, tenant-scoped, Wendelon-stored
registry entry with a stable id; the definition survives restart/resync; 088 decode +
`logical_type_spec` resolve against the synced registry (the invariant holds); the static seed
stays a reserved-range Rune/test bridge; SQL use-sites still reject (091); authority is Wendelon
schema metadata (not a Toron query-path table); `CREATE DOMAIN`/refinements/DROP/ALTER/PK-FK out;
087/088/089 + all-naked suites green. Migration:
`plans/impl/090_persisted_logical_type_definition_substrate.md`.
