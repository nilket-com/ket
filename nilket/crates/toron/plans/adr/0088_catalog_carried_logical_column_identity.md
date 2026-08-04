# ADR 088: Catalog-Carried Logical Column Identity

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-2, Claude iter-1, Gemini iter-1). The carrier
(metadata-resident, no `TableBinding` vector), `ResolvedColumn` physical-only, the
single `reconcile_identity` helper, fail-closed decode, and the no-propagation boundary
are all unanimous.
**Category:** Toron interior substrate / catalog→binder type-system continuation.
**Direct successor to ADR 087** — moves nominal identity from a query-authoring flourish
to a database contract.
**Scope:** Toron-only; `ir.rs` (decoder), `catalog.rs` (`from_descriptor` validation),
`binder.rs` (resolve/stamp/reconcile + join keys), tests. **No Wendelon descriptor/wire
change, no SQL DDL/domain syntax, no executor kernel or PBC opcode, no Rune-surface change.**

## Doctrine — the catalog is the source of truth

ADR 087 made nominal identity *enforceable* when a query authored it
(`col("id").as_logical_type::<CustomerId>()`). ADR 088 makes it a **database contract**:
the column declares its identity in the schema, and the binder protects *unannotated*
queries by default — `orders.customer_id = products.id` rejects with no `.as_logical_type`
in sight. **Authoring-surface parity with catalog policy:** a frontend may *add* identity
to a naked column or *restate* the catalog's identity, but it may **not contradict** it.
The schema is the source of truth; the query may not lie about a column's identity.

## Summary

A reserved Arrow field-metadata key carries the `LogicalTypeId` on each column. Toron
decodes + validates it at table binding (fail closed), then stamps ADR 087's
`node_logical_types` sidecar when a source column binds and resolves catalog identity for
bare equi-join keys before the join gate runs. The existing 087 nominal gate
(`nominal_compatible`) then rejects physically-equal-but-logically-distinct operands —
now sourced from the catalog, not just explicit ascription.

```text
nilket.toron.logical_type_id = "1"   # CustomerId      (missing key  → NAKED)
nilket.toron.logical_type_id = "2"   # ProductId        ("0"          → NAKED)
orders.customer_id = customers.id    # binds  (CustomerId = CustomerId)
orders.customer_id = products.id     # rejects (CustomerId vs ProductId)
orders.customer_id = 7               # binds  (087 naked-literal exception)
```

## Grounding ledger (verified)

- **Identity already lives in the schema, not a new field.** `TableSchemaDescriptor`
  stores Arrow schema IPC (`wendelon/src/schema/table.rs:998`) and the logical-schema
  fingerprint includes that IPC (`:1096`) — field metadata is already descriptor data and
  already part of schema identity. `TableBinding::from_descriptor` decodes
  `descriptor.arrow_schema()` (`catalog.rs:85`) — the fail-closed validation boundary.
- **The metadata-resident carrier survives to `resolve_column` (the carrier choice).**
  Arrow field metadata rides intact from the descriptor schema through every path 088
  touches: unprojected scan `scan_output_schema` (`ir/execution.rs:323`, `schema.clone()`
  / whole-`Field` clones); projected scan `project_schema` (`binder.rs:2509`,
  `field.as_ref().clone()`); joins `join_schema`/`left_fields`/`nullable_fields`
  (`binder.rs:2057`, whole-`Field` clone / `.with_nullable(true)`). So no `TableBinding`
  parallel vector and no new arity invariant are needed.
- **The 088/089 boundary falls on a real seam.** The *only* place field metadata is
  dropped is `schema_for_exprs` / `..._with_output_names` (`ir/execution.rs:792`,
  `binder.rs:3499`), which rebuild fields with `Field::new` — i.e. the projection/aggregate
  **output** path. That is exactly ADR 089's scope; 088 adds no propagation writer there.
- **`ResolvedColumn` must stay physical-only.** It derives `Serialize` (`ir/execution.rs:27`)
  and is embedded as `BoundExprNode::Column(ResolvedColumn)` (`:64`), serialized inside
  `BoundExprProgram` across coordinator→worker. A logical field there would be a *second*
  serialized logical carrier beside 087's `node_logical_types` — a drift hazard. Identity
  rides a **binder-local** wrapper during binding only; the sidecar stays the sole carrier.

## Decisions (13, promotion-locked)

**D1** reserved key `nilket.toron.logical_type_id`, decimal `u32` value; no type-name
strings, no parser. **D2** Wendelon stores/fingerprints the Arrow IPC unaware of the key's
meaning; Toron interprets it at bind. **D3** one shared
`decode_column_logical_type(field)`: missing key → NAKED; `"0"` → NAKED; parse-fail →
bounded error; unknown non-zero id → bounded error; known non-naked id must physically
match `field.data_type()` through the 087 registry. **D4** `from_descriptor` validates
every field through the decoder right after `descriptor.arrow_schema()` — fail closed
before query binding; no-key descriptors stay all-naked. **D5** no `TableBinding` parallel
vector (schema metadata is the durable source; the program sidecar is the execution
currency). **D6** `ResolvedColumn` stays physical-only; a binder-local
`ResolvedLogicalColumn { column, logical_type }` carries identity during binding;
`node_logical_types` is the sole serialized carrier. **D7** `Expr::Column` resolves a
`ResolvedLogicalColumn`, appends the physical column, then `set_node_logical_type` on that
node; derived outputs (cmp/bool/`IN`/`LIKE`/null/aggr/arith) stay naked per 087. **D8**
bare equi-join keys take catalog identity from field metadata before 087's join gate runs.
**D9** explicit `LogicalAscription` may **add** (to a naked node) or **restate** (same id)
but may **not contradict** a non-naked catalog identity → `NominalTypeMismatch`; to
reinterpret, erase first (`cast(type_i64())`) then ascribe. **D10** one shared
`reconcile_identity(existing, declared)` for add/restate/not-contradict, called by **both**
`bind_logical_ascription` and join-key resolution (no policy drift). **D11** reuse the
single 087 `nominal_compatible` helper for comparison/`IN`/join — 088 only changes where
source identity originates. **D12** no name heuristics — `customer_id`/`id` never imply
identity; only metadata + explicit ascription yield non-naked. **D13** no propagation
writers — existing scan-pruning/join field-clones may *carry* inherited metadata, but 088
synthesizes no logical metadata for project/aggregate/computed/CTAS/DML output schemas
(that is 089).

## The reconciliation mechanism (the precise sites)

D9/D10 live in one helper with two callers — the 088 analogue of 087's single
`nominal_compatible`:

- **`reconcile_identity(existing, declared)`:** `existing == declared` → that id (restate);
  `declared` naked → `existing`; `existing` naked → `declared` (add); both non-naked +
  different → `NominalTypeMismatch` (contradict).
- **Expr path — `bind_logical_ascription` flips from *set* to *reconcile-then-set*.** In
  087 it did an unconditional `set_node_logical_type`. Under D7 the inner column node now
  arrives **already stamped** with its catalog id, so the ascription reconciles
  `(node_logical_type(inner), ascription_id)` and sets the result. The cast-erase escape
  (D9) is free: `cast` outputs naked (087 D6) → inner naked → reconcile takes the declared
  id.
- **Join path — dual-source, same helper.** A join key has up to two sources: the explicit
  id peeled by `join_key_column_ref` (raw `Expr`) and the catalog id decoded from the
  resolved column's field. `resolved_join_key` reconciles `(catalog_id, explicit_id)` →
  `ResolvedJoinKey.logical_type`, which flows to 087's unchanged join gate. A bare key →
  `(catalog_id, NAKED)` → catalog id.

## Behavior gate (10)

metadata column vs naked literal **binds** (`customer_id = 7`); same catalog id **binds**;
different catalog id → `NominalTypeMismatch`; catalog id vs naked-non-literal **rejects**;
invalid metadata (unknown id / non-decimal / physical mismatch) → bounded **catalog** error
at descriptor bind; catalog join same-id **binds**; catalog join different-id → reject;
**post-join filter still sees source identity** (proves `join_schema` preserved metadata —
the metadata-resident payoff); **D9 triad** (restate binds / contradict rejects /
cast-erase-then-reascribe binds); all-naked SQL/Rune suites stay green (behavior-additive).

## Architecture Guards (11 + carried 087)

`logical-identity-uses-Arrow-field-metadata-not-Wendelon-descriptor-field`;
`no-TableBinding-parallel-logical-vector`; `ResolvedColumn-stays-physical-only` (serialized
purity); `field-metadata-decoder-validates-parse-registry-and-physical-type`;
`descriptor-binding-validates-declared-logical-metadata`;
`column-binding-stamps-bound-expr-logical-sidecar`;
`join-key-takes-identity-from-catalog-metadata`;
`identity-reconciliation-is-single-shared-helper` (one `reconcile_identity`, both call
sites — drift guard); `explicit-ascription-cannot-contradict-existing-identity`;
`no-column-name-heuristic-for-logical-identity`;
`no-new-projection-or-ctas-logical-metadata-writer`. Carry 087:
sidecar-not-Arrow-replacement, no-runtime-`LogicalAscription`-node, one shared
`nominal_compatible`, cast-outputs-naked.

## Council History

iter-0: all three lock the topic and defer propagation to 089; **Claude grounds the
smaller metadata-resident carrier** (Codex's initial `TableBinding` vector was
unnecessary). iter-1: both peers adopt it; **Codex catches that `ResolvedColumn` is
serialized → keep it physical-only**; **Claude concedes that, closes the scan-leaf metadata
survival, and pins the single `reconcile_identity` mechanism** across the expr + join paths;
Gemini frames the catalog-as-source-of-truth doctrine. iter-2: converged.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **ADR 089 — Projection / CTAS logical identity propagation** (the direct successor).
  Carry identity through the one place 088 deliberately stops: `schema_for_exprs` /
  `schema_for_exprs_with_output_names` (the `Field::new` rebuild). A direct projection
  (`SELECT customer_id AS cid`) should preserve `CustomerId`; computed expressions stay
  naked. Needs a typed-schema or Arrow field-metadata-emit decision — the first 088
  follow-on and the gate that makes identity flow through multi-hop pipelines.
- **ADR 090 — SQL / domain authoring** for logical identity (`CAST(id AS CustomerId)` /
  domain-style DDL) so identity is declarable through SQL, not just test-built Arrow
  metadata; pairs with a dynamic/tenant marker registry (088 uses the static 087 registry).
- **ADR 091+ — Refinement contracts** — now that identity has a durable catalog handle
  (the whole point of 088), attach value predicates to a `LogicalTypeId`
  (`PersonId(Int64, value > 0)`) and let the binder elide `WHERE id = -5` → `EmptyRelation`.
- **Carried Rune leads** (substrate run was prioritized over these): Rune `Like`/`ILike`;
  Rune joins (relation-alias + qualified-column ergonomics).
- **Non-Rune lead:** residual minimization for factored shapes (078 follow-on).

## Promotion criteria

Arrow field-metadata key; Toron-side fail-closed decode/validate; no Wendelon field; no
`TableBinding` vector; no serialized `ResolvedColumn` logical duplicate; source-column
sidecar stamping plus join-key catalog identity resolution; one shared
`reconcile_identity` (add/restate/not-contradict);
reuse 087 `nominal_compatible`; no name heuristics; no propagation/DDL/refinement scope; 10
fixtures incl. post-join-sees-identity + the D9 triad; all-naked suites green. Migration:
`plans/impl/0088_catalog_carried_logical_column_identity.md`.
