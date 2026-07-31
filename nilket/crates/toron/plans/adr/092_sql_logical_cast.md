# ADR 092: SQL `CAST(x AS LogicalType)` — Coercing Ascription

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified against the
latest iter-2 drafts (Claude, Codex, Gemini); all three are identical in shape (transient
`SqlLogicalCast`, async durable spec pre-resolution, spec-map to the sync binder,
`LogicalAscription` unchanged, reconcile-before-coerce, TRY_CAST deferred). **Completes the SQL
authoring surface** for the nominal type system (define → DDL column → CAST).
**Predecessor:** ADR 091 (landed `ae1a685`). **Toron-only.**
**Scope:** `ir.rs`, `sql_planner.rs`, `binder.rs`, `exec.rs` (match-arm only), tests. **No
Wendelon, executor kernel, PBC, or Rune-surface change; no new nominal gate** (087/088 enforce
post-bind); typed CTAS via 089 composition (no new CTAS grammar).

## Doctrine — the receipt, at expression position

`CAST(x AS CustomerId)` is a **use-site** that presupposes a durable definition receipt — it
resolves **durable-only** (091's resolver, the static seed excluded). And it is **not a silent
relabel**: crossing a declared identity requires an explicit primitive erase. The rule:

> `CAST(x AS T)` binds **iff** `T` is durably defined ∧ `x`'s physical converts to `T.physical`
> by existing cast semantics ∧ `reconcile(x.identity, T)` does not conflict.

```sql
CAST(raw_i32 AS CustomerId)                    -- ok:  reconcile(naked, CustomerId)=add, Int32->Int64
CAST(product_id AS CustomerId)                 -- reject: ProductId conflicts (BEFORE any erase)
CAST(CAST(product_id AS BIGINT) AS CustomerId) -- ok:  explicit primitive erase -> naked -> re-ascribe
```

**Reconcile before physical coercion is the whole feature** — a lowering through `Cast(product_id,
BIGINT)` *before* the conflict check would erase `ProductId` and silently relabel.

## Grounding (the two facts that shaped the IR)

- `SqlPlanner::plan` is sync + catalog-less (can't resolve tenant-scoped names); and `bind` is
  async-with-catalog but the recursive `bind_expr_into_program` is **sync + catalog-less**. Making
  the recursive binder async would widen the cut — so resolution moves to an async pass *before*
  it.
- **The seed-only-spec catch (load-bearing).** `bind_logical_ascription` validates physicals via
  `logical_type_spec(id)` = `LogicalTypeResolver::seed_only().spec(id)` — seed-only. So a *durable*
  id **cannot** be physically validated id-only; the resolved **`LogicalTypeSpec` (not just the
  id)** must reach the binder. This is why the resolved form is *not* an id-only
  `LogicalAscription` + coerce flag (the desugar that path implies is broken), but a spec-map fed
  to the binder.

## Decisions (11, promotion-locked)

**D1 transient IR** — `Expr::SqlLogicalCast { expr: Box<Expr>, logical_name: String, try_cast:
bool }`, planner-emitted; **never reaches `BoundExprProgram`/execution** (the binder consumes it;
`exec.rs` gets an exhaustiveness arm only). **D2 `LogicalAscription` unchanged** — id-only,
non-coercing; **no `coerce` flag**; Rune + 087's `cast(…).as_logical_type::<T>()` escape stay
byte-identical. **D3 planner records, doesn't resolve** — `CAST(expr AS <one-part, unquoted ASCII,
no-mod, non-`naked`>)` → `SqlLogicalCast` (091 name policy); primitive targets keep `Expr::Cast`;
no catalog/seed lookup at plan time. **D4 async durable pre-resolution** — before sync binding,
collect `SqlLogicalCast` names across expression-bearing nodes, resolve each via
`Catalog::resolve_durable_logical_type_by_name` (durable-only, seed excluded, refresh-on-miss);
unknown → `CatalogError::LogicalTypeNotFound { name }`. **D5 spec map reaches the sync binder** —
the pass yields a `HashMap<String, LogicalTypeSpec>` threaded into the bind context; the binder uses
the resolved `id` + `physical`, **not** seed-only `logical_type_spec` (the load-bearing
correction). **D6 reconcile before coerce** — `bind_logical_cast` binds the operand, reads its
sidecar, calls `reconcile_identity(existing, spec.id, source_physical)` (088, unchanged) **first**,
then appends any physical cast. **D7 existing physical lattice only** — same physical (incl.
`Utf8→Utf8`, `Boolean→Boolean`) ascribes with no kernel; cross-physical numeric reuses
`is_supported_explicit_numeric_cast` + `append_cast`; cross-physical string/boolean (e.g.
`Utf8→CustomerId(Int64)`) stays unsupported (no new kernels). **D8 no runtime logical node** —
identity is only the 087 `node_logical_types` sidecar; no `BoundExprNode`, executor branch, or PBC
opcode. **D9 explicit primitive erase is the only cross-identity override.** **D10 TRY_CAST
deferred** — `TRY_CAST`/`SAFE_CAST(… AS LogicalType)` reject (bounded); the nullable-conversion
contract + non-numeric physicals are a separate semantic call (093). **D11 typed CTAS by
composition** — `CREATE TABLE t AS SELECT CAST(raw AS CustomerId) AS cid …` materializes through
089's writer + survives the 090/088 decode path; explicit typed CTAS column lists reject.

## Behavior gate

`cast_defined_logical_type_ascribes_identity`; `cast_static_seed_does_not_satisfy_sql_name`
(seed-first-trap inherited from 091); `cast_undefined_fails_closed` (`LogicalTypeNotFound`);
`cast_restates_same_identity`; **`cast_conflicting_identity_rejects`** (`NominalTypeMismatch`);
**`cast_explicit_primitive_erase_then_relabel_binds`**;
`cast_int32_to_logical_int64_uses_existing_cast`; `cast_utf8_same_physical_ascribes_without_kernel`;
`cast_unsupported_cross_physical_rejects`; **`ctas_projection_cast_materializes_and_survives_resync`**
(089 ∘ 090); **`rune_cast_then_ascribe_still_relabels`** (087 escape intact);
`try_cast_logical_target_rejects` (the 092/093 line); `explicit_typed_ctas_column_list_rejects`;
087/088/089/090/091 + all-naked green.

## Architecture Guards

`adr092_sql_logical_cast_target_recorded_unresolved_not_looked_up_in_planner`;
`adr092_logical_cast_resolves_durable_only_before_sync_expr_binder`;
`adr092_resolved_durable_spec_reaches_binder_not_seed_only_logical_type_spec` (the spec, not the
id); `adr092_cast_reconciles_identity_before_physical_cast`;
`adr092_physical_coercion_reuses_existing_cast_lattice_no_new_kernels`;
`adr092_logical_ascription_unchanged_no_coerce_flag` (Rune/087 + the cast-erase escape preserved);
`adr092_no_logical_bound_node_executor_or_pbc_change`;
`adr092_typed_ctas_uses_089_projection_writer_no_new_ctas_grammar`;
`adr092_try_cast_logical_target_deferred`. **Reconcile** 091's `no_cast_or_typed_ctas_authoring_yet`
→ "logical CAST owned by 092; typed CTAS column lists + logical TRY_CAST still deferred", and the
`lower_cast_target_type → DataType` cast-shape guards (discipline #4, in-change — the lesson 090's
review surfaced and 091 honored). Carry 087/088/089/090/091.

## What to do next

*(Per the roadmap convention: this section is the next-topic source.)*

- **Refinement contracts** (the next integrity level; strongest lead) — value predicates on a
  durable `LogicalTypeId` (`PersonId(Int64, value > 0)`) → static plan elision (`WHERE id = -5` →
  `EmptyRelation`, zero I/O). The brainstorm-007 horizon; the durable receipt substrate (090) +
  the full authoring surface (091/092) now make it real.
- **`TRY_CAST(x AS LogicalType)`** — nullable conversion (reconcile compile-time + physical
  `TryCast`); the deferred 092 sibling, a clean small ADR.
- **PK/FK-derived identity** ("magic that follows constraints, not names" — derive from `PRIMARY
  KEY`, inherit via `REFERENCES`; needs FK support + a canonical identity name).
- **`DROP`/`ALTER LOGICAL TYPE`**; read-only `information_schema.logical_types`; the optional
  Wendelon-validate-at-authority (090 knob). Carried Rune leads (Like/ILike, joins); non-Rune
  residual minimization (078 follow-on).

## Council History

iter-0: all three seed `CAST` over the durable registry + typed CTAS by composition; Claude flags
the resolution-timing challenge + the ascription-over-x reason (Gemini reconfirms). iter-1: Claude
resolves D7 (async pre-pass) + recommends the transient `SqlLogicalCast` *plus a coerce flag*;
**Codex grounds the coerce-flag desugar is broken — the resolved spec, not the id, must reach the
binder (seed-only `logical_type_spec`).** iter-2: **Claude concedes the coerce flag and adopts the
spec-map shape**; Gemini concedes TRY_CAST → deferred; genuine 3-of-3. Migration:
`plans/impl/092_sql_logical_cast.md`.

## Promotion criteria

Strict SQL `CAST(x AS CustomerId)` resolves durable-only (seed excluded, spec threaded → physically
validated), reconciles conflicts before erase, supports explicit primitive erase as the override,
reuses existing physical cast semantics (no new kernels), materializes through CTAS via 089 +
survives resync; `LogicalAscription`/Rune unchanged (087 escape intact); TRY_CAST rejects (092/093
line); no Wendelon/executor/PBC; the 091 + cast-shape guards reconciled (suite green, not red);
087–091 + all-naked green. Migration: `plans/impl/092_sql_logical_cast.md`.
