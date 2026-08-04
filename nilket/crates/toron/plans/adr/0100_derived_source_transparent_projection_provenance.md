# ADR 100: Derived-Source Transparent Projection Provenance

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on pairing the first
narrow derived `FROM` source (the consumer) with refinement-elision provenance that traces through a
transparent inner projection to a stored scan receipt (the proof). The one fork (where "transparent"
lives) resolved to **B**: transparency is an elision-proof property decided in the binder atom, not a SQL
parser property. **Toron-only.** **Predecessors:** ADR 089 (projection-carried logical identity — the
load-bearing reuse), 095 (`EmptyRelation`), 097/098 (refinement elision + the eager-error fence), 099
(proved standalone pass-through provenance had no SQL consumer — the reason this ADR builds the consumer
first). **Migration:** `plans/impl/0100_derived_source_transparent_projection_provenance.md`.

## Doctrine — the proof follows the receipt, even through a transparent rename

097/098 made refinement contradictions elide to `EmptyRelation`, but the refinement atom is *direct*
scan-rooted: `stored_scan_refinement_atom` requires `ExecutionPlan::Scan { projection: None }` as the
immediate filter input. ADR 099's topic round found why standalone pass-through provenance was inert:
ordinary SQL lowers `WHERE` *below* the SELECT list, so `SELECT id FROM people WHERE id < 0` already binds
`Project(Filter(Scan))` — the classifier sees the scan directly. The smallest SQL shape that puts a
projection *between* the outer filter and the scan is a derived table:

```sql
SELECT person_id FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0
```

The outer `WHERE` wraps the derived source → `Filter(Project(Scan))`. ADR 100 makes that shape reachable
**and** lets the atom cross the transparent inner projection to the stored receipt. A transparent
projection may *carry* a column's lineage, but only the stored scan column is the durable integrity
receipt — so ADR 100 remaps ordinals to the scan and reuses the durable-registry proof; it never infers
identity from names or SQL text.

## Grounding (verified)

- **Derived-source seam.** `lower_table_factor` (`sql_planner.rs:3312`) accepts only `TableFactor::Table`;
  every other factor hits `unsupported("unsupported table factor")` (`:3355`). `TableFactor::Derived` is
  the new arm.
- **The reachable shape.** `lower_select` builds FROM, then `lower_where` (`sql_planner.rs:3359`:
  `plan = Filter { input: Box::new(plan), .. }`), then applies the SELECT projection last (`:2766`). A
  derived `Project(Scan)` source is therefore the first SQL path to `Filter(Project(Scan))`.
- **The atom bails on a project.** `stored_scan_refinement_atom` (`binder.rs:1488`) requires
  `Scan { projection: None, schema, .. }` as the *direct* input and reads
  `schema.fields()[predicate_ordinal]` (`:1509`); any intervening `Project` → `None` → no elision.
- **Identity already flows (089).** The projection schema builders write `root_logical_type(idx)` via
  `field_with_logical_type_metadata` (`adr089_named_projection_preserves_metadata_on_rename`,
  `adr089_project_pruning_preserves_root_identity`). A column-root projection's output field carries the
  scan column's logical-type id — so the predicate node's `node_logical_type` already resolves correctly
  *through* the project. ADR 100 needs structural provenance, not identity re-derivation.
- **Projected scans are post-classification.** `bind()` (`binder.rs:58-59`) runs `bind_plan` (the full
  lowering, where each `Filter` classifies via `plan_filter_with_predicate_pushdown`→`static_filter_class`
  at `:119-126`/`:1181`) **then** `push_down_scan_projection` (`:3359`→`optimize_for_required` `:3364`),
  the sole producer of `Scan { projection: Some }` (`:3413`). A projected scan never coexists with an
  elision check — so a projected-scan receipt is inert (D5).

## Decisions (promotion-locked)

**D1 — Pair the consumer and the proof.** Ship derived-source support and transparent-project receipt
discovery together. Proof-only is inert (099's finding); source-only leaves a known missed optimization on
the exact motivating shape.

**D2 — Narrow derived source, *not* transparency-gated (Fork B).** The planner accepts only non-lateral,
aliased, unsampled derived tables whose subquery is a single-source plain SELECT. **Computed projections
are accepted by the source gate** — they bind and run, they just don't satisfy the elision proof
(transparency stays out of `lower_table_factor`). Reject: missing alias, alias column lists, lateral,
sample, joins inside or around the derived table, nested derived sources, CTE, set ops,
aggregate/GROUP/HAVING/DISTINCT, and query-level ORDER BY/LIMIT/OFFSET/FETCH.

**D3 — Transparency is structural.** A project output is transparent **iff** its bound root is exactly
`BoundExprNode::Column(child_ordinal)`. Subset, reorder, and rename are transparent; cast, TRY_CAST,
arithmetic, function calls, logical ascription, literals, and any computed root are non-transparent
(return `Unknown`).

**D4 — Two-shape stored-receipt resolver.** Replace the direct `Scan { projection: None }` atom gate with
a binder-local resolver:
- `ExecutionPlan::Scan { projection: None, schema, .. }` → base receipt (stored ordinal = current ordinal);
- `ExecutionPlan::Project { input, exprs, .. }` → if `exprs.roots[ordinal]` is `Column(child_ordinal)`,
  remap output→child and recurse into `input`; any non-column root → `None`;
- anything else → `None`.
A small recursion bound (≈8) is a defensive constant with a safe failure mode (over-budget → `None` → no
elision); it is **not** load-bearing — the `ExecutionPlan` tree is acyclic and D2 fences nested derived
sources, so the admitted depth is 1.

**D5 — Exclude the projected-scan receipt (inert).** Do **not** add `Scan { projection: Some(..) }`
handling. Projection pushdown (`push_down_scan_projection`/`optimize_for_required`) runs *after*
classification, so that branch has no consumer at the point this proof runs; adding it would re-import the
inert-optimizer-code anti-pattern ADR 100 exists to avoid. Revisit only if elision is ever moved or
duplicated to run after projection pushdown (its own ADR).

**D6 — Identity stays 089-propagated; no re-drive.** The atom keeps reading
`predicate.node_logical_type(column_node)`, rejecting `naked`, and resolving through the current tenant's
durable `LogicalTypeRegistry` (`logical_types.spec`). The resolver supplies only the stored field (for the
carrier-equality check at the remapped ordinal) and the provenance. No name heuristics, no seed lookup, no
SQL-text inspection.

**D7 — 098's eager-error fence unchanged.** `static_filter_class` / `static_filter_and_class` /
`static_elision_error_free_conjunct` keep their policy; ADR 100 widens only *stored-receipt discovery*. It
still never removes `AlwaysPass` filters.

**D8 — Outer projection unconstrained.** The proof fires at the `WHERE` looking down; the derived (inner)
projection on the filter input must be transparent for elision, but the outer SELECT list above the filter
may compute freely (`SELECT person_id + 1 FROM (SELECT id AS person_id FROM people) AS p WHERE
person_id < 0` still elides).

**D9 — Alias stays minimal.** The derived alias is syntactically required; behavior gates use unqualified
outer references. Qualified `p.column` only if it falls out of existing single-source qualifier handling
without weakening unknown-qualifier errors — no broad alias-resolution rewrite.

**No cross-ring surface.** No new `LogicalPlan`/`ExecutionPlan` variant (the existing `Project` carries
`exprs: BoundExprProgram` + `output_schema` — enough to read column-root lineage and remap ordinals); no
PBC opcode, executor kernel, storage API, or Wendelon change. ADR 100 broadens the trigger for the
existing `EmptyRelation` substitution.

## Behavior gate

- `derived_table_transparent_projection_refinement_elides_to_empty_relation` (the motivating SQL shape →
  `EmptyRelation`).
- `derived_table_projection_subset_reorder_and_rename_preserve_refinement_elision`.
- `derived_table_computed_projection_binds_but_does_not_refinement_elide`
  (`SELECT id FROM (SELECT id + 0 AS id FROM people) AS p WHERE id < 0` stays `Filter(Project(Scan))`).
- `derived_table_outer_projection_is_unconstrained`.
- `derived_table_outer_filter_keeps_098_eager_error_fence`.
- `derived_table_source_scope_rejects_general_subquery_features` (no-alias / alias-column-list / lateral /
  sample / nested-derived / join-inside / join-around / aggregate-group-having / order-limit / CTE-set).
- **Retargeted (IR-level unit proof):** `refinement_on_non_scan_input_does_not_elide`
  (`logical_type_ddl_test.rs:1320`) builds `Filter(Project([column("id")], Scan(people)))` — a transparent
  bare-column project — and currently asserts no elision. Under ADR 100 this shape **elides**; rename →
  `refinement_through_transparent_project_elides`, assert `EmptyRelation` present / scan gone under the
  filter. This is the binder-half unit proof, independent of the planner's derived-table path.
- **Preserved (transparency boundary):** `cast_ascribed_projected_column_does_not_elide`
  (`:1285`, `SqlLogicalCast` root → non-transparent) stays non-eliding. Existing 097/098 direct-scan tests
  (`refined_positive_column_eq_negative_elides_to_empty_relation`,
  `refinement_and_safe_conjunct_elides_to_empty_relation`,
  `refinement_and_strict_cast_sibling_does_not_elide`, `refinement_and_non_receipt_shapes_do_not_elide`)
  stay green (receipt base case unchanged).

## Architecture guards

- `adr100_derived_table_source_is_narrow_not_general_subquery_support`.
- `adr100_refinement_provenance_crosses_only_transparent_project` (explicit `stored_refinement_receipt` +
  `transparent_project_child_ordinal`; matches `ExecutionPlan::Project`, requires `BoundExprNode::Column`,
  recurses to a real `Scan`).
- `adr100_refinement_receipt_root_is_scan_or_transparent_project_chain` (retargets the prior
  `Scan { projection: None }`-only invariant; updates the string-pinned fixture name to the renamed test).
- `adr100_refinement_receipt_reuses_089_identity_no_redrive` (atom keeps `node_logical_type` + `spec`; no
  `logical_type_spec`/seed lookup/name heuristic/SQL-text inspection).
- `adr100_eager_error_fence_policy_unchanged`.
- `adr100_no_projected_scan_receipt_until_elision_has_a_consumer` (receipt helper does not match
  `projection: Some`; the post-pushdown rationale is pinned).
- `adr100_no_ir_exec_pbc_storage_or_wendelon_change`.

## Scale boundary & seam check

Toron-only SQL planner + binder optimizer. The derived-source arm is a new `TableFactor` branch lowering
through the existing `lower_select`/`lower_query` path wrapped in `LogicalPlan::Alias`; the proof is a
local refactor of one atom into a receipt resolver. No load-bearing seam: no new IR/exec variant, no PBC,
no executor kernel, no storage/Wendelon surface.

## What to do next

- **Projected-scan receipt** — only if/when refinement elision is ever duplicated or moved to run *after*
  `push_down_scan_projection` (then it has a consumer); carried with general projection-pushdown-aware
  elision.
- **General derived tables / subqueries** (joins, aggregates, sort/limit, alias scoping, decorrelation,
  IN/scalar/correlated) — a separate track beyond this proof.
- Carried from 099: `information_schema.tables`/`.columns` (2nd virtual-table cycle); `DROP`/`ALTER LOGICAL
  TYPE`; Rune `Like`/`ILike`; the Wendelon `*V1` de-version sweep (Wendelon-home chore).

## Council history

099 deferred standalone pass-through provenance (no SQL consumer — Claude's proof). 100 iter-0: Codex
seeded the paired derived-source+provenance cut; Claude grounded that 089 already propagates identity
(provenance is structural-only) and surfaced the transparency-placement fork; Gemini endorsed. iter-1:
Claude proved the projected-`Scan { projection: Some }` receipt is inert (classification precedes
`push_down_scan_projection`) and reduced it to a two-shape resolver; fork resolved to B. iter-2 (dual):
3-of-3 on topic/fork/two-shape receipt; Claude confirmed the `refinement_on_non_scan_input_does_not_elide`
retarget (transparent bare-column project → now elides) with `cast_ascribed_projected_column_does_not_elide`
preserved as the boundary; recursion bound kept defensive-only. iter-3 final: Codex/Gemini adopted the
retarget verbatim; no open divergence. Migration:
`plans/impl/0100_derived_source_transparent_projection_provenance.md`.

## Promotion criteria

A narrow aliased single-source derived table is accepted; `Filter(Project(Scan))` is SQL-reachable;
contradictions through a transparent derived projection elide to `EmptyRelation` (ordinal-remapped to a
real stored scan, reusing 089 identity + the 097/098 domain proof); computed derived projections bind
without elision; the outer projection is unconstrained; 098's eager-error fence is intact; the
projected-scan receipt is excluded as inert; Toron-local, no IR/executor/PBC/storage/Wendelon surface;
full Toron suite green. Migration:
`plans/impl/0100_derived_source_transparent_projection_provenance.md`.
