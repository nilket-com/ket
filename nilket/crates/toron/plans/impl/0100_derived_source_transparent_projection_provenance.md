# Impl Plan — ADR 100: Derived-Source Transparent Projection Provenance

**Migration for** `plans/adr/0100_derived_source_transparent_projection_provenance.md`. **Toron-only** SQL
planner + binder optimizer. No new IR/exec variant, PBC opcode, executor kernel, storage API, or Wendelon
surface. Two halves: (1) a narrow `TableFactor::Derived` arm in the planner; (2) a stored-receipt resolver
that lets the existing refinement atom trace through a transparent `ExecutionPlan::Project` to a scan.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Derived-source seam | `lower_table_factor` `sql_planner.rs:3312` — only `TableFactor::Table`; `_ => unsupported("unsupported table factor")` `:3355` |
| SQL path to `Filter(Project(Scan))` | `lower_where` `sql_planner.rs:3359` (`Filter { input: plan, .. }`) wraps the FROM source; SELECT projection applied last `:2766` |
| Current scan-only atom | `stored_scan_refinement_atom` `binder.rs:1483-1525`; requires `Scan { projection: None, schema, .. }` `:1488`, reads `schema.fields()[ordinal]` `:1509` |
| Elision call site | `plan_filter_with_predicate_pushdown` `binder.rs:1173`; `static_filter_class` `:1181`; invoked from the `LogicalPlan::Filter` arm `:119-126` (during lowering) |
| Elision-vs-pushdown order | `bind()` `binder.rs:58-59`: `bind_plan(...)` **then** `push_down_scan_projection` `:3359`→`optimize_for_required` `:3364`; `projection: Some` set only at `:3413` (post-classification → projected-scan receipt inert) |
| Multi-root projection API | `BoundExprProgram.roots: Vec<BoundExprId>` (one per output); `roots.get(ordinal.0 as usize)` `:3752`, `program.node(root)` for the `BoundExprNode` |
| Identity propagation (reuse) | 089 builders `root_logical_type(idx)` + `field_with_logical_type_metadata`; guards `adr089_named_projection_preserves_metadata_on_rename`, `adr089_project_pruning_preserves_root_identity` |
| Static fence (untouched) | `static_filter_class` / `static_filter_and_class` / `static_elision_error_free_conjunct` own 098 policy |

## Phase 1 — Planner: narrow derived table source

Add a `TableFactor::Derived { lateral, subquery, alias, sample }` arm to `SqlPlanner::lower_table_factor`
(`sql_planner.rs:3312`). Keep transparency **out** of the planner (D2/Fork B).

1. `lower_derived_table(subquery: &Query, alias: &TableAlias) -> Result<LogicalPlan, FrontendError>`.
2. `validate_derived_query_shape(query: &Query) -> Result<&Select, FrontendError>` — reject query-level
   `ORDER BY`/`LIMIT`/`OFFSET`/`FETCH`/locks/`WITH` (CTE)/set ops/pipe ops; unwrap to the inner `Select`.
3. `validate_derived_select_shape(select: &Select)` — single-source plain SELECT: reject GROUP BY/HAVING/
   aggregate projection/DISTINCT/`INTO`/window/qualify/value-table; reject `>1` FROM source and any join;
   reject a nested `TableFactor::Derived` in the inner source (ADR 100 fence).
4. At the arm: reject `lateral`, `sample`, missing `alias`, nonempty `alias.columns`; then lower via the
   existing `lower_select(select)?` and wrap `LogicalPlan::Alias { input, alias:
   TableRef::new(alias.name.value.clone()) }`. Reject a join *around* the derived table in the
   `lower_select` join-build path.

**Computed projection expressions are accepted here** — they bind and run; transparency is decided later,
in the atom.

## Phase 2 — Binder: stored-receipt resolver (the two-shape proof)

Refactor `stored_scan_refinement_atom` (`binder.rs:1483`) so comparison parsing stays put and provenance
becomes a small resolver. The current direct match (`:1488-1495`) + scan-field read (`:1509`) become:

```rust
const STORED_REFINEMENT_RECEIPT_MAX_DEPTH: usize = 8;

struct StoredRefinementReceipt<'a> { field: &'a Field }

fn transparent_project_child_ordinal(
    exprs: &BoundExprProgram,
    ordinal: ColumnOrdinal,
) -> Option<ColumnOrdinal> {
    let root = *exprs.roots.get(ordinal.0 as usize)?;
    match exprs.node(root)? {
        BoundExprNode::Column(column) => Some(column.ordinal),
        _ => None,                              // computed root → non-transparent
    }
}

fn stored_refinement_receipt<'a>(
    input: &'a ExecutionPlan,
    ordinal: ColumnOrdinal,
    depth: usize,
) -> Option<StoredRefinementReceipt<'a>> {
    if depth > STORED_REFINEMENT_RECEIPT_MAX_DEPTH {
        return None;
    }
    match input {
        ExecutionPlan::Scan { projection: None, schema, .. } => {
            let field = schema.fields().get(ordinal.0 as usize)?.as_ref();
            Some(StoredRefinementReceipt { field })
        }
        ExecutionPlan::Project { input, exprs, .. } => {
            let child = transparent_project_child_ordinal(exprs, ordinal)?;
            stored_refinement_receipt(input, child, depth + 1)
        }
        _ => None,                              // incl. Scan { projection: Some } — inert, excluded (D5)
    }
}
```

Then in `stored_scan_refinement_atom`: keep the binary-comparison extraction + the single column/literal
node split (`:1496-1508`); keep `predicate.node_logical_type(column_node)` + `is_naked` reject + `spec`
(`:1510-1518`, 089-propagated identity, D6); replace the direct scan-field read with
`stored_refinement_receipt(input, column.ordinal, 0)?.field`; keep both carrier checks against that
receipt field (`field.data_type() == &spec.physical` **and** `column.data_type == spec.physical`,
`:1515`); keep `predicate_carrier_literal(predicate, literal_node, field.data_type())` and the unchanged
domain intersection.

## Phase 3 — Preserve static policy (098)

Do not touch `static_filter_class`, `static_filter_and_class`, `static_elision_error_free_conjunct`,
`closed_root_filter_class`, or `plan_filter_with_predicate_pushdown` beyond the atom now seeing through
transparent projects. The eager-error fence and the no-`AlwaysPass`-removal rule are unchanged.

## Phase 4 — Behavior tests

New SQL-level fixtures in `crates/toron/tests/logical_type_ddl_test.rs` (`PersonId AS BIGINT CHECK
(VALUE > 0)`, `people(id PersonId, note TEXT)`):

- `derived_table_transparent_projection_refinement_elides_to_empty_relation` —
  `SELECT person_id FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0` → assert
  `find_empty_relation(&bound).is_some()` and no scan remains under the filter path.
- `derived_table_projection_subset_reorder_and_rename_preserve_refinement_elision` — multi-column source,
  prove subset/reorder/rename remaps output ordinal back to stored `id`.
- `derived_table_computed_projection_binds_but_does_not_refinement_elide` —
  `SELECT id FROM (SELECT id + 0 AS id FROM people) AS p WHERE id < 0` binds; `find_empty_relation` none,
  `contains_scan` true.
- `derived_table_outer_projection_is_unconstrained` —
  `SELECT person_id + 1 FROM (SELECT id AS person_id FROM people) AS p WHERE person_id < 0` still elides.
- `derived_table_outer_filter_keeps_098_eager_error_fence` — contradictory refinement conjunct +
  unsafe-cast/division sibling → no `EmptyRelation`.
- `derived_table_source_scope_rejects_general_subquery_features` — missing alias / alias column list /
  lateral / sample / nested derived / join inside / join around / aggregate-group-having / order-limit /
  CTE-set each reject.

**Retarget the IR-level proof (confirmed, not optional):** `refinement_on_non_scan_input_does_not_elide`
(`logical_type_ddl_test.rs:1320`) currently builds `Filter(id=-5, Project([Expr::column("id")] AS "id",
ScanNamed(people)))` — a **transparent bare-column** project — and asserts no elision. ADR 100 makes this
shape elide. **Invert it:** rename → `refinement_through_transparent_project_elides`, assert
`find_empty_relation(&bound).is_some()` and `!contains_scan` under the filter. (This is the binder-half
unit proof — direct `LogicalPlan`, no derived-table SQL — complementing the integration fixtures above.)

**Preserve unchanged:** `cast_ascribed_projected_column_does_not_elide` (`:1285`, `SqlLogicalCast` root →
non-transparent) stays non-eliding — the negative boundary. Existing 097/098 direct-scan tests
(`refined_positive_column_eq_negative_elides_to_empty_relation`,
`refinement_and_safe_conjunct_elides_to_empty_relation`,
`refinement_and_strict_cast_sibling_does_not_elide`, `refinement_and_non_receipt_shapes_do_not_elide`)
stay green (receipt base case unchanged).

## Phase 5 — Architecture guards

In `crates/toron/tests/architecture/predicate_pushdown.rs` (add/retarget):

- `adr100_derived_table_source_is_narrow_not_general_subquery_support` — `lower_table_factor` names
  `TableFactor::Derived`; the validators reject lateral/sample/missing-alias/alias-columns/nested/joins/
  aggregate-group-order-limit/CTE-set.
- `adr100_refinement_provenance_crosses_only_transparent_project` — binder has `stored_refinement_receipt`
  + `transparent_project_child_ordinal`; matches `ExecutionPlan::Project`, requires `BoundExprNode::Column`,
  recurses to `Scan`.
- `adr100_refinement_receipt_root_is_scan_or_transparent_project_chain` — retargets the prior
  `Scan { projection: None }`-only invariant. **Note:** the existing guard pins the fixture name by string
  (`logical_type_ddl.contains("refinement_on_non_scan_input_does_not_elide")`) — update that literal to the
  renamed `refinement_through_transparent_project_elides` so the inverted test stays pinned.
- `adr100_refinement_receipt_reuses_089_identity_no_redrive` — atom still contains
  `predicate.node_logical_type(column_node)` + `logical_types.spec(logical_type)`; no `logical_type_spec`,
  no seed lookup, no logical-type-name matching, no SQL-text inspection.
- `adr100_eager_error_fence_policy_unchanged` — `static_filter_class` / `static_filter_and_class` still own
  098 policy.
- `adr100_no_projected_scan_receipt_until_elision_has_a_consumer` — receipt helper does **not** match
  `projection: Some`; the post-`push_down_scan_projection` rationale is pinned in a comment/guard.
- `adr100_no_ir_exec_pbc_storage_or_wendelon_change` — no new `LogicalPlan`/`ExecutionPlan` variant, no PBC
  opcode, no storage/Wendelon mention of ADR 100 helpers.

## Phase 6 — Verification

```sh
cargo test -p toron --test logical_type_ddl_test derived_table_     # new SQL fixtures + retarget
cargo test -p toron --test logical_type_ddl_test refinement_        # 097/098 + inverted unit proof
cargo test -p toron --test architecture_test adr100                 # guards + retarget
cargo test -p toron                                                 # full suite, no regression (baseline 35 green)
```

## Acceptance

A narrow aliased single-source derived table is accepted; `Filter(Project(Scan))` is SQL-reachable;
contradictions through a transparent inner projection elide to `EmptyRelation` (ordinal-remapped to a real
stored scan, reusing 089 identity + the 097/098 domain proof); computed derived projections bind without
elision; the outer projection is unconstrained; 098's eager-error fence is unchanged; direct-scan elision
still works; the projected-scan receipt is excluded as inert; full Toron suite green with no
IR/executor/PBC/storage/Wendelon surface change.

## Risks / notes

- **The `refinement_on_non_scan_input_does_not_elide` inversion is a deliberate behavior change** — it's
  the visible payoff of ADR 100, not a regression. The guard pinning its name (Phase 5) must update in the
  same change or the suite breaks on the rename.
- **Recursion bound is defensive only** — the `ExecutionPlan` tree is acyclic and D2 fences nested derived
  sources, so the real depth is 1; the bound's only effect is a safe-fail (`None` → no elision) on a
  pathological/future stacked-project shape.
- **Alias resolution stays minimal** — unqualified outer refs resolve against the derived output schema
  (schema-driven via `LogicalPlan::Alias`); do not widen qualifier resolution in ADR 100.
