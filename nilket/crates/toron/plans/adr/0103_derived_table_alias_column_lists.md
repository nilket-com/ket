# ADR 103: Derived-Table Alias Column Lists (explicit-projection)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on the topic, the
explicit-only scope, and the seam (Claude seeded the topic + the 089/100 transparency tie-in; Codex
grounded the impl seam + the explicit-vs-wildcard scope correction + duplicate-alias inheritance; Gemini
ratified; operator approved the explicit-only scope + wildcard fence). Closes the ADR 100 alias-column
deferral. **Pure `sql_planner`** — no IR/bound-IR/binder-semantics/executor/PBC/storage/Wendelon/
`information_schema` surface. **Predecessors:** 100 (derived-source machinery + transparent-projection
receipt), 089 (metadata-preserving rename). **Migration:**
`plans/impl/0103_derived_table_alias_column_lists.md`.

## Doctrine — a derived alias-column list is a positional rename of the explicit projection

`FROM (SELECT id, note FROM people) AS p(pid, label)` renames the derived subquery's output columns
positionally. It is not a new operator: it rewrites the existing lowered `LogicalPlan::Project.output_names`,
so all type-identity and refinement behavior flows through the same 089/100 project path that already
preserves metadata for bare-column renames. A column-ref rename stays transparent → ADR 100's elision
receipt still traces it; ADR 103 extends the transparency guarantee to alias renames, it doesn't add a new
semantic.

## Grounding (verified)

- **The rejection to move.** `lower_table_factor`'s `TableFactor::Derived` arm rejects
  `!alias.columns.is_empty()` (`sql_planner.rs:3394`) *before* `lower_derived_table` runs.
- **The seam.** `lower_derived_table` has the inner `select` (→ explicit projection length / wildcard
  detection) **and** `LoweredSelect { plan }`, then `alias_plan(plan, Some(alias))`.
- **Alias is metadata-free; binder ignores it.** `alias_plan` builds `LogicalPlan::Alias { alias:
  TableRef::new(...) }` (no column list); `binder.rs:123` does `LogicalPlan::Alias { input, .. } =>
  self.bind_plan(input)`. So alias columns can only live on `LogicalPlan::Project { output_names }`.
- **Parser carrier.** In sqlparser 0.62, `TableAlias.columns` is `Vec<TableAliasColumnDef>`, not
  `Vec<Ident>`; each item has `name: Ident` and optional `data_type`. ADR 103 uses only the name
  (`column.name.value`) and rejects typed alias-column definitions.
- **Explicit → Project; wildcard → bare plan.** Explicit projection lowers to `Project { exprs,
  output_names }` with plan-time arity (`:2766`); `[SelectItem::Wildcard(_)]` returns the bare plan, no
  Project (`:2738`) → arity/names binder-known, out of the pure-planner cut.
- **Transparency reuse.** A bare `Expr::column` item renamed keeps a column root, so
  `schema_for_exprs_with_output_names` preserves identity (089) and `transparent_project_child_ordinal`
  (100) still traces the receipt.

## Decisions (promotion-locked, 3-of-3)

**D1 — Positional rename of explicit derived projections.** For derived subqueries that lower to a
`Project`, `alias.columns` overwrites `Project.output_names` positionally (fills unnamed exprs, overrides
per-item `AS`). `SELECT pid FROM (SELECT id FROM people) AS p(pid)` lowers as the same transparent inner
`Project` ADR 100 uses with `output_names = [Some("pid")]`; the old name `id` is **not** visible at the
derived boundary.

**D2 — Explicit-projection only; wildcard alias-lists reject (named fence).** Supported only when the inner
projection is planner-known (an explicit `Project`). An alias column list over a wildcard derived subquery
rejects: "alias column lists over `SELECT *` derived tables are not supported". Binder-carried
wildcard-alias representation is a deferred future ADR.

**D3 — Exact arity equality.** `alias.columns.len()` must equal `output_names.len()`; too-many and too-few
reject **before binding** with an error stating both expected and actual counts.

**D4 — Alias-list entries are names only.** If any `TableAliasColumnDef` carries `data_type`, reject with a
clear unsupported message. A derived-table alias column list renames output fields; it does not ascribe,
cast, or validate column types.

**D5 — Rename only; transparency stays structural.** A renamed bare column stays transparent (root is still
`BoundExprNode::Column`, identity preserved) and still refinement-elides; a renamed computed expression
binds with the new name but stays non-transparent and does not elide. The rename adds no transparency
logic.

**D6 — Inherit duplicate output-name behavior (no new uniqueness rule).** `AS p(x, x)` is accepted at plan
rename time — it is just another way to produce duplicate output field names; a later unqualified reference
to the duplicated name reuses the existing `AmbiguousColumn` behavior. ADR 103 adds no special
duplicate-alias rejection; CTAS duplicate-output rejection remains CTAS-owned.

**D7 — Preserve every ADR 100 derived-source fence; retarget only the categorical guard.** Required alias,
non-lateral, no sample, no nested derived, no joins (inside/around), no aggregate/GROUP/HAVING/window, no
sort/limit/offset/CTE/set — all stay. Only the *categorical* alias-column rejection in
`adr100_derived_table_source_is_narrow_not_general_subquery_support` is retargeted.

**D8 — No new runtime or cross-ring surface.** No `LogicalPlan`/`ExecutionPlan` variant, no bound-IR node,
no PBC opcode, no storage/executor/Wendelon API, no `information_schema` surface, no general derived-table
widening. (Out of scope, named: qualified `ORDER BY` output-name support; uniqueness rules for alias
names.)

## Behavior gate

- `derived_table_alias_columns_rename_output` — `SELECT pid FROM (SELECT id FROM people) AS p(pid)` binds,
  exposes `pid`.
- `derived_table_alias_columns_hide_original_names` — `SELECT id FROM (SELECT id FROM people) AS p(pid)`
  rejects with `BindingError::UnknownColumn { column: "id" }` / `unknown column: id` (the old name is
  gone at the boundary).
- `derived_table_alias_columns_arity_mismatch_rejects` — too-many and too-few reject with the arity message
  (expected + got).
- `derived_table_alias_columns_preserve_refinement_elision` — `SELECT pid FROM (SELECT id FROM people) AS
  p(pid) WHERE pid < 0` over `PersonId CHECK (VALUE > 0)` → `EmptyRelation`, no scan.
- `derived_table_alias_columns_computed_projection_renames_but_is_not_transparent` — `SELECT computed FROM
  (SELECT id + 0 FROM people) AS p(computed) WHERE computed < 0` binds but does not elide.
- `derived_table_alias_columns_wildcard_remains_deferred` — `SELECT pid FROM (SELECT * FROM people) AS
  p(pid)` rejects with the named `SELECT *` fence.
- `derived_table_alias_columns_reject_type_annotations` — `SELECT pid FROM (SELECT id FROM people) AS
  p(pid BIGINT)` reaches the derived-alias helper and rejects with the type-annotation fence rather than
  acting like a cast/ascription.
- `derived_table_alias_columns_duplicate_names_inherit_ambiguity` — `AS p(dup, dup)` is accepted at rename
  time; referencing `dup` is ambiguous under existing binder rules.
- `derived_table_alias_columns_compose` — projection / `WHERE` / unqualified `ORDER BY` over renamed
  outputs work.

## Architecture guards

- **Retarget** `adr100_derived_table_source_is_narrow_not_general_subquery_support` — drop the categorical
  alias-column rejection assertion; keep all other ADR 100 fences.
- `adr103_derived_alias_columns_are_project_output_name_rewrites` — the helper arity-checks and rewrites
  `LogicalPlan::Project.output_names` from `TableAliasColumnDef.name.value` (not `Alias`) and rejects
  `data_type`.
- `adr103_alias_columns_do_not_extend_alias_or_ir` — `alias_plan` still builds `Alias` with only `TableRef`;
  binder still ignores `Alias` row shape; no new IR/exec variant.
- `adr103_alias_column_renames_reuse_transparent_project_receipt` — pins the elision gate + continued
  `transparent_project_child_ordinal` / `BoundExprNode::Column` use (no name heuristic).
- `adr103_wildcard_alias_columns_remain_deferred` — the `SELECT *` fence message.
- `adr103_no_runtime_cross_ring_or_information_schema_surface` — no PBC/executor/storage/coordinator/
  Wendelon/`information_schema` expansion.

## Scale boundary & seam check

Toron `sql_planner` only: move one rejection + add one arity/rename helper in `lower_derived_table`,
reusing the 089 project-rename and the 100 derived-source machinery. No new IR/exec/PBC/storage/Wendelon/
`information_schema` surface, no binder-semantics change. Smallest cut that closes the 100 deferral; the
wildcard case is a named deferral, not a silent gap.

## What to do next

- **Binder-carried wildcard alias-columns** — the deferred half (carry the alias list to bind time, rename
  after scan resolution); its own ADR.
- Carried: `information_schema.schemata` (tidy 102 reuse, thin until namespaces plural); Rune `ESCAPE` for
  `LIKE` (small frontend lead — `Expr::Like` has the field, SQL+Rune reject it); general derived
  tables/subqueries; PK/FK identity + `DROP`/`ALTER LOGICAL TYPE`; Rune joins / regex.

## Council history

102 promoted authoritative `information_schema.tables`/`.columns`. iter-0: Claude seeded derived-table
alias column lists (narrowest 100 follow-on + the 089/100 transparency tie-in); Codex grounded the seam +
the explicit-vs-wildcard scope correction (wildcard needs binder-carried arity); Gemini ratified; operator
approved explicit-only + the wildcard fence. iter-1 (dual): Claude/Codex/Gemini aligned on the
`lower_derived_table` rewrite of `Project.output_names`. iter-2: Claude folded in Codex's duplicate-alias
inheritance (D6) + the hide-original-names gate + the sharper `Alias`-not-extended guard; 3-of-3. Codex
review grounded the sqlparser 0.62 `TableAliasColumnDef` carrier and added the typed-alias rejection.
Migration: `plans/impl/0103_derived_table_alias_column_lists.md`.

## Promotion criteria

Explicit derived projections rename by alias column lists with exact arity checks (mismatch rejects before
binding); a bare-column rename preserves logical identity and refinement elision (100 receipt traces); a
computed rename binds without eliding; a wildcard alias-list rejects with the named fence; duplicate
aliases inherit existing ambiguity behavior; typed alias-column definitions reject rather than acting as
casts/ascriptions; all ADR 100 fences hold (only the categorical alias rejection retargeted); pure
`sql_planner`, no new IR/exec/PBC/storage/Wendelon/`information_schema` surface; Toron suite green.
Migration: `plans/impl/0103_derived_table_alias_column_lists.md`.
