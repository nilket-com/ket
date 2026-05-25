# ADR 104: Unified Table Alias Column Lists at Bind

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on Fork B (unified
bind-time rename) after the operator's small-cut-bias steer; the **decisive grounding** is the base-table
silent-ignore (below), which makes the binder rename mandatory regardless and makes "just the wildcard
half" (A) incomplete. **Toron-only** (IR + planner + binder); no executor/PBC/storage/Wendelon/
`information_schema` surface. **Predecessors:** 100 (derived-source + transparent receipt), 103
(explicit-derived alias columns — *superseded* here), 089 (metadata-preserving rename). **Migration:**
`plans/impl/104_unified_alias_column_lists.md`.

## Doctrine — an alias column list is relation-output rename metadata; rename once, at bind

```sql
FROM people AS p(pid, label)                        -- base table
FROM (SELECT id, note FROM people) AS p(pid, label) -- explicit derived
FROM (SELECT * FROM people) AS p(pid, label)         -- wildcard derived
```

All three mean the same thing: positionally rename the relation's output columns at the alias boundary.
The **binder** is the first tier where every case has a resolved field count + logical-type metadata, so
the binder owns the rename. The planner owns syntax validation and name carriage. One mechanism, three
call shapes — not a planner path for explicit + a separate binder path for wildcard.

## Grounding (verified)

- **Base-table alias columns are silently dropped today — a real gap (the decisive fact).** The
  `TableFactor::Table` arm (`sql_planner.rs:3335+`) validates `args`/hints/`version`/`sample`/… but
  **never `alias.columns`**, then calls `alias_plan(plan, alias.as_ref())`, which builds
  `LogicalPlan::Alias { input, alias: TableRef }` **dropping the column list**. So `SELECT pid FROM people
  AS p(pid)` silently ignores the rename and `pid` fails to resolve. The only alias-column handling is
  103's *derived* path (`apply_derived_alias_column_list`, `:3331`). Base-table has **no plan-time
  `Project`** (a `Scan`), so — like wildcard — it can only be renamed at bind.
- **Wildcard derived has no plan-time Project either.** `lower_select` returns the bare plan for
  `[SelectItem::Wildcard(_)]` (`:2738`); arity is binder-known.
- **So the binder-rename path is mandatory** for base-table **and** wildcard. Fork A (keep 103's planner
  rewrite + add a wildcard binder path) is two mechanisms + a branch that *still* leaves base-table broken
  or separately fixed. B is the single mechanism covering all three and fixing the gap.
- **`Alias` carries no columns; binder discards it.** `LogicalPlan::Alias { input, alias }` (`ir.rs:649`);
  `binder.rs:123` `LogicalPlan::Alias { input, .. } => self.bind_plan(input)`. 104 extends it.
- **The binder has the rename primitives.** `BoundExprProgramBuilder::append_column`,
  `set_node_logical_type`, `decode_column_logical_type`, `schema_for_exprs_with_output_names`.
- **No project-folding in Toron** (no merge/collapse pass; `optimize_for_required` prunes columns but
  doesn't fold `Project(Project)`). So under B the explicit-derived case keeps a redundant identity
  `Project` (subquery `Project` + rename `Project`). Accepted as B's price (tiny, transparent); the right
  fix is a *future general* project-collapse pass, **out of 104 scope** — not a special-case branch here
  (that reintroduces the split).

## Decisions (promotion-locked, 3-of-3)

**D1 — Extend `LogicalPlan::Alias` with `columns: Vec<String>`.** Empty = no rename (preserves all prior
behavior). LogicalPlan-shape change only — no `ExecutionPlan` variant. Triggers a mini-sweep of exhaustive
`LogicalPlan::Alias { input, alias }` matches → add `columns`/`..` (e.g. `collect_qualifiers`).

**D2 — The planner carries names for *every* table alias (base + derived); it does not rename.** A fallible
`alias_plan` (via `alias_column_names`) rejects type annotations and stores `alias.columns[*].name.value`
in `Alias.columns`, for base-table and derived aliases alike. Closes the base-table silent-ignore.

**D3 — Delete ADR 103's planner rewrite.** Remove `apply_derived_alias_column_list` and its
`lower_derived_table` call; explicit, wildcard, and base-table alias lists all flow through `Alias.columns`
+ the one binder rename. (103's behavior gates re-verify through the unified path — zero regression.)

**D4 — Arity validated at bind for *all* alias lists (fully unified — no planner arity special-case).** A
plan-time arity check for explicit would preserve the split B removes; the bind-time check
(`columns.len() == bound_schema.fields().len()`) is identical for all three. `BindingError::UnsupportedExpression`
with `arity mismatch` / `expected` / `got`. (Explicit arity errors surface at bind, not plan — same
pre-execution phase from the user's view.)

**D5 — Bind-time rename is an *ordinal* identity `Project`.** Build the `BoundExprProgram` from source
*ordinals* (`append_column` + `set_node_logical_type` from each source field's `decode_column_logical_type`)
+ `schema_for_exprs_with_output_names`. **Do not** rebuild `Expr::column(field.name())` and re-resolve by
name — that turns duplicate *source* names into a pre-rename ambiguity. Returns `ExecutionPlan::Project`,
no new variant.

**D6 — Transparency inherited, not special-cased.** Synthesized roots are `BoundExprNode::Column`, so 089
metadata preservation and ADR 100 `transparent_project_child_ordinal` receipt tracing work for base,
explicit, and wildcard renames alike (renamed bare column still elides; renamed computed expr stays
non-transparent).

**D7 — Duplicate alias names inherit existing ambiguity.** `AS p(dup, dup)` binds the rename; a later
unqualified `dup` reuses `AmbiguousColumn`. No new uniqueness rule.

**D8 — Type annotations reject at planning, everywhere.** `AS p(pid BIGINT)` does not cast/ascribe — a
table-alias-column type-annotation fence in `alias_column_names`.

**D9 — ADR 100 derived-source fences intact.** Required alias, non-lateral, no sample, no nested derived,
no joins inside/around, no aggregate/GROUP/HAVING/window/sort/limit/offset/CTE/set. 104 does not generalize
derived tables.

**D10 — No runtime or cross-ring surface.** No bound-IR node, `ExecutionPlan` variant, PBC opcode, executor
operator, storage/coordinator/Wendelon API, or `information_schema` relation. Scope is wider than the
"wildcard half" framing (it includes base-table aliases + deletes 103's path), but the widening is
bug-driven: building the binder mechanism and declining to fix the base-table gap it covers would leave
the alias-column feature structurally split and observably incomplete.

## Behavior gate

- `alias_columns_base_table_rename_output` — `SELECT pid FROM people AS p(pid, …)` exposes `pid` (was
  silently ignored before 104).
- `alias_columns_explicit_derived_rename_output` — `SELECT pid FROM (SELECT id FROM people) AS p(pid)`.
- `alias_columns_wildcard_derived_rename_output` — `SELECT pid FROM (SELECT * FROM people) AS p(pid, …)`.
- `alias_columns_hide_original_names` — after `AS p(pid, …)`, `SELECT id …` →
  `BindingError::UnknownColumn { column: "id" }`.
- `alias_columns_arity_mismatch_rejects_at_bind` — base / explicit / wildcard: `plan` succeeds, `bind`
  fails with `arity mismatch` + `expected` + `got`.
- `alias_columns_preserve_refinement_elision_explicit_and_wildcard` — explicit *and* wildcard renames over
  `PersonId CHECK (VALUE > 0)`, `WHERE pid < 0` → `EmptyRelation`, no scan.
- `alias_columns_computed_projection_still_not_transparent` — renamed computed projection binds, doesn't elide.
- `alias_columns_reject_type_annotations` — `AS p(pid BIGINT)` rejects at planning.
- `alias_columns_duplicate_names_inherit_ambiguity` — `AS p(dup, dup)` binds; later `dup` is ambiguous.
- `derived_source_fences_still_hold` — ADR 100 restrictions green.

## Architecture guards

- **Retarget** the three ADR 103 guards: `..._are_project_output_name_rewrites` → names carried on
  `Alias.columns` (no planner rewrite helper remains); `..._wildcard_alias_columns_remain_deferred` →
  wildcard **supported**; `..._alias_columns_do_not_extend_alias_or_ir` → **`Alias` is extended;
  `ExecutionPlan` is not** (inverted-guard discipline #4).
- `adr104_alias_column_lists_are_single_bind_time_mechanism` — `apply_derived_alias_column_list` gone;
  `alias_column_names` + `Alias.columns` present; planner carries, doesn't rename.
- `adr104_binder_alias_columns_build_ordinal_identity_project` — `bind_alias_column_rename` uses
  `append_column`/`set_node_logical_type`/`decode_column_logical_type`/`schema_for_exprs_with_output_names`;
  does **not** contain `Expr::column`.
- `adr104_alias_column_renames_reuse_transparent_project_receipt` — explicit + wildcard elision gates trace
  through `transparent_project_child_ordinal` / `BoundExprNode::Column`.
- `adr104_no_execution_pbc_storage_wendelon_information_schema_surface`.
- `adr104_alias_match_sweep_is_complete` — no stale `LogicalPlan::Alias { input, alias }` omitting
  `columns`/`..`.

## Scale boundary & seam check (honest)

Wider than 103: a `LogicalPlan::Alias` field + a binder rename + the planner name-carry + **deleting 103's
planner rewrite** + the base-table fix + the Alias-match mini-sweep + retargeting three 103 guards. Still
bounded and Toron-local — the rename reuses `Project` (no exec variant) and the 089 path; no
executor/PBC/storage/Wendelon surface. "Refactoring travels with the ADR," and it closes a real latent bug
instead of preserving a narrower split.

## What to do next

- General derived-table support (joins-around, aggregates, sort/limit, nested, CTE/set, correlated) —
  still separate.
- A **general adjacent-identity-`Project` collapse** optimizer pass would remove the explicit-derived
  redundant node (and help elsewhere) — its own small ADR, not 104's job.
- `information_schema.schemata` + the 102 introspection follow-ons remain carried.

## Council history

Operator teed "the deferred binder-carried wildcard-alias half." Iter 0: all three leaned A (wildcard-only
binder path, keep 103's planner rewrite). Operator flagged a small-cut/structure-ossification bias and
asked whether the narrower cut was genuine scope or just the smaller diff. Grounding (Codex surfaced +
Claude confirmed): **base-table alias columns are silently dropped** → the binder path is mandatory for
base + wildcard → A keeps two mechanisms *and* leaves base-table broken; **B unifies all three at bind and
fixes the bug.** All three flipped to B; within B, arity stays fully unified at bind after the earlier
retained-planner-check variant was resolved against. The lesson carried forward is to ground the full
alias-column surface before treating the narrowest diff as the architecture. Iter 2: Codex + Gemini duals
matched Claude iter-1; 3-of-3. Migration:
`plans/impl/104_unified_alias_column_lists.md`.

## Promotion criteria

`LogicalPlan::Alias.columns` is the single representation; base-table, explicit-derived, and
wildcard-derived alias lists all rename through one bind-time ordinal-identity `Project`; the base-table
silent-ignore is fixed; old names vanish at the boundary; arity mismatch is a bind-time error with
expected/got; typed alias columns reject at planning; duplicates inherit ambiguity; explicit + wildcard
refinement elision still trace through transparent receipts; 103's planner rewrite is gone (its gates green
through the unified path); ADR 100 fences hold; no execution/PBC/storage/Wendelon/`information_schema`
surface; Toron suite green. Migration: `plans/impl/104_unified_alias_column_lists.md`.
