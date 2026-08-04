# Impl Plan — ADR 103: Derived-Table Alias Column Lists (explicit-projection)

**Migration for** `plans/adr/0103_derived_table_alias_column_lists.md`. **Toron `sql_planner` only.** Move one
rejection + add one arity/rename helper in `lower_derived_table`. Reuse the 089 project-rename + the ADR 100
derived-source machinery. **No** IR/bound-IR/binder-semantics/executor/PBC/storage/Wendelon/`information_schema`
change.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Rejection to move | `lower_table_factor` `TableFactor::Derived` arm: `if !alias.columns.is_empty()` `sql_planner.rs:3394` |
| Seam (select + plan) | `lower_derived_table` — `lower_select(select) -> LoweredSelect { plan }` → `alias_plan(plan, Some(alias))` |
| Alias is metadata-free | `alias_plan` → `LogicalPlan::Alias { input, alias: TableRef::new(alias.name.value) }` |
| Binder ignores Alias row shape | `binder.rs:123` — `LogicalPlan::Alias { input, .. } => self.bind_plan(input).await` |
| Explicit→Project / wildcard→bare | `lower_select`: explicit `Project { exprs, output_names }` `:2766`; wildcard bare plan, no Project `:2738` |
| Parser carrier | sqlparser 0.62 `TableAlias.columns: Vec<TableAliasColumnDef>`; use `column.name.value`, reject `column.data_type` |
| Transparent-rename reuse | 089 `schema_for_exprs_with_output_names`; ADR 100 `transparent_project_child_ordinal` / `BoundExprNode::Column` |

## Phase 1 — Move the rejection into `lower_derived_table`

- In `lower_table_factor`'s `Derived` arm: **remove** the categorical `alias.columns` rejection (`:3394`);
  keep required-alias, non-lateral, no-sample, no-`alias.at`.
- In `lower_derived_table`, after `lower_select`:

```rust
let LoweredSelect { plan, aggregate_output_catalog } = self.lower_select(select)?;
if aggregate_output_catalog.is_some() {
    return Err(unsupported("aggregate derived tables are not supported"));
}
let plan = apply_derived_alias_column_list(plan, &alias.columns)?;   // no-op when empty
Ok(alias_plan(plan, Some(alias)))
```

## Phase 2 — `apply_derived_alias_column_list` helper

```rust
fn apply_derived_alias_column_list(
    plan: LogicalPlan,
    alias_columns: &[sqlparser::ast::TableAliasColumnDef],
) -> Result<LogicalPlan, FrontendError> {
    if alias_columns.is_empty() {
        return Ok(plan);                                   // ADR 100 behavior, unchanged
    }
    if alias_columns.iter().any(|column| column.data_type.is_some()) {
        return Err(unsupported(
            "derived table alias column type annotations are not supported",
        ));
    }
    match plan {
        LogicalPlan::Project { input, exprs, output_names } => {
            if alias_columns.len() != output_names.len() {
                return Err(unsupported(format!(
                    "derived table alias column list arity mismatch: expected {}, got {}",
                    output_names.len(), alias_columns.len()
                )));
            }
            let output_names = alias_columns.iter().map(|c| Some(c.name.value.clone())).collect();
            Ok(LogicalPlan::Project { input, exprs, output_names })   // positional overwrite
        }
        _ => Err(unsupported(
            "alias column lists over SELECT * derived tables are not supported",
        )),
    }
}
```

Notes:
- Arity source is `output_names.len()` (explicit projection). Rejects too-many and too-few, before binding.
- Alias-list entries are names only: reject `TableAliasColumnDef.data_type` instead of treating it as a
  cast/ascription/validation rule.
- **No transparency inspection here** — the helper only renames fields (rename ≠ transparency).
- **No duplicate-alias rejection** (D6) — `AS p(x, x)` is accepted; existing duplicate-output-name
  ambiguity handles later references. Adding a uniqueness rule would be new policy, inconsistent with
  current behavior.
- The `_` arm is intentionally broader than "wildcard" — any non-`Project` lowered shape has no
  planner-owned `output_names` to rewrite; under ADR 100 validation that is the wildcard case.

## Phase 3 — Behavior tests (beside the ADR 100 derived-table cases, `logical_type_ddl_test.rs`)

- `derived_table_alias_columns_rename_output` — `SELECT pid FROM (SELECT id FROM people) AS p(pid)`; output
  field is `pid`.
- `derived_table_alias_columns_hide_original_names` — `SELECT id FROM (SELECT id FROM people) AS p(pid)` →
  `BindingError::UnknownColumn { column: "id" }` / `unknown column: id`.
- `derived_table_alias_columns_arity_mismatch_rejects` — `AS p(a, b)` over 1 col **and** `AS p(a)` over 2
  cols; assert the error contains `arity mismatch`, `expected`, `got`.
- `derived_table_alias_columns_preserve_refinement_elision` — `setup_person_id_people`; `SELECT pid FROM
  (SELECT id FROM people) AS p(pid) WHERE pid < 0` → `find_empty_relation(&bound).is_some()` &&
  `!contains_scan(&bound)`.
- `derived_table_alias_columns_computed_projection_renames_but_is_not_transparent` — `SELECT computed FROM
  (SELECT id + 0 FROM people) AS p(computed) WHERE computed < 0` → `find_empty_relation` none, scan present.
- `derived_table_alias_columns_wildcard_remains_deferred` — `SELECT pid FROM (SELECT * FROM people) AS
  p(pid)` → error contains `alias column lists over SELECT * derived tables are not supported`.
- `derived_table_alias_columns_reject_type_annotations` — `SELECT pid FROM (SELECT id FROM people) AS
  p(pid BIGINT)` reaches the derived-alias helper and rejects with the type-annotation fence; it is not a
  cast or logical ascription.
- `derived_table_alias_columns_duplicate_names_inherit_ambiguity` — `SELECT dup FROM (SELECT id, note FROM
  people) AS p(dup, dup)` → existing `AmbiguousColumn` error for `dup`.
- `derived_table_alias_columns_compose` — `SELECT label FROM (SELECT id, note FROM people) AS p(pid, label)
  WHERE pid > 0 ORDER BY label` (unqualified `ORDER BY`, inside existing sort-key support).

## Phase 4 — Architecture guards (`predicate_pushdown.rs` / the ADR 100 derived-guard module)

- **Retarget** `adr100_derived_table_source_is_narrow_not_general_subquery_support` — remove the
  `"derived table alias column lists are not supported"` assertion; keep lateral/sample/required-alias/
  `alias.at`/nested/joins/aggregate-window/group-having/sort-limit-offset/CTE-set assertions (+ retain the
  remaining general-subquery rejection test-name assertion).
- `adr103_derived_alias_columns_are_project_output_name_rewrites` — `apply_derived_alias_column_list`
  matches `LogicalPlan::Project`, checks `alias_columns.len() != output_names.len()`, rebuilds
  `output_names` from `column.name.value`, rejects `column.data_type`, returns the wildcard fence in the
  non-`Project` arm.
- `adr103_alias_columns_do_not_extend_alias_or_ir` — `alias_plan` still builds `Alias` with only
  `TableRef::new`; binder still has `LogicalPlan::Alias { input, .. } => self.bind_plan(input)`; `ir.rs` /
  `ir/execution.rs` gain no alias-column/derived variant.
- `adr103_alias_column_renames_reuse_transparent_project_receipt` — pins
  `derived_table_alias_columns_preserve_refinement_elision`; binder still uses
  `transparent_project_child_ordinal`; the helper mentions no `LogicalTypeId` / `decode_column_logical_type`
  / name heuristic.
- `adr103_wildcard_alias_columns_remain_deferred` — the helper contains the `SELECT *` fence string; the
  behavior-test name exists.
- `adr103_no_runtime_cross_ring_or_information_schema_surface` — no ADR 103 helper/string in `pbc.rs` /
  executor / storage / coordinator / Wendelon / `information_schema` branches; no new `ExecutionPlan`
  variant.

## Phase 5 — Verification

```sh
cargo test -p toron --test logical_type_ddl_test derived_table_alias_columns   # the 9 gates
cargo test -p toron --test architecture_test adr103                            # new 103 guards
cargo test -p toron --test architecture_test adr100                            # retargeted 100 fences still green
cargo test -p toron                                                            # full suite, no regression
```

## Acceptance

Explicit derived projections support positional alias-column renames; arity mismatch rejects before
binding; wildcard alias-lists reject with the named fence; old names disappear at the derived boundary;
typed alias-column definitions reject rather than acting as casts/ascriptions; duplicate aliases inherit
existing ambiguity behavior; bare-column renames still elide through ADR 100 provenance; computed renames
bind without eliding; all ADR 100 fences hold (only the categorical alias rejection retargeted); no
IR/binder/executor/PBC/storage/Wendelon/`information_schema` surface added; Toron suite green.

## Risks / notes

- **Move the rejection, don't duplicate it** — leaving the `:3394` block fails every gate before the helper
  runs.
- **Don't carry alias columns on `LogicalPlan::Alias`** — dead metadata (binder ignores Alias row shape);
  the only live representation is `Project.output_names`.
- **Don't guess wildcard arity in the planner** — that shape needs a future binder-aware design; the `_`
  arm fences it.
- **Don't add duplicate-alias validation** — new policy, inconsistent with existing duplicate-output-name
  ambiguity; D6 inherits it.
- **Arity message** — arity-specific (expected vs got) so both over- and under-naming give a clear error.
