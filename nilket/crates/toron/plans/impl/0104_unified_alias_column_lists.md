# Impl Plan — ADR 104: Unified Table Alias Column Lists at Bind

**Migration for** `plans/adr/0104_unified_alias_column_lists.md`. **Scope:** Toron `ir.rs` + `sql_planner`
+ `binder` + tests/guards. **Non-scope:** no `ExecutionPlan` variant, executor operator, PBC opcode,
storage/coordinator/Wendelon API, `information_schema` surface. One mechanism: planner carries names,
binder renames once.

## Pre-flight anchors (grounded)

| Concern | Anchor |
|---|---|
| Base-table silent-ignore (the bug) | `TableFactor::Table` arm (`sql_planner.rs:3335+`) never checks `alias.columns`; `alias_plan` drops them |
| 103 planner rewrite to delete | `apply_derived_alias_column_list` (`sql_planner.rs:5447`), called at `lower_derived_table` `:3331` |
| Wildcard has no plan-time Project | `lower_select` returns bare plan for `[SelectItem::Wildcard(_)]` `:2738` |
| Alias variant + binder arm | `LogicalPlan::Alias { input, alias }` `ir.rs:649`; bind arm `binder.rs:123` (`bind_plan(input)`, drops alias) |
| Rename primitives | `BoundExprProgramBuilder::append_column`, `set_node_logical_type`, `decode_column_logical_type`, `schema_for_exprs_with_output_names` |
| Transparency reuse | 089 metadata preservation; ADR 100 `transparent_project_child_ordinal` / `BoundExprNode::Column` |
| No project-folding | no merge/collapse pass; explicit-derived keeps a redundant identity `Project` (accepted; future general pass) |

## Phase 1 — Extend `LogicalPlan::Alias` (`ir.rs:649`)

```rust
Alias {
    input: Box<LogicalPlan>,
    alias: TableRef,
    columns: Vec<String>,   // empty = no rename (all prior behavior preserved)
}
```

Serialized with the rest of `LogicalPlan`. **Mini-sweep** exhaustive `LogicalPlan::Alias` matches → add
`columns`/`..`: `binder.rs` bind arm + `collect_qualifiers` (`:~2962`, uses `{ input, alias }`),
`sql_planner.rs` plan walkers, and any `exec.rs`/`audit.rs`/tests matches (most already use `..`; the
`adr104_alias_match_sweep_is_complete` guard pins this).

## Phase 2 — Planner: carry names for every alias; delete the 103 rewrite

- **Delete** `apply_derived_alias_column_list` (`:5447`) and its call at `lower_derived_table` (`:3331`).
- Add `alias_column_names` (rejects type annotations — D8):

```rust
fn alias_column_names(alias: &TableAlias) -> Result<Vec<String>, FrontendError> {
    if alias.columns.iter().any(|c| c.data_type.is_some()) {
        return Err(unsupported("table alias column type annotations are not supported"));
    }
    Ok(alias.columns.iter().map(|c| c.name.value.clone()).collect())
}
```

- Make `alias_plan` fallible and carry the names (covers base-table **and** derived in one place):

```rust
fn alias_plan(plan: LogicalPlan, alias: Option<&TableAlias>) -> Result<LogicalPlan, FrontendError> {
    match alias {
        Some(alias) => Ok(LogicalPlan::Alias {
            input: Box::new(plan),
            alias: TableRef::new(alias.name.value.clone()),
            columns: alias_column_names(alias)?,
        }),
        None => Ok(plan),
    }
}
```

- Call sites: base `TableFactor::Table` → `alias_plan(plan, alias.as_ref())?`; `lower_derived_table` →
  `alias_plan(plan, Some(alias))?` directly after the aggregate rejection (no rewrite, no planner arity
  check — D4).

## Phase 3 — Binder: one bind-time rename (`binder.rs`)

```rust
LogicalPlan::Alias { input, columns, .. } => {
    let input = self.bind_plan(input).await?;
    bind_alias_column_rename(input, columns)
}
```

```rust
fn bind_alias_column_rename(
    input: ExecutionPlan,
    columns: &[String],
) -> Result<ExecutionPlan, BindingError> {
    if columns.is_empty() { return Ok(input); }
    let schema = input.schema();
    if columns.len() != schema.fields().len() {
        return Err(BindingError::UnsupportedExpression {
            message: format!(
                "alias column list arity mismatch: expected {}, got {}",
                schema.fields().len(), columns.len()
            ),
        });
    }
    // ordinal identity project — build from source ordinals, NOT Expr::column(name) (avoids
    // duplicate-source-name ambiguity); preserve logical identity from each source field (D5/D6)
    let mut builder = BoundExprProgramBuilder::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let ordinal = ColumnOrdinal(u32::try_from(idx).expect("ordinal overflow"));
        let logical_type = decode_column_logical_type(field)
            .map_err(|e| BindingError::UnsupportedExpression { message: e.to_string() })?;
        let node = builder.append_column(ResolvedColumn {
            name: ColumnName(field.name().clone()),
            ordinal,
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            pbc_eligible: is_pbc_eligible_type(field.data_type()),
        });
        if !builder.set_node_logical_type(node, logical_type) {
            return Err(BindingError::UnsupportedExpression {
                message: "alias column rename target is malformed".to_string(),
            });
        }
        builder.push_root(node);
    }
    let exprs = builder.build();
    let output_names = columns.iter().cloned().map(Some).collect::<Vec<_>>();
    let output_schema = schema_for_exprs_with_output_names(&exprs, &output_names);
    Ok(ExecutionPlan::Project { input: Box::new(input), exprs, output_schema })
}
```

(Match the exact `ResolvedColumn`/builder field names against the existing `bind_expr_program` call sites.)

## Phase 4 — Behavior tests (`logical_type_ddl_test.rs`)

The 10 gates from the ADR: base/explicit/wildcard rename; hide-original-names; arity-mismatch-at-bind
(all three shapes — `plan` ok, `bind` errors with `arity mismatch`/`expected`/`got`); preserve-elision
(explicit **and** wildcard over `PersonId CHECK (VALUE > 0)`); computed-not-transparent;
reject-type-annotations (at planning); duplicate-inherit-ambiguity; ADR-100-fences-hold. Migrate the 103
alias-column gates into the `alias_columns_*` set; **retarget the 103 plan-time arity test** (explicit
arity now fails at bind, not plan).

## Phase 5 — Architecture guards (`tests/architecture/predicate_pushdown.rs`)

Retarget the three 103 guards (output-names-rewrite → `Alias.columns` carry; wildcard-deferred →
supported; alias-not-extended → `Alias` extended / `ExecutionPlan` not). Add the five `adr104_*`:
single-bind-time-mechanism (no `apply_derived_alias_column_list`; `alias_column_names` + `Alias.columns`
present); ordinal-identity-project (`bind_alias_column_rename` uses the four primitives, **no**
`Expr::column`); transparent-receipt-reuse (explicit + wildcard elision gates + `transparent_project_child_ordinal`);
no-exec/pbc/storage/wendelon/info_schema; alias-match-sweep-complete. Keep ADR 100 guards green.

## Phase 6 — Verification

```sh
cargo fmt
cargo test -p toron --test logical_type_ddl_test alias_columns
cargo test -p toron --test logical_type_ddl_test derived_table_source_scope
cargo test -p toron --test architecture_test adr104
cargo test -p toron --test architecture_test adr103
cargo test -p toron --test architecture_test adr100
cargo check -p toron
cargo test -p toron
git diff --check
```

## Acceptance

`LogicalPlan::Alias.columns` is the single representation; base-table, explicit-derived, and
wildcard-derived alias lists rename through one bind-time ordinal-identity `Project`; the base-table
silent-ignore is fixed; old names vanish; arity mismatch is a bind-time error with expected/got; typed
alias columns reject at planning; duplicates inherit ambiguity; explicit + wildcard refinement elision
trace through transparent receipts; 103's planner rewrite is deleted (its gates green through the unified
path); ADR 100 fences hold; no execution/PBC/storage/Wendelon/`information_schema` surface; Toron green.

## Risks / notes

- **Explicit-derived gains a redundant identity `Project`** (subquery `Project` + rename `Project`) — Toron
  has no project-folding. Accepted as the price of one mechanism; the right fix is a *future general*
  adjacent-identity-`Project` collapse pass, **not** a special-case branch (that reintroduces the split).
- **Ordinal identity project, not `Expr::column(name)`** — re-resolving by name breaks on duplicate source
  names; build from ordinals + set logical type from the source field metadata (D5).
- **Delete the 103 rewrite, don't leave it** — two mechanisms is the smell 104 removes; the 103 gates must
  re-verify through the bind path.
- **Base-table inclusion is bug-driven, not creep** — `alias_plan` dropping `alias.columns` is a silent
  wrong-behavior; the unified mechanism fixes it for free.
- **Mini-sweep the `LogicalPlan::Alias` matches** — a struct-variant field addition; the
  `..._alias_match_sweep_is_complete` guard pins it.
