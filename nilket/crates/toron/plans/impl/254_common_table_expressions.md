# Implementation Plan — ADR 254: Common Table Expressions (non-recursive `WITH`)

## Summary

Frontend-only desugar in `sql_planner.rs`: build a CTE scope (`name → lowered LogicalPlan`) from the
`WITH` clause, thread it through the lowering chain, and resolve a CTE-named `TableFactor::Table` to its
plan aliased as a named relation (before the catalog). Bodies lower via the **full query path**, not the
derived-table validator. No IR / binder / executor / fragmenter change.

## Verify-first probe (already done in iter-0 — recorded here)

`SELECT d.id, d.right_value FROM (SELECT left_t.id, right_t.right_value FROM left_t JOIN right_t ON
left_t.id = right_t.id) d WHERE d.id = 1` **binds** with the derived-table join fence lifted → the binder
presents a join-bodied subplan as a named relation (the `LogicalPlan::Alias` arm is structure-agnostic).
Receipt that CTEs stay frontend-only and the join fold is safe.

## Phase 1 — The CTE scope type + threading

- `CteScope<'a> = HashMap<String, &'a LogicalPlan>` (or a thin wrapper mirroring `NamedWindowMap`).
  Lowered bodies stay owned by the lowering frame; the scope is a borrowed view over the prefix that is
  already finalized for that point in the `WITH` list. An empty scope is a shared empty map, so the
  no-`WITH` path allocates nothing.
- Thread `cte: &CteScope` (explicit param, mirroring how `named_windows` threads — no hidden mutable
  state on the stateless `SqlPlanner`) through:
  - `lower_query_body(body, cte)` (`:3272`) → both arms pass it down.
  - `lower_select(select, cte)` (`:3426`) → passes to every `lower_table_factor` call (`:3575/3577/3617/3619`).
  - `lower_table_factor(factor, cte)` (`:4975`) → the resolution + the `TableFactor::Derived` arm.
  - `lower_derived_table(subquery, alias, cte)` (`:4955`) → so a derived table / subquery inside a body
    can reference a CTE; passes `cte` into its inner `lower_select`.
- `lower_query` (`:3224`) owns scope construction and passes the current borrowed view to the query body.

## Phase 2 — Build the scope in `lower_query`

Replace the `if with.is_some() { return Err("WITH is not supported") }` reject (`:3238`) with:

```
let mut owned: Vec<(String, LogicalPlan)> = Vec::new();
let mut scope = CteScope::default();
if let Some(with) = with {
    if with.recursive {
        return Err(unsupported("WITH RECURSIVE is not supported"));
    }
    for cte in &with.cte_tables {
        let name = cte.alias.name.value.clone();
        // each body lowers with the EARLIER CTEs visible (linear scope); self/forward refs miss the map
        let body = self.lower_query(&cte.query /* via a cte-aware lower */, &scope)?;
        let aliased = alias_plan(body, Some(&cte.alias))?; // column-alias list reuse
        if owned.iter().any(|(n, _)| n == &name) {
            return Err(unsupported(format!("duplicate WITH name '{name}'")));
        }
        owned.push((name, aliased));
        scope = CteScope::from(&owned); // rebuild the borrowed view after each push
    }
}
let LoweredSelect { plan, aggregate_output_catalog } = self.lower_query_body(body, &scope)?;
```

(Borrow mechanics: keep one owner (`owned: Vec<(String, LogicalPlan)>`) and rebuild the cheap borrowed
`HashMap<String, &LogicalPlan>` view as needed, rather than trying to store self-referential borrows. A
CTE body is lowered through the **full** path — `lower_query` over `cte.query` so a CTE body may itself
carry ORDER BY/LIMIT and set-ops — with the partial scope threaded in.)

Each CTE body is aliased once via `alias_plan(body, Some(&cte.alias))`, which already applies the
`WITH a(x, y)` column-rename through `alias_column_names`. Resolving a reference then **clones** the
aliased plan (inline-per-reference).

## Phase 3 — Resolve CTE references in `lower_table_factor` (shadow before catalog)

In the `TableFactor::Table` arm (`:4977`), **before** `lower_table_ref_object_name` / `ScanNamed`
(`:5015`): if `name` is a bare single-ident matching a CTE in `cte`, return a clone of the scoped plan,
re-aliased to the *reference's* alias if present (`SELECT … FROM a AS t`), else under the CTE name.

```
if let Some(cte_plan) = name.0.len()==1.then(|| cte.get(&name.0[0].value)).flatten() {
    let plan = (*cte_plan).clone();
    return match alias { Some(a) => alias_plan(plan, Some(a)), None => Ok(plan) };
}
```

(A multi-part name like `schema.t` never matches a CTE — CTE names are unqualified.) The catalog
`ScanNamed` path is unchanged for non-CTE names.

## Phase 4 — Lift the derived-table joins-inside fence (in scope)

In `validate_derived_select_shape`, remove the `joins inside derived tables are not supported` reject (the
probe proved the binder handles it). Keep the **nested-derived** and **lateral** rejects (ungrounded —
separate fences, still deferred). Same "subplan as a named relation" capability as CTEs, exercised from a
different syntax entry point — folding it keeps the cut honest and avoids a pointless split. Add
`derived_table_join_body_executes` to the existing derived-table tests, and reconcile any architecture
guard that pins the removed reject string.

## Phase 5 — Tests, guards, verification, seal

### Behavioral gates (`tests/cte_test.rs`, new — added to the adr176 allowlist)

`cte_resolves_like_inline_subquery`, `cte_body_with_join_binds_and_executes`,
`multiple_ctes_later_references_earlier`, `cte_referenced_in_a_join`, `cte_shadows_catalog_table`,
`cte_column_alias_list_renames_output`, `recursive_and_self_reference_ctes_stay_rejected`. Need a
two-table execute fixture (model on `hash_join_kinds_test` / a catalog with a shadow-name table). If
Phase 4 lands, add `derived_table_join_body_executes` to the existing derived-table tests.

### Architecture guards (`tests/architecture/` — new `cte.rs` module, or fold into an existing surface file)

- `adr254_cte_resolves_in_lowering` — `lower_query` builds a CTE scope; the `WITH is not supported` reject
  is gone; `lower_table_factor` consults the scope before `ScanNamed`; bodies lower via `lower_query`/
  `lower_query_body`, not `validate_derived_select_shape`.
- `adr254_cte_is_frontend_only` — no `CteScope` / cte resolution in `ir.rs` / `binder.rs` / `exec.rs` /
  `distributed/fragment.rs`.
- `adr254_recursive_cte_stays_deferred` — `WITH RECURSIVE is not supported` reject + the gate name pinned.

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets           # signature-threading touches many call sites
scripts/cargo-cgroup test  -p toron --test cte_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr254
scripts/cargo-cgroup test  -p toron --test architecture_test               # FULL (new test file → adr176 allowlist; reject-string scanners)
scripts/cargo-cgroup test  -p toron --test sql_select_test                 # derived-table / FROM regression
scripts/cargo-cgroup test  -p toron --test distributed_process_test        # regression; no new gate
scripts/cargo-cgroup test  -p toron --lib                                  # no IR/wire change
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 254 common table expressions`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **Threading churn** is the bulk — a `&CteScope` param on `lower_query_body` / `lower_select` /
  `lower_table_factor` / `lower_derived_table`. `--all-targets` is the catch-all for the signature change.
  It is the feature, not avoidable churn (the scope must be live at table-factor resolution).
- **adr176 git-status guard** fails pre-commit on the new `tests/cte_test.rs` until it is in the
  `later_adr_test_and_runtime_surfaces` allowlist (or accepted as transient pre-commit; resolves on commit).
- **Borrow mechanics** of the incremental scope: keep one owner (`owned: Vec<(String, LogicalPlan)>`) and
  rebuild the cheap `HashMap<&str, &LogicalPlan>` view, rather than threading mutable borrows.
- **Shadowing correctness**: the CTE check must precede `lower_table_ref_object_name`, and only match
  single-ident names — a qualified `schema.t` is never a CTE.
- **Reject-string scanners**: removing `WITH is not supported` / `joins inside derived tables` may trip an
  architecture guard that pins those strings (grep `adr*` for them first — reconcile like the 250/252
  deferral guards in 253).
