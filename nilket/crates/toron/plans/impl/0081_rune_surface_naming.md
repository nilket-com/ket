# Impl Plan 081: Rune Surface Naming — Delete the `.select` Keyword Shim

**Status:** Promoted. Paired with
`plans/adr/0081_rune_surface_naming.md`. Builds on the landed ADR 080 feat
(`12f370f`). Genuine 3-of-3.
**Posture:** Behavior-zero refinement — a rename + deletions in
`crates/toron/src/rune_engine.rs`, a respell of 8 fixtures, and a guard
reconciliation. No new production code paths, no production machinery added.

## Implementation invariants

- Composed `LogicalPlan` and all execution results are **byte-identical** to 080 —
  only the authored method spelling changes (`.select` → `.project_named`).
- No source-text rewriting survives; `compile_script` compiles the user script
  verbatim.
- `RuneNamedExpr`, `RuneExpr::named`, aggregate helper pack, `Project.output_names`
  lowering, `ctx.sql`, SQL grammar, binder, IR — all unchanged.

## Phase 0 — respell fixtures first (`tests/rune_relation_handles_test.rs`)

Replace the **6 `.select(` call-sites** across the 8 ADR 080 fixtures with
`.project_named(`. These must stay green with **identical** assertions. Rename the
fixtures that encode the old surface so the behavior net no longer advertises
`.select(...)`:

- `rune_named_sql_multi_hop_continuation_binds_and_executes` ->
  `rune_project_named_sql_multi_hop_continuation_binds_and_executes`
- `rune_named_select_sets_project_output_names` ->
  `rune_project_named_sets_project_output_names`
- `rune_named_select_binds_visible_schema` ->
  `rune_project_named_binds_visible_schema`
- `rune_aggregate_helper_pack_binds_stable_outputs` unchanged
- `rune_named_post_aggregate_select_renames_outputs` ->
  `rune_project_named_post_aggregate_renames_outputs`
- `rune_duplicate_aggregate_output_name_is_ambiguous` unchanged
- `rune_named_select_rejects_misused_named_expression` ->
  `rune_named_expr_rejects_misused_named_expression`
- `rune_named_projection_preserves_adr079_sql_relation_guards` unchanged

They fail until Phase 1-3 land; that is the behavior-zero proof.

## Phase 1 — rename the method (`rune_engine.rs`)

`RuneDataFrame::select` → `RuneDataFrame::project_named` (body unchanged):

```rust
pub fn project_named(self, exprs: Vec<RuneNamedExpr>) -> Self {
    let mut plan_exprs = Vec::with_capacity(exprs.len());
    let mut output_names = Vec::with_capacity(exprs.len());
    for named in exprs { plan_exprs.push(named.expr); output_names.push(Some(named.name)); }
    Self { plan: LogicalPlan::Project { input: Box::new(self.plan), exprs: plan_exprs, output_names } }
}
```

## Phase 2 — delete the shim (`rune_engine.rs`)

- Remove the two `compile_script` normalization lines:
  ```rust
  let normalized_script = normalize_rune_named_select_surface(script);
  let script = normalized_script.as_deref().unwrap_or(script);
  ```
  → `compile_script` uses the incoming `script` directly.
- Delete `normalize_rune_named_select_surface`, `skip_rune_quoted`,
  `skip_rune_line_comment`, `skip_rune_block_comment`, `is_ascii_ident_continue`.
- Delete the unit test `named_select_surface_normalizes_keyword_method_only_in_code`.

## Phase 3 — registration (`create_toron_module`)

Replace both lines:
```rust
m.associated_function("select", RuneDataFrame::select)?;        // delete (dead)
m.associated_function("select_named", RuneDataFrame::select)?;  // delete
```
with the single:
```rust
m.associated_function("project_named", RuneDataFrame::project_named)?;
```

## Phase 4 — reconcile guards (`tests/architecture/rune_frontend.rs`, discipline #4)

- **`adr080_named_select_lowers_to_existing_project_output_names`** (`:328`):
  - `source_block(&rune, "pub fn select")` → `"pub fn project_named"`.
  - Delete the `normalize` block read (`:331`) and the three shim assertions
    (`:340-342`: `FROM`, `TO`, `compile_script.contains("normalize_…")`).
  - Keep `:334-339` (`exprs: Vec<RuneNamedExpr>`, `LogicalPlan::Project`,
    `output_names.push(Some(named.name))`, `plan_exprs.push(named.expr)`, no
    `LogicalPlan::Join`, no `Aggregate`).
  - Rename the guard `…named_select_lowers…` → `…named_projection_lowers…`
    (keep the `adr080_` provenance prefix).
- **`adr080_rune_sandbox_allow_list_remains_toron_authoring_only`** (`:515-516`):
  replace both registration assertions with
  `module.contains("m.associated_function(\"project_named\", RuneDataFrame::project_named)?;")`.
- **Add `adr081_rune_frontend_does_not_rewrite_user_source`:**
  ```rust
  let rune = rune_engine_source();
  let compile = source_block(&rune, "fn compile_script");
  assert!(!rune.contains("fn normalize_rune_named_select_surface"));
  assert!(!compile.contains("normalize_"));
  ```
- **Add `adr081_authored_rune_methods_avoid_reserved_keywords`:** read
  `create_toron_module`, collect the `associated_function`/`function` name-string
  literals, assert none equals a Rune keyword (at minimum `"select"`, `"as"`); a
  small embedded keyword list is fine.

## Phase 5 — verification (reuse the 080 net)

```
cargo fmt
cargo test -p toron --test rune_relation_handles_test     # 16, gate row east,30,1
cargo test -p toron --test architecture_test adr080_      # retargeted, still green
cargo test -p toron --test architecture_test adr081_      # 2 new durable guards
cargo test -p toron --test rune_sandbox_test
cargo test -p toron --test group_by_test                  # 125 (079 anchor intact)
cargo clippy -p toron -- -D warnings
git diff --check
cargo test -p toron                                       # full package
```

## Evidence doc (`plans/impl/0081_rune_surface_naming_evidence.md`)

Record: files touched (`rune_engine.rs` + the two test files); lines deleted (shim
+ unit test ≈ ~95); the gate still binds+executes to `east,30,1` (behavior-zero);
the two reconciled 080 guards + two new 081 guards; commands + counts.

## Watch items

1. **No behavior drift** — the only `.select`→`.project_named` change is the
   spelling; assert the gate row and duplicate-name error are unchanged.
2. **No orphaned `select` references** — grep `rune_engine.rs` for `select`
   post-change; only legitimate uses (none expected) may remain. The
   `adr081_*` keyword guard backstops this.
3. **`compile_script` shape** — confirm removing the two normalization lines leaves
   a clean `let mut sources = Sources::new();` entry; no dangling `normalized_script`.
4. **Guard rename** — if renaming `named_select`→`named_projection`, update the
   `core_invariants.rs` / cross-module guard-name registries if any reference it by
   name (077 corpus convention).

## Acceptance checklist

- [ ] `.project_named` is the only authored named-projection surface; `.select(...)`
      no longer parses.
- [ ] Shim (rewriter + 4 helpers + unit test + `compile_script` call) fully deleted.
- [ ] Single `"project_named"` registration; no `"select"`/`"select_named"`.
- [ ] 8 fixtures green, gate row `east,30,1`, duplicate-name `AmbiguousColumn`.
- [ ] Two 080 guards reconciled; two `adr081_*` durable guards added and green.
- [ ] Full package + clippy + `git diff --check` clean.
