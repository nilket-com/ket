# Impl Plan — ADR 121: General Qualified Relation-Scope Binding

**Migration for** `plans/adr/0121_general_qualified_relation_scope_binding.md`. **Scope:** Toron —
`binder.rs` (single resolver + `RelationScope` sidecar + scope propagation + named error surface),
`sql_planner.rs` (5 scalar reject-site conversions; 4 wildcard/RETURNING rejects stay),
`rune_engine.rs` (`alias` + free `qcol`). **Non-scope:** IR; executor; PBC; Wendelon; SQL grammar;
JOIN semantics; **qualified wildcards; qualified DML RETURNING; DML target aliases; Rune
`.join(...)`; GROUP BY over joins; joins-around-derived; CTEs / set ops / windows / lateral.**

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Qualifier struct (pre-existing) | `ir.rs:474-489` `ColumnRef { qualifier: Option<TableRef> }` |
| JOIN-key resolver (reference impl) | `binder.rs:3130 resolve_join_column`; `:3281 resolve_join_column_across`; matching at `:3135` / `:3296` |
| Binder catalog access | `binder.rs:62/70` `catalog: &'a dyn Catalog` |
| **5 scalar reject sites (convert in 121, gone per G1)** | |
| ORDER BY | `binder.rs:1249-1251` `"ORDER BY only supports unqualified output column names"` |
| GROUP BY key | `sql_planner.rs:4944` `"GROUP BY keys must be simple unqualified columns"` |
| HAVING | `sql_planner.rs:4952` `"qualified HAVING column references are not supported"` |
| Aggregate projection | `sql_planner.rs:5037` `"qualified aggregate projection references are not supported"` |
| Visible-names gate | `sql_planner.rs:4135` `column.qualifier.is_some() \|\| !visible_names.contains(...)` |
| **4 wildcard/RETURNING reject sites (stay verbatim per G2)** | |
| SELECT wildcard | `sql_planner.rs:5113` `"qualified wildcard projections are not supported"` |
| Mixed wildcard | `sql_planner.rs:2797` `"mixed wildcard projections are not supported"` |
| RETURNING column | `sql_planner.rs:1796` `"qualified RETURNING columns are not supported"` |
| RETURNING wildcard | `sql_planner.rs:1777` `"qualified wildcard RETURNING projections are not supported"` |
| Rune impl block | `rune_engine.rs:209-` |
| Free `col(name)` (kept unqualified) | `rune_engine.rs:384` |
| JOIN regression suite | `crates/toron/tests/join_kind_test.rs` (9 join kinds) |

## Phase 0 — Write the failing gates first

In `crates/toron/tests/qualified_resolution_test.rs`:

- 13 convert-and-pass gates (each scalar position + alias-column-list continuity + Rune surface —
  see ADR Behavior gates).
- 2 stays-rejected gates (`qualified_wildcard_stays_rejected`,
  `qualified_returning_stays_rejected`).

Each convert gate **must fail on `main`** with existing reject behavior (one of the four G1 scalar
reject strings, the visible-names qualifier gate, or the absent Rune/alias-column-list behavior). The
two stays-rejected gates **must pass on `main`** and remain green throughout.

## Phase 1 — `RelationScope` sidecar + scope propagation (D2)

In `binder.rs`, add `RelationScope` + `RelationScopeEntry` as binder-internal types (no
`ExecutionPlan` change). Propagation per plan node — locked per ADR table (Scan/Alias/Filter/Sort/
Take/Exchange/Project/Aggregate/Join). The `Alias` propagation **replaces** the visible qualifier
with the alias (D4 alias-hides-base-qualifier; pinned by G6). If `LogicalPlan::Alias { columns }`
has a column list, expose those renamed column names under the alias qualifier, not the input names
(pinned by G7).

## Phase 2 — Extract `resolve_qualified_column` (D8, G3)

Extract qualifier-matching core from `resolve_join_column` (`binder.rs:3130`) +
`resolve_join_column_across` (`:3281`) into:

```rust
fn resolve_qualified_column(
    column_ref: &ColumnRef,
    scope: &RelationScope,
) -> Result<BoundColumnRef, BindingError>;
```

An implementation-specific private helper error is also fine if it is converted into `BindingError`
at the callsite.

Refactor JOIN-key resolvers to delegate. **Behavior-preserving** — `join_kind_test.rs` must still
pass exactly after this phase (G4).

## Phase 3 — Convert the 5 scalar reject sites (D3)

Replace each scalar reject with `resolve_qualified_column`:

1. **`binder.rs:1249-1251` (ORDER BY)** — D3 constraint preserved: 121 does *not* widen ORDER BY
   beyond projected/source-preserved columns.
2. **`sql_planner.rs:4944` (GROUP BY key)** — accept two-part qualified column references for the
   currently-supported aggregate shapes; keep computed / multi-part group keys rejected.
3. **`sql_planner.rs:4952` (HAVING)** — qualifier-aware HAVING.
4. **`sql_planner.rs:5037` (aggregate projection)** — qualifier-aware aggregate arg.
5. **`sql_planner.rs:4135` (visible-names gate)** — relax `column.qualifier.is_some() ||` where the
   qualified column resolves to a projected/source-preserved direct ref. Keep
   `expr_contains_qualified_column` for qualified RETURNING (G2).

After Phase 3, the four G1-named scalar reject strings are absent from source and the visible-names
qualifier rejection shape is relaxed.

## Phase 4 — Named/actionable error surface (D7, G5)

Add or reuse binder error surfaces for:

- unknown relation / table qualifier with available qualifiers
- ambiguous relation / table qualifier with candidate count or names
- unknown column within a known qualifier with available column names

An internal `QualifierResolution` enum is acceptable, but not required. Reuse existing
`UnknownTableRef` / `AmbiguousColumnRef` where naturally fitting; add a named relation-qualified
unknown-column error if needed. G5 pins the actionable error surface, not an exact helper type.

## Phase 5 — Rune surface (D5, G8)

In `rune_engine.rs`:
- `pub fn alias(self, name: String) -> RuneDataFrame` on `impl RuneDataFrame` at `:209` — lowers
  to `LogicalPlan::Alias`.
- Free `pub fn qcol(rel: String, col: String) -> RuneExpr` parallel to existing free `col(name)`
  at `:384` — lowers to `Expr::qualified_column(TableRef::new(rel), col)`.
- Free `col(name)` kept unqualified.

## Phase 6 — Architecture guards (G1-G10)

In `crates/toron/tests/architecture/`:

- `adr121_scalar_qualifier_rejects_are_gone` (G1)
- `adr121_qualified_wildcard_and_returning_stay_out_of_scope` (G2)
- `adr121_qualifier_resolution_uses_single_resolver` (G3)
- `adr121_join_key_qualifier_resolution_preserved` (G4)
- `adr121_qualifier_resolution_errors_are_named` (G5)
- `adr121_alias_hides_base_qualifier` (G6)
- `adr121_alias_column_lists_feed_relation_scope_names` (G7)
- `adr121_rune_alias_qcol_surface_exposed` (G8)
- `adr121_column_ref_remains_the_only_qualified_column_ir` (G9)
- `adr121_no_executor_pbc_wendelon_change` (G10)

## Phase 7 — Verification

```sh
cargo fmt
cargo test -p toron qualified
cargo test -p toron --test join_kind_test
cargo test -p toron --test architecture_test adr121
cargo test -p toron
cargo test -p wendelon
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

All 15 behavior gates green (13 convert + 2 stays-rejected); `join_kind_test.rs` regression passes
exactly; the four G1-named scalar reject strings absent from source and the visible-names qualifier
rejection shape relaxed; the 4 G2-named wildcard/RETURNING reject strings remain in source verbatim;
single resolver (G3); JOIN-key delegation preserves semantics (G4); named/actionable error surface
(G5); alias shadowing explicit (G6); alias-column-list scope names preserved (G7); Rune surface
exposed (G8); IR unchanged (G9); no executor / PBC / Wendelon change (G10); 115-120 regressions
hold; full Toron + Wendelon suites + `--all-targets` green.
Migration: `plans/adr/0121_general_qualified_relation_scope_binding.md`.
