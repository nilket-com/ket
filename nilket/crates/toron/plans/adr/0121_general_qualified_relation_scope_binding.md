# ADR 121: General Qualified Relation-Scope Binding

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — Codex grounded iter-0
(joins/aliases/`ColumnRef.qualifier` already landed); Codex grounded iter-1 (wildcards need
planner↔binder restructuring); final promotion keeps the named/actionable error surface without
requiring a specific helper enum shape. **Category:** Toron interior — binder + sql_planner +
rune_engine. **Predecessors:** 079 (Rune relation handles), pre-existing join/alias/
`ColumnRef.qualifier` infrastructure.
**Migration:** `plans/impl/0121_general_qualified_relation_scope_binding.md`.

## Scale boundary

ADR 121 makes qualified relation-scope binding **general for scalar `Expr::Column` positions** using
the existing `ColumnRef { qualifier: Option<TableRef> }` IR (no new variants). The cut converts **5 of
9** reject sites; **4 stay rejected** as the deliberately-named follow-on (qualified wildcards `t.*`,
mixed wildcard, qualified RETURNING DML columns + wildcard). Wildcards need projection-expansion
machinery the SQL planner doesn't have at the expansion site (the planner has only
`AggregateOutputCatalog` for scope-naming; the schema catalog lives in the binder); they're a distinct
cut with its own scope considerations. Non-scope: wildcards under any qualifier; DML target aliases;
qualified DML assignment targets; GROUP BY over joins; joins-around-derived; nested derived / CTEs /
set ops / windows / lateral; new join kinds / strategies; Rune `.join(...)`.

## Doctrine — completion of scalar qualifier resolution; wildcard expansion is a separate cut

> The qualifier infrastructure is in the IR; the binder has catalog access and resolves qualifiers
> for JOIN keys. ADR 121 extends that resolver across all scalar expression positions where
> qualifiers are *parsed and rejected*. Positions that need *schema-aware expansion* — wildcards and
> DML RETURNING — require restructuring the planner↔binder boundary and are a deliberately-named
> follow-on, not 121.

The 9-site reject surface partitions:

| Position | Site | Class |
|---|---|---|
| ORDER BY | `binder.rs:1249-1251` | scalar — **convert** |
| GROUP BY key | `sql_planner.rs:4944` | scalar — **convert** |
| HAVING | `sql_planner.rs:4952` | scalar — **convert** |
| Aggregate projection | `sql_planner.rs:5037` | scalar — **convert** |
| Visible-names gate | `sql_planner.rs:4135` | scalar — **convert** |
| SELECT-list qualified wildcard | `sql_planner.rs:5113` | wildcard — **stays rejected, pinned** |
| Mixed wildcard with qualifier | `sql_planner.rs:2797` | wildcard — **stays rejected, pinned** |
| RETURNING qualified column | `sql_planner.rs:1796` | DML returning — **stays rejected, pinned** |
| RETURNING qualified wildcard | `sql_planner.rs:1777` | wildcard + DML — **stays rejected, pinned** |

G1 asserts the four converted reject strings are gone and the visible-names qualifier gate is
relaxed; G2 asserts the 4 deferred rejects remain verbatim. Both halves of the cut are pinned in
source.

## Decisions (promotion-locked)

**D1 — Reuse existing IR; no new variants.** `Expr::Column(ColumnRef { qualifier, name })` already
carries qualifier. ADR 121 changes **binding**, not expression shape. No `QualifiedColumn` variant;
no `Wildcard` variant.

**D2 — Binder carries a `RelationScope` sidecar.** Internal to the binder, mapping `(qualifier,
visible_column_name) → (output_ordinal, physical_type, nullability, logical_identity)`. The public
`ExecutionPlan` is unchanged; the executor sees only `BoundExprNode::Column(ResolvedColumn)`.

Scope propagation per plan node:

| Plan node | RelationScope behavior |
|---|---|
| `ScanNamed` | exposes table name as qualifier; one entry per column |
| `Alias` | exposes only the alias qualifier; **base table qualifier hidden** (D4); alias column lists rename the qualified column names positionally |
| `Filter` / `Sort` / `Take` / `Exchange` | preserve input scope (schema unchanged) |
| `Project` | preserves entries only for direct source-column projections; computed expressions drop their relation-qualified provenance |
| `Aggregate` | preserves scope only for group keys that are direct source columns |
| `Join` | combines visible-side scopes, offsetting right ordinals; semi/anti expose only the preserved side |

**D3 — Scalar qualified-column resolution across the existing `Expr::Column` positions.** Expression
binding resolves `Expr::Column`:
- Qualified: resolve `(qualifier, name)` against `RelationScope`.
- Unqualified: keep existing schema-name resolution and ambiguity behavior.

Positions in scope: SELECT projection scalar refs, WHERE, JOIN ON + join exchange keys (preserved),
GROUP BY keys for currently-supported aggregate shapes, aggregate function arguments, HAVING (subject
to existing "no SELECT alias in HAVING" doctrine), ORDER BY *only when the qualified reference names
a projected/source-preserved column in the sort input* — ADR 121 does **not** widen ORDER BY beyond
the existing projection model.

**D4 — Alias shadowing is explicit and load-bearing.** `FROM people AS p` exposes `p.id`; the base
qualifier `people.id` is **rejected** with an unknown-relation error that lists available qualifiers
such as `p`. If an alias column list exists, `FROM people AS p(pid, label)` exposes `p.pid` /
`p.label`, not `p.id` / `p.name`. Duplicate visible qualifiers reject as an ambiguous-relation
error. Pinned by G6/G7.

**D5 — Rune surface: `alias` + free `qcol`.**
- `pub fn alias(self, name: String) -> RuneDataFrame` on `impl RuneDataFrame` — lowers to
  `LogicalPlan::Alias`.
- Free `pub fn qcol(rel: String, col: String) -> RuneExpr` parallel to existing free `col(name)` —
  lowers to `Expr::qualified_column(TableRef::new(rel), col)`.

No Rune `.join(...)` in 121 (named follow-on). No "alias.col" string parsing (explicit method is
honest; string parsing bleeds scope).

**D6 — Explicit non-scope; pinned by guard.** The 4 wildcard/RETURNING reject strings remain in
source verbatim. Named follow-ons: qualified wildcards (planner↔binder restructuring or `Wildcard`
IR); qualified DML RETURNING (same machinery + DML target alias semantics); DML target aliases;
GROUP BY over joins; joins-around-derived; CTEs / set ops / windows / lateral; new join kinds /
strategies; Rune `.join(...)`.

**D7 — Named, actionable error surface; helper enum optional.** Canonical surface for the 5
converted reject sites:
- unknown relation / table qualifier with available qualifiers
- ambiguous relation / table qualifier with candidate count or names
- unknown column within a known qualifier with available column names

Existing `UnknownTableRef` / `AmbiguousColumnRef` may be reused where they fit naturally; add a named
relation-qualified unknown-column error if needed. An internal `QualifierResolution` enum is fine,
but G5 pins the error surface, not the exact helper type.

**D8 — JOIN-key qualifier resolution preserved bit-for-bit.** `resolve_join_column` /
`resolve_join_column_across` delegate to the new shared core `resolve_qualified_column`. JOIN-ON
predicate semantics unchanged. `join_kind_test.rs` regression suite (9 kinds) is the pin (G4).

**D9 — Scope boundary.** No new IR variants (D1). No `ExecutionPlan` change. No PBC change. No
Wendelon change. No SQL grammar change. No new JOIN semantics. The cut is entirely in `binder.rs` +
`sql_planner.rs` + `rune_engine.rs`. Pinned by G9/G10.

## Behavior gates

- `qualified_alias_projection_disambiguates_join_outputs`
- `qualified_where_resolves_right_join_input`
- `qualified_order_by_resolves_projected_source_column` (D3 constraint)
- `qualified_group_by_single_source_resolves`
- `qualified_having_group_key_resolves`
- `qualified_aggregate_argument_resolves`
- `alias_hides_base_table_qualifier` (D4)
- `alias_column_list_renames_qualified_column_scope` (D4 / ADR 103-105 continuity)
- `duplicate_relation_alias_rejects_as_ambiguous`
- `unknown_qualified_column_reports_relation_context`
- `rune_alias_and_qcol_filter_execute`
- `rune_qcol_project_named_over_joined_sql_executes`
- `rune_qcol_constructs_qualified_column_ref`
- `qualified_wildcard_stays_rejected` (G2 pin)
- `qualified_returning_stays_rejected` (G2 pin)

Regressions: `join_kind_test.rs` (9 join kinds) passes exactly; ADR 103/104/105 alias-column-list
tests pass; existing HAVING alias rejection unchanged; 115/116/117/118/119/120 suites green.

## Architecture guards

- **`adr121_scalar_qualifier_rejects_are_gone`** (G1) — `binder.rs` + `sql_planner.rs` contain none
  of the four converted scalar reject strings (`ORDER BY` unqualified-only, `GROUP BY` simple
  unqualified-only, qualified HAVING, qualified aggregate projection); the hidden-sort
  `column.qualifier.is_some() || !visible_names.contains(...)` rejection shape is relaxed where it
  blocks in-scope scalar direct refs. This guard does **not** require
  `expr_contains_qualified_column` to disappear; qualified RETURNING still uses that helper.
- **`adr121_qualified_wildcard_and_returning_stay_out_of_scope`** (G2) — `sql_planner.rs` contains
  all 4 wildcard/RETURNING reject strings verbatim, including the qualified-RETURNING predicate
  backed by `expr_contains_qualified_column`.
- **`adr121_qualifier_resolution_uses_single_resolver`** (G3) — `resolve_qualified_column` exists;
  JOIN-key resolvers delegate to it.
- **`adr121_join_key_qualifier_resolution_preserved`** (G4) — JOIN-key resolvers exist;
  `join_kind_test.rs` is the pin.
- **`adr121_qualifier_resolution_errors_are_named`** (G5) — named/actionable unknown-relation,
  ambiguous-relation, and unknown-column-in-relation error surfaces exist.
- **`adr121_alias_hides_base_qualifier`** (G6) — `LogicalPlan::Alias` scope-propagation replaces
  visible qualifiers with the alias.
- **`adr121_alias_column_lists_feed_relation_scope_names`** (G7) — `LogicalPlan::Alias { columns }`
  renamed column names are the names exposed under the alias qualifier.
- **`adr121_rune_alias_qcol_surface_exposed`** (G8) — `RuneDataFrame::alias` + free `qcol` exist.
- **`adr121_column_ref_remains_the_only_qualified_column_ir`** (G9) — `ir.rs` `ColumnRef` struct
  unchanged; no `QualifiedColumn` variant.
- **`adr121_no_executor_pbc_wendelon_change`** (G10) — `exec.rs` + `pbc.rs` + Wendelon source files
  contain no ADR 121 markers.
- **Recipe:** `cargo test -p toron` + `cargo test -p wendelon` + `cargo test -p toron --test
  architecture_test adr121` + `cargo check -p toron --all-targets`.

## Council history

iter-0: Claude framed "joins-from-scratch prereq"; Codex grounded full join/alias/`ColumnRef`
infrastructure already landed (`ir.rs:474/603/647`; `sql_planner.rs:2645/2849`;
`exec.rs:790/2037/2155-2235`; `distributed/fragment.rs:357`); Claude conceded
([[feedback_ground_the_premise_before_sketching_scope]], third grounding-miss after 119
inline-serial / 120 worker-hop). iter-1: Claude folded wildcard + RETURNING; Codex grounded wildcard
expansion lives in `sql_planner.rs` with no catalog access at the expansion site (planner has only
`AggregateOutputCatalog`; binder has the schema catalog at `:62/:70/:87/...`). Claude conceded — the
same lesson applied *recursively at sub-features* within a cut. iter-2 adopted Codex's Narrow scope
+ G1/G2 pin pair (G1 removes converted scalar rejects, G2 preserves deferred wildcard/RETURNING
rejects) + D2 RelationScope sidecar + D4 alias-hides-base-qualifier + D3 ORDER BY constraint; folded
in Claude's 9-site enumeration. final review replaced the stale predicate-gate conversion with the
real scalar `GROUP BY` reject, kept `expr_contains_qualified_column` as the RETURNING pin, required
alias-column-list names to feed relation scope, and made D7 a named/actionable error-surface
contract instead of an exact helper-enum contract.

## What to do next

- **Qualified wildcards (`t.*` + mixed)** — planner↔binder restructuring or `Wildcard` IR.
- **Qualified DML RETURNING** — same machinery + DML target alias semantics.
- **Rune `.join(...)`** — direct authoring over existing `LogicalPlan::Join`.
- **Multi-upstream live pipelining** (119's named follow-on) — customer-ready once Rune `.join(...)`
  lands.
- **GROUP BY over joins; joins-around-derived; CTEs / set ops / windows / lateral.**
- **Streamable `Sort` / `Aggregate` coverage** (120's V2).
- **Logical-type lifecycle** (`DROP`/`ALTER LOGICAL TYPE`) — tier pivot.

## Promotion criteria

All 15 behavior gates green; `join_kind_test.rs` (9 join kinds) regression passes exactly; the four
G1-named scalar reject strings absent from source and the visible-names qualifier rejection shape
relaxed; the 4 G2-named wildcard/RETURNING reject strings remain in source verbatim; single resolver
(G3); JOIN-key delegation preserves semantics (G4); named/actionable error surface (G5); alias
shadowing explicit (G6); alias-column-list scope names preserved (G7); Rune surface exposed (G8); IR
unchanged (G9); no executor / PBC / Wendelon change (G10); 115-120 regressions hold; full
`cargo test -p toron` + `cargo test -p wendelon` + `cargo test -p toron --test architecture_test
adr121` + `cargo check -p toron --all-targets` green.
Migration: `plans/impl/0121_general_qualified_relation_scope_binding.md`.
