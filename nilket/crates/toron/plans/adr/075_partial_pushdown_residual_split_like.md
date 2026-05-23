# ADR 075: Partial-Pushdown Residual Split + Literal `LIKE`/`ILIKE`

**Status:** Promoted (plan). Council converged on the ADR topic: residual
splitting unlocks non-PBC predicates, and literal `LIKE` / `ILIKE` executes via
Arrow kernels already present through the existing `arrow` dependency. The
promoted design follows the narrower Codex/Claude consensus on the remaining
specifics: one IR variant, no `ESCAPE`, compile-attempt classification, and
SELECT / UPDATE / DELETE scan-predicate coverage. See Council History.
**Seed:** ADR 074 added `IN`/`BETWEEN` but deferred `LIKE` because the binder's
PBC policy is all-or-nothing.
**Category:** Toron compute-ring SQL predicate surface + PBC attachment policy.
**No Wendelon / SQL-wire / worker-wire / storage / PBC-opcode / dependency
change.**

## The shape

The load-bearing feature is **top-level-`AND` residual splitting** for PBC
pushdown; literal `LIKE`/`ILIKE` is the first new surface that depends on it.
Today `WHERE id=7 AND name LIKE 'A%'` either pushes the whole predicate or
evaluates it all in Toron, so a non-PBC `LIKE` makes `id=7` lose its point-read.
075 splits the conjunction: push the PBC-eligible conjuncts, residual the rest.

## Decisions (15, Promotion-Locked)

### Literal `LIKE` / `ILIKE`

**D1 Accepted surface:** `<utf8-expr> [NOT] LIKE/ILIKE '<literal-pattern>'`.
**Reject** `LIKE ANY`, non-literal/dynamic patterns, **`ESCAPE`**, `SIMILAR TO`,
`RLIKE`/`REGEXP`, PG operator spellings (`~~`/`~~*`/`!~~`/`!~~*`), `CASE`, and the
074-carried surfaces.

**D2 Literal-pattern bound.** The pattern lowers from a single-quoted string
literal, stored as a `String` in IR (not a runtime expression): static pattern
work, bounded planner accounting. A SQL `NULL` pattern is outside the surface.
Because `ESCAPE` is rejected, literal `%`/`_` aren't expressible in 075 (explicit;
a future ADR can widen).

**D3 One logical IR variant.** `Expr::Like { expr: Box<Expr>, pattern: String,
negated: bool, case_insensitive: bool }`. **No separate `Expr::ILike`**: `ILIKE`
sets `case_insensitive`. No sqlparser AST escapes `sql_planner.rs`.

**D4 Bound IR.** `BoundExprNode::Like { expr, pattern, negated, case_insensitive }`;
binder requires `Utf8` input, returns `Boolean`; type mismatch fails at bind with
bounded diagnostics.

**D5 Execution via Arrow kernels: no new dependency.** Use
`arrow::compute::kernels::comparison::{like, ilike, nlike, nilike}`. The existing
`arrow` umbrella **already** brings in `arrow-string` and `regex` (verified
`cargo tree -p toron -i regex`: via arrow-csv + arrow-string); `exec.rs`
already uses the same `arrow::compute::kernels::*` import style. **No custom
matcher, no direct `arrow-string`/`regex` dep.** Arrow's deterministic SQL-LIKE semantics
(`%` multi-char, `_` single-char, null input propagates to null); `ILIKE` is
Arrow's case-insensitive behavior. Toron adds **no locale/collation/Unicode-
normalization contract**.

**D6 `LIKE` is residual-only.** `is_pbc_pushdown_eligible(Like)=false`; `pbc.rs`
declines `BoundExprNode::Like` (a guard asserts **no `LIKE` PBC opcode**). `NOT
LIKE`/`NOT ILIKE` via the negation flag (Arrow `nlike`/`nilike`).

### Partial-pushdown residual split (the structural core)

**D7 Split only top-level `AND`.** Do **not** distribute through `OR`, split under
`NOT`, or rewrite boolean algebra (the SQL three-valued-logic correctness guard).
A top-level conjunct that is itself an `OR`/non-eligible expr is residual as a
whole. `a>10 AND (x LIKE 'A%' OR r='w')` => push `a>10`, residual the `OR`.

**D8 Classify by *attempting* `compile_pbc` per bound conjunct**: not by
`is_pbc_pushdown_eligible` alone. The compile attempt preserves the existing
type/column/opcode/bytecode-size/stack-depth checks. Combine successes into one
pushed program (re-compiled once before attaching to the scan), failures into one
residual predicate.

**D9 `FilterDisposition` stays binary, retargeted.** `FullyPushed` = no Toron
residual (filter skipped); `EvaluateInToron` = residual work remains. After 075,
`EvaluateInToron` **no longer implies "no PBC below"**: a partial push is
scan-with-PBC + residual `Filter { EvaluateInToron }`. No new serialized variant.

**D10 Scan profile from the *pushed* predicate.** Compute `QueryProfileHint` from
the pushed conjuncts, preserving the point-read for `id=7 AND name LIKE 'A%'`
(residual `LIKE` filters the point-read result in Toron).

**D11 Projection pruning.** Columns referenced only by pushed PBC predicates need
not be projected back to Toron; PBC bytecode stays interpreted by storage against
the physical schema (as today).

**D12 DML handling: apply the split to all scan-predicate sites.** SELECT/CTAS/
INSERT-SELECT read plans, **UPDATE source scans**, and **DELETE candidate scans**.
For DELETE, `ExecutionPlan::Delete.pbc_program` carries the pushed candidate
predicate and `.predicate` carries only the residual (both already `Option`,
matching the existing delete plan shape); candidate-plan derivation wraps the scan
in a residual `Filter` when `predicate: Some(_)` even with `pbc_program` present.

**D13 The split benefits *existing* predicates (the immediate win).** Not
LIKE-specific: `is_pbc_pushdown_eligible` is **false** for `Add/Sub/Mul/Div`,
`Cast/TryCast`, and `TimeBucket`, so `WHERE x+1>5 AND y>10` - which loses all
pushdown today - now pushes `y>10`. Behavior-additive: same results, strictly more
storage-side filtering.

**D14 Conservative fallback.** If any helper can't prove the split is
semantics-preserving (clone/recomposition failure, final PBC compile failure,
unsupported shape), keep the original all-residual behavior.

**D15 Toron-local boundary.** Planner/binder/exec/pbc-compiler/tests only; no
Wendelon protocol/opcode, storage, telemetry, audit, wire, or dependency change.

## Architecture Guards (16 + meta)

`adr075_like_is_residual_only_no_wendelon_opcode`,
`adr075_like_pattern_surface_is_literal_only`,
`adr075_like_rejects_escape_any_dynamic_and_pg_spellings`,
`adr075_like_executes_via_arrow_comparison_kernels_no_new_dependency`,
`adr075_like_is_one_variant_with_case_insensitive_flag_not_separate_ilike`,
`adr075_ilike_uses_arrow_semantics_without_toron_collation`,
`adr075_partial_pushdown_splits_only_top_level_and`,
`adr075_partial_pushdown_does_not_distribute_or_or_not`,
`adr075_pushdown_classification_uses_compile_pbc_attempts`,
`adr075_final_pbc_compile_failure_falls_back_to_all_residual`,
`adr075_filter_disposition_semantics_are_retargeted`,
`adr075_pushed_predicate_drives_point_read_profile`,
`adr075_projection_pruning_does_not_require_pushed_only_columns`,
`adr075_delete_and_update_scan_predicates_apply_residual_split`,
`adr075_existing_toron_only_conjuncts_no_longer_block_pbc_siblings`,
`adr075_no_wendelon_wire_storage_audit_telemetry_or_dependency_change`.
**Meta:** `adr075_sqlparser_ast_remains_quarantined_to_sql_planner`,
`adr075_new_surface_is_brand_clean_and_has_no_v1_suffix`,
`adr075_prior_adr074_in_between_and_pbc_guards_continue_to_pass`.

## Integration Fixtures (17)

`where_like_filters_rows`; `where_not_like_filters_rows`;
`where_ilike_matches_arrow_reference_behavior`; `where_like_null_input_drops_rows`;
`like_rejects_dynamic_pattern_escape_any_and_null_pattern`;
`like_rejects_non_utf8_input`; `like_alone_remains_residual_no_pbc`;
`mixed_like_and_numeric_pushes_numeric_conjunct_and_filters_residual`;
`mixed_arithmetic_residual_pushes_comparison_sibling` (D13);
`mixed_cast_residual_pushes_pk_equality`;
`mixed_ilike_and_pk_equality_uses_point_read_profile`;
`partial_pushdown_does_not_split_or`; `partial_pushdown_does_not_split_under_not`;
`final_pbc_compile_failure_falls_back_to_all_residual`;
`projection_pruning_preserves_pbc_only_column`;
`delete_mixed_pbc_and_like_filters_candidates_residually`;
`update_mixed_pbc_and_like_uses_pushed_source_scan_then_residual`. ADR 074
`IN`/`BETWEEN` + WHERE/PBC fixtures stay green.

## Out of Scope / Carried

`ESCAPE`, dynamic `LIKE` patterns, `LIKE ANY`; `SIMILAR TO`/`RLIKE`/`REGEXP`
(needs `arrow_string::regexp` + a CPU-cost contract); `OR`/`NOT` predicate
distribution; `CASE`; subquery/`UNNEST`/row-value `IN`; `ALTER`/composite-PK;
a Wendelon `LIKE`/regex PBC opcode (separate cross-ring pushdown ADR).

## Council History

iter-0: Claude proposed the split alone (`LIKE` deferred to 076); Codex and
Gemini bundled the split with literal `LIKE` / `ILIKE`. iter-1: the council
converged on the bundle and narrowed the main fork to `LIKE` execution. iter-2/3:
the Arrow-kernel path became the promoted execution strategy after verifying that
`arrow-string` and `regex` are already in Toron's dependency tree through
`arrow`. The final design also folds in the DELETE / UPDATE scan-predicate
catch, the existing-arithmetic benefit, single `Expr::Like` representation,
no-`ESCAPE` surface, and compile-attempt pushdown classification. Gemini's final
draft kept broader alternatives (`Expr::ILike`, `ESCAPE`, and eligibility-based
classification); the promoted plan records those as rejected for ADR 075.

## Promotion criteria met

D-set 15; 16 + meta guards; 17 fixtures; behavior-additive (results unchanged,
more pushdown, including for existing arithmetic/cast predicates); Arrow-kernel
`LIKE` with **no new dependency**; residual-only, no PBC opcode; Toron-local. The
074 `IN`/`BETWEEN` + PBC fixtures stay green. Migration:
`plans/impl/075_partial_pushdown_residual_split_like.md`.

## Conclusion

075 = the **partial-pushdown residual split** (push PBC-eligible top-level-`AND`
conjuncts via `compile_pbc`-attempt classification, residual the rest across
SELECT/UPDATE/DELETE scan predicates; `FilterDisposition` retargeted, conservative
fallback) + **literal `LIKE`/`ILIKE`** (one `Expr::Like` variant, Arrow
`comparison` kernels, residual-only, no new dependency, no PBC opcode).
Behavior-additive and Toron-bounded; it also recovers pushdown for existing
arithmetic/cast predicates.
