# Impl Plan 075: Partial-Pushdown Residual Split + Literal `LIKE`/`ILIKE`

**Status:** Promoted, paired with
`plans/adr/0075_partial_pushdown_residual_split_like.md`.
**Posture:** Binder-local **partial-pushdown residual split** (push PBC-eligible
top-level-`AND` conjuncts, residual the rest, across SELECT/UPDATE/DELETE scan
predicates) + **literal `LIKE`/`ILIKE`** (one `Expr::Like` variant, Arrow
`comparison` kernels, residual-only). **Toron-bounded**, behavior-additive, **no
new dependency**. Six phases.

## Verified anchors

- **All-or-nothing binder (to split):** existing `LogicalPlan::Filter` binding
  sites compile a whole predicate with `compile_pbc`, and `attach_pbc_program`
  attaches the pushed program to the scan.
- **Pushdown eligibility:** `is_pbc_pushdown_eligible` is false for
  `Add/Sub/Mul/Div`, `Cast/TryCast`, and `TimeBucket` (D13 beneficiaries).
  Classify by `compile_pbc` *attempt* (D8), not this helper alone.
- **Exec disposition consumer:** `FullyPushed` returns scan batches directly;
  `EvaluateInToron` applies the residual `Filter`.
- **DELETE shape:** `ExecutionPlan::Delete { predicate: Option<BoundExprProgram>,
  pbc_program: Option<PbcProgram> }` already has the push/residual shape.
  **UPDATE** uses a source scan that takes the same split.
- **Arrow LIKE kernels (no new dep, verified):** `cargo tree -p toron -i regex`
  shows `regex` already via arrow-csv + arrow-string; `arrow::compute::kernels::
  comparison` re-exports `arrow_string::like::*`; `exec.rs` already uses Arrow
  compute-kernel imports. **Planner precedent:** `lower_expr` already has
  literal-NULL rejection and `split_top_level_and` exists on the planner side.

## Invariants carried in

- Split **top-level `AND` only** (no `OR`/`NOT`); classify by `compile_pbc`
  attempt; **fallback to full-residual** if combined-pushed declines.
  `FilterDisposition` stays binary (retarget `EvaluateInToron`). Scan profile from
  the **pushed** predicate. **One `Expr::Like` variant** (ILIKE = flag), Arrow
  `comparison` kernels, **no new dependency**, residual-only (no PBC opcode),
  literal-pattern only (reject ESCAPE/dynamic/ANY/SIMILAR/RLIKE/PG/CASE), UTF-8
  input. **Results unchanged** (behavior-additive). sqlparser quarantined; no
  Wendelon/wire/storage/audit change.

## Phase 1 - Logical IR (`ir.rs`)

- `Expr::Like { expr: Box<Expr>, pattern: String, negated: bool, case_insensitive:
  bool }` (no separate `ILike`). `is_pbc_pushdown_eligible(Like)=false`. Update
  exhaustive `Expr` visitors.

## Phase 2 - Planner (`sql_planner.rs` `lower_expr`)

- `SqlExpr::Like`/`SqlExpr::ILike` -> `Expr::Like` (`case_insensitive` for ILIKE);
  require a **string-literal** pattern; reject `escape_char.is_some()`, `any`,
  dynamic/non-literal patterns. **Reject** `SqlExpr::{SimilarTo, RLike, Case}` +
  PG operator spellings (`PG{,I}LikeMatch`, `PGNot{,I}LikeMatch`).

## Phase 3 - Binder: the residual split (core; `binder.rs`)

- Bound-IR top-level-`And` splitter. At **both** Filter sites:
  split -> for each conjunct `compile_pbc` against the bound input -> combine
  successes into one pushed program (re-compile once; `attach_pbc_program`) +
  failures into one residual `BoundExprProgram`.
  - all-pushed => `FullyPushed`; pushed+residual => scan-with-PBC + `Filter
    { predicate: residual, EvaluateInToron }`; none-pushed => current behavior;
    **combined-pushed decline => fall back** to all-residual (D14).
- `scan_profile_hint`/`apply_scan_profile_hint` from the **pushed** predicate.
  `bind_like`: require `Utf8` input (bounded error otherwise).

## Phase 4 - DML scan predicates (`binder.rs`, DELETE/UPDATE derivation)

- Apply the same split to **UPDATE source scans** and **DELETE candidate scans**.
  DELETE: pushed -> `Delete.pbc_program`, residual -> `Delete.predicate`; wrap the
  candidate scan in a residual filter when `predicate: Some(_)` even with
  `pbc_program` present.

## Phase 5 - Exec + PBC (`ir/execution.rs`, `exec.rs`, `pbc.rs`)

- `BoundExprNode::Like { expr, pattern, negated, case_insensitive }` + builder,
  validation, `pretty_node`, child-walk, nullable propagation, offset/remap.
- `exec.rs`: evaluate `Like` via `arrow::compute::kernels::comparison::{like,
  ilike, nlike, nilike}` (negated -> the `n*` variant). NULL input propagates to
  NULL; WHERE filtering drops NULL through existing boolean filter semantics.
- `pbc.rs`: `compile_pbc` **declines** `BoundExprNode::Like` (existing
  `ExpressionShapeUnsupported`); guard asserts no `LIKE` opcode.

## Phase 6 - Guards, fixtures, evidence

- 16 + meta guards (incl. **top-level-AND-only**, **classify-by-compile_pbc**,
  **fallback**, **FilterDisposition-retarget**, **scan-profile-from-pushed**,
  **projection-pruning**, **DELETE/UPDATE split**, **existing-arithmetic-partial-
  push** (D13), one-variant-not-separate-ILike, **like-via-arrow-no-new-dep**,
  like-literal-only/reject-escape, like-residual-no-opcode, results-unchanged,
  sqlparser-quarantined, no-Wendelon/dep change, prior-074-pass).
- 17 fixtures (ADR list), anchored by `mixed_arithmetic_residual_pushes_
  comparison_sibling` (D13), `mixed_ilike_and_pk_equality_uses_point_read_profile`,
  `final_pbc_compile_failure_falls_back_to_all_residual`, `projection_pruning_
  preserves_pbc_only_column`, `delete_mixed_pbc_and_like_filters_candidates_
  residually`, `update_mixed_pbc_and_like_uses_pushed_source_scan_then_residual`.
- `plans/impl/0075_partial_pushdown_residual_split_like_evidence.md`:
  `architecture_test adr075` + full (incl. prior 074/PBC guards green); `cargo test
  -p toron` (predicate + pbc-pushdown + planner/binder/exec + delete/update suites);
  existing WHERE/filter corpus green; `cargo check -p toron` feature matrix;
  `clippy -D warnings`; `git diff --check`; a **bytecode-shape snapshot** (`amount>10
  AND status LIKE 'A%'` -> scan PBC for `amount>10`, residual `Like`, no LIKE opcode)
  + a **before/after pushdown** note (arithmetic conjunct now partial-pushes).

## Watch-items

1. **Split soundness** - top-level `AND` only; never distribute `OR`/`NOT`.
2. **Classify by `compile_pbc` attempt + fallback** - not `is_pbc_pushdown_eligible`
   alone (catches per-conjunct column/type/bytecode/stack failures); combined-pushed
   decline => all-residual.
3. **`FilterDisposition` retarget** - `EvaluateInToron` may have PBC below now;
   update the guard interpretation + any assertion that assumed otherwise.
4. **All scan-predicate sites** - SELECT + UPDATE source + DELETE candidate.
5. **`LIKE` no new dep, residual-only** - Arrow `comparison` kernels; no PBC opcode;
   one `Expr::Like` variant; reject ESCAPE/dynamic/ANY/SIMILAR/RLIKE/PG/CASE.
6. **Behavior-additive** - existing results unchanged; only pushdown shifts; the
   074/PBC corpus is the parity net.

## Acceptance checklist

- [x] Phase 1: `Expr::Like` (one variant); `is_pbc_pushdown_eligible(Like)=false`.
- [x] Phase 2: lower Like/ILike (literal pattern; reject escape/dynamic/any/PG/
      SimilarTo/RLike/Case).
- [x] Phase 3: binder top-level-AND split + per-conjunct `compile_pbc` + fallback;
      scan-profile-from-pushed; both Filter sites; `bind_like` UTF-8.
- [x] Phase 4: UPDATE source + DELETE candidate scans apply the split.
- [x] Phase 5: `BoundExprNode::Like` + Arrow `comparison` exec; `pbc.rs` declines Like.
- [x] Phase 6: 16 guards + 17 fixtures (incl. arithmetic-partial-push + DELETE/UPDATE
      + bytecode shape) + evidence; prior 074/PBC corpus green; no Wendelon/dep change.
