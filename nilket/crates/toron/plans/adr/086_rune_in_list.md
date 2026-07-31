# ADR 086: Rune InList Membership Surface

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-2, Claude iter-2, Gemini iter-2): topic,
surface, native negation, in-Rune frontend-bound validation, and the split SQL-parity
guard all agreed; no divergence outstanding.
**Category:** Toron frontend composition / Rune expression authoring over shared IR.
**Scope:** Toron-only; production change localized to `crates/toron/src/rune_engine.rs`
(2 `VmResult`-returning `RuneExpr` methods + a shared helper + a length constant +
registration).
Lowers to **existing** `Expr::InList` (ADR 074). No IR/binder/executor/PBC-opcode/
SQL-grammar/dependency/`syn`/sandbox change.

## Summary

`id IN (1,2,3)` / `NOT IN (...)` from Rune via `.in_list`/`.not_in_list`, lowering to
existing `Expr::InList`. **InList pushes** (`OP_IN_LIST`; negated → `OP_IN_LIST`+`OP_NOT`)
— this cut **restores the FullyPushed proof texture** after 084/085's residual/computed
cuts. Computed (PBC-ineligible) targets or members fall back to residual.

## The frontend-bound parity refinement

The 32-item `IN`-list bound + empty reject live in `SqlPlanner::lower_in_list`
(`sql_planner.rs:38, 3159–3170`) — the **planner**, which Rune bypasses; the binder
rejects empty but does **not** enforce the size bound. So `.in_list`/`.not_in_list`
return `VmResult` and **re-validate in `rune_engine.rs`** (non-empty +
`<= RUNE_MAX_IN_LIST_ITEMS (32)`, SQL-parity messages), with a dedicated guard
asserting `RUNE_MAX_IN_LIST_ITEMS == MAX_IN_LIST_ITEMS` (no drift). Preserves an
existing frontend contract across the new entry point; not engine scope creep.

## Grounding ledger (verified)

- `Expr::InList { expr, list, negated }` (ADR 074); `is_pbc_pushdown_eligible` admits
  it when target + all members eligible; binder `bind_in_list` (`:669`); executor
  `evaluate_in_list`; PBC `OP_IN_LIST` (negated → `+OP_NOT`).
- SQL `IN`/`NOT IN` push + execute (predicate_widening_test, ADR 074).
- 32-bound is **planner-only** (`sql_planner.rs:38`) → Rune re-validates.
- `in_list`/`not_in_list` keyword-free; NULL target under `NOT IN` → not selected (3VL,
  verified row sets).

## Decisions (10, promotion-locked)

**D1 `.in_list(Vec<RuneExpr>) -> VmResult<RuneExpr>`** → `InList { expr: self, list,
negated: false }`.
**D2 `.not_in_list(Vec<RuneExpr>) -> VmResult<RuneExpr>`** → `… negated: true` (native
flag, pushes; **not** `.in_list(...).logical_not()`).
**D3 In-Rune validation** — `const RUNE_MAX_IN_LIST_ITEMS: usize = 32`; empty →
`VmResult::panic("IN list must not be empty")`; `>32` →
`VmResult::panic(format!("IN list exceeds {RUNE_MAX_IN_LIST_ITEMS} items"))`.
**D4 SQL-bound parity by guard, not coupling** — duplicate the constant locally
(`MAX_IN_LIST_ITEMS` is private to `sql_planner.rs`); a guard reads both files and
asserts equality.
**D5 Members are `Vec<RuneExpr>`** (literals/columns/computed) — the existing
array-to-`Vec` surface (`project`/`project_named`/`aggregate`).
**D6 Inherit binder coercion + type-mismatch + 3VL** — literal coercion; mismatch →
existing `BindingError` ("IN operands"); NULL 3VL inherited. No Rune-local type logic.
**D7 Pushes when eligible / residual when not** — `FullyPushed` + `OP_IN_LIST` when
target + members PBC-eligible; computed target or members → `EvaluateInToron`, no scan
PBC.
**D8 Reconcile the scope guard (discipline #4)** — `in_list`/`not_in_list` → required;
`like`/`ilike`/exact bare `not`/`safe_cast` forbidden.
**D9 Keep Like separate** (ADR 087) — residual, pattern/case/negation policy.
**D10 Sandbox intact.**

## Behavior gate (8; ids hand-verified on the 082–085 `sales` fixture)

1. **`in_list` pushes (gate)** — `filter(col("id").in_list([lit_i64(1), lit_i64(2)]))`
   → ids **[1,2]**; `FullyPushed` + scan PBC + `OP_IN_LIST`.
2. **`not_in_list` pushes** — `filter(col("status").not_in_list([lit_str("open")]))`
   → ids **[3]**; `FullyPushed` + `OP_IN_LIST`+`OP_NOT`.
3. **NOT IN + NULL (3VL)** — `filter(col("region").not_in_list([lit_str("east")]))`
   → ids **[2,5]** (NULL `region` not selected).
4. **Literal coercion inherited** — `col("id").in_list([lit(1), lit_i64(2)])` →
   ids **[1,2]**.
5. **Computed member → residual (type-correct)** —
   `col("id").in_list([col("amount").sub(lit_i64(9)), col("amount").sub(lit_i64(18))])`
   → ids **[1,2]**; `EvaluateInToron`, no scan PBC. (`lit_i64` forces Int64 members
   matching the Int64 `id` target; the arithmetic is the PBC-ineligible part.)
6. **Empty list rejected** — `.in_list([])` → VM panic "IN list must not be empty".
7. **Over-32 rejected** — 33 members → VM panic "IN list exceeds 32 items".
8. **Type mismatch → BindingError** — `col("status").in_list([lit(1)])` → existing
   `BindingError` containing "IN operands".

## Architecture Guards

- **Retarget** `adr082_typed_literal_cut_does_not_expand_operator_surface`:
  `in_list`/`not_in_list` required; `like`/`ilike`/exact bare `not`/`safe_cast`
  forbidden.
- `adr086_rune_in_list_methods_lower_to_existing_expr_variant` (negated false/true via
  a shared `build_in_list`).
- `adr086_rune_in_list_methods_validate_frontend_bounds` (`RUNE_MAX_IN_LIST_ITEMS=32`;
  empty + over-limit `VmResult::panic`).
- `adr086_rune_in_list_authoring_bounds_match_sql_planner` — reads both
  `rune_engine.rs` and `sql_planner.rs`; `RUNE_MAX_IN_LIST_ITEMS == MAX_IN_LIST_ITEMS`
  (no drift).
- `adr086_rune_in_list_methods_are_registered` (2; no exact exported `in`/`not` names).
- `adr086_rune_in_list_pushes_when_pbc_eligible` (`FullyPushed` + `OP_IN_LIST`).
- `adr086_rune_in_list_reuses_existing_substrate` (no new IR/binder/executor/PBC/SQL).
- Carry 079–085 guards + `predicate_widening_test` + `rune_sandbox_test`.

## Out of Scope

`Like`/`ILike` (087), row-value/subquery `IN`, `UNNEST`, literal NULL IN-list members
or a Rune null-literal surface, changing the 32-item bound, new PBC opcode, dot-field
sugar, joins, sort/limit, SQL grammar, worker-side Rune.

## Council History

iter-0: all three → Rune InList (the pushed cut); **Codex caught the frontend-bound
parity gap** (Rune bypasses `lower_in_list`'s 32-bound; the binder doesn't enforce it).
iter-1: **Claude conceded** the iter-0 "inherit ADR 074" framing → in-Rune validation;
all three on the surface. iter-2: genuine 3-of-3 — **Claude adopted Codex's
type-correct computed-member fixture** (Int64 target + `lit_i64` members) and
**Gemini's split parity guard**. 3VL / computed-member / planner-only-bound facts
verified.

## Promotion criteria

`.in_list`/`.not_in_list` (`VmResult`) → existing `Expr::InList`; in-Rune non-empty +
`<= 32` validation with SQL-parity messages + the `== MAX_IN_LIST_ITEMS` guard; 8 gates
pass (pushed `[1,2]`/`[3]`, NULL-3VL `[2,5]`, coercion, computed residual,
empty/over-32/type-mismatch rejects); scope guard reconciled; no production change
outside `rune_engine.rs`; 079–085 + `predicate_widening_test` + `rune_sandbox_test` +
clippy + full package green. Migration: `plans/impl/086_rune_in_list.md`.
