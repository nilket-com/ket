# ADR 109: SQL conditional-sugar family (`COALESCE` / `NULLIF` / `IF` / `IIF` / `IFNULL` / `NVL`)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the family + the one binder hook.
**Toron-only — pure SQL-frontend desugar over ADR 108's `Expr::Case`**, plus a single narrow `bind_case`
hook for internal typed-NULL result branches; **no new `Expr`/`BoundExprNode` variant, executor kernel, PBC
opcode, storage/Wendelon surface, or statement language.** **Predecessor:** 108 (the `CASE` substrate);
reuses the `typed_update_null_for_target` `Utf8(None)` placeholder convention. **Migration:**
`plans/impl/0109_conditional_sugar_family.md`.

## Doctrine — spend the 108 substrate, don't build another

108 created the conditional-expression substrate; 109 cashes it out. All six function spellings parse as
ordinary `SqlExpr::Function` calls (sqlparser 0.62 — there are no `SqlExpr::Coalesce`/`NullIf` variants), so
they are name-matched in `lower_expr` beside `TIME_BUCKET` and lowered to `Expr::Case`. The only thing that
isn't planner-only is `NULLIF`'s internal `THEN NULL`, handled by one small reuse of Toron's existing
untyped-NULL placeholder convention.

## Scale boundary

`COALESCE` / `NULLIF` / `IF` / `IIF` / `IFNULL` / `NVL`, case-insensitive one-part names, anywhere `lower_expr`
runs (projection, `WHERE`, `ORDER BY`, aggregate args, `HAVING`, derived tables, DML scalar exprs). **Out of
scope (each on its own merits):** `NVL2(a,b,c)` (3-arg Oracle form, *not* first-non-null sugar); general
explicit `NULL` literals outside conditional-sugar value/result positions (bare `SELECT NULL` stays rejected);
procedural `IF … THEN … END IF` (`Statement::If`, already rejected); new IR/bound/executor/PBC/Wendelon.

## Grounding (verified)

- **Entry point:** scalar functions are name-matched in the `SqlExpr::Function` arm of `lower_expr`
  (`sql_planner.rs:3780`, TIME_BUCKET precedent), args from `FunctionArguments::List`. No existing guard pins
  these — they currently fall through to unsupported-function; only a new 109 guard is added.
- **The NULLIF wrinkle + its solution:** `NULLIF(a,b)` ≡ `CASE WHEN a=b THEN NULL ELSE a`, needing a typed
  `THEN NULL` the planner can't type (no catalog, no typeless `ScalarValue::Null` — every null is typed).
  **`Expr::Literal(ScalarValue::Utf8(None))` is already Toron's untyped-NULL placeholder** —
  `typed_update_null_for_target` (`binder.rs:1074`) detects it and re-types it for UPDATE NULL staging. 109
  reuses that sentinel + a `bind_case` re-typing hook (the analog of 108's `typed_null_for_case`). **Zero new
  substrate.**
- **Safety invariant:** a *literal* `Utf8(None)` in a CASE **result** branch is unambiguously a planner-emitted
  sentinel: normal `lower_expr` still rejects explicit `NULL` (`Value::Null => Err`), and only the
  conditional-sugar result/value lowering path may create this placeholder. The UPDATE-assignment path and the
  CASE-result path are disjoint; the placeholder is re-typed inside `bind_case` before the bound program
  propagates. Revisit if general explicit-NULL ever lands.
- **The family reuses 108 end-to-end:** bind (`bind_case` + `case_common_type`), execute (eager-vectorized
  `zip`), residual (PBC declines), naked result.

## Decisions (promotion-locked, 3-of-3)

**D1 — One conditional-function classifier.** Case-insensitive, one-part names: `COALESCE`, `NULLIF`, `IF`,
`IIF`, `IFNULL`, `NVL`. Positional plain args only — reject qualified names, named args, wildcards,
`DISTINCT`, `FILTER`, `OVER`, `WITHIN GROUP`, argument clauses, ODBC syntax, subquery args.

**D2 — `IF` / `IIF`** → `CASE WHEN c THEN t ELSE e END`; arity exactly 3; `c` Boolean via 108's
`require_boolean`.

**D3 — `COALESCE(a1..aN)` (N ≥ 1)** → `CASE WHEN a1 IS NOT NULL THEN a1 … ELSE aN END`; `N = 1` → `a1`.

**D4 — `NULLIF(a,b)`** → `CASE WHEN a = b THEN <Utf8(None) placeholder> ELSE a END`; arity exactly 2; result
type inferred from `a` (the non-placeholder branch); NAKED identity.

**D5 — `IFNULL(a,b)` / `NVL(a,b)`** → strict 2-arg `COALESCE` (`CASE WHEN a IS NOT NULL THEN a ELSE b END`);
arity **exactly 2** (so `IFNULL(a)` / `IFNULL(a,b,c)` reject, unlike variadic `COALESCE`). `NVL2(a,b,c)` is
**not** matched (different shape).

**D6 — Explicit `NULL` value/result arguments are accepted only inside conditional-sugar lowering** (Codex's
enhancement, adopted under the operator's "small + fits → include" principle). A literal `NULL` in a position
that can become a CASE result maps to the `Utf8(None)` placeholder, so `COALESCE(NULL, x)` /
`IF(c, NULL, x)` work — while bare `SELECT NULL`, `IF(NULL, t, e)`, and `NULLIF(NULL, x)` stay rejected.
All-placeholder results reject (no inferable type).

**D7 — One shared `bind_case` hook (the only engine change).** A CASE result binding to literal `Utf8(None)`
is the placeholder: excluded from `case_common_type`, rejected if *every* result is a placeholder, replaced
with `typed_null_for_case(output_type)` before `append_case` (108's missing-ELSE path unchanged). Carries the
safety-invariant doc-comment. Lives in shared `bind_case` (SQL `CASE` + sugar); normal `lower_expr` cannot
produce the sentinel.

**D8 — Eager CASE semantics inherited + pinned.** Post-desugar these are 108 CASE: branches evaluate eagerly,
so a masked hard-erroring branch (`IF(true, 1, overflow())`) still fails the query. Documented + a regression.

**D9 — No new substrate.** No `Expr::Coalesce`/`NullIf`, `BoundExprNode::*`, executor function, PBC opcode,
storage/Wendelon/coordinator change, or procedural statement support.

## Behavior gates

`coalesce_returns_first_non_null`, `coalesce_single_arg_is_identity`,
`coalesce_explicit_null_argument_uses_typed_fallback`, `coalesce_numeric_branches_widen`,
`coalesce_all_null_arguments_reject`, `nullif_returns_null_when_equal`, `nullif_returns_first_arg_when_unequal`,
`nullif_result_type_is_first_argument_type`, `if_function_executes`, `iif_is_if_alias`,
`if_null_result_branch_uses_typed_case_null`, `if_condition_must_be_boolean`,
`if_explicit_null_condition_remains_unsupported`, `if_arity_not_three_rejects`,
`nullif_explicit_null_operand_remains_unsupported`, `ifnull_and_nvl_match_two_arg_coalesce`,
`ifnull_nvl_arity_not_two_rejects`, `nvl2_remains_unsupported`,
`conditional_sugar_in_where_order_by_having_and_aggregate_arg`, `conditional_sugar_is_residual`,
`conditional_sugar_inherits_case_eager_hard_error`, `procedural_if_statement_remains_unsupported`,
`bare_select_null_remains_unsupported`.

## Architecture guards

- `adr109_conditional_sugar_desugars_to_case` — the six-name classifier + `SqlExpr::Function` dispatch build
  `Expr::Case` (+`Expr::IsNotNull`/`Expr::Eq`); `IFNULL`/`NVL` are strict-2 aliases; `NVL2` and `Statement::If`
  are **not** matched; **no** `Expr::Coalesce`/`NullIf`, `BoundExprNode` variant, executor kernel, or PBC opcode.
- `adr109_case_binder_types_untyped_null_sentinel` — `bind_case` detects literal `Utf8(None)` result branches,
  excludes them from `case_common_type`, rejects all-placeholder, and replaces via `typed_null_for_case`
  (reusing the `typed_update_null_for_target` `Utf8(None)` convention).
- ADR 108 guards retained (109 rides `Expr::Case`; residual/PBC-decline stay owned there).

## Council history

iter-0: all three landed CASE-sugar; **Claude grounded that `NULLIF` isn't planner-only** (no typeless NULL)
and deferred it; **Codex grounded the small `bind_case` placeholder hook** (reusing the `Utf8(None)`
convention) to keep `NULLIF` in-family; Gemini aligned. iter-1: **Claude conceded the deferral** and verified
the sentinel is the *existing* `typed_update_null_for_target` convention (pinning the no-collision +
explicit-NULL-revisit invariants); **operator steered `IFNULL`/`NVL` in** (D5) — "108 named only four" was
not a real reason; they're trivial 2-arg `COALESCE` aliases. Final: adopted Codex's explicit-NULL
value/result sugar enhancement (D6) under the same inclusive principle. Genuine 3-of-3 on the family + hook;
`NVL2` is the explicit boundary, deferred on its own merits.

## What to do next

General explicit-`NULL` literal support (lifts `Value::Null` rejection; can retire/formalize the
placeholder convention); `NVL2(a,b,c)`; Rune `.case` authoring; uniform-branch logical-identity propagation
(108 D7).

## Promotion criteria

All six conditionals bind + execute through `Expr::Case` everywhere scalar exprs run; arity/shape rejects are
explicit; `NULLIF`, `COALESCE(NULL,x)`, `IFNULL(NULL,x)`, `NVL(NULL,x)` get typed NULL branches via the shared
`bind_case` hook; all-placeholder rejects; bare `SELECT NULL` and `NVL2` and procedural `IF` stay rejected;
eager-error inherited + pinned; **no new IR/bound/executor/PBC/Wendelon surface**; the two 109 guards added,
108 guards retained; Toron suite green. Migration: `plans/impl/0109_conditional_sugar_family.md`.
