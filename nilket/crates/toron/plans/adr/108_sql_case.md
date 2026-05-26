# ADR 108: SQL `CASE` (value conditionals)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** on the substrate contract. **Toron-only
substrate leap** — one logical `Expr`, one bound arena node, a residual executor kernel, and complete
**two-sided** expression-walker coverage; the first conditional-expression substrate (and first IR+executor
leap since the 087 nominal spine). **No** new `LogicalPlan`/`ExecutionPlan` variant, Wendelon/storage/worker
surface, or PBC opcode. **Predecessors:** 031 (bound-program arena), 074/075 (residual expressions), 084
(arithmetic totality — div0→NULL), 087 (nominal identity / `node_logical_types`). **IF/COALESCE/NULLIF
resolved to the ADR 109 sugar family** (operator steer). **Migration:** `plans/impl/108_sql_case.md`.

## Doctrine — residual conditional expressions, one searched-form node

The smallest *correct* CASE ADR is **not** "accept `SqlExpr::Case`" — it is "add residual conditional
expressions with searched-CASE IR, simple-CASE desugar, Boolean conditions, a narrow branch-coercion lattice,
explicit nullability, naked logical identity, eager vectorized execution, and **complete two-sided
expression-walker coverage**." `CASE` then works everywhere `lower_expr` already runs — projection, `WHERE`,
`ORDER BY`, aggregate arguments, `HAVING`.

## Scale boundary

Searched + simple `CASE` as a residual scalar expression. **Out of scope:** PBC pushdown (residual-only),
logical-type propagation through branches (naked result), the conditional-sugar family
(`COALESCE`/`NULLIF`/`IF`/`IIF` → ADR 109), a Rune `.case` surface (follow-on), `SIMILAR TO`/`RLIKE` (separate
substrate), and per-row lazy/short-circuit branch evaluation (see §Evaluation semantics).

## Grounding (verified)

- `sql_planner.rs:3619` rejects `SqlExpr::Case` (beside SIMILAR TO / RLIKE / InSubquery / InUnnest);
  `adr074_similar_rlike_case_subquery_in_remain_unsupported` (`predicate_pushdown.rs:499`) and
  `unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported` pin that reject marker.
- sqlparser 0.62 parses searched + simple CASE as `SqlExpr::Case { operand, conditions: Vec<CaseWhen{condition,
  result}>, else_result }` — simple desugars by `operand = condition`.
- `Expr` (`ir.rs:496`) has no conditional variant; `is_pbc_pushdown_eligible` (`:552`) already groups residual
  computed variants (`Cast`/`Add`/`Like`/`TimeBucket`) returning `false` — `Case` joins them.
- `BoundExprNode` (`ir/execution.rs`) is a `BoundExprId` arena with **9 variants** (ADR 031); `Case` is the
  10th. It is matched in **4 files** — `ir/execution.rs` (validation/`node_nullable`/node children/display),
  `binder.rs` (binding, clone/remap, structural equality, offsetting, logical-cast collection), `pbc.rs`
  (decline), `exec.rs` (evaluate). `grep BoundExprNode::Like` anchors the bound-side sweep; `grep Expr::Like`
  the logical-side visitor sweep.
- `arithmetic_output_type` (`ir/execution.rs`) computes the numeric common type — the right tool for branch
  unification (not `is_widening_cast`, which only checks direction).
- Arrow `arrow::compute::kernels::zip::zip(mask, truthy, falsy)` selects per the WHEN mask, taking **falsy on
  false *or NULL*** — matching SQL `WHEN NULL` as no-match.

## Decisions (promotion-locked, 3-of-3)

**D1 — `Expr::Case { when_then: Vec<(Box<Expr>, Box<Expr>)>, else_expr: Option<Box<Expr>> }`** (searched form,
only shape; deterministic; `is_pbc_pushdown_eligible` → `false`).

**D2 — Simple CASE desugars in the planner** (`CASE x WHEN v` → `WHEN x = v`, operand cloned per branch);
reject empty WHEN set. One binder/executor/PBC path.

**D3 — Logical-side visitors recurse into CASE.** The planner's `SqlExpr`/`Expr` visitors are updated
intentionally: nested-aggregate rejection, aggregate/HAVING helpers, subquery/window detection,
hidden-sort-key validation, qualified-column checks, non-aggregate column-ref detection, and **logical
structural hash/equality** (a new stable tag; hash/compare ordered `(cond,result)` pairs + else).

**D4 — Conditions bind Boolean; NULL → no-match.** Each WHEN binds `DataType::Boolean` (nullable allowed);
NULL mask values select the fallback.

**D5 — Narrow branch lattice via `arithmetic_output_type`.** Result branches unify by identical types (incl.
`Boolean`/`Utf8`) or numeric combinations `arithmetic_output_type` accepts; else bind-time mismatch. The
binder inserts widening `Cast` nodes after the common type is selected.

**D6 — Bound CASE always has an else.** `BoundExprNode::Case { when_then: Vec<(BoundExprId, BoundExprId)>,
else_input: BoundExprId }`; a missing SQL `ELSE` becomes a typed-NULL literal of the common type.

**D7 — Precise nullability; naked identity.** `node_nullable(Case)` = any result branch (incl. `else_input`)
nullable; conditions don't make the result nullable. First-WHEN-true wins. Result `LogicalTypeId::NAKED`
(uniform-branch identity propagation deferred).

**D8 — Eager vectorized execution.** See §Evaluation semantics. PBC declines; the executor folds branches in
reverse with `zip`; both `evaluate_bound_program_without_storage` and the unbound `evaluate_expr` covered.

**D9 — PBC decline.** No opcode; `pbc.rs` rejects `BoundExprNode::Case` in both `compile_node` and `emit_node`.

**D10 — Bound-side walker coverage.** A `Case` arm in every `BoundExprNode` match across the 4 files (mirror
`Like`); children are valid `BoundExprId`s (arena arity invariant).

**D11 — `IF`/`IIF`/`COALESCE`/`NULLIF` are the ADR 109 sugar family.** Cheap after CASE, but they touch the
`SqlExpr::Function` scalar-dispatch surface (a different parser arm) and are *standard* (`COALESCE`/`NULLIF`)
vs *dialect* (`IF`/`IIF`) — so they ship together as one pure-desugar ADR over `Expr::Case`, not piecemeal in
108. (Procedural `IF … THEN … END IF` is out — Toron has no PL surface.)

## Evaluation semantics — eager branches (the lazy/eager tradeoff, **documented**)

A deliberate, documented semantic gap — it must read clearly here, as a doc-comment on the executor
`evaluate_case`, and be pinned by tests.

- **SQL CASE reads like a lazy conditional:** row-by-row, an unmatched branch is expected not to matter —
  `CASE WHEN x <> 0 THEN 100/x ELSE 0 END` should not expose the guarded division when `x = 0`.
- **Toron is vectorized, so CASE is eager:** we compute the *whole* `THEN`/`ELSE` arrays for the batch, then
  `zip` by the WHEN mask. A branch value is computed even for rows it won't be selected for — and if that
  computation **hard-errors, the whole query fails**, though SQL would have masked that row out. The
  "trip-over" trap.
- **Mental model — branchless evaluation at array granularity.** `zip` is a vectorized `cmov`/blend (compute
  both sides, select by mask). The eager-error exposure is the *standard branchless constraint*: only
  **total** (non-faulting) operations are safe to speculate. Per-branch masked execution is the engine analog
  of a compiler falling back to a real branch when a speculated side can fault.
- **Precise exposure (narrower than the general worry, and reassuring):** Toron's kernels already make the
  *common* guard safe — ADR 084 gives integer **div-by-zero → NULL** (float → IEEE inf/NaN), so
  `CASE WHEN x<>0 THEN 100/x ELSE 0` is **correct** (x=0 rows produce NULL in the THEN array, then zip to
  ELSE). The residual exposure is **hard-erroring** kernels: integer **overflow** (ADR 084 → `Kernel` error)
  and **strict `CAST`** failure inside a masked branch (`TRY_CAST` → NULL is the escape).
- **Accepted tradeoff for the first cut** — eager is Toron's vectorized starting point; per-branch masked
  (lazy) execution is a named deferred follow-on, taken only if the eager hard-error exposure proves to matter.

## Behavior gates

`case_searched_projection_executes`, `case_simple_desugars_and_executes`, `case_first_true_wins`,
`case_no_match_uses_else`, `case_missing_else_returns_null`, `case_nullable_condition_is_no_match`,
`case_numeric_branches_widen`, `case_identical_text_branches_execute`, `case_type_mismatch_rejects_at_bind`,
`case_in_where_executes_as_residual`, `case_in_order_by_executes`, `case_in_aggregate_argument_executes`,
`case_result_is_naked`, plus the eager-eval pins: **`case_divide_guard_is_safe_via_null`** (the common guard
works — div0→NULL) and **`case_branches_eval_eagerly_masked_hard_error_fails`** (a masked overflow / strict
`CAST` branch fails the query — pins the documented eager contract so a future lazy change is conscious).

## Architecture guards

- Retarget `adr074_similar_rlike_case_subquery_in_remain_unsupported` (drop `SqlExpr::Case`; keep SIMILAR TO /
  RLIKE / InSubquery / InUnnest / PG*Match).
- Retarget `unsupported_pattern_predicates_and_case_subquery_in_remain_unsupported` (drop only the CASE
  fixture).
- Retarget `adr031_bound_expr_node_has_no_boxes` (9 → 10; assert `Case`; keep no-boxed-exec-expr).
- Add `adr108_case_is_residual_bound_expression` — pins `Expr::Case`, `BoundExprNode::Case`,
  `else_input: BoundExprId`, Boolean-condition validation, `arithmetic_output_type` unification, typed-NULL
  synthesis, `node_nullable`/child/walker arms, `zip::zip` in exec, `pbc.rs` decline + no opcode, no
  Wendelon/storage surface.

## Council history

iter-0: Gemini + Claude led CASE as the capability swing (the introspection small-cuts exhausted at 107);
Codex grounded that it's an expression-substrate ADR, not a parser unlock — 3-of-3. iter-1/2: Claude added
the **eager-vectorized-eval semantic** (and the branchless framing the operator found clarifying); Codex
grounded the **two-sided walker surface**, `arithmetic_output_type` unification, always-present `else_input`,
and precise nullability. One scope fork — Gemini pulled `IF` into 108 — **resolved by the operator to the ADR
109 sugar family** (108 stays the clean substrate; 109 ships the whole family at once). Genuine 3-of-3 on the
substrate.

## What to do next — the conditionals arc (108 is the substrate; the rest ride it)

108 builds the conditional node once; a family then rides it at near-zero substrate cost:

- **ADR 109 — Conditional-sugar family** (the obvious next, operator-blessed): pure `SqlExpr::Function`-arm
  desugars onto `Expr::Case`, zero new substrate — `COALESCE(a,b,…)` → nested `IS NOT NULL` CASE,
  `NULLIF(a,b)` → `CASE WHEN a=b THEN NULL ELSE a`, `IF`/`IIF(c,t,e)` → 2-branch CASE. Standard first, dialect
  alongside.
- **Rune `.case(...)`** (the 101 admission pattern over the new node); **uniform-branch logical-identity
  propagation** (lifts D7 NAKED into the 087–098 system + elision); **per-row masked/lazy eval** (only if D8
  hard-error exposure bites); **`OP_CASE` pushdown** (far-horizon PBC).

## Promotion criteria

Searched + simple CASE bind and execute everywhere `lower_expr` runs; branches unify via
`arithmetic_output_type` (mismatch rejects at bind); missing ELSE → typed NULL; NULL condition → no-match;
first-true-wins; residual (no PBC), naked result; **both** the logical-side `SqlExpr`/`Expr` visitor ripple
and the bound-side `BoundExprNode` walker ripple complete; eager-eval documented (ADR + executor doc-comment)
and pinned (the two gates); 074 / pattern-predicates / 031 guards reconciled + `adr108_case_is_residual_bound_expression`
added; no plan/exec-variant/Wendelon/storage/PBC-opcode surface; Toron suite green. Migration:
`plans/impl/108_sql_case.md`.
