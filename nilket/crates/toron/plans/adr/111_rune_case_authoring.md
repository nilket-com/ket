# ADR 111: Rune CASE Authoring

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — pure Rune authoring admission over
108's `Expr::Case`, **zero engine change**. **Toron-only, `rune_engine.rs`-localized.** **Predecessors:**
080/086/101 (Rune carrier + admission pattern), 108 (`Expr::Case` substrate), 109 (SQL conditional sugars),
110 (CASE logical-identity preservation). **Migration:** `plans/impl/111_rune_case_authoring.md`.

## Doctrine — admit authoring, don't re-own CASE policy

111 completes the conditionals arc across **both** frontends. SQL authors CASE directly (108) and via sugar
(109); CASE now preserves compatible logical identity (110). Rune should *construct the same node* and **not
grow** a local type checker, executor, PBC policy, or typed-NULL story. This is the 101 pattern (LIKE) over
`Expr::Case`: Rune builds existing IR, then `bind_case` / `evaluate_case` / PBC-decline / 110 identity do all
the substantive work. The arc landed in the right order — fix the shared binder (110) *before* adding the
surface — so Rune CASE binds over **already-correct** semantics.

## Scale boundary

**In:** one Rune host carrier (`RuneWhenThen`), two top-level constructors (`when`, `case`), behavior tests,
one Rune guard. Searched form only. **Out (deferred):** SQL grammar, IR/bound variants, binder/executor/PBC/
storage/Wendelon change, worker-side Rune, explicit `NULL`, missing-ELSE Rune CASE, simple-CASE shortcut,
`if_else`, fluent builder, `coalesce`/`ifnull`/`nvl`/`NVL2` in Rune, lazy CASE.

## Grounding (verified)

- `RuneExpr { expr: Expr }` is a thin host wrapper; Rune methods construct `Expr` and leave policy to binding.
  The structured-authoring precedent is a typed host carrier: `RuneNamedExpr` (`.named` → `project_named`),
  `RuneAggr` (`aggregate`). Free constructors register via `m.function([...], f)` (`col`/`lit`/`time_bucket`,
  `create_toron_module` `rune_engine.rs:602`); types via `m.ty::<T>()`.
- `RuneExpr::in_list` / `build_in_list` (`:550`/`:575`) is the `VmResult` precedent for a schema-free
  *structural* check (non-empty); `LIKE` is infallible because it has no precondition.
- 108 pins CASE substrate (`Expr::Case`, `bind_case`, `evaluate_case`, residual-only PBC decline "CASE is not
  PBC-eligible", eager branch eval, no `OP_CASE`); 110 pins CASE logical-identity preservation in `bind_case`
  — Rune-authored CASE inherits both.
- `case`/`when` are **not** reserved in Rune 0.14.1 (`select`/`match`/`if`/`else`/`not`/`in`/`as`/`is` are).

## Decisions (promotion-locked, 3-of-3)

**D1 — Searched CASE as top-level `case([...], else_expr)`** (not a receiver method — searched CASE has no
natural receiver; the roadmap shorthand "Rune `.case`" means Rune CASE authoring):
```rune
case([
    when(col("amount").gt(lit(0)), lit_str("positive")),
    when(col("amount").lt(lit(0)), lit_str("negative")),
], lit_str("zero"))
```

**D2 — `RuneWhenThen` carrier, not tuple vectors.** `RuneWhenThen { condition: Expr, result: Expr }` +
`when(condition: RuneExpr, result: RuneExpr) -> RuneWhenThen` — follows the local carrier pattern, gives the
guard a concrete host contract, avoids host-boundary tuple ambiguity.

**D3 — `case(branches: Vec<RuneWhenThen>, else_expr: RuneExpr) -> VmResult<RuneExpr>`** → `Expr::Case {
when_then, else_expr: Some(...) }`, **authored order preserved exactly** (no sort/normalize/desugar/optimize).

**D4 — Reject empty branches at the Rune surface** (`VmResult::panic("CASE requires at least one WHEN
branch")`) — the **only** Rune-side check, because it's schema-free and structural (the `in_list` precedent).

**D5 — Require `else_expr` in V1.** Missing-ELSE Rune CASE would expose the internal typed-NULL placeholder
before the explicit-NULL ADR; deferred. (SQL missing-ELSE unchanged.)

**D6 — No schema-dependent Rune checks.** Rune does not verify Boolean-ness or branch-type compatibility —
those need binding context and stay in `bind_case`.

**D7 — Inherit 108 CASE semantics.** Boolean-condition enforcement, NULL-condition no-match, first-true-wins,
branch type unification, all-naked numeric widening, residual-only PBC, eager branch evaluation.

**D8 — Inherit 110 nominal identity.** Uniform compatible identity carries through Rune-authored CASE; mixed
identities, identity + naked-non-literal, and identity-bearing physical widening reject via `bind_case`.

**D9 — No shortcuts this cut.** No simple-CASE, fluent builder, `if_else`, method alias, or nullable variant —
authoring conveniences, not required to admit the substrate.

**D10 — Retarget the Rune surface-name discipline.** Admit `case`/`when`; keep reserved/deferred names
(`select`/`match`/`if`/`else`/`as`/`is`/`in`/`not`/`safe_cast`) unadmitted.

## Behavior gates

- `rune_case_projection_executes` — multi-branch searched CASE projects expected rows (string + numeric).
- `rune_case_numeric_widening_stays_naked` — all-naked numeric branches widen and stay `NAKED`.
- `rune_case_empty_branches_reject` — `case([], else)` panics at Rune construction (D4).
- `rune_case_type_errors_inherited` — non-Boolean conditions / ununifiable branches reject in `bind_case`.
- `rune_case_filter_is_residual_and_keeps_pushable_conjunct` — a CASE filter is `EvaluateInToron`; a sibling
  pushable conjunct still reaches scan PBC.
- `rune_case_eager_eval_masked_error_fails` — a masked hard-erroring branch still fails (inherited 108).
- `rune_case_carries_logical_identity` — `as_logical_type::<CustomerId>()` branch + naked-literal else →
  `CustomerId` (110 inheritance — the arc payoff).
- `rune_case_mixed_identities_reject` / `rune_case_identity_widening_rejects` — inherited 110 (the latter with
  the explicit-cast diagnostic).

## Architecture guards

- Add `adr111_rune_case_lowers_to_existing_expr_case` (`tests/architecture/rune_frontend.rs`): pin
  `RuneWhenThen`, `when`, `case`, their `create_toron_module` registration, empty-branch `VmResult::panic`,
  `Expr::Case` + `else_expr: Some`, and the **absence** of local binder/executor/PBC policy markers
  (`BindingError`, `BoundExpr`, `ExecutionPlan`, `PushdownDeclineReason`, `OP_CASE`, and the PBC decline
  diagnostic) in `rune_engine.rs`.
- Retarget the Rune surface-name discipline (admit `case`/`when`; reserved/deferred names stay out).
- Keep 108/110 CASE-policy guards green — 111 reuses them, doesn't restate CASE policy in the Rune frontend.

## Council history

3-of-3 lead on Rune CASE (all iter-0). Iter-1 had **one API fork**: Gemini proposed optional-else, an
`if_else` shortcut, and a Rune-side Boolean check; Claude + Codex held required-else, no shortcuts, and
binder-owned schema checks (the "admit authoring, don't re-own policy" doctrine). **Gemini iter-2 converged**
to that narrowed shape. Claude also conceded its iter-0 *infallible* lean to `VmResult` (`.case` has a
non-empty precondition, like `in_list`, unlike infallible `LIKE`). Genuine 3-of-3 on the carrier +
top-level-constructor form.

## What to do next

General explicit-`NULL` / typed-null authoring (unblocks missing-ELSE Rune CASE + `NVL2`); the `if_else` /
simple-CASE Rune shortcuts; aggregate-output identity (the other 110 nominal-flow continuation); Rune
sort/limit and joins remain carried.

## Promotion criteria

Rune authors construct searched CASE via `case([when(...), ...], else_expr)`; it lowers to existing
`Expr::Case`; empty branches reject at the Rune surface; all schema-dependent + execution behavior is
inherited from 108/110 (type unify, 110 identity, residual PBC with pushable siblings still pushing, eager
eval); the `adr111_*` guard pins the desugar-only nature; Rune surface-name guards reconciled; **no**
SQL/IR/binder/executor/PBC/storage/Wendelon change; targeted Rune + architecture tests + the Toron suite
green. The conditionals arc is complete across both frontends. Migration:
`plans/impl/111_rune_case_authoring.md`.
