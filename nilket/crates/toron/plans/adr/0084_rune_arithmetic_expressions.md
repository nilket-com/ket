# ADR 084: Rune Arithmetic Expression Surface

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the merged council drafts. Gemini's iter-0 overflow-→-NULL slip is corrected
and pinned (overflow → `ExecutionError::Kernel`; integer divide-by-zero → NULL).
Minor fixture-granularity divergence resolved to the lean folded set (5 fixtures incl.
a targeted overflow fixture; mixed-promotion + nested arithmetic folded into the
projection gate).
**Category:** Toron frontend composition / Rune expression authoring over shared IR.
**Scope:** Toron-only; production change localized to `crates/toron/src/rune_engine.rs`
(4 `RuneExpr` methods + registration). Lowers to **existing** `Expr::Add/Sub/Mul/Div`
(ADR 024). No IR/binder/executor/SQL/PBC/dependency/`syn`/sandbox change.

## Summary

ADR 083 completed Rune predicates; ADR 084 adds `.add`/`.sub`/`.mul`/`.div` — thin
constructors over the existing ADR 024 arithmetic IR — so Rune can author computed
columns, arithmetic aggregate arguments, and arithmetic in predicates without
dropping to `ctx.sql`. This closes the basic numeric-expression gap in the current
Rune run (literals + predicates + arithmetic), while explicit casts, pattern/list
predicates, and conditionals remain separate candidates.

## The texture change (not another pushed cut)

`Expr::is_pbc_pushdown_eligible` returns **false** for arithmetic, and PBC rejects it
via the existing "arithmetic, Cast, and TryCast are not PBC-eligible" path. So unlike
079–083, 084's proof is **computed projection + residual filter execution**, not
fully-pushed: an arithmetic predicate binds `FilterDisposition::EvaluateInToron` with
no scan PBC. `FullyPushed` here would be a finding, not a success.

## Grounding ledger (verified)

- `Expr::{Add,Sub,Mul,Div}` exist; `bind_expr_into_program` routes all four through
  `bind_arithmetic` (`binder.rs:2899`), which injects widening casts and rejects
  non-numeric operands.
- `arithmetic_output_type` owns the lattice: `Int32+Int32→Int32`; integer width
  promotion through `Int64`; pure `Float32→Float32`; mixed int/float or `Float64`
  participation → `Float64`.
- The bound + unbound executor paths (`evaluate_unbound_arithmetic`, `exec.rs:4050`)
  already evaluate arithmetic.
- **ADR 024 semantics inherited (corrected):** checked integer overflow →
  `ExecutionError::Kernel { "arithmetic overflow: …" }` (**not** NULL); integer
  divide-by-zero → **NULL**; truncating integer division; float follows IEEE 754.
- **Aggregate-over-arithmetic is already a path** — SQL `SELECT SUM(amount + 1)` is
  tested (`group_by_test:305,317`), so Rune `sum(col("amount").add(lit(1)))` lowers to
  the same `Aggregate(Sum,[Add])` — no kernel change.
- `add/sub/mul/div` are not Rune keywords (083 grounding). Current `RuneExpr` surface:
  `named/gt/eq` + the 083 predicate methods; no arithmetic.

## Decisions (10, promotion-locked)

**D1 Four arithmetic methods** — `.add/.sub/.mul/.div(other)` → `Expr::Add/Sub/Mul/Div`.
**D2 Existing substrate only** — no new `Expr`/`ScalarValue`/bound-expr/executor
kernel/PBC opcode/SQL grammar.
**D3 ADR 024 semantics inherited** — overflow → `Kernel`; int div-by-zero → NULL;
truncating int division; `arithmetic_output_type` promotion; IEEE floats. No
Rune-local evaluation.
**D4 Computed columns via `.named`** — named only through `.named("alias")` inside
`.project_named(...)`; no alias state on `RuneExpr` (ADR 081 shape).
**D5 Arithmetic filters are residual** — bind `EvaluateInToron`, no scan PBC;
`FullyPushed` would be a finding.
**D6 Aggregate arguments may contain arithmetic** — e.g. `sum(col("amount").add(lit(1)))`
(existing computed-aggregate-argument path).
**D7 Reconcile the scope guard (discipline #4, again)** — move `add/sub/mul/div` to
**required**; keep `not/like/ilike/in_list/cast/try_cast` forbidden.
**D8 Cast/TryCast deferred** (ADR 085+) — basic arithmetic auto-promotes via the
existing lattice.
**D9 No broader grab bag** — no unary negation, modulo, decimal/date arithmetic,
string concat, pattern/list, sort/limit, joins, dot-field sugar.
**D10 Sandbox intact** — host methods construct IR only; no capability/shim/dependency.

## Behavior gate (hand-verified on the 082/083 `sales` fixture)

1. **Computed projection (gate)** — exercises all four methods, NULL propagation,
   truncating int division, Int64 arithmetic, and mixed `Int32+Float64→Float64`:
   ```rune
   ctx.table("sales").project_named([
       col("amount").add(lit(1)).named("amount_plus_one"),
       col("amount").div(lit(2)).named("amount_half"),
       col("price").mul(lit_f64(2.0)).named("price_x2"),
       col("id").sub(lit_i64(1)).named("zero_based_id"),
       col("amount").add(col("price")).named("amount_plus_price"),
   ])
   ```
   → `[11,21,NULL,31,NULL,8]` / `[5,10,NULL,15,NULL,3]` /
   `[21.0,40.5,NULL,61.5,11.0,14.5]` / `[0,1,2,3,4,5]` /
   `[20.5,40.25,NULL,60.75,NULL,14.25]`.
2. **Arithmetic predicate is residual** — `filter(col("amount").add(lit(1)).gt(lit(20)))`
   → ids **[2,4]**; assert `FilterDisposition::EvaluateInToron` + no scan PBC.
3. **Aggregate-argument composition** — `aggregate([], [sum(col("amount").add(lit(1)))])`
   → scalar **[Some(71)]**.
4. **Integer divide-by-zero → NULL** —
   `project_named([col("amount").div(col("amount").sub(col("amount"))).named("q")])`
   → `q` all NULL.
5. **Checked integer overflow → Kernel error** —
   `project_named([lit_i64(9223372036854775807).add(lit_i64(1)).named("overflow")])`
   → `ExecutionError::Kernel` containing `"arithmetic overflow"`. (Included to pin the
   corrected inheritance, given the iter-0 overflow-→-NULL slip.)

## Architecture Guards

- **Retarget** `adr082_typed_literal_cut_does_not_expand_operator_surface`: required =
  `{eq,gt,lt,lte,gte,ne,and,or,logical_not,is_null,is_not_null, add,sub,mul,div}`;
  forbidden = `{not, like,ilike,in_list,cast,try_cast}`.
- `adr084_rune_arithmetic_methods_lower_to_existing_expr_variants`.
- `adr084_rune_arithmetic_methods_are_registered` (4; no overload/shim/cast).
- `adr084_rune_arithmetic_reuses_adr024_substrate` (no binder/executor/PBC/normalizer
  markers in `rune_engine.rs`).
- `adr084_rune_arithmetic_remains_pbc_ineligible` (pins `is_pbc_pushdown_eligible`
  false + PBC rejection → explicit residual boundary).
- Carry 079–083 guards + `group_by_test` (ADR-024 net) + `rune_sandbox_test`.

## Out of Scope

`Cast`/`TryCast` (ADR 085+), `Like`/`InList`, unary negation, modulo, decimal/date/
time arithmetic, string concat, PBC arithmetic, dot-field sugar, joins, relation
aliases, sort/limit, SQL grammar, worker-side Rune.

## Council History

iter-0: all three chose Rune arithmetic; **Codex corrected Gemini's overflow-→-NULL
slip** (overflow is `Kernel`, div-by-zero is NULL); **Claude framed the residual/
computed texture change** + the discipline-#4 recurrence. iter-1: Claude + Gemini
duals; the only material difference was fixture breadth (Claude treated Rune overflow
as optional since SQL carries ADR 024; Gemini proposed a wider net). iter-2:
**resolved** — include a targeted Rune overflow fixture, fold mixed-promotion +
nested arithmetic into the projection gate. Genuine 3-of-3; fixtures hand-verified;
SUM-over-arithmetic SQL precedent confirmed.

## Promotion criteria

4 methods lower to existing `Expr`; all registered; arithmetic required by the
reconciled scope guard; `cast`/`try_cast`/`like`/`in_list` still forbidden; 5 gates
pass (incl. residual `EvaluateInToron` + overflow `Kernel`); no production change
outside `rune_engine.rs`; 079–083 + `group_by_test` + `rune_sandbox_test` + clippy +
full package green. Migration: `plans/impl/0084_rune_arithmetic_expressions.md`.
