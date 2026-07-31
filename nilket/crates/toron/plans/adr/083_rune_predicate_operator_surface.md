# ADR 083: Rune Predicate Operator Surface

**Status:** Promoted (plan). **Genuine 3-of-3 (Codex, Claude, Gemini)** — verified
against the latest peer drafts (Codex iter-1, Claude iter-1, Gemini iter-1): all
three lock the predicate-only cut, `.logical_not`, and the ADR 082 guard
reconciliation; Gemini conceded its iter-0 full-scalar-completion bundle. No
divergence outstanding.
**Category:** Toron frontend composition / Rune expression authoring over shared IR.
**Scope:** Toron-only; production change localized to `crates/toron/src/rune_engine.rs`
(9 `RuneExpr` methods + registration). Lowers to **existing** `Expr` variants. No
IR/binder/executor/SQL/PBC-opcode/dependency/`syn`/sandbox change; purely additive.

## Summary

ADR 082 made typed literals authorable from Rune, but expressions still had only
`.eq`/`.gt` — leaving `<`, `<=`, `>=`, `!=`, `OR`, `NOT`, and `IS NULL`/`IS NOT NULL`
unexpressible. ADR 083 completes the **filter** authoring surface with comparison
parity, boolean combinators, and null predicates as thin `RuneExpr` methods, each
lowering to an existing `Expr` variant and flowing through the normal binder,
pushdown compiler, placement, and executor. Arithmetic is deferred to ADR 084.

## Doctrine

**Rune authors intent; the IR carries meaning.** The authoring surface should
express standard relational logic, with method names chosen to avoid Rune keyword
collisions — no source rewriting (the ADR 081 doctrine).

## Grounding ledger (verified)

- **Keyword resolution** (Rune 0.14 keyword set): `lt/lte/gte/ne/and/or` and
  `is_null/is_not_null` are **free**; **`not` is RESERVED** → negation is
  `.logical_not()`. `.and`/`.or` go in directly; no `.not` method, no shim.
- **All lower to existing IR** — `Expr::{Lt,Lte,Gte,Neq,And,Or,Not,IsNull,IsNotNull}`
  exist; `bind_expr_into_program` already handles them (comparison/boolean/unary-Not/
  null-predicate arms); the executor evaluates them in bound + unbound paths.
- **The pushed path is preserved** — `Expr::is_pbc_pushdown_eligible` (`ir.rs`)
  returns **true** recursively for `Eq/Neq/Lt/Lte/Gt/Gte/And/Or` and `Not/IsNull/
  IsNotNull` over `Column`/`Literal`, and the PBC compiler + Wendelon PBC executor
  already have the matching opcodes. So Rune-authored predicate trees keep entering
  the normal fully-pushed scan path, not Rune-local logic.
- **Arithmetic is a different story (the 084 split)** — `is_pbc_pushdown_eligible`
  returns **false** for `Add/Sub/Mul/Div` (and `Cast`/`Like`/`TimeBucket`), so an
  arithmetic cut shifts the proof to residual/computed-projection execution with its
  own edge cases (overflow, int-div-by-zero, promotion). Keep it separate.
- **Discipline #4 — the 082 guard must invert.**
  `adr082_typed_literal_cut_does_not_expand_operator_surface` currently forbids this
  whole operator set; ADR 083 retargets it (below). `not` stays forbidden forever.
- Current surface: `RuneExpr` has only `named`/`gt`/`eq` (`rune_engine.rs:355-368`,
  registered `:434-436`).

## Decisions (10, promotion-locked)

**D1 Comparison parity** — `.lt`/`.lte`/`.gte`/`.ne` → `Expr::Lt/Lte/Gte/Neq`;
`.gt`/`.eq` unchanged.
**D2 Boolean combinators** — `.and(other)`/`.or(other)` → `Expr::And/Or`.
**D3 Keyword-safe negation** — `.logical_not()` → `Expr::Not`; **`not` is a Rune
keyword**, so `.not` is forbidden (no source shim, per ADR 081).
**D4 Null predicates** — `.is_null()`/`.is_not_null()` → `Expr::IsNull/IsNotNull`.
**D5 Existing IR only** — no new `Expr`/`ScalarValue`/`BoundExprNode`/PBC opcode/
executor representation.
**D6 Normal Toron pipeline** — Rune constructs the predicate; binding, pushdown
classification, PBC compilation, residual + scan execution stay Toron-owned.
**D7 Arithmetic deferred to ADR 084** — different use case + pushdown-ineligible.
**D8 Pattern/list/cast deferred** — `Like`/`InList`/`Cast`/`TryCast` out of scope.
**D9 Reconcile the 082 scope guard (discipline #4)** — retarget so the predicate
operators are required and `not`/arithmetic/`like`/`in_list`/`cast` stay forbidden.
**D10 Sandbox intact** — authoring-only methods on `RuneExpr`; no allow-list/
capability/source-normalizer change.

## Behavior gate (hand-verified under Kleene 3VL, on ADR 082's `sales` fixture)

1. **Gate — compound filter** (binds + **executes** → ids **[1,2,3]**):
   ```rune
   ctx.table("sales").filter(
       col("id").lt(lit_i64(4))
           .and(col("amount").gte(lit(10))).and(col("amount").lte(lit(20)))
           .or(col("status").ne(lit_str("open")).and(col("price").is_null()))
           .and(col("region").is_not_null()))
   ```
   Exercises `.lt/.gte/.lte/.ne/.and/.or/.is_null/.is_not_null` and the load-bearing
   3VL cases: `NULL OR TRUE = TRUE` (id3), while rows with NULL amount do not pass
   unless another branch is true (id5 excluded).
2. **`.logical_not`** — `filter(col("active").logical_not())` → ids **[2,5]**.
3. **Null predicate** — `filter(col("price").is_null())` → id **[3]**.
4. **Pushdown preservation** — bind the same compound predicate and assert
   `FilterDisposition::FullyPushed` + scan PBC present. The full gate uses only
   existing PBC-eligible columns, literals, comparisons, booleans, and null
   predicates, and `compile_pbc` already supports those opcodes; a one-operator range
   fixture would under-prove the ADR.

## Architecture Guards

- **Retarget** `adr082_typed_literal_cut_does_not_expand_operator_surface`
  (discipline #4): **required** = `{eq,gt,lt,lte,gte,ne,and,or,logical_not,is_null,
  is_not_null}`; **forbidden** = `{not, add,sub,mul,div, like,ilike,in_list,cast,
  try_cast}`.
- `adr083_rune_predicate_operators_lower_to_existing_expr_variants` — each method
  builds its intended existing `Expr::*`; no new IR.
- `adr083_rune_predicate_operators_are_registered` — all 9 registered as `RuneExpr`
  associated functions; no `.not`.
- `adr083_logical_not_avoids_keyword_collision_with_not` — composes
  `adr081_authored_rune_methods_avoid_reserved_keywords`; the negation method is
  `logical_not`, never `not`.
- `adr083_predicate_operator_cut_does_not_change_engine_substrate` — no production
  change outside `rune_engine.rs`; no binder/executor/IR/PBC/normalizer markers in
  the Rune frontend.
- Carry 079/080/081/082 guards (the reconciled 082 operator-scope guard is the only
  intentional inversion).

## Out of Scope

Arithmetic (ADR 084), `Like`/`InList` (ADR 085+), `Cast`/`TryCast`, typed-null
constructors, dot-field sugar, joins, relation aliases, sort/limit, polymorphic
dispatch, SQL grammar, worker-side Rune, source rewriting.

## Council History

iter-0: Gemini proposed full scalar-expression completion; **Codex + Claude split to
predicate-only** — Claude grounded the `not` keyword collision + caught the 082-guard
inversion (discipline #4); Codex added the arithmetic-pushdown-ineligibility ground
and the compound 3VL gate. iter-1: **Gemini conceded** to predicate-only +
`.logical_not` + guard reconciliation. Genuine 3-of-3; gate ids re-verified under
Kleene 3VL; `is_pbc_pushdown_eligible` confirmed for the predicate set.

## Promotion criteria

9 methods lower to existing `Expr`; `.gt`/`.eq` unchanged; no `.not`, no arithmetic;
4 fixtures cover execution + pushdown (`[1,2,3]`/compound FullyPushed/`[2,5]`/`[3]`);
082 scope guard
reconciled; no production change outside `rune_engine.rs`; 079–082 + sandbox +
group-by + clippy + full package green. Migration:
`plans/impl/083_rune_predicate_operator_surface.md`.
