# ADR 112: Explicit `NULL` Literal

**Status:** Promoted (plan). **Genuine 3-of-3 on carrier + context-typed scope; operator-resolved D11 include.**
**Toron-only, transient-IR + binder substrate; no bound/executor-kernel/PBC/Wendelon substrate change, with one
executor evaluator correction for residual IN-list 3VL.** **Predecessors:** 024 (literals), 087/092/093
(transient binder-consumed `Expr` + logical-cast reconcile path), 108/109/110/111 (CASE substrate + the
`Utf8(None)` sentinel this ADR retires). **Migration:** `plans/impl/112_explicit_null_literal.md`.

## Doctrine — NULL is typed late by context, never by pretending to be Utf8

`NULL` has no type of its own. Today the planner fakes one: it emits `ScalarValue::Utf8(None)` at two authorized
sites (`update_null_placeholder` `sql_planner.rs:797`, `conditional_null_placeholder` `:5467`) and the binder
pattern-matches that exact value to re-type it (`typed_update_null_for_target` `binder.rs:1074`,
`is_case_null_placeholder`/`typed_null_for_case` `:4818`/`:4905`), while the general expression path **rejects**
NULL outright (`:3690`). The 109 code carries the to-do in a comment: *"Revisit this if general explicit NULL
literals become supported"* (`binder.rs:4873`). This is that ADR.

112 replaces the sentinel with **one real marker and one rule**:

> `Expr::UntypedNull` may appear in an unbound plan; a `BoundExprProgram` contains **only** typed `T(None)`. The
> binder materializes the marker wherever a **local context supplies a physical type** — a designated slot (CAST
> target, column assignment, CASE/Boolean/null-test position) or a typed sibling operand (comparison, arithmetic,
> IN) — and
> **rejects** where no type is available, pointing the author at `CAST(NULL AS <type>)`.

Reject is the *default* arm, so any position not taught to type a NULL is sound by construction. This is the
`SqlLogicalCast`/`LogicalAscription` pattern (a transient `Expr` the binder resolves away): `UntypedNull` **never
reaches `BoundExprNode`**, so there is no executor kernel, PBC opcode, Arrow `NullArray`, storage, or Wendelon
change. The one executor nuance is not a NULL representation: residual IN-list evaluation must combine member
equality masks with Kleene OR so the newly legal `x IN (NULL, v)` obeys 3VL (`TRUE` wins over `UNKNOWN`).

## Scale boundary — the *complete* explicit-NULL cut (not a granular slice)

This ADR deliberately spans the whole explicit-NULL surface in one cut rather than spawning a string of tiny
follow-ons. **In:** `Expr::UntypedNull`; lift the `:3690` reject; materialize at every local-type context — CAST /
TRY_CAST (physical **and** logical), UPDATE assignment, CASE result + missing-ELSE, Boolean/null-test positions,
comparison, arithmetic, IN-list; retire the sentinel + unify the typer; Rune `lit_null()`. **Out — by genuine boundary, not
granularity:** contextless `SELECT NULL` / `COALESCE(NULL, NULL)` (needs a `DataType::Null` + Arrow `NullArray`
*execution* substrate — a real separate cut, not friction); `NVL2` (a 109-family conditional sugar, a different
topic — it never produces a NULL literal); INSERT row-value rewrite (its `InsertLiteral::Null` path already works).
One executor evaluator correction rides with D7: `evaluate_in_list` uses `or_kleene` for residual IN-list masks so
NULL members do not poison later TRUE matches. This is not a new execution substrate, kernel, or typed-NULL carrier.

## Grounding (verified)

- `ScalarValue` is typed-only (`ir.rs:451`, six `T(Option<_>)`); typeless-ness can't live there → a transient
  `Expr` carrier, joining `SqlLogicalCast`/`LogicalAscription` (`ir.rs:499`/`:503`).
- **PBC already declines typed NULL:** `emit_literal` (`pbc.rs:198`) encodes only `T(Some(_))`; the catch-all `_
  => NullLiteralUnrepresentable` (`:231`) is pinned by `Int32(None)` tests (`:389`/`:409`). So `WHERE x = NULL`
  stays residual `EvaluateInToron` with **zero** PBC/Wendelon change.
- **Binary-op already supplies a local type contract:** `bind_comparison` coerces then checks `left_type !=
  right_type` (`:5138`); `bind_arithmetic` computes `output_type` + `cast_if_needed` both sides (`:4743`). Resolve
  the untyped-null operand to the peer type *before* that check — a small insertion, not new machinery.
- **Logical-cast reconcile handles a typeless operand for free:** a materialized typed null is naked-identity, and
  the 092/093 path runs `reconcile_identity` then stamps; `reconcile(naked, T) = add`, so `CAST(NULL AS CustomerId)`
  stamps `CustomerId` with no conflict (physical from the logical spec).
- Sentinel producers `:797`/`:5467`; consumers `:1074`/`:4818`/`:4905`. IN-member NULL reject at `:4012` (lifted by
  D7); LIKE-pattern NULL reject `:3981` (kept). INSERT NULL is the separate `InsertLiteral::Null` path (`:908`). 108
  treats a NULL condition as no-match.

## Decisions (promotion-locked; D11 operator-resolved include)

**D1 — `Expr::UntypedNull` transient leaf.** `Value::Null → Expr::UntypedNull` (delete the `:3690` reject).
Binder-consumed; never in `BoundExprNode`.

**D2 — Reject is the default; well-formedness is opt-in.** The generic `bind_expr_into_program` arm →
`UnsupportedExpression`: *"NULL literal has no type in this position; wrap it in CAST(NULL AS <type>)."*
Context-providers intercept `UntypedNull` before this arm.

**D3 — One materializer.** `typed_null_for_data_type(&DataType) -> Result<ScalarValue>` (Boolean/Int32/Int64/
Float32/Float64/Utf8). Replaces `typed_null_for_case` and the inline `typed_update_null_for_target` match.

**D4 — Designated-slot providers.** `CAST(NULL AS T)` / `TRY_CAST(NULL AS T)` → typed null from `T` (physical, no
kernel); `UPDATE … SET col = NULL` → typed from the target column; CASE result branch and missing-ELSE → typed
from the unified non-null branches. `SELECT CAST(NULL AS BIGINT)` is the universal projection escape.

**D5 — Typed-sibling scalar providers.** `bind_comparison` / `bind_arithmetic` resolve an `UntypedNull` operand to
the *other* operand's physical type before coercion. `amount = NULL` / `amount + NULL` bind; **residual** (typed
NULL declines PBC); `amount = NULL` returns no rows (3VL). Both operands `UntypedNull` → reject.

**D6 — Boolean/null-test-position NULL.** A CASE/IF condition and `AND`/`OR`/`NOT` operand of `UntypedNull` →
`Boolean(None)`; a NULL condition is no-match (108). `IF(NULL, 1, 0)` → else. `IS NULL` / `IS NOT NULL` over
`UntypedNull` also materialize a typed NULL input (Boolean is fine; the input type is irrelevant to the null test),
which is required for `COALESCE(NULL, amount)` after the sentinel is retired.

**D7 — IN-list NULL + the `NOT IN (NULL)` trap.** Lift the `:4012` reject; members type from the IN target. Pinned
3VL: `x IN (NULL, v)` → only `x = v` rows; **`x NOT IN (NULL, v)` → ZERO rows** (`x <> NULL` is NULL, so the
conjunction is never TRUE). If the IN target is `UntypedNull`, type it from the compatible non-null members
(`NULL IN (1)` binds and returns no rows in a WHERE predicate); all-null target/list (`NULL IN (NULL)`) rejects.
Residual (typed-null member declines PBC).

**D8 — Retire the sentinel.** Delete `update_null_placeholder`, `conditional_null_placeholder`,
`is_case_null_placeholder`. The six 109 sugars (`COALESCE`/`IF`/`IIF`/`IFNULL`/`NVL`/`NULLIF`) desugar through
normal `lower_expr` onto `Expr::Case`, no placeholder; `NULLIF`'s synthetic THEN-NULL emits `Expr::UntypedNull`
directly. `CASE WHEN c THEN NULL ELSE x END` and `COALESCE(NULL, amount)` work via D4/D6.

**D9 — Preserve all-NULL reject + 110 identity.** All-`UntypedNull` conditional rejects ("no typed (non-NULL)
result branch"). A NULL branch is identity-neutral by **source shape** (`matches!(_, Expr::UntypedNull)`), not a
bound sentinel — 110 preserve/mixed-reject/widening-reject are byte-identical. `NULLIF(x, NULL)` → `x`;
`NULLIF(NULL, x)` rejects (result type unknown).

**D10 — Rune `lit_null()`.** Thin `lit_null() -> RuneExpr` building `Expr::UntypedNull`; no Rune-side checks, same
binder rules. **Delivers the 111 D5 unblock**: `case([when(c, t)], lit_null())` is a Rune CASE with an explicit
null else. Retarget the Rune surface-name guards (admit `lit_null`).

**D11 — `CAST(NULL AS <LogicalType>)` INCLUDED.** A `SqlLogicalCast` over `UntypedNull` resolves its physical from
the durable logical spec (092/093 async resolution), materializes `typed_null_for_data_type(physical)`, and stamps
the logical identity via the existing `reconcile_identity` path (`reconcile(naked, T) = add`). `CAST(NULL AS
CustomerId)` → a null Int64 carrying `CustomerId`; `TRY_CAST` nullable parity. *(Operator call: Codex owns the
impl and judged it a natural extension of the same helper; folded in rather than deferred to avoid a granular
follow-on ADR.)*

**D12 — No bound-arena / executor kernel / PBC / Wendelon / storage substrate change.** `UntypedNull` is a
transient leaf; the logical-`Expr` walkers 108 touched gain a trivial leaf arm. The executor has one load-bearing
evaluator correction: residual `evaluate_in_list` uses Kleene OR for `x IN (NULL, v)` / `NOT IN (NULL, v)` 3VL
correctness. The defensive raw-`Expr` `UntypedNull` arm is an internal backstop; no `BoundExprNode::UntypedNull`,
PBC opcode, `NullArray`, storage, or Wendelon carrier exists. INSERT `InsertLiteral::Null` (`:908`) stays (may
share the typer).

## Behavior gates

- `cast_null_as_type_projects_typed_null` (`CAST(NULL AS BIGINT)` → Int64 null row); `try_cast` parity.
- `cast_null_as_logical_type_carries_identity` — `CAST(NULL AS CustomerId)` → null Int64 stamped `CustomerId`
  (089 schema metadata); `TRY_CAST` parity (D11).
- `case_explicit_null_branch_types_from_siblings`; `case_missing_else_typed_without_sentinel` (regression).
- `comparison_with_null_is_3vl_residual` (`WHERE amount = NULL` → 0 rows, residual, no PBC);
  `arithmetic_with_null_executes`.
- `in_list_null_member_3vl` + **`not_in_null_member_returns_zero_rows`** (D7) + `null_in_list_target_returns_none`.
- `null_is_null_and_is_not_null_bind_without_sentinel` (D6, sugar regression support).
- `boolean_null_condition_is_no_match` (`IF(NULL,1,0)` → else); `coalesce_if_explicit_null_via_case` (sugar
  regression); `nullif_second_operand_null_returns_first` / `nullif_first_operand_null_rejects`.
- `update_set_null_regresses`; `case_all_null_branches_reject`; `case_null_branch_is_identity_neutral` (110).
- `bare_null_projection_rejects` (D2 message, `CAST(NULL AS T)` guidance); `coalesce_all_null_arguments_reject`.
- `rune_lit_null_in_case_and_comparison` (D10).

## Architecture guards

- `adr112_explicit_null_is_transient_binder_consumed` — `Expr::UntypedNull` exists; **no** `BoundExprNode` / PBC
  arm (pins transient-ness, the `SqlLogicalCast` precedent); the executor's raw-`Expr` arm is only a defensive
  unresolved-marker backstop.
- `adr112_null_sentinel_retired` — no `Utf8(None)` sentinel; `update_null_placeholder` /
  `conditional_null_placeholder` / `is_case_null_placeholder` absent; one `typed_null_for_data_type`.
- `adr112_untyped_null_rejects_without_context` — the reject backstop.
- `adr112_null_predicates_stay_residual` — typed-NULL predicates decline PBC (`NullLiteralUnrepresentable`).
- Reconcile the 109 sentinel guards + the 110 identity guard (skip NULL branches by source shape) and the Rune
  surface-name guard (admit `lit_null`) — discipline #4.

## Council history

Topic 3-of-3 from iter-0. **Representation** converged 3-of-3 on the transient `Expr::UntypedNull` (Gemini conceded
its iter-1 `ScalarValue::Null` — an execution value can't carry planner-only state — and its default-contextless-
to-Utf8, which is silent schema drift). **Scope** converged 3-of-3 after Claude conceded its iter-1
comparison/arithmetic *deferral* — on grounding, not headcount: the binary-op type contract is already in
`bind_comparison`/`bind_arithmetic` and PBC already makes typed-NULL predicates residual, so deferring would only
spawn an immediate ADR for `WHERE x = NULL`. Claude's grounded contributions: the PBC-residual *mechanism* pin and
the `NOT IN (NULL) → empty` 3VL trap as an explicit gate. **D11** (`CAST(NULL AS LogicalType)`) was Claude-defer
vs Codex-include at iter-2; the operator resolved it **include** (Codex owns the impl and judged it a natural
extension of the same helper + reconcile path) — keeping the explicit-NULL story complete in one cut rather than a
granular follow-on. Name `UntypedNull` (clearer than `Null`).

## What to do next

Contextless `SELECT NULL` via a deliberate `DataType::Null` / Arrow `NullArray` projection contract (a real
execution-substrate cut); `NVL2(a,b,c)` (the last 109-family sugar straggler); aggregate-output logical identity
(the non-NULL iter-0 lead); collapse INSERT `InsertLiteral::Null` onto `typed_null_for_data_type`.

## Promotion criteria

Explicit NULL lowers as `Expr::UntypedNull` and materializes only in typed contexts (CAST physical+logical /
assignment / CASE / Boolean/null-test / comparison / arithmetic / IN); contextless NULL rejects with `CAST(NULL AS T)`
guidance; the `Utf8(None)` sentinel is gone (one marker, one `typed_null_for_data_type`); the six 109 sugars + `SET
col = NULL` regress green; the `NOT IN (NULL) → empty` trap + 3VL comparisons are pinned residual; 110 identity +
all-NULL reject hold; `CAST(NULL AS CustomerId)` carries identity; Rune `lit_null()` binds by the same rules; the
`adr112_*` guards pin transient-ness + sentinel-retirement + reject backstop + PBC-residual; **no** bound-arena /
executor-kernel / PBC / Wendelon / storage substrate change, except residual `evaluate_in_list` adopting Kleene OR
for 3VL with NULL members; targeted + Toron suite green. Migration:
`plans/impl/112_explicit_null_literal.md`.
