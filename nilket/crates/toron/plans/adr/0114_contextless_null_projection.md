# ADR 114: Contextless `NULL` Projection Substrate

**Status:** Promoted (plan). **Genuine 3-of-3 on doctrine + scope; representation operator-resolved to B.**
**Toron-only, bound-expression + executor projection substrate; `ScalarValue`/PBC/Wendelon/storage untouched.**
**Predecessors:** 109 (conditional sugar; `NVL2` deferred), 110 (CASE identity), 112 (explicit `NULL` as
binder-consumed `Expr::UntypedNull`; contextless result NULL explicitly deferred), 113 (carried it forward).
**Migration:** `plans/impl/0114_contextless_null_projection.md`.

## Doctrine — untyped NULL stays non-executable; a contextless *result* NULL materializes as `DataType::Null`

112's invariant holds unchanged: `Expr::UntypedNull` is a binder marker that **never reaches execution** — it
types late from context or rejects. 114 adds the one missing *result* value: an expression that has no typed
sibling and no surrounding type, **at a result position**, binds to Arrow `DataType::Null` and executes as a
`NullArray` of input-batch length.

> **`DataType::Null` is a result type of last resort** — never a participant in arithmetic, comparison, storage
> inference, logical identity, or PBC. A contextless NULL is "this column is null for every row and has no better
> type," not a universal coercion source.

## Representation — `BoundExprNode::NullLiteral` (operator-steered, locked)

The concrete value is a **result-only bound-arena leaf `BoundExprNode::NullLiteral`** (built only by the binder via
`append_null()`; root type `DataType::Null`, nullable, `NAKED`), **not** a `ScalarValue::Null`. **`DataType::Null`
is an ephemeral bound-result artifact, not a public scalar literal.** A `ScalarValue::Null` would surface it across
`Expr::Literal`, system rows, insert/write paths, PBC literal emission, and tests — any of which could construct or
encounter it — degrading containment to *defensive rejects*. `NullLiteral` makes containment **structural**: only
the binder's result-null path creates it, and `ScalarValue` stays typed-only so **112's `!scalar_enum.contains("Null")`
guard holds unchanged**. The cost is a bound-arena variant sweep (the 108 `Case` pattern) — real but local and
auditable. *(Rejected: `ScalarValue::Null` — smaller via the `BoundExprNode::Literal(u32)` pool, but it trades
structural containment for vigilance and puts a result-only artifact on the public scalar surface.)*

## Scale boundary

**In:** contextless-NULL materialization at **result-projection roots** — SELECT `Project` (including derived
SELECT outputs through the same `LogicalPlan::Project` arm), `RETURNING`, and **Rune `.project`/`.project_named`**
(which build `LogicalPlan::Project`, inheriting for free) — plus all-null conditionals (the 109 family:
`COALESCE(NULL[,NULL])`, `IFNULL`/`NVL(NULL,NULL)`,
`NVL2(a,NULL,NULL)`, all-null `CASE` incl. missing-ELSE); `DataType::Null`/`NullArray` execution; schema reporting;
`NVL2` as CASE sugar. **Out:** no `ScalarValue::Null`; no executable `Expr::UntypedNull`; no PBC opcode / Wendelon /
storage carrier; no CTAS persistence of `DataType::Null`; no `DataType::Null` coercion into numeric/string/boolean;
no null-only aggregate args (`COUNT(NULL)`/`SUM(NULL)`), `NULL = NULL`/`NULLIF(NULL,NULL)`, `GROUP BY NULL`/`ORDER BY
NULL`/join-key null; no lazy CASE; no Rune `nvl2` surface.

## Grounding (verified)

- `Expr::UntypedNull` exists; `ScalarValue` is typed-only; `BoundExprNode::Literal(u32)` indexes a `Vec<ScalarValue>`
  pool, so a null-only result can't be represented without either broadening `ScalarValue` (rejected) or a new
  bound node (chosen).
- `bind_expr_into_program` rejects `Expr::UntypedNull` (112) — kept as the default; result positions opt in around it.
- Pre-114, `bind_case` errored when `result_types.is_empty()`; 114 changes *that* partition to
  `DataType::Null`.
- The shared result-projection path is `bind_expr_program` + `schema_for_exprs_with_output_names`: SELECT `Project`
  (`binder.rs:151`, covering top-level and derived SELECT outputs), `RETURNING` (`:850`); Rune
  `.project`/`.project_named` build `LogicalPlan::Project` (`rune_engine.rs:224`/`:240`) → same path. The
  **other** `bind_expr_program` callers — predicate (`:132`/`:802`), GROUP BY (`:167`), sort (`:191`), UPDATE
  source projection (`:666`), aggregate args (`:4307`) — must stay on 112's reject/typed-context behavior.
- `new_null_array(&dt, batch.num_rows())` already exists (raw CASE missing-ELSE, `exec.rs:1018`); 114 moves
  row-count-correct `NullArray` into the bound path.
- `is_supported_ctas_column_type` (`infer_ctas_schema`) excludes `DataType::Null` — the storage boundary, kept.
- The 109 conditional-sugar classifier excludes `NVL2` — retargeted to include it as CASE sugar.

## Decisions (promotion-locked)

**D1 — `BoundExprNode::NullLiteral`** (leaf; `append_null()`; type `DataType::Null`, nullable, `NAKED`, pretty
`NULL`; `node_children` empty; PBC treats it as unrepresentable/decline). `ScalarValue` unchanged.

**D2 — Result-position opt-in (scoping hinge).** A raw root `Expr::UntypedNull` materializes to `NullLiteral`
**only** through a `NullRootPolicy::MaterializeResultNull` path used at the result-projection sites (SELECT
`Project`, including derived SELECT outputs, `RETURNING` — and thus Rune projection). The generic `bind_expr_into_program`
`Expr::UntypedNull => Err` arm is **unchanged**; predicate / GROUP BY / sort / aggregate-arg / assignment positions
keep 112's reject-or-resolve. (Gemini's iter-1 blanket materialization in `bind_expr_into_program` was rejected
precisely because it would mistype `WHERE NULL`/`GROUP BY NULL`/`SUM(NULL)`/`NULL+1`.)

**D3 — All-null CASE → `DataType::Null`.** In `bind_case`, when every result arm is `UntypedNull`
(`result_types.is_empty()`), bind output as `DataType::Null`/`NAKED` instead of erroring. Conditions still bind
normally (NULL condition → `Boolean(None)`, no-match). **Mixed** typed/null CASE is unchanged (112/110): typed
branches set the common type, null arms materialize at it, identity is computed from non-null branches only.

**D4 — Separate typed-null vs result-null helpers.** `typed_null_for_data_type` stays the six physical types (112).
Add `materialize_null_for_output_type(builder, output_type)`: `output_type == DataType::Null → append_null()`, else
`resolve_untyped_null(output_type)`. Used in CASE result/missing-ELSE typing. Keeps `DataType::Null` out of
typed-local contexts.

**D5 — Bound executor owns `NullArray`.** `evaluate_bound_program*`: `BoundExprNode::NullLiteral =>
new_null_array(&DataType::Null, batch.num_rows())` — one value per input row, including **zero** rows (empty input →
zero-length `NullArray`, not a one-row artifact). The raw `Expr::UntypedNull` evaluator error stays as a defensive
backstop.

**D6 — Output schema naked `DataType::Null`.** Root type `DataType::Null`, nullable, `NAKED`; no logical metadata
written; decode reads `NAKED`.

**D7 — CTAS/persistence requires a declared type (operator-steered).** *Persisted columns require an actual
declared type.* `CREATE TABLE AS SELECT NULL AS n` (and all-null computed) **fails** through the existing
unsupported-CTAS-output-type gate with cast guidance; `CREATE TABLE AS SELECT CAST(NULL AS BIGINT) AS n` **persists**
nullable `Int64` (the explicit escape — a free consequence of 112's typed cast). 114 stays a query substrate, no
storage-schema change.

**D8 — `NVL2(a,b,c)` is CASE sugar only.** Extend the 109 classifier; exactly three positional args; lower to
`CASE WHEN a IS NOT NULL THEN b ELSE c END`. Inherits CASE type-reconcile, 110 identity, aggregate-rejection, eager
eval, residual execution. No `OP_NVL2`, no bound node, no executor branch, no Rune surface.

**D9 — `DataType::Null` is not a coercion participant.** A materialized null fed into arithmetic/comparison rejects
(`arithmetic_output_type`/comparison return None/reject for `DataType::Null`): `COALESCE(NULL,NULL) + 1` rejects.
`NULLIF(NULL, amount)` → `DataType::Null` (the comparison is typed by `amount`, both result arms null), but
`NULLIF(NULL, NULL)` still **rejects** (the comparison has no typed operand).

## Behavior gates

- `bare_select_null_projects_null_array` — `SELECT NULL FROM sales`: one `DataType::Null` column, nullable, NAKED,
  `len == row count`; `select_null_respects_empty_input_row_count` (zero rows → zero-length).
- `sugar_family_all_null_projects_null` — `COALESCE(NULL)`, `COALESCE(NULL,NULL)`, `IFNULL(NULL,NULL)`,
  `NVL(NULL,NULL)`, `NVL2(a,NULL,NULL)` → `DataType::Null`.
- `case_all_null_branches_project_null` — explicit `ELSE NULL` + missing-ELSE both → `DataType::Null`.
- `nullif_first_explicit_null_operand_projects_null` (`NULLIF(NULL, amount)`) +
  `nullif_two_contextless_nulls_still_rejects` (`NULLIF(NULL, NULL)`).
- `null_result_is_not_a_coercion_participant` — `COALESCE(NULL,NULL) + 1` rejects (no silent widening).
- `mixed_typed_null_behavior_unchanged` — `COALESCE(NULL, amount)`, `CASE WHEN flag THEN NULL ELSE amount END`,
  110 identity, all 112 typed-null contexts unchanged.
- `nvl2_desugars_and_executes`, `nvl2_all_null_results_project_null`, `nvl2_arity_rejects`,
  `nvl2_inherits_case_identity_and_aggregate_rejection`.
- `update_returning_contextless_null_projects_null_column` — `… RETURNING NULL` uses the same substrate
  (ephemeral, not persisted).
- `rune_lit_null_projection_materializes_null_column` — Rune `.project([lit_null()])` → `DataType::Null` for free.
- `ctas_select_null_rejects` + `ctas_cast_null_to_concrete_type_succeeds` (persists `Int64`).
- `untyped_null_still_non_executable` — operand-position null-only still 112; `Expr::UntypedNull` never executes.

## Architecture guards

- `adr114_bound_null_projection_substrate` — `BoundExprNode::NullLiteral` leaf + `append_null` + `DataType::Null`
  root/nullable/NAKED.
- `adr114_untyped_null_stays_binder_only` — `Expr::UntypedNull` still exists & rejects in `bind_expr_into_program`;
  `ScalarValue` has no `Null` (112 guard intact); `BoundExprNode` has no `UntypedNull`; raw-exec marker error kept.
- `adr114_projection_policy_is_the_only_raw_untyped_null_escape` — result/projection binding uses the materializing
  policy; non-result bind sites stay reject-by-default.
- `adr114_bound_executor_materializes_null_array` — executor matches `NullLiteral` → `new_null_array(&DataType::Null,
  batch.num_rows())`.
- `adr114_all_null_case_resolves_to_null_type` — the all-null CASE branch sets `DataType::Null`/NAKED; mixed CASE
  still skips untyped nulls for identity.
- Retarget the 109 sugar guard (+`NVL2`/`lower_nvl2`, no `OP_NVL2`) and the old 112 contextless-null negative gates
  to positive result-position coverage, keeping 112's generic-reject + PBC-residual + transient-marker assertions.
  `pbc.rs` has no null opcode; any `NullLiteral` arm is a decline.

## Council history

iter-0: genuine 3-of-3 on contextless NULL + `NVL2` rider (Codex championed it across 113–114; Claude added the
complete-cut framing). iter-1 doctrine 3-of-3 (UntypedNull non-executable, `DataType::Null` last-resort, NVL2 CASE
sugar, CTAS-out). **Representation: operator-resolved to B (`BoundExprNode::NullLiteral`)** — Codex + Claude leaned
B for structural containment; Gemini leaned A (`ScalarValue::Null`, smaller), superseded by the operator steer
("`DataType::Null` is a result-only bound artifact, not a public scalar literal"). **Scoping hinge:** Codex + Claude
scope materialization to result-projection roots; Gemini's iter-1 blanket `bind_expr_into_program` materialization
was the wrong cut (would mistype `WHERE`/`GROUP BY`/aggregate/operand NULL). **Small-wins sweep (Codex,
operator-relayed):** the 109 sugar-family all-null cases, `RETURNING NULL`, and Rune `lit_null()` projection all
inherit through two shared chokepoints (the all-null CASE path + the result-projection binder), so they're gates,
not new mechanism; the `NULLIF`/coercion-participant gates sharpen the boundary.

## What to do next

Composable derived `ORDER BY`/`LIMIT` + Rune `.sort`/`.limit`; logical-type `DROP`/`ALTER`; declared-additive
logical types (`SUM`/`AVG`); Rune `if_else`/simple-CASE; persisted/storage `DataType::Null` only if a real need
appears; lazy CASE / conditional pushdown if evaluation semantics ever change.

## Promotion criteria

Contextless result NULL binds to `BoundExprNode::NullLiteral` and executes as a row-count-correct `DataType::Null`
`NullArray`; all-null CASE/COALESCE/IFNULL/NVL/NVL2 + `RETURNING NULL` + Rune `lit_null()` projection all project
`DataType::Null`; 112 typed-null + 110 identity unchanged; `DataType::Null` never coerces (`COALESCE(NULL,NULL)+1`
rejects; `NULLIF(NULL,NULL)` rejects); `NVL2` is CASE sugar only; generic `Expr::UntypedNull` still rejects outside
result opt-in; `ScalarValue`/PBC/Wendelon/storage free of a null carrier; CTAS of null-typed output rejects,
`CAST(NULL AS T)` persists; the `adr114_*` guards + reconciled 109/112 guards hold; targeted + Toron suite green.
Migration: `plans/impl/0114_contextless_null_projection.md`.
