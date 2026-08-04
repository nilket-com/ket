# ADR 074: Predicate Widening — `IN` (native `OP_IN_LIST`) + `BETWEEN`

**Status:** Promoted (plan) — **2-of-3 (Claude, Codex)** on the full design;
**Gemini iter-4 diverged** (included `LIKE`/`ILIKE` and claimed 3-of-3), resolved
against by the all-or-nothing PBC binding that makes a residual `LIKE` regress
pushdown of its eligible siblings (see Council History).
**Seed:** ADR 073 refreshed the parser (0.62) without widening SQL; this is the
first predicate-surface widening it de-risked.
**Category:** Toron compute-ring SQL predicate surface. **No Wendelon / SQL-wire /
worker-wire / storage / audit change** — the `OP_IN_LIST` opcode + its Wendelon
execution already exist; 074 only teaches Toron to use that contract.

## Scale Boundary

074 widens Toron's single-table predicate surface at the planner / binder /
executor / PBC-compiler layer. It does not change distributed scheduling,
fragmentation, worker wire payloads, Wendelon bytecode semantics, Wendelon storage
predicate extraction, schema evolution, or mixed-filter pushdown policy. Partial
PBC residual splitting is the next scale boundary and is explicitly deferred to
ADR 075.

## The decisive grounding (one finding drives two decisions)

Toron's PBC binding is **all-or-nothing**: `lower_where` recombines all conjuncts
into one `Filter` (`sql_planner.rs:2778`), and the binder runs
`compile_pbc(&bound_predicate)` on the **whole** predicate ⇒ `FullyPushed`, else
`EvaluateInToron` — **no partial pushdown** (`binder.rs:105-116`). This drives:

1. **`IN` is first-class, not desugared.** A desugared `IN` → deep `OR`-tree of
   `Eq`; a large list can exceed PBC program/stack limits ⇒ `compile_pbc`
   declines ⇒ the **whole** mixed filter drops to Toron (eligible siblings stop
   pushing). The native **`OP_IN_LIST(count)` is compact**, so it compiles and
   pushes even at the list bound. (Both Claude and Codex converged here — Codex
   withdrew its earlier pure-desugar position for exactly this reason.)
2. **`LIKE`/`ILIKE` are deferred to ADR 075.** A non-PBC `LIKE` makes
   `compile_pbc` return `None`, so `WHERE amount > 10 AND name LIKE 'A%'` would
   stop pushing `amount > 10` too. Doing `LIKE` right needs a **partial-pushdown
   residual-split** (push eligible conjuncts, residual the `LIKE`) that doesn't
   exist today — ADR 075's job, not 074's.

## Decisions (13, Promotion-Locked)

**D1 Accepted surface:** `<expr> [NOT] IN (<expr>, …)`, `<expr> [NOT] BETWEEN
<low> AND <high>`. Reject `LIKE`/`ILIKE`/`CASE`/subquery-`IN`/`UNNEST`/row-value
`IN`/`ALTER`.

**D2 `IN` is first-class in Toron IR.** Add `Expr::InList { expr, list, negated }`.
No sqlparser AST leaks past `sql_planner.rs`.

**D3 `IN` is first-class in bound IR.** Add `BoundExprNode::InList { expr, list,
negated }` + builder, validation, pretty-print, nullable propagation, child-walk,
offset/remap.

**D4 `IN` pushes via the existing `OP_IN_LIST`.** `pbc.rs` imports the existing
`OP_IN_LIST` byte constant (`wendelon/src/schema/pbc.rs:66`) and emits
`OP_IN_LIST` + little-endian u16 count for `InList` — **not a new Wendelon opcode,
no Wendelon source change**. Stack order matches the existing Wendelon consumer:
push target first, then members; `OP_IN_LIST(count)` consumes the top `count`
entries as members and the next entry as target. `NOT IN` = `IN_LIST` + existing
`OP_NOT`.

**D5 `IN` residual eval is explicit.** When not pushed, `exec.rs` evaluates
`InList` by folding the existing equality + Kleene-`or` kernels over the bound
members. Arrow `in_list`/`in_list_utf8` may be used only as a proven-equivalent
fast path. The requirement is **exact parity** with the PBC path and with
`x = a OR x = b`, including nullable target/member columns.

**D6 `IN` list bounds.** `MAX_IN_LIST_ITEMS = 32`. The bound is deliberately
below Wendelon's `STACK_DEPTH_LIMIT = 64`: native `OP_IN_LIST(count)` consumes a
target plus `count` live member values, and mixed boolean predicates may already
have pending operands on the stack. Reject empty, over-limit, `IN (SELECT …)`,
`IN UNNEST(…)`, row-value `IN`, and explicit `NULL` members. Members reuse the
existing `lower_expr` surface; PBC emission still requires the target and all
members to be PBC-eligible, so arithmetic members use the residual path until
arithmetic PBC exists.

**D7 `IN` typing reuses comparison policy.** Target + each member use the existing
comparison/literal-coercion lattice; mismatches fail at binding with bounded
errors.

**D8 `BETWEEN` stays planner sugar.** `x BETWEEN lo AND hi` → `And(Gte, Lte)`;
`NOT BETWEEN` → `Or(Lt, Gt)`. **No `Expr::Between`, no bound node, no executor arm,
no opcode** — pushes via existing comparison opcodes. Left expr duplicated
(deterministic).

**D9 NULL posture inherited.** Explicit `NULL` literals still reject in
`lower_expr` (`:2943`), so `IN (…, NULL)` / `BETWEEN NULL …` reject through the
existing path; column nulls flow through Arrow comparison/boolean semantics + the
new `IN` evaluator.

**D10 Parser accounting (073 discipline).** `lower_expr` names `SqlExpr::{InList,
Between}` and explicitly rejects `InSubquery`, `InUnnest`, `Like`, `ILike`,
`SimilarTo`, `RLike`, `Case`, and PostgreSQL `~~` / `~~*` binary-operator spellings
— no wildcard accept; sqlparser stays quarantined to `sql_planner.rs`.

**D11 `LIKE`/`ILIKE` are ADR 075 material** — together with **partial PBC residual
splitting** (or an explicit accept of mixed-filter pushdown loss). 074 does
neither.

**D12 ADR-014 reconciliation.** Retarget `adr014_does_not_add_sql_in_lowering_to_
toron_planner` (`architecture_test.rs:470`): ADR 014 did not add SQL `IN`; **074
now owns list-`IN`**; subquery-`IN` remains unsupported.

**D13 No cross-ring drift.** Toron planner/IR/binder/exec/pbc-compiler/tests only;
no Wendelon/storage/wire/worker/audit/telemetry change; no new dependency.

## Architecture Guards (12 + meta)

`adr074_in_list_is_first_class_expr_and_bound_node`,
`adr074_in_list_compiles_to_existing_op_in_list_opcode`,
`adr074_in_list_adds_no_new_wendelon_pbc_opcode`,
`adr074_in_list_negation_uses_existing_not_opcode`,
`adr074_in_list_residual_eval_is_implemented_without_new_wendelon_surface`,
`adr074_in_list_pushdown_eligible_iff_members_eligible`,
`adr074_in_list_has_static_item_limit`,
`adr074_between_desugars_with_no_new_ir_or_opcode`,
`adr074_like_ilike_similar_rlike_case_subquery_in_remain_unsupported`,
`adr074_alter_table_remains_unsupported`,
`adr074_retargets_adr014_sql_in_absence_guard`,
`adr074_no_wendelon_storage_wire_worker_audit_or_telemetry_change`.
**Meta/convention:** `adr074_sqlparser_ast_remains_quarantined_to_sql_planner`,
`adr074_nilket_root_stays_runtime_free`,
`adr074_new_surface_is_brand_clean_and_has_no_v1_suffix`,
`adr074_prior_010_023_024_025_026_029_030_034_036_064_073_guards_continue_to_pass`.

## Integration Fixtures (13)

`where_in_list_pushes_op_in_list_to_wendelon`;
`where_not_in_list_pushes_op_in_list_plus_not`;
`in_list_at_max_items_pushes_without_pbc_limit_error` (the opcode-vs-OR-tree win
at the 32-item stack-safe bound);
`where_in_list_residual_evals_when_not_pushdown_eligible`;
`where_in_list_filters_rows`; `where_not_in_list_filters_rows`;
`in_list_with_column_or_arithmetic_member_reuses_existing_lowering`;
`in_list_rejects_empty_over_limit_subquery_unnest_rowvalue_or_null`;
`where_between_desugars_and_filters`; `where_not_between_filters`;
`between_pbc_matches_equivalent_and_chain`;
`like_ilike_case_subquery_in_remain_unsupported`;
`alter_table_remains_unsupported`. Existing WHERE/HAVING/projection/RETURNING/
UPDATE + 073 parser fixtures stay green.

## Carried → ADR 075 and beyond

**ADR 075: `LIKE`/`ILIKE` + partial PBC residual-splitting** (the prerequisite —
it lets any non-PBC predicate ride alongside pushed conjuncts without regressing
their pushdown). Also: `CASE`/value-conditionals; subquery/`UNNEST`/row-value
`IN`; `ESCAPE`/`SIMILAR TO`/`RLIKE`/regex; `ALTER TABLE` + schema evolution
(cross-ring); composite-PK/`UNIQUE`.

## Council History

iter-0: 3-way (Claude IN+BETWEEN desugar; Codex +residual-`LIKE`; Gemini `ALTER`).
iter-1: Claude+Codex on predicate-widening (`ALTER`→cross-ring). iter-2: Claude
verified `OP_IN_LIST` exists → **first-class `IN`**, kept `LIKE` residual; Codex
held pure desugar + flagged the `LIKE` residual-split gap. iter-3: Claude verified
the binder is **all-or-nothing** (`binder.rs:105-116`) → **defer `LIKE`** (Codex
was right) and the finding **strengthens first-class `IN`**. iter-4: **Codex
withdrew pure-desugar and adopted first-class `IN`** — converging with Claude on
first-class `IN` + `BETWEEN` desugar + defer `LIKE`/`ILIKE`. **Gemini iter-4 held
`LIKE`+`ILIKE`** and claimed 3-of-3 — resolved against by the all-or-nothing
binding (its residual `LIKE` silently regresses pushdown of eligible siblings; the
residual-split it needs is ADR 075).

## Promotion criteria met

D-set 13; 12 + meta guards; 13 fixtures; first-class `IN` emits the **existing**
`OP_IN_LIST` (no Wendelon change); `BETWEEN` adds no IR/opcode; `LIKE`/`ILIKE`
deferred with the residual-splitting reason; ADR-014 retargeted; all carried
rejects explicit; no cross-ring drift. Migration:
`plans/impl/0074_predicate_widening_in_between.md`.

## Conclusion

074 widens predicates to first-class `IN` (→ the native `OP_IN_LIST`, compact +
robust pushdown, no Wendelon change) + `BETWEEN` desugar (no IR). `LIKE`/`ILIKE`
wait for ADR 075 together with the **partial-pushdown residual-split** they
require — because the binder's pushdown is all-or-nothing today, a residual `LIKE`
would silently regress pushdown of its eligible siblings.
