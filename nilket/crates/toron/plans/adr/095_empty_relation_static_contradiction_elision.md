# ADR 095: EmptyRelation + Static Contradiction Elision

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — verified against the iter-2
duals: all three carry the identical shape (execution-IR-only `EmptyRelation`; filter-local elision
before PBC; `StaticFilterClass` closed-root + structural-null classifier with no recursive
short-circuit; error-preserving; closed-`NULL` elides; structural cardinality fence; durable
predicates → 096). **Origin:** the elision-substrate slice the council forked over in the 094 cycle
(slice 1 of refinement contracts). **Toron-only.**
**Scope:** `ir/execution.rs`, `binder.rs`, `exec.rs`, and the `ExecutionPlan` walkers; tests.

## Doctrine — a plan-shape optimization, never a new truth source

Replace a `Filter` with `ExecutionPlan::EmptyRelation` **only** when the binder proves the filter is
*statically always-rejecting* under SQL `WHERE` 3VL — the predicate can never be `TRUE`, so it admits
zero rows. A `WHERE` keeps a row **iff** the predicate is `TRUE` (Arrow `filter_record_batch`,
`exec.rs:234`, drops both `false`- and `null`-mask rows), so "always-rejecting" = statically
`{FALSE} ∪ {NULL}`. Two hard limits: **Unknown stays Unknown** (a *data-dependent* predicate never
elides), and **errors are preserved** (eliding must not turn a query that errors today into a
zero-row success). EmptyRelation means "zero input rows with this schema," **not** "zero output rows
for the query" — so it replaces only the `Filter` node; parent operators keep their cardinality.

## Grounding

- **No `EmptyRelation` exists.** `ExecutionPlan` (`ir/execution.rs:177`) has no empty-relation
  variant; `ExecutionPlan::schema()` (`:288`) and ~15 exhaustive walkers must learn the new one.
- **The filter seam is centralized.** `LogicalPlan::Filter` binds input + predicate, then calls
  `plan_filter_with_predicate_pushdown(input, predicate)` (`binder.rs:1084`). The predicate arrives
  already bound — the classifier runs at the **top**, before `predicate_pushdown_plan` (`:1090`).
- **`ExecutionPlan` is serialized in `TaskSpec`** (`distributed/task.rs`) — so a new variant *is* a
  same-version Toron-internal task-payload change (not "no wire").
- **The closed-root evaluator exists.** `Executor::evaluate_bound_program_without_storage(program,
  batch)` (`exec.rs:1017`) reads columns by ordinal; a **column-free** predicate evaluates fine on a
  zero-column batch — but the batch needs `RecordBatchOptions::new().with_row_count(Some(1))` (a
  zero-row dummy can't reveal the literal root value).
- **The cardinality fence is free.** `bound_global_aggregate` (`exec.rs:1117`) always emits one row
  (`CountAll` sums `num_rows → 0`, `:1126`), so `Aggregate(EmptyRelation)` correctly yields `count=0`.
  Replacing only the `Filter` keeps the parent `Aggregate` intact.

## Decisions (promotion-locked)

**D1 — Execution-IR-only node.** `ExecutionPlan::EmptyRelation { schema: SchemaRef }`. No `LogicalPlan`
variant, no SQL/Rune surface — SQL still authors ordinary filters; the binder proves emptiness
post-bind. **D2 — Internal payload, honest scope.** One Toron-internal execution-plan variant (valid
in `TaskSpec`, serde-derived); **no SQL syntax, no logical-IR variant, no PBC opcode, no
Wendelon/storage/public-protocol wire, no dependency, no cross-version contract.** Executor arm:
`Ok(vec![RecordBatch::new_empty(schema.clone())])` — no storage scan; downstream operators already
consume empty batches. **D3 — Filter-local elision.** In `plan_filter_with_predicate_pushdown`, run
the classifier first; on `AlwaysReject` return `(ExecutionPlan::EmptyRelation { schema:
input.schema() }, fallback_profile.unwrap_or(QueryProfileHint::Scan))` — no `Filter` above, no PBC
attached, no `apply_scan_profile_hint` (the elided plan has no scan to profile). **D4 — Conservative
always-reject classifier** `StaticFilterClass { AlwaysPass, AlwaysReject, Unknown }` (named for filter
semantics; only **`AlwaysReject` elides**; `AlwaysPass` is computed but 095 does not remove
true-filters):
- **whole root column-free** (single root + no `BoundExprNode::Column`) → evaluate via
  `evaluate_bound_program_without_storage` on the 1-row zero-column dummy → `Boolean(Some(false))` /
  `Boolean(None)` ⇒ `AlwaysReject`; `Boolean(Some(true))` ⇒ `AlwaysPass`; **any error / non-boolean /
  arity surprise ⇒ `Unknown`**.
- root is exactly `BoundExprNode::Unary { op: BoundUnaryOp::IsNull, input }` over a `Column` whose
  `ResolvedColumn.nullable` is `false` ⇒ `AlwaysReject`; nullable ⇒ `Unknown`.
- everything else (any column reference) ⇒ `Unknown`. **No recursive boolean short-circuit** —
  `FALSE AND id=5` is column-bearing ⇒ `Unknown` (avoids a partial evaluator *and* eager-error
  masking by construction; no `FALSE OR …`, no `NOT`/De Morgan factoring).

**D5 — Error preservation.** Because the closed-root path reuses the eager evaluator, a strict
cast/arithmetic error during classification becomes `Err ⇒ Unknown ⇒ don't elide`, so the query
still raises it at execution exactly as today. **D6 — No always-true filter removal** (deferred
hygiene). **D7 — Logical identity survives.** EmptyRelation carries a `SchemaRef`, so 087–093 logical
sidecars ride the carried schema through the 089 metadata path structurally; CTAS over an empty
relation materializes a zero-row table with the correct durable identities (guard-lite fixture).
**D8 — DML scope.** SELECT in scope; **UPDATE-source elision falls out** (it routes through the
shared helper); **DELETE deferred** (it stores its predicate directly — `DELETE WHERE 1=0` still
scans + deletes nothing, correct but not zero-I/O). **D9 — Durable refinement predicates deferred to
ADR 096** — no `LogicalTypeDefinitionV1` field, no `CREATE LOGICAL TYPE … CHECK`, no write-time
enforcement; 096 feeds durable facts into this same classifier.

## The ripple (the real bulk — named, not hand-waved)

A new `ExecutionPlan` variant compile-forces ~15 exhaustive matches; ~4 carry real logic —
`exec.rs:649` dispatch (empty batch), `ir/execution.rs:288` `schema()` (carried schema),
`binder.rs:2370` `optimize_for_required` (return-as-is leaf), `fragment.rs:97-122` (no-exchange
single-task stage) — and ~11 are trivial childless-leaf arms (`binder.rs:2277`; `fragment.rs`
{`883`,`934`,`964`,`990`,`1035`,`1109`}; `scheduler.rs:769` → `None`; `server/mod.rs`
{`862`,`885`,`908`,`1059`,`2011`} → label `"empty_relation"`). No new `_` wildcard anywhere — the
compile-forced exhaustiveness is the safety net, pinned by a walker guard.

## Behavior gate

`where_false_literal_elides_no_storage_scan` (recording scan: 0 calls); `where_literal_comparison_false_elides`
(`1=0`); `where_closed_null_predicate_elides` (built via `ScalarValue::Boolean(None)` if SQL lacks a
boolean-NULL literal); `nonnull_column_is_null_elides`; **`nullable_column_is_null_does_not_elide`**;
`where_column_predicate_does_not_elide` (`id=5`); **`where_false_and_column_predicate_stays_out_of_scope`**
(`FALSE AND id=5` is column-bearing ⇒ no elide — proves no recursive short-circuit);
**`where_false_and_erroring_expression_does_not_elide`** (eager-error fence — closed strict cast that
errors stays a Filter; execution still raises); `empty_relation_executor_returns_one_empty_batch`;
**`count_star_over_static_false_returns_single_zero`** + `grouped_aggregate_over_static_false_returns_zero_groups`
(cardinality fence); `project_over_empty_relation_preserves_schema`; (guard-lite)
`update_where_static_false_uses_empty_source`, `empty_projection_preserves_logical_identity`.

## Architecture guards

`adr095_empty_relation_is_execution_plan_only` (in `ExecutionPlan`, absent from `LogicalPlan`);
`adr095_filter_elision_runs_before_pbc_pushdown` (classifier precedes `predicate_pushdown_plan`;
elided plans carry no PBC); `adr095_static_filter_classifier_preserves_where_3vl` (closed
`false`/`null` ⇒ reject; data-dependent ⇒ unknown); `adr095_static_filter_classifier_is_error_conservative`
(column-free whole root + structural `IsNull` only; no recursive `FALSE AND _`; error ⇒ `Unknown`);
`adr095_empty_relation_executor_does_not_scan_storage` (behavior guard);
`adr095_scalar_aggregate_cardinality_survives_empty_relation`;
`adr095_plan_walkers_handle_empty_relation_as_leaf` (the ~15-site sweep, no wildcard);
`adr095_no_sql_pbc_wendelon_storage_surface`; `adr095_refinement_persistence_deferred_to_096`.

## Scale boundary

Toron-internal, single-relation, and a no-exchange single-task stage when routed through distributed
execution; reuses the existing evaluator + aggregate + empty-batch idioms. No
Wendelon/storage/PBC/public-wire/SQL surface; one internal execution-plan variant. No new scale axis.

## What to do next

- **ADR 096 — durable refinement-predicate persistence + write-time enforcement** (cross-ring:
  `LogicalTypeDefinitionV1` gains a predicate field; enforcement is net-new — `CHECK` rejected at
  `sql_planner.rs:668`, INSERT validation type-only). A durable `value > 0` fact becomes another
  `StaticFilterClass` input — no plan-substrate reopening.
- Carried: Rune `Like`/`ILike` (smallest); DROP/ALTER LOGICAL TYPE; information_schema.logical_types;
  PK/FK-derived identity.

## Council history

094 cycle forked over this (elision-first vs durable-predicate); operator carried it to 095. iter-0:
Claude grounded the ~15-site ripple + the structural cardinality fence; Codex grounded the
`TaskSpec`/wire reality + the eager-error masking risk + the 1-row dummy-batch requirement; Gemini
synthesized. iter-1: Codex conceded closed-`NULL` elides (matching Claude's column-free-vs-data-
dependent distinction, grounded on Arrow filter dropping null-mask rows); Claude conceded the
no-wire reframing + the error fence; 3-of-3. iter-2 (dual): all three duals identical; Codex's exact
`BoundUnaryOp::IsNull` spelling + helper split folded in. Migration:
`plans/impl/095_empty_relation_static_contradiction_elision.md`.

## Promotion criteria

`WHERE 1=0` / `WHERE false` / closed-`NULL` / `non_nullable_col IS NULL` elide to `EmptyRelation` with
zero storage scans; data-dependent predicates + `nullable_col IS NULL` + erroring closed expressions
do **not** elide (errors still raised); `SELECT COUNT(*) … WHERE 1=0` returns one `0` row, grouped
aggregate returns zero groups, project preserves schema; the ~15 walkers handle the variant
exhaustively (guarded); no SQL/PBC/Wendelon/public-wire surface; durable predicates deferred to 096.
Migration: `plans/impl/095_empty_relation_static_contradiction_elision.md`.
