# Impl Plan — ADR 134: Optimizer Join-Decision EXPLAIN (SQL + Rune)

**Migration for** `plans/adr/134_optimizer_join_decision_explain.md`.
**Scope:** `ir.rs` (`LogicalPlan::Explain`), `ir/execution.rs`
(`SystemTableKind::OptimizerExplain`), `sql_planner.rs` (lower `Statement::Explain`,
reject non-default + DML; read-only relation gate), `rune_engine.rs` (`.explain()` +
Module registration), `binder.rs` (bind `Explain` → render trace as SystemTable),
guards, tests. **Non-scope:** executor logic, fragmenter, scheduler, SQL wire frame,
Wendelon, 129–133 strategy/trace logic (reused as-is).

## Pre-flight anchors (grounded)
| Concern | Anchor |
|---|---|
| trace source (133) | `bind_with_trace(...) -> (ExecutionPlan, OptimizerTrace)` `binder.rs:74` |
| recursive bind seam | `bind_plan_with_trace` (`binder.rs`) — add the `Explain` arm here |
| SQL lowering arm | `sql_planner.rs:1524 fn lower_statement` + `unsupported(...)`; sqlparser 0.62 `Statement::Explain { describe_alias, analyze, verbose, query_plan, estimate, statement, format, options }` |
| SystemTable render seam | `SystemTableKind` `ir/execution.rs:183`; `SystemTable { schema, rows }` materializes `exec.rs:667` |
| Rune dataframe + Module | `rune_engine.rs:171 RuneDataFrame { plan: LogicalPlan }`; `create_toron_module` assoc-fn registration; `execute_script` returns `df.plan` |
| ctx.sql relation gate (079) | `sql_planner.rs` `relation_plan_is_terminal` (retarget — keep DDL/DML only) |
| receipt fields | `DecisionReceipt { target.join_index, strategy, placement, reason }` |

## Phases
0. **Failing gates first:** `rune_explain_broadcast_join_yields_decision_rows` +
   `sql_explain_matches_rune_explain_rows` fail until lowering+render exist;
   rejection gates cover `ANALYZE`, `VERBOSE`, `QUERY PLAN`, `ESTIMATE`, `FORMAT`,
   `options`, `DESC`/`DESCRIBE`, `EXPLAIN TABLE`, nested EXPLAIN, and DML/DDL.
1. **IR:** add `LogicalPlan::Explain { input }` + `SystemTableKind::OptimizerExplain`
   (`--all-targets` catches every exhaustive-match site — binder recursion, executor
   unbound-plan errors, server labels, fragmenter/scheduler walkers, local test
   walkers; the intended backstop).
2. **SQL:** `lower_statement` `Statement::Explain` arm — accept default-over-`Query`
   only; reject `analyze`/`verbose`/`query_plan`/`estimate`/`format`/`options`/
   non-`Query`/`ExplainTable` with explicit `unsupported(...)`. Match inner `Query`
   directly (no recursive `lower_statement`).
3. **Rune:** `RuneDataFrame::explain(self) -> RuneDataFrame` wrapping `self.plan` in
   `LogicalPlan::Explain`; register via `m.associated_function("explain", …)` in
   `create_toron_module`. No new `RuneExplain` type; `execute_script` contract
   unchanged.
4. **Binder + render:** in `bind_plan_with_trace`, `LogicalPlan::Explain { input } =>`
   call `bind_with_trace(input, catalog, config)`, discard inner plan, build
   `SystemTable { kind: OptimizerExplain, schema: optimizer_explain_schema(),
   rows: render(trace) }`; `ScopedExecutionPlan::empty_scope`. Helpers:
   `optimizer_explain_schema()`, `optimizer_explain_rows(trace)`, label fns
   (`join_strategy_label`/`join_placement_label`/`join_decision_reason_label`/
   `hash_default_reason_label`/`broadcast_build_side_label`). Reason → `reason` +
   `reason_detail` per ADR D5; checked `usize/u64 → i64`. Do **not** add qualifier
   columns or change `JoinTraceTarget`; human-readable join labels are a follow-on.
   No executor arm for `Explain`.
5. **Read-only relation:** keep `relation_plan_is_terminal` DDL/DML-only; do NOT add
   `Explain`. Update local terminal-variant enumerations
   (`tests/rune_relation_handles_test.rs::is_terminal_plan`,
   `tests/architecture/.../adr079_ctx_sql_rejects_ddl_and_dml`).
6. **Guards + tests:** the `adr134_*` guards + behavior (incl. the SQL↔Rune parity
   gate + read-only-ctx.sql + bind-not-execute scan-count + default-only SQL
   rejection coverage + index-only target guard) + retarget
   `adr133_no_public_explain_surface` and the 079 terminal guard. Re-run 129–133.
7. **Verify:** `cargo fmt -p toron` · `cargo test -p toron` · `--test
   architecture_test -- adr129 adr130 adr131 adr132 adr133 adr134` · `cargo check -p
   toron --all-targets` ([[feedback_verify_all_targets_on_new_ir_field]]) · `git diff
   --check`.

## Risk register
- **New IR variants → exhaustive-match breakage**; `--all-targets` backstop. EXPLAIN
  binds to SystemTable; no executor arm for `Explain`.
- **Rune Module registration:** `.explain()` must be VM-callable (registered on the
  `rune::Module`), not just a Rust method — else not script-reachable. Pin with
  `rune_explain_broadcast_join_yields_decision_rows`.
- **SQL↔Rune divergence:** shared node makes them identical by construction; parity
  gate is the backstop.
- **Reason-on-wire:** only rendered scalar rows cross the wire; 133 no-wire guard +
  `adr134_reason_types_stay_crate_private_and_off_wire`.
- **ctx.sql over-/under-rejection:** EXPLAIN read-only allowed, mutation EXPLAIN
  rejected; retarget 079 explicitly (discipline #4), don't delete.
- **Scope creep:** no ANALYZE/format/verbose/query_plan/estimate/options/full-plan/
  DML — explicit rejects + guard.
- **Schema drift:** explicit label helpers, not `Debug`; `explain_result_schema_is_stable`.
- **Trace target scope creep:** keep `JoinTraceTarget { join_index }`; defer
  qualifier / human-label semantics.

## Acceptance
Rune `df.explain()` and SQL `EXPLAIN SELECT` → row-identical deterministic per-join
decision rows (full 129–132 reason incl. `rejected_side_bytes`, `reason`/`reason_detail`
split) via the shared node + SystemTable; inner query bound but never executed; stable
schema; ANALYZE/non-default forms/DML rejected; `ctx.sql` allows read-only EXPLAIN, rejects
mutations; reason types `pub(crate)` off wire; `JoinTraceTarget` remains
index-only; `adr134_*` + retargeted 079/133 + 129–133 guards green.
