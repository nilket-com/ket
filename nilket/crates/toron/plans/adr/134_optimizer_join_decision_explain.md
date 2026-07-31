# ADR 134: Optimizer Join-Decision EXPLAIN (SQL + Rune)

**Status:** Promoted. **Genuine 3-of-3 (Claude, Codex, Gemini); operator-steered
(Rune-primary).** Exposes ADR 133's `OptimizerTrace` as a read-only diagnostic
relation through **both** Toron frontends — **Rune `RuneDataFrame::explain()`
(primary)** and SQL `EXPLAIN <query>` (compatibility) — both lowering to one shared
`LogicalPlan::Explain { input }`, bound via `bind_with_trace`, rendered as an
`ExecutionPlan::SystemTable`. Completes the observability arc: 133 made CBO
decisions visible to *tests*; 134 makes them visible to *authors* of both
frontends. Frontend + render only; binds the inner plan but never executes it; no
new strategy logic, no substrate, no SQL wire message.

**Category:** Toron interior — optimizer observability surface (both frontends).
**Predecessors:** 129–132 (the decisions), 133 (`bind_with_trace -> (ExecutionPlan,
OptimizerTrace)`), 125 (Rune authoring), 128 (SQL hint authoring), 079 (`ctx.sql`
terminal-statement gate, retargeted here).
**Migration:** `plans/impl/134_optimizer_join_decision_explain.md`.

## Doctrine — the prescriptive author sees the optimizer's reasoning, Rune-first
> Toron's audience is the power user who *prescribes* plans (hand-tuned Rune joins),
> not just hints them; SQL is a compatibility surface, not the design center. Such
> an author must be able to ask "what did the optimizer decide about my
> prescription?" without leaving Rune — and a Rune **script** in the `RuneEngine`
> VM cannot reach the crate-private `bind_with_trace`, so `df.explain()` is the
> *only* way to inspect a Rune-authored plan. EXPLAIN is therefore **Rune-first**:
> `df.explain()` is primary; SQL `EXPLAIN` mirrors it. Both lower to one
> `LogicalPlan::Explain` node so the render is shared and the two surfaces are
> provably identical. EXPLAIN is a **read-only diagnostic relation** — it binds the
> inner plan to populate the 133 trace and renders the receipts as rows, never
> executing the inner query. Bounded: join-strategy decisions only — not a
> plan-tree/cost renderer, not `ANALYZE`.

## Decisions

**D1 — One shared IR node.** `LogicalPlan::Explain { input: Box<LogicalPlan> }`,
frontend-agnostic. Both frontends lower to it; only the entry point differs — which
pins SQL↔Rune parity by construction.

**D2 — Rune surface (primary): `RuneDataFrame::explain()`.** Add
`fn explain(self) -> RuneDataFrame { Self { plan: LogicalPlan::Explain { input:
Box::new(self.plan) } } }` and **register it on the Rune `Module`** (script-callable,
not just a Rust method). Returns an ordinary dataframe of explanation rows — no new
typed trace object, no change to the `execute_script` return contract. Example:
`ctx.table("dim").broadcast_join(ctx.table("fact"), on).explain()`.

**D3 — SQL surface (compatibility): `EXPLAIN <SELECT>`.** Add a `Statement::Explain`
arm to `lower_statement` (`sql_planner.rs:1524`) accepting only the default form
(`describe_alias` plain `EXPLAIN`; `analyze`/`verbose`/`query_plan`/`estimate` all
false; `format`/`options` none) over an inner `Statement::Query` → `LogicalPlan::Explain`.
Reject `DESC`/`DESCRIBE` aliases, `ANALYZE`, format/verbose/query_plan/estimate/
options, `EXPLAIN TABLE`, nested EXPLAIN, and DML/DDL EXPLAIN with explicit
`unsupported(...)`. (Match the inner `Query` directly; do not recurse
`lower_statement`.)

**D4 — Bind, don't execute; render via SystemTable.** Binding `Explain { input }`:
`bind_with_trace(input)` → discard the bound inner `ExecutionPlan` → build
`ExecutionPlan::SystemTable { kind: SystemTableKind::OptimizerExplain, schema, rows }`.
Existing `exec.rs:667` SystemTable execution + `ResultBatch`/`Complete` carry the
rows — **no new SQL wire message**. No executor arm for `Explain` itself (it lowers
to an already-executing SystemTable). Render labels via explicit match helpers, not
`Debug`.

**D5 — Stable structured schema** (one row per `DecisionReceipt`, in `join_index`
order; checked `usize/u64 → i64`):

| column | type | notes |
|---|---|---|
| `join_index` | `Int64 not null` | deterministic 133 bind-order index |
| `strategy` | `Utf8 not null` | `hash_partitioned`, `broadcast` |
| `placement` | `Utf8 not null` | `natural`, `swapped_right_build` |
| `reason` | `Utf8 not null` | `pinned_by_author`, `broadcast_inferred`, `hash_by_default` |
| `reason_detail` | `Utf8 null` | hash subreason, e.g. `probe_not_scan_linear` |
| `build_side` | `Utf8 null` | `left`/`right` for inferred broadcast |
| `build_bytes` | `Int64 null` | inferred broadcast only |
| `threshold_bytes` | `Int64 null` | inferred broadcast only |
| `rejected_side_bytes` | `Int64 null` | both-eligible comparison (132) only |

Stable lowercase snake_case strings; never Rust enum `Debug`.

**D6 — EXPLAIN is a read-only relation, not a mutation terminal.** `ctx.sql("EXPLAIN
SELECT …")` is an allowed read-only dataframe; `ctx.sql("EXPLAIN INSERT …")` still
rejects at SQL lowering; native `df.explain()` is the preferred Rune path. The
invariant is "EXPLAIN binds but never executes the inner plan," NOT "EXPLAIN is
terminal." **Retarget** ADR 079's `ctx.sql` terminal guard / `relation_plan_is_terminal`
to stay DDL/DML/mutation-focused — do not add `Explain` to the terminal set, and do
not assert every non-`Query` relation is rejected.

**D7 — Trace/reason types stay crate-private; off wire.** `OptimizerTrace`,
`DecisionReceipt`, and the four reason enums remain `pub(crate)`, rendered to scalar
strings/ints only. **Retarget** ADR 133's `adr133_no_public_explain_surface` →
"public EXPLAIN renders strings/ints only; trace/reason Rust types stay internal,
off `ExecutionPlan`/wire." `SystemTableKind::OptimizerExplain` is a serialized
ExecutionPlan enum addition (not a new SQL wire frame, not a trace type on wire).

**D8 — Keep ADR 133's target identity index-only.** Do not add table qualifiers or
human-readable join labels to `JoinTraceTarget` in this ADR. Qualifiers are useful
later, but they need their own semantics for aliases, self-joins, derived inputs,
and multi-source sides. ADR 133 chose deterministic `join_index` because it is
self-join-safe; ADR 134 renders that stable target first.

## Behavior gates
- `rune_explain_broadcast_join_yields_decision_rows` — **primary**:
  `dim.broadcast_join(fact,on).explain()` → broadcast/placement/build_side/`rejected_side_bytes` row.
- `sql_explain_matches_rune_explain_rows` — **parity gate**: SQL `EXPLAIN SELECT …`
  and the equivalent Rune `.explain()` produce byte-identical rows (proves the shared node).
- `sql_explain_broadcast_inference_renders_receipt_rows`.
- `explain_probe_not_scan_linear_reports_hash_reason_detail` — hash → `reason=hash_by_default`,
  `reason_detail=probe_not_scan_linear`, byte/side cols null.
- `explain_pinned_broadcast_reports_pinned_by_author`.
- `explain_emits_one_row_per_join_in_index_order`.
- `explain_binds_but_does_not_execute_inner_query` — a query that would scan/fail at
  runtime returns rows with zero scans (both frontends).
- `explain_result_schema_is_stable` — exact columns/types (digest-pinned).
- `ctx_sql_explain_is_read_only_relation` — `ctx.sql("EXPLAIN SELECT")` accepted;
  `ctx.sql("EXPLAIN INSERT")` rejected.
- Rejection (SQL):
  `explain_rejects_analyze_verbose_query_plan_estimate_format_options` and
  `explain_rejects_describe_alias_explain_table_nested_and_dml`.

## Architecture guards
- `adr134_single_shared_explain_node` — both frontends route to `LogicalPlan::Explain`.
- `adr134_sql_lowers_explain_to_logical_node`.
- `adr134_rune_dataframe_explain_wraps_logical_node` (registered on the Module).
- `adr134_explain_binds_via_bind_with_trace_not_execute`.
- `adr134_explain_renders_via_systemtable_existing_result_path` (no new SQL wire frame).
- `adr134_sql_explain_accepts_only_default_query_form`.
- `adr134_schema_labels_are_stable_not_debug`.
- `adr134_join_trace_target_remains_index_only`.
- `adr134_explain_is_join_decision_only_not_plan_tree_or_analyze`.
- `adr134_reason_types_stay_crate_private_and_off_wire`.
- `adr134_ctx_sql_allows_readonly_explain_but_still_rejects_mutations` — retargets
  the ADR 079 terminal guard (not deleted).
- Retarget `adr133_no_public_explain_surface` (D7). 129–133 guards stay green.

## Non-goals
No `EXPLAIN ANALYZE` / runtime metrics / format options / verbose / `EXPLAIN TABLE` /
DML-DDL EXPLAIN. No full plan-tree or cost/cardinality rendering. No new SQL wire
message. No new typed public trace API. No human-readable join target labels. No new
strategy logic / substrate. No change to 133 trace contents. No public exposure of
`bind_with_trace` (separate decision).

## Council history
133 deferred the public surface. iter-0/1: Claude led EXPLAIN, scoped SQL-only
(deferring Rune on the 125/128 per-ADR precedent); Codex + Gemini included Rune.
**Operator steer: Rune is primary, SQL secondary.** Claude conceded SQL-only →
both frontends — the deferral rested on a false premise (`bind_with_trace` is
`pub(crate)`, unreachable from Rune scripts, so `.explain()` is the *only* Rune
inspection path). iter-2 converged 3-of-3 on the shared node + both surfaces;
adopted Codex's `reason`/`reason_detail` split and diagnostic-relation framing
(retract iter-0's "EXPLAIN is terminal" → "binds-but-never-executes"); resolved the
method name `.explain()` and column types. Grounding + steer aligned
([[feedback_human_steer_catches_the_wrong_cut]], [[rune_is_primary_interface]]).

## Follow-ons
`EXPLAIN ANALYZE` / fuller plan rendering (once runtime metrics exist) ·
Wendelon-backed live facts (flip dark-launch; EXPLAIN validates it) · recursive
fragmentation / multi-way · selectivity · typed programmatic trace API if Rust
callers need it.

## Promotion criteria
Both `df.explain()` (Rune) and `EXPLAIN SELECT` (SQL) return one deterministic
decision row per join with the full 129–132 reason incl. `rejected_side_bytes`, via
the shared node + SystemTable on the existing result path; the two surfaces are
row-identical; the inner query is bound but never executed; stable schema;
ANALYZE/non-default forms/DML rejected; `ctx.sql` allows read-only EXPLAIN but still rejects
mutations; reason types stay `pub(crate)` off wire; `JoinTraceTarget` remains
index-only; `adr134_*` + retargeted 079/133 + 129–133 guards green; `cargo test -p
toron`, `--test architecture_test -- adr129 adr130 adr131 adr132 adr133 adr134`,
`cargo check -p toron --all-targets`, `git diff --check` all green.
