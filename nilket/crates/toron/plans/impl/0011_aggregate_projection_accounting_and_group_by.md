# Implementation Plan: ADR 011 — Aggregate Projection Accounting and GROUP BY SQL Reachability

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/0011_aggregate_projection_accounting_and_group_by.md`
**Bench-results artifact:** none — ADR 011 introduces no operator
and makes no performance claim. Promotion evidence is plan-shape
inspection, result/digest parity against hand-built `Aggregate`
plans, distributed stage-shape inspection, and preserved
architecture guards (see "Definition of done").

## Status

Promoted to `plans/impl/` and implemented.

Built across five iteration rounds with the AI Swarm Council
(`plans/wip/{claude,codex,gemini}_iter_*`). Three-way final
convergence reached; Codex iter-5 and Gemini iter-5 ratified the
frozen contract, primary draft handed to Claude.

## Substrate

- `crates/toron/src/sql_planner.rs` — `SqlPlanner::lower_select`
  plus one `SqlPlanner::lower_query` composition guard.
  Today: destructures `Select` exhaustively (ADR 010);
  loud-rejects `GROUP BY` (`if !group_by_is_empty(group_by)`),
  `HAVING`, `DISTINCT`; detects aggregate functions
  (`COUNT` / `SUM` / `LINEAR_REGRESS` / `APPROX_QUANTILE`) in the
  projection scan and returns `LogicalPlan::Aggregate { group_expr:
  vec![], aggr_expr }` — **silently dropping every non-aggregate
  projection item** (the integrity defect). ADR 011 production
  code lands here: mostly in `lower_select`, with `lower_query`
  rejecting `GROUP BY ... ORDER BY` before ADR 010's `Sort`
  wrapper is built. `group_by_is_empty` helper and
  `DEFAULT_JOIN_PARTITION_COUNT` already present.
- `crates/toron/src/ir.rs` — `LogicalPlan::Aggregate { input,
  group_expr: Vec<Expr>, aggr_expr: Vec<(AggregateFunction,
  Vec<Expr>)> }` and `AggregateFunction` (`CountAll`, `Count`,
  `Sum`, `LinearRegress`, `ApproxQuantile(f64)`). **Already
  exists — `LogicalPlan` stays 9 variants. No IR change.**
- `crates/toron/src/ir/execution.rs` — `ExecutionPlan::Aggregate
  { input, group_expr, aggr_expr: Vec<AggregateCall>,
  output_schema }`. **Already exists — `ExecutionPlan` stays 7
  variants. No IR change.**
- `crates/toron/src/binder.rs` — already binds
  `LogicalPlan::Aggregate` → `ExecutionPlan::Aggregate`:
  `bind_aggregate_call`, `aggregate_schema` (`[group_expr fields]
  ++ [aggr_expr fields]`), `aggregate_field`. The
  experimental-kernel binder gate lives here. **Used unchanged;
  audited in Phase 0.**
- `crates/toron/src/exec.rs` — `bound_grouped_aggregate` and
  `bound_global_aggregate` already implement grouped and global
  aggregation, exercised by hand-built plans and a Wendelon
  storage-parity test. **Used unchanged.**
- `crates/toron/src/distributed/fragment.rs` — `fragment_plan`'s
  non-join single-exchange path; `count_exchanges` /
  `find_exchange` recurse through `ExecutionPlan::Aggregate` as a
  pass-through; `MultipleExchangeBoundaries` rejects >1 non-join
  exchange. **Used unchanged — no fragmenter code in ADR 011.**
- `crates/toron/tests/architecture_test.rs` — the IR
  variant-count guards (`LogicalPlan` 9, `ExecutionPlan` 7), the
  ADR 010 `lower_query_and_lower_select_have_no_rest_pattern`
  source guard, the `JoinKind` 8 / `JoinStrategy` 2 /
  `TaskFailureKind` 7 guards, `enum_variant_count` helper.
  **Asserted green; not updated.**
- `crates/toron/tests/aggregate_projection_accounting_test.rs` —
  new; the no-silent-drop rejection table tests.
- `crates/toron/tests/group_by_test.rs` — new; public SQL /
  end-to-end grouped-aggregate + projection-shape + guard gates.

## Goal

Ship the ADR 011 contract: (1) `lower_select` classifies every
item of an aggregate projection — group key, stable-set aggregate
call, or `FrontendError::Unsupported` — with no item silently
dropped (field-item-level accounting, the ADR 010 invariant
pushed one level down); (2) a narrow top-level `GROUP BY` over a
single base table lowers to the existing `LogicalPlan::Aggregate`
over an explicit group-keyed `LogicalPlan::Exchange`, with an
exact `[all keys in order] ++ [aggregates]` projection shape, at
least one aggregate call, simple unqualified group keys, and
syntactic + structural single-table guards. No new IR variant,
wire, `TaskFailureKind`, `BudgetDimension`, architecture guard,
or Wendelon surface. ADR 008 / 009 / 010 behavior byte-identical
for every query that does not contain a now-supported `GROUP BY`
and did not previously rely on a now-loud-rejected aggregate
projection being silently narrowed.

## Implementation cautions (guardrails, not scope)

- **C1. Classify, don't recognize-and-ignore.** The aggregate
  projection scan must classify *every* item; the
  `recognize-some-aggregates, return Aggregate` shortcut is
  removed.
- **C2. Exact projection shape; no terminal `Project`.** The
  first `n` projection items equal the `GROUP BY` keys position
  for position; the rest are aggregate calls. Never inject a
  post-aggregate `Project`, never accept a key subset or
  reorder.
- **C3. Both single-table guards.** Syntactic
  (`from.len() == 1`, no `joins`) **and** structural (no
  `LogicalPlan::Join` below the aggregate input after `WHERE`
  lowering). The structural guard is load-bearing for ADR 009
  `WHERE EXISTS` decorrelation.
- **C4. Planner emits the group-keyed `Exchange`.** Explicitly,
  matching SQL join lowering. The fragmenter never synthesizes
  it.
- **C5. No two-phase aggregation, no partial-state semantics.**
  One raw-row group-keyed exchange; the existing final grouped
  aggregate runs over complete groups.
- **C6. No fragmenter code.** The grouped plan is a single
  non-join exchange boundary the existing `fragment_plan` path
  already handles. ADR 011 touches `fragment.rs` not at all;
  Phase 3 only inspects its output.
- **C7. No IR / guard surface.** `Aggregate` already exists in
  both IRs. Do not add an IR variant; do not add or update an
  architecture guard.
- **C8. Byte-identity first.** The projection-accounting refactor
  regression gate runs before any `GROUP BY` rule lands; accepted
  global aggregates stay byte-identical.
- **C9. Loud-reject the compositions.** `GROUP BY` over a join,
  `GROUP BY` over decorrelated `EXISTS`, and `GROUP BY ... ORDER
  BY` reject in the planner — never grow the fragmenter to absorb
  them.

## Phases

### Phase 0 — Audit

Locked decisions (recorded here, not open questions):

- **P0.A: no IR variant, no architecture-guard change.**
  `LogicalPlan` stays 9, `ExecutionPlan` stays 7; ADR 011 asserts
  the existing guard set green.
- **P0.B: exact projection shape** — `[every GROUP BY key, in
  GROUP BY order] ++ [aggregate calls, in projection order]`;
  subset / reorder / aggregate-first / alias reject.
- **P0.C: at least one aggregate call required** — aggregate-
  empty `GROUP BY` rejects.
- **P0.D: single-table input via syntactic + structural guards**
  — both required; the structural guard is not optional.
- **P0.E: no two-phase aggregation** — one raw-row group-keyed
  exchange.

True audit details (resolved in Phase 0, then a `// P0.x:`
comment each):

- **P0.1:** exact `sqlparser` `GroupByExpr` shape — confirm the
  `GroupByExpr::Expressions(exprs, modifiers)` arm carries the
  group-key expressions and that modifiers (`ROLLUP`, `CUBE`,
  `GROUPING SETS`) are present-and-rejectable; `GroupByExpr::All`
  rejects.
- **P0.2:** exact `SelectItem` AST shapes for the
  exact-projection-shape check — `UnnamedExpr(Identifier)` for a
  group key, `UnnamedExpr(Function)` for an aggregate call,
  `ExprWithAlias` / `Wildcard` / `QualifiedWildcard` reject;
  qualified `CompoundIdentifier` group keys reject.
- **P0.3:** `DEFAULT_AGGREGATE_PARTITION_COUNT` — reuse
  `DEFAULT_JOIN_PARTITION_COUNT` or introduce a named aggregate
  constant; implementer discretion, pinned with a `// P0.3:`
  comment.
- **P0.4:** confirm the SQL path for `LINEAR_REGRESS` /
  `APPROX_QUANTILE` already respects the existing
  `allow_experimental_kernels` binder gate. If it does not, that
  adjacent gap is flagged in the impl plan and surfaced to the
  council — **not** fixed inside ADR 011 unless trivially
  contained.
- **P0.5:** confirm existing distributed global-aggregate
  behavior is correct as-is. The Phase 1 / Phase 4 byte-identity
  gates protect it; if a correctness issue surfaces it is raised
  to the council, not absorbed as silent scope.
- **P0.6:** the structural-guard walk — confirm a small
  `plan_contains_join(&LogicalPlan) -> bool` recursive check is
  sufficient and that ADR 009 decorrelation is the only path that
  introduces a `Join` below a syntactically single-table
  `lower_select`.

Phase 0 produces the `// P0.x:` comments, then a single commit
`plan: adr 011 aggregate projection accounting and group by`.

### Phase 1 — Aggregate projection-item accounting (the integrity fix, lands first)

Files:
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. Aggregate-projection classifier.** Replace the
  recognize-some-ignore-the-rest projection scan with one
  exhaustive pass. Detect whether the projection is an *aggregate
  projection* (contains ≥1 recognized aggregate function); if so,
  classify every item: bare unqualified column reference
  (candidate group key — in Phase 1, with no `GROUP BY` present,
  a bare column in an aggregate projection **rejects**),
  recognized aggregate call, or `FrontendError::Unsupported`
  (wildcard, computed non-aggregate expression, unrecognized /
  unsupported function, aliased item, qualified column, malformed
  aggregate arity / modifier).
- **B. Aggregate-call lowering.** Keep `COUNT(*)`,
  `COUNT(expr)`, `SUM(expr)` lowering as today. Preserve existing
  global-aggregate SQL behavior for experimental kernels
  (`LINEAR_REGRESS`, `APPROX_QUANTILE`) behind the existing
  binder gate (P0.4), while Phase 2 keeps grouped SQL limited to
  the stable set. A malformed call (wrong arity, unsupported
  modifier) rejects cleanly — no panic, no silent narrowing.
- **C. Global-aggregate preservation.** A pure aggregate
  projection with no group-key items and no `GROUP BY` continues
  to lower to `LogicalPlan::Aggregate { group_expr: vec![],
  aggr_expr }` exactly as today. This phase changes which
  projections are *rejected*, not how an accepted global
  aggregate lowers.
- **D. Byte-identity check (FIRST).** Every existing SQL test
  runs unchanged; a moved digest on an accepted global aggregate
  reverts the refactor.

Tests (`tests/aggregate_projection_accounting_test.rs`):
- `existing_global_aggregates_byte_identical_after_accounting` —
  **Hard gate 5**, runs first.
- `mixed_aggregate_and_non_aggregate_projection_without_group_by_rejects`
  — **Hard gate 1.**
- `wildcard_mixed_with_aggregate_rejects` — **Hard gate 2.**
- `unsupported_projection_function_mixed_with_aggregate_rejects`
  — **Hard gate 3.**
- `malformed_aggregate_arity_or_modifiers_reject_cleanly` —
  **Hard gate 4.**

Phase 1 commits as a self-contained correctness fix —
`feat: adr 011 phase 1 aggregate projection-item accounting`.
Promotable on its own value before `GROUP BY` widens the accepted
surface (ADR 010 Phase 1 posture).

### Phase 2 — `GROUP BY` lowering + projection shape + single-table guards

Files:
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. `GROUP BY` lowering.** Replace the `if
  !group_by_is_empty(group_by) { reject }` arm with lowering.
  When `group_by` is non-empty:
  1. **syntactic guard** — `from.len() == 1` and
     `from[0].joins.is_empty()`; else `FrontendError::Unsupported`;
  2. validate the `GROUP BY` key list — simple unqualified
     column names only; `GroupByExpr::All`, modifiers
     (`ROLLUP` / `CUBE` / `GROUPING SETS`), computed expressions,
     qualified names, and ordinals reject (P0.1);
  3. lower `FROM` + optional `WHERE` as today;
  4. **structural guard** — `plan_contains_join(&input)` is
     `false`; else `FrontendError::Unsupported` (P0.6);
  5. classify the projection (Phase 1A) and validate the **exact
     shape** (work item B);
  6. build `LogicalPlan::Aggregate { input:
     Box::new(LogicalPlan::Exchange { input, partition_count:
     DEFAULT_AGGREGATE_PARTITION_COUNT, key: group_expr.clone()
     }), group_expr, aggr_expr }`.
- **B. Exact-projection-shape validation.** The first `n`
  projection items (`n` = `GROUP BY` arity) must be bare column
  references equal, position for position, to the `GROUP BY`
  keys; every remaining item must be a stable-set aggregate call;
  at least one aggregate call is required (P0.C). Subset,
  reorder, aggregate-before-key, alias, and aggregate-empty all
  reject.
- **C. Helper:** `plan_contains_join(&LogicalPlan) -> bool` — a
  small recursive walk; placed near the existing `lower_*`
  helpers.
- **D. `GROUP BY ... ORDER BY` guard in `lower_query`.** When
  `query.order_by` is present, reject if the plan returned by
  `lower_select` is the ADR 011 grouped-aggregate shape before
  ADR 010 wraps it in `LogicalPlan::Sort`. `ORDER BY` alone and
  global aggregate `ORDER BY` keep their ADR 010 behavior; only
  grouped aggregate composition rejects.

Tests (`tests/group_by_test.rs`):
- `group_by_lowers_to_aggregate_over_group_keyed_exchange` —
  **Hard gate 6.**
- `group_by_requires_at_least_one_aggregate_call` — **Hard
  gate 7.**
- `group_by_projection_must_be_exact_full_key_list_then_aggregates`
  — **Hard gate 8.**
- `group_by_projection_subset_reorder_alias_or_aggregate_first_rejects`
  — **Hard gate 9.**
- `group_by_accepts_only_stable_aggregate_set` — **Hard gate 10.**
- `group_by_keys_must_be_simple_unqualified_input_columns` —
  **Hard gate 11.**
- `group_by_over_explicit_join_rejects` — **Hard gate 12.**
- `group_by_over_decorrelated_exists_rejects` — **Hard gate 13.**
- `group_by_with_order_by_rejects` — **Hard gate 17.**
- `having_is_unsupported_not_dropped` — **Hard gate 18.**
- `distinct_is_unsupported_not_dropped` — **Hard gate 19.**

Gate: `cargo check --workspace` + `cargo test -p toron --test
group_by_test`.

### Phase 3 — Distributed parity + stage shape

Files:
- `crates/toron/tests/group_by_test.rs`
- `crates/toron/tests/distributed_process_test.rs`

No production code — Phase 3 asserts the existing binder /
executor / fragmenter machinery carries the ADR 011 plan shape
correctly.

Work items:

- **A. Single-process parity.** Grouped SQL executes and matches
  an equivalent hand-built `LogicalPlan::Aggregate` for
  `COUNT(*)`, `COUNT(expr)`, `SUM(expr)`, including null
  handling.
- **B. Stage-shape inspection.** `fragment_plan` on the bound
  grouped plan produces exactly one producer stage (the
  group-keyed `Exchange`) and one consumer stage of N tasks
  carrying `ExecutionPlan::Aggregate`, output `FinalResult`; no
  new exchange mode, no `MultipleExchangeBoundaries`.
- **C. Distributed parity.** Grouped SQL output matches between
  single-process and distributed execution.

Tests:
- `grouped_aggregate_sql_matches_hand_built_aggregate_plan` —
  **Hard gate 15.**
- `grouped_aggregate_matches_single_process_and_distributed` —
  **Hard gate 16.**
- `distributed_group_by_has_single_group_keyed_exchange`
  (`distributed_process_test.rs`) — **Hard gate 14.**

Gate: `cargo test -p toron --test group_by_test --test
distributed_process_test`.

### Phase 4 — Architecture guards + verification + promotion

Files:
- `crates/toron/tests/architecture_test.rs` (assertion only — no
  guard added or changed)
- `crates/toron/plans/adr/0011_aggregate_projection_accounting_and_group_by.md`
- `crates/toron/plans/impl/0011_aggregate_projection_accounting_and_group_by.md`
- `crates/toron/plans/wip/roadmap.md`

Work items:

- **A. Architecture-guard assertion.** Confirm `LogicalPlan`
  9-variant and `ExecutionPlan` 7-variant guards stay green
  **without update**; `Join` single-variant, `JoinKind` 8,
  `JoinStrategy` 2, `TaskFailureKind` 7, `BudgetDimension` 7, and
  the ADR 010 `lower_query_and_lower_select_have_no_rest_pattern`
  source guard all stay green. **Hard gate 21.** No new guard
  added.
- **B. Full-surface regression.**
  `existing_sql_queries_byte_identical_after_adr_011` — **Hard
  gate 20**, runs first in the regression set.
- **C. Verification + promotion.** Run the full verification
  block. ADR 011 produces no bench-results artifact.

Gate: full verification block + `cargo test --workspace
--no-fail-fast`.

## Verification commands at promotion

```bash
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron --no-fail-fast
cargo test -p toron --test architecture_test --no-fail-fast
cargo test -p toron --test aggregate_projection_accounting_test --no-fail-fast
cargo test -p toron --test group_by_test --no-fail-fast
cargo test -p toron --test distributed_process_test --no-fail-fast
cargo test --workspace --no-fail-fast
git diff --check
```

## Risks + mitigations

- **R1: the projection-scan refactor regresses an accepted
  global aggregate.**
  *Mitigation:* C8 + P0.5; gate 5 runs first in Phase 1; a moved
  digest reverts the refactor.

- **R2: a silently-narrowed aggregate projection was
  load-bearing for a query that "works" today.**
  *Mitigation:* intended, not a regression — such a query already
  returns wrong results (a global sum where a per-group
  breakdown was asked for). Phase 1 ships as a self-contained
  correctness commit.

- **R3: the structural single-table guard misses an ADR 009
  decorrelated `EXISTS`.** A `GROUP BY` over a buried `LeftSemi`
  join produces partition-local grouped results.
  *Mitigation:* C3 + P0.6; gate 13 pins
  `SELECT ... WHERE EXISTS (...) GROUP BY ...` rejection
  explicitly; the structural guard walks the lowered plan, not
  the SQL AST.

- **R4: the exact-projection-shape check is too loose and admits
  a subset / reorder that needs a hidden `Project`.**
  *Mitigation:* C2; gates 8 and 9 pin the exact shape and the
  subset / reorder / alias / aggregate-first rejections.

- **R5: scope creep toward two-phase aggregation.** The
  `Aggregate(Exchange(Aggregate(Scan)))` shape is tempting for
  shuffle-byte reduction.
  *Mitigation:* C5; ADR §6 and §"Rejected alternatives" are
  explicit; no partial/final aggregate-state types are
  introduced; gate 14 asserts exactly one exchange.

- **R6: scope creep toward `GROUP BY ... ORDER BY` or `GROUP BY`
  over joins by growing the fragmenter.**
  *Mitigation:* C9; gates 12, 13, 17 pin the planner-level
  rejections; ADR 011 touches `fragment.rs` not at all.

- **R7: an IR variant or architecture guard is added "for
  cleanliness."**
  *Mitigation:* C7 — `Aggregate` already exists in both IRs;
  gate 21 asserts the variant-count guards stay green *without
  update*.

- **R8: experimental kernels accidentally graduated through the
  SQL path.**
  *Mitigation:* P0.4 audits the `allow_experimental_kernels`
  gate; gate 10 pins the stable accepted set; experimental
  lowering routes through unchanged.

- **R9: `sqlparser` `GroupByExpr` / `SelectItem` shapes differ
  from the P0.1 / P0.2 audit assumptions.**
  *Mitigation:* Phase 0 audits the exact AST shapes before code;
  the contract does not move — only the matching code adjusts.

## Definition of done

- Phase 0 audit complete: locked decisions confirmed, true audit
  details recorded as `// P0.x:` comments, plus the planning
  commit. P0.4 / P0.5 findings surfaced to the council if they
  expose adjacent gaps.
- Phases 1–4 complete: hard gates 1–21 passing.
- Phase 1 is independently reviewable as a self-contained
  correctness commit (aggregate projection-item silent drop →
  loud reject).
- Promotion evidence: plan-shape inspection (gate 6),
  result/digest parity against hand-built `Aggregate` plans
  (gates 15, 16), distributed stage-shape inspection (gate 14),
  and preserved architecture guards (gate 21). No bench-results
  artifact.
- All verification commands above green.
- Documentation: ADR 011 and impl plan land at promotion.
- Roadmap (`plans/wip/roadmap.md`) updated: ADR 011 marked
  Promoted + implemented; the post-ADR-009 A–E candidate list
  annotated as again superseded by code evidence (a second
  silent-wrong-results defect, one level below ADR 010); ADR 012
  candidates reframed with the ADR 006 Wendelon production row as
  the parallel-track input and the named ADR 011 follow-ons
  (`GROUP BY` over joins, `GROUP BY ... ORDER BY`, `HAVING`,
  `DISTINCT`, post-aggregate projection, two-phase partial
  aggregation) recorded.

## Files touched (expected)

- `crates/toron/src/sql_planner.rs` — aggregate-projection
  classifier, exact-projection-shape validation, `GROUP BY`
  lowering with the explicit group-keyed `Exchange`, syntactic +
  structural single-table guards, `plan_contains_join` helper.
- `crates/toron/tests/aggregate_projection_accounting_test.rs` —
  new; no-silent-drop rejection table tests.
- `crates/toron/tests/group_by_test.rs` — new; `GROUP BY`
  lowering, projection shape, guard, and single-process parity
  gates.
- `crates/toron/tests/distributed_process_test.rs` — distributed
  grouped-aggregate stage-shape + parity gates.
- `crates/toron/tests/architecture_test.rs` — assertion only:
  confirm the existing guard set stays green; no guard added or
  changed.
- `crates/toron/plans/adr/011_*.md`,
  `crates/toron/plans/impl/011_*.md`,
  `crates/toron/plans/wip/roadmap.md`.

## Files not expected to change

- `crates/toron/src/ir.rs`, `crates/toron/src/ir/execution.rs` —
  `LogicalPlan::Aggregate` / `ExecutionPlan::Aggregate` already
  exist; no IR variant added.
- `crates/toron/src/binder.rs` — the aggregate binding path is
  used unchanged (Phase 0 audits the experimental-kernel gate but
  does not modify it absent a P0.4 finding).
- `crates/toron/src/exec.rs` — `bound_grouped_aggregate` /
  `bound_global_aggregate` used unchanged.
- `crates/toron/src/distributed/fragment.rs` — the existing
  one-exchange non-join path carries the grouped plan; ADR 011
  adds no fragmenter code.
- `crates/toron/src/wire.rs`, `crates/toron/src/distributed/task.rs`,
  `crates/toron/src/error.rs`, `crates/toron/src/error/distributed.rs`,
  `crates/toron/src/distributed/budget.rs` — no new wire,
  `MessageKind`, `TaskFailureKind`, `ExecutionError`, or
  `BudgetDimension`.
- `crates/toron/Cargo.toml` and the workspace `Cargo.toml` — no
  new dependency.
- `crates/toron/benches/*` — no new operator, no perf claim.
- Wendelon crates.

## Promotion / fallback

Promote if all 21 hard gates pass and the full verification block
is green.

If `sqlparser`'s `GroupByExpr` / `SelectItem` shapes differ from
the P0.1 / P0.2 audit assumptions, adjust the AST-matching code;
the contract does not move.

If P0.4 shows the SQL path for `LINEAR_REGRESS` /
`APPROX_QUANTILE` does **not** respect the
`allow_experimental_kernels` gate, surface it to the council as
an adjacent gap. Default posture: flag it, do not fold the fix
into ADR 011 unless it is a one-line gate check trivially
contained — ADR 011's contract is projection accounting +
`GROUP BY` reachability, not experimental-kernel gating.

If P0.5 shows existing distributed global-aggregate behavior is
incorrect, surface it to the council. It is not ADR 011 scope to
fix by default — but the gate-5 / gate-20 byte-identity gates
must not be weakened to paper over it; a real defect is its own
narrow correctness commit.

If pressure for an out-of-scope shape (`GROUP BY` over joins,
`GROUP BY ... ORDER BY`, `HAVING`, `DISTINCT`, post-aggregate
projection, two-phase partial aggregation, new aggregate
functions) appears during review, hold the boundary. Each is a
named, deferred follow-on; ADR 011 ships aggregate projection
accounting plus the narrow single-table `GROUP BY` surface.
