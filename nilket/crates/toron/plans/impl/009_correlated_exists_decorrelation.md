# Implementation Plan: ADR 009 — Correlated `EXISTS` / `NOT EXISTS` Decorrelation

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/009_correlated_exists_decorrelation.md`
**Bench-results artifact:** none — ADR 009 introduces no new
operator path. Promotion evidence is digest parity against
hand-written `LeftSemi` / `LeftAnti` joins plus preserved
architecture guards (see "Definition of done").

## Status

Promoted to `plans/impl/` + implemented.

Built across three WIP iteration rounds with the AI Swarm Council
(`plans/wip/{claude,codex,gemini}_iter_*_adr_009.md`). Three-way
final convergence reached by iter-2.

## Substrate

- `crates/toron/src/sql_planner.rs` — `SqlPlanner::lower_select`,
  the outer `WHERE` (`select.selection`) lowering point, the
  existing `expr_side` qualifier classifier, and the existing
  unsupported-expression rejection path for `SqlExpr::Exists`. All
  ADR 009 production code lands here.
- `crates/toron/src/ir.rs` — `JoinKind::LeftSemi` /
  `JoinKind::LeftAnti`, `LogicalPlan::Join`, `LogicalPlan::Filter`,
  `LogicalPlan::Exchange`. Reused unchanged.
- `crates/toron/src/binder.rs` — `bind_join`, equi-key validation,
  float-key rejection, exchange-shape validation, non-Inner
  broadcast rejection. Semantic validation reused unchanged — the
  planner does not duplicate these checks. `collect_qualifiers`
  mirrors semi/anti output schema so chained decorrelations do not
  leak prior inner aliases into later join-key binding.
- `crates/toron/tests/architecture_test.rs` — the ADR 003 / 004 /
  008 guard set. Stays green; no new guard added.
- `crates/toron/tests/exists_decorrelation_test.rs` — new test
  file, home for the public SQL / end-to-end hard gates. The
  private two-scope classifier is unit-tested in-module under
  `#[cfg(test)]` in `src/sql_planner.rs` (it is not reachable
  from an integration test).

## Goal

Ship the correlated `EXISTS` / `NOT EXISTS` decorrelation contract
from ADR 009: a narrow, safe SQL-AST rewrite in
`SqlPlanner::lower_select` that lowers single-level, single-table
correlated existence predicates to ADR 008 `LeftSemi` / `LeftAnti`
joins, with every unsupported shape continuing to reject through
the existing path. No new IR, executor, wire, Wendelon, or
architecture surface. M7 / ADR 008 behavior byte-identical for
every query that does not contain a now-supported `EXISTS`.

## Implementation cautions (Codex iter-2 — guardrails, not scope)

- **C1. Classify on the SQL AST before lowering to Toron `Expr`.**
  The two-scope classifier inspects `sqlparser::ast::Expr`
  directly; only accepted `CorrelatedPair` columns and
  `InnerResidual` predicates are lowered. Lowering the whole
  subquery predicate through `lower_expr` first loses the chance
  to reject unsafe outer-reference shapes precisely.
- **C2. Treat classifier ambiguity as unsupported, not planner
  guesswork.** If a column resolves to both scopes, neither
  scope, or multiple qualifiers the current planner cannot prove,
  reject the rewrite. "Too narrow and correct" over matching a
  permissive engine.
- **C3. Keep projection irrelevance visible in tests.** Prove the
  subquery select list is ignored for `SELECT 1`, `SELECT *`, and
  `SELECT inner.k`, so a future refactor cannot accidentally bind
  the projection and reject valid existence checks.
- **C4. Preserve binder ownership.** The planner classifies shape
  and builds the logical rewrite; the binder stays the authority
  for type compatibility, float-key rejection, computed-key
  rejection, exchange shape, and non-Inner broadcast rejection.

## Phases

### Phase 0 — Audit

Seven sub-pins, each producing a concrete decision recorded in
this doc before code lands:

- **P0.1: `sqlparser` 0.54 `SqlExpr::Exists` shape.** Confirm
  `SqlExpr::Exists { subquery, negated }` — `negated` is the
  `NOT EXISTS` discriminator, `subquery` is a `Box<Query>`.
- **P0.2: supported subquery body.** Confirm the supported body
  matches `SetExpr::Select` with a single-element `from` whose
  `relation` is `TableFactor::Table` (+ optional alias);
  enumerate the AST shapes that route to rejection.
- **P0.3: outer-scope visibility.** Confirm the outer `FROM`
  scope is fully resolvable at the point `select.selection` is
  lowered, so the two-scope classifier has both qualifier sets
  available.
- **P0.4: rejection-path error class.** Confirm the existing
  unsupported-expression error class carries the rejection for
  every out-of-scope shape; no new variant (ADR 008 P0.8
  discipline).
- **P0.5: two-scope classifier helper shape.** Pin the helper
  signature and return type — `CorrelatedPair` / `InnerResidual`
  / `Unsupported` — and confirm it is unit-testable independent
  of full SQL planning. Starts private in `sql_planner.rs`;
  promoted to a helper module only if the file becomes hard to
  read.
- **P0.6: subquery projection irrelevance.** Confirm the
  right-side plan is built from the subquery relation + optional
  `InnerResidual` filter + correlation keys, with the subquery
  select list never lowered or bound.
- **P0.7: golden fixture matrix.** Enumerate the fixtures used
  across Phases 2-4: correlated `EXISTS` / `NOT EXISTS`,
  with/without inner residual, with/without outer residual,
  duplicate inner matches, `NULL` correlation key, multiple
  conjuncts (two source orders), composition over an explicit
  outer-`FROM` join, and one fixture per out-of-scope shape
  proving rejection. Expected row counts tabulated.

// P0.1: Local `sqlparser` 0.54 exposes correlated existence
// predicates as `SqlExpr::Exists { subquery, negated }`; `negated`
// is the `NOT EXISTS` discriminator and `subquery` is the boxed
// `Query` lowered by `SqlPlanner::lower_exists_join`.
// P0.2: The supported subquery body is `SetExpr::Select` with one
// `from` item; `validate_exists_select_shape` rejects joins and
// SELECT-level clauses such as `DISTINCT`, `GROUP BY`, `HAVING`,
// `LIMIT`, `ORDER BY`, and `QUALIFY`. The relation is lowered
// through `lower_table_factor`, so V1 admits only
// `TableFactor::Table` plus optional alias.
// P0.3: Outer scope is available after the outer `FROM` and
// explicit joins are lowered; `lower_exists_join` reads it from
// `plan_qualifiers(&outer_plan)`. Inner scope is read from the
// lowered subquery table plan. Semi/anti qualifier collection is
// narrowed to the preserving side in both planner and binder
// helpers so chained decorrelations do not leak prior inner
// aliases.
// P0.4: All rejected shapes use the existing unsupported frontend
// path via `unsupported(...)` / `FrontendError::Unsupported`; ADR
// 009 adds no new error variant and no semantic fallback path.
// P0.5: The two-scope classifier landed as private
// `SqlPlanner::classify_exists_conjunct(&SqlExpr, &[TableRef],
// &[TableRef]) -> Result<ExistsConjunct, FrontendError>`.
// `ExistsConjunct` has `CorrelatedPair { outer, inner }` and
// `InnerResidual(Expr)` arms; `Unsupported` is represented by an
// `Err`. The helper is unit-tested in-module in `src/sql_planner.rs`.
// P0.6: Subquery projection is validated only for the narrow V1
// surface and is never lowered or bound. The right-side plan is
// built solely from the subquery table, optional `InnerResidual`
// filter, and correlation keys.
// P0.7: Public fixtures live in
// `tests/exists_decorrelation_test.rs`: lowering shape for
// `EXISTS`/`NOT EXISTS`, digest parity with hand-written
// `LeftSemi`/`LeftAnti`, inner and outer residual placement,
// duplicate inner matches (`pk` 1, 3, 5), anti/null-key semantics
// (`pk` 2, 4, 6), multiple source orders, reused inner aliases,
// explicit outer-join composition, select-list irrelevance, and
// unsupported-shape rejection.

Phase 0 produces seven `// P0.x:` comments in this doc, then a
single commit `plan: adr 009 correlated exists decorrelation`.

### Phase 1 — Two-scope classifier

Files:
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. Classifier helper.** A private function over a single
  `sqlparser::ast::Expr` conjunct plus the outer and inner
  qualifier sets, returning `CorrelatedPair { outer_col,
  inner_col }` / `InnerResidual(ast_expr)` / `Unsupported`. Per
  ADR §4. Operates on the SQL AST (C1); ambiguity → `Unsupported`
  (C2).
- **B. Conjunct splitter.** A helper to split a top-level `AND`
  expression into a `Vec` of conjuncts, reused for both the outer
  `WHERE` and the subquery `WHERE`.

Tests — **in-module `#[cfg(test)]` in `src/sql_planner.rs`**, not
the integration test file. The classifier helper is private to
`sql_planner.rs` (P0.5), so integration tests in `tests/` cannot
reach it; `tests/exists_decorrelation_test.rs` is reserved for the
public SQL / end-to-end gates (Phases 2-4):
- `classifier_recognizes_correlated_equi_pair`.
- `classifier_recognizes_inner_only_residual_including_literals` —
  `dim.active = true`, `dim.score > 10`.
- `classifier_rejects_outer_only_subquery_predicate`.
- `classifier_rejects_non_equi_and_computed_correlation`.
- `classifier_rejects_unqualified_ambiguous_correlation`.

Gate: `cargo test -p toron --lib sql_planner::tests::classifier`.

### Phase 2 — Single-`EXISTS` rewrite in `lower_select`

Files:
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. Conjunct dispatch in `lower_select`.** After the outer
  `FROM` / explicit joins are lowered, split `select.selection`
  into top-level `AND` conjuncts. For each conjunct:
  - `SqlExpr::Exists { subquery, negated }` that passes the
    support check → decorrelation rule;
  - any conjunct containing an unsupported subquery or an
    `EXISTS` in an unsupported position → existing
    unsupported-expression rejection (C2, ADR §10);
  - otherwise → outer residual filter expression.
- **B. Decorrelation rule (single `EXISTS`).** Verify the
  subquery body shape (P0.2); classify the subquery `WHERE`
  conjuncts (Phase 1A); require ≥1 `CorrelatedPair` (ADR §5);
  build the right-side plan from the subquery relation + optional
  `InnerResidual` `LogicalPlan::Filter` (C3 — select list never
  lowered); construct the `LogicalPlan::Join` with
  `HashPartitioned` exchanges on both sides, `kind = LeftAnti` if
  `negated` else `LeftSemi`, `on` from the `CorrelatedPair` list
  (ADR §1).
- **C. Outer residual placement.** Remaining outer-only conjuncts
  wrap the final plan in `LogicalPlan::Filter` after the
  existence join (ADR §6) — safe by ADR 008 schema identity.
- **D. ADR 008 / M7 byte-identity check (FIRST).** Every existing
  SQL test that does not contain a now-supported `EXISTS` runs
  unchanged against the modified `lower_select`. If any digest
  moves, the conjunct-dispatch refactor is reverted before
  proceeding.

Tests (`tests/exists_decorrelation_test.rs`):
- `existing_sql_queries_byte_identical_after_lower_select_refactor`
  — runs first.
- `sql_correlated_exists_lowers_to_left_semi_join` — **Hard gate
  1.**
- `sql_correlated_not_exists_lowers_to_left_anti_join` — **Hard
  gate 2.**
- `exists_digest_matches_hand_written_left_semi_join` — **Hard
  gate 3.**
- `not_exists_digest_matches_hand_written_left_anti_join` — **Hard
  gate 4.**
- `exists_with_inner_only_residual_pushes_filter_on_subquery_side`
  — **Hard gate 5.**
- `exists_with_outer_residual_preserves_filter_semantics` — **Hard
  gate 6.**
- `exists_duplicate_inner_matches_emit_outer_once` — **Hard gate
  9.**
- `not_exists_null_correlation_key_preserves_anti_semantics` —
  **Hard gate 10.**
- `exists_select_list_is_semantically_ignored_for_select_one_star_and_inner_column`
  — **Hard gate 11.**

Gate: `cargo test -p toron --test exists_decorrelation_test`.

### Phase 3 — Left-deep chaining + composition

Files:
- `crates/toron/src/sql_planner.rs`

Work items:

- **A. Multiple-conjunct chaining.** When more than one top-level
  conjunct is a decorrelatable `[NOT] EXISTS`, apply each rule in
  source order, building a left-deep chain of semi/anti joins
  (ADR §7). Each rule consumes the current plan as its left input.
- **B. Composition over an explicit outer-`FROM` join.** No new
  code — the decorrelation rule already consumes whatever Phase A
  of `lower_select` produced as the outer plan, join or scan. The
  gate proves it.

Tests:
- `multiple_exists_conjuncts_lower_to_left_deep_semi_anti_chain` —
  **Hard gate 7.**
- `multiple_exists_conjuncts_result_independent_of_chain_order` —
  digest parity across two source orderings — **Hard gate 8.**
- `exists_above_explicit_from_join_preserves_scope_and_digest` —
  `EXISTS` correlated against one side of an explicit inner
  outer-`FROM` join; baseline is the explicit join followed by an
  explicit `LeftSemi` join — **Hard gate 16.**

Gate: `cargo test -p toron --test exists_decorrelation_test`.

### Phase 4 — Unsupported-shape rejection sweep

Files:
- `crates/toron/src/sql_planner.rs` (no new code expected — this
  phase proves the rejection paths)

Work items:

- **A. Rejection coverage.** Confirm every out-of-scope shape
  (ADR §"Not in scope" + §2/§3/§5/§8) routes to the existing
  unsupported-expression class with no panic, no silent fallback,
  no new error variant.

Tests:
- `exists_with_zero_correlation_pairs_is_unsupported` — **Hard
  gate 12.**
- `exists_with_outer_only_subquery_predicate_is_unsupported` —
  **Hard gate 13.**
- `exists_with_unqualified_ambiguous_correlation_is_unsupported` —
  includes the unaliased self-correlation fixture
  (`SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t WHERE t.k =
  t.k)`); the aliased form is supported — **Hard gate 14.**
- `exists_unsupported_shapes_stay_unsupported` — table test:
  `EXISTS` under `OR`; non-equi correlation; computed
  correlation; subquery aggregate / `LIMIT` / `DISTINCT`; nested
  subquery; subquery join — **Hard gate 15.**

Gate: `cargo test -p toron --test exists_decorrelation_test`.

### Phase 5 — Architecture guards + verification + promotion

Files:
- `crates/toron/tests/architecture_test.rs` — no edit; assertion
  that the existing guard set stays green.
- `crates/toron/plans/adr/009_correlated_exists_decorrelation.md`
- `crates/toron/plans/impl/009_correlated_exists_decorrelation.md`
- `crates/toron/plans/wip/roadmap.md`

Work items:

- **A. Architecture preservation.** Confirm
  `logical_plan_join_single_variant`,
  `execution_plan_join_single_variant`,
  `join_kind_eight_variants`, `join_strategy_two_variants`,
  `task_failure_kind_seven_variants`, and the message/wire-surface
  guards all stay green. **Hard gate 17.** ADR 009 adds no new
  guard.
- **B. Distributed parity spot-check.** A distributed-process
  test confirming a decorrelated `EXISTS` over a hash-partitioned
  plan produces a digest equal to the single-process baseline —
  reuses ADR 008's distributed semi/anti machinery, so this is a
  spot-check, not new coverage.
- **C. Verification + promotion.** Run the full verification
  command block. ADR 009 produces no bench-results artifact.

Gate: full verification block + `cargo test --workspace
--no-fail-fast`.

## Verification commands at promotion

```bash
cargo fmt -p toron --check
cargo check --workspace
cargo test -p toron --no-fail-fast
cargo test -p toron --test architecture_test --no-fail-fast
cargo test -p toron --test join_kind_test --no-fail-fast
cargo test -p toron --test hash_join_kinds_test --no-fail-fast
cargo test -p toron --test exists_decorrelation_test --no-fail-fast
cargo test -p toron --test distributed_process_test --no-fail-fast
cargo test --workspace --no-fail-fast
git diff --check
```

## Risks + mitigations

- **R1: `lower_select` conjunct-dispatch refactor regresses
  existing SQL.** Touching the outer `WHERE` lowering path is the
  most invasive change.
  *Mitigation:* P0.7 fixture inventory plus
  `existing_sql_queries_byte_identical_after_lower_select_refactor`
  runs first in Phase 2; if it fails, the refactor is reverted
  before any `EXISTS` rule lands.

- **R2: Classifier admits an unsafe mixed predicate.** A subquery
  conjunct mixing an outer reference with a non-trivial inner
  expression must reject, not become an `on` pair or a filter.
  *Mitigation:* C1 (classify on the SQL AST), C2 (ambiguity →
  unsupported); gates 13, 14, 15 pin the rejection.

- **R3: `NOT EXISTS` + outer-only subquery predicate produces
  wrong rows.** `NOT EXISTS (SELECT 1 FROM dim WHERE fact.flag =
  true AND dim.k = fact.k)` is **not** equivalent to applying
  `fact.flag = true` after an anti join.
  *Mitigation:* the classifier marks any outer-only subquery
  conjunct `Unsupported`, rejecting the whole rewrite — gate 13.

- **R4: Subquery projection accidentally bound.** A future
  refactor could lower the subquery select list and reject valid
  `EXISTS` forms on irrelevant select-list syntax.
  *Mitigation:* C3; gate 11 pins `SELECT 1` / `SELECT *` /
  `SELECT inner.k` equivalence.

- **R5: `NULL` correlation-key semantics divergence.**
  *Mitigation:* the rewrite introduces no NULL logic; it relies
  on ADR 008's `null_keys_never_match`. Gate 10 pins the
  end-to-end `NOT EXISTS` behavior.

- **R6: Left-deep chain order affects results.**
  *Mitigation:* every semi/anti join is a filter on the outer row
  set (ADR §7); gate 8 asserts digest parity across two source
  orderings.

- **R7: Composition over an explicit `FROM` join confuses
  scope.** The decorrelation rule operates after the outer `FROM`
  is lowered, which may already be a `LogicalPlan::Join`.
  *Mitigation:* gate 16 proves the existence join stacks on an
  explicit join with scope and digest preserved.

- **R8: Scope creep toward a general subquery engine.**
  *Mitigation:* ADR §"Not in scope" and §"Rejected alternatives"
  are explicit; the classifier's `Unsupported` arm is the
  structural boundary — every out-of-scope shape rejects.

- **R9: Binder responsibilities leak into the planner.**
  *Mitigation:* C4; the planner builds the logical rewrite only,
  the binder validates it exactly as it validates an explicit
  `SEMI` / `ANTI` join.

## Definition of done

- Phase 0 audit complete: seven `// P0.x:` decisions recorded in
  this doc, plus the planning commit.
- Phases 1-5 complete: hard gates 1-17 passing.
- Promotion evidence: digest parity against hand-written
  `LeftSemi` / `LeftAnti` joins (gates 3, 4, 8, 16) and preserved
  architecture guards (gate 17). No bench-results artifact.
- All verification commands above green.
- Documentation: ADR 009
  (`plans/adr/009_correlated_exists_decorrelation.md`) and impl
  plan (`plans/impl/009_correlated_exists_decorrelation.md`) land
  at promotion.
- Roadmap (`plans/wip/roadmap.md`) updated: ADR 009 marked
  Promoted + implemented; ADR 010 candidates reordered with
  "`EXISTS` decorrelation" removed from the candidate list.

## Files touched (expected)

- `crates/toron/src/sql_planner.rs` — two-scope classifier,
  conjunct splitter, `lower_select` conjunct dispatch,
  decorrelation rule, left-deep chaining, plus in-module
  `#[cfg(test)]` classifier unit tests.
- `crates/toron/src/binder.rs` — qualifier collection narrowed for
  semi/anti output scope; no new binder semantic check.
- `crates/toron/tests/exists_decorrelation_test.rs` — new;
  public SQL / end-to-end gates.
- `crates/toron/tests/distributed_process_test.rs` — one
  distributed parity spot-check.
- `crates/toron/plans/adr/009_correlated_exists_decorrelation.md`
- `crates/toron/plans/impl/009_correlated_exists_decorrelation.md`
- `crates/toron/plans/wip/roadmap.md`

## Files not expected to change

- `crates/toron/src/ir.rs` (`JoinKind` / `LogicalPlan` /
  `ExecutionPlan` shapes preserved).
- `crates/toron/src/exec.rs` (no executor change).
- `crates/toron/src/distributed/fragment.rs` (no fragmenter
  change; runtime-filter eligibility untouched).
- `crates/toron/src/wire.rs`, `crates/toron/src/distributed/task.rs`,
  `crates/toron/src/error/distributed.rs`.
- `crates/toron/tests/architecture_test.rs` (guards stay green;
  no new guard).
- `crates/toron/benches/*` (no new operator path).
- Wendelon crates.

## Promotion / fallback

Promote if all 17 hard gates pass and the full verification block
is green.

If `sqlparser` 0.54's `SqlExpr::Exists` shape or supported-body
detection differs from the P0.1 / P0.2 audit assumption, adjust
the classifier's AST matching; the contract (which shapes are
supported) does not move — only the AST-matching code does.

If a subquery shape the council assumed supported turns out to
need binder changes to lower safely, **narrow the scope** — move
that shape to the unsupported set rather than growing the binder.
ADR 009's value does not depend on any single shape; the
classifier's `Unsupported` arm is a safe contraction point.

If `EXISTS` decorrelation pressure for an out-of-scope shape
(subquery joins, uncorrelated `EXISTS`, `IN` / `NOT IN`) appears
during review, hold the boundary. Each is a named, deferred
extension; ADR 009 ships the narrow correlated single-table class.
