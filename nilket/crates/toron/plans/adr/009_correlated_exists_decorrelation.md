# ADR 009: Correlated `EXISTS` / `NOT EXISTS` Decorrelation

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/009_correlated_exists_decorrelation.md`

## Status

Promoted to `plans/adr/`. Implementation pending. No bench-results
artifact — ADR 009 introduces no new operator path; promotion
evidence is digest parity against hand-written `LeftSemi` /
`LeftAnti` joins plus preserved architecture guards (see
"Promotion evidence").

Builds on three WIP iteration rounds across the council
(`plans/wip/{claude,codex,gemini}_iter_*_adr_009.md`). Three-way
final convergence reached by iter-2; iteration artifacts
retired (git holds the narrative). Codex's two-scope classifier
shape, SQL-AST-first classification ordering, single-`TableFactor`
tightening, unqualified-correlation rejection, and
`SqlPlanner::lower_select` lowering order; Gemini's V1/post-V1
resolution and out-of-scope-routing framing; Claude's S1–S5
residual sharpenings, scale-boundary convention adherence, the
consolidated locked-contract block, and the canonical hard-gate
list are the load-bearing antecedents.

This ADR is a **narrow frontend-planner surface extension** that
closes the analytical-SQL existence-predicate gap left by ADR 008.
ADR 008 made `LeftSemi` / `LeftAnti` executable and tested; the
most common SQL path into those operators — correlated `EXISTS` /
`NOT EXISTS` — still hits the unsupported-expression rejection
path. ADR 009 lowers a narrow, safe class of correlated existence
predicates onto the existing ADR 008 join contract. No new join
algorithm, no new IR, no new wire surface, no new
`TaskFailureKind` variant, no executor change.

## Goal

Recognize a narrow, safe class of single-level correlated `EXISTS`
/ `NOT EXISTS` predicates in SQL `WHERE` clauses and lower them to
ADR 008's existing `JoinKind::LeftSemi` / `JoinKind::LeftAnti`
joins. Every subquery shape outside the supported class continues
to reject through the existing unsupported-expression path — there
is no semantic fallback. After ADR 009, the routine analytical
pattern `WHERE [NOT] EXISTS (SELECT ... FROM dim WHERE dim.k =
fact.k AND ...)` executes against the ADR 008 substrate.

## Scale boundary

**Primary:** frontend planner. ADR 009 is the first ADR to
exercise this boundary. The change is contained to
`crates/toron/src/sql_planner.rs` (`SqlPlanner::lower_select` and a
private two-scope classifier helper) and the bound `LogicalPlan`
it produces. Nothing below the planner moves: not the binder
contract, not the executor, not the fragmenter, not the wire.

**Secondary:** none. ADR 009 produces a `LogicalPlan::Join` that
is structurally identical to one the SQL planner already emits for
an explicit `SEMI` / `ANTI` join — there is no new serialized
shape.

**Not in scope:**
- No new IR. `LogicalPlan::Join` / `ExecutionPlan::Join` reused
  exactly as ADR 008 left them.
- No new `JoinKind`, `JoinStrategy`, `MessageKind`,
  `TaskFailureKind`, `BudgetDimension`, telemetry field, or
  Wendelon scan-contract change.
- No executor change. The rewrite targets ADR 008's already-
  benched `LeftSemi` / `LeftAnti` execution paths.
- No subquery execution engine. ADR 009 adds no subquery node to
  Toron's IR — it is a targeted SQL-AST rewrite.
- No `EXISTS` outside a top-level `AND` conjunct of the outer
  `WHERE` (no `EXISTS` under `OR` / `CASE` / function args /
  arithmetic / nested boolean structure).
- No uncorrelated `EXISTS` — semantically a scalar cardinality
  check, not a semi-join.
- No `IN` / `NOT IN` subquery decorrelation — three-valued-logic
  NULL semantics are a different decision matrix.
- No scalar subqueries in `SELECT` / `WHERE` / `HAVING` /
  projections.
- No subquery joins, nested subqueries, or subquery
  `GROUP BY` / aggregate / `HAVING` / `DISTINCT` / `LIMIT` /
  `ORDER BY` / `QUALIFY` / window / set operations.
- No non-equality, computed, or floating-point correlation; no
  new subquery name-resolution rule (unqualified / ambiguous
  correlation rejects).
- No rolling coordinator/worker mixed-version concern — ADR 009
  changes no serialized shape, so there is nothing to negotiate.

## V1 contract summary

- **`WHERE EXISTS (subquery)` → `JoinKind::LeftSemi`;
  `WHERE NOT EXISTS (subquery)` → `JoinKind::LeftAnti`.** The
  rewrite lives in `SqlPlanner::lower_select`, applied before
  generic expression lowering rejects `SqlExpr::Exists`.
- **Top-level outer `AND` conjuncts only.** The outer `WHERE` is
  split into top-level `AND` conjuncts; each is classified
  independently. An `EXISTS` anywhere other than a top-level
  conjunct is unsupported.
- **One subquery `TableFactor::Table`** (plus optional alias). No
  subquery joins.
- **Two-scope classifier.** Each top-level conjunct of the
  subquery `WHERE` classifies — on the SQL AST, before lowering —
  as `CorrelatedPair`, `InnerResidual`, or `Unsupported`.
- **At least one `CorrelatedPair` is required.** Zero pairs is
  uncorrelated `EXISTS` → unsupported.
- **Residual placement.** `InnerResidual` conjuncts become a
  `LogicalPlan::Filter` on the subquery input. Outer-only
  conjuncts of the outer `WHERE` stay an ordinary
  `LogicalPlan::Filter` over the outer plan and commute with the
  existence join by schema identity.
- **Source-order left-deep chaining.** Multiple top-level
  `[NOT] EXISTS` conjuncts lower as a left-deep chain of
  semi/anti joins in source order; chain order is
  correctness-neutral.
- **Unqualified / ambiguous correlation is unsupported.** No new
  subquery name-resolution rule.
- **Subquery select list is semantically ignored.**
- **ANSI `NULL != NULL` semantics delegated to ADR 008.** The
  rewrite introduces no NULL logic of its own.
- **Unsupported shapes reject — no semantic fallback.** Every
  out-of-scope shape continues through the existing
  unsupported-expression path.
- **No new contract surface.** No new IR, wire, executor,
  Wendelon, telemetry, `TaskFailureKind`, `JoinKind`, or
  `JoinStrategy` shape. ADR 009 adds no new architecture guard; it
  relies on the ADR 003 / 004 / 008 guards staying green.

## Context

ADR 008 landed the eight-variant `JoinKind` at `b28e8b0`, making
`LeftSemi` / `LeftAnti` (and the right-side variants) load-bearing,
tested, distributed-parity-checked physical operators. ADR 008's
SQL surface lowers explicit `SEMI` / `ANTI` join syntax to those
kinds, but the dominant SQL path into existence semantics —
correlated `EXISTS` / `NOT EXISTS` in a `WHERE` clause — still
hits the unsupported-expression rejection path in
`sql_planner.rs`, pinned by ADR 008's
`sql_exists_predicate_remains_unsupported` test. The execution
engine is built; the SQL door for the routine pattern is shut.

The roadmap pinned this as ADR 009 Candidate E and predicted it
would "likely follow ADR 008 implementation evidence." The council
scored it against the live candidate set (A multi-tenant
governance, B wire efficiency, E this) and selected E: A is
explicitly post-V1 by the roadmap's V1-target paragraph and its
contract shape wants multi-tenant workload evidence that does not
exist yet; B is the compute-ring fallback whose promotion
condition ("ADR 006's selective bench misses") cannot be evaluated
until the pending Wendelon-side row lands; E is the one candidate
whose substrate just shipped, whose scope is fully determined
today, and which continues the V1 "First Query with Integrity"
analytical-SQL-width thread. Candidate C stays locked on the
Wendelon row; D and F stay reserved/demoted.

## Decision

### 1. Rewrite shape

In `SqlPlanner::lower_select`, after the outer `FROM` and explicit
joins are lowered, the outer `WHERE` (`select.selection`) is split
into top-level `AND` conjuncts. Each conjunct that is
`SqlExpr::Exists { subquery, negated }` and passes the support
check (Decision §3–§5) is removed from the residual filter set and
lowered to a `LogicalPlan::Join`:

```rust
LogicalPlan::Join {
    left:  <current outer plan, wrapped in HashPartitioned Exchange>,
    right: <subquery relation + optional InnerResidual Filter,
            wrapped in HashPartitioned Exchange>,
    kind:  if negated { JoinKind::LeftAnti } else { JoinKind::LeftSemi },
    on:    <CorrelatedPair list>,
    strategy: JoinStrategy::HashPartitioned,
}
```

The planner emits explicit `HashPartitioned` exchanges on both
sides, exactly as ADR 008 SQL join lowering does for non-Inner
kinds. The binder is the unchanged authority for equi-key
validation, key-side resolution, type equality, float-key
rejection, exchange shape, and non-Inner broadcast rejection — the
planner classifies shape and builds the rewrite; it does not
duplicate binder semantic checks.

### 2. Top-level outer `AND` conjunct decomposition

Only top-level `AND` conjuncts of the outer `WHERE` are
decomposed. An `EXISTS` below `OR`, inside `CASE`, inside a
function argument, inside arithmetic, or under any other nested
boolean structure is **unsupported** — it remains on the existing
rejection path. ADR 009 does not introduce boolean-rewriting
machinery to hoist `EXISTS` out of arbitrary expressions.

### 3. Supported subquery shape

The subquery body is a single `SELECT` whose `FROM` has exactly
one `TableFactor::Table` (with optional alias). Any other body —
subquery joins, set operations, `GROUP BY` / aggregate /
`HAVING` / `DISTINCT` / `LIMIT` / `ORDER BY` / `QUALIFY` / window
clauses, or a nested subquery — is unsupported. The subquery
**select list is semantically ignored**: `SELECT 1`, `SELECT *`,
and `SELECT inner.k` are all equally valid in a supported `EXISTS`
form, and none is lowered or bound. The right-side plan is built
from the subquery relation, the optional `InnerResidual` filter,
and the correlation keys — never from the select list.

### 4. Two-scope classifier

Each top-level conjunct of the subquery `WHERE` is classified —
operating directly on the `sqlparser` AST, before any lowering to
Toron `Expr` — against the outer qualifier set and the inner
qualifier set:

- **`CorrelatedPair`** — exactly one side is a bare column
  reference resolvable in the outer scope, exactly one side is a
  bare column reference resolvable in the inner scope, and the
  operator is equality. Becomes one join `on` pair.
- **`InnerResidual`** — the conjunct has **no outer references**
  and lowers against the inner relation through the existing
  expression surface. Admits ordinary inner filters such as
  `dim.active = true`, `dim.score > 10`, `dim.region = 'us'`.
- **`Unsupported`** — anything else: outer-only predicates inside
  the subquery `WHERE`, non-equality correlation, computed
  correlation (`dim.k + 1 = fact.k`), predicates that mix an
  outer reference with anything other than a single inner column,
  nested subqueries, or unsupported expression nodes. Any
  `Unsupported` conjunct rejects the whole `EXISTS` rewrite.

The classifier is the load-bearing decision of the ADR. Lowering
to Toron `Expr` happens **only** for accepted `CorrelatedPair`
columns and `InnerResidual` predicates — the whole subquery
predicate is never lowered through generic `lower_expr` first,
because that would lose the chance to reject unsafe
outer-reference shapes precisely.

### 5. At least one correlation pair required

A candidate `EXISTS` whose subquery `WHERE` produces **zero**
`CorrelatedPair`s after classification is, by definition,
uncorrelated `EXISTS` — unsupported. The `on` list must be
non-empty. This makes "uncorrelated `EXISTS` stays unsupported"
fall out of the same classifier rather than needing a separate
detection path.

### 6. Residual placement

- **`InnerResidual` conjuncts** are combined into a
  `LogicalPlan::Filter` on the subquery input, beneath the
  right-side exchange.
- **Outer-only conjuncts of the outer `WHERE`** (conjuncts that
  are neither `EXISTS` nor reference the subquery) remain ordinary
  `LogicalPlan::Filter`s over the outer plan. They commute with
  the existence join because `LeftSemi` / `LeftAnti` output
  schema **is** the left (outer) schema unchanged (ADR 008 §3) —
  applying an outer residual before or after the existence join
  produces the same result. This is why the binder needs no
  change: the residual filter is an ordinary filter over an
  outer-schema plan.

### 7. Source-order left-deep chaining

Multiple top-level `[NOT] EXISTS` conjuncts, each independently
decorrelatable, lower as a left-deep chain of semi/anti joins. Each
semi and anti join is a *filter* on the outer row set — it
preserves each outer row at most once, adds no columns, and leaves
the schema as the outer schema — so the chain result multiset is
**independent of chain order**. V1 lowers in **source order**;
reordering is a correctness-neutral future cost decision and ADR
009 makes no cost claim.

### 8. Unqualified / ambiguous correlation is unsupported

Correlation columns must be qualified or otherwise unambiguously
scoped by the existing qualifier logic. ADR 009 does **not**
invent a new unqualified-name resolution rule inside subqueries.
If a column reference resolves to both scopes, to neither, or to
multiple qualifiers in a way the current planner cannot prove —
including same-table self-correlation without aliases (`SELECT *
FROM t WHERE EXISTS (SELECT 1 FROM t WHERE t.k = t.k)`) — the
existence rewrite rejects. The aliased self-correlation form
(`... FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.k = t.k)`)
is supported. The ADR prefers "too narrow and correct" over
matching a more permissive engine's name-resolution behavior; if
later planner work introduces robust name-resolution scopes, this
widens without touching the join executor.

### 9. NULL semantics delegated to ADR 008

The rewrite introduces no NULL handling of its own. A `NULL`
correlation key matches nothing under ADR 008's
`null_keys_never_match` guarantee, so a `NULL`-keyed outer row is
dropped by `EXISTS` (`LeftSemi`) and preserved by `NOT EXISTS`
(`LeftAnti`) — the correct ANSI three-valued result. Decorrelation
is not where NULL logic lives.

### 10. Unsupported shapes reject — no semantic fallback

Every shape outside the supported class continues through the
existing unsupported-expression rejection path. There is no
scalar-subquery execution path in Toron today, so there is nothing
to "fall back" to — a rejected shape behaves byte-identically to
today. ADR 009 can only widen what executes; it cannot regress
what executes. No new error variant is introduced — the existing
unsupported-expression class carries the rejection (ADR 008 P0.8
discipline).

### 11. Surface invariants

`LogicalPlan::Join` and `ExecutionPlan::Join` stay single-variant;
`JoinKind` stays eight variants; `JoinStrategy` stays two
variants; `TaskFailureKind` stays seven variants; no
`MessageKind`, `BudgetDimension`, telemetry field, or Wendelon
scan-contract change. ADR 009 adds **no new architecture guard** —
it asserts the ADR 003 / 004 / 008 guard set stays green.

## Hard gates

### Lowering correctness (1-8)

1. `sql_correlated_exists_lowers_to_left_semi_join`.
2. `sql_correlated_not_exists_lowers_to_left_anti_join`.
3. `exists_digest_matches_hand_written_left_semi_join`.
4. `not_exists_digest_matches_hand_written_left_anti_join`.
5. `exists_with_inner_only_residual_pushes_filter_on_subquery_side`.
6. `exists_with_outer_residual_preserves_filter_semantics`.
7. `multiple_exists_conjuncts_lower_to_left_deep_semi_anti_chain`.
8. `multiple_exists_conjuncts_result_independent_of_chain_order` —
   digest parity across two source orderings of the same conjunct
   set.

### Semantics (9-11)

9. `exists_duplicate_inner_matches_emit_outer_once`.
10. `not_exists_null_correlation_key_preserves_anti_semantics`.
11. `exists_select_list_is_semantically_ignored_for_select_one_star_and_inner_column`.

### Unsupported-rejection — no silent fallback (12-15)

12. `exists_with_zero_correlation_pairs_is_unsupported` —
    uncorrelated `EXISTS`, falling out of the classifier.
13. `exists_with_outer_only_subquery_predicate_is_unsupported`.
14. `exists_with_unqualified_ambiguous_correlation_is_unsupported`
    — includes the unaliased self-correlation fixture.
15. `exists_unsupported_shapes_stay_unsupported` — table test:
    `EXISTS` under `OR`; non-equi correlation; computed
    correlation; subquery aggregate / `LIMIT` / `DISTINCT`;
    nested subquery; subquery join.

### Composition (16)

16. `exists_above_explicit_from_join_preserves_scope_and_digest` —
    an `EXISTS` correlated against one side of an explicit
    (inner) outer-`FROM` join; the hand-written baseline is the
    same explicit join followed by an explicit `LeftSemi` join.
    Proves the existence join stacks on top of an existing
    `LogicalPlan::Join` without scope or schema confusion.
    ("`from` join" — the outer query's `FROM` join, not an SQL
    outer-join kind.)

### Architecture preservation (17)

17. `architecture_guards_unchanged` — `logical_plan_join_single_variant`,
    `execution_plan_join_single_variant`, `join_kind_eight_variants`,
    `join_strategy_two_variants`, `task_failure_kind_seven_variants`,
    and the message/wire-surface guards all stay green. ADR 009
    adds no new guard.

## Promotion evidence

ADR 009 is a planner rewrite onto ADR 008's already-bench-recorded
operators — there is no new execution path to benchmark. Promotion
evidence is **digest parity against hand-written `LeftSemi` /
`LeftAnti` joins** (gates 3, 4, 8, 16) plus the preserved
architecture guards (gate 17). No bench-results artifact is
produced; the verification-commands block carries the parity-test
invocations in place of a bench run.

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

## Open questions

- **`sqlparser` 0.54 `SqlExpr::Exists` shape.** Confirm `negated`
  is the `NOT EXISTS` discriminator and `subquery` is a
  `Box<Query>`, and confirm the supported body matches
  `SetExpr::Select` with a single `TableFactor::Table`. Pinned to
  Phase 0 audit (P0.1, P0.2).

## What Not To Do

- Don't add a subquery node to Toron's IR. ADR 009 is a targeted
  SQL-AST rewrite; the logical and physical join contract is
  reused unchanged.
- Don't lower the whole subquery predicate through `lower_expr`
  before classifying. Classification operates on the SQL AST so
  unsafe outer-reference shapes can be rejected precisely; a
  generic lowering pass would route a mixed predicate into a
  generic filter path.
- Don't invent a new unqualified-name resolution rule for
  subqueries. If the existing qualifier logic cannot pin one side
  to the outer scope and one to the inner scope, reject.
- Don't decorrelate uncorrelated `EXISTS`. Its semantics are a
  scalar cardinality check, not a semi-join — a separate planner
  rule if evidence ever justifies it.
- Don't decorrelate `IN` / `NOT IN` subqueries. `NOT IN` carries
  NULL-sensitive three-valued logic that must not be smuggled
  into anti-join semantics — its own decision matrix, its own ADR.
- Don't hoist `EXISTS` out of `OR` / `CASE` / function args /
  arithmetic. Top-level outer `AND` conjuncts only.
- Don't widen the subquery body beyond one `TableFactor::Table`.
  Subquery joins, aggregates, `LIMIT`, `DISTINCT`, set ops, and
  nested subqueries are unsupported and reject.
- Don't bind or lower the subquery select list. It is
  semantically irrelevant to the existence check.
- Don't duplicate binder semantic checks in the planner. The
  binder stays the authority for type compatibility, float-key
  rejection, computed-key rejection, exchange shape, and
  non-Inner broadcast rejection.
- Don't introduce a new error variant for unsupported `EXISTS`
  shapes. Reuse the existing unsupported-expression class.
- Don't make a cost claim. Source-order left-deep chaining is
  correctness-neutral; reordering is a future cost decision.
- Don't add a new architecture guard or any new contract surface.
  ADR 009 relies on the ADR 003 / 004 / 008 guards staying green.

## Rejected alternatives

- **General subquery execution engine.** ADR 009 is a narrow
  SQL-AST rewrite, not a subquery IR node + executor. The general
  engine is unbounded scope; the rewrite covers the routine
  analytical pattern with zero new execution surface.
- **Subquery joins in the V1 body.** Considered (Claude iter-0's
  "base relation or join"); rejected (Codex iter-1). Subquery
  joins widen side-resolution and residual-placement rules — scope
  bleed the narrow-ADR discipline exists to stop. Named, deferred
  extension.
- **Uncorrelated `EXISTS` in the same cycle.** Considered;
  rejected. Not a semi/anti rewrite — it is a scalar cardinality
  check. Conflating it widens the semantic surface; it falls out
  of the classifier as zero-correlation-pairs and stays
  unsupported.
- **Inner-only residual defined as "both sides resolve in the
  inner scope."** Considered (Claude iter-1 S1 phrasing);
  rejected (Codex iter-1) in favor of "no outer references and
  lowerable against the inner relation" — the latter correctly
  admits literal predicates like `dim.active = true`.
- **A new unqualified-name resolution rule for subqueries.**
  Considered; rejected. ADR 009 should not grow fragile
  name-resolution machinery; ambiguous correlation rejects, and
  the contract widens for free if later planner work hardens
  scoping.
- **`IN` / `NOT IN` decorrelation.** Out of scope — `NOT IN`'s
  three-valued NULL logic is a separate decision matrix.
- **New error variant for unsupported `EXISTS`.** Considered;
  rejected. No new failure class — the existing
  unsupported-expression class is the right surface.
- **Treating ADR 009 as the first post-V1 ADR (to select
  Candidate A instead).** Considered (the roadmap's candidate
  ordering lists A first); rejected. The roadmap's V1-target
  paragraph lists multi-tenant governance as post-V1, and no
  post-ADR-008 evidence surfaces multi-tenancy as the binding
  constraint while a routine V1 SQL-surface gap is open. ADR 009
  is a V1 ADR.

## Council convergence summary

Three-way agreement at iter-2:

| Decision                                              | Status      |
|-------------------------------------------------------|-------------|
| Select Candidate E (`EXISTS` decorrelation)           | converged   |
| ADR 009 is V1, not the post-V1 transition             | converged   |
| Scale boundary: frontend planner                      | converged   |
| Reuse ADR 008 `LeftSemi` / `LeftAnti`, no new IR      | converged   |
| No new wire / executor / Wendelon / bench surface     | converged   |
| Single `TableFactor` subquery, no subquery joins      | converged   |
| Top-level outer `AND` conjuncts only                  | converged   |
| Two-scope classifier on the SQL AST                   | converged   |
| `CorrelatedPair` / `InnerResidual` / `Unsupported`    | converged   |
| At least one correlation pair required                | converged   |
| Inner residual = no outer refs, lowers against inner  | converged   |
| Unqualified / ambiguous correlation unsupported       | converged   |
| Subquery select list semantically ignored             | converged   |
| Source-order left-deep chain, correctness-neutral     | converged   |
| Outer residual commutes by ADR 008 schema identity    | converged   |
| `NULL` semantics delegated to ADR 008                 | converged   |
| Unsupported shapes reject, no semantic fallback        | converged   |
| Digest parity = promotion evidence, no perf gate      | converged   |
| Rewrite in `SqlPlanner::lower_select`, binder untouch | converged   |
| No new architecture guard                             | converged   |

Remaining items at promotion are pin-level Phase 0 audit work
(P0.1 sqlparser shape, P0.2 supported body, P0.5 classifier helper
shape), recorded in the impl plan.
