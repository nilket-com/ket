# ADR 018: Scalar-Aggregate `HAVING` / `ORDER BY` SQL

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Companion implementation plan:**
`plans/impl/018_scalar_aggregate_having_order_by.md`

## Status

Promoted. Implementation pending.

Three-way AI Swarm Council convergence reached after the
ADR 018 planning session. Iter-0 nominated scalar-aggregate
`HAVING` / `ORDER BY` as the lead candidate alongside an
evidence-hunt sweep that surfaced one narrow inert-feature
finding (experimental-kernel bound/unbound parity gap at
`exec.rs:912-916` and `exec.rs:1035-1039`) which did not
outrank — gated behind `allow_experimental_kernels` (default
false), so production SQL paths don't reach it. The gap
escalates the existing roadmap kernel-graduation block as
ADR 019 evidence pressure but not as ADR 018 substitute.
The planning iterations ground-truth-verified the substrate:
the HAVING-without-GROUP-BY lift shape is a conditional at
`sql_planner.rs:193-195`; empty-input aggregate behavior
inherits ADR 011 verbatim; prior-ADR V1-exclusion sweeps
surfaced no hidden candidate. Gemini's post-aggregate-
projection alternative was deferred because it needs output-
alias schema work and a fragmenter decision for `Project`
above grouped aggregate output. Final convergence ratified
two load-bearing corrections:

- **Ordered scalar shape has one exchange, not zero.**
  `lower_query` at `sql_planner.rs:101-121` unconditionally
  wraps `Sort` over `Exchange(partition_count: 1, key: [])`
  whenever `ORDER BY` is present. Ordered scalar aggregate
  fragments through the generic one-exchange producer/consumer
  path at `fragment.rs:88-162`, not `single_task_stage`.
  Only the HAVING-only case (no top-level `ORDER BY`) is
  zero-exchange. Multi-process digest parity therefore *does*
  apply to ordered scalar and the distributed fixture lands.
- **The grouped branch's reject-ordering is user-visible and
  load-bearing.** Pre-ADR-018, `SELECT region FROM sales
  GROUP BY region HAVING SUM(amount) > 100` rejects with
  `"HAVING aggregate-call must appear in SELECT list"` because
  HAVING-predicate materialization happens before the
  `aggr_expr.is_empty()` check. An earlier draft proposed
  reordering the check above the HAVING attachment via a
  shared helper; final convergence retired the proposal.
  ADR 018 keeps the grouped branch byte-identical and does not extract a
  shared helper.

The council finalized the contract on this corrected shape;
iteration artifacts retire with this promotion — git
holds the narrative.

ADR 018 owns no independent bench artifact. Its
planned implementation is the fourth instance of the
composition cycle established by ADR 015 (`Sort` over
`Aggregate`), proved by ADR 016 (`Filter` over `Aggregate`),
generalized by ADR 017 (`ORDER BY` aggregate-call rewrite
over the grouped surface). The load-bearing property of the
cycle shape — source-level boundary clarity at named-reject
granularity, no silent acceptance, no silent drop — is
reaffirmed against the scalar-aggregate surface. The
substrate (walker, scope, `ResolveOutcome`, four-way
ambiguity classification, message helpers,
`bind_comparison` literal coercion) generalizes without
modification — the scope's `group_keys: &[GroupKey]`
parameter accepts `&[]`, and the empty-group-keys case is
mechanical.

Load-bearing antecedents: ADR 010 (`Sort` operator surface,
sort-key binder semantics, total-order tie-break, the
unconditional `Sort + Exchange(1, [])` wrap when `ORDER BY`
is present); ADR 011 (global-aggregate single-stage shape,
P0.5 inheritance, projection-shape contract); ADR 015
(terminal-gather shape for the grouped path; scalar ordered
path uses the generic one-exchange fragmenter route);
ADR 016 (planner-side rewrite walker substrate, `Filter`
placement, `GroupedAggregateScope`, `bind_comparison`
numeric-literal coercion); ADR 017 (`LoweredSelect` seam,
shape-neutral walker, `ResolveOutcome`,
`reject_qualified_having_column_references` pre-pass,
clause-label threading, message-helper functions).

## Goal

Enable `HAVING` and `ORDER BY` clauses that reference scalar
(no-`GROUP BY`) aggregate calls projected by the SELECT
list. The canonical analytical patterns:

```sql
SELECT SUM(amount)
FROM sales
HAVING SUM(amount) > 1000
```

```sql
SELECT SUM(amount)
FROM sales
ORDER BY SUM(amount) DESC
```

```sql
SELECT SUM(amount)
FROM sales
HAVING SUM(amount) > 1000
ORDER BY SUM(amount) DESC
LIMIT 1
```

Today the first rejects at `sql_planner.rs:193-195`
(`"HAVING without GROUP BY is not supported"`); the second
rejects at `sql_planner.rs:795` (the generic
`"unsupported expression"` catch-all of `lower_expr`'s
`SqlExpr::Function` arm when ORDER BY references an
aggregate call without a GROUP BY scope); the third
compounds both rejections. ADR 016 §1 and ADR 017 §10
explicitly nominated these as catch-all rejects that "a
future ADR's surface" would lift. ADR 018 is that future
ADR.

## Scale boundary

**Primary:** Toron planner.

In scope:

- conditioning the `HAVING`-without-`GROUP BY` reject at
  `sql_planner.rs:193-195` on the absence of an aggregate
  projection; reject-string lift to `"HAVING requires
  GROUP BY or an aggregate projection"`;
- constructing `GroupedAggregateScope::from_projection(&[],
  &aggr_expr)` in the global-aggregate branch of
  `lower_select`;
- inlining the optional HAVING-attachment in the global-
  aggregate branch (a small `if let Some(having) = ...`
  block that lowers the predicate via
  `lower_having_predicate` and wraps the aggregate in
  `LogicalPlan::Filter`);
- planner / executor / fragmenter / distributed /
  architecture test coverage in `group_by_test.rs`,
  `distributed_process_test.rs`, and `architecture_test.rs`;
- semantic architecture guards: presence of the new lifted
  reject string, absence of the old reject string,
  continued presence of the ADR 016 / ADR 017 walker /
  scope / pre-pass reject strings.

Explicitly unchanged:

- ADR 010's `Sort` operator surface and the unconditional
  `Sort + Exchange(1, [])` wrap;
- ADR 011's `Aggregate` operator surface, projection-shape
  contract, P0.5 single-stage scalar shape, hash-
  partitioned exchange for grouped aggregates;
- ADR 015's nested-shape detector and three-stage
  `StagePlan` decomposition (correctly returns `None` for
  scalar shapes — its inner-Exchange destructure fails
  because scalar plans have no inner Exchange under the
  Aggregate);
- ADR 016's `Filter` placement, `FilterDisposition::
  EvaluateInToron`, four-way ambiguity classification,
  `bind_comparison` numeric-literal coercion;
- ADR 017's `LoweredSelect` seam, shape-neutral walker,
  `ResolveOutcome` enum, clause-label threading, message-
  helper functions,
  `reject_qualified_having_column_references` pre-pass;
- the grouped-aggregate branch of `lower_select` at
  `sql_planner.rs:241-277`. ADR 018 does not refactor it;
  the existing `having_predicate` materialization,
  `aggr_expr.is_empty()` check ordering, and the `if let
  Some(predicate)` filter wrap stay byte-identical. Reject-
  ordering for the malformed-input shape `SELECT region
  FROM sales GROUP BY region HAVING SUM(amount) > 100`
  remains `"HAVING aggregate-call must appear in SELECT
  list"`;
- `Filter` operator surface, `FilterDisposition` variant
  set, `bind_expr` expression set;
- table-query wire frame, runtime-filter descriptor
  surface, PBC bytecode;
- `JoinKind`, `JoinStrategy`, scheduler-barrier semantics,
  `TaskSpec`, `TaskFailureKind`, `BudgetDimension`,
  `MessageKind`, `TaskOutputBinding` variants;
- `lower_order_by`'s signature
  (`scope: Option<&GroupedAggregateScope>` parameter
  established by ADR 017 already accepts the scalar case).

No new IR variant, no new fragmenter recognition site, no
new shuffle binding, no new stage role, no new failure
kind, no new wire surface, no new aggregate function. Every
extension named in ADR 017's handoff remains a separate
future ADR.

## Cycle shape

ADR 018 is the fourth instance of the composition cycle.
The load-bearing property across the four:

> **Source-level boundary clarity.** Every SQL surface
> either lowers to an existing operator chain or rejects
> through a deterministic planner reject. No silent
> acceptance. No silent drop. The reject is typically a
> narrow named string for the common analytical shapes;
> deliberately generic catch-all rejects
> (`"unsupported expression"`) are acceptable for shapes a
> future ADR will lift into a dedicated surface.

The four instances:

- **ADR 015 (`Sort` over `Aggregate`):** removed one wide
  reject; added one narrow fragmenter detector. No new IR.
- **ADR 016 (`Filter` over `Aggregate`):** removed one wide
  reject; introduced three narrow named rejects;
  introduced the planner-side walker substrate. No new IR.
- **ADR 017 (`Sort` over `Filter`-or-`Aggregate` with
  rewritten keys):** refactored the walker shape-neutral;
  reused via one new caller in `lower_order_by`. Two new
  narrow named rejects. No new IR, no detector change.
- **ADR 018 (scalar `Filter`-and-`Sort` over `Aggregate`):**
  conditioned one existing reject; constructed the scope in
  a previously-`None` branch; inlined the HAVING-attachment
  in the global branch. One reject-string lift (old
  retires, new lands). No helper extraction. No grouped-
  branch refactor. No new IR, no detector change, no walker
  signature change.

Iteration four generalizes. ADR 019 selection starts from a
substrate now proved against both grouped and scalar
aggregate shapes, with the same walker, scope, and
ambiguity-classification logic serving both.

## Context

The conditional reject site:

- `SqlPlanner::lower_select` at `sql_planner.rs:193-195` —
  unconditional reject today; ADR 018 conditions on the
  absence of an aggregate projection.

The scope-construction site:

- `SqlPlanner::lower_select` global-aggregate branch at
  `sql_planner.rs:280-291` — currently returns
  `LoweredSelect { plan, grouped_aggregate_scope: None }`.
  ADR 018 constructs the scope and inlines the optional
  HAVING-attachment.

The scope type:

- `GroupedAggregateScope::from_projection` at
  `sql_planner.rs:885-912` accepts `group_keys:
  &[GroupKey]`. The empty-`group_keys` case populates
  `output_name_counts` with only aggregate names; zero
  group-key entries flow through the existing iteration.
  Generalization is mechanical.

The order-wrap site:

- `SqlPlanner::lower_query` at `sql_planner.rs:101-121` —
  unconditionally wraps `Sort` over `Exchange(partition_
  count: 1, key: [])` when `order_by.is_some()`. This is
  the ADR 010 / ADR 015 terminal-gather contract; it
  applies to all queries, scalar and grouped alike. Scalar
  ordered queries therefore land with one exchange, not
  zero.

The fragmenter routing:

- `fragment_plan` at `fragment.rs:65-86` branches on
  `count_exchanges(&plan)`. Three relevant cases for ADR
  018:
  - Scalar HAVING-only `Filter(Aggregate(scan))` — zero
    exchanges, routes via `single_task_stage` at
    `fragment.rs:467-489`.
  - Scalar ordered `Sort(Exchange(1, [])(Aggregate(scan)))`
    — one exchange, routes via the generic producer/
    consumer two-stage path at `fragment.rs:88-162`.
  - Scalar dual-rewrite
    `Sort(Exchange(1, [])(Filter(Aggregate(scan))))` — one
    exchange, routes via the generic two-stage path.

The executor:

- `execute_bound` at `exec.rs:537-554` dispatches
  `bound_global_aggregate` vs `bound_grouped_aggregate`
  based on `group_expr.is_empty()`. `bound_filter` and
  `bound_sort` compose on the single-row scalar output.

The binder's literal coercion (inherited ADR 016):

- `bind_comparison` coerces RHS numeric literals to match
  LHS column type for comparisons like
  `SUM(amount) > 1000`. ADR 018 inherits unchanged.

## Decision

### §1. Conditional reject lift at `sql_planner.rs:193-195`

Replace:

```rust
if having.is_some() && group_keys.is_none() {
    return Err(unsupported("HAVING without GROUP BY is not supported"));
}
```

with:

```rust
if having.is_some()
    && group_keys.is_none()
    && !projection_contains_aggregate_call(projection)
{
    return Err(unsupported(
        "HAVING requires GROUP BY or an aggregate projection",
    ));
}
```

Reject-string lift: precondition-style, matches the
declarative form of `"GROUP BY requires at least one
aggregate call"`. Architecture-guard delta:

- **Presence (new):** `"HAVING requires GROUP BY or an
  aggregate projection"`.
- **Absence (new):** `"HAVING without GROUP BY is not
  supported"`.

### §2. Global branch scope construction + inline HAVING wiring

`sql_planner.rs:280-291` becomes:

```rust
if projection_contains_aggregate_call(projection) {
    let aggr_expr = self.lower_global_aggregate_projection(projection)?;
    let scope = GroupedAggregateScope::from_projection(&[], &aggr_expr);
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(plan),
        group_expr: vec![],
        aggr_expr,
    };
    let plan = if let Some(having) = having.as_ref() {
        let predicate = self.lower_having_predicate(having, &scope)?;
        LogicalPlan::Filter {
            input: Box::new(aggregate),
            predicate,
        }
    } else {
        aggregate
    };
    return Ok(LoweredSelect {
        plan,
        grouped_aggregate_scope: Some(scope),
    });
}
```

The scope's `group_keys` argument is `&[]`. The
`output_name_counts` HashMap populates with only aggregate
output names. The four-way classification still has four
arms; the "group-key collision" arm is unreachable in the
scalar case but the code path is identical.

Returning `Some(scope)` lights up the existing
`lower_order_by` walker invocation. No `lower_order_by`
signature change.

The HAVING-attachment is inlined rather than extracted into
a shared helper. With only one caller (the global branch),
a helper would not earn the named-seam value the cycle's
source-level boundary clarity property would otherwise
favor. If a future ADR adds a third HAVING-attachment caller
(e.g., post-aggregate projection over grouped), helper
extraction becomes worthwhile then; ADR 018 does not
preempt that decision.

### §3. Grouped branch — explicitly unchanged

`sql_planner.rs:241-277` is byte-identical pre- and post-
ADR-018. The existing `having_predicate` materialization
order (HAVING lowered before `aggr_expr.is_empty()` check)
is preserved. Reject-ordering for the malformed-input shape
`SELECT region FROM sales GROUP BY region HAVING SUM(amount)
> 100` remains `"HAVING aggregate-call must appear in
SELECT list"` (fires from inside the HAVING-predicate
lowering, before the empty-aggregate check). Pre- and
post-ADR-018 user behavior on the grouped branch is
identical.

### §4. Pre-pass symmetry — automatic inheritance

`reject_qualified_having_column_references` at
`sql_planner.rs:1010-1024` runs unchanged. The global
branch invokes it via `lower_having_predicate`. The
pre-pass recurses through `CompoundIdentifier`, `BinaryOp`,
`IsNull`, `IsNotNull`, and `Nested`, mirroring the walker's
recursion surface. Scalar `HAVING t.col > 0` rejects with
the existing `"qualified HAVING column references are not
supported"` string — symmetric with the grouped case.

### §5. Fragmenter routing — three cases, zero detector code

Scalar plans split into three fragmenter routes:

- **HAVING-only** (`Filter(Aggregate(scan))`) — zero
  exchanges, routes via `single_task_stage` at
  `fragment.rs:467-489`. The whole plan executes on one
  worker as one task with `output: FinalResult`.
- **ORDER BY-only**
  (`Sort(Exchange(1, [])(Aggregate(scan)))`) — one
  exchange, routes via the generic producer/consumer
  two-stage path at `fragment.rs:88-162`. Stage 0 runs the
  scalar `Aggregate(scan)` and publishes through the
  `Exchange(1, [])` to a single shuffle partition; Stage 1
  runs `Sort(<shuffle scan>)` and produces `FinalResult`.
- **Dual rewrite**
  (`Sort(Exchange(1, [])(Filter(Aggregate(scan))))`) — one
  exchange, same two-stage routing as ORDER BY-only; the
  `Filter` evaluates inside Stage 0 before publish.

`is_terminal_sort_over_grouped_aggregate` at
`fragment.rs:562-632` correctly returns `None` for all
three scalar shapes. The HAVING-only case has no outer
`Sort`; the ORDER BY cases destructure the outer
`Sort -> Exchange(1, [])` but fail at the inner-Exchange
destructure because scalar plans have no `Exchange(K,
group_keys)` between the `Aggregate` and the scan. The
detector's negative-shape behavior is preserved; the
grouped detector continues to fire only for grouped plans.

No new fragmenter code. No new detector. The generic
one-exchange path was already there.

### §6. Walker, scope, message helpers — zero change

`rewrite_grouped_aggregate_calls`,
`resolve_grouped_aggregate_reference`,
`GroupedAggregateScope::{from_projection, resolve}`,
`ResolveOutcome`, `aggregate_call_not_in_select_message`,
`ambiguous_aggregate_call_message` — all unchanged. The
message-helper `unreachable!` arms continue to guard
against unknown clause labels; ADR 018's new call paths
still pass `"HAVING"` and `"ORDER BY"`. No new clause
label.

### §7. Binder coercion — inherited verbatim from ADR 016

`bind_comparison`'s numeric-literal coercion composes
unchanged for scalar HAVING predicates. The rewritten
expression `Expr::Gt(Expr::column("sum"), Literal(1000))`
binds identically to the grouped case: LHS column type
from `Aggregate { group_expr: [] }`'s output schema; RHS
literal coerces to match. Zero binder change.

### §8. Empty-input semantics inherit ADR 011

`bound_global_aggregate` at `exec.rs:871-924` returns the
canonical inherited behavior. `Filter` and `Sort` compose
on the single-row scalar aggregate output. The post-HAVING
filtered output is either zero or one rows; ORDER BY on a
one-row plan is trivially identity-ordered.

### §9. No new surface

ADR 018 introduces no new IR variant; no new `JoinKind`,
`JoinStrategy`, `MessageKind`, `TaskFailureKind`,
`BudgetDimension`, `FilterDisposition`, scheduler barrier
kind, `TaskOutputBinding` variant; no new wire frame,
runtime-filter descriptor field, PBC opcode; no new
aggregate function, `aggregate_field` contract, or
aggregate-output schema; no new `bind_expr` match arm; no
new planner cost-model knob; no fragmenter recognition
site; no new architecture guard *class*.

## V1 exclusions

ADR 018-specific exclusions:

1. **Scalar aggregate two-phase distribution.** ADR 018
   ships the scalar aggregate as a single-stage compute
   (HAVING-only routes via `single_task_stage`; ordered
   variants route via the generic two-stage producer/
   consumer path with a single-partition shuffle). A future
   two-phase partial/final aggregation ADR would replace
   the single-stage scalar compute with per-partition
   partial aggregates feeding a final aggregator; ADR 018
   does not.
2. **`HAVING` over a non-aggregate projection.** `SELECT
   region FROM sales HAVING region = 'EMEA'` continues to
   reject with the lifted string.
3. **`HAVING` referencing a non-projected scalar
   aggregate.** Rejects via the existing `"HAVING
   aggregate-call must appear in SELECT list"` string.
4. **Ambiguous `HAVING` / `ORDER BY` aggregate-call
   references in the scalar case.** Four-way classification
   fires identically to the grouped case.
5. **Qualified column references in `HAVING`.** Rejects
   via the existing pre-pass symmetric to the grouped
   case.
6. **`ORDER BY -SUM(amount)`** and other `UnaryOp` /
   arithmetic over aggregate calls. Walker does not recurse
   outside its declared arm set. Existing behavior, future
   ADR if evidence justifies. ADR 016 §V1 §6 and ADR 017
   §V1 §5/§6 carry forward.
7. **Experimental aggregate-call references**
   (`LINEAR_REGRESS`, `APPROX_QUANTILE`). ADR 018 does not
   add signatures for these in
   `grouped_aggregate_signature`. The known bound-executor
   parity gap belongs to the kernel-graduation block.

Inherited rejects (preserved verbatim from prior ADRs):

8. **`DISTINCT`** — ADR 011's `"DISTINCT is not
   supported"`.
9. **Computed aggregate-call arguments** — ADR 011's
   restrictions.
10. **Aliased aggregates** — ADR 011 rejects.
11. **Experimental kernels under GROUP BY** —
    `lower_aggregate_function` already rejects.

12. **Empty-input aggregate semantics.** ADR 018 inherits
    `bound_global_aggregate`'s current behavior verbatim:
    `COUNT` returns 0 and `SUM` materializes NULL when no
    input value was seen. The regression test pins
    inherited behavior; ADR 018 does not redesign aggregate
    kernel semantics.

## Consequences

- The three canonical scalar-aggregate patterns lower,
  fragment, and execute end-to-end. The HAVING-only case
  fragments single-stage; the ordered cases fragment as a
  generic one-exchange two-stage plan with byte-identical
  digest across worker counts.
- ADR 016 §1 and ADR 017 §10 catch-all rejects are closed.
- The walker, scope, `ResolveOutcome`, four-way ambiguity
  classification, pre-pass, message helpers, and
  `bind_comparison` literal coercion — all generalize to
  the scalar surface without modification. The cycle's
  "shape-neutral walker" framing earns its fourth proof.
- The grouped-aggregate branch is byte-identical pre- and
  post-ADR-018; user-visible HAVING-rejection ordering for
  malformed grouped inputs is preserved.
- `GroupedAggregateScope` is now a narrow name for a scope
  also used by scalar aggregate output. A rename ("output-
  name catalog" or similar) may be appropriate when a
  future ADR widens the scope into a broader post-aggregate
  projection surface; ADR 018 does not rename.
- ADR 011's P0.5 single-stage scalar compute inherits; the
  ordered variants gain a one-partition shuffle stage from
  ADR 010's existing wrap, but the *aggregate* stays
  single-task. Performance ceiling remains "one worker for
  the scan + aggregate"; future two-phase ADR carries that
  lift.
- Architecture guards are semantic.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - Canonical patterns lower correctly per Decision §2.
     The HAVING-only case lowers to `Filter(Aggregate {
     group_expr: [] })`. The ORDER BY case lowers to
     `Sort(Exchange(1, [])(Aggregate { group_expr: [] }))`.
     The dual-rewrite case lowers to
     `Sort(Exchange(1, [])(Filter(Aggregate { group_expr:
     [] })))`.
   - New presence guard fires; old absence guard confirms
     removal.
   - Grouped branch tests from ADR 016 / ADR 017 remain
     byte-identical green (branch is unchanged).
   - V1 exclusion items §2–§11 reject via inherited
     paths; §12 pinned by regression test that asserts
     inherited behavior.

2. **Binder / executor** —
   - Rewritten ORDER BY column references resolve via
     existing `bind_expr` + `resolve_column` paths.
   - Rewritten HAVING comparisons bind via the inherited
     ADR 016 `bind_comparison` numeric-literal coercion.
     No new binder code path.
   - End-to-end execution returns inherited results for the
     canonical patterns.

3. **Fragmenter** —
   - HAVING-only shape routes via `single_task_stage`.
   - Ordered scalar shape routes via the generic
     one-exchange producer/consumer two-stage path.
   - `is_terminal_sort_over_grouped_aggregate` correctly
     returns `None` for all three scalar shapes; grouped
     detector no-regression.

4. **In-process** —
   - SQL result digest-matches a hand-built logical plan
     for both the HAVING-only and dual-rewrite shapes.

5. **Distributed** —
   - Ordered scalar aggregate (with and without HAVING)
     produces byte-identical normalized output across two-
     and four-worker fixtures in
     `distributed_process_test.rs`. The one-exchange
     fragmenter route is exercised end-to-end.

No bench artifact.

## What not to do

- Do not introduce a new IR variant for scalar aggregates.
  `Aggregate { group_expr: [] }` is the existing
  representation.
- Do not extend `lower_order_by`'s signature. The ADR 017
  `scope: Option<&GroupedAggregateScope>` parameter
  already accepts the scalar case.
- Do not add a new clause label to the message-helper
  functions. `"HAVING"` and `"ORDER BY"` are the only
  labels.
- Do not refactor the grouped branch to share a helper
  with the global branch. The grouped branch's existing
  reject-ordering is user-visible and load-bearing; a
  shared helper would force a reorder that changes
  rejection messages for malformed grouped inputs. If a
  future ADR adds a third HAVING-attachment caller, helper
  extraction becomes worthwhile *then* and the third
  caller's review can ratify the grouped-branch
  refactor explicitly.
- Do not extend the cycle-shape framing to claim "no
  exchange" for ordered scalar aggregate. Ordered scalar
  has one exchange via ADR 010's wrap; the fragmenter
  routes through the generic producer/consumer path. The
  *aggregate* stays single-task, but the plan has a
  shuffle stage.
- Do not add a multi-process fixture only for HAVING-only.
  Single-task-stage routing of HAVING-only is exercised by
  in-process digest parity. The ordered cases DO warrant
  multi-process fixtures and ADR 018 ships them.
- Do not promote scalar aggregates to a two-phase
  partial/final distribution shape. Separate ADR.
- Do not lift the V1 exclusion §6 (`UnaryOp` over
  aggregate calls) opportunistically. Separate decision.
- Do not widen `bind_expr` to learn `Expr::AggregateCall`.
  ADR 017's posture applies verbatim.
- Do not extend `grouped_aggregate_signature` with
  `LinearRegress` / `ApproxQuantile` variants. Experimental
  kernels remain a kernel-graduation-block concern; V1
  exclusion §7 names the boundary.
- Do not rename `GroupedAggregateScope` to a broader name
  in ADR 018. The rename is a future-ADR concern when
  post-aggregate projection widens the scope into a richer
  output catalog.
- Do not redesign empty-input aggregate semantics in
  ADR 018. V1 exclusion §12 names this as inherited
  behavior; any future kernel-semantic change must be
  deliberate and separately pinned.

## ADR 019 handoff

ADR 019 selection starts from the post-ADR-018 remainder:

- **Aggregate-catalog widening (`AVG` / `MIN` / `MAX`).**
  One ADR per kernel under Wendelon-ADR-070 pattern. The
  scope, walker, and ambiguity classification are now
  proved against both grouped and scalar shapes; a new
  `AggregateFunction` variant + `GroupedAggregateSignature`
  variant + `lower_*_aggregate` arm composes mechanically.
- **Post-aggregate projection** (aliases, computed
  expressions, key subsets, reordered keys). The council
  deferred this pending output-alias schema work and a
  fragmenter decision for `Project` above grouped aggregate output.
  ADR 017's `GroupedAggregateScope` output-name multiset
  becomes the authoritative output-name catalog when this
  widens — and the cycle's third HAVING-attachment caller
  may emerge here, at which point ADR 018's inlined HAVING
  block can be revisited as a shared helper.
- **`DISTINCT`.** Requires loosening ADR 011's zero-
  aggregate reject; not pure composition.
- **`GROUP BY` over joins** — composes ADR 008 + ADR 011.
- **Two-phase partial/final aggregation.** Lifts the
  scalar single-stage ceiling and the grouped per-
  partition contract.
- **Kernel graduation (`LinearRegress` / `ApproxQuantile`
  / `TimeBucket`).** The bound-executor parity gap at
  `exec.rs:912-916` and `exec.rs:1035-1039` is the natural
  first evidence-pressure target.
- **Aggregate-kernel semantic changes.** ADR 018 pins
  empty-input behavior by inheritance; any future semantic
  change belongs to a kernel-focused ADR.
- **V1.x runtime-filter extensions** (Candidate C
  cluster).

**Code-evidence defects continue to outrank the printed
list when one surfaces.** Iter-1's sweep surfaced the
experimental-kernel parity gap; the gap is gated behind
the opt-in flag and belongs to the kernel-graduation
block, not ADR 019 as a substitute candidate.

The roadmap's `ADR 019 candidates` section owns the
forward-looking detail.
