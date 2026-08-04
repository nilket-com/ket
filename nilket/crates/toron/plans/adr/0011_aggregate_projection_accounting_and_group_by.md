# ADR 011: Aggregate Projection Accounting and GROUP BY SQL Reachability

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/0011_aggregate_projection_accounting_and_group_by.md`

## Status

Promoted to `plans/adr/` and implemented. Promotion evidence is
plan-shape inspection plus result/digest parity against
hand-built `Aggregate` plans plus distributed stage-shape
inspection plus preserved architecture guards (see "Promotion
evidence"). No bench-results artifact — ADR 011 introduces no
operator and makes no performance claim.

Three-way AI Swarm Council convergence reached across five
iteration rounds (`plans/wip/{claude,codex,gemini}_iter_*`).
Codex iter-5 and Gemini iter-5 ratified the frozen contract; the
primary draft was handed to Claude. Iteration artifacts retire
when this plan is promoted — git holds the narrative.

Load-bearing antecedents: Codex's `Candidate F` framing, the
two-branch-then-unconditional selection rule, the single-shuffle
distributed lock (no two-phase partial aggregation), the
syntactic + structural single-table guard pair, and the final
contract consolidation; Gemini's integrity-over-performance
selection argument and D-round ratifications; Claude's
projection-item silent-drop finding, the exact-projection-shape
sharpening, the `GROUP BY` over-a-join fragmenter-composition
finding, and the consolidated drafting basis.

This ADR is a **frontend-planner surface extension** that closes
a live V1 "First Query with Integrity" correctness defect one
level below ADR 010: `SqlPlanner::lower_select` detects aggregate
functions in a SELECT projection and lowers them to
`LogicalPlan::Aggregate`, but **silently discards every
projection item that is not a recognized aggregate function**.
`SELECT region, SUM(amount) FROM sales` parses, lowers, binds,
distributes, and executes today **with the `region` column
dropped on the floor**. ADR 011 makes every projection item in an
aggregate SELECT either intentionally lowered or intentionally
rejected, and — having closed the silent drop — wires the
already-implemented grouped-aggregate operator to the routine
`GROUP BY` SQL form it is currently unreachable from.

After ADR 011, Toron may still reject a great deal of aggregate
SQL — but it no longer *accepts* an aggregate SELECT while
silently discarding part of the user's projection, and routine
`SELECT key, COUNT(*) FROM t GROUP BY key` executes correctly.

## Goal

Two coupled halves:

1. **Aggregate projection-item accounting (the integrity fix).**
   No accepted aggregate SELECT may contain a projection *item*
   that the planner silently ignores. Once the projection is
   classified as aggregate, every item is *accounted for* —
   lowered as a group key or an aggregate call, or rejected
   through `FrontendError::Unsupported`. This is the ADR 010
   accounting invariant pushed one level down: ADR 010 made every
   `Query` / `Select` *field* accounted; ADR 011 makes every
   *item within* the `Select.projection` field accounted, for the
   aggregate case.
2. **`GROUP BY` SQL reachability (the feature).** Lower a narrow
   top-level `GROUP BY` form to the existing
   `LogicalPlan::Aggregate` / `ExecutionPlan::Aggregate`
   operator — executable today but unreachable from `GROUP BY`
   SQL, exactly as ADR 008's `LeftSemi` / `LeftAnti` operators
   were unreachable from `WHERE [NOT] EXISTS` before ADR 009.

## Scale boundary

**Primary:** frontend planner. The contract change is
concentrated in `SqlPlanner::lower_select` — the projection-item
classification rewrite and the `GROUP BY` lowering, including the
single-table syntactic and structural guards and the explicit
group-keyed `Exchange` emission.

**Secondary:** the existing binder / executor aggregate path and
the existing one-exchange non-join distributed fragmenter path,
both used **unchanged**. The binder already binds aggregate
calls and builds aggregate output schemas; the executor already
runs grouped and global aggregation; the fragmenter already
splits a single non-join `Exchange` boundary and recurses
through `ExecutionPlan::Aggregate` as a pass-through. ADR 011
adds no operator, no IR variant, and no stage-building logic — it
makes the planner emit a plan shape the existing machinery
already handles.

**Not in scope:**
- **No new IR variant.** `LogicalPlan::Aggregate` and
  `ExecutionPlan::Aggregate` already exist. `LogicalPlan` stays
  9 variants; `ExecutionPlan` stays 7. This is *narrower* than
  ADR 010, which added `Sort`.
- **No wire change.** No new `MessageKind`, `TaskFailureKind`,
  `BudgetDimension`, telemetry field, or Wendelon scan-contract
  change.
- **No two-phase partial aggregation.** V1 lowers grouped SQL to
  a single raw-row hash exchange keyed on the group columns;
  every row of a group lands in one consumer partition and the
  existing final grouped aggregate runs over complete groups.
  Partial → repartition → final aggregation is a *performance*
  contract — it requires partial/final aggregate-state
  semantics, state schemas, nullable `SUM` state, `COUNT(expr)`
  merge behavior, overflow timing, and future-kernel state
  rules — and is a named, evidence-driven follow-on.
- **No `GROUP BY` over joins.** A grouped aggregate above a
  distributed join produces a post-join group-keyed exchange the
  current join fragmenter buries inside the join-consumer task
  rather than splitting into a real stage; if the group key
  differs from the join partitioning key, results become
  partition-local instead of global. V1 input is a single base
  table; `GROUP BY` over a join is a named follow-on.
- **No `GROUP BY` over decorrelated `EXISTS` / `NOT EXISTS`.**
  ADR 009's `WHERE [NOT] EXISTS` rewrite introduces `LeftSemi` /
  `LeftAnti` joins under the hood — the same buried-exchange
  hazard. Rejected by the structural guard.
- **No `GROUP BY ... ORDER BY`.** The composition produces two
  exchange boundaries on a non-join plan
  (`Sort(Exchange₁(Aggregate(Exchange_group(Scan))))`) the
  non-join fragmenter cannot split. `GROUP BY` alone and
  `ORDER BY` alone are each supported; the composition is a
  named follow-on.
- **No `HAVING`** — a post-aggregate predicate carries its own
  binding policy (aggregate expressions not in the projection,
  aggregate-name dedup, aliases); aggregate-path follow-on.
- **No `DISTINCT`** — `DISTINCT` is `GROUP BY <all output
  columns>` with no aggregates; aggregate-path follow-on.
- **No aggregate-empty `GROUP BY`** (`SELECT region FROM t
  GROUP BY region`) — structurally `DISTINCT`-adjacent; V1
  requires at least one aggregate call.
- **No post-aggregate projection.** V1 accepts only the exact
  projection shape that maps 1:1 onto the bound `Aggregate`
  output schema. Projected group-key subsets, reordering,
  aggregate-before-key ordering, aggregate aliases, computed
  group keys, and group ordinals are one named follow-on
  surface.
- **No new aggregate functions and no experimental-kernel
  graduation.** ADR 011 wires the existing stable set; `avg` /
  `min` / `max` and any `LinearRegress` / `ApproxQuantile`
  graduation are out of scope.

## V1 contract summary

- **Aggregate projection-item accounting.** When
  `lower_select`'s projection scan detects at least one aggregate
  function, the projection is an *aggregate projection*. Every
  item in an aggregate projection is then classified
  exhaustively: a group-key column reference, a recognized
  aggregate call, or `FrontendError::Unsupported`. Grouped SQL
  admits only stable-set aggregate calls; existing global
  experimental aggregate calls keep their binder-gated behavior.
  No item is silently omitted.
  Mixed aggregate / non-aggregate projection without `GROUP BY`,
  wildcard mixed with an aggregate, an unsupported function mixed
  with an aggregate, and malformed aggregate arity / modifiers
  all **loud-reject** instead of producing a silently-narrowed
  `Aggregate` plan.
- **`GROUP BY` → existing `LogicalPlan::Aggregate`.** A top-level
  `GROUP BY` over a single base table is lowered to
  `LogicalPlan::Aggregate` whose `group_expr` is the `GROUP BY`
  key list, over an explicit `LogicalPlan::Exchange` keyed on the
  group columns. No new IR variant.
- **Accepted grouped SQL shape.**
  ```sql
  SELECT <gk-1>, ..., <gk-n>, <agg-1>, ..., <agg-m>
  FROM   <single base table>
  WHERE  <already-supported predicate>      -- optional
  GROUP BY <gk-1>, ..., <gk-n>
  ```
  with `m >= 1`.
- **Group keys.** Simple unqualified input column names only.
  Computed group expressions, qualified names, group ordinals
  (`GROUP BY 1`), and aliases **reject**.
- **Exact projection shape.** The projection must be **exactly**
  `[every GROUP BY key, in GROUP BY order]` followed by
  `[the aggregate calls, in projection order]`. A projected
  strict subset of the group keys, any reordering, an
  aggregate-before-key ordering, or an aliased item **rejects**.
  This is the unique shape that maps 1:1 onto the bound
  `ExecutionPlan::Aggregate` output schema
  (`[all group_expr] ++ [aggr_expr]`) with no terminal
  post-aggregate `Project` and no aliasing — the ADR 010
  "output-schema-only, no hidden machinery" discipline applied to
  the aggregate surface.
- **At least one aggregate call required.** A `GROUP BY` with no
  aggregate call (`SELECT region FROM t GROUP BY region`) is
  `DISTINCT`-adjacent and **rejects**.
- **Stable aggregate set only for grouped SQL.** `COUNT(*)`,
  `COUNT(expr)`, and `SUM(expr)` are accepted in grouped SQL.
  Experimental kernels (`LinearRegress`, `ApproxQuantile`) keep
  their existing global-aggregate SQL behavior behind the binder
  gate, but are **not** graduated into grouped SQL by ADR 011.
- **Single-table input, two guards.** A **syntactic** guard
  (`from.len() == 1` and the single `TableWithJoins` carries no
  `joins`) plus a **structural** guard (after `WHERE` lowering,
  the grouped aggregate input plan contains no
  `LogicalPlan::Join`). The structural guard is load-bearing:
  ADR 009's `WHERE [NOT] EXISTS` decorrelation rewrites to
  `LeftSemi` / `LeftAnti` joins, so a syntactically single-table
  query can still carry a `Join` below the aggregate.
- **Single terminal group-keyed exchange.** The planner emits one
  explicit `LogicalPlan::Exchange { key: group_expr }` below the
  `Aggregate`; the fragmenter never synthesizes it. The existing
  one-exchange non-join fragmenter path produces a producer stage
  and N group-partitioned consumer tasks, each running the
  existing final grouped aggregate over complete groups. No
  two-phase shape, no new stage-building logic.
- **Global aggregates preserved.** `SELECT COUNT(*), SUM(x) FROM
  t` and the other already-accepted global-aggregate forms stay
  **byte-identical**. The projection-item accounting rewrite
  changes which projections are *rejected*, not how an accepted
  global aggregate lowers.
- **No semantic fallback.** Every shape outside the supported
  class continues through `FrontendError::Unsupported`. ADR 011
  can only widen what executes correctly and convert silent drops
  to loud rejections.
- **No new contract surface.** No IR variant, no wire,
  `TaskFailureKind`, `BudgetDimension`, `JoinKind`,
  `JoinStrategy`, `MessageKind`, or Wendelon change. The IR
  variant-count architecture guards and the ADR 010
  clause-accounting source guard stay green unchanged.

## Context

The roadmap's post-ADR-009 candidate list — A multi-tenant
governance, B wire efficiency, C V1.x runtime-filter extensions,
D worker-side Rune UDFs, E executor upgrade — rotated forward
unchanged as the ADR 011 candidate set, with B and C explicitly
gated on the pending ADR 006 Wendelon production row, A wanting
multi-tenant workload evidence it does not have, and D / E
reserved or demoted.

The council's discipline overrode the printed list, exactly as it
did for ADR 010: *"Evidence drives scope"* includes the code, not
only the prior ADR's bench artifact. Reading
`SqlPlanner::lower_select` exposed a second silent-wrong-results
defect of the ADR 010 class, one level below the clause
accounting ADR 010 closed:

```rust
for item in projection {
    if let SelectItem::UnnamedExpr(SqlExpr::Function(f)) = item {
        // ... recognize COUNT / SUM / LINEAR_REGRESS / APPROX_QUANTILE,
        //     push into aggr_exprs, set is_aggregate = true ...
        // unrecognized function name => `_ => {}` — ignored
    }
    // non-function items => not inspected at all
}
if is_aggregate {
    return Ok(LogicalPlan::Aggregate {
        input: Box::new(plan),
        group_expr: vec![],          // always empty — GROUP BY unreachable
        aggr_expr: aggr_exprs,       // only the recognized aggregates
    });
}
```

`SELECT region, SUM(amount) FROM sales` sets `is_aggregate =
true`, builds `aggr_exprs = [Sum(amount)]`, and returns at the
`is_aggregate` branch — **`region` is silently discarded**. The
query returns a single global sum where the user asked for a
per-region breakdown. This is the failure mode the roadmap's
standing guardrail names — *"Silently-wrong-results is the
failure mode ADR 002 paid complexity to prevent"* — and a direct
violation of the V1 "First Query with Integrity" target.

ADR 010 explicitly named this exact gap as deferred work — *"mixed
aggregate / non-aggregate projection, wildcard mixed with explicit
items … aggregate-path-adjacent work, not closed here."* ADR 010's
accounting invariant was field-level by construction;
`Select.projection` kept its prior item handling. ADR 011 closes
the projection-item level for the aggregate case.

Closing the silent drop forces the second half. Once every
projection item in an aggregate SELECT must be accounted, the
honest accounting of a non-aggregate column item *is* "this is a
group key" — which is meaningless unless `GROUP BY` is reachable.
The grouped-aggregate operator already exists and is exercised by
hand-built logical plans and a Wendelon storage-parity test; it
is simply unreachable from `GROUP BY` SQL because `lower_select`
loud-rejects the clause. ADR 011 is therefore ADR-009-shaped
(executable substrate unreachable from routine SQL) *and*
ADR-010-shaped (a live silent-drop integrity defect, with Phase 1
closing the class before the feature surface widens).

The council scored this candidate against A and selected it: it
is V1 (a correctness bug, not a missing feature), pure-Toron with
zero Wendelon dependency, and selected on code evidence the A–E
list predated. The ADR 006 Wendelon production row stays parallel
work and feeds ADR 012.

## Decision

### 1. Aggregate projection-item accounting

`lower_select`'s projection handling is restructured so that
detecting an aggregate projection and *classifying its items* are
one exhaustive pass, not a recognize-some-ignore-the-rest scan. A
projection is an **aggregate projection** if it contains at least
one recognized aggregate function call. For an aggregate
projection, every item is classified into exactly one bucket:

- **group-key item** — a bare unqualified column reference
  (`SelectItem::UnnamedExpr(SqlExpr::Identifier(_))`); valid only
  if it also appears in `GROUP BY` at the matching position (see
  Decision §3);
- **aggregate-call item** — a recognized aggregate function call.
  In grouped SQL this is limited to the stable set (`COUNT(*)`,
  `COUNT(expr)`, `SUM(expr)`). In existing global-aggregate SQL,
  experimental aggregate functions keep their pre-ADR behavior
  behind the existing binder gate (Phase 0 audits that gate);
- **rejected** — anything else: a non-aggregate computed
  expression, a wildcard (`SELECT *`), an unrecognized or
  unsupported function, an aliased item, a qualified column, a
  malformed aggregate call (wrong arity, unsupported modifier).
  `FrontendError::Unsupported`.

The pre-ADR-011 behavior — detect one aggregate, return an
`Aggregate` plan built only from recognized aggregates, silently
dropping the rest — is replaced. No projection item disappears.

This accounting is **projection-item-level by construction** for
the aggregate case. A non-aggregate projection (no aggregate
function present) keeps its existing ADR 008 / 009 `Project`
lowering unchanged — ADR 011 does not touch the non-aggregate
path.

### 2. `GROUP BY` lowering shape

When `group_by` is non-empty, `lower_select`:

1. applies the **syntactic single-table guard**: `from.len() ==
   1` and `from[0].joins.is_empty()`; otherwise
   `FrontendError::Unsupported`;
2. lowers `FROM` (a single `TableFactor::Table`) and the optional
   `WHERE` exactly as today;
3. applies the **structural single-table guard**: the resulting
   input plan contains no `LogicalPlan::Join` — this rejects a
   `WHERE [NOT] EXISTS` query whose ADR 009 decorrelation
   produced a `LeftSemi` / `LeftAnti` join;
4. classifies the projection (Decision §1) and validates it
   against the `GROUP BY` key list (Decision §3);
5. builds:

```rust
LogicalPlan::Aggregate {
    input: Box::new(LogicalPlan::Exchange {
        input: Box::new(<input plan after FROM/WHERE>),
        partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT,
        key: group_expr.clone(),   // hash-partition on the group keys
    }),
    group_expr,                    // the GROUP BY key list
    aggr_expr,                     // the projection's aggregate calls
}
```

The planner emits the group-keyed `Exchange` **explicitly**,
matching the established pattern where SQL join lowering emits
explicit hash-partitioned exchanges (Decision §7) and the
fragmenter never synthesizes an exchange.
`DEFAULT_AGGREGATE_PARTITION_COUNT` may reuse the existing join
partition-count default or be a named aggregate constant —
implementer discretion (impl Phase 0).

### 3. Exact projection shape against the `GROUP BY` key list

The bound `ExecutionPlan::Aggregate` output schema is
`[all group_expr fields] ++ [aggr_expr fields]` — every `GROUP
BY` key, then the aggregates. For the SQL projection to map 1:1
onto that schema with no terminal `Project` and no aliasing, the
projection must be **exactly**:

`[every GROUP BY key, in GROUP BY order]` ++
`[the aggregate calls, in projection order]`

The planner validates: the first `n` projection items (where `n`
is the `GROUP BY` arity) are bare column references equal,
position for position, to the `GROUP BY` keys; the remaining
items are all aggregate calls. Any deviation —

```sql
SELECT a, COUNT(*)        FROM t GROUP BY a, b;  -- subset of keys
SELECT b, a, COUNT(*)     FROM t GROUP BY a, b;  -- reordered keys
SELECT COUNT(*), a        FROM t GROUP BY a;     -- aggregate before key
SELECT a, COUNT(*) AS n   FROM t GROUP BY a;     -- alias
```

— **loud-rejects**. These are the named `post-aggregate
projection` follow-on, not SQL-completeness failures: they are
the explicit "no hidden post-aggregate `Project`" boundary for
V1.

### 4. At least one aggregate call

A `GROUP BY` projection with zero aggregate calls
(`SELECT region FROM t GROUP BY region`) is pure deduplication —
structurally `GROUP BY <all output columns>` with no aggregates,
which is `DISTINCT`. It **rejects**. Folding it in would drag the
deferred `DISTINCT` decision (duplicate column names, null
grouping, output-order policy) in through a side door. V1
requires `m >= 1` aggregate calls.

### 5. Single-table input — syntactic and structural guards

V1 grouped input is one base table plus an optional supported
`WHERE` filter. The two guards are defense-in-depth, and the
**structural guard is the load-bearing one**:

- `SELECT a, COUNT(*) FROM t1 JOIN t2 ... GROUP BY a` is caught
  syntactically (`from[0].joins` non-empty);
- `SELECT a, COUNT(*) FROM t1 WHERE EXISTS (SELECT 1 FROM t2
  WHERE t2.k = t1.k) GROUP BY a` is syntactically single-table —
  `from.len() == 1`, no `joins` — but ADR 009 decorrelation
  rewrites the `WHERE` to a `LeftSemi` join, so the input plan
  carries a `LogicalPlan::Join` the structural guard rejects.

This prevents a post-join group-keyed exchange from being buried
in the join-consumer task plan, where it would never become a
real distributed stage and grouped results could be
partition-local rather than global.

### 6. No two-phase partial aggregation

V1 lowers grouped SQL to a single raw-row hash exchange keyed on
the group columns. The exchange partitions raw input rows by
group key; every row of a given group lands in one consumer
partition; each consumer runs the existing final grouped
aggregate over its complete groups; the consumer outputs are
disjoint final results, concatenated like any other unordered
distributed result. This is **correct without aggregate-state
merge semantics** and uses the existing one-exchange fragmenter
path unchanged.

Two-phase partial → repartition → final aggregation
(`Aggregate(Exchange(Aggregate(Scan)))`) is a *performance*
optimization that reduces shuffle bytes by pre-aggregating before
the exchange. It is not a planner-reachability change: it creates
a new semantic contract — partial aggregate states must be
mergeable as inputs to a final aggregate, which the current
`AggregateFunction` set is not typed for (raw-input vs
aggregate-state rows, nullable `SUM` state vs nullable output,
`COUNT(*)` partial-count merge vs row count, integer-overflow
timing across phases, partial-state output-schema naming). ADR
011 ships the correct single-shuffle shape and names two-phase
aggregation as an evidence-driven follow-on, the same posture
ADR 010 took shipping one terminal gather-sort and deferring
distributed range sort.

### 7. Distributed shape: existing one-exchange fragmenter path

The grouped plan `Aggregate(Exchange{key: group_expr}(Scan|Filter))`
has exactly one exchange boundary on a non-join plan.
`fragment_plan` already handles this: `count_exchanges` and
`find_exchange` recurse through `ExecutionPlan::Aggregate` as a
pass-through, the single boundary splits into a producer stage
(the group-keyed exchange) and a consumer stage of N tasks each
running `Aggregate` over its shuffle partition. Because the
exchange is hash-keyed on the group columns, each consumer task
receives complete groups and its output is final and disjoint.

ADR 011 adds **no fragmenter code** — it relies on stage-shape
inspection gates to assert the planner emits the explicit
group-keyed exchange and the existing fragmenter preserves it
into exactly one producer + one consumer stage with no new
exchange mode.

### 8. `GROUP BY` composition rejects — no fragmenter rewrite

`GROUP BY` composes with neither a join input nor a top-level
`ORDER BY` in V1, for the same structural reason: each produces a
multi-exchange shape the current fragmenter handles only for
join-specific paths.

- `GROUP BY` over a join (explicit or ADR-009-decorrelated) —
  rejected by Decision §5.
- `GROUP BY ... ORDER BY` — `Sort` (ADR 010) emits its own
  terminal `Exchange { partition_count: 1 }`, so the composition
  is `Sort(Exchange₁(Aggregate(Exchange_group(Scan))))`: two
  exchange boundaries on a non-join plan, which `fragment_plan`
  rejects with `MultipleExchangeBoundaries`. ADR 011 loud-rejects
  the composition in `SqlPlanner::lower_query`, after
  `lower_select` has identified a grouped aggregate plan and
  before ADR 010's `Sort` wrapper is built. `GROUP BY` alone is
  ADR-011-supported; `ORDER BY` alone stays ADR-010-supported;
  the composition is a named follow-on. Teaching the non-join
  fragmenter path the agg + terminal-sort multi-exchange shape is
  real scope for its own ADR.

### 9. Unsupported shapes reject — no semantic fallback

Every shape outside the supported class continues through
`FrontendError::Unsupported`. A rejected shape behaves
byte-identically to a parse-then-reject — except aggregate
projections which today are *silently narrowed* now *loudly
reject*. No new error variant — the existing unsupported-
expression class carries every rejection (ADR 008 P0.8 / ADR 009
/ ADR 010 discipline).

### 10. Surface invariants

`LogicalPlan` stays 9 variants; `ExecutionPlan` stays 7 —
`Aggregate` already exists in both. `LogicalPlan::Join` /
`ExecutionPlan::Join` stay single-variant; `JoinKind` stays
eight; `JoinStrategy` stays two; `TaskFailureKind` stays seven;
`BudgetDimension` stays seven; no `MessageKind`, telemetry field,
or Wendelon scan-contract change. ADR 011 **adds no architecture
guard and updates none** — it asserts the ADR 003 / 004 / 008 /
010 guard set, including the ADR 010 no-`..` clause-accounting
source guard, stays green unchanged. This is the narrowest
contract surface of any ADR since ADR 009.

## Hard gates

### Projection-item accounting — no silent drop (1-5)

1. `mixed_aggregate_and_non_aggregate_projection_without_group_by_rejects`
   — `SELECT region, SUM(amount) FROM sales` rejects, no item
   dropped.
2. `wildcard_mixed_with_aggregate_rejects` — `SELECT *, COUNT(*)
   FROM sales` rejects.
3. `unsupported_projection_function_mixed_with_aggregate_rejects`.
4. `malformed_aggregate_arity_or_modifiers_reject_cleanly` — no
   panic, no silent narrowing.
5. `existing_global_aggregates_byte_identical_after_accounting`
   — `SELECT COUNT(*), SUM(amount) FROM sales` and peers
   unchanged; **runs first** (ADR 009 / 010 R1 posture).

### `GROUP BY` lowering + projection shape (6-11)

6. `group_by_lowers_to_aggregate_over_group_keyed_exchange`.
7. `group_by_requires_at_least_one_aggregate_call` —
   `SELECT region FROM t GROUP BY region` rejects.
8. `group_by_projection_must_be_exact_full_key_list_then_aggregates`.
9. `group_by_projection_subset_reorder_alias_or_aggregate_first_rejects`.
10. `group_by_accepts_only_stable_aggregate_set` — `COUNT(*)`,
    `COUNT(expr)`, `SUM(expr)` accepted; experimental kernels
    stay binder-gated, not graduated.
11. `group_by_keys_must_be_simple_unqualified_input_columns` —
    computed keys, qualified names, and group ordinals reject.

### Single-table input guards (12-13)

12. `group_by_over_explicit_join_rejects` — the syntactic guard.
13. `group_by_over_decorrelated_exists_rejects` — the structural
    guard; `SELECT a, COUNT(*) FROM t1 WHERE EXISTS (...) GROUP
    BY a` rejects because ADR 009 produced a `Join` below the
    aggregate input.

### Distributed parity + stage shape (14-16)

14. `distributed_group_by_has_single_group_keyed_exchange` —
    exactly one producer stage (the group-keyed exchange) and one
    consumer stage of N tasks carrying `ExecutionPlan::Aggregate`,
    output `FinalResult`; no new exchange mode.
15. `grouped_aggregate_sql_matches_hand_built_aggregate_plan` —
    result/digest parity against an equivalent hand-built
    `LogicalPlan::Aggregate`, for `COUNT(*)`, `COUNT(expr)`, and
    `SUM(expr)`, including null handling.
16. `grouped_aggregate_matches_single_process_and_distributed`.

### Composition rejects (17-19)

17. `group_by_with_order_by_rejects` — the two-non-join-exchange
    composition.
18. `having_is_unsupported_not_dropped`.
19. `distinct_is_unsupported_not_dropped`.

### Regression / architecture (20-21)

20. `existing_sql_queries_byte_identical_after_adr_011` — full
    pre-ADR-011 SQL surface unchanged; **runs first**.
21. `architecture_guards_unchanged` — `LogicalPlan` 9-variant and
    `ExecutionPlan` 7-variant guards stay green *without update*;
    `Join` single-variant, `JoinKind` eight, `JoinStrategy` two,
    `TaskFailureKind` seven, `BudgetDimension` seven, and the
    ADR 010 no-`..` clause-accounting source guard all stay
    green; no new architecture guard added.

## Promotion evidence

ADR 011 introduces no operator and makes **no performance
claim** — it closes a correctness defect and wires an existing
operator to SQL. Promotion evidence is **plan-shape inspection**
(the `GROUP BY` lowering shape, gate 6), **result/digest parity**
against hand-built `Aggregate` plans (gates 15-16), **distributed
stage-shape inspection** for the single group-keyed exchange
(gate 14), and the **preserved architecture guards** (gate 21).
No bench-results artifact — structurally identical to the ADR 009
promotion shape, and narrower than ADR 010 (no IR variant, no
guard update).

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

## Open questions

All policy forks are locked (Decision §1–§10). The residual items
are true Phase 0 implementation-audit details, recorded in the
impl plan — none is a contract fork:

- exact `sqlparser` `GroupByExpr` and projection `SelectItem` AST
  shapes for the exact-projection-shape check;
- `DEFAULT_AGGREGATE_PARTITION_COUNT` — reuse the join default or
  introduce a named aggregate constant;
- confirm the SQL path for `LINEAR_REGRESS` / `APPROX_QUANTILE`
  already respects the existing `allow_experimental_kernels`
  binder gate — if it does not, that adjacent gap is flagged, not
  necessarily fixed inside ADR 011;
- confirm existing distributed global-aggregate behavior is
  correct as-is; the gate-5 / gate-20 byte-identity gates protect
  it, and a correctness issue, if any surfaces, is handled then
  rather than expanding scope by default.

## What Not To Do

- **Don't let an aggregate projection silently narrow.** Every
  item in an aggregate projection is lowered or loud-rejected —
  the projection-item accounting invariant is the load-bearing
  integrity fix.
- **Don't add an IR variant.** `LogicalPlan::Aggregate` and
  `ExecutionPlan::Aggregate` already exist; `LogicalPlan` stays
  9, `ExecutionPlan` stays 7. ADR 011 adds and updates no
  architecture guard.
- **Don't place a terminal `Project` above `Aggregate`.** V1
  accepts only the exact `[keys] ++ [aggregates]` projection
  shape that maps 1:1 onto the bound `Aggregate` output schema;
  post-aggregate projection is a named follow-on.
- **Don't accept a projected subset or reorder of group keys.**
  Exact full key list, in `GROUP BY` order, before the
  aggregates.
- **Don't accept aggregate-empty `GROUP BY`.** `m >= 1` aggregate
  calls required; the bare form is `DISTINCT`-adjacent.
- **Don't rely on the syntactic single-table guard alone.** The
  structural guard (no `LogicalPlan::Join` below the aggregate
  input) is load-bearing — ADR 009 `WHERE EXISTS` decorrelation
  produces a join inside a syntactically single-table query.
- **Don't ship two-phase partial aggregation.** V1 is one raw-row
  group-keyed exchange; partial/final aggregate-state semantics
  are a named follow-on.
- **Don't let the planner emit `GROUP BY` over a join or
  `GROUP BY ... ORDER BY`.** Both are multi-exchange shapes the
  non-join fragmenter cannot split; loud-reject in the planner,
  do not grow the fragmenter.
- **Don't synthesize the group-keyed `Exchange` in the
  fragmenter.** The planner emits it explicitly, matching SQL
  join lowering.
- **Don't implement `HAVING` or `DISTINCT` in ADR 011.** They
  loud-reject; implementing them is aggregate-path follow-on
  work.
- **Don't graduate experimental aggregate kernels.** `LinearRegress`
  and `ApproxQuantile` stay behind the existing binder gate;
  ADR 011 wires the existing stable set only.
- **Don't add a new aggregate function.** `avg` / `min` / `max`
  are their own trivially-justified follow-on, not ADR 011
  scope.
- **Don't add a new `TaskFailureKind`, `BudgetDimension`, wire,
  or Wendelon surface.** ADR 011's contract surface is
  frontend-planner only.

## Rejected alternatives

- **Suspend ADR 011 / wait for the Wendelon ADR 006 row.**
  Considered; rejected. The A–E candidate list predated the
  projection-item silent-drop defect; "evidence drives scope"
  includes current code (the ADR 010 precedent). The ADR 006
  Wendelon row stays parallel work and feeds ADR 012.
- **Promote Candidate A (multi-tenant governance).** Considered
  as the strongest unblocked printed candidate; rejected. The
  roadmap's own A entry wants real multi-tenant workload evidence
  before pinning per-tenant budgets / admission / scheduler
  allocation; nothing post-ADR-010 changed that posture, and a
  live V1 integrity defect outranks a feature whose shape is
  evidence-hungry.
- **Two-phase partial aggregation in V1.** Considered (Gemini
  iter-1 raised the `Aggregate(Exchange(Aggregate(Scan)))`
  audit); rejected (Codex iter-2). It is a performance contract
  requiring partial/final aggregate-state semantics, not a
  planner-reachability change; the single raw-row group-keyed
  exchange is correct without it.
- **Loose projection ordering ("keys first" without exact full
  key list).** Considered (council iter-3 phrasing); rejected
  (Claude iter-3, ratified Codex / Gemini iter-4). A projected
  subset or reorder of group keys forces a terminal
  post-aggregate `Project` and the aliasing question with it; the
  exact full key list is the unique zero-`Project` shape.
- **`GROUP BY` over an "already-supported join shape."**
  Considered (Codex iter-3 phrasing); rejected (Claude iter-3,
  Codex iter-4 self-correction). The join fragmenter buries a
  post-join group-keyed exchange inside the join-consumer task;
  if the group key differs from the join key, results become
  partition-local. V1 input is a single base table.
- **Syntactic single-table guard only.** Considered; rejected
  (Codex iter-4). ADR 009 `WHERE EXISTS` decorrelation produces a
  `Join` inside a syntactically single-table query; the
  structural guard (no `LogicalPlan::Join` below the aggregate
  input) is required.
- **Restricted `HAVING` fold** (referencing only already-projected
  aggregates / keys). Considered (Claude iter-1 / iter-3);
  rejected (Codex / Gemini iter-3, Claude conceded iter-4). Even
  a restricted fold carries aggregate-name-dedup and binding
  edge-case surface; a clean hard defer keeps ADR 011 surgical.
- **Aggregate-empty `GROUP BY`** (`SELECT region FROM t GROUP BY
  region`). Considered as a micro-fork (Codex iter-3); rejected.
  Structurally `DISTINCT`; folding it in drags the deferred
  `DISTINCT` decision through a side door. V1 requires `m >= 1`.
- **`GROUP BY ... ORDER BY` in V1.** Considered; rejected. The
  composition is two non-join exchange boundaries the fragmenter
  cannot split; teaching the non-join fragmenter the agg +
  terminal-sort shape is its own ADR.
- **A new IR variant or architecture guard for ADR 011.**
  Considered against the ADR 008 / 010 pattern; rejected.
  `Aggregate` already exists in both IRs; ADR 011 changes no
  serialized shape and adds no variant — there is nothing to
  pin. It asserts the existing guard set stays green.
- **Graduating `LinearRegress` / `ApproxQuantile` while wiring
  `GROUP BY`.** Considered as adjacent scope; rejected. Kernel
  graduation is the roadmap's "one ADR per kernel" pattern with
  its own bench evidence; ADR 011 wires the existing stable set
  and leaves the experimental gate intact.

## Council convergence summary

Three-way agreement reached across five iteration rounds. Codex
iter-5 and Gemini iter-5 ratified the frozen contract; the
primary draft was handed to Claude.

| Decision                                                    | Status    |
|-------------------------------------------------------------|-----------|
| Select ADR 011 from code evidence, not the A–E list         | converged |
| Selection basis: aggregate projection-item silent-drop defect | converged |
| Title: aggregate projection accounting + `GROUP BY` reachability | converged |
| Scale boundary: frontend planner; existing binder/exec/fragmenter | converged |
| Projection-item accounting for aggregate SELECT lists       | converged |
| `GROUP BY` → existing `LogicalPlan::Aggregate`, no new IR    | converged |
| Exact `[all keys in order] ++ [aggregates]` projection shape | converged |
| At least one aggregate call required                        | converged |
| Stable aggregate set only; experimental kernels stay gated  | converged |
| Single-table input: syntactic + structural guards           | converged |
| Structural guard catches ADR 009 decorrelated `EXISTS`      | converged |
| Single group-keyed exchange; existing one-exchange fragmenter | converged |
| No two-phase partial aggregation                            | converged |
| `GROUP BY` over joins rejects; named follow-on              | converged |
| `GROUP BY ... ORDER BY` rejects; named follow-on            | converged |
| `HAVING` / `DISTINCT` loud-reject, not implemented           | converged |
| No new IR / wire / `TaskFailureKind` / `BudgetDimension`     | converged |
| No new or updated architecture guard                        | converged |
| Phase 1 is an independently reviewable correctness commit   | converged |
| Plan-shape + parity + stage-shape = promotion evidence, no bench | converged |
| ADR 006 Wendelon row stays parallel, feeds ADR 012          | converged |

All policy forks are locked. Residual items are Phase 0
implementation-audit details, recorded in the impl plan.
