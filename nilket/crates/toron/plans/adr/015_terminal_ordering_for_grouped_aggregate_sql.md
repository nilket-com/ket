# ADR 015: Terminal Ordering for Grouped Aggregate SQL

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude iter-4 with Codex iter-5 redlines
**Companion implementation plan:**
`plans/impl/015_terminal_ordering_for_grouped_aggregate_sql.md`

## Status

Promoted, pending implementation.

Three-way AI Swarm Council convergence reached across five
iteration rounds. Iteration artifacts retire when this plan is
promoted — git holds the narrative.

ADR 015 owns no independent bench artifact. It composes ADR 010
(`Sort`) and ADR 011 (`Aggregate`) into one nested execution shape
and adds the corresponding three-stage distributed fragmentation;
no new operator and no performance claim warrant a bench row.

Load-bearing antecedents: Codex iter-1's surfacing of the second
seam in the fragmenter (multi-exchange non-join reject), iter-2's
narrow-detector framing (an `ExecutionPlan`-level structural match
for the exact nested shape, placed before the generic
multi-exchange reject) and detailed required-evidence list
partitioned by layer (planner / binder-executor / fragmenter /
integration); Claude iter-1's "composition over representation"
framing and iter-2 concession on the fragmenter seam, iter-2
sharpening of ordinals as the load-bearing top-N form when
aggregate output names collide, iter-4 content draft folding the
council's positions into promoted-file shape; Gemini iter-2/3's
consolidation of the three-stage pipeline, V1-scope decision
(names + 1-based ordinals, no `ORDER BY <aggregate-call>`), and
exclusion list (`HAVING`, `DISTINCT`, `GROUP BY` over joins,
two-phase aggregation, new aggregate functions); Codex iter-5's
final redlines (don't pin internal helper names in the contract,
keep the detector shape as the load-bearing surface, don't
overpromise the exact rejection layer for aggregate-call sort
expressions).

## Goal

Enable top-level `ORDER BY` (with inherited `LIMIT` / `OFFSET`)
over grouped aggregate SQL, by composing the existing `Sort` and
`Aggregate` operators into one nested logical shape and fragmenting
it into three distributed stages.

ADR 010 introduced bound top-level ordering with terminal-gather
distribution. ADR 011 introduced single-table grouped aggregation
with a hash-partitioned exchange on the group key. Each shipped in
isolation; the cross-composition was explicitly rejected at two
seams. ADR 015 removes those two rejects with no new IR variant,
no new wire frame, and no broadening of either antecedent's surface.

## Scale boundary

**Primary:** Toron planner and distributed fragmenter composition
of two already-shipped operators.

In scope:

- removing the grouped-aggregate-`ORDER BY` reject in
  `SqlPlanner::lower_query`;
- a narrow fragmenter detector that matches the
  `Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)(input))))`
  shape, evaluated ahead of the generic multi-exchange reject;
- sort-key resolution against the `Aggregate` output schema (group
  keys first, then aggregate fields), inheriting ADR 010's binder
  semantics verbatim;
- three-stage `StagePlan` decomposition with a
  `upstream_stages == [aggregate_stage]` invariant on the terminal
  sort stage;
- planner / binder / fragmenter / integration test coverage.

Explicitly unchanged:

- ADR 010's `Sort` operator surface, sort-key binder semantics, and
  terminal-gather shape;
- ADR 011's `Aggregate` operator surface, projection-shape
  contract, hash-partitioned exchange on the group key, and the
  one-exchange fast path when no `ORDER BY` is present;
- the table-query wire frame, runtime-filter descriptor surface,
  PBC bytecode, and the ADR 013/014 application hooks;
- `JoinKind`, `JoinStrategy`, scheduler-barrier semantics,
  `TaskSpec`, `TaskFailureKind`, `BudgetDimension`, `MessageKind`,
  `TaskOutputBinding` variants;
- `RuntimeFilterDescriptor`, `RuntimeFilterKind`, and every
  ADR 006/013/014 carrier-and-application contract;
- the existing duplicate-name behavior of the binder.

No new IR variant, no new shuffle binding, no new stage role, no
new failure kind, and no new wire surface is reserved here. Every
extension below (`HAVING`, `DISTINCT`, post-aggregate projection,
expression sort keys, `ORDER BY <aggregate-call>`, `GROUP BY` over
joins, two-phase partial/final aggregation, new aggregate
functions, broadcast / outer / semi / anti / sort-merge / non-equi
join surface) is a separate future ADR.

## Context

Two load-bearing antecedents, two lines each:

- **ADR 010 — Top-Level Clause Accounting and Ordering.** Bound
  top-level `ORDER BY`/`LIMIT`/`OFFSET` distributes as
  `Sort(Exchange(1, [])(...))`; the binder resolves sort keys
  against `input.schema()` and accepts unqualified names + 1-based
  positive ordinals.
- **ADR 011 — Aggregate Projection Accounting and `GROUP BY`.**
  Single-table grouped aggregation distributes as
  `Aggregate(Exchange(K, group_expr)(scan))`; the projection must
  list group keys first in declared order, then aggregate calls;
  `HAVING`, `DISTINCT`, joins under `GROUP BY`, aliases on
  aggregates, and computed group keys are explicitly rejected.

The reject sites:

- `SqlPlanner::lower_query` — an explicit
  `"GROUP BY with ORDER BY is not supported"` block fires when the
  query body lowers to a grouped aggregate plan and the outer
  `Query` carries an `ORDER BY`.
- `terminal_sort_gather` in `distributed/fragment.rs` — recognizes
  the terminal-gather pattern only above joins; the
  `contains_join(input)` gate excludes everything else.
- `fragment_plan`'s generic non-join branch — rejects more than
  one `Exchange` with `MultipleExchangeBoundaries`.

The composition shape lowering already produces is
`Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)(scan))))`.
The planner reject is the *only* thing preventing the binder and
executor from accepting it; the fragmenter reject is the *only*
thing preventing the distributed scheduler from staging it.

## Decision

### 1. Remove the planner reject

In `SqlPlanner::lower_query`, remove the grouped-aggregate
`ORDER BY` reject. The existing `lower_query` flow already wraps
the inner plan with `LogicalPlan::Sort { input: LogicalPlan::
Exchange { partition_count: 1, key: [] }, order_by, limit, offset
}` when `ORDER BY` is present; once the reject is gone, this
naturally produces:

```text
Sort(Exchange(1, [])
  Aggregate(Exchange(K, group_expr)
    <single-table input>))
```

The planner-time grouped aggregate shape check may stay local to
`sql_planner.rs`; the fragmenter adds its own `ExecutionPlan`
structural detector for the bound nested shape (§5). The two
operate on different IR levels (`LogicalPlan` vs `ExecutionPlan`);
no shared helper is required.

### 2. Sort-key resolution against `Aggregate` output

ADR 010's binder already resolves sort keys *after* binding the
sort input and derives `output_schema = input.schema()`. For
ADR 015, the sort input *is* the `Exchange` above the `Aggregate`,
whose schema is exactly `Aggregate.output_schema` — the same
`aggregate_schema(group_expr, aggr_expr)` ADR 011 produces.

Sort keys therefore resolve against `[group_keys] ++
[aggregate_fields]` automatically:

- **By name** — unqualified column names match group-key field
  names directly, and match aggregate fields by their generated
  names (`count`, `sum`, `linreg`, `quantile` from
  `aggregate_field`).
- **By ordinal** — 1-based positive integer literals select the
  Nth output column via the existing ordinal-binding path.

No binder change. No widening of `lower_expr`. The promoted ADR
ships a planner test that exercises both forms on a grouped
aggregate output.

### 3. Aggregate-name ambiguity is unchanged; ordinals are the
load-bearing top-N form

The generated aggregate names exposed by `aggregate_field`
(`count`, `sum`, …) are *not* unique under repeated aggregate
calls. `SELECT region, COUNT(*), COUNT(other) FROM sales GROUP BY
region ORDER BY count DESC` produces two output fields named
`count`; the existing binder's name-resolution behavior under
collision (whatever ADR 010 left it as — typically
ambiguous-rejection) carries forward unchanged.

ADR 015 makes one **scope-discipline** call: ordinals are the
recommended sort-key form for multi-aggregate fixtures.
Name-resolution still works when the output name is unambiguous
(group keys, single-aggregate cases). The ADR does not widen
name-resolution, does not pin a new canonical aggregate-naming
contract, and does not commit to disambiguation behavior that
ADR 011 did not commit to.

### 4. `ORDER BY <aggregate-call>` is excluded from V1

Aggregate-call sort expressions such as `ORDER BY SUM(amount)`
reject as unsupported; ADR 015 does not add aggregate-call
recognition in an `ORDER BY` context. The exact rejection layer
(planner, binder, or expression-lowering) is implementation choice,
not contract: the contract is that the V1 surface does not accept
the form.

`ORDER BY 2 DESC LIMIT 10` covers the canonical top-N use case
("top regions by total amount") without widening the planner. The
promotion test fixture demonstrates both:

```sql
SELECT region, SUM(amount)
FROM sales
GROUP BY region
ORDER BY 2 DESC
LIMIT 10
OFFSET 1
```

### 5. Fragmenter — narrow detector before the multi-exchange
reject

`distributed/fragment.rs` adds a detector for the exact ADR 015
shape, evaluated **before** the generic non-join multi-exchange
reject in `fragment_plan`:

```text
Sort(Exchange(partition_count = 1, key = [])
  Aggregate(Exchange(partition_count = K, key = group_expr)
    <single-table input>))
```

On a match, the fragmenter emits exactly three `StagePlan`s:

- **Stage 0 — Producers.** Scan / filter / projection over the
  single-table input. Publishes the group-keyed exchange via
  `ShuffleProducer` with `partition_count = K, key = group_expr`.
- **Stage 1 — Grouped aggregate consumers.** One task per
  partition. `ShuffleConsumer` reads its hash partition from
  Stage 0; the task computes the final grouped aggregate; a
  `ShuffleProducer` publishes the result via the terminal-gather
  exchange (`partition_count = 1, key = []`).
- **Stage 2 — Terminal sort.** One task. `ShuffleConsumer` reads
  partition 0 from Stage 1's terminal-gather exchange; a single
  `Sort` task applies `ORDER BY` / `LIMIT` / `OFFSET`;
  `TaskOutputBinding::FinalResult` publishes results.

**Stage-dependency invariant:** the terminal sort stage's
`upstream_stages` is exactly `[Stage 1.id]`. The terminal sort
stage cannot begin until every Stage 1 task has published its
manifest — this is the stage barrier that prevents the final sort
from observing partial aggregate output.

The narrow detector framing — match the exact nested shape,
*before* the multi-exchange reject — is deliberate. The broader
multi-exchange guard stays in place for every other non-join
shape. The exception is exactly this composition. Anything else
with two non-join exchanges continues to reject. Internal helper
names and the specific reuse of `terminal_sort_gather` vs a new
builder are implementation choices; the contract is the detector
shape and the negative-shape preservation.

### 6. Preserved fast path

When the query has `GROUP BY` but no `ORDER BY`, ADR 011's
existing one-exchange shape is unchanged:

```text
Aggregate(Exchange(K, group_expr)(scan))
```

The narrow detector in §5 fires only on the nested-Sort-over-
Aggregate composition. ADR 011's two-stage fragmentation path
(producer → aggregate consumer with `FinalResult` binding) is
preserved verbatim. A promotion regression test pins this.

### 7. No new surface

ADR 015 introduces no new:

- IR variant (`LogicalPlan::Sort` and `LogicalPlan::Aggregate` are
  unchanged);
- `JoinKind`, `JoinStrategy`, or join surface;
- `MessageKind`, `TaskFailureKind`, `BudgetDimension`, or
  scheduler barrier kind;
- `TaskOutputBinding` variant;
- wire frame, runtime-filter descriptor field, or PBC opcode;
- aggregate function or aggregate-name contract;
- planner cost-model knob.

The composition rides on the existing `StagePlan.upstream_stages:
Vec<StageId>`, `ShuffleProducer`, and `ShuffleConsumer` surfaces.

## Consequences

- Analytical top-N queries are supported: `SELECT region,
  SUM(amount) FROM sales GROUP BY region ORDER BY 2 DESC LIMIT 10`
  lowers, fragments, and runs against the live in-process Wendelon
  harness.
- The fragmenter recognizes one additional nested topology
  (the `Sort(Exchange(1, [])(Aggregate(Exchange(K, group_expr)(...))))`
  shape) at exactly one new detection site.
- ADR 011's narrowness is preserved: `HAVING`, `DISTINCT`,
  computed keys, aliases, and post-aggregate projection still
  reject. ADR 015 is composition over the *current* ADR 011
  surface, not a widening of it.

## Promotion evidence

Implementation promotion requires:

1. **Planner** —
   - `SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY
     region` lowers to `Sort(Exchange(1, [])(Aggregate(Exchange(K,
     group_expr)(...))))`.
   - `SELECT region, SUM(amount) FROM sales GROUP BY region ORDER
     BY 2 DESC LIMIT 10 OFFSET 1` is accepted with the same shape.
   - `SELECT region, SUM(amount) FROM sales GROUP BY region ORDER
     BY SUM(amount)` is rejected (V1 exclusion; exact rejection
     layer is implementation choice).
   - Every existing ADR 011 reject remains: `GROUP BY` over joins,
     `HAVING`, `DISTINCT`, reordered keys, aliases, computed keys.
   - The one-exchange no-`ORDER BY` fast path is unchanged.

2. **Binder / executor** —
   - Sort ordinals resolve against `Aggregate` output, not source
     input — `ORDER BY 2` over `(region, SUM(amount))` sorts by
     the aggregate result column, not the second source column.
   - Output is sorted *after* aggregation — the final result is
     post-aggregate row order, not pre-aggregate input order.
   - Duplicate generated aggregate names remain ambiguous via the
     existing binder behavior — no silent field choice.

3. **Fragmenter** —
   - The composition emits exactly three `StagePlan`s.
   - Stage 0 publishes a group-keyed shuffle (`partition_count =
     K, key = group_expr`).
   - Stage 1 consumes the group-keyed shuffle and publishes a
     terminal-gather shuffle (`partition_count = 1, key = []`).
   - Stage 2 has `upstream_stages == [Stage 1.id]`, exactly one
     `ShuffleConsumer`, and `TaskOutputBinding::FinalResult`.
   - A *different* non-join two-exchange shape (e.g. `Sort(Exchange
     (1, [])(Project(Exchange(K, key)(Scan))))`) still rejects
     with `MultipleExchangeBoundaries` — the detector is narrow,
     not a generic two-exchange permit.

4. **Integration** —
   - In-process SQL result digest-matches a hand-built logical
     plan with the same `Sort(Exchange(Aggregate(Exchange(Scan))))`
     shape.
   - Multi-process distributed execution returns deterministic,
     digest-equal results for the ordered grouped aggregate
     fixture across multiple worker counts.
   - `LIMIT` and `OFFSET` compose correctly under the terminal
     sort stage.

No bench artifact. ADR 015 makes no performance claim; it unlocks
a composed SQL surface with correctness evidence.

## What not to do

- Do not implement `ORDER BY <aggregate-call>` syntax
  (`ORDER BY SUM(x)`).
- Do not implement `HAVING` or `DISTINCT`.
- Do not enable `GROUP BY` over joins.
- Do not add two-phase partial/final aggregation.
- Do not add new aggregate functions, aliases, computed group
  keys, group-key subsets, or expression sort keys.
- Do not pin a new canonical aggregate-naming contract — the V1
  recommendation is ordinals when aggregate names collide.
- Do not widen the fragmenter's multi-exchange guard beyond the
  one nested shape in §5; in particular, do not generalize the
  detector to "any two exchanges with a single terminal sort."
- Do not introduce new IR variants, wire frames, message kinds,
  failure kinds, or budget dimensions for this composition — the
  existing surfaces are sufficient.
- Do not add a planner cost-model opt-in or knob; ADR 015 is
  unconditional once the planner reject is removed.

## ADR 016 handoff

ADR 016 selection starts from the adjacent SQL-surface candidates
(`HAVING`, `DISTINCT`, `ORDER BY <aggregate-call>`,
post-aggregate projection, `GROUP BY` over joins) plus any fresh
evidence (silent-drop / inert-feature defects) that outranks them.
The roadmap's `ADR 016 candidates` section owns the forward-looking
detail.
