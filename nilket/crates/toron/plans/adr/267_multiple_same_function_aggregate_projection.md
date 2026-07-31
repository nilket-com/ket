# ADR 267 — Multiple same-function aggregate projection

## Status

Proposed.

## Context
q01's measured first wall (ADR 266): `SELECT …, SUM(a), SUM(b) … GROUP BY` rejects at PLAN with
`ambiguous aggregate projection`. Root cause is a frontend **output-naming** bug, not an execution limit:
ADR 023's `AggregateOutputCatalog` names each aggregate output by the function's natural name
(`aggregate_output_name`: `SUM→"sum"`, `AVG→"avg"`, …). `resolve_natural` (`sql_planner.rs:7083`) finds
the entry **by signature** (so `SUM(a)`/`SUM(b)` are distinct), then bails `ResolveOutcome::Ambiguous`
solely because that natural name is shared (`natural_output_name_counts["sum"] > 1`). Collides **with or
without** SELECT aliases.

> **Implementation reconciliation (seal):** the plan asserted "the binder already synthesizes unique
> per-aggregate names downstream (`__md_b0_a0`…), so this is purely the frontend rewrite being
> over-conservative." That holds only for ADR 240's distinct-rewrite path. The **general** grouped-aggregate
> path re-derives output field names in the binder's `aggregate_schema` (`binder/aggregate.rs`) straight
> from the natural name (`SUM→"sum"`), so two `SUM`s produce two `"sum"` fields there too. The frontend
> rename alone left the binder re-colliding (q01 surfaced `UnknownColumn { "__agg_0" }`). The fix therefore
> applies the **same** position-keyed disambiguation in both places — see Decision 3.

## Decision

1. **Disambiguate by a synthesized, internal, stable aggregate key (Fork 1a).** Each distinct-signature
   aggregate entry resolves to its own column via a synthesized **internal** name (deterministic — natural
   name + a stable per-entry uniqueness key, e.g. the entry index), so the projection rewrite maps every
   `SUM(expr)` call to its own output. The internal name is **never the user-visible label**: when a SELECT
   alias exists the final projection column is the alias; a single un-aliased aggregate keeps its natural
   name (`"sum"`) as today; only genuinely-ambiguous un-aliased same-function aggregates fall back to the
   synthesized internal name as their final label (deterministic, documented).
2. **Fix at the naming layer, covering all three resolvers (Fork 2a).** The same shared-name ambiguity is
   in `resolve_natural` (projection + HAVING) and the aggregate-call branch of `resolve_for_order_by`.
   `ORDER BY SUM(b)` must resolve by the aggregate signature to `SUM(b)`'s unique internal output name,
   not through the old visible-name vector where multiple unaliased `SUM(...)` entries all look like
   `"sum"`. Disambiguating the output names once resolves all three uniformly — don't leave a twin gap
   one resolver over.
3. **Naming layer, frontend + binder (no exec / IR change).** The disambiguation lives in two name-deriving
   sites that must agree: `sql_planner`'s `AggregateOutputCatalog` (the projection rewrite) **and** the
   binder's `aggregate_schema` (the `Aggregate` node's output fields). Both rename colliding same-natural-name
   aggregates to `__agg_<i>`, keyed on the **aggregate's position** — identical between the two because
   `aggr_expr` is built from the catalog entries in order — so the rewritten references resolve to the
   binder's columns. No execution-path, `ExecutionPlan`, or `LogicalPlan` IR change: the aggregate *values*
   are unchanged and `LogicalPlan::Aggregate` gains no naming field (still derived). The SELECT `Project` on
   top applies aliases (user-visible) exactly as before. Pre-existing group-key/output-label clashes remain
   fenced as genuine ambiguity.

## Watch-items (operator) → guards
- Synthesized names are **internal + stable**: deterministic across runs (so architecture guards can pin
  them), prefixed/shaped so they're unmistakably internal, and **not surfaced as a final projection label
  whenever an alias is present**. A discriminating test asserts the aliased projection's output columns are
  the aliases (no internal name leaks).
- No execution-path change: this is a naming fix; the aggregate values are unchanged. The binder change is
  confined to `aggregate_schema` field naming (the same `__agg_<i>` disambiguation), not the kernels.

## Acceptance
- `SELECT …, SUM(a), SUM(b) … GROUP BY` — **aliased and un-aliased** — plans, binds, and executes with
  correct, distinct per-aggregate results.
- An ORDER-BY-by-same-function-aggregate and a HAVING-by-same-function-aggregate case resolve (the fix
  claims those paths — they must be tested). The ORDER BY case must be result-discriminating: ordering by
  `SUM(b)` beside a sibling `SUM(a)` proves it references the second aggregate's unique output, not the
  first aggregate's still-natural `"sum"` label.
- Gauge: q01 advances from `blocked(ambiguous_aggregate_projection@plan)` to the measured
  `blocked(unsupported_decimal_avg@bind)` (266's decimal `+`/`-` now exercised in the q01 path) — or
  deeper. q06 unchanged.

## Deferred (named)
- **ADR 268 — AVG(Decimal):** decimal division (SUM/COUNT) + finalize/rounding policy; q01's next wall
  once 267 lands (`unsupported_decimal_avg`, already classified).

## Why toron-home
Toron frontend aggregate-projection accounting. No cross-ring surface.
