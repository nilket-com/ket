# Toron ADR 234: DISTINCT Completion

**Status:** Promoted. **Migration:** `plans/impl/234_distinct_completion.md`.
**Category:** Toron core query surface — close the DISTINCT surface by lifting its remaining fences:
**(A)** aggregate-argument DISTINCT (`count(DISTINCT x)`) via a pre-aggregate desugar onto the
existing `LogicalPlan::Distinct`, and **(B)** DISTINCT over/inside derived tables and inside EXISTS.
**No new execution operator, IR variant or `AggregateFunction` flag, fragmenter code, Rune code,
Wendelon pushdown, durable key, system table, or optimizer-evidence surface.**

## Why this cut exists

231→232→233 built the DISTINCT/aggregate-over-composite substrate: `SELECT DISTINCT`, DISTINCT over
joins, grouped real aggregates over joins. The DISTINCT surface still has three conservative fences:
aggregate-argument DISTINCT (`count(DISTINCT x)`), DISTINCT over/inside derived tables, and DISTINCT
inside EXISTS. All three are *fence-lifts onto the one engine the arc already built* —
`LogicalPlan::Distinct` — with no new machinery. `count(DISTINCT x)` in particular is an extremely
common SQL gap and the natural completion of the aggregate-*function* surface after 233 completed
aggregates-over-*joins*.

## Grounding (verified)

- **No aggregate DISTINCT flag exists and none is added.** `AggregateFunction` (ir.rs:858) =
  `CountAll/Count/Sum/Min/Max/Avg/LinearRegress/ApproxQuantile`. The only `Distinct` is the dedup
  node `LogicalPlan::Distinct` (ir.rs:962). So `count(DISTINCT x)` is a *logical desugar*, not a
  kernel mode.
- **The desugar must be owned by `lower_select`, not `lower_aggregate_projection`.** The latter
  (sql_planner.rs:3749) discovers aggregate calls + builds `aggr_expr`/final projection but **never
  owns the input plan**; `lower_select` assembles `Aggregate ← Exchange(key=g) ← plan` after it
  returns (sql_planner.rs:3544-3552, and the global branch at :3589). The input rewrite (insert
  `Distinct`) therefore lives in `lower_select`.
- **NULL trap — `COUNT(DISTINCT x) → Count(x)`, never `count(*)`.** `LogicalPlan::Distinct` keeps one
  `(g, NULL)` row. `AggregateFunction::CountAll` counts all rows (exec.rs:374); `Count` skips nulls
  (exec.rs:387-392, `if !arr.is_null(row)`). `COUNT(DISTINCT x)` = count of distinct **non-null** x,
  so the dedup is followed by `Count(x)` — `CountAll` would overcount by one whenever x is null in a
  group.
- **The aggregate `count(DISTINCT x)` fence** is the `duplicate_treatment.is_some()` reject in
  `aggregate_function_args` (sql_planner.rs:6916, the aggregate-discovery path via :4202).
  `function_positional_args` has a parallel modifier reject (:6674), but that path is for scalar
  helpers like IF/COALESCE/NULLIF, not aggregate discovery, and stays unchanged.
- **Derived DISTINCT is ordinal-safe as-is.** Lifting `distinct_requested && table_factor_is_derived`
  (:3449) wraps the lowered derived relation in `Distinct`, and the binder builds dedup group keys by
  **ordinal** via `all_output_columns_program(&input.schema())` (binder.rs:1144) — the ADR 232
  duplicate-name-safe path, not by-name resolution.
- **DISTINCT inside derived / EXISTS** are single fields on the shape validators:
  `validate_derived_select_shape` (:7045) and `validate_exists_select_shape` (:7084), each
  `select.distinct.is_some()`.

## Decisions (promotion-locked)

**D1 — Aggregate DISTINCT is a pre-aggregate desugar owned by `lower_select`.** Discovery returns a
planner-private `DistinctAggregateRewrite { dedup_exprs, rewritten_aggr_expr }`; `lower_select` wraps
the input as `Project([group_keys ∪ {x}]) → Distinct` *below* the existing group-key
`Exchange → Aggregate` (grouped branch) or directly below the global `Aggregate`. No
`AggregateFunction` flag, no new node.

**D2 — `f(DISTINCT x) → f(x)` over the dedup, with `COUNT → Count(x)` (the NULL pin).**
`COUNT(DISTINCT x)→Count(x)`; `SUM(DISTINCT x)→Sum(x)`; `MIN/MAX(DISTINCT x)→Min/Max(x)`
(distinct-inert, surface-only, share the dedup + validation). Never `CountAll`. This is the cut's
load-bearing correctness pin.

**D3 — One-dedup-exact scope.** Accepted iff every aggregate in SELECT *and* HAVING is DISTINCT over
the **same single plain-column** argument, function ∈ {COUNT, SUM, MIN, MAX}; with or without
GROUP BY; with or without a join.

**D4 — Composes with the 232/233 substrate, free.** The dedup is `LogicalPlan::Distinct` (232's
distributed dedup, its own `(g,x)` shuffle); the group aggregate is 233's `Exchange(key=g) →
Aggregate`. `count(DISTINCT x)` over a join is just 232 feeding 233 — two existing shuffle stages,
validated distributed (>1 worker).

**D5 — Deferred is one mechanism: multi-dedup fan-out.** Mixed distinct + non-distinct
(`count(DISTINCT x), sum(y)`), multiple distinct args (`count(DISTINCT x), count(DISTINCT y)`),
`AVG(DISTINCT x)` (= `Sum/Count` decomposition, not ride-free), and computed-arg DISTINCT
(`count(DISTINCT x+1)`) all need fan-out-and-recombine and stay rejected with sharper messages.

**D6 — B/derived: lift two fences, reuse `Distinct`, ordinal-safe.** Outer DISTINCT over a derived
table (lift :3449) and DISTINCT inside a derived subquery (drop `select.distinct.is_some()` from
`validate_derived_select_shape`) both reuse `wrap_distinct_if_needed → Distinct`; the outer case is
ordinal-safe via binder.rs:1144. Retain the other derived shape fences (one FROM source, no
joins-inside-derived [233], no nested derived, no windows).

**D7 — B/EXISTS: DISTINCT is existence-inert → accept-and-strip.** Drop `select.distinct.is_some()`
from `validate_exists_select_shape`; the decorrelator builds **no** `Distinct` in the EXISTS
subquery (dedup cannot change existence). Retain GROUP BY/HAVING/join/window fences inside EXISTS.

**D8 — Keep every other fence.** `DISTINCT with GROUP BY` (:3414) and `DISTINCT with aggregate
projections` (:3421) — top-level `SELECT DISTINCT … <agg>`, a *different* construct — stay rejected;
`DISTINCT ON` (:3363); window-DISTINCT (:3426); grouping-set modifiers (:6106).

**D9 — No new physical / optimizer surface.** Reuse `LogicalPlan::Distinct` + `ExecutionPlan`
dedup/aggregate. No new `ExecutionPlan` variant, fragmenter case, or optimizer-evidence change.

## Semantics

```sql
-- now supported
SELECT count(DISTINCT x) FROM t                              -- global, dedup x then Count(x)
SELECT g, count(DISTINCT x) FROM t GROUP BY g                -- dedup (g,x) then Count(x) per g
SELECT g, sum(DISTINCT x) FROM a JOIN b ON … GROUP BY g      -- rides 232 Distinct + 233 Aggregate
SELECT DISTINCT region FROM (SELECT region FROM sales) d     -- outer DISTINCT over derived
SELECT r FROM (SELECT DISTINCT region AS r FROM sales) d     -- DISTINCT inside derived
SELECT 1 FROM t WHERE EXISTS (SELECT DISTINCT 1 FROM u …)    -- DISTINCT inert, stripped
-- still rejected (deferred = multi-dedup fan-out)
SELECT count(DISTINCT x), sum(y) FROM t                      -- mixed distinct/non-distinct
SELECT count(DISTINCT x), count(DISTINCT y) FROM t           -- multiple distinct args
SELECT avg(DISTINCT x) FROM t                                -- AVG(DISTINCT) not ride-free
-- still rejected (retained, different construct)
SELECT DISTINCT region, count(*) FROM sales GROUP BY region  -- top-level DISTINCT + aggregate
```

## Honesty gates (discriminating)

- **`count_distinct_ignores_null`** — `count(DISTINCT x)` with NULLs present: `Count(x)` after dedup,
  not `CountAll`. *Cardinal* (fails under the obvious `count(*)` rewrite).
- `count_distinct_grouped_dedups_per_group`; `sum_distinct_grouped_matches_manual_dedup`;
  `min_max_distinct_match_plain_min_max` (surface-only ride-free).
- **`count_distinct_over_join_merges_across_partitions`** — distributed: a group's distinct x values
  produced by >1 worker yield the correct count (the 233-cardinal lesson — genuine cross-producer
  dedup-then-count, fixture must split a group's distinct values across workers).
- `global_count_distinct_no_group_by_works`.
- `mixed_distinct_and_plain_aggregate_is_explicit_unsupported`;
  `multiple_distinct_arguments_are_explicit_unsupported`; `avg_distinct_is_explicit_unsupported`.
- `outer_distinct_over_derived_is_ordinal_safe` (the `SELECT DISTINCT x, x FROM (… AS x, … AS x)`
  position pin); `distinct_inside_derived_lowers_to_existing_marker`;
  `distinct_inside_exists_is_accepted_and_stripped` (digest == plain EXISTS; no Distinct in semi-join).
- Retained: `select_distinct_with_group_by_still_rejects`,
  `select_distinct_with_aggregate_projection_still_rejects`.

## Architecture guards (promotion-locked)

- `adr234_distinct_aggregate_rewrites_to_project_distinct_then_existing_aggregate`
- `adr234_distinct_aggregate_keeps_aggregate_function_enum_flagless`
- `adr234_count_distinct_rewrites_to_count_arg_not_count_all` (structural NULL-trap pin)
- `adr234_distinct_aggregate_retains_multi_dedup_fanout_fences`
- `adr234_derived_distinct_reuses_232_ordinal_safe_binder`
- `adr234_exists_distinct_is_inert_and_decorrelates_without_distinct_plan`
- `adr234_adds_no_physical_fragmenter_optimizer_or_wendelon_surface`

**Reconciled prior guards (named):**
- The aggregate `duplicate_treatment` reject in `aggregate_function_args` (:6916) → retargeted:
  accept the one-dedup-exact case, reject the multi-dedup-fan-out cases with sharper messages.
- The scalar `function_positional_args` duplicate-treatment/clauses reject (:6674) → unchanged; ADR
  234 does not admit scalar-function DISTINCT/modifier syntax.
- ADR 231/232 DISTINCT marker / ordinal-safe binder guards → reused unchanged (now also serve the
  aggregate-dedup and derived/EXISTS paths).
- ADR 233 aggregate-over-join / post-join exchange guards → reused (count(DISTINCT) over join rides
  them).
- Optimizer arc (216→230) → untouched.

## Consensus (transparent)

3-of-3. Claude iter-0 proposed the topic menu and led the A+B "DISTINCT completion" framing. Codex
iter-1 grounded the two load-bearing corrections — the desugar is owned by `lower_select` (not
`lower_aggregate_projection`, which never owns the input plan), and `COUNT(DISTINCT x)→Count(x)` not
`count(*)` for NULL correctness — and moved `AVG(DISTINCT)`/computed-arg into the deferred fan-out.
Claude iter-2 verified both against the code (exec.rs:374/387, binder.rs:1144, sql_planner.rs:3544)
and converged. Gemini iter-1 independently converged on every load-bearing point: desugar in the
planner not the kernel, the `Count(x)` NULL pin, derived ordinal-safe dup-name pin, EXISTS
accept-and-strip, and the single-arg scope with mixed/multiple/AVG deferred. No residual divergence.

## Forward arc

```text
231 SELECT DISTINCT -> 232 DISTINCT over joins -> 233 join-scoped aggregation completion
-> 234 DISTINCT completion (this: distinct aggregates + derived/EXISTS DISTINCT)
-> set operations (UNION/INTERSECT/EXCEPT)   [235 candidate — new LogicalPlan SetOp variant]
-> multi-dedup distinct aggregates (mixed / multiple / AVG)
-> window functions
```

## Deferred (named)

Mixed distinct + non-distinct aggregates; multiple distinct arguments; `AVG(DISTINCT x)`;
computed-arg DISTINCT — all the **multi-dedup fan-out** mechanism. Plus: `DISTINCT ON`; grouping
sets / ROLLUP / CUBE; set operations; window functions.
