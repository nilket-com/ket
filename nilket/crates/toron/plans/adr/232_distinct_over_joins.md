# Toron ADR 232: DISTINCT over Joins

**Status:** Promoted. **Migration:** `plans/impl/232_distinct_over_joins.md`.
**Category:** Toron core query surface — widen ADR 231 `SELECT DISTINCT` to join inputs by
lifting two conservative fences. **No new execution operator, fragmenter case, IR variant,
Rune code, Wendelon pushdown, durable key, system table, or optimizer-evidence surface.**

## Why this cut exists

ADR 231 landed top-level single-source DISTINCT through a bind-eliminated `LogicalPlan::Distinct`
marker and named DISTINCT-over-joins as the next cut. The only thing keeping
`SELECT DISTINCT … FROM a JOIN b` dark is two conservative first-cut fences; the dedup machinery
already handles join output. This cut lifts the fences and proves the marker is the right
altitude over joins.

## Grounding (verified)

- The fences are conservative, side-by-side, no fundamental-reason comment: the SQL classifier
  rejects with `"DISTINCT over joins is not supported"` (sql_planner.rs:3449), and the binder
  repeats it in `distinct_input_unsupported_message` (binder.rs:2562:
  `LogicalPlan::Join { .. } => Some("DISTINCT over joins is not supported")`).
- **Correct cross-partition dedup is by construction.** The 231 marker lowering (binder.rs:1144)
  builds `group_expr = all_output_columns_program(input.schema())` then
  `Exchange { key = group_expr }` (binder.rs:1153) → empty-aggregate. Over a join the physical
  shape is `Join (partitioned by join key) → Exchange(key = all output columns) → Aggregate`; the
  all-columns `Exchange` **re-partitions the join output by the full dedup key**, so duplicates
  in different post-join partitions collapse — regardless of the join's own partitioning. No
  fragmenter change.
- The fragmenter has the structural traversal needed for aggregate above joined input:
  `replace_exchange_with_scan` and `replace_first_join` recurse through
  `ExecutionPlan::Aggregate`. The existing `query_coordinator_executes_aggregate_and_join_ctas`
  fixture proves aggregate CTAS and join CTAS separately, **not** aggregate-over-join as one
  query; therefore the distributed cross-partition DISTINCT-over-join gate below is required, not
  optional. The `Distinct` marker is already transparent in the join walkers
  (`plan_contains_join` / `logical_plan_contains_join` recurse it), so a joined input needs no
  new logical altitude.
- Rune needs no code change: `RuneDataFrame::distinct()` already wraps any frame (incl. a join);
  the binder fence is the only thing rejecting it.

## Decisions (promotion-locked)

**D1 — Lift both fences; keep everything else.** Remove the SQL classifier join reject
(sql_planner.rs:3449) and replace the binder fence's `LogicalPlan::Join { .. }` rejection
(binder.rs:2562) with recursive inspection of `left` and `right`. Joined scans become legal, but
joined aggregate inputs remain rejected. **Keep** the `LogicalPlan::Aggregate { .. }` arm
(`"DISTINCT over aggregate inputs is not supported"`) and every other 231 fence
(derived/EXISTS, GROUP BY/HAVING, window, set operations, `DISTINCT ON`). Lifting only the SQL
fence is insufficient — Rune and hand-authored logical plans hit the binder fence.

**D2 — Correct cross-partition dedup by construction; no physical change.** The marker's
unconditional all-columns `Exchange` re-partitions the join output by the full dedup key. No new
`ExecutionPlan` op, no fragmenter case, no exchange-placement special-casing. The distributed
cross-partition gate proves this composition in the real fragmenter/coordinator path.

**D3 — DISTINCT stays post-join, post-projection.** SQL lowers the join tree, then the normal
projection, then wraps the projected plan in `LogicalPlan::Distinct`. `SELECT DISTINCT *` over a
join dedups the full joined output. No per-table or input-side dedup is introduced.

**D4 — Outer joins are in scope and are the altitude proof.** LEFT/RIGHT/FULL outer-join
fixtures where null-padded rows survive the join and are then deduped by the final key. This is
the discriminator that dedup sits *above* the join, not pushed onto a build/probe input. (Outer
joins add no DISTINCT complexity beyond 231's NULL-equal dedup.)

**D5 — Ordinal-safe over joined output.** A joined projection with duplicate output names (e.g.
`a.id` and `b.id`) dedups by position, extending 231's ordinal-safe group keys to joined output.

**D6 — Frontend-neutral parity over joins.** `ctx.table("a").join(b, …).project([…]).distinct()`
binds through the same marker and produces the **identical bound plan and rows** as the SQL twin
(the 231 cardinal, extended; no Rune code change).

**D7 — No physical / evidence / optimizer surface change.** No `ExecutionPlan::Distinct`,
fragmenter case, Wendelon pushdown, wire tag, system table, durable key, receipt gate, or
optimizer-policy consumer. A core query-surface widening, not an optimizer cut.

## Semantics

```sql
-- now supported
SELECT DISTINCT a.region, b.tier FROM sales a JOIN dim b ON a.k = b.k
SELECT DISTINCT * FROM sales a LEFT JOIN dim b ON a.k = b.k
-- still rejected (retained fences)
SELECT DISTINCT count(*) FROM a JOIN b ON a.k = b.k       -- aggregate input
SELECT DISTINCT x FROM (SELECT x FROM t) s                -- derived
SELECT region FROM a JOIN b ON a.k=b.k GROUP BY region    -- public GROUP BY over joins
```

## Honesty gates (discriminating)

- `select_distinct_inner_join_dedups_projected_join_rows`.
- `select_distinct_left_outer_join_dedups_after_null_padding` — altitude proof (fails if dedup is
  pushed below the join).
- `select_distinct_join_star_dedups_full_join_output`.
- `select_distinct_join_duplicate_output_names_group_by_position` (D5).
- **`select_distinct_join_dedups_across_partitions`** — *cardinal*, distributed: duplicates whose
  join-key partitions differ still collapse (proves the all-cols re-exchange, D2). Single-node
  tests alone would pass even if the re-exchange were wrong.
- `select_distinct_join_order_by_limit_runs_after_dedup`.
- `rune_distinct_over_join_matches_sql_plan_and_rows` (D6).
- `distinct_over_aggregate_inputs_still_rejects` / `distinct_in_derived_table_and_exists_still_reject`
  / `public_group_by_without_aggregate_still_rejects` (retained fences).
- `distinct_over_joined_aggregate_input_still_rejects` — proves the binder join lift still
  recurses into children instead of changing `LogicalPlan::Join { .. }` to unconditional `None`.
- `distinct_join_lowers_to_existing_empty_aggregate_no_new_execution_op` (D2/D7).

## Architecture guards (promotion-locked)

- `adr232_distinct_over_joins_lifts_sql_and_binder_fences_together`
- `adr232_distinct_join_binder_lift_preserves_nested_aggregate_fence`
- `adr232_distinct_join_remains_bind_eliminated_empty_aggregate`
- `adr232_distinct_join_dedup_is_post_join_via_all_column_exchange`
- `adr232_distinct_join_ordinal_safe`
- `adr232_distinct_retains_aggregate_derived_exists_and_groupby_fences`
- `adr232_distinct_join_adds_no_physical_or_optimizer_surface`

**Reconciled prior guards (named):**
- ADR 231 `adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates` → retargeted to drop
  the *join* fence expectation while keeping every other first-cut fence.
- ADR 231 marker / ordinal-safe / GROUP-BY-unrelaxed / frontend-neutral guards → unchanged.
- Optimizer arc (ADR 216→230) → untouched.

## Consensus (transparent)

3-of-3. Claude iter-0 proposed DISTINCT-over-joins (Option A) and flagged the fragmentation
question; Codex caught the **second (binder) fence** and argued **outer joins as the altitude
proof**; Gemini concurred on lifting both fences + composability + outer-join dedup. Claude iter-1
contributed the decisive grounding — the marker's unconditional all-columns `Exchange` makes
cross-partition dedup correct **by construction** if the existing aggregate-above-join traversal
holds — and the distributed cross-partition cardinal gate that proves that composition directly.
Claude conceded its iter-0 "inner-only first" to Codex's outer-joins-as-proof. No residual
divergence.

## Forward arc

```text
231 SELECT DISTINCT (single source)
-> 232 DISTINCT over joins (this)
-> DISTINCT in derived tables / EXISTS
-> set operations (UNION/INTERSECT/EXCEPT)
-> window functions
```

GROUP-BY-over-joins is the same `Exchange→Aggregate`-over-join mechanism but a separate feature
(real aggregates + HAVING); it is a named follow-on, not folded here.

## Deferred (named)

GROUP BY over joins; DISTINCT in derived/EXISTS; DISTINCT with aggregate projections / GROUP BY /
HAVING / windows; `DISTINCT ON`; set operations; window functions.
