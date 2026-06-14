# Toron ADR 233: Join-Scoped Aggregation Completion

**Status:** Promoted. **Migration:** `plans/impl/233_join_scoped_aggregation_completion.md`.
**Category:** Toron core query surface — complete aggregate-over-joins by lifting the two grouped
`GROUP BY over joins` fences so real aggregates (count/sum) + HAVING run over joins through ADR
232's post-join exchange split. **No new execution operator, IR variant, fragmenter code, Rune
code, Wendelon pushdown, durable key, system table, or optimizer-evidence surface.**

## Why this cut exists

ADR 232 lifted DISTINCT over joins and proved the post-join all-columns `Exchange` must split
into a real shuffle stage before final aggregation — a verified **aggregate-over-join substrate**.
But DISTINCT is an *empty-aggregate* consumer: it dedups keys and carries **no aggregate calls**
across the shuffle. Leaving only DISTINCT under-tests that substrate. 233 completes it: lift the
grouped `GROUP BY over joins` fences so ordinary aggregates (count/sum) + HAVING run over joined
rows, validating that **the group-key shuffle co-locates all rows for a group before real aggregate
state is computed** — the case the empty-aggregate path never exercised. GROUP BY over a join is
also a very common SQL gap.

## Grounding (verified)

- The grouped fences are conservative and `group_keys.is_some()`-gated:
  `"GROUP BY over joins is not supported"` at sql_planner.rs:3452 (pre-FROM) and 3525 (post-WHERE/
  decorrelation, `plan_contains_join`). No fundamental-reason comment; side-by-side with the
  DISTINCT fences ADR 232 lifted.
- **Global aggregates over joins already work** — being `group_keys.is_some()`-gated, the fences
  do not catch `SELECT count(*) FROM a JOIN b` (no GROUP BY); it flows through the global branch
  (`AggregateSqlMode::Global`, `group_expr: vec![]`, sql_planner.rs:3583). So 233 lifts only the
  *grouped* (keyed) fences; global-aggregate-over-join is a **regression pin**, not new work.
- The grouped aggregate lowers to `Exchange(key = group keys) → Aggregate`
  (`group_by_lowers_to_aggregate_over_group_keyed_exchange`, group_by_test.rs:30).
- ADR 232's `leading_exchange_before_join` (fragment.rs:264) is **generic** — it splits *any*
  `Exchange` above a join into a `ShuffleProducer`/consumer stage keyed by that exchange's key. So
  the group-key exchange over a join is split into a real shuffle boundary by the existing code.
- **HAVING composes as a Filter** — the aggregate lowering already wraps HAVING as
  `LogicalPlan::Filter { input: aggregate, predicate }` (sql_planner.rs:3600). Once the grouped
  fence lifts, HAVING-over-join filters after the grouped aggregate; no separate fence.

## Decisions (promotion-locked)

**D1 — Lift the two grouped GROUP-BY-over-join fences (3452, 3525); reuse 232's split.** No new
lowering, no new fragmenter code. The grouped `Exchange(group-keys) → Aggregate` is unchanged;
232's generic `leading_exchange_before_join` makes the group-key exchange a shuffle boundary over
the join.

**D2 — Real aggregates validate the substrate (the "Completion" point).** The cardinal is a
*real* aggregate (count/sum) over a distributed join where a group's rows are produced by multiple
join workers: the final total must be correct, proving the group-key shuffle co-locates rows
before aggregation — not just that keys dedup (which is all 232's empty-aggregate DISTINCT
proved). `>1` worker exercised.

**D3 — HAVING in scope.** GROUP BY + HAVING over a join filters after the grouped aggregate via
the existing Filter-on-aggregate lowering. No new fence to lift; pinned by a discriminator.

**D4 — Outer joins in scope (altitude proof).** GROUP BY over LEFT/RIGHT/FULL outer joins
aggregates null-padded buckets correctly — the discriminator that grouping sits *above* the join.

**D5 — Compose with existing join filter-pushdown.** GROUP BY over a join must not bypass the
WHERE-into-join pushdown (`all_inner_join_chain` / `join_operator_kind_for_filter_pushdown`,
sql_planner.rs:3454); the join still pushes its predicates, the grouped aggregate sits above.

**D6 — Decorrelated EXISTS is in scope for the outer GROUP BY.** The second grouped fence is after
WHERE lowering and decorrelation. A query whose WHERE EXISTS becomes a supported semi/anti join
and is then grouped must be accepted and tested; otherwise only the early explicit-join fence was
really lifted. GROUP BY inside the EXISTS subquery itself remains rejected by the existing EXISTS
shape validator.

**D7 — Keep every other fence.** Public no-aggregate `GROUP BY` still rejects (231 D4);
`DISTINCT with GROUP BY` still rejects; grouping sets, DISTINCT aggregates, window functions,
joins inside derived tables, GROUP BY inside EXISTS subqueries, and derived/EXISTS DISTINCT stay
out. Plain aggregate derived tables already work and are not a retained fence. This is grouped
aggregation over joins, not a DISTINCT+aggregate fusion or a subquery cut.

**D8 — No new physical op / optimizer surface.** Reuse `ExecutionPlan::Aggregate` + ADR 232's
post-join exchange split. No new `ExecutionPlan` variant, no optimizer-evidence change.

## Semantics

```sql
-- now supported
SELECT a.region, count(*) FROM sales a JOIN dim b ON a.k = b.k GROUP BY a.region
SELECT b.tier, sum(a.amt) FROM sales a LEFT JOIN dim b ON a.k=b.k GROUP BY b.tier HAVING sum(a.amt) > 0
-- already worked (regression pin)
SELECT count(*) FROM sales a JOIN dim b ON a.k = b.k          -- global aggregate, no GROUP BY
-- still rejected (retained fences)
SELECT a.region FROM sales a JOIN dim b ON a.k=b.k GROUP BY a.region    -- no aggregate
SELECT DISTINCT a.region, count(*) FROM … GROUP BY …                    -- DISTINCT + GROUP BY
```

## Honesty gates (discriminating)

- `group_by_inner_join_counts_matched_rows`.
- `group_by_left_outer_join_counts_null_padded_bucket` / right / full — altitude proof.
- `group_by_join_having_filters_after_aggregate` (D3).
- `group_by_join_order_by_limit_runs_after_aggregate`.
- **`group_by_join_real_aggregate_merges_across_partitions`** — *cardinal*, distributed: a sum/count
  whose group rows are produced by multiple join workers yields the correct final total, `>1`
  worker (D2 — row co-location before real aggregate state is computed, which empty-aggregate
  DISTINCT never exercised).
- `group_by_decorrelated_exists_aggregates_after_semi_join` — proves the late post-WHERE/
  decorrelation fence was lifted, not only the early explicit-join fence.
- `global_aggregate_over_join_still_works` — regression pin (already supported).
- `rune_group_by_over_join_matches_sql_plan_and_rows` — frontend parity over joins.
- Retained fences: `public_group_by_without_aggregate_still_rejects`,
  `distinct_with_group_by_still_rejects`, `joins_inside_derived_tables_still_reject`,
  `group_by_inside_exists_subquery_still_rejects`.

## Architecture guards (promotion-locked)

- `adr233_group_by_over_joins_lifts_both_grouped_fences`
- `adr233_group_by_join_reuses_232_post_join_exchange_split`
- `adr233_group_by_join_lowers_through_existing_aggregate_no_new_op`
- `adr233_group_by_join_retains_no_aggregate_distinct_and_derived_fences`
- `adr233_join_scoped_aggregation_adds_no_optimizer_surface`

**Reconciled prior guards (named):**
- The existing GROUP-BY-over-join guard → retargeted to expect the grouped fences lifted while
  keeping no-aggregate / DISTINCT+GROUP-BY / derived fences.
- ADR 232 post-join exchange split guard → reused unchanged (now serves grouped aggregates too).
- ADR 231 marker / ordinal-safe / no-aggregate-GROUP-BY-rejected guards → unchanged.
- Optimizer arc (ADR 216→230) → untouched.

## Consensus (transparent)

3-of-3. Codex iter-0 proposed Candidate A and the title "Join-Scoped Aggregation Completion",
with the load-bearing insight that empty-aggregate DISTINCT under-tested the 232 substrate and a
real aggregate validates count/sum state flow + HAVING + outer-join null buckets. Claude iter-0
proposed GROUP-BY-over-joins (same cut) and converged; Claude iter-1 grounded the scope —
global aggregates over joins already work (so 233 is grouped + HAVING, not global), HAVING
composes as a Filter (no new fence), and 232's split is generic (already covers the group-key
exchange) — and sharpened the cardinal to a distributed real-aggregate row co-location proof.
No residual divergence.

## Forward arc

```text
231 SELECT DISTINCT -> 232 DISTINCT over joins -> 233 join-scoped aggregation completion (this)
-> DISTINCT in derived tables / EXISTS
-> set operations (UNION/INTERSECT/EXCEPT)
-> window functions
```

The aggregate-over-joins substrate is now fully consumed (empty-aggregate DISTINCT *and* real
grouped aggregates + HAVING).

## Deferred (named)

GROUP BY without aggregate as dedup; grouping sets; DISTINCT aggregates (`count(DISTINCT x)`);
DISTINCT + GROUP BY fusion; joins inside derived tables; GROUP BY inside EXISTS subqueries;
DISTINCT inside derived tables and EXISTS; set operations; window functions.
