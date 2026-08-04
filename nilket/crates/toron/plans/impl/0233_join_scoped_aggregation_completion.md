# Impl Plan — Toron ADR 233: Join-Scoped Aggregation Completion

Pairs with `plans/adr/0233_join_scoped_aggregation_completion.md`. Lift the two grouped
`GROUP BY over joins` fences so real aggregates (count/sum) + HAVING run over joins through ADR
232's post-join exchange split — completing the aggregate-over-joins substrate that 232's
empty-aggregate DISTINCT only partially exercised. **No new execution operator, IR variant,
fragmenter code, Rune code, or optimizer surface.**

Touch surface:
- `sql_planner.rs` — remove the two `"GROUP BY over joins is not supported"` rejects
  (3452 pre-FROM, 3525 post-WHERE/decorrelation); keep no-aggregate / DISTINCT+GROUP-BY /
  derived fences. HAVING's `Filter`-on-aggregate wrap (~3600) needs no change.
- `tests/group_by_test.rs` — grouped-aggregate-over-join behavior (inner, outer, HAVING, ORDER
  BY/LIMIT, retained fences, Rune parity, global-aggregate regression pin).
- `tests/distributed_process_test.rs` — the cardinal real-aggregate cross-partition co-location
  proof.
- `tests/architecture/*` — retarget the GROUP-BY-over-join guard; add `adr233_*`.
- `crates/wendelon/tests/architecture_test.rs` — retarget the ADR 076 cross-ring guard
  (`adr076_no_toron_production_change`) to a `toron/src` symbol scan, since this cut's
  `sql_planner.rs` edit trips its old "`toron/src` clean" assertion; and retire the sibling
  `adr077_*` / `adr078_*` git-status tripwires that shared the color-fragile pattern (their move-only
  invariant is covered by structural siblings). Wendelon *test-guard* only.

No `ir.rs`, `binder.rs` (grouped aggregate lowering unchanged), `exec.rs`,
`distributed/fragment.rs` (232's split reused as-is), `rune_engine.rs`, or Wendelon *production*
change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Grouped fences to remove | sql_planner.rs:3452 (`group_keys.is_some() && joins`), 3525 (`group_keys.is_some() && plan_contains_join`) |
| Global aggregate over join (ALREADY works — regression pin) | `AggregateSqlMode::Global`, `group_expr: vec![]` (sql_planner.rs:3583) |
| HAVING composes as Filter | `Filter { input: aggregate, predicate }` (sql_planner.rs:3600) |
| Grouped aggregate → group-keyed exchange | `group_by_lowers_to_aggregate_over_group_keyed_exchange` (group_by_test.rs:30) |
| 232 generic post-join split (reused) | `leading_exchange_before_join` (fragment.rs:264) |
| Filter-pushdown to compose with | `all_inner_join_chain` / `join_operator_kind_for_filter_pushdown` (sql_planner.rs:3454) |

## Phase 0 — Red discriminators

`group_by_inner_join_counts_matched_rows`;
`group_by_left_outer_join_counts_null_padded_bucket`;
`group_by_join_having_filters_after_aggregate`;
`group_by_decorrelated_exists_aggregates_after_semi_join`;
`group_by_join_real_aggregate_merges_across_partitions` (cardinal, distributed). Red on `main`.

## Phase 1 — Lift the two grouped fences (the whole behavior change)

Remove the two `group_keys.is_some() && <join present>` rejects (3452, 3525). The
grouped-aggregate lowering is unchanged — it already builds `Exchange(group keys) → Aggregate`,
and 232's `leading_exchange_before_join` splits that exchange over a join. HAVING's `Filter` wrap
composes unchanged. Confirm the WHERE filter-pushdown right after the removed fence
(`all_inner_join_chain`) still runs.

## Phase 2 — Behavior coverage

Two tables with multiple rows per group across the join key:
- Inner join: per-group counts/sums over matched rows.
- LEFT/RIGHT/FULL outer: null-padded buckets aggregate correctly (altitude proof).
- HAVING filters after the grouped aggregate over the join.
- ORDER BY / LIMIT after the aggregate.
- **Distributed real-aggregate co-location:** a sum/count whose group rows are produced by
  different join workers yields the correct final total; assert `>1` worker (group-key shuffle
  co-location before aggregate state is computed — the validation empty-aggr DISTINCT never gave).
- Decorrelated EXISTS: an outer GROUP BY over a supported EXISTS decorrelation aggregates after
  the semi/anti join, proving the late fence was lifted.
- `global_aggregate_over_join_still_works` (regression pin — already supported).
- Rune `.join(b,…).aggregate([group],[aggrs])` == SQL GROUP BY over join (bound plan + rows).
- Retained fences: no-aggregate GROUP BY rejects; DISTINCT+GROUP BY rejects; joins inside derived
  tables reject; GROUP BY inside EXISTS subqueries rejects. Plain aggregate derived tables already
  work and are not a retained fence.

## Phase 3 — Guards

Retarget the existing GROUP-BY-over-join guard by name (grouped fences lifted, others intact).
Add the five `adr233_*` guards. Assert 232's split guard reused, the 231 marker/no-aggregate
guards, and the optimizer arc (216→230) unchanged. Cross-ring: retarget Wendelon's
`adr076_no_toron_production_change` to a `toron/src` symbol scan (the `sql_planner.rs` edit trips its
old "`toron/src` clean" form), and retire the sibling `adr077_*` / `adr078_*` git-status tripwires
(color-fragile, and harmful if working since they fire on any unrelated dirty production file;
covered by structural siblings).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test group_by
scripts/cargo-cgroup test  -p toron --test architecture_test adr233
scripts/cargo-cgroup test  -p toron --test architecture_test adr232
scripts/cargo-cgroup test  -p toron --test architecture_test adr231
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retarget participates
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Rows for a group are not co-located before aggregation:
   `group_by_join_real_aggregate_merges_across_partitions` fails (wrong totals) — the cardinal
   that empty-aggr DISTINCT couldn't catch.
2. Lifting/regressing the global-aggregate path: `global_aggregate_over_join_still_works` fails.
3. HAVING not composing over a join: `group_by_join_having_filters_after_aggregate` fails.
4. Grouping pushed below the join (outer): the outer-join null-bucket tests fail.
5. Leaving the late fence in place: `group_by_decorrelated_exists_aggregates_after_semi_join`
   fails while explicit joins may pass.
6. Relaxing no-aggregate GROUP BY or DISTINCT+GROUP BY: their retained-fence tests fail.
7. A new `ExecutionPlan` op / fragmenter case: the no-new-op guard fails (232's split must suffice).

## Hygiene

- No m-word; no version-suffix idiom; no new physical op / durable key / optimizer surface; no
  goldens beyond aggregate-over-join result rows.
- `--all-targets` confirms the guard retarget compiles.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the distributed fixture genuinely has one group's rows produced by `>1` join worker
   before the group-key shuffle (else the cardinal gate passes for the wrong reason).
2. FULL outer join: a group formed only by null-padded rows from one side aggregates correctly.
3. Confirm no GROUP-BY-over-join fence remains beyond 3452/3525 (grep `"GROUP BY over joins"`
   returns zero after the cut).
