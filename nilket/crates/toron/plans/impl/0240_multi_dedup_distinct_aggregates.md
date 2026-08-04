# Impl Plan — Toron ADR 240: Multi-Dedup Distinct Aggregates

Pairs with `plans/adr/0240_multi_dedup_distinct_aggregates.md`. Generalize the ADR 234 single-dedup
desugar (`build_distinct_aggregate_rewrite`, sql_planner.rs:5788) into an N-branch fan-out + recombine
that composes `Project`/`Distinct`/`Aggregate`/null-safe `Join`/`Cross`. **Frontend-only rewrite; no new
physical operator, optimizer, durable, Wendelon, or Rune surface.**

## Touch surface

- `sql_planner.rs` — the rewrite:
  - **`build_distinct_aggregate_rewrite`** (5788): stop returning a single inline-wrapped rewrite; build
    the full fan-out + recombine `LogicalPlan` plus the final SELECT projection. Group `catalog.entries`
    into dedup branches by structural distinct-arg identity (reuse `canonical_aggregate_signature_expr` +
    `expr_structural_eq`, already used at :5803); collect non-distinct entries into plain branch(es),
    sharding only when duplicate aggregate output names would make branch-normalization ambiguous.
  - **`distinct_aggregate_plain_column_arg`** (5861): extend to accept `AVG` (D5) and computed
    single-expression args (D3) instead of rejecting them; computed args lower via an injected
    `Project(keys, __distinct_arg_k = expr)` ahead of `Distinct`.
  - **`DistinctAggregateRewrite`** struct + `wrap_input` (the inline applier): replace with a builder
    that returns the recombine plan; the single-branch case (N=1, no plain branch) must reduce to ADR
    234's exact `Distinct → Aggregate` shape (subsumption, not a parallel path).
  - **application sites** (grouped 3658, global 3713): when a multi-dedup rewrite is present, use its
    recombine plan directly as the lowered result rather than feeding the single `Aggregate` at
    3667/3718. The `aggr_expr.is_empty()` guard (3656) is unaffected — the rewrite never emits an
    empty-agg `Aggregate` (D6).
  - the four rejects at 5795/5807/5870/5883 are lifted; keep the deferred rejects (FILTER, window,
    grouping sets) and `"unsupported DISTINCT aggregate function"` (5873) for non-{COUNT,SUM,MIN,MAX,AVG}.
- recombine construction (reuse, do not invent):
  - grouped: null-safe group-key `LogicalPlan::Join { null_safe_keys: true, on: [(gᵢ_left, gᵢ_right)] }`
    — the ADR 235 private null-safe join (anchor: `lower_distinct_membership_set_op` /
    `null_safe_keys: true`). Left-deep chain over branches.
  - global: `build_auto_join_plan(left, right, JoinKind::Cross, Vec::new())` (the ADR 239 keyless Cross;
    forced broadcast by the binder).
  - final projection: a `LogicalPlan::Project` mapping recombine output **ordinals** → SELECT order.
    `AVG(DISTINCT)` is emitted as ordinary branch `AVG` over deduped rows; no user-visible-name
    resolution and no planner-owned sum/count final projection.
- tests: `tests/group_by_test.rs` (SQL behavior) + `tests/distributed_process_test.rs` (distributed
  value-discriminating + join base) + `tests/architecture/` (the ten `adr240_*` guards).

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| Single-dedup rewrite (generalize) | `build_distinct_aggregate_rewrite` (sql_planner.rs:5788) |
| Distinct-arg identity test | `canonical_aggregate_signature_expr` + `expr_structural_eq` (:5803) |
| Per-entry arg legality (extend) | `distinct_aggregate_plain_column_arg` (:5861) |
| Aggregate-call catalog | `AggregateOutputCatalog` / `AggregateOutputEntry` (:5889) |
| Rewrite application (grouped/global) | sql_planner.rs:3658 / :3713 |
| Null-safe recombine join | ADR 235 `null_safe_keys: true` join path |
| Keyless global recombine | `build_auto_join_plan(.., JoinKind::Cross, Vec::new())` (ADR 239) |
| Empty-agg Aggregate is forbidden (why D6 matters) | "GROUP BY requires at least one aggregate call" (:3656) |

## Phase 0 — Red discriminators

`mixed_distinct_and_plain_aggregate_executes`; `multiple_distinct_arguments_execute`;
`grouped_multi_dedup_all_null_distinct_column_keeps_group`; `multi_dedup_distinct_aggregate_distributed_merges_across_producers`.
Red on `main` (the four rejects fire / the result is single-producer-unreachable).

## Phase 1 — Branch fan-out

Group `catalog.entries` by structural distinct-arg identity → one dedup branch each; collect
non-distinct entries → one plain branch unless duplicate aggregate output names (`sum`, `sum`, etc.)
require extra plain shards for ordinal-safety. Build each branch:
`Project(keys [+ __distinct_arg_k]) → Distinct → Aggregate(group=keys, branch_aggs)` (distinct) and
`Aggregate(group=keys, non_distinct_aggs)` (plain), each cloning the lowered base `plan` (D8 — join base
falls out for free). Single-branch / no-plain reduces to ADR 234's shape (subsumption).

## Phase 2 — Recombine + final projection

Grouped: left-deep null-safe group-key joins across branches (D6, no group-domain branch). Global:
left-deep `Cross` across branches (D7). Final `Project` remaps recombine ordinals to SELECT order; it
does not implement aggregate semantics.

## Phase 3 — AVG + computed args

`AVG(DISTINCT x)` shares the `x` branch, emitting branch `AVG(x)` over the deduped rows; lift the AVG
reject. Do not add a sum/count final projection unless re-grounding proves direct `AggregateFunction::Avg`
cannot compose. Computed `DISTINCT expr` injects `Project(__distinct_arg_k = expr)` before `Distinct`;
all references by ordinal (D3). Lift the computed-arg reject.

## Phase 4 — Lift rejects, keep deferrals

Remove the four "...require multi-dedup fan-out" rejects; keep FILTER / window / grouping-sets /
non-{COUNT,SUM,MIN,MAX,AVG} rejects. `--all-targets` (the rewrite changes a widely-constructed path).

## Phase 5 — Distributed + join base

`multi_dedup_distinct_aggregate_distributed_merges_across_producers` (value-discriminating per 236/238 —
a group's distinct values and a plain contribution split across producers); `multi_dedup_over_join_executes`.
Each branch rides existing distributed `Distinct`, grouped `Aggregate`, broadcast `Cross`, and
null-safe-join machinery.

## Phase 6 — Guards

Add the ten `adr240_*` guards. Retarget the four ADR 234 reject-string guards (now accepted forms).
Assert ADR 235 null-safe-join and ADR 239 Cross guards are reused; no new physical operator; the
recombine introduces no internal empty-agg `Aggregate` (locks D6).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test                       # distinct-aggregate surface
scripts/cargo-cgroup test  -p toron --test distributed_process_test multi_dedup
scripts/cargo-cgroup test  -p toron --test architecture_test adr240
scripts/cargo-cgroup test  -p toron --test architecture_test                   # FULL (reconciled 234 guards)
scripts/cargo-cgroup test  -p toron --test distributed_process_test            # FULL
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Mixed stays rejected / plain branch dropped — `mixed_distinct_and_plain_aggregate_executes` fails;
   duplicate plain aggregate output names bind ambiguously or read the wrong slot —
   `mixed_plain_branch_duplicate_output_names_stay_ordinal_safe` fails.
2. Second distinct identity collapses into one branch — `multiple_distinct_arguments_execute` /
   `same_distinct_argument_shares_one_dedup_branch` fail.
3. All-NULL distinct group dropped (D6 wrong; a groups-domain branch was actually needed) —
   `grouped_multi_dedup_all_null_distinct_column_keeps_group` fails.
4. Global branch emits >1 row → `Cross` multiplies — `global_multi_dedup_branch_is_single_row_before_cross` fails.
5. `AVG(DISTINCT)` new kernel / sum-count projection / non-NULL on empty —
   `avg_distinct_uses_avg_over_dedup_and_null_semantics` fails.
6. Computed arg resolved by name not ordinal — `computed_distinct_argument_executes` fails.
7. Join base fenced or mis-qualified — `multi_dedup_over_join_executes` fails.
8. Distributed result reachable from one producer — the distributed gate fails (236/238 discipline).

## Hygiene

- No m-word; no version-suffix idiom; reuse primitives (no new operator/kernel); goldens limited to
  recombined result rows.
- `--all-targets` (the rewrite touches a widely-constructed lowering path); cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the single-branch (N=1, no plain) path reduces to ADR 234's exact `Distinct → Aggregate`
   output (no regression in the landed 234 tests).
2. Confirm `Distinct(keys, x)` retains `(keys, NULL)` so every group survives a branch with
   `count = 0` (the load-bearing fact behind D6 / no group-domain branch).
3. Confirm null-safe group-key recombine join type (INNER vs FULL) yields exactly one row per group
   given equal/complete branch group-sets.
4. Confirm each global branch is provably single-row before `Cross` (empty-table scalar aggregate still
   emits one row).
5. Confirm cloning a lowered **join** base per branch needs no qualifier/ordinal re-resolution (D8).
