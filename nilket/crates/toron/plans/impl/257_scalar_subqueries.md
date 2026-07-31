# Implementation Plan — ADR 257: Scalar Subqueries (provably-scalar / aggregate form)

## Summary

A binder extension + a frontend pull-up. Binder: a provably-scalar (`row_upper_bound <= 1`) side is
broadcast-eligible for a `Cross`, so `outer × scalar` binds. Frontend: a scalar `SqlExpr::Subquery`
(uncorrelated global aggregate) in a SELECT projection (non-aggregate query) or a WHERE comparison operand
pulls up into a scalar `Cross` and is replaced by a reference to the subplan's single output column.
Reuses the `Cross`/broadcast executor — no new execution op. HAVING / aggregate-query, non-aggregate, and
correlated scalars stay rejected.

## Verify-first probe (iter-cycle — recorded; the premise shifted)

`regions × (SELECT AVG(amount) FROM sales)` built as a `Cross` **rejects** at bind:
*"CROSS JOIN requires a broadcast-eligible build side… no eligible side fits."* So the scalar (composite,
row=1) side is not broadcast-eligible today → 257 needs the binder extension below. (Re-run this exact
shape after Phase 1 to confirm it then binds + executes.)

## Phase 1 — Binder: broadcast a one-side-scalar `Cross` (`binder/join.rs`)

`choose_cross_join_strategy` (`:548`) handles **both**-scalar via `ScalarLocalCross`. Add a branch (after
the both-scalar check, before the `force_hash`/Auto fallthrough): if exactly **one** side is
`plan_is_scalar` (`row_upper_bound <= 1`), return a `Broadcast` decision with **that side as the build
side** (`BroadcastBuildSide`), placement natural. Rationale (mirror the existing `ScalarLocalCross`
comment): a 1-row branch broadcast to N partitions yields N×1 rows — no partition multiplication — so it
is broadcast-exempt from the scan-linear/base-relation eligibility requirement (which exists to bound
broadcast *size*; 1 row is trivially bounded). Reuse `plan_is_scalar`; pick the build side as the scalar
one. Confirm the broadcast executor accepts a composite (aggregate) 1-row build side (the probe re-run).

## Phase 2 — Frontend: pull scalar subqueries up into a `Cross` (`sql_planner.rs`)

In `lower_select`, after the FROM/join plan is built (pre-aggregation, below WHERE), scan the
**projection items** (non-aggregate query only) and the **WHERE** comparison operands for scalar
`SqlExpr::Subquery` expressions:
- **Aggregate-query gate**: if the query is an aggregate (GROUP BY, or an aggregate projection) and a
  scalar subquery appears in HAVING or the projection, reject ("scalar subqueries in GROUP BY/HAVING are
  not supported") — the HAVING/aggregate recombination is above the aggregate, a deferred cut. (A scalar
  in WHERE of an aggregate query is fine — it crosses pre-aggregation and the filter consumes it.)
- For each scalar subquery: validate it is **uncorrelated** (reuse the EXISTS scope classifier /
  `collect_scope_refs` → "correlated scalar subqueries are not supported") and **provably scalar** = a
  **global aggregate** (single aggregate projection, no GROUP BY → "non-aggregate scalar subqueries are
  not supported"). Lower it to its subplan (row=1), alias it `__scalar_N(value)`.
- `Cross`-join each subplan onto the running plan; replace the `SqlExpr::Subquery` node with a
  `__scalar_N.value` `CompoundIdentifier`, then continue normal projection/WHERE lowering.

Mechanism note: build the scalar `Cross` via **`build_auto_join_plan`** (the keyless-Cross builder the
comma-join fold uses) — NOT a pinned `build_join_plan(... HashPartitioned ...)`. The `Auto` intent is
required for the binder to physicalize the broadcast (`ScalarBroadcastCross`); a frontend-pinned
`HashPartitioned` would leave the two default exchanges in place and the broadcast placement check would
reject. (The 256 lesson still holds — build the node directly, not via SQL JOIN-with-derived syntax.)
Scalar subqueries are an *expression*-position feature, so the rewrite recurses through the comparison/
arithmetic/CASE/etc. expression containing the subquery.

## Phase 3 — Tests, guards, verification, seal

### Behavioral gates (`tests/group_by_test.rs` — sales/regions fixture)

`scalar_aggregate_subquery_in_projection_attaches_value` (`SELECT region, (SELECT AVG(amount) FROM sales)
FROM regions` → every region row carries the avg), `scalar_aggregate_subquery_in_where_filters`
(`… WHERE id > (SELECT COUNT(*) FROM regions)` — Int-vs-Int; cross-type Int-vs-Float comparison isn't
coerced, a follow-on), `scalar_aggregate_subquery_in_having_filters` (`… GROUP BY … HAVING … (SELECT
AVG(amount) FROM sales)` **asserts the deferred reject**), `one_side_scalar_cross_broadcasts_the_scalar`
(the binder extension — the `outer × global-aggregate` Cross that rejects on main now binds + executes),
`non_aggregate_and_correlated_scalar_subqueries_stay_rejected`.
Avoid relying on a pushed scan predicate inside the subquery (the FixtureScan predicate-drop — use
aggregates, as the 254–256 tests do).

### Architecture guards (`tests/architecture/scalar_subquery.rs`, new module + mod.rs)

`adr257_one_side_scalar_cross_is_broadcast_eligible` (`choose_cross_join_strategy` broadcasts a
`row_upper_bound <= 1` side; `ScalarLocalCross` unchanged), `adr257_scalar_subquery_pulls_up_into_cross`
(scalar `SqlExpr::Subquery` → `Cross` + column ref; reject lifted), `adr257_non_aggregate_and_correlated_scalar_stay_deferred`.

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr257
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (new test file → adr176 allowlist; scalar-Cross guards)
scripts/cargo-cgroup test  -p toron --test cross_join_test            # Cross-strategy regression (the binder change)
scripts/cargo-cgroup test  -p toron --test hash_join_kinds_test       # broadcast/cross executor regression
scripts/cargo-cgroup test  -p toron --test distributed_process_test
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 257 scalar subqueries`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **Binder broadcast-eligibility change is the load-bearing piece** (not frontend-only). Re-run the probe
  shape after Phase 1 before building the frontend pull-up; confirm the broadcast executor accepts a
  composite 1-row build side (it should — broadcast materializes the build side regardless of provenance).
- **Cross-strategy regression**: the new one-side-scalar branch must not change non-scalar Cross behavior
  (still requires a broadcast-eligible base-relation side or rejects). `cross_join_test` is the guard.
- **Expression-position threading** is the frontend bulk: substituting the subquery node with the scalar
  column ref inside an arbitrary projection/comparison expression (the rewriter recurses through
  comparison/arithmetic/CASE/Nested/Cast/IsNull/Between). Scope to scalar subqueries in (non-aggregate)
  projection items and WHERE comparison operands; HAVING / aggregate-query scalars are deferred.
- **Provably-scalar = global aggregate** this cut (no runtime cardinality machinery); a non-aggregate
  single-row subquery rejects (deferred).
- Reject-string scanners: lifting the scalar-subquery reject may trip a guard pinning it — grep `adr*` and
  reconcile (as in 250/252/254/256).
