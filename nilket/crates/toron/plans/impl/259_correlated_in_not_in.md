# Implementation Plan — ADR 259: Correlated `IN` / `NOT IN` (membership-subquery decorrelation)

## Summary

A frontend-only change in `sql_planner.rs`: `lower_membership_subquery` stops rejecting the correlated
equi-pair and instead **returns** the collected `correlation_pairs`; the two membership callers push them
onto the join `on`. The correlated form is the landed pre-256 correlated-EXISTS path (`LeftSemi`/`LeftAnti`
with a non-empty `on`). The uncorrelated path is the `correlation_pairs == []` special case — 256/258 lower
identically. No IR / binder / executor / fragmenter change; no new join kind.

## Verify-first probe (iter-cycle — recorded)

`exists_decorrelation_test` proves the decorrelation target, including that an inner residual **executes**:
`EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k AND dim.active = true)` → `pk ∈ {1,3}` (the residual is a
Toron `Filter` node — `exists_decorrelation_test.rs:83` — honored, not dropped). The membership helper
already classifies the pair (`classify_exists_conjunct` → `CorrelatedPair`) and currently rejects it
(`sql_planner.rs:5425`) — the gap is the reject, not missing machinery.

## Phase 1 — collect correlation pairs in `lower_membership_subquery` (`sql_planner.rs`)

Change the helper's return to `(LogicalPlan, Expr, Vec<(Expr, Expr)>)` = `(inner_plan, inner_key,
correlation_pairs)`. In the conjunct loop, replace the `CorrelatedPair` reject with a collect:

```
ExistsConjunct::CorrelatedPair { outer, inner } => correlation_pairs.push((outer, inner)),
ExistsConjunct::InnerResidual(predicate) => inner_residuals.push(predicate),
```

(Residual handling — combine + `Filter(inner_plan, …)` — is unchanged; residuals and correlation pairs are
independent, exactly as `lower_exists_join` keeps them.) Return the pairs.

## Phase 2 — push the pairs onto each membership join's `on`

- **`lower_in_subquery_join`**: `let mut on = correlation_pairs; on.push((x, inner_key)); build_join_plan(…
  LeftSemi, on …)`.
- **`lower_not_in_subquery_join`**:
  - null-guard: `build_join_plan(plan, Filter(inner_plan.clone(), IsNull(inner_key)), LeftAnti,
    correlation_pairs.clone(), …)` — the pairs replace the empty `on`; when `correlation_pairs` is empty this
    is exactly 258's keyless null-guard.
  - membership: `let mut on = correlation_pairs; on.push((x, inner_key)); build_join_plan(plan, inner_plan,
    LeftAnti, on, …)`.

Build the joins directly (the adr145/256 lesson). The correlated antis preserve the outer columns across the
chain (anti emits left rows), so the correlation `outer` exprs stay in scope through all three pieces — same
as 258's chained antis.

## Phase 3 — tests, guards, reconcile, verification, seal

### Behavioral gates (`tests/exists_decorrelation_test.rs` — fact/dim fixture; the decorrelation home)

- `correlated_in_decorrelates_to_semi_join` *(execute)* — `fact.pk IN (SELECT dim.k FROM dim WHERE dim.k =
  fact.k)` → `{1, 3}`; assert it differs from the uncorrelated `{1, 3, 4}` (the correlation drops pk=4, whose
  `k` is NULL → empty correlated subquery). Pure semi-join — fully mock-honored.
- `correlated_in_membership_and_correlation_pairs_both_on_join` *(plan-shape)* — membership `Join.on.len()
  == 2`, `kind == LeftSemi`.
- `correlated_in_with_inner_residual_decorrelates` *(execute)* — `… WHERE dim.k = fact.k AND dim.active =
  false` → `{1}` (residual removes pk=3's active-only correlated dim row; residual on the inner Filter
  coexists with the correlation pair).
- `correlated_not_in_decorrelates_to_anti_joins` *(execute, with plan-shape fallback)* — `fact.pk NOT IN
  (SELECT dim.k FROM dim WHERE dim.k = fact.k)` → `{2, 4, 5, 6}`; plan-shape: `IsNotNull` filter + two
  `LeftAnti`, the membership anti with two `on` pairs. **Verify at impl** that the null-guard's `inner_key IS
  NULL` filter executes on this path (the EXISTS residual precedent says Toron `Filter` nodes are honored); if
  it is pushed-and-dropped (258's scan-path behavior), narrow the assertion to plan-shape (the null-guard is
  vacuous for equi-correlation regardless).
- `uncorrelated_in_and_not_in_unchanged` *(plan-shape regression)* — uncorrelated `IN`/`NOT IN` keep
  `on.len() == 1` / keyless null-guard (the `correlation_pairs == []` path is byte-identical to 256/258).
- `non_equi_correlated_in_and_correlated_scalar_stay_rejected` *(reject)* — `… WHERE dim.score > fact.pk`
  (`"unsupported EXISTS subquery predicate"`) and a correlated scalar subquery
  (`"correlated scalar subqueries are not supported"`).

### Architecture guards (`tests/architecture/subquery_predicates.rs` — extend the 256/258 module)

`adr259_correlated_membership_decorrelates_via_exists_pairs`,
`adr259_correlated_in_not_in_is_frontend_only`, `adr259_non_equi_and_scalar_correlation_stay_deferred`.

### Reconcile (lifting the correlated-membership reject)

- `adr256_correlated_in_and_scalar_subquery_stay_deferred` asserts `"correlated {context} is not supported"`
  is present — that string is **removed** by this cut. Re-target the guard to the surviving deferred rejects
  (`"non-aggregate scalar subqueries are not supported"` / `"SELECT without FROM is not supported"`).
- `correlated_in_and_scalar_subquery_stay_rejected` (group_by_test) — drop the correlated-`IN` case (it
  lowers now); keep the scalar-subquery + `SELECT`-without-`FROM` cases.
- Grep `adr*` + the test corpus for `"correlated IN is not supported"` / `"correlated {context}"` and
  reconcile every pin (250/252/254/256/258 precedent).

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test exists_decorrelation_test
scripts/cargo-cgroup test  -p toron --test group_by_test            # the reconciled correlated-IN gate
scripts/cargo-cgroup test  -p toron --test architecture_test adr259
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL (256 reconcile + new adr259 guards)
scripts/cargo-cgroup test  -p toron --test distributed_process_test # correlated semi/anti distributes (256 path)
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 259 correlated in not in`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **Reject reconcile is the load-bearing housekeeping** — removing `"correlated {context} is not supported"`
  trips the 256 guard + the group_by_test gate; both must be re-targeted in the same cut (the 258 precedent).
- **Helper signature churn**: `lower_membership_subquery` gains a return field; only two callers — keep the
  change tight and confirm the uncorrelated path is unchanged via `uncorrelated_in_and_not_in_unchanged`.
- **Multi-pair `on`**: the membership join now carries ≥2 equi-pairs (correlation + membership). Multi-key
  hash semi/anti is landed (compound `ON`); the correlated EXISTS tests already cover multi-conjunct `on`.
- **Correlated null-guard** is a keyed anti, not keyless — confirm the `correlation_pairs == []` branch still
  yields a keyless null-guard (258 byte-identity). The null-guard is vacuous for equi-correlated subqueries
  (a NULL key can't equi-match); do not chase value-membership coverage the fixture/mock can't honor.
- **Outer-column scope across the anti chain**: the correlation `outer` exprs must stay bound through the
  `IsNotNull` filter + the two antis; anti-joins preserve the left columns (258 chain precedent), so they do.
