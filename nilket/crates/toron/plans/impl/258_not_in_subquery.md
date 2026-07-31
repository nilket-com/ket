# Implementation Plan — ADR 258: `NOT IN (subquery)` (null-aware)

## Summary

Frontend-only desugar in `sql_planner.rs`: `lower_in_subquery_join`'s `negated` arm (today a reject)
builds `x IS NOT NULL` filter + a keyless null-guard `LeftAnti` + a correlated membership `LeftAnti`, all
via the landed `build_join_plan`. No IR / binder / executor / fragmenter change. Cross-type and correlated
`NOT IN` stay rejected.

## Verify-first probe (iter-cycle — recorded)

The composition `x IS NOT NULL AND NOT EXISTS(SELECT 1 FROM sub WHERE y IS NULL) AND NOT EXISTS(SELECT 1
FROM sub WHERE y = x)`, as explicit SQL, **binds + executes** with correct NOT IN semantics (no-NULL →
non-members kept; NULL in subquery → empty). Surfaced caveat: the membership key `y = x` requires matching
types (`Int64` vs `Int32` → `JoinKeyTypeMismatch`) — cross-type `NOT IN` defers to the coercion follow-on.

## Phase 1 — `NOT IN` lowering (`sql_planner.rs`)

In `lower_where`, the `SqlExpr::InSubquery { negated: true, .. }` arm currently returns the 256 reject
(`"NOT IN (subquery) is not supported; use NOT EXISTS"`). Replace it with `lower_not_in_subquery_join`
(sibling to `lower_in_subquery_join`):
1. Validate the subquery is **uncorrelated** + **single-column** (reuse `lower_in_subquery_join`'s checks;
   correlated `NOT IN` → reject "correlated NOT IN is not supported"). Lower the subquery FROM → an
   `inner_plan`; resolve the single membership key column (qualified to the subquery relation, as in 256).
2. `plan = Filter(outer, x IS NOT NULL)` — exclude NULL `x` rows. (`x` = `self.lower_expr(expr)`.)
3. **Null guard** — `plan = build_join_plan(plan, Filter(inner_plan, key IS NULL), LeftAnti, on = [],
   HashPartitioned, Direct, None)`: a keyless anti over the subquery rows whose key is NULL. If the
   subquery has any NULL key, that filtered relation is non-empty → the keyless anti drops every row
   (empty); if none, it is a no-op. (Clone `inner_plan` for the two anti-joins.)
4. **Membership** — `plan = build_join_plan(plan, inner_plan, LeftAnti, on = [(x, key)], HashPartitioned,
   Direct, None)`: drops rows whose `x` equals some `y`.

Factor the shared subquery validation + key resolution out of `lower_in_subquery_join` so IN and NOT IN
share it. Build the joins **directly** (the 256 adr145 lesson). `key IS NULL` is an `Expr::IsNull` filter
on the inner plan.

## Phase 2 — Tests, guards, verification, seal

### Behavioral gates (`tests/group_by_test.rs` — left_dups/right_dups/sales fixture; note the
FixtureScan pushed-predicate drop — use membership/joins, not pushed scan filters)

- `not_in_subquery_excludes_members` — `… WHERE left_dups.k NOT IN (SELECT k FROM right_dups)` → keeps
  `k = 3` rows (the non-members; right_dups.k ∈ {1,2,4}). Uses a **non-nullable** subquery key so the
  null-guard `key IS NULL` constant-folds (the mock can't honor a pushed nullable `IS NULL` filter).
- `not_in_with_null_in_subquery_returns_no_rows` — a subquery whose key column has a NULL (e.g.
  `sales.region`) → empty result.
- `not_in_lowers_to_anti_joins_with_null_guard_and_null_x_filter` — plan-shape: `LeftAnti` (never
  `LeftSemi`) + an `IsNotNull` filter (null-`x`) + an `IsNull` null-guard, no new node.
- `cross_type_and_correlated_not_in_stay_rejected` — `Int64 NOT IN (SELECT <Int32>)` (type mismatch) and a
  correlated `NOT IN` reject.

**Mock limitation (carried into the docs)**: the FixtureScan **drops pushed scan predicates**
(`reference_multitablestoragescan_ignores_pushed_predicates`), so a nullable `IS NULL` / `IS NOT NULL`
filter is a no-op against the mock, and the fixture has no nullable `Int64` column to pair a nullable outer
`x` against a non-nullable subquery key. The originally-penciled `not_in_excludes_null_outer_rows` (null-`x`
execution) and **`not_in_null_guard_holds_across_partitions`** (Gemini's distributed cross-partition gate)
are therefore **deferred to a predicate-honoring / real-storage harness** — the null-`x` and null-guard
semantics are pinned at **plan-shape** here and by the **manual NOT-EXISTS probe** (which real storage
honors); the desugar reuses the distributed `LeftAnti` unchanged, so global null-guard non-emptiness rides
on the landed 256 distributed anti-join.

### Architecture guards (`tests/architecture/subquery_predicates.rs` — extend the 256 module)

`adr258_not_in_desugars_to_not_exists_anti_joins`, `adr258_not_in_is_frontend_lowering_only`,
`adr258_cross_type_and_correlated_not_in_stay_deferred`. The 256 `adr256_not_in_and_scalar_subquery_stay_deferred`
guard pins the old `"NOT IN (subquery) is not supported"` reject — **reconcile it** (NOT IN now lowers;
re-target to the remaining deferred rejects).

### Verification gate (every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr258
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL (reconcile the 256 NOT-IN-deferred guard)
scripts/cargo-cgroup test  -p toron --test exists_decorrelation_test  # reuses the anti-join + scope machinery
scripts/cargo-cgroup test  -p toron --test distributed_process_test
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

### Seal

`impl: adr 258 not in subquery`, `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Risk notes

- **256 guard reconcile**: `adr256_not_in_and_scalar_subquery_stay_deferred` + the `group_by_test`
  `not_in_and_scalar_subquery_stay_rejected` gate assert the NOT IN reject — both must be reconciled (NOT
  IN lowers now; the scalar-subquery deferral stays). Grep `adr*` for `"NOT IN (subquery) is not
  supported"`.
- **Two anti-joins stack**: the null-guard anti and the membership anti both fold onto the outer; confirm
  the binder accepts the keyless `LeftAnti` (256 verified keyless semi/anti) followed by a correlated
  `LeftAnti`.
- **Cross-type membership key** rejects with `JoinKeyTypeMismatch` — that's the deferred coercion gap, not
  a NOT IN bug; the reject test pins it.
- **NULL guard correctness**: the keyless anti must run over the subquery filtered to `key IS NULL` (not
  the whole subquery); a NULL anywhere in the key → empty, matching SQL.
- **Distributed null-guard** (Gemini's gate): the null-guard is a keyless `LeftAnti` — the *same* operator
  256 already lands and distributes (a keyless anti gathers the right side, so it observes the subquery's
  *global* non-emptiness, not a per-partition view). 258 adds **no new distribution logic** — it composes
  the landed keyless anti — so there is no separable distributed-correctness risk to surface. An execute
  gate for it is deferred to a predicate-honoring harness (the mock drops the `key IS NULL` filter, so it
  can't run for the right reason).
- **Omit-guard optimization (deferred, Gemini)**: when the binder proves the subquery key non-nullable,
  the null-guard anti is a no-op and could be elided — **always emit it this cut**; the elision is a later
  optimization, not in scope.
