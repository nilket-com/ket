# ADR 279 — decorrelation generality: unqualified correlated EXISTS refs (q04 wall)

## Status
Proposed.

## Context
ADR 278 measured `q04 = blocked(unsupported_correlated_exists@plan)`: q04's `EXISTS (… l_orderkey =
o_orderkey …)` uses **unqualified** correlated refs. The failure is **not a missing parser case — it is a
phase-boundary issue.** `SqlPlanner` is a unit struct (`pub struct SqlPlanner;`) and `plan(&self, sql)` takes
no catalog: the frontend is purely *syntactic*. `sql_column_ref` resolves only a qualified `table.col`; a bare
identifier hits `unsupported EXISTS column reference`, because `resolve_column_scope` classifies a ref to the
outer/inner scope by **qualifier**, and the frontend has no per-relation column list to resolve an unqualified
name. Column→relation resolution is the **binder's** job.

There is direct precedent: ADR 270's comma-join recognizer (`recognize_cross_equi_joins`) **runs in the binder,
post-resolution, precisely because official TPC-H join columns are unqualified** — its `column_side` returns
`None` for an ambiguous/unresolved column. That same machinery is the home for this cut.

## Decision
Resolve unqualified correlated EXISTS refs in the **binder** (catalog-aware), reusing the ADR 270 resolution
machinery — **scoped to correlated EXISTS only**. The frontend stops rejecting an unclassifiable correlated
EXISTS and instead defers it to the binder, which classifies each ref's scope and lifts the validated
correlated pairs into the `LeftSemi` join keys.

- **Resolver is binder/catalog-aware.** Classify each EXISTS correlated ref with the binder's
  `column_side`/`RelationScope` (the `recognize_cross_equi_joins` infrastructure), not the frontend's
  qualifier-only `resolve_column_scope`.
- **Safe-resolution invariant.** An unqualified ref resolves to the scope whose relation **uniquely** owns the
  column; if both scopes own it (genuinely ambiguous) or neither does, **keep rejecting** — never guess.
  (q04's `l_orderkey`/`o_orderkey` each live in exactly one relation.)
- **No name-shape heuristics.** Resolution is by column ownership, never by `l_`/`o_` prefix or name shape.
- **`SqlPlanner` stays catalog-free.** No catalog is threaded into the frontend; the frontend only *defers* the
  unclassifiable correlated EXISTS, it does not resolve it.
- **Scope boundary.** Correlated **EXISTS** only — enough to lower q04's `EXISTS` to its `orders ⋉ lineitem`
  `LeftSemi`. Out: scalar subqueries, correlated `IN`/`NOT IN`, q17/q20/q22, uncorrelated EXISTS (already
  works). These are different decorrelation mechanisms, deferred.

## The mechanism is NOT yet proven small
The invariant above is tight, but the **plumbing may not be** (per the council review point). The load-bearing
unknown is the frontend→binder **handoff representation**: today `lower_exists_join` emits a fully-classified
`LeftSemi` at the frontend, and today's IR has neither `Expr::Exists` nor a residual predicate field on
`LogicalPlan::Join`. A binder-aware handoff therefore needs a real representation (for example, a narrow
deferred-EXISTS marker or an equivalent pre-bind handoff that carries the unresolved predicate until scopes
exist), not a keyless semi-join placeholder. A keyless `LeftSemi` would ignore the correlation; a `Cross` +
filter would risk outer-row multiplication; and a filter above a semi-join would not have inner columns in
scope. The impl **grounds and measures that handoff first** (Phase 0) and does **not** describe the cut as
"localized" until the chosen mechanism is proven small.

## Watch-items (operator) → guards
- **Prove the size; don't assert it.** No "localized" claim until the handoff mechanism is grounded small;
  surface the measured plumbing honestly.
- **Ambiguity rejects.** A column owned by both scopes (or neither) still errors — a pinning test.
- **No frontend catalog, no name heuristics.** `SqlPlanner` stays catalog-free; classification is by column
  ownership.
- **No fake semi-join placeholder.** The deferred form must preserve the unresolved correlation until the
  binder can turn it into real `LeftSemi` keys.
- **Engine cut.** Full `-p toron` gate + the complete `architecture_test` (after fmt), then `-p toron-tpch`.

## Acceptance (verify-first — measure where q04 lands)
- dev + strict q04 move `blocked(unsupported_correlated_exists@plan)` → green or the next measured wall. Once
  the refs resolve, the EXISTS lowers to its semi-join and the (constant-folded) interval +
  `COUNT`/`GROUP BY o_orderpriority`/`ORDER BY` should follow — likely `pass@execute` (q04 the 7th reach cell)
  — but the impl measures; a next wall honestly names the ADR after.
- `exists_decorrelation_test` + the six reach cells + the q06/q01 correctness cells stay green; new tests pin an
  unqualified correlated EXISTS resolving **and** a genuinely-ambiguous correlated ref still rejecting.

## Forward arc
q04 green → the remaining frontier is the runtime date-interval kernel (`column ± interval`) and the harder
correlated forms (scalar/`IN`, q17/q20/q22). A q03/q05 correctness oracle stays available on the correctness
rail.

## Why toron-home
`crates/toron` frontend/binder — the SQL→plan decorrelation path. A pure engine capability; no cross-ring
surface.
