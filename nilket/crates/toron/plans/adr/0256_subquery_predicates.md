# ADR 256 — Uncorrelated Subquery Predicates (`IN` / `EXISTS` / `NOT EXISTS`)

## Status

Proposed.

## Context

After local recursive CTEs (255), the predicate surface is the next high-value gap. Today
`x IN (subquery)` rejects (`sql_planner.rs:5440`), uncorrelated `EXISTS` rejects (`:5265`), and `SELECT`
without `FROM` rejects (`:3657`). Meanwhile **correlated `EXISTS` already decorrelates into `LeftSemi`/
`LeftAnti` joins** via `lower_exists_join` + `build_join_plan` (493-line `exists_decorrelation_test.rs`).
So the semi/anti-join substrate the rest of the surface needs is already built; this cut is a
frontend-lowering desugar over it.

The council diverged (Codex: recursive `UNION` distinct + joins-in-recursion; Claude+Gemini: subquery
predicates). Resolved to subquery predicates on grounding: recursive-completion needs joins-in-recursion,
which lower to exchanges → distributed-fixpoint planning (the heavy arc 255 fenced out), whereas subquery
predicates are pure frontend reuse of landed kernels. Recursive-completion is the named next arc.

## Grounded receipts (iter-cycle probes, the 119/120/121 discipline)

- **Keyed `LeftSemi` membership** (the `IN` shape): `left_dups SEMI right_dups ON k=k`, grouped, →
  `(A,2),(B,1)` — membership semantics bind + execute (also covered by `join_kind_test`/
  `hash_join_kinds_test`).
- **Keyless `LeftSemi`** (the uncorrelated-`EXISTS` shape): a hand-built `Join { kind: LeftSemi, on: [] }`
  passes all 5 outer rows when the right is non-empty and 0 when empty — **no new operator needed**.
- **Fence note**: a SEMI JOIN with a *derived-table* right side hits the adr145
  "derived-tables-in-non-inner-chains" reject — so `IN`-lowering must **build the `Join` node directly**,
  not route through SQL JOIN-with-derived syntax.

## Decision

Lower uncorrelated subquery predicates in the WHERE clause to the existing semi/anti-join kernels — no
IR / binder / executor / fragmenter change.

- **`x IN (subquery)`** → `build_join_plan(outer, subquery_plan, LeftSemi, on = [(lower(x),
  subquery_col_0)])`. The subquery is uncorrelated (a standalone plan) projecting exactly one column;
  the join key pairs the IN-LHS with that column. (Built directly — see the fence note.)
- **uncorrelated `EXISTS (subquery)`** → a **keyless** `LeftSemi` (`on = []`): every outer row passes iff
  the subquery is non-empty. Lift the `on.is_empty()` reject in `lower_exists_join` (`:5264`) for the
  uncorrelated case.
- **`NOT EXISTS (subquery)`** → `LeftAnti` (clean — `EXISTS` existence has no NULL three-valued gotcha).
## Scope

- **In**: `x IN (subquery)`, uncorrelated `EXISTS`/`NOT EXISTS`, single-column uncorrelated subqueries.
- **Out / deferred (mechanism boundaries)**: **`NOT IN`** — its NULL three-valued semantics (any NULL in
  the subquery makes the predicate NULL for every row) are a distinct correctness concern; a plain
  `LeftAnti` would be wrong, and `NOT EXISTS` gives the clean anti-join this cut. **Scalar subqueries**
  (`(SELECT max(x))` as a value — runtime single-row-or-error cardinality + value join). **Correlated
  `IN`** (outer ref inside the IN subquery). `row-value IN`, `IN UNNEST`. `SELECT` without `FROM`.
  Recursive-completion (Codex's arc).

## Discriminators (each fails on `main`)

- **`in_subquery_lowers_to_semi_join`** — `x IN (SELECT y FROM t)` plans to `LeftSemi` keyed on (x, y) and
  executes the membership filter.
- **`uncorrelated_exists_passes_all_rows_iff_subquery_nonempty`** — keyless `LeftSemi`: non-empty subquery
  ⇒ all rows, empty ⇒ none.
- **`not_exists_lowers_to_anti_join`** — `NOT EXISTS` → `LeftAnti`.
- **`not_in_and_scalar_subquery_stay_rejected`** — `NOT IN`, scalar subqueries, and correlated `IN` reject
  with precise messages (the deferred boundaries).

## Architecture guards

- `adr256_in_and_uncorrelated_exists_reuse_semi_anti_join` — `IN`/`EXISTS`/`NOT EXISTS` lower through
  `build_join_plan` (`LeftSemi`/`LeftAnti`); the `IN subquery` / `uncorrelated EXISTS` rejects are gone;
  no new join machinery.
- `adr256_subquery_predicates_are_frontend_lowering_only` — no IR / executor / fragmenter surface.
- `adr256_not_in_and_scalar_subquery_stay_deferred` — those rejects remain; `SELECT without FROM` stays
  deferred.

## Deferred

`NOT IN` (null-aware anti); scalar subqueries; correlated `IN`; `row-value IN` / `IN UNNEST`; recursive
`UNION` distinct + joins-in-recursion + distributed fixpoint.

## Forward arc

256 `IN` / uncorrelated `EXISTS` / `NOT EXISTS` → 257 `NOT IN` (null-aware) + scalar subqueries + `SELECT`
without `FROM` → (correlated `IN`) → recursive-CTE completion (distinct + joins + distributed fixpoint) →
the standard query surface is broadly complete. (Window `EXCLUDE` remains the lone window item,
externally blocked on a sqlparser bump.)
