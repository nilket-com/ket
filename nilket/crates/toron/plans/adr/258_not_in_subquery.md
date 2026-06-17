# ADR 258 — `NOT IN (subquery)` (null-aware)

## Status

Proposed.

## Context

ADR 256 landed `IN` / uncorrelated `EXISTS` / `NOT EXISTS` and deferred **`NOT IN`** precisely because its
SQL NULL three-valued semantics make a plain `LeftAnti` wrong:

`x NOT IN (SELECT y FROM sub)` is TRUE for a row only when **`x` is non-NULL, `sub` contains no NULL `y`,
and no `y` equals `x`** — so *any* NULL in the subquery makes the whole result empty, and NULL `x` rows
drop. A naive anti-join handles none of that.

(258 was originally penciled as HAVING scalar subqueries; that was grounded to need a planner-rule change
with broadcast/aggregate blast radius — `terminal_exchange` recurses through `Aggregate`, so a scalar
`Cross` over an aggregate can't satisfy the broadcast probe's `terminal_exchange == None` requirement.
That is deferred to a **separate planner-rule review**, not widened into 258.)

## Decision — desugar NOT IN into 256's `NOT EXISTS` machinery (no new operator)

`x NOT IN (SELECT y FROM sub)` is exactly equivalent to:

```
x IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM sub WHERE y IS NULL)     -- the NULL guard (uncorrelated, keyless)
  AND NOT EXISTS (SELECT 1 FROM sub WHERE y = x)         -- the membership (correlated, equi)
```

Each piece is **already landed**: the uncorrelated keyless `NOT EXISTS` (256, `LeftAnti` with `on = []`),
the correlated `NOT EXISTS` (pre-256, `LeftAnti` on `x = y`), and a `x IS NOT NULL` filter. The lowering
builds this composition directly via `build_join_plan(... LeftAnti ...)` (extend
`lower_in_subquery_join`'s `negated` arm, today a reject):

- `x IS NOT NULL` filter on the outer plan;
- keyless `LeftAnti` over the subquery filtered to `key IS NULL` → if the subquery has any NULL key, the
  filtered right is non-empty and the keyless anti drops **all** rows (empty result); if no NULL, it is a
  no-op;
- correlated `LeftAnti` over the subquery on `(x, key)` → drops rows whose `x` matches a `y`.

### Grounded receipt (iter-cycle probe)

The composition above, written as explicit SQL, **binds + executes** with correct NOT IN semantics:
no-NULL subquery → keeps the non-members; a NULL in the subquery → empty result. No new operator — it
reuses the 256 anti-join substrate.

## Scope

- **In**: `x NOT IN (SELECT y FROM t [WHERE …])` — uncorrelated, single-column subquery, with the full
  NULL three-valued semantics (NULL in subquery → empty; NULL `x` → dropped). Reuses the keyless +
  correlated `LeftAnti` machinery.
- **Out / deferred (mechanism boundaries)**: **cross-type** `NOT IN` (the membership key `y = x` requires
  matching types — `Int64` vs `Int32`/`Float` is a `JoinKeyTypeMismatch`, the same join-key-coercion gap
  257 noted); **correlated** `NOT IN` (outer ref inside the subquery — decorrelation); `row-value NOT IN`,
  `NOT IN UNNEST`; HAVING scalar subqueries (the separate planner-rule review); non-aggregate / correlated
  scalar subqueries; recursive-completion.

## Discriminators (each fails on `main`)

- **`not_in_subquery_excludes_members`** — `x NOT IN (SELECT y FROM t)` over a NULL-free subquery keeps
  exactly the non-members.
- **`not_in_with_null_in_subquery_returns_no_rows`** — a NULL in the subquery key empties the result (the
  three-valued gate; a plain `LeftAnti` would wrongly return rows).
- **`not_in_excludes_null_outer_rows`** — a NULL `x` row never qualifies.
- **`not_in_lowers_to_anti_joins_no_new_op`** — the plan is anti-joins + a filter (no new execution node).
- **`not_in_null_guard_holds_across_partitions`** *(distributed, Gemini)* — a NULL on any one worker's
  subquery partition empties the whole result (the keyless null-guard anti must see global non-emptiness).
- **`cross_type_and_correlated_not_in_stay_rejected`** — cross-type and correlated `NOT IN` reject.

## Architecture guards

- `adr258_not_in_desugars_to_not_exists_anti_joins` — `lower_in_subquery_join`'s `negated` arm builds the
  `x IS NOT NULL` filter + the keyless null-guard `LeftAnti` + the correlated membership `LeftAnti`; the
  256 `"NOT IN (subquery) is not supported"` reject is gone.
- `adr258_not_in_is_frontend_lowering_only` — no IR / executor / fragmenter surface (reuses the landed
  `LeftAnti`).
- `adr258_cross_type_and_correlated_not_in_stay_deferred` — those rejects remain.

## Deferred

Cross-type / coerced `NOT IN`; correlated `NOT IN`; `row-value NOT IN` / `NOT IN UNNEST`; HAVING scalar
subqueries (separate planner-rule review); the rest of the scalar-subquery family; recursive-completion.
**Optimization (deferred, Gemini): omit the null-guard anti when the binder proves the subquery key
non-nullable** — always emit it for correctness in this cut; eliding it is a later optimization.

## Forward arc

258 `NOT IN` (null-aware) → (planner-rule review: `Aggregate` as a `terminal_exchange` barrier → HAVING /
aggregate-query scalar subqueries) → non-aggregate / correlated scalars + cross-type coercion → correlated
`IN`/`NOT IN` → recursive-completion → the standard query surface is broadly complete. (Window `EXCLUDE`
remains the lone window item, externally blocked on a sqlparser bump.)
