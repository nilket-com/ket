# ADR 259 — Correlated `IN` / `NOT IN` (membership-subquery decorrelation)

## Status

Proposed.

## Context

256 landed uncorrelated `IN`/`EXISTS`/`NOT EXISTS`; 258 landed null-aware `NOT IN`. Both route the
subquery's WHERE through `classify_exists_conjunct`, which already distinguishes a **correlated equi-pair**
(`sub.a = outer.b`) from an **inner residual**. Correlated EXISTS/NOT EXISTS *consume* that pair (push it
onto the join `on`); the membership helper `lower_membership_subquery` (258) instead **rejects** it:

```
sql_planner.rs:5425   ExistsConjunct::CorrelatedPair { .. } => Err("correlated {context} is not supported")
sql_planner.rs:5332   ExistsConjunct::CorrelatedPair { outer, inner } => on.push((outer, inner))  // EXISTS
```

So `x [NOT] IN (SELECT y FROM t WHERE t.a = outer.b)` rejects today purely because the membership helper
declines a pair the decorrelation substrate already knows how to spend.

## Grounding (iter-cycle — the decorrelation target is already proven)

`exists_decorrelation_test` proves the target shape end-to-end:

- correlated `LeftSemi`/`LeftAnti` on a `(outer, inner)` equi-pair (`dim.k = fact.k`);
- an **inner residual executes** — `EXISTS (SELECT 1 FROM dim WHERE dim.k = fact.k AND dim.active = true)`
  runs to `pk ∈ {1, 3}` (vs `{1, 3, 5}` without the residual): the residual lands as a **Toron `Filter`
  node** above the scan (asserted at `exists_decorrelation_test.rs:83`) and is **honored**, not pushed-and-
  dropped. So filters built in the decorrelation path execute correctly.

A correlated membership join is that same `build_join_plan(... on = pairs ...)` with the **membership key
appended to `on`** — no new operator and no new join kind.

## Decision — lift the `CorrelatedPair` reject into a key-push (reuse the EXISTS substrate)

`lower_membership_subquery` stops rejecting `CorrelatedPair` and instead **collects** the pairs, returning
them alongside the inner plan + membership key. Each membership caller adds them to the join `on`:

- **Correlated `IN`** → `LeftSemi(outer, inner, on = correlation_pairs ++ [(x, inner_key)])`.
- **Correlated `NOT IN`** → the 258 three-piece desugar, each piece gaining the correlation pairs:
  1. `Filter(outer, x IS NOT NULL)` — null-`x` exclusion (uncorrelated, unchanged);
  2. null-guard `LeftAnti(plan, Filter(inner, inner_key IS NULL), on = correlation_pairs)` — correlated when
     `correlation_pairs` is non-empty: the keyed anti **scopes the NULL-emptying to the matching correlation
     group** (a NULL key in one outer row's correlated subquery empties only that row, not the whole result),
     and is **keyless when empty** (so 258's uncorrelated whole-result null-guard is byte-for-byte preserved);
  3. membership `LeftAnti(plan, inner, on = correlation_pairs ++ [(x, inner_key)])`.

**The uncorrelated path is the `correlation_pairs == []` special case** — 256's `IN` (`on=[(x,key)]`) and
258's `NOT IN` (keyless null-guard + `on=[(x,key)]` membership) lower *identically* to today. 259 is purely
additive: a non-empty `correlation_pairs` is the only new behavior.

### Surface

Frontend (`sql_planner.rs`) only — `lower_membership_subquery` + its two callers. Reuses the landed
`LeftSemi`/`LeftAnti` (correlated form is the pre-256 correlated-EXISTS path). **No IR node, no execution op,
no fragmenter change, no new join kind.**

## Scope

- **In**: `x [NOT] IN (SELECT y FROM t WHERE <equi-correlation> [AND <inner residual> …])` — a single-column
  membership subquery correlated to the outer by one or more **equi-conjuncts**; inner residuals and
  multiple correlation pairs fold in (they ride the EXISTS classifier already invoked). Full NULL
  three-valued semantics for correlated `NOT IN` (the null-guard becomes a correlated anti).
- **Out / deferred (mechanism boundaries)**:
  - **Non-equi correlation** (`t.a < outer.b`) — the classifier only decorrelates equi-pairs; a non-equi
    outer reference already rejects as `"unsupported EXISTS subquery predicate"` (shared boundary, unchanged).
  - **Correlated *scalar* subqueries** — a genuinely different mechanism (decorrelate to a LEFT JOIN +
    grouped aggregate keyed on the correlation, plus a cardinality angle); its own cut.
  - Cross-type membership-key coercion (258's `JoinKeyTypeMismatch` gap); HAVING/aggregate scalars;
    non-aggregate scalars.

## Discriminators (each fails on `main`)

- **`correlated_in_decorrelates_to_semi_join`** *(execute)* — `SELECT fact.pk FROM fact WHERE fact.pk IN
  (SELECT dim.k FROM dim WHERE dim.k = fact.k)` → `{1, 3}`. The correlation **changes the answer**: the
  uncorrelated `fact.pk IN (SELECT dim.k FROM dim)` is `{1, 3, 4}` — pk=4 (whose `k` is NULL, so its
  correlated subquery is empty) is kept uncorrelated but dropped correlated. (Pure semi-join, no null
  filters → fully mock-honored.)
- **`correlated_in_membership_and_correlation_pairs_both_on_join`** *(plan-shape)* — the lowered membership
  `Join.on.len() == 2` (correlation pair `(fact.k, dim.k)` + membership pair `(fact.pk, dim.k)`), `kind ==
  LeftSemi`. Locks that *both* pairs land on the `on`.
- **`correlated_in_with_inner_residual_decorrelates`** *(execute)* — `… IN (SELECT dim.k FROM dim WHERE
  dim.k = fact.k AND dim.active = false)` → `{1}` (the residual removes pk=3's only correlated dim row, which
  is active) — residual + correlation coexist; the residual executes (the proven `Filter`-node path).
- **`correlated_not_in_decorrelates_to_anti_joins`** *(execute)* — `SELECT fact.pk FROM fact WHERE fact.pk
  NOT IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)` → `{2, 4, 5, 6}` (the complement among non-null-`x`
  rows; pk=4 kept because its correlated subquery is empty). Plan-shape: three pieces — an `IsNotNull` filter
  + two `LeftAnti`, the membership anti carrying correlation + membership pairs.
- **`uncorrelated_in_and_not_in_unchanged`** *(plan-shape regression)* — an uncorrelated `IN`/`NOT IN` still
  lowers to `on.len() == 1` / a keyless null-guard (the `correlation_pairs == []` path is identical to
  256/258).
- **`non_equi_correlated_in_and_correlated_scalar_stay_rejected`** *(reject)* — `… IN (SELECT dim.k FROM dim
  WHERE dim.score > fact.pk)` rejects (`"unsupported EXISTS subquery predicate"`, non-equi); a correlated
  *scalar* subquery rejects (`"correlated scalar subqueries are not supported"`, deferred mechanism).

### Test-harness note

Tests live in **`exists_decorrelation_test.rs`** (the decorrelation home; fact/dim fixture). The inner
residual + the correlated semi/anti execute against the FixtureScan (the residual lands as a Toron `Filter`
node and is honored — `exists_decorrelation_test.rs:83`). The correlated `NOT IN` null-guard splits into two
cases by whether the **projected key equals the correlation column**:

- **projected key = correlation column** (e.g. `… NOT IN (SELECT dim.k FROM dim WHERE dim.k = fact.k)`): the
  null-guard is **vacuous** — an equi-correlated subquery can never contain a NULL projected key (`dim.k =
  fact.k` is never TRUE for a NULL `dim.k`). `correlated_not_in_decorrelates_to_anti_joins` is this case, so
  it is driven by the membership anti + null-`x` exclusion.
- **projected key ≠ correlation column** (e.g. project `dim.c`, correlate on `dim.b = fact.a`): the
  null-guard is **meaningful** and the correlation-keying correctly **scopes it per group** (a NULL `dim.c`
  in one outer row's correlated group empties only that row — Gemini's per-group point). The fact/dim fixture
  has no nullable non-correlation column reachable via a matching correlation, so this per-group behavior is
  pinned at **plan-shape** (null-guard keyed on `correlation_pairs`) + the 258 manual-expansion probe, not an
  execute-test.

(And as in 258, if the null-guard's `inner_key IS NULL` filter is pushed-and-dropped on the membership path,
its execute coverage narrows to plan-shape — confirmed at implementation.)

## Architecture guards

- `adr259_correlated_membership_decorrelates_via_exists_pairs` — `lower_membership_subquery` collects
  `CorrelatedPair`s (no longer rejects); the membership callers push them onto the join `on`; reuses
  `LeftSemi`/`LeftAnti` (no new kind/op).
- `adr259_correlated_in_not_in_is_frontend_only` — no IR / executor / fragmenter / join-kind surface.
- `adr259_non_equi_and_scalar_correlation_stay_deferred` — non-equi correlation
  (`"unsupported EXISTS subquery predicate"`) and correlated scalar subqueries
  (`"correlated scalar subqueries are not supported"`) remain rejected.

## Reconcile (lifting the correlated-membership reject)

- `lower_membership_subquery` no longer emits `"correlated {context} is not supported"` — the
  256 guard `adr256_correlated_in_and_scalar_subquery_stay_deferred` pins that string; **re-target** it to
  the surviving deferred rejects (scalar subqueries, `SELECT` without `FROM`).
- `correlated_in_and_scalar_subquery_stay_rejected` (group_by_test) asserts correlated `IN` rejects — drop
  that case (it lowers now), keep the scalar-subquery case.

## Deferred

Correlated **scalar** subqueries (grouped-aggregate decorrelation); non-equi correlation; cross-type
membership coercion; HAVING/aggregate scalars; non-aggregate scalars; recursive-completion.

## Forward arc

259 correlated `IN`/`NOT IN` → correlated scalar + non-aggregate scalar (grouped-aggregate decorrelation +
≤1-row cardinality assert) → HAVING/aggregate scalars (the `terminal_exchange`/gather planner-rule review) →
cross-type coercion → recursive-completion → the standard query surface is broadly complete. (Window
`EXCLUDE` remains the lone window item, externally blocked on a sqlparser bump.)
