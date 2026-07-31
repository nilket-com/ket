# ADR 255 — `WITH RECURSIVE` (local `UNION ALL` fixpoint, first cut)

## Status

Proposed.

## Context

ADR 254 landed non-recursive CTEs and explicitly deferred `WITH RECURSIVE` as "its own engine mechanism"
(`"WITH RECURSIVE is not supported"`, `sql_planner.rs`). The Council selected it as 255. Unlike every
window/CTE/subquery cut so far, this is **not a frontend desugar** — grounding confirms there is *zero*
fixpoint/iterate/working-table machinery in the engine, so it is a from-scratch execution mechanism.

The canonical shape this cut must deliver:

```sql
WITH RECURSIVE t(n) AS (
    SELECT 1                       -- seed (anchor) term
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 10   -- recursive term: references t
)
SELECT n FROM t
```

## Grounding (the execution model this rides on)

- The executor is **pull-based and fully materializing**: `Executor::execute_bound(plan, morsel) ->
  Vec<RecordBatch>` (`exec.rs:697`); every operator materializes its input. A fixpoint loop fits
  naturally — execute the recursive term repeatedly, each pass over the prior pass's rows.
- `ExecutionPlan::SystemTable { schema, rows: Vec<Vec<ScalarValue>> }` (`ir/execution.rs:249`) is the
  existing **in-memory relation** node — the vehicle for injecting the working table each iteration.
- `ExecutionPlan::Union` is batch concatenation (`exec.rs:777`) — the `UNION ALL` accumulation is the
  same append, done in the fixpoint loop. (`Distinct`'s Exchange+Aggregate dedup at `binder.rs:1193` is
  what the deferred `UNION` (distinct) form would reuse — not this cut.)
- The distributed fragmenter places an **exchange-free** subtree in a single local task
  (`fragment.rs` `exchange_count == 0`) — so a local-only first cut is gated by keeping the recursive
  node opaque/exchange-free to fragmentation.

## Decision

Add a real recursive-CTE execution mechanism, **scoped to local (single-worker) fixpoint** for this cut.

### New IR (logical + execution)

- `LogicalPlan::RecursiveCte { name, column_names, seed: Box<LogicalPlan>, recursive: Box<LogicalPlan> }`
  — `UNION ALL` semantics only this cut, so there is **no `all` flag** (the distinct form is
  unrepresentable here, not a latent bool — it lands with dedup in the follow-on).
- `LogicalPlan::RecursiveRef { name, column_names }` — the self-reference leaf inside the recursive term
  (resolves to the working table at execution; carries only names, types come from the seed at bind).
- `ExecutionPlan::RecursiveCte { seed, recursive, output_schema }` and
  `ExecutionPlan::RecursiveRef { output_schema }` — the bound counterparts.

### Frontend lowering (extends the 254 CTE scope)

In `lower_query_scoped`, the `with.recursive` branch (today a reject) detects the canonical body shape —
`cte.query.body` is `SetExpr::SetOperation { op: Union, left: seed, right: recursive }` — and:

1. requires the body to be `seed UNION ALL recursive` — a non-`ALL` `UNION` (distinct) recursive body
   rejects ("recursive `UNION` (distinct) is not supported; use `UNION ALL`"), deferring the dedup form;
2. lowers `seed` (the anchor, non-recursive) in the *outer* scope;
3. lowers `recursive` in a scope where the CTE `name` resolves to `LogicalPlan::RecursiveRef { name,
   column_names }` (the self-reference) — this is the one place a CTE name is in scope *within its own
   body*;
4. assembles `LogicalPlan::RecursiveCte { name, column_names, seed, recursive }` and registers
   `name → &RecursiveCte` in the CTE scope so the *outer* body references resolve to it like any CTE (254).

A recursive body that is not the `seed UNION ALL recursive` shape, or whose recursive term does not
reference the CTE, rejects with a precise message.

### Binder

Bind `seed` → schema `S` (the CTE's column types, relabeled by `column_names`). Bind `recursive` in a
context where `RecursiveRef(name)` binds to a relation of schema `S`. Produce
`ExecutionPlan::RecursiveCte { seed, recursive, output_schema: S }`. Seed/recursive output schemas must
union-align (same arity + unifiable types), reusing the existing `Union` arm's schema reconciliation.

### Executor — the fixpoint loop (semi-naive)

`execute_recursive_cte`:

1. `working = execute_bound(seed)`; `result = working.clone()`.
2. loop (bounded by a `RECURSIVE_CTE_MAX_ITERATIONS` safety cap):
   - substitute the working table into the recursive term — clone the bound `recursive` plan, replacing
     each `RecursiveRef` leaf with a `SystemTable` built from `working`'s rows — and `execute_bound` it →
     `next`;
   - if `next` is empty → break (delta-empty termination); else `result.extend(next)`, `working = next`
     (semi-naive: the recursive term sees only the prior iteration's *new* rows — `UNION ALL`, no dedup);
   - if the cap is exceeded → error (`recursive query did not terminate within N iterations`).
3. return `result`.

Termination is explicit: delta-empty (the recursive term self-limits via its `WHERE`) or the cap — no
hidden "hope it stops". (`UNION` distinct's convergence-by-dedup terminator is the deferred form.)

### Distributed: rejected for this cut

`RecursiveCte` executes entirely on one worker (the loop materializes locally). The fragmenter treats it
as an opaque, exchange-free local node (single task); a recursive term that would require an exchange
(e.g. a distributed join/aggregate inside the recursion) is rejected. Distributed fixpoint is a
follow-on.

## Scope

- **In**: `WITH RECURSIVE name[(cols)] AS (seed UNION ALL recursive) SELECT … FROM name`; single
  recursive CTE; one self-reference; a `WHERE` termination predicate in the recursive term; the iteration
  safety cap; local execution.
- **Out / deferred**: **`UNION` (distinct) recursion** — its dedup + convergence-termination is a
  separable sub-mechanism whose primary use is cycle-avoiding graph traversal, which needs a join in the
  recursive term; it defers *together with* joins-in-recursion to the richer follow-on. Also deferred:
  **distributed** recursive execution (exchange inside the recursion); **mutual / multiple** recursive
  CTEs; more than one self-reference; a recursive term that expands beyond the first-cut
  scan+filter+project shape; `INTERSECT`/`EXCEPT` recursive bodies; `SEARCH`/`CYCLE` clauses.

## Discriminators (each fails on `main` — `WITH RECURSIVE` rejects today)

- **`recursive_cte_counter_series_enumerates`** — `WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1
  FROM t WHERE n < 5) SELECT n FROM t` → rows 1..5.
- **`recursive_cte_terminates_on_empty_recursive_term`** — terminates (delta-empty) when the recursive
  term's `WHERE` stops producing rows.
- **`recursive_cte_nonterminating_hits_iteration_cap`** — an unbounded recursion errors at the cap rather
  than hanging.
- **`recursive_cte_outer_query_consumes_result`** — `SELECT … FROM t WHERE …` over the recursive result
  (the outer reference resolves like a CTE).
- **`recursive_cte_union_distinct_and_malformed_shapes_reject`** — `UNION` (distinct) recursion,
  a join/exchange inside the recursion, a non-`UNION ALL` body, and a recursive term that doesn't
  reference the CTE all reject with precise messages.

## Architecture guards

- `adr255_recursive_cte_has_logical_and_execution_nodes` — `RecursiveCte` + `RecursiveRef` exist in
  `ir.rs` / `ir/execution.rs`; `lower_query_scoped` builds them; the `WITH RECURSIVE is not supported`
  reject is gone.
- `adr255_recursive_cte_executes_a_bounded_fixpoint` — `exec.rs` has the fixpoint loop with the iteration
  cap; reuses `SystemTable` injection; `UNION ALL` append, no dedup.
- `adr255_recursive_cte_is_local_only` — the fragmenter keeps `RecursiveCte` in a single stage; a
  recursive term requiring an exchange rejects (distributed deferred).

## Deferred

`UNION` (distinct) recursion (dedup + convergence termination — lands with joins-in-recursion); joins/
aggregates in the recursive term; distributed fixpoint; mutual/multiple recursion; `SEARCH`/`CYCLE`;
`INTERSECT`/`EXCEPT` recursive bodies.

## Forward arc

255 local `UNION ALL` `WITH RECURSIVE` (this) → recursive `UNION` (distinct) + joins-in-recursion (the
dedup/graph-traversal cut) → distributed fixpoint → subquery-expression completion (`IN` / scalar /
uncorrelated `EXISTS`, the deferred 255-alternative) → the standard query surface is broadly complete.
(Window `EXCLUDE` remains the lone window item, externally blocked on a sqlparser bump.)
