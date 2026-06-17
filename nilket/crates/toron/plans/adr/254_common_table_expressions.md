# ADR 254 — Common Table Expressions (non-recursive `WITH`)

## Status

Proposed.

## Context

`WITH … SELECT …` rejects at every statement context (`sql_planner.rs:3239` query body, plus
`:1522`/`:8252`/`:8268` DML/EXPLAIN). CTEs are the single largest absent standard SQL surface after the
window arc — and a direct continuation of the named-fragment theme: ADRs 250/253 gave *named, reusable
spec fragments resolved at lowering*; CTEs are *named, reusable subquery fragments resolved at lowering*,
one level up (query, not window).

Set operations (`lower_set_operation`), joins, aggregation, DISTINCT, and derived tables already land, so
the substrate a CTE body needs is present. The Rune equivalent **pre-exists** (bind a dataframe to a host
variable and reuse it — `let a = df.filter(…); a.join(a, …)`), exactly as named-window reuse was N/A for
Rune (250/253): 254 is a **SQL-compatibility** cut, Rune N/A — not a Rune-surface gap.

## Decision

Implement **non-recursive CTEs as a frontend desugar** in `sql_planner.rs` — no IR / binder / executor /
fragmenter change. A `WITH` clause builds a **CTE scope** (`name → lowered LogicalPlan`); a
`TableFactor::Table` whose name hits the scope resolves to that plan **aliased as a named relation**
(reusing `alias_plan`), instead of a catalog `ScanNamed`.

### Grounding — CTEs are "named subplans", NOT "named derived tables" (the load-bearing receipt)

The naive framing — "substitute a CTE reference as a `TableFactor::Derived`" — **under-delivers and is
self-contradictory**: derived tables route through `validate_derived_select_shape`, which rejects
**joins inside, nesting, and set-op bodies**, so `WITH a AS (SELECT … JOIN …)` — the *whole point* of a
CTE — would reject. A cut must deliver its name.

The CTE body is therefore lowered via the **full** query path (`lower_query` for the CTE query, then its
`body` through `lower_query_body` → `lower_select` / `lower_set_operation`, join/aggregate/set-op
capable — the same path a top-level query uses), and the result is presented as a named relation via
`alias_plan` (→ `LogicalPlan::Alias`). This is sound because
the binder's `LogicalPlan::Alias` arm (`binder.rs:965`) is **structure-agnostic**: it binds the input
(whatever it is — a `Join`, `Aggregate`, `Union`), takes its *output schema*, and builds
`RelationScope::alias` over that — the inner structure is transparent.

**Empirically grounded** (iter-0 probe, since reading bit us on 119/120/121): with the derived-table join
fence temporarily lifted, `SELECT d.id, d.right_value FROM (SELECT left_t.id, right_t.right_value FROM
left_t JOIN right_t ON left_t.id = right_t.id) d WHERE d.id = 1` **plans and binds** — a join-bodied
subplan referenced as a named relation, *filtered on the alias's columns at the outer level*. So the
join fence is frontend-arbitrary, not binder-fundamental.

### Scope, shadowing, column aliases

- **Linear scope** — CTEs are visible to the main body and to *subsequently defined* CTEs in the same
  `WITH` block (`WITH a AS (…), b AS (SELECT … FROM a)`), built incrementally so each body lowers with the
  earlier CTEs in scope. A CTE referencing itself or a later sibling is **unknown** (non-recursive); no
  cycle machinery is needed (forward/self references simply miss the scope). The derived-table
  joins-inside fence falls out of the same capability and stays folded into this ADR.
- **Shadowing** — the CTE scope is consulted **before** the catalog in `lower_table_factor`, so a CTE
  named `users` shadows the Wendelon `users` table for that query (standard SQL).
- **Column aliases** — `WITH a(x, y) AS (SELECT p, q …)` reuses `alias_plan`'s existing `columns` rename
  (the same path derived tables use), arity-checked against the body's output.

### Threading (the bulk of the work)

`SqlPlanner` is stateless and `named_windows` already threads as an explicit `&NamedWindowMap` parameter —
so the CTE scope threads the same way (explicit param, no hidden mutable re-entry state): a
`&CteScope` flows `lower_query` → `lower_query_body` → `lower_select` → the FROM/join lowering →
`lower_table_factor` (and into `lower_derived_table`, so a derived table or subquery inside the body can
also reference a CTE). The scope is empty (a borrow of a shared empty map) when there is no `WITH`.

## Scope

- **In**: non-recursive `WITH` (single + multiple CTEs), linear earlier-CTE references, join / aggregate /
  set-op CTE bodies, CTE referenced in `FROM` and inside joins, catalog shadowing, `WITH a(x, y)` column
  aliases; **and** removal of the **derived-table joins-inside fence**
  (`validate_derived_select_shape`'s `joins inside derived tables` reject). The probe proved the binder
  already presents a join-bodied subplan as a named relation, so "joins inside derived tables" is **not a
  separate mechanism** — it is the same capability CTEs need, exercised from a different syntax entry
  point. Keeping it out would leave 254 artificially narrow and push a pointless follow-on ADR for no
  architectural gain.
- **Out / deferred (genuine mechanism boundaries)**: **nested derived tables** and **lateral** (separate
  fences, not grounded by the probe); `WITH RECURSIVE` (fixpoint iteration — its own engine mechanism, its
  own ADR); CTEs as DML/EXPLAIN statement targets (`:8252`/`:8268` — follow-on); shared materialization of
  a multiply-referenced CTE (inline-per-reference is correct; materialize-once is an optimization, not a
  correctness need).

## Discriminators (each fails on `main` — `WITH` rejects today)

- **`cte_resolves_like_inline_subquery`** — `WITH a AS (SELECT … FROM t WHERE …) SELECT … FROM a` ==
  the inline spelling.
- **`cte_body_with_join_binds_and_executes`** — `WITH a AS (SELECT … FROM x JOIN y …) SELECT … FROM a`
  plans + executes (the deliver-the-name gate; the case the derived-table framing would fail).
- **`multiple_ctes_later_references_earlier`** — `WITH a AS (…), b AS (SELECT … FROM a) SELECT … FROM b`.
- **`cte_referenced_in_a_join`** — `WITH a AS (…) SELECT … FROM t JOIN a ON …`.
- **`cte_shadows_catalog_table`** — a CTE named like a catalog table reads the CTE, not the table.
- **`cte_column_alias_list_renames_output`** — `WITH a(x, y) AS (SELECT p, q …) SELECT x, y FROM a`.
- **`recursive_and_self_reference_ctes_stay_rejected`** — `WITH RECURSIVE …` and a self/forward reference
  reject.

## Architecture guards

- `adr254_cte_resolves_in_lowering` — `WITH` no longer rejects; a CTE scope (`name → plan`) is built in
  `lower_query` and consulted in `lower_table_factor` before the catalog; the body lowers via the full
  query path (not the derived-table validator).
- `adr254_cte_is_frontend_only` — no IR / binder / executor / fragmenter surface; the scope + resolution
  live in `sql_planner.rs`.
- `adr254_recursive_cte_stays_deferred` — `WITH RECURSIVE` rejects (the next mechanism boundary).

## Deferred

- **`WITH RECURSIVE`** — fixpoint iteration; a distinct execution mechanism (its own ADR, the 255 lead).
- **CTEs in DML / EXPLAIN targets** — the `:8252`/`:8268` reject sites; a thin follow-on once query-level
  CTEs land.
- **Shared CTE materialization** — an optimization over inline-per-reference.

## Forward arc

254 non-recursive CTEs (this) → 255 `WITH RECURSIVE` → subquery-expression completion (`IN (subquery)` /
scalar subqueries / uncorrelated `EXISTS`) → the standard query surface is broadly complete. (Window
`EXCLUDE` remains the lone window item, externally blocked on a sqlparser AST bump — Codex's iter-0 point,
unchanged.)
