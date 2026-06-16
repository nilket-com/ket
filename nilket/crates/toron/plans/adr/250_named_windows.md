# ADR 250 — Named Windows

## Status

Proposed.

## Context

The window arc (241–249) covers the full standard analytic **function** surface. The remaining
common **syntax** is the named window: define an OVER spec once in a `WINDOW` clause and reference
it by name, so several columns share one spelling.

```sql
SELECT customer,
       RANK()       OVER w AS r,
       ROW_NUMBER() OVER w AS n,
       SUM(sales)   OVER w AS running
FROM orders
WINDOW w AS (PARTITION BY region ORDER BY sales)
```

Today this rejects in three places: `lower_select` drops a non-empty `WINDOW` clause
(`sql_planner.rs:3487`, "WINDOW is not supported"), and `extract_window_call` rejects both
reference forms — `WindowType::NamedWindow` (`:3909`) and `spec.window_name` (`:3913`).

### Grounding — a pure frontend desugar

`lower_select` (`sql_planner.rs:3424`) already destructures `select.named_window` (the
`Vec<NamedWindowDefinition>` of `name AS (...)` definitions) and currently rejects it. Window calls
flow `lower_select` → `lower_select_window` → `extract_window_call`. So named windows are a
**lowering desugar**: collect the definitions into a name→spec map, thread it down, and resolve
`OVER w` to the named spec before the existing ranking/aggregate/value/NTILE classification runs. A
resolved named window is an ordinary `WindowSpec` (partition + order + optional frame), so it
inherits all of 242–249 unchanged and composes with ADR 248 for free — calls sharing `w` group into
one Window node; distinct named windows stack. **No IR, binder, executor, or fragmenter change.**

## Decision

Resolve `func() OVER w` against the SELECT's `WINDOW w AS (<spec>)` definitions, in the lowering.

- `lower_select` builds a name→`&WindowSpec` map from `select.named_window`, keyed by the definition
  identifier (following the existing identifier-resolution casing). It passes the map to
  `lower_select_window`, which threads it to `extract_window_call`.
- In `extract_window_call`, `WindowType::NamedWindow(name)` resolves to the mapped spec; an
  unreferenced name rejects ("unknown named window `w`"). The resolved spec is processed exactly as
  an inline `WindowType::WindowSpec` — partition, order, and any frame the definition carries.
- A `WINDOW` definition that is itself a reference (`NamedWindowExpr::NamedWindow`, i.e.
  `WINDOW w2 AS w1`) rejects at map-build (window inheritance, deferred).
- The `OVER (w ...)` extension form (`spec.window_name.is_some()`) still rejects — it merges a base
  spec with added clauses, a distinct mechanism (deferred). Only the `OVER w` reference form lands.

### Scope boundary

`OVER w` (a bare named reference) is the common form and the whole of this cut. Window
inheritance (`WINDOW w2 AS w1`) and extension (`OVER (w ORDER BY ...)`) both *merge* two specs and
are a separate mechanism — deferred, with explicit rejects so the boundary is pinned.

## Consequences

- The practical named-window syntax surface is complete for bare references; analysts reuse one
  spec across many columns.
- No new mechanism: a resolved named window is an inline spec, so binding, execution, fragmentation
  (single spec → 241; multiple → 248), and every function family behave identically. No new
  distributed gate is needed — a named window fragments exactly as its inline equivalent, already
  covered by the 241/248 distributed gates.

## Discriminators (each fails on `main` — named windows reject today)

- **`named_window_resolves_like_inline_spec`** — `SELECT grp, RANK() OVER w FROM wins WINDOW w AS
  (PARTITION BY grp ORDER BY score)` plans and executes identically to the inline
  `RANK() OVER (PARTITION BY grp ORDER BY score)` (same ranks).
- **`named_window_shared_across_calls_is_one_node`** — `RANK() OVER w, ROW_NUMBER() OVER w` with one
  `WINDOW w AS (...)` → `count_window_nodes == 1` (composes with 248 grouping).
- **`distinct_named_windows_stack`** — `RANK() OVER w1, ROW_NUMBER() OVER w2` with two definitions →
  `count_window_nodes == 2` (composes with 248 stacking).
- **`unknown_named_window_rejects`** — `RANK() OVER w` with no `WINDOW w AS` rejects.
- **`window_inheritance_and_extension_stay_rejected`** — `WINDOW w2 AS w1` and
  `... OVER (w ORDER BY ...)` reject (the deferred fence).

## Architecture guards

- `adr250_named_window_resolves_in_lowering` — `lower_select` builds the definition map and
  `extract_window_call` resolves `WindowType::NamedWindow`; the "WINDOW is not supported" reject is
  gone.
- `adr250_named_windows_are_frontend_only` — no IR / binder / executor / fragmenter surface (the
  desugar lives entirely in `sql_planner.rs`).
- `adr250_window_inheritance_and_extension_stay_rejected` — `NamedWindowExpr::NamedWindow` and
  `spec.window_name` reject (deferred mechanism).

## Deferred

- **Window inheritance / extension** (`WINDOW w2 AS w1`, `OVER (w ...)`) — a spec-merge mechanism.
- **IGNORE / RESPECT NULLS** — value-function null-skipping mechanism.
- **EXCLUDE** frame clause — sqlparser exposes no field for it; external blocker.

## Forward arc

250 named windows (this) → remaining deferred window surface: inheritance / extension,
IGNORE NULLS, and EXCLUDE; ready to branch.
