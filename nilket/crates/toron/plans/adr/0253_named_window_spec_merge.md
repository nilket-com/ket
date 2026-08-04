# ADR 253 — Named-Window Spec-Merge (Inheritance + Extension)

## Status

Proposed.

## Context

ADR 250 resolves a *bare* `OVER w` against the SELECT's `WINDOW w AS (...)` definitions and rejects the
two **merge** forms:

- **Extension** `func() OVER (w ORDER BY … [frame])` — `sql_planner.rs:3926` (`spec.window_name.is_some()`).
- **Inheritance** `WINDOW w2 AS w1` / `WINDOW w2 AS (w1 ORDER BY …)` — `sql_planner.rs:3496`
  (`NamedWindowExpr::NamedWindow`).

Both name a base window and add clauses. This is the last implementable window feature — a SQL-only
frontend desugar over 250's `NamedWindowMap`. Rune is N/A (host-variable reuse covers spec reuse).
After 253 the window surface is complete bar the sqlparser-blocked EXCLUDE.

## Decision

Resolve named-window references **recursively** and **merge only the allowed pieces** into a plain
inline `WindowSpec` (`window_name = None`), which then flows through the existing
ranking/aggregate/value/NTILE classification + frame validation unchanged. Frontend-only — no IR /
binder / executor / fragmenter change, no new distributed gate (a merged spec is an ordinary spec,
covered by the 241/248 gates).

### The merge rule `merge(base, reference)`

`reference` names `base` and may add clauses:

- **PARTITION BY inherits, never overridable** — `reference.partition_by` non-empty → reject
  ("a window reference cannot add PARTITION BY").
- **ORDER BY inherits, never replaced** — `reference.order_by` non-empty requires `base.order_by`
  empty, else reject; otherwise inherit `base.order_by`.
- **Frame belongs to the referencing window** — the referenced/base window must not already specify a
  frame; if it does, the merge rejects. The referencing window may add a frame only when the base has
  none. A pure alias `WINDOW w2 AS w1` inherits only if `w1` is frame-free.
- The result has `window_name = None` (now a plain inline spec).

### Recursive resolution + cycle detection

`NamedWindowMap` changes from `HashMap<String, &WindowSpec>` to `HashMap<String, &NamedWindowExpr>` so a
definition may itself be a reference. `resolve_named_window(name, map, visiting)`:

- `NamedWindowExpr::WindowSpec(spec)` with `spec.window_name = Some(base)` → `merge(resolve(base), spec)`;
  with no name → the plain spec.
- `NamedWindowExpr::NamedWindow(base)` (the bare `WINDOW w2 AS w1` alias) → `resolve(base)` and
  reject if the resolved base already carries a frame.
- A `visiting` set rejects cycles (`WINDOW a AS b, b AS a` → error, not a hang).

In `extract_window_call`: `WindowType::NamedWindow(name)` → `resolve(name)`; `WindowType::WindowSpec(spec)`
with `spec.window_name = Some(base)` → `merge(resolve(base), spec)` (replaces the 3926 reject); a plain
inline spec is unchanged. The resolved spec is held as `Cow<WindowSpec>` (borrowed for the plain inline
path, owned for a merge) so the common case clones nothing.

## Scope

- **In**: extension (`OVER (w …)`), inheritance (`WINDOW w2 AS w1` and `WINDOW w2 AS (w1 …)`), multi-level
  chains, cycle rejection, the three merge rules.
- **Out**: EXCLUDE (sqlparser-blocked); aggregate IGNORE NULLS (252).

## Discriminators (each fails on `main` — both forms reject today)

- **`over_extension_inherits_base_partition`** — `RANK() OVER (w ORDER BY score)` with
  `WINDOW w AS (PARTITION BY grp)` plans + executes identically to inline
  `RANK() OVER (PARTITION BY grp ORDER BY score)`.
- **`window_inheritance_alias_resolves`** — `WINDOW w2 AS w1` makes `OVER w2` == `OVER w1`.
- **`window_inheritance_chain_resolves`** — `w3 AS (w2 …)`, `w2 AS (w1 PARTITION BY grp)` resolves the
  chain (locks multi-level).
- **`frame_on_referenced_window_rejects`** — `WINDOW w2 AS w1` rejects when `w1` already has a frame,
  and `OVER (w ORDER BY …)` over such a base rejects too.
- **`extension_cannot_override_partition_or_order`** — `OVER (w PARTITION BY …)`, and `OVER (w ORDER BY …)`
  over a base that already has ORDER BY, both reject.
- **`window_reference_cycle_rejects`** — `WINDOW a AS b, b AS a` rejects (terminates, no hang).

## Architecture guards

- `adr253_named_window_spec_merge_resolves_in_lowering` — the 3496 / 3926 rejects are gone;
  `resolve_named_window` + `merge` live in the lowering; `NamedWindowMap` holds `&NamedWindowExpr`.
- `adr253_spec_merge_is_frontend_only` — no IR / binder / executor / fragmenter surface.
- `adr253_window_reference_merge_rules_enforced` — partition-not-overridable / order-not-overridable /
  cycle rejects are pinned (the behavioral gate names).

## Deferred

- **EXCLUDE** frame clause — sqlparser 0.62 `// TBD: EXCLUDE`, externally blocked.

## Forward arc

253 named-window spec-merge (this) → **window surface complete** (only EXCLUDE deferred, on the
sqlparser external blocker).
