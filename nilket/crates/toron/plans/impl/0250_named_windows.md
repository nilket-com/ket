# Implementation Plan — ADR 250: Named Windows

## Summary

Resolve `func() OVER w` against the SELECT's `WINDOW w AS (<spec>)` definitions in the lowering.
Frontend-only: build a name→`&WindowSpec` map in `lower_select`, thread it to
`lower_select_window` → `extract_window_call`, resolve `WindowType::NamedWindow`. No IR / binder /
executor / fragmenter change; a resolved named window is an ordinary inline spec.

## Verify-first probe

1. Confirm `SELECT RANK() OVER w FROM wins WINDOW w AS (PARTITION BY grp ORDER BY score)` rejects
   today (`lower_select` "WINDOW is not supported" at `sql_planner.rs:3487`; `extract_window_call`
   "named WINDOW references are not supported" at `:3909`).
2. Confirm `lower_select` destructures `select.named_window` (3445) and that
   `lower_select_window` (3800) / `extract_window_call` (3896) are reachable from it — the threading
   path.
3. The two `!select.named_window.is_empty()` references (`:8171`, `:8213`) are in
   `validate_derived_select_shape` / `validate_exists_select_shape` — they reject named windows inside
   derived tables / EXISTS subqueries. **Leave them as-is**: windows in those nested positions are
   already unsupported (`deferred_window_forms_stay_rejected` pins "window inside a derived table"), so
   named windows there stay rejected too. The desugar lands only in top-level `lower_select`.

## Phase 1 — Collect definitions in `lower_select`

Replace the `if !named_window.is_empty() { return Err(...) }` reject (`sql_planner.rs:3487`) with a
map build:

- For each `NamedWindowDefinition(name, expr)`:
  - `NamedWindowExpr::WindowSpec(spec)` → insert `name.value → &spec` (key by the identifier,
    following existing identifier-resolution casing; duplicate definition names reject).
  - `NamedWindowExpr::NamedWindow(_)` → reject ("window inheritance is not supported"): deferred.
- Pass the map (e.g. `&HashMap<String, &WindowSpec>`, borrowing from `select`) into
  `lower_select_window`. An empty map (no `WINDOW` clause) is the common path and changes nothing.

## Phase 2 — Thread the map

Add a `named_windows: &HashMap<String, &WindowSpec>` parameter to `lower_select_window` (3800) and
`extract_window_call` (3896). The map's `&WindowSpec` borrows live as long as `&Select`, so the
references outlive the lowering. No clone of the spec is needed.

## Phase 3 — Resolve in `extract_window_call`

At the OVER match (`sql_planner.rs:3907`):

- `WindowType::NamedWindow(name)` → look up `name` in the map. Found → use that `&WindowSpec` as
  `spec` and continue exactly as the inline path. Missing → `Err(unsupported("unknown named window
  '<name>'"))`.
- `WindowType::WindowSpec(spec)` with `spec.window_name.is_some()` → keep rejecting, reworded to name
  the deferral ("window reference inside OVER (...) is not supported; use OVER <name>") — the
  extension/inheritance form.
- A resolved definition spec has `window_name = None` (it is a definition, not a reference), so it
  flows through the existing partition/order/frame handling untouched.

## Phase 4 — Tests, guards, verification, seal

### Behavioral gates (`tests/window_test.rs`)

Reuse the `wins` fixture.
- `named_window_resolves_like_inline_spec` — `RANK() OVER w` with `WINDOW w AS (PARTITION BY grp
  ORDER BY score)` produces the same ranks as the inline form (compare `group_values`).
- `named_window_shared_across_calls_is_one_node` — `RANK() OVER w, ROW_NUMBER() OVER w` →
  `count_window_nodes == 1`, `first_window_functions == Some(2)`.
- `distinct_named_windows_stack` — two definitions `w1`/`w2` with different partitions →
  `count_window_nodes == 2`.
- `unknown_named_window_rejects` — `RANK() OVER w` with no `WINDOW w AS` → planner error.
- Reconcile `deferred_window_forms_stay_rejected` — the `OVER w` cases now plan; add
  `WINDOW w2 AS w1` (inheritance) and `... OVER (w ORDER BY ...)` (extension) as the new rejects.

(Frame coverage: also assert a named window carrying a frame works, e.g. `SUM(score) OVER w` with
`WINDOW w AS (PARTITION BY grp ORDER BY score ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)`, proving the
resolved spec inherits 243.)

### Architecture guards (`tests/architecture/window_functions.rs`)

- `adr250_named_window_resolves_in_lowering`
- `adr250_named_windows_are_frontend_only`
- `adr250_window_inheritance_and_extension_stay_rejected`

No new distributed gate — a resolved named window fragments identically to its inline equivalent,
already covered by the 241/248 distributed gates (note this in the ADR, do not add a redundant gate).

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr250
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL — reconciled fences
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL (no new gate; regression)
scripts/cargo-cgroup test  -p toron --lib                           # no IR/wire change expected
cargo fmt -p toron -- --check
git diff --check
```

### Seal

Commit the implementation as `impl: adr 250 named windows` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **Threading churn**: adding a parameter to `lower_select_window` / `extract_window_call` touches
  their call sites — `--all-targets` / the compiler catch any missed caller.
- **Identifier casing**: key the map by the same normalization the binder uses for column/table
  identifiers, so `OVER W` resolves `WINDOW w AS ...` iff Toron treats those as the same identifier
  elsewhere. Match existing behavior; do not invent new casing rules.
- **Derived/EXISTS subquery validators** (`:8171`, `:8213`): these stay — they reject named windows
  inside derived tables / EXISTS subqueries, consistent with windows being unsupported there. Do not
  relax them; the desugar is top-level-only.
- **Scope discipline**: only `OVER w` lands; `OVER (w ...)` and `WINDOW w2 AS w1` reject. The
  `window_inheritance_and_extension_stay_rejected` gate pins this so the boundary cannot silently
  erode.
