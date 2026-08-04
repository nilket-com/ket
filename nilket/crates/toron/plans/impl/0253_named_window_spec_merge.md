# Implementation Plan — ADR 253: Named-Window Spec-Merge

## Summary

Resolve named-window references recursively and merge the allowed clauses into a plain inline
`WindowSpec`. Change `NamedWindowMap` to hold `&NamedWindowExpr`; add `resolve_named_window` (recursive,
cycle-checked) + `merge_window_specs`; replace the 250 inheritance (`:3496`) and extension (`:3926`)
rejects with the resolver. Frontend-only — no IR / binder / executor / fragmenter change.

## Verify-first probe

1. Confirm `RANK() OVER (w ORDER BY score) FROM wins WINDOW w AS (PARTITION BY grp)` and
   `WINDOW w2 AS w1` reject today (`sql_planner.rs:3926` / `:3496`).
2. Confirm the resolved spec feeds the existing partition/order/frame lowering unchanged (the merged
   `WindowSpec` is processed like an inline one — no downstream change).

## Phase 1 — Map holds full definitions

Change `type NamedWindowMap<'a> = HashMap<String, &'a WindowSpec>` (`:7236`) to
`HashMap<String, &'a NamedWindowExpr>`. In `lower_select`'s map build, store the whole
`NamedWindowExpr` (drop the `NamedWindowExpr::NamedWindow` reject — references are now resolved lazily);
keep the duplicate-name reject.

## Phase 2 — Merge + recursive resolver (`sql_planner.rs`)

```
fn merge_window_specs(base: WindowSpec, reference: &WindowSpec) -> Result<WindowSpec, FrontendError>
```
- `!reference.partition_by.is_empty()` → reject ("a window reference cannot add PARTITION BY").
- `order_by`: empty reference → `base.order_by`; non-empty reference requires `base.order_by` empty
  (else reject "cannot override the referenced window's ORDER BY"), then `reference.order_by`.
- `window_frame`: `base.window_frame.is_some()` → reject ("the referenced window already defines a
  frame") — a framed window may not be referenced at all; else `window_frame = reference.window_frame`
  (the referencing window may add a frame only when the base has none).
- Result `WindowSpec { window_name: None, partition_by: base.partition_by, order_by, window_frame }`.

```
fn resolve_named_window(name, map, visiting: &mut Vec<String>) -> Result<WindowSpec, FrontendError>
```
- cycle: `visiting` already contains `name` → reject ("window reference cycle involving '{name}'").
- unknown `name` → reject ("unknown named window '{name}'").
- push `name`; resolve the mapped `NamedWindowExpr`:
  - `WindowSpec(spec)` + `spec.window_name = Some(base)` → `merge(resolve(base, …), spec)`;
  - `WindowSpec(spec)` + no name → `spec.clone()`;
  - `NamedWindow(base)` → `resolve(base, …)` and reject if the resolved base already carries a frame;
- pop `name`.

## Phase 3 — Resolve in `extract_window_call`

Replace the 250 resolution + the `spec.window_name.is_some()` reject (`:3917–3930`) with:

```
let resolved: Cow<WindowSpec> = match over {
    WindowType::WindowSpec(spec) if spec.window_name.is_none() => Cow::Borrowed(spec),
    WindowType::WindowSpec(spec) => Cow::Owned(merge_window_specs(
        resolve_named_window(&spec.window_name.as_ref().unwrap().value, named_windows, &mut Vec::new())?,
        spec,
    )?),
    WindowType::NamedWindow(name) =>
        Cow::Owned(resolve_named_window(&name.value, named_windows, &mut Vec::new())?),
};
let spec = resolved.as_ref();
```

The rest of `extract_window_call` reads `spec.partition_by` / `spec.order_by` / `spec.window_frame`
through the `Cow` deref — unchanged. (`use std::borrow::Cow`.)

## Phase 4 — Tests, guards, verification, seal

### Behavioral gates (`tests/window_test.rs`)

- `over_extension_inherits_base_partition` — `RANK() OVER (w ORDER BY score)` + `WINDOW w AS
  (PARTITION BY grp)` == inline `RANK() OVER (PARTITION BY grp ORDER BY score)` (compare executed rows).
- `window_inheritance_alias_resolves` — `WINDOW w2 AS w1` → `OVER w2` == `OVER w1`.
- `window_inheritance_chain_resolves` — `w3 AS (w2 …)`, `w2 AS (w1 PARTITION BY grp ORDER BY score)`
  resolves; `RANK() OVER w3` matches the inline equivalent.
- `window_alias_to_framed_base_rejects` — `WINDOW w2 AS w1` rejects when `w1` already specifies a
  frame.
- `frame_on_referenced_window_rejects` — `OVER (w ORDER BY …)` over a base with a frame rejects.
- Reconcile `deferred_window_forms_stay_rejected` — drop the inheritance + extension cases (they now
  plan); add `extension_cannot_override_partition_or_order` cases (`OVER (w PARTITION BY …)`,
  `OVER (w ORDER BY …)` over an ordered base) and `WINDOW a AS b, b AS a` (cycle) as the new rejects, or
  dedicated gates `extension_cannot_override_partition_or_order` / `window_reference_cycle_rejects`.

No new distributed gate — a merged spec fragments exactly as its inline equivalent (241/248 cover it).

### Architecture guards (`tests/architecture/window_functions.rs`)

- `adr253_named_window_spec_merge_resolves_in_lowering` — `resolve_named_window` + `merge_window_specs`
  exist; `NamedWindowMap` holds `&NamedWindowExpr`; the `3496`/`3926` reject strings are gone.
- `adr253_spec_merge_is_frontend_only` — no IR / binder / exec / fragmenter surface (`NamedWindowExpr` /
  `merge_window_specs` confined to `sql_planner.rs`).
- `adr253_window_reference_merge_rules_enforced` — the partition / order / cycle reject strings + the
  behavioral gate names are pinned.
- `adr253_referenced_frame_blocks_merge` — a referenced window with a frame rejects, whether referenced
  by alias or extension.
- Reconcile `adr250_named_window_resolves_in_lowering` if it asserts the old `&WindowSpec` map shape or
  the extension reject string (update to the resolver). Reconcile
  `adr252_named_window_spec_merge_stays_deferred` — that 252 boundary guard asserts the 3496/3926
  rejects exist; it must be **removed/retargeted** now that the merge lands (the boundary is closed).

### Verification gate (route every cargo through `scripts/cargo-cgroup`)

```
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test window_test
scripts/cargo-cgroup test  -p toron --test architecture_test adr253
scripts/cargo-cgroup test  -p toron --test architecture_test        # FULL (reconcile 250/252 guards)
scripts/cargo-cgroup test  -p toron --test distributed_process_test # FULL (regression; no new gate)
scripts/cargo-cgroup test  -p toron --lib                           # no IR/wire change
cargo fmt -p toron -- --check
git diff --check
```

### Seal

Commit the implementation as `impl: adr 253 named window spec merge` with the
`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

## Risk notes

- **252 boundary guard**: `adr252_named_window_spec_merge_stays_deferred` deliberately pins the
  3496/3926 rejects — it MUST be retargeted/removed in this cut (the boundary it guarded is now the
  feature). Expect it to fail the FULL architecture run until reconciled.
- **Cow deref**: hold the resolved spec long enough that the borrowed/owned `Cow` outlives its use in
  `extract_window_call`; bind it to a `let` before `spec.as_ref()`.
- **Cycle set cost**: a `Vec<String>` `visiting` is fine (named-window counts are tiny); reject on
  re-entry, not on map size.
- **Frontend-only**: a merged spec is an ordinary `WindowSpec`; `--lib` / goldens unaffected (no IR
  change), and no distributed gate is added.
