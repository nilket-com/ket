# ADR 176: Semantic Binder Decomposition

**Status:** Promoted. **Migration:** `plans/impl/176_semantic_binder_decomposition.md`.

`crates/toron/src/binder.rs` is **22,819 lines**. ADR 176 splits it into a bounded
`binder.rs` root + a `binder/` directory of **six semantic** submodules — cohesive
units (arithmetic, pushdown, aggregate, join, selectivity, explain), all landed this
round. It is a **behavior-neutral relocation**: the honesty check is that existing lib
and architecture behavior gates stay green, the new ADR176 structure guards pass, and
`git diff` is moves + narrow visibility changes + `mod` declarations + the
test-source-helper consolidation — no logic edits.

## Grounding

- **Half the file is one test block.** Code runs to 11,974; a single
  `#[cfg(test)] mod tests { … }` (line 11,976 → EOF, ~10.8k lines) is the rest.
- **378 top-level code items** to distribute across six cohesive modules.
- **House precedent, no `#[path]` needed.** `exec.rs` declares `pub(crate) mod
  runtime_filter;` which auto-resolves to `exec/runtime_filter.rs` (Rust file+dir
  rule). So `binder.rs` uses plain `mod arithmetic;` → `binder/arithmetic.rs`; keep
  `binder.rs` as the root (not `binder/mod.rs`, avoiding a spurious rename).
- **Rust module loading is not the cycle risk.** `binder.rs` can own child modules
  through plain `mod` declarations, and sibling modules can call each other through
  explicit paths. The real discipline is ownership and visibility: prefer private or
  `pub(super)` helpers; reserve `pub(crate)` for APIs that genuinely cross the binder
  boundary.
- **The one real hazard: scattered architecture probes read `src/binder.rs` as a single
  file** to feed `source_block(&binder, "fn …")` assertions. Local grep finds 594
  binder-oriented `binder_source` / `source("src/binder.rs")` /
  `source_block(&binder, ...)` style hits, with raw reads spread across predicate
  pushdown, SQL planner surface, storage/Wendelon boundary, null semantics, cockpit,
  temporal, converted predicate pushdown, and optimizer/statistics guard modules. A
  move out of `binder.rs` breaks any stale single-file reader unless all binder source
  scans are centralized first.

## Decisions

**D1 - Keystone first: centralized directory-aware binder source helpers.** Add
canonical helpers to `tests/architecture/support.rs`: `binder_root_source()` for the
few guards that intentionally inspect the facade itself, `binder_source_files()` for
the deterministic file list, and `binder_source()` for aggregate scans. The aggregate
source concatenates `src/binder.rs` plus every Rust file under `src/binder/`, with the
facade first, production modules next, and test files last (so a moved code fn always
matches before a same-named test fn). Delete duplicate local helpers and reroute every
`source("src/binder.rs")` / raw `read_to_string(... "src/binder.rs")` binder scan
through the shared helper. `source_block`/`first_block` are unchanged. While `binder/`
is empty this is a no-op (`binder.rs` only), so it lands first and the full architecture
suite is green before any code moves.

**D2 - Extract the test module.** Move the single `#[cfg(test)] mod tests { … }` body
to `src/binder/tests.rs`; replace it in the root with `#[cfg(test)] mod tests;`. Keep
test access narrow: the extracted test module remains under `binder`, so most parent
access should not require widening. Where moved sibling modules force visibility,
prefer `pub(super)`; use `pub(crate)` only for real non-binder callers. ~halves the
root.

**D3 - Carve all six semantic submodules this round.** Create
`src/binder/{arithmetic,pushdown,aggregate,join,selectivity,explain}.rs`, declared via
plain `mod …;` in the root. Move clusters compile-driven (move → `cargo check` → fix
`use` paths → widen only where needed → repeat):

- `arithmetic` — `bind_arithmetic`, `bind_dimensioned_arithmetic`,
  `bind_logical_arithmetic`, `bind_point_delta_arithmetic`, `bind_sql_convert`,
  `arithmetic_symbol`, unit/identity domain enums + helpers.
- `pushdown` — `predicate_pushdown_plan`, `partition_bound_predicate`, `compile_pbc`,
  `plan_filter_with_predicate_pushdown`, `attach_pbc_program`, static-filter classing,
  173 converted matcher/derivation.
- `aggregate` — `aggregate_schema`/`aggregate_field`/`aggregate_output_logical_type`,
  `additive_unit_aggregate_logical_type`.
- `join` — `bind_join`, `choose_join_strategy`, `multi_key_join_cardinality`, join
  `DecisionReceipt`/`JoinStrategyDecisionReason`.
- `selectivity` — range/equality/IN-list estimators, `converted_linear_range_atom`,
  `FilterEstimateSource`.
- `explain` — `OptimizerTrace`, `ScanFilterReceipt`/outcome, 175 render sidecar,
  `optimizer_explain_*`.
- root `binder.rs` — the `Binder` struct, `bind`/`bind_with_trace`/`bind_explain`,
  `bind_plan_with_trace` dispatch, shared small helpers, `mod` decls.

A function lands with the cluster it serves; boundaries are semantic-cohesion-led.

**D4 - Pure moves + visibility only.** No logic edits, no simplification, no renames
(flag if a move forces a collision). Intra-module items stay private. Cross-module
binder helpers widen to `pub(super)` by default; `pub(crate)` is allowed only for
pre-existing crate-visible APIs or helpers with real non-binder callers. No
crate-external (`pub`) API change.

**D5 - One ADR, all six modules.** All of D1-D3 land in ADR 176. A follow-on (ADR 177)
is a **true-overrun safety net only** — if the move-diff proves unreviewable in one
pass, the keystone makes a partial carve guard-safe, but the intent and target is all
six this round.

**D6 - Behavior-neutral honesty check.** The gate: existing lib and architecture
behavior suites stay green, ADR176 structure guards pass, `cargo check -p toron
--all-targets` is clean, a source scan shows zero ad hoc single-file binder readers
outside the shared architecture helpers, and a diff scan confirms moves + narrow
visibility + `mod` declarations + helper consolidation only. "No logic edit" is not
fully mechanically assertable — the existing behavior suite plus diff review is the
proof; structural guards pin the shape.

**D7 - No functional surface.** No executor, PBC, storage, Wendelon, descriptor,
optimizer-fact, wire, or frontend behavior change. Pure source reorganization.

## Headline Gates

- `binder_suites_unchanged` - existing Toron lib and architecture behavior gates stay
  green after the split, with ADR176 structure guards added.
- `binder_source_is_directory_aware` - the consolidated `binder_source()` concatenates
  `binder.rs` + binder child Rust files in facade/production/test order; every
  `source_block` guard resolves.
- `binder_root_is_bounded` - `src/binder.rs` drops far below 22,819 (the `Binder`
  struct + entry points + dispatch + `mod` decls; no inline tests, no carved cluster
  bodies).

## Supporting Gates

- `binder_semantic_modules_exist` - the six `binder/*.rs` + `binder/tests.rs` exist and
  compile.
- `all_source_block_guards_green` - every `adrNNN_*` guard using `source_block(&binder,
  …)` passes with no per-guard retarget.
- `no_residual_single_file_binder_read` - no ad hoc `read_to_string(... "src/binder.rs")`
  or `source("src/binder.rs")` remains outside the consolidated support helper and the
  ADR176 guard that verifies it.

## Architecture Guards

- `adr176_binder_source_concatenates_submodules` - `support::binder_source` enumerates
  `binder/` recursively (reads `binder.rs` + child Rust files in
  facade/production/test order); single aggregate definition.
- `adr176_no_scattered_binder_file_reads` - architecture modules do not directly read
  `src/binder.rs`; aggregate scans use `binder_source()`, and root-only checks use
  `binder_root_source()`.
- `adr176_binder_root_excludes_carved_clusters` - the root file, read through
  `binder_root_source()`, no longer defines the carved cluster fns (e.g. `fn
  predicate_pushdown_plan`, `fn aggregate_schema`, `fn choose_join_strategy`); it
  retains `struct Binder` + `fn bind_plan_with_trace`.
- `adr176_test_module_is_extracted` - `binder/tests.rs` exists; `binder.rs` declares
  `mod tests;`, not an inline `mod tests {`.
- `adr176_semantic_modules_exist` - the six modules exist and are declared in the root.
- `adr176_no_runtime_or_wire_surface_change` - the diff touches only `src/binder.rs`,
  `src/binder/`, and `tests/architecture/` source helpers.

## Non-Goals

- no logic change, simplification, or decoupling (relocation only);
- no public API change; `pub(crate)` widening is allowed only for APIs that already
  have non-binder callers;
- no per-module test redistribution (the extracted `tests.rs` stays one file;
  co-location is an optional later cleanup);
- no `#[path]` / no `binder/mod.rs` (plain `mod` auto-resolution); no functional change.

## Consensus Trail

Operator flagged the 22,819-line `binder.rs` and scoped one ADR covering all six
modules. Grounding found the cheap structure (one test block = half the file; plain
`mod` auto-resolution per `exec.rs`) and the one real risk: scattered architecture
guards read `binder.rs` as a single file across several patterns. The risk is defused
by shared directory-aware `binder_source` / root-only `binder_root_source` helpers
landed first and used everywhere before any code moves. The cut is behavior-neutral,
proven by the existing behavior suites staying green.

## Forward Arc

`176 semantic binder decomposition -> later: genuine decoupling / trait extraction
(simplification) / per-module test co-location / same decomposition for other oversized
files`.

After 176 the binder is a bounded root + six cohesive semantic modules, navigable and
guard-stable, with no behavior change.

## Deferred (named)

Genuine decoupling/trait extraction (simplification, not relocation); per-module test
co-location; decomposition of other oversized files; ADR 177 only as a true-overrun
fallback.
