# Impl Plan - ADR 176: Semantic Binder Decomposition

Pairs with `plans/adr/176_semantic_binder_decomposition.md`.

Split `binder.rs` (22,819 lines) into a bounded root + six `binder/` semantic
submodules, all this round. Keystone (consolidated directory-aware source helpers)
lands first so the architecture guards keep seeing moved binder code. Behavior-neutral:
moves + narrow visibility changes + `mod` declarations + the test-helper
consolidation; existing behavior gates stay green and ADR176 structure guards pin the
new shape.

## Anchors

| Concern | Anchor |
|---|---|
| Module dir, plain `mod` | `exec.rs:87` `pub(crate) mod runtime_filter;` (auto-resolves; keep `binder.rs` as root, no `binder/mod.rs`) |
| Test block (single) | `#[cfg(test)] mod tests {` at `binder.rs:11976` → EOF |
| Scattered binder source readers | Local `fn binder_source()` dupes, `source("src/binder.rs")`, and raw `read_to_string(... "src/binder.rs")` calls across architecture modules; inventory with the Phase 1 grep rather than relying on a fixed hand count |
| Shared support | `support.rs::manifest_dir()` (`:252`) |
| Semantic clusters | per ADR D3 |

## Phase 1 - Keystone: consolidated directory-aware binder source helpers

Add to `tests/architecture/support.rs`:

```rust
pub(crate) fn binder_root_source() -> String {
    std::fs::read_to_string(manifest_dir().join("src/binder.rs"))
        .expect("read binder root source")
}

pub(crate) fn binder_source_files() -> Vec<PathBuf> {
    let src = manifest_dir().join("src");
    let module_root = src.join("binder");
    let mut production = Vec::new();
    let mut tests = Vec::new();

    if module_root.exists() {
        for path in source_files(&module_root) {
            let is_test_file = path
                .strip_prefix(&module_root)
                .ok()
                .and_then(|relative| relative.components().next())
                .is_some_and(|component| {
                    component.as_os_str() == std::ffi::OsStr::new("tests")
                });
            if is_test_file || path.file_name() == Some(std::ffi::OsStr::new("tests.rs")) {
                tests.push(path);
            } else {
                production.push(path);
            }
        }
    }

    production.sort();
    tests.sort();

    let mut paths = vec![src.join("binder.rs")];
    paths.extend(production);
    paths.extend(tests);
    paths
}

pub(crate) fn binder_source() -> String {
    binder_source_files()
        .into_iter()
        .map(|p| std::fs::read_to_string(p).expect("read binder source"))
        .collect::<Vec<_>>().join("\n")
}
```

- **Delete per-file `binder_source()` dupes**; import the shared one.
- **Replace `source("src/binder.rs")` binder scans** with `binder_source()`.
- **Replace raw inline reads** `read_to_string(... "src/binder.rs")` used for aggregate
  binder scans with `binder_source()`.
- Use `binder_root_source()` only for guards that intentionally assert the facade file
  itself, such as `adr176_binder_root_excludes_carved_clusters`.
- Leave generic `source(relative)` for non-binder files. No-op while `binder/` is empty
  → land alone, full arch suite green first; then grep to confirm zero residual ad hoc
  readers.

## Phase 2 - Extract the test module

- Move the `#[cfg(test)] mod tests { … }` body (`binder.rs:11976`→EOF) to
  `src/binder/tests.rs`.
- In `binder.rs`: `#[cfg(test)] mod tests;` (plain; auto-resolves `binder/tests.rs`).
- `tests.rs` keeps `use super::*;`. Most parent access should remain private because the
  tests stay under `binder`; if moved sibling modules force visibility, prefer
  `pub(super)`, and use `pub(crate)` only for real non-binder callers. `cargo test -p
  toron --lib` green.

## Phase 3 - Carve all six semantic submodules

Add `mod arithmetic; mod pushdown; mod aggregate; mod join; mod selectivity; mod
explain;` to `binder.rs` (plain `mod`, auto-resolve). Move each cluster compile-driven;
cross-module binder calls should normally use `super::<module>::...` plus `pub(super)`.
Membership per ADR D3. Order: leaf-most first (selectivity, aggregate, explain), then
pushdown, arithmetic, join — minimizes churn as visibility errors surface. Widen only
compiler-required items; no `pub`; no logic edits; no renames (flag collisions).

## Phase 4 - Verification (behavior-neutral proof)

```sh
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
# zero residual ad hoc single-file binder readers outside support/ADR176 guards:
rg -n 'source\("src/binder\.rs"\)|read_to_string[^\n]*binder\.rs|join\("src/binder\.rs"\)' \
  crates/toron/tests/architecture
git --no-pager -c color.ui=false diff --stat                    # only binder.rs + binder/ + arch helpers
git diff --check
```

Run Phase 1 alone and confirm the arch suite green (no-op proof) + the zero-reader scan
before Phase 2/3. The scan may report the shared helper and the ADR176 guard that
asserts it; it should not report ordinary architecture modules. Run the suites after
each phase.

## Phase 5 - Architecture guards

In `tests/architecture/cockpit.rs` (or new `binder_structure.rs`):

- `adr176_binder_source_concatenates_submodules` - the consolidated helper enumerates
  `binder/` recursively (reads `binder.rs` + child Rust files in
  facade/production/test order); single aggregate definition.
- `adr176_no_scattered_binder_file_reads` - no residual per-file helper, inline read, or
  `source("src/binder.rs")` remains outside `support.rs` and this guard module.
- `adr176_binder_root_excludes_carved_clusters` - use `binder_root_source()`: no `fn
  predicate_pushdown_plan`, `fn aggregate_schema`, `fn choose_join_strategy`, etc.; still
  has `struct Binder` + `fn bind_plan_with_trace`.
- `adr176_test_module_is_extracted` - `binder/tests.rs` exists; root has `mod tests;`
  not inline `mod tests {`.
- `adr176_semantic_modules_exist` - the six `binder/*.rs` modules exist + declared.
- `adr176_no_runtime_or_wire_surface_change` - diff touches only `src/binder.rs`,
  `src/binder/`, `tests/architecture/`.

## Risks

- **Missed reader.** A stale single-file reader sees the truncated facade and its guard
  fails or, worse, scans the wrong surface. The Phase 4 source scan must be clean except
  for support/ADR176 guard code; land Phase 1 alone with the full arch suite green as the
  completeness proof.
- **Keystone ordering.** `tests.rs` must concatenate last; land Phase 1 (no-op) green
  before any move.
- **Visibility creep.** Prefer private or `pub(super)`; use `pub(crate)` only for
  existing non-binder callers; never `pub`.
- **Hidden logic edits.** Relocation only — resist "fix while moving"; diff-is-moves
  scan + green suites guard it.
- **Test visibility.** Extracted `tests.rs` may need sibling-module visibility; keep
  test-only helpers out of non-test visibility.
- **Review size.** ~12k lines relocate; review leans on green suites + structural guards
  + spot-checks. (D5: follow-on only if truly unreviewable.)
- **No `mod.rs`.** Keep `binder.rs` as the root (plain `mod`); do not convert to
  `binder/mod.rs`.

## Deferred

Genuine decoupling/trait extraction; per-module test co-location; decomposition of
other oversized files; ADR 177 only as a true-overrun fallback.
