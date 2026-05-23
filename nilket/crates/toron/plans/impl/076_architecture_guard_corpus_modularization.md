# Impl Plan 076: Architecture Guard Corpus Modularization

**Status:** Promoted, paired with
`plans/adr/076_architecture_guard_corpus_modularization.md`.
**Posture:** Mechanical, **behavior-zero**, test-only refactor. Split
`tests/architecture_test.rs` (~18.7k lines, **938 tests**) into semantic modules
under `tests/architecture/`; preserve the Cargo target, guard leaf-names,
assertions, attrs/gates, and dependencies. Two gated requirements beyond a plain
move: **duplicate-preserving leaf-name conservation** and the
**self-inspecting-guard retarget**. Six phases.

## Verified anchors (grounded)

- **Count:** `cargo test -p toron --test architecture_test` ⇒ **938 passed** (903
  `fn adrNNN_*` + ~35 helper/other tests). The conservation baseline.
- **Cargo discovery:** `tests/architecture/*.rs` are not targets alone;
  `architecture_test.rs` = `mod architecture;` → `tests/architecture/mod.rs` wires
  submodules into the single target.
- **Self-inspecting guards (the hazard):** `read_to_string(manifest_dir().join(
  "tests/architecture_test.rs"))` + `.contains("adrNNN_…")` at `:7495`, `:7811`,
  `:7836`, `:8257` (`:8267` asserts `adr065_flat_observability_http_runtime_crate_
  removed`), `:8728` — break on the move unless retargeted to the corpus.
- **Attrs/gates:** 21 `#[cfg(feature=…)]` / `#[tokio::test]` sites to preserve.
- **Helper cluster** at the monolith tail (move to `support.rs`): `source_files`,
  `collect_rs_files`, `brace_blocks_after_marker`, `format_offenders`,
  `manifest_dir`, `wendelon_manifest_dir`, `workspace_root_dir`, the
  `*_source_text` readers, `workspace_cargo_tomls`, `RecordingStorageScan`,
  `descriptor`, `batch`.

## Phase 0 — Baseline (the gate input)

Capture the pre-move **leaf-name list**: `cargo test -p toron --test
architecture_test -- --list`, keep test lines, strip the trailing `: test`, split
the test path on `::`, take the final segment, sort the result, and preserve
duplicates. Store the 938-entry list + count. This is the conservation reference
for Phase 6.

## Phase 1 — Harness + support + corpus reader

- `architecture_test.rs` → `mod architecture;` (+ only crate-level attrs the moved
  modules need). `architecture/mod.rs` declares submodules + `support`.
- Move the helper cluster to `architecture/support.rs` (`pub(crate)`).
- Add `pub(crate) fn architecture_corpus_source() -> String` — concatenate
  `tests/architecture/**/*.rs` in stable order, **excluding** the harness.

## Phase 2 — Move tests by coarse semantic bucket

Buckets per the ADR (`predicate_pushdown`, `sql_planner_surface`,
`observability_audit`, `observability_telemetry`, `network_tls`,
`distributed_execution`, `storage_wendelon_boundary`, `ring_placement`,
`core_invariants`). Move in chunks, **preserving each test's leaf name, body,
`#[cfg]`/`#[tokio::test]` attrs**. (074/075 → `predicate_pushdown`; 065/066/071/072
→ `observability_audit`; 067/068/069 → `observability_telemetry`.)

## Phase 3 — Retarget self-inspecting guards (load-bearing)

Swap every `read_to_string(".../architecture_test.rs")` (used for a content/name
assertion) → `architecture_corpus_source()`. Sweep all five known sites and grep
the moved modules for any other `architecture_test.rs` reads. The **only**
legitimate harness read is the new harness-size guard.

## Phase 4 — Add the ADR 076 guards (`core_invariants.rs`)

- `adr076_architecture_test_is_only_a_harness` (no `#[test]`/`#[tokio::test]`,
  only `mod architecture;` + approved attrs).
- `adr076_architecture_modules_are_semantic_not_adr_numbered` (no `adr_*.rs` /
  `adr[0-9]*.rs` under `tests/architecture/`).
- `adr076_corpus_content_guards_read_the_architecture_corpus_not_the_harness`.
- `adr076_existing_adr_guard_leaf_names_are_preserved` (representative `adrNNN_`
  leaf names present in the corpus).
- `adr076_support_helpers_stay_test_local`, `adr076_architecture_command_remains_
  stable`, `adr076_no_production_runtime_or_dependency_change`.

## Phase 5 — Compile + fix imports

`cargo test -p toron --test architecture_test --no-run`. Fix module-local imports
explicitly (`use super::support::{…};`); no blanket `pub use support::*`. Preserve
async/`RecordingStorageScan` imports.

## Phase 6 — Evidence (the gate)

`crates/toron/plans/impl/076_architecture_guard_corpus_modularization_evidence.md`:

- **Leaf-name conservation:** every Phase-0 leaf occurrence present post-move +
  passing; the *only* additions are `adr076_*` (post-count = 938 + N_new). Show
  both sorted duplicate-preserving lists.
- Full `cargo test -p toron --test architecture_test` green; `adr074`/`adr075`/
  `adr076` filters green; `git diff --check`; `cargo clippy -p toron --test
  architecture_test -- -D warnings` (if disk allows).
- State: test-only, behavior-zero, no production/manifest/dep change, command
  unchanged, no `syn`.

## Watch-items

1. **Leaf-name conservation** — compare sorted duplicate-preserving *leaf* names
   (the full path gains the module prefix); every pre-move leaf occurrence present
   + passing; additions only `adr076_*`.
2. **Self-inspecting-guard retarget** — sweep all `architecture_test.rs` reads to
   the corpus reader; the harness-size guard is the only harness read.
3. **`#[cfg]`/`#[tokio::test]` attrs** preserved on move (21 sites).
4. **Cargo discovery** — don't delete the harness; modules won't run without it.
5. **Helper visibility** — `pub(crate)`, test-local; nothing to production.
6. **No guard-quality cleanup, no `syn`** — mechanical move only; the AST rewrite
   is the deferred future ADR.

## Acceptance checklist

- [ ] Phase 0: pre-move sorted leaf-name list + count (938) captured.
- [ ] Phase 1: harness = `mod architecture;`; support + `architecture_corpus_source`.
- [ ] Phase 2: tests moved by bucket; leaf names + attrs/gates preserved.
- [ ] Phase 3: all self-inspecting guards read the corpus, not the harness.
- [ ] Phase 4: 7 ADR-076 guards present.
- [ ] Phase 6: every pre-move leaf occurrence present + passing, additions only
      `adr076_*`; full corpus green; command unchanged; no production/dep change;
      `git diff --check` clean.
