# Impl Plan 077: Architecture Guard Sub-Bucketing

**Status:** Promoted, paired with `plans/adr/0077_architecture_guard_subbucketing.md`.
**Posture:** Mechanical, **behavior-zero**, test-only refactor — 076 one level
deeper. Convert the two >4000-line architecture modules to semantic
directory-modules; preserve the Cargo target, leaf names, attrs/gates, deps. Gate
= duplicate-preserving leaf-name conservation. Five phases.

## Verified anchors (grounded)

- **Scope:** `storage_wendelon_boundary.rs` 5364, `distributed_execution.rs` 4635
  (the two jumbos). `sql_planner_surface.rs` 2175 — left intact.
- **Corpus reader already recurses (no change):** `architecture_corpus_source()` →
  `source_files()` → `collect_rs_files()` recurses into subdirs (`support.rs:192`);
  the 076 conservation + self-inspecting guards pick up the new sub-files
  automatically.
- **Baseline count:** 945 tests (938 + 7 `adr076_*`).
- **Cargo discovery:** `mod distributed_execution;` in `architecture/mod.rs`
  resolves to `distributed_execution/mod.rs` once the file becomes a directory —
  standard resolution; the existing declarations in `architecture/mod.rs` stay
  textually unchanged; single `architecture_test` target.

## Invariants carried in

- Behavior-zero: no test renames, no assertion rewrites, no production/manifest/dep
  change, no `syn`. **Leaf-name conservation** (945 + `adr077_*`). Single Cargo
  target + command unchanged. `#[cfg]`/`#[tokio::test]` attrs preserved. **No reader
  change** (recursion already handled). No support-facade growth.

## Phase 0 — Baseline

Capture the pre-move duplicate-preserving leaf-name list (945) per the 076 rule.
The conservation reference for Phase 4/5.

## Phase 1 — Convert the two jumbos to directory-modules

For `distributed_execution` and `storage_wendelon_boundary`: create the directory,
add `mod.rs` declaring the semantic sub-files, remove the flat `.rs`.
The existing `mod distributed_execution;` and `mod storage_wendelon_boundary;`
declarations in `architecture/mod.rs` stay textually unchanged. Sub-buckets:

```text
distributed_execution/mod.rs
distributed_execution/tenant_admission.rs
distributed_execution/descriptor_sync.rs
distributed_execution/cancellation.rs
distributed_execution/sql_ingress.rs
distributed_execution/worker_lifecycle.rs
distributed_execution/scheduler_dispatch.rs
distributed_execution/resource_governance.rs

storage_wendelon_boundary/mod.rs
storage_wendelon_boundary/ddl_admin.rs
storage_wendelon_boundary/row_identity.rs
storage_wendelon_boundary/dml_delete_update.rs
storage_wendelon_boundary/returning.rs
storage_wendelon_boundary/descriptor_invalidation.rs
storage_wendelon_boundary/wendelon_catalog.rs
```

## Phase 2 — Move tests; fix the support import depth

Move each test into its semantic sub-file, **preserving leaf name, body,
`#[cfg]`/`#[tokio::test]` attrs**. The primary mechanical import change:
sub-files are two levels under `architecture/`, so support imports become
`use super::super::support::{…};` (was `super::support::{…}`); sibling guard
imports/reexports stay test-local as needed. Imperfect placement is fine if
conservation stays green.

## Phase 3 — Add the ADR 077 guards (`core_invariants.rs`)

- `adr077_no_architecture_module_exceeds_line_ceiling` (`line_count <= 2500` per
  non-`support` architecture `.rs`; **generous** to avoid mid-feature churn).
- `adr077_second_level_modules_are_semantic_not_adr_numbered` (no `adr_*.rs` in the
  new directories).
- `adr077_architecture_corpus_source_recurses_subdirectories` (pin `support.rs:192`).
- `adr077_architecture_command_remains_stable`;
  `adr077_leaf_names_conserved_after_subbucketing`. The full duplicate-preserving
  leaf-name comparison remains an evidence gate; the guard should pin the
  structure and conserved-name invariant without requiring a brittle embedded
  945-name manifest.

## Phase 4 — Compile + conservation check

`cargo test -p toron --test architecture_test --no-run`; fix the
`super::super::support` imports. Full run: expect **945 + N_adr077** passed, all
green; prove every Phase-0 leaf occurrence still present (additions only `adr077_*`).

## Phase 5 — Evidence

`crates/toron/plans/impl/0077_architecture_guard_subbucketing_evidence.md`:
duplicate-preserving leaf-name lists before/after (every pre-move leaf present;
additions only `adr077_*`); full `cargo test -p toron --test architecture_test`
green; `adr076`/`adr077` filters green; `git diff --check`; `cargo clippy -p toron
--test architecture_test -- -D warnings` (if disk allows); state test-only,
behavior-zero, no production/dep change, command unchanged, no `syn`.

## Watch-items

1. **Leaf-name conservation** — every pre-move leaf occurrence present + passing;
   additions only `adr077_*`.
2. **Support import depth** — sub-files use `super::super::support` (two levels);
   sibling guard imports/reexports stay test-local as needed.
3. **No reader change** — `collect_rs_files` already recurses; don't touch it (the
   corpus-recurses guard pins it).
4. **`#[cfg]`/`#[tokio::test]` attrs** preserved on move.
5. **`mod.rs` wiring** — each new directory's `mod.rs` declares all its sub-files;
   the existing declarations in `architecture/mod.rs` stay textually unchanged.
6. **Generous ceiling** — 2500 leaves headroom; only the two jumbos must split.

## Acceptance checklist

- [ ] Phase 0: pre-move leaf-name list (945) captured.
- [ ] Phase 1: two jumbos are directories with `mod.rs`; existing
      `architecture/mod.rs` declarations stay textually unchanged; flat `.rs`
      removed.
- [ ] Phase 2: tests moved by sub-bucket; leaf names/attrs/gates preserved;
      `super::super::support` imports fixed.
- [ ] Phase 3: 5 ADR-077 guards present (incl. line-ceiling + corpus-recurses).
- [ ] Phase 4/5: 945 + `adr077_*` all green; every pre-move leaf present; command
      unchanged; no production/dep change; `git diff --check` clean.
