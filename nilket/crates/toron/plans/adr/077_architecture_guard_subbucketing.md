# ADR 077: Architecture Guard Sub-Bucketing

**Status:** Promoted (plan) — **genuine 3-of-3 (Codex, Gemini, Claude)**, fully
converged by iter-1, no fork.
**Category:** Toron test architecture / council workflow substrate.
**Context:** ADR 076 moved the 18.7k-line monolith behind a tiny harness + 9
semantic modules (945 tests). Two of those modules are still large enough to be
review / context hazards — and these guard files are read by the context-limited
AI council itself, so file size is a real per-grep token cost. 077 cuts the next
behavior-zero slice.
**Behavior-zero: no production / Wendelon / SQL / storage / wire / telemetry /
audit change. No new dependency, including no `syn`.**

## The shape

Convert the two >4000-line modules into semantic **directory-modules**, one level
deeper than 076, behind the same single Cargo target:

```text
tests/architecture_test.rs                       # mod architecture; (unchanged)
tests/architecture/mod.rs                         # existing declarations stay textually unchanged
tests/architecture/distributed_execution/         # was distributed_execution.rs (4635)
  mod.rs  tenant_admission.rs  descriptor_sync.rs  cancellation.rs
  sql_ingress.rs  worker_lifecycle.rs  scheduler_dispatch.rs  resource_governance.rs
tests/architecture/storage_wendelon_boundary/     # was storage_wendelon_boundary.rs (5364)
  mod.rs  ddl_admin.rs  row_identity.rs  dml_delete_update.rs
  returning.rs  descriptor_invalidation.rs  wendelon_catalog.rs
```

`mod distributed_execution;` now resolves to `distributed_execution/mod.rs`
(standard Rust resolution); the existing declarations in `architecture/mod.rs`
stay textually unchanged. Still one `architecture_test` target, one command.

## Decisions (6, Promotion-Locked)

**D1 Scope = the two >4000-line jumbos only.** `distributed_execution.rs` (4635)
and `storage_wendelon_boundary.rs` (5364). **Leave `sql_planner_surface.rs` (2175)
intact** — under any sane ceiling; don't split a non-hazard.

**D2 Directory-modules, semantic sub-files, not ADR-numbered.** Each new directory
has a `mod.rs` declaring sub-files named by invariant surface; **no `adr_0xx.rs`**
(the 076 D2 rule, recursed).

**D3 Leaf-name conservation (the 076 gate).** No test renames, no assertion
rewrites, no production edits. Duplicate-preserving leaf-name list: post =
**945** + any `adr077_*` structure guards, additions accounted for exactly.

**D4 Zero reader drift (grounded).** `architecture_corpus_source()` →
`source_files()` → `collect_rs_files()` already recurses into subdirectories
(`support.rs:192`), so the conservation + self-inspecting guards from 076 pick up
the new sub-files with **no reader change**. Pinned by a guard.

**D5 Line-ceiling guard — the durable teeth, set generously.** A guard fails if any
non-`support` architecture module exceeds 2500 lines (`line_count <= 2500` is
the invariant). This makes sub-bucketing self-enforcing (a future guard family
must find/create a sub-bucket, not regrow a jumbo). **Generous on purpose** — a
tight ceiling would let a *feature* ADR trip a test-file-size failure mid-work;
2500 bites only real jumbos, and crossing it is the signal to sub-split.

**D6 Stable harness + support; no facade, no `syn`.** `architecture_test` command
unchanged; sub-bucket files import helpers via the deeper `super::super::support`
path; sibling guard imports/reexports stay test-local as needed;
`#[cfg]`/`#[tokio::test]` attrs preserved. No support-facade growth, no AST
parsing, no new dependency.

## Architecture Guards (5)

`adr077_no_architecture_module_exceeds_line_ceiling`,
`adr077_second_level_modules_are_semantic_not_adr_numbered`,
`adr077_architecture_corpus_source_recurses_subdirectories`,
`adr077_architecture_command_remains_stable`,
`adr077_leaf_names_conserved_after_subbucketing`.

Imperfect first-cut sub-bucket placement is acceptable as long as leaf-name
conservation stays green (the 076 standard). Full duplicate-preserving
leaf-name comparison is an evidence gate; the in-tree guard should pin the
structure and conserved-name invariant as far as practical without becoming a
second brittle manifest.

## Evidence requirements

Capture pre-move duplicate-preserving leaf-name list (945); prove post-move
contains every pre-move leaf occurrence + passing; prove new leaf names are only
`adr077_*`; full `cargo test -p toron --test architecture_test` green; `git diff
--check`; state production / manifests / dependencies / behavior unchanged.
Recommended: `adr076`/`adr077` filtered runs; `cargo clippy -p toron --test
architecture_test -- -D warnings`.

## Posture and sequence (the bounded one-off)

077 is the **last test-infra cut for now.** After it, the council **returns to
feature work** on the V1 "First Query with Integrity" target: `OR`/`NOT`
predicate-distribution pushdown (the 075 follow-on) is the lead, then `SIMILAR
TO`/regex, `CASE`, the Wendelon trace-continuation. The source-guard support
**facade** and the **`syn`/AST pilot** remain useful but are **on-demand future
work, not ADR 078/079 commitments** (3-of-3) — 077 makes either one per-bucket and
small if a concrete maintenance cost later justifies it.

## Council History

Codex seeded the topic (sub-bucketing, re-sequencing the deferred `syn` work).
Claude and Gemini tightened it to a **bounded one-off** (two jumbos only, leave
`sql_planner_surface`, return to features after — 078/079 on-demand, not
committed); Claude **grounded that the corpus reader already recurses**
(`support.rs:192`), removing the obvious behavior-zero hazard, and added the
line-ceiling churn caveat (generous ceiling). Locked 3-of-3 by iter-1 — no
substantive fork.

## Promotion criteria met

D-set 6; 5 guards; behavior-zero (leaf-name-conserved, no production/dep change,
no reader change); single Cargo target + command stable; `syn` deferred. Migration:
`plans/impl/077_architecture_guard_subbucketing.md`.

## Conclusion

077 sub-buckets the two remaining jumbo architecture modules into semantic
directory-modules — behavior-zero, no reader change (the 076 corpus reader already
recurses), generous line-ceiling guard as the durable teeth, leaf-name
conservation as the gate. A **bounded one-off**: `sql_planner_surface` untouched,
078/079 deferred on-demand, and feature work (`OR`/`NOT` pushdown) resumes next.
