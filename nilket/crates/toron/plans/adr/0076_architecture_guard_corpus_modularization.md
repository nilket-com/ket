# ADR 076: Architecture Guard Corpus Modularization

**Status:** Promoted (plan) — **genuine 3-of-3 (Codex, Gemini, Claude)**, the
cleanest convergence of the recent run; no substantive fork.
**Category:** Toron test architecture / council workflow substrate.
**Context:** `crates/toron/tests/architecture_test.rs` has accreted to ~18.7k
lines (903 `adrNNN_*` guards; **938 tests total**). The monolith did its job —
it let the guard families become visible — and now degrades review speed. 076
modularizes it **before** the next feature ADR widens it further.
**Behavior-zero: no Toron runtime / Wendelon / SQL / storage / wire / telemetry /
audit change. No new dependency, including no `syn`.**

## The shape

One stable Cargo target — `cargo test -p toron --test architecture_test` — backed
by a tiny harness and semantic modules:

```text
tests/architecture_test.rs        # harness: `mod architecture;`
tests/architecture/mod.rs         # submodule declarations
tests/architecture/support.rs     # shared readers/fixtures + corpus reader
tests/architecture/{predicate_pushdown, sql_planner_surface, observability_audit,
  observability_telemetry, network_tls, distributed_execution,
  storage_wendelon_boundary, ring_placement, core_invariants}.rs
```

The **file** says which surface is guarded; the **test name** keeps ADR
provenance (`adr075_partial_pushdown_splits_bound_top_level_and_only` lives in
`predicate_pushdown.rs`). Semantic navigation without losing grep-able chronology.

## Decisions (8, Promotion-Locked)

**D1 Stable harness.** `architecture_test.rs` becomes `mod architecture;` — the
only Cargo integration target for this corpus. `tests/architecture/*.rs` are
module files (not standalone targets), wired from `architecture/mod.rs`. Command
and evidence muscle-memory unchanged.

**D2 Semantic module files, ADR-prefixed guard names.** *Not* `adr_075.rs` /
`adr_076.rs` — that recreates the append-only ledger in smaller pieces. Semantic
files give the next guard family an obvious home; `adrNNN_` function names
preserve auditability.

**D3 Test-local support + corpus reader.** Move shared helpers/fixtures to
`support.rs` as `pub(crate)` (`source_files`, `brace_blocks_after_marker`,
`manifest_dir`, `wendelon_manifest_dir`, the `*_source_text` readers,
`format_offenders`, `RecordingStorageScan`, `descriptor`, `batch`, …). Add
`pub(crate) fn architecture_corpus_source() -> String` — concatenates
`tests/architecture/**/*.rs` in stable order, **excluding the harness**.
Content/name meta-guards read this, not `architecture_test.rs`.

**D4 Behavior-zero via leaf-name conservation.** Moving tests into modules changes
Cargo's full test paths (`adr075_…` → `architecture::predicate_pushdown::adr075_…`),
so the conservation gate compares **leaf** test-function names, not raw
`--list` strings. Capture the pre-move leaf-name list (938 entries), sorted with
duplicates preserved; prove **every pre-move leaf occurrence still exists and
passes**; the only additions are this ADR's `adr076_*` guards. (Corrects the
iter-0 "raw count equality" framing — the count *grows* by the new guards.)

**D5 Retarget self-inspecting guards.** Several existing guards read
`tests/architecture_test.rs` and assert another guard's name is present (e.g. a
guard asserting `adr065_flat_observability_http_runtime_crate_removed` exists).
After the split that file is the empty harness, so those must read
`architecture_corpus_source()`. **The only legitimate harness read post-076 is the
harness-regrowth guard.** This is the move most likely to falsify "behavior-zero"
if missed.

**D6 Pin the structure with guards** (below).

**D7 Preserve attrs, cfg gates, async.** Move `#[test]`, `#[tokio::test]`,
`#[cfg(…)]`, and required crate-level allowances without semantic change (21 such
sites; e.g. the feature-gated telemetry guards stay under the same gate).

**D8 No extra Cargo targets, no new dependency.** No `[[test]]` entries, no extra
top-level test files. One corpus, one command, one evidence surface. `syn`/AST
rewrites are out of scope (see Deferred).

## Architecture Guards (7)

`adr076_architecture_test_is_only_a_harness`,
`adr076_architecture_modules_are_semantic_not_adr_numbered`,
`adr076_corpus_content_guards_read_the_architecture_corpus_not_the_harness`,
`adr076_existing_adr_guard_leaf_names_are_preserved`,
`adr076_support_helpers_stay_test_local`,
`adr076_architecture_command_remains_stable`,
`adr076_no_production_runtime_or_dependency_change`.

The first three are mandatory; the corpus-reader guard is load-bearing (it
prevents the most likely false behavior-zero migration).

## Module buckets (first cut)

`predicate_pushdown` (PBC, runtime-filter, scan-profile, 074 `IN`/`BETWEEN`, 075
split + literal `LIKE`/`ILIKE`); `sql_planner_surface` (sqlparser quarantine, AST
destructuring, planner admit/reject, DDL/DML); `observability_audit` (065/066/071/
072); `observability_telemetry` (067/068/069); `network_tls` (PKI, authenticated
listeners, SAN/identity); `distributed_execution` (scheduler/task/worker,
cancellation, admission, queues, dispatch); `storage_wendelon_boundary` (descriptor
pins, catalog/storage boundary, row identity, PK DML, returning, admin);
`ring_placement` (Toron/Wendelon placement, runtime-root); `core_invariants`
(global leftovers + the 076 guards). Imperfect first-cut placement is fine if the
next guard has a semantic home.

## Evidence requirements

Capture the pre-move sorted leaf-name list + count (938, duplicate-preserving);
prove post-move contains every pre-move leaf occurrence; prove new leaf names are
only `adr076_*`; full `cargo test -p toron --test architecture_test` green; `git
diff --check`; state production source / manifests / dependencies / Wendelon /
storage / wire / telemetry / audit unchanged. Recommended: filtered
`adr074`/`adr075`/`adr076` runs; `cargo clippy -p toron --test architecture_test
-- -D warnings`.

## Deferred → future ADR

**`syn`/AST semantic guards (≈080+).** Most guards are brittle string-search
(`.contains()`/regex) coupled to formatting; the robust form is `syn`-AST queries
("the `match` arm for `SqlExpr::Like` returns an error"). Combining that rewrite
with the mechanical move would destroy the behavior-zero net, so it's deferred —
**and 076 is its prerequisite:** the semantic buckets become the *unit* of that
future per-surface upgrade (migrate `predicate_pushdown.rs` to AST guards as one
reviewable step, then the next). The standing roadmap candidates (`OR`/`NOT`
distribution pushdown, `SIMILAR TO`/regex, `CASE`, the Wendelon trace-continuation)
resume once this foundation lands.

## Council History

Topic pre-set; Codex seeded the iter-0 dual. Claude added the **conservation gate**
and grounded the **self-inspecting-guard hazard** (≥5 guards that read
`architecture_test.rs` would break on the move) + the `architecture_corpus_source`
fix; Gemini added the `syn`/AST deferral. iter-1: Codex corrected Claude's "raw
count equality" to name-list conservation; Claude conceded and framed 076 as the
`syn`-rewrite prerequisite. iter-2: Codex refined to **leaf-name conservation**
(the module prefix changes the full path; compare duplicate-preserving leaf-name
lists). No substantive fork — 3-of-3 throughout.

## Promotion criteria met

D-set 8; 7 guards; behavior-zero (leaf-name-conserved, no production/dep/behavior
change); the self-inspecting-guard retarget pinned; `syn` explicitly deferred;
single Cargo target + command stable. Migration:
`plans/impl/0076_architecture_guard_corpus_modularization.md`.

## Conclusion

076 modularizes Toron's architecture guard corpus before the next feature widens
it: stable harness, semantic module files, ADR provenance in guard names,
test-local support + corpus reader, **duplicate-preserving leaf-name
conservation** as the behavior-zero proof, self-inspecting-guard retargeting, and
explicit deferral of the `syn`/AST rewrite (whose unit of work 076's buckets
become).
