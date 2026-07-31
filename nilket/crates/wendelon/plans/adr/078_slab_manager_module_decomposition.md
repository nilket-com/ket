# Wendelon ADR 078: Slab-Manager Module Decomposition

**Status:** Promoted. **Migration:** `plans/impl/078_slab_manager_module_decomposition.md`.
**Category:** Wendelon storage-ring maintainability cut — the storage-side twin of ADR 077,
drafted in the cross-ring council (operator + Codex + Claude aligned). A behavior-neutral
decomposition of `crates/wendelon/src/storage/manager.rs` (26,442 lines) into a module directory,
following the `plans/conventions/module-decomposition.md` playbook ADR 077 established.

## Why this cut exists

`manager.rs` is the repo's second-largest file and the whole slab/LSM/encoding/tiering storage
engine in one file. ADR 077 carved the data plane's request surface and **codified the
decomposition convention** precisely so this one is near-mechanical. This is the named 078 follow-on
in 077's forward arc. It is **not** a copy-paste of 077: `data_server.rs` was a *dispatch* monolith
(thin `handle_*` entry points atop shared helpers — so its root stayed large); `manager.rs` is a
*lifecycle/engine* monolith where the methods **are** the subsystem substance, so carving cohesive
subsystem islands relocates real volume.

## Grounding (verified)

- `SlabManager`'s inherent impl owns the bulk: **241 methods across 2 impl blocks**;
  `pub struct SlabManager` (manager.rs:509) has **no derive/macro attribute**, so a child module of
  `storage::manager` may write `impl SlabManager` and read its 13 private fields — the ADR 176 / 077
  mechanism holds (the one technical unknown, pre-confirmed).
- **The root must stay `storage/manager.rs`, not `manager/mod.rs`.** `mod.rs` files are
  manifests-only in this crate (an architecture guard enforces it) — ADR 077's docs said "→ mod.rs"
  but its *implementation* corrected to a sibling root file under Rust 2018 layout. The public path
  `wendelon::storage::manager::…` is preserved either way (`storage/mod.rs` already declares
  `pub mod manager;`).
- **Re-export burden is bounded, not zero.** Most externally-referenced public/crate-public types —
  `SlabManagerConfig`, `RehydrationPolicy`, `SealedSlabInfo`, `TieredSlabPointer`,
  `StorageBackupImage`, `TombstoneCompactionReport`, `RetentionPruningReport`,
  `RetentionCompactionStats`, `PreparedTieredRetentionRewrite` — sit in the public-types cluster
  above the struct (lines ~224–509), and that cluster stays in root. One public type is deliberately
  island-owned: `SecondaryIndexSegmentInfo` belongs with `secondary_index_segments.rs`; if it moves,
  root must `pub use secondary_index_segments::SecondaryIndexSegmentInfo;` so the public
  `wendelon::storage::manager::SecondaryIndexSegmentInfo` path stays unchanged. Other island types
  (`Encoding*` ~7545–7724, `Compaction*`/`Recluster*`/`CompactedFrame*` ~12434+, stats candidate
  structs ~577–624) are private unless `cargo check` proves otherwise.
- **Sibling↔sibling calls resolve via `pub(super)`.** A `compaction` method calling
  `self.persist_record_row()` (a core method staying in root) is child→parent (free); a method in
  one island calling another island's method works because `pub(super)` makes it visible in the
  common parent `manager` scope, which descendants inherit. Uniform `pub(super)` on every moved item
  covers the web — no trait extraction, no behavior reshaping.
- Wendelon's architecture guards live in one file, `crates/wendelon/tests/architecture_test.rs`;
  several prior guards read `src/storage/manager.rs` directly and currently use simple
  `contains`/token checks (no fragile region-extraction), so the retarget is a clean
  concatenation-helper swap.
- The `#[cfg(test)] mod tests` block is lines 15825–26442 (**~10,600 lines — ~40% of the file**),
  cleanly isolated. Three `#[cfg(test)]`-gated test-helper *methods* (1872, 2366, 8471) are the only
  nested test code; they stay methods (call sites unchanged) by moving into a `#[cfg(test)] impl
  SlabManager` block in `tests.rs`.

## Decisions (promotion-locked)

**D1 — Behavior-neutral, inherent-impl-across-submodules.** Methods stay methods on `SlabManager`;
they move, they are not re-architected. **No storage-format, encoding, LSM, compaction, retention,
secondary-index, tiering, crypto, or wire behavior change** — that would make the move non-mechanical
and void the proof. Proof of neutrality: **the full Wendelon suite — every `lsm_*`,
`encoding_*`, `*_secondary_index`, `tombstone_compaction`, `backup_restore`, `control_ring_retention`
integration target plus `--lib` — green with zero assertion/behavior edits**, plus the surface-change
guard. No goldens change (no preimage-feeding type is touched — pure source reorg).

**D2 — Coarse, contiguous subsystem islands; types colocated with their island (council-aligned
granularity).** No generic `helpers/` bucket. Carve the cohesive, roughly-contiguous islands:
- `stats_facts.rs` — the stats candidate structs (`NdvColumnCandidate`/`RangeColumnCandidate`/
  `HeavyHitterColumnCandidate`/`ColumnGroup*Candidate`/`RangeDistributionColumnCandidate`),
  `TieredStatsContributor`, `TieredStatsCacheKey` + the stats-block/optimizer-contributor loaders.
- `encoding_compute.rs` — the `Encoding*` type family (~7545–7724) + impl block 2's L2 read /
  projected-record / aggregate / compute-phase-B / FSST / predicate-page methods.
- `tiered_io.rs` — rehydrate/retire/apply-tier, `decrypt_tiered_object_blob`/`encode_tiered_object_blob`,
  `fetch_full_tiered_plaintext`, L2 object read path.
- `lsm_driver.rs` — the LSM run/tick/pick/stage/install/complete/validate/sweep driver
  (`run_lsm_compaction_*`, `pick_lsm_compaction_plan`, `install_lsm_compaction_temp_outputs`, …).
- `compaction.rs` — `compact_one_sealed_slab`/`compact_sealed_slab_records`/recluster +
  `CompactionRecord`/`Recluster*`/`CompactedFrame*` (~12434+) + manifest-commit application.
- `retention_reclaim.rs` — `prune_versions_*`, table-drop reclaim, tombstone-range reclaim,
  `mark_record(s)_for_table_drop` + the prepared-rewrite types not pinned to root by external use.
- `secondary_index_segments.rs` — persist/load/delete/reconcile/gc secondary-index segments,
  coverage tracking, `SecondaryIndexSegmentInfo`, `LsmPreparedSecondaryIndexSegment(s)`.
- `tests.rs` — the extracted `#[cfg(test)]` block + the three test-helper methods.

Exact span→island assignment is derived at impl (`cargo check` + guards are the enforcement); a
method whose island is genuinely ambiguous stays in root.

**D3 — The public surface, construction, and the cross-cutting core path stay in root.** Root keeps:
the `SlabManager` struct + the public config/report types in the root public cluster; construction
(`open`/`new`/builders); shared state accessors; and the **genuinely cross-cutting engine core —
`append_durable*`, `scan*`, `persist_record_*`, the routing/locality index** — which every island
calls and which does not belong to one subsystem. Any public type that moves for domain ownership
(`SecondaryIndexSegmentInfo` is the known case) is re-exported from root to preserve the
`storage::manager::Type` path. Root remains the legible entry point that names every island via
`mod` declarations.

**D4 — Follows the existing convention; no new convention doc.** This cut applies
`plans/conventions/module-decomposition.md` (established by ADR 077). It refines that doc only if the
storage carve surfaces a genuinely new rule (e.g. the storage-format surface-guard variant).

**D5 — Scope: `manager.rs` first-carve only; deeper root ratcheting is a named follow-on
(operator-directed, Codex-reinforced).** ADR 077's Phase-2-style helper relocation is explicitly
**not** in this cut. After the islands + tests are out, the root is materially smaller but **not
minimal** — the core read/write/scan/persist path and many helpers shared across engine/LSM/
encoding/retention/table-drop legitimately remain. Pushing the root toward a small struct+construction
core means triaging shared-vs-domain helpers, which **stops being move-only** and is deferred. The
cap is set from the real post-carve size + headroom and **ratcheted down** in that follow-on.

**D6 — Root is a sibling file, never `mod.rs`.** A guard pins it (manifests-only is a hard crate
rule; this is the one place 077's promoted docs were wrong before implementation corrected them).

**D7 — De-version the one in-file V1 prose comment.** The test comment `// V1 sparse projection
fetch consumes …` (in the test block, ~24318) is retired-milestone prose; reword to the referent
(`// Sparse projection fetch consumes …`) as it rides the `tests.rs` move. No `## V1` heading exists
here (unlike 077). Byte-domain `*.v1` constants and `default_kv_table_descriptor_v1`/`b"v1"` test
fixtures are protocol/format identifiers and stay.

## Guards (mirror ADR 077, manager-specific names, promotion-locked)

In `crates/wendelon/tests/architecture_test.rs`:
- `adr078_manager_source_concatenates_submodules` — exact file manifest of `storage/manager/*.rs` in
  stable order (root first, islands sorted, `tests.rs` excluded), plus the `manager_source_files()` /
  `manager_source()` concatenation helper. Every prior guard reading the manager source uses this
  helper, not `src/storage/manager.rs`.
- `adr078_semantic_modules_exist` — each named island present + declared in root.
- `adr078_slab_manager_public_surface_stays_in_root` — `SlabManager` def + its public config/report
  cluster + construction in root; any moved public island type is re-exported from root; carved
  subsystem method bodies are NOT in root.
- `adr078_root_is_bounded` — root ≤ a line cap **set from the real post-carve size + headroom**
  (no aspirational number; ratchet down in the D5 follow-on).
- `adr078_tests_extracted` — the `#[cfg(test)]` block in `tests.rs`.
- `adr078_no_storage_wire_or_runtime_surface_change` — dirty-tree scan: only `storage/manager/**`,
  the decomposition guard, and the plan files changed; **no edits to on-disk slab/manifest format,
  encoding, crypto, wire, or any production crate file outside `storage/manager/**`** (the
  cardinal-risk guard, storage-format-aware).
- `adr078_no_mod_rs_root` — the module root is `storage/manager.rs`, and `storage/manager/mod.rs`
  does **not** exist (the manifests-only lesson, pinned).

## Honesty gate

No red-on-main *behavioral* gate — a pure move changes no behavior by design. The gate is inverted:
**the entire existing Wendelon storage suite passes with no assertion/behavior edits**, and the
`adr078_*` structural guards (which DO fail on main — `manager.rs` is a single file, no submodules
exist) pass after. The storage integration suites are the "delivers its name without breaking
anything" proof; the structural guards are the "fails on main" proof.

## Risks / honest notes

- **More shared-helper entanglement than 077 (Codex's central caution).** The engine core touches
  every subsystem; moving a genuinely-shared helper into one island breaks move-only and creates a
  visibility web. The discriminator: carve a helper only if it belongs to exactly one island;
  otherwise it stays in root. Over-carving is the failure mode here, not under-carving.
- **Storage-format neutrality is the cardinal risk.** Unlike 077's wire surface, the danger here is a
  silent on-disk format/encoding/manifest change. The `lsm_*` / `encoding_*` / `*_secondary_index` /
  `backup_restore` integration suites and the descriptor-fingerprint/golden tests are the proof; run
  them, with the `LiveWendelonFixture` flake discipline on any distributed fixtures.
- **Re-export completeness** (subtle but bounded here): if a public or crate-public mover is
  externally referenced, a missing `pub use` breaks `storage::manager::Foo` crate-wide — caught by
  `cargo check`. `SecondaryIndexSegmentInfo` is the likely intentional re-export; anything else is a
  verify-first finding during the carve.
- **Island sizing** — `encoding_compute` and the LSM driver/compaction pair may each still be
  multi-thousand-line; the ceiling guard forces a further split rather than a blown cap, but a
  sub-split is deferred unless it busts the cap (small-cut discipline).
- **Enormous mechanical diff; the suite is the review.** The instant a move "fixes" anything it
  stops being mechanical and the proof collapses (D1).

## Forward arc

`077 data_server decomposition → 078 manager.rs island decomposition (this) → deeper root ratcheting
for both (the deferred shared-helper triage, when a path needs the legibility) → the next monolith by
the same documented playbook`. Dividend: a legible `compaction.rs` / `lsm_driver.rs` eases the
elasticity-gated counter-handoff and any future LSM-policy cut.

## Deferred (named)

ADR 077's Phase-2 helper relocation; this cut's own deeper root ratcheting (D5); a `encoding_compute`
or LSM sub-split if it crosses the ceiling; `consensus/raft_storage.rs` (9,783 lines) and
`sql_planner.rs` (8,314) if they cross a future ceiling; any behavior change to the storage engine
(explicitly out of scope — this is a move-only cut).
