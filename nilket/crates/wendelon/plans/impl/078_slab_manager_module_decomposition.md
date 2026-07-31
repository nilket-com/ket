# Impl Plan — Wendelon ADR 078: Slab-Manager Module Decomposition

Pairs with `plans/adr/078_slab_manager_module_decomposition.md`. A behavior-neutral move-only cut:
`storage/manager.rs` → `storage/manager.rs` (root) + `storage/manager/{islands + tests.rs}`,
applying `plans/conventions/module-decomposition.md`. **Every edit is a move, an import-path fix, a
`pub(super)`/`pub use` visibility change, or a guard retarget — never a logic change.** The review
is the unchanged storage suite + the structural guards + a moves-only diff.

## The mechanism (confirmed for SlabManager)

`impl SlabManager { … }` blocks live in child modules of `storage::manager`; the 13 private fields
defined in the root file are visible to descendants (`SlabManager` has no derive — pre-confirmed).
The root stays the sibling file `storage/manager.rs` (mod.rs is manifests-only; `storage/mod.rs`
already has `pub mod manager;` — path `wendelon::storage::manager::…` preserved). Each island is
headed `use super::*;`. `pub(super)` on every moved item, field, and method (covers sibling↔sibling
calls — all resolve in the common parent scope). Root `pub use child::Foo;` preserves public paths
for any moved public/crate-public type; `SecondaryIndexSegmentInfo` is the known likely case if it
moves with `secondary_index_segments.rs`.

## Module map (council-aligned: coarse contiguous islands, types colocated)

| Module | Owns |
|---|---|
| `manager.rs` (root) | `SlabManager` struct + public config/report cluster (~224–509); construction (`open`/`new`/builders); shared accessors; the cross-cutting engine core (`append_durable*`, `scan*`, `persist_record_*`, routing/locality index); `mod` declarations; `pub use` re-exports for moved public island types |
| `stats_facts.rs` | stats candidate structs (`NdvColumnCandidate`/`RangeColumnCandidate`/`HeavyHitterColumnCandidate`/`ColumnGroupTopKCandidate`/`ColumnGroupNdvCandidate`/`RangeDistributionColumnCandidate`), `TieredStatsContributor`, `TieredStatsCacheKey` + stats-block/optimizer-contributor loaders |
| `encoding_compute.rs` | `Encoding*` family (~7545–7724) + impl-block-2 L2 read / projected-record / aggregate / compute-phase-B / FSST / predicate-page methods |
| `tiered_io.rs` | rehydrate/retire/apply-tier, `decrypt_tiered_object_blob`/`encode_tiered_object_blob`, `fetch_full_tiered_plaintext`, L2 object read path |
| `lsm_driver.rs` | `run_lsm_compaction_*`, `pick_lsm_compaction_plan`, install/stage/complete/validate, `sweep_lsm_delete_pending_once` |
| `compaction.rs` | `compact_one_sealed_slab`/`compact_sealed_slab_records`/recluster + `CompactionRecord`/`Recluster*`/`CompactedFrame*` (~12434+) + manifest-commit application |
| `retention_reclaim.rs` | `prune_versions_*`, table-drop reclaim, tombstone-range reclaim, `mark_record(s)_for_table_drop` + prepared-rewrite types not pinned to root |
| `secondary_index_segments.rs` | persist/load/delete/reconcile/gc secondary-index segments, coverage, `SecondaryIndexSegmentInfo`, `LsmPreparedSecondaryIndexSegment(s)` |
| `tests.rs` | the `#[cfg(test)] mod tests` block (~15825–end) + the three `#[cfg(test)]` helper methods as a `#[cfg(test)] impl SlabManager` block |

(The table is the intent; exact span→island assignment is derived at impl. A method whose island is
ambiguous stays in root — over-carving is the failure mode, not under-carving.)

## Phases (each ends `cargo check -p wendelon` green; commit-sized steps if useful)

**Phase 0 — Scaffold + architecture source helper.** Create the `storage/manager/` directory; add the
architecture-test `manager_source_files()` / `manager_source()` helper (root first, islands sorted,
`tests.rs` excluded — clone of `data_server_source_files()`) and retarget every existing direct read
of `src/storage/manager.rs` that is meant to scan production manager source. Confirm `cargo check` +
the full architecture binary green before moving any code. Isolates "the directory machinery works"
from "the moves are correct."

**Phase 1 — Extract `tests.rs` first (the big, safe win — Codex's ordering).** Move the
`#[cfg(test)] mod tests` block (~10,600 lines, ~40% of the file) to `storage/manager/tests.rs`
(`use super::*;` header; assertions untouched), declared `#[cfg(test)]\nmod tests;` in root. Move the
three `#[cfg(test)]` helper methods into a `#[cfg(test)] impl SlabManager` block in `tests.rs` (call
sites unchanged). De-version the `// V1 sparse projection …` comment (D7). The root drops by ~40%
before any behavior-bearing code moves. `cargo test -p wendelon --lib` + the storage integration
targets must stay green.

**Phase 2 — Carve the islands (the bulk).** One island at a time, in the D2 order
(stats_facts → encoding_compute → tiered_io → lsm_driver → compaction → retention_reclaim →
secondary_index_segments): move the island's methods into an `impl SlabManager` block + its colocated
types, head `use super::*;`, `pub(super)` everything moved (items, fields touched, methods),
`mod island;` in root. `cargo check --all-targets` + the island's targeted integration suite between
each (e.g. `lsm_driver` → `lsm_picker`/`lsm_worker`/`lsm_commit`; `compaction` → `lsm_output`/
`tombstone_compaction`; `secondary_index_segments` → `lsm_secondary_index`/`encoding_secondary_index`;
`encoding_compute` → `encoding_lsm`/`encoding_materialize`).

**Phase 3 — Guards.** Add the seven `adr078_*` guards. Tighten `adr078_root_is_bounded`'s cap to the
real post-carve root size + modest headroom (no aspirational number — the D5 follow-on ratchets it
down). `adr078_no_mod_rs_root` asserts `storage/manager/mod.rs` does not exist.

## Verification gate

```text
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test  -p wendelon --test architecture_test adr078
scripts/cargo-cgroup test  -p wendelon --test architecture_test          # FULL binary
scripts/cargo-cgroup test  -p wendelon --lib                             # zero assertion edits
# focused storage-engine witnesses; full `--tests` below is the canonical "every storage target" proof:
scripts/cargo-cgroup test  -p wendelon --test lsm_commit
scripts/cargo-cgroup test  -p wendelon --test lsm_picker
scripts/cargo-cgroup test  -p wendelon --test lsm_worker
scripts/cargo-cgroup test  -p wendelon --test lsm_secondary_index
scripts/cargo-cgroup test  -p wendelon --test encoding_lsm
scripts/cargo-cgroup test  -p wendelon --test encoding_secondary_index
scripts/cargo-cgroup test  -p wendelon --test encoding_materialize
scripts/cargo-cgroup test  -p wendelon --test tombstone_compaction
scripts/cargo-cgroup test  -p wendelon --test backup_restore
scripts/cargo-cgroup test  -p wendelon --test control_ring_retention
scripts/cargo-cgroup test  -p wendelon --test descriptor_fingerprint   # golden/preimage stability
scripts/cargo-cgroup test  -p toron    --test wendelon_storage_execution_test   # cross-ring
scripts/cargo-cgroup test  -p wendelon --tests   # canonical full-binary proof required by D1
cargo fmt -p wendelon -- --check
git diff --check
```

## Review aids (a 26k-line move needs them)

- **Moves-only claim, mechanically checkable:** `git diff -M` rename detection + a per-island
  assertion that moved method bodies are byte-identical modulo `impl`/import scaffolding. The
  evidence doc states no statement, branch, match arm, predicate, encoding decision, manifest write,
  or await ordering was edited.
- **Storage-format continuity is the neutrality proof:** the focused witnesses above plus full
  `wendelon --tests` passing with zero assertion edits cover the `lsm_*`/`encoding_*`/
  `*_secondary_index`/`backup_restore` storage paths — `--lib` alone does not exercise the on-disk
  paths. Call these out explicitly.
- **Re-export completeness:** `cargo check` (no unresolved `storage::manager::` paths).
- **Architecture source continuity:** full `architecture_test` proves every prior guard reads
  `manager_source()`, not the removed flat-file path.

## Named falsifications (for the review)

1. Edit an island method body during the move ("while I'm here"): the storage integration suites must
   catch any behavior change; the evidence doc must assert moves-only regardless.
2. Move a public or crate-public type without preserving its root path with `pub use`: `cargo check`
   fails for crate-internal references; the public-surface guard pins the known public case.
3. Leave an island's method bodies in root: `adr078_slab_manager_public_surface_stays_in_root` /
   `adr078_root_is_bounded` fail.
4. Touch an on-disk format / encoding / crypto / wire file: `adr078_no_storage_wire_or_runtime_surface_change` fails.
5. Create `storage/manager/mod.rs`: `adr078_no_mod_rs_root` fails (and the manifests-only guard fails).
6. Leave an old guard reading `src/storage/manager.rs`: full `architecture_test` fails after the
   directory conversion.

## Hygiene

- No m-word; no version-suffix idiom. The `// V1 sparse projection` comment is the one in-scope
  de-version (D7); `*.v1`/`default_kv_table_descriptor_v1`/`b"v1"` format & fixture identifiers stay.
- No goldens change (pure source reorg — no preimage-feeding type touched); `descriptor_fingerprint`
  confirms. If a fingerprint moves, the move was not neutral — stop and find the edit.
- `cargo fmt` after the moves (several guards read formatted source; `pub(super)`-widened signatures
  reflow, as in 077).
- Cargo via `scripts/cargo-cgroup` (a 26k-line file's build is exactly what the cgroup wrapper bounds).
- `LiveWendelonFixture` flake discipline: rerun any distributed-fixture failure in isolation 3–5×
  before treating it as a regression — the full-binary result is canonical.

## Open for the implementing session (verify-first)

1. Exact span→island boundaries (Phase 2 — derive from the file; the method clusters and the type
   definition lines are the anchors). A method shared across islands stays in root.
2. The real root line cap once Phase 2 lands (set the guard from the actual number + headroom).
3. Whether `encoding_compute` or the `lsm_driver`/`compaction` pair needs a sub-split (decide from
   post-carve size; defer unless it busts the cap).
4. Whether any island-internal type turns out externally referenced (`pub use` it from root), beyond
   the known `SecondaryIndexSegmentInfo` path-preservation case.
