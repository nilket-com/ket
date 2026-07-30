# Topology-only semantics for immutable diff snapshots

Scope: `:panediff`/`:diffstep` snapshot documents request a topology-only
semantic pass — full structural graft, lexical viewport coloring, **no
`AnalysisHost`, no diagnostics, no deep semantic highlighting**. Eliminates
the second full semantic pass per diff and the per-commit provider-host
accumulation. Deliverable is one `impl:` commit reviewed against this plan.

## Verified costs being removed (2026-07-30)

1. Snapshots materialize via `ReplaceDocument` with an OID-keyed virtual
   path (`git.rs:318`, `.baryon-snapshots/<oid>/<rel_path>`) and then run
   the full pipeline like any document.
2. The Rust provider's `semantic_reactor` caches one single-file
   `AnalysisHost` per path in `ProviderState.single_files` with **no
   eviction** — every `:diffstep` step retains another rust-analyzer
   database for the life of the process.
3. `parse_structural_topology` for Rust is a pure `ra_ap_syntax` parse that
   never touches `ProviderState` — topology-only requests create no host.

## Mechanism

- `SemanticRequest` gains `depth: AnalysisDepth` (`Full | TopologyOnly`) —
  a field, not a new `ReactorRequest` variant, so routing, request-id
  bookkeeping, and stale-rejection stay untouched.
- Worker, `TopologyOnly`: run `parse_structural_topology` on the request
  bytes; skip `semantic_reactor`, skip injections, return empty
  highlights/diagnostics/annotations. (UTF-8 and `MAX_SEMANTIC_BYTES`
  gates unchanged.)
- Policy lives at the existing explicit provenance boundary:
  `snapshot_partners: HashMap<BufferId, SnapshotProvenance>`, which rendering
  already receives. A snapshot provenance selects `TopologyOnly`; absence
  selects `Full`. Nothing infers policy from paths. Do **not** add a duplicate
  bit to `TextBuffer`: provenance is removed only after successful `:w PATH`
  conversion and on release, so it already owns the exact lifecycle and cannot
  drift from the document's actual role.
- Analysis depth participates in semantic request/cache identity. Snapshot
  materialization resets the pipeline before provenance is attached; successful
  `:w PATH` resets it before provenance is removed. A response from a prior
  depth is still rejected by the existing request id, and a same-state policy
  transition must schedule a request at the new depth rather than reuse the old
  cache entry.
- Rendering: snapshot panes keep the always-on lexical baseline (already
  viewport-local and provider-cheap); empty semantic highlight overlay is
  the correct, honest result. The hybrid diff still gets grafted topology
  on both sides — structural diff quality is unchanged.
- Freshness: the graft applies normally (`applied_structural_state_id`
  set); the `:uast` inspector on a snapshot reads `semantic: current` with
  provider kinds intact. Nothing records `Unavailable` for a pass that
  deliberately skipped analysis — if a distinct display label is wanted,
  it is cosmetic and optional.

## Explicitly parked (each needs its own promotion)

- **Semantic-span translation** through unchanged diff ranges (live pane →
  snapshot pane): phase 2, only if lexical-only snapshot panes prove
  visually insufficient in practice. It is an approximation (context
  changes can alter resolution even in textually unchanged ranges) and
  must be labeled as such if built.
- **Provider host eviction** (`single_files` has no eviction policy at
  all — worktree fallback files accumulate too): real follow-up, separate
  pair; this plan only stops snapshots from feeding it.
- **Incremental provider reparse + transactional patch-grafts**
  (`StructuralUpdate::Patch` sketch): parked pending evidence; deserves an
  ADR. Notes for that ADR recorded here so they aren't lost: the
  `retire_orphaned_forest` multi-head transport already fits patch
  retirement; patch v1 keeps conservative whole-tree stamp invalidation;
  Rust first via `ra_ap_syntax`'s native incremental reparse.
- **Never**: grafting historical content into the live root or sharing red
  nodes across roots. Single-parent, single-owner identity is load-bearing
  for the stage-1/3/5 accounting, retirement, and reuse proofs. Two panes
  are two documents.

## Tests

1. A `TopologyOnly` reactor request returns provider topology while a counting
   provider proves its deep `semantic_reactor` and injection paths were never
   called. For Rust, topology parsing leaves `single_files` unchanged (the
   host-leak assertion — the headline).
2. Materializing and stepping a snapshot across N commits issues topology-only
   work: topology grafted at each step, no deep-provider calls, structural
   (AST-model) diff blocks present.
3. Snapshot pane renders lexical coloring; live pane's full semantics
   unaffected; diagnostics absent on the snapshot, present on the live
   side.
4. Inspector on a snapshot: grafted topology, provider kinds, honest
   freshness.
5. Existing panediff/diffstep/chronology suites pass unchanged.
6. Policy is explicit: a non-snapshot document never receives
   `TopologyOnly`; successful snapshot `:w PATH` returns to `Full`, while a
   failed conversion retains `TopologyOnly`.

## Review checklist

- [ ] `AnalysisDepth` threaded through request/worker; stale-rejection and
      routing untouched.
- [ ] Policy derives only from explicit snapshot provenance; default `Full`;
      successful provenance removal schedules `Full`.
- [ ] Rust `TopologyOnly` path provably never constructs or touches an
      `AnalysisHost` (test + the audit-family source check if cheap).
- [ ] Diff quality unchanged (structural blocks, chronology colors).
- [ ] No new inspector regime confusion; freshness stays honest.
- [ ] Parked items stay parked — no span translation, no eviction, no
      patch machinery smuggled in.
