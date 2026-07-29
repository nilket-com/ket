# ADR 0001: UAST registry memory reclamation

Status: accepted 2026-07-29. Owner: staged rollout, one `plan:`/`impl:` pair
per stage. This ADR records the decision and its invariants; each stage gets a
separate implementation plan under `../impl/`.

## Context

The `UastRegistry` is a process-global, fixed-capacity (16M node) bump
allocator. `NodeId`s are never freed or reused (`ecs/registry.rs`: no free
list; `stats().allocated_nodes` is `next_id - 1`). Some bulk creation paths
return capacity errors, while mutation and graft allocation still reach the
panicking `alloc_node_internal`. Every structural graft orphans the previous
child tree; every load/reload replaces a root; generation-swap undo/redo
replay mints fresh roots per keystroke (`engine/runtime/edit.rs`,
`plans/baryon/object-identity.md` §2.2). Orphaned subtrees retain their
`virtual_data` payloads (full source copies distributed across leaves at graft
time) indefinitely. The `:uast` inspector exposes the symptom — allocated
high-water far above any bounded reachable count — but cannot attribute it,
because the counter is process-wide across all documents, panels, and retired
generations.

## Invariant (load-bearing, preserved at every stage)

**A stale `NodeId` must never denote a different live object.** Today this
holds by construction (ids are never reused). Any stage that changes what a
stale id dereferences to must replace the by-construction guarantee with an
explicitly checked one, and must audit consumers of the current behavior
("reads of an orphaned node succeed and return stale content").

## Decision: five stages, in order

1. **Owner-attributed node accounting.** Exact, O(1)-maintained node counters
   before any optimization. Attribution follows *buffer/owner lineage*, not
   just the current root — reload replaces the root, so root-local counters
   alone would sever the link to earlier retired generations. Stage 1 does not
   pretend that `virtual_data` bytes are owner-attributable yet: hydration,
   promotion, splicing, replacement, and eviction currently lack one
   owner-aware accounting boundary. See
   `../impl/reclamation-1-accounting.md`.
2. **Eliminate pathological allocation callers.** Foremost: generation-swap
   undo/redo replay re-ingesting a fresh root per keystroke. Measured by the
   stage-1 counters before and after.
3. **Attribute, then retire orphan payload bytes.** First establish an exact
   owner-aware payload-byte boundary across hydration, mutation, graft,
   rebinding, and eviction. Then drop `virtual_data` of orphaned subtrees after
   publication plus an in-flight-work quarantine (model: request/state/root
   stale rejection in the semantic and navigation paths). Ids remain
   never-reused; only heap payloads are reclaimed. Stale reads of retired
   nodes degrade from "old content" to inert/unavailable, and that changed
   contract must be audited rather than assumed equivalent to an ordinary
   cold physical extent.
4. **Degrade gracefully at capacity.** Replace the
   `expect("UastRegistry capacity exceeded…")` panic with refusal paths
   (skip the semantic pass, keep the flat tree, surface a status message).
   Must land before any reclamation of stage 5's kind, so reclamation bugs
   manifest as degradation rather than crashes.
5. **Reuse retired slots and optionally decommit dead pages.** These are
   complementary, not alternatives, with separate proof obligations.
   *Amended 2026-07-29: accepted ADR
   `0002-registry-address-space-reclamation.md` decides the order (bounded,
   non-repeating generation-checked slot reuse first; page decommit deferred
   and conditional), its prerequisite stages, and both proofs.*
   - *Page retirement* (drop fully-retired, quarantine-cleared backing pages)
     reclaims **memory** but not address space. It only pays when allocations
     are clustered so pages become wholly dead. Proof obligation: define and
     audit late access before choosing tombstones, inert defaults, or another
     representation.
   - *Slot reuse* uses a 24-bit slot + 8-bit non-repeating generation inside
     the existing `NonZeroU32`, preserving the `Option<NodeId>` niche and
     12-byte `TreeEdges`. A slot is burned rather than wrapping. It reclaims
     **address space** — `next_id` otherwise still approaches the 16M ceiling.
     Proof obligations include checked dereference, I/O quiescence, exact-set
     reservations, full slot/index reset, and allocation-event accounting.

## Non-goals

- No change to graft semantics, id stability within a generation, or the
  headless boundary.
- The inspector remains a constant-node observer (it reuses its output
  root/leaf via `replace_virtual_document_data`) and is the measurement
  dashboard for every stage, not a driver of registry behavior.

## Plan-document convention

Decisions live in `crates/libaryon/plans/adr/NNNN-*.md`; per-stage
implementation plans in `crates/libaryon/plans/impl/*.md`. Repo-root `plans/`
is unchanged and remains the home of earlier documents.
