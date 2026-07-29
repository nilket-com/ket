# Reclamation stage 1: owner-attributed node accounting

Parent: `../adr/0001-ecs-memory-reclamation.md`. Scope: exact node-allocation,
retirement, and liveness counters plus their display. **No freeing, no
allocation-behavior change, no payload-byte accounting, and no `NodeId`
semantics change.** Deliverable is one plain `impl:` commit reviewed against
this plan.

## What exists (verified 2026-07-29)

- `UastRegistry::next_id` is the process-global allocation high-water.
  `stats().allocated_nodes` reads it in O(1).
- Production text mutations, semantic graft application, reload, replacement,
  and generation replay pass through `with_text_buffer_mut` /
  `EditorSession::mutate_text_buffer`. Registry allocation does not occur on
  the semantic parser thread; the parser returns an owned `GreenNode`, and the
  engine thread applies it.
- `apply_graft` validates before allocation, retains the document root, detaches
  every old child subtree, and installs an all-new descendant tree.
- Ordinary mutation retirement funnels through `disconnect_node`; one call
  retires exactly one node. `prune_empty_ancestors` may call it repeatedly.
- A root replacement is visible after the mutation closure as
  `root_before != root_after`. Closing a buffer is a separate lifecycle
  boundary in `runtime::release_buffer`.
- `virtual_data` changes through graft distribution, hydration, in-place
  splicing, physical-to-virtual promotion, save rebinding, output-panel
  replacement, and eviction. There is no single owner-aware O(1) delta seam
  today; stage 1 must not label resident bytes as retained owner payload.

## Counter model

### Process-global retirement events

`UastRegistry` gains one monotonic `AtomicU64`:

```rust
retired_nodes: AtomicU64
```

`note_nodes_retired(count)` performs the checked/saturating atomic increment
and bumps `inspector_revision` when `count != 0`.
`RegistryStatsSnapshot` exposes `retired_nodes`. The value is historical
retirement traffic, not reclaimed capacity: stage 1 never decrements
`next_id`, frees a page, or makes a retired node unreadable.

Production retirement sites:

| boundary | retired count |
|---|---:|
| `disconnect_node` | `1` |
| successful graft | the owning buffer's `current_live_nodes - 1` (root retained) |
| successful root replacement | the owning buffer's entire pre-mutation `current_live_nodes` |
| abandoned newly allocated candidate tree | the allocation delta for that candidate |
| final buffer release | the buffer's `current_live_nodes` |
| file-browser release | its fixed two-node document |

Output panels are retained by current session policy; replacement reuses their
stable root and leaf and therefore emits no retirement event.

### Per-buffer owner lineage

`TextBuffer` gains:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UastOwnerAccounting {
    allocated_nodes: u64,
    retired_nodes: u64,
    current_live_nodes: u64,
    root_generations: u32,
}
```

Invariant while the buffer is live:

```text
allocated_nodes - retired_nodes == current_live_nodes
```

`allocated_nodes` and `retired_nodes` are lifetime traffic for this `BufferId`;
they survive root replacement. `root_generations` starts at one and increments
only when the buffer publishes a different root. A newly created unnamed text
buffer begins at `allocated=2, retired=0, live=2, roots=1`.

The file browser remains outside per-owner inspector attribution because
`:uast` inspects text buffers. Its fixed nodes still participate in the global
allocated/retired totals.

## Transaction boundary

`EditorSession::mutate_text_buffer` snapshots before the closure:

- registry allocated high-water;
- registry retired-event high-water;
- buffer root and `UastOwnerAccounting`; and
- state/topology revisions already captured by the existing fan-out logic.

After the closure it reads the two registry high-waters again. The allocation
delta belongs to the acting `BufferId`: production registry allocation is
serialized on this engine mutation boundary. Retirement events emitted inside
the closure likewise belong to that buffer. Then:

1. If the root is unchanged, add the allocation and retirement deltas and
   derive `current_live_nodes` from the invariant.
2. If the root changed, emit one additional retirement event for the old
   `current_live_nodes`, add it to owner retirement, increment
   `root_generations`, and derive the new live count. Allocations for the
   replacement root are already in the sampled allocation delta. Current
   production replacement paths do not also disconnect pieces of the old root
   in the same transaction; assert that rule. If a future operation needs both,
   it must report typed old-root versus candidate-root retirement rather than
   double-counting the generic event delta.
3. If an operation allocates a candidate tree but does not publish it, that
   operation must call `note_nodes_retired(candidate_count)` before returning.
   The wrapper then charges both allocation and retirement to the initiating
   owner without falsely increasing live nodes.

No owner update occurs outside this transaction. Direct root assignments must
remain inside `with_text_buffer_mut`; add a source-level architecture test or
focused audit assertion if the current module boundary cannot enforce that
statically.

### Successful graft

Immediately after `validate_graft`, the old tree is still intact. The engine
knows the owner's exact `current_live_nodes`. On successful publication it
emits retirement for `current_live_nodes - 1`; the generic mutation wrapper
samples that event and the graft allocation delta. Therefore:

```text
new live = old live + newly allocated - (old live - 1)
         = newly allocated + 1
```

The root is counted exactly once and is never reported retired by a graft.
Rejected grafts validate before allocating or emitting retirement, leaving all
counters unchanged.

### Disconnects and abandoned candidates

`disconnect_node` emits one event after the unlink succeeds. Repeated ancestor
pruning naturally emits repeated events; no subtree walk is introduced.

Audit every production path that can reserve nodes and then fail before
attachment, especially file ingest/device registration and shadow-generation
reload. Such a path records its reserved count as retired before returning the
error. This is accounting only: nodes remain readable and allocated.

## Surfacing

The textual inspector header keeps the existing global high-water and adds:

```text
owner-nodes: live=N retired=R allocated=A roots=G · global-retired=T
```

The engine obtains owner accounting from the paired source `TextBuffer` and
passes it to `render_snapshot`; `UastInspectorSnapshot` remains a registry-only
metadata value and does not learn `BufferId`.

This is a sixth textual header row, so `INSPECTOR_HEADER_LINES`, stamped row
coordinates, cursor selection, and related fixtures advance together.

The existing `reachable` field remains the bounded snapshot result. It may be
less than `owner-nodes: live` and must retain its `>=N (truncated)` wording.
Existing global `reachable-resident` / registry resident-byte diagnostics stay
explicitly unowned.

## Tests and proof obligations

1. **Counter algebra:** unit tests reject underflow and preserve
   `allocated - retired == live`.
2. **Exact bounded reconciliation:** a `#[cfg(test)]` exact reachable-node walk
   equals `current_live_nodes` after initial unnamed creation, initial file
   load, first graft, regraft, insert/delete paths that allocate and prune,
   output replacement (constant nodes), generation-swap undo/redo, and reload.
3. **Retained-root graft rule:** first graft and regraft both count the root
   exactly once.
4. **Graft rejection:** mismatched bytes change neither registry nor owner
   counters.
5. **Abandoned candidate:** a forced post-allocation load failure increases
   owner/global allocated and retired equally and leaves live unchanged.
6. **Lineage:** load, reload, and generation swap increment roots exactly once;
   ordinary edits and grafts do not.
7. **Release:** closing the last reference emits retirement once; jump-history
   retention defers it with the buffer, as today.
8. **Inspector:** host coverage distinguishes bounded reachable, owner live,
   global allocated, and global retired.
9. **Stage-2 evidence:** an ignored diagnostic test may demonstrate
   generation-swap replay increasing allocated/retired traffic per traversal;
   it must not encode current-bad growth as desired behavior.
10. **No hidden hot walk:** no new production subtree traversal and no
    per-token accounting in graft construction or `render_frame`.

## Non-goals

- No `virtual_data` byte attribution. Stage 3 first designs an owner-aware
  boundary that distinguishes logical virtual payload from hydrated residency.
- No payload drop, caller fix, allocation refusal change, page retirement, or
  id reuse.
- No claim that `global allocated - global retired` is reusable capacity.

## Review checklist

- [ ] All production retirement boundaries above emit exactly once.
- [ ] Every production allocate-then-abandon error path is audited.
- [ ] The root survives graft accounting.
- [ ] Owner algebra reconciles with exact walks after every listed lifecycle.
- [ ] Root lineage follows `BufferId`, not root identity.
- [ ] The inspector output panel remains a constant-node observer.
- [ ] Registry-only APIs remain free of engine `BufferId`.
- [ ] No new payload-byte claim or synchronous traversal appears.
