# Reclamation stage 3b: reclaim orphaned payload bytes

Parent: `../adr/0001-ecs-memory-reclamation.md` stage 3, second half. Scope:
drop payload from retired UAST forests after a publication fence. This is the
first stage that frees retained document bytes. Node ids, topology, metrics,
spans, devices, and capacity behavior remain unchanged. Deliverable is one
`impl:` commit reviewed against this plan.

## Scope: retired forests, not parked trees

Parked generation roots remain live owner state. They are bounded by the
four-entry cache and can contain authoritative boundary splices that are not
reconstructible without re-ingestion. Reclaiming them while parked would break
zero-allocation revival and is out of scope.

Once a parked entry is evicted or its buffer is released, it becomes a retired
forest and follows the same reclamation path as every other orphan. The
unbounded target is the stage-3a gauge:

```text
orphaned_payload_bytes =
    orphaned_payload_bytes_added - orphaned_payload_bytes_removed
```

The recorded baseline is `(N - 1) * S` orphaned bytes after `N` accepted grafts
of an `S`-byte source. Stage 3b must return that gauge to zero without changing
the owner's live `S`.

## Corrected reader audit

The contract changes from “an orphaned `NodeId` keeps stale payload forever” to
“a reclaimed `NodeId` keeps identity/topology/metrics but has no resident
payload.” `NodeId` slots still never alias.

| Reader/state | Why reclamation is safe |
|---|---|
| semantic worker | Receives copied source bytes and never reads the registry. Responses are routed by monotonic request id and revalidated against buffer/root/state before grafting. |
| resolver whole/partial hydration | Publication checks per-node `content_generation`. **Reclamation must bump it before dropping bytes**; retirement alone does not currently do this. |
| metric scans | Retirement sites cancel the owning scan. Cancellation bumps pending leaves' generations; reclamation also bumps every visited node. |
| carets and folds | Root replacement invalidates/re-resolves carets and folds; semantic graft snapshots/restores them before the first post-retirement viewport is published. |
| diffs | Store byte/line ranges, not persistent `NodeId`s. A graft recomputes paired diffs before publishing its first new viewport. |
| inspector | Rows carry `(buffer, root, state, topology)` stamps. The inspector refreshes from the live root before the first new viewport; stale clicks are rejected/remapped without reading an old payload. |
| minimap / markdown preview | Publish owned snapshots and refresh by buffer/root/state/topology identity; they do not retain borrowed payload. |
| navigation / jump history | Pending navigation is stamped and root-revalidated. History stores rebased byte anchors; root replacement invalidates them. |
| undo / shadow replay | Reads immutable generation files or parked live roots, never retired trees. |

Implementation must re-run the `NodeId`/`virtual_data` source audit. Any
unguarded engine state that can dereference an old node after the publication
fence blocks implementation.

Update `plans/baryon/object-identity.md` §2.2: stale ids still cannot alias, but
retired payload becomes absent after the stage-3b fence. Do not claim that
metrics become zero: `get_total_bytes` and edge reads deliberately retain the
old inert metadata.

## Retirement transport: record exact forests once

Stage 3a retirement currently records only aggregate node/payload counters.
Reclamation also needs the forest heads. Replace production pairs of
`note_nodes_retired` + `note_orphaned_payload` with one typed registry seam:

```rust
enum ReclaimFence {
    AfterPublication,
    NeverPublished,
}

fn retire_orphaned_forest(
    &self,
    heads: impl IntoIterator<Item = NodeId>,
    retired_nodes: u64,
    authoritative_payload: u64,
    fence: ReclaimFence,
)
```

The seam:

- advances the stage-1/3a aggregate counters exactly once;
- asserts every head is detached (`parent == None`);
- posts a small retirement notice to a registry outbox;
- does not walk or free the forest.

The outbox is transport, not reclamation policy. `EditorSession` owns the
quarantine and DFS drain state and collects notices during rendering.

Production sites:

1. Successful graft: return/capture every old direct child detached from the
   stable root. Each is a forest head. Traversal of a head must visit its
   descendants but must not follow the head's stale `next_sibling`, because the
   other direct children are separately listed.
2. `RootTransition::Retire`: old root.
3. `disconnect_node`: the just-detached node.
4. Partial synthetic-fold endpoint replacement: each detached original leaf.
5. Parked eviction/disposal: each evicted root.
6. Buffer release: live root plus all parked roots.
7. Abandoned candidates: root with `NeverPublished`.

`NeverPublished` skips the publication epoch but remains budget-drained. A
large unpublished candidate must not introduce an unbounded synchronous walk.

Provide a tracked graft result for the production semantic path, while a
fixture-only compatibility wrapper may keep tests that only need the new module
id concise. Rejected grafts produce no retirement notice.

Add a source-audit test preventing production retirement from falling back to
bare aggregate counter calls; otherwise a future site could recreate an
unreclaimable orphan gauge.

## Publication fence: two cohorts, not same-frame draining

A retirement created before or during frame `F` must survive until frame `F`
has been fully projected and sent. Draining it later in the same call is not a
full-frame quarantine.

`EditorSession` owns:

```rust
struct ReclaimQueue {
    quarantined: VecDeque<RetiredForest>,
    eligible: VecDeque<RetiredForest>,
    in_progress: Option<ReclaimCursor>,
}

struct ReclaimCursor {
    stack: Vec<NodeId>,
}

const RECLAIM_NODES_PER_FRAME: usize = 4_096;
```

Frame order:

1. Collect registry retirement notices created by commands before the frame.
2. Dispatch semantic responses; collect notices created by grafts.
3. Drain at most 4,096 nodes from the **previously eligible** cohort.
4. Refresh diffs/previews/inspectors and project/send the viewport.
5. Collect notices once more after projection (synthetic-fold materialization
   can retire endpoints while a pane renders), then promote this frame's
   quarantined notices to eligible.
6. If eligible work remains, `try_send(InternalEvent::Reclaim)` so an idle
   editor continues bounded draining. This maintenance event drains one batch
   without publishing an otherwise indistinguishable viewport, then requeues
   itself while work remains. When its final batch completes with a UAST
   inspector open, publish one dashboard refresh. If the bounded engine
   request queue is full, the ordinary post-dispatch path observes the queue
   again and retries the maintenance event without making reclaim work itself
   render-worthy. Coalesce the wake per session so queued user work cannot
   multiply identical reclaim events.

`NeverPublished` notices enter `eligible` directly when collected.

The DFS cursor starts with exactly one forest head. When visiting a node it
pushes that node's direct children by walking their sibling chain; it never
walks the starting head's `next_sibling`. Edges of retired nodes are immutable,
so the cursor is stable across maintenance turns. Report both the visit count
and whether work remains from each drain step.

`take_hydrated_data` and reclamation are engine-thread serialized. A physical
split must complete publishing its replacement pieces before the mutation
returns and before any reclaim drain can run.

## Per-node reclamation

Add one intent method:

```rust
fn reclaim_orphaned_payload(&self, node: NodeId) -> ReclaimedPayload
```

Under the residency write lock it:

1. bumps `content_generation` before changing payload, invalidating late
   resolver publications and whole-leaf outcomes;
2. takes full payload and clears partial residency;
3. zeros resident-byte, hot-extent, and LRU bookkeeping;
4. if the node is span-less, increments both
   `authoritative_bytes_removed` and `orphaned_payload_bytes_removed` by the
   bytes dropped;
5. if span-backed, treats the bytes as hydrated cache only and moves neither
   authoritative/orphan counter;
6. returns bytes freed and payload class for exact tests.

Calling it again on an empty node is a no-op. Duplicate scheduling is a logic
bug worth a debug assertion, but release builds must not underflow counters or
panic while reclaiming an already-empty node.

The stage-3a write audit must recognize this method as the only production
span-less payload remover beyond the six class/write methods.

## Observable state

`RegistryStatsSnapshot` exposes cumulative
`orphaned_payload_bytes_reclaimed` in addition to the live orphan gauge.

The UAST header exposes reclaim progress from the engine queue, for example:

```text
orphaned-payload=12345 B reclaimed=67890 B reclaim-heads=3
```

The exact spelling is implementation-owned. It must distinguish “gauge has not
drained yet” from “no reclaim work is scheduled,” and queue progress must
participate in the inspector refresh key.

## Tests and proof obligations

1. Headline host test: `N` grafts of size `S`, publish the quarantine frame,
   then drain maintenance turns. Orphan gauge returns to zero, owner live
   remains `S`, and cumulative reclaimed grows by `(N - 1) * S`.
2. Partial drain: a retired forest larger than 4,096 nodes spans multiple
   maintenance turns; every step visits at most the budget. At each boundary:
   `registry retained == all owners' (live + parked) + orphan gauge`.
3. Fence: retirement frame still exposes the old payload to a deliberately
   held test id; only the following eligible drain removes it.
4. Late whole and partial hydration: a completion admitted before reclaim
   cannot repopulate a reclaimed span-backed orphan because its generation is
   stale.
5. Stale semantic/navigation/inspector paths reject after reclamation without
   reading payload.
6. Parked eviction and buffer release enqueue exact forests. Jump-history
   retention still defers buffer release and therefore retirement.
7. Abandoned candidate is immediately eligible but still obeys the per-turn
   budget.
8. Inert reads: after reclamation, metadata and edges remain readable,
   `get_total_bytes` retains its old metric, `resolve_physical_bytes` is empty,
   and `read_node_bytes_sync` on a span-less reclaimed leaf returns `NotFound`
   rather than panicking.
9. Live/parked safety: enqueueing an attached head fails. Fixtures reconcile
   queued heads against known live and parked roots.
10. Source audits cover both intent writes and typed retirement; object
    identity §2.2 is updated.

## Non-goals

- No payload reclamation from a root while it remains parked.
- No topology clearing, dead bit, page/slot reuse, or `next_id` change.
- No graceful capacity behavior (stage 4).
- No resident-eviction policy change for live physical extents.
- No synchronous unbounded cleanup, including abandoned candidates.

## Review checklist

- [ ] Reader audit re-verified; late resolver publication is generation-invalidated.
- [ ] Every production retirement records exact forest heads once.
- [ ] Two-cohort fence proves one fully published frame before reclaim.
- [ ] Idle draining self-schedules without blocking on a full request queue.
- [ ] DFS is resumable and visits at most 4,096 nodes per maintenance turn.
- [ ] Authoritative and orphan removal counters move together; hydrated drops
      move neither.
- [ ] Registry/owner/orphan reconciliation holds after every partial step.
- [ ] Reclaimed nodes retain inert identity/topology/metrics without panics.
- [ ] Parked live roots are never scheduled.
- [ ] Intent and retirement source audits pass; object-identity contract updated.
