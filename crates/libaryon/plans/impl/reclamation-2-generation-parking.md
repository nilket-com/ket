# Reclamation stage 2: park generation roots across undo replay

Parent: `../adr/0001-ecs-memory-reclamation.md` stage 2. Scope: eliminate the
dominant pathological allocation caller—generation-swap undo/redo replay—by
parking and reviving generation roots instead of re-ingesting on every
crossing. **No payload dropping, no id/device reuse, and no change to
within-generation undo.** Deliverable is one plain `impl:` commit reviewed
against this plan.

## Pathology (verified 2026-07-29)

`HistoryReplay::SwapGeneration` (`engine/runtime/edit.rs`) currently performs
on every boundary crossing:

1. `load_root_from_path` ingests the target shadow-generation file into a new
   root and sparse extent tree and consumes a new dynamic device id.
2. The outgoing root is retired and its backing device is unregistered.

Undo/redo ping-pong therefore grows global allocated and retired node traffic
by approximately one tree and burns one device id per keypress while owner
live nodes remain flat.

At a generation marker, the outgoing document has had its later text deltas
unapplied. Its bytes match the immutable generation target, although its
mutation-born topology need not match a fresh ingest. That byte-exact tree is
safe to revive and avoids rebuilding equivalent document state.

## Cache and identity

`TextBuffer` owns a small engine-side cache; no registry API learns
`BufferId`:

```rust
const GENERATION_PARK_CAP: usize = 4;

struct ParkedGenerationRoot {
    target: GenerationTarget,       // generation id, path, expected byte len
    root: NodeId,
    device_id: Option<u16>,         // remains registered while parked
    live_nodes: u64,
    file_identity: BackingIdentity, // O(1) metadata identity at park time
}

struct ParkedGenerationCache {
    // At most four entries; a VecDeque in LRU -> MRU order is sufficient.
    entries: VecDeque<ParkedGenerationRoot>,
}
```

Do not add an LRU dependency for four entries. Keys compare both generation id
and path. A cache hit validates all of:

- cached target byte length equals the requested target;
- `registry.get_total_bytes(root)` equals that byte length; and
- the current generation file metadata identity equals the identity recorded
  at park time.

Factor the existing backing metadata construction so validation includes
length, modification time, and file identity where the platform exposes it.
Missing or changed files invalidate the entry. Validation performs metadata
I/O only on an explicit undo/redo command, never during rendering.

Same-length external tampering that a platform cannot distinguish by metadata
is outside the immutable shadow-directory contract.

## Atomic swap flow

Parking the outgoing root before the incoming side is ready is forbidden: a
load or manifest-sync failure must leave the active document unchanged. A
temporarily removed valid hit is restored; an entry already proven invalid
remains evicted.

The `SwapGeneration` arm becomes:

1. Capture the current `ShadowSession::current_generation()` as the outgoing
   generation target. It must describe the active shadow-backed document.
2. Acquire the incoming root:
   - valid cache hit: temporarily remove the entry;
   - invalid hit: retire/unregister it through the typed parked-eviction path,
     then fresh-ingest;
   - miss: fresh-ingest with today's abandoned-candidate cleanup.
3. Call `sync_shadow_to_generation`. On failure:
   - restore a temporarily removed valid cache entry without eviction; or
   - cancel/unregister/retire a newly ingested candidate;
   - do not park, replace, or retire the active root.
4. Only after successful sync, insert the outgoing root and its still
   registered device into the cache, set the typed root transition, and
   publish the incoming root/device. LRU overflow retires the evicted parked
   entry exactly once.
5. Ledger commit, folds/cursor restoration, pipeline reset, and search clearing
   remain shared between cache-hit and ingest paths.

Metric scans remain active while a root is parked because its backing device
also remains registered. Do **not** call `cancel_metric_scan` when parking.
Cancellation occurs immediately before parked eviction/unregistration. A
revived root therefore needs no scan reconstruction and retains its metric
certainty.

The first traversal of two generations still performs their necessary
ingests. Once both are resident, repeated ping-pong allocates no nodes and
consumes no device ids. Linear traversal or revisiting an evicted generation
can still consume ids; stage 2 reduces repeated consumption but does not claim
id reuse or a global bound.

## Shadow lifecycle

Parked roots are valid only while their `ShadowSession` still owns the
corresponding immutable generation.

- Before `checkpoint_substitute_generations` truncates a redo tail, collect
  that tail from the current session and retire matching cache entries before
  their files are deleted.
- Before any attached shadow session is discarded by save, write-as,
  write-and-quit, load/reload, replacement, or buffer teardown, drain its
  parked cache, cancel scans, retire nodes, and unregister devices.
- A newly created or recovered shadow session starts with an empty cache.
- Buffer release drains active and parked roots once; jump-history retention
  continues to defer the whole buffer and cache.

Introduce one engine-side helper for attached-shadow disposal rather than
scattering cache draining beside raw `discard_shadow_session` calls. Raw
discard remains valid only for a local shadow candidate that was never
attached and therefore cannot own parked roots. Add a focused source audit or
architecture test documenting this split.

## Typed accounting

`UastOwnerAccounting` gains `parked_nodes`; its live-buffer invariant becomes:

```text
allocated_nodes - retired_nodes == current_live_nodes + parked_nodes
```

`TextBuffer` gains two pieces of mutation-boundary state:

```rust
enum RootTransition {
    Retire, // default for load/reload/replacement
    Park { revived_live: Option<u64> }, // None=fresh ingest, Some=cache hit
}

pending_root_transition: Option<RootTransition>,
pending_parked_retirements: u64,
```

Parked eviction inside a text mutation emits the global registry retirement
event and adds its exact count to `pending_parked_retirements`; it does not
directly update owner counters. This distinction is required because the
stage-1 wrapper otherwise sees only one global retirement delta and cannot
tell live-node pruning from parked-node eviction.

After the mutation closure, the wrapper:

1. takes `pending_parked_retirements` and asserts it does not exceed the
   sampled global retirement delta;
2. applies that typed portion by decrementing `parked_nodes` and incrementing
   owner `retired_nodes`;
3. treats the remaining retirement delta as ordinary live/candidate
   retirement; and
4. consumes the root transition if and only if the root changed.

Transition rules:

- `Retire` (also the default when no transition was recorded): ordinary
  closure retirement must be zero; retire the complete old live tree, preserve
  the already-applied parked evictions, and make the allocation delta the new
  live count.
- `Park { revived_live: None }`: ordinary closure retirement must be zero;
  move old live to parked and make the fresh allocation delta current live.
- `Park { revived_live: Some(n) }`: ordinary closure retirement and allocation
  delta must both be zero; move old live to parked and `n` from parked to live.

All three root changes increment `root_generations`. An unchanged root with a
pending root transition is a logic error. An unchanged-root transaction may
combine ordinary live pruning/candidate retirement with typed parked
eviction; both portions reconcile separately.

Eviction outside the wrapper (final buffer release) retires active plus parked
nodes globally; the owner is being destroyed and needs no subsequent counter
update.

This removes stage 1's release-build ambiguity: root replacement no longer
drops an untyped closure retirement delta. Unexpected combinations use hard
assertions (not `debug_assert!`) rather than silently skewing release-build
accounting.

## Surfacing and negative guards

The owner header becomes:

```text
owner-nodes: live=L parked=P retired=R allocated=A roots=G
```

Update the exhaustive `UastOwnerAccounting` destructure and exact owner-header
assertion to include `parked_nodes` / `parked=`. Keep both ADR stage-3 messages
and keep the shape byte-free; the stage-1 negative guards are intentionally
tripped and consciously resealed.

## Evidence and tests

1. Record the pre-change per-crossing allocated/retired/device-id deltas from a
   forced generation-swap fixture in a test comment or plan addendum, not in a
   decorated commit subject.
2. Ping-pong across one boundary at least six times. Once both generations
   have been ingested, later crossings allocate zero nodes and consume zero
   device ids. `root_generations` still increments per publication.
3. Exact bounded walks of the active root plus every parked root equal
   `current_live_nodes + parked_nodes` throughout the ping-pong.
4. Algebra tests cover park, fresh publication, revive, parked eviction,
   ordinary pruning, and underflow rejection for both live and parked buckets.
5. A forced sync failure proves the active root/device/accounting are
   unchanged and a temporarily removed valid hit is restored.
6. Cache overflow evicts LRU order, cancels scans, retires exactly once, and
   unregisters exactly one device.
7. Generation-file identity mismatch evicts the hit and successfully
   fresh-ingests the target.
8. Redo-tail truncation and attached-shadow disposal remove matching/all
   entries before filesystem deletion.
9. Buffer release retires active plus parked nodes once and unregisters every
   device; jump-history retention still defers release.
10. Load/reload/replacement retain default `Retire` behavior and lineage.
11. A pending metric scan can continue while parked and is canceled on
    eviction.
12. Both stage-1 negative guards gain `parked` and remain byte-free.

## Non-goals

- No payload retirement. A parked tree may contain mutation-born virtual
  leaves; immutable generation bytes make future reconstruction possible only
  after stage 3 designs an absolute-range ownership seam.
- No reload/`:e` dedup.
- No node-id or device-id reuse.
- No ledger marker, state-id, or within-generation replay change.

## Review checklist

- [ ] Cache-hit crossing allocates zero nodes and device ids.
- [ ] Failed acquire/sync leaves the active root unchanged and restores a
      temporarily removed valid hit.
- [ ] Parked roots are not retired and retain devices/scans until eviction.
- [ ] Overflow, invalidation, shadow truncation/disposal, and release each
      retire/unregister exactly once.
- [ ] Typed parked retirement is split from ordinary retirement at the wrapper.
- [ ] `RootTransition` is consumed exactly on root change; default `Retire`
      preserves load/reload/replacement.
- [ ] Owner algebra reconciles against active plus parked exact walks.
- [ ] Both negative guards remain explicit and byte-free.
- [ ] No semantic-thread work or render-time I/O is added.
