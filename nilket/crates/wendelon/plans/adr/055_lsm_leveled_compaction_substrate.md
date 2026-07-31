# ADR 055: LSM-Style Leveled Compaction Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/055_lsm_leveled_compaction_substrate.md`

## Status

Accepted

V1 substrate commitments (council-confirmed):

- substrate-only scope: leveled merge picker + worker;
  the existing single-slab `tombstone_compaction` /
  `retention_compaction` /
  `secondary_index_compaction_context` primitives in
  `src/storage/manager.rs` are the *elision* primitives
  the leveled compactor invokes — lifted from per-slab to
  a streaming multi-slab merger;
- per-locality compactor on the locality leader,
  mirroring the `CdcLocalityHeartbeatWorker` shape (one
  worker per locality, spawned unconditionally on
  locality-leader startup);
- four levels (`L0..L3`); level count is a deployment
  setting, not a wire contract;
- internal `CompactionMergeKey = (row_hlc, row_key_id,
  record_row_index)`; storage-local; never crosses into
  CDC schema;
- L0 holds freshly-finalized slabs with overlapping HLC
  ranges; L1+ slabs are HLC-disjoint within each level
  per locality; L3 has no next level — slabs stay there
  until retention deletes them whole;
- compaction is **out-of-place**: the worker reads N
  input slabs from level `n`, plus all overlapping slabs
  from level `n+1`, and writes M output slabs at level
  `n+1`;
- per-locality manifest mirror carrying
  `(slab_id, level, state, hlc_range, byte_size,
  row_count, sidecar_coverage)`; manifest mutations ride
  the existing locality control ring (ADR 016) as a
  `CompactionCommit` mutation under the existing
  finalization-marker boundary; followers receive the
  mutation for failover-safe synchronization;
- compaction is **deterministic** — same inputs + same
  elision floor + same merge ordering = same logical
  output. Each replica runs its own compaction off its
  local copy of input slabs and produces logically-
  identical output (with locally-allocated `SlabId`).
  The control-ring mutation describes the *logical fact*;
  each replica applies it against its local manifest
  mirror;
- **MVCC reader pins**: readers acquire a manifest
  snapshot + slab pins at iterator open; pins survive
  concurrent compaction; retired slabs delete only after
  pin-count reaches zero. **No new LSM-introduced
  `relocated_reads` rows.** The existing
  `relocated_reads` machinery in
  `src/storage/manager.rs` (retention rewrites) remains
  in place untouched; ADR 055 declines to *add* LSM-
  driven relocation rows because sidecars are rebuilt
  out-of-place during compaction (ADR 052 §9), so
  cached `RecordLocation` references in old sidecars
  retire with the old sidecars;
- elision floor is **identical to ADR 048's retention
  floor**; no separate compaction floor, CDC floor, or
  operator override;
- elision rules at the merged stream are inherited
  verbatim: append-only / content-hash data+tombstone
  pair-GC (ADR 047 / 049); PK prefix pruning with
  **boundary-sentinel retention** (ADR 050 §7);
  migrated-range tombstone reclaim; fail-closed on
  ambiguous table ownership;
- secondary-index sidecar rebuild composes via ADR 052
  §9: every output slab gets a freshly-built sidecar;
  rebuilt sidecar digest verified before old sidecars
  retire; **replace-by-new, not in-place rewrite**;
- visible-state `StateDigest` invariance: cold rebuild
  of `digest.table.row.visible.v1` over the pre-commit
  and post-commit *manifest views localized to touched
  slabs* (O(input + output bytes), not O(table)).
  Unconditional at every commit. Mismatch aborts the
  commit and pauses compaction on the affected
  `(locality, table)` scope;
- per-version-entry digest (ADR 052
  `digest.secondary_index.entry.v1`) updated
  algebraically on the elision stream; sampled cold-
  rebuild verification at
  `lsm_digest_verify_sample_rate`;
- CDC interaction: substrate-only, retention-bound. CDC
  catchup acquires manifest-snapshot iteration pins at
  open; live-tail handoff reads ADR 054's in-memory ring
  (no pins). Subscriptions below retention floor reject
  with the existing `CDC_REJECT_BELOW_RETENTION_FLOOR`;
- bounded `lsm_*` metric family with
  `{level, result, kind}` label enumerations; pinned by
  architectural test;
- closed 3-code `LsmRejectCode` taxonomy:
  `ManifestRace`, `SidecarDigestMismatch`,
  `VisibleDigestInvariance`;
- pinned V1 defaults: `max_l0_slab_count = 4`,
  `lsm_l0_max_age_millis = 5_000`,
  `lsm_l1_slab_size_target_bytes = 64 MiB`,
  `lsm_size_ratio = 10`, `max_l1_slab_count = 4`,
  `lsm_picker_tick_millis = 100`,
  `lsm_gc_tick_millis = 1_000`,
  `lsm_compaction_concurrency_per_locality = 1`,
  `lsm_digest_verify_sample_rate = 1.0` (debug) /
  `0.01` (release);
- no new wire `MsgType`, no new `FRAME_FLAG_*`, no new
  `NodeCapability` bit, no Cargo feature, no new digest
  domain, no `ObjectStore` trait change, no on-disk
  format change beyond extending the per-locality slab
  manifest with `level`, `state`, `successors`, and HLC
  range (control-ring `MutationKind` extension is the
  carrier).

## Context

Roadmap #11. The first post-CDC large multi-ADR.

Storage already has tombstone (ADR 047), retention (ADR
048), supersession (ADR 050), and sidecar (ADR 052) per-
slab primitives operating one slab at a time. Without an
inter-slab merge schedule, write amplification stays at
1× but slab count grows monotonically with admission
rate; read amplification on PK lookups and HLC-range
scans degrades linearly. ADR 046 / 047 / 048 / 050 / 052
/ 054 collectively give the algebraic and lifecycle
substrate a leveled compactor needs to *prove* it does
the right thing — visible-state preservation, per-entry
digest algebra, sidecar rebuild discipline, CDC
reachability.

The roadmap pinned the load-bearing CDC question:

> "CDC's `safe_emission_hlc` watermark interaction with
> compaction needs explicit treatment (compaction must
> not advance below any active subscription's
> `cursor_high_water` when sidecars are being rewritten
> in-place, or ring-overflow recovery must reject
> cleanly)."

This ADR commits to the **second branch** —
ring-overflow / below-retention rejects cleanly via
existing mechanisms; compaction does *not* coordinate
with subscription cursors; sidecars rewrite **out-of-
place** so the "in-place rewrite" failure mode the
roadmap referenced never arises.

The constraints from ADR 003 (bifurcated writes), ADR
016 (R0 control ring; locality-keyed manifest mutation
carrier), ADR 029 (Identity vs Locality), ADR 030
(deployment-profile-tunables), ADR 037 (additive
evolution), ADR 042 (multi-row records), ADR 044 (slab
footer / row-group fetch), ADR 045 / 049 / 050 (row
identity), ADR 046 (`StateDigest`), ADR 047 / 048
(lifecycle), ADR 052 (secondary indexes), ADR 053 (wire
compression), and ADR 054 (CDC substrate) all hold.

## Architectural inextricability

Two design choices are physically inextricable here:

**1. Compaction's elision floor and ADR 048's retention
floor are the same value.** Treating them as separate
would mean either:

- a *stricter* compaction floor (compaction holds
  versions above retention floor) — letting read paths
  see rows the retention contract said are unreachable;
- a *looser* compaction floor (compaction drops
  versions above retention floor) — letting CDC
  subscriptions above retention floor silently miss
  events.

Neither is correct. The retention floor IS the
reachability contract; compaction is its physical
realization.

**2. MVCC reader pins, not subscription-cursor
pinning.** A "compaction yields to active subscriptions"
model would couple compaction throughput to consumer
liveness — a stuck consumer would freeze object-store
retirement cluster-wide. Manifest-snapshot reader pins
decouple this:

- pins are bounded by *active iteration*, not
  subscription identity;
- pin lifetime is per-iterator, bounded by single-scan
  duration;
- compaction always advances; an individual reader's
  pinned slabs deferred-retire only while the reader
  holds an iterator open;
- below-retention-floor consumers receive
  `CDC_REJECT_BELOW_RETENTION_FLOOR` (ADR 054); they
  don't block compaction.

The two flip together: a "yield to subscriptions" model
would also need a separate compaction floor (since
otherwise yielding buys nothing — the subscriber's
already-acked cursor is below retention by the time it
matters). V1 declines the bundle.

## Identity-vs-Locality framing

Per `plans/philosophy.md`, every record has an Identity
and a Locality. LSM compaction is **locality-axis**:

- Slabs are owned by a locality leader; compaction
  workers run on the locality leader.
- The merge key is `(row_hlc, row_key_id,
  record_row_index)` — three identity-bound newtypes.
  Locality is implicit (compaction is per-locality);
  table ownership is read from version-entry row-axis
  synthetic columns at retention-floor lookup, not from
  the merge key.
- The output is *equivalent on the identity axis*:
  every `RowKeyId` whose latest visible version-entry
  was reachable pre-compaction is reachable post-
  compaction.
- Cross-locality compaction is not a thing. The Unified
  Ring rule applies — there is one storage-management
  surface; "compaction-plane" is a forbidden framing.
- Identity-axis consumers (CDC subscribers naming
  `(TenantId, TableObjectId)`) see the locality
  compaction schedule as invisible internal state.
  Their cursor is preserved by compaction by
  construction (compaction does not invent new HLCs or
  rewrite existing ones).
- Replicas within a locality have synchronized
  manifests via the locality control ring. Compaction
  is deterministic enough that each replica reproduces
  the same logical output set from the same input set;
  local `SlabId` allocation differs but logical content
  is identical (visible-state digest matches across
  replicas).

## V1 scope

ADR 055 ships **only** the leveled compaction
substrate:

1. per-locality slab manifest extended with
   `(compaction_level, state, successors, hlc_range, byte_size,
   row_count, sidecar_coverage, manifest_epoch)`;
2. a pure compaction picker over a manifest snapshot +
   config + clock;
3. a compaction worker that reads input slabs, runs the
   merged stream through the existing elision
   primitives, writes output slabs at the next level,
   and atomically installs them via `CompactionCommit`
   on the locality control ring;
4. `SlabPinTable` for active-iterator manifest-snapshot
   pins;
5. `LsmGcWorker` deletes retired-and-unpinned slabs;
6. cold-rebuild visible-domain digest invariance check
   over input/output slab views at every commit;
7. per-version-entry digest algebraic update; sampled
   cold-rebuild verification;
8. bounded `lsm_*` metrics with pinned label
   enumerations.

It deliberately does **not** ship:

- size-tiered compaction picker;
- cross-locality compaction;
- subcompaction (parallelizing one merge across CPUs);
- compaction throttling / IO QoS;
- bloom filters per slab;
- manual compaction trigger RPC;
- CDC-cursor-aware compaction throttling;
- `relocated_reads` LSM helper;
- cross-replica compaction *deduplication*;
- hot-key compaction prioritization;
- compaction-time schema-evolution transforms;
- L4+ extension;
- direct compaction of already-tiered objects without
  local rehydration;
- replicated logical compaction event log separate
  from the control ring.

## Non-goals (V1)

- **No size-tiered compaction.** Leveled only.
- **No CDC-cursor-aware compaction throttling.**
- **No new compaction floor parameter.** Retention
  floor is the elision floor.
- **No compaction-yields-to-subscriptions pin.**
- **No `relocated_reads` LSM helper.** Manifest-
  snapshot pins suffice for LSM readers.
- **No `DigestEvictionMarker`.** Compaction is
  visible-state-preserving by construction; retention
  floor advance (ADR 048) owns visible-state digest
  reductions.
- **No new digest domain.** Reuse
  `digest.table.row.visible.v1` (ADR 046) and
  `digest.secondary_index.entry.v1` (ADR 052).
- **No new elision policy.**
- **No subcompaction.**
- **No cross-locality merging.**
- **No new wire `MsgType`.** Manifest mutation extends
  the existing locality control-ring `MutationKind`.
- **No new `NodeCapability` bit.**
- **No HRFT envelope `format_version` bump.**
- **No new `FRAME_FLAG_*`.**
- **No new wire compression codec.** ADR 053
  unchanged.
- **No new storage compression codec.** Outputs
  inherit current descriptor's compression policy for
  unambiguous typed rows; ambiguous rows preserve
  their existing encoded frame.
- **No `ObjectStore` trait change.**
- **No PBC / AGG / CDC protocol changes.**
- **No reserved enum slots, compatibility shims, or
  `V1` / `V2` Rust type suffixes.**
- **No project-name prefix on new symbols.** `lsm_*`
  metrics, `LSM_*` constants are functional acronyms.
- **No new Cargo feature or dependency.**
- **No durable visible-`StateDigest` placement** in
  slab footer or run manifest. Anti-entropy
  territory.
- **No L4+ extension.**
- **No `StorageTier` on the LSM substrate.** Existing
  sweeper handles L1=local / L2=tiered moves
  orthogonally.
- **No SQL / DDL surface.**

## Decision

### 1. Levels and runs

Four levels:

```rust
#[repr(transparent)]
#[derive(Copy, Clone, Pod, Zeroable, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CompactionLevelIndex(u8);

impl CompactionLevelIndex {
    pub fn try_new(raw: u8) -> Option<Self> {
        if raw <= 3 { Some(Self(raw)) } else { None }
    }
    pub const L0: Self = Self(0);
    pub const L3: Self = Self(3);
}
```

A compaction run is a slab plus its manifest summary:

```rust
pub struct LsmRunSummary {
    pub slab_id: SlabId,
    pub compaction_level: CompactionLevelIndex,
    pub state: SlabState,
    pub min_row_hlc: Hlc,
    pub max_row_hlc: Hlc,
    pub size_bytes: ByteCount,
    pub row_count: RowCount,
}

pub enum SlabState {
    Live,
    Retired { successors: Vec<SlabId> },
    DeletePending,
}
```

ADR 055 introduces no separate SSTable format. Slabs
remain the run format.

L0 holds freshly sealed slabs and may have overlapping
HLC ranges. L1+ slabs are **HLC-disjoint within each
level** for the locality. L3 has no next level in V1;
slabs stay there until retention deletes them whole.

The compactor's merge key inside a locality is:

```rust
pub struct CompactionMergeKey {
    pub row_hlc: Hlc,
    pub row_key_id: RowKeyId,
    pub record_row_index: RecordRowIndex,
}
```

HLC is primary because sealed data-ring slabs are
physically ordered for HLC range scans (ADR 045 / 050
/ 052). PK lookup goes through ADR 052 sidecars
(RowKeyId-keyed, independent of slab partitioning).
The CDC cursor remains identity-axis and never exposes
locality or level.

The merge key is **storage-local**. Architectural
guard pins: `CompactionMergeKey` is not imported by
any CDC module.

`table_object_id` for retention-floor lookup is read
from the version-entry's row-axis synthetic columns
(ADR 045 / 047 / 050), not from the merge key.

**The active slab is never a compaction input.**

### 2. Per-locality manifest with control-ring atomic install

The slab manifest is a per-locality artifact alongside
the persistent local row index. It is a *local
mirror*; mutations replicate to followers via the
existing locality control ring (ADR 016).

The manifest records:

- `manifest_epoch: ManifestEpoch`;
- per-slab: `slab_id`, `level`, `state`, `successors`,
  HLC interval, byte size, row count, sidecar coverage
  references.

Sealed admission slabs enter the manifest as
`(L0, Live)`. Compaction proposes a
`CompactionCommit` mutation:

```rust
pub struct CompactionCommit {
    pub job_id: CompactionJobId,
    pub manifest_epoch_pre: ManifestEpoch,
    pub slabs_to_retire: Vec<SlabId>,
    pub slabs_to_add: Vec<LsmRunSummary>,
    pub slabs_to_mark_delete_pending: Vec<SlabId>,
    pub slabs_to_remove: Vec<SlabId>,
    pub sidecar_swaps: Vec<SidecarSwap>,
    pub digest_summary: CompactionDigestSummary,
}

pub struct CompactionDigestSummary {
    pub visible_pre: StateDigest,
    pub visible_post: StateDigest,
    pub version_entry_delta: StateDigest,
}
```

Regular compaction commits populate
`slabs_to_retire` / `slabs_to_add`; GC lifecycle
commits populate exactly one of
`slabs_to_mark_delete_pending` or `slabs_to_remove`.
Mixed shapes are rejected.

The control-ring apply path validates:

- `manifest_epoch_pre` matches current epoch (else
  reject `ManifestRace`);
- every input slab exists, is `Live`, and matches the
  plan's level;
- every output `SlabId` is fresh;
- sidecar swap digest summaries match;
- visible-domain invariance summary is valid (else
  reject `VisibleDigestInvariance`).

On success: increment `manifest_epoch`; insert outputs
at the target level as `Live`; transition inputs to
`Retired { successors }`; install sidecar swaps; emit
`LsmCompactionCommitted`.

This rides the existing ADR 016 finalization marker
discipline. Pre-finalization, readers see the old
manifest; post-finalization, readers see the new
manifest. No mid-state.

**Why control-ring atomic install for physical state.**
Compaction is local physical work, but readers within
the locality need an atomic epoch boundary AND
followers need synchronized manifests for failover.
Using the existing control-ring finalization
discipline gives both without creating a new protocol
surface.

**Why this isn't "putting compaction on consensus."**
Compaction is **deterministic** — same inputs + same
elision floor + same merge ordering = same outputs.
Each replica runs its own compaction off its local
copy of input slabs and produces logically-identical
output (with locally-allocated `SlabId`). The control-
ring mutation describes the *logical fact* (these
inputs retired, these outputs admitted with these
summaries) — each replica applies the mutation
against its local manifest mirror. Cross-replica
logical equality holds via determinism; physical
bytes stay local.

### 3. Compaction picker

Triggers per locality leader, on a tick
(`lsm_picker_tick_millis`, V1 default 100 ms). Pure
function:

```rust
pub fn pick_compaction(
    manifest: &ManifestSnapshot,
    config: &LsmConfig,
    now: Hlc,
) -> Option<CompactionPlan>;
```

No IO. Time as argument. No lease acquisition.

V1 defaults:

```rust
pub struct LsmConfig {
    pub max_l0_slab_count: u32,                       // 4
    pub lsm_l0_max_age_millis: u64,                   // 5_000
    pub lsm_level_count: u8,                          // 4
    pub lsm_size_ratio: u32,                          // 10
    pub lsm_l1_slab_size_target_bytes: u64,           // 64 << 20
    pub max_l1_slab_count: u32,                       // 4
    pub lsm_picker_tick_millis: u64,                  // 100
    pub lsm_gc_tick_millis: u64,                      // 1_000
    pub lsm_compaction_concurrency_per_locality: u32, // 1
    pub lsm_digest_verify_sample_rate: f32,           // 1.0 / 0.01
}
```

Primary triggers:

- **`L0 → L1`**: `compaction_level_slab_count(L0) >=
  max_l0_slab_count` AND
  `oldest_l0_slab_age_millis(now) >=
  lsm_l0_max_age_millis`. Job picks the current L0
  set; adds every L1 slab whose HLC interval
  intersects the L0 union range.
- **`L1 → L2` and `L2 → L3`**: source-level total
  bytes exceed `target(level)`. Picks the source-
  level slab with highest size-pressure score; adds
  every next-level slab whose HLC interval
  intersects.

Level size targets:

```text
target(L1) = lsm_l1_slab_size_target_bytes * max_l1_slab_count
target(Ln) = target(L_{n-1}) * lsm_size_ratio   for n >= 2
```

Default targets yield L1 ≤ 256 MiB, L2 ≤ 2.56 GiB, L3
≤ 25.6 GiB per locality.

**Reasons as scoring inputs, not picker rules.** The
primary trigger is level-size; retention debt,
tombstone debt, index-coverage debt, layout debt, and
operator maintenance influence *which* eligible run
gets picked within a level, not whether to pick. The
job's `result` label on `lsm_compactions_total`
remains bounded; per-job audit records the dominant
reason.

The active slab is never an input. Backfill read-
leases on source slabs compose with compaction
sidecar-write leases per ADR 052 §7; compaction wins
on contention, backfill aborts with `compaction_race`.

**Time-travel above floor preserved first; space
recovery follows from the floor.**

### 4. Survivor stream — elision rules

The compactor builds one ordered version-entry stream
from the input slabs in `CompactionMergeKey` order, and
applies lifecycle rules in this order:

1. **Retention** (ADR 048) using a single
   `(compaction_start_hlc, lifecycle_catalog_snapshot,
   table_schema_snapshot)` captured at job start. Rows
   strictly below the floor become elision candidates.
2. **Append-only / content-hash data+tombstone pair-
   GC** (ADR 047 / 049) below floor.
3. **PK prefix pruning** (ADR 050 §7) with **boundary-
   sentinel retention**: newest below-floor PK version
   retained; older below-floor versions drop.
4. **Migrated-range tombstone reclaim** when reason
   applies.
5. **Fail-closed** on ambiguous table ownership.

Rows above the floor — including superseded PK
versions needed for time travel — survive verbatim.

The existing per-slab `tombstone_compaction` /
`retention_compaction` primitives encode rules 1-3;
LSM lifts them to streaming-multi-slab via a thin
wrapper.

Content-hash duplicate collapse (ADR 049 §5) does NOT
re-run at compaction.

### 5. Output slabs and sidecars

Output runs are fresh slab files written through the
normal slab writer, packed in `CompactionMergeKey`
order, split at target output size. The compactor
recomputes (does not merge): row index entries,
locality membership, tombstone metadata, supersession
metadata, column statistics blocks (CSB; ADR 039), and
secondary-index sidecars (ADR 052 §9).

Per ADR 052 §9:

- `Building` and `Ready` indexes both rebuild;
- `Dropping` indexes do **not** rebuild; ADR 052 §10
  two-phase GC reclaims their bytes;
- rebuilt sidecar digest verified against cold
  rebuild before old sidecar retires;
- rebuild is **replace-by-new**, never in-place
  rewrite;
- compaction over an uncovered input run heals
  coverage by emitting a covered output sidecar.

Backfill that loses the lease race aborts with
`compaction_race`; the output sidecar provides
coverage for the next backfill pass.

**No LSM relocation table.** Old sidecars retire with
old slabs; new sidecars point at output slabs. Active
iterators use pins (§7).

### 6. Pre-commit verification

Before proposing the commit, the worker:

1. fsyncs every output slab and sidecar;
2. acquires compaction write-leases on input slabs in
   sorted-`SlabId` order (deadlock-free; composes
   with ADR 052 §7);
3. validates input slabs still match the plan's
   `manifest_epoch_pre` and per-slab generation;
4. verifies rebuilt sidecar digests against cold
   rebuild;
5. **cold-rebuilds the visible-domain digest over the
   pre-commit and post-commit *manifest views
   localized to touched slabs* — `pre =
   digest.table.row.visible.v1(input_slab_visible_rows)`;
   `post = digest.table.row.visible.v1
   (output_slab_visible_rows)`; assert pre == post**;
6. samples per-version-entry digest verification at
   `lsm_digest_verify_sample_rate`;
7. proposes `CompactionCommit` to the locality
   control ring.

The visible-domain audit is O(input + output bytes),
not O(table). ADR 046's digest is monoidal, so
comparing the touched-slab contribution is equivalent
to comparing full table state with all untouched
slabs canceling out. Cold rebuild over views catches
both elision bugs (a visible row dropped) and
materialization bugs (a synthetic row in output) —
strictly stronger than streaming
`visible_contrib(elision_set) == identity`.

A mismatch aborts the commit, emits
`LsmInvariantViolation`, and pauses compaction on the
affected `(locality, table)` scope. Page-grade.

### 7. Reader pins (MVCC) and ref-counted retirement

Readers acquire a manifest snapshot + slab pins at
iterator open:

```rust
pub struct SlabPinTable { /* internal */ }

impl SlabPinTable {
    pub fn acquire(&self, slabs: &[SlabId])
        -> Result<SlabPinGuard, AcquireError>;
}
```

Bulk acquire is atomic. A slab in `DeletePending`
fails acquire; the caller retries against a fresh
manifest snapshot.

The pin lifetime is the iterator lifetime. A reader
pinned at manifest epoch E continues to read input
slabs even after a compaction commits epoch E+1.
Readers opened after the commit see output slabs.

CDC catchup acquires pins for the slabs covering
`[start_cursor, snapshot_hlc]` at subscription open;
pins drop at handoff to live tail. Live tail uses
ADR 054's in-memory ring (no slab pins).

Remote read RPC handlers hold pins for the duration
of the response stream.

Retirement lifecycle:

- on `CompactionCommit` apply, retired inputs
  transition `Live → Retired { successors }`. They
  remain readable through the manifest by pinned
  readers.
- `LsmGcWorker` sweep (tick `lsm_gc_tick_millis`):
  - finds `Retired` slabs with `pin_count == 0`;
  - proposes a manifest mutation transitioning to
    `DeletePending`;
  - finds `DeletePending` slabs;
  - deletes slab and sidecar bytes through existing
    storage delete paths;
  - after deletion succeeds, removes manifest rows.

A failed delete leaves the slab `DeletePending` for
retry; not exposed to new readers.

**No LSM-introduced `relocated_reads` shim.** The
existing `relocated_reads` machinery in
`src/storage/manager.rs` (a
`DashMap<RecordLocation, RecordLocation>` populated by
retention rewrites and dereferenced at read time)
remains in place and untouched by ADR 055. ADR 055
specifically declines to *add* LSM-driven relocation
rows: pins make old and new manifest epochs
independently readable, and sidecars rebuild during
compaction (ADR 052 §9) so cached `RecordLocation`
references in old sidecars retire with the old
sidecars. Other cached-reference populations (query
plans, RPC response streams, catalog lookups) either
acquire pins through the new manifest-snapshot path
or fall inside an existing non-LSM relocation domain;
the impl plan's Phase 0 cached-reference audit
classifies every such site before Phase 1 begins.

### 8. CDC interaction

CDC reachability is bounded by retention floor (ADR
054 §5, §6). Compaction physically realizes that
boundary.

CDC catchup acquires manifest-snapshot pins at open;
those pins survive concurrent compaction commits.
Live-tail handoff (ADR 054 §6) reads the in-memory
ring and does not pin slabs.

A subscription whose `cursor_high_water` falls below
retention floor rejects with the existing
`CDC_REJECT_BELOW_RETENTION_FLOOR` (ADR 054). LSM
does not introduce new reject codes for CDC.

**No new wire surface. No `safe_emission_hlc`
interaction.** That watermark gates *emission* from
the in-memory live-tail ring (ADR 054 §6), upstream
of any compaction decision. By the time a version-
entry reaches a slab eligible for compaction, it's
been finalized, emitted (or skipped past) the live-
tail ring, and the ring entry has dropped.

The roadmap's framing of "compaction must not advance
below `cursor_high_water` when sidecars are being
rewritten in-place" is satisfied by **not rewriting
sidecars in place** — replace-by-new ensures every
reader observes a complete sidecar at all times. The
"ring overflow rejects cleanly" branch is the
existing `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`
(ADR 054); LSM does not mask it.

A slow or disconnected subscription does not hold a
compaction lease after its active read ends. Pins
are bounded by *active iteration*, not subscription
identity.

### 9. Observability

Metrics under `lsm_*`:

```text
lsm_compactions_total{level,result}
lsm_compaction_input_slabs{level}
lsm_compaction_output_bytes{level}
lsm_compaction_duration_millis{level}
lsm_level_slab_count{level}
lsm_level_total_bytes{level}
lsm_pending_delete_slabs{}
lsm_retired_slab_pinned_bytes{}
lsm_digest_invariance_violations_total{kind}
```

Pinned label enumerations:

```text
level  = 0 | 1 | 2 | 3 for level gauges
level  = 0 | 1 | 2 for compaction source-level metrics
result = ok | manifest_race | sidecar_digest_mismatch
       | visible_digest_invariance | aborted
kind   = visible_domain | version_entry
```

No tenant / table / subscription / peer / schema /
slab_id / error-string labels.

Audit events (`src/audit/events.rs`):

- `LsmCompactionCommitted { locality_id,
  manifest_epoch_pre, manifest_epoch_post, level_from,
  input_slab_count, output_slab_count, bytes_in,
  bytes_out, elided_versions, dominant_reason }`
- `LsmInvariantViolation { locality_id, kind,
  manifest_epoch }`
- `LsmCompactionPaused { locality_id, reason }`

### 10. Reject taxonomy

```rust
#[repr(u32)]
pub enum LsmRejectCode {
    ManifestRace            = 1,
    SidecarDigestMismatch   = 2,
    VisibleDigestInvariance = 3,
}
```

Closed V1 taxonomy. Discriminants start at 1 so a
zero-init reject doesn't decode as valid. New codes
require a follow-up ADR.

### 11. Scheduling and operator surface

Compaction is a bounded background worker:

- `lsm_compaction_concurrency_per_locality` concurrent
  jobs per locality leader (V1 default 1);
- one job per output level at a time;
- worker tick is `lsm_picker_tick_millis`; on
  `ManifestRace`, worker re-plans on next tick.

**Pause/resume via node-local maintenance config**, not
a new wire protocol. Operators express "pause
compaction on node N for one hour" through the
existing maintenance configuration surface.

Tier sweeper coordination:

- compaction can select sealed slabs as inputs;
- tier sweeper does not upload a slab held by a
  compaction write-lease;
- compaction outputs return as ordinary sealed slabs;
  tier sweeper may move them later under existing
  policy.

Admission does not block on compaction. Query reads
do not block except briefly during the local index
batch commit applying the manifest mutation.

### 12. Failure model

Idempotent and restart-safe:

- temp output files use a `compaction-temp-<job_id>-`
  prefix; startup deletes any not referenced by a
  finalized manifest epoch;
- manifest mirrors are rebuildable from finalized
  control-ring mutations + durable slab bytes;
- partial sidecar files are ignored unless the
  manifest points at them and their digest verifies;
- input slabs are never deleted before replacement
  state is visible;
- a failed compaction job leaves inputs `Live`;
- a manifest race rejects the commit; the picker
  re-plans from the next manifest snapshot.

### 13. Architectural lockdown

`tests/architecture_test.rs` enforces:

1. exactly one `CompactionLevelIndex` newtype-unit definition;
2. exactly one `CompactionJobId`, `ManifestEpoch`
   newtype-unit definitions;
3. `CompactionLevelIndex` accepts only `0..=3`;
4. exactly one `CompactionMergeKey` definition; not
   imported by any CDC module;
5. `lsm_*` metric registry contains exactly the
   bounded label enumerations above;
6. no new `MsgType` containing `LSM` or `COMPACTION`;
7. no new `FRAME_FLAG_*`;
8. no new `NodeCapability` bit;
9. no `ObjectStore` trait method addition;
10. no table descriptor field for compaction level /
    manifest;
11. no persisted visible `StateDigest` field on slab
    footer or run manifest;
12. no CDC subscription holds a compaction-blocking
    lease / pin that outlives its iterator;
13. secondary-index sidecar compaction path writes
    new sidecar files; no in-place rewrite helper
    exists;
14. no `Wendelon`-prefixed LSM identifiers;
15. no `Lsm*V[0-9]` / `Compaction*V[0-9]` type
    suffixes;
16. the elision floor in `LsmCompactionWorker` reads
    only from `TableLifecycleCatalog::retention_floor(...)`;
17. visible-domain invariance audit is unconditional
    (no sampling threshold);
18. `LsmRejectCode` taxonomy is closed to the three
    V1 codes;
19. `LsmConfig` defaults match V1 commitments
    verbatim;
20. no LSM code path writes into `relocated_reads`
    and no LSM type embeds a relocation table;
    existing non-LSM relocation machinery in
    `src/storage/manager.rs` (retention rewrites,
    sweeper paths) remains in place — guard is
    scoped to LSM-introduced code only;
21. no `DigestEvictionMarker` or compaction-driven
    digest mutation on the control ring (only
    `CompactionCommit` carries digest summary, not
    digest mutation);
22. no `StorageTier` field on LSM run summaries.

## Rationale

**Why control-ring atomic install.** Compaction is
local physical work, but readers within the locality
need an atomic epoch boundary AND followers need
synchronized manifests for failover. ADR 016's
existing finalization discipline gives both without
creating new protocol surface.

**Why deterministic compaction.** Same inputs + same
elision floor + same merge ordering = same logical
output. Each replica reproduces the output locally
without leader-shipped bytes. Replication traffic
stays bounded to manifest mutations (one entry per
commit).

**Why leveled, not tiered.** Leveled compaction caps
space amplification at `1 + 1/(size_ratio - 1)` ≈
1.11 for size_ratio = 10. Read paths (PK lookup via
sidecars; HLC scans; CDC catchup) all benefit from
low slab count per level.

**Why HLC-disjoint partitioning at L1+, not RowKeyId-
disjoint.** ADR 045 / 050 / 052 all index slabs by
`(HLC, RowKeyId)` with HLC primary. The seek key for
HLC-range scans is HLC; partitioning on HLC keeps the
binary-search-on-manifest cost constant per scan. PK
access goes through sidecars, independent of slab
partitioning.

**Why retention floor IS the elision floor.**
§"Architectural inextricability" gives the structural
argument. Pragmatic: ADR 048 already promised
reachability up to the floor; ADR 054 already wired
CDC reachability to the floor. A second floor would
diverge under operational drift.

**Why MVCC reader pins, not `relocated_reads`.** Pins
make old and new manifest epochs independently
readable. They avoid per-row relocation state,
preserve CDC catchup during concurrent compaction
without coupling it to the retirement schedule, and
keep deletion bounded by *active readers* rather than
by dormant subscriptions or cached references that
may never be dereferenced. Sidecars are rebuilt
during compaction, so cached `RecordLocation`
references in old sidecars retire with the old
sidecars; they don't need cross-epoch redirection.

**Why no CDC `safe_emission_hlc` coupling.** The
roadmap quote frames this as a possible interaction.
On close inspection, it isn't: `safe_emission_hlc`
gates emission from the in-memory live-tail ring (ADR
054 §6), which is upstream of any compaction
decision. By the time a version-entry reaches a slab
eligible for compaction, it's already past the live-
tail ring.

**Why localized cold-rebuild visible-domain audit.**
Cold rebuild over input/output slab views catches
both elision bugs (drops a visible row) and
materialization bugs (synthesizes a visible row);
streaming `visible_contrib(elision_set) == identity`
only catches elision. The cost is bounded by touched
slab bytes (O(input + output)), not table size,
because the digest is monoidal.

**Why keep slabs as the run format.** The repo
already has slab encryption, CSB, secondary-index
sidecar, tiering, and recovery machinery. A separate
SSTable format would duplicate those surfaces.

**Why active slab is never a compaction input.**
Cheaper to seal-then-compact than to coordinate
compaction with admission writes.

**Why pause/resume via node-local config.**
Operational toggle, not a protocol decision.

## Consequences

- Space amplification bounded by leveled targets
  (≈ 1.11× at steady state with size_ratio = 10).
- Retention and tombstone physical cleanup get a
  predictable cadence.
- HLC range-scan read amplification falls as L0 debt
  drains.
- Secondary-index stale candidates decay with
  compaction.
- Compaction CPU and write bandwidth become explicit
  background consumers.
- Replica physical layouts may diverge by byte
  address; logical digests remain the comparison
  surface.
- CDC consumers remain responsible for staying inside
  retention.
- Long-running readers can pin retired slabs.
  `lsm_retired_slab_pinned_bytes{}` provides operator
  visibility.
- Compaction-time elision changes per-version-entry
  digest (algebraic, verified). Visible-state digest
  remains invariant.
- Append-only tables see no behavior change for
  visible state. Pure slab-count compression.
- `ManifestRace` aborts add re-plan overhead;
  bounded by picker tick.

## Rejected alternatives

**Purely local manifest with no control-ring
carrier.** Rejected. Followers would lose manifest
state on leader failover; recovery would require a
full local rebuild from slab bytes. Control-ring
carrier is bounded (one entry per commit) and gives
synchronized failover.

**Replicated logical compaction event log separate
from the control ring.** Rejected. The
`CompactionCommit` mutation IS the log entry; a
separate event family duplicates the control-ring
discipline.

**Adding LSM-driven `relocated_reads` rows for stale
`RecordLocation` references.** Rejected. (The
existing `relocated_reads` machinery for retention
rewrites in `src/storage/manager.rs` is unchanged by
ADR 055 — this rejection is specifically about adding
*new* LSM-driven relocation rows.) Pins and manifest
snapshots make old and new epochs independently
readable. Sidecars are rebuilt during compaction
(replace-by-new), so old sidecars retire intact with
their stale references. New per-row LSM relocation
rows would add chain-management complexity without
improving CDC catchup or iterator safety. The Phase 0
cached-reference audit verifies every existing
`RecordLocation`-caching site is covered by either
the new pin discipline, the existing non-LSM
relocation machinery, or sidecar replace-by-new
retirement.

**In-place sidecar rewrite.** Rejected. Race-prone
for readers and CDC catchup.

**CDC cursor pinning / "compaction yields to active
catchup".** Rejected. A stuck or dormant consumer
would freeze physical lifecycle cluster-wide.
Reachability remains retention-bound; pinning shifts
the failure mode from "consumer falls behind" to
"compactor falls behind." The latter is the worse
failure: it can't be recovered by reconnecting a
single consumer.

**`DigestEvictionMarker` on the control ring.**
Rejected. Mixes layers. Visible-state digest update
is owned by retention floor advance (ADR 048), not
by physical compaction.

**Drop all superseded PK versions during compaction.**
Rejected. Breaks time-travel above retention floor.
The boundary-sentinel rule (ADR 050 §7) preserves
the newest below-floor version.

**New SSTable format.** Rejected. Slabs remain the
run format.

**Persist visible `StateDigest` in slab footers.**
Rejected. Anti-entropy territory.

**Single global compaction floor parameter independent
of retention.** Rejected. Two reachability contracts
diverge.

**Streaming `visible_contrib(elision_set) ==
identity` audit.** Rejected as default gate. Catches
elision bugs but trusts the survivor stream and
misses output materialization bugs. Cold rebuild over
input/output views is strictly stronger at equivalent
O(touched bytes) cost. Streaming check remains a
cheap pre-filter useful in benches / debug.

**Seven levels (`L0..L6`) for headroom.** Rejected
for V1. L3 with size_ratio = 10 yields ≤ 25.6 GiB
per locality at default settings; sufficient for ADR
030 deployment envelope. L4+ extension is a follow-
up ADR if bench reveals need.

**Multi-segment merge key with `routing_locality ||
tenant_id || table_object_id` prefix.** Rejected.
Compaction is per-locality (no need to discriminate
locality). `table_object_id` is read from row-axis
synthetic columns at retention-floor lookup, not
from the merge key. Narrow merge key matches the
data-ring slab seek key exactly.

**`StorageTier` on the LSM substrate.** Rejected.
Existing sweeper handles tier moves orthogonally.
Coupling them would force the substrate to coordinate
with the sweeper without buying capability.

## Open questions

1. **Direct compaction of already-tiered objects.**
   Council leans yes when the existing tiered read
   path can fetch complete plaintext; output goes
   sealed (Local), sweeper retiers. Final decision in
   impl plan's L2 input phase.

2. **Visible-domain audit cost in production.** Cold
   rebuild over input/output views is bounded by
   touched bytes, but high compaction rates may push
   the budget. Bench measures; if prohibitive, a
   sampled fallback with high-priority alarm on
   sampling miss is a follow-up — V1 commits to
   default-on.

3. **L3 retention.** L3 has no "next" level; slabs
   stay there until retention deletes them whole.
   For very long retention, L3 may grow large enough
   that a future ADR introduces L4+. V1 defers.

4. **Manifest history compaction.** How large can
   manifest history grow before periodic checkpoint
   compaction? Defer to operational evidence.

5. **Picker tick cadence vs admission rate.** 100 ms
   tick may be too aggressive on quiet localities and
   too coarse on bursty admissions. Admission-side
   nudge when L0 crosses threshold is a latency
   optimization. Defer to bench.

6. **Per-locality concurrency tuning.** V1 = 1;
   should L0→L1 (small inputs) and L_n→L_{n+1}
   (large) run concurrently within a locality?
   Defer to bench.

## Forward-compatibility analysis

Each follow-up ADR consumes the substrate without
changing its shape:

- **Size-tiered compaction picker** plugs in
  alongside the leveled picker.
- **Subcompaction** parallelizes the worker's merge
  step.
- **Tiered storage moves** continue to ride the
  existing sweeper.
- **Bloom filters per slab** add a sidecar-equivalent
  artifact.
- **Cross-replica compaction deduplication** lifts
  the per-replica autonomy assumption.
- **L4+ extension** raises `CompactionLevelIndex` range and
  level-count config.
- **Vector embeddings (roadmap #13)** compose as
  another sidecar family.

## Deferred to follow-up ADRs

- Size-tiered compaction picker.
- Subcompaction.
- Tiered storage tier moves (separate sweeper).
- Bloom filters per slab.
- Cross-replica compaction deduplication.
- Manual compaction trigger RPC.
- Compaction throttling / IO QoS.
- Hot-key compaction prioritization.
- L4+ extension for long-retention workloads.
- Per-row audit grain at compaction-time elision.
- Rolling sidecar rebuild without full compaction.
- Compaction-time schema-evolution transforms.
- Durable visible-`StateDigest` placement (anti-
  entropy).
- Direct compaction of already-tiered objects without
  local rehydration.
- Manifest history checkpoint compaction.
- Periodic background cold-rebuild visible-domain
  audit.

## References

- `plans/adr/003_stellarator_bifurcated_writes.md` —
  finalization is the publish gate.
- `plans/adr/016_durability_model_fsync_group_commit.md`
  — control ring; locality-keyed manifest mutation
  carrier.
- `plans/adr/029_identity_vs_locality.md` —
  compaction is locality-axis.
- `plans/adr/030_*.md` — deployment-profile-tunables.
- `plans/adr/039_*.md` — column statistics blocks
  (rebuilt, not merged).
- `plans/adr/042_analytical_multi_row_fragments.md` —
  multi-row record helpers.
- `plans/adr/045_pk_less_append_only_tables.md` —
  append-only is LSM-clean.
- `plans/adr/046_state_digest_substrate.md` —
  visible-domain digest invariance.
- `plans/adr/047_row_tombstones_append_only.md` —
  tombstone pair-GC.
- `plans/adr/048_per_table_retention_policy.md` —
  retention floor IS the elision floor.
- `plans/adr/049_content_hash_row_identity.md` —
  pair-GC for content-hash duplicates.
- `plans/adr/050_pk_delete_supersession.md` §7 —
  supersession with boundary sentinel.
- `plans/adr/051_descriptor_fingerprint_split.md` —
  descriptor fingerprint discipline.
- `plans/adr/052_secondary_indexes.md` §7, §9, §10 —
  sidecar rebuild, race-lease, two-phase drop.
- `plans/adr/053_wire_arrow_ipc_body_compression.md`
  — unchanged path-label enumeration.
- `plans/adr/054_cdc_subscription_substrate.md` —
  reachability is the retention floor.
- `plans/conventions/newtype-units.md`,
  `plans/conventions/no-version-suffixes.md`,
  `plans/conventions/no-premature-backward-compat.md`,
  `plans/conventions/no-project-name-in-protocols.md`.
