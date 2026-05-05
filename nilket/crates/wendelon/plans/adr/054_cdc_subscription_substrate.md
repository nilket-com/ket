# ADR 054: CDC Subscription Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion implementation plan:
`plans/impl/054_cdc_subscription_substrate.md`

## Status

Accepted

V1 substrate commitments (council-confirmed):

- substrate-only scope; triggers, PBC filter pushdown,
  webhooks, and continuous queries are follow-up ADRs;
- identity-axis logical cursor
  `(row_hlc: Hlc, row_key_id: RowKeyId, record_row_index: RecordRowIndex)`
  — the cursor matches the data-ring slab seek key
  (zero-write-amplification mandates this — see § "Architectural
  inextricability");
- version-entry event grain; no `CdcEventKind` /
  `CdcOperation` enum at substrate;
- in-memory per-locality `CdcLiveTailRing` with
  admission-time staging gated by ADR 003
  `FinalizationCommitted`;
- global `CdcCursor` lexicographic order on every
  subscription stream, enforced by a safe-emission watermark
  `safe_emission_hlc = min(locality_finalized_hlc(l))` that
  holds finalized live-tail entries until every source
  locality has finalized through their HLC;
- cold-locality heartbeat contract: locality-scoped (not
  per-table) — every locality MUST advance its
  `locality_finalized_hlc` at a bounded cadence
  (`cdc_locality_heartbeat_interval_millis`, V1 default
  1000 ms) via a no-op finalization marker on the control
  ring (one worker per locality, one marker per quiet
  interval, regardless of how many tables map to that
  locality), so the watermark cannot stall behind an idle
  locality. Cluster cost: `N_localities /
  heartbeat_interval`;
- catchup uses full `CdcCursor` tuple bounds (not HLC
  ranges) over a safe global cut
  `snapshot_hlc = min(locality_finalized_hlc(l))` at
  subscription open; catchup via existing source-storage
  scan paths; no durable CDC event log;
- open-time ring-continuity guard: every non-empty source
  locality ring satisfies
  `ring.head_cursor[l] <= (snapshot_hlc, RowKeyId::MAX,
  RecordRowIndex::MAX)`; violation rejects open with
  `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`;
- visible-domain `StateDigest` checkpoints
  (`digest.table.row.visible.v1`); no new digest domain;
- resume-time digest validation via
  `CdcSubscribeRequestMeta::resume_visible_state_digest` +
  `resume_digest_present` (mismatch rejects at open with
  `CDC_REJECT_CHECKPOINT_DIGEST_MISMATCH`);
- four `MsgType` constants
  (`CDC_SUBSCRIBE_REQ` / `CDC_FRAME` / `CDC_CHECKPOINT` /
  `CDC_REJECT`);
- 8-code `CDC_REJECT_*` taxonomy;
- bounded metric labels (`source` / `result` / `code`);
- pinned V1 defaults: `max_cdc_live_tail_bytes_per_locality =
  64 MiB`; checkpoint cadence `max(1024 events, 1s wall time)`;
- `Live` and `SnapshotAtOpen` subscription bounds; cross-replica
  reconnect is a fresh subscription;
- `CDC_FRAME` meta carries both `frame_low_cursor` and
  `frame_high_cursor`.

## Context

Brainstorm 004 groups four real-time-eventing ideas:

- zero-polling CDC over QUIC;
- pushdown triggers via PBC (compiled from Rune above the
  Wendelon storage boundary);
- continuous queries (historical scan + live tail blended on
  one stream);
- async webhook queues backed by a native append-only sink.

The roadmap calls the area "large, multi-ADR" and pins the
substrate ADR's load-bearing decisions:

> "Append-only is the canonical CDC sink. Cursor surface
> decision lands here (HLC-derived? `__row_key_id__`-derived?
> tuple?); consumers verify cursor checkpoints against
> partition `StateDigest` points algebraically."

The council has converged on these substrate decisions:

1. **Cursor is identity-axis (logical).** A Raft-log-index
   cursor leaks locality / control-log state and re-anchors on
   topology changes. The natural cursor is the same 3-tuple
   Wendelon storage already uses to name a version-entry:
   `(row_hlc, row_key_id, record_row_index)`. The cursor shape
   and the no-durable-log decision are physically inextricable
   — see §"Architectural inextricability" below.
2. **Event grain is the storage version-entry.** No
   server-synthesized `CdcEventKind` enum. Consumers derive
   insert / update / delete / resurrection semantics from
   `__row_key_id__` / `__row_hlc__` / `__row_tombstone__` and
   their own materialized state. This matches ADR 052's
   `digest.secondary_index.entry.v1` discipline and avoids
   server-side per-row visible-state tracking.
3. **Live-tail uses an in-memory ring with no durable CDC log.**
   Wendelon's storage already retains version-entries; CDC
   reads them via existing scan paths for catchup and via an
   in-memory ring for the live frontier. Zero CDC-side write
   amplification.
4. **Live-tail entries stage at admission and release at
   finalization.** Per ADR 003, the bifurcated write path
   produces `DataBranchPending` / `DurableQuorumReached` /
   `FinalizationCommitted` outcomes. Pre-finalization states
   are reversible; CDC subscribers see staged entries only
   after `FinalizationMarker` commits on the control ring.
5. **Checkpoints reuse the existing visible-domain digest.**
   `digest.table.row.visible.v1` (ADR 046) is already
   computed; CDC checkpoints carry it at the cursor's HLC. No
   new digest domain in V1.
6. **Triggers, PBC filter pushdown, webhooks, and continuous
   historical-plus-live queries are follow-up ADRs.** Each has
   its own contract surface (admission-time CPU budget for
   sync triggers; trigger catalog + sink validation for
   post-commit triggers; PBC validator integration plus
   watermark frame plus filter-budget plus
   no-match-checkpoint-advancement for filter pushdown;
   HTTP retry / DLQ for webhooks; cursor-overlap semantics
   for continuous queries). Bundling produces a
   four-ADR-in-one nobody can review carefully.

## Architectural inextricability

A subtle but load-bearing point: the cursor shape and the
no-durable-CDC-log decision are *physically inextricable*.
They are not two independent stylistic choices that happen to
agree.

- **Wendelon's data-ring slabs are physically ordered by
  `(HLC, RowKeyId)` (per the storage shape that ADR 045 / ADR
  050 / ADR 052 all share).** The slab's seek / scan
  primitives index those fields directly.
- **Wendelon's data-ring slabs are *not* indexed by the R0
  control ring's `Epoch` or `LogIndex`.** No on-disk artifact
  pre-computes "given log-index X, here are the rows finalized
  in that admission."
- **A control-log-finality cursor `(Epoch, LogIndex,
  EventOrdinal)` would force a full source-table scan on every
  reconnect** to find which rows correspond to a given
  `log_index`. Catchup throughput would collapse to
  table-scan throughput, defeating the substrate's purpose.

The two design choices flip together:

| Choice                       | Implication                                                                        |
|------------------------------|------------------------------------------------------------------------------------|
| Durable CDC event log        | Owns its own physical layout; can index by `(Epoch, LogIndex)` natively.           |
| No durable CDC event log     | Replay reads source slabs; cursor *must* match slab seek key `(HLC, RowKeyId, …)`. |

V1 ships zero-write-amplification, which mandates the logical
cursor. A future ADR that argues for a durable CDC log would
also have to argue for a different cursor shape (or accept the
table-scan-on-reconnect cost). The two pieces move together.

The substrate's accepted operational trade-off is therefore
explicit: **CDC reachability is bounded by source-table
retention.** Consumers that fall behind retention must
re-bootstrap. A future ADR introducing a durable CDC log can
extend the reachability window, but it would be a coupled
shape change — durable log + log-index cursor (or a multi-
cursor surface where the substrate cursor remains logical and
the log adds a finality-axis cursor for log-internal
seeking). V1 declines the bundled shape change.

Three further facts shape the design:

- **ADR 053 settled wire-side compression.** CDC frames are
  wire-side fresh encoders; they reuse
  `wire_encode_batch_for_path()` and register a new bounded
  `path` label value (`cdc_event`). No CDC-specific codec.
- **ADR 050 §6 already classifies admission outcomes.**
  `InsertedNewVisible` / `UpsertedAlreadyVisible` /
  `DeletedSupersedingVisible` / `DeleteAbsorbedNotVisible` /
  `ContentHashCollapsed`. The admission path's existing
  classification is enough — substrate code checks it to
  decide whether to stage an entry, but does not surface a
  derived semantic kind on the wire.
- **ADR 048 retention is authoritative on reachability.** CDC
  reads version-entries from existing storage; retained
  storage is the replay authority. A cursor below
  `TableLifecycleCatalog`'s floor rejects with a typed code.

The constraints from ADR 003 (bifurcated writes), ADR 004 / 010
(zero-copy POD metadata, Arrow IPC), ADR 011 (QUIC-first),
ADR 016 (R0 control ring; finalization gating), ADR 020 / 025
(envelope encryption / audit), ADR 029 (Identity vs Locality),
ADR 037 (additive evolution only), ADR 042 (multi-row records),
ADR 045 / 049 / 050 (row identity policies), ADR 046
(`StateDigest` substrate), ADR 047 / 048 (lifecycle), ADR 051
(logical / physical fingerprint split), ADR 052 (secondary
indexes), and ADR 053 (wire compression) all hold.

## Identity-vs-Locality framing

Per `plans/philosophy.md`, every record has an Identity (what it
IS) and a Locality (where it LIVES). CDC is **identity-axis**:

- Subscriptions name `(TenantId, TableObjectId)`. Identity.
  Not "subscribe to locality 0xABCD" or "subscribe to Raft log
  position N at leader X."
- The cursor is `(Hlc, RowKeyId, RecordRowIndex)` — three
  identity-bound newtype-units. Topology changes (locality
  splits, rebalancing, leader transfer) are invisible to the
  cursor. Slab compaction may rewrite physical placement; the
  cursor still names the same logical version-entry.
- Event payloads are Arrow `RecordBatch` values whose rows are
  the version-entries themselves. Row identity is
  `__row_key_id__`; row HLC is `__row_hlc__`; row tombstone
  state is `__row_tombstone__`. Locality is a server-internal
  fan-out detail.
- Raft finalization remains load-bearing as a **release gate**,
  not as a CDC source-of-truth. ADR 003 keeps row payloads off
  the Raft log; the control log carries `FinalizationMarker`
  records. Tailing the Raft WAL for CDC would either reverse
  ADR 003 (push payloads into Raft) or surface non-payload
  finalization records to consumers. The substrate reads
  staged version-entries off the data path and uses
  finalization commit as the publish gate.
- The Unified Ring rule applies — there is no "control plane
  CDC" versus "data plane CDC." There is one CDC RPC family on
  the ring; metadata events (catalog mutations) are
  out of scope and may or may not eventually share the same
  wire surface in a follow-up ADR.

## V1 scope

ADR 054 ships **only** the CDC subscription substrate:

1. row-level CDC event streams with a stable identity-axis
   cursor;
2. an in-memory live-tail ring per source locality, gated by
   `FinalizationCommitted`;
3. catchup via existing storage-scan paths over retained source
   storage (no separate CDC log);
4. visible-domain `StateDigest` checkpoints
   (`digest.table.row.visible.v1`, ADR 046 V1 helper);
5. `Live` and `SnapshotAtOpen` subscription bounds.

It deliberately does **not** ship:

- PBC filter pushdown on subscriptions (continuous queries);
- post-commit or synchronous PBC triggers / `TriggerCatalog`;
- webhook workers / `__webhook_events__` system table /
  external HTTP retry / DLQ;
- continuous queries that blend historical L2 scan results and
  live tail on one stream;
- direct Rune / SQL / Wasm execution at the storage boundary;
- exactly-once delivery to external consumers;
- multi-table or tenant-firehose subscriptions;
- before-image CDC payloads;
- a CDC-specific durable event log;
- a CDC-specific `StateDigest` domain;
- column projection on subscriptions;
- cross-replica cursor handoff;
- consumer-driven checkpoint cadence;
- per-event audit;
- metadata / catalog CDC.

Each deferral has its own follow-up ADR slot; the forward-
compatibility analysis below sketches the boundaries.

## Non-goals (V1)

- **No PBC filter pushdown.** Follow-up ADR; substrate carries
  no `pbc_filter` field on subscribe meta.
- **No triggers.** Follow-up ADR; substrate carries no
  `TriggerCatalog` / `SchemaObjectKind::Trigger`.
- **No webhook worker or `__webhook_events__` table.**
- **No continuous historical-plus-live queries.**
- **No durable CDC event log.** Replay reads retained source
  storage.
- **No new `StateDigest` domain.** Reuse
  `digest.table.row.visible.v1`.
- **No `CdcEventKind` / `CdcOperation` enum at substrate.**
  Consumers derive semantic kind from row-axis columns.
- **No before-image CDC payloads.** Consumers needing
  historical values use time-travel reads while retention
  permits.
- **No exactly-once delivery.** V1 is at-least-once; consumers
  dedupe on the cursor.
- **No multi-table / tenant-firehose subscriptions.**
- **No schema-evolution transforms across non-additive
  fingerprint changes.** ADR 037 forbids non-additive
  evolution today; per-frame fingerprint binding handles
  additive evolution through consumer re-fetch.
- **No new node capability bit.**
- **No HRFT envelope `format_version` bump.**
- **No new `FrameHeader.flags` bit.** Per ADR 053 the
  freed bit-0 / bit-1 reservations stay freed.
- **No new `FRAME_FLAG_*` constant for CDC stream lifecycle.**
  `FRAME_FLAG_STREAM_END` plus typed `MsgType` carry the
  lifecycle.
- **No new wire compression codec.** Inherits ADR 053
  `LZ4_FRAME` per-buffer.
- **No new storage compression codec.**
- **No `ObjectStore` trait change.**
- **No PBC / AGG protocol changes.**
- **No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes.**
- **No project-name prefix on new symbols.** `cdc_*` metrics,
  `CDC_REJECT_*` codes; no brand prefix.
- **No new Cargo feature or dependency.**
- **No on-disk format change.** No descriptor field on
  `TableSchemaDescriptorV1`; no slab-footer change; no new
  durable artifact family.

## Decision

### 1. Substrate scope only

V1 ships the four substrate properties listed in §"V1 scope"
and nothing else. Follow-up ADRs consume the substrate:

- A *PBC-filter-pushdown ADR* extends `CDC_SUBSCRIBE_REQ` with
  a trailing-section bytecode payload and adds a `CDC_WATERMARK`
  message type for low-selectivity filtered streams. The
  substrate cursor and event grain are unchanged.
- A *post-commit-trigger ADR* adds `TriggerCatalog` + new
  `SchemaObjectKind::Trigger` + a trigger worker that runs PBC
  over CDC events and appends to descriptor-bound append-only
  sinks. Substrate unchanged.
- A *synchronous-trigger ADR* adds an admission-time PBC hook
  with its own CPU budget and failure-domain rules.
- A *webhook ADR* adds `__webhook_events__` (or equivalent)
  plus an internal HTTP worker.
- A *continuous-query ADR* either (a) composes externally via
  L2 read at HLC = T plus CDC subscription from cursor matching
  T, or (b) introduces a dedicated streaming `QUERY_REQ` /
  `QUERY_RESP` shape.

### 2. Cursor: `(row_hlc, row_key_id, record_row_index)`

V1 cursor:

```rust
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CdcCursor {
    pub row_hlc: Hlc,
    pub row_key_id: RowKeyId,
    pub record_row_index: RecordRowIndex,
}

impl CdcCursor {
    pub const MIN: Self = Self {
        row_hlc: Hlc::MIN,
        row_key_id: RowKeyId::MIN,
        record_row_index: RecordRowIndex::ZERO,
    };
}
```

Three newtype-units (`newtype-units.md` discipline):

- `Hlc` — the existing HLC newtype (matches the
  `__row_hlc__` synthetic column).
- `RowKeyId` — the existing 32-byte row-identity newtype
  (matches the `__row_key_id__` synthetic column; per
  philosophy.md, this carries the row's True Name —
  the BLAKE3 identity).
- `RecordRowIndex` — the per-multi-row-record row index
  newtype ADR 052 §6 widened to `u32`. Reusing it here
  resolves multi-row-record ambiguity within a single HLC
  tick (ADR 042 multi-row records share `__row_hlc__` and
  `__row_key_id__` — but each row has a distinct
  `__row_idx__`).

Ordering is lexicographic over `(row_hlc, row_key_id,
record_row_index)`. Resume requests carry an **exclusive**
floor: the first emitted event has cursor strictly greater
than `start_cursor`.

`CdcCursor::MIN` is "from the beginning of retained history."
The engine reads from the source-table retention floor forward
when given `MIN`; consumers asking for prehistory get the
floor's visibility, not an error.

**Why this shape, not alternatives.** §"Architectural
inextricability" above explains why the cursor must align with
the data-ring slab seek key. The shape choices on top of that:

- **HLC-only.** Ambiguous when multiple events share one HLC
  tick (cross-locality concurrency, multi-row records).
- **`row_key_id`-only.** Loses version ordering; a single
  `RowKeyId` can carry many version-entries on PK / content-
  hash tables.
- **`(Epoch, LogIndex, CdcEventOrdinal)` (control-log
  finality).** Stable for finalization order, but data-ring
  slabs are not indexed by `(Epoch, LogIndex)`. Resume would
  force full table scans (see "Architectural
  inextricability"). The engine carries finalization
  log-index internally for diagnostics, audit correlation, and
  pending-finalization cleanup; it is not part of the external
  CDC cursor.

### 3. Event grain: storage version-entry

Every CDC event is a retained version-entry:

- append-only data row → one event;
- append-only tombstone (ADR 047) → one event with
  `__row_tombstone__` set;
- PK insert / upsert (ADR 050 admission outcomes
  `InsertedNewVisible` / `UpsertedAlreadyVisible`) → one event
  under the same `__row_key_id__`;
- PK delete (ADR 050 admission outcome
  `DeletedSupersedingVisible` or `DeleteAbsorbedNotVisible`) →
  one tombstone event;
- PK resurrection (ADR 050 §7) → tombstone event followed by
  later data event under the same `__row_key_id__`;
- content-hash duplicate collapse (ADR 049 §5) → no event
  for the absorbed duplicate.

V1 carries **no `CdcEventKind` / `CdcOperation` enum at the
substrate**. Consumers derive insert / update / delete /
resurrection semantics from row-axis synthetic columns
(`__row_key_id__`, `__row_hlc__`, `__row_tombstone__`,
optional supersession columns) plus their own materialized
state. This matches ADR 052's version-entry digest discipline
and avoids server-side per-row visible-state tracking.

A future "consumer-friendly higher-level CDC" ADR may layer a
derived `CdcEventKind` view on top of the substrate stream;
the substrate itself stays storage-axis-aligned.

The wire payload is an Arrow `RecordBatch` containing:

- the table's user columns (per the declared
  `LogicalSchemaFingerprint`);
- row-axis synthetic columns: `__row_key_id__`,
  `__row_hlc__`, `__row_tombstone__`, `__row_idx__`,
  `__row_table_object_id__` (ADR 045 / 047 / 050);
- optionally — if the source slab carried supersession
  metadata (ADR 042 / ADR 050 §7) — a flattened
  `__supersedes_hlc__` synthetic column. Resurrection (ADR 050
  §7) is observable through this column.

Schema-fingerprint binding is per-frame (§9); the consumer
re-fetches the descriptor on logical fingerprint change.

### 4. Live-tail ring with admission-stage / finalization-release

The engine maintains a per-locality bounded **CdcLiveTailRing**.
Each entry carries:

```rust
pub struct CdcLiveEntry {
    pub cursor: CdcCursor,
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
    pub record_batch: Arc<RecordBatch>,
    pub finalized: bool,
}
```

Ring properties:

- **Bounded by bytes**, capped by
  `max_cdc_live_tail_bytes_per_locality`.
  V1 default: **64 MiB** (deployment-profile-tunable per
  ADR 030).
- **FIFO eviction** on overflow — oldest entries drop;
  monotonic `head_cursor` and `tail_cursor`.
- **Per-locality, multi-table** — each locality leader owns
  one ring shared across the tables it serves; the per-
  `(tenant, table)` fan-out registry (§5) filters entries at
  drain time and joins per-locality streams in cursor order
  with `RowKeyId` / `RecordRowIndex` tiebreakers. The
  multi-table sharing is a V1 simplification; if a hot table
  starves a quiet table sharing the same locality, a follow-up
  ADR may add per-`(locality, tenant)` or per-`(locality,
  table)` rings.

**Admission-stage / finalization-release.** Per ADR 003 §6,
the bifurcated write path produces explicit outcomes
(`DataBranchPending` / `DurableQuorumReached` /
`FinalizationCommitted` / `FinalizationRecoverablePending` /
`FinalizationTerminalFailure`). The CDC substrate composes
with this discipline:

1. **Stage at admission.** Every produced version-entry is
   pushed into the live-tail ring with `finalized = false`
   when the data-branch durable append succeeds. Stages are
   visible to the *engine* (for fan-out workers and pending-
   finalization recovery) but **invisible to subscribers**.
2. **Release at finalization.** A finalization observer marks
   the corresponding ring entry `finalized = true` when the
   `FinalizationMarker` commits on the control ring. Fan-out
   workers drain only finalized entries.
3. **Discard on failure.** A staged entry whose data-branch
   fails or whose `FinalizationRecoverablePending` →
   `FinalizationTerminalFailure` transition fires is evicted
   by pending-finalization cleanup. Subscribers never observe
   the entry.

Why stage-then-release rather than emit-after-finalization-
arrives: the staged-then-released path lets the materializer
do the expensive work (decode, rowification, fingerprint bind)
once at admission and flip a bit at finalization, minimizing
post-finalization latency. ADR 003's pending-finalization
cleanup is already a load-bearing path; CDC reuses it.

**Why per-locality ring (not per-table).** Each locality has
its own monotonic HLC. A per-locality ring is the simplest
buffer (single producer, multiple consumers); cross-locality
ordering happens in the fan-out worker. Per-table rings would
require coordination across locality leaders for every admit;
per-locality rings localize the contention.

**Why no durable CDC event log.** §"Architectural
inextricability" gives the structural argument
(zero-write-amplification mandates the logical cursor; durable
log would mandate a different shape). The operational
argument is symmetric: a parallel durable log adds write
amplification and a second retention surface without buying
replay properties source-table storage doesn't already offer.
The trade-off is explicit: CDC reachability is bounded by
source-table retention; consumers that fall behind retention
must re-bootstrap. A future ADR introducing a durable log
must bundle the cursor-shape change.

### 5. Subscription registry and fan-out

```rust
pub struct CdcSubscriptionRegistry {
    inner: DashMap<(TenantId, TableObjectId), Vec<SubscriptionHandle>>,
}

pub struct SubscriptionHandle {
    pub subscription_id: CdcSubscriptionId,
    pub cursor_high_water: AtomicCdcCursor,
    pub bound: CdcSubscribeBound,
    pub schema_fingerprint: LogicalSchemaFingerprint,
    pub send: mpsc::Sender<CdcLiveEntry>,
}
```

A per-subscription writer task drains the `mpsc::Receiver`,
encodes batches via ADR 053 `wire_encode_batch_for_path()`,
and emits `CDC_FRAME` over the QUIC stream. On
`mpsc::TrySendError::Full`, the admission path **does not
block** — the affected subscription detects the lag at the
next drain and falls back to catchup (§6).

`CdcSubscriptionId` is server-allocated `u128`, used for
log / metric correlation only. Not durable; not consumer-
resumable.

### 6. Catchup via storage scan

V1 commits to **global `CdcCursor` lexicographic order** on a
single subscription stream. Both catchup and live tail emit
events in non-decreasing cursor order. Three pieces compose
to make this true under independent per-locality
finalization:

1. **Safe global cut at subscription open.**

   ```text
   snapshot_hlc = min(locality_finalized_hlc(l))
                  for l in source_localities(table)
                  at subscription-open time
   ```

   `snapshot_hlc` is the highest HLC for which *every* source
   locality has already finalized through. By per-locality HLC
   monotonicity (ADR 016), no locality can later admit an
   event with `row_hlc <= snapshot_hlc` that isn't already in
   storage. Catchup over `(start_cursor, snapshot_hlc]` is
   therefore complete: no future finalization can produce an
   event below the catchup ceiling.

   `max(locality_finalized_hlc)` is the wrong cut for V1 — it
   would include rows in the catchup window that haven't been
   finalized on lagging localities yet, forcing catchup to
   wait or skip. `min(...)` is the safe cut.

2. **Catchup uses full `CdcCursor` tuple bounds, not HLC
   ranges.**

   ```text
   scan range per locality l:
       lower (exclusive) = start_cursor
       upper (inclusive) = (snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)
   ```

   HLC-only ranges are wrong: a `start_cursor = (T, k5, 0)`
   with HLC bound `> T` would miss the same-HLC suffix
   `(T, k7, 0)`, while `>= T` would replay the prefix
   `(T, k0..k5, 0)` already acked. The full lex-tuple bound
   `cursor > start_cursor` plus `cursor <= (snapshot_hlc,
   RowKeyId::MAX, RecordRowIndex::MAX)` is the right
   discipline — it matches the seek key the storage slabs
   are indexed on (§"Architectural inextricability"). The
   trigger condition for whether catchup is needed against
   locality `l`'s ring uses the same full-tuple comparison:
   `start_cursor < ring.head_cursor[l]`.

   Per-locality scans yield rows in their own
   `CdcCursor` order; a min-heap merger across localities
   emits the global lex-order. Catchup uses existing storage
   scan paths; no new storage code lands.

3. **Safe-emission watermark on the live tail.**

   ```text
   safe_emission_hlc(t) = min(locality_finalized_hlc(l))
                          for l in source_localities(table)
                          evaluated at every emit
   ```

   A finalized live-tail entry with
   `cursor.row_hlc > safe_emission_hlc(t)` waits in the ring
   even though `finalized = true`. Once the lagging locality
   advances and `safe_emission_hlc(t)` rises past the entry's
   HLC, the entry becomes eligible for emission. This is the
   load-bearing fix for the cross-locality ordering bug:
   without it, locality A finalized through 100 could emit
   `A@90` to subscribers before locality B finalizes `B@75`,
   producing the global sequence `A@90, B@75` that violates
   cursor monotonicity.

   With the watermark, `A@90` cannot be emitted until
   `safe_emission_hlc >= 90`, which requires every locality
   (including B) to finalize through HLC 90 first. Whatever
   `B@75`-class events B finalizes during that interval are
   committed and merged into the lex-ordered output ahead of
   `A@90`.

   The cost is bounded: live-tail visible latency is the
   slowest locality's finalization-marker cadence above the
   subscription's current cursor. ADR 003 finalization latency
   bounds it. The benefit is global cursor monotonicity
   without exposing `LocalityId` on the cursor.

**Catchup-to-live handoff at `snapshot_hlc`.** The handoff is
exact: catchup ends at `(snapshot_hlc, RowKeyId::MAX,
RecordRowIndex::MAX)`. The live-tail merger then emits the
first finalized entry whose cursor is strictly greater than
that and whose `row_hlc <= safe_emission_hlc(t)`. The
cross-locality min-heap merger preserves order across the
boundary because each locality is monotonic and the merger
always pulls the smallest available cursor that has cleared
the watermark.

**Open-time ring-continuity guard.** Catchup ends at the
ceiling and live tail picks up *strictly above* it; this only
works if the live-tail ring still contains every finalized
entry above the ceiling at open. The bounded ring may have
already evicted some of them — for example, when locality A
is finalized through HLC 1000 with `snapshot_hlc =
min(1000, 50) = 50` and the ring has already FIFO-evicted
A@90 because newer A entries pushed it out. Catchup will not
scan A@90 (it is above `snapshot_hlc`), and the ring no
longer holds it; the stream would silently miss A@90 unless
caught at open.

V1 ships an explicit open-time guard. For every source
locality `l` of the subscribed table, if the locality's ring
is non-empty, the engine asserts:

```text
ring.head_cursor[l] <= (snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)
```

`ring.head_cursor[l]` is the oldest entry currently in `l`'s
ring (FIFO front). The invariant the ring promises is "every
finalized entry on `l` with cursor `>= ring.head_cursor[l]`
is in the ring." If `ring.head_cursor[l] >
(snapshot_hlc, MAX, MAX)`, then entries between the catchup
ceiling and the ring head have been evicted, and the
substrate cannot fulfill the handoff.

Violation rejects at open with
`CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP` (same code as the
mid-catchup overflow path; the two are operationally the
same — ring is too small for current write rate vs catchup
ceiling). The runbook directs operators to either increase
`max_cdc_live_tail_bytes_per_locality` for hotter tables, or
have the consumer subscribe with a fresher cursor.

Empty rings on cold localities skip this check. Idle
localities still advance `locality_finalized_hlc` via the
heartbeat contract below; their empty rings cannot violate
continuity because no entries exist past the ceiling.

**Cold-locality heartbeat contract for `locality_finalized_hlc`.**
The watermark `safe_emission_hlc =
min(locality_finalized_hlc(l))` would stall on a cold
locality whose `locality_finalized_hlc` never advances —
events on every other locality would block forever. V1
commits to a **locality-wide heartbeat**, not a per-table
one. `locality_finalized_hlc(l)` is a single per-locality
value (ADR 016 control ring is locality-keyed, not
table-keyed); a heartbeat marker on locality `l` advances
that one value, which feeds the watermark for *every* table
whose source localities include `l`.

> **Every locality MUST advance its
> `locality_finalized_hlc` at a bounded cadence even when no
> row writes or finalization markers naturally fire, by
> emitting a no-op heartbeat finalization marker on the
> control ring (ADR 016). The heartbeat is locality-scoped,
> not (locality, table)-scoped: one worker per locality, one
> marker per quiet interval, regardless of how many tables
> map to that locality.**

V1 cadence default:
`cdc_locality_heartbeat_interval_millis = 1000`
(one heartbeat per locality per second; runbook tunable per
ADR 030 deployment profile). The heartbeat marker is a
single control-ring entry; it carries no row payload, no
table identity, and contributes no CDC event.

Cost analysis: control-ring write rate adds
`N_localities × 1 Hz` heartbeat markers cluster-wide
(*not* `N_cdc_eligible_tables × N_localities_per_table`).
Heartbeats piggyback whenever a locality's natural
finalization fires within the cadence window — a busy
locality emits zero extra heartbeats. The heartbeat worker
spawns on locality-leader startup for every locality
unconditionally; data presence is not a precondition.
Skipping the heartbeat on an "idle" locality would
reintroduce the watermark stall for any other table whose
source-locality set includes it.

The heartbeat is a CDC-substrate requirement on the control
ring's behavior; ADR 054 introduces no new wire surface for
it (existing finalization markers on the control log are
the carrier). The contract is what V1 commits to; the ADR
notes that future ADRs may relax cadence or extend the
mechanism (e.g., a per-table opt-out follow-up could
narrow heartbeats to localities serving CDC-subscribed
tables, but V1 declines that complexity).

**Skewed-locality regression test (Phase 5).** Two source
localities A and B; at subscription open A is finalized
through HLC 100, B through HLC 50. Open with
`start_cursor = MIN`. The test pre-populates storage with
events from both localities up to their open-time frontiers
and exercises post-open finalization. Specifically:

- pre-open storage: A@30, A@60, A@90 (A finalized through
  100); B@30 (B finalized through 50);
- post-open finalizations (in scenario order):
  B@75, B@90, A@110.

Assertions:

- `snapshot_hlc` at open = `min(100, 50) = 50`.
- Open-time ring-continuity guard passes (ring covers every
  entry above 50 on both localities; otherwise reject).
- Catchup emits exactly the events with cursor
  `<= (50, MAX, MAX)`: `A@30, B@30` in lex order
  (`A@30 < B@30` if `RowKeyId(A) < RowKeyId(B)`, otherwise
  swap; the lex order is determinate, the assertion uses
  the actual lex result).
- Right after open, `safe_emission_hlc = min(100, 50) = 50`.
  A@60 and A@90 are in the ring, finalized, but
  watermark-blocked (`60 > 50` and `90 > 50`).
- Scenario step 1 — B finalizes B@75. B's frontier advances
  from 50 to 75, so `safe_emission_hlc = min(100, 75) = 75`.
  Now eligible: A@60 (60 ≤ 75), B@75 (75 ≤ 75). Both
  emit, in lex order. A@90 is **still blocked** (90 > 75).
- Scenario step 2 — B finalizes B@90. B's frontier advances
  to 90, so `safe_emission_hlc = min(100, 90) = 90`. Now
  eligible: A@90 (90 ≤ 90), B@90 (90 ≤ 90). Both emit, in
  lex order.
- Scenario step 3 — A finalizes A@110. A's frontier
  advances to 110; B is still at 90, so
  `safe_emission_hlc = min(110, 90) = 90`. A@110 is blocked
  (110 > 90).
- Scenario step 4 — B's heartbeat (or B's next admission)
  advances B's frontier to ≥ 110. `safe_emission_hlc` ≥ 110.
  A@110 emits.
- The full emitted cursor sequence is non-decreasing in
  `CdcCursor` lex order; every event is delivered exactly
  once.

**Cold-locality watermark test (Phase 5).** Two source
localities A and B; A is busy, B is **completely cold** (no
admissions for the duration of the test). Subscribe with
`start_cursor = MIN`. Admit and finalize a sequence of A
events. Assertions:

- B's `locality_finalized_hlc` advances purely via the
  heartbeat at cadence
  `cdc_locality_heartbeat_interval_millis`.
- A's events emit with bounded delay equal to at most one
  heartbeat interval — without the heartbeat, A's events
  would stall forever.
- The watermark hold time per A event is `≤ 1 ×
  cdc_locality_heartbeat_interval_millis` (V1 default
  1000 ms). The bench (Phase 9 #10) reports this hold time.

**Ring-overflow during catchup.** Beyond the open-time guard
above, if any locality's `ring.head_cursor` advances past
`(snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)` while
catchup is *still running* (slow consumer, hot table), the
live-tail ring has dropped post-snapshot events catchup
expected to find on handoff. Two choices:

- **Re-anchor** to the new ring head and continue catchup past
  the original snapshot. V1 declines (operator-confusing
  semantics: snapshot moves under the consumer's feet).
- **Reject and reconnect.** V1 ships this path: emit
  `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`, close the QUIC
  stream. Consumer reconnects with the cursor it has reached.
  Operator runbook documents the recovery (increase
  `max_cdc_live_tail_bytes_per_locality`; reduce admission
  rate; advance consumer).

### 7. Wire surface

V1 adds four new `MsgType` constants:

```text
CDC_SUBSCRIBE_REQ        client → server: open subscription (one frame)
CDC_FRAME                server → client: batch of finalized events
CDC_CHECKPOINT           server → client: visible-state digest at cursor
CDC_REJECT               server → client: terminal typed reject
```

Stream lifecycle uses `FRAME_FLAG_STREAM_END` (existing) on a
final empty `CDC_FRAME` for graceful close. `CDC_REJECT` is
terminal but typed; consumers may reconnect after fixing the
condition.

Per-frame meta is bytemuck-castable. Sparse enums (`Live`,
`SnapshotAtOpen`, the eight reject codes, the two frame
sources) are **not** declared `Pod` directly — most bit
patterns of those enums are invalid, so deriving `Pod` would
be unsound. Instead, the POD meta structs carry raw integer
fields, and the engine decodes them through checked accessors
(`bytemuck::CheckedBitPattern` or hand-written
`TryFrom<uN>`):

```rust
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct CdcSubscribeRequestMeta {
    pub tenant_id: TenantId,
    pub table_object_id: TableObjectId,
    pub start_cursor: CdcCursor,
    pub bound_raw: u8,            // CdcSubscribeBound discriminant; checked on decode
    pub resume_digest_present: u8,
    pub _pad: [u8; 6],
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
    pub resume_visible_state_digest: StateDigest,
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CdcSubscribeBound {
    Live = 0,            // forever; close only via cancel or reject
    SnapshotAtOpen = 1,  // close after snapshot frontier with STREAM_END
}

impl CdcSubscribeBound {
    pub fn from_raw(raw: u8) -> Result<Self, CdcDecodeError> {
        match raw {
            0 => Ok(Self::Live),
            1 => Ok(Self::SnapshotAtOpen),
            _ => Err(CdcDecodeError::InvalidBound(raw)),
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct CdcFrameMeta {
    pub subscription_id: CdcSubscriptionId,
    pub frame_low_cursor: CdcCursor,
    pub frame_high_cursor: CdcCursor,
    pub logical_schema_fingerprint: LogicalSchemaFingerprint,
    pub event_count: RowCount,
    pub source_raw: u8,           // CdcFrameSource discriminant; checked on decode
    pub _pad: [u8; 7],
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CdcFrameSource {
    Ring = 0,
    Catchup = 1,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct CdcCheckpointMeta {
    pub subscription_id: CdcSubscriptionId,
    pub cursor: CdcCursor,
    pub visible_state_digest: StateDigest,
    pub digest_domain_raw: u32,   // DigestDomain discriminant; checked on decode
    pub _pad: [u8; 4],
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct CdcRejectMeta {
    pub subscription_id: CdcSubscriptionId,
    pub code_raw: u32,            // CdcRejectCode discriminant; checked on decode
    pub _pad: [u8; 4],
}

#[repr(u32)]
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CdcRejectCode {
    BelowRetentionFloor              = 1,
    RingOverflowDuringCatchup        = 2,
    SchemaFingerprintIncompatible    = 3,
    CheckpointDigestMismatch         = 4,
    UnknownTable                     = 5,
    TenantAuthDenied                 = 6,
    TableNotReplicatedHere           = 7,
    InternalEngineError              = 8,
}
```

Decoding is checked: a sender that emits an unknown
discriminant produces `CdcDecodeError`, which the receiver
turns into a stream-level reject (server side) or an error
return on `next_frame` (client side). This preserves
zero-copy POD framing on the wire while keeping the type
system sound. Reject-code discriminants start at 1 so that a
zero-initialized `CdcRejectMeta` cannot accidentally read as
a valid reject; valid bit patterns are enumerated, not
implied.

`CDC_FRAME` carries the meta plus an Arrow IPC payload.
`CDC_CHECKPOINT` and `CDC_REJECT` carry meta only — no Arrow
payload.

Arrow IPC encoding goes through ADR 053
`wire_encode_batch_for_path()` with new
`NetworkCompressionPath::CdcEvent`; `path` label in
`network_compression_*` metrics extends to the fifth value
`cdc_event` (architectural test pinned).

### 8. Retention bounds replay (ADR 048)

CDC reachability is bounded by the **same retention floor**
that bounds table reads. The engine consults
`TableLifecycleCatalog` (ADR 048) at subscription open and
during live tail:

- Modes `InheritCluster` / `Disabled` /
  `FixedWindow(RetentionWindowMillis)` apply unchanged.
- A cursor with `start_cursor.row_hlc < retention_floor`
  rejects `CDC_REJECT_BELOW_RETENTION_FLOOR` at open, with
  one exception: `start_cursor == CdcCursor::MIN` is the
  retained-history sentinel (§2) and clamps to the current
  retention floor instead of rejecting. The engine begins
  emission from the floor's earliest retained version-entry.
- During a live subscription, a retention-floor advance that
  passes the consumer's last-acknowledged cursor closes the
  subscription with the same reject code.

PK retention deferral (ADR 048's
`RETENTION_REJECTED_PK_DISALLOWED`, preserved by ADR 050)
inherits — PK tables stay on cluster-MVCC retention; CDC sees
whatever the cluster MVCC retains.

CDC introduces no new retention-policy field, no per-
subscription retention extension, and no "pin this cursor
against retention GC" primitive. A future ADR may add the
latter when an operational pattern demands.

### 9. Schema-fingerprint binding

`CDC_SUBSCRIBE_REQ` carries `logical_schema_fingerprint`. The
server validates against the source table's current
fingerprint at open; mismatch rejects
`CDC_REJECT_SCHEMA_FINGERPRINT_INCOMPATIBLE`.

Each `CDC_FRAME` carries `logical_schema_fingerprint` reflecting
the source table's fingerprint at the events in that frame.
On additive evolution (ADR 037), the fingerprint changes
mid-stream; consumers re-fetch the descriptor and re-bind.
Non-additive evolution is forbidden by ADR 037 today; if the
source table's fingerprint changes non-additively (which V1
expects to be unreachable in production), the server rejects
the next-frame encode and closes the stream with
`CDC_REJECT_SCHEMA_FINGERPRINT_INCOMPATIBLE`.

`PhysicalDescriptorFingerprint` is **not** carried — physical
retunes (compression policy, locality / layout) are
locality / placement changes and do not affect CDC semantics
(ADR 051 §"Consumer routing").

### 10. `StateDigest` checkpoints reuse the visible-domain digest

`CDC_CHECKPOINT` carries the table's visible-domain digest at
`cursor.row_hlc`:

```text
digest.table.row.visible.v1   (ADR 046 V1 helper)
```

Consumers checkpoint:

```text
(last_cursor, visible_state_digest_at_last_cursor_hlc)
```

**Resume-time digest validation.** `CdcSubscribeRequestMeta`
carries `resume_digest_present: u8` and
`resume_visible_state_digest: StateDigest`. When
`resume_digest_present != 0`, the server validates the
supplied digest against retained visible state at
`start_cursor.row_hlc` *before streaming any frames*. Mismatch
rejects at open with
`CDC_REJECT_CHECKPOINT_DIGEST_MISMATCH`; the consumer must
re-bootstrap from a fresh snapshot. This surfaces drift
immediately on reconnect rather than waiting for the next
in-stream checkpoint frame.

The same reject code fires if a per-stream checkpoint
verification ever fails server-side (e.g., on a future
durable-cursor-commit ADR's commit path); V1 only uses it on
subscribe.

The digest proves **visible table state at a cursor boundary**,
not membership of every emitted version-entry. Two consequences
follow, and both are V1-accepted limitations:

1. **PK upsert-of-unchanged-value events do not change visible
   state.** A consumer that drops such an event between two
   checkpoints sees no digest mismatch.
2. **Lexicographic cursor comparison only detects
   regression and exact duplication, not arbitrary missing
   events.** If two consecutive emitted cursors are
   `(100, k1, 0)` and `(100, k7, 0)`, a missing
   `(100, k4, 0)` between them is invisible to a
   sparse-cursor comparison: the consumer cannot distinguish
   "k4 was never emitted on this stream" from "k4 was
   emitted but the consumer dropped it." Cursor sequencing
   reliably catches *regression* (`new ≤ last_acked`) and
   *exact duplication* (`new == last_acked`); it does not
   catch sparse drops, even within one HLC tick.

V1 substrate therefore offers two integrity properties:
visible-state digest match at checkpoint boundaries, and
cursor monotonicity / non-regression. Membership of every
emitted version-entry on the stream is **not** proven by the
substrate. A consumer that requires stream-prefix membership
proof (e.g., a downstream secondary-index replicator) waits
for the future event-history digest domain
(`digest.cdc.event.v1`) or commits at-least-once delivery
plus consumer-side dedup on `CdcCursor`. The substrate ADR
declines to bundle that domain because no V1 caller demands
it.

The roadmap line "consumers verify cursor checkpoints against
partition `StateDigest` points algebraically" is satisfied by
the visible-domain digest under ADR 046's commutative-monoid
algebra. V1 ships exactly that.

**Cadence.** V1 emits one checkpoint every `max(1024 events,
1 second of stream wall time)`, whichever fires first. Cadence
is a runbook tunable, not a wire contract.

### 11. Authorization and tenancy

Subscriptions are tenant-scoped. A tenant-bound actor may
subscribe only to that tenant's tables; cross-tenant
subscriptions require the existing data-node authorization
gate. Authentication and authorization reuse the existing
mTLS / role surface (ADR 018 / 021); no new auth contract.

### 12. Failure and backpressure

- **Slow live subscribers do not block ingest.** If the per-
  subscription `mpsc::Sender::try_send` returns
  `TrySendError::Full`, the staged entry stays in the live-
  tail ring; the subscription's writer task drains as fast as
  QUIC flow control allows. Source admission is unaffected by
  subscriber count — positive performance guard verified by
  bench (Phase 9).
- **Subscriber falls behind ring head.** The writer task
  detects `cursor_high_water < ring.head_cursor` on the next
  drain attempt and triggers catchup-or-reject:
  - if `cursor_high_water.row_hlc >= retention_floor`, run a
    catchup over `(cursor_high_water, ring.head_cursor]` from
    storage;
  - else reject `CDC_REJECT_BELOW_RETENTION_FLOOR`.
- **Cursor below retention floor at open or mid-stream.**
  `CDC_REJECT_BELOW_RETENTION_FLOOR`. Consumer must
  re-bootstrap.
- **Ring overflow during catchup, or open-time
  ring-continuity violation.**
  `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`. Fires at open
  if any non-empty locality ring has
  `ring.head_cursor[l] > (snapshot_hlc, RowKeyId::MAX,
  RecordRowIndex::MAX)` (entries above the catchup ceiling
  were evicted before subscription open), or mid-catchup if
  the ring head advances past the ceiling during the scan.
- **Cold-locality watermark stall.** The substrate requires
  every source locality to advance `locality_finalized_hlc`
  at cadence
  `cdc_locality_heartbeat_interval_millis` even without
  writes. Operational misconfiguration (e.g., heartbeat
  worker disabled on a source locality) manifests as
  unbounded live-tail latency for higher-HLC events;
  diagnosis is via `cdc_subscription_lag_micros{source=ring}`
  rising despite admission rate falling. No reject code —
  this is an operator-visible degradation, not a stream
  failure.
- **Checkpoint digest mismatch on resume.**
  `CDC_REJECT_CHECKPOINT_DIGEST_MISMATCH`. Consumer must
  re-bootstrap from a fresh snapshot.
- **Logical fingerprint mismatch at open.**
  `CDC_REJECT_SCHEMA_FINGERPRINT_INCOMPATIBLE`.
- **Logical fingerprint changes non-additively mid-stream.**
  Same reject; consumer reconnects.
- **Subscription cancel via QUIC stream close.** Server
  detects the close; the subscription's writer task drops the
  receiver; the next admit that tries to send sees
  `SendError::Closed` and removes the handle from the
  registry. Bounded-time release verified by golden test.
- **Internal engine error.**
  `CDC_REJECT_INTERNAL_ENGINE_ERROR`. Catch-all for unexpected
  server failures; diagnostic detail in logs / spans.

### 13. Observability

V1 metrics use **bounded labels only** — same discipline ADR
052 / ADR 053 codified. Per-table / per-subscription /
per-tenant detail moves to logs / spans with top-K
aggregation patterns documented in the runbook.

```text
cdc_subscriptions_total{result}
cdc_events_emitted_total{source}
cdc_event_bytes_emitted_total{source}
cdc_checkpoints_emitted_total{source}
cdc_rejects_total{code}
cdc_subscription_lag_micros{source}
cdc_ring_overflow_total{}
cdc_catchup_to_live_handoff_micros{}
cdc_staged_entries_in_ring{}            (gauge; finalized + unfinalized)
cdc_pending_finalization_evicted_total{} (counter; staged but never finalized)
```

Allowed bounded labels:

```text
source = ring | catchup
result = opened | closed | rejected | cancelled
code   = below_retention_floor
       | ring_overflow_during_catchup
       | schema_fingerprint_incompatible
       | checkpoint_digest_mismatch
       | unknown_table
       | tenant_auth_denied
       | table_not_replicated_here
       | internal_engine_error
```

The label sets are pinned by architectural test; same
discipline ADR 053 followed for `path`.

No tenant, table, peer, subscription id, schema fingerprint,
or error-string labels.

No per-event audit. Lifecycle audit events:

- `CdcSubscriptionOpened { subscription_id, tenant_id,
  table_object_id, start_cursor, bound, opened_at_hlc }`;
- `CdcSubscriptionClosed { subscription_id, reason,
  final_cursor, closed_at_hlc }`.

### 14. Composition with prior ADRs

- **ADR 003** — bifurcated writes; CDC entries stage at data-
  branch durable append, release at `FinalizationCommitted`,
  discard on terminal failure.
- **ADR 004 / 010** — bytemuck POD metadata + Arrow IPC; CDC
  meta and frame shape unchanged.
- **ADR 011** — QUIC-first transport; subscriptions ride
  existing QUIC streams.
- **ADR 016** — R0 control ring; finalization marker is the
  release gate (internal only; not part of the cursor).
- **ADR 020** — L2 envelope encryption unchanged.
- **ADR 025** — audit chain; subscription lifecycle emits
  bounded audit records.
- **ADR 029** — Identity vs Locality; CDC is identity-axis.
- **ADR 037** — additive evolution composes through per-frame
  fingerprint binding.
- **ADR 042** — multi-row records; `RecordRowIndex` resolves
  intra-record ordering inside a single HLC tick.
- **ADR 045 / 049 / 050** — row identity policies; admission-
  outcome classification (ADR 050 §6) decides whether a hook
  fires per ADR 049 §5 (collapsed-duplicate suppression).
- **ADR 046** — `StateDigest` substrate; CDC checkpoints reuse
  `digest.table.row.visible.v1`.
- **ADR 047** — append-only tombstones flow as events with
  `__row_tombstone__` set.
- **ADR 048** — per-table retention is authoritative on CDC
  reachability.
- **ADR 051** — `LogicalSchemaFingerprint` is the per-frame
  schema binding; physical fingerprint is invisible.
- **ADR 052** — secondary indexes; version-entry grain is the
  same grain ADR 052 uses for index entries; `RecordRowIndex`
  newtype is shared.
- **ADR 053** — wire-side Arrow IPC body compression; CDC
  frames use `wire_encode_batch_for_path()` and register
  `cdc_event` as the fifth `path` label value.

### 15. Correctness domains are unchanged

CDC observation does not change:

- **Admission decisions** — CDC is post-admission; data-branch
  durable append must succeed before a stage; finalization
  must commit before a release.
- **Idempotency keys** — admission-side; not CDC-substrate-
  side.
- **Existing digest domains** —
  `digest.table.row.visible.v1` (ADR 046) and
  `digest.secondary_index.entry.v1` (ADR 052) untouched. ADR
  054 adds no new digest domain.
- **HLC monotonicity** — admission-side guarantee.
- **Raft / witness semantics** — Raft replicates control-ring
  state; CDC is over decoded row-axis events post-finalization.
- **Storage descriptor fingerprints** —
  `LogicalSchemaFingerprint` /
  `PhysicalDescriptorFingerprint` (ADR 051) preimages
  unchanged.
- **Object-store keys** — storage-domain.
- **CSB / AGG / PBC payload semantics** — unchanged.

Any hash, signature, or digest over logical records continues
to operate over the existing storage / catalog domain, never
over wire bytes (ADR 053 §9 invariant carries through).

## Rationale

**Why now.** Roadmap #10. ADR 053 closed the wire-codec axis;
CDC is the next forward-sequence item. The roadmap's stated
framing names exactly the substrate ADR 054 ships.

**Why substrate-only.** Three reasons compose:

1. The substrate alone has enough hard decisions (cursor,
   event grain, catchup, live-tail, finalization composition,
   checkpointing, retention, backpressure) that it deserves a
   focused ADR.
2. Triggers, filter pushdown, webhooks, and continuous
   queries each have their own contract surface (CPU budget /
   determinism rules for sync triggers; trigger catalog +
   sink validation for post-commit triggers; PBC validator
   integration for filter pushdown; HTTP retry / DLQ for
   webhooks; cursor-overlap semantics for continuous queries).
   Each earns its own ADR.
3. The council's two iterations both expanded scope and then
   retracted. The lesson is to ship the substrate first and
   let the consumers earn their own surfaces. The deferred
   list explicitly names every consumer.

**Why identity-axis cursor.** Three properties:

1. **Topology-invariant.** Slab compaction, replica
   replacement, and leader transfer do not change the cursor
   for already-emitted events.
2. **Disambiguation.** Multi-row records (ADR 042) and cross-
   locality concurrent writes are resolved by the
   `RowKeyId` / `RecordRowIndex` tiebreakers.
3. **Storage-axis-aligned.** The 3-tuple matches the storage-
   axis name for a version-entry; consumers can reuse the
   tuple in audit chains and `StateDigest` preimages without
   translation.

**Why version-entry grain (no `CdcEventKind`).** Hides nothing
the consumer needs; matches storage; matches ADR 052's
secondary-index entry domain; lets resurrection be observable;
lets PK upsert-of-unchanged-value be observable; does not
introduce a parallel event taxonomy or server-side state
tracking. The consumer-friendly `CdcEventKind` view is a
follow-up ADR.

**Why in-memory live-tail ring (no durable CDC log).** §
"Architectural inextricability" gives the structural argument:
zero-write-amplification is physically inextricable from the
logical-cursor choice, because data-ring slabs are seek-keyed
on `(HLC, RowKeyId)` not `(Epoch, LogIndex)`. A durable log
would force a coupled cursor-shape change. V1 declines the
bundled change.

**Why stage-then-release on the ring.** Per ADR 003, the
bifurcated write path produces explicit pre-finalization
states. The substrate composes by staging at admission (when
the entry is available and the materialization cost is
amortized against admission) and releasing at finalization (a
single bit flip). Pending-finalization cleanup is already
load-bearing under ADR 003; CDC reuses it. The alternative —
emit-after-finalization-arrives — is operationally equivalent
but pays the materialization cost on the finalization-event
path instead of the admission path, where it is more visible
to the per-table latency budget.

**Why checkpoints reuse `digest.table.row.visible.v1`.** The
substrate's V1 integrity contract is "visible table state
matches at checkpoint boundaries, and cursors never regress
on a single stream." Visible-state digest proves the first
half; cursor monotonicity proves the second half. Neither
proves stream-prefix *membership* — a sparse-cursor
comparison cannot distinguish "k4 was never emitted" from
"k4 was emitted and dropped." V1 caller patterns are met by
the two properties above; no V1 caller requires
event-history membership proof. A future
event-history digest domain (`digest.cdc.event.v1`) is the
right tool when (e.g.) a downstream secondary-index
replicator needs prefix-membership; the substrate ADR
declines to bundle it.

**Why bounded metric labels.** ADR 052 / ADR 053 codified the
discipline. The argument for unbounded labels (operators want
per-table CDC visibility) applies equally to compression
metrics, and the council declined there. Per-table detail
moves to logs / spans with top-K aggregation.

**Why no node capability bit.** CDC is a coordinated storage
protocol surface, not an optional intra-cluster optimization.
Same discipline ADR 053 followed.

**Why no envelope `format_version` bump, no new
`FRAME_FLAG_*`.** Four new `MsgType` constants carry the
protocol surface; existing envelope and flag set cover stream
lifecycle (`FRAME_FLAG_STREAM_END`, `FRAME_FLAG_KNOWN_MASK`).

**Why this is its own ADR rather than waiting for the
trigger ADR.** Triggers have their own surface (catalog,
sink validation, lifecycle states, output budgets, pause-
resume operator surface) and their own threat model
(deterministic PBC, sink-write idempotency, indirect cycle
detection). Bundling produces a mega-ADR that defers a real
substrate decision behind a larger one.

## Consequences

- **Zero CDC write amplification.** The substrate adds no
  on-disk artifact and no per-row durable CDC entry. Storage
  pass-through and existing retention bound replay.
- **In-memory ring memory cost.** Per-locality ring is bounded
  by `max_cdc_live_tail_bytes_per_locality`; default tunable
  via ADR 030. Production memory footprint is `N_localities ×
  max_cdc_live_tail_bytes_per_locality`.
- **CDC is at-least-once.** Cursor resume after reconnect may
  replay events the consumer already saw; consumers dedupe on
  the 3-tuple cursor.
- **CDC delivery latency.** Bounded by one-way QUIC RTT plus
  ring-drain wakeup plus encode CPU **plus the slowest
  source locality's finalization or heartbeat cadence above
  the emission HLC** (the safe-emission watermark holds
  events on faster localities until the slowest locality has
  finalized through). The slowest-locality term is the
  load-bearing cost of preserving global `CdcCursor`
  monotonicity without exposing `LocalityId` on the cursor.
  Cold localities are bounded above by
  `cdc_locality_heartbeat_interval_millis` (V1 default
  1000 ms). Bench matrix verifies p99 ≤ 5× one-way RTT
  under 10k events/sec on a balanced workload, plus
  separate skewed-locality and cold-locality benches
  measure watermark hold time.
- **Heartbeat cost on the control ring.** Every locality
  emits at most one no-op finalization marker per
  `cdc_locality_heartbeat_interval_millis` (default 1 Hz)
  when its `locality_finalized_hlc` has not naturally
  advanced within the cadence window. Heartbeats are
  locality-scoped, not (locality, table)-scoped: one marker
  per locality serves the watermark for every table whose
  source localities include it. Cluster-wide write rate to
  the control ring grows by `N_localities /
  heartbeat_interval` (independent of CDC-table count).
  Busy localities skip heartbeats whenever a natural
  finalization fires within the cadence window. The
  contribution is dominated by existing finalization-marker
  traffic on busy clusters and cluster-insignificant on
  quiet ones; the runbook documents tuning.
- **Slow consumers don't block ingest.** Bench-verified
  positive performance guard at 100 active subscribers.
- **CDC reachability is bounded by source-table retention.**
  Consumers that fall behind retention must re-bootstrap.
  Operationally identical to Kafka-class consumer-lag handling.
- **The substrate proves visible-state match at checkpoint
  boundaries and cursor non-regression on a single stream;
  it does *not* prove stream-prefix membership.** A consumer
  that drops a PK upsert-of-unchanged-value, or any single
  event between two emitted cursors, sees neither a digest
  mismatch (visible state unchanged or unrelated) nor a
  cursor regression (the next cursor is still greater than
  the last acked one). A future event-history digest ADR
  (`digest.cdc.event.v1`) can add stream-prefix membership
  proof when a consumer requires it; V1 callers do not.
- **CDC adds one new ADR 053 wire-compression `path` label
  value: `cdc_event`.** Operators reading
  `network_compression_*` dashboards see CDC bandwidth in the
  same family as other wire-side fresh-encoders.
- **No on-disk shape change, no descriptor field, no envelope
  `format_version` bump, no new HRFT frame family, no new
  node capability bit, no Cargo feature change, no new
  digest domain.**

## Rejected alternatives

- **Raft-WAL row-byte CDC tail** (brainstorm 004 idea #1
  literal). Rejected. Reverses ADR 003.
- **Control-log-finality cursor `(Epoch, LogIndex,
  CdcEventOrdinal)`.** Rejected. Data-ring slabs are not
  indexed by `(Epoch, LogIndex)`; would force full table
  scans on resume (see "Architectural inextricability").
- **HLC-only cursors.** Rejected. Ambiguous on cross-locality
  ties or multi-row records.
- **`__row_key_id__`-only cursors.** Rejected. Loses version
  ordering.
- **Locality-tagged cursor.** Rejected. Identity-axis cursor
  survives topology.
- **Visible-state-change event grain** (insert / update /
  delete enum at substrate). Rejected. Hides resurrection,
  hides PK upsert-of-unchanged-value, requires server-side
  per-row state tracking. A future "consumer-friendly
  higher-level CDC" ADR may layer the derived view.
- **Durable CDC event log.** Rejected. Write amplification
  without replay benefits storage doesn't already provide;
  also forces the cursor-shape change (see "Architectural
  inextricability").
- **New `digest.cdc.event.v1` domain at substrate.**
  Rejected. Visible-state digest covers V1 caller patterns;
  add when a consumer needs membership proofs.
- **PBC filter pushdown bundled in substrate.** Rejected.
  Filters need watermarks + filter-budget +
  no-match-checkpoint-advancement; orthogonal to substrate.
- **Post-commit triggers + `TriggerCatalog` bundled.**
  Rejected. Each trigger surface (catalog, sinks, lifecycle,
  budget, pause-resume) earns its own ADR.
- **Synchronous triggers.** Rejected for V1.
- **Webhook worker / `__webhook_events__` table.** Rejected.
- **Continuous historical-plus-live queries on one stream.**
  Rejected. Composes externally.
- **Direct Rune / SQL / Wasm at the storage boundary.**
  Rejected by ADR 036.
- **Tenant / table metric labels.** Rejected for cardinality.
- **CDC-specific compression policy.** Rejected. ADR 053 owns
  wire compression.
- **Cluster-level CDC kill-switch.** Rejected per
  `no-premature-backward-compat.md`. Per-table opt-in / opt-
  out is a follow-up if benchmarks pull.
- **Server-side durable cursor commit / consumer-group
  semantics.** Rejected for V1.
- **Exactly-once delivery.** Rejected for V1.
- **Multi-table / tenant-firehose subscription scope.**
  Rejected.
- **Reserved enum slots / compatibility shims / `V1` / `V2`
  type suffixes.** Rejected.
- **`WendelonCdc*` brand-leaked symbol prefixes.** Rejected.
- **Bump `HRFT_ENVELOPE_VERSION`.** Rejected. Four new
  `MsgType` are sufficient.
- **Allocate `FRAME_FLAG_*` for CDC stream lifecycle.**
  Rejected. `FRAME_FLAG_STREAM_END` covers it.
- **New `NodeCapability::CdcSubscribe` bit.** Rejected.
- **`CDC_WATERMARK` `MsgType` in V1.** Rejected. The
  watermark is operationally useful only when filter
  pushdown drops events; deferred along with filter
  pushdown.
- **`CdcSubscribeBound::UntilHlc(Hlc)`.** Rejected for V1; no
  load-bearing caller.

## Resolved questions

V1 commitments — each can be revisited in a follow-up ADR
without on-disk surface change.

1. Substrate scope only. Triggers, filter pushdown, webhooks,
   continuous queries deferred.
2. Cursor: `(Hlc, RowKeyId, RecordRowIndex)`.
3. Event grain: storage version-entry. No `CdcEventKind` /
   `CdcOperation` enum at substrate.
4. Live-tail mechanism: in-memory per-locality ring;
   admission stages with `finalized = false`; finalization
   observer sets `finalized = true`; pending-finalization
   cleanup discards staged entries on terminal failure.
5. Catchup mechanism: existing storage scan paths over
   retained source storage; full `CdcCursor` tuple bounds
   over a safe global cut `snapshot_hlc =
   min(locality_finalized_hlc(l))`; min-heap merge across
   localities.
6. Catchup-to-live handoff at `(snapshot_hlc, RowKeyId::MAX,
   RecordRowIndex::MAX)`; live-tail emission is gated by the
   safe-emission watermark
   `safe_emission_hlc = min(locality_finalized_hlc(l))` so
   that the global emitted cursor sequence is non-decreasing
   in `CdcCursor` order across localities; open-time
   ring-continuity guard (`ring.head_cursor[l] <=
   (snapshot_hlc, MAX, MAX)` for every non-empty locality
   ring) plus mid-catchup ring-overflow detection both →
   `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP` + reconnect.
7. Cold-locality heartbeat contract: **locality-scoped, not
   per-table.** Every locality advances its
   `locality_finalized_hlc` at cadence
   `cdc_locality_heartbeat_interval_millis` (V1 default
   1000 ms) via a no-op control-ring finalization marker.
   One worker per locality, one marker per quiet interval,
   serving the watermark for every table whose source
   localities include that locality. Cluster cost:
   `N_localities / heartbeat_interval`.
8. Wire surface: 4 new `MsgType` (`CDC_SUBSCRIBE_REQ`,
   `CDC_FRAME`, `CDC_CHECKPOINT`, `CDC_REJECT`).
9. Subscription bound: `Live` or `SnapshotAtOpen`.
10. Retention: bounded by ADR 048
    `TableLifecycleCatalog`; below-floor cursor →
    `CDC_REJECT_BELOW_RETENTION_FLOOR`.
11. Schema binding: per-frame `LogicalSchemaFingerprint`
    (ADR 051); consumer re-binds on additive evolution; non-
    additive evolution rejects
    `CDC_REJECT_SCHEMA_FINGERPRINT_INCOMPATIBLE`.
12. Checkpoints: `digest.table.row.visible.v1` (ADR 046 V1
    helper) at cadence 1024 events or 1 second wall time
    (runbook tunable). No new digest domain.
13. Server-side state: stateless on subscription identity
    (no consumer-group, no durable cursor commit); per-
    locality ring is the only mutable CDC state.
14. Delivery: at-least-once; cursor is the dedupe key.
15. Reject taxonomy: 8 `CDC_REJECT_*` codes (Phase 0 §5
    enumerates).
16. Metrics: `cdc_*` family with bounded label sets only.
17. Audit: subscription open / close lifecycle events; no
    per-event audit.
18. ADR 053 path-label enumeration extends with `cdc_event`
    as the fifth allowed value.
19. No envelope `format_version` bump; no new HRFT frame
    family beyond the four `MsgType`; no new
    `FRAME_FLAG_*`; no new `NodeCapability` bit; no new
    digest domain; no Cargo feature change.

## Open questions

**None on substrate shape.** Every commitment is recorded in
§"Resolved questions" above; remaining tunables (live-tail
cap, checkpoint cadence) are runbook-level and reversible per
deployment per ADR 030. The watermark frame, durable cursor
commit, cross-replica handoff, before-image payloads, and
event-history digest domain are all explicit follow-up ADRs
(see §"Forward-compatibility analysis" and §"Deferred to
follow-up ADRs").

## Forward-compatibility analysis

**Future PBC filter pushdown ADR (brainstorm 004 idea #3).**
Extends `CDC_SUBSCRIBE_REQ` with a trailing-section bytecode
payload (PBC validator surface from ADR 036). Adds
`CDC_WATERMARK` message type for low-selectivity streams.
Substrate cursor and event grain unchanged.

**Future post-commit-trigger ADR (brainstorm 004 idea #2,
post-commit variant).** Adds `TriggerCatalog` mirroring ADR
052's `SecondaryIndexCatalog` shape; new
`SchemaObjectKind::Trigger`; trigger worker that runs PBC
over CDC events and appends to descriptor-bound append-only
sinks. Substrate unchanged.

**Future synchronous-trigger ADR (brainstorm 004 idea #2,
sync variant).** Admission-time PBC hook with its own CPU
budget and failure-domain rules. Different code path from the
CDC fan-out; substrate unchanged.

**Future webhook ADR (brainstorm 004 idea #4).** Adds
`__webhook_events__` (or equivalent) plus an internal HTTP
worker with retry / DLQ semantics. Webhook worker is a CDC
consumer; substrate unchanged.

**Future continuous-historical-plus-live ADR (brainstorm 004
idea #3).** Composes externally — consumer issues an L2 read
at HLC = T plus a CDC subscription with cursor matching T.
Or, alternatively, a dedicated streaming `QUERY_REQ` /
`QUERY_RESP` shape; either composition pattern leaves the
substrate unchanged.

**Future before-image CDC ADR.** Adds `before_*` columns to
the CDC event row schema. Substrate cursor / digest domain
unchanged; payload schema gains additional columns; consumers
that don't care can ignore them.

**Future event-history digest ADR.** Adds
`digest.cdc.event.v1` domain when a consumer (e.g.,
secondary-index replicator) needs membership proofs that
include not-visible-state-changing events. Substrate cursor
unchanged.

**Future durable cursor commit ADR.** Server-side state for
consumer-group semantics. New `CDC_CURSOR_COMMIT_REQ` /
`_RESP` MsgType pair. Substrate cursor unchanged.

**Future exactly-once ADR.** Requires server-side cursor
commit + per-frame ack RPC. Substrate cursor unchanged.

**Future multi-table / tenant-firehose ADR.** Subscription
scope generalizes. Cursor extends to carry `TableObjectId`
explicitly. V1's two-field scope is forward-compatible.

**Future cross-replica cursor handoff ADR.** Server-side
durable cursor + replica synchronization for zero-reconnect
failover. Substrate cursor unchanged.

**Roadmap #11 LSM-style leveled compaction.** Storage-axis
only; CDC reads version-entries which survive compaction by
construction (compaction drops only tombstone-superseded rows
that are below retention; CDC's ring + storage-scan path
doesn't depend on storage layout).

**Roadmap #12 vector embeddings.** ANN write paths produce
ordinary admissions; CDC observes them like any other write.
Cursor shape unchanged.

**Roadmap #13 encoding refinements.** LCE V2 / V3 and column
projection are pluggable on storage / wire axes; CDC events
inherit ADR 053's wire compression; column projection on CDC
is a follow-up that composes with per-buffer LZ4.

## Deferred to follow-up ADRs

1. PBC filter pushdown on subscriptions (brainstorm 004
   idea #3).
2. Post-commit PBC triggers + `TriggerCatalog` (brainstorm
   004 idea #2, post-commit variant).
3. Synchronous PBC triggers at admission (brainstorm 004
   idea #2, sync variant).
4. Webhook worker + `__webhook_events__` system table
   (brainstorm 004 idea #4).
5. Continuous historical-plus-live queries on one stream
   (brainstorm 004 idea #3).
6. Before-image CDC payloads.
7. Event-history `StateDigest` domain
   (`digest.cdc.event.v1`).
8. Server-side durable cursor commit (consumer-group).
9. Exactly-once delivery contract.
10. Multi-table / tenant-firehose subscription scope.
11. Column projection on subscriptions.
12. Cross-replica cursor handoff (zero-reconnect failover).
13. Consumer-driven checkpoint cadence.
14. Per-event audit.
15. Schema-evolution-as-CDC-event taxonomy.
16. Metadata / catalog CDC.
17. Per-table CDC opt-in / opt-out.
18. Per-table CDC retention extension beyond source-table
    retention floor.
19. Consumer-friendly higher-level `CdcEventKind` view layered
    on top of substrate version-entries.
20. External SDK CDC contract (when non-Rust SDKs ship).
21. Live plaintext-byte / ratio accounting for CDC events
    (mirrors ADR 053 §"plaintext-byte counter omission").

## References

- **ADR 003** — Stellarator bifurcated writes;
  `FinalizationCommitted` is the substrate's release gate.
- **ADR 004** — zero-copy custom transport.
- **ADR 010** — tiered serialization.
- **ADR 011** — QUIC-first transport.
- **ADR 016** — R0 control ring; finalization marker is the
  internal release gate (not part of the cursor).
- **ADR 020** — L2 envelope encryption.
- **ADR 025** — audit chain; subscription lifecycle events.
- **ADR 029** — Identity vs Locality.
- **ADR 030** — deployment profiles.
- **ADR 036** — PBC pushdown contract; future filter / trigger
  follow-up ADRs reuse the validator family.
- **ADR 037** — tenant table schemas; additive evolution
  only.
- **ADR 042** — multi-row records; `RecordRowIndex` is the
  cursor tiebreaker.
- **ADR 045** — append-only tables (canonical CDC sink).
- **ADR 046** — `StateDigest` substrate; CDC checkpoints
  reuse `digest.table.row.visible.v1`.
- **ADR 047** — append-only tombstones.
- **ADR 048** — per-table source-axis retention is
  authoritative on CDC reachability.
- **ADR 049** — content-hash row identity (collapsed writes
  emit no event).
- **ADR 050** — PK delete via supersession; admission outcome
  classification (§6) decides whether a hook fires.
- **ADR 051** — descriptor fingerprint split;
  `LogicalSchemaFingerprint` is the per-frame schema binding.
- **ADR 052** — secondary indexes; `RecordRowIndex` newtype
  shared.
- **ADR 053** — wire-side Arrow IPC body compression;
  `wire_encode_batch_for_path` + new
  `NetworkCompressionPath::CdcEvent` value.
- `plans/conventions/newtype-units.md` — `CdcCursor`
  components are newtype-units.
- `plans/conventions/no-version-suffixes.md` — no
  `Cdc*V1` / `Cdc*V2`.
- `plans/conventions/no-premature-backward-compat.md` —
  load-bearing on the deferred-features list.
- `plans/conventions/no-project-name-in-protocols.md` —
  `cdc_*` metrics; `CDC_REJECT_*` codes.
- `plans/conventions/capability-vs-feature-naming.md` — no
  `NodeCapability::Cdc*` bit.
- `plans/philosophy.md` — Identity-vs-Locality on the cursor.
- `plans/brainstorms/004_realtime_cdc_and_triggers.md` —
  source brainstorm.

## Promotion checklist

Council-confirmed dispositions:

- [x] V1 scope is substrate-only; triggers, filter pushdown,
      webhooks, and continuous historical-plus-live queries
      stay in follow-up ADRs.
- [x] Cursor shape: `(Hlc, RowKeyId, RecordRowIndex)`.
- [x] Event grain: storage version-entry; no `CdcEventKind` /
      `CdcOperation` enum at substrate.
- [x] Live-tail mechanism: in-memory per-locality ring;
      admission stages with `finalized = false`; finalization
      releases; live-tail emission gated by safe-emission
      watermark `min(locality_finalized_hlc(l))` so the
      stream is non-decreasing in `CdcCursor` order under
      skewed locality finalization.
- [x] Catchup mechanism: existing storage scan paths; full
      `CdcCursor` tuple bounds (not HLC ranges) over safe
      global cut `snapshot_hlc =
      min(locality_finalized_hlc(l))`; min-heap merge across
      localities.
- [x] Open-time ring-continuity guard:
      `ring.head_cursor[l] <= (snapshot_hlc, RowKeyId::MAX,
      RecordRowIndex::MAX)` for every non-empty locality
      ring; violation rejects open with
      `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`.
- [x] Cold-locality heartbeat is **locality-scoped, not
      per-table**: one heartbeat worker per locality on
      the locality leader; one no-op control-ring
      finalization marker per quiet interval at cadence
      `cdc_locality_heartbeat_interval_millis` (V1 default
      1000 ms); cluster cost
      `N_localities / heartbeat_interval`, independent of
      CDC-table count.
- [x] Retention contract: ADR 048 source-axis retention is
      authoritative; below-floor cursor rejects
      `CDC_REJECT_BELOW_RETENTION_FLOOR`.
- [x] `StateDigest` checkpoint domain
      (`digest.table.row.visible.v1`, ADR 046 V1 helper) and
      cadence (`max(1024 events, 1s)` V1 default).
- [x] Resume-time digest validation via
      `CdcSubscribeRequestMeta::resume_visible_state_digest`
      + `resume_digest_present`.
- [x] Wire surface: 4 new `MsgType` (`CDC_SUBSCRIBE_REQ`,
      `CDC_FRAME`, `CDC_CHECKPOINT`, `CDC_REJECT`).
- [x] V1 default `max_cdc_live_tail_bytes_per_locality =
      64 MiB`.
- [x] `Live` and `SnapshotAtOpen` ship in V1; cross-replica
      reconnect is a fresh subscription.
- [x] `CDC_FRAME` meta carries both `frame_low_cursor` and
      `frame_high_cursor`.
- [x] No new HRFT envelope `format_version` bump, no new
      `FRAME_FLAG_*`, no new `NodeCapability`, no Cargo
      feature change, no new digest domain, no
      project-name-prefixed CDC symbol.
- [x] No `CdcEventKind` / `CdcOperation` enum exists.
- [x] No reserved enum slots, no compatibility shims, no
      `V1` / `V2` Rust type suffixes.

Pre-promotion engineering tasks (settled in code / test):

- [x] `CdcCursor`, `CdcSubscriptionId`, and CDC meta POD
      assertions land; sparse CDC enums carry raw integer fields
      on POD meta and decode through checked accessors; newtype-
      unit discipline holds for `RecordRowIndex` / `Hlc` /
      `RowKeyId`.
- [x] Bench smoke populated:
      `plans/impl/054_cdc_subscription_substrate_bench_results.md`
      records quick live-tail latency, fan-out, catchup tuple
      throughput, ring-overflow threshold, checkpoint cost,
      admission isolation, schema-fingerprint compare,
      wire-compression, stage/release/drain, and watermark-hold
      probes.
- [ ] Bench acceptance: live-tail p99 (admit → consumer
      receive after finalization) ≤ 5× one-way QUIC RTT
      under 10k events/sec; catchup throughput ≥ 100 MiB/s
      decoded; ring overflow does not block admission;
      admission p99 regresses ≤ 5% at 100 active
      subscribers. Representative runs use `--full` plus
      `--acceptance-one-way-quic-rtt-micros
      <measured_one_way_quic_rtt_micros>` and must record
      `cdc_acceptance_* status=pass` for every matrix case.
- [x] Runbook lands at
      `plans/ops/runbooks/cdc_subscription.md`.
- [x] `plans/wip/roadmap.md` updated to reflect ADR 054
      promoted.
