# Wendelon Roadmap

The forward sequence. Stable path (`plans/wip/roadmap.md`) —
git holds the archeology, this file looks forward.

**Update cadence.** The roadmap is rewritten at planning
sessions, not at implementation commits. The council operates on
a block-chain rhythm: plan → implement → plan → implement, and
this file is a snapshot of the most recent plan. Between
sessions the roadmap lags the code on purpose — an item can read
"implementation phases pending" while the implementation is
already merged on `main`, or "full 100k / 1M bench matrix
remains ahead" while a smoke run has already landed. That is
expected, not a discrepancy. Don't open a session by flagging
the gap as if it were news; check `git log` and the relevant
impl plan / bench-results file for actual status, and roll
roadmap edits into the next planning session along with the
forward-sequence rewrite.

## Where we are

ADR 054 (CDC Subscription Substrate) just promoted. Closes
the cursor / live-tail / catchup / checkpoint / retention
shape question for change-data capture. Substrate-only
scope: triggers, PBC filter pushdown, webhooks, and
continuous historical-plus-live queries are explicit
follow-up ADRs. Identity-axis logical cursor `(row_hlc:
Hlc, row_key_id: RowKeyId, record_row_index:
RecordRowIndex)` matching the data-ring slab seek key
(zero-write-amplification mandates this — control-log
finality cursors would force table scans on resume). Event
grain is the storage version-entry; no `CdcEventKind` /
`CdcOperation` enum at substrate. In-memory per-locality
`CdcLiveTailRing` with admission-time staging gated by ADR
003 `FinalizationCommitted`; pending-finalization cleanup
discards on terminal failure. Catchup uses **full
`CdcCursor` tuple bounds** (not HLC ranges, which would
miss same-HLC suffix) over a safe global cut `snapshot_hlc
= min(locality_finalized_hlc(l))` at subscribe-open;
existing storage scan paths, no durable CDC event log.
Live-tail emission is gated by safe-emission watermark
`safe_emission_hlc = min(locality_finalized_hlc(l))` so
the emitted cursor sequence is non-decreasing in
`CdcCursor` lex order under skewed locality finalization
(without it, A finalized through 100 could emit A@90
before B finalizes B@75, violating monotonicity).
Open-time ring-continuity guard:
`ring.head_cursor[l] <= (snapshot_hlc, RowKeyId::MAX,
RecordRowIndex::MAX)` for every non-empty locality ring;
violation rejects open with
`CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP` rather than
silently dropping pre-open evictions. Cold-locality
heartbeat contract is **locality-scoped, not per-table**:
exactly one `CdcLocalityHeartbeatWorker` per locality on
the locality leader emitting at most one no-op control-
ring finalization marker per quiet interval at cadence
`cdc_locality_heartbeat_interval_millis` (V1 default
1000 ms) — `locality_finalized_hlc(l)` is a single
per-locality value (ADR 016 control ring is locality-
keyed), so one marker advances the watermark for every
table whose source localities include `l`. Cluster cost:
`N_localities / heartbeat_interval`, independent of
CDC-table count; the worker spawns unconditionally on
locality-leader startup. Visible-domain `StateDigest`
checkpoints reuse `digest.table.row.visible.v1` (ADR 046
V1 helper); no new digest domain. Resume-time digest
validation via
`CdcSubscribeRequestMeta::resume_visible_state_digest` +
`resume_digest_present` rejects mismatched checkpoints at
open with `CDC_REJECT_CHECKPOINT_DIGEST_MISMATCH` rather
than waiting for the next checkpoint frame. Visible-state
digest match plus cursor non-regression cover V1
integrity; the substrate explicitly does **not** prove
stream-prefix membership (sparse-cursor comparison cannot
distinguish "k4 was never emitted" from "k4 was emitted
and dropped") — a future event-history digest ADR
(`digest.cdc.event.v1`) can add prefix-membership when a
consumer demands it. Four new `MsgType` constants
(`CDC_SUBSCRIBE_REQ` / `CDC_FRAME` / `CDC_CHECKPOINT` /
`CDC_REJECT`); 8-code `CDC_REJECT_*` taxonomy; sparse
enums (`CdcSubscribeBound`, `CdcFrameSource`,
`CdcRejectCode`, `DigestDomain`) carry raw integer fields
on POD meta with `from_raw` checked accessors (deriving
`Pod` on sparse enums is unsound — most bit patterns are
invalid). Reject-code discriminants start at 1 so a zero-
initialized `CdcRejectMeta` cannot decode as a valid
reject. `Live` and `SnapshotAtOpen` subscription bounds;
cross-replica reconnect is a fresh subscription. CDC
inherits ADR 053's wire helper and registers
`NetworkCompressionPath::CdcEvent` as the fifth `path`
label value. Bounded `cdc_*` metrics
(`source ∈ {ring, catchup}`, `result ∈ {opened, closed,
rejected, cancelled}`, `code ∈` 8-value enum); no
tenant / table / subscription labels. V1 default
`max_cdc_live_tail_bytes_per_locality = 64 MiB` (ADR 030
deployment-profile-tunable); checkpoint cadence
`max(1024 events, 1s)` (runbook tunable). No new HRFT
envelope `format_version` bump, no new `FRAME_FLAG_*`,
no new `NodeCapability` bit, no Cargo feature change, no
new digest domain, no on-disk shape change. CDC delivery
latency is bounded by one-way QUIC RTT plus ring-drain
wakeup plus encode CPU plus the slowest source
locality's finalization or heartbeat cadence above the
emission HLC — the load-bearing cost of preserving
global cursor monotonicity without exposing
`LocalityId` on the cursor. See
`plans/adr/054_cdc_subscription_substrate.md` and
`plans/impl/054_cdc_subscription_substrate.md`.
Implementation phases pending.

ADR 053 (Wire-Side Arrow IPC Body Compression) is the
immediate predecessor and the binding ADR 054 inherits.
Per-buffer LZ4_FRAME on fresh wire-side Arrow IPC
encoders via Arrow IPC's native `BodyCompression`,
mirroring ADR 043 on the wire axis. Single helper
`wire_ipc_write_options()` pinned by
`WIRE_BODY_COMPRESSION` with `wire_encode_batch` as its
function-level companion; four wire-side fresh-encoder
sites flip in V1; storage pass-through bytes flow
verbatim. Bounded `network_compression_*` metric family
with `{codec, path}` labels — CDC adds `cdc_event` as
the fifth allowed `path` value. See
`plans/adr/053_wire_arrow_ipc_body_compression.md` and
`plans/impl/053_wire_arrow_ipc_body_compression.md`.
Implementation phases pending.

## Prioritization philosophy

Wendelon is being sequenced as a complete-system launch rather
than as an incrementally exposed service. That changes the
roadmap constraint. In a live product, near-term user pain,
production evidence, and delivery commitments would rightly
pull high-impact access features forward. In this pre-launch
phase, those signals inform the design, but they do not have to
dictate the order of construction.

The ordering principle for this stretch is **software
composition**. The goal is to **minimize cumulative rework
across the whole tree before launch**: choose the order that
lets later ADRs build on settled foundations instead of
revisiting earlier work because a later ADR changed a core
shape. A feature wants to come *earlier* when it settles a
foundational type or invariant (`Identity`, `NodeId`, slab
footer, descriptor fingerprint) that many later features build
on. A feature wants to come *later* when it is a consumer or
refinement of those surfaces and composes cleanly once they are
stable.

This does not make LSM compaction, vector embeddings, or
encoding refinements less important. They remain central
warehouse capabilities. The sequencing claim is narrower: they
should consume the algebraic substrate (`StateDigest`),
lifecycle layer (tombstones, retention), topology decision,
identity / descriptor cleanup, access-pattern surface
(secondary indexes), wire-compression policy, and CDC substrate
rather than force those foundations to be retrofitted
afterward.

## Forward sequence (council-converged)

Decided across multiple iterations between Claude, Codex, and
Gemini. Steps 1–10 are promoted (in `plans/adr/`); the
forward shape begins at #11.

1. **`StateDigest` substrate** — ADR 046 promoted.
2. **Row-level tombstones on append-only** — ADR 047 promoted.
3. **Per-table retention policy** — ADR 048 promoted.
4. **Algebraic topology gate** — closed 2026-04-29 with
   Outcome B (opaque `NodeId`); see brainstorm 015 closure.
   Splits to brainstorm 018 (FROST-WAL authority) and
   brainstorm 019 (Sybil-resistance threat model), both
   off the active queue.
5. **Content-hash identity (`AppendOnlyContentHash`)** —
   ADR 049 promoted.
6. **PK-keyed row-delete via supersession** — ADR 050
   promoted.
7. **Logical / physical descriptor fingerprint split** —
   ADR 051 promoted.
8. **Secondary indexes on non-clustering columns** —
   ADR 052 promoted.
9. **Wire-side Arrow IPC body compression** — ADR 053
   promoted.
10. **CDC subscription substrate** — ADR 054 promoted.
    Substrate-only; triggers, PBC filter pushdown,
    webhooks, and continuous historical-plus-live queries
    are explicit follow-up ADRs (each consumes the
    substrate). See *Where we are* above.

11. **LSM-style leveled compaction.** The compactor reads
    multi-row segments, drops rows matched by tombstones,
    generates new secondary index segments (rebuilding
    sidecars per ADR 052 §"Compaction"), and updates
    `StateDigest` points algebraically (additions for new
    survivor versions, subtractions for evicted versions).
    Append-only segments are LSM-clean by construction.
    Composes against ADR 048 retention floors, ADR 050
    PK supersession chains, and ADR 052 secondary-index
    sidecars. CDC's `safe_emission_hlc` watermark
    interaction with compaction needs explicit treatment
    (compaction must not advance below any active
    subscription's `cursor_high_water` when sidecars are
    being rewritten in-place, or ring-overflow recovery
    must reject cleanly). *Large, multi-ADR.*

12. **CDC follow-up ADRs (consume ADR 054 substrate).**
    Independent; each ships its own contract surface:
    - **PBC filter pushdown on subscriptions.** Trailing-
      section bytecode on `CDC_SUBSCRIBE_REQ`; new
      `CDC_WATERMARK` MsgType for low-selectivity streams
      (every emitted event advances the cursor at
      substrate, but watermarks become load-bearing once
      filters can drop events without advancing); filter
      reject taxonomy; budget accounting; no-match
      checkpoint advancement. Reuses ADR 036 PBC validator
      family.
    - **Continuous historical-plus-live queries.** Either
      composes externally (L2 read at HLC=T plus CDC
      subscription with cursor matching T) or introduces a
      dedicated streaming `QUERY_REQ` / `QUERY_RESP` shape.
    - **Post-commit PBC triggers.** New `TriggerCatalog`
      (mirroring ADR 052 `SecondaryIndexCatalog` shape),
      `SchemaObjectKind::Trigger`, trigger worker that runs
      PBC over CDC events and appends to descriptor-bound
      append-only sinks; sink-write idempotency; indirect
      cycle detection; pause-resume operator surface.
    - **Synchronous PBC triggers at admission.**
      Admission-time PBC hook with its own CPU budget and
      failure-domain rules; deterministic PBC requirement.
    - **Webhook worker + `__webhook_events__` system table
      (or equivalent).** Internal HTTP retry / DLQ
      semantics; webhook worker is a CDC consumer.
    - **Server-side durable cursor commit (consumer-group
      semantics).** New `CDC_CURSOR_COMMIT_REQ` / `_RESP`
      MsgType pair; substrate cursor unchanged.
    - **Exactly-once delivery contract.** Server-side
      cursor commit + per-frame ack RPC.
    - **Cross-replica cursor handoff (zero-reconnect
      failover).** Server-side durable cursor + replica
      synchronization.
    - **Multi-table / tenant-firehose subscription scope.**
      Cursor extends to carry `TableObjectId` explicitly;
      V1's two-field scope is forward-compatible.
    - **Before-image CDC payloads.** Adds `before_*`
      columns to the CDC event row schema; substrate
      cursor / digest domain unchanged.
    - **Event-history digest domain
      (`digest.cdc.event.v1`).** Adds prefix-membership
      proof when a consumer (e.g., downstream secondary-
      index replicator) needs it independent of visible
      state.
    - **Consumer-friendly higher-level `CdcEventKind`
      view.** Layered on top of substrate version-entries;
      derives insert / update / delete / resurrection
      semantics from row-axis synthetic columns.
    - **Per-table CDC opt-in / opt-out** (and per-table
      retention extension beyond source-table retention).
    *Each is its own ADR; ordering opportunistic.*

13. **Vector embeddings + vector-form identity / locality
    PBC opcodes** *(brainstorm 002.)* ANN as a first-class
    index family, composing on the secondary-index
    substrate (#8) and LSM (#11). *Large, multi-ADR.*

14. **Encoding, fetch, and distributed compute
    refinements.** Bundle: LCE V2 / V3 (brainstorm 011 —
    homerolled SIMD encodings, COCD); mini-page row-group
    fetch (refines ADR 044 from buffer to row-group
    level); per-buffer page directory; zstd capacity-tier
    codec + `CompressionLevel` newtype; serverless PBC L2
    (brainstorm 010); L1 inline erasure coding
    (brainstorm 003). *Multi-quarter, several ADRs.*

15. **Refinement plumbing (opportunistic, no ordering
    constraint).** AGG follow-ups (float canonical,
    Decimal SUM/AVG, GROUP BY *(medium-large)*,
    string/binary quantiles, exact COUNT(DISTINCT),
    `COUNT(DISTINCT __row_key_id__)` shortcut, CSB
    visibility-metadata consolidation). Hilbert locality
    variant. Append-only `SingleRow` low-latency path.
    `IntraBatchIndex` widening to u64. Provider-native
    multi-range API in `ObjectStore`.
    `RecordBufferDirectory` cache.

## What not to do next

- **Don't bundle the CDC follow-up ADRs into one
  mega-ADR.** Each consumer of the ADR 054 substrate
  (filters, triggers, webhooks, continuous queries,
  durable cursor commit, multi-table firehose, before-
  images, event-history digest) has its own contract
  surface; bundling defers real decisions behind a larger
  one. Land them independently against the substrate.
- **Don't open LSM compaction without first thinking
  through the CDC live-tail interaction.** ADR 054's
  safe-emission watermark plus ring-continuity guard
  assume locality slabs do not get rewritten under an
  active subscription's catchup ceiling. LSM compaction
  needs an explicit policy: either compaction yields to
  active subscriptions, or `RING_OVERFLOW_DURING_CATCHUP`
  fires cleanly. Decide before shipping.
- **Don't promote stabilization watch items into roadmap
  items.** Stabilization watch — observability metrics,
  architectural-test guards, post-merge dashboards — lives
  in runbooks and tests, not as a roadmap feature.
  Decision-enabling measurement (e.g., the CDC watermark
  hold-time bench at multiple heartbeat cadences) belongs
  inside the owning ADR.
- **Don't re-open the topology gate without citing the
  rework budget recorded in brainstorm 015's closure
  status.** Speculative threat models, aesthetic
  preferences, and the desire to reuse curve machinery
  are not sufficient grounds. A re-open requires concrete
  verifier requirements or operational evidence that
  opaque-handle plus membership-bound signing forces more
  rework than a `NodeId`-shape migration.
- **Don't add a `CdcEventKind` enum to the substrate.**
  Consumer-friendly insert / update / delete / resurrection
  semantics layer on top in a future higher-level view ADR
  (#12 sub-bullet). The substrate stays storage-axis-
  aligned with the version-entry grain.
- **Don't expose `LocalityId` on the CDC cursor.** The
  identity-axis cursor is the load-bearing decision; the
  safe-emission watermark plus heartbeat contract is what
  makes global cursor monotonicity work without it.

## Convergence summary

```text
StateDigest substrate                         (ADR 046)
  → tombstones (validate algebra @ deletion)  (ADR 047)
  → retention (validate algebra @ compaction) (ADR 048)
  → topology gate CLOSED — Outcome B (opaque NodeId)
  → content-hash identity                     (ADR 049)
  → PK-keyed row delete via supersession      (ADR 050)
  → descriptor fingerprint split              (ADR 051)
  → secondary indexes                         (ADR 052)
  → wire-side Arrow IPC body compression      (ADR 053)
  → CDC subscription substrate                (ADR 054)  ← here
  → LSM (next: roadmap #11)
  → CDC follow-up ADRs (filters, triggers, webhooks, …)
  → vector embeddings
  → encoding / fetch / distributed compute
  → refinement plumbing
```

Design tip is past ADR 054 with the CDC subscription
substrate *promoted* (design accepted; implementation
phases pending). Next ADR is LSM-style leveled compaction
(#11), the first of the post-CDC large multi-ADR
sequences; ordering against the CDC follow-up cluster
(#12) is opportunistic — a CDC consumer ADR can land
ahead of LSM if the council prefers, since neither
blocks the other on shape.
