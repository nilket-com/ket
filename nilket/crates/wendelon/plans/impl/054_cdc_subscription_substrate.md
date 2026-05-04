# Implementation Plan: ADR 054 — CDC Subscription Substrate

Author: AI Swarm Council (Claude / Codex / Gemini)

Companion ADR: `plans/adr/054_cdc_subscription_substrate.md`

Inherits write-path bifurcation:
`plans/adr/003_stellarator_bifurcated_writes.md` (the
substrate's release gate).
Inherits row identity surface:
`plans/adr/045_pk_less_append_only_tables.md`,
`plans/adr/049_content_hash_row_identity.md`,
`plans/adr/050_pk_delete_supersession.md`.
Inherits tombstone surface:
`plans/adr/047_row_tombstones_append_only.md`.
Inherits retention contract:
`plans/adr/048_per_table_retention_policy.md`.
Inherits descriptor binding:
`plans/adr/051_descriptor_fingerprint_split.md`.
Inherits digest substrate:
`plans/adr/046_state_digest_substrate.md`.
Inherits multi-row record shape:
`plans/adr/042_analytical_multi_row_fragments.md`.
Inherits `RecordRowIndex` newtype widening (to `u32`):
`plans/adr/052_secondary_indexes.md`.
Inherits wire compression:
`plans/adr/053_wire_arrow_ipc_body_compression.md`.

## Goal

Land the CDC subscription substrate.

The implementation lets CDC drive:

- one cursor newtype `CdcCursor =
  (Hlc, RowKeyId, RecordRowIndex)` (POD, ordered, identity-
  axis; three newtype-unit fields);
- one `CdcSubscriptionId` newtype (`u128`, server-allocated,
  log/metric-correlation only);
- one in-memory per-locality `CdcLiveTailRing`
  (`src/storage/cdc.rs`); admission-time staging with
  `finalized = false`; finalization observer flips bit;
  pending-finalization cleanup on terminal failure;
- one per-`(tenant, table)` `CdcSubscriptionRegistry`
  (`src/network/cdc.rs`);
- four new wire `MsgType` constants
  (`CDC_SUBSCRIBE_REQ`, `CDC_FRAME`, `CDC_CHECKPOINT`,
  `CDC_REJECT`) and a long-lived QUIC stream RPC handler;
- `cdc_emit_frame()`, `cdc_emit_checkpoint()`,
  `cdc_emit_reject()` helpers in `src/network/cdc.rs` that
  route through ADR 053 `wire_encode_batch_for_path()` with
  the new `NetworkCompressionPath::CdcEvent` enumeration value;
- catchup via existing storage scan paths over retained
  source storage; full `CdcCursor` tuple bounds (not HLC
  ranges) over a safe global cut `snapshot_hlc =
  min(locality_finalized_hlc(l))`; open-time
  ring-continuity guard at handoff; live tail is gated by a
  safe-emission watermark `safe_emission_hlc =
  min(locality_finalized_hlc(l))` so global `CdcCursor`
  ordering holds under skewed locality finalization;
- cold-locality heartbeat workers (locality-scoped, not
  per-table): one `CdcLocalityHeartbeatWorker` per locality
  on the locality leader emitting a single no-op
  control-ring finalization marker per quiet interval at
  cadence `cdc_locality_heartbeat_interval_millis` (V1
  default 1000 ms), so the watermark does not stall behind
  an idle locality. Cluster cost: `N_localities /
  heartbeat_interval`;
- visible-domain `StateDigest` checkpoints
  (`digest.table.row.visible.v1`, ADR 046 V1 helper);
- bounded `cdc_*` metric family with the enumerations pinned
  by architectural test;
- 8-code `CDC_REJECT_*` taxonomy;
- audit lifecycle events (`CdcSubscriptionOpened`,
  `CdcSubscriptionClosed`);
- bench matrix that distinguishes admission-overhead from
  live-tail latency from catchup throughput from
  ring-overflow threshold from checkpoint cost.

## Deliverables

- `CdcCursor`, `CdcSubscriptionId`, `CdcSubscribeBound`,
  `CdcSubscribeRequestMeta`, `CdcFrameMeta`,
  `CdcCheckpointMeta`, `CdcRejectMeta`, `CdcRejectCode`,
  `CdcFrameSource` in `src/schema/cdc.rs` (new). POD where
  applicable; newtype-unit where applicable.
- `CdcLiveTailRing`, `CdcLiveEntry`, finalization release-gate
  observer, pending-finalization cleanup integration in
  `src/storage/cdc.rs` (new).
- `CdcSubscriptionRegistry`, `SubscriptionHandle`,
  `cdc_emit_frame`, `cdc_emit_checkpoint`, `cdc_emit_reject`
  in `src/network/cdc.rs` (new).
- `handle_cdc_subscribe` long-lived stream handler in
  `src/network/data_server.rs`.
- Client API: `Client::cdc_subscribe`,
  `CdcSubscription::next_frame` in `src/client/core.rs`.
- New `MsgType` constants in `src/schema/core.rs` (exactly
  four).
- New `NetworkCompressionPath::CdcEvent` in
  `src/network/ipc.rs` (extends ADR 053 path-label
  enumeration as the fifth allowed value).
- Admission-time stage hooks at every `__row_key_id__`-emitting
  call site: `table_write` admission, `pk_delete` admission
  (ADR 050), append-only admission (ADR 045), content-hash
  admission post-collapse-decision (ADR 049 — collapsed
  duplicates do *not* stage), multi-row admit (ADR 042).
- Finalization observer wired into the `FinalizationCommitted`
  / `FinalizationTerminalFailure` paths from ADR 003.
- `cdc_*` metric family in `src/metrics/registry.rs` with
  bounded label enumerations.
- Audit event kinds in `src/audit/events.rs`.
- Architectural guards — listed in Phase 10.
- Golden vectors covering:
  - cursor lex-ordering;
  - HLC tie broken by `RowKeyId`;
  - `(Hlc, RowKeyId)` tie broken by `RecordRowIndex`;
  - PK insert / update / delete / resurrection sequence;
  - append-only insert / tombstone sequence;
  - content-hash collapsed-write produces no staged entry
    (positive guard);
  - multi-row record produces N staged entries with
    `record_row_index ∈ [0, N)`;
  - staged entry is not visible to subscribers before
    finalization (positive guard);
  - finalized entry emits to live subscriber within bounded
    time;
  - terminal-failure entry is discarded by pending-
    finalization cleanup; subscribers never observe it;
  - ring overflow drops oldest staged entries; metric
    increments;
  - catchup over `CdcCursor::MIN` reads retained history;
  - catchup-to-live handoff produces no duplicates / no gaps
    under concurrent admissions;
  - ring overflow during catchup → reject;
  - retention floor advance during live → reject;
  - StateDigest checkpoint at cursor matches independent
    visible-domain digest at the same HLC;
  - `SnapshotAtOpen` bound: subscription closes after
    catchup reaches `(snapshot_hlc, RowKeyId::MAX,
    RecordRowIndex::MAX)` with a final empty `CDC_FRAME`
    carrying `STREAM_END`;
  - skewed-locality global ordering: under independent
    locality finalization, the emitted cursor sequence on
    one subscription is non-decreasing in `CdcCursor` lex
    order (Phase 5 regression test);
  - cold-locality watermark heartbeat: events on an active
    locality emit with bounded latency even when one source
    locality is completely idle (Phase 5 regression test);
  - open-time ring-continuity guard: pre-open eviction of
    entries above the catchup ceiling rejects at open with
    `RING_OVERFLOW_DURING_CATCHUP` rather than silently
    dropping events (Phase 5 regression test);
  - same-HLC suffix included by full-tuple scan bound
    (Phase 5 regression test);
  - subscription cancel via QUIC stream close releases
    registry entry within bounded time;
  - admission throughput with 100 active subscribers
    regresses by ≤ 5% (positive performance guard);
  - schema fingerprint per-frame; additive evolution mid-
    stream causes per-frame fingerprint to change; consumer
    re-fetch path works;
  - non-additive evolution (synthesized) → typed reject;
  - each `CDC_REJECT_*` code exercised end-to-end.
- Bench: live-tail latency, catchup throughput, ring-overflow
  threshold, checkpoint cost, admission-isolation
  (`benches/cdc_subscription.rs`).
- Operator runbook at
  `plans/ops/runbooks/cdc_subscription.md`.

## Non-goals

- No PBC filter pushdown.
- No triggers (sync or post-commit).
- No `TriggerCatalog` / `SchemaObjectKind::Trigger`.
- No webhook worker / `__webhook_events__` system table.
- No continuous historical-plus-live query API.
- No durable CDC event log.
- No new `StateDigest` domain.
- No `CdcEventKind` / `CdcOperation` enum at substrate.
- No before-image CDC payloads.
- No exactly-once delivery.
- No multi-table / tenant-firehose subscription scope.
- No column projection on subscriptions.
- No cross-replica cursor handoff.
- No consumer-driven checkpoint cadence.
- No per-event audit.
- No metadata / catalog CDC.
- No new node capability bit.
- No HRFT envelope `format_version` bump.
- No new `FRAME_FLAG_*` constant for CDC.
- No new wire compression codec.
- No new storage compression codec.
- No `ObjectStore` trait change.
- No reserved enum slots, compatibility shims, or `V1` /
  `V2` Rust type suffixes.
- No project-name prefix on new symbols.
- No high-cardinality metric labels.
- No new Cargo feature or dependency.
- No on-disk format change.

## Phase 0 — Contract freeze, fixtures, architectural guards

Lock the cursor, the wire `MsgType` set, the live-tail ring
shape, the admission stage hook surface, the finalization
release gate, the reject taxonomy, the metric family
enumerations, the architectural guards, and the open-question
defaults before touching the RPC handler or admission paths.

Files:

- `src/schema/cdc.rs` (new — POD types, reject codes)
- `src/schema/core.rs` (`MsgType` extension)
- `src/storage/cdc.rs` (new — ring shape only)
- `src/network/cdc.rs` (new — registry shape only)
- `src/network/ipc.rs` (path-label enumeration extension)
- `src/metrics/registry.rs` (new metric family)
- `tests/fixtures/cdc_subscription/` (new)
- `tests/architecture_test.rs`

Work:

1. Freeze the cursor type:

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

   Three newtype-unit fields:
   - `Hlc` matches the existing HLC newtype (column
     `__row_hlc__`).
   - `RowKeyId` matches the existing 32-byte row-identity
     newtype (column `__row_key_id__`).
   - `RecordRowIndex` is the existing newtype ADR 052 §6
     widened to `u32`; reused here.

   `Ord` derive yields lex ordering; verified via Phase 1
   golden test.

2. Freeze `CdcSubscriptionId` as a `u128` newtype-unit.
   Server-allocated; not durable; not consumer-resumable.

3. Freeze the POD wire structs per ADR 054 §7. All meta
   structs are `#[repr(C)]`, `Pod + Zeroable`; explicit
   padding fields make alignment deterministic across
   platforms. **Sparse enums (`CdcSubscribeBound`,
   `CdcFrameSource`, `CdcRejectCode`, `DigestDomain`) are
   *not* `Pod`** — most discriminant bit patterns are
   invalid, so a `Pod` derive would be unsound. The meta
   structs carry raw integer fields (`bound_raw: u8`,
   `source_raw: u8`, `code_raw: u32`,
   `digest_domain_raw: u32`), and the engine decodes via
   checked accessors (`CdcSubscribeBound::from_raw`,
   `CdcFrameSource::from_raw`, etc.). Reject-code
   discriminants start at 1 so a zero-initialized
   `CdcRejectMeta` does not accidentally read as a valid
   reject. `CdcSubscribeRequestMeta` carries
   `resume_digest_present: u8` and
   `resume_visible_state_digest: StateDigest` so resume-time
   digest validation can fire at open rather than after the
   first checkpoint. A decode failure (unknown discriminant)
   is a stream-level reject server-side and an error return
   on `next_frame` client-side.

4. Freeze the **wire MsgType set**. Add exactly four
   constants:

   ```rust
   pub const MSG_TYPE_CDC_SUBSCRIBE_REQ: MsgType = MsgType::new(...);
   pub const MSG_TYPE_CDC_FRAME: MsgType = MsgType::new(...);
   pub const MSG_TYPE_CDC_CHECKPOINT: MsgType = MsgType::new(...);
   pub const MSG_TYPE_CDC_REJECT: MsgType = MsgType::new(...);
   ```

   Phase 0 audit reads the existing MsgType allocation table
   in `src/schema/core.rs` and pins the next four unused
   numeric values; record them in a comment alongside the
   constants. `HRFT_ENVELOPE_VERSION` unchanged. No new
   `FRAME_FLAG_*` constant.

5. Freeze the **reject taxonomy**:

   ```rust
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

   impl CdcRejectCode {
       pub fn from_raw(raw: u32) -> Result<Self, CdcDecodeError> {
           match raw {
               1 => Ok(Self::BelowRetentionFloor),
               2 => Ok(Self::RingOverflowDuringCatchup),
               3 => Ok(Self::SchemaFingerprintIncompatible),
               4 => Ok(Self::CheckpointDigestMismatch),
               5 => Ok(Self::UnknownTable),
               6 => Ok(Self::TenantAuthDenied),
               7 => Ok(Self::TableNotReplicatedHere),
               8 => Ok(Self::InternalEngineError),
               _ => Err(CdcDecodeError::InvalidRejectCode(raw)),
           }
       }
   }
   ```

   Exactly eight V1 codes; architectural test pins the set.
   No reserved slots. Discriminants start at 1 (not 0) so a
   zero-initialized `CdcRejectMeta` cannot accidentally read
   as a valid reject. The enum does **not** derive `Pod` —
   sparse u32 enums cannot soundly satisfy `Pod`'s
   "every-bit-pattern-valid" contract. The wire-layer
   `code_raw: u32` field on `CdcRejectMeta` is the POD
   carrier; `from_raw` is the receive-side decode.

6. Freeze the new `NetworkCompressionPath::CdcEvent` value and
   update ADR 053's path-label enumeration in
   `src/network/ipc.rs` and the architectural test:

   ```text
   client_batch
   migration_records
   query_response
   read_response
   cdc_event
   ```

7. Freeze the **live-tail ring shape**. V1 default
   `max_cdc_live_tail_bytes_per_locality = 64 MiB`
   (deployment-profile-tunable per ADR 030):

   ```rust
   pub const DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY: u64 = 64 * 1024 * 1024;

   pub struct CdcLiveTailRing {
       bytes_cap: u64,                 // max_cdc_live_tail_bytes_per_locality
       entries: VecDeque<CdcLiveEntry>,
       head_cursor: CdcCursor,         // monotonic; entries.front().cursor
       tail_cursor: CdcCursor,         // monotonic; entries.back().cursor
   }

   pub struct CdcLiveEntry {
       pub cursor: CdcCursor,
       pub tenant_id: TenantId,
       pub table_object_id: TableObjectId,
       pub logical_schema_fingerprint: LogicalSchemaFingerprint,
       pub record_batch: Arc<RecordBatch>,
       pub finalized: AtomicBool,
   }
   ```

   The ring is per-locality, lifetime-bound to the locality
   leader's admission state. On leadership transfer the ring
   is dropped (subscriptions reconnect to the new leader).
   FIFO eviction on byte-cap overflow.

7a. Freeze the **cold-locality heartbeat cadence**. V1 default
    `cdc_locality_heartbeat_interval_millis = 1000`
    (deployment-profile-tunable per ADR 030):

    ```rust
    pub const DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS: u64 = 1000;
    ```

    The heartbeat is **locality-scoped, not per-table**.
    Each locality runs *one* heartbeat worker (Phase 5 /
    step 1b) on the locality leader; that worker emits a
    single no-op finalization marker on the control ring
    whenever `locality_finalized_hlc(l)` has not advanced
    within the cadence window. The marker carries no row
    payload and no table identity; one marker advances the
    one per-locality `locality_finalized_hlc(l)` value,
    which feeds the watermark for every table whose source
    localities include `l`. Cluster cost is `N_localities /
    heartbeat_interval`, independent of CDC-table count.
    This is a substrate requirement on control-ring
    behavior; ADR 054 introduces no new wire surface for the
    heartbeat itself.

8. Freeze the **finalization release-gate observer**:

   ```rust
   pub trait FinalizationObserver {
       fn on_finalized(&self, finalization_log_index: LogIndex);
       fn on_terminal_failure(&self, finalization_log_index: LogIndex);
   }

   pub struct CdcFinalizationObserver {
       ring: Arc<CdcLiveTailRing>,
       pending_index: Mutex<HashMap<LogIndex, Vec<CursorRef>>>,
   }
   ```

   `on_finalized` flips `finalized = true` on every staged
   entry whose admission was associated with the given
   `finalization_log_index`. `on_terminal_failure` evicts
   those staged entries from the ring (pending-finalization
   cleanup). The observer hooks into the existing ADR 003
   pending-finalization machinery; ADR 054 does not introduce
   a new pending-finalization path.

   The internal `LogIndex → CursorRef` map is the only place
   the substrate touches finalization log indexes; consumers
   never see them.

9. Freeze the **registry shape**:

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

   Per-subscription `mpsc::Sender` with bounded capacity. On
   reader cancel (QUIC stream close), the writer task drops
   the receiver; the next admit's `try_send` sees
   `SendError::Closed` and removes the handle.

10. Freeze the **metric family** with bounded enumerations
    (ADR 052 / ADR 053 discipline carries through):

    ```text
    cdc_subscriptions_total{result}
    cdc_events_emitted_total{source}
    cdc_event_bytes_emitted_total{source}
    cdc_checkpoints_emitted_total{source}
    cdc_rejects_total{code}
    cdc_subscription_lag_micros{source}
    cdc_ring_overflow_total{}
    cdc_catchup_to_live_handoff_micros{}
    cdc_staged_entries_in_ring{}
    cdc_pending_finalization_evicted_total{}
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

    No tenant, table, peer, subscription id, schema
    fingerprint, or error-string labels.

11. Architectural guards (TODO entries; populated by Phase
    10):

    - `CdcCursor` is `Pod + Zeroable + Ord`; lex order is
      `(hlc, row_key_id, record_row_index)`.
    - `CdcCursor` fields are `Hlc`, `RowKeyId`,
      `RecordRowIndex` newtypes — no raw `u64` / `[u8; 32]` /
      `u32`.
    - `CdcSubscriptionId` is a `u128` newtype.
    - `CdcRejectCode` enumerates exactly 8 V1 variants.
    - Exactly four new `MsgType` constants:
      `CDC_SUBSCRIBE_REQ`, `CDC_FRAME`, `CDC_CHECKPOINT`,
      `CDC_REJECT`.
    - `cdc_emit_frame` routes through
      `wire_encode_batch_for_path()` with
      `NetworkCompressionPath::CdcEvent` (positive guard).
    - `cdc_event` is added to ADR 053's path-label
      enumeration in `src/network/ipc.rs`.
    - ADR 053's path-label architectural guard accepts
      `cdc_event` as the fifth allowed value.
    - No `WendelonCdc*` brand-leaked symbol. Construct
      forbidden needle via `concat!("Wendelon", "Cdc")`.
    - No `CdcCursorV[0-9]` / `CdcSubscriptionV[0-9]` /
      `CdcOperationV[0-9]` suffix.
    - No `CdcEventKind` / `CdcOperation` enum exists.
      Construct forbidden needles via
      `concat!("Cdc", "EventKind")` and
      `concat!("Cdc", "Operation")`.
    - No new `FRAME_FLAG_*` constant. Construct forbidden
      needle via `concat!("FRAME_FLAG_", "CDC")`.
    - No new `NodeCapability` variant.
      `NodeCapability::ALL.len()` unchanged from `main`.
    - `HRFT_ENVELOPE_VERSION` unchanged from `main`.
    - No Cargo feature change.
    - No new digest domain. The substrate references
      `digest.table.row.visible.v1` only. Construct forbidden
      needle via `concat!("digest.cdc.", "event.v1")`.
    - No `TriggerObjectId` / `TriggerCatalog` /
      `SchemaObjectKind::Trigger` / `TriggerDescriptor` (ADR
      054 V1 deferral).
    - No `SchemaObjectKind::Webhook`.
    - Reject namespace is `CDC_REJECT_*`. Construct forbidden
      needle via `concat!("WENDELON_CDC_REJECT_")`.
    - Metric prefix is `cdc_*`. Construct forbidden needle
      via `concat!("wendelon_cdc_", "subscriptions_")`.
      (Repo-level Prometheus namespace adds `wendelon_`
      downstream; the protocol prefix is `cdc_*`.)
    - Metric label `source` enumerates exactly
      `{ring, catchup}`.
    - Metric label `result` enumerates exactly
      `{opened, closed, rejected, cancelled}`.
    - Metric label `code` enumerates exactly the 8
      `CdcRejectCode` snake-case values.
    - Metric labels do **not** include `tenant_id` /
      `table_object_id` / peer / subscription_id / schema /
      error-string. Forbidden-literal scan.
    - Content-hash collapsed-write path does **not** stage a
      ring entry (positive guard via golden test).
    - Multi-row admit stages N entries for an N-row record
      with `record_row_index ∈ [0, N)`.
    - Staged entries are invisible to subscribers until
      `finalized = true` (positive guard via golden test).
    - Pending-finalization cleanup evicts staged entries on
      terminal failure; subscribers never observe them.
    - Compress-then-encrypt order preserved (CDC inherits
      ADR 053).

Behaviour fixtures under `tests/fixtures/cdc_subscription/`:

- Cursor monotonicity and lex-ordering golden.
- HLC tie broken by `RowKeyId`.
- `(Hlc, RowKeyId)` tie broken by `RecordRowIndex`.
- PK insert / update / delete / resurrection sequence;
  expected cursor sequence and row-axis columns.
- Append-only insert / tombstone sequence.
- Content-hash collapse: write twice with same content;
  expected zero staged entries for the second write.
- Multi-row record: one record with N rows produces N
  staged entries with `record_row_index ∈ [0, N)`.
- Staged-but-not-finalized entry is invisible to subscriber.
- Finalized entry emits.
- Terminal-failure entry is evicted; subscriber never
  observes.
- Ring overflow drops oldest; metric increments.
- Schema fingerprint additive evolution: schema bumps
  mid-stream; per-frame `LogicalSchemaFingerprint` reflects
  the change.
- Reject path samples for each `CDC_REJECT_*` code.
- StateDigest checkpoint at cursor matches independent
  `digest.table.row.visible.v1` accumulation.

Exit:

- Contract pinned; types stable; fixtures fail with
  `unimplemented!()` until later phases.
- Architectural guards in place (skeletons; populated by
  Phase 10).

## Phase 1 — Cursor, frame schema, and golden fixtures

Files:

- `src/schema/cdc.rs`
- `tests/cdc_cursor_ordering.rs` (new)
- `tests/cdc_frame_schema.rs` (new)

Work:

1. Implement `CdcCursor` per Phase 0 §1 with derived `Ord` and
   a `From<&CdcLiveEntry>` extraction helper.

2. Implement `CdcSubscriptionId` per Phase 0 §2.

3. Implement `CdcRejectCode` per Phase 0 §5.

4. Implement the CDC frame Arrow schema builder. The payload
   schema is the source table user columns plus row-axis
   synthetic columns:

   ```text
   __row_key_id__
   __row_hlc__
   __row_tombstone__
   __row_table_object_id__
   __row_routing_locality__   (when present per ADR 045 / 050)
   __row_idx__                (RecordRowIndex per ADR 052 §6)
   __supersedes_hlc__         (when present per ADR 050 §7)
   <source user columns…>
   ```

   The schema builder takes a source `LogicalSchemaFingerprint`
   plus the source logical schema and returns an Arrow
   `Schema` for the CDC event row.

5. Tests:

   - Cursor lex-ordering golden (HLC tie → row-key tie →
     record-row-index tie).
   - Schema builder produces a schema with all expected
     synthetic columns plus user columns.
   - `CdcCursor::MIN` is the strict minimum.
   - Two cursors with identical `(hlc, row_key_id)` but
     different `record_row_index` order correctly.

Exit:

- Cursor and event payload shape are test-pinned.

## Phase 2 — Live-tail ring and admission stage hooks

Files:

- `src/storage/cdc.rs`
- `src/network/data_server.rs`
- `src/storage/multirow.rs`
- `src/storage/content_hash.rs` (or wherever ADR 049 admission
  lives)
- `src/storage/pk_delete.rs` (or wherever ADR 050 admission
  lives)
- `tests/cdc_live_tail.rs` (new)
- `tests/cdc_admission_stage.rs` (new)

Work:

1. Implement `CdcLiveTailRing` per Phase 0 §7 with FIFO
   byte-bound eviction. The ring is owned per-locality,
   lifetime bound to the locality leader's admission state.

2. Add `cdc_emit_admitted` stage hook calls at every
   admission path that produces a version-entry. Survey via:

   ```sh
   rg -n '__row_key_id__|__row_hlc__|admit_version|VersionEntry|FinalizationCommitted' src
   ```

   Classify every match into one of:
   - **`stages-cdc`** — produces a version-entry; stages a
     ring entry. V1 known sites:
     - `data_server.rs::handle_table_write` (via the
       admission outcome path)
     - `pk_delete.rs::admit` (ADR 050)
     - `append_only.rs::admit` (ADR 045)
     - `content_hash.rs::admit` — stages **only** for the
       collapse winner; collapsed duplicates do **not**
       stage
     - `multirow.rs::admit_multirow` — stages N times for an
       N-row record
   - **`metadata-only`** — admits descriptor / catalog
     mutations; does *not* stage.
   - **`internal-rebuild`** — compaction, secondary-index
     backfill, etc.; does *not* stage.
   - **`test-or-bench`** — fixtures; classified separately.

   Architectural test (Phase 10) pins the classification.

3. Stage entries with `finalized = false`. The stage hook
   wraps the decoded `Arc<RecordBatch>` into a
   `CdcLiveEntry`, computes the `CdcCursor`, and pushes onto
   the locality's ring. The ring is **shared per-locality
   across tables** — it's keyed by source locality, not by
   `(tenant, table)`. The fan-out registry filters entries by
   `(tenant_id, table_object_id)` at drain time.

4. Pending-finalization map: stage hooks also record
   `(finalization_log_index, cursor)` into the observer's
   pending map so finalization commit / failure can find the
   ring entries.

5. Ring overflow:
   - on byte-cap overflow, drop the oldest entries
     (regardless of finalized state);
   - increment `cdc_ring_overflow_total{}`;
   - dropped staged-but-not-finalized entries: pending-
     finalization cleanup later finds them missing and
     records a `cdc_pending_finalization_evicted_total{}`
     bump on the eviction path. (V1 audit grain: this is a
     count, not a list.)

6. Tests:

   - PK insert: hook called once; ring carries an entry with
     `finalized = false`.
   - Append-only insert: hook called once.
   - PK delete: hook called once; entry has
     `__row_tombstone__ = true`.
   - PK resurrection: tombstone-then-data sequence stages two
     entries; resurrection's data event has
     `__supersedes_hlc__` set.
   - Append-only tombstone: hook called once;
     `__row_tombstone__ = true`.
   - Content-hash collapsed write: hook called *zero* times
     for the collapsed duplicate (positive guard).
   - Multi-row record: N rows produce N staged entries with
     `record_row_index ∈ [0, N)`.
   - Ring overflow: staged entries beyond byte cap drop;
     metric increments.

Exit:

- Live-tail ring populated by every version-entry-producing
  admission path.
- Staged entries carry `finalized = false`; not yet released.

## Phase 3 — Finalization release gate

Files:

- `src/storage/cdc.rs` (`CdcFinalizationObserver`)
- `src/consensus/raft_storage.rs` or
  `src/storage/manager.rs` (FinalizationCommitted /
  FinalizationTerminalFailure hooks)
- `tests/cdc_finalization_release.rs` (new)

Work:

1. Implement `CdcFinalizationObserver` per Phase 0 §8.

2. Hook the observer into ADR 003's existing finalization
   paths:
   - `FinalizationCommitted` → `observer.on_finalized`
   - `FinalizationTerminalFailure` →
     `observer.on_terminal_failure`
   - `FinalizationRecoverablePending` is intentionally
     unhandled at this layer; recovery either succeeds (next
     leader's finalization commit) or fails (terminal
     failure path).

3. `on_finalized(log_index)`:
   - look up cursor refs in the pending map;
   - for each ring entry, atomic CAS `finalized: false →
     true`;
   - if the locality's `locality_finalized_hlc` advanced as
     a result, fire the per-table `safe_emission_wake`
     `Notify` so every subscriber's drain task re-evaluates
     `safe_emission_hlc(table)` and releases any entries
     that cleared the new watermark (Phase 4 / step 3a);
   - notify subscribers (registry fan-out) of the newly
     released cursors;
   - remove entries from the pending map.

4. `on_terminal_failure(log_index)`:
   - look up cursor refs in the pending map;
   - for each ring entry, evict from the ring (the entry
     never released);
   - increment `cdc_pending_finalization_evicted_total{}`;
   - remove entries from the pending map.

5. Tests:

   - Staged entry pre-finalization: ring contains entry with
     `finalized = false`; subscribers do not see it.
   - Finalized entry below watermark: `on_finalized` flips
     the bit; `safe_emission_hlc` already covers it; the
     subsequent drain emits to subscribers.
   - Finalized entry above watermark: `on_finalized` flips
     the bit, but a lagging locality's frontier holds
     `safe_emission_hlc` below the entry's HLC; the entry
     stays in the ring with `finalized = true` and is **not**
     emitted. Once the lagging locality advances past the
     entry's HLC, the next `on_finalized` wakes the drain
     task and the entry is emitted.
   - Terminal failure: `on_terminal_failure` evicts the
     entry; subscribers never see it; metric increments.
   - Recoverable-pending: no observer call; entry stays
     staged until next leader's finalization or timeout.
   - Bounded-time release: `on_finalized` to subscriber
     drain ≤ X ms when the watermark already covers the
     entry (Phase 9 bench).
   - Multiple entries in one finalization batch all release
     together provided the watermark covers them.

Exit:

- ADR 003 finalization composes correctly with the substrate;
  staged entries release in lockstep with finalization commit;
  failed admissions never reach subscribers.

## Phase 4 — Wire surface (subscription open + frame emit)

Files:

- `src/schema/core.rs` (MsgType constants)
- `src/network/data_server.rs` (`handle_cdc_subscribe`)
- `src/network/cdc.rs` (`cdc_emit_frame`, `cdc_emit_reject`)
- `src/network/ipc.rs` (path-label enumeration extension)
- `src/client/core.rs` (subscription client API)
- `tests/cdc_subscribe_open.rs` (new)
- `tests/cdc_frame_round_trip.rs` (new)

Work:

1. Add MsgType constants per Phase 0 §4.

2. Extend ADR 053's path-label enumeration with `cdc_event`.

3. Implement `handle_cdc_subscribe`:

   ```rust
   async fn handle_cdc_subscribe(
       self,
       req: CdcSubscribeRequestMeta,
       quic_writer: SendStream,
   ) -> Result<()> {
       // 1. Auth: tenant + table identity check.
       //    Reject UNKNOWN_TABLE / TENANT_AUTH_DENIED.
       // 2. Lookup table descriptor; validate
       //    req.logical_schema_fingerprint vs current.
       //    Reject SCHEMA_FINGERPRINT_INCOMPATIBLE on mismatch.
       // 3. Lookup retention floor via TableLifecycleCatalog (ADR 048).
       //    If req.start_cursor.row_hlc < floor (and start != MIN),
       //    reject BELOW_RETENTION_FLOOR.
       // 4. Check that the table is locally replicated.
       //    Else reject TABLE_NOT_REPLICATED_HERE.
       // 5. If req.resume_digest_present != 0, compute
       //    digest.table.row.visible.v1 at req.start_cursor.row_hlc
       //    and compare to req.resume_visible_state_digest.
       //    Mismatch → reject CHECKPOINT_DIGEST_MISMATCH at open.
       // 6. Allocate CdcSubscriptionId; capture safe global
       //    cut (Phase 5):
       //      snapshot.snapshot_hlc = min(locality_finalized_hlc(l)).
       // 6a. Open-time ring-continuity guard. For each source
       //     locality l with a non-empty ring, assert
       //       ring.head_cursor[l] <= (snapshot.snapshot_hlc,
       //                                RowKeyId::MAX,
       //                                RecordRowIndex::MAX).
       //     Empty rings skip the check. Violation rejects
       //     CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP at open.
       // 7. Register handle in registry.
       // 8. Spawn writer task: drain mpsc::Receiver via
       //    cdc_emit_frame, gated by the safe-emission
       //    watermark (next step); handle backpressure via
       //    QUIC flow control.
       // 9. Catchup loop (Phase 5) using full-cursor bounds:
       //    cursor > req.start_cursor and
       //    cursor <= (snapshot.snapshot_hlc, RowKeyId::MAX,
       //    RecordRowIndex::MAX); cross-locality min-heap merge.
       // 10. Live-tail handoff at (snapshot.snapshot_hlc,
       //     RowKeyId::MAX, RecordRowIndex::MAX), gated by
       //     safe_emission_hlc() so global cursor monotonicity
       //     holds under skewed locality finalization.
       // 11. On bound = SnapshotAtOpen, terminate after catchup
       //     with FRAME_FLAG_STREAM_END on a final empty CDC_FRAME.
       // 12. On any error, emit CDC_REJECT and close.
   }
   ```

3a. **Wire the safe-emission watermark.** The writer task's
    drain loop pulls a finalized live-tail entry, then checks
    `entry.cursor.row_hlc <= safe_emission_hlc(table)` before
    encoding. Holding entries:

    ```rust
    async fn drain_live_tail(
        ring: Arc<CdcLiveTailRing>,
        table: Arc<Table>,
        out: mpsc::Sender<CdcLiveEntry>,
        wake: Arc<Notify>,
    ) {
        loop {
            let watermark = safe_emission_hlc(&table);
            while let Some(entry) = ring.peek_finalized() {
                if entry.cursor.row_hlc <= watermark {
                    let entry = ring.pop_finalized();
                    if out.send(entry).await.is_err() { return; }
                } else {
                    break; // wait for watermark advance
                }
            }
            wake.notified().await;
        }
    }
    ```

    `wake` is notified by the locality finalization observer
    (Phase 3) whenever any locality's
    `locality_finalized_hlc` advances. The drain loop
    re-evaluates `safe_emission_hlc(&table)` and releases any
    entries that cleared the new watermark, in `CdcCursor`
    lex order across localities (the ring's `peek_finalized`
    yields the smallest finalized cursor across all
    localities).

    Catchup output is **not** watermark-gated: every catchup
    row has `row_hlc <= snapshot.snapshot_hlc`, and by
    construction every locality has finalized through
    `snapshot.snapshot_hlc`, so the watermark is already
    satisfied for every catchup row.

4. Implement `cdc_emit_frame`:

   ```rust
   pub async fn cdc_emit_frame(
       writer: &mut SendStream,
       meta: &CdcFrameMeta,
       batch: &RecordBatch,
   ) -> std::io::Result<()> {
       // 1. wire_encode_batch_for_path(batch,
       //    NetworkCompressionPath::CdcEvent)?
       //    — registers cdc_event path metric (ADR 053).
       // 2. Build HRFT frame: header (MsgType::CDC_FRAME)
       //    + meta (Pod copy) + arrow_payload.
       // 3. writer.write_all(&frame).
   }
   ```

5. Implement `cdc_emit_reject`:

   ```rust
   pub async fn cdc_emit_reject(
       writer: &mut SendStream,
       meta: &CdcRejectMeta,
   ) -> std::io::Result<()> {
       // No arrow_payload; just header + meta.
       // After writing, the QUIC stream closes; this is a
       // terminal frame.
   }
   ```

6. Implement client-side subscription API in
   `src/client/core.rs`:

   ```rust
   pub struct CdcSubscription { ... }

   impl Client {
       pub async fn cdc_subscribe(
           &self,
           tenant_id: TenantId,
           table_object_id: TableObjectId,
           start_cursor: CdcCursor,
           bound: CdcSubscribeBound,
           logical_schema_fingerprint: LogicalSchemaFingerprint,
           resume_digest: Option<StateDigest>,
       ) -> Result<CdcSubscription>;
   }

   impl CdcSubscription {
       pub async fn next_frame(&mut self) -> Result<CdcFrame>;
       // CdcFrame ∈ {Frame{meta, batch}, Checkpoint{meta},
       //             Reject{meta}, End}
   }
   ```

7. Tests:

   - Subscribe open with valid fingerprint: receives first
     frame within bounded time.
   - Frame round-trip: encode `CdcFrameMeta` + RecordBatch
     on server; decode on client; identical payload.
   - Subscribe with cursor below retention floor: receive
     `BELOW_RETENTION_FLOOR`.
   - Subscribe with stale logical fingerprint: receive
     `SCHEMA_FINGERPRINT_INCOMPATIBLE`.
   - Subscribe to non-existent table: receive `UNKNOWN_TABLE`.
   - Subscribe to wrong-tenant table: receive
     `TENANT_AUTH_DENIED`.
   - Subscribe to remote-only table: receive
     `TABLE_NOT_REPLICATED_HERE`.
   - Subscribe with `bound = SnapshotAtOpen`: stream
     terminates with `FRAME_FLAG_STREAM_END`.
   - Subscribe with valid `resume_visible_state_digest` matching
     retained visible state at `start_cursor.row_hlc`: open
     succeeds.
   - Subscribe with mismatched
     `resume_visible_state_digest`: receive
     `CHECKPOINT_DIGEST_MISMATCH` *at open* (before any
     `CDC_FRAME` arrives).
   - Subscribe with `resume_digest_present = 0`: server skips
     digest validation; open succeeds without a digest read.
   - Client cancel via QUIC stream close: server registry
     entry released within bounded time
     (probe via `cdc_subscriptions_total{result=cancelled}`).
   - Frame carries Arrow IPC body compression
     (positive guard: emit decode reads
     `BodyCompression { codec = LZ4_FRAME }`).

Exit:

- Wire surface live; clients can subscribe and receive
  frames.
- Live tail is wired (Phase 2/3 stage + release →
  registry → channel → writer task → frame emit).
- Catchup is *not* wired yet; subscriptions with
  `start_cursor < (snapshot_hlc, RowKeyId::MAX,
  RecordRowIndex::MAX)` see only post-snapshot live-tail
  events (gated by the safe-emission watermark).

## Phase 5 — Catchup storage scan and handoff

Files:

- `src/network/data_server.rs` (catchup loop)
- `src/storage/cdc.rs` (catchup scan helper)
- `tests/cdc_catchup.rs` (new)

Work:

1. Capture a **safe global cut** at subscription open. Per
   ADR §6, the safe cut is the *minimum* finalized HLC across
   all source localities (not the maximum and not per-locality
   frontiers — both shapes lose either correctness or global
   ordering):

   ```rust
   pub struct CatchupSnapshot {
       /// snapshot_hlc = min(locality_finalized_hlc(l)) for l
       /// in source_localities(table) at subscribe-open time.
       /// Defines the upper end of the catchup window.
       pub snapshot_hlc: Hlc,
   }

   fn capture_snapshot(table: &Table) -> CatchupSnapshot {
       let snapshot_hlc = table
           .source_localities()
           .map(|l| l.locality_finalized_hlc())
           .min()
           .unwrap_or(Hlc::MIN);
       CatchupSnapshot { snapshot_hlc }
   }
   ```

   Per-locality HLC monotonicity (ADR 016) guarantees that no
   locality can later admit an event with `row_hlc <=
   snapshot_hlc` that isn't already in storage; the cut is
   complete.

1a. **Open-time ring-continuity guard.** Catchup ends at
    `(snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)` and
    live tail picks up strictly above it; this only works if
    every finalized entry above the ceiling is still in the
    ring at open. The bounded ring may have already evicted
    some. Run the check immediately after `capture_snapshot`,
    before registering the subscription handle:

    ```rust
    fn open_time_ring_continuity_check(
        ring: &CdcLiveTailRing,
        snapshot: &CatchupSnapshot,
    ) -> Result<(), CdcRejectCode> {
        let ceiling = CdcCursor {
            row_hlc: snapshot.snapshot_hlc,
            row_key_id: RowKeyId::MAX,
            record_row_index: RecordRowIndex::MAX,
        };
        for l in ring.source_localities() {
            if let Some(head) = ring.head_cursor(l) {
                if head > ceiling {
                    return Err(CdcRejectCode::RingOverflowDuringCatchup);
                }
            } // empty ring: skip
        }
        Ok(())
    }
    ```

    Violation emits
    `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP` at open before
    any frame is sent. Same code as the mid-catchup overflow
    path; operationally the two are the same situation
    (ring too small for current write rate vs catchup
    ceiling).

1b. **Cold-locality heartbeat contract for
    `locality_finalized_hlc`.** The watermark `min(...)` over
    locality frontiers can stall on a cold locality whose
    `locality_finalized_hlc` never advances. V1 commits to a
    **locality-scoped (not per-table) heartbeat**: every
    locality runs *one* heartbeat worker on the locality
    leader; that worker emits *one* no-op finalization
    marker per quiet interval at cadence
    `cdc_locality_heartbeat_interval_millis` (V1 default
    1000 ms). `locality_finalized_hlc(l)` is a single
    per-locality value; one marker advances it for every
    table whose source localities include `l`.

    Implementation (locality-scoped):

    ```rust
    pub const DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS: u64 = 1000;

    pub struct CdcLocalityHeartbeatWorker {
        locality_id: LocalityId,
        interval: Duration,
        // No table_object_id field. The worker is locality-
        // wide; one per locality, regardless of CDC-table
        // count.
    }

    impl CdcLocalityHeartbeatWorker {
        async fn run(self) {
            let mut ticker = interval(self.interval);
            loop {
                ticker.tick().await;
                if locality_quiet_since_last_finalization(self.locality_id) {
                    // No table identity in the marker.
                    emit_heartbeat_finalization_marker(self.locality_id).await;
                }
            }
        }
    }
    ```

    The heartbeat marker is a single control-ring entry; it
    carries no row payload, no table identity, and
    contributes no CDC event. It only advances the one
    per-locality `locality_finalized_hlc(l)`. Busy
    localities skip heartbeats whenever a natural
    finalization fires within the cadence window. The
    worker spawns at locality-leader startup for every
    locality (one worker per locality, not per
    `(locality, table)`). Cluster cost: `N_localities /
    heartbeat_interval`, independent of CDC-table count.

    The contract is a substrate requirement on the control
    ring's behavior; ADR 054 does not introduce a new wire
    surface for it.

2. Implement catchup in `handle_cdc_subscribe` using **full
   `CdcCursor` tuple bounds**, not HLC ranges. HLC-only ranges
   miss same-HLC suffixes (`(T, k7, 0)` past
   `start_cursor = (T, k5, 0)`) or replay same-HLC prefixes
   already acked:

   ```rust
   async fn run_catchup(
       &self,
       table_object_id: TableObjectId,
       start_cursor: CdcCursor,
       snapshot: &CatchupSnapshot,
       writer: &mut SendStream,
       handle: &SubscriptionHandle,
   ) -> Result<()> {
       let upper_inclusive = CdcCursor {
           row_hlc: snapshot.snapshot_hlc,
           row_key_id: RowKeyId::MAX,
           record_row_index: RecordRowIndex::MAX,
       };
       // For each source locality l of (tenant, table):
       //   1. Storage scan with full-cursor bounds:
       //        cursor > start_cursor (lex)
       //        cursor <= upper_inclusive (lex)
       //   2. Yield rows in per-locality CdcCursor order.
       // Cross-locality merge is a min-heap over per-locality
       // streams ordered by CdcCursor; the merger's output is
       // streamed into cdc_emit_frame in global cursor order.
       // Catchup output is unconditionally <= upper_inclusive
       // by construction, so the safe-emission watermark
       // (Phase 4 / step 3) does not gate catchup frames —
       // every catchup row has row_hlc <= snapshot_hlc, and
       // every locality has finalized through snapshot_hlc by
       // definition of the cut.
   }
   ```

   The scan reuses existing storage scan paths; ADR 054 adds
   no new storage scan code. The catchup-needed test for any
   locality `l` is the full-tuple comparison `start_cursor <
   ring.head_cursor[l]` (not row_hlc-only).

3. **Catchup-to-live handoff at `snapshot_hlc`.** Catchup ends
   at `(snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)`.
   Live tail then takes over for cursors strictly greater
   than that. Live-tail emission is gated by the
   safe-emission watermark wired in Phase 4 / step 3:

   ```rust
   fn safe_emission_hlc(table: &Table) -> Hlc {
       table
           .source_localities()
           .map(|l| l.locality_finalized_hlc())
           .min()
           .unwrap_or(Hlc::MIN)
   }
   ```

   A finalized live-tail entry whose
   `cursor.row_hlc > safe_emission_hlc()` is held in the ring
   even with `finalized = true`. The writer task drains
   eligible entries (`cursor.row_hlc <= safe_emission_hlc()`)
   in `CdcCursor` order. As any locality's
   `locality_finalized_hlc` advances, eligibility is
   re-evaluated and waiting entries become draining; a
   `Notify` / condvar on `safe_emission_hlc` advancement wakes
   the writer task.

   Without the watermark, A finalized through 100 could emit
   `A@90` before B finalizes `B@75`, producing the global
   sequence `A@90, B@75` that violates cursor monotonicity.
   With it, `A@90` is held until every locality (including B)
   has finalized through HLC 90.

4. Detect ring overflow during catchup: if any locality's
   `ring.head_cursor[l]` advances past `(snapshot_hlc,
   RowKeyId::MAX, RecordRowIndex::MAX)` while catchup is
   running, the live-tail ring has dropped post-snapshot
   events. V1 ships the reject path:
   - emit
     `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`;
   - close QUIC stream;
   - consumer reconnects.

5. Tests:

   - Catchup over historical window: subscribe with
     `CdcCursor::MIN`; receive all retained events with
     cursor `<= (snapshot_hlc, MAX, MAX)`; final emitted
     cursor matches the catchup ceiling.
   - Catchup with concurrent admissions: events admitted
     during catchup with `row_hlc > snapshot_hlc` appear in
     live tail subject to the safe-emission watermark; no
     duplicates; no gaps.
   - **Same-HLC suffix (full-cursor scan bound regression).**
     Pre-populate storage with rows
     `(T, k0, 0), (T, k5, 0), (T, k7, 0), (T, k9, 0)`.
     Subscribe with `start_cursor = (T, k5, 0)`. Assert:
     consumer receives `(T, k7, 0)` and `(T, k9, 0)` and
     **no others** — `(T, k0, 0)` and `(T, k5, 0)` are
     filtered by the lex lower bound, and same-HLC suffix
     after `start_cursor` is included by the lex upper bound.
     This regression test pins the full-tuple scan
     discipline against drift back to HLC-only ranges.
   - **Skewed-locality ordering (cross-locality global-order
     regression).** Two source localities A and B; at
     subscription open A is finalized through HLC 100, B
     through HLC 50. Pre-open storage: `A@30, A@60, A@90`
     (A finalized through 100), `B@30` (B finalized through
     50). Open with `start_cursor = MIN`. The test then
     finalizes (in scenario order): `B@75`, `B@90`, `A@110`.

     Assertions follow directly from the watermark rule
     (`emit when entry.cursor.row_hlc <= safe_emission_hlc`):

     - `snapshot_hlc` at open is `min(100, 50) = 50`.
     - Open-time ring-continuity guard passes (the live-tail
       ring's `head_cursor` for both localities is below
       the catchup ceiling; otherwise reject at open with
       `RING_OVERFLOW_DURING_CATCHUP`).
     - Catchup emits exactly the events with cursor
       `<= (50, MAX, MAX)`: `A@30, B@30` in their
       deterministic lex order.
     - Right after open, `safe_emission_hlc =
       min(100, 50) = 50`. `A@60` and `A@90` sit in the ring
       with `finalized = true` but watermark-blocked
       (60 > 50, 90 > 50).
     - Scenario step 1 — B finalizes `B@75`. B's frontier
       advances 50 → 75; `safe_emission_hlc =
       min(100, 75) = 75`. Now eligible: `A@60` (60 ≤ 75),
       `B@75` (75 ≤ 75). Both emit, in lex order. `A@90`
       stays blocked (90 > 75).
     - Scenario step 2 — B finalizes `B@90`. B's frontier
       advances 75 → 90; `safe_emission_hlc =
       min(100, 90) = 90`. Now eligible: `A@90` (90 ≤ 90),
       `B@90` (90 ≤ 90). Both emit, in lex order.
     - Scenario step 3 — A finalizes `A@110`. A's frontier
       advances 100 → 110; B is still at 90, so
       `safe_emission_hlc = min(110, 90) = 90`. `A@110`
       sits in the ring with `finalized = true` but blocked
       (110 > 90).
     - Scenario step 4 — B's heartbeat advances B's frontier
       past 110 (or B finalizes another admission ≥ 110);
       `safe_emission_hlc ≥ 110`. `A@110` emits.
     - Full emitted cursor sequence is non-decreasing in
       `CdcCursor` lex order; every event delivered exactly
       once; no event dropped.
   - **Cold-locality watermark heartbeat (regression test
     for stall-free watermark).** Two source localities A
     and B; A is busy, B is **completely cold** for the
     duration of the test (no admissions). Subscribe with
     `start_cursor = MIN`. Admit and finalize a sequence
     `A@T, A@T+10, A@T+20` on A. Without heartbeats on B,
     `safe_emission_hlc` would stall at B's last value
     forever and A's events would never emit. With the V1
     heartbeat contract:
     - B's `locality_finalized_hlc` advances at cadence
       `cdc_locality_heartbeat_interval_millis` (V1 default
       1000 ms) via no-op control-ring finalization markers.
     - A's events emit with bounded latency `≤ 1 ×
       cdc_locality_heartbeat_interval_millis` per event.
     - Test harness: configure heartbeat cadence to 100 ms
       for fast assertion; admit `A@T+10` and assert it
       emits within ≤ 200 ms.
     - The bench (Phase 9 #10) reports the watermark hold
       time at the production default cadence.
   - **Open-time ring-continuity guard (regression test for
     pre-open eviction).** Configure `max_cdc_live_tail_
     bytes_per_locality` small. On locality A admit and
     finalize a long sequence, then admit and finalize many
     more, deliberately evicting earlier entries from A's
     ring. Locality B remains at HLC 50 (and admits enough
     to keep B's ring-head near B's tail). Subscribe with
     `start_cursor = MIN`. `snapshot_hlc = min(A_frontier,
     50) = 50`, but A's `ring.head_cursor[A]` is now well
     above `(50, MAX, MAX)` (entries between 50 and the
     ring head were evicted). Assert: subscription rejects
     at open with `CDC_REJECT_RING_OVERFLOW_DURING_CATCHUP`
     before any frame is sent. Test verifies the consumer
     does **not** silently miss the evicted A events.
   - Catchup with concurrent admissions exceeding ring
     budget: `RING_OVERFLOW_DURING_CATCHUP` fires
     mid-catchup.
   - Catchup over a retention-policy advance: floor advances
     past `start_cursor.row_hlc` mid-catchup; subscription
     rejects `BELOW_RETENTION_FLOOR`.
   - Cross-locality catchup with no skew: events from N
     source localities interleave in global cursor order.
   - `CdcCursor::MIN` clamps to the current retention floor
     (does not reject as below-floor); first emitted event's
     `row_hlc` is `>= retention_floor`.

Exit:

- Catchup wired; subscriptions with `start_cursor <
  (snapshot_hlc, MAX, MAX)` receive history from every source
  locality, merged in global lex order.
- Live tail emits in global `CdcCursor` order under skewed
  locality finalization, gated by `safe_emission_hlc()`.
- Pre-open eviction above the catchup ceiling rejects at
  open with `RING_OVERFLOW_DURING_CATCHUP` rather than
  silently dropping events.
- Cold localities advance `locality_finalized_hlc` via
  one `CdcLocalityHeartbeatWorker` per locality (not per
  table) at cadence
  `cdc_locality_heartbeat_interval_millis`, so the
  watermark cannot stall behind an idle locality.
- Same-HLC suffixes after `start_cursor` are included by
  full-tuple scan bounds; same-HLC prefixes already acked are
  not.

## Phase 6 — `StateDigest` checkpoints

Files:

- `src/network/cdc.rs` (`cdc_emit_checkpoint`)
- `src/network/data_server.rs` (checkpoint cadence)
- `src/storage/digest.rs` (visible-domain digest at HLC; reuses
  ADR 046 substrate)
- `tests/cdc_checkpoint.rs` (new)

Work:

1. Implement `cdc_emit_checkpoint`:

   ```rust
   pub async fn cdc_emit_checkpoint(
       writer: &mut SendStream,
       meta: &CdcCheckpointMeta,
   ) -> std::io::Result<()> {
       // No arrow_payload; just header + meta.
   }
   ```

2. Cadence in the writer task:
   - Track `events_since_checkpoint` (counter) and
     `wall_time_since_checkpoint` (Instant).
   - After each emit, if either exceeds threshold (1024
     events or 1 second wall time), compute the visible-
     state digest at the current `cursor.row_hlc` and emit
     checkpoint.

3. Digest computation: read the visible-domain
   `digest.table.row.visible.v1` accumulator from ADR 046
   substrate at `cursor.row_hlc`. V1 reuses the existing
   per-table digest cache; if the cache misses at this HLC,
   compute via the substrate's accumulator helper.

4. **Resume-time digest validation.** When
   `req.resume_digest_present != 0`, the server reads
   `digest.table.row.visible.v1` at `req.start_cursor.row_hlc`
   from the same per-table digest cache the cadence path uses,
   compares to `req.resume_visible_state_digest`, and rejects
   with `CHECKPOINT_DIGEST_MISMATCH` on inequality. The
   validation runs in `handle_cdc_subscribe` step 5 (Phase 4)
   before any subscription registry mutation; if the digest
   read times out, fall back to `INTERNAL_ENGINE_ERROR`.

   `req.resume_digest_present = 0` skips validation entirely;
   the consumer accepts whatever the server's retained state
   says at `start_cursor`. This is the V1 default for fresh
   subscriptions (no prior checkpoint to compare against).

5. Tests:

   - Checkpoint at cursor matches independent
     `digest.table.row.visible.v1` accumulation at the same
     HLC.
   - Checkpoint cadence: 1024-event burst emits exactly one
     checkpoint; 2-second slow-stream emits two checkpoints.
   - Checkpoint after reject path is not emitted (the
     reject path closes the stream before cadence fires).
   - Checkpoint validation across replica failover:
     reconnect to a different replica, receive checkpoint at
     the same cursor; digest matches.
   - **Resume digest validation matching path.** Subscribe
     with `resume_digest_present = 1` and a digest matching
     retained state: open succeeds; first `CDC_FRAME` arrives
     normally.
   - **Resume digest validation mismatching path.** Subscribe
     with mismatched digest: open rejects
     `CHECKPOINT_DIGEST_MISMATCH` before any `CDC_FRAME`.
   - **Resume digest disabled.** Subscribe with
     `resume_digest_present = 0`: server skips digest read;
     no spurious cache pressure.

Exit:

- Checkpoints emit on cadence; consumers can verify against
  independent visible-domain digest.
- The roadmap line "consumers verify cursor checkpoints
  against partition `StateDigest` points algebraically" is
  satisfied.

## Phase 7 — Backpressure and slow consumers

Files:

- `src/network/cdc.rs` (writer task flow-control logic)
- `tests/cdc_backpressure.rs` (new)

Work:

1. The writer task (per subscription) drains
   `mpsc::Receiver<CdcLiveEntry>` and writes to the QUIC
   `SendStream`. QUIC flow control naturally backpressures
   the writer when the consumer is slow.

2. The admission stage hook's `mpsc::Sender::try_send` is the
   producer-side primitive. On `TrySendError::Full`, the
   admission path **does not block** — the staged entry stays
   in the live-tail ring; the writer task drains it on the
   next ring scan.

3. If the live-tail ring overflows because both the
   subscription's mpsc and the ring overflow simultaneously,
   the subscription's `cursor_high_water` falls below
   `ring.head_cursor`. On the next drain attempt, the writer
   task detects the cursor lag and triggers the
   catchup-or-reject path:
   - if `cursor_high_water.row_hlc >= retention_floor`, run
     a fresh catchup over `(cursor_high_water,
     ring.head_cursor]`;
   - else reject `BELOW_RETENTION_FLOOR`.

4. Tests:

   - Slow consumer: admit 100k events at 50k/sec; consumer
     reads at 10k/sec; QUIC flow control blocks the writer;
     `cdc_subscription_lag_micros` grows; no admission
     blocking (positive guard).
   - Slow consumer with overflow: ring is undersized;
     consumer falls behind ring head; writer task auto-runs
     catchup.
   - Slow consumer below floor: retention floor advances
     past `cursor_high_water`; subscription rejects
     `BELOW_RETENTION_FLOOR`.
   - Admission throughput unaffected by slow consumer:
     admission rate at p99 unchanged whether 0 or 100
     subscriptions are open (positive performance guard).

Exit:

- Slow consumers don't block admission.
- Recovery paths (catchup, reject) are deterministic.
- Admission throughput is independent of subscription count.

## Phase 8 — Metrics, audit, runbook

Files:

- `src/metrics/registry.rs`
- `src/audit/events.rs`
- `src/network/cdc.rs` (metric emission)
- `src/storage/cdc.rs` (metric emission)
- `tests/metrics_smoke.rs`
- `plans/ops/observability.md`
- `plans/ops/runbooks/cdc_subscription.md` (new)
- `plans/ops/runbooks/README.md`

Work:

1. Implement the `cdc_*` metric family per Phase 0 §10.
   Bounded labels (`source`, `result`, `code`) are pinned by
   the architectural test in Phase 10.

2. Emit metrics:
   - on subscribe open: `cdc_subscriptions_total{result=opened}`;
   - on subscribe close (graceful):
     `cdc_subscriptions_total{result=closed}`;
   - on subscribe reject:
     `cdc_subscriptions_total{result=rejected}` plus
     `cdc_rejects_total{code=...}`;
   - on subscribe cancel via QUIC close:
     `cdc_subscriptions_total{result=cancelled}`;
   - on every emitted frame:
     `cdc_events_emitted_total{source=ring|catchup}` and
     `cdc_event_bytes_emitted_total{...}`;
   - on every emitted checkpoint:
     `cdc_checkpoints_emitted_total{source=...}`;
   - on every flow-control block-and-resume:
     `cdc_subscription_lag_micros{...}`;
   - on every ring FIFO drop event:
     `cdc_ring_overflow_total{}`;
   - on catchup-to-live handoff:
     `cdc_catchup_to_live_handoff_micros{}` once per
     subscription;
   - `cdc_staged_entries_in_ring{}` gauge sampled on each
     stage / release / evict;
   - on each pending-finalization eviction:
     `cdc_pending_finalization_evicted_total{}`.

3. Audit events:
   - `CdcSubscriptionOpened { subscription_id, tenant_id,
     table_object_id, start_cursor, bound, opened_at_hlc }`
     emitted at handler entry.
   - `CdcSubscriptionClosed { subscription_id, reason,
     final_cursor, closed_at_hlc }` emitted at handler exit.
   - `reason` enumerates `{client_cancel, snapshot_complete,
     reject_<code>, internal_error}`.

4. Runbook content:
   - cursor semantics: `(Hlc, RowKeyId, RecordRowIndex)`;
   - finalization release-gate (ADR 003 composition);
   - live-tail vs catchup distinction in metrics
     (`source` label);
   - reject taxonomy decision tree (8 codes);
   - when to bump `max_cdc_live_tail_bytes_per_locality`
     (operator sees `cdc_ring_overflow_total` rising or
     `RING_OVERFLOW_DURING_CATCHUP` rejects);
   - when retention floor needs lengthening (consumer
     getting `BELOW_RETENTION_FLOOR`);
   - schema-evolution-during-subscription expected behavior
     (consumer re-fetches descriptor; no operator action);
   - replica failover (consumer reconnects with last
     cursor; cursor handoff is *not* zero-reconnect in V1);
   - composition with ADR 053 wire compression
     (`cdc_event` path label);
   - composition with ADR 048 retention
     (`TableLifecycleCatalog` is the floor authority);
   - Identity-vs-Locality framing on the cursor;
   - top-K aggregation pattern for per-table /
     per-subscription dashboards (since metric labels are
     bounded). Examples: aggregate `cdc_events_emitted_total`
     by tenant via log-derived counts; rank top-K subscribers
     by lag from log spans.

5. Tests:

   - Metrics smoke: every `cdc_*` metric registers; bounded
     labels pin to documented enumerations.
   - One round-trip increments expected counters.
   - Audit chain emits exactly one
     `CdcSubscriptionOpened` and one `CdcSubscriptionClosed`
     per subscription.

Exit:

- Operators can observe CDC traffic, lag, rejections; runbook
  documents the operational decision tree.

## Phase 9 — Bench

Files:

- `benches/cdc_subscription.rs` (new)
- `Cargo.toml` (`[[bench]]` entry)
- `plans/impl/054_cdc_subscription_substrate_bench_results.md`
  (new, populated when bench runs)

Bench matrix:

1. **Live-tail latency** (admit → finalization →
   consumer receive) at p50 / p95 / p99 under admission rates
   1k / 10k / 100k events/sec with one subscription.

2. **Live-tail latency under fan-out** at the same rates with
   1 / 10 / 100 / 1000 concurrent subscribers.

3. **Catchup throughput** at historical window sizes
   1 GiB / 10 GiB / 100 GiB; report decoded MiB/sec; report
   re-encode CPU share per subscription.

4. **Ring-overflow threshold**: at what consumer
   lag / admission-rate combination does the ring overflow?
   Verify the fallback (catchup-from-cursor) recovers cleanly.

5. **Checkpoint cost**: incremental cost per checkpoint
   (digest read at HLC + meta encode + frame emit).

6. **Admission-path overhead**: admission rate with 0 vs N
   subscribed CDC consumers. Confirm O(N) work per admission
   (one stage push + one `mpsc::try_send` per active
   subscription) does not regress admission p99 by more than
   5% at N = 100.

7. **Schema-fingerprint-change cost**: subscription that
   spans an additive schema bump; per-frame fingerprint
   compare; consumer descriptor re-fetch latency.

8. **Wire compression**: confirm CDC frames use ADR 053
   `LZ4_FRAME` path; per-shape encoded bytes under
   `NetworkCompressionPath::CdcEvent`.

9. **Stage-vs-release cost decomposition**: time spent in
   `stage` (decode, fingerprint, push) vs `release` (atomic
   bit flip, notify) vs `drain` (mpsc consume, encode, write).
   So a slow path has a named source.

10. **Safe-emission watermark hold time.** Two scenarios:

    a. **Skewed locality finalization.** Two source
       localities; configure one locality's finalization
       cadence at 2× the other's; measure median and p99
       hold time of finalized live-tail entries on the
       leading locality (time between `finalized = true`
       and emission).
    b. **Cold locality + heartbeat.** One busy locality, one
       completely cold locality with the heartbeat at the V1
       default cadence (1000 ms). Measure the per-event
       hold time on the busy locality. Repeat at heartbeat
       cadences {100 ms, 500 ms, 1000 ms, 2000 ms} to
       quantify the cadence-vs-control-ring-write-rate
       trade-off.

    These two together are the load-bearing cost of
    preserving global `CdcCursor` ordering without
    `LocalityId` on the cursor; the bench surfaces them
    explicitly so operators can size cadences against their
    CDC latency budget.

Acceptance starting points:

- Live-tail end-to-end latency (admit → finalization →
  consumer receive): p99 ≤ 5× one-way QUIC RTT under 10k
  events/sec.
- Catchup throughput ≥ 100 MiB/sec decoded per subscription.
- Ring overflow does not block admission (admission p99
  unaffected).
- Checkpoint cost ≤ 1 ms per checkpoint at typical
  table-cardinality.
- Admission overhead with 100 active subscriptions
  regresses p99 by ≤ 5%.
- Stage cost per admission ≤ 10% of admission CPU.

Bench results in
`plans/impl/054_cdc_subscription_substrate_bench_results.md`.
Quick smoke acceptable for code review; full
small / medium / large matrix with end-to-end latency under
fan-out is the ADR 054 promotion-to-shipped gate.

## Phase 10 — Architectural lockdown

Files:

- `tests/architecture_test.rs`

**Convention.** Forbidden-literal checks construct needles
via `concat!(...)` per the ADR 050 / 051 / 052 / 053
convention.

Guards to add:

- ☐ `CdcCursor` is `Pod + Zeroable + Ord`; lex order is
  `(hlc, row_key_id, record_row_index)`.
- ☐ `CdcCursor` fields are newtypes (`Hlc`, `RowKeyId`,
  `RecordRowIndex`) — no raw `u64` / `[u8; 32]` / `u32` in
  the field types.
- ☐ `CdcSubscriptionId` is a `u128` newtype with
  `Pod + Zeroable`.
- ☐ `CdcRejectCode` enumerates exactly the 8 V1 codes; no
  reserved slots; discriminants start at 1 so a
  zero-initialized `CdcRejectMeta` cannot decode as a valid
  reject.
- ☐ Sparse enums (`CdcSubscribeBound`, `CdcFrameSource`,
  `CdcRejectCode`, `DigestDomain`) do **not** derive `Pod`
  or `Zeroable`. Wire carriers are raw integer fields on the
  meta structs (`bound_raw`, `source_raw`, `code_raw`,
  `digest_domain_raw`); a `from_raw` checked decoder exists
  for each. Negative guard greps `tests/architecture_test.rs`
  for any `Pod` / `Zeroable` derive on the listed enums via
  `concat!(...)`.
- ☐ Exactly four new `MsgType` constants:
  `CDC_SUBSCRIBE_REQ`, `CDC_FRAME`, `CDC_CHECKPOINT`,
  `CDC_REJECT`.
- ☐ `wire_encode_batch_for_path()` is the only encoder used
  by `cdc_emit_frame` (positive guard reads ADR 053's
  helper).
- ☐ `cdc_event` is added to ADR 053's path-label enumeration
  in `src/network/ipc.rs`.
- ☐ ADR 053's path-label architectural guard accepts
  `cdc_event` as the fifth allowed value.
- ☐ Every `stages-cdc` admission site invokes the stage hook
  exactly once per produced version-entry. Classification
  table in
  `tests/architecture_test.rs::cdc_admission_stage_audit`.
- ☐ Content-hash collapsed-write path does **not** invoke
  the stage hook. Positive guard via golden test.
- ☐ Multi-row admit stages N entries for an N-row record
  with `record_row_index ∈ [0, N)`.
- ☐ Staged entries are invisible to subscribers until
  `finalized = true` (positive guard via golden test).
- ☐ Pending-finalization cleanup evicts staged entries on
  terminal failure; subscribers never observe them.
- ☐ Catchup scan uses **full `CdcCursor` tuple bounds**, not
  HLC ranges. Positive guard reads the catchup helper in
  `src/storage/cdc.rs` and asserts the upper bound is
  `(snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)` and
  the lower bound is `start_cursor` (full lex), not
  `start_cursor.row_hlc`. Negative guard via the Phase 5
  same-HLC suffix regression test.
- ☐ `snapshot_hlc` is `min(locality_finalized_hlc(l))`, not
  `max`. Positive guard reads `capture_snapshot` and pins
  the `min` reduction; negative guard via the Phase 5
  skewed-locality regression test (max would lose events).
- ☐ Live-tail emission is gated by
  `safe_emission_hlc(table) =
  min(locality_finalized_hlc(l))`. Positive guard reads
  `drain_live_tail` and pins the `<=
  safe_emission_hlc(table)` check before emit; negative
  guard via the Phase 5 skewed-locality global-ordering
  regression test (without the watermark, the test
  produces a non-monotonic cursor sequence).
- ☐ Open-time ring-continuity guard runs in
  `handle_cdc_subscribe` between `capture_snapshot` and
  registry registration. Positive guard reads the handler
  and pins the call to `open_time_ring_continuity_check`;
  negative guard via the Phase 5 pre-open-eviction
  regression test (without the open-time check, the test
  silently misses evicted entries).
- ☐ Cold-locality heartbeat worker is **locality-scoped,
  not per-table**: exactly one
  `CdcLocalityHeartbeatWorker` per locality on the
  locality leader; cadence is
  `cdc_locality_heartbeat_interval_millis` (default 1000
  ms). Positive guard greps for the worker struct and pins
  that it has a `locality_id` field but **no**
  `table_object_id` field; pins that the
  `emit_heartbeat_finalization_marker` call passes only
  the locality id. Negative guard via `concat!("Cdc",
  "PerTableHeartbeatWorker")` and similar table-scoped
  needle. Behavioral guard via the Phase 5 cold-locality
  test (active locality's events emit within bounded
  latency even though the cold locality has zero
  admissions and zero CDC subscriptions on the cold
  locality's tables); the test would fail if the worker is
  removed (events from the active locality stall).
- ☐ No `WendelonCdc*` brand-leaked symbol. Construct
  forbidden needle via `concat!("Wendelon", "Cdc")`.
- ☐ No `CdcCursorV[0-9]` / `CdcSubscriptionV[0-9]` /
  `CdcOperationV[0-9]` suffix.
- ☐ No `CdcEventKind` / `CdcOperation` enum exists.
  Construct forbidden needles via
  `concat!("Cdc", "EventKind")` and
  `concat!("Cdc", "Operation")`.
- ☐ No new `FRAME_FLAG_*` constant introduced by ADR 054.
  Construct forbidden needle via
  `concat!("FRAME_FLAG_", "CDC")`.
- ☐ No new `NodeCapability` variant.
  `NodeCapability::ALL.len()` unchanged from `main`.
- ☐ `HRFT_ENVELOPE_VERSION` unchanged from `main`.
- ☐ No Cargo feature change.
- ☐ No new digest domain. Construct forbidden needle via
  `concat!("digest.cdc.", "event.v1")`.
- ☐ No `TriggerObjectId` / `TriggerCatalog` /
  `SchemaObjectKind::Trigger` / `TriggerDescriptor` /
  `SchemaObjectKind::Webhook`. (V1 deferral; future ADR
  introduces.)
- ☐ Reject namespace prefix is `CDC_REJECT_*`. Construct
  forbidden needle via `concat!("WENDELON_CDC_REJECT_")`.
- ☐ Metric prefix is `cdc_*`. Construct forbidden needle
  via `concat!("wendelon_cdc_", "subscriptions_")`.
- ☐ Metric label `source` enumerates exactly
  `{ring, catchup}`.
- ☐ Metric label `result` enumerates exactly
  `{opened, closed, rejected, cancelled}`.
- ☐ Metric label `code` enumerates exactly the 8
  `CdcRejectCode` snake-case names.
- ☐ Metric labels do **not** include `tenant_id` /
  `table_object_id` / peer / subscription_id / schema /
  error-string. Forbidden-literal scan rejects any
  registration containing those names on `cdc_*` metrics.
- ☐ Audit events: exactly two new `AuditEventKind`:
  `CdcSubscriptionOpened`, `CdcSubscriptionClosed`.
- ☐ Compress-then-encrypt order preserved (CDC inherits
  ADR 053).
- ☐ The substrate publishes to subscriber fan-out **only
  after** a stage's `finalized = true` (positive guard via
  call-graph ordering).

Exit:

- Architecture is mechanically protected; future CDC follow-up
  ADRs (filter pushdown, triggers, webhooks, continuous
  queries) cannot drift the substrate without explicit
  amendment.

## Verification gate

Required before declaring ADR 054 shipped:

```sh
cargo test --lib schema::cdc
cargo test --lib network::cdc
cargo test --lib storage::cdc
cargo test cdc_cursor_ordering --quiet
cargo test cdc_frame_schema --quiet
cargo test cdc_live_tail --quiet
cargo test cdc_admission_stage --quiet
cargo test cdc_finalization_release --quiet
cargo test cdc_subscribe_open --quiet
cargo test cdc_frame_round_trip --quiet
cargo test cdc_catchup --quiet
cargo test cdc_checkpoint --quiet
cargo test cdc_backpressure --quiet
cargo test --test architecture_test --quiet
cargo test --test metrics_smoke --quiet
cargo bench --bench cdc_subscription --no-run
cargo bench --bench cdc_subscription -- --quick
git diff --check
cargo fmt --check
```

Grep guards:

- Cursor type:

  ```sh
  rg -n 'pub struct CdcCursor' src/schema/cdc.rs
  ```

  One match with `#[repr(C)]` and `Pod, Zeroable, Ord`.

- MsgType count:

  ```sh
  rg -n 'MSG_TYPE_CDC_' src/schema/core.rs
  ```

  Exactly four constants.

- Reject code count:

  ```sh
  rg -n 'CdcRejectCode::' src/schema/cdc.rs tests
  ```

  Exactly eight unique variants in the enum definition.

- Helper exists:

  ```sh
  rg -n 'fn cdc_emit_frame|fn cdc_emit_checkpoint|fn cdc_emit_reject' src
  ```

  All three present in `src/network/cdc.rs`.

- Path-label enumeration extended:

  ```sh
  rg -n 'cdc_event' src/network/ipc.rs tests/architecture_test.rs
  ```

  Present in both files.

- No new HRFT envelope `format_version` bump:

  ```sh
  rg -n 'HRFT_ENVELOPE_VERSION' src
  ```

  Value unchanged from `main`.

- No new node capability bit:

  ```sh
  rg -n 'enum NodeCapability' src
  ```

  Variant count unchanged from `main`.

- No `WendelonCdc*` brand-leaked symbols:

  ```sh
  rg -n 'WendelonCdc|wendelon_cdc_' src tests
  ```

  Empty.

- No version suffixes on ADR 054 Rust types:

  ```sh
  rg -n 'CdcCursor[A-Za-z]*V[0-9]|CdcSubscription[A-Za-z]*V[0-9]|CdcEventKind|CdcOperation' src tests
  ```

  Empty.

- No new `FRAME_FLAG_*` constant added by ADR 054:

  ```sh
  rg -n 'FRAME_FLAG_CDC' src tests
  ```

  Empty.

- No new digest domain:

  ```sh
  rg -n 'digest\.cdc\.event\.v1' src tests
  ```

  Empty.

- Trigger / webhook artifacts not present:

  ```sh
  rg -n 'TriggerObjectId|TriggerCatalog|SchemaObjectKind::Trigger|SchemaObjectKind::Webhook' src tests
  ```

  Empty.

- Content-hash collapsed-write path does not stage CDC entry:

  ```sh
  rg -n 'cdc_emit_admitted' src/storage/content_hash.rs
  ```

  Output: only at the post-collapse-decision branch where
  the *winning* version-entry is admitted, never on the
  collapsed-duplicate branch (positive guard read by
  golden test).

- Reject namespace prefix is functional:

  ```sh
  rg -n 'CDC_REJECT_|WENDELON_CDC_REJECT_' src tests
  ```

  Only `CDC_REJECT_*` present.

- Storage compression / network compression / CDC metric
  prefixes are distinct:

  ```sh
  rg -n 'storage_compression_|network_compression_|cdc_' src tests
  ```

  All three prefixes present and exercised.

- Metric label discipline:

  ```sh
  rg -n 'cdc_.*\{tenant_id|cdc_.*\{table_object_id' src
  ```

  Empty (no tenant/table labels on cdc_* metrics).

Convention guards:

- No `#[serde(default)]` shim on new types.
- No reserved enum slots.
- No envelope `format_version` bump.
- No new HRFT frame family beyond the four named MsgType.
- No new node capability bit.
- No `ObjectStore` trait break.
- No on-disk shape change.
- Cursor disposition (Phase 0 §1), reject taxonomy (Phase 0
  §5), live-tail ring shape (Phase 0 §7), finalization
  observer (Phase 0 §8), and metric labels (Phase 0 §10)
  are settled in code.

## File-by-file expected edits

`src/schema/cdc.rs` (new)
- `CdcCursor`, `CdcSubscriptionId` newtypes with POD derives.
- `CdcSubscribeRequestMeta`, `CdcFrameMeta`,
  `CdcCheckpointMeta`, `CdcRejectMeta` POD wire structs.
- `CdcSubscribeBound`, `CdcRejectCode`, `CdcFrameSource`
  sparse enums plus `from_raw` checked decoders. None of
  these enums derive `Pod`; their wire carriers are the
  raw integer fields on the meta structs.
- CDC frame Arrow schema builder.
- Documentation citing ADR 054 §2 / §3 / §7.

`src/schema/core.rs`
- Four new `MsgType` constants
  (`MSG_TYPE_CDC_SUBSCRIBE_REQ`, `MSG_TYPE_CDC_FRAME`,
  `MSG_TYPE_CDC_CHECKPOINT`, `MSG_TYPE_CDC_REJECT`).

`src/storage/cdc.rs` (new)
- `CdcLiveTailRing`, `CdcLiveEntry`.
- `CdcFinalizationObserver` and pending-finalization map.
- Catchup scan helper reading from existing storage scan
  primitives.
- Documentation citing ADR 054 §4.

`src/network/cdc.rs` (new)
- `CdcSubscriptionRegistry`, `SubscriptionHandle`.
- `cdc_emit_frame()`, `cdc_emit_checkpoint()`,
  `cdc_emit_reject()` helpers.
- Documentation listing the admission stage hook contract,
  the finalization release gate, and citing ADR 054 §4 / §5.

`src/network/mod.rs`
- Re-export the new module.

`src/network/data_server.rs`
- `handle_cdc_subscribe` long-lived stream RPC handler.
- Catchup loop + handoff logic.
- Admission-time stage hook calls at every `stages-cdc` site.
- Wire `CdcFinalizationObserver` to the existing
  `FinalizationCommitted` / `FinalizationTerminalFailure`
  hooks.

`src/network/ipc.rs`
- Path-label enumeration extended with `cdc_event`.

`src/storage/multirow.rs`
- Multi-row admit stages N entries.

`src/storage/content_hash.rs`
- Stage hook only on collapse-winner branch.

`src/storage/append_only.rs`
- Stage hook on every admit (data and tombstone).

`src/storage/pk_delete.rs` (or wherever ADR 050 admission
lands)
- Stage hook on tombstone admit; resurrection stages two
  entries.

`src/consensus/raft_storage.rs` or `src/storage/manager.rs`
- Wire `CdcFinalizationObserver` into the existing
  finalization paths.

`src/client/core.rs`
- Subscription client API: `cdc_subscribe`, `next_frame`.

`src/metrics/registry.rs`
- `cdc_*` metric family with bounded enumerations.

`src/audit/events.rs`
- `CdcSubscriptionOpened`, `CdcSubscriptionClosed` event
  kinds.

`tests/architecture_test.rs`
- ADR 054 guards.

`tests/cdc_cursor_ordering.rs` (new)
- Cursor lex-order golden.

`tests/cdc_frame_schema.rs` (new)
- Arrow schema golden.

`tests/cdc_live_tail.rs` (new)
- Ring shape and FIFO eviction.

`tests/cdc_admission_stage.rs` (new)
- Admission stage hook coverage golden.

`tests/cdc_finalization_release.rs` (new)
- Stage / release / discard fixtures.

`tests/cdc_subscribe_open.rs` (new)
- Subscription open / reject paths.

`tests/cdc_frame_round_trip.rs` (new)
- Frame encode / decode round-trip.

`tests/cdc_catchup.rs` (new)
- Catchup + handoff fixtures.

`tests/cdc_checkpoint.rs` (new)
- Checkpoint cadence + digest validation.

`tests/cdc_backpressure.rs` (new)
- Slow consumer + admission-throughput-unaffected golden.

`benches/cdc_subscription.rs` (new)
- Bench harness.

`Cargo.toml`
- `[[bench]]` entry for `cdc_subscription`. No feature-flag
  changes; no dependency additions.

`plans/ops/observability.md`
- Document new `cdc_*` metrics.

`plans/ops/runbooks/cdc_subscription.md` (new)
- Runbook.

`plans/ops/runbooks/README.md`
- Add CDC runbook to the index.

## V1 design summary

Engine surface decisions (within Wendelon's scope):

- CDC subscriptions are **identity-axis**, scoped to one
  `(TenantId, TableObjectId)`, with cursor
  `(Hlc, RowKeyId, RecordRowIndex)`.
- Two bound shapes: `Live` and `SnapshotAtOpen`.
- Event grain: storage version-entry. No `CdcEventKind` /
  `CdcOperation` enum at substrate; consumers derive
  insert / update / delete / resurrection semantics from
  row-axis synthetic columns (`__row_key_id__`,
  `__row_hlc__`, `__row_tombstone__`,
  `__supersedes_hlc__`).
- Live-tail mechanism: in-memory per-locality bounded ring;
  admission stages with `finalized = false`; ADR 003
  finalization observer flips the bit; pending-finalization
  cleanup discards on terminal failure. Live-tail emission is
  gated by safe-emission watermark `safe_emission_hlc =
  min(locality_finalized_hlc(l))` so the emitted cursor
  sequence is non-decreasing in `CdcCursor` lex order across
  source localities.
- Catchup mechanism: existing storage scan paths over
  retained source storage with **full `CdcCursor` tuple
  bounds** (lower exclusive `start_cursor`, upper inclusive
  `(snapshot_hlc, RowKeyId::MAX, RecordRowIndex::MAX)`).
  HLC-only scan ranges are not used (they miss same-HLC
  suffix). Safe global cut `snapshot_hlc =
  min(locality_finalized_hlc(l))`; min-heap merge across
  source localities.
- Catchup-to-live handoff at `(snapshot_hlc, RowKeyId::MAX,
  RecordRowIndex::MAX)`. **Open-time ring-continuity guard**
  asserts every non-empty source locality ring has
  `head_cursor[l] <= ceiling`; violation rejects
  `RING_OVERFLOW_DURING_CATCHUP` at open. Mid-catchup ring
  overflow rejects the same code.
- **Cold-locality heartbeat (locality-scoped, not
  per-table):** every locality runs *one*
  `CdcLocalityHeartbeatWorker` on its leader; that worker
  emits a single no-op control-ring finalization marker per
  quiet interval at cadence
  `cdc_locality_heartbeat_interval_millis` (V1 default
  1000 ms) when `locality_finalized_hlc(l)` has not advanced
  within the cadence window. The marker has no table
  identity and no row payload; one marker advances the one
  per-locality `locality_finalized_hlc(l)` value, which
  feeds the watermark for every table whose source
  localities include `l`. Cluster cost: `N_localities /
  heartbeat_interval`, independent of CDC-table count.
  Prevents `safe_emission_hlc = min(...)` from stalling
  behind a cold locality.
- Wire surface: 4 new `MsgType` (`CDC_SUBSCRIBE_REQ`,
  `CDC_FRAME`, `CDC_CHECKPOINT`, `CDC_REJECT`); inherits
  ADR 053 `wire_encode_batch_for_path()` with new
  `NetworkCompressionPath::CdcEvent`.
- Retention: bounded by ADR 048 `TableLifecycleCatalog`;
  below-floor cursor → `BELOW_RETENTION_FLOOR`.
- Schema binding: per-frame `LogicalSchemaFingerprint`
  (ADR 051); consumer re-binds on additive evolution; non-
  additive evolution rejects
  `SCHEMA_FINGERPRINT_INCOMPATIBLE`.
- Checkpoints: `digest.table.row.visible.v1` (ADR 046 V1
  helper) at cadence 1024 events or 1 second wall time.
  No new digest domain.
- Resume-time digest validation:
  `CdcSubscribeRequestMeta::resume_visible_state_digest` +
  `resume_digest_present` validate the consumer's last
  checkpoint at subscribe time; mismatch rejects at open with
  `CHECKPOINT_DIGEST_MISMATCH`.
- Server-side state: stateless on subscription identity;
  per-locality ring is the only mutable CDC state.
- Delivery: at-least-once; cursor is the dedupe key.
- Reject taxonomy: 8 `CDC_REJECT_*` codes.
- Metrics: `cdc_*` family with **bounded label sets only**.
  No tenant / table / peer / subscription_id / schema /
  error-string labels.
- Audit: subscription open / close lifecycle events; no
  per-event audit.
- Compress-then-encrypt order preserved by construction
  (CDC inherits ADR 053).
- Rolling deploy: pre-CDC nodes ignore the new MsgType
  (existing reject path); post-CDC nodes serve subscriptions.
- No on-disk shape change; no descriptor field; no envelope
  `format_version` bump; no new `FRAME_FLAG_*`; no new
  `NodeCapability`; no Cargo feature change; no new digest
  domain; no `TriggerCatalog` / `__webhook_events__`.

Out of Wendelon's scope (follow-up ADR territory):
PBC filter pushdown; post-commit PBC triggers + trigger
catalog; synchronous PBC triggers; webhook worker +
`__webhook_events__` table; continuous historical-plus-live
queries; durable cursor commit; exactly-once delivery;
multi-table / tenant-firehose subscriptions; before-image
CDC; column projection; cross-replica cursor handoff;
consumer-driven checkpoint cadence; per-event audit;
schema-evolution-as-event taxonomy; metadata / catalog CDC;
per-table CDC opt-in / opt-out; per-table CDC retention;
event-history `digest.cdc.event.v1` domain; consumer-
friendly higher-level `CdcEventKind` view.

## Deferred to follow-up ADRs

Mirrors ADR 054 *Deferred* section:

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
21. Live plaintext-byte / ratio accounting for CDC events.
