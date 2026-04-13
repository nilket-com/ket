# Moved from plans/raft-core.md

(This file was copied into plans/consensus/raft-core.md during a planned refactor.)


# Raft Core — bytemuck + WAL + Snapshot Design

This document specifies the Raft core on-disk and on-wire fixed-size formats (bytemuck), WAL record layout, snapshot format, and recovery invariants for HashRaft. Follow the same conventions as plans/schema/schemas.md: all fixed-size metadata are #[repr(C)] Pod + Zeroable; variable-length blobs are Arrow IPC or raw byte payloads as noted.

1. Goals and invariants

- Deterministic layout: every bytemuck struct includes explicit _pad fields and a static_assert comment with expected size.
- Recovery safety: commit_index, e_safe_index and WAL tail are durable; on restart no monotonic counters regress.
- Snapshot-install must include serialized idempotency cache checkpoint (see plans/consensus/idempotency_cache.md) and last-applied state-machine snapshot digest.
- WAL is append-only, CRC32c checksummed per record, and supports atomic fsync per-group.

---

2. Log entry fixed header — RaftLogEntryFixed (96 bytes)

rust


Copy

use bytemuck::{Pod, Zeroable};

/// Log entry kind discriminant (on-wire, fixed-size)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogEntryKind(pub u8);

impl LogEntryKind {
    pub const APPEND: Self = Self(0);   // regular key/value mutation (payload = Arrow/kv row)
    pub const CONFIG: Self = Self(1);   // epoch/config change (payload = epoch_entry_var_schema())
    pub const SNAPSHOT_MARKER: Self = Self(2); // marker for snapshot boundaries
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct RaftLogEntryFixed {
    /// 1 byte kind + 7 bytes pad => align on 8
    pub kind: LogEntryKind,         // 1
    pub _pad1: [u8; 7],             // 7
    /// Raft term for this entry
    pub term: u64,                  // 8
    /// Log index of this entry
    pub index: u64,                 // 8
    /// Epoch in which this entry was proposed
    pub epoch: u64,                 // 8
    /// Originating leader node id
    pub leader: super::NodeId,      // 8
    /// Authoritative HLC at entry creation (for causality)
    pub entry_hlc: super::HybridTimestamp, // 16
    /// Length in bytes of the variable payload following this header
    pub payload_len: u32,           // 4
    pub _pad2: [u8; 4],             // 4
    /// SHA-256 of payload bytes (zero for config-less entries)
    pub payload_hash: [u8; 32],     // 32
}
// size calc: 8 + 8 + 8 + 8 + 8 + 16 + 4 + 4 + 32 = 96
// static_assert: size_of::<RaftLogEntryFixed>() == 96

Notes:
- Payload immediately follows the fixed header in the WAL and on-wire log batches.
- For APPEND entries payload is an Arrow IPC RecordBatch row matching write_payload_schema(). For CONFIG entries payload is epoch_entry_var_schema().

---

3. Snapshot header — SnapshotHeader (80 bytes)

rust


Copy

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct SnapshotHeader {
    /// Last included index in snapshot
    pub last_included_index: u64,   // 8
    /// Term at last included index
    pub last_included_term: u64,    // 8
    /// Epoch at snapshot time
    pub last_included_epoch: u64,   // 8
    /// HLC when snapshot was taken
    pub snapshot_hlc: super::HybridTimestamp, // 16
    /// SHA-256 of serialized state-machine snapshot blob
    pub state_sha256: [u8; 32],     // 32
    /// Length in bytes of checkpointed idempotency cache section
    pub cache_checkpoint_len: u32,  // 4
    pub _pad: [u8; 4],              // 4
}
// size: 8+8+8+16+32+4+4 = 80
// static_assert: size_of::<SnapshotHeader>() == 80

Snapshot layout on-disk:
[ SnapshotHeader ][ state-machine blob (bytes) ][ idempotency cache checkpoint (bytemuck slice) ][ optional Arrow metadata ]

The cache checkpoint is a bytemuck-serialized contiguous slice of entries matching the layout described in plans/consensus/idempotency_cache.md (ClientWriteId + WriteResponseMeta per entry). The snapshot's cache_checkpoint_len names its byte length.

---

4. WAL record format (on-disk append record)

Each WAL record is encoded as:
[ WALRecordHeader (32 bytes) ][ body bytes (payload_len) ][ CRC32C (4 bytes) ]

rust


Copy

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WALRecordHeader {
    /// Record type: 0=LogEntry, 1=SnapshotMarker, 2=SnapshotInstall, 3=Checkpoint
    pub record_type: u8,
    pub _pad1: [u8; 7],        // align
    /// Total bytes in body that follow (payload_len)
    pub body_len: u64,
    /// Log index this record pertains to (0 for snapshot markers)
    pub log_index: u64,
    /// Term for this index / record
    pub term: u64,
}
// size: 1+7 + 8 + 8 + 8 = 32
// static_assert: size_of::<WALRecordHeader>() == 32

Notes:
- body_len = size of payload immediately following header. For LogEntry records this is sizeof(RaftLogEntryFixed) + payload_len.
- Each record ends with a 4-byte CRC32C covering header+body. Append procedure: write(header|body|crc), fsync group. Writers must ensure atomic append ordering.

---

5. Snapshot install & compaction

- Snapshot creation must be triggered when WAL size > compaction_threshold or when leader chooses.
- Snapshot install sequence (follower): receive SnapshotHeader, write to temporary file, validate state_sha256, atomically move into place, truncate WAL to byte-offset after last_included_index.
- If cache_checkpoint_len > 0, deserialize bytemuck slice and merge into in-memory idempotency cache: entries with log_index <= last_included_index may be safely reapplied as duplicates.

---

6. Recovery invariants and ordering

On startup:
1. Open WAL tail and replay records sequentially, verifying CRC32C.
2. Apply LogEntry records to state machine in index order, tracking last_applied, commit_index.
3. Load persisted e_safe_index (if persisted separately) and ensure e_safe_index <= commit_index. If violation, clamp e_safe_index = min(e_safe_index, commit_index).
4. Rebuild idempotency cache from snapshot checkpoint and recent WAL records up to configured scan window (see plans/consensus/idempotency_cache.md).

Durability rules:
- commit_index and e_safe_index are always persisted to WAL as a special Checkpoint record (WAL record_type=3) with body containing both u64 values. This guarantees monotonic recovery.
- During normal operation, leader fsyncs WAL on quorum durability levels as specified in plans/consensus/idempotency_cache.md.

---

7. State-machine interface (for application implementors)

Trait outline (Rust pseudocode):

rust


Copy

pub trait StateMachine: Send + Sync + 'static {
    /// Apply a committed log entry payload; must be idempotent for replay.
    fn apply(&mut self, index: u64, term: u64, payload: &[u8]) -> Result<(), ApplyError>;

    /// Serialize current state as a bytes blob for snapshotting.
    fn snapshot(&self) -> Vec<u8>;

    /// Load snapshot blob, replacing current state.
    fn load_snapshot(&mut self, blob: &[u8]) -> Result<(), ApplyError>;

    /// Optional: provide a stable hash (SHA-256) for snapshot validation.
    fn snapshot_hash(&self, blob: &[u8]) -> [u8; 32];
}

Guidelines:
- apply() must tolerate replay of the same index (idempotency) since cluster may reapply entries during recovery.
- For efficiency, applications should use ClientWriteId semantics when available to ensure safe deduplication at the application layer.

---

8. Tests & verification

- Fuzz the RaftLogEntryFixed parsing via bytemuck::cast_ref against random byte arrays; ensure crc checks catch corruption.
- Snapshot round-trip: take snapshot, write to disk, load into a fresh node and verify state_sha256 and cache checkpoint rehydrates idempotency cache.
- WAL replay window: simulate leadership transfer and ensure new leader rebuilds idempotency cache by scanning last N entries.
- Crash-recovery: simulate partial WAL writes (truncated tail), verify CRC detection and safe truncation to last good record.

---

9. Cross-references

- See plans/schema/schemas.md for Arrow schemas, ClientWriteId cache layout, and FinalizationMarker semantics. This document assumes HybridTimestamp and NodeId are defined in plans/schema/schemas.md and referenced via `super::` in Rust examples.

---

Control Ring & Hashing (cross-reference)

The system models a unified control ring: every node (regardless of role) is assigned a NodeId that participates in a single ordered ring topology. This simplifies small topologies used for dev/QA and unifies placement logic across control and data responsibilities. The hashing algorithm and ring parameters are chosen at cluster initialization and recorded in the epoch variable metadata (see plans/schema/hashing.md meta_keys/meta_values). They are immutable for the cluster lifetime; changing them requires a full cluster rebootstrap.

Recommended default: keyed HighwayHash with a 64-bit ring representation. For public or adversarial environments, a keyed BLAKE3 with 128-bit ring_bits is acceptable if crypto-level guarantees are required.

---

Epoch transition & streaming (non-blocking cutover)

Overview

Epoch transition is a two-phase safetyable cutover:
1) Decision (Raft): commit a single Raft entry describing the new epoch, membership, ring seed, and a compact transfer_plan (vnode lists or key-range descriptors plus optional rate limits).
2) Work (streaming): incrementally move affected key ranges from old owners to targets while both sides accept epoch-tagged requests.

Sequence (detailed)

- Commit epoch: leader appends RaftEpochEntry (config + transfer_plan) and waits for commit.
- Dual-accept: once committed, both old and new owners accept requests for affected ranges tagged with the new epoch. Servers validate epoch and return committed epoch in responses when they observe mismatch.
- Incremental streaming: sources stream data to targets in fixed-size chunks (default chunk_size = 256 KiB). Each chunk includes a checksum (CRC32C or optional SHA-256) and a monotonically increasing cursor.
  - Per-chunk ACKs let the source advance cursors; in-flight windows bounded per-transfer.
  - Rate-limiting (token-bucket) applies per-transfer and globally; defaults: 100 MB/s per transfer, adjustable via cluster metadata.
- Durable resume: transfer cursors and last-ACK are persisted locally (WAL or lightweight on-disk cursor file) so interrupted transfers can resume without re-streaming already-acked chunks.
- Finalization marker: after a target validates all chunks for a range, it requests a small Raft entry TransferComplete(range_id). When that entry is committed, the source may stop accepting writes for the range and safe-GC local copies.

Backpressure and safety

- Leader monitors Raft commit latency and e_safe lag. If commit latency exceeds threshold (default 200 ms), the leader reduces transfer concurrency and lowers per-transfer rates.
- Transfer operations never block the metadata executor: streaming runs on a separate data-plane worker pool with bounded concurrency and explicit yields to the control-plane executor.

Executor & I/O isolation

- Control plane: small, high-priority tokio runtime dedicated to Raft heartbeats, RPCs, and metadata processing (2–4 threads recommended).
- Data plane: separate worker pool for streaming/disks with io_uring-backed file I/O; use bounded task queues and spawn_blocking for synchronous tasks.
- Kernel and NIC tuning: document recommended sysctl, io_uring parameters, and NIC offload settings in bench/config.

Observability

Expose metrics per-transfer and cluster-wide:
- transfer_bytes_total, transfer_bytes_inflight, transfer_rate_bytes/s
- transfer_retries_total, transfer_resume_count
- raft_commit_latency_ms (histogram), e_safe_lag
- streaming_backpressure_events

Testing & verification

- Unit tests for chunk checksum and resume logic.
- Integration tests: simulated epoch change with synthetic data and network partitions; verify no data loss and successful finalization.
- Chaos tests: kill source/target mid-transfer, drop packet subsets, and assert resume correctness.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
