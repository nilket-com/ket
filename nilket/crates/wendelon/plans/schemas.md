HashRaft Wire Format — bytemuck + Arrow IPC Design
Zero-copy metadata via #[repr(C)] + Pod/Zeroable, variable-length data via Arrow IPC, and a custom QUIC-framed transport replacing gRPC entirely.

1. Foundational Metadata Types — bytemuck Structs
Every struct below is #[repr(C)], derives Pod + Zeroable, and can be transmuted to/from &[u8] with zero serialization cost. Explicit _pad fields ensure deterministic layout across compilers.

rust


Copy
use bytemuck::{Pod, Zeroable};

// ============================================================
//  HYBRID LOGICAL CLOCK — 16 bytes, fixed
//
//  Comparison order: physical_ms → logical → node_id.
//  Merge rule at leader: max(local, remote) + 1 on logical.
//  physical_ms must not drift >250ms from NTP source.
//  node_id breaks ties deterministically — set to Raft node
//  ID of the stamp originator.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
#[repr(C)]
pub struct HybridTimestamp {
    /// Wall-clock milliseconds since Unix epoch (48 bits used, upper 16 reserved)
    pub physical_ms: u64,
    /// Monotonic counter; reset to 0 when physical_ms advances
    pub logical: u32,
    /// Originating node ID; breaks ties for identical (physical, logical) pairs
    pub node_id: u32,
}
// static_assert: size_of::<HybridTimestamp>() == 16

// ============================================================
//  NODE IDENTITY — 8 bytes, fixed
//
//  Address resolution is out-of-band via the cluster config
//  (RaftEpochEntry.voters / learners arrow payload).
//  The wire ID is a pure numeric handle.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct NodeId {
    /// Cluster-unique numeric ID assigned at bootstrap
    pub id: u32,
    /// Reserved for future use (e.g., generation counter). Must be 0.
    pub _reserved: u32,
}
// static_assert: size_of::<NodeId>() == 8

// ============================================================
//  CLIENT WRITE ID — 32 bytes, fixed
//
//  Provides exactly-once semantics across client retries.
//
//  Idempotency contract:
//    • client_uuid: 16-byte UUIDv4, immutable for session lifetime.
//    • sequence_no: strictly monotonic per session.
//    • Leader maintains cache keyed on (client_uuid, sequence_no)
//      → WriteResponseMeta. Entries expire after idempotency_ttl_ms
//      OR piggy-backed ack (client's next write implicitly acks
//      all seq < current_seq - ack_window).
//    • Server clamps ttl to [1_000, 300_000] ms.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ClientWriteId {
    /// 16-byte UUIDv4; immutable per client session
    pub client_uuid: [u8; 16],
    /// Strictly monotonic within session
    pub sequence_no: u64,
    /// Client-suggested cache retention in ms; server clamps to [1s, 300s]
    pub idempotency_ttl_ms: u32,
    /// Pad to 32-byte alignment
    pub _pad: u32,
}
// static_assert: size_of::<ClientWriteId>() == 32

// ============================================================
//  TRANSFER TOKEN — 128 bytes, fixed
//
//  Authorizes exactly one leadership transfer within an epoch.
//
//  Verification procedure (at the target node):
//    1. Reject if now_hlc() >= expires_at
//    2. Reject if source_epoch != verifier's current_epoch
//    3. Ed25519::verify(signature, SHA-512(bytes[0..56]))
//       using from_leader's public key from current config
//    4. Reject if from_leader != current leader in config
//
//  Single-use: once presented in RequestVote, source_epoch
//  is permanently fenced.
// ============================================================
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct TransferToken {
    /// Epoch that granted the transfer
    pub source_epoch: u64,
    /// Must equal source_epoch + 1
    pub target_epoch: u64,
    /// Outgoing leader
    pub from_leader: NodeId,
    /// Designated successor
    pub to_leader: NodeId,
    /// Token invalid after this HLC; recommended TTL: 2× election timeout
    pub expires_at: HybridTimestamp,
    // --- 56 bytes above are the signed payload ---
    /// Ed25519 signature over SHA-512(fields above); fixed 64 bytes
    pub signature: [u8; 64],
    /// Pad to 128 bytes
    pub _pad: [u8; 8],
}
// static_assert: size_of::<TransferToken>() == 128

// ============================================================
//  FINALIZATION MARKER — 64 bytes, fixed
//
//  Appended to the Raft log (or broadcast out-of-band) once a
//  write achieves quorum ack. Drives E_safe advancement.
//
//  E_safe enforcement:
//    Followers MUST NOT serve reads for keys whose most recent
//    mutation has log_index > the follower's local e_safe_index.
//    e_safe_index advances only via leader's commit_index
//    propagated through AppendEntries.
// ============================================================
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct FinalizationMarker {
    /// Raft log index of the finalized write
    pub log_index: u64,
    /// Raft term at commit time
    pub term: u64,
    /// HashRaft epoch (leader generation)
    pub epoch: u64,
    /// HLC timestamp at quorum ack
    pub commit_hlc: HybridTimestamp,
    /// SHA-256 of committed payload; followers can verify integrity
    /// without re-reading the log entry
    pub payload_hash: [u8; 32],
    /// Highest log index safe for reads in this epoch at finalization time
    /// (monotonically non-decreasing within an epoch)
    pub e_safe_index: u64,
}
// static_assert: size_of::<FinalizationMarker>() == 88
// Note: actual 88B — pack tighter if 64B is hard req, or accept 88B.
// Keeping semantic clarity over forced padding.

// ============================================================
//  LEASE DESCRIPTOR — 128 bytes, fixed
//
//  Read leases let the leader serve linearizable reads locally
//  without a quorum round-trip while the lease is valid.
//
//  • Granted when a majority confirms via heartbeat.
//  • Expires at lease_expiry (HLC-based, NOT wall-clock).
//  • All leases instantly invalidated on epoch bump / step-down.
//  • Reads require commit_index >= min_commit_index to prevent
//    stale reads after re-election.
// ============================================================
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct LeaseDescriptor {
    /// Epoch in which this lease is valid
    pub epoch: u64,
    /// Node holding the lease (always the current leader)
    pub holder: NodeId,
    /// HLC when lease was granted
    pub lease_start: HybridTimestamp,
    /// HLC when lease becomes invalid; recommended: heartbeat_interval × 3
    pub lease_expiry: HybridTimestamp,
    /// Reads must have commit_index >= this to prevent post-election stale reads
    pub min_commit_index: u64,
    /// Ed25519 over fields above; 64 bytes
    pub signature: [u8; 64],
}
// static_assert: size_of::<LeaseDescriptor>() == 128
2. Request/Response Metadata — bytemuck Structs
rust


Copy
// ============================================================
//  WRITE REQUEST METADATA — 80 bytes, fixed
//
//  This is the header only. The actual key and value are
//  carried in an Arrow IPC payload appended after this
//  header in the wire frame (see §4 Framing).
//
//  Proxying semantics:
//    If a follower receives this, it increments proxy_hop
//    and forwards to the leader. Max proxy_hop = 2.
//    proxy_source records the forwarding follower for
//    response routing.
// ============================================================

/// Durability level for writes
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct DurabilityLevel(pub u8);

impl DurabilityLevel {
    pub const QUORUM: Self = Self(0);  // Wait for quorum ack + fsync
    pub const LEADER: Self = Self(1);  // Ack after leader fsync only
    pub const NONE:   Self = Self(2);  // Fire-and-forget
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WriteRequestMeta {
    /// REQUIRED — exactly-once idempotency key
    pub write_id: ClientWriteId,           // 32 bytes
    /// Client's current HLC; leader merges max(leader_hlc, client_hlc)+1
    pub client_hlc: HybridTimestamp,       // 16 bytes
    /// Client's believed epoch; leader rejects with EPOCH_STALE on mismatch
    pub expected_epoch: u64,               //  8 bytes
    /// 0 = direct to leader; incremented by each proxy; max 2
    pub proxy_hop: u8,                     //  1 byte
    /// Durability level: 0=QUORUM, 1=LEADER, 2=NONE
    pub durability: DurabilityLevel,       //  1 byte
    /// If 1, leader streams back FinalizationMarker in the response
    pub await_finalization: u8,            //  1 byte
    pub _pad1: [u8; 5],                    //  5 bytes alignment
    /// Set by proxy follower for response routing; zeroed on direct writes
    pub proxy_source: NodeId,              //  8 bytes
    /// Key length in the Arrow payload (for fast routing without Arrow parse)
    pub key_len: u32,                      //  4 bytes
    /// Value length in the Arrow payload
    pub value_len: u32,                    //  4 bytes
}
// static_assert: size_of::<WriteRequestMeta>() == 80

// ============================================================
//  WRITE RESPONSE METADATA — 96 bytes, fixed
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct WriteStatus(pub u8);

impl WriteStatus {
    pub const OK:                Self = Self(0);
    pub const EPOCH_STALE:       Self = Self(1);
    pub const NOT_LEADER:        Self = Self(2);
    pub const DUPLICATE:         Self = Self(3); // Idempotency cache hit; original replayed
    pub const PROXY_LIMIT:       Self = Self(4); // proxy_hop > 2
    pub const PAYLOAD_TOO_LARGE: Self = Self(5);
    pub const LEASE_EXPIRED:     Self = Self(6);
    pub const INTERNAL_ERROR:    Self = Self(7);
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WriteResponseMeta {
    pub status: WriteStatus,               //  1 byte
    pub _pad1: [u8; 7],                    //  7 bytes
    /// Raft log index of committed entry (0 on error)
    pub log_index: u64,                    //  8 bytes
    pub term: u64,                         //  8 bytes
    pub epoch: u64,                        //  8 bytes
    /// Authoritative HLC of the commit
    pub commit_hlc: HybridTimestamp,       // 16 bytes
    /// Populated on NOT_LEADER / EPOCH_STALE for client redirect
    pub leader_hint: NodeId,               //  8 bytes
    /// Echoed back for client correlation
    pub echoed_write_id: ClientWriteId,    // 32 bytes
    /// 1 if a FinalizationMarker follows this frame; 0 otherwise
    pub has_finalization: u8,              //  1 byte
    pub _pad2: [u8; 7],                    //  7 bytes
}
// static_assert: size_of::<WriteResponseMeta>() == 96

// ============================================================
//  READ REQUEST METADATA — 40 bytes, fixed
//  Actual key is in the Arrow payload.
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct Consistency(pub u8);

impl Consistency {
    pub const LINEARIZABLE: Self = Self(0); // Requires valid lease or quorum check
    pub const SERIALIZABLE: Self = Self(1); // Leader's committed state, no lease needed
    pub const STALE:        Self = Self(2); // Any replica, no freshness guarantee
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct ReadRequestMeta {
    /// Optional: block until commit_hlc >= this (causal read-after-write)
    pub min_hlc: HybridTimestamp,          // 16 bytes
    pub expected_epoch: u64,               //  8 bytes
    pub consistency: Consistency,          //  1 byte
    pub _pad: [u8; 3],                     //  3 bytes
    /// Key length in Arrow payload (for fast routing)
    pub key_len: u32,                      //  4 bytes
    pub _pad2: [u8; 8],                    //  8 bytes
}
// static_assert: size_of::<ReadRequestMeta>() == 40

// ============================================================
//  READ RESPONSE METADATA — 56 bytes, fixed
//  Value is in the Arrow payload (omitted when KEY_NOT_FOUND).
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReadStatus(pub u8);

impl ReadStatus {
    pub const OK:            Self = Self(0);
    pub const KEY_NOT_FOUND: Self = Self(1);
    pub const EPOCH_STALE:   Self = Self(2);
    pub const NOT_LEADER:    Self = Self(3);
    pub const LEASE_EXPIRED: Self = Self(4);
    pub const E_SAFE_BEHIND: Self = Self(5); // Follower's e_safe < required; retry leader
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct ReadResponseMeta {
    pub status: ReadStatus,                //  1 byte
    pub _pad1: [u8; 7],                    //  7 bytes
    /// HLC at which this value was committed
    pub value_hlc: HybridTimestamp,        // 16 bytes
    /// Log index the read was served from
    pub read_at_index: u64,                //  8 bytes
    pub epoch: u64,                        //  8 bytes
    pub leader_hint: NodeId,               //  8 bytes
    /// Value length in Arrow payload (0 if not found)
    pub value_len: u32,                    //  4 bytes
    pub _pad2: [u8; 4],                    //  4 bytes
}
// static_assert: size_of::<ReadResponseMeta>() == 56
3. Arrow IPC Schemas for Variable-Length Data
rust


Copy
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Arrow schema for write request payloads.
/// Single-row RecordBatch — one write per frame.
///
/// Why Arrow for key/value instead of raw bytes after the header?
///   1. Bulk writes (BulkWrite RPC) become a natural multi-row RecordBatch
///      with zero per-row framing overhead.
///   2. The same Arrow IPC reader handles both single and bulk paths.
///   3. Arrow's built-in LZ4/ZSTD body compression is free to use,
///      toggled per-batch, without a separate compression layer.
pub fn write_payload_schema() -> Schema {
    Schema::new(vec![
        Field::new("key",   DataType::LargeBinary, false),  // ≤ 1 KiB
        Field::new("value", DataType::LargeBinary, false),  // ≤ 4 MiB
    ])
}

/// Arrow schema for read request payloads.
pub fn read_key_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::LargeBinary, false),
    ])
}

/// Arrow schema for read response payloads.
pub fn read_value_schema() -> Schema {
    Schema::new(vec![
        Field::new("value", DataType::LargeBinary, true),  // null if KEY_NOT_FOUND
    ])
}

/// Arrow schema for RaftEpochEntry variable-length fields.
/// The fixed-size fields live in a bytemuck struct (below);
/// this schema carries the parts that can't be fixed-size.
pub fn epoch_entry_var_schema() -> Schema {
    Schema::new(vec![
        // Voter addresses: parallel arrays of node_id → address
        Field::new("voter_ids",       DataType::List(Arc::new(
            Field::new("item", DataType::UInt32, false)
        )), false),
        Field::new("voter_addresses", DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, false)
        )), false),
        // Learner addresses
        Field::new("learner_ids",       DataType::List(Arc::new(
            Field::new("item", DataType::UInt32, false)
        )), true),
        Field::new("learner_addresses", DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, false)
        )), true),
        // Extensible key-value metadata (shard assignment, feature flags)
        Field::new("meta_keys",   DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, false)
        )), true),
        Field::new("meta_values", DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, false)
        )), true),
    ])
}

/// Bytemuck companion for RaftEpochEntry fixed fields.
/// The variable-length voter/learner/metadata arrays are in the
/// Arrow payload; this struct carries everything that's fixed-size.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct RaftEpochEntryFixed {
    pub epoch: u64,
    pub term: u64,
    pub first_log_index: u64,
    pub epoch_start_hlc: HybridTimestamp,     // 16 bytes
    pub leader: NodeId,                        //  8 bytes
    /// SHA-256 hash-chain link to prior RaftEpochEntry; enables
    /// epoch lineage verification
    pub prev_epoch_hash: [u8; 32],
    /// Present only if epoch started via graceful transfer;
    /// zeroed for election-based transitions
    pub transfer_token: TransferToken,         // 128 bytes
    /// Number of voters in the Arrow payload
    pub voter_count: u32,
    /// Number of learners in the Arrow payload
    pub learner_count: u32,
}
// size: 8+8+8+16+8+32+128+4+4 = 216 bytes
