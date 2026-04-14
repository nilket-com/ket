use arrow::datatypes::{DataType, Field, Schema};
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================
//  HYBRID LOGICAL CLOCK — 16 bytes, fixed
//
//  Comparison order: physical_ms → logical → node_id.
//  Merge rule at leader: max(local, remote) + 1 on logical.
//  physical_ms must not drift >250ms from NTP source.
//  node_id breaks ties deterministically — set to Raft node
//  ID of the stamp originator.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct HybridTimestamp {
    /// Wall-clock milliseconds since Unix epoch (48 bits used, upper 16 reserved)
    pub physical_ms: u64,
    /// Monotonic counter; reset to 0 when physical_ms advances
    pub logical: u32,
    /// Originating node ID (the numeric `id` portion of a `NodeId`); breaks ties for identical (physical, logical) pairs
    pub node_id: u32,
}

// ============================================================
//  NODE IDENTITY — 8 bytes, fixed
//
//  Address resolution is out-of-band via the cluster config
//  (RaftEpochEntry.voters / learners arrow payload).
//  The wire ID is a pure numeric handle.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct NodeId {
    /// Cluster-unique numeric ID assigned at bootstrap
    pub id: u32,
    /// Reserved for future use (e.g., generation counter). Must be 0.
    pub _reserved: u32,
}

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
    /// Reserved slot inside the signed payload; zero-filled until
    /// assigned semantics (e.g. anti-replay nonce). Keeps the signed
    /// region at a round 56 bytes.
    pub _signed_reserved: u64,
    // --- 56 bytes above are the signed payload ---
    /// Ed25519 signature over SHA-512(fields above); fixed 64 bytes
    pub signature: [u8; 64],
    /// Pad to 128 bytes
    pub _pad: [u8; 8],
}

// ============================================================
//  FINALIZATION MARKER — 88 bytes, fixed
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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
    /// Reserved slot; zero-filled until assigned. Exists so the struct
    /// lands on the 88-byte size contract asserted below.
    pub _reserved: u64,
}

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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
    #[serde(with = "serde_bytes")]
    pub signature: [u8; 64],
    /// Pad to 128 bytes
    pub _pad: [u8; 8],
}

// ============================================================
//  FRAME HEADER — 24 bytes, fixed
//
//  The universal envelope for all Wendelon protocol messages.
// ============================================================
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct FrameHeader {
    /// Magic bytes: 0x48524654 = "HRFT"
    pub magic: u32,
    /// Protocol version; currently 1
    pub version: u8,
    /// Message type discriminant (see MsgType)
    pub msg_type: u8,
    /// Bitfield: bit0=arrow_compressed, bit1=has_trailing
    pub flags: u16,
    /// QUIC stream ID or TCP-mux logical stream
    pub stream_id: u32,
    /// Byte length of the metadata section (bytemuck struct)
    pub meta_len: u32,
    /// Byte length of the Arrow IPC section (0 = no Arrow payload)
    pub arrow_len: u32,
    /// Byte length of trailing bytemuck struct (0 = none)
    pub trailing_len: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MsgType(pub u8);

impl MsgType {
    // data ring
    pub const WRITE_REQ:       Self = Self(0x01);
    pub const WRITE_RESP:      Self = Self(0x02);
    pub const READ_REQ:        Self = Self(0x03);
    pub const READ_RESP:       Self = Self(0x04);
    pub const BULK_WRITE_REQ:  Self = Self(0x05);  // Arrow has multiple rows
    pub const BULK_WRITE_RESP: Self = Self(0x06);
    pub const STREAM_FIN_REQ:  Self = Self(0x07);  // StreamFinalization subscribe
    pub const STREAM_FIN_EVT:  Self = Self(0x08);  // FinalizationMarker push

    // control ring
    pub const TRANSFER_REQ:    Self = Self(0x10);
    pub const TRANSFER_RESP:   Self = Self(0x11);
    pub const LEASE_REQ:       Self = Self(0x12);
    pub const LEASE_RESP:      Self = Self(0x13);
    pub const EPOCH_ADV_REQ:   Self = Self(0x14);
    pub const EPOCH_ADV_RESP:  Self = Self(0x15);
    pub const STATUS_REQ:      Self = Self(0x16);
    pub const STATUS_RESP:     Self = Self(0x17);

    // raft internal (openraft RaftNetwork)
    pub const APPEND_ENTRIES_REQ:  Self = Self(0x20);
    pub const APPEND_ENTRIES_RESP: Self = Self(0x21);
    pub const INSTALL_SNAPSHOT_REQ:  Self = Self(0x22);
    pub const INSTALL_SNAPSHOT_RESP: Self = Self(0x23);
    pub const VOTE_REQ:  Self = Self(0x24);
    pub const VOTE_RESP: Self = Self(0x25);
}

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
    pub const QUOTA_EXCEEDED:    Self = Self(8);
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

// ============================================================
//  SLAB STORAGE METADATA
// ============================================================

#[derive(Clone, Copy, Debug, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct SlabRecordHeader {
    pub magic: u32,             // 0x534C4142 ("SLAB")
    pub _pad_align: u32,        // 4 bytes padding for u64 alignment
    pub key_hash: u64,          // The consistent hash of the key
    pub hlc: HybridTimestamp,   // 16 bytes: the timestamp of the write
    pub frame_len: u32,         // Total length of the HRFT frame that follows
    pub _pad: u32,              // 4 bytes padding to reach 40-byte alignment
}

#[derive(Clone, Copy, Debug, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct SlabManifest {
    pub slab_id: u64,
    /// 0 = Active (NVMe), 1 = Sealed (NVMe), 2 = Uploading, 3 = Stubbed (S3)
    pub state: u8,
    pub _pad: [u8; 7],
    pub min_hlc: HybridTimestamp, // Timestamp of first write
    pub max_hlc: HybridTimestamp, // Timestamp of last write
    pub total_bytes: u64,         // e.g., 1,073,741,824 (1GB)
    pub chunk_size: u32,          // e.g., 16,777,216 (16MB)
    /// Heatmap: counter of reads in the last 24h
    pub temperature: u32,         
    /// Remote routing info (zeroed if local)
    pub remote_bucket: [u8; 32],  // e.g., "nilket-cold-tier-1"
    #[serde(with = "serde_bytes")]
    pub remote_key: [u8; 64],     // e.g., "wendelon/epoch_5/slab_123.wnd"
}

// ============================================================
//  ARROW IPC SCHEMAS
// ============================================================

/// Arrow schema for write request payloads.
/// Single-row RecordBatch — one write per frame.
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

// ============================================================
//  RAFT EPOCH ENTRY FIXED
// ============================================================

/// Bytemuck companion for RaftEpochEntry fixed fields.
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

// Static assertions to ensure all our size calculations remain exactly correct
const _: () = assert!(std::mem::size_of::<HybridTimestamp>() == 16);
const _: () = assert!(std::mem::size_of::<NodeId>() == 8);
const _: () = assert!(std::mem::size_of::<ClientWriteId>() == 32);
const _: () = assert!(std::mem::size_of::<TransferToken>() == 128);
const _: () = assert!(std::mem::size_of::<FinalizationMarker>() == 88);
const _: () = assert!(std::mem::size_of::<LeaseDescriptor>() == 128);
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 24);
const _: () = assert!(std::mem::size_of::<WriteRequestMeta>() == 80);
const _: () = assert!(std::mem::size_of::<WriteResponseMeta>() == 96);
const _: () = assert!(std::mem::size_of::<ReadRequestMeta>() == 40);
const _: () = assert!(std::mem::size_of::<ReadResponseMeta>() == 56);
const _: () = assert!(std::mem::size_of::<SlabRecordHeader>() == 40);
const _: () = assert!(std::mem::size_of::<SlabManifest>() == 160);
const _: () = assert!(std::mem::size_of::<RaftEpochEntryFixed>() == 216);
