use arrow::datatypes::{DataType, Field, Schema};
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
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
#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Pod, Zeroable, Serialize, Deserialize)]
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
//  STREAM-FINALIZATION REQUEST — 16 bytes, fixed
//
//  Opens a long-lived HRFT bidi stream. The client sends exactly
//  one `STREAM_FIN_REQ` carrying this meta, then the server pushes
//  `STREAM_FIN_EVT` frames whose meta section is a
//  [`FinalizationMarker`].
//
//  Resume semantics:
//    `start_log_index` is an exclusive floor. Reconnect with the
//    last fully received `FinalizationMarker.log_index` to resume
//    without duplicates.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct StreamFinalizationRequestMeta {
	/// Exclusive replay floor. The first eligible marker has
	/// `log_index > start_log_index`.
	pub start_log_index: u64,
	/// HashRaft epoch to subscribe within. Replay and live push
	/// are both filtered to this epoch.
	pub epoch: u64,
}

// ============================================================
//  LEASE DESCRIPTOR — 144 bytes, fixed
//
//  Per-range leases (ADR 003 / plan 018) fence Stellarator writes
//  so only one data node accepts traffic for a given key_hash
//  range within an epoch. The same descriptor backs linearizable
//  reads: a node may serve `LINEARIZABLE` reads locally while a
//  valid lease covers the key.
//
//  • Granted by the Raft Leader once the entry commits.
//  • Each lease is scoped to the inclusive `[range_start,
//    range_end]` key_hash window and the specific epoch named in
//    `epoch` — a cluster transition to E+1 invalidates every
//    lease carrying a prior epoch.
//  • Expires at `lease_expiry` (HLC-based, NOT wall-clock).
//  • Reads require commit_index >= `min_commit_index` to prevent
//    stale reads after re-election (kept from the old layout —
//    the write-path gate ignores it until read-lease semantics
//    land in a follow-up).
//  • `issuer` is the Control Ring leader that signed the grant.
//    Replicas verify both the Ed25519 signature and that the
//    issuer is still the current Raft leader before trusting the
//    descriptor at the data-ring gate.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct LeaseDescriptor {
	/// Epoch in which this lease is valid
	pub epoch: u64,
	/// Node holding the lease (a data-capable primary)
	pub holder: NodeId,
	/// Inclusive lower bound of the `key_hash` range this lease
	/// authorises writes/linearizable-reads for.
	pub range_start: u64,
	/// Inclusive upper bound.
	pub range_end: u64,
	/// HLC when lease was granted
	pub lease_start: HybridTimestamp,
	/// HLC when lease becomes invalid; recommended: heartbeat_interval × 3
	pub lease_expiry: HybridTimestamp,
	/// Reads must have commit_index >= this to prevent post-election stale reads
	pub min_commit_index: u64,
	/// Control-ring leader that signed this lease.
	pub issuer: NodeId,
	/// Ed25519 over fields above; 64 bytes
	#[serde(with = "serde_bytes")]
	pub signature: [u8; 64],
}

// ============================================================
//  LEASE STATUS — 1 byte, fixed
//
//  Wire discriminant for [`AcquireLeaseResponseMeta`]. Values are
//  stable because the byte rides HRFT frames from Data Nodes to
//  the Control Ring.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct LeaseStatus(pub u8);

impl LeaseStatus {
	pub const GRANTED: Self = Self(0);
	pub const DENIED_OVERLAP: Self = Self(1);
	pub const DENIED_EPOCH_STALE: Self = Self(2);
	pub const DENIED_NOT_LEADER: Self = Self(3);
	pub const INTERNAL_ERROR: Self = Self(4);
	pub const DENIED_INVALID_SIGNATURE: Self = Self(5);
}

// ============================================================
//  ACQUIRE LEASE REQUEST METADATA — 56 bytes, fixed
//
//  Sent by a Data Node on the Control Ring when it wants to be
//  fenced as the primary for a key_hash range. The Raft Leader
//  proposes a `WendelonRequest::AcquireLease` entry on the
//  requester's behalf; the resolved `LeaseDescriptor` rides back
//  in a `LEASE_RESP` trailing section on success.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct AcquireLeaseRequestMeta {
	/// Inclusive lower bound of the `key_hash` range the
	/// requester wants to own as primary.
	pub range_start: u64, //  8
	/// Inclusive upper bound.
	pub range_end: u64, //  8
	/// Requester's believed cluster epoch. The Leader rejects the
	/// grant with `DENIED_EPOCH_STALE` on mismatch so the caller
	/// refreshes topology rather than forking.
	pub epoch: u64, //  8
	/// Identity of the requesting data node.
	pub holder: NodeId, //  8
	/// HLC stamped at request time; reused as `lease_start` if
	/// the grant succeeds so the resulting lease's validity
	/// window is well-defined against the cluster clock.
	pub requested_hlc: HybridTimestamp, // 16
	/// Requested time-to-live in milliseconds. The Leader clamps
	/// against a server-side ceiling before stamping
	/// `lease_expiry`.
	pub ttl_ms: u32, //  4
	pub _pad: u32, //  4
}

// ============================================================
//  ACQUIRE LEASE RESPONSE METADATA — 8 bytes, fixed
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct AcquireLeaseResponseMeta {
	pub status: LeaseStatus, //  1
	pub _pad: [u8; 7],       //  7
}

// ============================================================
//  ROLLING-UPGRADE VERSION WINDOW
// ============================================================

/// Fixed HRFT envelope version carried in [`FrameHeader.version`].
///
/// This is the framing contract, not the rolling-upgrade skew
/// window. A peer sending any other envelope version is rejected
/// before payload decode.
pub const HRFT_ENVELOPE_VERSION: u8 = 1;

/// Current cluster protocol version advertised by this build.
///
/// Rolling upgrades negotiate inside the compatibility window
/// rather than requiring an exact match on this number.
pub const CLUSTER_PROTOCOL_VERSION: u16 = 1;

/// Oldest cluster protocol version this build still supports.
pub const MIN_COMPATIBLE_CLUSTER_PROTOCOL_VERSION: u16 = 1;

/// Rolling-upgrade compatibility window for one node.
///
/// The node supports every protocol version in the inclusive
/// interval `[min_compatible, current]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct ProtocolVersionWindow {
	pub current: u16,
	pub min_compatible: u16,
}

impl ProtocolVersionWindow {
	pub const fn new(current: u16, min_compatible: u16) -> Self {
		Self {
			current,
			min_compatible,
		}
	}

	pub const fn exact(version: u16) -> Self {
		Self::new(version, version)
	}
}

impl Default for ProtocolVersionWindow {
	fn default() -> Self {
		Self::new(
			CLUSTER_PROTOCOL_VERSION,
			MIN_COMPATIBLE_CLUSTER_PROTOCOL_VERSION,
		)
	}
}

impl fmt::Display for ProtocolVersionWindow {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}..={}", self.min_compatible, self.current)
	}
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
	/// HRFT envelope version; currently [`HRFT_ENVELOPE_VERSION`]
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

/// Generic HRFT flag: the current frame is the terminal item in a
/// multi-frame response stream. Query streaming uses this on the
/// last `QUERY_RESP` frame so the client can stop without relying
/// on transport EOF as a framing signal.
pub const FRAME_FLAG_STREAM_END: u16 = 1 << 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MsgType(pub u8);

impl MsgType {
	// data ring
	pub const WRITE_REQ: Self = Self(0x01);
	pub const WRITE_RESP: Self = Self(0x02);
	pub const READ_REQ: Self = Self(0x03);
	pub const READ_RESP: Self = Self(0x04);
	// Client-streaming bulk write. One bidi stream carries many
	// `BULK_WRITE_REQ` item frames, each with the same fixed
	// `WriteRequestMeta` + Arrow payload shape as `WRITE_REQ`.
	// The server replies with one `BULK_WRITE_RESP` per item.
	pub const BULK_WRITE_REQ: Self = Self(0x05);
	pub const BULK_WRITE_RESP: Self = Self(0x06);
	pub const STREAM_FIN_REQ: Self = Self(0x07); // StreamFinalization subscribe
	pub const STREAM_FIN_EVT: Self = Self(0x08); // FinalizationMarker push
	// Stellarator replica fan-out (ADR 003). The Data Ring front
	// door uses these to replicate Arrow IPC bytes from the
	// accepting node to the remaining replica set members in
	// parallel — quorum acks on REPLICATE_RESP gate the
	// `FinalizationMarker` handoff to Raft.
	pub const REPLICATE_REQ: Self = Self(0x09);
	pub const REPLICATE_RESP: Self = Self(0x0A);
	// Pushdown scan from a Smart Client SDK (ADR 005 + ADR 004).
	// The client frames a WPBC program over a key-hash range and
	// the server streams one or more QUERY_RESP frames carrying the
	// filtered Arrow IPC result back on the same bidi stream.
	pub const QUERY_REQ: Self = Self(0x0B);
	pub const QUERY_RESP: Self = Self(0x0C);
	// Peer-only lazy-migration RPCs (ADR 006 + Plan 019). A new
	// owner pulls retained key versions from a prior physical
	// holder when a ledger entry names the range `Pending`. The
	// RPC bypasses lease checks but validates the migration id and
	// pinned `migration_hlc_floor` against the state machine's
	// Migration Ledger.
	pub const MIGRATE_REQ: Self = Self(0x0D);
	pub const MIGRATE_RESP: Self = Self(0x0E);

	// control ring
	pub const TRANSFER_REQ: Self = Self(0x10);
	pub const TRANSFER_RESP: Self = Self(0x11);
	pub const LEASE_REQ: Self = Self(0x12);
	pub const LEASE_RESP: Self = Self(0x13);
	pub const EPOCH_ADV_REQ: Self = Self(0x14);
	pub const EPOCH_ADV_RESP: Self = Self(0x15);
	pub const STATUS_REQ: Self = Self(0x16);
	pub const STATUS_RESP: Self = Self(0x17);
	pub const JOIN_REQ: Self = Self(0x18);
	pub const JOIN_RESP: Self = Self(0x19);

	// raft internal (openraft RaftNetwork)
	pub const APPEND_ENTRIES_REQ: Self = Self(0x20);
	pub const APPEND_ENTRIES_RESP: Self = Self(0x21);
	pub const INSTALL_SNAPSHOT_REQ: Self = Self(0x22);
	pub const INSTALL_SNAPSHOT_RESP: Self = Self(0x23);
	pub const VOTE_REQ: Self = Self(0x24);
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

/// Durability level for client writes.
///
/// M1 freezes `QUORUM` to mean "payload bytes durably flushed on a
/// quorum of replicas" before the leader finalizes the write in
/// Raft. `LEADER` and `NONE` remain accepted for API stability, but
/// the implementation intentionally stays conservative until a later
/// plan widens their latency/guarantee contract explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct DurabilityLevel(pub u8);

impl DurabilityLevel {
	/// Payload durably flushed in the destination slab file on a
	/// quorum of replicas before finalization is committed.
	pub const QUORUM: Self = Self(0);
	/// Reserved for a future "leader durable only" contract. M1
	/// still routes this through the conservative quorum-durable
	/// path rather than claiming a weaker guarantee.
	pub const LEADER: Self = Self(1);
	/// Reserved for a future fire-and-forget path. M1 still routes
	/// this through the conservative quorum-durable path.
	pub const NONE: Self = Self(2);
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WriteRequestMeta {
	/// REQUIRED — exactly-once idempotency key
	pub write_id: ClientWriteId, // 32 bytes
	/// Client's current HLC; leader merges max(leader_hlc, client_hlc)+1
	pub client_hlc: HybridTimestamp, // 16 bytes
	/// Client's believed epoch; leader rejects with EPOCH_STALE on mismatch
	pub expected_epoch: u64, //  8 bytes
	/// 0 = direct to leader; incremented by each proxy; max 2
	pub proxy_hop: u8, //  1 byte
	/// Durability level: 0=QUORUM, 1=LEADER, 2=NONE
	pub durability: DurabilityLevel, //  1 byte
	/// If 1, leader streams back FinalizationMarker in the response
	pub await_finalization: u8, //  1 byte
	pub _pad1: [u8; 5], //  5 bytes alignment
	/// Set by proxy follower for response routing; zeroed on direct writes
	pub proxy_source: NodeId, //  8 bytes
	/// Key length in the Arrow payload (for fast routing without Arrow parse)
	pub key_len: u32, //  4 bytes
	/// Value length in the Arrow payload
	pub value_len: u32, //  4 bytes
}

// ============================================================
//  WRITE RESPONSE METADATA — 96 bytes, fixed
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct WriteStatus(pub u8);

impl WriteStatus {
	pub const OK: Self = Self(0);
	pub const EPOCH_STALE: Self = Self(1);
	pub const NOT_LEADER: Self = Self(2);
	pub const DUPLICATE: Self = Self(3); // Idempotency cache hit; original replayed
	pub const PROXY_LIMIT: Self = Self(4); // proxy_hop > 2
	pub const PAYLOAD_TOO_LARGE: Self = Self(5);
	pub const LEASE_EXPIRED: Self = Self(6);
	pub const INVALID_LEASE_SIGNATURE: Self = Self(7);
	pub const INTERNAL_ERROR: Self = Self(8);
	pub const QUOTA_EXCEEDED: Self = Self(9);
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WriteResponseMeta {
	pub status: WriteStatus, //  1 byte
	pub _pad1: [u8; 7],      //  7 bytes
	/// Raft log index of committed entry (0 on error)
	pub log_index: u64, //  8 bytes
	pub term: u64,           //  8 bytes
	pub epoch: u64,          //  8 bytes
	/// Authoritative HLC of the commit
	pub commit_hlc: HybridTimestamp, // 16 bytes
	/// Populated on NOT_LEADER / EPOCH_STALE for client redirect
	pub leader_hint: NodeId, //  8 bytes
	/// Echoed back for client correlation
	pub echoed_write_id: ClientWriteId, // 32 bytes
	/// 1 if a FinalizationMarker follows this frame; 0 otherwise
	pub has_finalization: u8, //  1 byte
	pub _pad2: [u8; 7],      //  7 bytes
}

// ============================================================
//  REPLICATE REQUEST METADATA — 64 bytes, fixed
//
//  Carried on REPLICATE_REQ frames during Stellarator fan-out
//  (ADR 003). The accepting node bifurcates: local slab append +
//  parallel REPLICATE_REQ frames to peers; a quorum of durable
//  REPLICATE_RESP acknowledgements gates the 88-byte
//  FinalizationMarker on the Control Ring. Payload bytes ride in
//  the frame's arrow section exactly as they will land in the slab.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReplicaWriteMeta {
	/// HLC chosen by the accepting (origin) node. Replicas index
	/// the append under this timestamp so recovery sees a
	/// consistent cross-node ordering.
	pub commit_hlc: HybridTimestamp, // 16 bytes
	/// Epoch of the originating leader — replicas reject on
	/// mismatch against their own current epoch.
	pub epoch: u64, //  8 bytes
	/// HighwayHash of the key bytes. Replicas use this as the
	/// slab index key rather than re-hashing the Arrow payload.
	pub key_hash: u64, //  8 bytes
	/// Blake3-256 of the Arrow IPC bytes. Replicas verify the
	/// payload before ack so corruption surfaces as a failed
	/// ack rather than a silent divergence.
	pub payload_hash: [u8; 32], // 32 bytes
}

// ============================================================
//  REPLICATE RESPONSE METADATA — 8 bytes, fixed
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReplicaWriteStatus(pub u8);

impl ReplicaWriteStatus {
	pub const OK: Self = Self(0);
	pub const EPOCH_STALE: Self = Self(1);
	pub const HASH_MISMATCH: Self = Self(2);
	pub const INTERNAL_ERROR: Self = Self(3);
}

/// Explicit acknowledgement strength carried back from a replica.
///
/// The leader must count only [`Self::DURABLE`] toward quorum
/// durability. `APPEND_ONLY` exists so tests and later latency work
/// can model "bytes accepted but not durably flushed" without a
/// second wire-shape change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReplicaAckLevel(pub u8);

impl ReplicaAckLevel {
	/// No positive acknowledgement was earned; typically paired
	/// with a non-OK [`ReplicaWriteStatus`].
	pub const NONE: Self = Self(0);
	/// Payload bytes were appended but not durably flushed. The
	/// leader must not count this toward quorum durability.
	pub const APPEND_ONLY: Self = Self(1);
	/// Payload bytes were durably flushed in the destination slab
	/// file and may count toward quorum durability.
	pub const DURABLE: Self = Self(2);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReplicaWriteRespMeta {
	pub status: ReplicaWriteStatus, //  1 byte
	/// Strength of the acknowledgement carried by this response.
	/// Leaders may count only `DURABLE` toward quorum.
	pub ack_level: ReplicaAckLevel, //  1 byte
	pub _pad: [u8; 6],              //  6 bytes
}

// ============================================================
//  READ REQUEST METADATA — 56 bytes, fixed
//  Actual key is in the Arrow payload.
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct Consistency(pub u8);

impl Consistency {
	pub const LINEARIZABLE: Self = Self(0); // Requires valid lease or quorum check
	pub const SERIALIZABLE: Self = Self(1); // Leader's committed state, no lease needed
	pub const STALE: Self = Self(2); // Any replica, no freshness guarantee
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct ReadRequestMeta {
	/// Optional: block until commit_hlc >= this (causal read-after-write)
	pub min_hlc: HybridTimestamp, // 16 bytes
	pub expected_epoch: u64,      //  8 bytes
	pub consistency: Consistency, //  1 byte
	pub _pad: [u8; 3],            //  3 bytes
	/// Key length in Arrow payload (for fast routing)
	pub key_len: u32, //  4 bytes
	pub _pad2: [u8; 8],           //  8 bytes
	/// Time-travel upper bound (ADR 013). When non-zero, the
	/// server returns the newest version whose `hlc <= as_of_hlc`
	/// instead of the latest value. All-zero is the wire sentinel
	/// for "no upper bound" and preserves the default read path.
	pub as_of_hlc: HybridTimestamp, // 16 bytes
}

// ============================================================
//  READ RESPONSE METADATA — 56 bytes, fixed
//  Value is in the Arrow payload (omitted when KEY_NOT_FOUND).
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct ReadStatus(pub u8);

impl ReadStatus {
	pub const OK: Self = Self(0);
	pub const KEY_NOT_FOUND: Self = Self(1);
	pub const EPOCH_STALE: Self = Self(2);
	pub const NOT_LEADER: Self = Self(3);
	pub const LEASE_EXPIRED: Self = Self(4);
	pub const INVALID_LEASE_SIGNATURE: Self = Self(5);
	pub const E_SAFE_BEHIND: Self = Self(6); // Follower's e_safe < required; retry leader
	/// The query path could not complete its lazy-migration fan-out
	/// (Plan 019). One or more ledger-overlapping source replicas
	/// were unreachable or returned a non-OK status, so a local
	/// scan would miss versions only held on those sources. The
	/// client must retry; the answer is explicitly *not* partial.
	pub const MIGRATION_INCOMPLETE: Self = Self(7);
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct ReadResponseMeta {
	pub status: ReadStatus, //  1 byte
	pub _pad1: [u8; 7],     //  7 bytes
	/// HLC at which this value was committed
	pub value_hlc: HybridTimestamp, // 16 bytes
	/// Log index the read was served from
	pub read_at_index: u64, //  8 bytes
	pub epoch: u64,         //  8 bytes
	pub leader_hint: NodeId, //  8 bytes
	/// Value length in Arrow payload (0 if not found)
	pub value_len: u32, //  4 bytes
	pub _pad2: [u8; 4],     //  4 bytes
}

// ============================================================
//  QUERY REQUEST METADATA — 72 bytes, fixed
//
//  Carries the fixed envelope for a Smart Client pushdown scan
//  (ADR 005). The WPBC bytecode itself rides in the frame's
//  trailing section so it does not bloat this POD; the Arrow
//  section is unused on the request side.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct QueryRequestMeta {
	/// Causal read-after-write bound. Server waits until its
	/// observed `e_safe_hlc` for `expected_epoch` >= this, or
	/// returns `E_SAFE_BEHIND` so the client re-routes.
	pub min_hlc: HybridTimestamp, // 16 bytes
	pub expected_epoch: u64,      //  8 bytes
	pub consistency: Consistency, //  1 byte
	pub _pad: [u8; 7],            //  7 bytes
	/// Inclusive lower bound of the key_hash range to scan.
	pub key_range_start: u64, //  8 bytes
	/// Inclusive upper bound.
	pub key_range_end: u64, //  8 bytes
	/// Length of the WPBC bytecode in the trailing section. Zero
	/// means "no filter, all rows in range".
	pub bytecode_len: u32, //  4 bytes
	pub _pad2: [u8; 4],           //  4 bytes
	/// Time-travel upper bound (ADR 013). When non-zero, the scan
	/// yields the newest version per key whose `hlc <= as_of_hlc`
	/// and skips keys whose first version is still in the future.
	/// All-zero is the sentinel for "no upper bound".
	pub as_of_hlc: HybridTimestamp, // 16 bytes
}

// ============================================================
//  QUERY RESPONSE METADATA — 56 bytes, fixed
//
//  Response envelope for pushdown scans. The filtered Arrow IPC
//  result rides in the frame's arrow section; `rows` is the row
//  count after WPBC filter + project and lets the client size its
//  downstream collection without walking the batch first.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct QueryResponseMeta {
	/// Reuses [`ReadStatus`] so `NOT_LEADER` / `E_SAFE_BEHIND` /
	/// `EPOCH_STALE` redirect hints round-trip unchanged.
	pub status: ReadStatus, //  1 byte
	pub _pad1: [u8; 7], //  7 bytes
	/// Highest HLC observed in the returned rows (zero on error
	/// or empty result).
	pub value_hlc: HybridTimestamp, // 16 bytes
	pub epoch: u64,     //  8 bytes
	/// On `NOT_LEADER` / `E_SAFE_BEHIND`: the node the client
	/// should retry against.
	pub leader_hint: NodeId, //  8 bytes
	/// Row count after filter + project.
	pub rows: u64, //  8 bytes
	/// Byte length of the Arrow IPC section on the *first*
	/// `QUERY_RESP` frame. Continuation frames carry their Arrow
	/// length in the enclosing [`FrameHeader`].
	pub arrow_len: u32, //  4 bytes
	pub _pad2: [u8; 4], //  4 bytes
}

// ============================================================
//  MIGRATE REQUEST METADATA — 96 bytes, fixed
//
//  Lazy-migration pull frame (ADR 006 + Plan 019). The new owner
//  asks a prior physical holder for retained versions in its
//  `[range_start, range_end]` window whose `hlc >=
//  migration_hlc_floor`. The server validates the request against
//  its state-machine Migration Ledger and returns raw retained
//  records in an Arrow IPC batch shaped by
//  [`migration_record_schema`].
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MigrateRequestMeta {
	pub migration_id: u64, //  8
	/// The destination's belief about the current ownership epoch.
	/// The server rejects with `STALE_LEDGER` if its own ledger
	/// entry has advanced past this value.
	pub expected_to_epoch: u64, //  8
	/// Identity of the node pulling the data. Must match the
	/// ledger's `logical_owner` for this migration.
	pub requester: NodeId, //  8
	/// Inclusive lower bound of the hash range being pulled.
	pub range_start: u64, //  8
	/// Inclusive upper bound.
	pub range_end: u64, //  8
	/// Pinned MVCC cutoff. Every version with `hlc >=
	/// migration_hlc_floor` is migration-eligible; older versions
	/// fall outside the ADR 013 retention window and are skipped.
	/// Must match the floor stored on the server's ledger entry.
	pub migration_hlc_floor: HybridTimestamp, // 16
	/// Pagination cursor — key hash the prior page stopped within.
	/// Zero together with `cursor_hlc` means "start at the range
	/// floor"; non-zero means the source was in the middle of
	/// emitting versions for this key when `max_records` /
	/// `max_bytes` tripped.
	pub cursor_hash: u64, //  8
	/// HLC of the last version emitted for `cursor_hash` on the
	/// prior page. Combined with `cursor_hash`, it lets the source
	/// resume strictly after the last emitted MVCC slot so a fat
	/// key with more versions than fit in one page is still
	/// streamed exhaustively.
	pub cursor_hlc: HybridTimestamp, // 16
	/// Server-enforced caps on the response size.
	pub max_records: u32, //  4
	pub max_bytes: u32,    //  4
	/// 0 = PointLookup, 1 = RangePage. Point lookups return every
	/// retained version of exactly one key; range pages walk the
	/// inclusive window keyed by `(cursor_hash, cursor_hlc)`.
	pub kind: MigrateRequestKind, //  1
	pub _pad: [u8; 7],     //  7
}

/// Request kind discriminant rides inside [`MigrateRequestMeta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MigrateRequestKind(pub u8);

impl MigrateRequestKind {
	/// Serve every retained version of the single key whose hash
	/// matches `range_start` (and implicitly `range_end`). Used by
	/// the read-path fallback for point reads.
	pub const POINT_LOOKUP: Self = Self(0);
	/// Serve up to (`max_records`, `max_bytes`) of retained
	/// versions in the inclusive `[range_start, range_end]` window,
	/// keyed off `cursor`. Used by the background sweeper.
	pub const RANGE_PAGE: Self = Self(1);
}

// ============================================================
//  MIGRATE RESPONSE METADATA — 64 bytes, fixed
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MigrateResponseMeta {
	pub status: MigrateStatus, //  1
	/// 1 when the server has more pages beyond
	/// `(next_cursor_hash, next_cursor_hlc)`; 0 when this response
	/// exhausted the source for the pinned floor.
	pub has_more: u8, //  1
	pub _pad0: [u8; 6],        //  6
	/// Echo of the request's `migration_id` so the caller can
	/// correlate responses from fan-out pulls.
	pub migration_id: u64, //  8
	/// Populated on `REDIRECT` — the node the caller should retry
	/// against. Zero for every other status.
	pub source_hint: NodeId, //  8
	/// Key hash at which pagination should resume. Zero together
	/// with `next_cursor_hlc` when `has_more == 0`.
	pub next_cursor_hash: u64, //  8
	/// HLC of the last version emitted on this page for
	/// `next_cursor_hash`. The follow-up request must skip
	/// versions at or below this HLC so fat keys stream cleanly.
	pub next_cursor_hlc: HybridTimestamp, // 16
	/// Number of retained versions packed into the response Arrow
	/// batch. Lets the caller pre-size its ingest loop.
	pub versions_returned: u64, //  8
	/// Byte length of the Arrow IPC section. Redundant with the
	/// frame header's `arrow_len` but kept for schema symmetry
	/// with the other response PODs in this module.
	pub bytes_returned: u32, //  4
	pub _pad1: [u8; 4],        //  4
}

/// Wire status byte for [`MigrateResponseMeta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MigrateStatus(pub u8);

impl MigrateStatus {
	pub const OK: Self = Self(0);
	/// The requester asked for a specific key via `POINT_LOOKUP`
	/// and the server found no retained versions at or above the
	/// pinned floor.
	pub const KEY_NOT_FOUND: Self = Self(1);
	/// The server knows a more-current physical holder for this
	/// range (for example, a chained migration completed on
	/// another node). `source_hint` names the preferred target.
	pub const REDIRECT: Self = Self(2);
	/// The requester's `expected_to_epoch` or `migration_id` no
	/// longer matches the server's ledger — the caller must
	/// refresh from Raft before retrying.
	pub const STALE_LEDGER: Self = Self(3);
	/// The requester is not the named `logical_owner` for the
	/// ledger entry. MIGRATE is a peer RPC, but the state machine
	/// still enforces that only the current owner may pull.
	pub const NOT_AUTHORIZED: Self = Self(4);
	pub const INTERNAL_ERROR: Self = Self(5);
}

// ============================================================
//  SLAB STORAGE METADATA
// ============================================================

#[derive(Clone, Copy, Debug, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct SlabRecordHeader {
	pub magic: u32,           // 0x534C4142 ("SLAB")
	pub _pad_align: u32,      // 4 bytes padding for u64 alignment
	pub key_hash: u64,        // The consistent hash of the key
	pub hlc: HybridTimestamp, // 16 bytes: the timestamp of the write
	pub frame_len: u32,       // Total length of the HRFT frame that follows
	/// Integrity checksum over the header prefix (all fields above,
	/// with `checksum` itself read as zero) concatenated with the
	/// frame bytes. Recovery and read paths validate this to detect
	/// torn writes that reach the disk with a valid-looking header
	/// but a truncated frame.
	pub checksum: u32,
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
	pub remote_bucket: [u8; 32], // e.g., "nilket-cold-tier-1"
	#[serde(with = "serde_bytes")]
	pub remote_key: [u8; 64], // e.g., "wendelon/epoch_5/slab_123.wnd"
}

// ============================================================
//  ARROW IPC SCHEMAS
// ============================================================

/// Arrow schema for write request payloads.
/// Single-row RecordBatch — one write per frame.
pub fn write_payload_schema() -> Schema {
	Schema::new(vec![
		Field::new("key", DataType::LargeBinary, false), // ≤ 1 KiB
		Field::new("value", DataType::LargeBinary, false), // ≤ 4 MiB
	])
}

/// Arrow schema for read request payloads.
pub fn read_key_schema() -> Schema {
	Schema::new(vec![Field::new("key", DataType::LargeBinary, false)])
}

/// Arrow schema for read response payloads.
pub fn read_value_schema() -> Schema {
	Schema::new(vec![
		Field::new("value", DataType::LargeBinary, true), // null if KEY_NOT_FOUND
	])
}

/// Arrow schema for migration record batches (Plan 019). Each
/// row is one retained version being streamed from a prior
/// physical holder to the new logical owner. `frame_bytes` is
/// the verbatim Arrow IPC payload the holder originally stored
/// on disk — the receiver re-appends it via
/// [`SlabManager::append_migration`](crate::storage::manager::SlabManager::append_migration)
/// without reshaping.
pub fn migration_record_schema() -> Schema {
	Schema::new(vec![
		Field::new("key_hash", DataType::UInt64, false),
		Field::new("hlc_physical_ms", DataType::UInt64, false),
		Field::new("hlc_logical", DataType::UInt32, false),
		Field::new("hlc_node_id", DataType::UInt32, false),
		Field::new("payload_hash", DataType::FixedSizeBinary(32), false),
		Field::new("frame_bytes", DataType::LargeBinary, false),
	])
}

/// Arrow schema for RaftEpochEntry variable-length fields.
pub fn epoch_entry_var_schema() -> Schema {
	Schema::new(vec![
		// Voter addresses: parallel arrays of node_id → address
		Field::new(
			"voter_ids",
			DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
			false,
		),
		Field::new(
			"voter_addresses",
			DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
			false,
		),
		// Learner addresses
		Field::new(
			"learner_ids",
			DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
			true,
		),
		Field::new(
			"learner_addresses",
			DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
			true,
		),
		// Extensible key-value metadata (shard assignment, feature flags)
		Field::new(
			"meta_keys",
			DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
			true,
		),
		Field::new(
			"meta_values",
			DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
			true,
		),
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
	pub epoch_start_hlc: HybridTimestamp, // 16 bytes
	pub leader: NodeId,                   //  8 bytes
	/// SHA-256 hash-chain link to prior RaftEpochEntry; enables
	/// epoch lineage verification
	pub prev_epoch_hash: [u8; 32],
	/// Present only if epoch started via graceful transfer;
	/// zeroed for election-based transitions
	pub transfer_token: TransferToken, // 128 bytes
	/// Number of voters in the Arrow payload
	pub voter_count: u32,
	/// Number of learners in the Arrow payload
	pub learner_count: u32,
}

// ============================================================
//  RAFT LOG ENTRY FIXED — 96 bytes, fixed
//
//  Fixed portion of every Raft log record written through the
//  Control Ring WAL. Followed on disk by a variable payload
//  (postcard-encoded `Entry<WendelonTypeConfig>` payload bytes
//  today; Arrow IPC `write_payload_schema` when key/value
//  mutations start flowing through Raft).
// ============================================================

/// Log entry kind discriminant (on-wire, fixed-size).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogEntryKind(pub u8);

impl LogEntryKind {
	pub const APPEND: Self = Self(0); // regular mutation / normal entry
	pub const CONFIG: Self = Self(1); // epoch or membership change
	pub const SNAPSHOT_MARKER: Self = Self(2); // snapshot boundary marker
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct RaftLogEntryFixed {
	pub kind: LogEntryKind,         //  1
	pub _pad1: [u8; 7],             //  7
	pub term: u64,                  //  8
	pub index: u64,                 //  8
	pub epoch: u64,                 //  8
	pub leader: NodeId,             //  8
	pub entry_hlc: HybridTimestamp, // 16
	pub payload_len: u32,           //  4
	pub _pad2: [u8; 4],             //  4
	/// 32-byte content digest over the payload bytes. Currently
	/// populated with a truncated blake3 hash; the field name
	/// retains `sha256` nomenclature for future migration to the
	/// ADR-specified SHA-256 digest without a layout change.
	pub payload_hash: [u8; 32], // 32
}

// ============================================================
//  WAL RECORD HEADER — 32 bytes, fixed
//
//  Preamble for every Control Ring WAL record. Sits at the
//  start of each page-aligned append and names the record type,
//  the total body length, and the Raft coordinates the record
//  pertains to.
// ============================================================

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct WALRecordHeader {
	/// 0 = LogEntry, 1 = SnapshotMarker, 2 = SnapshotInstall, 3 = Checkpoint
	pub record_type: u8, //  1
	pub _pad1: [u8; 7], //  7
	/// Bytes of body following this header, excluding the trailing CRC.
	pub body_len: u64, //  8
	/// Raft log index this record pertains to (0 for non-log records).
	pub log_index: u64, //  8
	/// Raft term at the index above.
	pub term: u64, //  8
}

// ============================================================
//  SNAPSHOT HEADER — 80 bytes, fixed
//
//  Prefix of every snapshot blob, both on-disk and inside the
//  in-memory `Cursor<Vec<u8>>` that `WendelonTypeConfig` uses as
//  its `SnapshotData`. Followed by the postcard-encoded state
//  machine blob; the optional idempotency-cache checkpoint is
//  reserved by `cache_checkpoint_len` for future wiring.
// ============================================================

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct SnapshotHeader {
	pub last_included_index: u64,      //  8
	pub last_included_term: u64,       //  8
	pub last_included_epoch: u64,      //  8
	pub snapshot_hlc: HybridTimestamp, // 16
	/// Content digest over the serialized state-machine blob.
	/// 32-byte field retained with `sha256` nomenclature for a
	/// future migration to SHA-256; currently filled with a
	/// truncated blake3 digest of the same blob.
	pub state_sha256: [u8; 32], // 32
	/// Byte length of the idempotency cache checkpoint that
	/// follows the state-machine blob. Zero until that path lands.
	pub cache_checkpoint_len: u32, //  4
	pub _pad: [u8; 4],                 //  4
}

// Static assertions to ensure all our size calculations remain exactly correct
const _: () = assert!(std::mem::size_of::<HybridTimestamp>() == 16);
const _: () = assert!(std::mem::size_of::<NodeId>() == 8);
const _: () = assert!(std::mem::size_of::<ClientWriteId>() == 32);
const _: () = assert!(std::mem::size_of::<TransferToken>() == 128);
const _: () = assert!(std::mem::size_of::<FinalizationMarker>() == 88);
const _: () = assert!(std::mem::size_of::<StreamFinalizationRequestMeta>() == 16);
const _: () = assert!(std::mem::size_of::<LeaseDescriptor>() == 144);
const _: () = assert!(std::mem::size_of::<LeaseStatus>() == 1);
const _: () = assert!(std::mem::size_of::<AcquireLeaseRequestMeta>() == 56);
const _: () = assert!(std::mem::size_of::<AcquireLeaseResponseMeta>() == 8);
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 24);
const _: () = assert!(std::mem::size_of::<WriteRequestMeta>() == 80);
const _: () = assert!(std::mem::size_of::<WriteResponseMeta>() == 96);
const _: () = assert!(std::mem::size_of::<ReplicaWriteMeta>() == 64);
const _: () = assert!(std::mem::size_of::<ReplicaAckLevel>() == 1);
const _: () = assert!(std::mem::size_of::<ReplicaWriteRespMeta>() == 8);
const _: () = assert!(std::mem::size_of::<ReadRequestMeta>() == 56);
const _: () = assert!(std::mem::size_of::<ReadResponseMeta>() == 56);
const _: () = assert!(std::mem::size_of::<QueryRequestMeta>() == 72);
const _: () = assert!(std::mem::size_of::<QueryResponseMeta>() == 56);
const _: () = assert!(std::mem::size_of::<MigrateRequestMeta>() == 96);
const _: () = assert!(std::mem::size_of::<MigrateResponseMeta>() == 64);
const _: () = assert!(std::mem::size_of::<MigrateRequestKind>() == 1);
const _: () = assert!(std::mem::size_of::<MigrateStatus>() == 1);
const _: () = assert!(std::mem::size_of::<SlabRecordHeader>() == 40);
const _: () = assert!(std::mem::size_of::<SlabManifest>() == 160);
const _: () = assert!(std::mem::size_of::<RaftEpochEntryFixed>() == 216);
const _: () = assert!(std::mem::size_of::<LogEntryKind>() == 1);
const _: () = assert!(std::mem::size_of::<RaftLogEntryFixed>() == 96);
const _: () = assert!(std::mem::size_of::<WALRecordHeader>() == 32);
const _: () = assert!(std::mem::size_of::<SnapshotHeader>() == 80);
