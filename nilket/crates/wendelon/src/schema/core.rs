use super::units::{Identity, TenantId};
use arrow::datatypes::{DataType, Field, Schema};
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Error, ErrorKind};
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

/// ADR 040 tombstone-visibility metadata carried by tiered slab
/// pointers.
///
/// `Untracked` is the conservative decode/default state for pointers
/// written before tombstone metadata existed. It is intentionally
/// distinct from `None`, which means "this pointer is tracked and no
/// tombstone has been recorded."
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TombstoneMetadata {
	#[default]
	Untracked,
	None,
	Earliest(HybridTimestamp),
}

impl TombstoneMetadata {
	pub const fn earliest(self) -> Option<HybridTimestamp> {
		match self {
			Self::Earliest(timestamp) => Some(timestamp),
			Self::Untracked | Self::None => None,
		}
	}

	pub const fn is_tracked(self) -> bool {
		matches!(self, Self::None | Self::Earliest(_))
	}

	pub const fn is_untracked(self) -> bool {
		matches!(self, Self::Untracked)
	}
}

/// ADR 042 supersession-visibility metadata carried by tiered slab
/// pointers.
///
/// `Untracked` is the conservative decode/default state for pointers
/// written before supersession metadata existed. It is intentionally
/// distinct from `None`, which means "this pointer is tracked and no
/// newer row version has been recorded for rows in the slab."
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupersessionMetadata {
	#[default]
	Untracked,
	None,
	Earliest(HybridTimestamp),
}

impl SupersessionMetadata {
	pub const fn earliest(self) -> Option<HybridTimestamp> {
		match self {
			Self::Earliest(timestamp) => Some(timestamp),
			Self::Untracked | Self::None => None,
		}
	}

	pub const fn is_tracked(self) -> bool {
		matches!(self, Self::None | Self::Earliest(_))
	}

	pub const fn is_untracked(self) -> bool {
		matches!(self, Self::Untracked)
	}
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

impl NodeId {
	pub fn format_wire(self) -> String {
		format!("n-{:02}", self.id)
	}

	pub fn parse_wire(raw: &str) -> std::io::Result<Self> {
		let id = raw
			.strip_prefix("n-")
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, format!("invalid node_id {raw:?}")))?
			.parse::<u32>()
			.map_err(|_| Error::new(ErrorKind::InvalidData, format!("invalid node_id {raw:?}")))?;
		Ok(Self { id, _reserved: 0 })
	}
}

impl super::units::WriterNodeId {
	/// Derive a 16-byte ADR 045 `WriterNodeId` from the
	/// authenticated cluster `NodeId` (ADR 018).
	///
	/// Layout: bytes [0..4] hold `NodeId.id` in big-endian
	/// order; the remaining 12 bytes are zero. The trailing
	/// padding is reserved so future widening of the
	/// authenticated identity does not break the ADR 045
	/// identity preimage.
	pub const fn from_node_id(node_id: NodeId) -> Self {
		let id_bytes = node_id.id.to_be_bytes();
		let mut bytes = [0u8; 16];
		bytes[0] = id_bytes[0];
		bytes[1] = id_bytes[1];
		bytes[2] = id_bytes[2];
		bytes[3] = id_bytes[3];
		Self::new(bytes)
	}
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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

impl ClientWriteId {
	/// Canonical idempotency identity: the bytes that must agree
	/// across retries of the same logical write. Matches the
	/// server-side idempotency cache key
	/// (`(client_uuid, sequence_no)`) — `idempotency_ttl_ms` is a
	/// hint, `_pad` is alignment, neither contributes to identity.
	///
	/// Anything that needs a retry-stable derivation off
	/// `ClientWriteId` (e.g., the ADR 045 §4a append-only
	/// locality preimage) MUST use these bytes — never the raw
	/// 32-byte struct via `bytemuck::bytes_of`, which would also
	/// hash TTL / padding and break retry convergence.
	pub fn canonical_idempotency_bytes(&self) -> [u8; 24] {
		let mut bytes = [0u8; 24];
		bytes[..16].copy_from_slice(&self.client_uuid);
		bytes[16..24].copy_from_slice(&self.sequence_no.to_le_bytes());
		bytes
	}
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
//  write achieves its currently-implemented durability boundary.
//  Drives E_safe advancement and records the requested durability
//  class in the durable control-plane history.
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
	/// Requested client durability class that produced this marker.
	/// The data path may still execute more conservatively than this
	/// value implies until W6 lands the weaker ack semantics, but the
	/// requested class is durable and observable from W1 onward.
	pub durability: DurabilityLevel,
	pub _pad: [u8; 7],
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
//  so only one data node accepts traffic for a given locality
//  range within an epoch. The same descriptor backs linearizable
//  reads: a node may serve `LINEARIZABLE` reads locally while a
//  valid lease covers the key.
//
//  • Granted by the Raft Leader once the entry commits.
//  • Each lease is scoped to the inclusive `[range_start,
//    range_end]` locality window and the specific epoch named in
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
	/// Inclusive lower bound of the locality range this lease
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
	pub const DENIED_INVALID_TARGET: Self = Self(6);
}

// ============================================================
//  ACQUIRE LEASE REQUEST METADATA — 56 bytes, fixed
//
//  Sent by a Data Node on the Control Ring when it wants to be
//  fenced as the primary for a locality range. The Raft Leader
//  proposes a `WendelonRequest::AcquireLease` entry on the
//  requester's behalf; the resolved `LeaseDescriptor` rides back
//  in a `LEASE_RESP` trailing section on success.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct AcquireLeaseRequestMeta {
	/// Inclusive lower bound of the locality range the
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
//  RELEASE LEASE REQUEST / RESPONSE METADATA
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct ReleaseLeaseRequestMeta {
	/// Inclusive lower bound of the locality range to release.
	pub range_start: u64, // 8
	/// Inclusive upper bound.
	pub range_end: u64, // 8
	/// Epoch the caller believes the lease belongs to.
	pub epoch: u64, // 8
	/// Holder asking to release the lease.
	pub holder: NodeId, // 8
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct ReleaseLeaseResponseMeta {
	pub status: LeaseStatus, // 1
	pub _pad: [u8; 7],       // 7
}

// ============================================================
//  TABLE DESCRIPTOR GET REQUEST / RESPONSE METADATA
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableDescriptorGetRequestMeta {
	pub tenant_id: TenantId,
	pub namespace: [u8; 128],
	pub table_name: [u8; 128],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableDescriptorGetResponseMeta {
	pub accepted: u8,
	pub reason_code: u8,
	pub _pad1: [u8; 6],
	pub leader_hint: NodeId,
	pub descriptor_len: u32,
	pub _pad2: [u8; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableDescriptorListRequestMeta {
	pub tenant_id: TenantId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableDescriptorListResponseMeta {
	pub accepted: u8,
	pub reason_code: u8,
	pub _pad1: [u8; 6],
	pub leader_hint: NodeId,
	pub descriptors_len: u32,
	pub _pad2: [u8; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableOptimizerFactsGetRequestMeta {
	pub tenant_id: TenantId,
	pub namespace: [u8; 128],
	pub table_name: [u8; 128],
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableOptimizerFactsGetResponseMeta {
	pub accepted: u8,
	pub reason_code: u8,
	pub _pad1: [u8; 6],
	pub leader_hint: NodeId,
	pub facts_len: u32,
	pub _pad2: [u8; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogicalTypeGetRequestMeta {
	pub tenant_id: TenantId,
	pub name: [u8; 128],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogicalTypeGetResponseMeta {
	pub accepted: u8,
	pub reason_code: u8,
	pub _pad1: [u8; 6],
	pub leader_hint: NodeId,
	pub definition_len: u32,
	pub _pad2: [u8; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogicalTypeListRequestMeta {
	pub tenant_id: TenantId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct LogicalTypeListResponseMeta {
	pub accepted: u8,
	pub reason_code: u8,
	pub _pad1: [u8; 6],
	pub leader_hint: NodeId,
	pub definitions_len: u32,
	pub _pad2: [u8; 4],
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
	/// Bitfield of known HRFT frame flags; see [`FRAME_FLAG_KNOWN_MASK`].
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
pub const FRAME_FLAG_KNOWN_MASK: u16 = FRAME_FLAG_STREAM_END;

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
	// The client frames a PBC program over a key-hash range and
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
	// Peer-only orphan-witness recovery RPCs. A newly elected
	// leader pulls unresolved pending-finalization witnesses from
	// data peers before it starts admitting fresh writes in the new
	// epoch, so duplicate retries converge instead of re-appending.
	pub const PENDING_WITNESS_REQ: Self = Self(0x26);
	pub const PENDING_WITNESS_RESP: Self = Self(0x27);
	// Typed table write frames (ADR 037). Kept out of the legacy
	// WRITE_REQ shape so existing KV clients retain their fixed
	// `WriteRequestMeta` contract.
	pub const TABLE_WRITE_REQ: Self = Self(0x28);
	pub const TABLE_WRITE_RESP: Self = Self(0x29);
	pub const TABLE_REPLICATE_REQ: Self = Self(0x2A);
	pub const TABLE_REPLICATE_RESP: Self = Self(0x2B);
	pub const TABLE_QUERY_REQ: Self = Self(0x2C);
	pub const TABLE_QUERY_RESP: Self = Self(0x2D);
	pub const AGG_REQ: Self = Self(0x32);
	pub const AGG_RESP: Self = Self(0x33);
	pub const TABLE_DELETE_REQ: Self = Self(0x34);
	pub const TABLE_DELETE_RESP: Self = Self(0x35);
	// CDC subscription substrate (ADR 054). Values 0x36..0x39
	// are the next unused Data Ring protocol slots after
	// TABLE_DELETE_RESP.
	pub const CDC_SUBSCRIBE_REQ: Self = Self(0x36);
	pub const CDC_FRAME: Self = Self(0x37);
	pub const CDC_CHECKPOINT: Self = Self(0x38);
	pub const CDC_REJECT: Self = Self(0x39);
	// Idempotent atomic counter fetch-add (ADR 075). Kept out of the legacy
	// WRITE_REQ shape so existing KV clients retain their fixed `WriteRequestMeta`
	// contract. NOTE: 0x3A/0x3B (the slots after CDC_REJECT) are taken by
	// SCHEMA_DROP_REQ/RESP in the control-ring block below — cut-1 missed that and
	// shipped a dark collision, caught by the global MsgType uniqueness guard. The
	// counter kinds use the free slots after the current max.
	pub const COUNTER_ADD_REQ: Self = Self(0x4C);
	pub const COUNTER_ADD_RESP: Self = Self(0x4D);
	// ADR 075 cut-5b: the public counter point-read. A counter value is stored as a
	// framed `CounterTentativeFrame` (record class COUNTER_TENTATIVE), NOT an Arrow
	// value batch, so the blob READ_REQ path (which decodes the body as an Arrow
	// batch) cannot read it — counters need their own point-read op over the
	// counter-aware reader. Free slots after the current max (0x4D).
	pub const COUNTER_READ_REQ: Self = Self(0x4E);
	pub const COUNTER_READ_RESP: Self = Self(0x4F);
	// Internal leader→replica counter-replication protocol (ADR 075 cut-2).
	// NOT a public client surface: a counter add's primary fans the tentative
	// out over COUNTER_REPLICATE_REQ (phase tentative), then drives the
	// accepted participants with COUNTER_REPLICATE_REQ (phase commit) on quorum
	// success or COUNTER_ROLLBACK_REQ on failure. Distinct from REPLICATE_REQ
	// because a counter replica must NOT emit a pending-finalization witness on
	// append (the reconciler finalizes witnesses; a witnessed-but-unquorumed
	// add would be wrongly committed).
	// Opcodes 0x3C-0x3F are taken by the TABLE_DESCRIPTOR_GET / TABLE_QUERY_CANCEL
	// kinds below; the counter replicate/rollback kinds use the next free slots
	// after the current max (0x47). (COUNTER_ADD_REQ/RESP at 0x3A/0x3B predate the
	// TABLE_* block and are unique.)
	pub const COUNTER_REPLICATE_REQ: Self = Self(0x48);
	pub const COUNTER_REPLICATE_RESP: Self = Self(0x49);
	pub const COUNTER_ROLLBACK_REQ: Self = Self(0x4A);
	pub const COUNTER_ROLLBACK_RESP: Self = Self(0x4B);

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
	pub const LEASE_RELEASE_REQ: Self = Self(0x1A);
	pub const LEASE_RELEASE_RESP: Self = Self(0x1B);
	pub const TENANT_ADMIN_REQ: Self = Self(0x1C);
	pub const TENANT_ADMIN_RESP: Self = Self(0x1D);
	pub const AUDIT_CHAIN_HEAD_REQ: Self = Self(0x1E);
	pub const AUDIT_CHAIN_HEAD_RESP: Self = Self(0x1F);
	pub const SCHEMA_UPDATE_REQ: Self = Self(0x2E);
	pub const SCHEMA_UPDATE_RESP: Self = Self(0x2F);
	pub const REVOCATION_ADMIN_REQ: Self = Self(0x30);
	pub const REVOCATION_ADMIN_RESP: Self = Self(0x31);
	pub const SCHEMA_DROP_REQ: Self = Self(0x3A);
	pub const SCHEMA_DROP_RESP: Self = Self(0x3B);
	pub const TABLE_DESCRIPTOR_GET_REQ: Self = Self(0x3C);
	pub const TABLE_DESCRIPTOR_GET_RESP: Self = Self(0x3D);
	pub const TABLE_QUERY_CANCEL_REQ: Self = Self(0x3E);
	pub const TABLE_QUERY_CANCEL_RESP: Self = Self(0x3F);
	pub const LOGICAL_TYPE_GET_REQ: Self = Self(0x40);
	pub const LOGICAL_TYPE_GET_RESP: Self = Self(0x41);
	pub const LOGICAL_TYPE_LIST_REQ: Self = Self(0x42);
	pub const LOGICAL_TYPE_LIST_RESP: Self = Self(0x43);
	pub const TABLE_DESCRIPTOR_LIST_REQ: Self = Self(0x44);
	pub const TABLE_DESCRIPTOR_LIST_RESP: Self = Self(0x45);
	pub const TABLE_OPTIMIZER_FACTS_GET_REQ: Self = Self(0x46);
	pub const TABLE_OPTIMIZER_FACTS_GET_RESP: Self = Self(0x47);
	/// ADR 081: read-only secondary-index lifecycle/coverage status. Mutations
	/// continue to use SCHEMA_UPDATE_REQ; this pair is observation only.
	pub const SECONDARY_INDEX_STATUS_REQ: Self = Self(0x50);
	pub const SECONDARY_INDEX_STATUS_RESP: Self = Self(0x51);
	/// ADR 542: resolve a retained table-mutation outcome by caller-minted id
	/// without re-submitting the mutation.
	pub const MUTATION_OUTCOME_REQ: Self = Self(0x52);
	pub const MUTATION_OUTCOME_RESP: Self = Self(0x53);
	/// ADR 548: mutate or resolve the durable CTAS staged-publication saga.
	pub const CTAS_SAGA_REQ: Self = Self(0x54);
	pub const CTAS_SAGA_RESP: Self = Self(0x55);
	/// ADR 548: pin row-mutation evidence until the Control Ring acknowledges it.
	pub const MUTATION_OUTCOME_PIN_REQ: Self = Self(0x56);
	pub const MUTATION_OUTCOME_PIN_RESP: Self = Self(0x57);

	// raft internal (openraft RaftNetwork)
	pub const APPEND_ENTRIES_REQ: Self = Self(0x20);
	pub const APPEND_ENTRIES_RESP: Self = Self(0x21);
	pub const INSTALL_SNAPSHOT_REQ: Self = Self(0x22);
	pub const INSTALL_SNAPSHOT_RESP: Self = Self(0x23);
	pub const VOTE_REQ: Self = Self(0x24);
	pub const VOTE_RESP: Self = Self(0x25);

	/// EVERY Data/Control Ring opcode. New `MsgType` consts MUST be added here —
	/// the compile-time uniqueness guard below relies on this list to make an
	/// opcode collision a build error. (An aliased opcode silently routes one
	/// kind's frames to another kind's handler; ADR 075 cut-3b hit exactly that
	/// when the counter replicate/rollback kinds aliased the TABLE_* block.)
	pub const ALL: &'static [MsgType] = &[
		Self::WRITE_REQ,
		Self::WRITE_RESP,
		Self::READ_REQ,
		Self::READ_RESP,
		Self::BULK_WRITE_REQ,
		Self::BULK_WRITE_RESP,
		Self::STREAM_FIN_REQ,
		Self::STREAM_FIN_EVT,
		Self::REPLICATE_REQ,
		Self::REPLICATE_RESP,
		Self::QUERY_REQ,
		Self::QUERY_RESP,
		Self::MIGRATE_REQ,
		Self::MIGRATE_RESP,
		Self::PENDING_WITNESS_REQ,
		Self::PENDING_WITNESS_RESP,
		Self::TABLE_WRITE_REQ,
		Self::TABLE_WRITE_RESP,
		Self::TABLE_REPLICATE_REQ,
		Self::TABLE_REPLICATE_RESP,
		Self::TABLE_QUERY_REQ,
		Self::TABLE_QUERY_RESP,
		Self::AGG_REQ,
		Self::AGG_RESP,
		Self::TABLE_DELETE_REQ,
		Self::TABLE_DELETE_RESP,
		Self::CDC_SUBSCRIBE_REQ,
		Self::CDC_FRAME,
		Self::CDC_CHECKPOINT,
		Self::CDC_REJECT,
		Self::COUNTER_ADD_REQ,
		Self::COUNTER_ADD_RESP,
		Self::COUNTER_READ_REQ,
		Self::COUNTER_READ_RESP,
		Self::COUNTER_REPLICATE_REQ,
		Self::COUNTER_REPLICATE_RESP,
		Self::COUNTER_ROLLBACK_REQ,
		Self::COUNTER_ROLLBACK_RESP,
		Self::TRANSFER_REQ,
		Self::TRANSFER_RESP,
		Self::LEASE_REQ,
		Self::LEASE_RESP,
		Self::EPOCH_ADV_REQ,
		Self::EPOCH_ADV_RESP,
		Self::STATUS_REQ,
		Self::STATUS_RESP,
		Self::JOIN_REQ,
		Self::JOIN_RESP,
		Self::LEASE_RELEASE_REQ,
		Self::LEASE_RELEASE_RESP,
		Self::TENANT_ADMIN_REQ,
		Self::TENANT_ADMIN_RESP,
		Self::AUDIT_CHAIN_HEAD_REQ,
		Self::AUDIT_CHAIN_HEAD_RESP,
		Self::SCHEMA_UPDATE_REQ,
		Self::SCHEMA_UPDATE_RESP,
		Self::REVOCATION_ADMIN_REQ,
		Self::REVOCATION_ADMIN_RESP,
		Self::SCHEMA_DROP_REQ,
		Self::SCHEMA_DROP_RESP,
		Self::TABLE_DESCRIPTOR_GET_REQ,
		Self::TABLE_DESCRIPTOR_GET_RESP,
		Self::TABLE_QUERY_CANCEL_REQ,
		Self::TABLE_QUERY_CANCEL_RESP,
		Self::LOGICAL_TYPE_GET_REQ,
		Self::LOGICAL_TYPE_GET_RESP,
		Self::LOGICAL_TYPE_LIST_REQ,
		Self::LOGICAL_TYPE_LIST_RESP,
		Self::TABLE_DESCRIPTOR_LIST_REQ,
		Self::TABLE_DESCRIPTOR_LIST_RESP,
		Self::TABLE_OPTIMIZER_FACTS_GET_REQ,
		Self::TABLE_OPTIMIZER_FACTS_GET_RESP,
		Self::SECONDARY_INDEX_STATUS_REQ,
		Self::SECONDARY_INDEX_STATUS_RESP,
		Self::MUTATION_OUTCOME_REQ,
		Self::MUTATION_OUTCOME_RESP,
		Self::CTAS_SAGA_REQ,
		Self::CTAS_SAGA_RESP,
		Self::MUTATION_OUTCOME_PIN_REQ,
		Self::MUTATION_OUTCOME_PIN_RESP,
		Self::APPEND_ENTRIES_REQ,
		Self::APPEND_ENTRIES_RESP,
		Self::INSTALL_SNAPSHOT_REQ,
		Self::INSTALL_SNAPSHOT_RESP,
		Self::VOTE_REQ,
		Self::VOTE_RESP,
	];
}

/// Compile-time global uniqueness of every Data/Control Ring opcode. An aliased
/// opcode routes one kind's frames to another handler — invisible until both
/// kinds dispatch (ADR 075 cut-3b). O(n²) over a fixed ~76-element list at build
/// time.
const fn msg_types_globally_unique(all: &[MsgType]) -> bool {
	let mut i = 0;
	while i < all.len() {
		let mut j = i + 1;
		while j < all.len() {
			if all[i].0 == all[j].0 {
				return false;
			}
			j += 1;
		}
		i += 1;
	}
	true
}

const _: () = assert!(
	msg_types_globally_unique(MsgType::ALL),
	"MsgType opcodes must be globally unique — an aliased opcode collides on the wire"
);

// ============================================================
//  WRITE REQUEST METADATA — 120 bytes, fixed
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
/// `QUORUM` is the fully implemented contract today: payload bytes
/// are durably flushed on a quorum of replicas before the leader
/// finalizes the write in Raft. `LEADER` and `NONE` are accepted on
/// the wire and now persist through the Control Ring marker for
/// auditability, but the server still routes them through the same
/// conservative quorum-durable execution until W6 widens their
/// end-to-end ack semantics explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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
	/// Tenant the write targets. Single-tenant pilot nodes still
	/// populate this explicitly so the durable schema never takes a
	/// "no tenant" branch.
	pub tenant_id: TenantId, //  8 bytes
	/// 0 = direct to leader; incremented by each proxy; max 2
	pub proxy_hop: u8, //  1 byte
	/// Durability level: 0=QUORUM, 1=LEADER, 2=NONE
	pub durability: DurabilityLevel, //  1 byte
	/// If 1, leader streams back FinalizationMarker in the response
	pub await_finalization: u8, //  1 byte
	pub _pad1: [u8; 5], //  5 bytes alignment
	/// Set by proxy follower for response routing; zeroed on direct writes
	pub proxy_source: NodeId, //  8 bytes
	/// Stable 256-bit record identity computed from `(tenant_id, key)`.
	/// The server verifies this against the Arrow payload before the
	/// write crosses the bifurcation boundary.
	pub key_identity: Identity, // 32 bytes
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
	pub const TENANT_SUSPENDED: Self = Self(10);
	pub const TENANT_UNAVAILABLE: Self = Self(11);
	pub const AUTHORIZATION_FAILED: Self = Self(12);
	/// The data branch is already durable and the leader durably
	/// recorded pending-finalization intent, but the Control Ring
	/// finalization has not committed yet. Retrying the same write
	/// id is safe and should either reconcile the write to a
	/// committed result or return this same status again.
	pub const FINALIZATION_PENDING: Self = Self(13);
	/// The request was rejected by bounded post-auth admission
	/// control before the data path began durable work. Retrying
	/// after a short backoff is allowed.
	pub const BACKPRESSURED: Self = Self(14);
	/// ADR 075 D5: a `COUNTER_ADD` with `delta == 0` (a mutation must
	/// move the counter; explicit point reads observe the value). No
	/// mutation occurred. Not retryable as-is.
	pub const COUNTER_ZERO_DELTA: Self = Self(15);
	/// ADR 075 D5: a `COUNTER_ADD` whose `previous + delta` would
	/// overflow `u64`. Rejected WITHOUT mutating — no wrap, no
	/// saturate. Not retryable as-is.
	pub const COUNTER_OVERFLOW: Self = Self(16);
	/// ADR 075: a `COUNTER_ADD` was rejected because the cluster runs
	/// with replication factor > 1 but multi-node counter replication
	/// (ring-driven participant set + peer-identity witness, cut-8b) is
	/// not yet wired. Fail-closed: the add would otherwise be durable
	/// only on the primary, silently violating D3's durable-quorum.
	/// `rf = 1` (the default) is unaffected.
	pub const COUNTER_REPLICATION_UNSUPPORTED: Self = Self(17);
	/// The target table is fenced because a published reference points at a
	/// missing backing slab. No append is acknowledged while that state holds.
	pub const TABLE_REFERENCE_INTEGRITY_FAILED: Self = Self(18);
	/// ADR 542: the caller's original end-to-end mutation deadline expired.
	pub const WRITE_DEADLINE_EXCEEDED: Self = Self(19);
	/// ADR 542: no durability progress occurred for the server stall bound.
	pub const WRITE_STALLED: Self = Self(20);
	/// ADR 542: the caller consumed its bounded attempt budget before either clock.
	pub const WRITE_ATTEMPTS_EXHAUSTED: Self = Self(21);
	/// ADR 542: the retained outcome was reclaimed; this id may never be applied anew.
	pub const WRITE_OPERATION_EXPIRED: Self = Self(22);
	pub const WRITE_OPERATION_UNKNOWN: Self = Self(23);

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::OK => "ok",
			Self::EPOCH_STALE => "epoch_stale",
			Self::NOT_LEADER => "not_leader",
			Self::DUPLICATE => "duplicate",
			Self::PROXY_LIMIT => "proxy_limit",
			Self::PAYLOAD_TOO_LARGE => "payload_too_large",
			Self::LEASE_EXPIRED => "lease_expired",
			Self::INVALID_LEASE_SIGNATURE => "invalid_lease_signature",
			Self::INTERNAL_ERROR => "internal_error",
			Self::QUOTA_EXCEEDED => "quota_exceeded",
			Self::TENANT_SUSPENDED => "tenant_suspended",
			Self::TENANT_UNAVAILABLE => "tenant_unavailable",
			Self::AUTHORIZATION_FAILED => "authorization_failed",
			Self::FINALIZATION_PENDING => "finalization_pending",
			Self::BACKPRESSURED => "backpressured",
			Self::COUNTER_ZERO_DELTA => "counter_zero_delta",
			Self::COUNTER_OVERFLOW => "counter_overflow",
			Self::COUNTER_REPLICATION_UNSUPPORTED => "counter_replication_unsupported",
			Self::TABLE_REFERENCE_INTEGRITY_FAILED => "table_reference_integrity_failed",
			Self::WRITE_DEADLINE_EXCEEDED => "write_deadline_exceeded",
			Self::WRITE_STALLED => "write_stalled",
			Self::WRITE_ATTEMPTS_EXHAUSTED => "write_attempts_exhausted",
			Self::WRITE_OPERATION_EXPIRED => "write_operation_expired",
			Self::WRITE_OPERATION_UNKNOWN => "write_operation_unknown",
			_ => "unknown",
		}
	}
}

/// ADR 542's durable-knowledge axis. This is separate from [`WriteStatus`], which
/// names why an attempt stopped. Keeping both representable prevents an expiry from
/// pretending to answer whether the parent batch committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(transparent)]
pub struct MutationCommitKnowledge(pub u8);

impl MutationCommitKnowledge {
	pub const UNSPECIFIED: Self = Self(0);
	pub const COMMITTED: Self = Self(1);
	pub const KNOWN_NOT_COMMITTED: Self = Self(2);
	pub const INDETERMINATE: Self = Self(3);
}

/// ADR 542's orthogonal stop-cause axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(transparent)]
pub struct MutationStopReason(pub u8);

impl MutationStopReason {
	pub const NONE: Self = Self(0);
	pub const DEADLINE: Self = Self(1);
	pub const NO_PROGRESS: Self = Self(2);
	pub const ATTEMPT_EXHAUSTION: Self = Self(3);
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

impl WriteResponseMeta {
	pub fn with_mutation_outcome(
		mut self,
		knowledge: MutationCommitKnowledge,
		reason: MutationStopReason,
	) -> Self {
		self._pad1[0] = knowledge.0;
		self._pad1[1] = reason.0;
		self
	}

	pub const fn mutation_commit_knowledge(self) -> MutationCommitKnowledge {
		MutationCommitKnowledge(self._pad1[0])
	}

	pub const fn mutation_stop_reason(self) -> MutationStopReason {
		MutationStopReason(self._pad1[1])
	}
}

// ============================================================
//  COUNTER ADD REQUEST METADATA — 128 bytes, fixed (ADR 075)
//
//  Idempotent atomic fetch-add on a counter key. Mirrors
//  `WriteRequestMeta` but carries an additive `delta` instead of a
//  value payload — a counter add has no Arrow value section, only
//  the `u64` delta. Routed to the key's primary owner, applied under
//  a per-key read-modify-write critical section.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterAddRequestMeta {
	/// REQUIRED — exactly-once idempotency key
	pub write_id: ClientWriteId, // 32 bytes
	/// Client's current HLC; leader merges max(leader_hlc, client_hlc)+1
	pub client_hlc: HybridTimestamp, // 16 bytes
	/// Client's believed epoch; leader rejects with EPOCH_STALE on mismatch
	pub expected_epoch: u64, //  8 bytes
	/// Tenant the counter belongs to.
	pub tenant_id: TenantId, //  8 bytes
	/// 0 = direct to leader; incremented by each proxy; max 2
	pub proxy_hop: u8, //  1 byte
	/// Durability level: 0=QUORUM, 1=LEADER, 2=NONE
	pub durability: DurabilityLevel, //  1 byte
	/// If 1, leader streams back FinalizationMarker in the response
	pub await_finalization: u8, //  1 byte
	pub _pad1: [u8; 5], //  5 bytes alignment
	/// Set by proxy follower for response routing; zeroed on direct adds
	pub proxy_source: NodeId, //  8 bytes
	/// Stable 256-bit identity of the counter key (server-verified,
	/// derived from the reserved counter key-domain `(tenant_id, key)`).
	pub key_identity: Identity, // 32 bytes
	/// Key length in the Arrow payload (for fast routing without Arrow parse)
	pub key_len: u32, //  4 bytes
	pub _pad2: [u8; 4], //  4 bytes alignment
	/// Additive delta. `0` is rejected (a mutation, not a read).
	pub delta: u64, //  8 bytes
}

// ============================================================
//  COUNTER ADD RESPONSE METADATA — 112 bytes, fixed (ADR 075)
//
//  Mirrors `WriteResponseMeta` plus the pre/post counter values so a
//  duplicate `write_id` replay returns the original `previous_value`
//  / `current_value` without re-applying the delta.
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterAddResponseMeta {
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
	/// Counter value BEFORE this add (0 if the counter was absent).
	pub previous_value: u64, //  8 bytes
	/// Counter value AFTER this add (`previous_value + delta`).
	pub current_value: u64, //  8 bytes
	/// 1 if a FinalizationMarker follows this frame; 0 otherwise
	pub has_finalization: u8, //  1 byte
	pub _pad2: [u8; 7],      //  7 bytes
}

// ============================================================
//  COUNTER READ REQUEST / RESPONSE METADATA — 48 / 32 bytes (ADR 075 cut-5b)
//
//  A point read of a counter key. No Arrow payload: the server-verified
//  domain-separated `key_identity` is sufficient (the read is non-mutating, so it
//  routes/looks up by identity exactly like a point read but returns the framed
//  `u64` rather than an Arrow value batch).
// ============================================================
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReadRequestMeta {
	/// Client's believed epoch; the server rejects with `EPOCH_STALE` on mismatch.
	pub expected_epoch: u64, //  8 bytes
	/// Tenant the counter belongs to.
	pub tenant_id: TenantId, //  8 bytes
	/// Domain-separated counter identity (the same derivation the add verifies).
	pub key_identity: Identity, // 32 bytes
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReadResponseMeta {
	/// `OK` with a present value, `KEY_NOT_FOUND` for an absent counter (D6 —
	/// absence is absence, never a fabricated 0), or a redirect/stale status.
	pub status: ReadStatus, //  1 byte
	pub _pad1: [u8; 7], //  7 bytes
	/// The committed counter value (meaningful only when `status == OK`).
	pub value: u64, //  8 bytes
	pub epoch: u64,     //  8 bytes
	/// Populated on `NOT_LEADER` / `EPOCH_STALE` for client redirect.
	pub leader_hint: NodeId, //  8 bytes
}

// ============================================================
//  COUNTER REPLICATE / ROLLBACK METADATA (ADR 075 cut-2)
//
//  Internal leader→replica frames. A counter add's primary fans the
//  tentative out with `CounterReplicaPhase::TENTATIVE`, gathers the
//  exact set of participants that acked tentative success, then on
//  quorum success drives that set with `CounterReplicaPhase::COMMIT`
//  (write the durable commit marker) or on failure with a
//  `CounterRollbackRequestMeta` (drop the live tentative; recovery
//  skips the orphan). `op_id` is the leader-assigned, cross-node
//  stable handle for one counter add; the on-disk tentative/commit
//  pairing on each replica uses that replica's local record offset.
// ============================================================

/// Phase of a [`CounterReplicateRequestMeta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReplicaPhase(pub u8);

impl CounterReplicaPhase {
	/// Durably append the tentative counter value and index it live — NO commit
	/// marker, NO pending-finalization witness.
	pub const TENTATIVE: Self = Self(0);
	/// Write the durable commit marker for a previously-replicated tentative —
	/// the only path that makes a replica's counter value survive restart.
	pub const COMMIT: Self = Self(1);
}

/// Acknowledgement status from a counter replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReplicaStatus(pub u8);

impl CounterReplicaStatus {
	pub const OK: Self = Self(0);
	pub const EPOCH_STALE: Self = Self(1);
	pub const NOT_AUTHORIZED: Self = Self(2);
	pub const INTERNAL_ERROR: Self = Self(3);
}

/// Leader→replica counter-replication request — 88 bytes, fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReplicateRequestMeta {
	/// HLC chosen by the leader so replicas index the tentative under a
	/// consistent cross-node timestamp.
	pub commit_hlc: HybridTimestamp, // 16 bytes
	/// Leader-assigned stable handle for this counter add (the key the leader's
	/// commit/rollback step references; replica maps it to a local tentative).
	pub op_id: u64, //  8 bytes
	/// Tentative counter value (`previous + delta`) on `TENTATIVE`; ignored on
	/// `COMMIT`.
	pub value: u64, //  8 bytes
	/// Originating leader epoch — replicas reject on mismatch.
	pub epoch: u64, //  8 bytes
	/// Tenant the counter belongs to.
	pub tenant_id: TenantId, //  8 bytes
	/// Server-verified counter key identity (reserved counter key-domain).
	pub key_identity: Identity, // 32 bytes
	/// `TENTATIVE` or `COMMIT`.
	pub phase: CounterReplicaPhase, //  1 byte
	pub _pad: [u8; 7], //  7 bytes
}

/// Leader→replica counter rollback request — 56 bytes, fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterRollbackRequestMeta {
	/// Server-verified counter key identity.
	pub key_identity: Identity, // 32 bytes
	/// The leader-assigned op handle to abort.
	pub op_id: u64, //  8 bytes
	/// Originating leader epoch.
	pub epoch: u64, //  8 bytes
	/// Tenant the counter belongs to.
	pub tenant_id: TenantId, //  8 bytes
}

/// Replica acknowledgement for a counter replicate/rollback — 16 bytes, fixed.
/// Shared by `COUNTER_REPLICATE_RESP` and `COUNTER_ROLLBACK_RESP`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct CounterReplicaAck {
	pub status: CounterReplicaStatus, //  1 byte
	pub _pad: [u8; 7],                //  7 bytes
	/// Echoed leader-assigned op handle for correlation.
	pub op_id: u64, //  8 bytes
}

// ============================================================
//  REPLICATE REQUEST METADATA — 96 bytes, fixed
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
	/// Original client idempotency key. Replicas persist this with
	/// the pending-finalization witness so a later leader can
	/// reconcile the write without seeing the original caller
	/// again.
	pub write_id: ClientWriteId, // 32 bytes
	/// Epoch of the originating leader — replicas reject on
	/// mismatch against their own current epoch.
	pub epoch: u64, //  8 bytes
	/// Tenant the replicated record belongs to.
	pub tenant_id: TenantId, //  8 bytes
	/// Authoritative quota charge computed by the accepting node.
	pub charged_bytes: u64, //  8 bytes
	/// Stable 256-bit record identity computed from the key bytes.
	/// Replicas persist this directly rather than re-hashing the
	/// Arrow payload.
	pub key_identity: Identity, // 32 bytes
	/// Blake3-256 of the Arrow IPC bytes. Replicas verify the
	/// payload before ack so corruption surfaces as a failed
	/// ack rather than a silent divergence.
	pub payload_hash: [u8; 32], // 32 bytes
	/// Durability class echoed into the eventual finalization marker.
	pub durability: DurabilityLevel, //  1 byte
	/// Mirrors the caller's receipt preference so duplicate replay
	/// on a later leader can preserve the original trailing-marker
	/// contract.
	pub await_finalization: u8, //  1 byte
	pub _pad: [u8; 6], //  6 bytes
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
	pub const NOT_AUTHORIZED: Self = Self(4);
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
//  PENDING-WITNESS PULL METADATA — 16 bytes request, 16 bytes response
// ============================================================

/// Request a peer's unresolved pending-finalization witnesses.
///
/// Used only during post-election recovery: a newly elected
/// leader pulls orphan witnesses from data peers before it starts
/// treating the new epoch as write-ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct PendingWitnessPullRequestMeta {
	/// Node performing the recovery sweep.
	pub requester: NodeId, //  8 bytes
	/// Membership epoch the requester believes it is recovering
	/// for. Carried for tracing and future tightening; peers today
	/// serve unresolved witnesses regardless of the epoch value.
	pub request_epoch: u64, //  8 bytes
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct PendingWitnessPullStatus(pub u8);

impl PendingWitnessPullStatus {
	pub const OK: Self = Self(0);
	pub const INTERNAL_ERROR: Self = Self(1);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct PendingWitnessPullResponseMeta {
	pub status: PendingWitnessPullStatus, //  1 byte
	pub _pad1: [u8; 7],                   //  7 bytes
	/// Number of witnesses encoded in the payload.
	pub witness_count: u64, //  8 bytes
}

// ============================================================
//  READ REQUEST METADATA — 96 bytes, fixed
//  Actual key is in the Arrow payload.
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
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
	pub expected_epoch: u64, //  8 bytes
	/// Tenant the read targets.
	pub tenant_id: TenantId, //  8 bytes
	pub consistency: Consistency, //  1 byte
	pub _pad: [u8; 3],       //  3 bytes
	/// Stable 256-bit record identity computed from `(tenant_id, key)`.
	/// The server uses this directly for storage lookup and may verify it
	/// against the optional Arrow key payload when present.
	pub key_identity: Identity, // 32 bytes
	/// Key length in Arrow payload (for fast routing)
	pub key_len: u32, //  4 bytes
	pub _pad2: [u8; 8],      //  8 bytes reserved
	/// Time-travel upper bound (ADR 013). When non-zero, the
	/// server returns the newest version whose `hlc <= as_of_hlc`
	/// instead of the latest value. All-zero is the wire sentinel
	/// for "no upper bound" and preserves the default read path.
	pub as_of_hlc: HybridTimestamp, // 16 bytes
}

// ============================================================
//  READ RESPONSE METADATA — 56 bytes, fixed
//  Value is in the Arrow payload only when status == OK.
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
	pub const TENANT_UNAVAILABLE: Self = Self(8);
	pub const AUTHORIZATION_FAILED: Self = Self(9);
	/// The query's PBC program failed validation or referenced an
	/// unsupported column shape. The caller must fix the program;
	/// retrying unchanged bytes will not succeed.
	pub const QUERY_INVALID_PROGRAM: Self = Self(10);
	/// The server terminated the query after it exceeded its bounded
	/// execution envelope (rows, slabs, or wall time).
	pub const QUERY_BUDGET_EXCEEDED: Self = Self(11);
	/// The caller requested a snapshot older than the node's MVCC
	/// retention floor. Retrying the same `as_of_hlc` is not useful
	/// unless retention policy changes.
	pub const RETENTION_EXPIRED: Self = Self(12);
	/// The request was rejected by bounded post-auth admission
	/// control before the read or query work began. Retrying after
	/// a short backoff is allowed.
	pub const BACKPRESSURED: Self = Self(13);
	/// The table-query request was planned against a physical
	/// descriptor fingerprint that no longer matches the server's
	/// descriptor for the requested table generation.
	pub const TABLE_DESCRIPTOR_EVOLVED: Self = Self(14);
	/// The table-query request was cancelled through TABLE_QUERY_CANCEL_REQ.
	pub const QUERY_CANCELLED: Self = Self(15);
	/// A published table reference points at a slab that is no longer
	/// resolvable. The accompanying query-response trailer carries the
	/// table/reference/slab/origin diagnostic; retrying cannot make the
	/// acknowledged state readable.
	pub const TABLE_REFERENCE_INTEGRITY_FAILED: Self = Self(16);
	/// The query made no progress for the server's sliding no-progress bound. A
	/// progressing scan never carries this status; a stalled one always does.
	pub const QUERY_STALLED: Self = Self(17);
	/// The caller's end-to-end deadline expired. The deadline is absolute and
	/// non-resetting; it travels as remaining duration and is derived into a local
	/// monotonic instant on receipt.
	pub const QUERY_DEADLINE_EXCEEDED: Self = Self(18);

	/// The canonical named-status list. ADR 540's totality guard holds the mapping
	/// honest: a new status constant added without an entry here (and an `as_str`
	/// name) fails the guard, because a status the server can emit but the boundary
	/// cannot name is how failures collapse into generic errors.
	pub const ALL_NAMED: [Self; 19] = [
		Self::OK,
		Self::KEY_NOT_FOUND,
		Self::EPOCH_STALE,
		Self::NOT_LEADER,
		Self::LEASE_EXPIRED,
		Self::INVALID_LEASE_SIGNATURE,
		Self::E_SAFE_BEHIND,
		Self::MIGRATION_INCOMPLETE,
		Self::TENANT_UNAVAILABLE,
		Self::AUTHORIZATION_FAILED,
		Self::QUERY_INVALID_PROGRAM,
		Self::QUERY_BUDGET_EXCEEDED,
		Self::RETENTION_EXPIRED,
		Self::BACKPRESSURED,
		Self::TABLE_DESCRIPTOR_EVOLVED,
		Self::QUERY_CANCELLED,
		Self::TABLE_REFERENCE_INTEGRITY_FAILED,
		Self::QUERY_STALLED,
		Self::QUERY_DEADLINE_EXCEEDED,
	];

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::OK => "ok",
			Self::KEY_NOT_FOUND => "key_not_found",
			Self::EPOCH_STALE => "epoch_stale",
			Self::NOT_LEADER => "not_leader",
			Self::LEASE_EXPIRED => "lease_expired",
			Self::INVALID_LEASE_SIGNATURE => "invalid_lease_signature",
			Self::E_SAFE_BEHIND => "e_safe_behind",
			Self::MIGRATION_INCOMPLETE => "migration_incomplete",
			Self::TENANT_UNAVAILABLE => "tenant_unavailable",
			Self::AUTHORIZATION_FAILED => "authorization_failed",
			Self::QUERY_INVALID_PROGRAM => "query_invalid_program",
			Self::QUERY_BUDGET_EXCEEDED => "query_budget_exceeded",
			Self::RETENTION_EXPIRED => "retention_expired",
			Self::BACKPRESSURED => "backpressured",
			Self::TABLE_DESCRIPTOR_EVOLVED => "table_descriptor_evolved",
			Self::QUERY_CANCELLED => "query_cancelled",
			Self::TABLE_REFERENCE_INTEGRITY_FAILED => "table_reference_integrity_failed",
			Self::QUERY_STALLED => "query_stalled",
			Self::QUERY_DEADLINE_EXCEEDED => "query_deadline_exceeded",
			_ => "unknown",
		}
	}
}

/// Fixed diagnostic carried as the trailing payload of a terminal query
/// response whose status is [`ReadStatus::TABLE_REFERENCE_INTEGRITY_FAILED`].
///
/// This is deliberately a wire value rather than a formatted error string:
/// Toron needs the same table/reference/slab/origin identity that caused
/// Wendelon to fence the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableReferenceIntegrityFailureMeta {
	pub tenant_id: TenantId,
	pub table_object_id: [u8; 16],
	pub missing_slab_id: u64,
	pub reference_kind: u8,
	pub origin: u8,
	pub _pad: [u8; 6],
}

const _: () = assert!(std::mem::size_of::<TableReferenceIntegrityFailureMeta>() == 40);

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
//  QUERY REQUEST METADATA — 80 bytes, fixed
//
//  Carries the fixed envelope for a Smart Client pushdown scan
//  (ADR 005). The PBC bytecode itself rides in the frame's
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
	pub expected_epoch: u64, //  8 bytes
	/// Tenant whose keyspace should be scanned.
	pub tenant_id: TenantId, //  8 bytes
	pub consistency: Consistency, //  1 byte
	pub _pad: [u8; 7],       //  7 bytes
	/// Inclusive lower bound of the locality range to scan.
	pub locality_range_start: u64, //  8 bytes
	/// Inclusive upper bound.
	pub locality_range_end: u64, //  8 bytes
	/// Length of the PBC bytecode in the trailing section. Zero
	/// means "no filter, all rows in range".
	pub bytecode_len: u32, //  4 bytes
	pub _pad2: [u8; 4],      //  4 bytes
	/// Time-travel upper bound (ADR 013). When non-zero, the scan
	/// yields the newest version per key whose `hlc <= as_of_hlc`
	/// and skips keys whose first version is still in the future.
	/// All-zero is the sentinel for "no upper bound".
	pub as_of_hlc: HybridTimestamp, // 16 bytes
}

/// Query request `_pad[0]` bit: force ADR 039 slab-statistics
/// pruning off for this query. The default zero bit means stats
/// pruning may be used when the node advertises support.
pub const QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING: u8 = 0b0000_0001;

pub fn query_request_stats_pruning_enabled(meta: &QueryRequestMeta) -> bool {
	meta._pad[0] & QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING == 0
}

// ============================================================
//  QUERY RESPONSE METADATA — 56 bytes, fixed
//
//  Response envelope for pushdown scans. The filtered Arrow IPC
//  result rides in the frame's arrow section; `rows` is the row
//  count after PBC filter + project and lets the client size its
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
//  MIGRATE REQUEST METADATA — 160 bytes, fixed
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
	/// Tenant targeted by a `POINT_LOOKUP`. `RANGE_PAGE` ignores
	/// this and streams every tenant resident in the locality
	/// window.
	pub tenant_id: TenantId, //  8
	/// Inclusive lower bound of the locality range being pulled.
	pub range_start: u64, //  8
	/// Inclusive upper bound.
	pub range_end: u64, //  8
	/// Exact record identity requested for `POINT_LOOKUP`. Zero for
	/// `RANGE_PAGE`, where the server walks the full locality
	/// window instead.
	pub key_identity: Identity, // 32
	/// Pinned MVCC cutoff. Every version with `hlc >=
	/// migration_hlc_floor` is migration-eligible; older versions
	/// fall outside the ADR 013 retention window and are skipped.
	/// Must match the floor stored on the server's ledger entry.
	pub migration_hlc_floor: HybridTimestamp, // 16
	/// Pagination cursor — the exact record identity the prior page
	/// stopped within. Zero together with `cursor_hlc` means "start
	/// at the range floor"; non-zero means the source was in the
	/// middle of emitting versions for this identity when
	/// `max_records` / `max_bytes` tripped.
	pub cursor_identity: Identity, // 32
	/// HLC of the last version emitted for `cursor_identity` on the
	/// prior page. Combined with `cursor_identity`, it lets the
	/// source resume strictly after the last emitted MVCC slot so a
	/// fat key with more versions than fit in one page is still
	/// streamed exhaustively.
	pub cursor_hlc: HybridTimestamp, // 16
	/// Server-enforced caps on the response size.
	pub max_records: u32, //  4
	pub max_bytes: u32,    //  4
	/// 0 = PointLookup, 1 = RangePage. Point lookups return every
	/// retained version of exactly one key; range pages walk the
	/// inclusive window keyed by `(cursor_identity, cursor_hlc)`.
	pub kind: MigrateRequestKind, //  1
	pub _pad: [u8; 7],     //  7
}

/// Request kind discriminant rides inside [`MigrateRequestMeta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MigrateRequestKind(pub u8);

impl MigrateRequestKind {
	/// Serve every retained version of the single key whose tenant +
	/// locality
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
	/// `(next_cursor_identity, next_cursor_hlc)`; 0 when this response
	/// exhausted the source for the pinned floor.
	pub has_more: u8, //  1
	pub _pad0: [u8; 6],        //  6
	/// Echo of the request's `migration_id` so the caller can
	/// correlate responses from fan-out pulls.
	pub migration_id: u64, //  8
	/// Populated on `REDIRECT` — the node the caller should retry
	/// against. Zero for every other status.
	pub source_hint: NodeId, //  8
	/// Identity at which pagination should resume. Zero together
	/// with `next_cursor_hlc` when `has_more == 0`.
	pub next_cursor_identity: Identity, // 32
	/// HLC of the last version emitted on this page for
	/// `next_cursor_identity`. The follow-up request must skip
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
	pub magic: u32, // 0x534C4142 ("SLAB")
	/// Record class (ADR 075). `0` = ordinary blob/Arrow record. Every
	/// record written before ADR 075, and every non-counter record, carries
	/// `0` in this slot (it was the `_pad_align` zero-fill), so their
	/// recovery is unchanged and byte-identical. Counter frames carry a
	/// non-zero [`record_class`] so the recovery scan applies the
	/// counter-specific tentative/commit rule without sniffing opaque frame
	/// bytes. Covered by [`checksum`](Self::checksum) like every other field.
	pub record_class: u32,
	pub tenant_id: TenantId,    // durable tenant scope for this record
	pub key_identity: Identity, // The stable 256-bit identity of the key
	pub hlc: HybridTimestamp,   // 16 bytes: the timestamp of the write
	pub frame_len: u32,         // Total length of the HRFT frame that follows
	/// Integrity checksum over the header prefix (all fields above,
	/// with `checksum` itself read as zero) concatenated with the
	/// frame bytes. Recovery and read paths validate this to detect
	/// torn writes that reach the disk with a valid-looking header
	/// but a truncated frame.
	pub checksum: u32,
}

/// [`SlabRecordHeader::record_class`] values (ADR 075).
///
/// `STANDARD` is the zero default carried by every blob/Arrow record (and
/// every record written before ADR 075), so its recovery path is unchanged.
/// The two counter classes mark the ADR 075 durable counter frames so the
/// recovery scan can apply the tentative/commit-marker rule.
pub mod record_class {
	/// Ordinary blob / Arrow record — recovered exactly as before ADR 075.
	pub const STANDARD: u32 = 0;
	/// A counter tentative-value frame ([`super::CounterTentativeFrame`]). Its
	/// op-id is its durable record offset; recovery indexes it only if a
	/// matching commit marker exists.
	pub const COUNTER_TENTATIVE: u32 = 1;
	/// A counter commit-marker frame ([`super::CounterCommitFrame`]) naming the
	/// tentative op-id (offset) that is now committed.
	pub const COUNTER_COMMIT: u32 = 2;
}

/// ADR 075 counter tentative-value frame: the durable little-endian `u64`
/// counter value. The op-id is the record's durable offset (unique, never
/// reused — the write cursor resumes past it on restart), so the frame itself
/// carries only the value; the paired [`CounterCommitFrame`] references that
/// offset. A point read of a committed counter reads exactly these 8 bytes.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CounterTentativeFrame {
	pub value: u64,
}

/// ADR 075 counter commit-marker frame: references the tentative record's
/// op-id (its durable offset). Recovery indexes a tentative value only if a
/// commit marker for its offset exists; orphan tentatives are skipped.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CounterCommitFrame {
	pub tentative_op_id: u64,
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
		Field::new("value", DataType::LargeBinary, true), // null unless status == OK
	])
}

/// Arrow schema for migration record batches (Plan 019). Each
/// row is one retained version being streamed from a prior
/// physical holder to the new logical owner. `frame_bytes` is
/// the verbatim Arrow IPC payload the holder originally stored
/// on disk — the receiver re-appends it via
/// [`SlabManager::append_migration_with_locality`](crate::storage::manager::SlabManager::append_migration_with_locality)
/// without reshaping or losing custom table routing locality.
pub fn migration_record_schema() -> Schema {
	Schema::new(vec![
		Field::new("tenant_id", DataType::UInt64, false),
		Field::new("key_identity", DataType::FixedSizeBinary(32), false),
		Field::new("routing_locality", DataType::UInt64, false),
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
	/// 32-byte content digest over the payload bytes.
	/// Stored with the active Control Ring profile hash while
	/// keeping the field name stable for on-disk/on-wire
	/// compatibility.
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
//  file-backed `SnapshotData` stream that `WendelonTypeConfig`
//  exposes to OpenRaft. Followed by the postcard-encoded state
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
	/// Stored with the active Control Ring profile hash while
	/// keeping the field name stable for on-disk/on-wire
	/// compatibility.
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
const _: () = assert!(std::mem::size_of::<TableDescriptorGetRequestMeta>() == 264);
const _: () = assert!(std::mem::size_of::<TableDescriptorGetResponseMeta>() == 24);
const _: () = assert!(std::mem::size_of::<TableDescriptorListRequestMeta>() == 8);
const _: () = assert!(std::mem::size_of::<TableDescriptorListResponseMeta>() == 24);
const _: () = assert!(std::mem::size_of::<TableOptimizerFactsGetRequestMeta>() == 304);
const _: () = assert!(std::mem::size_of::<TableOptimizerFactsGetResponseMeta>() == 24);
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 24);
const _: () = assert!(std::mem::size_of::<WriteRequestMeta>() == 120);
const _: () = assert!(std::mem::size_of::<WriteResponseMeta>() == 96);
const _: () = assert!(std::mem::size_of::<ReplicaWriteMeta>() == 144);
const _: () = assert!(std::mem::size_of::<CounterAddRequestMeta>() == 128);
const _: () = assert!(std::mem::size_of::<CounterAddResponseMeta>() == 112);
const _: () = assert!(std::mem::size_of::<CounterReadRequestMeta>() == 48);
const _: () = assert!(std::mem::size_of::<CounterReadResponseMeta>() == 32);
const _: () = assert!(std::mem::size_of::<CounterReplicateRequestMeta>() == 88);
const _: () = assert!(std::mem::size_of::<CounterRollbackRequestMeta>() == 56);
const _: () = assert!(std::mem::size_of::<CounterReplicaAck>() == 16);

// (Counter-opcode collisions are now covered by the global MsgType uniqueness
// guard next to the `MsgType::ALL` list above.)
const _: () = assert!(std::mem::size_of::<ReplicaAckLevel>() == 1);
const _: () = assert!(std::mem::size_of::<ReplicaWriteRespMeta>() == 8);
const _: () = assert!(std::mem::size_of::<PendingWitnessPullRequestMeta>() == 16);
const _: () = assert!(std::mem::size_of::<PendingWitnessPullStatus>() == 1);
const _: () = assert!(std::mem::size_of::<PendingWitnessPullResponseMeta>() == 16);
const _: () = assert!(std::mem::size_of::<ReadRequestMeta>() == 96);
const _: () = assert!(std::mem::size_of::<ReadResponseMeta>() == 56);
const _: () = assert!(std::mem::size_of::<QueryRequestMeta>() == 80);
const _: () = assert!(std::mem::size_of::<QueryResponseMeta>() == 56);
const _: () = assert!(std::mem::size_of::<MigrateRequestMeta>() == 160);
const _: () = assert!(std::mem::size_of::<MigrateResponseMeta>() == 88);
const _: () = assert!(std::mem::size_of::<MigrateRequestKind>() == 1);
const _: () = assert!(std::mem::size_of::<MigrateStatus>() == 1);
const _: () = assert!(std::mem::size_of::<SlabRecordHeader>() == 72);
const _: () = assert!(std::mem::size_of::<CounterTentativeFrame>() == 8);
const _: () = assert!(std::mem::size_of::<CounterCommitFrame>() == 8);
const _: () = assert!(std::mem::size_of::<SlabManifest>() == 160);
const _: () = assert!(std::mem::size_of::<RaftEpochEntryFixed>() == 216);
const _: () = assert!(std::mem::size_of::<LogEntryKind>() == 1);
const _: () = assert!(std::mem::size_of::<RaftLogEntryFixed>() == 96);
const _: () = assert!(std::mem::size_of::<WALRecordHeader>() == 32);
const _: () = assert!(std::mem::size_of::<SnapshotHeader>() == 80);

#[cfg(test)]
mod tests {
	use super::{
		LogicalTypeGetRequestMeta, LogicalTypeGetResponseMeta, LogicalTypeListRequestMeta,
		LogicalTypeListResponseMeta, MsgType, NodeId, QueryRequestMeta, QueryResponseMeta,
		ReadStatus, TableDescriptorGetRequestMeta, TableDescriptorGetResponseMeta,
		TableDescriptorListRequestMeta, TableDescriptorListResponseMeta,
		TableOptimizerFactsGetRequestMeta, TableOptimizerFactsGetResponseMeta,
	};

	#[test]
	fn node_id_wire_text_round_trips() {
		let node_id = NodeId {
			id: 123,
			_reserved: 0,
		};
		assert_eq!(node_id.format_wire(), "n-123");
		assert_eq!(NodeId::parse_wire("n-123").unwrap(), node_id);
	}

	#[test]
	fn node_id_parse_wire_rejects_invalid_text() {
		assert!(NodeId::parse_wire("123").is_err());
		assert!(NodeId::parse_wire("n-abc").is_err());
	}

	#[test]
	fn query_wire_contract_has_no_stats_or_slab_surface() {
		assert_eq!(MsgType::QUERY_REQ.0, 0x0B);
		assert_eq!(MsgType::QUERY_RESP.0, 0x0C);
		assert_eq!(MsgType::AGG_REQ.0, 0x32);
		assert_eq!(MsgType::AGG_RESP.0, 0x33);
		assert_eq!(MsgType::TABLE_QUERY_CANCEL_REQ.0, 0x3E);
		assert_eq!(MsgType::TABLE_QUERY_CANCEL_RESP.0, 0x3F);
		assert_eq!(ReadStatus::QUERY_CANCELLED.0, 15);
		assert_eq!(ReadStatus::QUERY_CANCELLED.as_str(), "query_cancelled");
		assert_eq!(std::mem::size_of::<QueryRequestMeta>(), 80);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, min_hlc), 0);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, expected_epoch), 16);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, tenant_id), 24);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, consistency), 32);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, _pad), 33);
		assert_eq!(
			std::mem::offset_of!(QueryRequestMeta, locality_range_start),
			40
		);
		assert_eq!(
			std::mem::offset_of!(QueryRequestMeta, locality_range_end),
			48
		);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, bytecode_len), 56);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, _pad2), 60);
		assert_eq!(std::mem::offset_of!(QueryRequestMeta, as_of_hlc), 64);

		assert_eq!(std::mem::size_of::<QueryResponseMeta>(), 56);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, status), 0);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, _pad1), 1);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, value_hlc), 8);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, epoch), 24);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, leader_hint), 32);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, rows), 40);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, arrow_len), 48);
		assert_eq!(std::mem::offset_of!(QueryResponseMeta, _pad2), 52);
	}

	#[test]
	fn descriptor_get_wire_contract_is_fixed_pod_and_exact_opcode() {
		assert_eq!(MsgType::TABLE_DESCRIPTOR_GET_REQ.0, 0x3C);
		assert_eq!(MsgType::TABLE_DESCRIPTOR_GET_RESP.0, 0x3D);
		assert_eq!(MsgType::TABLE_DESCRIPTOR_LIST_REQ.0, 0x44);
		assert_eq!(MsgType::TABLE_DESCRIPTOR_LIST_RESP.0, 0x45);
		assert_eq!(MsgType::TABLE_OPTIMIZER_FACTS_GET_REQ.0, 0x46);
		assert_eq!(MsgType::TABLE_OPTIMIZER_FACTS_GET_RESP.0, 0x47);

		assert_eq!(std::mem::size_of::<TableDescriptorGetRequestMeta>(), 264);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetRequestMeta, tenant_id),
			0
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetRequestMeta, namespace),
			8
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetRequestMeta, table_name),
			136
		);

		assert_eq!(std::mem::size_of::<TableDescriptorGetResponseMeta>(), 24);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetResponseMeta, accepted),
			0
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetResponseMeta, reason_code),
			1
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetResponseMeta, leader_hint),
			8
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorGetResponseMeta, descriptor_len),
			16
		);

		assert_eq!(std::mem::size_of::<TableDescriptorListRequestMeta>(), 8);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorListRequestMeta, tenant_id),
			0
		);

		assert_eq!(std::mem::size_of::<TableDescriptorListResponseMeta>(), 24);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorListResponseMeta, accepted),
			0
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorListResponseMeta, reason_code),
			1
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorListResponseMeta, leader_hint),
			8
		);
		assert_eq!(
			std::mem::offset_of!(TableDescriptorListResponseMeta, descriptors_len),
			16
		);

		assert_eq!(
			std::mem::size_of::<TableOptimizerFactsGetRequestMeta>(),
			304
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetRequestMeta, tenant_id),
			0
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetRequestMeta, namespace),
			8
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetRequestMeta, table_name),
			136
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetRequestMeta, schema_generation),
			264
		);
		assert_eq!(
			std::mem::offset_of!(
				TableOptimizerFactsGetRequestMeta,
				physical_descriptor_fingerprint
			),
			272
		);

		assert_eq!(
			std::mem::size_of::<TableOptimizerFactsGetResponseMeta>(),
			24
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetResponseMeta, accepted),
			0
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetResponseMeta, reason_code),
			1
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetResponseMeta, leader_hint),
			8
		);
		assert_eq!(
			std::mem::offset_of!(TableOptimizerFactsGetResponseMeta, facts_len),
			16
		);
	}

	#[test]
	fn logical_type_get_wire_contract_is_fixed_pod_and_exact_opcode() {
		assert_eq!(MsgType::LOGICAL_TYPE_GET_REQ.0, 0x40);
		assert_eq!(MsgType::LOGICAL_TYPE_GET_RESP.0, 0x41);
		assert_eq!(MsgType::LOGICAL_TYPE_LIST_REQ.0, 0x42);
		assert_eq!(MsgType::LOGICAL_TYPE_LIST_RESP.0, 0x43);

		assert_eq!(std::mem::size_of::<LogicalTypeGetRequestMeta>(), 136);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeGetRequestMeta, tenant_id),
			0
		);
		assert_eq!(std::mem::offset_of!(LogicalTypeGetRequestMeta, name), 8);

		assert_eq!(std::mem::size_of::<LogicalTypeGetResponseMeta>(), 24);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeGetResponseMeta, accepted),
			0
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeGetResponseMeta, reason_code),
			1
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeGetResponseMeta, leader_hint),
			8
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeGetResponseMeta, definition_len),
			16
		);

		assert_eq!(std::mem::size_of::<LogicalTypeListRequestMeta>(), 8);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeListRequestMeta, tenant_id),
			0
		);

		assert_eq!(std::mem::size_of::<LogicalTypeListResponseMeta>(), 24);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeListResponseMeta, accepted),
			0
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeListResponseMeta, reason_code),
			1
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeListResponseMeta, leader_hint),
			8
		);
		assert_eq!(
			std::mem::offset_of!(LogicalTypeListResponseMeta, definitions_len),
			16
		);
	}
}

#[cfg(test)]
mod adr540_read_status_tests {
	use super::*;

	#[test]
	fn every_named_status_has_a_distinct_name_and_code() {
		let mut codes = std::collections::BTreeSet::new();
		let mut names = std::collections::BTreeSet::new();
		for status in ReadStatus::ALL_NAMED {
			assert_ne!(
				status.as_str(),
				"unknown",
				"status {} is in ALL_NAMED without an as_str name",
				status.0
			);
			assert!(codes.insert(status.0), "duplicate code {}", status.0);
			assert!(names.insert(status.as_str()), "duplicate name");
		}
	}
}
