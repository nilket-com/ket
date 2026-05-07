use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Error, ErrorKind};
use std::num::NonZeroU64;

/// Defines a newtype wrapper for a primitive, implementing common traits and
/// ensuring type safety across the Unified Ring.
macro_rules! impl_unit {
	($(#[$meta:meta])* $name:ident, $type:ty) => {
		$(#[$meta])*
		#[derive(
			Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
			Default, Pod, Zeroable, Serialize, Deserialize
		)]
		#[repr(transparent)]
		pub struct $name(pub $type);

		impl $name {
			pub const ZERO: Self = Self(0);
			pub const MAX: Self = Self(<$type>::MAX);

			pub const fn new(value: $type) -> Self {
				Self(value)
			}

			pub const fn get(self) -> $type {
				self.0
			}

			pub const fn saturating_add(self, rhs: $type) -> Self {
				Self(self.0.saturating_add(rhs))
			}

			pub const fn saturating_sub(self, rhs: $type) -> Self {
				Self(self.0.saturating_sub(rhs))
			}
		}

		impl fmt::Display for $name {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				self.0.fmt(f)
			}
		}

		impl fmt::LowerHex for $name {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				fmt::LowerHex::fmt(&self.0, f)
			}
		}

		impl fmt::UpperHex for $name {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				fmt::UpperHex::fmt(&self.0, f)
			}
		}

		impl From<$type> for $name {
			fn from(value: $type) -> Self {
				Self(value)
			}
		}

		impl From<$name> for $type {
			fn from(value: $name) -> Self {
				value.0
			}
		}
	};
}

// ============================================================
//  CORE UNIFIED RING UNITS
// ============================================================

/// The "True Name" of a record.
///
/// Identity uses 256 bits (32 bytes) of BLAKE3 output. At Wendelon's
/// target scale this is the authoritative storage coordinate, while
/// `Locality` is the lossy routing projection.
#[derive(
	Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct Identity([u8; 32]);

impl Identity {
	pub const ZERO: Self = Self([0; 32]);
	pub const MAX: Self = Self([0xff; 32]);

	pub const fn new(bytes: [u8; 32]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 32] {
		&self.0
	}

	pub const fn into_bytes(self) -> [u8; 32] {
		self.0
	}
}

impl fmt::Debug for Identity {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Identity(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, ")")
	}
}

impl fmt::Display for Identity {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for byte in &self.as_bytes()[..8] {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, "...")
	}
}

impl Identity {
	/// Compute the BLAKE3-256 Identity of a byte slice.
	///
	/// Record Identity is format-stable across build profiles; the FIPS
	/// profile keeps this helper outside the validated module boundary
	/// rather than changing the on-disk/on-wire coordinate.
	pub fn blake3(bytes: &[u8]) -> Self {
		Self(crate::hash::identity::digest(bytes))
	}

	/// Compute the tenant-scoped BLAKE3-256 Identity of a key.
	///
	/// The public tenant id is part of the stable record namespace, so
	/// the same raw key bytes in two tenants never alias to the same
	/// durable identity.
	pub fn blake3_tenant(tenant_id: TenantId, bytes: &[u8]) -> Self {
		Self(crate::hash::identity::tenant_scoped(tenant_id.get(), bytes))
	}

	/// Tenant-scoped keyed BLAKE3-256 Identity.
	///
	/// The exact tenant-domain-separation formulation is locked in by W5.
	/// This helper is about domain-separation expression, not secrecy;
	/// abuse resistance against engineered locality hotspots requires a
	/// separate secret-salt decision.
	pub fn blake3_keyed(key: &[u8; 32], bytes: &[u8]) -> Self {
		Self(crate::hash::identity::keyed(key, bytes))
	}
}

#[derive(
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Default,
	Pod,
	Zeroable,
	Serialize,
	Deserialize,
)]
#[repr(transparent)]
pub struct IdentityPrefix([u8; 16]);

impl IdentityPrefix {
	pub const ZERO: Self = Self([0; 16]);

	pub const fn new(bytes: [u8; 16]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}

	pub const fn into_bytes(self) -> [u8; 16] {
		self.0
	}
}

impl fmt::Debug for IdentityPrefix {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "IdentityPrefix(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, ")")
	}
}

impl fmt::Display for IdentityPrefix {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for byte in &self.as_bytes()[..8] {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, "...")
	}
}

impl From<[u8; 16]> for IdentityPrefix {
	fn from(value: [u8; 16]) -> Self {
		Self(value)
	}
}

impl From<IdentityPrefix> for [u8; 16] {
	fn from(value: IdentityPrefix) -> Self {
		value.0
	}
}

impl From<Identity> for IdentityPrefix {
	fn from(value: Identity) -> Self {
		let mut bytes = [0_u8; 16];
		bytes.copy_from_slice(&value.as_bytes()[..16]);
		Self(bytes)
	}
}

impl From<&Identity> for IdentityPrefix {
	fn from(value: &Identity) -> Self {
		(*value).into()
	}
}

// The "Address" of a record on the Ring. A 64-bit projection of the
// Identity used for routing and placement. A collision here is
// merely a "fat bucket" on the ring, not a data loss event.
#[derive(
	Debug,
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Default,
	Pod,
	Zeroable,
	Serialize,
	Deserialize,
)]
#[repr(transparent)]
pub struct Locality(u64);

impl Locality {
	pub const ZERO: Self = Self(0);
	pub const MAX: Self = Self(u64::MAX);

	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}

	pub const fn saturating_add(self, rhs: u64) -> Self {
		Self(self.0.saturating_add(rhs))
	}

	pub const fn saturating_sub(self, rhs: u64) -> Self {
		Self(self.0.saturating_sub(rhs))
	}
}

impl fmt::Display for Locality {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl From<u64> for Locality {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl From<Locality> for u64 {
	fn from(value: Locality) -> Self {
		value.0
	}
}

impl From<Identity> for Locality {
	fn from(id: Identity) -> Self {
		// Locality is the 64-bit projection of the Identity.
		// We use the first 8 bytes of the BLAKE3 identity coordinate.
		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(&id.as_bytes()[..8]);
		Self(u64::from_le_bytes(bytes))
	}
}

// ============================================================
//  INFRASTRUCTURE UNITS
// ============================================================

// `NodeId` is defined in `schema/core.rs` as an 8-byte `#[repr(C)]` struct
// (`{ id: u32, _reserved: u32 }`) that predates the units module. It
// satisfies the unit discipline as a named Pod type and is not re-declared
// here; its on-wire layout is load-bearing and must not change.

impl_unit!(SlabId, u64);
impl_unit!(LogIndex, u64);
impl_unit!(Epoch, u64);
impl_unit!(TenantId, u64);
impl_unit!(AggregateRequestId, u128);
impl_unit!(AggregateExpressionIndex, u16);
impl_unit!(KllK, u32);
impl_unit!(LocationCount, u64);
impl_unit!(RowCount, u64);
impl_unit!(TargetRowCount, u32);
impl_unit!(MaxRowCount, u32);
impl_unit!(RecordRowIndex, u32);
impl_unit!(DurationMillis, u64);
impl_unit!(ByteCount, u64);
impl_unit!(EncryptedChunkIndex, u64);
impl_unit!(EncryptedChunkCount, u32);
impl_unit!(IpcBufferOffset, u64);
impl_unit!(IpcBufferLength, u64);
impl_unit!(IpcBufferIndex, u32);
impl_unit!(IpcColumnOrdinal, u32);
impl_unit!(RowGroupIndex, u32);
impl_unit!(RowGroupRowCount, u32);
impl_unit!(BitsPerValue, u8);
impl_unit!(BaseValue, i128);
impl_unit!(RunCount, u32);
impl_unit!(DictValueCount, u32);
impl_unit!(Exponent, i16);
impl_unit!(Factor, i64);
impl_unit!(ExceptionCount, u32);
impl_unit!(EncodingPageIndex, u32);
impl_unit!(EncodingPageByteOffset, u32);
impl_unit!(EncodingCascadeIndex, u16);
impl_unit!(BufferValueIndex, u32);
impl_unit!(BufferValueCount, u32);
impl_unit!(SimdPageValueCount, u32);
impl_unit!(SampleRowCount, u32);
impl_unit!(EncodingPageCount, u32);
impl_unit!(TaskCount, u32);
impl_unit!(RecordBodyByteOffset, u64);
impl_unit!(RecordBodyByteLen, u64);
impl_unit!(PartialStateLength, u32);
impl_unit!(OverflowFlag, u8);

/// ADR 055 LSM compaction level index.
///
/// V1 deliberately has exactly four compaction levels, `L0..=L3`. The
/// private field plus manual serde impl keep invalid compaction levels
/// out of persisted and control-ring-adjacent surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Pod, Zeroable)]
#[repr(transparent)]
pub struct CompactionLevelIndex(u8);

impl CompactionLevelIndex {
	pub const MIN_RAW: u8 = 0;
	pub const MAX_RAW: u8 = 3;

	pub const L0: Self = Self(0);
	pub const L1: Self = Self(1);
	pub const L2: Self = Self(2);
	pub const L3: Self = Self(3);
	pub const ALL: [Self; 4] = [Self::L0, Self::L1, Self::L2, Self::L3];
	pub const COMPACTION_SOURCES: [Self; 3] = [Self::L0, Self::L1, Self::L2];

	pub const fn try_new(raw: u8) -> Option<Self> {
		if raw <= Self::MAX_RAW {
			Some(Self(raw))
		} else {
			None
		}
	}

	pub const fn get(self) -> u8 {
		self.0
	}

	pub const fn next(self) -> Option<Self> {
		Self::try_new(self.0.saturating_add(1))
	}

	pub const fn is_compaction_source(self) -> bool {
		self.0 < Self::MAX_RAW
	}
}

impl TryFrom<u8> for CompactionLevelIndex {
	type Error = Error;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		Self::try_new(value).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("LSM compaction level index {value} is outside 0..=3"),
			)
		})
	}
}

impl From<CompactionLevelIndex> for u8 {
	fn from(value: CompactionLevelIndex) -> Self {
		value.0
	}
}

impl fmt::Display for CompactionLevelIndex {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl Serialize for CompactionLevelIndex {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_u8(self.0)
	}
}

impl<'de> Deserialize<'de> for CompactionLevelIndex {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let raw = u8::deserialize(deserializer)?;
		Self::try_new(raw)
			.ok_or_else(|| serde::de::Error::custom(format!("invalid LSM compaction level {raw}")))
	}
}

impl_unit!(CompactionJobId, u64);
impl_unit!(ManifestEpoch, u64);

// ============================================================
//  ADR 045 — APPEND-ONLY IDENTITY UNITS
// ============================================================

/// 16-byte writer node identity bound from authenticated mTLS
/// material (ADR 018) and consumed by ADR 045's append-row
/// identity preimage.
///
/// The 16-byte slot is wider than the existing 8-byte
/// `NodeId` shape so the preimage layout can survive future
/// authentication-identity widening without a wire/format
/// break. V1 derives the bytes from `NodeId` via
/// `WriterNodeId::from_node_id`; the high 12 bytes are zero.
#[derive(
	Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct WriterNodeId([u8; 16]);

impl WriterNodeId {
	pub const ZERO: Self = Self([0; 16]);

	pub const fn new(bytes: [u8; 16]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}

	pub const fn into_bytes(self) -> [u8; 16] {
		self.0
	}
}

impl Default for WriterNodeId {
	fn default() -> Self {
		Self::ZERO
	}
}

impl fmt::Debug for WriterNodeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WriterNodeId(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, ")")
	}
}

impl fmt::Display for WriterNodeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for byte in &self.as_bytes()[..8] {
			write!(f, "{:02x}", byte)?;
		}
		write!(f, "...")
	}
}

impl From<[u8; 16]> for WriterNodeId {
	fn from(value: [u8; 16]) -> Self {
		Self(value)
	}
}

impl From<WriterNodeId> for [u8; 16] {
	fn from(value: WriterNodeId) -> Self {
		value.0
	}
}

/// Per-row position within a single admitted write batch.
///
/// Allocated 0-indexed by the admitting node. The constructor
/// rejects `u32::MAX`, which is reserved as the overflow
/// sentinel; a write request that would require the sentinel
/// rejects atomically before any slab append.
#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct IntraBatchIndex(u32);

impl IntraBatchIndex {
	pub const ZERO: Self = Self(0);
	pub const MAX_VALID: Self = Self(u32::MAX - 1);
	const OVERFLOW_SENTINEL: u32 = u32::MAX;

	pub const fn get(self) -> u32 {
		self.0
	}

	/// Construct an `IntraBatchIndex` from a raw `u32`.
	///
	/// Rejects `u32::MAX` (reserved for overflow detection in
	/// the per-batch allocator path).
	pub fn try_new(value: u32) -> std::io::Result<Self> {
		if value == Self::OVERFLOW_SENTINEL {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"intra-batch index u32::MAX is reserved for overflow detection",
			));
		}
		Ok(Self(value))
	}

	/// Big-endian `u32` representation.
	///
	/// This is the canonical wire/format encoding consumed by
	/// the ADR 045 BLAKE3 identity preimage. Never change.
	pub const fn to_be_bytes(self) -> [u8; 4] {
		self.0.to_be_bytes()
	}
}

impl Default for IntraBatchIndex {
	fn default() -> Self {
		Self::ZERO
	}
}

impl fmt::Display for IntraBatchIndex {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

pub const ADR044_RECORD_BODY_BYTE_BOUND: u64 = 8 * 1024 * 1024;

fn validate_adr044_record_body_unit(name: &str, value: u64) -> std::io::Result<()> {
	if value > ADR044_RECORD_BODY_BYTE_BOUND {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{name} must be <= {ADR044_RECORD_BODY_BYTE_BOUND} bytes"),
		));
	}
	Ok(())
}

impl IpcBufferOffset {
	pub fn try_new(value: u64) -> std::io::Result<Self> {
		validate_adr044_record_body_unit("ipc buffer offset", value)?;
		Ok(Self(value))
	}
}

impl IpcBufferLength {
	pub fn try_new(value: u64) -> std::io::Result<Self> {
		validate_adr044_record_body_unit("ipc buffer length", value)?;
		Ok(Self(value))
	}
}

impl RecordBodyByteOffset {
	pub fn try_new(value: u64) -> std::io::Result<Self> {
		validate_adr044_record_body_unit("record body byte offset", value)?;
		Ok(Self(value))
	}
}

impl RecordBodyByteLen {
	pub fn try_new(value: u64) -> std::io::Result<Self> {
		validate_adr044_record_body_unit("record body byte length", value)?;
		Ok(Self(value))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct CompressionRatio(f32);

impl CompressionRatio {
	pub fn new(value: f32) -> std::io::Result<Self> {
		let ratio = Self(value);
		ratio.validate()?;
		Ok(ratio)
	}

	pub const fn get(self) -> f32 {
		self.0
	}

	pub fn validate(self) -> std::io::Result<()> {
		if !self.0.is_finite() || self.0 < 0.0 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"compression ratio must be finite and non-negative",
			));
		}
		Ok(())
	}
}

impl fmt::Display for CompressionRatio {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct TenantKekId(NonZeroU64);

impl TenantKekId {
	pub const fn new(value: NonZeroU64) -> Self {
		Self(value)
	}

	pub const fn try_new(value: u64) -> Option<Self> {
		match NonZeroU64::new(value) {
			Some(value) => Some(Self(value)),
			None => None,
		}
	}

	pub const fn get(self) -> u64 {
		self.0.get()
	}
}

impl fmt::Display for TenantKekId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.get().fmt(f)
	}
}

impl fmt::LowerHex for TenantKekId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::LowerHex::fmt(&self.get(), f)
	}
}

impl fmt::UpperHex for TenantKekId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::UpperHex::fmt(&self.get(), f)
	}
}

impl TryFrom<u64> for TenantKekId {
	type Error = &'static str;

	fn try_from(value: u64) -> Result<Self, Self::Error> {
		Self::try_new(value).ok_or("tenant KEK id must be non-zero")
	}
}

impl From<TenantKekId> for u64 {
	fn from(value: TenantKekId) -> Self {
		value.get()
	}
}

impl_unit!(
	/// Raft term. Monotonically non-decreasing across leader elections.
	Term, u64
);

impl_unit!(
	/// Wall-clock milliseconds since the Unix epoch. Component of `HybridTimestamp`.
	/// Bounded-drift (ADR 012) is enforced against this value, not against raw `u64`.
	PhysicalMs, u64
);

impl_unit!(
	/// HLC logical counter. Resets to zero when `PhysicalMs` advances.
	HlcLogical, u32
);

impl_unit!(
	/// Per-session monotonic write sequence number. Component of `ClientWriteId`.
	/// Two writes from the same session with the same `SequenceNo` are duplicates.
	SequenceNo, u64
);

impl_unit!(
	/// Vnode ordinal on the Unified Ring.
	VnodeIndex, u32
);

impl_unit!(
	/// Chunk ordinal within a slab. Encoded into the AEAD nonce per ADR 019.
	ChunkIndex, u64
);

// ============================================================
//  PHYSICS-INSPIRED HELPER TYPES
// ============================================================

/// A range on the Unified Ring, defined by Locality coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(C)]
pub struct LocalityRange {
	pub start: Locality,
	pub end: Locality,
}

impl LocalityRange {
	pub const fn full() -> Self {
		Self {
			start: Locality::ZERO,
			end: Locality::MAX,
		}
	}

	pub const fn contains(&self, loc: Locality) -> bool {
		loc.get() >= self.start.get() && loc.get() <= self.end.get()
	}

	pub const fn overlaps(&self, other: &Self) -> bool {
		self.start.get() <= other.end.get() && other.start.get() <= self.end.get()
	}
}
