use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
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
impl_unit!(PartialStateLength, u32);
impl_unit!(OverflowFlag, u8);

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
