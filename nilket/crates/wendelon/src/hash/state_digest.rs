use crate::schema::{
	HybridTimestamp, Identity, IndexObjectId, LogicalSchemaFingerprint, TableObjectId, TenantId,
};
use bytemuck::{Pod, Zeroable};
use curve25519_dalek::constants::RISTRETTO_BASEPOINT_TABLE;
use curve25519_dalek::ristretto::{CompressedRistretto, RistrettoPoint};
use curve25519_dalek::scalar::Scalar;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use thiserror::Error;

const ELEMENT_PROJECTION_DOMAIN: &[u8] = b"state-digest.element";

pub const DOMAIN_TABLE_ROW_VISIBLE: StateDigestDomain =
	StateDigestDomain(b"digest.table.row.visible.v1");
pub const DOMAIN_TABLE_ROW_PHYSICAL: StateDigestDomain =
	StateDigestDomain(b"digest.table.row.physical.v1");
pub const DOMAIN_TABLE_ROW_TOMBSTONE: StateDigestDomain =
	StateDigestDomain(b"digest.table.row.tombstone.v1");
pub const DOMAIN_SECONDARY_INDEX_ENTRY: StateDigestDomain =
	StateDigestDomain(b"digest.secondary_index.entry.v1");
pub const DOMAIN_RETENTION_GC: StateDigestDomain = StateDigestDomain(b"digest.retention.gc.v1");
/// ADR 0699 S5a: one certified base survivor.
pub const DOMAIN_CERTIFIED_BASE: StateDigestDomain = StateDigestDomain(b"digest.certified.base.v1");

/// Compressed Ristretto255 state identity for an accumulated multiset.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, Pod, Zeroable, Serialize)]
#[repr(transparent)]
pub struct StateDigest([u8; 32]);

impl StateDigest {
	/// The compressed Ristretto255 identity point.
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

	pub fn decompress(self) -> Result<RistrettoPoint, StateDigestError> {
		CompressedRistretto(self.0)
			.decompress()
			.ok_or(StateDigestError::InvalidPoint)
	}
}

impl<'de> Deserialize<'de> for StateDigest {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let bytes = <[u8; 32]>::deserialize(deserializer)?;
		let digest = Self(bytes);
		digest.decompress().map_err(D::Error::custom)?;
		Ok(digest)
	}
}

impl fmt::Debug for StateDigest {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "StateDigest(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{byte:02x}")?;
		}
		write!(f, ")")
	}
}

/// Mutable cold-path accumulator for algebraic state digests.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StateDigestAccumulator {
	point: RistrettoPoint,
}

impl StateDigestAccumulator {
	pub fn new() -> Self {
		Self {
			point: RistrettoPoint::default(),
		}
	}

	pub fn from_digest(digest: StateDigest) -> Result<Self, StateDigestError> {
		Ok(Self {
			point: digest.decompress()?,
		})
	}

	pub fn add_element(&mut self, element: StateDigestElement) {
		self.point += &element.point;
	}

	pub fn subtract_element(&mut self, element: StateDigestElement) {
		self.point -= &element.point;
	}

	pub fn merge_digest(&mut self, digest: StateDigest) -> Result<(), StateDigestError> {
		self.point += &digest.decompress()?;
		Ok(())
	}

	pub fn subtract_digest(&mut self, digest: StateDigest) -> Result<(), StateDigestError> {
		self.point -= &digest.decompress()?;
		Ok(())
	}

	pub fn finish(self) -> StateDigest {
		StateDigest(self.point.compress().to_bytes())
	}
}

impl Default for StateDigestAccumulator {
	fn default() -> Self {
		Self::new()
	}
}

impl fmt::Debug for StateDigestAccumulator {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"StateDigestAccumulator({:?})",
			StateDigest(self.point.compress().to_bytes())
		)
	}
}

/// Projected digest element. Construction is domain-separated.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StateDigestElement {
	point: RistrettoPoint,
}

impl StateDigestElement {
	pub fn project(domain: StateDigestDomain, preimage: &[u8]) -> Self {
		Self::project_checked(domain, preimage)
			.expect("state digest domain constants must be well-formed")
	}

	pub fn project_checked(
		domain: StateDigestDomain,
		preimage: &[u8],
	) -> Result<Self, StateDigestError> {
		Ok(Self::from_projected_scalar_bytes(
			projected_scalar_bytes_checked(domain, preimage)?,
		))
	}

	pub fn digest(self) -> StateDigest {
		StateDigest(self.point.compress().to_bytes())
	}

	fn from_projected_scalar_bytes(bytes: [u8; 32]) -> Self {
		let scalar = Scalar::from_bytes_mod_order(bytes);
		Self {
			point: &scalar * RISTRETTO_BASEPOINT_TABLE,
		}
	}
}

impl fmt::Debug for StateDigestElement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "StateDigestElement({:?})", self.digest())
	}
}

/// Domain tag used in state-digest element projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct StateDigestDomain(pub &'static [u8]);

impl StateDigestDomain {
	pub const fn tag(self) -> &'static [u8] {
		self.0
	}

	pub fn validate(self) -> Result<(), StateDigestError> {
		let tag = self.0;
		if tag.is_empty()
			|| !tag.starts_with(b"digest.")
			|| !tag.ends_with(b".v1")
			|| !tag.iter().all(|byte| {
				byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(*byte, b'.' | b'_')
			}) {
			return Err(StateDigestError::MalformedDomainTag);
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum StateDigestError {
	#[error("state digest bytes are not a valid compressed Ristretto255 point")]
	InvalidPoint,
	#[error("state digest domain tag is malformed")]
	MalformedDomainTag,
}

/// Compute the domain-separated scalar bytes used by `StateDigestElement::project`.
pub fn projected_scalar_bytes(domain: StateDigestDomain, preimage: &[u8]) -> [u8; 32] {
	projected_scalar_bytes_checked(domain, preimage)
		.expect("state digest domain constants must be well-formed")
}

pub fn projected_scalar_bytes_checked(
	domain: StateDigestDomain,
	preimage: &[u8],
) -> Result<[u8; 32], StateDigestError> {
	domain.validate()?;
	let mut hasher = blake3::Hasher::new();
	hasher.update(ELEMENT_PROJECTION_DOMAIN);
	hasher.update(&(domain.tag().len() as u32).to_be_bytes());
	hasher.update(domain.tag());
	hasher.update(&(preimage.len() as u64).to_be_bytes());
	hasher.update(preimage);
	Ok(*hasher.finalize().as_bytes())
}

/// Accumulate already-projected scalar bytes without a multiply-per-row loop.
pub fn accumulate_projected_scalars(scalars: impl IntoIterator<Item = [u8; 32]>) -> StateDigest {
	let scalar_sum = scalars
		.into_iter()
		.map(Scalar::from_bytes_mod_order)
		.fold(Scalar::ZERO, |acc, scalar| acc + scalar);
	StateDigest(
		(&scalar_sum * RISTRETTO_BASEPOINT_TABLE)
			.compress()
			.to_bytes(),
	)
}

/// ADR 046 V1 visible-row helper.
pub fn visible_row_element(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	row_key_id: Identity,
) -> StateDigestElement {
	let mut preimage = [0u8; 64];
	preimage[..16].copy_from_slice(&tenant_id_digest_bytes(tenant_id));
	preimage[16..32].copy_from_slice(table_object_id.as_bytes());
	preimage[32..64].copy_from_slice(row_key_id.as_bytes());
	StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage)
}

/// ADR 048 retention eviction reversal for the visible row domain.
///
/// Data rows add `+K` at admission, so eviction subtracts `K`.
/// Row tombstones add `-K` at admission, so eviction adds `K` back.
pub fn apply_retention_eviction_to_visible_digest(
	accumulator: &mut StateDigestAccumulator,
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	row_key_id: Identity,
	is_tombstone: bool,
) {
	let element = visible_row_element(tenant_id, table_object_id, row_key_id);
	if is_tombstone {
		accumulator.add_element(element);
	} else {
		accumulator.subtract_element(element);
	}
}

/// ADR 052 logical secondary-index version-entry helper.
///
/// Physical locator fields are intentionally absent: replicas can
/// rewrite slab placement independently without producing digest
/// drift for the same logical index entries.
pub fn secondary_index_entry_element(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	index_object_id: IndexObjectId,
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	column_ordinal: u32,
	encoded_key: &[u8],
	row_key_id: Identity,
	row_hlc: HybridTimestamp,
) -> StateDigestElement {
	let mut preimage = Vec::with_capacity(8 + 16 + 16 + 32 + 4 + 8 + encoded_key.len() + 32 + 16);
	preimage.extend_from_slice(&tenant_id.get().to_le_bytes());
	preimage.extend_from_slice(table_object_id.as_bytes());
	preimage.extend_from_slice(index_object_id.as_bytes());
	preimage.extend_from_slice(logical_schema_fingerprint.as_bytes());
	preimage.extend_from_slice(&column_ordinal.to_le_bytes());
	preimage.extend_from_slice(&(encoded_key.len() as u64).to_le_bytes());
	preimage.extend_from_slice(encoded_key);
	preimage.extend_from_slice(row_key_id.as_bytes());
	preimage.extend_from_slice(&row_hlc.physical_ms.to_le_bytes());
	preimage.extend_from_slice(&row_hlc.logical.to_le_bytes());
	preimage.extend_from_slice(&row_hlc.node_id.to_le_bytes());
	StateDigestElement::project(DOMAIN_SECONDARY_INDEX_ENTRY, &preimage)
}

fn tenant_id_digest_bytes(tenant_id: TenantId) -> [u8; 16] {
	let mut bytes = [0u8; 16];
	bytes[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
	bytes
}

#[cfg(test)]
mod tests {
	use super::*;

	const MALFORMED_DOMAIN_FOR_TEST: StateDigestDomain = StateDigestDomain(b"table.row.visible");

	fn hex(bytes: &[u8]) -> String {
		bytes.iter().map(|byte| format!("{byte:02x}")).collect()
	}

	fn visible_preimage(
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		row_key_id: Identity,
	) -> [u8; 64] {
		let mut preimage = [0u8; 64];
		preimage[..16].copy_from_slice(&tenant_id_digest_bytes(tenant_id));
		preimage[16..32].copy_from_slice(table_object_id.as_bytes());
		preimage[32..64].copy_from_slice(row_key_id.as_bytes());
		preimage
	}

	fn fixture_value(key: &str) -> &'static str {
		include_str!("../../tests/fixtures/state_digest/vectors.txt")
			.lines()
			.filter_map(|line| line.split_once('='))
			.find(|(name, _)| *name == key)
			.map(|(_, value)| value)
			.unwrap_or_else(|| panic!("missing fixture key {key}"))
	}

	#[test]
	fn zero_digest_is_compressed_identity() {
		assert_eq!(StateDigest::ZERO, StateDigest::new([0; 32]));
		assert!(StateDigest::ZERO.decompress().is_ok());
	}

	#[test]
	fn invalid_compressed_point_rejects() {
		assert_eq!(
			StateDigest::new([0xff; 32]).decompress(),
			Err(StateDigestError::InvalidPoint)
		);
	}

	#[test]
	fn serde_deserialize_rejects_invalid_point() {
		let zero_json = serde_json::to_string(&StateDigest::ZERO).unwrap();
		assert_eq!(
			serde_json::from_str::<StateDigest>(&zero_json).unwrap(),
			StateDigest::ZERO
		);
		let invalid_json = format!("[{}]", ["255"; 32].join(","));
		assert!(serde_json::from_str::<StateDigest>(&invalid_json).is_err());
	}

	#[test]
	fn from_digest_round_trips() {
		let digest = StateDigest::new([0; 32]);
		assert_eq!(
			StateDigestAccumulator::from_digest(digest)
				.unwrap()
				.finish(),
			digest
		);
	}

	#[test]
	fn malformed_domain_rejects_checked_projection() {
		assert_eq!(
			projected_scalar_bytes_checked(MALFORMED_DOMAIN_FOR_TEST, b"row"),
			Err(StateDigestError::MalformedDomainTag)
		);
	}

	#[test]
	fn visible_row_tenant_slot_is_u64_le_zero_padded() {
		assert_eq!(std::mem::size_of::<TenantId>(), 8);
		let tenant = TenantId::new(0x0102_0304_0506_0708);
		let table = TableObjectId::new([0x46; 16]);
		let row = Identity::new([0xa5; 32]);
		let preimage = visible_preimage(tenant, table, row);
		assert_eq!(
			&preimage[..16],
			&[
				0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
			]
		);
		assert_eq!(&preimage[16..32], table.as_bytes());
		assert_eq!(&preimage[32..64], row.as_bytes());
	}

	#[test]
	fn golden_vectors_are_pinned() {
		let tenant = TenantId::new(0x0102_0304_0506_0708);
		let table = TableObjectId::new([0x46; 16]);
		let row_a = Identity::new([0xa5; 32]);
		let row_b = Identity::new([0x5a; 32]);
		let preimage_a = visible_preimage(tenant, table, row_a);
		let preimage_b = visible_preimage(tenant, table, row_b);

		let element_a = StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage_a);
		assert_eq!(
			hex(StateDigest::ZERO.as_bytes()),
			fixture_value("empty_digest")
		);
		assert_eq!(
			hex(&element_a.digest().into_bytes()),
			fixture_value("single_visible_row_element")
		);

		let mut ab = StateDigestAccumulator::new();
		ab.add_element(StateDigestElement::project(
			DOMAIN_TABLE_ROW_VISIBLE,
			&preimage_a,
		));
		ab.add_element(StateDigestElement::project(
			DOMAIN_TABLE_ROW_VISIBLE,
			&preimage_b,
		));
		let digest_ab = ab.finish();

		let mut ba = StateDigestAccumulator::new();
		ba.add_element(StateDigestElement::project(
			DOMAIN_TABLE_ROW_VISIBLE,
			&preimage_b,
		));
		ba.add_element(StateDigestElement::project(
			DOMAIN_TABLE_ROW_VISIBLE,
			&preimage_a,
		));
		assert_eq!(digest_ab, ba.finish());
		assert_eq!(
			hex(digest_ab.as_bytes()),
			fixture_value("two_visible_rows_ab")
		);
		assert_eq!(
			hex(digest_ab.as_bytes()),
			fixture_value("two_visible_rows_ba")
		);

		let mut split = StateDigestAccumulator::new();
		split.merge_digest(element_a.digest()).unwrap();
		split
			.merge_digest(
				StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage_b).digest(),
			)
			.unwrap();
		assert_eq!(digest_ab, split.finish());
		assert_eq!(
			hex(digest_ab.as_bytes()),
			fixture_value("split_visible_rows")
		);

		let mut subtract = StateDigestAccumulator::from_digest(digest_ab).unwrap();
		subtract.subtract_element(StateDigestElement::project(
			DOMAIN_TABLE_ROW_VISIBLE,
			&preimage_b,
		));
		let subtract_digest = subtract.finish();
		assert_eq!(element_a.digest(), subtract_digest);
		assert_eq!(
			hex(subtract_digest.as_bytes()),
			fixture_value("subtract_visible_row_b")
		);

		let physical = StateDigestElement::project(DOMAIN_TABLE_ROW_PHYSICAL, &preimage_a);
		assert_ne!(element_a.digest(), physical.digest());
		assert_eq!(
			hex(&physical.digest().into_bytes()),
			fixture_value("single_physical_row_element")
		);

		assert_ne!(
			projected_scalar_bytes(DOMAIN_TABLE_ROW_VISIBLE, &preimage_a),
			row_a.into_bytes()
		);
		assert_eq!(
			hex(&projected_scalar_bytes(
				DOMAIN_TABLE_ROW_VISIBLE,
				&preimage_a
			)),
			fixture_value("visible_row_scalar")
		);
	}

	#[test]
	fn batched_accumulation_matches_per_element_accumulation() {
		let tenant = TenantId::new(42);
		let table = TableObjectId::new([0x24; 16]);
		let preimages = (0u8..16)
			.map(|byte| visible_preimage(tenant, table, Identity::new([byte; 32])))
			.collect::<Vec<_>>();

		let mut per_element = StateDigestAccumulator::new();
		for preimage in &preimages {
			per_element.add_element(StateDigestElement::project(
				DOMAIN_TABLE_ROW_VISIBLE,
				preimage,
			));
		}
		let scalar_bytes = preimages
			.iter()
			.map(|preimage| projected_scalar_bytes(DOMAIN_TABLE_ROW_VISIBLE, preimage))
			.collect::<Vec<_>>();
		assert_eq!(
			accumulate_projected_scalars(scalar_bytes),
			per_element.finish()
		);
	}

	#[test]
	fn retention_eviction_helper_reverses_visible_row_admission() {
		let tenant = TenantId::new(48);
		let table = TableObjectId::new([0x48; 16]);
		let row = Identity::new([0x48; 32]);
		let element = visible_row_element(tenant, table, row);

		let mut data_eviction = StateDigestAccumulator::new();
		data_eviction.add_element(element);
		apply_retention_eviction_to_visible_digest(&mut data_eviction, tenant, table, row, false);
		assert_eq!(StateDigest::ZERO, data_eviction.finish());

		let mut tombstone_eviction = StateDigestAccumulator::new();
		tombstone_eviction.subtract_element(element);
		apply_retention_eviction_to_visible_digest(
			&mut tombstone_eviction,
			tenant,
			table,
			row,
			true,
		);
		assert_eq!(StateDigest::ZERO, tombstone_eviction.finish());
	}
}
