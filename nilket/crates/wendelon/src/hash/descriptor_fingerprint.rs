use crate::schema::{
	ColumnStatsPolicyV1, LocalityPolicy, RecordBodyCompressionPolicy, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{Error, ErrorKind};

pub const LOGICAL_SCHEMA_DOMAIN: &[u8] = b"digest.table.schema.logical.v1";
pub const PHYSICAL_DESCRIPTOR_DOMAIN: &[u8] = b"digest.table.descriptor.physical.v1";

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogicalSchemaFingerprint([u8; 32]);

impl LogicalSchemaFingerprint {
	pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 32] {
		&self.0
	}
}

impl fmt::Debug for LogicalSchemaFingerprint {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "LogicalSchemaFingerprint(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{byte:02x}")?;
		}
		write!(f, ")")
	}
}

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PhysicalDescriptorFingerprint([u8; 32]);

impl PhysicalDescriptorFingerprint {
	pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 32] {
		&self.0
	}
}

impl fmt::Debug for PhysicalDescriptorFingerprint {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "PhysicalDescriptorFingerprint(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{byte:02x}")?;
		}
		write!(f, ")")
	}
}

pub(crate) fn compute_logical_schema_fingerprint(
	table_object_id: &TableObjectId,
	namespace: &str,
	table_name: &str,
	arrow_schema_ipc: &[u8],
	row_identity_policy: &RowIdentityPolicy,
	evolution_policy: &SchemaEvolutionPolicyV1,
) -> std::io::Result<LogicalSchemaFingerprint> {
	let mut hasher = blake3::Hasher::new();
	hasher.update(LOGICAL_SCHEMA_DOMAIN);
	hasher.update(table_object_id.as_bytes());
	update_length_prefixed(&mut hasher, namespace.as_bytes())?;
	update_length_prefixed(&mut hasher, table_name.as_bytes())?;
	update_length_prefixed(&mut hasher, arrow_schema_ipc)?;
	update_length_prefixed_postcard(&mut hasher, row_identity_policy)?;
	update_length_prefixed_postcard(&mut hasher, evolution_policy)?;
	Ok(LogicalSchemaFingerprint(*hasher.finalize().as_bytes()))
}

pub(crate) fn compute_physical_descriptor_fingerprint(
	logical: LogicalSchemaFingerprint,
	locality_policy: &LocalityPolicy,
	record_layout_policy: &RecordLayoutPolicy,
	record_body_compression_policy: &RecordBodyCompressionPolicy,
	stats_policy: &ColumnStatsPolicyV1,
) -> std::io::Result<PhysicalDescriptorFingerprint> {
	let mut hasher = blake3::Hasher::new();
	hasher.update(PHYSICAL_DESCRIPTOR_DOMAIN);
	hasher.update(logical.as_bytes());
	update_length_prefixed_postcard(&mut hasher, locality_policy)?;
	update_length_prefixed_postcard(&mut hasher, record_layout_policy)?;
	update_length_prefixed_postcard(&mut hasher, record_body_compression_policy)?;
	update_length_prefixed_postcard(&mut hasher, stats_policy)?;
	Ok(PhysicalDescriptorFingerprint(*hasher.finalize().as_bytes()))
}

fn update_length_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) -> std::io::Result<()> {
	// ADR 051 fixes the component length prefix at u64 LE so Arrow IPC
	// and postcard payload sizes remain unconstrained by the fingerprint format.
	let len = u64::try_from(bytes.len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidInput,
			"descriptor fingerprint component length exceeds u64::MAX",
		)
	})?;
	hasher.update(&len.to_le_bytes());
	hasher.update(bytes);
	Ok(())
}

fn update_length_prefixed_postcard<T: Serialize>(
	hasher: &mut blake3::Hasher,
	value: &T,
) -> std::io::Result<()> {
	let bytes = postcard::to_allocvec(value).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode descriptor fingerprint component: {err}"),
		)
	})?;
	update_length_prefixed(hasher, &bytes)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn domain_tags_are_pinned() {
		assert_eq!(LOGICAL_SCHEMA_DOMAIN, b"digest.table.schema.logical.v1");
		assert_eq!(
			PHYSICAL_DESCRIPTOR_DOMAIN,
			b"digest.table.descriptor.physical.v1"
		);
	}

	#[test]
	fn debug_formats_hex_bytes() {
		let logical = LogicalSchemaFingerprint::from_bytes([0xAB; 32]);
		let physical = PhysicalDescriptorFingerprint::from_bytes([0xCD; 32]);
		assert!(format!("{logical:?}").contains("abababab"));
		assert!(format!("{physical:?}").contains("cdcdcdcd"));
	}
}
