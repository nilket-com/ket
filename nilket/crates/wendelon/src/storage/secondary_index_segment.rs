use crate::hash::state_digest::{
	StateDigest, StateDigestAccumulator, secondary_index_entry_element,
};
use crate::schema::{
	HybridTimestamp, Identity, IndexArrowTypeTag, IndexObjectId, LogicalSchemaFingerprint,
	RecordRowIndex, SlabId, TableObjectId, TenantId,
};
use crate::storage::RecordLocation;
use crate::storage::secondary_index::{SecondaryIndexEntry, SecondaryIndexIdentity};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

pub const SECONDARY_INDEX_SEGMENT_FORMAT_VERSION: u16 = 1;
const SECONDARY_INDEX_SEGMENT_MAGIC: [u8; 8] = *b"SIDXSEG1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexSegmentHeader {
	pub magic: [u8; 8],
	pub format_version: u16,
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub index_object_id: IndexObjectId,
	pub source_slab_id: SlabId,
	pub table_schema_generation: u64,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub column_ordinal: u32,
	pub arrow_type_tag: IndexArrowTypeTag,
	pub entry_count: u64,
	#[serde(with = "serde_bytes")]
	pub min_encoded_key: Vec<u8>,
	#[serde(with = "serde_bytes")]
	pub max_encoded_key: Vec<u8>,
	pub state_digest: StateDigest,
	pub payload_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexSegment {
	pub header: SecondaryIndexSegmentHeader,
	pub entries: Vec<SecondaryIndexEntry>,
}

impl SecondaryIndexSegment {
	pub fn build(
		identity: SecondaryIndexIdentity,
		source_slab_id: SlabId,
		table_schema_generation: u64,
		arrow_type_tag: IndexArrowTypeTag,
		mut entries: Vec<SecondaryIndexEntry>,
	) -> std::io::Result<Self> {
		entries.sort_by(SecondaryIndexEntry::logical_cmp);
		let state_digest = digest_entries(identity, &entries);
		let min_encoded_key = entries
			.first()
			.map(|entry| entry.encoded_key.clone())
			.unwrap_or_default();
		let max_encoded_key = entries
			.last()
			.map(|entry| entry.encoded_key.clone())
			.unwrap_or_default();
		let encoded_entries = postcard::to_allocvec(&SerializableEntries::from_entries(&entries))
			.map_err(|err| {
			Error::new(ErrorKind::InvalidData, format!("encode entries: {err}"))
		})?;
		let payload_hash = *blake3::hash(&encoded_entries).as_bytes();
		Ok(Self {
			header: SecondaryIndexSegmentHeader {
				magic: SECONDARY_INDEX_SEGMENT_MAGIC,
				format_version: SECONDARY_INDEX_SEGMENT_FORMAT_VERSION,
				tenant_id: identity.tenant_id,
				table_object_id: identity.table_object_id,
				index_object_id: identity.index_object_id,
				source_slab_id,
				table_schema_generation,
				logical_schema_fingerprint: identity.logical_schema_fingerprint,
				column_ordinal: identity.column_ordinal,
				arrow_type_tag,
				entry_count: entries.len() as u64,
				min_encoded_key,
				max_encoded_key,
				state_digest,
				payload_hash,
			},
			entries,
		})
	}

	pub fn encode(&self) -> std::io::Result<Vec<u8>> {
		let encoded = SerializableSegment::from_segment(self);
		postcard::to_allocvec(&encoded).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to encode secondary index segment: {err}"),
			)
		})
	}

	pub fn decode(bytes: &[u8]) -> std::io::Result<Self> {
		let encoded: SerializableSegment = postcard::from_bytes(bytes).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to decode secondary index segment: {err}"),
			)
		})?;
		let segment = encoded.into_segment();
		segment.validate()?;
		Ok(segment)
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.header.magic != SECONDARY_INDEX_SEGMENT_MAGIC {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment magic mismatch",
			));
		}
		if self.header.format_version != SECONDARY_INDEX_SEGMENT_FORMAT_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment format_version mismatch",
			));
		}
		if self.header.entry_count != self.entries.len() as u64 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment entry_count mismatch",
			));
		}
		if self
			.entries
			.windows(2)
			.any(|window| window[0].logical_cmp(&window[1]).is_gt())
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment entries are not sorted",
			));
		}
		let identity = self.identity();
		if digest_entries(identity, &self.entries) != self.header.state_digest {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment state digest mismatch",
			));
		}
		let encoded_entries = postcard::to_allocvec(&SerializableEntries::from_entries(
			&self.entries,
		))
		.map_err(|err| Error::new(ErrorKind::InvalidData, format!("encode entries: {err}")))?;
		if *blake3::hash(&encoded_entries).as_bytes() != self.header.payload_hash {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index segment payload hash mismatch",
			));
		}
		Ok(())
	}

	pub fn identity(&self) -> SecondaryIndexIdentity {
		SecondaryIndexIdentity {
			tenant_id: self.header.tenant_id,
			table_object_id: self.header.table_object_id,
			index_object_id: self.header.index_object_id,
			logical_schema_fingerprint: self.header.logical_schema_fingerprint,
			column_ordinal: self.header.column_ordinal,
		}
	}
}

pub fn secondary_index_segment_storage_path(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	index_object_id: IndexObjectId,
	source_slab_id: SlabId,
) -> String {
	format!(
		"secondary_index/{:016x}/{}/{}/{:016x}.idx",
		tenant_id.get(),
		hex16(table_object_id.as_bytes()),
		hex16(index_object_id.as_bytes()),
		source_slab_id.get()
	)
}

fn hex16(bytes: &[u8; 16]) -> String {
	let mut out = String::with_capacity(32);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
	}
	out
}

fn digest_entries(
	identity: SecondaryIndexIdentity,
	entries: &[SecondaryIndexEntry],
) -> StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for entry in entries {
		accumulator.add_element(secondary_index_entry_element(
			identity.tenant_id,
			identity.table_object_id,
			identity.index_object_id,
			identity.logical_schema_fingerprint,
			identity.column_ordinal,
			&entry.encoded_key,
			entry.row_key_id,
			entry.row_hlc,
		));
	}
	accumulator.finish()
}

#[derive(Serialize, Deserialize)]
struct SerializableSegment {
	header: SecondaryIndexSegmentHeader,
	entries: SerializableEntries,
}

impl SerializableSegment {
	fn from_segment(segment: &SecondaryIndexSegment) -> Self {
		Self {
			header: segment.header.clone(),
			entries: SerializableEntries::from_entries(&segment.entries),
		}
	}

	fn into_segment(self) -> SecondaryIndexSegment {
		SecondaryIndexSegment {
			header: self.header,
			entries: self.entries.into_entries(),
		}
	}
}

#[derive(Serialize, Deserialize)]
struct SerializableEntries {
	entries: Vec<SerializableEntry>,
}

impl SerializableEntries {
	fn from_entries(entries: &[SecondaryIndexEntry]) -> Self {
		Self {
			entries: entries.iter().map(SerializableEntry::from_entry).collect(),
		}
	}

	fn into_entries(self) -> Vec<SecondaryIndexEntry> {
		self.entries
			.into_iter()
			.map(SerializableEntry::into_entry)
			.collect()
	}
}

#[derive(Serialize, Deserialize)]
struct SerializableEntry {
	#[serde(with = "serde_bytes")]
	encoded_key: Vec<u8>,
	row_key_id: Identity,
	row_hlc: HybridTimestamp,
	slab_id: SlabId,
	record_offset: u64,
	record_length: u32,
	record_row_index: RecordRowIndex,
}

impl SerializableEntry {
	fn from_entry(entry: &SecondaryIndexEntry) -> Self {
		Self {
			encoded_key: entry.encoded_key.clone(),
			row_key_id: entry.row_key_id,
			row_hlc: entry.row_hlc,
			slab_id: entry.record_location.slab_id,
			record_offset: entry.record_location.offset,
			record_length: entry.record_location.length,
			record_row_index: entry.record_location.record_row_index,
		}
	}

	fn into_entry(self) -> SecondaryIndexEntry {
		SecondaryIndexEntry {
			encoded_key: self.encoded_key,
			row_key_id: self.row_key_id,
			row_hlc: self.row_hlc,
			record_location: RecordLocation {
				slab_id: self.slab_id,
				offset: self.record_offset,
				length: self.record_length,
				hlc: self.row_hlc,
				record_row_index: self.record_row_index,
				is_tombstone: false,
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		IndexObjectId, LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, TableSchemaDescriptor,
	};
	use arrow::datatypes::{DataType, Field, Schema};

	fn hlc(value: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: value,
			logical: 0,
			node_id: 1,
		}
	}

	fn identity(byte: u8) -> Identity {
		Identity::new([byte; 32])
	}

	fn table() -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([0x52; 16]),
			1,
			"analytics",
			"events",
			&Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("customer_id", DataType::UInt64, true),
			]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn entry(encoded_key: Vec<u8>, row: Identity, row_hlc: HybridTimestamp) -> SecondaryIndexEntry {
		SecondaryIndexEntry {
			encoded_key,
			row_key_id: row,
			row_hlc,
			record_location: RecordLocation {
				slab_id: SlabId::new(9),
				offset: row_hlc.physical_ms,
				length: 64,
				hlc: row_hlc,
				record_row_index: RecordRowIndex::ZERO,
				is_tombstone: false,
			},
		}
	}

	#[test]
	fn segment_roundtrip_sorts_entries_and_verifies_digest() {
		let table = table();
		let segment_identity = SecondaryIndexIdentity {
			tenant_id: TenantId::new(7),
			table_object_id: table.table_object_id,
			index_object_id: IndexObjectId::new([0x99; 16]),
			logical_schema_fingerprint: table.logical_schema_fingerprint,
			column_ordinal: 1,
		};
		let segment = SecondaryIndexSegment::build(
			segment_identity,
			SlabId::new(9),
			table.schema_generation,
			IndexArrowTypeTag::UInt64,
			vec![
				entry(vec![0x02], identity(2), hlc(20)),
				entry(vec![0x01], identity(1), hlc(10)),
			],
		)
		.unwrap();
		assert_eq!(segment.entries[0].encoded_key, vec![0x01]);
		let encoded = segment.encode().unwrap();
		let decoded = SecondaryIndexSegment::decode(&encoded).unwrap();
		assert_eq!(decoded, segment);
	}

	#[test]
	fn storage_path_uses_stable_sidecar_namespace() {
		assert_eq!(
			secondary_index_segment_storage_path(
				TenantId::new(7),
				TableObjectId::new([0x10; 16]),
				IndexObjectId::new([0x52; 16]),
				SlabId::new(41),
			),
			"secondary_index/0000000000000007/10101010101010101010101010101010/52525252525252525252525252525252/0000000000000029.idx"
		);
	}

	#[test]
	fn segment_validation_rejects_tampered_payload_hash() {
		let table = table();
		let segment_identity = SecondaryIndexIdentity {
			tenant_id: TenantId::new(7),
			table_object_id: table.table_object_id,
			index_object_id: IndexObjectId::new([0x99; 16]),
			logical_schema_fingerprint: table.logical_schema_fingerprint,
			column_ordinal: 1,
		};
		let mut segment = SecondaryIndexSegment::build(
			segment_identity,
			SlabId::new(9),
			table.schema_generation,
			IndexArrowTypeTag::UInt64,
			vec![entry(vec![0x01], identity(1), hlc(10))],
		)
		.unwrap();
		segment.header.payload_hash[0] ^= 0xff;
		assert!(segment.validate().is_err());
	}
}
