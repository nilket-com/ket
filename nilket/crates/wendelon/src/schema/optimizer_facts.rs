use crate::schema::{PhysicalDescriptorFingerprint, TableObjectId};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

pub const ARROW_STATS_TAG_INT8: u8 = 0x01;
pub const ARROW_STATS_TAG_INT16: u8 = 0x02;
pub const ARROW_STATS_TAG_INT32: u8 = 0x03;
pub const ARROW_STATS_TAG_INT64: u8 = 0x04;
pub const ARROW_STATS_TAG_UINT8: u8 = 0x05;
pub const ARROW_STATS_TAG_UINT16: u8 = 0x06;
pub const ARROW_STATS_TAG_UINT32: u8 = 0x07;
pub const ARROW_STATS_TAG_UINT64: u8 = 0x08;
pub const ARROW_STATS_TAG_OPAQUE_UNSUPPORTED: u8 = 0x0E;
pub const ARROW_STATS_TAG_UTF8: u8 = 0x10;
pub const ARROW_STATS_TAG_LARGE_UTF8: u8 = 0x11;
pub const ARROW_STATS_TAG_BINARY: u8 = 0x12;
pub const ARROW_STATS_TAG_LARGE_BINARY: u8 = 0x13;
pub const ARROW_STATS_TAG_TIMESTAMP_MILLISECOND: u8 = 0x20;
pub const ARROW_STATS_TAG_TIMESTAMP_MICROSECOND: u8 = 0x21;
pub const ARROW_STATS_TAG_TIMESTAMP_NANOSECOND: u8 = 0x22;
pub const ARROW_STATS_TAG_DATE32: u8 = 0x30;
pub const ARROW_STATS_TAG_DATE64: u8 = 0x31;
pub const ARROW_STATS_TAG_BOOLEAN: u8 = 0x40;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableOptimizerFacts {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub producer_epoch: u64,
	#[serde(default)]
	pub row_upper_bound: Option<u64>,
	#[serde(default)]
	pub column_ndv: Vec<ColumnNdvFact>,
	#[serde(default)]
	pub column_range: Vec<ColumnRangeFact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnNdvFact {
	pub column_ordinal: u16,
	pub distinct: NdvFact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRangeFact {
	pub column_ordinal: u16,
	pub arrow_type_tag: u8,
	pub null_count: u64,
	pub non_null_count: u64,
	pub min: Option<Vec<u8>>,
	pub max: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NdvFact {
	Exact { count: u64 },
	Approx { estimate: u64, rel_err_ppm: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyTableOptimizerFacts {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub producer_epoch: u64,
	#[serde(default)]
	pub column_ndv: Vec<ColumnNdvFact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TableOptimizerFactsWithoutRowBound {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub producer_epoch: u64,
	#[serde(default)]
	pub column_ndv: Vec<ColumnNdvFact>,
	#[serde(default)]
	pub column_range: Vec<ColumnRangeFact>,
}

impl From<TableOptimizerFactsWithoutRowBound> for TableOptimizerFacts {
	fn from(facts: TableOptimizerFactsWithoutRowBound) -> Self {
		Self {
			table_object_id: facts.table_object_id,
			schema_generation: facts.schema_generation,
			physical_descriptor_fingerprint: facts.physical_descriptor_fingerprint,
			producer_epoch: facts.producer_epoch,
			row_upper_bound: None,
			column_ndv: facts.column_ndv,
			column_range: facts.column_range,
		}
	}
}

impl From<LegacyTableOptimizerFacts> for TableOptimizerFacts {
	fn from(facts: LegacyTableOptimizerFacts) -> Self {
		Self {
			table_object_id: facts.table_object_id,
			schema_generation: facts.schema_generation,
			physical_descriptor_fingerprint: facts.physical_descriptor_fingerprint,
			producer_epoch: facts.producer_epoch,
			row_upper_bound: None,
			column_ndv: facts.column_ndv,
			column_range: Vec::new(),
		}
	}
}

pub fn range_fact_tag_supported(tag: u8) -> bool {
	matches!(
		tag,
		ARROW_STATS_TAG_INT8
			| ARROW_STATS_TAG_INT16
			| ARROW_STATS_TAG_INT32
			| ARROW_STATS_TAG_INT64
			| ARROW_STATS_TAG_UINT8
			| ARROW_STATS_TAG_UINT16
			| ARROW_STATS_TAG_UINT32
			| ARROW_STATS_TAG_UINT64
			| ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
			| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
			| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND
			| ARROW_STATS_TAG_DATE32
			| ARROW_STATS_TAG_DATE64
	)
}

pub fn encode_signed_optimizer_fact_scalar(
	value: i64,
	byte_width: usize,
) -> std::io::Result<Vec<u8>> {
	if !matches!(byte_width, 1 | 2 | 4 | 8) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"signed optimizer fact scalar byte width must be one of 1, 2, 4, 8",
		));
	}
	let bits = byte_width * 8;
	let min = -(1_i128 << (bits - 1));
	let max = (1_i128 << (bits - 1)) - 1;
	let value_i128 = i128::from(value);
	if value_i128 < min || value_i128 > max {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"signed optimizer fact scalar exceeds requested byte width",
		));
	}
	let sign_bit = 1_u64 << (bits - 1);
	let mask = if bits == 64 {
		u64::MAX
	} else {
		(1_u64 << bits) - 1
	};
	let biased = ((value as u64) & mask) ^ sign_bit;
	Ok(biased.to_be_bytes()[8 - byte_width..].to_vec())
}

pub fn decode_signed_optimizer_fact_scalar(bytes: &[u8]) -> Option<i64> {
	let byte_width = bytes.len();
	if !matches!(byte_width, 1 | 2 | 4 | 8) {
		return None;
	}
	let bits = byte_width * 8;
	let mut raw_bytes = [0_u8; 8];
	raw_bytes[8 - byte_width..].copy_from_slice(bytes);
	let biased = u64::from_be_bytes(raw_bytes);
	let sign_bit = 1_u64 << (bits - 1);
	let raw = biased ^ sign_bit;
	let sign_extended = if bits == 64 {
		raw
	} else if raw & sign_bit != 0 {
		raw | (!0_u64 << bits)
	} else {
		raw
	};
	Some(sign_extended as i64)
}

pub fn encode_unsigned_optimizer_fact_scalar(
	value: u64,
	byte_width: usize,
) -> std::io::Result<Vec<u8>> {
	if !matches!(byte_width, 1 | 2 | 4 | 8) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"unsigned optimizer fact scalar byte width must be one of 1, 2, 4, 8",
		));
	}
	let max = if byte_width == 8 {
		u64::MAX
	} else {
		(1_u64 << (byte_width * 8)) - 1
	};
	if value > max {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"unsigned optimizer fact scalar exceeds requested byte width",
		));
	}
	Ok(value.to_be_bytes()[8 - byte_width..].to_vec())
}

pub fn encode_table_optimizer_facts(facts: &TableOptimizerFacts) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(facts).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode table optimizer facts: {err}"),
		)
	})
}

pub fn decode_table_optimizer_facts(bytes: &[u8]) -> std::io::Result<TableOptimizerFacts> {
	match postcard::from_bytes(bytes) {
		Ok(facts) => Ok(facts),
		Err(new_err) => match postcard::from_bytes::<TableOptimizerFactsWithoutRowBound>(bytes) {
			Ok(facts) => Ok(TableOptimizerFacts::from(facts)),
			Err(without_row_err) => postcard::from_bytes::<LegacyTableOptimizerFacts>(bytes)
				.map(TableOptimizerFacts::from)
				.map_err(|legacy_err| {
					Error::new(
						ErrorKind::InvalidData,
						format!(
							"failed to decode table optimizer facts: {new_err}; without-row-bound decode also failed: {without_row_err}; legacy decode also failed: {legacy_err}"
						),
					)
				}),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn table_id() -> TableObjectId {
		TableObjectId::new([0x42; 16])
	}

	fn fingerprint() -> PhysicalDescriptorFingerprint {
		PhysicalDescriptorFingerprint::from_bytes([0x51; 32])
	}

	#[test]
	fn signed_optimizer_fact_scalar_round_trips_supported_widths() {
		for (value, width) in [
			(i8::MIN as i64, 1),
			(-1, 1),
			(0, 1),
			(i8::MAX as i64, 1),
			(i32::MIN as i64, 4),
			(42, 4),
			(i32::MAX as i64, 4),
			(i64::MIN, 8),
			(i64::MAX, 8),
		] {
			let encoded = encode_signed_optimizer_fact_scalar(value, width).unwrap();
			assert_eq!(decode_signed_optimizer_fact_scalar(&encoded), Some(value));
		}
	}

	#[test]
	fn table_optimizer_facts_legacy_payload_decodes_empty_range() {
		let legacy = LegacyTableOptimizerFacts {
			table_object_id: table_id(),
			schema_generation: 7,
			physical_descriptor_fingerprint: fingerprint(),
			producer_epoch: 9,
			column_ndv: vec![ColumnNdvFact {
				column_ordinal: 1,
				distinct: NdvFact::Exact { count: 3 },
			}],
		};
		let bytes = postcard::to_allocvec(&legacy).expect("encode legacy facts");

		let decoded = decode_table_optimizer_facts(&bytes).expect("decode legacy facts");
		assert_eq!(decoded.table_object_id, legacy.table_object_id);
		assert_eq!(decoded.schema_generation, legacy.schema_generation);
		assert_eq!(decoded.row_upper_bound, None);
		assert_eq!(decoded.column_ndv, legacy.column_ndv);
		assert!(decoded.column_range.is_empty());
	}

	#[test]
	fn table_optimizer_facts_round_trips_range_facts() {
		let facts = TableOptimizerFacts {
			table_object_id: table_id(),
			schema_generation: 7,
			physical_descriptor_fingerprint: fingerprint(),
			producer_epoch: 9,
			row_upper_bound: Some(12),
			column_ndv: Vec::new(),
			column_range: vec![ColumnRangeFact {
				column_ordinal: 0,
				arrow_type_tag: ARROW_STATS_TAG_INT64,
				null_count: 1,
				non_null_count: 2,
				min: Some(encode_signed_optimizer_fact_scalar(-5, 8).unwrap()),
				max: Some(encode_signed_optimizer_fact_scalar(7, 8).unwrap()),
			}],
		};

		let bytes = encode_table_optimizer_facts(&facts).expect("encode facts");
		assert_eq!(decode_table_optimizer_facts(&bytes).unwrap(), facts);
	}

	#[test]
	fn table_optimizer_facts_current_payload_decodes_row_bound_absent() {
		let without_row_bound = TableOptimizerFactsWithoutRowBound {
			table_object_id: table_id(),
			schema_generation: 7,
			physical_descriptor_fingerprint: fingerprint(),
			producer_epoch: 9,
			column_ndv: Vec::new(),
			column_range: vec![ColumnRangeFact {
				column_ordinal: 0,
				arrow_type_tag: ARROW_STATS_TAG_INT64,
				null_count: 1,
				non_null_count: 2,
				min: Some(encode_signed_optimizer_fact_scalar(-5, 8).unwrap()),
				max: Some(encode_signed_optimizer_fact_scalar(7, 8).unwrap()),
			}],
		};
		let bytes = postcard::to_allocvec(&without_row_bound).expect("encode current facts");

		let decoded = decode_table_optimizer_facts(&bytes).expect("decode current facts");
		assert_eq!(decoded.table_object_id, without_row_bound.table_object_id);
		assert_eq!(
			decoded.schema_generation,
			without_row_bound.schema_generation
		);
		assert_eq!(decoded.row_upper_bound, None);
		assert_eq!(decoded.column_range, without_row_bound.column_range);
	}
}
