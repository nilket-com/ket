use std::fmt;

use arrow::array::{
	Array, BooleanArray, BooleanBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
	StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use twox_hash::XxHash3_64;

use crate::schema::PhysicalDescriptorFingerprint;

pub const RUNTIME_FILTER_KEY_ENCODING_VERSION: u32 = 1;
pub const RUNTIME_FILTER_HASH_SEED: u64 = 0x6e69_6c6b_6574_0006;
const RUNTIME_FILTER_HASH_SEED_2: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeFilterKeyType {
	Int8,
	Int16,
	Int32,
	Int64,
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Utf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFilterDescriptor {
	pub key_encoding_version: u32,
	pub key_types: Vec<RuntimeFilterKeyType>,
	pub probe_key_columns: Vec<u32>,
	pub hash_seed: u64,
	pub bit_count: u32,
	pub hash_count: u8,
	pub byte_budget: u32,
	pub target_fp_rate_bp: u16,
	pub payload: Vec<u8>,
	pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuntimeFilterDropReason {
	DecodeFailed,
	FingerprintMismatch,
	UnsupportedShape,
	MaterializationIncompatible,
}

impl RuntimeFilterDropReason {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::DecodeFailed => "decode_failed",
			Self::FingerprintMismatch => "fingerprint_mismatch",
			Self::UnsupportedShape => "unsupported_shape",
			Self::MaterializationIncompatible => "materialization_incompatible",
		}
	}
}

impl fmt::Display for RuntimeFilterDropReason {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

pub fn runtime_filter_key_type(data_type: &DataType) -> Option<RuntimeFilterKeyType> {
	match data_type {
		DataType::Int8 => Some(RuntimeFilterKeyType::Int8),
		DataType::Int16 => Some(RuntimeFilterKeyType::Int16),
		DataType::Int32 => Some(RuntimeFilterKeyType::Int32),
		DataType::Int64 => Some(RuntimeFilterKeyType::Int64),
		DataType::UInt8 => Some(RuntimeFilterKeyType::UInt8),
		DataType::UInt16 => Some(RuntimeFilterKeyType::UInt16),
		DataType::UInt32 => Some(RuntimeFilterKeyType::UInt32),
		DataType::UInt64 => Some(RuntimeFilterKeyType::UInt64),
		DataType::Utf8 => Some(RuntimeFilterKeyType::Utf8),
		_ => None,
	}
}

pub fn validate_runtime_filter_descriptor(
	descriptor: &RuntimeFilterDescriptor,
	schema: &Schema,
	expected_fingerprint: PhysicalDescriptorFingerprint,
) -> Result<(), RuntimeFilterDropReason> {
	if descriptor.probe_descriptor_fingerprint != expected_fingerprint {
		return Err(RuntimeFilterDropReason::FingerprintMismatch);
	}
	validate_runtime_filter_payload_shape(descriptor)?;
	for (ordinal, key_type) in descriptor
		.probe_key_columns
		.iter()
		.zip(descriptor.key_types.iter())
	{
		let field = schema
			.fields()
			.get(*ordinal as usize)
			.ok_or(RuntimeFilterDropReason::MaterializationIncompatible)?;
		if runtime_filter_key_type(field.data_type()) != Some(*key_type) {
			return Err(RuntimeFilterDropReason::UnsupportedShape);
		}
	}
	Ok(())
}

pub fn validate_runtime_filter_payload_shape(
	descriptor: &RuntimeFilterDescriptor,
) -> Result<(), RuntimeFilterDropReason> {
	if descriptor.key_encoding_version != RUNTIME_FILTER_KEY_ENCODING_VERSION
		|| descriptor.key_types.is_empty()
		|| descriptor.key_types.len() != descriptor.probe_key_columns.len()
		|| descriptor.bit_count == 0
		|| descriptor.hash_count == 0
		|| descriptor.byte_budget == 0
	{
		return Err(RuntimeFilterDropReason::UnsupportedShape);
	}
	let byte_budget = usize::try_from(descriptor.byte_budget)
		.map_err(|_| RuntimeFilterDropReason::UnsupportedShape)?;
	if descriptor.payload.len() != byte_budget {
		return Err(RuntimeFilterDropReason::UnsupportedShape);
	}
	let payload_bits = byte_budget
		.checked_mul(8)
		.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
	if descriptor.bit_count as usize > payload_bits {
		return Err(RuntimeFilterDropReason::UnsupportedShape);
	}
	Ok(())
}

pub fn insert_runtime_filter_encoded_key(
	payload: &mut [u8],
	bit_count: u32,
	hash_count: u8,
	hash_seed: u64,
	encoded: &[u8],
) {
	for bit in runtime_filter_bloom_bits(hash_seed, bit_count, hash_count, encoded) {
		let byte = (bit / 8) as usize;
		let mask = 1u8 << (bit % 8);
		if let Some(value) = payload.get_mut(byte) {
			*value |= mask;
		}
	}
}

pub fn runtime_filter_might_contain_encoded_key(
	descriptor: &RuntimeFilterDescriptor,
	encoded: &[u8],
) -> bool {
	if descriptor.bit_count == 0 || descriptor.hash_count == 0 || descriptor.payload.is_empty() {
		return true;
	}
	runtime_filter_bloom_bits(
		descriptor.hash_seed,
		descriptor.bit_count,
		descriptor.hash_count,
		encoded,
	)
	.all(|bit| {
		let byte = (bit / 8) as usize;
		let mask = 1u8 << (bit % 8);
		descriptor
			.payload
			.get(byte)
			.map(|value| value & mask != 0)
			.unwrap_or(true)
	})
}

pub fn runtime_filter_might_contain(
	descriptor: &RuntimeFilterDescriptor,
	array: &dyn Array,
	row: usize,
) -> Result<bool, RuntimeFilterDropReason> {
	let Some(encoded) = encode_runtime_filter_key(array, row)? else {
		return Ok(false);
	};
	Ok(runtime_filter_might_contain_encoded_key(
		descriptor, &encoded,
	))
}

pub fn build_runtime_filter_mask_for_batch(
	batch: &RecordBatch,
	descriptor: &RuntimeFilterDescriptor,
) -> Result<BooleanArray, RuntimeFilterDropReason> {
	let mut arrays = Vec::with_capacity(descriptor.probe_key_columns.len());
	for ordinal in &descriptor.probe_key_columns {
		let array = batch
			.columns()
			.get(*ordinal as usize)
			.ok_or(RuntimeFilterDropReason::MaterializationIncompatible)?;
		arrays.push(array.as_ref());
	}
	build_runtime_filter_mask(descriptor, &arrays)
}

pub fn build_runtime_filter_mask(
	descriptor: &RuntimeFilterDescriptor,
	key_arrays: &[&dyn Array],
) -> Result<BooleanArray, RuntimeFilterDropReason> {
	validate_runtime_filter_payload_shape(descriptor)?;
	if key_arrays.len() != descriptor.key_types.len() || key_arrays.is_empty() {
		return Err(RuntimeFilterDropReason::UnsupportedShape);
	}
	let row_count = key_arrays[0].len();
	if key_arrays.iter().any(|array| array.len() != row_count) {
		return Err(RuntimeFilterDropReason::MaterializationIncompatible);
	}
	let mut builder = BooleanBuilder::with_capacity(row_count);
	for row in 0..row_count {
		let Some(encoded) = encode_runtime_filter_row(key_arrays, row)? else {
			builder.append_value(false);
			continue;
		};
		builder.append_value(runtime_filter_might_contain_encoded_key(
			descriptor, &encoded,
		));
	}
	Ok(builder.finish())
}

pub fn encode_runtime_filter_row(
	key_arrays: &[&dyn Array],
	row: usize,
) -> Result<Option<Vec<u8>>, RuntimeFilterDropReason> {
	if key_arrays.is_empty() {
		return Err(RuntimeFilterDropReason::UnsupportedShape);
	}
	let mut encoded = Vec::new();
	for array in key_arrays {
		let Some(component) = encode_runtime_filter_key(*array, row)? else {
			return Ok(None);
		};
		encoded.extend_from_slice(&component);
	}
	Ok(Some(encoded))
}

pub fn encode_runtime_filter_key(
	array: &dyn Array,
	row: usize,
) -> Result<Option<Vec<u8>>, RuntimeFilterDropReason> {
	if row >= array.len() {
		return Err(RuntimeFilterDropReason::MaterializationIncompatible);
	}
	if array.is_null(row) {
		return Ok(None);
	}

	let mut encoded = Vec::new();
	match array.data_type() {
		DataType::Int8 => {
			let values = array
				.as_any()
				.downcast_ref::<Int8Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(2);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int16 => {
			let values = array
				.as_any()
				.downcast_ref::<Int16Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(3);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int32 => {
			let values = array
				.as_any()
				.downcast_ref::<Int32Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(4);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int64 => {
			let values = array
				.as_any()
				.downcast_ref::<Int64Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(5);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt8 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt8Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(6);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt16 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt16Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(7);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt32 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt32Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(8);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt64 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt64Array>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			encoded.push(9);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Utf8 => {
			let values = array
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or(RuntimeFilterDropReason::UnsupportedShape)?;
			let value = values.value(row).as_bytes();
			encoded.push(12);
			encoded.extend_from_slice(
				&u32::try_from(value.len())
					.map_err(|_| RuntimeFilterDropReason::UnsupportedShape)?
					.to_le_bytes(),
			);
			encoded.extend_from_slice(value);
		}
		_ => return Err(RuntimeFilterDropReason::UnsupportedShape),
	}
	Ok(Some(encoded))
}

fn runtime_filter_bloom_bits(
	hash_seed: u64,
	bit_count: u32,
	hash_count: u8,
	encoded: &[u8],
) -> impl Iterator<Item = u32> {
	let h1 = XxHash3_64::oneshot_with_seed(hash_seed, encoded);
	let mut h2 = XxHash3_64::oneshot_with_seed(hash_seed ^ RUNTIME_FILTER_HASH_SEED_2, encoded);
	if h2 == 0 {
		h2 = RUNTIME_FILTER_HASH_SEED_2;
	}
	(0..u64::from(hash_count)).map(move |idx| {
		let hash = h1.wrapping_add(idx.wrapping_mul(h2));
		(hash % u64::from(bit_count)) as u32
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;

	use arrow::array::{
		ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array,
		UInt16Array, UInt32Array, UInt64Array,
	};
	use arrow::datatypes::{Field, Schema};

	#[test]
	fn runtime_filter_descriptor_round_trips_through_postcard() {
		let descriptor = RuntimeFilterDescriptor {
			key_encoding_version: 1,
			key_types: vec![RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![2],
			hash_seed: 7,
			bit_count: 16,
			hash_count: 2,
			byte_budget: 2,
			target_fp_rate_bp: 100,
			payload: vec![0b1010_0101, 0b0101_1010],
			probe_descriptor_fingerprint: PhysicalDescriptorFingerprint::from_bytes([6; 32]),
		};

		let encoded = postcard::to_allocvec(&descriptor).expect("encode runtime filter");
		let decoded: RuntimeFilterDescriptor =
			postcard::from_bytes(&encoded).expect("decode runtime filter");

		assert_eq!(decoded, descriptor);
	}

	#[test]
	fn runtime_filter_descriptor_preserves_version_field() {
		let descriptor = RuntimeFilterDescriptor {
			key_encoding_version: 42,
			key_types: vec![RuntimeFilterKeyType::Utf8],
			probe_key_columns: vec![1],
			hash_seed: 9,
			bit_count: 8,
			hash_count: 1,
			byte_budget: 1,
			target_fp_rate_bp: 250,
			payload: vec![0xFF],
			probe_descriptor_fingerprint: PhysicalDescriptorFingerprint::from_bytes([7; 32]),
		};

		let encoded = postcard::to_allocvec(&descriptor).expect("encode runtime filter");
		let decoded: RuntimeFilterDescriptor =
			postcard::from_bytes(&encoded).expect("decode runtime filter");

		assert_eq!(decoded.key_encoding_version, 42);
		assert_eq!(decoded, descriptor);
	}

	#[test]
	fn runtime_filter_encoding_preserves_type_tags() {
		let int32 = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
		let int64 = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

		assert_ne!(
			encode_runtime_filter_key(int32.as_ref(), 0).unwrap(),
			encode_runtime_filter_key(int64.as_ref(), 0).unwrap()
		);
	}

	#[test]
	fn runtime_filter_membership_has_no_false_negatives_for_supported_types() {
		let cases: Vec<(RuntimeFilterKeyType, ArrayRef)> = vec![
			(
				RuntimeFilterKeyType::Int8,
				Arc::new(Int8Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::Int16,
				Arc::new(Int16Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::Int32,
				Arc::new(Int32Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::Int64,
				Arc::new(Int64Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::UInt8,
				Arc::new(UInt8Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::UInt16,
				Arc::new(UInt16Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::UInt32,
				Arc::new(UInt32Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::UInt64,
				Arc::new(UInt64Array::from(vec![1, 2])),
			),
			(
				RuntimeFilterKeyType::Utf8,
				Arc::new(StringArray::from(vec!["a", "b"])),
			),
		];

		for (key_type, array) in cases {
			let mut descriptor = descriptor_for_key_type(key_type, vec![0; 16]);
			for row in 0..array.len() {
				let encoded = encode_runtime_filter_key(array.as_ref(), row)
					.unwrap()
					.expect("encoded key");
				insert_runtime_filter_encoded_key(
					&mut descriptor.payload,
					descriptor.bit_count,
					descriptor.hash_count,
					descriptor.hash_seed,
					&encoded,
				);
			}
			for row in 0..array.len() {
				assert!(
					runtime_filter_might_contain(&descriptor, array.as_ref(), row).unwrap(),
					"false negative for {key_type:?} row {row}"
				);
			}
		}
	}

	#[test]
	fn runtime_filter_mask_treats_nulls_as_non_matches() {
		let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(2)])) as ArrayRef;
		let mut descriptor = descriptor_for_key_type(RuntimeFilterKeyType::Int32, vec![0; 16]);
		let encoded = encode_runtime_filter_key(array.as_ref(), 0)
			.unwrap()
			.expect("encoded key");
		insert_runtime_filter_encoded_key(
			&mut descriptor.payload,
			descriptor.bit_count,
			descriptor.hash_count,
			descriptor.hash_seed,
			&encoded,
		);

		let mask = build_runtime_filter_mask(&descriptor, &[array.as_ref()]).unwrap();
		assert_eq!(mask.value(0), true);
		assert_eq!(mask.value(1), false);
	}

	#[test]
	fn runtime_filter_descriptor_validation_checks_shape_and_schema() {
		let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
		let descriptor = descriptor_for_key_type(RuntimeFilterKeyType::Int32, vec![0; 16]);

		validate_runtime_filter_descriptor(
			&descriptor,
			&schema,
			PhysicalDescriptorFingerprint::from_bytes([6; 32]),
		)
		.unwrap();

		let err = validate_runtime_filter_descriptor(
			&descriptor,
			&schema,
			PhysicalDescriptorFingerprint::from_bytes([7; 32]),
		)
		.unwrap_err();
		assert_eq!(err, RuntimeFilterDropReason::FingerprintMismatch);
	}

	fn descriptor_for_key_type(
		key_type: RuntimeFilterKeyType,
		payload: Vec<u8>,
	) -> RuntimeFilterDescriptor {
		RuntimeFilterDescriptor {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![key_type],
			probe_key_columns: vec![0],
			hash_seed: RUNTIME_FILTER_HASH_SEED,
			bit_count: u32::try_from(payload.len() * 8).unwrap(),
			hash_count: 3,
			byte_budget: u32::try_from(payload.len()).unwrap(),
			target_fp_rate_bp: 100,
			payload,
			probe_descriptor_fingerprint: PhysicalDescriptorFingerprint::from_bytes([6; 32]),
		}
	}
}
