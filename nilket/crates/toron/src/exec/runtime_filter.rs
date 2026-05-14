use std::collections::HashSet;

use arrow::array::{
	Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array,
	UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use twox_hash::XxHash3_64;
use wendelon::schema::{
	PhysicalDescriptorFingerprint, RuntimeFilterDescriptor, RuntimeFilterKeyType,
};

use crate::distributed::task::RuntimeFilterFragment;
use crate::error::ExecutionError;
use crate::ir::{ColumnOrdinal, ExecutionExpr};

pub(crate) const RUNTIME_FILTER_KEY_ENCODING_VERSION: u32 = 1;
const RUNTIME_FILTER_HASH_SEED: u64 = 0x6e69_6c6b_6574_0006;
const RUNTIME_FILTER_HASH_SEED_2: u64 = 0x9e37_79b9_7f4a_7c15;
const TARGET_FP_RATE_BP: u16 = 100;
const MIN_FILTER_BYTES: usize = 16 * 1024;
const MAX_FILTER_BYTES: usize = 1024 * 1024;

pub(crate) fn build_runtime_filter_fragment(
	batches: &[RecordBatch],
	key: &[ExecutionExpr],
) -> Result<Option<RuntimeFilterFragment>, ExecutionError> {
	let Some((ordinal, key_type)) = single_supported_key(key) else {
		return Ok(None);
	};
	let non_null_count = non_null_key_count(batches, ordinal)?;
	if non_null_count > cap_distinct_threshold() {
		let sizing = bloom_sizing(non_null_count);
		let mut payload = vec![0u8; sizing.byte_budget];
		for batch in batches {
			let array = runtime_filter_key_array(batch, ordinal)?;
			for row in 0..batch.num_rows() {
				if let Some(encoded) = encode_runtime_filter_key(array, row)? {
					insert_key(&mut payload, sizing.bit_count, sizing.hash_count, &encoded);
				}
			}
		}
		return Ok(Some(RuntimeFilterFragment {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![key_type],
			hash_seed: RUNTIME_FILTER_HASH_SEED,
			bit_count: sizing.bit_count,
			hash_count: sizing.hash_count,
			byte_budget: u32::try_from(sizing.byte_budget).expect("filter byte budget fits u32"),
			target_fp_rate_bp: TARGET_FP_RATE_BP,
			payload,
		}));
	}

	let mut keys = HashSet::new();
	for batch in batches {
		let array = runtime_filter_key_array(batch, ordinal)?;
		for row in 0..batch.num_rows() {
			if let Some(encoded) = encode_runtime_filter_key(array, row)? {
				keys.insert(encoded);
			}
		}
	}

	let sizing = bloom_sizing(keys.len());
	let mut payload = vec![0u8; sizing.byte_budget];
	for encoded in &keys {
		insert_key(&mut payload, sizing.bit_count, sizing.hash_count, encoded);
	}

	Ok(Some(RuntimeFilterFragment {
		key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![key_type],
		hash_seed: RUNTIME_FILTER_HASH_SEED,
		bit_count: sizing.bit_count,
		hash_count: sizing.hash_count,
		byte_budget: u32::try_from(sizing.byte_budget).expect("filter byte budget fits u32"),
		target_fp_rate_bp: TARGET_FP_RATE_BP,
		payload,
	}))
}

fn runtime_filter_key_array(
	batch: &RecordBatch,
	ordinal: ColumnOrdinal,
) -> Result<&ArrayRef, ExecutionError> {
	batch
		.columns()
		.get(ordinal.0 as usize)
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("runtime filter key ordinal {} is out of bounds", ordinal.0),
		})
}

fn non_null_key_count(
	batches: &[RecordBatch],
	ordinal: ColumnOrdinal,
) -> Result<usize, ExecutionError> {
	let mut count = 0usize;
	for batch in batches {
		let array = runtime_filter_key_array(batch, ordinal)?;
		count = count.saturating_add(batch.num_rows().saturating_sub(array.null_count()));
	}
	Ok(count)
}

#[cfg(test)]
pub(crate) fn runtime_filter_might_contain(
	descriptor: &RuntimeFilterDescriptor,
	array: &ArrayRef,
	row: usize,
) -> Result<bool, ExecutionError> {
	let Some(encoded) = encode_runtime_filter_key(array, row)? else {
		return Ok(false);
	};
	Ok(might_contain(
		&descriptor.payload,
		descriptor.bit_count,
		descriptor.hash_count,
		descriptor.hash_seed,
		&encoded,
	))
}

pub(crate) fn descriptor_from_fragment(
	fragment: &RuntimeFilterFragment,
	probe_key_columns: Vec<u32>,
	probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
) -> RuntimeFilterDescriptor {
	RuntimeFilterDescriptor {
		key_encoding_version: fragment.key_encoding_version,
		key_types: fragment.key_types.clone(),
		probe_key_columns,
		hash_seed: fragment.hash_seed,
		bit_count: fragment.bit_count,
		hash_count: fragment.hash_count,
		byte_budget: fragment.byte_budget,
		target_fp_rate_bp: fragment.target_fp_rate_bp,
		payload: fragment.payload.clone(),
		probe_descriptor_fingerprint,
	}
}

pub(crate) fn fragments_compatible(
	left: &RuntimeFilterFragment,
	right: &RuntimeFilterFragment,
) -> bool {
	left.key_encoding_version == right.key_encoding_version
		&& left.key_types == right.key_types
		&& left.hash_seed == right.hash_seed
		&& left.bit_count == right.bit_count
		&& left.hash_count == right.hash_count
		&& left.byte_budget == right.byte_budget
		&& left.target_fp_rate_bp == right.target_fp_rate_bp
		&& left.payload.len() == right.payload.len()
}

pub(crate) fn merge_fragments(
	target: &mut RuntimeFilterFragment,
	fragment: &RuntimeFilterFragment,
) -> bool {
	if !fragments_compatible(target, fragment) {
		return false;
	}
	for (target_byte, fragment_byte) in target.payload.iter_mut().zip(&fragment.payload) {
		*target_byte |= *fragment_byte;
	}
	true
}

pub(crate) fn runtime_filter_key_type(data_type: &DataType) -> Option<RuntimeFilterKeyType> {
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

pub(crate) fn single_supported_key(
	key: &[ExecutionExpr],
) -> Option<(ColumnOrdinal, RuntimeFilterKeyType)> {
	let [ExecutionExpr::Column(column)] = key else {
		return None;
	};
	runtime_filter_key_type(&column.data_type).map(|key_type| (column.ordinal, key_type))
}

struct BloomSizing {
	bit_count: u32,
	hash_count: u8,
	byte_budget: usize,
}

fn bloom_sizing(distinct_keys: usize) -> BloomSizing {
	let optimal_bits = if distinct_keys == 0 {
		MIN_FILTER_BYTES * 8
	} else {
		((distinct_keys as f64) * 9.585_058).ceil() as usize
	};
	let byte_budget = optimal_bits
		.div_ceil(8)
		.clamp(MIN_FILTER_BYTES, MAX_FILTER_BYTES);
	let bit_count = u32::try_from(byte_budget * 8).expect("filter bit count fits u32");
	let hash_count = if distinct_keys == 0 {
		1
	} else {
		let hash_sizing_bits = optimal_bits.min(byte_budget * 8);
		let optimal =
			((hash_sizing_bits as f64 / distinct_keys as f64) * std::f64::consts::LN_2).round();
		(optimal as u8).max(1)
	};
	BloomSizing {
		bit_count,
		hash_count,
		byte_budget,
	}
}

fn cap_distinct_threshold() -> usize {
	((MAX_FILTER_BYTES * 8) as f64 / 9.585_058).floor() as usize
}

fn insert_key(payload: &mut [u8], bit_count: u32, hash_count: u8, encoded: &[u8]) {
	for bit in bloom_bits(RUNTIME_FILTER_HASH_SEED, bit_count, hash_count, encoded) {
		let byte = (bit / 8) as usize;
		let mask = 1u8 << (bit % 8);
		payload[byte] |= mask;
	}
}

#[cfg(test)]
fn might_contain(
	payload: &[u8],
	bit_count: u32,
	hash_count: u8,
	hash_seed: u64,
	encoded: &[u8],
) -> bool {
	if bit_count == 0 || hash_count == 0 || payload.is_empty() {
		return true;
	}
	bloom_bits(hash_seed, bit_count, hash_count, encoded).all(|bit| {
		let byte = (bit / 8) as usize;
		let mask = 1u8 << (bit % 8);
		payload
			.get(byte)
			.map(|value| value & mask != 0)
			.unwrap_or(true)
	})
}

fn bloom_bits(
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

fn encode_runtime_filter_key(
	array: &ArrayRef,
	row: usize,
) -> Result<Option<Vec<u8>>, ExecutionError> {
	if array.is_null(row) {
		return Ok(None);
	}

	let mut encoded = Vec::new();
	match array.data_type() {
		DataType::Int8 => {
			let values = array.as_any().downcast_ref::<Int8Array>().expect("int8");
			encoded.push(2);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int16 => {
			let values = array.as_any().downcast_ref::<Int16Array>().expect("int16");
			encoded.push(3);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int32 => {
			let values = array.as_any().downcast_ref::<Int32Array>().expect("int32");
			encoded.push(4);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Int64 => {
			let values = array.as_any().downcast_ref::<Int64Array>().expect("int64");
			encoded.push(5);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt8 => {
			let values = array.as_any().downcast_ref::<UInt8Array>().expect("uint8");
			encoded.push(6);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt16 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt16Array>()
				.expect("uint16");
			encoded.push(7);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt32 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt32Array>()
				.expect("uint32");
			encoded.push(8);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::UInt64 => {
			let values = array
				.as_any()
				.downcast_ref::<UInt64Array>()
				.expect("uint64");
			encoded.push(9);
			encoded.extend_from_slice(&values.value(row).to_le_bytes());
		}
		DataType::Utf8 => {
			let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
			let value = values.value(row).as_bytes();
			encoded.push(12);
			encoded.extend_from_slice(
				&u32::try_from(value.len())
					.map_err(|_| ExecutionError::InvalidExpression {
						message: "runtime filter string key is too large to encode".to_string(),
					})?
					.to_le_bytes(),
			);
			encoded.extend_from_slice(value);
		}
		data_type => {
			return Err(ExecutionError::InvalidExpression {
				message: format!("unsupported runtime filter key type: {data_type:?}"),
			});
		}
	}
	Ok(Some(encoded))
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::array::{Float64Array, Int32Array, Int64Array};
	use arrow::datatypes::{Field, Schema, TimeUnit};
	use std::sync::Arc;
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

	#[test]
	fn runtime_filter_generation_preserves_type_tag() {
		let int32 = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
		let int64 = Arc::new(Int64Array::from(vec![1])) as ArrayRef;

		assert_ne!(
			encode_runtime_filter_key(&int32, 0).unwrap(),
			encode_runtime_filter_key(&int64, 0).unwrap()
		);
	}

	#[test]
	fn runtime_filter_generation_no_false_negatives() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int32,
			nullable: false,
			pbc_eligible: true,
		})];
		let fragment = build_runtime_filter_fragment(&[batch.clone()], &key)
			.unwrap()
			.expect("fragment");
		assert_eq!(fragment.byte_budget, 16 * 1024);
		assert_eq!(fragment.hash_count, 7);
		let descriptor =
			descriptor_from_fragment(&fragment, vec![0], runtime_filter_test_fingerprint());

		for row in 0..batch.num_rows() {
			assert!(runtime_filter_might_contain(&descriptor, batch.column(0), row).unwrap());
		}
	}

	#[test]
	fn runtime_filter_generation_fp_rate_within_target() {
		let build_values = (0_i64..4096).collect::<Vec<_>>();
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let build = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(build_values)) as ArrayRef],
		)
		.unwrap();
		let probes = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from_iter_values(100_000_i64..110_000)) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: true,
		})];
		let fragment = build_runtime_filter_fragment(&[build], &key)
			.unwrap()
			.expect("fragment");
		let descriptor =
			descriptor_from_fragment(&fragment, vec![0], runtime_filter_test_fingerprint());
		let false_positives = (0..probes.num_rows())
			.filter(|row| {
				runtime_filter_might_contain(&descriptor, probes.column(0), *row)
					.expect("membership")
			})
			.count();

		assert!(
			false_positives <= 150,
			"false positive count {false_positives} exceeded 1.5% noise bound"
		);
	}

	#[test]
	fn runtime_filter_fragment_merge_compatibility() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let left = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
		)
		.unwrap();
		let right = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: true,
		})];
		let mut merged = build_runtime_filter_fragment(std::slice::from_ref(&left), &key)
			.unwrap()
			.expect("left fragment");
		let right_fragment = build_runtime_filter_fragment(std::slice::from_ref(&right), &key)
			.unwrap()
			.expect("right fragment");

		assert!(fragments_compatible(&merged, &right_fragment));
		assert!(merge_fragments(&mut merged, &right_fragment));
		let descriptor =
			descriptor_from_fragment(&merged, vec![0], runtime_filter_test_fingerprint());

		for batch in [&left, &right] {
			for row in 0..batch.num_rows() {
				assert!(runtime_filter_might_contain(&descriptor, batch.column(0), row).unwrap());
			}
		}

		let mut incompatible = right_fragment;
		incompatible.hash_count = incompatible.hash_count.saturating_add(1);
		assert!(!merge_fragments(&mut merged, &incompatible));
	}

	#[test]
	fn runtime_filter_generation_eligibility_excludes_floats() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"id",
			DataType::Float64,
			false,
		)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Float64,
			nullable: false,
			pbc_eligible: false,
		})];

		assert!(
			build_runtime_filter_fragment(&[batch], &key)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn runtime_filter_generation_eligibility_excludes_composites() {
		let key = [
			ExecutionExpr::Column(crate::ir::ResolvedColumn {
				name: crate::ir::ColumnName("id".to_string()),
				ordinal: ColumnOrdinal(0),
				data_type: DataType::Int32,
				nullable: false,
				pbc_eligible: true,
			}),
			ExecutionExpr::Column(crate::ir::ResolvedColumn {
				name: crate::ir::ColumnName("tenant".to_string()),
				ordinal: ColumnOrdinal(1),
				data_type: DataType::Int32,
				nullable: false,
				pbc_eligible: true,
			}),
		];

		assert!(build_runtime_filter_fragment(&[], &key).unwrap().is_none());
	}

	#[test]
	fn runtime_filter_generation_eligibility_excludes_unsupported_types() {
		for data_type in [
			DataType::Date32,
			DataType::Date64,
			DataType::Timestamp(TimeUnit::Microsecond, None),
			DataType::Decimal128(10, 2),
		] {
			let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
				name: crate::ir::ColumnName("id".to_string()),
				ordinal: ColumnOrdinal(0),
				data_type,
				nullable: false,
				pbc_eligible: false,
			})];

			assert!(build_runtime_filter_fragment(&[], &key).unwrap().is_none());
		}
	}

	fn runtime_filter_test_fingerprint() -> PhysicalDescriptorFingerprint {
		let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([6; 16]),
			1,
			"default",
			"runtime_filter_test",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
		.physical_descriptor_fingerprint
	}
}
