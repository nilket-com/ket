use std::collections::HashSet;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use wendelon::schema::runtime_filter_might_contain as schema_runtime_filter_might_contain;
use wendelon::schema::{
	PhysicalDescriptorFingerprint, RUNTIME_FILTER_HASH_SEED, RUNTIME_FILTER_KEY_ENCODING_VERSION,
	RuntimeFilterBloom, RuntimeFilterDescriptor, RuntimeFilterDropReason, RuntimeFilterKeyType,
	RuntimeFilterKind, encode_runtime_filter_key, insert_runtime_filter_encoded_key,
	insert_runtime_filter_exact_set_into_bloom, runtime_filter_exact_set_from_encoded_keys,
	runtime_filter_exact_set_keys, runtime_filter_key_type as schema_runtime_filter_key_type,
};

use crate::distributed::task::RuntimeFilterFragment;
use crate::error::ExecutionError;
use crate::ir::{ColumnOrdinal, ExecutionExpr};

const TARGET_FP_RATE_BP: u16 = 100;
const MIN_FILTER_BYTES: usize = 16 * 1024;
const MAX_FILTER_BYTES: usize = 1024 * 1024;
const EXACT_FILTER_MAX_BYTES: usize = MIN_FILTER_BYTES;

pub(crate) fn build_runtime_filter_fragment(
	batches: &[RecordBatch],
	key: &[ExecutionExpr],
) -> Result<Option<RuntimeFilterFragment>, ExecutionError> {
	let Some((ordinal, key_type)) = single_supported_key(key) else {
		return Ok(None);
	};
	let non_null_count = non_null_key_count(batches, ordinal)?;
	let exact_byte_budget =
		u32::try_from(EXACT_FILTER_MAX_BYTES).expect("exact filter byte budget fits u32");
	let mut exact_keys = HashSet::new();
	let mut exact_payload_bytes = 0usize;
	let mut exact_overflow = false;

	for batch in batches {
		let array = runtime_filter_key_array(batch, ordinal)?;
		for row in 0..batch.num_rows() {
			if let Some(encoded) =
				encode_runtime_filter_key(array.as_ref(), row).map_err(runtime_filter_error)?
			{
				if exact_keys.insert(encoded.clone()) {
					exact_payload_bytes = exact_payload_bytes
						.saturating_add(exact_payload_len(&encoded).map_err(runtime_filter_error)?);
					if exact_payload_bytes > EXACT_FILTER_MAX_BYTES {
						exact_overflow = true;
						break;
					}
				}
			}
		}
		if exact_overflow {
			break;
		}
	}

	if !exact_overflow {
		let exact_set = runtime_filter_exact_set_from_encoded_keys(
			exact_keys.into_iter().collect(),
			exact_byte_budget,
		)
		.map_err(runtime_filter_error)?;
		return Ok(Some(RuntimeFilterFragment {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![key_type],
			kind: RuntimeFilterKind::ExactSet(exact_set),
		}));
	}

	build_bloom_runtime_filter_fragment(batches, ordinal, key_type, non_null_count)
}

fn build_bloom_runtime_filter_fragment(
	batches: &[RecordBatch],
	ordinal: ColumnOrdinal,
	key_type: RuntimeFilterKeyType,
	non_null_count: usize,
) -> Result<Option<RuntimeFilterFragment>, ExecutionError> {
	if non_null_count > cap_distinct_threshold() {
		let sizing = bloom_sizing(non_null_count);
		let mut payload = vec![0u8; sizing.byte_budget];
		for batch in batches {
			let array = runtime_filter_key_array(batch, ordinal)?;
			for row in 0..batch.num_rows() {
				if let Some(encoded) =
					encode_runtime_filter_key(array.as_ref(), row).map_err(runtime_filter_error)?
				{
					insert_key(&mut payload, sizing.bit_count, sizing.hash_count, &encoded);
				}
			}
		}
		return Ok(Some(RuntimeFilterFragment {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![key_type],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: RUNTIME_FILTER_HASH_SEED,
				bit_count: sizing.bit_count,
				hash_count: sizing.hash_count,
				byte_budget: u32::try_from(sizing.byte_budget)
					.expect("filter byte budget fits u32"),
				target_fp_rate_bp: TARGET_FP_RATE_BP,
				payload,
			}),
		}));
	}

	let mut keys = HashSet::new();
	for batch in batches {
		let array = runtime_filter_key_array(batch, ordinal)?;
		for row in 0..batch.num_rows() {
			if let Some(encoded) =
				encode_runtime_filter_key(array.as_ref(), row).map_err(runtime_filter_error)?
			{
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
		kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
			hash_seed: RUNTIME_FILTER_HASH_SEED,
			bit_count: sizing.bit_count,
			hash_count: sizing.hash_count,
			byte_budget: u32::try_from(sizing.byte_budget).expect("filter byte budget fits u32"),
			target_fp_rate_bp: TARGET_FP_RATE_BP,
			payload,
		}),
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
	schema_runtime_filter_might_contain(descriptor, array.as_ref(), row)
		.map_err(runtime_filter_error)
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
		kind: fragment.kind.clone(),
		probe_descriptor_fingerprint,
	}
}

/// Returns true iff `left` and `right` can be unified by `merge_fragments`.
///
/// Same-kind pairs require parameter compatibility (Blooms must share
/// hash/bit/budget shape; ExactSets must share `byte_budget`). Mixed-kind
/// pairs (Bloom + ExactSet, in either order) are always reported compatible
/// — the merge unifies them by lowering the ExactSet keys into the Bloom
/// payload, producing a Bloom output per the contract's
/// "Bloom in → Bloom out" rule.
pub(crate) fn fragments_compatible(
	left: &RuntimeFilterFragment,
	right: &RuntimeFilterFragment,
) -> bool {
	if left.key_encoding_version != right.key_encoding_version || left.key_types != right.key_types
	{
		return false;
	}
	match (&left.kind, &right.kind) {
		(RuntimeFilterKind::Bloom(left), RuntimeFilterKind::Bloom(right)) => {
			blooms_compatible(left, right)
		}
		(RuntimeFilterKind::ExactSet(left), RuntimeFilterKind::ExactSet(right)) => {
			left.byte_budget == right.byte_budget
		}
		(RuntimeFilterKind::Bloom(_), RuntimeFilterKind::ExactSet(_))
		| (RuntimeFilterKind::ExactSet(_), RuntimeFilterKind::Bloom(_)) => true,
	}
}

pub(crate) fn merge_fragments(
	target: &mut RuntimeFilterFragment,
	fragment: &RuntimeFilterFragment,
) -> bool {
	if !fragments_compatible(target, fragment) {
		return false;
	}
	let next_kind = match (&target.kind, &fragment.kind) {
		(RuntimeFilterKind::Bloom(target), RuntimeFilterKind::Bloom(fragment)) => {
			let mut bloom = target.clone();
			merge_bloom_payload(&mut bloom, fragment);
			RuntimeFilterKind::Bloom(bloom)
		}
		(
			RuntimeFilterKind::ExactSet(target_exact),
			RuntimeFilterKind::ExactSet(fragment_exact),
		) => {
			let mut keys = match runtime_filter_exact_set_keys(target_exact) {
				Ok(keys) => keys,
				Err(_) => return false,
			};
			let fragment_keys = match runtime_filter_exact_set_keys(fragment_exact) {
				Ok(keys) => keys,
				Err(_) => return false,
			};
			keys.extend(fragment_keys);
			match runtime_filter_exact_set_from_encoded_keys(keys.clone(), target_exact.byte_budget)
			{
				Ok(exact) => RuntimeFilterKind::ExactSet(exact),
				Err(_) => match bloom_from_encoded_keys(keys) {
					Ok(bloom) => RuntimeFilterKind::Bloom(bloom),
					Err(_) => return false,
				},
			}
		}
		(RuntimeFilterKind::Bloom(target_bloom), RuntimeFilterKind::ExactSet(fragment_exact)) => {
			let mut bloom = target_bloom.clone();
			if insert_runtime_filter_exact_set_into_bloom(fragment_exact, &mut bloom).is_err() {
				return false;
			}
			RuntimeFilterKind::Bloom(bloom)
		}
		(RuntimeFilterKind::ExactSet(target_exact), RuntimeFilterKind::Bloom(fragment_bloom)) => {
			let mut bloom = fragment_bloom.clone();
			if insert_runtime_filter_exact_set_into_bloom(target_exact, &mut bloom).is_err() {
				return false;
			}
			RuntimeFilterKind::Bloom(bloom)
		}
	};
	target.kind = next_kind;
	true
}

fn blooms_compatible(left: &RuntimeFilterBloom, right: &RuntimeFilterBloom) -> bool {
	left.hash_seed == right.hash_seed
		&& left.bit_count == right.bit_count
		&& left.hash_count == right.hash_count
		&& left.byte_budget == right.byte_budget
		&& left.target_fp_rate_bp == right.target_fp_rate_bp
		&& left.payload.len() == right.payload.len()
}

fn merge_bloom_payload(target: &mut RuntimeFilterBloom, fragment: &RuntimeFilterBloom) {
	for (target_byte, fragment_byte) in target.payload.iter_mut().zip(&fragment.payload) {
		*target_byte |= *fragment_byte;
	}
}

fn bloom_from_encoded_keys(
	keys: Vec<Vec<u8>>,
) -> Result<RuntimeFilterBloom, RuntimeFilterDropReason> {
	let sizing = bloom_sizing(keys.len());
	let mut bloom = RuntimeFilterBloom {
		hash_seed: RUNTIME_FILTER_HASH_SEED,
		bit_count: sizing.bit_count,
		hash_count: sizing.hash_count,
		byte_budget: u32::try_from(sizing.byte_budget).expect("filter byte budget fits u32"),
		target_fp_rate_bp: TARGET_FP_RATE_BP,
		payload: vec![0u8; sizing.byte_budget],
	};
	for encoded in keys {
		insert_runtime_filter_encoded_key(
			&mut bloom.payload,
			bloom.bit_count,
			bloom.hash_count,
			bloom.hash_seed,
			&encoded,
		);
	}
	Ok(bloom)
}

pub(crate) fn runtime_filter_key_type(data_type: &DataType) -> Option<RuntimeFilterKeyType> {
	schema_runtime_filter_key_type(data_type)
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
	insert_runtime_filter_encoded_key(
		payload,
		bit_count,
		hash_count,
		RUNTIME_FILTER_HASH_SEED,
		encoded,
	);
}

fn exact_payload_len(encoded: &[u8]) -> Result<usize, RuntimeFilterDropReason> {
	let _ = u32::try_from(encoded.len()).map_err(|_| RuntimeFilterDropReason::UnsupportedShape)?;
	encoded
		.len()
		.checked_add(4)
		.ok_or(RuntimeFilterDropReason::UnsupportedShape)
}

fn runtime_filter_error(error: RuntimeFilterDropReason) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!("runtime filter {error}"),
	}
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
			encode_runtime_filter_key(int32.as_ref(), 0).unwrap(),
			encode_runtime_filter_key(int64.as_ref(), 0).unwrap()
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
		let RuntimeFilterKind::ExactSet(exact_set) = &fragment.kind else {
			panic!("small build set should emit ExactSet");
		};
		assert_eq!(exact_set.encoded_key_count, 4);
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
		let RuntimeFilterKind::Bloom(_) = &fragment.kind else {
			panic!("large build set should fall back to Bloom");
		};
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

		let mut incompatible = bloom_fragment(vec![1, 2], RuntimeFilterKeyType::Int64);
		let mut incompatible_other = incompatible.clone();
		let RuntimeFilterKind::Bloom(bloom) = &mut incompatible_other.kind else {
			panic!("expected bloom fragment");
		};
		bloom.hash_count = bloom.hash_count.saturating_add(1);
		assert!(!merge_fragments(&mut incompatible, &incompatible_other));
	}

	#[test]
	fn runtime_filter_fragment_merge_lowers_exact_overflow_to_bloom() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let left = RecordBatch::try_new(
			schema.clone(),
			vec![Arc::new(Int64Array::from_iter_values(0_i64..1000)) as ArrayRef],
		)
		.unwrap();
		let right = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from_iter_values(1000_i64..2000)) as ArrayRef],
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

		assert!(matches!(merged.kind, RuntimeFilterKind::ExactSet(_)));
		assert!(matches!(
			right_fragment.kind,
			RuntimeFilterKind::ExactSet(_)
		));
		assert!(merge_fragments(&mut merged, &right_fragment));
		assert!(matches!(merged.kind, RuntimeFilterKind::Bloom(_)));

		let descriptor =
			descriptor_from_fragment(&merged, vec![0], runtime_filter_test_fingerprint());
		for batch in [&left, &right] {
			for row in [0, batch.num_rows() - 1] {
				assert!(runtime_filter_might_contain(&descriptor, batch.column(0), row).unwrap());
			}
		}
	}

	#[test]
	fn runtime_filter_generation_duplicate_heavy_can_emit_exact_set() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![42_i64; 100_000])) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: true,
		})];

		let fragment = build_runtime_filter_fragment(&[batch], &key)
			.unwrap()
			.expect("fragment");
		let RuntimeFilterKind::ExactSet(exact_set) = &fragment.kind else {
			panic!("duplicate-heavy build should emit ExactSet");
		};
		assert_eq!(exact_set.encoded_key_count, 1);
	}

	#[test]
	fn runtime_filter_generation_unique_heavy_bloom_wins_baseline() {
		// R5 baseline. Unique-heavy build that overflows the ExactSet gate
		// (8192 distinct Int64 keys ≈ 106 KiB encoded, well above 16 KiB).
		// Today's implementation scans batches once during ExactSet
		// accumulation (early-exits at overflow), then re-scans for the
		// Bloom build — informational baseline for an eventual dual-
		// accumulator refactor. Prints wall time for the bench artifact;
		// no assertion so the test stays portable.
		let build_values = (0_i64..8192).collect::<Vec<_>>();
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(build_values)) as ArrayRef],
		)
		.unwrap();
		let key = [ExecutionExpr::Column(crate::ir::ResolvedColumn {
			name: crate::ir::ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: true,
		})];

		let start = std::time::Instant::now();
		let fragment = build_runtime_filter_fragment(&[batch.clone()], &key)
			.unwrap()
			.expect("fragment");
		let elapsed = start.elapsed();
		let RuntimeFilterKind::Bloom(_) = &fragment.kind else {
			panic!("unique-heavy build should fall back to Bloom");
		};
		println!(
			"adr014_runtime_filter_unique_heavy_bloom_wins build_rows={} build_distinct={} elapsed_us={}",
			batch.num_rows(),
			batch.num_rows(),
			elapsed.as_micros(),
		);
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

	fn bloom_fragment(payload: Vec<u8>, key_type: RuntimeFilterKeyType) -> RuntimeFilterFragment {
		RuntimeFilterFragment {
			key_encoding_version: RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![key_type],
			kind: RuntimeFilterKind::Bloom(RuntimeFilterBloom {
				hash_seed: RUNTIME_FILTER_HASH_SEED,
				bit_count: u32::try_from(payload.len() * 8).unwrap(),
				hash_count: 3,
				byte_budget: u32::try_from(payload.len()).unwrap(),
				target_fp_rate_bp: TARGET_FP_RATE_BP,
				payload,
			}),
		}
	}
}
