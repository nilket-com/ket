//! ADR 621: fixtures for inline `Generic` build keys. Owned and inline tables are built from the
//! same keys in one process (`build_in_memory_join_rows_with_storage`), whichever storage the
//! production token selects, and must agree on hashing, equality, lookups and join results.

use std::sync::Arc;

use arrow::array::{
	ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
	Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
	TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
	UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::{
	BuildInsertMode, GenericBuildKeyStorage, INLINE_JOIN_KEY_BYTES, InlineJoinKey,
	build_in_memory_join_rows_with_storage, generic_inline_eligible,
};
use crate::exec::join::{
	JoinBuildRows, JoinKey, JoinKeyPlan, estimated_build_hash_bytes, join_key_bytes,
	probe_in_memory_join_rows,
};
use crate::ir::JoinKind;

fn build(arrays: &[ArrayRef], storage: GenericBuildKeyStorage, null_safe: bool) -> JoinBuildRows {
	build_in_memory_join_rows_with_storage(
		JoinKeyPlan::Generic,
		arrays,
		arrays[0].len(),
		u64::MAX,
		null_safe,
		BuildInsertMode::Windowed,
		storage,
	)
	.expect("build")
	.expect("an uncapped build is admitted")
}

fn batch(arrays: &[ArrayRef]) -> RecordBatch {
	let fields: Vec<Field> = arrays
		.iter()
		.enumerate()
		.map(|(i, a)| Field::new(format!("k{i}"), a.data_type().clone(), true))
		.collect();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.to_vec()).expect("batch")
}

/// Mixed-width composite keys with duplicates (one past the inline row-list capacity), nulls, and
/// float bit patterns (NaN payloads, signed zero), shaped as three columns: Int64, Int32, Float64.
fn build_keys() -> Vec<ArrayRef> {
	let quiet_nan = f64::from_bits(0x7ff8_0000_0000_0001);
	let other_nan = f64::from_bits(0x7ff8_0000_0000_0002);
	let mut a = vec![
		Some(1i64),
		Some(2),
		None,
		Some(4),
		Some(5),
		Some(6),
		Some(7),
	];
	let mut b = vec![
		Some(10i32),
		Some(20),
		Some(30),
		None,
		Some(50),
		Some(60),
		Some(70),
	];
	let mut c = vec![
		Some(0.0f64),
		Some(-0.0),
		Some(1.5),
		Some(2.5),
		Some(quiet_nan),
		Some(other_nan),
		Some(3.5),
	];
	for _ in 0..20 {
		a.push(Some(1));
		b.push(Some(10));
		c.push(Some(0.0));
	}
	vec![
		Arc::new(Int64Array::from(a)) as ArrayRef,
		Arc::new(Int32Array::from(b)) as ArrayRef,
		Arc::new(Float64Array::from(c)) as ArrayRef,
	]
}

fn probe_keys() -> Vec<ArrayRef> {
	let quiet_nan = f64::from_bits(0x7ff8_0000_0000_0001);
	vec![
		Arc::new(Int64Array::from(vec![
			Some(1i64),
			Some(2),
			Some(2),
			Some(5),
			Some(9),
			None,
			Some(7),
		])) as ArrayRef,
		Arc::new(Int32Array::from(vec![
			Some(10i32),
			Some(20),
			Some(20),
			Some(50),
			Some(90),
			Some(1),
			Some(70),
		])) as ArrayRef,
		Arc::new(Float64Array::from(vec![
			Some(0.0f64),
			Some(-0.0),
			Some(0.0),
			Some(quiet_nan),
			Some(1.0),
			Some(1.0),
			Some(3.5),
		])) as ArrayRef,
	]
}

#[test]
fn inline_keys_hash_exactly_as_owned_generic_keys() {
	let state = ahash::RandomState::new();
	for width in 0..=INLINE_JOIN_KEY_BYTES {
		let bytes: Vec<u8> = (0..width).map(|i| (i * 37 + 11) as u8).collect();
		let inline = InlineJoinKey::from_slice(&bytes).expect("fits inline");
		assert_eq!(
			state.hash_one(&JoinKey::Generic(bytes.clone())),
			state.hash_one(&inline),
			"width {width}"
		);
	}
	assert!(InlineJoinKey::from_slice(&[0u8; INLINE_JOIN_KEY_BYTES + 1]).is_none());
}

#[test]
fn inline_key_equality_uses_only_the_encoded_prefix() {
	let a = InlineJoinKey::from_slice(&[5, 1, 2, 3]).expect("fits");
	let mut padded = a;
	padded.bytes[10] = 0xff; // differing unused padding
	assert_eq!(a, padded);
	assert_ne!(a, InlineJoinKey::from_slice(&[5, 1, 2]).expect("fits")); // a different length
	assert_ne!(a, InlineJoinKey::from_slice(&[4, 1, 2, 3]).expect("fits")); // a different type tag
	assert_ne!(a, InlineJoinKey::from_slice(&[5, 1, 2, 4]).expect("fits")); // a different byte
}

#[test]
fn the_width_helper_matches_the_encoder_for_every_type() {
	let utc = Some(Arc::<str>::from("UTC"));
	let supported: Vec<ArrayRef> = vec![
		Arc::new(BooleanArray::from(vec![true])),
		Arc::new(Int8Array::from(vec![1i8])),
		Arc::new(UInt8Array::from(vec![1u8])),
		Arc::new(Int16Array::from(vec![1i16])),
		Arc::new(UInt16Array::from(vec![1u16])),
		Arc::new(Int32Array::from(vec![1i32])),
		Arc::new(UInt32Array::from(vec![1u32])),
		Arc::new(Float32Array::from(vec![1.0f32])),
		Arc::new(Date32Array::from(vec![1i32])),
		Arc::new(Int64Array::from(vec![1i64])),
		Arc::new(UInt64Array::from(vec![1u64])),
		Arc::new(Float64Array::from(vec![1.0f64])),
		Arc::new(TimestampMillisecondArray::from(vec![1i64]).with_timezone_opt(utc.clone())),
		Arc::new(TimestampMicrosecondArray::from(vec![1i64]).with_timezone_opt(utc.clone())),
		Arc::new(TimestampNanosecondArray::from(vec![1i64]).with_timezone_opt(utc.clone())),
		Arc::new(
			Decimal128Array::from(vec![1i128])
				.with_precision_and_scale(15, 2)
				.expect("decimal"),
		),
	];
	for array in &supported {
		let mut encoded = Vec::new();
		crate::exec::encode_partition_key_value(array.as_ref(), 0, &mut encoded).expect("encode");
		assert_eq!(
			crate::exec::encoded_join_key_width(array.data_type()),
			Some(encoded.len()),
			"{:?}",
			array.data_type()
		);
	}
	let rejected: Vec<ArrayRef> = vec![
		Arc::new(TimestampSecondArray::from(vec![1i64]).with_timezone_opt(utc.clone())),
		Arc::new(TimestampMillisecondArray::from(vec![1i64])),
		Arc::new(
			TimestampMicrosecondArray::from(vec![1i64])
				.with_timezone_opt(Some(Arc::<str>::from("Europe/Paris"))),
		),
		Arc::new(StringArray::from(vec!["a"])),
	];
	for array in &rejected {
		assert_eq!(
			crate::exec::encoded_join_key_width(array.data_type()),
			None,
			"{:?}",
			array.data_type()
		);
	}
	assert_eq!(
		crate::exec::encoded_join_key_width(&DataType::Timestamp(TimeUnit::Second, None)),
		None
	);
}

#[test]
fn eligibility_is_non_null_safe_all_fixed_width_and_at_most_thirty_two_bytes() {
	let decimal = || {
		Arc::new(
			Decimal128Array::from(vec![1i128])
				.with_precision_and_scale(15, 2)
				.expect("decimal"),
		) as ArrayRef
	};
	let exactly_32: Vec<ArrayRef> = vec![
		decimal(),
		Arc::new(Int64Array::from(vec![1i64])),
		Arc::new(Int16Array::from(vec![1i16])),
		Arc::new(Int16Array::from(vec![1i16])),
	]; // 17 + 9 + 3 + 3
	let thirty_three: Vec<ArrayRef> = vec![
		decimal(),
		Arc::new(Int64Array::from(vec![1i64])),
		Arc::new(Int32Array::from(vec![1i32])),
		Arc::new(Int8Array::from(vec![1i8])),
	]; // 17 + 9 + 5 + 2
	assert!(generic_inline_eligible(&exactly_32, false));
	assert!(!generic_inline_eligible(&thirty_three, false));
	assert!(!generic_inline_eligible(&exactly_32, true)); // null-safe
	let with_text: Vec<ArrayRef> = vec![
		Arc::new(Int64Array::from(vec![1i64])),
		Arc::new(StringArray::from(vec!["a"])),
	];
	assert!(!generic_inline_eligible(&with_text, false));
	// Ineligible builds stay owned under the inline token.
	for (arrays, null_safe) in [
		(&thirty_three, false),
		(&exactly_32, true),
		(&with_text, false),
	] {
		let table = build(arrays, GenericBuildKeyStorage::Inline, null_safe);
		assert_eq!(table.profile_shape().representation, "generic_hash");
	}
	let table = build(&exactly_32, GenericBuildKeyStorage::Inline, false);
	assert_eq!(table.profile_shape().representation, "generic_inline");
	assert!(table.inline_eligible());
}

#[test]
fn owned_and_inline_tables_agree_on_every_lookup() {
	let keys = build_keys();
	let owned = build(&keys, GenericBuildKeyStorage::Owned, false);
	let inline = build(&keys, GenericBuildKeyStorage::Inline, false);
	assert_eq!(owned.profile_shape().representation, "generic_hash");
	assert_eq!(inline.profile_shape().representation, "generic_inline");
	assert_eq!(owned.len(), inline.len());
	assert!(owned.inline_eligible() && inline.inline_eligible());
	let mut owned_lists: Vec<Vec<u32>> = owned.values().map(|r| r.rows().collect()).collect();
	let mut inline_lists: Vec<Vec<u32>> = inline.values().map(|r| r.rows().collect()).collect();
	owned_lists.sort();
	inline_lists.sort();
	assert_eq!(owned_lists, inline_lists);
	assert!(
		owned_lists.iter().any(|l| l.len() > 8),
		"an overflow row list is exercised"
	);
	// Every build and probe key, a length mismatch, and a non-Generic key.
	let probes = probe_keys();
	let mut lookups: Vec<JoinKey> = Vec::new();
	for arrays in [&keys, &probes] {
		for row in 0..arrays[0].len() {
			if let Some(bytes) = join_key_bytes(arrays, row).expect("encode") {
				lookups.push(JoinKey::Generic(bytes));
			}
		}
	}
	lookups.push(JoinKey::Generic(vec![5, 1, 0, 0, 0, 0, 0, 0, 0]));
	lookups.push(JoinKey::Generic(vec![0u8; INLINE_JOIN_KEY_BYTES + 4]));
	lookups.push(JoinKey::Int64(1));
	for key in &lookups {
		let o = owned.get(key).map(|r| r.rows().collect::<Vec<_>>());
		let i = inline.get(key).map(|r| r.rows().collect::<Vec<_>>());
		assert_eq!(o, i, "{key:?}");
	}
	assert_eq!(inline.get_int64(1).map(|_| ()), None);
	assert_eq!(inline.get_int32(1).map(|_| ()), None);
}

#[test]
fn owned_and_inline_tables_give_identical_join_results_for_every_kind() {
	let build_arrays = build_keys();
	let probe_arrays = probe_keys();
	let (build_batch, probe_batch) = (batch(&build_arrays), batch(&probe_arrays));
	let owned = build(&build_arrays, GenericBuildKeyStorage::Owned, false);
	let inline = build(&build_arrays, GenericBuildKeyStorage::Inline, false);
	for kind in [
		JoinKind::Inner,
		JoinKind::LeftOuter,
		JoinKind::RightOuter,
		JoinKind::FullOuter,
		JoinKind::LeftSemi,
		JoinKind::RightSemi,
		JoinKind::LeftAnti,
		JoinKind::RightAnti,
	] {
		let run = |table: &JoinBuildRows| {
			probe_in_memory_join_rows(
				kind,
				JoinKeyPlan::Generic,
				table,
				build_batch.num_rows(),
				&probe_arrays,
				probe_batch.num_rows(),
				None,
				&build_batch,
				&probe_batch,
				false,
			)
			.expect("probe")
		};
		assert_eq!(
			format!("{:?}", run(&owned)),
			format!("{:?}", run(&inline)),
			"{kind:?}"
		);
	}
}

#[test]
fn empty_and_all_null_builds_agree() {
	let empty: Vec<ArrayRef> = vec![
		Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
		Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
	];
	let all_null: Vec<ArrayRef> = vec![
		Arc::new(Int64Array::from(vec![None, Some(1), None])) as ArrayRef,
		Arc::new(Int64Array::from(vec![Some(2), None, None])) as ArrayRef,
	];
	for arrays in [&empty, &all_null] {
		let owned = build_in_memory_join_rows_with_storage(
			JoinKeyPlan::Generic,
			arrays,
			arrays[0].len(),
			u64::MAX,
			false,
			BuildInsertMode::Windowed,
			GenericBuildKeyStorage::Owned,
		)
		.expect("build")
		.expect("admitted");
		let inline = build_in_memory_join_rows_with_storage(
			JoinKeyPlan::Generic,
			arrays,
			arrays[0].len(),
			u64::MAX,
			false,
			BuildInsertMode::Windowed,
			GenericBuildKeyStorage::Inline,
		)
		.expect("build")
		.expect("admitted");
		assert_eq!((owned.len(), inline.len()), (0, 0));
		assert_eq!(inline.profile_shape().representation, "generic_inline");
	}
}

#[test]
fn inline_accounting_counts_capacity_entries_and_row_list_heaps() {
	let keys = build_keys();
	let inline = build(&keys, GenericBuildKeyStorage::Inline, false);
	let shape = inline.profile_shape();
	let entry = std::mem::size_of::<(InlineJoinKey, super::BuildRowList)>() + 16;
	let heap: usize = inline
		.values()
		.map(|r| r.heap_capacity_rows() * std::mem::size_of::<u32>())
		.sum();
	assert!(
		heap > 0,
		"a duplicate key's row list has grown onto the heap"
	);
	assert_eq!(
		shape.index_accounted_bytes,
		(shape.bucket_capacity as usize * entry + heap) as u64
	);
	let owned = build(&keys, GenericBuildKeyStorage::Owned, false);
	assert!(owned.profile_shape().index_accounted_bytes > 0);
}

/// Lookup and all-kind join-result parity between owned and inline tables for one key shape.
fn assert_parity(build_arrays: &[ArrayRef], probe_arrays: &[ArrayRef]) {
	let owned = build(build_arrays, GenericBuildKeyStorage::Owned, false);
	let inline = build(build_arrays, GenericBuildKeyStorage::Inline, false);
	assert_eq!(inline.representation(), "generic_inline");
	assert_eq!(owned.len(), inline.len());
	for arrays in [build_arrays, probe_arrays] {
		for row in 0..arrays[0].len() {
			if let Some(bytes) = join_key_bytes(arrays, row).expect("encode") {
				let key = JoinKey::Generic(bytes);
				assert_eq!(
					owned.get(&key).map(|r| r.rows().collect::<Vec<_>>()),
					inline.get(&key).map(|r| r.rows().collect::<Vec<_>>()),
					"{key:?}"
				);
			}
		}
	}
	let (build_batch, probe_batch) = (batch(build_arrays), batch(probe_arrays));
	for kind in [
		JoinKind::Inner,
		JoinKind::LeftOuter,
		JoinKind::RightOuter,
		JoinKind::FullOuter,
		JoinKind::LeftSemi,
		JoinKind::RightSemi,
		JoinKind::LeftAnti,
		JoinKind::RightAnti,
	] {
		let run = |table: &JoinBuildRows| {
			probe_in_memory_join_rows(
				kind,
				JoinKeyPlan::Generic,
				table,
				build_batch.num_rows(),
				probe_arrays,
				probe_batch.num_rows(),
				None,
				&build_batch,
				&probe_batch,
				false,
			)
			.expect("probe")
		};
		assert_eq!(
			format!("{:?}", run(&owned)),
			format!("{:?}", run(&inline)),
			"{kind:?}"
		);
	}
}

fn decimals(values: Vec<Option<i128>>) -> ArrayRef {
	Arc::new(
		Decimal128Array::from(values)
			.with_precision_and_scale(15, 2)
			.expect("decimal"),
	) as ArrayRef
}

#[test]
fn int64_and_decimal_composite_keys_agree() {
	let build_arrays = vec![
		Arc::new(Int64Array::from(vec![
			Some(1i64),
			Some(1),
			Some(2),
			None,
			Some(3),
			Some(1),
		])) as ArrayRef,
		decimals(vec![
			Some(150),
			Some(150),
			Some(-7),
			Some(1),
			None,
			Some(151),
		]),
	];
	let probe_arrays = vec![
		Arc::new(Int64Array::from(vec![
			Some(1i64),
			Some(2),
			Some(3),
			Some(1),
			Some(9),
		])) as ArrayRef,
		decimals(vec![Some(150), Some(-7), Some(0), Some(151), Some(150)]),
	];
	assert_parity(&build_arrays, &probe_arrays);
}

#[test]
fn three_int64_composite_keys_agree() {
	let col = |v: Vec<Option<i64>>| Arc::new(Int64Array::from(v)) as ArrayRef;
	let build_arrays = vec![
		col(vec![Some(1), Some(1), Some(2), Some(1), None]),
		col(vec![Some(2), Some(2), Some(3), Some(2), Some(9)]),
		col(vec![Some(3), Some(3), Some(4), Some(4), Some(9)]),
	];
	let probe_arrays = vec![
		col(vec![Some(1), Some(2), Some(1), Some(7)]),
		col(vec![Some(2), Some(3), Some(2), Some(7)]),
		col(vec![Some(3), Some(4), Some(4), Some(7)]),
	];
	assert_parity(&build_arrays, &probe_arrays);
}

#[test]
fn map_growth_keeps_accounting_duplicate_order_and_lookups() {
	let mut owned = JoinBuildRows::new_hash(1);
	let mut inline = JoinBuildRows::new_inline_generic(1);
	let key = |i: u64| {
		let mut bytes = vec![5u8];
		bytes.extend_from_slice(&(i as i64).to_le_bytes());
		bytes.push(5);
		bytes.extend_from_slice(&((i * 7) as i64).to_le_bytes());
		bytes
	};
	let accounting = |table: &JoinBuildRows| {
		let shape = table.profile_shape();
		let entry = std::mem::size_of::<(InlineJoinKey, super::BuildRowList)>() + 16;
		let heap: usize = table
			.values()
			.map(|r| r.heap_capacity_rows() * std::mem::size_of::<u32>())
			.sum();
		(shape.bucket_capacity as usize * entry + heap) as u64
	};
	let before = inline.capacity();
	assert_eq!(
		inline.profile_shape().index_accounted_bytes,
		accounting(&inline)
	);
	let mut row = 0u32;
	for i in 0..500u64 {
		for _ in 0..(1 + i % 3) {
			owned.insert(JoinKey::Generic(key(i)), row);
			inline.insert(JoinKey::Generic(key(i)), row);
			row += 1;
		}
	}
	assert!(inline.capacity() > before, "the inline map grew");
	assert_eq!(
		inline.profile_shape().index_accounted_bytes,
		accounting(&inline)
	);
	assert_eq!(owned.len(), inline.len());
	for i in 0..510u64 {
		let k = JoinKey::Generic(key(i));
		assert_eq!(
			owned.get(&k).map(|r| r.rows().collect::<Vec<_>>()),
			inline.get(&k).map(|r| r.rows().collect::<Vec<_>>()),
			"key {i}"
		);
	}
}

#[test]
fn the_memory_cap_admits_and_refuses_both_forms_alike() {
	let arrays = vec![
		Arc::new(Int64Array::from((0..200i64).map(Some).collect::<Vec<_>>())) as ArrayRef,
		Arc::new(Int64Array::from(
			(0..200i64).map(|i| Some(i * 3)).collect::<Vec<_>>(),
		)) as ArrayRef,
	];
	let full = estimated_build_hash_bytes(&arrays, 200);
	for cap in [
		0,
		full / 2,
		full.saturating_sub(1),
		full,
		full + 1,
		u64::MAX,
	] {
		let run = |storage| {
			build_in_memory_join_rows_with_storage(
				JoinKeyPlan::Generic,
				&arrays,
				200,
				cap,
				false,
				BuildInsertMode::Windowed,
				storage,
			)
			.expect("build")
			.map(|t| t.len())
		};
		assert_eq!(
			run(GenericBuildKeyStorage::Owned),
			run(GenericBuildKeyStorage::Inline),
			"cap {cap}"
		);
	}
	let refused = |cap| {
		build_in_memory_join_rows_with_storage(
			JoinKeyPlan::Generic,
			&arrays,
			200,
			cap,
			false,
			BuildInsertMode::Windowed,
			GenericBuildKeyStorage::Inline,
		)
		.expect("build")
		.is_none()
	};
	assert!(refused(full.saturating_sub(1)) && !refused(full));
}
