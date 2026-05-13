use arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use std::path::Path;
use wendelon::schema::pbc::ColumnPredicateLiteralV1;
use wendelon::schema::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
	AggregateRequest, AggregateRequestId, BaseValue, BitsPerValue, BufferValueCount, Consistency,
	DictValueCount, EncodingCascadeIndex, HybridTimestamp, IpcBufferIndex, IpcColumnOrdinal,
	Locality, LocalityPolicy, LocalityRange, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1,
	RecordBodyByteLen, RecordLayoutPolicy, RecordRowIndex, RowCount, RowGroupIndex,
	RowIdentityPolicy, RunCount, SchemaEvolutionPolicyV1, SimdPageValueCount, TableObjectId,
	TableSchemaDescriptor, TenantId,
};
use wendelon::storage::encoding::cascade::{EncodedBuffer, encode_buffer};
use wendelon::storage::encoding::compute::kernels::validity::{
	compose_value_mask_with_validity, validity_bit,
};
use wendelon::storage::encoding::{
	BufferEncoding, EncodedAggregateContext, EncodedRowMask, EncodingCascade, EncodingLeafCascade,
	PredicateAtom, PredicateAtomOp, SelectionRun, coerce_literal, evaluate, evaluate_decode_scalar,
	evaluate_validity_bitmap, try_fold_encoded_aggregate_request,
};

#[test]
fn decode_scalar_oracle_matches_current_dispatch_for_native_cascade_matrix() {
	assert_oracle_matches_dispatch(
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(4),
			base: BaseValue::ZERO,
		},
		DataType::UInt32,
		&u32_bytes(&[0, 1, 2, 5, 5, 7, 9, 15]),
		PredicateAtomOp::Eq,
		ColumnPredicateLiteralV1::Int(5),
	);
	assert_oracle_matches_dispatch(
		EncodingCascade::Rle {
			run_count: RunCount::new(4),
			inner: Box::new(EncodingLeafCascade::Plain),
		},
		DataType::UInt8,
		&[1, 1, 1, 2, 2, 7, 7, 9],
		PredicateAtomOp::Gte,
		ColumnPredicateLiteralV1::Int(7),
	);
	assert_oracle_matches_dispatch(
		EncodingCascade::Dictionary {
			dict_byte_len: RecordBodyByteLen::new(16),
			dict_value_count: DictValueCount::new(4),
			inner: Box::new(EncodingLeafCascade::Plain),
		},
		DataType::UInt32,
		&u32_bytes(&[3, 3, 5, 7, 5, 11, 7, 3]),
		PredicateAtomOp::InList,
		ColumnPredicateLiteralV1::List(vec![
			ColumnPredicateLiteralV1::Int(5),
			ColumnPredicateLiteralV1::Int(11),
		]),
	);
	assert_oracle_matches_dispatch(
		EncodingCascade::Dictionary {
			dict_byte_len: RecordBodyByteLen::new(16),
			dict_value_count: DictValueCount::new(4),
			inner: Box::new(EncodingLeafCascade::BitPacked {
				bits: BitsPerValue::new(2),
				base: BaseValue::ZERO,
			}),
		},
		DataType::UInt32,
		&u32_bytes(&[3, 3, 5, 7, 5, 11, 7, 3]),
		PredicateAtomOp::Lt,
		ColumnPredicateLiteralV1::Int(7),
	);
	assert_oracle_matches_dispatch(
		EncodingCascade::Delta {
			base: BaseValue::ZERO,
			inner: Box::new(EncodingLeafCascade::Plain),
		},
		DataType::Int64,
		&i64_bytes(&[10, 13, 12, 20, 20, 31, 29, 35]),
		PredicateAtomOp::Gte,
		ColumnPredicateLiteralV1::Int(20),
	);
}

#[test]
fn bitpacked_native_predicate_matches_decode_scalar_for_ops_and_base_offsets() {
	let unsigned_values = u32_bytes(&[0, 1, 2, 5, 5, 7, 9, 15]);
	for (op, literal) in [
		(PredicateAtomOp::Eq, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Neq, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Gt, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Gte, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Lt, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Lte, ColumnPredicateLiteralV1::Int(5)),
		(PredicateAtomOp::Eq, ColumnPredicateLiteralV1::Int(99)),
		(PredicateAtomOp::Neq, ColumnPredicateLiteralV1::Int(99)),
	] {
		assert_oracle_matches_dispatch(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(4),
				base: BaseValue::ZERO,
			},
			DataType::UInt32,
			&unsigned_values,
			op,
			literal,
		);
	}
	assert_oracle_matches_dispatch(
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(4),
			base: BaseValue::ZERO,
		},
		DataType::UInt32,
		&unsigned_values,
		PredicateAtomOp::InList,
		ColumnPredicateLiteralV1::List(vec![
			ColumnPredicateLiteralV1::Int(2),
			ColumnPredicateLiteralV1::Int(9),
		]),
	);

	assert_oracle_matches_dispatch(
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(3),
			base: BaseValue::new(10),
		},
		DataType::UInt32,
		&u32_bytes(&[10, 11, 12, 14, 15, 15]),
		PredicateAtomOp::Gte,
		ColumnPredicateLiteralV1::Int(14),
	);
	assert_oracle_matches_dispatch(
		EncodingCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::ZERO,
		},
		DataType::Int8,
		&i8_bytes(&[-2, -1, 0, 1, 5, 7]),
		PredicateAtomOp::Gt,
		ColumnPredicateLiteralV1::Int(-1),
	);
}

#[test]
fn dictionary_native_predicate_matches_decode_scalar_for_plain_and_bitpacked_indices() {
	let decoded = u32_bytes(&[3, 3, 5, 7, 5, 11, 7, 3]);
	for inner in [
		EncodingLeafCascade::Plain,
		EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(2),
			base: BaseValue::ZERO,
		},
	] {
		for (op, literal) in [
			(PredicateAtomOp::Eq, ColumnPredicateLiteralV1::Int(5)),
			(PredicateAtomOp::Neq, ColumnPredicateLiteralV1::Int(5)),
			(PredicateAtomOp::Gt, ColumnPredicateLiteralV1::Int(5)),
			(PredicateAtomOp::Lte, ColumnPredicateLiteralV1::Int(7)),
		] {
			assert_oracle_matches_dispatch(
				EncodingCascade::Dictionary {
					dict_byte_len: RecordBodyByteLen::new(16),
					dict_value_count: DictValueCount::new(4),
					inner: Box::new(inner.clone()),
				},
				DataType::UInt32,
				&decoded,
				op,
				literal,
			);
		}
		assert_oracle_matches_dispatch(
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(16),
				dict_value_count: DictValueCount::new(4),
				inner: Box::new(inner),
			},
			DataType::UInt32,
			&decoded,
			PredicateAtomOp::InList,
			ColumnPredicateLiteralV1::List(vec![
				ColumnPredicateLiteralV1::Int(3),
				ColumnPredicateLiteralV1::Int(11),
			]),
		);
	}
}

#[test]
fn delta_native_predicate_matches_decode_scalar_for_prefix_sum_walk() {
	let decoded = i64_bytes(&[10, 13, 12, 20, 20, 31, 29, 35]);
	for (op, literal) in [
		(PredicateAtomOp::Eq, ColumnPredicateLiteralV1::Int(20)),
		(PredicateAtomOp::Neq, ColumnPredicateLiteralV1::Int(20)),
		(PredicateAtomOp::Gt, ColumnPredicateLiteralV1::Int(20)),
		(PredicateAtomOp::Lte, ColumnPredicateLiteralV1::Int(20)),
	] {
		assert_oracle_matches_dispatch(
			EncodingCascade::Delta {
				base: BaseValue::ZERO,
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			DataType::Int64,
			&decoded,
			op,
			literal,
		);
	}
}

#[test]
fn rle_native_predicate_matches_decode_scalar_for_byte_runs() {
	let decoded = [1_u8, 1, 1, 2, 2, 7, 7, 9, 9, 9, 3];
	for inner in [
		EncodingLeafCascade::Plain,
		EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(4),
			base: BaseValue::ZERO,
		},
	] {
		for (op, literal) in [
			(PredicateAtomOp::Eq, ColumnPredicateLiteralV1::Int(7)),
			(PredicateAtomOp::Neq, ColumnPredicateLiteralV1::Int(7)),
			(PredicateAtomOp::Gte, ColumnPredicateLiteralV1::Int(7)),
			(PredicateAtomOp::Lt, ColumnPredicateLiteralV1::Int(3)),
		] {
			assert_oracle_matches_dispatch(
				EncodingCascade::Rle {
					run_count: RunCount::new(5),
					inner: Box::new(inner.clone()),
				},
				DataType::UInt8,
				&decoded,
				op,
				literal,
			);
		}
		assert_oracle_matches_dispatch(
			EncodingCascade::Rle {
				run_count: RunCount::new(5),
				inner: Box::new(inner),
			},
			DataType::UInt8,
			&decoded,
			PredicateAtomOp::InList,
			ColumnPredicateLiteralV1::List(vec![
				ColumnPredicateLiteralV1::Int(1),
				ColumnPredicateLiteralV1::Int(9),
			]),
		);
	}
}

#[test]
fn native_aggregate_partials_match_decoded_oracle_for_native_cascades() {
	let selected =
		EncodedRowMask::from_bool_slice(&[true, false, true, true, false, true, false, true]);
	let validity =
		EncodedRowMask::from_bool_slice(&[true, true, false, true, true, false, true, true]);

	for (cascade, data_type, decoded, value_tags) in [
		(
			EncodingCascade::BitPacked {
				bits: BitsPerValue::new(4),
				base: BaseValue::ZERO,
			},
			DataType::UInt32,
			u32_bytes(&[0, 1, 2, 5, 5, 7, 9, 15]),
			vec![
				AggregateFunctionTag::UNSIGNED_SUM,
				AggregateFunctionTag::UNSIGNED_AVG,
				AggregateFunctionTag::MIN,
				AggregateFunctionTag::MAX,
			],
		),
		(
			EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(16),
				dict_value_count: DictValueCount::new(4),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(2),
					base: BaseValue::ZERO,
				}),
			},
			DataType::UInt32,
			u32_bytes(&[3, 3, 5, 7, 5, 11, 7, 3]),
			vec![
				AggregateFunctionTag::UNSIGNED_SUM,
				AggregateFunctionTag::UNSIGNED_AVG,
				AggregateFunctionTag::MIN,
				AggregateFunctionTag::MAX,
			],
		),
		(
			EncodingCascade::Delta {
				base: BaseValue::ZERO,
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			DataType::Int64,
			i64_bytes(&[10, 13, 12, 20, 20, 31, 29, 35]),
			vec![
				AggregateFunctionTag::SIGNED_SUM,
				AggregateFunctionTag::SIGNED_AVG,
				AggregateFunctionTag::MIN,
				AggregateFunctionTag::MAX,
			],
		),
		(
			EncodingCascade::Rle {
				run_count: RunCount::new(4),
				inner: Box::new(EncodingLeafCascade::Plain),
			},
			DataType::UInt8,
			vec![1_u8, 1, 1, 2, 2, 7, 7, 9],
			vec![
				AggregateFunctionTag::UNSIGNED_SUM,
				AggregateFunctionTag::UNSIGNED_AVG,
				AggregateFunctionTag::MIN,
				AggregateFunctionTag::MAX,
			],
		),
	] {
		assert_native_aggregates_match_decoded(
			cascade,
			data_type,
			&decoded,
			&selected,
			&validity,
			&value_tags,
		);
	}
}

#[test]
fn validity_composition_matches_scalar_masks_for_all_shapes() {
	let rows = RowCount::new(10);
	let validity = [0b1011_0101_u8, 0b0000_0010_u8];
	let shapes = vec![
		EncodedRowMask::All { rows },
		EncodedRowMask::None { rows },
		EncodedRowMask::from_bool_slice(&[
			true, false, true, true, false, true, false, true, false, true,
		]),
		EncodedRowMask::Runs {
			rows,
			runs: vec![
				SelectionRun {
					first_row: RecordRowIndex::new(0),
					row_count: RowCount::new(3),
					selected: true,
				},
				SelectionRun {
					first_row: RecordRowIndex::new(3),
					row_count: RowCount::new(2),
					selected: false,
				},
				SelectionRun {
					first_row: RecordRowIndex::new(5),
					row_count: RowCount::new(5),
					selected: true,
				},
			],
		},
	];

	for shape in shapes {
		let actual =
			compose_value_mask_with_validity(&shape, Some(&validity)).expect("compose validity");
		let expected = shape
			.to_bool_vec()
			.expect("shape bools")
			.into_iter()
			.enumerate()
			.map(|(row, selected)| selected && validity_bit(Some(&validity), rows, row).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(
			actual.to_bool_vec().expect("actual bools"),
			expected,
			"shape: {shape:?}"
		);
		assert_eq!(
			compose_value_mask_with_validity(&shape, None).expect("no validity"),
			shape
		);
	}

	assert_eq!(
		evaluate_validity_bitmap(Some(&validity), rows, true)
			.expect("is null")
			.to_bool_vec()
			.expect("null bools"),
		(0..10)
			.map(|row| !validity_bit(Some(&validity), rows, row).unwrap())
			.collect::<Vec<_>>()
	);
	assert!(validity_bit(Some(&validity), rows, 10).is_err());
	assert_eq!(
		compose_value_mask_with_validity(&EncodedRowMask::All { rows }, Some(&[0xff_u8, 0xff_u8]))
			.expect("tail bits ignored"),
		EncodedRowMask::All { rows }
	);
}

#[test]
fn mask_algebra_specializations_match_bool_oracle() {
	let rows = RowCount::new(10);
	let bitmap = EncodedRowMask::Bitmap {
		rows,
		bytes: vec![0b1010_1010, 0b0000_0011],
	};
	let runs = EncodedRowMask::Runs {
		rows,
		runs: vec![
			SelectionRun {
				first_row: RecordRowIndex::new(1),
				row_count: RowCount::new(3),
				selected: true,
			},
			SelectionRun {
				first_row: RecordRowIndex::new(6),
				row_count: RowCount::new(2),
				selected: true,
			},
		],
	};
	let shapes = vec![
		EncodedRowMask::All { rows },
		EncodedRowMask::None { rows },
		bitmap,
		runs,
	];
	for left in &shapes {
		for right in &shapes {
			let left_values = left.to_bool_vec().expect("left bools");
			let right_values = right.to_bool_vec().expect("right bools");
			assert_eq!(
				left.and(right)
					.expect("and")
					.to_bool_vec()
					.expect("and bools"),
				left_values
					.iter()
					.zip(&right_values)
					.map(|(left, right)| *left && *right)
					.collect::<Vec<_>>()
			);
			assert_eq!(
				left.or(right).expect("or").to_bool_vec().expect("or bools"),
				left_values
					.iter()
					.zip(&right_values)
					.map(|(left, right)| *left || *right)
					.collect::<Vec<_>>()
			);
		}
		assert_eq!(
			left.not().expect("not").to_bool_vec().expect("not bools"),
			left.to_bool_vec()
				.expect("left bools")
				.into_iter()
				.map(|selected| !selected)
				.collect::<Vec<_>>()
		);
	}
}

#[test]
fn fsst_kernel_invariants_are_locked_to_streaming_multi_buffer_shape() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let fsst_kernel =
		fs::read_to_string(root.join("src/storage/encoding/compute/kernels/fsst.rs")).unwrap();
	let manager = fs::read_to_string(root.join("src/storage/manager.rs")).unwrap();
	for required in [
		"FsstDecodedByteReader",
		"reader.skip(row_len)?",
		"by_len.get(&row_len)",
		"decode_offsets",
		"offset < previous",
		"offsets.last().copied() != Some(expected_final)",
	] {
		assert!(
			fsst_kernel.contains(required),
			"FSST kernel invariant guard is missing token {required}"
		);
	}
	for forbidden in [
		"symbol_table_byte_len",
		"decode_fsst_bytes",
		"materialize_encoding_buffer",
		"StringArray",
	] {
		assert!(
			!fsst_kernel.contains(forbidden),
			"FSST kernel must stay streaming and header-authoritative; forbidden token {forbidden}"
		);
	}
	for required in [
		"FsstValues",
		"FsstOffsets",
		"offsets_buffer_ordinal",
		"max_encoded_pages_per_record",
		"output_columns.contains(*column_index as u32)",
	] {
		assert!(
			manager.contains(required),
			"FSST manager invariant guard is missing token {required}"
		);
	}
}

fn assert_oracle_matches_dispatch(
	cascade: EncodingCascade,
	data_type: DataType,
	decoded: &[u8],
	op: PredicateAtomOp,
	literal: ColumnPredicateLiteralV1,
) {
	let rows = match data_type {
		DataType::Int8 => decoded.len(),
		DataType::UInt8 => decoded.len(),
		DataType::UInt32 => decoded.len() / 4,
		DataType::Int64 => decoded.len() / 8,
		_ => unreachable!("test data type"),
	};
	let encoded = encode_buffer(
		decoded,
		&data_type,
		cascade.clone(),
		BufferValueCount::new(rows as u32),
		SimdPageValueCount::new(4),
	)
	.expect("encode fixture");
	let encoding = buffer_encoding(&encoded);
	let atom = PredicateAtom {
		op,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).expect("coerce literal");
	let production = evaluate(&encoded.bytes, &encoding, &cascade, &atom, &coercion)
		.expect("production evaluate");
	let oracle = evaluate_decode_scalar(&encoded.bytes, &encoding, &cascade, &atom, &coercion)
		.expect("oracle evaluate");
	assert_eq!(
		production.to_bool_vec().expect("production mask"),
		oracle.to_bool_vec().expect("oracle mask")
	);
}

fn assert_native_aggregates_match_decoded(
	cascade: EncodingCascade,
	data_type: DataType,
	decoded: &[u8],
	selected: &EncodedRowMask,
	validity: &EncodedRowMask,
	value_tags: &[AggregateFunctionTag],
) {
	let rows = rows_for_data_type(&data_type, decoded);
	let encoded = encode_buffer(
		decoded,
		&data_type,
		cascade.clone(),
		BufferValueCount::new(rows as u32),
		SimdPageValueCount::new(4),
	)
	.expect("encode aggregate fixture");
	let encoding = buffer_encoding(&encoded);
	let row_count = RowCount::new(rows as u64);
	let decoded_context = EncodedAggregateContext::new(row_count)
		.with_validity_mask(0, validity.clone())
		.expect("decoded validity")
		.with_decoded_column(0, data_type.clone(), decoded.to_vec());
	let encoded_context = EncodedAggregateContext::new(row_count)
		.with_validity_mask(0, validity.clone())
		.expect("encoded validity")
		.with_encoded_column(0, data_type, &encoded.bytes, &encoding, &cascade)
		.expect("encoded aggregate column");
	let mut expressions = vec![
		AggregateExpression::count_star(),
		aggregate_expr(AggregateFunctionTag::COUNT_NON_NULL, 0),
	];
	expressions.extend(value_tags.iter().copied().map(|tag| aggregate_expr(tag, 0)));
	let request = aggregate_request(expressions);
	let decoded_partials =
		try_fold_encoded_aggregate_request(&request, &decoded_context, Some(selected))
			.expect("decoded aggregate")
			.expect("decoded aggregate supported");
	let encoded_partials =
		try_fold_encoded_aggregate_request(&request, &encoded_context, Some(selected))
			.expect("encoded aggregate")
			.expect("encoded aggregate supported");
	assert_eq!(encoded_partials, decoded_partials);
}

fn rows_for_data_type(data_type: &DataType, decoded: &[u8]) -> usize {
	match data_type {
		DataType::Int8 | DataType::UInt8 => decoded.len(),
		DataType::UInt32 => decoded.len() / 4,
		DataType::Int64 => decoded.len() / 8,
		_ => unreachable!("test data type"),
	}
}

fn buffer_encoding(encoded: &EncodedBuffer) -> BufferEncoding {
	BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: encoded.decoded_byte_len,
		decoded_value_count: encoded.decoded_value_count,
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(4),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	}
}

fn aggregate_request(aggregates: Vec<AggregateExpression>) -> AggregateRequest {
	AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(59),
		tenant_id: TenantId::new(1),
		table_object_id: TableObjectId::new([9; 16]),
		schema_generation: 1,
		logical_schema_fingerprint: logical_fingerprint(),
		consistency: Consistency::LINEARIZABLE,
		min_hlc: HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 1,
		},
		as_of_hlc: HybridTimestamp {
			physical_ms: 2,
			logical: 0,
			node_id: 1,
		},
		locality_ranges: vec![LocalityRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		predicate_program: None,
		aggregates,
		budgets: AggregateBudgets::default(),
	}
}

fn aggregate_expr(tag: AggregateFunctionTag, column_ordinal: u16) -> AggregateExpression {
	AggregateExpression {
		tag,
		column_ordinal,
		parameter_block: Vec::new(),
	}
}

fn logical_fingerprint() -> wendelon::schema::LogicalSchemaFingerprint {
	let schema = Schema::new(vec![Field::new("id", DataType::UInt8, false)]);
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x59; 16]),
		1,
		"test",
		"encoding_compute_kernel_invariants",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
	.logical_schema_fingerprint
}

fn u32_bytes(values: &[u32]) -> Vec<u8> {
	values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect()
}

fn i64_bytes(values: &[i64]) -> Vec<u8> {
	values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect()
}

fn i8_bytes(values: &[i8]) -> Vec<u8> {
	values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect()
}
