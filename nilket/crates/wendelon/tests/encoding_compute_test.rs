use arrow::datatypes::{DataType, Field, Schema};
use std::time::Duration;
use wendelon::metrics::MetricsRegistry;
use wendelon::schema::pbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, ColumnPredicateV1, PredicateTreeV1,
};
use wendelon::schema::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
	AggregateRequest, AggregateRequestId, AggregateState, BaseValue, BitsPerValue,
	BufferValueCount, Consistency, EncodingCascadeIndex, EncodingPageByteOffset, HybridTimestamp,
	IpcBufferIndex, IpcColumnOrdinal, Locality, LocalityPolicyV1, LocalityRange,
	MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordLayoutPolicy,
	RowCount, RowGroupIndex, RowIdentityPolicy, SchemaEvolutionPolicyV1, SimdPageValueCount,
	TableObjectId, TableSchemaDescriptorV1, TenantId, encode_aggregate_state,
};
use wendelon::schema::{DictValueCount, RunCount};
use wendelon::storage::encoding::cascade::encode_buffer;
use wendelon::storage::encoding::{
	BufferEncoding, EncodedAggregateContext, EncodedAggregateKind, EncodedBufferByteRange,
	EncodedPredicateNode, EncodedRowMask, EncodingCascade, EncodingComputeConfig,
	EncodingComputeEvaluatorKind, EncodingComputePlanMode, EncodingComputeRejectCode,
	EncodingLeafCascade, PredicateAtom, PredicateAtomOp, aggregate_kind_for_tag, coerce_literal,
	encoded_predicate_node_from_tree, evaluate, evaluate_fsst_predicate,
	evaluate_fsst_predicate_with_stats, evaluate_validity_bitmap, evaluator_kind_for_cascade,
	route_selectivity, try_fold_encoded_aggregate_request,
};

#[test]
fn encoding_compute_config_defaults_are_locked() {
	let config = EncodingComputeConfig::default();
	assert!(config.enabled);
	assert_eq!(config.max_dict_match_set_size.get(), 4_096);
	assert_eq!(config.min_buffer_rows_for_encoded_eval.get(), 256);
	assert_eq!(config.max_parallel_kernel_tasks.get(), 4);
	assert_eq!(config.max_mask_bytes_per_record.get(), 256 * 1024);
	assert_eq!(config.max_encoded_pages_per_record.get(), 256);
	assert_eq!(
		config.min_selectivity_for_encoded_eval_micros.get(),
		250_000
	);
	assert_eq!(config.min_prefilter_rejection_micros.get(), 100_000);
	assert!(config.validate().is_ok());
	assert!(config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::BitPacked));
	assert!(config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::DictionaryPlain));
	assert!(config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::DictionaryBitPacked));
	assert!(config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::Delta));
	assert!(!config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::Fsst));
	assert!(!config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::RlePlain));
	assert!(!config.predicate_evaluator_enabled(EncodingComputeEvaluatorKind::RleBitPacked));
	assert!(config.aggregate_kernel_enabled(EncodedAggregateKind::CountStar));
	assert_eq!(
		aggregate_kind_for_tag(AggregateFunctionTag::QUANTILE_KLL),
		None
	);
}

#[test]
fn encoded_row_mask_validates_and_round_trips() {
	let mask = EncodedRowMask::from_bool_slice(&[true, false, true, true, false]);
	assert_eq!(mask.popcount().unwrap().get(), 3);
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		vec![true, false, true, true, false]
	);

	let inverted = mask.not().unwrap();
	assert_eq!(
		inverted.to_bool_vec().unwrap(),
		vec![false, true, false, false, true]
	);

	let arrow = mask.to_arrow_boolean_array().unwrap();
	assert_eq!(EncodedRowMask::from_arrow_boolean_array(&arrow), mask);

	let all = EncodedRowMask::All {
		rows: RowCount::new(5),
	};
	assert_eq!(mask.and(&all).unwrap(), mask);
	assert_eq!(mask.or(&inverted).unwrap(), all);
}

#[test]
fn evaluator_kind_for_cascade_routes_unsupported_inners_to_dense_fallback() {
	let bp = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(8),
		base: BaseValue::new(0),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&bp),
		Some(EncodingComputeEvaluatorKind::BitPacked)
	);

	let rle_plain = EncodingCascade::Rle {
		run_count: RunCount::new(1),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&rle_plain),
		Some(EncodingComputeEvaluatorKind::RlePlain)
	);

	let rle_bp = EncodingCascade::Rle {
		run_count: RunCount::new(1),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(4),
			base: BaseValue::new(0),
		}),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&rle_bp),
		Some(EncodingComputeEvaluatorKind::RleBitPacked)
	);

	let rle_lz4 = EncodingCascade::Rle {
		run_count: RunCount::new(1),
		inner: Box::new(EncodingLeafCascade::Lz4Frame),
	};
	assert_eq!(evaluator_kind_for_cascade(&rle_lz4), None);

	let dict_plain = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(16),
		dict_value_count: DictValueCount::new(4),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&dict_plain),
		Some(EncodingComputeEvaluatorKind::DictionaryPlain)
	);

	let dict_bp = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(16),
		dict_value_count: DictValueCount::new(4),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(2),
			base: BaseValue::new(0),
		}),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&dict_bp),
		Some(EncodingComputeEvaluatorKind::DictionaryBitPacked)
	);

	let dict_lz4 = EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(16),
		dict_value_count: DictValueCount::new(4),
		inner: Box::new(EncodingLeafCascade::Lz4Frame),
	};
	assert_eq!(evaluator_kind_for_cascade(&dict_lz4), None);

	let delta_bp = EncodingCascade::Delta {
		base: BaseValue::new(0),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::new(0),
		}),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&delta_bp),
		Some(EncodingComputeEvaluatorKind::Delta)
	);

	// Delta inner is byte-irrelevant at the encoder
	// (encode_delta_bytes / decode_delta_bytes ignore inner; Delta
	// dispatch is full-buffer grain regardless of inner). All three
	// variants route to the Delta evaluator.
	let delta_plain = EncodingCascade::Delta {
		base: BaseValue::new(0),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&delta_plain),
		Some(EncodingComputeEvaluatorKind::Delta)
	);

	let delta_lz4 = EncodingCascade::Delta {
		base: BaseValue::new(0),
		inner: Box::new(EncodingLeafCascade::Lz4Frame),
	};
	assert_eq!(
		evaluator_kind_for_cascade(&delta_lz4),
		Some(EncodingComputeEvaluatorKind::Delta)
	);

	assert_eq!(evaluator_kind_for_cascade(&EncodingCascade::Plain), None);
	assert_eq!(evaluator_kind_for_cascade(&EncodingCascade::Lz4Frame), None);
	assert_eq!(
		evaluator_kind_for_cascade(&EncodingCascade::Fsst {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		}),
		Some(EncodingComputeEvaluatorKind::Fsst)
	);
}

#[test]
fn delta_inner_variants_produce_byte_identical_encoded_frames() {
	// The COCD evaluator catalog accepts any Delta inner because the
	// encoder ignores the inner field (encode_delta_bytes does not take
	// inner) and Delta dispatch is full-buffer grain regardless. This
	// guard pins the byte-equivalence invariant: if the encoder ever
	// starts differentiating Delta variants by inner, this test fires
	// and forces a council review of the catalog change.
	let rows = 256_usize;
	let mut decoded = Vec::with_capacity(rows * 8);
	for row in 0..rows {
		let value = (row as i64) * 7;
		decoded.extend_from_slice(&value.to_le_bytes());
	}
	let inners = [
		EncodingLeafCascade::Plain,
		EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(8),
			base: BaseValue::new(0),
		},
		EncodingLeafCascade::Lz4Frame,
	];
	let mut prior: Option<Vec<u8>> = None;
	for inner in inners {
		let cascade = EncodingCascade::Delta {
			base: BaseValue::new(0),
			inner: Box::new(inner),
		};
		let encoded = encode_buffer(
			&decoded,
			&DataType::Int64,
			cascade,
			BufferValueCount::new(rows as u32),
			SimdPageValueCount::new(256),
		)
		.expect("delta encode");
		if let Some(reference) = prior.as_ref() {
			assert_eq!(
				&encoded.bytes, reference,
				"Delta encoded bytes must be inner-irrelevant; \
				 if this fires, the COCD catalog needs council review"
			);
		} else {
			prior = Some(encoded.bytes);
		}
	}
}

#[test]
fn encoded_selectivity_boundary_routes_at_twenty_five_percent() {
	let config = EncodingComputeConfig::default();
	assert_eq!(
		route_selectivity(
			wendelon::schema::SelectivityFractionMicros::new(250_000),
			&config
		),
		EncodingComputePlanMode::Exact
	);
	assert_eq!(
		route_selectivity(
			wendelon::schema::SelectivityFractionMicros::new(250_001),
			&config
		),
		EncodingComputePlanMode::DenseFallback
	);
}

#[test]
fn bitpacked_predicate_evaluator_matches_decoded_values() {
	let data_type = DataType::Int32;
	let values = [0_i32, 1, 2, 5, 5, 7, 9, 15];
	let decoded = values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect::<Vec<_>>();
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(4),
		base: BaseValue::new(0),
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade.clone(),
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};

	let literal = ColumnPredicateLiteralV1::Int(5);
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let mask = evaluate(&encoded.bytes, &encoding, &cascade, &atom, &coercion).unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		vec![false, false, false, true, true, false, false, false]
	);

	let literal = ColumnPredicateLiteralV1::List(vec![
		ColumnPredicateLiteralV1::Int(1),
		ColumnPredicateLiteralV1::Int(7),
	]);
	let atom = PredicateAtom {
		op: PredicateAtomOp::InList,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let mask = evaluate(&encoded.bytes, &encoding, &cascade, &atom, &coercion).unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		vec![false, true, false, false, false, true, false, false]
	);
}

#[test]
fn fsst_predicate_evaluator_matches_decoded_string_oracle() {
	let data_type = DataType::Utf8;
	let values: &[&[u8]] = &[
		b"/checkout/confirm",
		b"/cart/view",
		b"/checkout/confirm",
		b"",
		b"/account/settings",
		b"/checkout/confirm",
	];
	let (decoded, offsets) = variable_width_payload(values);
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade.clone(),
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let offset_bytes = offsets_i32_bytes(&offsets);

	let eq_literal = ColumnPredicateLiteralV1::Str(b"/checkout/confirm".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(eq_literal.clone()),
	};
	let coercion = coerce_literal(Some(&eq_literal), &data_type).unwrap();
	let mask = evaluate_fsst_predicate(
		&encoded.bytes,
		&encoding,
		&offset_bytes,
		None,
		None,
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		values
			.iter()
			.map(|value| *value == b"/checkout/confirm")
			.collect::<Vec<_>>()
	);

	let atom = PredicateAtom {
		op: PredicateAtomOp::Neq,
		literal: Some(eq_literal.clone()),
	};
	let coercion = coerce_literal(Some(&eq_literal), &data_type).unwrap();
	let mask = evaluate_fsst_predicate(
		&encoded.bytes,
		&encoding,
		&offset_bytes,
		None,
		None,
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		values
			.iter()
			.map(|value| *value != b"/checkout/confirm")
			.collect::<Vec<_>>()
	);

	let list_literal = ColumnPredicateLiteralV1::List(vec![
		ColumnPredicateLiteralV1::Str(b"/cart/view".to_vec()),
		ColumnPredicateLiteralV1::Str(Vec::new()),
	]);
	let atom = PredicateAtom {
		op: PredicateAtomOp::InList,
		literal: Some(list_literal.clone()),
	};
	let coercion = coerce_literal(Some(&list_literal), &data_type).unwrap();
	let mask = evaluate_fsst_predicate(
		&encoded.bytes,
		&encoding,
		&offset_bytes,
		None,
		None,
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		values
			.iter()
			.map(|value| *value == b"/cart/view" || value.is_empty())
			.collect::<Vec<_>>()
	);
}

#[test]
fn fsst_length_prefilter_records_zero_byte_comparisons_for_disjoint_lengths() {
	let data_type = DataType::Utf8;
	let values: &[&[u8]] = &[b"alpha", b"beta", b"gamma", b"delta"];
	let (decoded, offsets) = variable_width_payload(values);
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade,
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let literal = ColumnPredicateLiteralV1::Str(b"x".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let (mask, stats) = evaluate_fsst_predicate_with_stats(
		&encoded.bytes,
		&encoding,
		&offsets_i32_bytes(&offsets),
		None,
		None,
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(mask.to_bool_vec().unwrap(), vec![false; values.len()]);
	assert_eq!(stats.rows, values.len());
	assert_eq!(stats.length_mismatch_rows, values.len());
	assert_eq!(stats.length_match_rows, 0);
	assert_eq!(stats.byte_comparisons, 0);
	assert_eq!(stats.decoded_bytes_walked, decoded.len());
	assert_eq!(stats.decoded_bytes_skipped, decoded.len());
}

#[test]
fn fsst_streaming_reader_handles_symbol_crossing_row_boundary() {
	let data_type = DataType::Utf8;
	let values: &[&[u8]] = &[b"A", b"B", b"AB", b"AB", b"AB"];
	let (decoded, offsets) = variable_width_payload(values);
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade,
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	assert!(
		encoded.bytes.contains(&(0x80_u8)),
		"fixture should encode the row-boundary AB pair as an FSST symbol"
	);
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let literal = ColumnPredicateLiteralV1::Str(b"B".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let mask = evaluate_fsst_predicate(
		&encoded.bytes,
		&encoding,
		&offsets_i32_bytes(&offsets),
		None,
		None,
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(
		mask.to_bool_vec().unwrap(),
		vec![false, true, false, false, false]
	);
}

#[test]
fn fsst_offset_corruption_fails_closed() {
	let data_type = DataType::Utf8;
	let values: &[&[u8]] = &[b"alpha", b"beta", b"gamma"];
	let (decoded, mut offsets) = variable_width_payload(values);
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade,
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	offsets[2] = 1;
	let literal = ColumnPredicateLiteralV1::Str(b"alpha".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	assert_eq!(
		evaluate_fsst_predicate(
			&encoded.bytes,
			&encoding,
			&offsets_i32_bytes(&offsets),
			None,
			None,
			&data_type,
			&atom,
			&coercion,
		)
		.unwrap_err(),
		EncodingComputeRejectCode::KernelDecodedLengthMismatch
	);
}

#[test]
fn fsst_malformed_frames_fail_closed() {
	let data_type = DataType::Utf8;
	let literal = ColumnPredicateLiteralV1::Str(b"A".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let offset_bytes = offsets_i32_bytes(&[0, 1]);
	for frame in [
		b"BAD!".to_vec(),
		b"WFS1\x01A".to_vec(),
		b"WFS1\x00\x80".to_vec(),
		b"WFS1\x00\xff".to_vec(),
	] {
		let encoding = fsst_test_encoding(1, 1, frame.len());
		assert_eq!(
			evaluate_fsst_predicate(
				&frame,
				&encoding,
				&offset_bytes,
				None,
				None,
				&data_type,
				&atom,
				&coercion,
			)
			.unwrap_err(),
			EncodingComputeRejectCode::KernelDecodedLengthMismatch
		);
	}
}

#[test]
fn fsst_large_and_binary_types_match_decoded_oracle() {
	let values: &[&[u8]] = &[b"alpha", b"beta", b"alpha", b"", b"\xff-bin"];
	let (decoded, offsets) = variable_width_payload(values);
	let literal = ColumnPredicateLiteralV1::Str(b"alpha".to_vec());
	for data_type in [DataType::LargeUtf8, DataType::Binary, DataType::LargeBinary] {
		let cascade = EncodingCascade::Fsst {
			symbol_table_byte_len: RecordBodyByteLen::ZERO,
		};
		let encoded = encode_buffer(
			&decoded,
			&data_type,
			cascade,
			BufferValueCount::new(values.len() as u32),
			SimdPageValueCount::new(1024),
		)
		.unwrap();
		let encoding = fsst_test_encoding(decoded.len(), values.len(), encoded.bytes.len());
		let offset_bytes = if matches!(data_type, DataType::LargeUtf8 | DataType::LargeBinary) {
			offsets_i64_bytes(
				&offsets
					.iter()
					.map(|offset| i64::from(*offset))
					.collect::<Vec<_>>(),
			)
		} else {
			offsets_i32_bytes(&offsets)
		};
		let atom = PredicateAtom {
			op: PredicateAtomOp::Eq,
			literal: Some(literal.clone()),
		};
		let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
		let mask = evaluate_fsst_predicate(
			&encoded.bytes,
			&encoding,
			&offset_bytes,
			None,
			None,
			&data_type,
			&atom,
			&coercion,
		)
		.unwrap();
		assert_eq!(
			mask.to_bool_vec().unwrap(),
			values
				.iter()
				.map(|value| *value == b"alpha")
				.collect::<Vec<_>>()
		);
	}
}

#[test]
fn fsst_decodes_encoded_offsets_buffer() {
	let data_type = DataType::Utf8;
	let values: &[&[u8]] = &[b"alpha", b"beta", b"alpha"];
	let (decoded, offsets) = variable_width_payload(values);
	let cascade = EncodingCascade::Fsst {
		symbol_table_byte_len: RecordBodyByteLen::ZERO,
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade,
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let value_encoding = fsst_test_encoding(decoded.len(), values.len(), encoded.bytes.len());
	let dense_offsets = offsets_i32_bytes(&offsets);
	let offset_cascade = EncodingCascade::Lz4Frame;
	let encoded_offsets = encode_buffer(
		&dense_offsets,
		&DataType::Int32,
		offset_cascade.clone(),
		BufferValueCount::new(offsets.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let offset_encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(dense_offsets.len() as u64),
		decoded_value_count: BufferValueCount::new(offsets.len() as u32),
		encoded_range: encoded_offsets.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded_offsets.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let literal = ColumnPredicateLiteralV1::Str(b"alpha".to_vec());
	let atom = PredicateAtom {
		op: PredicateAtomOp::Eq,
		literal: Some(literal.clone()),
	};
	let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
	let mask = evaluate_fsst_predicate(
		&encoded.bytes,
		&value_encoding,
		&encoded_offsets.bytes,
		Some(&offset_encoding),
		Some(&offset_cascade),
		&data_type,
		&atom,
		&coercion,
	)
	.unwrap();
	assert_eq!(mask.to_bool_vec().unwrap(), vec![true, false, true]);
}

#[test]
fn value_dispatch_rejects_null_atoms_without_validity_bitmap() {
	let data_type = DataType::Int32;
	let values = [0_i32, 1, 2, 3];
	let decoded = values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect::<Vec<_>>();
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(2),
		base: BaseValue::new(0),
	};
	let encoded = encode_buffer(
		&decoded,
		&data_type,
		cascade.clone(),
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(1024),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let coercion = coerce_literal(None, &data_type).unwrap();
	for op in [PredicateAtomOp::IsNull, PredicateAtomOp::IsNotNull] {
		let atom = PredicateAtom { op, literal: None };
		assert_eq!(
			evaluate(&encoded.bytes, &encoding, &cascade, &atom, &coercion).unwrap_err(),
			EncodingComputeRejectCode::KernelDecodedLengthMismatch
		);
	}

	let validity = [0b0000_0101_u8];
	assert_eq!(
		evaluate_validity_bitmap(Some(&validity), RowCount::new(4), true)
			.unwrap()
			.to_bool_vec()
			.unwrap(),
		vec![false, true, false, true]
	);
	assert_eq!(
		evaluate_validity_bitmap(Some(&validity), RowCount::new(4), false)
			.unwrap()
			.to_bool_vec()
			.unwrap(),
		vec![true, false, true, false]
	);
}

#[test]
fn predicate_tree_conversion_preserves_boolean_shape() {
	let tree = PredicateTreeV1::And(vec![
		PredicateTreeV1::Comparison(ColumnPredicateV1 {
			column_index: 0,
			comparison: ColumnPredicateComparisonV1::Eq,
			literal: ColumnPredicateLiteralV1::Int(7),
		}),
		PredicateTreeV1::Or(vec![
			PredicateTreeV1::IsNotNull { column_index: 1 },
			PredicateTreeV1::Comparison(ColumnPredicateV1 {
				column_index: 2,
				comparison: ColumnPredicateComparisonV1::InList,
				literal: ColumnPredicateLiteralV1::List(vec![
					ColumnPredicateLiteralV1::Int(1),
					ColumnPredicateLiteralV1::Int(2),
				]),
			}),
		]),
	]);

	let node = encoded_predicate_node_from_tree(&tree).unwrap();
	let EncodedPredicateNode::And(branches) = node else {
		panic!("expected AND predicate node");
	};
	assert_eq!(branches.len(), 2);
	assert!(matches!(branches[0], EncodedPredicateNode::Comparison(_)));
	let EncodedPredicateNode::Or(or_branches) = &branches[1] else {
		panic!("expected OR predicate node");
	};
	assert!(matches!(or_branches[0], EncodedPredicateNode::IsNotNull(_)));
	assert!(matches!(or_branches[1], EncodedPredicateNode::InList(_)));
	assert!(encoded_predicate_node_from_tree(&PredicateTreeV1::Unsupported).is_none());
}

#[test]
fn aggregate_count_star_folds_all_or_nothing() {
	let request = aggregate_request(vec![AggregateExpression::count_star()]);
	let mask = EncodedRowMask::from_bool_slice(&[true, false, true, false, true]);
	let context = EncodedAggregateContext::new(RowCount::new(5));
	let partials = try_fold_encoded_aggregate_request(&request, &context, Some(&mask))
		.unwrap()
		.unwrap();
	assert_eq!(partials.len(), 1);
	assert_eq!(partials[0].state, 3_u64.to_be_bytes());

	let mixed = aggregate_request(vec![
		AggregateExpression::count_star(),
		AggregateExpression {
			tag: AggregateFunctionTag::SIGNED_SUM,
			column_ordinal: 0,
			parameter_block: Vec::new(),
		},
	]);
	assert!(
		try_fold_encoded_aggregate_request(&mixed, &context, Some(&mask))
			.unwrap()
			.is_none()
	);
}

#[test]
fn aggregate_integer_kernels_fold_selected_rows() {
	let values = [-3_i64, 5, 7, 1];
	let decoded = values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect::<Vec<_>>();
	let context = EncodedAggregateContext::new(RowCount::new(4)).with_decoded_column(
		0,
		DataType::Int64,
		decoded,
	);
	let mask = EncodedRowMask::from_bool_slice(&[true, true, false, true]);
	let request = aggregate_request(vec![
		aggregate_expr(AggregateFunctionTag::SIGNED_SUM, 0),
		aggregate_expr(AggregateFunctionTag::SIGNED_AVG, 0),
		aggregate_expr(AggregateFunctionTag::MIN, 0),
		aggregate_expr(AggregateFunctionTag::MAX, 0),
	]);
	let partials = try_fold_encoded_aggregate_request(&request, &context, Some(&mask))
		.unwrap()
		.unwrap();
	assert_eq!(partials.len(), 4);
	assert_eq!(
		partials[0].state,
		encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: 3,
				non_null_count: 3,
				overflow: false,
			},
		)
		.unwrap()
	);
	assert_eq!(
		partials[1].state,
		encode_aggregate_state(
			AggregateFunctionTag::SIGNED_AVG,
			&AggregateState::SignedAverageParts {
				sum: 3,
				count: 3,
				overflow: false,
			},
		)
		.unwrap()
	);
	assert_eq!(partials[2].tag, AggregateFunctionTag::MIN);
	assert_eq!(partials[3].tag, AggregateFunctionTag::MAX);

	let unsigned = [1_u64, 2, 3];
	let decoded = unsigned
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect::<Vec<_>>();
	let context = EncodedAggregateContext::new(RowCount::new(3)).with_decoded_column(
		1,
		DataType::UInt64,
		decoded,
	);
	let request = aggregate_request(vec![
		aggregate_expr(AggregateFunctionTag::UNSIGNED_SUM, 1),
		aggregate_expr(AggregateFunctionTag::UNSIGNED_AVG, 1),
	]);
	let partials = try_fold_encoded_aggregate_request(&request, &context, None)
		.unwrap()
		.unwrap();
	assert_eq!(
		partials[0].state,
		encode_aggregate_state(
			AggregateFunctionTag::UNSIGNED_SUM,
			&AggregateState::UnsignedSum {
				sum: 6,
				non_null_count: 3,
				overflow: false,
			},
		)
		.unwrap()
	);
	assert_eq!(
		partials[1].state,
		encode_aggregate_state(
			AggregateFunctionTag::UNSIGNED_AVG,
			&AggregateState::UnsignedAverageParts {
				sum: 6,
				count: 3,
				overflow: false,
			},
		)
		.unwrap()
	);
}

#[test]
fn aggregate_kernels_apply_validity_masks() {
	let values = [10_i64, 20, 30, 40];
	let decoded = values
		.iter()
		.flat_map(|value| value.to_le_bytes())
		.collect::<Vec<_>>();
	let validity = EncodedRowMask::from_bool_slice(&[true, false, true, false]);
	let selected = EncodedRowMask::from_bool_slice(&[true, true, true, false]);
	let context = EncodedAggregateContext::new(RowCount::new(4))
		.with_validity_mask(0, validity)
		.unwrap()
		.with_decoded_column(0, DataType::Int64, decoded);
	let request = aggregate_request(vec![
		aggregate_expr(AggregateFunctionTag::COUNT_NON_NULL, 0),
		aggregate_expr(AggregateFunctionTag::SIGNED_SUM, 0),
	]);

	let partials = try_fold_encoded_aggregate_request(&request, &context, Some(&selected))
		.unwrap()
		.unwrap();
	assert_eq!(
		partials[0].state,
		encode_aggregate_state(
			AggregateFunctionTag::COUNT_NON_NULL,
			&AggregateState::Count { value: 2 },
		)
		.unwrap()
	);
	assert_eq!(
		partials[1].state,
		encode_aggregate_state(
			AggregateFunctionTag::SIGNED_SUM,
			&AggregateState::SignedSum {
				sum: 40,
				non_null_count: 2,
				overflow: false,
			},
		)
		.unwrap()
	);

	let count_only = aggregate_request(vec![aggregate_expr(
		AggregateFunctionTag::COUNT_NON_NULL,
		0,
	)]);
	let count_context = EncodedAggregateContext::new(RowCount::new(4))
		.with_validity_mask(
			0,
			EncodedRowMask::from_bool_slice(&[true, false, true, false]),
		)
		.unwrap();
	let partials = try_fold_encoded_aggregate_request(&count_only, &count_context, Some(&selected))
		.unwrap()
		.unwrap();
	assert_eq!(
		partials[0].state,
		encode_aggregate_state(
			AggregateFunctionTag::COUNT_NON_NULL,
			&AggregateState::Count { value: 2 },
		)
		.unwrap()
	);
}

#[test]
fn aggregate_context_decodes_encoded_column_bytes() {
	let values = [1_u8, 2, 7, 9, 11, 13, 17, 19];
	let cascade = EncodingCascade::BitPacked {
		bits: BitsPerValue::new(5),
		base: BaseValue::new(0),
	};
	let encoded = encode_buffer(
		&values,
		&DataType::UInt8,
		cascade.clone(),
		BufferValueCount::new(values.len() as u32),
		SimdPageValueCount::new(4),
	)
	.unwrap();
	let encoding = BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(1),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(values.len() as u64),
		decoded_value_count: BufferValueCount::new(values.len() as u32),
		encoded_range: encoded.encoded_range(),
		page_value_count: SimdPageValueCount::new(4),
		page_end_offsets: encoded.page_end_offsets.clone(),
		page_stats: Vec::new(),
	};
	let context = EncodedAggregateContext::new(RowCount::new(values.len() as u64))
		.with_encoded_column(0, DataType::UInt8, &encoded.bytes, &encoding, &cascade)
		.unwrap();
	let mask =
		EncodedRowMask::from_bool_slice(&[true, false, true, false, true, false, false, true]);
	let request = aggregate_request(vec![
		aggregate_expr(AggregateFunctionTag::UNSIGNED_SUM, 0),
		aggregate_expr(AggregateFunctionTag::MAX, 0),
	]);
	let partials = try_fold_encoded_aggregate_request(&request, &context, Some(&mask))
		.unwrap()
		.unwrap();
	assert_eq!(
		partials[0].state,
		encode_aggregate_state(
			AggregateFunctionTag::UNSIGNED_SUM,
			&AggregateState::UnsignedSum {
				sum: 38,
				non_null_count: 4,
				overflow: false,
			},
		)
		.unwrap()
	);
	assert_eq!(partials[1].tag, AggregateFunctionTag::MAX);
}

#[test]
fn encoding_compute_metrics_render_bounded_labels() {
	let metrics = MetricsRegistry::default();
	metrics.observe_storage_l2_encoding_compute_plan("dense_fallback", "reader_disabled");
	metrics.observe_storage_l2_encoding_compute_kernel(
		"bit_packed",
		"bit_packed",
		Duration::from_micros(7),
	);
	metrics.observe_storage_l2_encoding_compute_rows("scanned", 5);
	metrics.observe_storage_l2_encoding_compute_mask_bytes(2);
	metrics.observe_storage_l2_encoding_compute_pages_fetched("selected", 1);
	metrics.observe_storage_l2_encoding_compute_aggregate(
		"count_star",
		"dense_fallback",
		"aggregate_unsupported",
	);
	metrics.observe_storage_l2_encoding_compute_invariant_violation("mask_length_mismatch");

	let rendered = metrics.render_prometheus();
	for expected in [
		"wendelon_storage_l2_encoding_compute_plan_total",
		"decision=\"dense_fallback\",reason=\"reader_disabled\"",
		"wendelon_storage_l2_encoding_compute_kernel_micros",
		"kernel=\"bit_packed\",cascade=\"bit_packed\"",
		"wendelon_storage_l2_encoding_compute_rows_total",
		"kind=\"scanned\"",
		"wendelon_storage_l2_encoding_compute_mask_bytes",
		"wendelon_storage_l2_encoding_compute_pages_fetched_total",
		"kind=\"selected\"",
		"wendelon_storage_l2_encoding_compute_aggregate_total",
		"aggregate=\"count_star\",decision=\"dense_fallback\",reason=\"aggregate_unsupported\"",
		"wendelon_storage_l2_encoding_compute_invariant_violations_total",
		"violation_kind=\"mask_length_mismatch\"",
	] {
		assert!(
			rendered.contains(expected),
			"missing metric output: {expected}"
		);
	}
}

fn variable_width_payload(values: &[&[u8]]) -> (Vec<u8>, Vec<i32>) {
	let mut decoded = Vec::new();
	let mut offsets = Vec::with_capacity(values.len() + 1);
	offsets.push(0_i32);
	for value in values {
		decoded.extend_from_slice(value);
		offsets.push(i32::try_from(decoded.len()).expect("test offset fits i32"));
	}
	(decoded, offsets)
}

fn offsets_i32_bytes(offsets: &[i32]) -> Vec<u8> {
	offsets
		.iter()
		.flat_map(|offset| offset.to_le_bytes())
		.collect()
}

fn offsets_i64_bytes(offsets: &[i64]) -> Vec<u8> {
	offsets
		.iter()
		.flat_map(|offset| offset.to_le_bytes())
		.collect()
}

fn fsst_test_encoding(decoded_len: usize, rows: usize, encoded_len: usize) -> BufferEncoding {
	BufferEncoding {
		row_group_index: RowGroupIndex::new(0),
		ipc_column_ordinal: IpcColumnOrdinal::new(0),
		ipc_buffer_ordinal: IpcBufferIndex::new(2),
		cascade_index: EncodingCascadeIndex::new(0),
		decoded_byte_len: RecordBodyByteLen::new(decoded_len as u64),
		decoded_value_count: BufferValueCount::new(rows as u32),
		encoded_range: EncodedBufferByteRange {
			start: EncodingPageByteOffset::ZERO,
			len: RecordBodyByteLen::new(encoded_len as u64),
		},
		page_value_count: SimdPageValueCount::new(1024),
		page_end_offsets: Vec::new(),
		page_stats: Vec::new(),
	}
}

fn aggregate_request(aggregates: Vec<AggregateExpression>) -> AggregateRequest {
	AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(58),
		tenant_id: TenantId::new(1),
		table_object_id: TableObjectId::new([8; 16]),
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
	TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x58; 16]),
		1,
		"test",
		"encoding_compute",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
	.logical_schema_fingerprint
}
