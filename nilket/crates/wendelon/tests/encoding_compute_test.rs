use arrow::datatypes::{DataType, Field, Schema};
use std::time::Duration;
use wendelon::metrics::MetricsRegistry;
use wendelon::schema::pbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, ColumnPredicateV1, PredicateTreeV1,
};
use wendelon::schema::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
	AggregateRequest, AggregateRequestId, AggregateState, BaseValue, BitsPerValue,
	BufferValueCount, Consistency, EncodingCascadeIndex, HybridTimestamp, IpcBufferIndex,
	IpcColumnOrdinal, Locality, LocalityPolicyV1, LocalityRange, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordLayoutPolicy, RowCount, RowGroupIndex,
	RowIdentityPolicy, SchemaEvolutionPolicyV1, SimdPageValueCount, TableObjectId,
	TableSchemaDescriptorV1, TenantId, encode_aggregate_state,
};
use wendelon::storage::encoding::cascade::encode_buffer;
use wendelon::schema::{DictValueCount, RunCount};
use wendelon::storage::encoding::{
	BufferEncoding, EncodedAggregateContext, EncodedAggregateKind, EncodedPredicateNode,
	EncodedRowMask, EncodingCascade, EncodingComputeConfig, EncodingComputeEvaluatorKind,
	EncodingComputePlanMode, EncodingComputeRejectCode, EncodingLeafCascade, PredicateAtom,
	PredicateAtomOp, aggregate_kind_for_tag, coerce_literal, encoded_predicate_node_from_tree,
	evaluate, evaluate_validity_bitmap, evaluator_kind_for_cascade, route_selectivity,
	try_fold_encoded_aggregate_request,
};

#[test]
fn encoding_compute_config_defaults_are_locked() {
	let config = EncodingComputeConfig::default();
	assert!(!config.enabled);
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
	assert_eq!(
		evaluator_kind_for_cascade(&EncodingCascade::Lz4Frame),
		None
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
