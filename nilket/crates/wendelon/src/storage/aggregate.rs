use crate::schema::aggregate::{
	AGG_MIN_MAX_PREFIX_BYTES, AggRejectCode, AggregateDiagnostics, AggregateExpression,
	AggregateFunction, AggregateFunctionTag, AggregateRequest, AggregateResponse, AggregateState,
	AggregateStatus, QuantileSketch, encode_aggregate_state,
};
use crate::schema::pbc::Program as PbcProgram;
use crate::schema::{
	AggregateExpressionIndex, HllPrecision, HybridTimestamp, KllK, SupersessionMetadata,
	TableObjectId, TenantId, TombstoneMetadata,
};
use crate::storage::sketch::{HllRegisters, KllSketch};
use crate::storage::stats::{
	ARROW_STATS_TAG_BINARY, ARROW_STATS_TAG_BOOLEAN, ARROW_STATS_TAG_DATE32,
	ARROW_STATS_TAG_DATE64, ARROW_STATS_TAG_INT8, ARROW_STATS_TAG_INT16, ARROW_STATS_TAG_INT32,
	ARROW_STATS_TAG_INT64, ARROW_STATS_TAG_TIMESTAMP_MICROSECOND,
	ARROW_STATS_TAG_TIMESTAMP_MILLISECOND, ARROW_STATS_TAG_TIMESTAMP_NANOSECOND,
	ARROW_STATS_TAG_UINT8, ARROW_STATS_TAG_UINT16, ARROW_STATS_TAG_UINT32, ARROW_STATS_TAG_UINT64,
	ARROW_STATS_TAG_UTF8,
};
use arrow::array::{
	Array, BinaryArray, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array,
	LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray, UInt8Array, UInt16Array,
	UInt32Array, UInt64Array,
};
use arrow::datatypes::{
	ArrowPrimitiveType, DataType, Date32Type, Date64Type, TimeUnit, TimestampMicrosecondType,
	TimestampMillisecondType, TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct AggregateExecutionResult {
	pub response: AggregateResponse,
	pub states: Vec<AggregateState>,
}

pub fn aggregate_record_batches(
	request: &AggregateRequest,
	batches: &[RecordBatch],
	exact_only: bool,
) -> Result<AggregateExecutionResult, AggRejectCode> {
	let started = Instant::now();
	request.validate_envelope(exact_only)?;
	let program = request
		.predicate_program
		.as_deref()
		.map(PbcProgram::compile)
		.transpose()
		.map_err(|_| AggRejectCode::ProgramInvalid)?;
	if let Some(first_batch) = batches.first() {
		validate_expression_types(&request.aggregates, first_batch)?;
	}
	let mut accumulators = request
		.aggregates
		.iter()
		.map(|expression| {
			let function = expression.function()?;
			Accumulator::new(
				function,
				request.tenant_id,
				request.table_object_id,
				request.request_id.get(),
			)
		})
		.collect::<Result<Vec<_>, AggRejectCode>>()?;

	let mut diagnostics = AggregateDiagnostics::default();
	for batch in batches {
		let scanned = u64::try_from(batch.num_rows()).map_err(|_| AggRejectCode::BudgetExceeded)?;
		diagnostics.rows_scanned = diagnostics.rows_scanned.saturating_add(scanned);
		if diagnostics.rows_scanned > request.budgets.max_rows.get() {
			return Err(AggRejectCode::BudgetExceeded);
		}
		let admitted = if let Some(program) = program.as_ref() {
			crate::schema::pbc::execute_program(program, batch)
				.map_err(|_| AggRejectCode::ProgramInvalid)?
		} else {
			batch.clone()
		};
		let admitted_rows =
			u64::try_from(admitted.num_rows()).map_err(|_| AggRejectCode::BudgetExceeded)?;
		diagnostics.rows_admitted = diagnostics.rows_admitted.saturating_add(admitted_rows);
		diagnostics.rows_filtered_out = diagnostics
			.rows_filtered_out
			.saturating_add(scanned.saturating_sub(admitted_rows));
		for accumulator in &mut accumulators {
			accumulator.fold_batch(&admitted)?;
		}
	}

	let states = accumulators
		.iter()
		.map(Accumulator::state)
		.collect::<Vec<_>>();
	let mut partials = Vec::with_capacity(states.len());
	let mut partial_bytes = 0_u64;
	let mut overflow = false;
	let mut has_truncated_minmax = false;
	for (index, (expression, state)) in request.aggregates.iter().zip(states.iter()).enumerate() {
		overflow |= state.overflowed();
		has_truncated_minmax |= state.has_truncated_minmax();
		let state_bytes = encode_aggregate_state(expression.tag, state)
			.map_err(|_| AggRejectCode::BudgetExceeded)?;
		partial_bytes = partial_bytes.saturating_add(state_bytes.len() as u64);
		if partial_bytes > request.budgets.max_partial_state_bytes.get() {
			return Err(AggRejectCode::PartialStateTooLarge);
		}
		partials.push(
			crate::schema::AggregatePartial::new(
				AggregateExpressionIndex::new(index as u16),
				expression.tag,
				state_bytes,
			)
			.map_err(|_| AggRejectCode::PartialStateTooLarge)?,
		);
	}
	let status = if overflow {
		AggregateStatus::NumericOverflow
	} else if has_truncated_minmax {
		AggregateStatus::Partial
	} else {
		AggregateStatus::Success
	};
	let response = AggregateResponse {
		format_version: request.format_version,
		request_id: request.request_id,
		status,
		reject_code: if overflow {
			Some(AggRejectCode::NumericOverflow)
		} else {
			None
		},
		table_object_id: request.table_object_id,
		schema_generation: request.schema_generation,
		logical_schema_fingerprint: request.logical_schema_fingerprint,
		aggregate_expression_digest: request.expression_digest(),
		as_of_hlc: request.as_of_hlc,
		max_hlc: request.as_of_hlc,
		partials,
		diagnostics,
	};
	crate::metrics::global().observe_agg_execution(
		response.diagnostics.rows_scanned,
		response.diagnostics.rows_admitted,
		started.elapsed(),
		partial_bytes,
		overflow,
	);
	Ok(AggregateExecutionResult { response, states })
}

pub fn merge_aggregate_responses(
	request: &AggregateRequest,
	responses: &[AggregateResponse],
) -> Result<AggregateResponse, AggRejectCode> {
	let started = Instant::now();
	request.validate_envelope(false)?;
	if responses.is_empty() {
		return Ok(aggregate_record_batches(request, &[], false)?.response);
	}

	let mut merged_states = vec![AggregateState::Empty; request.aggregates.len()];
	let mut diagnostics = AggregateDiagnostics::default();
	let mut max_hlc = HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	};
	let mut saw_numeric_overflow = false;
	let mut saw_partial_response = false;
	let mut has_truncated_minmax = false;
	let mut partials_seen = 0_u64;
	let mut peer_partials_merged = 0_u32;
	let mut peer_partial_bytes = 0_u64;

	for (response_index, response) in responses.iter().enumerate() {
		if response.format_version != request.format_version
			|| !response_crossbind_matches(request, response)
		{
			return Err(AggRejectCode::PartialCrossbindFailed);
		}
		match response.status {
			AggregateStatus::Success | AggregateStatus::Partial => {}
			AggregateStatus::NumericOverflow => saw_numeric_overflow = true,
			AggregateStatus::Rejected => {
				return Err(response
					.reject_code
					.unwrap_or(AggRejectCode::PartialCrossbindFailed));
			}
		}
		saw_partial_response |= response.status == AggregateStatus::Partial;
		if response.partials.len() != request.aggregates.len() {
			return Err(AggRejectCode::PartialCrossbindFailed);
		}
		if response.max_hlc > max_hlc {
			max_hlc = response.max_hlc;
		}
		add_aggregate_diagnostics(&mut diagnostics, &response.diagnostics);

		for partial in &response.partials {
			partials_seen = partials_seen.saturating_add(1);
			if response_index > 0 {
				peer_partials_merged = peer_partials_merged.saturating_add(1);
				peer_partial_bytes = peer_partial_bytes.saturating_add(partial.state.len() as u64);
			}
			if partial.state_length.get() as usize != partial.state.len() {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			let index = partial.expression_index.get() as usize;
			let Some(expression) = request.aggregates.get(index) else {
				return Err(AggRejectCode::PartialCrossbindFailed);
			};
			if partial.tag != expression.tag {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			let state = decode_aggregate_state(partial.tag, &partial.state)?;
			merge_aggregate_state(&mut merged_states[index], expression.tag, state)?;
			saw_numeric_overflow |= merged_states[index].overflowed();
			has_truncated_minmax |= merged_states[index].has_truncated_minmax();
		}
	}

	let mut partials = Vec::with_capacity(merged_states.len());
	let mut partial_bytes = 0_u64;
	for (index, (expression, state)) in request
		.aggregates
		.iter()
		.zip(merged_states.iter())
		.enumerate()
	{
		saw_numeric_overflow |= state.overflowed();
		has_truncated_minmax |= state.has_truncated_minmax();
		let state_bytes = encode_aggregate_state(expression.tag, state)
			.map_err(|_| AggRejectCode::PartialStateTooLarge)?;
		partial_bytes = partial_bytes.saturating_add(state_bytes.len() as u64);
		if partial_bytes > request.budgets.max_partial_state_bytes.get() {
			return Err(AggRejectCode::PartialStateTooLarge);
		}
		partials.push(
			crate::schema::AggregatePartial::new(
				AggregateExpressionIndex::new(index as u16),
				expression.tag,
				state_bytes,
			)
			.map_err(|_| AggRejectCode::PartialStateTooLarge)?,
		);
	}

	let merge_micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
	diagnostics.peer_partials_merged = diagnostics
		.peer_partials_merged
		.saturating_add(peer_partials_merged);
	diagnostics.peer_partial_bytes = diagnostics
		.peer_partial_bytes
		.saturating_add(peer_partial_bytes);
	diagnostics.merge_micros = diagnostics.merge_micros.saturating_add(merge_micros);
	crate::metrics::global().observe_agg_merge(partials_seen, started.elapsed());

	Ok(AggregateResponse {
		format_version: request.format_version,
		request_id: request.request_id,
		status: if saw_numeric_overflow {
			AggregateStatus::NumericOverflow
		} else if saw_partial_response || has_truncated_minmax {
			AggregateStatus::Partial
		} else {
			AggregateStatus::Success
		},
		reject_code: if saw_numeric_overflow {
			Some(AggRejectCode::NumericOverflow)
		} else {
			None
		},
		table_object_id: request.table_object_id,
		schema_generation: request.schema_generation,
		logical_schema_fingerprint: request.logical_schema_fingerprint,
		aggregate_expression_digest: request.expression_digest(),
		as_of_hlc: request.as_of_hlc,
		max_hlc,
		partials,
		diagnostics,
	})
}

fn add_aggregate_diagnostics(left: &mut AggregateDiagnostics, right: &AggregateDiagnostics) {
	left.slabs_consulted = left.slabs_consulted.saturating_add(right.slabs_consulted);
	left.slabs_pruned_by_csb = left
		.slabs_pruned_by_csb
		.saturating_add(right.slabs_pruned_by_csb);
	left.slabs_answered_from_csb = left
		.slabs_answered_from_csb
		.saturating_add(right.slabs_answered_from_csb);
	left.slabs_stats_unavailable = left
		.slabs_stats_unavailable
		.saturating_add(right.slabs_stats_unavailable);
	left.rows_scanned = left.rows_scanned.saturating_add(right.rows_scanned);
	left.rows_admitted = left.rows_admitted.saturating_add(right.rows_admitted);
	left.rows_filtered_out = left
		.rows_filtered_out
		.saturating_add(right.rows_filtered_out);
	left.peer_partials_merged = left
		.peer_partials_merged
		.saturating_add(right.peer_partials_merged);
	left.peer_partial_bytes = left
		.peer_partial_bytes
		.saturating_add(right.peer_partial_bytes);
	left.merge_micros = left.merge_micros.saturating_add(right.merge_micros);
}

fn decode_aggregate_state(
	tag: AggregateFunctionTag,
	bytes: &[u8],
) -> Result<AggregateState, AggRejectCode> {
	if bytes.is_empty() {
		return Ok(AggregateState::Empty);
	}
	match tag {
		AggregateFunctionTag::COUNT_STAR | AggregateFunctionTag::COUNT_NON_NULL => {
			if bytes.len() != 8 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::Count {
				value: u64::from_be_bytes(bytes.try_into().expect("count state len")),
			})
		}
		AggregateFunctionTag::SIGNED_SUM => {
			if bytes.len() != 17 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::SignedSum {
				sum: decode_signed_i128_comparable(&bytes[..16]),
				non_null_count: 1,
				overflow: bytes[16] != 0,
			})
		}
		AggregateFunctionTag::UNSIGNED_SUM => {
			if bytes.len() != 17 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::UnsignedSum {
				sum: u128::from_be_bytes(bytes[..16].try_into().expect("u128 state len")),
				non_null_count: 1,
				overflow: bytes[16] != 0,
			})
		}
		AggregateFunctionTag::MIN | AggregateFunctionTag::MAX => {
			if bytes.len() < 5 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			let len = u32::from_be_bytes(bytes[..4].try_into().expect("min/max len")) as usize;
			if bytes.len() != 4_usize.saturating_add(len).saturating_add(1) {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::MinMax {
				value: Some(bytes[4..4 + len].to_vec()),
				arrow_type_tag: 0,
				truncated: bytes[4 + len] != 0,
			})
		}
		AggregateFunctionTag::SIGNED_AVG => {
			if bytes.len() != 25 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::SignedAverageParts {
				sum: i128::from_be_bytes(bytes[..16].try_into().expect("avg sum len")),
				count: u64::from_be_bytes(bytes[16..24].try_into().expect("avg count len")),
				overflow: bytes[24] != 0,
			})
		}
		AggregateFunctionTag::UNSIGNED_AVG => {
			if bytes.len() != 25 {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			Ok(AggregateState::UnsignedAverageParts {
				sum: u128::from_be_bytes(bytes[..16].try_into().expect("avg sum len")),
				count: u64::from_be_bytes(bytes[16..24].try_into().expect("avg count len")),
				overflow: bytes[24] != 0,
			})
		}
		AggregateFunctionTag::APPROX_DISTINCT => {
			let precision =
				HllPrecision::new(bytes[0]).map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
			let registers = bytes[1..].to_vec();
			let hll = HllRegisters::from_registers(precision, registers)
				.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
			Ok(hll.into_state())
		}
		AggregateFunctionTag::QUANTILE_KLL => decode_kll_state(bytes),
		_ => Err(AggRejectCode::UnsupportedFunction),
	}
}

fn decode_kll_state(bytes: &[u8]) -> Result<AggregateState, AggRejectCode> {
	let mut cursor = 0_usize;
	let arrow_type_tag = read_u8(bytes, &mut cursor)?;
	let value_width =
		kll_value_width(arrow_type_tag).ok_or(AggRejectCode::PartialCrossbindFailed)?;
	let k = KllK::new(read_u32(bytes, &mut cursor)?);
	validate_kll_k_for_partial(k)?;
	let total_item_count = read_u64(bytes, &mut cursor)?;
	let level_count = read_u32(bytes, &mut cursor)? as usize;
	let mut levels = Vec::with_capacity(level_count);
	for _ in 0..level_count {
		let level_size = read_u32(bytes, &mut cursor)? as usize;
		let mut level = Vec::with_capacity(level_size);
		for _ in 0..level_size {
			let end = cursor
				.checked_add(value_width)
				.ok_or(AggRejectCode::PartialCrossbindFailed)?;
			if end > bytes.len() {
				return Err(AggRejectCode::PartialCrossbindFailed);
			}
			level.push(bytes[cursor..end].to_vec());
			cursor = end;
		}
		levels.push(level);
	}
	let compaction_seed = read_u32(bytes, &mut cursor)?;
	if cursor != bytes.len() {
		return Err(AggRejectCode::PartialCrossbindFailed);
	}
	Ok(AggregateState::Kll {
		arrow_type_tag,
		k,
		total_item_count,
		levels,
		compaction_seed,
	})
}

fn validate_kll_k_for_partial(k: KllK) -> Result<(), AggRejectCode> {
	crate::schema::aggregate::validate_kll_k(k.get())
		.map_err(|_| AggRejectCode::PartialCrossbindFailed)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, AggRejectCode> {
	let value = *bytes
		.get(*cursor)
		.ok_or(AggRejectCode::PartialCrossbindFailed)?;
	*cursor = (*cursor).saturating_add(1);
	Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, AggRejectCode> {
	let end = cursor
		.checked_add(4)
		.ok_or(AggRejectCode::PartialCrossbindFailed)?;
	if end > bytes.len() {
		return Err(AggRejectCode::PartialCrossbindFailed);
	}
	let value = u32::from_be_bytes(bytes[*cursor..end].try_into().expect("u32 state len"));
	*cursor = end;
	Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, AggRejectCode> {
	let end = cursor
		.checked_add(8)
		.ok_or(AggRejectCode::PartialCrossbindFailed)?;
	if end > bytes.len() {
		return Err(AggRejectCode::PartialCrossbindFailed);
	}
	let value = u64::from_be_bytes(bytes[*cursor..end].try_into().expect("u64 state len"));
	*cursor = end;
	Ok(value)
}

fn decode_signed_i128_comparable(bytes: &[u8]) -> i128 {
	let comparable = u128::from_be_bytes(bytes.try_into().expect("signed sum state len"));
	(comparable ^ (1_u128 << 127)) as i128
}

fn merge_aggregate_state(
	left: &mut AggregateState,
	tag: AggregateFunctionTag,
	right: AggregateState,
) -> Result<(), AggRejectCode> {
	if matches!(right, AggregateState::Empty) {
		return Ok(());
	}
	if matches!(left, AggregateState::Empty) {
		*left = right;
		return Ok(());
	}
	match (tag, left, right) {
		(
			AggregateFunctionTag::COUNT_STAR | AggregateFunctionTag::COUNT_NON_NULL,
			AggregateState::Count { value },
			AggregateState::Count { value: rhs },
		) => {
			*value = value
				.checked_add(rhs)
				.ok_or(AggRejectCode::BudgetExceeded)?;
		}
		(
			AggregateFunctionTag::SIGNED_SUM,
			AggregateState::SignedSum {
				sum,
				non_null_count,
				overflow,
			},
			AggregateState::SignedSum {
				sum: rhs,
				overflow: rhs_overflow,
				..
			},
		) => {
			add_i128(sum, rhs, overflow);
			*overflow |= rhs_overflow;
			*non_null_count = 1;
		}
		(
			AggregateFunctionTag::UNSIGNED_SUM,
			AggregateState::UnsignedSum {
				sum,
				non_null_count,
				overflow,
			},
			AggregateState::UnsignedSum {
				sum: rhs,
				overflow: rhs_overflow,
				..
			},
		) => {
			add_u128(sum, rhs, overflow);
			*overflow |= rhs_overflow;
			*non_null_count = 1;
		}
		(
			AggregateFunctionTag::MIN,
			AggregateState::MinMax {
				value, truncated, ..
			},
			AggregateState::MinMax {
				value: rhs,
				truncated: rhs_truncated,
				..
			},
		) => merge_min_max(value, truncated, rhs, rhs_truncated, true),
		(
			AggregateFunctionTag::MAX,
			AggregateState::MinMax {
				value, truncated, ..
			},
			AggregateState::MinMax {
				value: rhs,
				truncated: rhs_truncated,
				..
			},
		) => merge_min_max(value, truncated, rhs, rhs_truncated, false),
		(
			AggregateFunctionTag::SIGNED_AVG,
			AggregateState::SignedAverageParts {
				sum,
				count,
				overflow,
			},
			AggregateState::SignedAverageParts {
				sum: rhs_sum,
				count: rhs_count,
				overflow: rhs_overflow,
			},
		) => {
			add_i128(sum, rhs_sum, overflow);
			*overflow |= rhs_overflow;
			*count = count
				.checked_add(rhs_count)
				.ok_or(AggRejectCode::BudgetExceeded)?;
		}
		(
			AggregateFunctionTag::UNSIGNED_AVG,
			AggregateState::UnsignedAverageParts {
				sum,
				count,
				overflow,
			},
			AggregateState::UnsignedAverageParts {
				sum: rhs_sum,
				count: rhs_count,
				overflow: rhs_overflow,
			},
		) => {
			add_u128(sum, rhs_sum, overflow);
			*overflow |= rhs_overflow;
			*count = count
				.checked_add(rhs_count)
				.ok_or(AggRejectCode::BudgetExceeded)?;
		}
		(
			AggregateFunctionTag::APPROX_DISTINCT,
			AggregateState::Hll {
				precision,
				registers,
			},
			AggregateState::Hll {
				precision: rhs_precision,
				registers: rhs_registers,
			},
		) => {
			let mut merged = HllRegisters::from_registers(*precision, std::mem::take(registers))
				.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
			let right = HllRegisters::from_registers(rhs_precision, rhs_registers)
				.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
			merged
				.merge(&right)
				.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
			*registers = merged.registers().to_vec();
			*precision = merged.precision();
		}
		(AggregateFunctionTag::QUANTILE_KLL, left_state, right_state) => {
			*left_state = merge_kll_states(left_state.clone(), right_state)?;
		}
		_ => return Err(AggRejectCode::PartialCrossbindFailed),
	}
	Ok(())
}

fn merge_min_max(
	left: &mut Option<Vec<u8>>,
	left_truncated: &mut bool,
	right: Option<Vec<u8>>,
	right_truncated: bool,
	choose_min: bool,
) {
	let Some(right) = right else {
		return;
	};
	match left {
		None => {
			*left = Some(right);
			*left_truncated = right_truncated;
		}
		Some(current) => {
			let replace = if choose_min {
				right.as_slice() < current.as_slice()
			} else {
				right.as_slice() > current.as_slice()
			};
			if replace {
				*current = right;
				*left_truncated = right_truncated;
			} else if right.as_slice() == current.as_slice() {
				*left_truncated |= right_truncated;
			}
		}
	}
}

fn merge_kll_states(
	left: AggregateState,
	right: AggregateState,
) -> Result<AggregateState, AggRejectCode> {
	let AggregateState::Kll {
		arrow_type_tag,
		k,
		total_item_count,
		levels,
		compaction_seed,
	} = left
	else {
		return Err(AggRejectCode::PartialCrossbindFailed);
	};
	let AggregateState::Kll {
		arrow_type_tag: rhs_arrow_type_tag,
		k: rhs_k,
		total_item_count: rhs_total_item_count,
		levels: rhs_levels,
		compaction_seed: rhs_compaction_seed,
	} = right
	else {
		return Err(AggRejectCode::PartialCrossbindFailed);
	};
	let mut left_sketch = KllSketch::from_retained_values(
		arrow_type_tag,
		k,
		total_item_count,
		levels.into_iter().flatten().collect(),
		compaction_seed,
	)
	.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
	let right_sketch = KllSketch::from_retained_values(
		rhs_arrow_type_tag,
		rhs_k,
		rhs_total_item_count,
		rhs_levels.into_iter().flatten().collect(),
		rhs_compaction_seed,
	)
	.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
	left_sketch
		.merge(&right_sketch)
		.map_err(|_| AggRejectCode::PartialCrossbindFailed)?;
	Ok(left_sketch.into_state())
}

fn kll_value_width(arrow_type_tag: u8) -> Option<usize> {
	match arrow_type_tag {
		ARROW_STATS_TAG_INT8 | ARROW_STATS_TAG_UINT8 => Some(1),
		ARROW_STATS_TAG_INT16 | ARROW_STATS_TAG_UINT16 => Some(2),
		ARROW_STATS_TAG_INT32 | ARROW_STATS_TAG_UINT32 | ARROW_STATS_TAG_DATE32 => Some(4),
		ARROW_STATS_TAG_INT64
		| ARROW_STATS_TAG_UINT64
		| ARROW_STATS_TAG_TIMESTAMP_MILLISECOND
		| ARROW_STATS_TAG_TIMESTAMP_MICROSECOND
		| ARROW_STATS_TAG_TIMESTAMP_NANOSECOND
		| ARROW_STATS_TAG_DATE64 => Some(8),
		_ => None,
	}
}

#[derive(Clone, Debug)]
enum Accumulator {
	CountStar {
		value: u64,
	},
	CountNonNull {
		column: usize,
		value: u64,
	},
	SignedSum {
		column: usize,
		sum: i128,
		non_null_count: u64,
		overflow: bool,
	},
	UnsignedSum {
		column: usize,
		sum: u128,
		non_null_count: u64,
		overflow: bool,
	},
	Min {
		column: usize,
		arrow_type_tag: u8,
		value: Option<Vec<u8>>,
		truncated: bool,
	},
	Max {
		column: usize,
		arrow_type_tag: u8,
		value: Option<Vec<u8>>,
		truncated: bool,
	},
	SignedAverageParts {
		column: usize,
		sum: i128,
		count: u64,
		overflow: bool,
	},
	UnsignedAverageParts {
		column: usize,
		sum: u128,
		count: u64,
		overflow: bool,
	},
	ApproxDistinct {
		column: usize,
		hll: HllRegisters,
	},
	QuantileKll {
		column: usize,
		k: KllK,
		compaction_seed: u32,
		sketch: Option<KllSketch>,
	},
}

impl Accumulator {
	fn new(
		function: AggregateFunction,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		request_id: u128,
	) -> Result<Self, AggRejectCode> {
		Ok(match function {
			AggregateFunction::CountStar => Self::CountStar { value: 0 },
			AggregateFunction::CountNonNull { column } => Self::CountNonNull {
				column: column as usize,
				value: 0,
			},
			AggregateFunction::SignedSum { column } => Self::SignedSum {
				column: column as usize,
				sum: 0,
				non_null_count: 0,
				overflow: false,
			},
			AggregateFunction::UnsignedSum { column } => Self::UnsignedSum {
				column: column as usize,
				sum: 0,
				non_null_count: 0,
				overflow: false,
			},
			AggregateFunction::Min { column } => Self::Min {
				column: column as usize,
				arrow_type_tag: 0,
				value: None,
				truncated: false,
			},
			AggregateFunction::Max { column } => Self::Max {
				column: column as usize,
				arrow_type_tag: 0,
				value: None,
				truncated: false,
			},
			AggregateFunction::SignedAverageParts { column } => Self::SignedAverageParts {
				column: column as usize,
				sum: 0,
				count: 0,
				overflow: false,
			},
			AggregateFunction::UnsignedAverageParts { column } => Self::UnsignedAverageParts {
				column: column as usize,
				sum: 0,
				count: 0,
				overflow: false,
			},
			AggregateFunction::ApproxDistinct { column, precision } => Self::ApproxDistinct {
				column: column as usize,
				hll: HllRegisters::new(precision),
			},
			AggregateFunction::Quantile { column, sketch } => {
				let QuantileSketch::Kll { k } = sketch;
				Self::QuantileKll {
					column: column as usize,
					k,
					compaction_seed: kll_compaction_seed(tenant_id, table_object_id, request_id),
					sketch: None,
				}
			}
		})
	}

	fn fold_batch(&mut self, batch: &RecordBatch) -> Result<(), AggRejectCode> {
		match self {
			Self::CountStar { value } => {
				*value = value.saturating_add(batch.num_rows() as u64);
				Ok(())
			}
			Self::CountNonNull { column, value } => {
				let array = column_array(batch, *column)?;
				let non_null = array.len().saturating_sub(array.null_count()) as u64;
				*value = value.saturating_add(non_null);
				Ok(())
			}
			Self::SignedSum {
				column,
				sum,
				non_null_count,
				overflow,
			} => fold_signed_numeric(batch, *column, |value| {
				*non_null_count = non_null_count.saturating_add(1);
				add_i128(sum, value, overflow);
			}),
			Self::UnsignedSum {
				column,
				sum,
				non_null_count,
				overflow,
			} => fold_unsigned_numeric(batch, *column, |value| {
				*non_null_count = non_null_count.saturating_add(1);
				add_u128(sum, value, overflow);
			}),
			Self::Min {
				column,
				arrow_type_tag,
				value,
				truncated,
			} => fold_canonical_values(batch, *column, true, |candidate| {
				*arrow_type_tag = candidate.arrow_type_tag;
				if value
					.as_deref()
					.map(|current| candidate.bytes.as_slice() < current)
					.unwrap_or(true)
				{
					*value = Some(candidate.bytes);
					*truncated = candidate.truncated;
				}
				Ok(())
			}),
			Self::Max {
				column,
				arrow_type_tag,
				value,
				truncated,
			} => fold_canonical_values(batch, *column, true, |candidate| {
				*arrow_type_tag = candidate.arrow_type_tag;
				if value
					.as_deref()
					.map(|current| candidate.bytes.as_slice() > current)
					.unwrap_or(true)
				{
					*value = Some(candidate.bytes);
					*truncated = candidate.truncated;
				}
				Ok(())
			}),
			Self::SignedAverageParts {
				column,
				sum,
				count,
				overflow,
			} => fold_signed_numeric(batch, *column, |value| {
				*count = count.saturating_add(1);
				add_i128(sum, value, overflow);
			}),
			Self::UnsignedAverageParts {
				column,
				sum,
				count,
				overflow,
			} => fold_unsigned_numeric(batch, *column, |value| {
				*count = count.saturating_add(1);
				add_u128(sum, value, overflow);
			}),
			Self::ApproxDistinct { column, hll } => {
				fold_canonical_values(batch, *column, false, |candidate| {
					hll.insert(&candidate.bytes);
					Ok(())
				})
			}
			Self::QuantileKll {
				column,
				k,
				compaction_seed,
				sketch,
			} => {
				let arrow_type_tag =
					agg_arrow_type_tag(column_array(batch, *column)?.data_type(), false)
						.ok_or(AggRejectCode::TypeUnsupported)?;
				if sketch.is_none() {
					*sketch = Some(
						KllSketch::new(arrow_type_tag, *k, *compaction_seed)
							.map_err(|_| AggRejectCode::SketchParameterInvalid)?,
					);
				}
				let sketch = sketch.as_mut().expect("KLL sketch initialized");
				fold_kll_values(batch, *column, |value| {
					sketch.insert(value);
					Ok(())
				})
			}
		}
	}

	fn state(&self) -> AggregateState {
		match self {
			Self::CountStar { value } | Self::CountNonNull { value, .. } => {
				AggregateState::Count { value: *value }
			}
			Self::SignedSum {
				sum,
				non_null_count,
				overflow,
				..
			} => AggregateState::SignedSum {
				sum: *sum,
				non_null_count: *non_null_count,
				overflow: *overflow,
			},
			Self::UnsignedSum {
				sum,
				non_null_count,
				overflow,
				..
			} => AggregateState::UnsignedSum {
				sum: *sum,
				non_null_count: *non_null_count,
				overflow: *overflow,
			},
			Self::Min {
				arrow_type_tag,
				value,
				truncated,
				..
			}
			| Self::Max {
				arrow_type_tag,
				value,
				truncated,
				..
			} => AggregateState::MinMax {
				value: value.clone(),
				arrow_type_tag: *arrow_type_tag,
				truncated: *truncated,
			},
			Self::SignedAverageParts {
				sum,
				count,
				overflow,
				..
			} => AggregateState::SignedAverageParts {
				sum: *sum,
				count: *count,
				overflow: *overflow,
			},
			Self::UnsignedAverageParts {
				sum,
				count,
				overflow,
				..
			} => AggregateState::UnsignedAverageParts {
				sum: *sum,
				count: *count,
				overflow: *overflow,
			},
			Self::ApproxDistinct { hll, .. } => hll.clone().into_state(),
			Self::QuantileKll { sketch, .. } => sketch
				.clone()
				.map(KllSketch::into_state)
				.unwrap_or(AggregateState::Empty),
		}
	}
}

fn column_array(batch: &RecordBatch, column: usize) -> Result<&dyn Array, AggRejectCode> {
	batch
		.columns()
		.get(column)
		.map(|array| array.as_ref())
		.ok_or(AggRejectCode::ColumnUnknown)
}

fn fold_signed_numeric<F>(
	batch: &RecordBatch,
	column: usize,
	mut emit: F,
) -> Result<(), AggRejectCode>
where
	F: FnMut(i128),
{
	let array = column_array(batch, column)?;
	match array.data_type() {
		DataType::Int8 => emit_primitive_i128::<arrow::datatypes::Int8Type, _>(array, &mut emit),
		DataType::Int16 => emit_primitive_i128::<arrow::datatypes::Int16Type, _>(array, &mut emit),
		DataType::Int32 => emit_primitive_i128::<arrow::datatypes::Int32Type, _>(array, &mut emit),
		DataType::Int64 => emit_primitive_i128::<arrow::datatypes::Int64Type, _>(array, &mut emit),
		_ => Err(AggRejectCode::TypeUnsupported),
	}
}

fn fold_unsigned_numeric<F>(
	batch: &RecordBatch,
	column: usize,
	mut emit: F,
) -> Result<(), AggRejectCode>
where
	F: FnMut(u128),
{
	let array = column_array(batch, column)?;
	match array.data_type() {
		DataType::UInt8 => emit_primitive_u128::<arrow::datatypes::UInt8Type, _>(array, &mut emit),
		DataType::UInt16 => {
			emit_primitive_u128::<arrow::datatypes::UInt16Type, _>(array, &mut emit)
		}
		DataType::UInt32 => {
			emit_primitive_u128::<arrow::datatypes::UInt32Type, _>(array, &mut emit)
		}
		DataType::UInt64 => {
			emit_primitive_u128::<arrow::datatypes::UInt64Type, _>(array, &mut emit)
		}
		_ => Err(AggRejectCode::TypeUnsupported),
	}
}

fn emit_primitive_i128<T, F>(array: &dyn Array, emit: &mut F) -> Result<(), AggRejectCode>
where
	T: ArrowPrimitiveType,
	T::Native: Copy + Into<i128>,
	F: FnMut(i128),
{
	let typed = downcast_array::<PrimitiveArray<T>>(array)?;
	for row in 0..typed.len() {
		if typed.is_valid(row) {
			emit(typed.value(row).into());
		}
	}
	Ok(())
}

fn emit_primitive_u128<T, F>(array: &dyn Array, emit: &mut F) -> Result<(), AggRejectCode>
where
	T: ArrowPrimitiveType,
	T::Native: Copy + Into<u128>,
	F: FnMut(u128),
{
	let typed = downcast_array::<PrimitiveArray<T>>(array)?;
	for row in 0..typed.len() {
		if typed.is_valid(row) {
			emit(typed.value(row).into());
		}
	}
	Ok(())
}

fn add_i128(sum: &mut i128, value: i128, overflow: &mut bool) {
	if let Some(next) = sum.checked_add(value) {
		*sum = next;
	} else {
		*sum = sum.saturating_add(value);
		*overflow = true;
		crate::metrics::global().observe_agg_int_overflow();
	}
}

fn add_u128(sum: &mut u128, value: u128, overflow: &mut bool) {
	if let Some(next) = sum.checked_add(value) {
		*sum = next;
	} else {
		*sum = sum.saturating_add(value);
		*overflow = true;
		crate::metrics::global().observe_agg_int_overflow();
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalValue {
	arrow_type_tag: u8,
	bytes: Vec<u8>,
	truncated: bool,
}

fn fold_canonical_values<F>(
	batch: &RecordBatch,
	column: usize,
	truncate_prefix: bool,
	mut emit: F,
) -> Result<(), AggRejectCode>
where
	F: FnMut(CanonicalValue) -> Result<(), AggRejectCode>,
{
	let array = column_array(batch, column)?;
	let data_type = array.data_type();
	if agg_arrow_type_tag(data_type, true).is_none() {
		return Err(AggRejectCode::TypeUnsupported);
	}
	for row in 0..array.len() {
		if let Some(value) = canonical_value(data_type, array, row, truncate_prefix)? {
			emit(value)?;
		}
	}
	Ok(())
}

fn fold_kll_values<F>(batch: &RecordBatch, column: usize, mut emit: F) -> Result<(), AggRejectCode>
where
	F: FnMut(Vec<u8>) -> Result<(), AggRejectCode>,
{
	let array = column_array(batch, column)?;
	let data_type = array.data_type();
	if agg_arrow_type_tag(data_type, false).is_none() {
		return Err(AggRejectCode::TypeUnsupported);
	}
	for row in 0..array.len() {
		if let Some(value) = canonical_value(data_type, array, row, false)? {
			emit(value.bytes)?;
		}
	}
	Ok(())
}

pub fn agg_arrow_type_tag(data_type: &DataType, include_bool_and_bytes: bool) -> Option<u8> {
	match data_type {
		DataType::Int8 => Some(ARROW_STATS_TAG_INT8),
		DataType::Int16 => Some(ARROW_STATS_TAG_INT16),
		DataType::Int32 => Some(ARROW_STATS_TAG_INT32),
		DataType::Int64 => Some(ARROW_STATS_TAG_INT64),
		DataType::UInt8 => Some(ARROW_STATS_TAG_UINT8),
		DataType::UInt16 => Some(ARROW_STATS_TAG_UINT16),
		DataType::UInt32 => Some(ARROW_STATS_TAG_UINT32),
		DataType::UInt64 => Some(ARROW_STATS_TAG_UINT64),
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			Some(ARROW_STATS_TAG_TIMESTAMP_MILLISECOND)
		}
		DataType::Timestamp(TimeUnit::Microsecond, _) => {
			Some(ARROW_STATS_TAG_TIMESTAMP_MICROSECOND)
		}
		DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(ARROW_STATS_TAG_TIMESTAMP_NANOSECOND),
		DataType::Date32 => Some(ARROW_STATS_TAG_DATE32),
		DataType::Date64 => Some(ARROW_STATS_TAG_DATE64),
		DataType::Boolean if include_bool_and_bytes => Some(ARROW_STATS_TAG_BOOLEAN),
		DataType::Utf8 | DataType::LargeUtf8 if include_bool_and_bytes => {
			Some(ARROW_STATS_TAG_UTF8)
		}
		DataType::Binary | DataType::LargeBinary if include_bool_and_bytes => {
			Some(ARROW_STATS_TAG_BINARY)
		}
		_ => None,
	}
}

fn canonical_value(
	data_type: &DataType,
	array: &dyn Array,
	row: usize,
	truncate_prefix: bool,
) -> Result<Option<CanonicalValue>, AggRejectCode> {
	let Some(arrow_type_tag) = agg_arrow_type_tag(data_type, true) else {
		return Err(AggRejectCode::TypeUnsupported);
	};
	if !array.is_valid(row) {
		return Ok(None);
	}
	let mut truncated = false;
	let mut bytes = match data_type {
		DataType::Int8 => {
			encode_signed_value(downcast_array::<Int8Array>(array)?.value(row) as i128, 1)
		}
		DataType::Int16 => {
			encode_signed_value(downcast_array::<Int16Array>(array)?.value(row) as i128, 2)
		}
		DataType::Int32 => {
			encode_signed_value(downcast_array::<Int32Array>(array)?.value(row) as i128, 4)
		}
		DataType::Int64 => {
			encode_signed_value(downcast_array::<Int64Array>(array)?.value(row) as i128, 8)
		}
		DataType::UInt8 => {
			encode_unsigned_value(downcast_array::<UInt8Array>(array)?.value(row) as u128, 1)
		}
		DataType::UInt16 => {
			encode_unsigned_value(downcast_array::<UInt16Array>(array)?.value(row) as u128, 2)
		}
		DataType::UInt32 => {
			encode_unsigned_value(downcast_array::<UInt32Array>(array)?.value(row) as u128, 4)
		}
		DataType::UInt64 => {
			encode_unsigned_value(downcast_array::<UInt64Array>(array)?.value(row) as u128, 8)
		}
		DataType::Timestamp(TimeUnit::Millisecond, _) => encode_signed_value(
			downcast_array::<PrimitiveArray<TimestampMillisecondType>>(array)?.value(row) as i128,
			8,
		),
		DataType::Timestamp(TimeUnit::Microsecond, _) => encode_signed_value(
			downcast_array::<PrimitiveArray<TimestampMicrosecondType>>(array)?.value(row) as i128,
			8,
		),
		DataType::Timestamp(TimeUnit::Nanosecond, _) => encode_signed_value(
			downcast_array::<PrimitiveArray<TimestampNanosecondType>>(array)?.value(row) as i128,
			8,
		),
		DataType::Date32 => encode_signed_value(
			downcast_array::<PrimitiveArray<Date32Type>>(array)?.value(row) as i128,
			4,
		),
		DataType::Date64 => encode_signed_value(
			downcast_array::<PrimitiveArray<Date64Type>>(array)?.value(row) as i128,
			8,
		),
		DataType::Boolean => vec![u8::from(downcast_array::<BooleanArray>(array)?.value(row))],
		DataType::Utf8 => downcast_array::<StringArray>(array)?
			.value(row)
			.as_bytes()
			.to_vec(),
		DataType::LargeUtf8 => downcast_array::<LargeStringArray>(array)?
			.value(row)
			.as_bytes()
			.to_vec(),
		DataType::Binary => downcast_array::<BinaryArray>(array)?.value(row).to_vec(),
		DataType::LargeBinary => downcast_array::<LargeBinaryArray>(array)?
			.value(row)
			.to_vec(),
		_ => return Err(AggRejectCode::TypeUnsupported),
	};
	if truncate_prefix && bytes.len() > AGG_MIN_MAX_PREFIX_BYTES {
		bytes.truncate(AGG_MIN_MAX_PREFIX_BYTES);
		truncated = true;
	}
	Ok(Some(CanonicalValue {
		arrow_type_tag,
		bytes,
		truncated,
	}))
}

fn encode_signed_value(value: i128, byte_width: usize) -> Vec<u8> {
	let bits = byte_width * 8;
	let mask = if bits == 128 {
		u128::MAX
	} else {
		(1_u128 << bits) - 1
	};
	let sign_bit = 1_u128 << (bits - 1);
	let raw = (value as u128) & mask;
	let comparable = raw ^ sign_bit;
	let be = comparable.to_be_bytes();
	be[16 - byte_width..].to_vec()
}

fn encode_unsigned_value(value: u128, byte_width: usize) -> Vec<u8> {
	let be = value.to_be_bytes();
	be[16 - byte_width..].to_vec()
}

fn downcast_array<T: 'static>(array: &dyn Array) -> Result<&T, AggRejectCode> {
	array
		.as_any()
		.downcast_ref::<T>()
		.ok_or(AggRejectCode::TypeUnsupported)
}

fn kll_compaction_seed(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	request_id: u128,
) -> u32 {
	let mut hasher = blake3::Hasher::new();
	hasher.update(&tenant_id.get().to_be_bytes());
	hasher.update(table_object_id.as_bytes());
	hasher.update(&request_id.to_be_bytes());
	u32::from_be_bytes(
		hasher.finalize().as_bytes()[..4]
			.try_into()
			.expect("fixed seed prefix"),
	)
}

pub fn slab_visibility_complete(
	seal_hlc: HybridTimestamp,
	tombstone_metadata: TombstoneMetadata,
	supersession_metadata: SupersessionMetadata,
	as_of_hlc: HybridTimestamp,
) -> bool {
	seal_hlc <= as_of_hlc
		&& tombstone_metadata.is_tracked()
		&& tombstone_metadata
			.earliest()
			.is_none_or(|tombstone_hlc| tombstone_hlc > as_of_hlc)
		&& supersession_metadata.is_tracked()
		&& supersession_metadata
			.earliest()
			.is_none_or(|supersession_hlc| supersession_hlc > as_of_hlc)
}

pub fn merge_earliest_supersession(
	current: SupersessionMetadata,
	supersession_hlc: HybridTimestamp,
) -> SupersessionMetadata {
	match current {
		SupersessionMetadata::Untracked => SupersessionMetadata::Untracked,
		SupersessionMetadata::None => SupersessionMetadata::Earliest(supersession_hlc),
		SupersessionMetadata::Earliest(existing) => {
			SupersessionMetadata::Earliest(existing.min(supersession_hlc))
		}
	}
}

pub fn merge_earliest_tombstone(
	current: TombstoneMetadata,
	candidate: HybridTimestamp,
) -> TombstoneMetadata {
	match current {
		TombstoneMetadata::Untracked => TombstoneMetadata::Untracked,
		TombstoneMetadata::None => TombstoneMetadata::Earliest(candidate),
		TombstoneMetadata::Earliest(previous) => {
			TombstoneMetadata::Earliest(previous.min(candidate))
		}
	}
}

pub fn response_crossbind_matches(
	request: &AggregateRequest,
	response: &AggregateResponse,
) -> bool {
	let logical_fingerprint_matches =
		response.logical_schema_fingerprint == request.logical_schema_fingerprint;
	if logical_fingerprint_matches {
		crate::metrics::global().observe_storage_aggregate_cache_logical_fingerprint_hit(
			request.tenant_id,
			request.table_object_id,
		);
	} else {
		crate::metrics::global().observe_storage_aggregate_cache_logical_fingerprint_miss(
			request.tenant_id,
			request.table_object_id,
		);
	}
	response.request_id == request.request_id
		&& response.table_object_id == request.table_object_id
		&& response.schema_generation == request.schema_generation
		&& logical_fingerprint_matches
		&& response.aggregate_expression_digest == request.expression_digest()
		&& response.as_of_hlc == request.as_of_hlc
}

pub fn validate_expression_types(
	expressions: &[AggregateExpression],
	batch: &RecordBatch,
) -> Result<(), AggRejectCode> {
	for expression in expressions {
		let function = expression.function()?;
		let Some(column) = function.column() else {
			continue;
		};
		let data_type = column_array(batch, column as usize)?.data_type();
		let allowed = match function.tag() {
			AggregateFunctionTag::COUNT_STAR => true,
			AggregateFunctionTag::COUNT_NON_NULL => true,
			AggregateFunctionTag::SIGNED_SUM | AggregateFunctionTag::SIGNED_AVG => matches!(
				data_type,
				DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
			),
			AggregateFunctionTag::UNSIGNED_SUM | AggregateFunctionTag::UNSIGNED_AVG => matches!(
				data_type,
				DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
			),
			AggregateFunctionTag::MIN
			| AggregateFunctionTag::MAX
			| AggregateFunctionTag::APPROX_DISTINCT => agg_arrow_type_tag(data_type, true).is_some(),
			AggregateFunctionTag::QUANTILE_KLL => agg_arrow_type_tag(data_type, false).is_some(),
			_ => false,
		};
		if !allowed {
			return Err(AggRejectCode::TypeUnsupported);
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::aggregate::{AGG_FORMAT_VERSION, AggregateBudgets};
	use crate::schema::{
		AggregateRequestId, ByteCount, Consistency, Locality, LocalityRange,
		LogicalSchemaFingerprint, RowCount, TableObjectId, TenantId,
	};
	use arrow::array::{BooleanArray, Int64Array, StringArray, UInt32Array};
	use arrow::datatypes::{Field, Schema};
	use std::sync::Arc;

	fn hlc(ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 1,
		}
	}

	fn request(aggregates: Vec<AggregateExpression>) -> AggregateRequest {
		AggregateRequest {
			format_version: AGG_FORMAT_VERSION,
			request_id: AggregateRequestId::new(11),
			tenant_id: TenantId::new(2),
			table_object_id: TableObjectId::new([3; 16]),
			schema_generation: 1,
			logical_schema_fingerprint: LogicalSchemaFingerprint::from_bytes([4; 32]),
			consistency: Consistency::LINEARIZABLE,
			min_hlc: hlc(1),
			as_of_hlc: hlc(2),
			locality_ranges: vec![LocalityRange {
				start: Locality::ZERO,
				end: Locality::MAX,
			}],
			predicate_program: None,
			aggregates,
			budgets: AggregateBudgets {
				max_rows: RowCount::new(1_000),
				..AggregateBudgets::default()
			},
		}
	}

	fn batch() -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![
			Field::new("signed", DataType::Int64, true),
			Field::new("unsigned", DataType::UInt32, true),
			Field::new("name", DataType::Utf8, true),
			Field::new("flag", DataType::Boolean, true),
		]));
		RecordBatch::try_new(
			schema,
			vec![
				Arc::new(Int64Array::from(vec![Some(7), None, Some(-2), Some(5)])),
				Arc::new(UInt32Array::from(vec![Some(1), Some(2), None, Some(4)])),
				Arc::new(StringArray::from(vec![
					Some("delta"),
					Some("alpha"),
					None,
					Some("zulu"),
				])),
				Arc::new(BooleanArray::from(vec![
					Some(true),
					Some(false),
					None,
					Some(true),
				])),
			],
		)
		.unwrap()
	}

	fn single_utf8_batch(value: impl Into<String>) -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
		RecordBatch::try_new(
			schema,
			vec![Arc::new(StringArray::from(vec![value.into()]))],
		)
		.unwrap()
	}

	#[test]
	fn exact_accumulators_match_scalar_oracles() {
		let request = request(vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::COUNT_NON_NULL,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::SIGNED_SUM,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::UNSIGNED_AVG,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 2,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MAX,
				column_ordinal: 3,
				parameter_block: Vec::new(),
			},
		]);

		let result = aggregate_record_batches(&request, &[batch()], false).unwrap();
		assert_eq!(result.response.status, AggregateStatus::Success);
		assert_eq!(result.response.diagnostics.rows_scanned, 4);
		assert_eq!(result.response.diagnostics.rows_admitted, 4);
		assert_eq!(result.states[0], AggregateState::Count { value: 4 });
		assert_eq!(result.states[1], AggregateState::Count { value: 3 });
		assert_eq!(
			result.states[2],
			AggregateState::SignedSum {
				sum: 10,
				non_null_count: 3,
				overflow: false,
			}
		);
		assert_eq!(
			result.states[3],
			AggregateState::UnsignedAverageParts {
				sum: 7,
				count: 3,
				overflow: false,
			}
		);
		assert_eq!(
			result.states[4],
			AggregateState::MinMax {
				value: Some(b"alpha".to_vec()),
				arrow_type_tag: ARROW_STATS_TAG_UTF8,
				truncated: false,
			}
		);
		assert_eq!(
			result.states[5],
			AggregateState::MinMax {
				value: Some(vec![1]),
				arrow_type_tag: ARROW_STATS_TAG_BOOLEAN,
				truncated: false,
			}
		);
	}

	#[test]
	fn merge_responses_combines_exact_partials() {
		let request = request(vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::SIGNED_SUM,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 2,
				parameter_block: Vec::new(),
			},
		]);
		let left = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;
		let right = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;

		let merged = merge_aggregate_responses(&request, &[left, right]).unwrap();
		assert_eq!(merged.status, AggregateStatus::Success);
		assert_eq!(merged.diagnostics.peer_partials_merged, 3);

		assert_eq!(
			decode_aggregate_state(merged.partials[0].tag, &merged.partials[0].state).unwrap(),
			AggregateState::Count { value: 8 }
		);
		assert_eq!(
			decode_aggregate_state(merged.partials[1].tag, &merged.partials[1].state).unwrap(),
			AggregateState::SignedSum {
				sum: 20,
				non_null_count: 1,
				overflow: false,
			}
		);
		assert_eq!(
			decode_aggregate_state(merged.partials[2].tag, &merged.partials[2].state).unwrap(),
			AggregateState::MinMax {
				value: Some(b"alpha".to_vec()),
				arrow_type_tag: 0,
				truncated: false,
			}
		);
	}

	#[test]
	fn merge_responses_rejects_crossbind_mismatch() {
		let request = request(vec![AggregateExpression::count_star()]);
		let mut response = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;
		response.as_of_hlc = hlc(99);

		assert_eq!(
			merge_aggregate_responses(&request, &[response]).unwrap_err(),
			AggRejectCode::PartialCrossbindFailed
		);
	}

	#[test]
	fn response_crossbind_observes_logical_fingerprint_axis() {
		let mut request = request(vec![AggregateExpression::count_star()]);
		request.table_object_id = TableObjectId::new([0xA5; 16]);
		let mut response = AggregateResponse::rejected(&request, AggRejectCode::ProgramInvalid);
		let hit_before = crate::metrics::global()
			.storage_aggregate_cache_logical_fingerprint_hit_total_for_test(
				request.tenant_id,
				request.table_object_id,
			);
		let miss_before = crate::metrics::global()
			.storage_aggregate_cache_logical_fingerprint_miss_total_for_test(
				request.tenant_id,
				request.table_object_id,
			);

		assert!(response_crossbind_matches(&request, &response));
		response.logical_schema_fingerprint = LogicalSchemaFingerprint::from_bytes([0xA6; 32]);
		assert!(!response_crossbind_matches(&request, &response));

		assert_eq!(
			crate::metrics::global()
				.storage_aggregate_cache_logical_fingerprint_hit_total_for_test(
					request.tenant_id,
					request.table_object_id,
				),
			hit_before + 1
		);
		assert_eq!(
			crate::metrics::global()
				.storage_aggregate_cache_logical_fingerprint_miss_total_for_test(
					request.tenant_id,
					request.table_object_id,
				),
			miss_before + 1
		);
	}

	#[test]
	fn merge_responses_enforces_partial_state_budget() {
		let mut request = request(vec![AggregateExpression::count_star()]);
		let response = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;
		request.budgets.max_partial_state_bytes = ByteCount::new(4);

		assert_eq!(
			merge_aggregate_responses(&request, &[response]).unwrap_err(),
			AggRejectCode::PartialStateTooLarge
		);
	}

	#[test]
	fn merge_responses_propagates_peer_partial_status() {
		let request = request(vec![AggregateExpression::count_star()]);
		let mut left = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;
		left.status = AggregateStatus::Partial;
		let right = aggregate_record_batches(&request, &[batch()], false)
			.unwrap()
			.response;

		let merged = merge_aggregate_responses(&request, &[left, right]).unwrap();
		assert_eq!(merged.status, AggregateStatus::Partial);
		assert_eq!(
			decode_aggregate_state(merged.partials[0].tag, &merged.partials[0].state).unwrap(),
			AggregateState::Count { value: 8 }
		);
	}

	#[test]
	fn merge_responses_marks_truncated_minmax_state_partial() {
		let request = request(vec![AggregateExpression {
			tag: AggregateFunctionTag::MIN,
			column_ordinal: 0,
			parameter_block: Vec::new(),
		}]);
		let long = "a".repeat(AGG_MIN_MAX_PREFIX_BYTES + 8);
		let mut left = aggregate_record_batches(&request, &[single_utf8_batch(long)], false)
			.unwrap()
			.response;
		assert_eq!(left.status, AggregateStatus::Partial);
		left.status = AggregateStatus::Success;
		let right = aggregate_record_batches(&request, &[single_utf8_batch("zzzz")], false)
			.unwrap()
			.response;
		assert_eq!(right.status, AggregateStatus::Success);

		let merged = merge_aggregate_responses(&request, &[left, right]).unwrap();
		assert_eq!(merged.status, AggregateStatus::Partial);
		assert_eq!(
			decode_aggregate_state(merged.partials[0].tag, &merged.partials[0].state).unwrap(),
			AggregateState::MinMax {
				value: Some(vec![b'a'; AGG_MIN_MAX_PREFIX_BYTES]),
				arrow_type_tag: 0,
				truncated: true,
			}
		);
	}

	#[test]
	fn truncated_minmax_marks_response_partial() {
		let request = request(vec![AggregateExpression {
			tag: AggregateFunctionTag::MIN,
			column_ordinal: 0,
			parameter_block: Vec::new(),
		}]);
		let long = "a".repeat(AGG_MIN_MAX_PREFIX_BYTES + 8);
		let batch = single_utf8_batch(long);

		let result = aggregate_record_batches(&request, &[batch], false).unwrap();
		assert_eq!(result.response.status, AggregateStatus::Partial);
		assert_eq!(result.response.partials[0].state.last(), Some(&1));
	}

	#[test]
	fn visibility_helper_uses_earliest_tombstone_direction() {
		assert!(!slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::Untracked,
			SupersessionMetadata::None,
			hlc(11)
		));
		assert!(slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::None,
			SupersessionMetadata::None,
			hlc(11)
		));
		assert!(slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::Earliest(hlc(12)),
			SupersessionMetadata::None,
			hlc(11)
		));
		assert!(!slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::Earliest(hlc(11)),
			SupersessionMetadata::None,
			hlc(11)
		));
		assert!(!slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::None,
			SupersessionMetadata::Untracked,
			hlc(11)
		));
		assert!(slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::None,
			SupersessionMetadata::Earliest(hlc(12)),
			hlc(11)
		));
		assert!(!slab_visibility_complete(
			hlc(10),
			TombstoneMetadata::None,
			SupersessionMetadata::Earliest(hlc(11)),
			hlc(11)
		));
		assert_eq!(
			merge_earliest_tombstone(TombstoneMetadata::Untracked, hlc(20)),
			TombstoneMetadata::Untracked
		);
		assert_eq!(
			merge_earliest_tombstone(TombstoneMetadata::None, hlc(20)),
			TombstoneMetadata::Earliest(hlc(20))
		);
		assert_eq!(
			merge_earliest_tombstone(TombstoneMetadata::Earliest(hlc(20)), hlc(15)),
			TombstoneMetadata::Earliest(hlc(15))
		);
	}

	#[test]
	fn unsupported_sum_type_rejects_without_panic() {
		let request = request(vec![AggregateExpression {
			tag: AggregateFunctionTag::SIGNED_SUM,
			column_ordinal: 2,
			parameter_block: Vec::new(),
		}]);
		assert_eq!(
			aggregate_record_batches(&request, &[batch()], false).unwrap_err(),
			AggRejectCode::TypeUnsupported
		);
	}
}
