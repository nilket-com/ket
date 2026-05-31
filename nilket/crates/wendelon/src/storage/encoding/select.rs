use crate::schema::{
	BaseValue, BitsPerValue, BufferValueCount, BufferValueIndex, EncodingPageByteOffset,
	IpcColumnOrdinal, RecordBodyByteLen, RunCount,
};
use crate::storage::encoding::cascade::estimator::{
	FsstRowSafeEstimatorDecision, FsstRowSafeEstimatorRouting, FsstRowSafeExactLengths,
	estimate_fsst_row_safe, estimate_fsst_row_safe_cost_free_decline,
	estimate_fsst_row_safe_with_cached_tables,
};
use crate::storage::encoding::cascade::{
	EncodedBuffer, FsstSymbolTable, encode_buffer, encode_buffer_with_value_offsets,
	fixed_width_bytes_per_value, fsst_row_safe_encoded_len_with_table,
	fsst_whole_buffer_encoded_len_with_table, train_fsst_row_safe_and_whole_buffer_symbol_tables,
	train_fsst_row_safe_symbol_table,
};
use crate::storage::encoding::page_materialize::validity_page_decoded_byte_len;
use crate::storage::encoding::sample::{ColumnSample, sample_buffer, sample_fsst_compressible};
use crate::storage::encoding::{
	EncodingCascade, EncodingCascadeKind, EncodingLeafCascade, EncodingWriterConfig,
};
use arrow::datatypes::DataType;
use std::collections::{BTreeMap, HashSet};
use std::io::{Error, ErrorKind};

#[derive(Default)]
pub(crate) struct WriterCascadeContext {
	fsst_tables_by_column: BTreeMap<IpcColumnOrdinal, ColumnFsstSymbolTableCache>,
}

pub(crate) struct ColumnFsstSymbolTableCache {
	row_safe_table: FsstSymbolTable,
	whole_buffer_table: Option<FsstSymbolTable>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum WriterValidityCascadeRouting {
	Disabled,
	Enabled,
}

pub(crate) const VALIDITY_RLE_BYTE_SAVING_FRACTION: (usize, usize) = (1, 4);
pub(crate) const MIN_VALIDITY_ENCODE_BYTES: usize = 256;

pub(crate) struct ValidityRleEncodingPlan {
	pub(crate) encoded_bytes: Vec<u8>,
	pub(crate) page_end_offsets: Vec<EncodingPageByteOffset>,
	pub(crate) run_count: RunCount,
	pub(crate) raw_byte_len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ValuePageRowSpan {
	pub(crate) first_value_index: BufferValueIndex,
	pub(crate) value_count: BufferValueCount,
}

struct FsstCurrentBufferExactCandidate {
	row_safe_len: usize,
	whole_buffer_len: Option<usize>,
	row_safe_table: FsstSymbolTable,
	whole_buffer_table: Option<FsstSymbolTable>,
}

pub fn select_and_encode_buffer(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
) -> std::io::Result<EncodedBuffer> {
	select_and_encode_buffer_with_value_offsets(input, data_type, decoded_value_count, config, None)
}

pub fn select_and_encode_buffer_with_value_offsets(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
) -> std::io::Result<EncodedBuffer> {
	select_and_encode_buffer_with_value_offsets_and_context(
		input,
		data_type,
		decoded_value_count,
		config,
		value_offsets,
		None,
		None,
	)
}

pub(crate) fn select_and_encode_buffer_with_value_offsets_and_context(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<EncodedBuffer> {
	config.validate()?;
	let page_rows = config.target_simd_page_values;
	if !config.enabled || input.len() as u64 <= config.min_buffer_bytes_for_encoding.get() {
		return encode_buffer(
			input,
			data_type,
			EncodingCascade::Plain,
			decoded_value_count,
			page_rows,
		);
	}

	if writer_context.is_some()
		&& ipc_column_ordinal.is_some()
		&& !config.cascade_enabled(EncodingCascadeKind::Rle)
		&& config.cascade_enabled(EncodingCascadeKind::FsstWholeBuffer)
		&& config.cascade_enabled(EncodingCascadeKind::FsstRowSafe)
		&& is_variable_width_candidate_type(data_type)
		&& let Some(offsets) = value_offsets
		&& estimate_fsst_row_safe_cost_free_decline(input, decoded_value_count, offsets).is_some()
		&& sample_fsst_compressible(data_type, input)
	{
		return encode_buffer_with_value_offsets(
			input,
			data_type,
			EncodingCascade::FsstWholeBuffer {
				symbol_table_byte_len: RecordBodyByteLen::ZERO,
			},
			decoded_value_count,
			page_rows,
			value_offsets,
		);
	}

	let sample = sample_buffer(
		input,
		data_type,
		decoded_value_count,
		config.cascade_sample_rows,
	);
	let cascade = select_candidate_cascade(
		input,
		data_type,
		decoded_value_count,
		&sample,
		config,
		value_offsets,
		writer_context,
		ipc_column_ordinal,
	)?;
	encode_buffer_with_value_offsets(
		input,
		data_type,
		cascade,
		decoded_value_count,
		page_rows,
		value_offsets,
	)
}

pub(crate) fn select_validity_cascade(
	raw_validity_bytes: &[u8],
	value_page_row_spans: &[ValuePageRowSpan],
	column_null_count: usize,
) -> Option<ValidityRleEncodingPlan> {
	if column_null_count == 0
		|| value_page_row_spans.is_empty()
		|| raw_validity_bytes.len() < MIN_VALIDITY_ENCODE_BYTES
	{
		return None;
	}
	let mut encoded_bytes = Vec::new();
	let mut page_end_offsets = Vec::with_capacity(value_page_row_spans.len());
	let mut run_count = 0_usize;
	let page_table_bytes = value_page_row_spans
		.len()
		.checked_mul(std::mem::size_of::<u32>())?;
	let (saving_numerator, saving_denominator) = VALIDITY_RLE_BYTE_SAVING_FRACTION;
	if saving_numerator >= saving_denominator || saving_denominator == 0 {
		return None;
	}
	let saving_rhs = raw_validity_bytes
		.len()
		.checked_mul(saving_denominator.checked_sub(saving_numerator)?)?;
	for (page_index, span) in value_page_row_spans.iter().enumerate() {
		let page = extract_page_local_validity(raw_validity_bytes, *span)?;
		let (encoded_page, page_run_count) = encode_validity_rle_page(&page)?;
		run_count = run_count.checked_add(page_run_count)?;
		encoded_bytes.extend_from_slice(&encoded_page);
		page_end_offsets.push(EncodingPageByteOffset::new(
			u32::try_from(encoded_bytes.len()).ok()?,
		));
		let remaining_pages = value_page_row_spans
			.len()
			.checked_sub(page_index)?
			.checked_sub(1)?;
		let best_possible_remaining_payload = remaining_pages.checked_mul(5)?;
		let lower_bound_total = encoded_bytes
			.len()
			.checked_add(best_possible_remaining_payload)?
			.checked_add(page_table_bytes)?;
		if lower_bound_total.checked_mul(saving_denominator)? >= saving_rhs {
			return None;
		}
	}
	let encoded_total = encoded_bytes.len().checked_add(page_table_bytes)?;
	if !validity_rle_strictly_beats_raw(encoded_total, raw_validity_bytes.len())? {
		return None;
	}
	Some(ValidityRleEncodingPlan {
		encoded_bytes,
		page_end_offsets,
		run_count: RunCount::new(u32::try_from(run_count).ok()?),
		raw_byte_len: raw_validity_bytes.len(),
	})
}

fn extract_page_local_validity(
	raw_validity_bytes: &[u8],
	span: ValuePageRowSpan,
) -> Option<Vec<u8>> {
	let first = span.first_value_index.get() as usize;
	let value_count = span.value_count.get() as usize;
	if value_count == 0 {
		return None;
	}
	let end = first.checked_add(value_count)?;
	if end > raw_validity_bytes.len().checked_mul(8)? {
		return None;
	}
	let mut out = vec![0_u8; validity_page_decoded_byte_len(value_count)];
	for row in 0..value_count {
		let source = first + row;
		if (raw_validity_bytes[source / 8] & (1_u8 << (source % 8))) != 0 {
			out[row / 8] |= 1_u8 << (row % 8);
		}
	}
	Some(out)
}

fn encode_validity_rle_page(input: &[u8]) -> Option<(Vec<u8>, usize)> {
	let mut out = Vec::new();
	let mut cursor = 0_usize;
	let mut runs = 0_usize;
	while cursor < input.len() {
		let byte = input[cursor];
		let mut run = 1_usize;
		while cursor + run < input.len() && input[cursor + run] == byte && run < u32::MAX as usize {
			run += 1;
		}
		out.extend_from_slice(&u32::try_from(run).ok()?.to_be_bytes());
		out.push(byte);
		cursor += run;
		runs = runs.checked_add(1)?;
	}
	Some((out, runs))
}

fn validity_rle_strictly_beats_raw(encoded_total: usize, raw_total: usize) -> Option<bool> {
	let (numerator, denominator) = VALIDITY_RLE_BYTE_SAVING_FRACTION;
	if numerator >= denominator || denominator == 0 {
		return None;
	}
	let lhs = encoded_total.checked_mul(denominator)?;
	let rhs = raw_total.checked_mul(denominator.checked_sub(numerator)?)?;
	Some(lhs < rhs)
}

fn select_candidate_cascade(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	sample: &ColumnSample,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	mut writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<EncodingCascade> {
	if input.is_empty() {
		return Ok(EncodingCascade::Plain);
	}
	if config.cascade_enabled(EncodingCascadeKind::Rle) {
		let runs = sample.run_count_estimate as usize;
		if sample.sampled_value_count > 0 && runs.saturating_mul(10) < sample.sampled_value_count {
			return Ok(EncodingCascade::Rle {
				run_count: RunCount::new(u32::try_from(runs).map_err(|_| {
					Error::new(ErrorKind::InvalidData, "Encoding RLE run count exceeds u32")
				})?),
				inner: Box::new(EncodingLeafCascade::Plain),
			});
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::Delta)
		&& is_delta_candidate_type(data_type)
		&& let Some(width) = fixed_width_bytes_per_value(input, decoded_value_count)
			.filter(|width| matches!(width, 1 | 2 | 4 | 8))
	{
		let value_count = input.len() / width;
		if value_count > 1 && sample.is_monotonic {
			let (base, delta_width) = monotonic_delta_width(input, width).unwrap_or_else(|| {
				(
					sample.min.map(|value| value.get()).unwrap_or_default(),
					sample.delta_width_bytes.unwrap_or(16),
				)
			});
			let encoded_len = 26_usize.saturating_add(value_count.saturating_sub(1) * delta_width);
			if encoded_len.saturating_mul(2) <= input.len() {
				return Ok(EncodingCascade::Delta {
					base: BaseValue::new(base),
					inner: Box::new(EncodingLeafCascade::Plain),
				});
			}
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::BitPacked)
		&& is_bitpacked_candidate_type(data_type)
		&& matches!(sample.bytes_per_value, Some(1 | 2 | 4 | 8))
		&& let Some(width) = sample.bytes_per_value
		&& let Some((min, max)) = fixed_width_min_max(input, width)
	{
		let span = max.saturating_sub(min);
		let bits = bits_required_u128(span);
		let value_count = decoded_value_count.get() as usize;
		let packed_len = value_count.saturating_mul(bits as usize).div_ceil(8);
		if bits < (width as u8).saturating_mul(8) && packed_len.saturating_mul(2) <= input.len() {
			return Ok(EncodingCascade::BitPacked {
				bits: BitsPerValue::new(bits),
				base: BaseValue::new(i128::try_from(min).unwrap_or(i128::MAX)),
			});
		}
	}

	if (config.cascade_enabled(EncodingCascadeKind::FsstWholeBuffer)
		|| config.cascade_enabled(EncodingCascadeKind::FsstRowSafe))
		&& is_variable_width_candidate_type(data_type)
		&& sample.fsst_compressible
		&& let Some(candidate) = select_fsst_candidate(
			input,
			data_type,
			decoded_value_count,
			config,
			value_offsets,
			writer_context.as_deref_mut(),
			ipc_column_ordinal,
		)? {
		return Ok(candidate);
	}

	if config.cascade_enabled(EncodingCascadeKind::Dictionary)
		&& is_variable_width_candidate_type(data_type)
		&& decoded_value_count.get() <= config.target_simd_page_values.get()
		&& let Some(offsets) = value_offsets
		&& let Some(candidate) = variable_width_dictionary_candidate(input, offsets)?
	{
		return Ok(candidate);
	}

	if config.cascade_enabled(EncodingCascadeKind::Dictionary)
		&& matches!(sample.bytes_per_value, Some(1 | 2 | 4 | 8))
		&& sample.distinct_count_estimate > 0
		&& sample.distinct_count_estimate <= 4
	{
		let value_count = decoded_value_count.get() as usize;
		let width = sample.bytes_per_value.unwrap_or(1);
		let distinct_count = fixed_width_distinct_count(input, width).min(u32::MAX as usize) as u32;
		if distinct_count == 0 || distinct_count > 65_536 {
			return Ok(EncodingCascade::Plain);
		}
		let index_width = if distinct_count <= 256 {
			1
		} else if distinct_count <= 65_536 {
			2
		} else {
			4
		};
		let index_bits = bits_required_u32(distinct_count.saturating_sub(1));
		let bitpacked_index_len = value_count.saturating_mul(index_bits as usize).div_ceil(8);
		let estimated_len = 14_usize
			.saturating_add(distinct_count as usize * width)
			.saturating_add(bitpacked_index_len.min(value_count.saturating_mul(index_width)));
		if estimated_len.saturating_mul(2) <= input.len() {
			return Ok(EncodingCascade::Dictionary {
				dict_byte_len: RecordBodyByteLen::new(
					u64::from(distinct_count).saturating_mul(width as u64),
				),
				dict_value_count: crate::schema::DictValueCount::new(distinct_count),
				inner: Box::new(EncodingLeafCascade::BitPacked {
					bits: BitsPerValue::new(index_bits),
					base: BaseValue::ZERO,
				}),
			});
		}
	}

	if config.cascade_enabled(EncodingCascadeKind::Alp) && sample.alp_candidate {
		return Ok(EncodingCascade::Alp {
			exponent: sample.alp_exponent.unwrap_or(crate::schema::Exponent::ZERO),
			factor: sample.alp_factor.unwrap_or(crate::schema::Factor::new(1)),
			exception_count: crate::schema::ExceptionCount::ZERO,
			inner: Box::new(EncodingLeafCascade::Plain),
		});
	}

	Ok(plain_or_lz4(config))
}

fn select_fsst_candidate(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<Option<EncodingCascade>> {
	let routing = if writer_context.is_some() && ipc_column_ordinal.is_some() {
		FsstRowSafeEstimatorRouting::Estimator
	} else {
		FsstRowSafeEstimatorRouting::ExactOnly
	};
	select_fsst_candidate_with_routing_and_context(
		input,
		data_type,
		decoded_value_count,
		config,
		value_offsets,
		routing,
		writer_context,
		ipc_column_ordinal,
	)
}

#[allow(dead_code)]
pub(crate) fn select_fsst_candidate_with_routing(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	routing: FsstRowSafeEstimatorRouting,
) -> std::io::Result<Option<EncodingCascade>> {
	select_fsst_candidate_with_routing_and_context(
		input,
		data_type,
		decoded_value_count,
		config,
		value_offsets,
		routing,
		None,
		None,
	)
}

pub(crate) fn select_fsst_candidate_with_routing_and_context(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	routing: FsstRowSafeEstimatorRouting,
	writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<Option<EncodingCascade>> {
	match routing {
		FsstRowSafeEstimatorRouting::ExactOnly => select_fsst_candidate_exact(
			input,
			data_type,
			decoded_value_count,
			config,
			value_offsets,
			writer_context,
			ipc_column_ordinal,
		),
		FsstRowSafeEstimatorRouting::Estimator => select_fsst_candidate_estimator(
			input,
			data_type,
			decoded_value_count,
			config,
			value_offsets,
			writer_context,
			ipc_column_ordinal,
		),
	}
}

fn select_fsst_candidate_estimator(
	input: &[u8],
	data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<Option<EncodingCascade>> {
	let fsst_enabled = config.cascade_enabled(EncodingCascadeKind::FsstWholeBuffer);
	let row_safe_enabled = config.cascade_enabled(EncodingCascadeKind::FsstRowSafe);
	let fsst = EncodingCascade::FsstWholeBuffer {
		symbol_table_byte_len: RecordBodyByteLen::new(0),
	};
	if row_safe_enabled && let Some(offsets) = value_offsets {
		let row_safe = EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::new(0),
		};
		if let Some(_reason) =
			estimate_fsst_row_safe_cost_free_decline(input, decoded_value_count, offsets)
		{
			if fsst_enabled {
				return Ok(Some(fsst));
			}
			return Ok(None);
		}
		let cached_outcome = match (writer_context.as_ref(), ipc_column_ordinal) {
			(Some(context), Some(column)) => {
				let context = &**context;
				context
					.fsst_tables_by_column
					.get(&column)
					.map(|cache| {
						estimate_fsst_row_safe_with_cached_tables(
							input,
							decoded_value_count,
							config.cascade_sample_rows,
							offsets,
							&cache.row_safe_table,
							cache.whole_buffer_table.as_ref(),
							fsst_enabled,
						)
					})
					.transpose()?
			}
			_ => None,
		};
		let mut warm_cache_sampled_rows = None;
		let outcome = if let Some(outcome) = cached_outcome {
			warm_cache_sampled_rows = Some(outcome.stats.sampled_rows);
			outcome
		} else if writer_context.is_some() && ipc_column_ordinal.is_some() {
			return select_fsst_candidate_exact(
				input,
				data_type,
				decoded_value_count,
				config,
				value_offsets,
				writer_context,
				ipc_column_ordinal,
			);
		} else {
			estimate_fsst_row_safe(
				input,
				decoded_value_count,
				config.cascade_sample_rows,
				offsets,
				fsst_enabled,
			)?
		};
		match outcome.decision {
			FsstRowSafeEstimatorDecision::Decline(_) => {}
			FsstRowSafeEstimatorDecision::RequireExactGuard => {
				if let Some(FsstRowSafeExactLengths::CurrentBuffer {
					row_safe_len,
					whole_buffer_len,
				}) = outcome.exact_lengths
				{
					if row_safe_exact_guard(row_safe_len, input.len(), whole_buffer_len) {
						return Ok(Some(row_safe));
					}
				} else {
					let refresh_context = if warm_cache_sampled_rows.unwrap_or_default() > 0 {
						writer_context
					} else {
						None
					};
					return select_fsst_candidate_exact(
						input,
						data_type,
						decoded_value_count,
						config,
						value_offsets,
						refresh_context,
						ipc_column_ordinal,
					);
				}
			}
		}
	}
	if fsst_enabled {
		return Ok(Some(fsst));
	}
	Ok(None)
}

fn select_fsst_candidate_exact(
	input: &[u8],
	_data_type: &DataType,
	decoded_value_count: BufferValueCount,
	config: &EncodingWriterConfig,
	value_offsets: Option<&[u64]>,
	writer_context: Option<&mut WriterCascadeContext>,
	ipc_column_ordinal: Option<IpcColumnOrdinal>,
) -> std::io::Result<Option<EncodingCascade>> {
	let fsst_enabled = config.cascade_enabled(EncodingCascadeKind::FsstWholeBuffer);
	let row_safe_enabled = config.cascade_enabled(EncodingCascadeKind::FsstRowSafe);
	let fsst = EncodingCascade::FsstWholeBuffer {
		symbol_table_byte_len: RecordBodyByteLen::new(0),
	};
	if row_safe_enabled && let Some(offsets) = value_offsets {
		let row_safe = EncodingCascade::FsstRowSafe {
			symbol_table_byte_len: RecordBodyByteLen::new(0),
		};
		if let Some(candidate) =
			fsst_current_buffer_exact_candidate(input, decoded_value_count, offsets, fsst_enabled)?
		{
			let FsstCurrentBufferExactCandidate {
				row_safe_len,
				whole_buffer_len,
				row_safe_table,
				whole_buffer_table,
			} = candidate;
			if let (Some(context), Some(column)) = (writer_context, ipc_column_ordinal) {
				context.fsst_tables_by_column.insert(
					column,
					ColumnFsstSymbolTableCache {
						row_safe_table,
						whole_buffer_table,
					},
				);
			}
			if row_safe_exact_guard(row_safe_len, input.len(), whole_buffer_len) {
				return Ok(Some(row_safe));
			}
		}
	}
	if fsst_enabled {
		return Ok(Some(fsst));
	}
	Ok(None)
}

fn fsst_current_buffer_exact_candidate(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	value_offsets: &[u64],
	whole_buffer_guard_enabled: bool,
) -> std::io::Result<Option<FsstCurrentBufferExactCandidate>> {
	let (row_safe_table, whole_buffer_table) = if whole_buffer_guard_enabled {
		let (row_safe_table, whole_buffer_table) =
			train_fsst_row_safe_and_whole_buffer_symbol_tables(
				input,
				decoded_value_count,
				value_offsets,
			)?;
		(row_safe_table, Some(whole_buffer_table))
	} else {
		(
			train_fsst_row_safe_symbol_table(input, decoded_value_count, value_offsets)?,
			None,
		)
	};
	let Some(row_safe_len) = fsst_row_safe_encoded_len_with_table(
		input,
		decoded_value_count,
		value_offsets,
		&row_safe_table,
	)?
	else {
		return Ok(None);
	};
	let whole_buffer_len = if let Some(table) = whole_buffer_table.as_ref() {
		let Some(len) = fsst_whole_buffer_encoded_len_with_table(input, table) else {
			return Ok(None);
		};
		Some(len)
	} else {
		None
	};
	Ok(Some(FsstCurrentBufferExactCandidate {
		row_safe_len,
		whole_buffer_len,
		row_safe_table,
		whole_buffer_table,
	}))
}

fn row_safe_exact_guard(
	row_safe_len: usize,
	decoded_plaintext_len: usize,
	whole_buffer_len: Option<usize>,
) -> bool {
	row_safe_len <= decoded_plaintext_len
		&& whole_buffer_len
			.map(|len| row_safe_len <= len.saturating_mul(11).div_ceil(10))
			.unwrap_or(true)
}

fn plain_or_lz4(config: &EncodingWriterConfig) -> EncodingCascade {
	if config.cascade_enabled(EncodingCascadeKind::Plain)
		|| !config.cascade_enabled(EncodingCascadeKind::Lz4Frame)
	{
		EncodingCascade::Plain
	} else {
		EncodingCascade::Lz4Frame
	}
}

fn bits_required_u32(span: u32) -> u8 {
	if span == 0 {
		1
	} else {
		(32 - span.leading_zeros()) as u8
	}
}

fn bits_required_u128(span: u128) -> u8 {
	if span == 0 {
		1
	} else {
		(128 - span.leading_zeros()) as u8
	}
}

fn fixed_width_min_max(input: &[u8], width: usize) -> Option<(u128, u128)> {
	let mut chunks = input.chunks_exact(width);
	if !chunks.remainder().is_empty() {
		return None;
	}
	let first = read_unsigned_le_as_u128(chunks.next()?);
	let mut min = first;
	let mut max = first;
	for chunk in chunks {
		let value = read_unsigned_le_as_u128(chunk);
		min = min.min(value);
		max = max.max(value);
	}
	Some((min, max))
}

fn fixed_width_distinct_count(input: &[u8], width: usize) -> usize {
	let mut distinct = Vec::<i128>::new();
	let mut large_distinct = None::<HashSet<i128>>;
	for value in input.chunks_exact(width) {
		let value = read_unsigned_le_as_i128(value);
		if let Some(distinct) = large_distinct.as_mut() {
			distinct.insert(value);
			if distinct.len() > 65_536 {
				break;
			}
		} else if !distinct.contains(&value) {
			distinct.push(value);
			if distinct.len() > 64 {
				let mut promoted = HashSet::with_capacity(128);
				promoted.extend(distinct.drain(..));
				large_distinct = Some(promoted);
			}
		}
	}
	large_distinct
		.as_ref()
		.map(HashSet::len)
		.unwrap_or(distinct.len())
}

fn is_delta_candidate_type(data_type: &DataType) -> bool {
	is_bitpacked_candidate_type(data_type)
}

fn is_bitpacked_candidate_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Date32
			| DataType::Date64
			| DataType::Time32(_)
			| DataType::Time64(_)
			| DataType::Timestamp(_, _)
			| DataType::Duration(_)
	)
}

fn is_variable_width_candidate_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
	)
}

fn variable_width_dictionary_candidate(
	input: &[u8],
	offsets: &[u64],
) -> std::io::Result<Option<EncodingCascade>> {
	if offsets.len() < 2 || offsets.last().copied() != Some(input.len() as u64) {
		return Ok(None);
	}
	let value_count = offsets.len() - 1;
	let mut distinct = HashSet::<&[u8]>::new();
	let mut previous = 0_u64;
	for pair in offsets.windows(2) {
		let start = pair[0];
		let end = pair[1];
		if start < previous || end < start || end > input.len() as u64 {
			return Ok(None);
		}
		previous = end;
		distinct.insert(&input[start as usize..end as usize]);
		if distinct.len() > 65_536 {
			return Ok(None);
		}
	}
	if distinct.is_empty() || distinct.len().saturating_mul(4) > value_count {
		return Ok(None);
	}
	let index_width = if distinct.len() <= 256 {
		1
	} else if distinct.len() <= 65_536 {
		2
	} else {
		4
	};
	let index_bits = bits_required_u32(u32::try_from(distinct.len().saturating_sub(1)).map_err(
		|_| {
			Error::new(
				ErrorKind::InvalidData,
				"Encoding Dictionary distinct count overflow",
			)
		},
	)?);
	let dict_payload_len = distinct.iter().try_fold(0_usize, |sum, value| {
		sum.checked_add(4)
			.and_then(|next| next.checked_add(value.len()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "Encoding Dictionary size overflow"))
	})?;
	let estimated_len = 17_usize.saturating_add(dict_payload_len).saturating_add(
		value_count
			.saturating_mul(index_bits as usize)
			.div_ceil(8)
			.min(value_count.saturating_mul(index_width)),
	);
	if estimated_len.saturating_mul(2) > input.len() {
		return Ok(None);
	}
	Ok(Some(EncodingCascade::Dictionary {
		dict_byte_len: RecordBodyByteLen::new(dict_payload_len as u64),
		dict_value_count: crate::schema::DictValueCount::new(
			u32::try_from(distinct.len()).map_err(|_| {
				Error::new(
					ErrorKind::InvalidData,
					"Encoding Dictionary distinct count overflow",
				)
			})?,
		),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: BitsPerValue::new(index_bits),
			base: BaseValue::ZERO,
		}),
	}))
}

fn monotonic_delta_width(input: &[u8], width: usize) -> Option<(i128, usize)> {
	let mut chunks = input.chunks_exact(width);
	let first = read_unsigned_le_as_i128(chunks.next()?);
	let mut previous = first;
	let mut direction = 0_i8;
	let mut delta_width = 1_usize;
	for chunk in chunks {
		let value = read_unsigned_le_as_i128(chunk);
		let delta = value.checked_sub(previous)?;
		let step_direction = if delta == 0 {
			0
		} else {
			if delta > 0 { 1 } else { -1 }
		};
		if step_direction != 0 {
			if direction == 0 {
				direction = step_direction;
			} else if direction != step_direction {
				return None;
			}
		}
		delta_width = delta_width.max(delta_width_bytes(delta));
		previous = value;
	}
	Some((first, delta_width))
}

fn read_unsigned_le_as_i128(bytes: &[u8]) -> i128 {
	let mut value = 0_i128;
	for (shift, byte) in bytes.iter().enumerate() {
		value |= i128::from(*byte) << (shift * 8);
	}
	value
}

fn read_unsigned_le_as_u128(bytes: &[u8]) -> u128 {
	match bytes.len() {
		1 => u128::from(bytes[0]),
		2 => u128::from(u16::from_le_bytes(bytes.try_into().expect("2-byte value"))),
		4 => u128::from(u32::from_le_bytes(bytes.try_into().expect("4-byte value"))),
		8 => u128::from(u64::from_le_bytes(bytes.try_into().expect("8-byte value"))),
		_ => {
			let mut value = 0_u128;
			for (shift, byte) in bytes.iter().enumerate() {
				value |= u128::from(*byte) << (shift * 8);
			}
			value
		}
	}
}

fn delta_width_bytes(delta: i128) -> usize {
	if i8::try_from(delta).is_ok() {
		1
	} else if i16::try_from(delta).is_ok() {
		2
	} else if i32::try_from(delta).is_ok() {
		4
	} else if i64::try_from(delta).is_ok() {
		8
	} else {
		16
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::SimdPageValueCount;
	use std::collections::BTreeSet;
	use std::process::Command;

	fn fsst_config() -> EncodingWriterConfig {
		EncodingWriterConfig {
			enabled: true,
			min_buffer_bytes_for_encoding: RecordBodyByteLen::new(1),
			enabled_cascades: BTreeSet::from([
				EncodingCascadeKind::Plain,
				EncodingCascadeKind::FsstWholeBuffer,
				EncodingCascadeKind::FsstRowSafe,
			]),
			..EncodingWriterConfig::default()
		}
	}

	fn variable_width_payload(values: &[&[u8]]) -> (Vec<u8>, Vec<u64>) {
		let mut input = Vec::new();
		let mut offsets = Vec::with_capacity(values.len() + 1);
		offsets.push(0_u64);
		for value in values {
			input.extend_from_slice(value);
			offsets.push(input.len() as u64);
		}
		(input, offsets)
	}

	fn validity_bitmap(rows: usize, mut is_valid: impl FnMut(usize) -> bool) -> Vec<u8> {
		let mut bytes = vec![0_u8; rows.div_ceil(8)];
		for row in 0..rows {
			if is_valid(row) {
				bytes[row / 8] |= 1_u8 << (row % 8);
			}
		}
		bytes
	}

	#[test]
	fn select_validity_cascade_emits_page_local_rle_for_clustered_bitmap() {
		let raw = validity_bitmap(4096, |row| row < 2048);
		let spans = (0..4)
			.map(|page| ValuePageRowSpan {
				first_value_index: BufferValueIndex::new(page * 1024),
				value_count: BufferValueCount::new(1024),
			})
			.collect::<Vec<_>>();
		let plan = select_validity_cascade(&raw, &spans, 2048).unwrap();
		assert_eq!(plan.raw_byte_len, 512);
		assert_eq!(
			plan.page_end_offsets
				.iter()
				.map(|offset| offset.get())
				.collect::<Vec<_>>(),
			vec![5, 10, 15, 20]
		);
		assert_eq!(plan.run_count, RunCount::new(4));
		assert_eq!(plan.encoded_bytes.len(), 20);
	}

	#[test]
	fn select_validity_cascade_declines_all_valid_and_small_raw_buffers() {
		let raw = validity_bitmap(4096, |_| true);
		let spans = [ValuePageRowSpan {
			first_value_index: BufferValueIndex::ZERO,
			value_count: BufferValueCount::new(4096),
		}];
		assert!(select_validity_cascade(&raw, &spans, 0).is_none());
		assert!(select_validity_cascade(&raw[..255], &spans, 1).is_none());
	}

	#[test]
	fn select_validity_cascade_declines_random_nullity() {
		let rows = 4096;
		let mut state = 0x9e37_79b9_7f4a_7c15_u64;
		let raw = validity_bitmap(rows, |_| {
			state = state
				.wrapping_mul(6_364_136_223_846_793_005)
				.wrapping_add(1);
			(state >> 63) != 0
		});
		let spans = (0..4)
			.map(|page| ValuePageRowSpan {
				first_value_index: BufferValueIndex::new(page * 1024),
				value_count: BufferValueCount::new(1024),
			})
			.collect::<Vec<_>>();
		let null_count = rows
			- raw
				.iter()
				.map(|byte| byte.count_ones() as usize)
				.sum::<usize>();
		assert!(null_count > 0);
		assert!(select_validity_cascade(&raw, &spans, null_count).is_none());
	}

	#[test]
	fn validity_rle_economics_declines_threshold_equality() {
		assert_eq!(validity_rle_strictly_beats_raw(383, 512), Some(true));
		assert_eq!(validity_rle_strictly_beats_raw(384, 512), Some(false));
	}

	#[test]
	fn validity_page_extraction_rebases_non_byte_aligned_span() {
		let raw = validity_bitmap(32, |row| matches!(row, 3 | 4 | 7 | 11));
		let page = extract_page_local_validity(
			&raw,
			ValuePageRowSpan {
				first_value_index: BufferValueIndex::new(3),
				value_count: BufferValueCount::new(9),
			},
		)
		.unwrap();
		assert_eq!(page, vec![0b0001_0011, 0b0000_0001]);
	}

	fn encode_with_routing(
		input: &[u8],
		data_type: &DataType,
		decoded_value_count: BufferValueCount,
		config: &EncodingWriterConfig,
		offsets: &[u64],
		routing: FsstRowSafeEstimatorRouting,
	) -> EncodedBuffer {
		let cascade = select_fsst_candidate_with_routing(
			input,
			data_type,
			decoded_value_count,
			config,
			Some(offsets),
			routing,
		)
		.unwrap()
		.expect("FSST route should select a cascade");
		encode_buffer_with_value_offsets(
			input,
			data_type,
			cascade,
			decoded_value_count,
			SimdPageValueCount::new(decoded_value_count.get().max(1)),
			Some(offsets),
		)
		.unwrap()
	}

	#[test]
	fn fsst_writer_context_warms_column_cache_from_exact_route() {
		let row = vec![b'a'; 128];
		let values = vec![row.as_slice(); 16];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();
		let column = IpcColumnOrdinal::new(7);
		let mut context = WriterCascadeContext::default();

		let selected = select_fsst_candidate_with_routing_and_context(
			&input,
			&DataType::Utf8,
			BufferValueCount::new(values.len() as u32),
			&config,
			Some(&offsets),
			FsstRowSafeEstimatorRouting::Estimator,
			Some(&mut context),
			Some(column),
		)
		.unwrap();

		assert!(matches!(
			selected,
			Some(EncodingCascade::FsstRowSafe { .. })
		));
		assert!(context.fsst_tables_by_column.contains_key(&column));
		assert_eq!(context.fsst_tables_by_column.len(), 1);
	}

	#[test]
	fn fsst_estimator_route_matches_exact_route_on_row_safe_winner() {
		let row = vec![b'a'; 128];
		let values = vec![row.as_slice(); 16];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();

		let exact = select_fsst_candidate_with_routing(
			&input,
			&DataType::Utf8,
			BufferValueCount::new(values.len() as u32),
			&config,
			Some(&offsets),
			FsstRowSafeEstimatorRouting::ExactOnly,
		)
		.unwrap();
		let estimated = select_fsst_candidate_with_routing(
			&input,
			&DataType::Utf8,
			BufferValueCount::new(values.len() as u32),
			&config,
			Some(&offsets),
			FsstRowSafeEstimatorRouting::Estimator,
		)
		.unwrap();

		assert_eq!(estimated, exact);
		assert!(matches!(
			estimated,
			Some(EncodingCascade::FsstRowSafe { .. })
		));
	}

	#[test]
	fn fsst_estimator_route_emits_exact_bytes_on_row_safe_winner() {
		let row = vec![b'a'; 128];
		let values = vec![row.as_slice(); 16];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();
		let decoded_value_count = BufferValueCount::new(values.len() as u32);

		let exact = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::ExactOnly,
		);
		let estimated = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::Estimator,
		);

		assert_eq!(estimated.cascade, exact.cascade);
		assert_eq!(estimated.bytes, exact.bytes);
		assert!(matches!(
			estimated.cascade,
			EncodingCascade::FsstRowSafe { .. }
		));
	}

	#[test]
	fn fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_8k() {
		assert_estimator_matches_exact_on_warehouse_winners(8_192);
	}

	#[test]
	#[ignore]
	fn fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_all_tiers() {
		for rows in [8_192_usize, 65_536, 1_048_576] {
			assert_estimator_matches_exact_on_warehouse_winners(rows);
		}
	}

	#[test]
	fn fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_8k() {
		assert_estimator_matches_exact_predicate_masks_on_warehouse_winners(8_192);
	}

	#[test]
	#[ignore]
	fn fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_all_tiers()
	 {
		for rows in [8_192_usize, 65_536, 1_048_576] {
			assert_estimator_matches_exact_predicate_masks_on_warehouse_winners(rows);
		}
	}

	#[test]
	fn fsst_estimator_metadata_decline_matches_exact_fallback() {
		let values = vec![b"a".as_slice(); 512];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();

		let exact = select_fsst_candidate_with_routing(
			&input,
			&DataType::Utf8,
			BufferValueCount::new(values.len() as u32),
			&config,
			Some(&offsets),
			FsstRowSafeEstimatorRouting::ExactOnly,
		)
		.unwrap();
		let estimated = select_fsst_candidate_with_routing(
			&input,
			&DataType::Utf8,
			BufferValueCount::new(values.len() as u32),
			&config,
			Some(&offsets),
			FsstRowSafeEstimatorRouting::Estimator,
		)
		.unwrap();

		assert_eq!(estimated, exact);
		assert!(matches!(
			estimated,
			Some(EncodingCascade::FsstWholeBuffer { .. })
		));
	}

	#[test]
	fn fsst_estimator_decline_route_emits_exact_fallback_bytes() {
		let values = vec![b"a".as_slice(); 512];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();
		let decoded_value_count = BufferValueCount::new(values.len() as u32);

		let exact = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::ExactOnly,
		);
		let estimated = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::Estimator,
		);

		assert_eq!(estimated.cascade, exact.cascade);
		assert_eq!(estimated.bytes, exact.bytes);
		assert!(matches!(
			estimated.cascade,
			EncodingCascade::FsstWholeBuffer { .. }
		));
	}

	#[test]
	fn fsst_estimator_internal_routings_are_cross_process_deterministic() {
		if std::env::var_os("ADR068_DETERMINISM_CHILD").is_some() {
			println!("ADR068_DETERMINISM {}", estimator_route_digest());
			return;
		}

		let left = run_determinism_child(
			"fsst_estimator_internal_routings_are_cross_process_deterministic",
			"ADR068_DETERMINISM_CHILD",
			"ADR068_DETERMINISM ",
			false,
		);
		let right = run_determinism_child(
			"fsst_estimator_internal_routings_are_cross_process_deterministic",
			"ADR068_DETERMINISM_CHILD",
			"ADR068_DETERMINISM ",
			false,
		);
		assert_eq!(left, right);
	}

	#[test]
	#[ignore]
	fn fsst_estimator_warehouse_winner_matrix_is_cross_process_deterministic() {
		if std::env::var_os("ADR068_MATRIX_DETERMINISM_CHILD").is_some() {
			println!(
				"ADR068_MATRIX_DETERMINISM {}",
				warehouse_winner_matrix_digest()
			);
			return;
		}

		let left = run_determinism_child(
			"fsst_estimator_warehouse_winner_matrix_is_cross_process_deterministic",
			"ADR068_MATRIX_DETERMINISM_CHILD",
			"ADR068_MATRIX_DETERMINISM ",
			true,
		);
		let right = run_determinism_child(
			"fsst_estimator_warehouse_winner_matrix_is_cross_process_deterministic",
			"ADR068_MATRIX_DETERMINISM_CHILD",
			"ADR068_MATRIX_DETERMINISM ",
			true,
		);
		assert_eq!(left, right);
	}

	fn run_determinism_child(
		test_name: &str,
		child_env: &str,
		line_prefix: &str,
		ignored: bool,
	) -> String {
		let current_exe = std::env::current_exe().unwrap();
		let mut command = Command::new(&current_exe);
		command.arg(test_name);
		if ignored {
			command.arg("--ignored");
		}
		let output = command
			.arg("--nocapture")
			.env(child_env, "1")
			.output()
			.unwrap();
		assert!(
			output.status.success(),
			"child determinism probe failed: {}",
			String::from_utf8_lossy(&output.stderr)
		);
		String::from_utf8(output.stdout)
			.unwrap()
			.lines()
			.find_map(|line| {
				line.strip_prefix(line_prefix)
					.map(str::to_owned)
					.or_else(|| {
						line.split_once(" ... ")
							.and_then(|(_, rest)| rest.strip_prefix(line_prefix))
							.map(str::to_owned)
					})
			})
			.expect("child determinism digest line")
	}

	fn estimator_route_digest() -> String {
		let row = vec![b'a'; 128];
		let values = vec![row.as_slice(); 16];
		let (input, offsets) = variable_width_payload(&values);
		let config = fsst_config();
		let decoded_value_count = BufferValueCount::new(values.len() as u32);
		let exact = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::ExactOnly,
		);
		let estimated = encode_with_routing(
			&input,
			&DataType::Utf8,
			decoded_value_count,
			&config,
			&offsets,
			FsstRowSafeEstimatorRouting::Estimator,
		);
		format!(
			"exact={}:{}:{} estimator={}:{}:{}",
			exact.cascade.kind().as_str(),
			exact.bytes.len(),
			blake3::hash(&exact.bytes).to_hex(),
			estimated.cascade.kind().as_str(),
			estimated.bytes.len(),
			blake3::hash(&estimated.bytes).to_hex(),
		)
	}

	fn warehouse_winner_matrix_digest() -> String {
		let config = fsst_config();
		let mut digest = String::new();
		for rows in [8_192_usize, 65_536, 1_048_576] {
			for (name, build_values) in warehouse_winner_fixture_builders() {
				let values = build_values(rows);
				let refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
				let (input, offsets) = variable_width_payload(&refs);
				let decoded_value_count = BufferValueCount::new(values.len() as u32);
				let exact = encode_with_routing(
					&input,
					&DataType::Utf8,
					decoded_value_count,
					&config,
					&offsets,
					FsstRowSafeEstimatorRouting::ExactOnly,
				);
				let estimated = encode_with_routing(
					&input,
					&DataType::Utf8,
					decoded_value_count,
					&config,
					&offsets,
					FsstRowSafeEstimatorRouting::Estimator,
				);
				digest.push_str(&format!(
					"{name}:{rows}:exact={}:{}:{}:estimator={}:{}:{};",
					exact.cascade.kind().as_str(),
					exact.bytes.len(),
					blake3::hash(&exact.bytes).to_hex(),
					estimated.cascade.kind().as_str(),
					estimated.bytes.len(),
					blake3::hash(&estimated.bytes).to_hex(),
				));
			}
		}
		digest
	}

	fn assert_estimator_matches_exact_on_warehouse_winners(rows: usize) {
		let config = fsst_config();
		for (name, build_values) in warehouse_winner_fixture_builders() {
			let values = build_values(rows);
			let refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
			let (input, offsets) = variable_width_payload(&refs);
			let decoded_value_count = BufferValueCount::new(values.len() as u32);

			let exact = encode_with_routing(
				&input,
				&DataType::Utf8,
				decoded_value_count,
				&config,
				&offsets,
				FsstRowSafeEstimatorRouting::ExactOnly,
			);
			let estimated = encode_with_routing(
				&input,
				&DataType::Utf8,
				decoded_value_count,
				&config,
				&offsets,
				FsstRowSafeEstimatorRouting::Estimator,
			);

			assert_eq!(estimated.cascade, exact.cascade, "{name} rows={rows}");
			assert_eq!(estimated.bytes, exact.bytes, "{name} rows={rows}");
			assert!(
				matches!(estimated.cascade, EncodingCascade::FsstRowSafe { .. }),
				"{name} rows={rows}: expected exact route to select FsstRowSafe"
			);
		}
	}

	fn warehouse_winner_fixture_builders() -> [(&'static str, fn(usize) -> Vec<Vec<u8>>); 5] {
		[
			("url_eq_5p", url_eq_values),
			("url_inlist4_5p", url_inlist4_values),
			("long_pair_eq_5p", long_pair_values),
			("weak_compression", weak_compression_values),
			("log_line_eq_5p", log_line_values),
		]
	}

	fn warehouse_winner_predicate_literals() -> [(&'static str, &'static [u8]); 5] {
		[
			(
				"url_eq_5p",
				b"/tenant/alpha/catalog/product/000000/details",
			),
			(
				"url_inlist4_5p",
				b"/tenant/alpha/catalog/product/000000/details",
			),
			(
				"long_pair_eq_5p",
				b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/selected/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			),
			("weak_compression", b"absent_literal_not_in_fixture"),
			(
				"log_line_eq_5p",
				b"level=error service=checkout region=us-east-1 status=500 message=payment_authorization_timeout",
			),
		]
	}

	fn assert_estimator_matches_exact_predicate_masks_on_warehouse_winners(rows: usize) {
		use crate::schema::pbc::ColumnPredicateLiteralV1;
		use crate::schema::{
			EncodingCascadeIndex, EncodingPageByteOffset, IpcBufferIndex, RowGroupIndex,
		};
		use crate::storage::encoding::{
			BufferEncoding, EncodedBufferByteRange, PredicateAtom, PredicateAtomOp, coerce_literal,
			evaluate_fsst_row_safe_predicate,
		};

		let config = fsst_config();
		let data_type = DataType::Utf8;
		let literals = warehouse_winner_predicate_literals();
		for ((name, build_values), (literal_name, literal_bytes)) in
			warehouse_winner_fixture_builders()
				.iter()
				.zip(literals.iter())
		{
			assert_eq!(name, literal_name, "fixture/literal table alignment");
			let values = build_values(rows);
			let refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
			let (input, offsets) = variable_width_payload(&refs);
			let decoded_value_count = BufferValueCount::new(values.len() as u32);

			let exact = encode_with_routing(
				&input,
				&data_type,
				decoded_value_count,
				&config,
				&offsets,
				FsstRowSafeEstimatorRouting::ExactOnly,
			);
			let estimated = encode_with_routing(
				&input,
				&data_type,
				decoded_value_count,
				&config,
				&offsets,
				FsstRowSafeEstimatorRouting::Estimator,
			);
			assert_eq!(estimated.cascade, exact.cascade, "{name} rows={rows}");
			assert_eq!(estimated.bytes, exact.bytes, "{name} rows={rows}");
			assert!(
				matches!(exact.cascade, EncodingCascade::FsstRowSafe { .. }),
				"{name} rows={rows}: warehouse-winner fixture must emit FsstRowSafe"
			);

			let offset_bytes: Vec<u8> = offsets
				.iter()
				.map(|&o| i32::try_from(o).expect("offset fits i32 in test fixture"))
				.flat_map(|o| o.to_le_bytes())
				.collect();
			let encoding = BufferEncoding {
				row_group_index: RowGroupIndex::new(0),
				ipc_column_ordinal: IpcColumnOrdinal::new(0),
				ipc_buffer_ordinal: IpcBufferIndex::new(2),
				cascade_index: EncodingCascadeIndex::new(0),
				decoded_byte_len: RecordBodyByteLen::new(input.len() as u64),
				decoded_value_count,
				encoded_range: EncodedBufferByteRange {
					start: EncodingPageByteOffset::ZERO,
					len: RecordBodyByteLen::new(exact.bytes.len() as u64),
				},
				page_value_count: SimdPageValueCount::new(decoded_value_count.get().max(1)),
				page_end_offsets: Vec::new(),
				page_stats: Vec::new(),
			};

			let literal = ColumnPredicateLiteralV1::Str(literal_bytes.to_vec());
			let atom = PredicateAtom {
				op: PredicateAtomOp::Eq,
				literal: Some(literal.clone()),
			};
			let coercion = coerce_literal(Some(&literal), &data_type).unwrap();
			let exact_mask = evaluate_fsst_row_safe_predicate(
				&exact.bytes,
				&encoding,
				&offset_bytes,
				None,
				None,
				&data_type,
				&atom,
				&coercion,
			)
			.unwrap();
			let estimated_mask = evaluate_fsst_row_safe_predicate(
				&estimated.bytes,
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
				estimated_mask, exact_mask,
				"{name} rows={rows}: predicate masks must be byte-identical across routings"
			);
		}
	}

	fn url_eq_values(rows: usize) -> Vec<Vec<u8>> {
		let selected_rows = rows.saturating_div(20).max(1);
		(0..rows)
			.map(|row| {
				if row < selected_rows {
					"/tenant/alpha/catalog/product/000000/details"
						.as_bytes()
						.to_vec()
				} else {
					format!(
						"/tenant/alpha/catalog/product/{:06}/details",
						100_000 + row % 900_000
					)
					.into_bytes()
				}
			})
			.collect()
	}

	fn url_inlist4_values(rows: usize) -> Vec<Vec<u8>> {
		let selected_rows = rows.saturating_div(20).max(1);
		(0..rows)
			.map(|row| {
				if row < selected_rows {
					format!("/tenant/alpha/catalog/product/{:06}/details", row % 4).into_bytes()
				} else {
					format!(
						"/tenant/alpha/catalog/product/{:06}/details",
						100_000 + row % 900_000
					)
					.into_bytes()
				}
			})
			.collect()
	}

	fn long_pair_values(rows: usize) -> Vec<Vec<u8>> {
		let selected_rows = rows.saturating_div(20).max(1);
		(0..rows)
			.map(|row| {
				if row < selected_rows {
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/selected/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
						.as_bytes()
						.to_vec()
				} else {
					format!(
						"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/{row:08}/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
					)
					.into_bytes()
				}
			})
			.collect()
	}

	fn weak_compression_values(rows: usize) -> Vec<Vec<u8>> {
		(0..rows)
			.map(|row| {
				let mixed = (row as u64)
					.wrapping_mul(0x9e37_79b9_7f4a_7c15)
					.rotate_left((row % 31) as u32);
				format!("{mixed:016x}{:08x}", row as u32 ^ 0xa5a5_5a5a).into_bytes()
			})
			.collect()
	}

	fn log_line_values(rows: usize) -> Vec<Vec<u8>> {
		let selected_rows = rows.saturating_div(20).max(1);
		(0..rows)
			.map(|row| {
				if row < selected_rows {
					"level=error service=checkout region=us-east-1 status=500 message=payment_authorization_timeout"
						.as_bytes()
						.to_vec()
				} else {
					format!(
						"level=info service=checkout region=us-east-1 status=200 request_id={row:08} message=completed_cart_checkout user={:06}",
						row % 100_000
					)
					.into_bytes()
				}
			})
			.collect()
	}
}
