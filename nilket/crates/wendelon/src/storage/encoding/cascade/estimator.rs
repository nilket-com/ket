use crate::schema::{BufferValueCount, SampleRowCount};
use crate::storage::encoding::cascade::{
	FsstSymbolTable, fsst_payload_len_with_symbol_map, fsst_row_safe_encoded_len_with_table,
	fsst_row_safe_metadata_bytes, fsst_row_safe_metadata_floor_bytes,
	fsst_row_safe_metadata_payload_floor_bytes, fsst_whole_buffer_encoded_len_with_table,
	train_fsst_row_safe_and_whole_buffer_symbol_tables, train_fsst_row_safe_symbol_table,
};
use crate::storage::encoding::sample::deterministic_sample_indices;

pub(crate) const EARLY_DECLINE_SAFETY_MULTIPLIER_NUMERATOR: usize = 6;
pub(crate) const EARLY_DECLINE_SAFETY_MULTIPLIER_DENOMINATOR: usize = 5;
const CROSS_BUFFER_SAMPLE_FLOOR_PRESSURE_NUMERATOR: usize = 1;
const CROSS_BUFFER_SAMPLE_FLOOR_PRESSURE_DENOMINATOR: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeEstimatorDecision {
	Decline(FsstRowSafeEstimatorDecline),
	RequireExactGuard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeEstimatorDecline {
	MetadataFloorExceedsPlaintext,
	MetadataPayloadFloorExceedsPlaintext,
	TrainedMetadataFloorExceedsPlaintext,
	SampledRowSafeExceedsPlaintext,
	SampledRowSafeExceedsWholeBufferGuard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeEstimatorSampleShape {
	DeterministicSpread,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeEstimatorRouting {
	ExactOnly,
	Estimator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FsstRowSafeSampleParts {
	pub(crate) payload_len: usize,
	pub(crate) exception_count: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FsstRowSafeEstimatorStats {
	pub(crate) sampled_rows: usize,
	pub(crate) estimated_fsst_whole_buffer_encoded_len: Option<usize>,
	pub(crate) estimated_fsst_row_safe_encoded_len: Option<usize>,
	pub(crate) decision: Option<FsstRowSafeEstimatorDecision>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeEstimatorTableSource {
	CurrentBuffer,
	CrossBufferCached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsstRowSafeExactLengths {
	CurrentBuffer {
		row_safe_len: usize,
		whole_buffer_len: Option<usize>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FsstRowSafeEstimatorOutcome {
	pub(crate) decision: FsstRowSafeEstimatorDecision,
	pub(crate) exact_lengths: Option<FsstRowSafeExactLengths>,
	pub(crate) stats: FsstRowSafeEstimatorStats,
}

pub(crate) fn estimate_fsst_row_safe_cost_free_decline(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	value_offsets: &[u64],
) -> Option<FsstRowSafeEstimatorDecline> {
	let row_count = decoded_value_count.get() as usize;
	if let Some(metadata_floor) = fsst_row_safe_metadata_floor_bytes(row_count)
		&& metadata_floor > input.len()
	{
		return Some(FsstRowSafeEstimatorDecline::MetadataFloorExceedsPlaintext);
	}
	if let Some(metadata_payload_floor) =
		fsst_row_safe_metadata_payload_floor_bytes(row_count, input.len(), value_offsets)
		&& metadata_payload_floor > input.len()
	{
		return Some(FsstRowSafeEstimatorDecline::MetadataPayloadFloorExceedsPlaintext);
	}
	None
}

pub(crate) fn estimate_fsst_row_safe(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	sample_rows: SampleRowCount,
	value_offsets: &[u64],
	whole_buffer_guard_enabled: bool,
) -> std::io::Result<FsstRowSafeEstimatorOutcome> {
	let stats = FsstRowSafeEstimatorStats::default();
	if let Some(reason) =
		estimate_fsst_row_safe_cost_free_decline(input, decoded_value_count, value_offsets)
	{
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::Decline(reason),
			None,
			stats,
		));
	}
	if fsst_row_safe_metadata_floor_bytes(decoded_value_count.get() as usize).is_none() {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			None,
			stats,
		));
	}

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
	estimate_fsst_row_safe_with_tables(
		input,
		decoded_value_count,
		sample_rows,
		value_offsets,
		&row_safe_table,
		whole_buffer_table.as_ref(),
		FsstRowSafeEstimatorTableSource::CurrentBuffer,
	)
}

pub(crate) fn estimate_fsst_row_safe_with_cached_tables(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	_sample_rows: SampleRowCount,
	value_offsets: &[u64],
	_row_safe_table: &FsstSymbolTable,
	_whole_buffer_table: Option<&FsstSymbolTable>,
	_whole_buffer_guard_enabled: bool,
) -> std::io::Result<FsstRowSafeEstimatorOutcome> {
	let stats = FsstRowSafeEstimatorStats::default();
	if let Some(reason) =
		estimate_fsst_row_safe_cost_free_decline(input, decoded_value_count, value_offsets)
	{
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::Decline(reason),
			None,
			stats,
		));
	}
	Ok(outcome(
		FsstRowSafeEstimatorDecision::RequireExactGuard,
		None,
		stats,
	))
}

fn estimate_fsst_row_safe_with_tables(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	sample_rows: SampleRowCount,
	value_offsets: &[u64],
	row_safe_table: &FsstSymbolTable,
	whole_buffer_table: Option<&FsstSymbolTable>,
	table_source: FsstRowSafeEstimatorTableSource,
) -> std::io::Result<FsstRowSafeEstimatorOutcome> {
	let row_count = decoded_value_count.get() as usize;
	let mut stats = FsstRowSafeEstimatorStats::default();
	if let Some(trained_floor) =
		fsst_row_safe_metadata_bytes(row_count, row_safe_table.symbols.len(), 0)
	{
		if trained_floor > input.len() {
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::Decline(
					FsstRowSafeEstimatorDecline::TrainedMetadataFloorExceedsPlaintext,
				),
				None,
				stats,
			));
		}
		if table_source == FsstRowSafeEstimatorTableSource::CrossBufferCached
			&& !cross_buffer_sample_pressure_high_enough(trained_floor, input.len())
		{
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::RequireExactGuard,
				None,
				stats,
			));
		}
	} else {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			None,
			stats,
		));
	}

	let sample_indices = sample_indices_for_shape(
		row_count,
		sample_rows.get() as usize,
		FsstRowSafeEstimatorSampleShape::DeterministicSpread,
	);
	stats.sampled_rows = sample_indices.len();
	if sample_indices.is_empty() {
		let exact_lengths = match table_source {
			FsstRowSafeEstimatorTableSource::CurrentBuffer => exact_lengths(
				input,
				decoded_value_count,
				value_offsets,
				row_safe_table,
				whole_buffer_table,
			)?,
			FsstRowSafeEstimatorTableSource::CrossBufferCached => None,
		};
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			exact_lengths,
			stats,
		));
	}

	let row_safe_sample =
		encode_fsst_row_safe_sample_parts(input, value_offsets, &sample_indices, row_safe_table)?;
	let projected_row_safe_payload =
		project_sample_to_full(row_safe_sample.payload_len, sample_indices.len(), row_count);
	let projected_exception_count = project_sample_to_full(
		row_safe_sample.exception_count,
		sample_indices.len(),
		row_count,
	);
	let (Some(projected_row_safe_payload), Some(projected_exception_count)) =
		(projected_row_safe_payload, projected_exception_count)
	else {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			None,
			stats,
		));
	};
	let Some(row_safe_metadata) = fsst_row_safe_metadata_bytes(
		row_count,
		row_safe_table.symbols.len(),
		projected_exception_count,
	) else {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			None,
			stats,
		));
	};
	let Some(estimated_row_safe_len) = row_safe_metadata.checked_add(projected_row_safe_payload)
	else {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::RequireExactGuard,
			None,
			stats,
		));
	};
	stats.estimated_fsst_row_safe_encoded_len = Some(estimated_row_safe_len);

	if table_source == FsstRowSafeEstimatorTableSource::CrossBufferCached
		&& let Some(reason) =
			sample_confidence_decline_reason(estimated_row_safe_len, input.len(), None)
	{
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::Decline(reason),
			None,
			stats,
		));
	}

	let estimated_whole_buffer_len = if let Some(whole_buffer_table) = whole_buffer_table {
		let sampled_whole_payload = encode_fsst_whole_buffer_sample_len(
			input,
			value_offsets,
			&sample_indices,
			whole_buffer_table,
		)?;
		let Some(projected_whole_payload) =
			project_sample_to_full(sampled_whole_payload, sample_indices.len(), row_count)
		else {
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::RequireExactGuard,
				None,
				stats,
			));
		};
		let Some(symbol_bytes) = whole_buffer_table.symbols.len().checked_mul(2) else {
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::RequireExactGuard,
				None,
				stats,
			));
		};
		let Some(header_len) = 5_usize.checked_add(symbol_bytes) else {
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::RequireExactGuard,
				None,
				stats,
			));
		};
		let Some(estimated_len) = header_len.checked_add(projected_whole_payload) else {
			return Ok(outcome(
				FsstRowSafeEstimatorDecision::RequireExactGuard,
				None,
				stats,
			));
		};
		Some(estimated_len)
	} else {
		None
	};
	stats.estimated_fsst_whole_buffer_encoded_len = estimated_whole_buffer_len;

	if let Some(reason) = sample_confidence_decline_reason(
		estimated_row_safe_len,
		input.len(),
		estimated_whole_buffer_len,
	) {
		return Ok(outcome(
			FsstRowSafeEstimatorDecision::Decline(reason),
			None,
			stats,
		));
	}

	let exact_lengths = match table_source {
		FsstRowSafeEstimatorTableSource::CurrentBuffer => exact_lengths(
			input,
			decoded_value_count,
			value_offsets,
			row_safe_table,
			whole_buffer_table,
		)?,
		FsstRowSafeEstimatorTableSource::CrossBufferCached => None,
	};
	Ok(outcome(
		FsstRowSafeEstimatorDecision::RequireExactGuard,
		exact_lengths,
		stats,
	))
}

fn cross_buffer_sample_pressure_high_enough(
	trained_floor: usize,
	decoded_plaintext_len: usize,
) -> bool {
	trained_floor.saturating_mul(CROSS_BUFFER_SAMPLE_FLOOR_PRESSURE_DENOMINATOR)
		>= decoded_plaintext_len.saturating_mul(CROSS_BUFFER_SAMPLE_FLOOR_PRESSURE_NUMERATOR)
}

fn outcome(
	decision: FsstRowSafeEstimatorDecision,
	exact_lengths: Option<FsstRowSafeExactLengths>,
	mut stats: FsstRowSafeEstimatorStats,
) -> FsstRowSafeEstimatorOutcome {
	stats.decision = Some(decision);
	FsstRowSafeEstimatorOutcome {
		decision,
		exact_lengths,
		stats,
	}
}

fn exact_lengths(
	input: &[u8],
	decoded_value_count: BufferValueCount,
	value_offsets: &[u64],
	row_safe_table: &FsstSymbolTable,
	whole_buffer_table: Option<&FsstSymbolTable>,
) -> std::io::Result<Option<FsstRowSafeExactLengths>> {
	let Some(row_safe_len) = fsst_row_safe_encoded_len_with_table(
		input,
		decoded_value_count,
		value_offsets,
		row_safe_table,
	)?
	else {
		return Ok(None);
	};
	let whole_buffer_len = if let Some(table) = whole_buffer_table {
		let Some(len) = fsst_whole_buffer_encoded_len_with_table(input, table) else {
			return Ok(None);
		};
		Some(len)
	} else {
		None
	};
	Ok(Some(FsstRowSafeExactLengths::CurrentBuffer {
		row_safe_len,
		whole_buffer_len,
	}))
}

fn sample_indices_for_shape(
	row_count: usize,
	sample_limit: usize,
	shape: FsstRowSafeEstimatorSampleShape,
) -> Vec<usize> {
	match shape {
		FsstRowSafeEstimatorSampleShape::DeterministicSpread => {
			deterministic_sample_indices(row_count, sample_limit.max(1))
		}
	}
}

fn encode_fsst_whole_buffer_sample_len(
	input: &[u8],
	value_offsets: &[u64],
	sample_indices: &[usize],
	table: &FsstSymbolTable,
) -> std::io::Result<usize> {
	let mut payload_len = 0_usize;
	for row_index in sample_indices {
		let start = value_offsets[*row_index] as usize;
		let end = value_offsets[row_index.saturating_add(1)] as usize;
		let row_len = fsst_payload_len_with_symbol_map(&input[start..end], &table.pair_to_symbol)
			.map_err(map_fsst_literal_encode_error)?;
		payload_len = payload_len.checked_add(row_len).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding FSST estimator whole-buffer sample length overflow",
			)
		})?;
	}
	Ok(payload_len)
}

fn encode_fsst_row_safe_sample_parts(
	input: &[u8],
	value_offsets: &[u64],
	sample_indices: &[usize],
	table: &FsstSymbolTable,
) -> std::io::Result<FsstRowSafeSampleParts> {
	let mut payload_len = 0_usize;
	let mut exception_count = 0_usize;
	for row_index in sample_indices {
		let start = value_offsets[*row_index] as usize;
		let end = value_offsets[row_index.saturating_add(1)] as usize;
		let row_len = fsst_payload_len_with_symbol_map(&input[start..end], &table.pair_to_symbol)
			.map_err(map_fsst_literal_encode_error)?;
		if row_len >= crate::storage::encoding::cascade::FSST_ROW_SAFE_LENGTH_SENTINEL as usize {
			exception_count = exception_count.saturating_add(1);
		}
		payload_len = payload_len.checked_add(row_len).ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"Encoding FSST estimator row-safe sample length overflow",
			)
		})?;
	}
	Ok(FsstRowSafeSampleParts {
		payload_len,
		exception_count,
	})
}

fn project_sample_to_full(sampled: usize, sampled_rows: usize, total_rows: usize) -> Option<usize> {
	if total_rows == 0 {
		return Some(0);
	}
	if sampled_rows == 0 {
		return None;
	}
	let numerator = (sampled as u128).checked_mul(total_rows as u128)?;
	let projected = numerator.div_ceil(sampled_rows as u128);
	usize::try_from(projected).ok()
}

fn sample_confidence_decline_reason(
	estimated_row_safe_len: usize,
	decoded_plaintext_len: usize,
	estimated_whole_buffer_len: Option<usize>,
) -> Option<FsstRowSafeEstimatorDecline> {
	let plaintext_threshold = multiply_ratio_ceil(
		decoded_plaintext_len,
		EARLY_DECLINE_SAFETY_MULTIPLIER_NUMERATOR,
		EARLY_DECLINE_SAFETY_MULTIPLIER_DENOMINATOR,
	)?;
	let whole_threshold = if let Some(whole_len) = estimated_whole_buffer_len {
		let whole_guard = multiply_ratio_ceil(whole_len, 11, 10)?;
		multiply_ratio_ceil(
			whole_guard,
			EARLY_DECLINE_SAFETY_MULTIPLIER_NUMERATOR,
			EARLY_DECLINE_SAFETY_MULTIPLIER_DENOMINATOR,
		)?
	} else {
		0
	};
	if estimated_row_safe_len <= plaintext_threshold.max(whole_threshold) {
		return None;
	}
	if plaintext_threshold >= whole_threshold {
		Some(FsstRowSafeEstimatorDecline::SampledRowSafeExceedsPlaintext)
	} else {
		Some(FsstRowSafeEstimatorDecline::SampledRowSafeExceedsWholeBufferGuard)
	}
}

fn multiply_ratio_ceil(value: usize, numerator: usize, denominator: usize) -> Option<usize> {
	if denominator == 0 {
		return None;
	}
	let scaled = (value as u128).checked_mul(numerator as u128)?;
	usize::try_from(scaled.div_ceil(denominator as u128)).ok()
}

fn map_fsst_literal_encode_error(
	error: crate::storage::encoding::EncodingRejectCode,
) -> std::io::Error {
	std::io::Error::new(
		std::io::ErrorKind::InvalidData,
		format!(
			"Encoding FSST estimator literal encode failed: {}",
			error.as_str()
		),
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::encoding::cascade::FSST_ROW_SAFE_FIXED_HEADER_BYTES;
	use crate::storage::encoding::cascade::fsst_row_safe_payload_parts_with_table;
	use std::hint::black_box;
	use std::time::{Duration, Instant};

	#[test]
	fn metadata_floor_tracks_row_safe_frame_layout() {
		assert_eq!(
			fsst_row_safe_metadata_floor_bytes(0),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES)
		);
		assert_eq!(
			fsst_row_safe_metadata_floor_bytes(1),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 8 + 1)
		);
		assert_eq!(
			fsst_row_safe_metadata_floor_bytes(255),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 8 + 255)
		);
		assert_eq!(
			fsst_row_safe_metadata_floor_bytes(256),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 8 + 256)
		);
		assert_eq!(
			fsst_row_safe_metadata_floor_bytes(257),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 16 + 257)
		);
		assert_eq!(
			fsst_row_safe_metadata_bytes(257, 3, 2),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 6 + 16 + 257 + 8)
		);
		assert_eq!(
			fsst_row_safe_metadata_payload_floor_bytes(3, 3, &[0, 1, 1, 3]),
			Some(FSST_ROW_SAFE_FIXED_HEADER_BYTES + 8 + 3 + 2)
		);
		assert_eq!(fsst_row_safe_metadata_floor_bytes(usize::MAX), None);
		assert_eq!(fsst_row_safe_metadata_bytes(1, usize::MAX, 0), None);
		assert_eq!(fsst_row_safe_metadata_bytes(1, 0, usize::MAX), None);
		assert_eq!(
			fsst_row_safe_metadata_payload_floor_bytes(2, 3, &[0, 1, 4]),
			None
		);
	}

	#[test]
	fn cost_free_metadata_floor_declines_without_training() {
		let input = Vec::new();
		let offsets = vec![0_u64; 33];
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(32),
			SampleRowCount::new(8),
			&offsets,
			true,
		)
		.unwrap();
		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::MetadataFloorExceedsPlaintext
			)
		);
	}

	#[test]
	fn cost_free_payload_floor_declines_without_training() {
		let values = vec![vec![b'a', b'b']; 32];
		let (input, offsets) = variable_width_payload(&values);
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(32),
			SampleRowCount::new(8),
			&offsets,
			true,
		)
		.unwrap();
		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::MetadataPayloadFloorExceedsPlaintext
			)
		);
		assert_eq!(outcome.stats.sampled_rows, 0);
	}

	#[test]
	fn trained_metadata_floor_declines_after_symbol_training() {
		let input = vec![b'a'; 23];
		let offsets = vec![0_u64, input.len() as u64];
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(1),
			SampleRowCount::new(1),
			&offsets,
			true,
		)
		.unwrap();
		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::TrainedMetadataFloorExceedsPlaintext
			)
		);
	}

	#[test]
	fn trained_metadata_floor_uses_strict_plaintext_overrun() {
		let input = vec![b'a'; 24];
		let offsets = vec![0_u64, input.len() as u64];
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(1),
			SampleRowCount::new(1),
			&offsets,
			true,
		)
		.unwrap();

		assert_ne!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::TrainedMetadataFloorExceedsPlaintext
			)
		);
	}

	#[test]
	fn cached_tables_require_exact_guard_without_exact_lengths() {
		let row = vec![b'a'; 128];
		let values = vec![row; 16];
		let (input, offsets) = variable_width_payload(&values);
		let decoded_value_count = BufferValueCount::new(values.len() as u32);
		let (row_safe_table, whole_buffer_table) =
			train_fsst_row_safe_and_whole_buffer_symbol_tables(
				&input,
				decoded_value_count,
				&offsets,
			)
			.unwrap();

		let outcome = estimate_fsst_row_safe_with_cached_tables(
			&input,
			decoded_value_count,
			SampleRowCount::new(8),
			&offsets,
			&row_safe_table,
			Some(&whole_buffer_table),
			true,
		)
		.unwrap();

		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::RequireExactGuard
		);
		assert_eq!(outcome.exact_lengths, None);
	}

	#[test]
	fn cached_tables_skip_sample_when_metadata_pressure_is_low() {
		let row = vec![b'a'; 128];
		let values = vec![row; 16];
		let (input, offsets) = variable_width_payload(&values);
		let decoded_value_count = BufferValueCount::new(values.len() as u32);
		let (row_safe_table, whole_buffer_table) =
			train_fsst_row_safe_and_whole_buffer_symbol_tables(
				&input,
				decoded_value_count,
				&offsets,
			)
			.unwrap();

		let outcome = estimate_fsst_row_safe_with_cached_tables(
			&input,
			decoded_value_count,
			SampleRowCount::new(8),
			&offsets,
			&row_safe_table,
			Some(&whole_buffer_table),
			true,
		)
		.unwrap();

		assert_eq!(outcome.stats.sampled_rows, 0);
		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::RequireExactGuard
		);
		assert_eq!(outcome.exact_lengths, None);
	}

	#[test]
	fn cached_tables_do_not_decline_from_cross_buffer_sample() {
		let values = (0..64)
			.map(|row| vec![row as u8, (row * 37) as u8, (row * 73) as u8])
			.collect::<Vec<_>>();
		let (input, offsets) = variable_width_payload(&values);
		let decoded_value_count = BufferValueCount::new(values.len() as u32);
		let (row_safe_table, whole_buffer_table) =
			train_fsst_row_safe_and_whole_buffer_symbol_tables(
				&input,
				decoded_value_count,
				&offsets,
			)
			.unwrap();

		let outcome = estimate_fsst_row_safe_with_cached_tables(
			&input,
			decoded_value_count,
			SampleRowCount::new(1),
			&offsets,
			&row_safe_table,
			Some(&whole_buffer_table),
			true,
		)
		.unwrap();

		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::RequireExactGuard
		);
		assert_eq!(outcome.stats.sampled_rows, 0);
		assert_eq!(outcome.exact_lengths, None);
	}

	#[test]
	fn sample_confidence_reports_dominant_guard() {
		assert_eq!(
			sample_confidence_decline_reason(130, 100, None),
			Some(FsstRowSafeEstimatorDecline::SampledRowSafeExceedsPlaintext)
		);
		assert_eq!(
			sample_confidence_decline_reason(200, 100, Some(140)),
			Some(FsstRowSafeEstimatorDecline::SampledRowSafeExceedsWholeBufferGuard)
		);
		assert_eq!(sample_confidence_decline_reason(120, 100, None), None);
	}

	#[test]
	fn sampled_plaintext_decline_records_diagnostic() {
		let values = [
			(0x80_u8..0x94).collect::<Vec<_>>(),
			(0xa0_u8..0xb4).collect::<Vec<_>>(),
		];
		let (input, offsets) = variable_width_payload(&values);
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(values.len() as u32),
			SampleRowCount::new(1),
			&offsets,
			false,
		)
		.unwrap();

		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::SampledRowSafeExceedsPlaintext
			)
		);
		assert_eq!(outcome.stats.decision, Some(outcome.decision));
	}

	#[test]
	fn sampled_whole_buffer_guard_decline_records_diagnostic() {
		let values = (0..64)
			.map(|row| vec![row as u8, (row * 37) as u8, (row * 73) as u8])
			.collect::<Vec<_>>();
		let (input, offsets) = variable_width_payload(&values);
		let outcome = estimate_fsst_row_safe(
			&input,
			BufferValueCount::new(values.len() as u32),
			SampleRowCount::new(1),
			&offsets,
			true,
		)
		.unwrap();

		assert_eq!(
			outcome.decision,
			FsstRowSafeEstimatorDecision::Decline(
				FsstRowSafeEstimatorDecline::SampledRowSafeExceedsWholeBufferGuard
			)
		);
		assert_eq!(outcome.stats.decision, Some(outcome.decision));
	}

	#[test]
	fn deterministic_spread_sample_shape_bounds_prefix_projection_error() {
		let rows = 8_192_usize;
		let sample_rows = 256_usize;
		let fixtures = [
			("url_eq_5p", url_eq_values(rows), true),
			("url_inlist4_5p", url_inlist4_values(rows), true),
			("long_pair_eq_5p", long_pair_values(rows), true),
			("weak_compression", weak_compression_values(rows), false),
			("log_line_eq_5p", log_line_values(rows), true),
		];
		let mut spread_wins = 0_usize;

		for (name, values, clustered_winner) in fixtures {
			let (input, offsets) = variable_width_payload(&values);
			let decoded_value_count = BufferValueCount::new(values.len() as u32);
			let table =
				train_fsst_row_safe_symbol_table(&input, decoded_value_count, &offsets).unwrap();
			let exact =
				fsst_row_safe_encoded_len_with_table(&input, decoded_value_count, &offsets, &table)
					.unwrap()
					.unwrap();
			let prefix_indices = (0..sample_rows.min(rows)).collect::<Vec<_>>();
			let spread_indices = sample_indices_for_shape(
				rows,
				sample_rows,
				FsstRowSafeEstimatorSampleShape::DeterministicSpread,
			);
			let prefix_estimate =
				projected_row_safe_len(&input, &offsets, rows, &table, &prefix_indices);
			let spread_estimate =
				projected_row_safe_len(&input, &offsets, rows, &table, &spread_indices);
			let prefix_error = prefix_estimate.abs_diff(exact);
			let spread_error = spread_estimate.abs_diff(exact);

			if clustered_winner {
				assert!(
					spread_error <= prefix_error,
					"{name}: deterministic spread error {spread_error} exceeded prefix error {prefix_error}"
				);
			} else {
				assert!(
					spread_error.saturating_mul(100) <= exact.saturating_mul(5),
					"{name}: deterministic spread error {spread_error} exceeded 5% of exact row-safe length {exact}"
				);
			}
			if spread_error < prefix_error {
				spread_wins = spread_wins.saturating_add(1);
			}
		}

		assert!(spread_wins >= 3);
	}

	#[test]
	#[ignore]
	fn fsst_symbol_training_cost_probe() {
		let rows = 8_192_usize;
		let iterations = 20_usize;
		let fixtures = [
			("url_eq_5p", url_eq_values(rows)),
			("url_inlist4_5p", url_inlist4_values(rows)),
			("long_pair_eq_5p", long_pair_values(rows)),
			("weak_compression", weak_compression_values(rows)),
			("log_line_eq_5p", log_line_values(rows)),
		];

		for (name, values) in fixtures {
			let (input, offsets) = variable_width_payload(&values);
			let decoded_value_count = BufferValueCount::new(values.len() as u32);
			let training_elapsed = bench_duration(iterations, || {
				let (row_safe_table, whole_buffer_table) =
					train_fsst_row_safe_and_whole_buffer_symbol_tables(
						&input,
						decoded_value_count,
						&offsets,
					)
					.unwrap();
				black_box((
					row_safe_table.symbols.len(),
					whole_buffer_table.symbols.len(),
				));
			});
			let row_safe_table =
				train_fsst_row_safe_symbol_table(&input, decoded_value_count, &offsets).unwrap();
			let greedy_elapsed = bench_duration(iterations, || {
				let parts =
					fsst_row_safe_payload_parts_with_table(&input, &offsets, &row_safe_table)
						.unwrap();
				black_box(parts);
			});
			println!(
				"ADR068_P5 fixture={name} rows={rows} iterations={iterations} training_micros_per_iter={:.3} row_safe_greedy_micros_per_iter={:.3} training_vs_greedy_micros={}",
				micros_per_iter(training_elapsed, iterations),
				micros_per_iter(greedy_elapsed, iterations),
				duration_ratio_micros(training_elapsed, greedy_elapsed),
			);
		}
	}

	fn bench_duration(iterations: usize, mut f: impl FnMut()) -> Duration {
		for _ in 0..iterations.min(3) {
			f();
		}
		let start = Instant::now();
		for _ in 0..iterations {
			f();
		}
		start.elapsed()
	}

	fn micros_per_iter(duration: Duration, iterations: usize) -> f64 {
		duration.as_secs_f64() * 1_000_000.0 / iterations.max(1) as f64
	}

	fn duration_ratio_micros(numerator: Duration, denominator: Duration) -> u64 {
		if denominator.is_zero() {
			return u64::MAX;
		}
		((numerator.as_nanos().saturating_mul(1_000)) / denominator.as_nanos()) as u64
	}

	fn projected_row_safe_len(
		input: &[u8],
		offsets: &[u64],
		row_count: usize,
		table: &FsstSymbolTable,
		sample_indices: &[usize],
	) -> usize {
		let parts =
			encode_fsst_row_safe_sample_parts(input, offsets, sample_indices, table).unwrap();
		let projected_payload =
			project_sample_to_full(parts.payload_len, sample_indices.len(), row_count).unwrap();
		let projected_exception_count =
			project_sample_to_full(parts.exception_count, sample_indices.len(), row_count).unwrap();
		fsst_row_safe_metadata_bytes(row_count, table.symbols.len(), projected_exception_count)
			.unwrap()
			.checked_add(projected_payload)
			.unwrap()
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

	fn variable_width_payload(values: &[Vec<u8>]) -> (Vec<u8>, Vec<u64>) {
		let mut input = Vec::new();
		let mut offsets = Vec::with_capacity(values.len() + 1);
		offsets.push(0_u64);
		for value in values {
			input.extend_from_slice(value);
			offsets.push(input.len() as u64);
		}
		(input, offsets)
	}
}
