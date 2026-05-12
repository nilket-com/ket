use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::cascade::{decode_buffer, encode_fsst_payload_with_symbol_map};
use crate::storage::encoding::compute::kernels::scalar::MaskBuilder;
use crate::storage::encoding::compute::{EncodedRowMask, EncodingComputeRejectCode};
use crate::storage::encoding::{BufferEncoding, EncodingCascade, EncodingRejectCode};
use arrow::datatypes::DataType;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub struct FsstFrame<'a> {
	payload: &'a [u8],
	symbols: &'a [u8],
	expected_decoded_byte_len: usize,
}

impl FsstFrame<'_> {
	pub fn expected_decoded_byte_len(&self) -> usize {
		self.expected_decoded_byte_len
	}
}

pub const FSST_ROW_SAFE_BLOCK_ROWS: usize = 256;
pub const FSST_ROW_SAFE_LENGTH_SENTINEL: u8 = 255;

#[derive(Clone, Debug)]
pub struct FsstRowSafeFrame<'a> {
	payload: &'a [u8],
	encoded_lengths: &'a [u8],
	exception_lengths: Vec<u32>,
	block_offsets: Vec<(u32, u32)>,
	pair_to_symbol: Vec<u16>,
	row_count: usize,
	expected_decoded_byte_len: usize,
}

impl FsstRowSafeFrame<'_> {
	pub fn expected_decoded_byte_len(&self) -> usize {
		self.expected_decoded_byte_len
	}

	pub fn row_count(&self) -> usize {
		self.row_count
	}

	fn block_seed(&self, block_index: usize) -> Result<(usize, usize), EncodingComputeRejectCode> {
		let (payload_offset, exception_index) = *self
			.block_offsets
			.get(block_index)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		Ok((payload_offset as usize, exception_index as usize))
	}

	#[inline(always)]
	fn resolve_row_encoded_len(
		&self,
		row_index: usize,
		exc_idx: &mut usize,
	) -> Result<usize, EncodingComputeRejectCode> {
		let raw = *self
			.encoded_lengths
			.get(row_index)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		if raw != FSST_ROW_SAFE_LENGTH_SENTINEL {
			Ok(raw as usize)
		} else {
			let length = *self
				.exception_lengths
				.get(*exc_idx)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?
				as usize;
			*exc_idx += 1;
			Ok(length)
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedOffsets {
	offsets: Vec<usize>,
}

impl DecodedOffsets {
	pub fn row_count(&self) -> usize {
		self.offsets.len().saturating_sub(1)
	}

	fn row_len(&self, row: usize) -> Result<usize, EncodingComputeRejectCode> {
		let start = *self
			.offsets
			.get(row)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		let end = *self
			.offsets
			.get(row + 1)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		end.checked_sub(start)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiteralBytes<'a> {
	bytes: &'a [u8],
}

impl<'a> LiteralBytes<'a> {
	pub fn new(bytes: &'a [u8]) -> Self {
		Self { bytes }
	}

	fn as_slice(&self) -> &[u8] {
		self.bytes
	}

	fn len(&self) -> usize {
		self.bytes.len()
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FsstEvaluationStats {
	pub rows: usize,
	pub length_match_rows: usize,
	pub length_mismatch_rows: usize,
	pub decoded_bytes_walked: usize,
	pub decoded_bytes_skipped: usize,
	pub byte_comparisons: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FsstRowSafeEvaluationStats {
	pub rows: usize,
	pub decoded_length_match_rows: usize,
	pub decoded_length_mismatch_rows: usize,
	pub encoded_length_match_rows: usize,
	pub encoded_length_mismatch_rows: usize,
	pub encoded_bytes_compared: usize,
}

#[derive(Clone, Debug)]
struct LiteralGroup<'a> {
	candidates: Vec<&'a [u8]>,
	common_prefix_len: usize,
}

impl<'a> LiteralGroup<'a> {
	fn new(candidates: Vec<&'a [u8]>) -> Self {
		let common_prefix_len = common_prefix_len(&candidates);
		Self {
			candidates,
			common_prefix_len,
		}
	}
}

fn common_prefix_len(candidates: &[&[u8]]) -> usize {
	let Some(first) = candidates.first() else {
		return 0;
	};
	let max = candidates
		.iter()
		.map(|candidate| candidate.len())
		.min()
		.unwrap_or(0);
	for index in 0..max {
		if candidates
			.iter()
			.skip(1)
			.any(|candidate| candidate[index] != first[index])
		{
			return index;
		}
	}
	max
}

pub fn parse_fsst_frame(
	buffer_bytes: &[u8],
	expected_byte_len: RecordBodyByteLen,
) -> Result<FsstFrame<'_>, EncodingComputeRejectCode> {
	let expected_decoded_byte_len = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if buffer_bytes.is_empty() {
		if expected_decoded_byte_len == 0 {
			return Ok(FsstFrame {
				payload: &[],
				symbols: &[],
				expected_decoded_byte_len,
			});
		}
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if buffer_bytes.len() < 5 || &buffer_bytes[..4] != b"WBFS" {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let symbol_count = buffer_bytes[4] as usize;
	if symbol_count > 127 {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let symbol_bytes = symbol_count
		.checked_mul(2)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let payload_start = 5_usize
		.checked_add(symbol_bytes)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if payload_start > buffer_bytes.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(FsstFrame {
		payload: &buffer_bytes[payload_start..],
		symbols: &buffer_bytes[5..payload_start],
		expected_decoded_byte_len,
	})
}

pub fn parse_fsst_row_safe_frame(
	buffer_bytes: &[u8],
	expected_byte_len: RecordBodyByteLen,
	expected_value_count: BufferValueCount,
) -> Result<FsstRowSafeFrame<'_>, EncodingComputeRejectCode> {
	let expected_decoded_byte_len = usize::try_from(expected_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if buffer_bytes.len() < 13 || &buffer_bytes[..4] != b"RSFS" {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let symbol_count = buffer_bytes[4] as usize;
	if symbol_count > 127 {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let symbol_bytes = symbol_count
		.checked_mul(2)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let mut cursor = 5_usize
		.checked_add(symbol_bytes)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if cursor > buffer_bytes.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let symbols = &buffer_bytes[5..cursor];
	let mut pair_to_symbol = vec![u16::MAX; 65_536];
	for symbol_index in 0..symbol_count {
		let pair_index = symbol_index
			.checked_mul(2)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let key = ((symbols[pair_index] as usize) << 8) | symbols[pair_index + 1] as usize;
		if pair_to_symbol[key] != u16::MAX {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		pair_to_symbol[key] = symbol_index as u16;
	}
	let row_count = read_row_safe_u32(buffer_bytes, &mut cursor)? as usize;
	if row_count != expected_value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let block_count = row_count.div_ceil(FSST_ROW_SAFE_BLOCK_ROWS);
	let mut block_offsets = Vec::<(u32, u32)>::with_capacity(block_count);
	for index in 0..block_count {
		let payload_offset = read_row_safe_u32(buffer_bytes, &mut cursor)?;
		let exception_index = read_row_safe_u32(buffer_bytes, &mut cursor)?;
		if index == 0 && (payload_offset != 0 || exception_index != 0) {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		if let Some((prev_payload, prev_exc)) = block_offsets.last()
			&& (payload_offset < *prev_payload || exception_index < *prev_exc)
		{
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		block_offsets.push((payload_offset, exception_index));
	}
	let lengths_end = cursor
		.checked_add(row_count)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if lengths_end > buffer_bytes.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let encoded_lengths = &buffer_bytes[cursor..lengths_end];
	cursor = lengths_end;
	let exception_count = read_row_safe_u32(buffer_bytes, &mut cursor)? as usize;
	let sentinel_count = encoded_lengths
		.iter()
		.filter(|&&len| len == FSST_ROW_SAFE_LENGTH_SENTINEL)
		.count();
	if sentinel_count != exception_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let exceptions_end = cursor
		.checked_add(exception_count.saturating_mul(4))
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if exceptions_end > buffer_bytes.len() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut exception_lengths = Vec::<u32>::with_capacity(exception_count);
	for _ in 0..exception_count {
		exception_lengths.push(read_row_safe_u32(buffer_bytes, &mut cursor)?);
	}
	let payload = &buffer_bytes[cursor..];
	// Verify sum-of-lengths invariant once, so the Strategy B loop can trust it.
	let mut running_sum: u64 = 0;
	let mut exc_idx = 0_usize;
	for (row_index, &raw) in encoded_lengths.iter().enumerate() {
		let row_encoded_len = if raw != FSST_ROW_SAFE_LENGTH_SENTINEL {
			raw as u64
		} else {
			let length = *exception_lengths
				.get(exc_idx)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)? as u64;
			exc_idx += 1;
			length
		};
		if row_index % FSST_ROW_SAFE_BLOCK_ROWS == 0 {
			let block_index = row_index / FSST_ROW_SAFE_BLOCK_ROWS;
			let (expected_payload_offset, expected_exc_idx) = block_offsets[block_index];
			let leading_exc_idx = exc_idx - usize::from(raw == FSST_ROW_SAFE_LENGTH_SENTINEL);
			if expected_payload_offset as u64 != running_sum
				|| expected_exc_idx as usize != leading_exc_idx
			{
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
		}
		running_sum = running_sum
			.checked_add(row_encoded_len)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	}
	if running_sum != payload.len() as u64 || exc_idx != exception_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(FsstRowSafeFrame {
		payload,
		encoded_lengths,
		exception_lengths,
		block_offsets,
		pair_to_symbol,
		row_count,
		expected_decoded_byte_len,
	})
}

pub fn decode_offsets(
	offset_bytes: &[u8],
	data_type: &DataType,
	row_count: BufferValueCount,
	expected_decoded_byte_len: RecordBodyByteLen,
	encoding: Option<&BufferEncoding>,
	cascade: Option<&EncodingCascade>,
) -> Result<DecodedOffsets, EncodingComputeRejectCode> {
	let decoded = match encoding {
		Some(encoding) => {
			let cascade = cascade.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			decode_buffer(
				offset_bytes,
				data_type,
				cascade,
				encoding.decoded_byte_len,
				encoding.decoded_value_count,
			)
			.map_err(map_encoding_reject)?
		}
		None => offset_bytes.to_vec(),
	};
	let width = offset_width(data_type)?;
	let rows = usize::try_from(row_count.get())
		.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	let offset_count = rows
		.checked_add(1)
		.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
	let expected_bytes = offset_count
		.checked_mul(width)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if decoded.len() != expected_bytes {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let expected_final = usize::try_from(expected_decoded_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let mut offsets = Vec::with_capacity(offset_count);
	let mut previous = 0_usize;
	for (index, chunk) in decoded.chunks_exact(width).enumerate() {
		let offset = if width == 4 {
			let value = i32::from_le_bytes(chunk.try_into().expect("offset width"));
			usize::try_from(value)
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?
		} else {
			let value = i64::from_le_bytes(chunk.try_into().expect("offset width"));
			usize::try_from(value)
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?
		};
		if index > 0 && offset < previous {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		previous = offset;
		offsets.push(offset);
	}
	if offsets.last().copied() != Some(expected_final) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(DecodedOffsets { offsets })
}

pub fn evaluate_fsst_equality(
	frame: &FsstFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	evaluate_fsst_equality_impl::<false>(frame, offsets, literals, op).map(|(mask, _)| mask)
}

pub fn evaluate_fsst_equality_with_stats(
	frame: &FsstFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<(EncodedRowMask, FsstEvaluationStats), EncodingComputeRejectCode> {
	evaluate_fsst_equality_impl::<true>(frame, offsets, literals, op)
}

pub fn evaluate_fsst_row_safe_equality(
	frame: &FsstRowSafeFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	evaluate_fsst_row_safe_equality_impl::<false>(frame, offsets, literals, op)
		.map(|(mask, _)| mask)
}

pub fn evaluate_fsst_row_safe_equality_with_stats(
	frame: &FsstRowSafeFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<(EncodedRowMask, FsstRowSafeEvaluationStats), EncodingComputeRejectCode> {
	evaluate_fsst_row_safe_equality_impl::<true>(frame, offsets, literals, op)
}

fn evaluate_fsst_equality_impl<const TRACK_STATS: bool>(
	frame: &FsstFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<(EncodedRowMask, FsstEvaluationStats), EncodingComputeRejectCode> {
	if !matches!(
		op,
		crate::storage::encoding::compute::PredicateAtomOp::Eq
			| crate::storage::encoding::compute::PredicateAtomOp::Neq
			| crate::storage::encoding::compute::PredicateAtomOp::InList
	) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if literals.is_empty() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let invert = matches!(op, crate::storage::encoding::compute::PredicateAtomOp::Neq);
	let rows = offsets.row_count();
	let mut reader = FsstDecodedByteReader::new(frame);
	let mut selected = MaskBuilder::with_rows(rows);
	let mut stats = FsstEvaluationStats {
		rows,
		..FsstEvaluationStats::default()
	};
	if let [literal] = literals {
		let literal = literal.as_slice();
		if invert {
			for row in 0..rows {
				let row_len = offsets.row_len(row)?;
				let row_matches_literal = if row_len == literal.len() {
					record_length_match::<TRACK_STATS>(&mut stats, row_len);
					reader.matches_one::<TRACK_STATS>(literal, &mut stats)?
				} else {
					record_length_mismatch::<TRACK_STATS>(&mut stats, row_len);
					reader.skip(row_len)?;
					false
				};
				selected.push(!row_matches_literal)?;
			}
		} else {
			for row in 0..rows {
				let row_len = offsets.row_len(row)?;
				let row_matches_literal = if row_len == literal.len() {
					record_length_match::<TRACK_STATS>(&mut stats, row_len);
					reader.matches_one::<TRACK_STATS>(literal, &mut stats)?
				} else {
					record_length_mismatch::<TRACK_STATS>(&mut stats, row_len);
					reader.skip(row_len)?;
					false
				};
				selected.push(row_matches_literal)?;
			}
		}
	} else {
		let mut by_len: BTreeMap<usize, Vec<&[u8]>> = BTreeMap::new();
		for literal in literals {
			by_len
				.entry(literal.len())
				.or_default()
				.push(literal.as_slice());
		}
		let by_len = by_len
			.into_iter()
			.map(|(len, candidates)| (len, LiteralGroup::new(candidates)))
			.collect::<BTreeMap<_, _>>();
		if invert {
			for row in 0..rows {
				let row_len = offsets.row_len(row)?;
				let row_matches_any_literal = if let Some(group) = by_len.get(&row_len) {
					record_length_match::<TRACK_STATS>(&mut stats, row_len);
					reader.matches_any::<TRACK_STATS>(group, &mut stats)?
				} else {
					record_length_mismatch::<TRACK_STATS>(&mut stats, row_len);
					reader.skip(row_len)?;
					false
				};
				selected.push(!row_matches_any_literal)?;
			}
		} else {
			for row in 0..rows {
				let row_len = offsets.row_len(row)?;
				let row_matches_any_literal = if let Some(group) = by_len.get(&row_len) {
					record_length_match::<TRACK_STATS>(&mut stats, row_len);
					reader.matches_any::<TRACK_STATS>(group, &mut stats)?
				} else {
					record_length_mismatch::<TRACK_STATS>(&mut stats, row_len);
					reader.skip(row_len)?;
					false
				};
				selected.push(row_matches_any_literal)?;
			}
		}
	}
	reader.finish()?;
	selected.finish().map(|mask| (mask, stats))
}

fn evaluate_fsst_row_safe_equality_impl<const TRACK_STATS: bool>(
	frame: &FsstRowSafeFrame<'_>,
	offsets: &DecodedOffsets,
	literals: &[LiteralBytes<'_>],
	op: crate::storage::encoding::compute::PredicateAtomOp,
) -> Result<(EncodedRowMask, FsstRowSafeEvaluationStats), EncodingComputeRejectCode> {
	if !matches!(
		op,
		crate::storage::encoding::compute::PredicateAtomOp::Eq
			| crate::storage::encoding::compute::PredicateAtomOp::Neq
			| crate::storage::encoding::compute::PredicateAtomOp::InList
	) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if literals.is_empty() || frame.row_count() != offsets.row_count() {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let row_safe_literals = encode_row_safe_literals(frame, literals)?;
	let mut by_decoded_len = BTreeMap::<usize, Vec<usize>>::new();
	for (index, literal) in row_safe_literals.iter().enumerate() {
		by_decoded_len
			.entry(literal.decoded_len)
			.or_default()
			.push(index);
	}
	let invert = matches!(op, crate::storage::encoding::compute::PredicateAtomOp::Neq);
	let rows = offsets.row_count();
	let mut selected = MaskBuilder::with_rows(rows);
	let mut stats = FsstRowSafeEvaluationStats {
		rows,
		..FsstRowSafeEvaluationStats::default()
	};
	let (mut current_offset, mut exc_idx) = frame.block_seed(0)?;
	for row in 0..rows {
		let decoded_len = offsets.row_len(row)?;
		let row_encoded_len = frame.resolve_row_encoded_len(row, &mut exc_idx)?;
		let row_end = current_offset
			.checked_add(row_encoded_len)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		let row_encoded = frame
			.payload
			.get(current_offset..row_end)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let mut row_matches = false;
		if let Some(candidates) = by_decoded_len.get(&decoded_len) {
			record_row_safe_decoded_length_match::<TRACK_STATS>(&mut stats);
			for candidate_index in candidates {
				let literal = &row_safe_literals[*candidate_index];
				if literal.encoded.len() != row_encoded.len() {
					record_row_safe_encoded_length_mismatch::<TRACK_STATS>(&mut stats);
					continue;
				}
				record_row_safe_encoded_length_match::<TRACK_STATS>(&mut stats, row_encoded.len());
				if literal.encoded.as_slice() == row_encoded {
					row_matches = true;
					break;
				}
			}
		} else {
			record_row_safe_decoded_length_mismatch::<TRACK_STATS>(&mut stats);
		}
		current_offset = row_end;
		selected.push(if invert { !row_matches } else { row_matches })?;
	}
	selected.finish().map(|mask| (mask, stats))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EncodedRowSafeLiteral {
	decoded_len: usize,
	encoded: Vec<u8>,
}

fn encode_row_safe_literals(
	frame: &FsstRowSafeFrame<'_>,
	literals: &[LiteralBytes<'_>],
) -> Result<Vec<EncodedRowSafeLiteral>, EncodingComputeRejectCode> {
	let mut row_safe_literals = Vec::with_capacity(literals.len());
	for literal in literals {
		let mut encoded = Vec::with_capacity(literal.len());
		encode_fsst_payload_with_symbol_map(
			literal.as_slice(),
			&frame.pair_to_symbol,
			&mut encoded,
		)
		.map_err(map_encoding_reject)?;
		row_safe_literals.push(EncodedRowSafeLiteral {
			decoded_len: literal.len(),
			encoded,
		});
	}
	Ok(row_safe_literals)
}

#[inline(always)]
fn record_length_match<const TRACK_STATS: bool>(stats: &mut FsstEvaluationStats, row_len: usize) {
	if TRACK_STATS {
		stats.length_match_rows = stats.length_match_rows.saturating_add(1);
		stats.decoded_bytes_walked = stats.decoded_bytes_walked.saturating_add(row_len);
	}
}

#[inline(always)]
fn record_row_safe_decoded_length_match<const TRACK_STATS: bool>(
	stats: &mut FsstRowSafeEvaluationStats,
) {
	if TRACK_STATS {
		stats.decoded_length_match_rows = stats.decoded_length_match_rows.saturating_add(1);
	}
}

#[inline(always)]
fn record_row_safe_decoded_length_mismatch<const TRACK_STATS: bool>(
	stats: &mut FsstRowSafeEvaluationStats,
) {
	if TRACK_STATS {
		stats.decoded_length_mismatch_rows = stats.decoded_length_mismatch_rows.saturating_add(1);
	}
}

#[inline(always)]
fn record_row_safe_encoded_length_match<const TRACK_STATS: bool>(
	stats: &mut FsstRowSafeEvaluationStats,
	len: usize,
) {
	if TRACK_STATS {
		stats.encoded_length_match_rows = stats.encoded_length_match_rows.saturating_add(1);
		stats.encoded_bytes_compared = stats.encoded_bytes_compared.saturating_add(len);
	}
}

#[inline(always)]
fn record_row_safe_encoded_length_mismatch<const TRACK_STATS: bool>(
	stats: &mut FsstRowSafeEvaluationStats,
) {
	if TRACK_STATS {
		stats.encoded_length_mismatch_rows = stats.encoded_length_mismatch_rows.saturating_add(1);
	}
}

#[inline(always)]
fn record_length_mismatch<const TRACK_STATS: bool>(
	stats: &mut FsstEvaluationStats,
	row_len: usize,
) {
	if TRACK_STATS {
		stats.length_mismatch_rows = stats.length_mismatch_rows.saturating_add(1);
		stats.decoded_bytes_walked = stats.decoded_bytes_walked.saturating_add(row_len);
		stats.decoded_bytes_skipped = stats.decoded_bytes_skipped.saturating_add(row_len);
	}
}

pub fn string_like_data_type_supported(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
	)
}

fn offset_width(data_type: &DataType) -> Result<usize, EncodingComputeRejectCode> {
	match data_type {
		DataType::Utf8 | DataType::Binary => Ok(4),
		DataType::LargeUtf8 | DataType::LargeBinary => Ok(8),
		_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	}
}

fn read_row_safe_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, EncodingComputeRejectCode> {
	let end = cursor
		.checked_add(4)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let Some(raw) = bytes.get(*cursor..end) else {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	};
	*cursor = end;
	Ok(u32::from_le_bytes(raw.try_into().expect("u32 width")))
}

struct FsstDecodedByteReader<'a, 'f> {
	frame: &'f FsstFrame<'a>,
	cursor: usize,
	pending: Option<u8>,
	emitted: usize,
}

impl<'a, 'f> FsstDecodedByteReader<'a, 'f> {
	fn new(frame: &'f FsstFrame<'a>) -> Self {
		Self {
			frame,
			cursor: 0,
			pending: None,
			emitted: 0,
		}
	}

	#[inline(always)]
	fn next_required(&mut self) -> Result<u8, EncodingComputeRejectCode> {
		self.next_byte()?
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)
	}

	fn matches_any<const TRACK_STATS: bool>(
		&mut self,
		group: &LiteralGroup<'_>,
		stats: &mut FsstEvaluationStats,
	) -> Result<bool, EncodingComputeRejectCode> {
		let candidates = group.candidates.as_slice();
		if candidates.len() == 1 {
			return self.matches_one::<TRACK_STATS>(candidates[0], stats);
		}
		let row_len = candidates
			.first()
			.map(|candidate| candidate.len())
			.unwrap_or(0);
		let common_prefix_len = group.common_prefix_len.min(row_len);
		for index in 0..common_prefix_len {
			let byte = self.next_required()?;
			if TRACK_STATS {
				stats.byte_comparisons = stats.byte_comparisons.saturating_add(1);
			}
			if candidates[0][index] != byte {
				self.skip_after_mismatch::<TRACK_STATS>(row_len.saturating_sub(index + 1), stats)?;
				return Ok(false);
			}
		}
		if common_prefix_len == row_len {
			return Ok(true);
		}
		if candidates.len() <= 128 {
			return self.matches_any_bitmask::<TRACK_STATS>(
				candidates,
				common_prefix_len,
				row_len,
				stats,
			);
		}
		self.matches_any_vec::<TRACK_STATS>(candidates, common_prefix_len, row_len, stats)
	}

	fn matches_any_bitmask<const TRACK_STATS: bool>(
		&mut self,
		candidates: &[&[u8]],
		start: usize,
		row_len: usize,
		stats: &mut FsstEvaluationStats,
	) -> Result<bool, EncodingComputeRejectCode> {
		let mut alive = if candidates.len() == 128 {
			u128::MAX
		} else {
			(1_u128 << candidates.len()) - 1
		};
		for index in start..row_len {
			let byte = self.next_required()?;
			let mut next_alive = 0_u128;
			for (candidate_index, candidate) in candidates.iter().enumerate() {
				let bit = 1_u128 << candidate_index;
				if alive & bit != 0 {
					if TRACK_STATS {
						stats.byte_comparisons = stats.byte_comparisons.saturating_add(1);
					}
					if candidate[index] == byte {
						next_alive |= bit;
					}
				}
			}
			alive = next_alive;
			if alive == 0 {
				self.skip_after_mismatch::<TRACK_STATS>(row_len.saturating_sub(index + 1), stats)?;
				return Ok(false);
			}
		}
		Ok(alive != 0)
	}

	fn matches_any_vec<const TRACK_STATS: bool>(
		&mut self,
		candidates: &[&[u8]],
		start: usize,
		row_len: usize,
		stats: &mut FsstEvaluationStats,
	) -> Result<bool, EncodingComputeRejectCode> {
		let mut alive = vec![true; candidates.len()];
		for index in start..row_len {
			let byte = self.next_required()?;
			let mut any_alive = false;
			for (candidate_alive, candidate) in alive.iter_mut().zip(candidates) {
				if *candidate_alive {
					if TRACK_STATS {
						stats.byte_comparisons = stats.byte_comparisons.saturating_add(1);
					}
					if candidate[index] == byte {
						any_alive = true;
					} else {
						*candidate_alive = false;
					}
				}
			}
			if !any_alive {
				self.skip_after_mismatch::<TRACK_STATS>(row_len.saturating_sub(index + 1), stats)?;
				return Ok(false);
			}
		}
		Ok(alive.into_iter().any(|candidate_alive| candidate_alive))
	}

	fn matches_one<const TRACK_STATS: bool>(
		&mut self,
		candidate: &[u8],
		stats: &mut FsstEvaluationStats,
	) -> Result<bool, EncodingComputeRejectCode> {
		if candidate.is_empty() {
			return Ok(true);
		}
		let mut index = 0_usize;
		if let Some(actual) = self.pending.take() {
			self.emitted += 1;
			if !compare_candidate_byte::<TRACK_STATS>(
				actual,
				candidate[index],
				&mut index,
				candidate.len(),
				stats,
			) {
				self.skip_after_mismatch::<TRACK_STATS>(candidate.len() - index, stats)?;
				return Ok(false);
			}
		}
		while index < candidate.len() {
			let Some(byte) = self.frame.payload.get(self.cursor).copied() else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			self.cursor += 1;
			if byte < 0x80 {
				self.emitted += 1;
				if !compare_candidate_byte::<TRACK_STATS>(
					byte,
					candidate[index],
					&mut index,
					candidate.len(),
					stats,
				) {
					self.skip_after_mismatch::<TRACK_STATS>(candidate.len() - index, stats)?;
					return Ok(false);
				}
				continue;
			}
			if byte == 0xff {
				let Some(actual) = self.frame.payload.get(self.cursor).copied() else {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				};
				self.cursor += 1;
				self.emitted += 1;
				if !compare_candidate_byte::<TRACK_STATS>(
					actual,
					candidate[index],
					&mut index,
					candidate.len(),
					stats,
				) {
					self.skip_after_mismatch::<TRACK_STATS>(candidate.len() - index, stats)?;
					return Ok(false);
				}
				continue;
			}
			let symbol_index = usize::from(byte & 0x7f)
				.checked_mul(2)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let Some(pair) = self.frame.symbols.get(symbol_index..symbol_index + 2) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			self.emitted += 1;
			if !compare_candidate_byte::<TRACK_STATS>(
				pair[0],
				candidate[index],
				&mut index,
				candidate.len(),
				stats,
			) {
				self.pending = Some(pair[1]);
				self.skip_after_mismatch::<TRACK_STATS>(candidate.len() - index, stats)?;
				return Ok(false);
			}
			if index == candidate.len() {
				self.pending = Some(pair[1]);
				return Ok(true);
			}
			self.emitted += 1;
			if !compare_candidate_byte::<TRACK_STATS>(
				pair[1],
				candidate[index],
				&mut index,
				candidate.len(),
				stats,
			) {
				self.skip_after_mismatch::<TRACK_STATS>(candidate.len() - index, stats)?;
				return Ok(false);
			}
		}
		Ok(true)
	}

	#[inline(always)]
	fn skip_after_mismatch<const TRACK_STATS: bool>(
		&mut self,
		remaining: usize,
		stats: &mut FsstEvaluationStats,
	) -> Result<(), EncodingComputeRejectCode> {
		if TRACK_STATS {
			stats.decoded_bytes_skipped = stats.decoded_bytes_skipped.saturating_add(remaining);
		}
		self.skip(remaining)
	}

	#[inline(always)]
	fn next_byte(&mut self) -> Result<Option<u8>, EncodingComputeRejectCode> {
		if let Some(byte) = self.pending.take() {
			self.emitted += 1;
			return Ok(Some(byte));
		}
		let Some(byte) = self.frame.payload.get(self.cursor).copied() else {
			return Ok(None);
		};
		self.cursor += 1;
		let decoded = if byte < 0x80 {
			byte
		} else if byte == 0xff {
			let Some(code) = self.frame.payload.get(self.cursor).copied() else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			self.cursor += 1;
			code
		} else {
			let index = usize::from(byte & 0x7f)
				.checked_mul(2)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let Some(pair) = self.frame.symbols.get(index..index + 2) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			self.pending = Some(pair[1]);
			pair[0]
		};
		self.emitted += 1;
		Ok(Some(decoded))
	}

	#[inline(always)]
	fn skip(&mut self, count: usize) -> Result<(), EncodingComputeRejectCode> {
		let mut remaining = count;
		if remaining == 0 {
			return Ok(());
		}
		if self.pending.take().is_some() {
			self.emitted += 1;
			remaining -= 1;
		}
		while remaining > 0 {
			let Some(byte) = self.frame.payload.get(self.cursor).copied() else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			self.cursor += 1;
			let emitted = if byte < 0x80 {
				1_usize
			} else if byte == 0xff {
				if self.frame.payload.get(self.cursor).is_none() {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				}
				self.cursor += 1;
				1
			} else {
				let index = usize::from(byte & 0x7f)
					.checked_mul(2)
					.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
				let Some(pair) = self.frame.symbols.get(index..index + 2) else {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				};
				if remaining == 1 {
					self.pending = Some(pair[1]);
					1
				} else {
					2
				}
			};
			self.emitted += emitted;
			remaining -= emitted;
		}
		Ok(())
	}

	fn finish(mut self) -> Result<(), EncodingComputeRejectCode> {
		if self.emitted != self.frame.expected_decoded_byte_len {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		if self.next_byte()?.is_some() {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		Ok(())
	}
}

#[inline(always)]
fn compare_candidate_byte<const TRACK_STATS: bool>(
	actual: u8,
	expected: u8,
	index: &mut usize,
	candidate_len: usize,
	stats: &mut FsstEvaluationStats,
) -> bool {
	if TRACK_STATS {
		stats.byte_comparisons = stats.byte_comparisons.saturating_add(1);
	}
	*index += 1;
	actual == expected && *index <= candidate_len
}

fn map_encoding_reject(code: EncodingRejectCode) -> EncodingComputeRejectCode {
	match code {
		EncodingRejectCode::DecodedLengthMismatch
		| EncodingRejectCode::EncodingDirectoryParseError
		| EncodingRejectCode::BufferEncodingMismatch
		| EncodingRejectCode::PageRangeOverflow
		| EncodingRejectCode::CascadeUnsupported => {
			EncodingComputeRejectCode::KernelDecodedLengthMismatch
		}
	}
}
