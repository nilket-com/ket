use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::BufferEncoding;
use crate::storage::encoding::cascade::RleByteRunIter;
use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;
use crate::storage::encoding::compute::kernels::scalar::{
	MaskBuilder, data_type_is_signed, data_type_is_unsigned, decoded_fixed_width_bytes_per_value,
	evaluate_value, fixed_width_for_data_type, map_encoding_reject, native_value_from_le_bytes,
	read_unsigned_le_as_u128, signed_predicate, signed_value_from_le_bytes, unsigned_predicate,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, LiteralCoercion, LiteralValue, PredicateAtom,
};
use crate::storage::encoding::page::expand_page_descriptors;

pub const fn plain_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::RlePlain
}

pub const fn bitpacked_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::RleBitPacked
}

pub fn evaluate_predicate(
	encoded_bytes: &[u8],
	encoding: &BufferEncoding,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let value_width = decoded_fixed_width_bytes_per_value(encoding)?;
	if fixed_width_for_data_type(&coercion.data_type) != Some(value_width) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let literal = coercion
		.literal
		.as_ref()
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let rows = usize::try_from(encoding.decoded_value_count.get())
		.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	let mut selected = MaskBuilder::with_rows(rows);
	if encoding.page_end_offsets.is_empty() {
		evaluate_frame(
			encoded_bytes,
			value_width,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
			atom,
			coercion,
			literal,
			&mut selected,
		)?;
	} else {
		for page in expand_page_descriptors(encoding).map_err(map_encoding_reject)? {
			let start = usize::try_from(page.byte_range.start.get())
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let len = usize::try_from(page.byte_range.len.get())
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let end = start
				.checked_add(len)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			let Some(page_bytes) = encoded_bytes.get(start..end) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			let decoded_byte_len = usize::try_from(page.value_count.get())
				.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?
				.checked_mul(value_width)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			evaluate_frame(
				page_bytes,
				value_width,
				RecordBodyByteLen::new(decoded_byte_len as u64),
				page.value_count,
				atom,
				coercion,
				literal,
				&mut selected,
			)?;
		}
	}
	selected.finish()
}

#[allow(clippy::too_many_arguments)]
fn evaluate_frame(
	encoded_bytes: &[u8],
	value_width: usize,
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let selected_start = selected.len();
	let expected = usize::try_from(decoded_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	let mut decoded_bytes = 0_usize;
	let unsigned = if data_type_is_unsigned(&coercion.data_type) {
		Some(unsigned_predicate(atom.op, literal)?)
	} else {
		None
	};
	let signed = if data_type_is_signed(&coercion.data_type) {
		Some(signed_predicate(atom.op, literal)?)
	} else {
		None
	};
	if value_width == 1 {
		for run in RleByteRunIter::new(encoded_bytes) {
			let run = run.map_err(map_encoding_reject)?;
			decoded_bytes = decoded_bytes
				.checked_add(run.run_len)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if decoded_bytes > expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			let keep = evaluate_lane(
				&[run.byte],
				atom,
				coercion,
				literal,
				unsigned.as_ref(),
				signed.as_ref(),
			)?;
			selected.push_many(keep, run.run_len)?;
		}
		if decoded_bytes != expected
			|| selected.len().saturating_sub(selected_start) != value_count.get() as usize
		{
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		}
		return Ok(());
	}
	let mut lane = [0_u8; 16];
	let mut lane_len = 0_usize;
	for run in RleByteRunIter::new(encoded_bytes) {
		let run = run.map_err(map_encoding_reject)?;
		for _ in 0..run.run_len {
			decoded_bytes = decoded_bytes
				.checked_add(1)
				.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			if decoded_bytes > expected {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			if lane_len >= lane.len() {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			}
			lane[lane_len] = run.byte;
			lane_len += 1;
			if lane_len == value_width {
				let keep = evaluate_lane(
					&lane[..value_width],
					atom,
					coercion,
					literal,
					unsigned.as_ref(),
					signed.as_ref(),
				)?;
				selected.push(keep)?;
				lane_len = 0;
			}
		}
	}
	if decoded_bytes != expected
		|| lane_len != 0
		|| selected.len().saturating_sub(selected_start) != value_count.get() as usize
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn evaluate_lane(
	lane: &[u8],
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	unsigned: Option<&crate::storage::encoding::compute::kernels::scalar::UnsignedPredicate>,
	signed: Option<&crate::storage::encoding::compute::kernels::scalar::SignedPredicate>,
) -> Result<bool, EncodingComputeRejectCode> {
	Ok(if let Some(predicate) = unsigned {
		predicate.evaluate(read_unsigned_le_as_u128(lane))
	} else if let Some(predicate) = signed {
		predicate.evaluate(signed_value_from_le_bytes(lane, &coercion.data_type)?)
	} else {
		let value = native_value_from_le_bytes(lane, &coercion.data_type)?;
		evaluate_value(&value, atom.op, literal)?
	})
}
