use crate::schema::{BaseValue, BitsPerValue, BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::BufferEncoding;
use crate::storage::encoding::cascade::parse_bitpacked_frame;
use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;
use crate::storage::encoding::compute::kernels::scalar::{
	MaskBuilder, UnsignedPredicate, data_type_is_signed, data_type_is_unsigned,
	decoded_fixed_width_bytes_per_value, evaluate_value, fixed_width_for_data_type,
	map_encoding_reject, native_value_from_u128, read_unsigned_le_as_u128, signed_predicate,
	signed_value_from_u128, unsigned_predicate,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, LiteralCoercion, LiteralValue, PredicateAtom,
};
use crate::storage::encoding::page::expand_page_descriptors;

pub const fn evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::BitPacked
}

pub fn evaluate_predicate(
	encoded_bytes: &[u8],
	encoding: &BufferEncoding,
	bits: BitsPerValue,
	base: BaseValue,
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
			bits,
			base,
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
				bits,
				base,
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
	bits: BitsPerValue,
	base: BaseValue,
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let frame = parse_bitpacked_frame(encoded_bytes, bits, base, decoded_byte_len, value_count)
		.map_err(map_encoding_reject)?;
	if data_type_is_unsigned(&coercion.data_type)
		&& matches!(frame.bits, 8 | 16 | 32 | 64)
		&& frame.value_width == usize::from(frame.bits / 8)
	{
		let predicate = unsigned_predicate(atom.op, literal)?;
		if frame.bits == 8 && frame.value_width == 1 {
			evaluate_u8_frame(&frame, &predicate, selected)?;
			return Ok(());
		}
		for bytes in frame.encoded.chunks_exact(frame.value_width) {
			let raw = frame.base.saturating_add(read_unsigned_le_as_u128(bytes));
			selected.push(predicate.evaluate(raw))?;
		}
		return Ok(());
	}
	let mask = if frame.bits == 64 {
		u128::from(u64::MAX)
	} else {
		(1_u128 << frame.bits) - 1
	};
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
	let mut acc = 0_u128;
	let mut acc_bits = 0_u8;
	let mut cursor = 0_usize;
	for _ in 0..frame.value_count {
		while acc_bits < frame.bits {
			let Some(byte) = frame.encoded.get(cursor) else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			acc |= u128::from(*byte) << acc_bits;
			acc_bits += 8;
			cursor += 1;
		}
		let raw = frame.base.saturating_add(acc & mask);
		let keep = if let Some(predicate) = &unsigned {
			predicate.evaluate(raw)
		} else if let Some(predicate) = &signed {
			let value = signed_value_from_u128(raw, frame.value_width, &coercion.data_type)?;
			predicate.evaluate(value)
		} else {
			let value = native_value_from_u128(raw, frame.value_width, &coercion.data_type)?;
			evaluate_value(&value, atom.op, literal)?
		};
		selected.push(keep)?;
		acc >>= frame.bits;
		acc_bits -= frame.bits;
	}
	Ok(())
}

fn evaluate_u8_frame(
	frame: &crate::storage::encoding::cascade::BitPackedFrame<'_>,
	predicate: &UnsignedPredicate,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let mut cursor = 0_usize;
	while cursor < frame.encoded.len() && selected.len() % 8 != 0 {
		let raw = frame.base.saturating_add(u128::from(frame.encoded[cursor]));
		selected.push(predicate.evaluate(raw))?;
		cursor += 1;
	}
	while cursor + 8 <= frame.encoded.len() {
		let mut mask = 0_u8;
		for (lane, byte) in frame.encoded[cursor..cursor + 8].iter().enumerate() {
			let raw = frame.base.saturating_add(u128::from(*byte));
			if predicate.evaluate(raw) {
				mask |= 1_u8 << lane;
			}
		}
		selected.push_aligned_bitmap_byte(mask, 8)?;
		cursor += 8;
	}
	while cursor < frame.encoded.len() {
		let raw = frame.base.saturating_add(u128::from(frame.encoded[cursor]));
		selected.push(predicate.evaluate(raw))?;
		cursor += 1;
	}
	Ok(())
}
