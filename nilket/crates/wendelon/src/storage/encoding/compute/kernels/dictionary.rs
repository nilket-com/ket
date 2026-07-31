use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::BufferEncoding;
use crate::storage::encoding::EncodingLeafCascade;
use crate::storage::encoding::cascade::{
	DictionaryIndexEncoding, parse_fixed_width_dictionary_frame,
};
use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;
use crate::storage::encoding::compute::kernels::scalar::{
	MaskBuilder, data_type_is_signed, data_type_is_unsigned, decoded_fixed_width_bytes_per_value,
	fixed_width_for_data_type, map_encoding_reject, read_unsigned_le_as_u128, signed_predicate,
	signed_value_from_le_bytes, unsigned_predicate,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, LiteralCoercion, LiteralValue, PredicateAtom,
};
use crate::storage::encoding::page::expand_page_descriptors;

pub const fn plain_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::DictionaryPlain
}

pub const fn bitpacked_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::DictionaryBitPacked
}

pub fn evaluate_predicate(
	encoded_bytes: &[u8],
	encoding: &BufferEncoding,
	inner: &EncodingLeafCascade,
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
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
			inner,
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
				RecordBodyByteLen::new(decoded_byte_len as u64),
				page.value_count,
				inner,
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
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
	inner: &EncodingLeafCascade,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let frame =
		parse_fixed_width_dictionary_frame(encoded_bytes, decoded_byte_len, value_count, inner)
			.map_err(map_encoding_reject)?;
	let matches = dictionary_matches(&frame, atom, coercion, literal)?;
	match frame.index_encoding {
		DictionaryIndexEncoding::Plain { width } => {
			for bytes in frame.index_payload.chunks_exact(width) {
				let index = read_index(bytes, width)?;
				push_index_match(&matches, index, selected)?;
			}
		}
		DictionaryIndexEncoding::BitPacked { bits } => {
			let mask = if bits == 64 {
				u128::from(u64::MAX)
			} else {
				(1_u128 << bits) - 1
			};
			let mut acc = 0_u128;
			let mut acc_bits = 0_u8;
			let mut cursor = 0_usize;
			for _ in 0..frame.value_count {
				while acc_bits < bits {
					let Some(byte) = frame.index_payload.get(cursor) else {
						return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
					};
					acc |= u128::from(*byte) << acc_bits;
					acc_bits += 8;
					cursor += 1;
				}
				push_index_match(&matches, (acc & mask) as usize, selected)?;
				acc >>= bits;
				acc_bits -= bits;
			}
		}
	}
	Ok(())
}

fn dictionary_matches(
	frame: &crate::storage::encoding::cascade::FixedWidthDictionaryFrame<'_>,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
) -> Result<Vec<bool>, EncodingComputeRejectCode> {
	let mut matches = Vec::with_capacity(frame.dict_count);
	if data_type_is_unsigned(&coercion.data_type) {
		let predicate = unsigned_predicate(atom.op, literal)?;
		for bytes in frame.dictionary.chunks_exact(frame.value_width) {
			matches.push(predicate.evaluate(read_unsigned_le_as_u128(bytes)));
		}
	} else if data_type_is_signed(&coercion.data_type) {
		let predicate = signed_predicate(atom.op, literal)?;
		for bytes in frame.dictionary.chunks_exact(frame.value_width) {
			matches
				.push(predicate.evaluate(signed_value_from_le_bytes(bytes, &coercion.data_type)?));
		}
	} else {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if matches.len() != frame.dict_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(matches)
}

fn push_index_match(
	matches: &[bool],
	index: usize,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let Some(keep) = matches.get(index) else {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	};
	selected.push(*keep)?;
	Ok(())
}

fn read_index(bytes: &[u8], width: usize) -> Result<usize, EncodingComputeRejectCode> {
	Ok(match width {
		1 => usize::from(bytes[0]),
		2 => usize::from(u16::from_le_bytes([bytes[0], bytes[1]])),
		4 => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize,
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}
