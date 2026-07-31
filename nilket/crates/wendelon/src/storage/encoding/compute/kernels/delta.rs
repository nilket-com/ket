use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::BufferEncoding;
use crate::storage::encoding::cascade::parse_delta_frame;
use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;
use crate::storage::encoding::compute::kernels::scalar::{
	MaskBuilder, data_type_is_signed, data_type_is_unsigned, decoded_fixed_width_bytes_per_value,
	evaluate_value, fixed_width_for_data_type, map_encoding_reject, native_value_from_u128,
	signed_predicate, signed_value_from_u128, unsigned_predicate,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, LiteralCoercion, LiteralValue, PredicateAtom,
	PredicateAtomOp,
};
use crate::storage::encoding::page::expand_page_descriptors;

pub const fn evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::Delta
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
	decoded_byte_len: RecordBodyByteLen,
	value_count: BufferValueCount,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let frame = parse_delta_frame(encoded_bytes, decoded_byte_len).map_err(map_encoding_reject)?;
	if frame.value_count != value_count.get() as usize {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	if frame.value_count == 0 {
		return Ok(());
	}
	if try_push_monotonic_signed_range(&frame, atom.op, coercion, literal, selected)? {
		return Ok(());
	}
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
	let mut emitted = 0_usize;
	let mut value = frame.first_value;
	push_value(
		value,
		frame.value_width,
		atom,
		coercion,
		literal,
		unsigned.as_ref(),
		signed.as_ref(),
		selected,
	)?;
	emitted += 1;
	for delta_bytes in frame.deltas.chunks_exact(frame.delta_width) {
		let delta = read_signed_delta(delta_bytes, frame.delta_width)?;
		value = value
			.checked_add(delta)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		push_value(
			value,
			frame.value_width,
			atom,
			coercion,
			literal,
			unsigned.as_ref(),
			signed.as_ref(),
			selected,
		)?;
		emitted += 1;
	}
	if emitted != frame.value_count {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(())
}

fn try_push_monotonic_signed_range(
	frame: &crate::storage::encoding::cascade::DeltaFrame<'_>,
	op: PredicateAtomOp,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	selected: &mut MaskBuilder,
) -> Result<bool, EncodingComputeRejectCode> {
	if !data_type_is_signed(&coercion.data_type) {
		return Ok(false);
	}
	let LiteralValue::Signed(bound) = literal else {
		return Ok(false);
	};
	let lower_bound = matches!(op, PredicateAtomOp::Gt | PredicateAtomOp::Gte);
	let upper_bound = matches!(op, PredicateAtomOp::Lt | PredicateAtomOp::Lte);
	if !lower_bound && !upper_bound {
		return Ok(false);
	}

	let mut value = frame.first_value;
	let mut transition = None::<usize>;
	for row in 0..frame.value_count {
		if transition.is_none() {
			let keep = match op {
				PredicateAtomOp::Gt => value > *bound,
				PredicateAtomOp::Gte => value >= *bound,
				PredicateAtomOp::Lt => value < *bound,
				PredicateAtomOp::Lte => value <= *bound,
				_ => false,
			};
			if (lower_bound && keep) || (upper_bound && !keep) {
				transition = Some(row);
			}
		}
		if row + 1 == frame.value_count {
			break;
		}
		let start = row
			.checked_mul(frame.delta_width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let end = start
			.checked_add(frame.delta_width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
		let Some(delta_bytes) = frame.deltas.get(start..end) else {
			return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
		};
		let delta = read_signed_delta(delta_bytes, frame.delta_width)?;
		if delta < 0 {
			return Ok(false);
		}
		value = value
			.checked_add(delta)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	}

	let transition = transition.unwrap_or(frame.value_count);
	if lower_bound {
		selected.push_many(false, transition)?;
		selected.push_many(true, frame.value_count - transition)?;
	} else {
		selected.push_many(true, transition)?;
		selected.push_many(false, frame.value_count - transition)?;
	}
	Ok(true)
}

fn push_value(
	value: i128,
	value_width: usize,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
	literal: &LiteralValue,
	unsigned: Option<&crate::storage::encoding::compute::kernels::scalar::UnsignedPredicate>,
	signed: Option<&crate::storage::encoding::compute::kernels::scalar::SignedPredicate>,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let keep = if let Some(predicate) = unsigned {
		predicate.evaluate(value as u128)
	} else if let Some(predicate) = signed {
		let value = signed_value_from_u128(value as u128, value_width, &coercion.data_type)?;
		predicate.evaluate(value)
	} else {
		let value = native_value_from_u128(value as u128, value_width, &coercion.data_type)?;
		evaluate_value(&value, atom.op, literal)?
	};
	selected.push(keep)?;
	Ok(())
}

fn read_signed_delta(bytes: &[u8], width: usize) -> Result<i128, EncodingComputeRejectCode> {
	Ok(match width {
		1 => i128::from(i8::from_le_bytes([bytes[0]])),
		2 => i128::from(i16::from_le_bytes([bytes[0], bytes[1]])),
		4 => i128::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
		8 => i128::from(i64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		])),
		16 => i128::from_le_bytes(
			bytes
				.try_into()
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?,
		),
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}
