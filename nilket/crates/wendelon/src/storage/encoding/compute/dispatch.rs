use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::cascade::decode_buffer;
use crate::storage::encoding::compute::kernels::scalar::{
	MaskBuilder, read_unsigned_le_as_u128, signed_predicate, signed_value_from_le_bytes,
	unsigned_predicate,
};
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeEvaluatorKind, EncodingComputeRejectCode, LiteralCoercion,
	LiteralValue, PredicateAtom, PredicateAtomOp, kernels,
};
use crate::storage::encoding::page::expand_page_descriptors;
use crate::storage::encoding::{
	BufferEncoding, EncodingCascade, EncodingLeafCascade, EncodingRejectCode,
};
use arrow::datatypes::DataType;
use std::cmp::Ordering;

pub fn evaluate(
	encoded_bytes: &[u8],
	encoding: &BufferEncoding,
	cascade: &EncodingCascade,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	if encoding.decoded_value_count.get() == 0 {
		return evaluate_decode_scalar(encoded_bytes, encoding, cascade, atom, coercion);
	}
	match native_evaluation_eligibility(cascade, atom, coercion) {
		NativeEvaluationEligibility::Eligible(EncodingComputeEvaluatorKind::BitPacked) => {
			let EncodingCascade::BitPacked { bits, base } = cascade else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			kernels::bitpacked::evaluate_predicate(
				encoded_bytes,
				encoding,
				*bits,
				*base,
				atom,
				coercion,
			)
		}
		NativeEvaluationEligibility::Eligible(
			EncodingComputeEvaluatorKind::DictionaryPlain
			| EncodingComputeEvaluatorKind::DictionaryBitPacked,
		) => {
			let EncodingCascade::Dictionary { inner, .. } = cascade else {
				return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
			};
			kernels::dictionary::evaluate_predicate(
				encoded_bytes,
				encoding,
				inner.as_ref(),
				atom,
				coercion,
			)
		}
		NativeEvaluationEligibility::Eligible(EncodingComputeEvaluatorKind::Delta) => {
			kernels::delta::evaluate_predicate(encoded_bytes, encoding, atom, coercion)
		}
		NativeEvaluationEligibility::Eligible(
			EncodingComputeEvaluatorKind::Fsst | EncodingComputeEvaluatorKind::FsstRowSafe,
		) => evaluate_decode_scalar(encoded_bytes, encoding, cascade, atom, coercion),
		NativeEvaluationEligibility::Eligible(
			EncodingComputeEvaluatorKind::RlePlain | EncodingComputeEvaluatorKind::RleBitPacked,
		) => kernels::rle::evaluate_predicate(encoded_bytes, encoding, atom, coercion),
		NativeEvaluationEligibility::DecodeScalarFallback => {
			evaluate_decode_scalar(encoded_bytes, encoding, cascade, atom, coercion)
		}
	}
}

pub fn evaluate_decode_scalar(
	encoded_bytes: &[u8],
	encoding: &BufferEncoding,
	cascade: &EncodingCascade,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let decoded = decode_encoded_buffer(encoded_bytes, &coercion.data_type, cascade, encoding)
		.map_err(map_decode_reject)?;
	evaluate_decoded(&decoded, encoding.decoded_value_count, atom, coercion)
}

pub fn evaluate_fsst_predicate(
	value_bytes: &[u8],
	value_encoding: &BufferEncoding,
	offset_bytes: &[u8],
	offset_encoding: Option<&BufferEncoding>,
	offset_cascade: Option<&EncodingCascade>,
	data_type: &DataType,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	if !matches!(
		atom.op,
		PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList
	) || !kernels::fsst::string_like_data_type_supported(data_type)
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let frame = kernels::fsst::parse_fsst_frame(value_bytes, value_encoding.decoded_byte_len)?;
	let offsets = kernels::fsst::decode_offsets(
		offset_bytes,
		data_type,
		value_encoding.decoded_value_count,
		RecordBodyByteLen::new(frame.expected_decoded_byte_len() as u64),
		offset_encoding,
		offset_cascade,
	)?;
	with_fsst_literal_bytes(coercion, |literals| {
		kernels::fsst::evaluate_fsst_equality(&frame, &offsets, literals, atom.op)
	})
}

pub fn evaluate_fsst_row_safe_predicate(
	value_bytes: &[u8],
	value_encoding: &BufferEncoding,
	offset_bytes: &[u8],
	offset_encoding: Option<&BufferEncoding>,
	offset_cascade: Option<&EncodingCascade>,
	data_type: &DataType,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	if !matches!(
		atom.op,
		PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList
	) || !kernels::fsst::string_like_data_type_supported(data_type)
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let frame = kernels::fsst::parse_fsst_row_safe_frame(
		value_bytes,
		value_encoding.decoded_byte_len,
		value_encoding.decoded_value_count,
	)?;
	let offsets = kernels::fsst::decode_offsets(
		offset_bytes,
		data_type,
		value_encoding.decoded_value_count,
		RecordBodyByteLen::new(frame.expected_decoded_byte_len() as u64),
		offset_encoding,
		offset_cascade,
	)?;
	with_fsst_literal_bytes(coercion, |literals| {
		kernels::fsst::evaluate_fsst_row_safe_equality(&frame, &offsets, literals, atom.op)
	})
}

pub fn evaluate_fsst_predicate_with_stats(
	value_bytes: &[u8],
	value_encoding: &BufferEncoding,
	offset_bytes: &[u8],
	offset_encoding: Option<&BufferEncoding>,
	offset_cascade: Option<&EncodingCascade>,
	data_type: &DataType,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<
	(
		EncodedRowMask,
		crate::storage::encoding::compute::kernels::fsst::FsstEvaluationStats,
	),
	EncodingComputeRejectCode,
> {
	if !matches!(
		atom.op,
		PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList
	) || !kernels::fsst::string_like_data_type_supported(data_type)
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let frame = kernels::fsst::parse_fsst_frame(value_bytes, value_encoding.decoded_byte_len)?;
	let offsets = kernels::fsst::decode_offsets(
		offset_bytes,
		data_type,
		value_encoding.decoded_value_count,
		RecordBodyByteLen::new(frame.expected_decoded_byte_len() as u64),
		offset_encoding,
		offset_cascade,
	)?;
	with_fsst_literal_bytes(coercion, |literals| {
		kernels::fsst::evaluate_fsst_equality_with_stats(&frame, &offsets, literals, atom.op)
	})
}

pub fn evaluate_fsst_row_safe_predicate_with_stats(
	value_bytes: &[u8],
	value_encoding: &BufferEncoding,
	offset_bytes: &[u8],
	offset_encoding: Option<&BufferEncoding>,
	offset_cascade: Option<&EncodingCascade>,
	data_type: &DataType,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<
	(
		EncodedRowMask,
		crate::storage::encoding::compute::kernels::fsst::FsstRowSafeEvaluationStats,
	),
	EncodingComputeRejectCode,
> {
	if !matches!(
		atom.op,
		PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList
	) || !kernels::fsst::string_like_data_type_supported(data_type)
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let frame = kernels::fsst::parse_fsst_row_safe_frame(
		value_bytes,
		value_encoding.decoded_byte_len,
		value_encoding.decoded_value_count,
	)?;
	let offsets = kernels::fsst::decode_offsets(
		offset_bytes,
		data_type,
		value_encoding.decoded_value_count,
		RecordBodyByteLen::new(frame.expected_decoded_byte_len() as u64),
		offset_encoding,
		offset_cascade,
	)?;
	with_fsst_literal_bytes(coercion, |literals| {
		kernels::fsst::evaluate_fsst_row_safe_equality_with_stats(
			&frame, &offsets, literals, atom.op,
		)
	})
}

pub fn evaluate_decoded(
	decoded_bytes: &[u8],
	decoded_value_count: BufferValueCount,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let rows = usize::try_from(decoded_value_count.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if matches!(
		atom.op,
		PredicateAtomOp::IsNull | PredicateAtomOp::IsNotNull
	) {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}

	let literal = coercion
		.literal
		.as_ref()
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if let Some(mask) = evaluate_decoded_fixed_width_integer(
		decoded_bytes,
		rows,
		&coercion.data_type,
		atom,
		literal,
	)? {
		return Ok(mask);
	}
	let values = decoded_scalars(decoded_bytes, rows, &coercion.data_type)?;
	let selected = values
		.iter()
		.map(|value| evaluate_scalar(value, atom.op, literal))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(EncodedRowMask::from_bool_slice(&selected))
}

fn evaluate_decoded_fixed_width_integer(
	decoded_bytes: &[u8],
	rows: usize,
	data_type: &DataType,
	atom: &PredicateAtom,
	literal: &LiteralValue,
) -> Result<Option<EncodedRowMask>, EncodingComputeRejectCode> {
	let Some(width) = fixed_width_integer_bytes(data_type) else {
		return Ok(None);
	};
	if decoded_bytes.len()
		!= rows
			.checked_mul(width)
			.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?
	{
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut selected = MaskBuilder::with_rows(rows);
	if fixed_width_integer_is_unsigned(data_type) {
		let predicate = unsigned_predicate(atom.op, literal)?;
		push_unsigned_decoded_mask(decoded_bytes, width, &predicate, &mut selected)?;
		return selected.finish().map(Some);
	}
	let predicate = signed_predicate(atom.op, literal)?;
	push_signed_decoded_mask(decoded_bytes, width, data_type, &predicate, &mut selected)?;
	selected.finish().map(Some)
}

fn push_unsigned_decoded_mask(
	decoded_bytes: &[u8],
	width: usize,
	predicate: &crate::storage::encoding::compute::kernels::scalar::UnsignedPredicate,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let mut cursor = 0_usize;
	while cursor < decoded_bytes.len() && selected.len() % 8 != 0 {
		selected.push(predicate.evaluate(read_unsigned_le_as_u128(
			&decoded_bytes[cursor..cursor + width],
		)))?;
		cursor += width;
	}
	while cursor + (8 * width) <= decoded_bytes.len() {
		let mut mask = 0_u8;
		for lane in 0..8 {
			let start = cursor + lane * width;
			if predicate.evaluate(read_unsigned_le_as_u128(
				&decoded_bytes[start..start + width],
			)) {
				mask |= 1_u8 << lane;
			}
		}
		selected.push_aligned_bitmap_byte(mask, 8)?;
		cursor += 8 * width;
	}
	while cursor < decoded_bytes.len() {
		selected.push(predicate.evaluate(read_unsigned_le_as_u128(
			&decoded_bytes[cursor..cursor + width],
		)))?;
		cursor += width;
	}
	Ok(())
}

fn push_signed_decoded_mask(
	decoded_bytes: &[u8],
	width: usize,
	data_type: &DataType,
	predicate: &crate::storage::encoding::compute::kernels::scalar::SignedPredicate,
	selected: &mut MaskBuilder,
) -> Result<(), EncodingComputeRejectCode> {
	let mut cursor = 0_usize;
	while cursor < decoded_bytes.len() && selected.len() % 8 != 0 {
		selected.push(predicate.evaluate(signed_value_from_le_bytes(
			&decoded_bytes[cursor..cursor + width],
			data_type,
		)?))?;
		cursor += width;
	}
	while cursor + (8 * width) <= decoded_bytes.len() {
		let mut mask = 0_u8;
		for lane in 0..8 {
			let start = cursor + lane * width;
			if predicate.evaluate(signed_value_from_le_bytes(
				&decoded_bytes[start..start + width],
				data_type,
			)?) {
				mask |= 1_u8 << lane;
			}
		}
		selected.push_aligned_bitmap_byte(mask, 8)?;
		cursor += 8 * width;
	}
	while cursor < decoded_bytes.len() {
		selected.push(predicate.evaluate(signed_value_from_le_bytes(
			&decoded_bytes[cursor..cursor + width],
			data_type,
		)?))?;
		cursor += width;
	}
	Ok(())
}

fn fixed_width_integer_bytes(data_type: &DataType) -> Option<usize> {
	match data_type {
		DataType::Int8 | DataType::UInt8 => Some(1),
		DataType::Int16 | DataType::UInt16 => Some(2),
		DataType::Int32 | DataType::UInt32 | DataType::Date32 | DataType::Time32(_) => Some(4),
		DataType::Int64
		| DataType::UInt64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Timestamp(_, _)
		| DataType::Duration(_) => Some(8),
		_ => None,
	}
}

fn fixed_width_integer_is_unsigned(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
	)
}

pub fn evaluator_kind_for_cascade(
	cascade: &EncodingCascade,
) -> Option<EncodingComputeEvaluatorKind> {
	match cascade {
		EncodingCascade::BitPacked { .. } => Some(EncodingComputeEvaluatorKind::BitPacked),
		EncodingCascade::Rle { inner, .. } => match inner.as_ref() {
			EncodingLeafCascade::Plain => Some(EncodingComputeEvaluatorKind::RlePlain),
			EncodingLeafCascade::BitPacked { .. } => {
				Some(EncodingComputeEvaluatorKind::RleBitPacked)
			}
			EncodingLeafCascade::Lz4Frame => None,
		},
		EncodingCascade::Dictionary { inner, .. } => match inner.as_ref() {
			EncodingLeafCascade::Plain => Some(EncodingComputeEvaluatorKind::DictionaryPlain),
			EncodingLeafCascade::BitPacked { .. } => {
				Some(EncodingComputeEvaluatorKind::DictionaryBitPacked)
			}
			EncodingLeafCascade::Lz4Frame => None,
		},
		EncodingCascade::Delta { .. } => Some(EncodingComputeEvaluatorKind::Delta),
		EncodingCascade::Fsst { .. } => Some(EncodingComputeEvaluatorKind::Fsst),
		EncodingCascade::FsstRowSafe { .. } => Some(EncodingComputeEvaluatorKind::FsstRowSafe),
		EncodingCascade::Plain | EncodingCascade::Lz4Frame | EncodingCascade::Alp { .. } => None,
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeEvaluationEligibility {
	Eligible(EncodingComputeEvaluatorKind),
	DecodeScalarFallback,
}

pub(crate) fn native_evaluation_eligibility(
	cascade: &EncodingCascade,
	atom: &PredicateAtom,
	coercion: &LiteralCoercion,
) -> NativeEvaluationEligibility {
	if matches!(
		atom.op,
		PredicateAtomOp::IsNull | PredicateAtomOp::IsNotNull
	) || coercion.literal.is_none()
	{
		return NativeEvaluationEligibility::DecodeScalarFallback;
	}
	let Some(kind) = evaluator_kind_for_cascade(cascade) else {
		return NativeEvaluationEligibility::DecodeScalarFallback;
	};
	if matches!(
		kind,
		EncodingComputeEvaluatorKind::Fsst | EncodingComputeEvaluatorKind::FsstRowSafe
	) {
		if fsst_native_evaluation_eligible(atom, coercion) {
			return NativeEvaluationEligibility::Eligible(kind);
		}
		return NativeEvaluationEligibility::DecodeScalarFallback;
	}
	if !native_value_type_supported(&coercion.data_type) {
		return NativeEvaluationEligibility::DecodeScalarFallback;
	}
	if !native_predicate_cascade_eligible(cascade, &coercion.data_type) {
		return NativeEvaluationEligibility::DecodeScalarFallback;
	}
	NativeEvaluationEligibility::Eligible(kind)
}

fn native_predicate_cascade_eligible(cascade: &EncodingCascade, data_type: &DataType) -> bool {
	match cascade {
		EncodingCascade::BitPacked { bits, .. } => {
			matches!(data_type, DataType::UInt8) && bits.get() == 8
		}
		EncodingCascade::Dictionary {
			dict_value_count,
			inner,
			..
		} => {
			dict_value_count.get() <= 4
				&& match inner.as_ref() {
					EncodingLeafCascade::Plain => true,
					EncodingLeafCascade::BitPacked { bits, .. } => bits.get() <= 2,
					EncodingLeafCascade::Lz4Frame => false,
				}
		}
		EncodingCascade::Delta { .. } | EncodingCascade::Rle { .. } => true,
		EncodingCascade::Plain
		| EncodingCascade::Lz4Frame
		| EncodingCascade::Fsst { .. }
		| EncodingCascade::FsstRowSafe { .. }
		| EncodingCascade::Alp { .. } => false,
	}
}

fn fsst_native_evaluation_eligible(atom: &PredicateAtom, coercion: &LiteralCoercion) -> bool {
	matches!(
		atom.op,
		PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList
	) && kernels::fsst::string_like_data_type_supported(&coercion.data_type)
		&& fsst_literal_bytes_supported(coercion)
}

fn with_fsst_literal_bytes<'a, R>(
	coercion: &'a LiteralCoercion,
	evaluate: impl FnOnce(&[kernels::fsst::LiteralBytes<'a>]) -> Result<R, EncodingComputeRejectCode>,
) -> Result<R, EncodingComputeRejectCode> {
	let literal = coercion
		.literal
		.as_ref()
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	match literal {
		LiteralValue::Bytes(bytes) => {
			let literals = [kernels::fsst::LiteralBytes::new(bytes.as_slice())];
			evaluate(&literals)
		}
		LiteralValue::List(values) => {
			let literals = values
				.iter()
				.map(|value| match value {
					LiteralValue::Bytes(bytes) => {
						Ok(kernels::fsst::LiteralBytes::new(bytes.as_slice()))
					}
					_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
				})
				.collect::<Result<Vec<_>, _>>()?;
			evaluate(&literals)
		}
		_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	}
}

fn fsst_literal_bytes_supported(coercion: &LiteralCoercion) -> bool {
	match coercion.literal.as_ref() {
		Some(LiteralValue::Bytes(_)) => true,
		Some(LiteralValue::List(values)) => values
			.iter()
			.all(|value| matches!(value, LiteralValue::Bytes(_))),
		_ => false,
	}
}

pub(crate) fn decode_encoded_buffer(
	encoded: &[u8],
	data_type: &DataType,
	cascade: &EncodingCascade,
	encoding: &BufferEncoding,
) -> Result<Vec<u8>, EncodingRejectCode> {
	if encoding.page_end_offsets.is_empty() {
		return decode_buffer(
			encoded,
			data_type,
			cascade,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		);
	}
	let Some(bytes_per_value) = decoded_fixed_width_bytes_per_value(encoding) else {
		return decode_buffer(
			encoded,
			data_type,
			cascade,
			encoding.decoded_byte_len,
			encoding.decoded_value_count,
		);
	};
	let pages = expand_page_descriptors(encoding)?;
	let mut out = Vec::with_capacity(encoding.decoded_byte_len.get() as usize);
	for page in pages {
		let start = usize::try_from(page.byte_range.start.get())
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let len = usize::try_from(page.byte_range.len.get())
			.map_err(|_| EncodingRejectCode::PageRangeOverflow)?;
		let end = start
			.checked_add(len)
			.ok_or(EncodingRejectCode::PageRangeOverflow)?;
		if end > encoded.len() {
			return Err(EncodingRejectCode::PageRangeOverflow);
		}
		let expected_bytes = usize::try_from(page.value_count.get())
			.map_err(|_| EncodingRejectCode::DecodedLengthMismatch)?
			.checked_mul(bytes_per_value)
			.ok_or(EncodingRejectCode::DecodedLengthMismatch)?;
		let decoded = decode_buffer(
			&encoded[start..end],
			data_type,
			cascade,
			RecordBodyByteLen::new(expected_bytes as u64),
			page.value_count,
		)?;
		out.extend_from_slice(&decoded);
	}
	if out.len() as u64 != encoding.decoded_byte_len.get() {
		return Err(EncodingRejectCode::DecodedLengthMismatch);
	}
	Ok(out)
}

fn decoded_fixed_width_bytes_per_value(encoding: &BufferEncoding) -> Option<usize> {
	let values = encoding.decoded_value_count.get() as usize;
	let bytes = usize::try_from(encoding.decoded_byte_len.get()).ok()?;
	if values == 0 || bytes == 0 || bytes % values != 0 {
		return None;
	}
	Some(bytes / values)
}

fn native_value_type_supported(data_type: &DataType) -> bool {
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

#[derive(Clone, Debug, PartialEq)]
enum DecodedScalar {
	Signed(i128),
	Unsigned(u128),
	Float(f64),
	Boolean(bool),
	Bytes(Vec<u8>),
}

fn decoded_scalars(
	decoded: &[u8],
	rows: usize,
	data_type: &DataType,
) -> Result<Vec<DecodedScalar>, EncodingComputeRejectCode> {
	match data_type {
		DataType::Boolean => decode_boolean_scalars(decoded, rows),
		DataType::Int8 => decode_fixed_width(decoded, rows, 1, |bytes| {
			DecodedScalar::Signed(i128::from(i8::from_le_bytes([bytes[0]])))
		}),
		DataType::Int16 => decode_fixed_width(decoded, rows, 2, |bytes| {
			DecodedScalar::Signed(i128::from(i16::from_le_bytes([bytes[0], bytes[1]])))
		}),
		DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
			decode_fixed_width(decoded, rows, 4, |bytes| {
				DecodedScalar::Signed(i128::from(i32::from_le_bytes([
					bytes[0], bytes[1], bytes[2], bytes[3],
				])))
			})
		}
		DataType::Int64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Timestamp(_, _)
		| DataType::Duration(_) => decode_fixed_width(decoded, rows, 8, |bytes| {
			DecodedScalar::Signed(i128::from(i64::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
			])))
		}),
		DataType::UInt8 => decode_fixed_width(decoded, rows, 1, |bytes| {
			DecodedScalar::Unsigned(u128::from(bytes[0]))
		}),
		DataType::UInt16 => decode_fixed_width(decoded, rows, 2, |bytes| {
			DecodedScalar::Unsigned(u128::from(u16::from_le_bytes([bytes[0], bytes[1]])))
		}),
		DataType::UInt32 => decode_fixed_width(decoded, rows, 4, |bytes| {
			DecodedScalar::Unsigned(u128::from(u32::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3],
			])))
		}),
		DataType::UInt64 => decode_fixed_width(decoded, rows, 8, |bytes| {
			DecodedScalar::Unsigned(u128::from(u64::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
			])))
		}),
		DataType::Float32 => decode_fixed_width(decoded, rows, 4, |bytes| {
			DecodedScalar::Float(f64::from(f32::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3],
			])))
		}),
		DataType::Float64 => decode_fixed_width(decoded, rows, 8, |bytes| {
			DecodedScalar::Float(f64::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
			]))
		}),
		DataType::FixedSizeBinary(width) => {
			let width = usize::try_from(*width)
				.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
			decode_fixed_width(decoded, rows, width, |bytes| {
				DecodedScalar::Bytes(bytes.to_vec())
			})
		}
		_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	}
}

fn decode_boolean_scalars(
	decoded: &[u8],
	rows: usize,
) -> Result<Vec<DecodedScalar>, EncodingComputeRejectCode> {
	let needed = rows.div_ceil(8);
	if decoded.len() < needed {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut out = Vec::with_capacity(rows);
	for row in 0..rows {
		let byte = decoded[row / 8];
		let bit = row % 8;
		out.push(DecodedScalar::Boolean(byte & (1_u8 << bit) != 0));
	}
	Ok(out)
}

fn decode_fixed_width(
	decoded: &[u8],
	rows: usize,
	width: usize,
	mut decode: impl FnMut(&[u8]) -> DecodedScalar,
) -> Result<Vec<DecodedScalar>, EncodingComputeRejectCode> {
	let needed = rows
		.checked_mul(width)
		.ok_or(EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if decoded.len() < needed {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	let mut out = Vec::with_capacity(rows);
	for row in 0..rows {
		let start = row * width;
		out.push(decode(&decoded[start..start + width]));
	}
	Ok(out)
}

fn evaluate_scalar(
	value: &DecodedScalar,
	op: PredicateAtomOp,
	literal: &LiteralValue,
) -> Result<bool, EncodingComputeRejectCode> {
	match op {
		PredicateAtomOp::Eq => Ok(compare_eq(value, literal)?),
		PredicateAtomOp::Neq => Ok(!compare_eq(value, literal)?),
		PredicateAtomOp::Gt => compare_ordered(value, literal, |ordering| ordering.is_gt()),
		PredicateAtomOp::Gte => compare_ordered(value, literal, |ordering| {
			ordering.is_gt() || ordering.is_eq()
		}),
		PredicateAtomOp::Lt => compare_ordered(value, literal, |ordering| ordering.is_lt()),
		PredicateAtomOp::Lte => compare_ordered(value, literal, |ordering| {
			ordering.is_lt() || ordering.is_eq()
		}),
		PredicateAtomOp::InList => match literal {
			LiteralValue::List(values) => {
				for candidate in values {
					if compare_eq(value, candidate)? {
						return Ok(true);
					}
				}
				Ok(false)
			}
			_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
		},
		PredicateAtomOp::IsNull | PredicateAtomOp::IsNotNull => {
			Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch)
		}
	}
}

fn compare_eq(
	value: &DecodedScalar,
	literal: &LiteralValue,
) -> Result<bool, EncodingComputeRejectCode> {
	Ok(match (value, literal) {
		(DecodedScalar::Signed(left), LiteralValue::Signed(right)) => left == right,
		(DecodedScalar::Unsigned(left), LiteralValue::Unsigned(right)) => left == right,
		(DecodedScalar::Float(left), LiteralValue::Float(right)) => left == right,
		(DecodedScalar::Boolean(left), LiteralValue::Boolean(right)) => left == right,
		(DecodedScalar::Bytes(left), LiteralValue::Bytes(right)) => left == right,
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

fn compare_ordered(
	value: &DecodedScalar,
	literal: &LiteralValue,
	predicate: impl Fn(Ordering) -> bool,
) -> Result<bool, EncodingComputeRejectCode> {
	let ordering = match (value, literal) {
		(DecodedScalar::Signed(left), LiteralValue::Signed(right)) => left.cmp(right),
		(DecodedScalar::Unsigned(left), LiteralValue::Unsigned(right)) => left.cmp(right),
		(DecodedScalar::Float(left), LiteralValue::Float(right)) => left
			.partial_cmp(right)
			.ok_or(EncodingComputeRejectCode::EncodedValueOverflow)?,
		(DecodedScalar::Bytes(left), LiteralValue::Bytes(right)) => left.cmp(right),
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	};
	Ok(predicate(ordering))
}

fn map_decode_reject(code: EncodingRejectCode) -> EncodingComputeRejectCode {
	match code {
		EncodingRejectCode::DecodedLengthMismatch => {
			EncodingComputeRejectCode::KernelDecodedLengthMismatch
		}
		EncodingRejectCode::EncodingDirectoryParseError
		| EncodingRejectCode::BufferEncodingMismatch
		| EncodingRejectCode::PageRangeOverflow
		| EncodingRejectCode::CascadeUnsupported => {
			EncodingComputeRejectCode::KernelDecodedLengthMismatch
		}
	}
}
