use crate::schema::{BufferValueCount, RecordBodyByteLen};
use crate::storage::encoding::cascade::decode_buffer;
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeEvaluatorKind, EncodingComputeRejectCode, LiteralCoercion,
	LiteralValue, PredicateAtom, PredicateAtomOp,
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
	let decoded = decode_encoded_buffer(encoded_bytes, &coercion.data_type, cascade, encoding)
		.map_err(map_decode_reject)?;
	evaluate_decoded(&decoded, encoding.decoded_value_count, atom, coercion)
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
	let values = decoded_scalars(decoded_bytes, rows, &coercion.data_type)?;
	let selected = values
		.iter()
		.map(|value| evaluate_scalar(value, atom.op, literal))
		.collect::<Result<Vec<_>, _>>()?;
	Ok(EncodedRowMask::from_bool_slice(&selected))
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
		EncodingCascade::Plain
		| EncodingCascade::Lz4Frame
		| EncodingCascade::Fsst { .. }
		| EncodingCascade::Alp { .. } => None,
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
