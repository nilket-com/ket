use crate::schema::RowCount;
use crate::storage::encoding::compute::{
	EncodedRowMask, EncodingComputeRejectCode, LiteralValue, PredicateAtomOp,
};
use crate::storage::encoding::{BufferEncoding, EncodingRejectCode};
use arrow::datatypes::DataType;
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NativeValue {
	Signed(i128),
	Unsigned(u128),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SignedPredicate {
	Eq(i128),
	Neq(i128),
	Gt(i128),
	Gte(i128),
	Lt(i128),
	Lte(i128),
	InList(Vec<i128>),
}

impl SignedPredicate {
	pub(crate) fn evaluate(&self, value: i128) -> bool {
		match self {
			Self::Eq(literal) => value == *literal,
			Self::Neq(literal) => value != *literal,
			Self::Gt(literal) => value > *literal,
			Self::Gte(literal) => value >= *literal,
			Self::Lt(literal) => value < *literal,
			Self::Lte(literal) => value <= *literal,
			Self::InList(values) => values.contains(&value),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsignedPredicate {
	Eq(u128),
	Neq(u128),
	Gt(u128),
	Gte(u128),
	Lt(u128),
	Lte(u128),
	InList(Vec<u128>),
}

impl UnsignedPredicate {
	pub(crate) fn evaluate(&self, value: u128) -> bool {
		match self {
			Self::Eq(literal) => value == *literal,
			Self::Neq(literal) => value != *literal,
			Self::Gt(literal) => value > *literal,
			Self::Gte(literal) => value >= *literal,
			Self::Lt(literal) => value < *literal,
			Self::Lte(literal) => value <= *literal,
			Self::InList(values) => values.contains(&value),
		}
	}
}

pub(crate) struct MaskBuilder {
	rows: RowCount,
	row: usize,
	bytes: Vec<u8>,
	any: bool,
	all: bool,
}

impl MaskBuilder {
	pub(crate) fn with_rows(rows: usize) -> Self {
		Self {
			rows: RowCount::new(rows as u64),
			row: 0,
			bytes: vec![0_u8; rows.div_ceil(8)],
			any: false,
			all: true,
		}
	}

	pub(crate) fn push(&mut self, selected: bool) -> Result<(), EncodingComputeRejectCode> {
		if self.row >= self.rows.get() as usize {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		if selected {
			self.bytes[self.row / 8] |= 1_u8 << (self.row % 8);
			self.any = true;
		} else {
			self.all = false;
		}
		self.row += 1;
		Ok(())
	}

	pub(crate) fn push_many(
		&mut self,
		selected: bool,
		count: usize,
	) -> Result<(), EncodingComputeRejectCode> {
		let end = self
			.row
			.checked_add(count)
			.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
		if end > self.rows.get() as usize {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		if selected {
			let mut row = self.row;
			while row < end && row % 8 != 0 {
				self.bytes[row / 8] |= 1_u8 << (row % 8);
				row += 1;
			}
			while row + 8 <= end {
				self.bytes[row / 8] = 0xff;
				row += 8;
			}
			while row < end {
				self.bytes[row / 8] |= 1_u8 << (row % 8);
				row += 1;
			}
			if count > 0 {
				self.any = true;
			}
		} else if count > 0 {
			self.all = false;
		}
		self.row = end;
		Ok(())
	}

	pub(crate) fn push_aligned_bitmap_byte(
		&mut self,
		mask: u8,
		count: usize,
	) -> Result<(), EncodingComputeRejectCode> {
		if count == 0 || count > 8 || self.row % 8 != 0 {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		let end = self
			.row
			.checked_add(count)
			.ok_or(EncodingComputeRejectCode::MaskLengthMismatch)?;
		if end > self.rows.get() as usize {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		let full_mask = if count == 8 {
			u8::MAX
		} else {
			(1_u8 << count) - 1
		};
		let selected = mask & full_mask;
		self.bytes[self.row / 8] = selected;
		self.any |= selected != 0;
		self.all &= selected == full_mask;
		self.row = end;
		Ok(())
	}

	pub(crate) fn len(&self) -> usize {
		self.row
	}

	pub(crate) fn finish(self) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
		if self.row != self.rows.get() as usize {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		Ok(if self.all {
			EncodedRowMask::All { rows: self.rows }
		} else if !self.any {
			EncodedRowMask::None { rows: self.rows }
		} else {
			EncodedRowMask::Bitmap {
				rows: self.rows,
				bytes: self.bytes,
			}
		})
	}
}

pub(crate) fn decoded_fixed_width_bytes_per_value(
	encoding: &BufferEncoding,
) -> Result<usize, EncodingComputeRejectCode> {
	let values = usize::try_from(encoding.decoded_value_count.get())
		.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	let bytes = usize::try_from(encoding.decoded_byte_len.get())
		.map_err(|_| EncodingComputeRejectCode::KernelDecodedLengthMismatch)?;
	if values == 0 || bytes == 0 || bytes % values != 0 {
		return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
	}
	Ok(bytes / values)
}

pub(crate) fn fixed_width_for_data_type(data_type: &DataType) -> Option<usize> {
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

pub(crate) fn native_value_from_u128(
	value: u128,
	width: usize,
	data_type: &DataType,
) -> Result<NativeValue, EncodingComputeRejectCode> {
	match data_type {
		DataType::Int8 if width == 1 => Ok(NativeValue::Signed(i128::from(i8::from_le_bytes([
			value as u8,
		])))),
		DataType::Int16 if width == 2 => Ok(NativeValue::Signed(i128::from(i16::from_le_bytes(
			(value as u16).to_le_bytes(),
		)))),
		DataType::Int32 | DataType::Date32 | DataType::Time32(_) if width == 4 => Ok(
			NativeValue::Signed(i128::from(i32::from_le_bytes((value as u32).to_le_bytes()))),
		),
		DataType::Int64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Timestamp(_, _)
		| DataType::Duration(_)
			if width == 8 =>
		{
			Ok(NativeValue::Signed(i128::from(i64::from_le_bytes(
				(value as u64).to_le_bytes(),
			))))
		}
		DataType::UInt8 if width == 1 => Ok(NativeValue::Unsigned(u128::from(value as u8))),
		DataType::UInt16 if width == 2 => Ok(NativeValue::Unsigned(u128::from(value as u16))),
		DataType::UInt32 if width == 4 => Ok(NativeValue::Unsigned(u128::from(value as u32))),
		DataType::UInt64 if width == 8 => Ok(NativeValue::Unsigned(u128::from(value as u64))),
		_ => Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	}
}

pub(crate) fn native_value_from_le_bytes(
	bytes: &[u8],
	data_type: &DataType,
) -> Result<NativeValue, EncodingComputeRejectCode> {
	native_value_from_u128(read_unsigned_le_as_u128(bytes), bytes.len(), data_type)
}

pub(crate) fn signed_value_from_le_bytes(
	bytes: &[u8],
	data_type: &DataType,
) -> Result<i128, EncodingComputeRejectCode> {
	Ok(match data_type {
		DataType::Int8 if bytes.len() == 1 => i128::from(i8::from_le_bytes([bytes[0]])),
		DataType::Int16 if bytes.len() == 2 => i128::from(i16::from_le_bytes([bytes[0], bytes[1]])),
		DataType::Int32 | DataType::Date32 | DataType::Time32(_) if bytes.len() == 4 => {
			i128::from(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
		}
		DataType::Int64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Timestamp(_, _)
		| DataType::Duration(_)
			if bytes.len() == 8 =>
		{
			i128::from(i64::from_le_bytes([
				bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
			]))
		}
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

pub(crate) fn signed_value_from_u128(
	value: u128,
	width: usize,
	data_type: &DataType,
) -> Result<i128, EncodingComputeRejectCode> {
	Ok(match data_type {
		DataType::Int8 if width == 1 => i128::from(i8::from_le_bytes([value as u8])),
		DataType::Int16 if width == 2 => {
			i128::from(i16::from_le_bytes((value as u16).to_le_bytes()))
		}
		DataType::Int32 | DataType::Date32 | DataType::Time32(_) if width == 4 => {
			i128::from(i32::from_le_bytes((value as u32).to_le_bytes()))
		}
		DataType::Int64
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Timestamp(_, _)
		| DataType::Duration(_)
			if width == 8 =>
		{
			i128::from(i64::from_le_bytes((value as u64).to_le_bytes()))
		}
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

pub(crate) fn signed_predicate(
	op: PredicateAtomOp,
	literal: &LiteralValue,
) -> Result<SignedPredicate, EncodingComputeRejectCode> {
	Ok(match (op, literal) {
		(PredicateAtomOp::Eq, LiteralValue::Signed(value)) => SignedPredicate::Eq(*value),
		(PredicateAtomOp::Neq, LiteralValue::Signed(value)) => SignedPredicate::Neq(*value),
		(PredicateAtomOp::Gt, LiteralValue::Signed(value)) => SignedPredicate::Gt(*value),
		(PredicateAtomOp::Gte, LiteralValue::Signed(value)) => SignedPredicate::Gte(*value),
		(PredicateAtomOp::Lt, LiteralValue::Signed(value)) => SignedPredicate::Lt(*value),
		(PredicateAtomOp::Lte, LiteralValue::Signed(value)) => SignedPredicate::Lte(*value),
		(PredicateAtomOp::InList, LiteralValue::List(values)) => {
			let mut signed = Vec::with_capacity(values.len());
			for value in values {
				let LiteralValue::Signed(value) = value else {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				};
				signed.push(*value);
			}
			SignedPredicate::InList(signed)
		}
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

pub(crate) fn unsigned_predicate(
	op: PredicateAtomOp,
	literal: &LiteralValue,
) -> Result<UnsignedPredicate, EncodingComputeRejectCode> {
	Ok(match (op, literal) {
		(PredicateAtomOp::Eq, LiteralValue::Unsigned(value)) => UnsignedPredicate::Eq(*value),
		(PredicateAtomOp::Neq, LiteralValue::Unsigned(value)) => UnsignedPredicate::Neq(*value),
		(PredicateAtomOp::Gt, LiteralValue::Unsigned(value)) => UnsignedPredicate::Gt(*value),
		(PredicateAtomOp::Gte, LiteralValue::Unsigned(value)) => UnsignedPredicate::Gte(*value),
		(PredicateAtomOp::Lt, LiteralValue::Unsigned(value)) => UnsignedPredicate::Lt(*value),
		(PredicateAtomOp::Lte, LiteralValue::Unsigned(value)) => UnsignedPredicate::Lte(*value),
		(PredicateAtomOp::InList, LiteralValue::List(values)) => {
			let mut unsigned = Vec::with_capacity(values.len());
			for value in values {
				let LiteralValue::Unsigned(value) = value else {
					return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch);
				};
				unsigned.push(*value);
			}
			UnsignedPredicate::InList(unsigned)
		}
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

pub(crate) fn data_type_is_signed(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::Date32
			| DataType::Date64
			| DataType::Time32(_)
			| DataType::Time64(_)
			| DataType::Timestamp(_, _)
			| DataType::Duration(_)
	)
}

pub(crate) fn data_type_is_unsigned(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
	)
}

pub(crate) fn evaluate_value(
	value: &NativeValue,
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

pub(crate) fn map_encoding_reject(code: EncodingRejectCode) -> EncodingComputeRejectCode {
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

fn compare_eq(
	value: &NativeValue,
	literal: &LiteralValue,
) -> Result<bool, EncodingComputeRejectCode> {
	Ok(match (value, literal) {
		(NativeValue::Signed(left), LiteralValue::Signed(right)) => left == right,
		(NativeValue::Unsigned(left), LiteralValue::Unsigned(right)) => left == right,
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	})
}

fn compare_ordered(
	value: &NativeValue,
	literal: &LiteralValue,
	predicate: impl Fn(Ordering) -> bool,
) -> Result<bool, EncodingComputeRejectCode> {
	let ordering = match (value, literal) {
		(NativeValue::Signed(left), LiteralValue::Signed(right)) => left.cmp(right),
		(NativeValue::Unsigned(left), LiteralValue::Unsigned(right)) => left.cmp(right),
		_ => return Err(EncodingComputeRejectCode::KernelDecodedLengthMismatch),
	};
	Ok(predicate(ordering))
}

pub(crate) fn read_unsigned_le_as_u128(bytes: &[u8]) -> u128 {
	let mut out = 0_u128;
	for (shift, byte) in bytes.iter().copied().enumerate() {
		out |= u128::from(byte) << (shift * 8);
	}
	out
}
