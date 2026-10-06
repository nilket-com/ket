use super::AdapterError;
use crate::storage::mutation_visibility::encoding::Reader;
use arrow::{
	array::*,
	datatypes::{DataType, TimeUnit},
};

/// Exact logical type; no timezone, metadata or numeric coercion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofScalarType {
	Boolean,
	Int8,
	Int16,
	Int32,
	Int64,
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Float32,
	Float64,
	Utf8,
	Binary,
	Date32,
	TimestampMillis,
	Decimal128(u8, u8),
	UtcMillis,
	UtcMicros,
	UtcNanos,
}
impl ProofScalarType {
	pub fn from_arrow(t: &DataType) -> Result<Self, AdapterError> {
		use DataType as D;
		Ok(match t {
			D::Boolean => Self::Boolean,
			D::Int8 => Self::Int8,
			D::Int16 => Self::Int16,
			D::Int32 => Self::Int32,
			D::Int64 => Self::Int64,
			D::UInt8 => Self::UInt8,
			D::UInt16 => Self::UInt16,
			D::UInt32 => Self::UInt32,
			D::UInt64 => Self::UInt64,
			D::Float32 => Self::Float32,
			D::Float64 => Self::Float64,
			D::Utf8 | D::LargeUtf8 => Self::Utf8,
			D::Binary | D::LargeBinary => Self::Binary,
			D::Date32 => Self::Date32,
			D::Timestamp(TimeUnit::Millisecond, None) => Self::TimestampMillis,
			D::Timestamp(unit, Some(zone)) if zone.as_ref() == "UTC" => match unit {
				TimeUnit::Millisecond => Self::UtcMillis,
				TimeUnit::Microsecond => Self::UtcMicros,
				TimeUnit::Nanosecond => Self::UtcNanos,
				_ => return Err(AdapterError::UnsupportedType),
			},
			D::Decimal128(p, s) if (1..=38).contains(p) && *s >= 0 && *s as u8 <= *p => {
				Self::Decimal128(*p, *s as u8)
			}
			_ => return Err(AdapterError::UnsupportedType),
		})
	}
	pub fn descriptor(self) -> Vec<u8> {
		use ProofScalarType::*;
		match self {
			Decimal128(p, s) => vec![16, p, s],
			UtcMillis => vec![17, 1, b'U', b'T', b'C'],
			UtcMicros => vec![18, 2, b'U', b'T', b'C'],
			UtcNanos => vec![19, 3, b'U', b'T', b'C'],
			_ => vec![match self {
				Boolean => 1,
				Int8 => 2,
				Int16 => 3,
				Int32 => 4,
				Int64 => 5,
				UInt8 => 6,
				UInt16 => 7,
				UInt32 => 8,
				UInt64 => 9,
				Float32 => 10,
				Float64 => 11,
				Utf8 => 12,
				Binary => 13,
				Date32 => 14,
				TimestampMillis => 15,
				_ => unreachable!(),
			}],
		}
	}
	pub fn pk_supported(self) -> bool {
		matches!(
			self,
			Self::Int8
				| Self::Int16
				| Self::Int32
				| Self::Int64
				| Self::UInt8
				| Self::UInt16
				| Self::UInt32
				| Self::UInt64
				| Self::Utf8 | Self::Binary
				| Self::TimestampMillis
				| Self::UtcMillis
		)
	}
	pub(super) fn read(r: &mut Reader<'_>) -> Result<Self, AdapterError> {
		let tag = r.u8()?;
		let t = match tag {
			1 => Self::Boolean,
			2 => Self::Int8,
			3 => Self::Int16,
			4 => Self::Int32,
			5 => Self::Int64,
			6 => Self::UInt8,
			7 => Self::UInt16,
			8 => Self::UInt32,
			9 => Self::UInt64,
			10 => Self::Float32,
			11 => Self::Float64,
			12 => Self::Utf8,
			13 => Self::Binary,
			14 => Self::Date32,
			15 => Self::TimestampMillis,
			16 => {
				let p = r.u8()?;
				let s = r.u8()?;
				if !(1..=38).contains(&p) || s > p {
					return Err(AdapterError::InvalidValue);
				}
				Self::Decimal128(p, s)
			}
			17..=19 => {
				if r.u8()? != tag - 16 || r.take(3)? != b"UTC" {
					return Err(AdapterError::InvalidValue);
				}
				match tag {
					17 => Self::UtcMillis,
					18 => Self::UtcMicros,
					_ => Self::UtcNanos,
				}
			}
			_ => return Err(AdapterError::UnsupportedType),
		};
		Ok(t)
	}
	pub(super) fn validate(self, b: &[u8]) -> Result<(), AdapterError> {
		use ProofScalarType::*;
		let width = match self {
			Boolean | Int8 | UInt8 => 1,
			Int16 | UInt16 => 2,
			Int32 | UInt32 | Float32 | Date32 => 4,
			Int64 | UInt64 | Float64 | TimestampMillis | UtcMillis | UtcMicros | UtcNanos => 8,
			Decimal128(..) => 16,
			Utf8 | Binary => b.len(),
		};
		if b.len() != width {
			return Err(AdapterError::InvalidValue);
		}
		match self {
			Boolean if b[0] > 1 => return Err(AdapterError::InvalidValue),
			Utf8 => {
				std::str::from_utf8(b).map_err(|_| AdapterError::InvalidValue)?;
			}
			Decimal128(p, _) => {
				let n = i128::from_le_bytes(b.try_into().map_err(|_| AdapterError::InvalidValue)?);
				if n.unsigned_abs() >= 10u128.pow(p as u32) {
					return Err(AdapterError::InvalidValue);
				}
			}
			_ => {}
		}
		Ok(())
	}
}

pub(super) enum CellBytes<'a> {
	Fixed([u8; 16], usize),
	Borrowed(&'a [u8]),
}
impl CellBytes<'_> {
	pub fn bytes(&self) -> &[u8] {
		match self {
			Self::Fixed(b, n) => &b[..*n],
			Self::Borrowed(b) => b,
		}
	}
}
pub(super) fn cell<'a>(
	a: &'a dyn Array,
	row: usize,
	t: ProofScalarType,
) -> Result<Option<CellBytes<'a>>, AdapterError> {
	if row >= a.len() {
		return Err(AdapterError::InvalidValue);
	}
	if a.is_null(row) {
		return Ok(None);
	}
	macro_rules! fixed {
		($a:ty) => {{
			let v = a
				.as_any()
				.downcast_ref::<$a>()
				.ok_or(AdapterError::SchemaMismatch)?
				.value(row)
				.to_le_bytes();
			let mut b = [0; 16];
			b[..v.len()].copy_from_slice(&v);
			CellBytes::Fixed(b, v.len())
		}};
	}
	macro_rules! borrowed {
		($a:ty) => {{
			CellBytes::Borrowed(
				a.as_any()
					.downcast_ref::<$a>()
					.ok_or(AdapterError::SchemaMismatch)?
					.value(row)
					.as_ref(),
			)
		}};
	}
	use ProofScalarType::*;
	let v = match t {
		Boolean => {
			let v = a
				.as_any()
				.downcast_ref::<BooleanArray>()
				.ok_or(AdapterError::SchemaMismatch)?
				.value(row);
			let mut b = [0; 16];
			b[0] = u8::from(v);
			CellBytes::Fixed(b, 1)
		}
		Int8 => fixed!(Int8Array),
		Int16 => fixed!(Int16Array),
		Int32 => fixed!(Int32Array),
		Int64 => fixed!(Int64Array),
		UInt8 => fixed!(UInt8Array),
		UInt16 => fixed!(UInt16Array),
		UInt32 => fixed!(UInt32Array),
		UInt64 => fixed!(UInt64Array),
		Float32 => fixed!(Float32Array),
		Float64 => fixed!(Float64Array),
		Date32 => fixed!(Date32Array),
		TimestampMillis | UtcMillis => fixed!(TimestampMillisecondArray),
		UtcMicros => fixed!(TimestampMicrosecondArray),
		UtcNanos => fixed!(TimestampNanosecondArray),
		Decimal128(..) => fixed!(Decimal128Array),
		Utf8 => match a.data_type() {
			DataType::Utf8 => borrowed!(StringArray),
			_ => borrowed!(LargeStringArray),
		},
		Binary => match a.data_type() {
			DataType::Binary => borrowed!(BinaryArray),
			_ => borrowed!(LargeBinaryArray),
		},
	};
	t.validate(v.bytes())?;
	Ok(Some(v))
}
