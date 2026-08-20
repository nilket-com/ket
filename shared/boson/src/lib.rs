//! Shared, dependency-free interchange contracts.
//!
//! KET-0006 owns the versioned canonical representation of an admitted,
//! generated relation. The crate deliberately owns no execution engine.

#![forbid(unsafe_code)]

use std::fmt;

pub const V1_FORMAT_VERSION: u16 = 1;
pub const RANGE_MAX_CARDINALITY: i64 = 1 << 28;

const MAGIC: &[u8; 8] = b"BOSONREL";

const TAG_GENERATED_RANGE: u8 = 0x01;
const TAG_NO_PROJECTION: u8 = 0x02;
const TAG_PROJECTION_LIST: u8 = 0x03;
const TAG_PROJECTION: u8 = 0x04;

const TAG_INT32: u8 = 0x10;
const TAG_INT64: u8 = 0x11;
const TAG_FLOAT64: u8 = 0x12;

const TAG_ORDINAL: u8 = 0x20;
const TAG_INT32_LITERAL: u8 = 0x21;
const TAG_INT64_LITERAL: u8 = 0x22;
const TAG_FLOAT64_LITERAL: u8 = 0x23;
const TAG_CAST: u8 = 0x24;
const TAG_ADD: u8 = 0x25;
const TAG_SUB: u8 = 0x26;
const TAG_MUL: u8 = 0x27;
const TAG_DIV: u8 = 0x28;
const TAG_FLOOR: u8 = 0x29;
const TAG_SEEDED_UNIT_INTERVAL: u8 = 0x2a;

/// The closed numeric type vocabulary of KET-0006 V1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericType {
	Int32,
	Int64,
	Float64,
}

impl NumericType {
	fn tag(self) -> u8 {
		match self {
			Self::Int32 => TAG_INT32,
			Self::Int64 => TAG_INT64,
			Self::Float64 => TAG_FLOAT64,
		}
	}

	fn decode(tag: u8) -> Result<Self, CanonicalExpressionError> {
		match tag {
			TAG_INT32 => Ok(Self::Int32),
			TAG_INT64 => Ok(Self::Int64),
			TAG_FLOAT64 => Ok(Self::Float64),
			other => Err(CanonicalExpressionError::UnknownTag(other)),
		}
	}
}

/// A relation definition accepted by the initial shared format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRelationExpression {
	pub format_version: u16,
	pub relation: GeneratedRange,
}

/// The only V1 relation source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedRange {
	pub count: i64,
	/// None is intentionally distinct from Some(vec![]).
	pub projection: Option<Vec<Projection>>,
}

/// One final, bound output column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Projection {
	pub output_name: String,
	pub expression: ScalarExpression,
}

/// A scalar definition carrying the exact bound result type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalarExpression {
	pub data_type: NumericType,
	pub kind: ScalarKind,
}

/// The closed scalar vocabulary of KET-0006 V1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarKind {
	Ordinal,
	Int32Literal(i32),
	Int64Literal(i64),
	/// IEEE-754 bits, rather than f64, make equality and encoding exact.
	Float64Literal(u64),
	Cast(Box<ScalarExpression>),
	Add(Box<ScalarExpression>, Box<ScalarExpression>),
	Sub(Box<ScalarExpression>, Box<ScalarExpression>),
	Mul(Box<ScalarExpression>, Box<ScalarExpression>),
	Div(Box<ScalarExpression>, Box<ScalarExpression>),
	Floor(Box<ScalarExpression>),
	SeededUnitInterval(Box<ScalarExpression>, Box<ScalarExpression>),
}

/// Strict V1 validation and decoding failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CanonicalExpressionError {
	InvalidFormatVersion(u16),
	InvalidMagic,
	UnexpectedEnd,
	UnknownTag(u8),
	TrailingBytes,
	InvalidUtf8,
	LengthOutOfBounds,
	RangeCountOutOfBounds(i64),
	NonFiniteFloat,
	InvalidScalarType,
	InvalidScalarArity,
}

impl fmt::Display for CanonicalExpressionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(formatter, "{self:?}")
	}
}

impl std::error::Error for CanonicalExpressionError {}

impl CanonicalRelationExpression {
	/// Encode the sole V1 form after validating every public invariant.
	pub fn encode_v1(&self) -> Result<Vec<u8>, CanonicalExpressionError> {
		self.validate_v1()?;
		let mut output = Vec::new();
		output.extend_from_slice(MAGIC);
		write_u16(&mut output, self.format_version);
		encode_relation(&mut output, &self.relation);
		Ok(output)
	}

	/// Decode and validate the sole accepted V1 form.
	pub fn decode_v1(input: &[u8]) -> Result<Self, CanonicalExpressionError> {
		let mut decoder = Decoder::new(input);
		if decoder.take_exact(MAGIC.len())? != MAGIC {
			return Err(CanonicalExpressionError::InvalidMagic);
		}
		let format_version = decoder.read_u16()?;
		if format_version != V1_FORMAT_VERSION {
			return Err(CanonicalExpressionError::InvalidFormatVersion(
				format_version,
			));
		}
		let relation = decoder.decode_relation()?;
		if !decoder.is_empty() {
			return Err(CanonicalExpressionError::TrailingBytes);
		}
		let expression = Self {
			format_version,
			relation,
		};
		expression.validate_v1()?;
		Ok(expression)
	}

	/// Validate the public value before it becomes durable bytes.
	pub fn validate_v1(&self) -> Result<(), CanonicalExpressionError> {
		if self.format_version != V1_FORMAT_VERSION {
			return Err(CanonicalExpressionError::InvalidFormatVersion(
				self.format_version,
			));
		}
		self.relation.validate()
	}
}

impl GeneratedRange {
	fn validate(&self) -> Result<(), CanonicalExpressionError> {
		if !(0..=RANGE_MAX_CARDINALITY).contains(&self.count) {
			return Err(CanonicalExpressionError::RangeCountOutOfBounds(self.count));
		}
		if let Some(projection) = &self.projection {
			validate_length(projection.len())?;
			for entry in projection {
				validate_length(entry.output_name.len())?;
				entry.expression.validate()?;
			}
		}
		Ok(())
	}
}

fn validate_length(length: usize) -> Result<(), CanonicalExpressionError> {
	u32::try_from(length)
		.map(|_| ())
		.map_err(|_| CanonicalExpressionError::LengthOutOfBounds)
}

impl ScalarExpression {
	fn validate(&self) -> Result<(), CanonicalExpressionError> {
		match &self.kind {
			ScalarKind::Ordinal if self.data_type == NumericType::Int64 => Ok(()),
			ScalarKind::Int32Literal(_) if self.data_type == NumericType::Int32 => Ok(()),
			ScalarKind::Int64Literal(_) if self.data_type == NumericType::Int64 => Ok(()),
			ScalarKind::Float64Literal(bits) if self.data_type == NumericType::Float64 => {
				if f64::from_bits(*bits).is_finite() {
					Ok(())
				} else {
					Err(CanonicalExpressionError::NonFiniteFloat)
				}
			}
			ScalarKind::Cast(child) => {
				child.validate()?;
				Ok(())
			}
			ScalarKind::Add(left, right)
			| ScalarKind::Sub(left, right)
			| ScalarKind::Mul(left, right)
			| ScalarKind::Div(left, right) => {
				left.validate()?;
				right.validate()?;
				if left.data_type == self.data_type && right.data_type == self.data_type {
					Ok(())
				} else {
					Err(CanonicalExpressionError::InvalidScalarType)
				}
			}
			ScalarKind::Floor(child) => {
				child.validate()?;
				if child.data_type == self.data_type {
					Ok(())
				} else {
					Err(CanonicalExpressionError::InvalidScalarType)
				}
			}
			ScalarKind::SeededUnitInterval(seed, key) => {
				seed.validate()?;
				key.validate()?;
				if self.data_type == NumericType::Float64
					&& seed.data_type == NumericType::Int64
					&& key.data_type == NumericType::Int64
				{
					Ok(())
				} else {
					Err(CanonicalExpressionError::InvalidScalarType)
				}
			}
			_ => Err(CanonicalExpressionError::InvalidScalarType),
		}
	}
}

fn encode_relation(output: &mut Vec<u8>, relation: &GeneratedRange) {
	output.push(TAG_GENERATED_RANGE);
	write_i64(output, relation.count);
	match &relation.projection {
		None => output.push(TAG_NO_PROJECTION),
		Some(projection) => {
			output.push(TAG_PROJECTION_LIST);
			write_len(output, projection.len());
			for entry in projection {
				output.push(TAG_PROJECTION);
				write_bytes(output, entry.output_name.as_bytes());
				encode_scalar(output, &entry.expression);
			}
		}
	}
}

fn encode_scalar(output: &mut Vec<u8>, expression: &ScalarExpression) {
	match &expression.kind {
		ScalarKind::Ordinal => output.push(TAG_ORDINAL),
		ScalarKind::Int32Literal(_) => output.push(TAG_INT32_LITERAL),
		ScalarKind::Int64Literal(_) => output.push(TAG_INT64_LITERAL),
		ScalarKind::Float64Literal(_) => output.push(TAG_FLOAT64_LITERAL),
		ScalarKind::Cast(_) => output.push(TAG_CAST),
		ScalarKind::Add(_, _) => output.push(TAG_ADD),
		ScalarKind::Sub(_, _) => output.push(TAG_SUB),
		ScalarKind::Mul(_, _) => output.push(TAG_MUL),
		ScalarKind::Div(_, _) => output.push(TAG_DIV),
		ScalarKind::Floor(_) => output.push(TAG_FLOOR),
		ScalarKind::SeededUnitInterval(_, _) => output.push(TAG_SEEDED_UNIT_INTERVAL),
	}
	output.push(expression.data_type.tag());
	match &expression.kind {
		ScalarKind::Ordinal => {}
		ScalarKind::Int32Literal(value) => write_i32(output, *value),
		ScalarKind::Int64Literal(value) => write_i64(output, *value),
		ScalarKind::Float64Literal(bits) => write_u64(output, *bits),
		ScalarKind::Cast(child) | ScalarKind::Floor(child) => encode_scalar(output, child),
		ScalarKind::Add(left, right)
		| ScalarKind::Sub(left, right)
		| ScalarKind::Mul(left, right)
		| ScalarKind::Div(left, right)
		| ScalarKind::SeededUnitInterval(left, right) => {
			encode_scalar(output, left);
			encode_scalar(output, right);
		}
	}
}

struct Decoder<'a> {
	input: &'a [u8],
	cursor: usize,
}

impl<'a> Decoder<'a> {
	fn new(input: &'a [u8]) -> Self {
		Self { input, cursor: 0 }
	}

	fn is_empty(&self) -> bool {
		self.cursor == self.input.len()
	}

	fn remaining(&self) -> usize {
		self.input.len().saturating_sub(self.cursor)
	}

	fn take_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalExpressionError> {
		let end = self
			.cursor
			.checked_add(length)
			.filter(|end| *end <= self.input.len())
			.ok_or(CanonicalExpressionError::UnexpectedEnd)?;
		let bytes = &self.input[self.cursor..end];
		self.cursor = end;
		Ok(bytes)
	}

	fn read_tag(&mut self) -> Result<u8, CanonicalExpressionError> {
		Ok(self.take_exact(1)?[0])
	}

	fn read_u16(&mut self) -> Result<u16, CanonicalExpressionError> {
		Ok(u16::from_be_bytes(
			self.take_exact(2)?.try_into().expect("length checked"),
		))
	}

	fn read_u32(&mut self) -> Result<u32, CanonicalExpressionError> {
		Ok(u32::from_be_bytes(
			self.take_exact(4)?.try_into().expect("length checked"),
		))
	}

	fn read_u64(&mut self) -> Result<u64, CanonicalExpressionError> {
		Ok(u64::from_be_bytes(
			self.take_exact(8)?.try_into().expect("length checked"),
		))
	}

	fn read_i32(&mut self) -> Result<i32, CanonicalExpressionError> {
		Ok(i32::from_be_bytes(
			self.take_exact(4)?.try_into().expect("length checked"),
		))
	}

	fn read_i64(&mut self) -> Result<i64, CanonicalExpressionError> {
		Ok(i64::from_be_bytes(
			self.take_exact(8)?.try_into().expect("length checked"),
		))
	}

	fn read_bytes(&mut self) -> Result<Vec<u8>, CanonicalExpressionError> {
		let length = usize::try_from(self.read_u32()?)
			.map_err(|_| CanonicalExpressionError::LengthOutOfBounds)?;
		if length > self.remaining() {
			return Err(CanonicalExpressionError::LengthOutOfBounds);
		}
		Ok(self.take_exact(length)?.to_vec())
	}

	fn decode_relation(&mut self) -> Result<GeneratedRange, CanonicalExpressionError> {
		if self.read_tag()? != TAG_GENERATED_RANGE {
			return Err(CanonicalExpressionError::UnknownTag(
				self.input[self.cursor.saturating_sub(1)],
			));
		}
		let count = self.read_i64()?;
		let projection = match self.read_tag()? {
			TAG_NO_PROJECTION => None,
			TAG_PROJECTION_LIST => {
				let count = usize::try_from(self.read_u32()?)
					.map_err(|_| CanonicalExpressionError::LengthOutOfBounds)?;
				// Every entry needs at least an entry tag, empty-name length, and
				// the two scalar prefix tags. Refuse before an attacker can force
				// a huge loop over truncated input.
				if count > self.remaining() {
					return Err(CanonicalExpressionError::LengthOutOfBounds);
				}
				let mut entries = Vec::with_capacity(count);
				for _ in 0..count {
					if self.read_tag()? != TAG_PROJECTION {
						return Err(CanonicalExpressionError::UnknownTag(
							self.input[self.cursor.saturating_sub(1)],
						));
					}
					let output_name = String::from_utf8(self.read_bytes()?)
						.map_err(|_| CanonicalExpressionError::InvalidUtf8)?;
					let expression = self.decode_scalar()?;
					entries.push(Projection {
						output_name,
						expression,
					});
				}
				Some(entries)
			}
			other => return Err(CanonicalExpressionError::UnknownTag(other)),
		};
		Ok(GeneratedRange { count, projection })
	}

	fn decode_scalar(&mut self) -> Result<ScalarExpression, CanonicalExpressionError> {
		let tag = self.read_tag()?;
		let data_type = NumericType::decode(self.read_tag()?)?;
		let kind = match tag {
			TAG_ORDINAL => ScalarKind::Ordinal,
			TAG_INT32_LITERAL => ScalarKind::Int32Literal(self.read_i32()?),
			TAG_INT64_LITERAL => ScalarKind::Int64Literal(self.read_i64()?),
			TAG_FLOAT64_LITERAL => ScalarKind::Float64Literal(self.read_u64()?),
			TAG_CAST => ScalarKind::Cast(Box::new(self.decode_scalar()?)),
			TAG_ADD => ScalarKind::Add(
				Box::new(self.decode_scalar()?),
				Box::new(self.decode_scalar()?),
			),
			TAG_SUB => ScalarKind::Sub(
				Box::new(self.decode_scalar()?),
				Box::new(self.decode_scalar()?),
			),
			TAG_MUL => ScalarKind::Mul(
				Box::new(self.decode_scalar()?),
				Box::new(self.decode_scalar()?),
			),
			TAG_DIV => ScalarKind::Div(
				Box::new(self.decode_scalar()?),
				Box::new(self.decode_scalar()?),
			),
			TAG_FLOOR => ScalarKind::Floor(Box::new(self.decode_scalar()?)),
			TAG_SEEDED_UNIT_INTERVAL => ScalarKind::SeededUnitInterval(
				Box::new(self.decode_scalar()?),
				Box::new(self.decode_scalar()?),
			),
			other => return Err(CanonicalExpressionError::UnknownTag(other)),
		};
		Ok(ScalarExpression { data_type, kind })
	}
}

fn write_len(output: &mut Vec<u8>, length: usize) {
	write_u32(
		output,
		u32::try_from(length).expect("KET-0006 V1 length exceeds u32"),
	);
}

fn write_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
	write_len(output, bytes.len());
	output.extend_from_slice(bytes);
}

fn write_u16(output: &mut Vec<u8>, value: u16) {
	output.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
	output.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
	output.extend_from_slice(&value.to_be_bytes());
}

fn write_i32(output: &mut Vec<u8>, value: i32) {
	output.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
	output.extend_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ordinal() -> ScalarExpression {
		ScalarExpression {
			data_type: NumericType::Int64,
			kind: ScalarKind::Ordinal,
		}
	}

	#[test]
	fn bare_range_round_trips_with_its_absent_projection() {
		let expression = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 3,
				projection: None,
			},
		};
		let bytes = expression.encode_v1().expect("encode");
		assert_eq!(
			bytes,
			[
				b'B',
				b'O',
				b'S',
				b'O',
				b'N',
				b'R',
				b'E',
				b'L',
				0,
				1,
				TAG_GENERATED_RANGE,
				0,
				0,
				0,
				0,
				0,
				0,
				0,
				3,
				TAG_NO_PROJECTION,
			]
		);
		assert_eq!(
			CanonicalRelationExpression::decode_v1(&bytes),
			Ok(expression)
		);
	}

	#[test]
	fn present_empty_projection_is_distinct_from_absent() {
		let expression = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 0,
				projection: Some(Vec::new()),
			},
		};
		let bytes = expression.encode_v1().expect("encode");
		assert_eq!(bytes.get(19), Some(&TAG_PROJECTION_LIST));
		assert_eq!(
			CanonicalRelationExpression::decode_v1(&bytes),
			Ok(expression)
		);
	}

	#[test]
	fn cast_is_one_child_and_identity_is_preserved() {
		let expression = ScalarExpression {
			data_type: NumericType::Int64,
			kind: ScalarKind::Cast(Box::new(ordinal())),
		};
		let relation = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 1,
				projection: Some(vec![Projection {
					output_name: "same".to_owned(),
					expression,
				}]),
			},
		};
		let bytes = relation.encode_v1().expect("encode");
		assert!(
			bytes
				.windows(3)
				.any(|window| window == [TAG_CAST, TAG_INT64, TAG_ORDINAL])
		);
		assert_eq!(CanonicalRelationExpression::decode_v1(&bytes), Ok(relation));
	}

	#[test]
	fn seeded_arithmetic_tree_round_trips() {
		let seeded = ScalarExpression {
			data_type: NumericType::Float64,
			kind: ScalarKind::SeededUnitInterval(
				Box::new(ScalarExpression {
					data_type: NumericType::Int64,
					kind: ScalarKind::Int64Literal(7),
				}),
				Box::new(ordinal()),
			),
		};
		let expression = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 9,
				projection: Some(vec![Projection {
					output_name: "value".to_owned(),
					expression: ScalarExpression {
						data_type: NumericType::Float64,
						kind: ScalarKind::Floor(Box::new(seeded)),
					},
				}]),
			},
		};
		let bytes = expression.encode_v1().expect("encode");
		assert_eq!(
			CanonicalRelationExpression::decode_v1(&bytes)
				.expect("decode")
				.encode_v1()
				.expect("reencode"),
			bytes
		);
	}

	#[test]
	fn every_v1_scalar_tag_has_one_round_trip_form() {
		let int32 = ScalarExpression {
			data_type: NumericType::Int32,
			kind: ScalarKind::Int32Literal(-7),
		};
		let int64 = ScalarExpression {
			data_type: NumericType::Int64,
			kind: ScalarKind::Int64Literal(-9),
		};
		let float = ScalarExpression {
			data_type: NumericType::Float64,
			kind: ScalarKind::Float64Literal(0.5_f64.to_bits()),
		};
		let cases = [
			(TAG_ORDINAL, ordinal()),
			(TAG_INT32_LITERAL, int32.clone()),
			(TAG_INT64_LITERAL, int64.clone()),
			(TAG_FLOAT64_LITERAL, float.clone()),
			(
				TAG_CAST,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Cast(Box::new(int32.clone())),
				},
			),
			(
				TAG_ADD,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Add(Box::new(float.clone()), Box::new(float.clone())),
				},
			),
			(
				TAG_SUB,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Sub(Box::new(float.clone()), Box::new(float.clone())),
				},
			),
			(
				TAG_MUL,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Mul(Box::new(float.clone()), Box::new(float.clone())),
				},
			),
			(
				TAG_DIV,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Div(Box::new(float.clone()), Box::new(float.clone())),
				},
			),
			(
				TAG_FLOOR,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::Floor(Box::new(float.clone())),
				},
			),
			(
				TAG_SEEDED_UNIT_INTERVAL,
				ScalarExpression {
					data_type: NumericType::Float64,
					kind: ScalarKind::SeededUnitInterval(
						Box::new(int64.clone()),
						Box::new(ordinal()),
					),
				},
			),
		];

		for (tag, scalar) in cases {
			let mut bytes = Vec::new();
			encode_scalar(&mut bytes, &scalar);
			assert_eq!(bytes[0], tag);
			let mut decoder = Decoder::new(&bytes);
			assert_eq!(decoder.decode_scalar(), Ok(scalar));
			assert!(decoder.is_empty());
		}
	}

	#[test]
	fn validation_refuses_invalid_range_and_scalar_shapes() {
		let invalid_range = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: -1,
				projection: None,
			},
		};
		assert_eq!(
			invalid_range.encode_v1(),
			Err(CanonicalExpressionError::RangeCountOutOfBounds(-1))
		);

		let mismatched_add = ScalarExpression {
			data_type: NumericType::Int64,
			kind: ScalarKind::Add(
				Box::new(ordinal()),
				Box::new(ScalarExpression {
					data_type: NumericType::Int32,
					kind: ScalarKind::Int32Literal(1),
				}),
			),
		};
		assert_eq!(
			mismatched_add.validate(),
			Err(CanonicalExpressionError::InvalidScalarType)
		);
	}

	#[test]
	fn decoder_refuses_unknown_tag_trailing_bytes_and_non_finite_float() {
		let mut unknown = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 0,
				projection: None,
			},
		}
		.encode_v1()
		.expect("encode");
		unknown[10] = 0xff;
		assert_eq!(
			CanonicalRelationExpression::decode_v1(&unknown),
			Err(CanonicalExpressionError::UnknownTag(0xff))
		);

		let mut trailing = CanonicalRelationExpression {
			format_version: V1_FORMAT_VERSION,
			relation: GeneratedRange {
				count: 0,
				projection: None,
			},
		}
		.encode_v1()
		.expect("encode");
		trailing.push(0);
		assert_eq!(
			CanonicalRelationExpression::decode_v1(&trailing),
			Err(CanonicalExpressionError::TrailingBytes)
		);

		let non_finite = ScalarExpression {
			data_type: NumericType::Float64,
			kind: ScalarKind::Float64Literal(f64::NAN.to_bits()),
		};
		assert_eq!(
			non_finite.validate(),
			Err(CanonicalExpressionError::NonFiniteFloat)
		);
	}
}
