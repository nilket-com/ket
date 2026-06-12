use crate::schema::pbc::ColumnPredicateLiteral;
use arrow::datatypes::DataType;

#[derive(Clone, Debug, PartialEq)]
pub struct LiteralCoercion {
	pub data_type: DataType,
	pub literal: Option<LiteralValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
	Signed(i128),
	Unsigned(u128),
	Float(f64),
	Boolean(bool),
	Bytes(Vec<u8>),
	List(Vec<LiteralValue>),
	Null,
}

pub fn coerce_literal(
	literal: Option<&ColumnPredicateLiteral>,
	data_type: &DataType,
) -> Option<LiteralCoercion> {
	let literal = match literal {
		Some(ColumnPredicateLiteral::List(values)) => {
			let values = values
				.iter()
				.map(|value| coerce_scalar_literal(value, data_type))
				.collect::<Option<Vec<_>>>()?;
			Some(LiteralValue::List(values))
		}
		Some(value) => Some(coerce_scalar_literal(value, data_type)?),
		None => None,
	};
	Some(LiteralCoercion {
		data_type: data_type.clone(),
		literal,
	})
}

fn coerce_scalar_literal(
	literal: &ColumnPredicateLiteral,
	data_type: &DataType,
) -> Option<LiteralValue> {
	match (literal, data_type) {
		(ColumnPredicateLiteral::Int(value), DataType::Int8)
		| (ColumnPredicateLiteral::Int(value), DataType::Int16)
		| (ColumnPredicateLiteral::Int(value), DataType::Int32)
		| (ColumnPredicateLiteral::Int(value), DataType::Int64)
		| (ColumnPredicateLiteral::Int(value), DataType::Date32)
		| (ColumnPredicateLiteral::Int(value), DataType::Date64)
		| (ColumnPredicateLiteral::Int(value), DataType::Time32(_))
		| (ColumnPredicateLiteral::Int(value), DataType::Time64(_))
		| (ColumnPredicateLiteral::Int(value), DataType::Duration(_)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteral::Ts(value), DataType::Timestamp(_, _)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteral::Int(value), DataType::Timestamp(_, _)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteral::Int(value), DataType::UInt8)
		| (ColumnPredicateLiteral::Int(value), DataType::UInt16)
		| (ColumnPredicateLiteral::Int(value), DataType::UInt32)
		| (ColumnPredicateLiteral::Int(value), DataType::UInt64) => {
			u128::try_from(*value).ok().map(LiteralValue::Unsigned)
		}
		(ColumnPredicateLiteral::Int(value), DataType::Boolean) => match *value {
			0 => Some(LiteralValue::Boolean(false)),
			1 => Some(LiteralValue::Boolean(true)),
			_ => None,
		},
		(ColumnPredicateLiteral::Float(value), DataType::Float32)
		| (ColumnPredicateLiteral::Float(value), DataType::Float64) => Some(LiteralValue::Float(*value)),
		(ColumnPredicateLiteral::Int(value), DataType::Float32)
		| (ColumnPredicateLiteral::Int(value), DataType::Float64) => {
			Some(LiteralValue::Float(*value as f64))
		}
		(ColumnPredicateLiteral::Str(value), DataType::Utf8)
		| (ColumnPredicateLiteral::Str(value), DataType::LargeUtf8)
		| (ColumnPredicateLiteral::Str(value), DataType::Utf8View)
		| (ColumnPredicateLiteral::Str(value), DataType::Binary)
		| (ColumnPredicateLiteral::Str(value), DataType::LargeBinary)
		| (ColumnPredicateLiteral::Str(value), DataType::BinaryView) => {
			Some(LiteralValue::Bytes(value.clone()))
		}
		(ColumnPredicateLiteral::Str(value), DataType::FixedSizeBinary(_)) => {
			Some(LiteralValue::Bytes(value.clone()))
		}
		_ => None,
	}
}
