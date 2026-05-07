use crate::schema::pbc::ColumnPredicateLiteralV1;
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
	literal: Option<&ColumnPredicateLiteralV1>,
	data_type: &DataType,
) -> Option<LiteralCoercion> {
	let literal = match literal {
		Some(ColumnPredicateLiteralV1::List(values)) => {
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
	literal: &ColumnPredicateLiteralV1,
	data_type: &DataType,
) -> Option<LiteralValue> {
	match (literal, data_type) {
		(ColumnPredicateLiteralV1::Int(value), DataType::Int8)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Int16)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Int32)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Int64)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Date32)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Date64)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Time32(_))
		| (ColumnPredicateLiteralV1::Int(value), DataType::Time64(_))
		| (ColumnPredicateLiteralV1::Int(value), DataType::Duration(_)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteralV1::Ts(value), DataType::Timestamp(_, _)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteralV1::Int(value), DataType::Timestamp(_, _)) => {
			Some(LiteralValue::Signed(i128::from(*value)))
		}
		(ColumnPredicateLiteralV1::Int(value), DataType::UInt8)
		| (ColumnPredicateLiteralV1::Int(value), DataType::UInt16)
		| (ColumnPredicateLiteralV1::Int(value), DataType::UInt32)
		| (ColumnPredicateLiteralV1::Int(value), DataType::UInt64) => {
			u128::try_from(*value).ok().map(LiteralValue::Unsigned)
		}
		(ColumnPredicateLiteralV1::Int(value), DataType::Boolean) => match *value {
			0 => Some(LiteralValue::Boolean(false)),
			1 => Some(LiteralValue::Boolean(true)),
			_ => None,
		},
		(ColumnPredicateLiteralV1::Float(value), DataType::Float32)
		| (ColumnPredicateLiteralV1::Float(value), DataType::Float64) => {
			Some(LiteralValue::Float(*value))
		}
		(ColumnPredicateLiteralV1::Int(value), DataType::Float32)
		| (ColumnPredicateLiteralV1::Int(value), DataType::Float64) => {
			Some(LiteralValue::Float(*value as f64))
		}
		(ColumnPredicateLiteralV1::Str(value), DataType::Utf8)
		| (ColumnPredicateLiteralV1::Str(value), DataType::LargeUtf8)
		| (ColumnPredicateLiteralV1::Str(value), DataType::Utf8View)
		| (ColumnPredicateLiteralV1::Str(value), DataType::Binary)
		| (ColumnPredicateLiteralV1::Str(value), DataType::LargeBinary)
		| (ColumnPredicateLiteralV1::Str(value), DataType::BinaryView) => {
			Some(LiteralValue::Bytes(value.clone()))
		}
		(ColumnPredicateLiteralV1::Str(value), DataType::FixedSizeBinary(_)) => {
			Some(LiteralValue::Bytes(value.clone()))
		}
		_ => None,
	}
}
