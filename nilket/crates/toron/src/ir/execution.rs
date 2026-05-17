use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::catalog::TableRef;
pub use crate::distributed::identity::QueryId;
use crate::ir::{AggregateFunction, JoinKind, JoinStrategy, ScalarValue};
use crate::ir::{QueryProfileHint, TableId};
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnOrdinal(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PbcProgram(pub Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u128);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedColumn {
	pub name: ColumnName,
	pub ordinal: ColumnOrdinal,
	pub data_type: DataType,
	pub nullable: bool,
	pub pbc_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionExpr {
	Column(ResolvedColumn),
	Literal(ScalarValue),
	Cast(Box<ExecutionExpr>, DataType),
	TryCast(Box<ExecutionExpr>, DataType),
	Add(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Sub(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Mul(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Div(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Eq(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Neq(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Lt(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Lte(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Gt(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Gte(Box<ExecutionExpr>, Box<ExecutionExpr>),
	And(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Or(Box<ExecutionExpr>, Box<ExecutionExpr>),
	Not(Box<ExecutionExpr>),
	IsNull(Box<ExecutionExpr>),
	IsNotNull(Box<ExecutionExpr>),
	TimeBucket(std::time::Duration, Box<ExecutionExpr>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateCall {
	pub function: AggregateFunction,
	pub args: Vec<ExecutionExpr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterDisposition {
	FullyPushed,
	EvaluateInToron,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSortKey {
	pub ordinal: ColumnOrdinal,
	pub descending: bool,
	pub nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionPlan {
	Scan {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		projection: Option<Vec<ColumnOrdinal>>,
		pbc_program: Option<PbcProgram>,
		profile_hint: QueryProfileHint,
		primary_key_columns: Vec<ColumnOrdinal>,
	},
	Filter {
		input: Box<ExecutionPlan>,
		predicate: ExecutionExpr,
		disposition: FilterDisposition,
	},
	Project {
		input: Box<ExecutionPlan>,
		exprs: Vec<ExecutionExpr>,
		output_schema: SchemaRef,
	},
	Aggregate {
		input: Box<ExecutionPlan>,
		group_expr: Vec<ExecutionExpr>,
		aggr_expr: Vec<AggregateCall>,
		output_schema: SchemaRef,
	},
	Exchange {
		input: Box<ExecutionPlan>,
		partition_count: u32,
		key: Vec<ExecutionExpr>,
	},
	Sort {
		input: Box<ExecutionPlan>,
		order_by: Vec<ExecutionSortKey>,
		limit: Option<usize>,
		offset: usize,
		output_schema: SchemaRef,
	},
	Join {
		left: Box<ExecutionPlan>,
		right: Box<ExecutionPlan>,
		kind: JoinKind,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		strategy: JoinStrategy,
		output_schema: SchemaRef,
	},
	CreateTable {
		name: TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
	},
}

impl ExecutionPlan {
	pub fn schema(&self) -> SchemaRef {
		match self {
			Self::Scan { schema, .. } => schema.clone(),
			Self::Filter { input, .. } => input.schema(),
			Self::Project { output_schema, .. } => output_schema.clone(),
			Self::Aggregate { output_schema, .. } => output_schema.clone(),
			Self::Exchange { input, .. } => input.schema(),
			Self::Sort { output_schema, .. } => output_schema.clone(),
			Self::Join { output_schema, .. } => output_schema.clone(),
			Self::CreateTable { .. } => Arc::new(Schema::empty()),
		}
	}
}

impl ExecutionExpr {
	pub fn data_type(&self) -> DataType {
		match self {
			Self::Column(column) => column.data_type.clone(),
			Self::Literal(value) => value.data_type(),
			Self::Cast(_, target) | Self::TryCast(_, target) => target.clone(),
			Self::Add(left, right)
			| Self::Sub(left, right)
			| Self::Mul(left, right)
			| Self::Div(left, right) => {
				debug_assert_eq!(left.data_type(), right.data_type());
				left.data_type()
			}
			Self::Eq(_, _)
			| Self::Neq(_, _)
			| Self::Lt(_, _)
			| Self::Lte(_, _)
			| Self::Gt(_, _)
			| Self::Gte(_, _)
			| Self::And(_, _)
			| Self::Or(_, _)
			| Self::Not(_)
			| Self::IsNull(_)
			| Self::IsNotNull(_) => DataType::Boolean,
			Self::TimeBucket(_, _) => DataType::Int64,
		}
	}

	pub fn nullable(&self) -> bool {
		match self {
			Self::Column(column) => column.nullable,
			Self::Literal(value) => match value {
				ScalarValue::Boolean(value) => value.is_none(),
				ScalarValue::Int32(value) => value.is_none(),
				ScalarValue::Int64(value) => value.is_none(),
				ScalarValue::Float32(value) => value.is_none(),
				ScalarValue::Float64(value) => value.is_none(),
				ScalarValue::Utf8(value) => value.is_none(),
			},
			Self::IsNull(_) | Self::IsNotNull(_) => false,
			Self::Cast(inner, _) => inner.nullable(),
			Self::TryCast(_, _) => true,
			Self::Add(left, right) | Self::Sub(left, right) | Self::Mul(left, right) => {
				left.nullable() || right.nullable()
			}
			Self::Div(left, right) => {
				left.nullable() || right.nullable() || is_integer_type(&left.data_type())
			}
			Self::Eq(left, right)
			| Self::Neq(left, right)
			| Self::Lt(left, right)
			| Self::Lte(left, right)
			| Self::Gt(left, right)
			| Self::Gte(left, right)
			| Self::And(left, right)
			| Self::Or(left, right) => left.nullable() || right.nullable(),
			Self::Not(inner) | Self::TimeBucket(_, inner) => inner.nullable(),
		}
	}

	pub fn pbc_eligible(&self) -> bool {
		match self {
			Self::Column(column) => column.pbc_eligible,
			Self::Literal(_) => true,
			Self::Cast(_, _)
			| Self::TryCast(_, _)
			| Self::Add(_, _)
			| Self::Sub(_, _)
			| Self::Mul(_, _)
			| Self::Div(_, _) => false,
			Self::Eq(left, right)
			| Self::Neq(left, right)
			| Self::Lt(left, right)
			| Self::Lte(left, right)
			| Self::Gt(left, right)
			| Self::Gte(left, right)
			| Self::And(left, right)
			| Self::Or(left, right) => left.pbc_eligible() && right.pbc_eligible(),
			Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => inner.pbc_eligible(),
			Self::TimeBucket(_, _) => false,
		}
	}
}

pub fn arithmetic_output_type(left: &DataType, right: &DataType) -> Option<DataType> {
	use DataType::*;

	match (left, right) {
		(Int32, Int32) => Some(Int32),
		(Int32, Int64) | (Int64, Int32) | (Int64, Int64) => Some(Int64),
		(Float32, Float32) => Some(Float32),
		(Int32 | Int64, Float32) | (Float32, Int32 | Int64) => Some(Float64),
		(Float64, Int32 | Int64 | Float32 | Float64) | (Int32 | Int64 | Float32, Float64) => {
			Some(Float64)
		}
		_ => None,
	}
}

pub fn is_integer_type(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Int32 | DataType::Int64)
}

pub fn schema_for_exprs(exprs: &[ExecutionExpr]) -> SchemaRef {
	let fields = exprs
		.iter()
		.enumerate()
		.map(|(idx, expr)| {
			let name = match expr {
				ExecutionExpr::Column(column) => column.name.0.clone(),
				_ => format!("col_{idx}"),
			};
			arrow::datatypes::Field::new(name, expr.data_type(), expr.nullable())
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn non_nullable_pbc_column() -> ExecutionExpr {
		ExecutionExpr::Column(ResolvedColumn {
			name: ColumnName("amount".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int32,
			nullable: false,
			pbc_eligible: true,
		})
	}

	#[test]
	fn try_cast_execution_nullable_is_always_true() {
		let try_cast = ExecutionExpr::TryCast(Box::new(non_nullable_pbc_column()), DataType::Int32);

		assert!(try_cast.nullable());
	}

	#[test]
	fn try_cast_execution_pbc_eligible_is_false() {
		let try_cast = ExecutionExpr::TryCast(Box::new(non_nullable_pbc_column()), DataType::Int64);

		assert!(!try_cast.pbc_eligible());
	}
}
