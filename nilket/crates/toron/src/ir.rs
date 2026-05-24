use std::collections::HashMap;

use crate::catalog::TableRef;
pub use crate::distributed::identity::{QueryId, StageId};
use arrow::datatypes::{DataType, Field, SchemaRef};
use serde::{Deserialize, Serialize};

#[path = "ir/execution.rs"]
pub mod execution;

pub use execution::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogicalTypeId(pub u32);

impl LogicalTypeId {
	pub const NAKED: Self = Self(0);

	pub fn is_naked(self) -> bool {
		self == Self::NAKED
	}
}

pub const CUSTOMER_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(1);
pub const PRODUCT_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(2);
pub const LOGICAL_TYPE_ID_METADATA_KEY: &str = "nilket.toron.logical_type_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTypeSpec {
	pub id: LogicalTypeId,
	pub name: &'static str,
	pub physical: DataType,
}

pub fn logical_type_spec(id: LogicalTypeId) -> Option<LogicalTypeSpec> {
	match id {
		LogicalTypeId::NAKED => Some(LogicalTypeSpec {
			id,
			name: "naked",
			physical: DataType::Null,
		}),
		CUSTOMER_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "CustomerId",
			physical: DataType::Int64,
		}),
		PRODUCT_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "ProductId",
			physical: DataType::Int64,
		}),
		_ => None,
	}
}

pub fn logical_type_name(id: LogicalTypeId) -> String {
	logical_type_spec(id)
		.map(|spec| spec.name.to_string())
		.unwrap_or_else(|| format!("LogicalTypeId({})", id.0))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalTypeMetadataError {
	InvalidDecimal {
		field: String,
		value_preview: String,
	},
	UnknownId {
		field: String,
		id: u32,
	},
	PhysicalMismatch {
		field: String,
		logical_type: &'static str,
		expected: DataType,
		actual: DataType,
	},
}

impl std::fmt::Display for LogicalTypeMetadataError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::InvalidDecimal {
				field,
				value_preview,
			} => write!(
				formatter,
				"logical type metadata for column {field} must be a decimal u32, got {value_preview:?}"
			),
			Self::UnknownId { field, id } => write!(
				formatter,
				"logical type metadata for column {field} references unknown id {id}"
			),
			Self::PhysicalMismatch {
				field,
				logical_type,
				expected,
				actual,
			} => write!(
				formatter,
				"logical type metadata for column {field} declares {logical_type}({expected:?}) on {actual:?}"
			),
		}
	}
}

impl std::error::Error for LogicalTypeMetadataError {}

pub fn decode_column_logical_type(
	field: &Field,
) -> Result<LogicalTypeId, LogicalTypeMetadataError> {
	let Some(raw) = field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY) else {
		return Ok(LogicalTypeId::NAKED);
	};
	let id = raw.parse::<u32>().map(LogicalTypeId).map_err(|_| {
		LogicalTypeMetadataError::InvalidDecimal {
			field: field.name().clone(),
			value_preview: metadata_value_preview(raw),
		}
	})?;
	if id.is_naked() {
		return Ok(id);
	}
	let Some(spec) = logical_type_spec(id) else {
		return Err(LogicalTypeMetadataError::UnknownId {
			field: field.name().clone(),
			id: id.0,
		});
	};
	if spec.physical != *field.data_type() {
		return Err(LogicalTypeMetadataError::PhysicalMismatch {
			field: field.name().clone(),
			logical_type: spec.name,
			expected: spec.physical,
			actual: field.data_type().clone(),
		});
	}
	Ok(id)
}

pub fn field_with_logical_type_metadata(
	name: impl Into<String>,
	data_type: DataType,
	nullable: bool,
	logical_type: LogicalTypeId,
) -> Field {
	let field = Field::new(name, data_type, nullable);
	if logical_type.is_naked() {
		return field;
	}
	field.with_metadata(HashMap::from([(
		LOGICAL_TYPE_ID_METADATA_KEY.to_string(),
		logical_type.0.to_string(),
	)]))
}

fn metadata_value_preview(value: &str) -> String {
	const MAX_PREVIEW_CHARS: usize = 64;
	let mut chars = value.chars();
	let mut preview = chars.by_ref().take(MAX_PREVIEW_CHARS).collect::<String>();
	if chars.next().is_some() {
		preview.push_str("...");
	}
	preview
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MorselId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryProfileHint {
	PointRead,
	Scan,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CreateTableRowIdentity {
	AppendOnly,
	PrimaryKey { column: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
	Boolean(Option<bool>),
	Int32(Option<i32>),
	Int64(Option<i64>),
	Float32(Option<f32>),
	Float64(Option<f64>),
	Utf8(Option<String>),
}

impl ScalarValue {
	pub fn data_type(&self) -> DataType {
		match self {
			Self::Boolean(_) => DataType::Boolean,
			Self::Int32(_) => DataType::Int32,
			Self::Int64(_) => DataType::Int64,
			Self::Float32(_) => DataType::Float32,
			Self::Float64(_) => DataType::Float64,
			Self::Utf8(_) => DataType::Utf8,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
	pub qualifier: Option<TableRef>,
	pub name: ColumnName,
}

impl ColumnRef {
	pub fn unqualified(name: impl Into<String>) -> Self {
		Self {
			qualifier: None,
			name: ColumnName(name.into()),
		}
	}

	pub fn qualified(qualifier: TableRef, name: impl Into<String>) -> Self {
		Self {
			qualifier: Some(qualifier),
			name: ColumnName(name.into()),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
	Column(ColumnRef),
	Literal(ScalarValue),
	LogicalAscription {
		expr: Box<Expr>,
		logical_type: LogicalTypeId,
	},
	Cast(Box<Expr>, DataType),
	TryCast(Box<Expr>, DataType),
	Add(Box<Expr>, Box<Expr>),
	Sub(Box<Expr>, Box<Expr>),
	Mul(Box<Expr>, Box<Expr>),
	Div(Box<Expr>, Box<Expr>),
	Eq(Box<Expr>, Box<Expr>),
	Neq(Box<Expr>, Box<Expr>),
	Lt(Box<Expr>, Box<Expr>),
	Lte(Box<Expr>, Box<Expr>),
	Gt(Box<Expr>, Box<Expr>),
	Gte(Box<Expr>, Box<Expr>),
	And(Box<Expr>, Box<Expr>),
	Or(Box<Expr>, Box<Expr>),
	Not(Box<Expr>),
	InList {
		expr: Box<Expr>,
		list: Vec<Expr>,
		negated: bool,
	},
	Like {
		expr: Box<Expr>,
		pattern: String,
		negated: bool,
		case_insensitive: bool,
	},
	IsNull(Box<Expr>),
	IsNotNull(Box<Expr>),
	TimeBucket(std::time::Duration, Box<Expr>),
}

impl Expr {
	pub fn column(name: impl Into<String>) -> Self {
		Self::Column(ColumnRef::unqualified(name))
	}

	pub fn qualified_column(qualifier: TableRef, name: impl Into<String>) -> Self {
		Self::Column(ColumnRef::qualified(qualifier, name))
	}

	pub fn is_deterministic(&self) -> bool {
		true
	}

	pub fn is_pbc_pushdown_eligible(&self) -> bool {
		match self {
			Self::Column(_) | Self::Literal(_) => true,
			Self::LogicalAscription { expr, .. } => expr.is_pbc_pushdown_eligible(),
			Self::Cast(_, _)
			| Self::TryCast(_, _)
			| Self::Add(_, _)
			| Self::Sub(_, _)
			| Self::Mul(_, _)
			| Self::Div(_, _)
			| Self::Like { .. }
			| Self::TimeBucket(_, _) => false,
			Self::Eq(left, right)
			| Self::Neq(left, right)
			| Self::Lt(left, right)
			| Self::Lte(left, right)
			| Self::Gt(left, right)
			| Self::Gte(left, right)
			| Self::And(left, right)
			| Self::Or(left, right) => left.is_pbc_pushdown_eligible() && right.is_pbc_pushdown_eligible(),
			Self::Not(inner) | Self::IsNull(inner) | Self::IsNotNull(inner) => {
				inner.is_pbc_pushdown_eligible()
			}
			Self::InList { expr, list, .. } => {
				expr.is_pbc_pushdown_eligible() && list.iter().all(Self::is_pbc_pushdown_eligible)
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
	CountAll,
	Count,
	Sum,
	Min,
	Max,
	Avg,
	LinearRegress,
	ApproxQuantile(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
	Inner,
	LeftOuter,
	RightOuter,
	FullOuter,
	LeftSemi,
	RightSemi,
	LeftAnti,
	RightAnti,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategy {
	HashPartitioned,
	Broadcast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortExpr {
	pub expr: Expr,
	pub descending: bool,
	pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertLiteral {
	Null,
	Boolean(bool),
	Int64(i64),
	UInt64(u64),
	Float64(f64),
	Utf8(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReturningProjection {
	Star,
	Exprs {
		exprs: Vec<Expr>,
		output_names: Vec<Option<String>>,
	},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
	Scan {
		table: TableId,
		projection: Option<Vec<String>>,
	},
	ScanNamed {
		table: TableRef,
		projection: Option<Vec<String>>,
	},
	Alias {
		input: Box<LogicalPlan>,
		alias: TableRef,
	},
	Filter {
		input: Box<LogicalPlan>,
		predicate: Expr,
	},
	Project {
		input: Box<LogicalPlan>,
		exprs: Vec<Expr>,
		output_names: Vec<Option<String>>,
	},
	Aggregate {
		input: Box<LogicalPlan>,
		group_expr: Vec<Expr>,
		aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>,
	},
	Exchange {
		input: Box<LogicalPlan>,
		partition_count: u32,
		key: Vec<Expr>,
	},
	Sort {
		input: Box<LogicalPlan>,
		order_by: Vec<SortExpr>,
		limit: Option<usize>,
		offset: usize,
	},
	Join {
		left: Box<LogicalPlan>,
		right: Box<LogicalPlan>,
		kind: JoinKind,
		on: Vec<(Expr, Expr)>,
		strategy: JoinStrategy,
	},
	CreateTable {
		name: TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
		or_replace: bool,
	},
	CreateTableAsSelect {
		table: TableRef,
		if_not_exists: bool,
		or_replace: bool,
		source: Box<LogicalPlan>,
	},
	DropTable {
		name: TableRef,
		if_exists: bool,
	},
	Delete {
		table: TableRef,
		predicate: Option<Expr>,
		returning: Option<ReturningProjection>,
	},
	Update {
		table: TableRef,
		assignments: Vec<(String, Expr)>,
		predicate: Option<Expr>,
		returning: Option<ReturningProjection>,
	},
	Insert {
		table: TableRef,
		columns: Option<Vec<String>>,
		rows: Vec<Vec<InsertLiteral>>,
		returning: Option<ReturningProjection>,
	},
	InsertSelect {
		table: TableRef,
		columns: Option<Vec<String>>,
		source: Box<LogicalPlan>,
		returning: Option<ReturningProjection>,
	},
}

pub fn project_plan(
	input: LogicalPlan,
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
) -> LogicalPlan {
	debug_assert_eq!(
		exprs.len(),
		output_names.len(),
		"Project exprs/output_names length mismatch"
	);
	LogicalPlan::Project {
		input: Box::new(input),
		exprs,
		output_names,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn try_cast_is_not_pbc_pushdown_eligible() {
		let try_cast = Expr::TryCast(Box::new(Expr::column("amount")), DataType::Int64);

		assert!(!try_cast.is_pbc_pushdown_eligible());
	}
}
