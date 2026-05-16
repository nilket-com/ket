use crate::catalog::TableRef;
pub use crate::distributed::identity::{QueryId, StageId};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[path = "ir/execution.rs"]
pub mod execution;

pub use execution::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MorselId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryProfileHint {
	PointRead,
	Scan,
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
	Cast(Box<Expr>, DataType),
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
			Self::Cast(_, _)
			| Self::Add(_, _)
			| Self::Sub(_, _)
			| Self::Mul(_, _)
			| Self::Div(_, _)
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
