use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StageId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MorselId(pub u32);

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Column(String),
    Literal(ScalarValue),
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
    pub fn is_deterministic(&self) -> bool {
        true // V0 expressions are all deterministic
    }

    pub fn is_pbc_pushdown_eligible(&self) -> bool {
        match self {
            Self::TimeBucket(_, _) => false, // New kernels not yet in PBC
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateFunction {
    CountAll,
    Sum,
    LinearRegress,
    ApproxQuantile(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan {
        table: TableId,
        projection: Option<Vec<String>>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
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
}
