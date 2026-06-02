use std::collections::HashMap;
use std::fmt;

use crate::catalog::TableRef;
pub use crate::distributed::identity::{QueryId, StageId};
use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};
use wendelon_client::{
	LOGICAL_TYPE_RESERVED_STATIC_MAX_ID, LogicalTypeDefinition, LogicalTypePhysical,
	LogicalTypeRefinementPredicate,
};

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
pub const DURABLE_LOGICAL_TYPE_MIN_ID: u32 = LOGICAL_TYPE_RESERVED_STATIC_MAX_ID + 1;
pub const LOGICAL_TYPE_ID_METADATA_KEY: &str = "nilket.toron.logical_type_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalTypeSpec {
	pub id: LogicalTypeId,
	pub name: String,
	pub physical: DataType,
	pub refinement: Option<LogicalTypeRefinementPredicate>,
}

pub fn seed_logical_type_spec(id: LogicalTypeId) -> Option<LogicalTypeSpec> {
	match id {
		LogicalTypeId::NAKED => Some(LogicalTypeSpec {
			id,
			name: "naked".to_string(),
			physical: DataType::Null,
			refinement: None,
		}),
		CUSTOMER_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "CustomerId".to_string(),
			physical: DataType::Int64,
			refinement: None,
		}),
		PRODUCT_ID_LOGICAL_TYPE => Some(LogicalTypeSpec {
			id,
			name: "ProductId".to_string(),
			physical: DataType::Int64,
			refinement: None,
		}),
		_ => None,
	}
}

pub fn logical_type_spec(id: LogicalTypeId) -> Option<LogicalTypeSpec> {
	LogicalTypeResolver::seed_only().spec(id)
}

pub fn logical_type_spec_by_name(name: &str) -> Option<LogicalTypeSpec> {
	LogicalTypeResolver::seed_only().spec_by_name(name)
}

pub fn seed_logical_type_spec_by_name(name: &str) -> Option<LogicalTypeSpec> {
	[CUSTOMER_ID_LOGICAL_TYPE, PRODUCT_ID_LOGICAL_TYPE]
		.into_iter()
		.find_map(|id| seed_logical_type_spec(id).filter(|spec| spec.name == name))
}

pub fn logical_type_name(id: LogicalTypeId) -> String {
	logical_type_spec(id)
		.map(|spec| spec.name)
		.unwrap_or_else(|| format!("LogicalTypeId({})", id.0))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalTypeRegistry {
	by_id: HashMap<LogicalTypeId, LogicalTypeSpec>,
	by_name: HashMap<String, LogicalTypeId>,
}

impl LogicalTypeRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn from_definitions(
		definitions: impl IntoIterator<Item = LogicalTypeDefinition>,
	) -> Result<Self, LogicalTypeRegistryError> {
		let mut registry = Self::new();
		for definition in definitions {
			registry.register_definition(definition)?;
		}
		Ok(registry)
	}

	pub fn register_definition(
		&mut self,
		definition: LogicalTypeDefinition,
	) -> Result<LogicalTypeSpec, LogicalTypeRegistryError> {
		if definition.logical_type_id < DURABLE_LOGICAL_TYPE_MIN_ID {
			return Err(LogicalTypeRegistryError::ReservedId {
				id: definition.logical_type_id,
			});
		}
		if definition.name.eq_ignore_ascii_case("naked") {
			return Err(LogicalTypeRegistryError::ReservedName {
				name: definition.name,
			});
		}
		let spec = LogicalTypeSpec {
			id: LogicalTypeId(definition.logical_type_id),
			name: definition.name,
			physical: logical_type_physical_to_data_type(definition.physical),
			refinement: definition.refinement,
		};
		if let Some(existing) = self.by_id.get(&spec.id) {
			if existing == &spec {
				return Ok(existing.clone());
			}
			return Err(LogicalTypeRegistryError::DuplicateId { id: spec.id.0 });
		}
		if let Some(existing_id) = self.by_name.get(&spec.name) {
			if *existing_id == spec.id {
				return Ok(spec);
			}
			return Err(LogicalTypeRegistryError::DuplicateName { name: spec.name });
		}
		self.by_name.insert(spec.name.clone(), spec.id);
		self.by_id.insert(spec.id, spec.clone());
		Ok(spec)
	}

	pub fn spec(&self, id: LogicalTypeId) -> Option<LogicalTypeSpec> {
		self.by_id.get(&id).cloned()
	}

	pub fn spec_by_name(&self, name: &str) -> Option<LogicalTypeSpec> {
		self.by_name
			.get(name)
			.and_then(|id| self.by_id.get(id))
			.cloned()
	}

	pub fn specs_sorted(&self) -> Vec<LogicalTypeSpec> {
		let mut specs = self.by_id.values().cloned().collect::<Vec<_>>();
		specs.sort_by(|left, right| {
			left.id
				.0
				.cmp(&right.id.0)
				.then_with(|| left.name.cmp(&right.name))
		});
		specs
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalTypeRegistryError {
	ReservedId { id: u32 },
	ReservedName { name: String },
	DuplicateId { id: u32 },
	DuplicateName { name: String },
}

impl std::fmt::Display for LogicalTypeRegistryError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::ReservedId { id } => write!(
				formatter,
				"logical type id {id} is in the reserved static seed range"
			),
			Self::ReservedName { name } => {
				write!(formatter, "logical type name {name:?} is reserved")
			}
			Self::DuplicateId { id } => {
				write!(
					formatter,
					"logical type id {id} already has a different definition"
				)
			}
			Self::DuplicateName { name } => write!(
				formatter,
				"logical type name {name:?} already has a different definition"
			),
		}
	}
}

impl std::error::Error for LogicalTypeRegistryError {}

#[derive(Debug, Clone, Copy)]
pub struct LogicalTypeResolver<'a> {
	registry: Option<&'a LogicalTypeRegistry>,
	include_seed: bool,
}

impl<'a> LogicalTypeResolver<'a> {
	pub fn seed_only() -> Self {
		Self {
			registry: None,
			include_seed: true,
		}
	}

	pub fn with_registry(registry: &'a LogicalTypeRegistry) -> Self {
		Self {
			registry: Some(registry),
			include_seed: true,
		}
	}

	pub fn durable_only(registry: &'a LogicalTypeRegistry) -> Self {
		Self {
			registry: Some(registry),
			include_seed: false,
		}
	}

	pub fn spec(self, id: LogicalTypeId) -> Option<LogicalTypeSpec> {
		if self.include_seed {
			seed_logical_type_spec(id)
		} else {
			None
		}
		.or_else(|| self.registry.and_then(|registry| registry.spec(id)))
	}

	pub fn spec_by_name(self, name: &str) -> Option<LogicalTypeSpec> {
		if self.include_seed {
			seed_logical_type_spec_by_name(name)
		} else {
			None
		}
		.or_else(|| {
			self.registry
				.and_then(|registry| registry.spec_by_name(name))
		})
	}
}

pub fn logical_type_physical_to_data_type(physical: LogicalTypePhysical) -> DataType {
	match physical {
		LogicalTypePhysical::Int64 => DataType::Int64,
		LogicalTypePhysical::Int32 => DataType::Int32,
		LogicalTypePhysical::Float64 => DataType::Float64,
		LogicalTypePhysical::Float32 => DataType::Float32,
		LogicalTypePhysical::Utf8 => DataType::Utf8,
		LogicalTypePhysical::Boolean => DataType::Boolean,
	}
}

pub fn data_type_to_logical_type_physical(data_type: &DataType) -> Option<LogicalTypePhysical> {
	match data_type {
		DataType::Int64 => Some(LogicalTypePhysical::Int64),
		DataType::Int32 => Some(LogicalTypePhysical::Int32),
		DataType::Float64 => Some(LogicalTypePhysical::Float64),
		DataType::Float32 => Some(LogicalTypePhysical::Float32),
		DataType::Utf8 => Some(LogicalTypePhysical::Utf8),
		DataType::Boolean => Some(LogicalTypePhysical::Boolean),
		_ => None,
	}
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
		logical_type: String,
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
	if let Some(spec) = seed_logical_type_spec(id) {
		if spec.physical != *field.data_type() {
			return Err(LogicalTypeMetadataError::PhysicalMismatch {
				field: field.name().clone(),
				logical_type: spec.name,
				expected: spec.physical,
				actual: field.data_type().clone(),
			});
		}
		return Ok(id);
	}
	if id.0 < DURABLE_LOGICAL_TYPE_MIN_ID {
		return Err(LogicalTypeMetadataError::UnknownId {
			field: field.name().clone(),
			id: id.0,
		});
	}
	Ok(id)
}

pub fn decode_column_logical_type_with_resolver(
	field: &Field,
	resolver: LogicalTypeResolver<'_>,
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
	let Some(spec) = resolver.spec(id) else {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableSchema {
	pub columns: Vec<CreateTableColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableColumn {
	pub name: String,
	pub nullable: bool,
	pub column_type: CreateTableColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreateTableColumnType {
	Physical(DataType),
	LogicalName(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
	Boolean(Option<bool>),
	Int32(Option<i32>),
	Int64(Option<i64>),
	Date32(Option<i32>),
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
			Self::Date32(_) => DataType::Date32,
			Self::Float32(_) => DataType::Float32,
			Self::Float64(_) => DataType::Float64,
			Self::Utf8(_) => DataType::Utf8,
		}
	}
}

impl fmt::Display for ScalarValue {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Boolean(Some(value)) => write!(f, "{value}"),
			Self::Boolean(None) => f.write_str("NULL"),
			Self::Int32(Some(value)) => write!(f, "{value}"),
			Self::Int32(None) => f.write_str("NULL"),
			Self::Int64(Some(value)) => write!(f, "{value}"),
			Self::Int64(None) => f.write_str("NULL"),
			Self::Date32(Some(value)) => f.write_str(&crate::date32::format_date_ymd(*value)),
			Self::Date32(None) => f.write_str("NULL"),
			Self::Float32(Some(value)) => write!(f, "{value}"),
			Self::Float32(None) => f.write_str("NULL"),
			Self::Float64(Some(value)) => write!(f, "{value}"),
			Self::Float64(None) => f.write_str("NULL"),
			Self::Utf8(Some(value)) => f.write_str(value),
			Self::Utf8(None) => f.write_str("NULL"),
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
	UntypedNull,
	LogicalAscription {
		expr: Box<Expr>,
		logical_type: LogicalTypeId,
	},
	SqlLogicalCast {
		expr: Box<Expr>,
		logical_name: String,
		try_cast: bool,
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
	Case {
		when_then: Vec<(Box<Expr>, Box<Expr>)>,
		else_expr: Option<Box<Expr>>,
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
			Self::UntypedNull => false,
			Self::LogicalAscription { expr, .. } => expr.is_pbc_pushdown_eligible(),
			Self::SqlLogicalCast { .. }
			| Self::Cast(_, _)
			| Self::TryCast(_, _)
			| Self::Add(_, _)
			| Self::Sub(_, _)
			| Self::Mul(_, _)
			| Self::Div(_, _)
			| Self::Like { .. }
			| Self::Case { .. }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyIntent {
	Auto,
	Pinned {
		strategy: JoinStrategy,
		source: JoinStrategyPinSource,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinStrategyPinSource {
	Direct,
	SqlHint,
	Rune,
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
	Date32(i32),
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
		columns: Vec<String>,
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
	Take {
		input: Box<LogicalPlan>,
		limit: usize,
	},
	Join {
		left: Box<LogicalPlan>,
		right: Box<LogicalPlan>,
		kind: JoinKind,
		on: Vec<(Expr, Expr)>,
		strategy_intent: JoinStrategyIntent,
	},
	Explain {
		input: Box<LogicalPlan>,
	},
	CreateTable {
		name: TableRef,
		schema: CreateTableSchema,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
		or_replace: bool,
	},
	CreateLogicalType {
		name: String,
		physical: DataType,
		refinement: Option<LogicalTypeRefinementPredicate>,
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
