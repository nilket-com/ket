use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::catalog::{
	ColumnGroupTopKFact, ColumnHeavyHitterFact, ColumnRangeDistributionFact, DistinctCountFact,
	RangeFact, TableRef,
};
pub use crate::distributed::identity::QueryId;
use crate::ir::{
	AggregateFunction, CreateTableRowIdentity, JoinKind, JoinStrategy, LogicalTypeId, ScalarValue,
	TemporalExtractField, TemporalTruncField, WindowFunction, field_with_logical_type_metadata,
};
use crate::ir::{QueryProfileHint, TableId};
use crate::storage::RowIdentityProjection;
use wendelon::schema::{PhysicalDescriptorFingerprint, STORAGE_ROW_KEY_ID_COLUMN, TableObjectId};
use wendelon_client::LogicalTypeRefinementPredicate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnOrdinal(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JoinReplayId(pub u32);

impl JoinReplayId {
	pub const SYNTHETIC: Self = Self(u32::MAX);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BroadcastReplayEligibility {
	Replayable,
	Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnName(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PbcProgram(pub Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub u128);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedColumn {
	pub name: ColumnName,
	pub ordinal: ColumnOrdinal,
	pub data_type: DataType,
	pub nullable: bool,
	pub pbc_eligible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BoundExprId(pub u32);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundBinaryOp {
	Add,
	Sub,
	Mul,
	Div,
	Eq,
	Neq,
	Lt,
	Lte,
	Gt,
	Gte,
	And,
	Or,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundUnaryOp {
	Not,
	IsNull,
	IsNotNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundExprNode {
	Column(ResolvedColumn),
	Literal(u32),
	NullLiteral,
	Cast {
		input: BoundExprId,
		target: DataType,
	},
	TryCast {
		input: BoundExprId,
		target: DataType,
	},
	Binary {
		op: BoundBinaryOp,
		left: BoundExprId,
		right: BoundExprId,
	},
	Unary {
		op: BoundUnaryOp,
		input: BoundExprId,
	},
	InList {
		expr: BoundExprId,
		list: Vec<BoundExprId>,
		negated: bool,
	},
	Like {
		expr: BoundExprId,
		pattern: String,
		negated: bool,
		case_insensitive: bool,
	},
	Case {
		when_then: Vec<(BoundExprId, BoundExprId)>,
		else_input: BoundExprId,
	},
	TimeBucket {
		interval: std::time::Duration,
		input: BoundExprId,
	},
	AtTimeZone {
		input: BoundExprId,
		zone: String,
	},
	Extract {
		field: TemporalExtractField,
		zone: Option<String>,
		input: BoundExprId,
	},
	DateTrunc {
		field: TemporalTruncField,
		zone: String,
		input: BoundExprId,
	},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundExprProgram {
	pub nodes: Vec<BoundExprNode>,
	pub node_types: Vec<DataType>,
	pub node_logical_types: Vec<LogicalTypeId>,
	pub literals: Vec<ScalarValue>,
	pub roots: Vec<BoundExprId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundAggregateCall {
	pub function: AggregateFunction,
	pub args: BoundExprProgram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundExprProgramError {
	NodeTypeArityMismatch {
		nodes: usize,
		node_types: usize,
	},
	NodeLogicalTypeArityMismatch {
		nodes: usize,
		node_logical_types: usize,
	},
	ChildOutOfBounds {
		parent: BoundExprId,
		child: BoundExprId,
	},
	ChildNotTopological {
		parent: BoundExprId,
		child: BoundExprId,
	},
	LiteralOutOfBounds {
		node: BoundExprId,
		literal: u32,
	},
	RootOutOfBounds {
		root: BoundExprId,
	},
	ColumnTypeMismatch {
		node: BoundExprId,
		column_type: DataType,
		node_type: DataType,
	},
}

#[derive(Debug, Clone, Default)]
pub struct BoundExprProgramBuilder {
	nodes: Vec<BoundExprNode>,
	node_types: Vec<DataType>,
	node_logical_types: Vec<LogicalTypeId>,
	literals: Vec<ScalarValue>,
	roots: Vec<BoundExprId>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundReturningProjection {
	pub exprs: BoundExprProgram,
	pub output_schema: SchemaRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SystemTableKind {
	InformationSchemaLogicalTypes,
	InformationSchemaTables,
	InformationSchemaColumns,
	InformationSchemaSchemata,
	InformationSchemaOptimizerDecisions,
	InformationSchemaOptimizerDecisionConflicts,
	InformationSchemaOptimizerMemoAdmission,
	InformationSchemaOptimizerProbeLinearityFences,
	InformationSchemaOptimizerRuntimeVerdicts,
	InformationSchemaOptimizerMarginAdvisory,
	InformationSchemaOptimizerReceiptGates,
	InformationSchemaOptimizerEvidenceStatus,
	OptimizerExplain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionPlan {
	EmptyRelation {
		schema: SchemaRef,
	},
	SystemTable {
		kind: SystemTableKind,
		schema: SchemaRef,
		rows: Vec<Vec<ScalarValue>>,
	},
	Scan {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		projection: Option<Vec<ColumnOrdinal>>,
		pbc_program: Option<PbcProgram>,
		row_limit: Option<usize>,
		row_identity: RowIdentityProjection,
		profile_hint: QueryProfileHint,
		primary_key_columns: Vec<ColumnOrdinal>,
		column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
		column_range: Vec<(ColumnOrdinal, RangeFact)>,
		column_range_distributions: Vec<(ColumnOrdinal, ColumnRangeDistributionFact)>,
		column_heavy_hitters: Vec<(ColumnOrdinal, ColumnHeavyHitterFact)>,
		column_group_top_k: Vec<(Vec<ColumnOrdinal>, ColumnGroupTopKFact)>,
		column_group_ndv: Vec<(Vec<ColumnOrdinal>, DistinctCountFact)>,
	},
	Filter {
		input: Box<ExecutionPlan>,
		predicate: BoundExprProgram,
		disposition: FilterDisposition,
	},
	Project {
		input: Box<ExecutionPlan>,
		exprs: BoundExprProgram,
		output_schema: SchemaRef,
	},
	Union {
		left: Box<ExecutionPlan>,
		right: Box<ExecutionPlan>,
		output_schema: SchemaRef,
	},
	Repeat {
		input: Box<ExecutionPlan>,
		count_column: ColumnOrdinal,
		output_schema: SchemaRef,
	},
	Aggregate {
		input: Box<ExecutionPlan>,
		group_expr: BoundExprProgram,
		aggr_expr: Vec<BoundAggregateCall>,
		output_schema: SchemaRef,
	},
	Exchange {
		input: Box<ExecutionPlan>,
		partition_count: u32,
		key: BoundExprProgram,
	},
	Sort {
		input: Box<ExecutionPlan>,
		order_by: Vec<ExecutionSortKey>,
		limit: Option<usize>,
		offset: usize,
		output_schema: SchemaRef,
	},
	/// ADR 241: ranking window functions. Row-preserving — `output_schema` is the input schema plus
	/// one column per `functions` entry (in order). The operator orders each co-located partition by
	/// `order_by` internally and streams ranks, resetting on each `partition_by` change.
	Window {
		input: Box<ExecutionPlan>,
		partition_by: BoundExprProgram,
		order_by: Vec<ExecutionSortKey>,
		functions: Vec<WindowFunction>,
		output_schema: SchemaRef,
	},
	Take {
		input: Box<ExecutionPlan>,
		limit: usize,
		output_schema: SchemaRef,
	},
	Join {
		replay_id: JoinReplayId,
		replay_eligibility: BroadcastReplayEligibility,
		left: Box<ExecutionPlan>,
		right: Box<ExecutionPlan>,
		kind: JoinKind,
		left_keys: Vec<ColumnOrdinal>,
		right_keys: Vec<ColumnOrdinal>,
		null_safe_keys: bool,
		strategy: JoinStrategy,
		output_schema: SchemaRef,
	},
	CreateTable {
		name: TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
		or_replace: bool,
	},
	CreateLogicalType {
		name: String,
		physical: DataType,
		refinement: Option<LogicalTypeRefinementPredicate>,
		unit: Option<wendelon_client::LogicalTypeUnit>,
	},
	CreateTableAsSelect {
		table: TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
		or_replace: bool,
		source: Box<ExecutionPlan>,
	},
	DropTable {
		name: TableRef,
		if_exists: bool,
	},
	Delete {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		predicate: Option<BoundExprProgram>,
		pbc_program: Option<PbcProgram>,
		profile_hint: QueryProfileHint,
		returning: Option<BoundReturningProjection>,
	},
	Update {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		source: Box<ExecutionPlan>,
		profile_hint: QueryProfileHint,
		returning: Option<BoundReturningProjection>,
	},
	Insert {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		rows: Vec<Vec<ScalarValue>>,
		returning: Option<BoundReturningProjection>,
	},
	InsertSelect {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		target_columns: Vec<ColumnOrdinal>,
		source: Box<ExecutionPlan>,
		returning: Option<BoundReturningProjection>,
	},
}

impl ExecutionPlan {
	pub fn schema(&self) -> SchemaRef {
		match self {
			Self::EmptyRelation { schema } => schema.clone(),
			Self::SystemTable { schema, .. } => schema.clone(),
			Self::Scan {
				schema,
				row_identity,
				..
			} => scan_output_schema(schema, *row_identity),
			Self::Filter { input, .. } => input.schema(),
			Self::Project { output_schema, .. } => output_schema.clone(),
			Self::Union { output_schema, .. } => output_schema.clone(),
			Self::Repeat { output_schema, .. } => output_schema.clone(),
			Self::Aggregate { output_schema, .. } => output_schema.clone(),
			Self::Exchange { input, .. } => input.schema(),
			Self::Sort { output_schema, .. } => output_schema.clone(),
			Self::Window { output_schema, .. } => output_schema.clone(),
			Self::Take { output_schema, .. } => output_schema.clone(),
			Self::Join { output_schema, .. } => output_schema.clone(),
			Self::CreateTable { .. } => Arc::new(Schema::empty()),
			Self::CreateLogicalType { .. } => Arc::new(Schema::empty()),
			Self::CreateTableAsSelect { .. } => Arc::new(Schema::empty()),
			Self::DropTable { .. } => Arc::new(Schema::empty()),
			Self::Delete {
				returning: Some(returning),
				..
			}
			| Self::Update {
				returning: Some(returning),
				..
			}
			| Self::Insert {
				returning: Some(returning),
				..
			}
			| Self::InsertSelect {
				returning: Some(returning),
				..
			} => returning.output_schema.clone(),
			Self::Delete { .. } => result_count_schema_int64("deleted"),
			Self::Update { .. } | Self::Insert { .. } | Self::InsertSelect { .. } => {
				Arc::new(Schema::empty())
			}
		}
	}
}

fn scan_output_schema(schema: &SchemaRef, row_identity: RowIdentityProjection) -> SchemaRef {
	match row_identity {
		RowIdentityProjection::None => schema.clone(),
		RowIdentityProjection::Append => {
			let mut fields = schema
				.fields()
				.iter()
				.map(|field| field.as_ref().clone())
				.collect::<Vec<_>>();
			fields.push(Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			));
			Arc::new(Schema::new(fields))
		}
	}
}

fn result_count_schema_int64(name: &'static str) -> SchemaRef {
	Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]))
}

impl BoundExprProgramBuilder {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn append_column(&mut self, column: ResolvedColumn) -> BoundExprId {
		let output_type = column.data_type.clone();
		self.append_node(BoundExprNode::Column(column), output_type)
	}

	pub fn append_literal(&mut self, value: ScalarValue) -> BoundExprId {
		let literal = u32::try_from(self.literals.len()).expect("literal index overflow");
		let output_type = value.data_type();
		self.literals.push(value);
		self.append_node(BoundExprNode::Literal(literal), output_type)
	}

	pub fn append_null(&mut self) -> BoundExprId {
		self.append_node(BoundExprNode::NullLiteral, DataType::Null)
	}

	pub fn append_cast(
		&mut self,
		input: BoundExprId,
		target: DataType,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::Cast { input, target }, output_type)
	}

	pub fn append_try_cast(
		&mut self,
		input: BoundExprId,
		target: DataType,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::TryCast { input, target }, output_type)
	}

	pub fn append_binary(
		&mut self,
		op: BoundBinaryOp,
		left: BoundExprId,
		right: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(left);
		self.assert_existing_child(right);
		self.append_node(BoundExprNode::Binary { op, left, right }, output_type)
	}

	pub fn append_unary(
		&mut self,
		op: BoundUnaryOp,
		input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::Unary { op, input }, output_type)
	}

	pub fn append_in_list(
		&mut self,
		expr: BoundExprId,
		list: Vec<BoundExprId>,
		negated: bool,
	) -> BoundExprId {
		self.assert_existing_child(expr);
		for item in &list {
			self.assert_existing_child(*item);
		}
		self.append_node(
			BoundExprNode::InList {
				expr,
				list,
				negated,
			},
			DataType::Boolean,
		)
	}

	pub fn append_like(
		&mut self,
		expr: BoundExprId,
		pattern: String,
		negated: bool,
		case_insensitive: bool,
	) -> BoundExprId {
		self.assert_existing_child(expr);
		self.append_node(
			BoundExprNode::Like {
				expr,
				pattern,
				negated,
				case_insensitive,
			},
			DataType::Boolean,
		)
	}

	pub fn append_case(
		&mut self,
		when_then: Vec<(BoundExprId, BoundExprId)>,
		else_input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		for (condition, result) in &when_then {
			self.assert_existing_child(*condition);
			self.assert_existing_child(*result);
		}
		self.assert_existing_child(else_input);
		self.append_node(
			BoundExprNode::Case {
				when_then,
				else_input,
			},
			output_type,
		)
	}

	pub fn append_time_bucket(
		&mut self,
		interval: std::time::Duration,
		input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::TimeBucket { interval, input }, output_type)
	}

	pub fn append_at_time_zone(
		&mut self,
		input: BoundExprId,
		zone: String,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::AtTimeZone { input, zone }, output_type)
	}

	pub fn append_extract(
		&mut self,
		field: TemporalExtractField,
		zone: Option<String>,
		input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::Extract { field, zone, input }, output_type)
	}

	pub fn append_date_trunc(
		&mut self,
		field: TemporalTruncField,
		zone: String,
		input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::DateTrunc { field, zone, input }, output_type)
	}

	pub fn push_root(&mut self, id: BoundExprId) {
		self.assert_existing_child(id);
		self.roots.push(id);
	}

	pub fn node_type(&self, id: BoundExprId) -> Option<&DataType> {
		self.node_types.get(id.0 as usize)
	}

	pub fn node_logical_type(&self, id: BoundExprId) -> Option<LogicalTypeId> {
		self.node_logical_types.get(id.0 as usize).copied()
	}

	pub fn set_node_logical_type(&mut self, id: BoundExprId, logical_type: LogicalTypeId) -> bool {
		let Some(slot) = self.node_logical_types.get_mut(id.0 as usize) else {
			return false;
		};
		*slot = logical_type;
		true
	}

	pub fn literal(&self, id: BoundExprId) -> Option<&ScalarValue> {
		let BoundExprNode::Literal(literal) = self.nodes.get(id.0 as usize)? else {
			return None;
		};
		self.literals.get(*literal as usize)
	}

	pub fn rewrite_literal(&mut self, id: BoundExprId, value: ScalarValue) -> bool {
		let Some(BoundExprNode::Literal(literal)) = self.nodes.get(id.0 as usize) else {
			return false;
		};
		let Some(slot) = self.literals.get_mut(*literal as usize) else {
			return false;
		};
		let output_type = value.data_type();
		*slot = value;
		if let Some(node_type) = self.node_types.get_mut(id.0 as usize) {
			*node_type = output_type;
			true
		} else {
			false
		}
	}

	pub fn build(self) -> BoundExprProgram {
		BoundExprProgram {
			nodes: self.nodes,
			node_types: self.node_types,
			node_logical_types: self.node_logical_types,
			literals: self.literals,
			roots: self.roots,
		}
	}

	fn append_node(&mut self, node: BoundExprNode, output_type: DataType) -> BoundExprId {
		let id = BoundExprId(u32::try_from(self.nodes.len()).expect("bound expression overflow"));
		self.nodes.push(node);
		self.node_types.push(output_type);
		self.node_logical_types.push(LogicalTypeId::NAKED);
		id
	}

	fn assert_existing_child(&self, id: BoundExprId) {
		assert!(
			(id.0 as usize) < self.nodes.len(),
			"BoundExprId {:?} must reference an already appended node",
			id
		);
	}
}

impl BoundExprProgram {
	pub fn empty() -> Self {
		BoundExprProgramBuilder::new().build()
	}

	pub fn root_count(&self) -> usize {
		self.roots.len()
	}

	pub fn root_type(&self, root_index: usize) -> Option<&DataType> {
		self.node_type(*self.roots.get(root_index)?)
	}

	pub fn single_root(&self) -> Option<BoundExprId> {
		match self.roots.as_slice() {
			[root] => Some(*root),
			_ => None,
		}
	}

	pub fn single_column_root(&self) -> Option<&ResolvedColumn> {
		let root = self.single_root()?;
		self.column_node(root)
	}

	pub fn root_columns(&self) -> Option<Vec<&ResolvedColumn>> {
		self.roots
			.iter()
			.map(|root| self.column_node(*root))
			.collect()
	}

	pub fn is_empty_roots(&self) -> bool {
		self.roots.is_empty()
	}

	pub fn node(&self, id: BoundExprId) -> Option<&BoundExprNode> {
		self.nodes.get(id.0 as usize)
	}

	pub fn node_type(&self, id: BoundExprId) -> Option<&DataType> {
		self.node_types.get(id.0 as usize)
	}

	pub fn node_logical_type(&self, id: BoundExprId) -> Option<LogicalTypeId> {
		self.node_logical_types.get(id.0 as usize).copied()
	}

	pub fn root_nullable(&self, root_index: usize) -> Option<bool> {
		self.node_nullable(*self.roots.get(root_index)?)
	}

	pub fn root_logical_type(&self, root_index: usize) -> Option<LogicalTypeId> {
		self.node_logical_type(*self.roots.get(root_index)?)
	}

	pub fn node_nullable(&self, id: BoundExprId) -> Option<bool> {
		let node = self.node(id)?;
		match node {
			BoundExprNode::Column(column) => Some(column.nullable),
			BoundExprNode::NullLiteral => Some(true),
			BoundExprNode::Literal(literal) => self.literals.get(*literal as usize).map(|value| {
				matches!(
					value,
					ScalarValue::Boolean(None)
						| ScalarValue::Int32(None)
						| ScalarValue::Int64(None)
						| ScalarValue::Date32(None)
						| ScalarValue::Interval(None)
						| ScalarValue::CalendarMonthInterval(None)
						| ScalarValue::Float32(None)
						| ScalarValue::Float64(None)
						| ScalarValue::Utf8(None)
				)
			}),
			BoundExprNode::Cast { input, .. } => self.node_nullable(*input),
			BoundExprNode::TryCast { .. } => Some(true),
			BoundExprNode::Binary { op, left, right } => {
				let left_nullable = self.node_nullable(*left)?;
				let right_nullable = self.node_nullable(*right)?;
				let div_integer_nullable = matches!(op, BoundBinaryOp::Div)
					&& self.node_type(*left).is_some_and(is_integer_type);
				Some(left_nullable || right_nullable || div_integer_nullable)
			}
			BoundExprNode::Unary { op, input } => match op {
				BoundUnaryOp::IsNull | BoundUnaryOp::IsNotNull => Some(false),
				BoundUnaryOp::Not => self.node_nullable(*input),
			},
			BoundExprNode::InList { expr, list, .. } => {
				let expr_nullable = self.node_nullable(*expr)?;
				let list_nullable = list
					.iter()
					.map(|item| self.node_nullable(*item))
					.collect::<Option<Vec<_>>>()?
					.into_iter()
					.any(|nullable| nullable);
				Some(expr_nullable || list_nullable)
			}
			BoundExprNode::Like { expr, .. } => self.node_nullable(*expr),
			BoundExprNode::Case {
				when_then,
				else_input,
			} => {
				let mut nullable = self.node_nullable(*else_input)?;
				for (_, result) in when_then {
					nullable |= self.node_nullable(*result)?;
				}
				Some(nullable)
			}
			BoundExprNode::TimeBucket { input, .. }
			| BoundExprNode::AtTimeZone { input, .. }
			| BoundExprNode::Extract { input, .. }
			| BoundExprNode::DateTrunc { input, .. } => self.node_nullable(*input),
		}
	}

	pub fn root_display_name(&self, root_index: usize) -> Option<&str> {
		let root = *self.roots.get(root_index)?;
		self.column_node(root).map(|column| column.name.0.as_str())
	}

	pub fn validate(&self) -> Result<(), BoundExprProgramError> {
		if self.nodes.len() != self.node_types.len() {
			return Err(BoundExprProgramError::NodeTypeArityMismatch {
				nodes: self.nodes.len(),
				node_types: self.node_types.len(),
			});
		}
		if self.nodes.len() != self.node_logical_types.len() {
			return Err(BoundExprProgramError::NodeLogicalTypeArityMismatch {
				nodes: self.nodes.len(),
				node_logical_types: self.node_logical_types.len(),
			});
		}
		for (idx, node) in self.nodes.iter().enumerate() {
			let parent = BoundExprId(u32::try_from(idx).expect("bound expression id overflow"));
			if let BoundExprNode::Column(column) = node {
				let node_type = self.node_types[idx].clone();
				if column.data_type != node_type {
					return Err(BoundExprProgramError::ColumnTypeMismatch {
						node: parent,
						column_type: column.data_type.clone(),
						node_type,
					});
				}
			}
			if let BoundExprNode::Literal(literal) = node
				&& (*literal as usize) >= self.literals.len()
			{
				return Err(BoundExprProgramError::LiteralOutOfBounds {
					node: parent,
					literal: *literal,
				});
			}
			for child in node_children(node) {
				if (child.0 as usize) >= self.nodes.len() {
					return Err(BoundExprProgramError::ChildOutOfBounds { parent, child });
				}
				if child.0 >= parent.0 {
					return Err(BoundExprProgramError::ChildNotTopological { parent, child });
				}
			}
		}
		for root in &self.roots {
			if (root.0 as usize) >= self.nodes.len() {
				return Err(BoundExprProgramError::RootOutOfBounds { root: *root });
			}
		}
		Ok(())
	}

	pub fn pretty(&self) -> String {
		if self.roots.is_empty() {
			return "<empty roots>".to_string();
		}
		self.roots
			.iter()
			.enumerate()
			.map(|(idx, root)| format!("root[{idx}] = {}", self.pretty_node(*root)))
			.collect::<Vec<_>>()
			.join("\n")
	}

	fn column_node(&self, id: BoundExprId) -> Option<&ResolvedColumn> {
		match self.node(id)? {
			BoundExprNode::Column(column) => Some(column),
			_ => None,
		}
	}

	fn pretty_node(&self, id: BoundExprId) -> String {
		let Some(node) = self.node(id) else {
			return format!("<invalid {:?}>", id);
		};
		match node {
			BoundExprNode::Column(column) => format!("{}#{}", column.name.0, column.ordinal.0),
			BoundExprNode::NullLiteral => "NULL".to_string(),
			BoundExprNode::Literal(literal) => self
				.literals
				.get(*literal as usize)
				.map(|value| format!("{value:?}"))
				.unwrap_or_else(|| format!("<invalid literal {literal}>")),
			BoundExprNode::Cast { input, target } => {
				format!("CAST({} AS {target:?})", self.pretty_node(*input))
			}
			BoundExprNode::TryCast { input, target } => {
				format!("TRY_CAST({} AS {target:?})", self.pretty_node(*input))
			}
			BoundExprNode::Binary { op, left, right } => {
				format!(
					"{op:?}({}, {})",
					self.pretty_node(*left),
					self.pretty_node(*right)
				)
			}
			BoundExprNode::Unary { op, input } => {
				format!("{op:?}({})", self.pretty_node(*input))
			}
			BoundExprNode::InList {
				expr,
				list,
				negated,
			} => {
				let members = list
					.iter()
					.map(|item| self.pretty_node(*item))
					.collect::<Vec<_>>()
					.join(", ");
				let op = if *negated { "NOT IN" } else { "IN" };
				format!("{} {op} ({members})", self.pretty_node(*expr))
			}
			BoundExprNode::Like {
				expr,
				pattern,
				negated,
				case_insensitive,
			} => {
				let op = match (*negated, *case_insensitive) {
					(false, false) => "LIKE",
					(true, false) => "NOT LIKE",
					(false, true) => "ILIKE",
					(true, true) => "NOT ILIKE",
				};
				format!("{} {op} {pattern:?}", self.pretty_node(*expr))
			}
			BoundExprNode::Case {
				when_then,
				else_input,
			} => {
				let mut parts = Vec::with_capacity(when_then.len() + 2);
				parts.push("CASE".to_string());
				for (condition, result) in when_then {
					parts.push(format!(
						"WHEN {} THEN {}",
						self.pretty_node(*condition),
						self.pretty_node(*result)
					));
				}
				parts.push(format!("ELSE {}", self.pretty_node(*else_input)));
				parts.push("END".to_string());
				parts.join(" ")
			}
			BoundExprNode::TimeBucket { interval, input } => {
				format!("TIME_BUCKET({interval:?}, {})", self.pretty_node(*input))
			}
			BoundExprNode::AtTimeZone { input, zone } => {
				format!("AT_TIME_ZONE({}, {zone:?})", self.pretty_node(*input))
			}
			BoundExprNode::Extract { field, zone, input } => match zone {
				Some(zone) => format!(
					"EXTRACT({} FROM {} AT {zone:?})",
					field.as_str(),
					self.pretty_node(*input)
				),
				None => format!(
					"EXTRACT({} FROM {})",
					field.as_str(),
					self.pretty_node(*input)
				),
			},
			BoundExprNode::DateTrunc { field, zone, input } => {
				format!(
					"DATE_TRUNC({}, {} AT {zone:?})",
					field.as_str(),
					self.pretty_node(*input)
				)
			}
		}
	}
}

fn node_children(node: &BoundExprNode) -> Vec<BoundExprId> {
	match node {
		BoundExprNode::Column(_) | BoundExprNode::Literal(_) | BoundExprNode::NullLiteral => {
			Vec::new()
		}
		BoundExprNode::Cast { input, .. }
		| BoundExprNode::TryCast { input, .. }
		| BoundExprNode::Unary { input, .. }
		| BoundExprNode::Like { expr: input, .. }
		| BoundExprNode::TimeBucket { input, .. }
		| BoundExprNode::AtTimeZone { input, .. }
		| BoundExprNode::Extract { input, .. }
		| BoundExprNode::DateTrunc { input, .. } => vec![*input],
		BoundExprNode::Binary { left, right, .. } => vec![*left, *right],
		BoundExprNode::InList { expr, list, .. } => {
			let mut children = Vec::with_capacity(list.len() + 1);
			children.push(*expr);
			children.extend(list.iter().copied());
			children
		}
		BoundExprNode::Case {
			when_then,
			else_input,
		} => {
			let mut children = Vec::with_capacity(when_then.len() * 2 + 1);
			for (condition, result) in when_then {
				children.push(*condition);
				children.push(*result);
			}
			children.push(*else_input);
			children
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

pub fn schema_for_exprs(exprs: &BoundExprProgram) -> SchemaRef {
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = exprs
				.root_display_name(idx)
				.map(str::to_string)
				.unwrap_or_else(|| format!("col_{idx}"));
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
			field_with_logical_type_metadata(name, data_type, nullable, logical_type)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn non_nullable_pbc_column() -> ResolvedColumn {
		ResolvedColumn {
			name: ColumnName("amount".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int32,
			nullable: false,
			pbc_eligible: true,
		}
	}

	#[test]
	fn try_cast_node_nullable_is_always_true() {
		let mut builder = BoundExprProgramBuilder::new();
		let input = builder.append_column(non_nullable_pbc_column());
		let try_cast = builder.append_try_cast(input, DataType::Int32, DataType::Int32);
		builder.push_root(try_cast);
		let program = builder.build();

		assert!(program.root_nullable(0).expect("root nullable"));
	}

	#[test]
	fn builder_rejects_malformed_child_ordering_in_validate() {
		let program = BoundExprProgram {
			nodes: vec![BoundExprNode::Unary {
				op: BoundUnaryOp::Not,
				input: BoundExprId(0),
			}],
			node_types: vec![DataType::Boolean],
			node_logical_types: vec![LogicalTypeId::NAKED],
			literals: Vec::new(),
			roots: vec![BoundExprId(0)],
		};

		assert!(matches!(
			program.validate(),
			Err(BoundExprProgramError::ChildNotTopological { .. })
		));
	}

	#[test]
	fn builder_reports_root_types_and_pretty_empty_roots() {
		let mut builder = BoundExprProgramBuilder::new();
		let column = builder.append_column(non_nullable_pbc_column());
		builder.push_root(column);
		let program = builder.build();

		assert_eq!(program.root_type(0), Some(&DataType::Int32));
		assert!(program.pretty().contains("amount#0"));

		let empty = BoundExprProgramBuilder::new().build();
		assert!(empty.is_empty_roots());
		assert_eq!(empty.pretty(), "<empty roots>");
	}
}
