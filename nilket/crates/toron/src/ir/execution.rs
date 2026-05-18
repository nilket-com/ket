use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
	TimeBucket {
		interval: std::time::Duration,
		input: BoundExprId,
	},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundExprProgram {
	pub nodes: Vec<BoundExprNode>,
	pub node_types: Vec<DataType>,
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
		predicate: BoundExprProgram,
		disposition: FilterDisposition,
	},
	Project {
		input: Box<ExecutionPlan>,
		exprs: BoundExprProgram,
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
	CreateTableAsSelect {
		table: TableRef,
		schema: SchemaRef,
		source: Box<ExecutionPlan>,
	},
	DropTable {
		name: TableRef,
		if_exists: bool,
	},
	Insert {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		rows: Vec<Vec<ScalarValue>>,
	},
	InsertSelect {
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		target_columns: Vec<ColumnOrdinal>,
		source: Box<ExecutionPlan>,
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
			Self::CreateTableAsSelect { .. } => Arc::new(Schema::empty()),
			Self::DropTable { .. } => Arc::new(Schema::empty()),
			Self::Insert { .. } => Arc::new(Schema::empty()),
			Self::InsertSelect { .. } => Arc::new(Schema::empty()),
		}
	}
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

	pub fn append_time_bucket(
		&mut self,
		interval: std::time::Duration,
		input: BoundExprId,
		output_type: DataType,
	) -> BoundExprId {
		self.assert_existing_child(input);
		self.append_node(BoundExprNode::TimeBucket { interval, input }, output_type)
	}

	pub fn push_root(&mut self, id: BoundExprId) {
		self.assert_existing_child(id);
		self.roots.push(id);
	}

	pub fn node_type(&self, id: BoundExprId) -> Option<&DataType> {
		self.node_types.get(id.0 as usize)
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
			literals: self.literals,
			roots: self.roots,
		}
	}

	fn append_node(&mut self, node: BoundExprNode, output_type: DataType) -> BoundExprId {
		let id = BoundExprId(u32::try_from(self.nodes.len()).expect("bound expression overflow"));
		self.nodes.push(node);
		self.node_types.push(output_type);
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

	pub fn root_nullable(&self, root_index: usize) -> Option<bool> {
		self.node_nullable(*self.roots.get(root_index)?)
	}

	pub fn node_nullable(&self, id: BoundExprId) -> Option<bool> {
		let node = self.node(id)?;
		match node {
			BoundExprNode::Column(column) => Some(column.nullable),
			BoundExprNode::Literal(literal) => self.literals.get(*literal as usize).map(|value| {
				matches!(
					value,
					ScalarValue::Boolean(None)
						| ScalarValue::Int32(None)
						| ScalarValue::Int64(None)
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
			BoundExprNode::TimeBucket { input, .. } => self.node_nullable(*input),
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
			BoundExprNode::TimeBucket { interval, input } => {
				format!("TIME_BUCKET({interval:?}, {})", self.pretty_node(*input))
			}
		}
	}
}

fn node_children(node: &BoundExprNode) -> Vec<BoundExprId> {
	match node {
		BoundExprNode::Column(_) | BoundExprNode::Literal(_) => Vec::new(),
		BoundExprNode::Cast { input, .. }
		| BoundExprNode::TryCast { input, .. }
		| BoundExprNode::Unary { input, .. }
		| BoundExprNode::TimeBucket { input, .. } => vec![*input],
		BoundExprNode::Binary { left, right, .. } => vec![*left, *right],
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
			Field::new(name, data_type, nullable)
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
