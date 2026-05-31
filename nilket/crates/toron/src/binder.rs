use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::aggregate_kernel::{
	self, AggregateArity, AggregateInputDomain, AggregateKernelSpec, StableAggregateKernel,
};
use crate::catalog::{
	Catalog, DistinctCountFact, TableBinding, TableOptimizerFacts, TableRef, TableRowIdentity,
};
use crate::distributed::budget::{ByteCount, QueryBudget};
use crate::distributed::task::RowCount;
use crate::error::BindingError;
use crate::exec::Executor;
use crate::ir::{
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprId, BoundExprNode,
	BoundExprProgram, BoundExprProgramBuilder, BoundReturningProjection, BoundUnaryOp,
	BroadcastReplayEligibility, ColumnName, ColumnOrdinal, ColumnRef, CreateTableColumnType,
	CreateTableSchema, ExecutionPlan, ExecutionSortKey, Expr, FilterDisposition, InsertLiteral,
	JoinKind, JoinReplayId, JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan,
	LogicalTypeId, LogicalTypeRegistry, LogicalTypeResolver, LogicalTypeSpec, PbcProgram,
	QueryProfileHint, ResolvedColumn, ReturningProjection, ScalarValue, SortExpr, SystemTableKind,
	arithmetic_output_type, decode_column_logical_type, decode_column_logical_type_with_resolver,
	field_with_logical_type_metadata, logical_type_name, logical_type_spec, schema_for_exprs,
};
use crate::pbc::PbcCompiler;
use wendelon::schema::{
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN,
};
use wendelon_client::{
	LogicalTypeRefinementComparisonOp, LogicalTypeRefinementPredicate, LogicalTypeRefinementValue,
	TableSchemaDescriptor,
};

pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
const DEFAULT_AUTO_JOIN_PARTITION_COUNT: u32 = 4;
const STORAGE_SYNTHETIC_COLUMN_NAMES: &[&str] = &[
	STORAGE_ROW_HLC_COLUMN,
	STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN,
];
const INFORMATION_SCHEMA_NAMESPACE: &str = "information_schema";
const INFORMATION_SCHEMA_LOGICAL_TYPES: &str = "information_schema.logical_types";
const INFORMATION_SCHEMA_TABLES: &str = "information_schema.tables";
const INFORMATION_SCHEMA_COLUMNS: &str = "information_schema.columns";
const INFORMATION_SCHEMA_SCHEMATA: &str = "information_schema.schemata";
const INFORMATION_SCHEMA_LOGICAL_TYPES_TABLE: &str = "logical_types";
const INFORMATION_SCHEMA_TABLES_TABLE: &str = "tables";
const INFORMATION_SCHEMA_COLUMNS_TABLE: &str = "columns";
const INFORMATION_SCHEMA_SCHEMATA_TABLE: &str = "schemata";
const ARROW_BUFFER_ALIGNMENT_BYTES: u64 = 64;
const DEFAULT_UTF8_BINARY_PAYLOAD_BYTES: u64 = 256;
const DEFAULT_LARGE_UTF8_BINARY_PAYLOAD_BYTES: u64 = 1024;
const RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
	pub allow_experimental_kernels: bool,
	pub auto_broadcast_max_build_bytes: Option<ByteCount>,
	pub force_hash_join_ids: BTreeSet<JoinReplayId>,
}

impl Default for PlannerConfig {
	fn default() -> Self {
		Self {
			allow_experimental_kernels: false,
			auto_broadcast_max_build_bytes: Some(ByteCount::new(1024 * 1024)),
			force_hash_join_ids: BTreeSet::new(),
		}
	}
}

pub async fn bind(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<ExecutionPlan, BindingError> {
	let (plan, _) = bind_with_trace(plan, catalog, config).await?;
	Ok(plan)
}

pub(crate) async fn bind_with_trace(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<(ExecutionPlan, OptimizerTrace), BindingError> {
	let mut trace = OptimizerTrace::default();
	let plan = Binder { catalog, config }
		.bind_plan_with_trace(plan, &mut trace)
		.await?;
	let plan = push_down_scan_projection(plan.plan)?;
	Ok((plan, trace))
}

struct Binder<'a> {
	catalog: &'a dyn Catalog,
	config: &'a PlannerConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinTraceTarget {
	pub(crate) join_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecisionReceipt {
	pub(crate) target: JoinTraceTarget,
	pub(crate) strategy: JoinStrategy,
	pub(crate) placement: JoinPhysicalPlacement,
	pub(crate) reason: JoinStrategyDecisionReason,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OptimizerTrace {
	pub(crate) join_decisions: Vec<DecisionReceipt>,
}

struct ScopedExecutionPlan {
	plan: ExecutionPlan,
	scope: RelationScope,
	facts: PlanOptimizerFacts,
}

impl ScopedExecutionPlan {
	fn new(plan: ExecutionPlan, scope: RelationScope) -> Self {
		Self {
			plan,
			scope,
			facts: PlanOptimizerFacts::default(),
		}
	}

	fn new_with_facts(
		plan: ExecutionPlan,
		scope: RelationScope,
		facts: PlanOptimizerFacts,
	) -> Self {
		Self { plan, scope, facts }
	}

	fn empty_scope(plan: ExecutionPlan) -> Self {
		let scope = RelationScope::empty(plan.schema());
		Self {
			plan,
			scope,
			facts: PlanOptimizerFacts::default(),
		}
	}

	fn schema(&self) -> SchemaRef {
		self.plan.schema()
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PlanOptimizerFacts {
	origin: PlanOrigin,
	byte_upper_bound: Option<ByteCount>,
	estimated_byte_size: Option<ByteCount>,
	estimated_row_count: Option<RowCount>,
	row_upper_bound: Option<RowCount>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PlanOrigin {
	#[default]
	Unknown,
	SingleBaseRelation,
	Composite,
}

impl PlanOptimizerFacts {
	fn from_table(facts: TableOptimizerFacts) -> Self {
		Self {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: facts.byte_upper_bound,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: facts.row_upper_bound,
		}
	}

	fn composite_unknown() -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
		}
	}

	fn composite_with_row(row_upper_bound: Option<RowCount>) -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound,
		}
	}

	fn composite_with_bounds(
		byte_upper_bound: Option<ByteCount>,
		row_upper_bound: Option<RowCount>,
	) -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound,
		}
	}

	fn empty_relation() -> Self {
		Self::composite_with_bounds(Some(ByteCount::ZERO), Some(RowCount::new(0)))
	}

	fn preserve_volume_non_increasing(self) -> Self {
		self
	}

	fn preserve_through_project(self, exprs: &BoundExprProgram) -> Self {
		if project_roots_are_distinct_columns(exprs) {
			return self.preserve_volume_non_increasing();
		}

		Self {
			origin: self.origin,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: self.estimated_row_count,
			row_upper_bound: self.row_upper_bound,
		}
	}

	fn with_pk_row_cap(self) -> Self {
		let cap = RowCount::new(1);
		let row_upper_bound = self
			.row_upper_bound
			.map(|row| std::cmp::min(row, cap))
			.or(Some(cap));
		let estimated_row_count = self.estimated_row_count.map(|row| std::cmp::min(row, cap));
		Self {
			row_upper_bound,
			estimated_row_count,
			..self
		}
	}

	fn with_estimated_row_count(self, estimate: RowCount) -> Self {
		let estimate = self
			.estimated_row_count
			.map(|existing| std::cmp::min(existing, estimate))
			.unwrap_or(estimate);
		let estimate = self
			.row_upper_bound
			.map(|row_upper_bound| std::cmp::min(row_upper_bound, estimate))
			.unwrap_or(estimate);
		Self {
			estimated_row_count: Some(estimate),
			..self
		}
	}

	fn exchange(self) -> Self {
		Self::composite_with_bounds(self.byte_upper_bound, self.row_upper_bound)
	}

	fn take(self, limit: usize) -> Self {
		let limit = RowCount::new(u64::try_from(limit).unwrap_or(u64::MAX));
		let row_upper_bound = self
			.row_upper_bound
			.map(|row| std::cmp::min(row, limit))
			.or(Some(limit));
		Self::composite_with_row(row_upper_bound)
	}

	fn derive_estimated_byte_size(self, schema: &Schema) -> Self {
		if self.origin != PlanOrigin::SingleBaseRelation {
			return self;
		}
		let Some(row_count) = estimate_row_source(self.row_upper_bound, self.estimated_row_count)
		else {
			return self;
		};
		let Some(estimated_byte_size) = estimate_materialized_byte_size(row_count, schema) else {
			return Self {
				estimated_byte_size: None,
				..self
			};
		};
		Self {
			estimated_byte_size: Some(
				self.estimated_byte_size
					.map(|existing| std::cmp::min(existing, estimated_byte_size))
					.unwrap_or(estimated_byte_size),
			),
			..self
		}
	}
}

fn estimate_row_source(
	row_upper_bound: Option<RowCount>,
	estimated_row_count: Option<RowCount>,
) -> Option<RowCount> {
	match (row_upper_bound, estimated_row_count) {
		(Some(hard), Some(estimated)) => Some(std::cmp::min(hard, estimated)),
		(Some(hard), None) => Some(hard),
		(None, Some(estimated)) => Some(estimated),
		(None, None) => None,
	}
}

fn project_roots_are_distinct_columns(exprs: &BoundExprProgram) -> bool {
	let mut seen = HashSet::new();
	exprs.roots.iter().all(|root| {
		let Some(BoundExprNode::Column(column)) = exprs.node(*root) else {
			return false;
		};
		seen.insert(column.ordinal)
	})
}

fn estimate_materialized_byte_size(
	row_upper_bound: RowCount,
	schema: &Schema,
) -> Option<ByteCount> {
	let rows = row_upper_bound.get();
	let mut bytes = RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES;
	for field in schema.fields() {
		let per_row = per_row_estimate_bytes(field.data_type())?;
		let values = pad_to_arrow_buffer_alignment(rows.saturating_mul(per_row));
		let validity = pad_to_arrow_buffer_alignment(div_ceil_u64(rows, 8));
		bytes = bytes.saturating_add(values).saturating_add(validity);
	}
	Some(ByteCount::new(bytes))
}

fn per_row_estimate_bytes(data_type: &DataType) -> Option<u64> {
	match data_type {
		DataType::Null => Some(0),
		DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
		DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
		DataType::Int32
		| DataType::UInt32
		| DataType::Float32
		| DataType::Date32
		| DataType::Time32(_)
		| DataType::Interval(IntervalUnit::YearMonth)
		| DataType::Decimal32(_, _) => Some(4),
		DataType::Int64
		| DataType::UInt64
		| DataType::Float64
		| DataType::Timestamp(_, _)
		| DataType::Date64
		| DataType::Time64(_)
		| DataType::Duration(_)
		| DataType::Interval(IntervalUnit::DayTime)
		| DataType::Decimal64(_, _) => Some(8),
		DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Decimal128(_, _) => Some(16),
		DataType::Decimal256(_, _) => Some(32),
		DataType::FixedSizeBinary(width) => u64::try_from(*width).ok(),
		DataType::Utf8 | DataType::Binary => Some(4 + DEFAULT_UTF8_BINARY_PAYLOAD_BYTES),
		DataType::LargeUtf8 | DataType::LargeBinary => {
			Some(8 + DEFAULT_LARGE_UTF8_BINARY_PAYLOAD_BYTES)
		}
		DataType::BinaryView
		| DataType::Utf8View
		| DataType::List(_)
		| DataType::ListView(_)
		| DataType::FixedSizeList(_, _)
		| DataType::LargeList(_)
		| DataType::LargeListView(_)
		| DataType::Struct(_)
		| DataType::Union(_, _)
		| DataType::Dictionary(_, _)
		| DataType::Map(_, _)
		| DataType::RunEndEncoded(_, _) => None,
	}
}

fn pad_to_arrow_buffer_alignment(bytes: u64) -> u64 {
	if bytes == 0 {
		return 0;
	}
	bytes
		.checked_add(ARROW_BUFFER_ALIGNMENT_BYTES - 1)
		.map(|bytes| bytes & !(ARROW_BUFFER_ALIGNMENT_BYTES - 1))
		.unwrap_or(u64::MAX)
}

fn div_ceil_u64(value: u64, divisor: u64) -> u64 {
	debug_assert!(divisor > 0);
	value / divisor + u64::from(value % divisor != 0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JoinStrategyDecision {
	strategy: JoinStrategy,
	placement: JoinPhysicalPlacement,
	reason: JoinStrategyDecisionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinPhysicalPlacement {
	Natural,
	SwappedRightBuild,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BroadcastBuildSide {
	Left,
	Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinStrategyDecisionReason {
	PinnedByAuthor,
	BroadcastInferred {
		build_side: BroadcastBuildSide,
		build_bytes: ByteCount,
		threshold: ByteCount,
		rejected_side_bytes: Option<ByteCount>,
	},
	BroadcastEstimated {
		build_side: BroadcastBuildSide,
		estimated_byte_size: ByteCount,
		threshold: ByteCount,
		rejected_side_bytes: Option<ByteCount>,
	},
	HashByDefault {
		reason: HashByDefaultReason,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashByDefaultReason {
	AutoBroadcastDisabled,
	NonInnerJoin,
	LeftInputNotSingleBaseRelation,
	RightInputNotSingleBaseRelation,
	ProbeNotScanLinear,
	MissingBuildByteUpperBound,
	BuildByteUpperBoundAboveThreshold,
	HashAfterBroadcastCap,
}

#[derive(Debug, Clone, Copy)]
struct JoinInputOptimization {
	facts: PlanOptimizerFacts,
	probe_scan_linear: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildByteSource {
	Hard,
	Estimated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BuildByteCandidate {
	bytes: ByteCount,
	source: BuildByteSource,
}

struct ExprBindContext<'a> {
	config: &'a PlannerConfig,
	logical_casts: &'a HashMap<String, LogicalTypeSpec>,
	relation_scope: Option<&'a RelationScope>,
}

impl<'a> Binder<'a> {
	#[cfg(test)]
	fn bind_plan<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<Box<dyn Future<Output = Result<ScopedExecutionPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let mut trace = OptimizerTrace::default();
			self.bind_plan_with_trace(plan, &mut trace).await
		})
	}

	fn bind_plan_with_trace<'b>(
		&'b self,
		plan: &'b LogicalPlan,
		trace: &'b mut OptimizerTrace,
	) -> Pin<Box<dyn Future<Output = Result<ScopedExecutionPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Scan { table, projection } => {
					let binding = self.catalog.resolve_table_id(*table).await?;
					let optimizer_facts = binding.optimizer_facts.clone();
					let projection = projection
						.as_ref()
						.map(|columns| {
							columns
								.iter()
								.map(|column| {
									resolve_column(column, &binding.schema)
										.map(|column| column.ordinal)
								})
								.collect::<Result<Vec<_>, _>>()
						})
						.transpose()?;

					let plan = scan_plan(binding, projection);
					let facts = PlanOptimizerFacts::from_table(optimizer_facts)
						.derive_estimated_byte_size(plan.schema().as_ref());
					let scope = RelationScope::empty(plan.schema());
					Ok(ScopedExecutionPlan::new_with_facts(plan, scope, facts))
				}
				LogicalPlan::ScanNamed { table, projection } => {
					if is_information_schema_namespace(table) {
						let plan = self
							.bind_information_schema_relation(table, projection.as_ref())
							.await?;
						let scope = RelationScope::scan(table.clone(), &plan.schema());
						return Ok(ScopedExecutionPlan::new(plan, scope));
					}
					let binding = self.catalog.resolve_table(table).await?;
					let optimizer_facts = binding.optimizer_facts.clone();
					let projection = projection
						.as_ref()
						.map(|columns| {
							columns
								.iter()
								.map(|column| {
									resolve_column(column, &binding.schema)
										.map(|column| column.ordinal)
								})
								.collect::<Result<Vec<_>, _>>()
						})
						.transpose()?;

					let plan = scan_plan(binding, projection);
					let facts = PlanOptimizerFacts::from_table(optimizer_facts)
						.derive_estimated_byte_size(plan.schema().as_ref());
					let scope = RelationScope::scan(table.clone(), &plan.schema());
					Ok(ScopedExecutionPlan::new_with_facts(plan, scope, facts))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let facts = input.facts;
					let plan = bind_alias_column_rename(input.plan, columns)?;
					let scope = RelationScope::alias(alias.clone(), &plan.schema());
					Ok(ScopedExecutionPlan::new_with_facts(plan, scope, facts))
				}
				LogicalPlan::Filter { input, predicate } => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let input_facts = input.facts;
					let bound_predicate = self
						.bind_expr_program_scoped(
							std::slice::from_ref(predicate),
							&input.schema(),
							&input.scope,
						)
						.await?;
					require_single_boolean_root(&bound_predicate, "WHERE")?;
					let logical_types = self.catalog.logical_type_registry();
					let input_scope = input.scope.clone();
					let has_pk_row_cap =
						filter_includes_primary_key_row_cap(&bound_predicate, &input.plan);
					let ndv_estimated_row_count = filter_equality_ndv_estimated_row_count(
						&bound_predicate,
						&input.plan,
						input_facts,
					);
					let (plan, _) = plan_filter_with_predicate_pushdown(
						input.plan,
						bound_predicate,
						&logical_types,
					);
					let scope = input_scope.with_schema(plan.schema());
					let facts = if matches!(plan, ExecutionPlan::EmptyRelation { .. }) {
						PlanOptimizerFacts::empty_relation()
					} else if has_pk_row_cap {
						input_facts
							.preserve_volume_non_increasing()
							.with_pk_row_cap()
					} else {
						input_facts.preserve_volume_non_increasing()
					};
					let facts = if let Some(estimated_row_count) = ndv_estimated_row_count {
						facts.with_estimated_row_count(estimated_row_count)
					} else {
						facts
					}
					.derive_estimated_byte_size(plan.schema().as_ref());
					Ok(ScopedExecutionPlan::new_with_facts(plan, scope, facts))
				}
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					if exprs.len() != output_names.len() {
						return Err(BindingError::UnsupportedExpression {
							message: "internal: Project exprs/output_names length mismatch"
								.to_string(),
						});
					}
					let input = self.bind_plan_with_trace(input, trace).await?;
					let exprs = self
						.bind_projection_expr_program_scoped(exprs, &input.schema(), &input.scope)
						.await?;
					let facts = input.facts.preserve_through_project(&exprs);
					let output_schema = schema_for_exprs_with_output_names(&exprs, output_names);
					let scope = input.scope.project(&exprs, &output_schema);

					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Project {
							input: Box::new(input.plan),
							exprs,
							output_schema,
						},
						scope,
						facts,
					))
				}
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let input_schema = input.schema();
					let group_expr = self
						.bind_expr_program_scoped(group_expr, &input_schema, &input.scope)
						.await?;
					let mut bound_aggr_expr = Vec::with_capacity(aggr_expr.len());
					for (function, args) in aggr_expr {
						bound_aggr_expr.push(
							self.bind_aggregate_call_scoped(
								function,
								args,
								&input_schema,
								&input.scope,
							)
							.await?,
						);
					}
					let output_schema = aggregate_schema(&group_expr, &bound_aggr_expr);
					let scope = input.scope.aggregate(&group_expr, &output_schema);

					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Aggregate {
							input: Box::new(input.plan),
							group_expr,
							aggr_expr: bound_aggr_expr,
							output_schema,
						},
						scope,
						PlanOptimizerFacts::composite_unknown(),
					))
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let facts = input.facts.exchange();
					let input_schema = input.schema();
					let key = self
						.bind_expr_program_scoped(key, &input_schema, &input.scope)
						.await?;
					let scope = input.scope.with_schema(input_schema);

					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Exchange {
							input: Box::new(input.plan),
							partition_count: *partition_count,
							key,
						},
						scope,
						facts,
					))
				}
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let facts = PlanOptimizerFacts::composite_with_row(input.facts.row_upper_bound);
					let output_schema = input.schema();
					let order_by = bind_sort_keys(order_by, &output_schema, &input.scope)?;
					let scope = input.scope.with_schema(output_schema.clone());

					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Sort {
							input: Box::new(input.plan),
							order_by,
							limit: *limit,
							offset: *offset,
							output_schema,
						},
						scope,
						facts,
					))
				}
				LogicalPlan::Take { input, limit } => {
					let input = self.bind_plan_with_trace(input, trace).await?;
					let facts = input.facts.take(*limit);
					let output_schema = input.schema();
					let scope = input.scope.with_schema(output_schema.clone());
					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Take {
							input: Box::new(input.plan),
							limit: *limit,
							output_schema,
						},
						scope,
						facts,
					))
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					strategy_intent,
				} => {
					self.bind_join(left, right, *kind, on, *strategy_intent, trace)
						.await
				}
				LogicalPlan::Explain { input } => {
					let plan = self.bind_explain(input).await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::CreateTable {
					name,
					schema,
					row_identity,
					if_not_exists,
					or_replace,
				} => {
					let plan = self
						.bind_create_table(name, schema, row_identity, *if_not_exists, *or_replace)
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::CreateLogicalType {
					name,
					physical,
					refinement,
				} => Ok(ScopedExecutionPlan::empty_scope(
					ExecutionPlan::CreateLogicalType {
						name: name.clone(),
						physical: physical.clone(),
						refinement: refinement.clone(),
					},
				)),
				LogicalPlan::CreateTableAsSelect {
					table,
					if_not_exists,
					or_replace,
					source,
				} => {
					let plan = self
						.bind_create_table_as_select(
							table,
							*if_not_exists,
							*or_replace,
							source,
							trace,
						)
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::DropTable { name, if_exists } => {
					reject_information_schema_target(name)?;
					Ok(ScopedExecutionPlan::empty_scope(ExecutionPlan::DropTable {
						name: name.clone(),
						if_exists: *if_exists,
					}))
				}
				LogicalPlan::Delete {
					table,
					predicate,
					returning,
				} => {
					let plan = self
						.bind_delete(table, predicate.as_ref(), returning.as_ref())
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::Update {
					table,
					assignments,
					predicate,
					returning,
				} => {
					let plan = self
						.bind_update(table, assignments, predicate.as_ref(), returning.as_ref())
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::Insert {
					table,
					columns,
					rows,
					returning,
				} => {
					let plan = self
						.bind_insert(table, columns.as_deref(), rows, returning.as_ref())
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::InsertSelect {
					table,
					columns,
					source,
					returning,
				} => {
					let plan = self
						.bind_insert_select(
							table,
							columns.as_deref(),
							source,
							returning.as_ref(),
							trace,
						)
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
			}
		})
	}

	async fn bind_explain(&self, input: &LogicalPlan) -> Result<ExecutionPlan, BindingError> {
		let (_input_plan, trace) = bind_with_trace(input, self.catalog, self.config).await?;
		optimizer_explain_plan(trace)
	}

	async fn bind_create_table(
		&self,
		name: &TableRef,
		schema: &CreateTableSchema,
		row_identity: &crate::ir::CreateTableRowIdentity,
		if_not_exists: bool,
		or_replace: bool,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(name)?;
		let mut fields = Vec::with_capacity(schema.columns.len());
		for column in &schema.columns {
			let field = match &column.column_type {
				CreateTableColumnType::Physical(data_type) => {
					Field::new(column.name.clone(), data_type.clone(), column.nullable)
				}
				CreateTableColumnType::LogicalName(name) => {
					let spec = self
						.catalog
						.resolve_durable_logical_type_by_name(name)
						.await?;
					field_with_logical_type_metadata(
						column.name.clone(),
						spec.physical,
						column.nullable,
						spec.id,
					)
				}
			};
			fields.push(field);
		}

		Ok(ExecutionPlan::CreateTable {
			name: name.clone(),
			schema: Arc::new(Schema::new(fields)),
			row_identity: row_identity.clone(),
			if_not_exists,
			or_replace,
		})
	}

	async fn bind_expr_program_scoped(
		&self,
		exprs: &[Expr],
		input_schema: &SchemaRef,
		scope: &RelationScope,
	) -> Result<BoundExprProgram, BindingError> {
		self.bind_expr_program_with_scope(exprs, input_schema, Some(scope))
			.await
	}

	async fn bind_expr_program_with_scope(
		&self,
		exprs: &[Expr],
		input_schema: &SchemaRef,
		relation_scope: Option<&RelationScope>,
	) -> Result<BoundExprProgram, BindingError> {
		let logical_casts = self.resolve_logical_casts(exprs).await?;
		let ctx = ExprBindContext {
			config: self.config,
			logical_casts: &logical_casts,
			relation_scope,
		};
		bind_expr_program(exprs, input_schema, &ctx)
	}

	async fn bind_projection_expr_program(
		&self,
		exprs: &[Expr],
		input_schema: &SchemaRef,
	) -> Result<BoundExprProgram, BindingError> {
		self.bind_projection_expr_program_with_scope(exprs, input_schema, None)
			.await
	}

	async fn bind_projection_expr_program_scoped(
		&self,
		exprs: &[Expr],
		input_schema: &SchemaRef,
		scope: &RelationScope,
	) -> Result<BoundExprProgram, BindingError> {
		self.bind_projection_expr_program_with_scope(exprs, input_schema, Some(scope))
			.await
	}

	async fn bind_projection_expr_program_with_scope(
		&self,
		exprs: &[Expr],
		input_schema: &SchemaRef,
		relation_scope: Option<&RelationScope>,
	) -> Result<BoundExprProgram, BindingError> {
		let logical_casts = self.resolve_logical_casts(exprs).await?;
		let ctx = ExprBindContext {
			config: self.config,
			logical_casts: &logical_casts,
			relation_scope,
		};
		bind_expr_program_with_null_policy(
			exprs,
			input_schema,
			&ctx,
			NullRootPolicy::MaterializeResultNull,
		)
	}

	async fn bind_aggregate_call_scoped(
		&self,
		function: &AggregateFunction,
		args: &[Expr],
		input_schema: &SchemaRef,
		scope: &RelationScope,
	) -> Result<BoundAggregateCall, BindingError> {
		self.bind_aggregate_call_with_scope(function, args, input_schema, Some(scope))
			.await
	}

	async fn bind_aggregate_call_with_scope(
		&self,
		function: &AggregateFunction,
		args: &[Expr],
		input_schema: &SchemaRef,
		relation_scope: Option<&RelationScope>,
	) -> Result<BoundAggregateCall, BindingError> {
		let logical_casts = self.resolve_logical_casts(args).await?;
		let ctx = ExprBindContext {
			config: self.config,
			logical_casts: &logical_casts,
			relation_scope,
		};
		bind_aggregate_call(function, args, input_schema, &ctx)
	}

	async fn resolve_logical_casts(
		&self,
		exprs: &[Expr],
	) -> Result<HashMap<String, LogicalTypeSpec>, BindingError> {
		let mut names = HashSet::new();
		for expr in exprs {
			collect_logical_cast_names(expr, &mut names)?;
		}
		let mut resolved = HashMap::with_capacity(names.len());
		for name in names {
			let spec = self
				.catalog
				.resolve_durable_logical_type_by_name(&name)
				.await?;
			resolved.insert(name, spec);
		}
		Ok(resolved)
	}

	async fn bind_create_table_as_select(
		&self,
		table: &TableRef,
		if_not_exists: bool,
		or_replace: bool,
		source: &LogicalPlan,
		trace: &mut OptimizerTrace,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let source = self.bind_plan_with_trace(source, trace).await?;
		let source = source.plan;
		let source_schema = source.schema();
		let schema = infer_ctas_schema(&source_schema)?;

		Ok(ExecutionPlan::CreateTableAsSelect {
			table: table.clone(),
			schema,
			if_not_exists,
			or_replace,
			source: Box::new(source),
		})
	}

	async fn bind_delete(
		&self,
		table: &TableRef,
		predicate: Option<&Expr>,
		returning: Option<&ReturningProjection>,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let binding = self.catalog.resolve_table(table).await?;
		let returning = self
			.bind_returning_projection(returning, &binding.schema)
			.await?;
		let table_scope = RelationScope::scan(table.clone(), &binding.schema);
		let bound_predicate = match predicate {
			Some(expr) => {
				let program = self
					.bind_expr_program_scoped(
						std::slice::from_ref(expr),
						&binding.schema,
						&table_scope,
					)
					.await?;
				require_single_boolean_root(&program, "DELETE WHERE")?;
				Some(program)
			}
			None => None,
		};
		let (bound_predicate, pbc_program, profile_hint) = match bound_predicate {
			Some(program) => {
				let scan = scan_plan(binding.clone(), None);
				let pushdown = predicate_pushdown_plan(program, &scan);
				(
					pushdown.residual_predicate,
					pushdown.pbc_program,
					pushdown.profile_hint.unwrap_or(QueryProfileHint::Scan),
				)
			}
			None => (None, None, QueryProfileHint::Scan),
		};

		Ok(ExecutionPlan::Delete {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema: binding.schema,
			predicate: bound_predicate,
			pbc_program,
			profile_hint,
			returning,
		})
	}

	async fn bind_insert(
		&self,
		table: &TableRef,
		columns: Option<&[String]>,
		rows: &[Vec<InsertLiteral>],
		returning: Option<&ReturningProjection>,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let binding = self.catalog.resolve_table(table).await?;
		if rows.is_empty() {
			return Err(BindingError::UnsupportedExpression {
				message: "INSERT VALUES must contain at least one row".to_string(),
			});
		}
		if rows.len() > TABLE_WRITE_MAX_ROWS {
			return Err(BindingError::UnsupportedExpression {
				message: format!(
					"INSERT row count {} exceeds maximum {TABLE_WRITE_MAX_ROWS}",
					rows.len()
				),
			});
		}

		let schema = binding.schema.clone();
		let returning = self.bind_returning_projection(returning, &schema).await?;
		let schema_fields = schema.fields();
		let target_columns = resolve_insert_target_columns(&schema, columns)?;
		let target_indices = target_columns
			.iter()
			.map(|column| column.0 as usize)
			.collect::<Vec<_>>();
		let expected_arity = target_indices.len();

		let mut bound_rows = Vec::with_capacity(rows.len());
		for (row_index, row) in rows.iter().enumerate() {
			if row.len() != expected_arity {
				return Err(BindingError::TypeMismatch {
					message: format!(
						"INSERT row {} has {} values; expected {expected_arity}",
						row_index + 1,
						row.len()
					),
				});
			}

			let bound_row = if columns.is_none() {
				row.iter()
					.zip(schema_fields.iter())
					.map(|(literal, field)| coerce_insert_literal(literal, field))
					.collect::<Result<Vec<_>, _>>()?
			} else {
				let mut values = vec![None; schema_fields.len()];
				for ((literal, target_index), _position) in
					row.iter().zip(target_indices.iter().copied()).zip(0..)
				{
					values[target_index] =
						Some(coerce_insert_literal(literal, schema.field(target_index))?);
				}
				for (field_index, value) in values.iter_mut().enumerate() {
					if value.is_none() {
						let field = schema.field(field_index);
						if !field.is_nullable() {
							return Err(BindingError::TypeMismatch {
								message: format!(
									"INSERT omits non-nullable column: {}",
									field.name()
								),
							});
						}
						*value = Some(typed_null_for_insert(field)?);
					}
				}
				values
					.into_iter()
					.map(|value| value.expect("all INSERT values filled"))
					.collect()
			};
			bound_rows.push(bound_row);
		}

		Ok(ExecutionPlan::Insert {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema,
			rows: bound_rows,
			returning,
		})
	}

	async fn bind_insert_select(
		&self,
		table: &TableRef,
		columns: Option<&[String]>,
		source: &LogicalPlan,
		returning: Option<&ReturningProjection>,
		trace: &mut OptimizerTrace,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let binding = self.catalog.resolve_table(table).await?;
		let schema = binding.schema.clone();
		let returning = self.bind_returning_projection(returning, &schema).await?;
		let target_columns = resolve_insert_target_columns(&schema, columns)?;
		validate_omitted_insert_target_columns(&schema, &target_columns)?;
		let source = self.bind_plan_with_trace(source, trace).await?;
		let source = source.plan;
		let source_schema = source.schema();
		if source_schema.fields().len() != target_columns.len() {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"INSERT SELECT source has {} columns; expected {}",
					source_schema.fields().len(),
					target_columns.len()
				),
			});
		}

		for (source_index, target_column) in target_columns.iter().enumerate() {
			let source_field = source_schema.field(source_index);
			let target_field = schema.field(target_column.0 as usize);
			if !is_insert_select_coercible(source_field.data_type(), target_field.data_type()) {
				return Err(BindingError::TypeMismatch {
					message: format!(
						"INSERT SELECT source column {} type {:?} is not compatible with target column {} type {:?}",
						source_index + 1,
						source_field.data_type(),
						target_field.name(),
						target_field.data_type()
					),
				});
			}
		}

		Ok(ExecutionPlan::InsertSelect {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema,
			target_columns,
			source: Box::new(source),
			returning,
		})
	}

	async fn bind_update(
		&self,
		table: &TableRef,
		assignments: &[(String, Expr)],
		predicate: Option<&Expr>,
		returning: Option<&ReturningProjection>,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let binding = self.catalog.resolve_table(table).await?;
		let TableRowIdentity::PrimaryKey {
			ordinal: primary_key_ordinal,
			..
		} = &binding.row_identity
		else {
			return Err(BindingError::UnsupportedExpression {
				message: "UPDATE requires a PRIMARY KEY table".to_string(),
			});
		};
		if !binding.primary_key_columns.contains(primary_key_ordinal) {
			return Err(BindingError::UnsupportedExpression {
				message: "UPDATE requires a PRIMARY KEY table".to_string(),
			});
		}

		let schema = binding.schema.clone();
		let table_scope = RelationScope::scan(table.clone(), &schema);
		let returning = self.bind_returning_projection(returning, &schema).await?;
		let mut seen = HashSet::new();
		let mut assigned = HashMap::new();
		for (column, expr) in assignments {
			if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column.as_str()) {
				return Err(BindingError::UnsupportedExpression {
					message: format!(
						"UPDATE target column is reserved for storage metadata: {column}"
					),
				});
			}
			if !seen.insert(column.clone()) {
				return Err(BindingError::UnsupportedExpression {
					message: format!("duplicate UPDATE target column: {column}"),
				});
			}
			let resolved = resolve_column(column, &schema)?;
			if resolved.ordinal == *primary_key_ordinal {
				return Err(BindingError::UnsupportedExpression {
					message: "UPDATE cannot modify the PRIMARY KEY column".to_string(),
				});
			}
			let field = schema.field(resolved.ordinal.0 as usize);
			assigned.insert(resolved.ordinal, typed_update_null_for_target(expr, field)?);
		}

		let mut source_exprs = Vec::with_capacity(schema.fields().len());
		let mut output_names = Vec::with_capacity(schema.fields().len());
		for (field_index, field) in schema.fields().iter().enumerate() {
			let ordinal =
				ColumnOrdinal(u32::try_from(field_index).expect("column ordinal overflow"));
			source_exprs.push(
				assigned
					.remove(&ordinal)
					.unwrap_or_else(|| Expr::column(field.name().clone())),
			);
			output_names.push(Some(field.name().clone()));
		}

		let scan = scan_plan(binding.clone(), None);
		let (project_input, profile_hint) = match predicate {
			Some(predicate) => {
				self.bind_update_source_for_predicate(scan, predicate, &schema, &table_scope)
					.await?
			}
			None => bind_update_source_for_all_rows(scan),
		};
		let exprs = self
			.bind_expr_program_scoped(&source_exprs, &project_input.schema(), &table_scope)
			.await?;
		let output_schema = schema_for_exprs_with_output_names(&exprs, &output_names);

		Ok(ExecutionPlan::Update {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema,
			source: Box::new(ExecutionPlan::Project {
				input: Box::new(project_input),
				exprs,
				output_schema,
			}),
			profile_hint,
			returning,
		})
	}

	async fn bind_information_schema_relation(
		&self,
		table: &TableRef,
		projection: Option<&Vec<String>>,
	) -> Result<ExecutionPlan, BindingError> {
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_LOGICAL_TYPES_TABLE)
		{
			let registry = self.catalog.refreshed_logical_type_registry().await?;
			let rows = registry
				.specs_sorted()
				.into_iter()
				.map(logical_type_spec_row)
				.collect::<Result<Vec<_>, _>>()?;
			return system_table_plan(
				SystemTableKind::InformationSchemaLogicalTypes,
				information_schema_logical_types_schema(),
				rows,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_TABLES_TABLE)
		{
			let descriptors = self.catalog.refreshed_table_schema_descriptors().await?;
			return system_table_plan(
				SystemTableKind::InformationSchemaTables,
				information_schema_tables_schema(),
				information_schema_table_rows(&descriptors),
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_COLUMNS_TABLE)
		{
			let descriptors = self.catalog.refreshed_table_schema_descriptors().await?;
			let logical_types = self.catalog.logical_type_registry();
			return system_table_plan(
				SystemTableKind::InformationSchemaColumns,
				information_schema_columns_schema(),
				information_schema_column_rows(&descriptors, &logical_types)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_SCHEMATA_TABLE)
		{
			let descriptors = self.catalog.refreshed_table_schema_descriptors().await?;
			return system_table_plan(
				SystemTableKind::InformationSchemaSchemata,
				information_schema_schemata_schema(),
				information_schema_schemata_rows(&descriptors),
				projection,
			);
		}
		Err(BindingError::UnsupportedExpression {
			message: format!(
				"unknown information_schema relation: {}; supported relations are {INFORMATION_SCHEMA_LOGICAL_TYPES}, {INFORMATION_SCHEMA_TABLES}, {INFORMATION_SCHEMA_COLUMNS}, {INFORMATION_SCHEMA_SCHEMATA}",
				table_name(table)
			),
		})
	}

	async fn bind_join(
		&self,
		left: &LogicalPlan,
		right: &LogicalPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		strategy_intent: JoinStrategyIntent,
		trace: &mut OptimizerTrace,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let mut left_plan = self.bind_plan_with_trace(left, trace).await?;
		let mut right_plan = self.bind_plan_with_trace(right, trace).await?;
		let mut left_keys = Vec::new();
		let mut right_keys = Vec::new();
		let mut left_resolved_keys = Vec::new();
		let mut right_resolved_keys = Vec::new();

		for (left_expr, right_expr) in on {
			let (left_key, right_key) =
				bind_join_key_pair(left_expr, right_expr, &left_plan.scope, &right_plan.scope)?;
			validate_join_key_types(&left_key, &right_key)?;
			left_keys.push(left_key.column.ordinal);
			right_keys.push(right_key.column.ordinal);
			left_resolved_keys.push(left_key);
			right_resolved_keys.push(right_key);
		}

		let declared_left_width = left_plan.schema().fields().len();
		let declared_right_width = right_plan.schema().fields().len();
		let declared_output_schema = join_schema(kind, &left_plan.schema(), &right_plan.schema());
		let declared_scope = RelationScope::join(
			kind,
			&left_plan.scope,
			&right_plan.scope,
			&declared_output_schema,
		);

		let join_index = trace.join_decisions.len();
		let replay_id = JoinReplayId(join_index as u32);
		let decision = choose_join_strategy(
			replay_id,
			strategy_intent,
			kind,
			JoinInputOptimization {
				facts: left_plan.facts,
				probe_scan_linear: broadcast_probe_is_scan_linear(&left_plan.plan),
			},
			JoinInputOptimization {
				facts: right_plan.facts,
				probe_scan_linear: broadcast_probe_is_scan_linear(&right_plan.plan),
			},
			self.config,
		);
		trace.join_decisions.push(DecisionReceipt {
			target: JoinTraceTarget { join_index },
			strategy: decision.strategy,
			placement: decision.placement,
			reason: decision.reason,
		});
		let force_demoted_sql_broadcast = matches!(
			strategy_intent,
			JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::SqlHint,
			}
		) && self.config.force_hash_join_ids.contains(&replay_id)
			&& decision.strategy == JoinStrategy::HashPartitioned;
		if force_demoted_sql_broadcast {
			left_plan.plan = strip_root_exchange(left_plan.plan);
		}
		if matches!(strategy_intent, JoinStrategyIntent::Auto) || force_demoted_sql_broadcast {
			match decision.strategy {
				JoinStrategy::HashPartitioned => {
					left_plan.plan = exchange_for_join_keys(
						left_plan.plan,
						&left_resolved_keys,
						DEFAULT_AUTO_JOIN_PARTITION_COUNT,
					)?;
					right_plan.plan = exchange_for_join_keys(
						right_plan.plan,
						&right_resolved_keys,
						DEFAULT_AUTO_JOIN_PARTITION_COUNT,
					)?;
				}
				JoinStrategy::Broadcast => match decision.placement {
					JoinPhysicalPlacement::Natural => {
						left_plan.plan =
							exchange_for_join_keys(left_plan.plan, &left_resolved_keys, 1)?;
					}
					JoinPhysicalPlacement::SwappedRightBuild => {
						right_plan.plan =
							exchange_for_join_keys(right_plan.plan, &right_resolved_keys, 1)?;
						std::mem::swap(&mut left_plan, &mut right_plan);
						std::mem::swap(&mut left_keys, &mut right_keys);
						std::mem::swap(&mut left_resolved_keys, &mut right_resolved_keys);
					}
				},
			}
		}

		validate_strategy_shape(
			kind,
			decision.strategy,
			&left_plan.plan,
			&right_plan.plan,
			&left_keys,
			&right_keys,
		)?;
		let physical_output_schema = join_schema(kind, &left_plan.schema(), &right_plan.schema());
		let join_plan = ExecutionPlan::Join {
			replay_id,
			replay_eligibility: broadcast_replay_eligibility(strategy_intent),
			left: Box::new(left_plan.plan),
			right: Box::new(right_plan.plan),
			kind,
			left_keys,
			right_keys,
			strategy: decision.strategy,
			output_schema: physical_output_schema.clone(),
		};
		let plan = match decision.placement {
			JoinPhysicalPlacement::Natural => join_plan,
			JoinPhysicalPlacement::SwappedRightBuild => restore_declared_join_order_project(
				join_plan,
				&physical_output_schema,
				declared_output_schema,
				declared_left_width,
				declared_right_width,
			)?,
		};

		Ok(ScopedExecutionPlan::new_with_facts(
			plan,
			declared_scope,
			PlanOptimizerFacts::composite_unknown(),
		))
	}

	async fn bind_update_source_for_predicate(
		&self,
		scan: ExecutionPlan,
		predicate: &Expr,
		schema: &SchemaRef,
		scope: &RelationScope,
	) -> Result<(ExecutionPlan, QueryProfileHint), BindingError> {
		let bound_predicate = self
			.bind_expr_program_scoped(std::slice::from_ref(predicate), schema, scope)
			.await?;
		require_single_boolean_root(&bound_predicate, "UPDATE WHERE")?;
		let logical_types = self.catalog.logical_type_registry();
		Ok(plan_filter_with_predicate_pushdown(
			scan,
			bound_predicate,
			&logical_types,
		))
	}

	async fn bind_returning_projection(
		&self,
		returning: Option<&ReturningProjection>,
		schema: &SchemaRef,
	) -> Result<Option<BoundReturningProjection>, BindingError> {
		let Some(returning) = returning else {
			return Ok(None);
		};
		let (exprs, output_names) = match returning {
			ReturningProjection::Star => {
				let exprs = schema
					.fields()
					.iter()
					.map(|field| Expr::column(field.name().clone()))
					.collect::<Vec<_>>();
				let output_names = schema
					.fields()
					.iter()
					.map(|field| Some(field.name().clone()))
					.collect::<Vec<_>>();
				(exprs, output_names)
			}
			ReturningProjection::Exprs {
				exprs,
				output_names,
			} => {
				if exprs.len() != output_names.len() {
					return Err(BindingError::UnsupportedExpression {
						message: "RETURNING expression/name length mismatch".to_string(),
					});
				}
				for expr in exprs {
					reject_storage_synthetic_returning_reference(expr)?;
				}
				(exprs.clone(), output_names.clone())
			}
		};
		let exprs = self.bind_projection_expr_program(&exprs, schema).await?;
		let output_schema = schema_for_exprs_with_output_names(&exprs, &output_names);
		Ok(Some(BoundReturningProjection {
			exprs,
			output_schema,
		}))
	}
}

fn bind_alias_column_rename(
	input: ExecutionPlan,
	columns: &[String],
) -> Result<ExecutionPlan, BindingError> {
	if columns.is_empty() {
		return Ok(input);
	}
	let schema = input.schema();
	if columns.len() != schema.fields().len() {
		return Err(BindingError::UnsupportedExpression {
			message: format!(
				"alias column list arity mismatch: expected {}, got {}",
				schema.fields().len(),
				columns.len()
			),
		});
	}

	let mut builder = BoundExprProgramBuilder::new();
	for (idx, field) in schema.fields().iter().enumerate() {
		let logical_type = decode_column_logical_type(field).map_err(|error| {
			BindingError::UnsupportedExpression {
				message: error.to_string(),
			}
		})?;
		let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
		let node = builder.append_column(ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal,
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(field.data_type()),
		});
		if !builder.set_node_logical_type(node, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "alias column rename target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	let exprs = builder.build();
	let output_names = columns.iter().cloned().map(Some).collect::<Vec<_>>();
	let output_schema = schema_for_exprs_with_output_names(&exprs, &output_names);
	Ok(ExecutionPlan::Project {
		input: Box::new(input),
		exprs,
		output_schema,
	})
}

fn bind_update_source_for_all_rows(scan: ExecutionPlan) -> (ExecutionPlan, QueryProfileHint) {
	(scan, QueryProfileHint::Scan)
}

fn reject_storage_synthetic_returning_reference(expr: &Expr) -> Result<(), BindingError> {
	match expr {
		Expr::Column(column)
			if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column.name.0.as_str()) =>
		{
			Err(BindingError::UnsupportedExpression {
				message: format!(
					"RETURNING column is reserved for storage metadata: {}",
					column.name.0
				),
			})
		}
		Expr::Column(_) | Expr::Literal(_) | Expr::UntypedNull => Ok(()),
		Expr::LogicalAscription { expr: inner, .. }
		| Expr::SqlLogicalCast { expr: inner, .. }
		| Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::Like { expr: inner, .. }
		| Expr::TimeBucket(_, inner) => reject_storage_synthetic_returning_reference(inner),
		Expr::InList { expr, list, .. } => {
			reject_storage_synthetic_returning_reference(expr)?;
			for item in list {
				reject_storage_synthetic_returning_reference(item)?;
			}
			Ok(())
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			for (condition, result) in when_then {
				reject_storage_synthetic_returning_reference(condition)?;
				reject_storage_synthetic_returning_reference(result)?;
			}
			if let Some(else_expr) = else_expr {
				reject_storage_synthetic_returning_reference(else_expr)?;
			}
			Ok(())
		}
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			reject_storage_synthetic_returning_reference(left)?;
			reject_storage_synthetic_returning_reference(right)
		}
	}
}

fn collect_logical_cast_names(
	expr: &Expr,
	names: &mut HashSet<String>,
) -> Result<(), BindingError> {
	match expr {
		Expr::Column(_) | Expr::Literal(_) | Expr::UntypedNull => Ok(()),
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast: _,
		} => {
			names.insert(logical_name.clone());
			collect_logical_cast_names(expr, names)
		}
		Expr::LogicalAscription { expr, .. }
		| Expr::Cast(expr, _)
		| Expr::TryCast(expr, _)
		| Expr::Not(expr)
		| Expr::IsNull(expr)
		| Expr::IsNotNull(expr)
		| Expr::Like { expr, .. }
		| Expr::TimeBucket(_, expr) => collect_logical_cast_names(expr, names),
		Expr::InList { expr, list, .. } => {
			collect_logical_cast_names(expr, names)?;
			for item in list {
				collect_logical_cast_names(item, names)?;
			}
			Ok(())
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			for (condition, result) in when_then {
				collect_logical_cast_names(condition, names)?;
				collect_logical_cast_names(result, names)?;
			}
			if let Some(else_expr) = else_expr {
				collect_logical_cast_names(else_expr, names)?;
			}
			Ok(())
		}
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			collect_logical_cast_names(left, names)?;
			collect_logical_cast_names(right, names)
		}
	}
}

fn resolve_insert_target_columns(
	schema: &SchemaRef,
	columns: Option<&[String]>,
) -> Result<Vec<ColumnOrdinal>, BindingError> {
	let schema_fields = schema.fields();
	let target_columns = match columns {
		None => (0..schema_fields.len())
			.map(|index| ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow")))
			.collect::<Vec<_>>(),
		Some(columns) => {
			let mut seen = HashSet::new();
			let mut target_columns = Vec::with_capacity(columns.len());
			for column in columns {
				if !seen.insert(column.clone()) {
					return Err(BindingError::UnsupportedExpression {
						message: format!("duplicate INSERT target column: {column}"),
					});
				}
				target_columns.push(resolve_column(column, schema)?.ordinal);
			}
			target_columns
		}
	};
	Ok(target_columns)
}

fn validate_omitted_insert_target_columns(
	schema: &SchemaRef,
	target_columns: &[ColumnOrdinal],
) -> Result<(), BindingError> {
	for (field_index, field) in schema.fields().iter().enumerate() {
		let ordinal = ColumnOrdinal(u32::try_from(field_index).expect("column ordinal overflow"));
		if !target_columns.contains(&ordinal) && !field.is_nullable() {
			return Err(BindingError::TypeMismatch {
				message: format!("INSERT omits non-nullable column: {}", field.name()),
			});
		}
	}
	Ok(())
}

fn typed_update_null_for_target(expr: &Expr, field: &Field) -> Result<Expr, BindingError> {
	if !is_untyped_null(expr) {
		return Ok(expr.clone());
	}
	let value = typed_null_for_data_type(field.data_type())?;
	Ok(Expr::Literal(value))
}

fn is_untyped_null(expr: &Expr) -> bool {
	matches!(expr, Expr::UntypedNull)
}

fn typed_null_for_data_type(data_type: &DataType) -> Result<ScalarValue, BindingError> {
	match data_type {
		DataType::Boolean => Ok(ScalarValue::Boolean(None)),
		DataType::Int32 => Ok(ScalarValue::Int32(None)),
		DataType::Int64 => Ok(ScalarValue::Int64(None)),
		DataType::Float32 => Ok(ScalarValue::Float32(None)),
		DataType::Float64 => Ok(ScalarValue::Float64(None)),
		DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
		other => Err(BindingError::TypeMismatch {
			message: format!("unsupported NULL target type: {other:?}"),
		}),
	}
}

fn resolve_untyped_null(
	builder: &mut BoundExprProgramBuilder,
	data_type: &DataType,
) -> Result<BoundExprId, BindingError> {
	Ok(builder.append_literal(typed_null_for_data_type(data_type)?))
}

fn materialize_null_for_output_type(
	builder: &mut BoundExprProgramBuilder,
	output_type: &DataType,
) -> Result<BoundExprId, BindingError> {
	if output_type == &DataType::Null {
		Ok(builder.append_null())
	} else {
		resolve_untyped_null(builder, output_type)
	}
}

fn infer_ctas_schema(source_schema: &SchemaRef) -> Result<SchemaRef, BindingError> {
	let mut seen = HashSet::new();
	let mut fields = Vec::with_capacity(source_schema.fields().len());
	for field in source_schema.fields() {
		validate_ctas_column_name(field.name())?;
		if !seen.insert(field.name().clone()) {
			return Err(BindingError::UnsupportedExpression {
				message: format!("duplicate CTAS output column: {}", field.name()),
			});
		}
		if !is_supported_ctas_column_type(field.data_type()) {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"unsupported CTAS output column type for {}: {:?}",
					field.name(),
					field.data_type()
				),
			});
		}
		let logical_type = decode_column_logical_type(field).map_err(|error| {
			BindingError::UnsupportedExpression {
				message: error.to_string(),
			}
		})?;
		fields.push(field_with_logical_type_metadata(
			field.name().clone(),
			field.data_type().clone(),
			field.is_nullable(),
			logical_type,
		));
	}
	Ok(Arc::new(Schema::new(fields)))
}

fn validate_ctas_column_name(name: &str) -> Result<(), BindingError> {
	if name.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "CTAS output column name must not be empty".to_string(),
		});
	}
	if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&name) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("CTAS output column name is reserved for storage metadata: {name}"),
		});
	}
	Ok(())
}

fn is_supported_ctas_column_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
	)
}

fn bind_sort_keys(
	order_by: &[SortExpr],
	output_schema: &SchemaRef,
	scope: &RelationScope,
) -> Result<Vec<ExecutionSortKey>, BindingError> {
	if order_by.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "ORDER BY requires at least one key".to_string(),
		});
	}

	let mut keys = Vec::with_capacity(order_by.len() + output_schema.fields().len());
	let mut user_ordinals = BTreeSet::new();
	for key in order_by {
		let ordinal = bind_sort_key_ordinal(&key.expr, output_schema, scope)?;
		user_ordinals.insert(ordinal);
		keys.push(ExecutionSortKey {
			ordinal,
			descending: key.descending,
			nulls_first: key.nulls_first,
		});
	}

	for ordinal in output_columns(output_schema.fields().len()) {
		if !user_ordinals.contains(&ordinal) {
			keys.push(ExecutionSortKey {
				ordinal,
				descending: false,
				nulls_first: false,
			});
		}
	}

	Ok(keys)
}

fn bind_sort_key_ordinal(
	expr: &Expr,
	output_schema: &SchemaRef,
	scope: &RelationScope,
) -> Result<ColumnOrdinal, BindingError> {
	match expr {
		Expr::Column(column_ref) => {
			if column_ref.qualifier.is_some() {
				return Ok(resolve_qualified_column(column_ref, scope)?.column.ordinal);
			}
			Ok(resolve_column(&column_ref.name.0, output_schema)?.ordinal)
		}
		Expr::Literal(ScalarValue::Int32(Some(value))) => {
			bind_sort_ordinal_literal(i64::from(*value), output_schema)
		}
		Expr::Literal(ScalarValue::Int64(Some(value))) => {
			bind_sort_ordinal_literal(*value, output_schema)
		}
		Expr::Literal(_) => Err(BindingError::UnsupportedExpression {
			message: "ORDER BY ordinals must be positive integers".to_string(),
		}),
		_ => Err(BindingError::UnsupportedExpression {
			message: "ORDER BY keys must reference output columns or positive ordinals".to_string(),
		}),
	}
}

fn bind_sort_ordinal_literal(
	value: i64,
	output_schema: &SchemaRef,
) -> Result<ColumnOrdinal, BindingError> {
	if value <= 0 {
		return Err(BindingError::UnsupportedExpression {
			message: "ORDER BY ordinal must be positive".to_string(),
		});
	}
	let zero_based =
		usize::try_from(value - 1).map_err(|_| BindingError::UnsupportedExpression {
			message: "ORDER BY ordinal is out of range".to_string(),
		})?;
	if zero_based >= output_schema.fields().len() {
		return Err(BindingError::UnknownColumn {
			column: format!("ORDER BY ordinal {value}"),
		});
	}
	Ok(ColumnOrdinal(
		u32::try_from(zero_based).expect("column ordinal overflow"),
	))
}

fn compile_pbc(predicate: &BoundExprProgram) -> Option<PbcProgram> {
	PbcCompiler::new()
		.compile(Some(predicate), None)
		.ok()
		.map(PbcProgram)
}

const BOOLEAN_FACTOR_MAX_NODES: usize = 256;
const BOOLEAN_FACTOR_MAX_OR_BRANCHES: usize = 16;
const BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH: usize = 16;
const BOOLEAN_FACTOR_MAX_CANDIDATES: usize = 8;
const STATIC_ELISION_MAX_AND_CONJUNCTS: usize = BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH;

fn attach_pbc_program(mut plan: ExecutionPlan, program: PbcProgram) -> (ExecutionPlan, bool) {
	match &mut plan {
		ExecutionPlan::Scan { pbc_program, .. } => {
			*pbc_program = Some(program);
			(plan, true)
		}
		_ => (plan, false),
	}
}

struct PredicatePushdownPlan {
	pushed_predicate: Option<BoundExprProgram>,
	residual_predicate: Option<BoundExprProgram>,
	pbc_program: Option<PbcProgram>,
	profile_hint: Option<QueryProfileHint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticFilterClass {
	AlwaysPass,
	AlwaysReject,
	Unknown,
}

fn plan_filter_with_predicate_pushdown(
	mut input: ExecutionPlan,
	predicate: BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> (ExecutionPlan, QueryProfileHint) {
	let original_predicate = predicate.clone();
	let fallback_profile = scan_profile_hint(&original_predicate, &input);
	if matches!(
		static_filter_class(&input, &original_predicate, logical_types),
		StaticFilterClass::AlwaysReject
	) {
		return (
			ExecutionPlan::EmptyRelation {
				schema: input.schema(),
			},
			fallback_profile.unwrap_or(QueryProfileHint::Scan),
		);
	}

	let pushdown = predicate_pushdown_plan(predicate, &input);
	let profile_hint = pushdown
		.profile_hint
		.or(fallback_profile)
		.unwrap_or(QueryProfileHint::Scan);
	apply_scan_profile_hint(&mut input, Some(profile_hint));

	if let Some(pbc_program) = pushdown.pbc_program {
		let (filter_input, attached) = attach_pbc_program(input, pbc_program);
		if attached {
			let (predicate, disposition) = match pushdown.residual_predicate {
				Some(residual_predicate) => {
					(residual_predicate, FilterDisposition::EvaluateInToron)
				}
				None => (
					pushdown
						.pushed_predicate
						.expect("PBC program requires pushed predicate"),
					FilterDisposition::FullyPushed,
				),
			};
			return (
				ExecutionPlan::Filter {
					input: Box::new(filter_input),
					predicate,
					disposition,
				},
				profile_hint,
			);
		}

		let mut filter_input = filter_input;
		let fallback_profile = fallback_profile.unwrap_or(QueryProfileHint::Scan);
		apply_scan_profile_hint(&mut filter_input, Some(fallback_profile));
		return (
			ExecutionPlan::Filter {
				input: Box::new(filter_input),
				predicate: original_predicate,
				disposition: FilterDisposition::EvaluateInToron,
			},
			fallback_profile,
		);
	}

	(
		ExecutionPlan::Filter {
			input: Box::new(input),
			predicate: pushdown.residual_predicate.unwrap_or(original_predicate),
			disposition: FilterDisposition::EvaluateInToron,
		},
		profile_hint,
	)
}

fn static_filter_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	if let Some(class) = static_filter_and_class(input, predicate, logical_types) {
		return class;
	}
	static_filter_atom_class(input, predicate, logical_types)
}

fn static_filter_atom_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	if structural_non_nullable_is_null(predicate) {
		return StaticFilterClass::AlwaysReject;
	}
	if matches!(
		refinement_static_filter_class(input, predicate, logical_types),
		StaticFilterClass::AlwaysReject
	) {
		return StaticFilterClass::AlwaysReject;
	}
	if predicate.single_root().is_some() && !program_has_column_references(predicate) {
		return closed_root_filter_class(predicate);
	}
	StaticFilterClass::Unknown
}

fn static_filter_and_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> Option<StaticFilterClass> {
	let root = predicate.single_root()?;
	if !matches!(
		predicate.node(root),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			..
		})
	) {
		return None;
	}
	let Ok(node_count) = count_bound_subgraph_nodes(predicate, root) else {
		return Some(StaticFilterClass::Unknown);
	};
	if node_count > BOOLEAN_FACTOR_MAX_NODES {
		return Some(StaticFilterClass::Unknown);
	}

	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);
	if conjuncts.len() > STATIC_ELISION_MAX_AND_CONJUNCTS {
		return Some(StaticFilterClass::Unknown);
	}

	let mut found_reject = false;
	for conjunct in conjuncts {
		let Ok(isolated) = isolate_bound_subgraph(predicate, conjunct) else {
			return Some(StaticFilterClass::Unknown);
		};
		match static_filter_atom_class(input, &isolated, logical_types) {
			StaticFilterClass::AlwaysReject => found_reject = true,
			StaticFilterClass::AlwaysPass | StaticFilterClass::Unknown => {
				if !static_elision_error_free_conjunct(&isolated) {
					return Some(StaticFilterClass::Unknown);
				}
			}
		}
	}

	if found_reject {
		Some(StaticFilterClass::AlwaysReject)
	} else {
		Some(StaticFilterClass::Unknown)
	}
}

fn static_elision_error_free_conjunct(predicate: &BoundExprProgram) -> bool {
	let Some(root) = predicate.single_root() else {
		return false;
	};
	if !program_has_column_references(predicate) {
		return !matches!(
			closed_root_filter_class(predicate),
			StaticFilterClass::Unknown
		);
	}
	static_elision_error_free_node(predicate, root)
}

fn static_elision_error_free_node(predicate: &BoundExprProgram, id: BoundExprId) -> bool {
	match predicate.node(id) {
		Some(BoundExprNode::Column(_)) | Some(BoundExprNode::Literal(_)) => true,
		Some(BoundExprNode::Binary {
			op:
				BoundBinaryOp::Eq
				| BoundBinaryOp::Neq
				| BoundBinaryOp::Lt
				| BoundBinaryOp::Lte
				| BoundBinaryOp::Gt
				| BoundBinaryOp::Gte,
			left,
			right,
		}) => {
			static_elision_error_free_operand(predicate, *left)
				&& static_elision_error_free_operand(predicate, *right)
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			static_elision_error_free_node(predicate, *left)
				&& static_elision_error_free_node(predicate, *right)
		}
		Some(BoundExprNode::Unary {
			op: BoundUnaryOp::IsNull | BoundUnaryOp::IsNotNull,
			input,
		}) => static_elision_error_free_operand(predicate, *input),
		_ => false,
	}
}

fn static_elision_error_free_operand(predicate: &BoundExprProgram, id: BoundExprId) -> bool {
	matches!(
		predicate.node(id),
		Some(BoundExprNode::Column(_)) | Some(BoundExprNode::Literal(_))
	)
}

fn structural_non_nullable_is_null(predicate: &BoundExprProgram) -> bool {
	let Some(root) = predicate.single_root() else {
		return false;
	};
	let Some(BoundExprNode::Unary {
		op: BoundUnaryOp::IsNull,
		input,
	}) = predicate.node(root)
	else {
		return false;
	};
	let Some(BoundExprNode::Column(column)) = predicate.node(*input) else {
		return false;
	};
	!column.nullable
}

fn program_has_column_references(predicate: &BoundExprProgram) -> bool {
	predicate
		.nodes
		.iter()
		.any(|node| matches!(node, BoundExprNode::Column(_)))
}

fn closed_root_filter_class(predicate: &BoundExprProgram) -> StaticFilterClass {
	let schema = Arc::new(Schema::empty());
	let options = RecordBatchOptions::new().with_row_count(Some(1));
	let Ok(batch) = RecordBatch::try_new_with_options(schema, Vec::new(), &options) else {
		return StaticFilterClass::Unknown;
	};
	let Ok(outputs) = Executor::evaluate_bound_program_without_storage(predicate, &batch) else {
		return StaticFilterClass::Unknown;
	};
	let [output] = outputs.as_slice() else {
		return StaticFilterClass::Unknown;
	};
	let Some(mask) = output.as_any().downcast_ref::<BooleanArray>() else {
		return StaticFilterClass::Unknown;
	};
	if mask.len() != 1 {
		return StaticFilterClass::Unknown;
	}
	if mask.is_null(0) || !mask.value(0) {
		StaticFilterClass::AlwaysReject
	} else {
		StaticFilterClass::AlwaysPass
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefinementCmpOp {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

impl RefinementCmpOp {
	fn flip(self) -> Self {
		match self {
			Self::Eq => Self::Eq,
			Self::Ne => Self::Ne,
			Self::Lt => Self::Gt,
			Self::Le => Self::Ge,
			Self::Gt => Self::Lt,
			Self::Ge => Self::Le,
		}
	}
}

#[derive(Debug, Clone)]
struct StoredRefinementAtom {
	op: RefinementCmpOp,
	literal: CarrierLiteral,
	refinement: LogicalTypeRefinementPredicate,
}

const STORED_REFINEMENT_RECEIPT_MAX_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy)]
struct StoredRefinementReceipt<'a> {
	field: &'a Field,
}

fn refinement_static_filter_class(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> StaticFilterClass {
	let Some(atom) = stored_scan_refinement_atom(input, predicate, logical_types) else {
		return StaticFilterClass::Unknown;
	};
	let Some(refinement_domain) = domain_from_refinement(&atom.refinement) else {
		return StaticFilterClass::Unknown;
	};
	let Some(filter_domain) = domain_from_comparison(atom.op, atom.literal) else {
		return StaticFilterClass::Unknown;
	};
	let Some(intersection) = refinement_domain.intersect(&filter_domain) else {
		return StaticFilterClass::Unknown;
	};
	if intersection.is_empty() {
		StaticFilterClass::AlwaysReject
	} else {
		StaticFilterClass::Unknown
	}
}

fn stored_scan_refinement_atom(
	input: &ExecutionPlan,
	predicate: &BoundExprProgram,
	logical_types: &LogicalTypeRegistry,
) -> Option<StoredRefinementAtom> {
	let root = predicate.single_root()?;
	let BoundExprNode::Binary { op, left, right } = predicate.node(root)? else {
		return None;
	};
	let op = filter_refinement_cmp_op(op)?;
	let (column_node, literal_node, op) = match (predicate.node(*left)?, predicate.node(*right)?) {
		(BoundExprNode::Column(_), BoundExprNode::Literal(_)) => (*left, *right, op),
		(BoundExprNode::Literal(_), BoundExprNode::Column(_)) => (*right, *left, op.flip()),
		_ => return None,
	};
	let BoundExprNode::Column(column) = predicate.node(column_node)? else {
		return None;
	};
	let receipt = stored_refinement_receipt(input, column.ordinal, 0)?;
	let field = receipt.field;
	let logical_type = predicate.node_logical_type(column_node)?;
	if logical_type.is_naked() {
		return None;
	}
	let spec = logical_types.spec(logical_type)?;
	if field.data_type() != &spec.physical || column.data_type != spec.physical {
		return None;
	}
	let refinement = spec.refinement?;
	let literal = predicate_carrier_literal(predicate, literal_node, field.data_type())?;
	Some(StoredRefinementAtom {
		op,
		literal,
		refinement,
	})
}

fn transparent_project_child_ordinal(
	exprs: &BoundExprProgram,
	ordinal: ColumnOrdinal,
) -> Option<ColumnOrdinal> {
	let root = *exprs.roots.get(ordinal.0 as usize)?;
	match exprs.node(root)? {
		BoundExprNode::Column(column) => Some(column.ordinal),
		_ => None,
	}
}

fn stored_refinement_receipt<'a>(
	input: &'a ExecutionPlan,
	ordinal: ColumnOrdinal,
	depth: usize,
) -> Option<StoredRefinementReceipt<'a>> {
	if depth > STORED_REFINEMENT_RECEIPT_MAX_DEPTH {
		return None;
	}
	match input {
		ExecutionPlan::Scan {
			projection: None,
			schema,
			..
		} => {
			let field = schema.fields().get(ordinal.0 as usize)?.as_ref();
			Some(StoredRefinementReceipt { field })
		}
		ExecutionPlan::Project { input, exprs, .. } => {
			let child = transparent_project_child_ordinal(exprs, ordinal)?;
			let input_schema = input.schema();
			let child_field = input_schema.fields().get(child.0 as usize)?;
			let child_logical_type = decode_column_logical_type(child_field).ok()?;
			if exprs.root_logical_type(ordinal.0 as usize)? != child_logical_type {
				return None;
			}
			stored_refinement_receipt(input, child, depth + 1)
		}
		_ => None,
	}
}

fn filter_refinement_cmp_op(op: &BoundBinaryOp) -> Option<RefinementCmpOp> {
	match op {
		BoundBinaryOp::Eq => Some(RefinementCmpOp::Eq),
		BoundBinaryOp::Neq => Some(RefinementCmpOp::Ne),
		BoundBinaryOp::Lt => Some(RefinementCmpOp::Lt),
		BoundBinaryOp::Lte => Some(RefinementCmpOp::Le),
		BoundBinaryOp::Gt => Some(RefinementCmpOp::Gt),
		BoundBinaryOp::Gte => Some(RefinementCmpOp::Ge),
		_ => None,
	}
}

fn predicate_carrier_literal(
	predicate: &BoundExprProgram,
	literal_node: BoundExprId,
	carrier: &DataType,
) -> Option<CarrierLiteral> {
	let BoundExprNode::Literal(literal) = predicate.node(literal_node)? else {
		return None;
	};
	let value = predicate.literals.get(*literal as usize)?;
	carrier_literal_from_scalar(value, carrier)
}

#[derive(Debug, Clone, PartialEq)]
enum CarrierLiteral {
	Int32(i32),
	Int64(i64),
	Float32(FiniteF32),
	Float64(FiniteF64),
	Utf8(String),
	Boolean(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct FiniteF32(f32);

impl FiniteF32 {
	fn new(value: f32) -> Option<Self> {
		value.is_finite().then_some(Self(value))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
struct FiniteF64(f64);

impl FiniteF64 {
	fn new(value: f64) -> Option<Self> {
		value.is_finite().then_some(Self(value))
	}
}

fn carrier_literal_from_scalar(value: &ScalarValue, carrier: &DataType) -> Option<CarrierLiteral> {
	match (carrier, value) {
		(DataType::Int32, ScalarValue::Int32(Some(value))) => Some(CarrierLiteral::Int32(*value)),
		(DataType::Int64, ScalarValue::Int64(Some(value))) => Some(CarrierLiteral::Int64(*value)),
		(DataType::Float32, ScalarValue::Float32(Some(value))) => {
			FiniteF32::new(*value).map(CarrierLiteral::Float32)
		}
		(DataType::Float64, ScalarValue::Float64(Some(value))) => {
			FiniteF64::new(*value).map(CarrierLiteral::Float64)
		}
		(DataType::Utf8, ScalarValue::Utf8(Some(value))) => {
			Some(CarrierLiteral::Utf8(value.clone()))
		}
		(DataType::Boolean, ScalarValue::Boolean(Some(value))) => {
			Some(CarrierLiteral::Boolean(*value))
		}
		_ => None,
	}
}

fn carrier_literal_from_refinement_value(
	value: &LogicalTypeRefinementValue,
) -> Option<CarrierLiteral> {
	match value {
		LogicalTypeRefinementValue::Value => None,
		LogicalTypeRefinementValue::Int64(value) => Some(CarrierLiteral::Int64(*value)),
		LogicalTypeRefinementValue::Int32(value) => Some(CarrierLiteral::Int32(*value)),
		LogicalTypeRefinementValue::Float64(bits) => {
			FiniteF64::new(f64::from_bits(*bits)).map(CarrierLiteral::Float64)
		}
		LogicalTypeRefinementValue::Float32(bits) => {
			FiniteF32::new(f32::from_bits(*bits)).map(CarrierLiteral::Float32)
		}
		LogicalTypeRefinementValue::Utf8(value) => Some(CarrierLiteral::Utf8(value.clone())),
		LogicalTypeRefinementValue::Boolean(value) => Some(CarrierLiteral::Boolean(*value)),
	}
}

#[derive(Debug, Clone)]
struct DomainBound<T> {
	value: T,
	inclusive: bool,
}

#[derive(Debug, Clone)]
struct OrderedDomain<T> {
	lower: Option<DomainBound<T>>,
	upper: Option<DomainBound<T>>,
	excluded: Vec<T>,
}

impl<T> OrderedDomain<T>
where
	T: Clone + PartialEq + PartialOrd,
{
	fn point(value: T) -> Self {
		Self {
			lower: Some(DomainBound {
				value: value.clone(),
				inclusive: true,
			}),
			upper: Some(DomainBound {
				value,
				inclusive: true,
			}),
			excluded: Vec::new(),
		}
	}

	fn lower(value: T, inclusive: bool) -> Self {
		Self {
			lower: Some(DomainBound { value, inclusive }),
			upper: None,
			excluded: Vec::new(),
		}
	}

	fn upper(value: T, inclusive: bool) -> Self {
		Self {
			lower: None,
			upper: Some(DomainBound { value, inclusive }),
			excluded: Vec::new(),
		}
	}

	fn excluded(value: T) -> Self {
		Self {
			lower: None,
			upper: None,
			excluded: vec![value],
		}
	}

	fn intersect(&self, other: &Self) -> Self {
		let lower = intersect_lower_bound(&self.lower, &other.lower);
		let upper = intersect_upper_bound(&self.upper, &other.upper);
		let mut excluded = self.excluded.clone();
		for value in &other.excluded {
			if !excluded.iter().any(|existing| existing == value) {
				excluded.push(value.clone());
			}
		}
		Self {
			lower,
			upper,
			excluded,
		}
	}

	fn is_empty(&self) -> bool {
		let (Some(lower), Some(upper)) = (&self.lower, &self.upper) else {
			return false;
		};
		match lower.value.partial_cmp(&upper.value) {
			Some(Ordering::Greater) | None => true,
			Some(Ordering::Less) => false,
			Some(Ordering::Equal) => {
				!lower.inclusive
					|| !upper.inclusive
					|| self.excluded.iter().any(|value| value == &lower.value)
			}
		}
	}
}

fn intersect_lower_bound<T>(
	left: &Option<DomainBound<T>>,
	right: &Option<DomainBound<T>>,
) -> Option<DomainBound<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match (left, right) {
		(None, None) => None,
		(Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
		(Some(left), Some(right)) => match left.value.partial_cmp(&right.value) {
			Some(Ordering::Greater) => Some(left.clone()),
			Some(Ordering::Less) => Some(right.clone()),
			Some(Ordering::Equal) => Some(DomainBound {
				value: left.value.clone(),
				inclusive: left.inclusive && right.inclusive,
			}),
			None => Some(DomainBound {
				value: left.value.clone(),
				inclusive: false,
			}),
		},
	}
}

fn intersect_upper_bound<T>(
	left: &Option<DomainBound<T>>,
	right: &Option<DomainBound<T>>,
) -> Option<DomainBound<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match (left, right) {
		(None, None) => None,
		(Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
		(Some(left), Some(right)) => match left.value.partial_cmp(&right.value) {
			Some(Ordering::Greater) => Some(right.clone()),
			Some(Ordering::Less) => Some(left.clone()),
			Some(Ordering::Equal) => Some(DomainBound {
				value: left.value.clone(),
				inclusive: left.inclusive && right.inclusive,
			}),
			None => Some(DomainBound {
				value: left.value.clone(),
				inclusive: false,
			}),
		},
	}
}

#[derive(Debug, Clone)]
struct Utf8Domain {
	only: Option<String>,
	excluded: Vec<String>,
}

impl Utf8Domain {
	fn only(value: String) -> Self {
		Self {
			only: Some(value),
			excluded: Vec::new(),
		}
	}

	fn excluding(value: String) -> Self {
		Self {
			only: None,
			excluded: vec![value],
		}
	}

	fn intersect(&self, other: &Self) -> Option<Self> {
		let only = match (&self.only, &other.only) {
			(Some(left), Some(right)) if left == right => Some(left.clone()),
			(Some(_), Some(_)) => return None,
			(Some(value), None) | (None, Some(value)) => Some(value.clone()),
			(None, None) => None,
		};
		let mut excluded = self.excluded.clone();
		for value in &other.excluded {
			if !excluded.iter().any(|existing| existing == value) {
				excluded.push(value.clone());
			}
		}
		if let Some(value) = &only
			&& excluded.iter().any(|excluded| excluded == value)
		{
			return None;
		}
		Some(Self { only, excluded })
	}
}

#[derive(Debug, Clone)]
struct BooleanDomain {
	allow_false: bool,
	allow_true: bool,
}

impl BooleanDomain {
	fn only(value: bool) -> Self {
		Self {
			allow_false: !value,
			allow_true: value,
		}
	}

	fn excluding(value: bool) -> Self {
		Self {
			allow_false: value,
			allow_true: !value,
		}
	}

	fn intersect(&self, other: &Self) -> Self {
		Self {
			allow_false: self.allow_false && other.allow_false,
			allow_true: self.allow_true && other.allow_true,
		}
	}

	fn is_empty(&self) -> bool {
		!self.allow_false && !self.allow_true
	}
}

#[derive(Debug, Clone)]
enum CarrierDomain {
	Empty,
	Int32(OrderedDomain<i32>),
	Int64(OrderedDomain<i64>),
	Float32(OrderedDomain<FiniteF32>),
	Float64(OrderedDomain<FiniteF64>),
	Utf8(Utf8Domain),
	Boolean(BooleanDomain),
}

impl CarrierDomain {
	fn intersect(&self, other: &Self) -> Option<Self> {
		match (self, other) {
			(Self::Empty, _) | (_, Self::Empty) => Some(Self::Empty),
			(Self::Int32(left), Self::Int32(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Int32(domain))
				}
			}
			(Self::Int64(left), Self::Int64(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Int64(domain))
				}
			}
			(Self::Float32(left), Self::Float32(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Float32(domain))
				}
			}
			(Self::Float64(left), Self::Float64(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Float64(domain))
				}
			}
			(Self::Utf8(left), Self::Utf8(right)) => {
				left.intersect(right).map(Self::Utf8).or(Some(Self::Empty))
			}
			(Self::Boolean(left), Self::Boolean(right)) => {
				let domain = left.intersect(right);
				if domain.is_empty() {
					Some(Self::Empty)
				} else {
					Some(Self::Boolean(domain))
				}
			}
			_ => None,
		}
	}

	fn is_empty(&self) -> bool {
		matches!(self, Self::Empty)
	}
}

fn domain_from_refinement(predicate: &LogicalTypeRefinementPredicate) -> Option<CarrierDomain> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => {
			let (op, literal) = refinement_comparison_literal(left, *op, right)?;
			let literal = carrier_literal_from_refinement_value(literal)?;
			domain_from_comparison(op, literal)
		}
		LogicalTypeRefinementPredicate::And(predicates) => {
			let mut domain: Option<CarrierDomain> = None;
			for predicate in predicates {
				let child = domain_from_refinement(predicate)?;
				domain = Some(match domain {
					None => child,
					Some(existing) => existing.intersect(&child)?,
				});
				if matches!(domain, Some(CarrierDomain::Empty)) {
					break;
				}
			}
			domain
		}
	}
}

fn refinement_comparison_literal<'a>(
	left: &'a LogicalTypeRefinementValue,
	op: LogicalTypeRefinementComparisonOp,
	right: &'a LogicalTypeRefinementValue,
) -> Option<(RefinementCmpOp, &'a LogicalTypeRefinementValue)> {
	let op = refinement_cmp_op(op);
	match (left, right) {
		(LogicalTypeRefinementValue::Value, literal)
			if !matches!(literal, LogicalTypeRefinementValue::Value) =>
		{
			Some((op, literal))
		}
		(literal, LogicalTypeRefinementValue::Value)
			if !matches!(literal, LogicalTypeRefinementValue::Value) =>
		{
			Some((op.flip(), literal))
		}
		_ => None,
	}
}

fn refinement_cmp_op(op: LogicalTypeRefinementComparisonOp) -> RefinementCmpOp {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => RefinementCmpOp::Eq,
		LogicalTypeRefinementComparisonOp::Ne => RefinementCmpOp::Ne,
		LogicalTypeRefinementComparisonOp::Lt => RefinementCmpOp::Lt,
		LogicalTypeRefinementComparisonOp::Le => RefinementCmpOp::Le,
		LogicalTypeRefinementComparisonOp::Gt => RefinementCmpOp::Gt,
		LogicalTypeRefinementComparisonOp::Ge => RefinementCmpOp::Ge,
	}
}

fn domain_from_comparison(op: RefinementCmpOp, literal: CarrierLiteral) -> Option<CarrierDomain> {
	match literal {
		CarrierLiteral::Int32(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Int32)
		}
		CarrierLiteral::Int64(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Int64)
		}
		CarrierLiteral::Float32(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Float32)
		}
		CarrierLiteral::Float64(value) => {
			ordered_domain_from_comparison(op, value).map(CarrierDomain::Float64)
		}
		CarrierLiteral::Utf8(value) => match op {
			RefinementCmpOp::Eq => Some(CarrierDomain::Utf8(Utf8Domain::only(value))),
			RefinementCmpOp::Ne => Some(CarrierDomain::Utf8(Utf8Domain::excluding(value))),
			_ => None,
		},
		CarrierLiteral::Boolean(value) => match op {
			RefinementCmpOp::Eq => Some(CarrierDomain::Boolean(BooleanDomain::only(value))),
			RefinementCmpOp::Ne => Some(CarrierDomain::Boolean(BooleanDomain::excluding(value))),
			_ => None,
		},
	}
}

fn ordered_domain_from_comparison<T>(op: RefinementCmpOp, value: T) -> Option<OrderedDomain<T>>
where
	T: Clone + PartialEq + PartialOrd,
{
	match op {
		RefinementCmpOp::Eq => Some(OrderedDomain::point(value)),
		RefinementCmpOp::Ne => Some(OrderedDomain::excluded(value)),
		RefinementCmpOp::Lt => Some(OrderedDomain::upper(value, false)),
		RefinementCmpOp::Le => Some(OrderedDomain::upper(value, true)),
		RefinementCmpOp::Gt => Some(OrderedDomain::lower(value, false)),
		RefinementCmpOp::Ge => Some(OrderedDomain::lower(value, true)),
	}
}

fn predicate_pushdown_plan(
	predicate: BoundExprProgram,
	input: &ExecutionPlan,
) -> PredicatePushdownPlan {
	let fallback_profile = scan_profile_hint(&predicate, input);
	let Some((pushed_predicate, residual_predicate)) = partition_bound_predicate(&predicate) else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: Some(predicate),
			pbc_program: None,
			profile_hint: fallback_profile,
		};
	};

	let Some(pushed_predicate) = pushed_predicate else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: residual_predicate.or(Some(predicate)),
			pbc_program: None,
			profile_hint: fallback_profile,
		};
	};

	let Some(pbc_program) = compile_pbc(&pushed_predicate) else {
		return PredicatePushdownPlan {
			pushed_predicate: None,
			residual_predicate: Some(predicate),
			pbc_program: None,
			profile_hint: fallback_profile,
		};
	};
	let profile_hint = scan_profile_hint(&pushed_predicate, input);

	PredicatePushdownPlan {
		pushed_predicate: Some(pushed_predicate),
		residual_predicate,
		pbc_program: Some(pbc_program),
		profile_hint,
	}
}

fn partition_bound_predicate(
	predicate: &BoundExprProgram,
) -> Option<(Option<BoundExprProgram>, Option<BoundExprProgram>)> {
	let root = predicate.single_root()?;
	match split_boolean_tree(predicate, root) {
		Ok(split) => Some((split.pushed, split.residual)),
		Err(BooleanSplitError::OverBudget) => legacy_top_level_and_partition(predicate),
		Err(BooleanSplitError::Malformed) => None,
	}
}

#[derive(Debug, Clone)]
struct BooleanPredicateSplit {
	pushed: Option<BoundExprProgram>,
	residual: Option<BoundExprProgram>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BooleanSplitError {
	Malformed,
	OverBudget,
}

fn split_boolean_tree(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BooleanPredicateSplit, BooleanSplitError> {
	let node_count = count_bound_subgraph_nodes(source, root)?;
	if node_count > BOOLEAN_FACTOR_MAX_NODES {
		return Err(BooleanSplitError::OverBudget);
	}

	let whole = isolate_bound_subgraph(source, root)?;
	if compile_pbc(&whole).is_some() {
		return Ok(BooleanPredicateSplit {
			pushed: Some(whole),
			residual: None,
		});
	}

	match source.node(root).ok_or(BooleanSplitError::Malformed)? {
		BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		} => {
			let left = split_boolean_tree(source, *left)?;
			let right = split_boolean_tree(source, *right)?;
			Ok(BooleanPredicateSplit {
				pushed: combine_bound_programs_as_and(
					[left.pushed, right.pushed]
						.into_iter()
						.flatten()
						.collect::<Vec<_>>(),
				)?,
				residual: combine_bound_programs_as_and(
					[left.residual, right.residual]
						.into_iter()
						.flatten()
						.collect::<Vec<_>>(),
				)?,
			})
		}
		BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		} => {
			if let Some(split) = try_extract_common_or_factors(source, root)? {
				return Ok(split);
			}
			Ok(BooleanPredicateSplit {
				pushed: None,
				residual: Some(isolate_bound_subgraph(source, root)?),
			})
		}
		BoundExprNode::Unary {
			op: BoundUnaryOp::Not,
			input,
		} => {
			if let Some(split) = try_extract_not_or_branch_negations(source, root, *input)? {
				return Ok(split);
			}
			Ok(BooleanPredicateSplit {
				pushed: None,
				residual: Some(isolate_bound_subgraph(source, root)?),
			})
		}
		_ => Ok(BooleanPredicateSplit {
			pushed: None,
			residual: Some(isolate_bound_subgraph(source, root)?),
		}),
	}
}

fn legacy_top_level_and_partition(
	predicate: &BoundExprProgram,
) -> Option<(Option<BoundExprProgram>, Option<BoundExprProgram>)> {
	let root = predicate.single_root()?;
	let mut conjuncts = Vec::new();
	collect_top_level_bound_and(predicate, root, &mut conjuncts);

	let mut pushed = Vec::new();
	let mut residual = Vec::new();
	for conjunct in conjuncts {
		let isolated = combine_bound_conjuncts(predicate, &[conjunct])?;
		if compile_pbc(&isolated).is_some() {
			pushed.push(conjunct);
		} else {
			residual.push(conjunct);
		}
	}

	Some((
		combine_bound_conjuncts(predicate, &pushed),
		combine_bound_conjuncts(predicate, &residual),
	))
}

fn try_extract_common_or_factors(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	let mut branches = Vec::new();
	collect_bound_or_branches(source, root, &mut branches)?;
	if branches.len() < 2 {
		return Ok(None);
	}

	let mut branch_terms = Vec::with_capacity(branches.len());
	for branch in branches {
		let mut terms = Vec::new();
		collect_top_level_bound_and(source, branch, &mut terms);
		if terms.len() > BOOLEAN_FACTOR_MAX_AND_TERMS_PER_BRANCH {
			return Err(BooleanSplitError::OverBudget);
		}
		branch_terms.push(terms);
	}

	let Some(first_branch) = branch_terms.first() else {
		return Ok(None);
	};
	let mut necessary_candidates = Vec::new();
	for term in first_branch {
		if necessary_candidates
			.iter()
			.any(|candidate| bound_subgraphs_structurally_equal(source, *candidate, *term))
		{
			continue;
		}
		let present_in_every_branch = branch_terms.iter().skip(1).all(|branch| {
			branch
				.iter()
				.any(|candidate| bound_subgraphs_structurally_equal(source, *term, *candidate))
		});
		if !present_in_every_branch {
			continue;
		}

		let isolated = isolate_bound_subgraph(source, *term)?;
		if compile_pbc(&isolated).is_some() {
			necessary_candidates.push(*term);
			if necessary_candidates.len() > BOOLEAN_FACTOR_MAX_CANDIDATES {
				return Err(BooleanSplitError::OverBudget);
			}
		}
	}

	if necessary_candidates.is_empty() {
		return Ok(None);
	}

	let pushed_programs = necessary_candidates
		.iter()
		.copied()
		.map(|candidate| isolate_bound_subgraph(source, candidate))
		.collect::<Result<Vec<_>, _>>()?;
	let pushed =
		combine_bound_programs_as_and(pushed_programs)?.ok_or(BooleanSplitError::Malformed)?;

	let mut branch_residuals = Vec::with_capacity(branch_terms.len());
	for terms in &branch_terms {
		let mut used = vec![false; terms.len()];
		for common in &necessary_candidates {
			let Some(index) = terms.iter().enumerate().find_map(|(index, term)| {
				(!used[index] && bound_subgraphs_structurally_equal(source, *common, *term))
					.then_some(index)
			}) else {
				return original_or_residual_factoring(source, root, pushed);
			};
			used[index] = true;
		}

		let remaining = terms
			.iter()
			.enumerate()
			.filter_map(|(index, term)| (!used[index]).then_some(*term))
			.collect::<Vec<_>>();
		if remaining.is_empty() {
			return Ok(Some(BooleanPredicateSplit {
				pushed: Some(pushed),
				residual: None,
			}));
		}

		let Some(branch) = combine_bound_conjuncts(source, &remaining) else {
			return original_or_residual_factoring(source, root, pushed);
		};
		branch_residuals.push(branch);
	}

	let Ok(residual) = combine_bound_programs_as_or(branch_residuals) else {
		return original_or_residual_factoring(source, root, pushed);
	};
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual,
	}))
}

fn try_extract_not_or_branch_negations(
	source: &BoundExprProgram,
	not_root: BoundExprId,
	or_root: BoundExprId,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	if !matches!(
		source.node(or_root),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			..
		})
	) {
		return Ok(None);
	}

	let mut branches = Vec::new();
	collect_bound_or_branches(source, or_root, &mut branches)?;

	let mut pushed_negations = Vec::new();
	let mut residual_negations = Vec::new();
	for branch in branches {
		let candidate = build_negated_subgraph(source, branch)?;
		if compile_pbc(&candidate).is_some() {
			pushed_negations.push(candidate);
			if pushed_negations.len() > BOOLEAN_FACTOR_MAX_CANDIDATES {
				return Err(BooleanSplitError::OverBudget);
			}
		} else {
			residual_negations.push(candidate);
		}
	}

	if pushed_negations.is_empty() {
		return Ok(None);
	}
	let pushed =
		combine_bound_programs_as_and(pushed_negations)?.ok_or(BooleanSplitError::Malformed)?;
	let Ok(residual) = combine_bound_programs_as_and(residual_negations) else {
		return original_not_or_residual_factoring(source, not_root, pushed);
	};
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual,
	}))
}

fn original_or_residual_factoring(
	source: &BoundExprProgram,
	root: BoundExprId,
	pushed: BoundExprProgram,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual: Some(isolate_bound_subgraph(source, root)?),
	}))
}

fn original_not_or_residual_factoring(
	source: &BoundExprProgram,
	root: BoundExprId,
	pushed: BoundExprProgram,
) -> Result<Option<BooleanPredicateSplit>, BooleanSplitError> {
	Ok(Some(BooleanPredicateSplit {
		pushed: Some(pushed),
		residual: Some(isolate_bound_subgraph(source, root)?),
	}))
}

fn collect_bound_or_branches(
	program: &BoundExprProgram,
	id: BoundExprId,
	branches: &mut Vec<BoundExprId>,
) -> Result<(), BooleanSplitError> {
	if branches.len() > BOOLEAN_FACTOR_MAX_OR_BRANCHES {
		return Err(BooleanSplitError::OverBudget);
	}
	match program.node(id).ok_or(BooleanSplitError::Malformed)? {
		BoundExprNode::Binary {
			op: BoundBinaryOp::Or,
			left,
			right,
		} => {
			collect_bound_or_branches(program, *left, branches)?;
			collect_bound_or_branches(program, *right, branches)?;
		}
		_ => {
			branches.push(id);
			if branches.len() > BOOLEAN_FACTOR_MAX_OR_BRANCHES {
				return Err(BooleanSplitError::OverBudget);
			}
		}
	}
	Ok(())
}

fn collect_top_level_bound_and(
	program: &BoundExprProgram,
	id: BoundExprId,
	conjuncts: &mut Vec<BoundExprId>,
) {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			collect_top_level_bound_and(program, *left, conjuncts);
			collect_top_level_bound_and(program, *right, conjuncts);
		}
		_ => conjuncts.push(id),
	}
}

fn isolate_bound_subgraph(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BoundExprProgram, BooleanSplitError> {
	combine_bound_conjuncts(source, &[root]).ok_or(BooleanSplitError::Malformed)
}

fn combine_bound_conjuncts(
	source: &BoundExprProgram,
	roots: &[BoundExprId],
) -> Option<BoundExprProgram> {
	let mut roots = roots.iter().copied();
	let first_root = roots.next()?;
	let mut builder = BoundExprProgramBuilder::new();
	let mut remap = HashMap::new();
	let mut combined = clone_bound_subgraph(source, first_root, &mut builder, &mut remap)?;

	for root in roots {
		let next = clone_bound_subgraph(source, root, &mut builder, &mut remap)?;
		combined = builder.append_binary(BoundBinaryOp::And, combined, next, DataType::Boolean);
	}

	builder.push_root(combined);
	let program = builder.build();
	program.validate().ok()?;
	Some(program)
}

fn combine_bound_programs_as_and(
	programs: Vec<BoundExprProgram>,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	combine_bound_programs_as_binary(programs, BoundBinaryOp::And)
}

fn combine_bound_programs_as_or(
	programs: Vec<BoundExprProgram>,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	combine_bound_programs_as_binary(programs, BoundBinaryOp::Or)
}

fn combine_bound_programs_as_binary(
	programs: Vec<BoundExprProgram>,
	op: BoundBinaryOp,
) -> Result<Option<BoundExprProgram>, BooleanSplitError> {
	let mut programs = programs.into_iter();
	let Some(first_program) = programs.next() else {
		return Ok(None);
	};
	let mut builder = BoundExprProgramBuilder::new();
	let mut combined = clone_single_root_into_builder(&first_program, &mut builder)?;

	for program in programs {
		let next = clone_single_root_into_builder(&program, &mut builder)?;
		combined = builder.append_binary(op.clone(), combined, next, DataType::Boolean);
	}

	builder.push_root(combined);
	let program = builder.build();
	program
		.validate()
		.map_err(|_| BooleanSplitError::Malformed)?;
	Ok(Some(program))
}

fn clone_single_root_into_builder(
	source: &BoundExprProgram,
	builder: &mut BoundExprProgramBuilder,
) -> Result<BoundExprId, BooleanSplitError> {
	let root = source.single_root().ok_or(BooleanSplitError::Malformed)?;
	let mut remap = HashMap::new();
	clone_bound_subgraph(source, root, builder, &mut remap).ok_or(BooleanSplitError::Malformed)
}

fn build_negated_subgraph(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<BoundExprProgram, BooleanSplitError> {
	let mut builder = BoundExprProgramBuilder::new();
	let mut remap = HashMap::new();
	let input = clone_bound_subgraph(source, root, &mut builder, &mut remap)
		.ok_or(BooleanSplitError::Malformed)?;
	let negated = builder.append_unary(BoundUnaryOp::Not, input, DataType::Boolean);
	builder.push_root(negated);
	let program = builder.build();
	program
		.validate()
		.map_err(|_| BooleanSplitError::Malformed)?;
	Ok(program)
}

fn clone_bound_subgraph(
	source: &BoundExprProgram,
	id: BoundExprId,
	builder: &mut BoundExprProgramBuilder,
	remap: &mut HashMap<BoundExprId, BoundExprId>,
) -> Option<BoundExprId> {
	if let Some(cloned) = remap.get(&id) {
		return Some(*cloned);
	}

	let output_type = source.node_type(id)?.clone();
	let cloned = match source.node(id)? {
		BoundExprNode::Column(column) => builder.append_column(column.clone()),
		BoundExprNode::Literal(literal) => {
			builder.append_literal(source.literals.get(*literal as usize)?.clone())
		}
		BoundExprNode::NullLiteral => builder.append_null(),
		BoundExprNode::Cast { input, target } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_cast(input, target.clone(), output_type)
		}
		BoundExprNode::TryCast { input, target } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_try_cast(input, target.clone(), output_type)
		}
		BoundExprNode::Binary { op, left, right } => {
			let left = clone_bound_subgraph(source, *left, builder, remap)?;
			let right = clone_bound_subgraph(source, *right, builder, remap)?;
			builder.append_binary(op.clone(), left, right, output_type)
		}
		BoundExprNode::Unary { op, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_unary(op.clone(), input, output_type)
		}
		BoundExprNode::InList {
			expr,
			list,
			negated,
		} => {
			let expr = clone_bound_subgraph(source, *expr, builder, remap)?;
			let list = list
				.iter()
				.map(|item| clone_bound_subgraph(source, *item, builder, remap))
				.collect::<Option<Vec<_>>>()?;
			builder.append_in_list(expr, list, *negated)
		}
		BoundExprNode::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => {
			let expr = clone_bound_subgraph(source, *expr, builder, remap)?;
			builder.append_like(expr, pattern.clone(), *negated, *case_insensitive)
		}
		BoundExprNode::Case {
			when_then,
			else_input,
		} => {
			let when_then = when_then
				.iter()
				.map(|(condition, result)| {
					Some((
						clone_bound_subgraph(source, *condition, builder, remap)?,
						clone_bound_subgraph(source, *result, builder, remap)?,
					))
				})
				.collect::<Option<Vec<_>>>()?;
			let else_input = clone_bound_subgraph(source, *else_input, builder, remap)?;
			builder.append_case(when_then, else_input, output_type)
		}
		BoundExprNode::TimeBucket { interval, input } => {
			let input = clone_bound_subgraph(source, *input, builder, remap)?;
			builder.append_time_bucket(*interval, input, output_type)
		}
	};
	if let Some(logical_type) = source.node_logical_type(id) {
		builder.set_node_logical_type(cloned, logical_type);
	}
	remap.insert(id, cloned);
	Some(cloned)
}

fn bound_subgraphs_structurally_equal(
	source: &BoundExprProgram,
	left: BoundExprId,
	right: BoundExprId,
) -> bool {
	let mut seen = HashSet::new();
	bound_subgraphs_structurally_equal_inner(source, left, right, &mut seen)
}

fn bound_subgraphs_structurally_equal_inner(
	source: &BoundExprProgram,
	left: BoundExprId,
	right: BoundExprId,
	seen: &mut HashSet<(BoundExprId, BoundExprId)>,
) -> bool {
	if left == right {
		return true;
	}
	if !seen.insert((left, right)) {
		return true;
	}
	if source.node_type(left) != source.node_type(right) {
		return false;
	}

	match (source.node(left), source.node(right)) {
		(Some(BoundExprNode::Column(left)), Some(BoundExprNode::Column(right))) => left == right,
		(Some(BoundExprNode::Literal(left)), Some(BoundExprNode::Literal(right))) => {
			source.literals.get(*left as usize) == source.literals.get(*right as usize)
		}
		(
			Some(BoundExprNode::Cast {
				input: left_input,
				target: left_target,
			}),
			Some(BoundExprNode::Cast {
				input: right_input,
				target: right_target,
			}),
		)
		| (
			Some(BoundExprNode::TryCast {
				input: left_input,
				target: left_target,
			}),
			Some(BoundExprNode::TryCast {
				input: right_input,
				target: right_target,
			}),
		) => {
			left_target == right_target
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::Binary {
				op: left_op,
				left: left_left,
				right: left_right,
			}),
			Some(BoundExprNode::Binary {
				op: right_op,
				left: right_left,
				right: right_right,
			}),
		) => {
			left_op == right_op
				&& bound_subgraphs_structurally_equal_inner(source, *left_left, *right_left, seen)
				&& bound_subgraphs_structurally_equal_inner(source, *left_right, *right_right, seen)
		}
		(
			Some(BoundExprNode::Unary {
				op: left_op,
				input: left_input,
			}),
			Some(BoundExprNode::Unary {
				op: right_op,
				input: right_input,
			}),
		) => {
			left_op == right_op
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		(
			Some(BoundExprNode::InList {
				expr: left_expr,
				list: left_list,
				negated: left_negated,
			}),
			Some(BoundExprNode::InList {
				expr: right_expr,
				list: right_list,
				negated: right_negated,
			}),
		) => {
			left_negated == right_negated
				&& left_list.len() == right_list.len()
				&& bound_subgraphs_structurally_equal_inner(source, *left_expr, *right_expr, seen)
				&& left_list
					.iter()
					.zip(right_list)
					.all(|(left_item, right_item)| {
						bound_subgraphs_structurally_equal_inner(
							source,
							*left_item,
							*right_item,
							seen,
						)
					})
		}
		(
			Some(BoundExprNode::Like {
				expr: left_expr,
				pattern: left_pattern,
				negated: left_negated,
				case_insensitive: left_case_insensitive,
			}),
			Some(BoundExprNode::Like {
				expr: right_expr,
				pattern: right_pattern,
				negated: right_negated,
				case_insensitive: right_case_insensitive,
			}),
		) => {
			left_pattern == right_pattern
				&& left_negated == right_negated
				&& left_case_insensitive == right_case_insensitive
				&& bound_subgraphs_structurally_equal_inner(source, *left_expr, *right_expr, seen)
		}
		(
			Some(BoundExprNode::Case {
				when_then: left_when_then,
				else_input: left_else,
			}),
			Some(BoundExprNode::Case {
				when_then: right_when_then,
				else_input: right_else,
			}),
		) => {
			left_when_then.len() == right_when_then.len()
				&& left_when_then.iter().zip(right_when_then.iter()).all(
					|((left_condition, left_result), (right_condition, right_result))| {
						bound_subgraphs_structurally_equal_inner(
							source,
							*left_condition,
							*right_condition,
							seen,
						) && bound_subgraphs_structurally_equal_inner(
							source,
							*left_result,
							*right_result,
							seen,
						)
					},
				) && bound_subgraphs_structurally_equal_inner(source, *left_else, *right_else, seen)
		}
		(
			Some(BoundExprNode::TimeBucket {
				interval: left_interval,
				input: left_input,
			}),
			Some(BoundExprNode::TimeBucket {
				interval: right_interval,
				input: right_input,
			}),
		) => {
			left_interval == right_interval
				&& bound_subgraphs_structurally_equal_inner(source, *left_input, *right_input, seen)
		}
		_ => false,
	}
}

fn count_bound_subgraph_nodes(
	source: &BoundExprProgram,
	root: BoundExprId,
) -> Result<usize, BooleanSplitError> {
	let mut seen = HashSet::new();
	count_bound_subgraph_nodes_inner(source, root, &mut seen)
}

fn count_bound_subgraph_nodes_inner(
	source: &BoundExprProgram,
	id: BoundExprId,
	seen: &mut HashSet<BoundExprId>,
) -> Result<usize, BooleanSplitError> {
	if !seen.insert(id) {
		return Ok(0);
	}
	let node = source.node(id).ok_or(BooleanSplitError::Malformed)?;
	let mut count = 1;
	for child in bound_node_children(node) {
		count += count_bound_subgraph_nodes_inner(source, child, seen)?;
		if count > BOOLEAN_FACTOR_MAX_NODES {
			return Ok(count);
		}
	}
	Ok(count)
}

fn bound_node_children(node: &BoundExprNode) -> Vec<BoundExprId> {
	match node {
		BoundExprNode::Column(_) | BoundExprNode::Literal(_) | BoundExprNode::NullLiteral => {
			Vec::new()
		}
		BoundExprNode::Cast { input, .. }
		| BoundExprNode::TryCast { input, .. }
		| BoundExprNode::Unary { input, .. }
		| BoundExprNode::Like { expr: input, .. }
		| BoundExprNode::TimeBucket { input, .. } => vec![*input],
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

fn scan_plan(binding: TableBinding, projection: Option<Vec<ColumnOrdinal>>) -> ExecutionPlan {
	let column_ndv = binding
		.optimizer_facts
		.column_ndv
		.iter()
		.map(|(ordinal, distinct_count)| (*ordinal, *distinct_count))
		.collect::<Vec<_>>();
	ExecutionPlan::Scan {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		projection,
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: sorted_columns(&binding.primary_key_columns),
		column_ndv,
	}
}

fn sorted_columns(columns: &HashSet<ColumnOrdinal>) -> Vec<ColumnOrdinal> {
	let mut columns = columns.iter().copied().collect::<Vec<_>>();
	columns.sort();
	columns
}

fn scan_profile_hint(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
) -> Option<QueryProfileHint> {
	match input {
		ExecutionPlan::Scan {
			primary_key_columns,
			..
		} => {
			if predicate_includes_primary_key_equality(predicate, primary_key_columns) {
				Some(QueryProfileHint::PointRead)
			} else {
				Some(QueryProfileHint::Scan)
			}
		}
		_ => None,
	}
}

fn require_single_boolean_root(
	program: &BoundExprProgram,
	context: &'static str,
) -> Result<(), BindingError> {
	if program.root_count() != 1 {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"{context} predicate must have exactly one root, found {}",
				program.root_count()
			),
		});
	}
	let Some(root_type) = program.root_type(0) else {
		return Err(BindingError::TypeMismatch {
			message: format!("{context} predicate root type is missing"),
		});
	};
	if root_type == &DataType::Boolean {
		Ok(())
	} else {
		Err(BindingError::TypeMismatch {
			message: format!("{context} predicate must return Boolean, found {root_type:?}"),
		})
	}
}

fn apply_scan_profile_hint(plan: &mut ExecutionPlan, profile_hint: Option<QueryProfileHint>) {
	if let Some(next_profile_hint) = profile_hint
		&& let ExecutionPlan::Scan { profile_hint, .. } = plan
	{
		*profile_hint = next_profile_hint;
	}
}

fn predicate_includes_primary_key_equality(
	predicate: &BoundExprProgram,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	if primary_key_columns.len() != 1 {
		return false;
	}
	let Some(root) = predicate.single_root() else {
		return false;
	};
	predicate_node_includes_primary_key_equality(predicate, root, primary_key_columns)
}

fn filter_includes_primary_key_row_cap(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
) -> bool {
	match input {
		ExecutionPlan::Scan {
			primary_key_columns,
			..
		} => predicate_includes_primary_key_equality(predicate, primary_key_columns),
		_ => false,
	}
}

fn filter_equality_ndv_estimated_row_count(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
	facts: PlanOptimizerFacts,
) -> Option<RowCount> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	let base_rows = facts.row_upper_bound?;
	let root = predicate.single_root()?;
	predicate_node_equality_ndv_estimated_row_count(predicate, root, input, base_rows)
}

fn predicate_node_equality_ndv_estimated_row_count(
	program: &BoundExprProgram,
	id: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<RowCount> {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => equality_ndv_estimated_row_count(program, *left, *right, input, base_rows)
			.or_else(|| equality_ndv_estimated_row_count(program, *right, *left, input, base_rows)),
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			let left =
				predicate_node_equality_ndv_estimated_row_count(program, *left, input, base_rows);
			let right =
				predicate_node_equality_ndv_estimated_row_count(program, *right, input, base_rows);
			match (left, right) {
				(Some(left), Some(right)) => Some(std::cmp::min(left, right)),
				(Some(left), None) => Some(left),
				(None, Some(right)) => Some(right),
				(None, None) => None,
			}
		}
		_ => None,
	}
}

fn equality_ndv_estimated_row_count(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	input: &ExecutionPlan,
	base_rows: RowCount,
) -> Option<RowCount> {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return None;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return None;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return None;
	};
	if !literal_is_non_null(value) {
		return None;
	}
	let distinct_count = column_ndv_for_input(input, column.ordinal)?;
	Some(estimated_rows_for_ndv(
		base_rows,
		distinct_count.point_estimate(),
	))
}

fn column_ndv_for_input(
	input: &ExecutionPlan,
	requested_column: ColumnOrdinal,
) -> Option<DistinctCountFact> {
	match input {
		ExecutionPlan::Scan { column_ndv, .. } => {
			column_ndv.iter().find_map(|(column, distinct_count)| {
				(*column == requested_column).then_some(*distinct_count)
			})
		}
		ExecutionPlan::Filter { input, .. } => column_ndv_for_input(input, requested_column),
		_ => None,
	}
}

fn estimated_rows_for_ndv(base_rows: RowCount, ndv: u64) -> RowCount {
	let rows = base_rows.get();
	if rows == 0 {
		return RowCount::new(0);
	}
	let divisor = u128::from(ndv.max(1));
	let estimated = (u128::from(rows) + divisor - 1) / divisor;
	RowCount::new(u64::try_from(estimated).unwrap_or(u64::MAX).max(1))
}

fn predicate_node_includes_primary_key_equality(
	program: &BoundExprProgram,
	id: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => {
			primary_key_literal_equality(program, *left, *right, primary_key_columns)
				|| primary_key_literal_equality(program, *right, *left, primary_key_columns)
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			predicate_node_includes_primary_key_equality(program, *left, primary_key_columns)
				|| predicate_node_includes_primary_key_equality(
					program,
					*right,
					primary_key_columns,
				)
		}
		_ => false,
	}
}

fn primary_key_literal_equality(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return false;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return false;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return false;
	};
	primary_key_columns.contains(&column.ordinal) && literal_is_non_null(value)
}

fn literal_is_non_null(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Boolean(value) => value.is_some(),
		ScalarValue::Int32(value) => value.is_some(),
		ScalarValue::Int64(value) => value.is_some(),
		ScalarValue::Float32(value) => value.is_some(),
		ScalarValue::Float64(value) => value.is_some(),
		ScalarValue::Utf8(value) => value.is_some(),
	}
}

#[derive(Debug, Clone)]
struct RelationScope {
	schema: SchemaRef,
	entries: Vec<RelationScopeEntry>,
	next_source_id: usize,
}

#[derive(Debug, Clone)]
struct RelationScopeEntry {
	qualifier: TableRef,
	column: String,
	ordinal: ColumnOrdinal,
	source_id: usize,
}

impl RelationScope {
	fn empty(schema: SchemaRef) -> Self {
		Self {
			schema,
			entries: Vec::new(),
			next_source_id: 0,
		}
	}

	fn scan(qualifier: TableRef, schema: &SchemaRef) -> Self {
		Self::single_source(qualifier, schema)
	}

	fn alias(qualifier: TableRef, schema: &SchemaRef) -> Self {
		Self::single_source(qualifier, schema)
	}

	fn single_source(qualifier: TableRef, schema: &SchemaRef) -> Self {
		let entries = schema
			.fields()
			.iter()
			.enumerate()
			.map(|(idx, field)| RelationScopeEntry {
				qualifier: qualifier.clone(),
				column: field.name().clone(),
				ordinal: ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")),
				source_id: 0,
			})
			.collect();
		Self {
			schema: schema.clone(),
			entries,
			next_source_id: 1,
		}
	}

	fn with_schema(&self, schema: SchemaRef) -> Self {
		let mut scope = self.clone();
		scope.schema = schema;
		scope
	}

	fn project(&self, exprs: &BoundExprProgram, output_schema: &SchemaRef) -> Self {
		let mut entries = Vec::new();
		for (output_idx, root) in exprs.roots.iter().enumerate() {
			let Some(BoundExprNode::Column(column)) = exprs.node(*root) else {
				continue;
			};
			let output_field = output_schema.field(output_idx);
			let output_ordinal =
				ColumnOrdinal(u32::try_from(output_idx).expect("column ordinal overflow"));
			for entry in self
				.entries
				.iter()
				.filter(|entry| entry.ordinal == column.ordinal)
			{
				entries.push(RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: output_field.name().clone(),
					ordinal: output_ordinal,
					source_id: entry.source_id,
				});
			}
		}
		Self {
			schema: output_schema.clone(),
			entries,
			next_source_id: self.next_source_id,
		}
	}

	fn aggregate(&self, group_expr: &BoundExprProgram, output_schema: &SchemaRef) -> Self {
		let mut entries = Vec::new();
		for (output_idx, root) in group_expr.roots.iter().enumerate() {
			let Some(BoundExprNode::Column(column)) = group_expr.node(*root) else {
				continue;
			};
			let output_field = output_schema.field(output_idx);
			let output_ordinal =
				ColumnOrdinal(u32::try_from(output_idx).expect("column ordinal overflow"));
			for entry in self
				.entries
				.iter()
				.filter(|entry| entry.ordinal == column.ordinal)
			{
				entries.push(RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: output_field.name().clone(),
					ordinal: output_ordinal,
					source_id: entry.source_id,
				});
			}
		}
		Self {
			schema: output_schema.clone(),
			entries,
			next_source_id: self.next_source_id,
		}
	}

	fn join(
		kind: JoinKind,
		left: &RelationScope,
		right: &RelationScope,
		output_schema: &SchemaRef,
	) -> Self {
		match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => left.with_schema(output_schema.clone()),
			JoinKind::RightSemi | JoinKind::RightAnti => right.with_schema(output_schema.clone()),
			JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
				let mut entries = left.entries.clone();
				let right_offset = ColumnOrdinal(
					u32::try_from(left.schema.fields().len()).expect("column ordinal overflow"),
				);
				entries.extend(right.entries.iter().map(|entry| RelationScopeEntry {
					qualifier: entry.qualifier.clone(),
					column: entry.column.clone(),
					ordinal: ColumnOrdinal(entry.ordinal.0 + right_offset.0),
					source_id: entry.source_id + left.next_source_id,
				}));
				Self {
					schema: output_schema.clone(),
					entries,
					next_source_id: left.next_source_id + right.next_source_id,
				}
			}
		}
	}

	fn resolve_join_column(
		&self,
		column_ref: &ColumnRef,
		side: &'static str,
	) -> Result<Option<ResolvedLogicalColumn>, BindingError> {
		if column_ref.qualifier.is_some() {
			if !self.contains_qualifier(column_ref.qualifier.as_ref().expect("checked")) {
				return Ok(None);
			}
			return resolve_qualified_column(column_ref, self).map(Some);
		}

		match resolve_logical_column(&column_ref.name.0, &self.schema) {
			Ok(column) => Ok(Some(column)),
			Err(BindingError::UnknownColumn { .. }) => Ok(None),
			Err(error) => Err(error),
		}
		.map_err(|error| match error {
			BindingError::AmbiguousColumn { column } => BindingError::AmbiguousColumnRef {
				name: column,
				found_in: vec![side.to_string()],
			},
			other => other,
		})
	}

	fn contains_qualifier(&self, qualifier: &TableRef) -> bool {
		self.entries
			.iter()
			.any(|entry| &entry.qualifier == qualifier)
	}
}

fn resolve_qualified_column(
	column_ref: &ColumnRef,
	scope: &RelationScope,
) -> Result<ResolvedLogicalColumn, BindingError> {
	let Some(qualifier) = &column_ref.qualifier else {
		return resolve_logical_column(&column_ref.name.0, &scope.schema);
	};
	let qualifier_entries = scope
		.entries
		.iter()
		.filter(|entry| &entry.qualifier == qualifier)
		.collect::<Vec<_>>();
	if qualifier_entries.is_empty() {
		return Err(BindingError::UnknownTableRef {
			qualifier: table_name(qualifier),
		});
	}
	let source_count = qualifier_entries
		.iter()
		.map(|entry| entry.source_id)
		.collect::<BTreeSet<_>>()
		.len();
	if source_count > 1 {
		return Err(BindingError::AmbiguousTableRef {
			qualifier: table_name(qualifier),
			found_in: qualifier_entries
				.iter()
				.map(|entry| entry.source_id.to_string())
				.collect::<BTreeSet<_>>()
				.into_iter()
				.collect(),
		});
	}
	let column_entries = qualifier_entries
		.iter()
		.filter(|entry| entry.column == column_ref.name.0)
		.collect::<Vec<_>>();
	let [entry] = column_entries.as_slice() else {
		if column_entries.is_empty() {
			return Err(BindingError::UnknownColumnInRelation {
				qualifier: table_name(qualifier),
				column: column_ref.name.0.clone(),
				available: qualifier_entries
					.iter()
					.map(|entry| entry.column.clone())
					.collect::<BTreeSet<_>>()
					.into_iter()
					.collect(),
			});
		}
		return Err(BindingError::AmbiguousColumnRef {
			name: format!("{}.{}", table_name(qualifier), column_ref.name.0),
			found_in: column_entries
				.iter()
				.map(|entry| entry.source_id.to_string())
				.collect::<BTreeSet<_>>()
				.into_iter()
				.collect(),
		});
	};
	resolved_logical_column_at_ordinal(&scope.schema, entry.ordinal)
}

fn bind_join_key_pair(
	left_expr: &Expr,
	right_expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Result<(ResolvedJoinKey, ResolvedJoinKey), BindingError> {
	let (left_ref, left_logical_type) = join_key_column_ref(left_expr)?;
	let (right_ref, right_logical_type) = join_key_column_ref(right_expr)?;

	match (left_expr, right_expr) {
		(Expr::Eq(_, _), _) | (_, Expr::Eq(_, _)) => {
			return Err(BindingError::JoinPredicateNotEqui);
		}
		_ => {}
	}

	match (
		resolve_join_column_across(left_ref, left_scope, right_scope)?,
		resolve_join_column_across(right_ref, left_scope, right_scope)?,
	) {
		(JoinColumnResolution::Left(left), JoinColumnResolution::Right(right)) => Ok((
			resolved_join_key(left, left_logical_type)?,
			resolved_join_key(right, right_logical_type)?,
		)),
		(JoinColumnResolution::Right(right), JoinColumnResolution::Left(left)) => Ok((
			resolved_join_key(left, right_logical_type)?,
			resolved_join_key(right, left_logical_type)?,
		)),
		(JoinColumnResolution::Left(_), JoinColumnResolution::Left(_))
		| (JoinColumnResolution::Right(_), JoinColumnResolution::Right(_)) => {
			Err(BindingError::JoinComputedKeyUnsupported)
		}
	}
}

#[derive(Debug, Clone)]
struct ResolvedJoinKey {
	column: ResolvedColumn,
	logical_type: LogicalTypeId,
}

#[derive(Debug, Clone)]
struct ResolvedLogicalColumn {
	column: ResolvedColumn,
	logical_type: LogicalTypeId,
}

fn broadcast_replay_eligibility(intent: JoinStrategyIntent) -> BroadcastReplayEligibility {
	match intent {
		JoinStrategyIntent::Auto
		| JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
		} => BroadcastReplayEligibility::Replayable,
		JoinStrategyIntent::Pinned { .. } => BroadcastReplayEligibility::Terminal,
	}
}

fn choose_join_strategy(
	replay_id: JoinReplayId,
	intent: JoinStrategyIntent,
	kind: JoinKind,
	left_input: JoinInputOptimization,
	right_input: JoinInputOptimization,
	config: &PlannerConfig,
) -> JoinStrategyDecision {
	let force_hash = config.force_hash_join_ids.contains(&replay_id);
	match intent {
		JoinStrategyIntent::Auto if force_hash => {
			hash_join_decision(HashByDefaultReason::HashAfterBroadcastCap)
		}
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			source: JoinStrategyPinSource::SqlHint,
		} if force_hash => hash_join_decision(HashByDefaultReason::HashAfterBroadcastCap),
		JoinStrategyIntent::Pinned { strategy, .. } => JoinStrategyDecision {
			strategy,
			placement: JoinPhysicalPlacement::Natural,
			reason: JoinStrategyDecisionReason::PinnedByAuthor,
		},
		JoinStrategyIntent::Auto => {
			choose_auto_join_strategy(kind, left_input, right_input, config)
		}
	}
}

fn choose_auto_join_strategy(
	kind: JoinKind,
	left_input: JoinInputOptimization,
	right_input: JoinInputOptimization,
	config: &PlannerConfig,
) -> JoinStrategyDecision {
	let Some(threshold) = effective_auto_broadcast_threshold(config) else {
		return hash_join_decision(HashByDefaultReason::AutoBroadcastDisabled);
	};
	if kind != JoinKind::Inner {
		return hash_join_decision(HashByDefaultReason::NonInnerJoin);
	}

	let left_build_candidate = build_byte_candidate(left_input.facts, threshold);
	let right_build_candidate = build_byte_candidate(right_input.facts, threshold);

	let left_viable = left_build_candidate.filter(|_| right_input.probe_scan_linear);
	let right_viable = right_build_candidate.filter(|_| left_input.probe_scan_linear);
	let chosen = match (left_viable, right_viable) {
		(Some(left), Some(right)) if right_build_materially_smaller(left.bytes, right.bytes) => {
			Some((
				BroadcastBuildSide::Right,
				JoinPhysicalPlacement::SwappedRightBuild,
				right,
				Some(left.bytes),
			))
		}
		(Some(left), Some(right)) => Some((
			BroadcastBuildSide::Left,
			JoinPhysicalPlacement::Natural,
			left,
			Some(right.bytes),
		)),
		(Some(left), None) => Some((
			BroadcastBuildSide::Left,
			JoinPhysicalPlacement::Natural,
			left,
			None,
		)),
		(None, Some(right)) => Some((
			BroadcastBuildSide::Right,
			JoinPhysicalPlacement::SwappedRightBuild,
			right,
			None,
		)),
		(None, None) => None,
	};
	if let Some((build_side, placement, candidate, rejected_side_bytes)) = chosen {
		let reason = match candidate.source {
			BuildByteSource::Hard => JoinStrategyDecisionReason::BroadcastInferred {
				build_side,
				build_bytes: candidate.bytes,
				threshold,
				rejected_side_bytes,
			},
			BuildByteSource::Estimated => JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size: candidate.bytes,
				threshold,
				rejected_side_bytes,
			},
		};
		return JoinStrategyDecision {
			strategy: JoinStrategy::Broadcast,
			placement,
			reason,
		};
	}
	if left_build_candidate.is_some() || right_build_candidate.is_some() {
		return hash_join_decision(HashByDefaultReason::ProbeNotScanLinear);
	}
	if left_input.facts.origin != PlanOrigin::SingleBaseRelation {
		return hash_join_decision(HashByDefaultReason::LeftInputNotSingleBaseRelation);
	}
	if right_input.facts.origin != PlanOrigin::SingleBaseRelation {
		return hash_join_decision(HashByDefaultReason::RightInputNotSingleBaseRelation);
	}
	if !has_build_size_fact(left_input.facts) || !has_build_size_fact(right_input.facts) {
		return hash_join_decision(HashByDefaultReason::MissingBuildByteUpperBound);
	}
	hash_join_decision(HashByDefaultReason::BuildByteUpperBoundAboveThreshold)
}

const AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR: u64 = 2;

fn build_byte_candidate(
	facts: PlanOptimizerFacts,
	threshold: ByteCount,
) -> Option<BuildByteCandidate> {
	if facts.origin != PlanOrigin::SingleBaseRelation {
		return None;
	}
	if let Some(build_bytes) = facts.byte_upper_bound {
		return (build_bytes < threshold).then_some(BuildByteCandidate {
			bytes: build_bytes,
			source: BuildByteSource::Hard,
		});
	}
	facts
		.estimated_byte_size
		.filter(|estimated_byte_size| *estimated_byte_size < threshold)
		.map(|estimated_byte_size| BuildByteCandidate {
			bytes: estimated_byte_size,
			source: BuildByteSource::Estimated,
		})
}

fn has_build_size_fact(facts: PlanOptimizerFacts) -> bool {
	facts.byte_upper_bound.is_some() || facts.estimated_byte_size.is_some()
}

fn right_build_materially_smaller(left_bytes: ByteCount, right_bytes: ByteCount) -> bool {
	u128::from(right_bytes.get()) * u128::from(AUTO_BROADCAST_RIGHT_DOMINANCE_FACTOR)
		<= u128::from(left_bytes.get())
}

fn broadcast_probe_is_scan_linear(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Scan { row_limit, .. } => row_limit.is_none(),
		ExecutionPlan::Filter { input, .. } | ExecutionPlan::Project { input, .. } => {
			broadcast_probe_is_scan_linear(input)
		}
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			strategy,
			..
		} if *kind == JoinKind::Inner
			&& *strategy == JoinStrategy::Broadcast
			&& matches!(terminal_exchange(left), Some((1, _))) =>
		{
			broadcast_probe_is_scan_linear(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Aggregate { .. }
		| ExecutionPlan::Exchange { .. }
		| ExecutionPlan::Sort { .. }
		| ExecutionPlan::Take { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => false,
	}
}

fn hash_join_decision(reason: HashByDefaultReason) -> JoinStrategyDecision {
	JoinStrategyDecision {
		strategy: JoinStrategy::HashPartitioned,
		placement: JoinPhysicalPlacement::Natural,
		reason: JoinStrategyDecisionReason::HashByDefault { reason },
	}
}

fn effective_auto_broadcast_threshold(config: &PlannerConfig) -> Option<ByteCount> {
	let configured = config.auto_broadcast_max_build_bytes?;
	let static_cap = default_auto_broadcast_cap_ceiling();
	Some(std::cmp::min(configured, static_cap))
}

fn default_auto_broadcast_cap_ceiling() -> ByteCount {
	ByteCount::new(QueryBudget::default().broadcast_build_bytes_per_task.get() / 4)
}

fn restore_declared_join_order_project(
	input: ExecutionPlan,
	physical_output_schema: &SchemaRef,
	declared_output_schema: SchemaRef,
	declared_left_width: usize,
	declared_right_width: usize,
) -> Result<ExecutionPlan, BindingError> {
	let physical_width = physical_output_schema.fields().len();
	if physical_width != declared_left_width + declared_right_width
		|| declared_output_schema.fields().len() != physical_width
	{
		return Err(BindingError::UnsupportedExpression {
			message: "right-build broadcast join projection arity mismatch".to_string(),
		});
	}

	let restore_ordinals = (declared_right_width..declared_right_width + declared_left_width)
		.chain(0..declared_right_width);
	let mut builder = BoundExprProgramBuilder::new();
	for physical_ordinal in restore_ordinals {
		let field = physical_output_schema.field(physical_ordinal);
		let logical_type = decode_column_logical_type(field).map_err(|error| {
			BindingError::UnsupportedExpression {
				message: error.to_string(),
			}
		})?;
		let node = builder.append_column(ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal: ColumnOrdinal(
				u32::try_from(physical_ordinal).expect("column ordinal overflow"),
			),
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(field.data_type()),
		});
		if !builder.set_node_logical_type(node, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "right-build broadcast join projection target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	let exprs = builder.build();
	exprs
		.validate()
		.map_err(|error| BindingError::UnsupportedExpression {
			message: format!("{error:?}"),
		})?;
	Ok(ExecutionPlan::Project {
		input: Box::new(input),
		exprs,
		output_schema: declared_output_schema,
	})
}

fn exchange_for_join_keys(
	input: ExecutionPlan,
	keys: &[ResolvedJoinKey],
	partition_count: u32,
) -> Result<ExecutionPlan, BindingError> {
	Ok(ExecutionPlan::Exchange {
		input: Box::new(input),
		partition_count,
		key: join_key_program(keys)?,
	})
}

fn strip_root_exchange(input: ExecutionPlan) -> ExecutionPlan {
	match input {
		ExecutionPlan::Exchange { input, .. } => *input,
		other => other,
	}
}

fn join_key_program(keys: &[ResolvedJoinKey]) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	for key in keys {
		let node = builder.append_column(key.column.clone());
		if !builder.set_node_logical_type(node, key.logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "logical join key target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	Ok(builder.build())
}

fn join_key_column_ref(expr: &Expr) -> Result<(&ColumnRef, LogicalTypeId), BindingError> {
	match expr {
		Expr::Column(column_ref) => Ok((column_ref, LogicalTypeId::NAKED)),
		Expr::LogicalAscription { expr, logical_type } => match expr.as_ref() {
			Expr::Column(column_ref) => Ok((column_ref, *logical_type)),
			Expr::Eq(_, _) => Err(BindingError::JoinPredicateNotEqui),
			_ => Err(BindingError::JoinComputedKeyUnsupported),
		},
		Expr::SqlLogicalCast { .. } => Err(BindingError::JoinComputedKeyUnsupported),
		Expr::Eq(_, _) => Err(BindingError::JoinPredicateNotEqui),
		_ => Err(BindingError::JoinComputedKeyUnsupported),
	}
}

fn resolved_join_key(
	column: ResolvedLogicalColumn,
	declared_logical_type: LogicalTypeId,
) -> Result<ResolvedJoinKey, BindingError> {
	validate_logical_ascription_physical(&column.column.data_type, declared_logical_type)?;
	let logical_type = reconcile_identity(
		column.logical_type,
		declared_logical_type,
		&column.column.data_type,
	)?;
	validate_logical_ascription_physical(&column.column.data_type, logical_type)?;
	Ok(ResolvedJoinKey {
		column: column.column,
		logical_type,
	})
}

enum JoinColumnResolution {
	Left(ResolvedLogicalColumn),
	Right(ResolvedLogicalColumn),
}

fn resolve_join_column_across(
	column_ref: &ColumnRef,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Result<JoinColumnResolution, BindingError> {
	let left = left_scope.resolve_join_column(column_ref, "left")?;
	let right = right_scope.resolve_join_column(column_ref, "right")?;
	match (left, right) {
		(Some(left), None) => Ok(JoinColumnResolution::Left(left)),
		(None, Some(right)) => Ok(JoinColumnResolution::Right(right)),
		(Some(_), Some(_)) => Err(BindingError::AmbiguousColumnRef {
			name: column_ref.name.0.clone(),
			found_in: vec!["left".to_string(), "right".to_string()],
		}),
		(None, None) => {
			if let Some(qualifier) = &column_ref.qualifier {
				Err(BindingError::UnknownTableRef {
					qualifier: table_name(qualifier),
				})
			} else {
				Err(BindingError::UnknownColumn {
					column: column_ref.name.0.clone(),
				})
			}
		}
	}
}

fn validate_join_key_types(
	left: &ResolvedJoinKey,
	right: &ResolvedJoinKey,
) -> Result<(), BindingError> {
	if left.column.data_type != right.column.data_type {
		return Err(BindingError::JoinKeyTypeMismatch {
			left_type: format!("{:?}", left.column.data_type),
			right_type: format!("{:?}", right.column.data_type),
		});
	}
	if matches!(
		left.column.data_type,
		DataType::Float16 | DataType::Float32 | DataType::Float64
	) {
		return Err(BindingError::JoinKeyTypeUnsupported {
			type_name: format!("{:?}", left.column.data_type),
		});
	}
	nominal_compatible(
		&left.column.data_type,
		&right.column.data_type,
		left.logical_type,
		right.logical_type,
		false,
		false,
	)?;
	Ok(())
}

fn validate_strategy_shape(
	kind: JoinKind,
	strategy: JoinStrategy,
	left: &ExecutionPlan,
	right: &ExecutionPlan,
	left_keys: &[ColumnOrdinal],
	right_keys: &[ColumnOrdinal],
) -> Result<(), BindingError> {
	match strategy {
		JoinStrategy::HashPartitioned => {
			let Some((left_count, left_exchange_keys)) = terminal_exchange(left) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			let Some((right_count, right_exchange_keys)) = terminal_exchange(right) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			if left_count != right_count {
				return Err(BindingError::JoinPartitionCountMismatch {
					left: left_count,
					right: right_count,
				});
			}
			if exchange_key_ordinals(left_exchange_keys) != left_keys
				|| exchange_key_ordinals(right_exchange_keys) != right_keys
			{
				return Err(BindingError::JoinKeyExchangeMismatch);
			}
		}
		JoinStrategy::Broadcast => {
			if kind != JoinKind::Inner {
				return Err(BindingError::UnsupportedExpression {
					message: "broadcast join is only supported for INNER joins".to_string(),
				});
			}
			let Some((left_count, _)) = terminal_exchange(left) else {
				return Err(BindingError::BroadcastBuildSidePlacement);
			};
			if left_count != 1 {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
			if terminal_exchange(right).is_some() {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
		}
	}
	Ok(())
}

fn terminal_exchange(plan: &ExecutionPlan) -> Option<(u32, &BoundExprProgram)> {
	match plan {
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} => Some((*partition_count, key)),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Take { input, .. } => terminal_exchange(input),
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn exchange_key_ordinals(key: &BoundExprProgram) -> Vec<ColumnOrdinal> {
	key.root_columns()
		.unwrap_or_default()
		.into_iter()
		.map(|column| column.ordinal)
		.collect()
}

pub fn join_schema(kind: JoinKind, left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	let fields = match kind {
		JoinKind::Inner => join_fields(left, right),
		JoinKind::LeftOuter => left_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::RightOuter => nullable_fields(left)
			.into_iter()
			.chain(left_fields(right))
			.collect(),
		JoinKind::FullOuter => nullable_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::LeftSemi | JoinKind::LeftAnti => left_fields(left),
		JoinKind::RightSemi | JoinKind::RightAnti => left_fields(right),
	};
	Arc::new(Schema::new(fields))
}

fn join_fields(left: &SchemaRef, right: &SchemaRef) -> Vec<Field> {
	left_fields(left)
		.into_iter()
		.chain(left_fields(right))
		.collect()
}

fn left_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect()
}

fn nullable_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone().with_nullable(true))
		.collect()
}

fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

fn is_information_schema_namespace(table: &TableRef) -> bool {
	table
		.schema
		.as_deref()
		.is_some_and(|schema| schema.eq_ignore_ascii_case(INFORMATION_SCHEMA_NAMESPACE))
}

fn reject_information_schema_target(table: &TableRef) -> Result<(), BindingError> {
	if is_information_schema_namespace(table) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("information_schema is read-only: {}", table_name(table)),
		});
	}
	Ok(())
}

fn information_schema_logical_types_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("logical_type_id", DataType::Int64, false),
		Field::new("name", DataType::Utf8, false),
		Field::new("physical", DataType::Utf8, false),
		Field::new("refinement", DataType::Utf8, true),
	]))
}

fn information_schema_tables_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("table_type", DataType::Utf8, false),
	]))
}

fn information_schema_columns_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("table_schema", DataType::Utf8, false),
		Field::new("table_name", DataType::Utf8, false),
		Field::new("column_name", DataType::Utf8, false),
		Field::new("ordinal_position", DataType::Int64, false),
		Field::new("data_type", DataType::Utf8, false),
		Field::new("is_nullable", DataType::Utf8, false),
		Field::new("logical_type_name", DataType::Utf8, true),
	]))
}

fn information_schema_schemata_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![Field::new(
		"schema_name",
		DataType::Utf8,
		false,
	)]))
}

fn optimizer_explain_schema() -> SchemaRef {
	Arc::new(Schema::new(vec![
		Field::new("join_index", DataType::Int64, false),
		Field::new("strategy", DataType::Utf8, false),
		Field::new("placement", DataType::Utf8, false),
		Field::new("reason", DataType::Utf8, false),
		Field::new("reason_detail", DataType::Utf8, true),
		Field::new("build_side", DataType::Utf8, true),
		Field::new("build_bytes", DataType::Int64, true),
		Field::new("threshold_bytes", DataType::Int64, true),
		Field::new("rejected_side_bytes", DataType::Int64, true),
	]))
}

fn optimizer_explain_plan(trace: OptimizerTrace) -> Result<ExecutionPlan, BindingError> {
	let rows = trace
		.join_decisions
		.iter()
		.map(optimizer_explain_row)
		.collect::<Result<Vec<_>, _>>()?;
	system_table_plan(
		SystemTableKind::OptimizerExplain,
		optimizer_explain_schema(),
		rows,
		None,
	)
}

fn optimizer_explain_row(receipt: &DecisionReceipt) -> Result<Vec<ScalarValue>, BindingError> {
	let (build_side, build_bytes, threshold_bytes, rejected_side_bytes) =
		optimizer_explain_build_columns(receipt)?;
	Ok(vec![
		ScalarValue::Int64(Some(usize_to_i64(receipt.target.join_index, "join_index")?)),
		ScalarValue::Utf8(Some(join_strategy_label(receipt.strategy).to_string())),
		ScalarValue::Utf8(Some(
			join_physical_placement_label(receipt.placement).to_string(),
		)),
		ScalarValue::Utf8(Some(
			join_strategy_decision_reason_label(&receipt.reason).to_string(),
		)),
		ScalarValue::Utf8(
			join_strategy_decision_reason_detail(&receipt.reason).map(str::to_string),
		),
		ScalarValue::Utf8(build_side.map(str::to_string)),
		ScalarValue::Int64(build_bytes),
		ScalarValue::Int64(threshold_bytes),
		ScalarValue::Int64(rejected_side_bytes),
	])
}

fn optimizer_explain_build_columns(
	receipt: &DecisionReceipt,
) -> Result<(Option<&'static str>, Option<i64>, Option<i64>, Option<i64>), BindingError> {
	match receipt.reason {
		JoinStrategyDecisionReason::BroadcastInferred {
			build_side,
			build_bytes,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(build_bytes, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::BroadcastEstimated {
			build_side,
			estimated_byte_size,
			threshold,
			rejected_side_bytes,
		} => Ok((
			Some(broadcast_build_side_label(build_side)),
			Some(byte_count_to_i64(estimated_byte_size, "build_bytes")?),
			Some(byte_count_to_i64(threshold, "threshold_bytes")?),
			rejected_side_bytes
				.map(|bytes| byte_count_to_i64(bytes, "rejected_side_bytes"))
				.transpose()?,
		)),
		JoinStrategyDecisionReason::PinnedByAuthor
			if receipt.strategy == JoinStrategy::Broadcast =>
		{
			Ok((
				Some(match receipt.placement {
					JoinPhysicalPlacement::Natural => "left",
					JoinPhysicalPlacement::SwappedRightBuild => "right",
				}),
				None,
				None,
				None,
			))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::HashByDefault { .. } => Ok((None, None, None, None)),
	}
}

fn join_strategy_label(strategy: JoinStrategy) -> &'static str {
	match strategy {
		JoinStrategy::HashPartitioned => "hash_partitioned",
		JoinStrategy::Broadcast => "broadcast",
	}
}

fn join_physical_placement_label(placement: JoinPhysicalPlacement) -> &'static str {
	match placement {
		JoinPhysicalPlacement::Natural => "natural",
		JoinPhysicalPlacement::SwappedRightBuild => "swapped_right_build",
	}
}

fn join_strategy_decision_reason_label(reason: &JoinStrategyDecisionReason) -> &'static str {
	match reason {
		JoinStrategyDecisionReason::PinnedByAuthor => "pinned_by_author",
		JoinStrategyDecisionReason::BroadcastInferred { .. } => "broadcast_inferred",
		JoinStrategyDecisionReason::BroadcastEstimated { .. } => "broadcast_estimated",
		JoinStrategyDecisionReason::HashByDefault { .. } => "hash_by_default",
	}
}

fn join_strategy_decision_reason_detail(
	reason: &JoinStrategyDecisionReason,
) -> Option<&'static str> {
	match reason {
		JoinStrategyDecisionReason::HashByDefault { reason } => {
			Some(hash_by_default_reason_label(*reason))
		}
		JoinStrategyDecisionReason::PinnedByAuthor
		| JoinStrategyDecisionReason::BroadcastInferred { .. }
		| JoinStrategyDecisionReason::BroadcastEstimated { .. } => None,
	}
}

fn hash_by_default_reason_label(reason: HashByDefaultReason) -> &'static str {
	match reason {
		HashByDefaultReason::AutoBroadcastDisabled => "auto_broadcast_disabled",
		HashByDefaultReason::NonInnerJoin => "non_inner_join",
		HashByDefaultReason::LeftInputNotSingleBaseRelation => {
			"left_input_not_single_base_relation"
		}
		HashByDefaultReason::RightInputNotSingleBaseRelation => {
			"right_input_not_single_base_relation"
		}
		HashByDefaultReason::ProbeNotScanLinear => "probe_not_scan_linear",
		HashByDefaultReason::MissingBuildByteUpperBound => "missing_build_byte_upper_bound",
		HashByDefaultReason::BuildByteUpperBoundAboveThreshold => {
			"build_byte_upper_bound_above_threshold"
		}
		HashByDefaultReason::HashAfterBroadcastCap => "hash_after_broadcast_cap",
	}
}

fn broadcast_build_side_label(side: BroadcastBuildSide) -> &'static str {
	match side {
		BroadcastBuildSide::Left => "left",
		BroadcastBuildSide::Right => "right",
	}
}

fn byte_count_to_i64(value: ByteCount, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value.get()).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

fn usize_to_i64(value: usize, column: &'static str) -> Result<i64, BindingError> {
	i64::try_from(value).map_err(|_| BindingError::UnsupportedExpression {
		message: format!("optimizer explain {column} exceeds i64::MAX"),
	})
}

fn system_table_plan(
	kind: SystemTableKind,
	full_schema: SchemaRef,
	rows: Vec<Vec<ScalarValue>>,
	projection: Option<&Vec<String>>,
) -> Result<ExecutionPlan, BindingError> {
	let Some(projection) = projection else {
		return Ok(ExecutionPlan::SystemTable {
			kind,
			schema: full_schema,
			rows,
		});
	};

	let requested = projection
		.iter()
		.map(|column| resolve_column(column, &full_schema).map(|column| column.ordinal))
		.collect::<Result<Vec<_>, _>>()?;
	let schema = project_schema(&full_schema, &requested)?;
	let rows = project_scalar_rows(&rows, &requested)?;

	Ok(ExecutionPlan::SystemTable { kind, schema, rows })
}

fn logical_type_spec_row(spec: LogicalTypeSpec) -> Result<Vec<ScalarValue>, BindingError> {
	Ok(vec![
		ScalarValue::Int64(Some(i64::from(spec.id.0))),
		ScalarValue::Utf8(Some(spec.name)),
		ScalarValue::Utf8(Some(sql_carrier_keyword(&spec.physical)?.to_string())),
		ScalarValue::Utf8(
			spec.refinement
				.as_ref()
				.map(render_refinement_check)
				.transpose()?,
		),
	])
}

fn information_schema_table_rows(descriptors: &[TableSchemaDescriptor]) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| {
			vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some("BASE TABLE".to_string())),
			]
		})
		.collect()
}

fn information_schema_schemata_rows(
	descriptors: &[TableSchemaDescriptor],
) -> Vec<Vec<ScalarValue>> {
	descriptors
		.iter()
		.map(|descriptor| descriptor.namespace.clone())
		.collect::<BTreeSet<_>>()
		.into_iter()
		.map(|schema_name| vec![ScalarValue::Utf8(Some(schema_name))])
		.collect()
}

fn information_schema_column_rows(
	descriptors: &[TableSchemaDescriptor],
	logical_types: &LogicalTypeRegistry,
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	let mut rows = Vec::new();
	for descriptor in descriptors {
		let schema =
			descriptor
				.arrow_schema()
				.map_err(|error| BindingError::UnsupportedExpression {
					message: format!(
						"failed to decode table descriptor schema for {}.{}: {error}",
						descriptor.namespace, descriptor.table_name
					),
				})?;
		for (index, field) in schema.fields().iter().enumerate() {
			let ordinal =
				i64::try_from(index + 1).map_err(|_| BindingError::UnsupportedExpression {
					message: "information_schema.columns ordinal overflow".to_string(),
				})?;
			rows.push(vec![
				ScalarValue::Utf8(Some(descriptor.namespace.clone())),
				ScalarValue::Utf8(Some(descriptor.table_name.clone())),
				ScalarValue::Utf8(Some(field.name().clone())),
				ScalarValue::Int64(Some(ordinal)),
				ScalarValue::Utf8(Some(sql_carrier_keyword(field.data_type())?.to_string())),
				ScalarValue::Utf8(Some(
					if field.is_nullable() { "YES" } else { "NO" }.to_string(),
				)),
				ScalarValue::Utf8(logical_type_display_name(field, logical_types)?),
			]);
		}
	}
	Ok(rows)
}

fn logical_type_display_name(
	field: &Field,
	logical_types: &LogicalTypeRegistry,
) -> Result<Option<String>, BindingError> {
	let logical_type = decode_column_logical_type_with_resolver(
		field,
		LogicalTypeResolver::with_registry(logical_types),
	)
	.map_err(|error| BindingError::UnsupportedExpression {
		message: error.to_string(),
	})?;
	if logical_type.is_naked() {
		return Ok(None);
	}
	LogicalTypeResolver::with_registry(logical_types)
		.spec(logical_type)
		.map(|spec| Some(spec.name))
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!(
				"logical type metadata for column {} references unknown id {}",
				field.name(),
				logical_type.0
			),
		})
}

fn sql_carrier_keyword(data_type: &DataType) -> Result<&'static str, BindingError> {
	match data_type {
		DataType::Int32 => Ok("INT"),
		DataType::Int64 => Ok("BIGINT"),
		DataType::Float32 => Ok("REAL"),
		DataType::Float64 => Ok("DOUBLE"),
		DataType::Utf8 => Ok("TEXT"),
		DataType::Boolean => Ok("BOOLEAN"),
		other => Err(BindingError::UnsupportedExpression {
			message: format!("unsupported SQL carrier for information_schema: {other:?}"),
		}),
	}
}

fn render_refinement_check(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	Ok(format!(
		"CHECK ({})",
		render_refinement_predicate(predicate)?
	))
}

fn render_refinement_predicate(
	predicate: &LogicalTypeRefinementPredicate,
) -> Result<String, BindingError> {
	match predicate {
		LogicalTypeRefinementPredicate::Comparison { left, op, right } => Ok(format!(
			"{} {} {}",
			render_refinement_value(left)?,
			refinement_op_sql(*op),
			render_refinement_value(right)?
		)),
		LogicalTypeRefinementPredicate::And(predicates) => predicates
			.iter()
			.map(render_refinement_predicate)
			.collect::<Result<Vec<_>, _>>()
			.map(|parts| parts.join(" AND ")),
	}
}

fn render_refinement_value(value: &LogicalTypeRefinementValue) -> Result<String, BindingError> {
	match value {
		LogicalTypeRefinementValue::Value => Ok("VALUE".to_string()),
		LogicalTypeRefinementValue::Int64(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Int32(value) => Ok(value.to_string()),
		LogicalTypeRefinementValue::Float64(bits) => {
			let value = f64::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT64 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Float32(bits) => {
			let value = f32::from_bits(*bits);
			if value.is_finite() {
				Ok(value.to_string())
			} else {
				Err(BindingError::UnsupportedExpression {
					message: "logical type FLOAT32 refinement literal is not finite".to_string(),
				})
			}
		}
		LogicalTypeRefinementValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
		LogicalTypeRefinementValue::Boolean(value) => {
			Ok(if *value { "TRUE" } else { "FALSE" }.to_string())
		}
	}
}

fn refinement_op_sql(op: LogicalTypeRefinementComparisonOp) -> &'static str {
	match op {
		LogicalTypeRefinementComparisonOp::Eq => "=",
		LogicalTypeRefinementComparisonOp::Ne => "!=",
		LogicalTypeRefinementComparisonOp::Lt => "<",
		LogicalTypeRefinementComparisonOp::Le => "<=",
		LogicalTypeRefinementComparisonOp::Gt => ">",
		LogicalTypeRefinementComparisonOp::Ge => ">=",
	}
}

fn project_scalar_rows(
	rows: &[Vec<ScalarValue>],
	requested: &[ColumnOrdinal],
) -> Result<Vec<Vec<ScalarValue>>, BindingError> {
	rows.iter()
		.map(|row| {
			requested
				.iter()
				.map(|column| {
					row.get(column.0 as usize).cloned().ok_or_else(|| {
						BindingError::UnsupportedExpression {
							message: format!(
								"internal: system table projection column {} out of range",
								column.0
							),
						}
					})
				})
				.collect::<Result<Vec<_>, _>>()
		})
		.collect()
}

type OrdinalRemap = HashMap<ColumnOrdinal, ColumnOrdinal>;

fn push_down_scan_projection(plan: ExecutionPlan) -> Result<ExecutionPlan, BindingError> {
	let (plan, _) = optimize_for_required(plan, None)?;
	Ok(plan)
}

fn apply_safe_row_limit_to_scan(plan: &mut ExecutionPlan, limit: usize) -> bool {
	match plan {
		ExecutionPlan::Scan {
			row_identity,
			row_limit,
			..
		} if *row_identity == crate::storage::RowIdentityProjection::None => {
			*row_limit = Some(row_limit.map_or(limit, |existing| existing.min(limit)));
			true
		}
		ExecutionPlan::Filter {
			input,
			disposition: FilterDisposition::FullyPushed,
			..
		} => apply_safe_row_limit_to_scan(input, limit),
		ExecutionPlan::Take {
			input,
			limit: inner_limit,
			..
		} => apply_safe_row_limit_to_scan(input, limit.min(*inner_limit)),
		_ => false,
	}
}

fn optimize_for_required(
	plan: ExecutionPlan,
	required_output: Option<BTreeSet<ColumnOrdinal>>,
) -> Result<(ExecutionPlan, OrdinalRemap), BindingError> {
	match plan {
		ExecutionPlan::EmptyRelation { schema } => {
			let all_columns = all_columns(&schema);
			let required = required_output.unwrap_or(all_columns);
			let requested = required.iter().copied().collect::<Vec<_>>();
			let remap = ordinal_remap(&requested);
			let schema = project_schema(&schema, &requested)?;
			Ok((ExecutionPlan::EmptyRelation { schema }, remap))
		}
		ExecutionPlan::SystemTable { kind, schema, rows } => {
			let all_columns = all_columns(&schema);
			let required = required_output.unwrap_or(all_columns);
			let requested = required.iter().copied().collect::<Vec<_>>();
			let remap = ordinal_remap(&requested);
			let schema = project_schema(&schema, &requested)?;
			let rows = project_scalar_rows(&rows, &requested)?;
			Ok((ExecutionPlan::SystemTable { kind, schema, rows }, remap))
		}
		ExecutionPlan::Scan {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			projection: _,
			pbc_program,
			row_limit,
			row_identity,
			profile_hint,
			primary_key_columns,
			column_ndv,
		} => {
			let all_columns = all_columns(&schema);
			let required = required_output.unwrap_or(all_columns);
			let requested = required.iter().copied().collect::<Vec<_>>();
			let remap = ordinal_remap(&requested);
			let schema = project_schema(&schema, &requested)?;
			let primary_key_columns = primary_key_columns
				.into_iter()
				.filter_map(|column| remap.get(&column).copied())
				.collect();
			let column_ndv = column_ndv
				.into_iter()
				.filter_map(|(column, distinct_count)| {
					remap
						.get(&column)
						.copied()
						.map(|mapped| (mapped, distinct_count))
				})
				.collect();

			Ok((
				ExecutionPlan::Scan {
					legacy_table_id,
					table,
					fingerprint,
					schema,
					projection: Some(requested),
					pbc_program,
					row_limit,
					row_identity,
					profile_hint,
					primary_key_columns,
					column_ndv,
				},
				remap,
			))
		}
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => {
			let output_required = required_output.unwrap_or_else(|| all_columns(&input.schema()));
			let mut child_required = output_required.clone();
			if disposition == FilterDisposition::EvaluateInToron {
				collect_program_columns(&predicate, &mut child_required);
			}
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let predicate = if disposition == FilterDisposition::EvaluateInToron {
				rewrite_program_ordinals(predicate, &remap)?
			} else {
				predicate
			};

			Ok((
				ExecutionPlan::Filter {
					input: Box::new(input),
					predicate,
					disposition,
				},
				remap,
			))
		}
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => {
			let requested_outputs =
				required_output.unwrap_or_else(|| output_columns(exprs.root_count()));
			let requested_outputs_vec = requested_outputs.iter().copied().collect::<Vec<_>>();
			let exprs = project_program_roots(&exprs, &requested_outputs_vec)?;
			let mut child_required = BTreeSet::new();
			collect_program_columns(&exprs, &mut child_required);
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let exprs = rewrite_program_ordinals(exprs, &remap)?;
			let output_schema = schema_for_projected_exprs_with_existing_names(
				&exprs,
				&output_schema,
				&requested_outputs_vec,
			);
			let output_remap = ordinal_remap(&requested_outputs_vec);

			Ok((
				collapse_adjacent_identity_project(input, exprs, output_schema),
				output_remap,
			))
		}
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema: _,
		} => {
			let mut child_required = BTreeSet::new();
			collect_program_columns(&group_expr, &mut child_required);
			for call in &aggr_expr {
				collect_program_columns(&call.args, &mut child_required);
			}

			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let group_expr = rewrite_program_ordinals(group_expr, &remap)?;
			let aggr_expr = aggr_expr
				.into_iter()
				.map(|call| {
					Ok(BoundAggregateCall {
						function: call.function,
						args: rewrite_program_ordinals(call.args, &remap)?,
					})
				})
				.collect::<Result<Vec<_>, BindingError>>()?;
			let output_schema = aggregate_schema(&group_expr, &aggr_expr);
			let output_remap = ordinal_remap(
				&output_columns(output_schema.fields().len())
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);

			Ok((
				ExecutionPlan::Aggregate {
					input: Box::new(input),
					group_expr,
					aggr_expr,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			let output_required = required_output.unwrap_or_else(|| all_columns(&input.schema()));
			let mut child_required = output_required.clone();
			collect_program_columns(&key, &mut child_required);
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let key = rewrite_program_ordinals(key, &remap)?;
			Ok((
				ExecutionPlan::Exchange {
					input: Box::new(input),
					partition_count,
					key,
				},
				remap,
			))
		}
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} => {
			let output_len = output_schema.fields().len();
			let (input, _) = optimize_for_required(*input, None)?;
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);

			Ok((
				ExecutionPlan::Sort {
					input: Box::new(input),
					order_by,
					limit,
					offset,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Take {
			input,
			limit,
			output_schema,
		} => {
			let output_len = output_schema.fields().len();
			let (mut input, _) = optimize_for_required(*input, None)?;
			apply_safe_row_limit_to_scan(&mut input, limit);
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::Take {
					input: Box::new(input),
					limit,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Join {
			replay_id,
			replay_eligibility,
			left,
			right,
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema,
		} => {
			let (left, _) = optimize_for_required(*left, None)?;
			let (right, _) = optimize_for_required(*right, None)?;
			let output_len = output_schema.fields().len();
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::Join {
					replay_id,
					replay_eligibility,
					left: Box::new(left),
					right: Box::new(right),
					kind,
					left_keys,
					right_keys,
					strategy,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::CreateTable {
			name,
			schema,
			row_identity,
			if_not_exists,
			or_replace,
		} => Ok((
			ExecutionPlan::CreateTable {
				name,
				schema,
				row_identity,
				if_not_exists,
				or_replace,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::CreateLogicalType {
			name,
			physical,
			refinement,
		} => Ok((
			ExecutionPlan::CreateLogicalType {
				name,
				physical,
				refinement,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::CreateTableAsSelect {
			table,
			schema,
			if_not_exists,
			or_replace,
			source,
		} => {
			let (source, _) = optimize_for_required(*source, None)?;
			Ok((
				ExecutionPlan::CreateTableAsSelect {
					table,
					schema,
					if_not_exists,
					or_replace,
					source: Box::new(source),
				},
				OrdinalRemap::new(),
			))
		}
		ExecutionPlan::DropTable { name, if_exists } => Ok((
			ExecutionPlan::DropTable { name, if_exists },
			OrdinalRemap::new(),
		)),
		ExecutionPlan::Delete {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			predicate,
			pbc_program,
			profile_hint,
			returning,
		} => Ok((
			ExecutionPlan::Delete {
				legacy_table_id,
				table,
				fingerprint,
				schema,
				predicate,
				pbc_program,
				profile_hint,
				returning,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::Update {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			source,
			profile_hint,
			returning,
		} => {
			let (source, _) = optimize_for_required(*source, None)?;
			Ok((
				ExecutionPlan::Update {
					legacy_table_id,
					table,
					fingerprint,
					schema,
					source: Box::new(source),
					profile_hint,
					returning,
				},
				OrdinalRemap::new(),
			))
		}
		ExecutionPlan::Insert {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			rows,
			returning,
		} => Ok((
			ExecutionPlan::Insert {
				legacy_table_id,
				table,
				fingerprint,
				schema,
				rows,
				returning,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::InsertSelect {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			target_columns,
			source,
			returning,
		} => {
			let (source, _) = optimize_for_required(*source, None)?;
			Ok((
				ExecutionPlan::InsertSelect {
					legacy_table_id,
					table,
					fingerprint,
					schema,
					target_columns,
					source: Box::new(source),
					returning,
				},
				OrdinalRemap::new(),
			))
		}
	}
}

fn collapse_adjacent_identity_project(
	input: ExecutionPlan,
	parent_exprs: BoundExprProgram,
	parent_output_schema: SchemaRef,
) -> ExecutionPlan {
	let collapsible = matches!(
		&input,
		ExecutionPlan::Project { output_schema, .. }
			if is_metadata_transparent_identity_project(&parent_exprs, output_schema)
	);
	if collapsible {
		let ExecutionPlan::Project {
			input: child_input,
			exprs: child_exprs,
			..
		} = input
		else {
			unreachable!("collapsible implies the child is a Project");
		};
		return ExecutionPlan::Project {
			input: child_input,
			exprs: child_exprs,
			output_schema: parent_output_schema,
		};
	}
	ExecutionPlan::Project {
		input: Box::new(input),
		exprs: parent_exprs,
		output_schema: parent_output_schema,
	}
}

fn is_metadata_transparent_identity_project(
	parent_exprs: &BoundExprProgram,
	child_output_schema: &SchemaRef,
) -> bool {
	let fields = child_output_schema.fields();
	if parent_exprs.root_count() != fields.len() {
		return false;
	}
	for (idx, field) in fields.iter().enumerate() {
		let field = field.as_ref();
		let Some(root_id) = parent_exprs.roots.get(idx).copied() else {
			return false;
		};
		let Some(BoundExprNode::Column(column)) = parent_exprs.node(root_id) else {
			return false;
		};
		let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
		if column.ordinal != ordinal {
			return false;
		}
		if column.data_type != *field.data_type() || column.nullable != field.is_nullable() {
			return false;
		}
		let Ok(child_logical_type) = decode_column_logical_type(field) else {
			return false;
		};
		if parent_exprs.root_logical_type(idx) != Some(child_logical_type) {
			return false;
		}
	}
	true
}

fn all_columns(schema: &SchemaRef) -> BTreeSet<ColumnOrdinal> {
	output_columns(schema.fields().len())
}

fn output_columns(len: usize) -> BTreeSet<ColumnOrdinal> {
	(0..len)
		.map(|idx| ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")))
		.collect()
}

fn project_program_roots(
	source: &BoundExprProgram,
	requested_outputs: &[ColumnOrdinal],
) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	let mut remap = HashMap::new();
	for ordinal in requested_outputs {
		let root = *source.roots.get(ordinal.0 as usize).ok_or_else(|| {
			BindingError::UnsupportedExpression {
				message: format!("project output ordinal {:?} out of bounds", ordinal),
			}
		})?;
		let cloned =
			clone_bound_subgraph(source, root, &mut builder, &mut remap).ok_or_else(|| {
				BindingError::UnsupportedExpression {
					message: format!("project output ordinal {:?} is malformed", ordinal),
				}
			})?;
		builder.push_root(cloned);
	}
	let program = builder.build();
	program
		.validate()
		.map_err(|error| BindingError::UnsupportedExpression {
			message: format!("project output program is malformed: {error:?}"),
		})?;
	Ok(program)
}

fn ordinal_remap(requested: &[ColumnOrdinal]) -> OrdinalRemap {
	requested
		.iter()
		.enumerate()
		.map(|(new, old)| {
			(
				*old,
				ColumnOrdinal(u32::try_from(new).expect("column ordinal overflow")),
			)
		})
		.collect()
}

fn project_schema(
	schema: &SchemaRef,
	requested: &[ColumnOrdinal],
) -> Result<SchemaRef, BindingError> {
	let fields = requested
		.iter()
		.map(|ordinal| {
			schema
				.fields()
				.get(ordinal.0 as usize)
				.map(|field| field.as_ref().clone())
				.ok_or_else(|| BindingError::UnknownColumn {
					column: format!("ordinal {}", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Schema::new(fields)))
}

fn collect_program_columns(program: &BoundExprProgram, columns: &mut BTreeSet<ColumnOrdinal>) {
	for root in &program.roots {
		collect_program_node_columns(program, *root, columns);
	}
}

fn collect_program_node_columns(
	program: &BoundExprProgram,
	id: BoundExprId,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	match program.node(id) {
		Some(BoundExprNode::Column(column)) => {
			columns.insert(column.ordinal);
		}
		Some(BoundExprNode::Literal(_)) | Some(BoundExprNode::NullLiteral) | None => {}
		Some(BoundExprNode::Cast { input, .. })
		| Some(BoundExprNode::TryCast { input, .. })
		| Some(BoundExprNode::Unary { input, .. })
		| Some(BoundExprNode::Like { expr: input, .. })
		| Some(BoundExprNode::TimeBucket { input, .. }) => {
			collect_program_node_columns(program, *input, columns);
		}
		Some(BoundExprNode::Binary { left, right, .. }) => {
			collect_program_node_columns(program, *left, columns);
			collect_program_node_columns(program, *right, columns);
		}
		Some(BoundExprNode::InList { expr, list, .. }) => {
			collect_program_node_columns(program, *expr, columns);
			for item in list {
				collect_program_node_columns(program, *item, columns);
			}
		}
		Some(BoundExprNode::Case {
			when_then,
			else_input,
		}) => {
			for (condition, result) in when_then {
				collect_program_node_columns(program, *condition, columns);
				collect_program_node_columns(program, *result, columns);
			}
			collect_program_node_columns(program, *else_input, columns);
		}
	}
}

fn rewrite_program_ordinals(
	mut program: BoundExprProgram,
	remap: &OrdinalRemap,
) -> Result<BoundExprProgram, BindingError> {
	for node in &mut program.nodes {
		if let BoundExprNode::Column(column) = node {
			column.ordinal =
				*remap
					.get(&column.ordinal)
					.ok_or_else(|| BindingError::UnsupportedExpression {
						message: format!("missing ordinal remap for {:?}", column.ordinal),
					})?;
		}
	}
	Ok(program)
}

fn bind_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
) -> Result<BoundAggregateCall, BindingError> {
	if matches!(function, AggregateFunction::CountAll) {
		return Ok(BoundAggregateCall {
			function: function.clone(),
			args: BoundExprProgramBuilder::new().build(),
		});
	}
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return bind_stable_aggregate_call(function, args, input_schema, ctx, spec);
	}
	match function {
		AggregateFunction::LinearRegress => {
			if !ctx.config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "LinearRegress",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, ctx))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundAggregateCall {
				function: function.clone(),
				args: merge_aggregate_args(args),
			})
		}
		AggregateFunction::ApproxQuantile(_) => {
			if !ctx.config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "ApproxQuantile",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, ctx))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundAggregateCall {
				function: function.clone(),
				args: merge_aggregate_args(args),
			})
		}
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

fn bind_stable_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	spec: &AggregateKernelSpec,
) -> Result<BoundAggregateCall, BindingError> {
	if args.len() != aggregate_arity_len(spec.arity) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("{} requires one argument", spec.sql_name),
		});
	}
	let args = bind_expr_program(args, input_schema, ctx)?;
	if !matches!(spec.input_domain, AggregateInputDomain::Any) {
		let Some(data_type) = args.root_type(0) else {
			return Err(BindingError::UnsupportedExpression {
				message: format!("{} requires one argument", spec.sql_name),
			});
		};
		if aggregate_kernel::stable_output_data_type(function, Some(data_type)).is_none() {
			return Err(BindingError::TypeMismatch {
				message: aggregate_type_mismatch_message(spec.kind, data_type),
			});
		}
	}
	Ok(BoundAggregateCall {
		function: function.clone(),
		args,
	})
}

fn merge_aggregate_args(programs: Vec<BoundExprProgram>) -> BoundExprProgram {
	if programs.is_empty() {
		return BoundExprProgramBuilder::new().build();
	}
	if programs.len() == 1 {
		return programs.into_iter().next().expect("one program");
	}
	let mut merged = BoundExprProgram {
		nodes: Vec::new(),
		node_types: Vec::new(),
		node_logical_types: Vec::new(),
		literals: Vec::new(),
		roots: Vec::new(),
	};
	for program in programs {
		let node_offset =
			u32::try_from(merged.nodes.len()).expect("bound expression node offset overflow");
		let literal_offset =
			u32::try_from(merged.literals.len()).expect("bound expression literal offset overflow");
		merged.literals.extend(program.literals);
		merged.node_types.extend(program.node_types);
		merged.node_logical_types.extend(program.node_logical_types);
		merged.nodes.extend(
			program
				.nodes
				.into_iter()
				.map(|node| offset_bound_node(node, node_offset, literal_offset)),
		);
		merged.roots.extend(
			program
				.roots
				.into_iter()
				.map(|root| BoundExprId(root.0 + node_offset)),
		);
	}
	merged
}

fn offset_bound_node(node: BoundExprNode, node_offset: u32, literal_offset: u32) -> BoundExprNode {
	let offset_id = |id: BoundExprId| BoundExprId(id.0 + node_offset);
	match node {
		BoundExprNode::Column(column) => BoundExprNode::Column(column),
		BoundExprNode::Literal(literal) => BoundExprNode::Literal(literal + literal_offset),
		BoundExprNode::NullLiteral => BoundExprNode::NullLiteral,
		BoundExprNode::Cast { input, target } => BoundExprNode::Cast {
			input: offset_id(input),
			target,
		},
		BoundExprNode::TryCast { input, target } => BoundExprNode::TryCast {
			input: offset_id(input),
			target,
		},
		BoundExprNode::Binary { op, left, right } => BoundExprNode::Binary {
			op,
			left: offset_id(left),
			right: offset_id(right),
		},
		BoundExprNode::Unary { op, input } => BoundExprNode::Unary {
			op,
			input: offset_id(input),
		},
		BoundExprNode::InList {
			expr,
			list,
			negated,
		} => BoundExprNode::InList {
			expr: offset_id(expr),
			list: list.into_iter().map(offset_id).collect(),
			negated,
		},
		BoundExprNode::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => BoundExprNode::Like {
			expr: offset_id(expr),
			pattern,
			negated,
			case_insensitive,
		},
		BoundExprNode::Case {
			when_then,
			else_input,
		} => BoundExprNode::Case {
			when_then: when_then
				.into_iter()
				.map(|(condition, result)| (offset_id(condition), offset_id(result)))
				.collect(),
			else_input: offset_id(else_input),
		},
		BoundExprNode::TimeBucket { interval, input } => BoundExprNode::TimeBucket {
			interval,
			input: offset_id(input),
		},
	}
}

fn aggregate_arity_len(arity: AggregateArity) -> usize {
	match arity {
		AggregateArity::Zero => 0,
		AggregateArity::One => 1,
	}
}

fn aggregate_type_mismatch_message(kernel: StableAggregateKernel, data_type: &DataType) -> String {
	match kernel {
		StableAggregateKernel::Sum => {
			format!("SUM expects numeric input, got {data_type:?}")
		}
		StableAggregateKernel::Avg => {
			format!("AVG expects numeric input, got {data_type:?}")
		}
		StableAggregateKernel::Min => {
			format!("MIN expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
		}
		StableAggregateKernel::Max => {
			format!("MAX expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
		}
		StableAggregateKernel::CountAll | StableAggregateKernel::Count => {
			unreachable!("COUNT accepts any input type")
		}
	}
}

fn bind_expr_program(
	exprs: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
) -> Result<BoundExprProgram, BindingError> {
	bind_expr_program_with_null_policy(exprs, input_schema, ctx, NullRootPolicy::Reject)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullRootPolicy {
	Reject,
	MaterializeResultNull,
}

fn bind_expr_program_with_null_policy(
	exprs: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	null_policy: NullRootPolicy,
) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	for expr in exprs {
		let root = if matches!(null_policy, NullRootPolicy::MaterializeResultNull)
			&& is_untyped_null(expr)
		{
			builder.append_null()
		} else {
			bind_expr_into_program(&mut builder, expr, input_schema, ctx)?
		};
		builder.push_root(root);
	}
	Ok(builder.build())
}

fn bind_expr_into_program(
	builder: &mut BoundExprProgramBuilder,
	expr: &Expr,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
) -> Result<BoundExprId, BindingError> {
	match expr {
		Expr::Column(column_ref) => {
			let column = resolve_expr_column(column_ref, input_schema, ctx.relation_scope)?;
			let node = builder.append_column(column.column);
			if !builder.set_node_logical_type(node, column.logical_type) {
				return Err(BindingError::UnsupportedExpression {
					message: "logical column target is malformed".to_string(),
				});
			}
			Ok(node)
		}
		Expr::Literal(value) => Ok(builder.append_literal(value.clone())),
		Expr::UntypedNull => Err(BindingError::UnsupportedExpression {
			message: "NULL literal has no type in this position; wrap it in CAST(NULL AS <type>)"
				.to_string(),
		}),
		Expr::LogicalAscription { expr, logical_type } => {
			bind_logical_ascription(builder, input_schema, ctx, expr, *logical_type)
		}
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast,
		} => bind_sql_logical_cast(builder, input_schema, ctx, expr, logical_name, *try_cast),
		Expr::Cast(inner, target) => bind_explicit_cast(builder, input_schema, ctx, inner, target),
		Expr::TryCast(inner, target) => bind_try_cast(builder, input_schema, ctx, inner, target),
		Expr::Add(left, right) => {
			bind_arithmetic(builder, input_schema, ctx, left, right, BoundBinaryOp::Add)
		}
		Expr::Sub(left, right) => {
			bind_arithmetic(builder, input_schema, ctx, left, right, BoundBinaryOp::Sub)
		}
		Expr::Mul(left, right) => {
			bind_arithmetic(builder, input_schema, ctx, left, right, BoundBinaryOp::Mul)
		}
		Expr::Div(left, right) => {
			bind_arithmetic(builder, input_schema, ctx, left, right, BoundBinaryOp::Div)
		}
		Expr::Eq(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Eq)
		}
		Expr::Neq(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Neq)
		}
		Expr::Lt(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Lt)
		}
		Expr::Lte(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Lte)
		}
		Expr::Gt(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Gt)
		}
		Expr::Gte(left, right) => {
			bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Gte)
		}
		Expr::And(left, right) => {
			bind_boolean(builder, input_schema, ctx, left, right, BoundBinaryOp::And)
		}
		Expr::Or(left, right) => {
			bind_boolean(builder, input_schema, ctx, left, right, BoundBinaryOp::Or)
		}
		Expr::Not(inner) => {
			let inner = bind_boolean_context_expr(builder, input_schema, ctx, "NOT", inner)?;
			Ok(builder.append_unary(BoundUnaryOp::Not, inner, DataType::Boolean))
		}
		Expr::InList {
			expr,
			list,
			negated,
		} => bind_in_list(builder, input_schema, ctx, expr, list, *negated),
		Expr::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => bind_like(
			builder,
			input_schema,
			ctx,
			expr,
			pattern,
			*negated,
			*case_insensitive,
		),
		Expr::Case {
			when_then,
			else_expr,
		} => bind_case(builder, input_schema, ctx, when_then, else_expr.as_deref()),
		Expr::IsNull(inner) => {
			let inner = bind_null_test_input(builder, input_schema, ctx, inner)?;
			Ok(builder.append_unary(BoundUnaryOp::IsNull, inner, DataType::Boolean))
		}
		Expr::IsNotNull(inner) => {
			let inner = bind_null_test_input(builder, input_schema, ctx, inner)?;
			Ok(builder.append_unary(BoundUnaryOp::IsNotNull, inner, DataType::Boolean))
		}
		Expr::TimeBucket(interval, inner) => {
			if !ctx.config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "TimeBucket",
				});
			}
			let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
			if builder.node_type(inner) != Some(&DataType::Int64) {
				return Err(BindingError::TypeMismatch {
					message: "TimeBucket expects Int64 (ms) input".to_string(),
				});
			}
			Ok(builder.append_time_bucket(*interval, inner, DataType::Int64))
		}
	}
}

fn bind_logical_ascription(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	logical_type: LogicalTypeId,
) -> Result<BoundExprId, BindingError> {
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let physical = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed logical ascription input");
	validate_logical_ascription_physical(&physical, logical_type)?;
	let existing = builder
		.node_logical_type(inner)
		.expect("builder returns logical type for logical ascription input");
	let logical_type = reconcile_identity(existing, logical_type, &physical)?;
	if !builder.set_node_logical_type(inner, logical_type) {
		return Err(BindingError::UnsupportedExpression {
			message: "logical ascription target is malformed".to_string(),
		});
	}
	Ok(inner)
}

fn bind_sql_logical_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	logical_name: &str,
	try_cast: bool,
) -> Result<BoundExprId, BindingError> {
	let spec =
		ctx.logical_casts
			.get(logical_name)
			.ok_or_else(|| BindingError::UnsupportedExpression {
				message: format!(
					"internal: SQL logical CAST target {logical_name} was not resolved"
				),
			})?;
	if is_untyped_null(inner) {
		let output = resolve_untyped_null(builder, &spec.physical)?;
		let existing = builder
			.node_logical_type(output)
			.expect("builder returns logical type for SQL logical NULL cast input");
		let logical_type = reconcile_identity(existing, spec.id, &spec.physical)?;
		if !builder.set_node_logical_type(output, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "SQL logical cast target is malformed".to_string(),
			});
		}
		return Ok(output);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed SQL logical cast input");
	let existing = builder
		.node_logical_type(inner)
		.expect("builder returns logical type for SQL logical cast input");
	let logical_type = reconcile_identity(existing, spec.id, &source)?;
	let output = if try_cast {
		if source == spec.physical || is_supported_explicit_numeric_cast(&source, &spec.physical) {
			builder.append_try_cast(inner, spec.physical.clone(), spec.physical.clone())
		} else {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"unsupported TRY_CAST: {source:?} to logical type {}({:?}); only same-physical casts or Int32, Int64, Float32, and Float64 numeric conversions are supported",
					spec.name, spec.physical
				),
			});
		}
	} else if source == spec.physical {
		inner
	} else if is_supported_explicit_numeric_cast(&source, &spec.physical) {
		builder.append_cast(inner, spec.physical.clone(), spec.physical.clone())
	} else {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to logical type {}({:?}); only same-physical casts or Int32, Int64, Float32, and Float64 numeric conversions are supported",
				spec.name, spec.physical
			),
		});
	};
	if !builder.set_node_logical_type(output, logical_type) {
		return Err(BindingError::UnsupportedExpression {
			message: "SQL logical cast target is malformed".to_string(),
		});
	}
	Ok(output)
}

fn validate_logical_ascription_physical(
	physical: &DataType,
	logical_type: LogicalTypeId,
) -> Result<(), BindingError> {
	if logical_type.is_naked() {
		return Ok(());
	}
	let Some(spec) = logical_type_spec(logical_type) else {
		return Err(BindingError::UnsupportedExpression {
			message: format!("unknown logical type {}", logical_type_name(logical_type)),
		});
	};
	if spec.physical != *physical {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"logical type {} expects {:?}, got {physical:?}",
				spec.name, spec.physical
			),
		});
	}
	Ok(())
}

fn bind_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	match (is_untyped_null(left), is_untyped_null(right)) {
		(true, true) => {
			return Err(BindingError::TypeMismatch {
				message: "arithmetic with NULL requires a typed operand".to_string(),
			});
		}
		(true, false) => {
			let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
			let right_type = builder
				.node_type(right)
				.cloned()
				.expect("builder returns typed right node");
			let output_type =
				arithmetic_output_type(&right_type, &right_type).ok_or_else(|| {
					BindingError::TypeMismatch {
						message: format!(
							"arithmetic operands must be Int32, Int64, Float32, or Float64, got NULL and {right_type:?}"
						),
					}
				})?;
			let left = resolve_untyped_null(builder, &output_type)?;
			let right = cast_if_needed(builder, right, &output_type)?;
			return Ok(builder.append_binary(op, left, right, output_type));
		}
		(false, true) => {
			let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
			let left_type = builder
				.node_type(left)
				.cloned()
				.expect("builder returns typed left node");
			let output_type = arithmetic_output_type(&left_type, &left_type).ok_or_else(|| {
				BindingError::TypeMismatch {
					message: format!(
						"arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and NULL"
					),
				}
			})?;
			let left = cast_if_needed(builder, left, &output_type)?;
			let right = resolve_untyped_null(builder, &output_type)?;
			return Ok(builder.append_binary(op, left, right, output_type));
		}
		(false, false) => {}
	}
	let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
	let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed left node");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed right node");
	let output_type =
		arithmetic_output_type(&left_type, &right_type).ok_or_else(|| {
			BindingError::TypeMismatch {
				message: format!(
					"arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and {right_type:?}"
				),
			}
		})?;
	let left = cast_if_needed(builder, left, &output_type)?;
	let right = cast_if_needed(builder, right, &output_type)?;
	Ok(builder.append_binary(op, left, right, output_type))
}

fn bind_case(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	when_then: &[(Box<Expr>, Box<Expr>)],
	else_expr: Option<&Expr>,
) -> Result<BoundExprId, BindingError> {
	if when_then.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "CASE must have at least one WHEN branch".to_string(),
		});
	}

	let mut bound_when_then_inputs = Vec::with_capacity(when_then.len());
	let mut typed_result_ids =
		Vec::with_capacity(when_then.len() + if else_expr.is_some() { 1 } else { 0 });
	for (condition, result) in when_then {
		let condition =
			bind_boolean_context_expr(builder, input_schema, ctx, "CASE WHEN", condition)?;
		let result = bind_case_result_input(builder, input_schema, ctx, result)?;
		if let CaseResultInput::Bound(result_id) = result {
			typed_result_ids.push(result_id);
		}
		bound_when_then_inputs.push((condition, result));
	}

	let bound_else = if let Some(else_expr) = else_expr {
		let else_input = bind_case_result_input(builder, input_schema, ctx, else_expr)?;
		if let CaseResultInput::Bound(else_id) = else_input {
			typed_result_ids.push(else_id);
		}
		Some(else_input)
	} else {
		None
	};

	let result_types = typed_result_ids
		.iter()
		.map(|id| {
			builder
				.node_type(*id)
				.cloned()
				.expect("builder returns typed CASE result")
		})
		.collect::<Vec<_>>();
	let (output_type, output_logical_type) = if result_types.is_empty() {
		(DataType::Null, LogicalTypeId::NAKED)
	} else {
		let output_type = case_common_type(&result_types)?;
		let output_logical_type =
			case_output_logical_type(builder, &typed_result_ids, &output_type)?;
		(output_type, output_logical_type)
	};

	let mut bound_when_then = Vec::with_capacity(bound_when_then_inputs.len());
	for (condition, result) in bound_when_then_inputs {
		bound_when_then.push((condition, type_case_result(builder, result, &output_type)?));
	}
	let else_input = if let Some(bound_else) = bound_else {
		type_case_result(builder, bound_else, &output_type)?
	} else {
		materialize_null_for_output_type(builder, &output_type)?
	};

	let case_id = builder.append_case(bound_when_then, else_input, output_type);
	if !output_logical_type.is_naked()
		&& !builder.set_node_logical_type(case_id, output_logical_type)
	{
		return Err(BindingError::UnsupportedExpression {
			message: "CASE logical identity target is malformed".to_string(),
		});
	}
	Ok(case_id)
}

#[derive(Debug, Clone, Copy)]
enum CaseResultInput {
	Bound(BoundExprId),
	UntypedNull,
}

fn bind_case_result_input(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	expr: &Expr,
) -> Result<CaseResultInput, BindingError> {
	if is_untyped_null(expr) {
		return Ok(CaseResultInput::UntypedNull);
	}
	Ok(CaseResultInput::Bound(bind_expr_into_program(
		builder,
		expr,
		input_schema,
		ctx,
	)?))
}

fn case_output_logical_type(
	builder: &BoundExprProgramBuilder,
	result_ids: &[BoundExprId],
	output_type: &DataType,
) -> Result<LogicalTypeId, BindingError> {
	let mut acc_logical = LogicalTypeId::NAKED;
	let mut acc_is_literal = true;
	for &id in result_ids {
		let branch_type = builder
			.node_type(id)
			.cloned()
			.expect("builder returns typed CASE result");
		let branch_logical = builder
			.node_logical_type(id)
			.expect("builder returns logical type for CASE result");
		if !branch_logical.is_naked() && branch_type != *output_type {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"CASE logical result {} has physical {branch_type:?}, but common CASE output is {output_type:?}; cast explicitly to erase identity before widening",
					logical_type_name(branch_logical)
				),
			});
		}

		let is_literal = builder.literal(id).is_some();
		nominal_compatible(
			output_type,
			output_type,
			acc_logical,
			branch_logical,
			acc_is_literal,
			is_literal,
		)?;
		if !branch_logical.is_naked() {
			acc_logical = branch_logical;
			acc_is_literal = false;
		} else if !is_literal {
			acc_is_literal = false;
		}
	}
	Ok(acc_logical)
}

fn type_case_result(
	builder: &mut BoundExprProgramBuilder,
	input: CaseResultInput,
	output_type: &DataType,
) -> Result<BoundExprId, BindingError> {
	match input {
		CaseResultInput::Bound(id) => cast_if_needed(builder, id, output_type),
		CaseResultInput::UntypedNull => materialize_null_for_output_type(builder, output_type),
	}
}

fn case_common_type(types: &[DataType]) -> Result<DataType, BindingError> {
	let mut iter = types.iter();
	let mut common = iter
		.next()
		.cloned()
		.ok_or_else(|| BindingError::TypeMismatch {
			message: "CASE requires at least one result branch".to_string(),
		})?;
	for data_type in iter {
		if common == *data_type {
			continue;
		}
		common = arithmetic_output_type(&common, data_type).ok_or_else(|| {
			BindingError::TypeMismatch {
				message: format!(
					"CASE result branches cannot be unified: {common:?} and {data_type:?}"
				),
			}
		})?;
	}
	Ok(common)
}

fn bind_explicit_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(inner) {
		return resolve_untyped_null(builder, target);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed cast input");
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(builder.append_cast(inner, target.clone(), target.clone()))
}

fn bind_try_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(inner) {
		return resolve_untyped_null(builder, target);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed try-cast input");
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(builder.append_try_cast(inner, target.clone(), target.clone()))
}

fn cast_if_needed(
	builder: &mut BoundExprProgramBuilder,
	expr: BoundExprId,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	let source = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed cast input");
	if source == *target {
		return Ok(expr);
	}
	if !is_widening_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"Cast from {source:?} to {target:?} is not a supported widening conversion"
			),
		});
	}
	Ok(builder.append_cast(expr, target.clone(), target.clone()))
}

fn is_widening_cast(source: &DataType, target: &DataType) -> bool {
	matches!(
		(source, target),
		(DataType::Int32, DataType::Int64)
			| (DataType::Int32, DataType::Float64)
			| (DataType::Int64, DataType::Float64)
			| (DataType::Float32, DataType::Float64)
	)
}

fn is_supported_explicit_numeric_cast(source: &DataType, target: &DataType) -> bool {
	matches!(
		(source, target),
		(DataType::Int32, DataType::Int32)
			| (DataType::Int32, DataType::Int64)
			| (DataType::Int32, DataType::Float32)
			| (DataType::Int32, DataType::Float64)
			| (DataType::Int64, DataType::Int32)
			| (DataType::Int64, DataType::Int64)
			| (DataType::Int64, DataType::Float32)
			| (DataType::Int64, DataType::Float64)
			| (DataType::Float32, DataType::Int32)
			| (DataType::Float32, DataType::Int64)
			| (DataType::Float32, DataType::Float32)
			| (DataType::Float32, DataType::Float64)
			| (DataType::Float64, DataType::Int32)
			| (DataType::Float64, DataType::Int64)
			| (DataType::Float64, DataType::Float32)
			| (DataType::Float64, DataType::Float64)
	)
}

fn is_insert_select_coercible(source: &DataType, target: &DataType) -> bool {
	source == target
		|| matches!(
			(source, target),
			(DataType::Int32, DataType::Int64)
				| (DataType::Int32, DataType::Float32)
				| (DataType::Int32, DataType::Float64)
				| (DataType::Int64, DataType::Int32)
				| (DataType::Int64, DataType::Float32)
				| (DataType::Int64, DataType::Float64)
				| (DataType::Float32, DataType::Int32)
				| (DataType::Float32, DataType::Int64)
				| (DataType::Float32, DataType::Float64)
				| (DataType::Float64, DataType::Int32)
				| (DataType::Float64, DataType::Int64)
				| (DataType::Float64, DataType::Float32)
		)
}

fn coerce_insert_literal(
	literal: &InsertLiteral,
	field: &Field,
) -> Result<ScalarValue, BindingError> {
	let target = field.data_type();
	let column = field.name();
	match literal {
		InsertLiteral::Null => {
			if field.is_nullable() {
				typed_null_for_insert(field)
			} else {
				Err(BindingError::TypeMismatch {
					message: format!("NULL is not allowed for non-nullable column: {column}"),
				})
			}
		}
		InsertLiteral::Boolean(value) => match target {
			DataType::Boolean => Ok(ScalarValue::Boolean(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Int64(value) => match target {
			DataType::Int32 => i32::try_from(*value)
				.map(|value| ScalarValue::Int32(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Int64 => Ok(ScalarValue::Int64(Some(*value))),
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value as f64))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::UInt64(value) => match target {
			DataType::Int32 => i32::try_from(*value)
				.map(|value| ScalarValue::Int32(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Int64 => i64::try_from(*value)
				.map(|value| ScalarValue::Int64(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value as f64))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Float64(value) => match target {
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Utf8(value) => match target {
			DataType::Utf8 => Ok(ScalarValue::Utf8(Some(value.clone()))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
	}
}

fn typed_null_for_insert(field: &Field) -> Result<ScalarValue, BindingError> {
	typed_null_for_data_type(field.data_type())
}

fn insert_type_mismatch(column: &str, literal: &InsertLiteral, target: &DataType) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"INSERT value {literal:?} is not compatible with column {column} type {target:?}"
		),
	}
}

fn insert_overflow(column: &str, literal: &InsertLiteral, target: &DataType) -> BindingError {
	BindingError::TypeMismatch {
		message: format!("INSERT value {literal:?} overflows column {column} type {target:?}"),
	}
}

fn bind_comparison(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	match (is_untyped_null(left), is_untyped_null(right)) {
		(true, true) => {
			return Err(BindingError::TypeMismatch {
				message: "NULL comparison requires a typed operand".to_string(),
			});
		}
		(true, false) => {
			let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
			let right_type = builder
				.node_type(right)
				.cloned()
				.expect("builder returns typed comparison right");
			let left = resolve_untyped_null(builder, &right_type)?;
			nominal_compatible(
				&right_type,
				&right_type,
				builder
					.node_logical_type(left)
					.expect("builder returns logical type for comparison left"),
				builder
					.node_logical_type(right)
					.expect("builder returns logical type for comparison right"),
				true,
				builder.literal(right).is_some(),
			)?;
			return Ok(builder.append_binary(op, left, right, DataType::Boolean));
		}
		(false, true) => {
			let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
			let left_type = builder
				.node_type(left)
				.cloned()
				.expect("builder returns typed comparison left");
			let right = resolve_untyped_null(builder, &left_type)?;
			nominal_compatible(
				&left_type,
				&left_type,
				builder
					.node_logical_type(left)
					.expect("builder returns logical type for comparison left"),
				builder
					.node_logical_type(right)
					.expect("builder returns logical type for comparison right"),
				builder.literal(left).is_some(),
				true,
			)?;
			return Ok(builder.append_binary(op, left, right, DataType::Boolean));
		}
		(false, false) => {}
	}
	let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
	let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
	coerce_comparison_literals(builder, left, right);
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed comparison left");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed comparison right");
	if left_type != right_type {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"comparison operands have different types: {:?} and {:?}",
				left_type, right_type
			),
		});
	}
	nominal_compatible(
		&left_type,
		&right_type,
		builder
			.node_logical_type(left)
			.expect("builder returns logical type for comparison left"),
		builder
			.node_logical_type(right)
			.expect("builder returns logical type for comparison right"),
		builder.literal(left).is_some(),
		builder.literal(right).is_some(),
	)?;
	Ok(builder.append_binary(op, left, right, DataType::Boolean))
}

fn bind_in_list(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	expr: &Expr,
	list: &[Expr],
	negated: bool,
) -> Result<BoundExprId, BindingError> {
	if list.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "IN list must not be empty".to_string(),
		});
	}
	if is_untyped_null(expr) {
		return bind_in_list_with_untyped_target(builder, input_schema, ctx, list, negated);
	}
	let expr = bind_expr_into_program(builder, expr, input_schema, ctx)?;
	let mut bound_items = Vec::with_capacity(list.len());
	for item in list {
		let expr_type = builder
			.node_type(expr)
			.cloned()
			.expect("builder returns typed IN target");
		let item = if is_untyped_null(item) {
			resolve_untyped_null(builder, &expr_type)?
		} else {
			bind_expr_into_program(builder, item, input_schema, ctx)?
		};
		coerce_comparison_literals(builder, expr, item);
		let current_expr_type = builder
			.node_type(expr)
			.cloned()
			.expect("builder returns typed IN target");
		let item_type = builder
			.node_type(item)
			.cloned()
			.expect("builder returns typed IN member");
		if current_expr_type != item_type {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"IN operands have different types: {:?} and {:?}",
					current_expr_type, item_type
				),
			});
		}
		nominal_compatible(
			&current_expr_type,
			&item_type,
			builder
				.node_logical_type(expr)
				.expect("builder returns logical type for IN target"),
			builder
				.node_logical_type(item)
				.expect("builder returns logical type for IN member"),
			builder.literal(expr).is_some(),
			builder.literal(item).is_some(),
		)?;
		bound_items.push(item);
	}
	Ok(builder.append_in_list(expr, bound_items, negated))
}

#[derive(Debug, Clone, Copy)]
enum InListInput {
	Bound(BoundExprId),
	UntypedNull,
}

fn bind_in_list_with_untyped_target(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	list: &[Expr],
	negated: bool,
) -> Result<BoundExprId, BindingError> {
	let mut inputs = Vec::with_capacity(list.len());
	let mut types = Vec::new();
	for item in list {
		if is_untyped_null(item) {
			inputs.push(InListInput::UntypedNull);
		} else {
			let item = bind_expr_into_program(builder, item, input_schema, ctx)?;
			types.push(
				builder
					.node_type(item)
					.cloned()
					.expect("builder returns typed IN member"),
			);
			inputs.push(InListInput::Bound(item));
		}
	}
	if types.is_empty() {
		return Err(BindingError::TypeMismatch {
			message: "NULL IN list requires a typed member".to_string(),
		});
	}
	let target_type = case_common_type(&types)?;
	let expr = resolve_untyped_null(builder, &target_type)?;
	let mut bound_items = Vec::with_capacity(inputs.len());
	for input in inputs {
		let item = match input {
			InListInput::Bound(item) => cast_if_needed(builder, item, &target_type)?,
			InListInput::UntypedNull => resolve_untyped_null(builder, &target_type)?,
		};
		let item_type = builder
			.node_type(item)
			.cloned()
			.expect("builder returns typed IN member");
		if item_type != target_type {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"IN operands have different types: {:?} and {:?}",
					target_type, item_type
				),
			});
		}
		bound_items.push(item);
	}
	Ok(builder.append_in_list(expr, bound_items, negated))
}

fn nominal_compatible(
	left_physical: &DataType,
	right_physical: &DataType,
	left_logical: LogicalTypeId,
	right_logical: LogicalTypeId,
	left_is_literal: bool,
	right_is_literal: bool,
) -> Result<(), BindingError> {
	debug_assert_eq!(left_physical, right_physical);
	if left_logical == right_logical {
		return Ok(());
	}
	if left_logical.is_naked() && right_logical.is_naked() {
		return Ok(());
	}
	if left_logical.is_naked() && left_is_literal {
		return Ok(());
	}
	if right_logical.is_naked() && right_is_literal {
		return Ok(());
	}
	Err(BindingError::NominalTypeMismatch {
		left: logical_type_name(left_logical),
		right: logical_type_name(right_logical),
		physical: format!("{left_physical:?}"),
	})
}

fn reconcile_identity(
	existing: LogicalTypeId,
	declared: LogicalTypeId,
	physical: &DataType,
) -> Result<LogicalTypeId, BindingError> {
	if existing == declared {
		return Ok(existing);
	}
	if declared.is_naked() {
		return Ok(existing);
	}
	if existing.is_naked() {
		return Ok(declared);
	}
	Err(BindingError::NominalTypeMismatch {
		left: logical_type_name(existing),
		right: logical_type_name(declared),
		physical: format!("{physical:?}"),
	})
}

fn bind_like(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	expr: &Expr,
	pattern: &str,
	negated: bool,
	case_insensitive: bool,
) -> Result<BoundExprId, BindingError> {
	let expr = bind_expr_into_program(builder, expr, input_schema, ctx)?;
	let expr_type = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed LIKE input");
	if expr_type != DataType::Utf8 {
		return Err(BindingError::TypeMismatch {
			message: format!("LIKE expects Utf8 input, got {expr_type:?}"),
		});
	}
	Ok(builder.append_like(expr, pattern.to_string(), negated, case_insensitive))
}

fn coerce_comparison_literals(
	builder: &mut BoundExprProgramBuilder,
	left: BoundExprId,
	right: BoundExprId,
) {
	if builder.literal(left).is_some() && builder.literal(right).is_some() {
		return;
	}
	if let Some(value) = builder.literal(left).cloned() {
		let Some(data_type) = builder.node_type(right).cloned() else {
			return;
		};
		let _ = builder.rewrite_literal(left, coerce_literal_value(value, &data_type));
		return;
	}
	if let Some(value) = builder.literal(right).cloned() {
		let Some(data_type) = builder.node_type(left).cloned() else {
			return;
		};
		let _ = builder.rewrite_literal(right, coerce_literal_value(value, &data_type));
	}
}

fn coerce_literal_value(value: ScalarValue, target_type: &DataType) -> ScalarValue {
	match (value, target_type) {
		(ScalarValue::Int32(value), DataType::Int64) => ScalarValue::Int64(value.map(i64::from)),
		(ScalarValue::Int32(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(ScalarValue::Int32(value), DataType::Float64) => {
			ScalarValue::Float64(value.map(f64::from))
		}
		(ScalarValue::Int64(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(ScalarValue::Int64(value), DataType::Float64) => {
			ScalarValue::Float64(value.map(|value| value as f64))
		}
		(ScalarValue::Float64(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(value, _) => value,
	}
}

fn bind_boolean(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	let left =
		bind_boolean_context_expr(builder, input_schema, ctx, "boolean operator left", left)?;
	let right =
		bind_boolean_context_expr(builder, input_schema, ctx, "boolean operator right", right)?;
	Ok(builder.append_binary(op, left, right, DataType::Boolean))
}

fn bind_boolean_context_expr(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	context: &'static str,
	expr: &Expr,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(expr) {
		return resolve_untyped_null(builder, &DataType::Boolean);
	}
	let expr = bind_expr_into_program(builder, expr, input_schema, ctx)?;
	require_boolean(builder, context, expr)?;
	Ok(expr)
}

fn bind_null_test_input(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	expr: &Expr,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(expr) {
		return resolve_untyped_null(builder, &DataType::Boolean);
	}
	bind_expr_into_program(builder, expr, input_schema, ctx)
}

fn require_boolean(
	builder: &BoundExprProgramBuilder,
	context: &'static str,
	expr: BoundExprId,
) -> Result<(), BindingError> {
	let data_type = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed boolean input");
	if data_type == DataType::Boolean {
		Ok(())
	} else {
		Err(BindingError::TypeMismatch {
			message: format!("{context} expected Boolean, got {data_type:?}"),
		})
	}
}

fn resolve_column(name: &str, input_schema: &SchemaRef) -> Result<ResolvedColumn, BindingError> {
	let matches = input_schema
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| field.name() == name)
		.map(|(idx, _)| idx)
		.collect::<Vec<_>>();
	let ordinal = match matches.as_slice() {
		[] => {
			return Err(BindingError::UnknownColumn {
				column: name.to_string(),
			});
		}
		[ordinal] => *ordinal,
		_ => {
			return Err(BindingError::AmbiguousColumn {
				column: name.to_string(),
			});
		}
	};
	let field = input_schema.field(ordinal);
	Ok(ResolvedColumn {
		name: ColumnName(field.name().clone()),
		ordinal: ColumnOrdinal(u32::try_from(ordinal).expect("column ordinal overflow")),
		data_type: field.data_type().clone(),
		nullable: field.is_nullable(),
		pbc_eligible: is_pbc_eligible_type(field.data_type()),
	})
}

fn resolve_expr_column(
	column_ref: &ColumnRef,
	input_schema: &SchemaRef,
	relation_scope: Option<&RelationScope>,
) -> Result<ResolvedLogicalColumn, BindingError> {
	if column_ref.qualifier.is_none() {
		return resolve_logical_column(&column_ref.name.0, input_schema);
	}
	let Some(scope) = relation_scope else {
		return Err(BindingError::UnknownTableRef {
			qualifier: table_name(column_ref.qualifier.as_ref().expect("checked")),
		});
	};
	resolve_qualified_column(column_ref, scope)
}

fn resolve_logical_column(
	name: &str,
	input_schema: &SchemaRef,
) -> Result<ResolvedLogicalColumn, BindingError> {
	let column = resolve_column(name, input_schema)?;
	resolved_logical_column_at_ordinal(input_schema, column.ordinal)
}

fn resolved_logical_column_at_ordinal(
	input_schema: &SchemaRef,
	ordinal: ColumnOrdinal,
) -> Result<ResolvedLogicalColumn, BindingError> {
	let field = input_schema.field(ordinal.0 as usize);
	let logical_type =
		decode_column_logical_type(field).map_err(|error| BindingError::UnsupportedExpression {
			message: error.to_string(),
		})?;
	Ok(ResolvedLogicalColumn {
		column: ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal,
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(field.data_type()),
		},
		logical_type,
	})
}

fn aggregate_schema(group_expr: &BoundExprProgram, aggr_expr: &[BoundAggregateCall]) -> SchemaRef {
	let mut fields = Vec::new();
	let group_schema = schema_for_exprs(group_expr);
	fields.extend(
		group_schema
			.fields()
			.iter()
			.map(|field| field.as_ref().clone()),
	);
	for call in aggr_expr {
		fields.push(aggregate_field(call));
	}
	Arc::new(Schema::new(fields))
}

fn schema_for_exprs_with_output_names(
	exprs: &BoundExprProgram,
	output_names: &[Option<String>],
) -> SchemaRef {
	debug_assert_eq!(exprs.root_count(), output_names.len());
	let derived = schema_for_exprs(exprs);
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = output_names[idx]
				.as_ref()
				.cloned()
				.unwrap_or_else(|| derived.field(idx).name().clone());
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
			field_with_logical_type_metadata(name, data_type, nullable, logical_type)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn schema_for_projected_exprs_with_existing_names(
	exprs: &BoundExprProgram,
	existing_schema: &SchemaRef,
	requested_outputs: &[ColumnOrdinal],
) -> SchemaRef {
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = requested_outputs
				.get(idx)
				.and_then(|ordinal| existing_schema.fields().get(ordinal.0 as usize))
				.map(|field| field.name().clone())
				.unwrap_or_else(|| {
					exprs
						.root_display_name(idx)
						.map(str::to_string)
						.unwrap_or_else(|| format!("col_{idx}"))
				});
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
			field_with_logical_type_metadata(name, data_type, nullable, logical_type)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn aggregate_field(call: &BoundAggregateCall) -> Field {
	let field =
		aggregate_kernel::stable_output_field(call).unwrap_or_else(|| match call.function {
			AggregateFunction::LinearRegress => Field::new("linreg", DataType::Float64, true),
			AggregateFunction::ApproxQuantile(_) => Field::new("quantile", DataType::Float64, true),
			AggregateFunction::CountAll
			| AggregateFunction::Count
			| AggregateFunction::Sum
			| AggregateFunction::Min
			| AggregateFunction::Max
			| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
		});
	let logical_type = aggregate_output_logical_type(call, field.data_type());
	if logical_type.is_naked() {
		field
	} else {
		field_with_logical_type_metadata(
			field.name().clone(),
			field.data_type().clone(),
			field.is_nullable(),
			logical_type,
		)
	}
}

fn aggregate_output_logical_type(
	call: &BoundAggregateCall,
	output_type: &DataType,
) -> LogicalTypeId {
	match call.function {
		AggregateFunction::Min | AggregateFunction::Max => {
			let Some(logical_type) = call.args.root_logical_type(0) else {
				return LogicalTypeId::NAKED;
			};
			let Some(input_type) = call.args.root_type(0) else {
				return LogicalTypeId::NAKED;
			};
			if logical_type.is_naked() || input_type != output_type {
				LogicalTypeId::NAKED
			} else {
				logical_type
			}
		}
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Avg
		| AggregateFunction::LinearRegress
		| AggregateFunction::ApproxQuantile(_) => LogicalTypeId::NAKED,
	}
}

fn is_pbc_eligible_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
			| DataType::LargeUtf8
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{
		DescriptorColumnNdvFact, DescriptorOptimizerFacts, MockCatalog, WendelonCatalog,
	};
	use crate::distributed::arrow_ipc::encode_record_batch;
	use crate::ir::{BoundBinaryOp, BoundExprNode, QueryProfileHint, ScalarValue, TableId};
	use crate::rune_engine::{RuneEngine, RuneSandboxConfig};
	use crate::sql_planner::SqlPlanner;
	use arrow::array::{
		ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray,
	};
	use arrow::datatypes::{Field, Schema};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

	#[test]
	fn predicate_pushdown_emitted_splits_preserve_kleene_3vl() {
		for program in [
			property_common_or_factor_program(),
			property_common_or_three_branch_program(),
			property_or_branch_strips_to_true_program(),
			property_not_or_program(),
			property_not_or_partial_push_program(),
			property_top_level_and_program(),
		] {
			let (pushed, residual) = partition_bound_predicate(&program).expect("predicate split");
			assert!(
				pushed.is_some(),
				"property fixture must emit a pushed predicate"
			);

			for p in TestK3::VALUES {
				for r1 in TestK3::VALUES {
					for r2 in TestK3::VALUES {
						let row = [p, r1, r2];
						let original = eval_test_program(&program, &row);
						let split = k3_and(
							pushed
								.as_ref()
								.map(|program| eval_test_program(program, &row))
								.unwrap_or(TestK3::True),
							residual
								.as_ref()
								.map(|program| eval_test_program(program, &row))
								.unwrap_or(TestK3::True),
						);
						assert_eq!(split, original, "split must preserve exact K3 value");
					}
				}
			}
		}
	}

	#[test]
	fn where_not_or_all_branch_negations_push_removes_residual() {
		let mut builder = BoundExprProgramBuilder::new();
		let p = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, true));
		let disjunction = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
		let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
		builder.push_root(root);
		let program = builder.build();

		let split = try_extract_not_or_branch_negations(&program, root, disjunction)
			.expect("extract not-or")
			.expect("all branch negations should push");
		assert!(split.pushed.is_some());
		assert!(
			split.residual.is_none(),
			"all pushed NOT(OR) branches must remove residual"
		);
	}

	#[tokio::test]
	async fn binder_resolves_columns_to_ordinals() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("amount")],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		match plan {
			ExecutionPlan::Project { input, exprs, .. } => {
				let column = exprs.single_column_root().expect("project column root");
				assert_eq!(column.ordinal, ColumnOrdinal(0));
				assert_eq!(column.data_type, DataType::Int32);
				assert!(column.nullable);
				assert!(column.pbc_eligible);
				match *input {
					ExecutionPlan::Scan {
						projection, schema, ..
					} => {
						assert_eq!(projection, Some(vec![ColumnOrdinal(1)]));
						assert_eq!(schema.field(0).name(), "amount");
					}
					other => panic!("expected scan input, got {other:?}"),
				}
			}
			other => panic!("expected project, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_pushes_exact_projection_union_through_filter() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
				predicate: Expr::Gt(
					Box::new(Expr::column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				),
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("id")],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		match plan {
			ExecutionPlan::Project { input, exprs, .. } => {
				let column = exprs.single_column_root().expect("project column root");
				assert_eq!(column.ordinal, ColumnOrdinal(0));
				match *input {
					ExecutionPlan::Filter {
						input,
						predicate,
						disposition,
					} => {
						assert_eq!(disposition, FilterDisposition::FullyPushed);
						let root = predicate.single_root().expect("predicate root");
						match predicate.node(root).expect("predicate node") {
							BoundExprNode::Binary {
								op: BoundBinaryOp::Gt,
								left,
								..
							} => match predicate.node(*left).expect("predicate left") {
								BoundExprNode::Column(column) => {
									assert_eq!(column.ordinal, ColumnOrdinal(1))
								}
								other => panic!("expected predicate column, got {other:?}"),
							},
							other => panic!("expected GT predicate, got {other:?}"),
						}
						match *input {
							ExecutionPlan::Scan {
								projection, schema, ..
							} => {
								assert_eq!(projection, Some(vec![ColumnOrdinal(0)]));
								assert_eq!(schema.fields().len(), 1);
								assert_eq!(schema.field(0).name(), "id");
							}
							other => panic!("expected scan input, got {other:?}"),
						}
					}
					other => panic!("expected filter input, got {other:?}"),
				}
			}
			other => panic!("expected project, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_rejects_unknown_columns() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("missing")],
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("unknown column should fail");
		assert!(matches!(
			err,
			BindingError::UnknownColumn { column } if column == "missing"
		));
	}

	#[tokio::test]
	async fn binder_gates_experimental_kernels_by_default() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			output_names: vec![None],
			exprs: vec![Expr::TimeBucket(
				std::time::Duration::from_secs(60),
				Box::new(Expr::column("ts")),
			)],
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("experimental kernel should fail by default");
		assert!(matches!(
			err,
			BindingError::ExperimentalKernel {
				kernel: "TimeBucket"
			}
		));

		let plan = bind(
			&plan,
			&catalog,
			&PlannerConfig {
				allow_experimental_kernels: true,
				..PlannerConfig::default()
			},
		)
		.await
		.expect("experimental kernel enabled");
		assert!(matches!(plan, ExecutionPlan::Project { .. }));
	}

	#[tokio::test]
	async fn binder_shapes_grouped_aggregate_plan() {
		let catalog = catalog();
		let plan = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind grouped aggregate");
		match plan {
			ExecutionPlan::Aggregate { output_schema, .. } => {
				assert_eq!(output_schema.field(0).name(), "region");
				assert_eq!(output_schema.field(1).name(), "sum");
			}
			other => panic!("expected aggregate, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_attaches_pbc_program_for_eligible_scan_filter() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind eligible filter");
		match plan {
			ExecutionPlan::Filter {
				input, disposition, ..
			} => {
				assert_eq!(disposition, FilterDisposition::FullyPushed);
				match *input {
					ExecutionPlan::Scan { pbc_program, .. } => assert!(pbc_program.is_some()),
					other => panic!("expected scan input, got {other:?}"),
				}
			}
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_selects_point_read_profile_for_primary_key_equality() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind primary-key equality");

		match plan {
			ExecutionPlan::Filter { input, .. } => match *input {
				ExecutionPlan::Scan {
					profile_hint,
					primary_key_columns,
					..
				} => {
					assert_eq!(profile_hint, QueryProfileHint::PointRead);
					assert_eq!(primary_key_columns, vec![ColumnOrdinal(0)]);
				}
				other => panic!("expected scan input, got {other:?}"),
			},
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_keeps_scan_profile_for_non_primary_key_equality() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::column("amount")),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(7)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind non-primary-key equality");

		match plan {
			ExecutionPlan::Filter { input, .. } => match *input {
				ExecutionPlan::Scan { profile_hint, .. } => {
					assert_eq!(profile_hint, QueryProfileHint::Scan);
				}
				other => panic!("expected scan input, got {other:?}"),
			},
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_marks_ineligible_filters_for_toron_evaluation() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("type mismatch should fail before pushdown");
		assert!(matches!(err, BindingError::TypeMismatch { .. }));
	}

	#[tokio::test]
	async fn bind_hash_join_resolves_qualified_keys_and_duplicate_output_names() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new("orders"),
					projection: None,
				}),
				partition_count: 2,
				key: vec![Expr::qualified_column(
					TableRef::new("orders"),
					"customer_id",
				)],
			}),
			right: Box::new(LogicalPlan::Exchange {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new("customers"),
					projection: None,
				}),
				partition_count: 2,
				key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind join");

		match plan {
			ExecutionPlan::Join {
				left_keys,
				right_keys,
				output_schema,
				..
			} => {
				assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
				assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
				assert_eq!(output_schema.fields().len(), 4);
				assert_eq!(output_schema.field(0).name(), "id");
				assert_eq!(output_schema.field(2).name(), "id");
			}
			other => panic!("expected join, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bind_hash_join_auto_swaps_reversed_on_pair() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind join");

		match plan {
			ExecutionPlan::Join {
				left_keys,
				right_keys,
				..
			} => {
				assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
				assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
			}
			other => panic!("expected join, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bind_join_rejects_ambiguous_unqualified_column() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(Expr::column("id"), Expr::column("id"))],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("ambiguous join key should fail");

		assert!(matches!(err, BindingError::AmbiguousColumnRef { name, .. } if name == "id"));
	}

	#[tokio::test]
	async fn bind_join_rejects_unknown_qualifier() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("missing"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("unknown qualifier should fail");

		assert!(matches!(
			err,
			BindingError::UnknownTableRef { qualifier } if qualifier == "missing"
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_non_equi_and_computed_keys() {
		let catalog = join_catalog();
		let non_equi = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::Eq(
					Box::new(Expr::qualified_column(
						TableRef::new("orders"),
						"customer_id",
					)),
					Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
				),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let computed = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::Literal(ScalarValue::Int64(Some(7))),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&non_equi, &catalog, &PlannerConfig::default())
				.await
				.expect_err("non-equi should fail"),
			BindingError::JoinPredicateNotEqui
		));
		assert!(matches!(
			bind(&computed, &catalog, &PlannerConfig::default())
				.await
				.expect_err("computed key should fail"),
			BindingError::JoinComputedKeyUnsupported
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_key_type_mismatch_and_float_keys() {
		let catalog = join_catalog();
		let mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "region", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "region"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("type mismatch should fail"),
			BindingError::JoinKeyTypeMismatch { .. }
		));

		let catalog = float_join_catalog();
		let float_key = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("left_floats", "f", 2)),
			right: Box::new(exchanged_named_scan("right_floats", "f", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("left_floats"), "f"),
				Expr::qualified_column(TableRef::new("right_floats"), "f"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&float_key, &catalog, &PlannerConfig::default())
				.await
				.expect_err("float key should fail"),
			BindingError::JoinKeyTypeUnsupported { .. }
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_exchange_partition_and_key_shape_mismatches() {
		let catalog = join_catalog();
		let partition_mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 3)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let key_mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&partition_mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("partition mismatch should fail"),
			BindingError::JoinPartitionCountMismatch { left: 2, right: 3 }
		));
		assert!(matches!(
			bind(&key_mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("exchange key mismatch should fail"),
			BindingError::JoinKeyExchangeMismatch
		));
	}

	#[tokio::test]
	async fn bind_broadcast_join_enforces_left_single_partition_build() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 1)),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&plan, &catalog, &PlannerConfig::default())
				.await
				.expect("bind broadcast"),
			ExecutionPlan::Join {
				strategy: JoinStrategy::Broadcast,
				..
			}
		));

		let wrong_side = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(exchanged_named_scan("customers", "id", 1)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};

		assert!(matches!(
			bind(&wrong_side, &catalog, &PlannerConfig::default())
				.await
				.expect_err("right-side publish must fail"),
			BindingError::BroadcastBuildSidePlacement
		));
	}

	#[tokio::test]
	async fn bind_broadcast_join_rejects_all_malformed_publish_shapes() {
		let catalog = join_catalog();
		let missing_left_publish = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let wrong_left_partition_count = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 4)),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let both_sides_published = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 1)),
			right: Box::new(exchanged_named_scan("orders", "customer_id", 1)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};

		for plan in [
			missing_left_publish,
			wrong_left_partition_count,
			both_sides_published,
		] {
			assert!(matches!(
				bind(&plan, &catalog, &PlannerConfig::default())
					.await
					.expect_err("malformed broadcast shape must fail"),
				BindingError::BroadcastBuildSidePlacement
			));
		}
	}

	#[tokio::test]
	async fn auto_broadcast_sql_unhinted_tiny_left_join_infers_broadcast() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
			.expect("plan SQL join");
		assert!(matches!(
			logical,
			LogicalPlan::Join {
				strategy_intent: JoinStrategyIntent::Auto,
				..
			}
		));

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind inferred broadcast");

		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = plan
		else {
			panic!("expected join");
		};
		assert_eq!(strategy, JoinStrategy::Broadcast);
		assert!(matches!(terminal_exchange(&left), Some((1, _))));
		assert!(terminal_exchange(&right).is_none());
	}

	#[tokio::test]
	async fn sql_unhinted_unknown_and_large_joins_stay_hash() {
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
			.expect("plan SQL join");

		let unknown = bind(&logical, &join_catalog(), &auto_broadcast_config(4_096))
			.await
			.expect("bind unknown-size join");
		assert_join_strategy(&unknown, JoinStrategy::HashPartitioned);

		let mut large_catalog = join_catalog();
		seed_byte_upper_bound(&mut large_catalog, "customers", 8_192);
		let large = bind(&logical, &large_catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind large join");
		assert_join_strategy(&large, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn planner_default_on_missing_facts_still_hashes() {
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id")
			.expect("plan SQL join");

		let (plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
			.await
			.expect("bind missing-facts default join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
		assert_eq!(trace.join_decisions.len(), 1);
		assert_eq!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::MissingBuildByteUpperBound,
			}
		);
	}

	#[tokio::test]
	async fn sql_fact_left_tiny_right_infers_broadcast() {
		let mut right_tiny_catalog = join_catalog();
		seed_byte_upper_bound(&mut right_tiny_catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut right_tiny_catalog, "customers", 1_024);
		let right_tiny = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan right-tiny SQL join");
		let right_tiny = bind(
			&right_tiny,
			&right_tiny_catalog,
			&auto_broadcast_config(4_096),
		)
		.await
		.expect("bind right-tiny join");
		assert_join_strategy(&right_tiny, JoinStrategy::Broadcast);
		let ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} = right_tiny
		else {
			panic!("right-build broadcast must restore declared order with a project");
		};
		assert_eq!(
			field_names(&output_schema),
			vec!["id", "customer_id", "id", "region"]
		);
		assert_eq!(
			exprs
				.root_columns()
				.expect("right-build restore roots are columns")
				.into_iter()
				.map(|column| column.ordinal)
				.collect::<Vec<_>>(),
			vec![
				ColumnOrdinal(2),
				ColumnOrdinal(3),
				ColumnOrdinal(0),
				ColumnOrdinal(1)
			]
		);
		let ExecutionPlan::Join {
			left,
			right,
			left_keys,
			right_keys,
			output_schema,
			..
		} = input.as_ref()
		else {
			panic!("restore project must wrap the physical join");
		};
		assert_eq!(
			field_names(output_schema),
			vec!["id", "region", "id", "customer_id"]
		);
		assert!(matches!(terminal_exchange(left), Some((1, _))));
		assert!(terminal_exchange(right).is_none());
		assert_eq!(left_keys, &vec![ColumnOrdinal(0)]);
		assert_eq!(right_keys, &vec![ColumnOrdinal(1)]);
	}

	#[tokio::test]
	async fn right_build_swapped_plan_is_valid_left_build_broadcast() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan right-tiny SQL join");

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind right-tiny join");
		let ExecutionPlan::Project { input, .. } = plan else {
			panic!("right-build broadcast must be restored after a physical swap");
		};
		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = input.as_ref()
		else {
			panic!("restore project must wrap physical broadcast join");
		};

		assert_eq!(*strategy, JoinStrategy::Broadcast);
		assert!(matches!(terminal_exchange(left), Some((1, _))));
		assert!(terminal_exchange(right).is_none());
	}

	#[tokio::test]
	async fn both_sides_eligible_right_materially_smaller_prefers_right_build() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 3_072);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan SQL join");

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind dominance-selected right-build join");

		let ExecutionPlan::Project {
			input,
			output_schema,
			..
		} = plan
		else {
			panic!("right dominance should swap physical build and restore declared order");
		};
		assert_eq!(
			field_names(&output_schema),
			vec!["id", "customer_id", "id", "region"]
		);
		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = input.as_ref()
		else {
			panic!("restore project must wrap physical broadcast join");
		};
		assert_eq!(*strategy, JoinStrategy::Broadcast);
		assert!(matches!(terminal_exchange(left), Some((1, _))));
		assert!(terminal_exchange(right).is_none());

		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(3_072)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&auto_broadcast_config(4_096),
		);
		assert_eq!(decision.placement, JoinPhysicalPlacement::SwappedRightBuild);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Right,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: Some(ByteCount::new(3_072)),
			}
		);
	}

	#[tokio::test]
	async fn trace_records_broadcast_inference_with_rejected_magnitude() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 3_072);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan SQL join");

		let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind with trace");

		assert_eq!(trace.join_decisions.len(), 1);
		let receipt = trace.join_decisions[0];
		assert_eq!(receipt.target.join_index, 0);
		assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
		assert_eq!(receipt.placement, JoinPhysicalPlacement::SwappedRightBuild);
		assert_eq!(
			receipt.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Right,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: Some(ByteCount::new(3_072)),
			}
		);
	}

	#[tokio::test]
	async fn trace_records_left_tie_with_rejected_right_bytes() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 1_024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan SQL join");

		let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind with trace");

		assert_eq!(trace.join_decisions.len(), 1);
		let receipt = trace.join_decisions[0];
		assert_eq!(receipt.target.join_index, 0);
		assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
		assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(
			receipt.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Left,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: Some(ByteCount::new(1_024)),
			}
		);
	}

	#[tokio::test]
	async fn both_sides_tiny_equal_keeps_left_build() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 1_024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan SQL join");

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind tiny-tie join");

		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			output_schema,
			..
		} = plan
		else {
			panic!("left tie winner should keep natural join shape");
		};
		assert_eq!(strategy, JoinStrategy::Broadcast);
		assert_eq!(
			field_names(&output_schema),
			vec!["id", "customer_id", "id", "region"]
		);
		assert!(matches!(terminal_exchange(&left), Some((1, _))));
		assert!(terminal_exchange(&right).is_none());

		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&auto_broadcast_config(4_096),
		);
		assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Left,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: Some(ByteCount::new(1_024)),
			}
		);
	}

	#[test]
	fn both_sides_eligible_near_tie_keeps_left_build() {
		let config = auto_broadcast_config(4_096);
		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_536)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		);

		assert_eq!(decision.strategy, JoinStrategy::Broadcast);
		assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Left,
				build_bytes: ByteCount::new(1_536),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: Some(ByteCount::new(1_024)),
			}
		);
	}

	#[tokio::test]
	async fn right_tiny_left_deep_join_infers_broadcast_star() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan left-deep right-tiny SQL join");

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind left-deep probe right-tiny join");

		assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
	}

	#[tokio::test]
	async fn join_replay_id_deterministic_across_rebind() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan left-deep SQL join");

		let (plan_a, trace_a) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("first bind");
		let (plan_b, trace_b) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("second bind");

		let targets_a: Vec<_> = trace_a
			.join_decisions
			.iter()
			.map(|receipt| receipt.target.join_index)
			.collect();
		let targets_b: Vec<_> = trace_b
			.join_decisions
			.iter()
			.map(|receipt| receipt.target.join_index)
			.collect();
		assert_eq!(targets_a, vec![0, 1]);
		assert_eq!(targets_a, targets_b);

		let mut ids_a = Vec::new();
		let mut ids_b = Vec::new();
		collect_join_replay_ids(&plan_a, &mut ids_a);
		collect_join_replay_ids(&plan_b, &mut ids_b);
		assert_eq!(ids_a, vec![JoinReplayId(1), JoinReplayId(0)]);
		assert_eq!(ids_a, ids_b);
	}

	#[tokio::test]
	async fn force_hash_join_ids_demotes_only_targeted_join() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan left-deep SQL join");
		let config = PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(4_096)),
			force_hash_join_ids: BTreeSet::from([JoinReplayId(1)]),
			..PlannerConfig::default()
		};

		let (plan, trace) = bind_with_trace(&logical, &catalog, &config)
			.await
			.expect("bind targeted replay");

		assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 1);
		assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 1);
		assert_eq!(trace.join_decisions[0].strategy, JoinStrategy::Broadcast);
		assert_eq!(
			trace.join_decisions[1].strategy,
			JoinStrategy::HashPartitioned
		);
		assert!(matches!(
			trace.join_decisions[1].reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::HashAfterBroadcastCap
			}
		));
	}

	#[tokio::test]
	async fn star_query_infers_broadcast_without_seed_optimizer_facts() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
		let logical = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan descriptor-sidecar SQL join");

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind descriptor-sidecar broadcast star");

		assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
	}

	#[tokio::test]
	async fn rune_ctx_sql_star_query_infers_broadcast_without_seed_optimizer_facts() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
		let logical = rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o \
                     INNER JOIN customers c1 ON o.customer_id = c1.id \
                     INNER JOIN customers c2 ON o.customer_id = c2.id")
        }
    "#,
		)
		.await;

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind descriptor-sidecar Rune broadcast star");

		assert_join_strategy_count(&plan, JoinStrategy::Broadcast, 2);
	}

	#[tokio::test]
	async fn string_bearing_tiny_dim_infers_broadcast_by_estimate() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
		let logical = rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.sql("SELECT * FROM orders o \
                     INNER JOIN customers c ON o.customer_id = c.id")
        }
    "#,
		)
		.await;

		let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind estimate-backed Rune join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
		assert_eq!(trace.join_decisions.len(), 1);
		match trace.join_decisions[0].reason {
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side,
				estimated_byte_size,
				threshold,
				rejected_side_bytes,
			} => {
				assert_eq!(build_side, BroadcastBuildSide::Right);
				assert!(estimated_byte_size < threshold);
				assert_eq!(rejected_side_bytes, None);
			}
			other => panic!("expected estimated broadcast receipt, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn pk_filtered_string_dim_broadcasts_by_estimate() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, None);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind PK-filtered string dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));
		assert!(scoped.facts.estimated_byte_size.is_some());

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind PK-filtered estimated dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
		assert!(matches!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side: BroadcastBuildSide::Right,
				..
			}
		));
	}

	#[tokio::test]
	async fn direct_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)],
		);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind NDV-filtered dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
		assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
		assert!(scoped.facts.estimated_byte_size.is_some());

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind NDV-estimated dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
		assert!(matches!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side: BroadcastBuildSide::Right,
				..
			}
		));
	}

	#[tokio::test]
	async fn rune_equality_filtered_dim_with_ndv_infers_broadcast_by_estimate() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)],
		);
		let logical = rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("customers").alias("c")
               .filter(qcol("c", "region").eq(lit_str("north")))
               .auto_join(
                   ctx.table("orders").alias("o"),
                   qcol("c", "id").eq(qcol("o", "customer_id"))
               )
        }
    "#,
		)
		.await;

		let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind Rune NDV-estimated dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
		assert!(matches!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side: BroadcastBuildSide::Left,
				..
			}
		));
	}

	#[tokio::test]
	async fn rune_equality_filtered_dim_broadcasts_over_wendelon_ndv_no_seed() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)],
		);
		let logical = rune_plan(
			r#"
        pub fn main(ctx) {
            ctx.table("customers").alias("c")
               .filter(qcol("c", "region").eq(lit_str("north")))
               .auto_join(
                   ctx.table("orders").alias("o"),
                   qcol("c", "id").eq(qcol("o", "customer_id"))
               )
        }
    "#,
		)
		.await;

		let (plan, trace) = bind_with_trace(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind Rune NDV-estimated dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
		assert!(matches!(
			trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::BroadcastEstimated {
				build_side: BroadcastBuildSide::Left,
				..
			}
		));
	}

	#[tokio::test]
	async fn missing_ndv_keeps_137_138_behavior() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(20_000));
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind filtered dimension without NDV");
		assert_eq!(scoped.facts.estimated_row_count, None);

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind missing-NDV dimension join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn ndv_selectivity_writes_estimated_row_count_not_hard_bound() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)],
		);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind NDV-filtered dimension");

		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
		assert!(scoped.facts.estimated_byte_size.is_some());
	}

	#[tokio::test]
	async fn exact_ndv_still_uses_estimate_channel() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(ColumnOrdinal(1), DistinctCountFact::Exact(10_000))],
		);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind exact-NDV filtered dimension");

		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(20_000)));
		assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(2)));
		assert_eq!(scoped.facts.byte_upper_bound, None);
	}

	#[tokio::test]
	async fn range_filter_does_not_use_ndv_selectivity() {
		let catalog = descriptor_sidecar_join_catalog_with_customer_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(20_000),
			vec![(
				ColumnOrdinal(1),
				DistinctCountFact::Approx {
					estimate: 10_000,
					rel_err_ppm: 26_000,
				},
			)],
		);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind range-filtered dimension");

		assert_eq!(scoped.facts.estimated_row_count, None);
	}

	#[tokio::test]
	async fn and_filter_uses_single_equality_not_independence_product() {
		let catalog = descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
			Some(64 * 1024 * 1024),
			None,
			Some(10_000),
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("region", DataType::Utf8, true),
				Field::new("segment", DataType::Utf8, true),
			]),
			vec![
				(ColumnOrdinal(1), DistinctCountFact::Exact(100)),
				(ColumnOrdinal(2), DistinctCountFact::Exact(100)),
			],
		);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::And(
				Box::new(Expr::Eq(
					Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
				)),
				Box::new(Expr::Eq(
					Box::new(Expr::qualified_column(
						TableRef::new("customers"),
						"segment",
					)),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some(
						"enterprise".to_string(),
					)))),
				)),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind AND-filtered dimension");

		assert_eq!(scoped.facts.estimated_row_count, Some(RowCount::new(100)));
		assert_ne!(scoped.facts.estimated_row_count, Some(RowCount::new(1)));
	}

	#[test]
	fn estimated_size_over_counts_encoded_payload() {
		for rows in [1_usize, 8, 1_000] {
			for batch in representative_estimate_batches(rows) {
				let estimate = estimate_materialized_byte_size(
					RowCount::new(u64::try_from(rows).expect("test rows fit u64")),
					batch.schema().as_ref(),
				)
				.expect("representative schema is estimate-supported");
				let encoded =
					encode_record_batch(&batch).expect("representative batch encodes to IPC");
				assert!(
					estimate.get()
						>= u64::try_from(encoded.len()).expect("encoded length fits u64"),
					"estimate {estimate:?} under-counted encoded IPC bytes {} for schema {:?} rows {rows}",
					encoded.len(),
					batch.schema()
				);
			}
		}
	}

	#[tokio::test]
	async fn estimated_broadcast_receipt_is_labeled_estimated() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
		let logical = SqlPlanner::new()
			.plan("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan estimated EXPLAIN join");

		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &PlannerConfig::default()).await;

		assert_eq!(rows.len(), 1);
		assert_eq!(
			rows[0][3],
			ScalarValue::Utf8(Some("broadcast_estimated".to_string()))
		);
		assert_eq!(rows[0][5], ScalarValue::Utf8(Some("right".to_string())));
		assert!(matches!(rows[0][6], ScalarValue::Int64(Some(bytes)) if bytes > 0));
	}

	#[tokio::test]
	async fn known_empty_table_infers_broadcast() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(0));
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan known-empty join");

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind known-empty broadcast join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
	}

	#[tokio::test]
	async fn filtered_tiny_dim_infers_broadcast() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind filtered tiny dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
	}

	#[tokio::test]
	async fn narrowing_project_tiny_dim_infers_broadcast() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), None);
		let projected_customers = LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			exprs: vec![
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("customers"), "region"),
			],
			output_names: vec![Some("id".to_string()), Some("region".to_string())],
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&projected_customers)
		.await
		.expect("bind projected tiny dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::new(1_024)));

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(projected_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind narrowing projected tiny dimension join");

		assert_join_strategy(&plan, JoinStrategy::Broadcast);
	}

	#[tokio::test]
	async fn expression_project_drops_byte_bound_and_hashes() {
		let catalog =
			descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
		let expression_customers = LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			exprs: vec![
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::Add(
					Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
					Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
				),
			],
			output_names: vec![Some("id".to_string()), Some("next_id".to_string())],
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&expression_customers)
		.await
		.expect("bind expression projected tiny dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(12)));

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(expression_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind expression projected tiny dimension join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn duplicate_project_drops_byte_bound() {
		let catalog =
			descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
		let duplicate_customers = LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			exprs: vec![
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			],
			output_names: vec![Some("id".to_string()), Some("id_again".to_string())],
		};

		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&duplicate_customers)
		.await
		.expect("bind duplicate projected tiny dimension");

		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(12)));
	}

	#[tokio::test]
	async fn pk_filtered_large_table_caps_rows_but_still_hashes() {
		let catalog =
			descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(64 * 1024 * 1024), None);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered_customers)
		.await
		.expect("bind PK-filtered large table");
		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(
			scoped.facts.byte_upper_bound,
			Some(ByteCount::new(64 * 1024 * 1024))
		);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind PK-filtered large table join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn composite_pk_single_column_filter_does_not_cap_rows() {
		let mut catalog = MockCatalog::new();
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("part", DataType::Int64, false),
		]));
		let descriptor = named_descriptor(31, "composite_pk", schema.as_ref().clone());
		let mut binding = TableBinding::new_with_primary_key_columns(
			TableId(31),
			descriptor.table_object_id,
			descriptor.schema_generation,
			descriptor.physical_descriptor_fingerprint,
			schema,
			HashSet::from([ColumnOrdinal(0), ColumnOrdinal(1)]),
		);
		binding.optimizer_facts = TableOptimizerFacts {
			byte_upper_bound: Some(ByteCount::new(1_024)),
			row_upper_bound: None,
			column_ndv: Default::default(),
		};
		catalog
			.register_table(TableRef::new("composite_pk"), binding)
			.expect("register composite-PK binding");
		let filtered = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("composite_pk"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("composite_pk"), "id")),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		};

		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&filtered)
		.await
		.expect("bind composite-PK component filter");

		assert_eq!(scoped.facts.origin, PlanOrigin::SingleBaseRelation);
		assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::new(1_024)));
		assert_eq!(scoped.facts.row_upper_bound, None);
	}

	#[tokio::test]
	async fn aggregate_or_expanding_project_stays_composite() {
		let catalog =
			descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), Some(1_024), Some(12));
		let aggregate_customers = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			group_expr: vec![Expr::qualified_column(TableRef::new("customers"), "region")],
			aggr_expr: Vec::new(),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&aggregate_customers)
		.await
		.expect("bind aggregate tiny dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.row_upper_bound, None);

		let project_over_aggregate = LogicalPlan::Project {
			input: Box::new(aggregate_customers),
			exprs: vec![Expr::column("region")],
			output_names: vec![Some("region".to_string())],
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&project_over_aggregate)
		.await
		.expect("bind project over aggregate tiny dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
		assert_eq!(scoped.facts.byte_upper_bound, None);
	}

	#[tokio::test]
	async fn filtered_dim_without_facts_still_hashes() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, None);
		let filtered_customers = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(TableRef::new("customers"), "region")),
				Box::new(Expr::Literal(ScalarValue::Utf8(Some("north".to_string())))),
			),
		};
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(filtered_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind filtered dimension without facts join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn left_tiny_with_filtered_probe_infers_broadcast() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		let filtered_orders = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::qualified_column(
					TableRef::new("orders"),
					"customer_id",
				)),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
			),
		};
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(filtered_orders),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind left-tiny filtered-probe join");

		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = plan
		else {
			panic!("expected natural broadcast join");
		};
		assert_eq!(strategy, JoinStrategy::Broadcast);
		assert!(matches!(terminal_exchange(&left), Some((1, _))));
		assert!(terminal_exchange(&right).is_none());
	}

	#[tokio::test]
	async fn right_tiny_with_projected_probe_infers_broadcast() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let projected_orders = LogicalPlan::Project {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			exprs: vec![
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("orders"), "id"),
			],
			output_names: vec![Some("customer_id".to_string()), Some("id".to_string())],
		};
		let logical = LogicalPlan::Join {
			left: Box::new(projected_orders),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::column("customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind right-tiny projected-probe join");

		let ExecutionPlan::Project {
			input,
			output_schema,
			..
		} = plan
		else {
			panic!("right-build broadcast must restore declared order with a project");
		};
		assert_eq!(
			field_names(&output_schema),
			vec!["customer_id", "id", "id", "region"]
		);
		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = input.as_ref()
		else {
			panic!("restore project must wrap the physical join");
		};
		assert_eq!(*strategy, JoinStrategy::Broadcast);
		assert!(matches!(terminal_exchange(left), Some((1, _))));
		assert!(terminal_exchange(right).is_none());
	}

	#[tokio::test]
	async fn nested_hash_probe_still_stays_hash() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = nested_hash_probe_with_tiny_right();

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind nested-probe join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
		assert_join_strategy_count(&plan, JoinStrategy::HashPartitioned, 2);
	}

	#[tokio::test]
	async fn trace_records_hash_reason_for_non_scan_linear_probe() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = nested_hash_probe_with_tiny_right();

		let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind with trace");

		assert_eq!(trace.join_decisions.len(), 2);
		assert_eq!(
			trace
				.join_decisions
				.iter()
				.map(|receipt| receipt.target.join_index)
				.collect::<Vec<_>>(),
			vec![0, 1]
		);
		let receipt = trace.join_decisions[1];
		assert_eq!(receipt.strategy, JoinStrategy::HashPartitioned);
		assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(
			receipt.reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::ProbeNotScanLinear,
			}
		);
	}

	#[tokio::test]
	async fn probe_with_aggregate_stays_hash() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let aggregate_orders = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			group_expr: vec![Expr::qualified_column(
				TableRef::new("orders"),
				"customer_id",
			)],
			aggr_expr: Vec::new(),
		};
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(aggregate_orders),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::column("customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind aggregate-probe join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn probe_with_take_or_limited_scan_stays_hash() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let taken_orders = LogicalPlan::Take {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			limit: 1,
		};
		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(taken_orders.clone()),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind take-probe join");
		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);

		let limited_scan = bind(&taken_orders, &catalog, &PlannerConfig::default())
			.await
			.expect("bind limited scan");
		assert!(!broadcast_probe_is_scan_linear(&limited_scan));
	}

	#[tokio::test]
	async fn sql_broadcast_hint_binds_pinned_by_author() {
		let logical = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan hinted broadcast");
		assert!(matches!(
			logical,
			LogicalPlan::Join {
				strategy_intent: JoinStrategyIntent::Pinned {
					strategy: JoinStrategy::Broadcast,
					source: JoinStrategyPinSource::SqlHint
				},
				..
			}
		));

		let plan = bind(&logical, &join_catalog(), &PlannerConfig::default())
			.await
			.expect("pinned broadcast binds without auto config");
		assert_join_strategy(&plan, JoinStrategy::Broadcast);

		let err = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
			)
			.expect_err("non-inner broadcast hint fails before inference");
		assert!(
			err.to_string()
				.contains("broadcast join is only supported for INNER joins"),
			"{err}"
		);
	}

	#[tokio::test]
	async fn force_hash_join_ids_demotes_auto_and_sql_broadcast_only() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let force_hash = PlannerConfig {
			force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
			..PlannerConfig::default()
		};

		let auto_logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan auto join");
		let (auto_plan, auto_trace) = bind_with_trace(&auto_logical, &catalog, &force_hash)
			.await
			.expect("bind force-hash auto join");
		assert_join_strategy(&auto_plan, JoinStrategy::HashPartitioned);
		assert!(matches!(
			auto_trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::HashAfterBroadcastCap
			}
		));

		let sql_hint = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan SQL hinted broadcast");
		let (sql_plan, sql_trace) = bind_with_trace(&sql_hint, &catalog, &force_hash)
			.await
			.expect("bind force-hash SQL hint");
		let ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} = sql_plan
		else {
			panic!("force-hash SQL hint should bind as a hash join");
		};
		assert_eq!(strategy, JoinStrategy::HashPartitioned);
		assert!(matches!(
			terminal_exchange(&left),
			Some((DEFAULT_AUTO_JOIN_PARTITION_COUNT, _))
		));
		assert!(matches!(
			terminal_exchange(&right),
			Some((DEFAULT_AUTO_JOIN_PARTITION_COUNT, _))
		));
		assert!(matches!(
			sql_trace.join_decisions[0].reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::HashAfterBroadcastCap
			}
		));

		let direct = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 1)),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let direct_plan = bind(&direct, &catalog, &force_hash)
			.await
			.expect("bind direct pinned broadcast under force hash");
		assert_join_strategy(&direct_plan, JoinStrategy::Broadcast);
	}

	#[tokio::test]
	async fn mixed_sql_and_rune_broadcast_plan_replays_sql_preserves_rune() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let sql_broadcast = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan SQL hinted broadcast");
		let rune_build = LogicalPlan::Alias {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			alias: TableRef::new("c2"),
			columns: Vec::new(),
		};
		let mixed = LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				partition_count: 1,
				key: vec![Expr::qualified_column(TableRef::new("c2"), "id")],
				input: Box::new(rune_build),
			}),
			right: Box::new(sql_broadcast),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("c2"), "id"),
				Expr::qualified_column(TableRef::new("c"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Rune,
			},
		};
		let config = PlannerConfig {
			force_hash_join_ids: BTreeSet::from([JoinReplayId(0)]),
			..PlannerConfig::default()
		};

		let (plan, trace) = bind_with_trace(&mixed, &catalog, &config)
			.await
			.expect("bind mixed SQL/Rune targeted replay");
		let mut metadata = Vec::new();
		collect_join_replay_metadata(&plan, &mut metadata);

		assert_eq!(
			trace.join_decisions[0].strategy,
			JoinStrategy::HashPartitioned
		);
		assert_eq!(trace.join_decisions[1].strategy, JoinStrategy::Broadcast);
		assert!(metadata.contains(&(
			JoinReplayId(0),
			BroadcastReplayEligibility::Replayable,
			JoinStrategy::HashPartitioned,
		)));
		assert!(metadata.contains(&(
			JoinReplayId(1),
			BroadcastReplayEligibility::Terminal,
			JoinStrategy::Broadcast,
		)));
	}

	#[tokio::test]
	async fn pinned_author_choice_not_downgraded_or_upgraded_by_inference() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);

		let pinned_hash = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 2)),
			right: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::HashPartitioned,
				source: JoinStrategyPinSource::Direct,
			},
		};
		let plan = bind(&pinned_hash, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind pinned hash");
		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);

		let pinned_broadcast = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan pinned broadcast");
		let plan = bind(&pinned_broadcast, &catalog, &PlannerConfig::default())
			.await
			.expect("bind pinned broadcast");
		assert_join_strategy(&plan, JoinStrategy::Broadcast);
	}

	#[tokio::test]
	async fn trace_records_pinned_strategy_intent() {
		let logical = SqlPlanner::new()
			.plan(
				"SELECT /*+ BROADCAST(c) */ * FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan hinted broadcast");

		let (_plan, trace) = bind_with_trace(&logical, &join_catalog(), &PlannerConfig::default())
			.await
			.expect("bind with trace");

		assert_eq!(trace.join_decisions.len(), 1);
		let receipt = trace.join_decisions[0];
		assert_eq!(receipt.target.join_index, 0);
		assert_eq!(receipt.strategy, JoinStrategy::Broadcast);
		assert_eq!(receipt.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(receipt.reason, JoinStrategyDecisionReason::PinnedByAuthor);
	}

	#[tokio::test]
	async fn trace_indexes_each_join_deterministically() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan two-join SQL");

		let (_plan, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind with trace");

		assert_eq!(
			trace
				.join_decisions
				.iter()
				.map(|receipt| receipt.target.join_index)
				.collect::<Vec<_>>(),
			vec![0, 1]
		);
	}

	#[tokio::test]
	async fn bind_remains_structurally_identical_with_and_without_trace() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 3_072);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan SQL join");

		let plain = bind(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind without trace");
		let (traced, trace) = bind_with_trace(&logical, &catalog, &auto_broadcast_config(4_096))
			.await
			.expect("bind with trace");

		assert!(!trace.join_decisions.is_empty());
		assert_eq!(format!("{plain:?}"), format!("{traced:?}"));
	}

	#[tokio::test]
	async fn sql_explain_broadcast_inference_renders_receipt_rows() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 3_072);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan("EXPLAIN SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id")
			.expect("plan EXPLAIN join");

		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

		assert_eq!(
			rows,
			vec![vec![
				ScalarValue::Int64(Some(0)),
				ScalarValue::Utf8(Some("broadcast".to_string())),
				ScalarValue::Utf8(Some("swapped_right_build".to_string())),
				ScalarValue::Utf8(Some("broadcast_inferred".to_string())),
				ScalarValue::Utf8(None),
				ScalarValue::Utf8(Some("right".to_string())),
				ScalarValue::Int64(Some(1_024)),
				ScalarValue::Int64(Some(4_096)),
				ScalarValue::Int64(Some(3_072)),
			]]
		);
	}

	#[tokio::test]
	async fn explain_probe_not_scan_linear_reports_hash_reason_detail() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = LogicalPlan::Explain {
			input: Box::new(nested_hash_probe_with_tiny_right()),
		};

		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

		assert_eq!(rows.len(), 2);
		assert_eq!(rows[1][0], ScalarValue::Int64(Some(1)));
		assert_eq!(
			rows[1][1],
			ScalarValue::Utf8(Some("hash_partitioned".to_string()))
		);
		assert_eq!(
			rows[1][3],
			ScalarValue::Utf8(Some("hash_by_default".to_string()))
		);
		assert_eq!(
			rows[1][4],
			ScalarValue::Utf8(Some("probe_not_scan_linear".to_string()))
		);
	}

	#[tokio::test]
	async fn explain_broadcast_star_emits_one_receipt_per_link() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"EXPLAIN SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan EXPLAIN broadcast-star join");

		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

		assert_eq!(rows.len(), 2);
		assert_eq!(
			rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
			vec![ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(1))]
		);
		for row in rows {
			assert_eq!(row[1], ScalarValue::Utf8(Some("broadcast".to_string())));
			assert_eq!(
				row[2],
				ScalarValue::Utf8(Some("swapped_right_build".to_string()))
			);
			assert_eq!(
				row[3],
				ScalarValue::Utf8(Some("broadcast_inferred".to_string()))
			);
			assert_eq!(row[5], ScalarValue::Utf8(Some("right".to_string())));
		}
	}

	#[tokio::test]
	async fn explain_pinned_broadcast_reports_pinned_by_author() {
		let logical = SqlPlanner::new()
			.plan(
				"EXPLAIN SELECT /*+ BROADCAST(c) */ * \
				 FROM customers c INNER JOIN orders o ON c.id = o.customer_id",
			)
			.expect("plan EXPLAIN hinted broadcast");

		let (_schema, rows) =
			bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0][1], ScalarValue::Utf8(Some("broadcast".to_string())));
		assert_eq!(rows[0][2], ScalarValue::Utf8(Some("natural".to_string())));
		assert_eq!(
			rows[0][3],
			ScalarValue::Utf8(Some("pinned_by_author".to_string()))
		);
		assert_eq!(rows[0][4], ScalarValue::Utf8(None));
		assert_eq!(rows[0][5], ScalarValue::Utf8(Some("left".to_string())));
	}

	#[tokio::test]
	async fn explain_emits_one_row_per_join_in_index_order() {
		let mut catalog = join_catalog();
		seed_byte_upper_bound(&mut catalog, "orders", 64 * 1024 * 1024);
		seed_byte_upper_bound(&mut catalog, "customers", 1_024);
		let logical = SqlPlanner::new()
			.plan(
				"EXPLAIN SELECT * FROM orders o \
				 INNER JOIN customers c1 ON o.customer_id = c1.id \
				 INNER JOIN customers c2 ON o.customer_id = c2.id",
			)
			.expect("plan EXPLAIN two-join SQL");

		let (_schema, rows) =
			bind_explain_rows(&logical, &catalog, &auto_broadcast_config(4_096)).await;

		assert_eq!(
			rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
			vec![ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(1))]
		);
	}

	#[tokio::test]
	async fn explain_result_schema_is_stable() {
		let logical = SqlPlanner::new()
			.plan("EXPLAIN SELECT * FROM orders")
			.expect("plan EXPLAIN scan");

		let (schema, rows) =
			bind_explain_rows(&logical, &join_catalog(), &PlannerConfig::default()).await;

		assert!(rows.is_empty());
		assert_eq!(
			schema
				.fields()
				.iter()
				.map(|field| (
					field.name().as_str(),
					field.data_type(),
					field.is_nullable()
				))
				.collect::<Vec<_>>(),
			vec![
				("join_index", &DataType::Int64, false),
				("strategy", &DataType::Utf8, false),
				("placement", &DataType::Utf8, false),
				("reason", &DataType::Utf8, false),
				("reason_detail", &DataType::Utf8, true),
				("build_side", &DataType::Utf8, true),
				("build_bytes", &DataType::Int64, true),
				("threshold_bytes", &DataType::Int64, true),
				("rejected_side_bytes", &DataType::Int64, true),
			]
		);
	}

	#[test]
	fn ctx_sql_explain_is_read_only_relation() {
		let logical = SqlPlanner::new()
			.plan_relation("EXPLAIN SELECT * FROM orders")
			.expect("ctx.sql accepts read-only EXPLAIN");

		assert!(matches!(logical, LogicalPlan::Explain { .. }));
	}

	#[test]
	fn ctx_sql_explain_rejects_mutation_explain() {
		let error = SqlPlanner::new()
			.plan_relation("EXPLAIN INSERT INTO orders (id, customer_id) VALUES (1, 2)")
			.expect_err("ctx.sql rejects EXPLAIN over mutation");

		assert!(
			error
				.to_string()
				.contains("EXPLAIN supports SELECT queries only"),
			"{error}"
		);
	}

	#[test]
	fn sql_explain_accepts_only_default_query_form() {
		let planner = SqlPlanner::new();
		for sql in [
			"DESC SELECT * FROM orders",
			"DESCRIBE SELECT * FROM orders",
			"EXPLAIN ANALYZE SELECT * FROM orders",
			"EXPLAIN VERBOSE SELECT * FROM orders",
			"EXPLAIN QUERY PLAN SELECT * FROM orders",
			"EXPLAIN ESTIMATE SELECT * FROM orders",
			"EXPLAIN (ANALYZE TRUE) SELECT * FROM orders",
			"EXPLAIN FORMAT JSON SELECT * FROM orders",
			"EXPLAIN TABLE orders",
			"EXPLAIN EXPLAIN SELECT * FROM orders",
			"EXPLAIN INSERT INTO orders (id, customer_id) VALUES (1, 2)",
		] {
			assert!(planner.plan(sql).is_err(), "{sql} must be rejected");
		}
		assert!(planner.plan("EXPLAIN SELECT * FROM orders").is_ok());
	}

	#[test]
	fn inferred_broadcast_carries_reason_receipt() {
		let config = auto_broadcast_config(4_096);
		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: None,
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		);

		assert_eq!(decision.strategy, JoinStrategy::Broadcast);
		assert_eq!(decision.placement, JoinPhysicalPlacement::Natural);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Left,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: None,
			}
		);

		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: None,
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(1_024)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		);
		assert_eq!(decision.strategy, JoinStrategy::Broadcast);
		assert_eq!(decision.placement, JoinPhysicalPlacement::SwappedRightBuild);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Right,
				build_bytes: ByteCount::new(1_024),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: None,
			}
		);
	}

	#[test]
	fn sole_eligible_side_reports_no_rejected_bytes() {
		let config = auto_broadcast_config(4_096);
		for (left_bytes, right_bytes, expected_side, expected_placement) in [
			(
				Some(1_024),
				None,
				BroadcastBuildSide::Left,
				JoinPhysicalPlacement::Natural,
			),
			(
				None,
				Some(1_024),
				BroadcastBuildSide::Right,
				JoinPhysicalPlacement::SwappedRightBuild,
			),
		] {
			let decision = choose_join_strategy(
				JoinReplayId::SYNTHETIC,
				JoinStrategyIntent::Auto,
				JoinKind::Inner,
				join_input(PlanOptimizerFacts {
					origin: PlanOrigin::SingleBaseRelation,
					byte_upper_bound: left_bytes.map(ByteCount::new),
					estimated_byte_size: None,
					estimated_row_count: None,
					row_upper_bound: None,
				}),
				join_input(PlanOptimizerFacts {
					origin: PlanOrigin::SingleBaseRelation,
					byte_upper_bound: right_bytes.map(ByteCount::new),
					estimated_byte_size: None,
					estimated_row_count: None,
					row_upper_bound: None,
				}),
				&config,
			);

			assert_eq!(decision.strategy, JoinStrategy::Broadcast);
			assert_eq!(decision.placement, expected_placement);
			match decision.reason {
				JoinStrategyDecisionReason::BroadcastInferred {
					build_side,
					build_bytes,
					threshold,
					rejected_side_bytes,
				} => {
					assert_eq!(build_side, expected_side);
					assert_eq!(build_bytes, ByteCount::new(1_024));
					assert_eq!(threshold, ByteCount::new(4_096));
					assert_eq!(rejected_side_bytes, None);
				}
				other => panic!("expected inferred broadcast reason, got {other:?}"),
			}
		}
	}

	#[test]
	fn hard_byte_bound_still_wins_over_estimate() {
		let config = auto_broadcast_config(4_096);
		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(8_192)),
				estimated_byte_size: Some(ByteCount::new(1_024)),
				estimated_row_count: None,
				row_upper_bound: Some(RowCount::new(1)),
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(8_192)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		);

		assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::BuildByteUpperBoundAboveThreshold,
			}
		);

		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(2_048)),
				estimated_byte_size: Some(ByteCount::new(512)),
				estimated_row_count: None,
				row_upper_bound: Some(RowCount::new(1)),
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: Some(ByteCount::new(8_192)),
				estimated_byte_size: None,
				estimated_row_count: None,
				row_upper_bound: None,
			}),
			&config,
		);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::BroadcastInferred {
				build_side: BroadcastBuildSide::Left,
				build_bytes: ByteCount::new(2_048),
				threshold: ByteCount::new(4_096),
				rejected_side_bytes: None,
			}
		);
	}

	#[test]
	fn estimate_above_effective_threshold_hashes() {
		let config = auto_broadcast_config(4_096);
		let decision = choose_join_strategy(
			JoinReplayId::SYNTHETIC,
			JoinStrategyIntent::Auto,
			JoinKind::Inner,
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: None,
				estimated_byte_size: Some(ByteCount::new(8_192)),
				estimated_row_count: None,
				row_upper_bound: Some(RowCount::new(100)),
			}),
			join_input(PlanOptimizerFacts {
				origin: PlanOrigin::SingleBaseRelation,
				byte_upper_bound: None,
				estimated_byte_size: Some(ByteCount::new(8_192)),
				estimated_row_count: None,
				row_upper_bound: Some(RowCount::new(100)),
			}),
			&config,
		);

		assert_eq!(decision.strategy, JoinStrategy::HashPartitioned);
		assert_eq!(
			decision.reason,
			JoinStrategyDecisionReason::HashByDefault {
				reason: HashByDefaultReason::BuildByteUpperBoundAboveThreshold,
			}
		);
	}

	#[tokio::test]
	async fn take_limited_dim_still_hashes() {
		let catalog = descriptor_sidecar_join_catalog(Some(64 * 1024 * 1024), None, Some(12));
		let taken_customers = LogicalPlan::Take {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			limit: 1,
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &PlannerConfig::default(),
		}
		.bind_plan(&taken_customers)
		.await
		.expect("bind limited dimension");
		assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
		assert_eq!(scoped.facts.byte_upper_bound, None);
		assert_eq!(scoped.facts.estimated_byte_size, None);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(1)));

		let logical = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(taken_customers),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		};

		let plan = bind(&logical, &catalog, &PlannerConfig::default())
			.await
			.expect("bind take-limited dimension join");

		assert_join_strategy(&plan, JoinStrategy::HashPartitioned);
	}

	#[tokio::test]
	async fn take_or_empty_relation_seeds_row_bound_without_dispatch_change() {
		let catalog = join_catalog();
		let config = PlannerConfig::default();
		let take = LogicalPlan::Take {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			limit: 0,
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &config,
		}
		.bind_plan(&take)
		.await
		.expect("bind take");
		assert_eq!(scoped.facts.origin, PlanOrigin::Composite);
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(0)));

		let empty = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			predicate: Expr::Literal(ScalarValue::Boolean(Some(false))),
		};
		let scoped = Binder {
			catalog: &catalog,
			config: &config,
		}
		.bind_plan(&empty)
		.await
		.expect("bind empty");
		assert!(matches!(scoped.plan, ExecutionPlan::EmptyRelation { .. }));
		assert_eq!(scoped.facts.byte_upper_bound, Some(ByteCount::ZERO));
		assert_eq!(scoped.facts.row_upper_bound, Some(RowCount::new(0)));
	}

	fn catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor())
			.expect("register descriptor");
		catalog
	}

	fn auto_broadcast_config(max_build_bytes: u64) -> PlannerConfig {
		PlannerConfig {
			auto_broadcast_max_build_bytes: Some(ByteCount::new(max_build_bytes)),
			..PlannerConfig::default()
		}
	}

	fn seed_byte_upper_bound(catalog: &mut MockCatalog, table: &str, bytes: u64) {
		catalog
			.seed_optimizer_facts(
				&TableRef::new(table),
				TableOptimizerFacts {
					byte_upper_bound: Some(ByteCount::new(bytes)),
					row_upper_bound: None,
					column_ndv: Default::default(),
				},
			)
			.expect("seed optimizer facts");
	}

	fn descriptor_sidecar_join_catalog(
		orders_byte_upper_bound: Option<u64>,
		customers_byte_upper_bound: Option<u64>,
		customers_row_upper_bound: Option<u64>,
	) -> WendelonCatalog {
		descriptor_sidecar_join_catalog_with_customer_ndv(
			orders_byte_upper_bound,
			customers_byte_upper_bound,
			customers_row_upper_bound,
			Vec::new(),
		)
	}

	fn descriptor_sidecar_join_catalog_with_customer_ndv(
		orders_byte_upper_bound: Option<u64>,
		customers_byte_upper_bound: Option<u64>,
		customers_row_upper_bound: Option<u64>,
		column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	) -> WendelonCatalog {
		descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
			orders_byte_upper_bound,
			customers_byte_upper_bound,
			customers_row_upper_bound,
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("region", DataType::Utf8, true),
			]),
			column_ndv,
		)
	}

	fn descriptor_sidecar_join_catalog_with_customers_schema_and_ndv(
		orders_byte_upper_bound: Option<u64>,
		customers_byte_upper_bound: Option<u64>,
		customers_row_upper_bound: Option<u64>,
		customers_schema: Schema,
		column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	) -> WendelonCatalog {
		let orders = named_descriptor(
			1,
			"orders",
			Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("customer_id", DataType::Int64, false),
			]),
		);
		let customers = named_descriptor(2, "customers", customers_schema);
		WendelonCatalog::from_descriptors_with_optimizer_facts([
			(
				TableId(1),
				orders.clone(),
				orders_byte_upper_bound
					.map(|bytes| descriptor_sidecar_facts(&orders, Some(bytes), None, Vec::new())),
			),
			(
				TableId(2),
				customers.clone(),
				Some(descriptor_sidecar_facts(
					&customers,
					customers_byte_upper_bound,
					customers_row_upper_bound,
					column_ndv,
				)),
			),
		])
		.expect("descriptor-sidecar catalog")
	}

	fn descriptor_sidecar_facts(
		descriptor: &TableSchemaDescriptor,
		materialized_byte_upper_bound: Option<u64>,
		row_upper_bound: Option<u64>,
		column_ndv: Vec<(ColumnOrdinal, DistinctCountFact)>,
	) -> DescriptorOptimizerFacts {
		DescriptorOptimizerFacts {
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			manifest_epoch: 7,
			materialized_byte_upper_bound: materialized_byte_upper_bound.map(ByteCount::new),
			row_upper_bound: row_upper_bound.map(RowCount::new),
			column_ndv: column_ndv
				.into_iter()
				.map(|(column_ordinal, distinct_count)| DescriptorColumnNdvFact {
					column_ordinal,
					distinct_count,
				})
				.collect(),
		}
	}

	async fn rune_plan(script: &str) -> LogicalPlan {
		let config = RuneSandboxConfig::v1_default();
		let engine = RuneEngine::with_config(config).expect("Rune engine");
		engine
			.execute_script(script, &config)
			.await
			.expect("Rune-authored plan")
	}

	fn representative_estimate_batches(rows: usize) -> Vec<RecordBatch> {
		let fixed_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
		]));
		let string_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("label", DataType::Utf8, true),
		]));
		let binary_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("payload", DataType::Binary, true),
		]));
		let large_string_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("description", DataType::LargeUtf8, true),
		]));

		vec![
			RecordBatch::try_new(
				fixed_schema,
				vec![
					Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
					Arc::new(Int32Array::from_iter_values(
						(0..rows).map(|value| value as i32),
					)) as ArrayRef,
				],
			)
			.expect("fixed-width representative batch"),
			RecordBatch::try_new(
				string_schema,
				vec![
					Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
					Arc::new(StringArray::from(vec![Some("north"); rows])) as ArrayRef,
				],
			)
			.expect("Utf8 representative batch"),
			RecordBatch::try_new(
				binary_schema,
				vec![
					Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
					Arc::new(BinaryArray::from_iter_values(vec![b"abc".as_slice(); rows]))
						as ArrayRef,
				],
			)
			.expect("Binary representative batch"),
			RecordBatch::try_new(
				large_string_schema,
				vec![
					Arc::new(Int64Array::from_iter_values(0_i64..rows as i64)) as ArrayRef,
					Arc::new(LargeStringArray::from(vec![Some("north"); rows])) as ArrayRef,
				],
			)
			.expect("LargeUtf8 representative batch"),
		]
	}

	async fn bind_explain_rows(
		logical: &LogicalPlan,
		catalog: &dyn Catalog,
		config: &PlannerConfig,
	) -> (SchemaRef, Vec<Vec<ScalarValue>>) {
		let plan = bind(logical, catalog, config).await.expect("bind EXPLAIN");
		let ExecutionPlan::SystemTable { kind, schema, rows } = plan else {
			panic!("expected optimizer explain system table");
		};
		assert_eq!(kind, SystemTableKind::OptimizerExplain);
		(schema, rows)
	}

	fn join_input(facts: PlanOptimizerFacts) -> JoinInputOptimization {
		JoinInputOptimization {
			facts,
			probe_scan_linear: true,
		}
	}

	fn assert_join_strategy(plan: &ExecutionPlan, expected: JoinStrategy) {
		match first_join_plan(plan) {
			ExecutionPlan::Join { strategy, .. } => assert_eq!(*strategy, expected),
			other => panic!("expected join, got {other:?}"),
		}
	}

	fn assert_join_strategy_count(
		plan: &ExecutionPlan,
		expected: JoinStrategy,
		expected_count: usize,
	) {
		let mut observed = Vec::new();
		collect_join_strategies(plan, &mut observed);
		assert_eq!(
			observed
				.into_iter()
				.filter(|strategy| *strategy == expected)
				.count(),
			expected_count
		);
	}

	fn collect_join_strategies(plan: &ExecutionPlan, strategies: &mut Vec<JoinStrategy>) {
		match plan {
			ExecutionPlan::Join {
				left,
				right,
				strategy,
				..
			} => {
				strategies.push(*strategy);
				collect_join_strategies(left, strategies);
				collect_join_strategies(right, strategies);
			}
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Exchange { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Take { input, .. } => collect_join_strategies(input, strategies),
			ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::Scan { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. } => {}
		}
	}

	fn collect_join_replay_ids(plan: &ExecutionPlan, ids: &mut Vec<JoinReplayId>) {
		match plan {
			ExecutionPlan::Join {
				replay_id,
				left,
				right,
				..
			} => {
				ids.push(*replay_id);
				collect_join_replay_ids(left, ids);
				collect_join_replay_ids(right, ids);
			}
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Exchange { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Take { input, .. } => collect_join_replay_ids(input, ids),
			ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::Scan { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. } => {}
		}
	}

	fn collect_join_replay_metadata(
		plan: &ExecutionPlan,
		metadata: &mut Vec<(JoinReplayId, BroadcastReplayEligibility, JoinStrategy)>,
	) {
		match plan {
			ExecutionPlan::Join {
				replay_id,
				replay_eligibility,
				strategy,
				left,
				right,
				..
			} => {
				metadata.push((*replay_id, *replay_eligibility, *strategy));
				collect_join_replay_metadata(left, metadata);
				collect_join_replay_metadata(right, metadata);
			}
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Exchange { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Take { input, .. } => collect_join_replay_metadata(input, metadata),
			ExecutionPlan::EmptyRelation { .. }
			| ExecutionPlan::SystemTable { .. }
			| ExecutionPlan::Scan { .. }
			| ExecutionPlan::CreateTable { .. }
			| ExecutionPlan::CreateLogicalType { .. }
			| ExecutionPlan::CreateTableAsSelect { .. }
			| ExecutionPlan::DropTable { .. }
			| ExecutionPlan::Delete { .. }
			| ExecutionPlan::Update { .. }
			| ExecutionPlan::Insert { .. }
			| ExecutionPlan::InsertSelect { .. } => {}
		}
	}

	fn first_join_plan(plan: &ExecutionPlan) -> &ExecutionPlan {
		match plan {
			ExecutionPlan::Join { .. } => plan,
			ExecutionPlan::Filter { input, .. }
			| ExecutionPlan::Project { input, .. }
			| ExecutionPlan::Aggregate { input, .. }
			| ExecutionPlan::Sort { input, .. }
			| ExecutionPlan::Take { input, .. } => first_join_plan(input),
			other => other,
		}
	}

	fn field_names(schema: &SchemaRef) -> Vec<&str> {
		schema
			.fields()
			.iter()
			.map(|field| field.name().as_str())
			.collect()
	}

	fn join_catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(1),
				named_descriptor(
					1,
					"orders",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("customer_id", DataType::Int64, false),
					]),
				),
			)
			.expect("register orders");
		catalog
			.register_descriptor(
				TableId(2),
				named_descriptor(
					2,
					"customers",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("region", DataType::Utf8, true),
					]),
				),
			)
			.expect("register customers");
		catalog
	}

	fn float_join_catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(11),
				named_descriptor(
					11,
					"left_floats",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("f", DataType::Float64, false),
					]),
				),
			)
			.expect("register left floats");
		catalog
			.register_descriptor(
				TableId(12),
				named_descriptor(
					12,
					"right_floats",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("f", DataType::Float64, false),
					]),
				),
			)
			.expect("register right floats");
		catalog
	}

	fn exchanged_named_scan(table: &str, key: &str, partition_count: u32) -> LogicalPlan {
		LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new(table),
				projection: None,
			}),
			partition_count,
			key: vec![Expr::qualified_column(TableRef::new(table), key)],
		}
	}

	fn aliased_named_scan(table: &str, alias: &str) -> LogicalPlan {
		LogicalPlan::Alias {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new(table),
				projection: None,
			}),
			alias: TableRef::new(alias),
			columns: Vec::new(),
		}
	}

	fn exchanged_aliased_named_scan(
		table: &str,
		alias: &str,
		key: &str,
		partition_count: u32,
	) -> LogicalPlan {
		LogicalPlan::Exchange {
			input: Box::new(aliased_named_scan(table, alias)),
			partition_count,
			key: vec![Expr::qualified_column(TableRef::new(alias), key)],
		}
	}

	fn nested_hash_probe_with_tiny_right() -> LogicalPlan {
		LogicalPlan::Join {
			left: Box::new(LogicalPlan::Join {
				left: Box::new(exchanged_aliased_named_scan(
					"orders",
					"o",
					"customer_id",
					2,
				)),
				right: Box::new(exchanged_aliased_named_scan("customers", "c1", "id", 2)),
				kind: JoinKind::Inner,
				on: vec![(
					Expr::qualified_column(TableRef::new("o"), "customer_id"),
					Expr::qualified_column(TableRef::new("c1"), "id"),
				)],
				strategy_intent: JoinStrategyIntent::Pinned {
					strategy: JoinStrategy::HashPartitioned,
					source: JoinStrategyPinSource::Direct,
				},
			}),
			right: Box::new(aliased_named_scan("customers", "c2")),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("o"), "customer_id"),
				Expr::qualified_column(TableRef::new("c2"), "id"),
			)],
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}

	#[derive(Clone, Copy, Debug, Eq, PartialEq)]
	enum TestK3 {
		True,
		False,
		Null,
	}

	impl TestK3 {
		const VALUES: [Self; 3] = [Self::True, Self::False, Self::Null];
	}

	fn property_common_or_factor_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p_left = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, false));
		let left = builder.append_binary(BoundBinaryOp::And, p_left, r1, DataType::Boolean);
		let p_right = builder.append_column(test_bool_column("p", 0, true));
		let r2 = builder.append_column(test_bool_column("r2", 2, false));
		let right = builder.append_binary(BoundBinaryOp::And, p_right, r2, DataType::Boolean);
		let root = builder.append_binary(BoundBinaryOp::Or, left, right, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn property_common_or_three_branch_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p_left = builder.append_column(test_bool_column("p", 0, true));
		let r1_left = builder.append_column(test_bool_column("r1", 1, false));
		let left = builder.append_binary(BoundBinaryOp::And, p_left, r1_left, DataType::Boolean);
		let p_middle = builder.append_column(test_bool_column("p", 0, true));
		let r2 = builder.append_column(test_bool_column("r2", 2, false));
		let middle = builder.append_binary(BoundBinaryOp::And, p_middle, r2, DataType::Boolean);
		let p_right = builder.append_column(test_bool_column("p", 0, true));
		let r1_right = builder.append_column(test_bool_column("r1", 1, false));
		let right = builder.append_binary(BoundBinaryOp::And, p_right, r1_right, DataType::Boolean);
		let left_or_middle =
			builder.append_binary(BoundBinaryOp::Or, left, middle, DataType::Boolean);
		let root =
			builder.append_binary(BoundBinaryOp::Or, left_or_middle, right, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn property_or_branch_strips_to_true_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p_left = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, false));
		let left = builder.append_binary(BoundBinaryOp::And, p_left, r1, DataType::Boolean);
		let right = builder.append_column(test_bool_column("p", 0, true));
		let root = builder.append_binary(BoundBinaryOp::Or, left, right, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn property_not_or_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, false));
		let disjunction = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
		let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn property_not_or_partial_push_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, true));
		let r2 = builder.append_column(test_bool_column("r2", 2, false));
		let left = builder.append_binary(BoundBinaryOp::Or, p, r1, DataType::Boolean);
		let disjunction = builder.append_binary(BoundBinaryOp::Or, left, r2, DataType::Boolean);
		let root = builder.append_unary(BoundUnaryOp::Not, disjunction, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn property_top_level_and_program() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let p = builder.append_column(test_bool_column("p", 0, true));
		let r1 = builder.append_column(test_bool_column("r1", 1, false));
		let root = builder.append_binary(BoundBinaryOp::And, p, r1, DataType::Boolean);
		builder.push_root(root);
		builder.build()
	}

	fn test_bool_column(name: &str, ordinal: u32, pbc_eligible: bool) -> ResolvedColumn {
		ResolvedColumn {
			name: ColumnName(name.to_string()),
			ordinal: ColumnOrdinal(ordinal),
			data_type: DataType::Boolean,
			nullable: true,
			pbc_eligible,
		}
	}

	fn eval_test_program(program: &BoundExprProgram, row: &[TestK3; 3]) -> TestK3 {
		let root = program.single_root().expect("single root");
		eval_test_node(program, root, row)
	}

	fn eval_test_node(program: &BoundExprProgram, id: BoundExprId, row: &[TestK3; 3]) -> TestK3 {
		match program.node(id).expect("node") {
			BoundExprNode::Column(column) => row[column.ordinal.0 as usize],
			BoundExprNode::Literal(literal) => match program.literals[*literal as usize] {
				ScalarValue::Boolean(Some(true)) => TestK3::True,
				ScalarValue::Boolean(Some(false)) => TestK3::False,
				ScalarValue::Boolean(None) => TestK3::Null,
				_ => panic!("property evaluator only supports Boolean literals"),
			},
			BoundExprNode::Binary { op, left, right } => {
				let left = eval_test_node(program, *left, row);
				let right = eval_test_node(program, *right, row);
				match op {
					BoundBinaryOp::And => k3_and(left, right),
					BoundBinaryOp::Or => k3_or(left, right),
					_ => panic!("property evaluator only supports Boolean binary ops"),
				}
			}
			BoundExprNode::Unary { op, input } => match op {
				BoundUnaryOp::Not => k3_not(eval_test_node(program, *input, row)),
				_ => panic!("property evaluator only supports NOT"),
			},
			_ => panic!("property evaluator only supports Boolean test nodes"),
		}
	}

	fn k3_not(value: TestK3) -> TestK3 {
		match value {
			TestK3::True => TestK3::False,
			TestK3::False => TestK3::True,
			TestK3::Null => TestK3::Null,
		}
	}

	fn k3_and(left: TestK3, right: TestK3) -> TestK3 {
		match (left, right) {
			(TestK3::False, _) | (_, TestK3::False) => TestK3::False,
			(TestK3::True, TestK3::True) => TestK3::True,
			_ => TestK3::Null,
		}
	}

	fn k3_or(left: TestK3, right: TestK3) -> TestK3 {
		match (left, right) {
			(TestK3::True, _) | (_, TestK3::True) => TestK3::True,
			(TestK3::False, TestK3::False) => TestK3::False,
			_ => TestK3::Null,
		}
	}

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
			Field::new("ts", DataType::Int64, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"sales",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}

	fn named_descriptor(id_byte: u8, table: &str, schema: Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([id_byte; 16]),
			1,
			"default",
			table,
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
