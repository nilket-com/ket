use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::date32::parse_date_ymd;
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use chrono_tz::Tz;

use crate::aggregate_kernel::{
	self, AggregateArity, AggregateInputDomain, AggregateKernelSpec, StableAggregateKernel,
};
use crate::catalog::{
	Catalog, ColumnGroupTopKFact, ColumnHeavyHitterFact, ColumnRangeDistributionFact,
	DistinctCountFact, RangeFact, TableBinding, TableOptimizerFacts, TableRef, TableRowIdentity,
};
use crate::distributed::budget::{ByteCount, QueryBudget};
use crate::distributed::task::{JoinRuntimeObservation, RowCount};
use crate::error::BindingError;
use crate::exec::Executor;
use crate::ir::{
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprId, BoundExprNode,
	BoundExprProgram, BoundExprProgramBuilder, BoundReturningProjection, BoundUnaryOp,
	BroadcastReplayEligibility, ColumnName, ColumnOrdinal, ColumnRef, CreateTableColumnType,
	CreateTableSchema, ExecutionPlan, ExecutionSortKey, Expr, FilterDisposition, InsertLiteral,
	JoinKind, JoinReplayId, JoinStrategy, JoinStrategyIntent, JoinStrategyPinSource, LogicalPlan,
	LogicalTypeId, LogicalTypeRegistry, LogicalTypeResolver, LogicalTypeSpec, PbcProgram,
	QueryProfileHint, ResolvedColumn, ReturningProjection, ScalarLiteralKey,
	ScalarLiteralKeyClassification, ScalarValue, SortExpr, SystemTableKind, TableId,
	TemporalExtractField, TemporalTruncField, arithmetic_output_type,
	calendar_month_interval_data_type, decode_column_logical_type,
	decode_column_logical_type_with_resolver, field_with_logical_type_metadata, interval_data_type,
	logical_type_name, logical_type_spec, nanos_per_timestamp_unit, scalar_literal_key,
	schema_for_exprs,
};
use crate::pbc::PbcCompiler;
use wendelon::schema::{
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN,
};
use wendelon_client::{
	ARROW_STATS_TAG_BOOLEAN, ARROW_STATS_TAG_DATE32, ARROW_STATS_TAG_FLOAT32,
	ARROW_STATS_TAG_FLOAT64, ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	ARROW_STATS_TAG_TIMESTAMP_MICROSECOND, ARROW_STATS_TAG_TIMESTAMP_MILLISECOND,
	ARROW_STATS_TAG_TIMESTAMP_NANOSECOND, ARROW_STATS_TAG_UTF8, LogicalTypeRefinementComparisonOp,
	LogicalTypeRefinementPredicate, LogicalTypeRefinementValue, LogicalTypeUnit,
	TableSchemaDescriptor, UnitOffset, UnitQuantityKind, UnitScale,
	decode_float32_optimizer_fact_scalar, decode_float64_optimizer_fact_scalar,
	decode_signed_optimizer_fact_scalar,
};

mod aggregate;
mod arithmetic;
mod explain;
mod join;
mod join_order;
mod pushdown;
mod selectivity;

use self::aggregate::*;
use self::arithmetic::*;
use self::explain::*;
use self::join::*;
use self::join_order::*;
use self::pushdown::*;
use self::selectivity::*;

pub use self::join::join_schema;
pub(crate) use self::join::{BroadcastBuildSide, JoinStrategyDecisionReason};

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
	pub session_timezone: String,
}

impl Default for PlannerConfig {
	fn default() -> Self {
		Self {
			allow_experimental_kernels: false,
			auto_broadcast_max_build_bytes: Some(ByteCount::new(1024 * 1024)),
			force_hash_join_ids: BTreeSet::new(),
			session_timezone: "UTC".to_string(),
		}
	}
}

impl PlannerConfig {
	pub fn with_session_timezone(mut self, session_timezone: impl Into<String>) -> Self {
		self.session_timezone = session_timezone.into();
		self
	}

	fn resolved_session_timezone(&self) -> Result<Tz, BindingError> {
		crate::timestamp::parse_session_timezone(&self.session_timezone).map_err(|error| {
			BindingError::TypeMismatch {
				message: error.to_string(),
			}
		})
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
	let mut render_trace = None;
	let plan = Binder { catalog, config }
		.bind_plan_with_trace(
			plan,
			&mut trace,
			&mut render_trace,
			JoinOrderSourceCapture::Off,
		)
		.await?;
	let logical_types = catalog.logical_type_registry();
	let plan = push_down_scan_projection(plan.plan, &logical_types)?;
	Ok((plan, trace))
}

async fn bind_with_explain_trace(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<(ExecutionPlan, OptimizerTrace, OptimizerExplainRenderTrace), BindingError> {
	let mut trace = OptimizerTrace::default();
	let mut render_trace = Some(OptimizerExplainRenderTrace::default());
	let plan = Binder { catalog, config }
		.bind_plan_with_trace(
			plan,
			&mut trace,
			&mut render_trace,
			JoinOrderSourceCapture::On,
		)
		.await?;
	let logical_types = catalog.logical_type_registry();
	let plan = push_down_scan_projection(plan.plan, &logical_types)?;
	let mut render_trace = render_trace.expect("render trace remains present");
	if render_trace.join_order.is_none() {
		render_trace.join_order = analyze(&render_trace.join_order_edges);
	}
	Ok((plan, trace, render_trace))
}

pub(crate) struct ExplainAnalyzeBinding {
	pub(crate) plan: ExecutionPlan,
	pub(crate) replay_ids: BTreeSet<JoinReplayId>,
	trace: OptimizerTrace,
	render_trace: OptimizerExplainRenderTrace,
}

pub(crate) async fn bind_explain_analyze(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<ExplainAnalyzeBinding, BindingError> {
	let (plan, trace, render_trace) = bind_with_explain_trace(plan, catalog, config).await?;
	let replay_ids = star_order_runtime_replay_ids(&render_trace)?;
	Ok(ExplainAnalyzeBinding {
		plan,
		replay_ids,
		trace,
		render_trace,
	})
}

pub(crate) fn render_explain_analyze(
	binding: ExplainAnalyzeBinding,
	observations: &[JoinRuntimeObservation],
) -> Result<ExecutionPlan, BindingError> {
	optimizer_analyze_plan(binding.trace, binding.render_trace, observations)
}

fn star_order_runtime_replay_ids(
	render_trace: &OptimizerExplainRenderTrace,
) -> Result<BTreeSet<JoinReplayId>, BindingError> {
	if !matches!(
		render_trace.join_order,
		Some(StarOrderOutcome::Recognized(_))
	) {
		return Ok(BTreeSet::new());
	}
	render_trace
		.join_order_edges
		.iter()
		.map(|edge| {
			u32::try_from(edge.target.join_index)
				.map(JoinReplayId)
				.map_err(|_| BindingError::UnsupportedExpression {
					message: "EXPLAIN ANALYZE join replay id exceeds u32".to_string(),
				})
		})
		.collect()
}

struct Binder<'a> {
	catalog: &'a dyn Catalog,
	config: &'a PlannerConfig,
}

#[derive(Debug, Clone)]
struct ScopedExecutionPlan {
	plan: ExecutionPlan,
	scope: RelationScope,
	facts: PlanOptimizerFacts,
	join_order_sources: Option<JoinOrderSourceMap>,
}

impl ScopedExecutionPlan {
	fn new(plan: ExecutionPlan, scope: RelationScope) -> Self {
		Self {
			plan,
			scope,
			facts: PlanOptimizerFacts::default(),
			join_order_sources: None,
		}
	}

	fn new_with_facts(
		plan: ExecutionPlan,
		scope: RelationScope,
		facts: PlanOptimizerFacts,
	) -> Self {
		Self {
			plan,
			scope,
			facts,
			join_order_sources: None,
		}
	}

	fn new_with_facts_and_join_order_sources(
		plan: ExecutionPlan,
		scope: RelationScope,
		facts: PlanOptimizerFacts,
		join_order_sources: Option<JoinOrderSourceMap>,
	) -> Self {
		Self {
			plan,
			scope,
			facts,
			join_order_sources,
		}
	}

	fn empty_scope(plan: ExecutionPlan) -> Self {
		let scope = RelationScope::empty(plan.schema());
		Self {
			plan,
			scope,
			facts: PlanOptimizerFacts::default(),
			join_order_sources: None,
		}
	}

	fn schema(&self) -> SchemaRef {
		self.plan.schema()
	}
}

#[derive(Debug, Clone)]
struct BoundStarDimension<'a> {
	plan: ScopedExecutionPlan,
	on: &'a [(Expr, Expr)],
	strategy_intent: JoinStrategyIntent,
	label: String,
	evidence: JoinOrderDimEvidence,
}

fn star_output_segments(
	probe_label: &str,
	probe_plan: &ScopedExecutionPlan,
	dimensions: &[BoundStarDimension<'_>],
) -> Vec<StarOutputSegment> {
	let mut segments = Vec::with_capacity(dimensions.len() + 1);
	segments.push(StarOutputSegment {
		label: probe_label.to_string(),
		width: probe_plan.schema().fields().len(),
	});
	segments.extend(dimensions.iter().map(|dimension| StarOutputSegment {
		label: dimension.label.clone(),
		width: dimension.plan.schema().fields().len(),
	}));
	segments
}

fn star_output_segments_for_order(
	probe_label: &str,
	probe_plan: &ScopedExecutionPlan,
	dimensions: &[BoundStarDimension<'_>],
	order: &[usize],
) -> Vec<StarOutputSegment> {
	let mut segments = Vec::with_capacity(order.len() + 1);
	segments.push(StarOutputSegment {
		label: probe_label.to_string(),
		width: probe_plan.schema().fields().len(),
	});
	segments.extend(order.iter().map(|idx| {
		let dimension = &dimensions[*idx];
		StarOutputSegment {
			label: dimension.label.clone(),
			width: dimension.plan.schema().fields().len(),
		}
	}));
	segments
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

	fn composite_with_join_estimate(
		estimated_row_count: Option<RowCount>,
		output_schema: &Schema,
	) -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound: None,
			estimated_byte_size: estimated_row_count
				.and_then(|rows| estimate_materialized_byte_size(rows, output_schema)),
			estimated_row_count,
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

	fn preserve_through_project(self, exprs: &BoundExprProgram, output_schema: &Schema) -> Self {
		if project_roots_are_distinct_columns(exprs) {
			return self
				.preserve_volume_non_increasing()
				.derive_estimated_byte_size(output_schema);
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

struct ExprBindContext<'a> {
	config: &'a PlannerConfig,
	session_timezone: Tz,
	logical_type_specs: &'a HashMap<String, LogicalTypeSpec>,
	logical_type_registry: &'a LogicalTypeRegistry,
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
			let mut render_trace = None;
			self.bind_plan_with_trace(
				plan,
				&mut trace,
				&mut render_trace,
				JoinOrderSourceCapture::Off,
			)
			.await
		})
	}

	fn bind_plan_with_trace<'b>(
		&'b self,
		plan: &'b LogicalPlan,
		trace: &'b mut OptimizerTrace,
		render_trace: &'b mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
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

					let join_order_sources = join_order_source_capture.enabled().then(|| {
						join_order_sources_for_scan(
							table,
							&binding.schema,
							projection.as_deref(),
							&optimizer_facts,
						)
					});
					let plan = scan_plan(binding, projection);
					let facts = PlanOptimizerFacts::from_table(optimizer_facts)
						.derive_estimated_byte_size(plan.schema().as_ref());
					let scope = RelationScope::scan(table.clone(), &plan.schema());
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						plan,
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
					let facts = input.facts;
					let join_order_sources =
						alias_join_order_sources(input.join_order_sources.clone(), alias);
					let plan = bind_alias_column_rename(input.plan, columns)?;
					let scope = RelationScope::alias(alias.clone(), &plan.schema());
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						plan,
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Filter { input, predicate } => {
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
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
					let input_join_order_sources = input.join_order_sources;
					let has_pk_row_cap =
						filter_includes_primary_key_row_cap(&bound_predicate, &input.plan);
					let ndv_estimate_evidence = filter_equality_ndv_estimate_evidence(
						&bound_predicate,
						&input.plan,
						input_facts,
					);
					let ndv_estimated_row_count = filter_equality_ndv_estimated_row_count(
						&bound_predicate,
						&input.plan,
						input_facts,
					);
					let range_estimate_evidence = filter_range_selectivity_estimate_evidence(
						&bound_predicate,
						&input.plan,
						input_facts,
						&logical_types,
					);
					let range_estimated_row_count = filter_range_selectivity_estimated_row_count(
						&bound_predicate,
						&input.plan,
						input_facts,
						&logical_types,
					);
					let selected_estimate_evidence = min_filter_estimate_evidence(
						ndv_estimate_evidence,
						range_estimate_evidence,
					);
					let estimated_row_count =
						min_optional_row_count(ndv_estimated_row_count, range_estimated_row_count);
					let render_predicates = render_trace.is_some();
					let predicate_text = render_predicates.then(|| bound_predicate.pretty());
					let spent_column_name = if render_predicates {
						selected_estimate_evidence
							.and_then(|evidence| evidence.column)
							.and_then(|column| {
								spent_column_name_for_predicate(&bound_predicate, column)
							})
					} else {
						None
					};
					let (plan, _, pushdown_evidence, predicate_render) =
						plan_filter_with_predicate_pushdown_evidence(
							input.plan,
							bound_predicate,
							&logical_types,
							render_predicates,
						);
					let scan_index = trace.scan_filter_decisions.len();
					let target = ScanTraceTarget {
						scan_index,
						table_id: pushdown_evidence.table_id,
					};
					trace.scan_filter_decisions.push(ScanFilterReceipt {
						target,
						outcome: pushdown_evidence.outcome,
						converted_pushdown: pushdown_evidence.converted_pushdown,
						profile_hint: pushdown_evidence.profile_hint,
						estimated_rows: selected_estimate_evidence.map(|evidence| evidence.rows),
						spent_fact: selected_estimate_evidence.map(|evidence| evidence.source),
						spent_column: selected_estimate_evidence
							.and_then(|evidence| evidence.column),
					});
					if let Some(render_trace) = render_trace.as_mut() {
						let (pushed_predicate_text, residual_predicate_text) =
							match predicate_render {
								Some(render) => {
									(render.pushed_predicate_text, render.residual_predicate_text)
								}
								None => (None, None),
							};
						render_trace.scan_filters.push(ScanFilterRenderReceipt {
							target,
							predicate_text,
							pushed_predicate_text,
							residual_predicate_text,
							spent_column_name,
						});
					}
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
					let facts = if let Some(estimated_row_count) = estimated_row_count {
						facts.with_estimated_row_count(estimated_row_count)
					} else {
						facts
					}
					.derive_estimated_byte_size(plan.schema().as_ref());
					let join_order_sources = update_single_source_join_order_rows(
						input_join_order_sources,
						&input_scope,
						estimate_row_source(facts.row_upper_bound, facts.estimated_row_count),
					);
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						plan,
						scope,
						facts,
						join_order_sources,
					))
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
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
					let exprs = self
						.bind_projection_expr_program_scoped(exprs, &input.schema(), &input.scope)
						.await?;
					let output_schema = schema_for_exprs_with_output_names(&exprs, output_names);
					let facts = input.facts.preserve_through_project(&exprs, &output_schema);
					let join_order_sources = input.join_order_sources.clone();
					let scope = input.scope.project(&exprs, &output_schema);

					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Project {
							input: Box::new(input.plan),
							exprs,
							output_schema,
						},
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
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
					let logical_types = self.catalog.logical_type_registry();
					let output_schema =
						aggregate_schema(&group_expr, &bound_aggr_expr, &logical_types)?;
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
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
					let facts = input.facts.exchange();
					let input_schema = input.schema();
					let key = self
						.bind_expr_program_scoped(key, &input_schema, &input.scope)
						.await?;
					let scope = input.scope.with_schema(input_schema);
					let join_order_sources = input.join_order_sources.clone();

					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Exchange {
							input: Box::new(input.plan),
							partition_count: *partition_count,
							key,
						},
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => {
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
					let facts = PlanOptimizerFacts::composite_with_row(input.facts.row_upper_bound);
					let output_schema = input.schema();
					let order_by = bind_sort_keys(order_by, &output_schema, &input.scope)?;
					let scope = input.scope.with_schema(output_schema.clone());
					let join_order_sources = input.join_order_sources.clone();

					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Sort {
							input: Box::new(input.plan),
							order_by,
							limit: *limit,
							offset: *offset,
							output_schema,
						},
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Take { input, limit } => {
					let input = self
						.bind_plan_with_trace(input, trace, render_trace, join_order_source_capture)
						.await?;
					let facts = input.facts.take(*limit);
					let output_schema = input.schema();
					let scope = input.scope.with_schema(output_schema.clone());
					let join_order_sources = input.join_order_sources.clone();
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Take {
							input: Box::new(input.plan),
							limit: *limit,
							output_schema,
						},
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					strategy_intent,
				} => {
					if let Some(plan) = self
						.try_bind_star_order_actuation(plan, trace, render_trace)
						.await?
					{
						return Ok(plan);
					}
					self.bind_join(
						left,
						right,
						*kind,
						on,
						*strategy_intent,
						trace,
						render_trace,
						join_order_source_capture,
					)
					.await
				}
				LogicalPlan::Explain { input } => {
					let plan = self.bind_explain(input).await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
				LogicalPlan::ExplainAnalyze { .. } => Err(BindingError::UnsupportedExpression {
					message: "EXPLAIN ANALYZE must execute through the query coordinator"
						.to_string(),
				}),
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
					unit,
				} => Ok(ScopedExecutionPlan::empty_scope(
					ExecutionPlan::CreateLogicalType {
						name: name.clone(),
						physical: physical.clone(),
						refinement: refinement.clone(),
						unit: unit.clone(),
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
							render_trace,
							join_order_source_capture,
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
							render_trace,
							join_order_source_capture,
						)
						.await?;
					Ok(ScopedExecutionPlan::empty_scope(plan))
				}
			}
		})
	}

	async fn bind_explain(&self, input: &LogicalPlan) -> Result<ExecutionPlan, BindingError> {
		let (_input_plan, trace, render_trace) =
			bind_with_explain_trace(input, self.catalog, self.config).await?;
		optimizer_explain_plan(trace, Some(render_trace))
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
		let logical_type_registry = self.catalog.logical_type_registry();
		let ctx = ExprBindContext {
			config: self.config,
			session_timezone: self.config.resolved_session_timezone()?,
			logical_type_specs: &logical_casts,
			logical_type_registry: &logical_type_registry,
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
		let logical_type_registry = self.catalog.logical_type_registry();
		let ctx = ExprBindContext {
			config: self.config,
			session_timezone: self.config.resolved_session_timezone()?,
			logical_type_specs: &logical_casts,
			logical_type_registry: &logical_type_registry,
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
		let logical_type_registry = self.catalog.logical_type_registry();
		let ctx = ExprBindContext {
			config: self.config,
			session_timezone: self.config.resolved_session_timezone()?,
			logical_type_specs: &logical_casts,
			logical_type_registry: &logical_type_registry,
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
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let source = self
			.bind_plan_with_trace(source, trace, render_trace, join_order_source_capture)
			.await?;
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
				let logical_types = self.catalog.logical_type_registry();
				let pushdown = predicate_pushdown_plan(program, &scan, &logical_types);
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
		let session_timezone = self.config.resolved_session_timezone()?;

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
					.map(|(literal, field)| coerce_insert_literal(literal, field, session_timezone))
					.collect::<Result<Vec<_>, _>>()?
			} else {
				let mut values = vec![None; schema_fields.len()];
				for ((literal, target_index), _position) in
					row.iter().zip(target_indices.iter().copied()).zip(0..)
				{
					values[target_index] = Some(coerce_insert_literal(
						literal,
						schema.field(target_index),
						session_timezone,
					)?);
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
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let binding = self.catalog.resolve_table(table).await?;
		let schema = binding.schema.clone();
		let returning = self.bind_returning_projection(returning, &schema).await?;
		let target_columns = resolve_insert_target_columns(&schema, columns)?;
		validate_omitted_insert_target_columns(&schema, &target_columns)?;
		let source = self
			.bind_plan_with_trace(source, trace, render_trace, join_order_source_capture)
			.await?;
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

	async fn try_bind_star_order_actuation(
		&self,
		plan: &LogicalPlan,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<Option<ScopedExecutionPlan>, BindingError> {
		let Some(candidate) = extract_star_join_candidate(plan) else {
			return Ok(None);
		};

		let mut scratch_trace = trace.clone();
		let mut scratch_render_trace = render_trace.clone();
		let probe_plan = self
			.bind_plan_with_trace(
				candidate.probe,
				&mut scratch_trace,
				&mut scratch_render_trace,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let Some(probe_label) = probe_plan.scope.unique_source_label() else {
			self.record_star_order_decline(
				render_trace,
				StarOrderDeclineReason::MissingRelationLabel,
				candidate.dimensions.len(),
			);
			return Ok(None);
		};

		let mut dimensions = Vec::with_capacity(candidate.dimensions.len());
		let mut seen_dimensions = BTreeSet::new();
		for dimension in &candidate.dimensions {
			let dimension_plan = self
				.bind_plan_with_trace(
					dimension.input,
					&mut scratch_trace,
					&mut scratch_render_trace,
					JoinOrderSourceCapture::On,
				)
				.await?;
			let mut left_resolved_keys = Vec::new();
			let mut right_resolved_keys = Vec::new();
			for (left_expr, right_expr) in dimension.on {
				let Ok((left_key, right_key)) = bind_join_key_pair(
					left_expr,
					right_expr,
					&probe_plan.scope,
					&dimension_plan.scope,
				) else {
					self.record_star_order_decline(
						render_trace,
						StarOrderDeclineReason::MissingRelationLabel,
						candidate.dimensions.len(),
					);
					return Ok(None);
				};
				if validate_join_key_types(&left_key, &right_key).is_err() {
					self.record_star_order_decline(
						render_trace,
						StarOrderDeclineReason::NonInnerEquiJoin,
						candidate.dimensions.len(),
					);
					return Ok(None);
				}
				left_resolved_keys.push(left_key);
				right_resolved_keys.push(right_key);
			}
			let probe = relation_label_for_join_keys(&probe_plan.scope, &left_resolved_keys);
			let dimension_label =
				relation_label_for_join_keys(&dimension_plan.scope, &right_resolved_keys);
			if probe.as_deref() != Some(probe_label.as_str()) || dimension_label.is_none() {
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::MissingRelationLabel,
					candidate.dimensions.len(),
				);
				return Ok(None);
			}
			let dimension_label = dimension_label.expect("checked above");
			if !seen_dimensions.insert(dimension_label.clone()) {
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::DuplicateDimension,
					candidate.dimensions.len(),
				);
				return Ok(None);
			}

			let join_estimated_row_count = Self::join_estimated_row_count_for_keys(
				JoinKind::Inner,
				&probe_plan,
				&dimension_plan,
				&left_resolved_keys,
				&right_resolved_keys,
			);
			let (estimated_rows, estimate_source) = join_order_edge_estimate(
				probe_plan.join_order_sources.as_ref(),
				dimension_plan.join_order_sources.as_ref(),
				Some(probe_label.as_str()),
				Some(dimension_label.as_str()),
				&left_resolved_keys,
				&right_resolved_keys,
				join_estimated_row_count,
			);
			let Some(estimated_rows) = estimated_rows else {
				self.record_star_order_decline(
					render_trace,
					StarOrderDeclineReason::MissingEstimate,
					candidate.dimensions.len(),
				);
				return Ok(None);
			};
			let confidence = join_order_estimate_confidence(
				probe_plan.join_order_sources.as_ref(),
				dimension_plan.join_order_sources.as_ref(),
				Some(probe_label.as_str()),
				Some(dimension_label.as_str()),
				&left_resolved_keys,
				&right_resolved_keys,
			);
			dimensions.push(BoundStarDimension {
				plan: dimension_plan,
				on: dimension.on,
				strategy_intent: dimension.strategy_intent,
				label: dimension_label.clone(),
				evidence: JoinOrderDimEvidence {
					dimension: dimension_label,
					declared_position: dimension.declared_position,
					estimated_rows,
					estimate_source,
					confidence,
				},
			});
		}

		let evidence_dimensions = dimensions
			.iter()
			.map(|dimension| dimension.evidence.clone())
			.collect::<Vec<_>>();
		let confidence = join_order_overall_confidence(&evidence_dimensions);
		let recognized = recognized_star_order_outcome(
			probe_label.clone(),
			evidence_dimensions.clone(),
			None,
			confidence,
		);
		let StarOrderOutcome::Recognized(evidence) = &recognized else {
			unreachable!("recognized_star_order_outcome always recognizes");
		};
		let mut preferred_indices = (0..dimensions.len()).collect::<Vec<_>>();
		preferred_indices.sort_by(|left, right| {
			dimensions[*left]
				.evidence
				.estimated_rows
				.cmp(&dimensions[*right].evidence.estimated_rows)
				.then(
					dimensions[*left]
						.evidence
						.declared_position
						.cmp(&dimensions[*right].evidence.declared_position),
				)
		});
		let declared_indices = (0..dimensions.len()).collect::<Vec<_>>();
		let selected_indices = if evidence.action == StarOrderAction::Reordered {
			preferred_indices
		} else {
			declared_indices
		};

		let mut preflight_trace = trace.clone();
		let preflight_join_start = preflight_trace.join_decisions.len();
		let mut no_render_trace = None;
		let _ = self.bind_star_order_sequence(
			probe_plan.clone(),
			&dimensions,
			&selected_indices,
			&mut preflight_trace,
			&mut no_render_trace,
		)?;
		let preflight_decisions = &preflight_trace.join_decisions[preflight_join_start..];
		if preflight_decisions.len() != selected_indices.len()
			|| preflight_decisions.iter().any(|decision| {
				decision.strategy != JoinStrategy::Broadcast
					|| decision.placement != JoinPhysicalPlacement::SwappedRightBuild
			}) {
			self.record_star_order_decline(
				render_trace,
				StarOrderDeclineReason::NotBroadcastStar,
				candidate.dimensions.len(),
			);
			return Ok(None);
		}

		let real_probe_plan = self
			.bind_plan_with_trace(
				candidate.probe,
				trace,
				render_trace,
				JoinOrderSourceCapture::On,
			)
			.await?;
		let mut real_dimensions = Vec::with_capacity(dimensions.len());
		for (dimension, bound_dimension) in candidate.dimensions.iter().zip(&dimensions) {
			let plan = self
				.bind_plan_with_trace(
					dimension.input,
					trace,
					render_trace,
					JoinOrderSourceCapture::On,
				)
				.await?;
			real_dimensions.push(BoundStarDimension {
				plan,
				on: dimension.on,
				strategy_intent: dimension.strategy_intent,
				label: bound_dimension.label.clone(),
				evidence: bound_dimension.evidence.clone(),
			});
		}

		let declared_segments =
			star_output_segments(&probe_label, &real_probe_plan, &real_dimensions);
		let mut declared_output_schema = real_probe_plan.schema();
		let mut declared_scope = real_probe_plan.scope.clone();
		for dimension in &real_dimensions {
			declared_output_schema = join_schema(
				JoinKind::Inner,
				&declared_output_schema,
				&dimension.plan.schema(),
			);
			declared_scope = RelationScope::join(
				JoinKind::Inner,
				&declared_scope,
				&dimension.plan.scope,
				&declared_output_schema,
			);
		}
		let applied_order = selected_indices
			.iter()
			.map(|idx| real_dimensions[*idx].label.clone())
			.collect::<Vec<_>>();
		let physical_segments = star_output_segments_for_order(
			&probe_label,
			&real_probe_plan,
			&real_dimensions,
			&selected_indices,
		);
		let mut output = self.bind_star_order_sequence(
			real_probe_plan,
			&real_dimensions,
			&selected_indices,
			trace,
			render_trace,
		)?;
		if evidence.action == StarOrderAction::Reordered {
			let physical_output_schema = output.schema();
			output.plan = restore_declared_star_order_project(
				output.plan,
				&physical_output_schema,
				declared_output_schema,
				&physical_segments,
				&declared_segments,
			)?;
			output.scope = declared_scope;
		}

		if let Some(render_trace) = render_trace.as_mut() {
			render_trace.join_order = Some(recognized_star_order_outcome(
				probe_label,
				evidence_dimensions,
				Some(applied_order),
				confidence,
			));
		}
		Ok(Some(output))
	}

	fn bind_star_order_sequence<'b>(
		&self,
		probe_plan: ScopedExecutionPlan,
		dimensions: &[BoundStarDimension<'b>],
		order: &[usize],
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let mut current = probe_plan;
		for idx in order {
			let dimension = &dimensions[*idx];
			current = self.bind_scoped_join(
				current,
				dimension.plan.clone(),
				JoinKind::Inner,
				dimension.on,
				dimension.strategy_intent,
				trace,
				render_trace,
				dimension.evidence.declared_position,
			)?;
		}
		Ok(current)
	}

	fn record_star_order_decline(
		&self,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		reason: StarOrderDeclineReason,
		edge_count: usize,
	) {
		if let Some(render_trace) = render_trace.as_mut() {
			render_trace.join_order = Some(StarOrderOutcome::Declined(StarOrderDecline {
				reason,
				edge_count,
			}));
		}
	}

	fn join_estimated_row_count_for_keys(
		kind: JoinKind,
		left_plan: &ScopedExecutionPlan,
		right_plan: &ScopedExecutionPlan,
		left_resolved_keys: &[ResolvedJoinKey],
		right_resolved_keys: &[ResolvedJoinKey],
	) -> Option<RowCount> {
		match (left_resolved_keys, right_resolved_keys) {
			([left_key], [right_key]) => join_output_ndv_containment_estimate(
				kind,
				left_plan,
				right_plan,
				left_key.column.ordinal,
				right_key.column.ordinal,
				&left_key.column.data_type,
				&right_key.column.data_type,
			),
			(left_keys, right_keys)
				if left_keys.len() == right_keys.len() && left_keys.len() >= 2 =>
			{
				multi_key_join_cardinality(kind, left_plan, right_plan, left_keys, right_keys)
			}
			_ => None,
		}
	}

	async fn bind_join(
		&self,
		left: &LogicalPlan,
		right: &LogicalPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		strategy_intent: JoinStrategyIntent,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let left_plan = self
			.bind_plan_with_trace(left, trace, render_trace, join_order_source_capture)
			.await?;
		let right_plan = self
			.bind_plan_with_trace(right, trace, render_trace, join_order_source_capture)
			.await?;
		let declared_position = trace.join_decisions.len();
		self.bind_scoped_join(
			left_plan,
			right_plan,
			kind,
			on,
			strategy_intent,
			trace,
			render_trace,
			declared_position,
		)
	}

	fn bind_scoped_join(
		&self,
		mut left_plan: ScopedExecutionPlan,
		mut right_plan: ScopedExecutionPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		strategy_intent: JoinStrategyIntent,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		declared_position: usize,
	) -> Result<ScopedExecutionPlan, BindingError> {
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
		let join_estimated_row_count = Self::join_estimated_row_count_for_keys(
			kind,
			&left_plan,
			&right_plan,
			&left_resolved_keys,
			&right_resolved_keys,
		);

		let join_index = trace.join_decisions.len();
		let replay_id = JoinReplayId(join_index as u32);
		let left_probe_scan_linear = broadcast_probe_is_scan_linear(&left_plan.plan);
		let right_probe_scan_linear = broadcast_probe_is_scan_linear(&right_plan.plan);
		if let Some(render_trace) = render_trace.as_mut() {
			let probe = relation_label_for_join_keys(&left_plan.scope, &left_resolved_keys);
			let dimension = relation_label_for_join_keys(&right_plan.scope, &right_resolved_keys);
			let (estimated_rows, estimate_source) = join_order_edge_estimate(
				left_plan.join_order_sources.as_ref(),
				right_plan.join_order_sources.as_ref(),
				probe.as_deref(),
				dimension.as_deref(),
				&left_resolved_keys,
				&right_resolved_keys,
				join_estimated_row_count,
			);
			render_trace.join_order_edges.push(JoinOrderEdgeEvidence {
				target: JoinTraceTarget { join_index },
				probe,
				dimension,
				declared_position,
				estimated_rows,
				estimate_source,
				inner_equi: kind == JoinKind::Inner && !left_resolved_keys.is_empty(),
				probe_scan_linear: left_probe_scan_linear,
			});
		}
		let decision = choose_join_strategy(
			replay_id,
			strategy_intent,
			kind,
			JoinInputOptimization {
				facts: left_plan.facts,
				probe_scan_linear: left_probe_scan_linear,
			},
			JoinInputOptimization {
				facts: right_plan.facts,
				probe_scan_linear: right_probe_scan_linear,
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
		let output_facts = PlanOptimizerFacts::composite_with_join_estimate(
			join_estimated_row_count,
			declared_output_schema.as_ref(),
		);
		let join_order_sources = merge_join_order_sources(
			left_plan.join_order_sources.clone(),
			right_plan.join_order_sources.clone(),
		);
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

		Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
			plan,
			declared_scope,
			output_facts,
			join_order_sources,
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
		| Expr::SqlConvert { expr: inner, .. }
		| Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::Like { expr: inner, .. }
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => reject_storage_synthetic_returning_reference(inner),
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
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => {
			names.insert(target_logical_name.clone());
			collect_logical_cast_names(expr, names)
		}
		Expr::LogicalAscription { expr, .. }
		| Expr::Cast(expr, _)
		| Expr::TryCast(expr, _)
		| Expr::Not(expr)
		| Expr::IsNull(expr)
		| Expr::IsNotNull(expr)
		| Expr::Like { expr, .. }
		| Expr::TimeBucket(_, expr)
		| Expr::AtTimeZone(expr, _)
		| Expr::Extract(_, expr)
		| Expr::DateTrunc(_, expr) => collect_logical_cast_names(expr, names),
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
		DataType::Date32 => Ok(ScalarValue::Date32(None)),
		DataType::Timestamp(unit, Some(timezone))
			if timezone.as_ref() == "UTC"
				&& matches!(
					unit,
					TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
				) =>
		{
			Ok(ScalarValue::Timestamp {
				value: None,
				unit: *unit,
			})
		}
		DataType::Float32 => Ok(ScalarValue::Float32(None)),
		DataType::Float64 => Ok(ScalarValue::Float64(None)),
		DataType::Duration(TimeUnit::Nanosecond) => Ok(ScalarValue::Interval(None)),
		DataType::Interval(IntervalUnit::YearMonth) => Ok(ScalarValue::CalendarMonthInterval(None)),
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
			| DataType::Date32
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
	) || crate::timestamp::is_supported_utc_timestamp(data_type)
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

fn scan_plan(binding: TableBinding, projection: Option<Vec<ColumnOrdinal>>) -> ExecutionPlan {
	let column_ndv = binding
		.optimizer_facts
		.column_ndv
		.iter()
		.map(|(ordinal, distinct_count)| (*ordinal, *distinct_count))
		.collect::<Vec<_>>();
	let column_range = binding
		.optimizer_facts
		.column_range
		.iter()
		.map(|(ordinal, range)| (*ordinal, range.clone()))
		.collect::<Vec<_>>();
	let column_range_distributions = binding
		.optimizer_facts
		.column_range_distributions
		.iter()
		.map(|(ordinal, fact)| (*ordinal, fact.clone()))
		.collect::<Vec<_>>();
	let column_heavy_hitters = binding
		.optimizer_facts
		.column_heavy_hitters
		.iter()
		.map(|(ordinal, fact)| (*ordinal, fact.clone()))
		.collect::<Vec<_>>();
	let column_group_top_k = binding
		.optimizer_facts
		.column_group_top_k
		.iter()
		.map(|(ordinals, fact)| (ordinals.clone(), fact.clone()))
		.collect::<Vec<_>>();
	let column_group_ndv = binding
		.optimizer_facts
		.column_group_ndv
		.iter()
		.map(|(ordinals, fact)| (ordinals.clone(), *fact))
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
		column_range,
		column_range_distributions,
		column_heavy_hitters,
		column_group_top_k,
		column_group_ndv,
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

fn push_down_scan_projection(
	plan: ExecutionPlan,
	logical_types: &LogicalTypeRegistry,
) -> Result<ExecutionPlan, BindingError> {
	let (plan, _) = optimize_for_required(plan, None, logical_types)?;
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
	logical_types: &LogicalTypeRegistry,
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
			column_range,
			column_range_distributions,
			column_heavy_hitters,
			column_group_top_k,
			column_group_ndv,
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
			let column_range = column_range
				.into_iter()
				.filter_map(|(column, range)| {
					remap.get(&column).copied().map(|mapped| (mapped, range))
				})
				.collect();
			let column_range_distributions = column_range_distributions
				.into_iter()
				.filter_map(|(column, fact)| {
					remap.get(&column).copied().map(|mapped| (mapped, fact))
				})
				.collect();
			let column_heavy_hitters = column_heavy_hitters
				.into_iter()
				.filter_map(|(column, fact)| {
					remap.get(&column).copied().map(|mapped| (mapped, fact))
				})
				.collect();
			let column_group_top_k = column_group_top_k
				.into_iter()
				.filter_map(|(columns, fact)| {
					let mapped = columns
						.iter()
						.map(|column| remap.get(column).copied())
						.collect::<Option<Vec<_>>>()?;
					Some((mapped, fact))
				})
				.collect();
			let column_group_ndv = column_group_ndv
				.into_iter()
				.filter_map(|(columns, fact)| {
					let mapped = columns
						.iter()
						.map(|column| remap.get(column).copied())
						.collect::<Option<Vec<_>>>()?;
					Some((mapped, fact))
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
					column_range,
					column_range_distributions,
					column_heavy_hitters,
					column_group_top_k,
					column_group_ndv,
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
			let (input, remap) =
				optimize_for_required(*input, Some(child_required), logical_types)?;
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
			let (input, remap) =
				optimize_for_required(*input, Some(child_required), logical_types)?;
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

			let (input, remap) =
				optimize_for_required(*input, Some(child_required), logical_types)?;
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
			let output_schema = aggregate_schema(&group_expr, &aggr_expr, logical_types)?;
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
			let (input, remap) =
				optimize_for_required(*input, Some(child_required), logical_types)?;
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
			let (input, _) = optimize_for_required(*input, None, logical_types)?;
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
			let (mut input, _) = optimize_for_required(*input, None, logical_types)?;
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
			let (left, _) = optimize_for_required(*left, None, logical_types)?;
			let (right, _) = optimize_for_required(*right, None, logical_types)?;
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
			unit,
		} => Ok((
			ExecutionPlan::CreateLogicalType {
				name,
				physical,
				refinement,
				unit,
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
			let (source, _) = optimize_for_required(*source, None, logical_types)?;
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
			let (source, _) = optimize_for_required(*source, None, logical_types)?;
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
			let (source, _) = optimize_for_required(*source, None, logical_types)?;
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
		| Some(BoundExprNode::TimeBucket { input, .. })
		| Some(BoundExprNode::AtTimeZone { input, .. })
		| Some(BoundExprNode::Extract { input, .. })
		| Some(BoundExprNode::DateTrunc { input, .. }) => {
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
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => bind_sql_convert(builder, input_schema, ctx, expr, target_logical_name),
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
		Expr::AtTimeZone(inner, zone) => bind_at_time_zone(builder, input_schema, ctx, inner, zone),
		Expr::Extract(field, inner) => {
			bind_temporal_extract(builder, input_schema, ctx, *field, inner)
		}
		Expr::DateTrunc(field, inner) => bind_date_trunc(builder, input_schema, ctx, *field, inner),
	}
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
	if source != *target && !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to {target:?}; only identity casts or Int32, Int64, Float32, and Float64 numeric conversions are supported"
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
	if source != *target && !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only identity casts or Int32, Int64, Float32, and Float64 numeric conversions are supported"
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
	session_timezone: Tz,
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
		InsertLiteral::Date32(value) => match target {
			DataType::Date32 => Ok(ScalarValue::Date32(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Timestamp { value, unit } => match target {
			DataType::Timestamp(target_unit, Some(timezone))
				if timezone.as_ref() == "UTC" && *target_unit == *unit =>
			{
				Ok(ScalarValue::Timestamp {
					value: Some(*value),
					unit: *unit,
				})
			}
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Float64(value) => match target {
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Utf8(value) => match target {
			DataType::Utf8 => Ok(ScalarValue::Utf8(Some(value.clone()))),
			DataType::Timestamp(unit, Some(timezone))
				if timezone.as_ref() == "UTC"
					&& matches!(
						unit,
						TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
					) =>
			{
				Ok(ScalarValue::Timestamp {
					value: Some(parse_timestamp_text_for_bind(
						value,
						session_timezone,
						*unit,
					)?),
					unit: *unit,
				})
			}
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
			reject_calendar_month_interval_comparison(&right_type)?;
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
			reject_calendar_month_interval_comparison(&left_type)?;
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
	coerce_comparison_literals(builder, left, right, ctx.session_timezone)?;
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed comparison left");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed comparison right");
	reject_calendar_month_interval_comparison(&left_type)?;
	reject_calendar_month_interval_comparison(&right_type)?;
	if let Some(output) = bind_dimensioned_comparison(builder, ctx, left, right, &op)? {
		return Ok(output);
	}
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

fn reject_calendar_month_interval_comparison(data_type: &DataType) -> Result<(), BindingError> {
	if is_calendar_month_interval_type(data_type) {
		return Err(BindingError::TypeMismatch {
			message: "CalendarMonthInterval comparison is deferred".to_string(),
		});
	}
	Ok(())
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
		coerce_comparison_literals(builder, expr, item, ctx.session_timezone)?;
		let current_expr_type = builder
			.node_type(expr)
			.cloned()
			.expect("builder returns typed IN target");
		reject_calendar_month_interval_comparison(&current_expr_type)?;
		let item_type = builder
			.node_type(item)
			.cloned()
			.expect("builder returns typed IN member");
		reject_calendar_month_interval_comparison(&item_type)?;
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
	reject_calendar_month_interval_comparison(&target_type)?;
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
	session_timezone: Tz,
) -> Result<(), BindingError> {
	if builder.literal(left).is_some() && builder.literal(right).is_some() {
		return Ok(());
	}
	if let Some(value) = builder.literal(left).cloned() {
		let Some(data_type) = builder.node_type(right).cloned() else {
			return Ok(());
		};
		let value = coerce_literal_value(value, &data_type, session_timezone)?;
		let _ = builder.rewrite_literal(left, value);
		return Ok(());
	}
	if let Some(value) = builder.literal(right).cloned() {
		let Some(data_type) = builder.node_type(left).cloned() else {
			return Ok(());
		};
		let value = coerce_literal_value(value, &data_type, session_timezone)?;
		let _ = builder.rewrite_literal(right, value);
	}
	Ok(())
}

fn coerce_literal_value(
	value: ScalarValue,
	target_type: &DataType,
	session_timezone: Tz,
) -> Result<ScalarValue, BindingError> {
	Ok(match (value, target_type) {
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
		(ScalarValue::Utf8(Some(value)), DataType::Date32) => {
			ScalarValue::Date32(Some(parse_date_ymd(&value).map_err(|error| {
				BindingError::TypeMismatch {
					message: error.to_string(),
				}
			})?))
		}
		(ScalarValue::Utf8(None), DataType::Date32) => ScalarValue::Date32(None),
		(ScalarValue::Utf8(Some(value)), DataType::Timestamp(unit, Some(timezone)))
			if timezone.as_ref() == "UTC"
				&& matches!(
					unit,
					TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
				) =>
		{
			ScalarValue::Timestamp {
				value: Some(parse_timestamp_text_for_bind(
					&value,
					session_timezone,
					*unit,
				)?),
				unit: *unit,
			}
		}
		(ScalarValue::Utf8(None), DataType::Timestamp(unit, Some(timezone)))
			if timezone.as_ref() == "UTC"
				&& matches!(
					unit,
					TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
				) =>
		{
			ScalarValue::Timestamp {
				value: None,
				unit: *unit,
			}
		}
		(value, _) => value,
	})
}

fn parse_timestamp_text_for_bind(
	value: &str,
	session_timezone: Tz,
	unit: TimeUnit,
) -> Result<i64, BindingError> {
	crate::timestamp::parse_timestamp_text(value, session_timezone, unit).map_err(|error| {
		BindingError::TypeMismatch {
			message: error.to_string(),
		}
	})
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
mod tests;
