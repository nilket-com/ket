use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::date32::parse_date_ymd;
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use chrono_tz::Tz;

use crate::aggregate_kernel::{
	self, AggregateInputDomain, AggregateKernelSpec, AggregateLogicalTypeRule,
	StableAggregateKernel,
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
	BoundExprProgram, BoundExprProgramBuilder, BoundKernelDensitySpec, BoundReturningProjection,
	BoundUnaryOp, BoundWindowCall, BroadcastBuildSide, BroadcastReplayEligibility, ColumnName,
	ColumnOrdinal, ColumnRef, CreateTableColumnType, CreateTableSchema, ExecutionPlan,
	ExecutionSortKey, Expr, FilterDisposition, InsertLiteral, JoinKind, JoinReplayId, JoinStrategy,
	JoinStrategyIntent, JoinStrategyPinSource, KernelDensitySpec, LogicalPlan, LogicalTypeId,
	LogicalTypeRegistry, LogicalTypeResolver, LogicalTypeSpec, PbcProgram, QueryProfileHint,
	ResolvedColumn, ReturningProjection, ScalarLiteralKey, ScalarLiteralKeyClassification,
	ScalarValue, SetOpAllKind, SortExpr, SystemTableKind, TableId, TemporalExtractField,
	TemporalTruncField, WindowFrame, WindowFrameBound, WindowFrameDelta, WindowFrameUnit,
	WindowFunction, arithmetic_output_type, calendar_month_interval_data_type,
	decimal_division_output_type, decode_column_logical_type,
	decode_column_logical_type_with_resolver, field_with_logical_type_metadata, interval_data_type,
	logical_type_name, nanos_per_timestamp_unit, scalar_literal_key, schema_for_exprs,
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
pub(crate) mod aggregate_exchange;
mod arithmetic;
mod cast;
mod decision_log;
mod explain;
mod expression_walk;
mod generated;
mod join;
mod join_order;
mod outer_count_preaggregation;
mod outer_join_demotion;
mod pushdown;
mod runtime_evidence;
mod scalar_function;
mod scalar_reduction;
mod selectivity;
mod set_ops;

use self::aggregate::*;
use self::aggregate_exchange::*;
use self::arithmetic::*;
use self::decision_log::*;
use self::explain::*;
use self::expression_walk::*;
pub(crate) use self::generated::bind_generated_relation_for_authoring;
use self::generated::require_certificate_describes_bound_source;
use self::join::*;
use self::join_order::*;
use self::outer_count_preaggregation::*;
use self::outer_join_demotion::*;
use self::pushdown::*;
use self::scalar_function::*;
use self::selectivity::*;
use self::set_ops::{
	all_output_columns_program, bind_union, distinct_input_unsupported_message,
	normalize_set_op_arm, repeat_count_column, repeat_output_schema, set_op_output_schema,
};

pub use self::decision_log::OptimizerDecisionLog;
pub use self::explain::bind_with_explain_summary;
pub use self::explain::bind_with_local_join_admissions;
pub(crate) use self::join::JoinStrategyDecisionReason;
pub use self::join::LocalHashJoinAdmission;
pub use self::join::join_schema;
pub use self::join_order::SoftMarginLadderStep;
pub use self::runtime_evidence::{DurableEvidenceBinding, OptimizerRuntimeEvidence};
pub(crate) use self::runtime_evidence::{
	OptimizerEvidenceStatusSnapshot, OptimizerReceiptGateSnapshot, OptimizerRuntimeDecisionRecorder,
};

pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
const DEFAULT_AUTO_JOIN_PARTITION_COUNT: u32 = 4;
const DEFAULT_DISTINCT_PARTITION_COUNT: u32 = DEFAULT_AUTO_JOIN_PARTITION_COUNT;
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
const INFORMATION_SCHEMA_OPTIMIZER_DECISIONS: &str = "information_schema.optimizer_decisions";
const INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS: &str =
	"information_schema.optimizer_decision_conflicts";
const INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION: &str =
	"information_schema.optimizer_memo_admission";
const INFORMATION_SCHEMA_OPTIMIZER_PROBE_LINEARITY_FENCES: &str =
	"information_schema.optimizer_probe_linearity_fences";
const INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS: &str =
	"information_schema.optimizer_runtime_verdicts";
const INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY: &str =
	"information_schema.optimizer_margin_advisory";
const INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES: &str =
	"information_schema.optimizer_receipt_gates";
const INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS: &str =
	"information_schema.optimizer_evidence_status";
const INFORMATION_SCHEMA_LOGICAL_TYPES_TABLE: &str = "logical_types";
const INFORMATION_SCHEMA_TABLES_TABLE: &str = "tables";
const INFORMATION_SCHEMA_COLUMNS_TABLE: &str = "columns";
const INFORMATION_SCHEMA_SCHEMATA_TABLE: &str = "schemata";
const INFORMATION_SCHEMA_OPTIMIZER_DECISIONS_TABLE: &str = "optimizer_decisions";
const INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS_TABLE: &str = "optimizer_decision_conflicts";
const INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION_TABLE: &str = "optimizer_memo_admission";
const INFORMATION_SCHEMA_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE: &str =
	"optimizer_probe_linearity_fences";
const INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS_TABLE: &str = "optimizer_runtime_verdicts";
const INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY_TABLE: &str = "optimizer_margin_advisory";
const INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES_TABLE: &str = "optimizer_receipt_gates";
const INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS_TABLE: &str = "optimizer_evidence_status";
const ARROW_BUFFER_ALIGNMENT_BYTES: u64 = 64;
const DEFAULT_UTF8_BINARY_PAYLOAD_BYTES: u64 = 256;
const DEFAULT_LARGE_UTF8_BINARY_PAYLOAD_BYTES: u64 = 1024;
const RECORD_BATCH_IPC_FRAMING_OVERHEAD_BYTES: u64 = 16 * 1024;

// ADR 191: how the configured join-order soft-margin step is applied to a bind. `Manual` keeps the
// ADR 190 operator-configured step as the effective step (default, byte-identical to 190).
// `ApplyAdvisory` opts in to the ADR 189 advisory: the effective step becomes the advisory's
// `recommended_step` (at most one ladder rung from the configured base). Typed, not a bool, so the
// surface stays extensible without another flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrderSoftMarginApplication {
	Manual,
	ApplyAdvisory,
}

pub(crate) fn join_order_soft_margin_application_label(
	application: JoinOrderSoftMarginApplication,
) -> &'static str {
	match application {
		JoinOrderSoftMarginApplication::Manual => "manual",
		JoinOrderSoftMarginApplication::ApplyAdvisory => "apply_advisory",
	}
}

#[derive(Debug, Clone)]
pub struct PlannerConfig {
	/// ADR 537: runtime-filter mode, resolved once here so every fragmentation path agrees.
	pub runtime_filters_enabled: bool,
	pub allow_experimental_kernels: bool,
	pub auto_broadcast_max_build_bytes: Option<ByteCount>,
	pub force_hash_join_ids: BTreeSet<JoinReplayId>,
	pub session_timezone: String,
	pub decision_log: Arc<OptimizerDecisionLog>,
	pub runtime_evidence: Arc<OptimizerRuntimeEvidence>,
	pub measured_accrual_sample_rate: u32,
	pub measured_accrual_counter: Arc<AtomicU64>,
	pub join_order_soft_margin_step: SoftMarginLadderStep,
	pub join_order_soft_margin_application: JoinOrderSoftMarginApplication,
	/// ADR 466-472: smaller-build placement for auto inner hash joins. Default-on
	/// since ADR 472: filter-aware comparison (471) plus the unknown-size guard
	/// (472) left three engaged cells, all material wins (+17.7%/+34.6%/+23.1%),
	/// and no regression. Retained as an operator off-switch.
	pub hash_smaller_build_placement: bool,
	/// ADR 511: allow the smaller-build swap toward a right side narrowed by an
	/// unestimated filter — its byte bound only overstates it, since filters are
	/// volume-non-increasing, so the raw-bound comparison stays conservative.
	/// Left-side staleness still declines. Default-on since the ADR 511 receipt:
	/// q17 pairs improved 21.5-23.5 s against a 3,554 ms floor, q20 was the only
	/// other engaged cell, and both hold digests with the swap on.
	pub filtered_build_side_swap: bool,
	/// ADR 512: reduce a decorrelated scalar aggregate's input to keys
	/// reachable from the recombine's outer side. A semi build that is a
	/// superset of the outer key domain eliminates only never-matched groups,
	/// so every aggregate in the grouped shape is preserved; the recognizer
	/// declines otherwise. Default-on since the ADR 512 receipt: q17 pairs
	/// improved 7,180-7,367 ms against the 3,554 ms floor with zero spill in
	/// both arms, q17 was the only engaged cell, and every digest held.
	pub scalar_aggregate_input_reduction: bool,
	/// ADR 515: commute an author-pinned membership semi through an Inner-join
	/// spine to the single source branch of its complete key tuple. A
	/// plain-equality semi is a row-level predicate on that branch, so the
	/// commute is filter-before-fanout; the recognizer declines any shape it
	/// cannot prove. Default-on since the ADR 515 receipt: q18 pairs improved
	/// 12,098-12,949 ms against the 3,554 ms floor, the 989,777,032 B spill
	/// vanished, q18 was the only engaged cell, and every digest held.
	pub membership_first_composition: bool,
	/// ADR 517: complete the ADR 401 side-local rule through inner joins — a
	/// deterministic, pushdown-safe, single-owner conjunct is a row-level
	/// predicate on its branch and commutes with an inner join; everything
	/// else stays residual. Default-on since the ADR 517 receipt: q08/q09
	/// pairs improved 3,809-4,055 ms against the 3,554 ms floor, the part
	/// suffix build collapsed 1.6M → 10,801/87,125 rows, every digest held.
	pub side_local_filter_routing: bool,
	/// ADR 518: let reachable-key traces cross a membership semi from its
	/// preserved side and derive a weaker semi from any nonempty, same-source
	/// subset of correlation keys. Default-on since the ADR 518 receipt: q20
	/// pairs improved 6,668-6,764 ms against the 3,554 ms floor (the scalar
	/// aggregate fell 5.48 s → 14 ms), sole engaged cell, every digest held.
	pub semi_filtered_reachable_keys: bool,
	/// ADR 521: preaggregate a counted right column beneath a plain-equality
	/// left outer join when proof-grade unique-key evidence establishes one
	/// preserved row per group. Default-on since the ADR 521 receipt: q13 pairs
	/// improved 5,948-6,021 ms; spill fell 5,131,808 B → 0; field contracts held.
	pub outer_count_preaggregation: bool,
	/// ADR 489: the caller asserts single-partition (non-distributed) execution, so
	/// the binder may admit a local no-exchange inner join. Default **false**
	/// (conservative): only an execution entry that knows the plan will not be
	/// fragmented sets it true. The distributed coordinator leaves it false, and the
	/// fragmenter's `JoinInputNotExchanged` is the loud-fail backstop. No reader yet —
	/// this increment only establishes the provability signal.
	pub single_partition_execution: bool,
	/// ADR 490: the raw task-memory coordinate supplied by a caller that also proves
	/// single-partition execution. `None` is conservative: local hash admission has no
	/// budget evidence and must stay off. This is policy input only; no binder reader
	/// consumes it in the rail-first increment.
	pub local_hash_join_memory_per_task: Option<ByteCount>,
}

impl Default for PlannerConfig {
	fn default() -> Self {
		Self {
			runtime_filters_enabled: true,
			allow_experimental_kernels: false,
			auto_broadcast_max_build_bytes: Some(ByteCount::new(1024 * 1024)),
			force_hash_join_ids: BTreeSet::new(),
			session_timezone: "UTC".to_string(),
			decision_log: Arc::new(OptimizerDecisionLog::default()),
			runtime_evidence: Arc::new(OptimizerRuntimeEvidence::local_only()),
			measured_accrual_sample_rate: 0,
			measured_accrual_counter: Arc::new(AtomicU64::new(0)),
			single_partition_execution: false,
			local_hash_join_memory_per_task: None,
			join_order_soft_margin_step: CURRENT_JOIN_ORDER_SOFT_MARGIN_STEP,
			join_order_soft_margin_application: JoinOrderSoftMarginApplication::Manual,
			hash_smaller_build_placement: true,
			filtered_build_side_swap: true,
			scalar_aggregate_input_reduction: true,
			membership_first_composition: true,
			side_local_filter_routing: true,
			semi_filtered_reachable_keys: true,
			outer_count_preaggregation: true,
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
	bind_with_statement_timestamp(
		plan,
		catalog,
		config,
		crate::exec::StatementContext::capture_system().timestamp_nanos,
	)
	.await
}

pub(crate) async fn bind_with_statement_timestamp(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
	statement_timestamp_nanos: i64,
) -> Result<ExecutionPlan, BindingError> {
	let (plan, _) = bind_with_trace_at(plan, catalog, config, statement_timestamp_nanos).await?;
	Ok(plan)
}

pub(crate) async fn bind_with_trace(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<(ExecutionPlan, OptimizerTrace), BindingError> {
	bind_with_trace_at(
		plan,
		catalog,
		config,
		crate::exec::StatementContext::capture_system().timestamp_nanos,
	)
	.await
}

async fn bind_with_trace_at(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
	statement_timestamp_nanos: i64,
) -> Result<(ExecutionPlan, OptimizerTrace), BindingError> {
	let mut trace = OptimizerTrace::default();
	let mut render_trace = None;
	let binder = Binder::new_at(catalog, config, statement_timestamp_nanos);
	let (normalized_plan, outer_join_demotions) = match plan {
		LogicalPlan::Explain { .. } => (plan.clone(), Vec::new()),
		_ => binder.demote_null_rejected_outer_joins(plan).await?,
	};
	// ADR 270: lift comma-join (and explicit-CROSS) cross-bridging equalities out of a keyless Cross into
	// keyed Inner joins — a binder pass after outer-join demotion, before binding.
	let normalized_plan = match plan {
		LogicalPlan::Explain { .. } => normalized_plan,
		_ => binder.recognize_cross_equi_joins(&normalized_plan).await?,
	};
	let decision_provenance = decision_provenance_for_outer_join_demotions(&outer_join_demotions);
	let binder = binder.with_decision_provenance(decision_provenance);
	let plan = binder
		.bind_plan_with_trace(
			&normalized_plan,
			&mut trace,
			&mut render_trace,
			JoinOrderSourceCapture::Off,
			None,
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
	bind_with_explain_trace_at(
		plan,
		catalog,
		config,
		crate::exec::StatementContext::capture_system().timestamp_nanos,
	)
	.await
}

async fn bind_with_explain_trace_at(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
	statement_timestamp_nanos: i64,
) -> Result<(ExecutionPlan, OptimizerTrace, OptimizerExplainRenderTrace), BindingError> {
	let mut trace = OptimizerTrace::default();
	let mut render_trace = Some(OptimizerExplainRenderTrace::default());
	let binder = Binder::new_at(catalog, config, statement_timestamp_nanos);
	// ADR 208: capture the bind-entry tree step before the binder is consumed so observed
	// star/snowflake receipts render on the same margin that actuation used earlier in this bind.
	let tree_join_order_soft_margin_step = binder.join_order_policy.tree_step;
	let (normalized_plan, outer_join_demotions) =
		binder.demote_null_rejected_outer_joins(plan).await?;
	let normalized_plan = binder.recognize_cross_equi_joins(&normalized_plan).await?;
	let decision_provenance = decision_provenance_for_outer_join_demotions(&outer_join_demotions);
	if let Some(render_trace) = render_trace.as_mut() {
		render_trace
			.outer_join_demotions
			.extend(outer_join_demotions);
	}
	let binder = binder.with_decision_provenance(decision_provenance);
	let plan = binder
		.bind_plan_with_trace(
			&normalized_plan,
			&mut trace,
			&mut render_trace,
			JoinOrderSourceCapture::On,
			None,
		)
		.await?;
	let logical_types = catalog.logical_type_registry();
	let plan = push_down_scan_projection(plan.plan, &logical_types)?;
	let mut render_trace = render_trace.expect("render trace remains present");
	if !render_trace.has_recognized_star_order() {
		if let Some(snowflake_order) = analyze_snowflake(
			&render_trace.join_order_edges,
			&trace.join_decisions,
			tree_join_order_soft_margin_step,
		) {
			render_trace.set_observed_snowflake(snowflake_order);
		} else if matches!(render_trace.join_order, JoinOrderRenderState::None) {
			if let Some(outcome) = analyze(
				&render_trace.join_order_edges,
				tree_join_order_soft_margin_step,
			) {
				let edge_targets = render_trace
					.join_order_edges
					.iter()
					.map(|edge| edge.target)
					.collect::<Vec<_>>();
				let probe_label = star_order_probe_label(&outcome);
				render_trace.set_star_receipt(StarOrderRenderReceipt {
					probe_label,
					outcome,
					edge_targets,
				});
			}
		}
	}
	// ADR 194: galaxy evidence is additive — it rides a sibling field and coexists with whatever
	// star/snowflake receipt the tree recognizer produced (the conformed two-fact query is observed as
	// a snowflake AND carries a galaxy row), so it runs regardless of the recognized-star state.
	if let Some(galaxy) = analyze_galaxy(&render_trace.join_order_edges, &trace.join_decisions) {
		render_trace.set_observed_galaxy(galaxy);
	}
	// ADR 196: bridge evidence is an additive sibling, recognized over the same role-neutral edges.
	if let Some(bridge) = analyze_bridge(&render_trace.join_order_edges, &trace.join_decisions) {
		render_trace.set_observed_bridge(bridge);
	}
	// ADR 203: a 2-hop bridge chain is recognized at bind time (so the 188/201 sampled verdict can judge
	// it without EXPLAIN), additive alongside the per-junction bridge rows. Observe-only.
	if let Some(chain) = analyze_bridge_chain(&render_trace.join_order_edges, &trace.join_decisions)
	{
		render_trace.set_observed_bridge_chain(chain);
	}
	if let Some(receipt) = preserved_outer_order_receipt(
		&render_trace.join_order_edges,
		tree_join_order_soft_margin_step,
	) {
		render_trace.set_preserved_outer_order(vec![receipt]);
	}
	Ok((plan, trace, render_trace))
}

pub(crate) struct ExplainAnalyzeBinding {
	pub(crate) plan: ExecutionPlan,
	pub(crate) replay_ids: BTreeSet<JoinReplayId>,
	trace: OptimizerTrace,
	render_trace: OptimizerExplainRenderTrace,
}

#[allow(dead_code)]
pub(crate) async fn bind_explain_analyze(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<ExplainAnalyzeBinding, BindingError> {
	bind_explain_analyze_with_statement_timestamp(
		plan,
		catalog,
		config,
		crate::exec::StatementContext::capture_system().timestamp_nanos,
	)
	.await
}

pub(crate) async fn bind_explain_analyze_with_statement_timestamp(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
	statement_timestamp_nanos: i64,
) -> Result<ExplainAnalyzeBinding, BindingError> {
	let (plan, trace, render_trace) =
		bind_with_explain_trace_at(plan, catalog, config, statement_timestamp_nanos).await?;
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

pub(crate) fn measured_explain_analyze_verdicts(
	binding: &ExplainAnalyzeBinding,
	observations: &[JoinRuntimeObservation],
) -> Result<Vec<MeasuredLevelVerdict>, BindingError> {
	measured_verdicts(&binding.render_trace, observations)
}

// Join-order EXPLAIN ANALYZE evidence uses the same replay-id capture for flat stars and
// recognized snowflakes. Non-star/non-snowflake plans keep the allowlist empty.
fn star_order_runtime_replay_ids(
	render_trace: &OptimizerExplainRenderTrace,
) -> Result<BTreeSet<JoinReplayId>, BindingError> {
	let mut targets = BTreeSet::new();
	match &render_trace.join_order {
		JoinOrderRenderState::ObservedSnowflake(_) => {
			targets.extend(render_trace.join_order_edges.iter().map(|edge| edge.target));
		}
		JoinOrderRenderState::StarReceipts(receipts) => {
			for receipt in receipts {
				if matches!(receipt.outcome, StarOrderOutcome::Recognized(_)) {
					targets.extend(receipt.edge_targets.iter().copied());
				}
			}
		}
		JoinOrderRenderState::None => {}
	}
	// ADR 195: galaxy runtime measurement is gathered explicitly from the galaxy substrate, never by
	// inheriting the tree allowlist. The reachable galaxy still rides a snowflake-shaped tree today, so
	// this is BTreeSet-deduped in that case — but the union is load-bearing: it makes the galaxy edges
	// the source of truth for which observations to collect, independent of the join_order state.
	if let Some(galaxy) = &render_trace.galaxy_order {
		targets.extend(galaxy.edges.iter().map(|edge| edge.target));
	}
	// ADR 196: bridge measurement is gathered from the bridge substrate's edge targets, the 195 pattern.
	if let Some(bridge) = &render_trace.bridge_order {
		targets.extend(
			bridge
				.links
				.iter()
				.flat_map(|link| [link.incoming_target, link.outgoing_target]),
		);
	}
	// ADR 203: gather the chain's per-hop edge targets so the sampled path measures the whole path.
	if let Some(chain) = &render_trace.bridge_chain_order {
		targets.extend(
			chain
				.hops
				.iter()
				.flat_map(|hop| [hop.incoming_target, hop.outgoing_target]),
		);
	}
	for receipt in &render_trace.preserved_outer_order {
		targets.extend(receipt.receipt.edge_targets.iter().copied());
	}
	targets
		.into_iter()
		.map(|target| {
			u32::try_from(target.join_index)
				.map(JoinReplayId)
				.map_err(|_| BindingError::UnsupportedExpression {
					message: "EXPLAIN ANALYZE join replay id exceeds u32".to_string(),
				})
		})
		.collect()
}

fn star_order_probe_label(outcome: &StarOrderOutcome) -> String {
	match outcome {
		StarOrderOutcome::Recognized(evidence) => evidence.probe.clone(),
		StarOrderOutcome::Declined(_) => "star_candidate".to_string(),
	}
}

fn preserved_outer_order_receipt(
	edges: &[JoinOrderEdgeEvidence],
	margin_step: SoftMarginLadderStep,
) -> Option<PreservedOuterOrderReceipt> {
	if !edges.iter().any(is_preserved_outer_equi_edge) {
		return None;
	}
	let mut counterfactual_edges = edges.to_vec();
	for edge in &mut counterfactual_edges {
		if is_preserved_outer_equi_edge(edge) {
			edge.inner_equi = true;
		}
		if edge.inner_equi {
			edge.probe_linearity = ProbeLinearity::Linear;
		}
	}
	let outcome = analyze(&counterfactual_edges, margin_step)?;
	if !matches!(outcome, StarOrderOutcome::Recognized(_)) {
		return None;
	}
	let edge_targets = counterfactual_edges
		.iter()
		.map(|edge| edge.target)
		.collect::<Vec<_>>();
	Some(PreservedOuterOrderReceipt {
		receipt: StarOrderRenderReceipt {
			probe_label: star_order_probe_label(&outcome),
			outcome,
			edge_targets,
		},
	})
}

fn is_preserved_outer_equi_edge(edge: &JoinOrderEdgeEvidence) -> bool {
	matches!(
		edge.join_kind,
		JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter
	) && !edge.inner_equi
		&& !edge.left_key_signature.is_empty()
		&& !edge.right_key_signature.is_empty()
}

struct Binder<'a> {
	catalog: &'a dyn Catalog,
	config: &'a PlannerConfig,
	// ADR 219: all join-order policy is resolved once at bind entry. Manual mode uses the
	// configured base without reading evidence; ApplyAdvisory derives every field from one
	// receipt-gate snapshot.
	join_order_policy: EffectiveJoinOrderPolicy,
	// The sink for optimizer decisions. Normally a runtime-evidence recorder; a scratch recognition
	// bind / declined-actuation re-bind carries a zero-size `NoOpDecisionRecorder` instead, so it cannot
	// pollute the shared log — a structural fact, not a runtime flag.
	decision_recorder: Arc<dyn OptimizerDecisionRecorder + Send + Sync>,
	statement_timestamp_nanos: i64,
}

impl<'a> Binder<'a> {
	#[cfg(test)]
	fn new(catalog: &'a dyn Catalog, config: &'a PlannerConfig) -> Self {
		Self::new_at(
			catalog,
			config,
			crate::exec::StatementContext::capture_system().timestamp_nanos,
		)
	}

	fn new_at(
		catalog: &'a dyn Catalog,
		config: &'a PlannerConfig,
		statement_timestamp_nanos: i64,
	) -> Self {
		Self::new_with_decision_provenance(
			catalog,
			config,
			OptimizerRuntimeVerdictProvenance::Native,
			statement_timestamp_nanos,
		)
	}

	fn new_with_decision_provenance(
		catalog: &'a dyn Catalog,
		config: &'a PlannerConfig,
		decision_provenance: OptimizerRuntimeVerdictProvenance,
		statement_timestamp_nanos: i64,
	) -> Self {
		let join_order_policy = effective_join_order_policy(config);
		Self {
			catalog,
			config,
			join_order_policy,
			decision_recorder: Arc::new(OptimizerRuntimeDecisionRecorder::new(
				Arc::clone(&config.runtime_evidence),
				decision_provenance,
			)),
			statement_timestamp_nanos,
		}
	}

	fn with_decision_provenance(
		&self,
		decision_provenance: OptimizerRuntimeVerdictProvenance,
	) -> Binder<'a> {
		Binder {
			catalog: self.catalog,
			config: self.config,
			join_order_policy: self.join_order_policy,
			decision_recorder: Arc::new(OptimizerRuntimeDecisionRecorder::new(
				Arc::clone(&self.config.runtime_evidence),
				decision_provenance,
			)),
			statement_timestamp_nanos: self.statement_timestamp_nanos,
		}
	}

	// A binder that discards optimizer decisions — for binds whose decisions must not reach the shared
	// log (scratch recognition, declined-actuation re-bind).
	fn without_decision_recording(&self) -> Binder<'a> {
		Binder {
			catalog: self.catalog,
			config: self.config,
			join_order_policy: self.join_order_policy,
			decision_recorder: Arc::new(NoOpDecisionRecorder),
			statement_timestamp_nanos: self.statement_timestamp_nanos,
		}
	}
}

fn decision_provenance_for_outer_join_demotions(
	outer_join_demotions: &[OuterJoinDemotionReceipt],
) -> OptimizerRuntimeVerdictProvenance {
	if outer_join_demotions.is_empty() {
		OptimizerRuntimeVerdictProvenance::Native
	} else {
		OptimizerRuntimeVerdictProvenance::Demoted
	}
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PlanOptimizerFacts {
	origin: PlanOrigin,
	byte_upper_bound: Option<ByteCount>,
	estimated_byte_size: Option<ByteCount>,
	estimated_row_count: Option<RowCount>,
	row_upper_bound: Option<RowCount>,
	/// ADR 472: a filter narrowed this side but no estimator could say by how
	/// much, so its byte bound is stale as a size proxy. Size-comparison
	/// consumers must treat the side as unknown-size rather than trust the bound.
	filtered_without_estimate: bool,
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
			filtered_without_estimate: false,
		}
	}

	fn composite_unknown() -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound: None,
			filtered_without_estimate: false,
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
			filtered_without_estimate: false,
		}
	}

	fn composite_with_row(row_upper_bound: Option<RowCount>) -> Self {
		Self {
			origin: PlanOrigin::Composite,
			byte_upper_bound: None,
			estimated_byte_size: None,
			estimated_row_count: None,
			row_upper_bound,
			filtered_without_estimate: false,
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
			filtered_without_estimate: false,
		}
	}

	fn empty_relation() -> Self {
		Self::composite_with_bounds(Some(ByteCount::ZERO), Some(RowCount::new(0)))
	}

	/// Exact by construction: `count` rows of one 8-byte column. The checked
	/// multiply cannot fire while the cardinality cap holds (the cap is far
	/// below `u64::MAX / 8`), and if the cap ever moved past that line this
	/// panics rather than saturating under an "exact" contract.
	fn range_relation(count: u64) -> Self {
		let bytes = count
			.checked_mul(8)
			.expect("range cardinality is capped below byte-fact overflow");
		Self {
			origin: PlanOrigin::SingleBaseRelation,
			byte_upper_bound: Some(ByteCount::new(bytes)),
			estimated_byte_size: Some(ByteCount::new(bytes)),
			estimated_row_count: Some(RowCount::new(count)),
			row_upper_bound: Some(RowCount::new(count)),
			filtered_without_estimate: false,
		}
	}

	fn preserve_volume_non_increasing(self) -> Self {
		self
	}

	/// ADR 472: a filter narrowed this side but no estimator could size it, so
	/// the byte bound is stale as a size proxy.
	fn mark_filtered_without_estimate(self) -> Self {
		Self {
			filtered_without_estimate: true,
			..self
		}
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
			filtered_without_estimate: self.filtered_without_estimate,
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
	statement_timestamp_nanos: i64,
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
				None,
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
		// ADR 255: the active recursive CTE's `(name, working-table schema)`. `Some` only while binding a
		// recursive term; the `RecursiveRef` leaf reads it. Only one recursive CTE is ever active (no
		// mutual recursion), so a single owned slot suffices — threaded (cloned) down the recursive
		// descent so a `RecursiveRef` nested under Filter/Project/Alias still sees it.
		recursive_ctx: Option<(String, Arc<Schema>)>,
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
					let mut plan = plan;
					set_scan_estimated_byte_size(&mut plan, facts.estimated_byte_size);
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
					let mut plan = plan;
					set_scan_estimated_byte_size(&mut plan, facts.estimated_byte_size);
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
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
						facts.mark_filtered_without_estimate()
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
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
				LogicalPlan::Union { left, right } => {
					let left = self
						.bind_plan_with_trace(
							left,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let right = self
						.bind_plan_with_trace(
							right,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					bind_union(left, right)
				}
				LogicalPlan::SetOpAll { left, right, kind } => {
					let left = self
						.bind_plan_with_trace(
							left,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let right = self
						.bind_plan_with_trace(
							right,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					self.bind_set_op_all(
						left,
						right,
						*kind,
						trace,
						render_trace,
						trace.join_decisions.len(),
					)
					.await
				}
				LogicalPlan::Repeat { input, count } => {
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let count_expr = self
						.bind_projection_expr_program_scoped(
							std::slice::from_ref(count),
							&input.schema(),
							&input.scope,
						)
						.await?;
					let count_column = repeat_count_column(&count_expr)?;
					let output_schema = repeat_output_schema(&input.schema(), count_column)?;
					let scope = input.scope.with_schema(output_schema.clone());
					let join_order_sources = input.join_order_sources.clone();
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Repeat {
							input: Box::new(input.plan),
							count_column,
							output_schema,
						},
						scope,
						PlanOptimizerFacts::composite_unknown(),
						join_order_sources,
					))
				}
				LogicalPlan::Distinct { input } => {
					if let Some(message) = distinct_input_unsupported_message(input) {
						return Err(BindingError::UnsupportedExpression {
							message: message.to_string(),
						});
					}
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let group_expr = all_output_columns_program(&input.schema())?;
					let output_schema = input.schema();
					let exchange_facts = input.facts.exchange();
					let exchange_scope = input.scope.with_schema(output_schema);
					let join_order_sources = input.join_order_sources.clone();
					let exchange = ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Exchange {
							input: Box::new(input.plan),
							partition_count: DEFAULT_DISTINCT_PARTITION_COUNT,
							key: group_expr.clone(),
						},
						exchange_scope,
						exchange_facts,
						join_order_sources,
					);
					self.bind_bound_aggregate(exchange, group_expr, Vec::new())
				}
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					if let Some(rewrite) =
						Box::pin(self.try_outer_count_preaggregation(input, group_expr, aggr_expr))
							.await?
					{
						let mut rewritten = self
							.bind_plan_with_trace(
								&rewrite.plan,
								trace,
								render_trace,
								join_order_source_capture,
								recursive_ctx.clone(),
							)
							.await?;
						let ExecutionPlan::Project {
							exprs,
							output_schema,
							..
						} = &mut rewritten.plan
						else {
							unreachable!(
								"outer-count preaggregation rewrite must bind to a root project"
							);
						};
						if !is_outer_count_contract_project(exprs) {
							unreachable!(
								"outer-count preaggregation rewrite lost its field-contract project"
							);
						}
						*output_schema = rewrite.output_schema;
						rewritten.scope = rewrite.output_scope;
						return Ok(rewritten);
					}
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
					self.bind_bound_aggregate(input, group_expr, bound_aggr_expr)
				}
				LogicalPlan::KernelDensity { input, spec } => {
					let KernelDensitySpec {
						group_expr,
						value_expr,
						points,
					} = spec.as_ref();
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let input_schema = input.schema();
					let group_program = self
						.bind_expr_program_scoped(
							std::slice::from_ref(group_expr),
							&input_schema,
							&input.scope,
						)
						.await?;
					let value_program = self
						.bind_expr_program_scoped(
							std::slice::from_ref(value_expr),
							&input_schema,
							&input.scope,
						)
						.await?;
					let group_type = group_program.root_type(0).cloned().ok_or_else(|| {
						BindingError::UnsupportedExpression {
							message: "KERNEL_DENSITY group expression has no output".to_string(),
						}
					})?;
					let value_type = value_program.root_type(0).ok_or_else(|| {
						BindingError::UnsupportedExpression {
							message: "KERNEL_DENSITY value expression has no output".to_string(),
						}
					})?;
					if !matches!(
						value_type,
						DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
					) {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"KERNEL_DENSITY value must be Int32, Int64, Float32, or Float64, got {value_type:?}"
							),
						});
					}
					if !is_supported_density_group_type(&group_type) {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"KERNEL_DENSITY group must be a supported scalar key type, got {group_type:?}"
							),
						});
					}
					let group_logical_type = group_program
						.root_logical_type(0)
						.unwrap_or(LogicalTypeId::NAKED);
					let output_schema = Arc::new(Schema::new(vec![
						field_with_logical_type_metadata(
							"group",
							group_type,
							true,
							group_logical_type,
						),
						Field::new("evaluation_value", DataType::Float64, false),
						Field::new("density", DataType::Float64, false),
					]));
					let input_scope = input.scope.with_schema(output_schema.clone());
					let join_order_sources = input.join_order_sources.clone();
					let child_plan = ExecutionPlan::Exchange {
						input: Box::new(input.plan),
						partition_count: 1,
						key: BoundExprProgram::empty(),
					};
					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::KernelDensity {
							input: Box::new(child_plan),
							spec: Box::new(BoundKernelDensitySpec {
								group_expr: group_program,
								value_expr: value_program,
								points: *points,
								output_schema,
							}),
						},
						input_scope,
						PlanOptimizerFacts::composite_unknown(),
						join_order_sources,
					))
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
				LogicalPlan::Window {
					input,
					partition_by,
					order_by,
					functions,
				} => {
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
						.await?;
					let input_schema = input.schema();
					let partition_by = self
						.bind_expr_program_scoped(partition_by, &input_schema, &input.scope)
						.await?;
					// Bind only the user's ORDER BY keys (no total-order tiebreaker expansion): these are
					// the *peer* keys for RANK/DENSE_RANK ties. The executor totalizes the sort itself for
					// deterministic ROW_NUMBER among peers.
					let order_by = order_by
						.iter()
						.map(|key| {
							Ok(ExecutionSortKey {
								ordinal: bind_sort_key_ordinal(
									&key.expr,
									&input_schema,
									&input.scope,
								)?,
								descending: key.descending,
								nulls_first: key.nulls_first,
							})
						})
						.collect::<Result<Vec<_>, BindingError>>()?;
					// Row-preserving: keep every input column, append one result column per window call.
					// Ranking + COUNT(*) → non-null Int64; other aggregate windows take the aggregate's
					// result type/nullability (e.g. AVG → nullable Float64), bound over the SQL default
					// frame. The argument program (if any) is bound here for the executor.
					let logical_types = self.catalog.logical_type_registry();
					let mut fields = input_schema
						.fields()
						.iter()
						.map(|field| field.as_ref().clone())
						.collect::<Vec<_>>();
					let mut bound_calls: Vec<BoundWindowCall> = Vec::with_capacity(functions.len());
					for call in functions {
						let arg = match &call.function {
							WindowFunction::RowNumber
							| WindowFunction::Rank
							| WindowFunction::DenseRank
							| WindowFunction::Ntile { .. } => {
								fields.push(Field::new(&call.output_name, DataType::Int64, false));
								None
							}
							WindowFunction::Aggregate(function) if call.args.is_empty() => {
								// COUNT(*): no argument; non-null Int64 row count over the frame.
								let _ = function;
								fields.push(Field::new(&call.output_name, DataType::Int64, false));
								None
							}
							WindowFunction::Aggregate(function) => {
								let arg = self
									.bind_expr_program_scoped(
										&call.args,
										&input_schema,
										&input.scope,
									)
									.await?;
								let bound_agg = BoundAggregateCall {
									function: function.clone(),
									args: arg.clone(),
								};
								let field = aggregate_field(&bound_agg, &logical_types)?
									.with_name(call.output_name.clone());
								fields.push(field);
								Some(arg)
							}
							WindowFunction::Value(_) => {
								// ADR 246: bind the value expression; the output column takes the expr's
								// type but is nullable (partition edges, empty frames, and out-of-range
								// NTH_VALUE yield NULL).
								let arg = self
									.bind_expr_program_scoped(
										&call.args,
										&input_schema,
										&input.scope,
									)
									.await?;
								let field = schema_for_exprs(&arg)
									.field(0)
									.as_ref()
									.clone()
									.with_name(call.output_name.clone())
									.with_nullable(true);
								fields.push(field);
								Some(arg)
							}
						};
						if let Some(frame) = &call.frame {
							validate_range_frame_offsets(frame, &order_by, &input_schema)?;
						}
						bound_calls.push(BoundWindowCall {
							function: call.function.clone(),
							arg,
							frame: call.frame.clone(),
							ignore_nulls: call.ignore_nulls,
						});
					}
					let output_schema = Arc::new(Schema::new(fields));
					let facts = PlanOptimizerFacts::composite_with_row(input.facts.row_upper_bound);
					let scope = input.scope.with_schema(output_schema.clone());
					let join_order_sources = input.join_order_sources.clone();

					Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
						ExecutionPlan::Window {
							input: Box::new(input.plan),
							partition_by,
							order_by,
							functions: bound_calls,
							output_schema,
						},
						scope,
						facts,
						join_order_sources,
					))
				}
				LogicalPlan::Take { input, limit } => {
					let input = self
						.bind_plan_with_trace(
							input,
							trace,
							render_trace,
							join_order_source_capture,
							recursive_ctx.clone(),
						)
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
				// ADR 255: a recursive CTE. Bind the seed (anchor) in the outer scope, then bind the
				// recursive term with the CTE name resolving (via the threaded `recursive_ctx`) to a
				// working-table relation whose schema is the seed's output types relabeled by the
				// declared `column_names`. Seed and recursive output schemas must union-align (same
				// arity + unifiable types), reusing the `Union` arm's reconciliation.
				LogicalPlan::RecursiveCte {
					name,
					column_names,
					seed,
					recursive,
				} => {
					self.bind_recursive_cte(
						name,
						column_names,
						seed,
						recursive,
						trace,
						render_trace,
						join_order_source_capture,
					)
					.await
				}
				// ADR 255: the self-reference leaf inside a recursive term. It binds to the working-table
				// relation carried by `recursive_ctx` (the seed schema relabeled by `column_names`). A
				// `RecursiveRef` reached outside a recursive term is a lowering invariant violation.
				LogicalPlan::RecursiveRef {
					name,
					column_names: _,
				} => {
					let Some((ctx_name, ctx_schema)) = recursive_ctx else {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"recursive self-reference to `{name}` appears outside a recursive CTE term"
							),
						});
					};
					let scope = RelationScope::scan(TableRef::new(ctx_name), &ctx_schema);
					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::RecursiveRef {
							output_schema: ctx_schema,
						},
						scope,
						PlanOptimizerFacts::composite_unknown(),
					))
				}
				LogicalPlan::Range { count } => {
					if *count < 0 {
						return Err(BindingError::UnsupportedExpression {
							message: format!("range cardinality must be non-negative, got {count}"),
						});
					}
					if *count > crate::ir::RANGE_MAX_CARDINALITY {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"range cardinality {count} exceeds the materializing source's \
								 cap of {}",
								crate::ir::RANGE_MAX_CARDINALITY
							),
						});
					}
					let schema = crate::ir::range_relation_schema();
					let scope = RelationScope::scan(TableRef::new("range"), &schema);
					Ok(ScopedExecutionPlan::new_with_facts(
						ExecutionPlan::Range {
							count: *count,
							schema,
						},
						scope,
						PlanOptimizerFacts::range_relation(*count as u64),
					))
				}
				LogicalPlan::DeferredExists {
					outer,
					inner,
					predicates,
					negated,
				} => {
					self.bind_deferred_exists(
						outer,
						inner,
						predicates,
						*negated,
						trace,
						render_trace,
						join_order_source_capture,
					)
					.await
				}
				LogicalPlan::DeferredScalarSubquery {
					outer,
					inner,
					predicates,
					aggregate,
					alias,
				} => {
					self.bind_deferred_scalar_subquery(
						outer,
						inner,
						predicates,
						aggregate,
						alias,
						trace,
						render_trace,
						join_order_source_capture,
					)
					.await
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					on_filter,
					null_safe_keys,
					strategy_intent,
				} => {
					if on_filter.is_some() {
						if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) && !on.is_empty()
						{
							return self
								.bind_join(
									left,
									right,
									*kind,
									on,
									*null_safe_keys,
									*strategy_intent,
									trace,
									render_trace,
									join_order_source_capture,
									on_filter.as_ref(),
								)
								.await;
						}
						let normalized = self
							.push_on_clause_residual(
								(**left).clone(),
								(**right).clone(),
								*kind,
								on.clone(),
								on_filter.clone(),
								*null_safe_keys,
								*strategy_intent,
							)
							.await?;
						return self
							.bind_plan_with_trace(
								&normalized,
								trace,
								render_trace,
								join_order_source_capture,
								recursive_ctx,
							)
							.await;
					}
					// ADR 518: official q20 lowers its membership filter one
					// wrapper above the deferred scalar. Move that exact semi
					// onto the scalar's outer input so reachable-key derivation
					// sees the surviving domain; the moved shape cannot match
					// this recognizer on re-entry.
					if matches!(kind, JoinKind::LeftSemi | JoinKind::RightSemi) {
						if let Some(rewritten) = self
							.try_expose_membership_to_deferred_scalar(
								left,
								right,
								*kind,
								on,
								*null_safe_keys,
								*strategy_intent,
							)
							.await
						{
							return self
								.bind_plan_with_trace(
									&rewritten,
									trace,
									render_trace,
									join_order_source_capture,
									recursive_ctx,
								)
								.await;
						}
					}
					// ADR 515: membership-first composition — commute the pinned
					// membership semi to its key-source branch, then bind the
					// rewritten tree. Termination is structural: the rewritten
					// root is Inner, and the relocated semi's outer side is a
					// single branch the recognizer declines on re-entry.
					if matches!(kind, JoinKind::LeftSemi) && !*null_safe_keys {
						if let Some(rewritten) = self
							.try_commute_membership_semi(left, right, on, *strategy_intent)
							.await
						{
							return self
								.bind_plan_with_trace(
									&rewritten,
									trace,
									render_trace,
									join_order_source_capture,
									recursive_ctx,
								)
								.await;
						}
					}
					// ADR 200: the unified bounded graph root-swap, galaxy then bridge. A query is a
					// galaxy XOR a bridge (conformed vs distinct junction signatures), so the arms never
					// both fire. The committed decision is accrued here (one per recognized graph); on the
					// no-op recorder of a re-rooted re-bind it is discarded, so no phantom doubles it. If
					// either actuates, the re-rooted snowflake is already bound, so we return it.
					for shape in [GraphShape::Galaxy, GraphShape::Bridge, GraphShape::Chain] {
						let result = self
							.try_bind_graph_actuation(plan, trace, render_trace, shape)
							.await?;
						match &result {
							GraphActuationResult::NotCandidate => {}
							GraphActuationResult::Actuated(_) => self.accrue_graph_order_decision(
								shape,
								OptimizerDecisionOutcome::Reordered,
							),
							GraphActuationResult::KeptAlreadyOptimal => self
								.accrue_graph_order_decision(
									shape,
									OptimizerDecisionOutcome::KeptOptimal,
								),
							GraphActuationResult::KeptThinMargin => self
								.accrue_graph_order_decision(
									shape,
									OptimizerDecisionOutcome::KeptThinMargin,
								),
							GraphActuationResult::Declined(reason) => self
								.accrue_graph_order_decision(
									shape,
									OptimizerDecisionOutcome::Declined(*reason),
								),
						}
						if let GraphActuationResult::Actuated(plan) = result {
							return Ok(plan);
						}
					}
					// ADR 512: reachable-key reduction on the scalar-recombine shape, applied
					// only on the direct bind path — an actuated graph re-bind forgoes the
					// reduction, and the rewrite never re-enters this arm, so no shape
					// oscillation with the root-swap passes is possible. The recognizer
					// declines unless every precondition is proved.
					let adr512_reduced_right;
					let right: &LogicalPlan =
						if matches!(kind, JoinKind::LeftOuter) && !*null_safe_keys {
							match self.try_reduce_scalar_recombine(left, right, on).await {
								Some(reduced) => {
									adr512_reduced_right = reduced;
									&adr512_reduced_right
								}
								None => right,
							}
						} else {
							right
						};
					match self
						.try_bind_preserved_outer_star_actuation(plan, trace, render_trace)
						.await?
					{
						StarOrderActuationResult::Actuated(plan) => return Ok(plan),
						StarOrderActuationResult::NotCandidate => {}
						StarOrderActuationResult::Declined => unreachable!(
							"preserved-outer star actuation is verdict-only and does not decline"
						),
					}
					match self
						.try_bind_star_order_actuation(plan, trace, render_trace)
						.await?
					{
						StarOrderActuationResult::Actuated(plan) => return Ok(plan),
						StarOrderActuationResult::NotCandidate => {}
						StarOrderActuationResult::Declined => {
							// The declined-actuation fall-back re-binds the same joins; bind on a binder
							// that discards decisions so the receipt is not recorded twice.
							return self
								.without_decision_recording()
								.bind_join(
									left,
									right,
									*kind,
									on,
									*null_safe_keys,
									*strategy_intent,
									trace,
									render_trace,
									join_order_source_capture,
									None,
								)
								.await;
						}
					}
					self.bind_join(
						left,
						right,
						*kind,
						on,
						*null_safe_keys,
						*strategy_intent,
						trace,
						render_trace,
						join_order_source_capture,
						None,
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
					intent_request,
					if_not_exists,
					or_replace,
					source,
					materialization_certificate,
				} => {
					let plan = self
						.bind_create_table_as_select(
							table,
							intent_request,
							*if_not_exists,
							*or_replace,
							source,
							materialization_certificate.clone(),
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

	// ADR 255: bind a recursive CTE. This is a SEPARATE boxed future (not an inline `bind_plan_with_trace`
	// arm) on purpose: `bind_plan_with_trace` is a deeply-recursing async fn, and inlining this body's
	// large locals (two `ScopedExecutionPlan`s held across awaits) would inflate every frame of that
	// recursion enough to overflow the stack on otherwise-shallow plans. Bind the seed (anchor) in the
	// outer scope, then bind the recursive term with the CTE name resolving (via `recursive_ctx`) to a
	// working-table relation whose schema is the seed's output types relabeled by `column_names`. Seed
	// and recursive output schemas must union-align (same arity + unifiable types), reusing the `Union`
	// arm's reconciliation.
	fn bind_recursive_cte<'b>(
		&'b self,
		name: &'b str,
		column_names: &'b [String],
		seed: &'b LogicalPlan,
		recursive: &'b LogicalPlan,
		trace: &'b mut OptimizerTrace,
		render_trace: &'b mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Pin<Box<dyn Future<Output = Result<ScopedExecutionPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let bound_seed = self
				.bind_plan_with_trace(seed, trace, render_trace, join_order_source_capture, None)
				.await?;
			let seed_schema = bound_seed.schema();
			if seed_schema.fields().len() != column_names.len() {
				return Err(BindingError::UnsupportedExpression {
					message: format!(
						"recursive CTE `{name}` declares {} column(s) but the seed produces {}",
						column_names.len(),
						seed_schema.fields().len()
					),
				});
			}
			// The working-table schema `s_prime`: seed data-types/nullability, names relabeled.
			let s_prime_fields = seed_schema
				.fields()
				.iter()
				.zip(column_names.iter())
				.map(|(field, column_name)| {
					field_with_logical_type_metadata(
						column_name.clone(),
						field.data_type().clone(),
						field.is_nullable(),
						LogicalTypeId::NAKED,
					)
				})
				.collect::<Vec<_>>();
			let s_prime: Arc<Schema> = Arc::new(Schema::new(s_prime_fields));
			let bound_recursive = self
				.bind_plan_with_trace(
					recursive,
					trace,
					render_trace,
					join_order_source_capture,
					Some((name.to_string(), s_prime.clone())),
				)
				.await?;
			// Reconcile seed/recursive output schemas (union-compatible), then normalize each arm to the
			// common `output_schema` so the fixpoint append is type-aligned.
			let output_schema = set_op_output_schema(&s_prime, &bound_recursive.schema())?;
			let seed_plan = normalize_set_op_arm(bound_seed.plan, &output_schema)?;
			let recursive_plan = normalize_set_op_arm(bound_recursive.plan, &output_schema)?;
			let scope = RelationScope::scan(TableRef::new(name.to_string()), &output_schema);
			Ok(ScopedExecutionPlan::new_with_facts(
				ExecutionPlan::RecursiveCte {
					seed: Box::new(seed_plan),
					recursive: Box::new(recursive_plan),
					output_schema,
				},
				scope,
				// A recursive result has an unknown row count — the most conservative facts.
				PlanOptimizerFacts::composite_unknown(),
			))
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
			statement_timestamp_nanos: self.statement_timestamp_nanos,
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
			statement_timestamp_nanos: self.statement_timestamp_nanos,
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
			statement_timestamp_nanos: self.statement_timestamp_nanos,
			logical_type_specs: &logical_casts,
			logical_type_registry: &logical_type_registry,
			relation_scope,
		};
		bind_aggregate_call(function, args, input_schema, &ctx)
	}

	fn bind_bound_aggregate(
		&self,
		input: ScopedExecutionPlan,
		group_expr: BoundExprProgram,
		aggr_expr: Vec<BoundAggregateCall>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let input = prepare_aggregate_input_exchange(input, &group_expr, self.config);
		let logical_types = self.catalog.logical_type_registry();
		let output_schema = aggregate_schema(&group_expr, &aggr_expr, &logical_types)?;
		let scope = input.scope.aggregate(&group_expr, &output_schema);

		// ADR 239 (scalar-local Cross refinement): a global aggregate (no group keys) produces exactly
		// one row. Record that as a hard `row_upper_bound = 1` fact so a CROSS recombine of global
		// aggregate branches can bind as a local cartesian rather than a (rejected) broadcast.
		let facts = if group_expr.is_empty_roots() {
			PlanOptimizerFacts::composite_with_row(Some(RowCount::new(1)))
		} else {
			PlanOptimizerFacts::composite_unknown()
		};

		Ok(ScopedExecutionPlan::new_with_facts(
			ExecutionPlan::Aggregate {
				input: Box::new(input.plan),
				group_expr,
				aggr_expr,
				output_schema,
			},
			scope,
			facts,
		))
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

	#[allow(clippy::too_many_arguments)]
	async fn bind_create_table_as_select(
		&self,
		table: &TableRef,
		intent_request: &str,
		if_not_exists: bool,
		or_replace: bool,
		source: &LogicalPlan,
		materialization_certificate: Option<Vec<u8>>,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ExecutionPlan, BindingError> {
		reject_information_schema_target(table)?;
		let source = self
			.bind_plan_with_trace(source, trace, render_trace, join_order_source_capture, None)
			.await?;
		let source = source.plan;
		if let Some(certificate) = materialization_certificate.as_deref() {
			require_certificate_describes_bound_source(&source, certificate)?;
		}
		let source_schema = source.schema();
		let schema = infer_ctas_schema(&source_schema)?;
		let intent_fingerprint = ctas_intent_fingerprint(
			table,
			intent_request,
			&schema,
			&self.config.session_timezone,
			self.config.allow_experimental_kernels,
		);

		Ok(ExecutionPlan::CreateTableAsSelect {
			table: table.clone(),
			schema,
			intent_fingerprint,
			if_not_exists,
			or_replace,
			source: Box::new(source),
			materialization_certificate,
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
			local_table_id: binding.local_table_id,
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
			local_table_id: binding.local_table_id,
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
			.bind_plan_with_trace(source, trace, render_trace, join_order_source_capture, None)
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
			local_table_id: binding.local_table_id,
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
			local_table_id: binding.local_table_id,
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
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_DECISIONS_TABLE)
		{
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerDecisions,
				information_schema_optimizer_decisions_schema(),
				information_schema_optimizer_decision_rows(
					&self.config.runtime_evidence.decision_snapshot(),
				)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS_TABLE)
		{
			let rows = optimizer_decision_conflict_rows(self.config);
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerDecisionConflicts,
				information_schema_optimizer_decision_conflicts_schema(),
				information_schema_optimizer_decision_conflict_rows(&rows)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION_TABLE)
		{
			let rows = optimizer_memo_admission_rows(self.config);
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerMemoAdmission,
				information_schema_optimizer_memo_admission_schema(),
				information_schema_optimizer_memo_admission_rows(&rows)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE)
		{
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerProbeLinearityFences,
				information_schema_optimizer_probe_linearity_fences_schema(),
				information_schema_optimizer_probe_linearity_fence_rows(
					&self
						.config
						.runtime_evidence
						.probe_linearity_fence_snapshot(),
				)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS_TABLE)
		{
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerRuntimeVerdicts,
				information_schema_optimizer_runtime_verdicts_schema(),
				information_schema_optimizer_runtime_verdict_rows(
					&self.config.runtime_evidence.verdict_snapshot(),
				)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY_TABLE)
		{
			let reports = margin_advisory_report_rows(self.config);
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerMarginAdvisory,
				information_schema_optimizer_margin_advisory_schema(),
				information_schema_optimizer_margin_advisory_rows(&reports)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES_TABLE)
		{
			let rows = optimizer_receipt_gate_rows(self.config);
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerReceiptGates,
				information_schema_optimizer_receipt_gates_schema(),
				information_schema_optimizer_receipt_gate_rows(&rows)?,
				projection,
			);
		}
		if table
			.table
			.eq_ignore_ascii_case(INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS_TABLE)
		{
			return system_table_plan(
				SystemTableKind::InformationSchemaOptimizerEvidenceStatus,
				information_schema_optimizer_evidence_status_schema(),
				information_schema_optimizer_evidence_status_rows(
					&self.config.runtime_evidence.status_snapshot(),
				)?,
				projection,
			);
		}
		Err(BindingError::UnsupportedExpression {
			message: format!(
				"unknown information_schema relation: {}; supported relations are {INFORMATION_SCHEMA_LOGICAL_TYPES}, {INFORMATION_SCHEMA_TABLES}, {INFORMATION_SCHEMA_COLUMNS}, {INFORMATION_SCHEMA_SCHEMATA}, {INFORMATION_SCHEMA_OPTIMIZER_DECISIONS}, {INFORMATION_SCHEMA_OPTIMIZER_DECISION_CONFLICTS}, {INFORMATION_SCHEMA_OPTIMIZER_MEMO_ADMISSION}, {INFORMATION_SCHEMA_OPTIMIZER_PROBE_LINEARITY_FENCES}, {INFORMATION_SCHEMA_OPTIMIZER_RUNTIME_VERDICTS}, {INFORMATION_SCHEMA_OPTIMIZER_MARGIN_ADVISORY}, {INFORMATION_SCHEMA_OPTIMIZER_RECEIPT_GATES}, {INFORMATION_SCHEMA_OPTIMIZER_EVIDENCE_STATUS}",
				table_name(table)
			),
		})
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

	/// ADR 279: bind a deferred correlated EXISTS (the frontend couldn't classify its unqualified refs). Bind
	/// both sides for their catalog-resolved scopes, classify each predicate (`column_side`-aware): correlated
	/// equalities become join keys (outer-first), inner-only/constant predicates become an inner residual
	/// filter, and ambiguous/outer-only/non-equi correlations reject. Then bind through the normal join path as
	/// a `LeftSemi`/`LeftAnti` (`negated`) — so no later pass or the executor ever sees `DeferredExists`.
	async fn bind_deferred_exists(
		&self,
		outer: &LogicalPlan,
		inner: &LogicalPlan,
		predicates: &[Expr],
		negated: bool,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ScopedExecutionPlan, BindingError> {
		// Probe-bind both sides only to read their catalog-resolved scopes for classification. These binds are
		// discarded, so they run on SCRATCH trace/render state — otherwise any joins inside the sub-plans
		// record optimizer decisions + render edges that the real `bind_join` below then records again,
		// corrupting EXPLAIN/replay IDs and shifting join indices off the returned plan (ADR 279 review).
		let mut scope_trace = OptimizerTrace::default();
		let mut scope_render = None;
		let outer_scoped = self
			.bind_plan_with_trace(
				outer,
				&mut scope_trace,
				&mut scope_render,
				join_order_source_capture,
				None,
			)
			.await?;
		let inner_scoped = self
			.bind_plan_with_trace(
				inner,
				&mut scope_trace,
				&mut scope_render,
				join_order_source_capture,
				None,
			)
			.await?;
		let mut on = Vec::new();
		let mut residuals = Vec::new();
		let mut correlated_residuals = Vec::new();
		for predicate in predicates {
			match classify_deferred_exists_predicate(
				predicate,
				&outer_scoped.scope,
				&inner_scoped.scope,
			)? {
				DeferredExistsConjunct::Correlated {
					outer_key,
					inner_key,
				} => on.push((outer_key, inner_key)),
				DeferredExistsConjunct::CorrelatedResidual(residual) => {
					correlated_residuals.push(residual);
				}
				DeferredExistsConjunct::InnerResidual(residual) => residuals.push(residual),
			}
		}
		// Re-bind through the normal join path: inner residuals filter the inner; correlated pairs are the keys.
		let inner_logical = match residuals
			.into_iter()
			.reduce(|acc, next| Expr::And(Box::new(acc), Box::new(next)))
		{
			Some(predicate) => LogicalPlan::Filter {
				input: Box::new(inner.clone()),
				predicate,
			},
			None => inner.clone(),
		};
		let kind = if negated {
			JoinKind::LeftAnti
		} else {
			JoinKind::LeftSemi
		};
		let match_filter = correlated_residuals
			.into_iter()
			.reduce(|acc, next| Expr::And(Box::new(acc), Box::new(next)));
		self.bind_join(
			outer,
			&inner_logical,
			kind,
			&on,
			false,
			JoinStrategyIntent::Auto,
			trace,
			render_trace,
			join_order_source_capture,
			match_filter.as_ref(),
		)
		.await
	}

	/// ADR 282: bind a deferred correlated scalar subquery (the frontend couldn't classify its unqualified
	/// ref). Derive both logical scopes without binding the raw inner comma list, classify each predicate by
	/// `column_side` ownership into correlation key pairs + inner residuals, then build ADR 260's shape
	/// directly — a grouped aggregate keyed on the inner correlation keys, aliased `<alias>[key_i…, value]`,
	/// recombined via `LeftOuter` (or a keyless `Cross` when uncorrelated). The projection wrapper already
	/// rides the rewritten outer expression (which references `<alias>.value`).
	async fn bind_deferred_scalar_subquery(
		&self,
		outer: &LogicalPlan,
		inner: &LogicalPlan,
		predicates: &[Expr],
		aggregate: &(AggregateFunction, Vec<Expr>),
		alias: &str,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let Some(outer_scope) = self.logical_scope_for_outer_demotion(outer).await? else {
			return Err(BindingError::UnsupportedExpression {
				message: "unsupported correlated scalar subquery outer scope".to_string(),
			});
		};
		let Some(inner_scope) = self.logical_scope_for_outer_demotion(inner).await? else {
			return Err(BindingError::UnsupportedExpression {
				message: "unsupported correlated scalar subquery inner scope".to_string(),
			});
		};
		let mut pairs: Vec<(Expr, Expr)> = Vec::new();
		let mut residuals: Vec<Expr> = Vec::new();
		for predicate in predicates {
			match classify_deferred_scalar_predicate(predicate, &outer_scope, &inner_scope)? {
				DeferredExistsConjunct::Correlated {
					outer_key,
					inner_key,
				} => pairs.push((outer_key, inner_key)),
				DeferredExistsConjunct::CorrelatedResidual(_) => {
					return Err(BindingError::UnsupportedExpression {
						message: "unsupported correlated scalar subquery predicate".to_string(),
					});
				}
				DeferredExistsConjunct::InnerResidual(residual) => residuals.push(residual),
			}
		}
		// Inner residual filter, then group + aggregate on the inner correlation keys (ADR 260's shape).
		let inner_filtered = match residuals
			.into_iter()
			.reduce(|acc, next| Expr::And(Box::new(acc), Box::new(next)))
		{
			Some(predicate) => LogicalPlan::Filter {
				input: Box::new(inner.clone()),
				predicate,
			},
			None => inner.clone(),
		};
		let inner_filtered = self.recognize_cross_equi_joins(&inner_filtered).await?;
		// ADR 512: reduce the inner to reachable keys before grouping; declines
		// leave it untouched.
		let inner_filtered = self
			.reduce_deferred_scalar_inner(outer, inner_filtered, &pairs)
			.await;
		let group_expr: Vec<Expr> = pairs
			.iter()
			.map(|(_, inner_key)| inner_key.clone())
			.collect();
		let aggregate_plan = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Exchange {
				input: Box::new(inner_filtered),
				partition_count: DEFAULT_AUTO_JOIN_PARTITION_COUNT,
				key: group_expr.clone(),
			}),
			group_expr,
			aggr_expr: vec![aggregate.clone()],
		};
		let mut columns: Vec<String> = (0..pairs.len())
			.map(|index| format!("key_{index}"))
			.collect();
		columns.push("value".to_string());
		let scalar_subplan = LogicalPlan::Alias {
			input: Box::new(aggregate_plan),
			alias: TableRef::new(alias.to_string()),
			columns,
		};
		let on: Vec<(Expr, Expr)> = pairs
			.into_iter()
			.enumerate()
			.map(|(index, (outer_key, _))| {
				(
					outer_key,
					Expr::qualified_column(
						TableRef::new(alias.to_string()),
						format!("key_{index}"),
					),
				)
			})
			.collect();
		let kind = if on.is_empty() {
			JoinKind::Cross
		} else {
			JoinKind::LeftOuter
		};
		self.bind_join(
			outer,
			&scalar_subplan,
			kind,
			&on,
			false,
			JoinStrategyIntent::Auto,
			trace,
			render_trace,
			join_order_source_capture,
			None,
		)
		.await
	}

	async fn bind_join(
		&self,
		left: &LogicalPlan,
		right: &LogicalPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		null_safe_keys: bool,
		strategy_intent: JoinStrategyIntent,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		join_order_source_capture: JoinOrderSourceCapture,
		match_filter: Option<&Expr>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let left_plan = self
			.bind_plan_with_trace(left, trace, render_trace, join_order_source_capture, None)
			.await?;
		let right_plan = self
			.bind_plan_with_trace(right, trace, render_trace, join_order_source_capture, None)
			.await?;
		let declared_position = trace.join_decisions.len();
		self.bind_scoped_join(
			left_plan,
			right_plan,
			kind,
			on,
			null_safe_keys,
			strategy_intent,
			trace,
			render_trace,
			declared_position,
			match_filter,
		)
	}

	fn bind_scoped_join(
		&self,
		mut left_plan: ScopedExecutionPlan,
		mut right_plan: ScopedExecutionPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		null_safe_keys: bool,
		strategy_intent: JoinStrategyIntent,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		declared_position: usize,
		match_filter: Option<&Expr>,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let mut left_keys = Vec::new();
		let mut right_keys = Vec::new();
		let mut left_resolved_keys = Vec::new();
		let mut right_resolved_keys = Vec::new();

		if null_safe_keys && on.is_empty() {
			let output_schema = set_op_output_schema(&left_plan.schema(), &right_plan.schema())?;
			left_plan.plan = normalize_set_op_arm(left_plan.plan, &output_schema)?;
			right_plan.plan = normalize_set_op_arm(right_plan.plan, &output_schema)?;
			left_plan.scope = left_plan.scope.with_schema(output_schema.clone());
			right_plan.scope = right_plan.scope.with_schema(output_schema.clone());
			for (idx, field) in output_schema.fields().iter().enumerate() {
				let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
				let left_key = ResolvedJoinKey {
					column: ResolvedColumn {
						name: ColumnName(field.name().clone()),
						ordinal,
						data_type: field.data_type().clone(),
						nullable: field.is_nullable(),
						pbc_eligible: is_pbc_eligible_type(field.data_type()),
					},
					logical_type: LogicalTypeId::NAKED,
				};
				let right_key = left_key.clone();
				left_keys.push(ordinal);
				right_keys.push(ordinal);
				left_resolved_keys.push(left_key);
				right_resolved_keys.push(right_key);
			}
		} else {
			// ADR 265: the join-key path validates each key's logical type; thread the catalog registry
			// so durable (runtime-registered) logical types resolve, not just seeds.
			let logical_type_registry = self.catalog.logical_type_registry();
			for (left_expr, right_expr) in on {
				let (left_key, right_key) = bind_join_key_pair(
					left_expr,
					right_expr,
					&left_plan.scope,
					&right_plan.scope,
					&logical_type_registry,
				)?;
				validate_join_key_types(&left_key, &right_key)?;
				left_keys.push(left_key.column.ordinal);
				right_keys.push(right_key.column.ordinal);
				left_resolved_keys.push(left_key);
				right_resolved_keys.push(right_key);
			}
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
		let left_probe_linearity = broadcast_probe_linearity(&left_plan.plan);
		let right_probe_linearity = broadcast_probe_linearity(&right_plan.plan);
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
			// ADR 194: role-neutral graph endpoints. The key signatures identify the join-key columns
			// on each side; the endpoint rows come from `join_order_sources` keyed by the key-source
			// label (the relation that owns the key), not `*_plan.facts` (which on a composite input
			// describes the whole built-up side, not the key-owning relation).
			let left_key_signature =
				key_signature_for_join_keys(&left_plan.scope, &left_resolved_keys);
			let right_key_signature =
				key_signature_for_join_keys(&right_plan.scope, &right_resolved_keys);
			let left_endpoint_rows =
				join_order_endpoint_rows(left_plan.join_order_sources.as_ref(), probe.as_deref());
			let right_endpoint_rows = join_order_endpoint_rows(
				right_plan.join_order_sources.as_ref(),
				dimension.as_deref(),
			);
			render_trace.join_order_edges.push(JoinOrderEdgeEvidence {
				target: JoinTraceTarget { join_index },
				join_kind: kind,
				probe,
				dimension,
				declared_position,
				estimated_rows,
				estimate_source,
				inner_equi: kind == JoinKind::Inner && !left_resolved_keys.is_empty(),
				probe_linearity: left_probe_linearity,
				left_key_signature,
				right_key_signature,
				left_endpoint_rows,
				right_endpoint_rows,
			});
		}
		let decision = choose_join_strategy(
			replay_id,
			strategy_intent,
			kind,
			JoinInputOptimization {
				facts: left_plan.facts,
				probe_linearity: left_probe_linearity,
			},
			JoinInputOptimization {
				facts: right_plan.facts,
				probe_linearity: right_probe_linearity,
			},
			self.config,
		)?;
		trace.join_decisions.push(DecisionReceipt {
			target: JoinTraceTarget { join_index },
			strategy: decision.strategy,
			placement: decision.placement,
			reason: decision.reason,
		});
		if let JoinStrategyDecisionReason::HashByDefault {
			reason: HashByDefaultReason::ProbeNotScanLinear(fence),
		} = decision.reason
		{
			self.decision_recorder
				.record_probe_linearity_fence(OptimizerProbeLinearityConsumer::HashFallback, fence);
		}
		let binder_physicalizes_join = matches!(strategy_intent, JoinStrategyIntent::Auto)
			|| matches!(
				strategy_intent,
				JoinStrategyIntent::Pinned {
					strategy: JoinStrategy::Broadcast,
					..
				}
			);
		let mut local_hash_join = false;
		if binder_physicalizes_join {
			match decision.strategy {
				JoinStrategy::HashPartitioned => {
					// ADR 466: the placement a decision emits and the rewrite the plan
					// performs are one contract. A hash SwappedRightBuild swaps the
					// operands here, before the exchanges, exactly as broadcast does —
					// the downstream restore project keys on placement alone and must
					// never run for a swap that did not happen.
					// After this swap, `left_*` names the physical build side and
					// `right_*`/`physical_probe_linearity` the physical probe side.
					let physical_probe_linearity;
					if decision.placement == JoinPhysicalPlacement::SwappedRightBuild {
						std::mem::swap(&mut left_plan, &mut right_plan);
						std::mem::swap(&mut left_keys, &mut right_keys);
						std::mem::swap(&mut left_resolved_keys, &mut right_resolved_keys);
						physical_probe_linearity = left_probe_linearity;
					} else {
						physical_probe_linearity = right_probe_linearity;
					}
					// ADR 490: a direct executor can retain a qualifying hash build in memory,
					// but only when its caller proves this bind will never be fragmented. The
					// coordinator clears that proof on every distributed bind (ADR 489), and
					// the fragmenter still rejects an exchange-free hash shape loudly as a
					// backstop. Keep pinned strategies and non-inner joins on the established
					// exchanged path.
					let admission = local_hash_join_admission(
						self.config,
						matches!(strategy_intent, JoinStrategyIntent::Auto),
						kind,
						physical_probe_linearity,
						left_plan.facts.row_upper_bound,
						&left_resolved_keys
							.iter()
							.map(|key| key.column.data_type.clone())
							.collect::<Vec<_>>(),
					);
					trace
						.local_hash_join_admissions
						.push(LocalHashJoinAdmissionReceipt {
							target: JoinTraceTarget { join_index },
							admission,
						});
					local_hash_join = admission.admitted();
					if local_hash_join {
						// The no-exchange shape is intentionally accepted only by the local
						// validator branch below. It is structurally illegal to fragment.
					} else {
						// ADR 239 (scalar-local Cross): gather both provably-scalar sides to a single
						// partition and join locally — a partition_count=1 gather, NOT an N-way empty-key
						// hash repartition (which stays forbidden for partitioned/unbounded Cross inputs).
						let partition_count = if matches!(
							decision.reason,
							JoinStrategyDecisionReason::ScalarLocalCross
						) {
							1
						} else {
							DEFAULT_AUTO_JOIN_PARTITION_COUNT
						};
						left_plan.plan = exchange_for_join_keys(
							left_plan.plan,
							&left_resolved_keys,
							partition_count,
						)?;
						right_plan.plan = exchange_for_join_keys(
							right_plan.plan,
							&right_resolved_keys,
							partition_count,
						)?;
					}
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
			mirrored_broadcast_physical_kind(kind, decision.placement),
			decision.strategy,
			local_hash_join,
			&left_plan.plan,
			&right_plan.plan,
			&left_keys,
			&right_keys,
		)?;
		let physical_kind = mirrored_broadcast_physical_kind(kind, decision.placement);
		let physical_output_schema =
			join_schema(physical_kind, &left_plan.schema(), &right_plan.schema());
		let match_filter = match match_filter {
			Some(filter) => {
				if left_keys.is_empty() {
					return Err(BindingError::UnsupportedExpression {
						message: "unsupported non-equi EXISTS correlation without an equality key"
							.to_string(),
					});
				}
				if !matches!(
					physical_kind,
					JoinKind::LeftSemi
						| JoinKind::LeftAnti
						| JoinKind::RightSemi
						| JoinKind::RightAnti
				) {
					return Err(BindingError::UnsupportedExpression {
						message: "unsupported semi/anti join residual placement".to_string(),
					});
				}
				let match_schema = Arc::new(Schema::new(join_fields(
					&left_plan.schema(),
					&right_plan.schema(),
				)));
				let match_scope = RelationScope::join(
					JoinKind::Inner,
					&left_plan.scope,
					&right_plan.scope,
					&match_schema,
				);
				let logical_type_specs = HashMap::new();
				let logical_type_registry = self.catalog.logical_type_registry();
				let ctx = ExprBindContext {
					config: self.config,
					session_timezone: self.config.resolved_session_timezone()?,
					statement_timestamp_nanos: self.statement_timestamp_nanos,
					logical_type_specs: &logical_type_specs,
					logical_type_registry: &logical_type_registry,
					relation_scope: Some(&match_scope),
				};
				let bound_filter =
					bind_expr_program(std::slice::from_ref(filter), &match_schema, &ctx)?;
				require_single_boolean_root(&bound_filter, "JOIN match filter")?;
				Some(bound_filter)
			}
			None => None,
		};
		let join_plan = ExecutionPlan::Join {
			replay_id,
			replay_eligibility: broadcast_replay_eligibility(strategy_intent),
			left: Box::new(left_plan.plan),
			right: Box::new(right_plan.plan),
			kind: physical_kind,
			left_keys,
			right_keys,
			match_filter,
			null_safe_keys,
			strategy: decision.strategy,
			output_schema: physical_output_schema.clone(),
		};
		let output_facts = if matches!(
			decision.reason,
			JoinStrategyDecisionReason::ScalarLocalCross
		) {
			PlanOptimizerFacts::composite_with_row(Some(RowCount::new(1)))
		} else {
			PlanOptimizerFacts::composite_with_join_estimate(
				join_estimated_row_count,
				declared_output_schema.as_ref(),
			)
		};
		let join_order_sources = merge_join_order_sources(
			left_plan.join_order_sources.clone(),
			right_plan.join_order_sources.clone(),
		);
		let plan = match decision.placement {
			JoinPhysicalPlacement::Natural => join_plan,
			JoinPhysicalPlacement::SwappedRightBuild
				if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) =>
			{
				if physical_output_schema.as_ref() != declared_output_schema.as_ref() {
					return Err(BindingError::UnsupportedExpression {
						message: "right-build one-sided broadcast join schema mismatch".to_string(),
					});
				}
				join_plan
			}
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

fn ctas_intent_fingerprint(
	table: &TableRef,
	request_shape: &str,
	result_schema: &SchemaRef,
	session_timezone: &str,
	allow_experimental_kernels: bool,
) -> wendelon_client::CtasIntentFingerprint {
	fn field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
		hasher.update(&(bytes.len() as u64).to_be_bytes());
		hasher.update(bytes);
	}
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"toron.ctas.intent.v1\0");
	field(
		&mut hasher,
		table.schema.as_deref().unwrap_or("default").as_bytes(),
	);
	field(&mut hasher, table.table.as_bytes());
	field(&mut hasher, request_shape.as_bytes());
	let encoded_schema = IpcDataGenerator {}
		.schema_to_bytes_with_dictionary_tracker(
			result_schema,
			&mut DictionaryTracker::new(false),
			&IpcWriteOptions::default(),
		)
		.ipc_message;
	field(&mut hasher, &encoded_schema);
	field(&mut hasher, session_timezone.as_bytes());
	hasher.update(&[u8::from(allow_experimental_kernels)]);
	wendelon_client::CtasIntentFingerprint::new(*hasher.finalize().as_bytes())
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

fn is_supported_density_group_type(data_type: &DataType) -> bool {
	match data_type {
		DataType::Boolean
		| DataType::Int32
		| DataType::Int64
		| DataType::Date32
		| DataType::Float32
		| DataType::Float64
		| DataType::Utf8 => true,
		DataType::Timestamp(_, Some(timezone)) => timezone.as_ref() == "UTC",
		_ => false,
	}
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

/// ADR 244: a RANGE frame's value offsets must be arithmetic-compatible with the single ORDER BY key
/// (numeric key ↔ numeric offset; temporal key ↔ interval offset). Peer/`UNBOUNDED` bounds carry no
/// offset and bind on any key, so only `Preceding`/`Following` value deltas are checked here.
fn validate_range_frame_offsets(
	frame: &WindowFrame,
	order_by: &[ExecutionSortKey],
	input_schema: &SchemaRef,
) -> Result<(), BindingError> {
	if frame.unit != WindowFrameUnit::Range {
		return Ok(());
	}
	for bound in [&frame.start, &frame.end] {
		let offset = match bound {
			WindowFrameBound::Preceding(WindowFrameDelta::Range(offset))
			| WindowFrameBound::Following(WindowFrameDelta::Range(offset)) => offset,
			_ => continue,
		};
		if order_by.len() != 1 {
			return Err(BindingError::UnsupportedExpression {
				message: "RANGE value offsets require exactly one ORDER BY column".to_string(),
			});
		}
		let key_type = input_schema
			.field(order_by[0].ordinal.0 as usize)
			.data_type();
		if !range_offset_compatible(key_type, offset) {
			return Err(BindingError::UnsupportedExpression {
				message: format!(
					"RANGE offset of type {:?} is not compatible with ORDER BY key of type {key_type:?}",
					offset.data_type()
				),
			});
		}
	}
	Ok(())
}

fn range_offset_compatible(key_type: &DataType, offset: &ScalarValue) -> bool {
	let offset_type = offset.data_type();
	let is_numeric = |data_type: &DataType| {
		matches!(
			data_type,
			DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
		)
	};
	if is_numeric(key_type) && is_numeric(&offset_type) {
		return true;
	}
	let temporal_key = matches!(key_type, DataType::Timestamp(_, _) | DataType::Date32);
	let interval_offset = matches!(offset_type, DataType::Duration(_) | DataType::Interval(_));
	temporal_key && interval_offset
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
		local_table_id: binding.local_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		projection,
		pbc_program: None,
		row_limit: None,
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		estimated_byte_size: None,
		primary_key_columns: sorted_columns(&binding.primary_key_columns),
		column_ndv,
		column_range,
		column_range_distributions,
		column_heavy_hitters,
		column_group_top_k,
		column_group_ndv,
	}
}

fn set_scan_estimated_byte_size(plan: &mut ExecutionPlan, estimated: Option<ByteCount>) {
	if let ExecutionPlan::Scan {
		estimated_byte_size,
		..
	} = plan
	{
		*estimated_byte_size = estimated.map(ByteCount::get);
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
		// A projection emits exactly one row per input row, so a bound above it is the same
		// bound below it. `LIMIT 0` is excluded: it has its own representation and its own
		// handling of `Some(0)`, and must not share a path with a real bound.
		ExecutionPlan::Project { input, .. } if limit > 0 => {
			apply_safe_row_limit_to_scan(input, limit)
		}
		// Closed positive list: every cardinality-changing node (Aggregate, Sort, Window,
		// Join, Exchange, Union) is refused here, so one added later cannot inherit
		// pushdown by failing to be excluded.
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
		ExecutionPlan::Range { count, schema } => {
			let all_columns = all_columns(&schema);
			let required = required_output.unwrap_or(all_columns);
			let requested = required.iter().copied().collect::<Vec<_>>();
			let remap = ordinal_remap(&requested);
			let schema = project_schema(&schema, &requested)?;
			Ok((ExecutionPlan::Range { count, schema }, remap))
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
			local_table_id,
			table,
			fingerprint,
			schema,
			projection: _,
			pbc_program,
			row_limit,
			row_identity,
			profile_hint,
			estimated_byte_size,
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
					local_table_id,
					table,
					fingerprint,
					schema,
					projection: Some(requested),
					pbc_program,
					row_limit,
					row_identity,
					profile_hint,
					estimated_byte_size,
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
			let preserve_outer_count_contract = is_outer_count_contract_project(&exprs);
			let requested_outputs =
				required_output.unwrap_or_else(|| output_columns(exprs.root_count()));
			let requested_outputs_vec = requested_outputs.iter().copied().collect::<Vec<_>>();
			let exprs = project_program_roots(&exprs, &requested_outputs_vec)?;
			let mut child_required = BTreeSet::new();
			collect_program_columns(&exprs, &mut child_required);
			let (input, remap) =
				optimize_for_required(*input, Some(child_required), logical_types)?;
			let exprs = rewrite_program_ordinals(exprs, &remap)?;
			let output_schema = if preserve_outer_count_contract {
				project_schema(&output_schema, &requested_outputs_vec)?
			} else {
				schema_for_projected_exprs_with_existing_names(
					&exprs,
					&output_schema,
					&requested_outputs_vec,
				)
			};
			let output_remap = ordinal_remap(&requested_outputs_vec);

			Ok((
				collapse_adjacent_identity_project(input, exprs, output_schema),
				output_remap,
			))
		}
		ExecutionPlan::Union {
			left,
			right,
			output_schema,
		} => {
			let requested_outputs = required_output.unwrap_or_else(|| all_columns(&output_schema));
			let requested_outputs_vec = requested_outputs.iter().copied().collect::<Vec<_>>();
			let (left, _) =
				optimize_for_required(*left, Some(requested_outputs.clone()), logical_types)?;
			let (right, _) =
				optimize_for_required(*right, Some(requested_outputs.clone()), logical_types)?;
			let output_schema = project_schema(&output_schema, &requested_outputs_vec)?;
			let output_remap = ordinal_remap(&requested_outputs_vec);
			Ok((
				ExecutionPlan::Union {
					left: Box::new(left),
					right: Box::new(right),
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Repeat {
			input,
			count_column,
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
				ExecutionPlan::Repeat {
					input: Box::new(input),
					count_column,
					output_schema,
				},
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
		ExecutionPlan::KernelDensity { input, spec } => {
			let (input, _) = optimize_for_required(*input, None, logical_types)?;
			let output_remap = ordinal_remap(
				&output_columns(spec.output_schema.fields().len())
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::KernelDensity {
					input: Box::new(input),
					spec,
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
		ExecutionPlan::Window {
			input,
			partition_by,
			order_by,
			functions,
			output_schema,
		} => {
			// A window is a pruning barrier: it reads its partition/order columns and appends rank
			// columns, so the input keeps all columns and the output is identity-remapped.
			let output_len = output_schema.fields().len();
			let (input, _) = optimize_for_required(*input, None, logical_types)?;
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::Window {
					input: Box::new(input),
					partition_by,
					order_by,
					functions,
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
			match_filter,
			null_safe_keys,
			strategy,
			output_schema,
		} => {
			let optimized = optimize_join_inputs_for_required(
				kind,
				*left,
				*right,
				left_keys,
				right_keys,
				match_filter,
				&output_schema,
				required_output,
				logical_types,
			)?;
			Ok((
				ExecutionPlan::Join {
					replay_id,
					replay_eligibility,
					left: Box::new(optimized.left),
					right: Box::new(optimized.right),
					kind,
					left_keys: optimized.left_keys,
					right_keys: optimized.right_keys,
					match_filter: optimized.match_filter,
					null_safe_keys,
					strategy,
					output_schema: optimized.output_schema,
				},
				optimized.output_remap,
			))
		}
		// ADR 255: a recursive CTE is a projection barrier — the working-table fixpoint feeds every
		// `output_schema` column back into the recursive term, so no outer requirement may be pushed
		// inward. Recurse into both subplans with `None` (nested scans still get their own pushdown),
		// keep the full output schema, and return an identity remap (like Join/Repeat).
		ExecutionPlan::RecursiveCte {
			seed,
			recursive,
			output_schema,
		} => {
			let (seed, _) = optimize_for_required(*seed, None, logical_types)?;
			let (recursive, _) = optimize_for_required(*recursive, None, logical_types)?;
			let output_len = output_schema.fields().len();
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::RecursiveCte {
					seed: Box::new(seed),
					recursive: Box::new(recursive),
					output_schema,
				},
				output_remap,
			))
		}
		// ADR 255: the self-reference leaf — a working-table placeholder. Identity over its full schema;
		// the executor substitutes a `SystemTable` of all columns each iteration, so no projection may be
		// pushed in.
		ExecutionPlan::RecursiveRef { output_schema } => {
			let output_len = output_schema.fields().len();
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((ExecutionPlan::RecursiveRef { output_schema }, output_remap))
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
			intent_fingerprint,
			if_not_exists,
			or_replace,
			source,
			materialization_certificate,
		} => {
			let (source, _) = optimize_for_required(*source, None, logical_types)?;
			Ok((
				ExecutionPlan::CreateTableAsSelect {
					table,
					schema,
					intent_fingerprint,
					if_not_exists,
					or_replace,
					source: Box::new(source),
					materialization_certificate,
				},
				OrdinalRemap::new(),
			))
		}
		ExecutionPlan::DropTable { name, if_exists } => Ok((
			ExecutionPlan::DropTable { name, if_exists },
			OrdinalRemap::new(),
		)),
		ExecutionPlan::Delete {
			local_table_id,
			table,
			fingerprint,
			schema,
			predicate,
			pbc_program,
			profile_hint,
			returning,
		} => Ok((
			ExecutionPlan::Delete {
				local_table_id,
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
			local_table_id,
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
					local_table_id,
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
			local_table_id,
			table,
			fingerprint,
			schema,
			rows,
			returning,
		} => Ok((
			ExecutionPlan::Insert {
				local_table_id,
				table,
				fingerprint,
				schema,
				rows,
				returning,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::InsertSelect {
			local_table_id,
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
					local_table_id,
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
		| Some(BoundExprNode::SimilarTo { expr: input, .. }) => {
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
		Some(BoundExprNode::ScalarFunction { args, .. }) => {
			for arg in args {
				collect_program_node_columns(program, *arg, columns);
			}
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
		Expr::Cast(inner, target) => {
			cast::bind_explicit_cast(builder, input_schema, ctx, inner, target)
		}
		Expr::TryCast(inner, target) => {
			cast::bind_try_cast(builder, input_schema, ctx, inner, target)
		}
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
		Expr::SimilarTo {
			expr,
			pattern,
			escape,
			negated,
		} => bind_similar_to(builder, input_schema, ctx, expr, pattern, *escape, *negated),
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
		Expr::ScalarFunction {
			id,
			args,
			parameter,
		} => bind_scalar_function(builder, input_schema, ctx, *id, args, parameter),
		Expr::IsDistinctFrom {
			left,
			right,
			negated,
		} => bind_is_distinct_from(builder, input_schema, ctx, left, right, *negated),
	}
}

/// ADR 566: the null-safe comparison binds as a composition over operands
/// bound exactly once. The equality node comes from `bind_comparison`, so
/// coercion, nominal checks, and the dimensioned path are inherited rather
/// than mirrored; the operand ids are read back from that node and shared by
/// every reference — the program's memoized slots make sharing single
/// evaluation by construction. Under Kleene booleans every conjunct is
/// two-valued in every row, so the result is never NULL.
fn bind_is_distinct_from(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	left: &Expr,
	right: &Expr,
	negated: bool,
) -> Result<BoundExprId, BindingError> {
	let equality = bind_comparison(builder, input_schema, ctx, left, right, BoundBinaryOp::Eq)?;
	let Some(BoundExprNode::Binary {
		left: left_id,
		right: right_id,
		..
	}) = builder.node(equality)
	else {
		return Err(BindingError::UnsupportedExpression {
			message: "IS DISTINCT FROM equality bound to an unexpected node shape".to_string(),
		});
	};
	let (left_id, right_id) = (*left_id, *right_id);
	let left_null = builder.append_unary(BoundUnaryOp::IsNull, left_id, DataType::Boolean);
	let right_null = builder.append_unary(BoundUnaryOp::IsNull, right_id, DataType::Boolean);
	let both_null =
		builder.append_binary(BoundBinaryOp::And, left_null, right_null, DataType::Boolean);
	let left_present = builder.append_unary(BoundUnaryOp::IsNotNull, left_id, DataType::Boolean);
	let right_present = builder.append_unary(BoundUnaryOp::IsNotNull, right_id, DataType::Boolean);
	let both_present = builder.append_binary(
		BoundBinaryOp::And,
		left_present,
		right_present,
		DataType::Boolean,
	);
	let present_and_equal = builder.append_binary(
		BoundBinaryOp::And,
		both_present,
		equality,
		DataType::Boolean,
	);
	let not_distinct = builder.append_binary(
		BoundBinaryOp::Or,
		both_null,
		present_and_equal,
		DataType::Boolean,
	);
	if negated {
		return Ok(not_distinct);
	}
	Ok(builder.append_unary(BoundUnaryOp::Not, not_distinct, DataType::Boolean))
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

	// A numeric-literal CASE branch coerces to a single Decimal branch type so the branches unify.
	coerce_case_decimal_literals(builder, ctx, &typed_result_ids)?;
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

fn coerce_decimal_literal_to_legacy_explicit_numeric_source(
	builder: &mut BoundExprProgramBuilder,
	id: BoundExprId,
	target: &DataType,
	session_timezone: Tz,
) -> Result<(), BindingError> {
	if !is_supported_explicit_numeric_cast(&DataType::Float64, target) {
		return Ok(());
	}
	let Some(value @ ScalarValue::Decimal128 { .. }) = builder.literal(id).cloned() else {
		return Ok(());
	};
	let value = coerce_literal_value(value, &DataType::Float64, session_timezone)?;
	let _ = builder.rewrite_literal(id, value);
	Ok(())
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
	let mut left = bind_expr_into_program(builder, left, input_schema, ctx)?;
	let mut right = bind_expr_into_program(builder, right, input_schema, ctx)?;
	coerce_comparison_literals(builder, left, right, ctx.session_timezone)?;
	arithmetic::reconcile_decimal_comparison(builder, &mut left, &mut right)?;
	arithmetic::reconcile_numeric_comparison(builder, &mut left, &mut right)?;
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

/// ADR 570: the pattern compiles here, so a malformed one fails the statement
/// once, before execution; the executor recompiles the already-validated text.
fn bind_similar_to(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	expr: &Expr,
	pattern: &str,
	escape: Option<char>,
	negated: bool,
) -> Result<BoundExprId, BindingError> {
	let expr = bind_expr_into_program(builder, expr, input_schema, ctx)?;
	let expr_type = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed SIMILAR TO input");
	if expr_type != DataType::Utf8 {
		return Err(BindingError::TypeMismatch {
			message: format!("SIMILAR TO expects Utf8 input, got {expr_type:?}"),
		});
	}
	crate::similar_to::compile_similar_to(pattern, escape).map_err(|message| {
		BindingError::UnsupportedExpression {
			message: format!("SIMILAR TO pattern error: {message}"),
		}
	})?;
	Ok(builder.append_similar_to(expr, pattern.to_string(), escape, negated))
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
		// ADR 263: coerce an Int/Float/Decimal literal to a Decimal column's exact `(p,s)` so the
		// comparison's operands match (Arrow cmp requires identical precision/scale). Approach 1 — the
		// literal is rewritten in place (no Cast node); Float→Decimal rounds (a known reachability
		// simplification, not answer-exactness).
		(ScalarValue::Int32(value), DataType::Decimal128(precision, scale)) => {
			decimal_literal_from_i128(value.map(i128::from), *precision, *scale)?
		}
		(ScalarValue::Int64(value), DataType::Decimal128(precision, scale)) => {
			decimal_literal_from_i128(value.map(i128::from), *precision, *scale)?
		}
		(ScalarValue::Float32(value), DataType::Decimal128(precision, scale)) => {
			decimal_literal_from_f64(value.map(f64::from), *precision, *scale)?
		}
		(ScalarValue::Float64(value), DataType::Decimal128(precision, scale)) => {
			decimal_literal_from_f64(value, *precision, *scale)?
		}
		(ScalarValue::Decimal128 { value, scale, .. }, DataType::Float32) => {
			ScalarValue::Float32(decimal_literal_to_f32(value, scale)?)
		}
		(ScalarValue::Decimal128 { value, scale, .. }, DataType::Float64) => {
			ScalarValue::Float64(decimal_literal_to_f64(value, scale)?)
		}
		(
			ScalarValue::Decimal128 {
				value,
				scale: from_scale,
				..
			},
			DataType::Decimal128(precision, scale),
		) => decimal_rescale(value, from_scale, *precision, *scale)?,
		(value, _) => value,
	})
}

/// ADR 263: the `10^scale` factor for a `Decimal128(_, scale)` literal. Rejects negative scale (out of
/// the Q6 slice) and any factor that would overflow `i128` — a clean bind error, never a panic.
fn decimal_scale_factor(precision: u8, scale: i8) -> Result<i128, BindingError> {
	let exponent = u32::try_from(scale).map_err(|_| decimal_out_of_range(precision, scale))?;
	10i128
		.checked_pow(exponent)
		.ok_or_else(|| decimal_out_of_range(precision, scale))
}

/// ADR 263: a clean bind error for a literal that does not fit a `Decimal128(precision, scale)`.
fn decimal_out_of_range(precision: u8, scale: i8) -> BindingError {
	BindingError::TypeMismatch {
		message: format!("literal does not fit Decimal128({precision}, {scale})"),
	}
}

/// ADR 263: wrap a raw `i128` into a `Decimal128(precision, scale)` literal, validating it fits the
/// declared precision (`|raw| < 10^precision`) so an oversized literal is a clean bind error rather than
/// a wrong value or a downstream panic at execute.
fn decimal_literal_checked(
	value: Option<i128>,
	precision: u8,
	scale: i8,
) -> Result<ScalarValue, BindingError> {
	if let Some(raw) = value {
		let bound = 10i128
			.checked_pow(u32::from(precision))
			.ok_or_else(|| decimal_out_of_range(precision, scale))?;
		if raw <= -bound || raw >= bound {
			return Err(decimal_out_of_range(precision, scale));
		}
	}
	Ok(ScalarValue::Decimal128 {
		value,
		precision,
		scale,
	})
}

/// ADR 263: an integer literal as a `Decimal128(precision, scale)` (value scaled by `10^scale`).
fn decimal_literal_from_i128(
	value: Option<i128>,
	precision: u8,
	scale: i8,
) -> Result<ScalarValue, BindingError> {
	let factor = decimal_scale_factor(precision, scale)?;
	let value = value
		.map(|value| {
			value
				.checked_mul(factor)
				.ok_or_else(|| decimal_out_of_range(precision, scale))
		})
		.transpose()?;
	decimal_literal_checked(value, precision, scale)
}

/// ADR 263: a float literal as a `Decimal128(precision, scale)`, rounding at the target scale. Non-finite
/// floats and out-of-range magnitudes reject at bind. Float→Decimal rounding is a known reachability
/// simplification, not answer-exactness.
fn decimal_literal_from_f64(
	value: Option<f64>,
	precision: u8,
	scale: i8,
) -> Result<ScalarValue, BindingError> {
	let factor = decimal_scale_factor(precision, scale)? as f64;
	let value = value
		.map(|value| {
			if !value.is_finite() {
				return Err(decimal_out_of_range(precision, scale));
			}
			let scaled = (value * factor).round();
			if scaled < i128::MIN as f64 || scaled > i128::MAX as f64 {
				return Err(decimal_out_of_range(precision, scale));
			}
			Ok(scaled as i128)
		})
		.transpose()?;
	decimal_literal_checked(value, precision, scale)
}

fn decimal_literal_to_f64(value: Option<i128>, scale: i8) -> Result<Option<f64>, BindingError> {
	let Some(value) = value else {
		return Ok(None);
	};
	let scaled = value as f64 / 10_f64.powi(i32::from(scale));
	if !scaled.is_finite() {
		return Err(BindingError::TypeMismatch {
			message: format!("literal does not fit Float64: Decimal128(_, {scale})"),
		});
	}
	Ok(Some(scaled))
}

fn decimal_literal_to_f32(value: Option<i128>, scale: i8) -> Result<Option<f32>, BindingError> {
	let value = decimal_literal_to_f64(value, scale)?;
	let value = value.map(|value| value as f32);
	if value.is_some_and(|value| !value.is_finite()) {
		return Err(BindingError::TypeMismatch {
			message: format!("literal does not fit Float32: Decimal128(_, {scale})"),
		});
	}
	Ok(value)
}

/// ADR 263: rescale a `Decimal128` literal from `from_scale` to `(precision, scale)`.
fn decimal_rescale(
	value: Option<i128>,
	from_scale: i8,
	precision: u8,
	scale: i8,
) -> Result<ScalarValue, BindingError> {
	let diff = i32::from(scale) - i32::from(from_scale);
	let value = value
		.map(|value| {
			let factor = 10i128
				.checked_pow(diff.unsigned_abs())
				.ok_or_else(|| decimal_out_of_range(precision, scale))?;
			if diff >= 0 {
				value
					.checked_mul(factor)
					.ok_or_else(|| decimal_out_of_range(precision, scale))
			} else {
				Ok(value / factor)
			}
		})
		.transpose()?;
	decimal_literal_checked(value, precision, scale)
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
