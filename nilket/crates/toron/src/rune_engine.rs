use crate::catalog::TableRef;
use crate::date32::parse_date_ymd;
use crate::error::{FrontendError, RuneDiagnosticSpan};
use crate::ir::{
	AggregateFunction, BroadcastBuildSide, CUSTOMER_ID_LOGICAL_TYPE, Expr, JoinKind, JoinStrategy,
	JoinStrategyIntent, JoinStrategyPinSource, KernelDensitySpec, LogicalPlan, LogicalTypeId,
	PRODUCT_ID_LOGICAL_TYPE, ScalarFunctionId, ScalarFunctionParameter, ScalarValue, SortExpr,
	TemporalExtractField, TemporalTruncField, ValueFunction, WindowFrame, WindowFrameBound,
	WindowFrameDelta, WindowFrameUnit, WindowFunction, WindowFunctionCall,
};
use crate::sql_planner::{
	SqlPlanner, build_join_plan, expr_to_join_keys, window_function_default_name,
	window_over_exchange,
};
use arrow::datatypes::{DataType, TimeUnit};
use rune::alloc::limit;
use rune::ast::Spanned;
use rune::diagnostics::{Diagnostic, FatalDiagnosticKind};
use rune::runtime::{TypeHash, VmResult, budget};
use rune::{Any, Context, Diagnostics, Module, Source, Sources, Unit, Vm};
use std::sync::Arc;
use std::time::Duration;

const RUNE_MAX_IN_LIST_ITEMS: usize = 32;
const RUNE_INFORMATION_SCHEMA_NAMESPACE: &str = "information_schema";
const RUNE_OPTIMIZER_DECISIONS_TABLE: &str = "optimizer_decisions";
const RUNE_OPTIMIZER_RUNTIME_VERDICTS_TABLE: &str = "optimizer_runtime_verdicts";
const RUNE_OPTIMIZER_EVIDENCE_STATUS_TABLE: &str = "optimizer_evidence_status";
const RUNE_OPTIMIZER_MARGIN_ADVISORY_TABLE: &str = "optimizer_margin_advisory";
const RUNE_OPTIMIZER_RECEIPT_GATES_TABLE: &str = "optimizer_receipt_gates";
const RUNE_OPTIMIZER_DECISION_CONFLICTS_TABLE: &str = "optimizer_decision_conflicts";
const RUNE_OPTIMIZER_MEMO_ADMISSION_TABLE: &str = "optimizer_memo_admission";
const RUNE_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE: &str = "optimizer_probe_linearity_fences";
const RUNE_OPTIMIZER_READOUT_TABLES: [&str; 8] = [
	RUNE_OPTIMIZER_DECISIONS_TABLE,
	RUNE_OPTIMIZER_RUNTIME_VERDICTS_TABLE,
	RUNE_OPTIMIZER_EVIDENCE_STATUS_TABLE,
	RUNE_OPTIMIZER_MARGIN_ADVISORY_TABLE,
	RUNE_OPTIMIZER_RECEIPT_GATES_TABLE,
	RUNE_OPTIMIZER_DECISION_CONFLICTS_TABLE,
	RUNE_OPTIMIZER_MEMO_ADMISSION_TABLE,
	RUNE_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE,
];

fn rune_compile_error(error: impl std::fmt::Display) -> FrontendError {
	FrontendError::RuneCompile {
		message: error.to_string(),
		span: None,
	}
}

fn rune_runtime_error(error: impl std::fmt::Display) -> FrontendError {
	FrontendError::RuneRuntime {
		message: error.to_string(),
		span: None,
	}
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AllowProfile {
	#[default]
	ToronAuthoring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuneSandboxConfig {
	pub fuel_budget: u64,
	pub memory_budget: u64,
	pub wall_timeout: Duration,
	pub max_source_bytes: usize,
	pub allow_profile: AllowProfile,
}

impl RuneSandboxConfig {
	pub fn v1_default() -> Self {
		Self {
			fuel_budget: 1_000_000,
			memory_budget: 16 * 1024 * 1024,
			wall_timeout: Duration::from_secs(5),
			max_source_bytes: 64 * 1024,
			allow_profile: AllowProfile::ToronAuthoring,
		}
	}

	pub fn with_fuel(fuel_budget: u64) -> Self {
		Self {
			fuel_budget,
			..Self::v1_default()
		}
	}
}

impl Default for RuneSandboxConfig {
	fn default() -> Self {
		Self::v1_default()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityCategory {
	Filesystem,
	Process,
	Network,
	Environment,
	Time,
	Random,
	Threading,
	Unknown,
}

impl CapabilityCategory {
	pub fn label(self) -> &'static str {
		match self {
			Self::Filesystem => "Filesystem",
			Self::Process => "Process",
			Self::Network => "Network",
			Self::Environment => "Environment",
			Self::Time => "Time",
			Self::Random => "Random",
			Self::Threading => "Threading",
			Self::Unknown => "Unknown",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuneSandboxViolation {
	FuelExhausted {
		fuel_consumed: u64,
		fuel_budget: u64,
	},
	MemoryExhausted {
		bytes_used: u64,
		bytes_budget: u64,
	},
	CapabilityDenied {
		capability: CapabilityCategory,
	},
	SourceTooLarge {
		bytes: usize,
		budget: usize,
	},
	Timeout {
		elapsed: Duration,
	},
	InvalidEntrypoint {
		detail: String,
	},
	InternalSandboxError {
		detail: String,
	},
}

impl RuneSandboxViolation {
	pub fn stable_label(&self) -> &'static str {
		match self {
			Self::FuelExhausted { .. } => "FuelExhausted",
			Self::MemoryExhausted { .. } => "MemoryExhausted",
			Self::CapabilityDenied { .. } => "CapabilityDenied",
			Self::SourceTooLarge { .. } => "SourceTooLarge",
			Self::Timeout { .. } => "Timeout",
			Self::InvalidEntrypoint { .. } => "InvalidEntrypoint",
			Self::InternalSandboxError { .. } => "InternalSandboxError",
		}
	}

	fn public_message(&self) -> String {
		match self {
			Self::FuelExhausted { fuel_budget, .. } => {
				format!("Rune fuel budget exhausted (budget: {fuel_budget})")
			}
			Self::MemoryExhausted { bytes_budget, .. } => {
				format!("Rune memory budget exhausted (budget: {bytes_budget} bytes)")
			}
			Self::CapabilityDenied { capability } => {
				format!("Rune capability denied: {}", capability.label())
			}
			Self::SourceTooLarge { bytes, budget } => {
				format!("Rune source too large ({bytes} bytes, budget: {budget} bytes)")
			}
			Self::Timeout { elapsed } => {
				format!("Rune execution exceeded wall timeout ({elapsed:?})")
			}
			Self::InvalidEntrypoint { detail } => {
				format!("Rune entrypoint rejected: {detail}")
			}
			Self::InternalSandboxError { detail } => {
				format!("Rune sandbox internal error: {detail}")
			}
		}
	}
}

fn rune_sandbox_error(kind: RuneSandboxViolation) -> FrontendError {
	let message = kind.public_message();
	FrontendError::RuneSandbox { kind, message }
}

#[derive(Any, Debug, Clone)]
pub struct RuneDataFrame {
	pub(crate) plan: LogicalPlan,
}

#[derive(Any, Debug, Clone)]
pub struct RuneExpr {
	pub(crate) expr: Expr,
}

#[derive(Any, Debug, Clone)]
pub struct RuneNamedExpr {
	pub(crate) expr: Expr,
	pub(crate) name: String,
}

#[derive(Any, Debug, Clone)]
pub struct RuneSortKey {
	pub(crate) expr: Expr,
	pub(crate) descending: bool,
	pub(crate) nulls_first: bool,
}

#[derive(Any, Debug, Clone)]
pub struct RuneWhenThen {
	pub(crate) condition: Expr,
	pub(crate) result: Expr,
}

#[derive(Any, Debug, Clone)]
pub struct RuneDataType {
	pub(crate) data_type: DataType,
}

#[derive(Any, Debug, Clone, Copy)]
pub enum CustomerId {}

#[derive(Any, Debug, Clone, Copy)]
pub enum ProductId {}

impl RuneDataFrame {
	pub fn new(table: String) -> Self {
		Self {
			plan: LogicalPlan::ScanNamed {
				table: TableRef::new(table),
				projection: None,
			},
		}
	}

	pub fn alias(self, name: String) -> Self {
		Self {
			plan: LogicalPlan::Alias {
				input: Box::new(self.plan),
				alias: TableRef::new(name),
				columns: Vec::new(),
			},
		}
	}

	pub fn join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::Inner, on)
	}

	pub fn inner_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::Inner, on)
	}

	pub fn auto_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		let pairs = match expr_to_join_keys(&on.expr) {
			Ok(pairs) => pairs,
			Err(error) => return VmResult::panic(error.to_string()),
		};
		VmResult::Ok(Self {
			plan: LogicalPlan::Join {
				left: Box::new(self.plan),
				right: Box::new(right.plan),
				kind: JoinKind::Inner,
				on: pairs,
				on_filter: None,
				null_safe_keys: false,
				strategy_intent: JoinStrategyIntent::Auto,
			},
		})
	}

	/// Broadcast-join `self` as the left/build side and probe `right`.
	/// INNER-only. `small.broadcast_join(big, on)` broadcasts `small`.
	pub fn broadcast_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_strategy(
			self.plan,
			right.plan,
			JoinKind::Inner,
			on,
			JoinStrategy::Broadcast,
			Some(BroadcastBuildSide::Left),
		)
	}

	pub fn broadcast_build(self, side: String) -> VmResult<Self> {
		let Some(build_side) = parse_broadcast_build_side(&side) else {
			return VmResult::panic(
				"broadcast_build expects build side `left` or `right`".to_string(),
			);
		};
		match pin_broadcast_build(self.plan, build_side) {
			Ok(plan) => VmResult::Ok(Self { plan }),
			Err(error) => VmResult::panic(error),
		}
	}

	pub fn explain(self) -> Self {
		Self {
			plan: LogicalPlan::Explain {
				input: Box::new(self.plan),
			},
		}
	}

	pub fn analyze(self) -> Self {
		Self {
			plan: LogicalPlan::ExplainAnalyze {
				input: Box::new(self.plan),
			},
		}
	}

	pub fn left_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::LeftOuter, on)
	}

	pub fn right_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::RightOuter, on)
	}

	pub fn full_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::FullOuter, on)
	}

	pub fn left_semi_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::LeftSemi, on)
	}

	pub fn right_semi_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::RightSemi, on)
	}

	pub fn left_anti_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::LeftAnti, on)
	}

	pub fn right_anti_join(self, right: RuneDataFrame, on: RuneExpr) -> VmResult<Self> {
		join_with_kind(self.plan, right.plan, JoinKind::RightAnti, on)
	}

	pub fn filter(self, predicate: RuneExpr) -> Self {
		Self {
			plan: LogicalPlan::Filter {
				input: Box::new(self.plan),
				predicate: predicate.expr,
			},
		}
	}

	pub fn project(self, exprs: Vec<RuneExpr>) -> Self {
		let exprs = exprs.into_iter().map(|e| e.expr).collect::<Vec<_>>();
		Self {
			plan: LogicalPlan::Project {
				input: Box::new(self.plan),
				output_names: vec![None; exprs.len()],
				exprs,
			},
		}
	}

	pub fn project_named(self, exprs: Vec<RuneNamedExpr>) -> Self {
		let mut plan_exprs = Vec::with_capacity(exprs.len());
		let mut output_names = Vec::with_capacity(exprs.len());
		for named in exprs {
			plan_exprs.push(named.expr);
			output_names.push(Some(named.name));
		}
		Self {
			plan: LogicalPlan::Project {
				input: Box::new(self.plan),
				exprs: plan_exprs,
				output_names,
			},
		}
	}

	pub fn distinct(self) -> Self {
		Self {
			plan: LogicalPlan::Distinct {
				input: Box::new(self.plan),
			},
		}
	}

	pub fn aggregate(self, group_expr: Vec<RuneExpr>, aggr_expr: Vec<RuneAggr>) -> Self {
		Self {
			plan: LogicalPlan::Aggregate {
				input: Box::new(self.plan),
				group_expr: group_expr.into_iter().map(|e| e.expr).collect(),
				aggr_expr: aggr_expr.into_iter().map(|a| (a.func, a.args)).collect(),
			},
		}
	}

	/// ADR 552: compute a shared-grid Gaussian density relation. `points` is authored by the
	/// caller; the engine owns the bandwidth and degenerate-group policy.
	pub fn kernel_density_default(
		self,
		group_expr: RuneExpr,
		value_expr: RuneExpr,
	) -> VmResult<Self> {
		self.kernel_density(group_expr, value_expr, 128)
	}

	pub fn kernel_density(
		self,
		group_expr: RuneExpr,
		value_expr: RuneExpr,
		points: i64,
	) -> VmResult<Self> {
		let Some(points) = non_negative_usize_arg(points).filter(|points| *points >= 2) else {
			return VmResult::panic("kernel_density points must be at least 2");
		};
		VmResult::Ok(Self {
			plan: LogicalPlan::KernelDensity {
				input: Box::new(self.plan),
				spec: Box::new(KernelDensitySpec {
					group_expr: group_expr.expr,
					value_expr: value_expr.expr,
					points,
				}),
			},
		})
	}

	pub fn sort(self, order_by: Vec<RuneSortKey>) -> VmResult<Self> {
		if order_by.is_empty() {
			return VmResult::panic("ORDER BY requires at least one expression");
		}
		let order_by = order_by
			.into_iter()
			.map(|key| SortExpr {
				expr: key.expr,
				descending: key.descending,
				nulls_first: key.nulls_first,
			})
			.collect();
		VmResult::Ok(Self {
			plan: LogicalPlan::Sort {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(self.plan),
					partition_count: 1,
					key: Vec::new(),
				}),
				order_by,
				limit: None,
				offset: 0,
			},
		})
	}

	pub fn limit(self, limit: i64) -> VmResult<Self> {
		let Some(limit) = non_negative_usize_arg(limit) else {
			return VmResult::panic("LIMIT/OFFSET expects a non-negative integer");
		};
		match self.plan {
			LogicalPlan::Sort {
				input,
				order_by,
				offset,
				..
			} => VmResult::Ok(Self {
				plan: LogicalPlan::Sort {
					input,
					order_by,
					limit: Some(limit),
					offset,
				},
			}),
			_ => VmResult::panic("LIMIT/OFFSET without ORDER BY is not supported"),
		}
	}

	pub fn take(self, limit: i64) -> VmResult<Self> {
		let Some(limit) = non_negative_usize_arg(limit) else {
			return VmResult::panic("LIMIT/OFFSET expects a non-negative integer");
		};
		VmResult::Ok(Self {
			plan: LogicalPlan::Take {
				input: Box::new(self.plan),
				limit,
			},
		})
	}

	pub fn offset(self, offset: i64) -> VmResult<Self> {
		let Some(offset) = non_negative_usize_arg(offset) else {
			return VmResult::panic("LIMIT/OFFSET expects a non-negative integer");
		};
		match self.plan {
			LogicalPlan::Sort {
				input,
				order_by,
				limit,
				..
			} => VmResult::Ok(Self {
				plan: LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				},
			}),
			_ => VmResult::panic("LIMIT/OFFSET without ORDER BY is not supported"),
		}
	}

	pub fn count(self) -> Self {
		Self {
			plan: LogicalPlan::Aggregate {
				input: Box::new(self.plan),
				group_expr: vec![],
				aggr_expr: vec![(AggregateFunction::CountAll, vec![Expr::column("count")])],
			},
		}
	}

	/// ADR 251: append a window node computing `calls` over `PARTITION BY partition_by ORDER BY
	/// order_by`. Routes through `window_over_exchange` so the Rune-built plan is byte-identical to the
	/// SQL one. Empty PARTITION BY / empty ORDER BY are both valid (a single-partition / unordered
	/// window); only an empty `calls` list is rejected.
	pub fn window(
		self,
		partition_by: Vec<RuneExpr>,
		order_by: Vec<RuneSortKey>,
		calls: Vec<WindowFunctionCall>,
	) -> VmResult<Self> {
		if calls.is_empty() {
			return VmResult::panic("window requires at least one window function");
		}
		let partition_exprs = partition_by.into_iter().map(|e| e.expr).collect::<Vec<_>>();
		let order_keys = order_by
			.into_iter()
			.map(|key| SortExpr {
				expr: key.expr,
				descending: key.descending,
				nulls_first: key.nulls_first,
			})
			.collect::<Vec<_>>();
		VmResult::Ok(Self {
			plan: window_over_exchange(self.plan, partition_exprs, order_keys, calls),
		})
	}
}

fn join_with_kind(
	left: LogicalPlan,
	right: LogicalPlan,
	kind: JoinKind,
	on: RuneExpr,
) -> VmResult<RuneDataFrame> {
	join_with_strategy(left, right, kind, on, JoinStrategy::HashPartitioned, None)
}

fn join_with_strategy(
	left: LogicalPlan,
	right: LogicalPlan,
	kind: JoinKind,
	on: RuneExpr,
	strategy: JoinStrategy,
	build_side: Option<BroadcastBuildSide>,
) -> VmResult<RuneDataFrame> {
	let pairs = match expr_to_join_keys(&on.expr) {
		Ok(pairs) => pairs,
		Err(error) => return VmResult::panic(error.to_string()),
	};
	match build_join_plan(
		left,
		right,
		kind,
		pairs,
		strategy,
		JoinStrategyPinSource::Rune,
		build_side,
	) {
		Ok(plan) => VmResult::Ok(RuneDataFrame { plan }),
		Err(error) => VmResult::panic(error.to_string()),
	}
}

fn parse_broadcast_build_side(side: &str) -> Option<BroadcastBuildSide> {
	if side.eq_ignore_ascii_case("left") {
		Some(BroadcastBuildSide::Left)
	} else if side.eq_ignore_ascii_case("right") {
		Some(BroadcastBuildSide::Right)
	} else {
		None
	}
}

fn pin_broadcast_build(
	plan: LogicalPlan,
	build_side: BroadcastBuildSide,
) -> Result<LogicalPlan, String> {
	let LogicalPlan::Join {
		left,
		right,
		kind,
		on,
		on_filter,
		null_safe_keys,
		strategy_intent,
	} = plan
	else {
		return Err("broadcast_build can only annotate a join".to_string());
	};

	match strategy_intent {
		JoinStrategyIntent::Auto => Ok(LogicalPlan::Join {
			left,
			right,
			kind,
			on,
			on_filter,
			null_safe_keys,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Rune,
				build_side: Some(build_side),
			},
		}),
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::HashPartitioned,
			source: JoinStrategyPinSource::Rune,
			build_side: None,
		} => Ok(LogicalPlan::Join {
			left: strip_logical_root_exchange(left),
			right: strip_logical_root_exchange(right),
			kind,
			on,
			on_filter,
			null_safe_keys,
			strategy_intent: JoinStrategyIntent::Pinned {
				strategy: JoinStrategy::Broadcast,
				source: JoinStrategyPinSource::Rune,
				build_side: Some(build_side),
			},
		}),
		JoinStrategyIntent::Pinned {
			strategy: JoinStrategy::Broadcast,
			..
		} => Err("broadcast_build cannot annotate an already broadcast-pinned join".to_string()),
		JoinStrategyIntent::Pinned { .. } => {
			Err("broadcast_build can only annotate a Rune-authored hash or auto join".to_string())
		}
	}
}

fn strip_logical_root_exchange(input: Box<LogicalPlan>) -> Box<LogicalPlan> {
	match *input {
		LogicalPlan::Exchange { input, .. } => input,
		other => Box::new(other),
	}
}

fn non_negative_usize_arg(value: i64) -> Option<usize> {
	usize::try_from(value).ok()
}

#[derive(Any, Debug, Clone)]
pub struct RuneAggr {
	pub(crate) func: AggregateFunction,
	pub(crate) args: Vec<Expr>,
}

#[derive(Any, Debug, Clone)]
pub struct ToronContext {}

impl ToronContext {
	pub fn table(&self, name: String) -> RuneDataFrame {
		RuneDataFrame::new(name)
	}

	pub fn optimizer_decisions(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_DECISIONS_TABLE)
	}

	pub fn optimizer_runtime_verdicts(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_RUNTIME_VERDICTS_TABLE)
	}

	pub fn optimizer_evidence_status(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_EVIDENCE_STATUS_TABLE)
	}

	pub fn optimizer_margin_advisory(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_MARGIN_ADVISORY_TABLE)
	}

	pub fn optimizer_receipt_gates(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_RECEIPT_GATES_TABLE)
	}

	pub fn optimizer_decision_conflicts(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_DECISION_CONFLICTS_TABLE)
	}

	pub fn optimizer_memo_admission(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_MEMO_ADMISSION_TABLE)
	}

	pub fn optimizer_probe_linearity_fences(&self) -> RuneDataFrame {
		Self::optimizer_readout(RUNE_OPTIMIZER_PROBE_LINEARITY_FENCES_TABLE)
	}

	pub fn sql(&self, query: String) -> VmResult<RuneDataFrame> {
		match SqlPlanner::new().plan_relation(&query) {
			Ok(plan) => VmResult::Ok(RuneDataFrame { plan }),
			Err(error) => VmResult::panic(error.to_string()),
		}
	}

	fn optimizer_readout(table: &'static str) -> RuneDataFrame {
		debug_assert!(RUNE_OPTIMIZER_READOUT_TABLES.contains(&table));
		RuneDataFrame {
			plan: LogicalPlan::ScanNamed {
				table: TableRef::with_schema(RUNE_INFORMATION_SCHEMA_NAMESPACE, table),
				projection: None,
			},
		}
	}
}

pub fn col(name: String) -> RuneExpr {
	RuneExpr {
		expr: Expr::column(name),
	}
}

pub fn qcol(rel: String, col: String) -> RuneExpr {
	RuneExpr {
		expr: Expr::qualified_column(TableRef::new(rel), col),
	}
}

pub fn lit(val: i32) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Int32(Some(val))),
	}
}

pub fn lit_null() -> RuneExpr {
	RuneExpr {
		expr: Expr::UntypedNull,
	}
}

pub fn lit_str(value: String) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Utf8(Some(value))),
	}
}

pub fn lit_f64(value: f64) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Float64(Some(value))),
	}
}

pub fn lit_bool(value: bool) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Boolean(Some(value))),
	}
}

pub fn lit_i64(value: i64) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Int64(Some(value))),
	}
}

pub fn convert(expr: RuneExpr, target_logical_name: String) -> VmResult<RuneExpr> {
	match validate_rune_logical_type_name("CONVERT", &target_logical_name) {
		Ok(target_logical_name) => VmResult::Ok(RuneExpr {
			expr: Expr::SqlConvert {
				expr: Box::new(expr.expr),
				target_logical_name,
			},
		}),
		Err(error) => VmResult::panic(error),
	}
}

fn validate_rune_logical_type_name(context: &'static str, name: &str) -> Result<String, String> {
	if name.eq_ignore_ascii_case("naked") {
		return Err(format!(
			"{context} logical type name cannot be the naked sentinel"
		));
	}
	if name
		.bytes()
		.any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
	{
		return Err(format!(
			"{context} logical type names must be one-part ASCII identifiers"
		));
	}
	Ok(name.to_string())
}

pub fn lit_date(value: String) -> VmResult<RuneExpr> {
	match parse_date_ymd(&value) {
		Ok(value) => VmResult::Ok(RuneExpr {
			expr: Expr::Literal(ScalarValue::Date32(Some(value))),
		}),
		Err(error) => VmResult::panic(error.to_string()),
	}
}

pub fn lit_timestamp(value: String) -> VmResult<RuneExpr> {
	match crate::timestamp::parse_explicit_timestamptz(&value, TimeUnit::Nanosecond) {
		Ok(value) => VmResult::Ok(RuneExpr {
			expr: Expr::Literal(ScalarValue::Timestamp {
				value: Some(value),
				unit: TimeUnit::Nanosecond,
			}),
		}),
		Err(error) => VmResult::panic(error.to_string()),
	}
}

fn interval_literal(amount: i64, nanos_per_unit: i64, unit: &'static str) -> VmResult<RuneExpr> {
	match amount.checked_mul(nanos_per_unit) {
		Some(nanos) => VmResult::Ok(RuneExpr {
			expr: Expr::Literal(ScalarValue::Interval(Some(nanos))),
		}),
		None => VmResult::panic(format!("interval {unit} value overflows i64 nanoseconds")),
	}
}

fn calendar_interval_literal(
	amount: i64,
	months_per_unit: i64,
	unit: &'static str,
) -> VmResult<RuneExpr> {
	match amount
		.checked_mul(months_per_unit)
		.and_then(|months| i32::try_from(months).ok())
	{
		Some(months) => VmResult::Ok(RuneExpr {
			expr: Expr::Literal(ScalarValue::CalendarMonthInterval(Some(months))),
		}),
		None => VmResult::panic(format!(
			"interval {unit} value overflows i32 calendar months"
		)),
	}
}

pub fn nanoseconds(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 1, "nanoseconds")
}

pub fn microseconds(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 1_000, "microseconds")
}

pub fn milliseconds(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 1_000_000, "milliseconds")
}

pub fn seconds(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 1_000_000_000, "seconds")
}

pub fn minutes(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 60 * 1_000_000_000, "minutes")
}

pub fn hours(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 60 * 60 * 1_000_000_000, "hours")
}

pub fn days(value: i64) -> VmResult<RuneExpr> {
	interval_literal(value, 24 * 60 * 60 * 1_000_000_000, "days")
}

pub fn months(value: i64) -> VmResult<RuneExpr> {
	calendar_interval_literal(value, 1, "months")
}

pub fn years(value: i64) -> VmResult<RuneExpr> {
	calendar_interval_literal(value, 12, "years")
}

pub fn asc(expr: RuneExpr) -> RuneSortKey {
	expr.asc()
}

pub fn desc(expr: RuneExpr) -> RuneSortKey {
	expr.desc()
}

pub fn when(condition: RuneExpr, result: RuneExpr) -> RuneWhenThen {
	RuneWhenThen {
		condition: condition.expr,
		result: result.expr,
	}
}

pub fn case(branches: Vec<RuneWhenThen>, else_expr: RuneExpr) -> VmResult<RuneExpr> {
	if branches.is_empty() {
		return VmResult::panic("CASE requires at least one WHEN branch");
	}
	VmResult::Ok(RuneExpr {
		expr: Expr::Case {
			when_then: branches
				.into_iter()
				.map(|branch| (Box::new(branch.condition), Box::new(branch.result)))
				.collect(),
			else_expr: Some(Box::new(else_expr.expr)),
		},
	})
}

pub fn type_i32() -> RuneDataType {
	RuneDataType {
		data_type: DataType::Int32,
	}
}

pub fn type_i64() -> RuneDataType {
	RuneDataType {
		data_type: DataType::Int64,
	}
}

pub fn type_f32() -> RuneDataType {
	RuneDataType {
		data_type: DataType::Float32,
	}
}

pub fn type_f64() -> RuneDataType {
	RuneDataType {
		data_type: DataType::Float64,
	}
}

pub fn time_bucket(interval_str: String, ts: RuneExpr) -> VmResult<RuneExpr> {
	// Rejects at lowering, matching SQL. Coercing an unknown interval to a zero
	// duration used to defer the failure to the evaluator, where it divided by
	// zero and aborted the process.
	let Some(interval) = crate::ir::time_bucket_interval_from_literal(&interval_str) else {
		return VmResult::panic(format!("unsupported time_bucket interval: {interval_str}"));
	};
	VmResult::Ok(RuneExpr {
		expr: crate::ir::time_bucket_scalar_function(interval, ts.expr),
	})
}

pub fn extract(field: String, expr: RuneExpr) -> VmResult<RuneExpr> {
	let Some(field) = TemporalExtractField::parse_name(&field) else {
		return VmResult::panic(format!("unsupported EXTRACT field: {field}"));
	};
	VmResult::Ok(RuneExpr {
		expr: crate::ir::extract_scalar_function(field, expr.expr),
	})
}

pub fn date_part(field: String, expr: RuneExpr) -> VmResult<RuneExpr> {
	extract(field, expr)
}

pub fn date_trunc(field: String, expr: RuneExpr) -> VmResult<RuneExpr> {
	let Some(field) = TemporalTruncField::parse_name(&field) else {
		return VmResult::panic(format!("unsupported date_trunc field: {field}"));
	};
	VmResult::Ok(RuneExpr {
		expr: crate::ir::date_trunc_scalar_function(field, expr.expr),
	})
}

pub fn at_time_zone(expr: RuneExpr, zone: String) -> RuneExpr {
	RuneExpr {
		expr: crate::ir::at_time_zone_scalar_function(expr.expr, zone),
	}
}

pub fn linear_regress(y: RuneExpr, x: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::LinearRegress,
		args: vec![y.expr, x.expr],
	}
}

pub fn approx_quantile(val: RuneExpr, q: f64) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::ApproxQuantile(q),
		args: vec![val.expr],
	}
}

pub fn exp(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Exp,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn sqrt(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Sqrt,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn power(base: RuneExpr, exponent: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Pow,
			args: vec![base.expr, exponent.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn abs(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Abs,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn ln(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Ln,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn log(base: RuneExpr, input: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Log,
			args: vec![base.expr, input.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

/// Rune spells a variadic call as a list, the way `project` and `case` already
/// do. SQL's `GREATEST(a, b, c)` is `greatest([a, b, c])` here.
/// SUBSTRING had no Rune spelling before it entered the registry, which the
/// one-identifier-two-spellings rule treats as a defect. Rune states the length
/// explicitly, as it does for `lag` and `round`.
pub fn substring(input: RuneExpr, from: RuneExpr, length: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Substring,
			args: vec![input.expr, from.expr, length.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn random() -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Random,
			args: Vec::new(),
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn greatest(args: Vec<RuneExpr>) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Greatest,
			args: args.into_iter().map(|arg| arg.expr).collect(),
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn least(args: Vec<RuneExpr>) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Least,
			args: args.into_iter().map(|arg| arg.expr).collect(),
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn width_bucket(operand: RuneExpr, low: RuneExpr, high: RuneExpr, count: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::WidthBucket,
			args: vec![operand.expr, low.expr, high.expr, count.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn upper(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Upper,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn lower(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Lower,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn initcap(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Initcap,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn reverse(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Reverse,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn trim(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Trim,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn char_length(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::CharLength,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn ceil(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Ceil,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn floor(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Floor,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn trunc(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Trunc,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn sign(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Sign,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

/// `mod` is a keyword in both Rust and Rune, so this is `modulo` on the Rune
/// surface. SQL keeps `MOD`; the registry identifier is the same either way.
pub fn modulo(left: RuneExpr, right: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Mod,
			args: vec![left.expr, right.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

/// Rune spells the digit count explicitly, the way `lag` spells its offset.
/// SQL's `ROUND(value)` is `round(value, lit(0))` here.
pub fn round(value: RuneExpr, digits: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Round,
			args: vec![value.expr, digits.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn count_all() -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::CountAll,
		args: vec![Expr::column("count")],
	}
}

pub fn count(expr: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::Count,
		args: vec![expr.expr],
	}
}

pub fn sum(expr: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::Sum,
		args: vec![expr.expr],
	}
}

pub fn avg(expr: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::Avg,
		args: vec![expr.expr],
	}
}

pub fn min(expr: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::Min,
		args: vec![expr.expr],
	}
}

pub fn max(expr: RuneExpr) -> RuneAggr {
	RuneAggr {
		func: AggregateFunction::Max,
		args: vec![expr.expr],
	}
}

// ---------------------------------------------------------------------------
// ADR 251: window-call constructors. Each returns a `WindowFunctionCall` with the default frame
// (`None`) and the derived default output name; `.named(..)` / `.frame(..)` refine it.
// ---------------------------------------------------------------------------

fn window_call(function: WindowFunction, args: Vec<Expr>) -> WindowFunctionCall {
	let output_name = window_function_default_name(&function).to_string();
	WindowFunctionCall {
		function,
		output_name,
		args,
		frame: None,
		ignore_nulls: false,
	}
}

pub fn rank() -> WindowFunctionCall {
	window_call(WindowFunction::Rank, Vec::new())
}

pub fn row_number() -> WindowFunctionCall {
	window_call(WindowFunction::RowNumber, Vec::new())
}

pub fn dense_rank() -> WindowFunctionCall {
	window_call(WindowFunction::DenseRank, Vec::new())
}

pub fn ntile(n: i64) -> VmResult<WindowFunctionCall> {
	if n <= 0 {
		return VmResult::panic("ntile requires a positive bucket count");
	}
	VmResult::Ok(window_call(
		WindowFunction::Ntile { buckets: n as u64 },
		Vec::new(),
	))
}

pub fn lag(expr: RuneExpr, offset: i64) -> WindowFunctionCall {
	value_window_call(
		ValueFunction::Lag {
			offset: offset as u64,
			default: None,
		},
		expr,
	)
}

pub fn lead(expr: RuneExpr, offset: i64) -> WindowFunctionCall {
	value_window_call(
		ValueFunction::Lead {
			offset: offset as u64,
			default: None,
		},
		expr,
	)
}

pub fn first_value(expr: RuneExpr) -> WindowFunctionCall {
	value_window_call(ValueFunction::FirstValue, expr)
}

pub fn last_value(expr: RuneExpr) -> WindowFunctionCall {
	value_window_call(ValueFunction::LastValue, expr)
}

pub fn nth_value(expr: RuneExpr, n: i64) -> WindowFunctionCall {
	value_window_call(ValueFunction::NthValue { n: n as u64 }, expr)
}

fn value_window_call(value: ValueFunction, expr: RuneExpr) -> WindowFunctionCall {
	window_call(WindowFunction::Value(value), vec![expr.expr])
}

// ---------------------------------------------------------------------------
// ADR 251: frame bound / frame builders. The Rune surface mirrors the SQL `<unit> BETWEEN .. AND ..`
// syntax: each bound is constructed neutrally as a Rows delta, then `rows`/`groups`/`range` coerce the
// carried delta to match the chosen unit.
// ---------------------------------------------------------------------------

pub fn unbounded_preceding() -> WindowFrameBound {
	WindowFrameBound::UnboundedPreceding
}

pub fn current_row() -> WindowFrameBound {
	WindowFrameBound::CurrentRow
}

pub fn unbounded_following() -> WindowFrameBound {
	WindowFrameBound::UnboundedFollowing
}

pub fn preceding(n: i64) -> WindowFrameBound {
	WindowFrameBound::Preceding(WindowFrameDelta::Rows(n as u64))
}

pub fn following(n: i64) -> WindowFrameBound {
	WindowFrameBound::Following(WindowFrameDelta::Rows(n as u64))
}

/// ADR 251 (Rune-finish): extract a `ScalarValue` from a literal Rune expression (`lit(..)`,
/// `lit_i64(..)`, `days(..)`, …). Non-literal expressions are rejected — defaults and value offsets
/// must be literals.
fn scalar_from_literal(expr: RuneExpr) -> Result<ScalarValue, &'static str> {
	match expr.expr {
		Expr::Literal(value) => Ok(value),
		_ => Err("expected a literal value (lit(..), lit_i64(..), days(..), …)"),
	}
}

/// ADR 251 (Rune-finish): a value-distance RANGE bound `<offset> PRECEDING` where the offset is a
/// literal value — `preceding_value(lit_i64(1))` (numeric key) or `preceding_value(days(1))`
/// (Timestamp key, INTERVAL distance). Use only with `range`.
pub fn preceding_value(offset: RuneExpr) -> VmResult<WindowFrameBound> {
	match scalar_from_literal(offset) {
		Ok(value) => VmResult::Ok(WindowFrameBound::Preceding(WindowFrameDelta::Range(value))),
		Err(message) => VmResult::panic(message),
	}
}

pub fn following_value(offset: RuneExpr) -> VmResult<WindowFrameBound> {
	match scalar_from_literal(offset) {
		Ok(value) => VmResult::Ok(WindowFrameBound::Following(WindowFrameDelta::Range(value))),
		Err(message) => VmResult::panic(message),
	}
}

pub fn rows(start: WindowFrameBound, end: WindowFrameBound) -> WindowFrame {
	WindowFrame {
		unit: WindowFrameUnit::Rows,
		start,
		end,
	}
}

pub fn groups(start: WindowFrameBound, end: WindowFrameBound) -> WindowFrame {
	WindowFrame {
		unit: WindowFrameUnit::Groups,
		start: coerce_bound_to_groups(start),
		end: coerce_bound_to_groups(end),
	}
}

pub fn range(start: WindowFrameBound, end: WindowFrameBound) -> WindowFrame {
	WindowFrame {
		unit: WindowFrameUnit::Range,
		start: coerce_bound_to_range(start),
		end: coerce_bound_to_range(end),
	}
}

/// Coerce a neutrally-built `Rows(n)` bound delta to a `Groups(n)` peer-group count for a GROUPS frame.
fn coerce_bound_to_groups(bound: WindowFrameBound) -> WindowFrameBound {
	match bound {
		WindowFrameBound::Preceding(WindowFrameDelta::Rows(n)) => {
			WindowFrameBound::Preceding(WindowFrameDelta::Groups(n))
		}
		WindowFrameBound::Following(WindowFrameDelta::Rows(n)) => {
			WindowFrameBound::Following(WindowFrameDelta::Groups(n))
		}
		other => other,
	}
}

/// Coerce a neutrally-built `Rows(n)` bound delta to a numeric `Range(Int64(n))` value distance for a
/// RANGE frame. Bounds already carrying a value offset (`preceding_value(..)` / `following_value(..)`,
/// including INTERVAL distances over a Timestamp key) pass through unchanged via the `other` arm.
fn coerce_bound_to_range(bound: WindowFrameBound) -> WindowFrameBound {
	match bound {
		WindowFrameBound::Preceding(WindowFrameDelta::Rows(n)) => {
			WindowFrameBound::Preceding(WindowFrameDelta::Range(ScalarValue::Int64(Some(n as i64))))
		}
		WindowFrameBound::Following(WindowFrameDelta::Rows(n)) => {
			WindowFrameBound::Following(WindowFrameDelta::Range(ScalarValue::Int64(Some(n as i64))))
		}
		other => other,
	}
}

impl RuneAggr {
	/// ADR 251: turn an aggregate constructor (`sum(col)`, `count_all()`, …) into an aggregate window
	/// call, reusing the same `(func, args)`. Pairs with `.frame(..)` for an explicit frame.
	pub fn over(self) -> WindowFunctionCall {
		window_call(WindowFunction::Aggregate(self.func), self.args)
	}
}

impl WindowFunctionCall {
	/// ADR 251: attach an explicit frame (from `rows`/`groups`/`range`) to this window call.
	pub fn frame(mut self, frame: WindowFrame) -> WindowFunctionCall {
		self.frame = Some(frame);
		self
	}

	/// ADR 251: set the output column name appended by this window call.
	pub fn named(mut self, name: String) -> WindowFunctionCall {
		self.output_name = name;
		self
	}

	/// ADR 251 (Rune-finish): set the partition-edge default for a `lag`/`lead` call from a literal —
	/// `lag(col, 2).with_default(lit_i64(-1))`. Errors on a non-LAG/LEAD call or a non-literal default.
	/// (Named `with_default` because `default` is a Rune reserved word.)
	pub fn with_default(mut self, default: RuneExpr) -> VmResult<WindowFunctionCall> {
		let value = match scalar_from_literal(default) {
			Ok(value) => value,
			Err(message) => return VmResult::panic(message),
		};
		match &mut self.function {
			WindowFunction::Value(ValueFunction::Lag { default: slot, .. })
			| WindowFunction::Value(ValueFunction::Lead { default: slot, .. }) => {
				*slot = Some(value);
			}
			_ => return VmResult::panic("default(..) applies only to lag/lead window calls"),
		}
		VmResult::Ok(self)
	}

	/// ADR 252: skip NULL values when navigating a value call — `lag(col, 1).ignore_nulls()`. Errors on
	/// a non-value call (ranking / aggregate / NTILE do not take IGNORE NULLS).
	pub fn ignore_nulls(mut self) -> VmResult<WindowFunctionCall> {
		if !matches!(self.function, WindowFunction::Value(_)) {
			return VmResult::panic(
				"ignore_nulls applies only to lag/lead/first_value/last_value/nth_value",
			);
		}
		self.ignore_nulls = true;
		VmResult::Ok(self)
	}
}

#[cfg(test)]
pub async fn sandbox_sleep_ms(ms: i64) {
	let ms = u64::try_from(ms).unwrap_or(0);
	tokio::time::sleep(Duration::from_millis(ms)).await;
}

impl RuneExpr {
	pub fn asc(self) -> RuneSortKey {
		RuneSortKey {
			expr: self.expr,
			descending: false,
			nulls_first: false,
		}
	}

	pub fn desc(self) -> RuneSortKey {
		RuneSortKey {
			expr: self.expr,
			descending: true,
			nulls_first: false,
		}
	}

	pub fn named(self, name: String) -> RuneNamedExpr {
		RuneNamedExpr {
			expr: self.expr,
			name,
		}
	}

	pub fn gt(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Gt(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn eq(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Eq(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn lt(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Lt(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn lte(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Lte(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn gte(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Gte(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn ne(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Neq(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn and(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::And(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn or(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Or(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn logical_not(self) -> RuneExpr {
		RuneExpr {
			expr: Expr::Not(Box::new(self.expr)),
		}
	}

	pub fn is_null(self) -> RuneExpr {
		RuneExpr {
			expr: Expr::IsNull(Box::new(self.expr)),
		}
	}

	pub fn is_not_null(self) -> RuneExpr {
		RuneExpr {
			expr: Expr::IsNotNull(Box::new(self.expr)),
		}
	}

	#[allow(clippy::should_implement_trait)]
	pub fn add(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Add(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	#[allow(clippy::should_implement_trait)]
	pub fn sub(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Sub(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	#[allow(clippy::should_implement_trait)]
	pub fn mul(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Mul(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	#[allow(clippy::should_implement_trait)]
	pub fn div(self, other: RuneExpr) -> RuneExpr {
		RuneExpr {
			expr: Expr::Div(Box::new(self.expr), Box::new(other.expr)),
		}
	}

	pub fn cast(self, target: RuneDataType) -> RuneExpr {
		RuneExpr {
			expr: Expr::Cast(Box::new(self.expr), target.data_type),
		}
	}

	pub fn try_cast(self, target: RuneDataType) -> RuneExpr {
		RuneExpr {
			expr: Expr::TryCast(Box::new(self.expr), target.data_type),
		}
	}

	pub fn convert(self, target_logical_name: String) -> VmResult<RuneExpr> {
		convert(self, target_logical_name)
	}

	fn logical_ascription(self, logical_type: LogicalTypeId) -> RuneExpr {
		RuneExpr {
			expr: Expr::LogicalAscription {
				expr: Box::new(self.expr),
				logical_type,
			},
		}
	}

	pub fn as_customer_id(self) -> RuneExpr {
		self.logical_ascription(CUSTOMER_ID_LOGICAL_TYPE)
	}

	pub fn as_product_id(self) -> RuneExpr {
		self.logical_ascription(PRODUCT_ID_LOGICAL_TYPE)
	}

	pub fn in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
		build_in_list(self.expr, list, false)
	}

	pub fn not_in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
		build_in_list(self.expr, list, true)
	}

	pub fn like(self, pattern: String) -> RuneExpr {
		build_like(self.expr, pattern, false, false)
	}

	pub fn not_like(self, pattern: String) -> RuneExpr {
		build_like(self.expr, pattern, true, false)
	}

	pub fn ilike(self, pattern: String) -> RuneExpr {
		build_like(self.expr, pattern, false, true)
	}

	pub fn not_ilike(self, pattern: String) -> RuneExpr {
		build_like(self.expr, pattern, true, true)
	}
}

pub fn ltrim(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Ltrim,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::Trim(Box::new(
				crate::ir::TrimScalarFunctionParameter {
					where_field: crate::ir::TrimWhere::Leading,
				},
			)),
		},
	}
}

pub fn rtrim(expr: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Rtrim,
			args: vec![expr.expr],
			parameter: ScalarFunctionParameter::Trim(Box::new(
				crate::ir::TrimScalarFunctionParameter {
					where_field: crate::ir::TrimWhere::Trailing,
				},
			)),
		},
	}
}

pub fn concat(args: Vec<RuneExpr>) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Concat,
			args: args.into_iter().map(|arg| arg.expr).collect(),
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn position(needle: RuneExpr, input: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Position,
			args: vec![needle.expr, input.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn strpos(input: RuneExpr, needle: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Strpos,
			args: vec![needle.expr, input.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

pub fn replace(input: RuneExpr, from: RuneExpr, to: RuneExpr) -> RuneExpr {
	RuneExpr {
		expr: Expr::ScalarFunction {
			id: ScalarFunctionId::Replace,
			args: vec![input.expr, from.expr, to.expr],
			parameter: ScalarFunctionParameter::None,
		},
	}
}

impl RuneSortKey {
	pub fn nulls_first(mut self) -> Self {
		self.nulls_first = true;
		self
	}

	pub fn nulls_last(mut self) -> Self {
		self.nulls_first = false;
		self
	}
}

fn build_in_list(expr: Expr, list: Vec<RuneExpr>, negated: bool) -> VmResult<RuneExpr> {
	if list.is_empty() {
		return VmResult::panic("IN list must not be empty");
	}
	if list.len() > RUNE_MAX_IN_LIST_ITEMS {
		return VmResult::panic(format!("IN list exceeds {RUNE_MAX_IN_LIST_ITEMS} items"));
	}
	VmResult::Ok(RuneExpr {
		expr: Expr::InList {
			expr: Box::new(expr),
			list: list.into_iter().map(|item| item.expr).collect(),
			negated,
		},
	})
}

fn build_like(expr: Expr, pattern: String, negated: bool, case_insensitive: bool) -> RuneExpr {
	RuneExpr {
		expr: Expr::Like {
			expr: Box::new(expr),
			pattern,
			negated,
			case_insensitive,
		},
	}
}

pub fn create_toron_module() -> Result<Module, rune::compile::ContextError> {
	let mut m = Module::new();
	m.ty::<RuneDataFrame>()?;
	m.ty::<RuneExpr>()?;
	m.ty::<RuneNamedExpr>()?;
	m.ty::<RuneSortKey>()?;
	m.ty::<RuneWhenThen>()?;
	m.ty::<RuneDataType>()?;
	m.ty::<CustomerId>()?;
	m.ty::<ProductId>()?;
	m.ty::<RuneAggr>()?;
	m.ty::<ToronContext>()?;
	m.ty::<WindowFunctionCall>()?;
	m.ty::<WindowFrame>()?;
	m.ty::<WindowFrameBound>()?;

	m.function(["col"], col).build()?;
	m.function(["qcol"], qcol).build()?;
	m.function(["lit"], lit).build()?;
	m.function(["lit_null"], lit_null).build()?;
	m.function(["lit_str"], lit_str).build()?;
	m.function(["lit_f64"], lit_f64).build()?;
	m.function(["lit_bool"], lit_bool).build()?;
	m.function(["lit_i64"], lit_i64).build()?;
	m.function(["convert"], convert).build()?;
	m.function(["lit_date"], lit_date).build()?;
	m.function(["lit_timestamp"], lit_timestamp).build()?;
	m.function(["nanoseconds"], nanoseconds).build()?;
	m.function(["microseconds"], microseconds).build()?;
	m.function(["milliseconds"], milliseconds).build()?;
	m.function(["seconds"], seconds).build()?;
	m.function(["minutes"], minutes).build()?;
	m.function(["hours"], hours).build()?;
	m.function(["days"], days).build()?;
	m.function(["months"], months).build()?;
	m.function(["years"], years).build()?;
	m.function(["asc"], asc).build()?;
	m.function(["desc"], desc).build()?;
	m.function(["when"], when).build()?;
	m.function(["case"], case).build()?;
	m.function(["type_i32"], type_i32).build()?;
	m.function(["type_i64"], type_i64).build()?;
	m.function(["type_f32"], type_f32).build()?;
	m.function(["type_f64"], type_f64).build()?;
	m.function(["time_bucket"], time_bucket).build()?;
	m.function(["extract"], extract).build()?;
	m.function(["date_part"], date_part).build()?;
	m.function(["date_trunc"], date_trunc).build()?;
	m.function(["at_time_zone"], at_time_zone).build()?;
	m.function(["exp"], exp).build()?;
	m.function(["sqrt"], sqrt).build()?;
	m.function(["power"], power).build()?;
	m.function(["abs"], abs).build()?;
	m.function(["ln"], ln).build()?;
	m.function(["log"], log).build()?;
	m.function(["round"], round).build()?;
	m.function(["ceil"], ceil).build()?;
	m.function(["floor"], floor).build()?;
	m.function(["trunc"], trunc).build()?;
	m.function(["sign"], sign).build()?;
	m.function(["upper"], upper).build()?;
	m.function(["lower"], lower).build()?;
	m.function(["initcap"], initcap).build()?;
	m.function(["reverse"], reverse).build()?;
	m.function(["trim"], trim).build()?;
	m.function(["ltrim"], ltrim).build()?;
	m.function(["rtrim"], rtrim).build()?;
	m.function(["concat"], concat).build()?;
	m.function(["position"], position).build()?;
	m.function(["strpos"], strpos).build()?;
	m.function(["replace"], replace).build()?;
	m.function(["char_length"], char_length).build()?;
	m.function(["width_bucket"], width_bucket).build()?;
	m.function(["greatest"], greatest).build()?;
	m.function(["least"], least).build()?;
	m.function(["random"], random).build()?;
	m.function(["substring"], substring).build()?;
	m.function(["modulo"], modulo).build()?;
	m.function(["linear_regress"], linear_regress).build()?;
	m.function(["approx_quantile"], approx_quantile).build()?;
	m.function(["count_all"], count_all).build()?;
	m.function(["count"], count).build()?;
	m.function(["sum"], sum).build()?;
	m.function(["avg"], avg).build()?;
	m.function(["min"], min).build()?;
	m.function(["max"], max).build()?;
	m.function(["rank"], rank).build()?;
	m.function(["row_number"], row_number).build()?;
	m.function(["dense_rank"], dense_rank).build()?;
	m.function(["ntile"], ntile).build()?;
	m.function(["lag"], lag).build()?;
	m.function(["lead"], lead).build()?;
	m.function(["first_value"], first_value).build()?;
	m.function(["last_value"], last_value).build()?;
	m.function(["nth_value"], nth_value).build()?;
	m.function(["unbounded_preceding"], unbounded_preceding)
		.build()?;
	m.function(["current_row"], current_row).build()?;
	m.function(["unbounded_following"], unbounded_following)
		.build()?;
	m.function(["preceding"], preceding).build()?;
	m.function(["following"], following).build()?;
	m.function(["preceding_value"], preceding_value).build()?;
	m.function(["following_value"], following_value).build()?;
	m.function(["rows"], rows).build()?;
	m.function(["groups"], groups).build()?;
	m.function(["range"], range).build()?;
	#[cfg(test)]
	m.function(["sandbox_sleep_ms"], sandbox_sleep_ms).build()?;

	m.associated_function("table", ToronContext::table)?;
	m.associated_function("optimizer_decisions", ToronContext::optimizer_decisions)?;
	m.associated_function(
		"optimizer_runtime_verdicts",
		ToronContext::optimizer_runtime_verdicts,
	)?;
	m.associated_function(
		"optimizer_evidence_status",
		ToronContext::optimizer_evidence_status,
	)?;
	m.associated_function(
		"optimizer_margin_advisory",
		ToronContext::optimizer_margin_advisory,
	)?;
	m.associated_function(
		"optimizer_receipt_gates",
		ToronContext::optimizer_receipt_gates,
	)?;
	m.associated_function(
		"optimizer_decision_conflicts",
		ToronContext::optimizer_decision_conflicts,
	)?;
	m.associated_function(
		"optimizer_memo_admission",
		ToronContext::optimizer_memo_admission,
	)?;
	m.associated_function(
		"optimizer_probe_linearity_fences",
		ToronContext::optimizer_probe_linearity_fences,
	)?;
	m.associated_function("sql", ToronContext::sql)?;
	m.associated_function("alias", RuneDataFrame::alias)?;
	m.associated_function("join", RuneDataFrame::join)?;
	m.associated_function("inner_join", RuneDataFrame::inner_join)?;
	m.associated_function("auto_join", RuneDataFrame::auto_join)?;
	m.associated_function("broadcast_join", RuneDataFrame::broadcast_join)?;
	m.associated_function("broadcast_build", RuneDataFrame::broadcast_build)?;
	m.associated_function("explain", RuneDataFrame::explain)?;
	m.associated_function("analyze", RuneDataFrame::analyze)?;
	m.associated_function("left_join", RuneDataFrame::left_join)?;
	m.associated_function("right_join", RuneDataFrame::right_join)?;
	m.associated_function("full_join", RuneDataFrame::full_join)?;
	m.associated_function("left_semi_join", RuneDataFrame::left_semi_join)?;
	m.associated_function("right_semi_join", RuneDataFrame::right_semi_join)?;
	m.associated_function("left_anti_join", RuneDataFrame::left_anti_join)?;
	m.associated_function("right_anti_join", RuneDataFrame::right_anti_join)?;
	m.associated_function("filter", RuneDataFrame::filter)?;
	m.associated_function("project", RuneDataFrame::project)?;
	m.associated_function("project_named", RuneDataFrame::project_named)?;
	m.associated_function("distinct", RuneDataFrame::distinct)?;
	m.associated_function("aggregate", RuneDataFrame::aggregate)?;
	m.associated_function(
		"kernel_density_default",
		RuneDataFrame::kernel_density_default,
	)?;
	m.associated_function("kernel_density", RuneDataFrame::kernel_density)?;
	m.associated_function("sort", RuneDataFrame::sort)?;
	m.associated_function("limit", RuneDataFrame::limit)?;
	m.associated_function("take", RuneDataFrame::take)?;
	m.associated_function("offset", RuneDataFrame::offset)?;
	m.associated_function("count", RuneDataFrame::count)?;
	m.associated_function("window", RuneDataFrame::window)?;

	// Window-call methods (ADR 251)
	m.associated_function("over", RuneAggr::over)?;
	m.associated_function("frame", WindowFunctionCall::frame)?;
	m.associated_function("named", WindowFunctionCall::named)?;
	m.associated_function("with_default", WindowFunctionCall::with_default)?;
	m.associated_function("ignore_nulls", WindowFunctionCall::ignore_nulls)?;

	// Expression methods
	m.associated_function("asc", RuneExpr::asc)?;
	m.associated_function("desc", RuneExpr::desc)?;
	m.associated_function("named", RuneExpr::named)?;
	m.associated_function("gt", RuneExpr::gt)?;
	m.associated_function("eq", RuneExpr::eq)?;
	m.associated_function("lt", RuneExpr::lt)?;
	m.associated_function("lte", RuneExpr::lte)?;
	m.associated_function("gte", RuneExpr::gte)?;
	m.associated_function("ne", RuneExpr::ne)?;
	m.associated_function("and", RuneExpr::and)?;
	m.associated_function("or", RuneExpr::or)?;
	m.associated_function("logical_not", RuneExpr::logical_not)?;
	m.associated_function("is_null", RuneExpr::is_null)?;
	m.associated_function("is_not_null", RuneExpr::is_not_null)?;
	m.associated_function("add", RuneExpr::add)?;
	m.associated_function("sub", RuneExpr::sub)?;
	m.associated_function("mul", RuneExpr::mul)?;
	m.associated_function("div", RuneExpr::div)?;
	m.associated_function("cast", RuneExpr::cast)?;
	m.associated_function("try_cast", RuneExpr::try_cast)?;
	m.associated_function("convert", RuneExpr::convert)?;
	m.associated_function(
		rune::Params::new("as_logical_type", [CustomerId::HASH]),
		RuneExpr::as_customer_id,
	)?;
	m.associated_function(
		rune::Params::new("as_logical_type", [ProductId::HASH]),
		RuneExpr::as_product_id,
	)?;
	m.associated_function("in_list", RuneExpr::in_list)?;
	m.associated_function("not_in_list", RuneExpr::not_in_list)?;
	m.associated_function("like", RuneExpr::like)?;
	m.associated_function("not_like", RuneExpr::not_like)?;
	m.associated_function("ilike", RuneExpr::ilike)?;
	m.associated_function("not_ilike", RuneExpr::not_ilike)?;
	m.associated_function("nulls_first", RuneSortKey::nulls_first)?;
	m.associated_function("nulls_last", RuneSortKey::nulls_last)?;

	Ok(m)
}

fn install_minimal_primitives(
	context: &mut Context,
	profile: AllowProfile,
) -> Result<(), rune::compile::ContextError> {
	match profile {
		AllowProfile::ToronAuthoring => {
			context.install(rune::modules::core::module()?)?;
			context.install(rune::modules::cmp::module()?)?;
			context.install(rune::modules::clone::module()?)?;
			context.install(rune::modules::num::module()?)?;
			context.install(rune::modules::i64::module()?)?;
			context.install(rune::modules::f64::module()?)?;
			context.install(rune::modules::tuple::module()?)?;
			context.install(rune::modules::future::module()?)?;
			context.install(rune::modules::iter::module()?)?;
			context.install(rune::modules::ops::module()?)?;
			context.install(rune::modules::option::module()?)?;
			context.install(rune::modules::result::module()?)?;
			context.install(rune::modules::string::module()?)?;
			context.install(rune::modules::vec::module()?)?;
		}
	}

	Ok(())
}

pub struct RuneEngine {
	context: Arc<Context>,
	runtime: Arc<rune::runtime::RuntimeContext>,
	allow_profile: AllowProfile,
}

impl RuneEngine {
	pub fn new() -> Result<Self, FrontendError> {
		Self::with_config(RuneSandboxConfig::v1_default())
	}

	pub fn with_config(config: RuneSandboxConfig) -> Result<Self, FrontendError> {
		let mut context = Context::new();
		install_minimal_primitives(&mut context, config.allow_profile)
			.map_err(rune_compile_error)?;
		context
			.install(create_toron_module().map_err(rune_compile_error)?)
			.map_err(rune_compile_error)?;

		let runtime = Arc::new(context.runtime().map_err(rune_compile_error)?);
		Ok(Self {
			context: Arc::new(context),
			runtime,
			allow_profile: config.allow_profile,
		})
	}

	pub async fn execute_script(
		&self,
		script: &str,
		config: &RuneSandboxConfig,
	) -> Result<LogicalPlan, FrontendError> {
		if config.allow_profile != self.allow_profile {
			return Err(rune_sandbox_error(
				RuneSandboxViolation::InternalSandboxError {
					detail: "execution config allow_profile does not match engine context"
						.to_string(),
				},
			));
		}

		if script.len() > config.max_source_bytes {
			return Err(rune_sandbox_error(RuneSandboxViolation::SourceTooLarge {
				bytes: script.len(),
				budget: config.max_source_bytes,
			}));
		}

		let unit = compile_script(script, &self.context, config)?;

		let mut vm = Vm::new(self.runtime.clone(), Arc::new(unit));

		let mut execution = vm
			.execute(["main"], (ToronContext {},))
			.map_err(|error| map_rune_runtime_error(error, config))?;

		let fuel_budget = usize_budget(config.fuel_budget);
		let memory_budget = usize_budget(config.memory_budget);
		let bounded_execution = limit::with(
			memory_budget,
			budget::with(fuel_budget, execution.async_complete()),
		);

		let output = tokio::time::timeout(config.wall_timeout, bounded_execution)
			.await
			.map_err(|_| {
				rune_sandbox_error(RuneSandboxViolation::Timeout {
					elapsed: config.wall_timeout,
				})
			})?
			.into_result()
			.map_err(|error| map_rune_runtime_error(error, config))?;

		let df: RuneDataFrame = rune::from_value(output).map_err(|_| {
			rune_sandbox_error(RuneSandboxViolation::InvalidEntrypoint {
				detail: "main did not return a Toron dataframe".to_string(),
			})
		})?;
		Ok(df.plan)
	}

	pub async fn execute_script_with_fuel(
		&self,
		script: &str,
		fuel_budget: u64,
	) -> Result<LogicalPlan, FrontendError> {
		let config = RuneSandboxConfig::with_fuel(fuel_budget);
		self.execute_script(script, &config).await
	}
}

fn compile_script(
	script: &str,
	context: &Context,
	config: &RuneSandboxConfig,
) -> Result<Unit, FrontendError> {
	limit::with(usize_budget(config.memory_budget), || {
		let mut sources = Sources::new();
		sources
			.insert(
				Source::new("script", script)
					.map_err(|error| map_rune_compile_error(error, config))?,
			)
			.map_err(|error| map_rune_compile_error(error, config))?;

		let mut diagnostics = Diagnostics::new();

		let unit = rune::prepare(&mut sources)
			.with_context(context)
			.with_diagnostics(&mut diagnostics)
			.build();

		if !diagnostics.is_empty() {
			let message = format!("{diagnostics:?}");
			if looks_like_memory_error(&message) {
				return Err(rune_sandbox_error(RuneSandboxViolation::MemoryExhausted {
					bytes_used: config.memory_budget,
					bytes_budget: config.memory_budget,
				}));
			}

			if let Some(capability) = classify_denied_capability(&message) {
				return Err(rune_sandbox_error(RuneSandboxViolation::CapabilityDenied {
					capability,
				}));
			}

			let span = diagnostics.diagnostics().iter().find_map(|diagnostic| {
				let Diagnostic::Fatal(fatal) = diagnostic else {
					return None;
				};
				let FatalDiagnosticKind::CompileError(error) = fatal.kind() else {
					return None;
				};
				let span = error.span();
				Some(RuneDiagnosticSpan {
					start: u32::try_from(span.start.into_usize()).ok()?,
					end: u32::try_from(span.end.into_usize()).ok()?,
				})
			});
			return Err(FrontendError::RuneCompile { message, span });
		}

		unit.map_err(|error| {
			let message = error.to_string();
			classify_denied_capability(&message).map_or_else(
				|| map_rune_compile_error(message, config),
				|capability| {
					rune_sandbox_error(RuneSandboxViolation::CapabilityDenied { capability })
				},
			)
		})
	})
	.call()
}

fn usize_budget(value: u64) -> usize {
	usize::try_from(value).unwrap_or(usize::MAX)
}

fn classify_denied_capability(source: &str) -> Option<CapabilityCategory> {
	let lower = source.to_ascii_lowercase();
	let checks: &[(CapabilityCategory, &[&str])] = &[
		(
			CapabilityCategory::Filesystem,
			&["std::fs", "fs::", "file::", "read_to_string("],
		),
		(
			CapabilityCategory::Process,
			&["std::process", "process::", "command::"],
		),
		(
			CapabilityCategory::Network,
			&["std::net", "net::", "tcpstream", "udp_socket", "udpsocket"],
		),
		(
			CapabilityCategory::Environment,
			&["std::env", "env::", "var_os("],
		),
		(
			CapabilityCategory::Time,
			&["std::time", "time::", "systemtime", "instant::"],
		),
		(
			CapabilityCategory::Random,
			// `random(` alone would match this engine's own `random()` scalar
			// function wherever a script's source appears in an error message,
			// turning a supported call into a reported security breach. The
			// denial signal is a foreign *path*, never a bare call.
			&["std::random", "rand::"],
		),
		(CapabilityCategory::Threading, &["std::thread", "thread::"]),
	];

	for (capability, patterns) in checks {
		if patterns.iter().any(|pattern| lower.contains(pattern)) {
			return Some(*capability);
		}
	}

	// A std path alone is not a capability signal. Rune reports type mismatches
	// with paths like `::std::f64` and `::std::string::String`, and treating
	// those as denials turns an ordinary mistake into a security report — which
	// misdirects the author and hides real denials in noise.
	//
	// What distinguishes a reach for a capability is a *module* path: an unknown
	// `std::<module>::<item>` call, the shape the specific lists above already
	// match. Value types under `std` are the sandbox's own supported surface, so
	// naming one says nothing about what the script tried to do.
	const STD_VALUE_TYPES: &[&str] = &[
		"bool", "bytes", "char", "f32", "f64", "float", "i8", "i16", "i32", "i64", "i128", "int",
		"isize", "object", "option", "result", "str", "string", "tuple", "u8", "u16", "u32", "u64",
		"u128", "usize", "vec",
	];
	let names_an_unknown_std_module = lower
		.split("std::")
		.skip(1)
		// A trailing segment with no further `::` is a type, not a module.
		.filter(|rest| rest.contains("::"))
		.filter_map(|rest| rest.split("::").next())
		.any(|module| !STD_VALUE_TYPES.contains(&module));
	if names_an_unknown_std_module {
		return Some(CapabilityCategory::Unknown);
	}

	None
}

fn map_rune_compile_error(
	error: impl std::fmt::Display,
	config: &RuneSandboxConfig,
) -> FrontendError {
	let message = error.to_string();
	if looks_like_memory_error(&message) {
		return rune_sandbox_error(RuneSandboxViolation::MemoryExhausted {
			bytes_used: config.memory_budget,
			bytes_budget: config.memory_budget,
		});
	}

	rune_compile_error(message)
}

fn map_rune_runtime_error(
	error: impl std::fmt::Display,
	config: &RuneSandboxConfig,
) -> FrontendError {
	let message = error.to_string();
	let lower = message.to_ascii_lowercase();

	if lower.contains("limited") {
		return rune_sandbox_error(RuneSandboxViolation::FuelExhausted {
			fuel_consumed: config.fuel_budget,
			fuel_budget: config.fuel_budget,
		});
	}

	if looks_like_memory_error(&message) {
		return rune_sandbox_error(RuneSandboxViolation::MemoryExhausted {
			bytes_used: config.memory_budget,
			bytes_budget: config.memory_budget,
		});
	}

	if (lower.contains("missing") || lower.contains("not found"))
		&& (lower.contains("main") || lower.contains("entry"))
	{
		return rune_sandbox_error(RuneSandboxViolation::InvalidEntrypoint {
			detail: "main entrypoint is missing or invalid".to_string(),
		});
	}

	if let Some(capability) = classify_denied_capability(&message) {
		return rune_sandbox_error(RuneSandboxViolation::CapabilityDenied { capability });
	}

	rune_runtime_error(message)
}

fn looks_like_memory_error(message: &str) -> bool {
	let lower = message.to_ascii_lowercase();
	lower.contains("memory") || lower.contains("alloc") || lower.contains("allocation")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rune_exp_uses_the_registered_scalar_identifier() {
		let expr = exp(lit_f64(1.0)).expr;
		assert!(matches!(
			expr,
			Expr::ScalarFunction {
				id: ScalarFunctionId::Exp,
				args,
				..
			} if matches!(args.as_slice(), [Expr::Literal(ScalarValue::Float64(Some(value)))] if *value == 1.0)
		));
	}

	#[tokio::test]
	async fn wall_timeout_fires_as_typed_timeout_on_yielding_script() {
		let config = RuneSandboxConfig {
			fuel_budget: 1_000_000,
			wall_timeout: Duration::from_millis(10),
			..RuneSandboxConfig::v1_default()
		};
		let engine = RuneEngine::with_config(config).expect("engine");
		let script = r#"
            pub async fn main(ctx) {
                sandbox_sleep_ms(100).await;
                ctx.table("sales").count()
            }
        "#;

		let error = engine
			.execute_script(script, &config)
			.await
			.expect_err("yielding script should time out");

		assert!(matches!(
			error,
			FrontendError::RuneSandbox {
				kind: RuneSandboxViolation::Timeout { .. },
				..
			}
		));
	}
}
