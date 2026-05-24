use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{AggregateFunction, Expr, LogicalPlan, ScalarValue};
use crate::sql_planner::SqlPlanner;
use arrow::datatypes::DataType;
use rune::alloc::limit;
use rune::runtime::{VmResult, budget};
use rune::{Any, Context, Diagnostics, Module, Source, Sources, Unit, Vm};
use std::sync::Arc;
use std::time::Duration;

const RUNE_MAX_IN_LIST_ITEMS: usize = 32;

fn rune_compile_error(error: impl std::fmt::Display) -> FrontendError {
	FrontendError::RuneCompile {
		message: error.to_string(),
	}
}

fn rune_runtime_error(error: impl std::fmt::Display) -> FrontendError {
	FrontendError::RuneRuntime {
		message: error.to_string(),
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
pub struct RuneDataType {
	pub(crate) data_type: DataType,
}

impl RuneDataFrame {
	pub fn new(table: String) -> Self {
		Self {
			plan: LogicalPlan::ScanNamed {
				table: TableRef::new(table),
				projection: None,
			},
		}
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

	pub fn aggregate(self, group_expr: Vec<RuneExpr>, aggr_expr: Vec<RuneAggr>) -> Self {
		Self {
			plan: LogicalPlan::Aggregate {
				input: Box::new(self.plan),
				group_expr: group_expr.into_iter().map(|e| e.expr).collect(),
				aggr_expr: aggr_expr.into_iter().map(|a| (a.func, a.args)).collect(),
			},
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

	pub fn sql(&self, query: String) -> VmResult<RuneDataFrame> {
		match SqlPlanner::new().plan_relation(&query) {
			Ok(plan) => VmResult::Ok(RuneDataFrame { plan }),
			Err(error) => VmResult::panic(error.to_string()),
		}
	}
}

pub fn col(name: String) -> RuneExpr {
	RuneExpr {
		expr: Expr::column(name),
	}
}

pub fn lit(val: i32) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Int32(Some(val))),
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

pub fn time_bucket(interval_str: String, ts: RuneExpr) -> RuneExpr {
	let interval = match interval_str.as_str() {
		"1m" => std::time::Duration::from_secs(60),
		"1h" => std::time::Duration::from_secs(3600),
		"1d" => std::time::Duration::from_secs(86400),
		_ => std::time::Duration::from_secs(0),
	};
	RuneExpr {
		expr: Expr::TimeBucket(interval, Box::new(ts.expr)),
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

#[cfg(test)]
pub async fn sandbox_sleep_ms(ms: i64) {
	let ms = u64::try_from(ms).unwrap_or(0);
	tokio::time::sleep(Duration::from_millis(ms)).await;
}

impl RuneExpr {
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

	pub fn in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
		build_in_list(self.expr, list, false)
	}

	pub fn not_in_list(self, list: Vec<RuneExpr>) -> VmResult<RuneExpr> {
		build_in_list(self.expr, list, true)
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

pub fn create_toron_module() -> Result<Module, rune::compile::ContextError> {
	let mut m = Module::new();
	m.ty::<RuneDataFrame>()?;
	m.ty::<RuneExpr>()?;
	m.ty::<RuneNamedExpr>()?;
	m.ty::<RuneDataType>()?;
	m.ty::<RuneAggr>()?;
	m.ty::<ToronContext>()?;

	m.function(["col"], col).build()?;
	m.function(["lit"], lit).build()?;
	m.function(["lit_str"], lit_str).build()?;
	m.function(["lit_f64"], lit_f64).build()?;
	m.function(["lit_bool"], lit_bool).build()?;
	m.function(["lit_i64"], lit_i64).build()?;
	m.function(["type_i32"], type_i32).build()?;
	m.function(["type_i64"], type_i64).build()?;
	m.function(["type_f32"], type_f32).build()?;
	m.function(["type_f64"], type_f64).build()?;
	m.function(["time_bucket"], time_bucket).build()?;
	m.function(["linear_regress"], linear_regress).build()?;
	m.function(["approx_quantile"], approx_quantile).build()?;
	m.function(["count_all"], count_all).build()?;
	m.function(["count"], count).build()?;
	m.function(["sum"], sum).build()?;
	m.function(["avg"], avg).build()?;
	m.function(["min"], min).build()?;
	m.function(["max"], max).build()?;
	#[cfg(test)]
	m.function(["sandbox_sleep_ms"], sandbox_sleep_ms).build()?;

	m.associated_function("table", ToronContext::table)?;
	m.associated_function("sql", ToronContext::sql)?;
	m.associated_function("filter", RuneDataFrame::filter)?;
	m.associated_function("project", RuneDataFrame::project)?;
	m.associated_function("project_named", RuneDataFrame::project_named)?;
	m.associated_function("aggregate", RuneDataFrame::aggregate)?;
	m.associated_function("count", RuneDataFrame::count)?;

	// Expression methods
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
	m.associated_function("in_list", RuneExpr::in_list)?;
	m.associated_function("not_in_list", RuneExpr::not_in_list)?;

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

			return Err(FrontendError::RuneCompile { message });
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
			&["std::random", "rand::", "random("],
		),
		(CapabilityCategory::Threading, &["std::thread", "thread::"]),
	];

	for (capability, patterns) in checks {
		if patterns.iter().any(|pattern| lower.contains(pattern)) {
			return Some(*capability);
		}
	}

	if lower.contains("std::") {
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
