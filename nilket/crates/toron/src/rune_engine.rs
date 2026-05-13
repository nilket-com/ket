use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{AggregateFunction, Expr, LogicalPlan, ScalarValue};
use rune::{Any, Context, Diagnostics, Module, Source, Sources, Vm};
use std::sync::Arc;

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

#[derive(Any, Debug, Clone)]
pub struct RuneDataFrame {
	pub(crate) plan: LogicalPlan,
}

#[derive(Any, Debug, Clone)]
pub struct RuneExpr {
	pub(crate) expr: Expr,
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
		Self {
			plan: LogicalPlan::Project {
				input: Box::new(self.plan),
				exprs: exprs.into_iter().map(|e| e.expr).collect(),
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
				aggr_expr: vec![(
					AggregateFunction::CountAll,
					vec![Expr::Column("count".to_string())],
				)],
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
}

pub fn col(name: String) -> RuneExpr {
	RuneExpr {
		expr: Expr::Column(name),
	}
}

pub fn lit(val: i32) -> RuneExpr {
	RuneExpr {
		expr: Expr::Literal(ScalarValue::Int32(Some(val))),
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

impl RuneExpr {
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
}

pub fn create_toron_module() -> Result<Module, rune::compile::ContextError> {
	let mut m = Module::new();
	m.ty::<RuneDataFrame>()?;
	m.ty::<RuneExpr>()?;
	m.ty::<RuneAggr>()?;
	m.ty::<ToronContext>()?;

	m.function(["col"], col).build()?;
	m.function(["lit"], lit).build()?;
	m.function(["time_bucket"], time_bucket).build()?;
	m.function(["linear_regress"], linear_regress).build()?;
	m.function(["approx_quantile"], approx_quantile).build()?;

	m.associated_function("table", ToronContext::table)?;
	m.associated_function("filter", RuneDataFrame::filter)?;
	m.associated_function("project", RuneDataFrame::project)?;
	m.associated_function("aggregate", RuneDataFrame::aggregate)?;
	m.associated_function("count", RuneDataFrame::count)?;

	// Expression methods
	m.associated_function("gt", RuneExpr::gt)?;
	m.associated_function("eq", RuneExpr::eq)?;

	Ok(m)
}

pub struct RuneEngine {
	context: Arc<Context>,
	runtime: Arc<rune::runtime::RuntimeContext>,
}

impl RuneEngine {
	pub fn new() -> Result<Self, FrontendError> {
		let mut context = Context::with_default_modules().map_err(rune_compile_error)?;
		context
			.install(create_toron_module().map_err(rune_compile_error)?)
			.map_err(rune_compile_error)?;

		let runtime = Arc::new(context.runtime().map_err(rune_compile_error)?);
		Ok(Self {
			context: Arc::new(context),
			runtime,
		})
	}

	pub async fn execute_script(
		&self,
		script: &str,
		_fuel: u64,
	) -> Result<LogicalPlan, FrontendError> {
		let mut sources = Sources::new();
		sources
			.insert(Source::new("script", script).map_err(rune_compile_error)?)
			.map_err(rune_compile_error)?;

		let mut diagnostics = Diagnostics::new();

		let unit = rune::prepare(&mut sources)
			.with_context(&self.context)
			.with_diagnostics(&mut diagnostics)
			.build();

		if !diagnostics.is_empty() {
			return Err(FrontendError::RuneCompile {
				message: format!("{diagnostics:?}"),
			});
		}

		let unit = unit.map_err(rune_compile_error)?;

		let mut vm = Vm::new(self.runtime.clone(), Arc::new(unit));
		// vm.set_fuel(Some(_fuel));

		let mut execution = vm
			.execute(["main"], (ToronContext {},))
			.map_err(rune_runtime_error)?;

		let output = execution
			.async_complete()
			.await
			.into_result()
			.map_err(rune_runtime_error)?;

		let df: RuneDataFrame = rune::from_value(output).map_err(rune_runtime_error)?;
		Ok(df.plan)
	}
}
