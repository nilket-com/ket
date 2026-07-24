//! Polariton's Rune host: the capability surface shared by plain Rune buffers
//! and notebook Rune cells.
//!
//! This is intentionally separate from libaryon's Rune host. Baryon scripts
//! configure and automate the editor; scripts here describe Polariton data
//! products. The only cross-boundary values are owned snapshots and deferred
//! plot requests, never a borrow of `AppState` or a live database
//! (`plans/wip/rune-data-products.md`).

use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use crate::plot::request::{PlotRequest, PlotSource};
use polars::frame::DataFrame;
use rune::runtime::{Value, VmResult};
use rune::{Any, Context, Module, Source, Sources, Vm};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Everything one Rune execution produced. The two channels are independent:
/// the trailing expression goes to the Results surface, every `.show()?` adds
/// a request, and neither depends on the other.
#[derive(Debug)]
pub struct RuneExecution {
	pub display: Option<String>,
	pub requests: Vec<PlotRequest>,
}

/// The owned snapshots one execution may read, cloned before the VM runs.
/// Rune never receives editor state, a registry handle, or a database
/// connection.
#[derive(Default)]
pub struct RuneCapabilities {
	/// Explicitly named notebook SQL-cell snapshots (`{#name}` fences), read
	/// with `workspace.cell("name")`.
	pub cells: HashMap<String, Arc<DataFrame>>,
	/// Session results published by plain SQL buffers (`-- @result name`),
	/// read with `workspace.result("name")`.
	pub results: HashMap<String, Arc<DataFrame>>,
}

#[derive(Default)]
struct RequestQueue {
	active: bool,
	requests: Vec<PlotRequest>,
}

impl RequestQueue {
	fn push(&mut self, request: PlotRequest) -> Result<(), String> {
		if !self.active {
			return Err("Plot handle expired".to_owned());
		}
		self.requests.push(request);
		Ok(())
	}

	fn finish(&mut self) -> Vec<PlotRequest> {
		self.active = false;
		std::mem::take(&mut self.requests)
	}
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Workspace)]
struct RuneWorkspace {
	cells: Arc<HashMap<String, Arc<DataFrame>>>,
	results: Arc<HashMap<String, Arc<DataFrame>>>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Cell)]
struct RuneCell {
	name: String,
	data: Arc<DataFrame>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Viz)]
struct RuneViz {
	queue: Arc<Mutex<RequestQueue>>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Plot)]
struct RunePlot {
	cell: RuneCell,
	composition: PlotComposition,
	projection: Option<(String, String)>,
	queue: Arc<Mutex<RequestQueue>>,
}

/// Execute one Rune module against immutable owned snapshots.
///
/// `main(workspace, viz)` is deliberately the entire initial language. The
/// caller owns the returned execution and decides where its display and
/// requests render.
pub fn execute(source: &str, capabilities: RuneCapabilities) -> Result<RuneExecution, String> {
	let queue = Arc::new(Mutex::new(RequestQueue {
		active: true,
		requests: Vec::new(),
	}));
	let workspace = RuneWorkspace {
		cells: Arc::new(capabilities.cells),
		results: Arc::new(capabilities.results),
	};
	let viz = RuneViz {
		queue: queue.clone(),
	};
	let context = build_context()?;
	let runtime = Arc::new(
		context
			.runtime()
			.map_err(|error| format!("Rune runtime context error: {error}"))?,
	);
	let mut sources = Sources::new();
	sources
		.insert(Source::new("polariton.rn", source).map_err(|error| error.to_string())?)
		.map_err(|error| error.to_string())?;
	let unit = rune::prepare(&mut sources)
		.with_context(&context)
		.build()
		.map_err(|error| format!("Rune compile error: {error}"))?;
	let mut vm = Vm::new(runtime, Arc::new(unit));
	let result = vm
		.call(["main"], (workspace, viz))
		.map_err(|error| format!("Rune runtime error: {error}"))
		.and_then(decode_return);
	drop(vm);
	let requests = queue
		.lock()
		.map_err(|_| "Plot request queue was poisoned".to_owned())?
		.finish();
	result.map(|display| RuneExecution { display, requests })
}

/// Execute the body of a Rune buffer or fenced notebook cell.
///
/// Authors write the useful part — `viz.plot(...).show()?;` or a trailing
/// expression — rather than ceremony around a VM entry point. The explicit
/// [`execute`] entry point remains available for unit tests and future
/// multi-function modules.
pub fn execute_body(source: &str, capabilities: RuneCapabilities) -> Result<RuneExecution, String> {
	execute(
		&format!("pub fn main(workspace, viz) {{\n{source}\n}}\n"),
		capabilities,
	)
}

fn format_value(value: Value) -> Result<String, String> {
	if let Ok(value) = value.as_signed() {
		return Ok(value.to_string());
	}
	if let Ok(value) = value.as_unsigned() {
		return Ok(value.to_string());
	}
	if let Ok(value) = value.as_float() {
		return Ok(value.to_string());
	}
	if let Ok(value) = value.as_bool() {
		return Ok(value.to_string());
	}
	if let Ok(value) = value.as_char() {
		return Ok(value.to_string());
	}
	if let Ok(value) = value.into_string() {
		return Ok(value.to_string());
	}
	Err("Rune execution returned a value that Polariton cannot display yet".to_owned())
}

/// Map the VM's return value onto the display channel. A script-level
/// `Err(message)` — usually from a `?` on a missing snapshot — fails the
/// execution; unit is silent; anything else must be displayable.
fn decode_return(value: Value) -> Result<Option<String>, String> {
	let original = value.clone();
	match value.downcast::<Result<Value, Value>>() {
		Ok(Ok(value)) => display_value(value),
		Ok(Err(error)) => match error.into_string() {
			Ok(message) => Err(message.to_string()),
			Err(_) => Err("Rune execution returned a non-string error".to_owned()),
		},
		Err(_) => display_value(original),
	}
}

fn display_value(value: Value) -> Result<Option<String>, String> {
	if value.clone().into_unit().is_ok() {
		return Ok(None);
	}
	format_value(value).map(Some)
}

fn build_context() -> Result<Context, String> {
	let mut context =
		Context::with_config(false).map_err(|error| format!("Rune context error: {error}"))?;
	// Rune 0.14's default `std::f64` module intentionally exposes basic
	// arithmetic, powers, roots, rounding, and constants, but not trigonometry.
	// Extend that same namespace so ordinary compute buffers do not need a
	// Polariton-specific math spelling.
	let mut f64_module = Module::with_crate_item("std", ["f64"])
		.map_err(|error| format!("Rune module error: {error}"))?;
	f64_module
		.function("sin", rune_f64_sin)
		.build()
		.map_err(|error| format!("Rune module error: {error}"))?;
	f64_module
		.function("cos", rune_f64_cos)
		.build()
		.map_err(|error| format!("Rune module error: {error}"))?;
	f64_module
		.function("tan", rune_f64_tan)
		.build()
		.map_err(|error| format!("Rune module error: {error}"))?;
	context
		.install(f64_module)
		.map_err(|error| format!("Rune context error: {error}"))?;
	let mut module =
		Module::with_crate("polariton").map_err(|error| format!("Rune module error: {error}"))?;
	module
		.ty::<RuneWorkspace>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.ty::<RuneCell>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.ty::<RuneViz>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.ty::<RunePlot>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("cell", workspace_cell)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("result", workspace_result)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("plot", viz_plot)
		.map_err(|error| format!("Rune module error: {error}"))?;
	// `scatter` is registered raw because it accepts either arity: `.scatter()`
	// keeps column inference, `.scatter("x", "y")` pins an explicit projection.
	module
		.raw_function("scatter", plot_scatter)
		.build_associated::<RunePlot>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("line", plot_line)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("show", plot_show)
		.map_err(|error| format!("Rune module error: {error}"))?;
	context
		.install(module)
		.map_err(|error| format!("Rune context error: {error}"))?;
	Ok(context)
}

fn rune_number(value: Value) -> VmResult<f64> {
	if let Ok(value) = value.as_float() {
		return VmResult::Ok(value);
	}
	if let Ok(value) = value.as_signed() {
		return VmResult::Ok(value as f64);
	}
	if let Ok(value) = value.as_unsigned() {
		return VmResult::Ok(value as f64);
	}
	VmResult::panic("std::f64 trigonometry expects a numeric argument")
}

fn rune_f64_sin(value: Value) -> VmResult<f64> {
	let value = rune::vm_try!(rune_number(value));
	VmResult::Ok(value.sin())
}

fn rune_f64_cos(value: Value) -> VmResult<f64> {
	let value = rune::vm_try!(rune_number(value));
	VmResult::Ok(value.cos())
}

fn rune_f64_tan(value: Value) -> VmResult<f64> {
	let value = rune::vm_try!(rune_number(value));
	VmResult::Ok(value.tan())
}

fn workspace_cell(workspace: &RuneWorkspace, name: &str) -> VmResult<Result<RuneCell, String>> {
	let Some(data) = workspace.cells.get(name) else {
		return VmResult::Ok(Err(format!("Unknown notebook cell: {name}")));
	};
	VmResult::Ok(Ok(RuneCell {
		name: name.to_owned(),
		data: data.clone(),
	}))
}

fn workspace_result(workspace: &RuneWorkspace, name: &str) -> VmResult<Result<RuneCell, String>> {
	let Some(data) = workspace.results.get(name) else {
		return VmResult::Ok(Err(format!(
			"No published result named {name}; publish one with `-- @result {name}` in a SQL buffer"
		)));
	};
	VmResult::Ok(Ok(RuneCell {
		name: name.to_owned(),
		data: data.clone(),
	}))
}

fn viz_plot(viz: &RuneViz, cell: RuneCell) -> RunePlot {
	RunePlot {
		cell,
		composition: PlotComposition::single(PlotType::Scatter),
		projection: None,
		queue: viz.queue.clone(),
	}
}

fn plot_scatter(
	stack: &mut dyn rune::runtime::Memory,
	addr: rune::runtime::InstAddress,
	args: usize,
	out: rune::runtime::Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let mut plot = match values.first().map(Value::borrow_ref::<RunePlot>) {
		Some(Ok(plot)) => plot.clone(),
		_ => return VmResult::panic("scatter must be called on a plot"),
	};
	plot.composition.set_single(PlotType::Scatter);
	match values {
		[_] => {}
		[_, x, y] => {
			let (Ok(x), Ok(y)) = (
				rune::from_value::<String>(x.clone()),
				rune::from_value::<String>(y.clone()),
			) else {
				return VmResult::panic("scatter column names must be strings");
			};
			plot.projection = Some((x, y));
		}
		_ => {
			return VmResult::panic(
				"scatter takes no arguments or two column names: .scatter() or .scatter(\"x\", \"y\")",
			);
		}
	}
	rune::vm_try!(out.store(stack, || rune::to_value(plot)));
	VmResult::Ok(())
}

fn plot_line(mut plot: RunePlot) -> RunePlot {
	let _ = plot.composition.add_layer(PlotType::Line);
	plot
}

fn plot_show(plot: RunePlot) -> VmResult<Result<(), String>> {
	let request = PlotRequest {
		source: PlotSource::Materialized {
			name: plot.cell.name,
			data: plot.cell.data,
		},
		composition: plot.composition,
		projection: plot.projection,
	};
	match plot
		.queue
		.lock()
		.map_err(|_| "Plot request queue was poisoned".to_owned())
		.and_then(|mut queue| queue.push(request))
	{
		Ok(()) => VmResult::Ok(Ok(())),
		Err(error) => VmResult::Ok(Err(error)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::df;

	fn cells(name: &str, data: DataFrame) -> RuneCapabilities {
		RuneCapabilities {
			cells: HashMap::from([(name.to_owned(), Arc::new(data))]),
			..RuneCapabilities::default()
		}
	}

	fn results(name: &str, data: DataFrame) -> RuneCapabilities {
		RuneCapabilities {
			results: HashMap::from([(name.to_owned(), Arc::new(data))]),
			..RuneCapabilities::default()
		}
	}

	#[test]
	fn plain_body_reports_its_trailing_expression() {
		let execution =
			execute_body("9 * 9", RuneCapabilities::default()).expect("arithmetic executes");
		assert_eq!(execution.display.as_deref(), Some("81"));
		assert!(execution.requests.is_empty());
	}

	#[test]
	fn plain_body_has_trigonometry_in_std_f64() {
		let execution = execute_body("std::f64::sin(2)", RuneCapabilities::default())
			.expect("trigonometry executes");
		let value = execution
			.display
			.expect("displayed float")
			.parse::<f64>()
			.expect("parsable float");
		assert!((value - 2.0_f64.sin()).abs() < f64::EPSILON);
	}

	#[test]
	fn unit_return_displays_nothing() {
		let execution =
			execute_body("let _x = 1;", RuneCapabilities::default()).expect("unit body executes");
		assert_eq!(execution.display, None);
	}

	#[test]
	fn cell_publishes_a_layered_plot_request() {
		let data = df!("x" => [1_i32, 2, 3], "y" => [2_i32, 4, 8]).unwrap();
		let execution = execute(
			r#"
				pub fn main(workspace, viz) {
					let trips = workspace.cell("trips")?;
					viz.plot(trips).scatter().line().show()?;
				}
			"#,
			cells("trips", data),
		)
		.expect("Rune notebook cell runs");

		assert_eq!(execution.requests.len(), 1);
		let PlotSource::Materialized { name, data } = &execution.requests[0].source;
		assert_eq!(name, "trips");
		assert_eq!(data.height(), 3);
		assert!(
			execution.requests[0]
				.composition
				.is_scatter_with_line_overlay()
		);
	}

	#[test]
	fn unknown_cell_reports_at_the_rune_call_site() {
		let error = execute(
			r#"
				pub fn main(workspace, viz) {
					viz.plot(workspace.cell("missing")?).show()?;
				}
			"#,
			RuneCapabilities::default(),
		)
		.expect_err("missing cell is a script error");
		assert!(error.contains("Unknown notebook cell: missing"));
	}

	#[test]
	fn body_needs_no_main_boilerplate() {
		let data = df!("x" => [1_i32], "y" => [2_i32]).expect("frame");
		let execution = execute_body(
			r#"
let trips = workspace.cell("trips")?;
viz.plot(trips).scatter().line().show()?;
"#,
			cells("trips", data),
		)
		.expect("direct body executes");
		assert_eq!(execution.requests.len(), 1);
	}

	#[test]
	fn published_results_resolve_through_workspace_result() {
		let data = df!("x" => [1_i32, 3], "y" => [2_i32, 4]).expect("frame");
		let execution = execute_body(
			r#"viz.plot(workspace.result("test")?).scatter().show()?;"#,
			results("test", data),
		)
		.expect("published result plots");
		assert_eq!(execution.requests.len(), 1);
		let PlotSource::Materialized { name, .. } = &execution.requests[0].source;
		assert_eq!(name, "test");
	}

	#[test]
	fn missing_result_names_the_publishing_annotation() {
		let error = execute_body(
			r#"viz.plot(workspace.result("test")?).show()?;"#,
			RuneCapabilities::default(),
		)
		.expect_err("missing result is a script error");
		assert!(
			error.contains("-- @result test"),
			"unhelpful error: {error}"
		);
	}

	#[test]
	fn explicit_scatter_columns_become_the_projection() {
		let data = df!("x" => [1_i32], "y" => [2_i32], "z" => [3_i32]).expect("frame");
		let execution = execute_body(
			r#"viz.plot(workspace.result("test")?).scatter("z", "y").show()?;"#,
			results("test", data),
		)
		.expect("explicit columns execute");
		assert_eq!(
			execution.requests[0].projection,
			Some(("z".to_owned(), "y".to_owned()))
		);
	}

	#[test]
	fn zero_argument_scatter_keeps_column_inference() {
		let data = df!("x" => [1_i32], "y" => [2_i32]).expect("frame");
		let execution = execute_body(
			r#"viz.plot(workspace.result("test")?).scatter().show()?;"#,
			results("test", data),
		)
		.expect("bare scatter executes");
		assert_eq!(execution.requests[0].projection, None);
	}

	#[test]
	fn one_argument_scatter_is_a_precise_error() {
		let error = execute_body(
			r#"viz.plot(workspace.result("test")?).scatter("x").show()?;"#,
			results("test", df!("x" => [1_i32]).expect("frame")),
		)
		.expect_err("one-argument scatter is rejected");
		assert!(
			error.contains("two column names"),
			"unhelpful error: {error}"
		);
	}

	#[test]
	fn display_and_requests_are_independent_channels() {
		let data = df!("x" => [1_i32], "y" => [2_i32]).expect("frame");
		let execution = execute_body(
			r#"
viz.plot(workspace.result("test")?).scatter().show()?;
"plotted test"
"#,
			results("test", data),
		)
		.expect("plot plus trailing expression executes");
		assert_eq!(execution.display.as_deref(), Some("plotted test"));
		assert_eq!(execution.requests.len(), 1);
	}
}
