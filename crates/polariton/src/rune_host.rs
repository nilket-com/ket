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
use crate::plot::spec::AuthoredPlotSpec;
use crate::result_artifact::ResultArtifact;
use rune::runtime::{Value, VmResult};
use rune::{Any, Context, Module, Source, Sources, Vm};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

/// A completed host-side data export, carried explicitly to the interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportReceipt {
	pub path: String,
	pub rows: usize,
}

impl ExportReceipt {
	pub fn status_message(&self) -> String {
		format!(
			"Exported {} row{} to {}.",
			self.rows,
			if self.rows == 1 { "" } else { "s" },
			self.path
		)
	}
}

/// Everything one Rune execution produced. The two channels are independent:
/// the trailing expression goes to the Results surface, every `.show()?` adds
/// a request, and neither depends on the other.
#[derive(Debug)]
pub struct RuneExecution {
	pub display: Option<String>,
	pub requests: Vec<PlotRequest>,
	/// Each request's place in the run's output sequence, shared with
	/// `views` sets, so a local batch keeps the order the source produced.
	pub request_order: Vec<usize>,
	pub receipts: Vec<ExportReceipt>,
	/// A local run's `frame.views([...])` sets, in call order.
	#[cfg(feature = "toron")]
	pub views: Vec<LocalViewSet>,
}

/// One `frame.views([...])` call: the frame and its named views, decoded
/// through the shared presentation vocabulary.
#[cfg(feature = "toron")]
#[derive(Debug)]
pub struct LocalViewSet {
	/// Its place in the run's output sequence (see `request_order`).
	pub order: usize,
	pub artifact: Arc<ResultArtifact>,
	pub views: Vec<crate::rune_relation_envelope::SpannedRuneViewIntent>,
	/// The source's name and original text, to locate resolution errors.
	pub source_name: String,
	pub source: Arc<str>,
}

/// Where a local run's views are queued. Present only in a local run: a
/// buffer or notebook cell has no queue, and `views` refuses there.
#[cfg_attr(not(feature = "toron"), allow(dead_code))]
pub(crate) struct ViewQueue {
	/// The original source text, for locating a view's diagnostics.
	pub(crate) source: Arc<str>,
	pub(crate) name: String,
	/// Every name used so far in this run; a view name is unique per run.
	pub(crate) names: std::collections::HashSet<String>,
	/// The run's output sequence, shared with plot requests.
	pub(crate) sequence: Arc<std::sync::atomic::AtomicUsize>,
	#[cfg(feature = "toron")]
	pub(crate) sets: Vec<LocalViewSet>,
}

/// The owned snapshots one execution may read, cloned before the VM runs.
/// Rune never receives editor state, a registry handle, or a database
/// connection.
#[derive(Default)]
pub struct RuneCapabilities {
	/// Explicitly named notebook SQL-cell snapshots (`{#name}` fences), read
	/// with `workspace.cell("name")`.
	pub cells: HashMap<String, Arc<ResultArtifact>>,
	/// Session results published by plain SQL buffers (`-- @result name`),
	/// read with `workspace.result("name")`.
	pub results: HashMap<String, Arc<ResultArtifact>>,
}

#[derive(Default)]
struct RequestQueue {
	active: bool,
	requests: Vec<PlotRequest>,
	order: Vec<usize>,
	sequence: Arc<std::sync::atomic::AtomicUsize>,
}

impl RequestQueue {
	fn push(&mut self, request: PlotRequest) -> Result<(), String> {
		if !self.active {
			return Err("Plot handle expired".to_owned());
		}
		self.requests.push(request);
		self.order.push(
			self.sequence
				.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
		);
		Ok(())
	}

	fn finish(&mut self) -> (Vec<PlotRequest>, Vec<usize>) {
		self.active = false;
		(
			std::mem::take(&mut self.requests),
			std::mem::take(&mut self.order),
		)
	}
}

#[derive(Default)]
pub(crate) struct ExportQueue {
	active: bool,
	receipts: Vec<ExportReceipt>,
}

impl ExportQueue {
	fn push(&mut self, receipt: ExportReceipt) -> Result<(), String> {
		if !self.active {
			return Err("Export handle expired".to_owned());
		}
		self.receipts.push(receipt);
		Ok(())
	}

	fn finish(&mut self) -> Vec<ExportReceipt> {
		self.active = false;
		std::mem::take(&mut self.receipts)
	}
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Workspace)]
pub(crate) struct RuneWorkspace {
	cells: Arc<HashMap<String, Arc<ResultArtifact>>>,
	results: Arc<HashMap<String, Arc<ResultArtifact>>>,
	receipts: Arc<Mutex<ExportQueue>>,
	views: Option<Arc<Mutex<ViewQueue>>>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Cell)]
pub(crate) struct RuneCell {
	pub(crate) name: String,
	pub(crate) artifact: Arc<ResultArtifact>,
	pub(crate) receipts: Arc<Mutex<ExportQueue>>,
	pub(crate) views: Option<Arc<Mutex<ViewQueue>>>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Viz)]
pub(crate) struct RuneViz {
	queue: Arc<Mutex<RequestQueue>>,
	pub(crate) receipts: Arc<Mutex<ExportQueue>>,
	pub(crate) views: Option<Arc<Mutex<ViewQueue>>>,
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Plot)]
struct RunePlot {
	cell: RuneCell,
	composition: PlotComposition,
	projection: Option<(String, String)>,
	/// A column whose values name the series each row belongs to.
	series: Option<String>,
	queue: Arc<Mutex<RequestQueue>>,
}

/// Execute one Rune module against immutable owned snapshots.
///
/// `main(workspace, viz)` is deliberately the entire initial language. The
/// caller owns the returned execution and decides where its display and
/// requests render.
pub fn execute(source: &str, capabilities: RuneCapabilities) -> Result<RuneExecution, String> {
	let surface = Surface::new(capabilities);
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
	let mut diagnostics = rune::Diagnostics::new();
	let unit = rune::prepare(&mut sources)
		.with_context(&context)
		.with_diagnostics(&mut diagnostics)
		.build()
		.map_err(|error| {
			let details = diagnostics
				.diagnostics()
				.iter()
				.filter_map(|diagnostic| match diagnostic {
					rune::diagnostics::Diagnostic::Fatal(error) => Some(error.to_string()),
					_ => None,
				})
				.take(8)
				.collect::<Vec<_>>();
			if details.is_empty() {
				format!("Rune compile error: {error}")
			} else {
				format!("Rune compile error: {}", details.join("\n"))
			}
		})?;
	let mut vm = Vm::new(runtime, Arc::new(unit));
	let result = vm
		.call(["main"], (surface.workspace.clone(), surface.viz.clone()))
		.map_err(|error| format!("Rune runtime error: {error}"))
		.and_then(decode_return);
	drop(vm);
	surface.finish(result)
}

/// The `workspace` and `viz` values one execution receives, and the queues
/// they fill. Owned per execution; `finish` drains what the script published.
pub(crate) struct Surface {
	pub(crate) workspace: RuneWorkspace,
	pub(crate) viz: RuneViz,
	queue: Arc<Mutex<RequestQueue>>,
	receipts: Arc<Mutex<ExportQueue>>,
	#[cfg_attr(not(feature = "toron"), allow(dead_code))]
	views: Option<Arc<Mutex<ViewQueue>>>,
}

impl Surface {
	pub(crate) fn new(capabilities: RuneCapabilities) -> Self {
		Self::with_views(capabilities, None, Arc::default())
	}

	/// A local run's surface: frames it builds can queue views.
	pub(crate) fn local(capabilities: RuneCapabilities, name: &str, source: &str) -> Self {
		let sequence = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		Self::with_views(
			capabilities,
			Some(Arc::new(Mutex::new(ViewQueue {
				source: Arc::from(source),
				name: name.to_owned(),
				names: std::collections::HashSet::new(),
				sequence: Arc::clone(&sequence),
				#[cfg(feature = "toron")]
				sets: Vec::new(),
			}))),
			sequence,
		)
	}

	fn with_views(
		capabilities: RuneCapabilities,
		views: Option<Arc<Mutex<ViewQueue>>>,
		sequence: Arc<std::sync::atomic::AtomicUsize>,
	) -> Self {
		let queue = Arc::new(Mutex::new(RequestQueue {
			active: true,
			requests: Vec::new(),
			order: Vec::new(),
			sequence,
		}));
		let receipts = Arc::new(Mutex::new(ExportQueue {
			active: true,
			receipts: Vec::new(),
		}));
		Self {
			workspace: RuneWorkspace {
				cells: Arc::new(capabilities.cells),
				results: Arc::new(capabilities.results),
				receipts: Arc::clone(&receipts),
				views: views.clone(),
			},
			viz: RuneViz {
				queue: Arc::clone(&queue),
				receipts: Arc::clone(&receipts),
				views: views.clone(),
			},
			queue,
			receipts,
			views,
		}
	}

	/// Close both queues and pair what they hold with the script's outcome.
	pub(crate) fn finish(
		self,
		display: Result<Option<String>, String>,
	) -> Result<RuneExecution, String> {
		let (requests, request_order) = self
			.queue
			.lock()
			.map_err(|_| "Plot request queue was poisoned".to_owned())?
			.finish();
		let receipts = self
			.receipts
			.lock()
			.map_err(|_| "Export receipt queue was poisoned".to_owned())?
			.finish();
		#[cfg(feature = "toron")]
		let views = match &self.views {
			Some(queue) => std::mem::take(
				&mut queue
					.lock()
					.map_err(|_| "View queue was poisoned".to_owned())?
					.sets,
			),
			None => Vec::new(),
		};
		display.map(|display| RuneExecution {
			display,
			requests,
			request_order,
			receipts,
			#[cfg(feature = "toron")]
			views,
		})
	}
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
pub(crate) fn decode_return(value: Value) -> Result<Option<String>, String> {
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
	register(&mut module)?;
	context
		.install(module)
		.map_err(|error| format!("Rune context error: {error}"))?;
	Ok(context)
}

/// Register the `polariton` crate's types and functions on `module`. Shared
/// by the plain context above and the rnx-hosted local context
/// (`rune_local`), so both sources see one capability surface.
pub(crate) fn register(module: &mut Module) -> Result<(), String> {
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
	module
		.associated_function("export_csv", cell_export_csv)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("frame", crate::rune_local::viz_frame)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("frame_typed", crate::rune_local::viz_frame_typed)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("views", crate::rune_local::cell_views)
		.map_err(|error| format!("Rune module error: {error}"))?;
	Ok(())
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
		artifact: data.clone(),
		receipts: Arc::clone(&workspace.receipts),
		views: workspace.views.clone(),
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
		artifact: data.clone(),
		receipts: Arc::clone(&workspace.receipts),
		views: workspace.views.clone(),
	}))
}

fn viz_plot(viz: &RuneViz, cell: RuneCell) -> RunePlot {
	RunePlot {
		cell,
		composition: PlotComposition::single(PlotType::Scatter),
		projection: None,
		series: None,
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
		[_, x, y, series] => {
			let (Ok(x), Ok(y), Ok(series)) = (
				rune::from_value::<String>(x.clone()),
				rune::from_value::<String>(y.clone()),
				rune::from_value::<String>(series.clone()),
			) else {
				return VmResult::panic("scatter column names must be strings");
			};
			plot.projection = Some((x, y));
			plot.series = Some(series);
		}
		_ => {
			return VmResult::panic(
				"scatter takes no arguments, two column names, or three: .scatter(), .scatter(\"x\", \"y\") or .scatter(\"x\", \"y\", \"series\")",
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
	let authored_spec = match plot.projection {
		Some((x, y)) => AuthoredPlotSpec::xy_color(plot.composition, x, y, plot.series),
		None => AuthoredPlotSpec::automatic(plot.composition),
	};
	let data = match plot.cell.artifact.projection() {
		Ok(data) => data,
		Err(error) => return VmResult::Ok(Err(error.to_string())),
	};
	let request = PlotRequest {
		source: PlotSource::Materialized {
			name: plot.cell.name,
			data: ResultArtifact::from_polars_derived(data, &plot.cell.artifact),
		},
		authored_spec,
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

fn cell_export_csv(cell: RuneCell, path: String) -> VmResult<Result<(), String>> {
	let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
		Ok(file) => file,
		Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
			return VmResult::Ok(Err(format!("CSV export refused: {path} already exists")));
		}
		Err(error) => return VmResult::Ok(Err(format!("CSV export failed for {path}: {error}"))),
	};
	let rows = match cell.artifact.write_csv(file) {
		Ok(rows) => rows,
		Err(error) => return VmResult::Ok(Err(format!("CSV export failed for {path}: {error}"))),
	};
	let receipt = ExportReceipt { path, rows };
	match cell
		.receipts
		.lock()
		.map_err(|_| "Export receipt queue was poisoned".to_owned())
		.and_then(|mut queue| queue.push(receipt))
	{
		Ok(()) => VmResult::Ok(Ok(())),
		Err(error) => VmResult::Ok(Err(error)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::df;
	use polars::frame::DataFrame;
	use std::sync::atomic::{AtomicU64, Ordering};

	static NEXT_EXPORT_FIXTURE: AtomicU64 = AtomicU64::new(1);

	/// The path as a Rune string literal body: Windows separators are escapes.
	fn rune_literal(path: &std::path::Path) -> String {
		path.display().to_string().replace('\\', "\\\\")
	}

	fn export_path(label: &str) -> std::path::PathBuf {
		std::env::temp_dir().join(format!(
			"polariton-product-0136-{label}-{}-{}.csv",
			std::process::id(),
			NEXT_EXPORT_FIXTURE.fetch_add(1, Ordering::Relaxed)
		))
	}

	fn cells(name: &str, data: DataFrame) -> RuneCapabilities {
		RuneCapabilities {
			cells: HashMap::from([(name.to_owned(), ResultArtifact::from_polars(Arc::new(data)))]),
			..RuneCapabilities::default()
		}
	}

	fn results(name: &str, data: DataFrame) -> RuneCapabilities {
		RuneCapabilities {
			results: HashMap::from([(
				name.to_owned(),
				ResultArtifact::from_polars(Arc::new(data)),
			)]),
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
	fn compile_failure_reports_the_unresolved_plot_function() {
		let error = execute_body("missing_plot_function();", RuneCapabilities::default())
			.expect_err("unknown plot function must fail compilation");
		assert!(error.contains("missing_plot_function"), "{error}");
		assert!(!error.contains("see diagnostics for details"), "{error}");
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
				.authored_spec
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
			execution.requests[0].authored_spec.x.field,
			crate::plot::spec::AuthoredChannel::Named("z".to_owned())
		);
		assert_eq!(
			execution.requests[0].authored_spec.y.field,
			crate::plot::spec::AuthoredChannel::Named("y".to_owned())
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
		assert_eq!(
			execution.requests[0].authored_spec.x.field,
			crate::plot::spec::AuthoredChannel::Auto
		);
		assert_eq!(
			execution.requests[0].authored_spec.y.field,
			crate::plot::spec::AuthoredChannel::Auto
		);
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

	#[test]
	fn product_0136_named_cell_exports_csv_and_reports_a_receipt() {
		let path = export_path("cell");
		let source = format!(
			r#"workspace.cell("trips")?.export_csv("{}")?;"#,
			rune_literal(&path)
		);
		let execution = execute_body(
			&source,
			cells(
				"trips",
				df!("x" => [1_i32, 2], "text" => ["comma, value", "plain"]).unwrap(),
			),
		)
		.expect("named cell exports");
		assert!(execution.requests.is_empty());
		assert_eq!(execution.receipts.len(), 1);
		assert_eq!(execution.receipts[0].path, path.display().to_string());
		assert_eq!(execution.receipts[0].rows, 2);
		assert_eq!(
			std::fs::read_to_string(&path).expect("CSV exists"),
			"x,text\n1,\"comma, value\"\n2,plain\n"
		);
		std::fs::remove_file(path).expect("remove export fixture");
	}

	#[test]
	fn product_0136_named_result_exports_csv() {
		let path = export_path("result");
		let source = format!(
			r#"workspace.result("sales")?.export_csv("{}")?;"#,
			rune_literal(&path)
		);
		let execution = execute_body(&source, results("sales", df!("amount" => [3_i32]).unwrap()))
			.expect("published result exports");
		assert_eq!(execution.receipts[0].rows, 1);
		assert_eq!(
			std::fs::read_to_string(&path).expect("CSV exists"),
			"amount\n3\n"
		);
		std::fs::remove_file(path).expect("remove export fixture");
	}

	#[test]
	fn product_0136_existing_file_refuses_without_clobbering() {
		let path = export_path("existing");
		std::fs::write(&path, "keep me").expect("seed existing file");
		let source = format!(
			r#"workspace.cell("trips")?.export_csv("{}")?;"#,
			rune_literal(&path)
		);
		let error = execute_body(&source, cells("trips", df!("x" => [1_i32]).unwrap()))
			.expect_err("existing file refuses");
		assert!(error.contains("already exists"), "unhelpful error: {error}");
		assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep me");
		std::fs::remove_file(path).expect("remove export fixture");
	}

	#[test]
	fn product_0136_write_failures_surface_through_the_rune_error_path() {
		let parent = std::env::temp_dir().join(format!(
			"polariton-product-0136-no-directory-{}-{}",
			std::process::id(),
			NEXT_EXPORT_FIXTURE.fetch_add(1, Ordering::Relaxed)
		));
		let _ = std::fs::remove_dir_all(&parent);
		let path = parent.join("sales.csv");
		let source = format!(
			r#"workspace.cell("trips")?.export_csv("{}")?;"#,
			rune_literal(&path)
		);
		let error = execute_body(&source, cells("trips", df!("x" => [1_i32]).unwrap()))
			.expect_err("missing parent directory fails visibly");
		assert!(
			error.contains("CSV export failed"),
			"unhelpful error: {error}"
		);
		assert!(!path.exists());
	}

	#[test]
	fn product_0136_missing_result_keeps_the_existing_name_diagnostic() {
		let path = export_path("missing");
		let source = format!(
			r#"workspace.result("absent")?.export_csv("{}")?;"#,
			rune_literal(&path)
		);
		let error = execute_body(&source, RuneCapabilities::default())
			.expect_err("unknown result is rejected before export");
		assert!(
			error.contains("-- @result absent"),
			"unhelpful error: {error}"
		);
		assert!(!path.exists());
	}
}
