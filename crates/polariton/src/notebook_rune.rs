//! Polariton's notebook-local Rune capability surface.
//!
//! This is intentionally separate from libaryon's Rune host. Baryon scripts
//! configure and automate the editor; notebook scripts describe Polariton
//! data products. The only cross-boundary values here are owned snapshots and
//! deferred plot effects, never a borrow of `AppState` or a live database.

use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use polars::frame::DataFrame;
use rune::runtime::{Value, VmResult};
use rune::{Any, Context, Module, Source, Sources, Vm};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A plot request emitted by a Rune cell after the VM has returned.
#[derive(Clone, Debug)]
pub struct PlotEffect {
	pub source_cell: String,
	pub data: Arc<DataFrame>,
	pub composition: PlotComposition,
}

#[derive(Default)]
struct EffectQueue {
	active: bool,
	effects: Vec<PlotEffect>,
}

impl EffectQueue {
	fn push(&mut self, effect: PlotEffect) -> Result<(), String> {
		if !self.active {
			return Err("Notebook handle expired".to_owned());
		}
		self.effects.push(effect);
		Ok(())
	}

	fn finish(&mut self) -> Vec<PlotEffect> {
		self.active = false;
		std::mem::take(&mut self.effects)
	}
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Workspace)]
struct RuneWorkspace {
	cells: Arc<HashMap<String, Arc<DataFrame>>>,
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
	queue: Arc<Mutex<EffectQueue>>,
}

#[derive(Any)]
#[rune(item = ::polariton, name = Plot)]
struct RunePlot {
	cell: RuneCell,
	composition: PlotComposition,
	queue: Arc<Mutex<EffectQueue>>,
}

/// Execute one Rune notebook cell against immutable named result snapshots.
///
/// `main(workspace, viz)` is deliberately the entire initial language. The
/// caller owns the returned effects and decides where they render in a later
/// notebook-dispatch cut.
pub fn execute(
	source: &str,
	cells: HashMap<String, Arc<DataFrame>>,
) -> Result<Vec<PlotEffect>, String> {
	let queue = Arc::new(Mutex::new(EffectQueue {
		active: true,
		effects: Vec::new(),
	}));
	let workspace = RuneWorkspace {
		cells: Arc::new(cells),
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
		.insert(Source::new("notebook.rn", source).map_err(|error| error.to_string())?)
		.map_err(|error| error.to_string())?;
	let unit = rune::prepare(&mut sources)
		.with_context(&context)
		.build()
		.map_err(|error| format!("Rune compile error: {error}"))?;
	let mut vm = Vm::new(runtime, Arc::new(unit));
	let result = vm
		.call(["main"], (workspace, viz))
		.map_err(|error| format!("Rune runtime error: {error}"))
		.and_then(decode_cell_return);
	drop(vm);
	let effects = queue
		.lock()
		.map_err(|_| "Notebook effect queue was poisoned".to_owned())?
		.finish();
	result.map(|_| effects)
}

/// Execute the body of a fenced notebook Rune cell.
///
/// Notebook authors write the useful part — `viz.plot(...).show()?;` — rather
/// than ceremony around a VM entry point. The explicit [`execute`] entry point
/// remains available for unit tests and future multi-function modules.
pub fn execute_cell(
	source: &str,
	cells: HashMap<String, Arc<DataFrame>>,
) -> Result<Vec<PlotEffect>, String> {
	execute(
		&format!("pub fn main(workspace, viz) {{\n{source}\n}}\n"),
		cells,
	)
}

fn decode_cell_return(value: Value) -> Result<(), String> {
	let original = value.clone();
	match value.downcast::<Result<Value, Value>>() {
		Ok(Ok(value)) => value
			.into_unit()
			.map_err(|_| "Rune cell must return () or Result<(), String>".to_owned()),
		Ok(Err(error)) => error
			.into_string()
			.map(|message| Err(message.to_string()))
			.map_err(|_| "Rune cell returned a non-string error".to_owned())?,
		Err(_) => original
			.into_unit()
			.map_err(|_| "Rune cell must return () or Result<(), String>".to_owned()),
	}
}

fn build_context() -> Result<Context, String> {
	let mut context =
		Context::with_config(false).map_err(|error| format!("Rune context error: {error}"))?;
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
		.associated_function("plot", viz_plot)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("scatter", plot_scatter)
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

fn workspace_cell(workspace: &RuneWorkspace, name: &str) -> VmResult<Result<RuneCell, String>> {
	let Some(data) = workspace.cells.get(name) else {
		return VmResult::Ok(Err(format!("Unknown notebook cell: {name}")));
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
		queue: viz.queue.clone(),
	}
}

fn plot_scatter(mut plot: RunePlot) -> RunePlot {
	plot.composition.set_single(PlotType::Scatter);
	plot
}

fn plot_line(mut plot: RunePlot) -> RunePlot {
	let _ = plot.composition.add_layer(PlotType::Line);
	plot
}

fn plot_show(plot: RunePlot) -> VmResult<Result<(), String>> {
	let effect = PlotEffect {
		source_cell: plot.cell.name,
		data: plot.cell.data,
		composition: plot.composition,
	};
	match plot
		.queue
		.lock()
		.map_err(|_| "Notebook effect queue was poisoned".to_owned())
		.and_then(|mut queue| queue.push(effect))
	{
		Ok(()) => VmResult::Ok(Ok(())),
		Err(error) => VmResult::Ok(Err(error)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::df;

	#[test]
	fn rune_cell_publishes_a_layered_plot_effect() {
		let data = Arc::new(df!("x" => [1_i32, 2, 3], "y" => [2_i32, 4, 8]).unwrap());
		let effects = execute(
			r#"
				pub fn main(workspace, viz) {
					let trips = workspace.cell("trips")?;
					viz.plot(trips).scatter().line().show()?;
				}
			"#,
			HashMap::from([("trips".to_owned(), data)]),
		)
		.expect("Rune notebook cell runs");

		assert_eq!(effects.len(), 1);
		assert_eq!(effects[0].source_cell, "trips");
		assert_eq!(effects[0].data.height(), 3);
		assert!(effects[0].composition.is_scatter_with_line_overlay());
	}

	#[test]
	fn unknown_cell_reports_at_the_rune_call_site() {
		let error = execute(
			r#"
				pub fn main(workspace, viz) {
					viz.plot(workspace.cell("missing")?).show()?;
				}
			"#,
			HashMap::new(),
		)
		.expect_err("missing cell is a script error");
		assert!(error.contains("Unknown notebook cell: missing"));
	}

	#[test]
	fn fenced_cell_body_needs_no_main_boilerplate() {
		let data = Arc::new(df!("x" => [1_i32], "y" => [2_i32]).expect("frame"));
		let effects = execute_cell(
			r#"
let trips = workspace.cell("trips")?;
viz.plot(trips).scatter().line().show()?;
"#,
			HashMap::from([("trips".to_owned(), data)]),
		)
		.expect("direct fenced cell body executes");
		assert_eq!(effects.len(), 1);
	}
}
