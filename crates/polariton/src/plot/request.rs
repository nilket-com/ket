//! The staged plot pipeline: `PlotRequest → plan → ResolvedPlot → PlotKernel`.
//!
//! A Rune execution emits declarative [`PlotRequest`]s; the host resolves them
//! into render-ready [`ResolvedPlot`]s after the VM has returned. Keeping the
//! stages as distinct types is what makes it impossible to treat an unresolved
//! source like an in-memory frame once deferred database relations arrive
//! (`plans/wip/rune-data-products.md`).

use crate::plot::composition::PlotComposition;
use polars::frame::DataFrame;
use std::sync::Arc;

/// Where a requested plot's data comes from.
///
/// `Materialized` is the only variant until deferred `db.table(...)` relations
/// land; the enum exists now so every consumer already distinguishes owned
/// snapshots from sources that still require planning.
#[derive(Clone, Debug)]
pub enum PlotSource {
	Materialized { name: String, data: Arc<DataFrame> },
}

/// A plot published by a Rune execution, not yet validated or fetched.
#[derive(Clone, Debug)]
pub struct PlotRequest {
	pub source: PlotSource,
	pub composition: PlotComposition,
	/// Explicit `(x, y)` column projection from `.scatter("x", "y")`. `None`
	/// keeps the factory's column inference over the full frame.
	pub projection: Option<(String, String)>,
}

/// A plan-approved plot: an owned frame the kernel factory can consume.
#[derive(Clone, Debug)]
pub struct ResolvedPlot {
	pub source_name: String,
	pub composition: PlotComposition,
	pub data: Arc<DataFrame>,
}

/// Resolve a request into a render-ready plot.
///
/// For a materialized source this validates and applies the explicit column
/// projection. Deferred relation sources will make this the seam where the
/// host plans, gates, and fetches — which is why it runs after the VM returns
/// and never inside it.
pub fn plan(request: PlotRequest) -> Result<ResolvedPlot, String> {
	match request.source {
		PlotSource::Materialized { name, data } => {
			let data = match request.projection {
				None => data,
				Some((x, y)) => {
					let projected = data.select([x.as_str(), y.as_str()]).map_err(|error| {
						format!("Cannot project ({x}, {y}) from {name}: {error}")
					})?;
					Arc::new(projected)
				}
			};
			Ok(ResolvedPlot {
				source_name: name,
				composition: request.composition,
				data,
			})
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::core::PlotType;
	use polars::df;

	fn materialized(name: &str, data: DataFrame) -> PlotSource {
		PlotSource::Materialized {
			name: name.to_owned(),
			data: Arc::new(data),
		}
	}

	#[test]
	fn plan_without_projection_keeps_the_full_frame() {
		let frame = df!("x" => [1_i32], "y" => [2_i32], "z" => [3_i32]).expect("frame");
		let resolved = plan(PlotRequest {
			source: materialized("test", frame),
			composition: PlotComposition::single(PlotType::Scatter),
			projection: None,
		})
		.expect("plan succeeds");
		assert_eq!(resolved.source_name, "test");
		assert_eq!(resolved.data.width(), 3);
	}

	#[test]
	fn plan_applies_an_explicit_projection_in_order() {
		let frame = df!("x" => [1_i32], "y" => [2_i32], "z" => [3_i32]).expect("frame");
		let resolved = plan(PlotRequest {
			source: materialized("test", frame),
			composition: PlotComposition::single(PlotType::Scatter),
			projection: Some(("z".to_owned(), "x".to_owned())),
		})
		.expect("plan succeeds");
		let names: Vec<&str> = resolved
			.data
			.get_column_names()
			.iter()
			.map(|name| name.as_str())
			.collect();
		assert_eq!(names, ["z", "x"]);
	}

	#[test]
	fn plan_reports_a_missing_projection_column() {
		let frame = df!("x" => [1_i32]).expect("frame");
		let error = plan(PlotRequest {
			source: materialized("test", frame),
			composition: PlotComposition::single(PlotType::Scatter),
			projection: Some(("x".to_owned(), "missing".to_owned())),
		})
		.expect_err("missing column fails planning");
		assert!(error.contains("missing"), "unhelpful error: {error}");
		assert!(
			error.contains("test"),
			"error must name the source: {error}"
		);
	}
}
