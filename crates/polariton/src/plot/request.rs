//! The staged plot pipeline: `PlotRequest → plan → ResolvedPlot → PlotKernel`.
//!
//! A Rune execution emits declarative [`PlotRequest`]s; the host resolves them
//! into render-ready [`ResolvedPlot`]s after the VM has returned. Keeping the
//! stages as distinct types is what makes it impossible to treat an unresolved
//! source like an in-memory frame once deferred database relations arrive
//! (`plans/wip/rune-data-products.md`).

use crate::plot::composition::PlotComposition;
use crate::plot::spec::{
	AuthoredChannel, AuthoredPlotSpec, ColorIdentity, OptionalAuthoredChannel, PlotProvenance,
	ResolvedPlotSpec, StampedResult, resolve_channels, resolve_domains,
};
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
	pub authored_spec: AuthoredPlotSpec,
}

/// A plan-approved plot: an owned frame the kernel factory can consume.
#[derive(Clone, Debug)]
pub struct ResolvedPlot {
	pub source_name: String,
	/// Compatibility field consumed by the Stage-2 kernel migration. Its value
	/// is identical to `resolved_spec.composition`.
	pub composition: PlotComposition,
	pub data: Arc<DataFrame>,
	#[allow(dead_code)] // Stage 1 carries it; Stage 2 makes kernels consume it.
	pub resolved_spec: ResolvedPlotSpec,
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
			let binding = resolve_channels(&request.authored_spec, data.schema().as_ref())
				.map_err(|error| format!("{name}: {error}"))?;
			let stamped = StampedResult {
				data: Arc::clone(&data),
				provenance: PlotProvenance {
					source_name: name.clone(),
					revision: None,
				},
			};
			let (resolved_spec, _) = resolve_domains(binding, &stamped, &ColorIdentity::default())
				.map_err(|error| format!("{name}: {error}"))?;
			let data = project_named_channels(&request.authored_spec, data, &name)?;
			Ok(ResolvedPlot {
				source_name: name,
				composition: resolved_spec.composition.clone(),
				data,
				resolved_spec,
			})
		}
	}
}

fn project_named_channels(
	spec: &AuthoredPlotSpec,
	data: Arc<DataFrame>,
	source_name: &str,
) -> Result<Arc<DataFrame>, String> {
	let (AuthoredChannel::Named(x), AuthoredChannel::Named(y)) = (&spec.x.field, &spec.y.field)
	else {
		return Ok(data);
	};
	let mut fields = vec![x.as_str()];
	if y != x {
		fields.push(y.as_str());
	}
	if let OptionalAuthoredChannel::Named(color) = &spec.color.field
		&& color != x
		&& color != y
	{
		fields.push(color.as_str());
	}
	let projected = data
		.select(fields)
		.map_err(|error| format!("Cannot project plot channels from {source_name}: {error}"))?;
	Ok(Arc::new(projected))
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
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Scatter)),
		})
		.expect("plan succeeds");
		assert_eq!(resolved.source_name, "test");
		assert_eq!(resolved.data.width(), 3);
		assert_eq!(resolved.resolved_spec.provenance.source_name, "test");
		assert_eq!(
			resolved
				.resolved_spec
				.channel(crate::plot::spec::Channel::X)
				.expect("resolved x")
				.binding
				.field,
			"x"
		);
	}

	#[test]
	fn plan_applies_an_explicit_projection_in_order() {
		let frame = df!("x" => [1_i32], "y" => [2_i32], "z" => [3_i32]).expect("frame");
		let resolved = plan(PlotRequest {
			source: materialized("test", frame),
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"z",
				"x",
			),
		})
		.expect("plan succeeds");
		let names: Vec<&str> = resolved
			.data
			.get_column_names()
			.iter()
			.map(|name| name.as_str())
			.collect();
		assert_eq!(names, ["z", "x"]);
		assert_eq!(
			resolved
				.resolved_spec
				.channel(crate::plot::spec::Channel::X)
				.expect("resolved x")
				.binding
				.field,
			"z"
		);
		assert_eq!(
			resolved
				.resolved_spec
				.channel(crate::plot::spec::Channel::Y)
				.expect("resolved y")
				.binding
				.field,
			"x"
		);
	}

	#[test]
	fn plan_reports_a_missing_projection_column() {
		let frame = df!("x" => [1_i32]).expect("frame");
		let error = plan(PlotRequest {
			source: materialized("test", frame),
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"missing",
			),
		})
		.expect_err("missing column fails planning");
		assert!(error.contains("missing"), "unhelpful error: {error}");
		assert!(
			error.contains("test"),
			"error must name the source: {error}"
		);
	}
}
