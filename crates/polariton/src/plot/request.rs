//! The staged plot pipeline: `PlotRequest → plan → ResolvedPlot → PlotKernel`.
//!
//! A Rune execution emits declarative [`PlotRequest`]s; the host resolves them
//! into render-ready [`ResolvedPlot`]s after the VM has returned. Keeping the
//! stages as distinct types is what makes it impossible to treat an unresolved
//! source like an in-memory frame once deferred database relations arrive
//! (`plans/wip/rune-data-products.md`).

use crate::plot::kernels::histogram::prepare_histogram_data;
use crate::plot::spec::{
	AuthoredChannel, AuthoredPlotSpec, CategoricalPaletteSelection, ColorIdentity,
	OptionalAuthoredChannel, PlotProvenance, ResolvedPlotSpec, StampedResult, resolve_channels,
	resolve_domains, resolve_domains_with_categorical_palette,
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
	#[allow(dead_code)] // Retained for session durability; no surface is accepted yet.
	pub authored_spec: Box<AuthoredPlotSpec>,
	pub data: Arc<DataFrame>,
	pub resolved_spec: ResolvedPlotSpec,
}

#[allow(dead_code)] // Conversion is exposed before the gated durable-plot surface.
impl ResolvedPlot {
	pub fn into_live_artifact(self) -> Result<crate::plot::durable::DurablePlot, String> {
		let result = StampedResult {
			data: self.data,
			provenance: self.resolved_spec.provenance.clone(),
		};
		crate::plot::durable::DurablePlot::live(*self.authored_spec, result)
	}

	pub fn into_pinned_artifact(self) -> Result<crate::plot::durable::DurablePlot, String> {
		let result = StampedResult {
			data: self.data,
			provenance: self.resolved_spec.provenance.clone(),
		};
		crate::plot::durable::DurablePlot::pinned(*self.authored_spec, self.resolved_spec, result)
	}
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
			validate_prepared_contract(&resolved_spec, data.as_ref())
				.map_err(|error| format!("{name}: {error}"))?;
			let data = project_named_channels(&request.authored_spec, data, &name)?;
			Ok(ResolvedPlot {
				source_name: name,
				authored_spec: Box::new(request.authored_spec),
				data,
				resolved_spec,
			})
		}
	}
}

/// Resolve one request through an explicit categorical palette and a snapshot
/// of its live continuation state. This substrate owns no Rune default.
pub fn plan_with_categorical_palette(
	request: PlotRequest,
	selection: &CategoricalPaletteSelection,
	identity: &ColorIdentity,
) -> Result<(ResolvedPlot, ColorIdentity), String> {
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
			let (resolved_spec, next_identity) =
				resolve_domains_with_categorical_palette(binding, &stamped, identity, selection)
					.map_err(|error| format!("{name}: {error}"))?;
			validate_prepared_contract(&resolved_spec, data.as_ref())
				.map_err(|error| format!("{name}: {error}"))?;
			let data = project_named_channels(&request.authored_spec, data, &name)?;
			Ok((
				ResolvedPlot {
					source_name: name,
					authored_spec: Box::new(request.authored_spec),
					data,
					resolved_spec,
				},
				next_identity,
			))
		}
	}
}

fn validate_prepared_contract(spec: &ResolvedPlotSpec, data: &DataFrame) -> Result<(), String> {
	if let crate::plot::spec::ResolvedKernelFields::Histogram {
		bin_start,
		bin_end,
		count,
	} = spec.kernel_fields.as_ref()
	{
		prepare_histogram_data(data, bin_start, bin_end, count)?;
	}
	Ok(())
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
	if let Some(end) = spec.histogram_end()
		&& !fields.contains(&end)
	{
		fields.push(end);
	}
	let projected = data
		.select(fields)
		.map_err(|error| format!("Cannot project plot channels from {source_name}: {error}"))?;
	Ok(Arc::new(projected))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::spec::Channel;
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
	fn product_0079_histogram_keeps_a_nonempty_color_identity_unchanged() {
		let selection = CategoricalPaletteSelection::from_theme_name("viridis").unwrap();
		let categorical = polars::df!(
			"category" => ["a", "b"],
			"value" => [1.0, 2.0],
		)
		.unwrap();
		let (_, identity) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized("categorical", categorical),
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Bar),
					"category",
					"value",
				),
			},
			&selection,
			&ColorIdentity::default(),
		)
		.unwrap();
		assert!(!identity.persistence_snapshot().assignments.is_empty());

		let bins = polars::df!(
			"bin_start" => [0.0, 2.0],
			"bin_end" => [1.0, 3.0],
			"count" => [4_i64, 7_i64],
		)
		.unwrap();
		let (resolved, after) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized("bins", bins),
				authored_spec: AuthoredPlotSpec::histogram("bin_start", "bin_end", "count"),
			},
			&selection,
			&identity,
		)
		.unwrap();
		assert_eq!(after, identity);
		assert!(resolved.resolved_spec.band_fills.is_empty());
		assert!(resolved.resolved_spec.channels.iter().all(|channel| {
			channel.binding.channel != crate::plot::spec::Channel::Color
				&& channel.colors.is_empty()
		}));
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

	#[test]
	fn product_0030_banks_the_pre_switch_legacy_request_boundary() {
		let mut actual = String::new();
		let scatter = plan(PlotRequest {
			source: materialized(
				"pre-switch scatter",
				df!(
					"x" => [1_i32, 2, 3, 4, 5, 6],
					"y" => [6_i32, 5, 4, 3, 2, 1],
					"cat" => ["A", "B", "C", "D", "E", "F"],
				)
				.expect("scatter frame"),
			),
			authored_spec: AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
				Some("cat".to_owned()),
			),
		})
		.expect("legacy scatter request");
		let fields = scatter
			.data
			.get_column_names()
			.iter()
			.map(|field| field.as_str())
			.collect::<Vec<_>>()
			.join(",");
		actual.push_str(&format!("scatter.fields={fields}\n"));
		actual.push_str(&format!(
			"scatter.band_fills={}\n",
			scatter.resolved_spec.band_fills.len()
		));
		for assignment in &scatter
			.resolved_spec
			.channel(Channel::Color)
			.expect("categorical color")
			.colors
		{
			let color = assignment.color;
			actual.push_str(&format!(
				"scatter.{}@{}=#{:02X}{:02X}{:02X}{:02X}\n",
				assignment.category,
				assignment.position.index(),
				color.red,
				color.green,
				color.blue,
				color.alpha,
			));
		}

		let bar = plan(PlotRequest {
			source: materialized(
				"pre-switch bar",
				df!(
					"cat" => ["A", "B", "C", "D", "E", "F"],
					"val" => [1_i32, 2, 3, 4, 5, 6],
				)
				.expect("bar frame"),
			),
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Bar),
				"cat",
				"val",
			),
		})
		.expect("legacy bar request");
		let fields = bar
			.data
			.get_column_names()
			.iter()
			.map(|field| field.as_str())
			.collect::<Vec<_>>()
			.join(",");
		actual.push_str(&format!("bar.fields={fields}\n"));
		actual.push_str(&format!(
			"bar.band_fills={}\n",
			bar.resolved_spec.band_fills.len()
		));
		actual.push_str(&format!(
			"bar.color_channel={}\n",
			bar.resolved_spec.channel(Channel::Color).is_some()
		));

		assert_eq!(
			actual,
			include_str!("spec/snapshots/product-0030-pre-switch-categorical.txt")
		);
	}

	#[test]
	fn product_0030_explicit_request_consumes_the_complete_categorical_selection() {
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c_s1")
			.expect("registered categorical palette");
		let (resolved, continuation) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized(
					"explicit bar",
					df!(
						"cat" => ["A", "B", "C", "D", "E", "F"],
						"val" => [1_i32, 2, 3, 4, 5, 6],
					)
					.expect("bar frame"),
				),
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Bar),
					"cat",
					"val",
				),
			},
			&selection,
			&ColorIdentity::default(),
		)
		.expect("explicit request");
		assert!(resolved.resolved_spec.channel(Channel::Color).is_none());
		assert_eq!(continuation.len(), 6);
		assert_eq!(
			resolved
				.resolved_spec
				.band_fills
				.iter()
				.map(|assignment| (
					assignment.category.as_str(),
					assignment.position.index(),
					(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
					),
				))
				.collect::<Vec<_>>(),
			[
				("A", 0, (150, 150, 150)),
				("B", 4, (198, 219, 239)),
				("C", 8, (253, 208, 162)),
				("D", 11, (161, 217, 155)),
				("E", 15, (188, 189, 220)),
				("F", 19, (63, 0, 125)),
			]
		);
	}
}
