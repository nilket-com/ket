//! The staged plot pipeline: `PlotRequest → plan → ResolvedPlot → PlotKernel`.
//!
//! A Rune execution emits declarative [`PlotRequest`]s; the host resolves them
//! into render-ready [`ResolvedPlot`]s after the VM has returned. Keeping the
//! stages as distinct types is what makes it impossible to treat an unresolved
//! source like an in-memory frame once deferred database relations arrive
//! (`plans/wip/rune-data-products.md`).

use crate::plot::kernels::heatmap::prepare_heatmap_data;
use crate::plot::kernels::histogram::prepare_histogram_data;
use crate::plot::kernels::multivariate_rows::prepare_multivariate_rows;
use crate::plot::spec::{
	AuthoredChannel, AuthoredPlotSpec, CategoricalPaletteSelection, ColorIdentity,
	OptionalAuthoredChannel, PlotProvenance, ResolvedPlotSpec, StampedResult, resolve_channels,
	resolve_domains, resolve_domains_with_categorical_palette,
};
use crate::result_artifact::ResultArtifact;
use polars::frame::DataFrame;
use std::sync::Arc;

/// Where a requested plot's data comes from.
///
/// `Materialized` is the only variant until deferred `db.table(...)` relations
/// land; the enum exists now so every consumer already distinguishes owned
/// snapshots from sources that still require planning.
#[derive(Clone, Debug)]
pub enum PlotSource {
	Materialized {
		name: String,
		data: Arc<ResultArtifact>,
	},
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
	pub artifact: Arc<ResultArtifact>,
	pub resolved_spec: ResolvedPlotSpec,
}

#[allow(dead_code)] // Conversion is exposed before the gated durable-plot surface.
impl ResolvedPlot {
	pub fn into_live_artifact(self) -> Result<crate::plot::durable::DurablePlot, String> {
		let result = StampedResult {
			data: self.artifact,
			provenance: self.resolved_spec.provenance.clone(),
		};
		crate::plot::durable::DurablePlot::live(*self.authored_spec, result)
	}

	pub fn into_pinned_artifact(self) -> Result<crate::plot::durable::DurablePlot, String> {
		let result = StampedResult {
			data: self.artifact,
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
			let frame = data.projection().map_err(|error| error.to_string())?;
			let binding = resolve_channels(&request.authored_spec, frame.schema().as_ref())
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
			validate_prepared_contract(&resolved_spec, frame.as_ref())
				.map_err(|error| format!("{name}: {error}"))?;
			let projected = project_named_channels(&request.authored_spec, frame, &name)?;
			Ok(ResolvedPlot {
				source_name: name,
				authored_spec: Box::new(request.authored_spec),
				data: projected,
				artifact: data,
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
			let frame = data.projection().map_err(|error| error.to_string())?;
			let binding = resolve_channels(&request.authored_spec, frame.schema().as_ref())
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
			validate_prepared_contract(&resolved_spec, frame.as_ref())
				.map_err(|error| format!("{name}: {error}"))?;
			let projected = project_named_channels(&request.authored_spec, frame, &name)?;
			Ok((
				ResolvedPlot {
					source_name: name,
					authored_spec: Box::new(request.authored_spec),
					data: projected,
					artifact: data,
					resolved_spec,
				},
				next_identity,
			))
		}
	}
}

fn validate_prepared_contract(spec: &ResolvedPlotSpec, data: &DataFrame) -> Result<(), String> {
	if let crate::plot::spec::ResolvedKernelFields::Bubble {
		x,
		y,
		size,
		color,
		label,
	} = spec.kernel_fields.as_ref()
	{
		let fields = crate::plot::spec::BubbleFields::new(
			crate::plot::spec::BubbleFieldNames::new(x, y, size, color),
			label.clone(),
		);
		let prepared = crate::plot::kernels::bubble::prepare_bubble_data(data, &fields)
			.map_err(|error| error.to_string())?;
		validate_continuous_scale(
			prepared.observations.color_range(),
			spec.continuous_intent.unwrap_or(prepared.color_intent),
		)?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Heatmap { x, y, value } =
		spec.kernel_fields.as_ref()
	{
		let prepared =
			prepare_heatmap_data(data, x, y, value).map_err(|error| error.to_string())?;
		validate_continuous_scale(
			prepared.cells.domain(),
			spec.continuous_intent.unwrap_or(prepared.color_intent),
		)?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Hexbin { x, y } = spec.kernel_fields.as_ref() {
		let prepared = crate::plot::kernels::hexbin::prepare_hexbin_data(data, x, y, 0.02);
		validate_continuous_scale((0.0, f64::from(prepared.max_count)), prepared.color_intent)?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Histogram {
		bin_start,
		bin_end,
		count,
	} = spec.kernel_fields.as_ref()
	{
		prepare_histogram_data(data, bin_start, bin_end, count)?;
	}
	if let crate::plot::spec::ResolvedKernelFields::BoxPlot {
		category,
		whisker_low,
		q1,
		median,
		q3,
		whisker_high,
		outlier,
	} = spec.kernel_fields.as_ref()
	{
		crate::plot::kernels::boxplot::prepare_box_plot_data(
			data,
			&crate::plot::spec::BoxPlotFields {
				category: category.clone(),
				whisker_low: whisker_low.clone(),
				q1: q1.clone(),
				median: median.clone(),
				q3: q3.clone(),
				whisker_high: whisker_high.clone(),
				outlier: outlier.clone(),
			},
		)
		.map_err(|error| error.to_string())?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Funnel { stage, value } =
		spec.kernel_fields.as_ref()
	{
		crate::plot::kernels::funnel::prepare_funnel_data(
			data,
			stage.as_deref().unwrap_or(""),
			value,
		)
		.map_err(|error| error.to_string())?;
	}
	if let crate::plot::spec::ResolvedKernelFields::RadialDial {
		label,
		value,
		maximum,
	} = spec.kernel_fields.as_ref()
	{
		crate::plot::kernels::radial_dial::prepare_radial_dial_data(
			data,
			label.as_deref(),
			value,
			maximum,
		)
		.map_err(|error| error.to_string())?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Pie { category, value } =
		spec.kernel_fields.as_ref()
	{
		crate::plot::kernels::pie::prepare_pie_data(data, category.as_deref(), value)
			.map_err(|error| error.to_string())?;
	}
	if let crate::plot::spec::ResolvedKernelFields::Parallel {
		dimensions,
		category,
	}
	| crate::plot::spec::ResolvedKernelFields::Radar {
		dimensions,
		category,
	} = spec.kernel_fields.as_ref()
	{
		prepare_multivariate_rows(data, dimensions, category.as_deref())
			.map_err(|error| error.to_string())?;
	}
	Ok(())
}

fn validate_continuous_scale(
	domain: (f64, f64),
	intent: crate::plot::continuous_color::ContinuousScaleIntent,
) -> Result<(), String> {
	crate::plot::continuous_color::ContinuousColorScale::new(
		domain,
		intent,
		crate::plot::continuous_color::ContinuousRamps::default(),
	)
	.map(|_| ())
	.map_err(|error| error.to_string())
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
	if let Some(mark_fields) = spec.mark_fields() {
		for field in mark_fields.all() {
			if !fields.contains(&field) {
				fields.push(field);
			}
		}
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
			data: ResultArtifact::test_frame(data),
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
	fn product_0096_planning_surfaces_funnel_refusals_before_factory_reconstruction() {
		let malformed = polars::df!(
			"stage" => ["A", "B", "C"],
			"value" => [Some(100.0_f64), None, Some(20.0)],
		)
		.unwrap();
		let error = plan(PlotRequest {
			source: materialized("funnel_probe", malformed),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Funnel)),
		})
		.unwrap_err();
		assert!(error.contains("funnel_probe: funnel value field `value` is null at row 2"));

		let valid = polars::df!(
			"stage" => ["A", "B", "C"],
			"value" => [100.0_f64, 50.0, 20.0],
		)
		.unwrap();
		assert!(
			plan(PlotRequest {
				source: materialized("funnel_probe", valid),
				authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(
					PlotType::Funnel,
				)),
			})
			.is_ok()
		);
	}

	#[test]
	fn product_0099_planning_surfaces_radial_dial_refusals_before_factory_reconstruction() {
		let duplicate = polars::df!(
			"category" => ["A", "A"],
			"value" => [4.0_f64, 4.0],
			"maximum" => [8.0_f64, 8.0],
		)
		.expect("duplicate radial-dial relation");
		let error = plan(PlotRequest {
			source: materialized("duplicate radial dial", duplicate),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(
				PlotType::RadialDial,
			)),
		})
		.unwrap_err();
		assert!(error.contains("duplicate radial dial: radial dial category `A`"));
		assert!(error.contains("duplicated at rows 1 and 2"));

		let valid = polars::df!(
			"category" => ["B", "A"],
			"value" => [4.0_f64, 3.0],
			"maximum" => [8.0_f64, 10.0],
		)
		.expect("valid radial-dial relation");
		let resolved = plan(PlotRequest {
			source: materialized("valid radial dial", valid),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(
				PlotType::RadialDial,
			)),
		})
		.expect("planning validates the radial-dial snapshot");
		let _kernel = crate::plot::factory::create_plot(
			&resolved.resolved_spec,
			resolved.data.as_ref(),
			400,
			300,
		);

		let ungrouped = polars::df!(
			"value" => [4.0_f64],
			"maximum" => [8.0_f64],
		)
		.expect("one ungrouped reading");
		assert!(
			plan(PlotRequest {
				source: materialized("one ungrouped radial dial", ungrouped),
				authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(
					PlotType::RadialDial,
				)),
			})
			.is_ok()
		);
		let ambiguous = polars::df!(
			"value" => [4.0_f64, 5.0],
			"maximum" => [8.0_f64, 8.0],
		)
		.expect("ambiguous ungrouped readings");
		let error = plan(PlotRequest {
			source: materialized("ambiguous radial dial", ambiguous),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(
				PlotType::RadialDial,
			)),
		})
		.unwrap_err();
		assert!(error.contains("ungrouped radial dial has more than one reading at row 2"));
	}

	#[test]
	fn product_0102_planning_surfaces_pie_refusals_before_factory_reconstruction() {
		let duplicate = polars::df!(
			"category" => ["A", "A", "B"],
			"value" => [2.0_f64, 3.0, 5.0],
		)
		.expect("duplicate pie relation");
		let error = plan(PlotRequest {
			source: materialized("duplicate pie", duplicate),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Pie)),
		})
		.unwrap_err();
		assert!(error.contains("duplicate pie: pie category `A`"), "{error}");
		assert!(error.contains("duplicated at rows 1 and 2"), "{error}");
		assert!(
			error.contains(".aggregate([col(\"category\")], [sum(col(\"value\"))])"),
			"{error}"
		);

		let valid = polars::df!(
			"category" => ["A", "B"],
			"value" => [5.0_f64, 5.0],
		)
		.expect("pre-aggregated pie relation");
		let resolved = plan(PlotRequest {
			source: materialized("pre-aggregated pie", valid),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Pie)),
		})
		.expect("planning validates the pie snapshot");
		let _kernel = crate::plot::factory::create_plot(
			&resolved.resolved_spec,
			resolved.data.as_ref(),
			400,
			300,
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
	fn product_0088_planning_surfaces_heatmap_refusals_before_factory_reconstruction() {
		let duplicate = polars::df!(
			"x" => ["a", "a"],
			"y" => ["u", "u"],
			"value" => [1.0, 9.0],
		)
		.unwrap();
		let error = plan(PlotRequest {
			source: materialized("duplicate heatmap", duplicate),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Heatmap)),
		})
		.unwrap_err();
		assert!(error.contains("duplicate heatmap: heatmap cell (`a`, `u`)"));
		assert!(error.contains("aggregate duplicate cells in the relation"));

		let valid = polars::df!(
			"x" => ["a", "b"],
			"y" => ["u", "v"],
			"value" => [1.0, 9.0],
		)
		.unwrap();
		let resolved = plan(PlotRequest {
			source: materialized("valid heatmap", valid),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Heatmap)),
		})
		.expect("planning validates the heatmap snapshot");
		let _kernel = crate::plot::factory::create_plot(
			&resolved.resolved_spec,
			resolved.data.as_ref(),
			400,
			300,
		);
	}

	#[test]
	fn product_0092_authored_heatmap_projects_all_roles_renders_and_keeps_identity() {
		let selection = CategoricalPaletteSelection::from_theme_name("viridis").unwrap();
		let mut identity = ColorIdentity::default();
		let categorical = polars::df!(
			"category" => ["a", "b"],
			"value" => [1.0, 2.0],
		)
		.unwrap();
		let (_, allocated) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized("categorical", categorical),
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Bar),
					"category",
					"value",
				),
			},
			&selection,
			&identity,
		)
		.unwrap();
		identity = allocated;

		let frame = polars::df!(
			"region" => ["north", "north", "south"],
			"quarter" => ["q1", "q2", "q1"],
			"margin" => [-10_i64, 5_i64, 20_i64],
			"dropped" => [7_i64, 8_i64, 9_i64],
		)
		.unwrap();
		let (resolved, after) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized("heat", frame),
				authored_spec: AuthoredPlotSpec::heatmap(crate::plot::spec::HeatmapFields {
					x: "region".to_owned(),
					y: "quarter".to_owned(),
					value: "margin".to_owned(),
				}),
			},
			&selection,
			&identity,
		)
		.expect("authored heatmap plans");
		assert_eq!(after, identity);
		assert_eq!(
			resolved
				.data
				.get_column_names()
				.into_iter()
				.map(|name| name.as_str())
				.collect::<Vec<_>>(),
			vec!["region", "quarter", "margin"]
		);
		assert_eq!(
			crate::plot::kernels::heatmap::prepare_heatmap_data(
				resolved.data.as_ref(),
				"region",
				"quarter",
				"margin",
			)
			.unwrap()
			.cells
			.domain(),
			(-10.0, 20.0)
		);
		let _kernel = crate::plot::factory::create_plot(
			&resolved.resolved_spec,
			resolved.data.as_ref(),
			400,
			300,
		);
	}

	#[test]
	fn product_0092_authored_duplicate_cells_refuse_before_render() {
		let duplicate = polars::df!(
			"region" => ["north", "north"],
			"quarter" => ["q1", "q1"],
			"margin" => [1.0, 9.0],
		)
		.unwrap();
		let error = plan(PlotRequest {
			source: materialized("authored duplicate", duplicate),
			authored_spec: AuthoredPlotSpec::heatmap(crate::plot::spec::HeatmapFields {
				x: "region".to_owned(),
				y: "quarter".to_owned(),
				value: "margin".to_owned(),
			}),
		})
		.unwrap_err();
		assert!(error.contains("(`north`, `q1`)"), "{error}");
		assert!(error.contains("rows 1 and 2"), "{error}");
		assert!(error.contains("aggregate duplicate cells"), "{error}");
	}

	#[test]
	fn product_0090_planning_surfaces_bubble_refusals_before_factory_reconstruction() {
		let malformed = polars::df!(
			"x" => [Some(1.0), None],
			"y" => [2.0, 3.0],
			"size" => [4.0, 5.0],
			"color" => [6.0, 7.0],
		)
		.unwrap();
		let error = plan(PlotRequest {
			source: materialized("malformed bubble", malformed),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Bubble)),
		})
		.unwrap_err();
		assert!(error.contains("malformed bubble: bubble x field `x` is null at row 2"));

		let valid = polars::df!(
			"x" => [1.0, 2.0],
			"y" => [2.0, 3.0],
			"size" => [4.0, 5.0],
			"color" => [6.0, 7.0],
		)
		.unwrap();
		let resolved = plan(PlotRequest {
			source: materialized("valid bubble", valid),
			authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Bubble)),
		})
		.expect("planning validates the bubble snapshot");
		let _kernel = crate::plot::factory::create_plot(
			&resolved.resolved_spec,
			resolved.data.as_ref(),
			400,
			300,
		);
	}

	#[test]
	fn product_0089_scale_refusals_stop_before_render_and_continuous_marks_keep_identity() {
		use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
		use crate::plot::spec::ColorJob;

		for intent in [
			ContinuousScaleIntent {
				job: ColorJob::Categorical,
				anchor: ContinuousAnchor::None,
			},
			ContinuousScaleIntent {
				job: ColorJob::Sequential,
				anchor: ContinuousAnchor::DefaultZero,
			},
			ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::Authored(2.0),
			},
		] {
			let mut rendered = false;
			let result = validate_continuous_scale((0.0, 1.0), intent).map(|()| {
				rendered = true;
			});
			assert!(result.is_err());
			assert!(!rendered, "a refused scale cannot reach rendering");
		}

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

		let fixtures = [
			(
				"heatmap",
				polars::df!("x" => ["a", "b"], "y" => ["u", "v"], "value" => [1.0, 2.0])
					.unwrap(),
				PlotType::Heatmap,
			),
			(
				"bubble",
				polars::df!("x" => [1.0, 2.0], "y" => [2.0, 3.0], "size" => [4.0, 5.0], "color" => [6.0, 7.0])
					.unwrap(),
				PlotType::Bubble,
			),
			(
				"hexbin",
				polars::df!("x" => [1.0, 2.0], "y" => [2.0, 3.0]).unwrap(),
				PlotType::Hexbin,
			),
		];
		for (name, frame, plot_type) in fixtures {
			let (_, after) = plan_with_categorical_palette(
				PlotRequest {
					source: materialized(name, frame),
					authored_spec: AuthoredPlotSpec::automatic(PlotComposition::single(plot_type)),
				},
				&selection,
				&identity,
			)
			.unwrap_or_else(|error| panic!("{plot_type}: {error}"));
			assert_eq!(
				after, identity,
				"{plot_type} allocated categorical identity"
			);
		}
	}

	#[test]
	fn product_0082_box_keeps_identity_and_projects_all_seven_fields() {
		let selection = CategoricalPaletteSelection::from_theme_name("viridis").unwrap();
		let categorical = polars::df!("category" => ["a", "b"], "value" => [1.0, 2.0]).unwrap();
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
		let fields = crate::plot::spec::BoxPlotFields {
			category: "group".into(),
			whisker_low: "lo".into(),
			q1: "p25".into(),
			median: "mid".into(),
			q3: "p75".into(),
			whisker_high: "hi".into(),
			outlier: "point".into(),
		};
		let frame = polars::df!(
			"unused" => [99_i32],
			"group" => ["A"],
			"lo" => [0_i32],
			"p25" => [1.0_f32],
			"mid" => [2_i64],
			"p75" => [3.0_f64],
			"hi" => [4_u32],
			"point" => [None::<f64>],
		)
		.unwrap();
		let (resolved, after) = plan_with_categorical_palette(
			PlotRequest {
				source: materialized("box", frame),
				authored_spec: AuthoredPlotSpec::box_plot(fields),
			},
			&selection,
			&identity,
		)
		.unwrap();
		assert_eq!(after, identity);
		assert_eq!(
			resolved
				.data
				.get_column_names()
				.iter()
				.map(|name| name.as_str())
				.collect::<Vec<_>>(),
			["group", "mid", "lo", "p25", "p75", "hi", "point"]
		);
		assert!(resolved.resolved_spec.band_fills.is_empty());
		assert!(resolved.resolved_spec.channel(Channel::Color).is_none());
	}

	#[test]
	fn product_0095_authored_bubble_projection_carries_exactly_four_or_five_roles() {
		let frame = Arc::new(
			polars::df!(
				"unused" => [99_i32],
				"x" => [1.0_f64],
				"y" => [2.0_f64],
				"size" => [3.0_f64],
				"color" => [4.0_f64],
				"label" => ["point"],
			)
			.unwrap(),
		);
		for (label, expected) in [
			(None, vec!["x", "y", "size", "color"]),
			(
				Some("label".to_owned()),
				vec!["x", "y", "size", "color", "label"],
			),
		] {
			let spec = AuthoredPlotSpec::bubble(crate::plot::spec::BubbleFields::new(
				crate::plot::spec::BubbleFieldNames::new("x", "y", "size", "color"),
				label,
			));
			let projected = project_named_channels(&spec, Arc::clone(&frame), "bubble test")
				.expect("bubble fields project through the staged carrier");
			assert_eq!(
				projected
					.get_column_names()
					.iter()
					.map(|name| name.as_str())
					.collect::<Vec<_>>(),
				expected
			);
		}
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
