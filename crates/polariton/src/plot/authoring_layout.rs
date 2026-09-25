//! Authoring-time layout resolution.
//!
//! The Rune authoring path refuses an over-capacity tick request with a span
//! attributed to the authored call, before any pane exists. That check needs
//! only the layout a plot resolves to: its ranges, axis types, categories and
//! exact temporal domains. It never needs the prepared marks, and for the
//! families that carry the most data the mark preparation is the whole cost
//! of building a kernel. This module resolves the layout from the resolved
//! specification and the frame directly, so authoring builds no kernel for
//! those families.
//!
//! Mirror rule: every function here returns exactly what the factory-built
//! kernel's `layout(settings)` returns for the same specification, frame and
//! settings. Line, scatter and bar resolve through the same extent helpers
//! their preparations call, so the two derivations cannot drift; every other
//! family builds its kernel, because its extent is a product of preparation
//! itself. The tests in this module pin the equality per family.

use crate::plot::common::{
	AxisType, CartesianExtent, PlotLayout, ResolvedPlotSettings, polars_type_to_axis_type,
	temporal_axis_domain,
};
use crate::plot::core::PlotType;
use crate::plot::factory::{create_plot, create_styled_plot};
use crate::plot::kernels::bar::bar_column_extent;
use crate::plot::kernels::line::line_column_extent;
use crate::plot::kernels::scatter::axis_type_for;
use crate::plot::spec::{
	Channel, LayerMark, LayerSettings, LayeredPlotSpec, PlotProvenance, ResolvedKernelFields,
	ResolvedLayout, ResolvedPlotSpec, ScaleDomain, ScaleFamily, StampedResult,
	resolve_layered_channels, resolve_layered_domains,
};
use polars::frame::DataFrame;
use std::sync::Arc;

/// The viewport the authoring path has always resolved against. No layout
/// range depends on it; it is carried so the kernel-building families are
/// built exactly as before.
const AUTHORING_VIEWPORT: (u32, u32) = (1200, 720);

/// The layout `create_plot_from_artifact(spec, df, ..)` would resolve, without
/// building the kernel for line, scatter and bar marks.
pub fn resolved_plot_layout(
	spec: &ResolvedPlotSpec,
	df: &DataFrame,
	settings: &ResolvedPlotSettings,
) -> PlotLayout {
	match spec.kernel_fields.as_ref() {
		ResolvedKernelFields::Line { x, y, .. } => {
			cartesian(line_column_extent(df, x, y), settings)
		}
		ResolvedKernelFields::Bar {
			category, value, ..
		} => {
			let (categories, range) = bar_column_extent(df, category, value);
			bar_layout(
				categories,
				range,
				spec.composition.primary_plot_type(),
				settings,
			)
		}
		ResolvedKernelFields::Scatter { x, y, .. } => {
			let scatter = scatter_extent(spec, df);
			if spec.composition.is_scatter_with_line_overlay() {
				// The overlay kernel spans the union of both marks' prepared
				// ranges and carries the scatter mark's axis declarations.
				let line = line_column_extent(df, x, y);
				cartesian(
					CartesianExtent {
						x_range: (
							scatter.extent.x_range.0.min(line.x_range.0),
							scatter.extent.x_range.1.max(line.x_range.1),
						),
						y_range: (
							scatter.extent.y_range.0.min(line.y_range.0),
							scatter.extent.y_range.1.max(line.y_range.1),
						),
						..scatter.extent
					},
					settings,
				)
			} else {
				scatter_layout(scatter, settings)
			}
		}
		_ => create_plot(spec, df, AUTHORING_VIEWPORT.0, AUTHORING_VIEWPORT.1)
			.layout(settings.clone()),
	}
}

/// The layout `create_styled_plot_from_artifact(spec, df, ..)` would resolve,
/// including its refusal of marks that take no standalone layer settings.
pub fn styled_plot_layout(
	spec: &ResolvedPlotSpec,
	df: &DataFrame,
	settings: &ResolvedPlotSettings,
) -> Result<PlotLayout, String> {
	match spec.kernel_fields.as_ref() {
		// The styled path never composes a line overlay onto a scatter mark.
		ResolvedKernelFields::Scatter { .. } => {
			Ok(scatter_layout(scatter_extent(spec, df), settings))
		}
		ResolvedKernelFields::Bar { .. } | ResolvedKernelFields::Line { .. } => {
			Ok(resolved_plot_layout(spec, df, settings))
		}
		_ => create_styled_plot(spec, df, LayerSettings::default())
			.map(|kernel| kernel.layout(settings.clone())),
	}
}

/// The layout `create_layered_plot_from_artifact(spec, df, ..)` would resolve,
/// including its refusal of a specification that does not bind to the frame.
pub fn layered_plot_layout(
	spec: &LayeredPlotSpec,
	df: &DataFrame,
	settings: &ResolvedPlotSettings,
) -> Result<PlotLayout, String> {
	let binding =
		resolve_layered_channels(spec, df.schema().as_ref()).map_err(|e| e.to_string())?;
	let stamped = StampedResult {
		data: crate::result_artifact::ResultArtifact::from_polars(Arc::new(df.clone())),
		provenance: PlotProvenance {
			source_name: "layered plot".to_owned(),
			revision: None,
		},
	};
	let resolved = resolve_layered_domains(binding, &stamped).map_err(|e| e.to_string())?;
	let (first, rest) = resolved.layers.into_parts();
	let is_single_layer = rest.is_empty();
	if is_single_layer && first.settings == LayerSettings::default() {
		return Ok(resolved_plot_layout(&first.spec, df, settings));
	}
	let x_label = first
		.spec
		.channel(Channel::X)
		.expect("line and scatter resolve x")
		.binding
		.field
		.clone();
	let y_label = first
		.spec
		.channel(Channel::Y)
		.expect("line and scatter resolve y")
		.binding
		.field
		.clone();
	let x_column = df
		.column(&x_label)
		.expect("resolved x field remains present");
	let y_column = df
		.column(&y_label)
		.expect("resolved y field remains present");
	let x_axis_type = polars_type_to_axis_type(x_column.dtype());
	let y_axis_type = polars_type_to_axis_type(y_column.dtype());
	// A styled single layer keeps its mark's prepared range and exact temporal
	// domains; a genuine composition takes the shared-domain layout of its
	// first layer and no temporal reconciliation.
	let (x_range, y_range, x_temporal_domain, y_temporal_domain) = if is_single_layer {
		let (x_range, y_range) = match first.mark {
			LayerMark::Line => {
				let extent = line_column_extent(df, &x_label, &y_label);
				(extent.x_range, extent.y_range)
			}
			LayerMark::Scatter => {
				let layout = ResolvedLayout::from_spec(
					&first.spec,
					AUTHORING_VIEWPORT.0,
					AUTHORING_VIEWPORT.1,
				);
				(layout.x_range, layout.y_range)
			}
		};
		(
			x_range,
			y_range,
			temporal_axis_domain(x_column, &x_axis_type),
			temporal_axis_domain(y_column, &y_axis_type),
		)
	} else {
		let layout =
			ResolvedLayout::from_spec(&first.spec, AUTHORING_VIEWPORT.0, AUTHORING_VIEWPORT.1);
		(layout.x_range, layout.y_range, None, None)
	};
	Ok(cartesian(
		CartesianExtent {
			x_range,
			y_range,
			x_axis_type,
			y_axis_type,
			x_temporal_domain,
			y_temporal_domain,
		},
		settings,
	))
}

struct ScatterExtent {
	x_categories: Option<Vec<String>>,
	extent: CartesianExtent,
}

/// What scatter preparation resolves for its layout: the specification's
/// padded scale domains, the axis types its scale families declare, and the
/// exact temporal domains the frame's columns carry.
fn scatter_extent(spec: &ResolvedPlotSpec, df: &DataFrame) -> ScatterExtent {
	let x = spec
		.channel(Channel::X)
		.expect("resolved scatter x channel");
	let y = spec
		.channel(Channel::Y)
		.expect("resolved scatter y channel");
	let layout = ResolvedLayout::from_spec(spec, AUTHORING_VIEWPORT.0, AUTHORING_VIEWPORT.1);
	let x_axis_type = axis_type_for(x.binding.scale, df, &x.binding.field);
	let y_axis_type = axis_type_for(y.binding.scale, df, &y.binding.field);
	if matches!(x.binding.scale, ScaleFamily::Point | ScaleFamily::Band) {
		let categories = match &x.domain {
			ScaleDomain::Categorical(categories) => categories.clone(),
			_ => Vec::new(),
		};
		return ScatterExtent {
			x_categories: Some(categories),
			extent: CartesianExtent {
				x_range: layout.x_range,
				y_range: layout.y_range,
				x_axis_type,
				y_axis_type,
				x_temporal_domain: None,
				y_temporal_domain: None,
			},
		};
	}
	let temporal_domain = |field: &str| {
		df.column(field).ok().and_then(|column| {
			temporal_axis_domain(column, &polars_type_to_axis_type(column.dtype()))
		})
	};
	ScatterExtent {
		x_categories: None,
		extent: CartesianExtent {
			x_range: layout.x_range,
			y_range: layout.y_range,
			x_axis_type,
			y_axis_type,
			x_temporal_domain: temporal_domain(&x.binding.field),
			y_temporal_domain: temporal_domain(&y.binding.field),
		},
	}
}

fn scatter_layout(scatter: ScatterExtent, settings: &ResolvedPlotSettings) -> PlotLayout {
	match scatter.x_categories {
		Some(categories) => PlotLayout::CategoricalX {
			categories,
			y_axis_type: scatter.extent.y_axis_type,
			y_range: (
				settings.y_min.unwrap_or(scatter.extent.y_range.0),
				settings.y_max.unwrap_or(scatter.extent.y_range.1),
			),
		},
		None => cartesian(scatter.extent, settings),
	}
}

fn bar_layout(
	categories: Vec<String>,
	range: (f64, f64),
	plot_type: PlotType,
	settings: &ResolvedPlotSettings,
) -> PlotLayout {
	if plot_type == PlotType::HorizontalBar {
		PlotLayout::CategoricalY {
			categories,
			x_axis_type: AxisType::Linear,
			x_range: (
				settings.x_min.unwrap_or(range.0),
				settings.x_max.unwrap_or(range.1),
			),
		}
	} else {
		PlotLayout::CategoricalX {
			categories,
			y_axis_type: AxisType::Linear,
			y_range: (
				settings.y_min.unwrap_or(range.0),
				settings.y_max.unwrap_or(range.1),
			),
		}
	}
}

fn cartesian(extent: CartesianExtent, settings: &ResolvedPlotSettings) -> PlotLayout {
	PlotLayout::Cartesian {
		x_range: (
			settings.x_min.unwrap_or(extent.x_range.0),
			settings.x_max.unwrap_or(extent.x_range.1),
		),
		y_range: (
			settings.y_min.unwrap_or(extent.y_range.0),
			settings.y_max.unwrap_or(extent.y_range.1),
		),
		x_axis_type: extent.x_axis_type,
		y_axis_type: extent.y_axis_type,
		x_temporal_domain: extent.x_temporal_domain,
		y_temporal_domain: extent.y_temporal_domain,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::composition::PlotComposition;
	use crate::plot::factory::{create_layered_plot, create_plot_from_artifact};
	use crate::plot::spec::{
		AuthoredPlotSpec, ColorIdentity, LayerSpec, Override, PointSize, resolve_channels,
		resolve_domains,
	};
	use crate::result_artifact::ResultArtifact;
	use polars::prelude::{
		DataType, IntoColumn, NamedFrom, Series, TimeUnit as PolarsTimeUnit, TimeZone, df,
	};

	/// A lossless rendering of a layout so two of them compare exactly.
	fn describe(layout: &PlotLayout) -> String {
		match layout {
			PlotLayout::Cartesian {
				x_range,
				y_range,
				x_axis_type,
				y_axis_type,
				x_temporal_domain,
				y_temporal_domain,
			} => format!(
				"Cartesian x={x_range:?} y={y_range:?} xt={x_axis_type:?} yt={y_axis_type:?} xd={x_temporal_domain:?} yd={y_temporal_domain:?}"
			),
			PlotLayout::CategoricalX {
				categories,
				y_range,
				y_axis_type,
			} => format!("CategoricalX c={categories:?} y={y_range:?} yt={y_axis_type:?}"),
			PlotLayout::CategoricalY {
				categories,
				x_range,
				x_axis_type,
			} => format!("CategoricalY c={categories:?} x={x_range:?} xt={x_axis_type:?}"),
			PlotLayout::CategoricalXY {
				x_categories,
				y_categories,
			} => format!("CategoricalXY x={x_categories:?} y={y_categories:?}"),
			PlotLayout::Parallel { dimensions, ranges } => {
				format!("Parallel d={dimensions:?} r={ranges:?}")
			}
			PlotLayout::Radial => "Radial".to_owned(),
		}
	}

	fn resolve(authored: AuthoredPlotSpec, frame: &DataFrame) -> ResolvedPlotSpec {
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("channels");
		let stamped = StampedResult {
			data: ResultArtifact::test_frame(frame.clone()),
			provenance: PlotProvenance {
				source_name: "authoring layout".to_owned(),
				revision: Some(1),
			},
		};
		resolve_domains(binding, &stamped, &ColorIdentity::default())
			.expect("domains")
			.0
	}

	/// Default settings and a pane viewport override on every axis, so the
	/// override arm of each family is exercised alongside the resolved one.
	fn settings_cases() -> [ResolvedPlotSettings; 2] {
		let overridden = ResolvedPlotSettings {
			x_min: Some(-3.5),
			x_max: Some(12.25),
			y_min: Some(0.75),
			y_max: Some(9.5),
			..Default::default()
		};
		[ResolvedPlotSettings::default(), overridden]
	}

	/// The equality the module exists for: the decoupled layout matches the
	/// factory-built kernel's layout for every settings case.
	fn assert_matches_single(resolved: &ResolvedPlotSpec, frame: &DataFrame, family: &str) {
		let artifact = ResultArtifact::test_frame(frame.clone());
		let kernel = create_plot_from_artifact(
			resolved,
			frame,
			&artifact,
			AUTHORING_VIEWPORT.0,
			AUTHORING_VIEWPORT.1,
		);
		for settings in settings_cases() {
			assert_eq!(
				describe(&resolved_plot_layout(resolved, frame, &settings)),
				describe(&kernel.layout(settings.clone())),
				"{family}: the authoring layout must equal the kernel layout"
			);
		}
	}

	fn assert_matches_styled(resolved: &ResolvedPlotSpec, frame: &DataFrame, family: &str) {
		let kernel = create_styled_plot(resolved, frame, LayerSettings::default());
		for settings in settings_cases() {
			let layout = styled_plot_layout(resolved, frame, &settings);
			match (&kernel, layout) {
				(Ok(kernel), Ok(layout)) => assert_eq!(
					describe(&layout),
					describe(&kernel.layout(settings.clone())),
					"{family}: the styled authoring layout must equal the styled kernel layout"
				),
				(Err(expected), Err(actual)) => assert_eq!(&actual, expected, "{family}"),
				(kernel, layout) => panic!(
					"{family}: styled admission diverged: kernel {:?}, layout {:?}",
					kernel.as_ref().map(|_| ()),
					layout.map(|_| ())
				),
			}
		}
	}

	fn assert_matches_layered(spec: &LayeredPlotSpec, frame: &DataFrame, family: &str) {
		let kernel = create_layered_plot(spec, frame, AUTHORING_VIEWPORT.0, AUTHORING_VIEWPORT.1);
		for settings in settings_cases() {
			let layout = layered_plot_layout(spec, frame, &settings);
			match (&kernel, layout) {
				(Ok(kernel), Ok(layout)) => assert_eq!(
					describe(&layout),
					describe(&kernel.layout(settings.clone())),
					"{family}: the layered authoring layout must equal the layered kernel layout"
				),
				(Err(expected), Err(actual)) => assert_eq!(&actual, expected, "{family}"),
				(kernel, layout) => panic!(
					"{family}: layered admission diverged: kernel {:?}, layout {:?}",
					kernel.as_ref().map(|_| ()),
					layout.map(|_| ())
				),
			}
		}
	}

	fn numeric_frame() -> DataFrame {
		df!(
			"x" => [1.0_f64, 2.5, 4.0, 7.5, 9.0, 3.0],
			"y" => [2.0_f64, 4.0, 3.0, 8.0, 1.0, 5.5],
			"series" => ["a", "b", "a", "b", "a", "b"]
		)
		.expect("numeric frame")
	}

	fn temporal_frame() -> DataFrame {
		let x = Series::new(
			"x".into(),
			&[
				1_700_000_000_000_i64,
				1_700_360_000_000_i64,
				1_700_720_000_000_i64,
			],
		)
		.cast(&DataType::Datetime(
			PolarsTimeUnit::Milliseconds,
			Some(
				TimeZone::opt_try_new(Some("UTC"))
					.expect("UTC zone")
					.expect("named zone"),
			),
		))
		.expect("temporal x");
		DataFrame::new(
			3,
			vec![
				x.into_column(),
				Series::new("y".into(), &[1.0_f64, 2.0, 1.5]).into_column(),
				Series::new("series".into(), &["a", "b", "a"]).into_column(),
			],
		)
		.expect("temporal frame")
	}

	fn categorical_frame() -> DataFrame {
		df!(
			"category" => ["north", "east", "south", "west", "east"],
			"value" => [3.0_f64, 5.5, 2.0, 7.25, 1.0],
			"series" => ["p", "q", "p", "q", "p"]
		)
		.expect("categorical frame")
	}

	#[test]
	fn scatter_layout_matches_the_kernel() {
		let frame = numeric_frame();
		let resolved = resolve(
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
				Some("series".to_owned()),
			),
			&frame,
		);
		assert_matches_single(&resolved, &frame, "scatter");
		assert_matches_styled(&resolved, &frame, "styled scatter");
	}

	#[test]
	fn scatter_layout_over_a_categorical_x_matches_the_kernel() {
		let frame = categorical_frame();
		let resolved = resolve(
			AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"category",
				"value",
			),
			&frame,
		);
		assert!(
			matches!(
				resolved_plot_layout(&resolved, &frame, &ResolvedPlotSettings::default()),
				PlotLayout::CategoricalX { .. }
			),
			"positive control: a string x resolves the categorical scatter arm"
		);
		assert_matches_single(&resolved, &frame, "categorical scatter");
		assert_matches_styled(&resolved, &frame, "styled categorical scatter");
	}

	#[test]
	fn scatter_layout_over_a_temporal_x_matches_the_kernel() {
		let frame = temporal_frame();
		let resolved = resolve(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&frame,
		);
		let PlotLayout::Cartesian {
			x_temporal_domain, ..
		} = resolved_plot_layout(&resolved, &frame, &ResolvedPlotSettings::default())
		else {
			panic!("temporal scatter stays Cartesian");
		};
		assert!(
			x_temporal_domain.is_some(),
			"positive control: the exact temporal domain is carried"
		);
		assert_matches_single(&resolved, &frame, "temporal scatter");
	}

	#[test]
	fn scatter_with_line_overlay_layout_matches_the_kernel() {
		let frame = numeric_frame();
		let mut composition = PlotComposition::single(PlotType::Scatter);
		assert!(composition.add_layer(PlotType::Line));
		assert!(composition.is_scatter_with_line_overlay());
		let resolved = resolve(AuthoredPlotSpec::xy(composition, "x", "y"), &frame);
		assert_matches_single(&resolved, &frame, "scatter with line overlay");
	}

	#[test]
	fn line_layout_matches_the_kernel() {
		let frame = numeric_frame();
		let resolved = resolve(
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Line),
				"x",
				"y",
				Some("series".to_owned()),
			),
			&frame,
		);
		assert_matches_single(&resolved, &frame, "line");
		assert_matches_styled(&resolved, &frame, "styled line");
	}

	#[test]
	fn line_layout_over_a_temporal_x_matches_the_kernel() {
		let frame = temporal_frame();
		let resolved = resolve(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
			&frame,
		);
		assert_matches_single(&resolved, &frame, "temporal line");
	}

	#[test]
	fn bar_layouts_match_the_kernel_in_both_orientations() {
		let frame = categorical_frame();
		// A horizontal bar is authored as a bar with the value on x; pair
		// resolution turns the orientation.
		for (plot_type, x, y) in [
			(PlotType::Bar, "category", "value"),
			(PlotType::HorizontalBar, "value", "category"),
		] {
			let resolved = resolve(
				AuthoredPlotSpec::xy_color(
					PlotComposition::single(PlotType::Bar),
					x,
					y,
					Some("series".to_owned()),
				),
				&frame,
			);
			assert_eq!(resolved.composition.primary_plot_type(), plot_type);
			assert_matches_single(&resolved, &frame, &format!("{plot_type:?}"));
			assert_matches_styled(&resolved, &frame, &format!("styled {plot_type:?}"));
		}
	}

	#[test]
	fn stacked_area_layouts_match_the_kernel_for_both_normalizations() {
		let frame = numeric_frame();
		for percent in [false, true] {
			let authored = AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::StackedArea),
				"x",
				"y",
				Some("series".to_owned()),
			);
			let authored = if percent {
				authored.with_percent()
			} else {
				authored
			};
			let resolved = resolve(authored, &frame);
			assert_matches_single(
				&resolved,
				&frame,
				&format!("stacked area percent={percent}"),
			);
			assert_matches_styled(
				&resolved,
				&frame,
				&format!("styled stacked area percent={percent}"),
			);
		}
	}

	#[test]
	fn kernel_building_families_match_the_kernel() {
		let frame = categorical_frame();
		let stacked = resolve(
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Bar),
				"category",
				"value",
				Some("series".to_owned()),
			)
			.with_stacked_bar(),
			&frame,
		);
		assert_eq!(
			stacked.composition.primary_plot_type(),
			PlotType::StackedBar
		);
		assert_matches_single(&stacked, &frame, "stacked bar");
		assert_matches_styled(&stacked, &frame, "styled stacked bar");

		let slices = df!(
			"category" => ["north", "east", "south"],
			"value" => [3.0_f64, 5.5, 2.0]
		)
		.expect("pie frame");
		let pie = resolve(
			AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Pie)),
			&slices,
		);
		assert_matches_single(&pie, &slices, "pie");
		assert_matches_styled(&pie, &slices, "styled pie");

		let bins = df!(
			"bin_start" => [0.0_f64, 1.0, 2.0],
			"bin_end" => [1.0_f64, 2.0, 3.0],
			"count" => [4_i64, 9, 2]
		)
		.expect("histogram frame");
		let histogram = resolve(
			AuthoredPlotSpec::histogram("bin_start", "bin_end", "count"),
			&bins,
		);
		assert_matches_single(&histogram, &bins, "histogram");
		assert_matches_styled(&histogram, &bins, "styled histogram");
	}

	/// Evidence that the treatment applies: at the raster fixture size the
	/// decoupled scatter layout costs a column scan, the kernel costs the
	/// whole mark preparation. Run with `--ignored --nocapture`.
	#[test]
	#[ignore]
	fn probe_resolved_plot_layout_at_the_raster_fixture_size() {
		let points = 105 * 105 * 105;
		let xs: Vec<f64> = (0..points).map(|i| (i % 1_009) as f64).collect();
		let ys: Vec<f64> = (0..points).map(|i| (i % 997) as f64 * 0.5).collect();
		let series: Vec<&str> = (0..points)
			.map(|i| ["Atlas", "Beacon", "Cinder"][i % 3])
			.collect();
		let frame = df!("x" => xs, "y" => ys, "series" => series).expect("frame");
		let resolved = resolve(
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
				Some("series".to_owned()),
			),
			&frame,
		);
		let settings = ResolvedPlotSettings::default();
		let started = std::time::Instant::now();
		let layout = resolved_plot_layout(&resolved, &frame, &settings);
		let layout_elapsed = started.elapsed();
		let started = std::time::Instant::now();
		let kernel = create_plot(
			&resolved,
			&frame,
			AUTHORING_VIEWPORT.0,
			AUTHORING_VIEWPORT.1,
		);
		let kernel_elapsed = started.elapsed();
		assert_eq!(describe(&layout), describe(&kernel.layout(settings)));
		eprintln!(
			"AUTHORING_LAYOUT_TIMING points={points} layout={:.3}s kernel={:.3}s profile={}",
			layout_elapsed.as_secs_f64(),
			kernel_elapsed.as_secs_f64(),
			if cfg!(debug_assertions) {
				"dev"
			} else {
				"release"
			}
		);
	}

	#[test]
	fn layered_layouts_match_the_kernel() {
		let frame = numeric_frame();
		let mut two_layers = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y"));
		two_layers.push(LayerSpec::xy(LayerMark::Scatter, "x", "y"));
		assert_matches_layered(&two_layers, &frame, "line under scatter");

		let plain = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Scatter, "x", "y"));
		assert_matches_layered(&plain, &frame, "single default scatter layer");

		let mut styled_scatter = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		styled_scatter.settings.point_size =
			Override::Set(PointSize::new(2.0).expect("point size"));
		assert_matches_layered(
			&LayeredPlotSpec::new(styled_scatter),
			&frame,
			"styled single scatter layer",
		);

		let temporal = temporal_frame();
		let mut styled_line = LayerSpec::xy(LayerMark::Line, "x", "y");
		styled_line.settings.point_size = Override::Set(PointSize::new(2.0).expect("point size"));
		assert_matches_layered(
			&LayeredPlotSpec::new(styled_line),
			&temporal,
			"styled single temporal line layer",
		);

		let unbound = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "missing", "y"));
		assert_matches_layered(&unbound, &frame, "unbound layer");
	}
}
