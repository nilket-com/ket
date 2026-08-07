use crate::plot::common::{Orientation, PlotKernel, polars_type_to_axis_type};
use crate::plot::core::PlotType;
use crate::plot::kernels::bar::{self, BarPlotKernel};
use crate::plot::kernels::boxplot::{self, BoxPlotKernel};
use crate::plot::kernels::bubble::{self, BubblePlotKernel};
use crate::plot::kernels::candlestick::{self, CandlestickPlotKernel};
use crate::plot::kernels::fill_between::{self, FillBetweenPlotKernel};
use crate::plot::kernels::fill_between_line::FillBetweenLinePlotKernel;
use crate::plot::kernels::funnel::{self, FunnelPlotKernel};
use crate::plot::kernels::heatmap::{self, HeatmapPlotKernel};
use crate::plot::kernels::hexbin::{self, HexbinPlotKernel};
use crate::plot::kernels::histogram::{self, HistogramPlotKernel};
use crate::plot::kernels::layered::{LayerRenderer, LayeredPlotKernel};
use crate::plot::kernels::line::{self, LinePlotKernel};
use crate::plot::kernels::parallel::{self, ParallelPlotKernel};
use crate::plot::kernels::pie::{self, PiePlotKernel};
use crate::plot::kernels::radar::{self, RadarPlotKernel};
use crate::plot::kernels::radial_dial::{self, RadialDialPlotKernel};
use crate::plot::kernels::scatter::{self, ScatterPlotKernel};
use crate::plot::kernels::scatter_line::ScatterLinePlotKernel;
use crate::plot::kernels::stacked_area::{self, StackedAreaPlotKernel};
use crate::plot::kernels::stacked_bar::{self, StackedBarPlotKernel};
use crate::plot::kernels::violin::{self, ViolinPlotKernel};
use crate::plot::spec::{
	BandFillAssignment, Channel, LayerMark, LayerSettings, LayeredPlotSpec, NonEmpty,
	PlotProvenance, ResolvedKernelFields, ResolvedLayerSpec, ResolvedLayout, ResolvedPlotSpec,
	StampedResult, resolve_layered_channels, resolve_layered_domains,
};
use polars::frame::DataFrame;
use std::sync::Arc;

/// Project a resolved product specification into an existing plot kernel.
/// Field choice belongs to `plot::spec`; this module performs no schema or
/// dtype inference and never substitutes one resolved role for another.
pub fn create_plot(
	spec: &ResolvedPlotSpec,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> Arc<dyn PlotKernel + Send + Sync> {
	let plot_type = spec.composition.primary_plot_type();
	match spec.kernel_fields.as_ref() {
		ResolvedKernelFields::Violin { category, value } => {
			let prepared =
				violin::prepare_violin_data(df, category.as_deref().unwrap_or(""), value, None);
			Arc::new(ViolinPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Hexbin { x, y } => {
			let prepared = hexbin::prepare_hexbin_data(df, x, y, 0.02);
			Arc::new(HexbinPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Line { series, x, y } => {
			let prepared = line::prepare_line_data(df, series.as_deref().unwrap_or(""), x, y);
			Arc::new(LinePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Bar {
			category,
			group,
			value,
		} => create_bar_kernel(
			df,
			category,
			group.as_deref().unwrap_or(""),
			value,
			plot_type,
			LayerSettings::default(),
			&spec.band_fills,
		),
		ResolvedKernelFields::Scatter { color, x, y } => {
			let scatter_data = Arc::new(scatter::prepare_scatter_data_resolved(
				df, spec, width, height,
			));
			if spec.composition.is_scatter_with_line_overlay() {
				let line_data = Arc::new(line::prepare_line_data(
					df,
					color.as_deref().unwrap_or(""),
					x,
					y,
				));
				Arc::new(ScatterLinePlotKernel::new(scatter_data, line_data))
			} else {
				Arc::new(ScatterPlotKernel::new(scatter_data))
			}
		}
		ResolvedKernelFields::StackedBar {
			category,
			group,
			value,
		} => {
			let prepared = stacked_bar::prepare_stacked_bar_data(
				df,
				category,
				group.as_deref().unwrap_or(""),
				value,
			);
			Arc::new(StackedBarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: if plot_type == PlotType::HorizontalStackedBar {
					Orientation::Horizontal
				} else {
					Orientation::Vertical
				},
			})
		}
		ResolvedKernelFields::Pie { category, value } => {
			let prepared = pie::prepare_pie_data(df, category.as_deref().unwrap_or(""), value);
			Arc::new(PiePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::BoxPlot { category, value } => {
			let prepared =
				boxplot::prepare_box_plot_data(df, category.as_deref().unwrap_or(""), value);
			Arc::new(BoxPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Histogram { value } => {
			let prepared = histogram::prepare_histogram_data(df, value, 16);
			Arc::new(HistogramPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Bubble {
			x,
			y,
			size,
			color,
			label,
		} => {
			let prepared = bubble::prepare_bubble_data(df, x, y, size, color, label.as_deref());
			Arc::new(BubblePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Heatmap { x, y, value } => {
			let prepared = heatmap::prepare_heatmap_data(df, x, y, value);
			Arc::new(HeatmapPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Parallel {
			dimensions,
			category,
		} => {
			let prepared =
				parallel::prepare_parallel_data(df, dimensions, category.as_deref().unwrap_or(""));
			Arc::new(ParallelPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Radar {
			dimensions,
			category,
		} => {
			let prepared =
				radar::prepare_radar_data(df, dimensions, category.as_deref().unwrap_or(""));
			Arc::new(RadarPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::RadialDial {
			label,
			value,
			maximum,
		} => {
			let prepared = radial_dial::prepare_radial_dial_data(
				df,
				label.as_deref().unwrap_or(""),
				value,
				maximum,
			);
			Arc::new(RadialDialPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Candlestick {
			x,
			open,
			high,
			low,
			close,
		} => {
			let prepared = candlestick::prepare_candlestick_data(df, x, open, high, low, close);
			Arc::new(CandlestickPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::Funnel { stage, value } => {
			let prepared = funnel::prepare_funnel_data(df, stage.as_deref().unwrap_or(""), value);
			Arc::new(FunnelPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::StackedArea { series, x, y } => {
			let prepared =
				stacked_area::prepare_stacked_area_data(df, series.as_deref().unwrap_or(""), x, y);
			Arc::new(StackedAreaPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		ResolvedKernelFields::FillBetween {
			x,
			middle,
			lower,
			upper,
		} => {
			let fill_between_data = Arc::new(fill_between::prepare_fill_between_data(
				df, x, middle, lower, upper,
			));
			if spec.composition.is_fill_between_with_line_overlay() {
				let line_data = Arc::new(line::prepare_line_data(df, "", x, middle));
				Arc::new(FillBetweenLinePlotKernel::new(fill_between_data, line_data))
			} else {
				Arc::new(FillBetweenPlotKernel {
					prepared_data: fill_between_data,
				})
			}
		}
	}
}

fn create_bar_kernel(
	df: &DataFrame,
	category: &str,
	group: &str,
	value: &str,
	plot_type: PlotType,
	layer_settings: LayerSettings,
	band_fills: &[BandFillAssignment],
) -> Arc<dyn PlotKernel + Send + Sync> {
	Arc::new(BarPlotKernel {
		prepared_data: Arc::new(bar::prepare_bar_data(df, category, group, value)),
		orientation: if plot_type == PlotType::HorizontalBar {
			Orientation::Horizontal
		} else {
			Orientation::Vertical
		},
		layer_settings,
		band_fills: band_fills.to_vec(),
	})
}

/// Build a standalone mark with PRODUCT-0007 Layer-stage settings without
/// admitting that mark to PRODUCT-0013's ordered shared-Cartesian layer type.
pub fn create_styled_plot(
	spec: &ResolvedPlotSpec,
	df: &DataFrame,
	layer_settings: LayerSettings,
) -> Result<Arc<dyn PlotKernel + Send + Sync>, String> {
	let plot_type = spec.composition.primary_plot_type();
	match spec.kernel_fields.as_ref() {
		ResolvedKernelFields::Scatter { .. } => Ok(Arc::new(
			crate::plot::kernels::scatter::ScatterPlotKernel::with_layer_settings(
				Arc::new(
					crate::plot::kernels::scatter::prepare_scatter_data_resolved(
						df, spec, 1200, 720,
					),
				),
				layer_settings,
			),
		)),
		ResolvedKernelFields::Bar {
			category,
			group,
			value,
		} => Ok(create_bar_kernel(
			df,
			category,
			group.as_deref().unwrap_or(""),
			value,
			plot_type,
			layer_settings,
			&spec.band_fills,
		)),
		_ => Err("standalone Layer settings are not implemented for this plot mark".to_owned()),
	}
}

/// Lower PRODUCT-0013's additive layered specification. One layer deliberately
/// reuses the established kernel path; multiple layers preserve authored order
/// in the ordered shared-domain composite.
pub fn create_layered_plot(
	spec: &LayeredPlotSpec,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> Result<Arc<dyn PlotKernel + Send + Sync>, String> {
	let binding =
		resolve_layered_channels(spec, df.schema().as_ref()).map_err(|e| e.to_string())?;
	let stamped = StampedResult {
		data: Arc::new(df.clone()),
		provenance: PlotProvenance {
			source_name: "layered plot".to_owned(),
			revision: None,
		},
	};
	let resolved = resolve_layered_domains(binding, &stamped).map_err(|e| e.to_string())?;
	let (first, rest) = resolved.layers.into_parts();
	let is_single_layer = rest.is_empty();
	if is_single_layer && first.settings == crate::plot::spec::LayerSettings::default() {
		return Ok(create_plot(&first.spec, df, width, height));
	}

	let shared_layout = ResolvedLayout::from_spec(&first.spec, width, height);
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
	let x_axis_type = polars_type_to_axis_type(
		df.column(&x_label)
			.expect("resolved x field remains present")
			.dtype(),
	);
	let y_axis_type = polars_type_to_axis_type(
		df.column(&y_label)
			.expect("resolved y field remains present")
			.dtype(),
	);
	let first = prepare_layer_renderer(first, df, width, height);
	// A modifier changes presentation, not scale. Preserve the established
	// mark-specific prepared range for a styled one-layer plot; only a genuine
	// composition enters PRODUCT-0013's shared-domain layout.
	let shared_layout = if is_single_layer {
		first.prepared_layout(width, height)
	} else {
		shared_layout
	};
	let mut layers = NonEmpty::new(first);
	for layer in rest {
		layers.push(prepare_layer_renderer(layer, df, width, height));
	}
	Ok(Arc::new(LayeredPlotKernel::new(
		layers,
		shared_layout,
		x_axis_type,
		y_axis_type,
		x_label,
		y_label,
	)))
}

#[allow(dead_code)] // Reachable through the test-only layered entry point.
fn prepare_layer_renderer(
	layer: ResolvedLayerSpec,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> LayerRenderer {
	match (layer.mark, layer.spec.kernel_fields.as_ref()) {
		(LayerMark::Line, ResolvedKernelFields::Line { series, x, y }) => LayerRenderer::line(
			Arc::new(line::prepare_line_data(
				df,
				series.as_deref().unwrap_or(""),
				x,
				y,
			)),
			layer.settings,
		),
		(LayerMark::Scatter, ResolvedKernelFields::Scatter { .. }) => LayerRenderer::scatter(
			Arc::new(scatter::prepare_scatter_data_resolved(
				df,
				&layer.spec,
				width,
				height,
			)),
			layer.settings,
		),
		_ => unreachable!("LayerMark makes unaccepted kernel fields unrepresentable"),
	}
}

#[cfg(test)]
pub fn create_automatic_plot(
	composition: crate::plot::composition::PlotComposition,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> Result<Arc<dyn PlotKernel + Send + Sync>, String> {
	use crate::plot::spec::AuthoredPlotSpec;
	create_authored_plot(AuthoredPlotSpec::automatic(composition), df, width, height)
}

#[cfg(test)]
pub fn create_authored_plot(
	authored_spec: crate::plot::spec::AuthoredPlotSpec,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> Result<Arc<dyn PlotKernel + Send + Sync>, String> {
	use crate::plot::request::{PlotRequest, PlotSource, plan};
	let resolved = plan(PlotRequest {
		source: PlotSource::Materialized {
			name: "factory test".to_owned(),
			data: Arc::new(df.clone()),
		},
		authored_spec,
	})?;
	Ok(create_plot(
		&resolved.resolved_spec,
		&resolved.data,
		width,
		height,
	))
}

#[cfg(test)]
mod product_0013_baseline_tests {
	use super::*;
	use crate::plot::common::{CoordinateTransformer, PlotLayout, ResolvedPlotSettings};
	use crate::plot::composition::PlotComposition;
	use crate::plot::export::SvgBackend;
	use crate::plot::spec::{
		AuthoredPlotSpec, BarWidth, CategoricalPaletteSelection, Channel, ColorIdentity, LayerMark,
		LayerSettings, LayerSpec, LayeredPlotSpec, Override, PalettePosition, PlotProvenance,
		PointSize, ResolvedColor, StampedResult, resolve_channels,
		resolve_domains_with_categorical_palette,
	};
	use crate::plot::theme::PlotSettings;
	use iced::advanced::mouse::Cursor;
	use iced::{Point, Rectangle, Size};
	use polars::df;

	fn render_marks(kernel: &dyn PlotKernel) -> String {
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		backend.finish()
	}

	fn render_marks_with_settings(kernel: &dyn PlotKernel, settings: &PlotSettings) -> String {
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let resolved = settings.resolve();
		let layout = kernel.layout(resolved);
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot_with_settings(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		backend.finish()
	}

	fn fixture() -> DataFrame {
		df!("x" => [1.0, 2.0], "y" => [2.0, 4.0]).expect("frame")
	}

	fn bar_fixture() -> DataFrame {
		df!("cat" => ["A", "B"], "val" => [1.0, 2.0]).expect("bar frame")
	}

	const LINE_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 4.55 95.45 L 95.45 4.55 \" stroke=\"rgb(32, 145, 141)\" stroke-width=\"2\" stroke-opacity=\"1.00\" fill=\"none\" />\n",
		"</svg>"
	);

	const SCATTER_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 11.33 91.67 A 3.00 3.00 0 1 1 5.33 91.67 A 3.00 3.00 0 1 1 11.33 91.67 Z M 94.67 8.33 A 3.00 3.00 0 1 1 88.67 8.33 A 3.00 3.00 0 1 1 94.67 8.33 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"</svg>"
	);

	const LEGACY_VERTICAL_BAR_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 7.00 54.55 L 43.00 54.55 L 43.00 100.00 L 7.00 100.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"<path d=\"M 57.00 9.09 L 93.00 9.09 L 93.00 100.00 L 57.00 100.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"</svg>"
	);

	const LEGACY_HORIZONTAL_BAR_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 0.00 57.00 L 45.45 57.00 L 45.45 93.00 L 0.00 93.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"<path d=\"M 0.00 7.00 L 90.91 7.00 L 90.91 43.00 L 0.00 43.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"</svg>"
	);

	const VERTICAL_BAR_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 2.50 54.55 L 47.50 54.55 L 47.50 100.00 L 2.50 100.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"<path d=\"M 52.50 9.09 L 97.50 9.09 L 97.50 100.00 L 52.50 100.00 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"</svg>"
	);

	const HORIZONTAL_BAR_SVG: &str = concat!(
		"<svg width=\"100\" height=\"100\" viewBox=\"0 0 100 100\" xmlns=\"http://www.w3.org/2000/svg\">\n",
		"<path d=\"M 0.00 52.50 L 45.45 52.50 L 45.45 97.50 L 0.00 97.50 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"<path d=\"M 0.00 2.50 L 90.91 2.50 L 90.91 47.50 L 0.00 47.50 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"</svg>"
	);

	#[test]
	fn product_0021_full_band_default_retires_the_legacy_inset() {
		let prepared = Arc::new(bar::prepare_bar_data(&bar_fixture(), "cat", "", "val"));
		for (orientation, expected, legacy) in [
			(
				Orientation::Vertical,
				VERTICAL_BAR_SVG,
				LEGACY_VERTICAL_BAR_SVG,
			),
			(
				Orientation::Horizontal,
				HORIZONTAL_BAR_SVG,
				LEGACY_HORIZONTAL_BAR_SVG,
			),
		] {
			let kernel = BarPlotKernel {
				prepared_data: Arc::clone(&prepared),
				orientation,
				layer_settings: LayerSettings::default(),
				band_fills: Vec::new(),
			};
			let rendered = render_marks(&kernel);
			assert_eq!(rendered, expected);
			assert_ne!(rendered, legacy, "the hidden 80% inset must be retired");
		}
	}

	#[test]
	fn product_0029_bar_draw_consumes_exact_band_fills_in_category_order() {
		let frame = df!(
			"cat" => ["A", "B", "C", "D", "E", "F"],
			"val" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
		)
		.expect("categorical bar frame");
		let authored = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "cat", "val");
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0029".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").unwrap();
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap();
		assert!(resolved.channel(Channel::Color).is_none());
		assert_eq!(resolved.band_fills.len(), 6);

		let svg = render_marks(create_plot(&resolved, &frame, 100, 100).as_ref());
		let expected = [
			"fill=\"rgb(49, 130, 189)\"",
			"fill=\"rgb(230, 85, 13)\"",
			"fill=\"rgb(49, 163, 84)\"",
			"fill=\"rgb(199, 233, 192)\"",
			"fill=\"rgb(218, 218, 235)\"",
			"fill=\"rgb(150, 150, 150)\"",
		];
		let mut cursor = 0;
		for fill in expected {
			let offset = svg[cursor..]
				.find(fill)
				.unwrap_or_else(|| panic!("missing ordered {fill} after byte {cursor}:\n{svg}"));
			cursor += offset + fill.len();
		}
	}

	fn styled_bar(orientation: Orientation, fraction: f32) -> BarPlotKernel {
		BarPlotKernel {
			prepared_data: Arc::new(bar::prepare_bar_data(&bar_fixture(), "cat", "", "val")),
			orientation,
			layer_settings: LayerSettings {
				bar_width: Override::Set(BarWidth::new(fraction).unwrap()),
				..LayerSettings::default()
			},
			band_fills: Vec::new(),
		}
	}

	#[test]
	fn product_0021_bar_width_occupies_the_full_band_slot_in_both_orientations() {
		for (orientation, baseline, half_rectangles, touching_rectangles) in [
			(
				Orientation::Vertical,
				VERTICAL_BAR_SVG,
				["M 12.50 54.55 L 37.50", "M 62.50 9.09 L 87.50"],
				["M 0.00 54.55 L 50.00", "M 50.00 9.09 L 100.00"],
			),
			(
				Orientation::Horizontal,
				HORIZONTAL_BAR_SVG,
				["M 0.00 62.50 L 45.45", "M 0.00 12.50 L 90.91"],
				["M 0.00 50.00 L 45.45", "M 0.00 0.00 L 90.91"],
			),
		] {
			let default = styled_bar(orientation, 0.9);
			assert_eq!(
				render_marks_with_settings(&default, &PlotSettings::default()),
				baseline
			);
			let half =
				render_marks_with_settings(&styled_bar(orientation, 0.5), &PlotSettings::default());
			for rectangle in half_rectangles {
				assert!(half.contains(rectangle), "missing {rectangle:?} in {half}");
			}
			let touching =
				render_marks_with_settings(&styled_bar(orientation, 1.0), &PlotSettings::default());
			for rectangle in touching_rectangles {
				assert!(
					touching.contains(rectangle),
					"missing {rectangle:?} in {touching}"
				);
			}
		}
	}

	#[test]
	fn product_0021_bar_hit_uses_the_same_visible_rectangle_as_paint() {
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let settings = PlotSettings::default();
		for (orientation, visible, padding, expected) in [
			(
				Orientation::Vertical,
				Point::new(20.0, 75.0),
				Point::new(10.0, 75.0),
				"A: Value (Value: ~0.55)",
			),
			(
				Orientation::Horizontal,
				Point::new(20.0, 70.0),
				Point::new(20.0, 60.0),
				"A: Value (Value: ~0.44)",
			),
		] {
			let kernel = styled_bar(orientation, 0.5);
			let layout = kernel.layout(settings.resolve());
			let transform = CoordinateTransformer::new(&layout, bounds);
			assert_eq!(
				kernel.hover_with_settings(&transform, Cursor::Available(visible), &settings),
				Some(expected.to_owned())
			);
			assert_eq!(
				kernel.hover_with_settings(&transform, Cursor::Available(padding), &settings),
				None
			);
		}
	}

	#[test]
	fn product_0021_bar_width_changes_no_layout_preparation_or_guide() {
		let narrow = styled_bar(Orientation::Vertical, 0.25);
		let wide = styled_bar(Orientation::Vertical, 1.0);
		let ranges = |kernel: &BarPlotKernel| match kernel.layout(ResolvedPlotSettings::default()) {
			PlotLayout::CategoricalX {
				categories,
				y_range,
			} => (categories, y_range),
			_ => panic!("vertical bar layout"),
		};
		assert_eq!(ranges(&narrow), ranges(&wide));
		assert_eq!(
			narrow.prepared_data.categories,
			wide.prepared_data.categories
		);
		assert_eq!(
			narrow.prepared_data.group_names,
			wide.prepared_data.group_names
		);
		assert_eq!(narrow.prepared_data.values, wide.prepared_data.values);
		assert_eq!(narrow.x_label(), wide.x_label());
		assert_eq!(narrow.y_label(), wide.y_label());

		let legend = |kernel: &BarPlotKernel| {
			let mut backend = SvgBackend::new(100.0, 100.0);
			let settings = ResolvedPlotSettings::default();
			if let Some(content) = kernel.legend_content(&settings) {
				crate::plot::legend::render_legend(
					&mut backend,
					Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
					&content,
					&settings,
				);
			}
			backend.finish()
		};
		assert_eq!(legend(&narrow), legend(&wide));
	}

	#[test]
	fn product_0013_pre_extraction_line_svg_baseline() {
		let kernel = create_authored_plot(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
			&fixture(),
			100,
			100,
		)
		.expect("line kernel");
		assert_eq!(render_marks(kernel.as_ref()), LINE_SVG);
	}

	#[test]
	fn product_0013_pre_extraction_scatter_svg_baseline() {
		let kernel = create_authored_plot(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&fixture(),
			100,
			100,
		)
		.expect("scatter kernel");
		assert_eq!(render_marks(kernel.as_ref()), SCATTER_SVG);
	}

	#[test]
	fn product_0013_one_layer_line_keeps_existing_svg() {
		let kernel = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y")),
			&fixture(),
			100,
			100,
		)
		.expect("layered line kernel");
		assert_eq!(render_marks(kernel.as_ref()), LINE_SVG);
	}

	#[test]
	fn product_0013_one_layer_scatter_keeps_existing_svg() {
		let kernel = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Scatter, "x", "y")),
			&fixture(),
			100,
			100,
		)
		.expect("layered scatter kernel");
		assert_eq!(render_marks(kernel.as_ref()), SCATTER_SVG);
	}

	#[test]
	fn product_0013_shared_domain_comes_from_common_source_columns() {
		let frame = df!("x" => [0.0, 100.0], "y" => [10.0, 30.0]).expect("frame");
		let mut spec = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y"));
		spec.push(LayerSpec::xy(LayerMark::Line, "x", "y"));
		let kernel = create_layered_plot(&spec, &frame, 100, 100).expect("layered kernel");
		let PlotLayout::Cartesian {
			x_range, y_range, ..
		} = kernel.layout(ResolvedPlotSettings::default())
		else {
			panic!("layered line plot must be Cartesian");
		};
		assert_eq!(x_range, (-10.0, 110.0));
		assert_eq!(y_range, (8.0, 32.0));

		let prepared = line::prepare_line_data(&frame, "", "x", "y");
		assert_eq!(prepared.x_range, (-5.0, 105.0));
		assert_eq!(prepared.y_range, (9.0, 31.0));
		assert_ne!(
			x_range, prepared.x_range,
			"mark padding must not choose layout"
		);
		assert_ne!(
			y_range, prepared.y_range,
			"mark padding must not choose layout"
		);
	}

	#[test]
	fn product_0013_layered_svg_paints_scatter_after_line() {
		let mut spec = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y"));
		spec.push(LayerSpec::xy(LayerMark::Scatter, "x", "y"));
		let kernel = create_layered_plot(&spec, &fixture(), 100, 100).expect("layered kernel");
		let svg = render_marks(kernel.as_ref());
		let line = svg.find("stroke-width=\"2\"").expect("line stroke");
		let scatter = svg.find("fill-opacity=\"1.00\"").expect("scatter marks");
		assert!(
			line < scatter,
			"scatter must appear after the line in SVG document order:\n{svg}"
		);
	}

	#[test]
	fn product_0013_bridge_hover_prefers_topmost_geometric_hit() {
		let frame = df!("x" => [1.0, 2.0, 3.0], "y" => [2.0, 4.0, 6.0]).expect("frame");
		let layered = |first, second| {
			let mut spec = LayeredPlotSpec::new(LayerSpec::xy(first, "x", "y"));
			spec.push(LayerSpec::xy(second, "x", "y"));
			create_layered_plot(&spec, &frame, 100, 100).expect("layered kernel")
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let settings = ResolvedPlotSettings::default();
		let line_then_scatter = layered(LayerMark::Line, LayerMark::Scatter);
		let layout = line_then_scatter.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let point = transform.cartesian(2.0, 4.0);
		let cursor = Cursor::Available(Point::new(point.x + 1.8, point.y - 3.6));
		assert_eq!(
			line_then_scatter.hover(&transform, cursor),
			Some("X: 2, Y: 4".to_owned()),
			"the topmost scatter hit must win over the line beneath it"
		);

		let scatter_then_line = layered(LayerMark::Scatter, LayerMark::Line);
		let reverse_layout = scatter_then_line.layout(settings);
		let reverse_transform = CoordinateTransformer::new(&reverse_layout, bounds);
		assert_ne!(
			scatter_then_line.hover(&reverse_transform, cursor),
			Some("X: 2, Y: 4".to_owned()),
			"reversing authored order must make the line hit win"
		);
	}

	#[test]
	fn product_0044_layered_legend_unions_paint_order_without_flattening_samples() {
		let mut spec = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y"));
		spec.push(LayerSpec::xy(LayerMark::Scatter, "x", "y"));
		let kernel = create_layered_plot(&spec, &fixture(), 100, 100).expect("layered kernel");
		let settings = ResolvedPlotSettings::default();
		let content = kernel.legend_content(&settings).expect("layered content");
		let crate::plot::legend::LegendSection::Entries(entries) = content.0.first() else {
			panic!("layered legend must have entries");
		};
		assert_eq!(entries.len(), 2);
		assert_eq!(entries.first().label, "All Data");
		assert_eq!(
			entries.first().sample,
			crate::plot::legend::LegendSample::LineSample
		);
		assert_eq!(
			entries.last().sample,
			crate::plot::legend::LegendSample::PointSample
		);
	}

	#[test]
	fn product_0017_layered_vector_and_downsampled_use_each_resolved_point_size() {
		let sized = |factor| LayerSettings {
			point_size: Override::Set(PointSize::new(factor).unwrap()),
			..LayerSettings::default()
		};
		let mut first = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		first.settings = sized(0.5);
		let mut second = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		second.settings = sized(2.0);
		let mut spec = LayeredPlotSpec::new(first);
		spec.push(second);
		let kernel = create_layered_plot(&spec, &fixture(), 100, 100).expect("layered kernel");

		let vector = render_marks_with_settings(kernel.as_ref(), &PlotSettings::default());
		assert!(
			vector.contains("A 1.50 1.50"),
			"small layer missing: {vector}"
		);
		assert!(
			vector.contains("A 6.00 6.00"),
			"large layer missing: {vector}"
		);

		let mut downsampled = PlotSettings::default();
		downsampled.library_defaults.scatter_render_mode =
			crate::plot::common::ScatterRenderMode::Downsampled;
		downsampled.library_defaults.scatter_downsample_target = 10;
		let svg = render_marks_with_settings(kernel.as_ref(), &downsampled);
		assert!(
			svg.contains("A 1.50 1.50"),
			"small sampled layer missing: {svg}"
		);
		assert!(
			svg.contains("A 6.00 6.00"),
			"large sampled layer missing: {svg}"
		);
	}

	#[test]
	fn product_0017_styled_hit_uses_the_layer_resolved_radius() {
		let frame = df!("x" => [1.0], "y" => [2.0]).expect("frame");
		let kernel = |factor| {
			let mut layer = LayerSpec::xy(LayerMark::Scatter, "x", "y");
			layer.settings.point_size = Override::Set(PointSize::new(factor).unwrap());
			create_layered_plot(&LayeredPlotSpec::new(layer), &frame, 100, 100)
				.expect("styled scatter")
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let settings = PlotSettings::default();
		let small = kernel(1.0);
		let layout = small.layout(settings.resolve());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let point = transform.cartesian(1.0, 2.0);
		let cursor = Cursor::Available(Point::new(point.x + 6.0, point.y));
		assert_eq!(
			small.hover_with_settings(&transform, cursor, &settings),
			None
		);

		let large = kernel(2.0);
		assert_eq!(
			large.hover_with_settings(&transform, cursor, &settings),
			Some("X: 1, Y: 2".to_owned())
		);
	}

	#[test]
	fn product_0017_point_size_does_not_acquire_guide_behavior() {
		let kernel = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Scatter, "x", "y")),
			&fixture(),
			100,
			100,
		)
		.expect("scatter kernel");
		let render = |point_size| {
			let mut backend = SvgBackend::new(100.0, 100.0);
			let mut settings = ResolvedPlotSettings::default();
			settings.point_size = PointSize::new(point_size).unwrap();
			if let Some(content) = kernel.legend_content(&settings) {
				crate::plot::legend::render_legend(
					&mut backend,
					Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
					&content,
					&settings,
				);
			}
			backend.finish()
		};
		assert_eq!(render(1.0), render(2.0));
	}

	#[test]
	fn product_0019_line_layers_paint_independent_resolved_widths() {
		let mut thin = LayerSpec::xy(LayerMark::Line, "x", "y");
		thin.settings.stroke_width =
			Override::Set(crate::plot::spec::StrokeWidth::new(0.5).unwrap());
		let mut thick = LayerSpec::xy(LayerMark::Line, "x", "y");
		thick.settings.stroke_width =
			Override::Set(crate::plot::spec::StrokeWidth::new(2.0).unwrap());
		let mut spec = LayeredPlotSpec::new(thin);
		spec.push(thick);
		let kernel = create_layered_plot(&spec, &fixture(), 100, 100).expect("line layers");
		let svg = render_marks_with_settings(kernel.as_ref(), &PlotSettings::default());
		assert!(
			svg.contains("stroke-width=\"1\""),
			"thin line missing: {svg}"
		);
		assert!(
			svg.contains("stroke-width=\"4\""),
			"thick line missing: {svg}"
		);
		let first = svg.find("stroke-width=\"1\"").unwrap();
		let second = svg.find("stroke-width=\"4\"").unwrap();
		assert!(
			first < second,
			"settings must not change authored paint order"
		);
	}

	#[test]
	fn product_0019_default_and_doubled_line_widths_keep_one_drawing_body() {
		let standalone = create_authored_plot(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y"),
			&fixture(),
			100,
			100,
		)
		.expect("standalone line");
		let layered = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y")),
			&fixture(),
			100,
			100,
		)
		.expect("layered line");
		assert_eq!(render_marks(standalone.as_ref()), LINE_SVG);
		assert_eq!(render_marks(layered.as_ref()), LINE_SVG);

		let mut doubled = PlotSettings::default();
		doubled.library_defaults.stroke_width = crate::plot::spec::StrokeWidth::new(2.0).unwrap();
		let svg = render_marks_with_settings(layered.as_ref(), &doubled);
		assert!(
			svg.contains("stroke-width=\"4\""),
			"doubled line missing: {svg}"
		);
	}

	#[test]
	fn product_0019_styled_hit_follows_half_stroke_plus_fixed_tolerance() {
		let frame = df!("x" => [1.0], "y" => [2.0]).expect("frame");
		let kernel = |factor| {
			let mut layer = LayerSpec::xy(LayerMark::Line, "x", "y");
			layer.settings.stroke_width =
				Override::Set(crate::plot::spec::StrokeWidth::new(factor).unwrap());
			create_layered_plot(&LayeredPlotSpec::new(layer), &frame, 100, 100)
				.expect("styled line")
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0));
		let settings = PlotSettings::default();
		let normal = kernel(1.0);
		let layout = normal.layout(settings.resolve());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let point = transform.cartesian(1.0, 2.0);
		let cursor = Cursor::Available(Point::new(point.x + 6.5, point.y));
		assert_eq!(
			normal.hover_with_settings(&transform, cursor, &settings),
			None
		);

		let doubled = kernel(2.0);
		assert_eq!(
			doubled.hover_with_settings(&transform, cursor, &settings),
			Some("X: 1, Y: 2".to_owned())
		);
	}

	#[test]
	fn product_0019_stroke_width_does_not_acquire_guide_behavior() {
		let kernel = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y")),
			&fixture(),
			100,
			100,
		)
		.expect("line kernel");
		let render = |factor| {
			let mut backend = SvgBackend::new(100.0, 100.0);
			let mut settings = ResolvedPlotSettings::default();
			settings.stroke_width = crate::plot::spec::StrokeWidth::new(factor).unwrap();
			if let Some(content) = kernel.legend_content(&settings) {
				crate::plot::legend::render_legend(
					&mut backend,
					Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
					&content,
					&settings,
				);
			}
			backend.finish()
		};
		assert_eq!(render(1.0), render(2.0));
	}

	#[test]
	fn product_0043_corpus_is_historical_and_renderer_is_byte_stable_for_fixed_content() {
		let corpus =
			include_str!("../../../../plans/product/impl/0043_legacy_legend_corpus.svg.txt");
		assert!(corpus.contains("## Bar Plot"));
		assert!(corpus.contains("## Scatter Line Plot"));

		let settings = ResolvedPlotSettings::default();
		let content =
			crate::plot::legend::entries_from_labels(["A", "B"], &settings).expect("fixed content");
		let render = || {
			let mut backend = SvgBackend::new(400.0, 300.0);
			crate::plot::legend::render_legend(
				&mut backend,
				Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
				&content,
				&settings,
			);
			backend.finish()
		};
		assert_eq!(render(), render());
	}

	#[test]
	fn product_0044_band_fill_bars_omit_legends_but_authored_series_keep_them() {
		let prepared = Arc::new(bar::prepare_bar_data(&bar_fixture(), "cat", "", "val"));
		let kernel = BarPlotKernel {
			prepared_data: prepared,
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: vec![BandFillAssignment {
				category: "A".to_owned(),
				position: PalettePosition::new(0, 1).unwrap(),
				color: ResolvedColor {
					red: 12,
					green: 34,
					blue: 56,
					alpha: 255,
				},
			}],
		};
		assert!(
			kernel
				.legend_content(&ResolvedPlotSettings::default())
				.is_none()
		);

		let grouped = BarPlotKernel {
			prepared_data: Arc::new(bar::prepare_bar_data(
				&df!(
					"cat" => ["A", "B"],
					"series" => ["north", "south"],
					"val" => [1.0_f64, 2.0]
				)
				.expect("grouped bar fixture"),
				"cat",
				"series",
				"val",
			)),
			orientation: Orientation::Vertical,
			layer_settings: LayerSettings::default(),
			band_fills: Vec::new(),
		};
		let content = grouped
			.legend_content(&ResolvedPlotSettings::default())
			.expect("authored color series legend");
		assert!(matches!(
			content.0.first(),
			crate::plot::legend::LegendSection::Entries(entries)
				if entries.first().provenance == crate::plot::legend::LegendProvenance::Named
		));
	}

	#[test]
	fn product_0043_gradient_and_dual_legends_keep_their_structural_geometry() {
		let frame = df!(
			"cat" => ["A", "B", "C"],
			"group" => ["G1", "G2", "G1"],
			"x" => [1.0, 2.0, 3.0],
			"y" => [2.0, 4.0, 6.0],
			"size" => [5.0, 10.0, 15.0],
			"color" => [0.1, 0.5, 0.9]
		)
		.expect("gradient frame");
		for plot_type in [PlotType::Heatmap, PlotType::Hexbin, PlotType::Histogram] {
			let kernel =
				create_automatic_plot(PlotComposition::single(plot_type), &frame, 400, 300)
					.unwrap_or_else(|error| panic!("{plot_type}: {error}"));
			let settings = ResolvedPlotSettings::default();
			let content = kernel.legend_content(&settings).expect("gradient content");
			assert_eq!(content.0.len(), 1);
			assert!(matches!(
				content.0.first(),
				crate::plot::legend::LegendSection::Gradient { low, high, .. }
					if *low == 0.0 && high.is_finite()
			));
			let mut backend = SvgBackend::new(400.0, 300.0);
			crate::plot::legend::render_legend(
				&mut backend,
				Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
				&content,
				&settings,
			);
			assert_eq!(
				backend.finish().matches("fill-opacity=\"1.00\"").count(),
				50
			);
		}

		let kernel =
			create_automatic_plot(PlotComposition::single(PlotType::Bubble), &frame, 400, 300)
				.expect("bubble kernel");
		let settings = ResolvedPlotSettings::default();
		let content = kernel.legend_content(&settings).expect("bubble content");
		assert_eq!(content.0.len(), 2);
		assert!(matches!(
			content.0.first(),
			crate::plot::legend::LegendSection::Gradient { .. }
		));
		assert!(matches!(
			content.0.last(),
			crate::plot::legend::LegendSection::SizeSample { low, high, .. }
				if low < high
		));
		let mut backend = SvgBackend::new(400.0, 300.0);
		crate::plot::legend::render_legend(
			&mut backend,
			Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
			&content,
			&settings,
		);
		assert!(backend.finish().matches("fill-opacity=\"1.00\"").count() >= 50);
	}

	#[test]
	fn product_0043_composite_corpus_sections_cover_both_delegations() {
		let frame = fixture();
		let mut layered_spec = LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y"));
		layered_spec.push(LayerSpec::xy(LayerMark::Scatter, "x", "y"));
		let layered = create_layered_plot(&layered_spec, &frame, 400, 300).expect("layered kernel");
		let settings = ResolvedPlotSettings::default();
		let layered_content = layered.legend_content(&settings).expect("layered content");
		assert!(matches!(
			layered_content.0.first(),
			crate::plot::legend::LegendSection::Entries(_)
		));

		let scatter_line = ScatterLinePlotKernel::new(
			Arc::new(scatter::prepare_scatter_data(&frame, "", "x", "y")),
			Arc::new(line::prepare_line_data(&frame, "", "x", "y")),
		);
		let scatter_line_content = scatter_line
			.legend_content(&settings)
			.expect("scatter-line content");
		assert!(matches!(
			scatter_line_content.0.first(),
			crate::plot::legend::LegendSection::Entries(_)
		));

		let corpus =
			include_str!("../../../../plans/product/impl/0043_legacy_legend_corpus.svg.txt");
		assert!(corpus.contains("## Layered Plot"));
		assert!(corpus.contains("## Scatter Line Plot"));
	}
}
