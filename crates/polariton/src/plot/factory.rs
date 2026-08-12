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
	BandFillAssignment, CategoryColorAssignment, Channel, ColorJob, LayerMark, LayerSettings,
	LayeredPlotSpec, NonEmpty, PlotProvenance, ResolvedKernelFields, ResolvedLayerSpec,
	ResolvedLayout, ResolvedPlotSpec, StackNormalization, StampedResult, resolve_layered_channels,
	resolve_layered_domains,
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
			let prepared = prepare_resolved_line(spec, df, series.as_deref().unwrap_or(""), x, y);
			Arc::new(LinePlotKernel::new(Arc::new(prepared)))
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
			resolved_category_colors(spec),
		),
		ResolvedKernelFields::Scatter { color, x, y } => {
			let scatter_data = Arc::new(scatter::prepare_scatter_data_resolved(
				df, spec, width, height,
			));
			if spec.composition.is_scatter_with_line_overlay() {
				let line_data = Arc::new(prepare_resolved_line(
					spec,
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
			create_stacked_bar_kernel(
				prepared,
				plot_type,
				LayerSettings::default(),
				&spec.band_fills,
				resolved_category_colors(spec),
				spec.stack_normalization,
			)
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
			let assignments = resolved_category_colors(spec);
			let prepared = if assignments.is_empty() {
				stacked_area::prepare_stacked_area_data(df, series.as_deref().unwrap_or(""), x, y)
			} else {
				stacked_area::prepare_stacked_area_data_with_resolved_order(
					df,
					series.as_deref().unwrap_or(""),
					x,
					y,
					assignments,
				)
			};
			Arc::new(StackedAreaPlotKernel {
				prepared_data: Arc::new(prepared),
				series_colors: assignments.to_vec(),
				normalization: spec.stack_normalization,
			})
		}
		ResolvedKernelFields::Histogram {
			bin_start,
			bin_end,
			count,
		} => Arc::new(crate::plot::kernels::histogram::HistogramPlotKernel {
			prepared_data: Arc::new(
				crate::plot::kernels::histogram::prepare_histogram_data(
					df, bin_start, bin_end, count,
				)
				.expect("resolved histogram was validated during planning"),
			),
		}),
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
	group_colors: &[CategoryColorAssignment],
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
		group_colors: group_colors.to_vec(),
	})
}

fn create_stacked_bar_kernel(
	prepared_data: stacked_bar::StackedBarPreparedData,
	plot_type: PlotType,
	layer_settings: LayerSettings,
	band_fills: &[BandFillAssignment],
	group_colors: &[CategoryColorAssignment],
	normalization: StackNormalization,
) -> Arc<dyn PlotKernel + Send + Sync> {
	Arc::new(StackedBarPlotKernel {
		prepared_data: Arc::new(prepared_data),
		orientation: if plot_type == PlotType::HorizontalStackedBar {
			Orientation::Horizontal
		} else {
			Orientation::Vertical
		},
		layer_settings,
		band_fills: band_fills.to_vec(),
		group_colors: group_colors.to_vec(),
		normalization,
	})
}

/// PRODUCT-0029 §1: the exact assignments an authored categorical channel wrote
/// into the resolved artifact, empty when no such channel exists.
fn resolved_category_colors(spec: &ResolvedPlotSpec) -> &[CategoryColorAssignment] {
	spec.channel(Channel::Color)
		.map(|channel| channel.colors.as_slice())
		.unwrap_or(&[])
}

fn prepare_resolved_line(
	spec: &ResolvedPlotSpec,
	df: &DataFrame,
	series: &str,
	x: &str,
	y: &str,
) -> line::LinePreparedData {
	match (spec.channel(Channel::Color), spec.color_job) {
		(Some(channel), Some(ColorJob::Categorical)) => {
			line::prepare_line_data_with_colors(df, series, x, y, &channel.colors)
		}
		_ => line::prepare_line_data(df, series, x, y),
	}
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
			resolved_category_colors(spec),
		)),
		ResolvedKernelFields::Line { series, x, y } => {
			Ok(Arc::new(LinePlotKernel::with_layer_settings(
				Arc::new(prepare_resolved_line(
					spec,
					df,
					series.as_deref().unwrap_or(""),
					x,
					y,
				)),
				layer_settings,
			)))
		}
		ResolvedKernelFields::StackedBar {
			category,
			group,
			value,
		} => Ok(create_stacked_bar_kernel(
			stacked_bar::prepare_stacked_bar_data(
				df,
				category,
				group.as_deref().unwrap_or(""),
				value,
			),
			plot_type,
			layer_settings,
			&spec.band_fills,
			resolved_category_colors(spec),
			spec.stack_normalization,
		)),
		ResolvedKernelFields::StackedArea { series, x, y } => {
			let assignments = resolved_category_colors(spec);
			let prepared = if assignments.is_empty() {
				stacked_area::prepare_stacked_area_data(df, series.as_deref().unwrap_or(""), x, y)
			} else {
				stacked_area::prepare_stacked_area_data_with_resolved_order(
					df,
					series.as_deref().unwrap_or(""),
					x,
					y,
					assignments,
				)
			};
			Ok(Arc::new(StackedAreaPlotKernel {
				prepared_data: Arc::new(prepared),
				series_colors: assignments.to_vec(),
				normalization: spec.stack_normalization,
			}))
		}
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
	use crate::coordinate::{Pt, Rect, Widget};
	use crate::plot::common::{CoordinateTransformer, PlotLayout, ResolvedPlotSettings};
	use crate::plot::common::{PathBuilder, PlotBackend};
	use crate::plot::composition::PlotComposition;
	use crate::plot::export::SvgBackend;
	use crate::plot::spec::{
		AuthoredPlotSpec, BarWidth, CategoricalPaletteSelection, CategoryGap, Channel,
		ColorIdentity, LayerMark, LayerSettings, LayerSpec, LayeredPlotSpec, Override,
		PalettePosition, PlotProvenance, PointSize, ResolvedColor, StampedResult, resolve_channels,
		resolve_domains_with_categorical_palette,
	};
	use crate::plot::theme::PlotSettings;
	use iced::advanced::mouse::Cursor;
	use iced::{Point, Rectangle, Size};
	use polars::df;

	#[derive(Default)]
	/// PRODUCT-0057 named oracle
	/// `plot::factory::product_0013_baseline_tests::RectangleBackend` records
	/// rectangle tuples only; its shared short name grants no other license.
	struct RectangleBackend {
		rectangles: Vec<(Point, Size, iced::Color)>,
	}

	impl PlotBackend for RectangleBackend {
		fn stroke_path(
			&mut self,
			_f: &dyn Fn(&mut dyn PathBuilder),
			_stroke: iced::widget::canvas::Stroke,
		) {
		}

		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: iced::Color) {}

		fn fill_rectangle(&mut self, top_left: Point, size: Size, color: iced::Color) {
			self.rectangles.push((top_left, size, color));
		}

		fn fill_text(&mut self, _text: iced::widget::canvas::Text) {}

		fn translate(&mut self, _translation: iced::Vector) {}

		fn rotate(&mut self, _angle: f32) {}

		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}

		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn legacy_legend_origin(
		bounds: Rectangle,
		content: &crate::plot::legend::LegendContent,
		settings: &ResolvedPlotSettings,
	) -> Pt<Widget> {
		let bounds = Rect::<Widget>::from_iced_frame(bounds);
		let size = crate::plot::legend::measure_legend(content, settings);
		bounds.origin().offset(
			(bounds.width() - size.width) * settings.legend_x,
			(bounds.height() - size.height) * settings.legend_y,
		)
	}

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
		"<path d=\"M 0.00 2.50 L 45.45 2.50 L 45.45 47.50 L 0.00 47.50 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
		"<path d=\"M 0.00 52.50 L 90.91 52.50 L 90.91 97.50 L 0.00 97.50 Z \" fill=\"rgb(32, 145, 141)\" fill-opacity=\"1.00\" />\n",
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
				group_colors: Vec::new(),
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

	fn product_0047_resolved(frame: &DataFrame, series: &str) -> ResolvedPlotSpec {
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"cat",
			"val",
			Some(series.to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("channels");
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0047".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").expect("palette");
		let (resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.expect("series bar resolution");
		resolved
	}

	#[test]
	fn product_0047_series_bar_groups_by_channel_and_suppresses_band_fills() {
		// PRODUCT-0029 §6: the authored channel is authoritative, band fill absent.
		let frame = df!(
			"cat" => ["A", "A", "B", "B", "C", "C", "D", "D"],
			"series" => ["x", "y", "x", "y", "x", "y", "x", "y"],
			"val" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
		)
		.expect("series bar frame");
		let resolved = product_0047_resolved(&frame, "series");
		assert!(
			resolved.channel(Channel::Color).is_some(),
			"the series field resolves as an authored color channel"
		);
		assert!(
			resolved.band_fills.is_empty(),
			"an authored channel suppresses band fill (PRODUCT-0029 §6)"
		);

		let ResolvedKernelFields::Bar { group, .. } = &*resolved.kernel_fields else {
			panic!("bar kernel fields");
		};
		assert_eq!(
			group.as_deref(),
			Some("series"),
			"the color channel supplies the grouping column"
		);

		// Four categories x two series occupy eight distinct subgroup slots.
		let svg = render_marks(create_plot(&resolved, &frame, 400, 100).as_ref());
		assert_eq!(
			svg.matches("<path").count(),
			8,
			"one rectangle per bar:\n{svg}"
		);

		// PRODUCT-0029 §1: the renderer consumes only the resolved colors. Every
		// painted fill is one of the authored channel's exact assignments, and
		// none is a render-index sample of the legacy ramp.
		let assignments = &resolved
			.channel(Channel::Color)
			.expect("authored channel")
			.colors;
		assert_eq!(assignments.len(), 2, "one assignment per series");
		for assignment in assignments {
			let fill = format!(
				"fill=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(
				svg.contains(&fill),
				"series {} must paint its resolved {fill}:\n{svg}",
				assignment.category
			);
		}
		let legacy = ResolvedPlotSettings::default().color_theme;
		for index in 0..2 {
			let sampled = legacy.get_color(index as f32);
			let ramp = format!(
				"fill=\"rgb({}, {}, {})\"",
				(sampled.r * 255.0).round() as u8,
				(sampled.g * 255.0).round() as u8,
				(sampled.b * 255.0).round() as u8
			);
			assert!(
				!svg.contains(&ramp),
				"a grouped bar must not sample the legacy ramp at index {index}:\n{svg}"
			);
		}
	}

	#[test]
	fn product_0047_single_valued_series_still_suppresses_band_fills() {
		// Suppression keys on channel presence, never on series cardinality: a
		// one-value series field is N = 1 *with* a channel, unlike the two-field
		// bar's N = 1 without one.
		let frame = df!(
			"cat" => ["A", "B", "C"],
			"series" => ["only", "only", "only"],
			"val" => [1.0, 2.0, 3.0]
		)
		.expect("single-series frame");
		let resolved = product_0047_resolved(&frame, "series");
		assert!(resolved.channel(Channel::Color).is_some());
		assert!(
			resolved.band_fills.is_empty(),
			"cardinality one is still an authored channel"
		);
	}

	#[test]
	fn product_0047_series_bar_earns_the_legend_the_two_field_form_declines() {
		// PRODUCT-0044 §5 both halves: the authored channel names identities the
		// axis does not, so this form earns entries; the band-filled form does not.
		let series_frame = df!(
			"cat" => ["A", "A", "B", "B"],
			"series" => ["x", "y", "x", "y"],
			"val" => [1.0, 2.0, 3.0, 4.0]
		)
		.expect("series bar frame");
		let resolved = product_0047_resolved(&series_frame, "series");
		let kernel = create_plot(&resolved, &series_frame, 200, 100);
		let settings = ResolvedPlotSettings::default();
		let content = kernel
			.legend_content(&settings)
			.expect("a series bar earns a legend");
		let crate::plot::legend::LegendSection::Entries(entries) = content.0.first() else {
			panic!("series legend must have entries");
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| entry.label.as_str())
				.collect::<Vec<_>>(),
			["x", "y"],
			"entries are the series values in display-domain order"
		);
		assert!(
			entries
				.iter()
				.all(|entry| entry.provenance == crate::plot::legend::LegendProvenance::Named),
			"series names come from the data, so they survive PRODUCT-0044 §1"
		);

		let band_frame = df!("cat" => ["A", "B"], "val" => [1.0, 2.0]).expect("band frame");
		let authored = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "cat", "val");
		let binding = resolve_channels(&authored, band_frame.schema().as_ref()).expect("channels");
		let stamped = StampedResult {
			data: Arc::new(band_frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0047".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").expect("palette");
		let (band_resolved, _) = resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.expect("band bar resolution");
		assert!(
			create_plot(&band_resolved, &band_frame, 200, 100)
				.legend_content(&settings)
				.is_none(),
			"the two-field bar keeps its legend-free default"
		);
	}

	#[test]
	fn product_0055_real_resolution_drives_grouped_horizontal_geometry_color_hit_and_hover() {
		let frame = df!(
			"category" => ["East", "East", "West", "West"],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"value" => [2.0, 4.0, 6.0, 8.0],
		)
		.unwrap();
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"value",
			"category",
			Some("series".to_owned()),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0055".to_owned(),
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
		assert_eq!(
			resolved.composition.primary_plot_type(),
			PlotType::HorizontalBar,
			"Stage 0 must enter through pair resolution, not a hand-built orientation"
		);
		let assignments = &resolved.channel(Channel::Color).unwrap().colors;
		let layer = LayerSettings {
			bar_width: Override::Set(BarWidth::new(0.5).unwrap()),
			category_gap: Override::Set(CategoryGap::new(0.2).unwrap()),
			..LayerSettings::default()
		};
		let kernel = create_styled_plot(&resolved, &frame, layer)
			.expect("the inherited bar register reaches the resolved horizontal member");
		let plot_settings = PlotSettings::default();
		let resolved_settings = plot_settings.resolve();

		let content = kernel
			.legend_content(&resolved_settings)
			.expect("a grouped horizontal bar earns the existing series legend");
		let crate::plot::legend::LegendSection::Entries(entries) = content.0.first() else {
			panic!("grouped horizontal legend must contain named swatches");
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| (entry.label.as_str(), entry.swatch))
				.collect::<Vec<_>>(),
			assignments
				.iter()
				.map(|assignment| {
					(
						assignment.category.as_str(),
						iced::Color::from_rgba8(
							assignment.color.red,
							assignment.color.green,
							assignment.color.blue,
							assignment.color.alpha as f32 / u8::MAX as f32,
						),
					)
				})
				.collect::<Vec<_>>()
		);

		for height in [100.0, 300.0] {
			let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, height));
			let layout = kernel.layout(resolved_settings.clone());
			let x_range = match &layout {
				PlotLayout::CategoricalY { x_range, .. } => *x_range,
				_ => panic!("canonical HorizontalBar must construct CategoricalY"),
			};
			let transform = CoordinateTransformer::new(&layout, bounds);
			let mut backend = RectangleBackend::default();
			kernel.plot_with_settings(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				&plot_settings,
			);
			assert_eq!(backend.rectangles.len(), 4);
			for (index, (_, _, actual)) in backend.rectangles.iter().enumerate() {
				let expected = &assignments[index % assignments.len()].color;
				assert_eq!(
					*actual,
					iced::Color::from_rgba8(
						expected.red,
						expected.green,
						expected.blue,
						expected.alpha as f32 / u8::MAX as f32,
					),
					"paint must consume the exact resolved series assignment"
				);
			}

			for category_index in 0..2 {
				let mut intervals = backend.rectangles[category_index * 2..category_index * 2 + 2]
					.iter()
					.map(|(origin, size, _)| (origin.y, origin.y + size.height))
					.collect::<Vec<_>>();
				intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
				let centroid = (intervals[0].0 + intervals[1].1) / 2.0;
				let transformed = transform.categorical(category_index, x_range.0).0.y;
				assert!(
					(centroid - transformed).abs() < 0.0001,
					"cluster centroid {centroid} must equal transformer answer {transformed}"
				);
				let band = height / 2.0;
				let slot = band * 0.8 / 2.0;
				assert!((intervals[1].0 - intervals[0].1 - slot * 0.5).abs() < 0.0001);
			}

			let (origin, size, _) = backend.rectangles[0];
			let visible = Point::new(origin.x + size.width / 2.0, origin.y + size.height / 2.0);
			let hover = kernel
				.hover_with_settings(&transform, Cursor::Available(visible), &plot_settings)
				.expect("painted rectangle must hit");
			let expected_value =
				x_range.0 + visible.x as f64 / bounds.width as f64 * (x_range.1 - x_range.0);
			assert!(
				hover.contains(&format!("Value: ~{expected_value:.2}")),
				"horizontal hover must derive value from x_range and cursor x: {hover}"
			);
			let reserve = Point::new(visible.x, height / 2.0 + 1.0);
			assert_eq!(
				kernel.hover_with_settings(&transform, Cursor::Available(reserve), &plot_settings,),
				None,
				"category reserve is neither paint nor hit"
			);
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
			group_colors: Vec::new(),
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
				["M 0.00 12.50 L 45.45", "M 0.00 62.50 L 90.91"],
				["M 0.00 0.00 L 45.45", "M 0.00 50.00 L 90.91"],
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
				Point::new(20.0, 20.0),
				Point::new(20.0, 10.0),
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
				..
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
					legacy_legend_origin(
						Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
						&content,
						&settings,
					),
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
					legacy_legend_origin(
						Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
						&content,
						&settings,
					),
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
					legacy_legend_origin(
						Rectangle::new(Point::ORIGIN, Size::new(100.0, 100.0)),
						&content,
						&settings,
					),
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
				legacy_legend_origin(
					Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
					&content,
					&settings,
				),
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
			group_colors: Vec::new(),
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
			group_colors: Vec::new(),
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
		for plot_type in [PlotType::Heatmap, PlotType::Hexbin] {
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
				legacy_legend_origin(
					Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
					&content,
					&settings,
				),
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
			legacy_legend_origin(
				Rectangle::new(Point::ORIGIN, Size::new(400.0, 300.0)),
				&content,
				&settings,
			),
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

	fn product_0054_resolved_line(
		frame: &DataFrame,
		composition: PlotComposition,
		identity: &ColorIdentity,
	) -> (ResolvedPlotSpec, ColorIdentity) {
		let authored = AuthoredPlotSpec::xy_color(composition, "x", "y", Some("series".to_owned()));
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("line channels");
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0054".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("tab20c").unwrap();
		resolve_domains_with_categorical_palette(binding, &stamped, identity, &selection)
			.expect("series line resolution")
	}

	#[test]
	fn product_0054_factory_routes_resolved_colors_to_line_paint_and_legend() {
		let frame = df!(
			"series" => ["Atlas", "Beacon", "Cinder", "Atlas", "Beacon", "Cinder"],
			"x" => [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
			"y" => [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
		)
		.expect("series line frame");
		let (resolved, _) = product_0054_resolved_line(
			&frame,
			PlotComposition::single(PlotType::Line),
			&ColorIdentity::default(),
		);
		let assignments = resolved
			.channel(Channel::Color)
			.expect("authored line color channel")
			.colors
			.clone();
		let kernel = create_plot(&resolved, &frame, 100, 100);
		let svg = render_marks(kernel.as_ref());
		let settings = ResolvedPlotSettings::default();
		let content = kernel
			.legend_content(&settings)
			.expect("series line legend");
		let crate::plot::legend::LegendSection::Entries(entries) = content.0.first() else {
			panic!("series line entries")
		};
		assert_eq!(entries.len(), assignments.len());
		for (entry, assignment) in entries.iter().zip(&assignments) {
			assert_eq!(entry.label, assignment.category);
			assert_eq!(
				entry.swatch,
				iced::Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / 255.0,
				)
			);
			let stroke = format!(
				"stroke=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(svg.contains(&stroke), "missing {stroke}:\n{svg}");
		}
	}

	fn product_0070_resolved_area(
		frame: &DataFrame,
		series: Option<&str>,
	) -> (ResolvedPlotSpec, ColorIdentity) {
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::StackedArea),
			"x",
			"y",
			series.map(str::to_owned),
		);
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("area channels");
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0070".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("blues").unwrap();
		resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.expect("area domain resolution")
	}

	#[test]
	fn product_0070_factory_routes_resolved_area_colors_to_fills_and_named_swatches() {
		let frame = df!(
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"x" => [1.0, 1.0, 2.0, 2.0],
			"y" => [2.0, 3.0, 4.0, 5.0],
		)
		.expect("area frame");
		let (resolved, _) = product_0070_resolved_area(&frame, Some("series"));
		let assignments = resolved_category_colors(&resolved).to_vec();
		let kernel = create_plot(&resolved, &frame, 100, 100);
		let svg = render_marks(kernel.as_ref());
		let content = kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("named area legend");
		let crate::plot::legend::LegendSection::Entries(entries) = content.0.first() else {
			panic!("area legend is categorical")
		};
		assert_eq!(entries.len(), assignments.len());
		for (entry, assignment) in entries.iter().zip(&assignments) {
			let expected = iced::Color::from_rgba8(
				assignment.color.red,
				assignment.color.green,
				assignment.color.blue,
				assignment.color.alpha as f32 / 255.0,
			);
			assert_eq!(entry.label, assignment.category);
			assert_eq!(entry.swatch, expected);
			let fill = format!(
				"fill=\"rgb({}, {}, {})\"",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(
				svg.contains(&fill),
				"resolved area fill missing {fill}:\n{svg}"
			);
		}

		let (single, _) = product_0070_resolved_area(&frame, None);
		let single = create_plot(&single, &frame, 100, 100);
		let content = single
			.legend_content(&ResolvedPlotSettings::default())
			.expect("kernel exposes synthetic content before policy");
		assert!(
			crate::plot::legend::apply_policy(content).is_none(),
			"two-field area contributes no retained legend"
		);
	}

	#[test]
	fn product_0054_filter_and_restore_keep_line_identity_at_the_renderer_boundary() {
		fn mapping(spec: &ResolvedPlotSpec, frame: &DataFrame) -> Vec<(String, iced::Color)> {
			let prepared = line::prepare_line_data_with_colors(
				frame,
				"series",
				"x",
				"y",
				resolved_category_colors(spec),
			);
			prepared
				.series
				.into_iter()
				.map(|series| (series.name, series.resolved_color.unwrap()))
				.collect()
		}

		let full = df!(
			"series" => ["Atlas", "Beacon", "Cinder", "Atlas", "Beacon", "Cinder"],
			"x" => [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
			"y" => [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
		)
		.expect("full line frame");
		let filtered = df!(
			"series" => ["Cinder", "Atlas", "Cinder", "Atlas"],
			"x" => [1.0, 1.0, 2.0, 2.0],
			"y" => [4.0, 2.0, 7.0, 5.0]
		)
		.expect("filtered line frame");
		let composition = PlotComposition::single(PlotType::Line);
		let (first, identity) =
			product_0054_resolved_line(&full, composition.clone(), &ColorIdentity::default());
		let (middle, identity) =
			product_0054_resolved_line(&filtered, composition.clone(), &identity);
		let (restored, _) = product_0054_resolved_line(&full, composition, &identity);
		let first = mapping(&first, &full);
		let middle = mapping(&middle, &filtered);
		let restored = mapping(&restored, &full);
		assert_eq!(first, restored);
		for (name, color) in middle {
			assert_eq!(
				color,
				first
					.iter()
					.find(|(candidate, _)| candidate == &name)
					.expect("retained identity")
					.1
			);
		}
	}

	#[test]
	fn product_0054_scatter_line_overlay_shares_each_resolved_series_color() {
		let frame = df!(
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"x" => [1.0, 1.0, 2.0, 2.0],
			"y" => [2.0, 3.0, 4.0, 5.0]
		)
		.expect("overlay frame");
		let mut composition = PlotComposition::single(PlotType::Scatter);
		assert!(composition.add_layer(PlotType::Line));
		let (resolved, _) =
			product_0054_resolved_line(&frame, composition, &ColorIdentity::default());
		let assignments = resolved_category_colors(&resolved).to_vec();
		let svg = render_marks(create_plot(&resolved, &frame, 100, 100).as_ref());
		for assignment in assignments {
			let rgb = format!(
				"rgb({}, {}, {})",
				assignment.color.red, assignment.color.green, assignment.color.blue
			);
			assert!(svg.contains(&format!("fill=\"{rgb}\"")), "{svg}");
			assert!(svg.contains(&format!("stroke=\"{rgb}\"")), "{svg}");
		}
	}

	#[test]
	fn product_0054_standalone_styled_factory_admits_the_series_line() {
		let frame = df!(
			"series" => ["Atlas", "Atlas", "Beacon", "Beacon"],
			"x" => [1.0, 2.0, 1.0, 2.0],
			"y" => [2.0, 4.0, 3.0, 5.0]
		)
		.expect("styled line frame");
		let (resolved, _) = product_0054_resolved_line(
			&frame,
			PlotComposition::single(PlotType::Line),
			&ColorIdentity::default(),
		);
		let settings = LayerSettings {
			stroke_width: Override::Set(crate::plot::spec::StrokeWidth::new(2.0).unwrap()),
			..LayerSettings::default()
		};
		let kernel = create_styled_plot(&resolved, &frame, settings).expect("styled series line");
		let svg = render_marks_with_settings(kernel.as_ref(), &PlotSettings::default());
		assert_eq!(svg.matches("stroke-width=\"4\"").count(), 2, "{svg}");
	}

	fn product_0064_resolved_stacked(
		frame: &DataFrame,
		x: &str,
		y: &str,
		series: Option<&str>,
	) -> crate::plot::spec::ResolvedPlotSpec {
		let authored = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			x,
			y,
			series.map(str::to_owned),
		)
		.with_stacked_bar();
		let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0064".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("blues").unwrap();
		resolve_domains_with_categorical_palette(
			binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap()
		.0
	}

	#[test]
	fn product_0064_real_surface_joins_geometry_hit_color_legend_and_orientation() {
		let frame = df!(
			"category" => ["East", "East", "West", "West"],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"value" => [10.0, 30.0, 20.0, 40.0],
		)
		.unwrap();
		let layer = LayerSettings {
			bar_width: Override::Set(BarWidth::new(0.5).unwrap()),
			category_gap: Override::Set(CategoryGap::new(0.2).unwrap()),
			..LayerSettings::default()
		};
		let plot_settings = PlotSettings::default();
		for (x, y, expected_type, orientation) in [
			(
				"category",
				"value",
				PlotType::StackedBar,
				Orientation::Vertical,
			),
			(
				"value",
				"category",
				PlotType::HorizontalStackedBar,
				Orientation::Horizontal,
			),
		] {
			let resolved = product_0064_resolved_stacked(&frame, x, y, Some("series"));
			assert_eq!(resolved.composition.primary_plot_type(), expected_type);
			let assignments = &resolved.channel(Channel::Color).unwrap().colors;
			let expected_colors = assignments
				.iter()
				.map(|assignment| {
					iced::Color::from_rgba8(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
						assignment.color.alpha as f32 / u8::MAX as f32,
					)
				})
				.collect::<Vec<_>>();
			let kernel = create_styled_plot(&resolved, &frame, layer.clone()).unwrap();
			let legend = kernel
				.legend_content(&plot_settings.resolve())
				.expect("named stacked series earns a legend");
			let crate::plot::legend::LegendSection::Entries(entries) = legend.0.first() else {
				panic!("stacked series legend entries")
			};
			assert_eq!(
				entries.iter().map(|entry| entry.swatch).collect::<Vec<_>>(),
				expected_colors,
				"legend consumes the exact resolved blues assignments"
			);

			for extent in [200.0_f32, 600.0] {
				let bounds = Rectangle::new(Point::ORIGIN, Size::new(extent, extent));
				let layout = kernel.layout(plot_settings.resolve());
				let transform = CoordinateTransformer::new(&layout, bounds);
				let mut backend = RectangleBackend::default();
				kernel.plot_with_settings(
					&mut backend,
					bounds,
					&transform,
					Cursor::Unavailable,
					&plot_settings,
				);
				assert_eq!(backend.rectangles.len(), 4);
				assert_eq!(
					backend
						.rectangles
						.iter()
						.map(|(_, _, color)| *color)
						.collect::<Vec<_>>(),
					vec![
						expected_colors[0],
						expected_colors[1],
						expected_colors[0],
						expected_colors[1],
					],
					"paint and legend share resolved display-domain order"
				);
				let band = extent / 2.0;
				let expected_cross_extent = band * 0.8 * 0.5;
				for (_, size, _) in &backend.rectangles {
					let actual = match orientation {
						Orientation::Vertical => size.width,
						Orientation::Horizontal => size.height,
					};
					assert!((actual - expected_cross_extent).abs() < 0.0001);
				}

				for (index, expected_hover) in [
					"East: Atlas (Value: 10.00, Total: 10.00)",
					"East: Beacon (Value: 30.00, Total: 40.00)",
					"West: Atlas (Value: 20.00, Total: 20.00)",
					"West: Beacon (Value: 40.00, Total: 60.00)",
				]
				.into_iter()
				.enumerate()
				{
					let (origin, size, _) = backend.rectangles[index];
					let cursor =
						Point::new(origin.x + size.width / 2.0, origin.y + size.height / 2.0);
					assert_eq!(
						kernel
							.hover_with_settings(
								&transform,
								Cursor::Available(cursor),
								&plot_settings
							)
							.as_deref(),
						Some(expected_hover),
						"paint and hit consume one baseline-outward rectangle sequence"
					);
				}

				let (center, band) = transform.categorical(0, 5.0);
				let reserve = match orientation {
					Orientation::Vertical => Point::new(center.x - band * 0.45, center.y),
					Orientation::Horizontal => Point::new(center.x, center.y - band * 0.45),
				};
				assert_eq!(
					kernel.hover_with_settings(
						&transform,
						Cursor::Available(reserve),
						&plot_settings,
					),
					None,
					"category reserve is neither paint nor hit"
				);
			}
		}
	}

	#[test]
	fn product_0064_degenerate_stack_matches_grouped_geometry_and_band_fill() {
		let frame = df!("category" => ["East", "West"], "value" => [10.0, 20.0]).unwrap();
		let grouped_authored =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "category", "value");
		let stacked_resolved = product_0064_resolved_stacked(&frame, "category", "value", None);
		let grouped_binding = resolve_channels(&grouped_authored, frame.schema().as_ref()).unwrap();
		let stamped = StampedResult {
			data: Arc::new(frame.clone()),
			provenance: PlotProvenance {
				source_name: "product-0064 grouped".to_owned(),
				revision: Some(1),
			},
		};
		let selection = CategoricalPaletteSelection::from_theme_name("blues").unwrap();
		let (grouped_resolved, _) = resolve_domains_with_categorical_palette(
			grouped_binding,
			&stamped,
			&ColorIdentity::default(),
			&selection,
		)
		.unwrap();
		assert_eq!(grouped_resolved.band_fills, stacked_resolved.band_fills);
		let layer = LayerSettings {
			bar_width: Override::Set(BarWidth::new(0.6).unwrap()),
			category_gap: Override::Set(CategoryGap::new(0.2).unwrap()),
			..LayerSettings::default()
		};
		let grouped = create_styled_plot(&grouped_resolved, &frame, layer.clone()).unwrap();
		let stacked = create_styled_plot(&stacked_resolved, &frame, layer).unwrap();
		assert_eq!(
			render_marks_with_settings(grouped.as_ref(), &PlotSettings::default()),
			render_marks_with_settings(stacked.as_ref(), &PlotSettings::default()),
			"the one-segment stacked form preserves exact geometry and band-fill color"
		);
		assert!(
			stacked
				.legend_content(&ResolvedPlotSettings::default())
				.is_some(),
			"kernel content is produced before PRODUCT-0044 removes one synthetic entry"
		);
		assert!(
			crate::plot::legend::apply_policy(
				stacked
					.legend_content(&ResolvedPlotSettings::default())
					.unwrap()
			)
			.is_none(),
			"the degenerate stack carries no rendered legend"
		);
	}
}
