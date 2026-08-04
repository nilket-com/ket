use crate::plot::common::{Orientation, PlotKernel};
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
	ColorIdentity, LayeredPlotSpec, PlotProvenance, ResolvedKernelFields, ResolvedPlotSpec,
	StampedResult, resolve_domains, resolve_layered_channels,
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
		} => {
			let prepared =
				bar::prepare_bar_data(df, category, group.as_deref().unwrap_or(""), value);
			Arc::new(BarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: if plot_type == PlotType::HorizontalBar {
					Orientation::Horizontal
				} else {
					Orientation::Vertical
				},
			})
		}
		ResolvedKernelFields::Scatter { color, x, y } => {
			let scatter_data = Arc::new(scatter::prepare_scatter_data_resolved(
				df, spec, width, height, 3.0,
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

/// Lower PRODUCT-0013's additive layered specification. The multi-layer branch
/// is completed by the ordered composite stage; one layer deliberately reuses
/// the established kernel path.
pub fn create_layered_plot(
	spec: &LayeredPlotSpec,
	df: &DataFrame,
	width: u32,
	height: u32,
) -> Result<Arc<dyn PlotKernel + Send + Sync>, String> {
	let binding =
		resolve_layered_channels(spec, df.schema().as_ref()).map_err(|e| e.to_string())?;
	let (first, rest) = binding.layers.into_parts();
	if !rest.is_empty() {
		return Err(
			"multi-layer rendering is not available until the ordered composite stage".into(),
		);
	}
	let stamped = StampedResult {
		data: Arc::new(df.clone()),
		provenance: PlotProvenance {
			source_name: "layered plot".to_owned(),
			revision: None,
		},
	};
	let (resolved, _) = resolve_domains(first.into_binding(), &stamped, &ColorIdentity::default())
		.map_err(|e| e.to_string())?;
	Ok(create_plot(&resolved, df, width, height))
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
	use crate::plot::common::{CoordinateTransformer, ResolvedPlotSettings};
	use crate::plot::composition::PlotComposition;
	use crate::plot::export::SvgBackend;
	use crate::plot::spec::{AuthoredPlotSpec, LayerMark, LayerSpec, LayeredPlotSpec};
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

	fn fixture() -> DataFrame {
		df!("x" => [1.0, 2.0], "y" => [2.0, 4.0]).expect("frame")
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
}
