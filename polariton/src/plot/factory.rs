use crate::plot::common::{Orientation, PlotKernel};
use crate::plot::composition::PlotComposition;
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
use polars::frame::DataFrame;
use polars::prelude::{Column, DataType};
use std::sync::Arc;

pub fn create_plot(
	composition: &PlotComposition,
	df: &DataFrame,
	_width: u32,
	_height: u32,
) -> Arc<dyn PlotKernel + Send + Sync> {
	if composition.is_fill_between_with_line_overlay() {
		return create_fill_between_line_plot(df);
	}
	if composition.is_scatter_with_line_overlay() {
		return create_scatter_line_plot(df);
	}

	create_single_plot(composition.primary_plot_type(), df)
}

fn create_single_plot(
	plot_type: PlotType,
	df: &DataFrame,
) -> Arc<dyn PlotKernel + Send + Sync> {
	let string_cols: Vec<&str> = df
		.columns()
		.iter()
		.filter(|c: &&Column| matches!(c.dtype(), DataType::String | DataType::Categorical(_, _)))
		.map(|c: &Column| c.name().as_str())
		.collect();
	let numeric_cols: Vec<&str> = df
		.columns()
		.iter()
		.filter(|c: &&Column| c.dtype().is_numeric())
		.map(|c: &Column| c.name().as_str())
		.collect();

	match plot_type {
		PlotType::Violin => {
			let cat = string_cols.first().copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = violin::prepare_violin_data(df, cat, val, None);
			Arc::new(ViolinPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Hexbin => {
			let prepared = hexbin::prepare_hexbin_data(df, 0.02);
			Arc::new(HexbinPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Line => {
			let cat = string_cols.first().copied().unwrap_or("");
			let x = numeric_cols.first().copied().unwrap_or("");
			let y = numeric_cols.get(1).copied().unwrap_or(x);
			let prepared = line::prepare_line_data(df, cat, x, y);
			Arc::new(LinePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Bar => {
			let cat = string_cols.first().copied().unwrap_or("");
			let group = string_cols.get(1).copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = bar::prepare_bar_data(df, cat, group, val);
			Arc::new(BarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: Orientation::Vertical,
			})
		}
		PlotType::HorizontalBar => {
			let cat = string_cols.first().copied().unwrap_or("");
			let group = string_cols.get(1).copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = bar::prepare_bar_data(df, cat, group, val);
			Arc::new(BarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: Orientation::Horizontal,
			})
		}
		PlotType::Scatter => {
			let cat = string_cols.first().copied().unwrap_or("");
			let x = numeric_cols.first().copied().unwrap_or("");
			let y = numeric_cols.get(1).copied().unwrap_or(x);
			let prepared = scatter::prepare_scatter_data(df, cat, x, y, 3.0);
			Arc::new(ScatterPlotKernel::new(Arc::new(prepared)))
		}
		PlotType::StackedBar => {
			let cat = string_cols.first().copied().unwrap_or("");
			let group = string_cols.get(1).copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = stacked_bar::prepare_stacked_bar_data(df, cat, group, val);
			Arc::new(StackedBarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: Orientation::Vertical,
			})
		}
		PlotType::HorizontalStackedBar => {
			let cat = string_cols.first().copied().unwrap_or("");
			let group = string_cols.get(1).copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = stacked_bar::prepare_stacked_bar_data(df, cat, group, val);
			Arc::new(StackedBarPlotKernel {
				prepared_data: Arc::new(prepared),
				orientation: Orientation::Horizontal,
			})
		}
		PlotType::Pie => {
			let cat = string_cols.first().copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = pie::prepare_pie_data(df, cat, val);
			Arc::new(PiePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::BoxPlot => {
			let cat = string_cols.first().copied().unwrap_or("");
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = boxplot::prepare_box_plot_data(df, cat, val);
			Arc::new(BoxPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Histogram => {
			let val = numeric_cols.first().copied().unwrap_or("");
			let prepared = histogram::prepare_histogram_data(df, val, 16);
			Arc::new(HistogramPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Bubble => {
			let x = numeric_cols.first().copied().unwrap_or("");
			let y = numeric_cols.get(1).copied().unwrap_or(x);
			let size = numeric_cols.get(2).copied().unwrap_or(y);
			let color = numeric_cols.get(3).copied().unwrap_or(size);
			let label = string_cols.first().copied();
			let prepared = bubble::prepare_bubble_data(df, x, y, size, color, label);
			Arc::new(BubblePlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Heatmap => {
			let x = string_cols.first().copied().unwrap_or("");
			let y = string_cols.get(1).copied().unwrap_or("");
			let z = numeric_cols.first().copied().unwrap_or("");
			let prepared = heatmap::prepare_heatmap_data(df, x, y, z);
			Arc::new(HeatmapPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Parallel => {
			let dims: Vec<String> = numeric_cols.iter().map(|col| (*col).to_string()).collect();
			let prepared =
				parallel::prepare_parallel_data(df, &dims, string_cols.first().copied().unwrap_or(""));
			Arc::new(ParallelPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Radar => {
			let dims: Vec<String> = numeric_cols.iter().map(|col| (*col).to_string()).collect();
			let prepared =
				radar::prepare_radar_data(df, &dims, string_cols.first().copied().unwrap_or(""));
			Arc::new(RadarPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::RadialDial => {
			let label = string_cols.first().copied().unwrap_or("");
			let value = numeric_cols.first().copied().unwrap_or("");
			let max_value = numeric_cols.get(1).copied().unwrap_or(value);
			let prepared = radial_dial::prepare_radial_dial_data(df, label, value, max_value);
			Arc::new(RadialDialPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Candlestick => {
			let x = string_cols.first().copied().unwrap_or("");
			let open = numeric_cols.first().copied().unwrap_or("");
			let high = numeric_cols.get(1).copied().unwrap_or(open);
			let low = numeric_cols.get(2).copied().unwrap_or(high);
			let close = numeric_cols.get(3).copied().unwrap_or(low);
			let prepared = candlestick::prepare_candlestick_data(df, x, open, high, low, close);
			Arc::new(CandlestickPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::Funnel => {
			let stage = string_cols.first().copied().unwrap_or("");
			let value = numeric_cols.first().copied().unwrap_or("");
			let prepared = funnel::prepare_funnel_data(df, stage, value);
			Arc::new(FunnelPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::StackedArea => {
			let x = string_cols.first().copied().unwrap_or("");
			let group = string_cols.get(1).copied().unwrap_or("");
			let y = numeric_cols.first().copied().unwrap_or("");
			let prepared = stacked_area::prepare_stacked_area_data(df, x, group, y);
			Arc::new(StackedAreaPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
		PlotType::FillBetween => {
			let x = numeric_cols.first().copied().unwrap_or("");
			let y_mid = numeric_cols.get(1).copied().unwrap_or(x);
			let y_lower = numeric_cols.get(2).copied().unwrap_or(y_mid);
			let y_upper = numeric_cols.get(3).copied().unwrap_or(y_lower);
			let prepared =
				fill_between::prepare_fill_between_data(df, x, y_mid, y_lower, y_upper);
			Arc::new(FillBetweenPlotKernel {
				prepared_data: Arc::new(prepared),
			})
		}
	}
}

fn create_scatter_line_plot(df: &DataFrame) -> Arc<dyn PlotKernel + Send + Sync> {
	let string_cols: Vec<&str> = df
		.columns()
		.iter()
		.filter(|c: &&Column| matches!(c.dtype(), DataType::String | DataType::Categorical(_, _)))
		.map(|c: &Column| c.name().as_str())
		.collect();
	let numeric_cols: Vec<&str> = df
		.columns()
		.iter()
		.filter(|c: &&Column| c.dtype().is_numeric())
		.map(|c: &Column| c.name().as_str())
		.collect();
	let cat = string_cols.first().copied().unwrap_or("");
	let x = numeric_cols.first().copied().unwrap_or("");
	let y = numeric_cols.get(1).copied().unwrap_or(x);
	let scatter_data = Arc::new(scatter::prepare_scatter_data(df, cat, x, y, 3.0));
	let line_data = Arc::new(line::prepare_line_data(df, cat, x, y));
	Arc::new(ScatterLinePlotKernel::new(scatter_data, line_data))
}

fn create_fill_between_line_plot(df: &DataFrame) -> Arc<dyn PlotKernel + Send + Sync> {
	let numeric_cols: Vec<&str> = df
		.columns()
		.iter()
		.filter(|c: &&Column| c.dtype().is_numeric())
		.map(|c: &Column| c.name().as_str())
		.collect();
	let x = numeric_cols.first().copied().unwrap_or("");
	let y_mid = numeric_cols.get(1).copied().unwrap_or(x);
	let y_lower = numeric_cols.get(2).copied().unwrap_or(y_mid);
	let y_upper = numeric_cols.get(3).copied().unwrap_or(y_lower);
	let fill_between_data =
		Arc::new(fill_between::prepare_fill_between_data(df, x, y_mid, y_lower, y_upper));
	let line_data = Arc::new(line::prepare_line_data(df, "", x, y_mid));
	Arc::new(FillBetweenLinePlotKernel::new(fill_between_data, line_data))
}
