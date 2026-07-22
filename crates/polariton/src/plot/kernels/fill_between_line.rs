use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use crate::plot::kernels::fill_between::{FillBetweenPlotKernel, FillBetweenPreparedData};
use crate::plot::kernels::line::{LinePlotKernel, LinePreparedData};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use std::sync::Arc;

pub struct FillBetweenLinePlotKernel {
	fill_between: FillBetweenPlotKernel,
	line: LinePlotKernel,
	x_range: (f64, f64),
	y_range: (f64, f64),
	x_axis_type: AxisType,
	y_axis_type: AxisType,
	x_label: String,
	y_label: String,
}

impl FillBetweenLinePlotKernel {
	pub fn new(
		fill_between_data: Arc<FillBetweenPreparedData>,
		line_data: Arc<LinePreparedData>,
	) -> Self {
		let x_range = (
			fill_between_data.x_range.0.min(line_data.x_range.0),
			fill_between_data.x_range.1.max(line_data.x_range.1),
		);
		let y_range = (
			fill_between_data.y_range.0.min(line_data.y_range.0),
			fill_between_data.y_range.1.max(line_data.y_range.1),
		);
		Self {
			x_range,
			y_range,
			x_axis_type: fill_between_data.x_axis_type,
			y_axis_type: fill_between_data.y_axis_type,
			x_label: fill_between_data.x_label.clone(),
			y_label: fill_between_data.y_label.clone(),
			fill_between: FillBetweenPlotKernel {
				prepared_data: fill_between_data,
			},
			line: LinePlotKernel {
				prepared_data: line_data,
			},
		}
	}
}

impl PlotKernel for FillBetweenLinePlotKernel {
	fn layout(&self, settings: PlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.x_range.0),
				settings.x_max.unwrap_or(self.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.y_range.0),
				settings.y_max.unwrap_or(self.y_range.1),
			),
			x_axis_type: self.x_axis_type,
			y_axis_type: self.y_axis_type,
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: PlotSettings,
	) {
		self.fill_between
			.plot(backend, bounds, transform, cursor, settings.clone());
		self.line.plot(backend, bounds, transform, cursor, settings);
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		self.fill_between.hover(transform, cursor)
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}
