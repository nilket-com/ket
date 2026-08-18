use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	TemporalAxisDomain,
};
use crate::plot::kernels::line::{LinePlotKernel, LinePreparedData};
use crate::plot::kernels::scatter::{ScatterPlotKernel, ScatterPreparedData};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use std::sync::Arc;

pub struct ScatterLinePlotKernel {
	scatter: ScatterPlotKernel,
	line: LinePlotKernel,
	x_range: (f64, f64),
	y_range: (f64, f64),
	x_axis_type: AxisType,
	y_axis_type: AxisType,
	x_temporal_domain: Option<TemporalAxisDomain>,
	y_temporal_domain: Option<TemporalAxisDomain>,
	x_label: String,
	y_label: String,
}

impl ScatterLinePlotKernel {
	pub fn new(scatter_data: Arc<ScatterPreparedData>, line_data: Arc<LinePreparedData>) -> Self {
		let x_range = (
			scatter_data.x_range.0.min(line_data.x_range.0),
			scatter_data.x_range.1.max(line_data.x_range.1),
		);
		let y_range = (
			scatter_data.y_range.0.min(line_data.y_range.0),
			scatter_data.y_range.1.max(line_data.y_range.1),
		);
		Self {
			x_range,
			y_range,
			x_axis_type: scatter_data.x_axis_type.clone(),
			y_axis_type: scatter_data.y_axis_type.clone(),
			x_temporal_domain: scatter_data.x_temporal_domain.clone(),
			y_temporal_domain: scatter_data.y_temporal_domain.clone(),
			x_label: scatter_data.x_label.clone(),
			y_label: scatter_data.y_label.clone(),
			scatter: ScatterPlotKernel::new(scatter_data),
			line: LinePlotKernel::new(line_data),
		}
	}
}

impl PlotKernel for ScatterLinePlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.x_range.0),
				settings.x_max.unwrap_or(self.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.y_range.0),
				settings.y_max.unwrap_or(self.y_range.1),
			),
			x_axis_type: self.x_axis_type.clone(),
			y_axis_type: self.y_axis_type.clone(),
			x_temporal_domain: self.x_temporal_domain.clone(),
			y_temporal_domain: self.y_temporal_domain.clone(),
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		self.scatter
			.plot(backend, bounds, transform, cursor, settings.clone());
		self.line.plot(backend, bounds, transform, cursor, settings);
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		self.scatter.legend_content(settings)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		self.scatter.hover(transform, cursor)
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}
