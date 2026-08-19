use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	TemporalAxisDomain,
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
	x_temporal_domain: Option<TemporalAxisDomain>,
	y_temporal_domain: Option<TemporalAxisDomain>,
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
			x_axis_type: fill_between_data.x_axis_type.clone(),
			y_axis_type: fill_between_data.y_axis_type.clone(),
			x_temporal_domain: fill_between_data.x_temporal_domain.clone(),
			y_temporal_domain: fill_between_data.y_temporal_domain.clone(),
			x_label: fill_between_data.x_label.clone(),
			y_label: fill_between_data.y_label.clone(),
			fill_between: FillBetweenPlotKernel {
				prepared_data: fill_between_data,
			},
			line: LinePlotKernel::new(line_data),
		}
	}
}

impl PlotKernel for FillBetweenLinePlotKernel {
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
		self.fill_between
			.plot(backend, bounds, transform, cursor, settings.clone());
		self.line.plot(backend, bounds, transform, cursor, settings);
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		crate::plot::common::PlotHit::best_of([
			self.fill_between
				.hit(transform, cursor)
				.map(|hit| hit.with_paint_order(0)),
			self.line
				.hit(transform, cursor)
				.map(|hit| hit.with_paint_order(1)),
		])
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}
