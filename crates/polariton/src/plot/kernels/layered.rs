#![allow(dead_code)] // PRODUCT-0014 supplies the first production caller.

use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label,
};
use crate::plot::kernels::line::{LineMarkRenderer, LinePreparedData, draw_line_legend};
use crate::plot::kernels::scatter::{
	ScatterMarkRenderer, ScatterPreparedData, draw_scatter_legend,
};
use crate::plot::spec::{NonEmpty, ResolvedLayout};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use std::sync::Arc;

pub enum LayerRenderer {
	Line(LineMarkRenderer),
	Scatter(ScatterMarkRenderer),
}

impl LayerRenderer {
	pub fn line(prepared_data: Arc<LinePreparedData>) -> Self {
		Self::Line(LineMarkRenderer::new(prepared_data))
	}

	pub fn scatter(prepared_data: Arc<ScatterPreparedData>) -> Self {
		Self::Scatter(ScatterMarkRenderer::new(prepared_data))
	}

	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	) {
		match self {
			Self::Line(renderer) => renderer.draw(backend, bounds, transform, settings),
			Self::Scatter(renderer) => renderer.draw(backend, bounds, transform, settings),
		}
	}

	fn hit_test(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<MarkHit> {
		match self {
			Self::Line(renderer) => renderer.hit_test(transform, cursor),
			Self::Scatter(renderer) => renderer.hit_test(transform, cursor),
		}
	}

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: ResolvedPlotSettings,
	) {
		match self {
			Self::Line(renderer) => {
				draw_line_legend(&renderer.prepared_data, backend, bounds, settings)
			}
			Self::Scatter(renderer) => {
				draw_scatter_legend(&renderer.prepared_data, backend, bounds, settings)
			}
		}
	}
}

pub struct LayeredPlotKernel {
	layers: NonEmpty<LayerRenderer>,
	shared_layout: ResolvedLayout,
	x_axis_type: AxisType,
	y_axis_type: AxisType,
	x_label: String,
	y_label: String,
}

impl LayeredPlotKernel {
	pub fn new(
		layers: NonEmpty<LayerRenderer>,
		shared_layout: ResolvedLayout,
		x_axis_type: AxisType,
		y_axis_type: AxisType,
		x_label: String,
		y_label: String,
	) -> Self {
		Self {
			layers,
			shared_layout,
			x_axis_type,
			y_axis_type,
			x_label,
			y_label,
		}
	}
}

impl PlotKernel for LayeredPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.shared_layout.x_range.0),
				settings.x_max.unwrap_or(self.shared_layout.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.shared_layout.y_range.0),
				settings.y_max.unwrap_or(self.shared_layout.y_range.1),
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
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		for layer in self.layers.iter() {
			layer.draw(backend, bounds, transform, settings.clone());
		}
	}

	/// PRODUCT-0013 bridge: topmost painted geometric hit wins, and this
	/// composite supplies the existing coordinate wording.
	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		let layers = self.layers.iter().collect::<Vec<_>>();
		for layer in layers.into_iter().rev() {
			if let Some(hit) = layer.hit_test(transform, cursor) {
				return Some(format!(
					"X: {}, Y: {}",
					format_label(hit.data_x, self.x_axis_type),
					format_label(hit.data_y, self.y_axis_type)
				));
			}
		}
		None
	}

	/// PRODUCT-0013 bridge: the last authored (topmost painted) layer owns the
	/// temporary, unmerged legend output.
	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: ResolvedPlotSettings,
	) {
		self.layers.last().draw_legend(backend, bounds, settings);
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}
