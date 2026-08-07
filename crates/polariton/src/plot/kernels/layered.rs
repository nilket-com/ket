#![allow(dead_code)] // PRODUCT-0014 supplies the first production caller.

use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label,
};
use crate::plot::kernels::line::{LineMarkRenderer, LinePreparedData};
use crate::plot::kernels::scatter::{ScatterMarkRenderer, ScatterPreparedData};
use crate::plot::spec::{LayerSettings, NonEmpty, ResolvedLayout};
use crate::plot::theme::PlotSettings;
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use std::sync::Arc;

pub enum LayerRenderer {
	Line(LineMarkRenderer, LayerSettings),
	Scatter(ScatterMarkRenderer, LayerSettings),
}

impl LayerRenderer {
	pub fn line(prepared_data: Arc<LinePreparedData>, settings: LayerSettings) -> Self {
		Self::Line(LineMarkRenderer::new(prepared_data), settings)
	}

	pub fn scatter(prepared_data: Arc<ScatterPreparedData>, settings: LayerSettings) -> Self {
		Self::Scatter(ScatterMarkRenderer::new(prepared_data), settings)
	}

	pub(crate) fn prepared_layout(
		&self,
		viewport_width: u32,
		viewport_height: u32,
	) -> ResolvedLayout {
		let (x_range, y_range) = match self {
			Self::Line(renderer, _) => (
				renderer.prepared_data.x_range,
				renderer.prepared_data.y_range,
			),
			Self::Scatter(renderer, _) => (
				renderer.prepared_data.x_range,
				renderer.prepared_data.y_range,
			),
		};
		ResolvedLayout {
			viewport_width,
			viewport_height,
			x_range,
			y_range,
		}
	}

	fn settings(&self) -> &LayerSettings {
		match self {
			Self::Line(_, settings) | Self::Scatter(_, settings) => settings,
		}
	}

	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	) {
		match self {
			Self::Line(renderer, _) => renderer.draw(backend, bounds, transform, settings),
			Self::Scatter(renderer, _) => renderer.draw(backend, bounds, transform, settings),
		}
	}

	fn hit_test(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<MarkHit> {
		match self {
			Self::Line(renderer, _) => renderer.hit_test(transform, cursor, settings),
			Self::Scatter(renderer, _) => renderer.hit_test(transform, cursor, settings),
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		match self {
			Self::Line(renderer, _) => {
				crate::plot::legend::entries_from_labels_with_sample_and_provenance(
					renderer
						.prepared_data
						.series
						.iter()
						.map(|series| series.name.clone()),
					settings,
					crate::plot::legend::LegendSample::LineSample,
					renderer
						.prepared_data
						.series
						.first()
						.map_or(crate::plot::legend::LegendProvenance::Named, |series| {
							series.provenance
						}),
				)
			}
			Self::Scatter(renderer, _) => {
				crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
					renderer.prepared_data.series.iter().map(|series| {
						(
							series.name.clone(),
							series
								.resolved_color
								.unwrap_or_else(|| settings.color_theme.get_color(series.color_t)),
						)
					}),
					crate::plot::legend::LegendSample::PointSample,
					renderer
						.prepared_data
						.series
						.first()
						.map_or(crate::plot::legend::LegendProvenance::Named, |series| {
							series.provenance
						}),
				)
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
		cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		let settings = PlotSettings::from_resolved(settings);
		self.plot_with_settings(backend, bounds, transform, cursor, &settings);
	}

	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: &PlotSettings,
	) {
		for layer in self.layers.iter() {
			layer.draw(
				backend,
				bounds,
				transform,
				settings.resolve_for_layer(layer.settings()),
			);
		}
	}

	/// PRODUCT-0013 bridge: topmost painted geometric hit wins, and this
	/// composite supplies the existing coordinate wording.
	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		self.hover_with_settings(transform, cursor, &PlotSettings::default())
	}

	fn hover_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<String> {
		let layers = self.layers.iter().collect::<Vec<_>>();
		for layer in layers.into_iter().rev() {
			let resolved = settings.resolve_for_layer(layer.settings());
			if let Some(hit) = layer.hit_test(transform, cursor, &resolved) {
				return Some(format!(
					"X: {}, Y: {}",
					format_label(hit.data_x, self.x_axis_type),
					format_label(hit.data_y, self.y_axis_type)
				));
			}
		}
		None
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::union(
			self.layers
				.iter()
				.filter_map(|layer| layer.legend_content(settings)),
		)
	}

	fn x_label(&self) -> String {
		self.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.y_label.clone()
	}
}
