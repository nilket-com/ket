#![allow(dead_code)] // PRODUCT-0014 supplies the first production caller.

use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, TemporalAxisDomain, format_label,
};
use crate::plot::kernels::line::{LineMarkRenderer, LinePreparedData, series_color};
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

	/// PRODUCT-0139 §3: the declaration belongs to the mark, so this asks the
	/// renderer rather than answering for it.
	fn requires_plot_area_clip(&self) -> bool {
		match self {
			Self::Line(renderer, _) => renderer.requires_plot_area_clip(),
			Self::Scatter(renderer, _) => renderer.requires_plot_area_clip(),
		}
	}

	fn mark_family(&self) -> crate::plot::common::MarkFamily {
		match self {
			Self::Line(_, _) => crate::plot::common::MarkFamily::LINE,
			Self::Scatter(_, _) => crate::plot::common::MarkFamily::SCATTER,
		}
	}

	fn dimensionality(&self) -> crate::plot::common::HitDimensionality {
		match self {
			Self::Line(_, _) => crate::plot::common::HitDimensionality::Line,
			Self::Scatter(_, _) => crate::plot::common::HitDimensionality::Point,
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

	fn collect_raster_want(
		&self,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &PlotSettings,
		slot: &crate::plot::raster::RasterSlotId,
		out: &mut Vec<crate::plot::raster::ScatterRasterWant>,
	) {
		if let Self::Scatter(renderer, layer) = self {
			let resolved = settings.resolve_for_layer(layer);
			renderer.collect_raster_want(bounds, transform, resolved, slot, out);
		}
	}

	fn draw_interactive(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &PlotSettings,
		reuse_policy: crate::plot::common::RasterReusePolicy,
		coordinator: &crate::plot::raster::RasterCoordinator,
		slot: &crate::plot::raster::RasterSlotId,
		preparing: &mut bool,
	) {
		let resolved = settings.resolve_for_layer(self.settings());
		match self {
			Self::Line(renderer, _) => renderer.draw(backend, bounds, transform, resolved),
			Self::Scatter(renderer, _) => renderer.draw_interactive(
				backend,
				bounds,
				transform,
				resolved,
				reuse_policy,
				coordinator,
				slot,
				preparing,
			),
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
				crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
					renderer
						.prepared_data
						.series
						.iter()
						.map(|series| (series.name.clone(), series_color(series, settings))),
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
	x_temporal_domain: Option<TemporalAxisDomain>,
	y_temporal_domain: Option<TemporalAxisDomain>,
	x_label: String,
	y_label: String,
}

impl LayeredPlotKernel {
	pub fn new(
		layers: NonEmpty<LayerRenderer>,
		shared_layout: ResolvedLayout,
		x_axis_type: AxisType,
		y_axis_type: AxisType,
		x_temporal_domain: Option<TemporalAxisDomain>,
		y_temporal_domain: Option<TemporalAxisDomain>,
		x_label: String,
		y_label: String,
	) -> Self {
		Self {
			layers,
			shared_layout,
			x_axis_type,
			y_axis_type,
			x_temporal_domain,
			y_temporal_domain,
			x_label,
			y_label,
		}
	}
}

impl PlotKernel for LayeredPlotKernel {
	/// PRODUCT-0139 §3: any layer whose mark must be clipped clips the figure.
	/// A one-layer wrapper around a scatter therefore clips exactly as the bare
	/// scatter kernel does, which is what makes the two authoring spellings of
	/// one mark agree.
	fn requires_plot_area_clip(&self) -> bool {
		self.layers
			.iter()
			.any(LayerRenderer::requires_plot_area_clip)
	}

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

	fn collect_raster_wants(
		&self,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &PlotSettings,
		slot: &crate::plot::raster::RasterSlotId,
		out: &mut Vec<crate::plot::raster::ScatterRasterWant>,
	) {
		for (index, layer) in self.layers.iter().enumerate() {
			layer.collect_raster_want(bounds, transform, settings, &slot.child(index), out);
		}
	}

	fn plot_interactive(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: &PlotSettings,
		_motion: crate::plot::common::ViewportMotion,
		reuse_policy: crate::plot::common::RasterReusePolicy,
		coordinator: &crate::plot::raster::RasterCoordinator,
		slot: &crate::plot::raster::RasterSlotId,
		preparing: &mut bool,
	) {
		for (index, layer) in self.layers.iter().enumerate() {
			layer.draw_interactive(
				backend,
				bounds,
				transform,
				settings,
				reuse_policy,
				coordinator,
				&slot.child(index),
				preparing,
			);
		}
	}

	/// PRODUCT-0133: dimensional specificity wins before paint order, then
	/// sibling order. A point therefore remains selectable when it overlaps a
	/// line, regardless of authored layer order.
	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		self.hit_with_settings(transform, cursor, &PlotSettings::default())
	}

	fn hit_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let layers = self.layers.iter().collect::<Vec<_>>();
		let mut candidates = Vec::new();
		for (paint_order, layer) in layers.into_iter().enumerate() {
			let resolved = settings.resolve_for_layer(layer.settings());
			if let Some(hit) = layer.hit_test(transform, cursor, &resolved) {
				let candidate = crate::plot::common::PlotHit::new(
					layer.mark_family(),
					layer.dimensionality(),
					hit.mark_index,
				)
				.with_paint_order(paint_order)
				.labeled(
					"X",
					crate::plot::common::axis_hit_semantic_type(&self.x_axis_type),
					crate::plot::common::axis_hit_value(
						hit.data_x,
						&self.x_axis_type,
						format_label(hit.data_x, &self.x_axis_type),
					),
				)
				.labeled(
					"Y",
					crate::plot::common::axis_hit_semantic_type(&self.y_axis_type),
					crate::plot::common::axis_hit_value(
						hit.data_y,
						&self.y_axis_type,
						format_label(hit.data_y, &self.y_axis_type),
					),
				);
				candidates.push(candidate);
			}
		}
		crate::plot::common::PlotHit::best_of(candidates.into_iter().map(Some))
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
