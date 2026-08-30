use crate::plot::common::{
	AxisType, CoordinateTransformer, HitDimensionality, HitValue, MarkFamily, PlotBackend, PlotHit,
	PlotKernel, PlotLayout, ResolvedPlotSettings, ViewportMotion,
};
use crate::plot::continuous_color::{ContinuousColorScale, ContinuousScaleIntent};
use crate::plot::kernels::hexbin_viewport::{
	HexLatticeSpec, HexRadiusPx, HexbinObservations, HexbinPreparationGeneration,
	HexbinPreparationRequest, HexbinViewportArtifact, MaterializedHexbinPreparer,
};
use crate::plot::legend::{LegendContent, count_gradient_section};
use crate::plot::spec::{NonEmpty, SemanticType};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct HexbinViewportCache {
	next_generation: HexbinPreparationGeneration,
	latest_request: Option<HexbinPreparationRequest>,
	artifact: Option<Arc<HexbinViewportArtifact>>,
	stale_preview: bool,
}

impl Default for HexbinViewportCache {
	fn default() -> Self {
		Self {
			next_generation: HexbinPreparationGeneration::INITIAL,
			latest_request: None,
			artifact: None,
			stale_preview: false,
		}
	}
}

/// The renderer-facing hexbin kernel consumes only viewport artifacts. Raw
/// observations remain private to the materialized preparer and are never
/// aggregated from `plot` or `hit`.
pub struct HexbinPlotKernel {
	preparer: MaterializedHexbinPreparer,
	cache: Mutex<HexbinViewportCache>,
}

impl HexbinPlotKernel {
	pub fn new(observations: Arc<HexbinObservations>) -> Self {
		Self {
			preparer: MaterializedHexbinPreparer::new(observations),
			cache: Mutex::new(HexbinViewportCache::default()),
		}
	}

	fn observations(&self) -> &HexbinObservations {
		self.preparer.observations()
	}

	fn scale(maximum: u64, settings: &ResolvedPlotSettings) -> Option<ContinuousColorScale> {
		(maximum > 0).then(|| {
			ContinuousColorScale::new(
				(0.0, maximum as f64),
				ContinuousScaleIntent::SEQUENTIAL,
				settings.continuous_ramps,
			)
			.expect("PRODUCT-0143 finite u64 count domain is an ascending sequential scale")
		})
	}

	fn lattice(transform: &CoordinateTransformer<'_>) -> Option<HexLatticeSpec> {
		let PlotLayout::Cartesian {
			x_range, y_range, ..
		} = transform.layout
		else {
			return None;
		};
		HexLatticeSpec::new(
			*x_range,
			*y_range,
			transform.bounds.width,
			transform.bounds.height,
			HexRadiusPx::DEFAULT,
		)
		.ok()
	}

	fn artifact(&self) -> Option<Arc<HexbinViewportArtifact>> {
		self.cache
			.lock()
			.expect("hexbin viewport cache lock")
			.artifact
			.clone()
	}

	fn publish_if_newest(
		&self,
		request: &HexbinPreparationRequest,
		artifact: HexbinViewportArtifact,
	) -> bool {
		let mut cache = self.cache.lock().expect("hexbin viewport cache lock");
		if cache.latest_request.as_ref() == Some(request) && artifact.matches(request) {
			cache.artifact = Some(Arc::new(artifact));
			cache.stale_preview = false;
			true
		} else {
			false
		}
	}

	#[cfg(test)]
	fn is_stale_preview(&self) -> bool {
		self.cache
			.lock()
			.expect("hexbin viewport cache lock")
			.stale_preview
	}
}

impl PlotKernel for HexbinPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.observations().x_range().0),
				settings.x_max.unwrap_or(self.observations().x_range().1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.observations().y_range().0),
				settings.y_max.unwrap_or(self.observations().y_range().1),
			),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
		}
	}

	fn requires_plot_area_clip(&self) -> bool {
		true
	}

	fn prepare_viewport(&self, transform: &CoordinateTransformer<'_>, motion: ViewportMotion) {
		let Some(lattice) = Self::lattice(transform) else {
			return;
		};
		let request = {
			let mut cache = self.cache.lock().expect("hexbin viewport cache lock");
			if cache.artifact.as_ref().is_some_and(|artifact| {
				artifact.source_token() == self.observations().source_token()
					&& artifact.lattice() == lattice
			}) {
				cache.stale_preview = false;
				return;
			}
			if motion == ViewportMotion::Moving && cache.artifact.is_some() {
				cache.stale_preview = true;
				return;
			}
			let generation = cache.next_generation;
			cache.next_generation = generation.next();
			let request = HexbinPreparationRequest {
				source_token: self.observations().source_token(),
				generation,
				x_field: self.observations().x_field().to_owned(),
				y_field: self.observations().y_field().to_owned(),
				lattice,
			};
			cache.latest_request = Some(request.clone());
			request
		};
		let artifact = self
			.preparer
			.prepare(&request)
			.expect("PRODUCT-0143 validated materialized observations satisfy their request");
		let _published = self.publish_if_newest(&request, artifact);
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		let Some(artifact) = self.artifact() else {
			return;
		};
		let Some(scale) = Self::scale(artifact.max_count(), &settings) else {
			return;
		};
		let radius = f64::from(artifact.lattice().radius_px().get());
		for (&(q, r), &count) in artifact.bins() {
			let (center_x, center_y) = artifact.lattice().center_for_axial(q, r);
			let hex_path = &|builder: &mut dyn crate::plot::common::PathBuilder| {
				for corner in 0..6 {
					let angle = (60.0 * f64::from(corner) - 30.0).to_radians();
					let point = Point::new(
						(center_x + radius * angle.cos()) as f32,
						(center_y + radius * angle.sin()) as f32,
					);
					if corner == 0 {
						builder.move_to(point);
					} else {
						builder.line_to(point);
					}
				}
				builder.close();
			};
			backend.fill_path(hex_path, scale.color(count as f64));
			backend.stroke_path(
				hex_path,
				Stroke {
					style: Style::Solid(Color::from_rgba(0.0, 0.0, 0.0, 0.1)),
					width: 0.5,
					..Default::default()
				},
			);
		}
	}

	fn legend_reservation_content(&self, settings: &ResolvedPlotSettings) -> Option<LegendContent> {
		let maximum = self.observations().source_count();
		let scale = Self::scale(maximum, settings)?;
		count_gradient_section("Count".to_owned(), &scale, maximum)
			.map(|section| LegendContent(NonEmpty::new(section)))
	}

	fn legend_content(&self, settings: &ResolvedPlotSettings) -> Option<LegendContent> {
		let maximum = self.artifact()?.max_count();
		let scale = Self::scale(maximum, settings)?;
		count_gradient_section("Count".to_owned(), &scale, maximum)
			.map(|section| LegendContent(NonEmpty::new(section)))
	}

	fn hit(&self, _transform: &CoordinateTransformer, cursor: Cursor) -> Option<PlotHit> {
		let cache = self.cache.lock().expect("hexbin viewport cache lock");
		if cache.stale_preview {
			return None;
		}
		let artifact = cache.artifact.as_ref()?;
		let cursor = cursor.position()?;
		let (q, r) = artifact
			.lattice()
			.axial_for_pixel(f64::from(cursor.x), f64::from(cursor.y))?;
		let count = artifact.count(q, r)?;
		let (center_x, center_y) = artifact.lattice().center_for_axial(q, r);
		let (data_x, data_y) = artifact.lattice().data_for_pixel(center_x, center_y);
		let count_for_hit = usize::try_from(count).unwrap_or(usize::MAX);
		Some(
			PlotHit::new(MarkFamily::HEXBIN, HitDimensionality::Area, count_for_hit)
				.labeled("Q", SemanticType::Ordinal, HitValue::ordinal(q.to_string()))
				.labeled("R", SemanticType::Ordinal, HitValue::ordinal(r.to_string()))
				.labeled(
					"Count",
					SemanticType::Quantitative,
					HitValue::Count(count_for_hit),
				)
				.labeled(
					self.observations().x_field(),
					SemanticType::Quantitative,
					HitValue::quantitative(data_x, format!("{data_x:.2}")),
				)
				.labeled(
					self.observations().y_field(),
					SemanticType::Quantitative,
					HitValue::quantitative(data_y, format!("{data_y:.2}")),
				),
		)
	}

	fn x_label(&self) -> String {
		self.observations().x_field().to_owned()
	}

	fn y_label(&self) -> String {
		self.observations().y_field().to_owned()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::kernels::hexbin_viewport::prepare_hexbin_observations;
	use crate::plot::legend::LegendSection;
	use crate::result_artifact::ResultArtifact;
	use iced::Size;
	use polars::df;

	fn kernel() -> HexbinPlotKernel {
		let frame = df!(
			"x" => [0.25_f64, 0.25, 0.75],
			"y" => [0.25_f64, 0.25, 0.75],
		)
		.unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		let observations =
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()).unwrap();
		HexbinPlotKernel::new(Arc::new(observations))
	}

	fn transform<'a>(layout: &'a PlotLayout, width: f32) -> CoordinateTransformer<'a> {
		CoordinateTransformer::new(
			layout,
			Rectangle::new(Point::ORIGIN, Size::new(width, 100.0)),
		)
	}

	#[test]
	fn product_0143_kernel_prepares_exact_settled_lattice_and_disables_stale_hits() {
		let kernel = kernel();
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let narrow = transform(&layout, 100.0);
		kernel.prepare_viewport(&narrow, ViewportMotion::Settled);
		assert!(kernel.legend_content(&settings).is_some());
		assert!(!kernel.is_stale_preview());

		let wide = transform(&layout, 300.0);
		kernel.prepare_viewport(&wide, ViewportMotion::Moving);
		assert!(kernel.is_stale_preview());
		assert!(
			kernel
				.hit(&wide, Cursor::Available(Point::new(25.0, 75.0)))
				.is_none()
		);

		kernel.prepare_viewport(&wide, ViewportMotion::Settled);
		assert!(!kernel.is_stale_preview());
		assert_eq!(kernel.artifact().unwrap().lattice().plot_width_px(), 300.0);
	}

	#[test]
	fn product_0143_delayed_executor_publication_is_newest_wins() {
		let kernel = kernel();
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings);
		let lattice = |width| HexbinPlotKernel::lattice(&transform(&layout, width)).unwrap();
		let request = |kernel: &HexbinPlotKernel, lattice| {
			let mut cache = kernel.cache.lock().unwrap();
			let generation = cache.next_generation;
			cache.next_generation = generation.next();
			let request = HexbinPreparationRequest {
				source_token: kernel.observations().source_token(),
				generation,
				x_field: kernel.observations().x_field().to_owned(),
				y_field: kernel.observations().y_field().to_owned(),
				lattice,
			};
			cache.latest_request = Some(request.clone());
			request
		};

		let generation_1 = request(&kernel, lattice(100.0));
		let artifact_1 = kernel.preparer.prepare(&generation_1).unwrap();
		let generation_2 = request(&kernel, lattice(200.0));
		let artifact_2 = kernel.preparer.prepare(&generation_2).unwrap();
		assert!(kernel.publish_if_newest(&generation_2, artifact_2));
		assert!(!kernel.publish_if_newest(&generation_1, artifact_1));
		assert_eq!(
			kernel.artifact().unwrap().generation(),
			generation_2.generation
		);

		let generation_3 = request(&kernel, lattice(300.0));
		let artifact_3 = kernel.preparer.prepare(&generation_3).unwrap();
		assert!(kernel.publish_if_newest(&generation_3, artifact_3));
		assert_eq!(
			kernel.artifact().unwrap().generation(),
			generation_3.generation
		);
	}

	#[test]
	fn product_0143_first_moving_frame_prepares_and_paint_only_change_reuses() {
		let kernel = kernel();
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let transform = transform(&layout, 100.0);
		kernel.prepare_viewport(&transform, ViewportMotion::Moving);
		let generation = kernel.artifact().unwrap().generation();
		assert!(!kernel.is_stale_preview());

		let mut paint_change = settings;
		paint_change.continuous_ramps = crate::plot::continuous_color::ContinuousRamps {
			sequential: crate::plot::colors::ColorTheme::Blues,
			..paint_change.continuous_ramps
		};
		kernel.prepare_viewport(&transform, ViewportMotion::Settled);
		assert_eq!(kernel.artifact().unwrap().generation(), generation);
	}

	#[test]
	fn product_0143_reservation_uses_source_count_and_display_uses_actual_maximum() {
		let kernel = kernel();
		let settings = ResolvedPlotSettings::default();
		let reservation = kernel.legend_reservation_content(&settings).unwrap();
		let LegendSection::CountGradient {
			maximum: reserved, ..
		} = reservation.0.first()
		else {
			panic!("count reservation")
		};
		assert_eq!(*reserved, 3);

		let layout = kernel.layout(settings.clone());
		kernel.prepare_viewport(&transform(&layout, 100.0), ViewportMotion::Settled);
		let actual = kernel.legend_content(&settings).unwrap();
		let LegendSection::CountGradient {
			maximum: displayed, ..
		} = actual.0.first()
		else {
			panic!("count display")
		};
		assert!(*displayed < *reserved);

		let outside_layout = PlotLayout::Cartesian {
			x_range: (10.0, 20.0),
			y_range: (10.0, 20.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
		};
		kernel.prepare_viewport(&transform(&outside_layout, 100.0), ViewportMotion::Settled);
		assert!(kernel.legend_content(&settings).is_none());
		assert!(kernel.legend_reservation_content(&settings).is_some());
	}

	#[test]
	fn product_0143_hit_reports_bin_center_for_every_cursor_in_the_cell() {
		let kernel = kernel();
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings);
		let transform = transform(&layout, 100.0);
		kernel.prepare_viewport(&transform, ViewportMotion::Settled);
		let artifact = kernel.artifact().unwrap();
		let (&(q, r), _) = artifact.bins().iter().next().unwrap();
		let (center_x, center_y) = artifact.lattice().center_for_axial(q, r);
		let first = kernel
			.hit(
				&transform,
				Cursor::Available(Point::new(center_x as f32, center_y as f32)),
			)
			.unwrap();
		let second = kernel
			.hit(
				&transform,
				Cursor::Available(Point::new(center_x as f32 + 1.0, center_y as f32 + 1.0)),
			)
			.unwrap();
		assert_eq!(first.fields, second.fields);
	}
}
