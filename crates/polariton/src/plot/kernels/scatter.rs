use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, ScatterRenderMode, TemporalAxisDomain, ViewportMotion, format_label,
	polars_type_to_axis_type, temporal_axis_domain,
};
use crate::plot::legend::LegendProvenance;
use crate::plot::spec::{
	Channel, LayerSettings, PointSize, ResolvedColor, ResolvedLayout, ResolvedPlotSpec,
	ScaleDomain, ScaleFamily,
};
use crate::plot::theme::PlotSettings;
use iced::advanced::image;
use iced::advanced::mouse::Cursor;
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::{Arc, Mutex};

pub struct ScatterPlotKernel {
	pub prepared_data: Arc<ScatterPreparedData>,
	renderer: ScatterMarkRenderer,
	layer_settings: LayerSettings,
}

pub struct ScatterMarkRenderer {
	pub prepared_data: Arc<ScatterPreparedData>,
	raster_cache: Mutex<Option<ScatterRasterCache>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResolvedScatterRenderMode {
	Vector,
	Downsampled,
	Rasterized,
}

#[derive(Clone)]
struct ScatterRasterCache {
	key: ScatterRasterKey,
	width: u32,
	height: u32,
	rgba: Arc<Vec<u8>>,
	image_handle: image::Handle,
}

#[derive(Clone, Debug, PartialEq)]
struct ScatterRasterKey {
	width: u32,
	height: u32,
	point_size: u32,
	x_range: (u64, u64),
	y_range: (u64, u64),
	background: [u8; 4],
	series_colors: Vec<[u8; 4]>,
}

/// PRODUCT-0139 §2: the placement of a reused raster, in image space.
///
/// `cartesian` (`plot/common.rs`) is linear in the domain, so the old-to-new
/// screen map is affine and determined entirely by the two key range pairs and
/// the raster size — no access to the data, and no access to anything the cache
/// does not already store.
#[derive(Clone, Copy, Debug, PartialEq)]
struct ReusePlacement {
	top_left: Point,
	size: iced::Size,
}

/// `None` when the map is not derivable — a degenerate extent, or a
/// non-Cartesian key whose ranges are recorded as `(0, 0)`.
fn reuse_placement(
	cached: &ScatterRasterKey,
	current: &ScatterRasterKey,
	width: f32,
	height: f32,
) -> Option<ReusePlacement> {
	let (x0o, x1o) = (
		f64::from_bits(cached.x_range.0),
		f64::from_bits(cached.x_range.1),
	);
	let (y0o, y1o) = (
		f64::from_bits(cached.y_range.0),
		f64::from_bits(cached.y_range.1),
	);
	let (x0n, x1n) = (
		f64::from_bits(current.x_range.0),
		f64::from_bits(current.x_range.1),
	);
	let (y0n, y1n) = (
		f64::from_bits(current.y_range.0),
		f64::from_bits(current.y_range.1),
	);
	let (dxo, dyo) = (x1o - x0o, y1o - y0o);
	let (dxn, dyn_) = (x1n - x0n, y1n - y0n);
	if ![dxo, dyo, dxn, dyn_]
		.iter()
		.all(|d| d.is_finite() && d.abs() > f64::EPSILON)
	{
		return None;
	}

	let scale_x = dxo / dxn;
	let scale_y = dyo / dyn_;
	let left = (x0o - x0n) * f64::from(width) / dxn;
	// The vertical axis is inverted by `cartesian`, so the offset absorbs the
	// flip: a row of the cached image maps to `H - (y0ᵒ − y0ⁿ)·H/Δyⁿ − H·scale_y`
	// plus `scale_y` times its own position.
	let top =
		f64::from(height) - (y0o - y0n) * f64::from(height) / dyn_ - f64::from(height) * scale_y;
	Some(ReusePlacement {
		top_left: Point::new(left as f32, top as f32),
		size: iced::Size::new(width * scale_x as f32, height * scale_y as f32),
	})
}

impl ScatterRasterKey {
	/// PRODUCT-0139 §2: everything except the domain must be identical, or the
	/// cached pixels do not depict the same marks at a different viewport.
	fn reusable_for(&self, current: &Self) -> bool {
		self.width == current.width
			&& self.height == current.height
			&& self.point_size == current.point_size
			&& self.background == current.background
			&& self.series_colors == current.series_colors
			&& (self.x_range != current.x_range || self.y_range != current.y_range)
	}
}

impl PlotKernel for ScatterPlotKernel {
	/// PRODUCT-0139 §3: honoured, not decided — the answer comes from the mark,
	/// so this kernel and the one-layer `LayeredPlotKernel` that a modified
	/// spelling produces cannot disagree.
	fn requires_plot_area_clip(&self) -> bool {
		self.renderer.requires_plot_area_clip()
	}

	/// PRODUCT-0139 §6: scatter is single-layer, so the value its renderer draws
	/// with is the authority for geometry too. `draw` resolves exactly this via
	/// `plot_with_settings` below, which is what makes the transform, the
	/// resolved render mode, and `ScatterRasterKey` one resolution, not two.
	fn resolve_settings(&self, settings: &PlotSettings) -> ResolvedPlotSettings {
		settings.resolve_for_layer(&self.layer_settings)
	}

	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		if let Some(categories) = &self.prepared_data.x_categories {
			return PlotLayout::CategoricalX {
				categories: categories.clone(),
				y_axis_type: self.prepared_data.y_axis_type.clone(),
				y_range: (
					settings.y_min.unwrap_or(self.prepared_data.y_range.0),
					settings.y_max.unwrap_or(self.prepared_data.y_range.1),
				),
			};
		}
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.prepared_data.x_range.0),
				settings.x_max.unwrap_or(self.prepared_data.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.prepared_data.y_range.0),
				settings.y_max.unwrap_or(self.prepared_data.y_range.1),
			),
			x_axis_type: self.prepared_data.x_axis_type.clone(),
			y_axis_type: self.prepared_data.y_axis_type.clone(),
			x_temporal_domain: self.prepared_data.x_temporal_domain.clone(),
			y_temporal_domain: self.prepared_data.y_temporal_domain.clone(),
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
		self.renderer.draw(backend, bounds, transform, settings);
	}

	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: &PlotSettings,
	) {
		self.renderer.draw(
			backend,
			bounds,
			transform,
			settings.resolve_for_layer(&self.layer_settings),
		);
	}

	fn plot_with_motion(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: &PlotSettings,
		motion: ViewportMotion,
	) {
		self.renderer.draw_with_motion(
			backend,
			bounds,
			transform,
			settings.resolve_for_layer(&self.layer_settings),
			motion,
		);
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		let pairs = self.prepared_data.series.iter().map(|series| {
			(
				series.name.clone(),
				series
					.resolved_color
					.unwrap_or_else(|| settings.color_theme.get_color(series.color_t)),
			)
		});
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			pairs,
			crate::plot::legend::LegendSample::PointSample,
			self.prepared_data
				.series
				.first()
				.map_or(LegendProvenance::Named, |series| series.provenance),
		)
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let settings = PlotSettings::default().resolve_for_layer(&self.layer_settings);
		self.structured_hit(transform, cursor, &settings)
	}

	fn hit_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let resolved = settings.resolve_for_layer(&self.layer_settings);
		self.structured_hit(transform, cursor, &resolved)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

impl ScatterPlotKernel {
	fn structured_hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::common::PlotHit> {
		let hit = self.renderer.hit_test(transform, cursor, settings)?;
		Some(
			crate::plot::common::PlotHit::new(
				crate::plot::common::MarkFamily::SCATTER,
				crate::plot::common::HitDimensionality::Point,
				hit.mark_index,
			)
			.labeled(
				"X",
				crate::plot::common::axis_hit_semantic_type(&self.prepared_data.x_axis_type),
				crate::plot::common::axis_hit_value(
					hit.data_x,
					&self.prepared_data.x_axis_type,
					format_label(hit.data_x, &self.prepared_data.x_axis_type),
				),
			)
			.labeled(
				"Y",
				crate::plot::common::axis_hit_semantic_type(&self.prepared_data.y_axis_type),
				crate::plot::common::axis_hit_value(
					hit.data_y,
					&self.prepared_data.y_axis_type,
					format_label(hit.data_y, &self.prepared_data.y_axis_type),
				),
			),
		)
	}

	pub fn new(prepared_data: Arc<ScatterPreparedData>) -> Self {
		Self::with_layer_settings(prepared_data, LayerSettings::default())
	}

	pub fn with_layer_settings(
		prepared_data: Arc<ScatterPreparedData>,
		layer_settings: LayerSettings,
	) -> Self {
		Self {
			prepared_data: Arc::clone(&prepared_data),
			renderer: ScatterMarkRenderer::new(prepared_data),
			layer_settings,
		}
	}
}

impl ScatterMarkRenderer {
	pub fn new(prepared_data: Arc<ScatterPreparedData>) -> Self {
		Self {
			prepared_data,
			raster_cache: Mutex::new(None),
		}
	}

	#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
	fn hit_point(&self, transform: &CoordinateTransformer, cursor: Point) -> Option<MarkHit> {
		self.hit_point_with_radius(transform, cursor, PointSize::ONE.radius_px())
	}

	fn hit_point_with_radius(
		&self,
		transform: &CoordinateTransformer,
		cursor: Point,
		point_radius: f32,
	) -> Option<MarkHit> {
		let hit_radius = point_radius + 2.0;
		let mut nearest_distance = hit_radius * hit_radius;
		let mut nearest = None;
		let mut mark_index = 0;
		for series in &self.prepared_data.series {
			series.for_each_point(|x, y| {
				let pixel = scatter_point(transform, x, y);
				let dx = cursor.x - pixel.x;
				let dy = cursor.y - pixel.y;
				let distance = dx * dx + dy * dy;
				if distance <= nearest_distance {
					nearest_distance = distance;
					nearest = Some(MarkHit {
						data_x: x,
						data_y: y,
						mark_index,
					});
				}
				mark_index += 1;
			});
		}
		nearest
	}

	fn plot_vector(
		&self,
		backend: &mut dyn PlotBackend,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) {
		let point_radius = settings.point_size.radius_px();
		for series in &self.prepared_data.series {
			let color = series
				.resolved_color
				.unwrap_or_else(|| settings.color_theme.get_color(series.color_t));
			backend.fill_path(
				&|builder| {
					series.for_each_point(|x, y| {
						let pixel_p = scatter_point(transform, x, y);
						builder.circle(pixel_p, point_radius);
					});
				},
				color,
			);
		}
	}

	fn plot_downsampled(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) {
		let point_radius = settings.point_size.radius_px();
		let series_count = self.prepared_data.series.len().max(1);
		let per_series_target = (settings.scatter_downsample_target.max(1) as usize)
			.div_ceil(series_count)
			.max(1);

		for series in &self.prepared_data.series {
			let sampled = downsample_series(series, bounds, transform, per_series_target);
			let color = series_color(series, settings);
			backend.fill_path(
				&|builder| {
					for point in &sampled {
						builder.circle(*point, point_radius);
					}
				},
				color,
			);
		}
	}

	fn plot_rasterized(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
		motion: ViewportMotion,
	) {
		let width = bounds.width.max(1.0);
		let height = bounds.height.max(1.0);
		let key = self.raster_key(bounds, transform, settings);

		// PRODUCT-0139 §1 and §5. A settled frame always rasterizes truly, so
		// "exactly one true rasterization at settle" needs no settle event: it
		// is the first frame after motion stops. Reuse is attempted only while
		// moving, and only against the **last true raster** — the reuse branch
		// stores nothing, which is §5's no-chaining rule by construction rather
		// than by discipline.
		if motion == ViewportMotion::Moving {
			let cached = self.raster_cache.lock().unwrap().clone();
			if let Some(cached) = cached {
				if cached.key != key
					&& cached.key.reusable_for(&key)
					&& let Some(placement) = reuse_placement(&cached.key, &key, width, height)
				{
					self.draw_raster(backend, placement.top_left, placement.size, &cached);
					return;
				}
			}
		}

		let raster = self.rasterize_with_key(key, bounds, transform, settings);
		self.draw_raster(
			backend,
			Point::ORIGIN,
			iced::Size::new(width, height),
			&raster,
		);
	}

	fn draw_raster(
		&self,
		backend: &mut dyn PlotBackend,
		top_left: Point,
		size: iced::Size,
		raster: &ScatterRasterCache,
	) {
		if backend.supports_native_image_handle() {
			backend.draw_image_handle(top_left, size, &raster.image_handle);
		} else {
			backend.draw_image_rgba(
				top_left,
				size,
				raster.width,
				raster.height,
				raster.rgba.as_slice(),
			);
		}
	}

	/// PRODUCT-0139 §1. `MarkRenderer::draw` delegates here with `Settled`, so
	/// every path that has not opted into motion rasterizes truly.
	pub(crate) fn draw_with_motion(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
		motion: ViewportMotion,
	) {
		let mode = resolve_render_mode(
			self.prepared_data.total_points,
			backend.supports_embedded_raster(),
			bounds,
			&settings,
		);
		match mode {
			ResolvedScatterRenderMode::Vector => self.plot_vector(backend, transform, &settings),
			ResolvedScatterRenderMode::Downsampled => {
				self.plot_downsampled(backend, bounds, transform, &settings)
			}
			ResolvedScatterRenderMode::Rasterized => {
				self.plot_rasterized(backend, bounds, transform, &settings, motion)
			}
		}
	}

	fn raster_key(
		&self,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) -> ScatterRasterKey {
		ScatterRasterKey {
			width: bounds.width.max(1.0).ceil() as u32,
			height: bounds.height.max(1.0).ceil() as u32,
			point_size: settings.point_size.factor().to_bits(),
			x_range: current_x_range_bits(transform),
			y_range: current_y_range_bits(transform),
			background: color_to_rgba8(settings.chrome.canvas_background.iced()),
			series_colors: self
				.prepared_data
				.series
				.iter()
				.map(|series| color_to_rgba8(series_color(series, settings)))
				.collect(),
		}
	}

	/// The whole-key form: compute the key, then hit or build. Reuse needs the
	/// key before deciding, so production keeps the two steps separate above.
	/// Tests use this convenience wrapper to exercise cache identity directly.
	#[cfg(test)]
	fn rasterize(
		&self,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) -> ScatterRasterCache {
		let key = self.raster_key(bounds, transform, settings);
		self.rasterize_with_key(key, bounds, transform, settings)
	}

	fn rasterize_with_key(
		&self,
		key: ScatterRasterKey,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) -> ScatterRasterCache {
		let width = key.width;
		let height = key.height;

		if let Some(existing) = self
			.raster_cache
			.lock()
			.unwrap()
			.as_ref()
			.filter(|cache| cache.key == key)
			.cloned()
		{
			return existing;
		}

		let rgba = Arc::new(build_raster_rgba(
			&self.prepared_data,
			bounds,
			transform,
			settings,
		));
		let image_handle = image::Handle::from_rgba(width, height, rgba.as_slice().to_vec());
		let cache = ScatterRasterCache {
			key,
			width,
			height,
			rgba,
			image_handle,
		};
		*self.raster_cache.lock().unwrap() = Some(cache.clone());
		cache
	}
}

impl MarkRenderer for ScatterMarkRenderer {
	/// PRODUCT-0139 §3. A translated or scaled raster is not bounded the way
	/// today's ORIGIN-anchored, exact-size blit is, so image-space reuse would
	/// paint over the axis bands and the legend without this. It also bounds the
	/// vector and downsampled paths, whose marks carry a radius and can already
	/// overhang a domain edge by it.
	fn requires_plot_area_clip(&self) -> bool {
		true
	}

	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	) {
		self.draw_with_motion(
			backend,
			bounds,
			transform,
			settings,
			ViewportMotion::Settled,
		);
	}

	fn hit_test(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<MarkHit> {
		self.hit_point_with_radius(
			transform,
			cursor.position()?,
			settings.point_size.radius_px(),
		)
	}
}

pub struct ScatterSeries {
	pub name: String,
	pub x_values: Float64Chunked,
	pub y_values: Float64Chunked,
	pub color_t: f32,
	pub resolved_color: Option<Color>,
	pub provenance: LegendProvenance,
}

impl ScatterSeries {
	fn len(&self) -> usize {
		self.x_values.len().min(self.y_values.len())
	}

	fn for_each_point(&self, mut f: impl FnMut(f64, f64)) {
		for idx in 0..self.len() {
			let Some(x) = self.x_values.get(idx) else {
				continue;
			};
			let Some(y) = self.y_values.get(idx) else {
				continue;
			};
			if x.is_finite() && y.is_finite() {
				f(x, y);
			}
		}
	}
}

pub struct ScatterPreparedData {
	pub series: Vec<ScatterSeries>,
	pub x_categories: Option<Vec<String>>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_temporal_domain: Option<TemporalAxisDomain>,
	pub y_temporal_domain: Option<TemporalAxisDomain>,
	pub x_label: String,
	pub y_label: String,
	pub total_points: usize,
}

fn current_x_range_bits(transform: &CoordinateTransformer) -> (u64, u64) {
	match transform.layout {
		PlotLayout::Cartesian { x_range, .. } => (x_range.0.to_bits(), x_range.1.to_bits()),
		_ => (0, 0),
	}
}

fn current_y_range_bits(transform: &CoordinateTransformer) -> (u64, u64) {
	match transform.layout {
		PlotLayout::Cartesian { y_range, .. } | PlotLayout::CategoricalX { y_range, .. } => {
			(y_range.0.to_bits(), y_range.1.to_bits())
		}
		_ => (0, 0),
	}
}

fn scatter_point(transform: &CoordinateTransformer<'_>, x: f64, y: f64) -> Point {
	match transform.layout {
		PlotLayout::CategoricalX { .. } => transform.categorical(x.max(0.0) as usize, y).0,
		_ => transform.cartesian(x, y),
	}
}

fn color_to_rgba8(color: Color) -> [u8; 4] {
	[
		(color.r * 255.0).round() as u8,
		(color.g * 255.0).round() as u8,
		(color.b * 255.0).round() as u8,
		(color.a * 255.0).round() as u8,
	]
}

fn series_color(series: &ScatterSeries, settings: &ResolvedPlotSettings) -> Color {
	series
		.resolved_color
		.unwrap_or_else(|| settings.color_theme.get_color(series.color_t))
}

/// PRODUCT-0139 §10 gate 1's instrument: how many times the source points have
/// been scanned. A reused frame must not move it.
///
/// Thread-local, not global: libtest runs each test on its own thread, and a
/// shared counter would be raced by any other test that rasterizes — which
/// showed up immediately as a gate that passed alone and failed in the suite.
#[cfg(test)]
thread_local! {
	pub(crate) static RASTER_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn build_raster_rgba(
	prepared_data: &ScatterPreparedData,
	bounds: Rectangle,
	transform: &CoordinateTransformer,
	settings: &ResolvedPlotSettings,
) -> Vec<u8> {
	let width = bounds.width.max(1.0).ceil() as usize;
	let height = bounds.height.max(1.0).ceil() as usize;
	#[cfg(test)]
	RASTER_BUILDS.with(|builds| builds.set(builds.get() + 1));
	let mut accum = vec![[0.0_f32; 4]; width * height];
	let radius = settings.point_size.radius_px();
	let radius_squared = radius * radius;

	for series in &prepared_data.series {
		let color = series_color(series, settings);
		series.for_each_point(|x, y| {
			let point = scatter_point(transform, x, y);
			let min_x = (point.x - radius).floor().max(0.0) as usize;
			let max_x = (point.x + radius).ceil().min(width as f32 - 1.0) as usize;
			let min_y = (point.y - radius).floor().max(0.0) as usize;
			let max_y = (point.y + radius).ceil().min(height as f32 - 1.0) as usize;
			if min_x > max_x || min_y > max_y {
				return;
			}
			for yi in min_y..=max_y {
				for xi in min_x..=max_x {
					let dx = xi as f32 + 0.5 - point.x;
					let dy = yi as f32 + 0.5 - point.y;
					if dx * dx + dy * dy > radius_squared {
						continue;
					}
					let idx = yi * width + xi;
					accum[idx][0] += color.r * color.a;
					accum[idx][1] += color.g * color.a;
					accum[idx][2] += color.b * color.a;
					accum[idx][3] += color.a.max(0.1);
				}
			}
		});
	}

	let bg = settings.chrome.canvas_background.iced();
	let mut rgba = vec![0_u8; width * height * 4];
	for (i, pixel) in accum.iter().enumerate() {
		let base = i * 4;
		if pixel[3] <= f32::EPSILON {
			rgba[base] = (bg.r * 255.0).round() as u8;
			rgba[base + 1] = (bg.g * 255.0).round() as u8;
			rgba[base + 2] = (bg.b * 255.0).round() as u8;
			rgba[base + 3] = (bg.a * 255.0).round() as u8;
			continue;
		}

		let density = pixel[3];
		let coverage = 1.0 - (-density / 2.0).exp();
		let avg_r = pixel[0] / density;
		let avg_g = pixel[1] / density;
		let avg_b = pixel[2] / density;
		let out_r = bg.r * (1.0 - coverage) + avg_r * coverage;
		let out_g = bg.g * (1.0 - coverage) + avg_g * coverage;
		let out_b = bg.b * (1.0 - coverage) + avg_b * coverage;
		rgba[base] = (out_r.clamp(0.0, 1.0) * 255.0).round() as u8;
		rgba[base + 1] = (out_g.clamp(0.0, 1.0) * 255.0).round() as u8;
		rgba[base + 2] = (out_b.clamp(0.0, 1.0) * 255.0).round() as u8;
		rgba[base + 3] = 255;
	}

	rgba
}

fn resolve_render_mode(
	total_points: usize,
	supports_embedded_raster: bool,
	bounds: Rectangle,
	settings: &ResolvedPlotSettings,
) -> ResolvedScatterRenderMode {
	match settings.scatter_render_mode {
		ScatterRenderMode::Vector => ResolvedScatterRenderMode::Vector,
		ScatterRenderMode::Downsampled => ResolvedScatterRenderMode::Downsampled,
		ScatterRenderMode::Rasterized => {
			if supports_embedded_raster {
				ResolvedScatterRenderMode::Rasterized
			} else {
				ResolvedScatterRenderMode::Downsampled
			}
		}
		ScatterRenderMode::Auto => {
			if total_points <= settings.scatter_max_vector_points as usize {
				return ResolvedScatterRenderMode::Vector;
			}

			let pixel_budget = (bounds.width.max(1.0) * bounds.height.max(1.0)) as usize;
			let raster_threshold = settings.scatter_raster_threshold as usize;
			if supports_embedded_raster
				&& total_points > raster_threshold
				&& total_points > pixel_budget.saturating_mul(8)
			{
				ResolvedScatterRenderMode::Rasterized
			} else {
				ResolvedScatterRenderMode::Downsampled
			}
		}
	}
}

fn downsample_series(
	series: &ScatterSeries,
	bounds: Rectangle,
	transform: &CoordinateTransformer,
	target_points: usize,
) -> Vec<Point> {
	let aspect = (bounds.width.max(1.0) / bounds.height.max(1.0).max(1.0)) as f64;
	let grid_w = ((target_points.max(1) as f64 * aspect).sqrt().ceil() as usize).max(1);
	let grid_h = target_points.max(1).div_ceil(grid_w).max(1);
	let mut cells = vec![None; grid_w * grid_h];

	series.for_each_point(|x, y| {
		let point = scatter_point(transform, x, y);
		if point.x < 0.0 || point.y < 0.0 || point.x > bounds.width || point.y > bounds.height {
			return;
		}
		let gx = ((point.x / bounds.width.max(1.0)) * grid_w as f32)
			.floor()
			.clamp(0.0, (grid_w - 1) as f32) as usize;
		let gy = ((point.y / bounds.height.max(1.0)) * grid_h as f32)
			.floor()
			.clamp(0.0, (grid_h - 1) as f32) as usize;
		let idx = gy * grid_w + gx;
		if cells[idx].is_none() {
			cells[idx] = Some(point);
		}
	});

	cells.into_iter().flatten().collect()
}

pub fn prepare_scatter_data_resolved(
	df: &DataFrame,
	spec: &ResolvedPlotSpec,
	viewport_width: u32,
	viewport_height: u32,
) -> ScatterPreparedData {
	let x = spec
		.channel(Channel::X)
		.expect("resolved scatter x channel");
	let y = spec
		.channel(Channel::Y)
		.expect("resolved scatter y channel");
	let color = spec.channel(Channel::Color);
	let color_field = color
		.map(|channel| channel.binding.field.as_str())
		.unwrap_or("");
	let x_field = x.binding.field.as_str();
	let y_field = y.binding.field.as_str();
	let layout = ResolvedLayout::from_spec(spec, viewport_width, viewport_height);

	if !matches!(x.binding.scale, ScaleFamily::Point | ScaleFamily::Band) {
		let mut prepared = prepare_scatter_data(df, color_field, x_field, y_field);
		prepared.x_axis_type = axis_type_from_scale(x.binding.scale);
		prepared.y_axis_type = axis_type_from_scale(y.binding.scale);
		prepared.x_range = layout.x_range;
		prepared.y_range = layout.y_range;
		apply_resolved_series_colors(&mut prepared.series, color);
		return prepared;
	}

	let categories = match &x.domain {
		ScaleDomain::Categorical(categories) => categories.clone(),
		_ => Vec::new(),
	};
	let y_range = layout.y_range;
	let partitions = if color_field.is_empty() {
		vec![df.clone()]
	} else {
		df.partition_by([color_field], true)
			.unwrap_or_else(|_| vec![df.clone()])
	};
	let num_partitions = partitions.len();
	let mut series = Vec::with_capacity(num_partitions);
	let mut total_points = 0;
	for (index, group) in partitions.into_iter().enumerate() {
		let name = if color_field.is_empty() {
			"All Data".to_owned()
		} else {
			group
				.column(color_field)
				.and_then(|column| column.get(0))
				.map(category_label)
				.unwrap_or_else(|_| "Null".to_owned())
		};
		let xs = group
			.column(x_field)
			.map(|column| {
				column
					.as_materialized_series()
					.iter()
					.map(|value| {
						let value = category_label(value);
						categories
							.iter()
							.position(|category| category == &value)
							.map(|i| i as f64)
					})
					.collect::<Vec<_>>()
			})
			.unwrap_or_default();
		let ys = group
			.column(y_field)
			.and_then(|column| column.cast(&DataType::Float64))
			.ok()
			.and_then(|column| column.f64().ok().cloned())
			.unwrap_or_else(|| Float64Chunked::full_null(y_field.into(), xs.len()));
		let xs = Series::new(x_field.into(), xs).f64().unwrap().clone();
		total_points += xs.len().min(ys.len());
		let color_t = if num_partitions > 1 {
			index as f32 / (num_partitions - 1) as f32
		} else {
			0.5
		};
		let resolved_color = color
			.and_then(|channel| channel.colors.iter().find(|entry| entry.category == name))
			.map(|entry| iced_color(entry.color));
		series.push(ScatterSeries {
			name,
			x_values: xs,
			y_values: ys,
			color_t,
			resolved_color,
			provenance: if color_field.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		});
	}
	if !color_field.is_empty() {
		order_series_by_display_domain(&mut series, df, color_field);
	}
	ScatterPreparedData {
		series,
		x_categories: Some(categories.clone()),
		x_range: layout.x_range,
		y_range,
		x_axis_type: axis_type_from_scale(x.binding.scale),
		y_axis_type: axis_type_from_scale(y.binding.scale),
		x_temporal_domain: None,
		y_temporal_domain: None,
		x_label: x_field.to_owned(),
		y_label: y_field.to_owned(),
		total_points,
	}
}

fn apply_resolved_series_colors(
	series: &mut [ScatterSeries],
	channel: Option<&crate::plot::spec::ResolvedChannel>,
) {
	let Some(channel) = channel else { return };
	for series in series {
		series.resolved_color = channel
			.colors
			.iter()
			.find(|entry| entry.category == series.name)
			.map(|entry| iced_color(entry.color));
	}
}

fn iced_color(color: ResolvedColor) -> Color {
	Color::from_rgba8(
		color.red,
		color.green,
		color.blue,
		color.alpha as f32 / 255.0,
	)
}

fn category_label(value: AnyValue<'_>) -> String {
	match value {
		AnyValue::String(value) => value.to_owned(),
		AnyValue::Null => "Null".to_owned(),
		value => value.to_string().replace('"', ""),
	}
}

/// `partition_by` has no display-order contract. Marks and their legends
/// share palette positions, so restore the source's first-appearance order
/// before assigning those positions.
fn order_series_by_display_domain(series: &mut [ScatterSeries], df: &DataFrame, field: &str) {
	let order = df
		.column(field)
		.ok()
		.into_iter()
		.flat_map(|column| (0..df.height()).filter_map(move |index| column.get(index).ok()))
		.map(category_label)
		.fold(Vec::new(), |mut seen, label| {
			if !seen.contains(&label) {
				seen.push(label);
			}
			seen
		});
	series.sort_by_key(|item| {
		order
			.iter()
			.position(|label| label == &item.name)
			.unwrap_or(usize::MAX)
	});
	let total = series.len();
	for (index, item) in series.iter_mut().enumerate() {
		item.color_t = if total > 1 {
			index as f32 / (total - 1) as f32
		} else {
			0.5
		};
	}
}

fn axis_type_from_scale(scale: ScaleFamily) -> AxisType {
	match scale {
		ScaleFamily::Time => AxisType::Datetime {
			unit: crate::plot::common::TimeUnit::Milliseconds,
			zone: None,
		},
		_ => AxisType::Linear,
	}
}

pub fn prepare_scatter_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
) -> ScatterPreparedData {
	let x_col_series = match df.column(x_col) {
		Ok(c) => c
			.cast(&DataType::Float64)
			.unwrap_or_else(|_| Column::from(Series::new_empty(x_col.into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty(x_col.into(), &DataType::Float64)),
	};
	let y_col_series = match df.column(y_col) {
		Ok(c) => c
			.cast(&DataType::Float64)
			.unwrap_or_else(|_| Column::from(Series::new_empty(y_col.into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty(y_col.into(), &DataType::Float64)),
	};
	let x_dtype = df
		.column(x_col)
		.map(|c| c.dtype().clone())
		.unwrap_or(DataType::Float64);
	let y_dtype = df
		.column(y_col)
		.map(|c| c.dtype().clone())
		.unwrap_or(DataType::Float64);
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let y_axis_type = polars_type_to_axis_type(&y_dtype);
	let x_temporal_domain = df
		.column(x_col)
		.ok()
		.and_then(|column| temporal_axis_domain(column, &x_axis_type));
	let y_temporal_domain = df
		.column(y_col)
		.ok()
		.and_then(|column| temporal_axis_domain(column, &y_axis_type));
	let x_series = x_col_series.f64().unwrap();
	let y_series = y_col_series.f64().unwrap();
	let x_range = (x_series.min().unwrap_or(0.0), x_series.max().unwrap_or(1.0));
	let y_range = (y_series.min().unwrap_or(0.0), y_series.max().unwrap_or(1.0));
	let x_pad = (x_range.1 - x_range.0).max(0.001) * 0.1;
	let y_pad = (y_range.1 - y_range.0).max(0.001) * 0.1;
	let x_range = (x_range.0 - x_pad, x_range.1 + x_pad);
	let y_range = (y_range.0 - y_pad, y_range.1 + y_pad);
	if df.height() == 0 || x_col.is_empty() || y_col.is_empty() {
		return ScatterPreparedData {
			series: vec![],
			x_categories: None,
			x_range,
			y_range,
			x_axis_type,
			y_axis_type,
			x_temporal_domain,
			y_temporal_domain,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
			total_points: 0,
		};
	}
	let partitions = if cat_col.is_empty() {
		vec![df.clone()]
	} else {
		df.partition_by([cat_col], true)
			.unwrap_or_else(|_| vec![df.clone()])
	};
	let num_partitions = partitions.len();
	let mut series_list = Vec::with_capacity(num_partitions);
	let mut total_points = 0_usize;
	for (i, group_df) in partitions.into_iter().enumerate() {
		let cat_name = if cat_col.is_empty() {
			"All Data".to_string()
		} else {
			let cat_val = group_df
				.column(cat_col)
				.and_then(|c| c.get(0))
				.unwrap_or(AnyValue::Null);
			if let AnyValue::String(s) = cat_val {
				s.to_string()
			} else if cat_val.is_null() {
				"Null".to_string()
			} else {
				cat_val.to_string().replace('"', "")
			}
		};
		let xs_col = match group_df.column(x_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(x_col.into(), &DataType::Float64))
			}),
			Err(_) => Column::from(Series::new_empty(x_col.into(), &DataType::Float64)),
		};
		let ys_col = match group_df.column(y_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(y_col.into(), &DataType::Float64))
			}),
			Err(_) => Column::from(Series::new_empty(y_col.into(), &DataType::Float64)),
		};
		let xs = xs_col.f64().unwrap().clone();
		let ys = ys_col.f64().unwrap().clone();
		total_points += xs.len().min(ys.len());
		let t = if num_partitions > 1 {
			i as f32 / (num_partitions - 1) as f32
		} else {
			0.5
		};
		series_list.push(ScatterSeries {
			name: cat_name,
			x_values: xs,
			y_values: ys,
			color_t: t,
			resolved_color: None,
			provenance: if cat_col.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		});
	}
	if !cat_col.is_empty() {
		order_series_by_display_domain(&mut series_list, df, cat_col);
	}
	ScatterPreparedData {
		series: series_list,
		x_categories: None,
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_temporal_domain,
		y_temporal_domain,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
		total_points,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::PathBuilder;
	use iced::widget::canvas::{Stroke, Text};

	// Shared measurement instrument: counts what actually reached the
	// backend, so a render path is observed rather than inferred.
	#[derive(Default)]
	struct CountingPath {
		circles: usize,
	}
	impl PathBuilder for CountingPath {
		fn move_to(&mut self, _p: Point) {}
		fn line_to(&mut self, _p: Point) {}
		fn arc_to(&mut self, _c: Point, _r: f32, _s: f32, _e: f32) {}
		fn circle(&mut self, _center: Point, _radius: f32) {
			self.circles += 1;
		}
		fn rectangle(&mut self, _tl: Point, _size: iced::Size) {}
		fn close(&mut self) {}
	}

	#[derive(Default)]
	struct CountingBackend {
		circles: usize,
		images: usize,
		/// The rasterized image's size is the inner plot area — the same
		/// bounds `resolve_render_mode` divides for its pixel budget, read
		/// from the rasterizer's own output rather than reconstructed.
		image_size: Option<(f32, f32)>,
	}
	impl PlotBackend for CountingBackend {
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {
			let mut p = CountingPath::default();
			f(&mut p);
			self.circles += p.circles;
		}
		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {
			let mut p = CountingPath::default();
			f(&mut p);
			self.circles += p.circles;
		}
		fn fill_rectangle(&mut self, _tl: Point, _size: iced::Size, _color: Color) {}
		fn fill_text_raw(&mut self, _text: Text) {}
		fn supports_embedded_raster(&self) -> bool {
			true
		}
		fn draw_image_rgba(
			&mut self,
			_top_left: Point,
			size: iced::Size,
			_width: u32,
			_height: u32,
			_rgba: &[u8],
		) {
			self.images += 1;
			self.image_size = Some((size.width, size.height));
		}
		fn draw_image_handle(
			&mut self,
			_top_left: Point,
			size: iced::Size,
			_handle: &image::Handle,
		) {
			self.images += 1;
			self.image_size = Some((size.width, size.height));
		}
		fn translate(&mut self, _t: iced::Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::spec::{
		AuthoredPlotSpec, ColorIdentity, PlotProvenance, StampedResult, resolve_channels,
		resolve_domains,
	};
	use iced::Size;

	fn bounds(width: f32, height: f32) -> Rectangle {
		Rectangle::new(Point::ORIGIN, Size::new(width, height))
	}

	fn resolved_scatter(
		frame: DataFrame,
		spec: AuthoredPlotSpec,
		identity: &ColorIdentity,
	) -> (ResolvedPlotSpec, ColorIdentity) {
		let frame = Arc::new(frame);
		let binding = resolve_channels(&spec, frame.schema().as_ref()).expect("channel binding");
		resolve_domains(
			binding,
			&StampedResult {
				data: crate::result_artifact::ResultArtifact::test_frame(frame.as_ref().clone()),
				provenance: PlotProvenance {
					source_name: "scatter fixture".to_owned(),
					revision: None,
				},
			},
			identity,
		)
		.expect("domain resolution")
	}

	#[test]
	fn categorical_x_uses_the_requested_field_and_point_scale() {
		let frame = df!("cat" => ["a", "b", "c"], "val" => [2_i64, 5, 3]).expect("frame");
		let (spec, _) = resolved_scatter(
			frame.clone(),
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "cat", "val"),
			&ColorIdentity::default(),
		);
		let prepared = prepare_scatter_data_resolved(&frame, &spec, 800, 480);
		assert_eq!(prepared.x_label, "cat");
		assert_eq!(prepared.y_label, "val");
		assert_eq!(
			prepared.x_categories.as_deref(),
			Some(&["a".to_owned(), "b".to_owned(), "c".to_owned()][..])
		);
		assert_eq!(
			prepared.series[0]
				.x_values
				.into_no_null_iter()
				.collect::<Vec<_>>(),
			vec![0.0, 1.0, 2.0]
		);
		let kernel = ScatterPlotKernel::new(Arc::new(prepared));
		assert!(matches!(
			kernel.layout(ResolvedPlotSettings::default()),
			PlotLayout::CategoricalX { categories, .. } if categories == ["a", "b", "c"]
		));
	}

	#[test]
	fn product_0044_series_order_follows_the_display_domain() {
		let first = polars::df!(
			"series" => ["B", "A", "C", "B"],
			"x" => [1.0_f64, 2.0, 3.0, 4.0],
			"y" => [4.0_f64, 3.0, 2.0, 1.0]
		)
		.expect("first-order frame");
		let reordered = polars::df!(
			"series" => ["C", "B", "A", "B"],
			"x" => [3.0_f64, 1.0, 2.0, 4.0],
			"y" => [2.0_f64, 4.0, 3.0, 1.0]
		)
		.expect("reordered frame");
		let names = |frame: &DataFrame| {
			prepare_scatter_data(frame, "series", "x", "y")
				.series
				.into_iter()
				.map(|series| series.name)
				.collect::<Vec<_>>()
		};
		assert_eq!(names(&first), ["B", "A", "C"]);
		assert_eq!(names(&reordered), ["C", "B", "A"]);
	}

	#[test]
	fn resolved_category_colors_survive_filtering_and_reach_the_kernel() {
		let composition = PlotComposition::single(PlotType::Scatter);
		let authored = AuthoredPlotSpec::xy_color(composition, "x", "y", Some("series".to_owned()));
		let full = df!(
			"x" => [1_i64, 2, 3],
			"y" => [2_i64, 4, 6],
			"series" => ["a", "b", "c"]
		)
		.expect("full frame");
		let (full_spec, identity) =
			resolved_scatter(full.clone(), authored.clone(), &ColorIdentity::default());
		let filtered =
			df!("x" => [2_i64], "y" => [4_i64], "series" => ["b"]).expect("filtered frame");
		let (filtered_spec, _) = resolved_scatter(filtered.clone(), authored, &identity);
		let expected = full_spec
			.channel(Channel::Color)
			.unwrap()
			.colors
			.iter()
			.find(|entry| entry.category == "b")
			.unwrap()
			.color;
		let prepared = prepare_scatter_data_resolved(&filtered, &filtered_spec, 800, 480);
		assert_eq!(prepared.series.len(), 1);
		assert_eq!(prepared.series[0].name, "b");
		assert_eq!(
			prepared.series[0].resolved_color,
			Some(iced_color(expected))
		);
	}

	// PRODUCT-0138 replacement evidence. The withdrawn draft priced a mode
	// switch using a figure measured on the other side of the switch. This
	// matrix measures both paths at the same cardinalities and records what
	// each one actually draws, so fidelity is observed rather than assumed.
	#[test]
	#[ignore = "render matrix evidence; minutes, and allocates a large frame"]
	fn product_0138_scatter_render_matrix_evidence() {
		use crate::plot::common::{PlotRenderLayer, PlotWidget};
		use crate::plot::factory::create_authored_plot;
		use std::time::Instant;

		// Deterministic, dependency-free. Value distribution is not the
		// variable under test; only cardinality and series count are.
		fn frame(n: usize, series: usize) -> DataFrame {
			let mut state: u64 = 0x2026_0825;
			let mut next = || {
				state = state
					.wrapping_mul(6364136223846793005)
					.wrapping_add(1442695040888963407);
				((state >> 11) as f64) / ((1u64 << 53) as f64)
			};
			let xs: Vec<f64> = (0..n).map(|_| next()).collect();
			let ys: Vec<f64> = (0..n).map(|_| next()).collect();
			if series <= 1 {
				df!("x" => xs, "y" => ys).expect("frame")
			} else {
				let labels: Vec<String> = (0..n).map(|i| format!("s{}", i % series)).collect();
				df!("x" => xs, "y" => ys, "series" => labels).expect("frame")
			}
		}

		// `vector_cap` models the *proposed* configuration: the auto arm must
		// be auto-under-the-new-threshold, or it resolves to Vector across the
		// whole band and the comparison is vacuous.
		fn measure(
			n: usize,
			series: usize,
			mode: ScatterRenderMode,
			vector_cap: u32,
			bounds_px: (f32, f32),
		) -> (f64, f64, usize, usize) {
			let composition = PlotComposition::single(PlotType::Scatter);
			let spec = if series <= 1 {
				AuthoredPlotSpec::xy(composition, "x", "y")
			} else {
				AuthoredPlotSpec::xy_color(composition, "x", "y", Some("series".to_owned()))
			};
			let df = frame(n, series);
			// Preparation is timed separately: rasterization builds its image
			// here, not in render, so a render-only figure makes raster look
			// free.
			let prepare_started = Instant::now();
			let kernel = create_authored_plot(spec, &df, bounds_px.0 as u32, bounds_px.1 as u32)
				.expect("kernel");
			let prepare_ms = prepare_started.elapsed().as_secs_f64() * 1000.0;
			// PRODUCT-0139 §6: the model is the only settings input, and
			// `PlotWidget::new` resolves it once through the kernel for both
			// geometry and marks. Before that rule, this exhibit also handed the
			// widget a separate `ResolvedPlotSettings`, and setting the cap on
			// the wrong one of the two silently left every arm on the default
			// path — which is how an earlier revision produced three identical
			// arms.
			let mut settings_model = crate::plot::theme::PlotSettings::default();
			settings_model.library_defaults.scatter_render_mode = mode;
			settings_model.library_defaults.scatter_max_vector_points = vector_cap;
			let widget = PlotWidget::new(
				kernel.as_ref(),
				String::new(),
				0.0,
				settings_model,
				0,
				false,
				PlotRenderLayer::Full,
			);
			let rect = Rectangle::new([0.0, 0.0].into(), [bounds_px.0, bounds_px.1].into());
			// Latency from the real drawing backend; the counting backend does
			// no drawing and under-measures it by about two orders of
			// magnitude. Fidelity from the counting pass. Never both from one.
			let mut times = Vec::new();
			for _ in 0..5 {
				let mut backend = crate::plot::export::SvgBackend::new(bounds_px.0, bounds_px.1);
				let started = Instant::now();
				widget.render(&mut backend, rect);
				times.push(started.elapsed().as_secs_f64() * 1000.0);
				let _ = backend.finish();
			}
			times.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
			let mut counter = CountingBackend::default();
			widget.render(&mut counter, rect);
			(
				prepare_ms,
				times[times.len() / 2],
				counter.circles,
				counter.images,
			)
		}

		let viewport = (1200.0_f32, 720.0_f32);
		const PRESENT_CAP: u32 = 50_000;
		const PROPOSED_CAP: u32 = 20_000;
		println!("\npoints,series,arm,prepare_ms,render_ms,circles,images");

		// Chrome baseline: whatever a 10-point plot draws beyond its 10 points
		// is axes, legend swatches, and decoration. Reported, never silently
		// subtracted.
		for series in [1_usize, 4, 8] {
			let (p, r, c, i) = measure(10, series, ScatterRenderMode::Auto, PRESENT_CAP, viewport);
			println!("10,{series},chrome-baseline,{p:.1},{r:.1},{c},{i}");
		}

		// The control is the present behaviour in this band, which is Vector.
		// The comparison arm is auto under the *proposed* cap, which is what
		// the record would actually cause.
		for n in [20_001_usize, 25_000, 30_000, 40_000, 50_000] {
			for series in [1_usize, 4, 8] {
				for (label, mode, cap) in [
					("forced-vector", ScatterRenderMode::Vector, PRESENT_CAP),
					("auto-present-50k", ScatterRenderMode::Auto, PRESENT_CAP),
					("auto-proposed-20k", ScatterRenderMode::Auto, PROPOSED_CAP),
				] {
					let (p, r, c, i) = measure(n, series, mode, cap, viewport);
					println!("{n},{series},{label},{p:.1},{r:.1},{c},{i}");
				}
			}
		}

		// Raster needs total_points > raster_threshold AND > pixel_budget * 8.
		// At 1200x720 the second gate is 6,912,000 — so 1,000,001 downsamples
		// here, which is the second reason raster had never been reached.
		for n in [1_000_001_usize, 7_000_000] {
			let (p, r, c, i) = measure(n, 1, ScatterRenderMode::Auto, PRESENT_CAP, viewport);
			println!("{n},1,auto,{p:.1},{r:.1},{c},{i}");
		}
	}

	// PRODUCT-0138 follow-on: raster policy evidence. The render matrix
	// reported a 7.9 ms raster render, which was a *cache hit* — rasterization
	// happens on first render, not in preparation, so a median-of-five hides
	// the cost that a pixel-density gate would actually be trading on.
	// `ScatterRasterKey` includes x_range and y_range, so zoom and pan
	// invalidate the cache every frame.
	// PRODUCT-0138 follow-on: raster policy evidence.
	//
	// Two corrections to the previous revision are load-bearing. The domain
	// reaches the raster key through `PlotWidget.settings` — `layout()` is
	// called with it — while the *render mode* resolves from `settings_model`.
	// Editing the model's library defaults moved the mode and never the key,
	// so an earlier revision's "invalidated" column was measuring cache hits.
	// And the pixel budget divides the **inner plot area**, not the widget
	// rect, so the gate sits below `width * height * 8`.
	#[test]
	#[ignore = "raster policy evidence; allocates multi-million-point frames"]
	fn product_0138_raster_cache_and_gate_evidence() {
		use crate::plot::common::{PlotRenderLayer, PlotWidget};
		use crate::plot::export::SvgBackend;
		use crate::plot::factory::create_authored_plot;
		use std::time::Instant;

		fn frame(n: usize) -> DataFrame {
			let mut state: u64 = 0x2026_0825;
			let mut next = || {
				state = state
					.wrapping_mul(6364136223846793005)
					.wrapping_add(1442695040888963407);
				((state >> 11) as f64) / ((1u64 << 53) as f64)
			};
			let xs: Vec<f64> = (0..n).map(|_| next()).collect();
			let ys: Vec<f64> = (0..n).map(|_| next()).collect();
			df!("x" => xs, "y" => ys).expect("frame")
		}

		// PRODUCT-0139 §6: the domain rides on the settings *model*, the one
		// authority `PlotWidget::new` resolves for geometry and marks alike. An
		// earlier revision handed the widget a separate `ResolvedPlotSettings`
		// carrying the domain while the model stayed default — the exact
		// divergence the record closed, and the reason a previous run's
		// "invalidated" column was measuring cache hits.
		fn domain(x_min: f64, x_max: f64) -> crate::plot::theme::PlotSettings {
			let mut model = crate::plot::theme::PlotSettings::default();
			model.library_defaults.x_min = Some(x_min);
			model.library_defaults.x_max = Some(x_max);
			model
		}

		fn x_range_bits(layout: &PlotLayout) -> (u64, u64) {
			match layout {
				PlotLayout::Cartesian { x_range, .. } => (x_range.0.to_bits(), x_range.1.to_bits()),
				_ => (0, 0),
			}
		}

		let (w, h) = (1200.0_f32, 720.0_f32);
		let rect = Rectangle::new([0.0, 0.0].into(), [w, h].into());
		let df = frame(7_000_000);
		let kernel = create_authored_plot(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&df,
			w as u32,
			h as u32,
		)
		.expect("kernel");

		// Proof before timing: the key input must actually move. These are the
		// exact bits `ScatterRasterKey.x_range` is built from.
		let resolved = |model: &crate::plot::theme::PlotSettings| kernel.resolve_settings(model);
		let base = x_range_bits(&kernel.layout(resolved(&domain(0.0, 1.0))));
		let panned = x_range_bits(&kernel.layout(resolved(&domain(0.10, 1.10))));
		let zoomed = x_range_bits(&kernel.layout(resolved(&domain(0.25, 0.75))));
		assert_ne!(base, panned, "a pan must move the raster key");
		assert_ne!(base, zoomed, "a zoom must move the raster key");
		println!(
			"\nkey_moves_on_pan={} key_moves_on_zoom={}",
			base != panned,
			base != zoomed
		);

		let render = |model: crate::plot::theme::PlotSettings,
		              motion: ViewportMotion,
		              backend: &mut dyn PlotBackend| {
			let widget = PlotWidget::new(
				kernel.as_ref(),
				String::new(),
				0.0,
				model,
				0,
				false,
				PlotRenderLayer::Full,
			)
			.with_viewport_motion(motion);
			widget.render(backend, rect);
		};

		let time = |model: crate::plot::theme::PlotSettings, motion: ViewportMotion| {
			let mut b = SvgBackend::new(w, h);
			let started = Instant::now();
			render(model, motion, &mut b);
			let ms = started.elapsed().as_secs_f64() * 1000.0;
			let _ = b.finish();
			ms
		};

		// Cache miss: nothing rasterized yet for this domain.
		let miss_ms = time(domain(0.0, 1.0), ViewportMotion::Settled);
		// Cache hits: identical domain, so the key matches.
		let mut hits: Vec<f64> = (0..5)
			.map(|_| time(domain(0.0, 1.0), ViewportMotion::Settled))
			.collect();
		hits.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
		// The baseline this record was written against: a moving domain with no
		// reuse, which rescans every source point per frame.
		let mut pans: Vec<f64> = (1..=5)
			.map(|i| {
				let shift = i as f64 * 0.01;
				time(domain(shift, 1.0 + shift), ViewportMotion::Settled)
			})
			.collect();
		pans.sort_by(|a, b| a.partial_cmp(b).expect("finite"));

		// PRODUCT-0139 §9's stop condition: is the blit materially cheaper than
		// the rescan? The same pan, with the frames declared as motion. The
		// cache is primed at the base domain first, because reuse composes from
		// the last true raster and there must be one.
		let _ = time(domain(0.0, 1.0), ViewportMotion::Settled);
		let mut reused: Vec<f64> = (1..=5)
			.map(|i| {
				let shift = i as f64 * 0.01;
				time(domain(shift, 1.0 + shift), ViewportMotion::Moving)
			})
			.collect();
		reused.sort_by(|a, b| a.partial_cmp(b).expect("finite"));

		// And the settle: the first frame after motion stops rasterizes truly,
		// so it should cost a cache miss, not a hit.
		let settle_ms = time(domain(0.05, 1.05), ViewportMotion::Settled);

		let mut counter = CountingBackend::default();
		render(domain(0.0, 1.0), ViewportMotion::Settled, &mut counter);
		let (iw, ih) = counter.image_size.unwrap_or((0.0, 0.0));
		let inner_budget = (iw as usize) * (ih as usize);

		println!("widget_px={}x{} inner_plot_px={iw}x{ih}", w, h);
		println!(
			"naive_gate={} actual_gate={}",
			(w as usize) * (h as usize) * 8,
			inner_budget * 8
		);
		println!("\nscenario,ms");
		println!("first_render_cache_miss,{miss_ms:.1}");
		println!("steady_same_domain_hit,{:.1}", hits[hits.len() / 2]);
		println!("pan_domain_moves_each_frame,{:.1}", pans[pans.len() / 2]);
		println!("pan_with_reuse_moving,{:.1}", reused[reused.len() / 2]);
		println!("settle_after_motion,{settle_ms:.1}");
		println!("circles={} images={}", counter.circles, counter.images);
		println!(
			"reuse_speedup_vs_rescan={:.1}x",
			pans[pans.len() / 2] / reused[reused.len() / 2].max(f64::EPSILON)
		);
	}

	/// PRODUCT-0139 §1 and §5, end to end through the widget.
	///
	/// Instrument: `RASTER_BUILDS` counts scans of the source points, and the
	/// capture backend records where each frame's image was placed. Both are
	/// needed — a build count alone cannot tell a reused frame from a skipped
	/// one, and a placement alone cannot tell reuse from a rebuild that happened
	/// to land at the origin.
	#[derive(Default)]
	struct RasterCaptureBackend {
		placements: Vec<(Point, iced::Size)>,
		last_rgba: Option<Vec<u8>>,
	}

	impl PlotBackend for RasterCaptureBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}
		fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {}
		fn fill_rectangle(&mut self, _tl: Point, _size: iced::Size, _color: Color) {}
		fn fill_text_raw(&mut self, _text: Text) {}
		fn supports_embedded_raster(&self) -> bool {
			true
		}
		fn supports_native_image_handle(&self) -> bool {
			false
		}
		fn draw_image_rgba(
			&mut self,
			top_left: Point,
			size: iced::Size,
			_width: u32,
			_height: u32,
			rgba: &[u8],
		) {
			self.placements.push((top_left, size));
			self.last_rgba = Some(rgba.to_vec());
		}
		fn translate(&mut self, _translation: iced::Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn reuse_fixture() -> (
		Arc<dyn PlotKernel + Send + Sync>,
		crate::plot::theme::PlotSettings,
	) {
		use crate::plot::factory::create_authored_plot;
		let frame = df!(
			"x" => [0.0_f64, 0.25, 0.5, 0.75, 1.0],
			"y" => [0.0_f64, 0.4, 0.8, 0.3, 1.0]
		)
		.expect("frame");
		let kernel = create_authored_plot(
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&frame,
			200,
			200,
		)
		.expect("kernel");
		let mut model = crate::plot::theme::PlotSettings::default();
		// Force the raster path rather than reaching it by point count: this
		// gate is about reuse, not about the auto gate.
		model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;
		(kernel, model)
	}

	fn reuse_domain(
		model: &crate::plot::theme::PlotSettings,
		x: (f64, f64),
	) -> crate::plot::theme::PlotSettings {
		let mut model = model.clone();
		model.library_defaults.x_min = Some(x.0);
		model.library_defaults.x_max = Some(x.1);
		model
	}

	fn render_frame(
		kernel: &Arc<dyn PlotKernel + Send + Sync>,
		model: &crate::plot::theme::PlotSettings,
		motion: ViewportMotion,
	) -> RasterCaptureBackend {
		render_frame_sized(kernel, model, motion, 200.0)
	}

	fn render_frame_sized(
		kernel: &Arc<dyn PlotKernel + Send + Sync>,
		model: &crate::plot::theme::PlotSettings,
		motion: ViewportMotion,
		extent: f32,
	) -> RasterCaptureBackend {
		use crate::plot::common::{PlotRenderLayer, PlotWidget};
		let widget = PlotWidget::new(
			kernel.as_ref(),
			String::new(),
			0.0,
			model.clone(),
			0,
			false,
			PlotRenderLayer::Full,
		)
		.with_viewport_motion(motion);
		let mut backend = RasterCaptureBackend::default();
		widget.render(
			&mut backend,
			Rectangle::new(Point::ORIGIN, iced::Size::new(extent, extent)),
		);
		backend
	}

	fn builds_during(f: impl FnOnce()) -> usize {
		let before = RASTER_BUILDS.with(std::cell::Cell::get);
		f();
		RASTER_BUILDS.with(std::cell::Cell::get) - before
	}

	#[test]
	fn product_0139_motion_reuses_and_settling_rasterizes_exactly_once() {
		let (kernel, model) = reuse_fixture();
		let pans: Vec<_> = (1..=4)
			.map(|i| reuse_domain(&model, (i as f64 * 0.05, 1.0 + i as f64 * 0.05)))
			.collect();

		// Prime the cache with one true raster at the base domain.
		let base = reuse_domain(&model, (0.0, 1.0));
		let primed = builds_during(|| {
			render_frame(&kernel, &base, ViewportMotion::Settled);
		});
		assert_eq!(primed, 1, "the first settled frame rasterizes");

		// Gate 1 claim: a pan under motion performs no scans at all.
		let moving = builds_during(|| {
			for domain in &pans {
				render_frame(&kernel, domain, ViewportMotion::Moving);
			}
		});
		assert_eq!(
			moving, 0,
			"a moving frame must not rescan the source points"
		);

		// Gate 1 control, asserted in the tree: the same sequence without motion
		// rescans once per frame. A zero that has never been shown nonzero
		// against the unchanged path proves nothing.
		let (control_kernel, control_model) = reuse_fixture();
		let control_base = reuse_domain(&control_model, (0.0, 1.0));
		render_frame(&control_kernel, &control_base, ViewportMotion::Settled);
		let settled = builds_during(|| {
			for domain in &pans {
				render_frame(&control_kernel, domain, ViewportMotion::Settled);
			}
		});
		assert_eq!(
			settled,
			pans.len(),
			"without motion every moved domain must rescan, or the gate above is vacuous"
		);

		// Gate 2: settling performs exactly one scan, and the frame it produces
		// is byte-identical to a cold raster at the same domain.
		let settle_domain = pans.last().expect("pans");
		let settle = builds_during(|| {
			render_frame(&kernel, settle_domain, ViewportMotion::Settled);
		});
		assert_eq!(settle, 1, "settling rasterizes exactly once");

		let settled_frame = render_frame(&kernel, settle_domain, ViewportMotion::Settled);
		let (cold_kernel, cold_model) = reuse_fixture();
		let cold_frame = render_frame(
			&cold_kernel,
			&reuse_domain(
				&cold_model,
				(
					settle_domain.library_defaults.x_min.expect("x"),
					settle_domain.library_defaults.x_max.expect("x"),
				),
			),
			ViewportMotion::Settled,
		);
		assert_eq!(
			settled_frame.last_rgba, cold_frame.last_rgba,
			"a settled frame after motion must equal a cold raster at that domain"
		);
	}

	#[test]
	fn product_0139_a_reused_frame_is_placed_not_redrawn_and_never_chains() {
		let (kernel, model) = reuse_fixture();
		let base = reuse_domain(&model, (0.0, 1.0));
		let primed = render_frame(&kernel, &base, ViewportMotion::Settled);
		assert_eq!(primed.placements.len(), 1, "one image per frame");
		let (origin, base_size) = primed.placements[0];
		assert_eq!(
			origin,
			Point::ORIGIN,
			"a true raster is drawn at the origin"
		);
		// The raster is the **inner plot area**, not the widget rect — chrome
		// takes the difference. The expectation is read from the frame rather
		// than assumed, which is how the widget-sized version of this assertion
		// was wrong.
		assert!(base_size.width > 0.0 && base_size.width < 200.0);

		// Each moving frame is a placement of the same last true raster, so the
		// offsets grow with the pan rather than resetting — which is what
		// composing from the last true raster (§5) looks like from outside.
		let mut offsets = Vec::new();
		for i in 1..=3 {
			let shift = f64::from(i) * 0.05;
			let moved = render_frame(
				&kernel,
				&reuse_domain(&model, (shift, 1.0 + shift)),
				ViewportMotion::Moving,
			);
			let (top_left, size) = moved.placements[0];
			// A pure pan never rescales: the baked-in point radius is preserved.
			assert!(
				(size.width - base_size.width).abs() < 1e-3
					&& (size.height - base_size.height).abs() < 1e-3,
				"a pure pan must place the cached image at its own size"
			);
			offsets.push(top_left.x);
		}
		assert!(
			offsets.windows(2).all(|w| w[1] < w[0]),
			"panning right moves the cached image left, monotonically: {offsets:?}"
		);

		// No chaining, observed from outside rather than by reading private
		// state: if any reuse frame had replaced the cache entry, the base
		// domain would no longer be cached and would have to rescan. It does
		// not, so the entry the reuses composed from is still the last true
		// raster.
		let rescans = builds_during(|| {
			render_frame(&kernel, &base, ViewportMotion::Settled);
		});
		assert_eq!(
			rescans, 0,
			"reuse must never replace the cache entry it composed from"
		);
	}

	/// PRODUCT-0139 §10 gate 6: refusal, each case asserted on its own.
	///
	/// Reuse depicts the same marks at a different viewport, so **only** the
	/// domain may differ. Each case here changes one non-domain key term while
	/// the plot is moving, and each must still rescan. A single "any change"
	/// case would pass on the strength of whichever one happened to work — and
	/// a mutant that dropped every non-domain term from the reuse predicate
	/// survived the rest of this file's gates until these existed.
	#[test]
	fn product_0139_reuse_refuses_every_non_domain_change_individually() {
		use crate::plot::colors::ColorTheme;
		use crate::plot::spec::PointSize;

		// Case 1 — the raster size. A different widget rect gives a different
		// inner plot area, so the cached pixels are the wrong shape.
		{
			let (kernel, model) = reuse_fixture();
			let base = reuse_domain(&model, (0.0, 1.0));
			render_frame(&kernel, &base, ViewportMotion::Settled);
			let moved = reuse_domain(&model, (0.1, 1.1));
			let rescans = builds_during(|| {
				render_frame_sized(&kernel, &moved, ViewportMotion::Moving, 260.0);
			});
			assert_eq!(rescans, 1, "a size change must rescan even while moving");
		}

		// Case 2 — point size. The radius is baked into the bitmap.
		{
			let (kernel, model) = reuse_fixture();
			let base = reuse_domain(&model, (0.0, 1.0));
			render_frame(&kernel, &base, ViewportMotion::Settled);
			let mut changed = reuse_domain(&model, (0.1, 1.1));
			changed.library_defaults.point_size = PointSize::new(2.0).expect("in range");
			let rescans = builds_during(|| {
				render_frame(&kernel, &changed, ViewportMotion::Moving);
			});
			assert_eq!(
				rescans, 1,
				"a point-size change must rescan even while moving"
			);
		}

		// Case 3 — series colour. The colour is baked in too.
		{
			let (kernel, model) = reuse_fixture();
			let base = reuse_domain(&model, (0.0, 1.0));
			render_frame(&kernel, &base, ViewportMotion::Settled);
			let mut changed = reuse_domain(&model, (0.1, 1.1));
			changed.library_defaults.color_theme = ColorTheme::Black;
			let rescans = builds_during(|| {
				render_frame(&kernel, &changed, ViewportMotion::Moving);
			});
			assert_eq!(rescans, 1, "a colour change must rescan even while moving");
		}

		// Case 4 — a categorical x. Placement is by category index and band
		// width, which is not a function of the key, so the map is not
		// derivable and reuse must refuse rather than guess.
		{
			use crate::plot::factory::create_authored_plot;
			let frame = df!(
				"x" => ["a", "b", "c", "d"],
				"y" => [1.0_f64, 2.0, 3.0, 4.0]
			)
			.expect("frame");
			let kernel = create_authored_plot(
				AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
				&frame,
				200,
				200,
			)
			.expect("kernel");
			let mut model = crate::plot::theme::PlotSettings::default();
			model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;

			let mut moved = model.clone();
			moved.library_defaults.y_min = Some(0.5);
			moved.library_defaults.y_max = Some(4.5);
			render_frame(&kernel, &model, ViewportMotion::Settled);
			let rescans = builds_during(|| {
				render_frame(&kernel, &moved, ViewportMotion::Moving);
			});
			assert_eq!(
				rescans, 1,
				"a categorical x is not reusable: its placement is not derivable from the key"
			);
		}
	}

	/// PRODUCT-0140 evidence: what rasterizing costs at the gate, and what the
	/// bare `× 8` multiplier is actually choosing.
	///
	/// Two questions the audit left open. **Cold raster cost** is the stall a
	/// lower gate makes more common, and it has never been priced at the
	/// cardinalities near the gate itself. **The multiplier is a density** —
	/// "rasterize above 8 points per pixel" — so pricing it means finding the
	/// density at which rasterizing actually beats downsampling, rather than
	/// assuming 8 is that density.
	///
	/// Every arm builds a fresh kernel, because the raster cache lives on the
	/// renderer and a reused kernel would price a hit instead of a cold build.
	/// Every arm also reports the draw calls it emitted, so the path each figure
	/// took is confirmed rather than assumed.
	#[test]
	#[ignore = "PRODUCT-0140 gate pricing; allocates multi-million-point frames"]
	fn product_0140_gate_pricing_evidence() {
		use crate::plot::common::{PlotRenderLayer, PlotWidget};
		use crate::plot::export::SvgBackend;
		use crate::plot::factory::create_authored_plot;
		use std::time::Instant;

		fn frame(n: usize) -> DataFrame {
			let mut state: u64 = 0x2026_0827;
			let mut next = || {
				state = state
					.wrapping_mul(6364136223846793005)
					.wrapping_add(1442695040888963407);
				((state >> 11) as f64) / ((1u64 << 53) as f64)
			};
			let xs: Vec<f64> = (0..n).map(|_| next()).collect();
			let ys: Vec<f64> = (0..n).map(|_| next()).collect();
			df!("x" => xs, "y" => ys).expect("frame")
		}

		/// One cold render at a forced mode. Returns (ms, circles, images).
		fn measure(n: usize, widget: (f32, f32), mode: ScatterRenderMode) -> (f64, usize, usize) {
			let df = frame(n);
			let kernel = create_authored_plot(
				AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
				&df,
				widget.0 as u32,
				widget.1 as u32,
			)
			.expect("kernel");
			let mut model = crate::plot::theme::PlotSettings::default();
			model.library_defaults.scatter_render_mode = mode;
			fn widget_for<'a>(
				kernel: &'a (dyn PlotKernel + Send + Sync),
				model: &crate::plot::theme::PlotSettings,
			) -> PlotWidget<'a> {
				PlotWidget::new(
					kernel,
					String::new(),
					0.0,
					model.clone(),
					0,
					false,
					PlotRenderLayer::Full,
				)
			}
			let rect = Rectangle::new(Point::ORIGIN, iced::Size::new(widget.0, widget.1));

			let mut svg = SvgBackend::new(widget.0, widget.1);
			let started = Instant::now();
			widget_for(kernel.as_ref(), &model).render(&mut svg, rect);
			let ms = started.elapsed().as_secs_f64() * 1000.0;
			let _ = svg.finish();

			// Path confirmation from a second, fresh kernel so the timing above
			// is not polluted by the counting pass.
			let cold = create_authored_plot(
				AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
				&df,
				widget.0 as u32,
				widget.1 as u32,
			)
			.expect("kernel");
			let mut counter = CountingBackend::default();
			widget_for(cold.as_ref(), &model).render(&mut counter, rect);
			(ms, counter.circles, counter.images)
		}

		// Part 1 — cold raster at gate-adjacent cardinalities. The full window's
		// gate is 5,203,000 (the pixel budget binds); the small pane's is
		// 1,000,000 (the threshold binds, its budget being only 407,000).
		println!("\npart,widget,points,mode,ms,circles,images");
		for (label, widget, cardinalities) in [
			(
				"full-window (budget binds)",
				(1200.0_f32, 720.0_f32),
				[2_600_000_usize, 5_203_000, 7_800_000],
			),
			(
				"small-pane (threshold binds)",
				(400.0, 300.0),
				[500_000, 1_000_000, 2_000_000],
			),
		] {
			for n in cardinalities {
				let (ms, circles, images) = measure(n, widget, ScatterRenderMode::Rasterized);
				println!(
					"cold-raster,{label} {}x{},{n},forced-raster,{ms:.1},{circles},{images}",
					widget.0, widget.1
				);
			}
		}

		// Part 2 — price the multiplier. It is a density, so sweep density and
		// find where rasterizing actually overtakes downsampling. If the
		// crossover is not near 8, the constant is not where the evidence puts
		// it.
		for (label, widget, area) in [
			("small-pane", (400.0_f32, 300.0_f32), 275_usize * 185),
			("full-window", (1200.0, 720.0), 1075_usize * 605),
		] {
			for density in [1_usize, 2, 4, 8, 16] {
				let n = area * density;
				let (down_ms, down_circles, _) = measure(n, widget, ScatterRenderMode::Downsampled);
				let (rast_ms, _, rast_images) = measure(n, widget, ScatterRenderMode::Rasterized);
				println!(
					"multiplier,{label} {}x{},{n},density={density},down={down_ms:.1},raster={rast_ms:.1},down_circles={down_circles},raster_images={rast_images}",
					widget.0, widget.1
				);
			}
		}
	}

	/// PRODUCT-0140 evidence: what the raster gate's pixel budget actually is.
	///
	/// `resolve_render_mode` divides `bounds`, and `bounds` is the **inner plot
	/// area** — the widget rect minus every guide band. This exhibit measures
	/// that rectangle for representative configurations, reading it from the
	/// rasterizer's own output rather than reconstructing it, so the gate is
	/// priced against what production computes.
	#[test]
	#[ignore = "PRODUCT-0140 gate-input evidence"]
	fn product_0140_gate_input_evidence() {
		use crate::plot::common::{PlotRenderLayer, PlotWidget};
		use crate::plot::factory::create_authored_plot;

		fn frame(series: usize, y_scale: f64) -> DataFrame {
			let xs: Vec<f64> = (0..64).map(|i| f64::from(i)).collect();
			let ys: Vec<f64> = (0..64).map(|i| f64::from(i) * y_scale).collect();
			if series <= 1 {
				df!("x" => xs, "y" => ys).expect("frame")
			} else {
				let labels: Vec<String> =
					(0..64).map(|i| format!("series {}", i % series)).collect();
				df!("x" => xs, "y" => ys, "series" => labels).expect("frame")
			}
		}

		fn inner_plot_px(
			series: usize,
			y_scale: f64,
			title: Option<&str>,
			widget: (f32, f32),
		) -> (f32, f32) {
			let composition = PlotComposition::single(PlotType::Scatter);
			let spec = if series <= 1 {
				AuthoredPlotSpec::xy(composition, "x", "y")
			} else {
				AuthoredPlotSpec::xy_color(composition, "x", "y", Some("series".to_owned()))
			};
			let df = frame(series, y_scale);
			let kernel =
				create_authored_plot(spec, &df, widget.0 as u32, widget.1 as u32).expect("kernel");
			let mut model = crate::plot::theme::PlotSettings::default();
			// Force the raster path so the image reports the inner rectangle.
			model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;
			let widget_plot = PlotWidget::new(
				kernel.as_ref(),
				title.unwrap_or_default().to_owned(),
				0.0,
				model,
				0,
				false,
				PlotRenderLayer::Full,
			);
			let mut counter = CountingBackend::default();
			widget_plot.render(
				&mut counter,
				Rectangle::new(Point::ORIGIN, iced::Size::new(widget.0, widget.1)),
			);
			counter
				.image_size
				.expect("the raster path reports its rectangle")
		}

		println!(
			"
configuration,widget_px,inner_px,inner_share,effective_gate,naive_gate"
		);
		let cases: [(&str, usize, f64, Option<&str>, (f32, f32)); 7] = [
			("baseline 1200x720", 1, 1.0, None, (1200.0, 720.0)),
			("with title", 1, 1.0, Some("A title"), (1200.0, 720.0)),
			("8 series (legend)", 8, 1.0, None, (1200.0, 720.0)),
			("wide y tick labels", 1, 1.0e9, None, (1200.0, 720.0)),
			(
				"wide labels + legend + title",
				8,
				1.0e9,
				Some("A title"),
				(1200.0, 720.0),
			),
			("small pane 400x300", 1, 1.0, None, (400.0, 300.0)),
			("small pane, wide labels", 1, 1.0e9, None, (400.0, 300.0)),
		];
		for (label, series, y_scale, title, widget) in cases {
			let (iw, ih) = inner_plot_px(series, y_scale, title, widget);
			let inner = (iw as usize) * (ih as usize);
			let naive = (widget.0 as usize) * (widget.1 as usize);
			println!(
				"{label},{}x{},{iw}x{ih},{:.3},{},{}",
				widget.0,
				widget.1,
				inner as f64 / naive as f64,
				inner * 8,
				naive * 8
			);
		}
	}

	/// PRODUCT-0139 §2 gate: the reuse placement maps the cached image onto the
	/// new viewport exactly.
	///
	/// The oracle is the **production projection**, not this test's own algebra:
	/// each data point is projected through `CoordinateTransformer::cartesian`
	/// under the cached domain and again under the new one, and the placement
	/// must carry the first onto the second. Recomputing the offset from the
	/// same formula the implementation uses would be circular.
	#[test]
	fn product_0139_reuse_placement_carries_old_pixels_onto_the_new_viewport() {
		fn key(x: (f64, f64), y: (f64, f64)) -> ScatterRasterKey {
			ScatterRasterKey {
				width: 400,
				height: 300,
				point_size: 1.0_f32.to_bits(),
				x_range: (x.0.to_bits(), x.1.to_bits()),
				y_range: (y.0.to_bits(), y.1.to_bits()),
				background: [0, 0, 0, 255],
				series_colors: vec![[1, 2, 3, 255]],
			}
		}
		fn layout(x: (f64, f64), y: (f64, f64)) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: x,
				y_range: y,
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
			}
		}

		let (w, h) = (400.0_f32, 300.0_f32);
		let bounds = Rectangle::new(Point::ORIGIN, iced::Size::new(w, h));
		let cached_domain = ((0.0, 1.0), (0.0, 1.0));

		for (label, new_domain) in [
			("pure pan", ((0.10, 1.10), (0.0, 1.0))),
			("pan in y", ((0.0, 1.0), (-0.25, 0.75))),
			("zoom in", ((0.25, 0.75), (0.25, 0.75))),
			("zoom out", ((-1.0, 2.0), (-1.0, 2.0))),
			("pan and zoom", ((0.30, 0.80), (0.10, 0.60))),
		] {
			let cached = key(cached_domain.0, cached_domain.1);
			let current = key(new_domain.0, new_domain.1);
			let placement =
				reuse_placement(&cached, &current, w, h).expect("cartesian keys are derivable");

			let cached_layout = layout(cached_domain.0, cached_domain.1);
			let new_layout = layout(new_domain.0, new_domain.1);
			let cached_transform = CoordinateTransformer::new(&cached_layout, bounds);
			let new_transform = CoordinateTransformer::new(&new_layout, bounds);

			let scale_x = placement.size.width / w;
			let scale_y = placement.size.height / h;
			for (dx, dy) in [(0.0, 0.0), (1.0, 1.0), (0.5, 0.5), (0.25, 0.75)] {
				let old_pixel = cached_transform.cartesian(dx, dy);
				let new_pixel = new_transform.cartesian(dx, dy);
				let carried = Point::new(
					placement.top_left.x + old_pixel.x * scale_x,
					placement.top_left.y + old_pixel.y * scale_y,
				);
				assert!(
					(carried.x - new_pixel.x).abs() < 1e-2
						&& (carried.y - new_pixel.y).abs() < 1e-2,
					"{label}: ({dx},{dy}) carried to {carried:?} but projects to {new_pixel:?}"
				);
			}
		}

		// A pure pan must not rescale: radius is baked into the bitmap, and
		// PRODUCT-0139 §2 says a translation preserves it exactly.
		let panned = reuse_placement(
			&key((0.0, 1.0), (0.0, 1.0)),
			&key((0.10, 1.10), (0.0, 1.0)),
			w,
			h,
		)
		.expect("derivable");
		assert!((panned.size.width - w).abs() < 1e-3);
		assert!((panned.size.height - h).abs() < 1e-3);

		// A non-Cartesian key records (0, 0) ranges, which is not a domain and
		// must refuse rather than produce a placement.
		let degenerate = ScatterRasterKey {
			x_range: (0, 0),
			y_range: (0, 0),
			..key((0.0, 1.0), (0.0, 1.0))
		};
		assert!(reuse_placement(&degenerate, &degenerate, w, h).is_none());
	}

	/// PRODUCT-0139 §6 gate, scatter's half: the kernel's resolution is exactly
	/// what its renderer draws with, so the transform, the resolved mode, and
	/// `ScatterRasterKey` are one resolution. The control asserts the fixture
	/// actually discriminates before the claim is made.
	#[test]
	fn product_0139_scatter_resolves_geometry_through_its_own_layer_settings() {
		use crate::plot::spec::{LayerSettings, Override, PointSize};
		use crate::plot::theme::{PlotDomainOverrides, PlotSettings};

		let layer_settings = LayerSettings {
			point_size: Override::Set(PointSize::new(1.5).expect("in range")),
			..LayerSettings::default()
		};
		let frame = df!("x" => [0.0_f64, 1.0, 2.0], "y" => [2_i64, 5, 3]).expect("frame");
		let (spec, _) = resolved_scatter(
			frame.clone(),
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&ColorIdentity::default(),
		);
		let prepared = prepare_scatter_data_resolved(&frame, &spec, 800, 480);
		let scatter =
			ScatterPlotKernel::with_layer_settings(Arc::new(prepared), layer_settings.clone());

		let mut model = PlotSettings::default();
		model.layer.domains = Override::Set(PlotDomainOverrides {
			x_min: Some(100.0),
			x_max: Some(200.0),
			y_min: None,
			y_max: None,
		});
		model.layer.point_size = Override::Set(PointSize::new(4.0).expect("in range"));

		let authority = scatter.resolve_settings(&model);
		let figure_wide = model.resolve();

		// Control: the fixture must make the two resolutions disagree.
		assert_ne!(figure_wide.point_size, authority.point_size);
		assert_ne!(figure_wide.x_min, authority.x_min);

		// Claim: scatter's authority is exactly what its renderer resolves.
		// `ResolvedPlotSettings` is not `PartialEq`, so the comparison names the
		// fields the layer tier actually moves — the point size it sets and the
		// domain it drops — rather than widening a public type for a test.
		let renderer_resolution = model.resolve_for_layer(&layer_settings);
		assert_eq!(authority.point_size, renderer_resolution.point_size);
		assert_eq!(authority.x_min, renderer_resolution.x_min);
		assert_eq!(authority.x_max, renderer_resolution.x_max);
		assert_eq!(
			authority.scatter_max_vector_points,
			renderer_resolution.scatter_max_vector_points
		);
	}

	#[test]
	fn automatic_render_mode_preserves_every_threshold_boundary() {
		let settings = ResolvedPlotSettings::default();
		assert_eq!(settings.scatter_max_vector_points, 50_000);
		assert_eq!(settings.scatter_downsample_target, 200_000);
		assert_eq!(settings.scatter_raster_threshold, 1_000_000);

		let viewport = bounds(100.0, 100.0);
		assert_eq!(
			resolve_render_mode(50_000, true, viewport, &settings),
			ResolvedScatterRenderMode::Vector
		);
		assert_eq!(
			resolve_render_mode(50_001, true, viewport, &settings),
			ResolvedScatterRenderMode::Downsampled
		);
		assert_eq!(
			resolve_render_mode(1_000_000, true, viewport, &settings),
			ResolvedScatterRenderMode::Downsampled
		);
		assert_eq!(
			resolve_render_mode(1_000_001, true, viewport, &settings),
			ResolvedScatterRenderMode::Rasterized
		);
		assert_eq!(
			resolve_render_mode(1_000_001, false, viewport, &settings),
			ResolvedScatterRenderMode::Downsampled,
			"raster threshold cannot override backend capability"
		);

		let mut density = settings;
		density.scatter_max_vector_points = 0;
		density.scatter_raster_threshold = 0;
		let hundred_pixels = bounds(10.0, 10.0);
		assert_eq!(
			resolve_render_mode(800, true, hundred_pixels, &density),
			ResolvedScatterRenderMode::Downsampled,
			"eight points per pixel is still downsampled"
		);
		assert_eq!(
			resolve_render_mode(801, true, hundred_pixels, &density),
			ResolvedScatterRenderMode::Rasterized,
			"rasterization begins strictly above eight points per pixel"
		);
	}

	#[test]
	fn explicit_render_modes_and_raster_fallback_ignore_auto_thresholds() {
		let viewport = bounds(1.0, 1.0);
		let mut settings = ResolvedPlotSettings {
			scatter_render_mode: ScatterRenderMode::Vector,
			..ResolvedPlotSettings::default()
		};
		assert_eq!(
			resolve_render_mode(usize::MAX, true, viewport, &settings),
			ResolvedScatterRenderMode::Vector
		);

		settings.scatter_render_mode = ScatterRenderMode::Downsampled;
		assert_eq!(
			resolve_render_mode(0, true, viewport, &settings),
			ResolvedScatterRenderMode::Downsampled
		);

		settings.scatter_render_mode = ScatterRenderMode::Rasterized;
		assert_eq!(
			resolve_render_mode(0, true, viewport, &settings),
			ResolvedScatterRenderMode::Rasterized
		);
		assert_eq!(
			resolve_render_mode(0, false, viewport, &settings),
			ResolvedScatterRenderMode::Downsampled
		);
	}

	#[test]
	fn downsample_grid_output_size_preserves_the_current_cell_budget() {
		let viewport = bounds(4.0, 3.0);
		let layout = PlotLayout::Cartesian {
			x_range: (0.0, 4.0),
			y_range: (0.0, 3.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
		};
		let transform = CoordinateTransformer::new(&layout, viewport);
		let mut xs = Vec::new();
		let mut ys = Vec::new();
		for row in 0..3 {
			for column in 0..4 {
				xs.push(column as f64 + 0.5);
				ys.push(3.0 - (row as f64 + 0.5));
			}
		}
		let series = ScatterSeries {
			name: "grid".to_owned(),
			x_values: Series::new("x".into(), xs).f64().unwrap().clone(),
			y_values: Series::new("y".into(), ys).f64().unwrap().clone(),
			color_t: 0.5,
			resolved_color: None,
			provenance: LegendProvenance::Named,
		};

		let sampled = downsample_series(&series, viewport, &transform, 11);
		assert_eq!(
			sampled.len(),
			12,
			"the ceil-derived 4x3 grid currently permits one point beyond target 11"
		);
		let one = downsample_series(&series, viewport, &transform, 1);
		assert_eq!(
			one.len(),
			2,
			"the 4:3 aspect ratio currently expands target one to a 2x1 grid"
		);
	}

	fn changed_pixels(rgba: &[u8], background: Color) -> usize {
		let background = color_to_rgba8(background);
		rgba.chunks_exact(4)
			.filter(|pixel| *pixel != background)
			.count()
	}

	#[test]
	fn product_0017_raster_draws_clipped_discs_and_keys_the_resolved_size() {
		let frame = df!("x" => [5.0], "y" => [5.0]).expect("frame");
		let (spec, _) = resolved_scatter(
			frame.clone(),
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
			&ColorIdentity::default(),
		);
		let prepared = Arc::new(prepare_scatter_data_resolved(&frame, &spec, 21, 21));
		let renderer = ScatterMarkRenderer::new(Arc::clone(&prepared));
		let viewport = bounds(21.0, 21.0);
		let centered_layout = PlotLayout::Cartesian {
			x_range: (4.0, 6.0),
			y_range: (4.0, 6.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
		};
		let centered = CoordinateTransformer::new(&centered_layout, viewport);
		let default_settings = ResolvedPlotSettings::default();
		let first = renderer.rasterize(viewport, &centered, &default_settings);
		let repeat = renderer.rasterize(viewport, &centered, &default_settings);
		assert!(
			Arc::ptr_eq(&first.rgba, &repeat.rgba),
			"same key must reuse cache"
		);
		let default_pixels = changed_pixels(
			&first.rgba,
			default_settings.chrome.canvas_background.iced(),
		);
		assert!(
			default_pixels > 1,
			"default radius must be a disc, not one pixel"
		);

		let mut doubled = default_settings.clone();
		doubled.point_size = PointSize::new(2.0).unwrap();
		let large = renderer.rasterize(viewport, &centered, &doubled);
		assert!(
			!Arc::ptr_eq(&first.rgba, &large.rgba),
			"size change must miss the prior cache entry"
		);
		assert!(
			changed_pixels(&large.rgba, doubled.chrome.canvas_background.iced()) > default_pixels,
			"doubled radius must paint a larger footprint"
		);
		let large_repeat = renderer.rasterize(viewport, &centered, &doubled);
		assert!(Arc::ptr_eq(&large.rgba, &large_repeat.rgba));

		let edge_layout = PlotLayout::Cartesian {
			x_range: (5.0, 7.0),
			y_range: (3.0, 5.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_temporal_domain: None,
			y_temporal_domain: None,
		};
		let edge = CoordinateTransformer::new(&edge_layout, viewport);
		let clipped = build_raster_rgba(&prepared, viewport, &edge, &default_settings);
		let clipped_pixels =
			changed_pixels(&clipped, default_settings.chrome.canvas_background.iced());
		assert!(clipped_pixels > 0);
		assert!(clipped_pixels < default_pixels, "edge disc must be clipped");
	}
}
