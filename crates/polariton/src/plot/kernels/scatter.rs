use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, ScatterRenderMode, format_label, polars_type_to_axis_type,
};
use crate::plot::spec::{
	Channel, ResolvedColor, ResolvedLayout, ResolvedPlotSpec, ScaleDomain, ScaleFamily,
};
use iced::advanced::image;
use iced::advanced::mouse::Cursor;
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::{Arc, Mutex};

pub struct ScatterPlotKernel {
	pub prepared_data: Arc<ScatterPreparedData>,
	renderer: ScatterMarkRenderer,
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
	x_range: (u64, u64),
	y_range: (u64, u64),
	background: [u8; 4],
	series_colors: Vec<[u8; 4]>,
}

impl PlotKernel for ScatterPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		if let Some(categories) = &self.prepared_data.x_categories {
			return PlotLayout::CategoricalX {
				categories: categories.clone(),
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
			x_axis_type: self.prepared_data.x_axis_type,
			y_axis_type: self.prepared_data.y_axis_type,
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

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: ResolvedPlotSettings,
	) {
		draw_scatter_legend(&self.prepared_data, backend, bounds, settings);
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos)
		{
			return Some(format!(
				"X: {}, Y: {}",
				format_label(x, self.prepared_data.x_axis_type),
				format_label(y, self.prepared_data.y_axis_type)
			));
		}
		None
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub(crate) fn draw_scatter_legend(
	prepared_data: &ScatterPreparedData,
	backend: &mut dyn PlotBackend,
	bounds: Rectangle,
	settings: ResolvedPlotSettings,
) {
	let num_series = prepared_data.series.len();
	if num_series == 0 {
		return;
	}
	let max_rows = settings.max_legend_rows.max(1) as usize;
	let num_cols = num_series.div_ceil(max_rows);
	let actual_rows = num_series.min(max_rows);
	let item_height = 25.0;
	let legend_padding = 10.0;
	let swatch_size = 12.0;
	let col_width = 150.0;
	let legend_width = num_cols as f32 * col_width + legend_padding * 2.0;
	let legend_height = actual_rows as f32 * item_height + legend_padding * 2.0;
	let x = (bounds.width - legend_width) * settings.legend_x;
	let y = (bounds.height - legend_height) * settings.legend_y;
	backend.fill_rectangle(
		iced::Point::new(x, y),
		iced::Size::new(legend_width, legend_height),
		Color {
			a: 0.6,
			..settings.background_color
		},
	);
	for (i, series) in prepared_data.series.iter().enumerate() {
		let color = series_color(series, &settings);
		let col = i / max_rows;
		let row = i % max_rows;
		let item_x = x + legend_padding + col as f32 * col_width;
		let item_y = y + legend_padding + row as f32 * item_height;
		backend.fill_path(
			&|builder| {
				builder.circle(
					iced::Point::new(item_x + swatch_size / 2.0, item_y + item_height / 2.0),
					swatch_size / 2.0,
				);
			},
			color,
		);
		backend.fill_text(iced::widget::canvas::Text {
			content: series.name.clone(),
			position: iced::Point::new(item_x + swatch_size + 10.0, item_y + item_height / 2.0),
			color: settings.decoration_color,
			size: iced::Pixels(settings.legend_size),
			align_x: iced::alignment::Horizontal::Left.into(),
			align_y: iced::alignment::Vertical::Center,
			..Default::default()
		});
	}
}

impl ScatterPlotKernel {
	pub fn new(prepared_data: Arc<ScatterPreparedData>) -> Self {
		Self {
			prepared_data: Arc::clone(&prepared_data),
			renderer: ScatterMarkRenderer::new(prepared_data),
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
		let hit_radius = self.prepared_data.point_size_px + 2.0;
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
		for series in &self.prepared_data.series {
			let color = series
				.resolved_color
				.unwrap_or_else(|| settings.color_theme.get_color(series.color_t));
			backend.fill_path(
				&|builder| {
					series.for_each_point(|x, y| {
						let pixel_p = scatter_point(transform, x, y);
						builder.circle(pixel_p, self.prepared_data.point_size_px);
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
						builder.circle(*point, self.prepared_data.point_size_px);
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
	) {
		let raster = self.rasterize(bounds, transform, settings);
		let size = iced::Size::new(bounds.width.max(1.0), bounds.height.max(1.0));
		if backend.supports_native_image_handle() {
			backend.draw_image_handle(Point::ORIGIN, size, &raster.image_handle);
		} else {
			backend.draw_image_rgba(
				Point::ORIGIN,
				size,
				raster.width,
				raster.height,
				raster.rgba.as_slice(),
			);
		}
	}

	fn rasterize(
		&self,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: &ResolvedPlotSettings,
	) -> ScatterRasterCache {
		let width = bounds.width.max(1.0).ceil() as u32;
		let height = bounds.height.max(1.0).ceil() as u32;
		let key = ScatterRasterKey {
			width,
			height,
			x_range: current_x_range_bits(transform),
			y_range: current_y_range_bits(transform),
			background: color_to_rgba8(settings.background_color),
			series_colors: self
				.prepared_data
				.series
				.iter()
				.map(|series| color_to_rgba8(series_color(series, settings)))
				.collect(),
		};

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
	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
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
				self.plot_rasterized(backend, bounds, transform, &settings)
			}
		}
	}

	fn hit_test(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<MarkHit> {
		self.hit_point(transform, cursor.position()?)
	}
}

pub struct ScatterSeries {
	pub name: String,
	pub x_values: Float64Chunked,
	pub y_values: Float64Chunked,
	pub color_t: f32,
	pub resolved_color: Option<Color>,
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
	pub point_size_px: f32,
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

fn build_raster_rgba(
	prepared_data: &ScatterPreparedData,
	bounds: Rectangle,
	transform: &CoordinateTransformer,
	settings: &ResolvedPlotSettings,
) -> Vec<u8> {
	let width = bounds.width.max(1.0).ceil() as usize;
	let height = bounds.height.max(1.0).ceil() as usize;
	let mut accum = vec![[0.0_f32; 4]; width * height];

	for series in &prepared_data.series {
		let color = series_color(series, settings);
		series.for_each_point(|x, y| {
			let point = scatter_point(transform, x, y);
			let xi = point.x.floor() as isize;
			let yi = point.y.floor() as isize;
			if xi < 0 || yi < 0 || xi >= width as isize || yi >= height as isize {
				return;
			}
			let idx = yi as usize * width + xi as usize;
			accum[idx][0] += color.r * color.a;
			accum[idx][1] += color.g * color.a;
			accum[idx][2] += color.b * color.a;
			accum[idx][3] += color.a.max(0.1);
		});
	}

	let bg = settings.background_color;
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
	point_size_px: f32,
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
		let mut prepared = prepare_scatter_data(df, color_field, x_field, y_field, point_size_px);
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
		});
	}
	ScatterPreparedData {
		series,
		x_categories: Some(categories.clone()),
		x_range: layout.x_range,
		y_range,
		x_axis_type: axis_type_from_scale(x.binding.scale),
		y_axis_type: axis_type_from_scale(y.binding.scale),
		point_size_px,
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

fn axis_type_from_scale(scale: ScaleFamily) -> AxisType {
	match scale {
		ScaleFamily::Time => AxisType::Datetime(crate::plot::common::TimeUnit::Milliseconds),
		_ => AxisType::Linear,
	}
}

pub fn prepare_scatter_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	point_size_px: f32,
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
			point_size_px,
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
		});
	}
	ScatterPreparedData {
		series: series_list,
		x_categories: None,
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		point_size_px,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
		total_points,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
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
				data: frame,
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
		let prepared = prepare_scatter_data_resolved(&frame, &spec, 800, 480, 3.0);
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
		let prepared = prepare_scatter_data_resolved(&filtered, &filtered_spec, 800, 480, 3.0);
		assert_eq!(prepared.series.len(), 1);
		assert_eq!(prepared.series[0].name, "b");
		assert_eq!(
			prepared.series[0].resolved_color,
			Some(iced_color(expected))
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
}
