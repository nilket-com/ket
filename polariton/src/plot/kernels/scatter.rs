use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout,
	PlotSettings, ScatterRenderMode, format_label, polars_type_to_axis_type,
};
use iced::advanced::image;
use iced::advanced::mouse::Cursor;
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::{Arc, Mutex};

pub struct ScatterPlotKernel {
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
	fn layout(&self, settings: PlotSettings) -> PlotLayout {
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
		settings: PlotSettings,
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

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: PlotSettings,
	) {
		let num_series = self.prepared_data.series.len();
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
		for (i, series) in self.prepared_data.series.iter().enumerate() {
			let color = settings.color_theme.get_color(series.color_t);
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

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos) {
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

impl ScatterPlotKernel {
	pub fn new(prepared_data: Arc<ScatterPreparedData>) -> Self {
		Self {
			prepared_data,
			raster_cache: Mutex::new(None),
		}
	}

	fn plot_vector(
		&self,
		backend: &mut dyn PlotBackend,
		transform: &CoordinateTransformer,
		settings: &PlotSettings,
	) {
		for series in &self.prepared_data.series {
			let color = settings.color_theme.get_color(series.color_t);
			backend.fill_path(
				&|builder| {
					series.for_each_point(|x, y| {
						let pixel_p = transform.cartesian(x, y);
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
		settings: &PlotSettings,
	) {
		let series_count = self.prepared_data.series.len().max(1);
		let per_series_target = (settings.scatter_downsample_target.max(1) as usize)
			.div_ceil(series_count)
			.max(1);

		for series in &self.prepared_data.series {
			let sampled = downsample_series(
				series,
				bounds,
				transform,
				per_series_target,
			);
			let color = settings.color_theme.get_color(series.color_t);
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
		settings: &PlotSettings,
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
		settings: &PlotSettings,
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
				.map(|series| color_to_rgba8(settings.color_theme.get_color(series.color_t)))
				.collect(),
		};

		if let Some(existing) = self.raster_cache.lock().unwrap().as_ref()
			.filter(|cache| cache.key == key)
			.cloned() {
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

pub struct ScatterSeries {
	pub name: String,
	pub x_values: Float64Chunked,
	pub y_values: Float64Chunked,
	pub color_t: f32,
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
		PlotLayout::Cartesian { y_range, .. } => (y_range.0.to_bits(), y_range.1.to_bits()),
		_ => (0, 0),
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

fn build_raster_rgba(
	prepared_data: &ScatterPreparedData,
	bounds: Rectangle,
	transform: &CoordinateTransformer,
	settings: &PlotSettings,
) -> Vec<u8> {
	let width = bounds.width.max(1.0).ceil() as usize;
	let height = bounds.height.max(1.0).ceil() as usize;
	let mut accum = vec![[0.0_f32; 4]; width * height];

	for series in &prepared_data.series {
		let color = settings.color_theme.get_color(series.color_t);
		series.for_each_point(|x, y| {
			let point = transform.cartesian(x, y);
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
	settings: &PlotSettings,
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
		let point = transform.cartesian(x, y);
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

pub fn prepare_scatter_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
	point_size_px: f32,
) -> ScatterPreparedData {
	let x_col_series = match df.column(x_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(x_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(x_col.into(), &DataType::Float64)),
	};
	let y_col_series = match df.column(y_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(y_col.into(), &DataType::Float64))
		}),
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
		df.partition_by([cat_col], true).unwrap_or_else(|_| vec![df.clone()])
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
		});
	}
	ScatterPreparedData {
		series: series_list,
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
