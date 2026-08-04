use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label, polars_type_to_axis_type,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Point, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct LinePlotKernel {
	pub prepared_data: Arc<LinePreparedData>,
}

pub struct LineMarkRenderer {
	pub prepared_data: Arc<LinePreparedData>,
}

impl LineMarkRenderer {
	pub fn new(prepared_data: Arc<LinePreparedData>) -> Self {
		Self { prepared_data }
	}
}

impl MarkRenderer for LineMarkRenderer {
	fn draw(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		settings: ResolvedPlotSettings,
	) {
		for series in &self.prepared_data.series {
			let color = settings.color_theme.get_color(series.color_t);
			let stroke = Stroke {
				style: Style::Solid(color),
				width: 2.0,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					for (i, p) in series.points.iter().enumerate() {
						let pixel_p = transform.cartesian(p[0], p[1]);
						if i == 0 {
							builder.move_to(pixel_p);
						} else {
							builder.line_to(pixel_p);
						}
					}
				},
				stroke,
			);
		}
	}

	fn hit_test(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<MarkHit> {
		const HIT_RADIUS_PX: f32 = 6.0;
		let cursor = cursor.position()?;
		let mut nearest = None;
		let mut nearest_distance = HIT_RADIUS_PX * HIT_RADIUS_PX;
		let mut mark_index = 0;

		for series in &self.prepared_data.series {
			if series.points.len() == 1 {
				let point = series.points[0];
				let pixel = transform.cartesian(point[0], point[1]);
				let distance = squared_distance(cursor, pixel);
				if distance <= nearest_distance {
					nearest_distance = distance;
					nearest = Some(MarkHit {
						data_x: point[0],
						data_y: point[1],
						mark_index,
					});
				}
			}

			for points in series.points.windows(2) {
				let start = transform.cartesian(points[0][0], points[0][1]);
				let end = transform.cartesian(points[1][0], points[1][1]);
				let (distance, t) = squared_distance_to_segment(cursor, start, end);
				if distance <= nearest_distance {
					nearest_distance = distance;
					nearest = Some(MarkHit {
						data_x: points[0][0] + (points[1][0] - points[0][0]) * t as f64,
						data_y: points[0][1] + (points[1][1] - points[0][1]) * t as f64,
						mark_index,
					});
				}
				mark_index += 1;
			}
			mark_index += usize::from(series.points.len() == 1);
		}
		nearest
	}
}

#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
fn squared_distance(first: Point, second: Point) -> f32 {
	let dx = first.x - second.x;
	let dy = first.y - second.y;
	dx * dx + dy * dy
}

#[allow(dead_code)] // Geometric hit-testing is test-reachable until PRODUCT-0014.
fn squared_distance_to_segment(point: Point, start: Point, end: Point) -> (f32, f32) {
	let dx = end.x - start.x;
	let dy = end.y - start.y;
	let length_squared = dx * dx + dy * dy;
	if length_squared <= f32::EPSILON {
		return (squared_distance(point, start), 0.0);
	}
	let t =
		(((point.x - start.x) * dx + (point.y - start.y) * dy) / length_squared).clamp(0.0, 1.0);
	let projection = Point::new(start.x + t * dx, start.y + t * dy);
	(squared_distance(point, projection), t)
}

impl PlotKernel for LinePlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
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
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		LineMarkRenderer::new(Arc::clone(&self.prepared_data))
			.draw(backend, _bounds, transform, settings);
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

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: ResolvedPlotSettings,
	) {
		draw_line_legend(&self.prepared_data, backend, bounds, settings);
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub(crate) fn draw_line_legend(
	prepared_data: &LinePreparedData,
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
	let line_width = 20.0;
	let col_width = 150.0;
	let legend_width = num_cols as f32 * col_width + legend_padding * 2.0;
	let legend_height = actual_rows as f32 * item_height + legend_padding * 2.0;
	let x = (bounds.width - legend_width) * settings.legend_x;
	let y = (bounds.height - legend_height) * settings.legend_y;
	backend.fill_rectangle(
		iced::Point::new(x, y),
		iced::Size::new(legend_width, legend_height),
		iced::Color {
			a: 0.6,
			..settings.background_color
		},
	);
	for (i, series) in prepared_data.series.iter().enumerate() {
		let color = settings.color_theme.get_color(series.color_t);
		let col = i / max_rows;
		let row = i % max_rows;
		let item_x = x + legend_padding + col as f32 * col_width;
		let item_y = y + legend_padding + row as f32 * item_height;
		let stroke = Stroke {
			style: Style::Solid(color),
			width: 3.0,
			..Default::default()
		};
		backend.stroke_path(
			&|builder| {
				builder.move_to(iced::Point::new(item_x, item_y + item_height / 2.0));
				builder.line_to(iced::Point::new(
					item_x + line_width,
					item_y + item_height / 2.0,
				));
			},
			stroke,
		);
		backend.fill_text(iced::widget::canvas::Text {
			content: series.name.clone(),
			position: iced::Point::new(item_x + line_width + 10.0, item_y + item_height / 2.0),
			color: settings.decoration_color,
			size: iced::Pixels(settings.legend_size),
			align_x: iced::alignment::Horizontal::Left.into(),
			align_y: iced::alignment::Vertical::Center,
			..Default::default()
		});
	}
}

#[allow(dead_code)]
pub struct SeriesData {
	pub name: String,
	pub points: Vec<[f64; 2]>,
	pub color_t: f32,
}

pub struct LinePreparedData {
	pub series: Vec<SeriesData>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_line_data(
	df: &DataFrame,
	cat_col: &str,
	x_col: &str,
	y_col: &str,
) -> LinePreparedData {
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
	let y_series_min = y_series.min().unwrap_or(0.0);
	let y_series_max = y_series.max().unwrap_or(1.0);
	let y_range = (y_series_min, y_series_max);
	let x_pad = (x_range.1 - x_range.0) * 0.05;
	let y_pad = (y_range.1 - y_range.0) * 0.05;
	let x_range = (x_range.0 - x_pad, x_range.1 + x_pad);
	let y_range = (y_range.0 - y_pad, y_range.1 + y_pad);
	if df.height() == 0 || x_col.is_empty() || y_col.is_empty() {
		return LinePreparedData {
			series: vec![],
			x_range,
			y_range,
			x_axis_type,
			y_axis_type,
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
		};
	}
	let mut series_list = Vec::new();
	let partitions = if cat_col.is_empty() {
		vec![df.clone()]
	} else {
		df.partition_by([cat_col], true)
			.unwrap_or_else(|_| vec![df.clone()])
	};
	let num_partitions = partitions.len();
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
				cat_val.to_string().replace("\"", "")
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
		let xs = xs_col.f64().unwrap();
		let ys = ys_col.f64().unwrap();
		let t = if num_partitions > 1 {
			i as f32 / (num_partitions - 1) as f32
		} else {
			0.5
		};
		let mut points = Vec::with_capacity(group_df.height());
		for j in 0..group_df.height() {
			points.push([xs.get(j).unwrap(), ys.get(j).unwrap()]);
		}
		series_list.push(SeriesData {
			name: cat_name,
			points,
			color_t: t,
		});
	}
	LinePreparedData {
		series: series_list,
		x_range,
		y_range,
		x_axis_type,
		y_axis_type,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
	}
}
