use crate::plot::common::{
	AxisType, CoordinateTransformer, MarkHit, MarkRenderer, PlotBackend, PlotKernel, PlotLayout,
	ResolvedPlotSettings, format_label, polars_type_to_axis_type,
};
use crate::plot::legend::LegendProvenance;
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
				width: settings.stroke_width.width_px(),
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

	fn hit_test(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<MarkHit> {
		const INTERACTION_TOLERANCE_PX: f32 = 5.0;
		let hit_radius = settings.stroke_width.width_px() / 2.0 + INTERACTION_TOLERANCE_PX;
		let cursor = cursor.position()?;
		let mut nearest = None;
		let mut nearest_distance = hit_radius * hit_radius;
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

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_labels_with_sample_and_provenance(
			self.prepared_data
				.series
				.iter()
				.map(|series| series.name.clone()),
			settings,
			crate::plot::legend::LegendSample::LineSample,
			self.prepared_data
				.series
				.first()
				.map_or(LegendProvenance::Named, |series| series.provenance),
		)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

#[allow(dead_code)]
pub struct SeriesData {
	pub name: String,
	pub points: Vec<[f64; 2]>,
	pub color_t: f32,
	pub provenance: LegendProvenance,
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
			provenance: if cat_col.is_empty() {
				LegendProvenance::Synthetic
			} else {
				LegendProvenance::Named
			},
		});
	}
	if !cat_col.is_empty() {
		let first_appearance = df
			.column(cat_col)
			.ok()
			.into_iter()
			.flat_map(|column| (0..df.height()).filter_map(move |index| column.get(index).ok()))
			.map(|value| {
				if let AnyValue::String(value) = value {
					value.to_owned()
				} else if value.is_null() {
					"Null".to_owned()
				} else {
					value.to_string().replace('"', "")
				}
			})
			.fold(Vec::new(), |mut seen, label| {
				if !seen.contains(&label) {
					seen.push(label);
				}
				seen
			});
		series_list.sort_by_key(|series| {
			first_appearance
				.iter()
				.position(|label| label == &series.name)
				.unwrap_or(usize::MAX)
		});
		let total = series_list.len();
		for (index, series) in series_list.iter_mut().enumerate() {
			series.color_t = if total > 1 {
				index as f32 / (total - 1) as f32
			} else {
				0.5
			};
		}
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
