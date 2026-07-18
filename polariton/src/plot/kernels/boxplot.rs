use crate::plot::common::{
	format_label, polars_type_to_axis_type, AxisType, CoordinateTransformer,
	PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct BoxPlotKernel {
	pub prepared_data: Arc<BoxPlotPreparedData>,
}

impl PlotKernel for BoxPlotKernel {
	fn layout(&self, settings: PlotSettings) -> PlotLayout {
		PlotLayout::CategoricalX {
			categories: self.prepared_data.categories.clone(),
			y_range: (
				settings.y_min.unwrap_or(self.prepared_data.y_range.0),
				settings.y_max.unwrap_or(self.prepared_data.y_range.1),
			),
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		_bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: PlotSettings,
	) {
		let num_cats = self.prepared_data.categories.len();
		for (i, stats) in self.prepared_data.stats.iter().enumerate() {
			let (center, band_width) = transform.categorical(i, 0.0);
			let box_width = band_width * 0.6;
			let left = center.x - box_width / 2.0;
			let right = center.x + box_width / 2.0;
			let t = if num_cats > 1 {
				i as f32 / (num_cats - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let color_iced = color;
			let line_color = settings.decoration_color;
			let (q1_px, _) = transform.categorical(i, stats.q1);
			let (q3_px, _) = transform.categorical(i, stats.q3);
			let box_rect = Rectangle {
				x: left,
				y: q3_px.y,
				width: box_width,
				height: (q1_px.y - q3_px.y).abs().max(1.0),
			};
			backend.fill_rectangle(box_rect.position(), box_rect.size(), color_iced);
			let outline_stroke = Stroke {
				style: Style::Solid(line_color),
				width: 1.0,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.rectangle(box_rect.position(), box_rect.size());
				},
				outline_stroke,
			);
			let (median_px, _) = transform.categorical(i, stats.median);
			let median_stroke = Stroke {
				style: Style::Solid(line_color),
				width: 3.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(left, median_px.y));
					builder.line_to(iced::Point::new(right, median_px.y));
				},
				median_stroke,
			);
			let whisker_stroke = Stroke {
				style: Style::Solid(line_color),
				width: 1.0,
				..Default::default()
			};
			let (min_px, _) = transform.categorical(i, stats.min);
			let (max_px, _) = transform.categorical(i, stats.max);
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(center.x, min_px.y));
					builder.line_to(iced::Point::new(center.x, q1_px.y));
					builder.move_to(iced::Point::new(center.x, q3_px.y));
					builder.line_to(iced::Point::new(center.x, max_px.y));
				},
				whisker_stroke,
			);
			let cap_width = box_width * 0.4;
			let cap_stroke = Stroke {
				style: Style::Solid(line_color),
				width: 3.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					builder.move_to(iced::Point::new(center.x - cap_width / 2.0, min_px.y));
					builder.line_to(iced::Point::new(center.x + cap_width / 2.0, min_px.y));
					builder.move_to(iced::Point::new(center.x - cap_width / 2.0, max_px.y));
					builder.line_to(iced::Point::new(center.x + cap_width / 2.0, max_px.y));
				},
				cap_stroke,
			);
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let PlotLayout::CategoricalX {
				categories,
				y_range,
			} = transform.layout
		{
			for (i, category) in categories.iter().enumerate() {
				let (center, band_width) = transform.categorical(i, 0.0);
				let left = center.x - band_width / 2.0;
				let right = center.x + band_width / 2.0;
				if cursor_pos.x >= left && cursor_pos.x <= right {
					let stats = &self.prepared_data.stats[i];
					let y_scale = transform.bounds.height as f64 / (y_range.1 - y_range.0);
					let data_y = y_range.0
						+ (transform.bounds.height - cursor_pos.y) as f64 / y_scale;
					if data_y >= stats.min && data_y <= stats.max {
						let yt = self.prepared_data.y_axis_type;
						return Some(format!(
							"{}\nMax: {}\nQ3: {}\nMedian: {}\nQ1: {}\nMin: {}",
							category,
							format_label(stats.max, yt),
							format_label(stats.q3, yt),
							format_label(stats.median, yt),
							format_label(stats.q1, yt),
							format_label(stats.min, yt)
						));
					}
				}
			}
		}
		None
	}

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: PlotSettings,
	) {
		let num_cats = self.prepared_data.categories.len();
		if num_cats == 0 {
			return;
		}
		let max_rows = settings.max_legend_rows.max(1) as usize;
		let num_cols = num_cats.div_ceil(max_rows);
		let actual_rows = num_cats.min(max_rows);
		let item_height = 25.0;
		let legend_padding = 10.0;
		let rect_size = 15.0;
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
		for (i, name) in self.prepared_data.categories.iter().enumerate() {
			let t = if num_cats > 1 {
				i as f32 / (num_cats - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let col = i / max_rows;
			let row = i % max_rows;
			let item_x = x + legend_padding + col as f32 * col_width;
			let item_y = y + legend_padding + row as f32 * item_height;
			backend.fill_rectangle(
				iced::Point::new(item_x, item_y + (item_height - rect_size) / 2.0),
				iced::Size::new(rect_size, rect_size),
				color,
			);
			backend.fill_text(iced::widget::canvas::Text {
				content: name.clone(),
				position: iced::Point::new(item_x + rect_size + 10.0, item_y + item_height / 2.0),
				color: settings.decoration_color,
				size: iced::Pixels(settings.legend_size),
				align_x: iced::alignment::Horizontal::Left.into(),
				align_y: iced::alignment::Vertical::Center,
				..Default::default()
			});
		}
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub struct BoxStats {
	pub min: f64,
	pub q1: f64,
	pub median: f64,
	pub q3: f64,
	pub max: f64,
}

pub struct BoxPlotPreparedData {
	pub categories: Vec<String>,
	pub stats: Vec<BoxStats>,
	pub y_range: (f64, f64),
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_box_plot_data(
	df: &DataFrame,
	cat_col: &str,
	val_col: &str,
) -> BoxPlotPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return BoxPlotPreparedData {
			categories: vec!["No Data".to_string()],
			stats: vec![BoxStats {
				min: 0.0,
				q1: 0.0,
				median: 0.0,
				q3: 0.0,
				max: 0.0,
			}],
			y_range: (0.0, 1.0),
			y_axis_type: AxisType::Linear,
			x_label: cat_col.to_string(),
			y_label: val_col.to_string(),
		};
	}
	let y_dtype = df.column(val_col).map(|c| c.dtype().clone()).unwrap_or(DataType::Float64);
	let y_axis_type = polars_type_to_axis_type(&y_dtype);
	let (categories, categories_series) = if cat_col.is_empty() {
		(vec!["All Data".to_string()], Series::new("dummy".into(), &["All Data"]))
	} else {
		match df.column(cat_col) {
			Ok(c) => {
				let series = c.unique().unwrap_or_else(|_| c.clone()).sort(Default::default()).unwrap_or_else(|_| c.clone());
				let cats = series
					.as_materialized_series()
					.iter()
					.map(|v| {
						if let AnyValue::String(s) = v {
							s.to_string()
						} else {
							v.to_string().replace("\"", "")
						}
					})
					.collect();
				(cats, series.as_materialized_series().clone())
			}
			Err(_) => (vec!["All Data".to_string()], Series::new("dummy".into(), &["All Data"])),
		}
	};
	let num_cats = categories.len();
	let mut stats = Vec::with_capacity(num_cats);
	let mut y_min_all = f64::MAX;
	let mut y_max_all = f64::MIN;
	for i in 0..num_cats {
		let group_df = if cat_col.is_empty() {
			df.clone()
		} else {
			let cat_val = categories_series.get(i).unwrap_or(AnyValue::Null);
			let lit_val = match cat_val {
				AnyValue::String(s) => lit(s),
				AnyValue::Int32(i) => lit(i),
				AnyValue::Int64(i) => lit(i),
				_ => lit(cat_val.to_string()),
			};
			df.clone()
				.lazy()
				.filter(col(cat_col).eq(lit_val))
				.collect()
				.unwrap_or_else(|_| df.head(Some(0)))
		};
		let vals = match group_df.column(val_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(val_col.into(), &DataType::Float64))),
			Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
		};
		let v = vals.f64().unwrap();
		let mut sorted_v: Vec<f64> = v.into_no_null_iter().collect();
		if sorted_v.is_empty() {
			stats.push(BoxStats {
				min: 0.0,
				q1: 0.0,
				median: 0.0,
				q3: 0.0,
				max: 0.0,
			});
			continue;
		}
		sorted_v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
		let n = sorted_v.len();
		let min = sorted_v[0];
		let max = sorted_v[n - 1];
		let q1 = sorted_v[n / 4];
		let median = sorted_v[n / 2];
		let q3 = sorted_v[3 * n / 4];
		stats.push(BoxStats {
			min,
			q1,
			median,
			q3,
			max,
		});
		if min < y_min_all {
			y_min_all = min;
		}
		if max > y_max_all {
			y_max_all = max;
		}
	}
	if y_min_all == f64::MAX {
		y_min_all = 0.0;
		y_max_all = 1.0;
	}
	let pad = (y_max_all - y_min_all).max(0.001) * 0.1;
	let y_range = (y_min_all - pad, y_max_all + pad);
	BoxPlotPreparedData {
		categories,
		stats,
		y_range,
		y_axis_type,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
	}
}
