use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct ParallelPlotKernel {
	pub prepared_data: Arc<ParallelPreparedData>,
}

impl PlotKernel for ParallelPlotKernel {
	fn layout(&self, _settings: PlotSettings) -> PlotLayout {
		PlotLayout::Parallel {
			dimensions: self.prepared_data.dimensions.clone(),
			ranges: self.prepared_data.ranges.clone(),
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
		let num_dims = self.prepared_data.dimensions.len();
		if num_dims < 2 {
			return;
		}
		for (i, row_data) in self.prepared_data.data_matrix.iter().enumerate() {
			let cat_idx = self.prepared_data.row_categories[i];
			let t = if self.prepared_data.num_categories > 1 {
				cat_idx as f32 / (self.prepared_data.num_categories - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let stroke = Stroke {
				style: Style::Solid(color),
				width: 1.5,
				..Default::default()
			};
			backend.stroke_path(
				&|builder| {
					for (d, &val) in row_data.iter().enumerate() {
						let (p, _) = transform.categorical(d, val);
						if d == 0 {
							builder.move_to(p);
						} else {
							builder.line_to(p);
						}
					}
				},
				stroke,
			);
		}
	}

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: PlotSettings,
	) {
		let num_cats = self.prepared_data.category_names.len();
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
		for (i, name) in self.prepared_data.category_names.iter().enumerate() {
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

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let num_dims = self.prepared_data.dimensions.len();
			if num_dims < 2 {
				return None;
			}
			let spacing = transform.bounds.width / (num_dims - 1) as f32;
			let axis_idx =
				((cursor_pos.x - transform.bounds.x + spacing / 2.0) / spacing).floor() as i32;
			if axis_idx >= 0 && (axis_idx as usize) < num_dims {
				let i = axis_idx as usize;
				let range = self.prepared_data.ranges[i];
				let y_scale = (range.1 - range.0) / transform.bounds.height as f64;
				let val = range.1 - (cursor_pos.y - transform.bounds.y) as f64 * y_scale;
				return Some(format!("{}: {:.2}", self.prepared_data.dimensions[i], val));
			}
		}
		None
	}

	fn x_label(&self) -> String {
		"Dimension".to_string()
	}

	fn y_label(&self) -> String {
		"Value".to_string()
	}
}

pub struct ParallelPreparedData {
	pub dimensions: Vec<String>,
	pub ranges: Vec<(f64, f64)>,
	pub data_matrix: Vec<Vec<f64>>,
	pub row_categories: Vec<usize>,
	pub category_names: Vec<String>,
	pub num_categories: usize,
}

pub fn prepare_parallel_data(
	df: &DataFrame,
	dims: &[String],
	cat_col: &str,
) -> ParallelPreparedData {
	if df.height() == 0 || dims.is_empty() {
		return ParallelPreparedData {
			dimensions: dims.to_vec(),
			ranges: vec![(0.0, 1.0); dims.len()],
			data_matrix: vec![],
			row_categories: vec![],
			category_names: vec!["No Data".to_string()],
			num_categories: 1,
		};
	}
	let num_dims = dims.len();
	let mut ranges = Vec::with_capacity(num_dims);
	let mut dim_columns = Vec::with_capacity(num_dims);
	for dim in dims {
		let col = match df.column(dim) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
				Column::from(Series::new_empty(dim.into(), &DataType::Float64))
			}),
			Err(_) => Column::from(Series::new_empty(dim.into(), &DataType::Float64)),
		};
		let series = col.f64().unwrap().clone();
		let min = series.min().unwrap_or(0.0);
		let max = series.max().unwrap_or(1.0);
		ranges.push((min, max));
		dim_columns.push(series);
	}
	let categories_series = if cat_col.is_empty() {
		Series::new("dummy_cat".into(), &["All Data"])
	} else {
		match df.column(cat_col) {
			Ok(c) => c
				.unique()
				.unwrap_or_else(|_| c.clone())
				.sort(Default::default())
				.unwrap_or_else(|_| c.clone())
				.as_materialized_series()
				.clone(),
			Err(_) => Series::new("dummy_cat".into(), &["All Data"]),
		}
	};
	let num_cats = categories_series.len();
	let mut category_names = Vec::with_capacity(num_cats);
	let mut cat_to_idx = std::collections::HashMap::new();
	for (i, v) in categories_series.iter().enumerate() {
		let s = if let AnyValue::String(s) = v {
			s.to_string()
		} else {
			v.to_string().replace("\"", "")
		};
		category_names.push(s.clone());
		cat_to_idx.insert(s, i);
	}
	let cat_vals = if cat_col.is_empty() {
		Series::new("dummy_cat".into(), vec!["All Data"; df.height()])
	} else {
		match df.column(cat_col) {
			Ok(c) => c.as_materialized_series().clone(),
			Err(_) => Series::new("dummy_cat".into(), vec!["All Data"; df.height()]),
		}
	};
	let num_rows = df.height();
	let mut data_matrix = Vec::with_capacity(num_rows);
	let mut row_categories = Vec::with_capacity(num_rows);
	for i in 0..num_rows {
		let mut row = Vec::with_capacity(num_dims);
		for dim_col in dim_columns.iter() {
			row.push(dim_col.get(i).unwrap_or(0.0));
		}
		data_matrix.push(row);
		let cat_val_raw = cat_vals.get(i).unwrap_or(AnyValue::Null);
		let cat_val_str = if let AnyValue::String(s) = cat_val_raw {
			s.to_string()
		} else {
			cat_val_raw.to_string().replace("\"", "")
		};
		row_categories.push(*cat_to_idx.get(&cat_val_str).unwrap_or(&0));
	}
	ParallelPreparedData {
		dimensions: dims.to_vec(),
		ranges,
		data_matrix,
		row_categories,
		category_names,
		num_categories: num_cats,
	}
}
