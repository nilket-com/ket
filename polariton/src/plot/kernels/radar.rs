use crate::plot::common::{
	format_label, AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel,
	PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct RadarPlotKernel {
	pub prepared_data: Arc<RadarPreparedData>,
}

impl PlotKernel for RadarPlotKernel {
	fn layout(&self, _settings: PlotSettings) -> PlotLayout {
		PlotLayout::Radial
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: PlotSettings,
	) {
		let num_dims = self.prepared_data.dimensions.len();
		if num_dims < 3 {
			return;
		}
		let center = Point::new(bounds.width / 2.0, bounds.height / 2.0);
		let max_radius = (bounds.width.min(bounds.height) / 2.0 - 80.0).max(10.0);
		let grid_stroke = Stroke {
			style: Style::Solid(Color::from_rgba(0.5, 0.5, 0.5, 0.2)),
			width: 1.0,
			..Default::default()
		};
		let num_circles = settings.x_ticks;
		for i in 1..=num_circles {
			let radius = max_radius * (i as f32 / num_circles as f32);
			backend.stroke_path(
				&|builder| {
					for j in 0..=num_dims {
						let angle = (j as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
							- std::f32::consts::FRAC_PI_2;
						let p = Point::new(
							center.x + radius * angle.cos(),
							center.y + radius * angle.sin(),
						);
						if j == 0 {
							builder.move_to(p);
						} else {
							builder.line_to(p);
						}
					}
				},
				grid_stroke,
			);
		}
		let axis_stroke = Stroke {
			style: Style::Solid(Color::from_rgba(1.0, 1.0, 1.0, 0.4)),
			width: 1.5,
			..Default::default()
		};
		for i in 0..num_dims {
			let angle = (i as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
				- std::f32::consts::FRAC_PI_2;
			let p = Point::new(
				center.x + max_radius * angle.cos(),
				center.y + max_radius * angle.sin(),
			);
			backend.stroke_path(
				&|builder| {
					builder.move_to(center);
					builder.line_to(p);
				},
				axis_stroke,
			);
			backend.fill_text(iced::widget::canvas::Text {
				content: self.prepared_data.dimensions[i].clone(),
				position: Point::new(
					center.x + (max_radius + 15.0) * angle.cos(),
					center.y + (max_radius + 15.0) * angle.sin(),
				),
				color: settings.decoration_color,
				size: iced::Pixels(settings.x_label_size),
				align_x: if angle.cos().abs() < 0.01 {
					iced::alignment::Horizontal::Center.into()
				} else if angle.cos() > 0.0 {
					iced::alignment::Horizontal::Left.into()
				} else {
					iced::alignment::Horizontal::Right.into()
				},
				align_y: if angle.sin().abs() < 0.01 {
					iced::alignment::Vertical::Center
				} else if angle.sin() > 0.0 {
					iced::alignment::Vertical::Top
				} else {
					iced::alignment::Vertical::Bottom
				},
				..Default::default()
			});
			if num_circles > 0 {
				let range = self.prepared_data.ranges[i];
				for c in 1..=num_circles {
					let radius = max_radius * (c as f32 / num_circles as f32);
					let val = range.0 + (range.1 - range.0) * (c as f64 / num_circles as f64);
					let label_pos = Point::new(
						center.x + radius * angle.cos() + 10.0,
						center.y + radius * angle.sin() + 2.0,
					);
					backend.fill_text(iced::widget::canvas::Text {
						content: format_label(val, AxisType::Linear),
						position: label_pos,
						color: Color {
							a: 0.7,
							..settings.decoration_color
						},
						size: iced::Pixels(settings.x_tick_size),
						align_x: iced::alignment::Horizontal::Left.into(),
						align_y: iced::alignment::Vertical::Center,
						..Default::default()
					});
				}
			}
		}
		for (i, row_data) in self.prepared_data.data_matrix.iter().enumerate() {
			let cat_idx = self.prepared_data.row_categories[i];
			let t = if self.prepared_data.num_categories > 1 {
				(cat_idx as f32 / (self.prepared_data.num_categories - 1) as f32).clamp(0.1, 0.9)
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let mut fill_color = color;
			fill_color.a = 0.3;
			let path_closure = |builder: &mut dyn PathBuilder| {
				for (d, &val) in row_data.iter().enumerate() {
					let angle = (d as f32 / num_dims as f32) * 2.0 * std::f32::consts::PI
						- std::f32::consts::FRAC_PI_2;
					let range = self.prepared_data.ranges[d];
					let norm_val = ((val - range.0) / (range.1 - range.0).max(1e-6)) as f32;
					let radius = max_radius * (0.15 + norm_val * 0.8);
					let p = Point::new(
						center.x + radius * angle.cos(),
						center.y + radius * angle.sin(),
					);
					if d == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
				builder.close();
			};
			backend.fill_path(&path_closure, fill_color);
			backend.stroke_path(
				&path_closure,
				Stroke {
					style: Style::Solid(color),
					width: 2.0,
					..Default::default()
				},
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
				(i as f32 / (num_cats - 1) as f32).clamp(0.1, 0.9)
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

	fn hover(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<String> {
		None
	}
}

pub struct RadarPreparedData {
	pub dimensions: Vec<String>,
	pub ranges: Vec<(f64, f64)>,
	pub data_matrix: Vec<Vec<f64>>,
	pub row_categories: Vec<usize>,
	pub category_names: Vec<String>,
	pub num_categories: usize,
}

pub fn prepare_radar_data(df: &DataFrame, dims: &[String], cat_col: &str) -> RadarPreparedData {
	if df.height() == 0 || dims.is_empty() {
		return RadarPreparedData {
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
		let diff = (max - min).max(1e-6);
		let padded_min = min - diff * 0.1;
		let padded_max = max + diff * 0.1;
		ranges.push((padded_min, padded_max));
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
	RadarPreparedData {
		dimensions: dims.to_vec(),
		ranges,
		data_matrix,
		row_categories,
		category_names,
		num_categories: num_cats,
	}
}
