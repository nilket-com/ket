use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct PiePlotKernel {
	pub prepared_data: Arc<PiePreparedData>,
}

impl PlotKernel for PiePlotKernel {
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
		let center = bounds.center();
		let radius = bounds.width.min(bounds.height) / 2.0 * 0.8;
		let num_categories = self.prepared_data.categories.len();
		for i in 0..num_categories {
			let start_angle = self.prepared_data.cumulative_angles[i] as f32;
			let end_angle = self.prepared_data.cumulative_angles[i + 1] as f32;
			if start_angle == end_angle {
				continue;
			}
			let t = if num_categories > 1 {
				i as f32 / (num_categories - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let path_closure = &|builder: &mut dyn crate::plot::common::PathBuilder| {
				builder.move_to(center);
				builder.arc_to(
					center,
					radius,
					start_angle.to_radians(),
					end_angle.to_radians(),
				);
				builder.line_to(center);
				builder.close();
			};
			backend.fill_path(path_closure, color);
			backend.stroke_path(
				path_closure,
				Stroke {
					style: Style::Solid(settings.background_color),
					width: 1.0,
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
		let num_categories = self.prepared_data.categories.len();
		if num_categories == 0 {
			return;
		}
		let max_rows = settings.max_legend_rows.max(1) as usize;
		let num_cols = num_categories.div_ceil(max_rows);
		let actual_rows = num_categories.min(max_rows);
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
		for i in 0..num_categories {
			let name = &self.prepared_data.categories[i];
			let t = if num_categories > 1 {
				i as f32 / (num_categories - 1) as f32
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
			let center = transform.bounds.center();
			let dx = cursor_pos.x - center.x;
			let dy = cursor_pos.y - center.y;
			let dist = (dx * dx + dy * dy).sqrt();
			let radius = transform.bounds.width.min(transform.bounds.height) * 0.45;
			let inner_radius = radius * 0.05;
			if dist >= inner_radius && dist <= radius {
				let pi = std::f32::consts::PI;
				let angle = dy.atan2(dx);
				let mut normalized_angle = angle - (-pi / 2.0);
				while normalized_angle < 0.0 {
					normalized_angle += 2.0 * pi;
				}
				while normalized_angle >= 2.0 * pi {
					normalized_angle -= 2.0 * pi;
				}
				let angle_ratio = normalized_angle / (2.0 * pi);
				for (i, &limit) in self.prepared_data.cumulative_angles.iter().enumerate() {
					if angle_ratio < limit as f32 {
						let cat = &self.prepared_data.categories[i];
						let val = self.prepared_data.values[i];
						return Some(format!(
							"{}: {:.2} ({:.1}%)",
							cat,
							val,
							val / self.prepared_data.total_sum * 100.0
						));
					}
				}
			}
		}
		None
	}
}

pub struct PiePreparedData {
	pub categories: Vec<String>,
	pub values: Vec<f64>,
	pub cumulative_angles: Vec<f64>,
	pub total_sum: f64,
}

pub fn prepare_pie_data(df: &DataFrame, cat_col: &str, val_col: &str) -> PiePreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return PiePreparedData {
			categories: vec!["No Data".to_string()],
			values: vec![1.0],
			cumulative_angles: vec![0.0, 360.0],
			total_sum: 1.0,
		};
	}
	let (categories, categories_series) = if cat_col.is_empty() {
		let cats: Vec<String> = (0..df.height()).map(|i| format!("Row {}", i + 1)).collect();
		(cats, Series::new_empty("dummy_cat".into(), &DataType::String))
	} else {
		match df.column(cat_col) {
			Ok(c) => {
				let series = c
					.unique()
					.unwrap_or_else(|_| c.clone())
					.sort(Default::default())
					.unwrap_or_else(|_| c.clone());
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
			Err(_) => {
				let cats: Vec<String> = (0..df.height()).map(|i| format!("Row {}", i + 1)).collect();
				(cats, Series::new_empty("dummy_cat".into(), &DataType::String))
			}
		}
	};
	let mut values = Vec::with_capacity(categories.len());
	let mut total_sum = 0.0f64;
	if cat_col.is_empty() || df.column(cat_col).is_err() {
		if let Ok(c) = df.column(val_col)
			&& let Ok(c_f64) = c.cast(&DataType::Float64) {
			let series = c_f64.f64().unwrap();
			for i in 0..df.height() {
				let val = series.get(i).unwrap_or(0.0);
				values.push(val);
				total_sum += val;
			}
		}
	} else {
		for i in 0..categories.len() {
			let cat_val = categories_series.get(i).unwrap_or(AnyValue::Null);
			let lit_val = match cat_val {
				AnyValue::String(s) => lit(s),
				AnyValue::Int32(i) => lit(i),
				AnyValue::Int64(i) => lit(i),
				_ => lit(cat_val.to_string()),
			};
			let filtered = df.clone()
				.lazy()
				.filter(col(cat_col).eq(lit_val))
				.select([col(val_col).sum()])
				.collect();

			let val = filtered
				.map(|f| {
					f.column(val_col)
						.and_then(|c| c.cast(&DataType::Float64))
						.map(|c| c.f64().unwrap().get(0).unwrap_or(0.0))
						.unwrap_or(0.0)
				})
				.unwrap_or(0.0);
			values.push(val);
			total_sum += val;
		}
	}
	let mut cumulative_angles = Vec::with_capacity(values.len() + 1);
	let mut current_angle = 0.0f64;
	cumulative_angles.push(0.0);
	if total_sum > 0.0 {
		for v in &values {
			current_angle += (v / total_sum) * 360.0;
			cumulative_angles.push(current_angle);
		}
	} else {
		cumulative_angles.push(360.0);
	}
	PiePreparedData {
		categories,
		values,
		cumulative_angles,
		total_sum,
	}
}
