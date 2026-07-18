use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct RadialDialPlotKernel {
	pub prepared_data: Arc<RadialDialPreparedData>,
}

impl PlotKernel for RadialDialPlotKernel {
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
		let num_cats = self.prepared_data.categories.len();
		if num_cats == 0 {
			return;
		}
		let center = Point::new(bounds.width / 2.0, bounds.height * 0.8);
		let max_radius = (bounds.width / 2.0).min(bounds.height * 0.7) * 1.0;
		let ring_spacing = 5.0;
		let total_spacing = (num_cats as f32 - 1.0) * ring_spacing;
		let ring_thickness = (max_radius - total_spacing) / num_cats as f32;
		let pi = std::f32::consts::PI;
		let start_angle = pi;
		for i in 0..num_cats {
			let val = self.prepared_data.values[i];
			let max_val = self.prepared_data.max_values[i].max(1e-6);
			let ratio = (val / max_val) as f32;
			let total_sweep = pi;
			let sweep = ratio * total_sweep;
			let t = if num_cats > 1 {
				i as f32 / (num_cats - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let outer_r = max_radius - i as f32 * (ring_thickness + ring_spacing);
			let inner_r = outer_r - ring_thickness;
			backend.fill_path(
				&|builder| {
					builder.arc_to(center, outer_r, start_angle, start_angle + total_sweep);
					builder.arc_to(center, inner_r, start_angle + total_sweep, start_angle);
					builder.close();
				},
				Color::from_rgba(0.5, 0.5, 0.5, 0.1),
			);
			if sweep > 0.001 {
				backend.fill_path(
					&|builder| {
						builder.arc_to(center, outer_r, start_angle, start_angle + sweep);
						builder.arc_to(center, inner_r, start_angle + sweep, start_angle);
						builder.close();
					},
					color,
				);
			}
		}
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

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let bounds = transform.bounds;
			let center = Point::new(
				bounds.x + bounds.width / 2.0,
				bounds.y + bounds.height * 0.8,
			);
			let dx = cursor_pos.x - center.x;
			let dy = cursor_pos.y - center.y;
			let dist = (dx * dx + dy * dy).sqrt();
			let num_cats = self.prepared_data.categories.len();
			let max_radius = (bounds.width / 2.0).min(bounds.height * 0.7) * 0.9;
			let ring_spacing = 5.0;
			let ring_thickness =
				(max_radius - (num_cats as f32 - 1.0) * ring_spacing) / num_cats as f32;
			if dy > 0.0 {
				return None;
			}
			for i in 0..num_cats {
				let outer_r = max_radius - i as f32 * (ring_thickness + ring_spacing);
				let inner_r = outer_r - ring_thickness;
				if dist >= inner_r && dist <= outer_r {
					let cat = &self.prepared_data.categories[i];
					let val = self.prepared_data.values[i];
					let max_val = self.prepared_data.max_values[i];
					return Some(format!(
						"{}: {:.1} / {:.1} ({:.0}%)",
						cat,
						val,
						max_val,
						(val / max_val.max(1e-6) * 100.0)
					));
				}
			}
		}
		None
	}
}

pub struct RadialDialPreparedData {
	pub categories: Vec<String>,
	pub values: Vec<f64>,
	pub max_values: Vec<f64>,
}

pub fn prepare_radial_dial_data(
	df: &DataFrame,
	cat_col: &str,
	val_col: &str,
	max_val_col: &str,
) -> RadialDialPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return RadialDialPreparedData {
			categories: vec!["No Data".to_string()],
			values: vec![0.0],
			max_values: vec![1.0],
		};
	}
	let (categories, categories_series) = if cat_col.is_empty() {
		(
			vec!["All Data".to_string()],
			Series::new("dummy_cat".into(), &["All Data"]),
		)
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
			Err(_) => (
				vec!["All Data".to_string()],
				Series::new("dummy_cat".into(), &["All Data"]),
			),
		}
	};
	let mut values = Vec::with_capacity(categories.len());
	let mut max_values = Vec::with_capacity(categories.len());
	for i in 0..categories.len() {
		let filtered_res = if cat_col.is_empty() {
			Ok(df.clone())
		} else {
			let cat_val = categories_series.get(i).unwrap_or(AnyValue::Null);
			let lit_val = match cat_val {
				AnyValue::String(s) => lit(s),
				AnyValue::Int32(i) => lit(i),
				AnyValue::Int64(i) => lit(i),
				_ => lit(cat_val.to_string()),
			};
			df.clone().lazy().filter(col(cat_col).eq(lit_val)).collect()
		};
		let filtered = filtered_res.unwrap_or_else(|_| df.clone());
		let val = filtered
			.column(val_col)
			.ok()
			.and_then(|c| c.cast(&DataType::Float64).ok())
			.and_then(|c| c.f64().ok().map(|s| s.get(0).unwrap_or(0.0)))
			.unwrap_or(0.0);
		let max_v = if max_val_col.is_empty() {
			1.0
		} else {
			filtered
				.column(max_val_col)
				.ok()
				.and_then(|c| c.cast(&DataType::Float64).ok())
				.and_then(|c| c.f64().ok().map(|s| s.get(0).unwrap_or(1.0)))
				.unwrap_or(1.0)
		};
		values.push(val);
		max_values.push(max_v);
	}
	RadialDialPreparedData {
		categories,
		values,
		max_values,
	}
}
