use crate::plot::common::{
	CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::spec::{BandFillAssignment, LayerSettings};
use crate::plot::theme::PlotSettings;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::Text;
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BarPlotKernel {
	pub prepared_data: Arc<BarPreparedData>,
	pub orientation: Orientation,
	pub layer_settings: LayerSettings,
	pub band_fills: Vec<BandFillAssignment>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BarRectangle {
	rect: Rectangle,
	category_index: usize,
	group_index: usize,
}

fn category_label(value: AnyValue<'_>) -> String {
	if let AnyValue::String(value) = value {
		value.to_string()
	} else {
		value.to_string().replace('"', "")
	}
}

impl BarPlotKernel {
	fn bar_rectangles(
		&self,
		transform: &CoordinateTransformer,
		width_fraction: f32,
	) -> Vec<BarRectangle> {
		let num_cats = self.prepared_data.categories.len();
		let num_groups = self.prepared_data.group_names.len();
		let mut bars = Vec::with_capacity(num_cats.saturating_mul(num_groups));
		match self.orientation {
			Orientation::Vertical => {
				let y_base = if let PlotLayout::CategoricalX { y_range, .. } = transform.layout {
					y_range.0
				} else {
					0.0
				};
				let band = transform.bounds.width / num_cats as f32;
				let slot = band / num_groups as f32;
				let padding = slot * (1.0 - width_fraction) / 2.0;
				for category_index in 0..num_cats {
					let category_left = category_index as f32 * band;
					for group_index in 0..num_groups {
						let value = self.prepared_data.values[category_index][group_index];
						if value <= y_base {
							continue;
						}
						let (top, _) = transform.categorical(category_index, value);
						let (bottom, _) = transform.categorical(category_index, y_base);
						bars.push(BarRectangle {
							rect: Rectangle {
								x: category_left + group_index as f32 * slot + padding,
								y: top.y,
								width: slot * width_fraction,
								height: (bottom.y - top.y).max(1.0),
							},
							category_index,
							group_index,
						});
					}
				}
			}
			Orientation::Horizontal => {
				let x_base = if let PlotLayout::CategoricalY { x_range, .. } = transform.layout {
					x_range.0
				} else {
					0.0
				};
				let band = transform.bounds.height / num_cats as f32;
				let slot = band / num_groups as f32;
				let padding = slot * (1.0 - width_fraction) / 2.0;
				for category_index in 0..num_cats {
					let category_top = (num_cats - 1 - category_index) as f32 * band;
					for group_index in 0..num_groups {
						let value = self.prepared_data.values[category_index][group_index];
						if value <= x_base {
							continue;
						}
						let (right, _) = transform.categorical(category_index, value);
						let (left, _) = transform.categorical(category_index, x_base);
						let reversed_group = num_groups - 1 - group_index;
						bars.push(BarRectangle {
							rect: Rectangle {
								x: left.x,
								y: category_top + reversed_group as f32 * slot + padding,
								width: (right.x - left.x).max(1.0),
								height: slot * width_fraction,
							},
							category_index,
							group_index,
						});
					}
				}
			}
		}
		bars
	}

	fn hover_resolved(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &ResolvedPlotSettings,
	) -> Option<String> {
		let cursor_pos = cursor.position()?;
		let hit = self
			.bar_rectangles(transform, settings.bar_width.fraction())
			.into_iter()
			.find(|bar| bar.rect.contains(cursor_pos))?;
		let category = &self.prepared_data.categories[hit.category_index];
		let group = &self.prepared_data.group_names[hit.group_index];
		let approximate_value = match transform.layout {
			PlotLayout::CategoricalX { y_range, .. } => {
				let scale = transform.bounds.height as f64 / (y_range.1 - y_range.0);
				y_range.0 + (transform.bounds.height - cursor_pos.y) as f64 / scale
			}
			PlotLayout::CategoricalY { x_range, .. } => {
				let scale = transform.bounds.width as f64 / (x_range.1 - x_range.0);
				x_range.0 + cursor_pos.x as f64 / scale
			}
			_ => return None,
		};
		Some(format!(
			"{}: {} (Value: ~{:.2})",
			category, group, approximate_value
		))
	}
}

impl PlotKernel for BarPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		match self.orientation {
			Orientation::Vertical => PlotLayout::CategoricalX {
				categories: self.prepared_data.categories.clone(),
				y_range: (
					settings.y_min.unwrap_or(self.prepared_data.y_range.0),
					settings.y_max.unwrap_or(self.prepared_data.y_range.1),
				),
			},
			Orientation::Horizontal => PlotLayout::CategoricalY {
				categories: self.prepared_data.categories.clone(),
				x_range: (
					settings.x_min.unwrap_or(self.prepared_data.y_range.0),
					settings.x_max.unwrap_or(self.prepared_data.y_range.1),
				),
			},
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
		let num_groups = self.prepared_data.group_names.len();
		for bar in self.bar_rectangles(transform, settings.bar_width.fraction()) {
			let category = &self.prepared_data.categories[bar.category_index];
			let color = if let Some(assignment) = self
				.band_fills
				.iter()
				.find(|assignment| assignment.category == *category)
			{
				Color::from_rgba8(
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha as f32 / u8::MAX as f32,
				)
			} else {
				let t = if num_groups > 1 {
					bar.group_index as f32 / (num_groups - 1) as f32
				} else {
					0.5
				};
				settings.color_theme.get_color(t)
			};
			backend.fill_rectangle(bar.rect.position(), bar.rect.size(), color);
		}
	}

	fn plot_with_settings(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) {
		self.plot(
			backend,
			bounds,
			transform,
			cursor,
			settings.resolve_for_layer(&self.layer_settings),
		);
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		self.hover_resolved(transform, cursor, &ResolvedPlotSettings::default())
	}

	fn hover_with_settings(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
		settings: &PlotSettings,
	) -> Option<String> {
		self.hover_resolved(
			transform,
			cursor,
			&settings.resolve_for_layer(&self.layer_settings),
		)
	}

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: ResolvedPlotSettings,
	) {
		let num_groups = self.prepared_data.group_names.len();
		if num_groups == 0 {
			return;
		}
		let max_rows = settings.max_legend_rows.max(1) as usize;
		let num_cols = num_groups.div_ceil(max_rows);
		let actual_rows = num_groups.min(max_rows);
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
		for (i, name) in self.prepared_data.group_names.iter().enumerate() {
			let t = if num_groups > 1 {
				i as f32 / (num_groups - 1) as f32
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
			backend.fill_text(Text {
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
		match self.orientation {
			Orientation::Vertical => self.prepared_data.x_label.clone(),
			Orientation::Horizontal => self.prepared_data.y_label.clone(),
		}
	}

	fn y_label(&self) -> String {
		match self.orientation {
			Orientation::Vertical => self.prepared_data.y_label.clone(),
			Orientation::Horizontal => self.prepared_data.x_label.clone(),
		}
	}
}

pub struct BarPreparedData {
	pub categories: Vec<String>,
	pub group_names: Vec<String>,
	pub values: Vec<Vec<f64>>,
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_bar_data(
	df: &DataFrame,
	cat_col: &str,
	group_col: &str,
	val_col: &str,
) -> BarPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return BarPreparedData {
			categories: vec!["No Data".to_string()],
			group_names: vec!["Value".to_string()],
			values: vec![vec![0.0]],
			y_range: (0.0, 1.0),
			x_label: cat_col.to_string(),
			y_label: val_col.to_string(),
		};
	}
	let (categories, _categories_series) = if cat_col.is_empty() {
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
	let (group_names, _groups_series) = if group_col.is_empty() {
		(
			vec!["Value".to_string()],
			Series::new("dummy_group".into(), &["Value"]),
		)
	} else {
		match df.column(group_col) {
			Ok(c) => {
				let series = c
					.unique()
					.unwrap_or_else(|_| c.clone())
					.sort(Default::default())
					.unwrap_or_else(|_| c.clone());
				let groups = series
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
				(groups, series.as_materialized_series().clone())
			}
			Err(_) => (
				vec!["Value".to_string()],
				Series::new("dummy_group".into(), &["Value"]),
			),
		}
	};
	let num_cats = categories.len();
	let num_groups = group_names.len();
	let vals_col_series = match df.column(val_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
	};
	let vals_f64 = vals_col_series.f64().unwrap();
	let y_max = vals_f64.max().unwrap_or(1.0);
	let y_min = 0.0f64;
	let y_range = (y_min, y_max * 1.1);
	let mut values = vec![vec![0.0f64; num_groups]; num_cats];
	let mut assigned = vec![vec![false; num_groups]; num_cats];
	let category_indices: HashMap<&str, usize> = categories
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let group_indices: HashMap<&str, usize> = group_names
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let category_column = (!cat_col.is_empty())
		.then(|| df.column(cat_col).ok())
		.flatten();
	let group_column = (!group_col.is_empty())
		.then(|| df.column(group_col).ok())
		.flatten();
	for row in 0..df.height() {
		let category_idx = if cat_col.is_empty() {
			Some(0)
		} else {
			category_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| category_indices.get(value.as_str()).copied())
		};
		let group_idx = if group_col.is_empty() {
			Some(0)
		} else {
			group_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| group_indices.get(value.as_str()).copied())
		};
		let (Some(category_idx), Some(group_idx)) = (category_idx, group_idx) else {
			continue;
		};
		if !assigned[category_idx][group_idx] {
			values[category_idx][group_idx] = vals_f64.get(row).unwrap_or(0.0);
			assigned[category_idx][group_idx] = true;
		}
	}
	BarPreparedData {
		categories,
		group_names,
		values,
		y_range,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn values_follow_category_identity_after_labels_are_sorted() {
		let frame = polars::df!(
			"cat" => ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Cat6"],
			"val" => [1.1_f64, 2.2, 3.3, 4.4, 5.5, 6.6],
		)
		.expect("bar fixture");

		let prepared = prepare_bar_data(&frame, "cat", "", "val");

		assert_eq!(
			prepared.categories,
			["Cat1", "Cat2", "Cat3", "Cat4", "Cat5", "Cat6"]
		);
		assert_eq!(
			prepared.values,
			vec![
				vec![1.1],
				vec![2.2],
				vec![3.3],
				vec![4.4],
				vec![5.5],
				vec![6.6]
			]
		);
	}

	#[test]
	fn values_follow_both_category_and_group_identity() {
		let frame = polars::df!(
			"cat" => ["B", "A", "B", "A", "A"],
			"group" => ["right", "left", "left", "right", "left"],
			"val" => [21_i64, 10, 20, 11, 999],
		)
		.expect("grouped bar fixture");

		let prepared = prepare_bar_data(&frame, "cat", "group", "val");

		assert_eq!(prepared.categories, ["A", "B"]);
		assert_eq!(prepared.group_names, ["left", "right"]);
		assert_eq!(prepared.values, vec![vec![10.0, 11.0], vec![20.0, 21.0]]);
	}
}
