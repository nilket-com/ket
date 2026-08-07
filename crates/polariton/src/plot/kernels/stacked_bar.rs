use crate::plot::common::{
	CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct StackedBarPlotKernel {
	pub prepared_data: Arc<StackedBarPreparedData>,
	pub orientation: Orientation,
}

fn category_label(value: AnyValue<'_>) -> String {
	if let AnyValue::String(value) = value {
		value.to_string()
	} else {
		value.to_string().replace('"', "")
	}
}

impl PlotKernel for StackedBarPlotKernel {
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
		let num_cats = self.prepared_data.categories.len();
		let num_groups = self.prepared_data.group_names.len();
		match self.orientation {
			Orientation::Vertical => {
				let y_base = if let PlotLayout::CategoricalX { y_range, .. } = transform.layout {
					y_range.0
				} else {
					0.0
				};
				let total_band_width = transform.bounds.width / num_cats as f32;
				let bar_width = total_band_width * 0.8;
				let bar_offset = (total_band_width - bar_width) / 2.0;
				for i in 0..num_cats {
					let cat_left = (i as f32 * total_band_width) + bar_offset;
					let mut current_y = y_base;
					for j in 0..num_groups {
						let val = self.prepared_data.category_values[i][j];
						if val <= 0.0 {
							continue;
						}
						let (p_top, _) = transform.categorical(i, current_y + val);
						let (p_bottom, _) = transform.categorical(i, current_y);
						let bar_rect = Rectangle {
							x: cat_left,
							y: p_top.y,
							width: bar_width,
							height: (p_bottom.y - p_top.y).max(1.0),
						};
						let t = if num_groups > 1 {
							j as f32 / (num_groups - 1) as f32
						} else {
							0.5
						};
						let color = settings.color_theme.get_color(t);
						backend.fill_rectangle(bar_rect.position(), bar_rect.size(), color);
						current_y += val;
					}
				}
			}
			Orientation::Horizontal => {
				let x_base = if let PlotLayout::CategoricalY { x_range, .. } = transform.layout {
					x_range.0
				} else {
					0.0
				};
				let total_band_height = transform.bounds.height / num_cats as f32;
				let bar_height = total_band_height * 0.8;
				let bar_offset = (total_band_height - bar_height) / 2.0;
				for i in 0..num_cats {
					let cat_top = (num_cats - 1 - i) as f32 * total_band_height + bar_offset;
					let mut current_x = x_base;
					for j in 0..num_groups {
						let val = self.prepared_data.category_values[i][j];
						if val <= 0.0 {
							continue;
						}
						let (p_right, _) = transform.categorical(i, current_x + val);
						let (p_left, _) = transform.categorical(i, current_x);
						let bar_rect = Rectangle {
							x: p_left.x,
							y: cat_top,
							width: (p_right.x - p_left.x).max(1.0),
							height: bar_height,
						};
						let t = if num_groups > 1 {
							j as f32 / (num_groups - 1) as f32
						} else {
							0.5
						};
						let color = settings.color_theme.get_color(t);
						backend.fill_rectangle(bar_rect.position(), bar_rect.size(), color);
						current_x += val;
					}
				}
			}
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			match self.orientation {
				Orientation::Vertical => {
					if let PlotLayout::CategoricalX {
						categories,
						y_range,
					} = transform.layout
					{
						for (i, cat_name) in categories.iter().enumerate() {
							let (center, band_width) = transform.categorical(i, 0.0);
							let left = center.x - band_width / 2.0;
							let right = center.x + band_width / 2.0;
							if cursor_pos.x >= left && cursor_pos.x <= right {
								let bar_width = band_width * 0.8;
								let bar_offset = (band_width - bar_width) / 2.0;
								let bar_left = left + bar_offset;
								let bar_right = left + bar_offset + bar_width;
								if cursor_pos.x >= bar_left && cursor_pos.x <= bar_right {
									let y_scale =
										transform.bounds.height as f64 / (y_range.1 - y_range.0);
									let data_y = y_range.0
										+ (transform.bounds.y + transform.bounds.height
											- cursor_pos.y) as f64 / y_scale;
									let mut current_sum = 0.0f64;
									for (j, &val) in
										self.prepared_data.category_values[i].iter().enumerate()
									{
										if data_y >= current_sum && data_y <= current_sum + val {
											let group_name = &self.prepared_data.group_names[j];
											return Some(format!(
												"{}: {} (Value: {:.2}, Total: {:.2})",
												cat_name,
												group_name,
												val,
												current_sum + val
											));
										}
										current_sum += val;
									}
									return Some(format!("{}: Total {:.2}", cat_name, current_sum));
								}
							}
						}
					}
				}
				Orientation::Horizontal => {
					if let PlotLayout::CategoricalY {
						categories,
						x_range,
					} = transform.layout
					{
						for (i, cat_name) in categories.iter().enumerate() {
							let (center, band_height) = transform.categorical(i, 0.0);
							let top = center.y - band_height / 2.0;
							let bottom = center.y + band_height / 2.0;
							if cursor_pos.y >= top && cursor_pos.y <= bottom {
								let bar_height = band_height * 0.8;
								let bar_offset = (band_height - bar_height) / 2.0;
								let bar_top = top + bar_offset;
								let bar_bottom = top + bar_offset + bar_height;
								if cursor_pos.y >= bar_top && cursor_pos.y <= bar_bottom {
									let x_scale =
										transform.bounds.width as f64 / (x_range.1 - x_range.0);
									let data_x = x_range.0
										+ (cursor_pos.x - transform.bounds.x) as f64 / x_scale;
									let mut current_sum = 0.0f64;
									for (j, &val) in
										self.prepared_data.category_values[i].iter().enumerate()
									{
										if data_x >= current_sum && data_x <= current_sum + val {
											let group_name = &self.prepared_data.group_names[j];
											return Some(format!(
												"{}: {} (Value: {:.2}, Total: {:.2})",
												cat_name,
												group_name,
												val,
												current_sum + val
											));
										}
										current_sum += val;
									}
									return Some(format!("{}: Total {:.2}", cat_name, current_sum));
								}
							}
						}
					}
				}
			}
		}
		None
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_labels(
			self.prepared_data.group_names.iter().cloned(),
			settings,
		)
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

pub struct StackedBarPreparedData {
	pub categories: Vec<String>,
	pub group_names: Vec<String>,
	pub category_values: Vec<Vec<f64>>,
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_stacked_bar_data(
	df: &DataFrame,
	cat_col: &str,
	group_col: &str,
	val_col: &str,
) -> StackedBarPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return StackedBarPreparedData {
			categories: vec!["No Data".to_string()],
			group_names: vec!["Value".to_string()],
			category_values: vec![vec![0.0]],
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
	let category_idx_map: HashMap<&str, usize> = categories
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let group_idx_map: HashMap<&str, usize> = group_names
		.iter()
		.enumerate()
		.map(|(index, value)| (value.as_str(), index))
		.collect();
	let num_cats = categories.len();
	let num_groups = group_names.len();
	let mut category_values = vec![vec![0.0f64; num_groups]; num_cats];
	let mut assigned = vec![vec![false; num_groups]; num_cats];
	let vals_col_series = match df.column(val_col) {
		Ok(column) => column.cast(&DataType::Float64).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float64))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float64)),
	};
	let vals_f64 = vals_col_series.f64().expect("value column was cast to f64");
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
				.and_then(|value| category_idx_map.get(value.as_str()).copied())
		};
		let group_idx = if group_col.is_empty() {
			Some(0)
		} else {
			group_column
				.and_then(|column| column.get(row).ok())
				.map(category_label)
				.and_then(|value| group_idx_map.get(value.as_str()).copied())
		};
		let (Some(category_idx), Some(group_idx)) = (category_idx, group_idx) else {
			continue;
		};
		if !assigned[category_idx][group_idx] {
			category_values[category_idx][group_idx] = vals_f64.get(row).unwrap_or(0.0);
			assigned[category_idx][group_idx] = true;
		}
	}
	let max_sum = category_values
		.iter()
		.map(|values| values.iter().sum::<f64>())
		.fold(0.0, f64::max);
	let y_min = 0.0f64;
	let y_range = (y_min, max_sum * 1.1);
	StackedBarPreparedData {
		categories,
		group_names,
		category_values,
		y_range,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn stacks_follow_category_and_group_identity() {
		let frame = polars::df!(
			"cat" => ["B", "A", "B", "A", "A"],
			"group" => ["right", "left", "left", "right", "left"],
			"val" => [21_i64, 10, 20, 11, 999],
		)
		.expect("stacked bar fixture");

		let prepared = prepare_stacked_bar_data(&frame, "cat", "group", "val");

		assert_eq!(prepared.categories, ["A", "B"]);
		assert_eq!(prepared.group_names, ["left", "right"]);
		assert_eq!(
			prepared.category_values,
			vec![vec![10.0, 11.0], vec![20.0, 21.0]]
		);
		assert_eq!(prepared.y_range, (0.0, 45.1));
	}
}
