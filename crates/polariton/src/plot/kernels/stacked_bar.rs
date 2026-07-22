use crate::plot::common::{
	CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::Text;
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct StackedBarPlotKernel {
	pub prepared_data: Arc<StackedBarPreparedData>,
	pub orientation: Orientation,
}

impl PlotKernel for StackedBarPlotKernel {
	fn layout(&self, settings: PlotSettings) -> PlotLayout {
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
		settings: PlotSettings,
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

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: PlotSettings,
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
	let (group_names, groups_series) = if group_col.is_empty() {
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
	let group_idx_map: HashMap<AnyValue, usize> = groups_series
		.iter()
		.enumerate()
		.map(|(i, v): (usize, AnyValue)| (v.into_static(), i))
		.collect();
	let num_cats = categories.len();
	let num_groups = group_names.len();
	let mut category_values = vec![vec![0.0f64; num_groups]; num_cats];
	let mut max_sum = 0.0f64;
	let partitions = if cat_col.is_empty() {
		vec![df.clone()]
	} else {
		df.partition_by([cat_col], true)
			.unwrap_or_else(|_| vec![df.clone()])
	};
	for (i, group_df) in partitions.into_iter().enumerate() {
		if i >= num_cats {
			break;
		}
		let group_partitions = if group_col.is_empty() {
			vec![group_df]
		} else {
			group_df
				.partition_by([group_col], true)
				.unwrap_or_else(|_| vec![group_df.clone()])
		};
		let mut current_cat_sum = 0.0f64;
		for sub_group_df in group_partitions {
			let group_idx = if group_col.is_empty() {
				Some(0)
			} else {
				let group_val = sub_group_df
					.column(group_col)
					.and_then(|c| c.get(0))
					.unwrap_or(AnyValue::Null);
				group_idx_map.get(&group_val).copied()
			};

			if let Some(group_idx) = group_idx {
				if group_idx >= num_groups {
					continue;
				}
				let val = sub_group_df
					.column(val_col)
					.and_then(|c| c.cast(&DataType::Float64))
					.map(|c| c.f64().unwrap().get(0).unwrap_or(0.0))
					.unwrap_or(0.0);
				category_values[i][group_idx] = val;
				current_cat_sum += val;
			}
		}
		if current_cat_sum > max_sum {
			max_sum = current_cat_sum;
		}
	}
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
