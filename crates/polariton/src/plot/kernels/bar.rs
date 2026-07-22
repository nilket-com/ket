use crate::plot::common::{
	CoordinateTransformer, Orientation, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::Text;
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct BarPlotKernel {
	pub prepared_data: Arc<BarPreparedData>,
	pub orientation: Orientation,
}

impl PlotKernel for BarPlotKernel {
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
				let group_area_width = total_band_width * 0.8;
				let group_area_offset = (total_band_width - group_area_width) / 2.0;
				let sub_group_width = group_area_width / num_groups as f32;
				let bar_padding = sub_group_width * 0.05;
				for i in 0..num_cats {
					let cat_left = (i as f32 * total_band_width) + group_area_offset;
					for j in 0..num_groups {
						let val = self.prepared_data.values[i][j];
						if val <= y_base {
							continue;
						}
						let (p_top, _) = transform.categorical(i, val);
						let (p_bottom, _) = transform.categorical(i, y_base);
						let x_start = cat_left + (j as f32 * sub_group_width) + bar_padding;
						let x_end = cat_left + ((j + 1) as f32 * sub_group_width) - bar_padding;
						let bar_rect = Rectangle {
							x: x_start,
							y: p_top.y,
							width: (x_end - x_start).max(1.0),
							height: (p_bottom.y - p_top.y).max(1.0),
						};
						let t = if num_groups > 1 {
							j as f32 / (num_groups - 1) as f32
						} else {
							0.5
						};
						let color = settings.color_theme.get_color(t);
						backend.fill_rectangle(bar_rect.position(), bar_rect.size(), color);
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
				let group_area_height = total_band_height * 0.8;
				let group_area_offset = (total_band_height - group_area_height) / 2.0;
				let sub_group_height = group_area_height / num_groups as f32;
				let bar_padding = sub_group_height * 0.05;
				for i in 0..num_cats {
					let cat_top = (num_cats - 1 - i) as f32 * total_band_height + group_area_offset;
					for j in 0..num_groups {
						let val = self.prepared_data.values[i][j];
						if val <= x_base {
							continue;
						}
						let (p_right, _) = transform.categorical(i, val);
						let (p_left, _) = transform.categorical(i, x_base);
						let y_start =
							cat_top + (num_groups - 1 - j) as f32 * sub_group_height + bar_padding;
						let y_end =
							cat_top + (num_groups - j) as f32 * sub_group_height - bar_padding;
						let bar_rect = Rectangle {
							x: p_left.x,
							y: y_start,
							width: (p_right.x - p_left.x).max(1.0),
							height: (y_end - y_start).max(1.0),
						};
						let t = if num_groups > 1 {
							j as f32 / (num_groups - 1) as f32
						} else {
							0.5
						};
						let color = settings.color_theme.get_color(t);
						backend.fill_rectangle(bar_rect.position(), bar_rect.size(), color);
					}
				}
			}
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let num_groups = self.prepared_data.group_names.len();
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
								let group_area_width = band_width * 0.8;
								let group_area_offset = (band_width - group_area_width) / 2.0;
								let cluster_left = left + group_area_offset;
								let cluster_right = left + group_area_offset + group_area_width;
								if cursor_pos.x >= cluster_left && cursor_pos.x <= cluster_right {
									let sub_group_width = group_area_width / num_groups as f32;
									let local_x = cursor_pos.x - cluster_left;
									let group_idx = (local_x / sub_group_width).floor() as usize;
									let group_idx = group_idx.min(num_groups - 1);
									let y_scale =
										transform.bounds.height as f64 / (y_range.1 - y_range.0);
									let data_y = y_range.0
										+ (transform.bounds.height - cursor_pos.y) as f64 / y_scale;
									let group_name = &self.prepared_data.group_names[group_idx];
									return Some(format!(
										"{}: {} (Value: ~{:.2})",
										cat_name, group_name, data_y
									));
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
								let group_area_height = band_height * 0.8;
								let group_area_offset = (band_height - group_area_height) / 2.0;
								let cluster_top = top + group_area_offset;
								let cluster_bottom = top + group_area_offset + group_area_height;
								if cursor_pos.y >= cluster_top && cursor_pos.y <= cluster_bottom {
									let sub_group_height = group_area_height / num_groups as f32;
									let local_y = cursor_pos.y - cluster_top;
									let group_idx = (num_groups
										- 1 - (local_y / sub_group_height).floor()
										as usize)
										.min(num_groups - 1);
									let x_scale =
										transform.bounds.width as f64 / (x_range.1 - x_range.0);
									let data_x = x_range.0 + cursor_pos.x as f64 / x_scale;
									let group_name = &self.prepared_data.group_names[group_idx];
									return Some(format!(
										"{}: {} (Value: ~{:.2})",
										cat_name, group_name, data_x
									));
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
		for sub_group_df in group_partitions {
			let g_idx = if group_col.is_empty() {
				0
			} else {
				let g_val = sub_group_df
					.column(group_col)
					.and_then(|c| c.get(0))
					.unwrap_or(AnyValue::Null);
				groups_series.iter().position(|v| v == g_val).unwrap_or(0)
			};
			if g_idx >= num_groups {
				continue;
			}
			let val = sub_group_df
				.column(val_col)
				.and_then(|c| c.cast(&DataType::Float64))
				.map(|c| c.f64().unwrap().get(0).unwrap_or(0.0))
				.unwrap_or(0.0);
			values[i][g_idx] = val;
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
