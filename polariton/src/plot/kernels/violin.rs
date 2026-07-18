use crate::plot::common::{
	CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct ViolinPlotKernel {
	pub prepared_data: Arc<ViolinPreparedData>,
}

impl PlotKernel for ViolinPlotKernel {
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
		cursor: Cursor,
		settings: PlotSettings,
	) {
		let num_violins = self.prepared_data.categories.len();
		let tex_height_bins = self.prepared_data.tex_height_bins;
		let (y_min, y_max) = self.prepared_data.y_range;
		let y_step = (y_max - y_min) / (tex_height_bins as f64 - 1.0);
		for i in 0..num_violins {
			let (_center, band_width) = transform.categorical(i, 0.0);
			let width_scale = band_width * 0.4;
			let t = if num_violins > 1 { i as f32 / (num_violins - 1) as f32 } else { 0.5 };
			let color = settings.color_theme.get_color(t);
			let mut first_bin = 0;
			for bin in 0..tex_height_bins {
				if self.prepared_data.kde_data[i * tex_height_bins + bin] > 0.01 {
					first_bin = bin;
					break;
				}
			}
			let mut last_bin = tex_height_bins - 1;
			for bin in (0..tex_height_bins).rev() {
				if self.prepared_data.kde_data[i * tex_height_bins + bin] > 0.01 {
					last_bin = bin;
					break;
				}
			}
			if first_bin >= last_bin { continue; }
			let violin_path_closure = |builder: &mut dyn PathBuilder| {
				for bin in first_bin..=last_bin {
					let data_y = y_min + bin as f64 * y_step;
					let density = self.prepared_data.kde_data[i * tex_height_bins + bin];
					let (p, _) = transform.categorical(i, data_y);
					if bin == first_bin {
						builder.move_to(iced::Point::new(p.x - density * width_scale, p.y));
					} else {
						builder.line_to(iced::Point::new(p.x - density * width_scale, p.y));
					}
				}
				for bin in (first_bin..=last_bin).rev() {
					let data_y = y_min + bin as f64 * y_step;
					let density = self.prepared_data.kde_data[i * tex_height_bins + bin];
					let (p, _) = transform.categorical(i, data_y);
					builder.line_to(iced::Point::new(p.x + density * width_scale, p.y));
				}
				builder.close();
			};
			backend.fill_path(&violin_path_closure, color);
			let border_stroke = Stroke {
				style: Style::Solid(settings.decoration_color),
				width: 2.5,
				..Default::default()
			};
			backend.stroke_path(&violin_path_closure, border_stroke);
			if let Some(&median_val) = self.prepared_data.medians.get(i) {
				let (median_px, _) = transform.categorical(i, median_val);
				let bin_idx = (((median_val - y_min) / (y_max - y_min)) * (tex_height_bins as f64 - 1.0))
					.floor() as usize;
				let bin_idx = bin_idx.min(tex_height_bins - 1);
				let density = self.prepared_data.kde_data[i * tex_height_bins + bin_idx];
				let line_half_width = density * width_scale;
				backend.stroke_path(
					&|builder| {
						builder.move_to(iced::Point::new(median_px.x - line_half_width, median_px.y));
						builder.line_to(iced::Point::new(median_px.x + line_half_width, median_px.y));
					},
					Stroke {
						style: Style::Solid(settings.decoration_color),
						width: 4.0,
						..Default::default()
					},
				);
			}
		}
		if let Some(cursor_pos) = cursor.position() {
			for i in 0..num_violins {
				let (center, band_width) = transform.categorical(i, 0.0);
				let left_edge = center.x - (band_width / 2.0);
				let right_edge = center.x + (band_width / 2.0);
				if cursor_pos.x >= left_edge && cursor_pos.x <= right_edge {
					if let Some(&median_val) = self.prepared_data.medians.get(i) {
						let (median_px, _) = transform.categorical(i, median_val);
						backend.stroke_path(
							&|builder| {
								builder.circle(median_px, 5.0);
							},
							Stroke {
								style: Style::Solid(Color::from_rgb(1.0, 0.2, 0.2)),
								width: 2.0,
								..Default::default()
							},
						);
					}
					break;
				}
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
		if num_cats == 0 { return; }
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
			Color { a: 0.6, ..settings.background_color }
		);
		for (i, name) in self.prepared_data.categories.iter().enumerate() {
			let t = if num_cats > 1 { i as f32 / (num_cats - 1) as f32 } else { 0.5 };
			let color = settings.color_theme.get_color(t);
			let col = i / max_rows;
			let row = i % max_rows;
			let item_x = x + legend_padding + col as f32 * col_width;
			let item_y = y + legend_padding + row as f32 * item_height;
			backend.fill_rectangle(
				iced::Point::new(item_x, item_y + (item_height - rect_size) / 2.0),
				iced::Size::new(rect_size, rect_size),
				color
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
		if let Some(cursor_pos) = cursor.position()
			&& let PlotLayout::CategoricalX { categories, .. } = transform.layout {
			for (i, cat) in categories.iter().enumerate() {
				let (center_point, band_width) = transform.categorical(i, 0.0);
				let left_edge = center_point.x - (band_width / 2.0);
				let right_edge = center_point.x + (band_width / 2.0);
				if cursor_pos.x >= left_edge && cursor_pos.x <= right_edge
					&& let Some(&median_val) = self.prepared_data.medians.get(i) {
					return Some(format!("{}: Median {:.2}", cat, median_val));
				}
			}
		}
		None
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

pub struct ViolinPreparedData {
	pub categories: Vec<String>,
	pub y_range: (f64, f64),
	pub medians: Vec<f64>,
	pub kde_data: Vec<f32>,
	pub tex_height_bins: usize,
	pub x_label: String,
	pub y_label: String,
}

fn compute_kde(
	y_vals: &[f64],
	num_bins: usize,
	y_min: f64,
	y_max: f64,
	bandwidth: f64,
) -> Vec<f32> {
	let mut density = vec![0.0; num_bins];
	let step = (y_max - y_min) / (num_bins as f64 - 1.0);
	let inv_bw = 1.0 / bandwidth;
	let norm_factor = 1.0 / (y_vals.len() as f64 * bandwidth * (2.0 * std::f64::consts::PI).sqrt());
	for (i, d) in density.iter_mut().enumerate() {
		let y = y_min + (i as f64) * step;
		let mut sum = 0.0;
		for &val in y_vals {
			let diff = (y - val) * inv_bw;
			sum += (-0.5 * diff * diff).exp();
		}
		*d = (sum * norm_factor) as f32;
	}
	let max_d = density.iter().cloned().fold(f32::MIN, f32::max);
	if max_d > 0.0 {
		for d in density.iter_mut() {
			*d /= max_d;
		}
	}
	density
}

pub fn prepare_violin_data(
	df: &DataFrame,
	cat_col: &str,
	val_col: &str,
	manual_range: Option<(f64, f64)>,
) -> ViolinPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return ViolinPreparedData {
			categories: vec!["No Data".to_string()],
			y_range: (0.0, 1.0),
			medians: vec![0.5],
			kde_data: vec![0.0; 256],
			tex_height_bins: 256,
			x_label: cat_col.to_string(),
			y_label: val_col.to_string(),
		};
	}
	let (y_min, y_max) = match manual_range {
		Some(r) => r,
		None => {
			let col_res = df.column(val_col)
				.map(|c| c.as_materialized_series().cast(&DataType::Float64).unwrap_or_else(|_| Series::new_empty(val_col.into(), &DataType::Float64)));
			
			match col_res {
				Ok(col) => {
					let col_f64 = col.f64().unwrap();
					let (y_min, y_max) = (col_f64.min().unwrap_or(0.0), col_f64.max().unwrap_or(1.0));
					let pad = (y_max - y_min).max(0.001) * 0.1;
					(y_min - pad, y_max + pad)
				}
				Err(_) => (0.0, 1.0),
			}
		}
	};
	let group_data_res = if cat_col.is_empty() {
		df.clone()
			.lazy()
			.select([
				lit("All Data").alias("dummy_cat"),
				col(val_col).median().alias("median"),
				col(val_col).alias("values"),
			])
			.collect()
	} else {
		df.clone()
			.lazy()
			.group_by([col(cat_col)])
			.agg([
				col(val_col).median().alias("median"),
				col(val_col).alias("values"),
			])
			.sort([cat_col], Default::default())
			.collect()
	};
	let group_data = match group_data_res {
		Ok(df) => df,
		Err(_) => {
			return ViolinPreparedData {
				categories: vec!["No Data".to_string()],
				y_range: (y_min, y_max),
				medians: vec![0.5],
				kde_data: vec![0.0; 256],
				tex_height_bins: 256,
				x_label: cat_col.to_string(),
				y_label: val_col.to_string(),
			};
		}
	};
	let actual_cat_col = if cat_col.is_empty() || !group_data.get_column_names().iter().any(|name| name.as_str() == cat_col) { "dummy_cat" } else { cat_col };
	let num_violins = group_data.height();
	let medians_series = group_data
		.column("median")
		.map(|c| c.as_materialized_series().cast(&DataType::Float64).unwrap_or_else(|_| Series::new_empty("median".into(), &DataType::Float64)))
		.unwrap_or_else(|_| Series::new_empty("median".into(), &DataType::Float64));
	let medians_f64 = medians_series.f64().unwrap();
	let values_list_series = group_data.column("values").map(|c| c.as_materialized_series().clone()).unwrap_or_else(|_| Series::new_empty("values".into(), &DataType::List(Box::new(DataType::Float64))));
	let values_list = values_list_series.list().unwrap();
	let categories_series = group_data.column(actual_cat_col).map(|c| c.as_materialized_series().clone()).unwrap_or_else(|_| Series::new("dummy_cat".into(), &["All Data"]));
	let categories: Vec<String> = if let Ok(ca) = categories_series.i32() {
		ca.into_no_null_iter().map(|i| i.to_string()).collect()
	} else {
		categories_series.iter().map(|v| {
			if let AnyValue::String(s) = v { s.to_string() } else { v.to_string().replace("\"", "") }
		}).collect()
	};
	let tex_height_bins = 256;
	let mut kde_data = vec![0.0f32; num_violins * tex_height_bins];
	let mut medians = Vec::with_capacity(num_violins);
	for i in 0..num_violins {
		medians.push(medians_f64.get(i).unwrap_or(0.0));
		let series_opt = values_list.get_as_series(i);
		if let Some(series) = series_opt {
			let y_slice: Vec<f64> = series.cast(&DataType::Float64).unwrap_or_else(|_| Series::new_empty("values".into(), &DataType::Float64)).f64().unwrap().into_no_null_iter().collect();
			let bandwidth = (y_max - y_min).max(0.001) * 0.03;
			let density = compute_kde(&y_slice, tex_height_bins, y_min, y_max, bandwidth);
			for bin in 0..tex_height_bins {
				kde_data[i * tex_height_bins + bin] = density[bin];
			}
		}
	}
	ViolinPreparedData {
		categories,
		y_range: (y_min, y_max),
		medians,
		kde_data,
		tex_height_bins,
		x_label: cat_col.to_string(),
		y_label: val_col.to_string(),
	}
}
