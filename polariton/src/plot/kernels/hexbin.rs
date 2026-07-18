use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout,
	PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::lazy::prelude::*;
use polars::prelude::*;
use std::sync::Arc;

pub struct HexbinPlotKernel {
	pub prepared_data: Arc<HexbinPreparedData>,
}

impl PlotKernel for HexbinPlotKernel {
	fn layout(&self, settings: PlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings.x_min.unwrap_or(self.prepared_data.x_range.0),
				settings.x_max.unwrap_or(self.prepared_data.x_range.1),
			),
			y_range: (
				settings.y_min.unwrap_or(self.prepared_data.y_range.0),
				settings.y_max.unwrap_or(self.prepared_data.y_range.1),
			),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
		}
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: PlotSettings,
	) {
		let radius = self.prepared_data.radius as f64;
		let sqrt_3 = 3.0f64.sqrt();
		let max_count = self.prepared_data.max_count as f64;
		backend.with_clip(bounds, &mut |backend| {
			for (&(q, r), &count) in &self.prepared_data.bins {
				if count == 0 {
					continue;
				}
				let lx = radius * (sqrt_3 * q as f64 + sqrt_3 / 2.0 * r as f64);
				let ly = radius * (3.0 / 2.0 * r as f64);
				let hex_path = &|builder: &mut dyn crate::plot::common::PathBuilder| {
					for i in 0..6 {
						let angle_deg = 60.0 * i as f64 - 30.0;
						let angle_rad = std::f64::consts::PI / 180.0 * angle_deg;
						let dx = radius * angle_rad.cos();
						let dy = radius * angle_rad.sin();
						let p = transform.cartesian(lx + dx, ly + dy);
						if i == 0 {
							builder.move_to(p);
						} else {
							builder.line_to(p);
						}
					}
					builder.close();
				};
				let t = count as f32 / max_count as f32;
				let color = settings.color_theme.get_color(t);
				backend.fill_path(hex_path, color);
				backend.stroke_path(
					hex_path,
					Stroke {
						style: Style::Solid(Color::from_rgba(0.0, 0.0, 0.0, 0.1)),
						width: 0.5,
						..Default::default()
					},
				);
			}
		});
	}

	fn draw_legend(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		settings: PlotSettings,
	) {
		let max_count = self.prepared_data.max_count;
		let legend_width = 60.0;
		let legend_height = 200.0;
		let legend_padding = 10.0;
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
		let bar_width = 15.0;
		let bar_height = legend_height - 55.0;
		let bar_x = x + legend_padding;
		let bar_y = y + 35.0;
		let steps = 50;
		for i in 0..steps {
			let t = i as f32 / (steps - 1) as f32;
			let color = settings.color_theme.get_color(t);
			let step_height = bar_height / steps as f32;
			let step_y = bar_y + bar_height - (i as f32 + 1.0) * step_height;
			backend.fill_rectangle(
				iced::Point::new(bar_x, step_y),
				iced::Size::new(bar_width, step_height + 0.5),
				color,
			);
		}
		backend.stroke_path(
			&|builder| {
				builder.move_to(iced::Point::new(bar_x, bar_y));
				builder.line_to(iced::Point::new(bar_x + bar_width, bar_y));
				builder.line_to(iced::Point::new(bar_x + bar_width, bar_y + bar_height));
				builder.line_to(iced::Point::new(bar_x, bar_y + bar_height));
				builder.close();
			},
			Stroke {
				style: Style::Solid(settings.decoration_color),
				width: 1.0,
				..Default::default()
			},
		);
		let label_x = bar_x + bar_width + 5.0;
		backend.fill_text(iced::widget::canvas::Text {
			content: format!("{}", max_count),
			position: iced::Point::new(label_x, bar_y),
			color: settings.decoration_color,
			size: iced::Pixels(settings.legend_size),
			align_y: iced::alignment::Vertical::Top,
			..Default::default()
		});
		backend.fill_text(iced::widget::canvas::Text {
			content: "0".to_string(),
			position: iced::Point::new(label_x, bar_y + bar_height),
			color: settings.decoration_color,
			size: iced::Pixels(settings.legend_size),
			align_y: iced::alignment::Vertical::Bottom,
			..Default::default()
		});
		backend.fill_text(iced::widget::canvas::Text {
			content: "Count".to_string(),
			position: iced::Point::new(x + legend_width / 2.0, y + 10.0),
			color: settings.decoration_color,
			size: iced::Pixels(settings.legend_size),
			align_x: iced::alignment::Horizontal::Center.into(),
			align_y: iced::alignment::Vertical::Top,
			..Default::default()
		});
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos) {
			let radius = self.prepared_data.radius as f64;
			let sqrt_3 = 3.0f64.sqrt();
			let q_frac = (sqrt_3 / 3.0 * x - 1.0 / 3.0 * y) / radius;
			let r_frac = (2.0 / 3.0 * y) / radius;
			let mut q = q_frac.round();
			let mut r = r_frac.round();
			let s = (-q_frac - r_frac).round();
			let q_diff = (q - q_frac).abs();
			let r_diff = (r - r_frac).abs();
			let s_diff = (s - (-q_frac - r_frac)).abs();
			if q_diff > r_diff && q_diff > s_diff {
				q = -r - s;
			} else if r_diff > s_diff {
				r = -q - s;
			}
			if let Some(&count) = self.prepared_data.bins.get(&(q as i32, r as i32)) {
				return Some(format!("Bin: ({}, {})\nCount: {}\nPos: ({:.2}, {:.2})", q, r, count, x, y));
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

pub struct HexbinPreparedData {
	pub bins: std::collections::HashMap<(i32, i32), u32>,
	pub max_count: u32,
	pub radius: f32,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
}

fn bin_data_to_hex(df: DataFrame, radius: f32) -> PolarsResult<DataFrame> {
	let sqrt_3 = 3.0f32.sqrt();
	let with_frac = df.lazy().with_columns([
		((lit(sqrt_3 / 3.0) * col("x") - lit(1.0f32 / 3.0) * col("y")) / lit(radius))
			.alias("q_frac"),
		((lit(2.0f32 / 3.0) * col("y")) / lit(radius)).alias("r_frac"),
	]);
	let with_rounded = with_frac.with_columns([
		col("q_frac").round(0, RoundMode::HalfToEven).alias("q"),
		col("r_frac").round(0, RoundMode::HalfToEven).alias("r"),
		(-col("q_frac") - col("r_frac"))
			.round(0, RoundMode::HalfToEven)
			.alias("s"),
	]);
	let with_diffs = with_rounded.with_columns([
		(col("q_frac") - col("q")).abs().alias("q_diff"),
		(col("r_frac") - col("r")).abs().alias("r_diff"),
		((-col("q_frac") - col("r_frac")) - col("s"))
			.abs()
			.alias("s_diff"),
	]);
	let with_corrected = with_diffs.with_columns([
		when(
			col("q_diff")
				.gt(col("r_diff"))
				.and(col("q_diff").gt(col("s_diff"))),
		)
		.then(-col("r") - col("s"))
		.otherwise(col("q"))
		.cast(DataType::Int32)
		.alias("q"),
		when(
			col("q_diff")
				.gt(col("r_diff"))
				.and(col("q_diff").gt(col("s_diff")))
				.not()
				.and(col("r_diff").gt(col("s_diff"))),
		)
		.then(-col("q") - col("s"))
		.otherwise(col("r"))
		.cast(DataType::Int32)
		.alias("r"),
	]);
	with_corrected
		.group_by([col("q"), col("r")])
		.agg([len().alias("count")])
		.collect()
}

pub fn prepare_hexbin_data(df: &DataFrame, radius: f32) -> HexbinPreparedData {
	if df.height() == 0 || df.column("x").is_err() || df.column("y").is_err() {
		return HexbinPreparedData {
			bins: std::collections::HashMap::new(),
			max_count: 1,
			radius,
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_label: "x".to_string(),
			y_label: "y".to_string(),
		};
	}
	let x_col_series = match df.column("x") {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty("x".into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty("x".into(), &DataType::Float64)),
	};
	let y_col_series = match df.column("y") {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty("y".into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty("y".into(), &DataType::Float64)),
	};
	let x_col = x_col_series.f64().unwrap();
	let y_col = y_col_series.f64().unwrap();
	let x_range = (x_col.min().unwrap_or(0.0), x_col.max().unwrap_or(1.0));
	let y_range = (y_col.min().unwrap_or(0.0), y_col.max().unwrap_or(1.0));
	let binned = match bin_data_to_hex(df.clone(), radius) {
		Ok(df) => df,
		Err(_) => {
			return HexbinPreparedData {
				bins: std::collections::HashMap::new(),
				max_count: 1,
				radius,
				x_range,
				y_range,
				x_label: "x".to_string(),
				y_label: "y".to_string(),
			};
		}
	};
	let q_col = binned.column("q").and_then(|c| c.i32()).expect("q column not found");
	let r_col = binned.column("r").and_then(|c| c.i32()).expect("r column not found");
	let count_col = binned.column("count").and_then(|c| c.u32()).expect("count column not found");
	let mut bins = std::collections::HashMap::new();
	let mut max_count = 0;
	for i in 0..binned.height() {
		let q = q_col.get(i).unwrap_or(0);
		let r = r_col.get(i).unwrap_or(0);
		let count = count_col.get(i).unwrap_or(0);
		bins.insert((q, r), count);
		if count > max_count {
			max_count = count;
		}
	}
	HexbinPreparedData {
		bins,
		max_count,
		radius,
		x_range,
		y_range,
		x_label: "x".to_string(),
		y_label: "y".to_string(),
	}
}
