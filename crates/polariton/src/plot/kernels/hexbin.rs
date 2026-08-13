use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::continuous_color::{ContinuousColorScale, ContinuousScaleIntent};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::lazy::prelude::*;
use polars::prelude::*;
use std::sync::Arc;

pub struct HexbinPlotKernel {
	pub prepared_data: Arc<HexbinPreparedData>,
}

impl HexbinPlotKernel {
	fn continuous_scale(&self, settings: &ResolvedPlotSettings) -> ContinuousColorScale {
		ContinuousColorScale::new(
			(0.0, f64::from(self.prepared_data.max_count)),
			self.prepared_data.color_intent,
			settings.continuous_ramps,
		)
		.expect("PRODUCT-0089 planning validated this hexbin continuous colour scale")
	}
}

impl PlotKernel for HexbinPlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
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
		settings: ResolvedPlotSettings,
	) {
		let radius = self.prepared_data.radius as f64;
		let sqrt_3 = 3.0f64.sqrt();
		let scale = self.continuous_scale(&settings);
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
				let color = scale.color(f64::from(count));
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

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		let scale = self.continuous_scale(settings);
		crate::plot::legend::gradient_section("Count".to_owned(), &scale).map(|section| {
			crate::plot::legend::LegendContent(crate::plot::spec::NonEmpty::new(section))
		})
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos)
		{
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
				return Some(format!(
					"Bin: ({}, {})\nCount: {}\nPos: ({:.2}, {:.2})",
					q, r, count, x, y
				));
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
	pub color_intent: ContinuousScaleIntent,
	pub radius: f32,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_label: String,
	pub y_label: String,
}

fn bin_data_to_hex(
	df: DataFrame,
	x_col: &str,
	y_col: &str,
	radius: f32,
) -> PolarsResult<DataFrame> {
	let sqrt_3 = 3.0f32.sqrt();
	let with_frac = df.lazy().with_columns([
		((lit(sqrt_3 / 3.0) * col(x_col) - lit(1.0f32 / 3.0) * col(y_col)) / lit(radius))
			.alias("q_frac"),
		((lit(2.0f32 / 3.0) * col(y_col)) / lit(radius)).alias("r_frac"),
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

pub fn prepare_hexbin_data(
	df: &DataFrame,
	x_col: &str,
	y_col: &str,
	radius: f32,
) -> HexbinPreparedData {
	if df.height() == 0 || df.column(x_col).is_err() || df.column(y_col).is_err() {
		return HexbinPreparedData {
			bins: std::collections::HashMap::new(),
			max_count: 1,
			color_intent: ContinuousScaleIntent::SEQUENTIAL,
			radius,
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_label: x_col.to_string(),
			y_label: y_col.to_string(),
		};
	}
	let x_col_series = match df.column(x_col) {
		Ok(c) => c
			.cast(&DataType::Float64)
			.unwrap_or_else(|_| Column::from(Series::new_empty(x_col.into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty(x_col.into(), &DataType::Float64)),
	};
	let y_col_series = match df.column(y_col) {
		Ok(c) => c
			.cast(&DataType::Float64)
			.unwrap_or_else(|_| Column::from(Series::new_empty(y_col.into(), &DataType::Float64))),
		Err(_) => Column::from(Series::new_empty(y_col.into(), &DataType::Float64)),
	};
	let x_values = x_col_series.f64().unwrap();
	let y_values = y_col_series.f64().unwrap();
	let x_range = (x_values.min().unwrap_or(0.0), x_values.max().unwrap_or(1.0));
	let y_range = (y_values.min().unwrap_or(0.0), y_values.max().unwrap_or(1.0));
	let binned = match bin_data_to_hex(df.clone(), x_col, y_col, radius) {
		Ok(df) => df,
		Err(_) => {
			return HexbinPreparedData {
				bins: std::collections::HashMap::new(),
				max_count: 1,
				color_intent: ContinuousScaleIntent::SEQUENTIAL,
				radius,
				x_range,
				y_range,
				x_label: x_col.to_string(),
				y_label: y_col.to_string(),
			};
		}
	};
	let q_col = binned
		.column("q")
		.and_then(|c| c.i32())
		.expect("q column not found");
	let r_col = binned
		.column("r")
		.and_then(|c| c.i32())
		.expect("r column not found");
	let count_col = binned
		.column("count")
		.and_then(|c| c.u32())
		.expect("count column not found");
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
		color_intent: ContinuousScaleIntent::SEQUENTIAL,
		radius,
		x_range,
		y_range,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0087_hexbin_omits_a_zero_count_gradient() {
		let kernel = HexbinPlotKernel {
			prepared_data: Arc::new(HexbinPreparedData {
				bins: std::collections::HashMap::new(),
				max_count: 0,
				color_intent: ContinuousScaleIntent::SEQUENTIAL,
				radius: 0.02,
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
			}),
		};
		assert!(
			kernel
				.legend_content(&ResolvedPlotSettings::default())
				.is_none()
		);
	}

	#[test]
	fn product_0089_hexbin_accepts_the_one_ulp_arithmetic_change() {
		let kernel = HexbinPlotKernel {
			prepared_data: Arc::new(HexbinPreparedData {
				bins: std::collections::HashMap::new(),
				max_count: 1_481_765_933,
				color_intent: ContinuousScaleIntent::SEQUENTIAL,
				radius: 0.02,
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
			}),
		};
		let scale = kernel.continuous_scale(&ResolvedPlotSettings::default());
		let count = 269_329_525_u32;
		let old = count as f32 / kernel.prepared_data.max_count as f32;
		let accepted = scale.normalize(f64::from(count)) as f32;
		assert_eq!(old.to_bits(), 0x3E3A_1FF6);
		assert_eq!(accepted.to_bits(), 0x3E3A_1FF5);
		assert_eq!(old.to_bits().abs_diff(accepted.to_bits()), 1);
	}
}
