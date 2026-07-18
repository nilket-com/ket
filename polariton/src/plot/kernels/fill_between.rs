use crate::plot::common::{
	format_label, polars_type_to_axis_type, AxisType, CoordinateTransformer,
	PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::Rectangle;
use polars::prelude::*;
use std::sync::Arc;

pub struct FillBetweenPlotKernel {
	pub prepared_data: Arc<FillBetweenPreparedData>,
}

impl PlotKernel for FillBetweenPlotKernel {
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
			x_axis_type: self.prepared_data.x_axis_type,
			y_axis_type: self.prepared_data.y_axis_type,
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
		if self.prepared_data.x.is_empty() {
			return;
		}
		let line_color = settings.color_theme.get_color(0.9);
		let band_color = {
			let mut c = settings.color_theme.get_color(0.1);
			c.a = 0.9;
			c
		};
		backend.fill_path(
			&|builder| {
				for (i, &x) in self.prepared_data.x.iter().enumerate() {
					let p = transform.cartesian(x, self.prepared_data.y_upper[i]);
					if i == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
				for (i, &x) in self.prepared_data.x.iter().enumerate().rev() {
					let p = transform.cartesian(x, self.prepared_data.y_lower[i]);
					builder.line_to(p);
				}
				builder.close();
			},
			band_color,
		);
		let stroke = Stroke {
			style: Style::Solid(line_color),
			width: 2.5,
			..Default::default()
		};
		backend.stroke_path(
			&|builder| {
				for (i, &x) in self.prepared_data.x.iter().enumerate() {
					let p = transform.cartesian(x, self.prepared_data.y_mid[i]);
					if i == 0 {
						builder.move_to(p);
					} else {
						builder.line_to(p);
					}
				}
			},
			stroke,
		);
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, y)) = transform.pixel_to_cartesian(cursor_pos)
		{
			let xs = &self.prepared_data.x;
			if xs.is_empty() {
				return None;
			}
			let idx = match xs.binary_search_by(|val| val.partial_cmp(&x).unwrap()) {
				Ok(i) => i,
				Err(i) => {
					if i == 0 {
						0
					} else if i == xs.len() {
						xs.len() - 1
					} else if (xs[i] - x).abs() < (xs[i - 1] - x).abs() {
						i
					} else {
						i - 1
					}
				}
			};
			return Some(format!(
				"X: {}\nMid: {:.2}\nRange: [{:.2}, {:.2}]\nCursor Y: {:.2}",
				format_label(xs[idx], self.prepared_data.x_axis_type),
				self.prepared_data.y_mid[idx],
				self.prepared_data.y_lower[idx],
				self.prepared_data.y_upper[idx],
				y
			));
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

pub struct FillBetweenPreparedData {
	pub x: Vec<f64>,
	pub y_mid: Vec<f64>,
	pub y_lower: Vec<f64>,
	pub y_upper: Vec<f64>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_fill_between_data(
	df: &DataFrame,
	x_col: &str,
	y_mid_col: &str,
	y_lower_col: &str,
	y_upper_col: &str,
) -> FillBetweenPreparedData {
	if df.height() == 0 || x_col.is_empty() {
		return FillBetweenPreparedData {
			x: vec![],
			y_mid: vec![],
			y_lower: vec![],
			y_upper: vec![],
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: x_col.to_string(),
			y_label: y_mid_col.to_string(),
		};
	}
	let x_dtype = df.column(x_col).map(|c| c.dtype().clone()).unwrap_or(DataType::Float64);
	let y_mid_dtype = if y_mid_col.is_empty() { DataType::Float64 } else { df.column(y_mid_col).map(|c| c.dtype().clone()).unwrap_or(DataType::Float64) };
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let y_axis_type = polars_type_to_axis_type(&y_mid_dtype);
	let x_series = match df.column(x_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(x_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
		Err(_) => vec![],
	};
	let y_mid = if y_mid_col.is_empty() { vec![0.0; x_series.len()] } else {
		match df.column(y_mid_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(y_mid_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![0.0; x_series.len()],
		}
	};
	let y_lower = if y_lower_col.is_empty() { vec![0.0; x_series.len()] } else {
		match df.column(y_lower_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(y_lower_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![0.0; x_series.len()],
		}
	};
	let y_upper = if y_upper_col.is_empty() { vec![0.0; x_series.len()] } else {
		match df.column(y_upper_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(y_upper_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![0.0; x_series.len()],
		}
	};
	let x_min = x_series.iter().copied().fold(f64::INFINITY, f64::min);
	let x_max = x_series.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let y_min = y_lower.iter().copied().fold(f64::INFINITY, f64::min);
	let y_max = y_upper.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let (x_min, x_max) = if x_min.is_infinite() { (0.0, 1.0) } else { (x_min, x_max) };
	let (y_min, y_max) = if y_min.is_infinite() { (0.0, 1.0) } else { (y_min, y_max) };
	let x_pad = (x_max - x_min).max(0.1) * 0.001;
	let y_pad = (y_max - y_min).max(0.1) * 0.001;
	FillBetweenPreparedData {
		x: x_series,
		y_mid,
		y_lower,
		y_upper,
		x_range: (x_min - x_pad, x_max + x_pad),
		y_range: (y_min - y_pad, y_max + y_pad),
		x_axis_type,
		y_axis_type,
		x_label: x_col.to_string(),
		y_label: y_mid_col.to_string(),
	}
}
