use crate::plot::common::{
	format_label, polars_type_to_axis_type, AxisType, CoordinateTransformer,
	PlotBackend, PlotKernel, PlotLayout, PlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Point, Rectangle, Size};
use polars::prelude::*;
use std::sync::Arc;

pub struct CandlestickPlotKernel {
	pub prepared_data: Arc<CandlestickPreparedData>,
}

impl PlotKernel for CandlestickPlotKernel {
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
		let n = self.prepared_data.x.len();
		if n == 0 {
			return;
		}
		let x_delta = if n > 1 {
			(self.prepared_data.x[1] - self.prepared_data.x[0]).abs()
		} else {
			1.0
		};
		let x_scale = transform.bounds.width as f64
			/ (self.prepared_data.x_range.1 - self.prepared_data.x_range.0);
		let candle_width = (x_scale * x_delta * 0.7).max(1.0) as f32;
		let bullish_color = settings.color_theme.get_color(1.0);
		let bearish_color = settings.color_theme.get_color(0.0);
		for i in 0..n {
			let x = self.prepared_data.x[i];
			let open = self.prepared_data.open[i];
			let high = self.prepared_data.high[i];
			let low = self.prepared_data.low[i];
			let close = self.prepared_data.close[i];
			let p_high = transform.cartesian(x, high);
			let p_low = transform.cartesian(x, low);
			let p_open = transform.cartesian(x, open);
			let p_close = transform.cartesian(x, close);
			let is_bullish = close >= open;
			let color = if is_bullish {
				bullish_color
			} else {
				bearish_color
			};
			backend.stroke_path(
				&|builder| {
					builder.move_to(p_low);
					builder.line_to(p_high);
				},
				Stroke {
					style: Style::Solid(color),
					width: 3.0,
					..Default::default()
				},
			);
			let body_top = p_open.y.min(p_close.y);
			let body_bottom = p_open.y.max(p_close.y);
			let body_height = (body_bottom - body_top).max(1.0);
			let body_x = p_open.x - candle_width / 2.0;
			backend.fill_rectangle(
				Point::new(body_x, body_top),
				Size::new(candle_width, body_height),
				color,
			);
			backend.stroke_path(
				&|builder| {
					builder.move_to(Point::new(p_open.x - candle_width / 2.0, p_open.y));
					builder.line_to(Point::new(p_open.x, p_open.y));
					builder.move_to(Point::new(p_close.x, p_close.y));
					builder.line_to(Point::new(p_close.x + candle_width / 2.0, p_close.y));
				},
				Stroke {
					style: Style::Solid(settings.decoration_color),
					width: 2.0,
					..Default::default()
				},
			);
		}
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position()
			&& let Some((x, _y)) = transform.pixel_to_cartesian(cursor_pos) {
			let xs = &self.prepared_data.x;
			if xs.is_empty() { return None; }
			let idx = match xs.binary_search_by(|val| val.partial_cmp(&x).unwrap()) {
				Ok(i) => i,
				Err(i) => {
					if i == 0 { 0 }
					else if i == xs.len() { xs.len() - 1 }
					else if (xs[i] - x).abs() < (xs[i-1] - x).abs() { i } else { i - 1 }
				}
			};
			let x_scale = transform.bounds.width as f64 / (self.prepared_data.x_range.1 - self.prepared_data.x_range.0);
			let dist_px = (xs[idx] - x).abs() * x_scale;
			if dist_px > 10.0 { return None; }
			return Some(format!(
				"X: {}\nOpen: {:.2}\nHigh: {:.2}\nLow: {:.2}\nClose: {:.2}",
				format_label(xs[idx], self.prepared_data.x_axis_type),
				self.prepared_data.open[idx],
				self.prepared_data.high[idx],
				self.prepared_data.low[idx],
				self.prepared_data.close[idx]
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

pub struct CandlestickPreparedData {
	pub x: Vec<f64>,
	pub open: Vec<f64>,
	pub high: Vec<f64>,
	pub low: Vec<f64>,
	pub close: Vec<f64>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
}

pub fn prepare_candlestick_data(
	df: &DataFrame,
	x_col: &str,
	open_col: &str,
	high_col: &str,
	low_col: &str,
	close_col: &str,
) -> CandlestickPreparedData {
	if df.height() == 0 || x_col.is_empty() {
		return CandlestickPreparedData {
			x: vec![],
			open: vec![],
			high: vec![],
			low: vec![],
			close: vec![],
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: x_col.to_string(),
			y_label: "Value".to_string(),
		};
	}
	let x_dtype = df.column(x_col).map(|c| c.dtype().clone()).unwrap_or(DataType::Float64);
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let y_axis_type = AxisType::Linear;
	let x = match df.column(x_col) {
		Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(x_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
		Err(_) => vec![],
	};
	let open = if open_col.is_empty() { vec![] } else {
		match df.column(open_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(open_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![],
		}
	};
	let high = if high_col.is_empty() { vec![] } else {
		match df.column(high_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(high_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![],
		}
	};
	let low = if low_col.is_empty() { vec![] } else {
		match df.column(low_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(low_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![],
		}
	};
	let close = if close_col.is_empty() { vec![] } else {
		match df.column(close_col) {
			Ok(c) => c.cast(&DataType::Float64).unwrap_or_else(|_| Column::from(Series::new_empty(close_col.into(), &DataType::Float64))).as_materialized_series().f64().unwrap().into_no_null_iter().collect::<Vec<_>>(),
			Err(_) => vec![],
		}
	};
	let x_min = x.iter().copied().fold(f64::INFINITY, f64::min);
	let x_max = x.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let y_min = low.iter().copied().fold(f64::INFINITY, f64::min);
	let y_max = high.iter().copied().fold(f64::NEG_INFINITY, f64::max);
	let (x_min, x_max) = if x_min.is_infinite() { (0.0, 1.0) } else { (x_min, x_max) };
	let (y_min, y_max) = if y_min.is_infinite() { (0.0, 1.0) } else { (y_min, y_max) };
	let x_pad = (x_max - x_min).max(0.1) * 0.05;
	let y_pad = (y_max - y_min).max(0.1) * 0.05;
	CandlestickPreparedData {
		x,
		open,
		high,
		low,
		close,
		x_range: (x_min - x_pad, x_max + x_pad),
		y_range: (y_min - y_pad, y_max + y_pad),
		x_axis_type,
		y_axis_type,
		x_label: x_col.to_string(),
		y_label: "Value".to_string(),
	}
}
