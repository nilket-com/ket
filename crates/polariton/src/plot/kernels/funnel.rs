use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style, Text};
use iced::{Color, Pixels, Point, Rectangle};
use polars::prelude::*;
use std::sync::Arc;

pub struct FunnelPlotKernel {
	pub prepared_data: Arc<FunnelPreparedData>,
}

pub struct FunnelPreparedData {
	pub stages: Vec<String>,
	pub values: Vec<f32>,
	pub total_max: f32,
	pub x_label: String,
	pub y_label: String,
}

impl PlotKernel for FunnelPlotKernel {
	fn layout(&self, _settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Radial
	}

	fn plot(
		&self,
		backend: &mut dyn PlotBackend,
		bounds: Rectangle,
		_transform: &CoordinateTransformer,
		_cursor: Cursor,
		settings: ResolvedPlotSettings,
	) {
		let num_stages = self.prepared_data.stages.len();
		if num_stages == 0 {
			return;
		}
		let stage_height = bounds.height / num_stages as f32;
		let center_x = bounds.width / 2.0;
		for i in 0..num_stages {
			let val = self.prepared_data.values[i];
			let next_val = if i + 1 < num_stages {
				self.prepared_data.values[i + 1]
			} else {
				val
			};
			let width = (val / self.prepared_data.total_max) * bounds.width * 0.8;
			let next_width = (next_val / self.prepared_data.total_max) * bounds.width * 0.8;
			let y_top = i as f32 * stage_height;
			let y_bottom = y_top + stage_height;
			let t = if num_stages > 1 {
				i as f32 / (num_stages - 1) as f32
			} else {
				0.5
			};
			let color = settings.color_theme.get_color(t);
			let trapezoid_path = &|builder: &mut dyn crate::plot::common::PathBuilder| {
				builder.move_to(Point::new(center_x - width / 2.0, y_top));
				builder.line_to(Point::new(center_x + width / 2.0, y_top));
				builder.line_to(Point::new(center_x + next_width / 2.0, y_bottom));
				builder.line_to(Point::new(center_x - next_width / 2.0, y_bottom));
				builder.close();
			};
			backend.fill_path(trapezoid_path, color);
			backend.stroke_path(
				trapezoid_path,
				Stroke {
					style: Style::Solid(Color {
						a: 0.2,
						..settings.mark_style.decoration.iced()
					}),
					width: 1.0,
					..Default::default()
				},
			);
			backend.fill_text(Text {
				content: format!("{:.0}", val),
				position: Point::new(center_x, y_top + stage_height / 2.0),
				color: settings.mark_style.label_text.iced(),
				size: Pixels(16.0),
				align_x: iced::alignment::Horizontal::Center.into(),
				align_y: iced::alignment::Vertical::Center,
				..Default::default()
			});
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_labels(
			self.prepared_data.stages.iter().cloned(),
			settings,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let bounds = transform.bounds;
			if !bounds.contains(cursor_pos) {
				return None;
			}
			let num_stages = self.prepared_data.stages.len();
			if num_stages == 0 {
				return None;
			}
			let stage_height = bounds.height / num_stages as f32;
			let center_x = bounds.x + bounds.width / 2.0;
			let stage_idx = ((cursor_pos.y - bounds.y) / stage_height).floor() as usize;
			let stage_idx = stage_idx.min(num_stages - 1);
			let val = self.prepared_data.values[stage_idx];
			let next_val = if stage_idx + 1 < num_stages {
				self.prepared_data.values[stage_idx + 1]
			} else {
				val
			};
			let y_within_stage =
				(cursor_pos.y - (bounds.y + stage_idx as f32 * stage_height)) / stage_height;
			let current_val_at_y = val - (val - next_val) * y_within_stage;
			let current_width_at_y =
				(current_val_at_y / self.prepared_data.total_max) * bounds.width * 0.8;
			let left_x = center_x - current_width_at_y / 2.0;
			let right_x = center_x + current_width_at_y / 2.0;
			if cursor_pos.x >= left_x && cursor_pos.x <= right_x {
				let stage_name = &self.prepared_data.stages[stage_idx];
				let mut hover_text = format!("{}: {:.0}", stage_name, val);
				if stage_idx > 0 {
					let prev_val = self.prepared_data.values[stage_idx - 1];
					let conversion = (val / prev_val) * 100.0;
					hover_text.push_str(&format!("\nConversion: {:.1}%", conversion));
				}
				let total_conversion = (val / self.prepared_data.values[0]) * 100.0;
				hover_text.push_str(&format!("\nTotal Rate: {:.1}%", total_conversion));
				return Some(hover_text);
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

pub fn prepare_funnel_data(df: &DataFrame, stage_col: &str, val_col: &str) -> FunnelPreparedData {
	if df.height() == 0 || val_col.is_empty() {
		return FunnelPreparedData {
			stages: vec![],
			values: vec![],
			total_max: 1.0,
			x_label: val_col.to_string(),
			y_label: stage_col.to_string(),
		};
	}
	let stages_series = if stage_col.is_empty() {
		Series::new(
			"dummy_stage".into(),
			(0..df.height())
				.map(|i| format!("Stage {}", i))
				.collect::<Vec<_>>(),
		)
	} else {
		match df.column(stage_col) {
			Ok(c) => c.as_materialized_series().clone(),
			Err(_) => Series::new(
				"dummy_stage".into(),
				(0..df.height())
					.map(|i| format!("Stage {}", i))
					.collect::<Vec<_>>(),
			),
		}
	};
	let values_series = match df.column(val_col) {
		Ok(c) => c.cast(&DataType::Float32).unwrap_or_else(|_| {
			Column::from(Series::new_empty(val_col.into(), &DataType::Float32))
		}),
		Err(_) => Column::from(Series::new_empty(val_col.into(), &DataType::Float32)),
	};
	let stages: Vec<String> = stages_series
		.iter()
		.map(|v| {
			if let AnyValue::String(s) = v {
				s.to_string()
			} else {
				v.to_string().replace("\"", "")
			}
		})
		.collect();
	let values: Vec<f32> = values_series.f32().unwrap().into_no_null_iter().collect();
	let total_max = values.iter().cloned().fold(0.0, f32::max).max(1.0);
	FunnelPreparedData {
		stages,
		values,
		total_max,
		x_label: val_col.to_string(),
		y_label: stage_col.to_string(),
	}
}
