use crate::coordinate::{Pt, Widget};
use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	format_label,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::funnel_stages::{
	FunnelStageRow, FunnelStages, FunnelStagesError, FunnelValueCell,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style, Text};
use iced::{Color, Pixels, Point, Rectangle};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

pub struct FunnelPlotKernel {
	pub prepared_data: Arc<FunnelPreparedData>,
}

pub struct FunnelPreparedData {
	pub stages: FunnelStages,
	pub x_label: String,
	pub y_label: String,
}

fn funnel_width(value: f64, first_value: f64, bounds_width: f32) -> f32 {
	((value / first_value) * f64::from(bounds_width) * 0.8) as f32
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FunnelPreparationChannel {
	Stage,
	Value,
}

impl fmt::Display for FunnelPreparationChannel {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::Stage => "stage",
			Self::Value => "value",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FunnelPreparationError {
	MissingColumn {
		channel: FunnelPreparationChannel,
		field: String,
	},
	UncastableColumn {
		channel: FunnelPreparationChannel,
		field: String,
	},
	Stages(FunnelStagesError),
}

impl fmt::Display for FunnelPreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { channel, field } => {
				write!(formatter, "funnel {channel} field `{field}` is missing")
			}
			Self::UncastableColumn { channel, field } => write!(
				formatter,
				"funnel {channel} field `{field}` has an unsupported type"
			),
			Self::Stages(source) => source.fmt(formatter),
		}
	}
}

impl std::error::Error for FunnelPreparationError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Stages(source) => Some(source),
			Self::MissingColumn { .. } | Self::UncastableColumn { .. } => None,
		}
	}
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
		let num_stages = self.prepared_data.stages.iter().len();
		if self.prepared_data.stages.is_empty() {
			return;
		}
		let stage_height = bounds.height / num_stages as f32;
		let center_x = bounds.width / 2.0;
		let first_value = self
			.prepared_data
			.stages
			.first_value()
			.expect("non-empty validated funnel has a positive first stage");
		for (i, stage) in self.prepared_data.stages.iter().enumerate() {
			let val = stage.value();
			let next_val = self
				.prepared_data
				.stages
				.get(i + 1)
				.map_or(val, |next| next.value());
			let width = funnel_width(val, first_value, bounds.width);
			let next_width = funnel_width(next_val, first_value, bounds.width);
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
				content: format_label(val, AxisType::Linear),
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
			self.prepared_data
				.stages
				.iter()
				.map(|stage| stage.stage().to_owned()),
			settings,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let cursor_pos = Pt::<Widget>::from_plot_kernel_cursor(cursor_pos);
			let (bounds, cursor_pos) = transform.widget_hit_geometry(cursor_pos);
			if !bounds.contains(cursor_pos) {
				return None;
			}
			let num_stages = self.prepared_data.stages.iter().len();
			if num_stages == 0 {
				return None;
			}
			let stage_height = bounds.height() / num_stages as f32;
			let center_x = bounds.x() + bounds.width() / 2.0;
			let stage_idx = ((cursor_pos.y() - bounds.y()) / stage_height).floor() as usize;
			let stage_idx = stage_idx.min(num_stages - 1);
			let stage = self
				.prepared_data
				.stages
				.get(stage_idx)
				.expect("hover stage is in bounds");
			let val = stage.value();
			let next_val = self
				.prepared_data
				.stages
				.get(stage_idx + 1)
				.map_or(val, |next| next.value());
			let y_within_stage =
				(cursor_pos.y() - (bounds.y() + stage_idx as f32 * stage_height)) / stage_height;
			let current_val_at_y = val - (val - next_val) * f64::from(y_within_stage);
			let first_value = self
				.prepared_data
				.stages
				.first_value()
				.expect("non-empty validated funnel has a positive first stage");
			let current_width_at_y = funnel_width(current_val_at_y, first_value, bounds.width());
			let left_x = center_x - current_width_at_y / 2.0;
			let right_x = center_x + current_width_at_y / 2.0;
			if cursor_pos.x() >= left_x && cursor_pos.x() <= right_x {
				let mut hover_text =
					format!("{}: {}", stage.stage(), format_label(val, AxisType::Linear));
				if stage_idx > 0 {
					let prev_val = self
						.prepared_data
						.stages
						.get(stage_idx - 1)
						.expect("previous hover stage is in bounds")
						.value();
					if prev_val == 0.0 {
						hover_text.push_str("\nConversion: —");
					} else {
						let conversion = (val / prev_val) * 100.0;
						hover_text.push_str(&format!("\nConversion: {:.1}%", conversion));
					}
				}
				let total_conversion = (val / first_value) * 100.0;
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

pub(crate) fn prepare_funnel_data(
	df: &DataFrame,
	stage_col: &str,
	val_col: &str,
) -> Result<FunnelPreparedData, FunnelPreparationError> {
	let stages = df
		.column(stage_col)
		.map_err(|_| FunnelPreparationError::MissingColumn {
			channel: FunnelPreparationChannel::Stage,
			field: stage_col.to_owned(),
		})?;
	if !matches!(
		stages.dtype(),
		DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) | DataType::Boolean
	) {
		return Err(FunnelPreparationError::UncastableColumn {
			channel: FunnelPreparationChannel::Stage,
			field: stage_col.to_owned(),
		});
	}
	let values = df
		.column(val_col)
		.map_err(|_| FunnelPreparationError::MissingColumn {
			channel: FunnelPreparationChannel::Value,
			field: val_col.to_owned(),
		})?;
	if !values.dtype().is_numeric() {
		return Err(FunnelPreparationError::UncastableColumn {
			channel: FunnelPreparationChannel::Value,
			field: val_col.to_owned(),
		});
	}

	let stages = stages.as_materialized_series();
	let values = values.as_materialized_series();
	let mut rows = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		let stage = match stages.get(row).expect("funnel stage row is in bounds") {
			AnyValue::Null => None,
			AnyValue::String(value) => Some(value.to_owned()),
			AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
			other => Some(other.to_string().replace('"', "")),
		};
		let value = values.get(row).expect("funnel value row is in bounds");
		let original = value.to_string();
		let value = match exact_f64(value) {
			Ok(value) => FunnelValueCell::Exact(value),
			Err(ExactF64Error::Null) => FunnelValueCell::Null,
			Err(ExactF64Error::Inexact) => FunnelValueCell::Inexact(original),
		};
		rows.push(FunnelStageRow { stage, value });
	}
	let stages = FunnelStages::from_rows(stage_col, val_col, rows)
		.map_err(FunnelPreparationError::Stages)?;
	Ok(FunnelPreparedData {
		stages,
		x_label: val_col.to_owned(),
		y_label: stage_col.to_owned(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{PathBuilder, PlotBackend};
	use iced::Size;
	use iced::widget::canvas::Text;

	#[derive(Default)]
	struct FunnelPath {
		points: Vec<Point>,
	}

	impl PathBuilder for FunnelPath {
		fn move_to(&mut self, point: Point) {
			self.points.push(point);
		}
		fn line_to(&mut self, point: Point) {
			self.points.push(point);
		}
		fn arc_to(&mut self, _center: Point, _radius: f32, _start: f32, _end: f32) {}
		fn circle(&mut self, _center: Point, _radius: f32) {}
		fn rectangle(&mut self, _top_left: Point, _size: Size) {}
		fn close(&mut self) {}
	}

	#[derive(Default)]
	struct FunnelBackend {
		fills: Vec<Vec<Point>>,
		texts: Vec<String>,
	}

	impl PlotBackend for FunnelBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), _color: Color) {
			let mut path = FunnelPath::default();
			f(&mut path);
			self.fills.push(path.points);
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}

		fn fill_text(&mut self, text: Text) {
			self.texts.push(text.content);
		}

		fn translate(&mut self, _translation: iced::Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn frame(stages: &[&str], values: &[f64]) -> DataFrame {
		DataFrame::new(
			stages.len(),
			vec![
				Series::new("stage".into(), stages).into(),
				Series::new("value".into(), values).into(),
			],
		)
		.expect("funnel frame")
	}

	fn kernel(frame: &DataFrame) -> FunnelPlotKernel {
		FunnelPlotKernel {
			prepared_data: Arc::new(
				prepare_funnel_data(frame, "stage", "value").expect("valid funnel"),
			),
		}
	}

	fn render(frame: &DataFrame, width: f32, height: f32) -> FunnelBackend {
		let kernel = kernel(frame);
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(width, height));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = FunnelBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		backend
	}

	#[test]
	fn product_0096_preparation_is_field_first_and_admits_boolean_stages() {
		let missing_stage = polars::df!("value" => ["wrong"]).unwrap();
		assert!(matches!(
			prepare_funnel_data(&missing_stage, "stage", "value"),
			Err(FunnelPreparationError::MissingColumn {
				channel: FunnelPreparationChannel::Stage,
				..
			})
		));

		let wrong_stage = polars::df!("stage" => [1_i64]).unwrap();
		assert!(matches!(
			prepare_funnel_data(&wrong_stage, "stage", "value"),
			Err(FunnelPreparationError::UncastableColumn {
				channel: FunnelPreparationChannel::Stage,
				..
			})
		));

		let boolean = polars::df!("stage" => [true, false], "value" => [2_i64, 1]).unwrap();
		let prepared = prepare_funnel_data(&boolean, "stage", "value").expect("boolean stages");
		assert_eq!(
			prepared
				.stages
				.iter()
				.map(|stage| stage.stage())
				.collect::<Vec<_>>(),
			["true", "false"]
		);
	}

	#[test]
	fn product_0096_exact_conversion_accepts_both_representable_boundary_values() {
		for value in [9_007_199_254_740_992_i64, 9_007_199_254_740_994] {
			let data = polars::df!("stage" => ["A"], "value" => [value]).unwrap();
			assert!(prepare_funnel_data(&data, "stage", "value").is_ok());
		}
		let inexact =
			polars::df!("stage" => ["A"], "value" => [9_007_199_254_740_993_i64]).unwrap();
		assert!(matches!(
			prepare_funnel_data(&inexact, "stage", "value"),
			Err(FunnelPreparationError::Stages(
				FunnelStagesError::InexactValue { row: 1, .. }
			))
		));
		let very_large = polars::df!("stage" => ["A"], "value" => [1e300_f64]).unwrap();
		assert!(prepare_funnel_data(&very_large, "stage", "value").is_ok());
	}

	#[test]
	fn product_0096_empty_schema_valid_frame_has_no_marks_or_legend() {
		let data = frame(&[], &[]);
		let kernel = kernel(&data);
		assert!(kernel.prepared_data.stages.is_empty());
		assert_eq!(
			kernel.legend_content(&ResolvedPlotSettings::default()),
			None
		);
		assert!(render(&data, 100.0, 100.0).fills.is_empty());

		let missing = DataFrame::empty();
		assert!(matches!(
			prepare_funnel_data(&missing, "stage", "value"),
			Err(FunnelPreparationError::MissingColumn { .. })
		));
	}

	#[test]
	fn product_0096_subunit_widths_match_equivalent_whole_values() {
		let fraction = render(&frame(&["A", "B", "C"], &[0.8, 0.4, 0.1]), 100.0, 90.0);
		let whole = render(&frame(&["A", "B", "C"], &[80.0, 40.0, 10.0]), 100.0, 90.0);
		assert_eq!(fraction.fills, whole.fills);
		assert_eq!(fraction.fills[0][0], Point::new(10.0, 0.0));
	}

	#[test]
	fn product_0096_f64_geometry_change_and_shared_label_format_are_explicit() {
		assert_eq!(funnel_width(1.0, 3.0, 100.0).to_bits(), 0x41d55555);

		let labels = render(&frame(&["A", "B"], &[0.5, 0.2]), 100.0, 100.0).texts;
		assert_eq!(labels, ["0.5", "0.2"]);
	}

	#[test]
	fn product_0096_zero_predecessor_changes_only_the_stage_conversion() {
		let data = frame(&["A", "B", "C"], &[100.0, 0.0, 0.0]);
		let kernel = kernel(&data);
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(100.0, 300.0));
		let transform = CoordinateTransformer::new(&layout, bounds);
		let hover = kernel
			.hover(&transform, Cursor::Available(Point::new(50.0, 250.0)))
			.expect("zero-width stage centre remains hoverable");
		assert!(hover.contains("Conversion: —"));
		assert!(hover.contains("Total Rate: 0.0%"));
		assert!(!hover.contains("NaN"));
	}
}
