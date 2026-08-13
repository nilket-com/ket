use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	format_label, polars_type_to_axis_type,
};
use crate::plot::kernels::bubble_observations::{
	BubbleFieldNames, BubbleObservationRow, BubbleObservations, BubbleObservationsError,
};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Rectangle};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

pub struct BubblePlotKernel {
	pub prepared_data: Arc<BubblePreparedData>,
}

#[derive(Debug, PartialEq)]
pub struct BubblePreparedData {
	pub observations: BubbleObservations,
	pub x_axis_type: AxisType,
	pub y_axis_type: AxisType,
	pub x_label: String,
	pub y_label: String,
	pub size_label: String,
	pub color_label: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BubblePreparationChannel {
	X,
	Y,
	Size,
	Color,
	Label,
}

impl fmt::Display for BubblePreparationChannel {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Y => "y",
			Self::Size => "size",
			Self::Color => "color",
			Self::Label => "label",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BubblePreparationError {
	MissingColumn {
		channel: BubblePreparationChannel,
		field: String,
	},
	UncastableColumn {
		channel: BubblePreparationChannel,
		field: String,
	},
	Observations(BubbleObservationsError),
}

impl fmt::Display for BubblePreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { channel, field } => {
				write!(formatter, "bubble {channel} field `{field}` is missing")
			}
			Self::UncastableColumn { channel, field } => write!(
				formatter,
				"bubble {channel} field `{field}` cannot convert to a quantitative value"
			),
			Self::Observations(source) => source.fmt(formatter),
		}
	}
}

impl std::error::Error for BubblePreparationError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Observations(source) => Some(source),
			Self::MissingColumn { .. } | Self::UncastableColumn { .. } => None,
		}
	}
}

impl PlotKernel for BubblePlotKernel {
	fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
		PlotLayout::Cartesian {
			x_range: (
				settings
					.x_min
					.unwrap_or(self.prepared_data.observations.x_range().0),
				settings
					.x_max
					.unwrap_or(self.prepared_data.observations.x_range().1),
			),
			y_range: (
				settings
					.y_min
					.unwrap_or(self.prepared_data.observations.y_range().0),
				settings
					.y_max
					.unwrap_or(self.prepared_data.observations.y_range().1),
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
		settings: ResolvedPlotSettings,
	) {
		let color_min = self.prepared_data.observations.color_range().0;
		let color_max = self.prepared_data.observations.color_range().1;
		let color_delta = (color_max - color_min).max(f64::EPSILON);
		let size_min = self.prepared_data.observations.size_range().0;
		let size_max = self.prepared_data.observations.size_range().1;
		let size_delta = (size_max - size_min).max(f64::EPSILON);
		for point in self.prepared_data.observations.iter() {
			let p = transform.cartesian(point.x(), point.y());
			let t_color = ((point.color() - color_min) / color_delta) as f32;
			let color = settings.color_theme.get_color(t_color);
			let t_size = ((point.size() - size_min) / size_delta) as f32;
			let radius = 2.0 + t_size * 28.0;
			backend.fill_path(
				&|builder| {
					builder.circle(p, radius);
				},
				Color { a: 0.7, ..color },
			);
			backend.stroke_path(
				&|builder| {
					builder.circle(p, radius);
				},
				Stroke {
					style: Style::Solid(Color { a: 0.9, ..color }),
					width: 1.0,
					..Default::default()
				},
			);
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		if self.prepared_data.observations.is_empty() {
			return None;
		}
		let size_range = self.prepared_data.observations.size_range();
		let size = crate::plot::legend::LegendSection::SizeSample {
			label: self.prepared_data.size_label.clone(),
			low: size_range.0,
			high: size_range.1,
		};
		let color_range = self.prepared_data.observations.color_range();
		let content = if let Some(gradient) = crate::plot::legend::gradient_section(
			self.prepared_data.color_label.clone(),
			color_range.0,
			color_range.1,
			settings.color_theme,
		) {
			let mut content =
				crate::plot::legend::LegendContent(crate::plot::spec::NonEmpty::new(gradient));
			content.push(size);
			content
		} else {
			crate::plot::legend::LegendContent(crate::plot::spec::NonEmpty::new(size))
		};
		Some(content)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let size_min = self.prepared_data.observations.size_range().0;
			let size_max = self.prepared_data.observations.size_range().1;
			let size_delta = (size_max - size_min).max(f64::EPSILON);
			for point in self.prepared_data.observations.iter() {
				let p = transform.cartesian(point.x(), point.y());
				let dx = cursor_pos.x - p.x;
				let dy = cursor_pos.y - p.y;
				let dist_sq = dx * dx + dy * dy;
				let t_size = ((point.size() - size_min) / size_delta) as f32;
				let radius = 2.0 + t_size * 28.0;
				if dist_sq <= radius * radius {
					let mut lines = Vec::with_capacity(5);
					if let Some(label) = point.label() {
						lines.push(label.to_owned());
					}
					lines.push(format!(
						"{}: {}",
						self.prepared_data.x_label,
						format_label(point.x(), self.prepared_data.x_axis_type)
					));
					lines.push(format!("{}: {:.2}", self.prepared_data.y_label, point.y()));
					lines.push(format!(
						"{}: {:.2}",
						self.prepared_data.size_label,
						point.size()
					));
					lines.push(format!(
						"{}: {:.2}",
						self.prepared_data.color_label,
						point.color()
					));
					return Some(lines.join("\n"));
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

pub fn prepare_bubble_data(
	df: &DataFrame,
	x_col: &str,
	y_col: &str,
	size_col: &str,
	color_col: &str,
	label_col: Option<&str>,
) -> Result<BubblePreparedData, BubblePreparationError> {
	fn quantitative_column(
		df: &DataFrame,
		channel: BubblePreparationChannel,
		field: &str,
	) -> Result<(Float64Chunked, DataType), BubblePreparationError> {
		let source = df
			.column(field)
			.map_err(|_| BubblePreparationError::MissingColumn {
				channel,
				field: field.to_owned(),
			})?;
		let source_nulls = source.null_count();
		let dtype = source.dtype().clone();
		let cast = source.cast(&DataType::Float64).map_err(|_| {
			BubblePreparationError::UncastableColumn {
				channel,
				field: field.to_owned(),
			}
		})?;
		if cast.null_count() > source_nulls {
			return Err(BubblePreparationError::UncastableColumn {
				channel,
				field: field.to_owned(),
			});
		}
		let values = cast
			.f64()
			.cloned()
			.map_err(|_| BubblePreparationError::UncastableColumn {
				channel,
				field: field.to_owned(),
			})?;
		Ok((values, dtype))
	}

	let (x_f64, x_dtype) = quantitative_column(df, BubblePreparationChannel::X, x_col)?;
	let (y_f64, y_dtype) = quantitative_column(df, BubblePreparationChannel::Y, y_col)?;
	let (size_f64, _) = quantitative_column(df, BubblePreparationChannel::Size, size_col)?;
	let (color_f64, _) = quantitative_column(df, BubblePreparationChannel::Color, color_col)?;
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let y_axis_type = polars_type_to_axis_type(&y_dtype);
	let labels = match label_col {
		Some(field) => {
			let column = df
				.column(field)
				.map_err(|_| BubblePreparationError::MissingColumn {
					channel: BubblePreparationChannel::Label,
					field: field.to_owned(),
				})?;
			column
				.as_materialized_series()
				.iter()
				.map(|value| match value {
					AnyValue::Null => None,
					AnyValue::String(value) => Some(value.to_owned()),
					AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
					other => Some(other.to_string().replace('"', "")),
				})
				.collect::<Vec<_>>()
		}
		None => (0..df.height())
			.map(|index| Some(format!("Point {index}")))
			.collect(),
	};
	let rows = (0..df.height())
		.map(|row| BubbleObservationRow {
			x: x_f64.get(row),
			y: y_f64.get(row),
			size: size_f64.get(row),
			color: color_f64.get(row),
			label: labels[row].clone(),
		})
		.collect();
	let observations = BubbleObservations::from_rows(
		BubbleFieldNames::new(x_col, y_col, size_col, color_col),
		rows,
	)
	.map_err(BubblePreparationError::Observations)?;
	Ok(BubblePreparedData {
		observations,
		x_axis_type,
		y_axis_type,
		x_label: x_col.to_string(),
		y_label: y_col.to_string(),
		size_label: size_col.to_string(),
		color_label: color_col.to_string(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::colors::ColorTheme;
	use crate::plot::common::PathBuilder;
	use crate::plot::gradient_stops::GradientStops;
	use crate::plot::legend::LegendSection;
	use iced::widget::canvas::Text;
	use iced::{Point, Size, Vector};

	#[derive(Clone, Debug, PartialEq)]
	enum CircleOperation {
		Fill {
			center: Point,
			radius: f32,
			color: Color,
		},
		Stroke {
			center: Point,
			radius: f32,
			color: Color,
			width: f32,
		},
	}

	#[derive(Default)]
	struct CirclePath {
		circles: Vec<(Point, f32)>,
	}

	impl PathBuilder for CirclePath {
		fn move_to(&mut self, _point: Point) {}
		fn line_to(&mut self, _point: Point) {}
		fn arc_to(&mut self, _center: Point, _radius: f32, _start_angle: f32, _end_angle: f32) {}
		fn circle(&mut self, center: Point, radius: f32) {
			self.circles.push((center, radius));
		}
		fn rectangle(&mut self, _top_left: Point, _size: Size) {}
		fn close(&mut self) {}
	}

	#[derive(Default)]
	struct CircleBackend {
		operations: Vec<CircleOperation>,
	}

	impl PlotBackend for CircleBackend {
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let mut path = CirclePath::default();
			f(&mut path);
			let color = match stroke.style {
				Style::Solid(color) => color,
				Style::Gradient(_) => panic!("banked bubble strokes are solid"),
			};
			self.operations
				.extend(
					path.circles
						.into_iter()
						.map(|(center, radius)| CircleOperation::Stroke {
							center,
							radius,
							color,
							width: stroke.width,
						}),
				);
		}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			let mut path = CirclePath::default();
			f(&mut path);
			self.operations
				.extend(
					path.circles
						.into_iter()
						.map(|(center, radius)| CircleOperation::Fill {
							center,
							radius,
							color,
						}),
				);
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}
		fn fill_text(&mut self, _text: Text) {}
		fn translate(&mut self, _translation: Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn well_formed_frame() -> DataFrame {
		df!(
			"x" => [0.0, 10.0],
			"y" => [0.0, 10.0],
			"size" => [1.0, 3.0],
			"color" => [10.0, 50.0],
			"label" => ["A", "B"],
		)
		.expect("banked bubble frame")
	}

	fn prepared_kernel(frame: &DataFrame, color: &str, label: Option<&str>) -> BubblePlotKernel {
		BubblePlotKernel {
			prepared_data: Arc::new(
				prepare_bubble_data(frame, "x", "y", "size", color, label)
					.expect("valid banked bubble preparation"),
			),
		}
	}

	fn render(kernel: &BubblePlotKernel) -> CircleBackend {
		let settings = ResolvedPlotSettings {
			color_theme: ColorTheme::Viridis,
			..ResolvedPlotSettings::default()
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(120.0, 120.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = CircleBackend::default();
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
	fn product_0090_stage_zero_banks_well_formed_draw_list_and_hover() {
		let kernel = prepared_kernel(&well_formed_frame(), "color", Some("label"));
		assert_eq!(kernel.prepared_data.observations.x_range(), (-1.0, 11.0));
		assert_eq!(kernel.prepared_data.observations.y_range(), (-1.0, 11.0));
		assert_eq!(kernel.prepared_data.observations.size_range(), (1.0, 3.0));
		assert_eq!(
			kernel.prepared_data.observations.color_range(),
			(10.0, 50.0)
		);

		let backend = render(&kernel);
		assert_eq!(
			backend.operations,
			[
				CircleOperation::Fill {
					center: Point::new(10.0, 110.0),
					radius: 2.0,
					color: Color::from_rgba(0.267, 0.004, 0.329, 0.7),
				},
				CircleOperation::Stroke {
					center: Point::new(10.0, 110.0),
					radius: 2.0,
					color: Color::from_rgba(0.267, 0.004, 0.329, 0.9),
					width: 1.0,
				},
				CircleOperation::Fill {
					center: Point::new(110.0, 10.0),
					radius: 30.0,
					color: Color::from_rgba(0.993, 0.906, 0.145, 0.7),
				},
				CircleOperation::Stroke {
					center: Point::new(110.0, 10.0),
					radius: 30.0,
					color: Color::from_rgba(0.993, 0.906, 0.145, 0.9),
					width: 1.0,
				},
			]
		);

		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings);
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(120.0, 120.0)),
		);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(Point::new(10.0, 110.0)))
				.as_deref(),
			Some("A\nx: 0\ny: 0.00\nsize: 1.00\ncolor: 10.00")
		);
	}

	#[test]
	fn product_0090_null_and_failed_conversion_now_refuse_the_banked_fabrications() {
		let null_color = df!(
			"x" => [0.0, 10.0],
			"y" => [0.0, 10.0],
			"size" => [1.0, 3.0],
			"color" => [None, Some(50.0)],
		)
		.expect("null-colour bank");
		assert!(matches!(
			prepare_bubble_data(&null_color, "x", "y", "size", "color", None),
			Err(BubblePreparationError::Observations(
				BubbleObservationsError::NullValue {
					channel: crate::plot::kernels::bubble_observations::BubbleNumericField::Color,
					field,
					row: 1,
				}
			)) if field == "color"
		));

		let uncastable_color = df!(
			"x" => [0.0, 10.0],
			"y" => [0.0, 10.0],
			"size" => [1.0, 3.0],
			"color" => ["low", "high"],
		)
		.expect("uncastable-colour bank");
		assert_eq!(
			prepare_bubble_data(&uncastable_color, "x", "y", "size", "color", None),
			Err(BubblePreparationError::UncastableColumn {
				channel: BubblePreparationChannel::Color,
				field: "color".to_owned(),
			})
		);
	}

	#[test]
	fn product_0090_missing_numeric_column_now_refuses_the_banked_panic() {
		let missing_color = df!(
			"x" => [0.0, 10.0],
			"y" => [0.0, 10.0],
			"size" => [1.0, 3.0],
		)
		.expect("missing-colour bank");
		assert_eq!(
			prepare_bubble_data(&missing_color, "x", "y", "size", "color", None),
			Err(BubblePreparationError::MissingColumn {
				channel: BubblePreparationChannel::Color,
				field: "color".to_owned(),
			})
		);
	}

	#[test]
	fn product_0090_preparation_is_per_field_and_wraps_observation_errors_unchanged() {
		let missing_color_with_null_x = df!(
			"x" => [None, Some(2.0)],
			"y" => [1.0, 2.0],
			"size" => [1.0, 3.0],
		)
		.expect("column-before-row precedence fixture");
		assert_eq!(
			prepare_bubble_data(&missing_color_with_null_x, "x", "y", "size", "color", None),
			Err(BubblePreparationError::MissingColumn {
				channel: BubblePreparationChannel::Color,
				field: "color".to_owned(),
			})
		);

		let uncastable_x_missing_y = df!(
			"x" => ["left", "right"],
			"size" => [1.0, 3.0],
			"color" => [10.0, 50.0],
		)
		.expect("cross-column precedence fixture");
		assert_eq!(
			prepare_bubble_data(&uncastable_x_missing_y, "x", "y", "size", "color", None),
			Err(BubblePreparationError::UncastableColumn {
				channel: BubblePreparationChannel::X,
				field: "x".to_owned(),
			})
		);

		let null_x = df!(
			"x" => [None, Some(2.0)],
			"y" => [1.0, 2.0],
			"size" => [1.0, 3.0],
			"color" => [10.0, 50.0],
		)
		.expect("wrapped observation fixture");
		let direct = BubbleObservations::from_rows(
			BubbleFieldNames::new("x", "y", "size", "color"),
			vec![
				BubbleObservationRow {
					x: None,
					y: Some(1.0),
					size: Some(1.0),
					color: Some(10.0),
					label: Some("Point 0".to_owned()),
				},
				BubbleObservationRow {
					x: Some(2.0),
					y: Some(2.0),
					size: Some(3.0),
					color: Some(50.0),
					label: Some("Point 1".to_owned()),
				},
			],
		)
		.unwrap_err();
		assert_eq!(
			prepare_bubble_data(&null_x, "x", "y", "size", "color", None),
			Err(BubblePreparationError::Observations(direct))
		);
	}

	#[test]
	fn product_0090_null_label_is_absent_and_hover_keeps_quantitative_lines() {
		let frame = df!(
			"x" => [0.0, 10.0],
			"y" => [0.0, 10.0],
			"size" => [1.0, 3.0],
			"color" => [10.0, 50.0],
			"label" => [None, Some("B")],
		)
		.expect("null-label bank");
		let kernel = prepared_kernel(&frame, "color", Some("label"));
		assert_eq!(
			kernel
				.prepared_data
				.observations
				.iter()
				.next()
				.expect("first observation")
				.label(),
			None
		);

		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(
			&layout,
			Rectangle::new(Point::ORIGIN, Size::new(120.0, 120.0)),
		);
		assert_eq!(
			kernel
				.hover(&transform, Cursor::Available(Point::new(10.0, 110.0)))
				.as_deref(),
			Some("x: 0\ny: 0.00\nsize: 1.00\ncolor: 10.00")
		);
	}

	#[test]
	fn product_0090_only_unselected_labels_generate_placeholders() {
		let frame = well_formed_frame()
			.drop("label")
			.expect("banked frame drops its label column");
		assert_eq!(
			prepare_bubble_data(&frame, "x", "y", "size", "color", Some("label")),
			Err(BubblePreparationError::MissingColumn {
				channel: BubblePreparationChannel::Label,
				field: "label".to_owned(),
			})
		);
		let kernel = prepared_kernel(&frame, "color", None);
		assert_eq!(
			kernel
				.prepared_data
				.observations
				.iter()
				.map(|point| point.label().expect("generated placeholder"))
				.collect::<Vec<_>>(),
			["Point 0", "Point 1"]
		);
	}

	#[test]
	fn product_0090_empty_schema_is_validated_and_invented_guides_are_omitted() {
		let valid_empty = df!(
			"x" => Vec::<f64>::new(),
			"y" => Vec::<f64>::new(),
			"size" => Vec::<f64>::new(),
			"color" => Vec::<f64>::new(),
		)
		.expect("schema-valid empty bank");
		let valid = prepared_kernel(&valid_empty, "color", None);
		assert_eq!(valid.prepared_data.observations.x_range(), (0.0, 1.0));
		assert_eq!(valid.prepared_data.observations.y_range(), (0.0, 1.0));
		assert_eq!(valid.prepared_data.observations.size_range(), (0.0, 0.0));
		assert_eq!(valid.prepared_data.observations.color_range(), (0.0, 0.0));
		assert_eq!(valid.legend_content(&ResolvedPlotSettings::default()), None);

		let missing_color_empty = df!(
			"x" => Vec::<f64>::new(),
			"y" => Vec::<f64>::new(),
			"size" => Vec::<f64>::new(),
		)
		.expect("schema-invalid empty bank");
		assert_eq!(
			prepare_bubble_data(&missing_color_empty, "x", "y", "size", "color", None),
			Err(BubblePreparationError::MissingColumn {
				channel: BubblePreparationChannel::Color,
				field: "color".to_owned(),
			})
		);
	}

	fn kernel(color_range: (f64, f64)) -> BubblePlotKernel {
		let observations = BubbleObservations::from_rows(
			BubbleFieldNames::new("x", "y", "size", "color"),
			vec![
				BubbleObservationRow {
					x: Some(0.0),
					y: Some(0.0),
					size: Some(1.0),
					color: Some(color_range.0),
					label: Some("A".to_owned()),
				},
				BubbleObservationRow {
					x: Some(1.0),
					y: Some(1.0),
					size: Some(3.0),
					color: Some(color_range.1),
					label: Some("B".to_owned()),
				},
			],
		)
		.expect("valid bubble guide fixture");
		BubblePlotKernel {
			prepared_data: Arc::new(BubblePreparedData {
				observations,
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_label: "x".to_owned(),
				y_label: "y".to_owned(),
				size_label: "size".to_owned(),
				color_label: "color".to_owned(),
			}),
		}
	}

	fn stops(section: &LegendSection) -> &GradientStops {
		let LegendSection::Gradient { stops, .. } = section else {
			panic!("gradient section")
		};
		stops
	}

	#[test]
	fn product_0087_bubble_omits_only_degenerate_colour_guides() {
		let content = kernel((4.0, 4.0))
			.legend_content(&ResolvedPlotSettings::default())
			.expect("size guide remains");
		assert_eq!(content.0.len(), 1);
		assert!(matches!(
			content.0.first(),
			LegendSection::SizeSample { .. }
		));
	}

	#[test]
	fn product_0087_bubble_asymmetric_diverging_neutral_remains_at_twenty_five() {
		let settings = ResolvedPlotSettings {
			color_theme: ColorTheme::GreenPurples,
			..ResolvedPlotSettings::default()
		};
		let content = kernel((-50.0, 100.0))
			.legend_content(&settings)
			.expect("gradient and size guides");
		let stops = stops(content.0.first());
		assert_eq!(stops.values().collect::<Vec<_>>(), [-50.0, 25.0, 100.0]);
		assert_eq!(stops.sample_normalized(0.5), Color::WHITE);
		assert_eq!(settings.color_theme.get_color(0.5), Color::WHITE);
	}
}
