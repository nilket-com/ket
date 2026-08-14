use crate::coordinate::{Pt, Widget};
use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::pie_slices::{PieSliceRow, PieSlices, PieSlicesError, PieValueCell};
use crate::plot::spec::CategoryColorAssignment;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use iced::{Color, Point, Rectangle, Size};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

pub struct PiePlotKernel {
	pub prepared_data: Arc<PiePreparedData>,
	/// Authored grouped pies carry one resolved assignment per category. `None`
	/// is the legacy/automatic positional compatibility path.
	pub category_colors: Option<Vec<CategoryColorAssignment>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PieGeometry<'a> {
	center: Point,
	radius: f32,
	boundaries: &'a [f64],
}

impl<'a> PieGeometry<'a> {
	fn new(size: Size, slices: &'a PieSlices) -> Self {
		Self {
			center: Point::new(size.width / 2.0, size.height / 2.0),
			radius: size.width.min(size.height) / 2.0 * 0.8,
			boundaries: slices.boundaries(),
		}
	}

	fn slice_at(self, point: Point) -> Option<usize> {
		let dx = point.x - self.center.x;
		let dy = point.y - self.center.y;
		if dx == 0.0 && dy == 0.0 {
			return None;
		}
		let distance = dx.hypot(dy);
		if distance > self.radius {
			return None;
		}

		let mut angle = f64::from(dy.atan2(dx));
		if angle < 0.0 {
			angle += std::f64::consts::TAU;
		}
		if angle >= std::f64::consts::TAU {
			angle -= std::f64::consts::TAU;
		}

		self.boundaries.windows(2).position(|pair| {
			let start = pair[0];
			let end = pair[1];
			end > start && angle >= start && angle < end
		})
	}
}

impl PiePlotKernel {
	fn display_label(&self, index: usize) -> String {
		self.prepared_data
			.slices
			.get(index)
			.and_then(|slice| slice.category())
			.map(str::to_owned)
			.unwrap_or_else(|| format!("Row {}", index + 1))
	}

	fn slice_color(&self, index: usize, settings: &ResolvedPlotSettings) -> Color {
		if let Some(assignments) = &self.category_colors {
			let category = self
				.prepared_data
				.slices
				.get(index)
				.and_then(|slice| slice.category())
				.expect("PRODUCT-0103 authored pie assignments have named categories");
			let assignment = assignments
				.iter()
				.find(|assignment| assignment.category == category)
				.expect("PRODUCT-0103 authored pie categories have complete colour assignments");
			return Color::from_rgba8(
				assignment.color.red,
				assignment.color.green,
				assignment.color.blue,
				assignment.color.alpha as f32 / u8::MAX as f32,
			);
		}
		let total = self.prepared_data.slices.iter().len();
		let position = if total > 1 {
			index as f32 / (total - 1) as f32
		} else {
			0.5
		};
		settings.color_theme.get_color(position)
	}
}

impl PlotKernel for PiePlotKernel {
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
		if self.prepared_data.slices.is_empty() {
			return;
		}
		let geometry = PieGeometry::new(bounds.size(), &self.prepared_data.slices);
		for (index, _) in self.prepared_data.slices.iter().enumerate() {
			let start_angle = geometry.boundaries[index] as f32;
			let end_angle = geometry.boundaries[index + 1] as f32;
			if start_angle == end_angle {
				continue;
			}
			let color = self.slice_color(index, &settings);
			let path = &|builder: &mut dyn crate::plot::common::PathBuilder| {
				builder.move_to(geometry.center);
				builder.arc_to(geometry.center, geometry.radius, start_angle, end_angle);
				builder.line_to(geometry.center);
				builder.close();
			};
			backend.fill_path(path, color);
			backend.stroke_path(
				path,
				Stroke {
					style: Style::Solid(settings.mark_style.separator.iced()),
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
		let provenance = if self
			.prepared_data
			.slices
			.get(0)
			.and_then(|slice| slice.category())
			.is_some()
		{
			crate::plot::legend::LegendProvenance::Named
		} else {
			crate::plot::legend::LegendProvenance::Synthetic
		};
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			self.prepared_data
				.slices
				.iter()
				.enumerate()
				.map(|(index, _)| (self.display_label(index), self.slice_color(index, settings))),
			crate::plot::legend::LegendSample::Swatch,
			provenance,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		let cursor = Pt::<Widget>::from_plot_kernel_cursor(cursor.position()?);
		let (bounds, cursor) = transform.widget_hit_geometry(cursor);
		if !bounds.contains(cursor) {
			return None;
		}
		let local = Point::new(cursor.x() - bounds.x(), cursor.y() - bounds.y());
		let geometry = PieGeometry::new(
			Size::new(bounds.width(), bounds.height()),
			&self.prepared_data.slices,
		);
		let index = geometry.slice_at(local)?;
		let slice = self
			.prepared_data
			.slices
			.get(index)
			.expect("pie geometry index is in validated slice bounds");
		let total = self
			.prepared_data
			.slices
			.total()
			.expect("hovered pie is non-empty");
		Some(format!(
			"{}: {:.2} ({:.1}%)",
			self.display_label(index),
			slice.value(),
			slice.value() / total * 100.0
		))
	}
}

#[derive(Debug)]
pub struct PiePreparedData {
	pub slices: PieSlices,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PiePreparationField {
	Category,
	Value,
}

impl fmt::Display for PiePreparationField {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::Category => "category",
			Self::Value => "value",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PiePreparationError {
	MissingColumn {
		field_role: PiePreparationField,
		field: String,
	},
	UnsupportedColumn {
		field_role: PiePreparationField,
		field: String,
	},
	Slices(PieSlicesError),
}

impl fmt::Display for PiePreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { field_role, field } => {
				write!(formatter, "pie {field_role} field `{field}` is missing")
			}
			Self::UnsupportedColumn { field_role, field } => write!(
				formatter,
				"pie {field_role} field `{field}` has an unsupported type"
			),
			Self::Slices(source) => source.fmt(formatter),
		}
	}
}

impl std::error::Error for PiePreparationError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Slices(source) => Some(source),
			Self::MissingColumn { .. } | Self::UnsupportedColumn { .. } => None,
		}
	}
}

pub(crate) fn prepare_pie_data(
	df: &DataFrame,
	category_field: Option<&str>,
	value_field: &str,
) -> Result<PiePreparedData, PiePreparationError> {
	let category = if let Some(field) = category_field {
		let column = df
			.column(field)
			.map_err(|_| PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Category,
				field: field.to_owned(),
			})?;
		if !pie_category_dtype(column.dtype()) {
			return Err(PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Category,
				field: field.to_owned(),
			});
		}
		Some(column.as_materialized_series())
	} else {
		None
	};
	let values = df
		.column(value_field)
		.map_err(|_| PiePreparationError::MissingColumn {
			field_role: PiePreparationField::Value,
			field: value_field.to_owned(),
		})?;
	if !values.dtype().is_numeric() {
		return Err(PiePreparationError::UnsupportedColumn {
			field_role: PiePreparationField::Value,
			field: value_field.to_owned(),
		});
	}

	let values = values.as_materialized_series();
	let mut rows = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		let category = category.map(|series| {
			category_value(
				series
					.get(row)
					.expect("pie category row is in frame bounds"),
			)
		});
		let value = values.get(row).expect("pie value row is in frame bounds");
		let original = value.to_string();
		let value = match exact_f64(value) {
			Ok(value) => PieValueCell::Exact(value),
			Err(ExactF64Error::Null) => PieValueCell::Null,
			Err(ExactF64Error::Inexact) => PieValueCell::Inexact(original),
		};
		rows.push(PieSliceRow {
			category: category.flatten(),
			value,
		});
	}
	let slices = PieSlices::from_rows(category_field, value_field, rows)
		.map_err(PiePreparationError::Slices)?;
	Ok(PiePreparedData { slices })
}

fn pie_category_dtype(dtype: &DataType) -> bool {
	matches!(
		dtype,
		DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) | DataType::Boolean
	)
}

fn category_value(value: AnyValue<'_>) -> Option<String> {
	match value {
		AnyValue::Null => None,
		AnyValue::String(value) => Some(value.to_owned()),
		AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
		other => Some(other.to_string().replace('"', "")),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::PathBuilder;
	use crate::plot::kernels::pie_slices::PieSlicesError;
	use iced::Color;
	use iced::widget::canvas::Text;
	use polars::chunked_array::builder::CategoricalChunkedBuilder;

	#[derive(Clone, Copy, Debug, PartialEq)]
	enum PathOperation {
		Move(Point),
		Line(Point),
		Arc {
			center: Point,
			radius: f32,
			start: f32,
			end: f32,
		},
		Close,
	}

	#[derive(Default)]
	struct PiePath {
		operations: Vec<PathOperation>,
	}

	impl PathBuilder for PiePath {
		fn move_to(&mut self, point: Point) {
			self.operations.push(PathOperation::Move(point));
		}

		fn line_to(&mut self, point: Point) {
			self.operations.push(PathOperation::Line(point));
		}

		fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
			self.operations.push(PathOperation::Arc {
				center,
				radius,
				start,
				end,
			});
		}

		fn circle(&mut self, _center: Point, _radius: f32) {}
		fn rectangle(&mut self, _top_left: Point, _size: Size) {}
		fn close(&mut self) {
			self.operations.push(PathOperation::Close);
		}
	}

	#[derive(Default)]
	struct PieBackend {
		fills: Vec<Vec<PathOperation>>,
		colors: Vec<Color>,
		strokes: Vec<Vec<PathOperation>>,
		stroke_colors: Vec<Color>,
		stroke_widths: Vec<f32>,
	}

	impl PlotBackend for PieBackend {
		fn stroke_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let mut path = PiePath::default();
			f(&mut path);
			let Style::Solid(color) = stroke.style else {
				panic!("pie test backend expects solid strokes")
			};
			self.strokes.push(path.operations);
			self.stroke_colors.push(color);
			self.stroke_widths.push(stroke.width);
		}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			let mut path = PiePath::default();
			f(&mut path);
			self.fills.push(path.operations);
			self.colors.push(color);
		}

		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}
		fn fill_text(&mut self, _text: Text) {}
		fn translate(&mut self, _translation: iced::Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
		fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
			f(self);
		}
	}

	fn frame(categories: &[&str], values: &[f64]) -> DataFrame {
		DataFrame::new(
			categories.len(),
			vec![
				Series::new("category".into(), categories).into(),
				Series::new("value".into(), values).into(),
			],
		)
		.expect("pie frame")
	}

	fn kernel(data: &DataFrame, category: Option<&str>) -> PiePlotKernel {
		PiePlotKernel {
			prepared_data: Arc::new(
				prepare_pie_data(data, category, "value").expect("valid pie data"),
			),
			category_colors: None,
		}
	}

	fn render(kernel: &PiePlotKernel, bounds: Rectangle) -> PieBackend {
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = PieBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings,
		);
		backend
	}

	fn hover(kernel: &PiePlotKernel, bounds: Rectangle, point: Point) -> Option<String> {
		let settings = ResolvedPlotSettings::default();
		let layout = kernel.layout(settings);
		let transform = CoordinateTransformer::new(&layout, bounds);
		kernel.hover(&transform, Cursor::Available(point))
	}

	#[test]
	fn product_0103_authored_category_colour_is_one_paint_and_legend_authority() {
		let mut kernel = kernel(&frame(&["A", "B"], &[2.0, 1.0]), Some("category"));
		kernel.category_colors = Some(vec![
			CategoryColorAssignment {
				category: "A".to_owned(),
				position: crate::plot::spec::PalettePosition::new(0, 2).unwrap(),
				color: crate::plot::spec::ResolvedColor {
					red: 1,
					green: 2,
					blue: 3,
					alpha: 255,
				},
			},
			CategoryColorAssignment {
				category: "B".to_owned(),
				position: crate::plot::spec::PalettePosition::new(1, 2).unwrap(),
				color: crate::plot::spec::ResolvedColor {
					red: 9,
					green: 8,
					blue: 7,
					alpha: 255,
				},
			},
		]);
		let backend = render(
			&kernel,
			Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0)),
		);
		let legend = kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("authored pie legend");
		let crate::plot::legend::LegendSection::Entries(entries) =
			legend.sections().next().unwrap()
		else {
			panic!("pie entries")
		};
		assert_eq!(
			backend.colors,
			entries.iter().map(|entry| entry.swatch).collect::<Vec<_>>()
		);
		assert!(
			entries
				.iter()
				.all(|entry| { entry.provenance == crate::plot::legend::LegendProvenance::Named })
		);
	}

	#[test]
	fn product_0103_synthetic_rows_are_emitted_then_shared_policy_decides_omission() {
		let one = kernel(&frame(&["ignored"], &[1.0]), None);
		let content = one
			.legend_content(&ResolvedPlotSettings::default())
			.expect("kernel emits lone synthetic entry");
		let crate::plot::legend::LegendSection::Entries(entries) =
			content.sections().next().unwrap()
		else {
			panic!("pie entries")
		};
		assert_eq!(entries.first().label, "Row 1");
		assert_eq!(
			entries.first().provenance,
			crate::plot::legend::LegendProvenance::Synthetic
		);
		assert!(crate::plot::legend::apply_policy(content).is_none());

		let two = kernel(&frame(&["ignored", "ignored"], &[2.0, 1.0]), None);
		let content = two
			.legend_content(&ResolvedPlotSettings::default())
			.expect("kernel emits two synthetic entries");
		let crate::plot::legend::LegendSection::Entries(entries) =
			content.sections().next().unwrap()
		else {
			panic!("pie entries")
		};
		assert_eq!(entries.iter().count(), 2);
		assert!(
			entries.iter().all(|entry| {
				entry.provenance == crate::plot::legend::LegendProvenance::Synthetic
			})
		);
		assert!(crate::plot::legend::apply_policy(content).is_some());
	}

	#[test]
	fn product_0103_opposite_relation_sorts_reverse_the_rendered_slice_order() {
		let assignments = [
			("A", [10, 11, 12]),
			("B", [20, 21, 22]),
			("C", [30, 31, 32]),
		]
		.into_iter()
		.enumerate()
		.map(
			|(index, (category, [red, green, blue]))| CategoryColorAssignment {
				category: category.to_owned(),
				position: crate::plot::spec::PalettePosition::new(index, 3).unwrap(),
				color: crate::plot::spec::ResolvedColor {
					red,
					green,
					blue,
					alpha: 255,
				},
			},
		)
		.collect::<Vec<_>>();
		let rendered = |categories: &[&str], values: &[f64]| {
			let mut kernel = kernel(&frame(categories, values), Some("category"));
			kernel.category_colors = Some(assignments.clone());
			render(
				&kernel,
				Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0)),
			)
			.colors
		};
		let ascending = rendered(&["A", "B", "C"], &[1.0, 2.0, 3.0]);
		let descending = rendered(&["C", "B", "A"], &[3.0, 2.0, 1.0]);
		assert_eq!(
			ascending.iter().copied().rev().collect::<Vec<_>>(),
			descending
		);
	}

	#[test]
	fn product_0102_preparation_is_field_first_and_relation_order_is_preserved() {
		let missing = polars::df!("value" => [None::<f64>]).unwrap();
		assert!(matches!(
			prepare_pie_data(&missing, Some("category"), "value"),
			Err(PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Category,
				..
			})
		));

		let wrong = polars::df!(
			"category" => [1_i64],
			"value" => ["bad"],
		)
		.unwrap();
		assert!(matches!(
			prepare_pie_data(&wrong, Some("category"), "value"),
			Err(PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Category,
				..
			})
		));
		assert!(matches!(
			prepare_pie_data(&wrong, None, "value"),
			Err(PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Value,
				..
			})
		));

		let prepared = prepare_pie_data(
			&frame(&["Zulu", "Alpha", "Middle"], &[3.0, 2.0, 1.0]),
			Some("category"),
			"value",
		)
		.expect("valid order fixture");
		assert_eq!(
			prepared
				.slices
				.iter()
				.map(|slice| slice.category().unwrap())
				.collect::<Vec<_>>(),
			["Zulu", "Alpha", "Middle"]
		);
		let ordered_kernel = kernel(
			&frame(&["Zulu", "Alpha", "Middle"], &[1.0, 1.0, 1.0]),
			Some("category"),
		);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		for (point, expected) in [
			(Point::new(100.0, 75.0), "Zulu:"),
			(Point::new(78.35, 37.5), "Alpha:"),
			(Point::new(121.65, 37.5), "Middle:"),
		] {
			assert!(
				hover(&ordered_kernel, bounds, point)
					.expect("ordered slice hover")
					.starts_with(expected)
			);
		}
	}

	#[test]
	fn product_0102_category_dtype_matrix_is_explicit() {
		assert!(pie_category_dtype(&DataType::String));
		assert!(pie_category_dtype(&DataType::Boolean));
		let categories = Categories::global();
		assert!(pie_category_dtype(&DataType::Categorical(
			categories.clone(),
			categories.mapping(),
		)));
		let frozen = FrozenCategories::new(["A", "B"]).expect("enum categories");
		assert!(pie_category_dtype(&DataType::Enum(
			frozen.clone(),
			frozen.mapping().clone(),
		)));
		assert!(!pie_category_dtype(&DataType::Int64));

		let boolean = polars::df!("category" => [true], "value" => [1.0]).unwrap();
		assert!(prepare_pie_data(&boolean, Some("category"), "value").is_ok());

		let mapping = Categories::global().mapping();
		let mut categorical = CategoricalChunkedBuilder::<Categorical32Type>::new(
			"category".into(),
			DataType::Categorical(Categories::global(), mapping),
		);
		categorical.append_str("A").unwrap();
		let categorical = DataFrame::new(
			1,
			vec![
				categorical.finish().into_series().into(),
				Series::new("value".into(), [1.0]).into(),
			],
		)
		.unwrap();
		assert!(prepare_pie_data(&categorical, Some("category"), "value").is_ok());

		let frozen = FrozenCategories::new(["A", "B"]).unwrap();
		let mut enumerated = CategoricalChunkedBuilder::<Categorical8Type>::new(
			"category".into(),
			DataType::Enum(frozen.clone(), frozen.mapping().clone()),
		);
		enumerated.append_str("A").unwrap();
		let enumerated = DataFrame::new(
			1,
			vec![
				enumerated.finish().into_series().into(),
				Series::new("value".into(), [1.0]).into(),
			],
		)
		.unwrap();
		assert!(prepare_pie_data(&enumerated, Some("category"), "value").is_ok());
	}

	#[test]
	fn product_0102_null_literal_null_exactness_and_numeric_refusals_are_distinct() {
		let null_category = polars::df!(
			"category" => [None::<&str>],
			"value" => [1.0],
		)
		.unwrap();
		assert!(matches!(
			prepare_pie_data(&null_category, Some("category"), "value"),
			Err(PiePreparationError::Slices(PieSlicesError::NullCategory {
				row: 1,
				..
			}))
		));
		let literal = frame(&["null"], &[1.0]);
		assert_eq!(
			prepare_pie_data(&literal, Some("category"), "value")
				.unwrap()
				.slices
				.get(0)
				.unwrap()
				.category(),
			Some("null")
		);

		let null_value = polars::df!(
			"category" => ["A"],
			"value" => [None::<f64>],
		)
		.unwrap();
		assert!(matches!(
			prepare_pie_data(&null_value, Some("category"), "value"),
			Err(PiePreparationError::Slices(PieSlicesError::NullValue {
				row: 1,
				..
			}))
		));

		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			assert!(matches!(
				prepare_pie_data(&frame(&["A"], &[value]), Some("category"), "value"),
				Err(PiePreparationError::Slices(
					PieSlicesError::NonFiniteValue { row: 1, .. }
				))
			));
		}
		assert!(matches!(
			prepare_pie_data(&frame(&["A"], &[-1.0]), Some("category"), "value"),
			Err(PiePreparationError::Slices(PieSlicesError::NegativeValue {
				row: 1,
				..
			}))
		));

		for value in [9_007_199_254_740_992_i64, 9_007_199_254_740_994] {
			let data = polars::df!("category" => ["A"], "value" => [value]).unwrap();
			assert!(prepare_pie_data(&data, Some("category"), "value").is_ok());
		}
		let inexact = polars::df!(
			"category" => ["A"],
			"value" => [9_007_199_254_740_993_i64],
		)
		.unwrap();
		assert!(matches!(
			prepare_pie_data(&inexact, Some("category"), "value"),
			Err(PiePreparationError::Slices(PieSlicesError::InexactValue {
				row: 1,
				..
			}))
		));
		assert!(prepare_pie_data(&frame(&["A"], &[1e300]), Some("category"), "value").is_ok());
	}

	#[test]
	fn product_0102_empty_and_zero_total_have_distinct_outcomes() {
		let empty = DataFrame::new(
			0,
			vec![
				Series::new_empty("category".into(), &DataType::String).into(),
				Series::new_empty("value".into(), &DataType::Float64).into(),
			],
		)
		.unwrap();
		let prepared = prepare_pie_data(&empty, Some("category"), "value")
			.expect("empty required columns are an empty result");
		assert!(prepared.slices.is_empty());
		let kernel = PiePlotKernel {
			prepared_data: Arc::new(prepared),
			category_colors: None,
		};
		assert!(
			render(
				&kernel,
				Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0))
			)
			.fills
			.is_empty()
		);
		assert!(
			kernel
				.legend_content(&ResolvedPlotSettings::default())
				.is_none()
		);

		assert!(matches!(
			prepare_pie_data(&frame(&["A", "B"], &[0.0, 0.0]), Some("category"), "value"),
			Err(PiePreparationError::Slices(
				PieSlicesError::NonPositiveTotal { .. }
			))
		));
		let missing = DataFrame::empty();
		assert!(matches!(
			prepare_pie_data(&missing, None, "value"),
			Err(PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Value,
				..
			})
		));
	}

	#[test]
	fn product_0102_zero_slice_draws_nothing_stays_in_legend_and_never_hovers() {
		let kernel = kernel(&frame(&["zero", "full"], &[0.0, 5.0]), Some("category"));
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let backend = render(&kernel, bounds);
		assert_eq!(backend.fills.len(), 1);
		let legend = kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("zero measurement remains in legend");
		let crate::plot::legend::LegendSection::Entries(entries) =
			legend.sections().next().unwrap()
		else {
			panic!("pie legend entries")
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| entry.label.as_str())
				.collect::<Vec<_>>(),
			["zero", "full"]
		);
		assert!(
			hover(&kernel, bounds, Point::new(125.0, 50.0))
				.unwrap()
				.starts_with("full:")
		);
	}

	#[test]
	fn product_0102_ungrouped_repeated_rows_render_as_independent_slices() {
		let data = polars::df!("value" => [2.0_f64, 2.0]).unwrap();
		let kernel = kernel(&data, None);
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		assert_eq!(render(&kernel, bounds).fills.len(), 2);
		let legend = kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("ungrouped rows retain presentation labels");
		let crate::plot::legend::LegendSection::Entries(entries) =
			legend.sections().next().unwrap()
		else {
			panic!("pie legend entries")
		};
		assert_eq!(
			entries
				.iter()
				.map(|entry| entry.label.as_str())
				.collect::<Vec<_>>(),
			["Row 1", "Row 2"]
		);
	}

	#[test]
	fn product_0102_hover_uses_the_same_local_geometry_as_paint() {
		let one = kernel(&frame(&["A"], &[1.0]), Some("category"));
		let origin = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		assert!(
			hover(&one, origin, Point::new(125.0, 50.0))
				.unwrap()
				.starts_with("A:")
		);
		assert!(hover(&one, origin, Point::new(100.0, 50.0)).is_none());
		assert!(
			hover(&one, origin, Point::new(100.001, 50.0))
				.unwrap()
				.starts_with("A:")
		);
		assert!(hover(&one, origin, Point::new(141.0, 50.0)).is_none());

		let two = kernel(&frame(&["A", "B"], &[1.0, 1.0]), Some("category"));
		assert!(
			hover(&two, origin, Point::new(125.0, 60.0))
				.unwrap()
				.starts_with("A:")
		);
		assert!(
			hover(&two, origin, Point::new(125.0, 40.0))
				.unwrap()
				.starts_with("B:")
		);

		let shifted = Rectangle::new(Point::new(50.0, 30.0), Size::new(200.0, 100.0));
		let backend = render(&one, shifted);
		assert!(matches!(
			backend.fills[0][0],
			PathOperation::Move(point) if point == Point::new(100.0, 50.0)
		));
		assert!(
			hover(&one, shifted, Point::new(175.0, 80.0))
				.unwrap()
				.starts_with("A:")
		);
	}

	#[test]
	fn product_0102_preaggregated_toron_shape_keeps_product_0101_origin_parity() {
		let kernel = kernel(&frame(&["A", "B"], &[5.0, 5.0]), Some("category"));
		let settings = ResolvedPlotSettings::default();
		let backend = render(
			&kernel,
			Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0)),
		);
		assert_eq!(backend.fills.len(), 2);
		assert_eq!(backend.strokes, backend.fills);
		assert_eq!(backend.stroke_widths, [1.0, 1.0]);
		assert_eq!(
			backend.stroke_colors,
			[
				settings.chrome.canvas_background.iced(),
				settings.chrome.canvas_background.iced(),
			]
		);
		assert_eq!(
			backend.colors,
			[
				settings.color_theme.get_color(0.0),
				settings.color_theme.get_color(1.0),
			]
		);
		assert_eq!(
			backend.fills[0],
			vec![
				PathOperation::Move(Point::new(100.0, 50.0)),
				PathOperation::Arc {
					center: Point::new(100.0, 50.0),
					radius: 40.0,
					start: 0.0,
					end: std::f32::consts::PI,
				},
				PathOperation::Line(Point::new(100.0, 50.0)),
				PathOperation::Close,
			]
		);
		assert_eq!(
			backend.fills[1],
			vec![
				PathOperation::Move(Point::new(100.0, 50.0)),
				PathOperation::Arc {
					center: Point::new(100.0, 50.0),
					radius: 40.0,
					start: std::f32::consts::PI,
					end: std::f32::consts::TAU,
				},
				PathOperation::Line(Point::new(100.0, 50.0)),
				PathOperation::Close,
			]
		);
	}
}
