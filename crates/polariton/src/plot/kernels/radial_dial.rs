use crate::coordinate::{Pt, Widget};
use crate::plot::common::{
	CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::plot::kernels::radial_dial_readings::{
	RadialDialNumericCell, RadialDialReadingRow, RadialDialReadings, RadialDialReadingsError,
};
use crate::plot::spec::CategoryColorAssignment;
use iced::advanced::mouse::Cursor;
use iced::{Color, Point, Rectangle};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

pub struct RadialDialPlotKernel {
	pub prepared_data: Arc<RadialDialPreparedData>,
	/// Authored grouped dials carry one resolved assignment per category.
	/// `None` is the legacy/automatic positional compatibility path.
	pub category_colors: Option<Vec<CategoryColorAssignment>>,
}

impl RadialDialPlotKernel {
	fn category_color(&self, index: usize, settings: &ResolvedPlotSettings) -> Color {
		if let Some(assignments) = &self.category_colors {
			let category = self
				.prepared_data
				.readings
				.iter()
				.nth(index)
				.and_then(|reading| reading.category())
				.expect("PRODUCT-0100 authored radial-dial assignments have named categories");
			let assignment = assignments
				.iter()
				.find(|assignment| assignment.category == category)
				.expect(
					"PRODUCT-0100 authored radial-dial categories have complete colour assignments",
				);
			return Color::from_rgba8(
				assignment.color.red,
				assignment.color.green,
				assignment.color.blue,
				assignment.color.alpha as f32 / u8::MAX as f32,
			);
		}
		let total = self.prepared_data.readings.iter().len();
		let position = if total > 1 {
			index as f32 / (total - 1) as f32
		} else {
			0.5
		};
		settings.color_theme.get_color(position)
	}
}

impl PlotKernel for RadialDialPlotKernel {
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
		if self.prepared_data.readings.is_empty() {
			return;
		}
		let num_cats = self.prepared_data.readings.iter().len();
		let center = Point::new(bounds.width / 2.0, bounds.height * 0.8);
		let max_radius = (bounds.width / 2.0).min(bounds.height * 0.7) * 1.0;
		let ring_spacing = 5.0;
		let total_spacing = (num_cats as f32 - 1.0) * ring_spacing;
		let ring_thickness = (max_radius - total_spacing) / num_cats as f32;
		let pi = std::f32::consts::PI;
		let start_angle = pi;
		for (i, reading) in self.prepared_data.readings.iter().enumerate() {
			let val = reading.value();
			let max_val = reading.maximum();
			let ratio = (val / max_val) as f32;
			let total_sweep = pi;
			let sweep = ratio * total_sweep;
			let color = self.category_color(i, &settings);
			let outer_r = max_radius - i as f32 * (ring_thickness + ring_spacing);
			let inner_r = outer_r - ring_thickness;
			backend.fill_path(
				&|builder| {
					builder.arc_to(center, outer_r, start_angle, start_angle + total_sweep);
					builder.arc_to(center, inner_r, start_angle + total_sweep, start_angle);
					builder.close();
				},
				Color::from_rgba(0.5, 0.5, 0.5, 0.1),
			);
			if sweep > 0.001 {
				backend.fill_path(
					&|builder| {
						builder.arc_to(center, outer_r, start_angle, start_angle + sweep);
						builder.arc_to(center, inner_r, start_angle + sweep, start_angle);
						builder.close();
					},
					color,
				);
			}
		}
	}

	fn legend_content(
		&self,
		settings: &ResolvedPlotSettings,
	) -> Option<crate::plot::legend::LegendContent> {
		crate::plot::legend::entries_from_pairs_with_sample_and_provenance(
			self.prepared_data
				.readings
				.iter()
				.enumerate()
				.map(|(index, reading)| {
					(
						reading.category().unwrap_or("All Data").to_owned(),
						self.category_color(index, settings),
					)
				}),
			crate::plot::legend::LegendSample::Swatch,
			crate::plot::legend::LegendProvenance::Named,
		)
	}

	fn hover(&self, transform: &CoordinateTransformer, cursor: Cursor) -> Option<String> {
		if let Some(cursor_pos) = cursor.position() {
			let cursor_pos = Pt::<Widget>::from_plot_kernel_cursor(cursor_pos);
			let (bounds, cursor_pos) = transform.widget_hit_geometry(cursor_pos);
			let center = Point::new(
				bounds.x() + bounds.width() / 2.0,
				bounds.y() + bounds.height() * 0.8,
			);
			let dx = cursor_pos.x() - center.x;
			let dy = cursor_pos.y() - center.y;
			let dist = (dx * dx + dy * dy).sqrt();
			if self.prepared_data.readings.is_empty() {
				return None;
			}
			let num_cats = self.prepared_data.readings.iter().len();
			let max_radius = (bounds.width() / 2.0).min(bounds.height() * 0.7) * 0.9;
			let ring_spacing = 5.0;
			let ring_thickness =
				(max_radius - (num_cats as f32 - 1.0) * ring_spacing) / num_cats as f32;
			if dy > 0.0 {
				return None;
			}
			for i in 0..num_cats {
				let outer_r = max_radius - i as f32 * (ring_thickness + ring_spacing);
				let inner_r = outer_r - ring_thickness;
				if dist >= inner_r && dist <= outer_r {
					let reading = self
						.prepared_data
						.readings
						.iter()
						.nth(i)
						.expect("radial-dial hover index is in bounds");
					let cat = reading.category().unwrap_or("All Data");
					let val = reading.value();
					let max_val = reading.maximum();
					return Some(format!(
						"{}: {:.1} / {:.1} ({:.0}%)",
						cat,
						val,
						max_val,
						(val / max_val * 100.0)
					));
				}
			}
		}
		None
	}
}

#[derive(Debug)]
pub struct RadialDialPreparedData {
	pub readings: RadialDialReadings,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RadialDialPreparationField {
	Category,
	Value,
	Maximum,
}

impl fmt::Display for RadialDialPreparationField {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::Category => "category",
			Self::Value => "value",
			Self::Maximum => "maximum",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RadialDialPreparationError {
	MissingColumn {
		field_role: RadialDialPreparationField,
		field: String,
	},
	UnsupportedColumn {
		field_role: RadialDialPreparationField,
		field: String,
	},
	Readings(RadialDialReadingsError),
}

impl fmt::Display for RadialDialPreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { field_role, field } => {
				write!(
					formatter,
					"radial dial {field_role} field `{field}` is missing"
				)
			}
			Self::UnsupportedColumn { field_role, field } => write!(
				formatter,
				"radial dial {field_role} field `{field}` has an unsupported type"
			),
			Self::Readings(source) => source.fmt(formatter),
		}
	}
}

impl std::error::Error for RadialDialPreparationError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Readings(source) => Some(source),
			Self::MissingColumn { .. } | Self::UnsupportedColumn { .. } => None,
		}
	}
}

pub(crate) fn prepare_radial_dial_data(
	df: &DataFrame,
	category_field: Option<&str>,
	val_col: &str,
	max_val_col: &str,
) -> Result<RadialDialPreparedData, RadialDialPreparationError> {
	let category = if let Some(field) = category_field {
		let column = df
			.column(field)
			.map_err(|_| RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Category,
				field: field.to_owned(),
			})?;
		if !radial_category_dtype(column.dtype()) {
			return Err(RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Category,
				field: field.to_owned(),
			});
		}
		Some(column.as_materialized_series())
	} else {
		None
	};
	let values = df
		.column(val_col)
		.map_err(|_| RadialDialPreparationError::MissingColumn {
			field_role: RadialDialPreparationField::Value,
			field: val_col.to_owned(),
		})?;
	if !values.dtype().is_numeric() {
		return Err(RadialDialPreparationError::UnsupportedColumn {
			field_role: RadialDialPreparationField::Value,
			field: val_col.to_owned(),
		});
	}
	let maxima = df
		.column(max_val_col)
		.map_err(|_| RadialDialPreparationError::MissingColumn {
			field_role: RadialDialPreparationField::Maximum,
			field: max_val_col.to_owned(),
		})?;
	if !maxima.dtype().is_numeric() {
		return Err(RadialDialPreparationError::UnsupportedColumn {
			field_role: RadialDialPreparationField::Maximum,
			field: max_val_col.to_owned(),
		});
	}

	let values = values.as_materialized_series();
	let maxima = maxima.as_materialized_series();
	let mut rows = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		let category = category.map(|series| {
			category_value(
				series
					.get(row)
					.expect("radial-dial category row is in bounds"),
			)
		});
		let value = numeric_cell(values.get(row).expect("radial-dial value row is in bounds"));
		let maximum = numeric_cell(
			maxima
				.get(row)
				.expect("radial-dial maximum row is in bounds"),
		);
		rows.push(RadialDialReadingRow {
			category: category.flatten(),
			value,
			maximum,
		});
	}
	let readings = RadialDialReadings::from_rows(category_field, val_col, max_val_col, rows)
		.map_err(RadialDialPreparationError::Readings)?;
	Ok(RadialDialPreparedData { readings })
}

fn radial_category_dtype(dtype: &DataType) -> bool {
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

fn numeric_cell(value: AnyValue<'_>) -> RadialDialNumericCell {
	let original = value.to_string();
	match exact_f64(value) {
		Ok(value) => RadialDialNumericCell::Exact(value),
		Err(ExactF64Error::Null) => RadialDialNumericCell::Null,
		Err(ExactF64Error::Inexact) => RadialDialNumericCell::Inexact(original),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::PathBuilder;
	use crate::plot::kernels::radial_dial_readings::RadialDialReadingsError;
	use iced::Size;
	use iced::widget::canvas::{Stroke, Text};
	use polars::chunked_array::builder::CategoricalChunkedBuilder;
	use polars::df;

	#[derive(Clone, Copy, Debug, PartialEq)]
	struct ArcOperation {
		center: Point,
		radius: f32,
		start: f32,
		end: f32,
	}

	#[derive(Default)]
	struct RadialDialPath {
		arcs: Vec<ArcOperation>,
	}

	impl PathBuilder for RadialDialPath {
		fn move_to(&mut self, _point: Point) {}
		fn line_to(&mut self, _point: Point) {}
		fn arc_to(&mut self, center: Point, radius: f32, start: f32, end: f32) {
			self.arcs.push(ArcOperation {
				center,
				radius,
				start,
				end,
			});
		}
		fn circle(&mut self, _center: Point, _radius: f32) {}
		fn rectangle(&mut self, _top_left: Point, _size: Size) {}
		fn close(&mut self) {}
	}

	#[derive(Default)]
	struct RadialDialBackend {
		fills: Vec<Vec<ArcOperation>>,
		colors: Vec<Color>,
	}

	impl PlotBackend for RadialDialBackend {
		fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), _stroke: Stroke) {}

		fn fill_path(&mut self, f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			let mut path = RadialDialPath::default();
			f(&mut path);
			self.fills.push(path.arcs);
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

	fn valid_frame() -> DataFrame {
		df!(
			"category" => ["Zulu", "Alpha"],
			"value" => [4.0, 2.0],
			"maximum" => [8.0, 10.0],
		)
		.expect("valid radial-dial frame")
	}

	fn assignment(category: &str, index: usize, color: [u8; 3]) -> CategoryColorAssignment {
		CategoryColorAssignment {
			category: category.to_owned(),
			position: crate::plot::spec::PalettePosition::new(index, 2).unwrap(),
			color: crate::plot::spec::ResolvedColor {
				red: color[0],
				green: color[1],
				blue: color[2],
				alpha: u8::MAX,
			},
		}
	}

	#[test]
	fn product_0100_authored_category_colour_is_one_paint_and_legend_authority() {
		let prepared =
			prepare_radial_dial_data(&valid_frame(), Some("category"), "value", "maximum").unwrap();
		let kernel = RadialDialPlotKernel {
			prepared_data: Arc::new(prepared),
			category_colors: Some(vec![
				assignment("Alpha", 0, [10, 20, 30]),
				assignment("Zulu", 1, [70, 80, 90]),
			]),
		};
		let settings = ResolvedPlotSettings::default();
		let expected = (0..2)
			.map(|index| kernel.category_color(index, &settings))
			.collect::<Vec<_>>();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let transform = CoordinateTransformer::new(&PlotLayout::Radial, bounds);
		let mut backend = RadialDialBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);
		assert_eq!(backend.colors[1], expected[0]);
		assert_eq!(backend.colors[3], expected[1]);
		let legend = kernel.legend_content(&settings).unwrap();
		let crate::plot::legend::LegendSection::Entries(entries) =
			legend.sections().next().unwrap()
		else {
			panic!("radial dial legend is categorical entries")
		};
		assert_eq!(
			entries.iter().map(|entry| entry.swatch).collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn product_0100_unassigned_radial_dial_fallback_remains_positional() {
		let settings = ResolvedPlotSettings::default();
		let first =
			prepare_radial_dial_data(&valid_frame(), Some("category"), "value", "maximum").unwrap();
		let kernel = RadialDialPlotKernel {
			prepared_data: Arc::new(first),
			category_colors: None,
		};
		assert_eq!(
			kernel.category_color(0, &settings),
			settings.color_theme.get_color(0.0)
		);
		assert_eq!(
			kernel.category_color(1, &settings),
			settings.color_theme.get_color(1.0)
		);
	}

	#[test]
	fn product_0100_binding_and_preparation_admit_and_refuse_the_same_role_dtypes() {
		let fields = crate::plot::spec::RadialDialFields {
			value: "value".to_owned(),
			maximum: "maximum".to_owned(),
			label: Some("label".to_owned()),
		};
		let authored = crate::plot::spec::AuthoredPlotSpec::radial_dial(fields);
		let valid = df!("label" => [true], "value" => [1_i64], "maximum" => [2_i64]).unwrap();
		crate::plot::spec::resolve_channels(&authored, valid.schema().as_ref())
			.expect("binding admits the valid role matrix");
		prepare_radial_dial_data(&valid, Some("label"), "value", "maximum")
			.expect("preparation admits the same role matrix");

		for invalid in [
			df!("label" => [1_i64], "value" => [1_i64], "maximum" => [2_i64]).unwrap(),
			df!("label" => ["A"], "value" => ["bad"], "maximum" => [2_i64]).unwrap(),
			df!("label" => ["A"], "value" => [1_i64], "maximum" => ["bad"]).unwrap(),
		] {
			assert!(
				crate::plot::spec::resolve_channels(&authored, invalid.schema().as_ref()).is_err()
			);
			assert!(prepare_radial_dial_data(&invalid, Some("label"), "value", "maximum").is_err());
		}
	}

	#[test]
	fn product_0099_preparation_validates_fields_before_rows_in_field_order() {
		for (category, value, maximum, expected_role) in [
			(
				Some("missing"),
				"value",
				"maximum",
				RadialDialPreparationField::Category,
			),
			(
				Some("category"),
				"missing",
				"maximum",
				RadialDialPreparationField::Value,
			),
			(
				Some("category"),
				"value",
				"missing",
				RadialDialPreparationField::Maximum,
			),
		] {
			let error =
				prepare_radial_dial_data(&valid_frame(), category, value, maximum).unwrap_err();
			assert!(matches!(
				error,
				RadialDialPreparationError::MissingColumn { field_role, .. }
					if field_role == expected_role
			));
		}

		let wrong = df!(
			"category" => [1_i64],
			"value" => ["bad"],
			"maximum" => [false],
		)
		.expect("wrong dtypes");
		for (category, value, maximum, expected_role) in [
			(
				Some("category"),
				"value",
				"maximum",
				RadialDialPreparationField::Category,
			),
			(None, "value", "maximum", RadialDialPreparationField::Value),
		] {
			let error = prepare_radial_dial_data(&wrong, category, value, maximum).unwrap_err();
			assert!(matches!(
				error,
				RadialDialPreparationError::UnsupportedColumn { field_role, .. }
					if field_role == expected_role
			));
		}
		let wrong_maximum = df!(
			"category" => ["A"],
			"value" => [1.0],
			"maximum" => [false],
		)
		.expect("wrong maximum dtype");
		assert!(matches!(
			prepare_radial_dial_data(&wrong_maximum, Some("category"), "value", "maximum",)
				.unwrap_err(),
			RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
		));
	}

	#[test]
	fn product_0099_category_dtype_matrix_is_explicit() {
		assert!(radial_category_dtype(&DataType::String));
		assert!(radial_category_dtype(&DataType::Boolean));
		let categories = Categories::global();
		assert!(radial_category_dtype(&DataType::Categorical(
			categories.clone(),
			categories.mapping(),
		)));
		let frozen = FrozenCategories::new(["A", "B"]).expect("enum categories");
		assert!(radial_category_dtype(&DataType::Enum(
			frozen.clone(),
			frozen.mapping().clone(),
		)));
		assert!(!radial_category_dtype(&DataType::Int64));

		let boolean = df!(
			"category" => [false, true],
			"value" => [1.0, 2.0],
			"maximum" => [10.0, 10.0],
		)
		.expect("boolean categories");
		let prepared = prepare_radial_dial_data(&boolean, Some("category"), "value", "maximum")
			.expect("boolean categories are nominal");
		assert_eq!(
			prepared
				.readings
				.iter()
				.map(|reading| reading.category())
				.collect::<Vec<_>>(),
			[Some("false"), Some("true")]
		);

		let mapping = Categories::global().mapping();
		let mut categorical = CategoricalChunkedBuilder::<Categorical32Type>::new(
			"category".into(),
			DataType::Categorical(Categories::global(), mapping),
		);
		categorical.append_str("Zulu").expect("categorical value");
		categorical.append_str("Alpha").expect("categorical value");
		let categorical = DataFrame::new(
			2,
			vec![
				categorical.finish().into_series().into(),
				Series::new("value".into(), [1.0, 2.0]).into(),
				Series::new("maximum".into(), [10.0, 10.0]).into(),
			],
		)
		.expect("categorical frame");
		assert!(
			prepare_radial_dial_data(&categorical, Some("category"), "value", "maximum",).is_ok()
		);

		let frozen = FrozenCategories::new(["Zulu", "Alpha"]).expect("enum categories");
		let mut enumerated = CategoricalChunkedBuilder::<Categorical8Type>::new(
			"category".into(),
			DataType::Enum(frozen.clone(), frozen.mapping().clone()),
		);
		enumerated.append_str("Zulu").expect("enum value");
		enumerated.append_str("Alpha").expect("enum value");
		let enumerated = DataFrame::new(
			2,
			vec![
				enumerated.finish().into_series().into(),
				Series::new("value".into(), [1.0, 2.0]).into(),
				Series::new("maximum".into(), [10.0, 10.0]).into(),
			],
		)
		.expect("enum frame");
		assert!(
			prepare_radial_dial_data(&enumerated, Some("category"), "value", "maximum",).is_ok()
		);
	}

	#[test]
	fn product_0099_null_nonfinite_and_range_refusals_name_each_field() {
		let nulls = df!(
			"category" => [None::<&str>],
			"value" => [None::<f64>],
			"maximum" => [None::<f64>],
		)
		.expect("null frame");
		assert!(matches!(
			prepare_radial_dial_data(&nulls, Some("category"), "value", "maximum",).unwrap_err(),
			RadialDialPreparationError::Readings(RadialDialReadingsError::NullCategory {
				row: 1,
				..
			})
		));
		assert!(matches!(
			prepare_radial_dial_data(&nulls, None, "value", "maximum").unwrap_err(),
			RadialDialPreparationError::Readings(RadialDialReadingsError::NullValue { row: 1, .. })
		));
		let null_maximum = df!(
			"category" => ["A"],
			"value" => [1.0],
			"maximum" => [None::<f64>],
		)
		.expect("null maximum");
		assert!(matches!(
			prepare_radial_dial_data(&null_maximum, Some("category"), "value", "maximum",)
				.unwrap_err(),
			RadialDialPreparationError::Readings(RadialDialReadingsError::NullMaximum {
				row: 1,
				..
			})
		));

		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let frame = df!(
				"category" => ["A"],
				"value" => [value],
				"maximum" => [10.0],
			)
			.expect("non-finite value");
			assert!(matches!(
				prepare_radial_dial_data(&frame, Some("category"), "value", "maximum",)
					.unwrap_err(),
				RadialDialPreparationError::Readings(RadialDialReadingsError::NonFiniteValue {
					row: 1,
					..
				})
			));
			let frame = df!(
				"category" => ["A"],
				"value" => [1.0],
				"maximum" => [value],
			)
			.expect("non-finite maximum");
			assert!(matches!(
				prepare_radial_dial_data(&frame, Some("category"), "value", "maximum",)
					.unwrap_err(),
				RadialDialPreparationError::Readings(RadialDialReadingsError::NonFiniteMaximum {
					row: 1,
					..
				})
			));
		}

		let excessive = df!(
			"category" => ["A"],
			"value" => [11.0],
			"maximum" => [10.0],
		)
		.expect("excessive value");
		assert!(matches!(
			prepare_radial_dial_data(&excessive, Some("category"), "value", "maximum",)
				.unwrap_err(),
			RadialDialPreparationError::Readings(RadialDialReadingsError::ValueExceedsMaximum {
				row: 1,
				..
			})
		));
	}

	#[test]
	fn product_0099_exactness_is_shared_for_value_and_maximum() {
		for (field, candidate, accepted) in [
			(
				RadialDialPreparationField::Value,
				9_007_199_254_740_992_i64,
				true,
			),
			(
				RadialDialPreparationField::Value,
				9_007_199_254_740_993_i64,
				false,
			),
			(
				RadialDialPreparationField::Value,
				9_007_199_254_740_994_i64,
				true,
			),
			(
				RadialDialPreparationField::Maximum,
				9_007_199_254_740_992_i64,
				true,
			),
			(
				RadialDialPreparationField::Maximum,
				9_007_199_254_740_993_i64,
				false,
			),
			(
				RadialDialPreparationField::Maximum,
				9_007_199_254_740_994_i64,
				true,
			),
		] {
			let (value, maximum) = match field {
				RadialDialPreparationField::Value => (candidate, 9_007_199_254_740_994_i64),
				RadialDialPreparationField::Maximum => (1_i64, candidate),
				RadialDialPreparationField::Category => unreachable!(),
			};
			let frame = df!(
				"category" => ["A"],
				"value" => [value],
				"maximum" => [maximum],
			)
			.expect("integer boundary frame");
			let result = prepare_radial_dial_data(&frame, Some("category"), "value", "maximum");
			assert_eq!(result.is_ok(), accepted, "{field} candidate {candidate}");
		}
	}

	#[test]
	fn product_0099_empty_relations_validate_every_required_field_first() {
		let empty = df!(
			"category" => Vec::<String>::new(),
			"value" => Vec::<f64>::new(),
			"maximum" => Vec::<f64>::new(),
		)
		.expect("empty schema");
		let prepared = prepare_radial_dial_data(&empty, Some("category"), "value", "maximum")
			.expect("complete empty schema");
		assert!(prepared.readings.is_empty());

		for (category, value, maximum, expected_role) in [
			(
				Some("missing"),
				"value",
				"maximum",
				RadialDialPreparationField::Category,
			),
			(
				Some("category"),
				"missing",
				"maximum",
				RadialDialPreparationField::Value,
			),
			(
				Some("category"),
				"value",
				"missing",
				RadialDialPreparationField::Maximum,
			),
			(
				None,
				"missing",
				"maximum",
				RadialDialPreparationField::Value,
			),
			(
				None,
				"value",
				"missing",
				RadialDialPreparationField::Maximum,
			),
		] {
			let error = prepare_radial_dial_data(&empty, category, value, maximum).unwrap_err();
			assert!(matches!(
				error,
				RadialDialPreparationError::MissingColumn { field_role, .. }
					if field_role == expected_role
			));
		}
	}

	#[test]
	fn product_0099_precedence_crosses_preparation_row_and_relation_phases() {
		let missing_maximum_with_null_value = df!(
			"category" => ["A"],
			"value" => [None::<f64>],
		)
		.expect("missing maximum and null value");
		assert!(matches!(
			prepare_radial_dial_data(
				&missing_maximum_with_null_value,
				Some("category"),
				"value",
				"maximum",
			)
			.unwrap_err(),
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
		));

		let missing_category_with_bad_value = df!(
			"value" => ["bad"],
			"maximum" => [10.0],
		)
		.expect("missing category and bad value");
		assert!(matches!(
			prepare_radial_dial_data(
				&missing_category_with_bad_value,
				Some("category"),
				"value",
				"maximum",
			)
			.unwrap_err(),
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Category,
				..
			}
		));
	}

	#[test]
	fn product_0099_valid_grouped_render_operations_match_the_legacy_oracle() {
		let frame = valid_frame();
		let prepared = prepare_radial_dial_data(&frame, Some("category"), "value", "maximum")
			.expect("valid grouped radial dial");
		let kernel = RadialDialPlotKernel {
			prepared_data: Arc::new(prepared),
			category_colors: None,
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = PlotLayout::Radial;
		let transform = CoordinateTransformer::new(&layout, bounds);
		let settings = ResolvedPlotSettings::default();
		let mut backend = RadialDialBackend::default();
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			Cursor::Unavailable,
			settings.clone(),
		);

		assert_eq!(backend.fills.len(), 4);
		assert_eq!(
			backend.colors,
			[
				Color::from_rgba(0.5, 0.5, 0.5, 0.1),
				settings.color_theme.get_color(0.0),
				Color::from_rgba(0.5, 0.5, 0.5, 0.1),
				settings.color_theme.get_color(1.0),
			]
		);
		let pi = std::f32::consts::PI;
		assert_eq!(
			backend.fills,
			[
				vec![
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 70.0,
						start: pi,
						end: 2.0 * pi,
					},
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 37.5,
						start: 2.0 * pi,
						end: pi,
					},
				],
				vec![
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 70.0,
						start: pi,
						end: pi + 0.2 * pi,
					},
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 37.5,
						start: pi + 0.2 * pi,
						end: pi,
					},
				],
				vec![
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 32.5,
						start: pi,
						end: 2.0 * pi,
					},
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 0.0,
						start: 2.0 * pi,
						end: pi,
					},
				],
				vec![
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 32.5,
						start: pi,
						end: pi + 0.5 * pi,
					},
					ArcOperation {
						center: Point::new(100.0, 80.0),
						radius: 0.0,
						start: pi + 0.5 * pi,
						end: pi,
					},
				],
			]
		);
	}

	#[test]
	fn product_0099_empty_and_ungrouped_presentation_remain_distinct() {
		let empty = df!(
			"value" => Vec::<f64>::new(),
			"maximum" => Vec::<f64>::new(),
		)
		.expect("empty ungrouped schema");
		let empty_kernel = RadialDialPlotKernel {
			prepared_data: Arc::new(
				prepare_radial_dial_data(&empty, None, "value", "maximum")
					.expect("empty ungrouped relation"),
			),
			category_colors: None,
		};
		assert!(
			empty_kernel
				.legend_content(&ResolvedPlotSettings::default())
				.is_none()
		);

		let ungrouped = df!(
			"value" => [4.0],
			"maximum" => [8.0],
		)
		.expect("one ungrouped reading");
		let ungrouped_kernel = RadialDialPlotKernel {
			prepared_data: Arc::new(
				prepare_radial_dial_data(&ungrouped, None, "value", "maximum")
					.expect("valid ungrouped relation"),
			),
			category_colors: None,
		};
		let legend = ungrouped_kernel
			.legend_content(&ResolvedPlotSettings::default())
			.expect("ungrouped presentation label");
		let crate::plot::legend::LegendSection::Entries(entries) =
			legend.sections().next().expect("one legend section")
		else {
			panic!("radial dial legend is categorical")
		};
		assert_eq!(entries.first().label, "All Data");
		assert_eq!(
			ungrouped_kernel
				.prepared_data
				.readings
				.iter()
				.next()
				.expect("one reading")
				.category(),
			None
		);
	}
}
