use crate::plot::common::{
	AxisType, CoordinateTransformer, PlotBackend, PlotKernel, PlotLayout, ResolvedPlotSettings,
	TemporalAxisDomain, format_label, polars_type_to_axis_type, temporal_axis_domain,
};
use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use iced::Rectangle;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style};
use polars::prelude::*;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FillBetweenRole {
	X,
	Lower,
	Upper,
	Middle,
}

impl fmt::Display for FillBetweenRole {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Lower => "lower",
			Self::Upper => "upper",
			Self::Middle => "middle",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum FillBetweenPreparationError {
	MissingColumn {
		role: FillBetweenRole,
		field: String,
	},
	TemporalColumn {
		role: FillBetweenRole,
		field: String,
		dtype: DataType,
	},
	UnsupportedColumn {
		role: FillBetweenRole,
		field: String,
		dtype: DataType,
	},
	NullValue {
		role: FillBetweenRole,
		field: String,
		row: usize,
	},
	NonFiniteValue {
		role: FillBetweenRole,
		field: String,
		row: usize,
		value: f64,
	},
	InexactValue {
		role: FillBetweenRole,
		field: String,
		row: usize,
		value: String,
	},
	UnorderedX {
		role: FillBetweenRole,
		field: String,
		previous_row: usize,
		row: usize,
	},
	CrossedBand {
		role: FillBetweenRole,
		field: String,
		row: usize,
		lower: f64,
		upper: f64,
	},
	MiddleOutsideBand {
		role: FillBetweenRole,
		field: String,
		row: usize,
		lower: f64,
		middle: f64,
		upper: f64,
	},
}

#[allow(dead_code)] // PRODUCT-0146 maps the carried role and field to authored spans.
impl FillBetweenPreparationError {
	pub fn role(&self) -> FillBetweenRole {
		match self {
			Self::MissingColumn { role, .. }
			| Self::TemporalColumn { role, .. }
			| Self::UnsupportedColumn { role, .. }
			| Self::NullValue { role, .. }
			| Self::NonFiniteValue { role, .. }
			| Self::InexactValue { role, .. }
			| Self::UnorderedX { role, .. }
			| Self::CrossedBand { role, .. }
			| Self::MiddleOutsideBand { role, .. } => *role,
		}
	}

	pub fn field(&self) -> &str {
		match self {
			Self::MissingColumn { field, .. }
			| Self::TemporalColumn { field, .. }
			| Self::UnsupportedColumn { field, .. }
			| Self::NullValue { field, .. }
			| Self::NonFiniteValue { field, .. }
			| Self::InexactValue { field, .. }
			| Self::UnorderedX { field, .. }
			| Self::CrossedBand { field, .. }
			| Self::MiddleOutsideBand { field, .. } => field,
		}
	}
}

impl fmt::Display for FillBetweenPreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { role, field } => {
				write!(formatter, "fill-between {role} field `{field}` is absent")
			}
			Self::TemporalColumn { role, field, dtype } => write!(
				formatter,
				"fill-between {role} field `{field}` requires a quantitative dtype, got temporal {dtype:?}"
			),
			Self::UnsupportedColumn { role, field, dtype } => {
				let requirement = if *role == FillBetweenRole::X {
					"a quantitative or temporal dtype"
				} else {
					"a quantitative dtype"
				};
				write!(
					formatter,
					"fill-between {role} field `{field}` requires {requirement}, got {dtype:?}"
				)
			}
			Self::NullValue { role, field, row } => write!(
				formatter,
				"fill-between {role} field `{field}` is null at row {row}"
			),
			Self::NonFiniteValue {
				role,
				field,
				row,
				value,
			} => write!(
				formatter,
				"fill-between {role} field `{field}` is non-finite at row {row}: {value}"
			),
			Self::InexactValue {
				role,
				field,
				row,
				value,
			} => write!(
				formatter,
				"fill-between {role} field `{field}` cannot be represented exactly at row {row}: {value}"
			),
			Self::UnorderedX {
				field,
				previous_row,
				row,
				..
			} => write!(
				formatter,
				"fill-between x field `{field}` must increase strictly: row {row} is not after row {previous_row}"
			),
			Self::CrossedBand {
				row, lower, upper, ..
			} => write!(
				formatter,
				"fill-between band crosses at row {row}: lower {lower} exceeds upper {upper}"
			),
			Self::MiddleOutsideBand {
				row,
				lower,
				middle,
				upper,
				..
			} => write!(
				formatter,
				"fill-between middle escapes the band at row {row}: {lower} <= {middle} <= {upper} does not hold"
			),
		}
	}
}

impl std::error::Error for FillBetweenPreparationError {}

pub struct FillBetweenPlotKernel {
	pub prepared_data: Arc<FillBetweenPreparedData>,
}

impl PlotKernel for FillBetweenPlotKernel {
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
			x_axis_type: self.prepared_data.x_axis_type.clone(),
			y_axis_type: AxisType::Linear,
			x_temporal_domain: self.prepared_data.x_temporal_domain.clone(),
			y_temporal_domain: None,
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
		if self.prepared_data.x.is_empty() {
			return;
		}
		backend.fill_path(
			&|builder| {
				for (index, &x) in self.prepared_data.x.iter().enumerate() {
					let point = transform.cartesian(x, self.prepared_data.y_upper[index]);
					if index == 0 {
						builder.move_to(point);
					} else {
						builder.line_to(point);
					}
				}
				for (index, &x) in self.prepared_data.x.iter().enumerate().rev() {
					builder.line_to(transform.cartesian(x, self.prepared_data.y_lower[index]));
				}
				builder.close();
			},
			settings.mark_style.fill.iced(),
		);
		if let Some(middle) = &self.prepared_data.y_mid {
			backend.stroke_path(
				&|builder| {
					for (index, (&x, &y)) in self.prepared_data.x.iter().zip(middle).enumerate() {
						let point = transform.cartesian(x, y);
						if index == 0 {
							builder.move_to(point);
						} else {
							builder.line_to(point);
						}
					}
				},
				Stroke {
					style: Style::Solid(settings.mark_style.fill_detail.iced()),
					width: 2.5,
					..Default::default()
				},
			);
		}
	}

	fn hit(
		&self,
		transform: &CoordinateTransformer,
		cursor: Cursor,
	) -> Option<crate::plot::common::PlotHit> {
		let cursor_pos = cursor.position()?;
		let (x, y) = transform.pixel_to_cartesian(cursor_pos)?;
		let xs = &self.prepared_data.x;
		if xs.is_empty() {
			return None;
		}
		let insertion = xs.partition_point(|value| *value < x);
		let index = if insertion < xs.len() && xs[insertion] == x {
			insertion
		} else if insertion == 0 {
			0
		} else if insertion == xs.len() {
			xs.len() - 1
		} else if (xs[insertion] - x).abs() < (xs[insertion - 1] - x).abs() {
			insertion
		} else {
			insertion - 1
		};
		let hit = crate::plot::common::PlotHit::new(
			crate::plot::common::MarkFamily::FILL_BETWEEN,
			crate::plot::common::HitDimensionality::Area,
			index,
		)
		.labeled(
			"X",
			crate::plot::common::axis_hit_semantic_type(&self.prepared_data.x_axis_type),
			crate::plot::common::axis_hit_value(
				xs[index],
				&self.prepared_data.x_axis_type,
				format_label(xs[index], &self.prepared_data.x_axis_type),
			),
		);
		let hit = if let Some(middle) = &self.prepared_data.y_mid {
			hit.labeled(
				"Mid",
				crate::plot::spec::SemanticType::Quantitative,
				crate::plot::common::HitValue::quantitative(
					middle[index],
					format!("{:.2}", middle[index]),
				),
			)
		} else {
			hit
		};
		Some(
			hit.labeled(
				"Range",
				crate::plot::spec::SemanticType::Quantitative,
				crate::plot::common::HitValue::Interval {
					start: Box::new(crate::plot::common::HitValue::quantitative(
						self.prepared_data.y_lower[index],
						format!("{:.2}", self.prepared_data.y_lower[index]),
					)),
					end: Box::new(crate::plot::common::HitValue::quantitative(
						self.prepared_data.y_upper[index],
						format!("{:.2}", self.prepared_data.y_upper[index]),
					)),
					end_inclusive: true,
				},
			)
			.labeled(
				"Cursor Y",
				crate::plot::spec::SemanticType::Quantitative,
				crate::plot::common::HitValue::quantitative(y, format!("{y:.2}")),
			),
		)
	}

	fn x_label(&self) -> String {
		self.prepared_data.x_label.clone()
	}

	fn y_label(&self) -> String {
		self.prepared_data.y_label.clone()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct FillBetweenPreparedData {
	pub x: Vec<f64>,
	pub y_mid: Option<Vec<f64>>,
	pub y_lower: Vec<f64>,
	pub y_upper: Vec<f64>,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
	pub x_axis_type: AxisType,
	pub x_temporal_domain: Option<TemporalAxisDomain>,
	pub x_label: String,
	pub y_label: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OrderedX {
	Quantitative(f64),
	Temporal(i64),
}

impl OrderedX {
	fn strictly_after(self, previous: Self) -> bool {
		match (self, previous) {
			(Self::Quantitative(current), Self::Quantitative(previous)) => current > previous,
			(Self::Temporal(current), Self::Temporal(previous)) => current > previous,
			_ => unreachable!("one fill-between x column has one semantic type"),
		}
	}
}

fn temporal_dtype(dtype: &DataType) -> bool {
	matches!(
		dtype,
		DataType::Date | DataType::Datetime(_, _) | DataType::Time
	)
}

fn temporal_cell(value: AnyValue<'_>) -> Option<i64> {
	match value {
		AnyValue::Date(value) => Some(i64::from(value)),
		AnyValue::Datetime(value, _, _) | AnyValue::DatetimeOwned(value, _, _) => Some(value),
		AnyValue::Time(value) => Some(value),
		_ => None,
	}
}

fn quantitative_cell(
	series: &Series,
	row_index: usize,
	role: FillBetweenRole,
	field: &str,
) -> Result<f64, FillBetweenPreparationError> {
	let value = series
		.get(row_index)
		.expect("fill-between row is in dataframe bounds");
	let original = value.to_string();
	let value = match exact_f64(value) {
		Ok(value) => value,
		Err(ExactF64Error::Null) => {
			return Err(FillBetweenPreparationError::NullValue {
				role,
				field: field.to_owned(),
				row: row_index + 1,
			});
		}
		Err(ExactF64Error::Inexact) => {
			return Err(FillBetweenPreparationError::InexactValue {
				role,
				field: field.to_owned(),
				row: row_index + 1,
				value: original,
			});
		}
	};
	if !value.is_finite() {
		return Err(FillBetweenPreparationError::NonFiniteValue {
			role,
			field: field.to_owned(),
			row: row_index + 1,
			value,
		});
	}
	Ok(value)
}

pub fn prepare_fill_between_data(
	df: &DataFrame,
	x_field: &str,
	lower_field: &str,
	upper_field: &str,
	middle_field: Option<&str>,
) -> Result<FillBetweenPreparedData, FillBetweenPreparationError> {
	let x_column = df
		.column(x_field)
		.map_err(|_| FillBetweenPreparationError::MissingColumn {
			role: FillBetweenRole::X,
			field: x_field.to_owned(),
		})?;
	let x_dtype = x_column.dtype().clone();
	if !x_dtype.is_numeric() && !temporal_dtype(&x_dtype) {
		return Err(FillBetweenPreparationError::UnsupportedColumn {
			role: FillBetweenRole::X,
			field: x_field.to_owned(),
			dtype: x_dtype,
		});
	}

	let mut y_columns = Vec::with_capacity(3);
	for (role, field) in [
		(FillBetweenRole::Lower, Some(lower_field)),
		(FillBetweenRole::Upper, Some(upper_field)),
		(FillBetweenRole::Middle, middle_field),
	] {
		let Some(field) = field else { continue };
		let column = df
			.column(field)
			.map_err(|_| FillBetweenPreparationError::MissingColumn {
				role,
				field: field.to_owned(),
			})?;
		if temporal_dtype(column.dtype()) {
			return Err(FillBetweenPreparationError::TemporalColumn {
				role,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		if !column.dtype().is_numeric() {
			return Err(FillBetweenPreparationError::UnsupportedColumn {
				role,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		y_columns.push((role, field, column.as_materialized_series()));
	}

	let x_series = x_column.as_materialized_series();
	let x_is_temporal = temporal_dtype(&x_dtype);
	let mut x = Vec::with_capacity(df.height());
	let mut ordered_x = Vec::with_capacity(df.height());
	let mut y_lower = Vec::with_capacity(df.height());
	let mut y_upper = Vec::with_capacity(df.height());
	let mut y_mid = middle_field.map(|_| Vec::with_capacity(df.height()));

	for row_index in 0..df.height() {
		if x_is_temporal {
			let value = x_series
				.get(row_index)
				.expect("fill-between temporal x row is in dataframe bounds");
			if matches!(value, AnyValue::Null) {
				return Err(FillBetweenPreparationError::NullValue {
					role: FillBetweenRole::X,
					field: x_field.to_owned(),
					row: row_index + 1,
				});
			}
			let typed =
				temporal_cell(value).expect("validated temporal dtype yields temporal cells");
			x.push(typed as f64);
			ordered_x.push(OrderedX::Temporal(typed));
		} else {
			let value = quantitative_cell(x_series, row_index, FillBetweenRole::X, x_field)?;
			x.push(value);
			ordered_x.push(OrderedX::Quantitative(value));
		}

		let lower = quantitative_cell(
			y_columns[0].2,
			row_index,
			FillBetweenRole::Lower,
			lower_field,
		)?;
		let upper = quantitative_cell(
			y_columns[1].2,
			row_index,
			FillBetweenRole::Upper,
			upper_field,
		)?;
		y_lower.push(lower);
		y_upper.push(upper);
		if let (Some(middle), Some(field)) = (&mut y_mid, middle_field) {
			middle.push(quantitative_cell(
				y_columns[2].2,
				row_index,
				FillBetweenRole::Middle,
				field,
			)?);
		}
	}

	for row_index in 0..df.height() {
		if row_index > 0 && !ordered_x[row_index].strictly_after(ordered_x[row_index - 1]) {
			return Err(FillBetweenPreparationError::UnorderedX {
				role: FillBetweenRole::X,
				field: x_field.to_owned(),
				previous_row: row_index,
				row: row_index + 1,
			});
		}
		if y_lower[row_index] > y_upper[row_index] {
			return Err(FillBetweenPreparationError::CrossedBand {
				role: FillBetweenRole::Lower,
				field: lower_field.to_owned(),
				row: row_index + 1,
				lower: y_lower[row_index],
				upper: y_upper[row_index],
			});
		}
		if let (Some(middle), Some(field)) = (&y_mid, middle_field)
			&& !(y_lower[row_index]..=y_upper[row_index]).contains(&middle[row_index])
		{
			return Err(FillBetweenPreparationError::MiddleOutsideBand {
				role: FillBetweenRole::Middle,
				field: field.to_owned(),
				row: row_index + 1,
				lower: y_lower[row_index],
				middle: middle[row_index],
				upper: y_upper[row_index],
			});
		}
	}

	let ranges = if x.is_empty() {
		((0.0, 1.0), (0.0, 1.0))
	} else {
		let x_min = x.iter().copied().fold(f64::INFINITY, f64::min);
		let x_max = x.iter().copied().fold(f64::NEG_INFINITY, f64::max);
		let y_min = y_lower.iter().copied().fold(f64::INFINITY, f64::min);
		let y_max = y_upper.iter().copied().fold(f64::NEG_INFINITY, f64::max);
		let x_pad = (x_max - x_min).max(0.1) * 0.001;
		let y_pad = (y_max - y_min).max(0.1) * 0.001;
		(
			(x_min - x_pad, x_max + x_pad),
			(y_min - y_pad, y_max + y_pad),
		)
	};
	let x_axis_type = polars_type_to_axis_type(&x_dtype);
	let x_temporal_domain = temporal_axis_domain(x_column, &x_axis_type);
	Ok(FillBetweenPreparedData {
		x,
		y_mid,
		y_lower,
		y_upper,
		x_range: ranges.0,
		y_range: ranges.1,
		x_axis_type,
		x_temporal_domain,
		x_label: x_field.to_owned(),
		y_label: middle_field
			.map(str::to_owned)
			.unwrap_or_else(|| format!("{lower_field} / {upper_field}")),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{HitValue, PathBuilder};
	use iced::widget::canvas::Text;
	use iced::{Color, Point, Size, Vector};

	fn numeric_frame() -> DataFrame {
		df!(
			"x" => [1.0_f64, 2.0, 3.0],
			"lower" => [0.0_f64, 1.0, 2.0],
			"upper" => [2.0_f64, 3.0, 4.0],
			"middle" => [1.0_f64, 2.0, 3.0]
		)
		.expect("fill-between fixture")
	}

	fn prepare(
		frame: &DataFrame,
		middle: Option<&str>,
	) -> Result<FillBetweenPreparedData, FillBetweenPreparationError> {
		prepare_fill_between_data(frame, "x", "lower", "upper", middle)
	}

	#[test]
	fn product_0145_optional_middle_empty_and_axis_identity_are_honest() {
		let frame = numeric_frame();
		let band = prepare(&frame, None).expect("band-only preparation");
		assert_eq!(band.y_mid, None);
		assert_eq!(band.y_label, "lower / upper");
		let middle = prepare(&frame, Some("middle")).expect("with-middle preparation");
		assert_eq!(middle.y_mid.as_deref(), Some(&[1.0, 2.0, 3.0][..]));
		assert_eq!(middle.y_label, "middle");

		let empty = frame.head(Some(0));
		let empty = prepare(&empty, None).expect("schema-valid empty preparation");
		assert!(empty.x.is_empty());
		assert_eq!(empty.x_range, (0.0, 1.0));
		assert_eq!(empty.y_range, (0.0, 1.0));
		let kernel = FillBetweenPlotKernel {
			prepared_data: Arc::new(empty),
		};
		assert_eq!(kernel.y_label(), "lower / upper");
		assert!(matches!(
			kernel.layout(ResolvedPlotSettings::default()),
			PlotLayout::Cartesian {
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				y_axis_type: AxisType::Linear,
				y_temporal_domain: None,
				..
			}
		));
	}

	#[test]
	fn product_0145_validation_precedes_structure_and_carries_typed_roles() {
		let frame = df!(
			"x" => [2.0_f64, 1.0],
			"lower" => [Some(0.0_f64), None],
			"upper" => [2.0_f64, 3.0]
		)
		.expect("fault precedence fixture");
		let error = prepare(&frame, None).expect_err("cell fault precedes x ordering");
		assert!(matches!(
			error,
			FillBetweenPreparationError::NullValue {
				role: FillBetweenRole::Lower,
				row: 2,
				..
			}
		));
		assert_eq!(error.role(), FillBetweenRole::Lower);
		assert_eq!(error.field(), "lower");

		let error = prepare_fill_between_data(&frame, "missing", "lower", "upper", None)
			.expect_err("missing x refuses first");
		assert!(matches!(
			error,
			FillBetweenPreparationError::MissingColumn {
				role: FillBetweenRole::X,
				..
			}
		));
	}

	#[test]
	fn product_0145_column_and_numeric_cell_faults_are_role_attributed() {
		let temporal = Column::new("lower".into(), [1_i32, 2_i32])
			.cast(&DataType::Date)
			.unwrap();
		let frame = DataFrame::new(
			2,
			vec![
				Column::new("x".into(), [1.0_f64, 2.0]),
				temporal,
				Column::new("upper".into(), [2.0_f64, 3.0]),
			],
		)
		.unwrap();
		assert!(matches!(
			prepare(&frame, None),
			Err(FillBetweenPreparationError::TemporalColumn {
				role: FillBetweenRole::Lower,
				..
			})
		));

		let unsupported = df!(
			"x" => [1.0_f64, 2.0],
			"lower" => ["a", "b"],
			"upper" => [2.0_f64, 3.0]
		)
		.unwrap();
		assert!(matches!(
			prepare(&unsupported, None),
			Err(FillBetweenPreparationError::UnsupportedColumn {
				role: FillBetweenRole::Lower,
				..
			})
		));

		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let nonfinite = df!(
				"x" => [1.0_f64],
				"lower" => [value],
				"upper" => [2.0_f64]
			)
			.unwrap();
			assert!(matches!(
				prepare(&nonfinite, None),
				Err(FillBetweenPreparationError::NonFiniteValue {
					role: FillBetweenRole::Lower,
					row: 1,
					..
				})
			));
		}

		let inexact = df!(
			"x" => [9_007_199_254_740_993_i64],
			"lower" => [0.0_f64],
			"upper" => [2.0_f64]
		)
		.unwrap();
		assert!(matches!(
			prepare(&inexact, None),
			Err(FillBetweenPreparationError::InexactValue {
				role: FillBetweenRole::X,
				row: 1,
				..
			})
		));
	}

	#[test]
	fn product_0145_structure_refuses_order_crossing_and_escape_but_allows_edges() {
		let unordered = df!(
			"x" => [1.0_f64, 1.0],
			"lower" => [0.0_f64, 0.0],
			"upper" => [2.0_f64, 2.0]
		)
		.unwrap();
		assert!(matches!(
			prepare(&unordered, None),
			Err(FillBetweenPreparationError::UnorderedX {
				previous_row: 1,
				row: 2,
				..
			})
		));

		let crossed = df!(
			"x" => [1.0_f64],
			"lower" => [3.0_f64],
			"upper" => [2.0_f64]
		)
		.unwrap();
		assert!(matches!(
			prepare(&crossed, None),
			Err(FillBetweenPreparationError::CrossedBand { row: 1, .. })
		));

		let escaped = df!(
			"x" => [1.0_f64],
			"lower" => [0.0_f64],
			"upper" => [2.0_f64],
			"middle" => [3.0_f64]
		)
		.unwrap();
		assert!(matches!(
			prepare(&escaped, Some("middle")),
			Err(FillBetweenPreparationError::MiddleOutsideBand { row: 1, .. })
		));

		let edges = df!(
			"x" => [1.0_f64, 2.0],
			"lower" => [1.0_f64, 2.0],
			"upper" => [1.0_f64, 4.0],
			"middle" => [1.0_f64, 4.0]
		)
		.unwrap();
		assert!(prepare(&edges, Some("middle")).is_ok());
	}

	#[test]
	fn product_0145_temporal_order_uses_typed_values_and_collapsed_hit_is_first() {
		let first = 1_704_067_200_000_000_000_i64;
		let zone = TimeZone::opt_try_new(Some("America/New_York"))
			.expect("zone syntax")
			.expect("named zone");
		let datetime = Column::new("when".into(), [first, first + 1])
			.cast(&DataType::Datetime(TimeUnit::Nanoseconds, Some(zone)))
			.expect("datetime column");
		let frame = DataFrame::new(
			2,
			vec![
				datetime,
				Column::new("lower".into(), [0.0_f64, 1.0]),
				Column::new("upper".into(), [2.0_f64, 3.0]),
			],
		)
		.unwrap();
		let prepared = prepare_fill_between_data(&frame, "when", "lower", "upper", None)
			.expect("adjacent nanoseconds validate on typed values");
		assert_eq!(prepared.x[0], prepared.x[1]);
		assert_eq!(
			prepared.x_temporal_domain,
			Some(TemporalAxisDomain::Datetime {
				first_tick: first,
				last_tick: first + 1,
				unit: crate::plot::common::TimeUnit::Nanoseconds,
				zone: Some(Arc::from("America/New_York")),
			})
		);
		let kernel = FillBetweenPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let cursor = transform.cartesian(first as f64, 1.0);
		let hit = kernel
			.hit(&transform, Cursor::Available(cursor))
			.expect("collapsed temporal coordinate remains hittable");
		assert_eq!(hit.sibling_ordinal, 0);
	}

	#[derive(Default)]
	struct PaintBank {
		fills: Vec<Color>,
		strokes: Vec<(Color, f32)>,
	}

	impl PlotBackend for PaintBank {
		fn stroke_path(&mut self, _path: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
			let Style::Solid(color) = stroke.style else {
				panic!("PRODUCT-0145 uses a solid middle stroke")
			};
			self.strokes.push((color, stroke.width));
		}
		fn fill_path(&mut self, _path: &dyn Fn(&mut dyn PathBuilder), color: Color) {
			self.fills.push(color);
		}
		fn fill_rectangle(&mut self, _top_left: Point, _size: Size, _color: Color) {}
		fn fill_text_raw(&mut self, _text: Text) {}
		fn translate(&mut self, _translation: Vector) {}
		fn rotate(&mut self, _angle: f32) {}
		fn with_save(&mut self, callback: &mut dyn FnMut(&mut dyn PlotBackend)) {
			callback(self);
		}
		fn with_clip(
			&mut self,
			_bounds: Rectangle,
			callback: &mut dyn FnMut(&mut dyn PlotBackend),
		) {
			callback(self);
		}
	}

	fn paint(prepared: FillBetweenPreparedData, settings: ResolvedPlotSettings) -> PaintBank {
		let kernel = FillBetweenPlotKernel {
			prepared_data: Arc::new(prepared),
		};
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut bank = PaintBank::default();
		kernel.plot(&mut bank, bounds, &transform, Cursor::Unavailable, settings);
		bank
	}

	#[test]
	fn product_0145_paint_uses_resolved_opaque_roles_and_optional_stroke() {
		let frame = numeric_frame();
		let settings = ResolvedPlotSettings::default();
		let with_middle = paint(prepare(&frame, Some("middle")).unwrap(), settings.clone());
		assert_eq!(with_middle.fills, [settings.mark_style.fill.iced()]);
		assert_eq!(
			with_middle.strokes,
			[(settings.mark_style.fill_detail.iced(), 2.5)]
		);
		let band_only = paint(prepare(&frame, None).unwrap(), settings);
		assert_eq!(band_only.fills.len(), 1);
		assert!(band_only.strokes.is_empty());
	}

	#[test]
	fn product_0145_hit_and_legend_are_middle_honest() {
		for middle in [None, Some("middle")] {
			let kernel = FillBetweenPlotKernel {
				prepared_data: Arc::new(prepare(&numeric_frame(), middle).unwrap()),
			};
			assert!(
				kernel
					.legend_content(&ResolvedPlotSettings::default())
					.is_none()
			);
			assert!(
				kernel
					.legend_reservation_content(&ResolvedPlotSettings::default())
					.is_none()
			);
			let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
			let layout = kernel.layout(ResolvedPlotSettings::default());
			let transform = CoordinateTransformer::new(&layout, bounds);
			let hit = kernel
				.hit(&transform, Cursor::Available(transform.cartesian(2.0, 2.0)))
				.unwrap();
			assert_eq!(
				hit.fields.iter().any(|field| field.label == "Mid"),
				middle.is_some()
			);
			let range = hit
				.fields
				.iter()
				.find(|field| field.label == "Range")
				.unwrap();
			assert!(matches!(
				range.value,
				HitValue::Interval {
					end_inclusive: true,
					..
				}
			));
		}
	}
}
