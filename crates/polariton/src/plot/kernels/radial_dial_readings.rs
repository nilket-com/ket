use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RadialDialNumericCell {
	Null,
	Exact(f64),
	Inexact(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RadialDialReadingRow {
	pub(crate) category: Option<String>,
	pub(crate) value: RadialDialNumericCell,
	pub(crate) maximum: RadialDialNumericCell,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RadialDialReading {
	category: Option<String>,
	value: f64,
	maximum: f64,
}

impl RadialDialReading {
	pub(crate) fn category(&self) -> Option<&str> {
		self.category.as_deref()
	}

	pub(crate) fn value(&self) -> f64 {
		self.value
	}

	pub(crate) fn maximum(&self) -> f64 {
		self.maximum
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct RadialDialReadings {
	readings: Vec<RadialDialReading>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RadialDialReadingsError {
	NullCategory {
		field: String,
		row: usize,
	},
	NullValue {
		field: String,
		row: usize,
	},
	InexactValue {
		field: String,
		row: usize,
		value: String,
	},
	NonFiniteValue {
		field: String,
		row: usize,
		value: f64,
	},
	NegativeValue {
		field: String,
		row: usize,
		value: f64,
	},
	NullMaximum {
		field: String,
		row: usize,
	},
	InexactMaximum {
		field: String,
		row: usize,
		value: String,
	},
	NonFiniteMaximum {
		field: String,
		row: usize,
		value: f64,
	},
	NonPositiveMaximum {
		field: String,
		row: usize,
		value: f64,
	},
	ValueExceedsMaximum {
		field: String,
		row: usize,
		value: f64,
		maximum: f64,
	},
	DuplicateCategory {
		field: String,
		category: String,
		first_row: usize,
		second_row: usize,
	},
	UngroupedMultipleRows {
		second_row: usize,
	},
}

impl RadialDialReadings {
	pub(crate) fn from_rows(
		category_field: Option<&str>,
		value_field: &str,
		maximum_field: &str,
		rows: Vec<RadialDialReadingRow>,
	) -> Result<Self, RadialDialReadingsError> {
		for (index, reading) in rows.iter().enumerate() {
			let row = index + 1;
			if let Some(field) = category_field
				&& reading.category.is_none()
			{
				return Err(RadialDialReadingsError::NullCategory {
					field: field.to_owned(),
					row,
				});
			}

			let value = match &reading.value {
				RadialDialNumericCell::Null => {
					return Err(RadialDialReadingsError::NullValue {
						field: value_field.to_owned(),
						row,
					});
				}
				RadialDialNumericCell::Inexact(value) => {
					return Err(RadialDialReadingsError::InexactValue {
						field: value_field.to_owned(),
						row,
						value: value.clone(),
					});
				}
				RadialDialNumericCell::Exact(value) if !value.is_finite() => {
					return Err(RadialDialReadingsError::NonFiniteValue {
						field: value_field.to_owned(),
						row,
						value: *value,
					});
				}
				RadialDialNumericCell::Exact(value) if *value < 0.0 => {
					return Err(RadialDialReadingsError::NegativeValue {
						field: value_field.to_owned(),
						row,
						value: *value,
					});
				}
				RadialDialNumericCell::Exact(value) => *value,
			};

			let maximum = match &reading.maximum {
				RadialDialNumericCell::Null => {
					return Err(RadialDialReadingsError::NullMaximum {
						field: maximum_field.to_owned(),
						row,
					});
				}
				RadialDialNumericCell::Inexact(value) => {
					return Err(RadialDialReadingsError::InexactMaximum {
						field: maximum_field.to_owned(),
						row,
						value: value.clone(),
					});
				}
				RadialDialNumericCell::Exact(value) if !value.is_finite() => {
					return Err(RadialDialReadingsError::NonFiniteMaximum {
						field: maximum_field.to_owned(),
						row,
						value: *value,
					});
				}
				RadialDialNumericCell::Exact(value) if *value <= 0.0 => {
					return Err(RadialDialReadingsError::NonPositiveMaximum {
						field: maximum_field.to_owned(),
						row,
						value: *value,
					});
				}
				RadialDialNumericCell::Exact(value) => *value,
			};

			if value > maximum {
				return Err(RadialDialReadingsError::ValueExceedsMaximum {
					field: value_field.to_owned(),
					row,
					value,
					maximum,
				});
			}
		}

		if let Some(category_field) = category_field {
			let mut first_rows = HashMap::with_capacity(rows.len());
			for (index, reading) in rows.iter().enumerate() {
				let category = reading
					.category
					.as_deref()
					.expect("grouped radial-dial reading has a validated category");
				if let Some(first_row) = first_rows.insert(category, index + 1) {
					return Err(RadialDialReadingsError::DuplicateCategory {
						field: category_field.to_owned(),
						category: category.to_owned(),
						first_row,
						second_row: index + 1,
					});
				}
			}
		} else if rows.len() > 1 {
			return Err(RadialDialReadingsError::UngroupedMultipleRows { second_row: 2 });
		}

		let mut readings: Vec<_> = rows
			.into_iter()
			.map(|reading| RadialDialReading {
				category: category_field.and(reading.category),
				value: match reading.value {
					RadialDialNumericCell::Exact(value) => value,
					RadialDialNumericCell::Null | RadialDialNumericCell::Inexact(_) => {
						unreachable!("radial-dial value was validated")
					}
				},
				maximum: match reading.maximum {
					RadialDialNumericCell::Exact(value) => value,
					RadialDialNumericCell::Null | RadialDialNumericCell::Inexact(_) => {
						unreachable!("radial-dial maximum was validated")
					}
				},
			})
			.collect();
		if category_field.is_some() {
			readings.sort_by(|left, right| left.category.cmp(&right.category));
		}
		Ok(Self { readings })
	}

	pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &RadialDialReading> {
		self.readings.iter()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.readings.is_empty()
	}
}

impl fmt::Display for RadialDialReadingsError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullCategory { field, row } => write!(
				formatter,
				"radial dial category field `{field}` is null at row {row}"
			),
			Self::NullValue { field, row } => write!(
				formatter,
				"radial dial value field `{field}` is null at row {row}"
			),
			Self::InexactValue { field, row, value } => write!(
				formatter,
				"radial dial value field `{field}` cannot represent `{value}` exactly as f64 at row {row}"
			),
			Self::NonFiniteValue { field, row, value } => write!(
				formatter,
				"radial dial value field `{field}` is non-finite at row {row}: {value}"
			),
			Self::NegativeValue { field, row, value } => write!(
				formatter,
				"radial dial value field `{field}` is negative at row {row}: {value}"
			),
			Self::NullMaximum { field, row } => write!(
				formatter,
				"radial dial maximum field `{field}` is null at row {row}"
			),
			Self::InexactMaximum { field, row, value } => write!(
				formatter,
				"radial dial maximum field `{field}` cannot represent `{value}` exactly as f64 at row {row}"
			),
			Self::NonFiniteMaximum { field, row, value } => write!(
				formatter,
				"radial dial maximum field `{field}` is non-finite at row {row}: {value}"
			),
			Self::NonPositiveMaximum { field, row, value } => write!(
				formatter,
				"radial dial maximum field `{field}` is not positive at row {row}: {value}"
			),
			Self::ValueExceedsMaximum {
				field,
				row,
				value,
				maximum,
			} => write!(
				formatter,
				"radial dial value field `{field}` exceeds its maximum at row {row}: {value} > {maximum}"
			),
			Self::DuplicateCategory {
				field,
				category,
				first_row,
				second_row,
			} => write!(
				formatter,
				"radial dial category `{category}` in field `{field}` is duplicated at rows {first_row} and {second_row}; aggregate duplicate categories in the relation before radial dial"
			),
			Self::UngroupedMultipleRows { second_row } => write!(
				formatter,
				"ungrouped radial dial has more than one reading at row {second_row}; bind a category field or reduce the relation to one row"
			),
		}
	}
}

impl std::error::Error for RadialDialReadingsError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn exact(value: f64) -> RadialDialNumericCell {
		RadialDialNumericCell::Exact(value)
	}

	fn row(
		category: Option<&str>,
		value: RadialDialNumericCell,
		maximum: RadialDialNumericCell,
	) -> RadialDialReadingRow {
		RadialDialReadingRow {
			category: category.map(str::to_owned),
			value,
			maximum,
		}
	}

	#[test]
	fn product_0099_row_validation_is_row_first_and_field_ordered() {
		let error = RadialDialReadings::from_rows(
			Some("category"),
			"value",
			"maximum",
			vec![row(None, RadialDialNumericCell::Null, exact(0.0))],
		)
		.unwrap_err();
		assert_eq!(
			error,
			RadialDialReadingsError::NullCategory {
				field: "category".to_owned(),
				row: 1,
			}
		);

		let error = RadialDialReadings::from_rows(
			Some("category"),
			"value",
			"maximum",
			vec![row(
				Some("A"),
				RadialDialNumericCell::Null,
				RadialDialNumericCell::Null,
			)],
		)
		.unwrap_err();
		assert_eq!(
			error,
			RadialDialReadingsError::NullValue {
				field: "value".to_owned(),
				row: 1,
			}
		);
	}

	#[test]
	fn product_0099_quantitative_constraints_are_field_specific() {
		for (value, expected) in [
			(
				RadialDialNumericCell::Null,
				RadialDialReadingsError::NullValue {
					field: "value".to_owned(),
					row: 1,
				},
			),
			(
				RadialDialNumericCell::Inexact("9007199254740993".to_owned()),
				RadialDialReadingsError::InexactValue {
					field: "value".to_owned(),
					row: 1,
					value: "9007199254740993".to_owned(),
				},
			),
			(
				exact(f64::NAN),
				RadialDialReadingsError::NonFiniteValue {
					field: "value".to_owned(),
					row: 1,
					value: f64::NAN,
				},
			),
			(
				exact(-1.0),
				RadialDialReadingsError::NegativeValue {
					field: "value".to_owned(),
					row: 1,
					value: -1.0,
				},
			),
		] {
			let actual = RadialDialReadings::from_rows(
				Some("category"),
				"value",
				"maximum",
				vec![row(Some("A"), value, exact(10.0))],
			)
			.unwrap_err();
			match (&actual, &expected) {
				(
					RadialDialReadingsError::NonFiniteValue { value: left, .. },
					RadialDialReadingsError::NonFiniteValue { value: right, .. },
				) if left.is_nan() && right.is_nan() => {}
				_ => assert_eq!(actual, expected),
			}
		}

		for (maximum, expected) in [
			(
				RadialDialNumericCell::Null,
				RadialDialReadingsError::NullMaximum {
					field: "maximum".to_owned(),
					row: 1,
				},
			),
			(
				RadialDialNumericCell::Inexact("9007199254740993".to_owned()),
				RadialDialReadingsError::InexactMaximum {
					field: "maximum".to_owned(),
					row: 1,
					value: "9007199254740993".to_owned(),
				},
			),
			(
				exact(f64::INFINITY),
				RadialDialReadingsError::NonFiniteMaximum {
					field: "maximum".to_owned(),
					row: 1,
					value: f64::INFINITY,
				},
			),
			(
				exact(0.0),
				RadialDialReadingsError::NonPositiveMaximum {
					field: "maximum".to_owned(),
					row: 1,
					value: 0.0,
				},
			),
			(
				exact(-1.0),
				RadialDialReadingsError::NonPositiveMaximum {
					field: "maximum".to_owned(),
					row: 1,
					value: -1.0,
				},
			),
		] {
			assert_eq!(
				RadialDialReadings::from_rows(
					Some("category"),
					"value",
					"maximum",
					vec![row(Some("A"), exact(1.0), maximum)],
				)
				.unwrap_err(),
				expected
			);
		}

		let extreme = RadialDialReadings::from_rows(
			Some("category"),
			"value",
			"maximum",
			vec![row(Some("A"), exact(1e300), exact(1e300))],
		)
		.expect("large finite reading remains valid");
		assert_eq!(extreme.iter().next().expect("one reading").value(), 1e300);
	}

	#[test]
	fn product_0099_row_local_faults_precede_cross_row_faults() {
		let error = RadialDialReadings::from_rows(
			Some("category"),
			"value",
			"maximum",
			vec![
				row(Some("A"), exact(1.0), exact(10.0)),
				row(Some("A"), exact(11.0), exact(10.0)),
			],
		)
		.unwrap_err();
		assert!(matches!(
			error,
			RadialDialReadingsError::ValueExceedsMaximum { row: 2, .. }
		));
	}

	#[test]
	fn product_0099_duplicate_categories_refuse_independent_of_values() {
		for second_value in [2.0, 1.0] {
			let error = RadialDialReadings::from_rows(
				Some("category"),
				"value",
				"maximum",
				vec![
					row(Some("A"), exact(1.0), exact(10.0)),
					row(Some("A"), exact(second_value), exact(10.0)),
				],
			)
			.unwrap_err();
			assert_eq!(
				error,
				RadialDialReadingsError::DuplicateCategory {
					field: "category".to_owned(),
					category: "A".to_owned(),
					first_row: 1,
					second_row: 2,
				}
			);
		}
	}

	#[test]
	fn product_0099_grouped_order_and_ungrouped_shape_are_distinct() {
		let grouped = RadialDialReadings::from_rows(
			Some("category"),
			"value",
			"maximum",
			vec![
				row(Some("Zulu"), exact(2.0), exact(10.0)),
				row(Some("Alpha"), exact(1.0), exact(10.0)),
			],
		)
		.expect("unique grouped readings");
		assert_eq!(
			grouped
				.iter()
				.map(|reading| reading.category())
				.collect::<Vec<_>>(),
			[Some("Alpha"), Some("Zulu")]
		);

		let ungrouped = RadialDialReadings::from_rows(
			None,
			"value",
			"maximum",
			vec![row(None, exact(1.0), exact(10.0))],
		)
		.expect("one ungrouped reading");
		assert_eq!(ungrouped.iter().next().and_then(|row| row.category()), None);
		assert_eq!(
			RadialDialReadings::from_rows(
				None,
				"value",
				"maximum",
				vec![
					row(None, exact(1.0), exact(10.0)),
					row(None, exact(2.0), exact(10.0)),
				],
			)
			.unwrap_err(),
			RadialDialReadingsError::UngroupedMultipleRows { second_row: 2 }
		);
	}
}
