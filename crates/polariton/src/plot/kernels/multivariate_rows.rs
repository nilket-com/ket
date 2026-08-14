use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use polars::prelude::*;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MultivariateFields {
	dimensions: Vec<String>,
	category: Option<String>,
}

impl MultivariateFields {
	pub(crate) fn new(dimensions: &[String], category: Option<&str>) -> Self {
		Self {
			dimensions: dimensions.to_vec(),
			category: category.map(str::to_owned),
		}
	}

	pub(crate) fn category(&self) -> Option<&str> {
		self.category.as_deref()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MultivariateValueCell {
	Null,
	Exact(f64),
	Inexact(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MultivariateInputRow {
	pub(crate) category: Option<String>,
	pub(crate) values: Vec<MultivariateValueCell>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MultivariateRow {
	category: Option<String>,
	values: Vec<f64>,
}

impl MultivariateRow {
	pub(crate) fn category(&self) -> Option<&str> {
		self.category.as_deref()
	}

	pub(crate) fn values(&self) -> &[f64] {
		&self.values
	}
}

/// Validated observations shared by radar and parallel coordinates.
///
/// Construction is crate-scoped so dimensions, rows, the category catalog, and
/// extrema cannot be supplied as independent truths (PRODUCT-0104).
#[derive(Clone, Debug, PartialEq)]
pub struct MultivariateRows {
	dimensions: Vec<String>,
	rows: Vec<MultivariateRow>,
	categories: Vec<Option<String>>,
	extrema: Option<Vec<(f64, f64)>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MultivariateRowsError {
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
}

impl MultivariateRows {
	pub(crate) fn from_rows(
		fields: &MultivariateFields,
		rows: Vec<MultivariateInputRow>,
	) -> Result<Self, MultivariateRowsError> {
		for (index, row) in rows.iter().enumerate() {
			assert_eq!(
				row.values.len(),
				fields.dimensions.len(),
				"PRODUCT-0104 multivariate input row must match its dimension list"
			);
			let one_based = index + 1;
			if let Some(field) = fields.category()
				&& row.category.is_none()
			{
				return Err(MultivariateRowsError::NullCategory {
					field: field.to_owned(),
					row: one_based,
				});
			}
			for (dimension, value) in fields.dimensions.iter().zip(&row.values) {
				match value {
					MultivariateValueCell::Null => {
						return Err(MultivariateRowsError::NullValue {
							field: dimension.clone(),
							row: one_based,
						});
					}
					MultivariateValueCell::Inexact(value) => {
						return Err(MultivariateRowsError::InexactValue {
							field: dimension.clone(),
							row: one_based,
							value: value.clone(),
						});
					}
					MultivariateValueCell::Exact(value) if !value.is_finite() => {
						return Err(MultivariateRowsError::NonFiniteValue {
							field: dimension.clone(),
							row: one_based,
							value: *value,
						});
					}
					MultivariateValueCell::Exact(_) => {}
				}
			}
		}

		let categories = if rows.is_empty() {
			Vec::new()
		} else if fields.category().is_none() {
			vec![None]
		} else {
			rows.iter()
				.map(|row| {
					row.category
						.clone()
						.expect("grouped multivariate row has a validated category")
				})
				.collect::<BTreeSet<_>>()
				.into_iter()
				.map(Some)
				.collect()
		};

		let extrema = if rows.is_empty() {
			None
		} else {
			let mut extrema = vec![(f64::INFINITY, f64::NEG_INFINITY); fields.dimensions.len()];
			for row in &rows {
				for ((low, high), value) in extrema.iter_mut().zip(&row.values) {
					let MultivariateValueCell::Exact(value) = value else {
						unreachable!("multivariate value was validated")
					};
					*low = low.min(*value);
					*high = high.max(*value);
				}
			}
			Some(extrema)
		};

		Ok(Self {
			dimensions: fields.dimensions.clone(),
			rows: rows
				.into_iter()
				.map(|row| MultivariateRow {
					category: fields.category().and(row.category),
					values: row
						.values
						.into_iter()
						.map(|value| match value {
							MultivariateValueCell::Exact(value) => value,
							MultivariateValueCell::Null | MultivariateValueCell::Inexact(_) => {
								unreachable!("multivariate value was validated")
							}
						})
						.collect(),
				})
				.collect(),
			categories,
			extrema,
		})
	}

	pub(crate) fn dimensions(&self) -> &[String] {
		&self.dimensions
	}

	pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &MultivariateRow> {
		self.rows.iter()
	}

	pub(crate) fn categories(&self) -> &[Option<String>] {
		&self.categories
	}

	pub(crate) fn category_position(&self, category: Option<&str>) -> Option<usize> {
		self.categories
			.iter()
			.position(|candidate| candidate.as_deref() == category)
	}

	pub(crate) fn extrema(&self) -> Option<&[(f64, f64)]> {
		self.extrema.as_deref()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MultivariatePreparationError {
	MissingDimension { field: String },
	UnsupportedDimension { field: String },
	MissingCategory { field: String },
	UnsupportedCategory { field: String },
	Rows(MultivariateRowsError),
}

pub(crate) fn prepare_multivariate_rows(
	df: &DataFrame,
	dimensions: &[String],
	category: Option<&str>,
) -> Result<MultivariateRows, MultivariatePreparationError> {
	let fields = MultivariateFields::new(dimensions, category);
	let mut dimension_columns = Vec::with_capacity(dimensions.len());
	for dimension in dimensions {
		let column =
			df.column(dimension)
				.map_err(|_| MultivariatePreparationError::MissingDimension {
					field: dimension.clone(),
				})?;
		if !column.dtype().is_numeric() {
			return Err(MultivariatePreparationError::UnsupportedDimension {
				field: dimension.clone(),
			});
		}
		dimension_columns.push(column.as_materialized_series());
	}
	let category_column = if let Some(field) = category {
		let column =
			df.column(field)
				.map_err(|_| MultivariatePreparationError::MissingCategory {
					field: field.to_owned(),
				})?;
		if !multivariate_category_dtype(column.dtype()) {
			return Err(MultivariatePreparationError::UnsupportedCategory {
				field: field.to_owned(),
			});
		}
		Some(column.as_materialized_series())
	} else {
		None
	};

	let mut rows = Vec::with_capacity(df.height());
	for row in 0..df.height() {
		let category = category_column.map(|series| {
			category_value(
				series
					.get(row)
					.expect("multivariate category row is in frame bounds"),
			)
		});
		let values = dimension_columns
			.iter()
			.map(|series| {
				let value = series
					.get(row)
					.expect("multivariate dimension row is in frame bounds");
				let original = value.to_string();
				match exact_f64(value) {
					Ok(value) => MultivariateValueCell::Exact(value),
					Err(ExactF64Error::Null) => MultivariateValueCell::Null,
					Err(ExactF64Error::Inexact) => MultivariateValueCell::Inexact(original),
				}
			})
			.collect();
		rows.push(MultivariateInputRow {
			category: category.flatten(),
			values,
		});
	}

	MultivariateRows::from_rows(&fields, rows).map_err(MultivariatePreparationError::Rows)
}

fn multivariate_category_dtype(dtype: &DataType) -> bool {
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

impl fmt::Display for MultivariateRowsError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullCategory { field, row } => write!(
				formatter,
				"multivariate category field `{field}` is null at row {row}"
			),
			Self::NullValue { field, row } => write!(
				formatter,
				"multivariate dimension field `{field}` is null at row {row}"
			),
			Self::InexactValue { field, row, value } => write!(
				formatter,
				"multivariate dimension field `{field}` cannot represent `{value}` exactly as f64 at row {row}"
			),
			Self::NonFiniteValue { field, row, value } => write!(
				formatter,
				"multivariate dimension field `{field}` is non-finite at row {row}: {value}"
			),
		}
	}
}

impl std::error::Error for MultivariateRowsError {}

impl fmt::Display for MultivariatePreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingDimension { field } => {
				write!(
					formatter,
					"multivariate dimension field `{field}` is missing"
				)
			}
			Self::UnsupportedDimension { field } => write!(
				formatter,
				"multivariate dimension field `{field}` is not quantitative"
			),
			Self::MissingCategory { field } => {
				write!(
					formatter,
					"multivariate category field `{field}` is missing"
				)
			}
			Self::UnsupportedCategory { field } => write!(
				formatter,
				"multivariate category field `{field}` is not nominal"
			),
			Self::Rows(error) => error.fmt(formatter),
		}
	}
}

impl std::error::Error for MultivariatePreparationError {}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::df;

	fn fields(dimensions: &[&str], category: Option<&str>) -> MultivariateFields {
		MultivariateFields::new(
			&dimensions
				.iter()
				.map(|dimension| (*dimension).to_owned())
				.collect::<Vec<_>>(),
			category,
		)
	}

	fn row(category: Option<&str>, values: &[f64]) -> MultivariateInputRow {
		MultivariateInputRow {
			category: category.map(str::to_owned),
			values: values
				.iter()
				.copied()
				.map(MultivariateValueCell::Exact)
				.collect(),
		}
	}

	#[test]
	fn product_0104_preserves_dimension_and_global_row_order_with_lexical_catalog() {
		let fields = fields(&["zeta", "alpha"], Some("group"));
		let rows = MultivariateRows::from_rows(
			&fields,
			vec![
				row(Some("B"), &[1.0, 10.0]),
				row(Some("A"), &[2.0, 20.0]),
				row(Some("B"), &[3.0, 30.0]),
				row(Some("A"), &[4.0, 40.0]),
			],
		)
		.expect("valid interleaved multivariate rows");

		assert_eq!(rows.dimensions(), ["zeta", "alpha"]);
		assert_eq!(
			rows.categories(),
			[Some("A".to_owned()), Some("B".to_owned())]
		);
		assert_eq!(
			rows.iter()
				.map(MultivariateRow::category)
				.collect::<Vec<_>>(),
			[Some("B"), Some("A"), Some("B"), Some("A")]
		);
		assert_eq!(
			rows.iter().map(MultivariateRow::values).collect::<Vec<_>>(),
			[&[1.0, 10.0][..], &[2.0, 20.0], &[3.0, 30.0], &[4.0, 40.0]]
		);
		assert_eq!(rows.extrema(), Some(&[(1.0, 4.0), (10.0, 40.0)][..]));
	}

	#[test]
	fn product_0104_bad_cells_refuse_with_dimension_and_one_based_row() {
		let fields = fields(&["first", "second"], Some("group"));
		for (cell, expected) in [
			(
				MultivariateValueCell::Null,
				MultivariateRowsError::NullValue {
					field: "second".to_owned(),
					row: 2,
				},
			),
			(
				MultivariateValueCell::Exact(f64::INFINITY),
				MultivariateRowsError::NonFiniteValue {
					field: "second".to_owned(),
					row: 2,
					value: f64::INFINITY,
				},
			),
			(
				MultivariateValueCell::Inexact("9007199254740993".to_owned()),
				MultivariateRowsError::InexactValue {
					field: "second".to_owned(),
					row: 2,
					value: "9007199254740993".to_owned(),
				},
			),
		] {
			let result = MultivariateRows::from_rows(
				&fields,
				vec![
					row(Some("A"), &[1.0, 2.0]),
					MultivariateInputRow {
						category: Some("A".to_owned()),
						values: vec![MultivariateValueCell::Exact(3.0), cell],
					},
				],
			);
			assert_eq!(result.unwrap_err(), expected);
		}
	}

	#[test]
	fn product_0104_exactness_uses_the_shared_f64_boundary() {
		let dimensions = vec!["value".to_owned()];
		for (value, accepted) in [
			(9_007_199_254_740_992_i64, true),
			(9_007_199_254_740_993_i64, false),
			(9_007_199_254_740_994_i64, true),
		] {
			let frame = df!("value" => [value]).expect("integer exactness frame");
			let result = prepare_multivariate_rows(&frame, &dimensions, None);
			assert_eq!(result.is_ok(), accepted, "value {value}");
		}
	}

	#[test]
	fn product_0104_category_absence_null_and_literal_all_data_are_distinct() {
		let dimensions = vec!["value".to_owned()];
		let ungrouped = df!("value" => [1.0, 2.0]).expect("ungrouped frame");
		let ungrouped = prepare_multivariate_rows(&ungrouped, &dimensions, None)
			.expect("omitted category is valid");
		assert_eq!(ungrouped.categories(), [None]);
		assert!(ungrouped.iter().all(|row| row.category().is_none()));

		let null =
			df!("group" => [Some("A"), None], "value" => [1.0, 2.0]).expect("null category frame");
		assert_eq!(
			prepare_multivariate_rows(&null, &dimensions, Some("group")).unwrap_err(),
			MultivariatePreparationError::Rows(MultivariateRowsError::NullCategory {
				field: "group".to_owned(),
				row: 2,
			})
		);

		let literal =
			df!("group" => ["All Data"], "value" => [3.0]).expect("literal category frame");
		let literal = prepare_multivariate_rows(&literal, &dimensions, Some("group"))
			.expect("literal category remains data");
		assert_eq!(literal.categories(), [Some("All Data".to_owned())]);
	}

	#[test]
	fn product_0104_missing_and_incompatible_fields_refuse_per_role() {
		let dimensions = vec!["value".to_owned()];
		let category_only = df!("group" => ["A"]).expect("category-only frame");
		assert_eq!(
			prepare_multivariate_rows(&category_only, &dimensions, Some("group")).unwrap_err(),
			MultivariatePreparationError::MissingDimension {
				field: "value".to_owned()
			}
		);

		let bad_dimension = df!("value" => ["bad"], "group" => ["A"]).expect("bad dimension frame");
		assert_eq!(
			prepare_multivariate_rows(&bad_dimension, &dimensions, Some("group")).unwrap_err(),
			MultivariatePreparationError::UnsupportedDimension {
				field: "value".to_owned()
			}
		);

		let value_only = df!("value" => [1.0]).expect("value-only frame");
		assert_eq!(
			prepare_multivariate_rows(&value_only, &dimensions, Some("group")).unwrap_err(),
			MultivariatePreparationError::MissingCategory {
				field: "group".to_owned()
			}
		);

		let bad_category = df!("value" => [1.0], "group" => [7_i64]).expect("bad category frame");
		assert_eq!(
			prepare_multivariate_rows(&bad_category, &dimensions, Some("group")).unwrap_err(),
			MultivariatePreparationError::UnsupportedCategory {
				field: "group".to_owned()
			}
		);
	}

	#[test]
	fn product_0104_many_rows_per_category_survive_without_aggregation() {
		let fields = fields(&["value"], Some("group"));
		let rows = MultivariateRows::from_rows(
			&fields,
			vec![
				row(Some("A"), &[1.0]),
				row(Some("A"), &[2.0]),
				row(Some("A"), &[3.0]),
			],
		)
		.expect("repeated categories are observations");
		assert_eq!(rows.iter().count(), 3);
		assert_eq!(rows.categories(), [Some("A".to_owned())]);
		assert_eq!(rows.extrema(), Some(&[(1.0, 3.0)][..]));
	}

	#[test]
	fn product_0104_empty_is_honest_but_schema_is_still_validated() {
		let frame = DataFrame::new(
			0,
			vec![
				Series::new_empty("first".into(), &DataType::Float64).into(),
				Series::new_empty("second".into(), &DataType::Float64).into(),
			],
		)
		.expect("typed empty frame");
		let dimensions = vec!["first".to_owned(), "second".to_owned()];
		let rows = prepare_multivariate_rows(&frame, &dimensions, None)
			.expect("empty relation with valid schema");
		assert_eq!(rows.iter().count(), 0);
		assert_eq!(rows.extrema(), None);
		assert!(rows.categories().is_empty());

		let missing = vec!["first".to_owned(), "missing".to_owned()];
		assert_eq!(
			prepare_multivariate_rows(&frame, &missing, None).unwrap_err(),
			MultivariatePreparationError::MissingDimension {
				field: "missing".to_owned()
			}
		);
	}

	#[test]
	fn product_0104_precedence_is_field_first_then_row_local() {
		let missing_dimension_with_null_category =
			df!("group" => [None::<&str>]).expect("missing dimension and null category");
		assert!(matches!(
			prepare_multivariate_rows(
				&missing_dimension_with_null_category,
				&["missing".to_owned()],
				Some("group")
			),
			Err(MultivariatePreparationError::MissingDimension { field }) if field == "missing"
		));

		let two_bad =
			df!("second" => ["bad"], "first" => ["bad"]).expect("two incompatible dimensions");
		assert_eq!(
			prepare_multivariate_rows(&two_bad, &["first".to_owned(), "second".to_owned()], None,)
				.unwrap_err(),
			MultivariatePreparationError::UnsupportedDimension {
				field: "first".to_owned()
			}
		);

		let category_and_dimension_null = df!(
			"group" => [None::<&str>],
			"first" => [None::<f64>],
		)
		.expect("two row-local faults");
		assert_eq!(
			prepare_multivariate_rows(
				&category_and_dimension_null,
				&["first".to_owned()],
				Some("group"),
			)
			.unwrap_err(),
			MultivariatePreparationError::Rows(MultivariateRowsError::NullCategory {
				field: "group".to_owned(),
				row: 1,
			})
		);
	}
}
