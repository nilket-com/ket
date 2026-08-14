use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PieValueCell {
	Null,
	Exact(f64),
	Inexact(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PieSliceRow {
	pub(crate) category: Option<String>,
	pub(crate) value: PieValueCell,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PieSlice {
	category: Option<String>,
	value: f64,
}

impl PieSlice {
	pub(crate) fn category(&self) -> Option<&str> {
		self.category.as_deref()
	}

	pub(crate) fn value(&self) -> f64 {
		self.value
	}
}

/// A validated pie relation and every quantity derived from it.
///
/// Construction is crate-scoped so callers cannot separate slices from their
/// total or angular boundaries (PRODUCT-0102).
#[derive(Clone, Debug, PartialEq)]
pub struct PieSlices {
	slices: Vec<PieSlice>,
	total: Option<f64>,
	boundaries: Vec<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PieSlicesError {
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
	DuplicateCategory {
		field: String,
		value_field: String,
		category: String,
		first_row: usize,
		second_row: usize,
	},
	NonFiniteTotal {
		field: String,
	},
	NonPositiveTotal {
		field: String,
	},
}

impl PieSlices {
	pub(crate) fn from_rows(
		category_field: Option<&str>,
		value_field: &str,
		rows: Vec<PieSliceRow>,
	) -> Result<Self, PieSlicesError> {
		// Empty is absence, not a zero-total figure. It is accepted before total
		// validation because there is no total to judge (PRODUCT-0102 §6).
		if rows.is_empty() {
			return Ok(Self {
				slices: Vec::new(),
				total: None,
				boundaries: Vec::new(),
			});
		}

		// Row-local validation is producer ordered, category before value.
		for (index, row) in rows.iter().enumerate() {
			let one_based = index + 1;
			if let Some(field) = category_field
				&& row.category.is_none()
			{
				return Err(PieSlicesError::NullCategory {
					field: field.to_owned(),
					row: one_based,
				});
			}
			match &row.value {
				PieValueCell::Null => {
					return Err(PieSlicesError::NullValue {
						field: value_field.to_owned(),
						row: one_based,
					});
				}
				PieValueCell::Inexact(value) => {
					return Err(PieSlicesError::InexactValue {
						field: value_field.to_owned(),
						row: one_based,
						value: value.clone(),
					});
				}
				PieValueCell::Exact(value) if !value.is_finite() => {
					return Err(PieSlicesError::NonFiniteValue {
						field: value_field.to_owned(),
						row: one_based,
						value: *value,
					});
				}
				PieValueCell::Exact(value) if *value < 0.0 => {
					return Err(PieSlicesError::NegativeValue {
						field: value_field.to_owned(),
						row: one_based,
						value: *value,
					});
				}
				PieValueCell::Exact(_) => {}
			}
		}

		// Key uniqueness precedes total validation. Equal-valued duplicates are
		// still duplicates because this is a relation-shape rule.
		if let Some(field) = category_field {
			let mut first_rows = HashMap::with_capacity(rows.len());
			for (index, row) in rows.iter().enumerate() {
				let category = row
					.category
					.as_deref()
					.expect("grouped pie row has a validated category");
				if let Some(first_row) = first_rows.insert(category, index + 1) {
					return Err(PieSlicesError::DuplicateCategory {
						field: field.to_owned(),
						value_field: value_field.to_owned(),
						category: category.to_owned(),
						first_row,
						second_row: index + 1,
					});
				}
			}
		}

		let total = rows.iter().fold(0.0, |sum, row| {
			let PieValueCell::Exact(value) = row.value else {
				unreachable!("pie value was validated")
			};
			sum + value
		});
		if !total.is_finite() {
			return Err(PieSlicesError::NonFiniteTotal {
				field: value_field.to_owned(),
			});
		}
		if total <= 0.0 {
			return Err(PieSlicesError::NonPositiveTotal {
				field: value_field.to_owned(),
			});
		}

		let mut boundaries = Vec::with_capacity(rows.len() + 1);
		boundaries.push(0.0);
		let mut cumulative = 0.0;
		for row in rows.iter().take(rows.len() - 1) {
			let PieValueCell::Exact(value) = row.value else {
				unreachable!("pie value was validated")
			};
			cumulative += value;
			boundaries.push(cumulative / total * std::f64::consts::TAU);
		}
		// Do not accumulate the terminal boundary: pin it exactly so the seam has
		// no rounding sliver.
		boundaries.push(std::f64::consts::TAU);

		Ok(Self {
			slices: rows
				.into_iter()
				.map(|row| PieSlice {
					category: category_field.and(row.category),
					value: match row.value {
						PieValueCell::Exact(value) => value,
						PieValueCell::Null | PieValueCell::Inexact(_) => {
							unreachable!("pie value was validated")
						}
					},
				})
				.collect(),
			total: Some(total),
			boundaries,
		})
	}

	pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &PieSlice> {
		self.slices.iter()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.slices.is_empty()
	}

	pub(crate) fn total(&self) -> Option<f64> {
		self.total
	}

	pub(crate) fn boundaries(&self) -> &[f64] {
		&self.boundaries
	}

	pub(crate) fn get(&self, index: usize) -> Option<&PieSlice> {
		self.slices.get(index)
	}
}

impl fmt::Display for PieSlicesError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullCategory { field, row } => {
				write!(
					formatter,
					"pie category field `{field}` is null at row {row}"
				)
			}
			Self::NullValue { field, row } => {
				write!(formatter, "pie value field `{field}` is null at row {row}")
			}
			Self::InexactValue { field, row, value } => write!(
				formatter,
				"pie value field `{field}` cannot represent `{value}` exactly as f64 at row {row}"
			),
			Self::NonFiniteValue { field, row, value } => write!(
				formatter,
				"pie value field `{field}` is non-finite at row {row}: {value}"
			),
			Self::NegativeValue { field, row, value } => write!(
				formatter,
				"pie value field `{field}` is negative at row {row}: {value}"
			),
			Self::DuplicateCategory {
				field,
				value_field,
				category,
				first_row,
				second_row,
			} => write!(
				formatter,
				"pie category `{category}` in field `{field}` is duplicated at rows {first_row} and {second_row}; aggregate duplicate categories in the relation with .aggregate([col(\"{field}\")], [sum(col(\"{value_field}\"))]) before pie"
			),
			Self::NonFiniteTotal { field } => write!(
				formatter,
				"pie value field `{field}` has a non-finite total"
			),
			Self::NonPositiveTotal { field } => write!(
				formatter,
				"pie value field `{field}` has a zero total in a non-empty relation"
			),
		}
	}
}

impl std::error::Error for PieSlicesError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn exact(category: Option<&str>, value: f64) -> PieSliceRow {
		PieSliceRow {
			category: category.map(str::to_owned),
			value: PieValueCell::Exact(value),
		}
	}

	#[test]
	fn product_0102_grouped_duplicates_refuse_but_ungrouped_repetitions_construct() {
		for values in [[2.0, 3.0], [2.0, 2.0]] {
			let error = PieSlices::from_rows(
				Some("category"),
				"value",
				vec![exact(Some("A"), values[0]), exact(Some("A"), values[1])],
			)
			.unwrap_err();
			assert!(matches!(
				error,
				PieSlicesError::DuplicateCategory {
					first_row: 1,
					second_row: 2,
					..
				}
			));
			assert!(error.to_string().contains(".aggregate("));
		}

		let ungrouped =
			PieSlices::from_rows(None, "value", vec![exact(None, 2.0), exact(None, 2.0)])
				.expect("ungrouped repetitions are independent slices");
		assert_eq!(ungrouped.iter().len(), 2);
	}

	#[test]
	fn product_0102_row_validation_is_category_then_value_and_precedes_duplicates() {
		assert!(matches!(
			PieSlices::from_rows(
				Some("category"),
				"value",
				vec![PieSliceRow {
					category: None,
					value: PieValueCell::Null,
				}],
			),
			Err(PieSlicesError::NullCategory { row: 1, .. })
		));
		assert!(matches!(
			PieSlices::from_rows(
				Some("category"),
				"value",
				vec![
					exact(Some("A"), 2.0),
					PieSliceRow {
						category: Some("A".to_owned()),
						value: PieValueCell::Null,
					}
				],
			),
			Err(PieSlicesError::NullValue { row: 2, .. })
		));
	}

	#[test]
	fn product_0102_total_is_last_and_empty_precedes_total_validation() {
		let empty = PieSlices::from_rows(Some("category"), "value", Vec::new())
			.expect("empty pie is absence");
		assert!(empty.is_empty());
		assert_eq!(empty.total(), None);
		assert!(empty.boundaries().is_empty());

		assert!(matches!(
			PieSlices::from_rows(
				Some("category"),
				"value",
				vec![exact(Some("A"), 0.0), exact(Some("B"), 0.0)],
			),
			Err(PieSlicesError::NonPositiveTotal { .. })
		));
		assert!(matches!(
			PieSlices::from_rows(
				Some("category"),
				"value",
				vec![exact(Some("A"), f64::MAX), exact(Some("B"), f64::MAX),],
			),
			Err(PieSlicesError::NonFiniteTotal { .. })
		));
		assert!(matches!(
			PieSlices::from_rows(
				Some("category"),
				"value",
				vec![exact(Some("A"), f64::MAX), exact(Some("A"), f64::MAX),],
			),
			Err(PieSlicesError::DuplicateCategory { .. })
		));
	}

	#[test]
	fn product_0102_zero_slice_has_zero_sweep_and_terminal_boundary_is_exact_tau() {
		let slices = PieSlices::from_rows(
			Some("category"),
			"value",
			vec![exact(Some("zero"), 0.0), exact(Some("full"), 5.0)],
		)
		.expect("zero slice is valid beside a positive slice");
		assert_eq!(slices.boundaries(), &[0.0, 0.0, std::f64::consts::TAU]);
		assert_eq!(slices.boundaries().last(), Some(&std::f64::consts::TAU));
	}
}
