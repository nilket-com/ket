use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FunnelValueCell {
	Null,
	Exact(f64),
	Inexact(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FunnelStageRow {
	pub(crate) stage: Option<String>,
	pub(crate) value: FunnelValueCell,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FunnelStage {
	stage: String,
	value: f64,
}

impl FunnelStage {
	pub(crate) fn stage(&self) -> &str {
		&self.stage
	}

	pub(crate) fn value(&self) -> f64 {
		self.value
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct FunnelStages {
	stages: Vec<FunnelStage>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FunnelStagesError {
	NullStage {
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
	DuplicateStage {
		stage: String,
		first_row: usize,
		second_row: usize,
	},
	FirstValueNotPositive {
		field: String,
		row: usize,
		value: f64,
	},
	RisingValue {
		field: String,
		previous_row: usize,
		row: usize,
		previous_value: f64,
		value: f64,
	},
}

impl FunnelStages {
	pub(crate) fn from_rows(
		stage_field: &str,
		value_field: &str,
		rows: Vec<FunnelStageRow>,
	) -> Result<Self, FunnelStagesError> {
		for (index, row) in rows.iter().enumerate() {
			let one_based = index + 1;
			if row.stage.is_none() {
				return Err(FunnelStagesError::NullStage {
					field: stage_field.to_owned(),
					row: one_based,
				});
			}
			match &row.value {
				FunnelValueCell::Null => {
					return Err(FunnelStagesError::NullValue {
						field: value_field.to_owned(),
						row: one_based,
					});
				}
				FunnelValueCell::Inexact(value) => {
					return Err(FunnelStagesError::InexactValue {
						field: value_field.to_owned(),
						row: one_based,
						value: value.clone(),
					});
				}
				FunnelValueCell::Exact(value) if !value.is_finite() => {
					return Err(FunnelStagesError::NonFiniteValue {
						field: value_field.to_owned(),
						row: one_based,
						value: *value,
					});
				}
				FunnelValueCell::Exact(value) if *value < 0.0 => {
					return Err(FunnelStagesError::NegativeValue {
						field: value_field.to_owned(),
						row: one_based,
						value: *value,
					});
				}
				FunnelValueCell::Exact(_) => {}
			}
		}

		let mut first_rows = HashMap::new();
		for (index, row) in rows.iter().enumerate() {
			let stage = row.stage.as_deref().expect("validated funnel stage");
			if let Some(first_row) = first_rows.insert(stage, index + 1) {
				return Err(FunnelStagesError::DuplicateStage {
					stage: stage.to_owned(),
					first_row,
					second_row: index + 1,
				});
			}
		}

		if let Some(first) = rows.first() {
			let FunnelValueCell::Exact(value) = first.value else {
				unreachable!("funnel values were validated")
			};
			if value <= 0.0 {
				return Err(FunnelStagesError::FirstValueNotPositive {
					field: value_field.to_owned(),
					row: 1,
					value,
				});
			}
		}

		for (index, pair) in rows.windows(2).enumerate() {
			let FunnelValueCell::Exact(previous_value) = pair[0].value else {
				unreachable!("funnel values were validated")
			};
			let FunnelValueCell::Exact(value) = pair[1].value else {
				unreachable!("funnel values were validated")
			};
			if value > previous_value {
				return Err(FunnelStagesError::RisingValue {
					field: value_field.to_owned(),
					previous_row: index + 1,
					row: index + 2,
					previous_value,
					value,
				});
			}
		}

		Ok(Self {
			stages: rows
				.into_iter()
				.map(|row| FunnelStage {
					stage: row.stage.expect("validated funnel stage"),
					value: match row.value {
						FunnelValueCell::Exact(value) => value,
						_ => unreachable!("funnel values were validated"),
					},
				})
				.collect(),
		})
	}

	pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &FunnelStage> {
		self.stages.iter()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.stages.is_empty()
	}

	pub(crate) fn first_value(&self) -> Option<f64> {
		self.stages.first().map(FunnelStage::value)
	}

	pub(crate) fn get(&self, index: usize) -> Option<&FunnelStage> {
		self.stages.get(index)
	}
}

impl fmt::Display for FunnelStagesError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullStage { field, row } => {
				write!(
					formatter,
					"funnel stage field `{field}` is null at row {row}"
				)
			}
			Self::NullValue { field, row } => {
				write!(
					formatter,
					"funnel value field `{field}` is null at row {row}"
				)
			}
			Self::InexactValue { field, row, value } => write!(
				formatter,
				"funnel value field `{field}` cannot round-trip through f64 exactly at row {row}: {value}"
			),
			Self::NonFiniteValue { field, row, value } => write!(
				formatter,
				"funnel value field `{field}` is non-finite at row {row}: {value}"
			),
			Self::NegativeValue { field, row, value } => write!(
				formatter,
				"funnel value field `{field}` is negative at row {row}: {value}"
			),
			Self::DuplicateStage {
				stage,
				first_row,
				second_row,
			} => write!(
				formatter,
				"funnel stage `{stage}` is duplicated at rows {first_row} and {second_row}"
			),
			Self::FirstValueNotPositive { field, row, value } => write!(
				formatter,
				"funnel value field `{field}` must be positive at row {row}: {value}"
			),
			Self::RisingValue {
				field,
				previous_row,
				row,
				previous_value,
				value,
			} => write!(
				formatter,
				"funnel value field `{field}` rises between rows {previous_row} and {row}: {previous_value} to {value}"
			),
		}
	}
}

impl std::error::Error for FunnelStagesError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(stage: Option<&str>, value: FunnelValueCell) -> FunnelStageRow {
		FunnelStageRow {
			stage: stage.map(str::to_owned),
			value,
		}
	}

	fn exact(stage: &str, value: f64) -> FunnelStageRow {
		row(Some(stage), FunnelValueCell::Exact(value))
	}

	fn stages(rows: Vec<FunnelStageRow>) -> Result<FunnelStages, FunnelStagesError> {
		FunnelStages::from_rows("stage_name", "amount", rows)
	}

	#[test]
	fn product_0096_null_stage_refuses_while_literal_null_constructs() {
		assert_eq!(
			stages(vec![row(None, FunnelValueCell::Exact(10.0))]),
			Err(FunnelStagesError::NullStage {
				field: "stage_name".to_owned(),
				row: 1,
			})
		);
		let literal = stages(vec![exact("null", 10.0)]).expect("literal null stage");
		assert_eq!(literal.get(0).unwrap().stage(), "null");
	}

	#[test]
	fn product_0096_null_inexact_nonfinite_and_negative_values_refuse_at_their_rows() {
		assert_eq!(
			stages(vec![
				exact("A", 10.0),
				row(Some("B"), FunnelValueCell::Null)
			]),
			Err(FunnelStagesError::NullValue {
				field: "amount".to_owned(),
				row: 2,
			})
		);
		assert_eq!(
			stages(vec![row(
				Some("A"),
				FunnelValueCell::Inexact("9007199254740993".to_owned())
			)]),
			Err(FunnelStagesError::InexactValue {
				field: "amount".to_owned(),
				row: 1,
				value: "9007199254740993".to_owned(),
			})
		);
		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			assert!(matches!(
				stages(vec![exact("A", value)]),
				Err(FunnelStagesError::NonFiniteValue { row: 1, .. })
			));
		}
		assert_eq!(
			stages(vec![exact("A", -1.0)]),
			Err(FunnelStagesError::NegativeValue {
				field: "amount".to_owned(),
				row: 1,
				value: -1.0,
			})
		);
	}

	#[test]
	fn product_0096_duplicates_precede_ordering_and_name_both_rows() {
		assert_eq!(
			stages(vec![exact("A", 10.0), exact("A", 20.0)]),
			Err(FunnelStagesError::DuplicateStage {
				stage: "A".to_owned(),
				first_row: 1,
				second_row: 2,
			})
		);
	}

	#[test]
	fn product_0096_first_positive_and_nonincreasing_rules_distinguish_plateaus() {
		assert!(matches!(
			stages(vec![exact("A", 0.0), exact("B", 0.0)]),
			Err(FunnelStagesError::FirstValueNotPositive { row: 1, .. })
		));
		assert_eq!(
			stages(vec![exact("A", 100.0), exact("B", 50.0), exact("C", 60.0)]),
			Err(FunnelStagesError::RisingValue {
				field: "amount".to_owned(),
				previous_row: 2,
				row: 3,
				previous_value: 50.0,
				value: 60.0,
			})
		);
		let plateau = stages(vec![exact("B", 100.0), exact("A", 100.0), exact("C", 50.0)])
			.expect("equal plateau");
		assert_eq!(
			plateau.iter().map(FunnelStage::stage).collect::<Vec<_>>(),
			["B", "A", "C"]
		);
		assert!(stages(vec![exact("A", 100.0), exact("B", 40.0), exact("C", 0.0)]).is_ok());
	}

	#[test]
	fn product_0096_empty_stage_collection_is_valid() {
		let empty = stages(Vec::new()).expect("empty funnel");
		assert!(empty.is_empty());
		assert_eq!(empty.first_value(), None);
	}

	#[test]
	fn product_0096_row_local_precedence_is_stage_then_value() {
		assert!(matches!(
			stages(vec![row(None, FunnelValueCell::Null)]),
			Err(FunnelStagesError::NullStage { .. })
		));
	}
}
