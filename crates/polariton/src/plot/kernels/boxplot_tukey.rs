use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct TukeyBoxRow {
	pub category: String,
	pub whisker_low: f64,
	pub q1: f64,
	pub median: f64,
	pub q3: f64,
	pub whisker_high: f64,
	pub outlier: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TukeyBox {
	whisker_low: f64,
	q1: f64,
	median: f64,
	q3: f64,
	whisker_high: f64,
	outliers: Vec<f64>,
}

impl TukeyBox {
	pub fn whisker_low(&self) -> f64 {
		self.whisker_low
	}

	pub fn q1(&self) -> f64 {
		self.q1
	}

	pub fn median(&self) -> f64 {
		self.median
	}

	pub fn q3(&self) -> f64 {
		self.q3
	}

	pub fn whisker_high(&self) -> f64 {
		self.whisker_high
	}

	pub fn outliers(&self) -> &[f64] {
		&self.outliers
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct TukeyBoxes {
	categories: Vec<String>,
	boxes: Vec<TukeyBox>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TukeyBoxesError {
	NonFiniteSummary {
		category: String,
		field: &'static str,
	},
	NonFiniteOutlier {
		category: String,
		ordinal: usize,
	},
	InvalidSummaryOrder {
		category: String,
	},
	OutlierInsideWhiskers {
		category: String,
		ordinal: usize,
	},
	SummaryMismatch {
		category: String,
		field: &'static str,
	},
	MultipleNullRows {
		category: String,
	},
	MixedNullAndOutlierRows {
		category: String,
	},
}

#[derive(Clone, Debug)]
struct BoxBuilder {
	summary: [f64; 5],
	outliers: Vec<f64>,
	null_seen: bool,
}

impl TukeyBoxes {
	pub fn new(rows: Vec<TukeyBoxRow>) -> Result<Self, TukeyBoxesError> {
		let mut categories = Vec::new();
		let mut builders: Vec<BoxBuilder> = Vec::new();
		for row in rows {
			let summary = [
				row.whisker_low,
				row.q1,
				row.median,
				row.q3,
				row.whisker_high,
			];
			for (field, value) in [
				("whisker_low", row.whisker_low),
				("q1", row.q1),
				("median", row.median),
				("q3", row.q3),
				("whisker_high", row.whisker_high),
			] {
				if !value.is_finite() {
					return Err(TukeyBoxesError::NonFiniteSummary {
						category: row.category,
						field,
					});
				}
			}
			if !(row.whisker_low <= row.q1
				&& row.q1 <= row.median
				&& row.median <= row.q3
				&& row.q3 <= row.whisker_high)
			{
				return Err(TukeyBoxesError::InvalidSummaryOrder {
					category: row.category,
				});
			}
			if let Some(outlier) = row.outlier
				&& !outlier.is_finite()
			{
				let ordinal = categories
					.iter()
					.position(|category| category == &row.category)
					.and_then(|index| builders.get(index))
					.map_or(0, |builder| builder.outliers.len());
				return Err(TukeyBoxesError::NonFiniteOutlier {
					category: row.category,
					ordinal,
				});
			}

			let index = if let Some(index) = categories
				.iter()
				.position(|category| category == &row.category)
			{
				index
			} else {
				categories.push(row.category.clone());
				builders.push(BoxBuilder {
					summary,
					outliers: Vec::new(),
					null_seen: false,
				});
				builders.len() - 1
			};
			let builder = &mut builders[index];
			for (field_index, field) in ["whisker_low", "q1", "median", "q3", "whisker_high"]
				.into_iter()
				.enumerate()
			{
				if builder.summary[field_index].to_bits() != summary[field_index].to_bits() {
					return Err(TukeyBoxesError::SummaryMismatch {
						category: row.category,
						field,
					});
				}
			}
			match row.outlier {
				Some(outlier) => {
					if builder.null_seen {
						return Err(TukeyBoxesError::MixedNullAndOutlierRows {
							category: row.category,
						});
					}
					let ordinal = builder.outliers.len();
					if outlier >= row.whisker_low && outlier <= row.whisker_high {
						return Err(TukeyBoxesError::OutlierInsideWhiskers {
							category: row.category,
							ordinal,
						});
					}
					builder.outliers.push(outlier);
				}
				None => {
					if !builder.outliers.is_empty() {
						return Err(TukeyBoxesError::MixedNullAndOutlierRows {
							category: row.category,
						});
					}
					if builder.null_seen {
						return Err(TukeyBoxesError::MultipleNullRows {
							category: row.category,
						});
					}
					builder.null_seen = true;
				}
			}
		}
		let boxes = builders
			.into_iter()
			.map(|builder| TukeyBox {
				whisker_low: builder.summary[0],
				q1: builder.summary[1],
				median: builder.summary[2],
				q3: builder.summary[3],
				whisker_high: builder.summary[4],
				outliers: builder.outliers,
			})
			.collect();
		Ok(Self { categories, boxes })
	}

	pub fn iter(&self) -> impl Iterator<Item = (&str, &TukeyBox)> {
		self.categories
			.iter()
			.map(String::as_str)
			.zip(self.boxes.iter())
	}

	pub fn categories(&self) -> &[String] {
		&self.categories
	}

	pub fn ranges(&self) -> (f64, f64) {
		let mut low = f64::INFINITY;
		let mut high = f64::NEG_INFINITY;
		for tukey_box in &self.boxes {
			low = low.min(tukey_box.whisker_low);
			high = high.max(tukey_box.whisker_high);
			for outlier in &tukey_box.outliers {
				low = low.min(*outlier);
				high = high.max(*outlier);
			}
		}
		if self.boxes.is_empty() {
			low = 0.0;
			high = 1.0;
		}
		let pad = (high - low).max(0.001) * 0.1;
		(low - pad, high + pad)
	}
}

impl fmt::Display for TukeyBoxesError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NonFiniteSummary { category, field } => {
				write!(
					formatter,
					"box category `{category}` has non-finite `{field}`"
				)
			}
			Self::NonFiniteOutlier { category, ordinal } => write!(
				formatter,
				"box category `{category}` has non-finite outlier {ordinal}"
			),
			Self::InvalidSummaryOrder { category } => write!(
				formatter,
				"box category `{category}` does not satisfy whisker_low <= q1 <= median <= q3 <= whisker_high"
			),
			Self::OutlierInsideWhiskers { category, ordinal } => write!(
				formatter,
				"box category `{category}` outlier {ordinal} lies inside the whisker span"
			),
			Self::SummaryMismatch { category, field } => write!(
				formatter,
				"box category `{category}` repeats a different `{field}`"
			),
			Self::MultipleNullRows { category } => write!(
				formatter,
				"box category `{category}` has more than one null outlier row"
			),
			Self::MixedNullAndOutlierRows { category } => write!(
				formatter,
				"box category `{category}` mixes null and non-null outlier rows"
			),
		}
	}
}

impl std::error::Error for TukeyBoxesError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(category: &str, outlier: Option<f64>) -> TukeyBoxRow {
		TukeyBoxRow {
			category: category.to_owned(),
			whisker_low: 0.0,
			q1: 1.0,
			median: 2.0,
			q3: 3.0,
			whisker_high: 4.0,
			outlier,
		}
	}

	#[test]
	fn product_0081_duplicates_and_first_seen_orders_survive_ingestion() {
		let boxes = TukeyBoxes::new(vec![
			row("B", Some(99.0)),
			row("B", Some(-10.0)),
			row("B", Some(99.0)),
			row("A", None),
		])
		.unwrap();
		assert_eq!(boxes.categories(), ["B", "A"]);
		assert_eq!(boxes.boxes[0].outliers(), [99.0, -10.0, 99.0]);
		assert!(boxes.boxes[1].outliers().is_empty());
	}

	#[test]
	fn product_0081_validation_names_each_invalid_relation_shape() {
		let mut invalid = row("finite", None);
		invalid.q1 = f64::NAN;
		assert!(matches!(
			TukeyBoxes::new(vec![invalid]),
			Err(TukeyBoxesError::NonFiniteSummary { field: "q1", .. })
		));
		assert!(TukeyBoxes::new(vec![row("finite", None)]).is_ok());

		assert!(matches!(
			TukeyBoxes::new(vec![row("outlier", Some(f64::INFINITY))]),
			Err(TukeyBoxesError::NonFiniteOutlier { .. })
		));
		assert!(TukeyBoxes::new(vec![row("outlier", Some(5.0))]).is_ok());

		let mut invalid = row("order", None);
		invalid.q1 = 2.5;
		assert!(matches!(
			TukeyBoxes::new(vec![invalid]),
			Err(TukeyBoxesError::InvalidSummaryOrder { .. })
		));
		assert!(TukeyBoxes::new(vec![row("order", None)]).is_ok());

		assert!(matches!(
			TukeyBoxes::new(vec![row("inside", Some(2.5))]),
			Err(TukeyBoxesError::OutlierInsideWhiskers { .. })
		));
		assert!(TukeyBoxes::new(vec![row("inside", Some(5.0))]).is_ok());

		let first = row("mismatch", Some(5.0));
		let mut second = row("mismatch", Some(6.0));
		second.q1 = f64::from_bits(first.q1.to_bits() ^ 1);
		assert!(matches!(
			TukeyBoxes::new(vec![first, second]),
			Err(TukeyBoxesError::SummaryMismatch { field: "q1", .. })
		));
		assert!(TukeyBoxes::new(vec![row("match", Some(5.0)), row("match", Some(6.0))]).is_ok());

		assert!(matches!(
			TukeyBoxes::new(vec![row("null", None), row("null", None)]),
			Err(TukeyBoxesError::MultipleNullRows { .. })
		));
		assert!(TukeyBoxes::new(vec![row("null", None)]).is_ok());

		assert!(matches!(
			TukeyBoxes::new(vec![row("mixed", None), row("mixed", Some(5.0))]),
			Err(TukeyBoxesError::MixedNullAndOutlierRows { .. })
		));
		assert!(TukeyBoxes::new(vec![row("mixed", Some(5.0)), row("mixed", Some(6.0))]).is_ok());
	}

	#[test]
	fn product_0081_summary_equality_is_bitwise_but_outlier_clusters_are_numeric() {
		let first = TukeyBoxRow {
			whisker_low: -0.0,
			..row("bits", Some(-1.0))
		};
		let second = TukeyBoxRow {
			whisker_low: 0.0,
			..row("bits", Some(-2.0))
		};
		assert!(matches!(
			TukeyBoxes::new(vec![first, second]),
			Err(TukeyBoxesError::SummaryMismatch {
				field: "whisker_low",
				..
			})
		));
	}

	#[test]
	fn product_0081_domain_includes_outliers_padding_and_empty_fallback() {
		let low = TukeyBoxRow {
			category: "lowhigh".to_owned(),
			whisker_low: 30.0,
			q1: 31.0,
			median: 33.0,
			q3: 34.0,
			whisker_high: 35.0,
			outlier: Some(-50.0),
		};
		let high = TukeyBoxRow {
			outlier: Some(200.0),
			..low.clone()
		};
		assert_eq!(
			TukeyBoxes::new(vec![low, high]).unwrap().ranges(),
			(-75.0, 225.0)
		);
		assert_eq!(TukeyBoxes::new(Vec::new()).unwrap().ranges(), (-0.1, 1.1));

		let flat = TukeyBoxRow {
			category: "flat".to_owned(),
			whisker_low: 5.0,
			q1: 5.0,
			median: 5.0,
			q3: 5.0,
			whisker_high: 5.0,
			outlier: None,
		};
		let range = TukeyBoxes::new(vec![flat]).unwrap().ranges();
		assert_eq!(range, (4.9999, 5.0001));
	}
}
