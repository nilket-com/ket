use crate::plot::spec::{BubbleFieldNames, BubbleNumericField};
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BubbleObservationRow {
	pub(crate) x: Option<f64>,
	pub(crate) y: Option<f64>,
	pub(crate) size: Option<f64>,
	pub(crate) color: Option<f64>,
	pub(crate) label: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BubbleObservation {
	x: f64,
	y: f64,
	size: f64,
	color: f64,
	label: Option<String>,
}

impl BubbleObservation {
	pub(crate) fn x(&self) -> f64 {
		self.x
	}

	pub(crate) fn y(&self) -> f64 {
		self.y
	}

	pub(crate) fn size(&self) -> f64 {
		self.size
	}

	pub(crate) fn color(&self) -> f64 {
		self.color
	}

	pub(crate) fn label(&self) -> Option<&str> {
		self.label.as_deref()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct BubbleObservations {
	observations: Vec<BubbleObservation>,
	x_range: (f64, f64),
	y_range: (f64, f64),
	size_range: (f64, f64),
	color_range: (f64, f64),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BubbleObservationsError {
	NullValue {
		channel: BubbleNumericField,
		field: String,
		row: usize,
	},
	NonFiniteValue {
		channel: BubbleNumericField,
		field: String,
		row: usize,
		value: f64,
	},
}

impl BubbleObservations {
	pub(crate) fn from_rows(
		fields: BubbleFieldNames,
		rows: Vec<BubbleObservationRow>,
	) -> Result<Self, BubbleObservationsError> {
		for (index, observation) in rows.iter().enumerate() {
			let row = index + 1;
			for (channel, value) in [
				(BubbleNumericField::X, observation.x),
				(BubbleNumericField::Y, observation.y),
				(BubbleNumericField::Size, observation.size),
				(BubbleNumericField::Color, observation.color),
			] {
				let Some(value) = value else {
					return Err(BubbleObservationsError::NullValue {
						channel,
						field: fields.get(channel).to_owned(),
						row,
					});
				};
				if !value.is_finite() {
					return Err(BubbleObservationsError::NonFiniteValue {
						channel,
						field: fields.get(channel).to_owned(),
						row,
						value,
					});
				}
			}
		}

		if rows.is_empty() {
			return Ok(Self {
				observations: Vec::new(),
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
				size_range: (0.0, 0.0),
				color_range: (0.0, 0.0),
			});
		}

		let mut x_range = (f64::INFINITY, f64::NEG_INFINITY);
		let mut y_range = (f64::INFINITY, f64::NEG_INFINITY);
		let mut size_range = (f64::INFINITY, f64::NEG_INFINITY);
		let mut color_range = (f64::INFINITY, f64::NEG_INFINITY);
		let mut observations = Vec::with_capacity(rows.len());
		for row in rows {
			let x = row.x.expect("validated bubble x");
			let y = row.y.expect("validated bubble y");
			let size = row.size.expect("validated bubble size");
			let color = row.color.expect("validated bubble color");
			x_range = (x_range.0.min(x), x_range.1.max(x));
			y_range = (y_range.0.min(y), y_range.1.max(y));
			size_range = (size_range.0.min(size), size_range.1.max(size));
			color_range = (color_range.0.min(color), color_range.1.max(color));
			observations.push(BubbleObservation {
				x,
				y,
				size,
				color,
				label: row.label,
			});
		}
		let x_pad = (x_range.1 - x_range.0).max(0.1) * 0.1;
		let y_pad = (y_range.1 - y_range.0).max(0.1) * 0.1;
		x_range = (x_range.0 - x_pad, x_range.1 + x_pad);
		y_range = (y_range.0 - y_pad, y_range.1 + y_pad);
		Ok(Self {
			observations,
			x_range,
			y_range,
			size_range,
			color_range,
		})
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = &BubbleObservation> {
		self.observations.iter()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.observations.is_empty()
	}

	pub(crate) fn x_range(&self) -> (f64, f64) {
		self.x_range
	}

	pub(crate) fn y_range(&self) -> (f64, f64) {
		self.y_range
	}

	pub(crate) fn size_range(&self) -> (f64, f64) {
		self.size_range
	}

	pub(crate) fn color_range(&self) -> (f64, f64) {
		self.color_range
	}
}

impl fmt::Display for BubbleObservationsError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullValue {
				channel,
				field,
				row,
			} => write!(
				formatter,
				"bubble {channel} field `{field}` is null at row {row}"
			),
			Self::NonFiniteValue {
				channel,
				field,
				row,
				value,
			} => write!(
				formatter,
				"bubble {channel} field `{field}` is non-finite at row {row}: {value}"
			),
		}
	}
}

impl std::error::Error for BubbleObservationsError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn fields() -> BubbleFieldNames {
		BubbleFieldNames::new("x_value", "y_value", "size_value", "color_value")
	}

	fn row(
		x: Option<f64>,
		y: Option<f64>,
		size: Option<f64>,
		color: Option<f64>,
	) -> BubbleObservationRow {
		BubbleObservationRow {
			x,
			y,
			size,
			color,
			label: Some("point".to_owned()),
		}
	}

	#[test]
	fn product_0090_all_required_nulls_name_channel_field_and_one_based_row() {
		for (channel, values) in [
			(
				BubbleNumericField::X,
				[None, Some(2.0), Some(3.0), Some(4.0)],
			),
			(
				BubbleNumericField::Y,
				[Some(1.0), None, Some(3.0), Some(4.0)],
			),
			(
				BubbleNumericField::Size,
				[Some(1.0), Some(2.0), None, Some(4.0)],
			),
			(
				BubbleNumericField::Color,
				[Some(1.0), Some(2.0), Some(3.0), None],
			),
		] {
			let error = BubbleObservations::from_rows(
				fields(),
				vec![
					row(Some(1.0), Some(2.0), Some(3.0), Some(4.0)),
					row(Some(1.0), Some(2.0), Some(3.0), Some(4.0)),
					row(Some(1.0), Some(2.0), Some(3.0), Some(4.0)),
					row(values[0], values[1], values[2], values[3]),
				],
			)
			.unwrap_err();
			let BubbleObservationsError::NullValue {
				channel: actual,
				field,
				row,
			} = error
			else {
				panic!("expected null refusal")
			};
			assert_eq!(actual, channel);
			assert_eq!(field, fields().get(channel));
			assert_eq!(row, 4);
		}
	}

	#[test]
	fn product_0090_nonfinite_refusals_are_channel_ordered_with_a_finite_twin() {
		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			for (channel, values) in [
				(BubbleNumericField::X, [value, 2.0, 3.0, 4.0]),
				(BubbleNumericField::Y, [1.0, value, 3.0, 4.0]),
				(BubbleNumericField::Size, [1.0, 2.0, value, 4.0]),
				(BubbleNumericField::Color, [1.0, 2.0, 3.0, value]),
			] {
				let error = BubbleObservations::from_rows(
					fields(),
					vec![row(
						Some(values[0]),
						Some(values[1]),
						Some(values[2]),
						Some(values[3]),
					)],
				)
				.unwrap_err();
				assert!(matches!(
					error,
					BubbleObservationsError::NonFiniteValue {
						channel: actual,
						row: 1,
						..
					} if actual == channel
				));
			}
		}

		let observations = BubbleObservations::from_rows(
			fields(),
			vec![row(Some(1.0), Some(2.0), Some(3.0), Some(1e300))],
		)
		.expect("extreme finite values remain observations");
		assert_eq!(observations.color_range(), (1e300, 1e300));
	}

	#[test]
	fn product_0090_null_precedes_nonfinite_within_a_row() {
		let error = BubbleObservations::from_rows(
			fields(),
			vec![row(None, Some(f64::NAN), Some(3.0), Some(4.0))],
		)
		.unwrap_err();
		assert!(matches!(
			error,
			BubbleObservationsError::NullValue {
				channel: BubbleNumericField::X,
				row: 1,
				..
			}
		));
	}

	#[test]
	fn product_0090_domains_come_from_the_same_validated_observations() {
		let observations = BubbleObservations::from_rows(
			fields(),
			vec![
				row(Some(0.0), Some(10.0), Some(3.0), Some(-50.0)),
				row(Some(10.0), Some(20.0), Some(9.0), Some(100.0)),
			],
		)
		.expect("valid observations");
		assert_eq!(observations.x_range(), (-1.0, 11.0));
		assert_eq!(observations.y_range(), (9.0, 21.0));
		assert_eq!(observations.size_range(), (3.0, 9.0));
		assert_eq!(observations.color_range(), (-50.0, 100.0));
		for observation in observations.iter() {
			for (value, range) in [
				(observation.x(), observations.x_range()),
				(observation.y(), observations.y_range()),
				(observation.size(), observations.size_range()),
				(observation.color(), observations.color_range()),
			] {
				assert!(range.0 <= value && value <= range.1);
			}
		}
	}

	#[test]
	fn product_0090_empty_observations_keep_only_axis_fallbacks() {
		let observations = BubbleObservations::from_rows(fields(), Vec::new())
			.expect("empty observations are valid");
		assert!(observations.is_empty());
		assert_eq!(observations.x_range(), (0.0, 1.0));
		assert_eq!(observations.y_range(), (0.0, 1.0));
		assert_eq!(observations.size_range(), (0.0, 0.0));
		assert_eq!(observations.color_range(), (0.0, 0.0));
	}
}
