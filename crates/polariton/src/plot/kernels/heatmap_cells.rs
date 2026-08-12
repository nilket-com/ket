use std::collections::{BTreeSet, HashMap};
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HeatmapCellRow {
	pub(crate) x: Option<String>,
	pub(crate) y: Option<String>,
	pub(crate) value: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeatmapCells {
	x_categories: Vec<String>,
	y_categories: Vec<String>,
	values: Vec<Vec<Option<f64>>>,
	domain: (f64, f64),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum HeatmapCellsError {
	NonFiniteValue {
		row: usize,
		value: f64,
	},
	NullX {
		row: usize,
	},
	NullY {
		row: usize,
	},
	DuplicateCell {
		x: String,
		y: String,
		first_row: usize,
		second_row: usize,
	},
}

impl HeatmapCells {
	pub(crate) fn from_rows(rows: Vec<HeatmapCellRow>) -> Result<Self, HeatmapCellsError> {
		for (row, cell) in rows.iter().enumerate() {
			if let Some(value) = cell.value
				&& !value.is_finite()
			{
				return Err(HeatmapCellsError::NonFiniteValue { row, value });
			}
			if cell.x.is_none() {
				return Err(HeatmapCellsError::NullX { row });
			}
			if cell.y.is_none() {
				return Err(HeatmapCellsError::NullY { row });
			}
		}

		let mut first_rows = HashMap::with_capacity(rows.len());
		for (row, cell) in rows.iter().enumerate() {
			let x = cell.x.as_deref().expect("row-local validation requires x");
			let y = cell.y.as_deref().expect("row-local validation requires y");
			if let Some(first_row) = first_rows.insert((x, y), row) {
				return Err(HeatmapCellsError::DuplicateCell {
					x: x.to_owned(),
					y: y.to_owned(),
					first_row,
					second_row: row,
				});
			}
		}

		if rows.is_empty() {
			return Ok(Self {
				x_categories: vec!["No Data".to_owned()],
				y_categories: vec!["No Data".to_owned()],
				values: vec![vec![None]],
				domain: (0.0, 0.0),
			});
		}

		let x_categories: Vec<_> = rows
			.iter()
			.map(|row| row.x.as_ref().expect("validated x").clone())
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect();
		let y_categories: Vec<_> = rows
			.iter()
			.map(|row| row.y.as_ref().expect("validated y").clone())
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect();
		let x_indices: HashMap<_, _> = x_categories
			.iter()
			.enumerate()
			.map(|(index, category)| (category.as_str(), index))
			.collect();
		let y_indices: HashMap<_, _> = y_categories
			.iter()
			.enumerate()
			.map(|(index, category)| (category.as_str(), index))
			.collect();
		let mut values = vec![vec![None; y_categories.len()]; x_categories.len()];
		let mut low = f64::INFINITY;
		let mut high = f64::NEG_INFINITY;
		for row in rows {
			let x = row.x.expect("validated x");
			let y = row.y.expect("validated y");
			let x_index = x_indices[x.as_str()];
			let y_index = y_indices[y.as_str()];
			values[x_index][y_index] = row.value;
			if let Some(value) = row.value {
				low = low.min(value);
				high = high.max(value);
			}
		}
		let domain = if low.is_infinite() {
			(0.0, 0.0)
		} else {
			(low, high)
		};
		Ok(Self {
			x_categories,
			y_categories,
			values,
			domain,
		})
	}

	pub(crate) fn x_categories(&self) -> &[String] {
		&self.x_categories
	}

	pub(crate) fn y_categories(&self) -> &[String] {
		&self.y_categories
	}

	pub(crate) fn value(&self, x: usize, y: usize) -> Option<f64> {
		self.values
			.get(x)
			.and_then(|column| column.get(y))
			.copied()
			.flatten()
	}

	pub(crate) fn domain(&self) -> (f64, f64) {
		self.domain
	}
}

impl fmt::Display for HeatmapCellsError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NonFiniteValue { row, value } => write!(
				formatter,
				"heatmap value at row {} is non-finite: {value}",
				row + 1
			),
			Self::NullX { row } => {
				write!(formatter, "heatmap x key is null at row {}", row + 1)
			}
			Self::NullY { row } => {
				write!(formatter, "heatmap y key is null at row {}", row + 1)
			}
			Self::DuplicateCell {
				x,
				y,
				first_row,
				second_row,
			} => write!(
				formatter,
				"heatmap cell (`{x}`, `{y}`) is duplicated at rows {} and {}; aggregate duplicate cells in the relation before heatmap",
				first_row + 1,
				second_row + 1
			),
		}
	}
}

impl std::error::Error for HeatmapCellsError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(x: Option<&str>, y: Option<&str>, value: Option<f64>) -> HeatmapCellRow {
		HeatmapCellRow {
			x: x.map(str::to_owned),
			y: y.map(str::to_owned),
			value,
		}
	}

	#[test]
	fn product_0088_validation_is_two_phase_and_key_uniqueness_is_total() {
		let error = HeatmapCells::from_rows(vec![row(None, None, Some(f64::NAN))]).unwrap_err();
		assert!(matches!(
			error,
			HeatmapCellsError::NonFiniteValue { row: 0, .. }
		));

		let error = HeatmapCells::from_rows(vec![row(None, None, Some(1.0))]).unwrap_err();
		assert_eq!(error, HeatmapCellsError::NullX { row: 0 });

		let error = HeatmapCells::from_rows(vec![
			row(Some("a"), Some("u"), Some(1.0)),
			row(Some("a"), Some("u"), Some(9.0)),
			row(Some("b"), None, Some(2.0)),
		])
		.unwrap_err();
		assert_eq!(error, HeatmapCellsError::NullY { row: 2 });

		for values in [
			[Some(1.0), Some(9.0)],
			[Some(1.0), Some(1.0)],
			[None, Some(1.0)],
			[None, None],
		] {
			let error = HeatmapCells::from_rows(vec![
				row(Some("a"), Some("u"), values[0]),
				row(Some("a"), Some("u"), values[1]),
			])
			.unwrap_err();
			assert_eq!(
				error,
				HeatmapCellsError::DuplicateCell {
					x: "a".to_owned(),
					y: "u".to_owned(),
					first_row: 0,
					second_row: 1,
				}
			);
			assert!(
				error
					.to_string()
					.contains("aggregate duplicate cells in the relation")
			);
		}
	}

	#[test]
	fn product_0088_sparse_null_and_signed_cells_derive_one_grid_and_domain() {
		let cells = HeatmapCells::from_rows(vec![
			row(Some("null"), Some("u"), Some(-50.0)),
			row(Some("b"), Some("v"), None),
			row(Some("c"), Some("u"), Some(100.0)),
		])
		.unwrap();
		assert_eq!(cells.x_categories(), ["b", "c", "null"]);
		assert_eq!(cells.y_categories(), ["u", "v"]);
		assert_eq!(cells.domain(), (-50.0, 100.0));
		assert_eq!(cells.value(0, 1), None);
		assert_eq!(cells.value(1, 0), Some(100.0));
		assert_eq!(cells.value(2, 0), Some(-50.0));

		let empty = HeatmapCells::from_rows(Vec::new()).unwrap();
		assert_eq!(empty.x_categories(), ["No Data"]);
		assert_eq!(empty.y_categories(), ["No Data"]);
		assert_eq!(empty.value(0, 0), None);
		assert_eq!(empty.domain(), (0.0, 0.0));
	}
}
