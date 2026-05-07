use crate::schema::{RecordRowIndex, RowCount};
use crate::storage::encoding::compute::EncodingComputeRejectCode;
use arrow::array::{Array, BooleanArray, BooleanBuilder};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodedRowMask {
	All {
		rows: RowCount,
	},
	None {
		rows: RowCount,
	},
	Bitmap {
		rows: RowCount,
		bytes: Vec<u8>,
	},
	Runs {
		rows: RowCount,
		runs: Vec<SelectionRun>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionRun {
	pub first_row: RecordRowIndex,
	pub row_count: RowCount,
	pub selected: bool,
}

impl EncodedRowMask {
	pub fn rows(&self) -> RowCount {
		match self {
			Self::All { rows }
			| Self::None { rows }
			| Self::Bitmap { rows, .. }
			| Self::Runs { rows, .. } => *rows,
		}
	}

	pub fn validate(&self) -> Result<(), EncodingComputeRejectCode> {
		match self {
			Self::All { .. } | Self::None { .. } => Ok(()),
			Self::Bitmap { rows, bytes } => {
				let expected = bitmap_byte_len(*rows)?;
				if bytes.len() != expected {
					return Err(EncodingComputeRejectCode::MaskLengthMismatch);
				}
				Ok(())
			}
			Self::Runs { rows, runs } => {
				let mut previous_end = 0_u64;
				for run in runs {
					if run.row_count.get() == 0 {
						return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
					}
					let first = u64::from(run.first_row.get());
					if first < previous_end {
						return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
					}
					let end = first
						.checked_add(run.row_count.get())
						.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
					if end > rows.get() {
						return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
					}
					previous_end = end;
				}
				Ok(())
			}
		}
	}

	pub fn byte_len(&self) -> usize {
		match self {
			Self::All { .. } | Self::None { .. } => 0,
			Self::Bitmap { bytes, .. } => bytes.len(),
			Self::Runs { runs, .. } => runs.len() * std::mem::size_of::<SelectionRun>(),
		}
	}

	pub fn popcount(&self) -> Result<RowCount, EncodingComputeRejectCode> {
		self.validate()?;
		let count = match self {
			Self::All { rows } => rows.get(),
			Self::None { .. } => 0,
			Self::Bitmap { rows, bytes } => {
				let mut selected = 0_u64;
				let rows = rows_as_usize(*rows)?;
				for row in 0..rows {
					if bitmap_get(bytes, row) {
						selected = selected.saturating_add(1);
					}
				}
				selected
			}
			Self::Runs { runs, .. } => runs.iter().try_fold(0_u64, |acc, run| {
				if run.selected {
					acc.checked_add(run.row_count.get())
						.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)
				} else {
					Ok(acc)
				}
			})?,
		};
		Ok(RowCount::new(count))
	}

	pub fn is_all_false(&self) -> Result<bool, EncodingComputeRejectCode> {
		Ok(self.popcount()?.get() == 0)
	}

	pub fn is_all_true(&self) -> Result<bool, EncodingComputeRejectCode> {
		Ok(self.popcount()?.get() == self.rows().get())
	}

	pub fn to_bool_vec(&self) -> Result<Vec<bool>, EncodingComputeRejectCode> {
		self.validate()?;
		match self {
			Self::All { rows } => Ok(vec![true; rows_as_usize(*rows)?]),
			Self::None { rows } => Ok(vec![false; rows_as_usize(*rows)?]),
			Self::Bitmap { rows, bytes } => {
				let rows = rows_as_usize(*rows)?;
				let mut out = Vec::with_capacity(rows);
				for row in 0..rows {
					out.push(bitmap_get(bytes, row));
				}
				Ok(out)
			}
			Self::Runs { rows, runs } => {
				let mut out = vec![false; rows_as_usize(*rows)?];
				for run in runs {
					let start = usize::try_from(run.first_row.get())
						.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
					let len = rows_as_usize(run.row_count)?;
					let end = start
						.checked_add(len)
						.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
					for value in &mut out[start..end] {
						*value = run.selected;
					}
				}
				Ok(out)
			}
		}
	}

	pub fn from_bool_slice(values: &[bool]) -> Self {
		let rows = RowCount::new(values.len() as u64);
		if values.iter().all(|value| *value) {
			return Self::All { rows };
		}
		if values.iter().all(|value| !*value) {
			return Self::None { rows };
		}
		let mut bytes = vec![0_u8; values.len().div_ceil(8)];
		for (row, selected) in values.iter().copied().enumerate() {
			if selected {
				bitmap_set(&mut bytes, row);
			}
		}
		Self::Bitmap { rows, bytes }
	}

	pub fn to_arrow_boolean_array(&self) -> Result<BooleanArray, EncodingComputeRejectCode> {
		let values = self.to_bool_vec()?;
		let mut builder = BooleanBuilder::with_capacity(values.len());
		builder.append_slice(&values);
		Ok(builder.finish())
	}

	pub fn from_arrow_boolean_array(array: &BooleanArray) -> Self {
		let values = (0..array.len())
			.map(|row| !array.is_null(row) && array.value(row))
			.collect::<Vec<_>>();
		Self::from_bool_slice(&values)
	}

	pub fn and(&self, rhs: &Self) -> Result<Self, EncodingComputeRejectCode> {
		self.combine(rhs, |left, right| left && right)
	}

	pub fn or(&self, rhs: &Self) -> Result<Self, EncodingComputeRejectCode> {
		self.combine(rhs, |left, right| left || right)
	}

	pub fn not(&self) -> Result<Self, EncodingComputeRejectCode> {
		let values = self
			.to_bool_vec()?
			.into_iter()
			.map(|selected| !selected)
			.collect::<Vec<_>>();
		Ok(Self::from_bool_slice(&values))
	}

	fn combine(
		&self,
		rhs: &Self,
		op: impl Fn(bool, bool) -> bool,
	) -> Result<Self, EncodingComputeRejectCode> {
		if self.rows() != rhs.rows() {
			return Err(EncodingComputeRejectCode::MaskLengthMismatch);
		}
		let left = self.to_bool_vec()?;
		let right = rhs.to_bool_vec()?;
		let values = left
			.into_iter()
			.zip(right)
			.map(|(left, right)| op(left, right))
			.collect::<Vec<_>>();
		Ok(Self::from_bool_slice(&values))
	}
}

fn rows_as_usize(rows: RowCount) -> Result<usize, EncodingComputeRejectCode> {
	usize::try_from(rows.get()).map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)
}

fn bitmap_byte_len(rows: RowCount) -> Result<usize, EncodingComputeRejectCode> {
	Ok(rows_as_usize(rows)?.div_ceil(8))
}

fn bitmap_get(bytes: &[u8], row: usize) -> bool {
	let byte = bytes[row / 8];
	let bit = row % 8;
	byte & (1_u8 << bit) != 0
}

fn bitmap_set(bytes: &mut [u8], row: usize) {
	let byte = &mut bytes[row / 8];
	let bit = row % 8;
	*byte |= 1_u8 << bit;
}
