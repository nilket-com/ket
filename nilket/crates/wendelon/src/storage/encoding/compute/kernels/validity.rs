use crate::schema::RowCount;
use crate::storage::encoding::compute::{EncodedRowMask, EncodingComputeRejectCode, SelectionRun};

pub fn evaluate_validity_bitmap(
	validity_bitmap: Option<&[u8]>,
	rows: RowCount,
	match_nulls: bool,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let Some(bytes) = validity_bitmap else {
		return Ok(if match_nulls {
			EncodedRowMask::None { rows }
		} else {
			EncodedRowMask::All { rows }
		});
	};
	validate_validity_bitmap(Some(bytes), rows)?;
	let rows_usize = rows_as_usize(rows)?;
	let selected = (0..rows_usize)
		.map(|row| {
			let valid = bitmap_get(bytes, row);
			if match_nulls { !valid } else { valid }
		})
		.collect::<Vec<_>>();
	Ok(EncodedRowMask::from_bool_slice(&selected))
}

pub fn validity_bit(
	validity_bitmap: Option<&[u8]>,
	rows: RowCount,
	row: usize,
) -> Result<bool, EncodingComputeRejectCode> {
	let rows_usize = rows_as_usize(rows)?;
	if row >= rows_usize {
		return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
	}
	let Some(bytes) = validity_bitmap else {
		return Ok(true);
	};
	validate_validity_bitmap(Some(bytes), rows)?;
	Ok(bitmap_get(bytes, row))
}

pub fn compose_value_mask_with_validity(
	value_mask: &EncodedRowMask,
	validity_bitmap: Option<&[u8]>,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	value_mask.validate()?;
	let rows = value_mask.rows();
	let Some(bytes) = validity_bitmap else {
		return Ok(value_mask.clone());
	};
	validate_validity_bitmap(Some(bytes), rows)?;
	match value_mask {
		EncodedRowMask::None { .. } => Ok(EncodedRowMask::None { rows }),
		EncodedRowMask::All { .. } => evaluate_validity_bitmap(Some(bytes), rows, false),
		EncodedRowMask::Bitmap {
			bytes: value_bytes, ..
		} => compose_bitmap_with_validity(rows, value_bytes, bytes),
		EncodedRowMask::Runs { runs, .. } => compose_runs_with_validity(rows, runs, bytes),
	}
}

fn compose_bitmap_with_validity(
	rows: RowCount,
	value_bytes: &[u8],
	validity_bytes: &[u8],
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let expected = bitmap_byte_len(rows)?;
	if value_bytes.len() != expected {
		return Err(EncodingComputeRejectCode::MaskLengthMismatch);
	}
	let mut bytes = value_bytes
		.iter()
		.zip(validity_bytes.iter())
		.take(expected)
		.map(|(value, valid)| value & valid)
		.collect::<Vec<_>>();
	mask_tail_bits(rows, &mut bytes)?;
	bitmap_mask(rows, bytes)
}

fn compose_runs_with_validity(
	rows: RowCount,
	runs: &[SelectionRun],
	validity_bytes: &[u8],
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	if let Some(runs) = run_homogeneous_validity(rows, runs, validity_bytes)? {
		return Ok(EncodedRowMask::Runs { rows, runs });
	}
	let mut values = vec![false; rows_as_usize(rows)?];
	for run in runs {
		let start = usize::try_from(run.first_row.get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
		let len = rows_as_usize(run.row_count)?;
		let end = start
			.checked_add(len)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		for row in start..end {
			values[row] = run.selected && bitmap_get(validity_bytes, row);
		}
	}
	Ok(EncodedRowMask::from_bool_slice(&values))
}

fn run_homogeneous_validity(
	rows: RowCount,
	runs: &[SelectionRun],
	validity_bytes: &[u8],
) -> Result<Option<Vec<SelectionRun>>, EncodingComputeRejectCode> {
	let mut out = Vec::with_capacity(runs.len());
	for run in runs {
		let start = usize::try_from(run.first_row.get())
			.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
		let len = rows_as_usize(run.row_count)?;
		let end = start
			.checked_add(len)
			.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
		if end > rows_as_usize(rows)? {
			return Err(EncodingComputeRejectCode::SelectionRangeOverflow);
		}
		let first_valid = bitmap_get(validity_bytes, start);
		if (start + 1..end).any(|row| bitmap_get(validity_bytes, row) != first_valid) {
			return Ok(None);
		}
		push_run(
			&mut out,
			SelectionRun {
				first_row: run.first_row,
				row_count: run.row_count,
				selected: run.selected && first_valid,
			},
		)?;
	}
	Ok(Some(out))
}

fn push_run(
	runs: &mut Vec<SelectionRun>,
	run: SelectionRun,
) -> Result<(), EncodingComputeRejectCode> {
	let Some(previous) = runs.last_mut() else {
		runs.push(run);
		return Ok(());
	};
	let previous_end = u64::from(previous.first_row.get())
		.checked_add(previous.row_count.get())
		.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?;
	if previous_end == u64::from(run.first_row.get()) && previous.selected == run.selected {
		previous.row_count = RowCount::new(
			previous
				.row_count
				.get()
				.checked_add(run.row_count.get())
				.ok_or(EncodingComputeRejectCode::SelectionRangeOverflow)?,
		);
	} else {
		runs.push(run);
	}
	Ok(())
}

fn validate_validity_bitmap(
	validity_bitmap: Option<&[u8]>,
	rows: RowCount,
) -> Result<(), EncodingComputeRejectCode> {
	let Some(bytes) = validity_bitmap else {
		return Ok(());
	};
	if bytes.len() < bitmap_byte_len(rows)? {
		return Err(EncodingComputeRejectCode::MaskLengthMismatch);
	}
	Ok(())
}

fn bitmap_mask(
	rows: RowCount,
	bytes: Vec<u8>,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let rows_usize = rows_as_usize(rows)?;
	let mut any = false;
	let mut all = true;
	for row in 0..rows_usize {
		let selected = bitmap_get(&bytes, row);
		any |= selected;
		all &= selected;
	}
	Ok(if all {
		EncodedRowMask::All { rows }
	} else if !any {
		EncodedRowMask::None { rows }
	} else {
		EncodedRowMask::Bitmap { rows, bytes }
	})
}

fn rows_as_usize(rows: RowCount) -> Result<usize, EncodingComputeRejectCode> {
	usize::try_from(rows.get()).map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)
}

fn bitmap_byte_len(rows: RowCount) -> Result<usize, EncodingComputeRejectCode> {
	Ok(rows_as_usize(rows)?.div_ceil(8))
}

fn bitmap_get(bytes: &[u8], row: usize) -> bool {
	bytes[row / 8] & (1_u8 << (row % 8)) != 0
}

fn mask_tail_bits(rows: RowCount, bytes: &mut [u8]) -> Result<(), EncodingComputeRejectCode> {
	let rows_usize = rows_as_usize(rows)?;
	let tail_bits = rows_usize % 8;
	if tail_bits == 0 {
		return Ok(());
	}
	if let Some(last) = bytes.last_mut() {
		*last &= (1_u8 << tail_bits) - 1;
	}
	Ok(())
}
