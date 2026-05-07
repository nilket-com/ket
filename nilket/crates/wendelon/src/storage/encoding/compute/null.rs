use crate::schema::RowCount;
use crate::storage::encoding::compute::{EncodedRowMask, EncodingComputeRejectCode};

pub fn evaluate_validity_bitmap(
	validity_bitmap: Option<&[u8]>,
	rows: RowCount,
	match_nulls: bool,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	let rows_usize = usize::try_from(rows.get())
		.map_err(|_| EncodingComputeRejectCode::SelectionRangeOverflow)?;
	let Some(bytes) = validity_bitmap else {
		return Ok(if match_nulls {
			EncodedRowMask::None { rows }
		} else {
			EncodedRowMask::All { rows }
		});
	};
	if bytes.len() < rows_usize.div_ceil(8) {
		return Err(EncodingComputeRejectCode::MaskLengthMismatch);
	}
	let selected = (0..rows_usize)
		.map(|row| {
			let valid = bytes[row / 8] & (1_u8 << (row % 8)) != 0;
			if match_nulls { !valid } else { valid }
		})
		.collect::<Vec<_>>();
	Ok(EncodedRowMask::from_bool_slice(&selected))
}
