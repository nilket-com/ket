use crate::schema::RowCount;
use crate::storage::encoding::compute::{EncodedRowMask, EncodingComputeRejectCode};

pub fn evaluate_validity_bitmap(
	validity_bitmap: Option<&[u8]>,
	rows: RowCount,
	match_nulls: bool,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	crate::storage::encoding::compute::null::evaluate_validity_bitmap(
		validity_bitmap,
		rows,
		match_nulls,
	)
}
