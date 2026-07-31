use crate::storage::encoding::compute::{EncodedRowMask, EncodingComputeRejectCode};

pub fn intersect_secondary_index_mask(
	encoded_mask: &EncodedRowMask,
	secondary_index_mask: &EncodedRowMask,
) -> Result<EncodedRowMask, EncodingComputeRejectCode> {
	encoded_mask.and(secondary_index_mask)
}
