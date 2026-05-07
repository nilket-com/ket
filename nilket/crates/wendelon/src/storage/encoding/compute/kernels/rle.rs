use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;

pub const fn plain_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::RlePlain
}

pub const fn bitpacked_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::RleBitPacked
}
