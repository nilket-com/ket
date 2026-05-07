use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;

pub const fn plain_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::DictionaryPlain
}

pub const fn bitpacked_evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::DictionaryBitPacked
}
