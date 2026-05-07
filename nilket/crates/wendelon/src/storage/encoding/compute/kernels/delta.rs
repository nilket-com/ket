use crate::storage::encoding::compute::EncodingComputeEvaluatorKind;

pub const fn evaluator_kind() -> EncodingComputeEvaluatorKind {
	EncodingComputeEvaluatorKind::Delta
}
