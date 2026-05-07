use crate::schema::{IpcBufferIndex, IpcColumnOrdinal, SelectivityFractionMicros};
use crate::storage::encoding::compute::{
	EncodedResidualMode, EncodingComputeConfig, EncodingComputePredicatePlan,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct EncodingComputePlan {
	pub predicate_plan: EncodingComputePredicatePlan,
	pub residual_mode: EncodedResidualMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncodingComputePlanMode {
	Exact,
	Prefilter,
	DenseFallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedColumnBufferKey {
	pub column_ordinal: IpcColumnOrdinal,
	pub buffer_ordinal: IpcBufferIndex,
}

pub fn encoded_leaf_selectivity_allowed(
	selectivity: SelectivityFractionMicros,
	config: &EncodingComputeConfig,
) -> bool {
	selectivity.get() <= config.min_selectivity_for_encoded_eval_micros.get()
}

pub fn prefilter_rejection_allowed(
	rejection: SelectivityFractionMicros,
	config: &EncodingComputeConfig,
) -> bool {
	rejection.get() >= config.min_prefilter_rejection_micros.get()
}

pub fn route_selectivity(
	selectivity: SelectivityFractionMicros,
	config: &EncodingComputeConfig,
) -> EncodingComputePlanMode {
	if encoded_leaf_selectivity_allowed(selectivity, config) {
		EncodingComputePlanMode::Exact
	} else {
		EncodingComputePlanMode::DenseFallback
	}
}
