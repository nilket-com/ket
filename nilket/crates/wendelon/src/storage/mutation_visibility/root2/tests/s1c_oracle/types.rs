pub(super) use crate::storage::mutation_visibility::root2::types::*;
use std::collections::BTreeMap;
#[derive(Clone, PartialEq, Eq)]
pub(super) struct State2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub bootstrap: Log2,
	pub covered: Log2,
	pub fingerprint: Digest2,
	pub command_digest: Digest2,
	pub initial_epoch: u64,
	pub control_digest: Digest2,
	pub control_len: u64,
	pub selectors: BTreeMap<[u8; 44], Selector2>,
	pub authorities: BTreeMap<[u8; 24], AuthorityRegistration2>,
	pub lineages: BTreeMap<[u8; 16], LineageRecord2>,
	pub cuts: Vec<EpochCut2>,
	pub parents: BTreeMap<[u8; 73], ParentRecord2>,
}
