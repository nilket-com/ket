//! Every installation of a backing or a published tier generation into the
//! manager state (ADR 0696, plan r3 D and F): the writes the disposal
//! evidence and the §8 lineage census must see, in one place, so an
//! architecture census can hold them to it.
use std::sync::Arc;

use super::{ManagerState, SlabId};

impl ManagerState {
	/// Install `slab_id`'s sealed backing. Every insertion into `sealed`
	/// goes through here, so a disposed slab's evidence never outlives a
	/// backing installed for it again.
	pub(super) fn install_sealed(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		slab_id: SlabId,
		slab: Arc<super::LocalSlabStore>,
	) {
		self.disposal_evidence.forget(budget, slab_id);
		self.sealed.insert(slab_id, slab);
	}

	/// Install `slab_id`'s tier pointer (see `install_sealed`).
	pub(super) fn install_tiered(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		slab_id: SlabId,
		pointer: super::TieredSlabPointer,
	) {
		self.disposal_evidence.forget(budget, slab_id);
		self.tiered.insert(slab_id, pointer);
	}

	/// Install `slab_id`'s rehydrated copy (see `install_sealed`).
	pub(super) fn install_rehydrated(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		slab_id: SlabId,
		copy: super::RehydratedSlabState,
	) {
		self.disposal_evidence.forget(budget, slab_id);
		self.rehydrated.insert(slab_id, copy);
	}

	/// Publish `layout` as `slab_id`'s current generation. Every write of
	/// `tier_layouts` goes through here or `remove_tier_layout`, the exact
	/// transitions the §8 lineage census counts.
	pub(super) fn install_tier_layout(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		slab_id: SlabId,
		layout: Arc<super::TierLayout>,
	) {
		let _new = layout.history_len();
		let replaced = self.tier_layouts.insert(slab_id, layout);
		let _old = replaced.map(|old| old.history_len());
		#[cfg(any(test, feature = "test-support"))]
		budget.census().replace(_old, Some(_new));
		let _ = budget;
	}

	/// Remove `slab_id`'s published generation (see `install_tier_layout`).
	pub(super) fn remove_tier_layout(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		slab_id: SlabId,
	) -> Option<Arc<super::TierLayout>> {
		let old = self.tier_layouts.remove(&slab_id).map(|(_, layout)| layout);
		let _old = old.as_ref().map(|old| old.history_len());
		#[cfg(any(test, feature = "test-support"))]
		budget.census().replace(_old, None);
		let _ = budget;
		old
	}
}
