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

	/// Install `slab_id`'s rehydrated copy (see `install_sealed`). Every insertion into
	/// `rehydrated` goes through here, and registers the copy's file in `ledger` in the
	/// same critical section (ADR 0132 D5).
	pub(super) fn install_rehydrated(
		&self,
		budget: &Arc<super::tier_budget::TierMemoryBudget>,
		ledger: &super::local_file_ledger::LocalFileLedger,
		slab_id: SlabId,
		copy: super::RehydratedCopy,
	) {
		self.disposal_evidence.forget(budget, slab_id);
		ledger.register(slab_id, copy.store.cached_file_identity());
		self.rehydrated.insert(slab_id, copy);
	}

	/// ADR 0132 D5: remove `slab_id`'s rehydrated copy. Every removal from `rehydrated`
	/// goes through here, and withdraws the copy's registration in `ledger` in the same
	/// critical section. With `Release` the caller records the copy's release; with
	/// `Evidence` the file stays as quarantine evidence and nothing may unlink it.
	pub(super) fn take_rehydrated(
		&self,
		ledger: &super::local_file_ledger::LocalFileLedger,
		slab_id: SlabId,
		disposition: super::local_file_ledger::CopyDisposition,
	) -> Option<super::RehydratedCopy> {
		let (_, copy) = self.rehydrated.remove(&slab_id)?;
		ledger.unregister(slab_id, disposition);
		Some(copy)
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
