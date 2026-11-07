//! Evidence that a slab's backing was disposed by an authorized disposal
//! (ADR 0692 §7, ADR 0696 fast follow, plan r3 D).
//!
//! A stale holder of a location in a slab that is gone gets a typed
//! `Superseded` only when the slab is in this evidence and is `Missing` in
//! the same `state` snapshot. Otherwise it gets the existing integrity
//! failure.
//!
//! - **Entered** at the end of an authorized disposal, after its own absence
//!   proof and under its guards: LSM GC removal, maintenance without
//!   replacement, tier retirement.
//! - **Invalidated** for a slab whenever a backing for it is installed again
//!   (`ManagerState::install_*`), and wholly on any bulk authority event.
//! - **Charged and copy-on-write.** Losing evidence (a refused reservation, a
//!   bulk event) only weakens the answer to the integrity failure, so a
//!   write that cannot build its copy degrades instead of failing:
//!   recording records nothing, and forgetting clears everything.
//!
//! The set lives in `ManagerState`, so every access holds the `state` lock.
use std::sync::{Arc, RwLock};

use super::SlabId;
use super::tier_budget::{
	BudgetReservation, BudgetedSet, TierBudgetKind, TierMemoryBudget, hash_table_bytes_bound,
	try_bounded_set,
};

#[derive(Debug, Default)]
struct EvidenceSet {
	slabs: BudgetedSet<SlabId>,
	_charge: Option<BudgetReservation>,
}

#[derive(Debug, Default)]
pub(super) struct DisposalEvidence {
	current: RwLock<Arc<EvidenceSet>>,
}

impl DisposalEvidence {
	fn load(&self) -> Arc<EvidenceSet> {
		self.current
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.clone()
	}

	fn store(&self, next: EvidenceSet) {
		*self
			.current
			.write()
			.unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(next);
	}

	/// A charged copy of the current set, changed by `edit`, or `None` when
	/// it cannot be reserved or built.
	fn copy(
		&self,
		budget: &Arc<TierMemoryBudget>,
		len: usize,
		keep: impl Fn(SlabId) -> bool,
		add: Option<SlabId>,
	) -> Option<EvidenceSet> {
		let current = self.load();
		let bytes = hash_table_bytes_bound::<SlabId>(len)?;
		let mut charge = budget.try_reserve(TierBudgetKind::Disposal, bytes)?;
		let mut slabs = try_bounded_set(len).ok()?;
		for slab in current.slabs.iter().copied().filter(|slab| keep(*slab)) {
			slabs.insert(slab);
		}
		if let Some(slab) = add {
			slabs.insert(slab);
		}
		charge
			.shrink_to(u64::try_from(slabs.allocation_size()).ok()?)
			.ok()?;
		Some(EvidenceSet {
			slabs,
			_charge: Some(charge),
		})
	}

	pub(super) fn contains(&self, slab_id: SlabId) -> bool {
		self.load().slabs.contains(&slab_id)
	}

	/// An authorized disposal of `slab_id` completed. Returns false, recording
	/// nothing, when the budget cannot hold the copy.
	pub(super) fn record(&self, budget: &Arc<TierMemoryBudget>, slab_id: SlabId) -> bool {
		let current = self.load();
		if current.slabs.contains(&slab_id) {
			return true;
		}
		match self.copy(budget, current.slabs.len() + 1, |_| true, Some(slab_id)) {
			Some(next) => {
				self.store(next);
				true
			}
			None => false,
		}
	}

	/// A backing for `slab_id` is installed again: its evidence no longer
	/// holds. If the copy without it cannot be built, all evidence goes.
	pub(super) fn forget(&self, budget: &Arc<TierMemoryBudget>, slab_id: SlabId) {
		let current = self.load();
		if !current.slabs.contains(&slab_id) {
			return;
		}
		match self.copy(
			budget,
			current.slabs.len() - 1,
			|slab| slab != slab_id,
			None,
		) {
			Some(next) => self.store(next),
			None => self.clear(),
		}
	}

	/// A bulk authority event: no disposal evidence survives it.
	pub(super) fn clear(&self) {
		self.store(EvidenceSet::default());
	}

	#[cfg(test)]
	pub(super) fn len_for_test(&self) -> usize {
		self.load().slabs.len()
	}
}

impl super::SlabManager {
	/// An authorized disposal of `slab_id` completed (its own absence proof
	/// and guards are the caller's). Recorded only while the slab is still
	/// missing in this `state` snapshot, so a backing installed again in the
	/// meantime is never shadowed by stale evidence.
	pub(super) async fn record_disposal_evidence(&self, slab_id: SlabId) {
		let state = self.state.lock().await;
		let missing = state.active.slab_id() != slab_id
			&& !state.sealed.contains_key(&slab_id)
			&& !state.tiered.contains_key(&slab_id)
			&& !state.rehydrated.contains_key(&slab_id);
		if missing && !state.disposal_evidence.record(&self.tier_budget, slab_id) {
			tracing::debug!(
				slab_id = slab_id.get(),
				"no budget for disposal evidence; a stale holder gets the integrity failure"
			);
		}
	}

	/// A bulk authority event: forget every disposal.
	pub(super) async fn clear_disposal_evidence(&self) {
		self.state.lock().await.disposal_evidence.clear();
	}
}
