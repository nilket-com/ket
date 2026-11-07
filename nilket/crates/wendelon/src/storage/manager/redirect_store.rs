//! Cross-slab read redirects published by a maintenance rewrite (ADR 0692
//! §7, ADR 0696 fast follow, plan r3 C).
//!
//! A rewrite of slab S into slab T maps every relocated old location in S to
//! its new location in T. The map is optional: it is built and charged to
//! the node budget before the rewrite's durable publication (D2), and a map
//! that cannot be reserved or built is skipped and counted. The old slab
//! stays sealed until its disposal, so a stale holder without a redirect
//! still reads its version there.
//!
//! - **Per-source map.** Immutable and sorted, with one publication bit per
//!   entry. A bit is set only when the key edit applies its pair, so the
//!   idempotent and refused branches publish nothing. A `revoked` flag that
//!   allocates nothing lets a writer withdraw a map before, or instead of,
//!   removing it.
//! - **Outer store.** Copy-on-write: every writer builds a new charged copy
//!   under one writer mutex, which holds no await, and swaps it in. Readers
//!   hold the `Arc` they loaded, so a removed map lives, still charged, until
//!   its last reader drops it.
//! - **Lifetime.** A source map is dropped whole when its old slab is
//!   disposed (LSM GC, maintenance without replacement) or when its source or
//!   target slab is retired from the tier; a stale holder then gets the
//!   disposal-evidence answer, never another row.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use super::tier_budget::{
	BudgetReservation, BudgetedMap, TierBudgetKind, TierMemoryBudget, checked_sum,
	hash_table_bytes_bound, try_bounded_map, vec_bytes,
};
use super::{RecordLocation, SlabId};

/// One rewrite's redirects out of its old slab.
#[derive(Debug)]
pub(super) struct SourceRedirects {
	source: SlabId,
	target: SlabId,
	/// Sorted by old location; old locations are distinct.
	entries: Box<[(RecordLocation, RecordLocation)]>,
	/// `published[i]` is set (Release) when `entries[i]`'s pair is applied.
	published: Box<[AtomicBool]>,
	revoked: AtomicBool,
	_charge: BudgetReservation,
}

impl SourceRedirects {
	/// The bytes a map of `entries` pairs holds, including its own `Arc`.
	pub(super) fn bytes_bound(entries: usize) -> Option<u64> {
		checked_sum(&[
			vec_bytes::<(RecordLocation, RecordLocation)>(entries),
			vec_bytes::<AtomicBool>(entries),
			u64::try_from(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()).ok(),
		])
	}

	/// The map of `pairs` (old location in `source`, new location in
	/// `target`), reserved before it is built. `None` when the budget cannot
	/// hold it, an allocation is refused, a pair leaves `source` or
	/// `target`, or one old location names two new ones: the rewrite then
	/// publishes no redirects.
	pub(super) fn try_build(
		budget: &Arc<TierMemoryBudget>,
		source: SlabId,
		target: SlabId,
		count: usize,
		pairs: impl Iterator<Item = (RecordLocation, RecordLocation)>,
	) -> Option<Arc<Self>> {
		let charge = budget.try_reserve(TierBudgetKind::Redirect, Self::bytes_bound(count)?)?;
		let mut entries = Vec::new();
		entries.try_reserve_exact(count).ok()?;
		for (old, new) in pairs {
			if old.slab_id != source || new.slab_id != target || entries.len() == count {
				return None;
			}
			entries.push((old, new));
		}
		entries.sort_unstable_by_key(|(old, _)| location_key(old));
		entries.dedup();
		if entries
			.windows(2)
			.any(|pair| location_key(&pair[0].0) == location_key(&pair[1].0))
		{
			return None;
		}
		let mut published = Vec::new();
		published.try_reserve_exact(entries.len()).ok()?;
		published.resize_with(entries.len(), || AtomicBool::new(false));
		Some(Arc::new(Self {
			source,
			target,
			entries: entries.into_boxed_slice(),
			published: published.into_boxed_slice(),
			revoked: AtomicBool::new(false),
			_charge: charge,
		}))
	}

	fn slot(&self, old: &RecordLocation) -> Option<usize> {
		self.entries
			.binary_search_by_key(&location_key(old), |(entry, _)| location_key(entry))
			.ok()
			.filter(|slot| self.entries[*slot].0 == *old)
	}

	/// The key edit applied `old`'s pair: publish its redirect.
	pub(super) fn publish(&self, old: &RecordLocation) {
		if let Some(slot) = self.slot(old) {
			self.published[slot].store(true, Ordering::Release);
		}
	}

	/// The published redirect of `old`, unless this map is revoked.
	fn resolve(&self, old: &RecordLocation) -> Option<RecordLocation> {
		if self.revoked.load(Ordering::Acquire) {
			return None;
		}
		let slot = self.slot(old)?;
		if !self.published[slot].load(Ordering::Acquire) {
			return None;
		}
		// A revocation that raced the lookup wins.
		(!self.revoked.load(Ordering::Acquire)).then_some(self.entries[slot].1)
	}

	fn touches(&self, slab_id: SlabId) -> bool {
		self.source == slab_id || self.target == slab_id
	}

	#[cfg(test)]
	pub(super) fn is_revoked(&self) -> bool {
		self.revoked.load(Ordering::Acquire)
	}
}

/// The order of a map's old locations: slab, offset and row index identify
/// one stored row, and a hit is checked against the full location.
fn location_key(location: &RecordLocation) -> (u64, u64, u32) {
	(
		location.slab_id.get(),
		location.offset,
		location.record_row_index.get(),
	)
}

/// One immutable generation of the outer store.
#[derive(Debug, Default)]
struct RedirectStore {
	maps: BudgetedMap<SlabId, Arc<SourceRedirects>>,
	_charge: Option<BudgetReservation>,
}

/// The node's cross-slab redirects.
#[derive(Debug, Default)]
pub(super) struct RedirectStores {
	current: RwLock<Arc<RedirectStore>>,
	writer: Mutex<()>,
}

/// The reserved room for one install, taken before the rewrite's D2.
#[derive(Debug)]
#[must_use = "dropping it refunds the room"]
pub(super) struct RedirectInstall {
	map: Arc<SourceRedirects>,
	room: BudgetReservation,
}

impl RedirectInstall {
	pub(super) fn map(&self) -> &Arc<SourceRedirects> {
		&self.map
	}
}

impl RedirectStores {
	fn load(&self) -> Arc<RedirectStore> {
		self.current
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.clone()
	}

	fn swap(&self, next: RedirectStore) {
		*self
			.current
			.write()
			.unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(next);
	}

	fn write_lock(&self) -> std::sync::MutexGuard<'_, ()> {
		self.writer
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// One hop: the published redirect of `location`, if any.
	pub(super) fn resolve(&self, location: &RecordLocation) -> Option<RecordLocation> {
		self.load()
			.maps
			.get(&location.slab_id)
			.and_then(|map| map.resolve(location))
	}

	/// Reserve the outer copy that will add `map`. Maintenance installs are
	/// serialized by the maintenance publication gate, which the caller
	/// holds through `install`, and every other writer only removes maps, so
	/// the store holds at most one more map at install than now.
	pub(super) fn reserve_install(
		&self,
		budget: &Arc<TierMemoryBudget>,
		map: Arc<SourceRedirects>,
	) -> Option<RedirectInstall> {
		let next = self.load().maps.len().checked_add(1)?;
		let bytes = hash_table_bytes_bound::<(SlabId, Arc<SourceRedirects>)>(next)?;
		let room = budget.try_reserve(TierBudgetKind::RedirectStore, bytes)?;
		Some(RedirectInstall { map, room })
	}

	/// Install a reserved map in place of any earlier one for its source.
	/// Maps revoked earlier (a removal that could not build its copy) are
	/// left out, so this successful write removes them as any other does.
	/// Returns false, installing nothing, when the copy cannot be built in
	/// the reserved room; the rewrite then publishes no redirects.
	pub(super) fn install(&self, install: RedirectInstall) -> bool {
		let RedirectInstall { map, mut room } = install;
		let _writer = self.write_lock();
		let current = self.load();
		// The existing live maps of other sources, and the new one: at most
		// the reserved count (the store's length then, plus one).
		let kept = |source: &SlabId, existing: &Arc<SourceRedirects>| {
			*source != map.source && !existing.revoked.load(Ordering::Acquire)
		};
		let len = current
			.maps
			.iter()
			.filter(|(source, existing)| kept(source, existing))
			.count() + 1;
		let Ok(mut maps) = try_bounded_map(len) else {
			return false;
		};
		for (source, existing) in &current.maps {
			if kept(source, existing) {
				maps.insert(*source, existing.clone());
			}
		}
		if let Some(replaced) = current.maps.get(&map.source) {
			replaced.revoked.store(true, Ordering::Release);
		}
		maps.insert(map.source, map);
		let Ok(actual) = u64::try_from(maps.allocation_size()) else {
			return false;
		};
		if room.shrink_to(actual).is_err() {
			return false;
		}
		self.swap(RedirectStore {
			maps,
			_charge: Some(room),
		});
		true
	}

	/// Drop every map whose source or target is `slab_id`: it was disposed or
	/// left the tier, so no redirect into or out of it can still be served.
	pub(super) fn remove_touching(&self, budget: &Arc<TierMemoryBudget>, slab_id: SlabId) {
		self.remove_where(budget, |map| map.touches(slab_id));
	}

	/// Revoke the matching maps first, which allocates nothing and is always
	/// possible, then swap in a charged copy without them. If that copy
	/// cannot be reserved or built, the revoked maps stay, still charged,
	/// until the next successful write removes them.
	fn remove_where(
		&self,
		budget: &Arc<TierMemoryBudget>,
		matches: impl Fn(&SourceRedirects) -> bool,
	) {
		let _writer = self.write_lock();
		let current = self.load();
		let mut any = false;
		for map in current.maps.values().filter(|map| matches(map)) {
			map.revoked.store(true, Ordering::Release);
			any = true;
		}
		let stale = current
			.maps
			.values()
			.filter(|map| map.revoked.load(Ordering::Acquire))
			.count();
		if !any && stale == 0 {
			return;
		}
		let len = current.maps.len() - stale;
		let Some(bytes) = hash_table_bytes_bound::<(SlabId, Arc<SourceRedirects>)>(len) else {
			return;
		};
		let Some(mut room) = budget.try_reserve(TierBudgetKind::RedirectStore, bytes) else {
			return;
		};
		let Ok(mut maps) = try_bounded_map(len) else {
			return;
		};
		for (source, map) in &current.maps {
			if !map.revoked.load(Ordering::Acquire) {
				maps.insert(*source, map.clone());
			}
		}
		let Ok(actual) = u64::try_from(maps.allocation_size()) else {
			return;
		};
		if room.shrink_to(actual).is_err() {
			return;
		}
		self.swap(RedirectStore {
			maps,
			_charge: Some(room),
		});
	}

	#[cfg(test)]
	pub(super) fn source_map_for_test(&self, source: SlabId) -> Option<Arc<SourceRedirects>> {
		self.load().maps.get(&source).cloned()
	}

	#[cfg(test)]
	pub(super) fn len_for_test(&self) -> usize {
		self.load().maps.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::manager::{HybridTimestamp, PlacementSemantics, RecordRowIndex};

	fn location(slab: u64, offset: u64) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::new(slab),
			offset,
			length: 64,
			hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: 1,
			},
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		}
	}

	fn map(budget: &Arc<TierMemoryBudget>, source: u64, target: u64) -> Arc<SourceRedirects> {
		let pairs = (0..4).map(|i| (location(source, i * 64), location(target, i * 64)));
		SourceRedirects::try_build(budget, SlabId::new(source), SlabId::new(target), 4, pairs)
			.unwrap()
	}

	fn installed(
		stores: &RedirectStores,
		budget: &Arc<TierMemoryBudget>,
		source: u64,
		target: u64,
	) {
		let map = map(budget, source, target);
		for i in 0..4 {
			map.publish(&location(source, i * 64));
		}
		assert!(stores.install(stores.reserve_install(budget, map).unwrap()));
	}

	/// A prepared map and its reserved install room dropped before D3 (the
	/// rewrite was cancelled or failed after D2) refund every byte.
	#[test]
	fn a_prepared_map_dropped_before_install_refunds_everything() {
		let budget = TierMemoryBudget::new(1 << 20);
		let stores = RedirectStores::default();
		let install = stores.reserve_install(&budget, map(&budget, 1, 2)).unwrap();
		assert!(budget.used_by(TierBudgetKind::Redirect) > 0);
		assert!(budget.used_by(TierBudgetKind::RedirectStore) > 0);
		drop(install);
		assert_eq!(budget.used(), 0);
		assert_eq!(stores.len_for_test(), 0);
	}

	/// A disposal while the budget is exhausted revokes the map at once (no
	/// allocation), so it serves nothing; the map is removed, and its charge
	/// returned, by the next write that can build its copy.
	#[test]
	fn a_disposal_under_an_exhausted_budget_revokes_now_and_removes_later() {
		let budget = TierMemoryBudget::new(1 << 20);
		let stores = RedirectStores::default();
		installed(&stores, &budget, 1, 2);
		installed(&stores, &budget, 3, 4);
		assert_eq!(stores.resolve(&location(1, 0)), Some(location(2, 0)));
		// The copy that keeps 3 -> 4 needs room the budget no longer has.
		budget.set_limit_for_test(budget.used());
		stores.remove_touching(&budget, SlabId::new(2));
		assert_eq!(stores.resolve(&location(1, 0)), None, "revoked at once");
		assert_eq!(stores.len_for_test(), 2, "still stored, still charged");
		assert!(
			stores
				.source_map_for_test(SlabId::new(1))
				.unwrap()
				.is_revoked()
		);
		assert_eq!(
			stores.resolve(&location(3, 0)),
			Some(location(4, 0)),
			"the other map still serves"
		);
		budget.set_limit_for_test(1 << 20);
		stores.remove_touching(&budget, SlabId::new(99));
		assert_eq!(
			stores.len_for_test(),
			1,
			"removed by the next successful write"
		);
		assert!(stores.source_map_for_test(SlabId::new(1)).is_none());
	}

	/// A successful install is a successful outer write: a map revoked by a
	/// removal that found no room is left out of its copy, while the other
	/// maps keep serving, and the revoked map's charge is refunded once the
	/// last reader of an older generation drops it.
	#[test]
	fn an_unrelated_install_removes_a_map_revoked_under_exhaustion() {
		let budget = TierMemoryBudget::new(1 << 20);
		let stores = RedirectStores::default();
		installed(&stores, &budget, 1, 2);
		installed(&stores, &budget, 3, 4);
		let redirect_before = budget.used_by(TierBudgetKind::Redirect);
		let old_reader = stores.source_map_for_test(SlabId::new(1)).unwrap();
		budget.set_limit_for_test(budget.used());
		stores.remove_touching(&budget, SlabId::new(2));
		assert_eq!(stores.len_for_test(), 2, "premise: removal found no room");
		budget.set_limit_for_test(1 << 20);
		installed(&stores, &budget, 5, 6);
		assert!(
			stores.source_map_for_test(SlabId::new(1)).is_none(),
			"the revoked map is not carried into the new generation"
		);
		assert_eq!(stores.len_for_test(), 2);
		assert_eq!(stores.resolve(&location(3, 0)), Some(location(4, 0)));
		assert_eq!(stores.resolve(&location(5, 0)), Some(location(6, 0)));
		assert_eq!(stores.resolve(&location(1, 0)), None);
		let with_reader = budget.used_by(TierBudgetKind::Redirect);
		drop(old_reader);
		let single = with_reader - budget.used_by(TierBudgetKind::Redirect);
		assert!(single > 0, "the last reader's drop refunds the revoked map");
		assert_eq!(
			budget.used_by(TierBudgetKind::Redirect),
			redirect_before,
			"two live maps' worth remains"
		);
	}

	/// Writers serialize on the writer mutex and each copies the current
	/// generation, so a concurrent install never resurrects a removed map.
	#[test]
	fn concurrent_writers_never_resurrect_a_removed_map() {
		for _ in 0..200 {
			let budget = TierMemoryBudget::new(1 << 20);
			let stores = Arc::new(RedirectStores::default());
			installed(&stores, &budget, 1, 2);
			let install = stores.reserve_install(&budget, map(&budget, 3, 4)).unwrap();
			let barrier = Arc::new(std::sync::Barrier::new(2));
			let remover = {
				let (stores, budget, barrier) = (stores.clone(), budget.clone(), barrier.clone());
				std::thread::spawn(move || {
					barrier.wait();
					stores.remove_touching(&budget, SlabId::new(1));
				})
			};
			let installer = {
				let (stores, barrier) = (stores.clone(), barrier.clone());
				std::thread::spawn(move || {
					barrier.wait();
					assert!(stores.install(install));
				})
			};
			remover.join().unwrap();
			installer.join().unwrap();
			assert!(stores.source_map_for_test(SlabId::new(1)).is_none());
			assert!(stores.source_map_for_test(SlabId::new(3)).is_some());
			assert_eq!(stores.len_for_test(), 1);
		}
	}

	/// A removed map a reader still holds stays charged until that reader
	/// drops it.
	#[test]
	fn a_removed_map_stays_charged_while_a_reader_holds_it() {
		let budget = TierMemoryBudget::new(1 << 20);
		let stores = RedirectStores::default();
		installed(&stores, &budget, 1, 2);
		let held = stores.source_map_for_test(SlabId::new(1)).unwrap();
		stores.remove_touching(&budget, SlabId::new(1));
		assert!(budget.used_by(TierBudgetKind::Redirect) > 0);
		drop(held);
		assert_eq!(budget.used_by(TierBudgetKind::Redirect), 0);
	}
}
