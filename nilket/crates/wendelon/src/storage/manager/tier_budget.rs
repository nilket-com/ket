//! The node memory budget for tier history and relocation redirects
//! (ADR 0692 §7, delivered by ADR 0696's fast follow).
//!
//! One counter per manager bounds every structure §7 covers: tier layouts,
//! publication scratch, proposer tokens, cross-slab redirect maps and their
//! outer store, the token registry and disposal evidence. A reservation is
//! all-or-nothing and is taken before the structure it pays for is
//! allocated; the structure is then built into the reserved capacity and
//! never grows past it. Dropping a reservation refunds it.
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::SlabId;

/// The §7 default: 256 MiB per manager.
pub(super) const DEFAULT_TIER_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// The most redirect plus superseded entries one slab's history may hold,
/// inclusive (§7 per-lineage cap).
pub(super) const LINEAGE_HISTORY_CAP: usize = 1_048_576;

/// What a reservation pays for. The per-kind totals always sum to `used`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TierBudgetKind {
	Token,
	Scratch,
	Layout,
	Redirect,
	RedirectStore,
	Registry,
	Disposal,
}

impl TierBudgetKind {
	pub(super) const ALL: [Self; 7] = [
		Self::Token,
		Self::Scratch,
		Self::Layout,
		Self::Redirect,
		Self::RedirectStore,
		Self::Registry,
		Self::Disposal,
	];

	const fn slot(self) -> usize {
		self as usize
	}

	/// The process-wide gauge this kind reports to, as per-manager deltas.
	fn gauge(self) -> Option<&'static crate::metrics::Gauge> {
		let metrics = crate::metrics::global();
		match self {
			Self::Layout => Some(&metrics.tier_history_bytes),
			Self::Redirect | Self::RedirectStore => Some(&metrics.relocation_redirect_bytes),
			_ => None,
		}
	}

	fn charge(self, bytes: u64) {
		if let Some(gauge) = self.gauge() {
			gauge.add(bytes);
		}
	}

	fn discharge(self, bytes: u64) {
		if let Some(gauge) = self.gauge() {
			gauge.sub(bytes);
		}
	}
}

/// One manager's §7 memory budget.
#[derive(Debug)]
pub(super) struct TierMemoryBudget {
	limit: u64,
	used: AtomicU64,
	by_kind: [AtomicU64; TierBudgetKind::ALL.len()],
	peak: AtomicU64,
	/// Advances on every refund, so a waiter fenced on budget can tell that
	/// memory was released since it last failed.
	release_epoch: AtomicU64,
	released: tokio::sync::Notify,
}

impl TierMemoryBudget {
	pub(super) fn new(limit: u64) -> Arc<Self> {
		Arc::new(Self {
			limit,
			used: AtomicU64::new(0),
			by_kind: Default::default(),
			peak: AtomicU64::new(0),
			release_epoch: AtomicU64::new(0),
			released: tokio::sync::Notify::new(),
		})
	}

	pub(super) fn limit(&self) -> u64 {
		self.limit
	}

	pub(super) fn used(&self) -> u64 {
		self.used.load(Ordering::Acquire)
	}

	pub(super) fn used_by(&self, kind: TierBudgetKind) -> u64 {
		self.by_kind[kind.slot()].load(Ordering::Acquire)
	}

	/// The most ever outstanding at once since the last `reset_peak`.
	pub(super) fn peak(&self) -> u64 {
		self.peak.load(Ordering::Acquire)
	}

	pub(super) fn reset_peak(&self) {
		self.peak.store(self.used(), Ordering::Release);
	}

	pub(super) fn release_epoch(&self) -> u64 {
		self.release_epoch.load(Ordering::Acquire)
	}

	/// Resolves after the next refund. Enable it before checking whether to
	/// wait, so a refund between the check and the wait is not lost.
	pub(super) fn released(&self) -> tokio::sync::futures::Notified<'_> {
		self.released.notified()
	}

	/// Reserve exactly `bytes`, or nothing.
	pub(super) fn try_reserve(
		self: &Arc<Self>,
		kind: TierBudgetKind,
		bytes: u64,
	) -> Option<BudgetReservation> {
		self.take(bytes)?;
		self.by_kind[kind.slot()].fetch_add(bytes, Ordering::AcqRel);
		kind.charge(bytes);
		Some(BudgetReservation {
			budget: self.clone(),
			kind,
			bytes,
		})
	}

	fn take(&self, bytes: u64) -> Option<()> {
		let mut used = self.used.load(Ordering::Acquire);
		loop {
			let next = used.checked_add(bytes).filter(|next| *next <= self.limit)?;
			match self
				.used
				.compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
			{
				Ok(_) => {
					self.peak.fetch_max(next, Ordering::AcqRel);
					return Some(());
				}
				Err(current) => used = current,
			}
		}
	}

	fn refund(&self, kind: TierBudgetKind, bytes: u64) {
		if bytes == 0 {
			return;
		}
		self.by_kind[kind.slot()].fetch_sub(bytes, Ordering::AcqRel);
		kind.discharge(bytes);
		self.used.fetch_sub(bytes, Ordering::AcqRel);
		self.release_epoch.fetch_add(1, Ordering::AcqRel);
		self.released.notify_waiters();
	}
}

/// Bytes held against a budget until dropped.
#[derive(Debug)]
#[must_use = "dropping a reservation refunds it"]
pub(super) struct BudgetReservation {
	budget: Arc<TierMemoryBudget>,
	kind: TierBudgetKind,
	bytes: u64,
}

impl BudgetReservation {
	pub(super) fn bytes(&self) -> u64 {
		self.bytes
	}

	pub(super) fn kind(&self) -> TierBudgetKind {
		self.kind
	}

	/// Whether this reservation is charged to `budget`.
	pub(super) fn is_from(&self, budget: &Arc<TierMemoryBudget>) -> bool {
		Arc::ptr_eq(&self.budget, budget)
	}

	/// Refund everything above `actual`. Refuses, changing nothing, when
	/// `actual` exceeds the reservation: memory already allocated past a
	/// reservation is never accepted after the fact.
	pub(super) fn shrink_to(&mut self, actual: u64) -> Result<(), BudgetOverrun> {
		if actual > self.bytes {
			return Err(BudgetOverrun {
				reserved: self.bytes,
				actual,
			});
		}
		let surplus = self.bytes - actual;
		self.budget.refund(self.kind, surplus);
		self.bytes = actual;
		Ok(())
	}

	/// Charge the same bytes to `kind` instead, without releasing them.
	pub(super) fn into_kind(mut self, kind: TierBudgetKind) -> Self {
		if kind != self.kind {
			let slots = &self.budget.by_kind;
			slots[self.kind.slot()].fetch_sub(self.bytes, Ordering::AcqRel);
			self.kind.discharge(self.bytes);
			slots[kind.slot()].fetch_add(self.bytes, Ordering::AcqRel);
			kind.charge(self.bytes);
			self.kind = kind;
		}
		self
	}

	/// Fold `other` into this reservation. Both must be charged to the same
	/// budget; `other` takes this reservation's kind.
	pub(super) fn absorb(&mut self, other: BudgetReservation) {
		assert!(
			Arc::ptr_eq(&self.budget, &other.budget),
			"a reservation only absorbs one from its own budget"
		);
		let other = other.into_kind(self.kind);
		self.bytes += other.bytes;
		std::mem::forget(other);
	}
}

impl Drop for BudgetReservation {
	fn drop(&mut self) {
		self.budget.refund(self.kind, self.bytes);
	}
}

/// The scratch charge a publication plan carries until its last copy drops.
/// Plans compare equal regardless of it.
#[derive(Clone, Debug, Default)]
pub(super) struct PlanScratch(Option<Arc<BudgetReservation>>);

impl PlanScratch {
	pub(super) fn new(charge: BudgetReservation) -> Self {
		Self(Some(Arc::new(charge)))
	}
}

impl PartialEq for PlanScratch {
	fn eq(&self, _: &Self) -> bool {
		true
	}
}

impl Eq for PlanScratch {}

/// A structure measured larger than the reservation that paid for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BudgetOverrun {
	pub(super) reserved: u64,
	pub(super) actual: u64,
}

/// Why a tier publication could not obtain the memory its history needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TierHistoryShortfall {
	/// The node budget cannot hold the new layout (or a token for it).
	History,
	/// The node budget cannot hold the publication's build scratch.
	Scratch,
	/// The slab's history would exceed the per-lineage cap.
	LineageCap,
	/// The plan holds more than its preflight counted: it is stale.
	StalePlan,
}

impl TierHistoryShortfall {
	pub(super) const fn label(self) -> &'static str {
		match self {
			Self::History => "node_history_budget",
			Self::Scratch => "no_scratch",
			Self::LineageCap => "lineage_history_cap",
			Self::StalePlan => "stale_plan",
		}
	}
}

/// The typed refusal of a tier publication short of history memory. Budget
/// shortfalls are `OutOfMemory`, which the authority worker retries; a
/// lineage cap or a stale plan is not retried as it stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TierHistoryRefusal {
	pub(super) slab_id: SlabId,
	pub(super) shortfall: TierHistoryShortfall,
}

impl std::fmt::Display for TierHistoryRefusal {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"tier publication for slab {} refused: {}",
			self.slab_id,
			self.shortfall.label()
		)
	}
}

impl std::error::Error for TierHistoryRefusal {}

pub(super) fn tier_history_refusal(slab_id: SlabId, shortfall: TierHistoryShortfall) -> Error {
	let kind = match shortfall {
		TierHistoryShortfall::History | TierHistoryShortfall::Scratch => ErrorKind::OutOfMemory,
		TierHistoryShortfall::LineageCap | TierHistoryShortfall::StalePlan => ErrorKind::Other,
	};
	Error::new(kind, TierHistoryRefusal { slab_id, shortfall })
}

/// The typed history refusal carried by `error`, if any.
pub(super) fn tier_history_refusal_of(error: &Error) -> Option<TierHistoryRefusal> {
	error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<TierHistoryRefusal>())
		.copied()
}

/// Group width of the pinned hashbrown's control bytes: 16 with SSE2, 8
/// otherwise. 16 bounds both.
const MAX_GROUP_WIDTH: u64 = 16;

/// An upper bound on the bytes a hashbrown 0.16 table allocates when
/// reserved for `capacity` entries of `T`, or `None` on overflow. It follows
/// `capacity_to_buckets` and `calculate_layout_for`: small tables (under 15
/// entries) take at most 16 buckets; larger ones `next_pow2(cap * 8 / 7)`;
/// the control bytes start at the next `max(align, 16)` boundary and take
/// one byte per bucket plus a trailing group.
pub(super) fn hash_table_bytes_bound<T>(capacity: usize) -> Option<u64> {
	if capacity == 0 {
		return Some(0);
	}
	let buckets = if capacity < 15 {
		16
	} else {
		(capacity.checked_mul(8)? / 7).checked_next_power_of_two()?
	};
	let buckets = u64::try_from(buckets).ok()?;
	let size = u64::try_from(std::mem::size_of::<T>()).ok()?;
	let align = u64::try_from(std::mem::align_of::<T>())
		.ok()?
		.max(MAX_GROUP_WIDTH);
	let data = size.checked_mul(buckets)?;
	let ctrl_offset = data.checked_add(align - 1)? / align * align;
	ctrl_offset
		.checked_add(buckets)?
		.checked_add(MAX_GROUP_WIDTH)
}

/// The bytes a `Vec<T>` holds for exactly `len` elements, or `None` on
/// overflow.
pub(super) fn vec_bytes<T>(len: usize) -> Option<u64> {
	u64::try_from(std::mem::size_of::<T>())
		.ok()?
		.checked_mul(u64::try_from(len).ok()?)
}

/// Sum byte bounds, failing on any missing term or overflow.
pub(super) fn checked_sum(terms: &[Option<u64>]) -> Option<u64> {
	terms
		.iter()
		.try_fold(0u64, |total, term| total.checked_add((*term)?))
}

/// A fill would exceed the capacity its reservation paid for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BoundExceeded;

/// A `Vec` that holds at most the capacity reserved for it and never grows.
#[derive(Debug)]
pub(super) struct BoundedVec<T> {
	items: Vec<T>,
	bound: usize,
}

impl<T> BoundedVec<T> {
	/// Allocate room for exactly `bound` elements, or fail without
	/// allocating.
	pub(super) fn try_with_bound(bound: usize) -> Result<Self, std::collections::TryReserveError> {
		let mut items = Vec::new();
		items.try_reserve_exact(bound)?;
		Ok(Self { items, bound })
	}

	/// Append, refusing before the push could reallocate.
	pub(super) fn push(&mut self, item: T) -> Result<(), BoundExceeded> {
		if self.items.len() >= self.bound {
			return Err(BoundExceeded);
		}
		self.items.push(item);
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		self.items.len()
	}

	pub(super) fn into_vec(self) -> Vec<T> {
		self.items
	}
}

pub(super) type BudgetedMap<K, V> = hashbrown::HashMap<K, V, std::hash::RandomState>;
pub(super) type BudgetedSet<K> = hashbrown::HashSet<K, std::hash::RandomState>;

/// Make an empty map that can hold `bound` entries without reallocating.
pub(super) fn try_bounded_map<K: Eq + std::hash::Hash, V>(
	bound: usize,
) -> Result<BudgetedMap<K, V>, hashbrown::TryReserveError> {
	let mut map = BudgetedMap::with_hasher(std::hash::RandomState::new());
	map.try_reserve(bound)?;
	Ok(map)
}

/// Make an empty set that can hold `bound` entries without reallocating.
pub(super) fn try_bounded_set<K: Eq + std::hash::Hash>(
	bound: usize,
) -> Result<BudgetedSet<K>, hashbrown::TryReserveError> {
	let mut set = BudgetedSet::with_hasher(std::hash::RandomState::new());
	set.try_reserve(bound)?;
	Ok(set)
}

/// Insert into a map built by `try_bounded_map(bound)`, refusing before an
/// insertion of a new key could reallocate it.
pub(super) fn bounded_insert<K: Eq + std::hash::Hash, V>(
	map: &mut BudgetedMap<K, V>,
	bound: usize,
	key: K,
	value: V,
) -> Result<Option<V>, BoundExceeded> {
	if map.len() >= bound && !map.contains_key(&key) {
		return Err(BoundExceeded);
	}
	Ok(map.insert(key, value))
}

/// Insert into a set built by `try_bounded_set(bound)`, refusing before an
/// insertion of a new key could reallocate it.
pub(super) fn bounded_set_insert<K: Eq + std::hash::Hash>(
	set: &mut BudgetedSet<K>,
	bound: usize,
	key: K,
) -> Result<bool, BoundExceeded> {
	if set.len() >= bound && !set.contains(&key) {
		return Err(BoundExceeded);
	}
	Ok(set.insert(key))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reservations_are_all_or_nothing_and_inclusive_at_the_limit() {
		let budget = TierMemoryBudget::new(100);
		let exact = budget.try_reserve(TierBudgetKind::Layout, 100);
		assert!(exact.is_some(), "a reservation equal to the limit succeeds");
		assert!(
			budget.try_reserve(TierBudgetKind::Token, 1).is_none(),
			"one byte over the limit refuses"
		);
		drop(exact);
		assert_eq!(budget.used(), 0);
		assert!(budget.try_reserve(TierBudgetKind::Layout, 101).is_none());
		assert_eq!(budget.used(), 0, "a refused reservation takes nothing");
	}

	#[test]
	fn per_kind_totals_sum_to_used_through_shrink_transfer_and_absorb() {
		let budget = TierMemoryBudget::new(1_000);
		let mut token = budget.try_reserve(TierBudgetKind::Token, 300).unwrap();
		let scratch = budget.try_reserve(TierBudgetKind::Scratch, 200).unwrap();
		token.shrink_to(250).unwrap();
		assert_eq!(
			token.shrink_to(251),
			Err(BudgetOverrun {
				reserved: 250,
				actual: 251
			})
		);
		let mut layout = token.into_kind(TierBudgetKind::Layout);
		let delta = budget.try_reserve(TierBudgetKind::Token, 50).unwrap();
		layout.absorb(delta);
		let sum = |budget: &TierMemoryBudget| {
			TierBudgetKind::ALL
				.iter()
				.map(|kind| budget.used_by(*kind))
				.sum::<u64>()
		};
		assert_eq!(layout.bytes(), 300);
		assert_eq!(budget.used_by(TierBudgetKind::Layout), 300);
		assert_eq!(budget.used_by(TierBudgetKind::Token), 0);
		assert_eq!(budget.used(), 500);
		assert_eq!(sum(&budget), budget.used());
		assert_eq!(budget.peak(), 500, "token 300 + scratch 200 was the high-water mark");
		drop(scratch);
		drop(layout);
		assert_eq!(budget.used(), 0);
		assert_eq!(sum(&budget), 0);
	}

	#[test]
	fn concurrent_reservations_never_overcommit() {
		let budget = TierMemoryBudget::new(10_000);
		std::thread::scope(|scope| {
			for _ in 0..8 {
				let budget = budget.clone();
				scope.spawn(move || {
					for _ in 0..10_000 {
						if let Some(held) = budget.try_reserve(TierBudgetKind::Scratch, 700) {
							assert!(budget.used() <= budget.limit());
							drop(held);
						}
					}
				});
			}
		});
		assert_eq!(budget.used(), 0);
		assert!(budget.peak() <= budget.limit());
	}

	#[test]
	fn refunds_advance_the_release_epoch() {
		let budget = TierMemoryBudget::new(10);
		let before = budget.release_epoch();
		drop(budget.try_reserve(TierBudgetKind::Token, 0).unwrap());
		assert_eq!(budget.release_epoch(), before, "a zero refund is no release");
		drop(budget.try_reserve(TierBudgetKind::Token, 4).unwrap());
		assert_eq!(budget.release_epoch(), before + 1);
	}

	fn assert_map_bound<K, V>(make: impl Fn(usize) -> (K, V))
	where
		K: Eq + std::hash::Hash,
	{
		let mut sizes = (0..=4096).collect::<Vec<usize>>();
		for shift in 13..=20 {
			let pow = 1usize << shift;
			sizes.extend([pow - 1, pow, pow + 1]);
		}
		for n in sizes {
			let mut map = try_bounded_map::<K, V>(n).unwrap();
			let capacity = map.capacity();
			for index in 0..n {
				let (key, value) = make(index);
				bounded_insert(&mut map, n, key, value).unwrap();
			}
			assert_eq!(map.capacity(), capacity, "n={n}: the map never grew");
			let bound = hash_table_bytes_bound::<(K, V)>(n).unwrap();
			assert!(
				map.allocation_size() as u64 <= bound,
				"n={n}: allocation {} exceeds bound {bound}",
				map.allocation_size()
			);
		}
	}

	#[test]
	fn the_table_bound_covers_every_budgeted_map_shape() {
		use super::super::{HybridTimestamp, PlacementSemantics, RecordLocation, RecordRowIndex, SlabId};
		let location = |index: usize| RecordLocation {
			slab_id: SlabId::new(1),
			offset: index as u64,
			length: 64,
			hlc: HybridTimestamp {
				physical_ms: index as u64,
				logical: 0,
				node_id: 1,
			},
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: PlacementSemantics::RawLocality,
			partition: None,
		};
		assert_map_bound::<RecordLocation, RecordLocation>(|index| {
			(location(index), location(index + 1))
		});
		assert_map_bound::<RecordLocation, ()>(|index| (location(index), ()));
		assert_map_bound::<u64, u32>(|index| (index as u64, index as u32));
	}

	#[test]
	fn bounded_containers_refuse_before_growing() {
		let mut items = BoundedVec::<u64>::try_with_bound(2).unwrap();
		items.push(1).unwrap();
		items.push(2).unwrap();
		assert_eq!(items.push(3), Err(BoundExceeded));
		assert_eq!(items.len(), 2);
		let mut map = try_bounded_map::<u64, u64>(1).unwrap();
		bounded_insert(&mut map, 1, 7, 1).unwrap();
		assert_eq!(bounded_insert(&mut map, 1, 7, 2), Ok(Some(1)), "an existing key never grows");
		assert_eq!(bounded_insert(&mut map, 1, 8, 1), Err(BoundExceeded));
		let mut set = try_bounded_set::<u64>(1).unwrap();
		assert_eq!(bounded_set_insert(&mut set, 1, 3), Ok(true));
		assert_eq!(bounded_set_insert(&mut set, 1, 4), Err(BoundExceeded));
	}

	#[test]
	fn byte_bounds_are_checked() {
		assert_eq!(hash_table_bytes_bound::<u64>(usize::MAX), None);
		assert_eq!(vec_bytes::<u64>(usize::MAX), None);
		assert_eq!(checked_sum(&[Some(u64::MAX), Some(1)]), None);
		assert_eq!(checked_sum(&[Some(1), None]), None);
		assert_eq!(checked_sum(&[Some(1), Some(2)]), Some(3));
	}
}
