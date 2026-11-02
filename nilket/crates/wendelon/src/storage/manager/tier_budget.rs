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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
///
/// Every admission, overlap claim, retirement and refund is one critical
/// section over [`BudgetState`], so each decision reads one consistent state
/// and no interleaving can admit against a total that has since changed.
#[derive(Debug)]
pub(super) struct TierMemoryBudget {
	limit: AtomicU64,
	state: Mutex<BudgetState>,
	/// Advances on every refund, so a waiter fenced on budget can tell that
	/// memory was released since it last failed.
	release_epoch: AtomicU64,
	released: tokio::sync::Notify,
	/// Test seam: hold the next admission `pauses` times inside its critical
	/// section, after it read the state and before it commits.
	#[cfg(test)]
	admission_hold: Mutex<Option<AdmissionHold>>,
}

#[cfg(test)]
#[derive(Debug)]
struct AdmissionHold {
	entered: std::sync::mpsc::SyncSender<()>,
	release: std::sync::mpsc::Receiver<()>,
	pauses: usize,
}

#[derive(Debug, Default)]
struct BudgetState {
	used: u64,
	by_kind: [u64; TierBudgetKind::ALL.len()],
	peak: u64,
	/// Bytes counted twice: what live reservations claimed from the history
	/// credit of tokens still held. What is truly held is `used - backed`.
	backed: u64,
	/// One entry per token registry slot (`install_backings`).
	backings: Vec<BackingEntry>,
}

/// A registry slot's lendable history credit. `attempt` 0 is empty.
#[derive(Clone, Copy, Debug, Default)]
struct BackingEntry {
	attempt: u64,
	credit: u64,
	claimed: u64,
}

impl BudgetState {
	#[cfg(test)]
	fn held(&self) -> u64 {
		self.used - self.backed
	}

	fn backing(&mut self, backing: TokenBacking) -> Option<&mut BackingEntry> {
		self.backings
			.get_mut(backing.index)
			.filter(|entry| entry.attempt == backing.attempt)
	}

	/// Return `give` bytes of `backing`'s claim, if it is still live.
	fn unclaim(&mut self, backing: TokenBacking, give: u64) {
		if let Some(entry) = self.backing(backing) {
			entry.claimed -= give;
			self.backed -= give;
		}
	}

	fn refund(&mut self, kind: TierBudgetKind, bytes: u64) {
		self.by_kind[kind.slot()] -= bytes;
		self.used -= bytes;
	}
}

impl TierMemoryBudget {
	pub(super) fn new(limit: u64) -> Arc<Self> {
		Arc::new(Self {
			limit: AtomicU64::new(limit),
			state: Mutex::default(),
			release_epoch: AtomicU64::new(0),
			released: tokio::sync::Notify::new(),
			#[cfg(test)]
			admission_hold: Mutex::default(),
		})
	}

	/// Test seam: hold the next admission (see `admission_hold`).
	#[cfg(test)]
	pub(super) fn hold_next_admission_for_test(
		&self,
		entered: std::sync::mpsc::SyncSender<()>,
		release: std::sync::mpsc::Receiver<()>,
		pauses: usize,
	) {
		*self.admission_hold.lock().unwrap() = Some(AdmissionHold {
			entered,
			release,
			pauses,
		});
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, BudgetState> {
		self.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	pub(super) fn limit(&self) -> u64 {
		self.limit.load(Ordering::Acquire)
	}

	/// Test seam: change the limit (controls that run a manager near or past
	/// its budget).
	#[cfg(test)]
	pub(super) fn set_limit_for_test(&self, limit: u64) {
		self.limit.store(limit, Ordering::Release);
	}

	#[cfg(test)]
	pub(super) fn used(&self) -> u64 {
		self.lock().used
	}

	#[cfg(test)]
	pub(super) fn used_by(&self, kind: TierBudgetKind) -> u64 {
		self.lock().by_kind[kind.slot()]
	}

	/// The most ever truly held at once since the last `reset_peak_for_test`.
	#[cfg(test)]
	pub(super) fn peak(&self) -> u64 {
		self.lock().peak
	}

	/// Test seam: restart the peak from what is truly held now.
	#[cfg(test)]
	pub(super) fn reset_peak_for_test(&self) {
		let mut state = self.lock();
		state.peak = state.held();
	}

	#[cfg(test)]
	pub(super) fn release_epoch(&self) -> u64 {
		self.release_epoch.load(Ordering::Acquire)
	}

	/// Test seam: the history credit `backing` lends, if still live.
	#[cfg(test)]
	pub(super) fn credit_for_test(&self, backing: TokenBacking) -> u64 {
		self.lock().backing(backing).map_or(0, |entry| entry.credit)
	}

	/// Test seam: the overlap live reservations hold against `backing`.
	#[cfg(test)]
	pub(super) fn claimed_for_test(&self, backing: TokenBacking) -> u64 {
		self.lock()
			.backing(backing)
			.map_or(0, |entry| entry.claimed)
	}

	/// The bytes of a backing table for `slots` registry slots, charged with
	/// the registry's own table.
	pub(super) fn backing_table_bytes(slots: usize) -> Option<u64> {
		vec_bytes::<BackingEntry>(slots)
	}

	/// Allocate the fixed backing table, once, after the registry reserved
	/// `backing_table_bytes(slots)` for it.
	pub(super) fn install_backings(&self, slots: usize) -> std::io::Result<()> {
		let mut state = self.lock();
		if !state.backings.is_empty() {
			return Err(Error::other("tier budget backing table installed twice"));
		}
		state
			.backings
			.try_reserve_exact(slots)
			.map_err(|_| Error::new(ErrorKind::OutOfMemory, "tier budget backing table"))?;
		state.backings.resize(slots, BackingEntry::default());
		Ok(())
	}

	/// Open registry slot `index`'s backing for `attempt`, lending `credit`
	/// bytes of its token's history reservation.
	pub(super) fn open_backing(&self, index: usize, attempt: u64, credit: u64) -> TokenBacking {
		let mut state = self.lock();
		if let Some(entry) = state.backings.get_mut(index) {
			*entry = BackingEntry {
				attempt,
				credit,
				claimed: 0,
			};
		}
		TokenBacking { index, attempt }
	}

	/// The token is being released: end every overlap claimed from it. Its
	/// slot calls this before the token's own charge refunds, so in between
	/// the budget counts more than is truly held, never less.
	pub(super) fn retire_backing(&self, backing: TokenBacking) {
		let mut state = self.lock();
		if let Some(entry) = state.backing(backing) {
			let claimed = entry.claimed;
			*entry = BackingEntry::default();
			state.backed -= claimed;
		}
	}

	/// Reserve exactly `bytes`, or nothing.
	pub(super) fn try_reserve(
		self: &Arc<Self>,
		kind: TierBudgetKind,
		bytes: u64,
	) -> Option<BudgetReservation> {
		self.admit(kind, bytes, None)
	}

	/// Reserve exactly `bytes`, or nothing, counting as already held up to the
	/// part of `backing`'s history credit no other live reservation claimed.
	/// The token stays charged until it is released; the overlap is excluded
	/// from what is held, because only one of the two is ever allocated. The
	/// claim returns when the reservation shrinks or drops, and ends when the
	/// token is released.
	pub(super) fn try_reserve_backed(
		self: &Arc<Self>,
		kind: TierBudgetKind,
		bytes: u64,
		backing: TokenBacking,
	) -> Option<BudgetReservation> {
		self.admit(kind, bytes, Some(backing))
	}

	fn admit(
		self: &Arc<Self>,
		kind: TierBudgetKind,
		bytes: u64,
		backing: Option<TokenBacking>,
	) -> Option<BudgetReservation> {
		let limit = self.limit();
		#[cfg(test)]
		let hold = self.admission_hold.lock().unwrap().take();
		let mut state = self.lock();
		let overlap = backing
			.and_then(|backing| state.backing(backing))
			.map_or(0, |entry| bytes.min(entry.credit - entry.claimed));
		let used = state.used.checked_add(bytes)?;
		let held = used - (state.backed + overlap);
		#[cfg(test)]
		if let Some(hold) = hold {
			for _ in 0..hold.pauses {
				if hold.entered.send(()).is_err() {
					break;
				}
				let _ = hold
					.release
					.recv_timeout(std::time::Duration::from_secs(30));
			}
		}
		if held > limit {
			return None;
		}
		if let Some(entry) = backing.and_then(|backing| state.backing(backing)) {
			entry.claimed += overlap;
		}
		state.used = used;
		state.backed += overlap;
		state.by_kind[kind.slot()] += bytes;
		state.peak = state.peak.max(held);
		drop(state);
		kind.charge(bytes);
		Some(BudgetReservation {
			budget: self.clone(),
			kind,
			bytes,
			backed: backing
				.filter(|_| overlap > 0)
				.map(|backing| (backing, overlap)),
		})
	}

	/// Refund `bytes` of a `kind` reservation, first returning `give` bytes of
	/// its claim on `backing`, in one critical section.
	fn release(&self, kind: TierBudgetKind, bytes: u64, claim: Option<(TokenBacking, u64)>) {
		{
			let mut state = self.lock();
			if let Some((backing, give)) = claim {
				state.unclaim(backing, give);
			}
			state.refund(kind, bytes);
		}
		if bytes == 0 {
			return;
		}
		kind.discharge(bytes);
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
	/// The token overlap this reservation claimed, at most `bytes`.
	backed: Option<(TokenBacking, u64)>,
}

impl BudgetReservation {
	pub(super) fn bytes(&self) -> u64 {
		self.bytes
	}

	/// Refund everything above `actual`, with the overlap above it. Refuses,
	/// changing nothing, when `actual` exceeds the reservation: memory already
	/// allocated past a reservation is never accepted after the fact.
	pub(super) fn shrink_to(&mut self, actual: u64) -> Result<(), BudgetOverrun> {
		if actual > self.bytes {
			return Err(BudgetOverrun {
				reserved: self.bytes,
				actual,
			});
		}
		let claim = self.backed.as_mut().map(|(backing, overlap)| {
			let give = *overlap - actual.min(*overlap);
			*overlap -= give;
			(*backing, give)
		});
		self.budget.release(self.kind, self.bytes - actual, claim);
		self.bytes = actual;
		Ok(())
	}

	/// End the overlap on `backing` (its token is released). See
	/// [`TierMemoryBudget::retire_backing`].
	pub(super) fn retire_backing(&self, backing: TokenBacking) {
		self.budget.retire_backing(backing);
	}
}

impl Drop for BudgetReservation {
	fn drop(&mut self) {
		self.budget.release(self.kind, self.bytes, self.backed);
	}
}

/// The scratch charge a publication plan carries until its last copy drops.
/// Plans compare equal regardless of it.
#[derive(Clone, Debug, Default)]
pub(super) struct PlanScratch(
	#[allow(
		dead_code,
		reason = "held for its Drop: the charge is released with the plan"
	)]
	Option<Arc<BudgetReservation>>,
);

impl PlanScratch {
	pub(super) fn new(charge: BudgetReservation) -> Self {
		Self(Some(Arc::new(charge)))
	}

	/// A keep-alive for this charge to travel with copies of the plan's rows
	/// (an index update moved into its blocking worker).
	pub(super) fn retain(&self) -> Option<crate::storage::index::RetainedCharge> {
		self.0.clone().map(|charge| {
			crate::storage::index::RetainedCharge(charge as Arc<dyn std::any::Any + Send + Sync>)
		})
	}
}

impl PartialEq for PlanScratch {
	fn eq(&self, _: &Self) -> bool {
		true
	}
}

impl Eq for PlanScratch {}

/// A registry slot's lendable history credit, by `(slot, attempt)`: a stale
/// handle never touches a later occupant's credit. It holds no memory; the
/// claims live in the budget's fixed backing table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TokenBacking {
	index: usize,
	attempt: u64,
}

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
	fn per_kind_totals_sum_to_used_through_shrink_and_drop() {
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
		let sum = |budget: &TierMemoryBudget| {
			TierBudgetKind::ALL
				.iter()
				.map(|kind| budget.used_by(*kind))
				.sum::<u64>()
		};
		assert_eq!(budget.used_by(TierBudgetKind::Token), 250);
		assert_eq!(budget.used(), 450);
		assert_eq!(sum(&budget), budget.used());
		assert_eq!(
			budget.peak(),
			500,
			"token 300 + scratch 200 was the high-water mark"
		);
		drop(scratch);
		drop(token);
		assert_eq!(budget.used(), 0);
		assert_eq!(sum(&budget), 0);
		let weak = Arc::downgrade(&budget);
		drop(budget);
		assert!(
			weak.upgrade().is_none(),
			"no reservation keeps the budget alive after its owners drop"
		);
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
		assert_eq!(
			budget.release_epoch(),
			before,
			"a zero refund is no release"
		);
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
		use super::super::{
			HybridTimestamp, PlacementSemantics, RecordLocation, RecordRowIndex, SlabId,
		};
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
		let mut map = try_bounded_map::<u64, u64>(1).unwrap();
		bounded_insert(&mut map, 1, 7, 1).unwrap();
		assert_eq!(
			bounded_insert(&mut map, 1, 7, 2),
			Ok(Some(1)),
			"an existing key never grows"
		);
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
