//! Committed tier pointers this node has not published, and why (ADR 0692
//! §7, ADR 0696 fast follow).
//!
//! A deferral records the outcome of the apply that could not publish: a
//! fence with its typed cause, a typed structural refusal, or an untyped
//! failure. Only a fence has a cause, and only a fence whose cause can clear
//! (the index gate, the node budget, or an LSM scope that has resolved) and that
//! came from a real `Some(index)` log callback is replayed
//! (`replay_tier_deferral`): the
//! replay runs that callback again through the function it used.
use std::io::{Error, ErrorKind};

use super::tiered_io::{needs_tier_preparation, tier_delivery_moved};

use super::*;

/// Why a committed apply was fenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FenceCause {
	/// The persistent index gate was not acquired within its bound.
	IndexGate,
	/// The node budget could not hold the new history.
	NoBudget,
	/// The node budget could not hold the publication's scratch.
	NoScratch,
	/// The tier object could not be fetched from L2.
	L2Unavailable,
	/// A control snapshot install was in flight.
	SnapshotInstall,
	/// An LSM intent named the slab, or its keys were unknown.
	LsmScope,
	/// Replay or snapshot staging registered the pointer, or a metadata
	/// observation fenced it, for verification to publish.
	Staged,
	/// ADR 0704: the committed state proved the publication neither current nor
	/// superseded. A replay re-runs it once it is current.
	CommittedStanding,
	/// ADR 0704: a certified generation or retirement whose local evidence (its
	/// certificate, source, drop or postimage) does not prove it. Not replayable:
	/// it installs and retires nothing until newer committed state resolves it.
	CertifiedEvidence,
	/// Another fence of the publication path.
	Unclassified,
}

impl FenceCause {
	/// A cause that clears without any newer committed state, so replaying
	/// the callback can publish.
	pub(super) const fn replayable(self) -> bool {
		matches!(
			self,
			Self::IndexGate
				| Self::NoBudget
				| Self::NoScratch
				| Self::L2Unavailable
				| Self::LsmScope
				| Self::CommittedStanding
		)
	}

	/// A cause a budget release can clear.
	pub(super) const fn waits_for_budget(self) -> bool {
		matches!(self, Self::NoBudget | Self::NoScratch)
	}
}

/// A `Fenced` refusal that also carries its cause. `stale_location` reads it
/// as the plain `StaleLocation` every reader already handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CausedFence {
	slab_id: SlabId,
	cause: FenceCause,
}

impl CausedFence {
	pub(super) fn stale(self) -> StaleLocation {
		StaleLocation {
			slab_id: self.slab_id,
			reason: StaleLocationReason::Fenced,
		}
	}
}

impl std::fmt::Display for CausedFence {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "{} ({:?})", self.stale(), self.cause)
	}
}

impl std::error::Error for CausedFence {}

/// An L2 fetch that failed (`tiered_object_from_l2`), keeping its kind: a
/// committed publication that hits it fences and is replayed later, while
/// every other reader sees the fetch error it always saw.
#[derive(Debug)]
pub(super) struct L2Unavailable(Error);

impl std::fmt::Display for L2Unavailable {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "L2 unavailable: {}", self.0)
	}
}

impl std::error::Error for L2Unavailable {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.0)
	}
}

pub(super) fn l2_unavailable(error: Error) -> Error {
	Error::new(error.kind(), L2Unavailable(error))
}

pub(super) fn is_l2_unavailable(error: &Error) -> bool {
	error
		.get_ref()
		.is_some_and(|inner| inner.is::<L2Unavailable>())
}

/// A `Fenced` refusal of `slab_id` with its cause.
pub(super) fn fenced_error(kind: ErrorKind, slab_id: SlabId, cause: FenceCause) -> Error {
	Error::new(kind, CausedFence { slab_id, cause })
}

/// The cause of the fence `error` carries, if it carries one.
pub(super) fn fence_cause(error: &Error) -> Option<FenceCause> {
	error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<CausedFence>())
		.map(|fence| fence.cause)
}

/// What the apply that deferred a pointer ended with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DeferralOutcome {
	Fenced(FenceCause),
	/// A typed structural refusal (never `Fenced`).
	Refused(StaleLocationReason),
	/// An untyped failure.
	Failed,
}

impl DeferralOutcome {
	/// The outcome `error` reports. A fence without a recorded cause is
	/// `Unclassified`.
	pub(super) fn of(error: &Error) -> Self {
		match stale_location(error).map(|stale| stale.reason) {
			Some(StaleLocationReason::Fenced) => {
				Self::Fenced(fence_cause(error).unwrap_or(FenceCause::Unclassified))
			}
			Some(reason) => Self::Refused(reason),
			None => Self::Failed,
		}
	}

	/// The typed refusal reason, as the deferral's readers compare it.
	#[cfg(test)]
	pub(super) fn reason(self) -> Option<StaleLocationReason> {
		match self {
			Self::Fenced(_) => Some(StaleLocationReason::Fenced),
			Self::Refused(reason) => Some(reason),
			Self::Failed => None,
		}
	}
}

/// A committed tier pointer this node has not published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TierDeferral {
	pub(super) pointer: TieredSlabPointer,
	pub(super) outcome: DeferralOutcome,
	/// The index of the `Some(index)` TierSlab callback that applied
	/// `pointer`. Metadata and comparison refusals never invent one.
	pub(super) provenance: Option<u64>,
}

impl TierDeferral {
	pub(super) fn fenced(
		pointer: TieredSlabPointer,
		cause: FenceCause,
		provenance: Option<u64>,
	) -> Self {
		Self {
			pointer,
			outcome: DeferralOutcome::Fenced(cause),
			provenance,
		}
	}

	pub(super) fn is_fenced(&self) -> bool {
		matches!(self.outcome, DeferralOutcome::Fenced(_))
	}

	/// A fence of exactly `pointer`.
	pub(super) fn fences(&self, pointer: &TieredSlabPointer) -> bool {
		self.is_fenced() && self.pointer == *pointer
	}

	/// The callback index a replay re-runs, when this deferral is one a
	/// replay may resolve.
	pub(super) fn replayable_index(&self) -> Option<u64> {
		match self.outcome {
			DeferralOutcome::Fenced(cause) if cause.replayable() => self.provenance,
			_ => None,
		}
	}

	/// The fields of the tuple this deferral replaced, for assertions.
	#[cfg(test)]
	pub(super) fn parts(&self) -> (TieredSlabPointer, Option<StaleLocationReason>, Option<u64>) {
		(self.pointer.clone(), self.outcome.reason(), self.provenance)
	}
}

/// What one replay attempt did (`replay_tier_deferral_under_publication_lock`).
#[derive(Debug)]
pub(super) enum TierReplay {
	/// No replayable deferral: nothing to do here.
	NotReplayable,
	/// Revalidation found newer or unknown state; the deferral is unchanged.
	Unchanged,
	/// The committed pointer names a slab this process retired, with no
	/// backing or rows to serve it: the deferral stays, fenced (an
	/// availability limitation registered with ADR 0693's node-local
	/// slab-identity limits).
	KeptFenced,
	/// An installed snapshot at or above the callback's index owns the slab,
	/// and its reconciliation is complete: exactly this deferral was cleared.
	Superseded,
	/// The callback ran again; its result (a failure re-defers with its cause).
	Replayed(std::io::Result<()>),
}

impl TierReplay {
	pub(super) fn resolved(&self) -> bool {
		matches!(self, Self::Superseded | Self::Replayed(Ok(())))
	}
}

impl SlabManager {
	/// Record `deferral` for `slab_id`. A replayable one schedules the
	/// authority worker once it is visible, so a committed fence never waits
	/// on an unrelated authority change or a sweeper pass to be replayed.
	pub(super) fn record_tier_deferral(&self, slab_id: SlabId, deferral: TierDeferral) {
		let replayable = deferral.replayable_index().is_some();
		self.unapplied_tier_pointers.insert(slab_id, deferral);
		if replayable {
			self.tier_authority.start();
			self.tier_authority.notify_changed();
		}
	}

	/// ADR 0704: record `deferral` only where the slab has none. An unknown standing never
	/// replaces another state's deferral; any deferral already fences the store's reads.
	pub(super) fn record_tier_deferral_if_vacant(&self, slab_id: SlabId, deferral: TierDeferral) {
		let replayable = deferral.replayable_index().is_some();
		let inserted = match self.unapplied_tier_pointers.entry(slab_id) {
			dashmap::mapref::entry::Entry::Vacant(entry) => {
				entry.insert(deferral);
				true
			}
			dashmap::mapref::entry::Entry::Occupied(_) => false,
		};
		if inserted && replayable {
			self.tier_authority.start();
			self.tier_authority.notify_changed();
		}
	}

	pub(crate) fn bind_canonical_tier_source(&self, source: crate::consensus::TierSnapshotSource) {
		let _ = self.canonical_tier_source.set(source);
	}

	/// The committed pointer for `slab_id`, read without waiting: `None`
	/// when no source is bound or the state machine is busy. ADR 0704: production
	/// reads it from a held lease (`lease_tier_pointer`).
	#[cfg(test)]
	pub(super) fn try_canonical_tier_pointer(
		&self,
		slab_id: SlabId,
	) -> Option<Option<TieredSlabPointer>> {
		// ADR 0697: this store's own pointer, classified against its node's
		// registry from the same state; a store that is not current reads none.
		let owner = self.owner_ref().ok()?;
		let (tier, registry) = self
			.canonical_tier_source
			.get()?
			.try_tiered_slab(owner.slab(slab_id))
			.ok()?;
		if !matches!(
			self.classify_own_node(&registry, super::slab_ids::OwnerEvidence::Current),
			super::slab_ids::Ownership::Own(_)
		) {
			return None;
		}
		Some(tier.as_ref().map(super::tier_tokens::tiered_slab_pointer))
	}

	/// This store's own committed pointer for `slab_id` in the state `lease` holds, classified
	/// as `try_canonical_tier_pointer` classifies it; `None` when the store is not current.
	pub(super) fn lease_tier_pointer(
		&self,
		lease: &crate::consensus::TierSnapshotLease,
		slab_id: SlabId,
	) -> Option<Option<TieredSlabPointer>> {
		let owner = self.owner_ref().ok()?;
		let registry = lease
			.incarnations()
			.get(&owner.node())
			.cloned()
			.unwrap_or_default();
		if !matches!(
			self.classify_own_node(&registry, super::slab_ids::OwnerEvidence::Current),
			super::slab_ids::Ownership::Own(_)
		) {
			return None;
		}
		Some(
			lease
				.committed_tier(owner.slab(slab_id))
				.as_ref()
				.map(super::tier_tokens::tiered_slab_pointer),
		)
	}

	/// Whether the committed pointer for `pointer`'s slab is `pointer`, in the state the
	/// caller's `lease` holds (ADR 0704: never a second read). With no lease (no bound source:
	/// a manager outside a state machine) the caller's own revalidation is the only check.
	pub(super) fn canonical_tier_pointer_is(
		&self,
		lease: Option<&crate::consensus::TierSnapshotLease>,
		pointer: &TieredSlabPointer,
	) -> bool {
		match lease {
			None => true,
			Some(lease) => self
				.lease_tier_pointer(lease, pointer.slab_id)
				.is_some_and(|committed| committed.as_ref() == Some(pointer)),
		}
	}

	/// Replay the `Some(index)` callback a replayable fence deferred, through
	/// the function the callback used, holding the guards it held. The caller
	/// holds `slab_id`'s tier publication lock and passes the committed
	/// pointer it read under that lock (`None` when absent or unknown).
	///
	/// Decision (ADR 0696 r3 E2):
	/// - an installed snapshot at or above `index` owns the slab: once its
	///   reconciliation is complete, clear exactly this deferral;
	/// - the committed pointer is not exactly the deferred one, or the epoch
	///   moved: change nothing; the newer state's own callback resolves it;
	/// - the slab is retired in this process: keep the fence;
	/// - otherwise re-run the callback, which publishes and clears the
	///   deferral, or re-defers with its new cause.
	///
	/// ADR 0704: `lease` is the caller's, taken before the slab lock, and `prepared` is what
	/// it prepared, with no authority held, for the pointer it names. A preparation that
	/// failed for exactly the deferred pointer ends here as the callback would have ended.
	pub(super) async fn replay_tier_deferral_under_publication_lock(
		&self,
		slab_id: SlabId,
		committed: Option<&TieredSlabPointer>,
		epoch: u64,
		lease: Option<&crate::consensus::TierSnapshotLease>,
		prepared: Option<(
			TieredSlabPointer,
			std::io::Result<super::tiered_io::PreparedTierDelivery>,
		)>,
	) -> TierReplay {
		let Some(deferral) = self
			.unapplied_tier_pointers
			.get(&slab_id)
			.map(|entry| entry.value().clone())
		else {
			return TierReplay::NotReplayable;
		};
		let Some(index) = deferral.replayable_index() else {
			return TierReplay::NotReplayable;
		};
		if self.observer_is_superseded(Some(index)) {
			if self.tier_authority.snapshot_reconciliation_pending() {
				return TierReplay::Unchanged;
			}
			self.unapplied_tier_pointers
				.remove_if(&slab_id, |_, current| *current == deferral);
			self.tier_authority.notify_changed();
			return TierReplay::Superseded;
		}
		if committed != Some(&deferral.pointer) || !self.snapshot_epoch_is_current(epoch) {
			return TierReplay::Unchanged;
		}
		if self.retired_tier_slabs.contains(&slab_id) {
			return TierReplay::KeptFenced;
		}
		let prepared = match prepared {
			Some((pointer, prepared)) if pointer == deferral.pointer => match prepared {
				Ok(prepared) => Some(prepared),
				Err(error) => {
					self.defer_unprepared_tier_delivery(&deferral.pointer, &error, index)
						.await;
					return TierReplay::Replayed(Err(error));
				}
			},
			_ => None,
		};
		let result = self
			.apply_tier_slab_under_publication_lock_as(
				deferral.pointer,
				Some(index),
				epoch,
				true,
				lease,
				prepared,
			)
			.await;
		if result.is_ok() {
			self.tier_authority.notify_changed();
		}
		TierReplay::Replayed(result)
	}
}

/// When the authority worker may next replay one deferral.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReplaySchedule {
	not_before: tokio::time::Instant,
	/// The budget's release epoch when the last attempt began. An attempt's
	/// own reservations never advance it (`within_replay_attempt`), so a
	/// later epoch is a release during or after that attempt, which may have
	/// made room for it.
	release_epoch: u64,
	/// The deferral left by the last attempt is fenced on the budget.
	budget: bool,
	attempts: u32,
}

/// One worker pass over the replayable deferrals.
#[derive(Debug, Default)]
pub(super) struct ReplayRound {
	pub(super) progressed: bool,
	/// Replays attempted in this round.
	pub(super) attempted: u32,
	/// The earliest scheduled retry of a deferral still waiting.
	pub(super) next: Option<tokio::time::Instant>,
	/// The lowest release epoch a budget-fenced deferral last observed: any
	/// later release may clear it, so the worker waits for releases past it.
	pub(super) budget_watermark: Option<u64>,
}

/// Test seams for the worker's replay.
#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct ReplayProbe {
	/// Attempts started.
	pub(super) attempts: std::sync::atomic::AtomicU64,
	/// Failed attempts whose schedule is recorded.
	pub(super) failures: std::sync::atomic::AtomicU64,
	pub(super) backoff: std::sync::Mutex<Option<std::time::Duration>>,
	/// Drop this reservation inside the next attempt for its slab: capacity
	/// held before the attempt and freed during it (as a consumed token is).
	pub(super) free_during_attempt:
		std::sync::Mutex<Option<(SlabId, super::tier_budget::BudgetReservation)>>,
	/// Hold the next worker replay attempt before it takes the canonical lease
	/// (ADR 0704: it holds no lease, slab lock or object fetch while held).
	pub(super) before_replay_apply:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Hold the worker after an attempt's outcome, before its schedule.
	pub(super) after_outcome:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
	/// Hold the worker after a round that attempted nothing, just before it
	/// registers its waiters: the last point a release can be missed.
	pub(super) before_wait:
		std::sync::Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

fn replay_backoff(attempts: u32) -> std::time::Duration {
	std::time::Duration::from_millis(200)
		.saturating_mul(1 << attempts.min(8))
		.min(std::time::Duration::from_secs(30))
}

fn fenced_on_budget(deferral: Option<&TierDeferral>) -> bool {
	deferral.is_some_and(
		|deferral| matches!(deferral.outcome, DeferralOutcome::Fenced(cause) if cause.waits_for_budget()),
	)
}

impl SlabManager {
	/// Replay every replayable deferral whose schedule allows it, each under
	/// its slab's tier publication lock, revalidated against the committed
	/// state read without waiting there. Without a bound committed source
	/// nothing is replayed: the sweeper's authoritative pass resolves those.
	pub(super) async fn replay_tier_deferrals(
		&self,
		schedule: &mut std::collections::HashMap<SlabId, ReplaySchedule>,
	) -> ReplayRound {
		let mut round = ReplayRound::default();
		let candidates = self
			.unapplied_tier_pointers
			.iter()
			.filter(|entry| entry.value().replayable_index().is_some())
			.map(|entry| *entry.key())
			.collect::<Vec<_>>();
		schedule.retain(|id, _| candidates.contains(id));
		if self.canonical_tier_source.get().is_none() {
			return round;
		}
		for slab_id in candidates {
			let now = tokio::time::Instant::now();
			if let Some(entry) = schedule.get(&slab_id) {
				let released =
					entry.budget && self.tier_budget.release_epoch() > entry.release_epoch;
				if now < entry.not_before && !released {
					continue;
				}
			}
			round.attempted += 1;
			#[cfg(test)]
			self.replay_probe
				.attempts
				.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
			let watermark = self.tier_budget.release_epoch();
			let (before, outcome) = super::tier_budget::within_replay_attempt(async {
				#[cfg(test)]
				{
					let mut slot = self.replay_probe.free_during_attempt.lock().unwrap();
					if slot.as_ref().is_some_and(|(slab, _)| *slab == slab_id) {
						drop(slot.take());
					}
				}
				#[cfg(test)]
				{
					let hold = self.replay_probe.before_replay_apply.lock().unwrap().take();
					if let Some((entered, release)) = hold {
						entered.add_permits(1);
						if let Ok(permit) = release.acquire().await {
							permit.forget();
						}
					}
				}
				// ADR 0704: the lease, then the slab lock, and the replay judged in the lease's
				// state. Object storage only on demand, with both released, then once more.
				let mut prepared = None;
				let mut first_before = None;
				for attempt in 0..2 {
					let (before, outcome) = {
						let lease = match self.canonical_tier_lease().await {
							Ok(Some(lease)) => lease,
							Ok(None) | Err(_) => {
								let current = self
									.unapplied_tier_pointers
									.get(&slab_id)
									.map(|entry| entry.value().clone());
								return (first_before.unwrap_or(current), TierReplay::Unchanged);
							}
						};
						let lock = self.tier_publication_lock(slab_id);
						let _tier_guard = lock.lock().await;
						let before = self
							.unapplied_tier_pointers
							.get(&slab_id)
							.map(|entry| entry.value().clone());
						let epoch = self.capture_snapshot_epoch();
						let outcome = match self.lease_tier_pointer(&lease, slab_id) {
							Some(committed) => {
								self.replay_tier_deferral_under_publication_lock(
									slab_id,
									committed.as_ref(),
									epoch,
									Some(&lease),
									prepared.take(),
								)
								.await
							}
							None => TierReplay::Unchanged,
						};
						(before, outcome)
					};
					let first = first_before.get_or_insert(before.clone()).clone();
					match outcome {
						TierReplay::Replayed(Err(error)) if needs_tier_preparation(&error) => {
							let Some(deferral) = before.filter(|_| attempt == 0) else {
								return (
									first,
									TierReplay::Replayed(Err(tier_delivery_moved(slab_id))),
								);
							};
							let ready =
								self.prepare_tier_delivery_observed(&deferral.pointer).await;
							prepared = Some((deferral.pointer, ready));
						}
						outcome => return (first, outcome),
					}
				}
				unreachable!("the second attempt always returns")
			})
			.await;
			#[cfg(test)]
			{
				let hold = self.replay_probe.after_outcome.lock().unwrap().clone();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					if let Ok(permit) = release.acquire().await {
						permit.forget();
					}
				}
			}
			// Progress is the exact deferral gone or replaced, never a reported
			// success alone: an `Ok` that leaves it in place is retried on the
			// backoff, so the worker cannot spin.
			let current = self
				.unapplied_tier_pointers
				.get(&slab_id)
				.map(|entry| entry.value().clone());
			if outcome.resolved() && current != before {
				schedule.remove(&slab_id);
				round.progressed = true;
				continue;
			}
			if let TierReplay::Replayed(Err(error)) = &outcome {
				tracing::debug!(slab_id = slab_id.get(), %error, "tier deferral replay re-fenced");
			}
			let attempts = schedule.get(&slab_id).map_or(0, |entry| entry.attempts) + 1;
			let backoff = replay_backoff(attempts);
			#[cfg(test)]
			let backoff = self.replay_probe.backoff.lock().unwrap().unwrap_or(backoff);
			schedule.insert(
				slab_id,
				ReplaySchedule {
					not_before: tokio::time::Instant::now() + backoff,
					release_epoch: watermark,
					// What the deferral is fenced on now, after this attempt.
					budget: fenced_on_budget(current.as_ref()),
					attempts,
				},
			);
			#[cfg(test)]
			self.replay_probe
				.failures
				.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
		}
		for entry in schedule.values() {
			round.next = Some(
				round
					.next
					.map_or(entry.not_before, |next| next.min(entry.not_before)),
			);
			if entry.budget {
				round.budget_watermark = Some(
					round
						.budget_watermark
						.map_or(entry.release_epoch, |mark| mark.min(entry.release_epoch)),
				);
			}
		}
		round
	}
}
