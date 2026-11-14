//! ADR 0734: one lease renewal pass has one budget, and a pass that ends by it is continued
//! fairly by the next.
//!
//! The lease manager runs one pass at a time, and a pass asks the control leader it read when
//! it began. Every wait of a pass on another node or on a commit therefore runs under one
//! absolute deadline, the pass's entry plus the lease length: a request that gets no answer
//! ends the pass instead of holding the manager, and the next pass reads leadership again.
//!
//! The deadline is a policy applied when the pass is polled. It cannot interrupt a stalled
//! executor or blocking work; it decides what the pass goes on waiting for and what it will
//! accept, and a result that completes after it is discarded before anything is cached.
//!
//! A request that reached the deadline is ambiguous: it may yet be committed. Nothing new is
//! cached from it, nothing cached earlier is removed, and the next pass asks again from the
//! applied state, where the existing rules decide what that request is.

use super::*;
use tokio::time::Instant;

/// One renewal pass's budget.
pub(crate) struct RenewalPass {
	deadline: Instant,
	budget: Duration,
}

impl RenewalPass {
	/// A pass beginning now, with the lease length `ttl_ms` as its budget.
	pub(crate) fn begin(ttl_ms: u32) -> Self {
		let budget = Duration::from_millis(u64::from(ttl_ms));
		Self {
			deadline: Instant::now() + budget,
			budget,
		}
	}

	pub(crate) fn budget(&self) -> Duration {
		self.budget
	}

	/// Whether the budget is spent: nothing further is attempted or accepted.
	pub(crate) fn expired(&self) -> bool {
		Instant::now() >= self.deadline
	}

	/// `work`'s result if it completed, and was observed, before the deadline; `None` when
	/// the deadline came first. The clock is read after the result is in hand: one that
	/// completed late, on a late poll, is discarded like one that never came.
	///
	/// A spent budget is checked BEFORE `work` is polled: a timeout wrapper polls what it
	/// wraps first, and a poll can submit a request that discarding its result cannot undo.
	pub(crate) async fn bounded<T>(&self, work: impl Future<Output = T>) -> Option<T> {
		if self.expired() {
			return None;
		}
		let result = tokio::time::timeout_at(self.deadline, work).await.ok()?;
		(Instant::now() < self.deadline).then_some(result)
	}
}

/// Where a pass that did not finish its walk stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stopped {
	/// The budget was spent before the target at this position of the walk was attempted.
	Before(usize),
	/// A unit that was attempted was not answered inside the budget. `unit` is the walk
	/// position of its first target, `next` the position after the whole unit (it may equal
	/// the walk's length).
	Unanswered { unit: usize, next: usize },
}

/// The order in which a pass attempts `targets` (sorted by range start): from the first
/// target whose start is at or after `cursor`, wrapping to the front. Each target appears
/// exactly once. A cursor past every start, or none, begins at the front.
pub(crate) fn walk_from<T: Copy>(
	targets: &[T],
	start_of: impl Fn(&T) -> u64,
	cursor: Option<u64>,
) -> Vec<T> {
	let at = cursor
		.and_then(|cursor| targets.iter().position(|target| start_of(target) >= cursor))
		.unwrap_or(0);
	targets[at..]
		.iter()
		.chain(&targets[..at])
		.copied()
		.collect()
}

/// The cursor a pass leaves when it stopped at `stopped` in a walk whose targets start at
/// `starts`. An empty walk leaves none.
/// - A target not attempted: that target.
/// - A unit that went unanswered AFTER earlier units of this pass had used part of the
///   budget: that same unit. It has not yet had a whole budget, and the next pass gives it
///   one by beginning with it.
/// - A unit that went unanswered as the pass's FIRST: the target after it, wrapping to the
///   walk's first. It had a whole budget and did not answer; the others are not made to
///   wait behind it again. It is reached later in that walk, and if it then goes unanswered
///   as a later unit it is first once more in the pass after.
///
/// So no unit is denied a whole budget for ever by the units before it, and no unit that
/// never answers can keep the others from being attempted.
pub(crate) fn continuation(starts: &[u64], stopped: Stopped) -> Option<u64> {
	if starts.is_empty() {
		return None;
	}
	let at = match stopped {
		Stopped::Before(at) => at,
		Stopped::Unanswered { unit, .. } if unit > 0 => unit,
		Stopped::Unanswered { next, .. } => next,
	};
	Some(starts[at % starts.len()])
}

/// ADR 0734: how one range's renewal ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RangeRenewal {
	/// Granted, validated and cached, inside the pass's budget.
	Granted,
	/// Answered in time, and not a grant this node caches: denied, refused or invalid.
	NotGranted,
	/// No answer inside the pass's budget, from what is named. Ambiguous: it may yet commit.
	Unanswered(String),
}

impl WendelonDataServer {
	/// Today's pass over this node's primary ranges: every range once, from where the last
	/// pass left off, each range's wait under `pass`.
	pub(super) async fn renew_ranges(
		&self,
		ranges: Vec<(u64, u64)>,
		epoch: u64,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
		current_leader: Option<u32>,
		pass: &RenewalPass,
	) -> usize {
		// The walk: every range once, from where the last pass left off.
		let mut ranges = ranges;
		ranges.sort_unstable();
		let walk = walk_from(&ranges, |range| range.0, self.lease_renewal_cursor());
		let starts: Vec<u64> = walk.iter().map(|range| range.0).collect();
		let mut granted = 0usize;
		for (at, (range_start, range_end)) in walk.iter().copied().enumerate() {
			if pass.expired() {
				self.end_renewal_pass_at(&starts, Stopped::Before(at));
				self.renewal_pass_expired(pass, epoch, "before the next range", walk.len() - at);
				return granted;
			}
			match self
				.renew_lease_range(
					range_start,
					range_end,
					epoch,
					clock,
					ttl_ms,
					current_leader,
					pass,
				)
				.await
			{
				RangeRenewal::Granted => granted += 1,
				RangeRenewal::NotGranted => {}
				RangeRenewal::Unanswered(waited_on) => {
					self.end_renewal_pass_at(
						&starts,
						Stopped::Unanswered {
							unit: at,
							next: at + 1,
						},
					);
					self.renewal_pass_expired(pass, epoch, &waited_on, walk.len() - at - 1);
					return granted;
				}
			}
		}
		self.set_lease_renewal_cursor(None);
		granted
	}

	/// ADR 0734: where the next renewal pass begins, if the last ended by its budget.
	pub(super) fn lease_renewal_cursor(&self) -> Option<u64> {
		*self
			.lease_renewal_cursor
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	pub(super) fn set_lease_renewal_cursor(&self, cursor: Option<u64>) {
		*self
			.lease_renewal_cursor
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner) = cursor;
	}

	/// Test seam: the continuation cursor.
	#[cfg(test)]
	pub(crate) fn lease_renewal_cursor_for_test(&self) -> Option<u64> {
		self.lease_renewal_cursor()
	}

	/// Test seam: sets the continuation cursor, as a pass that ended by its budget would.
	#[cfg(test)]
	pub(crate) fn set_lease_renewal_cursor_for_test(&self, cursor: Option<u64>) {
		self.set_lease_renewal_cursor(cursor);
	}

	/// Test seam: the leases this node has cached.
	#[cfg(test)]
	pub(crate) fn cached_leases_for_test(&self) -> Vec<LeaseDescriptor> {
		recover_rwlock_read(&self.leases, "lease_cache").clone()
	}

	/// Records where a pass that ended by its budget is to be continued.
	pub(super) fn end_renewal_pass_at(&self, starts: &[u64], stopped: Stopped) {
		self.set_lease_renewal_cursor(continuation(starts, stopped));
	}

	/// The one report of a pass that reached its budget: what it was waiting on, and how
	/// many targets it leaves for the next pass. Unattempted targets are not reported each.
	pub(super) fn renewal_pass_expired(
		&self,
		pass: &RenewalPass,
		epoch: u64,
		waited_on: &str,
		unattempted: usize,
	) {
		metrics().data_ring_lease_errors_total.inc();
		tracing::warn!(
			epoch,
			waited_on,
			budget_ms = pass.budget().as_millis() as u64,
			unattempted,
			"lease manager: the renewal pass reached its budget; an unanswered request is not a denial, and the next pass asks again"
		);
	}

	/// One range's lease renewal, stamped from the clock when it is requested. Its wait on
	/// the leader, or on its own commit, runs under `pass`; an answer is cached only when it
	/// was observed inside the pass's budget.
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn renew_lease_range(
		&self,
		range_start: u64,
		range_end: u64,
		epoch: u64,
		clock: &dyn PhysicalClock,
		ttl_ms: u32,
		current_leader: Option<u32>,
		pass: &RenewalPass,
	) -> RangeRenewal {
		let now_ms = clock.now_ms();
		let now_hlc = HybridTimestamp {
			physical_ms: now_ms,
			logical: 0,
			node_id: self.local_node.id,
		};
		let expiry_hlc = HybridTimestamp {
			physical_ms: now_ms.saturating_add(ttl_ms as u64),
			logical: 0,
			node_id: self.local_node.id,
		};
		let request = AcquireLeaseRequestMeta {
			range_start,
			range_end,
			epoch,
			holder: self.local_node,
			requested_hlc: now_hlc,
			ttl_ms,
			_pad: 0,
		};
		let outcome = if current_leader == Some(self.local_node.id) {
			let lease = self.make_signed_lease(
				epoch,
				self.local_node,
				range_start,
				range_end,
				now_hlc,
				expiry_hlc,
			);
			let Some(committed) = pass
				.bounded(self.raft.client_write(WendelonRequest::AcquireLease(lease)))
				.await
			else {
				return RangeRenewal::Unanswered("its own commit of a grant".into());
			};
			match committed {
				Ok(resp) => match resp.data {
					WendelonResponse::Success => Ok(LeaseAcquireOutcome::Granted(lease)),
					WendelonResponse::LeaseDenied => {
						Ok(LeaseAcquireOutcome::Denied(LeaseStatus::DENIED_OVERLAP))
					}
					WendelonResponse::Error(message) => Err(Error::new(
						ErrorKind::Other,
						format!("lease manager: state machine rejected AcquireLease: {message}"),
					)),
					WendelonResponse::Refused(refusal) => Err(Error::other(format!(
						"unexpected slab refusal for a non-slab command: {refusal:?}"
					))),
					unexpected @ (WendelonResponse::Op(_)
					| WendelonResponse::Decision(_)
					| WendelonResponse::LeaseResults(_)) => Err(Error::other(format!(
						"unexpected response to this command: {unexpected:?}"
					))),
					WendelonResponse::Root2Dormant => Err(Error::other("Root2 carrier is dormant")),
				},
				Err(err) => Err(Error::new(
					ErrorKind::Other,
					format!("lease manager: local AcquireLease commit failed: {err}"),
				)),
			}
		} else if let Some(leader_id) = current_leader {
			let Some(answered) = pass
				.bounded(self.request_remote_lease(
					NodeId {
						id: leader_id,
						_reserved: 0,
					},
					request,
				))
				.await
			else {
				return RangeRenewal::Unanswered(format!("the control leader, node {leader_id}"));
			};
			answered
		} else {
			return RangeRenewal::NotGranted;
		};
		match outcome {
			Ok(LeaseAcquireOutcome::Granted(lease)) => {
				if Some(lease.issuer.id) == current_leader
					&& self.lease_authority.verify_lease(&lease).is_ok()
				{
					self.cache_granted_lease(lease);
					metrics().data_ring_lease_grants_total.inc();
					return RangeRenewal::Granted;
				} else {
					tracing::warn!(
						range_start,
						range_end,
						epoch,
						"lease manager: leader returned an invalidly signed lease"
					);
				}
			}
			Ok(LeaseAcquireOutcome::Denied(_status)) => {
				metrics().data_ring_lease_denials_total.inc();
				// Another holder (or a stale-epoch request)
				// blocks us — skip, keep the old entry if any.
				// The next tick retries.
			}
			Err(e) => {
				metrics().data_ring_lease_errors_total.inc();
				tracing::warn!(
					range_start,
					range_end,
					epoch,
					error = %e,
					"lease manager: client_write failed; will retry next tick"
				);
			}
		}
		RangeRenewal::NotGranted
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_walk_holds_each_target_once_from_the_cursor_and_wraps() {
		let targets = [10_u64, 20, 30, 40];
		let walk = |cursor| walk_from(&targets, |start| *start, cursor);
		assert_eq!(walk(None), [10, 20, 30, 40]);
		assert_eq!(walk(Some(0)), [10, 20, 30, 40]);
		assert_eq!(walk(Some(10)), [10, 20, 30, 40]);
		assert_eq!(walk(Some(30)), [30, 40, 10, 20]);
		// A cursor naming a range no longer held selects the next one.
		assert_eq!(walk(Some(25)), [30, 40, 10, 20]);
		assert_eq!(walk(Some(40)), [40, 10, 20, 30]);
		// Past every start: from the front.
		assert_eq!(walk(Some(41)), [10, 20, 30, 40]);
		assert!(walk_from(&[] as &[u64], |start| *start, Some(7)).is_empty());
		for cursor in [None, Some(0), Some(15), Some(40), Some(99)] {
			let mut sorted = walk(cursor);
			sorted.sort_unstable();
			assert_eq!(sorted, targets, "cursor {cursor:?}");
		}
	}

	#[test]
	fn the_continuation_gives_a_starved_unit_the_next_whole_budget_and_moves_past_a_first_unit() {
		let starts = [30_u64, 40, 10, 20];
		// Not attempted: that target itself, never skipped.
		assert_eq!(continuation(&starts, Stopped::Before(0)), Some(30));
		assert_eq!(continuation(&starts, Stopped::Before(2)), Some(10));
		// Unanswered after earlier units had used the budget: that unit, first next time.
		let later = |unit, next| continuation(&starts, Stopped::Unanswered { unit, next });
		assert_eq!(later(1, 2), Some(40));
		assert_eq!(later(2, 4), Some(10));
		assert_eq!(later(3, 4), Some(20));
		// Unanswered as the first unit of the pass: the target after the whole unit.
		assert_eq!(later(0, 1), Some(40));
		assert_eq!(later(0, 2), Some(10));
		// The first unit was the whole walk: wrap to its first.
		assert_eq!(later(0, 4), Some(30));
		// One target: it is next again (there is no other).
		assert_eq!(
			continuation(&[7], Stopped::Unanswered { unit: 0, next: 1 }),
			Some(7)
		);
		assert_eq!(continuation(&[], Stopped::Before(0)), None);
	}

	/// One pass over units that each take `cost` milliseconds to answer (`None`: never),
	/// with the production helpers in the production order: a walk from the cursor, the
	/// spent-budget check before each unit, each unit under the one deadline, and the
	/// continuation left behind. Returns the units answered in time and the new cursor.
	async fn one_pass(
		units: &[(u64, Option<u64>)],
		cursor: Option<u64>,
		ttl_ms: u32,
	) -> (Vec<u64>, Option<u64>) {
		let pass = RenewalPass::begin(ttl_ms);
		let walk = walk_from(units, |unit| unit.0, cursor);
		let starts: Vec<u64> = walk.iter().map(|unit| unit.0).collect();
		let mut answered = Vec::new();
		for (at, (start, cost)) in walk.iter().copied().enumerate() {
			if pass.expired() {
				return (answered, continuation(&starts, Stopped::Before(at)));
			}
			let work = async move {
				match cost {
					Some(cost) => tokio::time::sleep(Duration::from_millis(cost)).await,
					None => std::future::pending().await,
				}
			};
			if pass.bounded(work).await.is_none() {
				let stopped = Stopped::Unanswered {
					unit: at,
					next: at + 1,
				};
				return (answered, continuation(&starts, stopped));
			}
			answered.push(start);
		}
		(answered, None)
	}

	/// Two units of 2 s each under a 3 s budget: one pass can answer only one. The unit
	/// that ran out of budget behind the other is first in the next pass and is answered
	/// there; neither is left unanswered for ever.
	#[tokio::test(start_paused = true)]
	async fn a_unit_starved_behind_another_is_answered_in_the_next_pass() {
		let units = [(10, Some(2_000)), (20, Some(2_000))];
		let (answered, cursor) = one_pass(&units, None, 3_000).await;
		assert_eq!((answered, cursor), (vec![10], Some(20)));
		let (answered, cursor) = one_pass(&units, cursor, 3_000).await;
		assert_eq!((answered, cursor), (vec![20], Some(10)));
		let (answered, cursor) = one_pass(&units, cursor, 3_000).await;
		assert_eq!((answered, cursor), (vec![10], Some(20)));
		// Over any two consecutive passes both were answered.
		let mut seen = std::collections::BTreeSet::new();
		let mut cursor = None;
		for _ in 0..6 {
			let (answered, next) = one_pass(&units, cursor, 3_000).await;
			assert_eq!(answered.len(), 1);
			seen.extend(answered);
			cursor = next;
		}
		assert_eq!(seen.into_iter().collect::<Vec<_>>(), [10, 20]);
	}

	/// A unit that never answers, first in the list, with two quick ones behind it: it
	/// takes one whole pass, and the pass after serves the others; it never keeps them from
	/// being answered, however long it stays unanswered.
	#[tokio::test(start_paused = true)]
	async fn a_unit_that_never_answers_does_not_keep_the_others_from_service() {
		let units = [(10, None), (20, Some(100)), (30, Some(100))];
		// It is first: a whole budget, nothing answered, and the next pass begins after it.
		let (answered, cursor) = one_pass(&units, None, 3_000).await;
		assert_eq!((answered, cursor), (vec![], Some(20)));
		// The others are served; it is reached with what is left, and is first next time.
		let (answered, cursor) = one_pass(&units, cursor, 3_000).await;
		assert_eq!((answered, cursor), (vec![20, 30], Some(10)));
		let mut served = 0;
		let mut cursor = cursor;
		for _ in 0..10 {
			let (answered, next) = one_pass(&units, cursor, 3_000).await;
			assert!(!answered.contains(&10));
			served += usize::from(answered == [20, 30]);
			cursor = next;
		}
		assert_eq!(served, 5, "the others are served in every second pass");
	}

	/// A spent budget polls nothing: work that would submit a request when polled is never
	/// started.
	#[tokio::test(start_paused = true)]
	async fn work_is_not_polled_once_the_budget_is_spent() {
		let polled = std::sync::atomic::AtomicBool::new(false);
		let spent = RenewalPass::begin(0);
		assert!(spent.expired());
		let work = async {
			polled.store(true, std::sync::atomic::Ordering::SeqCst);
			1
		};
		assert_eq!(spent.bounded(work).await, None);
		assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
		// With budget left the same work is polled and accepted.
		let pass = RenewalPass::begin(1_000);
		let work = async {
			polled.store(true, std::sync::atomic::Ordering::SeqCst);
			2
		};
		assert_eq!(pass.bounded(work).await, Some(2));
		assert!(polled.load(std::sync::atomic::Ordering::SeqCst));
	}

	#[tokio::test(start_paused = true)]
	async fn a_result_counts_only_when_it_is_observed_before_the_deadline() {
		let pass = RenewalPass::begin(3_000);
		assert_eq!(pass.budget(), Duration::from_secs(3));
		assert!(!pass.expired());
		assert_eq!(pass.bounded(async { 7 }).await, Some(7));
		let slow = async {
			tokio::time::sleep(Duration::from_millis(2_999)).await;
			8
		};
		assert_eq!(pass.bounded(slow).await, Some(8));
		// The budget is one: the time already spent is not given back.
		let over = async {
			tokio::time::sleep(Duration::from_millis(2)).await;
			9
		};
		assert_eq!(pass.bounded(over).await, None);
		assert!(pass.expired());
		// After the deadline even a ready result is not accepted.
		assert_eq!(pass.bounded(async { 10 }).await, None);
	}

	/// A result that is ready only when it is polled late is discarded: the wrapper's own
	/// timer is not the check.
	#[tokio::test(start_paused = true)]
	async fn a_result_that_completes_on_a_late_poll_is_discarded() {
		let pass = RenewalPass::begin(1_000);
		// Completes without yielding, but only after the clock was moved past the deadline
		// from inside its one poll: the timeout wrapper returns it as completed.
		let late = async {
			tokio::time::advance(Duration::from_millis(1_500)).await;
			11
		};
		assert_eq!(pass.bounded(late).await, None);
	}
}
