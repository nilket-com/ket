//! ADR 541's two query clocks and the deadline derivations that feed them.
//!
//! Carved out of the `data_server` root under ADR 077's bounded-root rule: the clocks
//! are a self-contained mechanism with their own invariants, and the root must ratchet
//! down rather than absorb each new one.

use std::time::{Duration, Instant};

use super::QueryExecutionBudget;
use crate::schema::ReadStatus;

/// ADR 541's two clocks, enforced concurrently with distinct expiry statuses.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryClocks {
	pub(crate) last_progress: Instant,
	pub(crate) stall_bound: Duration,
	pub(crate) caller_deadline: Option<Instant>,
	/// Count of progress marks. Progress must be a property of examination, not of
	/// output — a scan that matches nothing still progresses — and this counter is
	/// what lets a fixture assert that without racing wall clocks.
	pub(crate) progress_marks: u64,
}

impl QueryClocks {
	pub(crate) fn new(stall_bound: Duration, caller_deadline: Option<Instant>) -> Self {
		Self {
			last_progress: Instant::now(),
			stall_bound,
			caller_deadline,
			progress_marks: 0,
		}
	}

	pub(crate) fn from_budget(budget: &QueryExecutionBudget) -> Self {
		Self::new(budget.max_wall_time, budget.caller_deadline)
	}

	/// A unit of progress: a location processed or a batch produced. Deliberately not
	/// per row (a trickle must still expire) and not whole-result (that degenerates
	/// back into a lifetime).
	pub(crate) fn progressed(&mut self) {
		self.last_progress = Instant::now();
		self.progress_marks = self.progress_marks.saturating_add(1);
	}

	pub(crate) fn expired(&self) -> Option<ReadStatus> {
		if let Some(deadline) = self.caller_deadline {
			if Instant::now() > deadline {
				return Some(ReadStatus::QUERY_DEADLINE_EXCEEDED);
			}
		}
		if self.last_progress.elapsed() > self.stall_bound {
			return Some(ReadStatus::QUERY_STALLED);
		}
		None
	}

	/// Bound one awaited unit of work preemptively. Polling between work units cannot
	/// see a blocked operation at all — a wedged read would hold the query open
	/// forever and a slow-but-successful one would reset the clock afterwards, hiding
	/// the overrun. Here the operation itself is raced against both clocks
	/// concurrently: whichever bound is nearer supplies the timeout, and an expiry is
	/// attributed to the clock that actually fired. Completion marks progress.
	pub(crate) async fn bound<T>(
		&mut self,
		operation: impl std::future::Future<Output = T>,
	) -> Result<T, ReadStatus> {
		let mut limit = self.stall_bound;
		let mut deadline_is_nearer = false;
		if let Some(deadline) = self.caller_deadline {
			let remaining = deadline.saturating_duration_since(Instant::now());
			if remaining < limit {
				limit = remaining;
				deadline_is_nearer = true;
			}
		}
		match tokio::time::timeout(limit, operation).await {
			Ok(value) => {
				self.progressed();
				Ok(value)
			}
			Err(_) => Err(if deadline_is_nearer {
				ReadStatus::QUERY_DEADLINE_EXCEEDED
			} else {
				ReadStatus::QUERY_STALLED
			}),
		}
	}
}

/// Place a duration ahead of `now` without ever losing the fact that a deadline exists.
///
/// `Instant::checked_add` reports `None` for a duration past the platform's representable
/// horizon. Propagating that `None` as "no caller deadline" would let an absurdly large
/// wire value *remove* the caller's cap — presence degrading into absence, which is the
/// same permissive inversion as conflating the absent-sentinels. A deadline the platform
/// cannot represent is instead clamped to *a* representable instant, found by descending
/// halves: deterministic, and needing no platform constant. It is deliberately not the
/// furthest such instant — halving lands on a conservative one, at most a factor of two
/// short of the horizon. That is the safe direction (an earlier deadline never removes a
/// cap) and it only ever applies to durations no real caller sends.
fn instant_after_clamped(now: Instant, duration: Duration) -> Instant {
	if let Some(instant) = now.checked_add(duration) {
		return instant;
	}
	let mut candidate = duration;
	// u64 millis cannot need more halvings than this; the loop is a bound, not a search.
	for _ in 0..128 {
		candidate /= 2;
		if candidate.is_zero() {
			break;
		}
		if let Some(instant) = now.checked_add(candidate) {
			return instant;
		}
	}
	now
}

/// Derive a local monotonic deadline from the table-query wire's remaining duration,
/// where zero is the encoding for "the caller sent no deadline" — the client transmits
/// one millisecond rather than zero for an already-expired deadline, precisely so that
/// expiry stays attributable here instead of vanishing. Any non-zero value yields a
/// deadline: an unrepresentable one clamps (see [`instant_after_clamped`]) rather than
/// vanishing, and no wire value can panic.
pub(crate) fn caller_deadline_from_remaining_millis(millis: u64) -> Option<Instant> {
	if millis == 0 {
		return None;
	}
	Some(instant_after_clamped(
		Instant::now(),
		Duration::from_millis(millis),
	))
}

/// Derive a local monotonic deadline from a caller-transmitted wall-time *budget*, whose
/// absent-sentinel is `u64::MAX` rather than zero. The two absent-sentinels must not be
/// conflated: on this path zero is a real budget meaning "no time at all", so it yields
/// an already-expired instant and the query fails attributably. Routing it through the
/// wire rule above would invert the strictest budget into the loosest.
pub(crate) fn caller_deadline_from_budget_millis(millis: u64) -> Option<Instant> {
	if millis == u64::MAX {
		return None;
	}
	Some(instant_after_clamped(
		Instant::now(),
		Duration::from_millis(millis),
	))
}
