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

/// Derive a local monotonic deadline from the table-query wire's remaining duration,
/// where zero is the encoding for "the caller sent no deadline" — the client transmits
/// one millisecond rather than zero for an already-expired deadline, precisely so that
/// expiry stays attributable here instead of vanishing. Checked: an arbitrary wire u64
/// must not overflow `Instant` arithmetic — an unrepresentable deadline degrades to "no
/// caller deadline" (the stall bound still applies), never to a panic on
/// attacker-controlled input.
pub(crate) fn caller_deadline_from_remaining_millis(millis: u64) -> Option<Instant> {
	if millis == 0 {
		return None;
	}
	Instant::now().checked_add(Duration::from_millis(millis))
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
	Instant::now().checked_add(Duration::from_millis(millis))
}
