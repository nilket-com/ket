//! A coordinated table write attempt and its caller's deadline.
//!
//! The deadline bounds when the attempt has its answer, not the attempt's
//! work. An [`AttemptGate`] decides once between the two: the
//! attempt claims it immediately before it may submit a reservation or resume
//! a recorded one, and the deadline expires it. An attempt that claimed is
//! polled to its end whatever the deadline does, by the handler that owns it;
//! an attempt the deadline reached first is dropped, having submitted nothing.

use crate::schema::{TableWriteRequestMeta, TableWriteResponseMeta};
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

const OPEN: u8 = 0;
const OWNED: u8 = 1;
const EXPIRED: u8 = 2;

/// One attempt's gate: Open until the attempt claims it (Owned) or its
/// deadline expires it (Expired). Whichever comes first is final.
pub(super) struct AttemptGate {
	deadline: Option<Instant>,
	state: AtomicU8,
}

/// Proof that an attempt claimed its gate: it may submit a reservation or
/// resume a recorded one, and from here on its future is never dropped for
/// its deadline. Only [`AttemptGate::claim`] makes one.
pub(super) struct OwnedAttempt {
	_claimed: (),
}

impl AttemptGate {
	/// The gate of an attempt whose caller named no deadline: every claim
	/// succeeds.
	pub(super) fn unbounded() -> Self {
		Self::until(None)
	}

	/// The gate of an attempt that answers by `deadline`, if any.
	pub(super) fn until(deadline: Option<Instant>) -> Self {
		Self {
			deadline,
			state: AtomicU8::new(OPEN),
		}
	}

	/// Claims the gate for the attempt, unless its deadline has passed on the
	/// clock or has already expired the gate. The clock is read and the gate
	/// taken with nothing awaited between them, so a deadline that passed
	/// before its timer ran still refuses.
	pub(super) fn claim(&self) -> Option<OwnedAttempt> {
		self.claim_at(Instant::now())
	}

	fn claim_at(&self, now: Instant) -> Option<OwnedAttempt> {
		if self.deadline.is_some_and(|deadline| now >= deadline) {
			let _ = self
				.state
				.compare_exchange(OPEN, EXPIRED, Ordering::AcqRel, Ordering::Acquire);
			return None;
		}
		self.state
			.compare_exchange(OPEN, OWNED, Ordering::AcqRel, Ordering::Acquire)
			.ok()
			.map(|_| OwnedAttempt { _claimed: () })
	}

	/// The deadline's side: expires the gate unless the attempt owns it.
	/// `true` means the attempt never claimed and never can.
	fn expire(&self) -> bool {
		match self
			.state
			.compare_exchange(OPEN, EXPIRED, Ordering::AcqRel, Ordering::Acquire)
		{
			Ok(_) => true,
			Err(state) => state == EXPIRED,
		}
	}
}

/// Polls `attempt` under `gate`'s deadline. `Some` is the attempt's own
/// result, ready no later than the deadline was seen: a result that is ready
/// when the deadline is also due wins. `None` means the deadline is the
/// answer:
/// - a deadline already past is never polled at all;
/// - an attempt that had not claimed the gate is dropped, and can never claim;
/// - an attempt that had claimed it is not dropped: `answer_now` is called,
///   and the attempt is then polled here to its end. Its own result is not a
///   second answer.
pub(super) async fn answer_by_deadline<T>(
	gate: &AttemptGate,
	attempt: impl Future<Output = T>,
	answer_now: impl FnOnce(),
) -> Option<T> {
	let Some(deadline) = gate.deadline else {
		return Some(attempt.await);
	};
	if Instant::now() >= deadline && gate.expire() {
		return None;
	}
	tokio::pin!(attempt);
	tokio::select! {
		biased;
		result = &mut attempt => return Some(result),
		_ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
	}
	if gate.expire() {
		return None;
	}
	answer_now();
	let _ = attempt.await;
	None
}

/// A table write's deadline answer, given while the write's work goes on.
pub(super) struct EarlyTableWriteAnswer {
	pub(super) request: TableWriteRequestMeta,
	pub(super) response: TableWriteResponseMeta,
}

/// Where a table write gives its answer when that is ready before its work
/// has ended. The stream handler that made it keeps polling the work.
pub(super) struct EarlyAnswer(tokio::sync::oneshot::Sender<EarlyTableWriteAnswer>);

impl EarlyAnswer {
	pub(super) fn channel() -> (Self, tokio::sync::oneshot::Receiver<EarlyTableWriteAnswer>) {
		let (give, given) = tokio::sync::oneshot::channel();
		(Self(give), given)
	}

	pub(super) fn give(self, answer: EarlyTableWriteAnswer) {
		// The handler holds the other end for as long as it polls the write.
		let _ = self.0.send(answer);
	}
}

#[cfg(test)]
#[path = "write_deadline/tests.rs"]
mod tests;
