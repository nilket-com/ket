//! ADR 542 mutation clocks. Write progress means durability advancing, so these
//! clocks deliberately do not reuse `QueryClocks::bound`, whose completion-based
//! progress semantics would count a failed replica leg as progress.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::caller_deadline_from_remaining_millis;
use crate::schema::{MutationStopReason, NodeId, WriteStatus};

pub(crate) const DEFAULT_WRITE_STALL_BOUND: Duration = Duration::from_secs(30);
// This deliberately shares the local mutation stall bound: changing that
// policy also changes how long fresh-leader admission waits on a peer pull.
pub(crate) const ORPHAN_WITNESS_PULL_TIMEOUT: Duration = DEFAULT_WRITE_STALL_BOUND;

pub(crate) async fn bound_orphan_witness_pull<T>(
	peer: NodeId,
	pull: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
	match tokio::time::timeout(ORPHAN_WITNESS_PULL_TIMEOUT, pull).await {
		Ok(result) => result,
		Err(_) => Err(std::io::Error::new(
			std::io::ErrorKind::TimedOut,
			format!(
				"orphan-witness pull from peer {peer:?} timed out after {}s",
				ORPHAN_WITNESS_PULL_TIMEOUT.as_secs()
			),
		)),
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteExpiry {
	pub(crate) status: WriteStatus,
	pub(crate) reason: MutationStopReason,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteClocks {
	last_progress: Instant,
	stall_bound: Duration,
	caller_deadline: Option<Instant>,
	#[cfg(test)]
	pub(crate) progress_marks: u64,
}

impl WriteClocks {
	pub(crate) fn from_remaining_millis(millis: u64) -> Self {
		Self::new(
			DEFAULT_WRITE_STALL_BOUND,
			caller_deadline_from_remaining_millis(millis).map(Instant::from_std),
		)
	}

	pub(crate) fn new(stall_bound: Duration, caller_deadline: Option<Instant>) -> Self {
		Self {
			last_progress: Instant::now(),
			stall_bound,
			caller_deadline,
			#[cfg(test)]
			progress_marks: 0,
		}
	}

	/// One durability unit completed: a local durable append or a durable remote ack.
	pub(crate) fn progressed(&mut self) {
		self.last_progress = Instant::now();
		#[cfg(test)]
		{
			self.progress_marks = self.progress_marks.saturating_add(1);
		}
	}

	pub(crate) fn next_expiry(self) -> (Duration, WriteExpiry) {
		let stall_remaining = self
			.stall_bound
			.saturating_sub(self.last_progress.elapsed());
		if let Some(deadline) = self.caller_deadline {
			let deadline_remaining = deadline.saturating_duration_since(Instant::now());
			// At an exact tie the absolute caller deadline wins: progress may slide
			// the stall clock, but it must never obscure the caller's fixed cap.
			if deadline_remaining <= stall_remaining {
				return (
					deadline_remaining,
					WriteExpiry {
						status: WriteStatus::WRITE_DEADLINE_EXCEEDED,
						reason: MutationStopReason::DEADLINE,
					},
				);
			}
		}
		(
			stall_remaining,
			WriteExpiry {
				status: WriteStatus::WRITE_STALLED,
				reason: MutationStopReason::NO_PROGRESS,
			},
		)
	}

	pub(crate) fn expired(self) -> Option<WriteExpiry> {
		let (remaining, expiry) = self.next_expiry();
		remaining.is_zero().then_some(expiry)
	}
}

pub(crate) async fn acquire_mutation_work_permit(
	slots: Arc<Semaphore>,
	clocks: WriteClocks,
) -> std::io::Result<Result<OwnedSemaphorePermit, WriteExpiry>> {
	let (wait, expiry) = clocks.next_expiry();
	match tokio::time::timeout(wait, slots.acquire_owned()).await {
		Ok(Ok(permit)) => Ok(Ok(permit)),
		Ok(Err(_)) => Err(std::io::Error::other("mutation work semaphore closed")),
		Err(_) => Ok(Err(expiry)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test(start_paused = true)]
	async fn durability_progress_resets_stall_but_never_caller_deadline() {
		let deadline = Instant::now() + Duration::from_secs(5);
		let mut clocks = WriteClocks::new(Duration::from_secs(2), Some(deadline));
		tokio::time::advance(Duration::from_secs(1)).await;
		clocks.progressed();
		let (first, first_expiry) = clocks.next_expiry();
		assert_eq!(first, Duration::from_secs(2));
		assert_eq!(first_expiry.status, WriteStatus::WRITE_STALLED);
		tokio::time::advance(Duration::from_secs(2)).await;
		clocks.progressed();
		let (second, second_expiry) = clocks.next_expiry();
		assert!(second <= Duration::from_secs(2));
		assert_eq!(second_expiry.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
		assert_eq!(clocks.progress_marks, 2);
	}

	#[tokio::test(start_paused = true)]
	async fn queued_expiry_never_enters_work_and_permits_recover_after_true_completion() {
		let slots = Arc::new(Semaphore::new(1));
		let holder = acquire_mutation_work_permit(
			slots.clone(),
			WriteClocks::new(Duration::from_secs(10), None),
		)
		.await
		.unwrap()
		.unwrap();
		let queued = tokio::spawn(acquire_mutation_work_permit(
			slots.clone(),
			WriteClocks::new(Duration::from_secs(1), None),
		));
		tokio::time::advance(Duration::from_secs(1)).await;
		let expiry = queued.await.unwrap().unwrap().unwrap_err();
		assert_eq!(expiry.status, WriteStatus::WRITE_STALLED);
		assert_eq!(expiry.reason, MutationStopReason::NO_PROGRESS);
		assert_eq!(
			slots.available_permits(),
			0,
			"holder still owns real work slot"
		);
		drop(holder);
		let recovered = acquire_mutation_work_permit(
			slots.clone(),
			WriteClocks::new(Duration::from_secs(1), None),
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(slots.available_permits(), 0);
		drop(recovered);
		assert_eq!(slots.available_permits(), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn orphan_witness_pull_timeout_is_bounded_and_named() {
		let stalled = bound_orphan_witness_pull(
			NodeId {
				id: 7,
				_reserved: 0,
			},
			std::future::pending::<std::io::Result<()>>(),
		);
		tokio::pin!(stalled);
		tokio::task::yield_now().await;
		tokio::time::advance(ORPHAN_WITNESS_PULL_TIMEOUT).await;
		let error = stalled.await.unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
		assert!(error.to_string().contains("peer"));
		assert!(error.to_string().contains("30s"));
	}
}
