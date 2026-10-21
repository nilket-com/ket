//! An enter failure (or Rust unwind) does not establish kernel cessation.
//! Only a request's CQE permits resource reclamation, including after cancellation.
use super::{InFlightRequest, IoCompletion, IoUring, IoUringClass, metrics, types};
use std::collections::HashMap;
use std::io;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct InFlightRequests {
	requests: HashMap<u64, InFlightRequest>,
	failure_errno: Option<i32>,
}

impl InFlightRequests {
	pub(super) fn is_empty(&self) -> bool {
		self.requests.is_empty()
	}
	pub(super) fn len(&self) -> usize {
		self.requests.len()
	}
	pub(super) fn insert(&mut self, id: u64, request: InFlightRequest) {
		match self.requests.entry(id) {
			std::collections::hash_map::Entry::Vacant(slot) => {
				slot.insert(request);
			}
			std::collections::hash_map::Entry::Occupied(_) => panic!("live io_uring cookie reused"),
		}
	}
	pub(super) fn remove(&mut self, id: &u64) -> Option<InFlightRequest> {
		self.requests.remove(id)
	}
}

impl Drop for InFlightRequests {
	fn drop(&mut self) {
		// A panic or failed cancellation must not free memory the kernel can
		// still address. Deliberately retain the allocation, file, directory
		// owner and write permit until process exit. Retained permits keep a
		// node from reporting a clean close or releasing LOCK; its existing
		// shutdown budget/watchdog handles this unrecoverable state. No Linux
		// version or ring-close timing assumption makes reclamation safe here.
		let mut notifications = Vec::with_capacity(self.requests.len());
		for (_, request) in self.requests.drain() {
			let InFlightRequest {
				observation,
				_file,
				_owner,
				_permit,
				buf,
				reply,
				..
			} = request;
			std::mem::forget((_file, _owner, _permit, buf));
			notifications.push((observation, reply));
		}
		// Disarm ALL resource destructors before notifying anyone: a receiver's
		// custom waker can panic. Unwinding must not free the remaining buffers.
		for (observation, reply) in notifications {
			drop(observation); // Withhold observation: completion was not established.
			let _ = reply.send(IoCompletion {
				buf: None,
				result: -self.failure_errno.unwrap_or(libc::EIO),
			});
		}
	}
}

pub(super) fn retryable_enter_error(error: &io::Error) -> bool {
	matches!(
		error.raw_os_error(),
		Some(libc::EINTR | libc::EAGAIN | libc::EBUSY)
	)
}

/// Linux 6.0+ provides synchronous cancellation. An older kernel, an
/// uninterruptible operation or an unconfirmed cancellation uses quarantine.
pub(super) fn cancel_outstanding(ring: &IoUring) -> io::Result<()> {
	let deadline = Instant::now() + Duration::from_secs(1);
	loop {
		let remaining = deadline.saturating_duration_since(Instant::now());
		if remaining.is_zero() {
			return Err(io::Error::from_raw_os_error(libc::ETIME));
		}
		match ring.submitter().register_sync_cancel(
			Some(types::Timespec::from(remaining)),
			types::CancelBuilder::any(),
		) {
			Ok(()) => return Ok(()),
			Err(e) if e.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
			Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
			Err(e) => return Err(e),
		}
	}
}

pub(super) fn fail_ring(
	mut ring: IoUring,
	mut in_flight: InFlightRequests,
	class: IoUringClass,
	error: &io::Error,
	cancel: impl FnOnce(&mut IoUring) -> io::Result<()>,
) {
	let errno = error.raw_os_error().unwrap_or(libc::EIO);
	in_flight.failure_errno = Some(errno);
	for request in in_flight.requests.values() {
		metrics().observe_io_uring_error(class, request.op.as_str(), errno);
	}
	if let Err(e) = cancel(&mut ring) {
		tracing::error!(error = %e, "io_uring cancellation unconfirmed");
	}
	// Even a successful cancel-all call is not a per-request completion
	// certificate (it can count running cancellation attempts). Reclaim only
	// requests whose actual CQEs are present. Unsubmitted SQEs and any still
	// pending work remain quarantined; do not submit more work to flush them.
	let mut completed = Vec::new();
	for cqe in ring.completion() {
		if let Some(request) = in_flight.remove(&cqe.user_data()) {
			completed.push(request);
		}
	}
	// This worker is the sole submitter (no SQPOLL). Keep the remaining
	// resources alive through ring teardown, without assuming close waits.
	drop(ring);
	for request in completed {
		let _ = request.reply.send(IoCompletion {
			buf: request.buf,
			result: -errno,
		});
	}
	if !in_flight.is_empty() {
		tracing::error!(
			pending = in_flight.len(),
			"quarantining io_uring requests without CQEs"
		);
	}
}
