//! DC-287 (Codex 01a207a1): one completed WAL append batch, its measured time attributed across
//! the same batch, with the unattributed residual kept visible. Observation only: nothing here
//! changes what the append does.
//!
//! The measured boundaries, all from the monotonic clock:
//! - the batch: the WAL **entry-write loop**, on the std monotonic clock, from beside
//!   `wal_append_started` to the loop's endpoint (the existing `wal_append_latency` span; the
//!   outer `wal_gate` wait and the encoding precede it). The record is frozen at that endpoint,
//!   its total and completion time read together, before any registry access. A later fdatasync
//!   of the batch may still fail;
//! - an ordinary entry write (`AsyncFileOps::write_at_timed`, the io_uring implementation):
//!   - **admission**: the call began → the request was handed to the ring's channel. This
//!     covers the channel reserve, the ring's admission mutex and any owner write permit;
//!   - **queued**: handed to the channel → the ring thread entered `push_request` with it,
//!     before inserting its SQE. A reserve that succeeds at once still allows a long wait here;
//!   - **dispatched**: entered `push_request` → its completion observed by the ring thread while
//!     reaping. This spans the SQE insertion, the submission at the thread's next enter, the
//!     kernel and the device, and the reaping. A late observation alone does not separate the device from the ring's
//!     scheduling or reaping;
//!   - **handoff**: completion observed → the append resumed after the reply;
//! - an ordinary write through an implementation that cannot measure phases: its whole time,
//!   counted as **untraced**;
//! - a **rotation** (`create_wal_segment`): its whole call. The entry's write that follows is
//!   counted as an ordinary write;
//! - a **replace** of a checkpoint-only segment: its whole call, including the writes and syncs
//!   nested in it, which are not counted again as ordinary writes.
//!
//! Everything else inside the batch (the per-entry bookkeeping, the checkpoint page's encoding on
//! the replace branch, and scheduling between the measured spans) is the **residual**.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::storage::WriteTiming;

/// Process-wide batch sequence: 1 for the first completed append batch.
static WAL_APPEND_SEQ: AtomicU64 = AtomicU64::new(0);

/// One completed append batch, attributed. Durations are sums over the batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WalAppendRecord {
	pub(crate) seq: u64,
	pub(crate) first_index: u64,
	pub(crate) last_index: u64,
	pub(crate) entries: u64,
	/// Wall time when the batch's span ended, for placing it against other records.
	pub(crate) completed_unix_ms: u64,
	pub(crate) total: Duration,
	pub(crate) ordinary_writes: u64,
	pub(crate) admission: Duration,
	pub(crate) queued: Duration,
	pub(crate) dispatched: Duration,
	pub(crate) handoff: Duration,
	pub(crate) untraced_writes: u64,
	pub(crate) untraced: Duration,
	pub(crate) rotations: u64,
	pub(crate) rotation: Duration,
	pub(crate) replaces: u64,
	pub(crate) replace: Duration,
}

impl WalAppendRecord {
	/// The measured spans' sum: they are disjoint within the batch.
	pub(crate) fn attributed(&self) -> Duration {
		self.admission
			+ self.queued
			+ self.dispatched
			+ self.handoff
			+ self.untraced
			+ self.rotation
			+ self.replace
	}

	/// The batch time no measured span covers.
	pub(crate) fn residual(&self) -> Duration {
		self.total.saturating_sub(self.attributed())
	}

	/// The published fields: counts as such, durations in microseconds.
	pub(crate) fn fields(&self) -> Vec<(&'static str, u64)> {
		let us = |d: Duration| d.as_micros() as u64;
		vec![
			("seq", self.seq),
			("first_index", self.first_index),
			("last_index", self.last_index),
			("entries", self.entries),
			("completed_unix_ms", self.completed_unix_ms),
			("total_us", us(self.total)),
			("ordinary_writes", self.ordinary_writes),
			("admission_us", us(self.admission)),
			("queued_us", us(self.queued)),
			("dispatched_us", us(self.dispatched)),
			("handoff_us", us(self.handoff)),
			("untraced_writes", self.untraced_writes),
			("untraced_us", us(self.untraced)),
			("rotations", self.rotations),
			("rotation_us", us(self.rotation)),
			("replaces", self.replaces),
			("replace_us", us(self.replace)),
			("residual_us", us(self.residual())),
		]
	}
}

/// The batch being attributed. Its total is measured on the same std monotonic clock as its spans
/// (the log store's own `wal_append_latency` span uses tokio's clock, which a test may pause),
/// from beside that span's start to beside its observation.
pub(crate) struct WalAppendTrace {
	record: WalAppendRecord,
	started: Instant,
}

impl WalAppendTrace {
	pub(crate) fn begin(first_index: u64, last_index: u64, entries: usize) -> Self {
		Self {
			record: WalAppendRecord {
				first_index,
				last_index,
				entries: entries as u64,
				..WalAppendRecord::default()
			},
			started: Instant::now(),
		}
	}

	/// One ordinary entry write.
	pub(crate) fn ordinary(&mut self, timing: WriteTiming) {
		let r = &mut self.record;
		match timing.phases {
			Some(phases) => {
				r.ordinary_writes += 1;
				r.admission += phases.admission;
				r.queued += phases.queued;
				r.dispatched += phases.dispatched;
				r.handoff += phases.handoff;
			}
			None => {
				r.untraced_writes += 1;
				r.untraced += timing.total;
			}
		}
	}

	/// One rotation (`create_wal_segment`), timed from `started`.
	pub(crate) fn rotation(&mut self, started: Instant) {
		self.record.rotations += 1;
		self.record.rotation += started.elapsed();
	}

	/// One checkpoint-only replace, timed from `started`, its nested writes included.
	pub(crate) fn replace(&mut self, started: Instant) {
		self.record.replaces += 1;
		self.record.replace += started.elapsed();
	}

	/// The completed batch.
	pub(crate) fn finish(self) -> WalAppendRecord {
		let total = self.started.elapsed();
		WalAppendRecord {
			seq: WAL_APPEND_SEQ.fetch_add(1, Ordering::Relaxed) + 1,
			completed_unix_ms: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.map_or(0, |elapsed| elapsed.as_millis() as u64),
			total,
			..self.record
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::storage::{WritePhases, WriteTiming};

	fn ms(n: u64) -> Duration {
		Duration::from_millis(n)
	}

	#[test]
	fn a_batch_attributes_its_spans_and_keeps_the_residual() {
		let mut trace = WalAppendTrace::begin(7, 9, 3);
		trace.ordinary(WriteTiming {
			total: ms(10),
			phases: Some(WritePhases {
				admission: ms(1),
				queued: ms(2),
				dispatched: ms(3),
				handoff: ms(4),
			}),
		});
		trace.ordinary(WriteTiming {
			total: ms(5),
			phases: None,
		});
		trace.record.rotations += 1;
		trace.record.rotation += ms(6);
		trace.record.replaces += 1;
		trace.record.replace += ms(7);
		let mut record = trace.finish();
		record.total = ms(40);
		assert!(record.seq > 0);
		assert_eq!(
			(record.first_index, record.last_index, record.entries),
			(7, 9, 3)
		);
		assert_eq!((record.ordinary_writes, record.untraced_writes), (1, 1));
		assert_eq!(record.attributed(), ms(28));
		assert_eq!(record.residual(), ms(12));
		let fields = record.fields();
		assert_eq!(fields.len(), 18);
		assert_eq!(
			fields.iter().find(|(name, _)| *name == "residual_us"),
			Some(&("residual_us", 12_000))
		);
	}
}
