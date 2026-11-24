//! DC-290 (Codex 01a20ba5): beside the stalled-write record, the same opt-in trace keeps the
//! last [`CAPACITY`] coordinated table write attempts whose own result was their answer,
//! whatever that answer was and however long it took. Each carries the write's identity, the
//! wire attempts remaining, the answered outcome, and four contiguous monotonic spans:
//!
//! 1. `pre`: the request frame in hand → the coordinator's entry (decode and what precedes
//!    the coordinator); unknown for a caller with no frame;
//! 2. `coord_before`: the entry → the children's start (the descriptor read, every Lookup,
//!    the Reserve);
//! 3. `children`: the children's start → their return, all children as one interval;
//! 4. `coord_after`: the children's return → the answer (the Finalize).
//!
//! Each mark is taken once, at a fixed point outside the Lookup loop, so the spans partition
//! entry → answer exactly; an attempt answered before its children has zero in the last two.
//! Observation only. An attempt whose caller's deadline answered first is not recorded: its
//! absence says nothing of the time it took. A node without the trace takes no clock reading.

use std::time::Duration;

use crate::network::data_server::coordinator::Settled;

/// The attempts a trace keeps; older ones are overwritten and counted.
pub(crate) const CAPACITY: usize = 32;

/// What the coordinator answered, as exported: `outcome` and, for the two status-carrying
/// answers, the wire status.
pub(crate) fn outcome_of(settled: &Settled) -> (u8, u8) {
	match settled {
		Settled::Committed { .. } => (1, 0),
		Settled::KnownNotCommitted => (2, 0),
		Settled::Redirect(_) => (3, 0),
		Settled::Indeterminate(status) => (4, status.0),
		Settled::Refused(status) => (5, status.0),
		Settled::Deadline => (6, 0),
	}
}

/// One answered attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteAttemptRecord {
	/// 1 for the trace's first record.
	pub(crate) seq: u64,
	pub(crate) client_uuid: [u8; 16],
	pub(crate) sequence_no: u64,
	pub(crate) attempts_remaining: u8,
	/// [`outcome_of`].
	pub(crate) outcome: u8,
	pub(crate) status: u8,
	/// Lookups answered (saturating).
	pub(crate) lookups: u8,
	/// The children of the claim it wrote, when it reached them.
	pub(crate) children: Option<u16>,
	/// `None` when the attempt had no frame reading.
	pub(crate) pre: Option<Duration>,
	pub(crate) coord_before: Duration,
	pub(crate) children_span: Duration,
	pub(crate) coord_after: Duration,
	/// Wall clock at the answer.
	pub(crate) at_unix_ms: u64,
}

/// The three spans inside the coordinator, from its marks: the entry, the children's start and
/// their return when each was taken, and the answer. A mark never taken leaves its span zero
/// and the time up to the answer goes to the last span begun, so the three always sum to
/// entry → answer.
pub(crate) fn spans(
	entered: std::time::Instant,
	started: Option<std::time::Instant>,
	done: Option<std::time::Instant>,
	answered: std::time::Instant,
) -> (Duration, Duration, Duration) {
	match (started, done) {
		(Some(start), Some(done)) => (
			start.saturating_duration_since(entered),
			done.saturating_duration_since(start),
			answered.saturating_duration_since(done),
		),
		(Some(start), None) => (
			start.saturating_duration_since(entered),
			answered.saturating_duration_since(start),
			Duration::ZERO,
		),
		(None, _) => (
			answered.saturating_duration_since(entered),
			Duration::ZERO,
			Duration::ZERO,
		),
	}
}

fn micros(span: Duration) -> u64 {
	u64::try_from(span.as_micros()).unwrap_or(u64::MAX)
}

impl WriteAttemptRecord {
	/// The exported fields. The identity travels in 32-bit words, as the stalled record's.
	pub(crate) fn fields(&self) -> [(&'static str, u64); 19] {
		let word = |at: usize| {
			u64::from(u32::from_be_bytes(
				self.client_uuid[at..at + 4].try_into().expect("four bytes"),
			))
		};
		[
			("seq", self.seq),
			("client_uuid_w0", word(0)),
			("client_uuid_w1", word(4)),
			("client_uuid_w2", word(8)),
			("client_uuid_w3", word(12)),
			("sequence_no_hi", self.sequence_no >> 32),
			("sequence_no_lo", self.sequence_no & u64::from(u32::MAX)),
			("attempts_remaining", u64::from(self.attempts_remaining)),
			("outcome", u64::from(self.outcome)),
			("status", u64::from(self.status)),
			("lookups", u64::from(self.lookups)),
			("children_reached", u64::from(self.children.is_some())),
			("children", self.children.map_or(0, u64::from)),
			("pre_known", u64::from(self.pre.is_some())),
			("pre_us", self.pre.map_or(0, micros)),
			("coord_before_us", micros(self.coord_before)),
			("children_us", micros(self.children_span)),
			("coord_after_us", micros(self.coord_after)),
			("at_unix_ms", self.at_unix_ms),
		]
	}
}

/// A snapshot of the attempts kept: the total ever recorded and the records, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WriteAttemptSnapshot {
	pub(crate) recorded: u64,
	pub(crate) records: Vec<WriteAttemptRecord>,
}

impl WriteAttemptSnapshot {
	/// Records no longer kept.
	pub(crate) fn overwritten(&self) -> u64 {
		self.recorded - self.records.len() as u64
	}
}

struct Ring {
	recorded: u64,
	slots: [Option<WriteAttemptRecord>; CAPACITY],
}

/// One node's answered attempts.
pub(crate) struct WriteAttemptRing(std::sync::Mutex<Ring>);

impl Default for WriteAttemptRing {
	fn default() -> Self {
		Self(std::sync::Mutex::new(Ring {
			recorded: 0,
			slots: [None; CAPACITY],
		}))
	}
}

impl WriteAttemptRing {
	pub(crate) fn push(&self, mut record: WriteAttemptRecord) {
		let mut ring = self.0.lock().unwrap_or_else(|e| e.into_inner());
		ring.recorded += 1;
		record.seq = ring.recorded;
		ring.slots[((record.seq - 1) % CAPACITY as u64) as usize] = Some(record);
	}

	pub(crate) fn snapshot(&self) -> WriteAttemptSnapshot {
		let ring = self.0.lock().unwrap_or_else(|e| e.into_inner());
		let mut records: Vec<WriteAttemptRecord> = ring.slots.iter().flatten().copied().collect();
		records.sort_by_key(|record| record.seq);
		WriteAttemptSnapshot {
			recorded: ring.recorded,
			records,
		}
	}
}

#[cfg(test)]
#[path = "attempts/tests.rs"]
mod tests;
