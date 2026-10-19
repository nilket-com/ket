//! One logical add per counter write key.
//!
//! Within one tenant leader's store, a write key `W` (client uuid, then the
//! little-endian sequence number) names exactly one add `(counter key, delta)`.
//! The leader records that add beside the outcome it produced:
//!
//! - **K1.** A request whose `W` is known and whose `(key, delta)` matches the
//!   record is a retry: it gets the original outcome.
//! - **K2.** Any other request under a known `W` is refused with
//!   `WriteKeyConflict`. Nothing is applied and the recorded outcome is never
//!   returned. The full counter key bytes are compared, never a hash of them.
//! - **K3.** A per-`W` gate admits one add at a time, so two concurrent adds
//!   under one `W` cannot both miss the record and both apply.
//! - **K5.** Everything after the eligibility checks runs on one spawned task
//!   that owns the write-key gate and the counter-key lock. A dropped caller
//!   cancels no step and releases no guard, so a durable witness is never left
//!   without its record, its pending entry and its published value.
//!
//! An unkeyed applied record (written before the keyed frame existed, with no
//! keyed witness for its operation) cannot show which add it records, so every
//! request under its write key is refused, a genuine retry included. That is
//! the compatibility limit of the older record.
use std::num::NonZeroU64;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::storage::counter::visibility::CounterVisibilityError;
use crate::storage::counter::{CounterAddOutcome, CounterKey, CounterLeaderError, CounterWriteKey};

/// The add a write key names, and the outcome it produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RecordedAdd {
	/// `None` only for an unkeyed applied record; see the module docs.
	pub(super) key: Option<CounterKey>,
	pub(super) delta: NonZeroU64,
	pub(super) outcome: CounterAddOutcome,
}

impl RecordedAdd {
	/// The add a durable record or witness describes. Its delta is exact: a zero
	/// delta and an overflowing add are refused before anything is written, so
	/// `current <= previous` is corruption and recovery fails closed.
	pub(super) fn from_durable(
		key: Option<CounterKey>,
		previous: u64,
		current: u64,
	) -> std::io::Result<Self> {
		let delta = current
			.checked_sub(previous)
			.and_then(NonZeroU64::new)
			.ok_or_else(|| CounterVisibilityError::Conflict.io())?;
		Ok(Self {
			key,
			delta,
			outcome: CounterAddOutcome { previous, current },
		})
	}

	/// K1 and K2: the original outcome for a retry of this add, and a conflict
	/// for anything else under its write key (a zero delta included, since the
	/// recorded delta is at least one).
	pub(super) fn answer(
		&self,
		key: &CounterKey,
		delta: u64,
	) -> Result<CounterAddOutcome, CounterLeaderError> {
		match &self.key {
			Some(recorded) if recorded == key && self.delta.get() == delta => Ok(self.outcome),
			_ => Err(CounterLeaderError::WriteKeyConflict),
		}
	}
}

/// K3 admission for one write key. The add's own task takes it before the
/// record lookup and holds it until the task ends: through the install and the
/// commit fan-out, or until the request returns having applied nothing. A
/// dropped caller never releases it. Dropping it reclaims the gate's map entry
/// when no other request holds or awaits the gate.
pub(super) struct WriteKeyGate<'a> {
	gates: &'a DashMap<CounterWriteKey, Arc<Mutex<()>>>,
	write_key: CounterWriteKey,
	/// `None` only when a test admits the request without its gate.
	guard: Option<OwnedMutexGuard<()>>,
	#[cfg(test)]
	keep_entry: bool,
}

impl<'a> WriteKeyGate<'a> {
	pub(super) async fn admit(
		gates: &'a DashMap<CounterWriteKey, Arc<Mutex<()>>>,
		write_key: CounterWriteKey,
	) -> Self {
		// The entry reference is a temporary: no map reference lives across the await.
		let gate = gates.entry(write_key).or_default().clone();
		let guard = gate.lock_owned().await;
		Self {
			gates,
			write_key,
			guard: Some(guard),
			#[cfg(test)]
			keep_entry: false,
		}
	}

	#[cfg(test)]
	pub(super) fn bypassed(
		gates: &'a DashMap<CounterWriteKey, Arc<Mutex<()>>>,
		write_key: CounterWriteKey,
	) -> Self {
		Self {
			gates,
			write_key,
			guard: None,
			keep_entry: false,
		}
	}

	#[cfg(test)]
	pub(super) fn keep_entry(mut self) -> Self {
		self.keep_entry = true;
		self
	}
}

impl Drop for WriteKeyGate<'_> {
	fn drop(&mut self) {
		let Some(guard) = self.guard.take() else {
			return;
		};
		#[cfg(test)]
		if self.keep_entry {
			return;
		}
		// Still holding the gate, with no map reference held. Under the shard write
		// lock, two owners are the map and this guard: no request holds or awaits it.
		let gate = OwnedMutexGuard::mutex(&guard);
		self.gates.remove_if(&self.write_key, |_, stored| {
			Arc::ptr_eq(stored, gate) && Arc::strong_count(gate) == 2
		});
		drop(guard);
	}
}
