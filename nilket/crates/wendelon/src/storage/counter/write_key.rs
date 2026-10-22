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
use std::sync::{Arc, Weak};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
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
/// dropped caller never releases it. A participant count represents admissions,
/// including queued futures; it never infers liveness from mutex Arc counts.
pub(super) struct GateEntry {
	mutex: Arc<Mutex<()>>,
	participants: usize,
}

pub(super) type WriteKeyGates = DashMap<CounterWriteKey, GateEntry>;

/// One logical admission, from reservation through waiting and final unlock.
/// Cancelling a waiting future retires its reservation. Reclamation happens
/// only after its guard has unlocked and the last participant has retired.
pub(super) struct WriteKeyGate<'a> {
	gates: &'a WriteKeyGates,
	write_key: CounterWriteKey,
	// Weak identity cannot keep an idle mutex alive or count destructor-held
	// Arcs as admissions. Its allocation remains reserved, preventing ABA.
	identity: Option<Weak<Mutex<()>>>,
	/// `None` while awaiting the mutex, or for the test-only bypass mutant.
	guard: Option<OwnedMutexGuard<()>>,
	#[cfg(test)]
	keep_entry: bool,
}

impl<'a> WriteKeyGate<'a> {
	pub(super) async fn admit(gates: &'a WriteKeyGates, write_key: CounterWriteKey) -> Self {
		let gate = {
			let mut entry = gates.entry(write_key).or_insert_with(|| GateEntry {
				mutex: Arc::new(Mutex::new(())),
				participants: 0,
			});
			entry.participants = entry
				.participants
				.checked_add(1)
				.expect("gate admissions overflow");
			entry.mutex.clone()
		};
		// The ticket exists before waiting: cancelling a queued admission must
		// retire it just as dropping an acquired guard does. No shard spans await.
		let mut ticket = Self {
			gates,
			write_key,
			identity: Some(Arc::downgrade(&gate)),
			guard: None,
			#[cfg(test)]
			keep_entry: false,
		};
		ticket.guard = Some(gate.lock_owned().await);
		ticket
	}

	#[cfg(test)]
	pub(super) fn bypassed(gates: &'a WriteKeyGates, write_key: CounterWriteKey) -> Self {
		Self {
			gates,
			write_key,
			identity: None,
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
		let Some(identity) = self.identity.take() else {
			return;
		};
		// Unlock may synchronously wake and finish another admission. Keep this
		// ticket counted through that callback, and NEVER hold a shard while it runs.
		drop(self.guard.take());
		if let Entry::Occupied(mut entry) = self.gates.entry(self.write_key) {
			if !Weak::ptr_eq(&identity, &Arc::downgrade(&entry.get().mutex)) {
				debug_assert!(false, "live gate identity changed");
				return;
			}
			let count = &mut entry.get_mut().participants;
			*count = count.checked_sub(1).expect("gate ticket retired twice");
			let reclaim = *count == 0;
			#[cfg(test)]
			let reclaim = reclaim && !self.keep_entry;
			if reclaim {
				// Entry::remove releases its shard before returning the old value.
				drop(entry.remove());
			}
		} else {
			debug_assert!(false, "live gate entry disappeared");
		}
	}
}

#[cfg(test)]
mod reclamation_tests {
	use std::future::Future;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::{Mutex as StdMutex, mpsc};
	use std::task::{Context, Poll, Wake, Waker};
	use std::time::Duration;

	use super::*;

	// A correctness hang detector, not a scheduling delay or throughput budget.
	const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(60);

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn multithread_cancellation_stress_preserves_exclusion_and_reclaims_gates() {
		let gates = Arc::new(WriteKeyGates::new());
		let active = Arc::new([
			AtomicUsize::new(0),
			AtomicUsize::new(0),
			AtomicUsize::new(0),
		]);
		let admitted = Arc::new(AtomicUsize::new(0));
		let cancelled = Arc::new(AtomicUsize::new(0));
		let start = Arc::new(tokio::sync::Barrier::new(48));
		let mut tasks = tokio::task::JoinSet::new();
		for task in 0..48u64 {
			let gates = gates.clone();
			let active = active.clone();
			let admitted = admitted.clone();
			let cancelled = cancelled.clone();
			let start = start.clone();
			tasks.spawn(async move {
				let mut random = task + 1;
				start.wait().await;
				for _ in 0..80 {
					random ^= random << 13;
					random ^= random >> 7;
					random ^= random << 17;
					let key = (random % 3) as usize;
					let admission = WriteKeyGate::admit(&gates, [key as u8; 24]);
					let deadline = Duration::from_micros(1 + random % 200);
					if let Ok(guard) = tokio::time::timeout(deadline, admission).await {
						assert_eq!(
							active[key].fetch_add(1, Ordering::SeqCst),
							0,
							"two holders for key {key}"
						);
						admitted.fetch_add(1, Ordering::Relaxed);
						for _ in 0..(1 + random % 8) {
							tokio::task::yield_now().await;
						}
						assert_eq!(active[key].fetch_sub(1, Ordering::SeqCst), 1);
						drop(guard);
					} else {
						cancelled.fetch_add(1, Ordering::Relaxed);
					}
				}
			});
		}
		tokio::time::timeout(Duration::from_secs(60), async {
			while let Some(result) = tasks.join_next().await {
				result.unwrap();
			}
		})
		.await
		.unwrap();
		assert!(admitted.load(Ordering::Relaxed) > 0);
		assert!(
			cancelled.load(Ordering::Relaxed) > 0,
			"stress did not exercise timeout cancellation"
		);
		assert!(active.iter().all(|value| value.load(Ordering::SeqCst) == 0));
		assert!(
			gates.is_empty(),
			"retained gate after every participant ended"
		);
	}

	struct UnlockWake {
		run: mpsc::SyncSender<()>,
		finished: StdMutex<mpsc::Receiver<()>>,
		wait_inside_unlock: bool,
		inside_wake: AtomicBool,
		calls: AtomicUsize,
		protocol_failed: AtomicBool,
	}

	impl Wake for UnlockWake {
		fn wake(self: Arc<Self>) {
			self.wake_by_ref();
		}

		fn wake_by_ref(self: &Arc<Self>) {
			self.calls.fetch_add(1, Ordering::SeqCst);
			self.inside_wake.store(true, Ordering::SeqCst);
			if self.run.try_send(()).is_err() {
				self.protocol_failed.store(true, Ordering::SeqCst);
			}
			if self.wait_inside_unlock
				&& self
					.finished
					.lock()
					.unwrap()
					.recv_timeout(HANDSHAKE_DEADLINE)
					.is_err()
			{
				// Do not panic inside the owner's Drop. Let it finish, join the
				// bounded worker, then report the protocol failure normally.
				self.protocol_failed.store(true, Ordering::SeqCst);
			}
			self.inside_wake.store(false, Ordering::SeqCst);
		}
	}

	struct Finished(mpsc::SyncSender<()>);

	impl Drop for Finished {
		fn drop(&mut self) {
			// Also release the owner's waker if the worker unwinds.
			let _ = self.0.try_send(());
		}
	}

	/// No copied guard implementation, extra mutex Arc, or artificial guard is
	/// involved. The pinned Tokio semaphore invokes this waker synchronously,
	/// after dropping its waitlist lock, inside OwnedMutexGuard::drop and before
	/// Rust destroys that guard's lock Arc field. Waiting here therefore holds
	/// precisely the real unlock-before-Arc-destruction interval open.
	async fn terminal_waiter(wait_inside_unlock: bool) -> usize {
		let gates = DashMap::new();
		let key = [7; 24];
		let owner = WriteKeyGate::admit(&gates, key).await;
		let (run_tx, run_rx) = mpsc::sync_channel(1);
		let (finished_tx, finished_rx) = mpsc::sync_channel(1);
		let (ready_tx, ready_rx) = mpsc::sync_channel(1);
		let (owner_finished_tx, owner_finished_rx) = mpsc::sync_channel(1);
		let wake = Arc::new(UnlockWake {
			run: run_tx,
			finished: StdMutex::new(finished_rx),
			wait_inside_unlock,
			inside_wake: AtomicBool::new(false),
			calls: AtomicUsize::new(0),
			protocol_failed: AtomicBool::new(false),
		});
		std::thread::scope(|scope| {
			let wake_ref = &wake;
			let gates_ref = &gates;
			let worker = scope.spawn(move || {
				let _finished = Finished(finished_tx);
				let mut admission = Box::pin(WriteKeyGate::admit(gates_ref, key));
				let waker = Waker::from(wake_ref.clone());
				let pending = matches!(
					admission.as_mut().poll(&mut Context::from_waker(&waker)),
					Poll::Pending
				);
				ready_tx.send(pending).unwrap();
				assert!(pending, "waiter must actually queue behind owner");
				run_rx.recv_timeout(HANDSHAKE_DEADLINE).unwrap();
				if !wait_inside_unlock {
					owner_finished_rx.recv_timeout(HANDSHAKE_DEADLINE).unwrap();
				}
				// Do not install another blocking waker during completion.
				let guard = match admission
					.as_mut()
					.poll(&mut Context::from_waker(Waker::noop()))
				{
					Poll::Ready(guard) => guard,
					Poll::Pending => panic!("unlock must have assigned the real waiter its permit"),
				};
				drop(admission);
				let inside = wake_ref.inside_wake.load(Ordering::SeqCst);
				// Borrow the stored Arc; never clone it into the test witness.
				let owners = {
					let entry = gates_ref.get(&key).unwrap();
					Arc::strong_count(&entry.value().mutex)
				};
				drop(guard);
				(inside, owners)
			});
			// All protocol waits have a deadline. Even a readiness failure drops
			// owner and releases the waiting worker before the scoped join.
			let ready = ready_rx.recv_timeout(HANDSHAKE_DEADLINE);
			drop(owner);
			let _ = owner_finished_tx.try_send(());
			let observed = worker.join();
			assert_eq!(ready.unwrap(), true);
			let (inside, owners) = observed.unwrap();
			assert!(!wake.protocol_failed.load(Ordering::SeqCst));
			assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
			assert_eq!(inside, wait_inside_unlock, "terminal destructor phase");
			assert_eq!(owners, if wait_inside_unlock { 3 } else { 2 });
		});
		gates.len()
	}

	#[tokio::test]
	async fn sole_owner_reclaims_its_write_key_gate() {
		let gates = DashMap::new();
		drop(WriteKeyGate::admit(&gates, [7; 24]).await);
		assert_eq!(gates.len(), 0);
	}

	#[tokio::test]
	async fn terminal_waiter_after_owner_arc_drop_reclaims_write_key_gate() {
		assert_eq!(terminal_waiter(false).await, 0);
	}

	#[tokio::test]
	async fn terminal_waiter_inside_owner_unlock_reclaims_write_key_gate() {
		assert_eq!(
			terminal_waiter(true).await,
			0,
			"idle write-key gate must be reclaimed"
		);
	}

	fn poll_admission<'a>(
		future: std::pin::Pin<&mut impl Future<Output = WriteKeyGate<'a>>>,
	) -> Poll<WriteKeyGate<'a>> {
		future.poll(&mut Context::from_waker(Waker::noop()))
	}

	#[tokio::test]
	async fn cancelled_queued_ticket_preserves_owner_and_reclaims_after_last_release() {
		let gates = DashMap::new();
		let key = [8; 24];
		let owner = WriteKeyGate::admit(&gates, key).await;
		let identity = owner.identity.clone().unwrap();
		let mut cancelled = Box::pin(WriteKeyGate::admit(&gates, key));
		assert!(poll_admission(cancelled.as_mut()).is_pending());
		assert_eq!(gates.get(&key).unwrap().participants, 2);
		drop(cancelled);
		assert_eq!(gates.get(&key).unwrap().participants, 1);
		assert!(Weak::ptr_eq(
			&identity,
			&Arc::downgrade(&gates.get(&key).unwrap().mutex)
		));
		let mut successor = Box::pin(WriteKeyGate::admit(&gates, key));
		assert!(poll_admission(successor.as_mut()).is_pending());
		drop(owner);
		let Poll::Ready(successor_guard) = poll_admission(successor.as_mut()) else {
			panic!("queued successor must acquire after owner release");
		};
		drop(successor);
		assert!(Weak::ptr_eq(
			&identity,
			successor_guard.identity.as_ref().unwrap()
		));
		drop(successor_guard);
		assert!(gates.is_empty());
		assert!(identity.upgrade().is_none());
	}

	#[tokio::test]
	async fn live_waiters_share_one_gate_across_handoff_and_cancellation() {
		let gates = DashMap::new();
		let key = [9; 24];
		let owner = WriteKeyGate::admit(&gates, key).await;
		let identity = owner.identity.clone().unwrap();
		let mut first = Box::pin(WriteKeyGate::admit(&gates, key));
		let mut second = Box::pin(WriteKeyGate::admit(&gates, key));
		assert!(poll_admission(first.as_mut()).is_pending());
		assert!(poll_admission(second.as_mut()).is_pending());
		drop(owner);
		assert_eq!(gates.get(&key).unwrap().participants, 2);
		let Poll::Ready(first_guard) = poll_admission(first.as_mut()) else {
			panic!("first waiter must own handed-off permit");
		};
		drop(first);
		assert!(poll_admission(second.as_mut()).is_pending());
		let mut third = Box::pin(WriteKeyGate::admit(&gates, key));
		assert!(poll_admission(third.as_mut()).is_pending());
		assert_eq!(gates.get(&key).unwrap().participants, 3);
		drop(second);
		assert_eq!(gates.get(&key).unwrap().participants, 2);
		drop(first_guard);
		let Poll::Ready(third_guard) = poll_admission(third.as_mut()) else {
			panic!("live third waiter must acquire after cancelled second");
		};
		drop(third);
		assert!(Weak::ptr_eq(
			&identity,
			third_guard.identity.as_ref().unwrap()
		));
		drop(third_guard);
		assert!(gates.is_empty());
	}

	#[tokio::test]
	async fn assigned_last_waiter_cancellation_reclaims_without_polling_ready() {
		let gates = DashMap::new();
		let key = [14; 24];
		let owner = WriteKeyGate::admit(&gates, key).await;
		let mut waiter = Box::pin(WriteKeyGate::admit(&gates, key));
		assert!(poll_admission(waiter.as_mut()).is_pending());
		drop(owner);
		assert_eq!(gates.get(&key).unwrap().participants, 1);
		// Tokio assigned the permit, but the caller cancels before its next poll.
		drop(waiter);
		assert!(gates.is_empty());
	}

	#[tokio::test]
	async fn assigned_waiter_cancellation_inside_unlock_reclaims_last_ticket() {
		let gates = DashMap::new();
		let key = [10; 24];
		let owner = WriteKeyGate::admit(&gates, key).await;
		let (run_tx, run_rx) = mpsc::sync_channel(1);
		let (finished_tx, finished_rx) = mpsc::sync_channel(1);
		let (ready_tx, ready_rx) = mpsc::sync_channel(1);
		let wake = Arc::new(UnlockWake {
			run: run_tx,
			finished: StdMutex::new(finished_rx),
			wait_inside_unlock: true,
			inside_wake: AtomicBool::new(false),
			calls: AtomicUsize::new(0),
			protocol_failed: AtomicBool::new(false),
		});
		std::thread::scope(|scope| {
			let gates_ref = &gates;
			let wake_ref = &wake;
			let worker = scope.spawn(move || {
				let _finished = Finished(finished_tx);
				let mut admission = Box::pin(WriteKeyGate::admit(gates_ref, key));
				let waker = Waker::from(wake_ref.clone());
				let pending = admission
					.as_mut()
					.poll(&mut Context::from_waker(&waker))
					.is_pending();
				ready_tx.send(pending).unwrap();
				assert!(pending);
				run_rx.recv_timeout(HANDSHAKE_DEADLINE).unwrap();
				assert!(wake_ref.inside_wake.load(Ordering::SeqCst));
				assert_eq!(gates_ref.get(&key).unwrap().participants, 2);
				// Cancel after unlock assigned a permit, without polling Ready.
				// This must retire both the pending mutex acquisition and its ticket.
				drop(admission);
				assert_eq!(gates_ref.get(&key).unwrap().participants, 1);
			});
			let ready = ready_rx.recv_timeout(HANDSHAKE_DEADLINE);
			drop(owner);
			let result = worker.join();
			assert_eq!(ready.unwrap(), true);
			result.unwrap();
		});
		assert!(!wake.protocol_failed.load(Ordering::SeqCst));
		assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
		assert!(gates.is_empty());
	}

	#[tokio::test]
	async fn keep_entry_mutant_retires_ticket_but_fails_strict_reclamation_oracle() {
		let gates = DashMap::new();
		let key = [11; 24];
		drop(WriteKeyGate::admit(&gates, key).await.keep_entry());
		assert_eq!(gates.len(), 1, "mutant must fail strict idle-map emptiness");
		assert_eq!(gates.get(&key).unwrap().participants, 0);
		// A future normal admission can reuse and reclaim a retained idle entry.
		drop(WriteKeyGate::admit(&gates, key).await);
		assert!(gates.is_empty());
	}

	#[tokio::test]
	async fn independent_keys_do_not_wait_for_an_occupied_gate() {
		let gates = DashMap::new();
		let first = WriteKeyGate::admit(&gates, [12; 24]).await;
		let mut other = Box::pin(WriteKeyGate::admit(&gates, [13; 24]));
		let Poll::Ready(other_guard) = poll_admission(other.as_mut()) else {
			panic!("another write key must proceed immediately");
		};
		drop(other);
		drop(other_guard);
		assert_eq!(gates.len(), 1);
		drop(first);
		assert!(gates.is_empty());
	}
}
