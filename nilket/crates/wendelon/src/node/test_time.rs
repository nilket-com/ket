//! DST stage 1 (Codex 01a1e5f8, 01a1e5fd, 01a1e602, 01a1e607): test-only wall time and
//! abort ownership for ONE in-process cluster, carried in each of its nodes' configurations
//! (`NodeBootstrapConfig::test_time`, `cfg(test)` only), so the same clock and the same abort
//! latch reach every lifetime of every node: the first Full start, a ControlOnly
//! replacement's preparation and recovery, and a Full restart. Production builds have no such
//! field: a node reads `SystemClock` and aborts with `ProcessAbort`.
//!
//! Only WALL time is test-controlled. Monotonic time (Raft timers, the maintenance ticker,
//! deadlines, clock-sample age) stays real. Nothing here shortens a duration, an expiry, a
//! TTL or a skew bound, or disables a check.
//!
//! The pass wakes ask an existing owner (the decision maintenance loop, the lease manager) for
//! one more pass of its own work, consumed by that same owner; they never run the work from a
//! second caller, so the owner's cursor, probe and in-flight state stay single-owned.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::consensus::monitor::{AbortHandle, PhysicalClock, SystemClock};

/// One cluster's wall clock: real system time plus a forward-only offset, and never a
/// reading below one it has already returned. Every reading and every advance is serialized,
/// so `advance(d)` (d > 0) returns `W`, strictly greater than every reading returned before
/// it, and every reading after it is at least `W`: a sample whose remote reading is `>= W`
/// was read after the step.
pub(crate) struct TestWallClock {
	state: Mutex<WallState>,
}

struct WallState {
	offset_ms: u64,
	last_ms: u64,
}

impl TestWallClock {
	pub(crate) fn new() -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(WallState {
				offset_ms: 0,
				last_ms: 0,
			}),
		})
	}

	/// Moves wall time forward by `by_ms` (> 0) and returns `W`, the least value every later
	/// reading has.
	pub(crate) fn advance(&self, by_ms: u64) -> u64 {
		assert!(by_ms > 0, "a step moves wall time forward");
		let mut state = self.state.lock().unwrap();
		let current = SystemClock
			.now_ms()
			.saturating_add(state.offset_ms)
			.max(state.last_ms);
		state.offset_ms = state.offset_ms.checked_add(by_ms).expect("offset overflow");
		let step = current.checked_add(by_ms).expect("wall overflow");
		state.last_ms = step;
		step
	}

	/// The total forward offset applied so far.
	pub(crate) fn offset_ms(&self) -> u64 {
		self.state.lock().unwrap().offset_ms
	}
}

impl PhysicalClock for TestWallClock {
	fn now_ms(&self) -> u64 {
		let mut state = self.state.lock().unwrap();
		let reading = SystemClock
			.now_ms()
			.saturating_add(state.offset_ms)
			.max(state.last_ms);
		state.last_ms = reading;
		reading
	}
}

/// One cluster's abort latch. `abort` never returns (`AbortHandle`'s contract): it records
/// the reason, then panics in the calling task instead of aborting the test process. The
/// latch outlives every task and node of the cluster, so a test checks it whatever became of
/// the task that called it, across restarts.
#[derive(Default)]
pub(crate) struct ClusterAbortLatch {
	reasons: Mutex<Vec<String>>,
}

impl ClusterAbortLatch {
	/// Every abort recorded so far, in order.
	pub(crate) fn latched(&self) -> Vec<String> {
		self.reasons.lock().unwrap().clone()
	}
}

impl AbortHandle for ClusterAbortLatch {
	fn abort(&self, reason: &str) -> ! {
		self.reasons.lock().unwrap().push(reason.to_owned());
		panic!("cluster abort latched: {reason}");
	}
}

/// A coalescing request for one more pass of an existing owner's work. A request returns a
/// ticket; the owner reads the requested count when a pass BEGINS and records it as completed
/// when that pass ends, so a pass that began before a request never answers it. Requests made
/// before the owner wakes coalesce into one pass. Requested-but-unrun passes stay counted.
#[derive(Default)]
pub(crate) struct PassWake {
	requested: AtomicU64,
	completed: AtomicU64,
	wake: tokio::sync::Notify,
	done: tokio::sync::Notify,
	/// A one-shot hold the owner reaches right after finishing a pass (`after_pass`), so a
	/// control can make a stop and a wake ready together before the owner's next `select!`.
	hold: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

impl PassWake {
	/// Asks for one more pass; the ticket the owner's completion must reach.
	pub(crate) fn request(&self) -> u64 {
		let ticket = self.requested.fetch_add(1, Ordering::AcqRel) + 1;
		// A stored permit if the owner is not waiting: requests coalesce.
		self.wake.notify_one();
		ticket
	}

	/// The owner: resolves when a pass is requested.
	pub(crate) async fn requested(&self) {
		self.wake.notified().await;
	}

	/// The owner, at the start of a pass: what this pass answers.
	pub(crate) fn begin(&self) -> u64 {
		self.requested.load(Ordering::Acquire)
	}

	/// The owner, at the end of a pass begun with `begun`.
	pub(crate) fn finish(&self, begun: u64) {
		self.completed.fetch_max(begun, Ordering::AcqRel);
		self.done.notify_waiters();
	}

	/// Whether a pass that began at or after `ticket` was requested has completed.
	pub(crate) fn answered(&self, ticket: u64) -> bool {
		self.completed.load(Ordering::Acquire) >= ticket
	}

	/// Waits until `ticket` is answered, observed no later than `by` (real monotonic time).
	/// The bound is enforced where success is OBSERVED (Codex 01a1e616 R1): an answer seen
	/// after `by`, including one already there when the wait begins past `by`, is refused, not
	/// accepted late.
	pub(crate) async fn completed(
		&self,
		ticket: u64,
		by: tokio::time::Instant,
	) -> Result<(), String> {
		loop {
			let done = self.done.notified();
			tokio::pin!(done);
			done.as_mut().enable();
			let answered = self.answered(ticket);
			let observed_at = tokio::time::Instant::now();
			if observed_at > by {
				return Err(format!(
					"pass {ticket} {} only after its bound (completed {})",
					if answered { "answered" } else { "not answered" },
					self.completed.load(Ordering::Acquire)
				));
			}
			if answered {
				return Ok(());
			}
			if tokio::time::timeout_at(by, done).await.is_err() {
				return Err(format!(
					"pass {ticket} not completed by its bound (completed {})",
					self.completed.load(Ordering::Acquire)
				));
			}
		}
	}

	/// Arms a one-shot hold after the owner's next finished pass: `entered` gets a permit when
	/// the owner is held there; it continues when `release` gets one.
	pub(crate) fn hold_after_next_pass(
		&self,
	) -> (Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>) {
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		*self.hold.lock().unwrap() = Some((entered.clone(), release.clone()));
		(entered, release)
	}

	/// The owner, after `finish`: waits at an armed hold, once.
	pub(crate) async fn after_pass(&self) {
		let hold = self.hold.lock().unwrap().take();
		if let Some((entered, release)) = hold {
			entered.add_permits(1);
			let _ = release.acquire().await;
		}
	}

	/// Requested passes that have not completed.
	pub(crate) fn outstanding(&self) -> u64 {
		self.requested
			.load(Ordering::Acquire)
			.saturating_sub(self.completed.load(Ordering::Acquire))
	}
}

/// One node's wakes, by node id: a restart of the node finds the same ones.
#[derive(Default)]
pub(crate) struct NodeWakes {
	pub(crate) maintenance: Arc<PassWake>,
	pub(crate) leases: Arc<PassWake>,
}

/// One in-process cluster's time: the shared wall clock, the abort latch and every node's
/// wakes. Cloned into each node's configuration; clones share everything.
#[derive(Clone)]
pub(crate) struct TestTime {
	pub(crate) clock: Arc<TestWallClock>,
	pub(crate) abort: Arc<ClusterAbortLatch>,
	wakes: Arc<Mutex<HashMap<u32, Arc<NodeWakes>>>>,
}

impl TestTime {
	pub(crate) fn new() -> Self {
		Self {
			clock: TestWallClock::new(),
			abort: Arc::new(ClusterAbortLatch::default()),
			wakes: Arc::default(),
		}
	}

	/// Node `node`'s wakes, created once and found again by every later lifetime.
	pub(crate) fn wakes(&self, node: u32) -> Arc<NodeWakes> {
		self.wakes.lock().unwrap().entry(node).or_default().clone()
	}

	/// A client clock on this cluster's wall time (the UUIDv7 a client mints), with real
	/// monotonic time (its incarnation and attempt ages).
	pub(crate) fn client_clock(&self) -> Arc<dyn crate::client::core::ClientClock> {
		Arc::new(TestClientClock(self.clock.clone()))
	}
}

struct TestClientClock(Arc<TestWallClock>);

impl crate::client::core::ClientClock for TestClientClock {
	fn now(&self) -> std::time::Instant {
		std::time::Instant::now()
	}

	fn unix_millis(&self) -> u64 {
		self.0.now_ms()
	}
}

impl std::fmt::Debug for TestTime {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("TestTime")
			.field("offset_ms", &self.clock.offset_ms())
			.finish_non_exhaustive()
	}
}

/// One cluster's time is one object: configurations compare equal when they share it.
impl PartialEq for TestTime {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.clock, &other.clock)
	}
}

impl Eq for TestTime {}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::monitor::{NodeTracker, check_drift_once};
	use crate::schema::NodeId;
	use std::time::Duration;

	#[test]
	fn an_advance_is_strictly_after_every_earlier_reading_and_never_read_below() {
		let clock = TestWallClock::new();
		let before: Vec<u64> = (0..100).map(|_| clock.now_ms()).collect();
		assert!(
			before.windows(2).all(|pair| pair[0] <= pair[1]),
			"never decreasing"
		);
		let step = clock.advance(3_960_000);
		assert!(
			before.iter().all(|reading| *reading < step),
			"W is beyond every earlier reading"
		);
		let after: Vec<u64> = (0..100).map(|_| clock.now_ms()).collect();
		assert!(
			after.iter().all(|reading| *reading >= step),
			"every later reading is >= W"
		);
		assert!(after.windows(2).all(|pair| pair[0] <= pair[1]));
		assert_eq!(clock.offset_ms(), 3_960_000);
		// One cluster, one clock: clones share it, another cluster's does not.
		let time = TestTime::new();
		assert_eq!(time.clone(), time);
		assert_ne!(TestTime::new(), time);
	}

	#[tokio::test]
	async fn elapsed_deadlines_stay_real_when_wall_time_jumps() {
		let clock = TestWallClock::new();
		let started = tokio::time::Instant::now();
		clock.advance(3_960_000);
		tokio::time::sleep(Duration::from_millis(200)).await;
		let elapsed = started.elapsed();
		assert!(
			elapsed >= Duration::from_millis(200) && elapsed < Duration::from_secs(5),
			"monotonic time is untouched by a wall step: {elapsed:?}"
		);
	}

	/// An intentional drift condition, checked by the real drift check with the cluster's
	/// latch: the calling task ends in a panic, the latch holds the reason, the process lives.
	#[tokio::test]
	async fn an_intentional_drift_latches_a_failing_abort_without_killing_the_process() {
		let time = TestTime::new();
		let clock: Arc<dyn PhysicalClock> = time.clock.clone();
		let tracker = Arc::new(NodeTracker::new(
			NodeId {
				id: 1,
				_reserved: 0,
			},
			clock.clone(),
		));
		let now = clock.now_ms();
		for peer in [2, 3] {
			tracker.record(
				NodeId {
					id: peer,
					_reserved: 0,
				},
				now.saturating_sub(10_000),
			);
		}
		let abort = time.abort.clone();
		let task = tokio::spawn(async move {
			check_drift_once(&tracker, clock.as_ref(), abort.as_ref(), 500);
		});
		let ended = task.await.expect_err("the abort never returns");
		assert!(ended.is_panic(), "{ended:?}");
		let latched = time.abort.latched();
		assert_eq!(latched.len(), 1, "{latched:?}");
		assert!(latched[0].contains("clock drift"), "{latched:?}");
	}

	#[tokio::test]
	async fn a_wake_is_answered_only_by_a_pass_begun_after_it_and_requests_coalesce() {
		let wake = Arc::new(PassWake::default());
		// A pass already begun answers nothing requested after it.
		let begun_early = wake.begin();
		let first = wake.request();
		wake.finish(begun_early);
		assert!(
			!wake.answered(first),
			"an earlier pass is not this request's"
		);
		assert_eq!(wake.outstanding(), 1);
		// Two requests before the owner wakes: one pass answers both.
		let second = wake.request();
		let owner = {
			let wake = wake.clone();
			tokio::spawn(async move {
				wake.requested().await;
				let begun = wake.begin();
				wake.finish(begun);
			})
		};
		let by = tokio::time::Instant::now() + Duration::from_secs(5);
		wake.completed(second, by).await.unwrap();
		owner.await.unwrap();
		assert!(wake.answered(first) && wake.answered(second));
		assert_eq!(wake.outstanding(), 0);
		// An unanswered request is reported at its bound, not waited out.
		let third = wake.request();
		let short = tokio::time::Instant::now() + Duration::from_millis(50);
		assert!(wake.completed(third, short).await.is_err());
		assert_eq!(
			wake.outstanding(),
			1,
			"a requested, unrun pass stays counted"
		);
	}

	/// One cluster's clock reaches a node's every lifetime through its configuration: a real
	/// single node bootstrapped on it reads it (its HLC follows a wall step), and a restart from
	/// the same configuration reads the same clock; the routing the ControlOnly preparation and
	/// recovery use (`node_clock` of the same configuration) is the same clock; a client clock
	/// made from it mints at the cluster's wall time while its monotonic time stays real.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_clusters_clock_reaches_every_lifetime_of_a_node_and_its_client() {
		use crate::node::{NodeBootstrapConfig, WendelonNode, node_clock};
		let time = TestTime::new();
		let dir = crate::test_support::TestDir::new("dst-test-time-lifetimes");
		let cfg = NodeBootstrapConfig {
			test_time: Some(time.clone()),
			..NodeBootstrapConfig::dev(91, dir.to_path_buf(), "127.0.0.1:0".into())
		};
		let same = |clock: &Arc<dyn PhysicalClock>| {
			std::ptr::eq(
				Arc::as_ptr(clock) as *const u8,
				Arc::as_ptr(&time.clock) as *const u8,
			)
		};
		assert!(
			same(&node_clock(&cfg)),
			"the routing ControlOnly reads is the cluster's clock"
		);
		for lifetime in ["first start", "restart"] {
			let node = WendelonNode::bootstrap_single_node(cfg.clone())
				.await
				.unwrap();
			assert!(
				same(&node.clock),
				"{lifetime}: the node reads the cluster's clock"
			);
			let step = time.clock.advance(60_000);
			assert!(
				node.hlc.now().physical_ms >= step,
				"{lifetime}: the node's HLC follows the cluster's wall step"
			);
			node.shutdown().await.unwrap();
		}
		let client = time.client_clock();
		let step = time.clock.advance(1_000);
		assert!(
			client.unix_millis() >= step,
			"the client mints at the cluster's wall time"
		);
		let started = client.now();
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert!(client.now().duration_since(started) >= Duration::from_millis(50));
		assert!(time.abort.latched().is_empty(), "no abort latched");
	}

	/// Codex 01a1e616 R1: an answered ticket observed after its bound is refused; the same
	/// answer observed within its bound is accepted.
	#[tokio::test]
	async fn an_answer_observed_after_its_bound_is_refused_and_on_time_is_accepted() {
		let wake = PassWake::default();
		let ticket = wake.request();
		let begun = wake.begin();
		wake.finish(begun);
		assert!(wake.answered(ticket), "premise: the ticket is answered");
		let expired = tokio::time::Instant::now() - Duration::from_millis(1);
		let late = wake.completed(ticket, expired).await;
		assert!(
			late.is_err(),
			"an answer observed after its bound was accepted: {late:?}"
		);
		let on_time = tokio::time::Instant::now() + Duration::from_secs(5);
		assert_eq!(wake.completed(ticket, on_time).await, Ok(()));
	}

	#[test]
	fn a_nodes_wakes_are_found_again_by_its_later_lifetimes() {
		let time = TestTime::new();
		let first = time.wakes(3);
		let again = time.clone().wakes(3);
		assert!(Arc::ptr_eq(&first, &again));
		assert!(!Arc::ptr_eq(&first, &time.wakes(2)));
	}
}
