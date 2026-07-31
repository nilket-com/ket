//! Liveness + clock-drift monitoring (ADR 001, ADR 012).
//!
//! Two background watchdogs share one [`NodeTracker`]:
//!
//! 1. **Clock Drift Monitor (all nodes)** — periodically computes
//!    the median cluster physical time from peer heartbeats. If the
//!    local clock deviates from the median by more than
//!    [`MAX_OFFSET_MS`], the node invokes the configured
//!    [`AbortHandle`] — in production a wrapper around
//!    `std::process::abort()`. A healthy cluster therefore refuses
//!    to let any node with a drifted clock poison its HLCs (see
//!    ADR 012).
//!
//! 2. **Autonomous Eviction Watchdog (leader only)** — sweeps the
//!    tracker every [`EVICTION_CHECK_INTERVAL`]; any peer whose
//!    `local_receipt_ms` is older than [`EVICTION_THRESHOLD`] is
//!    removed from a freshly-derived [`MembershipEpoch`] and the new
//!    epoch is handed to the injected [`MembershipAdvancer`]
//!    (wired to `ControlRing::advance_epoch` in production).
//!    The sweep also applies the **Max Eviction Quota** guardrail:
//!    we refuse to commit an eviction that would drop the cluster
//!    below `(rf / 2) + 1`, the Raft quorum floor (ADR 001).
//!
//! Every hook on the hot path — the physical clock, the abort
//! mechanism, the leader status, the membership advancer — is a
//! trait object so unit tests can drive the full state machine
//! without a real process, real Raft, or real transport.

use crate::governance::{AutonomousActionSurface, authorize_autonomous_action};
use crate::metrics::global as metrics;
use crate::network::transport::MembershipEpoch;
use crate::schema::{HybridTimestamp, NodeId};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

// ============================================================
//  CONSTANTS — knobs borrowed straight from ADR 001 / ADR 012.
// ============================================================

/// ADR 012. Maximum tolerated deviation between the local physical
/// clock and the median cluster time. A node outside this band
/// aborts rather than continue poisoning HLCs.
pub const MAX_OFFSET_MS: u64 = 500;

/// ADR 001. Silence window past which the leader evicts a peer.
/// Five minutes is conservative — long enough to ride out a reboot
/// or TCP stall, short enough that routing heals before most SLOs
/// are exhausted.
pub const EVICTION_THRESHOLD: Duration = Duration::from_secs(300);

/// Cadence at which the drift monitor wakes. One second is a
/// compromise: fast enough that a bad clock rarely gets a chance
/// to serve incorrect data, slow enough that the median calculation
/// is not a measurable CPU cost.
pub const DRIFT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Cadence at which the leader evaluates eviction. Much coarser
/// than the drift check — network blips should be smoothed out by
/// the tracker's continually-updated `local_receipt_ms` long before
/// the leader looks at them.
pub const EVICTION_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Minimum number of peer samples required before the drift monitor
/// will trust its median. Avoids a single stale heartbeat from a
/// single peer triggering an abort during bootstrap.
pub const DRIFT_MIN_SAMPLES: usize = 2;

// ============================================================
//  INJECTABLE HOOKS
// ============================================================

/// Source of the local host's wall-clock time in milliseconds since
/// the UNIX epoch. A trait so tests can slam the clock forward or
/// backward deterministically; production wires to [`SystemClock`].
pub trait PhysicalClock: Send + Sync {
	fn now_ms(&self) -> u64;
}

/// Real wall-clock. `SystemTime::now()` can step backward across
/// NTP corrections — we tolerate that here because the drift
/// monitor's job is *specifically* to notice when that happens.
pub struct SystemClock;

impl PhysicalClock for SystemClock {
	fn now_ms(&self) -> u64 {
		SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map(|d| d.as_millis() as u64)
			.unwrap_or(0)
	}
}

/// Terminates the process when the drift monitor concludes that the
/// local clock is beyond saving. Production uses [`ProcessAbort`];
/// tests use a recording fake so the suite does not, in fact, abort.
pub trait AbortHandle: Send + Sync {
	fn abort(&self, reason: &str) -> !;
}

/// Real abort — calls [`std::process::abort`]. Skipping unwind lets
/// destructors with undefined behavior under drift (e.g. log flush
/// that stamps HLCs) be safely bypassed.
pub struct ProcessAbort;

impl AbortHandle for ProcessAbort {
	fn abort(&self, reason: &str) -> ! {
		eprintln!("wendelon: autonomous abort — {reason}");
		std::process::abort();
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockDriftEvent {
	pub local_node: NodeId,
	pub local_physical_ms: u64,
	pub cluster_median_ms: u64,
	pub offset_ms: u64,
	pub max_offset_ms: u64,
}

pub trait ClockDriftSubscriber: Send + Sync {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent);
}

pub struct NoopClockDriftSubscriber;

impl ClockDriftSubscriber for NoopClockDriftSubscriber {
	fn on_drift_exceeded(&self, _event: &ClockDriftEvent) {}
}

pub struct FanoutClockDriftSubscriber {
	subscribers: Vec<Arc<dyn ClockDriftSubscriber>>,
}

impl FanoutClockDriftSubscriber {
	pub fn new(subscribers: Vec<Arc<dyn ClockDriftSubscriber>>) -> Self {
		Self { subscribers }
	}
}

impl ClockDriftSubscriber for FanoutClockDriftSubscriber {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent) {
		for subscriber in &self.subscribers {
			subscriber.on_drift_exceeded(event);
		}
	}
}

pub struct WatchClockDriftSubscriber {
	tx: watch::Sender<Option<ClockDriftEvent>>,
}

impl WatchClockDriftSubscriber {
	pub fn new(tx: watch::Sender<Option<ClockDriftEvent>>) -> Self {
		Self { tx }
	}
}

impl ClockDriftSubscriber for WatchClockDriftSubscriber {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent) {
		let _ = self.tx.send_replace(Some(event.clone()));
	}
}

/// Signals whether the local node is currently the Raft leader. The
/// eviction watchdog uses this to cheaply skip the expensive
/// membership sweep on followers.
#[async_trait]
pub trait LeaderStatus: Send + Sync {
	async fn is_leader(&self) -> bool;
}

/// Proposes a new [`MembershipEpoch`]. Production wires this to
/// `ControlRing::advance_epoch`; tests record calls to verify the
/// watchdog's eviction decisions.
#[async_trait]
pub trait MembershipAdvancer: Send + Sync {
	async fn advance_epoch(&self, next: MembershipEpoch) -> std::io::Result<()>;
}

// ============================================================
//  NODE TRACKER
// ============================================================

/// Last-seen record for a single peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
	/// Peer's reported physical time at the moment the message was
	/// minted (milliseconds since UNIX epoch).
	pub remote_physical_ms: u64,
	/// Our local physical time at the moment the message was
	/// received. Drives both the median calculation (so we account
	/// for in-flight latency) and the eviction watchdog.
	pub local_receipt_ms: u64,
}

/// Concurrent map of `NodeId -> Heartbeat` plus the derived
/// summaries the watchdogs consume.
///
/// The tracker itself is intentionally simple — it does not know
/// about quorum, thresholds, or the local node's role. Every piece
/// of policy lives in the watchdog tasks above it.
pub struct NodeTracker {
	local_node: NodeId,
	clock: Arc<dyn PhysicalClock>,
	seen: DashMap<NodeId, Heartbeat>,
}

impl NodeTracker {
	pub fn new(local_node: NodeId, clock: Arc<dyn PhysicalClock>) -> Self {
		Self {
			local_node,
			clock,
			seen: DashMap::new(),
		}
	}

	pub fn local_node(&self) -> NodeId {
		self.local_node
	}

	/// Record a peer heartbeat given its raw remote physical time.
	/// Self-pings are ignored — the tracker only compares against
	/// *other* nodes' clocks.
	pub fn record(&self, peer: NodeId, remote_physical_ms: u64) {
		if peer.id == self.local_node.id {
			return;
		}
		let now = self.clock.now_ms();
		self.seen.insert(
			peer,
			Heartbeat {
				remote_physical_ms,
				local_receipt_ms: now,
			},
		);
		metrics().liveness_tracked_peers.set(self.seen.len() as u64);
	}

	/// Convenience: extract the physical component of an [`HLC`] and
	/// record it. The data-ring carries HLCs on every RPC, so this
	/// is the common ingestion path.
	///
	/// [`HLC`]: crate::schema::HybridTimestamp
	pub fn record_hlc(&self, peer: NodeId, hlc: HybridTimestamp) {
		self.record(peer, hlc.physical_ms);
	}

	/// Latest heartbeat for `peer`, if any.
	pub fn last_seen(&self, peer: NodeId) -> Option<Heartbeat> {
		self.seen.get(&peer).map(|e| *e.value())
	}

	/// Snapshot of every tracked peer. Order is unspecified — the
	/// eviction watchdog sorts by `NodeId` before proposing so the
	/// resulting epoch is deterministic across replicas.
	pub fn snapshot(&self) -> Vec<(NodeId, Heartbeat)> {
		self.seen.iter().map(|e| (*e.key(), *e.value())).collect()
	}

	pub fn len(&self) -> usize {
		self.seen.len()
	}

	pub fn is_empty(&self) -> bool {
		self.seen.is_empty()
	}

	/// Remove a peer from the tracker — used after an eviction is
	/// committed so the same peer does not trigger the watchdog
	/// again next cycle before its epoch catches up.
	pub fn forget(&self, peer: NodeId) {
		self.seen.remove(&peer);
		metrics().liveness_tracked_peers.set(self.seen.len() as u64);
	}

	/// Peers whose last receipt is older than `threshold` relative
	/// to `now_ms`. Used by the eviction watchdog to decide which
	/// nodes to propose for removal.
	pub fn stale_peers(&self, threshold: Duration, now_ms: u64) -> Vec<NodeId> {
		let cutoff = now_ms.saturating_sub(threshold.as_millis() as u64);
		let mut out: Vec<NodeId> = self
			.seen
			.iter()
			.filter_map(|e| {
				if e.value().local_receipt_ms < cutoff {
					Some(*e.key())
				} else {
					None
				}
			})
			.collect();
		out.sort_by_key(|n| n.id);
		out
	}

	/// Median estimate of the cluster's physical time at `now_ms`.
	///
	/// Each peer's contribution is
	/// `remote_physical_ms + (now_ms - local_receipt_ms)`, i.e. the
	/// peer's clock projected forward by however long it has been
	/// since we last heard from it. Returns `None` when fewer than
	/// [`DRIFT_MIN_SAMPLES`] peers are tracked — a bootstrapping
	/// cluster has no cluster time to compare against yet.
	pub fn median_cluster_ms(&self, now_ms: u64) -> Option<u64> {
		let mut samples: Vec<u64> = self
			.seen
			.iter()
			.map(|e| {
				let h = e.value();
				let delta = now_ms.saturating_sub(h.local_receipt_ms);
				h.remote_physical_ms.saturating_add(delta)
			})
			.collect();
		if samples.len() < DRIFT_MIN_SAMPLES {
			return None;
		}
		samples.sort_unstable();
		Some(median_sorted(&samples))
	}
}

fn median_sorted(sorted: &[u64]) -> u64 {
	let n = sorted.len();
	if n % 2 == 1 {
		sorted[n / 2]
	} else {
		// Average of the two middle elements, computed without
		// overflow (both samples are `u64` milliseconds).
		let a = sorted[n / 2 - 1];
		let b = sorted[n / 2];
		a / 2 + b / 2 + ((a & 1) + (b & 1)) / 2
	}
}

// ============================================================
//  DRIFT MONITOR
// ============================================================

/// Evaluate a single drift sample; abort when the deviation exceeds
/// [`MAX_OFFSET_MS`]. Broken out from the background loop so unit
/// tests can exercise the exact decision without scheduling.
pub fn check_drift_once(
	tracker: &NodeTracker,
	clock: &dyn PhysicalClock,
	abort: &dyn AbortHandle,
	max_offset_ms: u64,
) -> DriftCheck {
	check_drift_once_with_subscriber(
		tracker,
		clock,
		abort,
		&NoopClockDriftSubscriber,
		max_offset_ms,
	)
}

pub fn check_drift_once_with_subscriber(
	tracker: &NodeTracker,
	clock: &dyn PhysicalClock,
	abort: &dyn AbortHandle,
	subscriber: &dyn ClockDriftSubscriber,
	max_offset_ms: u64,
) -> DriftCheck {
	metrics().clock_drift_checks_total.inc();
	let now = clock.now_ms();
	let Some(median) = tracker.median_cluster_ms(now) else {
		metrics().clock_drift_insufficient_samples_total.inc();
		return DriftCheck::InsufficientSamples;
	};
	let offset = if now >= median {
		now - median
	} else {
		median - now
	};
	metrics().clock_drift_offset_ms.set(offset);
	if offset > max_offset_ms {
		let event = ClockDriftEvent {
			local_node: tracker.local_node(),
			local_physical_ms: now,
			cluster_median_ms: median,
			offset_ms: offset,
			max_offset_ms,
		};
		metrics().clock_drift_exceeded_total.inc();
		subscriber.on_drift_exceeded(&event);
		let _permit = authorize_autonomous_action(AutonomousActionSurface::ClockDriftAbort);
		abort.abort(&format!(
			"clock drift {offset}ms > {max_offset_ms}ms (node={}, local={now}, median={median})",
			tracker.local_node().id,
		));
	}
	DriftCheck::Within { offset_ms: offset }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftCheck {
	/// Not enough peers yet to trust a median.
	InsufficientSamples,
	/// Deviation within the allowed band.
	Within { offset_ms: u64 },
}

/// Spawn the all-nodes drift monitor. Terminates when the process
/// exits; the returned [`tokio::task::JoinHandle`] is held by the
/// bootstrap owner for graceful-shutdown observability.
pub fn spawn_drift_monitor(
	tracker: Arc<NodeTracker>,
	clock: Arc<dyn PhysicalClock>,
	abort: Arc<dyn AbortHandle>,
	interval: Duration,
) -> tokio::task::JoinHandle<()> {
	spawn_drift_monitor_with_bound(tracker, clock, abort, interval, MAX_OFFSET_MS)
}

pub fn spawn_drift_monitor_with_bound(
	tracker: Arc<NodeTracker>,
	clock: Arc<dyn PhysicalClock>,
	abort: Arc<dyn AbortHandle>,
	interval: Duration,
	max_offset_ms: u64,
) -> tokio::task::JoinHandle<()> {
	spawn_drift_monitor_with_subscriber(
		tracker,
		clock,
		abort,
		Arc::new(NoopClockDriftSubscriber),
		interval,
		max_offset_ms,
	)
}

pub fn spawn_drift_monitor_with_subscriber(
	tracker: Arc<NodeTracker>,
	clock: Arc<dyn PhysicalClock>,
	abort: Arc<dyn AbortHandle>,
	subscriber: Arc<dyn ClockDriftSubscriber>,
	interval: Duration,
	max_offset_ms: u64,
) -> tokio::task::JoinHandle<()> {
	tokio::spawn(async move {
		let mut ticker = tokio::time::interval(interval);
		// `Skip` means a long pause does not snowball into a burst
		// of back-to-back evaluations on wake.
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			ticker.tick().await;
			let _ = check_drift_once_with_subscriber(
				&tracker,
				&*clock,
				&*abort,
				&*subscriber,
				max_offset_ms,
			);
		}
	})
}

// ============================================================
//  EVICTION WATCHDOG
// ============================================================

/// Quorum floor: the leader refuses to evict so hard that the
/// resulting cluster would sit below this count. Expressed as a
/// free function so tests can pin the guardrail arithmetic.
pub fn quorum_floor(rf: usize) -> usize {
	(rf / 2) + 1
}

/// Outcome of a single eviction sweep. Exposed so the production
/// loop and the tests share the exact same decision path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictionDecision {
	/// Not the leader this cycle — nothing to do.
	NotLeader,
	/// Every peer is healthy.
	AllHealthy,
	/// Would have evicted, but the quota would have left the
	/// cluster below quorum — refusing to proceed, emitting the
	/// list for observability.
	RefusedForQuorum { candidates: Vec<NodeId> },
	/// New epoch was proposed, dropping the named peers.
	Evicted { dropped: Vec<NodeId>, epoch: u64 },
	/// The advancer returned an error (Raft not yet leader, stream
	/// closed, …). Recorded so the supervising task can retry.
	AdvancerFailed { dropped: Vec<NodeId>, error: String },
}

/// Run one eviction sweep. Separated from the spawned task so tests
/// can drive it with a mock clock + mock advancer.
///
/// The caller supplies the current [`MembershipEpoch`] so this
/// function can derive the new one. The production wiring reads it
/// out of the same `watch::Receiver` the ring router subscribes to.
pub async fn sweep_evictions_once(
	tracker: &NodeTracker,
	clock: &dyn PhysicalClock,
	leader: &dyn LeaderStatus,
	advancer: &dyn MembershipAdvancer,
	current: &MembershipEpoch,
	rf: usize,
	threshold: Duration,
) -> EvictionDecision {
	metrics().eviction_sweeps_total.inc();
	if !leader.is_leader().await {
		return EvictionDecision::NotLeader;
	}
	let now = clock.now_ms();
	let stale = tracker.stale_peers(threshold, now);
	metrics().liveness_stale_peers.set(stale.len() as u64);
	if stale.is_empty() {
		return EvictionDecision::AllHealthy;
	}

	// Derive survivors: peers present in the current epoch and not
	// in the stale set. The leader itself is always a survivor (we
	// refuse to evict ourselves — if our clock drifts we crash via
	// the drift monitor instead).
	let stale_ids: std::collections::HashSet<u32> = stale.iter().map(|n| n.id).collect();
	let mut survivors: std::collections::HashMap<NodeId, _> = current
		.peers
		.iter()
		.filter(|(id, _)| !stale_ids.contains(&id.id) || id.id == tracker.local_node().id)
		.map(|(k, v)| (*k, v.clone()))
		.collect();
	// Always keep the local node — we never evict ourselves.
	if let Some(caps) = current.peers.get(&tracker.local_node()) {
		survivors.insert(tracker.local_node(), caps.clone());
	}

	// Quorum floor guardrail: if the eviction would drop the cluster
	// below (rf / 2) + 1, refuse and surface the candidates for
	// observability.
	let floor = quorum_floor(rf);
	if survivors.len() < floor {
		metrics().eviction_refused_quorum_total.inc();
		return EvictionDecision::RefusedForQuorum { candidates: stale };
	}

	// The dropped set is every candidate that actually disappears
	// from the survivors map — filters out peers the current epoch
	// never knew about, so noisy trackers cannot fake an eviction.
	let dropped: Vec<NodeId> = stale
		.iter()
		.filter(|id| !survivors.contains_key(id))
		.copied()
		.collect();
	if dropped.is_empty() {
		return EvictionDecision::AllHealthy;
	}

	let next_epoch = current.epoch + 1;
	let next = MembershipEpoch {
		epoch: next_epoch,
		peers: survivors,
	};
	let _permit = authorize_autonomous_action(AutonomousActionSurface::ControlRingPeerEviction);
	match advancer.advance_epoch(next).await {
		Ok(()) => {
			for id in &dropped {
				tracker.forget(*id);
			}
			metrics().evictions_total.add(dropped.len() as u64);
			EvictionDecision::Evicted {
				dropped,
				epoch: next_epoch,
			}
		}
		Err(e) => {
			metrics().eviction_advancer_failures_total.inc();
			EvictionDecision::AdvancerFailed {
				dropped,
				error: e.to_string(),
			}
		}
	}
}

/// Spawn the leader-only eviction watchdog. The task loops forever
/// on `interval`, reading the current epoch out of `epoch_rx` each
/// time — so a mid-flight membership change from another source
/// (e.g. an admin-initiated `advance_epoch`) is picked up without
/// extra plumbing.
pub fn spawn_eviction_watchdog(
	tracker: Arc<NodeTracker>,
	clock: Arc<dyn PhysicalClock>,
	leader: Arc<dyn LeaderStatus>,
	advancer: Arc<dyn MembershipAdvancer>,
	epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
	rf: usize,
	interval: Duration,
) -> tokio::task::JoinHandle<()> {
	tokio::spawn(async move {
		let mut ticker = tokio::time::interval(interval);
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			ticker.tick().await;
			let current = epoch_rx.borrow().clone();
			let _ = sweep_evictions_once(
				&tracker,
				&*clock,
				&*leader,
				&*advancer,
				&current,
				rf,
				EVICTION_THRESHOLD,
			)
			.await;
		}
	})
}

// ============================================================
//  TEST DOUBLES
// ============================================================

/// Fixed-value physical clock. `set` is cheap so tests can step
/// time deterministically without sleeping.
#[cfg(test)]
pub struct FakeClock(Mutex<u64>);

#[cfg(test)]
impl FakeClock {
	pub fn new(initial_ms: u64) -> Self {
		Self(Mutex::new(initial_ms))
	}
	pub fn set(&self, ms: u64) {
		*self.0.lock().unwrap() = ms;
	}
	pub fn advance(&self, by_ms: u64) {
		let mut g = self.0.lock().unwrap();
		*g = g.saturating_add(by_ms);
	}
}

#[cfg(test)]
impl PhysicalClock for FakeClock {
	fn now_ms(&self) -> u64 {
		*self.0.lock().unwrap()
	}
}

// Always-available recording abort. Doesn't actually abort — stores
// the reason so tests can assert the drift monitor tripped and why.
pub struct RecordingAbort {
	reasons: Mutex<Vec<String>>,
}

impl RecordingAbort {
	pub fn new() -> Self {
		Self {
			reasons: Mutex::new(Vec::new()),
		}
	}
	pub fn reasons(&self) -> Vec<String> {
		self.reasons.lock().unwrap().clone()
	}
}

impl Default for RecordingAbort {
	fn default() -> Self {
		Self::new()
	}
}

impl AbortHandle for RecordingAbort {
	fn abort(&self, reason: &str) -> ! {
		self.reasons.lock().unwrap().push(reason.to_string());
		// Tests need a non-returning path without actually aborting;
		// panicking satisfies `-> !` and surfaces clearly if a test
		// forgets to guard against the call.
		panic!("RecordingAbort tripped: {reason}");
	}
}

#[cfg(test)]
pub struct RecordingDriftSubscriber {
	events: Mutex<Vec<ClockDriftEvent>>,
}

#[cfg(test)]
impl RecordingDriftSubscriber {
	pub fn new() -> Self {
		Self {
			events: Mutex::new(Vec::new()),
		}
	}

	pub fn events(&self) -> Vec<ClockDriftEvent> {
		self.events.lock().unwrap().clone()
	}
}

#[cfg(test)]
impl Default for RecordingDriftSubscriber {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
impl ClockDriftSubscriber for RecordingDriftSubscriber {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent) {
		self.events.lock().unwrap().push(event.clone());
	}
}

/// Recording leader hook. Flip with `set_leader`.
#[cfg(test)]
pub struct FakeLeader(Mutex<bool>);

#[cfg(test)]
impl FakeLeader {
	pub fn new(is_leader: bool) -> Self {
		Self(Mutex::new(is_leader))
	}
	pub fn set_leader(&self, is: bool) {
		*self.0.lock().unwrap() = is;
	}
}

#[cfg(test)]
#[async_trait]
impl LeaderStatus for FakeLeader {
	async fn is_leader(&self) -> bool {
		*self.0.lock().unwrap()
	}
}

/// Recording membership advancer — stashes every epoch the watchdog
/// tries to commit so tests can assert the exact content.
#[cfg(test)]
pub struct RecordingAdvancer {
	calls: Mutex<Vec<MembershipEpoch>>,
	fail_next: Mutex<bool>,
}

#[cfg(test)]
impl RecordingAdvancer {
	pub fn new() -> Self {
		Self {
			calls: Mutex::new(Vec::new()),
			fail_next: Mutex::new(false),
		}
	}
	pub fn fail_next(&self) {
		*self.fail_next.lock().unwrap() = true;
	}
	pub fn calls(&self) -> Vec<MembershipEpoch> {
		self.calls.lock().unwrap().clone()
	}
}

#[cfg(test)]
#[async_trait]
impl MembershipAdvancer for RecordingAdvancer {
	async fn advance_epoch(&self, next: MembershipEpoch) -> std::io::Result<()> {
		let mut fail = self.fail_next.lock().unwrap();
		if *fail {
			*fail = false;
			return Err(std::io::Error::new(
				std::io::ErrorKind::Other,
				"advancer forced failure",
			));
		}
		self.calls.lock().unwrap().push(next);
		Ok(())
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::transport::{
		MembershipEpoch, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities,
		TransportTierSet,
	};
	use std::collections::HashMap;
	use std::panic::AssertUnwindSafe;

	fn nid(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn caps() -> PeerCapabilities {
		PeerCapabilities {
			address: "127.0.0.1:0".into(),
			tiers: TransportTierSet::empty(),
			roles: NodeRoleSet::empty().with(NodeRole::Data),
			capabilities: NodeCapabilitySet::empty(),
		}
	}

	fn epoch_of(epoch: u64, ids: &[u32]) -> MembershipEpoch {
		let mut peers = HashMap::new();
		for id in ids {
			peers.insert(nid(*id), caps());
		}
		MembershipEpoch { epoch, peers }
	}

	// -------------- tracker -------------

	#[test]
	fn record_skips_self_pings() {
		let clock = Arc::new(FakeClock::new(1_000));
		let tracker = NodeTracker::new(nid(7), clock);
		tracker.record(nid(7), 999);
		assert!(tracker.last_seen(nid(7)).is_none());
	}

	#[test]
	fn record_stamps_local_receipt() {
		let clock = Arc::new(FakeClock::new(1_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 900);
		let hb = tracker.last_seen(nid(2)).unwrap();
		assert_eq!(hb.remote_physical_ms, 900);
		assert_eq!(hb.local_receipt_ms, 1_000);
		clock.set(2_500);
		tracker.record(nid(2), 2_400);
		let hb = tracker.last_seen(nid(2)).unwrap();
		assert_eq!(hb.local_receipt_ms, 2_500);
	}

	#[test]
	fn record_hlc_unpacks_physical_ms() {
		let clock = Arc::new(FakeClock::new(100));
		let tracker = NodeTracker::new(nid(1), clock);
		tracker.record_hlc(
			nid(2),
			HybridTimestamp {
				physical_ms: 55,
				logical: 9,
				node_id: 2,
			},
		);
		assert_eq!(tracker.last_seen(nid(2)).unwrap().remote_physical_ms, 55);
	}

	#[test]
	fn stale_peers_honors_threshold() {
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 9_990); // receipt at 10_000
		clock.set(20_000);
		tracker.record(nid(3), 19_990); // receipt at 20_000
		// threshold = 5s → 2 is stale (receipt 10s old), 3 is fresh
		clock.set(20_500);
		let stale = tracker.stale_peers(Duration::from_secs(5), 20_500);
		assert_eq!(stale, vec![nid(2)]);
	}

	// -------------- median -------------

	#[test]
	fn median_sorted_odd_and_even() {
		assert_eq!(median_sorted(&[5]), 5);
		assert_eq!(median_sorted(&[1, 2, 3]), 2);
		assert_eq!(median_sorted(&[1, 2, 3, 4]), 2); // (2+3)/2 = 2 (int)
		assert_eq!(median_sorted(&[2, 4]), 3);
		assert_eq!(median_sorted(&[u64::MAX, u64::MAX]), u64::MAX);
	}

	#[test]
	fn median_needs_min_samples() {
		let clock = Arc::new(FakeClock::new(100));
		let tracker = NodeTracker::new(nid(1), clock);
		tracker.record(nid(2), 100);
		assert!(tracker.median_cluster_ms(100).is_none());
	}

	#[test]
	fn median_projects_heartbeats_forward() {
		let clock = Arc::new(FakeClock::new(1_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		// Two peers reporting ~1_000 ms.
		tracker.record(nid(2), 1_000);
		tracker.record(nid(3), 1_000);
		// 200 ms later the median should have advanced to ~1_200.
		let now = 1_200;
		let median = tracker.median_cluster_ms(now).unwrap();
		assert_eq!(median, 1_200);
	}

	// -------------- drift monitor --------

	#[test]
	fn drift_within_bound_does_not_abort() {
		let clock = Arc::new(FakeClock::new(1_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 999);
		tracker.record(nid(3), 1_001);
		let abort = RecordingAbort::new();
		let subscriber = RecordingDriftSubscriber::new();
		match check_drift_once_with_subscriber(
			&tracker,
			&*clock,
			&abort,
			&subscriber,
			MAX_OFFSET_MS,
		) {
			DriftCheck::Within { offset_ms } => assert!(offset_ms <= 2),
			other => panic!("expected Within, got {other:?}"),
		}
		assert!(abort.reasons().is_empty());
		assert!(subscriber.events().is_empty());
	}

	#[test]
	fn drift_insufficient_samples_is_not_an_abort() {
		let clock = Arc::new(FakeClock::new(1_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		// Only one peer — below DRIFT_MIN_SAMPLES.
		tracker.record(nid(2), 1_000);
		let abort = RecordingAbort::new();
		let subscriber = RecordingDriftSubscriber::new();
		assert_eq!(
			check_drift_once_with_subscriber(&tracker, &*clock, &abort, &subscriber, MAX_OFFSET_MS,),
			DriftCheck::InsufficientSamples
		);
		assert!(abort.reasons().is_empty());
		assert!(subscriber.events().is_empty());
	}

	#[test]
	fn drift_beyond_bound_reports_event_before_abort() {
		// RecordingAbort panics after recording, so we catch the
		// unwind and inspect the stored reason.
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		// Peers report ~9_000 ms at receipt 10_000 — median ≈9_000.
		// Local clock is 10_000 → offset 1_000 ms > 500 MAX.
		tracker.record(nid(2), 9_000);
		tracker.record(nid(3), 9_000);
		let abort = Arc::new(RecordingAbort::new());
		let subscriber = Arc::new(RecordingDriftSubscriber::new());
		let abort_for_check = abort.clone();
		let subscriber_for_check = subscriber.clone();
		let tracker_ref = &tracker;
		let clock_ref: &dyn PhysicalClock = &*clock;
		let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
			check_drift_once_with_subscriber(
				tracker_ref,
				clock_ref,
				&*abort_for_check,
				&*subscriber_for_check,
				MAX_OFFSET_MS,
			)
		}));
		assert!(result.is_err(), "expected RecordingAbort panic");
		let reasons = abort.reasons();
		assert_eq!(reasons.len(), 1);
		assert!(
			reasons[0].contains("clock drift"),
			"unexpected abort reason: {}",
			reasons[0]
		);
		let events = subscriber.events();
		assert_eq!(events.len(), 1);
		assert_eq!(events[0].local_node, nid(1));
		assert_eq!(events[0].local_physical_ms, 10_000);
		assert_eq!(events[0].cluster_median_ms, 9_000);
		assert_eq!(events[0].offset_ms, 1_000);
		assert_eq!(events[0].max_offset_ms, MAX_OFFSET_MS);
	}

	// -------------- eviction watchdog ----

	#[test]
	fn quorum_floor_matches_raft_majority() {
		assert_eq!(quorum_floor(1), 1);
		assert_eq!(quorum_floor(3), 2);
		assert_eq!(quorum_floor(5), 3);
		assert_eq!(quorum_floor(7), 4);
	}

	#[tokio::test]
	async fn sweep_returns_not_leader_when_follower() {
		let clock = Arc::new(FakeClock::new(0));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		let leader = FakeLeader::new(false);
		let adv = RecordingAdvancer::new();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		assert_eq!(decision, EvictionDecision::NotLeader);
		assert!(adv.calls().is_empty());
	}

	#[tokio::test]
	async fn sweep_all_healthy_when_all_fresh() {
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 9_999);
		tracker.record(nid(3), 9_999);
		let leader = FakeLeader::new(true);
		let adv = RecordingAdvancer::new();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		assert_eq!(decision, EvictionDecision::AllHealthy);
		assert!(adv.calls().is_empty());
	}

	#[tokio::test]
	async fn sweep_evicts_stale_and_advances_epoch() {
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 9_999); // receipt 10_000
		tracker.record(nid(3), 9_999);
		// 10 minutes later — nid(2)'s receipt now 10 min old.
		clock.set(10_000 + 10 * 60 * 1000);
		tracker.record(nid(3), clock.now_ms() - 1); // nid(3) refreshed
		let leader = FakeLeader::new(true);
		let adv = RecordingAdvancer::new();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		match decision {
			EvictionDecision::Evicted { dropped, epoch } => {
				assert_eq!(dropped, vec![nid(2)]);
				assert_eq!(epoch, 2);
			}
			other => panic!("unexpected decision {other:?}"),
		}
		let calls = adv.calls();
		assert_eq!(calls.len(), 1);
		assert_eq!(calls[0].epoch, 2);
		let ids: std::collections::HashSet<u32> = calls[0].peers.keys().map(|n| n.id).collect();
		assert_eq!(ids, [1u32, 3].into_iter().collect());
		// Tracker forgets the evicted peer so it does not re-trip.
		assert!(tracker.last_seen(nid(2)).is_none());
	}

	#[tokio::test]
	async fn sweep_refuses_eviction_that_breaks_quorum() {
		// Cluster of three with RF=3 → quorum floor is 2. Evicting
		// two peers simultaneously would leave only node 1 — below
		// quorum — so the watchdog must refuse.
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 9_000);
		tracker.record(nid(3), 9_000);
		clock.set(10_000 + 10 * 60 * 1000);
		let leader = FakeLeader::new(true);
		let adv = RecordingAdvancer::new();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		match decision {
			EvictionDecision::RefusedForQuorum { candidates } => {
				assert_eq!(candidates, vec![nid(2), nid(3)]);
			}
			other => panic!("expected RefusedForQuorum, got {other:?}"),
		}
		assert!(adv.calls().is_empty());
		// Tracker entries remain so the next cycle can try again.
		assert!(tracker.last_seen(nid(2)).is_some());
		assert!(tracker.last_seen(nid(3)).is_some());
	}

	#[tokio::test]
	async fn sweep_surfaces_advancer_failure_without_forgetting() {
		let clock = Arc::new(FakeClock::new(10_000));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		tracker.record(nid(2), 9_000);
		tracker.record(nid(3), 9_000);
		clock.set(10_000 + 10 * 60 * 1000);
		tracker.record(nid(3), clock.now_ms() - 1); // node 3 healthy
		let leader = FakeLeader::new(true);
		let adv = RecordingAdvancer::new();
		adv.fail_next();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		match decision {
			EvictionDecision::AdvancerFailed { dropped, error } => {
				assert_eq!(dropped, vec![nid(2)]);
				assert!(error.contains("forced failure"));
			}
			other => panic!("unexpected decision {other:?}"),
		}
		// Failure leaves the tracker intact so the next sweep retries.
		assert!(tracker.last_seen(nid(2)).is_some());
	}

	#[tokio::test]
	async fn sweep_never_evicts_local_node() {
		// Even if our own receipt is somehow "stale" (a corner case
		// more likely to surface in tests than production), the
		// local node must never appear in the dropped set.
		let clock = Arc::new(FakeClock::new(0));
		let tracker = NodeTracker::new(nid(1), clock.clone());
		// Craft a bizarre state where nid(1) is tracked (records for
		// self are normally ignored — we seed via direct insert for
		// this invariant test) and stale.
		tracker.seen.insert(
			nid(1),
			Heartbeat {
				remote_physical_ms: 0,
				local_receipt_ms: 0,
			},
		);
		tracker.record(nid(2), 9_999);
		tracker.record(nid(3), 9_999);
		clock.set(10_000 + 10 * 60 * 1000);
		tracker.record(nid(2), clock.now_ms() - 1);
		tracker.record(nid(3), clock.now_ms() - 1);
		let leader = FakeLeader::new(true);
		let adv = RecordingAdvancer::new();
		let current = epoch_of(1, &[1, 2, 3]);
		let decision = sweep_evictions_once(
			&tracker,
			&*clock,
			&leader,
			&adv,
			&current,
			3,
			EVICTION_THRESHOLD,
		)
		.await;
		// Local node stale in tracker but current peers fresh → the
		// survivors map still contains node 1 via the "always keep
		// local" branch, so dropped is empty.
		assert_eq!(decision, EvictionDecision::AllHealthy);
	}
}
