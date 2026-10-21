//! Per-instance deterministic JOIN handoff observation; never a production path.
use super::JoinClusterRequestWire;
use openraft::{BasicNode, RaftMetrics, ServerState, StoredMembership};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinGateStage {
	AfterPromotion,
	BeforeSuccess,
	BeforeSelfFence,
}

#[derive(Clone, Debug)]
pub struct JoinPromotionGate(Arc<Inner>);

#[derive(Debug)]
struct Inner {
	joining_id: u32,
	stage: JoinGateStage,
	armed: AtomicBool,
	mutation_path_calls: AtomicUsize,
	promoted: Notify,
	release: Notify,
	observations: Mutex<Vec<String>>,
	metrics: Mutex<BTreeMap<u32, watch::Receiver<RaftMetrics<u32, BasicNode>>>>,
	automatic_election_intent: Mutex<BTreeMap<u32, bool>>,
	request: Mutex<Option<JoinClusterRequestWire>>,
}

impl PartialEq for JoinPromotionGate {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}
impl Eq for JoinPromotionGate {}

impl JoinPromotionGate {
	pub fn new(joining_id: u32, stage: JoinGateStage) -> Self {
		Self(Arc::new(Inner {
			joining_id,
			stage,
			armed: AtomicBool::new(true),
			mutation_path_calls: AtomicUsize::new(0),
			promoted: Notify::new(),
			release: Notify::new(),
			observations: Mutex::new(Vec::new()),
			metrics: Mutex::new(BTreeMap::new()),
			automatic_election_intent: Mutex::new(BTreeMap::new()),
			request: Mutex::new(None),
		}))
	}

	pub(crate) fn register_metrics(
		&self,
		node_id: u32,
		metrics: watch::Receiver<RaftMetrics<u32, BasicNode>>,
		automatic_elections: bool,
	) {
		self.0.metrics.lock().unwrap().insert(node_id, metrics);
		self.record_election_intent(node_id, automatic_elections);
	}

	/// Records the exact per-instance runtime toggle made by the fixture.
	/// OpenRaft exposes setters, not a public getter for this runtime option.
	pub fn record_election_intent(&self, node_id: u32, enabled: bool) {
		self.0
			.automatic_election_intent
			.lock()
			.unwrap()
			.insert(node_id, enabled);
	}

	pub fn metrics_observation(&self) -> String {
		let metrics = self
			.0
			.metrics
			.lock()
			.unwrap()
			.iter()
			.map(|(id, receiver)| (*id, receiver.borrow().clone()))
			.collect::<BTreeMap<_, _>>();
		format!(
			"metrics={metrics:?}; automatic_election_intent={:?}",
			self.0.automatic_election_intent.lock().unwrap()
		)
	}

	/// A committed membership at the leader does not imply its final uniform
	/// configuration has reached the joining voter. Partition only after that
	/// voter has applied it and can participate with the intended quorum.
	pub async fn wait_for_applied_membership(
		&self,
		node_id: u32,
		expected: &StoredMembership<u32, BasicNode>,
	) {
		assert_eq!(
			self.0
				.automatic_election_intent
				.lock()
				.unwrap()
				.get(&node_id),
			Some(&true),
			"joining voter must retain automatic elections in this fixture"
		);
		let mut metrics = self
			.0
			.metrics
			.lock()
			.unwrap()
			.get(&node_id)
			.expect("node registered before JOIN promotion")
			.clone();
		let expected_log = expected
			.log_id()
			.expect("leader membership must have a committed log identity");
		let ready = async {
			loop {
				let current = metrics.borrow_and_update().clone();
				if current.running_state.is_ok()
					&& current.membership_config.as_ref() == expected
					&& current
						.last_applied
						.is_some_and(|id| id.index >= expected_log.index)
					&& current.state == ServerState::Follower
					&& expected.membership().get_joint_config().len() == 1
					&& expected.voter_ids().any(|id| id == node_id)
				{
					return;
				}
				metrics
					.changed()
					.await
					.expect("joining Raft metrics remain available");
			}
		};
		if tokio::time::timeout(std::time::Duration::from_secs(5), ready)
			.await
			.is_err()
		{
			panic!(
				"joining voter did not apply the required uniform membership before partition: expected={expected:?}; {}",
				self.metrics_observation()
			);
		}
	}

	/// The first partition leaves incumbent + joining voter connected. Every
	/// request already admitted to the other voter owns its entries before
	/// opening a simulated stream; a leader tail sampled after the cut bounds
	/// those in-flight entries. Apply that prefix before the second cut.
	pub async fn wait_for_applied_prefix(&self, node_id: u32, index: u64) {
		let mut metrics = self
			.0
			.metrics
			.lock()
			.unwrap()
			.get(&node_id)
			.expect("registered joining voter")
			.clone();
		let applied = async {
			loop {
				let current = metrics.borrow_and_update().clone();
				if current.running_state.is_ok()
					&& current.last_applied.is_some_and(|id| id.index >= index)
				{
					return;
				}
				metrics
					.changed()
					.await
					.expect("joining Raft metrics remain available");
			}
		};
		if tokio::time::timeout(std::time::Duration::from_secs(5), applied)
			.await
			.is_err()
		{
			panic!(
				"node {node_id} did not apply pre-partition leader prefix {index}: {}",
				self.metrics_observation()
			);
		}
	}

	pub async fn wait_for_leader(&self, node_id: u32, leader_id: u32) {
		let mut metrics = self
			.0
			.metrics
			.lock()
			.unwrap()
			.get(&node_id)
			.expect("registered Raft metrics")
			.clone();
		let converged = async {
			loop {
				if metrics.borrow_and_update().current_leader == Some(leader_id) {
					return;
				}
				metrics
					.changed()
					.await
					.expect("Raft metrics remain available");
			}
		};
		if tokio::time::timeout(std::time::Duration::from_secs(5), converged)
			.await
			.is_err()
		{
			panic!(
				"node {node_id} did not observe leader {leader_id}: {}",
				self.metrics_observation()
			);
		}
	}

	pub fn release_on_drop(&self) -> ReleaseOnDrop {
		ReleaseOnDrop(self.clone())
	}

	pub async fn wait_promoted(&self) {
		tokio::time::timeout(
			std::time::Duration::from_secs(30),
			self.0.promoted.notified(),
		)
		.await
		.expect("JOIN must reach committed voter promotion");
	}

	pub fn release(&self) {
		self.0.release.notify_one();
	}

	pub fn observations(&self) -> Vec<String> {
		self.0.observations.lock().unwrap().clone()
	}

	pub(super) fn record(&self, observation: String) {
		self.0.observations.lock().unwrap().push(observation);
	}

	pub fn mutation_path_calls(&self) -> usize {
		self.0.mutation_path_calls.load(Ordering::Acquire)
	}

	pub(super) fn record_mutation_path(&self, joining_id: u32) {
		if self.0.joining_id == 0 || self.0.joining_id == joining_id {
			self.0.mutation_path_calls.fetch_add(1, Ordering::AcqRel);
		}
	}

	pub fn promoted_request(&self) -> JoinClusterRequestWire {
		self.0
			.request
			.lock()
			.unwrap()
			.clone()
			.expect("promotion request captured")
	}

	pub(super) async fn pause(
		&self,
		stage: JoinGateStage,
		req: &JoinClusterRequestWire,
	) -> std::io::Result<()> {
		if self.0.stage == stage
			&& (self.0.joining_id == 0 || self.0.joining_id == req.node_id)
			&& self.0.armed.swap(false, Ordering::AcqRel)
		{
			*self.0.request.lock().unwrap() = Some(req.clone());
			self.0.promoted.notify_one();
			tokio::time::timeout(
				std::time::Duration::from_secs(30),
				self.0.release.notified(),
			)
			.await
			.map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::TimedOut,
					"JOIN promotion gate was not released",
				)
			})?;
		}
		Ok(())
	}
}

pub struct ReleaseOnDrop(JoinPromotionGate);
impl Drop for ReleaseOnDrop {
	fn drop(&mut self) {
		self.0.release();
	}
}
