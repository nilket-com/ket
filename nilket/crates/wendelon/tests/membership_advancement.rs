#[path = "support/control_ring.rs"]
mod support;

use async_trait::async_trait;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use wendelon::client::ClusterStatusWire;
use wendelon::consensus::monitor::{
	AbortHandle, EVICTION_THRESHOLD, EvictionDecision, LeaderStatus, MAX_OFFSET_MS, NodeTracker,
	PhysicalClock, check_drift_once, sweep_evictions_once,
};
use wendelon::network::control_server::WendelonControlServer;
use wendelon::network::router::MsgRouterBuilder;
use wendelon::network::server::HrftService;
use wendelon::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportTier, TransportTierSet,
};
use wendelon::network::wire;
use wendelon::node::{
	ControlRingMembershipAdvancer, LEASE_TTL_MS, NodeBootstrapConfig, RaftLeaderStatus,
	WendelonNode,
};
use wendelon::schema::{MsgType, NodeId};

fn nid(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

fn caps(address: &str) -> PeerCapabilities {
	PeerCapabilities {
		address: address.into(),
		tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
		roles: NodeRoleSet::empty()
			.with(NodeRole::Control)
			.with(NodeRole::Data),
	}
}

fn membership(epoch: u64, peers: &[(u32, &str)]) -> MembershipEpoch {
	let mut map = HashMap::new();
	for (id, address) in peers {
		map.insert(nid(*id), caps(address));
	}
	MembershipEpoch { epoch, peers: map }
}

fn decode_status(resp: &wendelon::network::core::HrftFrame) -> ClusterStatusWire {
	wire::decode(&resp.meta).unwrap()
}

fn build_control_server(
	node: &WendelonNode,
	local_node: NodeId,
	current: MembershipEpoch,
	replication_factor: u32,
) -> (
	Arc<WendelonControlServer>,
	watch::Sender<Arc<MembershipEpoch>>,
) {
	let (epoch_tx, epoch_rx) = watch::channel(Arc::new(current));
	let base_service: Arc<dyn HrftService> = node.router.clone();
	let (factory, _) = support::loopback_factory(node.local_node, base_service);
	(
		Arc::new(WendelonControlServer::new(
			node.raft.clone(),
			node.state_machine.clone(),
			local_node,
			epoch_tx.clone(),
			epoch_rx,
			factory,
			node.pool.clone(),
			replication_factor,
			LEASE_TTL_MS,
		)),
		epoch_tx,
	)
}

fn build_status_client(
	local_node: NodeId,
	control: Arc<WendelonControlServer>,
) -> (
	Arc<wendelon::network::factory::TransportProviderFactory>,
	Arc<wendelon::network::pool::BufPool>,
) {
	let service: Arc<dyn HrftService> = Arc::new(
		MsgRouterBuilder::default()
			.with_control_ring(control)
			.build(),
	);
	support::loopback_factory(local_node, service)
}

struct FakeClock(Mutex<u64>);

impl FakeClock {
	fn new(now_ms: u64) -> Self {
		Self(Mutex::new(now_ms))
	}

	fn advance(&self, by_ms: u64) {
		let mut now = self.0.lock().unwrap();
		*now = now.saturating_add(by_ms);
	}
}

impl PhysicalClock for FakeClock {
	fn now_ms(&self) -> u64 {
		*self.0.lock().unwrap()
	}
}

#[derive(Default)]
struct RecordingAbort {
	reasons: Mutex<Vec<String>>,
}

impl RecordingAbort {
	fn reasons(&self) -> Vec<String> {
		self.reasons.lock().unwrap().clone()
	}
}

impl AbortHandle for RecordingAbort {
	fn abort(&self, reason: &str) -> ! {
		self.reasons.lock().unwrap().push(reason.to_string());
		panic!("RecordingAbort tripped: {reason}");
	}
}

struct AlwaysLeader;

#[async_trait]
impl LeaderStatus for AlwaysLeader {
	async fn is_leader(&self) -> bool {
		true
	}
}

#[tokio::test]
async fn stale_peer_eviction_commits_through_control_ring() {
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		1,
		support::tempdir("membership-commit"),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;

	let current = membership(1, &[(1, "127.0.0.1:0"), (2, "127.0.0.1:1")]);
	let (control, epoch_tx) = build_control_server(&node, node.local_node, current.clone(), 1);
	let advancer = ControlRingMembershipAdvancer::new(control.clone());
	let leader = RaftLeaderStatus::new(node.raft.clone(), node.local_node);
	let clock = Arc::new(FakeClock::new(10_000));
	let tracker = NodeTracker::new(node.local_node, clock.clone());
	tracker.record(nid(2), 9_999);
	clock.advance(EVICTION_THRESHOLD.as_millis() as u64 + 1);

	let decision = sweep_evictions_once(
		&tracker,
		&*clock,
		&leader,
		&advancer,
		&current,
		1,
		EVICTION_THRESHOLD,
	)
	.await;
	match decision {
		EvictionDecision::Evicted { dropped, epoch } => {
			assert_eq!(dropped, vec![nid(2)]);
			assert_eq!(epoch, 2);
		}
		other => panic!("expected committed eviction, got {other:?}"),
	}

	let visible = epoch_tx.borrow().clone();
	assert_eq!(visible.epoch, 2);
	assert_eq!(visible.peers.len(), 1);
	assert!(visible.peers.contains_key(&node.local_node));

	let snapshot = node.state_machine.state_snapshot().await;
	let committed = snapshot
		.epoch_history
		.get(&2)
		.expect("epoch 2 should be committed through Raft");
	assert_eq!(committed.peers.len(), 1);
	assert!(committed.peers.contains_key(&node.local_node.id));

	let (factory, pool) = build_status_client(node.local_node, control);
	let status_resp = support::call_frame(
		&factory,
		&pool,
		node.local_node,
		MsgType::STATUS_REQ,
		&[],
		None,
		None,
	)
	.await
	.unwrap();
	let status = decode_status(&status_resp);
	assert_eq!(status.epoch, 2);
	assert_eq!(status.leader, Some(node.local_node.id));
	assert_eq!(status.peers.len(), 1);
	assert_eq!(status.peers[0].node_id, node.local_node.id);

	support::abort_node(&node);
}

#[tokio::test]
async fn minority_watchdog_cannot_force_local_only_eviction() {
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		1,
		support::tempdir("membership-minority"),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	support::wait_for_local_leader(&node).await;

	let minority_local = nid(2);
	let current = membership(
		1,
		&[(1, "127.0.0.1:0"), (2, "127.0.0.1:1"), (3, "127.0.0.1:2")],
	);
	let (control, epoch_tx) = build_control_server(&node, minority_local, current.clone(), 3);
	let advancer = ControlRingMembershipAdvancer::new(control.clone());
	let clock = Arc::new(FakeClock::new(20_000));
	let tracker = NodeTracker::new(minority_local, clock.clone());
	tracker.record(nid(3), 19_999);
	clock.advance(EVICTION_THRESHOLD.as_millis() as u64 + 1);

	let decision = sweep_evictions_once(
		&tracker,
		&*clock,
		&AlwaysLeader,
		&advancer,
		&current,
		3,
		EVICTION_THRESHOLD,
	)
	.await;
	match decision {
		EvictionDecision::AdvancerFailed { dropped, error } => {
			assert_eq!(dropped, vec![nid(3)]);
			assert!(error.contains("not leader"), "unexpected error: {error}");
		}
		other => panic!("expected control-ring rejection, got {other:?}"),
	}

	let visible = epoch_tx.borrow().clone();
	assert_eq!(visible.epoch, 1);
	assert_eq!(visible.peers.len(), 3);
	assert!(tracker.last_seen(nid(3)).is_some());
	assert!(
		!node
			.state_machine
			.state_snapshot()
			.await
			.epoch_history
			.contains_key(&2)
	);

	let (factory, pool) = build_status_client(minority_local, control);
	let status_resp = support::call_frame(
		&factory,
		&pool,
		minority_local,
		MsgType::STATUS_REQ,
		&[],
		None,
		None,
	)
	.await
	.unwrap();
	let status = decode_status(&status_resp);
	assert_eq!(status.epoch, 1);
	assert_eq!(status.leader, Some(node.local_node.id));
	assert_eq!(status.peers.len(), 3);

	support::abort_node(&node);
}

#[test]
fn local_drift_still_aborts() {
	let clock = Arc::new(FakeClock::new(10_000));
	let tracker = NodeTracker::new(nid(1), clock.clone());
	tracker.record(nid(2), 9_000);
	tracker.record(nid(3), 9_000);
	let abort = Arc::new(RecordingAbort::default());
	let abort_for_check = abort.clone();

	let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
		check_drift_once(&tracker, &*clock, &*abort_for_check, MAX_OFFSET_MS)
	}));
	assert!(result.is_err(), "expected drift check to abort");

	let reasons = abort.reasons();
	assert_eq!(reasons.len(), 1);
	assert!(reasons[0].contains("clock drift"));
	assert!(reasons[0].contains("1000ms"));
}
