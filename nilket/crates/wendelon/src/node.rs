//! Single-node bootstrap composition (Target #8).
//!
//! [`WendelonNode::bootstrap_single_node`] is the one place that
//! knows how every subsystem fits together. Storage, consensus,
//! transport, liveness monitoring, and the Data / Raft Ring servers
//! are each built in isolation by their own module — this file is
//! the glue that hands one module's output to the next.
//!
//! The genesis pattern here is deliberately minimal: one node, RF=1,
//! durable local Raft artifacts, no QUIC endpoint. Multi-node
//! deployments layer on top by swapping the
//! `NoDialTransportProvider` for a real
//! [`QuicTransportProvider`] and by joining an existing cluster
//! instead of calling [`openraft::Raft::initialize`].
//!
//! ## What this file does NOT own
//!
//! * TLS material — callers construct [`TlsMaterial`] and pass an
//!   optional `quinn::Endpoint` in via [`WendelonNode::attach_quic`].
//! * CLI parsing / config file loading — `src/main.rs` handles that
//!   and hands [`NodeBootstrapConfig`] in fully populated.
//! * Graceful shutdown — the returned [`NodeHandles`] holds every
//!   background join handle so the owner can await or abort them;
//!   the eviction + drift watchdogs run forever by design.

use crate::consensus::WendelonRaft;
use crate::consensus::monitor::{
	AbortHandle, DRIFT_CHECK_INTERVAL, EVICTION_CHECK_INTERVAL, LeaderStatus, MembershipAdvancer,
	NodeTracker, PhysicalClock, ProcessAbort, SystemClock, spawn_drift_monitor,
	spawn_eviction_watchdog,
};
use crate::consensus::raft_network::WendelonRaftNetworkFactory;
use crate::consensus::raft_server::WendelonRaftInternalServer;
use crate::consensus::raft_storage::{
	DurableBootstrapMode, WendelonLogStore, WendelonStateMachine,
};
use crate::network::control_server::{WendelonControlServer, select_bootstrap_epoch};
use crate::network::data_server::{
	MIGRATION_SWEEP_FAILURE_BACKOFF, MIGRATION_SWEEP_INTERVAL, MIGRATION_SWEEP_MAX_PER_PASS,
	WendelonDataServer,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::{
	LeaseAuthority, dev_lease_signing_secret, lease_verifying_key_bytes,
};
use crate::network::pool::BufPool;
use crate::network::ring::RingRouter;
use crate::network::router::{MsgRouter, MsgRouterBuilder};
use crate::network::server::{HrftService, serve};
use crate::network::transfer_signing::{
	TransferAuthority, dev_transfer_signing_secret, transfer_verifying_key_bytes,
};
use crate::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportConnection,
	TransportProvider, TransportTier, TransportTierSet,
};
use crate::schema::NodeId;
use crate::storage::io_uring::{IoRingHandle, spawn_io_thread};
use crate::storage::l2::ObjectStoreConfig;
use crate::storage::manager::{SlabManager, SlabManagerConfig};
use async_trait::async_trait;
use openraft::{BasicNode, Config as OpenRaftConfig};
use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

// ============================================================
//  CONFIG
// ============================================================

/// Caller-supplied parameters for [`WendelonNode::bootstrap_single_node`].
/// Loaded from TOML in production (`plans/ops/configuration.md`) or
/// constructed programmatically in tests.
pub struct NodeBootstrapConfig {
	/// Numeric node id — also the openraft peer id. Must be unique
	/// across the cluster; single-node genesis picks any u32.
	pub node_id: u32,
	/// On-disk directory for L1 slab files. Must already exist —
	/// [`SlabManager::open`] refuses to create it so operator
	/// tooling stays in charge of permissions.
	pub data_dir: PathBuf,
	/// Seal threshold in bytes; rolls the active slab once exceeded.
	pub slab_max_bytes: u64,
	/// Replication factor published into the ring. Single-node
	/// deployments run at RF=1; production minimum is 3.
	pub rf: usize,
	/// Depth of the shared `io_uring` submission queue.
	pub io_depth: u32,
	/// `host:port` this node publishes into the membership epoch —
	/// peers dial here when we advertise a transport tier.
	pub advertise_address: String,
	/// Ed25519 secret key used when this node is the control-ring
	/// leader and signs a `LeaseDescriptor`.
	pub lease_signing_secret: [u8; 32],
	/// Trusted Ed25519 verifying keys for lease issuers, keyed by
	/// Raft `NodeId.id`. Data-ring lease gates reject descriptors
	/// from issuers not present here.
	pub trusted_lease_signers: BTreeMap<u32, [u8; 32]>,
	/// Ed25519 secret key used when this node signs a
	/// [`TransferToken`] as the current leader.
	pub transfer_signing_secret: [u8; 32],
	/// Trusted Ed25519 verifying keys for transfer-token issuers,
	/// keyed by Raft `NodeId.id`.
	pub trusted_transfer_signers: BTreeMap<u32, [u8; 32]>,
	/// Optional L2 backend for sealed-slab tiering.
	pub l2: Option<ObjectStoreConfig>,
}

impl NodeBootstrapConfig {
	/// Convenience for tests and dev deploys: tempdir-style data
	/// dir, RF=1, modest io_uring depth, loopback address.
	pub fn dev(node_id: u32, data_dir: PathBuf, advertise_address: String) -> Self {
		let lease_signing_secret = dev_lease_signing_secret(node_id);
		let mut trusted_lease_signers = BTreeMap::new();
		trusted_lease_signers.insert(node_id, lease_verifying_key_bytes(lease_signing_secret));
		let transfer_signing_secret = dev_transfer_signing_secret(node_id);
		let mut trusted_transfer_signers = BTreeMap::new();
		trusted_transfer_signers.insert(
			node_id,
			transfer_verifying_key_bytes(transfer_signing_secret),
		);
		Self {
			node_id,
			data_dir,
			slab_max_bytes: 64 * 1024 * 1024,
			rf: 1,
			io_depth: 64,
			advertise_address,
			lease_signing_secret,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			l2: None,
		}
	}
}

// ============================================================
//  NODE + HANDLES
// ============================================================

/// Assembled single-node bootstrap. Owns Arc handles to every
/// subsystem so `main.rs` (or a test) can introspect state and drive
/// the QUIC listener without reaching across module boundaries.
pub struct WendelonNode {
	pub local_node: NodeId,
	pub slab: Arc<SlabManager>,
	pub tracker: Arc<NodeTracker>,
	pub clock: Arc<dyn PhysicalClock>,
	pub raft: WendelonRaft,
	pub log_store: WendelonLogStore,
	pub state_machine: WendelonStateMachine,
	pub ring: Arc<RingRouter>,
	pub router: Arc<MsgRouter>,
	pub data_server: Arc<WendelonDataServer>,
	pub pool: Arc<BufPool>,
	pub io: IoRingHandle,
	pub epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	pub handles: NodeHandles,
}

/// Background task handles. Dropping the node drops these, which
/// aborts every watchdog and updater cleanly.
pub struct NodeHandles {
	pub ring_updater: JoinHandle<()>,
	pub drift_monitor: JoinHandle<()>,
	pub eviction_watchdog: JoinHandle<()>,
	pub lease_manager: JoinHandle<()>,
	pub epoch_reconciler: JoinHandle<()>,
	pub migration_sweeper: JoinHandle<()>,
}

/// Time-to-live the lease manager requests on every grant. The
/// renew interval is a third of this so two consecutive failed
/// ticks still leave a valid lease in place.
pub const LEASE_TTL_MS: u32 = 3_000;
/// Renewal cadence: `LEASE_TTL_MS / 3`.
pub const LEASE_RENEW_INTERVAL: std::time::Duration =
	std::time::Duration::from_millis((LEASE_TTL_MS / 3) as u64);

// ============================================================
//  BOOTSTRAP
// ============================================================

impl WendelonNode {
	/// Build every subsystem for a single-node Raft cluster and
	/// call [`openraft::Raft::initialize`] to elect this node leader.
	///
	/// Must run inside a tokio runtime — the
	/// [`TransportProviderFactory`] and every watchdog spawn
	/// immediately.
	pub async fn bootstrap_single_node(cfg: NodeBootstrapConfig) -> std::io::Result<Self> {
		let local_node = NodeId {
			id: cfg.node_id,
			_reserved: 0,
		};
		let lease_authority = Arc::new(LeaseAuthority::from_secret_bytes(
			local_node,
			cfg.lease_signing_secret,
			&cfg.trusted_lease_signers,
		)?);
		let transfer_authority = Arc::new(TransferAuthority::from_secret_bytes(
			local_node,
			cfg.transfer_signing_secret,
			&cfg.trusted_transfer_signers,
		)?);

		// 1. Shared buffer pool (HRFT frames + postcard bodies).
		let pool = BufPool::new(256, 1024 * 1024);

		// 2. io_uring thread for slab + WAL I/O.
		let io = spawn_io_thread(cfg.io_depth)?;

		// 3. L1 slab storage.
		let l2 = cfg.l2.as_ref().map(ObjectStoreConfig::build).transpose()?;
		let slab = Arc::new(SlabManager::open(SlabManagerConfig {
			dir: cfg.data_dir.clone(),
			max_slab_bytes: cfg.slab_max_bytes,
			io: io.clone(),
			l2,
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
		})?);

		// 4. Durable Raft storage + recovered state machine.
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		let log_store = WendelonLogStore::open(&wal_path, &vote_path, &snapshot_path, io.clone())?;
		let bootstrap_mode = log_store.bootstrap_mode().await;
		let state_machine = log_store.recovered_state_machine().await?;

		// 5. Seed membership epoch from durable history if present;
		//    otherwise fall back to the single-node genesis epoch.
		//    If recovery finds a committed-but-unseeded transition,
		//    `select_bootstrap_epoch` holds visibility at the prior
		//    epoch until the reconciler finishes auto-seeding.
		let fallback_epoch = seed_single_node_epoch(local_node, &cfg.advertise_address);
		let recovered_state = state_machine.state_snapshot().await;
		let seed_epoch = select_bootstrap_epoch(&recovered_state, &fallback_epoch, cfg.rf)?;
		let (epoch_tx, epoch_rx) = watch::channel(Arc::new(seed_epoch));

		// 6. Liveness tracker shared by the data-ring, the Raft
		//    internal server, the drift monitor, and the eviction
		//    watchdog.
		let clock: Arc<dyn PhysicalClock> = Arc::new(SystemClock);
		let tracker = Arc::new(NodeTracker::new(local_node, clock.clone()));

		// 7. Transport factory — empty provider map for the
		//    single-node bootstrap path (never dials a peer at
		//    RF=1). A no-op provider stands in so the factory has
		//    something to negotiate against if a stray dial ever
		//    slipped through.
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(
			TransportTier::QuicIoUring,
			Arc::new(NoDialTransportProvider),
		);
		let resolver = Arc::new(crate::network::transport::EpochCapabilityResolver::new(
			epoch_rx.clone(),
		));
		let local_tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let factory =
			TransportProviderFactory::new(local_tiers, providers, resolver, epoch_rx.clone());

		// 8. Raft network factory — uses the no-dial provider; a
		//    single-node cluster never spawns peer RPCs.
		let raft_network = WendelonRaftNetworkFactory::new(
			Arc::new(NoDialTransportProvider) as Arc<dyn TransportProvider>,
			pool.clone(),
		);

		// 9. openraft engine. `validate()` pins heartbeat / election
		//    timeouts against `Config`'s sanity checks.
		let raft_config = Arc::new(
			OpenRaftConfig {
				cluster_name: "wendelon".into(),
				..Default::default()
			}
			.validate()
			.map_err(|e| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("openraft config invalid: {e}"),
				)
			})?,
		);
		let raft = WendelonRaft::new(
			cfg.node_id,
			raft_config,
			raft_network,
			log_store.clone(),
			state_machine.clone(),
		)
		.await
		.map_err(|e| Error::new(ErrorKind::Other, format!("Raft::new: {e}")))?;

		// 10. Fresh bootstrap initializes a single-node cluster.
		//     Durable recovery must not silently rebootstrap over
		//     existing artifacts; it starts from the recovered
		//     vote/log/snapshot state instead.
		if bootstrap_mode == DurableBootstrapMode::FreshBootstrap {
			let mut members: BTreeMap<u32, BasicNode> = BTreeMap::new();
			members.insert(
				cfg.node_id,
				BasicNode {
					addr: cfg.advertise_address.clone(),
				},
			);
			raft.initialize(members)
				.await
				.map_err(|e| Error::new(ErrorKind::Other, format!("Raft::initialize: {e}")))?;
		}

		// 11. Ring router — starts empty, seeded by the first
		//     `epoch_rx` value the updater task observes before it
		//     begins blocking on `changed()`.
		let ring = Arc::new(RingRouter::empty());
		let ring_updater = ring.clone().spawn_updater(epoch_rx.clone(), cfg.rf);

		// 12. Drift monitor.
		let abort: Arc<dyn AbortHandle> = Arc::new(ProcessAbort);
		let drift_monitor = spawn_drift_monitor(
			tracker.clone(),
			clock.clone(),
			abort.clone(),
			DRIFT_CHECK_INTERVAL,
		);

		// 13. Data Ring + Raft Internal Ring servers. Both observe
		//     inbound HLCs into the shared tracker so the drift
		//     monitor sees Raft-layer and data-ring signal. The
		//     lease gate is flipped on after the manager tick has
		//     had a chance to file its first grant (step 15).
		let data_server = Arc::new(
			WendelonDataServer::new(
				slab.clone(),
				raft.clone(),
				log_store.clone(),
				state_machine.clone(),
				local_node,
				clock.clone(),
				ring.clone(),
				factory.clone(),
				pool.clone(),
			)
			.with_lease_authority(lease_authority.clone())
			.with_tracker(tracker.clone())
			.with_lease_gate(true),
		);
		let raft_server = Arc::new(
			WendelonRaftInternalServer::new(raft.clone(), pool.clone())
				.with_tracker(tracker.clone()),
		);
		let control_server = Arc::new(
			WendelonControlServer::new(
				raft.clone(),
				state_machine.clone(),
				local_node,
				epoch_tx.clone(),
				epoch_rx.clone(),
				factory.clone(),
				pool.clone(),
				cfg.rf as u32,
				LEASE_TTL_MS,
			)
			.with_lease_authority(lease_authority.clone())
			.with_transfer_authority(transfer_authority.clone()),
		);
		let epoch_reconciler = control_server.clone().spawn_epoch_reconciler();

		// 14. Eviction watchdog. Route leader-driven epoch changes
		//     back through the same Control Ring compound workflow
		//     the external RPC uses so migration auto-seeding stays
		//     attached to every transition source.
		let leader: Arc<dyn LeaderStatus> =
			Arc::new(RaftLeaderStatus::new(raft.clone(), local_node));
		let advancer: Arc<dyn MembershipAdvancer> =
			Arc::new(ControlRingMembershipAdvancer::new(control_server.clone()));
		let eviction_watchdog = spawn_eviction_watchdog(
			tracker.clone(),
			clock.clone(),
			leader,
			advancer,
			epoch_rx.clone(),
			cfg.rf,
			EVICTION_CHECK_INTERVAL,
		);

		// 15. Ring-aware dispatch — the single `HrftService` the QUIC
		//     listener hands every inbound frame to.
		let router = Arc::new(
			MsgRouterBuilder::default()
				.with_data_ring(data_server.clone())
				.with_control_ring(control_server)
				.with_raft_internal(raft_server)
				.build(),
		);

		// 16. Lease manager (plan 018). Runs in the background,
		//     renews every LEASE_TTL_MS / 3 and on every ring-epoch
		//     change, and updates the data server's local cache so
		//     the write/read gates installed above can admit traffic.
		let lease_manager = data_server.clone().spawn_lease_manager(
			ring.clone(),
			clock.clone(),
			epoch_rx.clone(),
			LEASE_RENEW_INTERVAL,
			LEASE_TTL_MS,
		);
		let migration_sweeper = data_server.clone().spawn_migration_sweeper(
			MIGRATION_SWEEP_INTERVAL,
			MIGRATION_SWEEP_FAILURE_BACKOFF,
			MIGRATION_SWEEP_MAX_PER_PASS,
		);

		Ok(Self {
			local_node,
			slab,
			tracker,
			clock,
			raft,
			log_store,
			state_machine,
			ring,
			router,
			data_server,
			pool,
			io,
			epoch_tx,
			handles: NodeHandles {
				ring_updater,
				drift_monitor,
				eviction_watchdog,
				lease_manager,
				epoch_reconciler,
				migration_sweeper,
			},
		})
	}

	/// Spawn the QUIC accept loop against an already-bound
	/// `quinn::Endpoint`. Kept separate from bootstrap so tests can
	/// exercise the in-process surface without binding a socket.
	pub fn serve_quic(&self, endpoint: quinn::Endpoint) -> JoinHandle<std::io::Result<()>> {
		let svc: Arc<dyn HrftService> = self.router.clone();
		let pool = self.pool.clone();
		tokio::spawn(async move { serve_dyn(endpoint, svc, pool).await })
	}
}

/// `serve` is generic over `S: HrftService`; wrapping an `Arc<dyn>`
/// in a thin shim lets the caller keep a trait-object handle on the
/// router rather than duplicating types per concrete service.
async fn serve_dyn(
	endpoint: quinn::Endpoint,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
) -> std::io::Result<()> {
	struct DynService(Arc<dyn HrftService>);
	#[async_trait]
	impl HrftService for DynService {
		async fn handle_frame(
			&self,
			remote: crate::schema::NodeId,
			frame: crate::network::core::HrftFrame,
		) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
			self.0.handle_frame(remote, frame).await
		}

		async fn handle_stream(
			&self,
			stream: Box<dyn crate::network::transport::TransportStream>,
			pool: Arc<BufPool>,
			remote: crate::schema::NodeId,
		) -> std::io::Result<()> {
			self.0.handle_stream(stream, pool, remote).await
		}
	}
	serve(endpoint, Arc::new(DynService(service)), pool).await
}

// ============================================================
//  HELPERS
// ============================================================

fn seed_single_node_epoch(local: NodeId, address: &str) -> MembershipEpoch {
	let mut peers = HashMap::new();
	peers.insert(
		local,
		PeerCapabilities {
			address: address.to_string(),
			tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
			roles: NodeRoleSet::empty()
				.with(NodeRole::Data)
				.with(NodeRole::Control),
		},
	);
	MembershipEpoch { epoch: 1, peers }
}

fn control_artifact_paths(data_dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
	(
		data_dir.join("control.wal"),
		data_dir.join("vote.wnd"),
		data_dir.join("snapshot.wnd"),
	)
}

// ============================================================
//  RAFT → MONITOR ADAPTERS
// ============================================================

/// Adapts [`WendelonRaft`]'s async leader query into the
/// [`LeaderStatus`] trait the eviction watchdog consumes.
pub struct RaftLeaderStatus {
	raft: WendelonRaft,
	local_id: u32,
}

impl RaftLeaderStatus {
	pub fn new(raft: WendelonRaft, local_node: NodeId) -> Self {
		Self {
			raft,
			local_id: local_node.id,
		}
	}
}

#[async_trait]
impl LeaderStatus for RaftLeaderStatus {
	async fn is_leader(&self) -> bool {
		self.raft
			.current_leader()
			.await
			.map(|id| id == self.local_id)
			.unwrap_or(false)
	}
}

/// Production membership advancer: routes watchdog-triggered epoch
/// changes through the committed Control Ring workflow rather than
/// mutating the local watch channel directly.
pub struct ControlRingMembershipAdvancer {
	control: Arc<WendelonControlServer>,
}

impl ControlRingMembershipAdvancer {
	pub fn new(control: Arc<WendelonControlServer>) -> Self {
		Self { control }
	}
}

#[async_trait]
impl MembershipAdvancer for ControlRingMembershipAdvancer {
	async fn advance_epoch(&self, next: MembershipEpoch) -> std::io::Result<()> {
		self.control.advance_membership_epoch(next).await
	}
}

// ============================================================
//  NO-OP TRANSPORT PROVIDER
// ============================================================

/// Fails every dial. Installed as the sole [`TransportProvider`] in
/// the single-node bootstrap so the factory has *something* to
/// negotiate against — at RF=1 we never fan out to a peer, so a
/// real provider would be pure overhead.
///
/// Production multi-node bootstrap will replace this with
/// [`crate::network::quic_transport::QuicTransportProvider`].
pub struct NoDialTransportProvider;

#[async_trait]
impl TransportProvider for NoDialTransportProvider {
	async fn connect(
		&self,
		target: NodeId,
		_address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		Err(Error::new(
			ErrorKind::Unsupported,
			format!("single-node bootstrap has no outbound transport (target={target:?})"),
		))
	}
}

// ============================================================
//  TESTS
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::consensus::WendelonRequest;
	use crate::consensus::monitor::{
		DriftCheck, EVICTION_THRESHOLD, EvictionDecision, MAX_OFFSET_MS, check_drift_once,
		sweep_evictions_once,
	};
	use crate::schema::{FinalizationMarker, HybridTimestamp};
	use std::time::Duration;

	fn tempdir(tag: &str) -> PathBuf {
		let base =
			std::env::temp_dir().join(format!("wendelon-bootstrap-{tag}-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir_all(&base).unwrap();
		base
	}

	async fn wait_for_local_leader(node: &WendelonNode) {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				return;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for node {} to become leader; current leader = {:?}",
				node.local_node.id,
				node.raft.current_leader().await
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	#[tokio::test]
	async fn single_node_bootstrap_assembles_every_subsystem() {
		let dir = tempdir("assemble");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			1,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Ring has been seeded from the genesis epoch.
		assert_eq!(node.ring.snapshot().data_capable_nodes(), 1);
		// Raft reaches leadership asynchronously after initialize.
		wait_for_local_leader(&node).await;
		assert_eq!(node.raft.current_leader().await, Some(node.local_node.id));
	}

	#[tokio::test]
	async fn single_node_bootstrap_recovers_durable_raft_artifacts() {
		let dir = tempdir("restart");
		let cfg = NodeBootstrapConfig::dev(11, dir.clone(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();

		wait_for_local_leader(&node).await;
		assert_eq!(node.raft.current_leader().await, Some(node.local_node.id));

		let commit_hlc = HybridTimestamp {
			physical_ms: 4_242,
			logical: 0,
			node_id: node.local_node.id,
		};
		node.raft
			.client_write(WendelonRequest::Finalize(FinalizationMarker {
				log_index: 2,
				term: 1,
				epoch: 99,
				commit_hlc,
				payload_hash: [0; 32],
				e_safe_index: 2,
				_reserved: 0,
			}))
			.await
			.unwrap();
		assert_eq!(
			node.state_machine
				.state_snapshot()
				.await
				.e_safe_watermarks
				.get(&99)
				.copied(),
			Some(commit_hlc)
		);

		node.handles.ring_updater.abort();
		node.handles.drift_monitor.abort();
		node.handles.eviction_watchdog.abort();
		node.handles.lease_manager.abort();
		node.handles.epoch_reconciler.abort();
		node.handles.migration_sweeper.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let recovered = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			11,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		assert_eq!(
			recovered
				.state_machine
				.state_snapshot()
				.await
				.e_safe_watermarks
				.get(&99)
				.copied(),
			Some(commit_hlc)
		);

		recovered.handles.ring_updater.abort();
		recovered.handles.drift_monitor.abort();
		recovered.handles.eviction_watchdog.abort();
		recovered.handles.lease_manager.abort();
		recovered.handles.epoch_reconciler.abort();
		recovered.handles.migration_sweeper.abort();
	}

	#[tokio::test]
	async fn bootstrapped_node_exposes_live_tracker_to_drift_monitor() {
		let dir = tempdir("drift");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Feed two synthetic peers matching the local clock so the
		// median calculation produces an in-band sample. The drift
		// check is hit directly (without the watchdog's ticker) so
		// the assertion does not race the scheduler.
		let now = node.clock.now_ms();
		node.tracker.record(
			NodeId {
				id: 100,
				_reserved: 0,
			},
			now,
		);
		node.tracker.record(
			NodeId {
				id: 101,
				_reserved: 0,
			},
			now,
		);

		struct PanicOnAbort;
		impl AbortHandle for PanicOnAbort {
			fn abort(&self, reason: &str) -> ! {
				panic!("drift monitor aborted: {reason}");
			}
		}
		let abort = PanicOnAbort;

		let decision = check_drift_once(&node.tracker, &*node.clock, &abort, MAX_OFFSET_MS);
		match decision {
			DriftCheck::Within { offset_ms } => assert!(offset_ms <= MAX_OFFSET_MS),
			DriftCheck::InsufficientSamples => {
				panic!("tracker should have two samples wired in");
			}
		}
	}

	#[tokio::test]
	async fn single_node_eviction_sweep_is_a_no_op_on_fresh_tracker() {
		let dir = tempdir("evict");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			42,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Exercise the real control-ring-backed advancer directly
		// without waiting for the watchdog's 30s ticker.
		let leader = RaftLeaderStatus::new(node.raft.clone(), node.local_node);
		let epoch_rx = node.epoch_tx.subscribe();
		let resolver = Arc::new(crate::network::transport::EpochCapabilityResolver::new(
			epoch_rx.clone(),
		));
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(
			TransportTier::QuicIoUring,
			Arc::new(NoDialTransportProvider),
		);
		let factory = TransportProviderFactory::new(
			TransportTierSet::empty().with(TransportTier::QuicIoUring),
			providers,
			resolver,
			epoch_rx.clone(),
		);
		let control_server = Arc::new(WendelonControlServer::new(
			node.raft.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.epoch_tx.clone(),
			epoch_rx,
			factory,
			node.pool.clone(),
			1,
			LEASE_TTL_MS,
		));
		let advancer = ControlRingMembershipAdvancer::new(control_server);
		// Wait until Raft reports this node as leader before
		// sampling — otherwise the sweep short-circuits to NotLeader.
		wait_for_local_leader(&node).await;
		let current = node.epoch_tx.borrow().clone();
		let decision = sweep_evictions_once(
			&node.tracker,
			&*node.clock,
			&leader,
			&advancer,
			&current,
			1,
			EVICTION_THRESHOLD,
		)
		.await;
		assert_eq!(decision, EvictionDecision::AllHealthy);
	}

	#[tokio::test]
	async fn router_rejects_unknown_msg_type() {
		let dir = tempdir("router");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			3,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let frame = crate::network::core::HrftFrame {
			header: crate::schema::FrameHeader {
				magic: crate::network::core::HRFT_MAGIC,
				version: 1,
				msg_type: 0xFF, // outside every defined ring
				flags: 0,
				stream_id: 0,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: node.pool.acquire(),
			arrow_payload: None,
			trailing: None,
		};
		let err = node
			.router
			.handle_frame(
				crate::schema::NodeId {
					id: 0,
					_reserved: 0,
				},
				frame,
			)
			.await
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
	}
}
