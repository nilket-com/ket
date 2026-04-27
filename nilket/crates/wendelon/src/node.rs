//! Single-node bootstrap composition (Target #8).
//!
//! [`WendelonNode::bootstrap_single_node`] is the one place that
//! knows how every subsystem fits together. Storage, consensus,
//! transport, liveness monitoring, and the Data / Raft Ring servers
//! are each built in isolation by their own module — this file is
//! the glue that hands one module's output to the next.
//!
//! The genesis pattern here is deliberately minimal: one node, RF=1,
//! durable local Raft artifacts, and the same transport abstractions
//! the multi-node path uses. QUIC is the primary supported transport.
//! When `[transport.tls]` is configured, bootstrap binds the real
//! QUIC endpoint and may also wire optional compatibility listeners
//! and providers through the shared transport factory; otherwise
//! tests and single-node dev flows can still supply an in-process
//! provider.
//!
//! ## What this file does NOT own
//!
//! * TLS material lifetime beyond initial endpoint construction —
//!   hot cert rotation still needs an outer supervisor.
//! * CLI parsing / config file loading — `src/main.rs` handles that
//!   and hands [`NodeBootstrapConfig`] in fully populated.
//! * Graceful shutdown orchestration beyond the node's explicit
//!   drain hooks. Background maintenance loops are still separate
//!   tasks; [`WendelonNode::shutdown`] aborts and joins them once
//!   request drain has completed.

use crate::audit::{
	AuditAuthority, AuditClockDriftSubscriber, AuditEvent, AuditMaterializationTracker,
	AuditOperation, AuditRootSigner, AuditSink, AuditSinksConfig,
	DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL, build_audit_sink, spawn_signed_chain_root_rollup,
};
use crate::backup::{ClusterBackupImage, ClusterNodeBackupImage};
use crate::build_info;
use crate::consensus::hlc::HybridLogicalClockDriver;
use crate::consensus::monitor::{
	AbortHandle, ClockDriftEvent, DRIFT_CHECK_INTERVAL, EVICTION_CHECK_INTERVAL,
	FanoutClockDriftSubscriber, LeaderStatus, MembershipAdvancer, NodeTracker, PhysicalClock,
	ProcessAbort, SystemClock, WatchClockDriftSubscriber, spawn_drift_monitor_with_subscriber,
	spawn_eviction_watchdog,
};
use crate::consensus::raft_network::WendelonRaftNetworkFactory;
use crate::consensus::raft_server::WendelonRaftInternalServer;
use crate::consensus::raft_storage::{
	CheckpointCoordinator, DurableBootstrapMode, RevocationSnapshot, WendelonLogStore,
	WendelonStateMachine,
};
use crate::consensus::{AdvanceEpochRecord, WendelonRaft, WendelonRequest, WendelonResponse};
use crate::crypto::{CryptoCacheConfig, CryptoManager, RootKekProviderConfig};
use crate::metrics::IoUringClass;
use crate::metrics::observability::{
	DrainState, ListenerAddresses, ReadinessReporter, ReadinessSnapshot,
};
use crate::metrics::telemetry::{TraceExportConfig, TraceExportProtocol};
use crate::network::control_server::{
	JoinClusterRequestWire, JoinClusterResponseWire, WendelonControlServer,
	epoch_config_from_membership, membership_from_epoch_config, select_bootstrap_epoch,
};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::data_server::{
	DEFAULT_TENANT_CACHE_REFRESH_INTERVAL, MIGRATION_SWEEP_FAILURE_BACKOFF,
	MIGRATION_SWEEP_INTERVAL, MIGRATION_SWEEP_MAX_PER_PASS, PENDING_FINALIZATION_RETRY_INTERVAL,
	WendelonDataServer,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::{
	LeaseAuthority, dev_lease_signing_secret, lease_verifying_key_bytes,
};
use crate::network::pool::BufPool;
use crate::network::quic_transport::{
	QuicTransportProvider, canonical_server_name_resolver, endpoint_from_tls,
};
use crate::network::ring::RingRouter;
use crate::network::router::{MsgRouter, MsgRouterBuilder};
use crate::network::server::{
	HrftService, LifecycleService, ServerAdmissionConfig, ServerLifecycle,
	serve_tcp_tls_with_admission, serve_with_admission,
};
use crate::network::tcp_tls_transport::TcpTlsTransportProvider;
use crate::network::tls::{TlsMaterial, rustls_client_config, rustls_server_config};
use crate::network::transfer_signing::{
	TransferAuthority, dev_transfer_signing_secret, transfer_verifying_key_bytes,
};
use crate::network::transport::{
	MembershipEpoch, NodeCapability, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities,
	TransportConnection, TransportProvider, TransportTier, TransportTierSet,
};
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId, ProtocolVersionWindow, TenantId, TenantKekId};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::io_uring::{IoRingHandle, spawn_io_thread_named_for_class};
use crate::storage::l2::ObjectStoreConfig;
use crate::storage::manager::{RehydrationPolicy, SlabManager, SlabManagerConfig};
use crate::storage::{
	GroupCommitConfig, L2ColumnFetchConfig, ManagerTierObserver, MvccRetentionPolicy,
	RaftCommitter, SlabSweeper, SweeperConfig,
};
use async_trait::async_trait;
use openraft::{BasicNode, Config as OpenRaftConfig};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

// The bootstrap/default tenant stays unbounded in W5 so dev and
// single-tenant clusters exercise the same tenant create/quota code path
// without tripping an arbitrary cap before operators set a real quota.
const DEFAULT_TENANT_QUOTA_BYTES: u64 = u64::MAX;

// ============================================================
//  CONFIG
// ============================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedNodeConfig {
	pub node_id: u32,
	pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterBootstrapConfig {
	Bootstrap,
	Join { seeds: Vec<SeedNodeConfig> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportTlsConfig {
	pub cert_path: PathBuf,
	pub key_path: PathBuf,
	pub node_ca_path: PathBuf,
	pub operator_ca_path: PathBuf,
	pub cluster_domain: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservabilityConfig {
	pub listeners: Option<ListenerAddresses>,
	pub traces: Option<TraceExportConfig>,
}

/// Caller-supplied parameters for [`WendelonNode::bootstrap`].
/// Loaded from TOML in production (`plans/ops/configuration.md`) or
/// constructed programmatically in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
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
	/// ADR 016 coalescing window for data-path durable writes.
	pub group_commit: GroupCommitConfig,
	/// Replication factor published into the ring. Single-node
	/// deployments run at RF=1; production minimum is 3.
	pub rf: usize,
	/// Depth of the dedicated Raft WAL `io_uring` submission queue.
	pub wal_io_depth: u32,
	/// Depth of the dedicated slab-storage `io_uring` submission queue.
	pub slab_io_depth: u32,
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
	/// Fresh-cluster bootstrap or explicit join semantics.
	pub cluster: ClusterBootstrapConfig,
	/// Optional L2 backend for sealed-slab tiering.
	pub l2: Option<ObjectStoreConfig>,
	/// Background sweeper policy for L1 -> L2 migration and
	/// local-cache reclaim. Used only when `l2` is configured.
	pub tiering: SweeperConfig,
	/// Policy governing when hot L2 slabs are promoted back into
	/// local L1 storage.
	pub rehydration: RehydrationPolicy,
	/// Node-local ADR 044 L2 columnar fetch strategy config.
	pub l2_column_fetch: L2ColumnFetchConfig,
	/// Root KEK provider used to wrap and unwrap tenant KEKs.
	pub root_kek_provider: RootKekProviderConfig,
	/// Bounds for the in-process tenant-KEK and slab-DEK caches.
	pub crypto_cache: CryptoCacheConfig,
	/// Rolling-upgrade compatibility window advertised by this
	/// node during join and reconnect flows.
	pub protocol: ProtocolVersionWindow,
	/// Optional transport TLS material used by concrete socket-based
	/// providers and observability listeners.
	pub transport_tls: Option<TransportTlsConfig>,
	/// Listener-level admission bounds for inbound transport work.
	pub transport_admission: ServerAdmissionConfig,
	/// Maximum tolerated drift between the local physical clock and
	/// the cluster median before the node aborts.
	pub drift_max_offset_ms: u64,
	/// ADR 013 MVCC history retention policy.
	pub mvcc_retention_policy: MvccRetentionPolicy,
	/// Optional three-listener observability topology.
	pub observability: Option<ObservabilityConfig>,
	/// Optional additive audit shipping sinks; the local NDJSON chain
	/// remains mandatory and is always enabled.
	pub audit: AuditSinksConfig,
}

pub const DEFAULT_MVCC_RETENTION_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_MVCC_RETENTION_POLICY: MvccRetentionPolicy =
	MvccRetentionPolicy::FixedWindow(DEFAULT_MVCC_RETENTION_WINDOW);

impl NodeBootstrapConfig {
	/// Convenience for tests and dev deploys: tempdir-style data
	/// dir, RF=1, modest WAL/slab `io_uring` depth, loopback address.
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
			group_commit: GroupCommitConfig::default(),
			rf: 1,
			wal_io_depth: 64,
			slab_io_depth: 64,
			advertise_address,
			lease_signing_secret,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			cluster: ClusterBootstrapConfig::Bootstrap,
			l2: None,
			tiering: SweeperConfig::default(),
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: L2ColumnFetchConfig::default(),
			root_kek_provider: RootKekProviderConfig::DevInsecure,
			crypto_cache: CryptoCacheConfig::default(),
			protocol: ProtocolVersionWindow::default(),
			transport_tls: None,
			transport_admission: ServerAdmissionConfig::default(),
			drift_max_offset_ms: crate::consensus::monitor::MAX_OFFSET_MS,
			// Dev/test helpers seed synthetic HLCs (1, 2, 3, …),
			// so keep retention disabled unless a caller opts in
			// explicitly.
			mvcc_retention_policy: MvccRetentionPolicy::Disabled,
			observability: None,
			audit: AuditSinksConfig::default(),
		}
	}

	pub fn with_protocol_window(mut self, current: u16, min_compatible: u16) -> Self {
		self.protocol = ProtocolVersionWindow::new(current, min_compatible);
		self
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.node_id == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"node_id must be greater than zero",
			));
		}
		if !self.data_dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"data_dir is not an existing directory: {}",
					self.data_dir.display()
				),
			));
		}
		if self.slab_max_bytes == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"slab_max_bytes must be greater than zero",
			));
		}
		if self.slab_max_bytes > crate::crypto::SLAB_MAX_BYTES_HARD_CEILING {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"slab_max_bytes={} exceeds the hard ceiling of {} bytes",
					self.slab_max_bytes,
					crate::crypto::SLAB_MAX_BYTES_HARD_CEILING
				),
			));
		}
		#[cfg(feature = "fips")]
		if matches!(&self.root_kek_provider, RootKekProviderConfig::DevInsecure) {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"crypto.root_kek_provider=dev-insecure is disabled in fips builds",
			));
		}
		self.root_kek_provider.validate()?;
		self.crypto_cache.validate()?;
		if self.group_commit.floor.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"group_commit.floor must be greater than zero",
			));
		}
		if self.group_commit.per_write_cost_estimate.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"group_commit.per_write_cost_estimate must be greater than zero",
			));
		}
		if self.group_commit.ceiling < self.group_commit.floor {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"group_commit.ceiling must be greater than or equal to group_commit.floor",
			));
		}
		if self.rf == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"rf must be greater than zero",
			));
		}
		if self.wal_io_depth == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"wal_io_depth must be greater than zero",
			));
		}
		if self.slab_io_depth == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"slab_io_depth must be greater than zero",
			));
		}
		if self.advertise_address.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"advertise_address must not be empty",
			));
		}
		self.transport_admission.validate()?;
		if !self.trusted_lease_signers.contains_key(&self.node_id) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"trusted_lease_signers must contain this node's verifying key ({})",
					self.node_id
				),
			));
		}
		if !self.trusted_transfer_signers.contains_key(&self.node_id) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"trusted_transfer_signers must contain this node's verifying key ({})",
					self.node_id
				),
			));
		}
		self.rehydration.validate()?;
		self.l2_column_fetch.validate()?;
		if let Some(l2) = &self.l2 {
			l2.build()?;
			if self.tiering.scan_interval.is_zero() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"tiering.scan_interval must be greater than zero when l2 is configured",
				));
			}
		}
		self.mvcc_retention_policy.validate()?;
		match &self.cluster {
			ClusterBootstrapConfig::Bootstrap => {}
			ClusterBootstrapConfig::Join { seeds } => {
				if seeds.is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"cluster.join requires at least one seed node",
					));
				}
				for seed in seeds {
					if seed.node_id == 0 {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"cluster.join seed node_id must be greater than zero",
						));
					}
					if seed.node_id == self.node_id {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"cluster.join seed list must not contain this node_id",
						));
					}
					if seed.address.trim().is_empty() {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							"cluster.join seed address must not be empty",
						));
					}
				}
			}
		}
		if let Some(tls) = &self.transport_tls {
			if tls.cluster_domain.trim().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"transport_tls.cluster_domain must not be empty",
				));
			}
			for (field, path) in [
				("transport_tls.cert_path", &tls.cert_path),
				("transport_tls.key_path", &tls.key_path),
				("transport_tls.node_ca_path", &tls.node_ca_path),
				("transport_tls.operator_ca_path", &tls.operator_ca_path),
			] {
				if !path.is_file() {
					return Err(Error::new(
						ErrorKind::NotFound,
						format!("{field} is not an existing file: {}", path.display()),
					));
				}
			}
		}
		if self.drift_max_offset_ms == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"drift_max_offset_ms must be greater than zero",
			));
		}
		if let Some(obs) = &self.observability {
			if let Some(listeners) = &obs.listeners {
				if self.transport_tls.is_none() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"observability requires transport_tls for the mTLS ready and metrics listeners",
					));
				}
				for (field, value) in [
					("observability.listeners.health", listeners.health.as_str()),
					("observability.listeners.ready", listeners.ready.as_str()),
					(
						"observability.listeners.metrics",
						listeners.metrics.as_str(),
					),
				] {
					if value.trim().is_empty() {
						return Err(Error::new(
							ErrorKind::InvalidInput,
							format!("{field} must not be empty"),
						));
					}
				}
			}
			if let Some(traces) = &obs.traces {
				if traces.endpoint.trim().is_empty() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"observability.traces.endpoint must not be empty",
					));
				}
				if !traces.validates_sampling_rate() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"observability.traces.sampling_rate must be between 0.0 and 1.0 inclusive",
					));
				}
				if traces.max_queue_size == 0 {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"observability.traces.max_queue_size must be greater than zero",
					));
				}
				if traces.export_timeout.is_zero() {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						"observability.traces.export_timeout_s must be greater than zero",
					));
				}
				for (field, path) in [
					(
						"observability.traces.client_cert_path",
						&traces.client_cert_path,
					),
					(
						"observability.traces.client_key_path",
						&traces.client_key_path,
					),
					(
						"observability.traces.ca_bundle_path",
						&traces.ca_bundle_path,
					),
				] {
					if !path.is_file() {
						return Err(Error::new(
							ErrorKind::NotFound,
							format!("{field} is not an existing file: {}", path.display()),
						));
					}
				}
				match traces.protocol {
					TraceExportProtocol::Grpc => {
						#[cfg(not(feature = "otlp-grpc"))]
						{
							return Err(Error::new(
								ErrorKind::Unsupported,
								"observability.traces.protocol=grpc requires the otlp-grpc cargo feature",
							));
						}
					}
					TraceExportProtocol::HttpProtobuf => {
						#[cfg(not(feature = "otlp-http"))]
						{
							return Err(Error::new(
								ErrorKind::Unsupported,
								"observability.traces.protocol=http/protobuf requires the otlp-http cargo feature",
							));
						}
					}
				}
			}
		}
		if let Some(syslog) = &self.audit.syslog {
			if syslog.socket_path.as_os_str().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"audit.syslog.socket_path must not be empty",
				));
			}
		}
		if let Some(kafka) = &self.audit.kafka {
			if kafka.brokers.trim().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"audit.kafka.brokers must not be empty",
				));
			}
			if kafka.topic.trim().is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"audit.kafka.topic must not be empty",
				));
			}
			#[cfg(not(feature = "audit-kafka"))]
			{
				return Err(Error::new(
					ErrorKind::Unsupported,
					"audit.kafka requires the audit-kafka cargo feature",
				));
			}
		}
		wire::validate_protocol_window(self.protocol)?;
		Ok(())
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
	pub crypto: Arc<CryptoManager>,
	pub tracker: Arc<NodeTracker>,
	pub clock: Arc<dyn PhysicalClock>,
	pub hlc: Arc<HybridLogicalClockDriver>,
	pub raft: WendelonRaft,
	pub log_store: WendelonLogStore,
	pub state_machine: WendelonStateMachine,
	pub ring: Arc<RingRouter>,
	pub router: Arc<MsgRouter>,
	pub service: Arc<dyn HrftService>,
	pub data_server: Arc<WendelonDataServer>,
	pub pool: Arc<BufPool>,
	pub slab_io: IoRingHandle,
	pub wal_io: IoRingHandle,
	pub epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	pub protocol: ProtocolVersionWindow,
	pub lifecycle: ServerLifecycle,
	pub transport_admission: ServerAdmissionConfig,
	pub audit_sink: Arc<dyn AuditSink>,
	pub audit_materialization: Arc<AuditMaterializationTracker>,
	restore_validation: watch::Receiver<RestoreValidationStatus>,
	clock_drift_events: watch::Receiver<Option<ClockDriftEvent>>,
	transport_servers: Vec<NodeTransportServer>,
	pub handles: NodeHandles,
}

/// Background task handles. Dropping the node drops these, which
/// aborts every watchdog and updater cleanly.
pub struct NodeHandles {
	pub ring_updater: JoinHandle<()>,
	pub drift_monitor: JoinHandle<()>,
	pub eviction_watchdog: JoinHandle<()>,
	pub lease_manager: JoinHandle<()>,
	pub pending_finalization_reconciler: JoinHandle<()>,
	pub epoch_reconciler: JoinHandle<()>,
	pub tenant_policy_reconciler: JoinHandle<()>,
	pub migration_sweeper: JoinHandle<()>,
	pub tiering_sweeper: Option<JoinHandle<()>>,
	pub restore_validation: Option<JoinHandle<()>>,
	pub audit_chain_root_rollup: JoinHandle<()>,
	pub default_tenant_seeder: JoinHandle<()>,
	pub checkpoint_emitter: JoinHandle<()>,
}

impl NodeHandles {
	pub fn abort(&self) {
		self.ring_updater.abort();
		self.drift_monitor.abort();
		self.eviction_watchdog.abort();
		self.lease_manager.abort();
		self.pending_finalization_reconciler.abort();
		self.epoch_reconciler.abort();
		self.tenant_policy_reconciler.abort();
		self.migration_sweeper.abort();
		if let Some(handle) = &self.tiering_sweeper {
			handle.abort();
		}
		if let Some(handle) = &self.restore_validation {
			handle.abort();
		}
		self.audit_chain_root_rollup.abort();
		self.default_tenant_seeder.abort();
		self.checkpoint_emitter.abort();
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		self.abort();
		await_background_task(self.ring_updater, "ring updater").await?;
		await_background_task(self.drift_monitor, "drift monitor").await?;
		await_background_task(self.eviction_watchdog, "eviction watchdog").await?;
		await_background_task(self.lease_manager, "lease manager").await?;
		await_background_task(
			self.pending_finalization_reconciler,
			"pending finalization reconciler",
		)
		.await?;
		await_background_task(self.epoch_reconciler, "epoch reconciler").await?;
		await_background_task(self.tenant_policy_reconciler, "tenant policy reconciler").await?;
		await_background_task(self.migration_sweeper, "migration sweeper").await?;
		if let Some(handle) = self.tiering_sweeper {
			await_background_task(handle, "tiering sweeper").await?;
		}
		if let Some(handle) = self.restore_validation {
			await_background_task(handle, "restore validation").await?;
		}
		await_background_task(self.audit_chain_root_rollup, "audit chain root rollup").await?;
		await_background_task(self.default_tenant_seeder, "default tenant seeder").await?;
		await_background_task(self.checkpoint_emitter, "checkpoint emitter").await?;
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RestoreValidationStatus {
	Complete,
	Pending,
	Failed(String),
}

impl RestoreValidationStatus {
	fn degraded_reason(&self) -> Option<String> {
		match self {
			Self::Complete => None,
			Self::Pending => Some("RestoreValidationPending".to_string()),
			Self::Failed(reason) => Some(reason.clone()),
		}
	}
}

async fn await_background_task(handle: JoinHandle<()>, label: &str) -> std::io::Result<()> {
	match handle.await {
		Ok(()) => Ok(()),
		Err(err) if err.is_cancelled() => Ok(()),
		Err(err) => Err(Error::new(
			ErrorKind::Other,
			format!("{label} task failed during shutdown: {err}"),
		)),
	}
}

pub struct QuicServerHandle {
	endpoint: quinn::Endpoint,
	lifecycle: ServerLifecycle,
	task: JoinHandle<std::io::Result<()>>,
}

impl QuicServerHandle {
	pub fn begin_drain(&self) {
		self.endpoint.set_server_config(None);
		self.lifecycle.begin_drain();
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		self.begin_drain();
		let result = self.task.await.map_err(|err| {
			Error::new(
				ErrorKind::Other,
				format!("QUIC server task failed during shutdown: {err}"),
			)
		})?;
		self.endpoint.close(0u32.into(), b"shutdown");
		self.endpoint.wait_idle().await;
		result
	}
}

pub struct TcpTlsServerHandle {
	lifecycle: ServerLifecycle,
	task: JoinHandle<std::io::Result<()>>,
}

impl TcpTlsServerHandle {
	pub fn begin_drain(&self) {
		self.lifecycle.begin_drain();
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		self.begin_drain();
		self.task.await.map_err(|err| {
			Error::new(
				ErrorKind::Other,
				format!("TCP+TLS server task failed during shutdown: {err}"),
			)
		})?
	}
}

enum NodeTransportServer {
	Quic(QuicServerHandle),
	TcpTls(TcpTlsServerHandle),
}

impl NodeTransportServer {
	fn begin_drain(&self) {
		match self {
			Self::Quic(handle) => handle.begin_drain(),
			Self::TcpTls(handle) => handle.begin_drain(),
		}
	}

	async fn shutdown(self) -> std::io::Result<()> {
		match self {
			Self::Quic(handle) => handle.shutdown().await,
			Self::TcpTls(handle) => handle.shutdown().await,
		}
	}
}

enum BootstrapListener {
	Quic {
		endpoint: quinn::Endpoint,
		cluster_domain: String,
	},
	TcpTls {
		listener: TcpListener,
		acceptor: TlsAcceptor,
		cluster_domain: String,
	},
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
	/// Build every subsystem using the config-selected bootstrap
	/// mode. A fresh node either initializes a one-node Control
	/// Ring or publishes itself and explicitly joins an existing
	/// ring through its seed list. Durable recovery skips the
	/// fresh-cluster step and restarts from the recovered Raft
	/// artifacts.
	pub async fn bootstrap(cfg: NodeBootstrapConfig) -> std::io::Result<Self> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		if let Some(tls) = cfg.transport_tls.clone() {
			let material = TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)?;
			let bind_addr = resolve_socket_addr(&cfg.advertise_address)?;
			let cluster_domain = tls.cluster_domain.clone();
			let server_name = canonical_server_name_resolver(&cluster_domain);
			let quic_endpoint = endpoint_from_tls(&material, bind_addr)?;
			let tcp_listener = TcpListener::bind(&cfg.advertise_address).await?;
			let tcp_acceptor = TlsAcceptor::from(Arc::new(rustls_server_config(&material)?));
			let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
			providers.insert(
				TransportTier::QuicIoUring,
				Arc::new(
					QuicTransportProvider::with_cluster_domain(
						Arc::new(quic_endpoint.clone()),
						server_name.clone(),
						&cluster_domain,
					)
					.with_revocations(revocation_tx.subscribe()),
				),
			);
			providers.insert(
				TransportTier::TcpTls,
				Arc::new(
					TcpTlsTransportProvider::with_cluster_domain(
						rustls_client_config(&material)?,
						server_name,
						&cluster_domain,
					)
					.with_revocations(revocation_tx.subscribe()),
				),
			);
			return Self::bootstrap_with_transport_stack(
				cfg,
				socket_transport_tiers(),
				providers,
				vec![
					BootstrapListener::Quic {
						endpoint: quic_endpoint,
						cluster_domain: cluster_domain.clone(),
					},
					BootstrapListener::TcpTls {
						listener: tcp_listener,
						acceptor: tcp_acceptor,
						cluster_domain,
					},
				],
				revocation_tx,
			)
			.await;
		}
		if matches!(cfg.cluster, ClusterBootstrapConfig::Join { .. }) {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"cluster.join requires transport_tls or a caller-supplied real transport provider",
			));
		}
		Self::bootstrap_with_provider(cfg, Arc::new(NoDialTransportProvider)).await
	}

	/// Compatibility wrapper kept for older tests that want the
	/// original one-node bootstrap semantics regardless of the
	/// config's cluster mode.
	pub async fn bootstrap_single_node(mut cfg: NodeBootstrapConfig) -> std::io::Result<Self> {
		cfg.cluster = ClusterBootstrapConfig::Bootstrap;
		Self::bootstrap(cfg).await
	}

	pub async fn restore_from_cluster_backup(
		backup: &ClusterBackupImage,
		cfg: NodeBootstrapConfig,
	) -> std::io::Result<Self> {
		Self::restore_cluster_backup_artifacts(backup, &cfg).await?;
		// Preserve the caller's cluster mode so the same restore helper can
		// support both standalone recovery bootstrap and same-identity
		// replacement into a live cluster.
		Self::bootstrap(cfg).await
	}

	pub async fn restore_from_cluster_backup_with_provider(
		backup: &ClusterBackupImage,
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		Self::restore_cluster_backup_artifacts(backup, &cfg).await?;
		Self::bootstrap_with_provider(cfg, provider).await
	}

	pub async fn restore_replacement_from_cluster_backup(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: NodeBootstrapConfig,
	) -> std::io::Result<Self> {
		Self::prepare_replacement_from_cluster_backup(backup, source_node_id, &cfg).await?;
		Self::bootstrap(cfg).await
	}

	pub async fn restore_replacement_from_cluster_backup_with_provider(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		Self::prepare_replacement_from_cluster_backup(backup, source_node_id, &cfg).await?;
		Self::bootstrap_with_provider(cfg, provider).await
	}

	async fn restore_cluster_backup_artifacts(
		backup: &ClusterBackupImage,
		cfg: &NodeBootstrapConfig,
	) -> std::io::Result<()> {
		std::fs::create_dir_all(&cfg.data_dir)?;
		backup.validate_integrity()?;
		let node_backup = backup
			.nodes
			.iter()
			.find(|node| node.node_id == cfg.node_id)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"cluster backup does not contain a node storage artifact for node {}",
						cfg.node_id
					),
				)
			})?;
		WendelonLogStore::restore_backup(&backup.control, &cfg.data_dir).await?;
		SlabManager::restore_backup(&node_backup.storage, &cfg.data_dir).await?;
		Ok(())
	}

	async fn prepare_replacement_from_cluster_backup(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: &NodeBootstrapConfig,
	) -> std::io::Result<()> {
		if !matches!(cfg.cluster, ClusterBootstrapConfig::Join { .. }) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"replacement restore requires cluster.join so the node can reattach to a live cluster",
			));
		}
		if cfg.node_id == source_node_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"replacement restore requires a fresh node_id distinct from the failed node",
			));
		}
		std::fs::create_dir_all(&cfg.data_dir)?;
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		for path in [&wal_path, &vote_path, &snapshot_path] {
			if path.exists() {
				return Err(Error::new(
					ErrorKind::AlreadyExists,
					format!(
						"replacement restore target must not contain existing Control Ring artifacts: {}",
						path.display()
					),
				));
			}
		}
		backup.validate_integrity()?;
		let node_backup = backup
			.nodes
			.iter()
			.find(|node| node.node_id == source_node_id)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					format!(
						"cluster backup does not contain a node storage artifact for node {}",
						source_node_id
					),
				)
			})?;
		SlabManager::restore_backup(&node_backup.storage, &cfg.data_dir).await?;
		Ok(())
	}

	pub async fn export_cluster_backup(&self) -> std::io::Result<ClusterBackupImage> {
		let control = self.log_store.export_backup(&self.state_machine).await?;
		let state = self.state_machine.state_snapshot().await;
		let storage = self.slab.export_backup().await?;
		ClusterBackupImage::assemble(
			control,
			&state,
			vec![ClusterNodeBackupImage {
				node_id: self.local_node.id,
				storage,
			}],
		)
	}

	pub async fn rotate_tenant_kek(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
		if !self
			.state_machine
			.state_snapshot()
			.await
			.tenants
			.contains_key(&tenant_id)
		{
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("tenant {} does not exist", tenant_id.get()),
			));
		}
		let rotation = self.crypto.prepare_rotation(tenant_id)?;
		let tenant_kek_id = rotation.tenant_kek_id;
		commit_install_tenant_kek_version(&self.raft, rotation, "rotate_tenant_kek").await?;
		let local_active = self.crypto.active_tenant_kek_id(tenant_id)?;
		if local_active != tenant_kek_id {
			return Err(Error::other(format!(
				"rotate_tenant_kek: committed tenant_kek_id {} but local crypto active_tenant_kek_id is {}",
				tenant_kek_id, local_active
			)));
		}
		Ok(tenant_kek_id)
	}

	pub async fn create_tenant(
		&self,
		tenant_id: TenantId,
		quota_bytes: u64,
		replication_factor: u32,
	) -> std::io::Result<()> {
		let snapshot = self.state_machine.state_snapshot().await;
		let request = build_create_tenant_request(
			&snapshot,
			self.crypto.as_ref(),
			tenant_id,
			quota_bytes,
			replication_factor,
		)?;
		commit_create_tenant(&self.raft, request, "create_tenant").await
	}

	pub async fn update_tenant_quota(
		&self,
		tenant_id: TenantId,
		quota_bytes: u64,
	) -> std::io::Result<()> {
		commit_tenant_request(
			&self.raft,
			WendelonRequest::UpdateTenantQuota(crate::consensus::UpdateTenantQuota {
				tenant_id,
				quota_bytes,
			}),
			"update_tenant_quota",
		)
		.await
	}

	pub async fn remove_member_for_replacement(&self, removed_node_id: u32) -> std::io::Result<()> {
		if removed_node_id == 0 || removed_node_id == self.local_node.id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"replacement removal must target a non-local non-zero node id",
			));
		}
		if self.raft.current_leader().await != Some(self.local_node.id) {
			return Err(Error::new(
				ErrorKind::NotConnected,
				"remove_member_for_replacement must be called on the current leader",
			));
		}

		let snapshot = self.state_machine.state_snapshot().await;
		let current_epoch_cfg = snapshot
			.epoch_history
			.iter()
			.next_back()
			.map(|(_, cfg)| cfg.clone())
			.ok_or_else(|| {
				Error::other("replacement removal requires committed membership epoch history")
			})?;
		let mut next_membership = membership_from_epoch_config(&current_epoch_cfg);
		let removed_node = NodeId {
			id: removed_node_id,
			_reserved: 0,
		};
		let removed_from_epoch = next_membership.peers.remove(&removed_node).is_some();

		let mut desired_voters: BTreeSet<u32> = snapshot.last_membership.voter_ids().collect();
		let removed_from_voters = desired_voters.remove(&removed_node_id);
		drop(snapshot);

		if !removed_from_epoch && !removed_from_voters {
			return Ok(());
		}
		if desired_voters.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"replacement removal would leave the cluster without any voters",
			));
		}

		if removed_from_voters {
			self.raft
				.change_membership(desired_voters, true)
				.await
				.map_err(|err| {
					Error::other(format!(
						"remove_member_for_replacement change_membership failed: {err}"
					))
				})?;
		}

		if removed_from_epoch {
			next_membership.epoch = current_epoch_cfg.epoch.saturating_add(1);
			self.raft
				.client_write(WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
					from: current_epoch_cfg,
					to: epoch_config_from_membership(&next_membership),
				}))
				.await
				.map_err(|err| {
					Error::other(format!(
						"remove_member_for_replacement advance_epoch failed: {err}"
					))
				})?;
		}

		Ok(())
	}

	pub async fn suspend_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		commit_tenant_request(
			&self.raft,
			WendelonRequest::SuspendTenant(tenant_id),
			"suspend_tenant",
		)
		.await
	}

	pub async fn resume_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		commit_tenant_request(
			&self.raft,
			WendelonRequest::ResumeTenant(tenant_id),
			"resume_tenant",
		)
		.await
	}

	pub async fn delete_tenant(&self, tenant_id: TenantId) -> std::io::Result<()> {
		commit_tenant_request(
			&self.raft,
			WendelonRequest::DeleteTenant(tenant_id),
			"delete_tenant",
		)
		.await
	}

	/// Build every subsystem using the supplied outbound transport.
	/// In tests this is an in-process loopback provider; production
	/// callers can hand in a real QUIC transport once endpoint and
	/// TLS setup are wired.
	pub async fn bootstrap_with_provider(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		Self::bootstrap_with_provider_at_tier(cfg, provider, TransportTier::QuicIoUring).await
	}

	pub async fn bootstrap_with_provider_at_tier(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
		provider_tier: TransportTier,
	) -> std::io::Result<Self> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		let startup_listeners = if provider_tier == TransportTier::TcpTls {
			let tls = cfg.transport_tls.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"TcpTls bootstrap requires transport_tls so the inbound listener can start before join",
				)
			})?;
			let material = TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)?;
			let listener = TcpListener::bind(&cfg.advertise_address).await?;
			let acceptor = TlsAcceptor::from(Arc::new(rustls_server_config(&material)?));
			vec![BootstrapListener::TcpTls {
				listener,
				acceptor,
				cluster_domain: tls.cluster_domain.clone(),
			}]
		} else {
			Vec::new()
		};
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(provider_tier, provider);
		Self::bootstrap_with_transport_stack(
			cfg,
			transport_tiers_for_provider(provider_tier),
			providers,
			startup_listeners,
			revocation_tx,
		)
		.await
	}

	async fn bootstrap_with_transport_stack(
		cfg: NodeBootstrapConfig,
		local_tiers: TransportTierSet,
		providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
		startup_listeners: Vec<BootstrapListener>,
		revocation_tx: watch::Sender<Arc<RevocationSnapshot>>,
	) -> std::io::Result<Self> {
		cfg.validate()?;
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
		if matches!(&cfg.root_kek_provider, RootKekProviderConfig::DevInsecure) {
			tracing::warn!(
				"crypto.root_kek_provider=dev-insecure is enabled; this is a dev/test-only custody path and not a serious deployment boundary"
			);
		}

		// 1. Shared buffer pool (HRFT frames + postcard bodies).
		let pool = BufPool::new(256, 1024 * 1024);

		// 2. Dedicated `io_uring` workers for the two durability classes.
		let slab_io = spawn_io_thread_named_for_class(
			cfg.slab_io_depth,
			"wendelon-io-uring-slab",
			IoUringClass::Slab,
		)?;
		let wal_io = spawn_io_thread_named_for_class(
			cfg.wal_io_depth,
			"wendelon-io-uring-wal",
			IoUringClass::Wal,
		)?;

		// 3. L1 slab storage.
		let l2 = cfg.l2.as_ref().map(ObjectStoreConfig::build).transpose()?;
		let l2_for_sweeper = l2.clone();
		let crypto = Arc::new(CryptoManager::new_with_cache_config(
			cfg.root_kek_provider.build(&cfg.data_dir)?,
			cfg.crypto_cache,
		));
		let slab: Arc<SlabManager> = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: cfg.data_dir.clone(),
				max_slab_bytes: cfg.slab_max_bytes,
				io: slab_io.clone(),
				l2,
				rehydration: cfg.rehydration.clone(),
				l2_column_fetch: cfg.l2_column_fetch.clone(),
				group_commit: cfg.group_commit.clone(),
				crypto: crypto.clone(),
			})
			.await?,
		);

		// 4. Durable Raft storage + recovered state machine.
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		let log_store =
			WendelonLogStore::open(&wal_path, &vote_path, &snapshot_path, wal_io.clone()).await?;
		let bootstrap_mode = log_store.bootstrap_mode().await;
		let checkpoint_coordinator = CheckpointCoordinator::new(log_store.clone());
		let tier_observer = Arc::new(ManagerTierObserver::new(slab.clone()));
		let state_machine = log_store
			.recovered_state_machine()
			.await?
			.with_tier_observer(tier_observer)
			.with_tenant_kek_observer(crypto.clone())
			.with_checkpoint_observer(checkpoint_coordinator.clone())
			.with_external_revocation_snapshot(revocation_tx.clone());
		let checkpoint_emitter = checkpoint_coordinator.spawn();

		// 5. Seed membership epoch from durable history if present;
		//    otherwise fall back to the single-node genesis epoch.
		//    If recovery finds a committed-but-unseeded transition,
		//    `select_bootstrap_epoch` holds visibility at the prior
		//    epoch until the reconciler finishes auto-seeding.
		let fallback_epoch =
			seed_single_node_epoch(local_node, &cfg.advertise_address, local_tiers);
		let recovered_state = state_machine.state_snapshot().await;
		let requires_restore_validation = bootstrap_mode == DurableBootstrapMode::Recovery
			&& !recovered_state.tiered_slabs.is_empty();
		let (restore_validation_tx, restore_validation_rx) =
			watch::channel(if requires_restore_validation {
				RestoreValidationStatus::Pending
			} else {
				RestoreValidationStatus::Complete
			});
		// Safe because Raft has not been constructed yet, so no tenant-KEK
		// observer callback can race this authoritative bootstrap snapshot.
		crypto.replace_keysets_from_authoritative_snapshot(&recovered_state.tenant_keysets)?;
		let mut recovered_tiers = recovered_state
			.tiered_slabs
			.values()
			.cloned()
			.collect::<Vec<_>>();
		let recovered_log_index = recovered_state.last_applied_log_id.map(|id| id.index);
		recovered_tiers.sort_by_key(|tier| tier.slab_id);
		for tier in recovered_tiers {
			slab.reconcile_tiered_slab(
				crate::storage::TieredSlabPointer {
					slab_id: tier.slab_id,
					l2_uri: tier.l2_uri,
					payload_hash: tier.payload_hash,
					is_encrypted: tier.is_encrypted,
					tenant_id: tier.tenant_id,
					tenant_kek_id: tier.tenant_kek_id,
					stats_format_version: tier.stats_format_version,
					stats_block_len: tier.stats_block_len,
					stats_block_hash128: tier.stats_block_hash128,
					tombstone_metadata: tier.tombstone_metadata,
					supersession_metadata: tier.supersession_metadata,
				},
				recovered_log_index,
			)
			.await?;
		}
		let seed_epoch = select_bootstrap_epoch(&recovered_state, &fallback_epoch, cfg.rf)?;
		let (epoch_tx, epoch_rx) = watch::channel(Arc::new(seed_epoch));

		// 6. Liveness tracker shared by the data-ring, the Raft
		//    internal server, the drift monitor, and the eviction
		//    watchdog.
		let clock: Arc<dyn PhysicalClock> = Arc::new(SystemClock);
		let hlc = Arc::new(HybridLogicalClockDriver::new(local_node, clock.clone()));
		let tracker = Arc::new(NodeTracker::new(local_node, clock.clone()));
		let audit_materialization = Arc::new(AuditMaterializationTracker::new());
		let fips_build_state = match build_info::initialize_fips_runtime() {
			Ok(state) => {
				if state.fips_mode {
					crate::metrics::global()
						.fips_module_self_test_ok_total
						.inc();
				}
				state
			}
			Err(err) => return Err(err),
		};
		let audit_sink = build_audit_sink(cfg.data_dir.join("audit.ndjson"), &cfg.audit)?;
		crypto.attach_audit_sink(local_node, audit_sink.clone())?;
		audit_sink.emit(
			AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::CryptoStartup,
				local_node,
				format!(
					"selected_cipher={} cipher_detection_method={} root_kek_provider={} fips={} crypto_profile={} build_profile={} mlock={}",
					crypto.selected_cipher_label(),
					crypto.selected_cipher_detection_method(),
					crypto.root_kek_provider_label(),
					fips_build_state.fips_mode,
					build_info::crypto_profile(),
					build_info::build_mode(),
					crypto.mlock_status_label(),
				),
			)
			.with_parameter("selected_cipher", crypto.selected_cipher_label())
			.with_parameter(
				"cipher_detection_method",
				crypto.selected_cipher_detection_method(),
			)
			.with_parameter("root_kek_provider", crypto.root_kek_provider_label())
			.with_parameter("fips", fips_build_state.fips_mode)
			.with_parameter("crypto_profile", build_info::crypto_profile())
			.with_parameter("build_profile", build_info::build_mode())
			.with_parameter("mlock", crypto.mlock_status_label()),
		)?;
		let state_machine = state_machine.with_audit_materializer(
			local_node,
			audit_sink.clone(),
			audit_materialization.clone(),
		);
		let audit_chain_root_rollup = spawn_signed_chain_root_rollup(
			audit_sink.clone(),
			local_node,
			AuditRootSigner::from_seed(cfg.transfer_signing_secret),
			DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL,
		);
		let (clock_drift_tx, clock_drift_rx) = watch::channel(None);

		// 7. Transport factory — tests can still swap in loopback
		//    providers, while the production bootstrap path binds a
		//    real QUIC endpoint and installs its provider above.
		let publish_providers = providers.values().cloned().collect::<Vec<_>>();
		let resolver = Arc::new(crate::network::transport::EpochCapabilityResolver::new(
			epoch_rx.clone(),
		));
		let factory =
			TransportProviderFactory::new(local_tiers, providers, resolver, epoch_rx.clone());
		let dial_provider: Arc<dyn TransportProvider> = factory.clone();

		// 8. Raft network factory — uses the same shared
		//    tier-negotiating dialer as the rest of the node, so
		//    consensus traffic participates in the same QUIC/TCP
		//    fallback behavior as join and data/control RPCs.
		let raft_network = WendelonRaftNetworkFactory::new(dial_provider.clone(), pool.clone());

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

		// 10. Ring router — starts empty, seeded by the first
		//     `epoch_rx` value the updater task observes before it
		//     begins blocking on `changed()`.
		let ring = Arc::new(RingRouter::empty());
		let ring_updater = ring.clone().spawn_updater(epoch_rx.clone(), cfg.rf);

		// 11. Drift monitor.
		let abort: Arc<dyn AbortHandle> = Arc::new(ProcessAbort);
		let drift_subscriber = Arc::new(FanoutClockDriftSubscriber::new(vec![
			Arc::new(WatchClockDriftSubscriber::new(clock_drift_tx)),
			Arc::new(AuditClockDriftSubscriber::new(audit_sink.clone())),
		]));
		let drift_monitor = spawn_drift_monitor_with_subscriber(
			tracker.clone(),
			clock.clone(),
			abort.clone(),
			drift_subscriber,
			DRIFT_CHECK_INTERVAL,
			cfg.drift_max_offset_ms,
		);

		// 12. Data Ring + Raft Internal Ring servers. Both observe
		//     inbound HLCs into the shared tracker so the drift
		//     monitor sees Raft-layer and data-ring signal. The
		//     lease gate is flipped on only after bootstrap/join has
		//     completed and the background lease manager starts.
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
			.map_err(|err| Error::other(format!("DataServer::new: {err}")))?
			.with_hlc(hlc.clone())
			.with_audit_sink(audit_sink.clone())
			.with_lease_authority(lease_authority.clone())
			.with_mvcc_retention_policy(cfg.mvcc_retention_policy)
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
				crypto.clone(),
				local_node,
				epoch_tx.clone(),
				epoch_rx.clone(),
				factory.clone(),
				pool.clone(),
				cfg.rf as u32,
				LEASE_TTL_MS,
				audit_sink.clone(),
			)
			.with_protocol_window(cfg.protocol)
			.with_lease_authority(lease_authority.clone())
			.with_transfer_authority(transfer_authority.clone()),
		);

		// 13. Ring-aware dispatch — the single `HrftService` the QUIC
		//     listener hands every inbound frame to.
		let router = Arc::new(
			MsgRouterBuilder::default()
				.with_data_ring(data_server.clone())
				.with_control_ring(control_server.clone())
				.with_raft_internal(raft_server)
				.build(),
		);
		let lifecycle = ServerLifecycle::new();
		let service: Arc<dyn HrftService> =
			Arc::new(LifecycleService::new(router.clone(), lifecycle.clone()));
		for provider in publish_providers {
			provider.publish_local(local_node, &cfg.advertise_address, service.clone())?;
		}
		let transport_servers = startup_listeners
			.into_iter()
			.map(|listener| match listener {
				BootstrapListener::Quic {
					endpoint,
					cluster_domain,
				} => NodeTransportServer::Quic(spawn_quic_server(
					endpoint,
					service.clone(),
					pool.clone(),
					lifecycle.clone(),
					cfg.transport_admission.clone(),
					cluster_domain,
					revocation_tx.subscribe(),
				)),
				BootstrapListener::TcpTls {
					listener,
					acceptor,
					cluster_domain,
				} => NodeTransportServer::TcpTls(spawn_tcp_tls_server(
					listener,
					acceptor,
					service.clone(),
					pool.clone(),
					lifecycle.clone(),
					cfg.transport_admission.clone(),
					cluster_domain,
					revocation_tx.subscribe(),
				)),
			})
			.collect::<Vec<_>>();
		let epoch_reconciler = control_server.clone().spawn_epoch_reconciler();

		// 14. Fresh nodes either initialize a one-node cluster or
		//     join an existing leader through the explicit Control
		//     Ring join path. Recovery-mode nodes rely on their
		//     durable Raft membership/log state instead of replaying
		//     a synthetic join RPC on restart; reissuing join after
		//     recovery can perturb follower replication ordering and
		//     trip OpenRaft's follower-log-reversion guard.
		match &cfg.cluster {
			ClusterBootstrapConfig::Bootstrap => {
				if bootstrap_mode == DurableBootstrapMode::FreshBootstrap {
					let mut members: BTreeMap<u32, BasicNode> = BTreeMap::new();
					members.insert(
						cfg.node_id,
						BasicNode {
							addr: cfg.advertise_address.clone(),
						},
					);
					raft.initialize(members).await.map_err(|e| {
						Error::new(ErrorKind::Other, format!("Raft::initialize: {e}"))
					})?;
					wait_for_local_leader(&raft, local_node).await?;
				}
			}
			ClusterBootstrapConfig::Join { seeds } => {
				if bootstrap_mode == DurableBootstrapMode::FreshBootstrap {
					request_join_via_seeds(
						local_node,
						&cfg.advertise_address,
						local_tiers,
						advertised_roles(),
						advertised_capabilities(),
						cfg.protocol,
						dial_provider.clone(),
						pool.clone(),
						seeds,
					)
					.await?;
					wait_for_join_visibility(epoch_rx.clone()).await?;
				}
			}
		}
		if bootstrap_mode == DurableBootstrapMode::FreshBootstrap
			&& matches!(cfg.cluster, ClusterBootstrapConfig::Bootstrap)
		{
			match seed_default_tenant_kek_if_missing(
				&raft,
				&state_machine,
				crypto.as_ref(),
				local_node,
				cfg.rf as u32,
			)
			.await?
			{
				DefaultTenantKekSeedState::Complete => {}
				DefaultTenantKekSeedState::Retry => {
					return Err(Error::new(
						ErrorKind::Other,
						"lost local leadership while seeding the default tenant during fresh bootstrap",
					));
				}
			}
		}
		let default_tenant_seeder = if state_machine
			.state_snapshot()
			.await
			.tenants
			.contains_key(&TenantId::ZERO)
		{
			tokio::spawn(async {})
		} else {
			spawn_default_tenant_seeder(
				raft.clone(),
				state_machine.clone(),
				crypto.clone(),
				local_node,
				cfg.rf as u32,
			)
		};

		// Prime the tenant policy cache from the freshly seeded state
		// machine before spawning the periodic reconciler. Otherwise
		// the reconciler's first tick could race ahead of the seed,
		// stamp `last_refresh` on an empty snapshot, and starve the
		// next `ensure_tenant_policy_fresh` call of a real refresh for
		// a full tick interval.
		let _ = data_server.sync_tenant_policy_cache_once().await;
		let tenant_policy_reconciler = data_server
			.clone()
			.spawn_tenant_policy_reconciler(DEFAULT_TENANT_CACHE_REFRESH_INTERVAL);

		// 15. Eviction watchdog. Route leader-driven epoch changes
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
		let pending_finalization_reconciler = data_server
			.clone()
			.spawn_pending_finalization_reconciler(PENDING_FINALIZATION_RETRY_INTERVAL);
		let migration_sweeper = data_server.clone().spawn_migration_sweeper(
			MIGRATION_SWEEP_INTERVAL,
			MIGRATION_SWEEP_FAILURE_BACKOFF,
			MIGRATION_SWEEP_MAX_PER_PASS,
		);
		let tiering_sweeper = l2_for_sweeper.map(|object_store| {
			SlabSweeper::new(
				slab.clone(),
				object_store,
				Arc::new(RaftCommitter::new(raft.clone())),
				state_machine.clone(),
				cfg.tiering.clone(),
			)
			.with_clock(clock.clone())
			.with_audit_sink(local_node, audit_sink.clone())
			.with_mvcc_retention_policy(cfg.mvcc_retention_policy)
			.spawn()
		});
		let restore_validation = if requires_restore_validation {
			let slab = slab.clone();
			Some(tokio::spawn(async move {
				match slab.validate_tiered_references().await {
					Ok(()) => {
						let _ = restore_validation_tx.send(RestoreValidationStatus::Complete);
					}
					Err(err) => {
						tracing::error!(error = %err, "restore tier validation failed");
						let _ = restore_validation_tx.send(RestoreValidationStatus::Failed(
							format!("RestoreValidationFailed: {err}"),
						));
					}
				}
			}))
		} else {
			None
		};

		let node = Self {
			local_node,
			slab,
			crypto,
			tracker,
			clock,
			hlc,
			raft,
			log_store,
			state_machine,
			ring,
			router,
			service,
			data_server,
			pool,
			slab_io,
			wal_io,
			epoch_tx,
			protocol: cfg.protocol,
			lifecycle,
			transport_admission: cfg.transport_admission.clone(),
			audit_sink,
			audit_materialization,
			restore_validation: restore_validation_rx,
			clock_drift_events: clock_drift_rx,
			transport_servers,
			handles: NodeHandles {
				ring_updater,
				drift_monitor,
				eviction_watchdog,
				lease_manager,
				pending_finalization_reconciler,
				epoch_reconciler,
				tenant_policy_reconciler,
				migration_sweeper,
				tiering_sweeper,
				restore_validation,
				audit_chain_root_rollup,
				default_tenant_seeder,
				checkpoint_emitter,
			},
		};
		if node.data_server.degraded_reason().is_some() {
			tracing::error!(
				node_id = node.local_node.id,
				"bootstrapped with persisted local-persistence distrust fence; shutting down raft participation"
			);
			if let Err(err) = node.raft.shutdown().await {
				tracing::error!(
					node_id = node.local_node.id,
					error = %err,
					"failed to shut down raft participation for persisted distrust fence"
				);
			}
		}
		Ok(node)
	}

	pub fn subscribe_clock_drift(&self) -> watch::Receiver<Option<ClockDriftEvent>> {
		self.clock_drift_events.clone()
	}

	/// Spawn the QUIC accept loop against an already-bound
	/// `quinn::Endpoint`. Kept separate from bootstrap so tests can
	/// exercise the in-process surface without binding a socket.
	pub fn serve_quic(
		&self,
		endpoint: quinn::Endpoint,
		cluster_domain: impl Into<String>,
	) -> QuicServerHandle {
		spawn_quic_server(
			endpoint,
			self.service.clone(),
			self.pool.clone(),
			self.lifecycle.clone(),
			self.transport_admission.clone(),
			cluster_domain.into(),
			self.state_machine.subscribe_revocation_snapshot(),
		)
	}

	pub fn serve_tcp_tls(
		&self,
		listener: TcpListener,
		acceptor: TlsAcceptor,
		cluster_domain: impl Into<String>,
	) -> TcpTlsServerHandle {
		let svc = self.service.clone();
		let pool = self.pool.clone();
		let lifecycle = self.lifecycle.clone();
		let admission = self.transport_admission.clone();
		let cluster_domain = cluster_domain.into();
		let revocations = self.state_machine.subscribe_revocation_snapshot();
		let task = tokio::spawn({
			let lifecycle = lifecycle.clone();
			async move {
				serve_dyn_tcp_tls(
					listener,
					acceptor,
					svc,
					pool,
					lifecycle,
					admission,
					cluster_domain,
					Some(revocations),
				)
				.await
			}
		});
		TcpTlsServerHandle { lifecycle, task }
	}

	pub fn begin_drain(&self) {
		self.lifecycle.begin_drain();
		for server in &self.transport_servers {
			server.begin_drain();
		}
	}

	pub async fn wait_for_drain(&self) {
		self.lifecycle.wait_for_quiescence().await;
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		let lifecycle = self.lifecycle;
		let transport_servers = self.transport_servers;
		let handles = self.handles;
		lifecycle.begin_drain();
		for server in &transport_servers {
			server.begin_drain();
		}
		// Stop internal maintenance loops before waiting for
		// quiescence. Otherwise a background lease renewal or
		// migration commit can keep generating in-flight Control
		// Ring work while shutdown is waiting for the surface to
		// drain, especially after a peer has already left.
		handles.abort();
		lifecycle.wait_for_quiescence().await;
		for server in transport_servers {
			server.shutdown().await?;
		}
		handles.shutdown().await
	}

	pub fn readiness_reporter(&self) -> NodeReadinessView {
		NodeReadinessView {
			local_node: self.local_node,
			epoch_tx: self.epoch_tx.clone(),
			raft: self.raft.clone(),
			data_server: self.data_server.clone(),
			lifecycle: self.lifecycle.clone(),
			audit_materialization: self.audit_materialization.clone(),
			restore_validation: self.restore_validation.clone(),
		}
	}
}

#[derive(Clone)]
pub struct NodeReadinessView {
	local_node: NodeId,
	epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	raft: WendelonRaft,
	data_server: Arc<WendelonDataServer>,
	lifecycle: ServerLifecycle,
	audit_materialization: Arc<AuditMaterializationTracker>,
	restore_validation: watch::Receiver<RestoreValidationStatus>,
}

#[async_trait]
impl ReadinessReporter for NodeReadinessView {
	async fn snapshot(&self) -> ReadinessSnapshot {
		let epoch = self.epoch_tx.borrow().clone();
		let mut members = epoch.peers.keys().map(|node| node.id).collect::<Vec<_>>();
		members.sort_unstable();
		let drain_state = if self.lifecycle.is_draining() {
			if self.lifecycle.in_flight() == 0 {
				DrainState::Drained
			} else {
				DrainState::Draining
			}
		} else {
			DrainState::None
		};
		let audit_materialization_lag_entries = self.audit_materialization.lag_entries();
		let fips_build_state = build_info::current_fips_build_state();
		let degraded_reason = self
			.data_server
			.degraded_reason()
			.or_else(|| self.restore_validation.borrow().degraded_reason())
			.or_else(|| {
				(audit_materialization_lag_entries > 0).then(|| "AuditLagExceeded".to_string())
			});
		ReadinessSnapshot {
			node_id: self.local_node.id,
			epoch: epoch.epoch,
			leader_id: self.raft.current_leader().await,
			cluster_members: members,
			drain_state,
			audit_materialization_lag_entries,
			degraded_reason,
			fips_mode: fips_build_state.fips_mode,
			fips_self_test_last_ts: fips_build_state.self_test_last_ts,
		}
	}
}

/// `serve` is generic over `S: HrftService`; wrapping an `Arc<dyn>`
/// in a thin shim lets the caller keep a trait-object handle on the
/// router rather than duplicating types per concrete service.
async fn serve_dyn(
	endpoint: quinn::Endpoint,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: String,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<()> {
	struct DynService(Arc<dyn HrftService>);
	#[async_trait]
	impl HrftService for DynService {
		async fn handle_frame(
			&self,
			context: crate::network::server::RequestContext,
			frame: crate::network::core::HrftFrame,
		) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
			self.0.handle_frame(context, frame).await
		}

		async fn handle_stream(
			&self,
			stream: Box<dyn crate::network::transport::TransportStream>,
			pool: Arc<BufPool>,
			context: crate::network::server::RequestContext,
		) -> std::io::Result<()> {
			self.0.handle_stream(stream, pool, context).await
		}
	}
	serve_with_admission(
		endpoint,
		Arc::new(DynService(service)),
		pool,
		lifecycle,
		admission,
		cluster_domain,
		revocations,
	)
	.await
}

async fn serve_dyn_tcp_tls(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: String,
	revocations: Option<watch::Receiver<Arc<RevocationSnapshot>>>,
) -> std::io::Result<()> {
	struct DynService(Arc<dyn HrftService>);
	#[async_trait]
	impl HrftService for DynService {
		async fn handle_frame(
			&self,
			context: crate::network::server::RequestContext,
			frame: crate::network::core::HrftFrame,
		) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
			self.0.handle_frame(context, frame).await
		}

		async fn handle_stream(
			&self,
			stream: Box<dyn crate::network::transport::TransportStream>,
			pool: Arc<BufPool>,
			context: crate::network::server::RequestContext,
		) -> std::io::Result<()> {
			self.0.handle_stream(stream, pool, context).await
		}
	}

	serve_tcp_tls_with_admission(
		listener,
		acceptor,
		Arc::new(DynService(service)),
		pool,
		lifecycle,
		admission,
		cluster_domain,
		revocations,
	)
	.await
}

fn spawn_quic_server(
	endpoint: quinn::Endpoint,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: String,
	revocations: watch::Receiver<Arc<RevocationSnapshot>>,
) -> QuicServerHandle {
	let task_endpoint = endpoint.clone();
	let task = tokio::spawn({
		let lifecycle = lifecycle.clone();
		async move {
			serve_dyn(
				task_endpoint,
				service,
				pool,
				lifecycle,
				admission,
				cluster_domain,
				Some(revocations),
			)
			.await
		}
	});
	QuicServerHandle {
		endpoint,
		lifecycle,
		task,
	}
}

fn spawn_tcp_tls_server(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	admission: ServerAdmissionConfig,
	cluster_domain: String,
	revocations: watch::Receiver<Arc<RevocationSnapshot>>,
) -> TcpTlsServerHandle {
	let task = tokio::spawn({
		let lifecycle = lifecycle.clone();
		async move {
			serve_dyn_tcp_tls(
				listener,
				acceptor,
				service,
				pool,
				lifecycle,
				admission,
				cluster_domain,
				Some(revocations),
			)
			.await
		}
	});
	TcpTlsServerHandle { lifecycle, task }
}

// ============================================================
//  HELPERS
// ============================================================

fn seed_single_node_epoch(
	local: NodeId,
	address: &str,
	tiers: TransportTierSet,
) -> MembershipEpoch {
	let mut peers = HashMap::new();
	peers.insert(
		local,
		PeerCapabilities {
			address: address.to_string(),
			tiers,
			roles: advertised_roles(),
			capabilities: advertised_capabilities(),
		},
	);
	MembershipEpoch { epoch: 1, peers }
}

fn advertised_capabilities() -> NodeCapabilitySet {
	NodeCapabilitySet::empty()
		.with(NodeCapability::SlabStatsPruning)
		.with(NodeCapability::PartialAggregatePushdown)
}

fn transport_tiers_for_provider(provider_tier: TransportTier) -> TransportTierSet {
	TransportTierSet::empty().with(provider_tier)
}

fn socket_transport_tiers() -> TransportTierSet {
	TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.with(TransportTier::TcpTls)
}

fn advertised_roles() -> NodeRoleSet {
	NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
}

fn control_artifact_paths(data_dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
	(
		data_dir.join("control.wal"),
		data_dir.join("vote.wnd"),
		data_dir.join("snapshot.wnd"),
	)
}

fn resolve_socket_addr(address: &str) -> std::io::Result<SocketAddr> {
	let mut addrs = address.to_socket_addrs().map_err(|err| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("advertise_address {address:?} is not a resolvable socket address: {err}"),
		)
	})?;
	addrs.next().ok_or_else(|| {
		Error::new(
			ErrorKind::InvalidInput,
			format!("advertise_address {address:?} resolved to no socket addresses"),
		)
	})
}

async fn wait_for_local_leader(raft: &WendelonRaft, local_node: NodeId) -> std::io::Result<()> {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		if raft.current_leader().await == Some(local_node.id) {
			return Ok(());
		}
		if tokio::time::Instant::now() >= deadline {
			return Err(Error::new(
				ErrorKind::TimedOut,
				format!(
					"timed out waiting for node {} to become leader",
					local_node.id
				),
			));
		}
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

async fn commit_install_tenant_kek_version(
	raft: &WendelonRaft,
	rotation: crate::crypto::InstallTenantKekVersion,
	context: &str,
) -> std::io::Result<()> {
	let response = raft
		.client_write(crate::consensus::WendelonRequest::InstallTenantKekVersion(
			rotation,
		))
		.await
		.map_err(|err| Error::new(ErrorKind::Other, format!("{context}: {err}")))?;
	match response.data {
		crate::consensus::WendelonResponse::Success => Ok(()),
		crate::consensus::WendelonResponse::Error(message) => Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("{context}: {message}"),
		)),
		crate::consensus::WendelonResponse::LeaseDenied => Err(Error::other(format!(
			"{context}: unexpected LeaseDenied response from InstallTenantKekVersion"
		))),
	}
}

async fn commit_tenant_request(
	raft: &WendelonRaft,
	request: WendelonRequest,
	context: &str,
) -> std::io::Result<()> {
	let response = raft
		.client_write(request)
		.await
		.map_err(|err| Error::new(ErrorKind::Other, format!("{context}: {err}")))?;
	match response.data {
		WendelonResponse::Success => Ok(()),
		WendelonResponse::Error(message) => Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{context}: {message}"),
		)),
		WendelonResponse::LeaseDenied => Err(Error::other(format!(
			"{context}: unexpected LeaseDenied response"
		))),
	}
}

async fn commit_create_tenant(
	raft: &WendelonRaft,
	request: crate::consensus::CreateTenant,
	context: &str,
) -> std::io::Result<()> {
	commit_tenant_request(raft, WendelonRequest::CreateTenant(request), context).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DefaultTenantKekSeedState {
	Complete,
	Retry,
}

fn build_create_tenant_request(
	snapshot: &crate::consensus::raft_storage::SmState,
	crypto: &CryptoManager,
	tenant_id: TenantId,
	quota_bytes: u64,
	replication_factor: u32,
) -> std::io::Result<crate::consensus::CreateTenant> {
	if snapshot.tenants.contains_key(&tenant_id) {
		return Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("tenant {} already exists", tenant_id.get()),
		));
	}
	if let Some(keyset) = snapshot.tenant_keysets.get(&tenant_id) {
		keyset.active_tenant_kek_id.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"tenant {} keyset exists but has no active tenant_kek_id",
					tenant_id.get()
				),
			)
		})?;
		return Ok(crate::consensus::CreateTenant {
			tenant_id,
			quota_bytes,
			replication_factor,
			tenant_keyset: keyset.clone(),
		});
	}
	let rotation = crypto.prepare_rotation(tenant_id)?;
	let mut tenant_keyset = crate::crypto::TenantKeyset::default();
	tenant_keyset.active_tenant_kek_id = Some(rotation.tenant_kek_id);
	tenant_keyset.keys.insert(
		rotation.tenant_kek_id,
		crate::crypto::WrappedTenantKek {
			wrapped_key: rotation.wrapped_tenant_kek,
		},
	);
	Ok(crate::consensus::CreateTenant {
		tenant_id,
		quota_bytes,
		replication_factor,
		tenant_keyset,
	})
}

async fn seed_default_tenant_kek_if_missing(
	raft: &WendelonRaft,
	state_machine: &WendelonStateMachine,
	crypto: &CryptoManager,
	local_node: NodeId,
	replication_factor: u32,
) -> std::io::Result<DefaultTenantKekSeedState> {
	let snapshot = state_machine.state_snapshot().await;
	if snapshot.tenants.contains_key(&TenantId::ZERO) {
		return Ok(DefaultTenantKekSeedState::Complete);
	}
	if raft.current_leader().await != Some(local_node.id) {
		return Ok(DefaultTenantKekSeedState::Retry);
	}
	let create = build_create_tenant_request(
		&snapshot,
		crypto,
		TenantId::ZERO,
		DEFAULT_TENANT_QUOTA_BYTES,
		replication_factor,
	)?;
	match commit_create_tenant(raft, create, "failed to seed default tenant").await {
		Ok(()) => Ok(DefaultTenantKekSeedState::Complete),
		Err(err) if raft.current_leader().await != Some(local_node.id) => {
			let _ = err;
			Ok(DefaultTenantKekSeedState::Retry)
		}
		Err(err) => Err(err),
	}
}

fn spawn_default_tenant_seeder(
	raft: WendelonRaft,
	state_machine: WendelonStateMachine,
	crypto: Arc<CryptoManager>,
	local_node: NodeId,
	replication_factor: u32,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		loop {
			match seed_default_tenant_kek_if_missing(
				&raft,
				&state_machine,
				crypto.as_ref(),
				local_node,
				replication_factor,
			)
			.await
			{
				Ok(DefaultTenantKekSeedState::Complete) => return,
				Ok(DefaultTenantKekSeedState::Retry) => {}
				Err(err) => {
					tracing::warn!(
						node_id = local_node.id,
						error = %err,
						"default tenant provisioning is still pending"
					);
				}
			}
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	})
}

async fn wait_for_join_visibility(
	mut epoch_rx: watch::Receiver<Arc<MembershipEpoch>>,
) -> std::io::Result<()> {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	loop {
		if epoch_rx.borrow().peers.len() > 1 {
			return Ok(());
		}
		if tokio::time::Instant::now() >= deadline {
			return Err(Error::new(
				ErrorKind::TimedOut,
				"timed out waiting for joined membership visibility",
			));
		}
		let _ =
			tokio::time::timeout(std::time::Duration::from_millis(100), epoch_rx.changed()).await;
	}
}

async fn request_join_via_seeds(
	local_node: NodeId,
	advertise_address: &str,
	local_tiers: TransportTierSet,
	local_roles: NodeRoleSet,
	local_capabilities: NodeCapabilitySet,
	local_protocol: ProtocolVersionWindow,
	provider: Arc<dyn TransportProvider>,
	pool: Arc<BufPool>,
	seeds: &[SeedNodeConfig],
) -> std::io::Result<()> {
	let req = JoinClusterRequestWire {
		node_id: local_node.id,
		advertise_address: advertise_address.to_string(),
		tier_bits: local_tiers.as_u8(),
		role_bits: local_roles.as_u8(),
		capability_bits: local_capabilities.as_u8(),
		protocol: local_protocol,
	};
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
	let mut last_err: Option<Error> = None;

	loop {
		for seed in seeds {
			match send_join_request(
				&provider,
				&pool,
				NodeId {
					id: seed.node_id,
					_reserved: 0,
				},
				&seed.address,
				&req,
			)
			.await
			{
				Ok(resp) if resp.accepted => {
					let expected = wire::negotiate_protocol(local_protocol, resp.local_protocol)?;
					let negotiated_protocol = resp.negotiated_protocol.unwrap_or(expected);
					if negotiated_protocol != expected {
						return Err(Error::new(
							ErrorKind::InvalidData,
							format!(
								"join accepted by seed {} with mismatched negotiated protocol {}; expected {}",
								seed.node_id, negotiated_protocol, expected
							),
						));
					}
					return Ok(());
				}
				Ok(resp) => {
					last_err = Some(Error::new(
						match resp.reason_code {
							crate::network::control_server::CONTROL_REASON_NOT_LEADER => {
								ErrorKind::NotConnected
							}
							crate::network::control_server::CONTROL_REASON_INCOMPATIBLE_VERSION => {
								ErrorKind::Unsupported
							}
							crate::network::control_server::CONTROL_REASON_INVALID_CONFIG => {
								ErrorKind::InvalidInput
							}
							_ => ErrorKind::Other,
						},
						format!(
							"join rejected by seed {} (reason_code={}, leader_hint={:?}, local_protocol={}, negotiated_protocol={:?})",
							seed.node_id,
							resp.reason_code,
							resp.leader_hint,
							resp.local_protocol,
							resp.negotiated_protocol
						),
					));
				}
				Err(err) => last_err = Some(err),
			}
		}

		if tokio::time::Instant::now() >= deadline {
			return Err(last_err.unwrap_or_else(|| {
				Error::new(
					ErrorKind::TimedOut,
					"timed out waiting for any seed to accept join",
				)
			}));
		}
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
}

async fn send_join_request(
	provider: &Arc<dyn TransportProvider>,
	pool: &Arc<BufPool>,
	target: NodeId,
	address: &str,
	req: &JoinClusterRequestWire,
) -> std::io::Result<JoinClusterResponseWire> {
	let conn = provider.connect(target, address).await?;
	let mut stream = conn.open_bi().await?;
	let meta = wire::encode(req)?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: crate::schema::HRFT_ENVELOPE_VERSION,
		msg_type: MsgType::JOIN_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: meta.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, &meta, None, None).await?;
	stream.finish().await?;
	let resp = read_frame(&mut *stream, pool).await?;
	if resp.header.msg_type != MsgType::JOIN_RESP.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"seed {} returned wrong msg_type=0x{:02x}",
				target.id, resp.header.msg_type
			),
		));
	}
	wire::decode(&resp.meta)
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
	use crate::consensus::TieredSlab;
	use crate::consensus::WendelonRequest;
	use crate::consensus::monitor::{
		DriftCheck, EVICTION_THRESHOLD, EvictionDecision, MAX_OFFSET_MS, check_drift_once,
		sweep_evictions_once,
	};
	use crate::metrics::observability::ReadinessReporter;
	use crate::schema::{FinalizationMarker, HybridTimestamp, Identity, TenantId};
	use crate::storage::{RaftCommitter, SlabSweeper, StorageEngine, SweeperConfig};
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::time::Duration;

	fn tempdir(tag: &str) -> PathBuf {
		let base =
			std::env::temp_dir().join(format!("wendelon-bootstrap-{tag}-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir_all(&base).unwrap();
		base
	}

	fn park_background_tiering(cfg: &mut NodeBootstrapConfig) {
		cfg.tiering.scan_interval = Duration::from_secs(300);
		cfg.tiering.min_slab_bytes = u64::MAX;
		cfg.tiering.max_plaintext_rewrites_per_pass = 0;
		cfg.tiering.max_dek_rewraps_per_pass = 0;
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

	async fn wait_for_default_tenant_kek(node: &WendelonNode) {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node
				.state_machine
				.state_snapshot()
				.await
				.tenant_keysets
				.contains_key(&TenantId::ZERO)
			{
				return;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for default tenant to be provisioned"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	async fn wait_for_ready(node: &WendelonNode) {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.readiness_reporter().snapshot().await.is_ready() {
				return;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for node {} to become ready; snapshot = {:?}",
				node.local_node.id,
				node.readiness_reporter().snapshot().await
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	async fn wait_for_tiered_slab(node: &WendelonNode, slab_id: crate::schema::SlabId) {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
		loop {
			if node.slab.is_tiered(slab_id).await {
				return;
			}
			if tokio::time::Instant::now() >= deadline {
				panic!(
					"timed out waiting for slab {slab_id} to tier; sealed={} tiered={}",
					node.slab.sealed_count().await,
					node.slab.tiered_count().await
				);
			}
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	fn read_audit_log(dir: &std::path::Path) -> String {
		std::fs::read_to_string(dir.join("audit.ndjson")).unwrap()
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
	async fn bootstrap_emits_crypto_startup_audit_and_seeds_default_tenant_kek() {
		let dir = tempdir("crypto-startup");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			13,
			dir.clone(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;
		let snapshot = node.state_machine.state_snapshot().await;
		assert!(
			snapshot.tenant_keysets.contains_key(&TenantId::ZERO),
			"default tenant must be provisioned during bootstrap"
		);
		let audit = read_audit_log(&dir);
		assert!(audit.contains("\"event_type\":\"crypto.startup\""));
		assert!(audit.contains(node.crypto.selected_cipher_label()));
		assert!(audit.contains(node.crypto.selected_cipher_detection_method()));
		assert!(audit.contains(node.crypto.root_kek_provider_label()));
		assert!(audit.contains(&format!("fips={}", cfg!(feature = "fips"))));
		assert!(audit.contains(&format!(
			"crypto_profile={}",
			crate::build_info::crypto_profile()
		)));
		assert!(audit.contains("build_profile=debug"));
		assert!(audit.contains("mlock="));
	}

	#[tokio::test]
	async fn readiness_reporter_does_not_flap_during_successful_raft_audit_materialization() {
		let dir = tempdir("readiness-no-flap");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			23,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		wait_for_local_leader(&node).await;
		let reporter = node.readiness_reporter();
		let stop = Arc::new(AtomicBool::new(false));
		let flapped = Arc::new(AtomicBool::new(false));
		let poll_stop = stop.clone();
		let poll_flapped = flapped.clone();
		let poller = tokio::spawn(async move {
			while !poll_stop.load(Ordering::Relaxed) {
				let snapshot = reporter.snapshot().await;
				if !snapshot.is_ready() {
					poll_flapped.store(true, Ordering::Relaxed);
					return;
				}
				tokio::task::yield_now().await;
			}
		});

		for seq in 0..64u64 {
			let commit_hlc = HybridTimestamp {
				physical_ms: 10_000 + seq,
				logical: 0,
				node_id: node.local_node.id,
			};
			node.raft
				.client_write(WendelonRequest::Finalize(
					crate::consensus::TenantFinalization {
						marker: FinalizationMarker {
							log_index: seq + 2,
							term: 1,
							epoch: 99,
							commit_hlc,
							payload_hash: [seq as u8; 32],
							e_safe_index: seq + 2,
							durability: crate::schema::DurabilityLevel::QUORUM,
							_pad: [0; 7],
						},
						tenant_id: TenantId::ZERO,
						key_identity: Identity::blake3_tenant(
							TenantId::ZERO,
							format!("ready-audit-{seq}").as_bytes(),
						),
						charged_bytes: seq,
						record_count: 1,
					},
				))
				.await
				.unwrap();
		}

		stop.store(true, Ordering::Relaxed);
		poller.await.unwrap();
		assert!(
			!flapped.load(Ordering::Relaxed),
			"readiness reporter unexpectedly degraded during healthy audit materialization"
		);
		assert_eq!(node.audit_materialization.lag_entries(), 0);
		node.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn readiness_reporter_surfaces_local_persistence_distrust() {
		let dir = tempdir("readiness-local-persistence-distrust");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			24,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		wait_for_local_leader(&node).await;
		node.data_server
			.trip_local_persistence_distrust_for_test()
			.await;

		let snapshot = node.readiness_reporter().snapshot().await;
		assert!(!snapshot.is_ready());
		assert_eq!(
			snapshot.degraded_reason.as_deref(),
			Some("LocalPersistenceDistrust")
		);

		node.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn bootstrap_restores_local_persistence_distrust_fence() {
		let dir = tempdir("restart-local-persistence-distrust");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			26,
			dir.clone(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		wait_for_local_leader(&node).await;
		node.data_server
			.trip_local_persistence_distrust_for_test()
			.await;

		node.handles.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let recovered = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			26,
			dir,
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		let snapshot = recovered.readiness_reporter().snapshot().await;
		assert!(!snapshot.is_ready());
		assert_eq!(
			snapshot.degraded_reason.as_deref(),
			Some("LocalPersistenceDistrust")
		);
		// B6 persists the distrust fence and restores degraded
		// readiness across restart. Clean Raft abdication is still
		// a separate follow-up; this test only locks the degraded
		// readiness contract.
		tokio::time::sleep(Duration::from_millis(100)).await;
		let later = recovered.readiness_reporter().snapshot().await;
		assert!(!later.is_ready());
		assert_eq!(
			later.degraded_reason.as_deref(),
			Some("LocalPersistenceDistrust")
		);

		recovered.handles.abort();
	}

	#[tokio::test]
	async fn tcp_tls_bootstrap_requires_transport_tls_material() {
		let result = WendelonNode::bootstrap_with_provider_at_tier(
			NodeBootstrapConfig::dev(9, tempdir("tcp-tls-missing"), "127.0.0.1:0".into()),
			Arc::new(NoDialTransportProvider),
			TransportTier::TcpTls,
		)
		.await;
		let err = match result {
			Ok(_) => panic!("expected TcpTls bootstrap without transport TLS to fail"),
			Err(err) => err,
		};
		assert_eq!(err.kind(), ErrorKind::InvalidInput);
		assert!(
			err.to_string()
				.contains("TcpTls bootstrap requires transport_tls")
		);
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
			.client_write(WendelonRequest::Finalize(
				crate::consensus::TenantFinalization {
					marker: FinalizationMarker {
						log_index: 2,
						term: 1,
						epoch: 99,
						commit_hlc,
						payload_hash: [0; 32],
						e_safe_index: 2,
						durability: crate::schema::DurabilityLevel::QUORUM,
						_pad: [0; 7],
					},
					tenant_id: TenantId::ZERO,
					key_identity: Identity::blake3_tenant(TenantId::ZERO, b"startup-finalize"),
					charged_bytes: 0,
					record_count: 1,
				},
			))
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

		node.handles.abort();
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
		wait_for_default_tenant_kek(&recovered).await;

		recovered.handles.abort();
	}

	#[tokio::test]
	async fn single_node_bootstrap_reconciles_tiered_slabs_from_recovered_state() {
		let dir = tempdir("restart-tiered");
		let l2_dir = tempdir("restart-tiered-l2");
		let mut cfg = NodeBootstrapConfig::dev(12, dir.clone(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.clone(),
		});
		park_background_tiering(&mut cfg);
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();

		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;

		let tenant_id = TenantId::ZERO;
		let tiered_key = Identity::blake3(b"tiered-row");
		let active_key = Identity::blake3(b"active-row");
		let tiered_bytes = b"tiered-row".to_vec();
		node.slab
			.append(
				tenant_id,
				tiered_key,
				HybridTimestamp {
					physical_ms: 10,
					logical: 0,
					node_id: node.local_node.id,
				},
				&tiered_bytes,
			)
			.await
			.unwrap();
		node.slab
			.append(
				tenant_id,
				active_key,
				HybridTimestamp {
					physical_ms: 20,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"active-row",
			)
			.await
			.unwrap();
		node.slab.fsync_all_local_slabs().await.unwrap();

		let sealed = node.slab.list_sealed_slabs().await;
		assert_eq!(sealed.len(), 1, "expected one sealed slab after rotation");
		let sealed_id = sealed[0].slab_id;
		let raw = node.slab.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = node
			.slab
			.encrypt_tiered_object_blob(sealed_id, &raw)
			.unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let l2_uri = l2
			.put(
				&format!("sealed/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();

		node.raft
			.client_write(WendelonRequest::TierSlab(TieredSlab {
				slab_id: sealed_id,
				l2_uri,
				payload_hash,
				is_encrypted: true,
				tenant_id: Some(tenant_id),
				tenant_kek_id: Some(encrypted.tenant_kek_id),
				stats_format_version: encrypted
					.stats_locator
					.map(|locator| locator.stats_format_version),
				stats_block_len: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_len),
				stats_block_hash128: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_hash128),
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			}))
			.await
			.unwrap();
		assert!(node.slab.is_tiered(sealed_id).await);
		assert!(
			!dir.join(format!("slab_{sealed_id:016x}.wnd")).exists(),
			"tiered slab should be unlinked from L1 after apply"
		);

		node.handles.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let redb_path = dir.join("storage_index.redb");
		if redb_path.exists() {
			std::fs::remove_file(&redb_path).unwrap();
		}

		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		wait_for_ready(&recovered).await;
		assert!(recovered.slab.is_tiered(sealed_id).await);
		let location = recovered
			.slab
			.lookup(tenant_id, tiered_key)
			.expect("tiered key should be restored from recovered state");
		assert_eq!(location.slab_id, sealed_id);
		assert_eq!(recovered.slab.read(location).await.unwrap(), tiered_bytes);

		recovered.handles.abort();
	}

	#[tokio::test]
	async fn recovery_blocks_readiness_when_tiered_reference_validation_fails() {
		let dir = tempdir("restart-tiered-missing-l2");
		let l2_dir = tempdir("restart-tiered-missing-l2-store");
		let mut cfg = NodeBootstrapConfig::dev(15, dir.clone(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.clone(),
		});
		park_background_tiering(&mut cfg);
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();

		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;

		let tenant_id = TenantId::ZERO;
		let tiered_key = Identity::blake3(b"missing-tiered-row");
		let active_key = Identity::blake3(b"missing-tiered-active-row");
		node.slab
			.append(
				tenant_id,
				tiered_key,
				HybridTimestamp {
					physical_ms: 10,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"tiered-row",
			)
			.await
			.unwrap();
		node.slab
			.append(
				tenant_id,
				active_key,
				HybridTimestamp {
					physical_ms: 20,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"active-row",
			)
			.await
			.unwrap();
		node.slab.fsync_all_local_slabs().await.unwrap();

		let sealed_id = node.slab.list_sealed_slabs().await[0].slab_id;
		let raw = node.slab.read_sealed_raw(sealed_id).await.unwrap();
		let encrypted = node
			.slab
			.encrypt_tiered_object_blob(sealed_id, &raw)
			.unwrap();
		let payload_hash = crate::hash::content::digest(&encrypted.bytes);
		let l2_uri = l2
			.put(
				&format!("sealed/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes,
			)
			.await
			.unwrap();

		node.raft
			.client_write(WendelonRequest::TierSlab(TieredSlab {
				slab_id: sealed_id,
				l2_uri: l2_uri.clone(),
				payload_hash,
				is_encrypted: true,
				tenant_id: Some(tenant_id),
				tenant_kek_id: Some(encrypted.tenant_kek_id),
				stats_format_version: encrypted
					.stats_locator
					.map(|locator| locator.stats_format_version),
				stats_block_len: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_len),
				stats_block_hash128: encrypted
					.stats_locator
					.map(|locator| locator.stats_block_hash128),
				tombstone_metadata: TombstoneMetadata::None,
				supersession_metadata: SupersessionMetadata::None,
			}))
			.await
			.unwrap();

		l2.delete(&l2_uri).await.unwrap();

		node.handles.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let snapshot = recovered.readiness_reporter().snapshot().await;
			if let Some(reason) = snapshot.degraded_reason.as_deref() {
				if reason == "RestoreValidationPending" {
					assert!(
						!snapshot.is_ready(),
						"restore validation must block readiness while validation is pending"
					);
					assert!(
						tokio::time::Instant::now() < deadline,
						"timed out waiting for restore validation failure"
					);
					tokio::time::sleep(Duration::from_millis(20)).await;
					continue;
				}
				assert!(
					reason.starts_with("RestoreValidationFailed:"),
					"unexpected degraded reason: {reason}"
				);
				assert!(
					!snapshot.is_ready(),
					"restore validation failure must keep readiness blocked"
				);
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for restore validation failure"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}

		recovered.handles.abort();
	}

	#[tokio::test]
	async fn tenant_kek_rotation_is_raft_committed_and_old_l2_slabs_remain_readable() {
		let dir = tempdir("tenant-kek-rotation");
		let l2_dir = tempdir("tenant-kek-rotation-l2");
		let mut cfg = NodeBootstrapConfig::dev(14, dir.clone(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.clone(),
		});
		park_background_tiering(&mut cfg);
		let object_store = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;

		let old_key = Identity::blake3(b"pre-rotation-tiered");
		let filler_key = Identity::blake3(b"rotation-filler");
		let old_loc = node
			.slab
			.append(
				TenantId::ZERO,
				old_key,
				HybridTimestamp {
					physical_ms: 1,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"before-rotation",
			)
			.await
			.unwrap();
		let filler_loc = node
			.slab
			.append(
				TenantId::ZERO,
				filler_key,
				HybridTimestamp {
					physical_ms: 2,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"seal-slab-zero",
			)
			.await
			.unwrap();

		let sweeper = SlabSweeper::new(
			node.slab.clone(),
			object_store.clone(),
			Arc::new(RaftCommitter::new(node.raft.clone())),
			node.state_machine.clone(),
			SweeperConfig {
				scan_interval: Duration::from_millis(10),
				min_slab_bytes: 0,
				max_upload_bytes_per_sec: None,
				key_prefix: "sealed/".into(),
				max_plaintext_rewrites_per_pass: 1,
				max_dek_rewraps_per_pass: 1,
			},
		);
		let first_report = sweeper.sweep_once().await.unwrap();
		assert!(first_report.tiered >= 1);
		assert!(node.slab.is_tiered(old_loc.slab_id).await);

		let old_pointer = node.slab.tier_pointer(old_loc.slab_id).await.unwrap();
		let old_blob = object_store.get(&old_pointer.l2_uri).await.unwrap();
		let old_footer = node
			.crypto
			.encrypted_footer_summary(&old_blob)
			.unwrap()
			.expect("tiered object must be envelope-encrypted");
		assert!(
			!old_blob
				.windows(b"before-rotation".len())
				.any(|w| w == b"before-rotation")
		);
		assert_eq!(node.slab.read(old_loc).await.unwrap(), b"before-rotation");

		let rotated_kek_id = node.rotate_tenant_kek(TenantId::ZERO).await.unwrap();
		let rotated_snapshot = node.state_machine.state_snapshot().await;
		assert_eq!(
			rotated_snapshot
				.tenant_keysets
				.get(&TenantId::ZERO)
				.unwrap()
				.active_tenant_kek_id,
			Some(rotated_kek_id)
		);
		assert_eq!(
			node.crypto.active_tenant_kek_id(TenantId::ZERO).unwrap(),
			rotated_kek_id
		);

		let _ = node
			.slab
			.append(
				TenantId::ZERO,
				Identity::blake3(b"post-rotation-write"),
				HybridTimestamp {
					physical_ms: 3,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"after-rotation",
			)
			.await
			.unwrap();
		let second_report = sweeper.sweep_once().await.unwrap();
		assert!(second_report.tiered >= 1);
		assert_eq!(second_report.rewrapped_tiered_deks, 1);
		assert_eq!(second_report.dek_rewrap_failures, 0);
		assert!(node.slab.is_tiered(filler_loc.slab_id).await);

		let rewritten_old_pointer = node.slab.tier_pointer(old_loc.slab_id).await.unwrap();
		assert_ne!(rewritten_old_pointer.l2_uri, old_pointer.l2_uri);
		let rewritten_old_blob = object_store
			.get(&rewritten_old_pointer.l2_uri)
			.await
			.unwrap();
		let rewritten_old_footer = node
			.crypto
			.encrypted_footer_summary(&rewritten_old_blob)
			.unwrap()
			.expect("re-wrapped tiered object must remain envelope-encrypted");
		assert_eq!(rewritten_old_footer.tenant_kek_id, rotated_kek_id);
		assert_ne!(rewritten_old_footer.tenant_kek_id, old_footer.tenant_kek_id);
		assert!(
			object_store.get(&old_pointer.l2_uri).await.is_err(),
			"old tenant-KEK object should be deleted after re-wrap"
		);

		let new_pointer = node.slab.tier_pointer(filler_loc.slab_id).await.unwrap();
		let new_blob = object_store.get(&new_pointer.l2_uri).await.unwrap();
		let new_footer = node
			.crypto
			.encrypted_footer_summary(&new_blob)
			.unwrap()
			.expect("post-rotation tiered object must be envelope-encrypted");
		assert_ne!(old_footer.tenant_kek_id, new_footer.tenant_kek_id);
		assert_eq!(new_footer.tenant_kek_id, rotated_kek_id);
		assert_eq!(node.slab.read(old_loc).await.unwrap(), b"before-rotation");
	}

	#[tokio::test]
	async fn single_node_bootstrap_starts_background_tiering_when_l2_is_configured() {
		let dir = tempdir("background-tiering");
		let l2_dir = tempdir("background-tiering-l2");
		let mut cfg = NodeBootstrapConfig::dev(15, dir.clone(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs { root: l2_dir });
		cfg.tiering.scan_interval = Duration::from_millis(20);
		let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();

		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;

		let tenant_id = TenantId::ZERO;
		let tiered_key = Identity::blake3(b"background-tiered");
		node.slab
			.append(
				tenant_id,
				tiered_key,
				HybridTimestamp {
					physical_ms: 1,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"background-tiered",
			)
			.await
			.unwrap();
		node.slab
			.append(
				tenant_id,
				Identity::blake3(b"background-tiered-filler"),
				HybridTimestamp {
					physical_ms: 2,
					logical: 0,
					node_id: node.local_node.id,
				},
				b"seal-slab-zero",
			)
			.await
			.unwrap();

		let location = node
			.slab
			.lookup(tenant_id, tiered_key)
			.expect("tiered key should remain indexed");
		wait_for_tiered_slab(&node, location.slab_id).await;

		assert_eq!(
			node.slab.read(location).await.unwrap(),
			b"background-tiered"
		);
		let pointer = node
			.slab
			.tier_pointer(location.slab_id)
			.await
			.expect("tier pointer should exist after background sweep");
		assert!(pointer.l2_uri.starts_with("file://"));
		assert!(
			!dir.join(format!("slab_{:016x}.wnd", location.slab_id))
				.exists(),
			"tiered slab should be unlinked from local L1 after the background sweep"
		);
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
			node.crypto.clone(),
			node.local_node,
			node.epoch_tx.clone(),
			epoch_rx,
			factory,
			node.pool.clone(),
			1,
			LEASE_TTL_MS,
			node.audit_sink.clone(),
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
				crate::network::server::RequestContext {
					remote: crate::schema::NodeId {
						id: 0,
						_reserved: 0,
					},
					actor: crate::network::tls::ActorIdentity::data_node(crate::schema::NodeId {
						id: 0,
						_reserved: 0,
					}),
				},
				frame,
			)
			.await
			.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidData);
	}
}
