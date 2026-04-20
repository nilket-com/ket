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
//! the multi-node path uses. When `[transport.tls]` is configured,
//! bootstrap binds a real QUIC endpoint plus the TCP+TLS fallback
//! listener, advertises both tiers, and routes outbound dials through
//! the shared transport factory; otherwise tests and single-node dev
//! flows can still supply an in-process provider.
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

use crate::audit::{AuditClockDriftSubscriber, AuditSink, LocalAuditSink};
use crate::consensus::WendelonRaft;
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
	CheckpointCoordinator, DurableBootstrapMode, WendelonLogStore, WendelonStateMachine,
};
use crate::crypto::{CryptoCacheConfig, CryptoManager, RootKekProviderConfig};
use crate::metrics::observability::{
	DrainState, ListenerAddresses, ReadinessReporter, ReadinessSnapshot,
};
use crate::network::control_server::{
	JoinClusterRequestWire, JoinClusterResponseWire, WendelonControlServer, select_bootstrap_epoch,
};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::data_server::{
	MIGRATION_SWEEP_FAILURE_BACKOFF, MIGRATION_SWEEP_INTERVAL, MIGRATION_SWEEP_MAX_PER_PASS,
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
	HrftService, LifecycleService, ServerLifecycle, serve, serve_tcp_tls,
};
use crate::network::tcp_tls_transport::TcpTlsTransportProvider;
use crate::network::tls::{TlsMaterial, rustls_client_config, rustls_server_config};
use crate::network::transfer_signing::{
	TransferAuthority, dev_transfer_signing_secret, transfer_verifying_key_bytes,
};
use crate::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportConnection,
	TransportProvider, TransportTier, TransportTierSet,
};
use crate::network::wire;
use crate::schema::{FrameHeader, MsgType, NodeId, ProtocolVersionWindow, TenantId, TenantKekId};
use crate::storage::GroupCommitConfig;
use crate::storage::ManagerTierObserver;
use crate::storage::io_uring::{IoRingHandle, spawn_io_thread};
use crate::storage::l2::ObjectStoreConfig;
use crate::storage::manager::{SlabManager, SlabManagerConfig};
use async_trait::async_trait;
use openraft::{BasicNode, Config as OpenRaftConfig};
use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;

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
	pub listeners: ListenerAddresses,
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
	/// Fresh-cluster bootstrap or explicit join semantics.
	pub cluster: ClusterBootstrapConfig,
	/// Optional L2 backend for sealed-slab tiering.
	pub l2: Option<ObjectStoreConfig>,
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
	/// Maximum tolerated drift between the local physical clock and
	/// the cluster median before the node aborts.
	pub drift_max_offset_ms: u64,
	/// Optional three-listener observability topology.
	pub observability: Option<ObservabilityConfig>,
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
			group_commit: GroupCommitConfig::default(),
			rf: 1,
			io_depth: 64,
			advertise_address,
			lease_signing_secret,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			cluster: ClusterBootstrapConfig::Bootstrap,
			l2: None,
			root_kek_provider: RootKekProviderConfig::DevInsecure,
			crypto_cache: CryptoCacheConfig::default(),
			protocol: ProtocolVersionWindow::default(),
			transport_tls: None,
			drift_max_offset_ms: crate::consensus::monitor::MAX_OFFSET_MS,
			observability: None,
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
					"slab_max_bytes={} exceeds the W4 hard ceiling of {} bytes",
					self.slab_max_bytes,
					crate::crypto::SLAB_MAX_BYTES_HARD_CEILING
				),
			));
		}
		#[cfg(feature = "fips")]
		if matches!(self.root_kek_provider, RootKekProviderConfig::DevInsecure) {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"crypto.root_kek_provider=dev-insecure is disabled in fips builds",
			));
		}
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
		if self.io_depth == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"io_depth must be greater than zero",
			));
		}
		if self.advertise_address.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"advertise_address must not be empty",
			));
		}
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
			if self.transport_tls.is_none() {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					"observability requires transport_tls for the mTLS ready and metrics listeners",
				));
			}
			for (field, value) in [
				(
					"observability.listeners.health",
					obs.listeners.health.as_str(),
				),
				(
					"observability.listeners.ready",
					obs.listeners.ready.as_str(),
				),
				(
					"observability.listeners.metrics",
					obs.listeners.metrics.as_str(),
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
	pub io: IoRingHandle,
	pub epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	pub protocol: ProtocolVersionWindow,
	pub lifecycle: ServerLifecycle,
	pub audit_sink: Arc<dyn AuditSink>,
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
	pub epoch_reconciler: JoinHandle<()>,
	pub migration_sweeper: JoinHandle<()>,
	pub tenant_kek_seeder: JoinHandle<()>,
	pub checkpoint_emitter: JoinHandle<()>,
}

impl NodeHandles {
	pub fn abort(&self) {
		self.ring_updater.abort();
		self.drift_monitor.abort();
		self.eviction_watchdog.abort();
		self.lease_manager.abort();
		self.epoch_reconciler.abort();
		self.migration_sweeper.abort();
		self.tenant_kek_seeder.abort();
		self.checkpoint_emitter.abort();
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		self.abort();
		await_background_task(self.ring_updater, "ring updater").await?;
		await_background_task(self.drift_monitor, "drift monitor").await?;
		await_background_task(self.eviction_watchdog, "eviction watchdog").await?;
		await_background_task(self.lease_manager, "lease manager").await?;
		await_background_task(self.epoch_reconciler, "epoch reconciler").await?;
		await_background_task(self.migration_sweeper, "migration sweeper").await?;
		await_background_task(self.tenant_kek_seeder, "tenant KEK seeder").await?;
		await_background_task(self.checkpoint_emitter, "checkpoint emitter").await?;
		Ok(())
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
				Arc::new(QuicTransportProvider::with_cluster_domain(
					Arc::new(quic_endpoint.clone()),
					server_name.clone(),
					&cluster_domain,
				)),
			);
			providers.insert(
				TransportTier::TcpTls,
				Arc::new(TcpTlsTransportProvider::with_cluster_domain(
					rustls_client_config(&material)?,
					server_name,
					&cluster_domain,
				)),
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

	pub async fn rotate_tenant_kek(&self, tenant_id: TenantId) -> std::io::Result<TenantKekId> {
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
		)
		.await
	}

	async fn bootstrap_with_transport_stack(
		cfg: NodeBootstrapConfig,
		local_tiers: TransportTierSet,
		providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
		startup_listeners: Vec<BootstrapListener>,
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

		// 1. Shared buffer pool (HRFT frames + postcard bodies).
		let pool = BufPool::new(256, 1024 * 1024);

		// 2. io_uring thread for slab + WAL I/O.
		let io = spawn_io_thread(cfg.io_depth)?;

		// 3. L1 slab storage.
		let l2 = cfg.l2.as_ref().map(ObjectStoreConfig::build).transpose()?;
		let crypto = Arc::new(CryptoManager::new_with_cache_config(
			cfg.root_kek_provider.build(&cfg.data_dir)?,
			cfg.crypto_cache,
		));
		let slab: Arc<SlabManager> = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: cfg.data_dir.clone(),
				max_slab_bytes: cfg.slab_max_bytes,
				io: io.clone(),
				l2,
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				group_commit: cfg.group_commit.clone(),
				crypto: crypto.clone(),
			})
			.await?,
		);

		// 4. Durable Raft storage + recovered state machine.
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		let log_store =
			WendelonLogStore::open(&wal_path, &vote_path, &snapshot_path, io.clone()).await?;
		let bootstrap_mode = log_store.bootstrap_mode().await;
		let checkpoint_coordinator = CheckpointCoordinator::new(log_store.clone());
		let tier_observer = Arc::new(ManagerTierObserver::new(slab.clone()));
		let state_machine = log_store
			.recovered_state_machine()
			.await?
			.with_tier_observer(tier_observer)
			.with_tenant_kek_observer(crypto.clone())
			.with_checkpoint_observer(checkpoint_coordinator.clone());
		let checkpoint_emitter = checkpoint_coordinator.spawn();

		// 5. Seed membership epoch from durable history if present;
		//    otherwise fall back to the single-node genesis epoch.
		//    If recovery finds a committed-but-unseeded transition,
		//    `select_bootstrap_epoch` holds visibility at the prior
		//    epoch until the reconciler finishes auto-seeding.
		let fallback_epoch =
			seed_single_node_epoch(local_node, &cfg.advertise_address, local_tiers);
		let recovered_state = state_machine.state_snapshot().await;
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
		let audit_sink: Arc<dyn AuditSink> =
			Arc::new(LocalAuditSink::open(cfg.data_dir.join("audit.ndjson"))?);
		crypto.attach_audit_sink(local_node, audit_sink.clone())?;
		audit_sink.emit(crate::audit::AuditEvent::new(
			crate::audit::AuditAuthority::NodeLocal,
			"crypto.startup",
			local_node,
			format!(
				"selected_cipher={} cipher_detection_method={} root_kek_provider={} fips={} build_profile={} mlock={}",
				crypto.selected_cipher_label(),
				crypto.selected_cipher_detection_method(),
				crypto.root_kek_provider_label(),
				cfg!(feature = "fips"),
				if cfg!(debug_assertions) {
					"debug"
				} else {
					"release"
				},
				crypto.mlock_status_label(),
			),
		))?;
		let state_machine = state_machine.with_audit_materializer(local_node, audit_sink.clone());
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
			.with_hlc(hlc.clone())
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
					cluster_domain,
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
					cluster_domain,
				)),
			})
			.collect::<Vec<_>>();
		let epoch_reconciler = control_server.clone().spawn_epoch_reconciler();

		// 14. Fresh nodes either initialize a one-node cluster or
		//     join an existing leader through the explicit Control
		//     Ring join path. Durable recovery skips this step.
		if bootstrap_mode == DurableBootstrapMode::FreshBootstrap {
			match &cfg.cluster {
				ClusterBootstrapConfig::Bootstrap => {
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
				ClusterBootstrapConfig::Join { seeds } => {
					request_join_via_seeds(
						local_node,
						&cfg.advertise_address,
						local_tiers,
						advertised_roles(),
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
			)
			.await?
			{
				DefaultTenantKekSeedState::Complete => {}
				DefaultTenantKekSeedState::Retry => {
					return Err(Error::new(
						ErrorKind::Other,
						"lost local leadership while seeding the default tenant KEK during fresh bootstrap",
					));
				}
			}
		}
		let tenant_kek_seeder = if state_machine
			.state_snapshot()
			.await
			.tenant_keysets
			.contains_key(&TenantId::ZERO)
		{
			tokio::spawn(async {})
		} else {
			spawn_default_tenant_kek_seeder(
				raft.clone(),
				state_machine.clone(),
				crypto.clone(),
				local_node,
			)
		};

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
		let migration_sweeper = data_server.clone().spawn_migration_sweeper(
			MIGRATION_SWEEP_INTERVAL,
			MIGRATION_SWEEP_FAILURE_BACKOFF,
			MIGRATION_SWEEP_MAX_PER_PASS,
		);

		Ok(Self {
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
			io,
			epoch_tx,
			protocol: cfg.protocol,
			lifecycle,
			audit_sink,
			clock_drift_events: clock_drift_rx,
			transport_servers,
			handles: NodeHandles {
				ring_updater,
				drift_monitor,
				eviction_watchdog,
				lease_manager,
				epoch_reconciler,
				migration_sweeper,
				tenant_kek_seeder,
				checkpoint_emitter,
			},
		})
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
			cluster_domain.into(),
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
		let cluster_domain = cluster_domain.into();
		let task = tokio::spawn({
			let lifecycle = lifecycle.clone();
			async move {
				serve_dyn_tcp_tls(listener, acceptor, svc, pool, lifecycle, cluster_domain).await
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
			lifecycle: self.lifecycle.clone(),
		}
	}
}

#[derive(Clone)]
pub struct NodeReadinessView {
	local_node: NodeId,
	epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	raft: WendelonRaft,
	lifecycle: ServerLifecycle,
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
		ReadinessSnapshot {
			node_id: self.local_node.id,
			epoch: epoch.epoch,
			leader_id: self.raft.current_leader().await,
			cluster_members: members,
			drain_state,
			audit_materialization_lag_entries: 0,
			degraded_reason: None,
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
	cluster_domain: String,
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
	serve(
		endpoint,
		Arc::new(DynService(service)),
		pool,
		lifecycle,
		cluster_domain,
	)
	.await
}

async fn serve_dyn_tcp_tls(
	listener: TcpListener,
	acceptor: TlsAcceptor,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	cluster_domain: String,
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

	serve_tcp_tls(
		listener,
		acceptor,
		Arc::new(DynService(service)),
		pool,
		lifecycle,
		cluster_domain,
	)
	.await
}

fn spawn_quic_server(
	endpoint: quinn::Endpoint,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	lifecycle: ServerLifecycle,
	cluster_domain: String,
) -> QuicServerHandle {
	let task_endpoint = endpoint.clone();
	let task = tokio::spawn({
		let lifecycle = lifecycle.clone();
		async move { serve_dyn(task_endpoint, service, pool, lifecycle, cluster_domain).await }
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
	cluster_domain: String,
) -> TcpTlsServerHandle {
	let task = tokio::spawn({
		let lifecycle = lifecycle.clone();
		async move {
			serve_dyn_tcp_tls(listener, acceptor, service, pool, lifecycle, cluster_domain).await
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
		},
	);
	MembershipEpoch { epoch: 1, peers }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DefaultTenantKekSeedState {
	Complete,
	Retry,
}

async fn seed_default_tenant_kek_if_missing(
	raft: &WendelonRaft,
	state_machine: &WendelonStateMachine,
	crypto: &CryptoManager,
	local_node: NodeId,
) -> std::io::Result<DefaultTenantKekSeedState> {
	if state_machine
		.state_snapshot()
		.await
		.tenant_keysets
		.contains_key(&TenantId::ZERO)
	{
		return Ok(DefaultTenantKekSeedState::Complete);
	}
	if raft.current_leader().await != Some(local_node.id) {
		return Ok(DefaultTenantKekSeedState::Retry);
	}
	let rotation = crypto.prepare_rotation(TenantId::ZERO)?;
	match commit_install_tenant_kek_version(raft, rotation, "failed to seed default tenant KEK")
		.await
	{
		Ok(()) => Ok(DefaultTenantKekSeedState::Complete),
		Err(err) if raft.current_leader().await != Some(local_node.id) => {
			let _ = err;
			Ok(DefaultTenantKekSeedState::Retry)
		}
		Err(err) => Err(err),
	}
}

fn spawn_default_tenant_kek_seeder(
	raft: WendelonRaft,
	state_machine: WendelonStateMachine,
	crypto: Arc<CryptoManager>,
	local_node: NodeId,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		loop {
			match seed_default_tenant_kek_if_missing(
				&raft,
				&state_machine,
				crypto.as_ref(),
				local_node,
			)
			.await
			{
				Ok(DefaultTenantKekSeedState::Complete) => return,
				Ok(DefaultTenantKekSeedState::Retry) => {}
				Err(err) => {
					tracing::warn!(
						node_id = local_node.id,
						error = %err,
						"default tenant KEK seeding is still pending"
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
	use crate::consensus::raft_storage::SmState;
	use crate::schema::{FinalizationMarker, HybridTimestamp, Identity, SnapshotHeader, TenantId};
	use crate::storage::{RaftCommitter, SlabSweeper, StorageEngine, SweeperConfig};
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
				"timed out waiting for default tenant KEK to be provisioned"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	fn control_artifact_paths_for_test(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
		(
			dir.join("control.wal"),
			dir.join("vote.wnd"),
			dir.join("snapshot.wnd"),
		)
	}

	fn write_snapshot_artifact(path: &std::path::Path, state: &SmState) {
		let mut snapshot_state = state.clone();
		if let Some(log_id) = snapshot_state.last_applied_log_id.as_mut() {
			*log_id = openraft::LogId::new(
				openraft::CommittedLeaderId::new(log_id.leader_id.term, 0),
				log_id.index,
			);
		}
		let body = postcard::to_allocvec(&snapshot_state).unwrap();
		let last_applied = snapshot_state.last_applied_log_id;
		let header = SnapshotHeader {
			last_included_index: last_applied.map(|id| id.index).unwrap_or(0),
			last_included_term: last_applied.map(|id| id.leader_id.term).unwrap_or(0),
			last_included_epoch: 0,
			snapshot_hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			state_sha256: *blake3::hash(&body).as_bytes(),
			cache_checkpoint_len: 0,
			_pad: [0; 4],
		};
		let mut out = Vec::with_capacity(std::mem::size_of::<SnapshotHeader>() + body.len());
		out.extend_from_slice(bytemuck::bytes_of(&header));
		out.extend_from_slice(&body);
		std::fs::write(path, out).unwrap();
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
			"default tenant KEK must be seeded during bootstrap"
		);
		let audit = read_audit_log(&dir);
		assert!(audit.contains("\"event_type\":\"crypto.startup\""));
		assert!(audit.contains(node.crypto.selected_cipher_label()));
		assert!(audit.contains(node.crypto.selected_cipher_detection_method()));
		assert!(audit.contains(node.crypto.root_kek_provider_label()));
		assert!(audit.contains("fips=false"));
		assert!(audit.contains("build_profile=debug"));
		assert!(audit.contains("mlock="));
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
			.client_write(WendelonRequest::Finalize(FinalizationMarker {
				log_index: 2,
				term: 1,
				epoch: 99,
				commit_hlc,
				payload_hash: [0; 32],
				e_safe_index: 2,
				durability: crate::schema::DurabilityLevel::QUORUM,
				_pad: [0; 7],
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
	async fn recovery_upgrade_seeds_default_tenant_kek_when_snapshot_predates_w4() {
		let dir = tempdir("upgrade-seeds-default-tenant-kek");
		let cfg = NodeBootstrapConfig::dev(15, dir.clone(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();

		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;
		let mut upgraded_snapshot = node.state_machine.state_snapshot().await;
		upgraded_snapshot.tenant_keysets.clear();

		node.handles.abort();
		drop(node);
		tokio::time::sleep(Duration::from_millis(25)).await;

		let (wal_path, _vote_path, snapshot_path) = control_artifact_paths_for_test(&dir);
		if wal_path.exists() {
			std::fs::remove_file(&wal_path).unwrap();
		}
		write_snapshot_artifact(&snapshot_path, &upgraded_snapshot);

		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		wait_for_local_leader(&recovered).await;
		wait_for_default_tenant_kek(&recovered).await;
		assert!(
			recovered
				.state_machine
				.state_snapshot()
				.await
				.tenant_keysets
				.contains_key(&TenantId::ZERO),
			"default tenant KEK must be auto-seeded for recovered pre-W4 state"
		);

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
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();

		wait_for_local_leader(&node).await;

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
		let payload_hash = *blake3::hash(&raw).as_bytes();
		let l2_uri = l2
			.put(&format!("sealed/slab_{sealed_id:016x}.wnd"), &raw)
			.await
			.unwrap();

		node.raft
			.client_write(WendelonRequest::TierSlab(TieredSlab {
				slab_id: sealed_id,
				l2_uri,
				payload_hash,
				is_encrypted: true,
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
	async fn tenant_kek_rotation_is_raft_committed_and_old_l2_slabs_remain_readable() {
		let dir = tempdir("tenant-kek-rotation");
		let l2_dir = tempdir("tenant-kek-rotation-l2");
		let mut cfg = NodeBootstrapConfig::dev(14, dir.clone(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.clone(),
		});
		let object_store = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		wait_for_local_leader(&node).await;

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
		assert!(node.slab.is_tiered(filler_loc.slab_id).await);

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
