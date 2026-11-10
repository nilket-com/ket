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

mod identity;
mod startup;
#[cfg(feature = "test-support")]
mod test_support;
pub use identity::{IdentityKeyError, IdentityKeyRole};
#[cfg(feature = "test-support")]
pub use test_support::TestSupportFileOps;

use crate::audit::{
	AuditAuthority, AuditClockDriftSubscriber, AuditEvent, AuditMaterializationTracker,
	AuditOperation, AuditRootSigner, AuditSink, AuditSinksConfig,
	DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL, build_audit_sink_with_owner,
	spawn_signed_chain_root_rollup,
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
use crate::consensus::retention::{ControlRetention, RetentionGuard};
use crate::consensus::{AdvanceEpochRecord, WendelonRaft, WendelonRequest, WendelonResponse};
use crate::crypto::{CryptoCacheConfig, CryptoManager, RootKekProviderConfig};
use crate::integration::compute::{
	SchemaGeneration, SchemaIntentId, SchemaMutationKind, SchemaObjectKind, SchemaObjectRef,
	SchemaUpdateEnvelope,
};
use crate::metrics::IoUringClass;
use crate::metrics::observability::{
	DrainState, ListenerAddresses, ReadinessReporter, ReadinessSnapshot,
};
use crate::metrics::telemetry::{TraceExportConfig, TraceExportProtocol};
use crate::network::control_server::{
	JoinClusterRequestWire, JoinClusterResponseWire, WendelonControlServer,
	epoch_config_from_membership, select_bootstrap_epoch,
};
use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
use crate::network::data_server::{
	DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES, DEFAULT_TENANT_CACHE_REFRESH_INTERVAL,
	DROP_RECLAIM_SWEEP_INTERVAL, MIGRATION_SWEEP_FAILURE_BACKOFF, MIGRATION_SWEEP_INTERVAL,
	MIGRATION_SWEEP_MAX_PER_PASS, PENDING_FINALIZATION_RETRY_INTERVAL, WendelonDataServer,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::lease_signing::{
	LeaseAuthority, dev_lease_signing_secret, lease_verifying_key_bytes,
};
use crate::network::pool::BufPool;
use crate::network::quic_transport::{
	QuicTransportProvider, canonical_server_name_resolver, endpoint_from_tls_with_peer_roots,
};
use crate::network::ring::RingRouter;
use crate::network::ring::membership_from_epoch_config;
use crate::network::router::{MsgRouter, MsgRouterBuilder};
use crate::network::server::{
	HrftService, LifecycleService, ServerAdmissionConfig, ServerLifecycle,
	serve_tcp_tls_with_admission, serve_with_admission,
};
use crate::network::tcp_tls_transport::TcpTlsTransportProvider;
use crate::network::tls::{
	ActorTrustRoots, TlsMaterial, rustls_client_config, rustls_server_config_with_peer_roots,
};
use crate::network::transfer_signing::{
	TransferAuthority, dev_transfer_signing_secret, transfer_verifying_key_bytes,
};
use crate::network::transport::{
	MembershipEpoch, NodeCapability, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities,
	TransportConnection, TransportProvider, TransportTier, TransportTierSet,
};
use crate::network::wire;
use crate::network::{audit_root_verifying_key_bytes, dev_audit_root_signing_secret};
use crate::schema::{
	FrameHeader, MsgType, NodeId, ProtocolVersionWindow, SecondaryIndexDescriptor,
	SecondaryIndexLifecycleComplete, SecondaryIndexState, SlabId, TenantId, TenantKekId,
	encode_secondary_index_lifecycle_complete,
};
#[cfg(test)]
use crate::schema::{SupersessionMetadata, TombstoneMetadata};
use crate::storage::encoding::{
	EncodingComputeConfig, EncodingWriterConfig, L2ChunkLayoutConfig, L2EncodingFetchConfig,
};
use crate::storage::io_uring::{IoRingHandle, spawn_io_thread_named_for_class};
use crate::storage::l2::ObjectStoreConfig;
use crate::storage::manager::{RehydrationPolicy, SlabManager, SlabManagerConfig};
use crate::storage::ownership::DataDirectoryOwner;
use crate::storage::row_group::{L2RowGroupFetchConfig, RowGroupWriterConfig};
use crate::storage::{
	DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS, GroupCommitConfig, L2ColumnFetchConfig,
	LsmCompactionDriver, LsmCompactionService, LsmCompactionServiceControl, LsmCompactionWorker,
	LsmConfig, LsmGcDriver, LsmGcService, LsmGcWorker, ManagerTierObserver, MvccRetentionPolicy,
	RaftCommitter, SlabSweeper, SweeperConfig, TierCommitter,
};
use async_trait::async_trait;
use openraft::BasicNode;
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
/// The quota the bootstrap tenant receives, and ADR 0167's default for a
/// tenant an operator creates without naming one.
pub const DEFAULT_TENANT_QUOTA_BYTES: u64 = u64::MAX;
const SECONDARY_INDEX_LIFECYCLE_INTERVAL: Duration = Duration::from_secs(1);
const SECONDARY_INDEX_BACKFILL_SLABS_PER_TICK: usize = 8;

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
	#[cfg(test)]
	pub(crate) startup_probe: Option<startup_ownership_tests::StartupProbe>,
	#[cfg(test)]
	pub(crate) startup_fault: Option<startup_ownership_tests::StartupFault>,
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
	/// ADR 0692 §7 node budget for tier history, in bytes.
	pub tier_history_budget_bytes: u64,
	/// Replication factor published into the ring. Single-node
	/// deployments run at RF=1; production minimum is 3.
	pub rf: usize,
	/// Depth of the dedicated Raft WAL `io_uring` submission queue.
	pub wal_io_depth: u32,
	/// Depth of the dedicated slab-storage `io_uring` submission queue.
	pub slab_io_depth: u32,
	/// Bound for storage write drain and worker joins; timeout retains LOCK.
	pub storage_shutdown_timeout: Duration,
	/// File operations injected only by deterministic acceptance fixtures.
	#[cfg(feature = "test-support")]
	pub slab_file_ops: Option<TestSupportFileOps>,
	#[cfg(feature = "test-support")]
	pub join_promotion_gate:
		Option<crate::network::control_server::join_test_support::JoinPromotionGate>,
	/// ADR 079 cadence for unconditional CDC locality progress markers.
	pub cdc_locality_heartbeat_interval: Duration,
	/// ADR 080 HLC fencing window for Data Ring range leases. The
	/// renewal cadence is derived as one third of this duration.
	pub data_ring_lease_ttl: Duration,
	/// `host:port` this node publishes into the membership epoch —
	/// peers dial here when we advertise a transport tier.
	pub advertise_address: String,
	/// Ed25519 secret key used when this node is the control-ring
	/// leader and signs a `LeaseDescriptor`.
	pub lease_signing_secret: [u8; 32],
	/// Explicitly permit recognized insecure dev signing keys. This never bypasses
	/// local-key consistency or role/identity separation.
	pub allow_dev_keys: bool,
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
	/// Independent audit chain-root signing secret.
	pub audit_root_signing_secret: [u8; 32],
	/// Explicit audit root enrollment; historical spans require externally selected keys.
	pub trusted_audit_root_keys: BTreeMap<u32, [u8; 32]>,
	/// Fresh-cluster bootstrap or explicit join semantics.
	pub cluster: ClusterBootstrapConfig,
	/// Optional L2 backend for sealed-slab tiering.
	pub l2: Option<ObjectStoreConfig>,
	/// Background sweeper policy for L1 -> L2 migration and
	/// local-cache reclaim. Used only when `l2` is configured.
	pub tiering: SweeperConfig,
	/// Node-local maintenance switch for ADR 055 LSM compaction.
	/// A paused service keeps its task alive but starts no new jobs
	/// until resumed through the exposed service control.
	pub lsm_compaction_paused: bool,
	/// Control log retention. `Compacting` is openraft's default policy;
	/// `Full` retains the Control log from genesis. See
	/// [`crate::consensus::retention`].
	pub control_retention: ControlRetention,
	/// Not a setting: selected only through
	/// [`NodeBootstrapConfig::with_parent_decisions_for_disposable_store`], for
	/// disposable stores. See `consensus::decision_mode`.
	pub(crate) decision_mode: crate::consensus::decision_mode::DecisionMode,
	/// ADR 0699 S3a: this node's decision-signing secret and every node's
	/// verifying key, ParentDecisions only. Not settings either: unset, both are
	/// the development keys, which that mode requires.
	pub(crate) decision_signing_secret: Option<[u8; 32]>,
	pub(crate) trusted_decision_signers: BTreeMap<u32, [u8; 32]>,
	/// Node-local maintenance switch for physical MVCC retention reclaim.
	/// Skips ONLY the reclaim pass inside the migration sweeper: migrations
	/// and tombstoned-range reclaim still run, and read-floor semantics are
	/// untouched, so this suppresses work rather than changing what a read
	/// can see.
	pub mvcc_retention_paused: bool,
	/// Policy governing when hot L2 slabs are promoted back into
	/// local L1 storage.
	pub rehydration: RehydrationPolicy,
	/// Node-local ADR 044 L2 columnar fetch strategy config.
	pub l2_column_fetch: L2ColumnFetchConfig,
	/// Deployment-wide ADR 056 row-group writer config.
	pub row_group_writer: RowGroupWriterConfig,
	/// Node-local ADR 056 L2 row-group fetch strategy config.
	pub l2_row_group_fetch: L2RowGroupFetchConfig,
	/// Deployment-wide ADR 057 Encoding writer config.
	pub encoding_writer: EncodingWriterConfig,
	/// Deployment-wide ADR 061 authenticated chunk layout writer config.
	pub l2_chunk_layout: L2ChunkLayoutConfig,
	/// Node-local ADR 057 L2 Encoding materialization config.
	pub l2_encoding_fetch: L2EncodingFetchConfig,
	/// Node-local ADR 058 compute-over-encoded-data executor config.
	pub encoding_compute: EncodingComputeConfig,
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
	/// Counter dedup horizon: attempt horizon `H`, clock-skew allowance `S`
	/// (operators keep NTP within it) and expiry `E >= L_sdk + H + S`.
	pub counter_retention: crate::storage::counter::CounterRetentionConfig,
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
	/// # Security
	/// These deterministic keys are publicly derivable. Use only in tests and
	/// local development; never provision a reachable node with them.
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
		let audit_root_signing_secret = dev_audit_root_signing_secret(node_id);
		let trusted_audit_root_keys = BTreeMap::from([(
			node_id,
			audit_root_verifying_key_bytes(audit_root_signing_secret),
		)]);
		Self {
			#[cfg(test)]
			startup_probe: None,
			#[cfg(test)]
			startup_fault: None,
			node_id,
			data_dir,
			slab_max_bytes: 64 * 1024 * 1024,
			group_commit: GroupCommitConfig::default(),
			tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
			rf: 1,
			wal_io_depth: 64,
			slab_io_depth: 64,
			storage_shutdown_timeout: Duration::from_secs(30),
			#[cfg(feature = "test-support")]
			slab_file_ops: None,
			#[cfg(feature = "test-support")]
			join_promotion_gate: None,
			cdc_locality_heartbeat_interval: Duration::from_millis(
				DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS,
			),
			data_ring_lease_ttl: Duration::from_millis(u64::from(
				DEFAULT_DATA_RING_LEASE_TTL_MILLIS,
			)),
			advertise_address,
			lease_signing_secret,
			allow_dev_keys: true,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			audit_root_signing_secret,
			trusted_audit_root_keys,
			cluster: ClusterBootstrapConfig::Bootstrap,
			l2: None,
			tiering: SweeperConfig::default(),
			lsm_compaction_paused: false,
			control_retention: ControlRetention::Compacting,
			decision_mode: Default::default(),
			decision_signing_secret: None,
			trusted_decision_signers: BTreeMap::new(),
			mvcc_retention_paused: false,
			rehydration: RehydrationPolicy::default(),
			l2_column_fetch: L2ColumnFetchConfig::default(),
			row_group_writer: RowGroupWriterConfig::default(),
			l2_row_group_fetch: L2RowGroupFetchConfig::default(),
			encoding_writer: EncodingWriterConfig::default(),
			l2_chunk_layout: L2ChunkLayoutConfig::default(),
			l2_encoding_fetch: L2EncodingFetchConfig::default(),
			encoding_compute: EncodingComputeConfig::default(),
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
			counter_retention: crate::storage::counter::CounterRetentionConfig::default(),
			observability: None,
			audit: AuditSinksConfig::default(),
		}
	}

	pub fn with_protocol_window(mut self, current: u16, min_compatible: u16) -> Self {
		self.protocol = ProtocolVersionWindow::new(current, min_compatible);
		self
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.storage_shutdown_timeout.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"storage_shutdown_timeout must be positive",
			));
		}
		if self.storage_shutdown_timeout > Duration::from_secs(3600) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"storage_shutdown_timeout must be at most one hour",
			));
		}

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
		if self.cdc_locality_heartbeat_interval.is_zero() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"cdc_locality_heartbeat_interval must be greater than zero",
			));
		}
		DataRingLeaseTiming::try_from_duration(self.data_ring_lease_ttl)?;
		if self.advertise_address.trim().is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"advertise_address must not be empty",
			));
		}
		self.transport_admission.validate()?;
		identity::validate_identity_keys(self)?;
		if self.decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions {
			identity::resolve_decision_keys(self)?;
		}
		self.rehydration.validate()?;
		self.l2_column_fetch.validate()?;
		self.row_group_writer.validate()?;
		self.l2_row_group_fetch.validate()?;
		self.encoding_writer.validate()?;
		self.l2_chunk_layout.validate()?;
		self.l2_encoding_fetch.validate()?;
		self.encoding_compute.validate()?;
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
		// ADR 0699: ParentDecisions keeps every acknowledged version until a
		// certified cleanup removes it; a node MVCC retention window would let
		// the tier sweeper prune tiered versions, destroying acknowledged copies.
		if self.decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions
			&& !matches!(
				self.mvcc_retention_policy,
				crate::storage::retention::MvccRetentionPolicy::Disabled
			) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"ParentDecisions mode refuses a node MVCC retention window",
			));
		}
		self.counter_retention.validate()?;
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
	_directory_owner: DataDirectoryOwner,
	storage_shutdown_timeout: Duration,
	recovered_after_unclean_shutdown: bool,
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
	retention: Arc<RetentionGuard>,
	clock_drift_events: watch::Receiver<Option<ClockDriftEvent>>,
	transport_servers: Vec<NodeTransportServer>,
	pub handles: NodeHandles,
}

/// Background task handles. Explicit shutdown aborts and joins these tasks;
/// dropping a raw Tokio JoinHandle alone detaches its task.
impl NodeBootstrapConfig {
	/// ADR 0700: selects the ParentDecisions authority for a fresh disposable
	/// development store. Bootstrap refuses it unless development keys are
	/// allowed (a disposable or test profile), refuses an existing store of the
	/// other mode, and a store keeps the mode it was created with. The default
	/// stays today's authority.
	pub fn with_parent_decisions_for_disposable_store(mut self) -> Self {
		self.decision_mode = crate::consensus::decision_mode::DecisionMode::ParentDecisions;
		self
	}
}

/// ADR 0699 S3a: how often a ParentDecisions node checks its signer binding and,
/// as leader, the retry floor.
const DECISION_MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
/// ADR 0699 S5a, ADR 0706: how long an orderly close waits, in all, for the
/// lifecycle services (the decision maintenance loop and the LSM compaction
/// service) to finish their current step. One deadline shared by both, drained
/// concurrently. Past it a service is aborted and joined, and an interrupted
/// lifecycle permit then revokes the clean record, as it must.
const LIFECYCLE_SERVICES_DRAIN: std::time::Duration = std::time::Duration::from_secs(15);
/// ADR 0699 S3c: Resolve proposals per maintenance tick, at most.
const DECISION_RESOLVE_BATCH: usize = 16;

pub struct NodeHandles {
	pub ring_updater: JoinHandle<()>,
	pub drift_monitor: JoinHandle<()>,
	pub eviction_watchdog: JoinHandle<()>,
	pub lease_manager: JoinHandle<()>,
	pub pending_finalization_reconciler: JoinHandle<()>,
	pub cdc_finalization_release_observer: JoinHandle<()>,
	pub cdc_locality_heartbeat: JoinHandle<()>,
	pub epoch_reconciler: JoinHandle<()>,
	pub tenant_policy_reconciler: JoinHandle<()>,
	pub drop_reclaim_sweeper: JoinHandle<()>,
	pub migration_sweeper: JoinHandle<()>,
	/// ADR 0706: `None` once an orderly close drained it.
	pub lsm_compaction: Option<JoinHandle<()>>,
	pub lsm_compaction_control: LsmCompactionServiceControl,
	pub lsm_gc: JoinHandle<()>,
	pub op_driver: JoinHandle<()>,
	pub secondary_index_lifecycle: JoinHandle<()>,
	pub tiering_sweeper: Option<JoinHandle<()>>,
	pub restore_validation: Option<JoinHandle<()>>,
	pub audit_chain_root_rollup: JoinHandle<()>,
	pub default_tenant_seeder: JoinHandle<()>,
	pub checkpoint_emitter: JoinHandle<()>,
	/// ADR 0699 S3a: ParentDecisions only. `None` once an orderly close
	/// drained it.
	pub decision_maintenance: Option<JoinHandle<()>>,
	/// The bound on the lifecycle services' orderly stop.
	lifecycle_drain_bound: std::time::Duration,
}

impl NodeHandles {
	pub fn abort(&self) {
		self.ring_updater.abort();
		self.drift_monitor.abort();
		self.eviction_watchdog.abort();
		self.lease_manager.abort();
		self.pending_finalization_reconciler.abort();
		self.cdc_finalization_release_observer.abort();
		self.cdc_locality_heartbeat.abort();
		self.epoch_reconciler.abort();
		self.tenant_policy_reconciler.abort();
		self.drop_reclaim_sweeper.abort();
		self.migration_sweeper.abort();
		if let Some(handle) = &self.lsm_compaction {
			handle.abort();
		}
		self.lsm_gc.abort();
		self.op_driver.abort();
		self.secondary_index_lifecycle.abort();
		if let Some(handle) = &self.tiering_sweeper {
			handle.abort();
		}
		if let Some(handle) = &self.restore_validation {
			handle.abort();
		}
		self.audit_chain_root_rollup.abort();
		self.default_tenant_seeder.abort();
		self.checkpoint_emitter.abort();
		if let Some(handle) = &self.decision_maintenance {
			handle.abort();
		}
	}

	/// ADR 0699 S5a, ADR 0706: waits for the lifecycle services, whose stops
	/// were both signalled, to stop after their current step: concurrently,
	/// under one shared deadline. Each handle is consumed here, so
	/// [`NodeHandles::shutdown`] never awaits it again. Past the deadline a
	/// service is aborted and joined before this returns: an interrupted
	/// lifecycle permit has then dropped and revoked the clean record. A panic
	/// is a failure, never an orderly stop.
	async fn drain_lifecycle_services(&mut self) -> std::io::Result<()> {
		let deadline = tokio::time::Instant::now() + self.lifecycle_drain_bound;
		let (lsm, decisions) = tokio::join!(
			drain_lifecycle_service(self.lsm_compaction.take(), deadline, "LSM compaction"),
			drain_lifecycle_service(
				self.decision_maintenance.take(),
				deadline,
				"decision maintenance"
			),
		);
		lsm.and(decisions)
	}

	/// Test-only: shortens the bound on the lifecycle services' orderly stop.
	#[cfg(test)]
	pub(crate) fn set_lifecycle_drain_bound_for_test(&mut self, bound: std::time::Duration) {
		self.lifecycle_drain_bound = bound;
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		self.abort();
		let mut first_error = None;
		macro_rules! join_all_way {
			($task:expr, $name:literal) => {
				if let Err(error) = await_background_task($task, $name).await {
					if first_error.is_none() {
						first_error = Some(error);
					}
				}
			};
		}
		join_all_way!(self.ring_updater, "ring updater");
		join_all_way!(self.drift_monitor, "drift monitor");
		join_all_way!(self.eviction_watchdog, "eviction watchdog");
		join_all_way!(self.lease_manager, "lease manager");
		join_all_way!(
			self.pending_finalization_reconciler,
			"pending finalization reconciler"
		);
		if let Err(error) = await_background_task(
			self.cdc_finalization_release_observer,
			"CDC finalization release observer",
		)
		.await
		{
			first_error.get_or_insert(error);
		}
		if let Err(error) =
			await_background_task(self.cdc_locality_heartbeat, "CDC locality heartbeat").await
		{
			first_error.get_or_insert(error);
		}
		join_all_way!(self.epoch_reconciler, "epoch reconciler");
		join_all_way!(self.tenant_policy_reconciler, "tenant policy reconciler");
		join_all_way!(self.drop_reclaim_sweeper, "drop reclaim sweeper");
		join_all_way!(self.migration_sweeper, "migration sweeper");
		if let Some(handle) = self.lsm_compaction {
			join_all_way!(handle, "LSM compaction");
		}
		join_all_way!(self.lsm_gc, "LSM GC");
		join_all_way!(self.op_driver, "operation driver");
		join_all_way!(self.secondary_index_lifecycle, "secondary-index lifecycle");
		if let Some(handle) = self.tiering_sweeper {
			join_all_way!(handle, "tiering sweeper");
		}
		if let Some(handle) = self.restore_validation {
			join_all_way!(handle, "restore validation");
		}
		join_all_way!(self.audit_chain_root_rollup, "audit chain root rollup");
		join_all_way!(self.default_tenant_seeder, "default tenant seeder");
		join_all_way!(self.checkpoint_emitter, "checkpoint emitter");
		if let Some(handle) = self.decision_maintenance {
			join_all_way!(handle, "decision maintenance");
		}
		match first_error {
			Some(error) => Err(error),
			None => Ok(()),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RestoreValidationStatus {
	Complete,
	Pending,
	RecoveringAfterUncleanShutdown,
	Failed(String),
}

impl RestoreValidationStatus {
	fn degraded_reason(&self) -> Option<String> {
		match self {
			Self::Complete => None,
			Self::Pending => Some("RestoreValidationPending".to_string()),
			Self::RecoveringAfterUncleanShutdown => {
				Some("RecoveringAfterUncleanShutdown".to_string())
			}
			Self::Failed(reason) => Some(reason.clone()),
		}
	}
}

/// ADR 0706: one lifecycle service's bounded stop (its stop already
/// signalled). Past `deadline` it is aborted, then joined.
async fn drain_lifecycle_service(
	handle: Option<JoinHandle<()>>,
	deadline: tokio::time::Instant,
	label: &str,
) -> std::io::Result<()> {
	let Some(mut handle) = handle else {
		return Ok(());
	};
	match tokio::time::timeout_at(deadline, &mut handle).await {
		Ok(Ok(())) => Ok(()),
		Ok(Err(err)) if err.is_cancelled() => Ok(()),
		Ok(Err(err)) => Err(Error::other(format!(
			"{label} task failed while it drained: {err}"
		))),
		Err(_elapsed) => {
			handle.abort();
			match handle.await {
				Err(err) if err.is_panic() => Err(Error::other(format!(
					"{label} task failed while it drained: {err}"
				))),
				// Aborted (or finished at the deadline): the close is not orderly.
				_ => {
					tracing::error!(
						service = label,
						"a lifecycle service did not stop within its bound; aborted"
					);
					Err(Error::new(
						ErrorKind::TimedOut,
						format!("{label} did not stop within the lifecycle drain bound; aborted"),
					))
				}
			}
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
		let result = match self.task.await {
			Ok(result) => result,
			Err(err) => Err(Error::new(
				ErrorKind::Other,
				format!("QUIC server task failed during shutdown: {err}"),
			)),
		};
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
		trust_roots: Arc<ActorTrustRoots>,
	},
	TcpTls {
		listener: TcpListener,
		acceptor: TlsAcceptor,
		cluster_domain: String,
		trust_roots: Arc<ActorTrustRoots>,
	},
}

/// Ordinary serious-deployment lease window. ADR 080 keeps this
/// default while local profile configuration chooses a visible
/// longer value.
pub const DEFAULT_DATA_RING_LEASE_TTL_MILLIS: u32 = 3_000;
/// Ordinary-default compatibility name used by focused lease fixtures.
/// Production bootstrap derives its value from configuration (ADR 080).
pub const LEASE_TTL_MS: u32 = DEFAULT_DATA_RING_LEASE_TTL_MILLIS;

/// One validated authority for both lease-grant paths and the
/// derived renewal scheduler. Keeping the renewal interval out of
/// configuration makes an interval at or beyond the TTL
/// unrepresentable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataRingLeaseTiming {
	ttl_millis: u32,
	renew_interval: Duration,
}

impl DataRingLeaseTiming {
	pub fn try_from_duration(ttl: Duration) -> std::io::Result<Self> {
		let ttl_millis = u32::try_from(ttl.as_millis()).map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				"data_ring_lease_ttl must fit the u32 millisecond wire carrier",
			)
		})?;
		if ttl_millis < 3 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"data_ring_lease_ttl must be at least 3 milliseconds",
			));
		}
		if ttl != Duration::from_millis(u64::from(ttl_millis)) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"data_ring_lease_ttl must be an integral number of milliseconds",
			));
		}
		Ok(Self {
			ttl_millis,
			renew_interval: Duration::from_millis(u64::from(ttl_millis / 3)),
		})
	}

	pub fn ttl_millis(self) -> u32 {
		self.ttl_millis
	}

	pub fn renew_interval(self) -> Duration {
		self.renew_interval
	}
}

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
		cfg.validate()?;
		let owner = DataDirectoryOwner::acquire_existing(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Configured,
			startup::RestoreChoice::None,
		)
		.await
	}

	async fn bootstrap_scoped(
		cfg: NodeBootstrapConfig,
		owner: DataDirectoryOwner,
		transport: startup::TransportChoice,
		restore: startup::RestoreChoice<'_>,
	) -> std::io::Result<Self> {
		let mut scope = startup::StartupScope::begin(owner, cfg.storage_shutdown_timeout).await?;
		#[cfg(test)]
		if let Some(deadline) = cfg
			.startup_probe
			.as_ref()
			.and_then(|probe| probe.watchdog_deadline())
		{
			scope.set_test_watchdog_deadline(deadline);
		}
		let result = async {
			let owner = scope.owner().clone();
			// Capture freshness while the owner is held and before restore or
			// other startup components create any files.
			scope.set_storage_init_fresh(startup::fresh_storage_initialization_allowed(
				&cfg.data_dir,
			)?);
			match restore {
				startup::RestoreChoice::None => {}
				startup::RestoreChoice::Cluster(backup) => {
					Self::restore_cluster_backup_artifacts(backup, &cfg, &owner).await?
				}
				startup::RestoreChoice::Replacement(backup, source) => {
					let evidence =
						Self::prepare_replacement_from_cluster_backup(backup, source, &cfg, &owner)
							.await?;
					scope.set_replacement_restore(evidence);
				}
			}
			match transport {
				startup::TransportChoice::Configured => {
					Self::bootstrap_owned(cfg, &mut scope).await
				}
				startup::TransportChoice::Provided(provider, tier) => {
					Self::bootstrap_with_provider_owned_at_tier(cfg, provider, tier, &mut scope)
						.await
				}
			}
		}
		.await;
		match result {
			Ok(parts) => match scope.finish_success(parts) {
				Ok(node) => Ok(node),
				Err((scope, error)) => Err(scope.fail(error).await),
			},
			Err(error) => Err(scope.fail(error).await),
		}
	}

	async fn bootstrap_owned(
		cfg: NodeBootstrapConfig,
		scope: &mut startup::StartupScope,
	) -> std::io::Result<startup::NodeParts> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		if let Some(tls) = cfg.transport_tls.clone() {
			let material = TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)?;
			let trust_roots = Arc::new(ActorTrustRoots::load(
				&tls.node_ca_path,
				&tls.operator_ca_path,
			)?);
			let bind_addr = resolve_socket_addr(&cfg.advertise_address)?;
			let cluster_domain = tls.cluster_domain.clone();
			let server_name = canonical_server_name_resolver(&cluster_domain);
			let quic_endpoint = endpoint_from_tls_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				bind_addr,
			)?;
			let tcp_listener = TcpListener::bind(&cfg.advertise_address).await?;
			let tcp_acceptor = TlsAcceptor::from(Arc::new(rustls_server_config_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				vec![crate::network::tls::HRFT_ALPN.to_vec()],
			)?));
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
			return Self::construct(
				cfg,
				socket_transport_tiers(),
				providers,
				vec![
					BootstrapListener::Quic {
						endpoint: quic_endpoint,
						cluster_domain: cluster_domain.clone(),
						trust_roots: trust_roots.clone(),
					},
					BootstrapListener::TcpTls {
						listener: tcp_listener,
						acceptor: tcp_acceptor,
						cluster_domain,
						trust_roots,
					},
				],
				revocation_tx,
				scope,
			)
			.await;
		}
		if matches!(cfg.cluster, ClusterBootstrapConfig::Join { .. }) {
			return Err(Error::new(
				ErrorKind::Unsupported,
				"cluster.join requires transport_tls or a caller-supplied real transport provider",
			));
		}
		Self::bootstrap_with_provider_owned(cfg, Arc::new(NoDialTransportProvider), scope).await
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
		cfg.validate()?;
		let owner = DataDirectoryOwner::acquire_or_create(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Configured,
			startup::RestoreChoice::Cluster(backup),
		)
		.await
	}

	pub async fn restore_from_cluster_backup_with_provider(
		backup: &ClusterBackupImage,
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		cfg.validate()?;
		let owner = DataDirectoryOwner::acquire_or_create(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Provided(provider, TransportTier::QuicIoUring),
			startup::RestoreChoice::Cluster(backup),
		)
		.await
	}

	pub async fn restore_replacement_from_cluster_backup(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: NodeBootstrapConfig,
	) -> std::io::Result<Self> {
		cfg.validate()?;
		Self::validate_replacement_target(source_node_id, &cfg)?;
		let owner = DataDirectoryOwner::acquire_or_create(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Configured,
			startup::RestoreChoice::Replacement(backup, source_node_id),
		)
		.await
	}

	pub async fn restore_replacement_from_cluster_backup_with_provider(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
	) -> std::io::Result<Self> {
		cfg.validate()?;
		Self::validate_replacement_target(source_node_id, &cfg)?;
		let owner = DataDirectoryOwner::acquire_or_create(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Provided(provider, TransportTier::QuicIoUring),
			startup::RestoreChoice::Replacement(backup, source_node_id),
		)
		.await
	}

	fn validate_replacement_target(
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
		Ok(())
	}

	async fn restore_cluster_backup_artifacts(
		backup: &ClusterBackupImage,
		cfg: &NodeBootstrapConfig,
		owner: &DataDirectoryOwner,
	) -> std::io::Result<()> {
		std::fs::create_dir_all(&cfg.data_dir)?;
		backup.validate_integrity()?;
		// ADR 0697: eligibility from the backup's replayed control state, before
		// any restore marker or artifact.
		backup.validate_restore_eligibility(cfg.node_id, crate::backup::RestoreTarget::SameNode)?;
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
		let resume = startup::record_validated_storage_restore(backup, cfg.node_id, owner)?;
		WendelonLogStore::restore_backup(&backup.control, &cfg.data_dir, owner, resume).await?;
		#[cfg(test)]
		if let Some(fault) = &cfg.startup_fault {
			fault
				.reached(
					"after_control_restore",
					owner.clone(),
					Vec::new(),
					None,
					None,
				)
				.await?;
		}

		SlabManager::restore_backup(&node_backup.storage, &cfg.data_dir, owner, resume).await?;
		Ok(())
	}

	async fn prepare_replacement_from_cluster_backup(
		backup: &ClusterBackupImage,
		source_node_id: u32,
		cfg: &NodeBootstrapConfig,
		owner: &DataDirectoryOwner,
	) -> std::io::Result<startup::ReplacementRestoreEvidence> {
		Self::validate_replacement_target(source_node_id, cfg)?;
		owner.ensure_path(&cfg.data_dir)?;
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
		// ADR 0697: eligibility from the backup's replayed control state, before
		// the AWAITING_CLUSTER marker or any artifact.
		backup.validate_restore_eligibility(
			source_node_id,
			crate::backup::RestoreTarget::Replacement {
				target_node: cfg.node_id,
			},
		)?;
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
		// Replacement storage is not authoritative until a complete leader state
		// has been installed and compared. Persist the fence before import.
		{
			let _permit = owner.write_permit()?;
			let marker = cfg
				.data_dir
				.join(crate::storage::manager::AWAITING_CLUSTER_MARKER);
			let mut file = std::fs::OpenOptions::new()
				.write(true)
				.create(true)
				.truncate(true)
				.open(&marker)?;
			std::io::Write::write_all(&mut file, b"awaiting-cluster-v1\n")?;
			file.sync_all()?;
			std::fs::File::open(&cfg.data_dir)?.sync_all()?;
		}
		SlabManager::restore_backup(&node_backup.storage, &cfg.data_dir, owner, false).await?;
		Ok(startup::ReplacementRestoreEvidence::restored(
			&node_backup.storage,
		))
	}

	pub async fn export_cluster_backup(&self) -> std::io::Result<ClusterBackupImage> {
		let (control, state) = self.log_store.export_backup_with_state().await?;
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
		cfg.validate()?;
		let owner = DataDirectoryOwner::acquire_existing(&cfg.data_dir)?;
		Self::bootstrap_scoped(
			cfg,
			owner,
			startup::TransportChoice::Provided(provider, provider_tier),
			startup::RestoreChoice::None,
		)
		.await
	}

	async fn bootstrap_with_provider_owned(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
		scope: &mut startup::StartupScope,
	) -> std::io::Result<startup::NodeParts> {
		Self::bootstrap_with_provider_owned_at_tier(
			cfg,
			provider,
			TransportTier::QuicIoUring,
			scope,
		)
		.await
	}

	async fn bootstrap_with_provider_owned_at_tier(
		cfg: NodeBootstrapConfig,
		provider: Arc<dyn TransportProvider>,
		provider_tier: TransportTier,
		scope: &mut startup::StartupScope,
	) -> std::io::Result<startup::NodeParts> {
		let (revocation_tx, _) = watch::channel(Arc::new(RevocationSnapshot::default()));
		let startup_listeners = if provider_tier == TransportTier::TcpTls {
			let tls = cfg.transport_tls.as_ref().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"TcpTls bootstrap requires transport_tls so the inbound listener can start before join",
				)
			})?;
			let material = TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)?;
			let trust_roots = Arc::new(ActorTrustRoots::load(
				&tls.node_ca_path,
				&tls.operator_ca_path,
			)?);
			let listener = TcpListener::bind(&cfg.advertise_address).await?;
			let acceptor = TlsAcceptor::from(Arc::new(rustls_server_config_with_peer_roots(
				&material,
				trust_roots.combined_roots(),
				vec![crate::network::tls::HRFT_ALPN.to_vec()],
			)?));
			vec![BootstrapListener::TcpTls {
				listener,
				acceptor,
				cluster_domain: tls.cluster_domain.clone(),
				trust_roots,
			}]
		} else {
			Vec::new()
		};
		let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
		providers.insert(provider_tier, provider);
		Self::construct(
			cfg,
			transport_tiers_for_provider(provider_tier),
			providers,
			startup_listeners,
			revocation_tx,
			scope,
		)
		.await
	}

	async fn construct(
		cfg: NodeBootstrapConfig,
		local_tiers: TransportTierSet,
		providers: HashMap<TransportTier, Arc<dyn TransportProvider>>,
		startup_listeners: Vec<BootstrapListener>,
		revocation_tx: watch::Sender<Arc<RevocationSnapshot>>,
		scope: &mut startup::StartupScope,
	) -> std::io::Result<startup::NodeParts> {
		if cfg.decision_mode != crate::consensus::decision_mode::DecisionMode::Today
			&& !cfg.allow_dev_keys
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"the ParentDecisions authority is for disposable development stores only",
			));
		}
		cfg.decision_mode.admit_store(&cfg.data_dir)?;
		#[cfg(test)]
		if let Some(fault) = &cfg.startup_fault {
			for listener in &startup_listeners {
				if let BootstrapListener::TcpTls { listener, .. } = listener {
					fault.bound_tcp.lock().unwrap().push(listener.local_addr()?);
				}
			}
		}

		let owner = scope.owner().clone();
		cfg.validate()?;
		owner.ensure_contains(&cfg.data_dir)?;
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
		#[cfg(test)]
		scope.checkpoint(&cfg, "pool").await?;

		// 2. Dedicated `io_uring` workers for the two durability classes.
		let slab_io = spawn_io_thread_named_for_class(
			cfg.slab_io_depth,
			"wendelon-io-uring-slab",
			IoUringClass::Slab,
		)?
		.with_data_directory_owner(owner.clone());
		let wal_io = spawn_io_thread_named_for_class(
			cfg.wal_io_depth,
			"wendelon-io-uring-wal",
			IoUringClass::Wal,
		)?
		.with_data_directory_owner(owner.clone());

		// 4. Durable Raft storage + recovered state machine.
		let (wal_path, vote_path, snapshot_path) = control_artifact_paths(&cfg.data_dir);
		let retention = RetentionGuard::new(cfg.control_retention);
		let log_store = WendelonLogStore::open_with_retention_and_owner(
			&wal_path,
			&vote_path,
			&snapshot_path,
			wal_io.clone(),
			retention.clone(),
			owner.clone(),
			cfg.decision_mode,
		)
		.await?;
		// Control replay is validated before it can authorize a missing-index rebuild.
		let recovered_control = log_store.recovered_state_machine().await?;
		let index_recovery =
			match startup::validated_index_recovery(&owner, cfg.node_id, &recovered_control).await?
			{
				Some(authority) => Some(authority),
				None => {
					startup::validated_replacement_recovery(&owner, scope.replacement_restore())
						.await?
				}
			};
		// A first bootstrap interrupted before the storage index existed resumes
		// as a fresh storage creation, on pristine Control evidence only.
		let storage_init_fresh = scope.storage_init_fresh()
			|| (index_recovery.is_none()
				&& startup::interrupted_first_creation(&owner, &log_store).await?);

		// 3. L1 slab storage.
		let l2 = cfg
			.l2
			.as_ref()
			.map(|config| config.build_with_owner(owner.clone()))
			.transpose()?;
		let l2_for_sweeper = l2.clone();
		#[cfg(test)]
		scope.checkpoint(&cfg, "io_workers").await?;
		let crypto = Arc::new(CryptoManager::new_with_cache_config_and_owner(
			cfg.root_kek_provider
				.build_with_owner(&cfg.data_dir, &owner)?,
			cfg.crypto_cache,
			owner.clone(),
		));
		#[cfg(not(feature = "test-support"))]
		let slab: Arc<SlabManager> = {
			let config = SlabManagerConfig {
				tier_history_budget_bytes: cfg.tier_history_budget_bytes,
				dir: cfg.data_dir.clone(),
				max_slab_bytes: cfg.slab_max_bytes,
				io: slab_io.clone(),
				l2,
				rehydration: cfg.rehydration.clone(),
				l2_column_fetch: cfg.l2_column_fetch.clone(),
				row_group_writer: cfg.row_group_writer.clone(),
				l2_row_group_fetch: cfg.l2_row_group_fetch.clone(),
				encoding_writer: cfg.encoding_writer.clone(),
				l2_chunk_layout: cfg.l2_chunk_layout.clone(),
				l2_encoding_fetch: cfg.l2_encoding_fetch.clone(),
				encoding_compute: cfg.encoding_compute.clone(),
				group_commit: cfg.group_commit.clone(),
				crypto: crypto.clone(),
			};
			if !cfg.data_dir.join("storage_index.redb").exists()
				&& let Some(authority) = &index_recovery
			{
				SlabManager::initialize_recovered_store_with_owner(config, owner.clone(), authority)
					.await?
			} else if storage_init_fresh && !cfg.data_dir.join("storage_index.redb").exists() {
				SlabManager::initialize_new_store_with_owner(config, owner.clone()).await?
			} else {
				SlabManager::open_with_owner(config, owner.clone()).await?
			}
		};
		#[cfg(feature = "test-support")]
		let slab: Arc<SlabManager> = {
			let config = SlabManagerConfig {
				tier_history_budget_bytes: cfg.tier_history_budget_bytes,
				dir: cfg.data_dir.clone(),
				max_slab_bytes: cfg.slab_max_bytes,
				io: slab_io.clone(),
				l2,
				rehydration: cfg.rehydration.clone(),
				l2_column_fetch: cfg.l2_column_fetch.clone(),
				row_group_writer: cfg.row_group_writer.clone(),
				l2_row_group_fetch: cfg.l2_row_group_fetch.clone(),
				encoding_writer: cfg.encoding_writer.clone(),
				l2_chunk_layout: cfg.l2_chunk_layout.clone(),
				l2_encoding_fetch: cfg.l2_encoding_fetch.clone(),
				encoding_compute: cfg.encoding_compute.clone(),
				group_commit: cfg.group_commit.clone(),
				crypto: crypto.clone(),
			};
			let initialize =
				storage_init_fresh && !cfg.data_dir.join("storage_index.redb").exists();
			if !cfg.data_dir.join("storage_index.redb").exists()
				&& let Some(authority) = &index_recovery
			{
				match cfg.slab_file_ops.clone() {
					Some(file_ops) => {
						SlabManager::initialize_recovered_store_with_file_ops_and_owner(
							config,
							file_ops.into_inner(),
							owner.clone(),
							authority,
						)
						.await?
					}
					None => {
						SlabManager::initialize_recovered_store_with_owner(
							config,
							owner.clone(),
							authority,
						)
						.await?
					}
				}
			} else {
				match (initialize, cfg.slab_file_ops.clone()) {
					(true, Some(file_ops)) => {
						SlabManager::initialize_new_store_with_file_ops_and_owner(
							config,
							file_ops.into_inner(),
							owner.clone(),
						)
						.await?
					}
					(true, None) => {
						SlabManager::initialize_new_store_with_owner(config, owner.clone()).await?
					}
					(false, Some(file_ops)) => {
						SlabManager::open_with_file_ops_and_owner(
							config,
							file_ops.into_inner(),
							owner.clone(),
						)
						.await?
					}
					(false, None) => SlabManager::open_with_owner(config, owner.clone()).await?,
				}
			}
		};
		// ADR 0697: this store names its slabs on the ring as this node.
		slab.bind_control_node(cfg.node_id)?;

		let bootstrap_mode = log_store.bootstrap_mode().await;
		let checkpoint_coordinator = CheckpointCoordinator::new(log_store.clone());
		let tier_observer = Arc::new(ManagerTierObserver::new(slab.clone()));
		let state_machine = recovered_control
			.with_tier_observer(tier_observer)
			.with_tenant_kek_observer(crypto.clone())
			.with_checkpoint_observer(checkpoint_coordinator.clone())
			.with_external_revocation_snapshot(revocation_tx.clone());
		scope
			.handles()
			.set_checkpoint_emitter(checkpoint_coordinator.spawn());
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_checkpoint_emitter").await?;

		// 5. Seed membership epoch from durable history if present;
		//    otherwise fall back to the single-node genesis epoch.
		//    If recovery finds a committed-but-unseeded transition,
		//    `select_bootstrap_epoch` holds visibility at the prior
		//    epoch until the reconciler finishes auto-seeding.
		let fallback_epoch = seed_single_node_epoch(
			cfg.decision_mode,
			local_node,
			&cfg.advertise_address,
			local_tiers,
		);
		let recovered_state = state_machine.state_snapshot().await;
		// Recovery mode: the previous owner did not finish a complete shutdown
		// (crash, kill, or self-termination after an incomplete close). Readiness
		// stays gated until the existing restore validation has run.
		let unclean_previous_shutdown = !owner.previous_shutdown_clean();
		// ADR 0699 S5a: an unclean start under ParentDecisions owes a physical
		// proof of its certified base before any close may record it clean.
		if unclean_previous_shutdown
			&& cfg.decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions
		{
			owner.require_base_recovery();
		}
		if unclean_previous_shutdown {
			tracing::warn!(
				data_dir = %cfg.data_dir.display(),
				"recovering after unclean shutdown"
			);
		}
		let requires_restore_validation = unclean_previous_shutdown
			|| (bootstrap_mode == DurableBootstrapMode::Recovery
				&& !recovered_state.tiered_slabs.is_empty());
		let (restore_validation_tx, restore_validation_rx) =
			watch::channel(if unclean_previous_shutdown {
				RestoreValidationStatus::RecoveringAfterUncleanShutdown
			} else if requires_restore_validation {
				RestoreValidationStatus::Pending
			} else {
				RestoreValidationStatus::Complete
			});
		// Safe because Raft has not been constructed yet, so no tenant-KEK
		// observer callback can race this authoritative bootstrap snapshot.
		crypto.replace_keysets_from_authoritative_snapshot(&recovered_state.tenant_keysets)?;
		// ADR 0697: only this store's own committed pointers are its tier
		// evidence. A view that is not authoritative for this store (a
		// superseded or unexplained identity, already fenced) stages nothing
		// and retires nothing.
		let own_tiers = slab.own_tier_view(
			recovered_state.tiered_slabs.values(),
			&recovered_state.incarnations,
		);
		let recovered_log_index = recovered_state.last_applied_log_id.map(|id| id.index);
		if !slab.is_awaiting_cluster() {
			if let Some(index) = recovered_log_index {
				slab.seed_installed_snapshot_index(index);
			}
		}
		if let Some(mut recovered_tiers) = own_tiers {
			// A recovered committed prefix is complete before Raft and observer startup.
			// Replacement imports remain fenced until a complete leader state arrives.
			if !slab.is_awaiting_cluster() && recovered_state.last_applied_log_id.is_some() {
				let committed_ids = recovered_tiers.iter().map(|tier| tier.slab_id).collect();
				slab.fence_local_tiers_absent_from_committed(&committed_ids, recovered_log_index)
					.await?;
			}
			recovered_tiers.sort_by_key(|tier| tier.slab_id);
			let committed_id_floor = recovered_tiers
				.last()
				.map(|tier| tier.slab_id)
				.unwrap_or(SlabId::ZERO);
			// Recovered committed callbacks are replay evidence even on an awaiting
			// replacement. Register them without clearing its marker or serving data.
			for tier in recovered_tiers {
				slab.stage_bootstrap_tier_authority(tier, recovered_log_index, committed_id_floor)
					.await?;
			}
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
		let audit_sink = build_audit_sink_with_owner(
			cfg.data_dir.join("audit.ndjson"),
			&cfg.audit,
			owner.clone(),
		)?;
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
		let state_machine = state_machine
			.with_observability_node_id(local_node)
			.with_audit_materializer(
				local_node,
				audit_sink.clone(),
				audit_materialization.clone(),
			);
		scope
			.handles()
			.set_audit_chain_root_rollup(spawn_signed_chain_root_rollup(
				audit_sink.clone(),
				local_node,
				AuditRootSigner::from_seed(cfg.audit_root_signing_secret),
				DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL,
			));
		#[cfg(test)]
		scope
			.checkpoint(&cfg, "set_audit_chain_root_rollup")
			.await?;
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
		scope.adopt_factory(factory.clone());
		#[cfg(test)]
		scope.checkpoint(&cfg, "adopt_factory").await?;
		owner.register_worker(factory.clone())?;
		let dial_provider: Arc<dyn TransportProvider> = factory.clone();

		// 8. Raft network factory — uses the same shared
		//    tier-negotiating dialer as the rest of the node, so
		//    consensus traffic participates in the same QUIC/TCP
		//    fallback behavior as join and data/control RPCs.
		let raft_network = WendelonRaftNetworkFactory::new(dial_provider.clone(), pool.clone())
			.with_decision_mode(cfg.decision_mode);

		// 9. openraft engine. `validate()` pins heartbeat / election
		//    timeouts against `Config`'s sanity checks.
		let raft_config = Arc::new(
			cfg.control_retention
				.openraft_config("wendelon")
				.validate()
				.map_err(|e| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("openraft config invalid: {e}"),
					)
				})?,
		);
		#[cfg(feature = "test-support")]
		let test_automatic_elections = raft_config.enable_elect;
		let raft = WendelonRaft::new(
			cfg.node_id,
			raft_config,
			raft_network,
			log_store.clone(),
			state_machine.clone(),
		)
		.await
		.map_err(|e| Error::new(ErrorKind::Other, format!("Raft::new: {e}")))?;
		scope.adopt_raft(raft.clone());
		#[cfg(test)]
		scope.checkpoint(&cfg, "adopt_raft").await?;
		#[cfg(feature = "test-support")]
		if let Some(gate) = &cfg.join_promotion_gate {
			gate.register_metrics(cfg.node_id, raft.metrics(), test_automatic_elections);
		}
		let lease_timing = DataRingLeaseTiming::try_from_duration(cfg.data_ring_lease_ttl)?;

		// 10. Ring router — starts empty, seeded by the first
		//     `epoch_rx` value the updater task observes before it
		//     begins blocking on `changed()`.
		let ring = Arc::new(RingRouter::empty());
		scope
			.handles()
			.set_ring_updater(ring.clone().spawn_updater(epoch_rx.clone(), cfg.rf));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_ring_updater").await?;

		// 11. Drift monitor.
		let abort: Arc<dyn AbortHandle> = Arc::new(ProcessAbort);
		let drift_subscriber = Arc::new(FanoutClockDriftSubscriber::new(vec![
			Arc::new(WatchClockDriftSubscriber::new(clock_drift_tx)),
			Arc::new(AuditClockDriftSubscriber::new(audit_sink.clone())),
		]));
		scope
			.handles()
			.set_drift_monitor(spawn_drift_monitor_with_subscriber(
				tracker.clone(),
				clock.clone(),
				abort.clone(),
				drift_subscriber,
				DRIFT_CHECK_INTERVAL,
				cfg.drift_max_offset_ms,
			));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_drift_monitor").await?;

		// 12. Data Ring + Raft Internal Ring servers. Both observe
		//     inbound HLCs into the shared tracker so the drift
		//     monitor sees Raft-layer and data-ring signal. The
		//     lease gate is flipped on only after bootstrap/join has
		//     completed and the background lease manager starts.
		// ADR 0699 S3a: decision proposals and signer binding, ParentDecisions only.
		let decision_proposals = if cfg.decision_mode
			== crate::consensus::decision_mode::DecisionMode::ParentDecisions
		{
			let (_, trusted) = identity::resolve_decision_keys(&cfg)?;
			Some(Arc::new(
				crate::network::decision_proposals::DecisionProposals::new(
					cfg.decision_mode,
					local_node,
					raft.clone(),
					state_machine.clone(),
					trusted,
					clock.clone(),
					factory.clone(),
					pool.clone(),
				),
			))
		} else {
			None
		};
		// ADR 0699 S3b: this store signs child acknowledgements as its incarnation.
		let decision_signer = if cfg.decision_mode
			== crate::consensus::decision_mode::DecisionMode::ParentDecisions
		{
			let (secret, _) = identity::resolve_decision_keys(&cfg)?;
			Some(Arc::new(
				crate::network::data_server::child_writes::DecisionSigner {
					key: ed25519_dalek::SigningKey::from_bytes(&secret),
					signer: crate::consensus::decisions::Signer {
						node: local_node.id,
						incarnation: slab.owner_ref()?.incarnation(),
					},
				},
			))
		} else {
			None
		};
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
			.with_counter_retention(cfg.counter_retention)?
			.with_tracker(tracker.clone())
			.with_lease_gate(true)
			.with_decision_mode(cfg.decision_mode)
			.with_decision_signer(decision_signer)
			.with_decision_proposals(decision_proposals.clone())
			.with_directory_owner(owner.clone()),
		);
		let raft_server = Arc::new(
			WendelonRaftInternalServer::new(raft.clone(), pool.clone(), retention.clone())
				.with_decision_mode(cfg.decision_mode)
				.with_tracker(tracker.clone())
				.with_replacement_replay(slab.clone(), state_machine.clone(), log_store.clone()),
		);
		let control_server = WendelonControlServer::new(
			raft.clone(),
			state_machine.clone(),
			crypto.clone(),
			local_node,
			epoch_tx.clone(),
			epoch_rx.clone(),
			factory.clone(),
			pool.clone(),
			cfg.rf as u32,
			lease_timing.ttl_millis(),
			audit_sink.clone(),
		)
		.with_protocol_window(cfg.protocol)
		.with_decision_mode(cfg.decision_mode)
		.with_lease_authority(lease_authority.clone())
		.with_transfer_authority(transfer_authority.clone())
		.with_storage_manager(slab.clone())
		.with_decision_proposals(decision_proposals.clone());
		#[cfg(feature = "test-support")]
		let control_server = control_server.with_join_promotion_gate(cfg.join_promotion_gate.clone());
		let control_server = Arc::new(control_server);
		if let Some(proposals) = decision_proposals {
			let incarnation = slab.owner_ref()?.incarnation();
			let data_server = data_server.clone();
			let mut stop = data_server.decision_maintenance_stop();
			scope
				.handles()
				.set_decision_maintenance(tokio::spawn(async move {
					let mut ticker = tokio::time::interval(DECISION_MAINTENANCE_INTERVAL);
					ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
					// ADR 0700: owned here; stopped with the loop, cancelled if
					// the loop is aborted.
					let mut probe = crate::network::decision_proposals::ClockProbe::default();
					// ADR 0707: this loop's own proposals wait for their answer
					// only until the node stops; once the stop is set no later
					// step of the admitted tick submits one, and the tick's
					// remaining local work finishes under the drain bound.
					let maintenance = crate::network::decision_proposals::MaintenanceProposals::new(
						&proposals,
						stop.clone(),
					);
					loop {
						// ADR 0707: a ready stop wins over a ready tick.
						let stopping = tokio::select! {
							biased;
							_ = stop.wait_for(|stopping| *stopping) => true,
							_ = ticker.tick() => false,
						};
						if stopping {
							probe.stop().await;
							return;
						}
						// ADR 0700: keep this follower's clock sample fresh at
						// the leader, whatever its readiness or the gate says.
						proposals.clock_probe_step(&mut probe).await;
						let _ = maintenance.ensure_signer_once(incarnation).await;
						let _ = maintenance.advance_floor_once().await;
						let _ = maintenance
							.resolve_closed_once(DECISION_RESOLVE_BATCH)
							.await;
						let _ = data_server.sweep_refused_intents().await;
						// ADR 0699 S5a: certified generations; reads wait for the
						// base to validate after each start.
						// Validation (activation of a committed staged candidate
						// first) precedes any new build or report.
						let _ = maintenance.generation_step().await;
						let _ = data_server.validate_certified_base().await;
						let _ = data_server.certification_step(&maintenance).await;
						// ADR 0699 S5b: one bounded cleanup step and one debt
						// sweep page, once the base is validated.
						let _ = data_server.certified_cleanup_step().await;
						let _ = data_server.debt_sweep_step().await;
					}
				}));
		}

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
		scope.adopt_lifecycle(lifecycle.clone());
		#[cfg(test)]
		scope.checkpoint(&cfg, "adopt_lifecycle").await?;
		let service: Arc<dyn HrftService> =
			Arc::new(LifecycleService::new(router.clone(), lifecycle.clone()));
		#[cfg(test)]
		if let Some(probe) = &cfg.startup_probe {
			probe.record(
				&slab,
				&control_server,
				&service,
				&lifecycle,
				&raft,
				scope.abort_handles(),
			);
		}
		for provider in publish_providers {
			scope.will_publish(
				provider.clone(),
				service.clone(),
				local_node,
				&cfg.advertise_address,
			);
			#[cfg(test)]
			scope.checkpoint(&cfg, "will_publish").await?;
			provider.publish_local(local_node, &cfg.advertise_address, service.clone())?;
		}
		for listener in startup_listeners {
			let server = match listener {
				BootstrapListener::Quic {
					endpoint,
					cluster_domain,
					trust_roots,
				} => NodeTransportServer::Quic(spawn_quic_server(
					endpoint,
					service.clone(),
					pool.clone(),
					lifecycle.clone(),
					cfg.transport_admission.clone(),
					cluster_domain,
					revocation_tx.subscribe(),
					trust_roots,
				)),
				BootstrapListener::TcpTls {
					listener,
					acceptor,
					cluster_domain,
					trust_roots,
				} => NodeTransportServer::TcpTls(spawn_tcp_tls_server(
					listener,
					acceptor,
					service.clone(),
					pool.clone(),
					lifecycle.clone(),
					cfg.transport_admission.clone(),
					cluster_domain,
					revocation_tx.subscribe(),
					trust_roots,
				)),
			};
			scope.adopt_transport(server);
			#[cfg(test)]
			scope.checkpoint(&cfg, "adopt_transport").await?;
		}
		scope
			.handles()
			.set_epoch_reconciler(control_server.clone().spawn_epoch_reconciler());
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_epoch_reconciler").await?;
		#[cfg(test)]
		if let Some(probe) = &cfg.startup_probe {
			probe.record(
				&slab,
				&control_server,
				&service,
				&lifecycle,
				&raft,
				scope.abort_handles(),
			);
		}

		// 14. Fresh nodes either initialize a one-node cluster or
		//     join an existing leader through the explicit Control
		//     Ring join path. Recovery-mode nodes rely on their
		//     durable Raft membership/log state instead of replaying
		//     a synthetic join RPC on restart; reissuing join after
		//     recovery can perturb follower replication ordering and
		//     trip OpenRaft's follower-log-reversion guard.
		#[cfg(test)]
		scope.checkpoint(&cfg, "before_cluster").await?;
		let cluster_result: std::io::Result<()> = async {
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
							advertised_capabilities(cfg.decision_mode),
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
			Ok(())
		}
		.await;
		if let Err(error) = cluster_result {
			return Err(error);
		}
		#[cfg(test)]
		if let Some(probe) = &cfg.startup_probe {
			probe.after_handoff()?;
		}

		#[cfg(test)]
		scope.checkpoint(&cfg, "epoch_reconcile").await?;

		// ADR 0095 stamps erasure participants from replicated epoch history.
		// Seed the genesis edge synchronously on the leader so an immediately
		// following DROP cannot race the background epoch reconciler.
		if let Err(error) = control_server.reconcile_pending_epoch().await
			&& error.kind() != ErrorKind::NotConnected
		{
			return Err(error);
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
		#[cfg(test)]
		scope.checkpoint(&cfg, "tenant_seed").await?;
		scope.handles().set_default_tenant_seeder(
			if state_machine
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
			},
		);
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_default_tenant_seeder").await?;

		// Prime the tenant policy cache from the freshly seeded state
		// machine before spawning the periodic reconciler. Otherwise
		// the reconciler's first tick could race ahead of the seed,
		// stamp `last_refresh` on an empty snapshot, and starve the
		// next `ensure_tenant_policy_fresh` call of a real refresh for
		// a full tick interval.
		let _ = data_server.sync_tenant_policy_cache_once().await;
		scope.handles().set_tenant_policy_reconciler(
			data_server
				.clone()
				.spawn_tenant_policy_reconciler(DEFAULT_TENANT_CACHE_REFRESH_INTERVAL),
		);
		#[cfg(test)]
		scope
			.checkpoint(&cfg, "set_tenant_policy_reconciler")
			.await?;

		// 15. Eviction watchdog. Route leader-driven epoch changes
		//     back through the same Control Ring compound workflow
		//     the external RPC uses so migration auto-seeding stays
		//     attached to every transition source.
		let leader: Arc<dyn LeaderStatus> =
			Arc::new(RaftLeaderStatus::new(raft.clone(), local_node));
		let advancer: Arc<dyn MembershipAdvancer> =
			Arc::new(ControlRingMembershipAdvancer::new(control_server.clone()));
		scope
			.handles()
			.set_eviction_watchdog(spawn_eviction_watchdog(
				tracker.clone(),
				clock.clone(),
				leader.clone(),
				advancer,
				epoch_rx.clone(),
				cfg.rf,
				EVICTION_CHECK_INTERVAL,
			));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_eviction_watchdog").await?;
		let raft_committer: Arc<dyn TierCommitter> =
			Arc::new(RaftCommitter::new(raft.clone()).with_state_machine(state_machine.clone()));
		let lsm_worker = LsmCompactionWorker::new(LsmConfig::default(), slab.slab_pin_table())?;
		let lsm_driver = Arc::new(
			LeaderGatedLsmCompactionDriver::new(
				slab.clone(),
				leader.clone(),
				raft_committer.clone(),
				state_machine.clone(),
			)
			.with_certified(
				(cfg.decision_mode
					== crate::consensus::decision_mode::DecisionMode::ParentDecisions)
					.then(|| data_server.clone()),
			),
		);
		let lsm_service = LsmCompactionService::new(lsm_driver, lsm_worker);
		let lsm_compaction_control = lsm_service.control();
		if cfg.lsm_compaction_paused {
			lsm_compaction_control.pause();
		}
		scope
			.handles()
			.set_lsm_compaction_control(lsm_compaction_control);
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_lsm_compaction_control").await?;
		scope.handles().set_lsm_compaction(lsm_service.spawn());
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_lsm_compaction").await?;
		let lsm_gc_worker = LsmGcWorker::new(LsmConfig::default(), slab.slab_pin_table())?;
		let lsm_gc_driver = Arc::new(LeaderGatedLsmGcDriver::new(
			slab.clone(),
			leader.clone(),
			raft_committer.clone(),
		));
		scope
			.handles()
			.set_lsm_gc(LsmGcService::new(lsm_gc_driver, lsm_gc_worker).spawn());
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_lsm_gc").await?;
		scope.handles().set_op_driver(tokio::spawn(
			Arc::new(crate::storage::op_driver::OpDriver::new(
				slab.clone(),
				raft_committer.clone(),
			))
			.run(cfg.tiering.op_driver_interval),
		));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_op_driver").await?;
		scope
			.handles()
			.set_secondary_index_lifecycle(spawn_secondary_index_lifecycle_driver(
				slab.clone(),
				state_machine.clone(),
				control_server.clone(),
				local_node,
			));
		#[cfg(test)]
		scope
			.checkpoint(&cfg, "set_secondary_index_lifecycle")
			.await?;

		// 16. Lease manager (plan 018 / ADR 080). Runs in the background,
		//     renews every configured TTL / 3 and on every ring-epoch
		//     change, and updates the data server's local cache so
		//     the write/read gates installed above can admit traffic.
		scope
			.handles()
			.set_lease_manager(data_server.clone().spawn_lease_manager(
				ring.clone(),
				clock.clone(),
				epoch_rx.clone(),
				lease_timing.renew_interval(),
				lease_timing.ttl_millis(),
			));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_lease_manager").await?;
		scope.handles().set_pending_finalization_reconciler(
			data_server
				.clone()
				.spawn_pending_finalization_reconciler(PENDING_FINALIZATION_RETRY_INTERVAL),
		);
		#[cfg(test)]
		scope
			.checkpoint(&cfg, "set_pending_finalization_reconciler")
			.await?;
		scope.handles().set_drop_reclaim_sweeper(
			data_server
				.clone()
				.spawn_drop_reclaim_sweeper(DROP_RECLAIM_SWEEP_INTERVAL),
		);
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_drop_reclaim_sweeper").await?;
		scope.handles().set_cdc_finalization_release_observer(
			data_server
				.clone()
				.spawn_cdc_finalization_release_observer(),
		);
		#[cfg(test)]
		scope
			.checkpoint(&cfg, "set_cdc_finalization_release_observer")
			.await?;
		scope.handles().set_cdc_locality_heartbeat(
			data_server.clone().spawn_cdc_locality_heartbeat_supervisor(
				ring.clone(),
				cfg.cdc_locality_heartbeat_interval,
			),
		);
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_cdc_locality_heartbeat").await?;
		scope
			.handles()
			.set_migration_sweeper(data_server.clone().spawn_migration_sweeper(
				MIGRATION_SWEEP_INTERVAL,
				MIGRATION_SWEEP_FAILURE_BACKOFF,
				MIGRATION_SWEEP_MAX_PER_PASS,
				cfg.mvcc_retention_paused,
			));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_migration_sweeper").await?;
		scope
			.handles()
			.set_tiering_sweeper(l2_for_sweeper.map(|object_store| {
				SlabSweeper::new(
					slab.clone(),
					object_store,
					raft_committer.clone(),
					state_machine.clone(),
					cfg.tiering.clone(),
				)
				.with_clock(clock.clone())
				.with_audit_sink(local_node, audit_sink.clone())
				.with_mvcc_retention_policy(cfg.mvcc_retention_policy)
				.spawn()
			}));
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_tiering_sweeper").await?;
		scope
			.handles()
			.set_restore_validation(if requires_restore_validation {
				let slab = slab.clone();
				let recovery_owner = owner.clone();
				Some(tokio::spawn(async move {
					loop {
						let result = match slab.wait_tier_authority_ready().await {
							Ok(()) => {
								slab.validate_tiered_references().await.map_err(|error| {
									let retry = crate::storage::manager::tier_authority::transient_tier_error(error.kind());
									if retry {
										crate::storage::manager::tier_authority::observe_tier_authority_retry(&error);
									}
									(error, retry)
								})
							}
							Err(error) => {
								// A pending attempt can become Ready before this status
								// observation. That is still retryable, never sticky.
								let retry = slab.tier_authority_failure_kind().is_none();
								Err((error, retry))
							}
						};
						match result {
							Ok(()) => {
								if let Err(error) =
									startup::complete_storage_index_recovery(&recovery_owner)
								{
									let _ = restore_validation_tx.send(
										RestoreValidationStatus::Failed(format!(
											"RestoreAuthorityClearFailed: {error}"
										)),
									);
									break;
								}
								recovery_owner.mark_recovery_complete();
								let _ =
									restore_validation_tx.send(RestoreValidationStatus::Complete);
								break;
							}
							Err((error, retry)) => {
								if retry {
									let _ = restore_validation_tx
										.send(RestoreValidationStatus::Pending);
									tokio::time::sleep(Duration::from_millis(200)).await;
									continue;
								}
								tracing::error!(%error, "restore tier validation failed");
								let _ =
									restore_validation_tx.send(RestoreValidationStatus::Failed(
										format!("RestoreValidationFailed: {error}"),
									));
								break;
							}
						}
					}
				}))
			} else {
				None
			});
		#[cfg(test)]
		scope.checkpoint(&cfg, "set_restore_validation").await?;

		if data_server.degraded_reason().is_some() {
			tracing::error!(
				node_id = local_node.id,
				"bootstrapped with persisted local-persistence distrust fence; shutting down raft participation"
			);
			if let Err(err) = raft.shutdown().await {
				tracing::error!(
					node_id = local_node.id,
					error = %err,
					"failed to shut down raft participation for persisted distrust fence"
				);
			}
		}
		let parts = startup::NodeParts {
			_directory_owner: owner,
			storage_shutdown_timeout: cfg.storage_shutdown_timeout,
			recovered_after_unclean_shutdown: unclean_previous_shutdown,
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
			retention,
			clock_drift_events: clock_drift_rx,
		};
		Ok(parts)
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
		trust_roots: Arc<ActorTrustRoots>,
	) -> QuicServerHandle {
		spawn_quic_server(
			endpoint,
			self.service.clone(),
			self.pool.clone(),
			self.lifecycle.clone(),
			self.transport_admission.clone(),
			cluster_domain.into(),
			self.state_machine.subscribe_revocation_snapshot(),
			trust_roots,
		)
	}

	pub fn serve_tcp_tls(
		&self,
		listener: TcpListener,
		acceptor: TlsAcceptor,
		cluster_domain: impl Into<String>,
		trust_roots: Arc<ActorTrustRoots>,
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
					Some(trust_roots),
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
		let mut handles = self.handles;
		// ADR 0699 S5a, ADR 0706: the lifecycle services (decision maintenance
		// and LSM compaction, whose certified steps hold lifecycle permits) stop
		// between steps, while Raft, the committer and the transports still
		// run, so an orderly close never interrupts a permit. Both are signalled
		// before either is awaited, then drained under one shared deadline.
		self.data_server.stop_decision_maintenance();
		handles.lsm_compaction_control.stop();
		let mut first_error = handles.drain_lifecycle_services().await.err();
		lifecycle.begin_drain();
		for server in &transport_servers {
			server.begin_drain();
		}
		if let Err(error) = self.raft.shutdown().await {
			first_error.get_or_insert(Error::other(format!("Raft shutdown: {error}")));
		}
		if let Err(error) = handles.shutdown().await {
			first_error.get_or_insert(error);
		}
		// Bounded like every other shutdown step: a quiescence that never comes
		// must not keep the node from reaching its fail-closed storage close.
		if tokio::time::timeout(
			self.storage_shutdown_timeout,
			lifecycle.wait_for_quiescence(),
		)
		.await
		.is_err()
		{
			first_error.get_or_insert(Error::new(
				ErrorKind::TimedOut,
				"in-flight request quiescence did not complete within the shutdown bound",
			));
		}
		for server in transport_servers {
			if let Err(error) = server.shutdown().await {
				if first_error.is_none() {
					first_error = Some(error);
				}
			}
		}
		if let Err(error) = self
			._directory_owner
			.close(self.storage_shutdown_timeout)
			.await
		{
			// An incomplete storage close outranks every earlier error: callers
			// decide from it whether writers may still be live (and the process
			// must terminate rather than run destructors).
			if let Some(earlier) = first_error.replace(error) {
				tracing::error!(error = %earlier, "earlier shutdown step failed");
			}
		}
		match first_error {
			Some(error) => Err(error),
			None => Ok(()),
		}
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
			retention: self.retention.clone(),
		}
	}

	/// True when this boot found no clean-shutdown record from the previous
	/// owner and therefore started in recovery mode.
	pub fn recovered_after_unclean_shutdown(&self) -> bool {
		self.recovered_after_unclean_shutdown
	}

	/// This node's Control log retention guard.
	pub fn retention(&self) -> &Arc<RetentionGuard> {
		&self.retention
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
	retention: Arc<RetentionGuard>,
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
		// ADR 0700: in ParentDecisions mode, Ready means this store can serve.
		let decision_reason = self.data_server.decision_readiness_reason().await;
		let degraded_reason = self
			.data_server
			.degraded_reason()
			.or_else(|| {
				self.data_server
					.certified_base_unvalidated()
					.then(|| "CertifiedBaseUnvalidated".to_string())
			})
			.or(decision_reason)
			.or_else(|| self.data_server.tier_authority_readiness_reason())
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
			control_retention: self.retention.status(),
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
	trust_roots: Option<Arc<ActorTrustRoots>>,
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
		trust_roots,
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
	trust_roots: Option<Arc<ActorTrustRoots>>,
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
		trust_roots,
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
	trust_roots: Arc<ActorTrustRoots>,
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
				Some(trust_roots),
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
	trust_roots: Arc<ActorTrustRoots>,
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
				Some(trust_roots),
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
	decision_mode: crate::consensus::decision_mode::DecisionMode,
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
			capabilities: advertised_capabilities(decision_mode),
		},
	);
	MembershipEpoch { epoch: 1, peers }
}

fn advertised_capabilities(
	decision_mode: crate::consensus::decision_mode::DecisionMode,
) -> NodeCapabilitySet {
	let capabilities = NodeCapabilitySet::empty()
		.with(NodeCapability::SlabStatsPruning)
		.with(NodeCapability::PartialAggregatePushdown);
	if decision_mode == crate::consensus::decision_mode::DecisionMode::ParentDecisions {
		capabilities
			.with(NodeCapability::ParentDecisions)
			.with(NodeCapability::ParentDecisionsBatchedLeases)
	} else {
		capabilities
	}
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
	install_tenant_kek_response(response.data, context)
}

fn install_tenant_kek_response(response: WendelonResponse, context: &str) -> std::io::Result<()> {
	match response {
		crate::consensus::WendelonResponse::Success => Ok(()),
		crate::consensus::WendelonResponse::Error(message) => Err(Error::new(
			ErrorKind::AlreadyExists,
			format!("{context}: {message}"),
		)),
		crate::consensus::WendelonResponse::Refused(refusal) => Err(Error::other(format!(
			"unexpected slab refusal for a non-slab command: {refusal:?}"
		))),
		crate::consensus::WendelonResponse::Op(refusal) => Err(Error::other(format!(
			"unexpected slab refusal for a non-slab command: {refusal:?}"
		))),
		crate::consensus::WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
			"unexpected lease results for a non-lease command: {results:?}"
		))),
		crate::consensus::WendelonResponse::Decision(result) => Err(Error::other(format!(
			"unexpected decision result for a non-decision command: {result:?}"
		))),
		crate::consensus::WendelonResponse::Root2Dormant => {
			let message = String::from("Root2 carrier is dormant, not applied");
			Err(Error::new(
				ErrorKind::Unsupported,
				format!("{context}: {message}"),
			))
		}
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
	tenant_request_response(response.data, context)
}

fn tenant_request_response(response: WendelonResponse, context: &str) -> std::io::Result<()> {
	match response {
		WendelonResponse::Success => Ok(()),
		WendelonResponse::Error(message) => Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{context}: {message}"),
		)),
		WendelonResponse::Refused(refusal) => Err(Error::other(format!(
			"unexpected slab refusal for a non-slab command: {refusal:?}"
		))),
		WendelonResponse::Op(refusal) => Err(Error::other(format!(
			"unexpected slab refusal for a non-slab command: {refusal:?}"
		))),
		WendelonResponse::LeaseResults(results) => Err(Error::other(format!(
			"unexpected lease results for a non-lease command: {results:?}"
		))),
		WendelonResponse::Decision(result) => Err(Error::other(format!(
			"unexpected decision result for a non-decision command: {result:?}"
		))),
		WendelonResponse::Root2Dormant => {
			let message = String::from("Root2 carrier is dormant, not applied");
			Err(Error::new(
				ErrorKind::Unsupported,
				format!("{context}: {message}"),
			))
		}
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

fn secondary_index_lifecycle_envelope(
	tenant_id: TenantId,
	descriptor: &SecondaryIndexDescriptor,
	local_node: NodeId,
	mutation: SchemaMutationKind,
) -> std::io::Result<SchemaUpdateEnvelope> {
	let payload = encode_secondary_index_lifecycle_complete(&SecondaryIndexLifecycleComplete {
		index_object_id: descriptor.index_object_id,
		replica_id: local_node,
	})?;
	Ok(SchemaUpdateEnvelope::wendelon_operator_intent(
		tenant_id,
		SchemaObjectRef {
			kind: SchemaObjectKind::Index,
			namespace: descriptor.namespace.clone(),
			name: descriptor.index_name.clone(),
		},
		SchemaGeneration::ZERO,
		SchemaGeneration::new(1),
		SchemaIntentId::new(*uuid::Uuid::new_v4().as_bytes()),
		mutation,
		payload,
	))
}

async fn run_secondary_index_lifecycle_tick(
	slab: &Arc<SlabManager>,
	state_machine: &WendelonStateMachine,
	control: &Arc<WendelonControlServer>,
	local_node: NodeId,
) -> std::io::Result<()> {
	let work = {
		let state = state_machine.state_snapshot().await;
		state
			.secondary_indexes
			.rows()
			.into_iter()
			.map(|(tenant_id, row)| {
				let table_indexes = state
					.secondary_indexes
					.for_table(tenant_id, row.descriptor.table_object_id)
					.into_iter()
					.filter(|candidate| candidate.state != SecondaryIndexState::Dropping)
					.map(|candidate| candidate.descriptor.clone())
					.collect::<Vec<_>>();
				(
					tenant_id,
					row.clone(),
					state
						.table_schemas
						.latest_by_id(tenant_id, row.descriptor.table_object_id)
						.cloned(),
					table_indexes,
				)
			})
			.collect::<Vec<_>>()
	};

	for (tenant_id, row, table, table_indexes) in work {
		match row.state {
			SecondaryIndexState::Building | SecondaryIndexState::Ready => {
				let Some(table) = table else {
					tracing::warn!(
						tenant_id = tenant_id.get(),
						index_object_id = ?row.descriptor.index_object_id,
						"secondary-index lifecycle skipped an index whose table descriptor is absent"
					);
					continue;
				};
				let has_receipt = row.has_backfill_completion(local_node);
				if !has_receipt {
					slab.establish_secondary_index_declaration_cut(
						tenant_id,
						&row.descriptor,
						&table_indexes,
						DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES,
					)
					.await?;
				}
				let report = slab
					.run_secondary_index_backfill_once(
						tenant_id,
						&table,
						&row.descriptor,
						SECONDARY_INDEX_BACKFILL_SLABS_PER_TICK,
						DEFAULT_SECONDARY_INDEX_ACTIVE_BUFFER_BYTES,
					)
					.await?;
				if report.historical_complete && !has_receipt {
					control
						.submit_secondary_index_lifecycle(secondary_index_lifecycle_envelope(
							tenant_id,
							&row.descriptor,
							local_node,
							SchemaMutationKind::IndexBackfillComplete,
						)?)
						.await?;
				}
			}
			SecondaryIndexState::Dropping => {
				slab.gc_secondary_index_segments_for_dropping_index(tenant_id, &row.descriptor)
					.await?;
				control
					.submit_secondary_index_lifecycle(secondary_index_lifecycle_envelope(
						tenant_id,
						&row.descriptor,
						local_node,
						SchemaMutationKind::IndexDropGcComplete,
					)?)
					.await?;
			}
		}
	}
	Ok(())
}

fn spawn_secondary_index_lifecycle_driver(
	slab: Arc<SlabManager>,
	state_machine: WendelonStateMachine,
	control: Arc<WendelonControlServer>,
	local_node: NodeId,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		loop {
			if let Err(error) =
				run_secondary_index_lifecycle_tick(&slab, &state_machine, &control, local_node)
					.await
			{
				tracing::warn!(
					node_id = local_node.id,
					error = %error,
					"secondary-index lifecycle tick failed"
				);
			}
			tokio::time::sleep(SECONDARY_INDEX_LIFECYCLE_INTERVAL).await;
		}
	})
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

struct LeaderGatedLsmCompactionDriver {
	slab: Arc<SlabManager>,
	leader: Arc<dyn LeaderStatus>,
	committer: Arc<dyn TierCommitter>,
	state_machine: WendelonStateMachine,
	/// ADR 0703: the data server that admits certified jobs, in mode.
	certified: Option<Arc<WendelonDataServer>>,
}

impl LeaderGatedLsmCompactionDriver {
	fn new(
		slab: Arc<SlabManager>,
		leader: Arc<dyn LeaderStatus>,
		committer: Arc<dyn TierCommitter>,
		state_machine: WendelonStateMachine,
	) -> Self {
		Self {
			slab,
			leader,
			committer,
			state_machine,
			certified: None,
		}
	}

	/// ADR 0703: in ParentDecisions mode every LSM job is a certified one,
	/// admitted by this data server.
	fn with_certified(mut self, data_server: Option<Arc<WendelonDataServer>>) -> Self {
		self.certified = data_server;
		self
	}
}

#[async_trait]
impl LsmCompactionDriver for LeaderGatedLsmCompactionDriver {
	async fn run_lsm_compaction_tick(
		&self,
		worker: &LsmCompactionWorker,
		now: crate::schema::HybridTimestamp,
	) -> Result<Option<crate::storage::CompactionOutcome>, crate::storage::CompactionFailure> {
		// The admission check: only the leader compacts, certified or not.
		if !self.leader.is_leader().await {
			return Ok(None);
		}
		if let Some(data_server) = &self.certified {
			// ADR 0704: one certified tier job, then the certified LSM job; a
			// tier step that fails leaves the LSM step to run.
			if let Err(error) = data_server
				.certified_tier_step(self.committer.as_ref())
				.await
			{
				tracing::warn!(%error, "certified tier step refused or failed; retried next tick");
			}
			return data_server
				.certified_lsm_step(worker, now, self.committer.as_ref())
				.await;
		}
		let state = self.state_machine.state_snapshot().await;
		self.slab
			.run_lsm_compaction_tick_via_control_ring_with_catalogs(
				worker,
				now,
				self.committer.as_ref(),
				&state.table_schemas,
				&state.secondary_indexes,
			)
			.await
	}
}

struct LeaderGatedLsmGcDriver {
	slab: Arc<SlabManager>,
	leader: Arc<dyn LeaderStatus>,
	committer: Arc<dyn TierCommitter>,
}

impl LeaderGatedLsmGcDriver {
	fn new(
		slab: Arc<SlabManager>,
		leader: Arc<dyn LeaderStatus>,
		committer: Arc<dyn TierCommitter>,
	) -> Self {
		Self {
			slab,
			leader,
			committer,
		}
	}
}

#[async_trait]
impl LsmGcDriver for LeaderGatedLsmGcDriver {
	async fn run_lsm_gc_tick(&self, worker: &LsmGcWorker) -> std::io::Result<Vec<SlabId>> {
		if !self.leader.is_leader().await {
			return Ok(Vec::new());
		}
		self.slab
			.sweep_lsm_delete_pending_once_via_control_ring(worker, self.committer.as_ref())
			.await
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
	fn withdraw_local(
		&self,
		_node_id: crate::schema::NodeId,
		_address: &str,
		_service: &std::sync::Arc<dyn crate::network::server::HrftService>,
	) -> std::io::Result<()> {
		// This provider does not retain publish_local registrations.
		Ok(())
	}

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

	/// ADR 0697: register this node's store on the ring (an exact replay is a
	/// receipt) and return its owner, for tests that write slab commands
	/// straight through Raft rather than through the committer.
	async fn registered_owner(node: &WendelonNode) -> crate::consensus::OwnerRef {
		let owner = node.slab.owner_ref().unwrap();
		let response = node
			.raft
			.client_write(WendelonRequest::RegisterIncarnation(
				crate::consensus::RegisterIncarnation {
					node: owner.node(),
					incarnation: owner.incarnation(),
					supersedes: None,
				},
			))
			.await
			.unwrap();
		assert_eq!(response.data, crate::consensus::WendelonResponse::Success);
		owner
	}

	fn tempdir(tag: &str) -> crate::test_support::TestDir {
		crate::test_support::TestDir::new(&format!("wendelon-bootstrap-{tag}"))
	}

	#[tokio::test]
	async fn node_bootstrap_refuses_a_second_owner_and_reopens_after_joined_shutdown() {
		let dir = tempdir("node-data-directory-owner");
		let cfg = NodeBootstrapConfig::dev(7001, dir.to_path_buf(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();

		let second = WendelonNode::bootstrap_single_node(cfg.clone()).await;
		let error = match second {
			Ok(node) => {
				node.shutdown().await.unwrap();
				panic!("a second node acquired an already-owned directory");
			}
			Err(error) => error,
		};
		let busy = error.get_ref().and_then(|source| {
			source.downcast_ref::<crate::storage::ownership::DataDirectoryBusy>()
		});
		assert!(busy.is_some(), "expected typed busy error, got {error}");

		node.shutdown().await.unwrap();
		let reopened = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		reopened.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn an_incomplete_storage_close_outranks_an_earlier_shutdown_error() {
		let dir = tempdir("shutdown-error-precedence");
		let mut cfg = NodeBootstrapConfig::dev(7004, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.storage_shutdown_timeout = Duration::from_millis(300);
		let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		// An in-flight request makes quiescence time out first; a held write
		// permit then keeps the storage close from completing. The caller must
		// still see the incomplete close, the one that decides termination.
		let _in_flight = node.lifecycle.track_work();
		let _permit = node._directory_owner.write_permit().unwrap();
		let error = node.shutdown().await.unwrap_err();
		assert!(
			error
				.get_ref()
				.is_some_and(|inner| inner.is::<crate::storage::ownership::ShutdownIncomplete>()),
			"expected ShutdownIncomplete, got {error}"
		);
	}

	/// ADR 0699: a node MVCC retention window is refused in ParentDecisions
	/// mode before anything starts; the same window is accepted otherwise.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn the_parent_decisions_mode_refuses_a_node_retention_window() {
		use crate::consensus::decision_mode::DecisionMode;
		use crate::storage::retention::MvccRetentionPolicy;
		let dir = tempdir("node-parent-decisions-retention");
		let mut cfg = NodeBootstrapConfig::dev(7005, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.mvcc_retention_policy = MvccRetentionPolicy::FixedWindow(Duration::from_secs(60));
		cfg.validate().expect("accepted outside the mode");
		cfg.decision_mode = DecisionMode::ParentDecisions;
		let error = cfg.validate().expect_err("refused in the mode");
		assert_eq!(error.kind(), ErrorKind::InvalidInput);
		assert!(error.to_string().contains("MVCC retention"), "{error}");
		let error = WendelonNode::bootstrap_single_node(cfg)
			.await
			.err()
			.expect("bootstrap refuses it before starting");
		assert_eq!(error.kind(), ErrorKind::InvalidInput);
		assert!(!dir.join("CLEAN_SHUTDOWN").exists());
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn the_parent_decisions_mode_serves_only_its_surfaces_and_binds_its_store() {
		use crate::consensus::decision_mode::DecisionMode;
		let dir = tempdir("node-parent-decisions");
		let mut cfg = NodeBootstrapConfig::dev(7004, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.decision_mode = DecisionMode::ParentDecisions;

		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		// ADR 0700: the store becomes Ready once it can serve: its base
		// validated, its signer bound and its primary ranges leased.
		let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
		loop {
			let snapshot = node.readiness_reporter().snapshot().await;
			if snapshot.is_ready() {
				break;
			}
			match snapshot.degraded_reason.as_deref() {
				Some(
					"CertifiedBaseUnvalidated"
					| "DecisionSignerUnbound"
					| "IncarnationNotCurrent"
					| "PrimaryRangeNotLeased",
				) => {
					assert!(
						tokio::time::Instant::now() < deadline,
						"never ready: {snapshot:?}"
					);
					tokio::time::sleep(Duration::from_millis(100)).await;
				}
				other => panic!("unexpected readiness {other:?}"),
			}
		}
		// ADR 0700: no CDC heartbeat producer runs in this mode.
		assert!(node.handles.cdc_locality_heartbeat.is_finished());
		let refuse = |msg_type: u8, version: u8| {
			use crate::network::tls::ActorIdentity;
			let router = node.router.clone();
			async move {
				let local = crate::schema::NodeId {
					id: 7004,
					_reserved: 0,
				};
				let context = crate::network::server::RequestContext {
					remote: Some(local),
					actor: ActorIdentity::data_node(local),
				};
				let frame = crate::network::core::HrftFrame {
					header: crate::schema::FrameHeader {
						magic: HRFT_MAGIC,
						version,
						msg_type,
						flags: 0,
						stream_id: 1,
						meta_len: 0,
						arrow_len: 0,
						trailing_len: 0,
					},
					meta: crate::network::pool::PooledBuf::orphan(Vec::new()),
					arrow_payload: None,
					trailing: None,
				};
				crate::network::server::HrftService::handle_frame(&*router, context, frame)
					.await
					.err()
					.map(|error| error.kind())
			}
		};
		// Surfaces outside the checkpoint are refused at dispatch, by a real
		// frame through the router; a served one passes the restriction (this
		// empty frame then fails on its own terms, not as Unsupported).
		assert_eq!(
			refuse(crate::schema::MsgType::BULK_WRITE_REQ.0, 1).await,
			Some(std::io::ErrorKind::Unsupported),
		);
		assert_eq!(
			refuse(crate::schema::MsgType::COUNTER_ADD_REQ.0, 1).await,
			Some(std::io::ErrorKind::Unsupported),
		);
		assert_ne!(
			refuse(crate::schema::MsgType::READ_REQ.0, 1).await,
			Some(std::io::ErrorKind::Unsupported),
		);
		// A Raft frame on the other authority's envelope is refused before decode.
		assert_eq!(
			refuse(crate::schema::MsgType::VOTE_REQ.0, 1).await,
			Some(std::io::ErrorKind::PermissionDenied),
		);
		// The other envelope on a non-Raft frame is refused where frames are read,
		// which these in-memory frames skip: `decision_mode::tests` covers it.
		node.shutdown().await.unwrap();

		// Its own authority reopens it. The other is refused, leaving it usable.
		let mut other = cfg.clone();
		other.decision_mode = DecisionMode::Today;
		assert!(WendelonNode::bootstrap_single_node(other).await.is_err());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		node.shutdown().await.unwrap();

		// A store with data whose marker has gone is never adopted, by either
		// authority. The interrupted-creation state, which holds only the first
		// control-log record, is covered where admission and the log store are tested.
		std::fs::remove_file(dir.join("PARENT_DECISIONS")).unwrap();
		let mut today = cfg.clone();
		today.decision_mode = DecisionMode::Today;
		assert!(WendelonNode::bootstrap_single_node(today).await.is_err());
		assert!(
			WendelonNode::bootstrap_single_node(cfg.clone())
				.await
				.is_err()
		);
		assert!(!dir.join("PARENT_DECISIONS").exists());
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_parent_decisions_creation_stopped_before_its_marker_resumes() {
		use crate::consensus::decision_mode::DecisionMode;
		let dir = tempdir("node-parent-decisions-resume");
		crate::consensus::raft_storage::WendelonLogStore::write_parent_decisions_holder_for_test(
			&dir,
		)
		.await
		.unwrap();
		let mut cfg = NodeBootstrapConfig::dev(7007, dir.to_path_buf(), "127.0.0.1:0".into());
		let mut today = cfg.clone();
		today.decision_mode = DecisionMode::Today;
		assert!(
			WendelonNode::bootstrap_single_node(today.clone())
				.await
				.is_err()
		);
		assert!(!dir.join("vote.wnd").exists(), "the refusal wrote nothing");

		cfg.decision_mode = DecisionMode::ParentDecisions;
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		assert!(dir.join("PARENT_DECISIONS").exists());
		assert!(node.data_server.decision_mode_refuses_service());
		node.shutdown().await.unwrap();
		assert!(WendelonNode::bootstrap_single_node(today).await.is_err());
		WendelonNode::bootstrap_single_node(cfg)
			.await
			.unwrap()
			.shutdown()
			.await
			.unwrap();
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn the_parent_decisions_mode_refuses_an_existing_store_and_a_real_profile() {
		use crate::consensus::decision_mode::DecisionMode;
		let dir = tempdir("node-parent-decisions-existing");
		let cfg = NodeBootstrapConfig::dev(7005, dir.to_path_buf(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		assert!(!node.data_server.decision_mode_refuses_service());
		node.shutdown().await.unwrap();
		let mut changed = cfg.clone();
		changed.decision_mode = DecisionMode::ParentDecisions;
		assert!(WendelonNode::bootstrap_single_node(changed).await.is_err());
		WendelonNode::bootstrap_single_node(cfg)
			.await
			.unwrap()
			.shutdown()
			.await
			.unwrap();

		let dir = tempdir("node-parent-decisions-profile");
		let mut real = NodeBootstrapConfig::dev(7006, dir.to_path_buf(), "127.0.0.1:0".into());
		real.decision_mode = DecisionMode::ParentDecisions;
		real.allow_dev_keys = false;
		assert!(WendelonNode::bootstrap_single_node(real).await.is_err());
	}

	#[tokio::test]
	async fn unclean_previous_shutdown_boots_in_recovery_mode_and_clean_does_not() {
		let dir = tempdir("node-recovery-mode");
		let cfg = NodeBootstrapConfig::dev(7003, dir.to_path_buf(), "127.0.0.1:0".into());

		// A never-owned directory has nothing to recover.
		let first = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		assert!(!first.recovered_after_unclean_shutdown());
		first.shutdown().await.unwrap();

		// A complete shutdown leaves the clean record; the next boot is normal
		// and consumes it.
		let second = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		assert!(!second.recovered_after_unclean_shutdown());
		assert!(!dir.join("CLEAN_SHUTDOWN").exists());
		second.shutdown().await.unwrap();

		// An owner that ends without a complete close (crash or kill) leaves no
		// record, so the next boot enters recovery mode and gates readiness.
		drop(crate::storage::ownership::DataDirectoryOwner::acquire_existing(&dir).unwrap());
		let recovering = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		assert!(recovering.recovered_after_unclean_shutdown());
		// Readiness stays gated until recovery validation finishes, then clears.
		let mut validation = recovering.restore_validation.clone();
		validation
			.wait_for(|status| matches!(status, RestoreValidationStatus::Complete))
			.await
			.unwrap();
		let reason = recovering
			.readiness_reporter()
			.snapshot()
			.await
			.degraded_reason;
		assert_ne!(reason.as_deref(), Some("RecoveringAfterUncleanShutdown"));
		recovering.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn shutdown_revokes_retained_slab_and_metadata_write_handles() {
		use crate::storage::AsyncFileOps;
		let dir = tempdir("shutdown-retained-handles");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7002,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let slab = node.slab.clone();
		let file_ops = crate::storage::IoUringFileOps::new(node.slab_io.clone());
		node.shutdown().await.unwrap();
		let new_owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let error = file_ops
			.write_all(&dir.join("late-write"), b"forbidden")
			.await
			.unwrap_err();
		assert!(
			error
				.get_ref()
				.unwrap()
				.is::<crate::storage::ownership::Closed>()
		);
		let error = slab
			.append_durable(
				TenantId::ZERO,
				Identity::blake3_tenant(TenantId::ZERO, b"late"),
				HybridTimestamp {
					physical_ms: 1,
					logical: 0,
					node_id: 7002,
				},
				b"late-write",
			)
			.await
			.unwrap_err();
		assert!(
			error
				.get_ref()
				.is_some_and(|error| error.is::<crate::storage::ownership::Closed>()),
			"{error}"
		);
		assert!(!dir.join("late-write").exists());
		drop(new_owner);
	}

	#[tokio::test]
	async fn shutdown_collects_task_error_and_still_closes_storage_workers() {
		struct SignalOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
		impl Drop for SignalOnDrop {
			fn drop(&mut self) {
				let _ = self.0.take().unwrap().send(());
			}
		}
		let dir = tempdir("shutdown-error-joins");
		let mut node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7003,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let retained_io = node.slab_io.clone();
		let (sent, received) = tokio::sync::oneshot::channel();
		let failed: JoinHandle<()> = tokio::spawn(async move {
			let _signal = SignalOnDrop(Some(sent));
			panic!("injected task failure");
		});
		received.await.unwrap();
		assert!(failed.is_finished());
		let old = std::mem::replace(&mut node.handles.ring_updater, failed);
		old.abort();
		let _ = old.await;
		assert!(node.shutdown().await.is_err());
		DataDirectoryOwner::acquire_existing(&dir).unwrap();
		let error = retained_io
			.submit_fsync(&Arc::new(std::fs::File::open("/dev/null").unwrap()))
			.await
			.err()
			.expect("closed worker");
		assert!(
			error
				.get_ref()
				.unwrap()
				.is::<crate::storage::ownership::Closed>()
		);
	}

	fn park_background_tiering(cfg: &mut NodeBootstrapConfig) {
		cfg.tiering.scan_interval = Duration::from_secs(300);
		cfg.tiering.min_slab_bytes = u64::MAX;
		cfg.tiering.max_plaintext_rewrites_per_pass = 0;
		cfg.tiering.max_dek_rewraps_per_pass = 0;
		cfg.tiering.op_driver_interval = Duration::from_secs(3600);
	}

	#[test]
	fn data_ring_lease_timing_keeps_one_validated_authority() {
		let timing = DataRingLeaseTiming::try_from_duration(Duration::from_millis(90_000)).unwrap();
		assert_eq!(timing.ttl_millis(), 90_000);
		assert_eq!(timing.renew_interval(), Duration::from_millis(30_000));
		for invalid in [
			Duration::ZERO,
			Duration::from_millis(2),
			Duration::from_nanos(3_000_001),
			Duration::from_millis(u64::from(u32::MAX) + 1),
		] {
			assert!(DataRingLeaseTiming::try_from_duration(invalid).is_err());
		}
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
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		// Ring has been seeded from the genesis epoch.
		assert_eq!(node.ring.snapshot().data_capable_nodes(), 1);
		// Raft reaches leadership asynchronously after initialize.
		wait_for_local_leader(&node).await;
		assert_eq!(node.raft.current_leader().await, Some(node.local_node.id));
		// Today runs the CDC heartbeat producer (ADR 0700 stops it only in
		// ParentDecisions mode).
		assert!(!node.handles.cdc_locality_heartbeat.is_finished());
	}

	#[tokio::test]
	async fn bootstrap_emits_crypto_startup_audit_and_seeds_default_tenant_kek() {
		let dir = tempdir("crypto-startup");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			13,
			dir.to_path_buf(),
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
		let expected_profile = if cfg!(debug_assertions) {
			"debug"
		} else {
			"release"
		};
		assert!(audit.contains(&format!("build_profile={expected_profile}")));
		assert!(audit.contains("mlock="));
	}

	#[tokio::test]
	async fn readiness_reporter_does_not_flap_during_successful_raft_audit_materialization() {
		let dir = tempdir("readiness-no-flap");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			23,
			dir.to_path_buf(),
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
			dir.to_path_buf(),
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
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();

		wait_for_local_leader(&node).await;
		node.data_server
			.trip_local_persistence_distrust_for_test()
			.await;

		node.shutdown().await.unwrap();

		let recovered = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			26,
			dir.to_path_buf(),
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
		let dir = tempdir("tcp-tls-missing");
		let result = WendelonNode::bootstrap_with_provider_at_tier(
			NodeBootstrapConfig::dev(9, dir.to_path_buf(), "127.0.0.1:0".into()),
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
		let cfg = NodeBootstrapConfig::dev(11, dir.to_path_buf(), "127.0.0.1:0".into());
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
		assert!(
			node.state_machine
				.state_snapshot()
				.await
				.e_safe_watermarks
				.get(&99)
				.copied()
				.is_some_and(|hlc| hlc >= commit_hlc)
		);

		node.shutdown().await.unwrap();

		let recovered = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			11,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		assert!(
			recovered
				.state_machine
				.state_snapshot()
				.await
				.e_safe_watermarks
				.get(&99)
				.copied()
				.is_some_and(|hlc| hlc >= commit_hlc)
		);
		wait_for_default_tenant_kek(&recovered).await;

		recovered.handles.abort();
	}

	/// ADR 0697 (DC-162 interim): a never-registered store's local-only image
	/// restores to its node as a fresh incarnation and serves. Once the node
	/// has registered its store on the ring, the same local-only restore is
	/// refused before any restore effect: a fresh store cannot take it over.
	#[tokio::test]
	async fn local_only_restore_requires_an_unregistered_target() {
		let tenant = crate::schema::TenantId::ZERO;
		let key = Identity::blake3(b"local-only-row");
		let hlc = crate::schema::HybridTimestamp {
			physical_ms: 13,
			logical: 0,
			node_id: 13,
		};
		let fresh_restore_dir = |source: &std::path::Path, tag: &str| {
			let dir = tempdir(tag);
			std::fs::create_dir_all(dir.join(".wendelon")).unwrap();
			std::fs::copy(
				source.join(".wendelon/dev-root-kek.bin"),
				dir.join(".wendelon/dev-root-kek.bin"),
			)
			.unwrap();
			dir
		};

		// Never registered: restores and serves.
		let dir = tempdir("local-only-restore-source");
		let cfg = NodeBootstrapConfig::dev(13, dir.to_path_buf(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		node.slab
			.append_durable(tenant, key, hlc, b"local-only-row")
			.await
			.unwrap();
		let backup = node.export_cluster_backup().await.unwrap();
		node.shutdown().await.unwrap();
		assert!(
			!backup
				.control
				.replayed_state()
				.unwrap()
				.incarnations
				.contains_key(&13),
			"premise: the source store never registered on the ring"
		);
		let restore_dir = fresh_restore_dir(&dir, "local-only-restore-target");
		let mut restore_cfg = cfg.clone();
		restore_cfg.data_dir = restore_dir.to_path_buf();
		let restored = WendelonNode::restore_from_cluster_backup(&backup, restore_cfg.clone())
			.await
			.unwrap();
		wait_for_ready(&restored).await;
		let location = restored.slab.lookup(tenant, key).unwrap().unwrap();
		assert_eq!(
			restored.slab.read(location).await.unwrap(),
			b"local-only-row"
		);
		restored.shutdown().await.unwrap();
		// The restored store reopens as itself and still serves.
		let reopened = WendelonNode::bootstrap_single_node(restore_cfg.clone())
			.await
			.unwrap();
		wait_for_ready(&reopened).await;
		let location = reopened.slab.lookup(tenant, key).unwrap().unwrap();
		assert_eq!(
			reopened.slab.read(location).await.unwrap(),
			b"local-only-row"
		);
		reopened.shutdown().await.unwrap();

		// Registered: the same kind of local-only restore is refused.
		let dir = tempdir("local-only-registered-source");
		let cfg = NodeBootstrapConfig::dev(13, dir.to_path_buf(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		node.slab
			.append_durable(tenant, key, hlc, b"local-only-row")
			.await
			.unwrap();
		registered_owner(&node).await;
		let backup = node.export_cluster_backup().await.unwrap();
		node.shutdown().await.unwrap();
		let restore_dir = fresh_restore_dir(&dir, "local-only-registered-target");
		let mut restore_cfg = cfg.clone();
		restore_cfg.data_dir = restore_dir.to_path_buf();
		let error = WendelonNode::restore_from_cluster_backup(&backup, restore_cfg)
			.await
			.err()
			.expect("a registered node cannot be taken over by a fresh store");
		assert!(
			error.get_ref().is_some_and(|cause| matches!(
				cause.downcast_ref::<crate::backup::UnsupportedRestore>(),
				Some(crate::backup::UnsupportedRestore::IncarnationTransition { node: 13 })
			)),
			"{error}"
		);
		for artifact in [
			"storage_index.redb",
			"control.wal",
			"STORAGE_INDEX_RESTORE_AUTHORITY",
		] {
			assert!(
				!restore_dir.join(artifact).exists(),
				"restore wrote {artifact}"
			);
		}
	}

	/// ADR 0697: an image whose embedded storage or control backup manifest
	/// declares a format this build does not read is refused at both restore
	/// entry points with no restore effect, though its outer hashes are valid
	/// and its enclosed bodies are current.
	#[tokio::test]
	async fn restore_refuses_an_unsupported_embedded_backup_format_at_both_entries() {
		let dir = tempdir("embedded-format-source");
		let cfg = NodeBootstrapConfig::dev(14, dir.to_path_buf(), "127.0.0.1:0".into());
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		node.slab
			.append_durable(
				crate::schema::TenantId::ZERO,
				Identity::blake3(b"embedded-format-row"),
				crate::schema::HybridTimestamp {
					physical_ms: 14,
					logical: 0,
					node_id: 14,
				},
				b"embedded-format-row",
			)
			.await
			.unwrap();
		let backup = node.export_cluster_backup().await.unwrap();
		node.shutdown().await.unwrap();
		let state = backup.control.replayed_state().unwrap();
		let no_effects = |target: &std::path::Path| {
			for artifact in [
				"storage_index.redb",
				"control.wal",
				"vote.wnd",
				"snapshot.wnd",
				"STORAGE_INDEX_RESTORE_AUTHORITY",
				crate::storage::manager::AWAITING_CLUSTER_MARKER,
			] {
				assert!(!target.join(artifact).exists(), "restore wrote {artifact}");
			}
		};
		let refused = |error: std::io::Error, artifact: &str, found: u16| {
			let format = error
				.get_ref()
				.and_then(|cause| cause.downcast_ref::<crate::backup::UnsupportedBackupFormat>())
				.copied();
			assert_eq!(
				format.map(|format| (format.artifact.to_string(), format.found)),
				Some((artifact.to_string(), found)),
				"{error}"
			);
		};
		for (artifact, found) in [
			("storage", 1_u16),
			("storage", 99),
			("control", 0),
			("control", 99),
		] {
			let mut control = backup.control.clone();
			let mut nodes = backup.nodes.clone();
			match artifact {
				"storage" => nodes[0].storage.manifest.format_version = found,
				_ => control.manifest.format_version = found,
			}
			let image =
				crate::backup::ClusterBackupImage::assemble(control, &state, nodes).unwrap();

			let target = tempdir("embedded-format-cluster");
			std::fs::create_dir_all(target.join(".wendelon")).unwrap();
			std::fs::copy(
				dir.join(".wendelon/dev-root-kek.bin"),
				target.join(".wendelon/dev-root-kek.bin"),
			)
			.unwrap();
			let mut same = cfg.clone();
			same.data_dir = target.to_path_buf();
			let error = WendelonNode::restore_from_cluster_backup(&image, same)
				.await
				.err()
				.expect("an unsupported embedded format must not restore");
			refused(error, artifact, found);
			no_effects(&target);

			let target = tempdir("embedded-format-replacement");
			std::fs::create_dir_all(target.join(".wendelon")).unwrap();
			std::fs::copy(
				dir.join(".wendelon/dev-root-kek.bin"),
				target.join(".wendelon/dev-root-kek.bin"),
			)
			.unwrap();
			let mut replacement =
				NodeBootstrapConfig::dev(15, target.to_path_buf(), "127.0.0.1:0".into());
			replacement.cluster = ClusterBootstrapConfig::Join {
				seeds: vec![SeedNodeConfig {
					node_id: 14,
					address: "127.0.0.1:1".into(),
				}],
			};
			let error =
				WendelonNode::restore_replacement_from_cluster_backup(&image, 14, replacement)
					.await
					.err()
					.expect("an unsupported embedded format must not restore");
			refused(error, artifact, found);
			no_effects(&target);
		}
	}

	/// ADR 0697 R7, through a real restarted node. An Ack that reached the ring
	/// whose response was lost is redriven after restart, the row retires by the
	/// canonical prefix with no sequence reused, and a duplicate of the pruned
	/// operation applies nothing.
	#[tokio::test]
	async fn a_restarted_node_redrives_a_lost_ack_and_a_pruned_duplicate_applies_nothing() {
		use crate::consensus::slab_ops::{AckOutcomes, OpOutcome, OpResponse};
		let dir = tempdir("op-restart");
		let mut cfg = NodeBootstrapConfig::dev(16, dir.to_path_buf(), "127.0.0.1:0".into());
		park_background_tiering(&mut cfg);
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&node).await;
		let owner = registered_owner(&node).await;
		let request = node
			.slab
			.retire_request_for_test(owner.slab(SlabId::new(0x7000)))
			.await;
		let decided = node.raft.client_write(request.clone()).await.unwrap();
		assert!(matches!(decided.data, WendelonResponse::Op(_)));
		let seq = match &request {
			WendelonRequest::SlabOp(op) => op.op_seq,
			other => panic!("expected a slab op, got {other:?}"),
		};
		let index = node.slab.require_op_index().unwrap();
		assert_eq!(index.load_ack_pending().await.unwrap(), vec![(seq, false)]);
		// The Ack lands on the ring; its response never reaches the local row.
		let acked = node
			.raft
			.client_write(WendelonRequest::AckOutcomes(AckOutcomes {
				owner,
				seqs: vec![seq],
			}))
			.await
			.unwrap();
		assert!(matches!(
			acked.data,
			WendelonResponse::Op(OpResponse::Ack(ref receipt)) if receipt.acked_through >= seq
		));
		assert_eq!(index.load_ack_pending().await.unwrap(), vec![(seq, false)]);
		node.shutdown().await.unwrap();

		// Same store, driver running.
		cfg.tiering.op_driver_interval = Duration::from_millis(20);
		let restarted = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&restarted).await;
		let index = restarted.slab.require_op_index().unwrap();
		tokio::time::timeout(Duration::from_secs(60), async {
			while !index.load_ack_pending().await.unwrap().is_empty() {
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		})
		.await
		.expect("the driver redrives after restart with no later handoff or election");

		let before = restarted.state_machine.state_snapshot().await;
		let duplicate = restarted.raft.client_write(request).await.unwrap();
		assert!(matches!(
			duplicate.data,
			WendelonResponse::Op(OpResponse::Receipt(ref receipt))
				if receipt.outcome == OpOutcome::Unavailable
		));
		let after = restarted.state_machine.state_snapshot().await;
		assert_eq!(before.tiered_slabs, after.tiered_slabs, "applies nothing");
		assert_eq!(
			before.op_tables[&owner].acked_through,
			after.op_tables[&owner].acked_through
		);
		assert_eq!(
			before.op_tables[&owner].outcomes.len(),
			after.op_tables[&owner].outcomes.len(),
			"records nothing"
		);
		let next = restarted
			.slab
			.retire_request_for_test(owner.slab(SlabId::new(0x7001)))
			.await;
		assert!(
			matches!(&next, WendelonRequest::SlabOp(op) if op.op_seq > seq),
			"no sequence is reused"
		);
		restarted.shutdown().await.unwrap();
	}

	#[tokio::test]
	/// ADR 0697 (DC-162 interim): restoring a store with tiered data to a
	/// fresh incarnation is refused before any restore effect, from the
	/// backup's replayed control state; clearing the image's reference sets
	/// cannot hide the committed pointers.
	async fn single_node_restore_with_tiered_data_is_refused_before_any_restore_effect() {
		let dir = tempdir("restart-tiered");
		let l2_dir = tempdir("restart-tiered-l2");
		let mut cfg = NodeBootstrapConfig::dev(12, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
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
			.client_write(
				node.slab
					.tier_request_for_test(TieredSlab {
						slab: registered_owner(&node).await.slab(sealed_id),
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
						certified: None,
					})
					.await,
			)
			.await
			.unwrap();
		assert!(node.slab.is_tiered(sealed_id).await);
		assert!(
			!dir.join(format!("slab_{sealed_id:016x}.wnd")).exists(),
			"tiered slab should be unlinked from L1 after apply"
		);

		let backup = node.export_cluster_backup().await.unwrap();
		node.shutdown().await.unwrap();

		// Missing-index rebuild is authorized by validated recovery, never
		// inferred from the file's absence during plain bootstrap.
		let restore_dir = tempdir("restart-tiered-validated-restore");
		// Backups carry wrapped tenant keys; the provider is provisioned
		// separately. Preserve the original fixture's provider across dirs.
		std::fs::create_dir_all(restore_dir.join(".wendelon")).unwrap();
		std::fs::copy(
			dir.join(".wendelon/dev-root-kek.bin"),
			restore_dir.join(".wendelon/dev-root-kek.bin"),
		)
		.unwrap();
		cfg.data_dir = restore_dir.to_path_buf();
		assert!(!cfg.data_dir.join("storage_index.redb").exists());

		let no_restore_effects = |dir: &std::path::Path| {
			for artifact in [
				"storage_index.redb",
				"control.wal",
				"vote.wnd",
				"snapshot.wnd",
				"STORAGE_INDEX_RESTORE_AUTHORITY",
				crate::storage::manager::AWAITING_CLUSTER_MARKER,
			] {
				assert!(!dir.join(artifact).exists(), "restore wrote {artifact}");
			}
			assert!(
				std::fs::read_dir(dir).unwrap().all(|entry| !entry
					.unwrap()
					.file_name()
					.to_string_lossy()
					.starts_with("slab_")),
				"restore wrote a slab file"
			);
		};

		// The real image: refused as cold data a fresh store would not own.
		let error = WendelonNode::restore_from_cluster_backup(&backup, cfg.clone())
			.await
			.err()
			.expect("a restore with tiered data must be refused");
		assert!(
			error.get_ref().is_some_and(|cause| matches!(
				cause.downcast_ref::<crate::backup::UnsupportedRestore>(),
				Some(crate::backup::UnsupportedRestore::ColdReferences { .. })
			)),
			"{error}"
		);
		no_restore_effects(&cfg.data_dir);

		// A re-assembled image (valid outer hashes) whose storage reference
		// sets were cleared: the replayed control state still names the
		// source's committed pointers, so it cannot pass as local-only.
		let state = backup.control.replayed_state().unwrap();
		let mut nodes = backup.nodes.clone();
		nodes[0].storage.manifest.tiered_slabs.clear();
		nodes[0].storage.manifest.omitted_rehydrated_slabs.clear();
		let tampered =
			crate::backup::ClusterBackupImage::assemble(backup.control.clone(), &state, nodes)
				.unwrap();
		tampered.validate_integrity().unwrap();
		let hidden_dir = tempdir("restart-tiered-hidden-cold");
		std::fs::create_dir_all(hidden_dir.join(".wendelon")).unwrap();
		std::fs::copy(
			dir.join(".wendelon/dev-root-kek.bin"),
			hidden_dir.join(".wendelon/dev-root-kek.bin"),
		)
		.unwrap();
		let mut hidden_cfg = cfg.clone();
		hidden_cfg.data_dir = hidden_dir.to_path_buf();
		let error = WendelonNode::restore_from_cluster_backup(&tampered, hidden_cfg)
			.await
			.err()
			.expect("cleared reference sets cannot hide committed cold pointers");
		assert!(
			error.get_ref().is_some_and(|cause| matches!(
				cause.downcast_ref::<crate::backup::BackupSourceMismatch>(),
				Some(crate::backup::BackupSourceMismatch::ReferencesDiffer { .. })
			)),
			"refused by the replayed control state, not by a hash check: {error}"
		);
		no_restore_effects(&hidden_dir);

		// The same image with its control WAL re-declared at v5 (outer hashes
		// recomputed): refused as an unsupported control format before any
		// restore effect, never replayed as an empty or default state.
		let legacy = crate::backup::ClusterBackupImage::assemble(
			backup.control.with_wal_format_version_for_test(5),
			&state,
			backup.nodes.clone(),
		)
		.unwrap();
		legacy.validate_integrity().unwrap();
		let legacy_dir = tempdir("restart-tiered-legacy-control");
		std::fs::create_dir_all(legacy_dir.join(".wendelon")).unwrap();
		std::fs::copy(
			dir.join(".wendelon/dev-root-kek.bin"),
			legacy_dir.join(".wendelon/dev-root-kek.bin"),
		)
		.unwrap();
		let mut legacy_cfg = cfg.clone();
		legacy_cfg.data_dir = legacy_dir.to_path_buf();
		let error = WendelonNode::restore_from_cluster_backup(&legacy, legacy_cfg)
			.await
			.err()
			.expect("a pre-v6 control image must not restore");
		let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
		let mut unsupported = false;
		while let Some(current) = cause {
			unsupported |= current.is::<crate::consensus::UnsupportedControlFormat>();
			// An io::Error's own source() skips its payload; walk the payload.
			cause = match current.downcast_ref::<std::io::Error>() {
				Some(io) => io
					.get_ref()
					.map(|inner| inner as &(dyn std::error::Error + 'static)),
				None => current.source(),
			};
		}
		assert!(unsupported, "refused for the wrong reason: {error}");
		no_restore_effects(&legacy_dir);
	}

	#[tokio::test]
	async fn a_rolled_back_tier_pointer_never_serves_a_version_its_committed_generation_pruned() {
		use crate::schema::table::TableSchemaCatalog;
		use crate::storage::manager::PreparedTieredRetentionRewrite;
		let dir = tempdir("rolled-back-node-index");
		let l2_dir = tempdir("rolled-back-node-index-l2");
		let mut cfg = NodeBootstrapConfig::dev(12, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1 << 20;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
		});
		park_background_tiering(&mut cfg);
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;
		let tenant_id = TenantId::ZERO;
		let (k, j) = (
			Identity::blake3(b"rolled-back-k"),
			Identity::blake3(b"rolled-back-j"),
		);
		let hlc = |ms| HybridTimestamp {
			physical_ms: ms,
			logical: 0,
			node_id: 12,
		};
		for (key, time, bytes) in [
			(k, 10, b"k-10".as_slice()),
			(k, 20, b"k-20".as_slice()),
			(j, 30, b"j-30".as_slice()),
		] {
			node.slab
				.append_durable(tenant_id, key, hlc(time), bytes)
				.await
				.unwrap();
		}
		let slab_id = node.slab.force_seal_active_for_test().await;
		let raw = node.slab.read_sealed_raw(slab_id).await.unwrap();
		let encrypted = node.slab.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
		let tier = TieredSlab {
			slab: registered_owner(&node).await.slab(slab_id),
			l2_uri: l2
				.put("rolled-back-node/generation-0", &encrypted.bytes)
				.await
				.unwrap(),
			payload_hash: crate::hash::content::digest(&encrypted.bytes),
			is_encrypted: true,
			tenant_id: Some(tenant_id),
			tenant_kek_id: Some(encrypted.tenant_kek_id),
			stats_format_version: encrypted.stats_locator.map(|v| v.stats_format_version),
			stats_block_len: encrypted.stats_locator.map(|v| v.stats_block_len),
			stats_block_hash128: encrypted.stats_locator.map(|v| v.stats_block_hash128),
			tombstone_metadata: node
				.slab
				.slab_row_tombstone_metadata(slab_id)
				.expect("fixture tombstone summary must be readable"),
			supersession_metadata: node
				.slab
				.slab_supersession_metadata(slab_id)
				.expect("fixture supersession summary must be readable"),
			certified: None,
		};
		node.raft
			.client_write(node.slab.tier_request_for_test(tier.clone()).await)
			.await
			.unwrap();
		assert!(
			node.slab
				.lookup_as_of(tenant_id, k, hlc(15))
				.unwrap()
				.is_some()
		);
		node.shutdown().await.unwrap();
		let index_path = dir.join("storage_index.redb");
		let generation_0 = std::fs::read(&index_path).unwrap();

		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&node).await;
		wait_for_ready(&node).await;
		let pointer = node.slab.tier_pointer(slab_id).await.unwrap();
		let PreparedTieredRetentionRewrite::Rewrite {
			encrypted_bytes,
			payload_hash,
			tombstone_metadata,
			supersession_metadata,
			..
		} = node
			.slab
			.prepare_tiered_retention_rewrite(&pointer, hlc(25), &TableSchemaCatalog::default())
			.await
			.unwrap()
			.expect("a retention rewrite")
		else {
			panic!("a rewrite with survivors is required");
		};
		let replacement = TieredSlab {
			l2_uri: l2
				.put("rolled-back-node/generation-1", &encrypted_bytes)
				.await
				.unwrap(),
			payload_hash,
			tombstone_metadata,
			supersession_metadata,
			stats_format_version: None,
			stats_block_len: None,
			stats_block_hash128: None,
			..tier
		};
		node.raft
			.client_write(node.slab.tier_request_for_test(replacement.clone()).await)
			.await
			.unwrap();
		assert!(
			node.slab
				.lookup_as_of(tenant_id, k, hlc(15))
				.unwrap()
				.is_none(),
			"premise: committed generation prunes k@10"
		);
		node.shutdown().await.unwrap();
		// Roll back only the local index. Durable Raft state still certifies
		// generation 1, and real bootstrap must compare before exposing reads.
		std::fs::write(&index_path, generation_0).unwrap();
		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		wait_for_ready(&recovered).await;
		let mgr = &recovered.slab;
		assert_eq!(
			mgr.tier_pointer(slab_id).await.unwrap().l2_uri,
			replacement.l2_uri
		);
		assert!(
			mgr.lookup_as_of(tenant_id, k, hlc(15)).unwrap().is_none(),
			"k@10, pruned by the committed generation, was served from rolled-back authority"
		);
		for (key, expected) in [(k, b"k-20".as_slice()), (j, b"j-30".as_slice())] {
			if let Some(location) = mgr.lookup(tenant_id, key).unwrap() {
				match mgr.read(location).await {
					Ok(bytes) => assert_eq!(bytes, expected),
					Err(error) => assert!(
						crate::storage::manager::stale_location(&error).is_some(),
						"{error}"
					),
				}
			}
		}
		recovered.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn committed_retirement_before_local_apply_stays_fenced_after_bootstrap() {
		let dir = tempdir("dc112-retirement-reopen");
		let l2_dir = tempdir("dc112-retirement-reopen-l2");
		let mut cfg = NodeBootstrapConfig::dev(12, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
		});
		park_background_tiering(&mut cfg);
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;
		let tenant_id = TenantId::ZERO;
		let key = Identity::blake3(b"dc112-old-tier");
		let ts = HybridTimestamp {
			physical_ms: 10,
			logical: 0,
			node_id: node.local_node.id,
		};
		node.slab
			.append(tenant_id, key, ts, b"old-tier")
			.await
			.unwrap();
		node.slab
			.append(
				tenant_id,
				Identity::blake3(b"dc112-rotate"),
				HybridTimestamp {
					physical_ms: 20,
					..ts
				},
				b"rotate",
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
			.client_write(
				node.slab
					.tier_request_for_test(TieredSlab {
						slab: registered_owner(&node).await.slab(sealed_id),
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
						certified: None,
					})
					.await,
			)
			.await
			.unwrap();
		assert!(node.slab.is_tiered(sealed_id).await);
		let old_location = node.slab.lookup(tenant_id, key).unwrap().unwrap();
		let old_uri = node.slab.tier_pointer(sealed_id).await.unwrap().l2_uri;
		node.slab.fail_next_retirement_transaction_for_test();
		node.raft
			.client_write(
				node.slab
					.retire_request_for_test(registered_owner(&node).await.slab(sealed_id))
					.await,
			)
			.await
			.unwrap();
		assert!(
			!node
				.state_machine
				.state_snapshot()
				.await
				.tiered_slabs
				.contains_key(&node.slab.owner_ref().unwrap().slab(sealed_id))
		);
		assert!(
			node.slab.is_tiered(sealed_id).await,
			"injected failure preserves the unapplied local tier pointer"
		);
		node.shutdown().await.unwrap();
		// Model the unclean-owner restart while preserving the exact committed/unapplied disk cut.
		drop(crate::storage::ownership::DataDirectoryOwner::acquire_existing(&dir).unwrap());
		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		assert!(recovered.recovered_after_unclean_shutdown());
		let verification = tokio::time::timeout(
			Duration::from_secs(60),
			recovered.slab.wait_tier_authority_ready(),
		)
		.await
		.expect("authority attempt must reach an explicit outcome");
		assert!(
			verification.is_ok(),
			"validated bootstrap completes the committed retirement"
		);
		recovered
			.slab
			.assert_retirement_completed_for_test(tenant_id, key, old_location, &old_uri)
			.await;
		wait_for_ready(&recovered).await;
		recovered.handles.abort();
	}

	#[tokio::test]
	async fn replacement_import_leaves_awaiting_cluster_marker_across_owner_restart() {
		let source_dir = tempdir("dc112-replacement-source");
		let source = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			81,
			source_dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		wait_for_local_leader(&source).await;
		let backup = source.export_cluster_backup().await.unwrap();
		let target_dir = tempdir("dc112-replacement-target");
		let mut cfg = NodeBootstrapConfig::dev(82, target_dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.cluster = ClusterBootstrapConfig::Join {
			seeds: vec![SeedNodeConfig {
				node_id: 81,
				address: "127.0.0.1:1".into(),
			}],
		};
		let owner =
			crate::storage::ownership::DataDirectoryOwner::acquire_existing(&target_dir).unwrap();
		WendelonNode::prepare_replacement_from_cluster_backup(&backup, 81, &cfg, &owner)
			.await
			.unwrap();
		let marker = target_dir.join("TIER_AUTHORITY_AWAITING_CLUSTER");
		assert!(
			marker.is_file(),
			"replacement import must durably retain its awaiting-cluster fence"
		);
		drop(owner);
		let owner =
			crate::storage::ownership::DataDirectoryOwner::acquire_existing(&target_dir).unwrap();
		assert!(
			marker.is_file(),
			"a restart before a complete leader state must retain the fence"
		);
		drop(owner);
		source.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn snapshot_publication_crash_before_comparison_reopens_fenced() {
		let dir = tempdir("dc112-snapshot-crash-reopen");
		let l2_dir = tempdir("dc112-snapshot-crash-reopen-l2");
		let mut cfg = NodeBootstrapConfig::dev(12, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
		});
		park_background_tiering(&mut cfg);
		let l2 = cfg.l2.as_ref().unwrap().build().unwrap();
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		wait_for_local_leader(&node).await;
		wait_for_default_tenant_kek(&node).await;
		let tenant_id = TenantId::ZERO;
		let key = Identity::blake3(b"dc112-old-tier");
		let ts = HybridTimestamp {
			physical_ms: 10,
			logical: 0,
			node_id: node.local_node.id,
		};
		node.slab
			.append(tenant_id, key, ts, b"old-tier")
			.await
			.unwrap();
		node.slab
			.append(
				tenant_id,
				Identity::blake3(b"dc112-rotate"),
				HybridTimestamp {
					physical_ms: 20,
					..ts
				},
				b"rotate",
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
			.client_write(
				node.slab
					.tier_request_for_test(TieredSlab {
						slab: registered_owner(&node).await.slab(sealed_id),
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
						certified: None,
					})
					.await,
			)
			.await
			.unwrap();
		assert!(node.slab.is_tiered(sealed_id).await);
		let old_location = node.slab.lookup(tenant_id, key).unwrap().unwrap();
		let old_uri = node.slab.tier_pointer(sealed_id).await.unwrap().l2_uri;
		use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
		let mut committed = node.state_machine.state_snapshot().await;
		assert!(
			committed.last_applied_log_id.is_some(),
			"tier publication is committed"
		);
		assert!(
			committed
				.tiered_slabs
				.remove(&node.slab.owner_ref().unwrap().slab(sealed_id))
				.is_some()
		);
		let mut source = WendelonStateMachine::new();
		source
			.seed_complete_snapshot_state_for_test(committed)
			.await;
		let mut builder = RaftStateMachine::get_snapshot_builder(&mut source).await;
		let built = builder.build_snapshot().await.unwrap();
		let entered = Arc::new(tokio::sync::Semaphore::new(0));
		let release = Arc::new(tokio::sync::Semaphore::new(0));
		node.state_machine
			.pause_snapshot_after_publication_for_test(entered.clone(), release);
		let mut target = node.state_machine.clone();
		let install = tokio::spawn(async move {
			RaftStateMachine::install_snapshot(&mut target, &built.meta, built.snapshot).await
		});
		tokio::time::timeout(Duration::from_secs(60), entered.acquire())
			.await
			.unwrap()
			.unwrap()
			.forget();
		install.abort();
		assert!(install.await.unwrap_err().is_cancelled());
		node.shutdown().await.unwrap();
		drop(crate::storage::ownership::DataDirectoryOwner::acquire_existing(&dir).unwrap());
		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		assert!(recovered.recovered_after_unclean_shutdown());
		let result = tokio::time::timeout(
			Duration::from_secs(60),
			recovered.slab.wait_tier_authority_ready(),
		)
		.await
		.expect("authority attempt must reach an explicit outcome");
		assert!(
			result.is_ok(),
			"validated bootstrap completes the committed retirement"
		);
		recovered
			.slab
			.assert_retirement_completed_for_test(tenant_id, key, old_location, &old_uri)
			.await;
		wait_for_ready(&recovered).await;
		recovered.handles.abort();
	}

	#[tokio::test]
	async fn recovery_blocks_readiness_when_tiered_reference_validation_fails() {
		let dir = tempdir("restart-tiered-missing-l2");
		let l2_dir = tempdir("restart-tiered-missing-l2-store");
		let mut cfg = NodeBootstrapConfig::dev(15, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
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
			.client_write(
				node.slab
					.tier_request_for_test(TieredSlab {
						slab: registered_owner(&node).await.slab(sealed_id),
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
						certified: None,
					})
					.await,
			)
			.await
			.unwrap();

		l2.delete(&l2_uri).await.unwrap();

		node.shutdown().await.unwrap();

		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			let snapshot = recovered.readiness_reporter().snapshot().await;
			assert!(!snapshot.is_ready(), "missing authority blocks readiness");
			let reason = snapshot.degraded_reason.as_deref().unwrap_or("");
			assert!(
				!reason.starts_with("RestoreValidationFailed:"),
				"availability loss must remain recoverable: {reason}"
			);
			if reason.starts_with("TierAuthorityRetry(") {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"missing authority must report backoff: {snapshot:?}"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		assert_eq!(
			l2.put(
				&format!("sealed/slab_{sealed_id:016x}.wnd"),
				&encrypted.bytes
			)
			.await
			.unwrap(),
			l2_uri
		);
		wait_for_ready(&recovered).await;

		recovered.handles.abort();
	}

	async fn tier_authority_probe(
		node: &WendelonNode,
	) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
		let tenant = TenantId::ZERO;
		let key = b"tier-authority-absent";
		let meta = crate::schema::ReadRequestMeta {
			min_hlc: bytemuck::Zeroable::zeroed(),
			expected_epoch: node.epoch_tx.borrow().epoch,
			tenant_id: tenant,
			consistency: crate::schema::Consistency::STALE,
			_pad: [0; 3],
			key_identity: Identity::blake3_tenant(tenant, key),
			key_len: key.len() as u32,
			_pad2: [0; 8],
			as_of_hlc: bytemuck::Zeroable::zeroed(),
		};
		let batch = arrow::record_batch::RecordBatch::try_new(
			Arc::new(crate::schema::read_key_schema()),
			vec![Arc::new(arrow::array::LargeBinaryArray::from_iter_values(
				[key.as_slice()],
			))],
		)
		.unwrap();
		let bytes = crate::network::ipc::wire_encode_batch(&batch).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&bytes);
		crate::network::router::DataRingServer::handle_data_frame(
			node.data_server.as_ref(),
			crate::network::server::RequestContext {
				remote: Some(node.local_node),
				actor: crate::network::tls::ActorIdentity::new(
					crate::network::tls::ActorRole::Operator,
					None,
					None,
					vec!["cluster".into()],
					vec![1, 2, 3, 4],
				)
				.unwrap(),
			},
			crate::network::core::HrftFrame {
				header: crate::schema::FrameHeader {
					magic: crate::network::core::HRFT_MAGIC,
					version: 1,
					msg_type: crate::schema::MsgType::READ_REQ.0,
					flags: 0,
					stream_id: 1,
					meta_len: meta_buf.len() as u32,
					arrow_len: arrow_buf.len() as u32,
					trailing_len: 0,
				},
				meta: meta_buf,
				arrow_payload: Some(arrow_buf),
				trailing: None,
			},
		)
		.await
	}

	async fn assert_tier_authority_outage_recovers(missing_mirror: bool, check_admission: bool) {
		let dir = tempdir(if missing_mirror {
			"tier-authority-missing-mirror"
		} else {
			"tier-authority-retry"
		});
		let l2_dir = tempdir("tier-authority-retry-store");
		let mut cfg = NodeBootstrapConfig::dev(15, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
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
			.client_write(
				node.slab
					.tier_request_for_test(TieredSlab {
						slab: registered_owner(&node).await.slab(sealed_id),
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
						certified: None,
					})
					.await,
			)
			.await
			.unwrap();

		l2.delete(&l2_uri).await.unwrap();

		if missing_mirror {
			// Keep redb available, but remove this committed tier and its mirror rows.
			node.slab
				.remove_tier_pointer_and_mirror_rows_for_test(sealed_id)
				.await
				.unwrap();
		}
		node.shutdown().await.unwrap();
		let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
		let snapshot = recovered.readiness_reporter().snapshot().await;
		assert!(!snapshot.is_ready());
		if check_admission {
			assert!(
				snapshot
					.degraded_reason
					.as_deref()
					.is_some_and(|reason| reason.starts_with("TierAuthority")),
				"{snapshot:?}"
			);
		}
		wait_for_local_leader(&recovered).await;
		// A control-ring commit still reaches apply while data authority is pending.
		recovered
			.raft
			.client_write(WendelonRequest::ResumeTenant(tenant_id))
			.await
			.expect("tier readiness must not shut Raft participation down");
		if check_admission {
			let error = tier_authority_probe(&recovered)
				.await
				.expect_err("actual data admission must refuse while authority is pending");
			let refusal = crate::storage::manager::stale_location(&error)
				.expect("typed tier authority refusal");
			assert_eq!(
				refusal.reason,
				crate::storage::manager::StaleLocationReason::Fenced
			);
		} else {
			let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
			loop {
				let detail = recovered
					.readiness_reporter()
					.snapshot()
					.await
					.degraded_reason
					.unwrap_or_default();
				let lower = detail.to_ascii_lowercase();
				if detail.starts_with("RestoreValidationFailed:")
					|| (detail.starts_with("TierAuthority")
						&& (lower.contains("backoff") || lower.contains("retry")))
				{
					break;
				}
				assert!(
					tokio::time::Instant::now() < deadline,
					"unavailable authority must report its retry backoff: {detail}"
				);
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		}
		l2.put(
			&format!("sealed/slab_{sealed_id:016x}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
		wait_for_ready(&recovered).await;
		let response = tier_authority_probe(&recovered).await.unwrap().unwrap();
		let response: crate::schema::ReadResponseMeta = *bytemuck::from_bytes(
			&response.meta[..std::mem::size_of::<crate::schema::ReadResponseMeta>()],
		);
		assert_eq!(
			response.status,
			crate::schema::ReadStatus::KEY_NOT_FOUND,
			"a genuine miss is authoritative after recovery"
		);
		let location = recovered
			.slab
			.lookup(tenant_id, tiered_key)
			.unwrap()
			.unwrap();
		assert_eq!(recovered.slab.read(location).await.unwrap(), b"tiered-row");
		recovered.shutdown().await.unwrap();
	}

	#[tokio::test]
	async fn tier_authority_outage_recovers_readiness_and_keeps_raft_participating() {
		assert_tier_authority_outage_recovers(false, true).await;
	}

	#[tokio::test]
	async fn tier_authority_with_a_missing_mirror_opens_and_repairs_after_l2_returns() {
		assert_tier_authority_outage_recovers(true, true).await;
	}

	#[tokio::test]
	async fn tier_authority_restore_validation_retries_an_outage_on_the_same_node() {
		assert_tier_authority_outage_recovers(false, false).await;
	}

	#[tokio::test]
	async fn tenant_kek_rotation_is_raft_committed_and_old_l2_slabs_remain_readable() {
		let dir = tempdir("tenant-kek-rotation");
		let l2_dir = tempdir("tenant-kek-rotation-l2");
		let mut cfg = NodeBootstrapConfig::dev(14, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
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
				op_driver_interval: Duration::from_millis(500),
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
		assert_eq!(
			object_store.get(&old_pointer.l2_uri).await.unwrap(),
			old_blob,
			"old tenant-KEK object must be retained after re-wrap until retirement authority exists"
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
		let mut cfg = NodeBootstrapConfig::dev(15, dir.to_path_buf(), "127.0.0.1:0".into());
		cfg.slab_max_bytes = 1;
		cfg.l2 = Some(ObjectStoreConfig::LocalFs {
			root: l2_dir.to_path_buf(),
		});
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
			.unwrap()
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
		// The slab reads as tiered as soon as the pointer is installed; the row
		// refresh and the L1 unlink follow in the same apply
		// (SlabManager::apply_tier_slab_committed). Wait for the unlink itself.
		let local = dir.join(format!("slab_{:016x}.wnd", location.slab_id));
		let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
		while local.exists() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"tiered slab should be unlinked from local L1 after the background sweep"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}

	#[tokio::test]
	async fn bootstrapped_node_exposes_live_tracker_to_drift_monitor() {
		let dir = tempdir("drift");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			7,
			dir.to_path_buf(),
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
			dir.to_path_buf(),
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
			dir.to_path_buf(),
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
					remote: Some(crate::schema::NodeId {
						id: 0,
						_reserved: 0,
					}),
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

#[cfg(test)]
mod dormant_response_tests {
	use super::*;
	#[test]
	fn install_dormant_response_is_unsupported_and_other_mappings_are_preserved() {
		assert!(install_tenant_kek_response(WendelonResponse::Success, "ctx").is_ok());
		let e = install_tenant_kek_response(WendelonResponse::Root2Dormant, "ctx").unwrap_err();
		assert_eq!(e.kind(), ErrorKind::Unsupported);
		assert_eq!(e.to_string(), "ctx: Root2 carrier is dormant, not applied");
		let e = install_tenant_kek_response(WendelonResponse::Error("rejected".into()), "ctx")
			.unwrap_err();
		assert_eq!(e.kind(), ErrorKind::AlreadyExists);
		assert_eq!(e.to_string(), "ctx: rejected");
		let e = install_tenant_kek_response(WendelonResponse::LeaseDenied, "ctx").unwrap_err();
		assert_eq!(e.kind(), ErrorKind::Other);
		assert_eq!(
			e.to_string(),
			"ctx: unexpected LeaseDenied response from InstallTenantKekVersion"
		);
	}
	#[test]
	fn tenant_dormant_response_is_unsupported_and_other_mappings_are_preserved() {
		assert!(tenant_request_response(WendelonResponse::Success, "ctx").is_ok());
		let e = tenant_request_response(WendelonResponse::Root2Dormant, "ctx").unwrap_err();
		assert_eq!(e.kind(), ErrorKind::Unsupported);
		assert_eq!(e.to_string(), "ctx: Root2 carrier is dormant, not applied");
		let e =
			tenant_request_response(WendelonResponse::Error("rejected".into()), "ctx").unwrap_err();
		assert_eq!(e.kind(), ErrorKind::InvalidInput);
		assert_eq!(e.to_string(), "ctx: rejected");
		let e = tenant_request_response(WendelonResponse::LeaseDenied, "ctx").unwrap_err();
		assert_eq!(e.kind(), ErrorKind::Other);
		assert_eq!(e.to_string(), "ctx: unexpected LeaseDenied response");
	}
}

#[cfg(test)]
mod startup_ownership_tests;

#[cfg(test)]
#[path = "node/parent_decisions_cluster_tests.rs"]
mod parent_decisions_cluster_tests;
