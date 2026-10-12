use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
	ArrayRef, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
	TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_bytes::ByteBuf;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::{interval, timeout};
use tokio_rustls::TlsAcceptor;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

use crate::audit::{
	AuditOutcome, QueryFrontend, StatementClass, ToronAuditEmitter, ToronAuditEvent,
	ToronAuditMetrics, ToronAuditOperation,
};
use crate::catalog::{WendelonCatalog, WendelonDescriptorRegistry};
use crate::config::{
	OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC, ToronBootstrapConfig, ToronConfigError,
	ToronWendelonStorageConfig, load_descriptor_bootstrap_with_optimizer_facts,
	load_toron_bootstrap_config,
};
use crate::coordinator::DistributedQueryCoordinator;
use crate::distributed::budget::{BudgetDimension, ByteCount, TenantDescriptorSyncAccountant};
use crate::distributed::cancellation::{
	CancellationMetrics, CancellationOutcome, CancellationTrigger, DownstreamCancellationMetrics,
	QueryCancellation,
};
use crate::distributed::identity::{LeaseEpoch, QueryId};
use crate::distributed::partition_store::InMemoryPartitionStore;
use crate::distributed::worker_registry::{
	ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
	WorkerEndpoint, WorkerEntry, WorkerLease, WorkerResourceState, WorkerStatus,
};
use crate::error::{BindingError, CatalogError, FrontendError, QueryError};
use crate::exec::ExecutionContext;
use crate::ir::{LogicalPlan, SessionId};
use crate::network::connection::SqlConnectionLimiter;
use crate::network::observability::{
	ListenerName, MetricsRegistry, MetricsRegistryConfig, ObservabilityEndpoints,
	ObservabilityShutdownReceivers, ReadinessState, RejectReason,
};
use crate::network::tls::{ListenerPolicy, ListenerRole, TlsError, accepted_sql_tenant};
use crate::quic::{QuicEndpoint, QuicStream};
use crate::rune_engine::{CapabilityCategory, RuneEngine, RuneSandboxConfig};
use crate::server::transport::QuicTransport;
use crate::server::{ToronWorker, memory::MemoryManager};
use crate::sql_planner::SqlPlanner;
use crate::sql_wire::{
	Complete, MutationCommitKnowledge, MutationOutcomeResponse, MutationResolution,
	MutationStopReason, ResultBatch, RuneCapability, RuneFailure, RuneFailureKind,
	RuneLanguageSurfaceVersion, RuneRequestProtocolVersion, RuneSandboxContractVersion,
	RuneSandboxViolation as PublicRuneSandboxViolation, RuneSourceSpan, RuneVersionAxis,
	RuneVersionSkew, SqlError, SqlErrorCode, SqlFrame, SqlMessage, read_request_frame_bounded,
	write_frame as write_sql_frame,
};
use crate::storage::{
	HlcBound, StorageScan, TenantId, WendelonStorageAdmin, WendelonStorageDeleteRows,
	WendelonStorageScan, WendelonStorageWrite,
};

const OPTIMIZER_EVIDENCE_FLUSH_TIMEOUT: Duration = Duration::from_millis(50);
const OPTIMIZER_EVIDENCE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum ToronDaemonError {
	#[error(transparent)]
	Config(#[from] ToronConfigError),
	#[error(transparent)]
	Catalog(#[from] CatalogError),
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),
	#[error("worker listener task failed: {0}")]
	ListenerTask(JoinError),
	#[error("worker connection task failed: {0}")]
	ConnectionTask(JoinError),
	#[error(transparent)]
	Query(#[from] QueryError),
	#[error("Wendelon client error: {0}")]
	WendelonClient(#[from] wendelon_client::ClientError),
	#[error(transparent)]
	Tls(#[from] TlsError),
	#[error("toron daemon shutdown exceeded configured drain timeout")]
	DrainTimeoutExceeded,
	#[error("{message}")]
	Invalid { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToronDaemonHandle {
	pub worker_endpoint: SocketAddr,
	pub sql_endpoint: Option<SocketAddr>,
	pub observability_endpoints: Option<ObservabilityEndpoints>,
}

#[allow(dead_code)]
struct ToronDaemonServices {
	catalog: Arc<WendelonCatalog>,
	scanner: Arc<WendelonStorageScan>,
	writer: Arc<WendelonStorageWrite>,
	deleter: Arc<WendelonStorageDeleteRows>,
	admin: Arc<WendelonStorageAdmin>,
	coordinator: Arc<DistributedQueryCoordinator<StaticWorkerRegistry, QuicTransport>>,
}

#[allow(dead_code)]
pub struct ToronDaemon {
	handle: ToronDaemonHandle,
	services: ToronDaemonServices,
	shutdown_txs: Vec<oneshot::Sender<()>>,
	listener_tasks: Vec<JoinHandle<()>>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	drain_timeout: std::time::Duration,
	readiness: Arc<ReadinessState>,
	audit: ToronAuditEmitter,
	telemetry: Option<crate::telemetry::TelemetryHandle>,
}

impl ToronDaemon {
	pub async fn start(config: ToronBootstrapConfig) -> Result<Self, ToronDaemonError> {
		if config.observability.telemetry.is_none() {
			install_tracing(&config);
		}
		let client = Arc::new(
			build_wendelon_client(
				&config.storage.wendelon,
				*config.node.worker_id.get().as_bytes(),
			)
			.await?,
		);
		Self::start_with_client(config, client).await
	}

	#[doc(hidden)]
	pub async fn start_with_wendelon_client_for_harness(
		config: ToronBootstrapConfig,
		client: Arc<wendelon_client::Client>,
	) -> Result<Self, ToronDaemonError> {
		if config.observability.telemetry.is_none() {
			install_tracing(&config);
		}
		Self::start_with_client(config, client).await
	}

	async fn start_with_client(
		config: ToronBootstrapConfig,
		client: Arc<wendelon_client::Client>,
	) -> Result<Self, ToronDaemonError> {
		let tls_policy = config
			.tls
			.as_ref()
			.map(|tls| ListenerPolicy::load(tls, &config.node.worker_id))
			.transpose()?
			.map(Arc::new);
		if config.observability.listeners.is_some() && tls_policy.is_none() {
			return Err(ToronDaemonError::Invalid {
				message: OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC.to_string(),
			});
		}
		let descriptor_records = match &config.catalog.descriptor_bootstrap_path {
			Some(path) => load_descriptor_bootstrap_with_optimizer_facts(path)?,
			None => Vec::new(),
		};
		let descriptors_loaded = descriptor_records.len() as u64;
		let registry = Arc::new(
			WendelonDescriptorRegistry::from_tenant_descriptors_with_facts(descriptor_records)?,
		);
		tracing::info!("descriptor_registry_constructed");

		let tenant_id = config.node.tenant_id;
		let descriptor_sync_accountant =
			TenantDescriptorSyncAccountant::new(config.tenant_budgets.clone());

		let catalog = Arc::new(
			WendelonCatalog::with_client_and_registry_and_descriptor_sync_accountant(
				Arc::clone(&client),
				tenant_id,
				Arc::clone(&registry),
				descriptor_sync_accountant,
			),
		);
		let scanner = Arc::new(WendelonStorageScan::with_descriptor_registry(
			Arc::clone(&client),
			Arc::clone(&registry),
		));
		let writer = Arc::new(WendelonStorageWrite::with_descriptor_registry(
			Arc::clone(&client),
			Arc::clone(&registry),
		));
		let deleter = Arc::new(WendelonStorageDeleteRows::with_descriptor_registry(
			Arc::clone(&client),
			Arc::clone(&registry),
		));
		let admin = Arc::new(WendelonStorageAdmin::new(
			Arc::clone(&client),
			Arc::clone(&registry),
		));

		let loopback_task_listener = config.worker.task_listener_bind.ip().is_loopback();
		let (listener, quic_worker_endpoint) = if loopback_task_listener {
			#[cfg(not(test))]
			let (listener, endpoint) = bind_loopback_worker_pair(&config, WORKER_PAIR_ATTEMPTS).await?;
			#[cfg(test)]
			let (listener, endpoint) =
				bind_loopback_worker_pair(&config, WORKER_PAIR_ATTEMPTS, &mut |_| None).await?;
			(Some(listener), endpoint)
		} else {
			let worker_bind_endpoint = config.worker.task_listener_bind;
			let endpoint = match (&tls_policy, worker_bind_endpoint.ip().is_loopback()) {
				(Some(policy), false) => QuicEndpoint::bind_authenticated(
					worker_bind_endpoint,
					policy.server_config_for(ListenerRole::WorkerTask),
				)?,
				_ => QuicEndpoint::bind_loopback(worker_bind_endpoint)?,
			};
			(None, endpoint)
		};
		let worker_endpoint = quic_worker_endpoint.local_addr()?;
		let advertised_endpoint = config
			.worker
			.advertised_endpoint
			.clone()
			.unwrap_or_else(|| WorkerEndpoint::new(worker_endpoint.to_string()));
		let memory_budget_bytes =
			usize::try_from(config.worker.memory_budget_bytes).map_err(|_| {
				ToronDaemonError::Invalid {
					message: "memory_budget_bytes exceeds platform usize".to_string(),
				}
			})?;
		let memory = Arc::new(MemoryManager::new(32, memory_budget_bytes));
		let worker_storage: Arc<dyn StorageScan> = scanner.clone();
		let downstream_cancellation_metrics = Arc::new(DownstreamCancellationMetrics::default());
		let partition_store = Arc::new(
			InMemoryPartitionStore::managed(
				config.worker.query_budget(),
				crate::distributed::partition_store::DEFAULT_WORKER_LOST_AFTER,
			)
			.map_err(|error| ToronDaemonError::Invalid {
				message: error.to_string(),
			})?,
		);
		// ADR 624: one transport, built once from the daemon's TLS policy, dials both the
		// coordinator's task dispatch and the worker's sealed shuffle pulls.
		let transport = Arc::new(match &tls_policy {
			Some(policy) => {
				QuicTransport::authenticated(policy.client_config(), policy.cluster_domain())?
			}
			None => QuicTransport::new()?,
		});
		let worker = ToronWorker::with_partition_store_and_endpoint(
			worker_storage,
			Arc::clone(&memory),
			partition_store.clone(),
			config.node.worker_id,
			advertised_endpoint.clone(),
		)
		.with_downstream_cancellation_metrics(Arc::clone(&downstream_cancellation_metrics))
		.with_peer_transport(transport.clone());

		let registry_workers = coordinator_workers(
			&config,
			worker_endpoint,
			advertised_endpoint,
			config.worker.memory_budget_bytes,
		);
		let worker_registry = Arc::new(StaticWorkerRegistry::new(registry_workers));
		let coordinator_catalog: Arc<dyn crate::catalog::Catalog> = catalog.clone();
		let coordinator_writer: Arc<dyn crate::storage::StorageWrite> = writer.clone();
		let coordinator_deleter: Arc<dyn crate::storage::StorageDeleteRows> = deleter.clone();
		let coordinator_admin: Arc<dyn crate::storage::StorageAdmin> = admin.clone();
		let cancellation_metrics = Arc::new(CancellationMetrics::default());
		let runtime_evidence = crate::binder::OptimizerRuntimeEvidence::with_wendelon(
			crate::binder::DurableEvidenceBinding::new(
				Arc::clone(&client),
				wendelon::schema::TenantId::new(tenant_id.0),
				OPTIMIZER_EVIDENCE_FLUSH_TIMEOUT,
			),
		);
		let _ = runtime_evidence.refresh_durable_cache().await;
		let planner_config = crate::binder::PlannerConfig {
			runtime_evidence: Arc::clone(&runtime_evidence),
			..crate::binder::PlannerConfig::default()
		};
		let coordinator = Arc::new(
			DistributedQueryCoordinator::with_writer_deleter_and_admin(
				coordinator_catalog,
				worker_registry,
				Arc::clone(&memory),
				transport,
				planner_config,
				// ADR 463: config-owned; absent knobs mean the explicit test
				// constant. A derivation formula waits for pricing evidence.
				config.worker.query_budget(),
				coordinator_writer,
				coordinator_deleter,
				coordinator_admin,
			)
			.with_tenant_budget_policy(config.tenant_budgets.clone())
			.with_admission_policy(config.admission)
			.with_cancellation_metrics(Arc::clone(&cancellation_metrics))
			.with_downstream_cancellation_metrics(Arc::clone(&downstream_cancellation_metrics)),
		);

		let connection_tasks = Arc::new(Mutex::new(JoinSet::new()));
		let mut shutdown_txs = Vec::new();
		let mut listener_tasks = Vec::new();
		let (evidence_refresh_shutdown_tx, mut evidence_refresh_shutdown_rx) = oneshot::channel();
		shutdown_txs.push(evidence_refresh_shutdown_tx);
		let refresh_runtime_evidence = Arc::clone(&runtime_evidence);
		listener_tasks.push(tokio::spawn(async move {
			let mut refresh_interval = interval(OPTIMIZER_EVIDENCE_REFRESH_INTERVAL);
			refresh_interval.tick().await;
			loop {
				tokio::select! {
					_ = &mut evidence_refresh_shutdown_rx => break,
					_ = refresh_interval.tick() => {
						refresh_runtime_evidence.flush_decision_deltas().await;
						refresh_runtime_evidence.flush_probe_linearity_deltas().await;
						refresh_runtime_evidence.flush_window_deltas().await;
						let _ = refresh_runtime_evidence.refresh_durable_cache().await;
					}
				}
			}
		}));
		let sql_connection_limiter = config.sql.as_ref().map(|sql| {
			SqlConnectionLimiter::new(
				sql.max_concurrent_connections,
				config.tenant_budgets.clone(),
			)
		});
		let audit_metrics = config
			.audit
			.is_enabled()
			.then(|| Arc::new(ToronAuditMetrics::default()));
		let metrics = Arc::new(MetricsRegistry::new(MetricsRegistryConfig {
			descriptors_loaded,
			static_workers_configured: config.coordinator.static_workers.len() as u64,
			tenant_budgets: config.tenant_budgets.clone(),
			descriptor_sync_metrics: catalog.descriptor_sync_metrics(),
			descriptor_refresh_metrics: catalog.descriptor_refresh_metrics(),
			cancellation_metrics,
			downstream_cancellation_metrics,
			sql_connection_limiter: sql_connection_limiter.clone(),
			audit_metrics: audit_metrics.clone(),
			runtime_evidence: Some(Arc::clone(&runtime_evidence)),
			tenant_admission: coordinator.tenant_admission_controller(),
			admission_rejections: coordinator.admission_rejection_counters(),
		}));
		metrics.attach_shuffle_lifecycle(partition_store);
		let telemetry = if config.observability.telemetry.is_some() {
			Some(crate::telemetry::install_global(
				&config,
				Arc::clone(&metrics),
			)?)
		} else {
			None
		};
		let readiness = Arc::new(ReadinessState::new(
			true,
			true,
			config.observability.listeners.is_some(),
			descriptors_loaded,
			config.coordinator.static_workers.len() as u64,
		));
		let audit =
			ToronAuditEmitter::from_config(&config.audit, audit_metrics, Arc::clone(&readiness))
				.await;
		audit.record(ToronAuditEvent::lifecycle_startup());
		let worker = Arc::new(worker.with_audit_emitter(audit.clone()));

		if let Some(listener) = listener {
			let (tcp_shutdown_tx, tcp_shutdown_rx) = oneshot::channel();
			shutdown_txs.push(tcp_shutdown_tx);
			listener_tasks.push(tokio::spawn(run_accept_loop(
				Arc::clone(&worker),
				listener,
				tcp_shutdown_rx,
				Arc::clone(&connection_tasks),
				Arc::clone(&metrics),
			)));
		}

		let (quic_worker_shutdown_tx, quic_worker_shutdown_rx) = oneshot::channel();
		shutdown_txs.push(quic_worker_shutdown_tx);
		listener_tasks.push(tokio::spawn(run_quic_worker_accept_loop(
			Arc::clone(&worker),
			quic_worker_endpoint.clone(),
			quic_worker_shutdown_rx,
			Arc::clone(&connection_tasks),
			Arc::clone(&metrics),
		)));

		let sql_endpoint = if let Some(sql_config) = config.sql.clone() {
			let rune_sandbox = RuneSandboxConfig::v1_default();
			let rune_engine = Arc::new(
				RuneEngine::with_config(rune_sandbox)
					.map_err(QueryError::Frontend)
					.map_err(ToronDaemonError::Query)?,
			);
			let authenticated_sql =
				tls_policy.is_some() && !sql_config.listener_bind.ip().is_loopback();
			let sql_tenant_source = match (&tls_policy, authenticated_sql) {
				(Some(policy), true) => SqlTenantSource::AuthenticatedPeer {
					cluster_domain: policy.cluster_domain().to_string(),
				},
				_ => SqlTenantSource::LoopbackConfig(config.node.tenant_id),
			};
			let sql_quic_endpoint = match (&tls_policy, sql_config.listener_bind.ip().is_loopback())
			{
				(Some(policy), false) => QuicEndpoint::bind_authenticated(
					sql_config.listener_bind,
					policy.server_config_for(ListenerRole::Sql),
				)?,
				_ => QuicEndpoint::bind_loopback(sql_config.listener_bind)?,
			};
			let sql_endpoint = sql_quic_endpoint.local_addr()?;
			let (sql_shutdown_tx, sql_shutdown_rx) = oneshot::channel();
			shutdown_txs.push(sql_shutdown_tx);
			listener_tasks.push(tokio::spawn(run_sql_accept_loop(
				Arc::clone(&coordinator),
				sql_quic_endpoint,
				sql_shutdown_rx,
				Arc::clone(&connection_tasks),
				Arc::clone(&metrics),
				audit.clone(),
				SqlAcceptOptions {
					tenant_source: sql_tenant_source,
					connection_limiter: sql_connection_limiter
						.clone()
						.expect("SQL limiter exists when SQL config exists"),
					statement_timeout: sql_config.statement_timeout,
					max_request_frame_bytes: sql_config.max_request_frame_bytes,
					max_statement_bytes: sql_config.max_statement_bytes,
					rune_engine,
					rune_sandbox,
				},
			)));
			Some(sql_endpoint)
		} else {
			None
		};
		let observability_endpoints =
			if let Some(observability_listeners) = config.observability.listeners.clone() {
				let policy = tls_policy
					.as_ref()
					.ok_or_else(|| ToronDaemonError::Invalid {
						message: OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC.to_string(),
					})?;
				let (health_tx, health_rx) = oneshot::channel();
				let (ready_tx, ready_rx) = oneshot::channel();
				let (metrics_tx, metrics_rx) = oneshot::channel();
				let handles = crate::network::observability::spawn(
					observability_listeners,
					TlsAcceptor::from(policy.rustls_server_config_for(ListenerRole::Observability)),
					Arc::clone(&readiness),
					Arc::clone(&metrics),
					audit.clone(),
					policy.cluster_domain().to_string(),
					ObservabilityShutdownReceivers {
						health: health_rx,
						ready: ready_rx,
						metrics: metrics_rx,
					},
				)
				.await?;
				let endpoints = ObservabilityEndpoints {
					health: handles.health.local_addr,
					ready: handles.ready.local_addr,
					metrics: handles.metrics.local_addr,
				};
				shutdown_txs.extend([health_tx, ready_tx, metrics_tx]);
				listener_tasks.extend([
					handles.health.task,
					handles.ready.task,
					handles.metrics.task,
				]);
				Some(endpoints)
			} else {
				None
			};

		Ok(Self {
			handle: ToronDaemonHandle {
				worker_endpoint,
				sql_endpoint,
				observability_endpoints,
			},
			services: ToronDaemonServices {
				catalog,
				scanner,
				writer,
				deleter,
				admin,
				coordinator,
			},
			shutdown_txs,
			listener_tasks,
			connection_tasks,
			drain_timeout: config.node.drain_timeout,
			readiness,
			audit,
			telemetry,
		})
	}

	pub fn handle(&self) -> &ToronDaemonHandle {
		&self.handle
	}

	pub async fn shutdown(mut self) -> Result<(), ToronDaemonError> {
		self.audit.record(ToronAuditEvent::lifecycle_shutdown());
		self.readiness
			.set_degraded_reason(Some("draining".to_string()))
			.await;
		for tx in self.shutdown_txs.drain(..) {
			let _ = tx.send(());
		}
		for task in self.listener_tasks {
			task.await.map_err(ToronDaemonError::ListenerTask)?;
		}

		let drain = async {
			let mut tasks = self.connection_tasks.lock().await;
			while let Some(result) = tasks.join_next().await {
				result.map_err(ToronDaemonError::ConnectionTask)?;
			}
			Ok(())
		};

		let drain_result = match timeout(self.drain_timeout, drain).await {
			Ok(result) => result,
			Err(_) => {
				let mut tasks = self.connection_tasks.lock().await;
				tasks.abort_all();
				while let Some(result) = tasks.join_next().await {
					let _ = result;
				}
				Err(ToronDaemonError::DrainTimeoutExceeded)
			}
		};
		if drain_result.is_ok()
			&& let Some(telemetry) = self.telemetry.take()
		{
			let export_timeout = telemetry.export_timeout();
			match timeout(export_timeout, telemetry.shutdown()).await {
				Ok(Ok(())) => {}
				Ok(Err(error)) => eprintln!("toron: telemetry shutdown failed: {error}"),
				Err(_) => eprintln!("toron: telemetry shutdown timed out"),
			}
		}
		drain_result
	}
}

pub fn run_start(config_path: PathBuf) -> ExitCode {
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Toron runtime: {error}");
			return ExitCode::from(2);
		}
	};
	runtime.block_on(run_start_async(config_path))
}

async fn run_start_async(config_path: PathBuf) -> ExitCode {
	let config = match load_toron_bootstrap_config(&config_path) {
		Ok(config) => config,
		Err(error) => {
			eprintln!("failed to load {}: {error}", config_path.display());
			return ExitCode::from(2);
		}
	};
	let daemon = match ToronDaemon::start(config).await {
		Ok(daemon) => daemon,
		Err(error) => {
			eprintln!("toron daemon bootstrap failed: {error}");
			return ExitCode::from(1);
		}
	};

	println!("READY {}", daemon.handle().worker_endpoint);
	if let Some(sql_endpoint) = daemon.handle().sql_endpoint {
		println!("SQL_READY {sql_endpoint}");
	}
	if let Some(observability) = daemon.handle().observability_endpoints {
		println!(
			"OBSERVABILITY_READY health={} ready={} metrics={}",
			observability.health, observability.ready, observability.metrics
		);
	}

	let _ = tokio::signal::ctrl_c().await;
	eprintln!("toron: draining");
	match daemon.shutdown().await {
		Ok(()) => {
			eprintln!("toron: shutdown complete");
			ExitCode::SUCCESS
		}
		Err(error) => {
			eprintln!("toron: shutdown failed: {error}");
			ExitCode::from(1)
		}
	}
}

async fn run_accept_loop(
	worker: Arc<ToronWorker>,
	listener: TcpListener,
	mut shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
) {
	let mut lifecycle_tick = tokio::time::interval(std::time::Duration::from_secs(1));
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => break,
			_ = lifecycle_tick.tick() => worker.reap_expired_shuffle_namespaces(),
			accepted = listener.accept() => match accepted {
				Ok((stream, _peer)) => {
					metrics.increment_accept(ListenerName::Worker);
					let worker = Arc::clone(&worker);
					connection_tasks.lock().await.spawn(async move {
						if let Err(error) = handle_worker_connection(worker, stream).await {
							tracing::warn!(error = %error, "toron worker connection error");
						}
					});
				}
				Err(error) => {
					tracing::error!(error = %error, "toron listener accept failed");
					break;
				}
			},
		}
	}
}

async fn handle_worker_connection(
	worker: Arc<ToronWorker>,
	stream: TcpStream,
) -> Result<(), QueryError> {
	worker.handle_connection(stream).await
}

async fn run_quic_worker_accept_loop(
	worker: Arc<ToronWorker>,
	endpoint: QuicEndpoint,
	shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
) {
	let reaper = Arc::clone(&worker);
	run_quic_worker_accept_loop_with_maintenance_interval(
		worker,
		endpoint,
		shutdown_rx,
		connection_tasks,
		metrics,
		Duration::from_secs(1),
		move || reaper.reap_expired_shuffle_namespaces(),
		#[cfg(test)]
		None,
	)
	.await;
}

#[cfg(test)]
#[derive(Clone)]
struct QuicWorkerHandshakeGate {
	entered: Arc<tokio::sync::Notify>,
	release: Arc<tokio::sync::Notify>,
}

async fn run_quic_worker_accept_loop_with_maintenance_interval<Maintenance>(
	worker: Arc<ToronWorker>,
	endpoint: QuicEndpoint,
	mut shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
	maintenance_interval: Duration,
	maintenance: Maintenance,
	#[cfg(test)] handshake_gate: Option<QuicWorkerHandshakeGate>,
) where
	Maintenance: Fn(),
{
	let mut maintenance_tick = tokio::time::interval(maintenance_interval);
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => {
				endpoint.close();
				break;
			}
			_ = maintenance_tick.tick() => maintenance(),
			accepted = endpoint.accept_sql_incoming() => match accepted {
				Ok(incoming) => {
					// `accept_connection()` combines dequeuing an incoming connection with
					// completing its handshake. Racing that combined future against the
					// maintenance tick can drop an already-dequeued `Incoming` and reject a
					// healthy peer. Once dequeued, the handshake is raced only against
					// shutdown; maintenance resumes after admission is decided.
					let admitted = async {
						#[cfg(test)]
						if let Some(gate) = handshake_gate.as_ref() {
							gate.entered.notify_one();
							gate.release.notified().await;
						}
						QuicEndpoint::accept_incoming_connection(incoming).await
					};
					let connection = tokio::select! {
						_ = &mut shutdown_rx => {
							endpoint.close();
							break;
						}
						accepted = admitted => match accepted {
							Ok(connection) => connection,
							Err(error) => {
								tracing::warn!(error = %error, "QUIC worker handshake failed");
								continue;
							}
						},
					};
					metrics.increment_accept(ListenerName::Worker);
					let worker = Arc::clone(&worker);
					connection_tasks.lock().await.spawn(async move {
						match QuicEndpoint::accept_stream_on(&connection).await {
							Ok(stream) => {
								if let Err(error) = handle_quic_worker_connection(worker, stream).await {
									tracing::warn!(error = %error, "QUIC worker connection error");
								}
								let _ = timeout(std::time::Duration::from_secs(1), connection.closed()).await;
							}
							Err(error) => {
								tracing::warn!(error = %error, "QUIC worker stream accept error");
							}
						}
					});
				}
				Err(error) => {
					tracing::error!(error = %error, "QUIC worker listener accept failed");
					break;
				}
			},
		}
	}
}

async fn handle_quic_worker_connection(
	worker: Arc<ToronWorker>,
	stream: QuicStream,
) -> Result<(), QueryError> {
	worker.handle_connection(stream).await
}

#[derive(Clone)]
enum SqlTenantSource {
	LoopbackConfig(TenantId),
	AuthenticatedPeer { cluster_domain: String },
}

impl SqlTenantSource {
	fn resolve(&self, connection: &quinn::Connection) -> Result<TenantId, TlsError> {
		match self {
			Self::LoopbackConfig(tenant_id) => Ok(*tenant_id),
			Self::AuthenticatedPeer { cluster_domain } => {
				accepted_sql_tenant(connection, cluster_domain)
			}
		}
	}
}

struct SqlAcceptOptions {
	tenant_source: SqlTenantSource,
	connection_limiter: SqlConnectionLimiter,
	statement_timeout: std::time::Duration,
	max_request_frame_bytes: NonZeroU32,
	max_statement_bytes: NonZeroU32,
	rune_engine: Arc<RuneEngine>,
	rune_sandbox: RuneSandboxConfig,
}

#[derive(Clone)]
struct SqlConnectionOptions {
	tenant_id: TenantId,
	statement_timeout: std::time::Duration,
	max_request_frame_bytes: NonZeroU32,
	max_statement_bytes: NonZeroU32,
	rune_engine: Arc<RuneEngine>,
	rune_sandbox: RuneSandboxConfig,
}

async fn run_sql_accept_loop(
	coordinator: Arc<DistributedQueryCoordinator<StaticWorkerRegistry, QuicTransport>>,
	endpoint: QuicEndpoint,
	mut shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
	audit: ToronAuditEmitter,
	options: SqlAcceptOptions,
) {
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => {
				endpoint.close();
				break;
			}
			accepted = endpoint.accept_sql_incoming() => match accepted {
				Ok(incoming) => {
					let tier1_permit = match options.connection_limiter.try_acquire_tier1() {
						Ok(permit) => permit,
						Err(_) => {
							metrics.increment_reject(ListenerName::Sql, RejectReason::ConnectionLimit);
							audit.record(ToronAuditEvent::edge_denial(
								ToronAuditOperation::SqlConnectionLimitDenied,
								None,
								"sql connection limit denied",
							));
							incoming.refuse();
							continue;
						}
					};
					let connection = tokio::select! {
						_ = &mut shutdown_rx => {
							endpoint.close();
							break;
						}
						accepted = QuicEndpoint::accept_incoming_connection(incoming) => match accepted {
							Ok(connection) => connection,
							Err(error) => {
								tracing::warn!(error = %error, "sql listener handshake failed");
								continue;
							}
						},
					};
					metrics.increment_accept(ListenerName::Sql);
					let tenant_id = match options.tenant_source.resolve(&connection) {
						Ok(tenant_id) => tenant_id,
						Err(error) => {
							metrics.increment_reject(
								ListenerName::Sql,
								RejectReason::SanShapeRejected,
							);
							audit.record(ToronAuditEvent::edge_denial(
								ToronAuditOperation::SqlTenantIdentityDenied,
								None,
								"sql tenant identity denied",
							));
							tracing::warn!(error = %error, "sql peer tenant extraction failed");
							connection.close(0_u32.into(), b"sql tenant rejected");
							continue;
						}
					};
					let tier2_permit = match options.connection_limiter.try_acquire_tier2(tenant_id) {
						Ok(permit) => permit,
						Err(_) => {
							metrics.increment_reject(ListenerName::Sql, RejectReason::ConnectionLimit);
							audit.record(ToronAuditEvent::edge_denial(
								ToronAuditOperation::SqlConnectionLimitDenied,
								Some(tenant_id),
								"sql connection limit denied",
							));
							connection.close(0_u32.into(), b"tenant sql connection limit exhausted");
							continue;
						}
					};
					let coordinator = Arc::clone(&coordinator);
					let connection_options = SqlConnectionOptions {
						tenant_id,
						statement_timeout: options.statement_timeout,
						max_request_frame_bytes: options.max_request_frame_bytes,
						max_statement_bytes: options.max_statement_bytes,
						rune_engine: Arc::clone(&options.rune_engine),
						rune_sandbox: options.rune_sandbox,
					};
					let metrics = Arc::clone(&metrics);
					let audit = audit.clone();
					connection_tasks.lock().await.spawn(async move {
						let _connection_permits = (tier1_permit, tier2_permit);
						match QuicEndpoint::accept_stream_on(&connection).await {
							Ok(stream) => {
								if let Err(error) = handle_sql_connection(
									coordinator,
									stream,
									connection.clone(),
									connection_options,
									metrics,
									audit,
								).await {
									tracing::warn!(error = %error, "sql connection error");
								}
								let _ = timeout(std::time::Duration::from_secs(1), connection.closed()).await;
							}
							Err(error) => {
								tracing::warn!(error = %error, "sql stream accept error");
							}
						}
					});
				}
				Err(error) => {
					tracing::error!(error = %error, "sql listener accept failed");
					break;
				}
			},
		}
	}
}

async fn handle_sql_connection(
	coordinator: Arc<DistributedQueryCoordinator<StaticWorkerRegistry, QuicTransport>>,
	mut stream: QuicStream,
	connection: quinn::Connection,
	options: SqlConnectionOptions,
	metrics: Arc<MetricsRegistry>,
	audit: ToronAuditEmitter,
) -> Result<(), QueryError> {
	let tenant_id = options.tenant_id;
	let frame =
		match read_request_frame_bounded(&mut stream, options.max_request_frame_bytes).await? {
			Ok(frame) => frame,
			Err(error) => {
				metrics.increment_reject(ListenerName::Sql, RejectReason::RequestFrameTooLarge);
				record_sql_audit_required_without_response(
					&audit,
					ToronAuditEvent::edge_denial(
						ToronAuditOperation::SqlRequestFrameTooLarge,
						Some(tenant_id),
						"sql request frame too large",
					),
				)
				.await;
				tracing::warn!(
					declared_len = error.declared_len,
					max_len = error.max_len.get(),
					"sql request frame too large"
				);
				let _ = stream.finish().await;
				return Ok(());
			}
		};
	if frame.protocol_version != crate::sql_wire::SqlProtocolVersion::Current {
		if !record_sql_audit_required_or_error(
			&audit,
			ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlProtocolMismatch,
				Some(tenant_id),
				"sql protocol mismatch",
			),
			&mut stream,
			frame.query_id,
		)
		.await?
		{
			return Ok(());
		}
		return write_sql_error(
			&mut stream,
			frame.query_id,
			SqlErrorCode::Internal,
			"unsupported SQL protocol version",
		)
		.await;
	}

	match frame.message {
		SqlMessage::Health(_) => {
			let response = SqlFrame::response(
				frame.query_id,
				SqlMessage::Healthy(crate::sql_wire::HealthyResponse),
			);
			write_sql_frame(&mut stream, &response).await?;
			stream.finish().await?;
		}
		SqlMessage::ResolveMutation(request) => {
			if let Some(entry) = coordinator
				.resolve_ctas_outcome(tenant_id, request.operation_query_id)
				.await
				.map_err(|error| std::io::Error::other(error.to_string()))?
			{
				let (phase, row_count) = match entry.phase {
					wendelon_client::CtasSagaPhase::Provisioned => {
						(crate::sql_wire::CtasCompositePhase::Provisioned, None)
					}
					wendelon_client::CtasSagaPhase::RowDispatched => (
						crate::sql_wire::CtasCompositePhase::RowDispatched,
						entry.row_count,
					),
					wendelon_client::CtasSagaPhase::RowsCommitted { row_count } => (
						crate::sql_wire::CtasCompositePhase::RowsCommitted,
						Some(row_count),
					),
					wendelon_client::CtasSagaPhase::Published => (
						crate::sql_wire::CtasCompositePhase::Published,
						entry.row_count,
					),
					wendelon_client::CtasSagaPhase::RetiringOld => (
						crate::sql_wire::CtasCompositePhase::RetiringOld,
						entry.row_count,
					),
					wendelon_client::CtasSagaPhase::Complete => (
						crate::sql_wire::CtasCompositePhase::Complete,
						entry.row_count,
					),
					wendelon_client::CtasSagaPhase::Conflict => (
						crate::sql_wire::CtasCompositePhase::Conflict,
						entry.row_count,
					),
					wendelon_client::CtasSagaPhase::Reclaimed => {
						(crate::sql_wire::CtasCompositePhase::Reclaimed, None)
					}
				};
				let prior_binding = match entry.expected_binding {
					wendelon_client::CtasExpectedBinding::Absent => None,
					wendelon_client::CtasExpectedBinding::Bound {
						table_object_id,
						schema_generation,
					} => Some(crate::sql_wire::CtasPriorBinding {
						table_object_id: *table_object_id.as_bytes(),
						schema_generation: schema_generation.get(),
					}),
				};
				let terminal_cause = entry.terminal_cause.map(|cause| match cause {
					wendelon_client::CtasTerminalCause::PublicationConflict => {
						crate::sql_wire::CtasTerminalCause::PublicationConflict
					}
					wendelon_client::CtasTerminalCause::RowKnownNotCommitted => {
						crate::sql_wire::CtasTerminalCause::RowKnownNotCommitted
					}
					wendelon_client::CtasTerminalCause::DeadlineBeforeRowDispatch => {
						crate::sql_wire::CtasTerminalCause::DeadlineBeforeRowDispatch
					}
					wendelon_client::CtasTerminalCause::OwnerAbandonedBeforeRowDispatch => {
						crate::sql_wire::CtasTerminalCause::OwnerAbandonedBeforeRowDispatch
					}
				});
				let committed = crate::sql_wire::MutationCommitKnowledge::Committed;
				let known_not_committed =
					crate::sql_wire::MutationCommitKnowledge::KnownNotCommitted;
				let indeterminate = crate::sql_wire::MutationCommitKnowledge::Indeterminate;
				let unspecified = crate::sql_wire::MutationCommitKnowledge::Unspecified;
				let row_commit_knowledge = match entry.phase {
					wendelon_client::CtasSagaPhase::Provisioned => known_not_committed,
					wendelon_client::CtasSagaPhase::RowDispatched => indeterminate,
					wendelon_client::CtasSagaPhase::RowsCommitted { .. }
					| wendelon_client::CtasSagaPhase::Published
					| wendelon_client::CtasSagaPhase::RetiringOld
					| wendelon_client::CtasSagaPhase::Complete
					| wendelon_client::CtasSagaPhase::Conflict => committed,
					wendelon_client::CtasSagaPhase::Reclaimed => known_not_committed,
				};
				let publication_commit_knowledge = match entry.phase {
					wendelon_client::CtasSagaPhase::Published
					| wendelon_client::CtasSagaPhase::RetiringOld
					| wendelon_client::CtasSagaPhase::Complete => committed,
					wendelon_client::CtasSagaPhase::Conflict
					| wendelon_client::CtasSagaPhase::Reclaimed => known_not_committed,
					_ => unspecified,
				};
				let retirement_commit_knowledge = match entry.phase {
					wendelon_client::CtasSagaPhase::Complete => committed,
					wendelon_client::CtasSagaPhase::Published
					| wendelon_client::CtasSagaPhase::RetiringOld => indeterminate,
					wendelon_client::CtasSagaPhase::Conflict
					| wendelon_client::CtasSagaPhase::Reclaimed => known_not_committed,
					_ => unspecified,
				};
				let catalog_cause = match entry.terminal_cause {
					Some(wendelon_client::CtasTerminalCause::DeadlineBeforeRowDispatch) => {
						Some(crate::sql_wire::CtasCatalogCause::DeadlineBeforeRowDispatch)
					}
					Some(wendelon_client::CtasTerminalCause::OwnerAbandonedBeforeRowDispatch) => {
						Some(crate::sql_wire::CtasCatalogCause::OwnerAbandonedBeforeRowDispatch)
					}
					_ => None,
				};
				let row_cause = matches!(
					entry.terminal_cause,
					Some(wendelon_client::CtasTerminalCause::RowKnownNotCommitted)
				)
				.then_some(crate::sql_wire::CtasRowCause::KnownNotCommitted);
				let publication_cause = matches!(
					entry.terminal_cause,
					Some(wendelon_client::CtasTerminalCause::PublicationConflict)
				)
				.then_some(crate::sql_wire::CtasPublicationCause::BindingConflict);
				let response = SqlFrame::response(
					frame.query_id,
					SqlMessage::CtasOutcome(crate::sql_wire::CtasOutcomeResponse {
						operation_query_id: request.operation_query_id,
						phase,
						provisional_table_object_id: *entry.descriptor.table_object_id.as_bytes(),
						prior_binding,
						row_count,
						terminal_cause,
						catalog_commit_knowledge: committed,
						catalog_cause,
						row_commit_knowledge,
						row_cause,
						publication_commit_knowledge,
						publication_cause,
						retirement_commit_knowledge,
						retirement_cause: (entry.retry_debt > 0
							&& entry.phase == wendelon_client::CtasSagaPhase::RetiringOld)
							.then_some(crate::sql_wire::CtasRetirementCause::RetryDeferred),
						owner_epoch: entry.owner_epoch,
						retry_debt: entry.retry_debt,
					}),
				);
				write_sql_frame(&mut stream, &response).await?;
				stream.finish().await?;
				return Ok(());
			}
			let outcome = match coordinator
				.resolve_mutation_outcome(tenant_id, request.operation_query_id)
				.await
			{
				Ok(outcome) => outcome,
				Err(error) => {
					return write_sql_error(
						&mut stream,
						frame.query_id,
						SqlErrorCode::StorageError,
						error.to_string(),
					)
					.await;
				}
			};
			let response = SqlFrame::response(
				frame.query_id,
				SqlMessage::MutationOutcome(MutationOutcomeResponse {
					operation_query_id: request.operation_query_id,
					resolution: match outcome.resolution {
						crate::storage::MutationResolution::Present => MutationResolution::Present,
						crate::storage::MutationResolution::Expired => MutationResolution::Expired,
						crate::storage::MutationResolution::Unknown => MutationResolution::Unknown,
					},
					write_status: outcome.write_status,
					stop_reason: public_mutation_stop_reason(outcome.stop_reason),
					commit_knowledge: public_mutation_commit_knowledge(outcome.commit_knowledge),
					delete_counts: outcome.delete_counts,
				}),
			);
			write_sql_frame(&mut stream, &response).await?;
			stream.finish().await?;
		}
		SqlMessage::Execute(request) => {
			let query_started = std::time::Instant::now();
			let session_id = new_sql_session_id();
			let query_trace = crate::telemetry::query_trace_context(frame.query_id);
			let query_span = crate::telemetry::query_span(query_trace);
			crate::telemetry::record_query_frontend(&query_span, "sql");
			if request.statement.len()
				> usize::try_from(options.max_statement_bytes.get()).expect("u32 fits usize")
			{
				metrics.increment_reject(ListenerName::Sql, RejectReason::StatementTooLarge);
				crate::telemetry::record_query_statement_class(
					&query_span,
					StatementClass::Unknown,
				);
				crate::telemetry::record_query_outcome(&query_span, "rejected");
				if !record_sql_audit_required_or_error(
					&audit,
					ToronAuditEvent::edge_denial(
						ToronAuditOperation::SqlStatementTooLarge,
						Some(tenant_id),
						"sql statement too large",
					),
					&mut stream,
					frame.query_id,
				)
				.await?
				{
					return Ok(());
				}
				return write_sql_error(
					&mut stream,
					frame.query_id,
					SqlErrorCode::ResourceExhausted,
					"SQL statement exceeds configured byte limit",
				)
				.await;
			}
			let timeout_duration = request
				.statement_timeout_seconds
				.map(|seconds| std::time::Duration::from_secs(u64::from(seconds)))
				.unwrap_or(options.statement_timeout);
			let planner = SqlPlanner::new();
			let planning_span = crate::telemetry::planning_span(query_trace);
			let plan = match planning_span.in_scope(|| planner.plan(&request.statement)) {
				Ok(plan) => plan,
				Err(error) => {
					let code = match error {
						FrontendError::SqlParse { .. } => SqlErrorCode::SyntaxError,
						FrontendError::Unsupported { .. }
						| FrontendError::RuneCompile { .. }
						| FrontendError::RuneRuntime { .. }
						| FrontendError::RuneSandbox { .. } => SqlErrorCode::PlanningError,
					};
					crate::telemetry::record_query_statement_class(
						&query_span,
						StatementClass::Unknown,
					);
					crate::telemetry::record_query_outcome(&query_span, "error");
					query_span.record("toron.error_class", "frontend");
					if crate::telemetry::enabled() {
						query_span.in_scope(|| {
							tracing::error!(
								error_class = "frontend",
								"toron query planning failed"
							);
						});
					}
					let audit_event = ToronAuditEvent::query_outcome(
						ToronAuditOperation::QueryPlannedError,
						tenant_id,
						session_id,
						frame.query_id,
						StatementClass::Unknown,
						AuditOutcome::Denied,
					)
					.with_sql_error_code(code)
					.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(
						&audit,
						audit_event,
						&mut stream,
						frame.query_id,
					)
					.await?
					{
						return Ok(());
					}
					return write_sql_error(&mut stream, frame.query_id, code, error.to_string())
						.await;
				}
			};
			let statement_class = StatementClass::from_plan(&plan);
			crate::telemetry::record_query_statement_class(&query_span, statement_class);
			let session_timezone = request.session_timezone.trim().to_string();
			if let Err(error) = crate::timestamp::parse_session_timezone(&session_timezone) {
				crate::telemetry::record_query_outcome(&query_span, "error");
				query_span.record("toron.error_class", "binding");
				let audit_event = ToronAuditEvent::query_outcome(
					ToronAuditOperation::QueryPlannedError,
					tenant_id,
					session_id,
					frame.query_id,
					statement_class,
					AuditOutcome::Denied,
				)
				.with_sql_error_code(SqlErrorCode::BindingError)
				.with_duration(query_started.elapsed());
				if !record_sql_audit_required_or_error(
					&audit,
					audit_event,
					&mut stream,
					frame.query_id,
				)
				.await?
				{
					return Ok(());
				}
				return write_sql_error(
					&mut stream,
					frame.query_id,
					SqlErrorCode::BindingError,
					error.to_string(),
				)
				.await;
			}
			let planner_config = coordinator
				.planner_config()
				.with_session_timezone(session_timezone.clone());
			let mut context = sql_execution_context(tenant_id, frame.query_id, session_id);
			context.deadline = std::time::Instant::now().checked_add(timeout_duration);
			context.trace_context = query_trace.trace_context();
			let returns_rows = sql_plan_returns_rows(&plan);
			let execution = async {
				if session_timezone == "UTC" {
					coordinator.execute_query_with_context(&plan, context).await
				} else {
					coordinator
						.execute_query_with_context_and_config(&plan, context, planner_config)
						.await
				}
			}
			.instrument(query_span.clone());
			tokio::pin!(execution);
			let timeout_sleep = tokio::time::sleep(timeout_duration);
			tokio::pin!(timeout_sleep);
			let connection_closed = connection.closed();
			tokio::pin!(connection_closed);
			let execution = tokio::select! {
				result = &mut execution => Some(result),
				_ = &mut timeout_sleep => {
					crate::telemetry::record_query_outcome(&query_span, "timeout");
					query_span.record("toron.error_class", "timeout");
					if crate::telemetry::enabled() {
						query_span.in_scope(|| {
							tracing::error!(error_class = "timeout", "toron query timed out");
						});
					}
					let cancellation = coordinator.cancel_query_for_tenant(
						tenant_id,
						frame.query_id,
						CancellationTrigger::StatementTimeout,
					);
					metrics.record_cancellation(
						CancellationTrigger::StatementTimeout,
						cancellation_request_outcome(&cancellation),
					);
					let audit_event = ToronAuditEvent::query_outcome(
							ToronAuditOperation::QueryTimedOut,
							tenant_id,
							session_id,
							frame.query_id,
							statement_class,
							AuditOutcome::Denied,
						)
						.with_sql_error_code(SqlErrorCode::StatementTimeout)
						.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(
						&audit,
						audit_event,
						&mut stream,
						frame.query_id,
					)
					.await?
					{
						return Ok(());
					}
					return write_sql_error(
						&mut stream,
						frame.query_id,
						SqlErrorCode::StatementTimeout,
						"statement timeout exceeded",
					)
					.await;
				}
				_ = &mut connection_closed => {
					crate::telemetry::record_query_outcome(&query_span, "client_disconnect");
					query_span.record("toron.error_class", "client_disconnect");
					if crate::telemetry::enabled() {
						query_span.in_scope(|| {
							tracing::error!(
								error_class = "client_disconnect",
								"toron query client disconnected"
							);
						});
					}
					let cancellation = coordinator.cancel_query_for_tenant(
						tenant_id,
						frame.query_id,
						CancellationTrigger::ClientDisconnect,
					);
					metrics.record_cancellation(
						CancellationTrigger::ClientDisconnect,
						cancellation_request_outcome(&cancellation),
					);
					record_sql_audit_required_without_response(
						&audit,
						ToronAuditEvent::query_outcome(
							ToronAuditOperation::QueryDisconnectAborted,
							tenant_id,
							session_id,
							frame.query_id,
							statement_class,
							AuditOutcome::Error,
						)
						.with_duration(query_started.elapsed()),
					)
					.await;
					return Ok(());
				}
			};
			let batches = match execution {
				Some(Ok(batches)) => batches,
				Some(Err(error)) => {
					let (code, message) = map_sql_query_error(error);
					crate::telemetry::record_query_outcome(&query_span, "error");
					let error_class = match code {
						SqlErrorCode::SyntaxError => "syntax",
						SqlErrorCode::PlanningError => "planning",
						SqlErrorCode::CatalogError => "catalog",
						SqlErrorCode::BindingError => "binding",
						SqlErrorCode::StorageError => "storage",
						SqlErrorCode::ResourceExhausted => "resource_exhausted",
						SqlErrorCode::StatementTimeout => "timeout",
						SqlErrorCode::Cancelled => "cancelled",
						SqlErrorCode::TenantMismatch => "tenant_mismatch",
						SqlErrorCode::CancellationUnsupported => "cancellation_unsupported",
						SqlErrorCode::NoLiveWorker => "no_live_worker",
						SqlErrorCode::Internal => "internal",
					};
					query_span.record("toron.error_class", error_class);
					if crate::telemetry::enabled() {
						query_span.in_scope(|| {
							tracing::error!(error_class, "toron query execution failed");
						});
					}
					let operation = if code == SqlErrorCode::Cancelled {
						ToronAuditOperation::QueryCancelled
					} else {
						ToronAuditOperation::QueryExecutionError
					};
					let outcome = if code == SqlErrorCode::Cancelled {
						AuditOutcome::Denied
					} else {
						AuditOutcome::Error
					};
					let audit_event = ToronAuditEvent::query_outcome(
						operation,
						tenant_id,
						session_id,
						frame.query_id,
						statement_class,
						outcome,
					)
					.with_sql_error_code(code)
					.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(
						&audit,
						audit_event,
						&mut stream,
						frame.query_id,
					)
					.await?
					{
						return Ok(());
					}
					return write_sql_error(&mut stream, frame.query_id, code, message).await;
				}
				None => unreachable!("SQL execution select always returns or exits"),
			};
			let batches = restamp_sql_result_timezones(&batches, &session_timezone)?;
			let row_count = if returns_rows {
				batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64
			} else {
				sql_affected_row_count(&batches)
			};
			let encoded_result_batches = if returns_rows && audit.fail_closed_enabled() {
				let mut encoded = Vec::with_capacity(batches.len());
				for (batch_index, batch) in batches.iter().enumerate() {
					let ipc = match encode_sql_batch(batch) {
						Ok(ipc) => ipc,
						Err(error) => {
							let (code, message) = map_sql_query_error(error);
							crate::telemetry::record_query_outcome(&query_span, "error");
							query_span.record("toron.error_class", "internal");
							let audit_event = ToronAuditEvent::query_outcome(
								ToronAuditOperation::QueryExecutionError,
								tenant_id,
								session_id,
								frame.query_id,
								statement_class,
								AuditOutcome::Error,
							)
							.with_sql_error_code(code)
							.with_duration(query_started.elapsed());
							if !record_sql_audit_required_or_error(
								&audit,
								audit_event,
								&mut stream,
								frame.query_id,
							)
							.await?
							{
								return Ok(());
							}
							return write_sql_error(&mut stream, frame.query_id, code, message)
								.await;
						}
					};
					let batch_index =
						u32::try_from(batch_index).map_err(|_| QueryError::Internal {
							message: "too many SQL result batches".to_string(),
						})?;
					encoded.push((batch_index, ipc));
				}
				Some(encoded)
			} else {
				None
			};
			let audit_event = ToronAuditEvent::query_outcome(
				ToronAuditOperation::QueryCompleted,
				tenant_id,
				session_id,
				frame.query_id,
				statement_class,
				AuditOutcome::Allowed,
			)
			.with_duration(query_started.elapsed())
			.with_row_count(row_count);
			if audit.fail_closed_enabled()
				&& !record_sql_audit_required_or_error(
					&audit,
					audit_event.clone(),
					&mut stream,
					frame.query_id,
				)
				.await?
			{
				return Ok(());
			}
			if returns_rows {
				if let Some(encoded) = encoded_result_batches {
					for (batch_index, ipc) in encoded {
						let response = SqlFrame::response(
							frame.query_id,
							SqlMessage::ResultBatch(ResultBatch {
								batch_index,
								arrow_ipc_bytes: ByteBuf::from(ipc),
							}),
						);
						write_sql_frame(&mut stream, &response).await?;
					}
				} else {
					for (batch_index, batch) in batches.iter().enumerate() {
						let ipc = encode_sql_batch(batch)?;
						let response = SqlFrame::response(
							frame.query_id,
							SqlMessage::ResultBatch(ResultBatch {
								batch_index: u32::try_from(batch_index).map_err(|_| {
									QueryError::Internal {
										message: "too many SQL result batches".to_string(),
									}
								})?,
								arrow_ipc_bytes: ByteBuf::from(ipc),
							}),
						);
						write_sql_frame(&mut stream, &response).await?;
					}
				}
			}
			if !audit.fail_closed_enabled()
				&& !record_sql_audit_required_or_error(
					&audit,
					audit_event,
					&mut stream,
					frame.query_id,
				)
				.await?
			{
				return Ok(());
			}
			crate::telemetry::record_query_outcome(&query_span, "ok");
			let response =
				SqlFrame::response(frame.query_id, SqlMessage::Complete(Complete { row_count }));
			write_sql_frame(&mut stream, &response).await?;
			stream.finish().await?;
		}
		SqlMessage::ExecuteRune(request) => {
			let query_started = std::time::Instant::now();
			let session_id = new_sql_session_id();
			let query_trace = crate::telemetry::query_trace_context(frame.query_id);
			let query_span = crate::telemetry::query_span(query_trace);
			crate::telemetry::record_query_frontend(&query_span, "rune");

			if let Some(failure) = rune_version_skew(&request) {
				crate::telemetry::record_query_statement_class(
					&query_span,
					StatementClass::Unknown,
				);
				crate::telemetry::record_query_outcome(&query_span, "rejected");
				let audit_event = ToronAuditEvent::query_outcome(
					ToronAuditOperation::QueryPlannedError,
					tenant_id,
					session_id,
					frame.query_id,
					StatementClass::Unknown,
					AuditOutcome::Denied,
				)
				.with_query_frontend(QueryFrontend::Rune)
				.with_duration(query_started.elapsed());
				if !record_sql_audit_required_or_error(
					&audit,
					audit_event,
					&mut stream,
					frame.query_id,
				)
				.await?
				{
					return Ok(());
				}
				return write_rune_failure(&mut stream, frame.query_id, failure).await;
			}

			let planning_span = crate::telemetry::planning_span(query_trace);
			let plan = match execute_rune_send(
				Arc::clone(&options.rune_engine),
				request.source,
				options.rune_sandbox,
			)
			.instrument(planning_span)
			.await
			{
				Ok(plan) => plan,
				Err(error) => {
					let failure = rune_failure_from_frontend(error);
					crate::telemetry::record_query_statement_class(
						&query_span,
						StatementClass::Unknown,
					);
					crate::telemetry::record_query_outcome(&query_span, "error");
					query_span.record("toron.error_class", "rune_frontend");
					let audit_event = ToronAuditEvent::query_outcome(
						ToronAuditOperation::QueryPlannedError,
						tenant_id,
						session_id,
						frame.query_id,
						StatementClass::Unknown,
						AuditOutcome::Denied,
					)
					.with_query_frontend(QueryFrontend::Rune)
					.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(
						&audit,
						audit_event,
						&mut stream,
						frame.query_id,
					)
					.await?
					{
						return Ok(());
					}
					return write_rune_failure(&mut stream, frame.query_id, failure).await;
				}
			};

			let statement_class = StatementClass::from_plan(&plan);
			crate::telemetry::record_query_statement_class(&query_span, statement_class);
			// The materialize verb is the sole Rune write: a CTAS carrying its
			// certificate. Everything else stays behind the read-only fence.
			let certified_materialization = matches!(
				&plan,
				LogicalPlan::CreateTableAsSelect {
					materialization_certificate: Some(_),
					..
				}
			);
			if statement_class != StatementClass::Select && !certified_materialization {
				let failure = RuneFailure {
					kind: RuneFailureKind::Execution(SqlErrorCode::PlanningError),
					message: "Rune workspace first contact is read-only".to_owned(),
					span: None,
				};
				crate::telemetry::record_query_outcome(&query_span, "rejected");
				let audit_event = ToronAuditEvent::query_outcome(
					ToronAuditOperation::QueryPlannedError,
					tenant_id,
					session_id,
					frame.query_id,
					statement_class,
					AuditOutcome::Denied,
				)
				.with_query_frontend(QueryFrontend::Rune)
				.with_duration(query_started.elapsed());
				if !record_sql_audit_required_or_error(
					&audit,
					audit_event,
					&mut stream,
					frame.query_id,
				)
				.await?
				{
					return Ok(());
				}
				return write_rune_failure(&mut stream, frame.query_id, failure).await;
			}

			let timeout_duration = request
				.statement_timeout_seconds
				.map(|seconds| std::time::Duration::from_secs(u64::from(seconds)))
				.unwrap_or(options.statement_timeout);
			let mut context = sql_execution_context(tenant_id, frame.query_id, session_id);
			context.deadline = std::time::Instant::now().checked_add(timeout_duration);
			context.trace_context = query_trace.trace_context();
			// ADR 574: a certified materialization takes the receipt-bearing
			// entry point. Both arms share the timeout and disconnect handling
			// below, so the outcome is carried rather than the call duplicated.
			let execution = async {
				if certified_materialization {
					coordinator
						.execute_materialization_with_context(&plan, context)
						.await
						.map(RuneExecutionOutput::Materialized)
				} else {
					coordinator
						.execute_query_with_context(&plan, context)
						.await
						.map(RuneExecutionOutput::Batches)
				}
			}
			.instrument(query_span.clone());
			tokio::pin!(execution);
			let timeout_sleep = tokio::time::sleep(timeout_duration);
			tokio::pin!(timeout_sleep);
			let connection_closed = connection.closed();
			tokio::pin!(connection_closed);
			let output = tokio::select! {
				result = &mut execution => match result {
					Ok(output) => output,
					Err(error) => {
						let failure = rune_execution_failure(error);
						crate::telemetry::record_query_outcome(&query_span, "error");
						query_span.record("toron.error_class", "rune_execution");
						let audit_event = ToronAuditEvent::query_outcome(
							ToronAuditOperation::QueryExecutionError,
							tenant_id,
							session_id,
							frame.query_id,
							statement_class,
							AuditOutcome::Error,
						)
						.with_query_frontend(QueryFrontend::Rune)
						.with_duration(query_started.elapsed());
						if !record_sql_audit_required_or_error(&audit, audit_event, &mut stream, frame.query_id).await? {
							return Ok(());
						}
						return write_rune_failure(&mut stream, frame.query_id, failure).await;
					}
				},
				_ = &mut timeout_sleep => {
					let cancellation = coordinator.cancel_query_for_tenant(
						tenant_id,
						frame.query_id,
						CancellationTrigger::StatementTimeout,
					);
					metrics.record_cancellation(
						CancellationTrigger::StatementTimeout,
						cancellation_request_outcome(&cancellation),
					);
					crate::telemetry::record_query_outcome(&query_span, "timeout");
					let audit_event = ToronAuditEvent::query_outcome(
						ToronAuditOperation::QueryTimedOut,
						tenant_id,
						session_id,
						frame.query_id,
						statement_class,
						AuditOutcome::Denied,
					)
					.with_query_frontend(QueryFrontend::Rune)
					.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(&audit, audit_event, &mut stream, frame.query_id).await? {
						return Ok(());
					}
					return write_rune_failure(&mut stream, frame.query_id, RuneFailure {
						kind: RuneFailureKind::Execution(SqlErrorCode::StatementTimeout),
						message: "statement timeout exceeded".to_owned(),
						span: None,
					}).await;
				},
				_ = &mut connection_closed => {
					let cancellation = coordinator.cancel_query_for_tenant(
						tenant_id,
						frame.query_id,
						CancellationTrigger::ClientDisconnect,
					);
					metrics.record_cancellation(
						CancellationTrigger::ClientDisconnect,
						cancellation_request_outcome(&cancellation),
					);
					record_sql_audit_required_without_response(
						&audit,
						ToronAuditEvent::query_outcome(
							ToronAuditOperation::QueryDisconnectAborted,
							tenant_id,
							session_id,
							frame.query_id,
							statement_class,
							AuditOutcome::Error,
						).with_query_frontend(QueryFrontend::Rune),
					).await;
					return Ok(());
				},
			};

			let batches = match output {
				RuneExecutionOutput::Batches(batches) => batches,
				RuneExecutionOutput::Materialized(receipt) => {
					crate::telemetry::record_query_outcome(&query_span, "ok");
					let audit_event = ToronAuditEvent::query_outcome(
						ToronAuditOperation::QueryCompleted,
						tenant_id,
						session_id,
						frame.query_id,
						statement_class,
						AuditOutcome::Allowed,
					)
					.with_query_frontend(QueryFrontend::Rune)
					.with_duration(query_started.elapsed());
					if !record_sql_audit_required_or_error(
						&audit,
						audit_event,
						&mut stream,
						frame.query_id,
					)
					.await?
					{
						return Ok(());
					}
					let response = SqlFrame::response(
						frame.query_id,
						SqlMessage::MaterializeOutcome(
							crate::sql_wire::MaterializeOutcomeResponse {
								namespace: receipt.namespace,
								table_name: receipt.table_name,
								certificate_format_version: receipt.certificate_format_version,
								rows_written: receipt.rows_written,
							},
						),
					);
					write_sql_frame(&mut stream, &response).await?;
					return Ok(());
				}
			};
			let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
			let mut encoded = Vec::with_capacity(batches.len());
			for (batch_index, batch) in batches.iter().enumerate() {
				let ipc = match encode_sql_batch(batch) {
					Ok(ipc) => ipc,
					Err(error) => {
						return write_rune_failure(
							&mut stream,
							frame.query_id,
							rune_execution_failure(error),
						)
						.await;
					}
				};
				encoded.push((
					u32::try_from(batch_index).map_err(|_| QueryError::Internal {
						message: "too many Rune result batches".to_owned(),
					})?,
					ipc,
				));
			}
			let audit_event = ToronAuditEvent::query_outcome(
				ToronAuditOperation::QueryCompleted,
				tenant_id,
				session_id,
				frame.query_id,
				statement_class,
				AuditOutcome::Allowed,
			)
			.with_query_frontend(QueryFrontend::Rune)
			.with_duration(query_started.elapsed())
			.with_row_count(row_count);
			if !record_sql_audit_required_or_error(&audit, audit_event, &mut stream, frame.query_id)
				.await?
			{
				return Ok(());
			}
			for (batch_index, ipc) in encoded {
				write_sql_frame(
					&mut stream,
					&SqlFrame::response(
						frame.query_id,
						SqlMessage::ResultBatch(ResultBatch {
							batch_index,
							arrow_ipc_bytes: ByteBuf::from(ipc),
						}),
					),
				)
				.await?;
			}
			crate::telemetry::record_query_outcome(&query_span, "ok");
			write_sql_frame(
				&mut stream,
				&SqlFrame::response(frame.query_id, SqlMessage::Complete(Complete { row_count })),
			)
			.await?;
			stream.finish().await?;
		}
		SqlMessage::Cancel(request) => {
			let cancellation = coordinator.cancel_query_for_tenant(
				tenant_id,
				request.query_id,
				CancellationTrigger::SqlCancel,
			);
			metrics.record_cancellation(
				CancellationTrigger::SqlCancel,
				cancellation_request_outcome(&cancellation),
			);
			let audit_event = ToronAuditEvent::query_outcome(
				ToronAuditOperation::QueryCancelled,
				tenant_id,
				new_sql_session_id(),
				request.query_id,
				StatementClass::Cancel,
				AuditOutcome::Denied,
			)
			.with_sql_error_code(SqlErrorCode::Cancelled);
			if !record_sql_audit_required_or_error(&audit, audit_event, &mut stream, frame.query_id)
				.await?
			{
				return Ok(());
			}
			write_sql_error(
				&mut stream,
				frame.query_id,
				SqlErrorCode::Cancelled,
				"query cancellation accepted",
			)
			.await?;
		}
		SqlMessage::ResultBatch(_)
		| SqlMessage::Complete(_)
		| SqlMessage::Error(_)
		| SqlMessage::RuneError(_)
		| SqlMessage::Healthy(_)
		| SqlMessage::MutationOutcome(_)
		| SqlMessage::CtasOutcome(_)
		| SqlMessage::MaterializeOutcome(_) => {
			if !record_sql_audit_required_or_error(
				&audit,
				ToronAuditEvent::edge_denial(
					ToronAuditOperation::SqlUnexpectedClientFrame,
					Some(tenant_id),
					"unexpected client sql response frame",
				),
				&mut stream,
				frame.query_id,
			)
			.await?
			{
				return Ok(());
			}
			write_sql_error(
				&mut stream,
				frame.query_id,
				SqlErrorCode::Internal,
				"unexpected client SQL response frame",
			)
			.await?;
		}
	}

	Ok(())
}

/// ADR 574: what a Rune execution produced — ordinary batches, or the receipt
/// a certified materialization owes its caller.
enum RuneExecutionOutput {
	Batches(Vec<RecordBatch>),
	Materialized(crate::coordinator::MaterializationReceipt),
}

fn public_mutation_stop_reason(value: u8) -> MutationStopReason {
	match value {
		0 => MutationStopReason::None,
		1 => MutationStopReason::Deadline,
		2 => MutationStopReason::NoProgress,
		3 => MutationStopReason::AttemptsExhausted,
		other => MutationStopReason::Other(other),
	}
}

fn public_mutation_commit_knowledge(value: u8) -> MutationCommitKnowledge {
	match value {
		0 => MutationCommitKnowledge::Unspecified,
		1 => MutationCommitKnowledge::Committed,
		2 => MutationCommitKnowledge::KnownNotCommitted,
		3 => MutationCommitKnowledge::Indeterminate,
		other => MutationCommitKnowledge::Other(other),
	}
}

fn new_sql_session_id() -> SessionId {
	SessionId(QueryId::new().get().as_u128())
}

fn sql_execution_context(
	tenant_id: TenantId,
	query_id: QueryId,
	session_id: SessionId,
) -> ExecutionContext {
	ExecutionContext::new(session_id, query_id, tenant_id, HlcBound(0))
}

async fn write_sql_error(
	stream: &mut QuicStream,
	query_id: QueryId,
	code: SqlErrorCode,
	message: impl Into<String>,
) -> Result<(), QueryError> {
	let response = SqlFrame::response(
		query_id,
		SqlMessage::Error(SqlError {
			code,
			message: message.into(),
		}),
	);
	write_sql_frame(stream, &response).await?;
	stream.finish().await?;
	Ok(())
}

async fn write_rune_failure(
	stream: &mut QuicStream,
	query_id: QueryId,
	failure: RuneFailure,
) -> Result<(), QueryError> {
	let response = SqlFrame::response(query_id, SqlMessage::RuneError(failure));
	write_sql_frame(stream, &response).await?;
	stream.finish().await?;
	Ok(())
}

fn rune_version_skew(request: &crate::sql_wire::ExecuteRuneRequest) -> Option<RuneFailure> {
	let checks = [
		(
			RuneVersionAxis::RequestProtocol,
			request.request_protocol_version.0,
			RuneRequestProtocolVersion::CURRENT.0,
		),
		(
			RuneVersionAxis::LanguageSurface,
			request.language_surface_version.0,
			RuneLanguageSurfaceVersion::CURRENT.0,
		),
		(
			RuneVersionAxis::SandboxContract,
			request.sandbox_contract_version.0,
			RuneSandboxContractVersion::CURRENT.0,
		),
	];
	checks
		.into_iter()
		.find(|(_, requested, supported)| requested != supported)
		.map(|(axis, requested, supported)| RuneFailure {
			kind: RuneFailureKind::VersionSkew(RuneVersionSkew {
				axis,
				requested,
				supported: vec![supported],
			}),
			message: format!(
				"unsupported Rune {axis:?} version {requested}; supported versions: [{supported}]"
			),
			span: None,
		})
}

fn public_rune_capability(capability: CapabilityCategory) -> RuneCapability {
	match capability {
		CapabilityCategory::Filesystem => RuneCapability::Filesystem,
		CapabilityCategory::Process => RuneCapability::Process,
		CapabilityCategory::Network => RuneCapability::Network,
		CapabilityCategory::Environment => RuneCapability::Environment,
		CapabilityCategory::Time => RuneCapability::Time,
		CapabilityCategory::Random => RuneCapability::Random,
		CapabilityCategory::Threading => RuneCapability::Threading,
		CapabilityCategory::Unknown => RuneCapability::Unknown,
	}
}

fn u64_saturating(value: usize) -> u64 {
	u64::try_from(value).unwrap_or(u64::MAX)
}

fn public_rune_sandbox_violation(
	violation: crate::rune_engine::RuneSandboxViolation,
) -> PublicRuneSandboxViolation {
	use crate::rune_engine::RuneSandboxViolation;
	match violation {
		RuneSandboxViolation::FuelExhausted {
			fuel_consumed,
			fuel_budget,
		} => PublicRuneSandboxViolation::FuelExhausted {
			fuel_consumed,
			fuel_budget,
		},
		RuneSandboxViolation::MemoryExhausted {
			bytes_used,
			bytes_budget,
		} => PublicRuneSandboxViolation::MemoryExhausted {
			bytes_used,
			bytes_budget,
		},
		RuneSandboxViolation::CapabilityDenied { capability } => {
			PublicRuneSandboxViolation::CapabilityDenied {
				capability: public_rune_capability(capability),
			}
		}
		RuneSandboxViolation::SourceTooLarge { bytes, budget } => {
			PublicRuneSandboxViolation::SourceTooLarge {
				bytes: u64_saturating(bytes),
				budget: u64_saturating(budget),
			}
		}
		RuneSandboxViolation::Timeout { elapsed } => PublicRuneSandboxViolation::Timeout {
			elapsed_millis: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
		},
		RuneSandboxViolation::InvalidEntrypoint { .. } => {
			PublicRuneSandboxViolation::InvalidEntrypoint
		}
		RuneSandboxViolation::InternalSandboxError { .. } => {
			PublicRuneSandboxViolation::InternalSandboxError
		}
	}
}

fn rune_failure_from_frontend(error: FrontendError) -> RuneFailure {
	match error {
		FrontendError::RuneCompile { message, span } => RuneFailure {
			kind: RuneFailureKind::Compile,
			message,
			span: span.map(|span| RuneSourceSpan {
				start: span.start,
				end: span.end,
			}),
		},
		FrontendError::RuneRuntime { message, span } => RuneFailure {
			kind: RuneFailureKind::Runtime,
			message,
			span: span.map(|span| RuneSourceSpan {
				start: span.start,
				end: span.end,
			}),
		},
		FrontendError::RuneSandbox { kind, message } => RuneFailure {
			kind: RuneFailureKind::Sandbox(public_rune_sandbox_violation(kind)),
			message,
			span: None,
		},
		other => RuneFailure {
			kind: RuneFailureKind::Execution(SqlErrorCode::PlanningError),
			message: other.to_string(),
			span: None,
		},
	}
}

fn rune_execution_failure(error: QueryError) -> RuneFailure {
	let (code, message) = map_sql_query_error(error);
	RuneFailure {
		kind: RuneFailureKind::Execution(code),
		message,
		span: None,
	}
}

async fn execute_rune_send(
	engine: Arc<RuneEngine>,
	source: String,
	config: RuneSandboxConfig,
) -> Result<LogicalPlan, FrontendError> {
	tokio::task::spawn_blocking(move || {
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.map_err(|error| FrontendError::RuneRuntime {
				message: format!("Rune execution runtime construction failed: {error}"),
				span: None,
			})?;
		runtime.block_on(engine.execute_script(&source, &config))
	})
	.await
	.map_err(|error| FrontendError::RuneRuntime {
		message: format!("Rune execution task failed: {error}"),
		span: None,
	})?
}

const AUDIT_UNAVAILABLE_SQL_MESSAGE: &str = "audit unavailable";

async fn record_sql_audit_required_or_error(
	audit: &ToronAuditEmitter,
	event: ToronAuditEvent,
	stream: &mut QuicStream,
	query_id: QueryId,
) -> Result<bool, QueryError> {
	match audit.record_required(event).await {
		Ok(()) => Ok(true),
		Err(error) => {
			tracing::warn!(error = %error, "required SQL audit unavailable");
			write_sql_error(
				stream,
				query_id,
				SqlErrorCode::Internal,
				AUDIT_UNAVAILABLE_SQL_MESSAGE,
			)
			.await?;
			Ok(false)
		}
	}
}

async fn record_sql_audit_required_without_response(
	audit: &ToronAuditEmitter,
	event: ToronAuditEvent,
) {
	if let Err(error) = audit.record_required(event).await {
		tracing::warn!(error = %error, "required SQL audit unavailable before close");
	}
}

fn map_sql_query_error(error: QueryError) -> (SqlErrorCode, String) {
	let (code, message) = match &error {
		QueryError::Fragment(_) => (SqlErrorCode::PlanningError, None),
		QueryError::Frontend(FrontendError::SqlParse { .. }) => (SqlErrorCode::SyntaxError, None),
		QueryError::Frontend(_) => (SqlErrorCode::PlanningError, None),
		QueryError::Binding(BindingError::Catalog(CatalogError::ResourceExhausted { message }))
		| QueryError::Catalog(CatalogError::ResourceExhausted { message }) => {
			(SqlErrorCode::ResourceExhausted, Some(message.as_str()))
		}
		QueryError::Binding(BindingError::Catalog(_)) => (SqlErrorCode::CatalogError, None),
		QueryError::Binding(_) => (SqlErrorCode::BindingError, None),
		QueryError::Catalog(_) => (SqlErrorCode::CatalogError, None),
		QueryError::Storage(crate::error::StorageError::Cancelled)
		| QueryError::Execution(crate::error::ExecutionError::Storage(
			crate::error::StorageError::Cancelled,
		)) => (SqlErrorCode::Cancelled, None),
		QueryError::Storage(_) => (SqlErrorCode::StorageError, None),
		QueryError::Execution(crate::error::ExecutionError::BudgetExhausted {
			dimension: BudgetDimension::Memory,
			..
		}) => (
			SqlErrorCode::ResourceExhausted,
			Some("tenant memory budget exhausted"),
		),
		QueryError::Execution(crate::error::ExecutionError::ConcurrencyExhausted { .. }) => (
			SqlErrorCode::ResourceExhausted,
			Some("tenant concurrency limit exhausted"),
		),
		QueryError::Runtime(crate::error::RuntimeError::TaskFailure {
			kind:
				crate::error::TaskFailureKind::BudgetExhausted {
					dimension: BudgetDimension::Memory,
					..
				},
		}) => (
			SqlErrorCode::ResourceExhausted,
			Some("tenant memory budget exhausted"),
		),
		QueryError::Runtime(crate::error::RuntimeError::TaskFailure {
			kind: crate::error::TaskFailureKind::Cancelled,
		}) => (SqlErrorCode::Cancelled, None),
		QueryError::Internal { message } if message.contains("no live worker") => {
			(SqlErrorCode::NoLiveWorker, None)
		}
		_ => (SqlErrorCode::Internal, None),
	};
	(
		code,
		message
			.map(ToString::to_string)
			.unwrap_or_else(|| error.to_string()),
	)
}

fn cancellation_request_outcome(cancellation: &QueryCancellation) -> CancellationOutcome {
	if cancellation.first_request {
		CancellationOutcome::Requested
	} else {
		CancellationOutcome::AlreadyTerminal
	}
}

fn sql_plan_returns_rows(plan: &LogicalPlan) -> bool {
	matches!(
		plan,
		LogicalPlan::Scan { .. }
			| LogicalPlan::ScanNamed { .. }
			| LogicalPlan::Alias { .. }
			| LogicalPlan::Filter { .. }
			| LogicalPlan::Project { .. }
			| LogicalPlan::Aggregate { .. }
			| LogicalPlan::Exchange { .. }
			| LogicalPlan::Sort { .. }
			| LogicalPlan::Take { .. }
			| LogicalPlan::Join { .. }
			| LogicalPlan::Delete {
				returning: Some(_),
				..
			} | LogicalPlan::Update {
			returning: Some(_),
			..
		} | LogicalPlan::Insert {
			returning: Some(_),
			..
		} | LogicalPlan::InsertSelect {
			returning: Some(_),
			..
		}
	)
}

fn sql_affected_row_count(batches: &[RecordBatch]) -> u64 {
	batches
		.first()
		.and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
		.map(|column| u64::try_from(column.value(0)).unwrap_or(0))
		.unwrap_or(0)
}

fn restamp_sql_result_timezones(
	batches: &[RecordBatch],
	session_timezone: &str,
) -> Result<Vec<RecordBatch>, QueryError> {
	batches
		.iter()
		.map(|batch| restamp_sql_result_batch(batch, session_timezone))
		.collect()
}

fn restamp_sql_result_batch(
	batch: &RecordBatch,
	session_timezone: &str,
) -> Result<RecordBatch, QueryError> {
	let fields = batch
		.schema()
		.fields()
		.iter()
		.map(|field| restamp_sql_result_field(field.as_ref(), session_timezone))
		.collect::<Vec<_>>();
	let columns = batch
		.columns()
		.iter()
		.map(|column| restamp_sql_result_array(column, session_timezone))
		.collect::<Vec<_>>();
	RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(QueryError::from)
}

fn restamp_sql_result_field(field: &Field, session_timezone: &str) -> Field {
	let data_type = match field.data_type() {
		DataType::Timestamp(unit, Some(timezone)) if timezone.as_ref() == "UTC" => {
			DataType::Timestamp(*unit, Some(session_timezone.to_string().into()))
		}
		other => other.clone(),
	};
	Field::new(field.name().clone(), data_type, field.is_nullable())
		.with_metadata(field.metadata().clone())
}

fn restamp_sql_result_array(array: &ArrayRef, session_timezone: &str) -> ArrayRef {
	match array.data_type() {
		DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Arc::new(
				array
					.as_any()
					.downcast_ref::<TimestampMillisecondArray>()
					.expect("timestamp millisecond data type")
					.clone()
					.with_timezone(session_timezone.to_string()),
			)
		}
		DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
			if timezone.as_ref() == "UTC" =>
		{
			Arc::new(
				array
					.as_any()
					.downcast_ref::<TimestampMicrosecondArray>()
					.expect("timestamp microsecond data type")
					.clone()
					.with_timezone(session_timezone.to_string()),
			)
		}
		DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) if timezone.as_ref() == "UTC" => {
			Arc::new(
				array
					.as_any()
					.downcast_ref::<TimestampNanosecondArray>()
					.expect("timestamp nanosecond data type")
					.clone()
					.with_timezone(session_timezone.to_string()),
			)
		}
		_ => array.clone(),
	}
}

fn encode_sql_batch(batch: &RecordBatch) -> Result<Vec<u8>, QueryError> {
	let mut buffer = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
		writer.write(batch)?;
		writer.finish()?;
	}
	Ok(buffer)
}

/// Total attempts for the ephemeral loopback worker pair (see `bind_loopback_worker_pair`).
const WORKER_PAIR_ATTEMPTS: u32 = 8;

/// Binds the loopback worker pair: the TCP compatibility listener at the configured address, then
/// the QUIC (UDP) endpoint at the TCP-selected port, so the one published `worker_endpoint` reaches
/// both. A TCP port reserves nothing on UDP: only when the configured port is 0 (kernel-chosen) and
/// the UDP bind fails with AddrInUse is that attempt's TCP listener dropped and the pair re-chosen,
/// at most `attempts` attempts in total; every other error, and any failure for an explicit port,
/// is returned unchanged. Under `cfg(test)` only, `before_udp` runs after each TCP bind and may
/// substitute the UDP bind's result; production builds have no such parameter.
async fn bind_loopback_worker_pair(
	config: &ToronBootstrapConfig,
	attempts: u32,
	#[cfg(test)] before_udp: &mut dyn FnMut(SocketAddr) -> Option<std::io::Error>,
) -> std::io::Result<(TcpListener, QuicEndpoint)> {
	let ephemeral = config.worker.task_listener_bind.port() == 0;
	let mut attempt = 1;
	loop {
		let listener = TcpListener::bind(config.worker.task_listener_bind).await?;
		let worker_bind_endpoint = listener.local_addr()?;
		#[cfg(test)]
		let bound = match before_udp(worker_bind_endpoint) {
			Some(error) => Err(error),
			None => QuicEndpoint::bind_loopback(worker_bind_endpoint),
		};
		#[cfg(not(test))]
		let bound = QuicEndpoint::bind_loopback(worker_bind_endpoint);
		match bound {
			Ok(endpoint) => return Ok((listener, endpoint)),
			Err(error)
				if ephemeral
					&& error.kind() == std::io::ErrorKind::AddrInUse
					&& attempt < attempts =>
			{
				tracing::warn!(
					address = %worker_bind_endpoint,
					attempt,
					attempts,
					"worker UDP port taken after the TCP bind; re-pairing"
				);
				drop(listener);
				attempt += 1;
			}
			Err(error) => return Err(error),
		}
	}
}

fn install_tracing(config: &ToronBootstrapConfig) {
	match config.observability.format {
		crate::config::TracingFormat::Pretty => {}
	}
	let filter = EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| EnvFilter::new(config.observability.level.clone()));
	let _ = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr)
		.try_init();
}

fn coordinator_workers(
	config: &ToronBootstrapConfig,
	worker_endpoint: SocketAddr,
	advertised_endpoint: WorkerEndpoint,
	memory_budget_bytes: u64,
) -> Vec<WorkerEntry> {
	let mut workers = Vec::new();
	if config.coordinator.include_self_worker {
		workers.push(worker_entry(
			config.node.worker_id,
			advertised_endpoint,
			memory_budget_bytes,
		));
	}
	workers.extend(config.coordinator.static_workers.iter().map(|worker| {
		worker_entry(
			worker.worker_id,
			WorkerEndpoint::new(worker.endpoint.to_string()),
			memory_budget_bytes,
		)
	}));
	if workers.is_empty() {
		tracing::warn!(
			endpoint = %worker_endpoint,
			"SQL coordinator has no configured workers"
		);
	}
	workers
}

fn worker_entry(
	worker_id: crate::distributed::identity::WorkerId,
	endpoint: WorkerEndpoint,
	memory_budget_bytes: u64,
) -> WorkerEntry {
	WorkerEntry {
		worker_id,
		endpoint,
		lease: WorkerLease {
			last_heartbeat: UnixMillis::new(1),
			epoch: LeaseEpoch::new(1),
		},
		capabilities: WorkerCapabilities {
			advertised_memory: ByteCount::new(memory_budget_bytes),
			simd_width: SimdLaneCount::new(8),
			spill_capacity_reserved: ByteCount::new(0),
		},
		resources: WorkerResourceState {
			in_flight: ByteCount::ZERO,
			active_tasks: ActiveTaskCount::default(),
		},
		status: WorkerStatus::Live,
	}
}

async fn build_wendelon_client(
	config: &ToronWendelonStorageConfig,
	client_uuid: [u8; 16],
) -> Result<wendelon_client::Client, ToronDaemonError> {
	if let Some(tls) = &config.tls {
		let mut client_config = wendelon_client::ClientConfig::new(
			config.client_bind,
			config
				.bootstrap_peers
				.iter()
				.map(|peer| wendelon_client::BootstrapPeer {
					node_id: peer.node_id,
					address: peer.address.clone(),
				})
				.collect(),
			wendelon_client::TlsPaths {
				cert_path: tls.cert_path.clone(),
				key_path: tls.key_path.clone(),
				ca_path: tls.ca_path.clone(),
			},
		);
		client_config.cluster_domain = config.cluster_domain.clone();
		client_config.pool_max_retained = config.pool_max_retained;
		client_config.pool_max_cap_bytes = config.pool_max_cap_bytes;
		return Ok(wendelon_client::Client::connect_quic(client_config).await?);
	}

	let tier_bits = wendelon::network::transport::TransportTierSet::empty()
		.with(wendelon::network::transport::TransportTier::QuicIoUring)
		.as_u8();
	let role_bits = wendelon::network::transport::NodeRoleSet::empty()
		.with(wendelon::network::transport::NodeRole::Data)
		.with(wendelon::network::transport::NodeRole::Control)
		.as_u8();
	let bootstrap = wendelon::client::ClusterStatusWire {
		epoch: 0,
		leader: None,
		replication_factor: 1,
		peers: config
			.bootstrap_peers
			.iter()
			.map(|peer| {
				if peer.address.trim().is_empty() {
					return Err(ToronDaemonError::Invalid {
						message: "storage.wendelon.bootstrap_peers address must not be empty"
							.to_string(),
					});
				}
				Ok(wendelon::client::ClusterPeerWire {
					node_id: peer.node_id,
					address: peer.address.clone(),
					tier_bits,
					role_bits,
					capability_bits: 0,
				})
			})
			.collect::<Result<Vec<_>, _>>()?,
	};
	let topology: Arc<dyn wendelon::client::TopologySource> = Arc::new(
		wendelon::client::StaticTopologySource::new(bootstrap.clone()),
	);
	let providers: HashMap<
		wendelon::network::transport::TransportTier,
		Arc<dyn wendelon::network::transport::TransportProvider>,
	> = HashMap::with_capacity(0);
	let inner = wendelon::client::WendelonClient::new(wendelon::client::WendelonClientConfig {
		local_tiers: wendelon::network::transport::TransportTierSet::empty()
			.with(wendelon::network::transport::TransportTier::QuicIoUring),
		providers,
		bootstrap,
		topology,
		pool_max_retained: config.pool_max_retained,
		pool_max_cap_bytes: config.pool_max_cap_bytes,
	});
	Ok(wendelon_client::Client::with_client_uuid(
		inner,
		client_uuid,
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{BudgetDimension, ByteCount};
	use crate::error::{ExecutionError, RuntimeError, TaskFailureKind};
	use arrow::array::Array;
	use arrow::record_batch::RecordBatch;
	use tokio::task::JoinSet;

	fn retention_config() -> ToronBootstrapConfig {
		ToronBootstrapConfig {
			node: crate::config::ToronNodeConfig {
				worker_id: crate::distributed::identity::WorkerId::from_u128(42),
				tenant_id: TenantId(1),
				drain_timeout: Duration::from_secs(1),
			},
			worker: crate::config::ToronWorkerConfig {
				task_listener_bind: "127.0.0.1:0".parse().expect("loopback addr"),
				advertised_endpoint: None,
				query_memory_per_task_bytes: None,
				query_spill_bytes_reserved_bytes: None,
				memory_budget_bytes: 1024 * 1024,
			},
			tenant_budgets: crate::distributed::budget::TenantBudgetPolicy::omitted_config(
				ByteCount::new(1024 * 1024),
			),
			admission: None,
			storage: crate::config::ToronStorageConfig {
				wendelon: crate::config::ToronWendelonStorageConfig {
					bootstrap_peers: vec![crate::config::ToronWendelonBootstrapPeer {
						node_id: 1,
						address: "127.0.0.1:7001".to_string(),
					}],
					client_bind: "127.0.0.1:0".parse().expect("client bind"),
					tls: None,
					cluster_domain: wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string(),
					pool_max_retained: 64,
					pool_max_cap_bytes: 8 * 1024 * 1024,
				},
			},
			catalog: crate::config::ToronCatalogConfig {
				descriptor_bootstrap_path: None,
			},
			sql: None,
			coordinator: crate::config::CoordinatorConfig {
				include_self_worker: true,
				static_workers: Vec::new(),
			},
			tls: None,
			observability: crate::config::ObservabilityConfig {
				level: "info".to_string(),
				format: crate::config::TracingFormat::Pretty,
				listeners: None,
				telemetry: None,
			},
			audit: crate::config::AuditConfig::Disabled,
		}
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn completed_sql_connections_remain_in_the_daemon_join_set_until_drained() {
		let mut config = retention_config();
		config.sql = Some(crate::config::SqlConfig {
			transport: crate::config::SqlTransport::Quic,
			listener_bind: "127.0.0.1:0".parse().unwrap(),
			statement_timeout: Duration::from_secs(1),
			max_concurrent_connections: None,
			max_request_frame_bytes: NonZeroU32::new(
				crate::sql_wire::DEFAULT_SQL_REQUEST_FRAME_BYTES,
			)
			.unwrap(),
			max_statement_bytes: NonZeroU32::new(crate::sql_wire::DEFAULT_SQL_STATEMENT_BYTES)
				.unwrap(),
		});
		let daemon = ToronDaemon::start(config).await.unwrap();
		let endpoint = daemon.handle().sql_endpoint.unwrap();
		assert_eq!(daemon.connection_tasks.lock().await.len(), 0);
		for n in 1..=8 {
			let client = toron_client::ToronClient::loopback(endpoint.to_string()).unwrap();
			client.health().await.unwrap();
			drop(client);
			assert_eq!(daemon.connection_tasks.lock().await.len(), n);
		}
		// Joining, rather than RSS, proves the retained entries have finished. We deliberately
		// drain only here, after measuring all eight entries; production does this at shutdown.
		let mut tasks = daemon.connection_tasks.lock().await;
		for _ in 0..8 {
			timeout(Duration::from_secs(3), tasks.join_next())
				.await
				.unwrap()
				.unwrap()
				.unwrap();
		}
		assert_eq!(tasks.len(), 0);
		drop(tasks);
		println!("CONNECTION_PIN requests=8 retained_entries=8 drained_completed=8");
		daemon.shutdown().await.unwrap();
	}

	#[test]
	fn budget_exhausted_maps_to_resource_exhausted_sql_error() {
		let (code, message) =
			map_sql_query_error(QueryError::Execution(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				producer: "test",
				requested: ByteCount::new(16),
				available: ByteCount::new(8),
				detail: Default::default(),
			}));

		assert_eq!(code, SqlErrorCode::ResourceExhausted);
		assert_eq!(message, "tenant memory budget exhausted");
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn admitted_quic_worker_handshake_survives_maintenance_ticks() {
		let memory = Arc::new(MemoryManager::new(2, 1024 * 1024));
		let store = Arc::new(InMemoryPartitionStore::new());
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(crate::storage::InMemoryStorageScan {
				table_data: RecordBatch::new_empty(Arc::new(Schema::empty())),
			}),
			memory,
			store,
			crate::distributed::identity::WorkerId::from_u128(543),
			crate::distributed::worker_registry::WorkerEndpoint::new("127.0.0.1:0"),
		));
		let endpoint =
			QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap()).expect("bind QUIC worker");
		let address = endpoint.local_addr().expect("worker address");
		let policy = crate::distributed::budget::TenantBudgetPolicy::omitted_config(
			ByteCount::new(1024 * 1024),
		);
		let tenant_admission =
			crate::distributed::budget::TenantAdmissionController::new(policy.clone(), None);
		let metrics = Arc::new(MetricsRegistry::new(MetricsRegistryConfig {
			descriptors_loaded: 0,
			static_workers_configured: 0,
			tenant_budgets: policy.clone(),
			descriptor_sync_metrics: Arc::new(crate::catalog::DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(
				crate::catalog::DescriptorRefreshMetrics::default(),
			),
			cancellation_metrics: Arc::new(CancellationMetrics::default()),
			downstream_cancellation_metrics: Arc::new(DownstreamCancellationMetrics::default()),
			sql_connection_limiter: None,
			audit_metrics: None,
			runtime_evidence: None,
			tenant_admission,
			admission_rejections: crate::distributed::budget::TenantAdmissionRejectionCounters::new(
				&policy,
			),
		}));
		let connection_tasks = Arc::new(Mutex::new(JoinSet::new()));
		let gate = QuicWorkerHandshakeGate {
			entered: Arc::new(tokio::sync::Notify::new()),
			release: Arc::new(tokio::sync::Notify::new()),
		};
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let loop_handle = tokio::spawn(run_quic_worker_accept_loop_with_maintenance_interval(
			worker,
			endpoint,
			shutdown_rx,
			Arc::clone(&connection_tasks),
			metrics,
			Duration::from_millis(5),
			|| {},
			Some(gate.clone()),
		));
		let client = QuicEndpoint::client_loopback().expect("QUIC client");
		let connect = tokio::spawn(async move { client.connect_stream(address).await });

		tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
			.await
			.expect("server dequeued the incoming peer");
		// Make at least one maintenance tick ready while the admitted handshake is
		// deliberately held. That tick must not participate in the handshake race.
		tokio::time::sleep(Duration::from_millis(20)).await;
		gate.release.notify_one();
		let stream = tokio::time::timeout(Duration::from_secs(2), connect)
			.await
			.expect("admitted handshake timeout")
			.expect("client join")
			.expect("maintenance must not reject an admitted peer");
		drop(stream);
		let _ = shutdown_tx.send(());
		loop_handle.await.expect("worker accept loop join");
		connection_tasks.lock().await.abort_all();
	}

	#[test]
	fn typed_fragment_liveness_error_maps_to_planning_error() {
		let (code, message) = map_sql_query_error(QueryError::Fragment(
			crate::distributed::fragment::FragmentError::UndrainableLiveShuffle {
				producer_stage: crate::distributed::identity::StageId::new(1),
				consumer_stage: Some(crate::distributed::identity::StageId::new(2)),
			},
		));

		assert_eq!(code, SqlErrorCode::PlanningError);
		assert!(message.contains("live shuffle"));
		assert!(message.contains("StageId(1)"));
	}

	#[test]
	fn sql_concurrency_exhaustion_returns_resource_exhausted() {
		let (code, message) = map_sql_query_error(QueryError::Execution(
			ExecutionError::ConcurrencyExhausted {
				tenant_id: TenantId(7),
				requested: 1,
				available: 0,
			},
		));

		assert_eq!(code, SqlErrorCode::ResourceExhausted);
		assert_eq!(message, "tenant concurrency limit exhausted");
	}

	#[test]
	fn sql_result_timestamp_restamp_preserves_utc_ticks_and_sets_session_zone() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"observed_at",
			DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
			true,
		)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(
				TimestampMillisecondArray::from(vec![Some(1_704_091_200_000i64)])
					.with_timezone("UTC"),
			)],
		)
		.expect("timestamp batch");

		let restamped = restamp_sql_result_timezones(&[batch], "America/Chicago")
			.expect("restamp timestamp result");

		assert_eq!(
			restamped[0].schema().field(0).data_type(),
			&DataType::Timestamp(TimeUnit::Millisecond, Some("America/Chicago".into()))
		);
		let values = restamped[0]
			.column(0)
			.as_any()
			.downcast_ref::<TimestampMillisecondArray>()
			.expect("timestamp array");
		assert_eq!(values.value(0), 1_704_091_200_000i64);
	}

	#[test]
	fn sql_result_timestamp_restamp_preserves_explicit_non_utc_zone() {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"observed_at",
			DataType::Timestamp(TimeUnit::Millisecond, Some("America/Chicago".into())),
			true,
		)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(
				TimestampMillisecondArray::from(vec![Some(1_704_091_200_000i64)])
					.with_timezone("America/Chicago"),
			)],
		)
		.expect("timestamp batch");

		let restamped = restamp_sql_result_timezones(&[batch], "America/New_York")
			.expect("restamp timestamp result");

		assert_eq!(
			restamped[0].schema().field(0).data_type(),
			&DataType::Timestamp(TimeUnit::Millisecond, Some("America/Chicago".into()))
		);
		let values = restamped[0]
			.column(0)
			.as_any()
			.downcast_ref::<TimestampMillisecondArray>()
			.expect("timestamp array");
		assert_eq!(
			values.data_type(),
			&DataType::Timestamp(TimeUnit::Millisecond, Some("America/Chicago".into()))
		);
		assert_eq!(values.value(0), 1_704_091_200_000i64);
	}

	#[test]
	fn catalog_resource_exhausted_maps_to_resource_exhausted_sql_error() {
		let (code, message) =
			map_sql_query_error(QueryError::Catalog(CatalogError::ResourceExhausted {
				message: "tenant descriptor sync limit exhausted".to_string(),
			}));

		assert_eq!(code, SqlErrorCode::ResourceExhausted);
		assert_eq!(message, "tenant descriptor sync limit exhausted");
	}

	#[test]
	fn distributed_task_budget_exhausted_maps_to_resource_exhausted_sql_error() {
		let (code, message) = map_sql_query_error(QueryError::Runtime(RuntimeError::TaskFailure {
			kind: TaskFailureKind::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				requested: ByteCount::new(16),
				available: ByteCount::new(8),
			},
		}));

		assert_eq!(code, SqlErrorCode::ResourceExhausted);
		assert_eq!(message, "tenant memory budget exhausted");
	}

	// The loopback worker pair. Collisions use real sockets held on the TCP-selected UDP port; only
	// the error-classification case substitutes an error. Free-port acquisition in the fixed-port
	// cases is a bounded race with other processes on the host, acknowledged where it occurs.
	fn pair_config(bind: &str) -> ToronBootstrapConfig {
		let mut config = retention_config();
		config.worker.task_listener_bind = bind.parse().expect("pair bind");
		config
	}
	fn kind<T>(result: &std::io::Result<T>) -> Option<std::io::ErrorKind> {
		result.as_ref().err().map(std::io::Error::kind)
	}
	// Holds UDP at `addr` if it is free; either way the subsequent UDP bind there is taken.
	fn hold_udp(held: &mut Vec<std::net::UdpSocket>, addr: SocketAddr) {
		if let Ok(socket) = std::net::UdpSocket::bind(addr) {
			held.push(socket);
		}
	}
	// A port free on both TCP and UDP when checked (a bounded race: released before use).
	fn free_pair_port() -> u16 {
		for _ in 0..16 {
			let tcp = std::net::TcpListener::bind("127.0.0.1:0").expect("probe TCP");
			let port = tcp.local_addr().expect("probe addr").port();
			if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
				return port;
			}
		}
		panic!("no port free on both TCP and UDP in 16 tries");
	}

	#[tokio::test]
	async fn ephemeral_pair_re_pairs_past_a_udp_port_held_after_the_tcp_bind() {
		let (mut held, mut seen) = (Vec::new(), Vec::new());
		let (tcp, quic) = bind_loopback_worker_pair(
			&pair_config("127.0.0.1:0"),
			WORKER_PAIR_ATTEMPTS,
			&mut |addr| {
				if seen.is_empty() {
					hold_udp(&mut held, addr);
				}
				seen.push(addr);
				None
			},
		)
		.await
		.expect("re-paired");
		let (t, u) = (tcp.local_addr().unwrap(), quic.local_addr().unwrap());
		assert_eq!(seen.len(), 2, "exactly one re-pair");
		assert_eq!(t.port(), u.port(), "one number reaches both");
		assert_ne!(t.port(), seen[0].port(), "the held port was skipped");
		assert!(t.ip().is_loopback() && u.ip().is_loopback());
	}

	#[tokio::test]
	async fn ephemeral_pair_exhausts_after_eight_attempts_and_releases_each_tcp_listener() {
		let (mut held, mut seen) = (Vec::new(), Vec::new());
		let result = bind_loopback_worker_pair(
			&pair_config("127.0.0.1:0"),
			WORKER_PAIR_ATTEMPTS,
			&mut |addr| {
				hold_udp(&mut held, addr);
				seen.push(addr);
				None
			},
		)
		.await;
		assert_eq!(kind(&result), Some(std::io::ErrorKind::AddrInUse));
		assert_eq!(seen.len(), 8, "eight attempts in total, no ninth");
		// Every attempt's TCP listener was dropped (bounded race: another process could take one).
		for addr in &seen {
			assert!(
				std::net::TcpListener::bind(addr).is_ok(),
				"{addr}: TCP released"
			);
		}
	}

	/// A control, not the old-source regression: one attempt leaves the collision unrepaired.
	#[tokio::test]
	async fn a_single_attempt_returns_the_unpaired_collision() {
		let (mut held, mut seen) = (Vec::new(), Vec::new());
		let result = bind_loopback_worker_pair(&pair_config("127.0.0.1:0"), 1, &mut |addr| {
			hold_udp(&mut held, addr);
			seen.push(addr);
			None
		})
		.await;
		assert_eq!(kind(&result), Some(std::io::ErrorKind::AddrInUse));
		assert_eq!(seen.len(), 1);
	}

	#[tokio::test]
	async fn an_ephemeral_udp_error_other_than_addr_in_use_is_returned_at_once() {
		let mut calls = 0;
		let result = bind_loopback_worker_pair(
			&pair_config("127.0.0.1:0"),
			WORKER_PAIR_ATTEMPTS,
			&mut |_| {
				calls += 1;
				Some(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
			},
		)
		.await;
		assert_eq!(kind(&result), Some(std::io::ErrorKind::PermissionDenied));
		assert_eq!(calls, 1, "not retried");
	}

	#[tokio::test]
	async fn a_fixed_port_binds_exactly_that_port_on_both_protocols() {
		let port = free_pair_port();
		let mut calls = 0;
		let (tcp, quic) = bind_loopback_worker_pair(
			&pair_config(&format!("127.0.0.1:{port}")),
			WORKER_PAIR_ATTEMPTS,
			&mut |_| {
				calls += 1;
				None
			},
		)
		.await
		.expect("fixed pair");
		assert_eq!(tcp.local_addr().unwrap().port(), port);
		assert_eq!(quic.local_addr().unwrap().port(), port);
		assert_eq!(calls, 1);
	}

	#[tokio::test]
	async fn a_fixed_port_udp_conflict_is_returned_once_and_never_moved() {
		let port = free_pair_port();
		let _held = std::net::UdpSocket::bind(("127.0.0.1", port)).expect("hold fixed UDP");
		let mut seen = Vec::new();
		let result = bind_loopback_worker_pair(
			&pair_config(&format!("127.0.0.1:{port}")),
			WORKER_PAIR_ATTEMPTS,
			&mut |addr| {
				seen.push(addr);
				None
			},
		)
		.await;
		assert_eq!(kind(&result), Some(std::io::ErrorKind::AddrInUse));
		assert_eq!(seen.len(), 1, "one attempt for an explicit port");
		assert_eq!(seen[0].port(), port, "never an alternate port");
	}
}
