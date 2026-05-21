use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_bytes::ByteBuf;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing_subscriber::EnvFilter;

use crate::catalog::{WendelonCatalog, WendelonDescriptorRegistry};
use crate::config::{
	OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC, ToronBootstrapConfig, ToronConfigError,
	ToronWendelonStorageConfig, load_descriptor_bootstrap, load_toron_bootstrap_config,
};
use crate::coordinator::DistributedQueryCoordinator;
use crate::distributed::budget::{
	BudgetDimension, ByteCount, QueryBudget, TenantDescriptorSyncAccountant,
};
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
use crate::server::transport::QuicTransport;
use crate::server::{ToronWorker, memory::MemoryManager};
use crate::sql_planner::SqlPlanner;
use crate::sql_wire::{
	Complete, ResultBatch, SqlError, SqlErrorCode, SqlFrame, SqlMessage,
	read_request_frame_bounded, write_frame as write_sql_frame,
};
use crate::storage::{
	HlcBound, StorageScan, TenantId, WendelonStorageAdmin, WendelonStorageDeleteRows,
	WendelonStorageScan, WendelonStorageWrite,
};

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
}

impl ToronDaemon {
	pub async fn start(config: ToronBootstrapConfig) -> Result<Self, ToronDaemonError> {
		install_tracing(&config);
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
		install_tracing(&config);
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
		let descriptor_triples = match &config.catalog.descriptor_bootstrap_path {
			Some(path) => load_descriptor_bootstrap(path)?,
			None => Vec::new(),
		};
		let descriptors_loaded = descriptor_triples.len() as u64;
		let registry = Arc::new(WendelonDescriptorRegistry::from_tenant_descriptors(
			descriptor_triples,
		)?);
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
		let listener = if loopback_task_listener {
			Some(TcpListener::bind(config.worker.task_listener_bind).await?)
		} else {
			None
		};
		let worker_bind_endpoint = if let Some(listener) = &listener {
			listener.local_addr()?
		} else {
			config.worker.task_listener_bind
		};
		let quic_worker_endpoint = match (&tls_policy, worker_bind_endpoint.ip().is_loopback()) {
			(Some(policy), false) => QuicEndpoint::bind_authenticated(
				worker_bind_endpoint,
				policy.server_config_for(ListenerRole::WorkerTask),
			)?,
			_ => QuicEndpoint::bind_loopback(worker_bind_endpoint)?,
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
		let worker = Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				worker_storage,
				Arc::clone(&memory),
				Arc::new(InMemoryPartitionStore::new()),
				config.node.worker_id,
				advertised_endpoint.clone(),
			)
			.with_downstream_cancellation_metrics(Arc::clone(&downstream_cancellation_metrics)),
		);

		let registry_workers = coordinator_workers(
			&config,
			worker_endpoint,
			advertised_endpoint,
			config.worker.memory_budget_bytes,
		);
		let worker_registry = Arc::new(StaticWorkerRegistry::new(registry_workers));
		let transport = Arc::new(match &tls_policy {
			Some(policy) => {
				QuicTransport::authenticated(policy.client_config(), policy.cluster_domain())?
			}
			None => QuicTransport::new()?,
		});
		let coordinator_catalog: Arc<dyn crate::catalog::Catalog> = catalog.clone();
		let coordinator_writer: Arc<dyn crate::storage::StorageWrite> = writer.clone();
		let coordinator_deleter: Arc<dyn crate::storage::StorageDeleteRows> = deleter.clone();
		let coordinator_admin: Arc<dyn crate::storage::StorageAdmin> = admin.clone();
		let cancellation_metrics = Arc::new(CancellationMetrics::default());
		let coordinator = Arc::new(
			DistributedQueryCoordinator::with_writer_deleter_and_admin(
				coordinator_catalog,
				worker_registry,
				Arc::clone(&memory),
				transport,
				crate::binder::PlannerConfig::default(),
				QueryBudget::default(),
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
		let sql_connection_limiter = config.sql.as_ref().map(|sql| {
			SqlConnectionLimiter::new(
				sql.max_concurrent_connections,
				config.tenant_budgets.clone(),
			)
		});
		let metrics = Arc::new(MetricsRegistry::new(MetricsRegistryConfig {
			descriptors_loaded,
			static_workers_configured: config.coordinator.static_workers.len() as u64,
			tenant_budgets: config.tenant_budgets.clone(),
			descriptor_sync_metrics: catalog.descriptor_sync_metrics(),
			descriptor_refresh_metrics: catalog.descriptor_refresh_metrics(),
			cancellation_metrics,
			downstream_cancellation_metrics,
			sql_connection_limiter: sql_connection_limiter.clone(),
			tenant_admission: coordinator.tenant_admission_controller(),
			admission_rejections: coordinator.admission_rejection_counters(),
		}));
		let readiness = Arc::new(ReadinessState::new(
			true,
			true,
			config.observability.listeners.is_some(),
			descriptors_loaded,
			config.coordinator.static_workers.len() as u64,
		));

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
				SqlAcceptOptions {
					tenant_source: sql_tenant_source,
					connection_limiter: sql_connection_limiter
						.clone()
						.expect("SQL limiter exists when SQL config exists"),
					statement_timeout: sql_config.statement_timeout,
					max_request_frame_bytes: sql_config.max_request_frame_bytes,
					max_statement_bytes: sql_config.max_statement_bytes,
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
		})
	}

	pub fn handle(&self) -> &ToronDaemonHandle {
		&self.handle
	}

	pub async fn shutdown(mut self) -> Result<(), ToronDaemonError> {
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

		match timeout(self.drain_timeout, drain).await {
			Ok(result) => result,
			Err(_) => {
				let mut tasks = self.connection_tasks.lock().await;
				tasks.abort_all();
				while let Some(result) = tasks.join_next().await {
					let _ = result;
				}
				Err(ToronDaemonError::DrainTimeoutExceeded)
			}
		}
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
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => break,
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
	mut shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
) {
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => {
				endpoint.close();
				break;
			}
			accepted = endpoint.accept_connection() => match accepted {
				Ok(connection) => {
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
}

#[derive(Clone, Copy)]
struct SqlConnectionOptions {
	tenant_id: TenantId,
	statement_timeout: std::time::Duration,
	max_request_frame_bytes: NonZeroU32,
	max_statement_bytes: NonZeroU32,
}

async fn run_sql_accept_loop(
	coordinator: Arc<DistributedQueryCoordinator<StaticWorkerRegistry, QuicTransport>>,
	endpoint: QuicEndpoint,
	mut shutdown_rx: oneshot::Receiver<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	metrics: Arc<MetricsRegistry>,
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
							tracing::warn!(error = %error, "sql peer tenant extraction failed");
							connection.close(0_u32.into(), b"sql tenant rejected");
							continue;
						}
					};
					let tier2_permit = match options.connection_limiter.try_acquire_tier2(tenant_id) {
						Ok(permit) => permit,
						Err(_) => {
							metrics.increment_reject(ListenerName::Sql, RejectReason::ConnectionLimit);
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
					};
					let metrics = Arc::clone(&metrics);
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
) -> Result<(), QueryError> {
	let tenant_id = options.tenant_id;
	let frame =
		match read_request_frame_bounded(&mut stream, options.max_request_frame_bytes).await? {
			Ok(frame) => frame,
			Err(error) => {
				metrics.increment_reject(ListenerName::Sql, RejectReason::RequestFrameTooLarge);
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
		return write_sql_error(
			&mut stream,
			frame.query_id,
			SqlErrorCode::Internal,
			"unsupported SQL protocol version",
		)
		.await;
	}

	match frame.message {
		SqlMessage::Execute(request) => {
			if request.statement.len()
				> usize::try_from(options.max_statement_bytes.get()).expect("u32 fits usize")
			{
				metrics.increment_reject(ListenerName::Sql, RejectReason::StatementTooLarge);
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
			let plan = match planner.plan(&request.statement) {
				Ok(plan) => plan,
				Err(error) => {
					let code = match error {
						FrontendError::SqlParse { .. } => SqlErrorCode::SyntaxError,
						FrontendError::Unsupported { .. }
						| FrontendError::RuneCompile { .. }
						| FrontendError::RuneRuntime { .. }
						| FrontendError::RuneSandbox { .. } => SqlErrorCode::PlanningError,
					};
					return write_sql_error(&mut stream, frame.query_id, code, error.to_string())
						.await;
				}
			};
			let context = sql_execution_context(tenant_id, frame.query_id);
			let returns_rows = sql_plan_returns_rows(&plan);
			let execution = coordinator.execute_query_with_context(&plan, context);
			tokio::pin!(execution);
			let timeout_sleep = tokio::time::sleep(timeout_duration);
			tokio::pin!(timeout_sleep);
			let connection_closed = connection.closed();
			tokio::pin!(connection_closed);
			let execution = tokio::select! {
				result = &mut execution => Some(result),
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
					return write_sql_error(
						&mut stream,
						frame.query_id,
						SqlErrorCode::StatementTimeout,
						"statement timeout exceeded",
					)
					.await;
				}
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
					return Ok(());
				}
			};
			let batches = match execution {
				Some(Ok(batches)) => batches,
				Some(Err(error)) => {
					let (code, message) = map_sql_query_error(error);
					return write_sql_error(&mut stream, frame.query_id, code, message).await;
				}
				None => unreachable!("SQL execution select always returns or exits"),
			};
			if returns_rows {
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
			let row_count = if returns_rows {
				batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64
			} else {
				sql_affected_row_count(&batches)
			};
			let response =
				SqlFrame::response(frame.query_id, SqlMessage::Complete(Complete { row_count }));
			write_sql_frame(&mut stream, &response).await?;
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
			write_sql_error(
				&mut stream,
				frame.query_id,
				SqlErrorCode::Cancelled,
				"query cancellation accepted",
			)
			.await?;
		}
		SqlMessage::ResultBatch(_) | SqlMessage::Complete(_) | SqlMessage::Error(_) => {
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

fn sql_execution_context(tenant_id: TenantId, query_id: QueryId) -> ExecutionContext {
	let session_id = SessionId(QueryId::new().get().as_u128());
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

fn map_sql_query_error(error: QueryError) -> (SqlErrorCode, String) {
	let (code, message) = match &error {
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
			| LogicalPlan::Join { .. }
	)
}

fn sql_affected_row_count(batches: &[RecordBatch]) -> u64 {
	batches
		.first()
		.and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
		.map(|column| u64::try_from(column.value(0)).unwrap_or(0))
		.unwrap_or(0)
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

	#[test]
	fn budget_exhausted_maps_to_resource_exhausted_sql_error() {
		let (code, message) =
			map_sql_query_error(QueryError::Execution(ExecutionError::BudgetExhausted {
				dimension: BudgetDimension::Memory,
				requested: ByteCount::new(16),
				available: ByteCount::new(8),
			}));

		assert_eq!(code, SqlErrorCode::ResourceExhausted);
		assert_eq!(message, "tenant memory budget exhausted");
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
}
