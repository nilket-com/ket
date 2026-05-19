use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

use crate::catalog::{WendelonCatalog, WendelonDescriptorRegistry};
use crate::config::{
	ToronBootstrapConfig, ToronConfigError, ToronWendelonStorageConfig, load_descriptor_bootstrap,
	load_toron_bootstrap_config,
};
use crate::distributed::partition_store::InMemoryPartitionStore;
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::error::{CatalogError, QueryError};
use crate::server::ToronWorker;
use crate::server::memory::MemoryManager;
use crate::storage::{
	StorageScan, WendelonStorageAdmin, WendelonStorageDeleteRows, WendelonStorageScan,
	WendelonStorageWrite,
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
	#[error("toron daemon shutdown exceeded configured drain timeout")]
	DrainTimeoutExceeded,
	#[error("{message}")]
	Invalid { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToronDaemonHandle {
	pub worker_endpoint: SocketAddr,
}

#[allow(dead_code)]
struct ToronDaemonServices {
	catalog: Arc<WendelonCatalog>,
	scanner: Arc<WendelonStorageScan>,
	writer: Arc<WendelonStorageWrite>,
	deleter: Arc<WendelonStorageDeleteRows>,
	admin: Arc<WendelonStorageAdmin>,
}

#[allow(dead_code)]
pub struct ToronDaemon {
	handle: ToronDaemonHandle,
	services: ToronDaemonServices,
	shutdown_tx: Option<oneshot::Sender<()>>,
	listener_task: JoinHandle<()>,
	connection_tasks: Arc<Mutex<JoinSet<()>>>,
	drain_timeout: std::time::Duration,
}

impl ToronDaemon {
	pub async fn start(config: ToronBootstrapConfig) -> Result<Self, ToronDaemonError> {
		install_tracing(&config);

		let descriptor_pairs = match &config.catalog.descriptor_bootstrap_path {
			Some(path) => load_descriptor_bootstrap(path)?,
			None => Vec::new(),
		};
		let registry = Arc::new(WendelonDescriptorRegistry::from_descriptors(
			descriptor_pairs,
		)?);
		tracing::info!("descriptor_registry_constructed");

		let client = Arc::new(build_wendelon_client(
			&config.storage.wendelon,
			*config.node.worker_id.get().as_bytes(),
		)?);
		let tenant_id = wendelon::schema::TenantId::new(config.node.tenant_id.0);

		let catalog = Arc::new(WendelonCatalog::with_client_and_registry(
			Arc::clone(&client),
			tenant_id,
			Arc::clone(&registry),
		));
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

		let listener = TcpListener::bind(config.worker.task_listener_bind).await?;
		let worker_endpoint = listener.local_addr()?;
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
		let worker_storage: Arc<dyn StorageScan> = scanner.clone();
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			worker_storage,
			Arc::new(MemoryManager::new(32, memory_budget_bytes)),
			Arc::new(InMemoryPartitionStore::new()),
			config.node.worker_id,
			advertised_endpoint,
		));

		let connection_tasks = Arc::new(Mutex::new(JoinSet::new()));
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let listener_task = tokio::spawn(run_accept_loop(
			worker,
			listener,
			shutdown_rx,
			Arc::clone(&connection_tasks),
		));

		Ok(Self {
			handle: ToronDaemonHandle { worker_endpoint },
			services: ToronDaemonServices {
				catalog,
				scanner,
				writer,
				deleter,
				admin,
			},
			shutdown_tx: Some(shutdown_tx),
			listener_task,
			connection_tasks,
			drain_timeout: config.node.drain_timeout,
		})
	}

	pub fn handle(&self) -> &ToronDaemonHandle {
		&self.handle
	}

	pub async fn shutdown(mut self) -> Result<(), ToronDaemonError> {
		if let Some(tx) = self.shutdown_tx.take() {
			let _ = tx.send(());
		}
		self.listener_task
			.await
			.map_err(ToronDaemonError::ListenerTask)?;

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
) {
	loop {
		tokio::select! {
			_ = &mut shutdown_rx => break,
			accepted = listener.accept() => match accepted {
				Ok((stream, _peer)) => {
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

fn install_tracing(config: &ToronBootstrapConfig) {
	match config.observability.format {
		crate::config::ToronTracingFormat::Pretty => {}
	}
	let filter = EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| EnvFilter::new(config.observability.level.clone()));
	let _ = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr)
		.try_init();
}

fn build_wendelon_client(
	config: &ToronWendelonStorageConfig,
	client_uuid: [u8; 16],
) -> Result<wendelon_client::Client, ToronDaemonError> {
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
	> = HashMap::new();
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
