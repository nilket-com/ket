use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::distributed::identity::WorkerId;
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::TableId;
use crate::storage::TenantId;

pub const NON_LOOPBACK_BIND_DIAGNOSTIC: &str =
	"toron worker listener requires authenticated transport for non-loopback bind address";
pub const NON_LOOPBACK_SQL_BIND_DIAGNOSTIC: &str =
	"toron sql listener requires authenticated transport for non-loopback bind address";
pub const NON_LOOPBACK_STATIC_WORKER_DIAGNOSTIC: &str =
	"toron coordinator static workers require authenticated transport for non-loopback endpoints";

const DEFAULT_DRAIN_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_POOL_MAX_RETAINED: usize = 64;
const DEFAULT_POOL_MAX_CAP_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_SQL_LISTENER_BIND: &str = "127.0.0.1:7742";
const DEFAULT_SQL_STATEMENT_TIMEOUT_SECONDS: u64 = 60;

const EXAMPLE_CONFIG: &str = r#"[node]
worker_id = "00000000-0000-0000-0000-000000000042"
tenant_id = 1
drain_timeout_seconds = 30

[worker]
task_listener_bind = "127.0.0.1:0"
memory_budget_bytes = 67108864
# advertised_endpoint = "127.0.0.1:0"

[storage.wendelon]
bootstrap_peers = [
  { node_id = 1, address = "127.0.0.1:7001" },
]
client_bind = "127.0.0.1:0"
# tls_cert_path = "certs/client.crt"
# tls_key_path = "certs/client.key"
# tls_ca_path = "certs/ca.crt"
# cluster_domain = "cluster.local"
pool_max_retained = 64
pool_max_cap_bytes = 8388608

[catalog]
# descriptor_bootstrap_path = "descriptors.json"

[sql]
transport = "quic"
listener_bind = "127.0.0.1:7742"
statement_timeout_seconds = 60

[coordinator]
include_self_worker = true
# [[coordinator.static_workers]]
# worker_id = "00000000-0000-0000-0000-000000000043"
# endpoint = "127.0.0.1:9101"

[observability]
level = "info"
format = "pretty"
"#;

#[derive(Debug, Error)]
pub enum ToronConfigError {
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),
	#[error("TOML parse error: {0}")]
	Toml(#[from] toml::de::Error),
	#[error("JSON parse error: {0}")]
	Json(#[from] serde_json::Error),
	#[error("{message}")]
	Invalid { message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronBootstrapConfig {
	pub node: ToronNodeConfig,
	pub worker: ToronWorkerConfig,
	pub storage: ToronStorageConfig,
	pub catalog: ToronCatalogConfig,
	pub sql: Option<SqlConfig>,
	pub coordinator: CoordinatorConfig,
	pub observability: ToronObservabilityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronNodeConfig {
	pub worker_id: WorkerId,
	pub tenant_id: TenantId,
	pub drain_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronWorkerConfig {
	pub task_listener_bind: SocketAddr,
	pub advertised_endpoint: Option<WorkerEndpoint>,
	pub memory_budget_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronStorageConfig {
	pub wendelon: ToronWendelonStorageConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronWendelonStorageConfig {
	pub bootstrap_peers: Vec<ToronWendelonBootstrapPeer>,
	pub client_bind: SocketAddr,
	pub tls: Option<WendelonTlsConfig>,
	pub cluster_domain: String,
	pub pool_max_retained: usize,
	pub pool_max_cap_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WendelonTlsConfig {
	pub cert_path: PathBuf,
	pub key_path: PathBuf,
	pub ca_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronWendelonBootstrapPeer {
	pub node_id: u32,
	pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronCatalogConfig {
	pub descriptor_bootstrap_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlConfig {
	pub transport: SqlTransport,
	pub listener_bind: SocketAddr,
	pub statement_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTransport {
	Quic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorConfig {
	pub include_self_worker: bool,
	pub static_workers: Vec<StaticWorkerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticWorkerConfig {
	pub worker_id: WorkerId,
	pub endpoint: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToronObservabilityConfig {
	pub level: String,
	pub format: ToronTracingFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronTracingFormat {
	Pretty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronDescriptorBootstrap {
	pub tables: Vec<ToronDescriptorBootstrapTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToronDescriptorBootstrapTable {
	pub table_id: u64,
	pub descriptor: wendelon_client::TableSchemaDescriptor,
}

#[derive(Debug, Deserialize)]
struct RawToronBootstrapConfig {
	#[serde(default)]
	node: RawToronNodeConfig,
	#[serde(default)]
	worker: RawToronWorkerConfig,
	storage: RawToronStorageConfig,
	#[serde(default)]
	catalog: RawToronCatalogConfig,
	sql: Option<RawSqlConfig>,
	#[serde(default)]
	coordinator: RawCoordinatorConfig,
	#[serde(default)]
	observability: RawToronObservabilityConfig,
}

#[derive(Debug, Deserialize)]
struct RawToronNodeConfig {
	worker_id: Option<String>,
	#[serde(default = "default_tenant_id")]
	tenant_id: u64,
	#[serde(default = "default_drain_timeout_seconds")]
	drain_timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct RawToronWorkerConfig {
	#[serde(default = "default_task_listener_bind")]
	task_listener_bind: String,
	advertised_endpoint: Option<String>,
	#[serde(default = "default_memory_budget_bytes")]
	memory_budget_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RawToronStorageConfig {
	wendelon: RawToronWendelonStorageConfig,
}

#[derive(Debug, Deserialize)]
struct RawToronWendelonStorageConfig {
	#[serde(default)]
	bootstrap_peers: Vec<RawToronWendelonBootstrapPeer>,
	#[serde(default = "default_wendelon_client_bind")]
	client_bind: String,
	tls_cert_path: Option<PathBuf>,
	tls_key_path: Option<PathBuf>,
	tls_ca_path: Option<PathBuf>,
	#[serde(default = "default_wendelon_cluster_domain")]
	cluster_domain: String,
	#[serde(default = "default_pool_max_retained")]
	pool_max_retained: usize,
	#[serde(default = "default_pool_max_cap_bytes")]
	pool_max_cap_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct RawToronWendelonBootstrapPeer {
	node_id: u32,
	address: String,
}

#[derive(Debug, Default, Deserialize)]
struct RawToronCatalogConfig {
	descriptor_bootstrap_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct RawSqlConfig {
	#[serde(default = "default_sql_transport")]
	transport: String,
	#[serde(default = "default_sql_listener_bind")]
	listener_bind: String,
	#[serde(default = "default_sql_statement_timeout_seconds")]
	statement_timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct RawCoordinatorConfig {
	#[serde(default = "default_include_self_worker")]
	include_self_worker: bool,
	#[serde(default)]
	static_workers: Vec<RawStaticWorkerConfig>,
}

#[derive(Debug, Deserialize)]
struct RawStaticWorkerConfig {
	worker_id: String,
	endpoint: String,
}

#[derive(Debug, Deserialize)]
struct RawToronObservabilityConfig {
	#[serde(default = "default_observability_level")]
	level: String,
	#[serde(default = "default_observability_format")]
	format: String,
}

impl Default for RawToronNodeConfig {
	fn default() -> Self {
		Self {
			worker_id: None,
			tenant_id: default_tenant_id(),
			drain_timeout_seconds: default_drain_timeout_seconds(),
		}
	}
}

impl Default for RawToronWorkerConfig {
	fn default() -> Self {
		Self {
			task_listener_bind: default_task_listener_bind(),
			advertised_endpoint: None,
			memory_budget_bytes: default_memory_budget_bytes(),
		}
	}
}

impl Default for RawToronObservabilityConfig {
	fn default() -> Self {
		Self {
			level: default_observability_level(),
			format: default_observability_format(),
		}
	}
}

impl Default for RawCoordinatorConfig {
	fn default() -> Self {
		Self {
			include_self_worker: default_include_self_worker(),
			static_workers: Vec::new(),
		}
	}
}

pub fn load_toron_bootstrap_config(
	path: impl AsRef<Path>,
) -> Result<ToronBootstrapConfig, ToronConfigError> {
	let path = path.as_ref();
	let raw: RawToronBootstrapConfig = toml::from_str(&std::fs::read_to_string(path)?)?;
	let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
	normalize_config(raw, base_dir)
}

pub fn render_example_config() -> String {
	EXAMPLE_CONFIG.to_string()
}

pub fn write_example_config(path: impl AsRef<Path>) -> Result<(), ToronConfigError> {
	let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
	file.write_all(EXAMPLE_CONFIG.as_bytes())?;
	file.flush()?;
	Ok(())
}

pub fn load_descriptor_bootstrap(
	path: impl AsRef<Path>,
) -> Result<Vec<(TableId, wendelon_client::TableSchemaDescriptor)>, ToronConfigError> {
	let bootstrap: ToronDescriptorBootstrap =
		serde_json::from_str(&std::fs::read_to_string(path)?)?;
	let mut seen = HashSet::new();
	let mut descriptors = Vec::with_capacity(bootstrap.tables.len());
	for table in bootstrap.tables {
		if table.table_id == 0 {
			return Err(invalid("descriptor bootstrap table_id 0 is reserved"));
		}
		if !seen.insert(table.table_id) {
			return Err(invalid(format!(
				"duplicate descriptor bootstrap table_id {}",
				table.table_id
			)));
		}
		descriptors.push((TableId(table.table_id), table.descriptor));
	}
	Ok(descriptors)
}

fn normalize_config(
	raw: RawToronBootstrapConfig,
	base_dir: &Path,
) -> Result<ToronBootstrapConfig, ToronConfigError> {
	let worker_id = match raw.node.worker_id {
		Some(value) => WorkerId::from_uuid(
			Uuid::parse_str(&value)
				.map_err(|error| invalid(format!("invalid worker_id: {error}")))?,
		),
		None => WorkerId::new(),
	};
	let drain_timeout = if raw.node.drain_timeout_seconds == 0 {
		return Err(invalid("drain_timeout_seconds must be greater than 0"));
	} else {
		Duration::from_secs(raw.node.drain_timeout_seconds)
	};
	let task_listener_bind = raw
		.worker
		.task_listener_bind
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid task_listener_bind: {error}")))?;
	if !task_listener_bind.ip().is_loopback() {
		return Err(invalid(NON_LOOPBACK_BIND_DIAGNOSTIC));
	}
	if raw.worker.memory_budget_bytes == 0 {
		return Err(invalid("memory_budget_bytes must be greater than 0"));
	}
	if raw.storage.wendelon.bootstrap_peers.is_empty() {
		return Err(invalid(
			"storage.wendelon.bootstrap_peers must contain at least one peer",
		));
	}
	let descriptor_bootstrap_path = raw.catalog.descriptor_bootstrap_path.map(|path| {
		if path.is_absolute() {
			path
		} else {
			base_dir.join(path)
		}
	});
	let sql = raw.sql.map(normalize_sql_config).transpose()?;
	let mut static_workers = Vec::with_capacity(raw.coordinator.static_workers.len());
	for worker in raw.coordinator.static_workers {
		let worker_id = WorkerId::from_uuid(
			Uuid::parse_str(&worker.worker_id)
				.map_err(|error| invalid(format!("invalid coordinator worker_id: {error}")))?,
		);
		let endpoint = worker.endpoint.parse::<SocketAddr>().map_err(|error| {
			invalid(format!(
				"invalid coordinator static worker endpoint: {error}"
			))
		})?;
		if !endpoint.ip().is_loopback() {
			return Err(invalid(NON_LOOPBACK_STATIC_WORKER_DIAGNOSTIC));
		}
		static_workers.push(StaticWorkerConfig {
			worker_id,
			endpoint,
		});
	}
	let wendelon_client_bind = normalize_wendelon_client_bind(&raw.storage.wendelon.client_bind)?;
	let wendelon_tls = normalize_wendelon_tls_config(
		raw.storage.wendelon.tls_cert_path,
		raw.storage.wendelon.tls_key_path,
		raw.storage.wendelon.tls_ca_path,
		base_dir,
	)?;
	let format = match raw.observability.format.as_str() {
		"pretty" => ToronTracingFormat::Pretty,
		other => {
			return Err(invalid(format!(
				"unsupported observability format: {other}"
			)));
		}
	};

	Ok(ToronBootstrapConfig {
		node: ToronNodeConfig {
			worker_id,
			tenant_id: TenantId(raw.node.tenant_id),
			drain_timeout,
		},
		worker: ToronWorkerConfig {
			task_listener_bind,
			advertised_endpoint: raw.worker.advertised_endpoint.map(WorkerEndpoint::new),
			memory_budget_bytes: raw.worker.memory_budget_bytes,
		},
		storage: ToronStorageConfig {
			wendelon: ToronWendelonStorageConfig {
				bootstrap_peers: raw
					.storage
					.wendelon
					.bootstrap_peers
					.into_iter()
					.map(|peer| ToronWendelonBootstrapPeer {
						node_id: peer.node_id,
						address: peer.address,
					})
					.collect(),
				client_bind: wendelon_client_bind,
				tls: wendelon_tls,
				cluster_domain: raw.storage.wendelon.cluster_domain,
				pool_max_retained: raw.storage.wendelon.pool_max_retained,
				pool_max_cap_bytes: raw.storage.wendelon.pool_max_cap_bytes,
			},
		},
		catalog: ToronCatalogConfig {
			descriptor_bootstrap_path,
		},
		sql,
		coordinator: CoordinatorConfig {
			include_self_worker: raw.coordinator.include_self_worker,
			static_workers,
		},
		observability: ToronObservabilityConfig {
			level: raw.observability.level,
			format,
		},
	})
}

fn normalize_sql_config(raw: RawSqlConfig) -> Result<SqlConfig, ToronConfigError> {
	let transport = match raw.transport.as_str() {
		"quic" => SqlTransport::Quic,
		other => return Err(invalid(format!("unsupported sql transport: {other}"))),
	};
	let listener_bind = raw
		.listener_bind
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid sql listener_bind: {error}")))?;
	if !listener_bind.ip().is_loopback() {
		return Err(invalid(NON_LOOPBACK_SQL_BIND_DIAGNOSTIC));
	}
	if raw.statement_timeout_seconds == 0 {
		return Err(invalid(
			"sql.statement_timeout_seconds must be greater than 0",
		));
	}
	Ok(SqlConfig {
		transport,
		listener_bind,
		statement_timeout: Duration::from_secs(raw.statement_timeout_seconds),
	})
}

fn normalize_wendelon_client_bind(raw: &str) -> Result<SocketAddr, ToronConfigError> {
	let addr = raw
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid storage.wendelon.client_bind: {error}")))?;
	if !addr.ip().is_loopback() {
		return Err(invalid(
			"storage.wendelon.client_bind must remain loopback until authenticated remote transport is configured",
		));
	}
	Ok(addr)
}

fn normalize_wendelon_tls_config(
	cert_path: Option<PathBuf>,
	key_path: Option<PathBuf>,
	ca_path: Option<PathBuf>,
	base_dir: &Path,
) -> Result<Option<WendelonTlsConfig>, ToronConfigError> {
	match (cert_path, key_path, ca_path) {
		(None, None, None) => Ok(None),
		(Some(cert_path), Some(key_path), Some(ca_path)) => Ok(Some(WendelonTlsConfig {
			cert_path: resolve_config_path(cert_path, base_dir),
			key_path: resolve_config_path(key_path, base_dir),
			ca_path: resolve_config_path(ca_path, base_dir),
		})),
		_ => Err(invalid(
			"storage.wendelon TLS config requires tls_cert_path, tls_key_path, and tls_ca_path together",
		)),
	}
}

fn resolve_config_path(path: PathBuf, base_dir: &Path) -> PathBuf {
	if path.is_absolute() {
		path
	} else {
		base_dir.join(path)
	}
}

fn invalid(message: impl Into<String>) -> ToronConfigError {
	ToronConfigError::Invalid {
		message: message.into(),
	}
}

fn default_tenant_id() -> u64 {
	1
}

fn default_drain_timeout_seconds() -> u64 {
	DEFAULT_DRAIN_TIMEOUT_SECONDS
}

fn default_task_listener_bind() -> String {
	"127.0.0.1:0".to_string()
}

fn default_memory_budget_bytes() -> u64 {
	DEFAULT_MEMORY_BUDGET_BYTES
}

fn default_pool_max_retained() -> usize {
	DEFAULT_POOL_MAX_RETAINED
}

fn default_pool_max_cap_bytes() -> usize {
	DEFAULT_POOL_MAX_CAP_BYTES
}

fn default_wendelon_client_bind() -> String {
	"127.0.0.1:0".to_string()
}

fn default_wendelon_cluster_domain() -> String {
	wendelon::network::tls::DEFAULT_CLUSTER_DOMAIN.to_string()
}

fn default_sql_transport() -> String {
	"quic".to_string()
}

fn default_sql_listener_bind() -> String {
	DEFAULT_SQL_LISTENER_BIND.to_string()
}

fn default_sql_statement_timeout_seconds() -> u64 {
	DEFAULT_SQL_STATEMENT_TIMEOUT_SECONDS
}

fn default_include_self_worker() -> bool {
	true
}

fn default_observability_level() -> String {
	"info".to_string()
}

fn default_observability_format() -> String {
	"pretty".to_string()
}
