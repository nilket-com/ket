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

const DEFAULT_DRAIN_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_POOL_MAX_RETAINED: usize = 64;
const DEFAULT_POOL_MAX_CAP_BYTES: usize = 8 * 1024 * 1024;

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
pool_max_retained = 64
pool_max_cap_bytes = 8388608

[catalog]
# descriptor_bootstrap_path = "descriptors.json"

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
	pub pool_max_retained: usize,
	pub pool_max_cap_bytes: usize,
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
				pool_max_retained: raw.storage.wendelon.pool_max_retained,
				pool_max_cap_bytes: raw.storage.wendelon.pool_max_cap_bytes,
			},
		},
		catalog: ToronCatalogConfig {
			descriptor_bootstrap_path,
		},
		observability: ToronObservabilityConfig {
			level: raw.observability.level,
			format,
		},
	})
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

fn default_observability_level() -> String {
	"info".to_string()
}

fn default_observability_format() -> String {
	"pretty".to_string()
}
