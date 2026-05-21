use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::distributed::budget::{
	AdmissionPolicy, ByteCount, DurationMillis, TenantBudget, TenantBudgetPolicy,
	derive_max_concurrent_descriptor_syncs, derive_max_concurrent_queries,
};
use crate::distributed::identity::WorkerId;
use crate::distributed::worker_registry::WorkerEndpoint;
use crate::ir::TableId;
use crate::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, reject_env_interpolation, validate_cluster_domain,
};
use crate::storage::TenantId;

pub const NON_LOOPBACK_BIND_DIAGNOSTIC: &str =
	"worker listener requires authenticated transport for non-loopback bind address";
pub const NON_LOOPBACK_SQL_BIND_DIAGNOSTIC: &str =
	"sql listener requires authenticated transport for non-loopback bind address";
pub const NON_LOOPBACK_STATIC_WORKER_DIAGNOSTIC: &str =
	"coordinator static workers require authenticated transport for non-loopback endpoints";
pub const OBSERVABILITY_LISTENERS_ALL_OR_NONE_DIAGNOSTIC: &str =
	"observability listeners require health, ready, and metrics bind addresses together";
pub const OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC: &str =
	"observability health listener must bind a loopback address";
pub const OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC: &str =
	"observability ready and metrics listeners require authenticated transport";
pub const LISTENER_BIND_COLLISION_DIAGNOSTIC: &str =
	"listener bind addresses must be pairwise distinct";

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

# [tenant_budgets.default]
# memory_per_task_bytes = 67108864
# aggregate_memory_bytes = 268435456
# max_concurrent_queries = 4
# max_concurrent_descriptor_syncs = 2
# admission_weight = 1
# max_queued_queries = 0
# max_borrowed_memory_bytes = 0
# max_borrowed_concurrent_queries = 0
# max_queries_per_second = 0
# query_burst_capacity = 0
# max_concurrent_connections = 0

# [[tenant_budgets.tenants]]
# tenant_id = 7
# memory_per_task_bytes = 16777216
# aggregate_memory_bytes = 67108864
# max_concurrent_queries = 4
# max_concurrent_descriptor_syncs = 2
# admission_weight = 1
# max_queued_queries = 0
# max_borrowed_memory_bytes = 0
# max_borrowed_concurrent_queries = 0
# max_queries_per_second = 0
# query_burst_capacity = 0
# max_concurrent_connections = 0

# [admission]
# max_queued_queries = 0
# max_queue_wait_ms = 0
# shared_memory_bytes = 0
# shared_concurrent_queries = 0

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
max_concurrent_connections = 0

[coordinator]
include_self_worker = true
# [[coordinator.static_workers]]
# worker_id = "00000000-0000-0000-0000-000000000043"
# endpoint = "127.0.0.1:9101"

# [tls]
# listener_cert = "certs/listener.pem"
# listener_key = "certs/listener.key"
# peer_ca = "certs/peer-ca.pem"
# cluster_domain = "cluster.local"

[observability]
level = "info"
format = "pretty"

# [observability.listeners]
# /health is plaintext and loopback-only.
# /ready and /metrics require [tls] and admit scraper client certs
# whose DNS SAN matches monitor-<principal>.<cluster_domain>.
# health = "127.0.0.1:7780"
# ready = "127.0.0.1:7781"
# metrics = "127.0.0.1:7782"
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
	pub tenant_budgets: TenantBudgetPolicy,
	pub admission: Option<AdmissionPolicy>,
	pub storage: ToronStorageConfig,
	pub catalog: ToronCatalogConfig,
	pub sql: Option<SqlConfig>,
	pub coordinator: CoordinatorConfig,
	pub tls: Option<TlsConfig>,
	pub observability: ObservabilityConfig,
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
	pub max_concurrent_connections: Option<NonZeroU32>,
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
pub struct TlsConfig {
	pub listener_cert: PathBuf,
	pub listener_key: PathBuf,
	pub peer_ca: PathBuf,
	pub cluster_domain: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityConfig {
	pub level: String,
	pub format: TracingFormat,
	pub listeners: Option<ObservabilityListeners>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TracingFormat {
	Pretty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityListeners {
	pub health: SocketAddr,
	pub ready: SocketAddr,
	pub metrics: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptorBootstrap {
	pub tables: Vec<DescriptorBootstrapEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescriptorBootstrapEntry {
	pub tenant_id: u64,
	pub table_id: u64,
	pub descriptor: wendelon_client::TableSchemaDescriptor,
}

#[derive(Debug, Deserialize)]
struct RawDescriptorBootstrap {
	tables: Vec<RawDescriptorBootstrapEntry>,
}

#[derive(Debug, Deserialize)]
struct RawDescriptorBootstrapEntry {
	tenant_id: Option<u64>,
	table_id: Option<u64>,
	descriptor: wendelon_client::TableSchemaDescriptor,
}

#[derive(Debug, Deserialize)]
struct RawToronBootstrapConfig {
	#[serde(default)]
	node: RawToronNodeConfig,
	#[serde(default)]
	worker: RawToronWorkerConfig,
	tenant_budgets: Option<RawTenantBudgetPolicy>,
	admission: Option<RawAdmissionPolicy>,
	storage: RawToronStorageConfig,
	#[serde(default)]
	catalog: RawToronCatalogConfig,
	sql: Option<RawSqlConfig>,
	#[serde(default)]
	coordinator: RawCoordinatorConfig,
	tls: Option<RawTlsConfig>,
	#[serde(default)]
	observability: RawObservabilityConfig,
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
struct RawTenantBudgetPolicy {
	default: Option<RawTenantBudget>,
	#[serde(default)]
	tenants: Vec<RawTenantBudgetEntry>,
}

#[derive(Debug, Deserialize)]
struct RawTenantBudget {
	memory_per_task_bytes: u64,
	aggregate_memory_bytes: u64,
	max_concurrent_queries: Option<u32>,
	max_concurrent_descriptor_syncs: Option<u32>,
	admission_weight: Option<u32>,
	max_queued_queries: Option<u32>,
	max_borrowed_memory_bytes: Option<u64>,
	max_borrowed_concurrent_queries: Option<u32>,
	max_queries_per_second: Option<u32>,
	query_burst_capacity: Option<u32>,
	max_concurrent_connections: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RawTenantBudgetEntry {
	tenant_id: u64,
	memory_per_task_bytes: u64,
	aggregate_memory_bytes: u64,
	max_concurrent_queries: Option<u32>,
	max_concurrent_descriptor_syncs: Option<u32>,
	admission_weight: Option<u32>,
	max_queued_queries: Option<u32>,
	max_borrowed_memory_bytes: Option<u64>,
	max_borrowed_concurrent_queries: Option<u32>,
	max_queries_per_second: Option<u32>,
	query_burst_capacity: Option<u32>,
	max_concurrent_connections: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct RawAdmissionPolicy {
	#[serde(default)]
	max_queued_queries: u32,
	#[serde(default)]
	max_queue_wait_ms: u32,
	#[serde(default)]
	shared_memory_bytes: u64,
	#[serde(default)]
	shared_concurrent_queries: u32,
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
	max_concurrent_connections: Option<u32>,
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
struct RawTlsConfig {
	listener_cert: PathBuf,
	listener_key: PathBuf,
	peer_ca: PathBuf,
	#[serde(default = "default_tls_cluster_domain")]
	cluster_domain: String,
}

#[derive(Debug, Deserialize)]
struct RawObservabilityConfig {
	#[serde(default = "default_observability_level")]
	level: String,
	#[serde(default = "default_observability_format")]
	format: String,
	listeners: Option<RawObservabilityListeners>,
}

#[derive(Debug, Deserialize)]
struct RawObservabilityListeners {
	health: Option<String>,
	ready: Option<String>,
	metrics: Option<String>,
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

impl Default for RawObservabilityConfig {
	fn default() -> Self {
		Self {
			level: default_observability_level(),
			format: default_observability_format(),
			listeners: None,
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
) -> Result<Vec<(TenantId, TableId, wendelon_client::TableSchemaDescriptor)>, ToronConfigError> {
	let bootstrap: RawDescriptorBootstrap = serde_json::from_str(&std::fs::read_to_string(path)?)?;
	let mut descriptors = Vec::with_capacity(bootstrap.tables.len());
	for table in bootstrap.tables {
		let tenant_id = table
			.tenant_id
			.ok_or_else(|| invalid("descriptor bootstrap tenant_id is required"))?;
		if tenant_id == 0 {
			return Err(invalid("descriptor bootstrap tenant_id 0 is reserved"));
		}
		let table_id = table
			.table_id
			.ok_or_else(|| invalid("descriptor bootstrap table_id is required"))?;
		if table_id == 0 {
			return Err(invalid("descriptor bootstrap table_id 0 is reserved"));
		}
		descriptors.push((TenantId(tenant_id), TableId(table_id), table.descriptor));
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
	let tls = raw
		.tls
		.map(|tls| normalize_tls_config(tls, base_dir))
		.transpose()?;
	let task_listener_bind = raw
		.worker
		.task_listener_bind
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid task_listener_bind: {error}")))?;
	if !task_listener_bind.ip().is_loopback() && tls.is_none() {
		return Err(invalid(NON_LOOPBACK_BIND_DIAGNOSTIC));
	}
	if raw.worker.memory_budget_bytes == 0 {
		return Err(invalid("memory_budget_bytes must be greater than 0"));
	}
	let tenant_budgets = normalize_tenant_budget_policy(
		raw.tenant_budgets,
		ByteCount::new(raw.worker.memory_budget_bytes),
	)?;
	let admission = raw.admission.map(normalize_admission_policy);
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
	let sql = raw
		.sql
		.map(|sql| normalize_sql_config(sql, tls.is_some()))
		.transpose()?;
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
		if !endpoint.ip().is_loopback() && tls.is_none() {
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
	let observability = normalize_observability_config(raw.observability, tls.is_some())?;
	let listener_addresses = listener_bind_addresses(
		task_listener_bind,
		sql.as_ref(),
		observability.listeners.as_ref(),
	);
	validate_pairwise_distinct_listeners(&listener_addresses)?;

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
		tenant_budgets,
		admission,
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
		tls,
		observability,
	})
}

fn normalize_tenant_budget_policy(
	raw: Option<RawTenantBudgetPolicy>,
	worker_memory_budget: ByteCount,
) -> Result<TenantBudgetPolicy, ToronConfigError> {
	let Some(raw) = raw else {
		return Ok(TenantBudgetPolicy::omitted_config(worker_memory_budget));
	};
	let default = raw
		.default
		.ok_or_else(|| invalid("tenant_budgets.default is required"))?;
	let default = normalize_tenant_budget(
		default.memory_per_task_bytes,
		default.aggregate_memory_bytes,
		default.max_concurrent_queries,
		default.max_concurrent_descriptor_syncs,
		default.admission_weight,
		default.max_queued_queries,
		default.max_borrowed_memory_bytes,
		default.max_borrowed_concurrent_queries,
		default.max_queries_per_second,
		default.query_burst_capacity,
		default.max_concurrent_connections,
	)?;
	let mut per_tenant = HashMap::new();
	for tenant in raw.tenants {
		if tenant.tenant_id == 0 {
			return Err(invalid("tenant_budgets tenant_id must be greater than 0"));
		}
		let tenant_id = TenantId(tenant.tenant_id);
		let budget = normalize_tenant_budget(
			tenant.memory_per_task_bytes,
			tenant.aggregate_memory_bytes,
			tenant.max_concurrent_queries,
			tenant.max_concurrent_descriptor_syncs,
			tenant.admission_weight,
			tenant.max_queued_queries,
			tenant.max_borrowed_memory_bytes,
			tenant.max_borrowed_concurrent_queries,
			tenant.max_queries_per_second,
			tenant.query_burst_capacity,
			tenant.max_concurrent_connections,
		)?;
		if per_tenant.insert(tenant_id, budget).is_some() {
			return Err(invalid(format!(
				"duplicate tenant_budgets entry for tenant_id {}",
				tenant.tenant_id
			)));
		}
	}
	Ok(TenantBudgetPolicy::new(default, per_tenant))
}

#[allow(clippy::too_many_arguments)]
fn normalize_tenant_budget(
	memory_per_task_bytes: u64,
	aggregate_memory_bytes: u64,
	max_concurrent_queries: Option<u32>,
	max_concurrent_descriptor_syncs: Option<u32>,
	admission_weight: Option<u32>,
	max_queued_queries: Option<u32>,
	max_borrowed_memory_bytes: Option<u64>,
	max_borrowed_concurrent_queries: Option<u32>,
	max_queries_per_second: Option<u32>,
	query_burst_capacity: Option<u32>,
	max_concurrent_connections: Option<u32>,
) -> Result<TenantBudget, ToronConfigError> {
	if memory_per_task_bytes == 0 {
		return Err(invalid(
			"tenant_budgets memory_per_task_bytes must be greater than 0",
		));
	}
	if aggregate_memory_bytes == 0 {
		return Err(invalid(
			"tenant_budgets aggregate_memory_bytes must be greater than 0",
		));
	}
	let memory_per_task = ByteCount::new(memory_per_task_bytes);
	let aggregate_memory = ByteCount::new(aggregate_memory_bytes);
	if memory_per_task > aggregate_memory {
		return Err(invalid(
			"tenant_budgets memory_per_task_bytes must be less than or equal to aggregate_memory_bytes",
		));
	}
	let max_concurrent_queries = match max_concurrent_queries {
		Some(0) => {
			return Err(invalid(
				"tenant_budgets max_concurrent_queries must be greater than 0",
			));
		}
		Some(value) => NonZeroU32::new(value).expect("zero rejected"),
		None => derive_max_concurrent_queries(memory_per_task, aggregate_memory),
	};
	let max_concurrent_descriptor_syncs = match max_concurrent_descriptor_syncs {
		Some(0) => {
			return Err(invalid(
				"tenant_budgets max_concurrent_descriptor_syncs must be greater than 0",
			));
		}
		Some(value) => NonZeroU32::new(value).expect("zero rejected"),
		None => derive_max_concurrent_descriptor_syncs(max_concurrent_queries),
	};
	let max_concurrent_connections = max_concurrent_connections.and_then(NonZeroU32::new);
	let admission_weight = match admission_weight {
		Some(0) => {
			return Err(invalid(
				"tenant_budgets admission_weight must be greater than 0",
			));
		}
		Some(value) => NonZeroU32::new(value).expect("zero rejected"),
		None => NonZeroU32::new(1).expect("one is nonzero"),
	};
	Ok(TenantBudget::with_admission_limits(
		memory_per_task,
		aggregate_memory,
		max_concurrent_queries,
		max_concurrent_descriptor_syncs,
		max_concurrent_connections,
		admission_weight,
		max_queued_queries.unwrap_or(0),
		ByteCount::new(max_borrowed_memory_bytes.unwrap_or(0)),
		max_borrowed_concurrent_queries.unwrap_or(0),
		max_queries_per_second.unwrap_or(0),
		query_burst_capacity.unwrap_or(0),
	))
}

fn normalize_admission_policy(raw: RawAdmissionPolicy) -> AdmissionPolicy {
	AdmissionPolicy {
		max_queued_queries: raw.max_queued_queries,
		max_queue_wait_ms: DurationMillis::new(raw.max_queue_wait_ms),
		shared_memory: ByteCount::new(raw.shared_memory_bytes),
		shared_concurrent_queries: raw.shared_concurrent_queries,
	}
}

fn listener_bind_addresses(
	task_listener_bind: SocketAddr,
	sql: Option<&SqlConfig>,
	observability: Option<&ObservabilityListeners>,
) -> Vec<SocketAddr> {
	let mut addresses = vec![task_listener_bind];
	if let Some(sql) = sql {
		addresses.push(sql.listener_bind);
	}
	if let Some(observability) = observability {
		addresses.extend([
			observability.health,
			observability.ready,
			observability.metrics,
		]);
	}
	addresses
}

fn validate_pairwise_distinct_listeners(addresses: &[SocketAddr]) -> Result<(), ToronConfigError> {
	let mut seen = HashSet::new();
	for address in addresses {
		if address.port() == 0 {
			continue;
		}
		if !seen.insert(*address) {
			return Err(invalid(LISTENER_BIND_COLLISION_DIAGNOSTIC));
		}
	}
	Ok(())
}

fn normalize_observability_config(
	raw: RawObservabilityConfig,
	tls_present: bool,
) -> Result<ObservabilityConfig, ToronConfigError> {
	let format = match raw.format.as_str() {
		"pretty" => TracingFormat::Pretty,
		other => {
			return Err(invalid(format!(
				"unsupported observability format: {other}"
			)));
		}
	};
	let listeners = raw
		.listeners
		.map(|listeners| normalize_observability_listeners(listeners, tls_present))
		.transpose()?;
	Ok(ObservabilityConfig {
		level: raw.level,
		format,
		listeners,
	})
}

fn normalize_observability_listeners(
	raw: RawObservabilityListeners,
	tls_present: bool,
) -> Result<ObservabilityListeners, ToronConfigError> {
	let (Some(health), Some(ready), Some(metrics)) = (raw.health, raw.ready, raw.metrics) else {
		return Err(invalid(OBSERVABILITY_LISTENERS_ALL_OR_NONE_DIAGNOSTIC));
	};
	let health = health.parse::<SocketAddr>().map_err(|error| {
		invalid(format!(
			"invalid observability health bind address: {error}"
		))
	})?;
	let ready = ready
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid observability ready bind address: {error}")))?;
	let metrics = metrics.parse::<SocketAddr>().map_err(|error| {
		invalid(format!(
			"invalid observability metrics bind address: {error}"
		))
	})?;
	if !health.ip().is_loopback() {
		return Err(invalid(OBSERVABILITY_HEALTH_LOOPBACK_DIAGNOSTIC));
	}
	if !tls_present {
		return Err(invalid(OBSERVABILITY_REQUIRES_TLS_DIAGNOSTIC));
	}
	Ok(ObservabilityListeners {
		health,
		ready,
		metrics,
	})
}

fn normalize_sql_config(
	raw: RawSqlConfig,
	tls_present: bool,
) -> Result<SqlConfig, ToronConfigError> {
	let transport = match raw.transport.as_str() {
		"quic" => SqlTransport::Quic,
		other => return Err(invalid(format!("unsupported sql transport: {other}"))),
	};
	let listener_bind = raw
		.listener_bind
		.parse::<SocketAddr>()
		.map_err(|error| invalid(format!("invalid sql listener_bind: {error}")))?;
	if !listener_bind.ip().is_loopback() && !tls_present {
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
		max_concurrent_connections: raw.max_concurrent_connections.and_then(NonZeroU32::new),
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

fn normalize_tls_config(raw: RawTlsConfig, base_dir: &Path) -> Result<TlsConfig, ToronConfigError> {
	validate_cluster_domain(&raw.cluster_domain).map_err(|error| invalid(error.to_string()))?;
	for path in [&raw.listener_cert, &raw.listener_key, &raw.peer_ca] {
		reject_env_interpolation(path).map_err(|error| invalid(error.to_string()))?;
	}
	let listener_cert = resolve_config_path(raw.listener_cert, base_dir);
	let listener_key = resolve_config_path(raw.listener_key, base_dir);
	let peer_ca = resolve_config_path(raw.peer_ca, base_dir);
	for (label, path) in [
		("tls cert file not found or unreadable", &listener_cert),
		(
			"tls listener_key file not found or unreadable",
			&listener_key,
		),
		("tls peer_ca file not found or unreadable", &peer_ca),
	] {
		if std::fs::File::open(path).is_err() {
			return Err(invalid(format!("{label}: {}", path.display())));
		}
	}
	Ok(TlsConfig {
		listener_cert,
		listener_key,
		peer_ca,
		cluster_domain: raw.cluster_domain,
	})
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

fn default_tls_cluster_domain() -> String {
	DEFAULT_CLUSTER_DOMAIN.to_string()
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
