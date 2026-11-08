use crate::audit::{
	ArchiveAuditConfig, AuditArchiveLockMode, AuditSinksConfig,
	DEFAULT_AUDIT_ARCHIVE_MAX_SEGMENT_BYTES, KafkaAuditConfig, KafkaAuditPayloadMode,
	SyslogAuditConfig,
};
use crate::crypto::{
	CryptoCacheConfig, DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES, DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES,
	RootKekProviderConfig, SLAB_MAX_BYTES_HARD_CEILING,
};
use crate::metrics::telemetry::{
	TRACE_SAMPLING_RATE_DENOMINATOR, TraceExportConfig, TraceExportProtocol,
};
use crate::network::{
	ServerAdmissionConfig, audit_root_verifying_key_bytes, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use crate::node::{
	ClusterBootstrapConfig, DEFAULT_DATA_RING_LEASE_TTL_MILLIS, DEFAULT_MVCC_RETENTION_WINDOW,
	NodeBootstrapConfig, ObservabilityConfig, SeedNodeConfig, TransportTlsConfig,
};
use crate::schema::ProtocolVersionWindow;
use crate::storage::{
	EncodedAggregateKind, EncodingCascadeKind, EncodingComputeConfig, EncodingComputeEvaluatorKind,
	EncodingWriterConfig, L2ChunkLayoutConfig, L2ColumnFetchConfig, L2EncodingFetchConfig,
	L2RowGroupFetchConfig, MvccRetentionPolicy, RowGroupSizingProfile, RowGroupWriterConfig,
};
use rand::TryRng;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const EXAMPLE_CONFIG_TEMPLATE: &[u8] = include_bytes!("../examples/wendelon.toml.example");
const EXAMPLE_NODE_ID: u32 = 1;
const EXAMPLE_ADVERTISE_ADDRESS: &str = "127.0.0.1:7001";
const EXAMPLE_DATA_DIR: &str = "data";

/// Load a `wendelon.toml` file and translate it into the
/// bootstrap shape consumed by [`crate::node::WendelonNode`].
pub fn load_node_bootstrap_config(path: impl AsRef<Path>) -> std::io::Result<NodeBootstrapConfig> {
	let path = path.as_ref();
	let text = std::fs::read_to_string(path)?;
	let raw: RawConfig = toml::from_str(&text).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to parse {}: {err}", path.display()),
		)
	})?;
	raw.into_bootstrap_config(path)
}

/// Render a concrete starter `wendelon.toml` from the checked-in
/// template. Each rendering provisions fresh OS-random signing keys and keeps
/// deterministic dev keys disabled. The generated file is valid for a single-node dev
/// bootstrap and can be edited in place by operators afterward.
pub fn render_example_config() -> String {
	let template = std::str::from_utf8(EXAMPLE_CONFIG_TEMPLATE)
		.expect("example config template must be valid UTF-8");
	// Fail closed on entropy failure; never fall back to public deterministic keys.
	let mut secrets = [[0u8; 32]; 3];
	for secret in &mut secrets {
		rand::rngs::SysRng
			.try_fill_bytes(secret)
			.expect("OS randomness for example signing secrets");
	}
	let [lease_secret, transfer_secret, audit_secret] = secrets;

	template
		.replace("{{NODE_ID}}", &EXAMPLE_NODE_ID.to_string())
		.replace("{{ADVERTISE_ADDRESS}}", EXAMPLE_ADVERTISE_ADDRESS)
		.replace("{{DATA_DIR}}", EXAMPLE_DATA_DIR)
		.replace("{{SLAB_MAX_BYTES}}", &default_slab_max_bytes().to_string())
		.replace(
			"{{REPLICATION_FACTOR}}",
			&default_replication_factor().to_string(),
		)
		.replace("{{WAL_IO_DEPTH}}", &default_io_depth().to_string())
		.replace("{{SLAB_IO_DEPTH}}", &default_io_depth().to_string())
		.replace(
			"{{CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS}}",
			&crate::storage::DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS.to_string(),
		)
		.replace(
			"{{DATA_RING_LEASE_TTL_MILLIS}}",
			&DEFAULT_DATA_RING_LEASE_TTL_MILLIS.to_string(),
		)
		.replace("{{LEASE_SIGNING_SECRET}}", &hex_string(lease_secret))
		.replace(
			"{{LEASE_VERIFYING_KEY}}",
			&hex_string(lease_verifying_key_bytes(lease_secret)),
		)
		.replace("{{TRANSFER_SIGNING_SECRET}}", &hex_string(transfer_secret))
		.replace(
			"{{TRANSFER_VERIFYING_KEY}}",
			&hex_string(transfer_verifying_key_bytes(transfer_secret)),
		)
		.replace("{{AUDIT_ROOT_SIGNING_SECRET}}", &hex_string(audit_secret))
		.replace(
			"{{AUDIT_ROOT_VERIFYING_KEY}}",
			&hex_string(audit_root_verifying_key_bytes(audit_secret)),
		)
}

/// Render the ordinary role configuration used by Nilket's local deployment
/// profile. Smart defaults live in the launcher artifact, not in
/// [`NodeBootstrapConfig::default`] (TORON-532).
pub fn render_local_profile_config(data_dir: &Path, pki: &tls_pki::DevelopmentPki) -> String {
	let mut config = render_example_config()
		.replace(
		"data_dir = \"data\"",
		&format!("data_dir = {}", toml_path(data_dir)),
		)
		.replace(
			"cdc_locality_heartbeat_interval_millis = 1000",
			"# Local convenience trades CDC frontier latency for quiet idle operation.\ncdc_locality_heartbeat_interval_millis = 30000",
		)
		.replace(
			"data_ring_lease_ttl_millis = 3000",
			"# Local single-node convenience trades distinct-successor failover latency for quiet idle operation.\ndata_ring_lease_ttl_millis = 90000",
		);
	config.push_str(&format!(
		r#"

# Generated development-only identity for `nilket local` (TORON-532).
[transport.tls]
cert_path = {node_cert}
key_path = {node_key}
node_ca_path = {node_ca}
operator_ca_path = {operator_ca}
cluster_domain = "cluster.local"

[observability.listeners]
health = "127.0.0.1:7431"
ready = "127.0.0.1:7432"
metrics = "127.0.0.1:7433"
"#,
		node_cert = toml_path(&pki.wendelon_node_cert),
		node_key = toml_path(&pki.wendelon_node_key),
		node_ca = toml_path(&pki.node_ca_cert),
		operator_ca = toml_path(&pki.operator_ca_cert),
	));
	config
}

fn toml_path(path: &Path) -> String {
	toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

/// Write a concrete starter config to `path`. Refuses to clobber an
/// existing file.
pub fn write_example_config(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref();
	let mut options = OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		options.mode(0o600);
	}
	let mut file = options.open(path)?;
	file.write_all(render_example_config().as_bytes())?;
	file.flush()?;
	Ok(())
}

#[derive(Debug, Deserialize)]
struct RawConfig {
	node: RawNodeConfig,
	storage: RawStorageConfig,
	identity: RawIdentityConfig,
	#[serde(default)]
	tuning: RawTuningConfig,
	#[serde(default)]
	time: RawTimeConfig,
	#[serde(default)]
	counter: RawCounterConfig,
	#[serde(default)]
	cluster: RawClusterConfig,
	#[serde(default)]
	crypto: RawCryptoConfig,
	#[serde(default)]
	transport: RawTransportConfig,
	#[serde(default)]
	observability: RawObservabilityConfig,
	#[serde(default)]
	audit: RawAuditConfig,
}

#[derive(Debug, Deserialize)]
struct RawNodeConfig {
	node_id: u32,
	advertise_address: String,
}

#[derive(Debug, Deserialize)]
struct RawStorageConfig {
	data_dir: PathBuf,
	#[serde(default = "default_slab_max_bytes")]
	slab_max_bytes: u64,
	#[serde(default = "default_tier_history_budget_bytes")]
	tier_history_budget_bytes: u64,
	#[serde(default)]
	lsm_compaction_paused: bool,
	#[serde(default)]
	control_retention: Option<String>,
	#[serde(default)]
	mvcc_retention_paused: bool,
	#[serde(default)]
	l2_column_fetch: RawL2ColumnFetchConfig,
	#[serde(default)]
	row_group_writer: RawRowGroupWriterConfig,
	#[serde(default)]
	l2_row_group_fetch: RawL2RowGroupFetchConfig,
	#[serde(default)]
	encoding_writer: RawEncodingWriterConfig,
	#[serde(default)]
	l2_chunk_layout: RawL2ChunkLayoutConfig,
	#[serde(default)]
	l2_encoding_fetch: RawL2EncodingFetchConfig,
	#[serde(default)]
	encoding_compute: RawEncodingComputeConfig,
}

#[derive(Debug, Default, Deserialize)]
struct RawL2ColumnFetchConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	ipc_metadata_initial_window_bytes: Option<u64>,
	#[serde(default)]
	max_ipc_metadata_probe_bytes: Option<u64>,
	#[serde(default)]
	min_record_bytes: Option<u64>,
	#[serde(default)]
	max_selected_fraction_micros: Option<u32>,
	#[serde(default)]
	max_ranges_per_record: Option<u32>,
	#[serde(default)]
	max_parallel_range_gets: Option<u32>,
}

impl RawL2ColumnFetchConfig {
	fn into_runtime_config(self) -> std::io::Result<L2ColumnFetchConfig> {
		let mut cfg = L2ColumnFetchConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(bytes) = self.ipc_metadata_initial_window_bytes {
			cfg.ipc_metadata_initial_window_bytes =
				crate::schema::RecordBodyByteLen::try_new(bytes)?;
		}
		if let Some(bytes) = self.max_ipc_metadata_probe_bytes {
			cfg.max_ipc_metadata_probe_bytes = crate::schema::RecordBodyByteLen::try_new(bytes)?;
		}
		if let Some(bytes) = self.min_record_bytes {
			cfg.min_record_bytes = crate::schema::RecordBodyByteLen::try_new(bytes)?;
		}
		if let Some(fraction) = self.max_selected_fraction_micros {
			cfg.max_selected_fraction_micros = fraction;
		}
		if let Some(count) = self.max_ranges_per_record {
			cfg.max_ranges_per_record = crate::schema::EncryptedChunkCount::new(count);
		}
		if let Some(count) = self.max_parallel_range_gets {
			cfg.max_parallel_range_gets = crate::schema::EncryptedChunkCount::new(count);
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawRowGroupWriterConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	profile: Option<String>,
	#[serde(default)]
	target_row_group_rows: Option<u32>,
	#[serde(default)]
	max_row_group_rows: Option<u32>,
	#[serde(default)]
	min_paged_record_rows: Option<u64>,
	#[serde(default)]
	min_paged_record_bytes: Option<u64>,
}

impl RawRowGroupWriterConfig {
	fn into_runtime_config(self) -> std::io::Result<RowGroupWriterConfig> {
		let profile = match self.profile.as_deref() {
			None | Some("point_read") => RowGroupSizingProfile::PointRead,
			Some("scan") => RowGroupSizingProfile::Scan,
			Some(profile) => {
				return Err(Error::new(
					ErrorKind::InvalidInput,
					format!(
						"unknown storage.row_group_writer profile {profile:?}; expected \"point_read\" or \"scan\""
					),
				));
			}
		};
		let mut cfg = RowGroupWriterConfig::with_profile(profile);
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		let has_numeric_override = self.target_row_group_rows.is_some()
			|| self.max_row_group_rows.is_some()
			|| self.min_paged_record_rows.is_some()
			|| self.min_paged_record_bytes.is_some();
		match profile {
			RowGroupSizingProfile::PointRead if has_numeric_override => {
				cfg.apply_point_read_overrides(
					self.target_row_group_rows,
					self.max_row_group_rows,
					self.min_paged_record_rows,
					self.min_paged_record_bytes,
				)?;
			}
			RowGroupSizingProfile::Scan if has_numeric_override => {
				tracing::warn!(
					"storage.row_group_writer numeric sizing fields are PointRead-only and ignored when profile = \"scan\""
				);
			}
			RowGroupSizingProfile::PointRead | RowGroupSizingProfile::Scan => {}
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawL2RowGroupFetchConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	max_selected_row_group_fraction_micros: Option<u32>,
	#[serde(default)]
	max_selected_byte_fraction_micros: Option<u32>,
	#[serde(default)]
	max_ranges_per_record: Option<u32>,
	#[serde(default)]
	max_parallel_range_gets: Option<u32>,
}

impl RawL2RowGroupFetchConfig {
	fn into_runtime_config(self) -> std::io::Result<L2RowGroupFetchConfig> {
		let mut cfg = L2RowGroupFetchConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(fraction) = self.max_selected_row_group_fraction_micros {
			cfg.max_selected_row_group_fraction_micros = fraction;
		}
		if let Some(fraction) = self.max_selected_byte_fraction_micros {
			cfg.max_selected_byte_fraction_micros = fraction;
		}
		if let Some(count) = self.max_ranges_per_record {
			cfg.max_ranges_per_record = crate::schema::EncryptedChunkCount::new(count);
		}
		if let Some(count) = self.max_parallel_range_gets {
			cfg.max_parallel_range_gets = crate::schema::EncryptedChunkCount::new(count);
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawEncodingWriterConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	target_simd_page_values: Option<u32>,
	#[serde(default)]
	cascade_sample_rows: Option<u32>,
	#[serde(default)]
	min_buffer_bytes_for_encoding: Option<u64>,
	#[serde(default)]
	enabled_cascades: Option<Vec<String>>,
}

impl RawEncodingWriterConfig {
	fn into_runtime_config(self) -> std::io::Result<EncodingWriterConfig> {
		let mut cfg = EncodingWriterConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(values) = self.target_simd_page_values {
			cfg.target_simd_page_values = crate::schema::SimdPageValueCount::new(values);
		}
		if let Some(rows) = self.cascade_sample_rows {
			cfg.cascade_sample_rows = crate::schema::SampleRowCount::new(rows);
		}
		if let Some(bytes) = self.min_buffer_bytes_for_encoding {
			cfg.min_buffer_bytes_for_encoding = crate::schema::RecordBodyByteLen::try_new(bytes)?;
		}
		if let Some(cascades) = self.enabled_cascades {
			cfg.enabled_cascades.clear();
			for cascade in cascades {
				let Some(kind) = EncodingCascadeKind::from_label(cascade.as_str()) else {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!("unknown Encoding cascade label {cascade:?}"),
					));
				};
				cfg.enabled_cascades.insert(kind);
			}
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawL2ChunkLayoutConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	max_pad_inflation_micros: Option<u32>,
}

impl RawL2ChunkLayoutConfig {
	fn into_runtime_config(self) -> std::io::Result<L2ChunkLayoutConfig> {
		let mut cfg = L2ChunkLayoutConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(micros) = self.max_pad_inflation_micros {
			cfg.max_pad_inflation_micros = crate::schema::SelectivityFractionMicros::new(micros);
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawL2EncodingFetchConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	max_simd_pages_per_record: Option<u32>,
	#[serde(default)]
	max_parallel_materialize_tasks: Option<u32>,
}

impl RawL2EncodingFetchConfig {
	fn into_runtime_config(self) -> std::io::Result<L2EncodingFetchConfig> {
		let mut cfg = L2EncodingFetchConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(count) = self.max_simd_pages_per_record {
			cfg.max_simd_pages_per_record = crate::schema::EncodingPageCount::new(count);
		}
		if let Some(count) = self.max_parallel_materialize_tasks {
			cfg.max_parallel_materialize_tasks = crate::schema::TaskCount::new(count);
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Default, Deserialize)]
struct RawEncodingComputeConfig {
	#[serde(default)]
	enabled: Option<bool>,
	#[serde(default)]
	max_dict_match_set_size: Option<u32>,
	#[serde(default)]
	min_buffer_rows_for_encoded_eval: Option<u32>,
	#[serde(default)]
	max_parallel_kernel_tasks: Option<u32>,
	#[serde(default)]
	max_mask_bytes_per_record: Option<u64>,
	#[serde(default)]
	max_encoded_pages_per_record: Option<u32>,
	#[serde(default)]
	min_selectivity_for_encoded_eval_micros: Option<u32>,
	#[serde(default)]
	min_prefilter_rejection_micros: Option<u32>,
	#[serde(default)]
	enabled_predicate_evaluators: Option<Vec<String>>,
	#[serde(default)]
	enabled_aggregate_kernels: Option<Vec<String>>,
}

impl RawEncodingComputeConfig {
	fn into_runtime_config(self) -> std::io::Result<EncodingComputeConfig> {
		let mut cfg = EncodingComputeConfig::default();
		if let Some(enabled) = self.enabled {
			cfg.enabled = enabled;
		}
		if let Some(count) = self.max_dict_match_set_size {
			cfg.max_dict_match_set_size = crate::schema::DictMatchSetSize::new(count);
		}
		if let Some(rows) = self.min_buffer_rows_for_encoded_eval {
			cfg.min_buffer_rows_for_encoded_eval = crate::schema::BufferValueCount::new(rows);
		}
		if let Some(tasks) = self.max_parallel_kernel_tasks {
			cfg.max_parallel_kernel_tasks = crate::schema::TaskCount::new(tasks);
		}
		if let Some(bytes) = self.max_mask_bytes_per_record {
			cfg.max_mask_bytes_per_record = crate::schema::RecordBodyByteLen::try_new(bytes)?;
		}
		if let Some(pages) = self.max_encoded_pages_per_record {
			cfg.max_encoded_pages_per_record = crate::schema::EncodingPageCount::new(pages);
		}
		if let Some(selectivity) = self.min_selectivity_for_encoded_eval_micros {
			cfg.min_selectivity_for_encoded_eval_micros =
				crate::schema::SelectivityFractionMicros::new(selectivity);
		}
		if let Some(rejection) = self.min_prefilter_rejection_micros {
			cfg.min_prefilter_rejection_micros =
				crate::schema::SelectivityFractionMicros::new(rejection);
		}
		if let Some(evaluators) = self.enabled_predicate_evaluators {
			cfg.enabled_predicate_evaluators.clear();
			for evaluator in evaluators {
				let Some(kind) = EncodingComputeEvaluatorKind::from_label(evaluator.as_str())
				else {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!("unknown Encoding compute evaluator label {evaluator:?}"),
					));
				};
				cfg.enabled_predicate_evaluators.insert(kind);
			}
		}
		if let Some(kernels) = self.enabled_aggregate_kernels {
			cfg.enabled_aggregate_kernels.clear();
			for kernel in kernels {
				let Some(kind) = EncodedAggregateKind::from_label(kernel.as_str()) else {
					return Err(Error::new(
						ErrorKind::InvalidInput,
						format!("unknown encoded aggregate kernel label {kernel:?}"),
					));
				};
				cfg.enabled_aggregate_kernels.insert(kind);
			}
		}
		cfg.validate()?;
		Ok(cfg)
	}
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTuningConfig {
	#[serde(default = "default_replication_factor")]
	replication_factor: usize,
	#[serde(default)]
	wal_io_depth: Option<u32>,
	#[serde(default)]
	slab_io_depth: Option<u32>,
	storage_shutdown_timeout_seconds: Option<u64>,
	#[serde(default)]
	cdc_locality_heartbeat_interval_millis: Option<u64>,
	#[serde(default)]
	data_ring_lease_ttl_millis: Option<u64>,
	#[serde(default)]
	max_inbound_connections: Option<usize>,
	#[serde(default)]
	max_inbound_connections_per_ip: Option<usize>,
	#[serde(default)]
	max_inbound_streams_per_connection: Option<usize>,
	#[serde(default)]
	max_concurrent_stream_tasks: Option<usize>,
}

impl Default for RawTuningConfig {
	fn default() -> Self {
		Self {
			replication_factor: default_replication_factor(),
			wal_io_depth: None,
			slab_io_depth: None,
			storage_shutdown_timeout_seconds: None,
			cdc_locality_heartbeat_interval_millis: None,
			data_ring_lease_ttl_millis: None,
			max_inbound_connections: None,
			max_inbound_connections_per_ip: None,
			max_inbound_streams_per_connection: None,
			max_concurrent_stream_tasks: None,
		}
	}
}

#[derive(Debug, Deserialize)]
struct RawTimeConfig {
	#[serde(default = "default_drift_max_offset_ms")]
	drift_max_offset_ms: u64,
	#[serde(default = "default_mvcc_retention_window_secs")]
	mvcc_retention_window_secs: u64,
}

impl Default for RawTimeConfig {
	fn default() -> Self {
		Self {
			drift_max_offset_ms: default_drift_max_offset_ms(),
			mvcc_retention_window_secs: default_mvcc_retention_window_secs(),
		}
	}
}

/// The counter dedup horizon, in seconds. Absent values take the defaults; the
/// expiry defaults to the SDK incarnation lifetime plus the other two.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCounterConfig {
	#[serde(default)]
	attempt_horizon_secs: Option<u64>,
	#[serde(default)]
	clock_skew_secs: Option<u64>,
	#[serde(default)]
	expiry_secs: Option<u64>,
}

impl RawCounterConfig {
	fn into_retention(self) -> std::io::Result<crate::storage::counter::CounterRetentionConfig> {
		let defaults = crate::storage::counter::CounterRetentionConfig::default();
		let millis = |secs: Option<u64>, default_ms: u64| {
			secs.map_or(Ok(default_ms), |secs| {
				secs.checked_mul(1_000).ok_or_else(|| {
					Error::new(ErrorKind::InvalidInput, "counter retention value overflows")
				})
			})
		};
		let attempt_horizon_ms = millis(self.attempt_horizon_secs, defaults.attempt_horizon_ms)?;
		let clock_skew_ms = millis(self.clock_skew_secs, defaults.clock_skew_ms)?;
		let floor = crate::schema::COUNTER_INCARNATION_LIFETIME_MS
			.saturating_add(attempt_horizon_ms)
			.saturating_add(clock_skew_ms);
		let retention = crate::storage::counter::CounterRetentionConfig {
			attempt_horizon_ms,
			clock_skew_ms,
			expiry_ms: millis(self.expiry_secs, floor)?,
		};
		retention.validate()?;
		Ok(retention)
	}
}

#[derive(Debug, Deserialize)]
struct RawIdentityConfig {
	/// Explicit dev-only authorization; absence always refuses recognized dev keys.
	#[serde(default)]
	allow_dev_keys: bool,
	lease_signing_secret: String,
	trusted_lease_signers: BTreeMap<String, String>,
	transfer_signing_secret: String,
	trusted_transfer_signers: BTreeMap<String, String>,
	audit_root_signing_secret: String,
	trusted_audit_root_keys: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawClusterConfig {
	#[serde(default)]
	mode: RawClusterMode,
	#[serde(default)]
	seed_nodes: Vec<RawSeedNode>,
}

#[derive(Debug, Deserialize)]
struct RawCryptoConfig {
	#[serde(default)]
	root_kek_provider: RootKekProviderConfig,
	#[serde(default = "default_tenant_kek_cache_max_entries")]
	tenant_kek_cache_max_entries: usize,
	#[serde(default = "default_slab_dek_cache_max_entries")]
	slab_dek_cache_max_entries: usize,
}

impl Default for RawCryptoConfig {
	fn default() -> Self {
		Self {
			root_kek_provider: RootKekProviderConfig::default(),
			tenant_kek_cache_max_entries: default_tenant_kek_cache_max_entries(),
			slab_dek_cache_max_entries: default_slab_dek_cache_max_entries(),
		}
	}
}

impl Default for RawClusterConfig {
	fn default() -> Self {
		Self {
			mode: RawClusterMode::Bootstrap,
			seed_nodes: Vec::new(),
		}
	}
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawClusterMode {
	#[default]
	Bootstrap,
	Join,
}

#[derive(Debug, Deserialize)]
struct RawSeedNode {
	node_id: u32,
	address: String,
}

#[derive(Debug, Default, Deserialize)]
struct RawTransportConfig {
	#[serde(default)]
	tls: Option<RawTransportTlsConfig>,
}

#[derive(Debug, Deserialize)]
struct RawTransportTlsConfig {
	cert_path: PathBuf,
	key_path: PathBuf,
	node_ca_path: PathBuf,
	operator_ca_path: PathBuf,
	#[serde(default = "default_cluster_domain")]
	cluster_domain: String,
}

#[derive(Debug, Default, Deserialize)]
struct RawObservabilityConfig {
	#[serde(default)]
	listeners: Option<RawObservabilityListeners>,
	#[serde(default)]
	traces: Option<RawObservabilityTraceConfig>,
}

#[derive(Debug, Deserialize)]
struct RawObservabilityListeners {
	health: String,
	ready: String,
	metrics: String,
}

#[derive(Debug, Deserialize)]
struct RawObservabilityTraceConfig {
	endpoint: String,
	#[serde(default)]
	protocol: RawObservabilityTraceProtocol,
	#[serde(default = "default_trace_sampling_rate")]
	sampling_rate: f64,
	client_cert_path: PathBuf,
	client_key_path: PathBuf,
	ca_bundle_path: PathBuf,
	#[serde(default = "default_trace_max_queue_size")]
	max_queue_size: usize,
	#[serde(default = "default_trace_export_timeout_s")]
	export_timeout_s: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
enum RawObservabilityTraceProtocol {
	#[default]
	#[serde(rename = "http/protobuf")]
	HttpProtobuf,
	#[serde(rename = "grpc")]
	Grpc,
}

#[derive(Debug, Default, Deserialize)]
struct RawAuditConfig {
	#[serde(default)]
	archive: Option<RawAuditArchiveConfig>,
	#[serde(default)]
	syslog: Option<RawAuditSyslogConfig>,
	#[serde(default)]
	kafka: Option<RawAuditKafkaConfig>,
}

#[derive(Debug, Deserialize)]
struct RawAuditArchiveConfig {
	directory: PathBuf,
	#[serde(default = "default_audit_archive_max_segment_bytes")]
	max_segment_bytes: u64,
	#[serde(default)]
	lock_mode: RawAuditArchiveLockMode,
	#[serde(default)]
	retain_days: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawAuditArchiveLockMode {
	#[default]
	None,
	Governance,
	Compliance,
}

#[derive(Debug, Deserialize)]
struct RawAuditSyslogConfig {
	socket_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct RawAuditKafkaConfig {
	brokers: String,
	topic: String,
	#[serde(default)]
	payload_mode: RawKafkaAuditPayloadMode,
	client_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawKafkaAuditPayloadMode {
	#[default]
	Distribution,
	Canonical,
}

impl RawConfig {
	fn into_bootstrap_config(mut self, source_path: &Path) -> std::io::Result<NodeBootstrapConfig> {
		let data_dir = absolutize_from_config_dir(source_path, &self.storage.data_dir);
		if !data_dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"configured data_dir is not an existing directory: {}",
					data_dir.display()
				),
			));
		}
		let advertise_address = self.node.advertise_address.trim().to_string();
		if advertise_address.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"node.advertise_address must not be empty",
			));
		}
		if self.storage.slab_max_bytes == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"storage.slab_max_bytes must be greater than zero",
			));
		}
		if self.storage.slab_max_bytes > SLAB_MAX_BYTES_HARD_CEILING {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				format!(
					"storage.slab_max_bytes={} exceeds the hard ceiling of {} bytes",
					self.storage.slab_max_bytes, SLAB_MAX_BYTES_HARD_CEILING
				),
			));
		}
		if self.storage.tier_history_budget_bytes == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"storage.tier_history_budget_bytes must be greater than zero",
			));
		}
		if self.tuning.replication_factor == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.replication_factor must be greater than zero",
			));
		}
		let wal_io_depth = self.tuning.wal_io_depth.unwrap_or(default_io_depth());
		let slab_io_depth = self.tuning.slab_io_depth.unwrap_or(default_io_depth());
		let cdc_locality_heartbeat_interval_millis = self
			.tuning
			.cdc_locality_heartbeat_interval_millis
			.unwrap_or(crate::storage::DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS);
		let data_ring_lease_ttl_millis = self
			.tuning
			.data_ring_lease_ttl_millis
			.unwrap_or(u64::from(DEFAULT_DATA_RING_LEASE_TTL_MILLIS));
		if self.tuning.wal_io_depth == Some(0)
			|| (self.tuning.wal_io_depth.is_none() && wal_io_depth == 0)
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.wal_io_depth must be greater than zero",
			));
		}
		if self.tuning.slab_io_depth == Some(0)
			|| (self.tuning.slab_io_depth.is_none() && slab_io_depth == 0)
		{
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.slab_io_depth must be greater than zero",
			));
		}
		if cdc_locality_heartbeat_interval_millis == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.cdc_locality_heartbeat_interval_millis must be greater than zero",
			));
		}
		if !(3..=u64::from(u32::MAX)).contains(&data_ring_lease_ttl_millis) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.data_ring_lease_ttl_millis must be between 3 and 4294967295",
			));
		}
		let transport_admission = ServerAdmissionConfig {
			max_inbound_connections: self
				.tuning
				.max_inbound_connections
				.unwrap_or(default_max_inbound_connections()),
			max_inbound_connections_per_ip: self
				.tuning
				.max_inbound_connections_per_ip
				.unwrap_or(default_max_inbound_connections_per_ip()),
			max_inbound_streams_per_connection: self
				.tuning
				.max_inbound_streams_per_connection
				.unwrap_or(default_max_inbound_streams_per_connection()),
			max_concurrent_stream_tasks: self
				.tuning
				.max_concurrent_stream_tasks
				.unwrap_or(default_max_concurrent_stream_tasks()),
		};
		transport_admission.validate()?;

		let lease_signing_secret = parse_key_material(
			"identity.lease_signing_secret",
			&self.identity.lease_signing_secret,
		)?;
		let transfer_signing_secret = parse_key_material(
			"identity.transfer_signing_secret",
			&self.identity.transfer_signing_secret,
		)?;
		let trusted_lease_signers = parse_trusted_signers(
			"identity.trusted_lease_signers",
			&self.identity.trusted_lease_signers,
		)?;
		let trusted_transfer_signers = parse_trusted_signers(
			"identity.trusted_transfer_signers",
			&self.identity.trusted_transfer_signers,
		)?;
		let audit_root_signing_secret = parse_key_material(
			"identity.audit_root_signing_secret",
			&self.identity.audit_root_signing_secret,
		)?;
		let trusted_audit_root_keys =
			parse_trusted_audit_root_keys(&self.identity.trusted_audit_root_keys)?;
		let cluster = match self.cluster.mode {
			RawClusterMode::Bootstrap => ClusterBootstrapConfig::Bootstrap,
			RawClusterMode::Join => ClusterBootstrapConfig::Join {
				seeds: self
					.cluster
					.seed_nodes
					.into_iter()
					.map(|seed| SeedNodeConfig {
						node_id: seed.node_id,
						address: seed.address,
					})
					.collect(),
			},
		};
		let transport_tls = self.transport.tls.map(|tls| TransportTlsConfig {
			cert_path: absolutize_from_config_dir(source_path, &tls.cert_path),
			key_path: absolutize_from_config_dir(source_path, &tls.key_path),
			node_ca_path: absolutize_from_config_dir(source_path, &tls.node_ca_path),
			operator_ca_path: absolutize_from_config_dir(source_path, &tls.operator_ca_path),
			cluster_domain: tls.cluster_domain,
		});
		self.crypto
			.root_kek_provider
			.absolutize_from_config_dir(source_path);
		let observability = self.observability.into_runtime_config(source_path)?;
		let audit = AuditSinksConfig {
			archive: self.audit.archive.map(|archive| ArchiveAuditConfig {
				directory: absolutize_from_config_dir(source_path, &archive.directory),
				max_segment_bytes: archive.max_segment_bytes,
				lock_mode: match archive.lock_mode {
					RawAuditArchiveLockMode::None => AuditArchiveLockMode::None,
					RawAuditArchiveLockMode::Governance => AuditArchiveLockMode::Governance,
					RawAuditArchiveLockMode::Compliance => AuditArchiveLockMode::Compliance,
				},
				retain_days: archive.retain_days,
			}),
			syslog: self.audit.syslog.map(|syslog| SyslogAuditConfig {
				socket_path: absolutize_from_config_dir(source_path, &syslog.socket_path),
			}),
			kafka: self.audit.kafka.map(|kafka| KafkaAuditConfig {
				brokers: kafka.brokers,
				topic: kafka.topic,
				payload_mode: match kafka.payload_mode {
					RawKafkaAuditPayloadMode::Distribution => KafkaAuditPayloadMode::Distribution,
					RawKafkaAuditPayloadMode::Canonical => KafkaAuditPayloadMode::Canonical,
				},
				client_id: kafka.client_id,
			}),
		};

		let slab_max_bytes = self.storage.slab_max_bytes;
		let control_retention =
			crate::consensus::ControlRetention::parse(self.storage.control_retention.as_deref())
				.map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
		let l2_column_fetch = self.storage.l2_column_fetch.into_runtime_config()?;
		let row_group_writer = self.storage.row_group_writer.into_runtime_config()?;
		let l2_row_group_fetch = self.storage.l2_row_group_fetch.into_runtime_config()?;
		let encoding_writer = self.storage.encoding_writer.into_runtime_config()?;
		let l2_chunk_layout = self.storage.l2_chunk_layout.into_runtime_config()?;
		let l2_encoding_fetch = self.storage.l2_encoding_fetch.into_runtime_config()?;
		let encoding_compute = self.storage.encoding_compute.into_runtime_config()?;
		let cfg = NodeBootstrapConfig {
			#[cfg(test)]
			startup_probe: None,
			#[cfg(test)]
			open_decision_writes_for_test: false,
			#[cfg(test)]
			startup_fault: None,
			#[cfg(feature = "test-support")]
			slab_file_ops: None,
			#[cfg(feature = "test-support")]
			join_promotion_gate: None,
			node_id: self.node.node_id,
			data_dir,
			slab_max_bytes,
			group_commit: crate::storage::GroupCommitConfig::default(),
			tier_history_budget_bytes: self.storage.tier_history_budget_bytes,
			rf: self.tuning.replication_factor,
			wal_io_depth,
			slab_io_depth,
			storage_shutdown_timeout: std::time::Duration::from_secs(
				self.tuning.storage_shutdown_timeout_seconds.unwrap_or(30),
			),
			cdc_locality_heartbeat_interval: Duration::from_millis(
				cdc_locality_heartbeat_interval_millis,
			),
			data_ring_lease_ttl: Duration::from_millis(data_ring_lease_ttl_millis),
			advertise_address,
			lease_signing_secret,
			allow_dev_keys: self.identity.allow_dev_keys,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			audit_root_signing_secret,
			trusted_audit_root_keys,
			cluster,
			l2: None,
			tiering: crate::storage::SweeperConfig::default(),
			lsm_compaction_paused: self.storage.lsm_compaction_paused,
			control_retention,
			decision_mode: Default::default(),
			decision_signing_secret: None,
			trusted_decision_signers: BTreeMap::new(),
			mvcc_retention_paused: self.storage.mvcc_retention_paused,
			rehydration: crate::storage::manager::RehydrationPolicy::default(),
			l2_column_fetch,
			row_group_writer,
			l2_row_group_fetch,
			encoding_writer,
			l2_chunk_layout,
			l2_encoding_fetch,
			encoding_compute,
			root_kek_provider: self.crypto.root_kek_provider,
			crypto_cache: CryptoCacheConfig {
				tenant_kek_cache_max_entries: self.crypto.tenant_kek_cache_max_entries,
				slab_dek_cache_max_entries: self.crypto.slab_dek_cache_max_entries,
			},
			protocol: ProtocolVersionWindow::default(),
			transport_tls,
			transport_admission,
			drift_max_offset_ms: self.time.drift_max_offset_ms,
			mvcc_retention_policy: match self.time.mvcc_retention_window_secs {
				0 => MvccRetentionPolicy::Disabled,
				secs => MvccRetentionPolicy::FixedWindow(Duration::from_secs(secs)),
			},
			counter_retention: self.counter.into_retention()?,
			observability,
			audit,
		};
		cfg.validate()?;
		Ok(cfg)
	}
}

impl RawObservabilityConfig {
	fn into_runtime_config(
		self,
		source_path: &Path,
	) -> std::io::Result<Option<ObservabilityConfig>> {
		let listeners =
			self.listeners.map(
				|listeners| crate::metrics::observability::ListenerAddresses {
					health: listeners.health,
					ready: listeners.ready,
					metrics: listeners.metrics,
				},
			);
		let traces = match self.traces {
			Some(traces) => Some(TraceExportConfig {
				endpoint: traces.endpoint,
				protocol: match traces.protocol {
					RawObservabilityTraceProtocol::HttpProtobuf => {
						TraceExportProtocol::HttpProtobuf
					}
					RawObservabilityTraceProtocol::Grpc => TraceExportProtocol::Grpc,
				},
				sampling_rate_per_million: parse_trace_sampling_rate(
					"observability.traces.sampling_rate",
					traces.sampling_rate,
				)?,
				client_cert_path: absolutize_from_config_dir(source_path, &traces.client_cert_path),
				client_key_path: absolutize_from_config_dir(source_path, &traces.client_key_path),
				ca_bundle_path: absolutize_from_config_dir(source_path, &traces.ca_bundle_path),
				max_queue_size: traces.max_queue_size,
				export_timeout: Duration::from_secs(traces.export_timeout_s),
			}),
			None => None,
		};
		if listeners.is_none() && traces.is_none() {
			return Ok(None);
		}
		Ok(Some(ObservabilityConfig { listeners, traces }))
	}
}

fn default_tenant_kek_cache_max_entries() -> usize {
	DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES
}

fn default_slab_dek_cache_max_entries() -> usize {
	DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES
}

fn default_trace_sampling_rate() -> f64 {
	0.01
}

fn default_trace_max_queue_size() -> usize {
	8192
}

fn default_trace_export_timeout_s() -> u64 {
	30
}

fn parse_trace_sampling_rate(field: &str, value: f64) -> std::io::Result<u32> {
	if !value.is_finite() || !(0.0..=1.0).contains(&value) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{field} must be between 0.0 and 1.0 inclusive"),
		));
	}
	let scaled = (value * TRACE_SAMPLING_RATE_DENOMINATOR as f64).round();
	Ok(scaled.clamp(0.0, TRACE_SAMPLING_RATE_DENOMINATOR as f64) as u32)
}

fn absolutize_from_config_dir(config_path: &Path, value: &Path) -> PathBuf {
	if value.is_absolute() {
		value.to_path_buf()
	} else {
		config_path
			.parent()
			.unwrap_or_else(|| Path::new("."))
			.join(value)
	}
}

fn parse_trusted_signers(
	field: &str,
	raw: &BTreeMap<String, String>,
) -> std::io::Result<BTreeMap<u32, [u8; 32]>> {
	if raw.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{field} must not be empty"),
		));
	}
	let mut parsed = BTreeMap::new();
	for (node_id, key) in raw {
		let node_id: u32 = node_id.parse().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains non-u32 node id {node_id:?}: {err}"),
			)
		})?;
		let key_bytes = parse_key_material(&format!("{field}.{node_id}"), key)?;
		parsed.insert(node_id, key_bytes);
	}
	Ok(parsed)
}

// Audit identities are normalized numerically; aliases must never overwrite enrollment.
fn parse_trusted_audit_root_keys(
	raw: &BTreeMap<String, String>,
) -> std::io::Result<BTreeMap<u32, [u8; 32]>> {
	let mut parsed = BTreeMap::new();
	for (raw_node, key) in raw {
		let node = raw_node.parse::<u32>().map_err(|_| {
			Error::new(
				ErrorKind::InvalidInput,
				crate::audit::AuditTrustError::InvalidNode,
			)
		})?;
		if parsed.contains_key(&node) {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				crate::audit::AuditTrustError::DuplicateNode { node },
			));
		}
		parsed.insert(
			node,
			parse_key_material("identity.trusted_audit_root_keys", key)?,
		);
	}
	crate::audit::audit_root_trust(parsed.iter().map(|(&node, &key)| (node, key)))
		.map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;
	Ok(parsed)
}

fn parse_key_material(field: &str, raw: &str) -> std::io::Result<[u8; 32]> {
	let compact = raw.trim();
	let compact = compact.strip_prefix("0x").unwrap_or(compact);
	if compact.len() != 64 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{field} must be 64 hex characters, got {}", compact.len()),
		));
	}
	let mut out = [0u8; 32];
	for (idx, chunk) in compact.as_bytes().chunks_exact(2).enumerate() {
		let hex = std::str::from_utf8(chunk).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains invalid UTF-8 hex: {err}"),
			)
		})?;
		out[idx] = u8::from_str_radix(hex, 16).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains invalid hex: {err}"),
			)
		})?;
	}
	Ok(out)
}

fn hex_string(bytes: [u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to string cannot fail");
	}
	out
}

const fn default_tier_history_budget_bytes() -> u64 {
	crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES
}

const fn default_slab_max_bytes() -> u64 {
	64 * 1024 * 1024
}

const fn default_replication_factor() -> usize {
	1
}

const fn default_io_depth() -> u32 {
	64
}

fn default_max_inbound_connections() -> usize {
	ServerAdmissionConfig::default().max_inbound_connections
}

fn default_max_inbound_connections_per_ip() -> usize {
	ServerAdmissionConfig::default().max_inbound_connections_per_ip
}

fn default_max_inbound_streams_per_connection() -> usize {
	ServerAdmissionConfig::default().max_inbound_streams_per_connection
}

fn default_max_concurrent_stream_tasks() -> usize {
	ServerAdmissionConfig::default().max_concurrent_stream_tasks
}

fn default_cluster_domain() -> String {
	crate::network::DEFAULT_CLUSTER_DOMAIN.to_string()
}

const fn default_drift_max_offset_ms() -> u64 {
	crate::consensus::monitor::MAX_OFFSET_MS
}

const fn default_mvcc_retention_window_secs() -> u64 {
	DEFAULT_MVCC_RETENTION_WINDOW.as_secs()
}

const fn default_audit_archive_max_segment_bytes() -> u64 {
	DEFAULT_AUDIT_ARCHIVE_MAX_SEGMENT_BYTES
}
