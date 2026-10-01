use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use observability::telemetry as shared_telemetry;
#[cfg(feature = "telemetry")]
pub use observability::telemetry::{
	OTEL_PARENT_SPAN_ID_FIELD, OTEL_SAMPLED_FIELD, OTEL_SPAN_ID_FIELD, OTEL_TRACE_ID_FIELD,
};
#[cfg(not(feature = "telemetry"))]
pub const OTEL_TRACE_ID_FIELD: &str = "otel.trace_id";
#[cfg(not(feature = "telemetry"))]
pub const OTEL_SPAN_ID_FIELD: &str = "otel.span_id";
#[cfg(not(feature = "telemetry"))]
pub const OTEL_PARENT_SPAN_ID_FIELD: &str = "otel.parent_span_id";
#[cfg(not(feature = "telemetry"))]
pub const OTEL_SAMPLED_FIELD: &str = "otel.sampled";
#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use observability::telemetry::{
	TraceExportConfig as SharedTraceExportConfig, TraceExportProtocol as SharedTraceExportProtocol,
};
use tracing::{Dispatch, Span, field};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;

use crate::audit::StatementClass;
#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use crate::audit::parse_toron_audit_node_numeric;
#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use crate::config::AuditConfig;
use crate::config::{TelemetryConfig, TelemetryProtocol, ToronBootstrapConfig};
use crate::distributed::identity::{QueryId, TaskIdentity};
use crate::distributed::task::{TaskSpec, TaskTelemetry};
use crate::error::{QueryError, TaskFailureKind};
use crate::network::observability::MetricsRegistry;
use crate::storage::{ScanRequest, TraceContext};

pub const TORON_LOG: &str = "TORON_LOG";
pub const OTLP_SCOPE_NAME: &str = "toron.traces";
pub const TORON_DROPPED_SPAN_METRIC: &str = "toron_otlp_spans_dropped_total";

const DEFAULT_LOG_FILTER: &str = "info";
const TRACE_ID_DOMAIN: &[u8] = b"toron.trace.v1";
const QUERY_SPAN_DOMAIN: &[u8] = b"toron.query.span.v1";
const DISPATCH_SPAN_DOMAIN: &[u8] = b"toron.dispatch.span.v1";
const TASK_SPAN_DOMAIN: &[u8] = b"toron.task.span.v1";
const STORAGE_SCAN_SPAN_DOMAIN: &[u8] = b"toron.storage.scan.span.v1";
const TRACE_SAMPLING_DENOMINATOR: u64 = 1_000_000;

static TELEMETRY_ENABLED: AtomicBool = AtomicBool::new(false);
static SAMPLING_RATE_PER_MILLION: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryTraceContext {
	trace_id: [u8; 16],
	query_span_id: [u8; 8],
	sampled: bool,
}

impl QueryTraceContext {
	pub const fn trace_context(self) -> Option<TraceContext> {
		if self.sampled {
			Some(TraceContext {
				trace_id: self.trace_id,
				span_id: self.query_span_id,
			})
		} else {
			None
		}
	}
}

pub struct TelemetryHandle {
	#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
	shutdown: Option<shared_telemetry::OtlpShutdownHandle>,
	export_timeout: std::time::Duration,
}

impl TelemetryHandle {
	pub fn export_timeout(&self) -> std::time::Duration {
		self.export_timeout
	}

	pub async fn shutdown(self) -> std::io::Result<()> {
		TELEMETRY_ENABLED.store(false, Ordering::Relaxed);
		SAMPLING_RATE_PER_MILLION.store(0, Ordering::Relaxed);
		#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
		if let Some(handle) = self.shutdown {
			return handle.shutdown().await;
		}
		Ok(())
	}
}

pub fn install_global(
	config: &ToronBootstrapConfig,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<TelemetryHandle> {
	let trace_cfg = config.observability.telemetry.as_ref();
	if trace_cfg.is_none() {
		let (dispatch, handle) = build_dispatch(config, metrics)?;
		let _ = tracing::dispatcher::set_global_default(dispatch);
		return Ok(handle);
	}
	let (dispatch, handle) = build_dispatch(config, metrics)?;
	tracing::dispatcher::set_global_default(dispatch).map_err(|err| {
		Error::new(
			ErrorKind::AlreadyExists,
			format!("global tracing subscriber already installed: {err}"),
		)
	})?;
	TELEMETRY_ENABLED.store(true, Ordering::Relaxed);
	SAMPLING_RATE_PER_MILLION.store(
		trace_cfg
			.map(|cfg| cfg.sampling_rate_per_million)
			.unwrap_or_default(),
		Ordering::Relaxed,
	);
	Ok(handle)
}

fn build_dispatch(
	config: &ToronBootstrapConfig,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<(Dispatch, TelemetryHandle)> {
	let filter = build_env_filter(&config.observability.level)?;
	match config.observability.telemetry.as_ref() {
		Some(cfg) => build_otlp_dispatch(config, cfg, metrics, filter),
		None => {
			let fmt_layer = tracing_subscriber::fmt::layer()
				.with_target(true)
				.with_writer(std::io::stderr);
			Ok((
				Dispatch::new(tracing_subscriber::registry().with(filter).with(fmt_layer)),
				TelemetryHandle {
					#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
					shutdown: None,
					export_timeout: std::time::Duration::from_secs(0),
				},
			))
		}
	}
}

fn build_env_filter(default_level: &str) -> std::io::Result<EnvFilter> {
	EnvFilter::try_from_env(TORON_LOG)
		.or_else(|_| EnvFilter::try_new(default_level))
		.or_else(|_| EnvFilter::try_new(DEFAULT_LOG_FILTER))
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid TORON_LOG filter: {err}"),
			)
		})
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
fn build_otlp_dispatch(
	config: &ToronBootstrapConfig,
	cfg: &TelemetryConfig,
	metrics: Arc<MetricsRegistry>,
	filter: EnvFilter,
) -> std::io::Result<(Dispatch, TelemetryHandle)> {
	let profile = toron_profile(config, metrics)?;
	let shared_cfg = shared_trace_config(cfg);
	let export_timeout = cfg.export_timeout;
	let (layer, shutdown) = shared_telemetry::OtlpTraceLayer::new(shared_cfg, profile)?;
	let fmt_layer = tracing_subscriber::fmt::layer()
		.with_target(true)
		.with_writer(std::io::stderr);
	let dispatch = Dispatch::new(
		tracing_subscriber::registry()
			.with(filter)
			.with(fmt_layer)
			.with(layer),
	);
	Ok((
		dispatch,
		TelemetryHandle {
			shutdown: Some(shutdown),
			export_timeout,
		},
	))
}

#[cfg(not(any(feature = "otlp-http", feature = "otlp-grpc")))]
fn build_otlp_dispatch(
	_config: &ToronBootstrapConfig,
	cfg: &TelemetryConfig,
	_metrics: Arc<MetricsRegistry>,
	_filter: EnvFilter,
) -> std::io::Result<(Dispatch, TelemetryHandle)> {
	let protocol = match cfg.protocol {
		TelemetryProtocol::HttpProtobuf => "http/protobuf",
		TelemetryProtocol::Grpc => "grpc",
	};
	Err(Error::new(
		ErrorKind::Unsupported,
		format!(
			"observability.telemetry.protocol={protocol} requires the matching toron otlp cargo feature"
		),
	))
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
fn shared_trace_config(cfg: &TelemetryConfig) -> SharedTraceExportConfig {
	SharedTraceExportConfig {
		endpoint: cfg.endpoint.clone(),
		protocol: match cfg.protocol {
			TelemetryProtocol::HttpProtobuf => SharedTraceExportProtocol::HttpProtobuf,
			TelemetryProtocol::Grpc => SharedTraceExportProtocol::Grpc,
		},
		sampling_rate_per_million: cfg.sampling_rate_per_million,
		client_cert_path: cfg.client_cert_path.clone(),
		client_key_path: cfg.client_key_path.clone(),
		ca_bundle_path: cfg.ca_bundle_path.clone(),
		max_queue_size: cfg.max_queue_size,
		export_timeout: cfg.export_timeout,
	}
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
fn toron_profile(
	config: &ToronBootstrapConfig,
	metrics: Arc<MetricsRegistry>,
) -> std::io::Result<shared_telemetry::TelemetryProfile> {
	let local_node_id = telemetry_node_id(config)?;
	Ok(shared_telemetry::TelemetryProfile::new(
		local_node_id,
		vec![
			shared_telemetry::TelemetryAttribute::string("service.name", "toron"),
			shared_telemetry::TelemetryAttribute::string(
				"service.version",
				env!("CARGO_PKG_VERSION"),
			),
			shared_telemetry::TelemetryAttribute::integer("toron.node_id", local_node_id as i64),
		],
		OTLP_SCOPE_NAME,
		env!("CARGO_PKG_VERSION"),
		Arc::new(ToronDroppedSpanObserver { metrics }),
		Arc::new(ToronTelemetryWarningHook),
	))
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
fn telemetry_node_id(config: &ToronBootstrapConfig) -> std::io::Result<u32> {
	if let AuditConfig::Enabled(audit) = &config.audit {
		let parsed = parse_toron_audit_node_numeric(&audit.node_id).map_err(|err| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid audit.node_id for telemetry: {err}"),
			)
		})?;
		if parsed != audit.node_numeric_id {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"audit.node_id and audit.node_numeric_id must match for telemetry",
			));
		}
		if parsed != 0 {
			return Ok(parsed);
		}
	}
	let bytes = *config.node.worker_id.get().as_bytes();
	let fallback = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
	Ok(fallback.max(1))
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
struct ToronDroppedSpanObserver {
	metrics: Arc<MetricsRegistry>,
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
impl shared_telemetry::DroppedSpanObserver for ToronDroppedSpanObserver {
	fn record_dropped_span(&self) -> u64 {
		self.metrics.record_dropped_otlp_span()
	}
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
struct ToronTelemetryWarningHook;

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
impl shared_telemetry::TelemetryWarningHook for ToronTelemetryWarningHook {
	fn warn(&self, warning: shared_telemetry::TelemetryWarning<'_>) {
		match warning {
			shared_telemetry::TelemetryWarning::QueueSaturated {
				queue_capacity,
				dropped_total,
			} => {
				tracing::warn!(
					queue_capacity,
					dropped_total,
					"toron otlp span export queue saturated; dropping sampled spans",
				);
			}
			shared_telemetry::TelemetryWarning::ExportTimedOut { error } => {
				eprintln!("toron: otlp export timed out: {error}");
			}
			shared_telemetry::TelemetryWarning::ExportFailed { error } => {
				eprintln!("toron: otlp export failed: {error}");
			}
		}
	}
}

pub fn enabled() -> bool {
	TELEMETRY_ENABLED.load(Ordering::Relaxed)
}

pub fn query_trace_context(query_id: QueryId) -> QueryTraceContext {
	let trace_id = trace_id_for_query(query_id);
	let sampled =
		enabled() && head_sample(trace_id, SAMPLING_RATE_PER_MILLION.load(Ordering::Relaxed));
	QueryTraceContext {
		trace_id,
		query_span_id: query_span_id(query_id),
		sampled,
	}
}

pub fn query_span(context: QueryTraceContext) -> Span {
	if !enabled() {
		return Span::none();
	}
	let trace_id = hex_trace_id(&context.trace_id);
	let span_id = hex_span_id(&context.query_span_id);
	tracing::info_span!(
		"toron.query",
		otel.trace_id = %trace_id,
		otel.span_id = %span_id,
		otel.sampled = context.sampled,
		toron.query.statement_class = field::Empty,
		toron.query.frontend = field::Empty,
		toron.query.outcome = field::Empty,
		toron.error_class = field::Empty,
	)
}

pub fn record_query_frontend(span: &Span, frontend: &'static str) {
	span.record("toron.query.frontend", frontend);
}

pub fn planning_span(parent: QueryTraceContext) -> Span {
	child_span(
		"toron.planning",
		parent.trace_id,
		planning_span_id(parent.trace_id),
		parent.query_span_id,
		parent.sampled,
	)
}

pub fn dispatch_span(spec: &TaskSpec) -> Span {
	let Some(parent) = spec.context.trace_context else {
		return unsampled_span(
			"toron.dispatch",
			spec.identity.query_id,
			dispatch_span_id(spec.identity),
		);
	};
	child_span(
		"toron.dispatch",
		parent.trace_id,
		dispatch_span_id(spec.identity),
		parent.span_id,
		true,
	)
}

pub fn dispatch_trace_context(spec: &TaskSpec) -> Option<TraceContext> {
	if !enabled() {
		return None;
	}
	spec.context.trace_context.map(|parent| TraceContext {
		trace_id: parent.trace_id,
		span_id: dispatch_span_id(spec.identity),
	})
}

pub fn task_span(spec: &TaskSpec) -> Span {
	let Some(parent) = spec.context.trace_context else {
		return unsampled_task_span(spec);
	};
	let trace_id = hex_trace_id(&parent.trace_id);
	let parent_span_id = hex_span_id(&parent.span_id);
	let span_id = hex_span_id(&task_span_id(spec.identity));
	tracing::info_span!(
		"toron.task",
		otel.trace_id = %trace_id,
		otel.parent_span_id = %parent_span_id,
		otel.span_id = %span_id,
		otel.sampled = true,
		toron.task.stage_role = field::Empty,
		toron.task.operator_kind = field::Empty,
		toron.task.strategy = field::Empty,
		toron.task.worker_count_bucket = field::Empty,
		toron.task.pulled_shuffle_bytes = field::Empty,
		toron.task.left_shuffle_bytes = field::Empty,
		toron.task.right_shuffle_bytes = field::Empty,
		toron.task.published_shuffle_bytes = field::Empty,
		toron.task.build_rows = field::Empty,
		toron.task.probe_rows = field::Empty,
		toron.task.matched_rows = field::Empty,
		toron.task.emitted_rows = field::Empty,
		toron.task.emitted_batches = field::Empty,
		toron.task.spill_bytes = field::Empty,
		toron.task.task_wall_micros = field::Empty,
		toron.task.shuffle_pull_decode_micros = field::Empty,
		toron.task.build_micros = field::Empty,
		toron.task.probe_micros = field::Empty,
		toron.task.spill_micros = field::Empty,
		toron.task.output_publish_micros = field::Empty,
		toron.task.task_setup_micros = field::Empty,
		toron.task.batch_encode_micros = field::Empty,
		toron.task.batch_send_micros = field::Empty,
		toron.task.batch_decode_micros = field::Empty,
		toron.task.dispatch_wait_micros = field::Empty,
		toron.task.telemetry_lock_micros = field::Empty,
		toron.task.framework_overhead_micros = field::Empty,
		toron.error_class = field::Empty,
	)
}

pub fn task_trace_context(spec: &TaskSpec) -> Option<TraceContext> {
	if !enabled() {
		return None;
	}
	spec.context.trace_context.map(|parent| TraceContext {
		trace_id: parent.trace_id,
		span_id: task_span_id(spec.identity),
	})
}

fn unsampled_task_span(spec: &TaskSpec) -> Span {
	if !enabled() {
		return Span::none();
	}
	let trace = trace_id_for_query(spec.identity.query_id);
	let trace_id = hex_trace_id(&trace);
	let span_id = hex_span_id(&task_span_id(spec.identity));
	tracing::info_span!(
		"toron.task",
		otel.trace_id = %trace_id,
		otel.span_id = %span_id,
		otel.sampled = false,
		toron.error_class = field::Empty,
	)
}

pub fn storage_scan_span(request: &ScanRequest) -> Span {
	let Some(parent) = request.trace_context else {
		return unsampled_span(
			"toron.storage.scan",
			request.query_id,
			storage_scan_span_id(request),
		);
	};
	child_span(
		"toron.storage.scan",
		parent.trace_id,
		storage_scan_span_id(request),
		parent.span_id,
		true,
	)
}

pub(crate) fn storage_scan_trace_context(request: &ScanRequest) -> Option<TraceContext> {
	request.trace_context.map(|parent| TraceContext {
		trace_id: parent.trace_id,
		span_id: storage_scan_span_id(request),
	})
}

fn child_span(
	name: &'static str,
	trace_id: [u8; 16],
	span_id: [u8; 8],
	parent_span_id: [u8; 8],
	sampled: bool,
) -> Span {
	if !enabled() {
		return Span::none();
	}
	let trace_id = hex_trace_id(&trace_id);
	let span_id = hex_span_id(&span_id);
	let parent_span_id = hex_span_id(&parent_span_id);
	match name {
		"toron.planning" => tracing::info_span!(
			"toron.planning",
			otel.trace_id = %trace_id,
			otel.parent_span_id = %parent_span_id,
			otel.span_id = %span_id,
			otel.sampled = sampled,
			toron.error_class = field::Empty,
		),
		"toron.dispatch" => tracing::info_span!(
			"toron.dispatch",
			otel.trace_id = %trace_id,
			otel.parent_span_id = %parent_span_id,
			otel.span_id = %span_id,
			otel.sampled = sampled,
			toron.error_class = field::Empty,
		),
		"toron.storage.scan" => tracing::info_span!(
			"toron.storage.scan",
			otel.trace_id = %trace_id,
			otel.parent_span_id = %parent_span_id,
			otel.span_id = %span_id,
			otel.sampled = sampled,
			toron.error_class = field::Empty,
		),
		_ => unreachable!("unknown telemetry child span name"),
	}
}

fn unsampled_span(name: &'static str, query_id: QueryId, span_id: [u8; 8]) -> Span {
	if !enabled() {
		return Span::none();
	}
	let trace = trace_id_for_query(query_id);
	let trace_id = hex_trace_id(&trace);
	let span_id = hex_span_id(&span_id);
	match name {
		"toron.dispatch" => tracing::info_span!(
			"toron.dispatch",
			otel.trace_id = %trace_id,
			otel.span_id = %span_id,
			otel.sampled = false,
			toron.error_class = field::Empty,
		),
		"toron.storage.scan" => tracing::info_span!(
			"toron.storage.scan",
			otel.trace_id = %trace_id,
			otel.span_id = %span_id,
			otel.sampled = false,
			toron.error_class = field::Empty,
		),
		_ => unreachable!("unknown telemetry unsampled span name"),
	}
}

pub fn record_query_statement_class(span: &Span, class: StatementClass) {
	span.record("toron.query.statement_class", statement_class_label(class));
}

pub fn record_query_outcome(span: &Span, outcome: &'static str) {
	span.record("toron.query.outcome", outcome);
}

pub fn record_query_error(span: &Span, error: &QueryError) {
	if !enabled() {
		return;
	}
	span.record("toron.error_class", query_error_class(error));
	span.in_scope(|| {
		tracing::error!(error_class = query_error_class(error), "toron query failed");
	});
}

pub fn record_task_failure(span: &Span, kind: &TaskFailureKind) {
	if !enabled() {
		return;
	}
	span.record("toron.error_class", task_failure_class(kind));
	span.in_scope(|| {
		tracing::error!(
			error_class = task_failure_class(kind),
			"toron task boundary failed"
		);
	});
}

pub fn record_task_telemetry(span: &Span, telemetry: &TaskTelemetry) {
	let labels = &telemetry.labels;
	span.record("toron.task.stage_role", labels.stage_role.as_str());
	span.record("toron.task.operator_kind", labels.operator_kind.as_str());
	if let Some(strategy) = labels.strategy.as_deref() {
		span.record("toron.task.strategy", strategy);
	}
	if let Some(worker_count) = labels.worker_count {
		span.record(
			"toron.task.worker_count_bucket",
			worker_count_bucket(worker_count),
		);
	}
	let counters = telemetry.counters;
	span.record(
		"toron.task.pulled_shuffle_bytes",
		counters.pulled_shuffle_bytes,
	);
	span.record("toron.task.left_shuffle_bytes", counters.left_shuffle_bytes);
	span.record(
		"toron.task.right_shuffle_bytes",
		counters.right_shuffle_bytes,
	);
	span.record(
		"toron.task.published_shuffle_bytes",
		counters.published_shuffle_bytes,
	);
	span.record("toron.task.build_rows", counters.build_rows);
	span.record("toron.task.probe_rows", counters.probe_rows);
	span.record("toron.task.matched_rows", counters.matched_rows);
	span.record("toron.task.emitted_rows", counters.emitted_rows);
	span.record("toron.task.emitted_batches", counters.emitted_batches);
	span.record("toron.task.spill_bytes", counters.spill_bytes);
	let phases = telemetry.phases;
	span.record("toron.task.task_wall_micros", phases.task_wall_micros);
	span.record(
		"toron.task.shuffle_pull_decode_micros",
		phases.shuffle_pull_decode_micros,
	);
	span.record("toron.task.build_micros", phases.build_micros);
	span.record("toron.task.probe_micros", phases.probe_micros);
	span.record("toron.task.spill_micros", phases.spill_micros);
	span.record(
		"toron.task.output_publish_micros",
		phases.output_publish_micros,
	);
	span.record("toron.task.task_setup_micros", phases.task_setup_micros);
	span.record("toron.task.batch_encode_micros", phases.batch_encode_micros);
	span.record("toron.task.batch_send_micros", phases.batch_send_micros);
	span.record("toron.task.batch_decode_micros", phases.batch_decode_micros);
	span.record(
		"toron.task.dispatch_wait_micros",
		phases.dispatch_wait_micros,
	);
	span.record(
		"toron.task.telemetry_lock_micros",
		phases.telemetry_lock_micros,
	);
	span.record(
		"toron.task.framework_overhead_micros",
		phases.framework_overhead_micros,
	);
}

fn statement_class_label(class: StatementClass) -> &'static str {
	match class {
		StatementClass::Select => "select",
		StatementClass::Insert => "insert",
		StatementClass::InsertSelect => "insert_select",
		StatementClass::Update => "update",
		StatementClass::Delete => "delete",
		StatementClass::Create => "create",
		StatementClass::CreateTableAsSelect => "create_table_as_select",
		StatementClass::Drop => "drop",
		StatementClass::Cancel => "cancel",
		StatementClass::Unknown => "unknown",
	}
}

fn query_error_class(error: &QueryError) -> &'static str {
	match error {
		QueryError::Fragment(_) => "fragment",
		QueryError::Frontend(_) => "frontend",
		QueryError::Binding(_) => "binding",
		QueryError::Catalog(_) => "catalog",
		QueryError::Storage(_) => "storage",
		QueryError::Execution(_) => "execution",
		QueryError::Runtime(_) => "runtime",
		QueryError::Wire(_) => "wire",
		QueryError::Spill(_) => "spill",
		QueryError::Io(_) => "io",
		QueryError::Arrow(_) => "arrow",
		QueryError::Remote { .. } => "remote",
		QueryError::UnexpectedMessage { .. } => "wire",
		QueryError::SchemaInstability => "schema_instability",
		QueryError::Internal { .. } => "internal",
	}
}

fn task_failure_class(kind: &TaskFailureKind) -> &'static str {
	match kind {
		TaskFailureKind::ProtocolMismatch => "protocol_mismatch",
		TaskFailureKind::DescriptorEvolved { .. } => "descriptor_evolved",
		TaskFailureKind::WorkerLost { .. } => "worker_lost",
		TaskFailureKind::TaskFailed { .. } => "task_failed",
		TaskFailureKind::BudgetExhausted { .. } => "budget_exhausted",
		TaskFailureKind::Cancelled => "cancelled",
		TaskFailureKind::QueryLeaseMissing => "query_lease_missing",
		TaskFailureKind::QueryLeaseExpired => "query_lease_expired",
		TaskFailureKind::Internal { .. } => "internal",
		TaskFailureKind::ProducerUnreachable { .. } => "producer_unreachable",
		TaskFailureKind::PeerRefused { .. } => "establishment_refused",
	}
}

fn worker_count_bucket(worker_count: u32) -> &'static str {
	match worker_count {
		0 => "0",
		1 => "1",
		2..=4 => "2_4",
		5..=8 => "5_8",
		9..=16 => "9_16",
		_ => "17_plus",
	}
}

pub fn trace_id_for_query(query_id: QueryId) -> [u8; 16] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TRACE_ID_DOMAIN);
	hasher.update(query_id.get().as_bytes());
	let mut trace_id = [0u8; 16];
	trace_id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
	nonzero_trace_id(trace_id)
}

pub fn head_sample(trace_id: [u8; 16], sampling_rate_per_million: u32) -> bool {
	match sampling_rate_per_million {
		0 => false,
		1_000_000 => true,
		rate => {
			let head = u64::from_be_bytes([
				trace_id[0],
				trace_id[1],
				trace_id[2],
				trace_id[3],
				trace_id[4],
				trace_id[5],
				trace_id[6],
				trace_id[7],
			]);
			head % TRACE_SAMPLING_DENOMINATOR < u64::from(rate)
		}
	}
}

fn query_span_id(query_id: QueryId) -> [u8; 8] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(QUERY_SPAN_DOMAIN);
	hasher.update(query_id.get().as_bytes());
	span_id_from_hash(hasher)
}

fn planning_span_id(trace_id: [u8; 16]) -> [u8; 8] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(b"toron.planning.span.v1");
	hasher.update(&trace_id);
	span_id_from_hash(hasher)
}

fn dispatch_span_id(identity: TaskIdentity) -> [u8; 8] {
	task_identity_span_id(DISPATCH_SPAN_DOMAIN, identity)
}

fn task_span_id(identity: TaskIdentity) -> [u8; 8] {
	task_identity_span_id(TASK_SPAN_DOMAIN, identity)
}

fn storage_scan_span_id(request: &ScanRequest) -> [u8; 8] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(STORAGE_SCAN_SPAN_DOMAIN);
	hasher.update(request.query_id.get().as_bytes());
	hasher.update(&request.table_id.0.to_be_bytes());
	if let Some((morsel, worker_count)) = request.morsel {
		hasher.update(&morsel.0.to_be_bytes());
		hasher.update(&worker_count.to_be_bytes());
	}
	span_id_from_hash(hasher)
}

fn task_identity_span_id(domain: &[u8], identity: TaskIdentity) -> [u8; 8] {
	let mut hasher = blake3::Hasher::new();
	hasher.update(domain);
	hasher.update(identity.query_id.get().as_bytes());
	hasher.update(&identity.stage_id.get().to_be_bytes());
	hasher.update(&identity.task_id.get().to_be_bytes());
	hasher.update(&identity.attempt_id.get().to_be_bytes());
	span_id_from_hash(hasher)
}

fn span_id_from_hash(hasher: blake3::Hasher) -> [u8; 8] {
	let mut span_id = [0u8; 8];
	span_id.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
	nonzero_span_id(span_id)
}

fn nonzero_trace_id(mut trace_id: [u8; 16]) -> [u8; 16] {
	if trace_id.iter().all(|byte| *byte == 0) {
		trace_id[15] = 1;
	}
	trace_id
}

fn nonzero_span_id(mut span_id: [u8; 8]) -> [u8; 8] {
	if span_id.iter().all(|byte| *byte == 0) {
		span_id[7] = 1;
	}
	span_id
}

pub fn hex_trace_id(trace_id: &[u8; 16]) -> String {
	hex_bytes(trace_id)
}

pub fn hex_span_id(span_id: &[u8; 8]) -> String {
	hex_bytes(span_id)
}

fn hex_bytes(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		out.push(hex_digit(byte >> 4));
		out.push(hex_digit(byte & 0x0f));
	}
	out
}

fn hex_digit(nibble: u8) -> char {
	match nibble {
		0..=9 => char::from(b'0' + nibble),
		10..=15 => char::from(b'a' + nibble - 10),
		_ => unreachable!("nibble is masked to 4 bits"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn trace_id_for_query_is_derived_not_literal_uuid() {
		let query_id = QueryId::from_u128(0x00112233445566778899aabbccddeeff);
		let trace_id = trace_id_for_query(query_id);

		assert_ne!(trace_id, *query_id.get().as_bytes());
		assert!(trace_id.iter().any(|byte| *byte != 0));
		assert_eq!(trace_id, trace_id_for_query(query_id));
	}

	#[test]
	fn head_sample_uses_big_endian_head_against_per_million_rate() {
		let mut trace_id = [0u8; 16];
		trace_id[..8].copy_from_slice(&42u64.to_be_bytes());

		assert!(!head_sample(trace_id, 0));
		assert!(head_sample(trace_id, 1_000_000));
		assert!(head_sample(trace_id, 43));
		assert!(!head_sample(trace_id, 42));
	}
}
