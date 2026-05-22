use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use observability::audit as shared_audit;
pub use observability::audit::{
	ArchiveAuditConfig, AuditArchiveLockMode, AuditOutcome, AuditSinksConfig,
	DEFAULT_AUDIT_ARCHIVE_MAX_SEGMENT_BYTES, SyslogAuditConfig,
};
use serde_json::json;
use tokio::sync::mpsc;

use crate::config::AuditConfig;
use crate::distributed::identity::QueryId;
use crate::ir::{LogicalPlan, SessionId};
use crate::network::observability::ReadinessState;
use crate::sql_wire::SqlErrorCode;
use crate::storage::TenantId;

const AUDIT_SCHEMA_VERSION: u16 = 1;
const AUDIT_GENESIS_TAG: &[u8] = b"toron-audit-genesis-v1";
const AUDIT_CHAIN_ROOT_TAG: &[u8] = b"toron-audit-chain-root-v1";
const AUDIT_CHAIN_ROOT_OPERATION: &str = "toron.audit.chain_root";
const AUDIT_SINK_FAILURE_OPERATION: &str = "toron.audit.sink_failure";
const AUDIT_ROOT_SIGNATURE_TAG: &str = "ed25519";
const SYSLOG_APP_NAME: &str = "toron";
const SYSLOG_ENTERPRISE_ID: &str = "toron@47450";
const DEFAULT_KAFKA_CLIENT_ID: &str = "toron-audit";
const TORON_NODE_ID_PREFIX: &str = "toron-n-";

pub fn toron_audit_format() -> shared_audit::AuditFormat {
	shared_audit::AuditFormat {
		schema_version: AUDIT_SCHEMA_VERSION,
		genesis_tag: AUDIT_GENESIS_TAG,
		chain_root_tag: AUDIT_CHAIN_ROOT_TAG,
		root_signature_tag: AUDIT_ROOT_SIGNATURE_TAG,
		chain_root_operation: AUDIT_CHAIN_ROOT_OPERATION,
		sink_failure_operation: AUDIT_SINK_FAILURE_OPERATION,
		syslog_app_name: SYSLOG_APP_NAME,
		syslog_enterprise_id: SYSLOG_ENTERPRISE_ID,
		default_kafka_client_id: DEFAULT_KAFKA_CLIENT_ID,
		parse_node_numeric: parse_toron_audit_node_numeric,
	}
}

pub fn parse_toron_audit_node_numeric(raw: &str) -> std::io::Result<u32> {
	let Some(numeric) = raw.strip_prefix(TORON_NODE_ID_PREFIX) else {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"audit.node_id must use toron-n-<u32>",
		));
	};
	if numeric.is_empty() || numeric.chars().any(|ch| !ch.is_ascii_digit()) {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			"audit.node_id must use toron-n-<u32>",
		));
	}
	numeric
		.parse::<u32>()
		.map_err(|error| Error::new(ErrorKind::InvalidInput, error))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToronAuditNodeId {
	wire: String,
	numeric: u32,
}

impl ToronAuditNodeId {
	pub fn new(wire: &str, numeric: u32) -> std::io::Result<Self> {
		let parsed = parse_toron_audit_node_numeric(wire)?;
		if parsed != numeric {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"audit.node_id and audit.node_numeric_id must match",
			));
		}
		Ok(Self {
			wire: wire.to_string(),
			numeric,
		})
	}

	fn to_shared(&self) -> shared_audit::AuditNodeId {
		shared_audit::AuditNodeId::new(self.wire.clone(), self.numeric)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditOperation {
	LifecycleStartup,
	LifecycleShutdown,
	AuditSinkFailure,
	SqlConnectionLimitDenied,
	SqlTenantIdentityDenied,
	SqlRequestFrameTooLarge,
	SqlStatementTooLarge,
	SqlProtocolMismatch,
	SqlUnexpectedClientFrame,
	QueryCompleted,
	QueryPlannedError,
	QueryExecutionError,
	QueryCancelled,
	QueryTimedOut,
	QueryDisconnectAborted,
}

impl ToronAuditOperation {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::LifecycleStartup => "toron.lifecycle.startup",
			Self::LifecycleShutdown => "toron.lifecycle.shutdown",
			Self::AuditSinkFailure => AUDIT_SINK_FAILURE_OPERATION,
			Self::SqlConnectionLimitDenied => "toron.sql.edge.connection_limit_denied",
			Self::SqlTenantIdentityDenied => "toron.sql.edge.tenant_identity_denied",
			Self::SqlRequestFrameTooLarge => "toron.sql.edge.request_frame_too_large",
			Self::SqlStatementTooLarge => "toron.sql.edge.statement_too_large",
			Self::SqlProtocolMismatch => "toron.sql.edge.protocol_mismatch",
			Self::SqlUnexpectedClientFrame => "toron.sql.edge.unexpected_client_frame",
			Self::QueryCompleted => "toron.sql.query.completed",
			Self::QueryPlannedError => "toron.sql.query.planned_error",
			Self::QueryExecutionError => "toron.sql.query.execution_error",
			Self::QueryCancelled => "toron.sql.query.cancelled",
			Self::QueryTimedOut => "toron.sql.query.timed_out",
			Self::QueryDisconnectAborted => "toron.sql.query.disconnect_aborted",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditEventClass {
	Lifecycle,
	EdgeDenial,
	QueryOutcome,
	Audit,
}

impl ToronAuditEventClass {
	pub const ALL: [Self; Self::COUNT] = [
		Self::Lifecycle,
		Self::EdgeDenial,
		Self::QueryOutcome,
		Self::Audit,
	];
	pub const COUNT: usize = 4;

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Lifecycle => "lifecycle",
			Self::EdgeDenial => "edge_denial",
			Self::QueryOutcome => "query_outcome",
			Self::Audit => "audit",
		}
	}

	const fn index(self) -> usize {
		match self {
			Self::Lifecycle => 0,
			Self::EdgeDenial => 1,
			Self::QueryOutcome => 2,
			Self::Audit => 3,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditDropReason {
	QueueFull,
	EmitterClosed,
}

impl ToronAuditDropReason {
	pub const ALL: [Self; Self::COUNT] = [Self::QueueFull, Self::EmitterClosed];
	pub const COUNT: usize = 2;

	pub fn as_str(self) -> &'static str {
		match self {
			Self::QueueFull => "queue_full",
			Self::EmitterClosed => "emitter_closed",
		}
	}

	const fn index(self) -> usize {
		match self {
			Self::QueueFull => 0,
			Self::EmitterClosed => 1,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditOutputKind {
	Local,
	Archive,
	Syslog,
	Kafka,
}

impl ToronAuditOutputKind {
	pub const ALL: [Self; Self::COUNT] = [Self::Local, Self::Archive, Self::Syslog, Self::Kafka];
	pub const COUNT: usize = 4;

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Local => "local",
			Self::Archive => "archive",
			Self::Syslog => "syslog",
			Self::Kafka => "kafka",
		}
	}

	const fn index(self) -> usize {
		match self {
			Self::Local => 0,
			Self::Archive => 1,
			Self::Syslog => 2,
			Self::Kafka => 3,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditSecurityEventUnrecordedReason {
	QueueFull,
	EmitterClosed,
	SinkError,
}

impl ToronAuditSecurityEventUnrecordedReason {
	pub const ALL: [Self; Self::COUNT] = [Self::QueueFull, Self::EmitterClosed, Self::SinkError];
	pub const COUNT: usize = 3;

	pub fn as_str(self) -> &'static str {
		match self {
			Self::QueueFull => "queue_full",
			Self::EmitterClosed => "emitter_closed",
			Self::SinkError => "sink_error",
		}
	}

	const fn index(self) -> usize {
		match self {
			Self::QueueFull => 0,
			Self::EmitterClosed => 1,
			Self::SinkError => 2,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementClass {
	Select,
	Insert,
	InsertSelect,
	Update,
	Delete,
	Create,
	CreateTableAsSelect,
	Drop,
	Cancel,
	Unknown,
}

impl StatementClass {
	pub fn from_plan(plan: &LogicalPlan) -> Self {
		match plan {
			LogicalPlan::Insert { .. } => Self::Insert,
			LogicalPlan::InsertSelect { .. } => Self::InsertSelect,
			LogicalPlan::Update { .. } => Self::Update,
			LogicalPlan::Delete { .. } => Self::Delete,
			LogicalPlan::CreateTable { .. } => Self::Create,
			LogicalPlan::CreateTableAsSelect { .. } => Self::CreateTableAsSelect,
			LogicalPlan::DropTable { .. } => Self::Drop,
			LogicalPlan::Scan { .. }
			| LogicalPlan::ScanNamed { .. }
			| LogicalPlan::Alias { .. }
			| LogicalPlan::Filter { .. }
			| LogicalPlan::Project { .. }
			| LogicalPlan::Aggregate { .. }
			| LogicalPlan::Exchange { .. }
			| LogicalPlan::Sort { .. }
			| LogicalPlan::Join { .. } => Self::Select,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Select => "select",
			Self::Insert => "insert",
			Self::InsertSelect => "insert_select",
			Self::Update => "update",
			Self::Delete => "delete",
			Self::Create => "create",
			Self::CreateTableAsSelect => "create_table_as_select",
			Self::Drop => "drop",
			Self::Cancel => "cancel",
			Self::Unknown => "unknown",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationBucket {
	Lt10Ms,
	Lt100Ms,
	Lt1S,
	Lt10S,
	Gte10S,
}

impl DurationBucket {
	pub fn from_duration(duration: Duration) -> Self {
		if duration < Duration::from_millis(10) {
			Self::Lt10Ms
		} else if duration < Duration::from_millis(100) {
			Self::Lt100Ms
		} else if duration < Duration::from_secs(1) {
			Self::Lt1S
		} else if duration < Duration::from_secs(10) {
			Self::Lt10S
		} else {
			Self::Gte10S
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Lt10Ms => "lt_10ms",
			Self::Lt100Ms => "lt_100ms",
			Self::Lt1S => "lt_1s",
			Self::Lt10S => "lt_10s",
			Self::Gte10S => "gte_10s",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCountBucket {
	Zero,
	One,
	Small,
	Medium,
	Large,
}

impl RowCountBucket {
	pub fn from_row_count(row_count: u64) -> Self {
		match row_count {
			0 => Self::Zero,
			1 => Self::One,
			2..=100 => Self::Small,
			101..=10_000 => Self::Medium,
			_ => Self::Large,
		}
	}

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Zero => "zero",
			Self::One => "one",
			Self::Small => "small",
			Self::Medium => "medium",
			Self::Large => "large",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorCodeClass {
	SyntaxError,
	PlanningError,
	BindingError,
	CatalogError,
	StorageError,
	TenantMismatch,
	StatementTimeout,
	Cancelled,
	CancellationUnsupported,
	NoLiveWorker,
	ResourceExhausted,
	Internal,
}

impl SqlErrorCodeClass {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::SyntaxError => "syntax_error",
			Self::PlanningError => "planning_error",
			Self::BindingError => "binding_error",
			Self::CatalogError => "catalog_error",
			Self::StorageError => "storage_error",
			Self::TenantMismatch => "tenant_mismatch",
			Self::StatementTimeout => "statement_timeout",
			Self::Cancelled => "cancelled",
			Self::CancellationUnsupported => "cancellation_unsupported",
			Self::NoLiveWorker => "no_live_worker",
			Self::ResourceExhausted => "resource_exhausted",
			Self::Internal => "internal",
		}
	}
}

impl From<SqlErrorCode> for SqlErrorCodeClass {
	fn from(code: SqlErrorCode) -> Self {
		match code {
			SqlErrorCode::SyntaxError => Self::SyntaxError,
			SqlErrorCode::PlanningError => Self::PlanningError,
			SqlErrorCode::BindingError => Self::BindingError,
			SqlErrorCode::CatalogError => Self::CatalogError,
			SqlErrorCode::StorageError => Self::StorageError,
			SqlErrorCode::TenantMismatch => Self::TenantMismatch,
			SqlErrorCode::StatementTimeout => Self::StatementTimeout,
			SqlErrorCode::Cancelled => Self::Cancelled,
			SqlErrorCode::CancellationUnsupported => Self::CancellationUnsupported,
			SqlErrorCode::NoLiveWorker => Self::NoLiveWorker,
			SqlErrorCode::ResourceExhausted => Self::ResourceExhausted,
			SqlErrorCode::Internal => Self::Internal,
		}
	}
}

#[derive(Debug, Clone)]
pub struct ToronAuditEvent {
	operation: ToronAuditOperation,
	event_class: ToronAuditEventClass,
	security_relevant: bool,
	outcome: AuditOutcome,
	tenant_id: Option<TenantId>,
	session_id: Option<SessionId>,
	query_id: Option<QueryId>,
	statement_class: Option<StatementClass>,
	sql_error_code_class: Option<SqlErrorCodeClass>,
	duration_bucket: Option<DurationBucket>,
	row_count_bucket: Option<RowCountBucket>,
	message: &'static str,
}

impl ToronAuditEvent {
	pub fn lifecycle_startup() -> Self {
		Self::new(
			ToronAuditOperation::LifecycleStartup,
			ToronAuditEventClass::Lifecycle,
			AuditOutcome::Allowed,
			"toron startup",
		)
	}

	pub fn lifecycle_shutdown() -> Self {
		Self::new(
			ToronAuditOperation::LifecycleShutdown,
			ToronAuditEventClass::Lifecycle,
			AuditOutcome::Allowed,
			"toron shutdown",
		)
	}

	pub fn edge_denial(
		operation: ToronAuditOperation,
		tenant_id: Option<TenantId>,
		message: &'static str,
	) -> Self {
		Self::new(
			operation,
			ToronAuditEventClass::EdgeDenial,
			AuditOutcome::Denied,
			message,
		)
		.with_security_relevant()
		.with_tenant_option(tenant_id)
	}

	pub fn query_outcome(
		operation: ToronAuditOperation,
		tenant_id: TenantId,
		session_id: SessionId,
		query_id: QueryId,
		statement_class: StatementClass,
		outcome: AuditOutcome,
	) -> Self {
		Self::new(
			operation,
			ToronAuditEventClass::QueryOutcome,
			outcome,
			"sql query outcome",
		)
		.with_security_relevant()
		.with_tenant_option(Some(tenant_id))
		.with_session_id(session_id)
		.with_query_id(query_id)
		.with_statement_class(statement_class)
	}

	pub fn sink_failure() -> Self {
		Self::new(
			ToronAuditOperation::AuditSinkFailure,
			ToronAuditEventClass::Audit,
			AuditOutcome::Error,
			"audit sink failure",
		)
		.with_security_relevant()
	}

	fn new(
		operation: ToronAuditOperation,
		event_class: ToronAuditEventClass,
		outcome: AuditOutcome,
		message: &'static str,
	) -> Self {
		Self {
			operation,
			event_class,
			security_relevant: false,
			outcome,
			tenant_id: None,
			session_id: None,
			query_id: None,
			statement_class: None,
			sql_error_code_class: None,
			duration_bucket: None,
			row_count_bucket: None,
			message,
		}
	}

	pub fn with_sql_error_code(mut self, code: SqlErrorCode) -> Self {
		self.sql_error_code_class = Some(code.into());
		self
	}

	pub fn with_duration(mut self, duration: Duration) -> Self {
		self.duration_bucket = Some(DurationBucket::from_duration(duration));
		self
	}

	pub fn with_row_count(mut self, row_count: u64) -> Self {
		self.row_count_bucket = Some(RowCountBucket::from_row_count(row_count));
		self
	}

	fn with_security_relevant(mut self) -> Self {
		self.security_relevant = true;
		self
	}

	fn with_tenant_option(mut self, tenant_id: Option<TenantId>) -> Self {
		self.tenant_id = tenant_id;
		self
	}

	fn with_session_id(mut self, session_id: SessionId) -> Self {
		self.session_id = Some(session_id);
		self
	}

	fn with_query_id(mut self, query_id: QueryId) -> Self {
		self.query_id = Some(query_id);
		self
	}

	fn with_statement_class(mut self, statement_class: StatementClass) -> Self {
		self.statement_class = Some(statement_class);
		self
	}

	pub fn event_class(&self) -> ToronAuditEventClass {
		self.event_class
	}

	pub fn security_relevant(&self) -> bool {
		self.security_relevant
	}

	fn to_shared(&self, node_id: &ToronAuditNodeId) -> std::io::Result<shared_audit::AuditEvent> {
		let mut event = shared_audit::AuditEvent::new(
			shared_audit::AuditAuthority::NodeLocal,
			self.operation.as_str(),
			node_id.to_shared(),
			self.message,
		)
		.with_outcome(self.outcome)
		.with_schema_value(json!({
			"schema": self.event_class.as_str(),
			"version": AUDIT_SCHEMA_VERSION,
		}))
		.with_parameter("event_class", self.event_class.as_str());
		if let Some(tenant_id) = self.tenant_id {
			event = event.with_tenant(shared_audit::AuditTenantId(tenant_id.0));
		}
		if let Some(session_id) = self.session_id {
			event = event.with_parameter("session_id", session_id.0.to_string());
		}
		if let Some(query_id) = self.query_id {
			event = event.with_parameter("query_id", query_id.get().to_string());
		}
		if let Some(statement_class) = self.statement_class {
			event = event.with_parameter("statement_class", statement_class.as_str());
		}
		if let Some(code) = self.sql_error_code_class {
			event = event.with_parameter("sql_error_code_class", code.as_str());
		}
		if let Some(duration_bucket) = self.duration_bucket {
			event = event.with_parameter("duration_bucket", duration_bucket.as_str());
		}
		if let Some(row_count_bucket) = self.row_count_bucket {
			event = event.with_parameter("row_count_bucket", row_count_bucket.as_str());
		}
		Ok(event)
	}
}

pub struct ToronAuditMetrics {
	events: [AtomicU64; ToronAuditEventClass::COUNT],
	dropped: [[AtomicU64; ToronAuditDropReason::COUNT]; ToronAuditEventClass::COUNT],
	sink_errors: [AtomicU64; ToronAuditOutputKind::COUNT],
	security_unrecorded: [AtomicU64; ToronAuditSecurityEventUnrecordedReason::COUNT],
}

impl Default for ToronAuditMetrics {
	fn default() -> Self {
		Self {
			events: std::array::from_fn(|_| AtomicU64::new(0)),
			dropped: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
			sink_errors: std::array::from_fn(|_| AtomicU64::new(0)),
			security_unrecorded: std::array::from_fn(|_| AtomicU64::new(0)),
		}
	}
}

impl ToronAuditMetrics {
	pub fn record_event(&self, event_class: ToronAuditEventClass) {
		self.events[event_class.index()].fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_drop(&self, event_class: ToronAuditEventClass, reason: ToronAuditDropReason) {
		self.dropped[event_class.index()][reason.index()].fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_sink_error(&self, sink: ToronAuditOutputKind) {
		self.sink_errors[sink.index()].fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_security_unrecorded(&self, reason: ToronAuditSecurityEventUnrecordedReason) {
		self.security_unrecorded[reason.index()].fetch_add(1, Ordering::Relaxed);
	}

	pub fn event_count(&self, event_class: ToronAuditEventClass) -> u64 {
		self.events[event_class.index()].load(Ordering::Relaxed)
	}

	pub fn dropped_count(
		&self,
		event_class: ToronAuditEventClass,
		reason: ToronAuditDropReason,
	) -> u64 {
		self.dropped[event_class.index()][reason.index()].load(Ordering::Relaxed)
	}

	pub fn sink_error_count(&self, sink: ToronAuditOutputKind) -> u64 {
		self.sink_errors[sink.index()].load(Ordering::Relaxed)
	}

	pub fn security_unrecorded_count(
		&self,
		reason: ToronAuditSecurityEventUnrecordedReason,
	) -> u64 {
		self.security_unrecorded[reason.index()].load(Ordering::Relaxed)
	}
}

struct ToronAuditObserver {
	metrics: Arc<ToronAuditMetrics>,
}

impl shared_audit::AuditObserver for ToronAuditObserver {
	fn local_emit_failed(&self) {
		self.metrics.record_sink_error(ToronAuditOutputKind::Local);
	}

	fn archive_failed(&self) {
		self.metrics
			.record_sink_error(ToronAuditOutputKind::Archive);
	}

	fn syslog_failed(&self) {
		self.metrics.record_sink_error(ToronAuditOutputKind::Syslog);
	}

	fn kafka_failed(&self) {
		self.metrics.record_sink_error(ToronAuditOutputKind::Kafka);
	}

	fn projection_failure_queue_dropped(&self, sink: &str) {
		match sink {
			"archive" => self
				.metrics
				.record_sink_error(ToronAuditOutputKind::Archive),
			"syslog" => self.metrics.record_sink_error(ToronAuditOutputKind::Syslog),
			"kafka" => self.metrics.record_sink_error(ToronAuditOutputKind::Kafka),
			_ => {}
		}
	}
}

#[derive(Clone)]
pub enum ToronAuditEmitter {
	Disabled,
	Enabled {
		tx: mpsc::Sender<ToronAuditEvent>,
		metrics: Arc<ToronAuditMetrics>,
	},
	Unavailable {
		metrics: Arc<ToronAuditMetrics>,
	},
}

impl ToronAuditEmitter {
	pub async fn from_config(
		config: &AuditConfig,
		metrics: Option<Arc<ToronAuditMetrics>>,
		readiness: Arc<ReadinessState>,
	) -> Self {
		let AuditConfig::Enabled(config) = config else {
			return Self::Disabled;
		};
		let metrics = metrics.expect("audit metrics exist when audit is enabled");
		let node_id = match ToronAuditNodeId::new(&config.node_id, config.node_numeric_id) {
			Ok(node_id) => node_id,
			Err(_) => {
				metrics.record_sink_error(ToronAuditOutputKind::Local);
				readiness
					.set_degraded_reason(Some("audit_local_chain_unavailable".to_string()))
					.await;
				return Self::Unavailable { metrics };
			}
		};
		let observer = Arc::new(ToronAuditObserver {
			metrics: Arc::clone(&metrics),
		});
		let sink = match shared_audit::build_audit_sink(
			&config.local_path,
			&config.sinks,
			toron_audit_format(),
			observer,
		) {
			Ok(sink) => sink,
			Err(error) => {
				metrics.record_sink_error(ToronAuditOutputKind::Local);
				tracing::warn!(error = %error, "audit local chain unavailable");
				readiness
					.set_degraded_reason(Some("audit_local_chain_unavailable".to_string()))
					.await;
				return Self::Unavailable { metrics };
			}
		};
		let (tx, rx) = mpsc::channel(config.queue_capacity.get());
		tokio::spawn(run_audit_worker(
			rx,
			sink,
			node_id,
			Arc::clone(&metrics),
			readiness,
		));
		Self::Enabled { tx, metrics }
	}

	pub fn record(&self, event: ToronAuditEvent) {
		match self {
			Self::Disabled => {}
			Self::Unavailable { metrics } => {
				record_drop(metrics, &event, ToronAuditDropReason::EmitterClosed);
			}
			Self::Enabled { tx, metrics } => match tx.try_send(event) {
				Ok(()) => {}
				Err(mpsc::error::TrySendError::Full(event)) => {
					record_drop(metrics, &event, ToronAuditDropReason::QueueFull);
				}
				Err(mpsc::error::TrySendError::Closed(event)) => {
					record_drop(metrics, &event, ToronAuditDropReason::EmitterClosed);
				}
			},
		}
	}

	pub fn disabled() -> Self {
		Self::Disabled
	}
}

fn record_drop(metrics: &ToronAuditMetrics, event: &ToronAuditEvent, reason: ToronAuditDropReason) {
	metrics.record_drop(event.event_class(), reason);
	if event.security_relevant() {
		let reason = match reason {
			ToronAuditDropReason::QueueFull => ToronAuditSecurityEventUnrecordedReason::QueueFull,
			ToronAuditDropReason::EmitterClosed => {
				ToronAuditSecurityEventUnrecordedReason::EmitterClosed
			}
		};
		metrics.record_security_unrecorded(reason);
	}
}

async fn run_audit_worker(
	mut rx: mpsc::Receiver<ToronAuditEvent>,
	sink: Arc<dyn shared_audit::AuditSink>,
	node_id: ToronAuditNodeId,
	metrics: Arc<ToronAuditMetrics>,
	readiness: Arc<ReadinessState>,
) {
	while let Some(event) = rx.recv().await {
		let event_class = event.event_class();
		let security_relevant = event.security_relevant();
		let shared_event = match event.to_shared(&node_id) {
			Ok(shared_event) => shared_event,
			Err(error) => {
				metrics.record_sink_error(ToronAuditOutputKind::Local);
				if security_relevant {
					metrics.record_security_unrecorded(
						ToronAuditSecurityEventUnrecordedReason::SinkError,
					);
				}
				tracing::warn!(error = %error, "audit event serialization failed");
				continue;
			}
		};
		match sink.emit(shared_event) {
			Ok(()) => metrics.record_event(event_class),
			Err(error) => {
				if security_relevant {
					metrics.record_security_unrecorded(
						ToronAuditSecurityEventUnrecordedReason::SinkError,
					);
				}
				tracing::warn!(error = %error, "audit sink write failed");
				readiness
					.set_degraded_reason(Some("audit_local_chain_write_failed".to_string()))
					.await;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn toron_audit_node_id_requires_toron_prefix() {
		assert!(parse_toron_audit_node_numeric("toron-n-7").is_ok());
		assert!(parse_toron_audit_node_numeric("7").is_err());
		assert!(parse_toron_audit_node_numeric("00000000-0000-0000-0000-000000000007").is_err());
	}

	#[test]
	fn statement_class_maps_mutation_and_select_plans() {
		let select = LogicalPlan::Scan {
			table: crate::ir::TableId(1),
			projection: None,
		};
		let drop = LogicalPlan::DropTable {
			name: crate::catalog::TableRef::new("t"),
			if_exists: false,
		};
		assert_eq!(StatementClass::from_plan(&select), StatementClass::Select);
		assert_eq!(StatementClass::from_plan(&drop), StatementClass::Drop);
	}

	#[test]
	fn unavailable_emitter_records_security_drop_without_panic() {
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Unavailable {
			metrics: Arc::clone(&metrics),
		};

		emitter.record(ToronAuditEvent::edge_denial(
			ToronAuditOperation::SqlConnectionLimitDenied,
			Some(TenantId(7)),
			"sql connection limit denied",
		));

		assert_eq!(
			metrics.dropped_count(
				ToronAuditEventClass::EdgeDenial,
				ToronAuditDropReason::EmitterClosed,
			),
			1
		);
		assert_eq!(
			metrics
				.security_unrecorded_count(ToronAuditSecurityEventUnrecordedReason::EmitterClosed,),
			1
		);
	}
}
