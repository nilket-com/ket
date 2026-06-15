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
use tokio::sync::{mpsc, oneshot};

use crate::config::AuditConfig;
use crate::distributed::identity::{AttemptId, QueryId, StageId, TaskId, WorkerId};
use crate::error::{
	BindingError, CatalogError, ExecutionError, FrontendError, QueryError, RuntimeError,
	SpillError, StorageError, TaskFailureKind, WireError,
};
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
	WorkerTaskStarted,
	WorkerTaskCompleted,
	WorkerTaskFailed,
	WorkerTaskCancelled,
	ObservabilityReadyAccess,
	ObservabilityMetricsAccess,
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
			Self::WorkerTaskStarted => "toron.worker.task.started",
			Self::WorkerTaskCompleted => "toron.worker.task.completed",
			Self::WorkerTaskFailed => "toron.worker.task.failed",
			Self::WorkerTaskCancelled => "toron.worker.task.cancelled",
			Self::ObservabilityReadyAccess => "toron.observability.ready.access",
			Self::ObservabilityMetricsAccess => "toron.observability.metrics.access",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToronAuditEventClass {
	Lifecycle,
	EdgeDenial,
	QueryOutcome,
	TaskLifecycle,
	ObservabilityAccess,
	Audit,
}

impl ToronAuditEventClass {
	pub const ALL: [Self; Self::COUNT] = [
		Self::Lifecycle,
		Self::EdgeDenial,
		Self::QueryOutcome,
		Self::TaskLifecycle,
		Self::ObservabilityAccess,
		Self::Audit,
	];
	pub const COUNT: usize = 6;

	pub fn as_str(self) -> &'static str {
		match self {
			Self::Lifecycle => "lifecycle",
			Self::EdgeDenial => "edge_denial",
			Self::QueryOutcome => "query_outcome",
			Self::TaskLifecycle => "task_lifecycle",
			Self::ObservabilityAccess => "observability_access",
			Self::Audit => "audit",
		}
	}

	const fn index(self) -> usize {
		match self {
			Self::Lifecycle => 0,
			Self::EdgeDenial => 1,
			Self::QueryOutcome => 2,
			Self::TaskLifecycle => 3,
			Self::ObservabilityAccess => 4,
			Self::Audit => 5,
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
			LogicalPlan::CreateTable { .. } | LogicalPlan::CreateLogicalType { .. } => Self::Create,
			LogicalPlan::CreateTableAsSelect { .. } => Self::CreateTableAsSelect,
			LogicalPlan::DropTable { .. } => Self::Drop,
			LogicalPlan::Scan { .. }
			| LogicalPlan::ScanNamed { .. }
			| LogicalPlan::Alias { .. }
			| LogicalPlan::Filter { .. }
			| LogicalPlan::Project { .. }
			| LogicalPlan::Distinct { .. }
			| LogicalPlan::Union { .. }
			| LogicalPlan::SetOpAll { .. }
			| LogicalPlan::Repeat { .. }
			| LogicalPlan::Aggregate { .. }
			| LogicalPlan::Exchange { .. }
			| LogicalPlan::Sort { .. }
			| LogicalPlan::Take { .. }
			| LogicalPlan::Join { .. }
			| LogicalPlan::Explain { .. }
			| LogicalPlan::ExplainAnalyze { .. } => Self::Select,
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
pub struct WorkerTaskAuditFields {
	pub(crate) tenant_id: TenantId,
	pub(crate) query_id: QueryId,
	pub(crate) stage_id: StageId,
	pub(crate) task_id: TaskId,
	pub(crate) attempt_id: AttemptId,
	pub(crate) worker_id: WorkerId,
	pub(crate) stage_role: &'static str,
	pub(crate) operator_kind: &'static str,
}

#[derive(Debug, Clone)]
pub struct ObservabilityAccessAuditFields {
	pub(crate) operation: ToronAuditOperation,
	pub(crate) outcome: AuditOutcome,
	pub(crate) endpoint: &'static str,
	pub(crate) method_class: &'static str,
	pub(crate) status_class: &'static str,
	pub(crate) auth_class: &'static str,
	pub(crate) outcome_class: &'static str,
	pub(crate) actor_key_id: Option<String>,
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
	stage_id: Option<StageId>,
	task_id: Option<TaskId>,
	attempt_id: Option<AttemptId>,
	worker_id: Option<WorkerId>,
	stage_role: Option<&'static str>,
	operator_kind: Option<&'static str>,
	outcome_class: Option<&'static str>,
	error_class: Option<&'static str>,
	endpoint: Option<&'static str>,
	method_class: Option<&'static str>,
	status_class: Option<&'static str>,
	auth_class: Option<&'static str>,
	actor_role: Option<&'static str>,
	actor_key_id: Option<String>,
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

	pub fn worker_task_started(fields: WorkerTaskAuditFields) -> Self {
		Self::worker_task(
			ToronAuditOperation::WorkerTaskStarted,
			fields,
			AuditOutcome::Allowed,
			"started",
			None,
			"worker task started",
		)
	}

	pub fn worker_task_completed(fields: WorkerTaskAuditFields) -> Self {
		Self::worker_task(
			ToronAuditOperation::WorkerTaskCompleted,
			fields,
			AuditOutcome::Allowed,
			"completed",
			None,
			"worker task completed",
		)
	}

	pub fn worker_task_failed(fields: WorkerTaskAuditFields, error_class: &'static str) -> Self {
		Self::worker_task(
			ToronAuditOperation::WorkerTaskFailed,
			fields,
			AuditOutcome::Error,
			"failed",
			Some(error_class),
			"worker task failed",
		)
	}

	pub fn worker_task_cancelled(fields: WorkerTaskAuditFields) -> Self {
		Self::worker_task(
			ToronAuditOperation::WorkerTaskCancelled,
			fields,
			AuditOutcome::Denied,
			"cancelled",
			Some("cancelled"),
			"worker task cancelled",
		)
	}

	pub fn observability_access(fields: ObservabilityAccessAuditFields) -> Self {
		Self::new(
			fields.operation,
			ToronAuditEventClass::ObservabilityAccess,
			fields.outcome,
			"observability access",
		)
		.with_security_relevant()
		.with_endpoint(fields.endpoint)
		.with_method_class(fields.method_class)
		.with_status_class(fields.status_class)
		.with_auth_class(fields.auth_class)
		.with_outcome_class(fields.outcome_class)
		.with_actor("observability_probe", fields.actor_key_id)
	}

	fn worker_task(
		operation: ToronAuditOperation,
		fields: WorkerTaskAuditFields,
		outcome: AuditOutcome,
		outcome_class: &'static str,
		error_class: Option<&'static str>,
		message: &'static str,
	) -> Self {
		let actor_key_id = format!("worker:{}", fields.worker_id.get());
		Self::new(
			operation,
			ToronAuditEventClass::TaskLifecycle,
			outcome,
			message,
		)
		.with_security_relevant()
		.with_tenant_option(Some(fields.tenant_id))
		.with_query_id(fields.query_id)
		.with_worker_task_identity(&fields)
		.with_outcome_class(outcome_class)
		.with_error_class_option(error_class)
		.with_actor("internal_compute", Some(actor_key_id))
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
			stage_id: None,
			task_id: None,
			attempt_id: None,
			worker_id: None,
			stage_role: None,
			operator_kind: None,
			outcome_class: None,
			error_class: None,
			endpoint: None,
			method_class: None,
			status_class: None,
			auth_class: None,
			actor_role: None,
			actor_key_id: None,
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

	fn with_worker_task_identity(mut self, fields: &WorkerTaskAuditFields) -> Self {
		self.stage_id = Some(fields.stage_id);
		self.task_id = Some(fields.task_id);
		self.attempt_id = Some(fields.attempt_id);
		self.worker_id = Some(fields.worker_id);
		self.stage_role = Some(fields.stage_role);
		self.operator_kind = Some(fields.operator_kind);
		self
	}

	fn with_outcome_class(mut self, outcome_class: &'static str) -> Self {
		self.outcome_class = Some(outcome_class);
		self
	}

	fn with_error_class_option(mut self, error_class: Option<&'static str>) -> Self {
		self.error_class = error_class;
		self
	}

	fn with_endpoint(mut self, endpoint: &'static str) -> Self {
		self.endpoint = Some(endpoint);
		self
	}

	fn with_method_class(mut self, method_class: &'static str) -> Self {
		self.method_class = Some(method_class);
		self
	}

	fn with_status_class(mut self, status_class: &'static str) -> Self {
		self.status_class = Some(status_class);
		self
	}

	fn with_auth_class(mut self, auth_class: &'static str) -> Self {
		self.auth_class = Some(auth_class);
		self
	}

	fn with_actor(mut self, actor_role: &'static str, actor_key_id: Option<String>) -> Self {
		self.actor_role = Some(actor_role);
		self.actor_key_id = actor_key_id;
		self
	}

	pub fn operation(&self) -> ToronAuditOperation {
		self.operation
	}

	pub fn event_class(&self) -> ToronAuditEventClass {
		self.event_class
	}

	#[cfg(test)]
	pub(crate) fn error_class_for_test(&self) -> Option<&'static str> {
		self.error_class
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
		if let Some(stage_id) = self.stage_id {
			event = event.with_parameter("stage_id", stage_id.get().to_string());
		}
		if let Some(task_id) = self.task_id {
			event = event.with_parameter("task_id", task_id.get().to_string());
		}
		if let Some(attempt_id) = self.attempt_id {
			event = event.with_parameter("attempt_id", attempt_id.get().to_string());
		}
		if let Some(worker_id) = self.worker_id {
			event = event.with_parameter("worker_id", worker_id.get().to_string());
		}
		if let Some(stage_role) = self.stage_role {
			event = event.with_parameter("stage_role", stage_role);
		}
		if let Some(operator_kind) = self.operator_kind {
			event = event.with_parameter("operator_kind", operator_kind);
		}
		if let Some(outcome_class) = self.outcome_class {
			event = event.with_parameter("outcome_class", outcome_class);
		}
		if let Some(error_class) = self.error_class {
			event = event.with_parameter("error_class", error_class);
		}
		if let Some(endpoint) = self.endpoint {
			event = event.with_parameter("endpoint", endpoint);
		}
		if let Some(method_class) = self.method_class {
			event = event.with_parameter("method_class", method_class);
		}
		if let Some(status_class) = self.status_class {
			event = event.with_parameter("status_class", status_class);
		}
		if let Some(auth_class) = self.auth_class {
			event = event.with_parameter("auth_class", auth_class);
		}
		if let Some(actor_role) = self.actor_role {
			event = event.with_parameter("actor_role", actor_role);
			if let Some(actor_key_id) = &self.actor_key_id {
				event = event
					.with_parameter("actor_key_id", actor_key_id.clone())
					.with_actor(shared_audit::AuditActor {
						cert_subject: None,
						role: actor_role.to_string(),
						trust_domain: "toron".to_string(),
						key_id: actor_key_id.clone(),
						actor_tenant_id: self.tenant_id.map(|tenant_id| tenant_id.0.to_string()),
						node_id: None,
						break_glass: false,
						scopes: Vec::new(),
					});
			}
		}
		Ok(event)
	}
}

pub fn worker_task_error_class(error: &QueryError) -> &'static str {
	match error {
		QueryError::Execution(error) => execution_error_class(error),
		QueryError::Storage(error) => storage_error_class(error),
		QueryError::Binding(error) => binding_error_class(error),
		QueryError::Catalog(error) => catalog_error_class(error),
		QueryError::Frontend(error) => frontend_error_class(error),
		QueryError::Wire(error) => wire_error_class(error),
		QueryError::Spill(error) => spill_error_class(error),
		QueryError::Runtime(error) => runtime_error_class(error),
		QueryError::Io(_) => "io",
		QueryError::Arrow(_) => "arrow",
		QueryError::Remote { .. } => "remote_worker",
		QueryError::UnexpectedMessage { .. } => "unexpected_message",
		QueryError::SchemaInstability => "schema_instability",
		QueryError::Internal { .. } => "internal",
	}
}

fn runtime_error_class(error: &RuntimeError) -> &'static str {
	match error {
		RuntimeError::ProtocolMismatch { .. } => "protocol_mismatch",
		RuntimeError::PartitionStoreMismatch { .. } => "partition_store_mismatch",
		RuntimeError::TaskFailure { kind } => task_failure_error_class(kind),
	}
}

pub fn task_failure_error_class(kind: &TaskFailureKind) -> &'static str {
	match kind {
		TaskFailureKind::ProtocolMismatch => "protocol_mismatch",
		TaskFailureKind::DescriptorEvolved { .. } => "descriptor_evolved",
		TaskFailureKind::WorkerLost { .. } => "worker_lost",
		TaskFailureKind::TaskFailed { .. } => "task_failed",
		TaskFailureKind::BudgetExhausted { .. } => "budget_exhausted",
		TaskFailureKind::Cancelled => "cancelled",
		TaskFailureKind::Internal { .. } => "internal",
	}
}

fn execution_error_class(error: &ExecutionError) -> &'static str {
	match error {
		ExecutionError::Storage(error) => storage_error_class(error),
		ExecutionError::Compute(_) => "compute",
		ExecutionError::Binding(error) => binding_error_class(error),
		ExecutionError::NotImplemented { .. } => "not_implemented",
		ExecutionError::ColumnNotFound { .. } => "column_not_found",
		ExecutionError::InvalidExpression { .. } => "invalid_expression",
		ExecutionError::Kernel { .. } => "kernel",
		ExecutionError::BudgetExhausted { .. } => "budget_exhausted",
		ExecutionError::ConcurrencyExhausted { .. } => "concurrency_exhausted",
	}
}

fn storage_error_class(error: &StorageError) -> &'static str {
	match error {
		StorageError::TableNotFound { .. } => "table_not_found",
		StorageError::DescriptorEvolved { .. } => "descriptor_evolved",
		StorageError::Cancelled => "cancelled",
		StorageError::ReadBoundUnavailable { .. } => "read_bound_unavailable",
		StorageError::InvalidPbcForDescriptor { .. } => "invalid_pbc_for_descriptor",
		StorageError::InvalidPbcProgram { .. } => "invalid_pbc_program",
		StorageError::PermissionDenied { .. } => "permission_denied",
		StorageError::Unavailable { .. } => "storage_unavailable",
		StorageError::Client { .. } => "storage_client",
		StorageError::Arrow(_) => "arrow",
		StorageError::Internal { .. } => "internal",
	}
}

fn binding_error_class(error: &BindingError) -> &'static str {
	match error {
		BindingError::Catalog(error) => catalog_error_class(error),
		BindingError::UnknownColumn { .. } => "unknown_column",
		BindingError::AmbiguousColumn { .. } | BindingError::AmbiguousColumnRef { .. } => {
			"ambiguous_column"
		}
		BindingError::UnknownTableRef { .. } => "unknown_table_ref",
		BindingError::AmbiguousTableRef { .. } => "ambiguous_table_ref",
		BindingError::UnknownColumnInRelation { .. } => "unknown_column_in_relation",
		BindingError::JoinPredicateNotEqui => "join_predicate_not_equi",
		BindingError::JoinKeyTypeMismatch { .. } => "join_key_type_mismatch",
		BindingError::JoinKeyTypeUnsupported { .. } => "join_key_type_unsupported",
		BindingError::JoinComputedKeyUnsupported => "join_computed_key_unsupported",
		BindingError::JoinPartitionCountMismatch { .. } => "join_partition_count_mismatch",
		BindingError::JoinKeyExchangeMismatch => "join_key_exchange_mismatch",
		BindingError::BroadcastBuildSidePlacement => "broadcast_build_side_placement",
		BindingError::BroadcastCapExceededAtBind { .. } => "broadcast_cap_exceeded",
		BindingError::TypeMismatch { .. } => "type_mismatch",
		BindingError::NominalTypeMismatch { .. } => "nominal_type_mismatch",
		BindingError::ExperimentalKernel { .. } => "experimental_kernel",
		BindingError::UnresolvedTable { .. } => "unresolved_table",
		BindingError::UnsupportedExpression { .. } => "unsupported_expression",
	}
}

fn catalog_error_class(error: &CatalogError) -> &'static str {
	match error {
		CatalogError::TableNotFound { .. } => "table_not_found",
		CatalogError::SchemaNotFound { .. } => "schema_not_found",
		CatalogError::TableAlreadyExists { .. } => "table_already_exists",
		CatalogError::LogicalTypeNotFound { .. } => "logical_type_not_found",
		CatalogError::SchemaMismatch { .. } => "schema_mismatch",
		CatalogError::AdminUnavailable { .. } => "admin_unavailable",
		CatalogError::AdminFailed { .. } => "admin_failed",
		CatalogError::ResourceExhausted { .. } => "resource_exhausted",
		CatalogError::FingerprintResolution { .. } => "fingerprint_resolution",
		CatalogError::Storage(error) => storage_error_class(error),
	}
}

fn frontend_error_class(error: &FrontendError) -> &'static str {
	match error {
		FrontendError::SqlParse { .. } => "sql_parse",
		FrontendError::RuneCompile { .. } => "rune_compile",
		FrontendError::RuneRuntime { .. } => "rune_runtime",
		FrontendError::RuneSandbox { .. } => "rune_sandbox",
		FrontendError::Unsupported { .. } => "unsupported_frontend",
	}
}

fn wire_error_class(error: &WireError) -> &'static str {
	match error {
		WireError::Decode(_) => "wire_decode",
		WireError::FrameTooLarge { .. } => "wire_frame_too_large",
	}
}

fn spill_error_class(error: &SpillError) -> &'static str {
	match error {
		SpillError::Random { .. } => "spill_random",
		SpillError::Crypto { .. } => "spill_crypto",
		SpillError::MissingSpill { .. } => "missing_spill",
		SpillError::EmptySpill { .. } => "empty_spill",
		SpillError::Io(_) => "spill_io",
		SpillError::Arrow(_) => "spill_arrow",
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
		tx: mpsc::Sender<ToronAuditCommand>,
		metrics: Arc<ToronAuditMetrics>,
		fail_closed: bool,
		ack_timeout: Duration,
	},
	Unavailable {
		metrics: Arc<ToronAuditMetrics>,
		fail_closed: bool,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditUnavailableReason {
	QueueFull,
	EmitterClosed,
	SerializationFailed,
	SinkFailed,
	Timeout,
}

impl AuditUnavailableReason {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::QueueFull => "queue_full",
			Self::EmitterClosed => "emitter_closed",
			Self::SerializationFailed => "serialization_failed",
			Self::SinkFailed => "sink_failed",
			Self::Timeout => "timeout",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditUnavailable {
	reason: AuditUnavailableReason,
}

impl AuditUnavailable {
	fn new(reason: AuditUnavailableReason) -> Self {
		Self { reason }
	}

	pub fn reason(&self) -> AuditUnavailableReason {
		self.reason
	}
}

impl std::fmt::Display for AuditUnavailable {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "audit unavailable: {}", self.reason.as_str())
	}
}

impl std::error::Error for AuditUnavailable {}

pub struct ToronAuditCommand {
	event: ToronAuditEvent,
	required_ack: Option<oneshot::Sender<Result<(), AuditUnavailable>>>,
}

impl ToronAuditCommand {
	pub fn fire_and_forget(event: ToronAuditEvent) -> Self {
		Self {
			event,
			required_ack: None,
		}
	}

	fn required(
		event: ToronAuditEvent,
		required_ack: oneshot::Sender<Result<(), AuditUnavailable>>,
	) -> Self {
		Self {
			event,
			required_ack: Some(required_ack),
		}
	}

	pub fn event(&self) -> &ToronAuditEvent {
		&self.event
	}

	pub fn operation(&self) -> ToronAuditOperation {
		self.event.operation()
	}

	pub fn event_class(&self) -> ToronAuditEventClass {
		self.event.event_class()
	}

	#[cfg(test)]
	pub(crate) fn error_class_for_test(&self) -> Option<&'static str> {
		self.event.error_class_for_test()
	}
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
				return Self::Unavailable {
					metrics,
					fail_closed: config.fail_closed,
				};
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
				return Self::Unavailable {
					metrics,
					fail_closed: config.fail_closed,
				};
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
		Self::Enabled {
			tx,
			metrics,
			fail_closed: config.fail_closed,
			ack_timeout: config.fail_closed_ack_timeout,
		}
	}

	pub fn record(&self, event: ToronAuditEvent) {
		match self {
			Self::Disabled => {}
			Self::Unavailable { metrics, .. } => {
				record_drop(metrics, &event, ToronAuditDropReason::EmitterClosed);
			}
			Self::Enabled { tx, metrics, .. } => {
				let event = ToronAuditCommand::fire_and_forget(event);
				match tx.try_send(event) {
					Ok(()) => {}
					Err(mpsc::error::TrySendError::Full(event)) => {
						record_drop(metrics, event.event(), ToronAuditDropReason::QueueFull);
					}
					Err(mpsc::error::TrySendError::Closed(event)) => {
						record_drop(metrics, event.event(), ToronAuditDropReason::EmitterClosed);
					}
				}
			}
		}
	}

	pub async fn record_required(&self, event: ToronAuditEvent) -> Result<(), AuditUnavailable> {
		if !event.security_relevant() || event.operation() == ToronAuditOperation::AuditSinkFailure
		{
			self.record(event);
			return Ok(());
		}
		match self {
			Self::Disabled => Ok(()),
			Self::Unavailable {
				metrics,
				fail_closed,
			} => {
				record_drop(metrics, &event, ToronAuditDropReason::EmitterClosed);
				if *fail_closed {
					Err(AuditUnavailable::new(AuditUnavailableReason::EmitterClosed))
				} else {
					Ok(())
				}
			}
			Self::Enabled {
				tx,
				metrics,
				fail_closed,
				ack_timeout,
			} => {
				if !*fail_closed {
					self.record(event);
					return Ok(());
				}
				let (ack_tx, ack_rx) = oneshot::channel();
				match tx.try_send(ToronAuditCommand::required(event, ack_tx)) {
					Ok(()) => {}
					Err(mpsc::error::TrySendError::Full(command)) => {
						record_drop(metrics, command.event(), ToronAuditDropReason::QueueFull);
						return Err(AuditUnavailable::new(AuditUnavailableReason::QueueFull));
					}
					Err(mpsc::error::TrySendError::Closed(command)) => {
						record_drop(
							metrics,
							command.event(),
							ToronAuditDropReason::EmitterClosed,
						);
						return Err(AuditUnavailable::new(AuditUnavailableReason::EmitterClosed));
					}
				}
				match tokio::time::timeout(*ack_timeout, ack_rx).await {
					Ok(Ok(result)) => result,
					Ok(Err(_closed)) => {
						Err(AuditUnavailable::new(AuditUnavailableReason::EmitterClosed))
					}
					Err(_elapsed) => Err(AuditUnavailable::new(AuditUnavailableReason::Timeout)),
				}
			}
		}
	}

	pub fn fail_closed_enabled(&self) -> bool {
		match self {
			Self::Disabled => false,
			Self::Enabled { fail_closed, .. } | Self::Unavailable { fail_closed, .. } => {
				*fail_closed
			}
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
	mut rx: mpsc::Receiver<ToronAuditCommand>,
	sink: Arc<dyn shared_audit::AuditSink>,
	node_id: ToronAuditNodeId,
	metrics: Arc<ToronAuditMetrics>,
	readiness: Arc<ReadinessState>,
) {
	while let Some(command) = rx.recv().await {
		let event = command.event;
		let required_ack = command.required_ack;
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
				if let Some(ack) = required_ack {
					let _ = ack.send(Err(AuditUnavailable::new(
						AuditUnavailableReason::SerializationFailed,
					)));
				}
				continue;
			}
		};
		match sink.emit(shared_event) {
			Ok(()) => {
				metrics.record_event(event_class);
				if let Some(ack) = required_ack {
					let _ = ack.send(Ok(()));
				}
			}
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
				if let Some(ack) = required_ack {
					let _ = ack.send(Err(AuditUnavailable::new(
						AuditUnavailableReason::SinkFailed,
					)));
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicU64, Ordering};

	struct RecordingAuditSink {
		emits: AtomicU64,
		fail: bool,
	}

	impl RecordingAuditSink {
		fn succeeding() -> Self {
			Self {
				emits: AtomicU64::new(0),
				fail: false,
			}
		}

		fn failing() -> Self {
			Self {
				emits: AtomicU64::new(0),
				fail: true,
			}
		}

		fn emits(&self) -> u64 {
			self.emits.load(Ordering::Relaxed)
		}
	}

	impl shared_audit::AuditSink for RecordingAuditSink {
		fn emit(&self, _event: shared_audit::AuditEvent) -> std::io::Result<()> {
			self.emits.fetch_add(1, Ordering::Relaxed);
			if self.fail {
				Err(std::io::Error::other("injected audit sink failure"))
			} else {
				Ok(())
			}
		}
	}

	#[test]
	fn toron_audit_node_id_requires_toron_prefix() {
		assert!(parse_toron_audit_node_numeric("toron-n-7").is_ok());
		assert!(parse_toron_audit_node_numeric("7").is_err());
		assert!(parse_toron_audit_node_numeric("00000000-0000-0000-0000-000000000007").is_err());
	}

	#[test]
	fn worker_audit_node_id_validation() {
		assert!(ToronAuditNodeId::new("toron-n-7", 7).is_ok());
		assert!(ToronAuditNodeId::new("toron-n-7", 8).is_err());
		assert!(ToronAuditNodeId::new("worker-7", 7).is_err());
		assert!(ToronAuditNodeId::new("toron-n-00000000-0000-0000-0000-000000000007", 7).is_err());
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
			fail_closed: false,
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

	#[tokio::test]
	async fn record_required_delegates_fail_open_when_fail_closed_false() {
		let (tx, mut rx) = mpsc::channel(1);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics,
			fail_closed: false,
			ack_timeout: Duration::from_millis(50),
		};

		emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect("fail-open required audit succeeds");

		let command = rx.recv().await.expect("audit command");
		assert_eq!(
			command.operation(),
			ToronAuditOperation::SqlConnectionLimitDenied
		);
	}

	#[tokio::test]
	async fn required_audit_queue_full_returns_bounded_audit_unavailable() {
		let (tx, _rx) = mpsc::channel(1);
		tx.try_send(ToronAuditCommand::fire_and_forget(
			ToronAuditEvent::lifecycle_startup(),
		))
		.expect("fill queue");
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics: Arc::clone(&metrics),
			fail_closed: true,
			ack_timeout: Duration::from_millis(50),
		};

		let error = emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect_err("queue full rejects required audit");

		assert_eq!(error.reason(), AuditUnavailableReason::QueueFull);
		assert_eq!(
			metrics.dropped_count(
				ToronAuditEventClass::EdgeDenial,
				ToronAuditDropReason::QueueFull,
			),
			1
		);
	}

	#[tokio::test]
	async fn required_audit_ack_timeout_returns_bounded_audit_unavailable() {
		let (tx, _rx) = mpsc::channel(1);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics,
			fail_closed: true,
			ack_timeout: Duration::from_millis(1),
		};

		let error = emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect_err("missing required ack rejects audit");

		assert_eq!(error.reason(), AuditUnavailableReason::Timeout);
	}

	#[tokio::test]
	async fn required_audit_closed_emitter_returns_bounded_audit_unavailable() {
		let (tx, rx) = mpsc::channel(1);
		drop(rx);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics,
			fail_closed: true,
			ack_timeout: Duration::from_millis(50),
		};

		let error = emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect_err("closed emitter rejects required audit");

		assert_eq!(error.reason(), AuditUnavailableReason::EmitterClosed);
	}

	#[tokio::test]
	async fn sink_failure_diagnostic_does_not_recursively_require_audit() {
		let (tx, _rx) = mpsc::channel(1);
		tx.try_send(ToronAuditCommand::fire_and_forget(
			ToronAuditEvent::lifecycle_startup(),
		))
		.expect("fill queue");
		let metrics = Arc::new(ToronAuditMetrics::default());
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics: Arc::clone(&metrics),
			fail_closed: true,
			ack_timeout: Duration::from_millis(50),
		};

		emitter
			.record_required(ToronAuditEvent::sink_failure())
			.await
			.expect("sink failure diagnostic is best-effort");

		assert_eq!(
			metrics.dropped_count(ToronAuditEventClass::Audit, ToronAuditDropReason::QueueFull),
			1
		);
	}

	#[tokio::test]
	async fn required_audit_ack_follows_local_sink_emit_success() {
		let sink = Arc::new(RecordingAuditSink::succeeding());
		let worker_sink: Arc<dyn shared_audit::AuditSink> = sink.clone();
		let (tx, rx) = mpsc::channel(4);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let readiness = Arc::new(ReadinessState::new(true, true, true, 0, 0));
		tokio::spawn(run_audit_worker(
			rx,
			worker_sink,
			ToronAuditNodeId::new("toron-n-1", 1).expect("node id"),
			Arc::clone(&metrics),
			readiness,
		));
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics: Arc::clone(&metrics),
			fail_closed: true,
			ack_timeout: Duration::from_secs(1),
		};

		emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect("required audit ack");

		assert_eq!(sink.emits(), 1);
		assert_eq!(metrics.event_count(ToronAuditEventClass::EdgeDenial), 1);
	}

	#[tokio::test]
	async fn required_audit_sink_error_rejects_and_degrades_readiness() {
		let sink = Arc::new(RecordingAuditSink::failing());
		let worker_sink: Arc<dyn shared_audit::AuditSink> = sink.clone();
		let (tx, rx) = mpsc::channel(4);
		let metrics = Arc::new(ToronAuditMetrics::default());
		let readiness = Arc::new(ReadinessState::new(true, true, true, 0, 0));
		tokio::spawn(run_audit_worker(
			rx,
			worker_sink,
			ToronAuditNodeId::new("toron-n-1", 1).expect("node id"),
			Arc::clone(&metrics),
			Arc::clone(&readiness),
		));
		let emitter = ToronAuditEmitter::Enabled {
			tx,
			metrics: Arc::clone(&metrics),
			fail_closed: true,
			ack_timeout: Duration::from_secs(1),
		};

		let error = emitter
			.record_required(ToronAuditEvent::edge_denial(
				ToronAuditOperation::SqlConnectionLimitDenied,
				Some(TenantId(7)),
				"sql connection limit denied",
			))
			.await
			.expect_err("sink failure rejects required audit");

		assert_eq!(error.reason(), AuditUnavailableReason::SinkFailed);
		assert_eq!(
			metrics.security_unrecorded_count(ToronAuditSecurityEventUnrecordedReason::SinkError),
			1
		);
		assert_eq!(
			readiness.snapshot().await.degraded_reason.as_deref(),
			Some("audit_local_chain_write_failed")
		);
	}

	#[test]
	fn worker_task_audit_event_serializes_bounded_identity_and_classes() {
		let node_id = ToronAuditNodeId::new("toron-n-1", 1).expect("node id");
		let fields = WorkerTaskAuditFields {
			tenant_id: TenantId(7),
			query_id: QueryId::from_u128(9),
			stage_id: StageId::new(2),
			task_id: TaskId::new(3),
			attempt_id: AttemptId::first(),
			worker_id: WorkerId::from_u128(4),
			stage_role: "scan",
			operator_kind: "scan",
		};

		let event = ToronAuditEvent::worker_task_failed(fields, "storage_unavailable")
			.with_duration(Duration::from_millis(25));
		let shared = event.to_shared(&node_id).expect("shared event");

		assert_eq!(shared.operation, "toron.worker.task.failed");
		assert_eq!(
			shared.parameters.get("event_class"),
			Some(&json!("task_lifecycle"))
		);
		assert_eq!(shared.parameters.get("stage_id"), Some(&json!("2")));
		assert_eq!(shared.parameters.get("task_id"), Some(&json!("3")));
		assert_eq!(
			shared.parameters.get("error_class"),
			Some(&json!("storage_unavailable"))
		);
		assert_eq!(
			shared.parameters.get("actor_role"),
			Some(&json!("internal_compute"))
		);
		assert!(shared.parameters.contains_key("actor_key_id"));
		assert!(!shared.parameters.contains_key("sql"));
		assert!(!shared.parameters.contains_key("raw_error"));
	}

	#[test]
	fn observability_access_audit_event_serializes_bounded_monitor_actor() {
		let node_id = ToronAuditNodeId::new("toron-n-1", 1).expect("node id");
		let event = ToronAuditEvent::observability_access(ObservabilityAccessAuditFields {
			operation: ToronAuditOperation::ObservabilityReadyAccess,
			outcome: AuditOutcome::Allowed,
			endpoint: "ready",
			method_class: "get",
			status_class: "2xx",
			auth_class: "mtls_monitor",
			outcome_class: "allowed",
			actor_key_id: Some("monitor:prometheus".to_string()),
		});

		let shared = event.to_shared(&node_id).expect("shared event");

		assert_eq!(shared.operation, "toron.observability.ready.access");
		assert_eq!(
			shared.parameters.get("event_class"),
			Some(&json!("observability_access"))
		);
		assert_eq!(shared.parameters.get("endpoint"), Some(&json!("ready")));
		assert_eq!(shared.parameters.get("method_class"), Some(&json!("get")));
		assert_eq!(shared.parameters.get("status_class"), Some(&json!("2xx")));
		assert_eq!(
			shared.parameters.get("auth_class"),
			Some(&json!("mtls_monitor"))
		);
		assert_eq!(
			shared.parameters.get("actor_key_id"),
			Some(&json!("monitor:prometheus"))
		);
		assert_eq!(
			shared.actor.as_ref().map(|actor| actor.role.as_str()),
			Some("observability_probe")
		);
		assert!(!shared.parameters.contains_key("san"));
		assert!(!shared.parameters.contains_key("cert"));
	}
}
