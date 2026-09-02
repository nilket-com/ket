use crate::consensus::monitor::{ClockDriftEvent, ClockDriftSubscriber};
use crate::metrics::global as metrics;
use crate::network::tls::ActorIdentity;
use crate::schema::{Identity, NodeId, TenantId};
use observability::audit as shared_audit;
#[cfg(feature = "audit-kafka")]
pub use observability::audit::KafkaProducer;
pub use observability::audit::{
	ArchiveAuditConfig, AuditArchiveLockMode, AuditAuthority, AuditAuthorityModel, AuditChainHead,
	AuditEvidenceDisposition, AuditOutcome, AuditProjectionSinkKind, AuditRootSigner,
	AuditSinksConfig, AuditVerificationIssue, AuditVerificationReport,
	DEFAULT_AUDIT_ARCHIVE_MAX_SEGMENT_BYTES, DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL, KafkaAuditConfig,
	KafkaAuditPayloadMode, SignedAuditChainRoot, SyslogAuditConfig,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

const AUDIT_SCHEMA_VERSION: u16 = 4;
const AUDIT_GENESIS_TAG: &[u8] = b"wendelon-audit-genesis-v1";
const AUDIT_CHAIN_ROOT_TAG: &[u8] = b"wendelon-audit-chain-root-v1";
const AUDIT_CHAIN_ROOT_OPERATION: &str = "audit.chain_root";
const AUDIT_SINK_FAILURE_OPERATION: &str = "audit.sink_failure";
const AUDIT_ROOT_SIGNATURE_TAG: &str = "ed25519";
const SYSLOG_APP_NAME: &str = "wendelon";
const SYSLOG_ENTERPRISE_ID: &str = "wendelon@47450";
const DEFAULT_KAFKA_CLIENT_ID: &str = "wendelon-audit";

fn wendelon_audit_format() -> shared_audit::AuditFormat {
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
		parse_node_numeric,
	}
}

fn parse_node_numeric(raw: &str) -> std::io::Result<u32> {
	Ok(NodeId::parse_wire(raw)?.id)
}

fn shared_node_id(node_id: NodeId) -> shared_audit::AuditNodeId {
	shared_audit::AuditNodeId::new(node_id.format_wire(), node_id.id)
}

pub fn hex_string(bytes: &[u8]) -> String {
	shared_audit::hex_string(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
	ClockDriftExceeded,
	ObservabilityReady,
	ObservabilityMetrics,
	IdentityHandshakeDenied,
	IdentityAuthorizationDenied,
	CryptoDecryptRejected,
	CryptoStartup,
	CryptoTenantKekRotated,
	CryptoTenantKekRetired,
	CryptoSlabDekRewrapped,
	IdentityBreakGlassIssued,
	IdentityActorKeyRevoked,
	DataRead,
	DataWrite,
	DataRangeQuery,
	DataWriteFinalized,
	CdcSubscriptionOpened,
	CdcSubscriptionClosed,
	ControlRingEpochAdvanced,
	MvccSnapshotPruned,
	TenantCreated,
	TenantQuotaUpdated,
	TableRetentionPolicyUpdated,
	LsmCompactionCommitted,
	LsmInvariantViolation,
	LsmCompactionPaused,
	RowGroupFetchSampled,
	RowGroupInvariantViolation,
	EncodingMaterializeSampled,
	EncodingInvariantViolation,
	EncodingComputeSampled,
	EncodingComputeInvariantViolation,
	TableSchemaDescriptorUpdated,
	SecondaryIndexCatalogUpdated,
	TenantSuspended,
	TenantResumed,
	TenantDeleteRequested,
	MaintenanceRequeueAttempt,
	MaintenanceRequeueCompletion,
	AuditChainRoot,
	AuditSinkFailure,
}

impl AuditOperation {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::ClockDriftExceeded => "clock_drift_exceeded",
			Self::ObservabilityReady => "observability.ready",
			Self::ObservabilityMetrics => "observability.metrics",
			Self::IdentityHandshakeDenied => "identity.handshake_denied",
			Self::IdentityAuthorizationDenied => "identity.authorization_denied",
			Self::CryptoDecryptRejected => "crypto.decrypt_rejected",
			Self::CryptoStartup => "crypto.startup",
			Self::CryptoTenantKekRotated => "crypto.tenant_kek_rotated",
			Self::CryptoTenantKekRetired => "crypto.tenant_kek_retired",
			Self::CryptoSlabDekRewrapped => "crypto.slab_dek_rewrapped",
			Self::IdentityBreakGlassIssued => "identity.break_glass_issued",
			Self::IdentityActorKeyRevoked => "identity.actor_key_revoked",
			Self::DataRead => "data.read",
			Self::DataWrite => "data.write",
			Self::DataRangeQuery => "data.range_query",
			Self::DataWriteFinalized => "data.write_finalized",
			Self::CdcSubscriptionOpened => "cdc.subscription_opened",
			Self::CdcSubscriptionClosed => "cdc.subscription_closed",
			Self::ControlRingEpochAdvanced => "control_ring.epoch_advanced",
			Self::MvccSnapshotPruned => "mvcc.snapshot_pruned",
			Self::TenantCreated => "tenant.created",
			Self::TenantQuotaUpdated => "tenant.quota_updated",
			Self::TableRetentionPolicyUpdated => "table.retention_policy_updated",
			Self::LsmCompactionCommitted => "lsm.compaction_committed",
			Self::LsmInvariantViolation => "lsm.invariant_violation",
			Self::LsmCompactionPaused => "lsm.compaction_paused",
			Self::RowGroupFetchSampled => "row_group.fetch_sampled",
			Self::RowGroupInvariantViolation => "row_group.invariant_violation",
			Self::EncodingMaterializeSampled => "encoding.materialize_sampled",
			Self::EncodingInvariantViolation => "encoding.invariant_violation",
			Self::EncodingComputeSampled => "encoding_compute.sampled",
			Self::EncodingComputeInvariantViolation => "encoding_compute.invariant_violation",
			Self::TableSchemaDescriptorUpdated => "table.schema_descriptor_updated",
			Self::SecondaryIndexCatalogUpdated => "secondary_index.catalog_updated",
			Self::TenantSuspended => "tenant.suspended",
			Self::TenantResumed => "tenant.resumed",
			Self::TenantDeleteRequested => "tenant.delete_requested",
			Self::MaintenanceRequeueAttempt => "maintenance.requeue_attempt",
			Self::MaintenanceRequeueCompletion => "maintenance.requeue_completion",
			Self::AuditChainRoot => AUDIT_CHAIN_ROOT_OPERATION,
			Self::AuditSinkFailure => AUDIT_SINK_FAILURE_OPERATION,
		}
	}
}

impl Serialize for AuditOperation {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

impl<'de> Deserialize<'de> for AuditOperation {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let raw = String::deserialize(deserializer)?;
		match raw.as_str() {
			"clock_drift_exceeded" => Ok(Self::ClockDriftExceeded),
			"observability.ready" => Ok(Self::ObservabilityReady),
			"observability.metrics" => Ok(Self::ObservabilityMetrics),
			"identity.handshake_denied" => Ok(Self::IdentityHandshakeDenied),
			"identity.authorization_denied" => Ok(Self::IdentityAuthorizationDenied),
			"crypto.decrypt_rejected" => Ok(Self::CryptoDecryptRejected),
			"crypto.startup" => Ok(Self::CryptoStartup),
			"crypto.tenant_kek_rotated" => Ok(Self::CryptoTenantKekRotated),
			"crypto.tenant_kek_retired" => Ok(Self::CryptoTenantKekRetired),
			"crypto.slab_dek_rewrapped" => Ok(Self::CryptoSlabDekRewrapped),
			"identity.break_glass_issued" => Ok(Self::IdentityBreakGlassIssued),
			"identity.actor_key_revoked" => Ok(Self::IdentityActorKeyRevoked),
			"data.read" => Ok(Self::DataRead),
			"data.write" => Ok(Self::DataWrite),
			"data.range_query" => Ok(Self::DataRangeQuery),
			"data.write_finalized" => Ok(Self::DataWriteFinalized),
			"cdc.subscription_opened" => Ok(Self::CdcSubscriptionOpened),
			"cdc.subscription_closed" => Ok(Self::CdcSubscriptionClosed),
			"control_ring.epoch_advanced" => Ok(Self::ControlRingEpochAdvanced),
			"mvcc.snapshot_pruned" => Ok(Self::MvccSnapshotPruned),
			"tenant.created" => Ok(Self::TenantCreated),
			"tenant.quota_updated" => Ok(Self::TenantQuotaUpdated),
			"table.retention_policy_updated" => Ok(Self::TableRetentionPolicyUpdated),
			"lsm.compaction_committed" => Ok(Self::LsmCompactionCommitted),
			"lsm.invariant_violation" => Ok(Self::LsmInvariantViolation),
			"lsm.compaction_paused" => Ok(Self::LsmCompactionPaused),
			"row_group.fetch_sampled" => Ok(Self::RowGroupFetchSampled),
			"row_group.invariant_violation" => Ok(Self::RowGroupInvariantViolation),
			"encoding.materialize_sampled" => Ok(Self::EncodingMaterializeSampled),
			"encoding.invariant_violation" => Ok(Self::EncodingInvariantViolation),
			"encoding_compute.sampled" => Ok(Self::EncodingComputeSampled),
			"encoding_compute.invariant_violation" => Ok(Self::EncodingComputeInvariantViolation),
			"table.schema_descriptor_updated" => Ok(Self::TableSchemaDescriptorUpdated),
			"secondary_index.catalog_updated" => Ok(Self::SecondaryIndexCatalogUpdated),
			"tenant.suspended" => Ok(Self::TenantSuspended),
			"tenant.resumed" => Ok(Self::TenantResumed),
			"tenant.delete_requested" => Ok(Self::TenantDeleteRequested),
			"maintenance.requeue_attempt" => Ok(Self::MaintenanceRequeueAttempt),
			"maintenance.requeue_completion" => Ok(Self::MaintenanceRequeueCompletion),
			"audit.chain_root" => Ok(Self::AuditChainRoot),
			"audit.sink_failure" => Ok(Self::AuditSinkFailure),
			other => Err(serde::de::Error::custom(format!(
				"unknown audit operation {other}"
			))),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditActor {
	pub cert_subject: Option<String>,
	pub role: String,
	pub trust_domain: String,
	pub key_id: String,
	pub actor_tenant_id: Option<String>,
	pub node_id: Option<String>,
	pub break_glass: bool,
	pub scopes: Vec<String>,
}

impl AuditActor {
	pub fn from_actor_identity(actor: &ActorIdentity) -> Self {
		Self {
			cert_subject: actor.cert_subject.clone(),
			role: actor.role.as_str().to_string(),
			trust_domain: actor.trust_domain.to_string(),
			key_id: hex_string(&actor.key_id),
			actor_tenant_id: actor.tenant_id.clone(),
			node_id: actor.node_id.map(NodeId::format_wire),
			break_glass: actor.break_glass,
			scopes: actor.scopes.clone(),
		}
	}

	fn to_shared(&self) -> shared_audit::AuditActor {
		shared_audit::AuditActor {
			cert_subject: self.cert_subject.clone(),
			role: self.role.clone(),
			trust_domain: self.trust_domain.clone(),
			key_id: self.key_id.clone(),
			actor_tenant_id: self.actor_tenant_id.clone(),
			node_id: self.node_id.clone(),
			break_glass: self.break_glass,
			scopes: self.scopes.clone(),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdministrativeControlAction {
	AdvanceEpoch,
	TenantCreate,
	TenantQuotaUpdate,
	TableRetentionPolicyUpdate,
	TableSchemaDescriptorUpdate,
	SecondaryIndexCatalogUpdate,
	TenantSuspend,
	TenantResume,
	TenantDeleteRequest,
	TenantKekRotate,
	TenantKekRetire,
	ActorKeyRevoke,
	BreakGlassIssue,
	MaintenanceRequeue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdministrativeControlSchema {
	#[serde(default)]
	pub target_tenant_id: Option<u64>,
	#[serde(default)]
	pub target_tenant_kek_id: Option<u64>,
	#[serde(default)]
	pub target_table_object_id: Option<String>,
	#[serde(default)]
	pub target_index_object_id: Option<String>,
	#[serde(default)]
	pub policy_generation: Option<u64>,
	#[serde(default)]
	pub retention_policy: Option<String>,
	#[serde(default)]
	pub retention_window_ms: Option<u64>,
	#[serde(default)]
	pub previous_retention_policy: Option<String>,
	#[serde(default)]
	pub previous_retention_window_ms: Option<u64>,
	#[serde(default)]
	pub retention_extends_window: Option<bool>,
	#[serde(default)]
	pub retention_extension_restores_history: Option<bool>,
	#[serde(default)]
	pub schema_mutation: Option<String>,
	#[serde(default)]
	pub schema_generation: Option<u64>,
	#[serde(default)]
	pub previous_schema_generation: Option<u64>,
	#[serde(default)]
	pub table_schema_generation: Option<u64>,
	#[serde(default)]
	pub indexed_column: Option<String>,
	#[serde(default)]
	pub index_lifecycle_state: Option<String>,
	#[serde(default)]
	pub previous_index_lifecycle_state: Option<String>,
	#[serde(default)]
	pub index_replica_id: Option<u32>,
	#[serde(default)]
	pub descriptor_mutation_axis: Option<String>,
	#[serde(default)]
	pub logical_schema_fingerprint: Option<String>,
	#[serde(default)]
	pub previous_logical_schema_fingerprint: Option<String>,
	#[serde(default)]
	pub physical_descriptor_fingerprint: Option<String>,
	#[serde(default)]
	pub previous_physical_descriptor_fingerprint: Option<String>,
	#[serde(default)]
	pub target_role: Option<String>,
	#[serde(default)]
	pub target_scope: Option<String>,
	#[serde(default)]
	pub target_node_id: Option<String>,
	#[serde(default)]
	pub from_epoch: Option<u64>,
	#[serde(default)]
	pub to_epoch: Option<u64>,
	#[serde(default)]
	pub target_trust_domain: Option<String>,
	#[serde(default)]
	pub target_key_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalSecuritySurface {
	Observability,
	Transport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalSecurityStage {
	TlsHandshake,
	PeerIdentity,
	Authorization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSecuritySchema {
	pub surface: LocalSecuritySurface,
	pub stage: LocalSecurityStage,
	#[serde(default)]
	pub path: Option<String>,
	#[serde(default)]
	pub required_permission: Option<String>,
	#[serde(default)]
	pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataAccessKind {
	Read,
	Write,
	RangeQuery,
	CdcSubscribe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataAccessPhase {
	Requested,
	Finalized,
	Completed,
	Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DataAccessSchema {
	#[serde(default)]
	pub locality: Option<u64>,
	#[serde(default)]
	pub locality_end: Option<u64>,
	#[serde(default)]
	pub durability: Option<u64>,
	#[serde(default)]
	pub charged_bytes: Option<u64>,
	#[serde(default)]
	pub returned_bytes: Option<u64>,
	#[serde(default)]
	pub result_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "detail", rename_all = "snake_case")]
pub enum AuditSchema {
	AdministrativeControl {
		action: AdministrativeControlAction,
		#[serde(flatten)]
		detail: AdministrativeControlSchema,
	},
	LocalSecurity(LocalSecuritySchema),
	DataAccess {
		access_kind: DataAccessKind,
		phase: DataAccessPhase,
		#[serde(flatten)]
		detail: DataAccessSchema,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditEvent {
	pub authority: AuditAuthority,
	pub operation: AuditOperation,
	pub node_id: NodeId,
	pub tenant_id: Option<TenantId>,
	pub record_identity: Option<Identity>,
	pub actor: Option<AuditActor>,
	pub outcome: AuditOutcome,
	pub raft_log_index: Option<u64>,
	pub message: String,
	pub schema: Option<AuditSchema>,
	pub parameters: BTreeMap<String, Value>,
}

impl AuditEvent {
	pub fn new(
		authority: AuditAuthority,
		operation: AuditOperation,
		node_id: NodeId,
		message: impl Into<String>,
	) -> Self {
		Self {
			authority,
			operation,
			node_id,
			tenant_id: None,
			record_identity: None,
			actor: None,
			outcome: AuditOutcome::Allowed,
			raft_log_index: None,
			message: message.into(),
			schema: None,
			parameters: BTreeMap::new(),
		}
	}

	pub fn with_actor(mut self, actor: &ActorIdentity) -> Self {
		self.actor = Some(AuditActor::from_actor_identity(actor));
		if actor.break_glass {
			self.outcome = AuditOutcome::BreakGlass;
		}
		self
	}

	pub fn with_tenant(mut self, tenant_id: TenantId) -> Self {
		self.tenant_id = Some(tenant_id);
		self
	}

	pub fn with_record_identity(mut self, record_identity: Identity) -> Self {
		self.record_identity = Some(record_identity);
		self
	}

	pub fn with_outcome(mut self, outcome: AuditOutcome) -> Self {
		self.outcome = outcome;
		self
	}

	pub fn with_raft_log_index(mut self, raft_log_index: u64) -> Self {
		self.raft_log_index = Some(raft_log_index);
		self
	}

	pub fn with_schema(mut self, schema: AuditSchema) -> Self {
		self.schema = Some(schema);
		self
	}

	pub fn with_parameter(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
		self.parameters.insert(key.into(), value.into());
		self
	}

	pub fn with_parameters<I, K>(mut self, entries: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		for (key, value) in entries {
			self.parameters.insert(key.into(), value);
		}
		self
	}

	pub fn break_glass(&self) -> bool {
		self.actor
			.as_ref()
			.map(|actor| actor.break_glass)
			.unwrap_or(false)
	}

	pub fn event_type(&self) -> &'static str {
		self.operation.as_str()
	}

	pub fn authority_model(&self) -> AuditAuthorityModel {
		match self.authority {
			AuditAuthority::NodeLocal => AuditAuthorityModel::NodeLocalAuthoritative,
			AuditAuthority::RaftLog => AuditAuthorityModel::RaftAuthoritativeMaterialized,
		}
	}

	fn to_shared(&self) -> std::io::Result<shared_audit::AuditEvent> {
		let mut event = shared_audit::AuditEvent::new(
			self.authority,
			self.operation.as_str(),
			shared_node_id(self.node_id),
			self.message.clone(),
		)
		.with_outcome(self.outcome);
		if let Some(tenant_id) = self.tenant_id {
			event = event.with_tenant(shared_audit::AuditTenantId(tenant_id.get()));
		}
		if let Some(identity) = &self.record_identity {
			event = event.with_record_identity(shared_audit::AuditRecordIdentity(hex_string(
				identity.as_bytes(),
			)));
		}
		if let Some(actor) = &self.actor {
			event = event.with_actor(actor.to_shared());
		}
		if let Some(raft_log_index) = self.raft_log_index {
			event = event.with_raft_log_index(raft_log_index);
		}
		if let Some(schema) = &self.schema {
			event = event.with_schema_value(
				serde_json::to_value(schema)
					.map_err(|err| Error::other(format!("audit schema json: {err}")))?,
			);
		}
		Ok(event.with_parameters(self.parameters.clone()))
	}
}

impl Serialize for AuditEvent {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut event = serializer.serialize_struct("AuditEvent", 13)?;
		event.serialize_field("authority", &self.authority)?;
		event.serialize_field("authority_model", &self.authority_model())?;
		event.serialize_field("operation", &self.operation)?;
		event.serialize_field("event_type", self.event_type())?;
		event.serialize_field("node_id", &self.node_id)?;
		event.serialize_field("tenant_id", &self.tenant_id)?;
		event.serialize_field("record_identity", &self.record_identity)?;
		event.serialize_field("actor", &self.actor)?;
		event.serialize_field("outcome", &self.outcome)?;
		event.serialize_field("raft_log_index", &self.raft_log_index)?;
		event.serialize_field("message", &self.message)?;
		event.serialize_field("schema", &self.schema)?;
		event.serialize_field("parameters", &self.parameters)?;
		event.end()
	}
}

#[derive(Debug, Clone, Default)]
pub struct PreparedAuditEmission;

pub trait AuditSink: Send + Sync {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()>;

	fn emit_and_prepare(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		self.emit(event)?;
		Ok(PreparedAuditEmission)
	}

	fn emit_projected(
		&self,
		event: AuditEvent,
		prepared: &PreparedAuditEmission,
	) -> std::io::Result<()> {
		let _ = prepared;
		self.emit(event)
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		AuditEvidenceDisposition::none()
	}

	fn chain_head(&self) -> std::io::Result<Option<AuditChainHead>> {
		Ok(None)
	}

	fn last_materialized_raft_log_index(&self) -> std::io::Result<Option<u64>> {
		Ok(None)
	}
}

pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
	fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
		Ok(())
	}
}

#[derive(Default)]
pub struct AuditMaterializationTracker {
	in_flight_entries: AtomicU64,
	lag_entries: AtomicU64,
}

impl AuditMaterializationTracker {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn note_authoritative_event(&self) {
		self.in_flight_entries.fetch_add(1, Ordering::Relaxed);
	}

	pub fn note_materialized(&self) {
		self.decrement_in_flight();
	}

	pub fn note_materialization_failed(&self) {
		self.decrement_in_flight();
		let mut current = self.lag_entries.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_add(1);
			match self.lag_entries.compare_exchange(
				current,
				next,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => {
					metrics().audit_materialization_lag_entries.set(next);
					return;
				}
				Err(observed) => current = observed,
			}
		}
	}

	pub fn lag_entries(&self) -> u64 {
		self.lag_entries.load(Ordering::Relaxed)
	}

	#[cfg(test)]
	pub fn in_flight_entries(&self) -> u64 {
		self.in_flight_entries.load(Ordering::Relaxed)
	}

	fn decrement_in_flight(&self) {
		let mut current = self.in_flight_entries.load(Ordering::Relaxed);
		loop {
			let next = current.saturating_sub(1);
			match self.in_flight_entries.compare_exchange(
				current,
				next,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return,
				Err(observed) => current = observed,
			}
		}
	}
}

struct WendelonAuditObserver;

impl shared_audit::AuditObserver for WendelonAuditObserver {
	fn local_emit_failed(&self) {
		metrics().audit_sink_failures_local_total.inc();
	}

	fn node_local_emitted(&self) {
		metrics().audit_emit_node_local_total.inc();
	}

	fn raft_log_emitted(&self) {
		metrics().audit_emit_raft_log_total.inc();
	}

	fn archive_failed(&self) {
		metrics().audit_sink_failures_archive_total.inc();
	}

	fn archive_pending_records(&self, records: u64) {
		metrics().audit_archive_pending_records.set(records);
	}

	fn archived_segment(&self, bytes: u64) {
		metrics().audit_archived_segments_total.inc();
		metrics().audit_archived_bytes_total.add(bytes);
	}

	fn syslog_failed(&self) {
		metrics().audit_sink_failures_syslog_total.inc();
	}

	fn kafka_failed(&self) {
		metrics().audit_sink_failures_kafka_total.inc();
	}

	fn projection_failure_queue_dropped(&self, sink: &str) {
		match sink {
			"archive" => metrics()
				.audit_sink_failures_archive_queue_dropped_total
				.inc(),
			"syslog" => metrics()
				.audit_sink_failures_syslog_queue_dropped_total
				.inc(),
			"kafka" => metrics()
				.audit_sink_failures_kafka_queue_dropped_total
				.inc(),
			_ => {}
		}
	}
}

pub struct LocalAuditSink {
	inner: Arc<dyn shared_audit::AuditSink>,
}

impl LocalAuditSink {
	pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
		Self::open_with_archive(path, None)
	}

	pub fn open_with_archive(
		path: impl AsRef<Path>,
		archive: Option<ArchiveAuditConfig>,
	) -> std::io::Result<Self> {
		let inner = shared_audit::LocalAuditSink::open_with_archive(
			path,
			archive,
			wendelon_audit_format(),
			Arc::new(WendelonAuditObserver),
		)?;
		Ok(Self {
			inner: Arc::new(inner),
		})
	}
}

impl AuditSink for LocalAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.inner.emit(event.to_shared()?)
	}

	fn emit_and_prepare(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		self.inner.emit_and_prepare(event.to_shared()?)?;
		Ok(PreparedAuditEmission)
	}

	fn chain_head(&self) -> std::io::Result<Option<AuditChainHead>> {
		self.inner.chain_head()
	}

	fn last_materialized_raft_log_index(&self) -> std::io::Result<Option<u64>> {
		self.inner.last_materialized_raft_log_index()
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		self.inner.evidence_disposition()
	}
}

struct SharedAuditSinkAdapter {
	inner: Arc<dyn shared_audit::AuditSink>,
}

impl AuditSink for SharedAuditSinkAdapter {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.inner.emit(event.to_shared()?)
	}

	fn emit_and_prepare(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		self.inner.emit_and_prepare(event.to_shared()?)?;
		Ok(PreparedAuditEmission)
	}

	fn chain_head(&self) -> std::io::Result<Option<AuditChainHead>> {
		self.inner.chain_head()
	}

	fn last_materialized_raft_log_index(&self) -> std::io::Result<Option<u64>> {
		self.inner.last_materialized_raft_log_index()
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		self.inner.evidence_disposition()
	}
}

pub fn build_audit_sink(
	local_path: impl AsRef<Path>,
	config: &AuditSinksConfig,
) -> std::io::Result<Arc<dyn AuditSink>> {
	let inner = shared_audit::build_audit_sink(
		local_path,
		config,
		wendelon_audit_format(),
		Arc::new(WendelonAuditObserver),
	)?;
	Ok(Arc::new(SharedAuditSinkAdapter { inner }))
}

pub fn emit_signed_chain_root(
	sink: &dyn AuditSink,
	node_id: NodeId,
	signer: &AuditRootSigner,
) -> std::io::Result<()> {
	let Some(head) = sink.chain_head()? else {
		return Ok(());
	};
	let shared_node_id = shared_node_id(node_id);
	let signed = signer.sign_chain_head(wendelon_audit_format(), &shared_node_id, &head);
	sink.emit(
		AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::AuditChainRoot,
			node_id,
			"emitted signed audit chain root",
		)
		.with_parameter(
			"chain_head",
			serde_json::to_value(&signed).map_err(|err| {
				Error::other(format!("audit chain root serialization failed: {err}"))
			})?,
		),
	)
}

pub fn spawn_signed_chain_root_rollup(
	sink: Arc<dyn AuditSink>,
	node_id: NodeId,
	signer: AuditRootSigner,
	interval: std::time::Duration,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		let mut ticker = tokio::time::interval(interval);
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		ticker.tick().await;
		loop {
			ticker.tick().await;
			let _ = emit_signed_chain_root(&*sink, node_id, &signer);
		}
	})
}

pub fn verify_signed_chain_root(root: &SignedAuditChainRoot) -> std::io::Result<()> {
	shared_audit::verify_signed_chain_root(wendelon_audit_format(), root)
}

pub fn verify_local_audit_evidence(
	path: impl AsRef<Path>,
) -> std::io::Result<AuditVerificationReport> {
	shared_audit::verify_local_audit_evidence(wendelon_audit_format(), path)
}

pub fn replay_audit_chain(path: impl AsRef<Path>) -> std::io::Result<Option<AuditChainHead>> {
	shared_audit::replay_audit_chain(wendelon_audit_format(), path)
}

#[cfg(feature = "audit-kafka")]
pub struct KafkaAuditSink {
	inner: shared_audit::KafkaAuditSink,
}

#[cfg(feature = "audit-kafka")]
impl KafkaAuditSink {
	pub fn new(
		topic: impl Into<String>,
		producer: Arc<dyn KafkaProducer>,
		payload_mode: KafkaAuditPayloadMode,
	) -> Self {
		Self {
			inner: shared_audit::KafkaAuditSink::new(
				topic,
				producer,
				payload_mode,
				Arc::new(WendelonAuditObserver),
			),
		}
	}
}

#[cfg(feature = "audit-kafka")]
impl AuditSink for KafkaAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		shared_audit::AuditSink::emit(&self.inner, event.to_shared()?)
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		shared_audit::AuditSink::evidence_disposition(&self.inner)
	}
}

pub struct AuditClockDriftSubscriber {
	sink: Arc<dyn AuditSink>,
}

impl AuditClockDriftSubscriber {
	pub fn new(sink: Arc<dyn AuditSink>) -> Self {
		Self { sink }
	}
}

impl ClockDriftSubscriber for AuditClockDriftSubscriber {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent) {
		let audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::ClockDriftExceeded,
			event.local_node,
			"local clock drift exceeded the configured bound",
		)
		.with_outcome(AuditOutcome::Error)
		.with_parameter("local_physical_ms", event.local_physical_ms)
		.with_parameter("cluster_median_ms", event.cluster_median_ms)
		.with_parameter("offset_ms", event.offset_ms)
		.with_parameter("max_offset_ms", event.max_offset_ms);
		if let Err(err) = self.sink.emit(audit) {
			eprintln!("wendelon: audit emission failed for clock drift event: {err}");
		}
	}
}

#[cfg(test)]
pub struct InMemoryAuditSink {
	events: std::sync::Mutex<Vec<AuditEvent>>,
}

#[cfg(test)]
impl InMemoryAuditSink {
	pub fn new() -> Self {
		Self {
			events: std::sync::Mutex::new(Vec::new()),
		}
	}

	pub fn events(&self) -> Vec<AuditEvent> {
		self.events.lock().unwrap().clone()
	}
}

#[cfg(test)]
impl Default for InMemoryAuditSink {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
impl AuditSink for InMemoryAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.events.lock().unwrap().push(event);
		Ok(())
	}
}
