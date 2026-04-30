use crate::consensus::monitor::{ClockDriftEvent, ClockDriftSubscriber};
use crate::metrics::global as metrics;
use crate::network::tls::ActorIdentity;
use crate::schema::{Identity, NodeId, TenantId};
#[cfg(feature = "fips")]
use aws_lc_rs::digest::{SHA256, digest};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
#[cfg(feature = "audit-kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "audit-kafka")]
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

const AUDIT_SCHEMA_VERSION: u16 = 4;
const AUDIT_GENESIS_TAG: &[u8] = b"wendelon-audit-genesis-v1";
const AUDIT_CHAIN_ROOT_TAG: &[u8] = b"wendelon-audit-chain-root-v1";
const ULID_RANDOM_MASK: u128 = (1u128 << 80) - 1;
const ULID_TIME_MASK: u128 = (1u128 << 48) - 1;
const ULID_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
pub const DEFAULT_CHAIN_ROOT_ROLLUP_INTERVAL: std::time::Duration =
	std::time::Duration::from_secs(60);
pub const DEFAULT_AUDIT_ARCHIVE_MAX_SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
const COMPOSITE_AUDIT_PENDING_FAILURE_CAP: usize = 1024;
const LOCAL_AUDIT_ARCHIVE_PENDING_CAP: usize = 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditSinksConfig {
	pub archive: Option<ArchiveAuditConfig>,
	pub syslog: Option<SyslogAuditConfig>,
	pub kafka: Option<KafkaAuditConfig>,
}

impl AuditSinksConfig {
	pub fn is_empty(&self) -> bool {
		self.archive.is_none() && self.syslog.is_none() && self.kafka.is_none()
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveAuditConfig {
	pub directory: PathBuf,
	pub max_segment_bytes: u64,
	pub lock_mode: AuditArchiveLockMode,
	pub retain_days: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditArchiveLockMode {
	#[default]
	None,
	Governance,
	Compliance,
}

impl AuditArchiveLockMode {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Governance => "governance",
			Self::Compliance => "compliance",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyslogAuditConfig {
	pub socket_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaAuditConfig {
	pub brokers: String,
	pub topic: String,
	pub client_id: Option<String>,
	pub payload_mode: KafkaAuditPayloadMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaAuditPayloadMode {
	#[default]
	Distribution,
	Canonical,
}

impl KafkaAuditPayloadMode {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Distribution => "distribution",
			Self::Canonical => "canonical",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum AuditAuthority {
	#[serde(rename = "node_local")]
	NodeLocal,
	#[serde(rename = "raft_log")]
	RaftLog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum AuditAuthorityModel {
	#[serde(rename = "node_local_authoritative")]
	NodeLocalAuthoritative,
	#[serde(rename = "raft_authoritative_materialized")]
	RaftAuthoritativeMaterialized,
}

impl AuditAuthorityModel {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::NodeLocalAuthoritative => "node_local_authoritative",
			Self::RaftAuthoritativeMaterialized => "raft_authoritative_materialized",
		}
	}

	fn from_authority(authority: AuditAuthority) -> Self {
		match authority {
			AuditAuthority::NodeLocal => Self::NodeLocalAuthoritative,
			AuditAuthority::RaftLog => Self::RaftAuthoritativeMaterialized,
		}
	}
}

impl Serialize for AuditAuthorityModel {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

impl AuditAuthority {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::NodeLocal => "node_local",
			Self::RaftLog => "raft_log",
		}
	}
}

impl Serialize for AuditAuthority {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum AuditOutcome {
	#[serde(rename = "allowed")]
	Allowed,
	#[serde(rename = "denied")]
	Denied,
	#[serde(rename = "error")]
	Error,
	#[serde(rename = "break_glass")]
	BreakGlass,
}

impl AuditOutcome {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Allowed => "allowed",
			Self::Denied => "denied",
			Self::Error => "error",
			Self::BreakGlass => "break_glass",
		}
	}
}

impl Serialize for AuditOutcome {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditProjectionSinkKind {
	Syslog,
	Kafka,
}

impl AuditProjectionSinkKind {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Syslog => "syslog",
			Self::Kafka => "kafka",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvidenceDisposition {
	pub authoritative_local_chain: bool,
	pub archived_local_chain: bool,
	pub archive_lock_mode: Option<AuditArchiveLockMode>,
	pub kafka_canonical_chain: bool,
	pub projection_sinks: Vec<AuditProjectionSinkKind>,
}

impl AuditEvidenceDisposition {
	pub const fn authoritative_local_chain_only() -> Self {
		Self {
			authoritative_local_chain: true,
			archived_local_chain: false,
			archive_lock_mode: None,
			kafka_canonical_chain: false,
			projection_sinks: Vec::new(),
		}
	}

	pub const fn none() -> Self {
		Self {
			authoritative_local_chain: false,
			archived_local_chain: false,
			archive_lock_mode: None,
			kafka_canonical_chain: false,
			projection_sinks: Vec::new(),
		}
	}
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
	ControlRingEpochAdvanced,
	MvccSnapshotPruned,
	TenantCreated,
	TenantQuotaUpdated,
	TableRetentionPolicyUpdated,
	TableSchemaDescriptorUpdated,
	TenantSuspended,
	TenantResumed,
	TenantDeleteRequested,
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
			Self::ControlRingEpochAdvanced => "control_ring.epoch_advanced",
			Self::MvccSnapshotPruned => "mvcc.snapshot_pruned",
			Self::TenantCreated => "tenant.created",
			Self::TenantQuotaUpdated => "tenant.quota_updated",
			Self::TableRetentionPolicyUpdated => "table.retention_policy_updated",
			Self::TableSchemaDescriptorUpdated => "table.schema_descriptor_updated",
			Self::TenantSuspended => "tenant.suspended",
			Self::TenantResumed => "tenant.resumed",
			Self::TenantDeleteRequested => "tenant.delete_requested",
			Self::AuditChainRoot => "audit.chain_root",
			Self::AuditSinkFailure => "audit.sink_failure",
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
			"control_ring.epoch_advanced" => Ok(Self::ControlRingEpochAdvanced),
			"mvcc.snapshot_pruned" => Ok(Self::MvccSnapshotPruned),
			"tenant.created" => Ok(Self::TenantCreated),
			"tenant.quota_updated" => Ok(Self::TenantQuotaUpdated),
			"table.retention_policy_updated" => Ok(Self::TableRetentionPolicyUpdated),
			"table.schema_descriptor_updated" => Ok(Self::TableSchemaDescriptorUpdated),
			"tenant.suspended" => Ok(Self::TenantSuspended),
			"tenant.resumed" => Ok(Self::TenantResumed),
			"tenant.delete_requested" => Ok(Self::TenantDeleteRequested),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdministrativeControlAction {
	AdvanceEpoch,
	TenantCreate,
	TenantQuotaUpdate,
	TableRetentionPolicyUpdate,
	TableSchemaDescriptorUpdate,
	TenantSuspend,
	TenantResume,
	TenantDeleteRequest,
	TenantKekRotate,
	TenantKekRetire,
	ActorKeyRevoke,
	BreakGlassIssue,
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
		AuditAuthorityModel::from_authority(self.authority)
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

pub trait AuditSink: Send + Sync {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()>;

	fn emit_and_prepare(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		self.emit(event)?;
		Ok(PreparedAuditEmission::none())
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditChainHead {
	pub sequence_number: u64,
	pub record_hash: String,
	pub hash_algorithm_tag: String,
}

impl AuditChainHead {
	fn payload_bytes(&self, node_id: NodeId) -> Vec<u8> {
		format!(
			"{}|{}|{}|{}",
			node_id.format_wire(),
			self.sequence_number,
			self.record_hash,
			self.hash_algorithm_tag,
		)
		.into_bytes()
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

struct LocalAuditState {
	file: std::fs::File,
	sequence_number: u64,
	last_record_hash: Option<[u8; 32]>,
	last_materialized_raft_log_index: Option<u64>,
	archive: Option<ArchiveMirrorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArchivedAuditSegmentManifest {
	node_id: String,
	first_sequence_number: u64,
	last_sequence_number: u64,
	first_prev_hash: String,
	last_record_hash: String,
	hash_algorithm_tag: String,
	lock_mode: AuditArchiveLockMode,
	retain_days: Option<u32>,
	retain_until_unix_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
struct PendingArchivedRecord {
	record: PersistedAuditRecord,
	line: String,
}

struct ArchiveSegmentState {
	node_id: String,
	first_sequence_number: u64,
	last_sequence_number: u64,
	first_prev_hash: String,
	last_record_hash: String,
	hash_algorithm_tag: String,
	file: std::fs::File,
	tmp_path: PathBuf,
	current_size_bytes: u64,
}

struct ArchiveMirrorState {
	config: ArchiveAuditConfig,
	current_segment: Option<ArchiveSegmentState>,
	pending_records: VecDeque<PendingArchivedRecord>,
	pending_cap: usize,
}

pub struct LocalAuditSink {
	state: Mutex<LocalAuditState>,
}

impl LocalAuditSink {
	pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
		Self::open_with_archive(path, None)
	}

	pub fn open_with_archive(
		path: impl AsRef<Path>,
		archive: Option<ArchiveAuditConfig>,
	) -> std::io::Result<Self> {
		let path = path.as_ref();
		let recovered = recover_local_state(path)?;
		let file = OpenOptions::new().create(true).append(true).open(path)?;
		let archive = match archive {
			Some(config) => Some(ArchiveMirrorState::open(config)?),
			None => None,
		};
		Ok(Self {
			state: Mutex::new(LocalAuditState {
				file,
				sequence_number: recovered.sequence_number,
				last_record_hash: recovered.last_record_hash,
				last_materialized_raft_log_index: recovered.last_materialized_raft_log_index,
				archive,
			}),
		})
	}

	fn persist_event(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		let mut state = self.state.lock().expect("audit file mutex poisoned");
		let sequence_number = state.sequence_number.saturating_add(1);
		let prev_hash = state
			.last_record_hash
			.unwrap_or_else(|| genesis_hash(event.node_id));
		let canonical = CanonicalAuditRecord::from_event(&event, sequence_number, &prev_hash);
		let canonical_bytes = match serde_json::to_vec(&canonical) {
			Ok(bytes) => bytes,
			Err(err) => {
				metrics().audit_sink_failures_local_total.inc();
				return Err(Error::other(format!("audit json: {err}")));
			}
		};
		let record_hash = chain_hash(&canonical_bytes);
		let persisted = PersistedAuditRecord::from_canonical(canonical, &record_hash);
		let line = match serde_json::to_string(&persisted) {
			Ok(line) => line,
			Err(err) => {
				metrics().audit_sink_failures_local_total.inc();
				return Err(Error::other(format!("audit json line: {err}")));
			}
		};
		if let Err(err) = state.file.write_all(line.as_bytes()) {
			metrics().audit_sink_failures_local_total.inc();
			return Err(err);
		}
		if let Err(err) = state.file.write_all(b"\n") {
			metrics().audit_sink_failures_local_total.inc();
			return Err(err);
		}
		if let Err(err) = state.file.sync_data() {
			metrics().audit_sink_failures_local_total.inc();
			return Err(err);
		}
		state.sequence_number = sequence_number;
		state.last_record_hash = Some(record_hash);
		if event.authority == AuditAuthority::RaftLog {
			metrics().audit_emit_raft_log_total.inc();
			if let Some(raft_log_index) = event.raft_log_index {
				state.last_materialized_raft_log_index = Some(raft_log_index);
			}
		} else {
			metrics().audit_emit_node_local_total.inc();
		}
		if let Some(archive) = state.archive.as_mut() {
			if let Err(err) = archive.flush_pending() {
				metrics().audit_sink_failures_archive_total.inc();
				archive.queue_record(PendingArchivedRecord {
					record: persisted.clone(),
					line: line.clone(),
				});
				tracing::warn!(error = %err, "audit archive flush failed");
				return Ok(PreparedAuditEmission::persisted(persisted));
			}
			if let Err(err) = archive.mirror_record(&persisted, &line) {
				metrics().audit_sink_failures_archive_total.inc();
				archive.queue_record(PendingArchivedRecord {
					record: persisted.clone(),
					line: line.clone(),
				});
				tracing::warn!(error = %err, "audit archive mirror failed");
			}
		}
		Ok(PreparedAuditEmission::persisted(persisted))
	}
}

impl AuditSink for LocalAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.persist_event(event).map(|_| ())
	}

	fn emit_and_prepare(&self, event: AuditEvent) -> std::io::Result<PreparedAuditEmission> {
		self.persist_event(event)
	}

	fn chain_head(&self) -> std::io::Result<Option<AuditChainHead>> {
		let state = self.state.lock().expect("audit file mutex poisoned");
		Ok(state.last_record_hash.map(|record_hash| AuditChainHead {
			sequence_number: state.sequence_number,
			record_hash: hex_string(&record_hash),
			hash_algorithm_tag: chain_hash_algorithm_tag().to_string(),
		}))
	}

	fn last_materialized_raft_log_index(&self) -> std::io::Result<Option<u64>> {
		let state = self.state.lock().expect("audit file mutex poisoned");
		Ok(state.last_materialized_raft_log_index)
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		let state = self.state.lock().expect("audit file mutex poisoned");
		AuditEvidenceDisposition {
			authoritative_local_chain: true,
			archived_local_chain: state.archive.is_some(),
			archive_lock_mode: state
				.archive
				.as_ref()
				.map(|archive| archive.config.lock_mode),
			kafka_canonical_chain: false,
			projection_sinks: Vec::new(),
		}
	}
}

impl ArchiveMirrorState {
	fn open(config: ArchiveAuditConfig) -> std::io::Result<Self> {
		if config.max_segment_bytes == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"audit.archive.max_segment_bytes must be greater than zero",
			));
		}
		std::fs::create_dir_all(&config.directory)?;
		let current_segment = recover_archive_segment(&config.directory)?;
		let pending_records = VecDeque::new();
		metrics().audit_archive_pending_records.set(0);
		Ok(Self {
			config,
			current_segment,
			pending_records,
			pending_cap: LOCAL_AUDIT_ARCHIVE_PENDING_CAP,
		})
	}

	fn flush_pending(&mut self) -> std::io::Result<()> {
		while let Some(record) = self.pending_records.front().cloned() {
			self.write_record(&record)?;
			self.pending_records.pop_front();
			self.update_pending_gauge();
		}
		Ok(())
	}

	fn mirror_record(&mut self, record: &PersistedAuditRecord, line: &str) -> std::io::Result<()> {
		self.write_record(&PendingArchivedRecord {
			record: record.clone(),
			line: line.to_string(),
		})
	}

	fn queue_record(&mut self, record: PendingArchivedRecord) {
		if self.pending_records.len() >= self.pending_cap {
			self.pending_records.pop_front();
			metrics()
				.audit_sink_failures_archive_queue_dropped_total
				.inc();
		}
		self.pending_records.push_back(record);
		self.update_pending_gauge();
	}

	fn write_record(&mut self, pending: &PendingArchivedRecord) -> std::io::Result<()> {
		self.ensure_segment(&pending.record)?;
		let segment = self
			.current_segment
			.as_mut()
			.expect("archive segment should be open after ensure_segment");
		segment.file.write_all(pending.line.as_bytes())?;
		segment.file.write_all(b"\n")?;
		segment.file.sync_data()?;
		segment.last_sequence_number = pending.record.sequence_number;
		segment.last_record_hash = pending.record.record_hash.clone();
		segment.current_size_bytes = segment
			.current_size_bytes
			.saturating_add(pending.line.len() as u64 + 1);
		if segment.current_size_bytes >= self.config.max_segment_bytes {
			self.finalize_current_segment()?;
		}
		Ok(())
	}

	fn ensure_segment(&mut self, record: &PersistedAuditRecord) -> std::io::Result<()> {
		if self.current_segment.is_some() {
			return Ok(());
		}
		let tmp_path = archive_part_path(
			&self.config.directory,
			&record.node_id,
			record.sequence_number,
		);
		let file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&tmp_path)?;
		self.current_segment = Some(ArchiveSegmentState {
			node_id: record.node_id.clone(),
			first_sequence_number: record.sequence_number,
			last_sequence_number: record.sequence_number.saturating_sub(1),
			first_prev_hash: record.prev_hash.clone(),
			last_record_hash: record.prev_hash.clone(),
			hash_algorithm_tag: record.hash_algorithm_tag.clone(),
			file,
			tmp_path,
			current_size_bytes: 0,
		});
		Ok(())
	}

	fn finalize_current_segment(&mut self) -> std::io::Result<()> {
		let Some(segment) = self.current_segment.take() else {
			return Ok(());
		};
		segment.file.sync_data()?;
		let final_path = archive_final_path(
			&self.config.directory,
			&segment.node_id,
			segment.first_sequence_number,
			segment.last_sequence_number,
		);
		if final_path.exists() {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!(
					"audit archive segment already exists: {}",
					final_path.display()
				),
			));
		}
		drop(segment.file);
		std::fs::rename(&segment.tmp_path, &final_path)?;
		if self.config.lock_mode != AuditArchiveLockMode::None {
			let mut permissions = std::fs::metadata(&final_path)?.permissions();
			permissions.set_readonly(true);
			std::fs::set_permissions(&final_path, permissions)?;
		}
		let manifest = ArchivedAuditSegmentManifest {
			node_id: segment.node_id,
			first_sequence_number: segment.first_sequence_number,
			last_sequence_number: segment.last_sequence_number,
			first_prev_hash: segment.first_prev_hash,
			last_record_hash: segment.last_record_hash,
			hash_algorithm_tag: segment.hash_algorithm_tag,
			lock_mode: self.config.lock_mode,
			retain_days: self.config.retain_days,
			retain_until_unix_seconds: self.config.retain_days.map(retention_deadline_unix_seconds),
		};
		let manifest_path = archive_manifest_path(
			&self.config.directory,
			final_path
				.file_name()
				.and_then(|name| name.to_str())
				.ok_or_else(|| Error::other("archive segment filename must be valid utf-8"))?,
		);
		let manifest_bytes = serde_json::to_vec_pretty(&manifest)
			.map_err(|err| Error::other(format!("audit archive manifest json: {err}")))?;
		std::fs::write(&manifest_path, manifest_bytes)?;
		sync_dir(&self.config.directory)?;
		metrics().audit_archived_segments_total.inc();
		metrics()
			.audit_archived_bytes_total
			.add(segment.current_size_bytes);
		Ok(())
	}

	fn update_pending_gauge(&self) {
		metrics()
			.audit_archive_pending_records
			.set(self.pending_records.len() as u64);
	}
}

fn recover_archive_segment(directory: &Path) -> std::io::Result<Option<ArchiveSegmentState>> {
	let mut part_path: Option<PathBuf> = None;
	for entry in std::fs::read_dir(directory)? {
		let entry = entry?;
		let path = entry.path();
		let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
			continue;
		};
		if parse_archive_part_filename(name).is_some() {
			if part_path.replace(path).is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"multiple in-progress audit archive segments found",
				));
			}
		}
	}
	let Some(path) = part_path else {
		return Ok(None);
	};
	let (node_id, first_sequence_number) = parse_archive_part_filename(
		path.file_name()
			.and_then(|value| value.to_str())
			.ok_or_else(|| Error::other("archive segment filename must be valid utf-8"))?,
	)
	.ok_or_else(|| Error::new(ErrorKind::InvalidData, "invalid archive part filename"))?;
	let mut bytes = std::fs::read(&path)?;
	if !bytes.is_empty() && !bytes.ends_with(b"\n") {
		let retained_len = bytes
			.iter()
			.rposition(|byte| *byte == b'\n')
			.map(|idx| idx + 1)
			.unwrap_or(0);
		bytes.truncate(retained_len);
		let mut file = OpenOptions::new().write(true).truncate(true).open(&path)?;
		file.write_all(&bytes)?;
		file.sync_data()?;
		sync_dir(directory)?;
	}
	let text = String::from_utf8(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"invalid utf-8 in archived audit segment {}: {err}",
				path.display()
			),
		)
	})?;
	let mut last_record: Option<PersistedAuditRecord> = None;
	let mut first_prev_hash: Option<String> = None;
	for line in text.lines().filter(|line| !line.trim().is_empty()) {
		let record: PersistedAuditRecord = serde_json::from_str(line).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid archived audit record in {}: {err}", path.display()),
			)
		})?;
		first_prev_hash.get_or_insert_with(|| record.prev_hash.clone());
		last_record = Some(record);
	}
	if last_record.is_none() {
		std::fs::remove_file(&path)?;
		sync_dir(directory)?;
		return Ok(None);
	}
	let last_record = last_record.expect("checked is_some");
	let file = OpenOptions::new().append(true).open(&path)?;
	Ok(Some(ArchiveSegmentState {
		node_id,
		first_sequence_number,
		last_sequence_number: last_record.sequence_number,
		first_prev_hash: first_prev_hash.expect("archive part had at least one record"),
		last_record_hash: last_record.record_hash.clone(),
		hash_algorithm_tag: last_record.hash_algorithm_tag.clone(),
		file,
		tmp_path: path.clone(),
		current_size_bytes: std::fs::metadata(&path)?.len(),
	}))
}

fn archive_part_path(directory: &Path, node_id: &str, first_sequence_number: u64) -> PathBuf {
	directory.join(format!(
		"audit_{}_{}.ndjson.part",
		node_id, first_sequence_number
	))
}

fn archive_final_path(
	directory: &Path,
	node_id: &str,
	first_sequence_number: u64,
	last_sequence_number: u64,
) -> PathBuf {
	directory.join(format!(
		"audit_{}_{}_{}.ndjson",
		node_id, first_sequence_number, last_sequence_number
	))
}

fn archive_manifest_path(directory: &Path, segment_name: &str) -> PathBuf {
	directory.join(format!("{segment_name}.manifest.json"))
}

#[cfg(test)]
fn parse_archive_final_filename(name: &str) -> Option<(String, u64, u64)> {
	let rest = name.strip_prefix("audit_")?.strip_suffix(".ndjson")?;
	let (prefix, last_sequence_number) = rest.rsplit_once('_')?;
	let (node_id, first_sequence_number) = prefix.rsplit_once('_')?;
	Some((
		node_id.to_string(),
		first_sequence_number.parse().ok()?,
		last_sequence_number.parse().ok()?,
	))
}

fn parse_archive_part_filename(name: &str) -> Option<(String, u64)> {
	let rest = name.strip_prefix("audit_")?.strip_suffix(".ndjson.part")?;
	let (node_id, first_sequence_number) = rest.rsplit_once('_')?;
	Some((node_id.to_string(), first_sequence_number.parse().ok()?))
}

fn retention_deadline_unix_seconds(retain_days: u32) -> u64 {
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_secs();
	now.saturating_add((retain_days as u64).saturating_mul(24 * 60 * 60))
}

fn sync_dir(path: &Path) -> std::io::Result<()> {
	let dir = std::fs::File::open(path)?;
	dir.sync_all()
}

pub struct CompositeAuditSink {
	local: Arc<dyn AuditSink>,
	additional: Vec<NamedAuditSink>,
	pending_failures: Mutex<VecDeque<PendingAuditSinkFailure>>,
	pending_failure_cap: usize,
}

struct NamedAuditSink {
	name: String,
	sink: Arc<dyn AuditSink>,
}

#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "audit-kafka"), allow(dead_code))]
pub struct PreparedAuditEmission {
	persisted_record: Option<PersistedAuditRecord>,
}

#[cfg_attr(not(feature = "audit-kafka"), allow(dead_code))]
impl PreparedAuditEmission {
	fn none() -> Self {
		Self {
			persisted_record: None,
		}
	}

	fn persisted(record: PersistedAuditRecord) -> Self {
		Self {
			persisted_record: Some(record),
		}
	}

	fn persisted_record(&self) -> Option<&PersistedAuditRecord> {
		self.persisted_record.as_ref()
	}
}

impl NamedAuditSink {
	fn new(name: impl Into<String>, sink: Arc<dyn AuditSink>) -> Self {
		Self {
			name: name.into(),
			sink,
		}
	}
}

#[derive(Debug, Clone)]
struct PendingAuditSinkFailure {
	node_id: NodeId,
	sink: String,
	error: String,
	failed_event_type: String,
	failed_authority: AuditAuthority,
}

impl PendingAuditSinkFailure {
	fn into_event(self) -> AuditEvent {
		AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::AuditSinkFailure,
			self.node_id,
			format!(
				"sink={} failed_event_type={} failed_authority={} error={}",
				self.sink,
				self.failed_event_type,
				self.failed_authority.as_str(),
				self.error,
			),
		)
		.with_outcome(AuditOutcome::Error)
		.with_parameter("sink", self.sink)
		.with_parameter("error", self.error)
		.with_parameter("failed_event_type", self.failed_event_type)
		.with_parameter("failed_authority", self.failed_authority.as_str())
	}
}

impl CompositeAuditSink {
	fn new(local: Arc<dyn AuditSink>, additional: Vec<NamedAuditSink>) -> Self {
		Self::new_with_pending_failure_cap(local, additional, COMPOSITE_AUDIT_PENDING_FAILURE_CAP)
	}

	fn new_with_pending_failure_cap(
		local: Arc<dyn AuditSink>,
		additional: Vec<NamedAuditSink>,
		pending_failure_cap: usize,
	) -> Self {
		Self {
			local,
			additional,
			pending_failures: Mutex::new(VecDeque::new()),
			pending_failure_cap: pending_failure_cap.max(1),
		}
	}

	fn flush_pending_failures(&self) {
		let mut pending = self
			.pending_failures
			.lock()
			.expect("pending audit failure mutex poisoned");
		let mut retained = VecDeque::new();
		while let Some(failure) = pending.pop_front() {
			if self.local.emit(failure.clone().into_event()).is_err() {
				retained.push_back(failure);
				retained.extend(pending.drain(..));
				*pending = retained;
				return;
			}
		}
	}

	fn note_additional_failure(&self, sink_name: &str, event: &AuditEvent, error: &Error) {
		let mut pending = self
			.pending_failures
			.lock()
			.expect("pending audit failure mutex poisoned");
		if pending.len() >= self.pending_failure_cap {
			if let Some(dropped) = pending.pop_front() {
				note_pending_failure_queue_drop(&dropped.sink);
			}
		}
		pending.push_back(PendingAuditSinkFailure {
			node_id: event.node_id,
			sink: sink_name.to_string(),
			error: error.to_string(),
			failed_event_type: event.event_type().to_string(),
			failed_authority: event.authority,
		});
	}

	#[cfg(test)]
	fn pending_failure_count(&self) -> usize {
		self.pending_failures
			.lock()
			.expect("pending audit failure mutex poisoned")
			.len()
	}
}

fn note_pending_failure_queue_drop(sink_name: &str) {
	match sink_name {
		"syslog" => metrics()
			.audit_sink_failures_syslog_queue_dropped_total
			.inc(),
		"kafka" => metrics()
			.audit_sink_failures_kafka_queue_dropped_total
			.inc(),
		_ => {}
	}
}

impl AuditSink for CompositeAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		let prepared = self.local.emit_and_prepare(event.clone())?;
		self.flush_pending_failures();
		for sink in &self.additional {
			if let Err(error) = sink.sink.emit_projected(event.clone(), &prepared) {
				self.note_additional_failure(&sink.name, &event, &error);
			}
		}
		Ok(())
	}

	fn chain_head(&self) -> std::io::Result<Option<AuditChainHead>> {
		self.local.chain_head()
	}

	fn last_materialized_raft_log_index(&self) -> std::io::Result<Option<u64>> {
		self.local.last_materialized_raft_log_index()
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		let mut disposition = self.local.evidence_disposition();
		for sink in &self.additional {
			let sink_disposition = sink.sink.evidence_disposition();
			disposition.kafka_canonical_chain |= sink_disposition.kafka_canonical_chain;
			match sink.name.as_str() {
				"syslog" => disposition
					.projection_sinks
					.push(AuditProjectionSinkKind::Syslog),
				"kafka" => disposition
					.projection_sinks
					.push(AuditProjectionSinkKind::Kafka),
				_ => {}
			}
		}
		disposition
	}
}

pub fn build_audit_sink(
	local_path: impl AsRef<Path>,
	config: &AuditSinksConfig,
) -> std::io::Result<Arc<dyn AuditSink>> {
	let local: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open_with_archive(
		local_path,
		config.archive.clone(),
	)?);
	let mut additional: Vec<NamedAuditSink> = Vec::new();

	if let Some(syslog) = &config.syslog {
		#[cfg(unix)]
		{
			additional.push(NamedAuditSink::new(
				"syslog",
				Arc::new(SyslogAuditSink::connect(&syslog.socket_path)?),
			));
		}
		#[cfg(not(unix))]
		{
			return Err(Error::new(
				ErrorKind::Unsupported,
				"audit.syslog is supported only on unix platforms",
			));
		}
	}

	if let Some(kafka) = &config.kafka {
		#[cfg(feature = "audit-kafka")]
		{
			additional.push(NamedAuditSink::new(
				"kafka",
				Arc::new(kafka_audit_sink_from_config(kafka)?),
			));
		}
		#[cfg(not(feature = "audit-kafka"))]
		{
			let _ = kafka;
			return Err(Error::new(
				ErrorKind::Unsupported,
				"audit.kafka requires the audit-kafka cargo feature",
			));
		}
	}

	if additional.is_empty() {
		Ok(local)
	} else {
		Ok(Arc::new(CompositeAuditSink::new(local, additional)))
	}
}

#[cfg(unix)]
pub struct SyslogAuditSink {
	socket: Mutex<UnixDatagram>,
}

#[cfg(unix)]
impl SyslogAuditSink {
	pub fn connect(path: impl AsRef<Path>) -> std::io::Result<Self> {
		let socket = UnixDatagram::unbound()?;
		socket.connect(path)?;
		Ok(Self {
			socket: Mutex::new(socket),
		})
	}
}

#[cfg(unix)]
impl AuditSink for SyslogAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		let actor_role = event
			.actor
			.as_ref()
			.map(|actor| actor.role.as_str())
			.unwrap_or("-");
		let payload = serde_json::to_string(&event.parameters)
			.map_err(|err| Error::other(format!("syslog audit payload: {err}")))?;
		let line = format!(
			"<134>1 - wendelon - audit - - [wendelon@1 operation=\"{}\" authority=\"{}\" outcome=\"{}\" actor_role=\"{}\"] {} {}",
			event.operation.as_str(),
			event.authority.as_str(),
			event.outcome.as_str(),
			actor_role,
			event.message,
			payload,
		);
		let socket = self
			.socket
			.lock()
			.expect("syslog audit socket mutex poisoned");
		if let Err(err) = socket.send(line.as_bytes()) {
			metrics().audit_sink_failures_syslog_total.inc();
			return Err(err);
		}
		Ok(())
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		AuditEvidenceDisposition {
			authoritative_local_chain: false,
			archived_local_chain: false,
			archive_lock_mode: None,
			kafka_canonical_chain: false,
			projection_sinks: vec![AuditProjectionSinkKind::Syslog],
		}
	}
}

#[cfg(feature = "audit-kafka")]
pub trait KafkaProducer: Send + Sync {
	fn publish(&self, topic: &str, payload: &[u8]) -> std::io::Result<()>;
}

#[cfg(feature = "audit-kafka")]
type KafkaProducerFactory =
	dyn Fn(&KafkaAuditConfig) -> std::io::Result<Arc<dyn KafkaProducer>> + Send + Sync;

#[cfg(feature = "audit-kafka")]
static KAFKA_PRODUCER_FACTORY: OnceLock<Arc<KafkaProducerFactory>> = OnceLock::new();

#[cfg(feature = "audit-kafka")]
pub fn register_kafka_producer_factory(factory: Arc<KafkaProducerFactory>) -> std::io::Result<()> {
	KAFKA_PRODUCER_FACTORY.set(factory).map_err(|_| {
		Error::new(
			ErrorKind::AlreadyExists,
			"Kafka audit producer factory already registered",
		)
	})
}

#[cfg(feature = "audit-kafka")]
pub struct KafkaAuditSink {
	topic: String,
	producer: Arc<dyn KafkaProducer>,
	payload_mode: KafkaAuditPayloadMode,
}

#[cfg(feature = "audit-kafka")]
impl KafkaAuditSink {
	pub fn new(
		topic: impl Into<String>,
		producer: Arc<dyn KafkaProducer>,
		payload_mode: KafkaAuditPayloadMode,
	) -> Self {
		Self {
			topic: topic.into(),
			producer,
			payload_mode,
		}
	}
}

#[cfg(feature = "audit-kafka")]
fn kafka_audit_sink_from_config(config: &KafkaAuditConfig) -> std::io::Result<KafkaAuditSink> {
	let producer = if let Some(factory) = KAFKA_PRODUCER_FACTORY.get() {
		factory(config)?
	} else {
		Arc::new(RdkafkaAuditProducer::from_config(config)?)
	};
	Ok(KafkaAuditSink::new(
		config.topic.clone(),
		producer,
		config.payload_mode,
	))
}

#[cfg(feature = "audit-kafka")]
impl AuditSink for KafkaAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.emit_projected(event, &PreparedAuditEmission::none())
	}

	fn emit_projected(
		&self,
		event: AuditEvent,
		prepared: &PreparedAuditEmission,
	) -> std::io::Result<()> {
		let bytes = match self.payload_mode {
			KafkaAuditPayloadMode::Distribution => serde_json::to_vec(&event)
				.map_err(|err| Error::other(format!("kafka audit payload: {err}")))?,
			KafkaAuditPayloadMode::Canonical => {
				serde_json::to_vec(prepared.persisted_record().ok_or_else(|| {
					Error::other(
						"kafka canonical audit payload requires a persisted local-chain record",
					)
				})?)
				.map_err(|err| Error::other(format!("kafka audit payload: {err}")))?
			}
		};
		if let Err(err) = self.producer.publish(&self.topic, &bytes) {
			metrics().audit_sink_failures_kafka_total.inc();
			return Err(err);
		}
		Ok(())
	}

	fn evidence_disposition(&self) -> AuditEvidenceDisposition {
		AuditEvidenceDisposition {
			authoritative_local_chain: false,
			archived_local_chain: false,
			archive_lock_mode: None,
			kafka_canonical_chain: self.payload_mode == KafkaAuditPayloadMode::Canonical,
			projection_sinks: vec![AuditProjectionSinkKind::Kafka],
		}
	}
}

#[cfg(feature = "audit-kafka")]
struct RdkafkaAuditProducer {
	producer: ThreadedProducer<DefaultProducerContext>,
}

#[cfg(feature = "audit-kafka")]
impl RdkafkaAuditProducer {
	fn from_config(config: &KafkaAuditConfig) -> std::io::Result<Self> {
		let mut client = ClientConfig::new();
		client
			.set("bootstrap.servers", &config.brokers)
			.set(
				"client.id",
				config.client_id.as_deref().unwrap_or("wendelon-audit"),
			)
			.set("message.timeout.ms", "5000")
			.set("queue.buffering.max.ms", "100");
		let producer = client
			.create()
			.map_err(|err| Error::other(format!("rdkafka producer create failed: {err}")))?;
		Ok(Self { producer })
	}
}

#[cfg(feature = "audit-kafka")]
impl KafkaProducer for RdkafkaAuditProducer {
	fn publish(&self, topic: &str, payload: &[u8]) -> std::io::Result<()> {
		self.producer
			.send(BaseRecord::<(), [u8]>::to(topic).payload(payload))
			.map_err(|(err, _record)| Error::other(format!("rdkafka send failed: {err}")))
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

pub struct AuditRootSigner {
	signing_key: SigningKey,
}

impl AuditRootSigner {
	pub fn from_seed(seed: [u8; 32]) -> Self {
		Self {
			signing_key: SigningKey::from_bytes(&seed),
		}
	}

	pub fn verifying_key_hex(&self) -> String {
		hex_string(self.signing_key.verifying_key().as_bytes())
	}

	pub fn sign_chain_head(&self, node_id: NodeId, head: &AuditChainHead) -> SignedAuditChainRoot {
		let payload = signed_root_payload_bytes(node_id, head);
		let signature = self.signing_key.sign(&payload);
		SignedAuditChainRoot {
			node_id: node_id.format_wire(),
			sequence_number: head.sequence_number,
			chain_head_hash: head.record_hash.clone(),
			hash_algorithm_tag: head.hash_algorithm_tag.clone(),
			signature_algorithm_tag: "ed25519".to_string(),
			signature: hex_string(&signature.to_bytes()),
			public_key: self.verifying_key_hex(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedAuditChainRoot {
	pub node_id: String,
	pub sequence_number: u64,
	pub chain_head_hash: String,
	pub hash_algorithm_tag: String,
	pub signature_algorithm_tag: String,
	pub signature: String,
	pub public_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditVerificationIssue {
	pub line: usize,
	pub sequence_number: Option<u64>,
	pub code: String,
	pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditVerificationReport {
	pub path: String,
	pub total_lines: u64,
	pub parsed_records: u64,
	pub malformed_records: u64,
	pub first_sequence_number: Option<u64>,
	pub last_sequence_number: Option<u64>,
	pub node_local_records: u64,
	pub raft_log_records: u64,
	pub signed_root_records: u64,
	pub signed_roots_verified: u64,
	pub last_materialized_raft_log_index: Option<u64>,
	pub issues: Vec<AuditVerificationIssue>,
}

impl AuditVerificationReport {
	pub fn ok(&self) -> bool {
		self.issues.is_empty()
	}
}

pub fn emit_signed_chain_root(
	sink: &dyn AuditSink,
	node_id: NodeId,
	signer: &AuditRootSigner,
) -> std::io::Result<()> {
	let Some(head) = sink.chain_head()? else {
		return Ok(());
	};
	let signed = signer.sign_chain_head(node_id, &head);
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
	if root.signature_algorithm_tag != "ed25519" {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"unsupported audit chain root signature algorithm {}",
				root.signature_algorithm_tag
			),
		));
	}
	let node_id = NodeId::parse_wire(&root.node_id)?;
	let head = AuditChainHead {
		sequence_number: root.sequence_number,
		record_hash: root.chain_head_hash.clone(),
		hash_algorithm_tag: root.hash_algorithm_tag.clone(),
	};
	let payload = signed_root_payload_bytes(node_id, &head);
	let key_bytes = decode_hex(&root.public_key)?;
	let key_bytes: [u8; 32] = key_bytes.try_into().map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"audit chain root public key must be 32 bytes",
		)
	})?;
	let signature_bytes = decode_hex(&root.signature)?;
	let signature_bytes: [u8; 64] = signature_bytes.try_into().map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"audit chain root signature must be 64 bytes",
		)
	})?;
	let verifying_key = VerifyingKey::from_bytes(&key_bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("invalid verifying key: {err}"),
		)
	})?;
	let signature = Signature::from_bytes(&signature_bytes);
	verifying_key.verify(&payload, &signature).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("invalid audit root signature: {err}"),
		)
	})
}

pub fn verify_local_audit_evidence(
	path: impl AsRef<Path>,
) -> std::io::Result<AuditVerificationReport> {
	let path = path.as_ref();
	let text = std::fs::read_to_string(path)?;
	let mut report = AuditVerificationReport {
		path: path.display().to_string(),
		total_lines: 0,
		parsed_records: 0,
		malformed_records: 0,
		first_sequence_number: None,
		last_sequence_number: None,
		node_local_records: 0,
		raft_log_records: 0,
		signed_root_records: 0,
		signed_roots_verified: 0,
		last_materialized_raft_log_index: None,
		issues: Vec::new(),
	};
	let mut last_record_hash: Option<[u8; 32]> = None;
	let mut last_sequence_number: Option<u64> = None;
	let mut last_raft_log_index: Option<u64> = None;

	for (line_idx, line) in text.lines().enumerate() {
		if line.trim().is_empty() {
			continue;
		}
		report.total_lines = report.total_lines.saturating_add(1);
		let line_no = line_idx + 1;
		let record: PersistedAuditRecord = match serde_json::from_str(line) {
			Ok(record) => record,
			Err(err) => {
				report.malformed_records = report.malformed_records.saturating_add(1);
				push_verification_issue(
					&mut report,
					line_no,
					None,
					"malformed_json",
					format!("failed to parse audit line: {err}"),
				);
				continue;
			}
		};
		report.parsed_records = report.parsed_records.saturating_add(1);
		report
			.first_sequence_number
			.get_or_insert(record.sequence_number);
		report.last_sequence_number = Some(record.sequence_number);

		if let Some(previous) = last_sequence_number {
			if record.sequence_number != previous.saturating_add(1) {
				push_verification_issue(
					&mut report,
					line_no,
					Some(record.sequence_number),
					"sequence_gap",
					format!(
						"expected sequence {} but found {}",
						previous.saturating_add(1),
						record.sequence_number
					),
				);
			}
		} else if record.sequence_number != 1 {
			push_verification_issue(
				&mut report,
				line_no,
				Some(record.sequence_number),
				"non_genesis_sequence",
				format!(
					"first persisted sequence must be 1, found {}",
					record.sequence_number
				),
			);
		}
		last_sequence_number = Some(record.sequence_number);

		match record.authority {
			AuditAuthority::NodeLocal => {
				report.node_local_records = report.node_local_records.saturating_add(1);
			}
			AuditAuthority::RaftLog => {
				report.raft_log_records = report.raft_log_records.saturating_add(1);
				match record.raft_log_index {
					Some(index) => {
						if let Some(previous) = last_raft_log_index {
							if index < previous {
								push_verification_issue(
									&mut report,
									line_no,
									Some(record.sequence_number),
									"raft_log_index_regressed",
									format!(
										"raft_log_index regressed from {} to {}",
										previous, index
									),
								);
							}
						}
						last_raft_log_index = Some(index);
						report.last_materialized_raft_log_index = Some(index);
					}
					None => {
						push_verification_issue(
							&mut report,
							line_no,
							Some(record.sequence_number),
							"missing_raft_log_index",
							"raft-authoritative record is missing raft_log_index".to_string(),
						);
					}
				}
			}
		}

		let expected_prev_hash = last_record_hash.unwrap_or_else(|| record.genesis_hash());
		match decode_fixed_hash(&record.prev_hash) {
			Ok(actual_prev_hash) => {
				if actual_prev_hash != expected_prev_hash {
					push_verification_issue(
						&mut report,
						line_no,
						Some(record.sequence_number),
						"prev_hash_mismatch",
						format!("prev_hash mismatch at sequence {}", record.sequence_number),
					);
				}
			}
			Err(err) => {
				report.malformed_records = report.malformed_records.saturating_add(1);
				push_verification_issue(
					&mut report,
					line_no,
					Some(record.sequence_number),
					"invalid_prev_hash",
					format!("invalid prev_hash: {err}"),
				);
			}
		}

		let canonical = record.to_canonical();
		let canonical_bytes = serde_json::to_vec(&canonical)
			.map_err(|err| Error::other(format!("audit verify canonical json: {err}")))?;
		let computed_hash = chain_hash(&canonical_bytes);
		match decode_fixed_hash(&record.record_hash) {
			Ok(actual_hash) => {
				if actual_hash != computed_hash {
					push_verification_issue(
						&mut report,
						line_no,
						Some(record.sequence_number),
						"record_hash_mismatch",
						format!(
							"record_hash mismatch at sequence {}",
							record.sequence_number
						),
					);
				}
				last_record_hash = Some(actual_hash);
			}
			Err(err) => {
				report.malformed_records = report.malformed_records.saturating_add(1);
				push_verification_issue(
					&mut report,
					line_no,
					Some(record.sequence_number),
					"invalid_record_hash",
					format!("invalid record_hash: {err}"),
				);
				last_record_hash = None;
			}
		}

		if record.operation == AuditOperation::AuditChainRoot {
			report.signed_root_records = report.signed_root_records.saturating_add(1);
			match record.parameters.get("chain_head") {
				Some(value) => {
					match serde_json::from_value::<SignedAuditChainRoot>(value.clone()) {
						Ok(root) => {
							let mut root_ok = true;
							if let Err(err) = verify_signed_chain_root(&root) {
								root_ok = false;
								push_verification_issue(
									&mut report,
									line_no,
									Some(record.sequence_number),
									"invalid_signed_root",
									format!("invalid signed chain root: {err}"),
								);
							}
							if root.node_id != record.node_id {
								root_ok = false;
								push_verification_issue(
									&mut report,
									line_no,
									Some(record.sequence_number),
									"signed_root_node_mismatch",
									format!(
										"signed root node_id {} does not match record node_id {}",
										root.node_id, record.node_id
									),
								);
							}
							if root.sequence_number != record.sequence_number.saturating_sub(1) {
								root_ok = false;
								push_verification_issue(
									&mut report,
									line_no,
									Some(record.sequence_number),
									"signed_root_sequence_mismatch",
									format!(
										"signed root sequence {} does not match previous record sequence {}",
										root.sequence_number,
										record.sequence_number.saturating_sub(1)
									),
								);
							}
							if root.chain_head_hash != record.prev_hash {
								root_ok = false;
								push_verification_issue(
								&mut report,
								line_no,
								Some(record.sequence_number),
								"signed_root_hash_anchor_mismatch",
								"signed root chain_head_hash does not match this record's prev_hash"
									.to_string(),
							);
							}
							if root_ok {
								report.signed_roots_verified =
									report.signed_roots_verified.saturating_add(1);
							}
						}
						Err(err) => {
							report.malformed_records = report.malformed_records.saturating_add(1);
							push_verification_issue(
								&mut report,
								line_no,
								Some(record.sequence_number),
								"malformed_signed_root",
								format!("malformed chain_head payload: {err}"),
							);
						}
					}
				}
				None => {
					report.malformed_records = report.malformed_records.saturating_add(1);
					push_verification_issue(
						&mut report,
						line_no,
						Some(record.sequence_number),
						"missing_signed_root",
						"audit.chain_root record is missing parameters.chain_head".to_string(),
					);
				}
			}
		}
	}

	Ok(report)
}

fn push_verification_issue(
	report: &mut AuditVerificationReport,
	line: usize,
	sequence_number: Option<u64>,
	code: &str,
	message: String,
) {
	report.issues.push(AuditVerificationIssue {
		line,
		sequence_number,
		code: code.to_string(),
		message,
	});
}

fn decode_fixed_hash(raw: &str) -> std::io::Result<[u8; 32]> {
	let bytes = decode_hex(raw)?;
	bytes.try_into().map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"audit hash must be exactly 32 bytes",
		)
	})
}

pub fn replay_audit_chain(path: impl AsRef<Path>) -> std::io::Result<Option<AuditChainHead>> {
	let path = path.as_ref();
	if !path.exists() {
		return Ok(None);
	}
	let text = std::fs::read_to_string(path)?;
	let mut last_record_hash: Option<[u8; 32]> = None;
	let mut head: Option<AuditChainHead> = None;
	for line in text.lines().filter(|line| !line.trim().is_empty()) {
		let record: PersistedAuditRecord = match serde_json::from_str(line) {
			Ok(record) => record,
			Err(_) => continue,
		};
		let expected_prev_hash = last_record_hash.unwrap_or_else(|| record.genesis_hash());
		let actual_prev_hash = decode_hex(&record.prev_hash)?;
		if actual_prev_hash != expected_prev_hash {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"audit chain break at sequence {}: prev_hash mismatch",
					record.sequence_number
				),
			));
		}
		let canonical = record.to_canonical();
		let canonical_bytes = serde_json::to_vec(&canonical)
			.map_err(|err| Error::other(format!("audit replay canonical json: {err}")))?;
		let computed_hash = chain_hash(&canonical_bytes);
		let actual_hash = decode_hex(&record.record_hash)?;
		if actual_hash != computed_hash {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"audit chain break at sequence {}: record_hash mismatch",
					record.sequence_number
				),
			));
		}
		last_record_hash = Some(computed_hash);
		head = Some(AuditChainHead {
			sequence_number: record.sequence_number,
			record_hash: record.record_hash,
			hash_algorithm_tag: record.hash_algorithm_tag,
		});
	}
	Ok(head)
}

struct RecoveredLocalState {
	sequence_number: u64,
	last_record_hash: Option<[u8; 32]>,
	last_materialized_raft_log_index: Option<u64>,
}

fn recover_local_state(path: &Path) -> std::io::Result<RecoveredLocalState> {
	if !path.exists() {
		return Ok(RecoveredLocalState {
			sequence_number: 0,
			last_record_hash: None,
			last_materialized_raft_log_index: None,
		});
	}
	let text = std::fs::read_to_string(path)?;
	let mut sequence_number = 0;
	let mut last_record_hash = None;
	let mut last_materialized_raft_log_index = None;
	for (line_no, line) in text
		.lines()
		.filter(|line| !line.trim().is_empty())
		.enumerate()
	{
		let record: PersistedAuditRecord = match serde_json::from_str(line) {
			Ok(record) => record,
			Err(err) => {
				tracing::warn!(
					path = %path.display(),
					line_no = line_no + 1,
					error = %err,
					"skipping malformed persisted audit record during local recovery"
				);
				continue;
			}
		};
		sequence_number = record.sequence_number;
		let record_hash = decode_hex(&record.record_hash)?;
		let record_hash: [u8; 32] = record_hash.try_into().map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				"audit record_hash must be exactly 32 bytes",
			)
		})?;
		last_record_hash = Some(record_hash);
		if record.authority == AuditAuthority::RaftLog {
			if let Some(raft_log_index) = record.raft_log_index {
				last_materialized_raft_log_index = Some(raft_log_index);
			}
		}
	}
	Ok(RecoveredLocalState {
		sequence_number,
		last_record_hash,
		last_materialized_raft_log_index,
	})
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CanonicalAuditRecord {
	event_id: String,
	timestamp_ns: u64,
	sequence_number: u64,
	schema_version: u16,
	hash_algorithm_tag: String,
	prev_hash: String,
	authority: AuditAuthority,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	authority_model: Option<AuditAuthorityModel>,
	outcome: AuditOutcome,
	operation: AuditOperation,
	event_type: String,
	node_id: String,
	tenant_id: Option<u64>,
	record_identity: Option<String>,
	actor: Option<AuditActor>,
	actor_role: Option<String>,
	actor_trust_domain: Option<String>,
	actor_key_id: Option<String>,
	break_glass: bool,
	raft_log_index: Option<u64>,
	message: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	schema: Option<AuditSchema>,
	parameters: BTreeMap<String, Value>,
}

impl CanonicalAuditRecord {
	fn from_event(event: &AuditEvent, sequence_number: u64, prev_hash: &[u8; 32]) -> Self {
		let actor_role = event.actor.as_ref().map(|actor| actor.role.clone());
		let actor_trust_domain = event.actor.as_ref().map(|actor| actor.trust_domain.clone());
		let actor_key_id = event.actor.as_ref().map(|actor| actor.key_id.clone());
		let break_glass = event
			.actor
			.as_ref()
			.map(|actor| actor.break_glass)
			.unwrap_or(false);
		Self {
			event_id: monotonic_ulid(),
			timestamp_ns: current_time_ns(),
			sequence_number,
			schema_version: AUDIT_SCHEMA_VERSION,
			hash_algorithm_tag: chain_hash_algorithm_tag().to_string(),
			prev_hash: hex_string(prev_hash),
			authority: event.authority,
			authority_model: Some(event.authority_model()),
			outcome: event.outcome,
			operation: event.operation,
			event_type: event.event_type().to_string(),
			node_id: event.node_id.format_wire(),
			tenant_id: event.tenant_id.map(|tenant_id| tenant_id.get()),
			record_identity: event
				.record_identity
				.as_ref()
				.map(|identity| hex_string(identity.as_bytes())),
			actor: event.actor.clone(),
			actor_role,
			actor_trust_domain,
			actor_key_id,
			break_glass,
			raft_log_index: event.raft_log_index,
			message: event.message.clone(),
			schema: event.schema.clone(),
			parameters: event.parameters.clone(),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAuditRecord {
	event_id: String,
	timestamp_ns: u64,
	sequence_number: u64,
	schema_version: u16,
	hash_algorithm_tag: String,
	prev_hash: String,
	record_hash: String,
	authority: AuditAuthority,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	authority_model: Option<AuditAuthorityModel>,
	outcome: AuditOutcome,
	operation: AuditOperation,
	event_type: String,
	node_id: String,
	tenant_id: Option<u64>,
	record_identity: Option<String>,
	actor: Option<AuditActor>,
	actor_role: Option<String>,
	actor_trust_domain: Option<String>,
	actor_key_id: Option<String>,
	break_glass: bool,
	raft_log_index: Option<u64>,
	message: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	schema: Option<AuditSchema>,
	parameters: BTreeMap<String, Value>,
}

impl PersistedAuditRecord {
	fn from_canonical(canonical: CanonicalAuditRecord, record_hash: &[u8; 32]) -> Self {
		Self {
			event_id: canonical.event_id,
			timestamp_ns: canonical.timestamp_ns,
			sequence_number: canonical.sequence_number,
			schema_version: canonical.schema_version,
			hash_algorithm_tag: canonical.hash_algorithm_tag,
			prev_hash: canonical.prev_hash,
			record_hash: hex_string(record_hash),
			authority: canonical.authority,
			authority_model: canonical.authority_model,
			outcome: canonical.outcome,
			operation: canonical.operation,
			event_type: canonical.event_type,
			node_id: canonical.node_id,
			tenant_id: canonical.tenant_id,
			record_identity: canonical.record_identity,
			actor: canonical.actor,
			actor_role: canonical.actor_role,
			actor_trust_domain: canonical.actor_trust_domain,
			actor_key_id: canonical.actor_key_id,
			break_glass: canonical.break_glass,
			raft_log_index: canonical.raft_log_index,
			message: canonical.message,
			schema: canonical.schema,
			parameters: canonical.parameters,
		}
	}

	fn genesis_hash(&self) -> [u8; 32] {
		genesis_hash(NodeId::parse_wire(&self.node_id).expect("persisted node_id must be valid"))
	}

	fn to_canonical(&self) -> CanonicalAuditRecord {
		CanonicalAuditRecord {
			event_id: self.event_id.clone(),
			timestamp_ns: self.timestamp_ns,
			sequence_number: self.sequence_number,
			schema_version: self.schema_version,
			hash_algorithm_tag: self.hash_algorithm_tag.clone(),
			prev_hash: self.prev_hash.clone(),
			authority: self.authority,
			authority_model: self.authority_model,
			outcome: self.outcome,
			operation: self.operation,
			event_type: self.event_type.clone(),
			node_id: self.node_id.clone(),
			tenant_id: self.tenant_id,
			record_identity: self.record_identity.clone(),
			actor: self.actor.clone(),
			actor_role: self.actor_role.clone(),
			actor_trust_domain: self.actor_trust_domain.clone(),
			actor_key_id: self.actor_key_id.clone(),
			break_glass: self.break_glass,
			raft_log_index: self.raft_log_index,
			message: self.message.clone(),
			schema: self.schema.clone(),
			parameters: self.parameters.clone(),
		}
	}
}

fn current_time_ns() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos() as u64)
		.unwrap_or(0)
}

fn monotonic_ulid() -> String {
	static STATE: OnceLock<Mutex<(u64, u128)>> = OnceLock::new();
	let now_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_millis() as u64)
		.unwrap_or(0);
	let state = STATE.get_or_init(|| Mutex::new((0, 0)));
	let mut guard = state.lock().expect("ulid state mutex poisoned");
	let mut time_ms = guard.0.max(now_ms);
	let random = if time_ms > guard.0 {
		rand::random::<u128>() & ULID_RANDOM_MASK
	} else if guard.1 == ULID_RANDOM_MASK {
		time_ms = time_ms.saturating_add(1);
		0
	} else {
		guard.1 + 1
	};
	guard.0 = time_ms;
	guard.1 = random;
	let value = (((time_ms as u128) & ULID_TIME_MASK) << 80) | random;
	encode_ulid(value)
}

fn encode_ulid(mut value: u128) -> String {
	let mut out = [0u8; 26];
	for slot in out.iter_mut().rev() {
		*slot = ULID_ALPHABET[(value & 0x1f) as usize];
		value >>= 5;
	}
	String::from_utf8(out.to_vec()).expect("ULID alphabet is ASCII")
}

fn signed_root_payload_bytes(node_id: NodeId, head: &AuditChainHead) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(
		AUDIT_CHAIN_ROOT_TAG.len() + std::mem::size_of::<u32>() + head.record_hash.len() + 32,
	);
	bytes.extend_from_slice(AUDIT_CHAIN_ROOT_TAG);
	bytes.extend_from_slice(&node_id.id.to_le_bytes());
	bytes.extend_from_slice(&head.payload_bytes(node_id));
	bytes
}

#[cfg(feature = "fips")]
fn chain_hash_algorithm_tag() -> &'static str {
	"sha256"
}

#[cfg(not(feature = "fips"))]
fn chain_hash_algorithm_tag() -> &'static str {
	"blake3-256"
}

#[cfg(feature = "fips")]
fn chain_hash(bytes: &[u8]) -> [u8; 32] {
	let digest = digest(&SHA256, bytes);
	let mut out = [0u8; 32];
	out.copy_from_slice(digest.as_ref());
	out
}

#[cfg(not(feature = "fips"))]
fn chain_hash(bytes: &[u8]) -> [u8; 32] {
	crate::hash::blake3::digest(bytes)
}

fn genesis_hash(node_id: NodeId) -> [u8; 32] {
	let mut bytes = Vec::with_capacity(AUDIT_GENESIS_TAG.len() + std::mem::size_of::<u32>());
	bytes.extend_from_slice(AUDIT_GENESIS_TAG);
	bytes.extend_from_slice(&node_id.id.to_le_bytes());
	chain_hash(&bytes)
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to string cannot fail");
	}
	out
}

fn decode_hex(raw: &str) -> std::io::Result<Vec<u8>> {
	if raw.len() % 2 != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("hex string {raw:?} has odd length"),
		));
	}
	let mut out = Vec::with_capacity(raw.len() / 2);
	let bytes = raw.as_bytes();
	for idx in (0..bytes.len()).step_by(2) {
		let hi = decode_hex_nibble(bytes[idx])?;
		let lo = decode_hex_nibble(bytes[idx + 1])?;
		out.push((hi << 4) | lo);
	}
	Ok(out)
}

fn decode_hex_nibble(byte: u8) -> std::io::Result<u8> {
	match byte {
		b'0'..=b'9' => Ok(byte - b'0'),
		b'a'..=b'f' => Ok(byte - b'a' + 10),
		b'A'..=b'F' => Ok(byte - b'A' + 10),
		_ => Err(Error::new(
			ErrorKind::InvalidData,
			format!("invalid hex byte 0x{byte:02x}"),
		)),
	}
}

#[cfg(test)]
pub struct InMemoryAuditSink {
	events: Mutex<Vec<AuditEvent>>,
}

#[cfg(test)]
impl InMemoryAuditSink {
	pub fn new() -> Self {
		Self {
			events: Mutex::new(Vec::new()),
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

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::{Path, PathBuf};
	use uuid::Uuid;

	fn fresh_audit_path(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"wendelon-audit-{tag}-{}-{}",
			std::process::id(),
			Uuid::new_v4()
		));
		std::fs::create_dir_all(&dir).unwrap();
		dir.join("audit.ndjson")
	}

	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}

	fn archive_lines_in_order(directory: &Path) -> Vec<String> {
		let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(directory)
			.unwrap()
			.filter_map(|entry| {
				let path = entry.ok()?.path();
				let name = path.file_name()?.to_str()?;
				if let Some((_node_id, first, _last)) = parse_archive_final_filename(name) {
					return Some((first, path));
				}
				if let Some((_node_id, first)) = parse_archive_part_filename(name) {
					return Some((first, path));
				}
				None
			})
			.collect();
		files.sort_by_key(|(first, _)| *first);
		let mut lines = Vec::new();
		for (_first, path) in files {
			lines.extend(
				std::fs::read_to_string(path)
					.unwrap()
					.lines()
					.filter(|line| !line.trim().is_empty())
					.map(ToOwned::to_owned),
			);
		}
		lines
	}

	#[test]
	fn local_audit_sink_recovers_sequence_and_chain_head_across_reopen() {
		let path = fresh_audit_path("reopen");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoStartup,
			node(7),
			"selected_cipher=aes256-gcm",
		))
		.unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::ObservabilityMetrics,
			node(7),
			"served /metrics scrape",
		))
		.unwrap();
		drop(sink);

		let reopened = LocalAuditSink::open(&path).unwrap();
		reopened
			.emit(AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::ObservabilityReady,
				node(7),
				"served /ready snapshot",
			))
			.unwrap();

		let head = replay_audit_chain(&path).unwrap().unwrap();
		assert_eq!(head.sequence_number, 3);
		assert_eq!(head.hash_algorithm_tag, chain_hash_algorithm_tag());
	}

	#[test]
	fn audit_event_serializes_typed_schema_detail() {
		let json = serde_json::to_value(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::TenantCreated,
				node(3),
				"tenant created",
			)
			.with_schema(AuditSchema::AdministrativeControl {
				action: AdministrativeControlAction::TenantCreate,
				detail: AdministrativeControlSchema {
					target_tenant_id: Some(42),
					..AdministrativeControlSchema::default()
				},
			}),
		)
		.unwrap();

		assert_eq!(json["schema"]["type"], "administrative_control");
		assert_eq!(json["schema"]["detail"]["action"], "tenant_create");
		assert_eq!(json["schema"]["detail"]["target_tenant_id"], 42);
	}

	#[test]
	fn audit_event_serializes_authority_model() {
		let json = serde_json::to_value(AuditEvent::new(
			AuditAuthority::RaftLog,
			AuditOperation::TenantCreated,
			node(3),
			"tenant created",
		))
		.unwrap();
		assert_eq!(json["authority"], "raft_log");
		assert_eq!(json["authority_model"], "raft_authoritative_materialized");
	}

	#[test]
	fn replay_audit_chain_detects_tamper() {
		let path = fresh_audit_path("tamper");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoStartup,
			node(9),
			"selected_cipher=aes256-gcm",
		))
		.unwrap();
		let text = std::fs::read_to_string(&path).unwrap();
		let tampered = text.replace("aes256-gcm", "tampered-cipher");
		std::fs::write(&path, tampered).unwrap();

		let err = replay_audit_chain(&path).unwrap_err();
		assert!(err.to_string().contains("record_hash mismatch"));
	}

	#[test]
	fn replay_audit_chain_detects_tamper_in_denormalized_fields() {
		let path = fresh_audit_path("tamper-denormalized");
		let sink = LocalAuditSink::open(&path).unwrap();
		let actor = crate::network::tls::ActorIdentity {
			role: crate::network::tls::ActorRole::BreakGlassAdmin,
			trust_domain: crate::network::tls::ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: vec!["cluster".to_string()],
			key_id: vec![1, 2, 3, 4],
			break_glass: true,
		};
		sink.emit(
			AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::ObservabilityMetrics,
				node(9),
				"served /metrics scrape",
			)
			.with_actor(&actor),
		)
		.unwrap();

		let text = std::fs::read_to_string(&path).unwrap();
		let mut record: serde_json::Value =
			serde_json::from_str(text.lines().next().unwrap()).unwrap();
		record["break_glass"] = serde_json::Value::Bool(false);
		std::fs::write(
			&path,
			format!("{}\n", serde_json::to_string(&record).unwrap()),
		)
		.unwrap();

		let err = replay_audit_chain(&path).unwrap_err();
		assert!(err.to_string().contains("record_hash mismatch"));
	}

	#[test]
	fn signed_chain_root_is_emitted_and_verifiable() {
		let path = fresh_audit_path("signed-root");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoStartup,
			node(11),
			"selected_cipher=aes256-gcm",
		))
		.unwrap();

		let signer = AuditRootSigner::from_seed([11; 32]);
		emit_signed_chain_root(&sink, node(11), &signer).unwrap();

		let text = std::fs::read_to_string(&path).unwrap();
		let record: PersistedAuditRecord =
			serde_json::from_str(text.lines().last().unwrap()).unwrap();
		let signed: SignedAuditChainRoot =
			serde_json::from_value(record.parameters["chain_head"].clone()).unwrap();
		verify_signed_chain_root(&signed).unwrap();
	}

	#[test]
	fn verify_local_audit_evidence_reports_clean_chain_and_signed_root() {
		let path = fresh_audit_path("verify-clean");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoStartup,
			node(12),
			"selected_cipher=aes256-gcm",
		))
		.unwrap();
		sink.emit(
			AuditEvent::new(
				AuditAuthority::RaftLog,
				AuditOperation::ControlRingEpochAdvanced,
				node(12),
				"advanced epoch",
			)
			.with_raft_log_index(44),
		)
		.unwrap();
		let signer = AuditRootSigner::from_seed([12; 32]);
		emit_signed_chain_root(&sink, node(12), &signer).unwrap();

		let report = verify_local_audit_evidence(&path).unwrap();
		assert!(report.ok(), "issues={:?}", report.issues);
		assert_eq!(report.total_lines, 3);
		assert_eq!(report.parsed_records, 3);
		assert_eq!(report.malformed_records, 0);
		assert_eq!(report.node_local_records, 2);
		assert_eq!(report.raft_log_records, 1);
		assert_eq!(report.signed_root_records, 1);
		assert_eq!(report.signed_roots_verified, 1);
		assert_eq!(report.last_materialized_raft_log_index, Some(44));
	}

	#[test]
	fn verify_local_audit_evidence_reports_malformed_and_tampered_lines() {
		let path = fresh_audit_path("verify-bad");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::CryptoStartup,
			node(13),
			"selected_cipher=aes256-gcm",
		))
		.unwrap();
		let mut lines: Vec<String> = std::fs::read_to_string(&path)
			.unwrap()
			.lines()
			.map(ToOwned::to_owned)
			.collect();
		lines.push("{not-json".to_string());
		let mut tampered: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
		tampered["record_hash"] = serde_json::Value::String("00".repeat(32));
		lines.push(serde_json::to_string(&tampered).unwrap());
		std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

		let report = verify_local_audit_evidence(&path).unwrap();
		assert!(!report.ok());
		assert!(
			report
				.issues
				.iter()
				.any(|issue| issue.code == "malformed_json")
		);
		assert!(
			report
				.issues
				.iter()
				.any(|issue| issue.code == "record_hash_mismatch")
		);
	}

	#[test]
	fn audit_materialization_tracker_only_counts_persistent_lag_entries() {
		let tracker = AuditMaterializationTracker::new();
		assert_eq!(tracker.lag_entries(), 0);
		tracker.note_authoritative_event();
		assert_eq!(tracker.in_flight_entries(), 1);
		assert_eq!(tracker.lag_entries(), 0);
		tracker.note_materialized();
		assert_eq!(tracker.in_flight_entries(), 0);
		assert_eq!(tracker.lag_entries(), 0);
		tracker.note_authoritative_event();
		tracker.note_materialization_failed();
		assert_eq!(tracker.lag_entries(), 1);
		assert_eq!(tracker.in_flight_entries(), 0);
	}

	#[test]
	fn composite_sink_audits_additional_sink_failure_on_next_success() {
		struct FlakySink {
			remaining_failures: Mutex<usize>,
		}

		impl FlakySink {
			fn new(remaining_failures: usize) -> Self {
				Self {
					remaining_failures: Mutex::new(remaining_failures),
				}
			}
		}

		impl AuditSink for FlakySink {
			fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
				let mut remaining = self.remaining_failures.lock().unwrap();
				if *remaining > 0 {
					*remaining -= 1;
					return Err(Error::other("simulated syslog failure"));
				}
				Ok(())
			}
		}

		let path = fresh_audit_path("composite-failure-audit");
		let local: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&path).unwrap());
		let composite = CompositeAuditSink::new(
			local,
			vec![NamedAuditSink::new("syslog", Arc::new(FlakySink::new(1)))],
		);

		composite
			.emit(AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::CryptoStartup,
				node(14),
				"selected_cipher=aes256-gcm",
			))
			.unwrap();
		let first = std::fs::read_to_string(&path).unwrap();
		assert_eq!(
			first
				.matches("\"event_type\":\"audit.sink_failure\"")
				.count(),
			0
		);

		composite
			.emit(AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::ObservabilityMetrics,
				node(14),
				"served /metrics scrape",
			))
			.unwrap();

		let text = std::fs::read_to_string(&path).unwrap();
		assert_eq!(
			text.matches("\"event_type\":\"audit.sink_failure\"")
				.count(),
			1
		);
		assert!(text.contains("\"sink\":\"syslog\""));
		assert!(text.contains("\"failed_event_type\":\"crypto.startup\""));
		assert!(text.contains("\"failed_authority\":\"node_local\""));
		assert!(text.contains("simulated syslog failure"));
	}

	#[test]
	fn local_and_composite_sinks_expose_evidence_disposition() {
		let path = fresh_audit_path("evidence-disposition");
		let local: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&path).unwrap());
		assert_eq!(
			local.evidence_disposition(),
			AuditEvidenceDisposition::authoritative_local_chain_only()
		);

		let composite = CompositeAuditSink::new(
			local,
			vec![NamedAuditSink::new("syslog", Arc::new(NoopAuditSink))],
		);
		assert_eq!(
			composite.evidence_disposition(),
			AuditEvidenceDisposition {
				authoritative_local_chain: true,
				archived_local_chain: false,
				archive_lock_mode: None,
				kafka_canonical_chain: false,
				projection_sinks: vec![AuditProjectionSinkKind::Syslog],
			}
		);
	}

	#[test]
	fn local_audit_sink_mirrors_archive_segments_and_worm_manifest() {
		let path = fresh_audit_path("archive-mirror");
		let archive_dir = path.parent().unwrap().join("archive");
		let sink = LocalAuditSink::open_with_archive(
			&path,
			Some(ArchiveAuditConfig {
				directory: archive_dir.clone(),
				max_segment_bytes: 700,
				lock_mode: AuditArchiveLockMode::Governance,
				retain_days: Some(30),
			}),
		)
		.unwrap();
		for idx in 0..6u32 {
			sink.emit(AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::ObservabilityMetrics,
				node(33),
				format!("served /metrics scrape idx={idx} {}", "x".repeat(180)),
			))
			.unwrap();
		}
		drop(sink);

		let mut finalized_segments: Vec<PathBuf> = std::fs::read_dir(&archive_dir)
			.unwrap()
			.filter_map(|entry| {
				let path = entry.ok()?.path();
				let name = path.file_name()?.to_str()?;
				parse_archive_final_filename(name).map(|_| path)
			})
			.collect();
		finalized_segments.sort();
		assert!(
			!finalized_segments.is_empty(),
			"expected at least one finalized archive segment"
		);
		let manifest_path = archive_manifest_path(
			&archive_dir,
			finalized_segments[0]
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap(),
		);
		let manifest: ArchivedAuditSegmentManifest =
			serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
		assert_eq!(manifest.lock_mode, AuditArchiveLockMode::Governance);
		assert_eq!(manifest.retain_days, Some(30));
		assert!(
			std::fs::metadata(&finalized_segments[0])
				.unwrap()
				.permissions()
				.readonly()
		);

		let replay_path = path.parent().unwrap().join("archive-replay.ndjson");
		std::fs::write(
			&replay_path,
			format!("{}\n", archive_lines_in_order(&archive_dir).join("\n")),
		)
		.unwrap();
		let report = verify_local_audit_evidence(&replay_path).unwrap();
		assert!(report.ok(), "archive report issues={:?}", report.issues);
	}

	#[test]
	fn recover_archive_segment_truncates_partial_crash_tail_line() {
		let path = fresh_audit_path("archive-crash-tail");
		let sink = LocalAuditSink::open(&path).unwrap();
		sink.emit(AuditEvent::new(
			AuditAuthority::NodeLocal,
			AuditOperation::ObservabilityMetrics,
			node(34),
			"served /metrics scrape",
		))
		.unwrap();
		drop(sink);

		let full_line = std::fs::read_to_string(&path).unwrap();
		let record: PersistedAuditRecord = serde_json::from_str(full_line.trim_end()).unwrap();
		let archive_dir = path.parent().unwrap().join("archive-crash-tail");
		std::fs::create_dir_all(&archive_dir).unwrap();
		let part_path = archive_part_path(&archive_dir, &record.node_id, record.sequence_number);
		std::fs::write(&part_path, format!("{full_line}{{\"truncated\":")).unwrap();

		let recovered = recover_archive_segment(&archive_dir).unwrap().unwrap();
		assert_eq!(recovered.last_sequence_number, record.sequence_number);
		assert_eq!(recovered.current_size_bytes, full_line.len() as u64);
		drop(recovered);

		assert_eq!(std::fs::read_to_string(&part_path).unwrap(), full_line);
	}

	#[test]
	fn composite_sink_caps_pending_failure_queue_and_counts_drops() {
		struct RejectFailureEventLocalSink;

		impl AuditSink for RejectFailureEventLocalSink {
			fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
				if event.operation == AuditOperation::AuditSinkFailure {
					return Err(Error::other("simulated local sink failure"));
				}
				Ok(())
			}
		}

		struct AlwaysFailSink;

		impl AuditSink for AlwaysFailSink {
			fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
				Err(Error::other("simulated syslog failure"))
			}
		}

		let before = metrics()
			.audit_sink_failures_syslog_queue_dropped_total
			.get();
		let composite = CompositeAuditSink::new_with_pending_failure_cap(
			Arc::new(RejectFailureEventLocalSink),
			vec![NamedAuditSink::new("syslog", Arc::new(AlwaysFailSink))],
			2,
		);

		for idx in 0..4 {
			composite
				.emit(AuditEvent::new(
					AuditAuthority::NodeLocal,
					AuditOperation::CryptoStartup,
					node(15),
					format!("event-{idx}"),
				))
				.unwrap();
		}

		assert_eq!(composite.pending_failure_count(), 2);
		assert_eq!(
			metrics()
				.audit_sink_failures_syslog_queue_dropped_total
				.get() - before,
			2
		);
	}

	#[cfg(feature = "audit-kafka")]
	#[test]
	fn kafka_audit_sink_serializes_full_event() {
		struct RecordingProducer(Mutex<Vec<Vec<u8>>>);

		impl KafkaProducer for RecordingProducer {
			fn publish(&self, _topic: &str, payload: &[u8]) -> std::io::Result<()> {
				self.0.lock().unwrap().push(payload.to_vec());
				Ok(())
			}
		}

		let producer = Arc::new(RecordingProducer(Mutex::new(Vec::new())));
		let sink = KafkaAuditSink::new(
			"wendelon.audit",
			producer.clone(),
			KafkaAuditPayloadMode::Distribution,
		);
		sink.emit(
			AuditEvent::new(
				AuditAuthority::NodeLocal,
				AuditOperation::ObservabilityMetrics,
				node(12),
				"served /metrics scrape",
			)
			.with_parameter("path", "/metrics"),
		)
		.unwrap();

		let payloads = producer.0.lock().unwrap();
		let json: serde_json::Value = serde_json::from_slice(&payloads[0]).unwrap();
		assert_eq!(json["event_type"], "observability.metrics");
		assert_eq!(json["parameters"]["path"], "/metrics");
	}

	#[cfg(feature = "audit-kafka")]
	#[test]
	fn composite_sink_can_publish_canonical_kafka_payload_matching_local_chain() {
		struct RecordingProducer(Mutex<Vec<Vec<u8>>>);

		impl KafkaProducer for RecordingProducer {
			fn publish(&self, _topic: &str, payload: &[u8]) -> std::io::Result<()> {
				self.0.lock().unwrap().push(payload.to_vec());
				Ok(())
			}
		}

		let path = fresh_audit_path("kafka-canonical");
		let local = Arc::new(LocalAuditSink::open(&path).unwrap());
		let producer = Arc::new(RecordingProducer(Mutex::new(Vec::new())));
		let composite = CompositeAuditSink::new(
			local,
			vec![NamedAuditSink::new(
				"kafka",
				Arc::new(KafkaAuditSink::new(
					"wendelon.audit",
					producer.clone(),
					KafkaAuditPayloadMode::Canonical,
				)),
			)],
		);
		composite
			.emit(
				AuditEvent::new(
					AuditAuthority::NodeLocal,
					AuditOperation::ObservabilityMetrics,
					node(44),
					"served /metrics scrape",
				)
				.with_parameter("path", "/metrics"),
			)
			.unwrap();

		let payloads = producer.0.lock().unwrap();
		assert_eq!(payloads.len(), 1);
		let kafka_line = String::from_utf8(payloads[0].clone()).unwrap();
		let local_text = std::fs::read_to_string(&path).unwrap();
		let local_line = local_text.lines().next().unwrap().to_string();
		assert_eq!(kafka_line, local_line);

		let kafka_path = fresh_audit_path("kafka-canonical-replay");
		std::fs::write(&kafka_path, format!("{kafka_line}\n")).unwrap();
		let report = verify_local_audit_evidence(&kafka_path).unwrap();
		assert!(report.ok(), "issues={:?}", report.issues);

		assert!(composite.evidence_disposition().kafka_canonical_chain);
	}
}
