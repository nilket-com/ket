#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!("features `fips` and `non-fips` are mutually exclusive");

use arrow::array::{Array, LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, CounterAddRequest,
	CounterReadRequest, DATA_RETRY_BUDGET, MutationOutcomePinRequest, MutationOutcomeRequest,
	ReadRequest, TableDeleteRowsRequest, TableQueryRequest, TableWriteRequest,
	WendelonClient as InnerClient, WendelonClientConfig, WriteRequest,
};
use wendelon::network::control_server::{
	CONTROL_REASON_NOT_LEADER, CtasSagaRequestWire, CtasSagaResponseWire, SchemaUpdateResponseWire,
	TableSchemaDropResponseWire,
};
pub use wendelon::network::control_server::{
	MAINTENANCE_CONTROL_MAX_REQUEST_BYTES, MAINTENANCE_CONTROL_WIRE_VERSION,
	MaintenanceCompletionAuditWire, MaintenanceOriginWire, MaintenanceReportItemWire,
	MaintenanceReportOutcomeWire, MaintenanceReportRequestWire, MaintenanceReportResponseWire,
	MaintenanceRequeueOutcomeWire, MaintenanceRequeueRequestWire, MaintenanceRequeueResponseWire,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::factory::TransportProviderFactory;
use wendelon::network::pool::BufPool;
use wendelon::network::quic_transport::{
	QuicTransportProvider, canonical_server_name_resolver, client_endpoint_from_tls,
};
use wendelon::network::tls::{DEFAULT_CLUSTER_DOMAIN, TlsMaterial};
use wendelon::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, NodeRole, NodeRoleSet, TransportProvider,
	TransportTier, TransportTierSet,
};
use wendelon::network::wire;
use wendelon::schema::{
	ClientWriteId, CounterAddRequestMeta, CounterReadRequestMeta, FinalizationMarker, FrameHeader,
	HybridTimestamp, Identity, MsgType, MutationCommitKnowledge, MutationOperationKind,
	MutationOutcomePinAction, MutationResolutionStatus, MutationStopReason, NodeId,
	QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING, ReadRequestMeta, ReadStatus,
	TABLE_QUERY_TRACE_FLAG_PRESENT, TABLE_QUERY_TRACE_FLAG_SAMPLED, TableDeleteRowsRequestMeta,
	TableQueryCancelResponseMeta, TableQueryRequestId, TableQueryRequestMeta,
	TableWriteRequestMeta, TenantId, WriteRequestMeta, WriteStatus,
	encode_table_schema_descriptor_v1, encode_table_write_row_count, read_key_schema,
	write_payload_schema,
};
use wendelon::storage::counter::{CounterKey, counter_identity};

pub use wendelon::client::{
	CdcFrame, CdcSubscription, QueryLocalityRange, RowIdentityProjection, StreamingQueryResult,
	TableQueryAttempt as WendelonTableQueryAttempt, TableQueryAttemptGuard,
	TableWriteSectionEncodeError, encode_table_write_arrow_section,
};
pub use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, ComputeQuerySessionHandoff, ComputeQuerySessionIssueRequest,
	ComputeTraceContext, DropOperationId, LOGICAL_TYPE_INTERNAL_NAMESPACE,
	LOGICAL_TYPE_RESERVED_STATIC_MAX_ID, LogicalTypeDefinition, LogicalTypeDefinitionCreate,
	LogicalTypePhysical, LogicalTypeRefinementComparisonOp, LogicalTypeRefinementPredicate,
	LogicalTypeRefinementValue, LogicalTypeUnit, SchemaGeneration, SchemaIntentId,
	SchemaIntentOrigin, SchemaMutationKind, SchemaObjectKind, SchemaObjectRef,
	SchemaUpdateEnvelope, StorageQuotaMetadata, StorageStatusKind, TableSchemaDropFields,
	TableSchemaDropRequest, UnitOffset, UnitQuantityKind, UnitScale, WendelonAdminError,
	decode_logical_type_definition, decode_logical_type_definitions,
	decode_schema_update_envelope_v1, encode_logical_type_definition_create,
	encode_logical_type_definitions, encode_schema_update_envelope_v1, map_read_status_for_compute,
	map_write_status_for_compute, prepare_query_session_handoff,
	validate_query_session_audit_attribution, validate_traceparent,
};
pub use wendelon::schema::{
	ARROW_STATS_TAG_BINARY, ARROW_STATS_TAG_BOOLEAN, ARROW_STATS_TAG_DATE32,
	ARROW_STATS_TAG_DATE64, ARROW_STATS_TAG_FLOAT32, ARROW_STATS_TAG_FLOAT64, ARROW_STATS_TAG_INT8,
	ARROW_STATS_TAG_INT16, ARROW_STATS_TAG_INT32, ARROW_STATS_TAG_INT64,
	ARROW_STATS_TAG_LARGE_BINARY, ARROW_STATS_TAG_LARGE_UTF8, ARROW_STATS_TAG_OPAQUE_UNSUPPORTED,
	ARROW_STATS_TAG_TIMESTAMP_MICROSECOND, ARROW_STATS_TAG_TIMESTAMP_MILLISECOND,
	ARROW_STATS_TAG_TIMESTAMP_NANOSECOND, ARROW_STATS_TAG_UINT8, ARROW_STATS_TAG_UINT16,
	ARROW_STATS_TAG_UINT32, ARROW_STATS_TAG_UINT64, ARROW_STATS_TAG_UTF8, CdcCursor, CdcRejectCode,
	CdcSubscribeBound, ColumnGroupNdvFact, ColumnGroupTopKEntry, ColumnGroupTopKFact,
	ColumnHeavyHitterEntry, ColumnHeavyHitterFact, ColumnNdvFact, ColumnRangeDistributionFact,
	ColumnRangeFact, Consistency as ReadConsistency, CtasExpectedBinding, CtasIntentFingerprint,
	CtasOperationId, CtasSagaEntry, CtasSagaMutation, CtasSagaPhase, CtasSagaReject,
	CtasSagaResponse, CtasTerminalCause, DurabilityLevel as DurabilityClass, IndexObjectId,
	LocalityPolicy, LogicalSchemaFingerprint, NdvFact, PbcRejectCode,
	PhysicalDescriptorFingerprint, PrimaryKeyDescriptor, PrimaryKeyEncoding,
	RangeDistributionBucket, RowIdentityPolicy, RuntimeFilterDescriptor, SchemaEvolutionPolicy,
	SecondaryIndexDescriptor, SecondaryIndexDropRequest, SecondaryIndexStatus, StateDigest,
	TableObjectId, TableOptimizerFacts, TableSchemaDescriptor, TableSpreadLocalityDescriptor,
	decode_float32_optimizer_fact_scalar, decode_float64_optimizer_fact_scalar,
	decode_signed_optimizer_fact_scalar, encode_float32_optimizer_fact_scalar,
	encode_float64_optimizer_fact_scalar, encode_secondary_index_descriptor,
	encode_secondary_index_drop_request, encode_signed_optimizer_fact_scalar,
};

pub use wendelon::schema::{
	CTAS_MAX_WRITE_CHUNKS, CTAS_SAGA_CAPACITY, CTAS_SAGA_DETAIL_RETENTION_MS,
	MATERIALIZATION_CERTIFICATE_MAX_BYTES,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapPeer {
	pub node_id: u32,
	pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCreateRequest {
	pub index_object_id: IndexObjectId,
	pub namespace: String,
	pub table_name: String,
	pub index_name: String,
	pub column_name: String,
	pub intent_id: SchemaIntentId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsPaths {
	pub cert_path: PathBuf,
	pub key_path: PathBuf,
	pub ca_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
	pub bind_addr: SocketAddr,
	pub bootstrap: Vec<BootstrapPeer>,
	pub tls: TlsPaths,
	pub cluster_domain: String,
	pub pool_max_retained: usize,
	pub pool_max_cap_bytes: usize,
}

impl ClientConfig {
	pub fn new(bind_addr: SocketAddr, bootstrap: Vec<BootstrapPeer>, tls: TlsPaths) -> Self {
		Self {
			bind_addr,
			bootstrap,
			tls,
			cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
			pool_max_retained: 64,
			pool_max_cap_bytes: 8 * 1024 * 1024,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOptions {
	pub durability: DurabilityClass,
	pub await_finalization: bool,
	pub client_hlc: Option<HybridTimestamp>,
	/// Absolute caller deadline. The core client derives a fresh remaining duration
	/// before every attempt; it never places this process-local instant on the wire.
	pub deadline: Option<std::time::Instant>,
	/// Caller-minted logical-operation identity (a UUIDv7 `client_uuid`). When
	/// absent, `table_write` and `table_delete` mint a fresh UUIDv7 per call
	/// (`sequence_no` 0, a 60 s TTL), so its issuance time is the operation's own:
	/// the server proves a v7 identity expired from that time, and an identity
	/// minted with the long-lived client would expire with the client's age.
	/// The SDK's in-call retries resend the same identity; a new call is a new
	/// operation. Blob writes and counter adds keep the per-client sequence.
	pub operation_id: Option<ClientWriteId>,
}

impl Default for WriteOptions {
	fn default() -> Self {
		Self {
			durability: DurabilityClass::QUORUM,
			await_finalization: false,
			client_hlc: None,
			deadline: None,
			operation_id: None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
	pub consistency: ReadConsistency,
	pub min_hlc: Option<HybridTimestamp>,
	pub as_of_hlc: Option<HybridTimestamp>,
}

impl Default for ReadOptions {
	fn default() -> Self {
		Self {
			consistency: ReadConsistency::LINEARIZABLE,
			min_hlc: None,
			as_of_hlc: None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryOptions {
	pub range: QueryLocalityRange,
	pub consistency: ReadConsistency,
	pub min_hlc: Option<HybridTimestamp>,
	pub as_of_hlc: Option<HybridTimestamp>,
	pub stats_pruning_enabled: bool,
}

impl Default for QueryOptions {
	fn default() -> Self {
		Self {
			range: QueryLocalityRange::full(),
			consistency: ReadConsistency::LINEARIZABLE,
			min_hlc: None,
			as_of_hlc: None,
			stats_pruning_enabled: true,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableQueryOptions {
	pub consistency: ReadConsistency,
	pub min_hlc: Option<HybridTimestamp>,
	pub as_of_hlc: Option<HybridTimestamp>,
	pub stats_pruning_enabled: bool,
	pub physical_descriptor_fingerprint: Option<PhysicalDescriptorFingerprint>,
	pub row_identity: RowIdentityProjection,
	pub trace_context: TableQueryTraceContext,
	pub row_limit: Option<NonZeroU64>,
	/// ADR 541: the caller's absolute end-to-end deadline, kept as a local monotonic
	/// instant. Each attempt transmits the REMAINING duration measured from this
	/// original instant, so retries consume the deadline and can never reset it.
	pub deadline: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableQueryTraceContext {
	#[default]
	Absent,
	Unsampled,
	Sampled {
		trace_id: [u8; 16],
		parent_span_id: [u8; 8],
	},
}

impl Default for TableQueryOptions {
	fn default() -> Self {
		Self {
			consistency: ReadConsistency::LINEARIZABLE,
			min_hlc: None,
			as_of_hlc: None,
			stats_pruning_enabled: true,
			physical_descriptor_fingerprint: None,
			row_identity: RowIdentityProjection::None,
			trace_context: TableQueryTraceContext::Absent,
			row_limit: None,
			deadline: None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteReceipt {
	pub identity: Identity,
	pub duplicate: bool,
	pub requested_durability: DurabilityClass,
	pub log_index: u64,
	pub term: u64,
	pub epoch: u64,
	pub commit_hlc: HybridTimestamp,
	pub finalization: Option<FinalizationMarker>,
}

/// Outcome of an idempotent atomic `counter_fetch_add` (ADR 075). `duplicate`
/// marks a replayed `write_id` (the values are the original add's, not re-applied).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterAddReceipt {
	pub identity: Identity,
	pub previous_value: u64,
	pub current_value: u64,
	pub duplicate: bool,
	pub requested_durability: DurabilityClass,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableWriteReceipt {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub rows: usize,
	pub duplicate: bool,
	pub requested_durability: DurabilityClass,
	pub log_index: u64,
	pub term: u64,
	pub epoch: u64,
	pub commit_hlc: HybridTimestamp,
	pub finalization: Option<FinalizationMarker>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableDeleteRowsReceipt {
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub requested: usize,
	pub deleted_count: u32,
	pub already_deleted_count: u32,
	pub not_found_count: u32,
	pub duplicate: bool,
	pub requested_durability: DurabilityClass,
	pub log_index: u64,
	pub term: u64,
	pub epoch: u64,
	pub commit_hlc: HybridTimestamp,
	pub finalization: Option<FinalizationMarker>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedMutationOutcome {
	Present {
		operation_id: ClientWriteId,
		status: WriteStatus,
		reason: MutationStopReason,
		knowledge: MutationCommitKnowledge,
		delete_counts: Option<[u32; 3]>,
	},
	Expired {
		operation_id: ClientWriteId,
	},
	Unknown {
		operation_id: ClientWriteId,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchemaUpdateRequest {
	pub tenant_id: TenantId,
	pub descriptor: TableSchemaDescriptor,
	pub expected_generation: SchemaGeneration,
	pub mutation: SchemaMutationKind,
	pub intent_id: SchemaIntentId,
}

impl TableSchemaUpdateRequest {
	pub fn create(
		tenant_id: TenantId,
		descriptor: TableSchemaDescriptor,
		intent_id: SchemaIntentId,
	) -> Self {
		Self {
			tenant_id,
			descriptor,
			expected_generation: SchemaGeneration::ZERO,
			mutation: SchemaMutationKind::Create,
			intent_id,
		}
	}

	pub fn alter(
		tenant_id: TenantId,
		descriptor: TableSchemaDescriptor,
		expected_generation: impl Into<SchemaGeneration>,
		intent_id: SchemaIntentId,
	) -> Self {
		Self {
			tenant_id,
			descriptor,
			expected_generation: expected_generation.into(),
			mutation: SchemaMutationKind::Alter,
			intent_id,
		}
	}

	pub fn envelope(&self) -> std::io::Result<SchemaUpdateEnvelope> {
		if self.mutation == SchemaMutationKind::Drop {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"table schema drops require a rewrite migration and are not supported by this helper",
			));
		}
		let payload = encode_table_schema_descriptor_v1(&self.descriptor)?;
		Ok(SchemaUpdateEnvelope::compute_client_intent(
			self.tenant_id,
			SchemaObjectRef {
				kind: SchemaObjectKind::Table,
				namespace: self.descriptor.namespace.clone(),
				name: self.descriptor.table_name.clone(),
			},
			self.expected_generation,
			SchemaGeneration::new(self.descriptor.schema_generation),
			self.intent_id,
			self.mutation,
			payload,
		))
	}

	pub fn encode(&self) -> std::io::Result<Vec<u8>> {
		encode_schema_update_envelope_v1(&self.envelope()?)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalTypeDefinitionCreateRequest {
	pub tenant_id: TenantId,
	pub name: String,
	pub physical: LogicalTypePhysical,
	pub refinement: Option<LogicalTypeRefinementPredicate>,
	pub unit: Option<LogicalTypeUnit>,
	pub intent_id: SchemaIntentId,
}

impl LogicalTypeDefinitionCreateRequest {
	pub fn create(
		tenant_id: TenantId,
		name: impl Into<String>,
		physical: LogicalTypePhysical,
		intent_id: SchemaIntentId,
	) -> Self {
		Self {
			tenant_id,
			name: name.into(),
			physical,
			refinement: None,
			unit: None,
			intent_id,
		}
	}

	pub fn create_with_refinement(
		tenant_id: TenantId,
		name: impl Into<String>,
		physical: LogicalTypePhysical,
		refinement: Option<LogicalTypeRefinementPredicate>,
		intent_id: SchemaIntentId,
	) -> Self {
		Self {
			tenant_id,
			name: name.into(),
			physical,
			refinement,
			unit: None,
			intent_id,
		}
	}

	pub fn create_with_metadata(
		tenant_id: TenantId,
		name: impl Into<String>,
		physical: LogicalTypePhysical,
		refinement: Option<LogicalTypeRefinementPredicate>,
		unit: Option<LogicalTypeUnit>,
		intent_id: SchemaIntentId,
	) -> Self {
		Self {
			tenant_id,
			name: name.into(),
			physical,
			refinement,
			unit,
			intent_id,
		}
	}

	pub fn envelope(&self) -> std::io::Result<SchemaUpdateEnvelope> {
		let payload = encode_logical_type_definition_create(&LogicalTypeDefinitionCreate {
			name: self.name.clone(),
			physical: self.physical,
			refinement: self.refinement.clone(),
			unit: self.unit.clone(),
		})?;
		Ok(SchemaUpdateEnvelope::compute_client_intent(
			self.tenant_id,
			SchemaObjectRef {
				kind: SchemaObjectKind::LogicalType,
				namespace: LOGICAL_TYPE_INTERNAL_NAMESPACE.to_string(),
				name: self.name.clone(),
			},
			SchemaGeneration::ZERO,
			SchemaGeneration::new(1),
			self.intent_id,
			SchemaMutationKind::Create,
			payload,
		))
	}

	pub fn encode(&self) -> std::io::Result<Vec<u8>> {
		encode_schema_update_envelope_v1(&self.envelope()?)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaUpdateReceipt {
	pub leader_hint: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadValue {
	pub identity: Identity,
	pub epoch: u64,
	pub value_hlc: HybridTimestamp,
	pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct QueryResult {
	pub epoch: u64,
	pub value_hlc: HybridTimestamp,
	pub rows: u64,
	pub batches: Vec<RecordBatch>,
}

/// The typed counterpart of a terminal
/// `TABLE_REFERENCE_INTEGRITY_FAILED` query response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableReferenceIntegrityFailure {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub reference_kind: u8,
	pub missing_slab_id: u64,
	pub origin: u8,
}

#[derive(Debug, Error)]
pub enum ClientError {
	#[error(transparent)]
	Io(#[from] std::io::Error),
	#[error("quota exceeded for tenant {tenant_id:?}")]
	QuotaExceeded { tenant_id: TenantId },
	#[error("tenant {tenant_id:?} is suspended")]
	TenantSuspended { tenant_id: TenantId },
	#[error("tenant {tenant_id:?} is unavailable")]
	TenantUnavailable { tenant_id: TenantId },
	#[error("authorization failed")]
	AuthorizationFailed,
	#[error("write failed with status {status:?}")]
	WriteFailed { status: WriteStatus },
	#[error(
		"mutation {operation_id:?} stopped with status {status:?}, reason {reason:?}, durable knowledge {knowledge:?}"
	)]
	MutationStopped {
		operation_id: ClientWriteId,
		status: WriteStatus,
		reason: wendelon::schema::MutationStopReason,
		knowledge: wendelon::schema::MutationCommitKnowledge,
	},
	#[error(
		"table write rejected: {} (0x{:04X})",
		.reject_code.name(),
		.reject_code.0
	)]
	TableWriteRejected { reject_code: PbcRejectCode },
	#[error("table descriptor evolved during query")]
	TableDescriptorEvolved {
		expected: PhysicalDescriptorFingerprint,
		observed: Option<PhysicalDescriptorFingerprint>,
	},
	#[error("table reference integrity failure: {failure:?}")]
	TableReferenceIntegrityFailed {
		failure: TableReferenceIntegrityFailure,
	},
	#[error("schema update rejected with reason {reason_code}: {message}")]
	SchemaUpdateRejected { reason_code: u8, message: String },
	#[error("admin operation rejected: {0:?}")]
	Admin(WendelonAdminError),
	#[error("read failed with status {status:?}")]
	ReadFailed { status: ReadStatus },
	#[error("wendelon query failed: {}({})", .status.as_str(), .status.0)]
	QueryFailed { status: ReadStatus },
}

impl ClientError {
	pub fn storage_status_kind(&self) -> StorageStatusKind {
		match self {
			Self::Io(_) => StorageStatusKind::TerminalStorageError,
			Self::QuotaExceeded { .. } => StorageStatusKind::QuotaExceeded,
			Self::TenantSuspended { .. } | Self::TenantUnavailable { .. } => {
				StorageStatusKind::TenantUnavailable
			}
			Self::AuthorizationFailed => StorageStatusKind::AuthorizationFailed,
			Self::WriteFailed { status } => map_write_status_for_compute(*status),
			Self::MutationStopped { .. } => StorageStatusKind::TerminalStorageError,
			Self::TableWriteRejected { .. } => StorageStatusKind::TerminalStorageError,
			Self::TableDescriptorEvolved { .. } => StorageStatusKind::TerminalStorageError,
			Self::TableReferenceIntegrityFailed { .. } => StorageStatusKind::TerminalStorageError,
			Self::SchemaUpdateRejected { .. } => StorageStatusKind::TerminalStorageError,
			Self::Admin(_) => StorageStatusKind::TerminalStorageError,
			Self::ReadFailed { status } => map_read_status_for_compute(*status),
			Self::QueryFailed { status } => map_read_status_for_compute(*status),
		}
	}
}

fn mutation_stopped_status(status: WriteStatus) -> bool {
	matches!(
		status,
		WriteStatus::WRITE_DEADLINE_EXCEEDED
			| WriteStatus::WRITE_STALLED
			| WriteStatus::WRITE_ATTEMPTS_EXHAUSTED
			| WriteStatus::WRITE_OPERATION_EXPIRED
	)
}

/// The identity of a table mutation the caller did not name: a fresh UUIDv7
/// minted for this operation. The server's expiry proof reads the issuance
/// time from the uuid, so it must be the operation's, never the client's.
fn default_table_mutation_id() -> ClientWriteId {
	ClientWriteId {
		client_uuid: *Uuid::now_v7().as_bytes(),
		sequence_no: 0,
		idempotency_ttl_ms: 60_000,
		_pad: 0,
	}
}

fn validate_caller_mutation_id(operation_id: ClientWriteId) -> Result<(), ClientError> {
	let uuid = operation_id.client_uuid;
	if uuid[6] >> 4 == 7 && uuid[8] >> 6 == 2 {
		return Ok(());
	}
	Err(ClientError::Io(std::io::Error::new(
		std::io::ErrorKind::InvalidInput,
		"caller-supplied mutation identity must use a UUIDv7 client_uuid",
	)))
}

pub struct Client {
	inner: Arc<InnerClient>,
	client_uuid: [u8; 16],
	next_sequence: AtomicU64,
}

impl Client {
	/// A client with a fresh UUIDv7 `client_uuid`, unique to this instance. Its
	/// write ids `(client_uuid, sequence_no)` can never be issued by another
	/// instance, in this process or any other.
	pub fn from_inner(inner: Arc<InnerClient>) -> Self {
		Self::with_client_uuid(inner, *Uuid::now_v7().as_bytes())
	}

	/// A client whose write ids use the caller's `client_uuid`. The uuid must be
	/// unique to this `Client` instance for all time: the sequence always
	/// restarts at 1, so a second instance under the same uuid (a restarted
	/// process, or another live instance) reissues earlier write ids. A counter
	/// add under a reissued write id that names a different add is refused with
	/// `WriteFailed { status: WRITE_KEY_CONFLICT }`: never applied, never
	/// replayed. Prefer [`from_inner`](Self::from_inner).
	pub fn with_client_uuid(inner: Arc<InnerClient>, client_uuid: [u8; 16]) -> Self {
		Self {
			inner,
			client_uuid,
			next_sequence: AtomicU64::new(1),
		}
	}

	/// The `client_uuid` this instance writes under.
	#[doc(hidden)]
	pub fn client_uuid(&self) -> [u8; 16] {
		self.client_uuid
	}

	pub async fn connect_quic(config: ClientConfig) -> Result<Self, ClientError> {
		let tls = TlsMaterial::load(
			&config.tls.cert_path,
			&config.tls.key_path,
			&config.tls.ca_path,
		)?;
		let endpoint = Arc::new(client_endpoint_from_tls(&tls, config.bind_addr)?);
		let server_name = canonical_server_name_resolver(config.cluster_domain.clone());
		let provider: Arc<dyn TransportProvider> =
			Arc::new(QuicTransportProvider::with_cluster_domain(
				endpoint,
				server_name,
				config.cluster_domain.clone(),
			));
		let local_tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
		let mut providers = HashMap::new();
		providers.insert(TransportTier::QuicIoUring, provider);

		let bootstrap = bootstrap_snapshot(&config.bootstrap);
		let seed = bootstrap.to_membership();
		let (_tx, rx) = watch::channel(Arc::new(seed.clone()));
		let resolver: Arc<dyn CapabilityResolver> =
			Arc::new(EpochCapabilityResolver::new(rx.clone()));
		let topology_factory =
			TransportProviderFactory::new(local_tiers, providers.clone(), resolver, rx);
		let topology_pool = BufPool::new(config.pool_max_retained, config.pool_max_cap_bytes);
		let topology: Arc<dyn wendelon::client::TopologySource> =
			Arc::new(ControlRingTopologySource::new(
				topology_factory,
				topology_pool,
				config
					.bootstrap
					.iter()
					.map(|peer| NodeId {
						id: peer.node_id,
						_reserved: 0,
					})
					.collect(),
			));
		let inner = InnerClient::new(WendelonClientConfig {
			local_tiers,
			providers,
			bootstrap,
			topology,
			pool_max_retained: config.pool_max_retained,
			pool_max_cap_bytes: config.pool_max_cap_bytes,
		});
		inner.refresh_topology().await?;
		Ok(Self::from_inner(inner))
	}

	pub fn epoch(&self) -> u64 {
		self.inner.epoch()
	}

	pub fn inner(&self) -> &Arc<InnerClient> {
		&self.inner
	}

	pub async fn refresh_topology(&self) -> Result<(), ClientError> {
		self.inner.refresh_topology().await?;
		Ok(())
	}

	pub async fn get_table_schema_descriptor(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		table_name: &str,
	) -> Result<Option<TableSchemaDescriptor>, ClientError> {
		self.inner
			.get_table_schema_descriptor(tenant_id, namespace, table_name)
			.await
			.map_err(ClientError::from)
	}

	pub async fn get_table_optimizer_facts(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		table_name: &str,
		descriptor: &TableSchemaDescriptor,
	) -> Result<Option<TableOptimizerFacts>, ClientError> {
		self.inner
			.get_table_optimizer_facts(tenant_id, namespace, table_name, descriptor)
			.await
			.map_err(ClientError::from)
	}

	pub async fn get_logical_type_definition(
		&self,
		tenant_id: TenantId,
		name: &str,
	) -> Result<Option<LogicalTypeDefinition>, ClientError> {
		self.inner
			.get_logical_type_definition(tenant_id, name)
			.await
			.map_err(ClientError::from)
	}

	pub async fn list_logical_type_definitions(
		&self,
		tenant_id: TenantId,
	) -> Result<Vec<LogicalTypeDefinition>, ClientError> {
		self.inner
			.list_logical_type_definitions(tenant_id)
			.await
			.map_err(ClientError::from)
	}

	/// Fetch the latest authoritative table descriptors for one tenant.
	///
	/// Keep this forwarding method explicit: callers use the public compute
	/// client rather than reaching through to its transport-owning inner client.
	pub async fn list_table_schema_descriptors(
		&self,
		tenant_id: TenantId,
	) -> Result<Vec<TableSchemaDescriptor>, ClientError> {
		self.inner
			.list_table_schema_descriptors(tenant_id)
			.await
			.map_err(ClientError::from)
	}

	pub async fn create_secondary_index(
		&self,
		tenant_id: TenantId,
		request: SecondaryIndexCreateRequest,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		let table = self
			.get_table_schema_descriptor(tenant_id, &request.namespace, &request.table_name)
			.await?
			.ok_or_else(|| ClientError::SchemaUpdateRejected {
				reason_code: wendelon::network::control_server::CONTROL_REASON_NOT_FOUND,
				message: format!(
					"table does not exist: {}.{}",
					request.namespace, request.table_name
				),
			})?;
		let descriptor = SecondaryIndexDescriptor::for_table_column(
			request.index_object_id,
			request.index_name,
			&table,
			&request.column_name,
		)?;
		let payload = encode_secondary_index_descriptor(&descriptor)?;
		let envelope = SchemaUpdateEnvelope::wendelon_operator_intent(
			tenant_id,
			SchemaObjectRef {
				kind: SchemaObjectKind::Index,
				namespace: descriptor.namespace.clone(),
				name: descriptor.index_name.clone(),
			},
			SchemaGeneration::ZERO,
			SchemaGeneration::new(1),
			request.intent_id,
			SchemaMutationKind::Create,
			payload,
		);
		self.submit_schema_update_payload(encode_schema_update_envelope_v1(&envelope)?)
			.await
	}

	pub async fn drop_secondary_index(
		&self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
		intent_id: SchemaIntentId,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		let status = self
			.list_secondary_indexes(tenant_id)
			.await?
			.into_iter()
			.find(|status| status.descriptor.index_object_id == index_object_id)
			.ok_or_else(|| ClientError::SchemaUpdateRejected {
				reason_code: wendelon::network::control_server::CONTROL_REASON_NOT_FOUND,
				message: "secondary index does not exist".to_owned(),
			})?;
		let payload =
			encode_secondary_index_drop_request(&SecondaryIndexDropRequest { index_object_id })?;
		let envelope = SchemaUpdateEnvelope::wendelon_operator_intent(
			tenant_id,
			SchemaObjectRef {
				kind: SchemaObjectKind::Index,
				namespace: status.descriptor.namespace,
				name: status.descriptor.index_name,
			},
			SchemaGeneration::ZERO,
			SchemaGeneration::new(1),
			intent_id,
			SchemaMutationKind::Drop,
			payload,
		);
		self.submit_schema_update_payload(encode_schema_update_envelope_v1(&envelope)?)
			.await
	}

	pub async fn list_secondary_indexes(
		&self,
		tenant_id: TenantId,
	) -> Result<Vec<SecondaryIndexStatus>, ClientError> {
		self.inner
			.list_secondary_indexes(tenant_id)
			.await
			.map_err(ClientError::from)
	}

	pub fn identity_for_key(tenant_id: TenantId, key: &[u8]) -> Identity {
		Identity::blake3_tenant(tenant_id, key)
	}

	pub fn prepare_table_schema_update(
		&self,
		tenant_id: TenantId,
		descriptor: TableSchemaDescriptor,
		expected_generation: impl Into<SchemaGeneration>,
		mutation: SchemaMutationKind,
		intent_id: SchemaIntentId,
	) -> TableSchemaUpdateRequest {
		TableSchemaUpdateRequest {
			tenant_id,
			descriptor,
			expected_generation: expected_generation.into(),
			mutation,
			intent_id,
		}
	}

	pub async fn submit_table_schema_update(
		&self,
		request: TableSchemaUpdateRequest,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		let payload = request.encode()?;
		self.submit_schema_update_payload(payload).await
	}

	/// Apply one ADR 548 control-ring saga transition.
	pub async fn mutate_ctas_saga(
		&self,
		mutation: CtasSagaMutation,
	) -> Result<CtasSagaResponse, ClientError> {
		self.submit_ctas_saga_request(CtasSagaRequestWire::Mutate(mutation))
			.await
	}

	/// Resolve the durable ADR 548 composite state without replaying a phase.
	pub async fn resolve_ctas_saga(
		&self,
		tenant_id: TenantId,
		operation_id: CtasOperationId,
	) -> Result<CtasSagaResponse, ClientError> {
		self.submit_ctas_saga_request(CtasSagaRequestWire::Resolve {
			tenant_id,
			operation_id,
		})
		.await
	}

	async fn submit_ctas_saga_request(
		&self,
		request: CtasSagaRequestWire,
	) -> Result<CtasSagaResponse, ClientError> {
		let payload = wire::encode(&request)?;
		let mut preferred_target = self.inner.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = if let Some(target) = preferred_target {
				target
			} else {
				self.inner.refresh_topology().await?;
				self.inner.leader().ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::NotConnected,
						"CTAS saga request requires a known control-ring leader",
					)
				})?
			};
			let response = self.submit_ctas_saga_once(target, &payload).await?;
			if response
				.leader_hint
				.is_some_and(|leader| leader != target.id)
				&& attempt + 1 < DATA_RETRY_BUDGET
			{
				preferred_target = response.leader_hint.map(|id| NodeId { id, _reserved: 0 });
				self.inner.refresh_topology().await?;
				continue;
			}
			return Ok(response.response);
		}
		Err(ClientError::Admin(WendelonAdminError::Transport {
			message: "CTAS saga retry budget exhausted".to_owned(),
		}))
	}

	pub async fn submit_logical_type_definition_create(
		&self,
		request: LogicalTypeDefinitionCreateRequest,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		let payload = request.encode()?;
		self.submit_schema_update_payload(payload).await
	}

	async fn submit_schema_update_payload(
		&self,
		payload: Vec<u8>,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		let mut preferred_target = self.inner.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = if let Some(target) = preferred_target {
				target
			} else {
				self.inner.refresh_topology().await?;
				self.inner.leader().ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::NotConnected,
						"schema update requires a known control-ring leader",
					)
				})?
			};
			let response = self.submit_schema_update_once(target, &payload).await?;
			if response.accepted {
				return Ok(SchemaUpdateReceipt {
					leader_hint: response.leader_hint.map(|id| NodeId { id, _reserved: 0 }),
				});
			}
			if response.reason_code == CONTROL_REASON_NOT_LEADER && attempt + 1 < DATA_RETRY_BUDGET
			{
				preferred_target = response.leader_hint.map(|id| NodeId { id, _reserved: 0 });
				self.inner.refresh_topology().await?;
				continue;
			}
			return Err(ClientError::SchemaUpdateRejected {
				reason_code: response.reason_code,
				message: response
					.message
					.unwrap_or_else(|| "schema update rejected".to_owned()),
			});
		}
		Err(ClientError::SchemaUpdateRejected {
			reason_code: CONTROL_REASON_NOT_LEADER,
			message: "schema update retry budget exhausted".to_owned(),
		})
	}

	pub async fn drop_table_schema(
		&self,
		request: TableSchemaDropRequest,
	) -> Result<(), ClientError> {
		let payload = wire::encode(&request)?;
		let mut preferred_target = self.inner.leader();
		for attempt in 0..DATA_RETRY_BUDGET {
			let target = if let Some(target) = preferred_target {
				target
			} else {
				self.inner.refresh_topology().await?;
				self.inner.leader().ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::NotConnected,
						"schema drop requires a known control-ring leader",
					)
				})?
			};
			let response = self.submit_schema_drop_once(target, &payload).await?;
			if response.accepted {
				return Ok(());
			}
			if response.reason_code == CONTROL_REASON_NOT_LEADER && attempt + 1 < DATA_RETRY_BUDGET
			{
				preferred_target = response.leader_hint.map(|id| NodeId { id, _reserved: 0 });
				self.inner.refresh_topology().await?;
				continue;
			}
			return Err(ClientError::Admin(response.admin_error.unwrap_or_else(
				|| {
					WendelonAdminError::Transport {
						message: response
							.message
							.unwrap_or_else(|| "schema drop rejected".to_owned()),
					}
				},
			)));
		}
		Err(ClientError::Admin(WendelonAdminError::Transport {
			message: "schema drop retry budget exhausted".to_owned(),
		}))
	}

	async fn submit_schema_update_once(
		&self,
		target: NodeId,
		payload: &[u8],
	) -> Result<SchemaUpdateResponseWire, ClientError> {
		let conn = self.inner.factory().get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::SCHEMA_UPDATE_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, payload, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;

		let frame = read_frame(&mut *stream, self.inner.pool()).await?;
		if frame.header.msg_type != MsgType::SCHEMA_UPDATE_RESP.0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"expected SCHEMA_UPDATE_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			)
			.into());
		}
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"SCHEMA_UPDATE_RESP must not carry Arrow or trailing payloads",
			)
			.into());
		}
		Ok(wire::decode::<SchemaUpdateResponseWire>(&frame.meta)?)
	}

	async fn submit_ctas_saga_once(
		&self,
		target: NodeId,
		payload: &[u8],
	) -> Result<CtasSagaResponseWire, ClientError> {
		let conn = self.inner.factory().get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::CTAS_SAGA_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, payload, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))?;

		let frame = read_frame(&mut *stream, self.inner.pool()).await?;
		if frame.header.msg_type != MsgType::CTAS_SAGA_RESP.0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"expected CTAS_SAGA_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			)
			.into());
		}
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"CTAS_SAGA_RESP must not carry Arrow or trailing payloads",
			)
			.into());
		}
		Ok(wire::decode::<CtasSagaResponseWire>(&frame.meta)?)
	}

	async fn submit_schema_drop_once(
		&self,
		target: NodeId,
		payload: &[u8],
	) -> Result<TableSchemaDropResponseWire, ClientError> {
		let conn = self.inner.factory().get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::SCHEMA_DROP_REQ.0,
			flags: 0,
			stream_id: 0,
			meta_len: payload.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, payload, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;

		let frame = read_frame(&mut *stream, self.inner.pool()).await?;
		if frame.header.msg_type != MsgType::SCHEMA_DROP_RESP.0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"expected SCHEMA_DROP_RESP, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			)
			.into());
		}
		Ok(wire::decode::<TableSchemaDropResponseWire>(&frame.meta)?)
	}

	/// ADR 582: read one page from exactly one node. This deliberately does not
	/// refresh, redirect, fan out, or aggregate; node-local policy state is useful
	/// only when it remains attributable to the node that owns its receipt table.
	pub async fn maintenance_report(
		&self,
		target_node_id: u32,
		max_items: u16,
		cursor: Vec<u8>,
	) -> Result<MaintenanceReportResponseWire, ClientError> {
		let target = NodeId {
			id: target_node_id,
			_reserved: 0,
		};
		let request = MaintenanceReportRequestWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			target_node_id,
			max_items,
			cursor,
		};
		let frame = self
			.submit_maintenance_once(target, MsgType::MAINTENANCE_REPORT_REQ, &request)
			.await?;
		if frame.header.msg_type != MsgType::MAINTENANCE_REPORT_RESP.0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance report target returned the wrong response type",
			)
			.into());
		}
		let response: MaintenanceReportResponseWire = wire::decode(&frame.meta)?;
		if response.version != MAINTENANCE_CONTROL_WIRE_VERSION {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance report response has an unsupported wire version",
			)
			.into());
		}
		if response.serving_node_id != target_node_id {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance report was served by a different node than requested",
			)
			.into());
		}
		Ok(response)
	}

	/// ADR 582: compare-and-clear one reviewed node-local quarantine.
	pub async fn maintenance_requeue(
		&self,
		target_node_id: u32,
		operation_code: u16,
		slabs: Vec<u64>,
		expected_fingerprint: Vec<u8>,
	) -> Result<MaintenanceRequeueResponseWire, ClientError> {
		let target = NodeId {
			id: target_node_id,
			_reserved: 0,
		};
		let request = MaintenanceRequeueRequestWire {
			version: MAINTENANCE_CONTROL_WIRE_VERSION,
			target_node_id,
			operation_code,
			slabs,
			expected_fingerprint,
		};
		let frame = self
			.submit_maintenance_once(target, MsgType::MAINTENANCE_REQUEUE_REQ, &request)
			.await?;
		if frame.header.msg_type != MsgType::MAINTENANCE_REQUEUE_RESP.0 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance requeue target returned the wrong response type",
			)
			.into());
		}
		let response: MaintenanceRequeueResponseWire = wire::decode(&frame.meta)?;
		if response.version != MAINTENANCE_CONTROL_WIRE_VERSION {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance requeue response has an unsupported wire version",
			)
			.into());
		}
		if response.serving_node_id != target_node_id {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance requeue was served by a different node than requested",
			)
			.into());
		}
		Ok(response)
	}

	async fn submit_maintenance_once<T: serde::Serialize>(
		&self,
		target: NodeId,
		msg_type: MsgType,
		request: &T,
	) -> Result<wendelon::network::core::HrftFrame, ClientError> {
		let meta = wire::encode(request)?;
		if meta.len() > MAINTENANCE_CONTROL_MAX_REQUEST_BYTES {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"maintenance request exceeds the wire byte bound",
			)
			.into());
		}
		let meta_len = u32::try_from(meta.len()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"maintenance request length does not fit the frame header",
			)
		})?;
		let conn = self.inner.factory().get_connection(target).await?;
		let mut stream = conn.open_bi().await?;
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: msg_type.0,
			flags: 0,
			stream_id: 0,
			meta_len,
			arrow_len: 0,
			trailing_len: 0,
		};
		write_frame(&mut *stream, &header, &meta, None, None).await?;
		stream
			.finish()
			.await
			.map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))?;
		let frame = read_frame(&mut *stream, self.inner.pool()).await?;
		if frame.arrow_payload.is_some() || frame.trailing.is_some() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"maintenance control response carried an unexpected payload",
			)
			.into());
		}
		Ok(frame)
	}

	pub async fn write(
		&self,
		tenant_id: TenantId,
		key: &[u8],
		value: &[u8],
		options: WriteOptions,
	) -> Result<WriteReceipt, ClientError> {
		let identity = Self::identity_for_key(tenant_id, key);
		let request = WriteRequest {
			meta: WriteRequestMeta {
				write_id: ClientWriteId {
					client_uuid: self.client_uuid,
					sequence_no: self.next_sequence.fetch_add(1, Ordering::Relaxed),
					idempotency_ttl_ms: 60_000,
					_pad: 0,
				},
				client_hlc: options.client_hlc.unwrap_or_else(default_client_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				proxy_hop: 0,
				durability: options.durability,
				await_finalization: u8::from(options.await_finalization),
				_pad1: [0; 5],
				proxy_source: zero_node(),
				key_identity: identity,
				key_len: key.len() as u32,
				value_len: value.len() as u32,
			},
			payload: kv_batch(key, value)?,
		};
		let result = self.inner.write(request).await?;
		match result.response.status {
			s if s == WriteStatus::OK || s == WriteStatus::DUPLICATE => Ok(WriteReceipt {
				identity,
				duplicate: result.response.status == WriteStatus::DUPLICATE,
				requested_durability: options.durability,
				log_index: result.response.log_index,
				term: result.response.term,
				epoch: result.response.epoch,
				commit_hlc: result.response.commit_hlc,
				finalization: result.finalization,
			}),
			s if s == WriteStatus::QUOTA_EXCEEDED => Err(ClientError::QuotaExceeded { tenant_id }),
			s if s == WriteStatus::TENANT_SUSPENDED => {
				Err(ClientError::TenantSuspended { tenant_id })
			}
			s if s == WriteStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == WriteStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			status => Err(ClientError::WriteFailed { status }),
		}
	}

	/// The domain-separated counter identity for `key` (ADR 075 D7) — distinct from
	/// the blob [`identity_for_key`](Self::identity_for_key) so a counter and a blob
	/// under the same caller bytes can never collide.
	pub fn counter_identity_for_key(key: &[u8]) -> Identity {
		counter_identity(&CounterKey(key.to_vec()))
	}

	/// Idempotent atomic fetch-add on a durable counter key (ADR 075). Bases an
	/// absent counter at `0` and creates it; a retried `write_id` replays the
	/// original `previous`/`current` without re-applying. `delta == 0` and overflow
	/// fail closed.
	pub async fn counter_fetch_add(
		&self,
		tenant_id: TenantId,
		key: &[u8],
		delta: u64,
		options: WriteOptions,
	) -> Result<CounterAddReceipt, ClientError> {
		let identity = Self::counter_identity_for_key(key);
		let request = CounterAddRequest {
			meta: CounterAddRequestMeta {
				write_id: ClientWriteId {
					client_uuid: self.client_uuid,
					sequence_no: self.next_sequence.fetch_add(1, Ordering::Relaxed),
					idempotency_ttl_ms: 60_000,
					_pad: 0,
				},
				client_hlc: options.client_hlc.unwrap_or_else(default_client_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				proxy_hop: 0,
				durability: options.durability,
				await_finalization: u8::from(options.await_finalization),
				_pad1: [0; 5],
				proxy_source: zero_node(),
				key_identity: identity,
				key_len: key.len() as u32,
				_pad2: [0; 4],
				delta,
			},
			payload: key_batch(key)?,
		};
		let result = self.inner.counter_add(request).await?;
		match result.response.status {
			// Defence in depth: a receipt must describe this add's own delta.
			s if (s == WriteStatus::OK || s == WriteStatus::DUPLICATE)
				&& result
					.response
					.current_value
					.checked_sub(result.response.previous_value)
					!= Some(delta) =>
			{
				Err(ClientError::WriteFailed {
					status: WriteStatus::INTERNAL_ERROR,
				})
			}
			s if s == WriteStatus::OK || s == WriteStatus::DUPLICATE => Ok(CounterAddReceipt {
				identity,
				previous_value: result.response.previous_value,
				current_value: result.response.current_value,
				duplicate: result.response.status == WriteStatus::DUPLICATE,
				requested_durability: options.durability,
			}),
			s if s == WriteStatus::QUOTA_EXCEEDED => Err(ClientError::QuotaExceeded { tenant_id }),
			s if s == WriteStatus::TENANT_SUSPENDED => {
				Err(ClientError::TenantSuspended { tenant_id })
			}
			s if s == WriteStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == WriteStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			// Counter rejects (COUNTER_ZERO_DELTA / COUNTER_OVERFLOW, D5, and
			// WRITE_KEY_CONFLICT) surface via the carried status; no Toron-visible
			// enum growth (D9 producer-only).
			status => Err(ClientError::WriteFailed { status }),
		}
	}

	/// Point-read a counter (ADR 075 D6/D8). An absent counter returns `None` — the
	/// API tells the truth about existence and never fabricates `0` (consumers that
	/// want a default map `None -> 0` themselves).
	pub async fn counter_read(
		&self,
		tenant_id: TenantId,
		key: &[u8],
	) -> Result<Option<u64>, ClientError> {
		let identity = Self::counter_identity_for_key(key);
		let request = CounterReadRequest {
			meta: CounterReadRequestMeta {
				expected_epoch: self.inner.epoch(),
				tenant_id,
				key_identity: identity,
			},
		};
		let result = self.inner.counter_read(request).await?;
		match result.response.status {
			s if s == ReadStatus::OK => Ok(Some(result.response.value)),
			s if s == ReadStatus::KEY_NOT_FOUND => Ok(None),
			s if s == ReadStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == ReadStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			status => Err(ClientError::ReadFailed { status }),
		}
	}

	pub async fn table_write(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		payload: RecordBatch,
		options: WriteOptions,
	) -> Result<TableWriteReceipt, ClientError> {
		let rows = payload.num_rows();
		let parent_write_id = match options.operation_id {
			Some(operation_id) => {
				validate_caller_mutation_id(operation_id)?;
				operation_id
			}
			None => default_table_mutation_id(),
		};
		let request = TableWriteRequest {
			meta: TableWriteRequestMeta {
				parent_write_id,
				client_hlc: options.client_hlc.unwrap_or_else(default_client_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				child_ordinal_base: 0,
				proxy_source: zero_node(),
				row_count: encode_table_write_row_count(rows)?,
				proxy_hop: 0,
				durability: options.durability,
				await_finalization: u8::from(options.await_finalization),
				_pad: [0; 3],
				deadline_remaining_millis: 0,
			},
			descriptor: descriptor.clone(),
			payload,
			deadline: options.deadline,
		};
		let result = self.inner.table_write(request).await?;
		if result.response.reject_code != PbcRejectCode::NONE.0 {
			return Err(ClientError::TableWriteRejected {
				reject_code: PbcRejectCode(result.response.reject_code),
			});
		}
		let response = result.response.write;
		match response.status {
			s if s == WriteStatus::OK || s == WriteStatus::DUPLICATE => Ok(TableWriteReceipt {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				rows,
				duplicate: response.status == WriteStatus::DUPLICATE,
				requested_durability: options.durability,
				log_index: response.log_index,
				term: response.term,
				epoch: response.epoch,
				commit_hlc: response.commit_hlc,
				finalization: result.finalization,
			}),
			s if s == WriteStatus::QUOTA_EXCEEDED => Err(ClientError::QuotaExceeded { tenant_id }),
			s if s == WriteStatus::TENANT_SUSPENDED => {
				Err(ClientError::TenantSuspended { tenant_id })
			}
			s if s == WriteStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == WriteStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			status if mutation_stopped_status(status) => Err(ClientError::MutationStopped {
				operation_id: response.echoed_write_id,
				status,
				reason: response.mutation_stop_reason(),
				knowledge: response.mutation_commit_knowledge(),
			}),
			status => Err(ClientError::WriteFailed { status }),
		}
	}

	pub async fn table_delete(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		row_key_ids: Vec<Identity>,
		options: WriteOptions,
	) -> Result<TableDeleteRowsReceipt, ClientError> {
		let requested = row_key_ids.len();
		let row_count = u32::try_from(requested).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"table delete row key count exceeds u32 length",
			)
		})?;
		let parent_write_id = match options.operation_id {
			Some(operation_id) => {
				validate_caller_mutation_id(operation_id)?;
				operation_id
			}
			None => default_table_mutation_id(),
		};
		let request = TableDeleteRowsRequest {
			meta: TableDeleteRowsRequestMeta {
				parent_write_id,
				client_hlc: options.client_hlc.unwrap_or_else(default_client_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				row_count,
				durability: options.durability,
				await_finalization: u8::from(options.await_finalization),
				_pad: [0; 10],
				deadline_remaining_millis: 0,
			},
			descriptor: descriptor.clone(),
			row_key_ids,
			deadline: options.deadline,
		};
		let result = self.inner.table_delete(request).await?;
		if result.response.reject_code != PbcRejectCode::NONE.0 {
			return Err(ClientError::TableWriteRejected {
				reject_code: PbcRejectCode(result.response.reject_code),
			});
		}
		let response = result.response.write;
		match response.status {
			s if s == WriteStatus::OK || s == WriteStatus::DUPLICATE => {
				Ok(TableDeleteRowsReceipt {
					table_object_id: descriptor.table_object_id,
					schema_generation: descriptor.schema_generation,
					requested,
					deleted_count: result.response.deleted_count,
					already_deleted_count: result.response.already_deleted_count,
					not_found_count: result.response.not_found_count,
					duplicate: response.status == WriteStatus::DUPLICATE,
					requested_durability: options.durability,
					log_index: response.log_index,
					term: response.term,
					epoch: response.epoch,
					commit_hlc: response.commit_hlc,
					finalization: result.finalization,
				})
			}
			s if s == WriteStatus::QUOTA_EXCEEDED => Err(ClientError::QuotaExceeded { tenant_id }),
			s if s == WriteStatus::TENANT_SUSPENDED => {
				Err(ClientError::TenantSuspended { tenant_id })
			}
			s if s == WriteStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == WriteStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			status if mutation_stopped_status(status) => Err(ClientError::MutationStopped {
				operation_id: response.echoed_write_id,
				status,
				reason: response.mutation_stop_reason(),
				knowledge: response.mutation_commit_knowledge(),
			}),
			status => Err(ClientError::WriteFailed { status }),
		}
	}

	/// Resolve a retained table mutation without submitting its payload again.
	pub async fn resolve_table_mutation(
		&self,
		tenant_id: TenantId,
		operation_id: ClientWriteId,
	) -> Result<ResolvedMutationOutcome, ClientError> {
		let result = self
			.inner
			.resolve_mutation_outcome(MutationOutcomeRequest {
				tenant_id,
				operation_id,
			})
			.await?;
		let response = result.response;
		if response.write.echoed_write_id.canonical_idempotency_bytes()
			!= operation_id.canonical_idempotency_bytes()
		{
			return Err(ClientError::Io(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"mutation outcome response echoed a different operation identity",
			)));
		}
		if response.resolution == MutationResolutionStatus::EXPIRED {
			return Ok(ResolvedMutationOutcome::Expired { operation_id });
		}
		if response.resolution == MutationResolutionStatus::UNKNOWN {
			return Ok(ResolvedMutationOutcome::Unknown { operation_id });
		}
		if response.resolution != MutationResolutionStatus::PRESENT {
			return Err(ClientError::Io(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"unknown mutation resolution status {}",
					response.resolution.0
				),
			)));
		}
		Ok(ResolvedMutationOutcome::Present {
			operation_id: response.write.echoed_write_id,
			status: response.write.status,
			reason: response.write.mutation_stop_reason(),
			knowledge: response.write.mutation_commit_knowledge(),
			delete_counts: (response.operation_kind == MutationOperationKind::DELETE).then_some([
				response.deleted_count,
				response.already_deleted_count,
				response.not_found_count,
			]),
		})
	}

	/// Pin terminal row-mutation evidence before a staged CTAS dispatch.
	pub async fn pin_table_mutation_outcome(
		&self,
		tenant_id: TenantId,
		operation_id: ClientWriteId,
	) -> Result<(), ClientError> {
		self.inner
			.set_mutation_outcome_pin(MutationOutcomePinRequest {
				tenant_id,
				operation_id,
				action: MutationOutcomePinAction::PIN,
			})
			.await
			.map_err(ClientError::Io)
	}

	/// Release a staged CTAS pin after the Control Ring durably attests the row
	/// outcome. This call is idempotent.
	pub async fn acknowledge_table_mutation_outcome(
		&self,
		tenant_id: TenantId,
		operation_id: ClientWriteId,
	) -> Result<(), ClientError> {
		self.inner
			.set_mutation_outcome_pin(MutationOutcomePinRequest {
				tenant_id,
				operation_id,
				action: MutationOutcomePinAction::ACKNOWLEDGE,
			})
			.await
			.map_err(ClientError::Io)
	}

	pub async fn read(
		&self,
		tenant_id: TenantId,
		key: &[u8],
		options: ReadOptions,
	) -> Result<Option<ReadValue>, ClientError> {
		let identity = Self::identity_for_key(tenant_id, key);
		let request = ReadRequest {
			meta: ReadRequestMeta {
				min_hlc: options.min_hlc.unwrap_or_else(zero_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				consistency: options.consistency,
				_pad: [0; 3],
				key_identity: identity,
				key_len: key.len() as u32,
				_pad2: [0; 8],
				as_of_hlc: options.as_of_hlc.unwrap_or_else(zero_hlc),
			},
			key: key_batch(key)?,
		};
		let result = self.inner.read(request).await?;
		match result.response.status {
			s if s == ReadStatus::OK => Ok(Some(ReadValue {
				identity,
				epoch: result.response.epoch,
				value_hlc: result.response.value_hlc,
				value: extract_value(&result.value.ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"READ_RESP status=OK without a value batch",
					)
				})?)?,
			})),
			s if s == ReadStatus::KEY_NOT_FOUND => Ok(None),
			s if s == ReadStatus::TENANT_UNAVAILABLE => {
				Err(ClientError::TenantUnavailable { tenant_id })
			}
			s if s == ReadStatus::AUTHORIZATION_FAILED => Err(ClientError::AuthorizationFailed),
			status => Err(ClientError::ReadFailed { status }),
		}
	}

	pub async fn query_stream(
		&self,
		tenant_id: TenantId,
		pbc: Vec<u8>,
		options: QueryOptions,
	) -> Result<StreamingQueryResult, ClientError> {
		Ok(self
			.inner
			.query(
				tenant_id,
				pbc,
				options.range,
				self.inner.epoch(),
				options.min_hlc.unwrap_or_else(zero_hlc),
				options.consistency,
				options.as_of_hlc.unwrap_or_else(zero_hlc),
				options.stats_pruning_enabled,
			)
			.await?)
	}

	pub async fn query(
		&self,
		tenant_id: TenantId,
		pbc: Vec<u8>,
		options: QueryOptions,
	) -> Result<QueryResult, ClientError> {
		let mut stream = self.query_stream(tenant_id, pbc, options).await?;
		let batches = stream.drain_all().await?;
		Ok(QueryResult {
			epoch: stream.epoch,
			value_hlc: stream.value_hlc,
			rows: stream.rows,
			batches,
		})
	}

	pub async fn table_query_stream(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		options: TableQueryOptions,
	) -> Result<StreamingQueryResult, ClientError> {
		self.table_query_stream_with_runtime_filter(tenant_id, descriptor, pbc, None, options)
			.await
	}

	pub async fn table_query_stream_with_runtime_filter(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		runtime_filter: Option<RuntimeFilterDescriptor>,
		options: TableQueryOptions,
	) -> Result<StreamingQueryResult, ClientError> {
		let expected_physical_descriptor_fingerprint = options
			.physical_descriptor_fingerprint
			.unwrap_or(descriptor.physical_descriptor_fingerprint);
		if descriptor.physical_descriptor_fingerprint != expected_physical_descriptor_fingerprint {
			return Err(ClientError::TableDescriptorEvolved {
				expected: expected_physical_descriptor_fingerprint,
				observed: Some(descriptor.physical_descriptor_fingerprint),
			});
		}

		let bytecode_len = u32::try_from(pbc.len()).map_err(|_| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"table query PBC bytecode exceeds u32 length",
			)
		})?;
		let runtime_filter_len = match &runtime_filter {
			Some(runtime_filter) => {
				let encoded = postcard::to_allocvec(runtime_filter).map_err(|err| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						format!("table query runtime filter encode failed: {err}"),
					)
				})?;
				u32::try_from(encoded.len()).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						"table query runtime filter exceeds u32 length",
					)
				})?
			}
			None => 0,
		};
		let mut pad = [0; 6];
		if !options.stats_pruning_enabled {
			pad[0] |= QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
		}
		let trace_tail = table_query_trace_tail(options.trace_context);
		let request = TableQueryRequest {
			meta: TableQueryRequestMeta {
				min_hlc: options.min_hlc.unwrap_or_else(zero_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				physical_descriptor_fingerprint: *expected_physical_descriptor_fingerprint
					.as_bytes(),
				consistency: options.consistency,
				row_identity_projection: options.row_identity.as_u8(),
				_pad: pad,
				bytecode_len,
				runtime_filter_len,
				as_of_hlc: options.as_of_hlc.unwrap_or_else(zero_hlc),
				row_limit_rows: options.row_limit.map(NonZeroU64::get).unwrap_or(0),
				request_id: TableQueryRequestId::ZERO,
				deadline_remaining_millis: deadline_remaining_millis(options.deadline),
				trace_id: trace_tail.trace_id,
				trace_parent_span_id: trace_tail.parent_span_id,
				trace_flags: trace_tail.flags,
				_trace_pad: [0; 7],
			},
			descriptor: descriptor.clone(),
			pbc,
			runtime_filter,
			row_identity: options.row_identity,
		};
		match self.inner.table_query(request).await {
			Ok(result) => Ok(result),
			Err(error) if is_table_descriptor_evolved_error(&error) => {
				Err(ClientError::TableDescriptorEvolved {
					expected: expected_physical_descriptor_fingerprint,
					observed: None,
				})
			}
			Err(error) => Err(table_query_client_error(error)),
		}
	}

	pub async fn table_query_stream_with_runtime_filter_and_attempt_hook<F>(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		runtime_filter: Option<RuntimeFilterDescriptor>,
		options: TableQueryOptions,
		mut attempt_hook: F,
	) -> Result<StreamingQueryResult, ClientError>
	where
		F: FnMut(WendelonTableQueryAttempt) -> std::io::Result<TableQueryAttemptGuard> + Send,
	{
		let expected_physical_descriptor_fingerprint = options
			.physical_descriptor_fingerprint
			.unwrap_or(descriptor.physical_descriptor_fingerprint);
		if descriptor.physical_descriptor_fingerprint != expected_physical_descriptor_fingerprint {
			return Err(ClientError::TableDescriptorEvolved {
				expected: expected_physical_descriptor_fingerprint,
				observed: Some(descriptor.physical_descriptor_fingerprint),
			});
		}

		let request = table_query_request(
			self,
			tenant_id,
			descriptor,
			pbc,
			runtime_filter,
			options,
			expected_physical_descriptor_fingerprint,
		)?;
		match self
			.inner
			.table_query_with_attempt_hook(request, &mut attempt_hook)
			.await
		{
			Ok(result) => Ok(result),
			Err(error) if is_table_descriptor_evolved_error(&error) => {
				Err(ClientError::TableDescriptorEvolved {
					expected: expected_physical_descriptor_fingerprint,
					observed: None,
				})
			}
			Err(error) => Err(table_query_client_error(error)),
		}
	}

	pub async fn table_query(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		options: TableQueryOptions,
	) -> Result<QueryResult, ClientError> {
		let mut stream = self
			.table_query_stream(tenant_id, descriptor, pbc, options)
			.await?;
		let batches = stream.drain_all().await?;
		Ok(QueryResult {
			epoch: stream.epoch,
			value_hlc: stream.value_hlc,
			rows: stream.rows,
			batches,
		})
	}

	pub async fn table_query_with_runtime_filter(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		runtime_filter: Option<RuntimeFilterDescriptor>,
		options: TableQueryOptions,
	) -> Result<QueryResult, ClientError> {
		let mut stream = self
			.table_query_stream_with_runtime_filter(
				tenant_id,
				descriptor,
				pbc,
				runtime_filter,
				options,
			)
			.await?;
		let batches = stream.drain_all().await?;
		Ok(QueryResult {
			epoch: stream.epoch,
			value_hlc: stream.value_hlc,
			rows: stream.rows,
			batches,
		})
	}

	pub async fn table_query_with_runtime_filter_and_attempt_hook<F>(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		runtime_filter: Option<RuntimeFilterDescriptor>,
		options: TableQueryOptions,
		attempt_hook: F,
	) -> Result<QueryResult, ClientError>
	where
		F: FnMut(WendelonTableQueryAttempt) -> std::io::Result<TableQueryAttemptGuard> + Send,
	{
		let mut stream = self
			.table_query_stream_with_runtime_filter_and_attempt_hook(
				tenant_id,
				descriptor,
				pbc,
				runtime_filter,
				options,
				attempt_hook,
			)
			.await?;
		let batches = stream.drain_all().await?;
		Ok(QueryResult {
			epoch: stream.epoch,
			value_hlc: stream.value_hlc,
			rows: stream.rows,
			batches,
		})
	}

	pub async fn cancel_table_query_attempt(
		&self,
		target: NodeId,
		tenant_id: TenantId,
		request_id: TableQueryRequestId,
	) -> Result<TableQueryCancelResponseMeta, ClientError> {
		Ok(self
			.inner
			.cancel_table_query_attempt(target, tenant_id, request_id)
			.await?)
	}

	pub async fn cdc_subscribe(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		start_cursor: CdcCursor,
		bound: CdcSubscribeBound,
		logical_schema_fingerprint: LogicalSchemaFingerprint,
		resume_digest: Option<StateDigest>,
	) -> Result<CdcSubscription, ClientError> {
		Ok(self
			.inner
			.cdc_subscribe(
				tenant_id,
				table_object_id,
				start_cursor,
				bound,
				logical_schema_fingerprint,
				resume_digest,
			)
			.await?)
	}
}

fn table_query_request(
	client: &Client,
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptor,
	pbc: Vec<u8>,
	runtime_filter: Option<RuntimeFilterDescriptor>,
	options: TableQueryOptions,
	expected_physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
) -> std::io::Result<TableQueryRequest> {
	let bytecode_len = u32::try_from(pbc.len()).map_err(|_| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"table query PBC bytecode exceeds u32 length",
		)
	})?;
	let runtime_filter_len = match &runtime_filter {
		Some(runtime_filter) => {
			let encoded = postcard::to_allocvec(runtime_filter).map_err(|err| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					format!("table query runtime filter encode failed: {err}"),
				)
			})?;
			u32::try_from(encoded.len()).map_err(|_| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					"table query runtime filter exceeds u32 length",
				)
			})?
		}
		None => 0,
	};
	let mut pad = [0; 6];
	if !options.stats_pruning_enabled {
		pad[0] |= QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
	}
	let trace_tail = table_query_trace_tail(options.trace_context);
	Ok(TableQueryRequest {
		meta: TableQueryRequestMeta {
			min_hlc: options.min_hlc.unwrap_or_else(zero_hlc),
			expected_epoch: client.inner.epoch(),
			tenant_id,
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: *expected_physical_descriptor_fingerprint.as_bytes(),
			consistency: options.consistency,
			row_identity_projection: options.row_identity.as_u8(),
			_pad: pad,
			bytecode_len,
			runtime_filter_len,
			as_of_hlc: options.as_of_hlc.unwrap_or_else(zero_hlc),
			row_limit_rows: options.row_limit.map(NonZeroU64::get).unwrap_or(0),
			request_id: TableQueryRequestId::ZERO,
			deadline_remaining_millis: deadline_remaining_millis(options.deadline),
			trace_id: trace_tail.trace_id,
			trace_parent_span_id: trace_tail.parent_span_id,
			trace_flags: trace_tail.flags,
			_trace_pad: [0; 7],
		},
		descriptor: descriptor.clone(),
		pbc,
		runtime_filter,
		row_identity: options.row_identity,
	})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableQueryTraceTail {
	trace_id: [u8; 16],
	parent_span_id: [u8; 8],
	flags: u8,
}

fn table_query_trace_tail(context: TableQueryTraceContext) -> TableQueryTraceTail {
	match context {
		TableQueryTraceContext::Absent => TableQueryTraceTail {
			trace_id: [0; 16],
			parent_span_id: [0; 8],
			flags: 0,
		},
		TableQueryTraceContext::Unsampled => TableQueryTraceTail {
			trace_id: [0; 16],
			parent_span_id: [0; 8],
			flags: TABLE_QUERY_TRACE_FLAG_PRESENT,
		},
		TableQueryTraceContext::Sampled {
			trace_id,
			parent_span_id,
		} => TableQueryTraceTail {
			trace_id,
			parent_span_id,
			flags: TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED,
		},
	}
}

fn bootstrap_snapshot(bootstrap: &[BootstrapPeer]) -> ClusterStatusWire {
	let tier_bits = TransportTierSet::empty()
		.with(TransportTier::QuicIoUring)
		.as_u8();
	let role_bits = NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8();
	ClusterStatusWire {
		epoch: 0,
		leader: None,
		replication_factor: 1,
		peers: bootstrap
			.iter()
			.map(|peer| ClusterPeerWire {
				node_id: peer.node_id,
				address: peer.address.clone(),
				tier_bits,
				role_bits,
				capability_bits: 0,
			})
			.collect(),
	}
}

fn kv_batch(key: &[u8], value: &[u8]) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn key_batch(key: &[u8]) -> std::io::Result<RecordBatch> {
	let schema = Arc::new(read_key_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	RecordBatch::try_new(schema, vec![Arc::new(keys.finish())])
		.map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn extract_value(batch: &RecordBatch) -> std::io::Result<Vec<u8>> {
	let values: &LargeBinaryArray = batch
		.column_by_name("value")
		.ok_or_else(|| {
			std::io::Error::new(std::io::ErrorKind::InvalidData, "missing 'value' column")
		})?
		.as_any()
		.downcast_ref()
		.ok_or_else(|| {
			std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				"'value' column is not LargeBinary",
			)
		})?;
	if values.is_empty() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"'value' column is empty",
		));
	}
	Ok(values.value(0).to_vec())
}

fn default_client_hlc() -> HybridTimestamp {
	let physical_ms = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis() as u64;
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id: 0,
	}
}

/// Remaining milliseconds until the caller's original absolute deadline, measured at
/// this attempt. An already-expired deadline transmits one millisecond rather than
/// zero, because zero means "absent" on the wire — the server then expires it
/// immediately with the deadline status instead of the client silently dropping it.
fn deadline_remaining_millis(deadline: Option<Instant>) -> u64 {
	match deadline {
		None => 0,
		Some(deadline) => {
			let remaining = deadline
				.saturating_duration_since(Instant::now())
				.as_millis() as u64;
			remaining.max(1)
		}
	}
}

fn zero_hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 0,
		logical: 0,
		node_id: 0,
	}
}

fn zero_node() -> NodeId {
	NodeId {
		id: 0,
		_reserved: 0,
	}
}

fn is_table_descriptor_evolved_error(error: &std::io::Error) -> bool {
	// Descriptor evolution is a *more specific* classification than "some named status
	// failed", and it must be recognised before the generic typed-status arm claims it.
	// ADR 540 stopped rendering statuses into the message, so matching on the message
	// text alone silently stopped recognising this case and collapsed it into
	// `QueryFailed` — the very reduction ADR 540 exists to prevent, one seam over. The
	// status is the durable signal; the string match stays only for errors that still
	// arrive without a typed source.
	if query_status_failure_from_io(error) == Some(ReadStatus::TABLE_DESCRIPTOR_EVOLVED) {
		return true;
	}
	error.kind() == std::io::ErrorKind::InvalidData
		&& error.to_string().contains("TABLE_DESCRIPTOR_EVOLVED")
}

fn table_query_client_error(error: std::io::Error) -> ClientError {
	if let Some(failure) = table_reference_integrity_failure_from_io(&error) {
		return ClientError::TableReferenceIntegrityFailed { failure };
	}
	// A named status must cross this seam typed; only errors that genuinely carry no
	// server status may remain Io — that is what keeps "unavailable" honest downstream.
	if let Some(status) = query_status_failure_from_io(&error) {
		return ClientError::QueryFailed { status };
	}
	ClientError::Io(error)
}

/// Extracts the typed query status when a terminal failure travels as an io error —
/// the parallel of [`table_reference_integrity_failure_from_io`] for every named (and
/// unnamed-but-numeric) `ReadStatus`.
pub fn query_status_failure_from_io(error: &std::io::Error) -> Option<ReadStatus> {
	error
		.get_ref()
		.and_then(|source| source.downcast_ref::<wendelon::client::QueryStatusError>())
		.map(|error| error.status)
}

/// Extracts the wire diagnostic when a streaming terminal failure arrives
/// after the initial query setup. Callers that consume a stream themselves
/// can preserve the same typed failure as the initial-response path.
pub fn table_reference_integrity_failure_from_io(
	error: &std::io::Error,
) -> Option<TableReferenceIntegrityFailure> {
	error
		.get_ref()
		.and_then(|source| {
			source.downcast_ref::<wendelon::client::TableReferenceIntegrityQueryError>()
		})
		.map(|error| TableReferenceIntegrityFailure {
			tenant_id: error.failure.tenant_id,
			table_object_id: TableObjectId::new(error.failure.table_object_id),
			reference_kind: error.failure.reference_kind,
			missing_slab_id: error.failure.missing_slab_id,
			origin: error.failure.origin,
		})
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::datatypes::{DataType, Field, Schema};
	use wendelon::schema::decode_table_schema_descriptor_v1;

	fn table_descriptor(generation: u64) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		TableSchemaDescriptor::new(
			TableObjectId::new([0x42; 16]),
			generation,
			"analytics",
			"events",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	#[test]
	fn table_schema_update_request_encodes_descriptor_payload() {
		let descriptor = table_descriptor(1);
		let request = TableSchemaUpdateRequest::create(
			TenantId::new(7),
			descriptor.clone(),
			SchemaIntentId::new([0xAB; 16]),
		);

		let encoded = request.encode().unwrap();
		let envelope = decode_schema_update_envelope_v1(&encoded).unwrap();
		assert_eq!(envelope.tenant_id, TenantId::new(7));
		assert_eq!(envelope.object.kind, SchemaObjectKind::Table);
		assert_eq!(envelope.object.namespace, "analytics");
		assert_eq!(envelope.object.name, "events");
		assert_eq!(envelope.expected_generation, SchemaGeneration::ZERO);
		assert_eq!(envelope.requested_generation, SchemaGeneration::new(1));
		assert_eq!(envelope.mutation, SchemaMutationKind::Create);
		assert_eq!(
			decode_table_schema_descriptor_v1(&envelope.payload).unwrap(),
			descriptor
		);
	}

	#[test]
	fn table_schema_update_request_rejects_drop_helper() {
		let request = TableSchemaUpdateRequest {
			tenant_id: TenantId::new(7),
			descriptor: table_descriptor(2),
			expected_generation: SchemaGeneration::new(1),
			mutation: SchemaMutationKind::Drop,
			intent_id: SchemaIntentId::new([0xCD; 16]),
		};

		let err = request.encode().unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
	}

	#[test]
	fn adr541_deadline_remaining_is_absent_only_when_no_deadline_exists() {
		assert_eq!(deadline_remaining_millis(None), 0);
	}

	#[test]
	fn adr541_deadline_remaining_is_measured_per_attempt() {
		let deadline = Instant::now() + std::time::Duration::from_millis(60_000);
		let first = deadline_remaining_millis(Some(deadline));
		assert!(first > 0 && first <= 60_000, "remaining {first}");
		let later = deadline_remaining_millis(Some(deadline));
		assert!(
			later <= first,
			"a later attempt must see no more time than an earlier one: {later} > {first}"
		);
	}

	#[test]
	fn adr541_expired_deadline_still_transmits_a_deadline() {
		let expired = Instant::now() - std::time::Duration::from_millis(50);
		assert_eq!(deadline_remaining_millis(Some(expired)), 1);
	}

	#[test]
	fn table_descriptor_evolved_io_error_is_detected() {
		let err = std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Wendelon query failed: TABLE_DESCRIPTOR_EVOLVED",
		);

		assert!(is_table_descriptor_evolved_error(&err));
	}

	/// ADR 540 must not swallow a *more specific* classification. Descriptor evolution
	/// arrives as a typed status once messages stop carrying the status name, and it has
	/// to stay recognisable there — otherwise it collapses into the generic
	/// `QueryFailed`, which is the reduction ADR 540 was written to prevent.
	#[test]
	fn typed_descriptor_evolved_status_is_not_reduced_to_generic_query_failure() {
		let typed = std::io::Error::new(
			std::io::ErrorKind::Other,
			wendelon::client::QueryStatusError {
				status: ReadStatus::TABLE_DESCRIPTOR_EVOLVED,
			},
		);

		assert!(
			is_table_descriptor_evolved_error(&typed),
			"a typed TABLE_DESCRIPTOR_EVOLVED status must be recognised without a message match"
		);

		// The neighbouring status must NOT be captured by the same arm.
		let neighbour = std::io::Error::new(
			std::io::ErrorKind::Other,
			wendelon::client::QueryStatusError {
				status: ReadStatus::QUERY_BUDGET_EXCEEDED,
			},
		);
		assert!(!is_table_descriptor_evolved_error(&neighbour));
	}

	#[test]
	fn table_reference_integrity_io_error_preserves_the_wire_diagnosis() {
		let error = std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			wendelon::client::TableReferenceIntegrityQueryError {
				failure: wendelon::schema::TableReferenceIntegrityFailureMeta {
					tenant_id: TenantId::new(7),
					table_object_id: [0xA5; 16],
					missing_slab_id: 4,
					reference_kind: 1,
					origin: 2,
					_pad: [0; 6],
				},
			},
		);
		let actual = table_query_client_error(error);
		assert!(matches!(
			actual,
			ClientError::TableReferenceIntegrityFailed {
				failure: TableReferenceIntegrityFailure {
					tenant_id,
					missing_slab_id: 4,
					reference_kind: 1,
					origin: 2,
					..
				}
			} if tenant_id == TenantId::new(7)
		));
	}

	/// ADR 540's discriminating pair: the same io transport carries a budget expiry
	/// and an integrity wedge, and they must surface as different typed variants —
	/// this exact confusion cost the soak a day of misattribution.
	#[test]
	fn table_query_named_status_surfaces_typed_never_io() {
		let error = std::io::Error::new(
			std::io::ErrorKind::Other,
			wendelon::client::QueryStatusError {
				status: ReadStatus::QUERY_BUDGET_EXCEEDED,
			},
		);
		let actual = table_query_client_error(error);
		assert!(matches!(
			actual,
			ClientError::QueryFailed {
				status: ReadStatus::QUERY_BUDGET_EXCEEDED
			}
		));
		assert_eq!(
			actual.to_string(),
			"wendelon query failed: query_budget_exceeded(11)"
		);

		// An unnamed raw status stays typed and keeps its numeric value.
		let unknown = table_query_client_error(std::io::Error::new(
			std::io::ErrorKind::Other,
			wendelon::client::QueryStatusError {
				status: ReadStatus(200),
			},
		));
		assert!(matches!(
			unknown,
			ClientError::QueryFailed {
				status: ReadStatus(200)
			}
		));
		assert_eq!(unknown.to_string(), "wendelon query failed: unknown(200)");

		// A statusless transport failure is the only thing left for Io.
		let transport = table_query_client_error(std::io::Error::new(
			std::io::ErrorKind::ConnectionReset,
			"peer reset",
		));
		assert!(matches!(transport, ClientError::Io(_)));
	}

	#[test]
	fn table_query_options_default_trace_context_absent() {
		assert_eq!(
			TableQueryOptions::default().trace_context,
			TableQueryTraceContext::Absent
		);
	}

	#[test]
	fn caller_mutation_identity_requires_time_ordered_uuidv7_namespace() {
		let mut valid_uuid = [0_u8; 16];
		valid_uuid[6] = 0x70;
		valid_uuid[8] = 0x80;
		let valid = ClientWriteId {
			client_uuid: valid_uuid,
			sequence_no: 0,
			idempotency_ttl_ms: 300_000,
			_pad: 0,
		};
		assert!(validate_caller_mutation_id(valid).is_ok());

		let invalid = ClientWriteId {
			client_uuid: [4; 16],
			..valid
		};
		let error = validate_caller_mutation_id(invalid).unwrap_err();
		assert!(
			matches!(error, ClientError::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidInput)
		);
	}

	#[test]
	fn table_query_trace_tail_encodes_absent_unsampled_and_sampled() {
		let absent = table_query_trace_tail(TableQueryTraceContext::Absent);
		assert_eq!(absent.trace_id, [0; 16]);
		assert_eq!(absent.parent_span_id, [0; 8]);
		assert_eq!(absent.flags, 0);

		let unsampled = table_query_trace_tail(TableQueryTraceContext::Unsampled);
		assert_eq!(unsampled.trace_id, [0; 16]);
		assert_eq!(unsampled.parent_span_id, [0; 8]);
		assert_eq!(unsampled.flags, TABLE_QUERY_TRACE_FLAG_PRESENT);

		let trace_id = [0x11; 16];
		let parent_span_id = [0x22; 8];
		let sampled = table_query_trace_tail(TableQueryTraceContext::Sampled {
			trace_id,
			parent_span_id,
		});
		assert_eq!(sampled.trace_id, trace_id);
		assert_eq!(sampled.parent_span_id, parent_span_id);
		assert_eq!(
			sampled.flags,
			TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED
		);
	}
}
