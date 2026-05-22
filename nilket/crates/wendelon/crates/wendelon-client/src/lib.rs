#[cfg(all(feature = "fips", feature = "non-fips"))]
compile_error!("features `fips` and `non-fips` are mutually exclusive");

use arrow::array::{Array, LargeBinaryArray, LargeBinaryBuilder};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, DATA_RETRY_BUDGET, ReadRequest,
	TableDeleteRowsRequest, TableQueryRequest, TableWriteRequest, WendelonClient as InnerClient,
	WendelonClientConfig, WriteRequest,
};
use wendelon::network::control_server::{
	CONTROL_REASON_NOT_LEADER, SchemaUpdateResponseWire, TableSchemaDropResponseWire,
};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::factory::TransportProviderFactory;
use wendelon::network::pool::BufPool;
use wendelon::network::quic_transport::{
	QuicTransportProvider, canonical_server_name_resolver, endpoint_from_tls,
};
use wendelon::network::tls::{DEFAULT_CLUSTER_DOMAIN, TlsMaterial};
use wendelon::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, NodeRole, NodeRoleSet, TransportProvider,
	TransportTier, TransportTierSet,
};
use wendelon::network::wire;
use wendelon::schema::{
	ClientWriteId, FinalizationMarker, FrameHeader, HybridTimestamp, Identity, MsgType, NodeId,
	QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING, ReadRequestMeta, ReadStatus,
	TABLE_QUERY_TRACE_FLAG_PRESENT, TABLE_QUERY_TRACE_FLAG_SAMPLED, TableDeleteRowsRequestMeta,
	TableQueryCancelResponseMeta, TableQueryRequestId, TableQueryRequestMeta,
	TableWriteRequestMeta, TenantId, WriteRequestMeta, WriteStatus,
	encode_table_schema_descriptor_v1, encode_table_write_row_count, read_key_schema,
	write_payload_schema,
};

pub use wendelon::client::{
	CdcFrame, CdcSubscription, QueryLocalityRange, RowIdentityProjection, StreamingQueryResult,
	TableQueryAttempt as WendelonTableQueryAttempt, TableQueryAttemptGuard,
};
pub use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, ComputeQuerySessionHandoff, ComputeQuerySessionIssueRequest,
	ComputeTraceContextV1, DropOperationId, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1, SchemaUpdateEnvelopeV1,
	StorageQuotaMetadataV1, StorageStatusKind, TableSchemaDropFields, TableSchemaDropRequest,
	WendelonAdminError, decode_schema_update_envelope_v1, encode_schema_update_envelope_v1,
	map_read_status_for_compute, map_write_status_for_compute, prepare_query_session_handoff,
	validate_query_session_audit_attribution, validate_traceparent,
};
pub use wendelon::schema::{
	CdcCursor, CdcRejectCode, CdcSubscribeBound, Consistency as ReadConsistency,
	DurabilityLevel as DurabilityClass, IndexObjectId, LocalityPolicy, LogicalSchemaFingerprint,
	PbcRejectCode, PhysicalDescriptorFingerprint, PrimaryKeyDescriptorV1, PrimaryKeyEncodingV1,
	RowIdentityPolicy, RuntimeFilterDescriptor, SchemaEvolutionPolicyV1, StateDigest,
	TableObjectId, TableSchemaDescriptor, TableSpreadLocalityDescriptorV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapPeer {
	pub node_id: u32,
	pub address: String,
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
}

impl Default for WriteOptions {
	fn default() -> Self {
		Self {
			durability: DurabilityClass::QUORUM,
			await_finalization: false,
			client_hlc: None,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchemaUpdateRequest {
	pub tenant_id: TenantId,
	pub descriptor: TableSchemaDescriptor,
	pub expected_generation: SchemaGeneration,
	pub mutation: SchemaMutationKindV1,
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
			mutation: SchemaMutationKindV1::Create,
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
			mutation: SchemaMutationKindV1::Alter,
			intent_id,
		}
	}

	pub fn envelope(&self) -> std::io::Result<SchemaUpdateEnvelopeV1> {
		if self.mutation == SchemaMutationKindV1::Drop {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"table schema drops require a rewrite migration and are not supported by this helper",
			));
		}
		let payload = encode_table_schema_descriptor_v1(&self.descriptor)?;
		Ok(SchemaUpdateEnvelopeV1::compute_client_intent(
			self.tenant_id,
			SchemaObjectRefV1 {
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
	#[error("table write rejected with code {reject_code:?}")]
	TableWriteRejected { reject_code: PbcRejectCode },
	#[error("table descriptor evolved during query")]
	TableDescriptorEvolved {
		expected: PhysicalDescriptorFingerprint,
		observed: Option<PhysicalDescriptorFingerprint>,
	},
	#[error("schema update rejected with reason {reason_code}: {message}")]
	SchemaUpdateRejected { reason_code: u8, message: String },
	#[error("admin operation rejected: {0:?}")]
	Admin(WendelonAdminError),
	#[error("read failed with status {status:?}")]
	ReadFailed { status: ReadStatus },
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
			Self::TableWriteRejected { .. } => StorageStatusKind::TerminalStorageError,
			Self::TableDescriptorEvolved { .. } => StorageStatusKind::TerminalStorageError,
			Self::SchemaUpdateRejected { .. } => StorageStatusKind::TerminalStorageError,
			Self::Admin(_) => StorageStatusKind::TerminalStorageError,
			Self::ReadFailed { status } => map_read_status_for_compute(*status),
		}
	}
}

pub struct Client {
	inner: Arc<InnerClient>,
	client_uuid: [u8; 16],
	next_sequence: AtomicU64,
}

impl Client {
	pub fn from_inner(inner: Arc<InnerClient>) -> Self {
		Self::with_client_uuid(inner, *Uuid::new_v4().as_bytes())
	}

	pub fn with_client_uuid(inner: Arc<InnerClient>, client_uuid: [u8; 16]) -> Self {
		Self {
			inner,
			client_uuid,
			next_sequence: AtomicU64::new(1),
		}
	}

	pub async fn connect_quic(config: ClientConfig) -> Result<Self, ClientError> {
		let tls = TlsMaterial::load(
			&config.tls.cert_path,
			&config.tls.key_path,
			&config.tls.ca_path,
		)?;
		let endpoint = Arc::new(endpoint_from_tls(&tls, config.bind_addr)?);
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

	pub fn identity_for_key(tenant_id: TenantId, key: &[u8]) -> Identity {
		Identity::blake3_tenant(tenant_id, key)
	}

	pub fn prepare_table_schema_update(
		&self,
		tenant_id: TenantId,
		descriptor: TableSchemaDescriptor,
		expected_generation: impl Into<SchemaGeneration>,
		mutation: SchemaMutationKindV1,
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

	pub async fn table_write(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		payload: RecordBatch,
		options: WriteOptions,
	) -> Result<TableWriteReceipt, ClientError> {
		let rows = payload.num_rows();
		let sequence_no = self.next_sequence.fetch_add(1, Ordering::Relaxed);
		let request = TableWriteRequest {
			meta: TableWriteRequestMeta {
				parent_write_id: ClientWriteId {
					client_uuid: self.client_uuid,
					sequence_no,
					idempotency_ttl_ms: 60_000,
					_pad: 0,
				},
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
			},
			descriptor: descriptor.clone(),
			payload,
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
		let sequence_no = self.next_sequence.fetch_add(1, Ordering::Relaxed);
		let request = TableDeleteRowsRequest {
			meta: TableDeleteRowsRequestMeta {
				parent_write_id: ClientWriteId {
					client_uuid: self.client_uuid,
					sequence_no,
					idempotency_ttl_ms: 60_000,
					_pad: 0,
				},
				client_hlc: options.client_hlc.unwrap_or_else(default_client_hlc),
				expected_epoch: self.inner.epoch(),
				tenant_id,
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				row_count,
				durability: options.durability,
				await_finalization: u8::from(options.await_finalization),
				_pad: [0; 10],
			},
			descriptor: descriptor.clone(),
			row_key_ids,
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
			status => Err(ClientError::WriteFailed { status }),
		}
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
		let stream = self.query_stream(tenant_id, pbc, options).await?;
		let epoch = stream.epoch;
		let value_hlc = stream.value_hlc;
		let rows = stream.rows;
		let batches = stream.collect_all().await?;
		Ok(QueryResult {
			epoch,
			value_hlc,
			rows,
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
				request_id: TableQueryRequestId::ZERO,
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
			Err(error) => Err(error.into()),
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
			Err(error) => Err(error.into()),
		}
	}

	pub async fn table_query(
		&self,
		tenant_id: TenantId,
		descriptor: &TableSchemaDescriptor,
		pbc: Vec<u8>,
		options: TableQueryOptions,
	) -> Result<QueryResult, ClientError> {
		let stream = self
			.table_query_stream(tenant_id, descriptor, pbc, options)
			.await?;
		let epoch = stream.epoch;
		let value_hlc = stream.value_hlc;
		let rows = stream.rows;
		let batches = stream.collect_all().await?;
		Ok(QueryResult {
			epoch,
			value_hlc,
			rows,
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
		let stream = self
			.table_query_stream_with_runtime_filter(
				tenant_id,
				descriptor,
				pbc,
				runtime_filter,
				options,
			)
			.await?;
		let epoch = stream.epoch;
		let value_hlc = stream.value_hlc;
		let rows = stream.rows;
		let batches = stream.collect_all().await?;
		Ok(QueryResult {
			epoch,
			value_hlc,
			rows,
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
		let stream = self
			.table_query_stream_with_runtime_filter_and_attempt_hook(
				tenant_id,
				descriptor,
				pbc,
				runtime_filter,
				options,
				attempt_hook,
			)
			.await?;
		let epoch = stream.epoch;
		let value_hlc = stream.value_hlc;
		let rows = stream.rows;
		let batches = stream.collect_all().await?;
		Ok(QueryResult {
			epoch,
			value_hlc,
			rows,
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
			request_id: TableQueryRequestId::ZERO,
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
	error.kind() == std::io::ErrorKind::InvalidData
		&& error.to_string().contains("TABLE_DESCRIPTOR_EVOLVED")
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
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
		assert_eq!(envelope.mutation, SchemaMutationKindV1::Create);
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
			mutation: SchemaMutationKindV1::Drop,
			intent_id: SchemaIntentId::new([0xCD; 16]),
		};

		let err = request.encode().unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
	}

	#[test]
	fn table_descriptor_evolved_io_error_is_detected() {
		let err = std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			"Wendelon query failed: TABLE_DESCRIPTOR_EVOLVED",
		);

		assert!(is_table_descriptor_evolved_error(&err));
	}

	#[test]
	fn table_query_options_default_trace_context_absent() {
		assert_eq!(
			TableQueryOptions::default().trace_context,
			TableQueryTraceContext::Absent
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
