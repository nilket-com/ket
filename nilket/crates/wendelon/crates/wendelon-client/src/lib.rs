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
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, ReadRequest,
	WendelonClient as InnerClient, WendelonClientConfig, WriteRequest,
};
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
use wendelon::schema::{
	ClientWriteId, FinalizationMarker, HybridTimestamp, Identity, NodeId, ReadRequestMeta,
	ReadStatus, TenantId, WriteRequestMeta, WriteStatus, read_key_schema, write_payload_schema,
};

pub use wendelon::client::{QueryLocalityRange, StreamingQueryResult};
pub use wendelon::schema::{Consistency as ReadConsistency, DurabilityLevel as DurabilityClass};

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
}

impl Default for QueryOptions {
	fn default() -> Self {
		Self {
			range: QueryLocalityRange::full(),
			consistency: ReadConsistency::LINEARIZABLE,
			min_hlc: None,
			as_of_hlc: None,
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
	#[error("read failed with status {status:?}")]
	ReadFailed { status: ReadStatus },
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

	pub fn identity_for_key(tenant_id: TenantId, key: &[u8]) -> Identity {
		Identity::blake3_tenant(tenant_id, key)
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
		wpbc: Vec<u8>,
		options: QueryOptions,
	) -> Result<StreamingQueryResult, ClientError> {
		Ok(self
			.inner
			.query(
				tenant_id,
				wpbc,
				options.range,
				self.inner.epoch(),
				options.min_hlc.unwrap_or_else(zero_hlc),
				options.consistency,
				options.as_of_hlc.unwrap_or_else(zero_hlc),
			)
			.await?)
	}

	pub async fn query(
		&self,
		tenant_id: TenantId,
		wpbc: Vec<u8>,
		options: QueryOptions,
	) -> Result<QueryResult, ClientError> {
		let stream = self.query_stream(tenant_id, wpbc, options).await?;
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
