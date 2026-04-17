use arrow::array::{ArrayRef, ListBuilder, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::watch;
use wendelon::client::{
	ClusterStatusWire, StaticTopologySource, TopologySource, WendelonClientConfig,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use wendelon::network::factory::TransportProviderFactory;
use wendelon::network::pool::BufPool;
use wendelon::network::server::HrftService;
use wendelon::network::transport::{
	EpochCapabilityResolver, MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities,
	TransportConnection, TransportProvider, TransportStream, TransportTier, TransportTierSet,
};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{FrameHeader, MsgType, NodeId};

const ZERO_NODE: NodeId = NodeId {
	id: 0,
	_reserved: 0,
};

#[derive(Default)]
pub struct LoopbackRegistry {
	services: Mutex<HashMap<NodeId, PublishedLoopbackService>>,
}

struct PublishedLoopbackService {
	service: Arc<dyn HrftService>,
}

impl LoopbackRegistry {
	fn publish(&self, node_id: NodeId, address: &str, service: Arc<dyn HrftService>) {
		let _ = address;
		self.services
			.lock()
			.unwrap()
			.insert(node_id, PublishedLoopbackService { service });
	}

	fn resolve(
		&self,
		target: NodeId,
		expected_address: &str,
	) -> std::io::Result<Arc<dyn HrftService>> {
		let guard = self.services.lock().unwrap();
		let published = guard.get(&target).ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("unknown loopback target {}", target.id),
			)
		})?;
		let _ = expected_address;
		Ok(published.service.clone())
	}

	fn contains(&self, target: NodeId, _expected_address: &str) -> bool {
		self.services.lock().unwrap().contains_key(&target)
	}
}

pub fn tempdir(tag: &str) -> PathBuf {
	let base = std::env::temp_dir().join(format!(
		"wendelon-control-ring-{tag}-{}",
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&base).unwrap();
	base
}

#[allow(dead_code)]
pub async fn wait_for_local_leader(node: &WendelonNode) {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader, current leader = {:?}",
			node.raft.current_leader().await
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

#[allow(dead_code)]
pub fn abort_node(node: &WendelonNode) {
	node.handles.ring_updater.abort();
	node.handles.drift_monitor.abort();
	node.handles.eviction_watchdog.abort();
	node.handles.lease_manager.abort();
	node.handles.epoch_reconciler.abort();
	node.handles.migration_sweeper.abort();
}

#[allow(dead_code)]
pub async fn bootstrap_node(tag: &str, node_id: u32) -> WendelonNode {
	WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		node_id,
		tempdir(tag),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap()
}

#[allow(dead_code)]
pub fn shared_loopback_registry() -> Arc<LoopbackRegistry> {
	Arc::new(LoopbackRegistry::default())
}

#[allow(dead_code)]
pub fn loopback_provider(
	local: NodeId,
	registry: Arc<LoopbackRegistry>,
) -> Arc<dyn TransportProvider> {
	Arc::new(LoopbackProvider {
		local,
		registry,
		pool: BufPool::new(64, 1024 * 1024),
		tier: TransportTier::QuicIoUring,
	})
}

#[allow(dead_code)]
pub fn loopback_factory(
	remote: NodeId,
	service: Arc<dyn HrftService>,
) -> (Arc<TransportProviderFactory>, Arc<BufPool>) {
	loopback_factory_from(ZERO_NODE, remote, service)
}

#[allow(dead_code)]
pub fn loopback_factory_from(
	local: NodeId,
	remote: NodeId,
	service: Arc<dyn HrftService>,
) -> (Arc<TransportProviderFactory>, Arc<BufPool>) {
	loopback_factory_many_from(local, vec![(remote, service)])
}

#[allow(dead_code)]
pub fn loopback_factory_many(
	services: Vec<(NodeId, Arc<dyn HrftService>)>,
) -> (Arc<TransportProviderFactory>, Arc<BufPool>) {
	loopback_factory_many_from(ZERO_NODE, services)
}

#[allow(dead_code)]
pub fn loopback_factory_many_from(
	local: NodeId,
	services: Vec<(NodeId, Arc<dyn HrftService>)>,
) -> (Arc<TransportProviderFactory>, Arc<BufPool>) {
	let pool = BufPool::new(64, 1024 * 1024);
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let registry = shared_loopback_registry();

	let mut peers = HashMap::new();
	for (remote, service) in services {
		let address = format!("loopback://node-{}", remote.id);
		peers.insert(
			remote,
			PeerCapabilities {
				address: address.clone(),
				tiers,
				roles: NodeRoleSet::empty()
					.with(NodeRole::Control)
					.with(NodeRole::Data),
			},
		);
		registry.publish(remote, &address, service);
	}
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_tx, rx) = watch::channel(Arc::new(epoch));
	let resolver = Arc::new(EpochCapabilityResolver::new(rx.clone()));

	let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
	providers.insert(
		TransportTier::QuicIoUring,
		Arc::new(LoopbackProvider {
			local,
			registry,
			pool: pool.clone(),
			tier: TransportTier::QuicIoUring,
		}),
	);

	let factory = TransportProviderFactory::new(tiers, providers, resolver, rx);
	(factory, pool)
}

#[allow(dead_code)]
pub fn loopback_client_config(
	remote: NodeId,
	service: Arc<dyn HrftService>,
	bootstrap: ClusterStatusWire,
) -> WendelonClientConfig {
	loopback_client_config_many_with_topology(
		bootstrap.clone(),
		Arc::new(StaticTopologySource::new(bootstrap)),
		vec![(remote, service)],
	)
}

#[allow(dead_code)]
pub fn loopback_client_config_many(
	bootstrap: ClusterStatusWire,
	services: Vec<(NodeId, Arc<dyn HrftService>)>,
) -> WendelonClientConfig {
	loopback_client_config_many_with_topology(
		bootstrap.clone(),
		Arc::new(StaticTopologySource::new(bootstrap)),
		services,
	)
}

#[allow(dead_code)]
pub fn loopback_client_config_many_with_topology(
	bootstrap: ClusterStatusWire,
	topology: Arc<dyn TopologySource>,
	services: Vec<(NodeId, Arc<dyn HrftService>)>,
) -> WendelonClientConfig {
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
	let registry = shared_loopback_registry();
	for (remote, service) in services {
		let address = format!("loopback://node-{}", remote.id);
		registry.publish(remote, &address, service);
	}
	providers.insert(
		TransportTier::QuicIoUring,
		Arc::new(LoopbackProvider {
			local: ZERO_NODE,
			registry,
			pool: BufPool::new(64, 1024 * 1024),
			tier: TransportTier::QuicIoUring,
		}),
	);
	WendelonClientConfig {
		local_tiers: tiers,
		providers,
		bootstrap,
		topology,
		pool_max_retained: 64,
		pool_max_cap_bytes: 1024 * 1024,
	}
}

#[allow(dead_code)]
pub fn loopback_client_config_with_topology(
	remote: NodeId,
	service: Arc<dyn HrftService>,
	bootstrap: ClusterStatusWire,
	topology: Arc<dyn TopologySource>,
) -> WendelonClientConfig {
	loopback_client_config_many_with_topology(bootstrap, topology, vec![(remote, service)])
}

#[allow(dead_code)]
pub async fn call_frame(
	factory: &Arc<TransportProviderFactory>,
	pool: &Arc<BufPool>,
	target: NodeId,
	msg_type: MsgType,
	meta: &[u8],
	arrow_payload: Option<&[u8]>,
	trailing: Option<&[u8]>,
) -> std::io::Result<HrftFrame> {
	let conn = factory.get_connection(target).await?;
	let mut stream = conn.open_bi().await?;
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: msg_type.0,
		flags: 0,
		stream_id: 0,
		meta_len: meta.len() as u32,
		arrow_len: arrow_payload.map(|bytes| bytes.len() as u32).unwrap_or(0),
		trailing_len: trailing.map(|bytes| bytes.len() as u32).unwrap_or(0),
	};
	write_frame(&mut *stream, &header, meta, arrow_payload, trailing).await?;
	stream.finish().await?;
	read_frame(&mut *stream, pool).await
}

#[allow(dead_code)]
pub fn encode_batch(batch: &arrow::record_batch::RecordBatch) -> std::io::Result<Vec<u8>> {
	let mut buf = Vec::new();
	{
		let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
			.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("encode batch: {e}")))?;
		writer
			.write(batch)
			.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("write batch: {e}")))?;
		writer
			.finish()
			.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("finish batch: {e}")))?;
	}
	Ok(buf)
}

#[allow(dead_code)]
pub fn epoch_config_batch(
	peers: &[(u32, &str, u8, u8)],
	voters: &[u32],
	learners: &[u32],
) -> RecordBatch {
	let mut addresses = HashMap::new();
	for (id, address, _, _) in peers {
		addresses.insert(*id, *address);
	}

	let voter_addresses: Vec<&str> = voters
		.iter()
		.map(|id| *addresses.get(id).expect("voter address missing"))
		.collect();
	let learner_addresses: Vec<&str> = learners
		.iter()
		.map(|id| *addresses.get(id).expect("learner address missing"))
		.collect();

	let mut meta_keys = Vec::new();
	let mut meta_values = Vec::new();
	for (id, _, tier_bits, role_bits) in peers {
		meta_keys.push(format!("node.{id}.tier_bits"));
		meta_values.push(tier_bits.to_string());
		meta_keys.push(format!("node.{id}.role_bits"));
		meta_values.push(role_bits.to_string());
	}

	let columns: Vec<ArrayRef> = vec![
		Arc::new(list_u32(voters)),
		Arc::new(list_utf8(&voter_addresses)),
		Arc::new(list_u32(learners)),
		Arc::new(list_utf8(&learner_addresses)),
		Arc::new(list_utf8_owned(&meta_keys)),
		Arc::new(list_utf8_owned(&meta_values)),
	];
	RecordBatch::try_new(
		Arc::new(wendelon::schema::epoch_entry_var_schema()),
		columns,
	)
	.unwrap()
}

#[allow(dead_code)]
fn list_u32(values: &[u32]) -> arrow::array::ListArray {
	let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
		"item",
		DataType::UInt32,
		false,
	));
	for value in values {
		builder.values().append_value(*value);
	}
	builder.append(true);
	builder.finish()
}

#[allow(dead_code)]
fn list_utf8(values: &[&str]) -> arrow::array::ListArray {
	let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
		"item",
		DataType::Utf8,
		false,
	));
	for value in values {
		builder.values().append_value(*value);
	}
	builder.append(true);
	builder.finish()
}

#[allow(dead_code)]
fn list_utf8_owned(values: &[String]) -> arrow::array::ListArray {
	let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
		"item",
		DataType::Utf8,
		false,
	));
	for value in values {
		builder.values().append_value(value);
	}
	builder.append(true);
	builder.finish()
}

struct DuplexTransportStream {
	inner: DuplexStream,
}

impl AsyncRead for DuplexTransportStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for DuplexTransportStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[async_trait]
impl TransportStream for DuplexTransportStream {
	fn stream_id(&self) -> u64 {
		0
	}

	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.inner.shutdown().await
	}
}

struct LoopbackConnection {
	local: NodeId,
	remote: NodeId,
	registry: Arc<LoopbackRegistry>,
	address: String,
	pool: Arc<BufPool>,
	tier: TransportTier,
}

#[async_trait]
impl TransportConnection for LoopbackConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		let service = self.registry.resolve(self.remote, &self.address)?;
		let (client_side, server_side) = tokio::io::duplex(64 * 1024);
		let pool = self.pool.clone();
		let local = self.local;
		tokio::spawn(serve_stream(server_side, service, pool, local));
		Ok(Box::new(DuplexTransportStream { inner: client_side }))
	}

	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		unreachable!("loopback connection does not accept server-initiated streams")
	}

	fn remote_id(&self) -> NodeId {
		self.remote
	}

	fn transport_type(&self) -> TransportTier {
		self.tier
	}

	fn is_closed(&self) -> bool {
		!self.registry.contains(self.remote, &self.address)
	}
}

struct LoopbackProvider {
	local: NodeId,
	registry: Arc<LoopbackRegistry>,
	pool: Arc<BufPool>,
	tier: TransportTier,
}

#[async_trait]
impl TransportProvider for LoopbackProvider {
	async fn connect(
		&self,
		target: NodeId,
		address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		self.registry.resolve(target, address)?;
		Ok(Arc::new(LoopbackConnection {
			local: self.local,
			remote: target,
			registry: self.registry.clone(),
			address: address.to_string(),
			pool: self.pool.clone(),
			tier: self.tier,
		}))
	}

	fn publish_local(
		&self,
		node_id: NodeId,
		address: &str,
		service: Arc<dyn HrftService>,
	) -> std::io::Result<()> {
		self.registry.publish(node_id, address, service);
		Ok(())
	}
}

async fn serve_stream(
	stream: DuplexStream,
	service: Arc<dyn HrftService>,
	pool: Arc<BufPool>,
	remote: NodeId,
) {
	let _ = service
		.handle_stream(
			Box::new(DuplexTransportStream { inner: stream }),
			pool,
			remote,
		)
		.await;
}
