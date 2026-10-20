use arrow::array::{ArrayRef, ListBuilder, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::sync::watch;
use wendelon::client::{
	ClusterStatusWire, StaticTopologySource, TopologySource, WendelonClientConfig,
};
use wendelon::network::core::{HRFT_MAGIC, HrftFrame, read_frame, write_frame};
use wendelon::network::factory::TransportProviderFactory;
use wendelon::network::pool::BufPool;
use wendelon::network::server::HrftService;
use wendelon::network::transport::{
	EpochCapabilityResolver, MembershipEpoch, NodeCapabilitySet, NodeRole, NodeRoleSet,
	PeerCapabilities, TransportProvider, TransportTier, TransportTierSet,
};
use wendelon::node::{NodeBootstrapConfig, WendelonNode};
use wendelon::schema::{FrameHeader, MsgType, NodeId};
use wendelon::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};

const ZERO_NODE: NodeId = NodeId {
	id: 0,
	_reserved: 0,
};

pub type LoopbackRegistry = SimulatedTransportNetwork;

#[path = "../../src/test_support/test_dir.rs"]
mod test_dir;
pub(crate) use test_dir::TestDir;

/// A scratch dir removed on drop; hold it for as long as any node uses it.
pub fn tempdir(tag: &str) -> TestDir {
	TestDir::new(&format!("wendelon-control-ring-{tag}"))
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
	node.handles.abort();
}

/// A single-node cluster and the scratch dir that backs it; keep both for the whole test.
#[allow(dead_code)]
pub async fn bootstrap_node(tag: &str, node_id: u32) -> (TestDir, WendelonNode) {
	let dir = tempdir(tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		node_id,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	(dir, node)
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
	Arc::new(SimulatedTransportProvider::new(
		local,
		registry,
		BufPool::new(64, 1024 * 1024),
		TransportTier::QuicIoUring,
	))
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
				capabilities: NodeCapabilitySet::empty(),
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
		Arc::new(SimulatedTransportProvider::new(
			local,
			registry,
			pool.clone(),
			TransportTier::QuicIoUring,
		)),
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
	let services = services
		.into_iter()
		.map(|(remote, service)| (remote, format!("loopback://node-{}", remote.id), service))
		.collect();
	loopback_client_config_published_many_with_topology(bootstrap, topology, services)
}

#[allow(dead_code)]
pub fn loopback_client_config_published_many_with_topology(
	bootstrap: ClusterStatusWire,
	topology: Arc<dyn TopologySource>,
	services: Vec<(NodeId, String, Arc<dyn HrftService>)>,
) -> WendelonClientConfig {
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let mut providers: HashMap<TransportTier, Arc<dyn TransportProvider>> = HashMap::new();
	let registry = shared_loopback_registry();
	for (remote, address, service) in services {
		registry.publish(remote, &address, service);
	}
	providers.insert(
		TransportTier::QuicIoUring,
		Arc::new(SimulatedTransportProvider::new(
			ZERO_NODE,
			registry,
			BufPool::new(64, 1024 * 1024),
			TransportTier::QuicIoUring,
		)),
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
