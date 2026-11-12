//! The smallest three-node workflow: three real nodes with separate stores and three voters, real
//! table writes and queries through their service, a slab tiered to a shared file object store and
//! its local copy reclaimed, then a follower restart and a leader failover, with the acknowledged
//! rows read back exactly at every stage.
//!
//! What a read here promises, from the client and the query gate. A client routes to its status's
//! leader when that node is a replica, and with three replicas of everything each node is one. So
//! naming a node as leader in the client's status is this test's way to enter there. It is a hint
//! to target choice, not a claim about leadership. Every configured node stays in that status while
//! stopped, because stopping a node does not take it out of the ring, and the query's targets are
//! recorded: the first must be the entry, and a redirect is followed and shown. A linearizable query carries the newest acknowledged commit as
//! `min_hlc`, and a node whose safe watermark is behind it refuses, which the client retries. So an
//! answer means that node claims to have caught up, and from then on any other set of rows is a
//! failure, at once. Writes are acknowledged at quorum, so a stopped or slow third node is allowed.
//!
//! The table is provisioned through the control log, so schema submission through the service is
//! not covered here.
//!
//! What this does not cover. Transport is the in-process simulated network, not a production
//! network. The object store is a local directory in the default test; the `--ignored` variant puts it
//! on a real RustFS server, still one process and one simulated network. A stop here is a graceful
//! shutdown on the same directory, not a power loss. Backup and restore, Root2, scale, memory and
//! chaos are elsewhere or nowhere.

#[path = "support/rustfs_env.rs"]
mod rustfs_env;
#[path = "support/control_ring.rs"]
mod support;

use arrow::array::{Array, Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use wendelon::client::{
	ClusterPeerWire, ClusterStatusWire, RowIdentityProjection, TableQueryAttemptGuard,
	TableQueryRequest, TableWriteRequest, WendelonClient,
};
use wendelon::consensus::StoreIncarnation;
use wendelon::consensus::WendelonRequest;
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use wendelon::metrics::observability::ReadinessReporter;
use wendelon::network::server::HrftService;
use wendelon::network::transport::{
	NodeRole, NodeRoleSet, TransportProvider, TransportTier, TransportTierSet,
};
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig, WendelonNode};
use wendelon::schema::{
	ClientWriteId, Consistency, DurabilityLevel, HybridTimestamp, LocalityPolicy,
	MutationCommitKnowledge, MutationStopReason, NodeId, PrimaryKeyDescriptor, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableQueryRequestId, TableQueryRequestMeta,
	TableSchemaDescriptor, TableWriteRequestMeta, TenantId, WriteResponseMeta, WriteStatus,
	WriterNodeId, encode_table_schema_descriptor_v1, encode_table_write_row_count,
	validate_table_write_request_batch,
};
use wendelon::storage::core::StorageEngine;
use wendelon::storage::manager::RehydrationPolicy;
use wendelon::storage::{ObjectStore, ObjectStoreConfig, SweeperConfig};

const NODES: [u32; 3] = [1, 2, 3];

/// The whole workflow, every await in it. The polls' shared deadline comes first so that the usual
/// failure says what it saw; this bound catches an await that never returns. It fails the test. It
/// does not forcibly stop blocking work.
const WORKFLOW_BOUND: Duration = Duration::from_secs(420);
const POLL_BOUND: Duration = Duration::from_secs(390);

type Rows = BTreeMap<i64, String>;

struct Member {
	_dir: support::TestDir,
	cfg: NodeBootstrapConfig,
	node: Option<WendelonNode>,
}

struct Cluster {
	registry: Arc<support::LoopbackRegistry>,
	members: Vec<Member>,
	descriptor: TableSchemaDescriptor,
	expected: Rows,
	next_id: i64,
	/// The client identity every write is issued under.
	client_uuid: [u8; 16],
	/// Each write attempt's own id, so a write known not to have committed can be sent as new.
	next_write_sequence: u64,
	/// How many writes the service said were not committed and the test sent again.
	writes_sent_again: u64,
	/// The newest acknowledged commit, which every later read must have caught up to.
	newest_ack: HybridTimestamp,
	polls_end: tokio::time::Instant,
	/// The nodes' shared object store, opened by the test through the same configuration.
	objects: Arc<dyn ObjectStore>,
}

/// A slab one node tiered and reclaimed, and a row it holds.
struct TieredRow {
	node_id: u32,
	slab_id: wendelon::schema::SlabId,
	l2_uri: String,
	local: PathBuf,
	row: i64,
}

/// A query that could not finish: refused for good, or not yet.
enum ReadFailure {
	Transient(String),
	Refused(String),
}

fn read_failure(error: std::io::Error) -> ReadFailure {
	// What the client's retry budget ends in when a node stays behind or a move is incomplete.
	match error.kind() {
		std::io::ErrorKind::NotConnected
		| std::io::ErrorKind::WouldBlock
		| std::io::ErrorKind::ConnectionRefused
		| std::io::ErrorKind::ConnectionReset
		| std::io::ErrorKind::BrokenPipe => ReadFailure::Transient(error.to_string()),
		_ => ReadFailure::Refused(error.to_string()),
	}
}

/// The client's own dial, refused by the simulated transport because the peer is one this fixture
/// stopped. Exactly that: the kind and the whole message the transport factory and the simulated
/// network produce for that node's address. A stopped node is not published; the QUIC transport
/// reports the same event as `ConnectionRefused`, which `read_failure` already retries. Anything
/// else `NotFound`, the same refusal for a running node, or a different target stays final.
fn dial_to_a_stopped_peer(error: &std::io::Error, stopped: &[u32]) -> bool {
	error.kind() == std::io::ErrorKind::NotFound
		&& stopped.iter().any(|node_id| {
			error.to_string()
				== format!(
					"connect via QuicIoUring to {} failed: simulated transport target {node_id} is not published",
					address(*node_id)
				)
		})
}

fn address(node_id: u32) -> String {
	format!("loopback://node-{node_id}")
}

fn provider(node_id: u32, registry: Arc<support::LoopbackRegistry>) -> Arc<dyn TransportProvider> {
	support::loopback_provider(
		NodeId {
			id: node_id,
			_reserved: 0,
		},
		registry,
	)
}

/// Which decision-signing keys the members run with.
#[derive(Clone, Copy)]
enum DecisionKeys {
	/// Unset: the development keys.
	Development,
	/// ADR 0721: a secret of each node's own, with every node's verifying key named, as a
	/// configuration file supplies them.
	OfTheirOwn,
}

fn own_decision_secret(node_id: u32) -> [u8; 32] {
	let mut secret = [0xD0; 32];
	secret[..4].copy_from_slice(&node_id.to_le_bytes());
	secret
}

fn member_config(
	node_id: u32,
	l2: &ObjectStoreConfig,
	keys: DecisionKeys,
) -> (support::TestDir, NodeBootstrapConfig) {
	let dir = support::tempdir(&format!("three-node-{node_id}"));
	let mut cfg = NodeBootstrapConfig::dev(node_id, dir.to_path_buf(), address(node_id))
		.with_parent_decisions_for_disposable_store();
	if let DecisionKeys::OfTheirOwn = keys {
		cfg = cfg.with_decision_keys(
			own_decision_secret(node_id),
			NODES
				.into_iter()
				.map(|id| {
					(
						id,
						wendelon::consensus::decisions::decision_verifying_key_bytes(
							own_decision_secret(id),
						),
					)
				})
				.collect(),
		);
	}
	cfg.rf = NODES.len();
	cfg.cluster = if node_id == NODES[0] {
		ClusterBootstrapConfig::Bootstrap
	} else {
		ClusterBootstrapConfig::Join {
			seeds: vec![SeedNodeConfig {
				node_id: NODES[0],
				address: address(NODES[0]),
			}],
		}
	};
	for trusted in NODES {
		cfg.trusted_lease_signers.insert(
			trusted,
			lease_verifying_key_bytes(dev_lease_signing_secret(trusted)),
		);
		cfg.trusted_transfer_signers.insert(
			trusted,
			transfer_verifying_key_bytes(dev_transfer_signing_secret(trusted)),
		);
	}
	cfg.slab_max_bytes = 8 * 1024;
	cfg.l2 = Some(l2.clone());
	// A slab stays off local disk once reclaimed, so a read of it cannot be served from a copy.
	cfg.rehydration = RehydrationPolicy {
		promote_after_l2_reads: 0,
		..RehydrationPolicy::default()
	};
	cfg.tiering = SweeperConfig {
		scan_interval: Duration::from_millis(100),
		min_slab_bytes: 1,
		..SweeperConfig::default()
	};
	(dir, cfg)
}

/// A UUIDv7 client identity issued now: the parent decisions authority keys a write by it.
fn client_uuid_v7() -> [u8; 16] {
	let now_ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut bytes = uuid::Uuid::new_v4().into_bytes();
	bytes[..6].copy_from_slice(&now_ms.to_be_bytes()[2..]);
	bytes[6] = 0x70 | (bytes[6] & 0x0f);
	bytes[8] = 0x80 | (bytes[8] & 0x3f);
	bytes
}

/// What a write's answer means to a client that wants its row.
#[derive(Debug, PartialEq, Eq)]
enum WriteAnswer {
	/// Committed: OK, or a replay of a commit whose answer was lost. Only this is an
	/// acknowledgement.
	Acknowledged,
	/// Known not to have committed: the service recorded the write as refused. A client that
	/// still wants the row sends a new write.
	SendAgain,
	/// The client's quick retries ran out, whatever knowledge that call's summary carries.
	/// A call summarizes only its own attempts: an earlier call of the same write id may have
	/// left a write that can still commit. So a client that still wants the row asks again
	/// with the same write id, and the service's record of that id decides.
	AskAgain,
	/// Anything else is an answer the client cannot act on.
	Other,
}

fn write_answer(write: &WriteResponseMeta) -> WriteAnswer {
	let (status, knowledge) = (write.status, write.mutation_commit_knowledge());
	if knowledge == MutationCommitKnowledge::COMMITTED
		&& (status == WriteStatus::OK || status == WriteStatus::DUPLICATE)
	{
		WriteAnswer::Acknowledged
	} else if knowledge == MutationCommitKnowledge::KNOWN_NOT_COMMITTED && status == WriteStatus::OK
	{
		WriteAnswer::SendAgain
	} else if status == WriteStatus::WRITE_ATTEMPTS_EXHAUSTED
		&& (knowledge == MutationCommitKnowledge::INDETERMINATE
			|| knowledge == MutationCommitKnowledge::KNOWN_NOT_COMMITTED)
	{
		WriteAnswer::AskAgain
	} else {
		WriteAnswer::Other
	}
}

/// OK alone is not an acknowledgement: the service answers OK for a write it recorded as
/// refused.
#[test]
fn only_a_committed_answer_acknowledges_a_row() {
	let answer = |status: WriteStatus, knowledge: MutationCommitKnowledge| {
		let write: WriteResponseMeta = bytemuck::Zeroable::zeroed();
		let mut write = write.with_mutation_outcome(knowledge, MutationStopReason::NONE);
		write.status = status;
		write_answer(&write)
	};
	use MutationCommitKnowledge as K;
	assert_eq!(
		answer(WriteStatus::OK, K::COMMITTED),
		WriteAnswer::Acknowledged
	);
	assert_eq!(
		answer(WriteStatus::DUPLICATE, K::COMMITTED),
		WriteAnswer::Acknowledged
	);
	assert_eq!(
		answer(WriteStatus::OK, K::KNOWN_NOT_COMMITTED),
		WriteAnswer::SendAgain
	);
	assert_eq!(
		answer(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			K::KNOWN_NOT_COMMITTED
		),
		WriteAnswer::AskAgain
	);
	assert_eq!(
		answer(WriteStatus::OK, K::INDETERMINATE),
		WriteAnswer::Other
	);
	assert_eq!(
		answer(WriteStatus::DUPLICATE, K::KNOWN_NOT_COMMITTED),
		WriteAnswer::Other
	);
	assert_eq!(
		answer(WriteStatus::WRITE_ATTEMPTS_EXHAUSTED, K::INDETERMINATE),
		WriteAnswer::AskAgain
	);
	assert_eq!(
		answer(WriteStatus::WRITE_STALLED, K::INDETERMINATE),
		WriteAnswer::Other
	);
}

fn table_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("name", DataType::LargeUtf8, true),
	])
}

fn table_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([0x3a; 16]),
		1,
		"workflow",
		"rows",
		&table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn peer(node_id: u32) -> ClusterPeerWire {
	ClusterPeerWire {
		node_id,
		address: address(node_id),
		tier_bits: TransportTierSet::empty()
			.with(TransportTier::QuicIoUring)
			.as_u8(),
		role_bits: NodeRoleSet::empty().with(NodeRole::Data).as_u8(),
		capability_bits: 0,
	}
}

impl Cluster {
	fn live(&self) -> Vec<&WendelonNode> {
		self.members
			.iter()
			.filter_map(|member| member.node.as_ref())
			.collect()
	}

	fn node(&self, node_id: u32) -> &WendelonNode {
		self.live()
			.into_iter()
			.find(|node| node.local_node.id == node_id)
			.unwrap_or_else(|| panic!("node {node_id} is not running"))
	}

	/// A client that knows every running node and enters at `enter`.
	fn client_entering_at(&self, enter: u32) -> Arc<WendelonClient> {
		let status = ClusterStatusWire {
			epoch: self.node(enter).ring.current_epoch(),
			leader: Some(enter),
			replication_factor: NODES.len() as u32,
			peers: NODES.iter().map(|id| peer(*id)).collect(),
		};
		// Every member stays in the status. Only a running one can be connected to.
		let services = self
			.live_ids()
			.iter()
			.map(|id| {
				let node = self.node(*id);
				let service: Arc<dyn HrftService> = node.router.clone();
				(node.local_node, service)
			})
			.collect();
		WendelonClient::new(support::loopback_client_config_many(status, services))
	}

	async fn wait_for_ready(&self) {
		for node in self.live() {
			loop {
				let snapshot = node.readiness_reporter().snapshot().await;
				if snapshot.is_ready() {
					break;
				}
				assert!(
					tokio::time::Instant::now() < self.polls_end,
					"node {} never became ready: {snapshot:?}",
					node.local_node.id
				);
				tokio::time::sleep(Duration::from_millis(50)).await;
			}
		}
	}

	async fn wait_for_voters(&self) {
		for node in self.live() {
			loop {
				let snapshot = node.state_machine.state_snapshot().await;
				let voters = snapshot.last_membership.voter_ids().collect::<Vec<_>>();
				if voters == NODES {
					break;
				}
				assert!(
					tokio::time::Instant::now() < self.polls_end,
					"node {} never saw three voters: {voters:?}",
					node.local_node.id
				);
				tokio::time::sleep(Duration::from_millis(50)).await;
			}
		}
	}

	/// The store identity this running node has registered, read from its own state machine once
	/// it has observed that registration. Nothing has superseded it.
	async fn registered_incarnation(&self, node_id: u32) -> StoreIncarnation {
		loop {
			let snapshot = self.node(node_id).state_machine.state_snapshot().await;
			if let Some(registry) = snapshot.incarnations.get(&node_id) {
				if let Some(current) = registry.current {
					assert!(registry.retired.is_empty(), "{registry:?}");
					return current;
				}
			}
			assert!(
				tokio::time::Instant::now() < self.polls_end,
				"node {node_id} never observed its own store's registration"
			);
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}

	fn live_ids(&self) -> Vec<u32> {
		self.live()
			.into_iter()
			.map(|node| node.local_node.id)
			.collect()
	}

	async fn current_leader(&self) -> Option<u32> {
		let live = self.live();
		let first = live[0].raft.current_leader().await?;
		for node in &live[1..] {
			if node.raft.current_leader().await != Some(first) {
				return None;
			}
		}
		self.live_ids().contains(&first).then_some(first)
	}

	async fn wait_for_leader(&self, what: &str) -> u32 {
		loop {
			if let Some(leader) = self.current_leader().await {
				return leader;
			}
			assert!(
				tokio::time::Instant::now() < self.polls_end,
				"{what}: the running nodes never agreed on a leader"
			);
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}

	fn request_for(&self, id: i64, name: &str, epoch: u64, sequence: u64) -> TableWriteRequest {
		let batch = RecordBatch::try_new(
			Arc::new(table_schema()),
			vec![
				Arc::new(Int64Array::from(vec![id])),
				Arc::new(LargeStringArray::from(vec![name])),
			],
		)
		.unwrap();
		TableWriteRequest {
			meta: TableWriteRequestMeta {
				parent_write_id: ClientWriteId {
					client_uuid: self.client_uuid,
					sequence_no: sequence,
					idempotency_ttl_ms: 30_000,
					_pad: 0,
				},
				client_hlc: HybridTimestamp {
					physical_ms: 1,
					logical: 0,
					node_id: 0,
				},
				expected_epoch: epoch,
				tenant_id: TenantId::ZERO,
				table_object_id: self.descriptor.table_object_id,
				schema_generation: self.descriptor.schema_generation,
				child_ordinal_base: 1_000,
				proxy_source: NodeId {
					id: 0,
					_reserved: 0,
				},
				row_count: encode_table_write_row_count(1).unwrap(),
				proxy_hop: 0,
				durability: DurabilityLevel::QUORUM,
				await_finalization: 0,
				_pad: [0; 3],
				deadline_remaining_millis: 0,
			},
			descriptor: self.descriptor.clone(),
			payload: batch,
			deadline: None,
		}
	}

	/// One acknowledged write through the running nodes' service. A row is expected only once
	/// it is acknowledged. A transport failure while the cluster settles retries the same id and
	/// write id within the poll bound, as a retrying client does. So does an exhausted answer,
	/// whatever knowledge it carries. A write the service recorded as refused goes again as a
	/// new write of the same row. Any other answer but OK fails.
	async fn write_one(&mut self, what: &str) {
		self.next_id += 1;
		let id = self.next_id;
		let name = format!("{what}-{id}-{}", "x".repeat(256));
		self.next_write_sequence += 1;
		let mut sequence = self.next_write_sequence;
		let mut last = String::new();
		loop {
			assert!(
				tokio::time::Instant::now() < self.polls_end,
				"{what}: row {id} was never acknowledged: {last}"
			);
			let leader = self.wait_for_leader(what).await;
			let epoch = self.node(leader).ring.current_epoch();
			let client = self.client_entering_at(leader);
			let request = self.request_for(id, &name, epoch, sequence);
			match client.table_write(request).await {
				// A replay of a commit whose answer was lost is that commit's acknowledgement.
				Ok(result) if write_answer(&result.response.write) == WriteAnswer::Acknowledged => {
					self.expected.insert(id, name);
					self.newest_ack = self.newest_ack.max(result.response.write.commit_hlc);
					return;
				}
				// The service recorded this write id as refused. A client that still wants the row
				// sends a new write.
				Ok(result) if write_answer(&result.response.write) == WriteAnswer::SendAgain => {
					last = format!("{:?}", result.response);
					self.writes_sent_again += 1;
					self.next_write_sequence += 1;
					sequence = self.next_write_sequence;
				}
				// The retries ran out. The same write id goes again, and its record decides.
				Ok(result) if write_answer(&result.response.write) == WriteAnswer::AskAgain => {
					last = format!("{:?}", result.response);
				}
				// The client has spent its retries on what can be retried, so this is an answer.
				Ok(result) => panic!("{what}: row {id} was refused: {:?}", result.response),
				// The same row and the same write id go again: `sequence` is not advanced.
				Err(error) if dial_to_a_stopped_peer(&error, &self.stopped_ids()) => {
					last = error.to_string();
				}
				Err(error) => match read_failure(error) {
					ReadFailure::Transient(error) => last = error,
					ReadFailure::Refused(error) => {
						panic!("{what}: row {id} was refused for good: {error}")
					}
				},
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}

	/// Every row a query entering at this node returns, with where it went. A row returned twice
	/// is wrong data, not a failure to read.
	async fn rows_through(
		&self,
		node_id: u32,
	) -> (Result<Vec<(i64, String)>, ReadFailure>, Vec<u32>) {
		let targets = Arc::new(std::sync::Mutex::new(Vec::new()));
		let client = self.client_entering_at(node_id);
		let meta = TableQueryRequestMeta {
			min_hlc: self.newest_ack,
			expected_epoch: self.node(node_id).ring.current_epoch(),
			tenant_id: TenantId::ZERO,
			table_object_id: self.descriptor.table_object_id,
			schema_generation: self.descriptor.schema_generation,
			physical_descriptor_fingerprint: *self
				.descriptor
				.physical_descriptor_fingerprint
				.as_bytes(),
			consistency: Consistency::LINEARIZABLE,
			row_identity_projection: RowIdentityProjection::None.as_u8(),
			_pad: [0; 6],
			bytecode_len: 0,
			runtime_filter_len: 0,
			as_of_hlc: bytemuck::Zeroable::zeroed(),
			row_limit_rows: 0,
			deadline_remaining_millis: 0,
			request_id: TableQueryRequestId::from_u128(self.next_id as u128 + 1),
			trace_id: [0; 16],
			trace_parent_span_id: [0; 8],
			trace_flags: 0,
			_trace_pad: [0; 7],
		};
		let seen = targets.clone();
		let started = client
			.table_query_with_attempt_hook(
				TableQueryRequest {
					meta,
					descriptor: self.descriptor.clone(),
					pbc: Vec::new(),
					runtime_filter: None,
					row_identity: RowIdentityProjection::None,
				},
				move |attempt| {
					seen.lock().unwrap().push(attempt.target.id);
					Ok(Box::new(()) as TableQueryAttemptGuard)
				},
			)
			.await;
		let mut result = match started {
			Ok(result) => result,
			Err(error) => {
				let went = targets.lock().unwrap().clone();
				return (Err(read_failure(error)), went);
			}
		};
		let mut rows = Vec::new();
		loop {
			let next = match result.next_batch().await {
				Ok(next) => next,
				Err(error) => {
					let went = targets.lock().unwrap().clone();
					return (Err(read_failure(error)), went);
				}
			};
			let Some(batch) = next else {
				break;
			};
			let ids = batch
				.column_by_name("id")
				.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
				.expect("an id column");
			let names = batch
				.column_by_name("name")
				.and_then(|column| column.as_any().downcast_ref::<LargeStringArray>())
				.expect("a name column");
			for row in 0..batch.num_rows() {
				rows.push((ids.value(row), names.value(row).to_owned()));
			}
		}
		let went = targets.lock().unwrap().clone();
		(Ok(rows), went)
	}

	/// A query entering at this node returns exactly the acknowledged rows. Only a refusal the
	/// client's retries could not outlast is waited out; any answer is judged at once, because
	/// the gate has said this node caught up. Returns where the answering attempt went.
	async fn assert_exact_through(&self, node_id: u32, what: &str) -> u32 {
		loop {
			let (answer, went) = self.rows_through(node_id).await;
			match answer {
				Ok(rows) => {
					assert_eq!(
						went.first(),
						Some(&node_id),
						"{what}: it did not enter there"
					);
					self.assert_exact(node_id, what, &rows, &went);
					return *went.last().expect("an answered query had an attempt");
				}
				Err(ReadFailure::Refused(error)) => {
					panic!(
						"{what}: a query entering at {node_id} was refused for good, going {went:?}: {error}"
					)
				}
				Err(ReadFailure::Transient(error)) => {
					assert!(
						tokio::time::Instant::now() < self.polls_end,
						"{what}: a query entering at {node_id} never answered, going {went:?}: {error}"
					);
					tokio::time::sleep(Duration::from_millis(50)).await;
				}
			}
		}
	}

	fn assert_exact(&self, node_id: u32, what: &str, rows: &[(i64, String)], went: &[u32]) {
		let mut seen = Rows::new();
		for (id, name) in rows {
			assert!(
				seen.insert(*id, name.clone()).is_none(),
				"{what}: entering at {node_id}, going {went:?}: row {id} returned twice"
			);
		}
		let missing = self
			.expected
			.keys()
			.filter(|id| !seen.contains_key(id))
			.take(8)
			.collect::<Vec<_>>();
		let unexpected = seen
			.keys()
			.filter(|id| !self.expected.contains_key(id))
			.take(8)
			.collect::<Vec<_>>();
		let different = seen
			.iter()
			.filter_map(|(id, name)| {
				let wanted = self.expected.get(id)?;
				(wanted != name).then(|| {
					(
						*id,
						wanted[..wanted.len().min(40)].to_owned(),
						name[..name.len().min(40)].to_owned(),
					)
				})
			})
			.take(8)
			.collect::<Vec<_>>();
		assert!(
			missing.is_empty() && unexpected.is_empty() && different.is_empty(),
			"{what}: entering at {node_id}, going {went:?}: {} rows against {} acknowledged; missing {missing:?}, unexpected {unexpected:?}, different {different:?}",
			seen.len(),
			self.expected.len(),
		);
	}

	/// Every running node, as an entry, returns exactly the acknowledged rows.
	async fn assert_exact_everywhere(&self, what: &str) {
		for node_id in self.live_ids() {
			let answered_by = self.assert_exact_through(node_id, what).await;
			eprintln!("workflow: {what}: entered at {node_id}, answered by {answered_by}");
		}
	}

	/// Write until a node's own service has tiered a slab holding an acknowledged row, with its
	/// local copy gone and its object there.
	async fn tier_and_reclaim(&mut self) -> TieredRow {
		loop {
			for member in &self.members {
				let Some(node) = member.node.as_ref() else {
					continue;
				};
				for pointer in node.slab.list_tiered_slabs().await {
					let local = member
						.cfg
						.data_dir
						.join(format!("slab_{:016x}.wnd", pointer.slab_id));
					if !self.object_is_there(&pointer.l2_uri).await || local.exists() {
						continue;
					}
					for id in self.expected.keys() {
						let location = node.slab.lookup(TenantId::ZERO, self.row_identity(*id));
						if location.is_ok_and(|found| {
							found.is_some_and(|found| found.slab_id == pointer.slab_id)
						}) {
							return TieredRow {
								node_id: node.local_node.id,
								slab_id: pointer.slab_id,
								l2_uri: pointer.l2_uri.clone(),
								local,
								row: *id,
							};
						}
					}
				}
			}
			self.write_one("tiering").await;
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	}

	/// The object answers a head through the store, and for a file store the file is there too.
	/// Existence only: what the object holds is shown by the queries, not by this.
	async fn object_is_there(&self, uri: &str) -> bool {
		let on_disk = uri
			.strip_prefix("file://")
			.is_none_or(|path| Path::new(path).exists());
		on_disk && self.objects.head(uri).await.is_ok()
	}

	/// The reclaiming node's data directory holds no payload file for the slab, and the object's
	/// bytes come back whole from the store. The other files left there are printed, not judged.
	async fn assert_payload_is_only_in_the_object(&self, tiered: &TieredRow) {
		assert!(!tiered.local.exists(), "the slab payload is on local disk");
		let head = self.objects.head(&tiered.l2_uri).await.unwrap();
		let bytes = self.objects.get(&tiered.l2_uri).await.unwrap();
		assert!(head.size_bytes > 0 && bytes.len() as u64 == head.size_bytes);
		let slab_name = format!("{:016x}", tiered.slab_id);
		let data_dir = &self.member(tiered.node_id).cfg.data_dir;
		let mut left = Vec::new();
		for entry in std::fs::read_dir(data_dir).unwrap() {
			let name = entry.unwrap().file_name().to_string_lossy().into_owned();
			if name.contains(&slab_name) {
				left.push(name);
			}
		}
		eprintln!(
			"workflow: slab {slab_name} object {} is {} bytes; local files naming it: {left:?}",
			tiered.l2_uri, head.size_bytes
		);
		assert!(
			left.iter().all(|name| !name.ends_with(".wnd")),
			"a payload file for the slab is still local: {left:?}"
		);
	}

	/// A reopened node refuses its tiered locations until it has checked them against their
	/// objects. Wait while the lookup of this row is refused as fenced on this slab, and only
	/// that, within the polls' shared bound; say how long it took. Any other error, or a payload
	/// file back on disk, fails at once. What the lookup then returns is judged by the caller.
	async fn wait_while_the_reopened_slab_is_fenced(&self, tiered: &TieredRow) {
		use wendelon::storage::manager::{StaleLocation, StaleLocationReason, stale_location};
		let node = self.node(tiered.node_id);
		let started = tokio::time::Instant::now();
		let mut pause = Duration::from_millis(10);
		loop {
			assert!(!tiered.local.exists(), "the slab payload is on local disk");
			let error = match node
				.slab
				.lookup(TenantId::ZERO, self.row_identity(tiered.row))
			{
				Ok(_) => break,
				Err(error) => error,
			};
			let fenced = StaleLocation {
				slab_id: tiered.slab_id,
				reason: StaleLocationReason::Fenced,
			};
			assert_eq!(
				stale_location(&error),
				Some(fenced),
				"locating the tiered row after reopening: {error:?}"
			);
			assert!(
				tokio::time::Instant::now() + pause < self.polls_end,
				"node {} still refuses slab {:?} as fenced {:?} after reopening: {error:?}",
				tiered.node_id,
				tiered.slab_id,
				started.elapsed()
			);
			tokio::time::sleep(pause).await;
			pause = (pause * 2).min(Duration::from_millis(200));
		}
		eprintln!(
			"workflow: node {} stopped refusing its tiered slab as fenced {:?} after reopening",
			tiered.node_id,
			started.elapsed()
		);
	}

	/// The nodes this fixture has stopped and not reopened.
	fn stopped_ids(&self) -> Vec<u32> {
		self.members
			.iter()
			.filter(|member| member.node.is_none())
			.map(|member| member.cfg.node_id)
			.collect()
	}

	fn member(&self, node_id: u32) -> &Member {
		self.members
			.iter()
			.find(|member| member.cfg.node_id == node_id)
			.unwrap()
	}

	fn row_identity(&self, id: i64) -> wendelon::schema::Identity {
		let request = self.request_for(id, "", 1, 0);
		let admission = validate_table_write_request_batch(
			&request.meta,
			&request.payload,
			&self.descriptor,
			WriterNodeId::ZERO,
			request.meta.client_hlc,
		)
		.unwrap();
		admission.key_identities[0]
	}

	/// A query served by the node that reclaimed this slab returns its row from the object. There
	/// is no other source for the row: the query path reads each row by its location in this node's
	/// own slab manager, with no fallback to a replica, and that location resolves to a slab whose local file is gone and whose backing is the object.
	/// That holds on both sides of the read. It may be served from a verified copy of the object in
	/// memory, so this does not claim a fresh read of the file. Query preparation can pull a pending
	/// migration range, and whether any ran here is not measured.
	async fn assert_row_served_from_the_object(&self, tiered: &TieredRow) {
		let node = self.node(tiered.node_id);
		let held = async || {
			let location = node
				.slab
				.lookup(TenantId::ZERO, self.row_identity(tiered.row))
				.unwrap()
				.expect("the row is indexed on this node");
			let pointers = node.slab.list_tiered_slabs().await;
			assert_eq!(location.slab_id, tiered.slab_id, "the row moved");
			assert!(
				pointers
					.iter()
					.any(|pointer| pointer.slab_id == tiered.slab_id
						&& pointer.l2_uri == tiered.l2_uri),
				"the slab is no longer backed by that object"
			);
			assert!(!tiered.local.exists(), "the slab is back on local disk");
		};
		held().await;
		let answered_by = self
			.assert_exact_through(tiered.node_id, "reading a tiered row")
			.await;
		assert_eq!(answered_by, tiered.node_id, "another node served it");
		held().await;
	}

	async fn stop(&mut self, node_id: u32) {
		let member = self
			.members
			.iter_mut()
			.find(|member| member.cfg.node_id == node_id)
			.unwrap();
		let node = member.node.take().unwrap();
		self.registry.unpublish(node.local_node);
		tokio::time::timeout(Duration::from_secs(30), node.shutdown())
			.await
			.expect("a graceful shutdown finished")
			.unwrap();
	}

	async fn reopen(&mut self, node_id: u32) {
		let member = self
			.members
			.iter_mut()
			.find(|member| member.cfg.node_id == node_id)
			.unwrap();
		let node = WendelonNode::bootstrap_with_provider(
			member.cfg.clone(),
			provider(node_id, self.registry.clone()),
		)
		.await
		.unwrap();
		member.node = Some(node);
	}
}

async fn workflow(stage: Arc<std::sync::Mutex<String>>, l2: ObjectStoreConfig, keys: DecisionKeys) {
	let enter = |now: &str| *stage.lock().unwrap() = now.to_owned();
	let objects = l2.build().unwrap();

	enter("bootstrap");
	let registry = support::shared_loopback_registry();
	let mut members = Vec::new();
	for node_id in NODES {
		let (dir, cfg) = member_config(node_id, &l2, keys);
		let node =
			WendelonNode::bootstrap_with_provider(cfg.clone(), provider(node_id, registry.clone()))
				.await
				.unwrap();
		members.push(Member {
			_dir: dir,
			cfg,
			node: Some(node),
		});
	}
	let mut cluster = Cluster {
		registry,
		members,
		descriptor: table_descriptor(),
		expected: Rows::new(),
		next_id: 0,
		client_uuid: client_uuid_v7(),
		next_write_sequence: 0,
		writes_sent_again: 0,
		newest_ack: bytemuck::Zeroable::zeroed(),
		polls_end: tokio::time::Instant::now() + POLL_BOUND,
		objects,
	};
	let first_leader = cluster.wait_for_leader("bootstrap").await;
	cluster.wait_for_voters().await;
	cluster.wait_for_ready().await;
	eprintln!("workflow: three voters, leader {first_leader}");

	enter("schema");
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: TenantId::ZERO,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: cluster.descriptor.namespace.clone(),
			name: cluster.descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(cluster.descriptor.schema_generation),
		intent_id: SchemaIntentId::new([0x3a; 16]),
		mutation: SchemaMutationKind::Create,
		payload: encode_table_schema_descriptor_v1(&cluster.descriptor).unwrap(),
	};
	// Through the control ring, as other integration tests create tables: this workflow is about
	// the rows, and schema submission through the service is not what it covers.
	cluster
		.node(first_leader)
		.raft
		.client_write(WendelonRequest::SchemaUpdate(
			encode_schema_update_envelope_v1(&envelope).unwrap(),
		))
		.await
		.unwrap();

	enter("first writes and reads");
	for _ in 0..8 {
		cluster.write_one("first").await;
	}
	cluster.assert_exact_everywhere("first writes").await;

	enter("tiering");
	let tiered = cluster.tier_and_reclaim().await;
	eprintln!(
		"workflow: node {} tiered and reclaimed the slab of row {}",
		tiered.node_id, tiered.row
	);
	cluster.assert_payload_is_only_in_the_object(&tiered).await;
	cluster.assert_row_served_from_the_object(&tiered).await;
	cluster.assert_exact_everywhere("after tiering").await;

	enter("tiering node restart");
	let incarnation = cluster.registered_incarnation(tiered.node_id).await;
	cluster.stop(tiered.node_id).await;
	cluster.reopen(tiered.node_id).await;
	assert_eq!(
		cluster.registered_incarnation(tiered.node_id).await,
		incarnation,
		"the same store came back"
	);
	cluster
		.wait_for_leader("after the tiering node restart")
		.await;
	cluster
		.wait_while_the_reopened_slab_is_fenced(&tiered)
		.await;
	cluster.assert_payload_is_only_in_the_object(&tiered).await;
	cluster.assert_row_served_from_the_object(&tiered).await;
	cluster
		.assert_exact_everywhere("after the tiering node restart")
		.await;

	enter("follower restart");
	let leader = cluster.wait_for_leader("before the follower restart").await;
	let follower = *NODES.iter().find(|id| **id != leader).unwrap();
	let incarnation = cluster.registered_incarnation(follower).await;
	cluster.stop(follower).await;
	cluster.write_one("follower-down").await;
	cluster.reopen(follower).await;
	assert_eq!(
		cluster.registered_incarnation(follower).await,
		incarnation,
		"the same store came back"
	);
	cluster
		.assert_exact_everywhere("after the follower restart")
		.await;
	cluster.write_one("follower-back").await;
	cluster
		.assert_exact_everywhere("after the follower caught up")
		.await;

	enter("leader failover");
	let old_leader = cluster.wait_for_leader("before the failover").await;
	let incarnation = cluster.registered_incarnation(old_leader).await;
	cluster.stop(old_leader).await;
	let new_leader = cluster.wait_for_leader("after the failover").await;
	assert_ne!(new_leader, old_leader);
	eprintln!("workflow: leader {old_leader} stopped, {new_leader} took over");
	cluster.write_one("failed-over").await;
	cluster
		.assert_exact_everywhere("with the old leader down")
		.await;

	enter("old leader back");
	cluster.reopen(old_leader).await;
	assert_eq!(
		cluster.registered_incarnation(old_leader).await,
		incarnation,
		"the same store came back"
	);
	cluster
		.assert_exact_everywhere("with the old leader back")
		.await;
	cluster.write_one("all-back").await;
	cluster.assert_exact_everywhere("after a last write").await;

	eprintln!(
		"workflow: {} writes went again as new after the service said not committed",
		cluster.writes_sent_again
	);
	enter("shutdown");
	for node_id in cluster.live_ids() {
		cluster.stop(node_id).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_nodes_keep_every_acknowledged_row_through_tiering_restart_and_failover() {
	let l2 = support::tempdir("three-node-l2");
	run_bounded(
		ObjectStoreConfig::LocalFs {
			root: l2.to_path_buf(),
		},
		DecisionKeys::Development,
	)
	.await;
}

/// ADR 0721: the same workflow with each node signing decisions with a secret of its own and
/// every node's verifying key named explicitly, instead of the development keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_nodes_keep_every_acknowledged_row_with_decision_keys_of_their_own() {
	let l2 = support::tempdir("three-node-l2-own-keys");
	run_bounded(
		ObjectStoreConfig::LocalFs {
			root: l2.to_path_buf(),
		},
		DecisionKeys::OfTheirOwn,
	)
	.await;
}

/// The same workflow with the three nodes' object store on a real RustFS server over HTTP. The
/// nodes still talk to each other over the in-process simulated network, in one process. External:
/// see `support/rustfs_env.rs`; run with
/// `cargo test -p wendelon --test three_node_workflow -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "external: needs a running RustFS (support/rustfs_env.rs); run with --ignored"]
async fn three_nodes_keep_every_acknowledged_row_with_their_objects_on_rustfs() {
	let remote = rustfs_env::remote_config("three-node-workflow");
	// Fail here, by name, when the server does not answer or refuses the credentials.
	let probe = ObjectStoreConfig::Remote(remote.clone()).build().unwrap();
	let uri = probe
		.put("probe", b"reachable")
		.await
		.expect("RustFS did not accept a put: endpoint, bucket or credentials");
	assert_eq!(probe.get(&uri).await.unwrap(), b"reachable");
	probe.delete(&uri).await.unwrap();
	run_bounded(ObjectStoreConfig::Remote(remote), DecisionKeys::Development).await;
}

#[test]
fn only_the_exact_dial_to_a_stopped_peer_is_retried() {
	use std::io::{Error, ErrorKind};
	let dial = |node_id: u32| {
		format!(
			"connect via QuicIoUring to {} failed: simulated transport target {node_id} is not published",
			address(node_id)
		)
	};
	let stopped = [3];
	assert!(dial_to_a_stopped_peer(
		&Error::new(ErrorKind::NotFound, dial(3)),
		&stopped
	));
	let stays_final = [
		// Something else that is not found: an object, a row, a replica.
		Error::new(ErrorKind::NotFound, "object not found"),
		Error::new(
			ErrorKind::NotFound,
			"no replica available for the requested key range",
		),
		// The same refusal for a node the fixture did not stop.
		Error::new(ErrorKind::NotFound, dial(2)),
		// A message that only mentions the stopped node, or wraps the refusal.
		Error::new(ErrorKind::NotFound, "node-3"),
		Error::new(ErrorKind::NotFound, format!("query failed: {}", dial(3))),
		Error::new(ErrorKind::NotFound, format!("{} (retry)", dial(3))),
		// The stopped node's address with another target, and the reverse.
		Error::new(
			ErrorKind::NotFound,
			format!(
				"connect via QuicIoUring to {} failed: simulated transport target 2 is not published",
				address(3)
			),
		),
		// The right message under another kind.
		Error::new(ErrorKind::Other, dial(3)),
		Error::new(ErrorKind::AddrNotAvailable, dial(3)),
	];
	for error in &stays_final {
		assert!(!dial_to_a_stopped_peer(error, &stopped), "{error:?}");
	}
	assert!(!dial_to_a_stopped_peer(
		&Error::new(ErrorKind::NotFound, dial(3)),
		&[]
	));
}

async fn run_bounded(l2: ObjectStoreConfig, keys: DecisionKeys) {
	let stage = Arc::new(std::sync::Mutex::new("start".to_owned()));
	let finished = tokio::time::timeout(WORKFLOW_BOUND, workflow(stage.clone(), l2, keys)).await;
	assert!(
		finished.is_ok(),
		"the workflow did not finish within {WORKFLOW_BOUND:?}, in: {}",
		stage.lock().unwrap(),
	);
}
