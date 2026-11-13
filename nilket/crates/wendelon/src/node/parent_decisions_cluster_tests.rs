//! ADR 0699 S3c: the DC-193 control. Three voters under ParentDecisions; a
//! table write whose first row's primary is not the Raft leader is coordinated
//! by that primary, reserved and finalized through the leader, and acknowledged
//! with complete intents on a quorum of each child's plan.

use super::*;
use crate::consensus::decisions::{Lookup, ParentKey, ParentNamespace, ParentStatus};
use crate::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use crate::network::lease_signing::{dev_lease_signing_secret, lease_verifying_key_bytes};
use crate::network::transfer_signing::{dev_transfer_signing_secret, transfer_verifying_key_bytes};
use crate::schema::{
	HybridTimestamp, MutationCommitKnowledge, PrimaryKeyDescriptor, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	TableWriteRequestMeta, WriteStatus, encode_table_schema_descriptor_v1,
	encode_table_write_row_count,
};
use crate::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

const NODES: [u32; 3] = [41, 42, 43];
const BOUND: std::time::Duration = std::time::Duration::from_secs(60);

fn address(id: u32) -> String {
	format!("loopback://node-{id}")
}

fn member(id: u32) -> (crate::test_support::TestDir, NodeBootstrapConfig) {
	let dir = crate::test_support::TestDir::new(&format!("parent-decisions-cluster-{id}"));
	let mut cfg = NodeBootstrapConfig::dev(id, dir.to_path_buf(), address(id));
	cfg.rf = NODES.len();
	cfg.cluster = if id == NODES[0] {
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
	cfg.decision_mode = crate::consensus::decision_mode::DecisionMode::ParentDecisions;
	(dir, cfg)
}

fn descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x51; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"events",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		crate::schema::LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::SingleRow,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn schema_update(descriptor: &TableSchemaDescriptor) -> Vec<u8> {
	encode_schema_update_envelope_v1(&SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: TenantId::ZERO,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new(*descriptor.table_object_id.as_bytes()),
		mutation: SchemaMutationKind::Create,
		payload: encode_table_schema_descriptor_v1(descriptor).unwrap(),
	})
	.unwrap()
}

struct Cluster {
	nodes: Vec<WendelonNode>,
	_dirs: Vec<crate::test_support::TestDir>,
	network: Arc<SimulatedTransportNetwork>,
	configs: std::collections::BTreeMap<u32, NodeBootstrapConfig>,
}

impl Cluster {
	async fn start() -> Self {
		Self::start_with(|_, _| None).await
	}

	/// `start`, each member's configuration adjusted by `configure`, which may
	/// return a directory the cluster keeps alive with it.
	async fn start_with(
		configure: impl Fn(u32, &mut NodeBootstrapConfig) -> Option<crate::test_support::TestDir>,
	) -> Self {
		let network = Arc::new(SimulatedTransportNetwork::default());
		let mut cluster = Self {
			nodes: Vec::new(),
			_dirs: Vec::new(),
			network,
			configs: std::collections::BTreeMap::new(),
		};
		for id in NODES {
			let (dir, mut cfg) = member(id);
			if let Some(extra) = configure(id, &mut cfg) {
				cluster._dirs.push(extra);
			}
			// ADR 0726 enrollment: a joiner is given the forming member's root
			// KEK artifact before its first start.
			// A test that gives a member a root of its own places it first.
			if id != NODES[0] && !cfg.data_dir.join(".wendelon/dev-root-kek.bin").exists() {
				crate::crypto::test_support::enroll_root_artifact(
					&cluster.configs[&NODES[0]].data_dir,
					&cfg.data_dir,
				)
				.unwrap();
			}
			cluster.configs.insert(id, cfg);
			cluster._dirs.push(dir);
			cluster.reopen(id).await;
		}
		cluster
	}

	/// Opens (or reopens) a node on its own store.
	async fn reopen(&mut self, id: u32) {
		let provider: Arc<dyn crate::network::transport::TransportProvider> =
			Arc::new(SimulatedTransportProvider::new(
				crate::schema::NodeId { id, _reserved: 0 },
				self.network.clone(),
				crate::network::pool::BufPool::new(64, 1024 * 1024),
				TransportTier::QuicIoUring,
			));
		let node = WendelonNode::bootstrap_with_provider(self.configs[&id].clone(), provider)
			.await
			.unwrap();
		self.nodes.push(node);
	}

	/// Closes a node completely; whether the close recorded it clean.
	async fn close(&mut self, id: u32) -> bool {
		let (result, clean) = self.close_with_result(id).await;
		result.unwrap();
		clean
	}

	/// Closes a node, bounded; the close's own result and whether it recorded
	/// the directory clean. Only a control that drives a close past its drain
	/// bound on purpose inspects the error; every other close goes through
	/// `close`, which requires success.
	async fn close_with_result(&mut self, id: u32) -> (std::io::Result<()>, bool) {
		let at = self
			.nodes
			.iter()
			.position(|node| node.local_node.id == id)
			.unwrap();
		let node = self.nodes.remove(at);
		self.network
			.unpublish(crate::schema::NodeId { id, _reserved: 0 });
		let result = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
			.await
			.expect("a complete close");
		(
			result,
			self.configs[&id].data_dir.join("CLEAN_SHUTDOWN").exists(),
		)
	}

	/// Stops a node with a complete close, which must record it clean.
	/// `crashed` then removes that record, so the next start is unclean, as
	/// after a kill.
	async fn stop(&mut self, id: u32, crashed: bool) {
		assert!(self.close(id).await, "the close recorded a clean shutdown");
		if crashed {
			std::fs::remove_file(self.configs[&id].data_dir.join("CLEAN_SHUTDOWN")).unwrap();
		}
	}

	/// Removes one staged row of a stopped node, offline, leaving its
	/// metadata.
	async fn damage_staged(&self, id: u32) {
		let index = crate::storage::index::PersistentLocalIndex::open(
			crate::storage::index::persistent_index_path(&self.configs[&id].data_dir),
		)
		.await
		.unwrap();
		index.drop_staged_row_for_test().await.unwrap();
	}

	/// Alters one active base entry of a stopped node, offline.
	async fn tamper_base(&self, id: u32) {
		let index = crate::storage::index::PersistentLocalIndex::open(
			crate::storage::index::persistent_index_path(&self.configs[&id].data_dir),
		)
		.await
		.unwrap();
		index.tamper_base_entry_for_test().await.unwrap();
	}

	/// Reads at `node` until it answers OK, accepting only the retryable
	/// refusal while it validates its certified base.
	async fn read_once_validated(&self, node: u32, descriptor: &TableSchemaDescriptor) -> Expected {
		let deadline = tokio::time::Instant::now() + BOUND;
		loop {
			let answer = read_at(self, node, descriptor).await;
			if answer.status == Some(crate::schema::ReadStatus::OK) {
				return answered(&answer);
			}
			assert_eq!(
				answer.status,
				Some(crate::schema::ReadStatus::BACKPRESSURED),
				"refused only until validated: {answer:?}"
			);
			assert!(tokio::time::Instant::now() < deadline, "never validated");
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}

	fn node(&self, id: u32) -> &WendelonNode {
		self.nodes
			.iter()
			.find(|node| node.local_node.id == id)
			.unwrap()
	}

	async fn leader(&self) -> u32 {
		let deadline = tokio::time::Instant::now() + BOUND;
		loop {
			if let Some(leader) = self.nodes[0].raft.current_leader().await {
				return leader;
			}
			assert!(tokio::time::Instant::now() < deadline, "no leader");
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
	}

	/// Polls the leader's applied state until `ready` holds.
	async fn until(
		&self,
		what: &str,
		ready: impl Fn(&crate::consensus::raft_storage::SmState) -> bool,
	) {
		let deadline = tokio::time::Instant::now() + BOUND;
		loop {
			let leader = self.leader().await;
			if self.node(leader).state_machine.read_state(&ready).await {
				return;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"never reached: {what}"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
}

/// A fresh UUIDv7: the current unix milliseconds, then random bits.
fn uuid_v7() -> [u8; 16] {
	let ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut bytes = *uuid::Uuid::new_v4().as_bytes();
	bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
	bytes[6] = 0x70 | (bytes[6] & 0x0F);
	bytes[8] = 0x80 | (bytes[8] & 0x3F);
	bytes
}

fn write_meta(
	descriptor: &TableSchemaDescriptor,
	rows: usize,
	client_uuid: [u8; 16],
) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid,
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(rows).unwrap();
	meta
}

fn rows(descriptor: &TableSchemaDescriptor, ids: &[i64]) -> RecordBatch {
	let values: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
	RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![
			Arc::new(Int64Array::from(ids.to_vec())),
			Arc::new(StringArray::from(values)),
		],
	)
	.unwrap()
}

/// A started cluster with every signer bound, every node leasing and the
/// table created; returns it, the leader and the descriptor.
async fn ready() -> (Cluster, u32, TableSchemaDescriptor) {
	ready_with(Cluster::start()).await
}

/// `ready` over a cluster started by `start`.
async fn ready_with(
	start: impl std::future::Future<Output = Cluster>,
) -> (Cluster, u32, TableSchemaDescriptor) {
	let cluster = start.await;
	let leader = cluster.leader().await;
	let descriptor = descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	// Every node registered, bound its key and holds its primary ranges.
	until_settled(&cluster, &descriptor).await;
	(cluster, leader, descriptor)
}

/// Waits until every node is registered with a bound key at its current
/// incarnation, every replica segment of the ring lies inside a lineage held
/// at its holder's current incarnation, and the schema is applied. A restart
/// moves lineages to the new incarnation; a write before that may meet a
/// closed lineage and be refused, which is a correct answer, not a fixture.
async fn until_settled(cluster: &Cluster, descriptor: &TableSchemaDescriptor) {
	let table = descriptor.table_object_id;
	cluster
		.until(
			"three signers, a fully leased ring and the schema",
			move |state| {
				let Some(decisions) = state.decisions.as_ref() else {
					return false;
				};
				let current = |id: u32| {
					state
						.incarnations
						.get(&id)
						.and_then(|registry| registry.current)
				};
				let bound = NODES.iter().all(|id| {
					current(*id).is_some_and(|incarnation| {
						decisions
							.signer_key(crate::consensus::decisions::Signer {
								node: *id,
								incarnation,
							})
							.is_some()
					})
				});
				let leased: std::collections::BTreeSet<u32> = decisions
					.lineages()
					.values()
					.map(|lineage| lineage.holder.id)
					.collect();
				// Every replica segment of the ring lies inside a lineage
				// held at its holder's current incarnation.
				let covered = state
					.epoch_history
					.last_key_value()
					.is_some_and(|(_, config)| {
						crate::network::ring::RingState::build(
							&crate::network::ring::membership_from_epoch_config(config),
							NODES.len(),
						)
						.replica_segments()
						.iter()
						.all(|segment| {
							decisions
								.lineages()
								.range(..=segment.start)
								.next_back()
								.is_some_and(|(_, lineage)| {
									segment.end <= lineage.range_end
										&& current(lineage.holder.id) == Some(lineage.incarnation)
								})
						})
					});
				bound
					&& covered && leased.len() == NODES.len()
					&& state.tenants.contains_key(&TenantId::ZERO)
					&& state
						.table_schemas
						.latest_by_id(TenantId::ZERO, table)
						.is_some()
			},
		)
		.await;
	// Every lineage's holder also holds a lease over it at the current epoch,
	// as its own holder check sees it; until then a holder write answers
	// LeaseMissing.
	let deadline = tokio::time::Instant::now() + BOUND;
	for id in NODES {
		let node = cluster.node(id);
		loop {
			let (epoch, held) = node
				.state_machine
				.read_state(move |state| {
					let epoch = state
						.epoch_history
						.last_key_value()
						.map(|(_, config)| config.epoch);
					let held: Vec<(u64, u64)> = state
						.decisions
						.as_ref()
						.map(|decisions| {
							decisions
								.lineages()
								.iter()
								.filter(|(_, lineage)| lineage.holder.id == id)
								.map(|(start, lineage)| (*start, lineage.range_end))
								.collect()
						})
						.unwrap_or_default();
					(epoch, held)
				})
				.await;
			let leases = node.data_server.held_leases_for_test().await;
			let covered = held.iter().all(|(start, end)| {
				leases.iter().any(|(low, high, lease_epoch)| {
					Some(*lease_epoch) == epoch && *low <= *start && *end <= *high
				})
			});
			if covered {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {id} never leased its lineages"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_nonleader_primary_coordinates_a_three_node_table_write() {
	let (cluster, leader, descriptor) = ready().await;

	// DC-193: find a row whose first-row primary is not the leader. The leader
	// redirects it there; that primary coordinates it to COMMITTED.
	let mut committed = None;
	for id in 1..200i64 {
		let batch = rows(&descriptor, &[id]);
		let meta = write_meta(&descriptor, 1, uuid_v7());
		let (response, _) = cluster
			.node(leader)
			.data_server
			.table_write_for_test(meta, batch.clone())
			.await
			.unwrap();
		if response.write.status != WriteStatus::NOT_LEADER {
			continue;
		}
		let primary = response.write.leader_hint.id;
		assert_ne!(primary, leader);
		let (response, _) = cluster
			.node(primary)
			.data_server
			.table_write_for_test(meta, batch)
			.await
			.unwrap();
		assert_eq!(response.write.status, WriteStatus::OK, "{response:?}");
		assert_eq!(
			response.write.mutation_commit_knowledge(),
			MutationCommitKnowledge::COMMITTED
		);
		committed = Some((meta, primary, id));
		break;
	}
	let (meta, primary, id) = committed.expect("some row's primary is not the leader");

	// Recorded Accepted at the leader; complete intents on a quorum of the plan.
	let key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: 1,
	};
	let record = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let decisions = state.decisions.as_ref().unwrap();
			decisions
				.bodies()
				.values()
				.find(|body| body.claim.key == key)
				.cloned()
		})
		.await
		.expect("the parent is recorded");
	assert!(
		matches!(record.status, ParentStatus::Accepted { .. }),
		"{record:?}"
	);
	let child = &record.claim.children[0];
	assert_eq!(child.holder.node, primary);
	let mut complete = 0;
	for member in &child.plan {
		let intent = cluster
			.node(member.node)
			.slab
			.persistent_index()
			.unwrap()
			.parent_intent(record.rsv, 0)
			.await
			.unwrap();
		if intent.is_some_and(|intent| {
			intent.state == crate::storage::index::parent_intents::ParentIntentState::Complete
		}) {
			complete += 1;
		}
	}
	assert!(
		complete > child.plan.len() / 2,
		"{complete} of {}",
		child.plan.len()
	);

	// A retry with the same identity learns the recorded result, by lookup.
	let (again, _) = cluster
		.node(primary)
		.data_server
		.table_write_for_test(meta, rows(&descriptor, &[id]))
		.await
		.unwrap();
	assert_eq!(again.write.status, WriteStatus::OK, "{again:?}");
	assert_eq!(
		again.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	let digest = record.claim.request_digest;
	let lookup = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			crate::consensus::decisions::lookup_parent(
				state.decisions.as_ref().unwrap(),
				&key,
				&digest,
			)
		})
		.await;
	assert!(matches!(lookup, Lookup::Recorded { record: again } if again.rsv == record.rsv));

	// DC-194: one statement across primaries is one parent with several
	// children, all accepted together, each on a quorum of its own plan.
	let ids: Vec<i64> = (1_000..1_040).collect();
	let meta = write_meta(&descriptor, ids.len(), uuid_v7());
	let mut entry = leader;
	let response = loop {
		let response = write_retrying(
			&cluster.node(entry).data_server,
			meta,
			rows(&descriptor, &ids),
		)
		.await;
		if response.write.status == WriteStatus::NOT_LEADER
			&& response.write.leader_hint.id != entry
		{
			entry = response.write.leader_hint.id;
			continue;
		}
		break response;
	};
	assert_eq!(response.write.status, WriteStatus::OK, "{response:?}");
	assert_eq!(
		response.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	let wide_key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: 1,
	};
	let wide = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.bodies()
				.values()
				.find(|body| body.claim.key == wide_key)
				.cloned()
		})
		.await
		.expect("the wide parent is recorded");
	assert!(matches!(wide.status, ParentStatus::Accepted { .. }));
	assert!(
		wide.claim.children.len() > 1,
		"40 spread rows cross several children"
	);
	let primaries: std::collections::BTreeSet<u32> = wide
		.claim
		.children
		.iter()
		.map(|child| child.holder.node)
		.collect();
	assert!(primaries.len() > 1, "children on several primaries");
	for (ordinal, child) in wide.claim.children.iter().enumerate() {
		let mut complete = 0;
		for member in &child.plan {
			let intent = cluster
				.node(member.node)
				.slab
				.persistent_index()
				.unwrap()
				.parent_intent(wide.rsv, ordinal as u16)
				.await
				.unwrap();
			if intent.is_some_and(|intent| {
				intent.state == crate::storage::index::parent_intents::ParentIntentState::Complete
			}) {
				complete += 1;
			}
		}
		assert!(
			complete > child.plan.len() / 2,
			"child {ordinal}: {complete}"
		);
	}

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

fn key_of(descriptor: &TableSchemaDescriptor, meta: &TableWriteRequestMeta) -> ParentKey {
	ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: meta.parent_write_id.sequence_no,
	}
}

/// The leader's applied record for `key`, if any.
async fn record_of(
	cluster: &Cluster,
	key: ParentKey,
) -> Option<crate::consensus::decisions::ParentRecord> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()?
				.bodies()
				.values()
				.find(|body| body.claim.key == key)
				.cloned()
		})
		.await
}

/// A row whose first-row primary is not the leader, and that primary, read
/// from the leader's applied lineages. Nothing is written.
async fn row_off_the_leader(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	from: i64,
) -> (i64, u32) {
	for id in from..from + 200 {
		let primary = cluster
			.node(leader)
			.data_server
			.primary_for_test(TenantId::ZERO, descriptor, &rows(descriptor, &[id]))
			.await
			.expect("every position is leased");
		if primary != leader {
			return (id, primary);
		}
	}
	panic!("no row's primary is off the leader");
}

fn spawn_write(
	cluster: &Cluster,
	node: u32,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> tokio::task::JoinHandle<crate::schema::TableWriteResponseMeta> {
	let server = cluster.node(node).data_server.clone();
	tokio::spawn(async move { write_retrying(&server, meta, batch).await })
}

/// A write retried with the same identity while it answers WRITE_STALLED, as
/// a client must: a stall is indeterminate, never a refusal. Under load a
/// holder's lease can lapse briefly while its lineage stays open, and its own
/// child write is then refused until the lease is renewed.
async fn write_retrying(
	server: &WendelonDataServer,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> crate::schema::TableWriteResponseMeta {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let (response, _) = server
			.table_write_for_test(meta, batch.clone())
			.await
			.unwrap();
		if response.write.status != WriteStatus::WRITE_STALLED
			|| tokio::time::Instant::now() >= deadline
		{
			return response;
		}
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
}

fn assert_committed(response: &crate::schema::TableWriteResponseMeta, what: &str) {
	assert_eq!(
		response.write.status,
		WriteStatus::OK,
		"{what}: {response:?}"
	);
	assert_eq!(
		response.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED,
		"{what}: {response:?}"
	);
}

/// CODEX-7: two attempts of one request race to Reserve. The loser acts only on
/// the winner's record: its claim, HLC and reservation, never its own build.
/// A retry for a Reserved parent at any other node is sent to the recorded
/// coordinator and leaves the record unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_attempts_and_retries_follow_the_recorded_claim() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;

	// The winner is already Accepted when the loser's Reserve lands.
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let server = &cluster.node(primary).data_server;
	let (reached, release) = server.pause_coordinator_for_test(PauseStage::AfterAbsent);
	let loser = spawn_write(&cluster, primary, meta, rows(&descriptor, &[id]));
	reached.notified().await;
	let (winner, _) = server
		.table_write_for_test(meta, rows(&descriptor, &[id]))
		.await
		.unwrap();
	assert_committed(&winner, "accepted winner");
	release.notify_one();
	let loser = loser.await.unwrap();
	assert_committed(&loser, "loser behind an accepted winner");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert!(matches!(record.status, ParentStatus::Accepted { .. }));
	assert_eq!(winner.write.commit_hlc, record.claim.commit_hlc);
	assert_eq!(loser.write.commit_hlc, record.claim.commit_hlc);
	assert_eq!(loser.write.log_index, winner.write.log_index);

	// The winner is still Reserved: the loser resumes the winner's claim.
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 300).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let server = &cluster.node(primary).data_server;
	let (loser_reached, loser_release) = server.pause_coordinator_for_test(PauseStage::AfterAbsent);
	let (winner_reached, winner_release) =
		server.pause_coordinator_for_test(PauseStage::AfterReserve);
	let loser = spawn_write(&cluster, primary, meta, rows(&descriptor, &[id]));
	loser_reached.notified().await;
	let winner = spawn_write(&cluster, primary, meta, rows(&descriptor, &[id]));
	winner_reached.notified().await;
	let reserved = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	loser_release.notify_one();
	let loser = loser.await.unwrap();
	assert_committed(&loser, "loser behind a reserved winner");
	winner_release.notify_one();
	let winner = winner.await.unwrap();
	assert_committed(&winner, "reserved winner");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(record.rsv, reserved.rsv);
	assert_eq!(
		record.claim, reserved.claim,
		"the reserved claim is the one accepted"
	);
	assert_eq!(loser.write.commit_hlc, reserved.claim.commit_hlc);
	assert_eq!(winner.write.commit_hlc, reserved.claim.commit_hlc);

	// A retry for a Reserved parent at another node goes to its coordinator.
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 600).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let first = spawn_write(&cluster, primary, meta, rows(&descriptor, &[id]));
	reached.notified().await;
	let reserved = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	for other in NODES.into_iter().filter(|node| *node != primary) {
		let (retry, _) = cluster
			.node(other)
			.data_server
			.table_write_for_test(meta, rows(&descriptor, &[id]))
			.await
			.unwrap();
		assert_eq!(retry.write.status, WriteStatus::NOT_LEADER, "{retry:?}");
		assert_eq!(retry.write.leader_hint.id, primary);
		assert_eq!(
			record_of(&cluster, key_of(&descriptor, &meta))
				.await
				.unwrap(),
			reserved,
			"a retry elsewhere leaves the record unchanged"
		);
	}
	release.notify_one();
	let first = first.await.unwrap();
	assert_committed(&first, "the recorded coordinator");
	assert_eq!(first.write.commit_hlc, reserved.claim.commit_hlc);
	for other in NODES.into_iter().filter(|node| *node != primary) {
		let (retry, _) = cluster
			.node(other)
			.data_server
			.table_write_for_test(meta, rows(&descriptor, &[id]))
			.await
			.unwrap();
		assert_committed(&retry, "a retry elsewhere after acceptance");
		assert_eq!(retry.write.commit_hlc, reserved.claim.commit_hlc);
	}

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

// ADR 0699 S4: the reader.

fn named_rows(descriptor: &TableSchemaDescriptor, rows: &[(i64, &str)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
			)),
		],
	)
	.unwrap()
}

fn query_meta(
	cluster: &Cluster,
	node: u32,
	descriptor: &TableSchemaDescriptor,
) -> crate::schema::TableQueryRequestMeta {
	static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
	let mut meta: crate::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = cluster.node(node).ring.current_epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.request_id = crate::schema::TableQueryRequestId::from_u128(u128::from(
		NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
	));
	meta
}

type Expected = std::collections::BTreeMap<i64, String>;

/// The rows of an answer; a row returned twice is wrong data.
fn answered(answer: &crate::network::data_server::reader::TestQueryAnswer) -> Expected {
	let mut rows = Expected::new();
	for batch in &answer.batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.expect("an id column")
			.clone();
		let values = arrow::compute::cast(
			batch.column_by_name("v").expect("a v column"),
			&DataType::Utf8,
		)
		.unwrap();
		let values = values.as_any().downcast_ref::<StringArray>().unwrap();
		for row in 0..batch.num_rows() {
			assert!(
				rows.insert(ids.value(row), values.value(row).to_owned())
					.is_none(),
				"row {} returned twice",
				ids.value(row)
			);
		}
	}
	rows
}

async fn read_at(
	cluster: &Cluster,
	node: u32,
	descriptor: &TableSchemaDescriptor,
) -> crate::network::data_server::reader::TestQueryAnswer {
	cluster
		.node(node)
		.data_server
		.table_query_for_test(query_meta(cluster, node, descriptor))
		.await
		.unwrap()
}

/// A query at `node` answers OK with exactly `expected`.
async fn expect_at(
	cluster: &Cluster,
	node: u32,
	descriptor: &TableSchemaDescriptor,
	expected: &Expected,
	what: &str,
) {
	let answer = read_at(cluster, node, descriptor).await;
	assert_eq!(
		answer.status,
		Some(crate::schema::ReadStatus::OK),
		"{what}: at {node}: {answer:?}"
	);
	assert_eq!(&answered(&answer), expected, "{what}: at {node}");
}

async fn expect_everywhere(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	expected: &Expected,
	what: &str,
) {
	for node in NODES {
		expect_at(cluster, node, descriptor, expected, what).await;
	}
}

/// A write entering at `entry`, following redirects to its coordinator.
async fn write_through(
	cluster: &Cluster,
	entry: u32,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> crate::schema::TableWriteResponseMeta {
	let mut entry = entry;
	loop {
		let response = write_retrying(&cluster.node(entry).data_server, meta, batch.clone()).await;
		if response.write.status == WriteStatus::NOT_LEADER
			&& response.write.leader_hint.id != entry
		{
			entry = response.write.leader_hint.id;
			continue;
		}
		return response;
	}
}

/// Refuses a Reserved parent with its coordinator's own signed abort.
async fn abort(cluster: &Cluster, record: &crate::consensus::decisions::ParentRecord) {
	use crate::consensus::decisions::{AbortBody, dev_decision_signing_secret, evidence};
	use crate::network::decision_proposals::ForwardedDecision;
	let coordinator = record.claim.children[usize::from(record.claim.coordinator)].clone();
	let key = ed25519_dalek::SigningKey::from_bytes(&dev_decision_signing_secret(
		coordinator.holder.node,
	));
	let body = AbortBody {
		key: record.claim.key,
		reservation: record.rsv,
		claim_digest: record.claim_digest,
		coordinator: coordinator.holder,
		lineage: coordinator.lineage,
	};
	let proposals = cluster
		.node(coordinator.holder.node)
		.data_server
		.decision_proposals_for_test();
	proposals
		.propose(ForwardedDecision::Abort {
			abort: evidence::sign(&key, body, coordinator.holder),
		})
		.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_node_reads_exactly_what_is_accepted_at_its_cut() {
	use crate::network::data_server::child_writes::PauseStage;
	use crate::schema::ReadStatus;
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	expect_everywhere(&cluster, &descriptor, &expected, "an empty table").await;

	// Read right after the acknowledgement, at every node.
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let written = write_through(
		&cluster,
		leader,
		write_meta(&descriptor, 1, uuid_v7()),
		named_rows(&descriptor, &[(id, "a")]),
	)
	.await;
	assert_committed(&written, "first");
	expected.insert(id, "a".into());
	expect_everywhere(&cluster, &descriptor, &expected, "after the first ack").await;

	// A Reserved parent with its children complete is invisible: the older
	// Accepted version shows through it.
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let pending = spawn_write(
		&cluster,
		primary,
		meta,
		named_rows(&descriptor, &[(id, "b")]),
	);
	reached.notified().await;
	let reserved = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"with a newer version Reserved",
	)
	.await;
	release.notify_one();
	assert_committed(&pending.await.unwrap(), "second");
	expected.insert(id, "b".into());
	expect_everywhere(&cluster, &descriptor, &expected, "newest Accepted wins").await;

	// A Refused parent's stored children are invisible.
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let pending = spawn_write(
		&cluster,
		primary,
		meta,
		named_rows(&descriptor, &[(id, "c")]),
	);
	reached.notified().await;
	let reserved = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	abort(&cluster, &reserved).await;
	let refused = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert!(
		matches!(refused.status, ParentStatus::Refused { .. }),
		"{refused:?}"
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"with a newer version Refused",
	)
	.await;
	release.notify_one();
	let answer = pending.await.unwrap();
	assert_eq!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"{answer:?}"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the refusal").await;

	// The cut is pinned: a decision applied while the statement runs is not in it.
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::ReaderAfterCut);
	let server = cluster.node(primary).data_server.clone();
	let pinned = query_meta(&cluster, primary, &descriptor);
	let reading = tokio::spawn(async move { server.table_query_for_test(pinned).await.unwrap() });
	reached.notified().await;
	let (later, _) = row_off_the_leader(&cluster, leader, &descriptor, 500).await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(later, "d")]),
		)
		.await,
		"a later write",
	);
	release.notify_one();
	let answer = reading.await.unwrap();
	assert_eq!(answer.status, Some(ReadStatus::OK), "{answer:?}");
	assert_eq!(answered(&answer), expected, "the statement's own cut");
	expected.insert(later, "d".into());
	expect_everywhere(&cluster, &descriptor, &expected, "a new statement").await;

	// The caller's deadline spans the cut: a statement delayed past it ends there.
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::ReaderAfterCut);
	let server = cluster.node(primary).data_server.clone();
	let mut late = query_meta(&cluster, primary, &descriptor);
	late.deadline_remaining_millis = 300;
	let reading = tokio::spawn(async move { server.table_query_for_test(late).await.unwrap() });
	reached.notified().await;
	tokio::time::sleep(std::time::Duration::from_millis(600)).await;
	release.notify_one();
	let answer = reading.await.unwrap();
	assert_eq!(
		answer.status,
		Some(ReadStatus::QUERY_DEADLINE_EXCEEDED),
		"{answer:?}"
	);
	assert!(answer.batches.is_empty());

	// Authority withdrawn during the statement: no success terminal. The two
	// checks before the scan pass, the first one after a batch fails.
	let peer = cluster.node(primary).data_server.decision_peer_for_test();
	peer.read_checks_left
		.store(2, std::sync::atomic::Ordering::Release);
	let answer = read_at(&cluster, primary, &descriptor).await;
	assert_eq!(answer.status, Some(ReadStatus::BACKPRESSURED), "{answer:?}");
	peer.read_checks_left
		.store(0, std::sync::atomic::Ordering::Release);
	let answer = read_at(&cluster, primary, &descriptor).await;
	assert_eq!(answer.status, Some(ReadStatus::BACKPRESSURED), "{answer:?}");
	assert!(answer.batches.is_empty(), "refused before any row");
	peer.read_checks_left
		.store(usize::MAX, std::sync::atomic::Ordering::Release);
	expect_at(&cluster, primary, &descriptor, &expected, "authority back").await;

	// Fail-closed boundaries.
	let server = &cluster.node(primary).data_server;
	let mut meta = query_meta(&cluster, primary, &descriptor);
	meta.consistency = crate::schema::Consistency::STALE;
	let answer = server.table_query_for_test(meta).await.unwrap();
	assert_eq!(
		answer.status,
		Some(ReadStatus::QUERY_EXECUTION_FAILED),
		"{answer:?}"
	);
	let mut meta = query_meta(&cluster, primary, &descriptor);
	meta.as_of_hlc = written.write.commit_hlc;
	let answer = server.table_query_for_test(meta).await.unwrap();
	assert_eq!(
		answer.status,
		Some(ReadStatus::QUERY_EXECUTION_FAILED),
		"{answer:?}"
	);

	// The inventory's budget refuses before anything is read.
	let tenant = TenantId::ZERO;
	let table = descriptor.table_object_id;
	let local = cluster.node(primary).data_server.decision_signer_for_test();
	let over = cluster
		.node(primary)
		.state_machine
		.read_state(move |state| {
			crate::network::data_server::reader::inventory_within_for_test(
				state, tenant, table, local, 1,
			)
		})
		.await;
	assert!(!over, "three Accepted versions exceed a budget of one");

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_missing_copy_is_repaired_from_its_plan_before_any_answer() {
	use crate::schema::ReadStatus;
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let lacking = *NODES.iter().find(|node| **node != primary).unwrap();
	let peer = |node: u32| cluster.node(node).data_server.decision_peer_for_test();
	let intent = |node: u32, rsv: u64| {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		async move { index.parent_intent(rsv, 0).await.unwrap() }
	};

	// Accepted on the holder and one replica; the third store missed it.
	peer(lacking).refuse_replicas.store(true, Release);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_through(
			&cluster,
			primary,
			meta,
			named_rows(&descriptor, &[(id, "a")]),
		)
		.await,
		"with one replica refusing",
	);
	peer(lacking).refuse_replicas.store(false, Release);
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert!(
		intent(lacking, record.rsv).await.is_none(),
		"the copy is missing"
	);

	// Cancelled while it waits to repair: the statement ends, with no rows.
	let (reached, release) = cluster
		.node(lacking)
		.data_server
		.pause_coordinator_for_test(
			crate::network::data_server::child_writes::PauseStage::ReaderBeforeRepair,
		);
	let server = cluster.node(lacking).data_server.clone();
	let cancelled = query_meta(&cluster, lacking, &descriptor);
	let request = cancelled.request_id;
	let reading =
		tokio::spawn(async move { server.table_query_for_test(cancelled).await.unwrap() });
	reached.notified().await;
	cluster
		.node(lacking)
		.data_server
		.cancel_table_query_for_test(TenantId::ZERO, request);
	let answer = reading.await.unwrap();
	release.notify_one();
	assert_eq!(
		answer.status,
		Some(ReadStatus::QUERY_CANCELLED),
		"{answer:?}"
	);
	assert!(answer.batches.is_empty());

	let mut expected = Expected::new();
	expected.insert(id, "a".into());
	expect_at(
		&cluster,
		lacking,
		&descriptor,
		&expected,
		"repaired by the read",
	)
	.await;
	let repaired = intent(lacking, record.rsv).await.expect("repaired");
	assert_eq!(
		repaired.state,
		crate::storage::index::parent_intents::ParentIntentState::Complete
	);

	// A complete intent whose stored version is gone: repaired, never skipped.
	let version = repaired.manifest[0];
	cluster.node(lacking).slab.forget_version_for_test(
		TenantId::ZERO,
		crate::schema::Identity::new(version.key_identity),
		version.hlc,
	);
	expect_at(&cluster, lacking, &descriptor, &expected, "a lost version").await;

	// With every source refusing, no answer at all, never a partial one.
	let (second, _) = row_off_the_leader(&cluster, leader, &descriptor, 300).await;
	peer(lacking).refuse_replicas.store(true, Release);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let written = write_through(
		&cluster,
		leader,
		meta,
		named_rows(&descriptor, &[(second, "b")]),
	)
	.await;
	assert_committed(&written, "a second write");
	peer(lacking).refuse_replicas.store(false, Release);
	let second_record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	let sources: Vec<u32> = NODES.into_iter().filter(|node| *node != lacking).collect();
	for source in &sources {
		peer(*source).refuse_fetches.store(true, Release);
	}
	let answer = read_at(&cluster, lacking, &descriptor).await;
	assert_eq!(answer.status, Some(ReadStatus::BACKPRESSURED), "{answer:?}");
	assert!(answer.batches.is_empty(), "no partial answer");

	// A source serving other bytes is refused by the requester's own checks.
	peer(sources[0]).refuse_fetches.store(false, Release);
	peer(sources[0]).tamper_fetches.store(true, Release);
	let answer = read_at(&cluster, lacking, &descriptor).await;
	assert_eq!(answer.status, Some(ReadStatus::BACKPRESSURED), "{answer:?}");
	assert!(
		intent(lacking, second_record.rsv).await.is_none(),
		"nothing persisted from a tampered source"
	);

	// One honest source suffices.
	peer(sources[1]).refuse_fetches.store(false, Release);
	expected.insert(second, "b".into());
	expect_at(
		&cluster,
		lacking,
		&descriptor,
		&expected,
		"an honest source",
	)
	.await;
	peer(sources[0]).tamper_fetches.store(false, Release);
	expect_everywhere(&cluster, &descriptor, &expected, "everywhere").await;

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A table whose locality projects the non-key column `v`: a key's versions
/// can route to different positions.
fn column_located() -> TableSchemaDescriptor {
	use crate::schema::{ClusteringColumnSpec, ColumnLocalityDescriptor, ProjectionFunction};
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x52; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"located",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, false),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		crate::schema::LocalityPolicy::ColumnProjection(ColumnLocalityDescriptor {
			columns: vec![ClusteringColumnSpec {
				name: "v".to_owned(),
				width_bits: 32,
				string_prefix_bytes: Some(4),
			}],
			projection: ProjectionFunction::ZOrder,
			spread_factor: 1,
			prefix_bits: 16,
		}),
		RecordLayoutPolicy::SingleRow,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// CODEX-2 R1: the Accepted version routes by its own locality. A newer version
/// of the same key with another locality column value, physically complete
/// and Reserved, then Refused, must not move the older one out of the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_selected_version_routes_by_its_own_locality() {
	use crate::network::data_server::child_writes::PauseStage;
	use crate::schema::ReadStatus;
	let (cluster, leader, _) = ready().await;
	let located = column_located();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&located),
		))
		.await
		.unwrap();
	let table = located.table_object_id;
	cluster
		.until("the located table", move |state| {
			state
				.table_schemas
				.latest_by_id(TenantId::ZERO, table)
				.is_some()
		})
		.await;
	let older = named_rows(&located, &[(7, "aaaa")]);
	let newer = named_rows(&located, &[(7, "zzzz")]);
	let leader_server = cluster.node(leader).data_server.clone();
	let (older_primary, newer_primary) = (
		leader_server
			.primary_for_test(TenantId::ZERO, &located, &older)
			.await
			.unwrap(),
		leader_server
			.primary_for_test(TenantId::ZERO, &located, &newer)
			.await
			.unwrap(),
	);
	eprintln!("located: older at {older_primary}, newer at {newer_primary}");
	assert_committed(
		&write_through(&cluster, leader, write_meta(&located, 1, uuid_v7()), older).await,
		"the older version",
	);
	let mut expected = Expected::new();
	expected.insert(7, "aaaa".into());
	expect_everywhere(&cluster, &located, &expected, "accepted").await;

	// The newer version is written everywhere and Reserved.
	let meta = write_meta(&located, 1, uuid_v7());
	let (reached, release) = cluster
		.node(newer_primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let pending = spawn_write(&cluster, newer_primary, meta, newer);
	reached.notified().await;
	let reserved = record_of(&cluster, key_of(&located, &meta)).await.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	// The hazard is live: on every node the key's current routing locality is
	// the Reserved version's, not the Accepted version's.
	let (key, older_locality) = leader_server.row_identity_and_locality_for_test(
		TenantId::ZERO,
		&located,
		&named_rows(&located, &[(7, "aaaa")]),
	);
	let (_, newer_locality) = leader_server.row_identity_and_locality_for_test(
		TenantId::ZERO,
		&located,
		&named_rows(&located, &[(7, "zzzz")]),
	);
	assert_ne!(older_locality, newer_locality);
	for node in NODES {
		assert_eq!(
			cluster
				.node(node)
				.slab
				.routing_locality_for_test(TenantId::ZERO, key),
			Some(newer_locality),
			"at {node}"
		);
	}
	expect_everywhere(&cluster, &located, &expected, "newer Reserved").await;
	// And through the whole-record descriptor check.
	for node in NODES {
		let mut meta = query_meta(&cluster, node, &located);
		meta.row_identity_projection = crate::client::RowIdentityProjection::Append.as_u8();
		let answer = cluster
			.node(node)
			.data_server
			.table_query_for_test(meta)
			.await
			.unwrap();
		assert_eq!(answer.status, Some(ReadStatus::OK), "{answer:?}");
		assert_eq!(answered(&answer), expected, "with row identities at {node}");
	}

	// Refused: still the older version.
	abort(&cluster, &reserved).await;
	expect_everywhere(&cluster, &located, &expected, "newer Refused").await;
	release.notify_one();
	let answer = pending.await.unwrap();
	assert_eq!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"{answer:?}"
	);

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

// ADR 0699 S4b: single-key KV.

fn kv_write_meta(key: &[u8], value: &[u8]) -> (crate::schema::WriteRequestMeta, RecordBatch) {
	use arrow::array::LargeBinaryArray;
	let mut meta: crate::schema::WriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.write_id = crate::schema::ClientWriteId {
		client_uuid: uuid_v7(),
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	meta.tenant_id = TenantId::ZERO;
	meta.key_identity = crate::schema::Identity::blake3_tenant(TenantId::ZERO, key);
	meta.key_len = key.len() as u32;
	meta.value_len = value.len() as u32;
	let payload = RecordBatch::try_new(
		Arc::new(crate::schema::write_payload_schema()),
		vec![
			Arc::new(LargeBinaryArray::from(vec![key])),
			Arc::new(LargeBinaryArray::from(vec![value])),
		],
	)
	.unwrap();
	(meta, payload)
}

/// A KV write entering at `entry`, following redirects and retrying stalls
/// with the same identity.
async fn kv_write_through(
	cluster: &Cluster,
	entry: u32,
	meta: crate::schema::WriteRequestMeta,
	payload: RecordBatch,
) -> crate::schema::WriteResponseMeta {
	let mut entry = entry;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let response = cluster
			.node(entry)
			.data_server
			.kv_write_for_test(meta, payload.clone())
			.await;
		if response.status == WriteStatus::NOT_LEADER && response.leader_hint.id != entry {
			entry = response.leader_hint.id;
			continue;
		}
		if response.status == WriteStatus::WRITE_STALLED && tokio::time::Instant::now() < deadline {
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			continue;
		}
		return response;
	}
}

/// A KV read at `node`: its status, value and value HLC.
async fn kv_read(
	cluster: &Cluster,
	node: u32,
	key: &[u8],
) -> (
	crate::schema::ReadStatus,
	Option<Vec<u8>>,
	crate::schema::HybridTimestamp,
) {
	use arrow::array::LargeBinaryArray;
	let mut meta: crate::schema::ReadRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = cluster.node(node).ring.current_epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.key_identity = crate::schema::Identity::blake3_tenant(TenantId::ZERO, key);
	meta.key_len = key.len() as u32;
	let batch = RecordBatch::try_new(
		Arc::new(crate::schema::read_key_schema()),
		vec![Arc::new(LargeBinaryArray::from(vec![key]))],
	)
	.unwrap();
	let (response, value) = cluster
		.node(node)
		.data_server
		.kv_read_for_test(meta, batch)
		.await
		.unwrap();
	let value = value.map(|batch| {
		batch
			.column_by_name("value")
			.and_then(|column| column.as_any().downcast_ref::<LargeBinaryArray>())
			.unwrap()
			.value(0)
			.to_vec()
	});
	(response.status, value, response.value_hlc)
}

async fn kv_expect_everywhere(cluster: &Cluster, key: &[u8], expected: Option<&[u8]>, what: &str) {
	use crate::schema::ReadStatus;
	for node in NODES {
		let (status, value, _) = kv_read(cluster, node, key).await;
		match expected {
			Some(expected) => {
				assert_eq!(status, ReadStatus::OK, "{what}: at {node}");
				assert_eq!(value.as_deref(), Some(expected), "{what}: at {node}");
			}
			None => assert_eq!(status, ReadStatus::KEY_NOT_FOUND, "{what}: at {node}"),
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_kv_key_is_written_at_its_primary_and_read_exactly_everywhere() {
	use crate::network::data_server::child_writes::PauseStage;
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, _) = ready().await;

	// Written through a redirect to the key's primary; read exactly everywhere.
	let (meta, payload) = kv_write_meta(b"alpha", b"one");
	let written = kv_write_through(&cluster, leader, meta, payload.clone()).await;
	assert_eq!(written.status, WriteStatus::OK, "{written:?}");
	assert_eq!(
		written.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	kv_expect_everywhere(&cluster, b"alpha", Some(b"one"), "after the ack").await;
	let again = kv_write_through(&cluster, leader, meta, payload).await;
	assert_eq!(again.status, WriteStatus::OK);
	assert_eq!(
		again.commit_hlc, written.commit_hlc,
		"a retry answers the record"
	);
	kv_expect_everywhere(&cluster, b"absent", None, "an absent key").await;

	// A wire identity that is not the key's is refused.
	let (mut forged, payload) = kv_write_meta(b"beta", b"x");
	forged.key_identity = crate::schema::Identity::blake3_tenant(TenantId::ZERO, b"gamma");
	let refused = kv_write_through(&cluster, leader, forged, payload).await;
	assert_eq!(refused.status, WriteStatus::WRITE_IDENTITY_INVALID);

	// A newer version, children written and Reserved, then Refused: invisible.
	let (_, primary) = cluster
		.node(leader)
		.data_server
		.kv_primary_for_test(TenantId::ZERO, b"alpha")
		.await;
	let (meta, payload) = kv_write_meta(b"alpha", b"two");
	let (reached, release) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let server = cluster.node(primary).data_server.clone();
	let pending = tokio::spawn(async move { server.kv_write_for_test(meta, payload).await });
	reached.notified().await;
	kv_expect_everywhere(&cluster, b"alpha", Some(b"one"), "newer Reserved").await;
	let key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Kv,
		client_uuid: meta.write_id.client_uuid,
		sequence_no: 1,
	};
	let reserved = record_of(&cluster, key).await.unwrap();
	abort(&cluster, &reserved).await;
	kv_expect_everywhere(&cluster, b"alpha", Some(b"one"), "newer Refused").await;
	release.notify_one();
	assert_eq!(
		pending.await.unwrap().mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);

	// Another key at the same position never answers for this one.
	let (alpha_position, _) = cluster
		.node(leader)
		.data_server
		.kv_primary_for_test(TenantId::ZERO, b"alpha")
		.await;
	for node in NODES {
		*cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.kv_position_override
			.lock()
			.unwrap() = Some((b"delta".to_vec(), alpha_position));
	}
	kv_expect_everywhere(
		&cluster,
		b"delta",
		None,
		"a different key at the same position",
	)
	.await;
	for node in NODES {
		*cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.kv_position_override
			.lock()
			.unwrap() = None;
	}

	// A store that missed the replicate is repaired by its read.
	let (_, primary) = cluster
		.node(leader)
		.data_server
		.kv_primary_for_test(TenantId::ZERO, b"omega")
		.await;
	let lacking = *NODES.iter().find(|node| **node != primary).unwrap();
	cluster
		.node(lacking)
		.data_server
		.decision_peer_for_test()
		.refuse_replicas
		.store(true, Release);
	let (meta, payload) = kv_write_meta(b"omega", b"last");
	let written = kv_write_through(&cluster, leader, meta, payload).await;
	assert_eq!(written.status, WriteStatus::OK, "{written:?}");
	cluster
		.node(lacking)
		.data_server
		.decision_peer_for_test()
		.refuse_replicas
		.store(false, Release);
	kv_expect_everywhere(&cluster, b"omega", Some(b"last"), "repaired").await;

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

// ADR 0699 S4b: single-row DELETE and RecordNoop.

fn delete_meta(descriptor: &TableSchemaDescriptor) -> crate::schema::TableDeleteRowsRequestMeta {
	let mut meta: crate::schema::TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid: uuid_v7(),
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	meta.tenant_id = TenantId::ZERO;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = 1;
	meta
}

/// A delete entering at `entry`, following redirects and retrying stalls with
/// the same identity.
async fn delete_through(
	cluster: &Cluster,
	entry: u32,
	meta: crate::schema::TableDeleteRowsRequestMeta,
	rows: Vec<crate::schema::Identity>,
) -> crate::schema::TableDeleteRowsResponseMeta {
	let mut entry = entry;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let response = cluster
			.node(entry)
			.data_server
			.table_delete_for_test(meta, rows.clone())
			.await;
		if response.write.status == WriteStatus::NOT_LEADER
			&& response.write.leader_hint.id != entry
		{
			entry = response.write.leader_hint.id;
			continue;
		}
		if response.write.status == WriteStatus::WRITE_STALLED
			&& tokio::time::Instant::now() < deadline
		{
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			continue;
		}
		return response;
	}
}

fn counts(response: &crate::schema::TableDeleteRowsResponseMeta) -> [u32; 3] {
	[
		response.deleted_count,
		response.already_deleted_count,
		response.not_found_count,
	]
}

/// ADR 0700: a claim whose commit HLC is far ahead of the leader's stamp is
/// refused at Reserve: nothing is reserved or written, and the client sees an
/// unresolved stall. The client HLC was merged before admission, so the
/// coordinator's later writes are refused too until its clock catches up: the
/// registered availability behavior, recorded here, not fixed. The test-only
/// rewind is fixture cleanup, not product recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_claim_from_the_future_is_refused_without_effect() {
	let (cluster, leader, descriptor) = ready().await;
	let batch = || named_rows(&descriptor, &[(7, "f")]);
	let primary = cluster
		.node(leader)
		.data_server
		.primary_for_test(TenantId::ZERO, &descriptor, &batch())
		.await
		.unwrap();
	let server = &cluster.node(primary).data_server;
	let mut ahead = write_meta(&descriptor, 1, uuid_v7());
	ahead.client_hlc = crate::schema::HybridTimestamp {
		physical_ms: std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64
			+ 600_000,
		logical: 0,
		node_id: 0,
	};
	let unresolved = |response: &crate::schema::TableWriteResponseMeta, what: &str| {
		assert_eq!(
			response.write.status,
			WriteStatus::WRITE_STALLED,
			"{what}: {response:?}"
		);
		assert_eq!(
			response.write.mutation_commit_knowledge(),
			MutationCommitKnowledge::INDETERMINATE,
			"{what}: {response:?}"
		);
	};
	let (refused, _) = server.table_write_for_test(ahead, batch()).await.unwrap();
	unresolved(&refused, "a claim from the future");
	assert!(
		record_of(&cluster, key_of(&descriptor, &ahead))
			.await
			.is_none()
	);
	// The poisoned coordinator HLC refuses a later ordinary write as well.
	let ordinary = write_meta(&descriptor, 1, uuid_v7());
	let (poisoned, _) = server
		.table_write_for_test(ordinary, batch())
		.await
		.unwrap();
	unresolved(&poisoned, "an ordinary write after the poisoning");
	assert!(
		record_of(&cluster, key_of(&descriptor, &ordinary))
			.await
			.is_none()
	);
	expect_everywhere(&cluster, &descriptor, &Expected::new(), "nothing written").await;
	// Fixture cleanup only.
	server.rewind_hlc_for_test();
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			batch(),
		)
		.await,
		"after the fixture's rewind",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_single_row_delete_selects_at_its_cut_and_records_a_noop_when_nothing_matches() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let leader_server = cluster.node(leader).data_server.clone();
	let row_of = |id: i64| {
		leader_server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let mut expected = Expected::new();
	for id in [7, 8] {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}

	// Matched: one tombstone, every node omits the row; a retry answers the record.
	let meta = delete_meta(&descriptor);
	let deleted = delete_through(&cluster, leader, meta, vec![row_of(7)]).await;
	assert_eq!(deleted.write.status, WriteStatus::OK, "{deleted:?}");
	assert_eq!(
		deleted.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	assert_eq!(counts(&deleted), [1, 0, 0]);
	expected.remove(&7);
	expect_everywhere(&cluster, &descriptor, &expected, "after the delete").await;
	let again = delete_through(&cluster, leader, meta, vec![row_of(7)]).await;
	assert_eq!(counts(&again), [1, 0, 0]);
	assert_eq!(again.write.commit_hlc, deleted.write.commit_hlc);

	// Already deleted, and never written: noops with their frozen counts.
	let already = delete_through(&cluster, leader, delete_meta(&descriptor), vec![row_of(7)]).await;
	assert_eq!(already.write.status, WriteStatus::OK, "{already:?}");
	assert_eq!(counts(&already), [0, 1, 0]);
	let absent_meta = delete_meta(&descriptor);
	let absent = delete_through(&cluster, leader, absent_meta, vec![row_of(99)]).await;
	assert_eq!(counts(&absent), [0, 0, 1]);
	let noop_key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: absent_meta.parent_write_id.client_uuid,
		sequence_no: 1,
	};
	let recorded = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.noops()
				.values()
				.any(|noop| noop.body.key == noop_key)
		})
		.await;
	assert!(recorded, "the noop is recorded");
	// A retry after the row is inserted still answers the frozen selection.
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(99, "z")]),
		)
		.await,
		"an insert after the noop",
	);
	expected.insert(99, "z".into());
	let retried = delete_through(&cluster, leader, absent_meta, vec![row_of(99)]).await;
	assert_eq!(counts(&retried), [0, 0, 1]);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"the insert survives the noop",
	)
	.await;

	// CODEX-2 req 1: a coordinator whose clock is behind the selected winner
	// still publishes its tombstone above it. ADR 0700: a winner far ahead of
	// every clock is now refused at Reserve, so the property is proved
	// deterministically by `deletes::tests::a_tombstone_is_minted_strictly_above_a_winner_ahead_of_the_clock`
	// on the helper this path uses; end to end, a rewound coordinator's
	// tombstone is strictly above the winner's full HLC.
	let primary = leader_server
		.primary_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(8, "a")]),
		)
		.await
		.unwrap();
	let winner = write_through(
		&cluster,
		leader,
		write_meta(&descriptor, 1, uuid_v7()),
		named_rows(&descriptor, &[(8, "b")]),
	)
	.await;
	assert_committed(&winner, "a winner before a rewound coordinator");
	cluster.node(primary).data_server.rewind_hlc_for_test();
	let behind = delete_through(&cluster, leader, delete_meta(&descriptor), vec![row_of(8)]).await;
	assert_eq!(behind.write.status, WriteStatus::OK, "{behind:?}");
	assert_eq!(counts(&behind), [1, 0, 0]);
	assert!(
		behind.write.commit_hlc > winner.write.commit_hlc,
		"above the winner: {:?} vs {:?}",
		behind.write.commit_hlc,
		winner.write.commit_hlc
	);
	expected.remove(&8);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"deleted above a later winner",
	)
	.await;

	// A concurrent insert accepted between the cut and the delete's decision is
	// visible exactly when its HLC is above the tombstone's.
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(10, "a")]),
		)
		.await,
		"row 10",
	);
	let coordinator = leader_server
		.primary_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(10, "a")]),
		)
		.await
		.unwrap();
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let server = cluster.node(coordinator).data_server.clone();
	let delete10 = delete_meta(&descriptor);
	let row10 = row_of(10);
	let pending =
		tokio::spawn(async move { server.table_delete_for_test(delete10, vec![row10]).await });
	reached.notified().await;
	let insert = write_through(
		&cluster,
		leader,
		write_meta(&descriptor, 1, uuid_v7()),
		named_rows(&descriptor, &[(10, "b")]),
	)
	.await;
	assert_committed(&insert, "the concurrent insert");
	release.notify_one();
	let finished = pending.await.unwrap();
	assert_eq!(counts(&finished), [1, 0, 0], "the frozen selection");
	if insert.write.commit_hlc > finished.write.commit_hlc {
		expected.insert(10, "b".into());
	} else {
		expected.remove(&10);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "HLC order decides").await;

	// Unsupported: more than one id.
	let two = delete_through(
		&cluster,
		leader,
		delete_meta(&descriptor),
		vec![row_of(7), row_of(8)],
	)
	.await;
	assert_eq!(two.write.status, WriteStatus::INTERNAL_ERROR, "{two:?}");

	// An unproved selected copy with no source is indeterminate, never a noop.
	let primary = leader_server
		.primary_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(99, "z")]),
		)
		.await
		.unwrap();
	let lost = row_of(99);
	let lost_hlc = {
		let index = cluster.node(primary).slab.persistent_index().unwrap();
		let mut found = None;
		for (rsv, ordinal) in index
			.parent_intents_in(&[
				crate::storage::index::parent_intents::ParentIntentState::Complete,
			])
			.await
			.unwrap()
		{
			if let Some(intent) = index.parent_intent(rsv, ordinal).await.unwrap() {
				for version in intent.manifest {
					if version.key_identity == *lost.as_bytes() {
						found = Some(
							found.map_or(version.hlc, |hlc: crate::schema::HybridTimestamp| {
								hlc.max(version.hlc)
							}),
						);
					}
				}
			}
		}
		found.unwrap()
	};
	cluster
		.node(primary)
		.slab
		.forget_version_for_test(TenantId::ZERO, lost, lost_hlc);
	for node in NODES.into_iter().filter(|node| *node != primary) {
		cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.refuse_fetches
			.store(true, std::sync::atomic::Ordering::Release);
	}
	let unproved_meta = delete_meta(&descriptor);
	let unproved = cluster
		.node(primary)
		.data_server
		.table_delete_for_test(unproved_meta, vec![lost])
		.await;
	assert_eq!(
		unproved.write.status,
		WriteStatus::WRITE_STALLED,
		"{unproved:?}"
	);
	assert_eq!(
		unproved.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	let unproved_key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: unproved_meta.parent_write_id.client_uuid,
		sequence_no: 1,
	};
	assert!(record_of(&cluster, unproved_key).await.is_none());
	let noops = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.noops()
				.values()
				.any(|noop| noop.body.key == unproved_key)
		})
		.await;
	assert!(!noops, "no noop from unproved data");

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// CODEX-3: absence answers need the final read authority; a delete's fresh
/// evaluation spends the caller's deadline; a KV read refuses a causal lower
/// bound it cannot prove.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn absence_needs_authority_a_delete_keeps_its_deadline_and_kv_refuses_min_hlc() {
	use crate::network::data_server::child_writes::PauseStage;
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, descriptor) = ready().await;
	let leader_server = cluster.node(leader).data_server.clone();
	let row_of = |id: i64| {
		leader_server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};

	// R1, KV: the first check passes, the final one fails: no KEY_NOT_FOUND.
	let peer = cluster.node(leader).data_server.decision_peer_for_test();
	peer.read_checks_left.store(1, Release);
	let (status, _, _) = kv_read(&cluster, leader, b"never-written").await;
	assert_eq!(status, crate::schema::ReadStatus::BACKPRESSURED);
	peer.read_checks_left.store(usize::MAX, Release);
	let (status, _, _) = kv_read(&cluster, leader, b"never-written").await;
	assert_eq!(status, crate::schema::ReadStatus::KEY_NOT_FOUND);

	// R1, DELETE: an absent row whose final authority fails records no noop.
	let coordinator = leader_server
		.primary_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(98, "x")]),
		)
		.await
		.unwrap();
	let server = cluster.node(coordinator).data_server.clone();
	server
		.decision_peer_for_test()
		.read_checks_left
		.store(1, Release);
	let meta = delete_meta(&descriptor);
	let answer = server.table_delete_for_test(meta, vec![row_of(98)]).await;
	server
		.decision_peer_for_test()
		.read_checks_left
		.store(usize::MAX, Release);
	assert_eq!(
		answer.write.status,
		WriteStatus::WRITE_STALLED,
		"{answer:?}"
	);
	let key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: 1,
	};
	let noop_recorded = |key: ParentKey| {
		let state_machine = cluster.node(leader).state_machine.clone();
		async move {
			state_machine
				.read_state(move |state| {
					state
						.decisions
						.as_ref()
						.unwrap()
						.noops()
						.values()
						.any(|noop| noop.body.key == key)
				})
				.await
		}
	};
	assert!(!noop_recorded(key).await, "no noop without final authority");

	// R2: a fresh evaluation that outlives the caller's deadline proposes
	// nothing and answers the deadline, indeterminate.
	let (reached, release) = server.pause_coordinator_for_test(PauseStage::DeleteEvaluation);
	let mut late = delete_meta(&descriptor);
	late.deadline_remaining_millis = 300;
	let late_server = server.clone();
	let row98 = row_of(98);
	let pending =
		tokio::spawn(async move { late_server.table_delete_for_test(late, vec![row98]).await });
	reached.notified().await;
	tokio::time::sleep(std::time::Duration::from_millis(600)).await;
	release.notify_one();
	let answer = pending.await.unwrap();
	assert_eq!(
		answer.write.status,
		WriteStatus::WRITE_DEADLINE_EXCEEDED,
		"{answer:?}"
	);
	assert_eq!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	let late_key = ParentKey {
		client_uuid: late.parent_write_id.client_uuid,
		..key
	};
	assert!(!noop_recorded(late_key).await);
	assert!(record_of(&cluster, late_key).await.is_none());
	// An already-recorded outcome is answered whatever the deadline.
	let mut hurried = recorded_meta_for_retry(&cluster, leader, &descriptor, row_of(98)).await;
	hurried.deadline_remaining_millis = 1;
	tokio::time::sleep(std::time::Duration::from_millis(5)).await;
	let retried = delete_through(&cluster, leader, hurried, vec![row_of(98)]).await;
	assert_eq!(counts(&retried), [0, 0, 1], "{retried:?}");

	// R3: a nonzero causal lower bound is refused, not ignored.
	let mut meta: crate::schema::ReadRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = cluster.node(leader).ring.current_epoch();
	meta.tenant_id = TenantId::ZERO;
	meta.consistency = crate::schema::Consistency::LINEARIZABLE;
	meta.key_identity = crate::schema::Identity::blake3_tenant(TenantId::ZERO, b"k");
	meta.key_len = 1;
	meta.min_hlc = crate::schema::HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 0,
	};
	let batch = RecordBatch::try_new(
		Arc::new(crate::schema::read_key_schema()),
		vec![Arc::new(arrow::array::LargeBinaryArray::from(vec![
			&b"k"[..],
		]))],
	)
	.unwrap();
	assert!(
		cluster
			.node(leader)
			.data_server
			.kv_read_for_test(meta, batch)
			.await
			.is_err()
	);

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A delete of `row` recorded through the leader, returned for a retry.
async fn recorded_meta_for_retry(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	row: crate::schema::Identity,
) -> crate::schema::TableDeleteRowsRequestMeta {
	let meta = delete_meta(descriptor);
	let answer = delete_through(cluster, leader, meta, vec![row]).await;
	assert_eq!(answer.write.status, WriteStatus::OK, "{answer:?}");
	meta
}

// ADR 0699 S5a: certified generations.

/// A UUIDv7 issued at `ms`: a controlled-clock identity.
fn uuid_v7_at(ms: u64) -> [u8; 16] {
	let mut bytes = *uuid::Uuid::new_v4().as_bytes();
	bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
	bytes[6] = 0x70 | (bytes[6] & 0x0F);
	bytes[8] = 0x80 | (bytes[8] & 0x3F);
	bytes
}

/// Controlled clock: one floor stamped `DECISION_EXPIRY_MS` past `floor_ms`,
/// which retires every key issued at or before it.
async fn jump_floor(cluster: &Cluster, floor_ms: u64) {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::Decision(
			crate::consensus::decisions::DecisionCommand::AdvanceFloor {
				floor_ms,
				stamp_ms: floor_ms + crate::consensus::decisions::DECISION_EXPIRY_MS,
			},
		))
		.await
		.unwrap();
}

/// Waits until every node activated generation `gid`.
async fn until_active(cluster: &Cluster, gid: u64) {
	cluster
		.until("the generation committed", move |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().current_gid() >= gid)
		})
		.await;
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		while index.base_meta().await.unwrap().active_gid < gid {
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node} never activated generation {gid}"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
}

/// CONTROLLED CLOCK (a finite multi-generation run, not a capacity test): a
/// generation commits on every member's agreeing report, its cut's decisions
/// and intents are erased, and every node reads exactly from the certified
/// base plus the retained parents. A second generation keeps state bounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_generation_erases_its_cut_and_reads_stay_exact_from_the_certified_base() {
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=3 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	expect_everywhere(&cluster, &descriptor, &expected, "before any generation").await;

	// Every key above is retired: the leader proposes, members stage and
	// report, the leader commits, every node activates.
	let floor = now + 60_000;
	jump_floor(&cluster, floor).await;
	until_active(&cluster, 1).await;
	let leader = cluster.leader().await;
	let (bodies, bytes) = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let decisions = state.decisions.as_ref().unwrap();
			(
				decisions.retained_bodies(),
				decisions.retained_claim_bytes(),
			)
		})
		.await;
	assert_eq!(
		(bodies, bytes),
		(0, 0),
		"the cut's records and bytes are erased"
	);
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert_eq!(
			index.base_entries(false).await.unwrap().len(),
			3,
			"at {node}"
		);
		assert!(
			index
				.parent_intents_in(&[
					crate::storage::index::parent_intents::ParentIntentState::Complete
				])
				.await
				.unwrap()
				.is_empty(),
			"the erased parents' intents are retired at {node}"
		);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "from the certified base").await;

	// New writes after the floor: a newer version beats its base entry.
	let later = |n: u64| uuid_v7_at(floor + n);
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, later(1)),
			named_rows(&descriptor, &[(1, "b")]),
		)
		.await,
		"an update over the base",
	);
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, later(2)),
			named_rows(&descriptor, &[(4, "a")]),
		)
		.await,
		"a new row",
	);
	expected.insert(1, "b".into());
	expected.insert(4, "a".into());
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"base plus retained parents",
	)
	.await;

	// A second generation: still bounded, still exact. A statement whose cut
	// predates it refuses (cut expired) rather than read erased decisions.
	let (reached, release) = cluster.node(leader).data_server.pause_coordinator_for_test(
		crate::network::data_server::child_writes::PauseStage::ReaderAfterCut,
	);
	let server = cluster.node(leader).data_server.clone();
	let early = query_meta(&cluster, leader, &descriptor);
	let reading = tokio::spawn(async move { server.table_query_for_test(early).await.unwrap() });
	reached.notified().await;
	jump_floor(&cluster, floor + 120_000).await;
	until_active(&cluster, 2).await;
	release.notify_one();
	let answer = reading.await.unwrap();
	assert_eq!(
		answer.status,
		Some(crate::schema::ReadStatus::BACKPRESSURED),
		"a cut below K_min is expired: {answer:?}"
	);
	assert!(answer.batches.is_empty());
	let bodies = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().retained_bodies())
		.await;
	assert_eq!(bodies, 0);
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert_eq!(
			index.base_entries(false).await.unwrap().len(),
			4,
			"one entry per key at {node}"
		);
	}
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"after the second generation",
	)
	.await;

	// Restarts keep generation 2 authoritative. A clean restart checks the
	// certificate against its active meta; an unclean one also proves every
	// base entry is stored. Reads refuse (retryable) until then, then answer
	// exactly.
	let leader = cluster.leader().await;
	let followers: Vec<u32> = NODES.into_iter().filter(|node| *node != leader).collect();
	for (follower, crashed) in [(followers[0], false), (followers[1], true)] {
		cluster.stop(follower, crashed).await;
		cluster.reopen(follower).await;
		until_settled(&cluster, &descriptor).await;
		let restarted = cluster.node(follower);
		assert_eq!(restarted.recovered_after_unclean_shutdown(), crashed);
		assert_eq!(
			cluster.read_once_validated(follower, &descriptor).await,
			expected,
			"after a restart at {follower} (unclean: {crashed})"
		);
		let peer = cluster.node(follower).data_server.decision_peer_for_test();
		assert!(
			peer.base_validated
				.load(std::sync::atomic::Ordering::Acquire)
		);
		let meta = cluster
			.node(follower)
			.slab
			.persistent_index()
			.unwrap()
			.base_meta()
			.await
			.unwrap();
		assert_eq!(
			meta.active_gid, 2,
			"generation 2 stays active at {follower}"
		);
	}

	// The crash row between Commit applied and local activation: a follower
	// staged and reported generation 3, then crashed before activating it.
	// Until it activates, its reads refuse (its active base is not Gcur); the
	// reopened store activates the exact staged candidate, then validates.
	let leader = cluster.leader().await;
	let held = *NODES.iter().find(|node| **node != leader).unwrap();
	let hold = cluster
		.node(held)
		.data_server
		.decision_peer_for_test()
		.hold_activation
		.clone();
	hold.store(true, std::sync::atomic::Ordering::Release);
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, later(120_001)),
			named_rows(&descriptor, &[(5, "a")]),
		)
		.await,
		"a row for generation 3",
	);
	expected.insert(5, "a".into());
	// A delete of a base row and a delete that matches nothing (a noop): the
	// winning tombstone stays in the base as the key's one entry, and the noop
	// is erased with the cut.
	let server = cluster.node(leader).data_server.clone();
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let mut delete = delete_meta(&descriptor);
	delete.parent_write_id.client_uuid = later(120_002);
	let deleted = delete_through(&cluster, leader, delete, vec![row_of(2)]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "{deleted:?}");
	let mut absent = delete_meta(&descriptor);
	absent.parent_write_id.client_uuid = later(120_003);
	let noop = delete_through(&cluster, leader, absent, vec![row_of(99)]).await;
	assert_eq!(counts(&noop), [0, 0, 1], "{noop:?}");
	expected.remove(&2);
	jump_floor(&cluster, floor + 240_000).await;
	cluster
		.until("generation 3 committed", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().current_gid() >= 3)
		})
		.await;
	let index = cluster.node(held).slab.persistent_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(held)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current_gid()
		})
		.await < 3
	{
		assert!(tokio::time::Instant::now() < deadline, "never applied");
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	let meta = index.base_meta().await.unwrap();
	assert_eq!(meta.active_gid, 2, "activation is held");
	assert_eq!(
		meta.staged.map(|staged| staged.gid),
		Some(3),
		"staged and reported"
	);
	let answer = read_at(&cluster, held, &descriptor).await;
	assert_eq!(
		answer.status,
		Some(crate::schema::ReadStatus::BACKPRESSURED),
		"no read between Commit and activation: {answer:?}"
	);
	assert!(answer.batches.is_empty());
	drop(index);
	cluster.stop(held, true).await;
	cluster.reopen(held).await;
	until_settled(&cluster, &descriptor).await;
	assert_eq!(
		cluster.read_once_validated(held, &descriptor).await,
		expected,
		"after activating on reopen"
	);
	let meta = cluster
		.node(held)
		.slab
		.persistent_index()
		.unwrap()
		.base_meta()
		.await
		.unwrap();
	assert_eq!((meta.active_gid, meta.staged.is_none()), (3, true));
	until_active(&cluster, 3).await;
	expect_everywhere(&cluster, &descriptor, &expected, "after generation 3").await;
	let leader = cluster.leader().await;
	let retained = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let decisions = state.decisions.as_ref().unwrap();
			(
				decisions.retained_bodies(),
				decisions.retained_claim_bytes(),
			)
		})
		.await;
	assert_eq!(
		retained,
		(0, 0),
		"bodies, the noop and their bytes are erased"
	);
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let entries = index.base_entries(false).await.unwrap();
		assert_eq!(entries.len(), 5, "one entry per key at {node}");
		assert_eq!(
			entries.iter().filter(|entry| entry.tombstone).count(),
			1,
			"the winning tombstone is retained at {node}"
		);
		// Bounded history: no staged candidate, and every intent of the
		// erased parents is retired, in every state.
		assert!(
			index.base_meta().await.unwrap().staged.is_none(),
			"at {node}"
		);
		use crate::storage::index::parent_intents::ParentIntentState;
		assert!(
			index
				.parent_intents_in(&[
					ParentIntentState::Intent,
					ParentIntentState::Complete,
					ParentIntentState::Refused,
				])
				.await
				.unwrap()
				.is_empty(),
			"every covered intent retired at {node}"
		);
	}
	// Generation metadata stays bounded: the current certificate only.
	let (current, pending, next) = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let generations = state.decisions.as_ref().unwrap().generations();
			(
				generations.current_gid(),
				generations.pending.is_some(),
				generations.next_gid,
			)
		})
		.await;
	assert!(
		current >= 3 && !pending && next == current + 1,
		"{current} {pending} {next}"
	);

	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Every complete intent of a store.
async fn complete_intents(
	index: &crate::storage::index::PersistentLocalIndex,
) -> Vec<crate::storage::index::parent_intents::ParentIntent> {
	use crate::storage::index::parent_intents::ParentIntentState;
	let mut found = Vec::new();
	for (rsv, ordinal) in index
		.parent_intents_in(&[ParentIntentState::Complete])
		.await
		.unwrap()
	{
		found.push(index.parent_intent(rsv, ordinal).await.unwrap().unwrap());
	}
	found
}

/// The leader's applied generations.
async fn generations_at_leader(cluster: &Cluster) -> crate::consensus::decisions::Generations {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().generations().clone())
		.await
}

/// CODEX-3 R1, CONTROLLED CLOCK: a member restarts on its own store while
/// the target waits for its report. A store keeps its incarnation across
/// restarts, so the pending target stays current: the restarted member builds
/// and reports through the maintenance path, the target commits, every base
/// activates and the records retire. (A replaced store, a new incarnation,
/// makes the target stale; the driver then replaces it, which the reducer
/// control `a_member_restart_replaces_its_pending_target_rather_than_stalling`
/// pins.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_member_restart_before_commit_still_commits_its_target() {
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	cluster.stop(follower, false).await;
	jump_floor(&cluster, now + 60_000).await;
	// The target is recorded with the stopped member's old incarnation and
	// the two live members report; it cannot commit.
	cluster
		.until("two of three reported", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().pending.as_ref())
				.is_some_and(|pending| {
					pending.reports.len() == 2 && !pending.reports.contains_key(&follower)
				})
		})
		.await;
	let stalled = generations_at_leader(&cluster).await;
	let pending = stalled.pending.as_ref().unwrap().target.clone();
	assert_eq!(stalled.current_gid(), 0);

	cluster.reopen(follower).await;
	let incarnation = cluster
		.node(follower)
		.slab
		.owner_ref()
		.unwrap()
		.incarnation();
	assert!(
		pending
			.members
			.iter()
			.any(|member| member.node == follower && member.incarnation == incarnation),
		"a same-store restart keeps its incarnation"
	);
	until_settled(&cluster, &descriptor).await;
	until_active(&cluster, 1).await;
	let generations = generations_at_leader(&cluster).await;
	let current = generations.current.as_ref().unwrap();
	assert_eq!(
		(current.gid, current.predecessor_gid),
		(pending.gid, 0),
		"the pending target commits after the restart"
	);
	assert!(generations.pending.is_none());
	let leader = cluster.leader().await;
	let bodies = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().retained_bodies())
		.await;
	assert_eq!(bodies, 0, "the records retire");
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert_eq!(index.base_meta().await.unwrap().active_gid, current.gid);
		assert_eq!(
			index.base_entries(false).await.unwrap().len(),
			2,
			"at {node}"
		);
	}
	for node in NODES {
		assert_eq!(
			cluster.read_once_validated(node, &descriptor).await,
			expected,
			"at {node}"
		);
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// CODEX-3 R2, CONTROLLED CLOCK: a member reports only a base whose every
/// survivor it proved stored. A follower whose complete intents lost their
/// stored copies (a row and a tombstone) repairs them from a peer before it
/// reports; a version whose owner entry disagrees with its child gets no
/// report at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_report_proves_every_survivor_stored_repairing_or_refusing() {
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let server = cluster.node(leader).data_server.clone();
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let deleted = delete_through(&cluster, leader, delete_meta(&descriptor), vec![row_of(2)]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "{deleted:?}");
	expected.remove(&2);
	expect_everywhere(&cluster, &descriptor, &expected, "before any generation").await;

	// The follower loses every stored copy its complete intents own.
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	let index = cluster.node(follower).slab.persistent_index().unwrap();
	let mut lost = 0;
	for intent in complete_intents(&index).await {
		for version in &intent.manifest {
			cluster.node(follower).slab.forget_version_for_test(
				TenantId::ZERO,
				crate::schema::Identity::new(version.key_identity),
				version.hlc,
			);
			lost += 1;
		}
	}
	assert_eq!(lost, 3, "two rows and a tombstone");

	// Its build repairs the survivors (row 1, and row 2's tombstone) before
	// it reports, so the generation commits.
	let floor = now + 60_000;
	jump_floor(&cluster, floor).await;
	until_active(&cluster, 1).await;
	let base = index.base_entries(false).await.unwrap();
	assert_eq!(base.len(), 2);
	for entry in &base {
		let locations = cluster
			.node(follower)
			.slab
			.version_locations(
				entry.tenant,
				crate::schema::Identity::new(entry.key_identity),
				entry.hlc,
			)
			.unwrap();
		assert!(
			!locations.is_empty(),
			"a survivor was repaired before the report"
		);
		assert!(
			locations
				.iter()
				.all(|location| location.is_tombstone == entry.tombstone)
		);
	}
	assert_eq!(
		cluster.read_once_validated(follower, &descriptor).await,
		expected,
		"exact from the repaired base"
	);

	// A version whose owner entry is not its child's: no report from it.
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7_at(floor + 1)),
			named_rows(&descriptor, &[(3, "a")]),
		)
		.await,
		"a later row",
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	let intent = loop {
		if let Some(intent) = complete_intents(&index).await.into_iter().next() {
			break intent;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the later row's intent"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	};
	index
		.set_version_owner_for_test(
			TenantId::ZERO,
			intent.manifest[0],
			(intent.rsv, intent.ordinal + 1),
		)
		.await
		.unwrap();
	jump_floor(&cluster, floor + 120_000).await;
	cluster
		.until("the other two reported", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().pending.as_ref())
				.is_some_and(|pending| pending.reports.len() == 2)
		})
		.await;
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	let generations = generations_at_leader(&cluster).await;
	let pending = generations.pending.as_ref().expect("still pending");
	assert!(
		!pending.reports.contains_key(&follower),
		"a refused proof is never reported"
	);
	assert_eq!(generations.current_gid(), 1, "no commit without it");
	assert!(
		index
			.base_meta()
			.await
			.unwrap()
			.staged
			.is_none_or(|staged| staged.gid != pending.target.gid),
		"nothing staged for it either"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// CODEX-3 R3, CONTROLLED CLOCK: an unclean start whose certified base fails
/// its physical proof stays unready, and no close records it clean, even
/// after the independent tier recovery finished; the next start proves the
/// base again. Once the base proves, a close records it clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_failed_base_proof_is_never_recorded_clean() {
	use crate::metrics::observability::ReadinessReporter;
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();

	// Crash, then physical damage to the base.
	cluster.stop(follower, true).await;
	cluster.tamper_base(follower).await;
	for attempt in 0..2 {
		cluster.reopen(follower).await;
		let node = cluster.node(follower);
		assert!(node.recovered_after_unclean_shutdown(), "attempt {attempt}");
		// The independent tier recovery finishes...
		let mut validation = node.restore_validation.clone();
		tokio::time::timeout(
			BOUND,
			validation.wait_for(|status| matches!(status, RestoreValidationStatus::Complete)),
		)
		.await
		.expect("tier recovery finished")
		.unwrap();
		tokio::time::sleep(std::time::Duration::from_secs(2)).await;
		// ...but the base proof does not: unready, reads refused.
		assert_eq!(
			node.readiness_reporter()
				.snapshot()
				.await
				.degraded_reason
				.as_deref(),
			Some("CertifiedBaseUnvalidated"),
			"attempt {attempt}"
		);
		let answer = read_at(&cluster, follower, &descriptor).await;
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::BACKPRESSURED),
			"{answer:?}"
		);
		// A complete close does not record it clean.
		assert!(
			!cluster.close(follower).await,
			"attempt {attempt}: never recorded clean"
		);
	}

	// The damage undone (the tamper is an involution): the next unclean start
	// proves the base, serves exactly, and a close records it clean.
	cluster.tamper_base(follower).await;
	cluster.reopen(follower).await;
	until_settled(&cluster, &descriptor).await;
	assert!(cluster.node(follower).recovered_after_unclean_shutdown());
	assert_eq!(
		cluster.read_once_validated(follower, &descriptor).await,
		expected,
		"proved, then exact"
	);
	assert_ne!(
		cluster
			.node(follower)
			.readiness_reporter()
			.snapshot()
			.await
			.degraded_reason
			.as_deref(),
		Some("CertifiedBaseUnvalidated")
	);
	assert!(cluster.close(follower).await, "a proved base closes clean");
	cluster.reopen(follower).await;
	assert!(!cluster.node(follower).recovered_after_unclean_shutdown());
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0699 S5a: an orderly close waits for a certification step that holds
/// its lifecycle permit, rather than interrupting it, and records the
/// directory clean. (Aborting the step mid-way would drop the permit and
/// revoke the clean record for good.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_orderly_close_waits_for_a_certification_step_and_records_clean() {
	let (mut cluster, leader, _descriptor) = ready().await;
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	let (reached, release) = cluster
		.node(follower)
		.data_server
		.pause_coordinator_for_test(
			crate::network::data_server::child_writes::PauseStage::CertificationStep,
		);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("a certification step holds its permit");
	let at = cluster
		.nodes
		.iter()
		.position(|node| node.local_node.id == follower)
		.unwrap();
	let node = cluster.nodes.remove(at);
	cluster.network.unpublish(crate::schema::NodeId {
		id: follower,
		_reserved: 0,
	});
	let closing = tokio::spawn(node.shutdown());
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	assert!(!closing.is_finished(), "the close waits for the step");
	release.notify_one();
	tokio::time::timeout(std::time::Duration::from_secs(30), closing)
		.await
		.expect("the close finished")
		.unwrap()
		.unwrap();
	assert!(
		cluster.configs[&follower]
			.data_dir
			.join("CLEAN_SHUTDOWN")
			.exists(),
		"an orderly close records it clean"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Two rows written, a second follower stopped so the next target cannot
/// commit, the floor advanced, and the leader and `reporter` reported.
async fn staged_and_reported(
	cluster: &mut Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
) -> (u32, u32, Expected) {
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				cluster,
				leader,
				write_meta(descriptor, 1, uuid_v7()),
				named_rows(descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let followers: Vec<u32> = NODES.into_iter().filter(|node| *node != leader).collect();
	let (reporter, absent) = (followers[0], followers[1]);
	cluster.stop(absent, false).await;
	jump_floor(cluster, now + 60_000).await;
	cluster
		.until("the leader and the reporter reported", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().pending.as_ref())
				.is_some_and(|pending| {
					pending.reports.contains_key(&leader) && pending.reports.contains_key(&reporter)
				})
		})
		.await;
	(reporter, absent, expected)
}

/// CODEX-4, CONTROLLED CLOCK: a member that staged and reported, then
/// crashed before Commit, with a survivor's copy lost and its staged rows
/// changed under unchanged metadata. On its unclean reopen the recovered
/// candidate inherits nothing: it is rebuilt with this start's proofs (the
/// lost copy repaired from a peer), reproduces the staged evidence, and only
/// then are its staged rows restored. The generation commits and the member
/// serves exactly from the activated base.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_recovered_staged_candidate_is_reproved_and_restored_before_commit() {
	let (mut cluster, leader, descriptor) = ready().await;
	let (reporter, absent, expected) = staged_and_reported(&mut cluster, leader, &descriptor).await;
	let index = cluster.node(reporter).slab.persistent_index().unwrap();
	let survivor = complete_intents(&index).await[0].manifest[0];
	cluster.node(reporter).slab.forget_version_for_test(
		TenantId::ZERO,
		crate::schema::Identity::new(survivor.key_identity),
		survivor.hlc,
	);
	drop(index);
	cluster.stop(reporter, true).await;
	cluster.damage_staged(reporter).await;
	cluster.reopen(reporter).await;
	assert!(cluster.node(reporter).recovered_after_unclean_shutdown());
	let index = cluster.node(reporter).slab.persistent_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !index.staged_rows_match().await.unwrap() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"the recovered candidate was never re-proved"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	assert!(
		!cluster
			.node(reporter)
			.slab
			.version_locations(
				TenantId::ZERO,
				crate::schema::Identity::new(survivor.key_identity),
				survivor.hlc
			)
			.unwrap()
			.is_empty(),
		"the lost copy was repaired before reuse"
	);
	cluster.reopen(absent).await;
	until_settled(&cluster, &descriptor).await;
	until_active(&cluster, 1).await;
	assert_eq!(index.base_entries(false).await.unwrap().len(), 2);
	for node in NODES {
		assert_eq!(
			cluster.read_once_validated(node, &descriptor).await,
			expected,
			"at {node}"
		);
	}
	assert!(
		cluster.close(reporter).await,
		"a proved generation closes clean"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// CODEX-4, CONTROLLED CLOCK: the recorded-report arm. A member reported,
/// crashed, and its staged rows were shortened under unchanged metadata; the
/// generation commits while it is down. On its unclean reopen the committed
/// candidate is never installed: its rows do not hash to the certificate, so
/// the store is behind the boundary. It never answers a read, stays not
/// Ready, and a complete close does not record it clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_commit_during_recovery_never_installs_changed_staged_rows() {
	use crate::metrics::observability::ReadinessReporter;
	let (mut cluster, leader, descriptor) = ready().await;
	let (reporter, absent, expected) = staged_and_reported(&mut cluster, leader, &descriptor).await;
	cluster.stop(reporter, true).await;
	cluster.damage_staged(reporter).await;
	cluster.reopen(absent).await;
	cluster
		.until("committed while the reporter is down", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().current_gid() >= 1)
		})
		.await;
	cluster.reopen(reporter).await;
	let node = cluster.node(reporter);
	assert!(node.recovered_after_unclean_shutdown());
	let peer = node.data_server.decision_peer_for_test();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !peer
		.behind_boundary
		.load(std::sync::atomic::Ordering::Acquire)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the changed candidate was never refused"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	let index = node.slab.persistent_index().unwrap();
	assert_eq!(
		index.base_meta().await.unwrap().active_gid,
		0,
		"nothing installed"
	);
	for _ in 0..3 {
		let answer = read_at(&cluster, reporter, &descriptor).await;
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::BACKPRESSURED),
			"never a shortened answer: {answer:?}"
		);
		assert!(answer.batches.is_empty());
		tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	}
	assert_eq!(
		node.readiness_reporter()
			.snapshot()
			.await
			.degraded_reason
			.as_deref(),
		Some("CertifiedBaseUnvalidated")
	);
	drop(index);
	// The others serve exactly; the damaged member never certifies clean.
	for other in NODES.into_iter().filter(|node| *node != reporter) {
		assert_eq!(
			cluster.read_once_validated(other, &descriptor).await,
			expected,
			"at {other}"
		);
	}
	assert!(!cluster.close(reporter).await, "never recorded clean");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Every version of `key` the complete intents of `node` own, by HLC.
async fn owned_versions(
	cluster: &Cluster,
	node: u32,
	key: crate::schema::Identity,
) -> Vec<HybridTimestamp> {
	let index = cluster.node(node).slab.persistent_index().unwrap();
	let mut versions: Vec<HybridTimestamp> = complete_intents(&index)
		.await
		.iter()
		.flat_map(|intent| intent.manifest.iter())
		.filter(|version| version.key_identity == *key.as_bytes())
		.map(|version| version.hlc)
		.collect();
	versions.sort();
	versions
}

/// Overwrites one row three times, writes and deletes another, seals every
/// node's slab and certifies generation 1. Returns the rows' identities, the
/// obsolete versions and the expected answer.
async fn overwritten_and_certified(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
) -> (
	Vec<(crate::schema::Identity, HybridTimestamp)>,
	Expected,
	std::collections::BTreeMap<u32, (u64, crate::schema::SlabId)>,
) {
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for value in ["a", "b", "c"] {
		assert_committed(
			&write_through(
				cluster,
				leader,
				write_meta(descriptor, 1, uuid_v7()),
				named_rows(descriptor, &[(1, value)]),
			)
			.await,
			"an overwrite",
		);
	}
	expected.insert(1, "c".into());
	assert_committed(
		&write_through(
			cluster,
			leader,
			write_meta(descriptor, 1, uuid_v7()),
			named_rows(descriptor, &[(2, "a")]),
		)
		.await,
		"a row to delete",
	);
	let server = cluster.node(leader).data_server.clone();
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				descriptor,
				&named_rows(descriptor, &[(id, "x")]),
			)
			.0
	};
	let deleted = delete_through(cluster, leader, delete_meta(descriptor), vec![row_of(2)]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "{deleted:?}");
	expect_everywhere(cluster, descriptor, &expected, "before any generation").await;
	// The obsolete versions: row 1's first two, and row 2's written row (its
	// tombstone wins and stays).
	let (one, two) = (row_of(1), row_of(2));
	let ones = owned_versions(cluster, leader, one).await;
	let twos = owned_versions(cluster, leader, two).await;
	assert_eq!((ones.len(), twos.len()), (3, 2), "every version owned");
	let obsolete = vec![(one, ones[0]), (one, ones[1]), (two, twos[0])];
	// Sealed bytes before any generation, so before any cleanup.
	let mut sealed = std::collections::BTreeMap::new();
	for node in NODES {
		let slab = &cluster.node(node).slab;
		let sealed_id = slab.force_seal_active_for_test().await;
		sealed.insert(node, (slab.sealed_local_bytes_for_test().await, sealed_id));
	}
	jump_floor(cluster, now + 60_000).await;
	until_active(cluster, 1).await;
	(obsolete, expected, sealed)
}

/// ADR 0699 S5b, CONTROLLED CLOCK (a finite workflow, not a capacity test):
/// after generation 1, every node's cleanup reclaims exactly the obsolete
/// local versions (row 1's overwritten versions and row 2's deleted row) and
/// retires their debt, while the winners and the winning tombstone stay and
/// every read is exact. Distinct versions and sealed bytes are reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn certified_cleanup_reclaims_the_obsolete_local_versions() {
	let (cluster, leader, descriptor) = ready().await;
	// CODEX-3 R2: one follower's first cleanup copy read fails, as a transient
	// read error would. The step refuses, caches nothing, and a later visit at
	// the same generation reclaims.
	let flaky = *NODES.iter().find(|node| **node != leader).unwrap();
	let fail_once = cluster
		.node(flaky)
		.data_server
		.decision_peer_for_test()
		.fail_copy_read_once
		.clone();
	fail_once.store(true, std::sync::atomic::Ordering::Release);
	let (obsolete, expected, before) =
		overwritten_and_certified(&cluster, leader, &descriptor).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in NODES {
		let slab = &cluster.node(node).slab;
		let index = slab.persistent_index().unwrap();
		loop {
			let mut stored = 0;
			for (key, hlc) in &obsolete {
				stored += slab
					.version_locations(TenantId::ZERO, *key, *hlc)
					.unwrap()
					.len();
			}
			let debt = index.debt_page(None, 100).await.unwrap().len();
			if stored == 0 && debt == 0 {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node}: {stored} obsolete copies and {debt} debt records remain"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
		// The winners stay: one entry per key, every one stored.
		let base = index.base_entries(false).await.unwrap();
		assert_eq!(base.len(), 2, "at {node}");
		for entry in &base {
			assert!(
				!slab
					.version_locations(
						entry.tenant,
						crate::schema::Identity::new(entry.key_identity),
						entry.hlc
					)
					.unwrap()
					.is_empty(),
				"a certified winner is stored at {node}"
			);
		}
		let after = slab.sealed_local_bytes_for_test().await;
		let (before_bytes, source) = before[&node];
		let source_on_disk = slab.sealed_slab_ids().await.contains(&source);
		eprintln!(
			"S5b node {node}: {} obsolete versions reclaimed; sealed local bytes {before_bytes} before cleanup, {after} after (rewritten source slab still awaiting disposal: {source_on_disk})",
			obsolete.len(),
		);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "after cleanup").await;
	assert!(
		!fail_once.load(std::sync::atomic::Ordering::Acquire),
		"the failing read was attempted"
	);
	assert_eq!(
		generations_at_leader(&cluster).await.current_gid(),
		1,
		"reclaimed at the same generation as the failed visit"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0699 S5b, CONTROLLED CLOCK: a cleanup interrupted before its rewrite
/// (the close past its drain bound aborts it) revokes the clean record; the
/// close reports the interruption (ADR 0706: `TimedOut`, naming decision
/// maintenance); the next start is unclean, proves its certified base, and
/// reads exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_interrupted_cleanup_is_proved_on_an_unclean_reopen() {
	let (mut cluster, leader, descriptor) = ready().await;
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	let (reached, _release) = cluster
		.node(follower)
		.data_server
		.pause_coordinator_for_test(
			crate::network::data_server::child_writes::PauseStage::CleanupBeforeRewrite,
		);
	let (_obsolete, expected, _) = overwritten_and_certified(&cluster, leader, &descriptor).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the cleanup reached its rewrite");
	// Never released: the close drains for its bound, then aborts the step.
	let (closed, clean) = cluster.close_with_result(follower).await;
	let error = closed.expect_err("a close past its drain bound reports it");
	assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
	assert!(
		error.to_string().contains("decision maintenance"),
		"the interrupted service is named: {error}"
	);
	assert!(
		!clean,
		"an interrupted cleanup never records a clean shutdown"
	);
	cluster.reopen(follower).await;
	assert!(cluster.node(follower).recovered_after_unclean_shutdown());
	until_settled(&cluster, &descriptor).await;
	assert_eq!(
		cluster.read_once_validated(follower, &descriptor).await,
		expected,
		"proved, then exact"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// CODEX-3 R1, CONTROLLED CLOCK: obsolete versions of a KV key (the reserved
/// default KV table, whose descriptor is synthetic) are reclaimed like any
/// table's: after three overwrites and generation 1, every node's sealed
/// cleanup removes the two older versions and retires their debt, and the
/// key reads its latest value everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn certified_cleanup_reclaims_obsolete_kv_versions() {
	let (cluster, leader, _descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	for value in [&b"one"[..], b"two", b"three"] {
		let (meta, payload) = kv_write_meta(b"alpha", value);
		let response = kv_write_through(&cluster, leader, meta, payload).await;
		assert_eq!(response.status, WriteStatus::OK, "{response:?}");
	}
	kv_expect_everywhere(&cluster, b"alpha", Some(b"three"), "before any generation").await;
	// The KV versions, oldest first: the two older are obsolete.
	let index = cluster.node(leader).slab.persistent_index().unwrap();
	let mut versions: Vec<(crate::schema::Identity, HybridTimestamp)> = complete_intents(&index)
		.await
		.iter()
		.flat_map(|intent| intent.manifest.iter())
		.filter(|version| version.table == TableObjectId::DEFAULT_KV)
		.map(|version| {
			(
				crate::schema::Identity::new(version.key_identity),
				version.hlc,
			)
		})
		.collect();
	versions.sort_by_key(|(_, hlc)| *hlc);
	assert_eq!(versions.len(), 3, "every KV version owned");
	let obsolete = &versions[..2];
	for node in NODES {
		cluster.node(node).slab.force_seal_active_for_test().await;
	}
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in NODES {
		let slab = &cluster.node(node).slab;
		let index = slab.persistent_index().unwrap();
		loop {
			let mut stored = 0;
			for (key, hlc) in obsolete {
				stored += slab
					.version_locations(TenantId::ZERO, *key, *hlc)
					.unwrap()
					.len();
			}
			let debt = index.debt_page(None, 100).await.unwrap().len();
			if stored == 0 && debt == 0 {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node}: {stored} obsolete KV copies and {debt} debt records remain"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
	kv_expect_everywhere(&cluster, b"alpha", Some(b"three"), "after cleanup").await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0700, the failover transition behind the workflow's stalled writes
/// (diag4): the original leader never forwards anything to a follower, so
/// after it stops the new leader has clock samples from its one live peer
/// only. Its decisions must still pass the clock gate, and a write becomes
/// available again: one refused by the new authority is resent and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_new_leader_with_one_voter_down_still_decides() {
	let (mut cluster, leader, descriptor) = ready().await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(1, "a")]),
		)
		.await,
		"before the failover",
	);
	cluster.stop(leader, false).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	let successor = loop {
		let current = cluster.leader().await;
		if current != leader {
			break current;
		}
		assert!(tokio::time::Instant::now() < deadline, "no new leader");
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	// A parent refused by the new authority (its lineage closed by the new
	// leader's grant) is a recorded, definite not-committed outcome: the write
	// is sent again under a new identity, within the same bound. Any other
	// answer keeps its identity (write_through already retries a stall).
	let committed = loop {
		let response = write_through(
			&cluster,
			successor,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(2, "b")]),
		)
		.await;
		if response.write.status == WriteStatus::OK
			&& response.write.mutation_commit_knowledge()
				== MutationCommitKnowledge::KNOWN_NOT_COMMITTED
			&& tokio::time::Instant::now() < deadline
		{
			continue;
		}
		break response;
	};
	assert_committed(&committed, "after the failover, with one voter down");
	let expected: Expected = [(1, "a".to_string()), (2, "b".to_string())].into();
	for node in NODES.into_iter().filter(|node| *node != leader) {
		expect_at(&cluster, node, &descriptor, &expected, "after the failover").await;
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0700 (r1 R2), the workflow's "with the old leader back" stage (diag3:
/// early eof): after a failover the old leader is reopened, and every query at
/// it, before and after it is Ready, ends in a typed terminal. None is a bare
/// close. It answers OK with exactly the acknowledged rows within the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reopened_member_answers_every_query_with_a_typed_terminal() {
	let (mut cluster, leader, descriptor) = ready().await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(1, "a")]),
		)
		.await,
		"before the failover",
	);
	cluster.stop(leader, false).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster.leader().await == leader {
		assert!(tokio::time::Instant::now() < deadline, "no new leader");
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	cluster.reopen(leader).await;
	let expected: Expected = [(1, "a".to_string())].into();
	let mut seen = std::collections::BTreeMap::new();
	loop {
		let answer = cluster
			.node(leader)
			.data_server
			.table_query_for_test(query_meta(&cluster, leader, &descriptor))
			.await
			.unwrap_or_else(|error| panic!("a bare close at the reopened member: {error:?}"));
		let status = answer
			.status
			.unwrap_or_else(|| panic!("no terminal at the reopened member: {answer:?}"));
		*seen.entry(status.as_str()).or_insert(0u32) += 1;
		if status == crate::schema::ReadStatus::OK {
			assert_eq!(answered(&answer), expected, "{seen:?}");
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never answered OK: {seen:?}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	eprintln!("reopened member answers: {seen:?}");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0700 (s6-inst2: "stale location in slab 0: fenced" closed a table
/// query stream bare at a reopened member): a query refused at admission
/// because the tier authority is not ready is answered with a typed,
/// retryable terminal, then served once the authority is ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_query_before_the_tier_authority_is_ready_gets_a_typed_retryable_terminal() {
	let (cluster, leader, descriptor) = ready().await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(1, "a")]),
		)
		.await,
		"a row",
	);
	let node = NODES.into_iter().find(|id| *id != leader).unwrap();
	cluster.node(node).slab.set_awaiting_cluster_for_test(true);
	let held = cluster
		.node(node)
		.data_server
		.table_query_for_test(query_meta(&cluster, node, &descriptor))
		.await
		.expect("a typed terminal, not a bare close");
	assert_eq!(held.status, Some(crate::schema::ReadStatus::BACKPRESSURED));
	assert!(held.batches.is_empty());
	cluster.node(node).slab.set_awaiting_cluster_for_test(false);
	expect_at(
		&cluster,
		node,
		&descriptor,
		&[(1, "a".to_string())].into(),
		"once ready",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0701: LEASES_REQ at the leader, as a follower sends it. A caller that
/// is not the batch's holder, or a range that is not the holder's own, refuses
/// the whole request before anything is signed or applied; the holder's own
/// ranges are granted with one ordered result each.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lease_batch_request_passes_the_single_grant_checks_for_every_range() {
	use crate::schema::{
		AcquireLeasesRequestHeader, AcquireLeasesResponseHeader, LeaseRangeWire, LeaseResultWire,
		decode_lease_batch,
	};
	let (cluster, leader, _descriptor) = ready().await;
	let follower = *NODES.iter().find(|id| **id != leader).unwrap();
	let third = *NODES
		.iter()
		.find(|id| **id != leader && **id != follower)
		.unwrap();
	let node_id = |id: u32| crate::schema::NodeId { id, _reserved: 0 };
	let ring = cluster.node(leader).ring.snapshot();
	let own = ring.primary_ranges_for(node_id(follower));
	let foreign = ring.primary_ranges_for(node_id(third))[0];
	let epoch = ring.epoch();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let send = |caller: u32, holder: u32, ranges: Vec<(u64, u64)>| {
		let router = cluster.node(leader).router.clone();
		async move {
			let header = AcquireLeasesRequestHeader {
				epoch,
				holder: node_id(holder),
				requested_hlc: crate::schema::HybridTimestamp {
					physical_ms: now,
					logical: 0,
					node_id: holder,
				},
				ttl_ms: 3_000,
				count: ranges.len() as u32,
			};
			let mut meta = bytemuck::bytes_of(&header).to_vec();
			for (range_start, range_end) in &ranges {
				meta.extend_from_slice(bytemuck::bytes_of(&LeaseRangeWire {
					range_start: *range_start,
					range_end: *range_end,
				}));
			}
			let frame = crate::network::core::HrftFrame {
				header: crate::schema::FrameHeader {
					magic: crate::network::core::HRFT_MAGIC,
					version: 1,
					msg_type: crate::schema::MsgType::LEASES_REQ.0,
					flags: 0,
					stream_id: 1,
					meta_len: meta.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta: crate::network::pool::PooledBuf::orphan(meta),
				arrow_payload: None,
				trailing: None,
			};
			let context = crate::network::server::RequestContext {
				remote: Some(node_id(caller)),
				actor: crate::network::tls::ActorIdentity::data_node(node_id(caller)),
			};
			crate::network::server::HrftService::handle_frame(&*router, context, frame)
				.await
				.unwrap()
				.unwrap()
		}
	};
	let refused_whole = |response: crate::network::core::HrftFrame| {
		let header: AcquireLeasesResponseHeader = bytemuck::pod_read_unaligned(&response.meta);
		assert_eq!(
			response.meta.len(),
			std::mem::size_of::<AcquireLeasesResponseHeader>()
		);
		assert_eq!(
			(header.status, header.count),
			(crate::schema::LeaseStatus::DENIED_INVALID_TARGET, 0)
		);
	};
	let issued_at = |cluster: &Cluster, ranges: Vec<(u64, u64)>| {
		let state_machine = cluster.node(leader).state_machine.clone();
		async move {
			state_machine
				.read_state(move |state| {
					ranges
						.iter()
						.filter(|(low, _)| {
							state
								.leases
								.get(low)
								.is_some_and(|lease| lease.lease_start.physical_ms == now)
						})
						.count()
				})
				.await
		}
	};
	// Another node asks for the follower's ranges.
	refused_whole(send(third, follower, own.clone()).await);
	// The follower includes a range that is not its own.
	let mut mixed = own.clone();
	mixed.push(foreign);
	refused_whole(send(follower, follower, mixed.clone()).await);
	assert_eq!(
		issued_at(&cluster, mixed).await,
		0,
		"nothing was signed or applied"
	);
	// The follower's own ranges: one ordered result each.
	let granted = send(follower, follower, own.clone()).await;
	let (header, results): (AcquireLeasesResponseHeader, Vec<LeaseResultWire>) =
		decode_lease_batch(&granted.meta, |header: &AcquireLeasesResponseHeader| {
			header.count
		})
		.unwrap();
	assert_eq!(header.status, crate::schema::LeaseStatus::GRANTED);
	assert_eq!(
		results
			.iter()
			.map(|result| result.range_start)
			.collect::<Vec<_>>(),
		own.iter().map(|(low, _)| *low).collect::<Vec<_>>()
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0701: a ParentDecisions pass stamps each batch from the clock read when
/// that batch is built; one chunk here, so the whole pass shares one reading
/// taken for it, not one carried across earlier waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lease_batch_is_stamped_when_it_is_built() {
	struct Ticking(std::sync::atomic::AtomicU64);
	impl crate::consensus::monitor::PhysicalClock for Ticking {
		fn now_ms(&self) -> u64 {
			self.0.fetch_add(50, std::sync::atomic::Ordering::SeqCst)
		}
	}
	let (cluster, leader, _descriptor) = ready().await;
	let node = cluster.node(leader);
	let start = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64
		+ 1_000;
	let clock = Ticking(std::sync::atomic::AtomicU64::new(start));
	let ranges = node
		.ring
		.snapshot()
		.primary_ranges_for(node.local_node)
		.len();
	let granted = node
		.data_server
		.refresh_leases_once(&node.ring, &clock, 3_000)
		.await;
	let local = node.local_node.id;
	let starts: Vec<u64> = node
		.state_machine
		.read_state(move |state| {
			state
				.leases
				.values()
				.filter(|lease| lease.holder.id == local && lease.lease_start.physical_ms >= start)
				.map(|lease| lease.lease_start.physical_ms)
				.collect()
		})
		.await;
	assert!(
		ranges > 1 && ranges <= crate::schema::MAX_LEASES_PER_BATCH,
		"{ranges}"
	);
	assert_eq!(granted, ranges, "one batch renewed every primary range");
	assert_eq!(starts.len(), ranges);
	assert!(starts.iter().all(|stamp| *stamp == start), "{starts:?}");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0703, CONTROLLED CLOCK (a finite workflow, not a capacity test): on
/// the leader, whose local cleanup is paused, certified LSM compaction of the
/// L0 runs holding the overwritten versions elides exactly the obsolete
/// versions after generation 1. The winners and the winning tombstone stay,
/// the leader's debt retires, and every read is exact everywhere. Distinct
/// versions and elided rows are reported separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn certified_lsm_elides_the_obsolete_versions_on_the_leader() {
	use std::sync::atomic::Ordering::{Acquire, Release};
	let (cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	// Each write lands in its own L0 run on the leader.
	let mut expected = Expected::new();
	for value in ["a", "b", "c"] {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(1, value)]),
			)
			.await,
			"an overwrite",
		);
		slab.force_seal_active_for_test().await;
	}
	expected.insert(1, "c".into());
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(2, "a")]),
		)
		.await,
		"a row to delete",
	);
	slab.force_seal_active_for_test().await;
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let deleted = delete_through(&cluster, leader, delete_meta(&descriptor), vec![row_of(2)]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "{deleted:?}");
	slab.force_seal_active_for_test().await;
	expect_everywhere(&cluster, &descriptor, &expected, "before any generation").await;
	let (one, two) = (row_of(1), row_of(2));
	let ones = owned_versions(&cluster, leader, one).await;
	let twos = owned_versions(&cluster, leader, two).await;
	assert_eq!((ones.len(), twos.len()), (3, 2), "every version owned");
	let obsolete = [(one, ones[0]), (one, ones[1]), (two, twos[0])];
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	// Certified LSM on the leader, driven here: every L0 run qualifies.
	let worker = crate::storage::LsmCompactionWorker::new(
		crate::storage::LsmConfig {
			max_l0_slab_count: 1,
			lsm_l0_max_age_millis: 0,
			..crate::storage::LsmConfig::default()
		},
		slab.slab_pin_table(),
	)
	.unwrap();
	// ADR 0725: as the node's own committer is built, with its owner authority.
	let committer = crate::storage::RaftCommitter::new(cluster.node(leader).raft.clone())
		.with_state_machine(cluster.node(leader).state_machine.clone())
		.with_owner_authority(cluster.node(leader).data_server.owner_authority());
	let at = HybridTimestamp {
		physical_ms: now + 120_000,
		logical: 0,
		node_id: 0,
	};
	let index = slab.persistent_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	let (mut jobs, mut elided) = (0, 0);
	loop {
		let mut stored = 0;
		for (key, hlc) in &obsolete {
			stored += slab
				.version_locations(TenantId::ZERO, *key, *hlc)
				.unwrap()
				.len();
		}
		if stored == 0 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{stored} obsolete copies remain on the leader after {jobs} certified jobs"
		);
		match server
			.certified_lsm_step_unpaused(&worker, at, &committer)
			.await
		{
			Ok(Some(outcome)) => {
				jobs += 1;
				elided += outcome.elided_versions;
			}
			Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
			Err(error) => panic!("a certified LSM job failed: {error}"),
		}
	}
	// The winners stay on the leader: one entry per key, every one stored.
	let base = index.base_entries(false).await.unwrap();
	assert_eq!(base.len(), 2);
	for entry in &base {
		assert!(
			!slab
				.version_locations(
					entry.tenant,
					crate::schema::Identity::new(entry.key_identity),
					entry.hlc
				)
				.unwrap()
				.is_empty(),
			"a certified winner is stored"
		);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "after certified LSM").await;
	// The leader's debt retires through the ordinary sweep.
	pause.store(false, Release);
	assert!(!pause.load(Acquire));
	loop {
		let debt = index.debt_page(None, 100).await.unwrap().len();
		if debt == 0 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{debt} debt records remain on the leader"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	eprintln!(
		"ADR 0703 leader {leader}: {} obsolete versions elided as {elided} rows by {jobs} certified LSM jobs",
		obsolete.len()
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0704, CONTROLLED CLOCK (a finite workflow, not a capacity test): every
/// member tiers to its own LocalFs object store. On the leader, whose cleanup is
/// paused while the data is laid out, three tiered slabs hold: S1 an obsolete
/// version of row 1, row 3 (never overwritten) and an obsolete version of row 4
/// that also keeps a second, local copy; S2 only an obsolete version of row 2;
/// S3 the winners. After generation 1 the leader's own leader-gated step (the
/// LSM service tick) rewrites S1 to exactly row 3 under a certified Tier and
/// retires S2 under a certified Retire; S3 has nothing to reclaim. Reads stay
/// exact everywhere, before, after and after a same-store restart of the
/// leader; debt retires except row 4's, whose local copy stays; the replaced
/// source objects are reported as retained and stay in the object store;
/// followers' tiered state is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn certified_tier_cleanup_rewrites_and_retires_the_leaders_tiered_slabs() {
	certified_tier_cleanup_workflow(false).await;
}

/// The same workflow, with the leader's first certified step that the ring
/// accepts ended by an error right after acceptance (the `AfterAcceptance`
/// cut): the step returns its error to the serial tick, nothing after it in
/// the proposer runs, and the accepted work is completed by the apply observer
/// and the operation driver. Retention and its durable records, reads, debt and
/// the restart are held to the same assertions as the uncut workflow. One
/// injected error at one point: not every fallible exit, and not a process crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn certified_tier_cleanup_recovers_a_step_that_errs_after_acceptance() {
	certified_tier_cleanup_workflow(true).await;
}

async fn certified_tier_cleanup_workflow(cut_after_acceptance: bool) {
	use crate::storage::core::StorageEngine;
	use crate::storage::l2::ObjectStore;
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor) = ready_with(Cluster::start_with(|id, cfg| {
		let root = crate::test_support::TestDir::new(&format!("certified-tier-l2-{id}"));
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs {
			root: root.to_path_buf(),
		});
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		Some(root)
	}))
	.await;
	let l2_of = |cluster: &Cluster, id: u32| match cluster.configs[&id].l2.as_ref().unwrap() {
		crate::storage::ObjectStoreConfig::LocalFs { root } => {
			crate::storage::l2::LocalFsObjectStore::new(root).unwrap()
		}
		other => panic!("{other:?}"),
	};
	let server = cluster.node(leader).data_server.clone();
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	let write = async |rows: &[(i64, &str)], what: &str| {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, rows.len(), uuid_v7()),
				named_rows(&descriptor, rows),
			)
			.await,
			what,
		);
	};
	// Row 3 is its own write (its own HLC): a survivor written in one parent
	// with a reclaimed row of the same length would take that row's exact
	// location in the replacement, which the tier layout refuses as a
	// collision (reported with this stage, not exercised here).
	write(&[(1, "a"), (4, "a")], "S1's obsolete rows").await;
	write(&[(3, "x")], "S1's survivor").await;
	let s1 = slab.force_seal_active_for_test().await;
	write(&[(2, "a")], "S2").await;
	let s2 = slab.force_seal_active_for_test().await;
	write(&[(1, "b"), (2, "b"), (4, "b")], "S3 (the winners)").await;
	let s3 = slab.force_seal_active_for_test().await;
	for (id, value) in [(1, "b"), (2, "b"), (3, "x"), (4, "b")] {
		expected.insert(id, value.into());
	}
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let (k1, k2, k3, k4) = (row_of(1), row_of(2), row_of(3), row_of(4));
	let versions = |key| owned_versions(&cluster, leader, key);
	let (v1, v2, v3, v4) = (
		versions(k1).await,
		versions(k2).await,
		versions(k3).await,
		versions(k4).await,
	);
	assert_eq!(
		(v1.len(), v2.len(), v3.len(), v4.len()),
		(2, 2, 1, 2),
		"every version owned"
	);
	// The leader's sweeper tiers its three sealed slabs; their local files go.
	let deadline = tokio::time::Instant::now() + BOUND;
	for id in [s1, s2, s3] {
		while slab.tier_pointer(id).await.is_none() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"slab {id:?} never tiered"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
		assert!(!slab.sealed_slab_ids().await.contains(&id), "no local copy");
	}
	// Row 4's obsolete version also keeps a second indexed copy, local.
	let location = slab.version_locations(TenantId::ZERO, k4, v4[0]).unwrap()[0];
	let frame = slab.read(location).await.unwrap();
	let locality = server
		.row_identity_and_locality_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(4, "x")]),
		)
		.1;
	slab.append_durable_with_locality(TenantId::ZERO, k4, locality, v4[0], &frame)
		.await
		.unwrap();
	assert_eq!(
		slab.version_locations(TenantId::ZERO, k4, v4[0])
			.unwrap()
			.len(),
		2
	);
	let sources = [
		slab.tier_pointer(s1).await.unwrap(),
		slab.tier_pointer(s2).await.unwrap(),
	];
	let s3_pointer = slab.tier_pointer(s3).await.unwrap();
	let leader_l2 = l2_of(&cluster, leader);
	// The source objects, byte for byte, as they are before the certified step.
	let mut source_objects = Vec::new();
	for source in &sources {
		source_objects.push(leader_l2.get(&source.l2_uri).await.unwrap());
	}
	let followers: Vec<u32> = NODES.into_iter().filter(|node| *node != leader).collect();
	let mut follower_tiers = std::collections::BTreeMap::new();
	for follower in &followers {
		follower_tiers.insert(
			*follower,
			cluster.node(*follower).slab.list_tiered_slabs().await,
		);
	}
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"before the certified step",
	)
	.await;
	let index = slab.persistent_index().unwrap();
	let debt = |index: Arc<crate::storage::index::PersistentLocalIndex>| async move {
		let mut debt: Vec<_> = index
			.debt_page(None, 100)
			.await
			.unwrap()
			.into_iter()
			.map(|(key, _)| (crate::schema::Identity::new(key.key_identity), key.hlc))
			.collect();
		debt.sort();
		debt
	};
	let mut obsolete = vec![(k1, v1[0]), (k2, v2[0]), (k4, v4[0])];
	obsolete.sort();
	assert_eq!(
		debt(index.clone()).await,
		obsolete,
		"premise: the leader's debt"
	);
	assert_eq!(slab.certified_retained_objects_for_test().count, 0);
	if cut_after_acceptance {
		slab.cut_next_certified_tier_step(
			crate::storage::manager::certified_tier::CertifiedTierCut::AfterAcceptance,
		);
	}
	// The leader's own leader-gated step runs, from its LSM service's tick.
	pause.store(false, Release);
	let copies = |key, hlc| {
		slab.version_locations(TenantId::ZERO, key, hlc)
			.unwrap()
			.len()
	};
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let rewritten = slab
			.tier_pointer(s1)
			.await
			.is_some_and(|pointer| pointer.l2_uri.contains("certified/"));
		let retired = slab.tier_pointer(s2).await.is_none();
		if rewritten && retired && copies(k1, v1[0]) == 0 && copies(k2, v2[0]) == 0 {
			break;
		}
		if tokio::time::Instant::now() >= deadline {
			// What the step answers now, for the failure's cause.
			// ADR 0725: as the node's own committer is built, with its owner authority.
			let committer = crate::storage::RaftCommitter::new(cluster.node(leader).raft.clone())
				.with_state_machine(cluster.node(leader).state_machine.clone())
				.with_owner_authority(cluster.node(leader).data_server.owner_authority());
			let answer = server.certified_tier_step(&committer).await;
			panic!("S1 rewritten {rewritten}, S2 retired {retired}; the step answers {answer:?}");
		}
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	// S1 is exactly row 3, under the ring's certified provenance; S2 is gone
	// from the ring; S3 is the same generation.
	let owner = slab.owner_ref().unwrap();
	let state = &cluster.node(leader).state_machine;
	let committed = state
		.tiered_slab(owner.slab(s1))
		.await
		.expect("S1 committed");
	let provenance = committed.certified.clone().expect("certified provenance");
	assert_eq!(
		provenance.source,
		crate::consensus::slab_ops::TierGeneration::from(&sources[0])
	);
	let durable = index.load_snapshot().await.unwrap();
	let s1_rows: Vec<_> = durable
		.record_rows
		.iter()
		.filter(|row| row.location.slab_id == s1)
		.map(|row| (row.key_identity, row.location.hlc))
		.collect();
	assert_eq!(s1_rows, vec![(k3, v3[0])], "the exact survivor set");
	assert_eq!(
		crate::storage::index::lsm_rows_digest(
			durable
				.record_rows
				.iter()
				.filter(|row| row.location.slab_id == s1)
				.copied()
		),
		provenance.postimage_rows_digest
	);
	assert_eq!(state.tiered_slab(owner.slab(s2)).await, None, "S2 retired");
	assert_eq!(slab.tier_pointer(s3).await, Some(s3_pointer.clone()));
	assert_eq!((copies(k4, v4[0]), copies(k3, v3[0])), (1, 1));
	for (key, versions) in [(k1, &v1), (k2, &v2), (k4, &v4)] {
		assert_eq!(copies(key, versions[1]), 1, "a winner stays");
	}
	expect_everywhere(&cluster, &descriptor, &expected, "after the certified step").await;
	// Debt retires where no indexed copy is left; row 4's local copy keeps its.
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let left = debt(index.clone()).await;
		if left == vec![(k4, v4[0])] {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the leader's debt is {left:?}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	// The replaced sources are retained and never deleted. The contract is the
	// store's own: a durable retained-object record per source, in its published
	// state, and no operation left open. The rewrite's record is published after
	// its operation is handed off, so the wait is on the records themselves.
	use crate::storage::index::RetainedObjectIntentState::{
		ReplacementPublished, RetirementPublished,
	};
	let replacement_uri = slab.tier_pointer(s1).await.unwrap().l2_uri;
	let published = [(s1, ReplacementPublished), (s2, RetirementPublished)];
	let deadline = tokio::time::Instant::now() + BOUND;
	let records = loop {
		let intents = index.load_snapshot_cleanup_intents().await.unwrap();
		let open_ops = slab
			.require_op_index()
			.unwrap()
			.load_op_intents()
			.await
			.unwrap();
		let records: Vec<_> = sources
			.iter()
			.zip(published)
			.map(|(source, (id, _))| {
				intents
					.iter()
					.find(|(slab_id, uri, _)| *slab_id == id && uri == &source.l2_uri)
					.map(|(_, _, intent)| intent.clone())
			})
			.collect();
		let settled = records.iter().zip(published).all(|(record, (_, state))| {
			record.as_ref().is_some_and(|intent| intent.state == state)
		});
		if settled && open_ops.is_empty() {
			break records;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the retained records are {records:?}; open operations {open_ops:?}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	let (s1_record, s2_record) = (records[0].clone().unwrap(), records[1].clone().unwrap());
	assert_eq!(
		(s1_record.bytes, s2_record.bytes),
		(
			Some(source_objects[0].len() as u64),
			Some(source_objects[1].len() as u64)
		)
	);
	assert_eq!(s1_record.replacement_uri.as_ref(), Some(&replacement_uri));
	assert!(s1_record.published_replacements.contains(&replacement_uri));
	assert_eq!(s2_record.replacement_uri, None);
	let tiered_now = slab.list_tiered_slabs().await;
	for (source, object) in sources.iter().zip(&source_objects) {
		assert_eq!(
			&leader_l2
				.get(&source.l2_uri)
				.await
				.expect("a retained object stays"),
			object,
			"the retained object is the source, byte for byte"
		);
		assert!(
			!tiered_now
				.iter()
				.any(|pointer| pointer.l2_uri == source.l2_uri),
			"never indexed again"
		);
	}
	// What the proposer itself reported: never anything but the sources. It is
	// not the retention record and claims no completeness, cut or not: nothing
	// here orders a proposer's own note before this sample.
	let reported = slab.certified_retained_objects_for_test();
	for uri in &reported.uris {
		assert!(sources.iter().any(|source| &source.l2_uri == uri), "{uri}");
	}
	if cut_after_acceptance {
		assert_eq!(
			slab.certified_tier_cut_pending_for_test(),
			None,
			"the armed cut was consumed"
		);
	}
	eprintln!(
		"ADR 0704 leader {leader}: certified rewrite of {s1:?} and retirement of {s2:?}; \
		 2 retained records, {} bytes; the proposer reported {}",
		source_objects[0].len() + source_objects[1].len(),
		reported.count
	);
	// Followers: tiered state untouched, no certified work.
	for follower in &followers {
		let node = cluster.node(*follower);
		assert_eq!(
			node.slab.list_tiered_slabs().await,
			follower_tiers[follower],
			"at {follower}"
		);
		assert_eq!(node.slab.certified_retained_objects_for_test().count, 0);
		assert!(
			node.slab
				.require_op_index()
				.unwrap()
				.load_op_intents()
				.await
				.unwrap()
				.iter()
				.all(|op| op.certificate_requirement()
					== crate::storage::index::CertificateRequirement::None),
			"at {follower}"
		);
	}
	drop(index);
	drop(slab);
	drop(server);
	// A same-store restart of the leader: reads stay exact, the rewrite and the
	// retirement stand, and the retained objects are still in the object store.
	cluster.stop(leader, false).await;
	cluster.reopen(leader).await;
	until_settled(&cluster, &descriptor).await;
	assert_eq!(
		cluster.read_once_validated(leader, &descriptor).await,
		expected,
		"after the leader's restart"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the restart").await;
	let restarted = cluster.node(leader).slab.clone();
	restarted.wait_tier_authority_ready().await.unwrap();
	assert_eq!(
		restarted
			.tier_pointer(s1)
			.await
			.map(|pointer| pointer.l2_uri),
		committed_uri(&committed)
	);
	assert_eq!(restarted.tier_pointer(s2).await, None);
	assert_eq!(
		restarted
			.version_locations(TenantId::ZERO, k1, v1[0])
			.unwrap()
			.len() + restarted
			.version_locations(TenantId::ZERO, k2, v2[0])
			.unwrap()
			.len(),
		0,
		"no resurrection"
	);
	let leader_l2 = l2_of(&cluster, leader);
	for (source, object) in sources.iter().zip(&source_objects) {
		assert_eq!(
			&leader_l2
				.get(&source.l2_uri)
				.await
				.expect("a retained object stays after the restart"),
			object,
			"byte for byte after the restart"
		);
		assert!(
			!restarted
				.list_tiered_slabs()
				.await
				.iter()
				.any(|pointer| pointer.l2_uri == source.l2_uri),
			"never indexed again after the restart"
		);
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

fn committed_uri(tier: &crate::consensus::TieredSlab) -> Option<String> {
	Some(tier.l2_uri.clone())
}

/// DC-195, fixed by ADR 0705 (acceptance 1), the certified arm, CONTROLLED
/// CLOCK: the original workflow shape, row 3 written in the same parent as row
/// 1 with a value of the same length, so a replacement written from offset 0
/// would put survivor row 3 at row 1's exact old location. The certified
/// rewrite now starts at its source's logical end: it publishes, reads are
/// exact everywhere before, after and after the leader's real restart, row 3's
/// new location is at or above the source's end, and a holder of row 1's
/// pruned location gets `Superseded` in process and `OutsideGeneration` after
/// the restart, never another row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certified_rewrite_never_reissues_a_pruned_rows_location() {
	use crate::storage::l2::ObjectStore;
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor) = ready_with(Cluster::start_with(|id, cfg| {
		let root = crate::test_support::TestDir::new(&format!("dc195-certified-l2-{id}"));
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs {
			root: root.to_path_buf(),
		});
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		Some(root)
	}))
	.await;
	let leader_l2 = match cluster.configs[&leader].l2.as_ref().unwrap() {
		crate::storage::ObjectStoreConfig::LocalFs { root } => {
			crate::storage::l2::LocalFsObjectStore::new(root).unwrap()
		}
		other => panic!("{other:?}"),
	};
	let server = cluster.node(leader).data_server.clone();
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	// The original workflow shape: S1 is one parent write of rows 1, 3 and 4
	// (rows 1 and 4 to be obsoleted, row 3 to survive); S2 row 2; S3 the winners.
	let mut seal = Vec::new();
	for (rows, what) in [
		(&[(1, "a"), (3, "x"), (4, "a")][..], "S1"),
		(&[(2, "a")][..], "S2"),
		(&[(1, "b"), (2, "b"), (4, "b")][..], "S3 (the winners)"),
	] {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, rows.len(), uuid_v7()),
				named_rows(&descriptor, rows),
			)
			.await,
			what,
		);
		seal.push(slab.force_seal_active_for_test().await);
	}
	let mut expected = Expected::new();
	for (id, value) in [(1, "b"), (2, "b"), (3, "x"), (4, "b")] {
		expected.insert(id, value.into());
	}
	let s1 = seal[0];
	let deadline = tokio::time::Instant::now() + BOUND;
	for id in &seal {
		while slab.tier_pointer(*id).await.is_none() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"{id:?} never tiered"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
	let source = slab.tier_pointer(s1).await.unwrap();
	// The versions and their S1 locations, read before the floor moves (the
	// parents' intents are complete until then).
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let (k1, k3) = (row_of(1), row_of(3));
	let (v1, v3) = (
		owned_versions(&cluster, leader, k1).await,
		owned_versions(&cluster, leader, k3).await,
	);
	let pruned = slab.version_locations(TenantId::ZERO, k1, v1[0]).unwrap()[0];
	let survivor_before = slab.version_locations(TenantId::ZERO, k3, v3[0]).unwrap()[0];
	assert_eq!(
		(pruned.slab_id, survivor_before.slab_id),
		(s1, s1),
		"premise: both in S1"
	);
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	expect_everywhere(&cluster, &descriptor, &expected, "before the step").await;
	// ADR 0725: as the node's own committer is built, with its owner authority.
	let committer = crate::storage::RaftCommitter::new(cluster.node(leader).raft.clone())
		.with_state_machine(cluster.node(leader).state_machine.clone())
		.with_owner_authority(cluster.node(leader).data_server.owner_authority());
	let source_end = slab
		.tier_object_layout_for_test(&source)
		.await
		.unwrap()
		.logical_end();
	pause.store(false, Release);
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let rewritten = slab
			.tier_pointer(s1)
			.await
			.is_some_and(|pointer| pointer.l2_uri.contains("certified/"));
		if rewritten
			&& slab
				.version_locations(TenantId::ZERO, k1, v1[0])
				.unwrap()
				.is_empty()
		{
			break;
		}
		if tokio::time::Instant::now() >= deadline {
			let answer = server.certified_tier_step(&committer).await;
			panic!("S1 never rewritten; the step answers {answer:?}");
		}
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
	let survivor = slab.version_locations(TenantId::ZERO, k3, v3[0]).unwrap()[0];
	assert!(
		survivor.offset >= source_end,
		"row 3 at {survivor:?}, the source ended at {source_end}"
	);
	assert_ne!(survivor, pruned, "never the pruned row's location");
	expect_everywhere(&cluster, &descriptor, &expected, "after the certified step").await;
	let reason = |result: std::io::Result<Vec<u8>>| {
		let error = result.expect_err("the pruned location never reads another row");
		crate::storage::manager::stale_location(&error).map(|stale| stale.reason)
	};
	{
		use crate::storage::core::StorageEngine;
		assert_eq!(
			reason(slab.read(pruned).await),
			Some(crate::storage::manager::StaleLocationReason::Superseded),
			"in process"
		);
	}
	let rewritten = slab.tier_pointer(s1).await.unwrap();
	drop(slab);
	drop(server);
	// The leader's real restart: no in-memory layout history survives it.
	cluster.stop(leader, false).await;
	cluster.reopen(leader).await;
	until_settled(&cluster, &descriptor).await;
	assert_eq!(
		cluster.read_once_validated(leader, &descriptor).await,
		expected,
		"after the leader's restart"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the restart").await;
	let restarted = cluster.node(leader).slab.clone();
	restarted.wait_tier_authority_ready().await.unwrap();
	assert_eq!(restarted.tier_pointer(s1).await, Some(rewritten));
	assert_eq!(
		restarted
			.version_locations(TenantId::ZERO, k3, v3[0])
			.unwrap(),
		vec![survivor],
		"the survivor's location stands"
	);
	{
		use crate::storage::core::StorageEngine;
		assert_eq!(
			reason(restarted.read(pruned).await),
			Some(crate::storage::manager::StaleLocationReason::OutsideGeneration),
			"after the restart"
		);
	}
	assert!(!leader_l2.list("certified/").await.unwrap().is_empty());
	drop(restarted);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0706: closes `id` with the real `Node::shutdown`, as `Cluster::close`
/// does, while one of its certified steps holds its lifecycle permit at a test
/// hold already reached. The close must not finish while the step is held,
/// and Raft must still run and lead then; the step is released and the close
/// must finish. Returns whether the close recorded the directory clean, and
/// the held step's terminal return.
async fn close_while_held(
	cluster: &mut Cluster,
	id: u32,
	release: Arc<tokio::sync::Notify>,
) -> (
	bool,
	crate::network::data_server::child_writes::HeldStepOutcome,
) {
	let outcomes = cluster
		.node(id)
		.data_server
		.decision_peer_for_test()
		.held_outcomes
		.clone();
	let at = cluster
		.nodes
		.iter()
		.position(|node| node.local_node.id == id)
		.unwrap();
	let node = cluster.nodes.remove(at);
	cluster
		.network
		.unpublish(crate::schema::NodeId { id, _reserved: 0 });
	let raft = node.raft.clone();
	let clean_record = cluster.configs[&id].data_dir.join("CLEAN_SHUTDOWN");
	let mut closing = tokio::spawn(node.shutdown());
	if let Ok(closed) = tokio::time::timeout(std::time::Duration::from_secs(2), &mut closing).await
	{
		panic!(
			"the close finished while the step was held ({closed:?}); clean record: {}",
			clean_record.exists()
		);
	}
	assert!(
		outcomes.lock().unwrap().is_empty(),
		"the step is still held"
	);
	tokio::time::timeout(
		std::time::Duration::from_secs(10),
		raft.ensure_linearizable(),
	)
	.await
	.expect("Raft answers while the close waits")
	.expect("Raft still runs and leads while the close waits");
	release.notify_one();
	tokio::time::timeout(std::time::Duration::from_secs(30), closing)
		.await
		.expect("the close finished")
		.unwrap()
		.unwrap();
	let mut outcomes = std::mem::take(&mut *outcomes.lock().unwrap());
	assert_eq!(
		outcomes.len(),
		1,
		"exactly the held step returned: {outcomes:?}"
	);
	(clean_record.exists(), outcomes.pop().unwrap())
}

/// ADR 0706 (acceptance 1, the certified tier step), CONTROLLED CLOCK: every
/// member tiers to its own LocalFs object store. On the leader, S0 holds row 3
/// (nothing to reclaim), S1 an obsolete version of row 1 and row 4 (each its
/// own parent), S2 row 1's winner. After generation 1 the leader's LSM service
/// runs its first certified tier step, which visits S1 first (its cursor
/// starts at 0 and takes the ids above it first), held after it took its
/// lifecycle permit; the leader's close begins then. The close waits for the
/// step with Raft live, the released step publishes S1's certified rewrite
/// (exactly row 4), the close records the directory clean, and the restarted
/// leader is not in recovery, keeps the rewrite and reads exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_orderly_close_waits_for_a_held_certified_tier_step_and_records_clean() {
	use crate::network::data_server::child_writes::{HeldStepOutcome, PauseStage};
	use crate::storage::manager::certified_tier::CertifiedTierStep;
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor) = ready_with(Cluster::start_with(|id, cfg| {
		let root = crate::test_support::TestDir::new(&format!("held-tier-l2-{id}"));
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs {
			root: root.to_path_buf(),
		});
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		Some(root)
	}))
	.await;
	let server = cluster.node(leader).data_server.clone();
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let write = async |rows: &[(i64, &str)], what: &str| {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, rows.len(), uuid_v7()),
				named_rows(&descriptor, rows),
			)
			.await,
			what,
		);
	};
	write(&[(3, "x")], "S0").await;
	let s0 = slab.force_seal_active_for_test().await;
	write(&[(1, "a")], "S1's obsolete row").await;
	write(&[(4, "y")], "S1's survivor").await;
	let s1 = slab.force_seal_active_for_test().await;
	write(&[(1, "b")], "S2 (the winner)").await;
	let s2 = slab.force_seal_active_for_test().await;
	let expected = Expected::from([
		(1, "b".to_string()),
		(3, "x".to_string()),
		(4, "y".to_string()),
	]);
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let (k1, k3, k4) = (row_of(1), row_of(3), row_of(4));
	let (v1, v3, v4) = (
		owned_versions(&cluster, leader, k1).await,
		owned_versions(&cluster, leader, k3).await,
		owned_versions(&cluster, leader, k4).await,
	);
	assert_eq!(
		(v1.len(), v3.len(), v4.len()),
		(2, 1, 1),
		"every version owned"
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	for id in [s0, s1, s2] {
		while slab.tier_pointer(id).await.is_none() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"slab {id:?} never tiered"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	expect_everywhere(&cluster, &descriptor, &expected, "before the held step").await;
	let index = slab.persistent_index().unwrap();
	let debt: Vec<_> = index
		.debt_page(None, 100)
		.await
		.unwrap()
		.into_iter()
		.map(|(key, _)| (crate::schema::Identity::new(key.key_identity), key.hlc))
		.collect();
	assert_eq!(debt, vec![(k1, v1[0])], "premise: the leader's debt");
	let mut tiered: Vec<_> = slab
		.list_tiered_slabs()
		.await
		.into_iter()
		.map(|pointer| pointer.slab_id)
		.collect();
	tiered.sort();
	assert_eq!(
		(tiered, s0.get()),
		(vec![s0, s1, s2], 0),
		"premise: the leader's tiered slabs, S1 first after the cursor"
	);
	// The leader's next certified tier step holds after taking its permit.
	let (reached, release) = server.pause_coordinator_for_test(PauseStage::CertifiedTierStep);
	pause.store(false, Release);
	drop(index);
	drop(slab);
	drop(server);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the certified tier step holds its permit");
	let (clean, outcome) = close_while_held(&mut cluster, leader, release).await;
	let candidate = match outcome {
		HeldStepOutcome::Tier(Ok(CertifiedTierStep::Rewritten {
			slab, candidate, ..
		})) if slab == s1 => candidate,
		other => panic!("the held step's terminal return (S1 {s1:?}): {other:?}"),
	};
	assert!(clean, "the close recorded a clean shutdown");
	cluster.reopen(leader).await;
	until_settled(&cluster, &descriptor).await;
	assert!(
		!cluster.node(leader).recovered_after_unclean_shutdown(),
		"a clean close: no recovery"
	);
	assert_eq!(
		cluster.read_once_validated(leader, &descriptor).await,
		expected,
		"after the leader's restart"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the restart").await;
	let restarted = cluster.node(leader).slab.clone();
	restarted.wait_tier_authority_ready().await.unwrap();
	assert_eq!(
		restarted
			.tier_pointer(s1)
			.await
			.map(|pointer| pointer.l2_uri),
		Some(candidate),
		"the published rewrite stands"
	);
	let durable = restarted
		.persistent_index()
		.unwrap()
		.load_snapshot()
		.await
		.unwrap();
	let s1_rows: Vec<_> = durable
		.record_rows
		.iter()
		.filter(|row| row.location.slab_id == s1)
		.map(|row| (row.key_identity, row.location.hlc))
		.collect();
	assert_eq!(s1_rows, vec![(k4, v4[0])], "the exact survivor set");
	let copies = |key, hlc| {
		restarted
			.version_locations(TenantId::ZERO, key, hlc)
			.unwrap()
			.len()
	};
	assert_eq!(
		(
			copies(k1, v1[0]),
			copies(k1, v1[1]),
			copies(k3, v3[0]),
			copies(k4, v4[0])
		),
		(0, 1, 1, 1)
	);
	drop(durable);
	drop(restarted);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// How many indexed copies `slab` keeps of each version.
fn stored(
	slab: &Arc<SlabManager>,
	versions: &[(crate::schema::Identity, HybridTimestamp)],
) -> Vec<usize> {
	versions
		.iter()
		.map(|(key, hlc)| {
			slab.version_locations(TenantId::ZERO, *key, *hlc)
				.unwrap()
				.len()
		})
		.collect()
}

/// ADR 0706 (acceptance 1, the certified LSM step), CONTROLLED CLOCK: the
/// leader holds five L0 runs (the default picker's count) carrying row 1's
/// overwritten versions and row 2's deleted row; its local cleanup stays
/// paused. After generation 1, and once its oldest run is past the default
/// picker's age, the leader's LSM service runs its certified LSM step, held
/// after it took its lifecycle permit; the leader's close begins then. The
/// close waits with Raft live, the released step publishes a job eliding
/// exactly the obsolete versions, the close records the directory clean, and
/// the restarted leader is not in recovery, has no obsolete copy, keeps every
/// winner and reads exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_orderly_close_waits_for_a_held_certified_lsm_step_and_records_clean() {
	use crate::network::data_server::child_writes::{HeldStepOutcome, PauseStage};
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let peer = server.decision_peer_for_test();
	let (pause, pause_local) = (peer.pause_cleanup.clone(), peer.pause_local_cleanup.clone());
	pause.store(true, Release);
	pause_local.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut first_run = None;
	for value in ["a", "b", "c"] {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(1, value)]),
			)
			.await,
			"an overwrite",
		);
		slab.force_seal_active_for_test().await;
		first_run.get_or_insert_with(tokio::time::Instant::now);
	}
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(2, "a")]),
		)
		.await,
		"a row to delete",
	);
	slab.force_seal_active_for_test().await;
	let row_of = |id: i64| {
		server
			.row_identity_and_locality_for_test(
				TenantId::ZERO,
				&descriptor,
				&named_rows(&descriptor, &[(id, "x")]),
			)
			.0
	};
	let deleted = delete_through(&cluster, leader, delete_meta(&descriptor), vec![row_of(2)]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "{deleted:?}");
	slab.force_seal_active_for_test().await;
	let expected = Expected::from([(1, "c".to_string())]);
	expect_everywhere(&cluster, &descriptor, &expected, "before any generation").await;
	let (one, two) = (row_of(1), row_of(2));
	let ones = owned_versions(&cluster, leader, one).await;
	let twos = owned_versions(&cluster, leader, two).await;
	assert_eq!((ones.len(), twos.len()), (3, 2), "every version owned");
	let obsolete = [(one, ones[0]), (one, ones[1]), (two, twos[0])];
	let winners = [(one, ones[2]), (two, twos[1])];
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	// The default picker also wants its oldest L0 run five seconds old.
	tokio::time::sleep_until(first_run.unwrap() + std::time::Duration::from_secs(6)).await;
	assert_eq!(stored(&slab, &obsolete), [1, 1, 1], "premise: stored once");
	// The leader's next certified LSM step holds after taking its permit.
	let (reached, release) = server.pause_coordinator_for_test(PauseStage::CertifiedLsmStep);
	pause.store(false, Release);
	drop(slab);
	drop(server);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the certified LSM step holds its permit");
	let (clean, outcome) = close_while_held(&mut cluster, leader, release).await;
	let outcome = match outcome {
		HeldStepOutcome::Lsm(Ok(Some(outcome))) => outcome,
		other => panic!("the held step's terminal return: {other:?}"),
	};
	assert_eq!(
		outcome.elided_versions,
		obsolete.len() as u64,
		"exactly the obsolete versions: {outcome:?}"
	);
	assert!(clean, "the close recorded a clean shutdown");
	cluster.reopen(leader).await;
	until_settled(&cluster, &descriptor).await;
	assert!(
		!cluster.node(leader).recovered_after_unclean_shutdown(),
		"a clean close: no recovery"
	);
	assert_eq!(
		cluster.read_once_validated(leader, &descriptor).await,
		expected,
		"after the leader's restart"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "after the restart").await;
	let restarted = cluster.node(leader).slab.clone();
	assert_eq!(stored(&restarted, &obsolete), [0, 0, 0], "elided, durably");
	assert_eq!(stored(&restarted, &winners), [1, 1], "every winner stays");
	drop(restarted);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0706 (acceptance 2): a certified step held and never released, with
/// the lifecycle drain bound shortened: the close aborts the LSM service at
/// the bound and joins it, reports the interruption, never records the
/// directory clean, and the next start is unclean and validates before it
/// reads exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_close_past_the_drain_bound_aborts_a_held_step_and_records_unclean() {
	use crate::network::data_server::child_writes::PauseStage;
	let (mut cluster, leader, descriptor) = ready().await;
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(1, "a")]),
		)
		.await,
		"a row",
	);
	let expected = Expected::from([(1, "a".to_string())]);
	let server = cluster.node(leader).data_server.clone();
	let outcomes = server.decision_peer_for_test().held_outcomes.clone();
	let (reached, _never_released) =
		server.pause_coordinator_for_test(PauseStage::CertifiedLsmStep);
	drop(server);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the certified LSM step holds its permit");
	let at = cluster
		.nodes
		.iter()
		.position(|node| node.local_node.id == leader)
		.unwrap();
	let mut node = cluster.nodes.remove(at);
	cluster.network.unpublish(crate::schema::NodeId {
		id: leader,
		_reserved: 0,
	});
	let bound = std::time::Duration::from_secs(2);
	node.handles.set_lifecycle_drain_bound_for_test(bound);
	let started = tokio::time::Instant::now();
	let error = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a bounded close")
		.expect_err("an interrupted close is not orderly");
	assert!(started.elapsed() >= bound, "it waited for the bound");
	assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
	assert!(error.to_string().contains("LSM compaction"), "{error}");
	assert!(
		outcomes.lock().unwrap().is_empty(),
		"the interrupted step never reached a terminal return"
	);
	assert!(
		!cluster.configs[&leader]
			.data_dir
			.join("CLEAN_SHUTDOWN")
			.exists(),
		"an interrupted step's permit revoked the clean record"
	);
	cluster.reopen(leader).await;
	until_settled(&cluster, &descriptor).await;
	assert!(
		cluster.node(leader).recovered_after_unclean_shutdown(),
		"the next start is unclean"
	);
	assert_eq!(
		cluster.read_once_validated(leader, &descriptor).await,
		expected,
		"validated, then exact"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0707: how long a close that stops waiting for its maintenance
/// proposals may take, far under the 15 s lifecycle drain bound.
const PROMPT_CLOSE: std::time::Duration = std::time::Duration::from_secs(5);

type LogEntry = openraft::Entry<crate::consensus::WendelonTypeConfig>;

fn node_id(id: u32) -> crate::schema::NodeId {
	crate::schema::NodeId { id, _reserved: 0 }
}

/// `id`'s last log index and last applied index, from its Raft metrics.
fn log_and_applied(cluster: &Cluster, id: u32) -> (u64, u64) {
	let metrics = cluster.node(id).raft.metrics();
	let metrics = metrics.borrow();
	(
		metrics.last_log_index.unwrap_or(0),
		metrics.last_applied.map_or(0, |log_id| log_id.index),
	)
}

/// `id`'s Raft log entries in `range`, from its log store.
async fn log_entries(
	cluster: &Cluster,
	id: u32,
	range: std::ops::RangeInclusive<u64>,
) -> Vec<LogEntry> {
	use openraft::storage::RaftLogReader;
	let mut store = cluster.node(id).log_store.clone();
	store.try_get_log_entries(range).await.unwrap()
}

fn decision_of(entry: &LogEntry) -> Option<&crate::consensus::decisions::DecisionCommand> {
	match &entry.payload {
		openraft::EntryPayload::Normal(crate::consensus::WendelonRequest::Decision(command)) => {
			Some(command)
		}
		_ => None,
	}
}

/// The premise of a stopped wait: an entry in `id`'s log, past its applied
/// index, that `wanted` picks and that is still unapplied after a settling
/// pause, a proposal that cannot commit. Returns that entry.
async fn stuck_entry(
	cluster: &Cluster,
	id: u32,
	wanted: impl Fn(&LogEntry) -> bool,
	what: &str,
) -> LogEntry {
	let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
	loop {
		let (last, applied) = log_and_applied(cluster, id);
		let pending = if last > applied {
			log_entries(cluster, id, applied + 1..=last)
				.await
				.into_iter()
				.find(|entry| wanted(entry))
		} else {
			None
		};
		if let Some(entry) = pending {
			tokio::time::sleep(std::time::Duration::from_millis(500)).await;
			if log_and_applied(cluster, id).1 < entry.log_id.index {
				return entry;
			}
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"premise: {what} pending at {id}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}
}

/// Closes `id` and requires an orderly, prompt and clean close.
async fn close_promptly(cluster: &mut Cluster, id: u32, what: &str) {
	let began = tokio::time::Instant::now();
	let (result, clean) = cluster.close_with_result(id).await;
	let took = began.elapsed();
	eprintln!("ADR0707 close {what} node={id} took={took:?} result={result:?} clean={clean}");
	result.unwrap_or_else(|error| panic!("{what}: the close is orderly: {error}"));
	assert!(took < PROMPT_CLOSE, "{what}: the close took {took:?}");
	assert!(clean, "{what}: the close recorded the directory clean");
}

/// Waits until `id` applied at least `index`.
async fn until_applied(cluster: &Cluster, id: u32, index: u64) {
	let deadline = tokio::time::Instant::now() + BOUND;
	while log_and_applied(cluster, id).1 < index {
		assert!(
			tokio::time::Instant::now() < deadline,
			"node {id} never applied {index}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
}

/// Whether `entry` survived at its index in `id`'s log (the old entry applied)
/// or was replaced (truncated, and its command derived again if needed).
async fn survived(cluster: &Cluster, id: u32, entry: &LogEntry) -> bool {
	let index = entry.log_id.index;
	let found = log_entries(cluster, id, index..=index).await;
	found
		.first()
		.is_some_and(|kept| kept.log_id == entry.log_id)
}

/// `id`'s committed log up to the applied index it read with `read`, and that
/// reading: a contiguous log, so every effect is accounted for.
async fn committed_with<T>(
	cluster: &Cluster,
	id: u32,
	read: impl FnOnce(&crate::consensus::raft_storage::SmState) -> T,
) -> (Vec<LogEntry>, T) {
	let (applied, value) = cluster
		.node(id)
		.state_machine
		.read_state(move |state| {
			(
				state.last_applied_log_id.map_or(0, |log_id| log_id.index),
				read(state),
			)
		})
		.await;
	let entries = log_entries(cluster, id, 0..=applied).await;
	let first = entries.first().map_or(u64::MAX, |entry| entry.log_id.index);
	assert!(
		first <= 1
			&& entries.last().map(|entry| entry.log_id.index) == Some(applied)
			&& entries
				.windows(2)
				.all(|pair| pair[1].log_id.index == pair[0].log_id.index + 1),
		"node {id}'s committed log is complete up to {applied}"
	);
	(entries, value)
}

/// ADR 0707 (acceptance 1), the DC-203 control: a leader that lost its last
/// reachable voter (one follower closed, the other running but unpublished)
/// with a floor advance pending in its log. Its close stops waiting for that
/// proposal: orderly, prompt and clean (on main it took 15 s and returned
/// TimedOut). With quorum back, either Raft outcome stands, and every node's
/// floor is exactly the highest floor its committed log advanced to: no copy
/// had a second effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_leader_without_quorum_stops_waiting_for_its_maintenance_proposal_and_closes() {
	use crate::consensus::decisions::DecisionCommand;
	let (mut cluster, leader, descriptor) = ready().await;
	let mut followers = NODES.iter().copied().filter(|node| *node != leader);
	let (closed, unpublished) = (followers.next().unwrap(), followers.next().unwrap());
	assert!(
		cluster.close(closed).await,
		"the first follower closed clean"
	);
	// The second follower stays up but unreachable, as during its own close.
	cluster.network.unpublish(node_id(unpublished));
	let stuck = stuck_entry(
		&cluster,
		leader,
		|entry| {
			matches!(
				decision_of(entry),
				Some(DecisionCommand::AdvanceFloor { .. })
			)
		},
		"a floor advance",
	)
	.await;
	let Some(DecisionCommand::AdvanceFloor {
		floor_ms: stuck_floor,
		..
	}) = decision_of(&stuck).cloned()
	else {
		unreachable!()
	};
	eprintln!(
		"ADR0707 premise leader={leader} closed={closed} unpublished={unpublished} stuck={:?} applied={}",
		stuck.log_id,
		log_and_applied(&cluster, leader).1
	);
	close_promptly(&mut cluster, leader, "the leader without quorum").await;

	// Quorum back: the unreachable follower is published again, the old
	// leader and the closed follower reopen on their stores.
	let service = cluster.node(unpublished).service.clone();
	cluster
		.network
		.publish(node_id(unpublished), &address(unpublished), service);
	cluster.reopen(leader).await;
	cluster.reopen(closed).await;
	until_settled(&cluster, &descriptor).await;
	for id in NODES {
		until_applied(&cluster, id, stuck.log_id.index).await;
	}
	let kept = survived(&cluster, cluster.leader().await, &stuck).await;
	eprintln!(
		"ADR0707 outcome: the stopped floor advance {}",
		if kept {
			"survived and was applied"
		} else {
			"was truncated"
		}
	);
	for id in NODES {
		let (entries, floor) = committed_with(&cluster, id, |state| {
			state.decisions.as_ref().unwrap().floor_ms()
		})
		.await;
		assert_eq!(
			survived(&cluster, id, &stuck).await,
			kept,
			"node {id} agrees on the outcome"
		);
		let advances: Vec<u64> = entries
			.iter()
			.filter_map(|entry| match decision_of(entry) {
				Some(DecisionCommand::AdvanceFloor { floor_ms, .. }) => Some(*floor_ms),
				_ => None,
			})
			.collect();
		assert_eq!(
			Some(floor),
			advances.iter().copied().max(),
			"node {id}: the floor is exactly the highest committed advance"
		);
		if kept {
			assert!(
				floor >= stuck_floor,
				"node {id}: the kept advance took effect"
			);
		}
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0707 (acceptance 2): the leader's certification step is held after it
/// durably staged its candidate and signed its report; quorum goes, the held
/// report is proposed and cannot commit, and the leader closes. The step's
/// Report wait is its one cut point: the step returns normally, its permit
/// completes, and the close is orderly, prompt and clean. With quorum back the
/// generation commits exactly the staged candidate, and every copy of the
/// leader's report in the committed log is that same report.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_report_stopped_at_its_wait_without_quorum_completes_its_permit() {
	use crate::consensus::decisions::DecisionCommand;
	use crate::network::data_server::child_writes::PauseStage;
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let (reached, release) = cluster
		.node(leader)
		.data_server
		.pause_coordinator_for_test(PauseStage::CertificationBeforeReport);
	jump_floor(&cluster, now + 60_000).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the leader's report is held after durable staging");
	let staged = {
		let index = cluster.node(leader).slab.persistent_index().unwrap();
		let meta = index.base_meta().await.unwrap();
		meta.staged
			.expect("the candidate is staged before its report")
	};
	let mut followers = NODES.iter().copied().filter(|node| *node != leader);
	let (closed, unpublished) = (followers.next().unwrap(), followers.next().unwrap());
	assert!(
		cluster.close(closed).await,
		"the first follower closed clean"
	);
	cluster.network.unpublish(node_id(unpublished));
	release.notify_one();
	let reported_by_leader = move |entry: &LogEntry| {
		matches!(
			decision_of(entry),
			Some(DecisionCommand::Report { report }) if report.body.reporter.node == leader
		)
	};
	let stuck = stuck_entry(&cluster, leader, reported_by_leader, "the leader's report").await;
	eprintln!(
		"ADR0707 premise leader={leader} gid={} stuck={:?} applied={}",
		staged.gid,
		stuck.log_id,
		log_and_applied(&cluster, leader).1
	);
	close_promptly(&mut cluster, leader, "the leader held at its report").await;

	let service = cluster.node(unpublished).service.clone();
	cluster
		.network
		.publish(node_id(unpublished), &address(unpublished), service);
	cluster.reopen(leader).await;
	cluster.reopen(closed).await;
	assert!(
		!cluster.node(leader).recovered_after_unclean_shutdown(),
		"the permit completed: the next start is clean"
	);
	until_settled(&cluster, &descriptor).await;
	until_active(&cluster, staged.gid).await;
	let generations = generations_at_leader(&cluster).await;
	let current = generations.current.clone().unwrap();
	assert_eq!(
		(
			current.gid,
			current.target_digest,
			current.predecessor_gid,
			&current.chunks
		),
		(
			staged.gid,
			staged.target_digest,
			staged.predecessor_gid,
			&staged.chunks
		),
		"the generation committed exactly the staged candidate"
	);
	let new_leader = cluster.leader().await;
	let kept = survived(&cluster, new_leader, &stuck).await;
	let (entries, ()) = committed_with(&cluster, new_leader, |_| ()).await;
	let copies: Vec<_> = entries
		.iter()
		.filter(|entry| reported_by_leader(entry))
		.filter_map(|entry| match decision_of(entry) {
			Some(DecisionCommand::Report { report }) if report.body.gid == staged.gid => {
				Some((entry.log_id, report.body.clone()))
			}
			_ => None,
		})
		.collect();
	eprintln!(
		"ADR0707 outcome: the stopped report {}; committed copies {:?}",
		if kept {
			"survived and was applied"
		} else {
			"was truncated"
		},
		copies.iter().map(|(log_id, _)| *log_id).collect::<Vec<_>>()
	);
	assert!(!copies.is_empty(), "the leader's report is committed");
	for (log_id, body) in &copies {
		assert_eq!(
			(body.target_digest, &body.chunks),
			(staged.target_digest, &staged.chunks),
			"every committed copy ({log_id}) is the staged report"
		);
	}
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert_eq!(
			index.base_meta().await.unwrap().active_chunks,
			staged.chunks,
			"node {node} activated the staged candidate"
		);
		assert_eq!(
			cluster.read_once_validated(node, &descriptor).await,
			expected,
			"at {node}"
		);
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0707 (acceptance 3): a follower's forwarded report waits on the
/// leader's answer, its handling held at the leader. The follower's close
/// stops that wait: orderly, prompt and clean. Released afterwards, the
/// report (or one derived again after the reopen) commits the generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_followers_forward_waiting_on_the_leader_is_stopped_by_its_close() {
	use crate::network::decision_proposals::{ForwardedDecision, ForwardedHold};
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=2 {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
		expected.insert(id, "a".into());
	}
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	let (reached, release, held) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(std::sync::Mutex::new(None)),
	);
	cluster
		.node(leader)
		.data_server
		.decision_peer_for_test()
		.proposals
		.as_ref()
		.unwrap()
		.hold_forwarded_for_test(ForwardedHold {
			sender: follower,
			matches: |request| matches!(request, ForwardedDecision::Report { .. }),
			reached: reached.clone(),
			release: release.clone(),
			held: held.clone(),
		});
	jump_floor(&cluster, now + 60_000).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the follower's report reached the leader's handler");
	assert!(
		matches!(
			held.lock().unwrap().as_ref(),
			Some(ForwardedDecision::Report { report }) if report.body.reporter.node == follower
		),
		"the held request is the follower's report"
	);
	close_promptly(
		&mut cluster,
		follower,
		"the follower waiting on its forward",
	)
	.await;
	release.notify_one();
	cluster.reopen(follower).await;
	until_settled(&cluster, &descriptor).await;
	until_active(&cluster, 1).await;
	for node in NODES {
		assert_eq!(
			cluster.read_once_validated(node, &descriptor).await,
			expected,
			"at {node}"
		);
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0707 (acceptance 4): the maintenance stop is decision maintenance's
/// alone. With it set on every node, ordinary proposals still complete: a
/// write coordinated at the leader (its data server's local proposal) and one
/// coordinated at a follower (forwarded to the leader's control server).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_maintenance_stop_never_cancels_an_ordinary_proposal() {
	let (cluster, leader, descriptor) = ready().await;
	for id in NODES {
		cluster.node(id).data_server.stop_decision_maintenance();
		assert!(
			*cluster
				.node(id)
				.data_server
				.decision_maintenance_stop()
				.borrow()
		);
	}
	let mut on_leader = None;
	for id in 1..200 {
		let primary = cluster
			.node(leader)
			.data_server
			.primary_for_test(TenantId::ZERO, &descriptor, &rows(&descriptor, &[id]))
			.await
			.expect("every position is leased");
		if primary == leader {
			on_leader = Some(id);
			break;
		}
	}
	let on_leader = on_leader.expect("a row whose primary is the leader");
	let (off_leader, primary) = row_off_the_leader(&cluster, leader, &descriptor, 200).await;
	for (id, entry, what) in [
		(on_leader, leader, "coordinated at the leader"),
		(off_leader, primary, "coordinated at a follower"),
	] {
		assert_committed(
			&write_through(
				&cluster,
				entry,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(id, "a")]),
			)
			.await,
			what,
		);
	}
	let expected: Expected = [(on_leader, "a".to_string()), (off_leader, "a".to_string())].into();
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"with the maintenance stop set",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// DC-206: a table write coordinated at a follower forwards its Reserve to
/// the leader, whose handling is held once the request has arrived. The
/// follower must still answer the client within [`UNKNOWN_ANSWER_BOUND`] with
/// a typed unknown outcome (WRITE_STALLED, commit knowledge INDETERMINATE),
/// never a refusal or a known non-commit. The hold is released, the original
/// attempt allowed to end, the same identity retried to its recorded outcome
/// and every node closed before anything is asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_follower_coordinated_write_answers_unknown_while_the_leader_holds_its_reserve() {
	use crate::network::decision_proposals::{ForwardedDecision, ForwardedHold};
	/// How long the client waits for the follower's answer once its Reserve
	/// is shown held at the leader: above the remote round trip's proposed
	/// bound, with margin for scheduling under suite load.
	const UNKNOWN_ANSWER_BOUND: std::time::Duration = std::time::Duration::from_secs(10);
	/// Releases the leader's held handling when dropped, so no failure path
	/// leaves the handler parked.
	struct ReleaseOnDrop(Arc<tokio::sync::Notify>);
	impl Drop for ReleaseOnDrop {
		fn drop(&mut self) {
			self.0.notify_one();
		}
	}

	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = rows(&descriptor, &[id]);
	let (reached, release, held) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(std::sync::Mutex::new(None)),
	);
	cluster
		.node(leader)
		.data_server
		.decision_peer_for_test()
		.proposals
		.as_ref()
		.unwrap()
		.hold_forwarded_for_test(ForwardedHold {
			sender: primary,
			matches: |request| matches!(request, ForwardedDecision::Reserve { .. }),
			reached: reached.clone(),
			release: release.clone(),
			held: held.clone(),
		});
	let release = ReleaseOnDrop(release);
	let server = cluster.node(primary).data_server.clone();
	let mut write = tokio::spawn({
		let batch = batch.clone();
		async move { server.table_write_for_test(meta, batch).await }
	});
	tokio::select! {
		_ = reached.notified() => {}
		early = &mut write => panic!("the write answered before its Reserve reached the leader: {:?}",
			early.map(|result| result.map(|(response, _)| response))),
		_ = tokio::time::sleep(BOUND) => panic!("the follower's Reserve never reached the leader's handler"),
	}
	assert!(
		matches!(
			held.lock().unwrap().as_ref(),
			Some(ForwardedDecision::Reserve { claim }) if claim.body.key == key
		),
		"the held request is this write's Reserve"
	);

	let answer = tokio::time::timeout(UNKNOWN_ANSWER_BOUND, &mut write).await;
	drop(release);
	// Settle on every path before asserting.
	let original = match &answer {
		Ok(_) => None,
		Err(_) => Some(
			tokio::time::timeout(BOUND, &mut write)
				.await
				.map(|joined| joined.map(|result| result.map(|(response, _)| response))),
		),
	};
	let retried = write_through(&cluster, primary, meta, batch).await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let Ok(answer) = answer else {
		panic!(
			"no answer within {UNKNOWN_ANSWER_BOUND:?} while the leader held the follower's \
			 forwarded Reserve; after the release the original attempt gave {original:?}"
		);
	};
	let (response, _) = answer.unwrap().unwrap();
	assert_eq!(
		response.write.status,
		WriteStatus::WRITE_STALLED,
		"{response:?}"
	);
	assert_eq!(
		response.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE,
		"{response:?}"
	);
	assert_committed(&retried, "the same identity, retried after the release");
}

/// DC-206: how long past the remote decision RPC deadline a follower's typed
/// unknown answer may take, for scheduling under suite load.
const UNKNOWN_ANSWER_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Every applied parent record under `key`, at the leader.
async fn records_of(
	cluster: &Cluster,
	key: ParentKey,
) -> Vec<crate::consensus::decisions::ParentRecord> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.map(|decisions| {
					decisions
						.bodies()
						.values()
						.filter(|body| body.claim.key == key)
						.cloned()
						.collect()
				})
				.unwrap_or_default()
		})
		.await
}

/// Which forwarded proposal of a follower-coordinated write the leader holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeldForward {
	Reserve,
	Finalize,
}

/// DC-206 (acceptance 2): the leader holds one forwarded proposal of a write
/// coordinated at a follower. The first attempt answers WRITE_STALLED with
/// INDETERMINATE within the deadline and its margin. No lower bound: the
/// timer starts after the reached handshake, while the RPC's own deadline
/// started before it connected. Released, the held
/// proposal commits late; the same original identity, retried, reaches that
/// one record: one parent, one committed write at the recorded decision, and
/// exactly the written row at every node.
async fn a_held_forward_answers_unknown_and_its_retry_reaches_the_one_record(stage: HeldForward) {
	use crate::network::decision_proposals::{
		DECISION_RPC_DEADLINE, ForwardedDecision, ForwardedHold,
	};
	/// Releases the leader's held handling when dropped.
	struct ReleaseOnDrop(Arc<tokio::sync::Notify>);
	impl Drop for ReleaseOnDrop {
		fn drop(&mut self) {
			self.0.notify_one();
		}
	}

	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &[(id, "a")]);
	let (reached, release, held) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(std::sync::Mutex::new(None)),
	);
	let matches: fn(&ForwardedDecision) -> bool = match stage {
		HeldForward::Reserve => |request| matches!(request, ForwardedDecision::Reserve { .. }),
		HeldForward::Finalize => |request| matches!(request, ForwardedDecision::Finalize { .. }),
	};
	cluster
		.node(leader)
		.data_server
		.decision_peer_for_test()
		.proposals
		.as_ref()
		.unwrap()
		.hold_forwarded_for_test(ForwardedHold {
			sender: primary,
			matches,
			reached: reached.clone(),
			release: release.clone(),
			held: held.clone(),
		});
	let release = ReleaseOnDrop(release);
	let server = cluster.node(primary).data_server.clone();
	let mut write = tokio::spawn({
		let batch = batch.clone();
		async move { server.table_write_for_test(meta, batch).await }
	});
	tokio::select! {
		_ = reached.notified() => {}
		early = &mut write => panic!("{stage:?}: the write answered before its forward reached the leader: {:?}",
			early.map(|result| result.map(|(response, _)| response))),
		_ = tokio::time::sleep(BOUND) => panic!("{stage:?}: the forward never reached the leader's handler"),
	}
	let held = held.lock().unwrap().clone();
	let held_claim = match (&held, stage) {
		(Some(ForwardedDecision::Reserve { claim }), HeldForward::Reserve)
			if claim.body.key == key =>
		{
			Some(claim.body.clone())
		}
		(Some(ForwardedDecision::Finalize { key: held_key, .. }), HeldForward::Finalize)
			if *held_key == key =>
		{
			None
		}
		other => panic!("{stage:?}: the held request is not this write's: {other:?}"),
	};

	let started = tokio::time::Instant::now();
	let first =
		tokio::time::timeout(DECISION_RPC_DEADLINE + UNKNOWN_ANSWER_MARGIN, &mut write).await;
	let first_after = started.elapsed();
	drop(release);
	// Settle before asserting: the original attempt ends, the held proposal
	// commits late, and the same identity is retried to its record.
	let original = match &first {
		Ok(_) => None,
		Err(_) => Some(
			tokio::time::timeout(BOUND, &mut write)
				.await
				.map(|joined| joined.map(|result| result.map(|(response, _)| response))),
		),
	};
	let late_deadline = tokio::time::Instant::now() + BOUND;
	let late = loop {
		let records = records_of(&cluster, key).await;
		let landed = records.first().is_some_and(|record| match stage {
			HeldForward::Reserve => true,
			HeldForward::Finalize => matches!(record.status, ParentStatus::Accepted { .. }),
		});
		if landed || tokio::time::Instant::now() >= late_deadline {
			break records;
		}
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	let retried = write_through(&cluster, primary, meta, batch).await;
	let records = records_of(&cluster, key).await;
	let mut reads = Vec::new();
	for node in NODES {
		reads.push((node, read_at(&cluster, node, &descriptor).await));
	}
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let Ok(first) = first else {
		panic!(
			"{stage:?}: no answer within {:?} while the leader held the forward; after the \
			 release the original attempt gave {original:?}",
			DECISION_RPC_DEADLINE + UNKNOWN_ANSWER_MARGIN
		);
	};
	let (first, _) = first.unwrap().unwrap();
	assert_eq!(
		(first.write.status, first.write.mutation_commit_knowledge()),
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"{stage:?}: the first attempt, after {first_after:?}: {first:?}"
	);
	match stage {
		HeldForward::Reserve => assert_eq!(
			late.first().map(|record| &record.claim),
			held_claim.as_ref(),
			"{stage:?}: the held Reserve committed late as the recorded claim"
		),
		HeldForward::Finalize => assert!(
			late.first()
				.is_some_and(|record| matches!(record.status, ParentStatus::Accepted { .. })),
			"{stage:?}: the held Finalize committed late: {late:?}"
		),
	}
	assert_committed(&retried, &format!("{stage:?}: the same identity, retried"));
	assert_eq!(records.len(), 1, "{stage:?}: one parent: {records:?}");
	assert_eq!(
		records[0].status,
		ParentStatus::Accepted {
			dec: retried.write.log_index
		},
		"{stage:?}: the retry answered the one recorded decision"
	);
	let expected: Expected = [(id, "a".to_string())].into();
	for (node, answer) in reads {
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::OK),
			"{stage:?}: at {node}: {answer:?}"
		);
		assert_eq!(answered(&answer), expected, "{stage:?}: at {node}");
	}
}

/// DC-206 (acceptance 2): the held forwarded Reserve.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reserve_held_past_the_deadline_is_unknown_and_its_retry_reaches_the_one_record() {
	a_held_forward_answers_unknown_and_its_retry_reaches_the_one_record(HeldForward::Reserve).await;
}

/// DC-206 (acceptance 2): the held forwarded Finalize.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_finalize_held_past_the_deadline_is_unknown_and_its_retry_reaches_the_one_record() {
	a_held_forward_answers_unknown_and_its_retry_reaches_the_one_record(HeldForward::Finalize)
		.await;
}

/// DC-206 (acceptance 2, the normal path): a write coordinated at a follower
/// whose forwards the leader answers in time commits on its first attempt, at
/// its one recorded decision, and reads back exactly everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_follower_coordinated_write_answered_in_time_commits_on_its_first_attempt() {
	let (cluster, leader, descriptor) = ready().await;
	let (id, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let (response, _) = cluster
		.node(primary)
		.data_server
		.table_write_for_test(meta, named_rows(&descriptor, &[(id, "a")]))
		.await
		.unwrap();
	assert_committed(&response, "the first attempt");
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert_eq!(
		records[0].status,
		ParentStatus::Accepted {
			dec: response.write.log_index
		}
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(id, "a".to_string())].into(),
		"answered in time",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// DC-206 (acceptance 3): the remote decision RPC's one deadline covers the
/// whole transport sequence. With the follower's own maintenance stopped, so
/// only this control's RPCs leave it:
/// - a leader that accepts the follower's streams and reads each request but
///   never answers ends a forwarded lookup and a cut `Indeterminate`, each
///   no earlier than the deadline and within its margin; once the leader
///   answers again the same cached connection (no new dial) serves both;
/// - a dial held in its connect stage ends a cut the same way, and the next
///   cut dials again and is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_silent_leader_ends_a_forwarded_lookup_and_cut_unknown_within_the_deadline() {
	use crate::network::decision_proposals::{
		CutResponse, DECISION_RPC_DEADLINE, LookupRequest, LookupResponse,
	};
	use crate::simulator::network::{
		InjectedSimulatedTransportFault, SimulatedTransportEventKind,
		SimulatedTransportFaultAction, SimulatedTransportFaultOp,
	};
	use std::sync::atomic::{AtomicUsize, Ordering};
	/// Accepts every stream and reads its request, but answers none until
	/// released; then each handler ends without an answer. Counts the lookups
	/// and cuts it read from `from`.
	struct SilentPeer {
		from: u32,
		lookups: AtomicUsize,
		cuts: AtomicUsize,
		released: tokio::sync::watch::Receiver<bool>,
	}
	#[async_trait::async_trait]
	impl crate::network::server::HrftService for SilentPeer {
		async fn handle_frame(
			&self,
			_context: crate::network::server::RequestContext,
			_frame: crate::network::core::HrftFrame,
		) -> std::io::Result<Option<crate::network::core::HrftFrame>> {
			let _ = self.released.clone().wait_for(|released| *released).await;
			Err(std::io::Error::other("a silent peer answers nothing"))
		}

		async fn handle_stream(
			&self,
			mut stream: Box<dyn crate::network::transport::TransportStream>,
			pool: Arc<crate::network::pool::BufPool>,
			context: crate::network::server::RequestContext,
		) -> std::io::Result<()> {
			let frame = crate::network::core::read_frame(&mut *stream, &pool).await?;
			if context.remote.map(|node| node.id) == Some(self.from) {
				let msg_type = frame.header.msg_type;
				if msg_type == crate::schema::MsgType::DECISION_LOOKUP_REQ.0 {
					self.lookups.fetch_add(1, Ordering::SeqCst);
				} else if msg_type == crate::schema::MsgType::DECISION_CUT_REQ.0 {
					self.cuts.fetch_add(1, Ordering::SeqCst);
				}
			}
			self.handle_frame(context, frame).await.map(|_| ())
		}
	}
	/// One RPC, timed, under an outer bound so a missing deadline fails here.
	async fn timed<T>(
		rpc: impl std::future::Future<Output = T>,
	) -> (Option<T>, std::time::Duration) {
		let started = tokio::time::Instant::now();
		let outcome = tokio::time::timeout(DECISION_RPC_DEADLINE + UNKNOWN_ANSWER_MARGIN, rpc)
			.await
			.ok();
		(outcome, started.elapsed())
	}

	let (cluster, leader, descriptor) = ready().await;
	let follower = *NODES.iter().find(|node| **node != leader).unwrap();
	cluster
		.node(follower)
		.data_server
		.stop_decision_maintenance();
	let proposals = cluster
		.node(follower)
		.data_server
		.decision_proposals_for_test();
	let lookup = || {
		proposals.lookup(LookupRequest {
			key: ParentKey {
				tenant: TenantId::ZERO,
				namespace: ParentNamespace::Table(descriptor.table_object_id),
				client_uuid: uuid_v7(),
				sequence_no: 1,
			},
			request_digest: [7; 32],
			sent_ms: proposals.now_ms(),
		})
	};
	let leader_id = node_id(leader);
	let leader_service = cluster.node(leader).service.clone();
	// The follower's connection to the leader is cached and answered.
	assert!(
		matches!(proposals.cut().await, CutResponse::Cut(_)),
		"answered before"
	);
	cluster.network.clear_events();

	let (release, released) = tokio::sync::watch::channel(false);
	let silent = Arc::new(SilentPeer {
		from: follower,
		lookups: AtomicUsize::new(0),
		cuts: AtomicUsize::new(0),
		released,
	});
	cluster
		.network
		.publish(leader_id, &address(leader), silent.clone());
	let (silent_lookup, lookup_after) = timed(lookup()).await;
	let (silent_cut, cut_after) = timed(proposals.cut()).await;
	let (lookups_read, cuts_read) = (
		silent.lookups.load(Ordering::SeqCst),
		silent.cuts.load(Ordering::SeqCst),
	);
	cluster
		.network
		.publish(leader_id, &address(leader), leader_service.clone());
	release.send_replace(true);
	let cut_again = timed(proposals.cut()).await.0;
	let lookup_again = timed(lookup()).await.0;
	let dialed_while_silent = cluster.network.events().iter().any(|event| {
		event.source == Some(node_id(follower))
			&& event.target == Some(leader_id)
			&& event.kind == SimulatedTransportEventKind::Connect
	});

	// The connect stage: the follower's next dial to the leader is held.
	cluster.network.inject(
		InjectedSimulatedTransportFault::on(
			SimulatedTransportFaultOp::Connect,
			SimulatedTransportFaultAction::Delay(std::time::Duration::from_secs(3600)),
		)
		.from(node_id(follower))
		.to(leader_id),
	);
	cluster.network.unpublish(leader_id);
	let (held_dial_cut, dial_after) = timed(proposals.cut()).await;
	cluster
		.network
		.publish(leader_id, &address(leader), leader_service);
	let dial_was_held = cluster.network.events().iter().any(|event| {
		event.source == Some(node_id(follower))
			&& event.target == Some(leader_id)
			&& matches!(
				event.kind,
				SimulatedTransportEventKind::Fault {
					op: SimulatedTransportFaultOp::Connect,
					..
				}
			)
	});
	let redialed_cut = timed(proposals.cut()).await.0;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let bound = DECISION_RPC_DEADLINE + UNKNOWN_ANSWER_MARGIN;
	assert_eq!(lookups_read, 1, "the silent leader read the lookup");
	assert_eq!(cuts_read, 1, "the silent leader read the cut");
	assert!(
		matches!(silent_lookup, Some(LookupResponse::Indeterminate(_))),
		"the lookup to a silent leader, after {lookup_after:?} (bound {bound:?}): {silent_lookup:?}"
	);
	assert!(
		lookup_after >= DECISION_RPC_DEADLINE,
		"the lookup ended at {lookup_after:?}"
	);
	assert!(
		matches!(silent_cut, Some(CutResponse::Indeterminate(_))),
		"the cut to a silent leader, after {cut_after:?} (bound {bound:?}): {silent_cut:?}"
	);
	assert!(
		cut_after >= DECISION_RPC_DEADLINE,
		"the cut ended at {cut_after:?}"
	);
	assert!(
		matches!(cut_again, Some(CutResponse::Cut(_))),
		"a cut once the leader answers again: {cut_again:?}"
	);
	assert!(
		matches!(lookup_again, Some(LookupResponse::Found(_))),
		"a lookup once the leader answers again: {lookup_again:?}"
	);
	assert!(
		!dialed_while_silent,
		"the timed-out RPCs left the cached connection in use: no new dial"
	);
	assert!(
		dial_was_held,
		"the follower's dial reached its held connect"
	);
	assert!(
		matches!(held_dial_cut, Some(CutResponse::Indeterminate(_))),
		"the cut whose dial is held, after {dial_after:?} (bound {bound:?}): {held_dial_cut:?}"
	);
	assert!(
		dial_after >= DECISION_RPC_DEADLINE,
		"the held dial's cut ended at {dial_after:?}"
	);
	assert!(
		matches!(redialed_cut, Some(CutResponse::Cut(_))),
		"the next cut dials again and is answered: {redialed_cut:?}"
	);
}

// DC-207: a child write RPC's deadline.

/// Scheduling margin over a child RPC budget.
const CHILD_ANSWER_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);
/// How long after a held child write is shown at its holder the coordinator's
/// answer may take. The held send started before that, so its budget ends
/// sooner; nothing asserts that a whole budget elapses after arrival.
const CHILD_WRITE_ANSWER_BOUND: std::time::Duration =
	crate::network::data_server::child_writes::CHILD_WRITE_DEADLINE
		.saturating_add(CHILD_ANSWER_MARGIN);
/// The same for a held holder-to-replica replicate.
const CHILD_REPLICATE_ANSWER_BOUND: std::time::Duration =
	crate::network::data_server::child_writes::CHILD_REPLICATE_DEADLINE
		.saturating_add(CHILD_ANSWER_MARGIN);

/// One child frame's handling held at a peer (see `ChildHold`); dropping it
/// releases the handler, so no failure path leaves it parked.
struct HeldChild {
	reached: Arc<tokio::sync::Notify>,
	release: Arc<tokio::sync::Notify>,
	held: Arc<std::sync::Mutex<Option<crate::network::data_server::child_writes::ChildWriteMeta>>>,
}

impl HeldChild {
	fn meta(&self) -> Option<crate::network::data_server::child_writes::ChildWriteMeta> {
		self.held.lock().unwrap().clone()
	}
}

impl Drop for HeldChild {
	fn drop(&mut self) {
		self.release.notify_one();
	}
}

/// Holds `at`'s handling of the next child frame of `role` from `sender`.
fn hold_child(
	cluster: &Cluster,
	at: u32,
	sender: u32,
	role: crate::network::data_server::child_writes::ChildRole,
) -> HeldChild {
	let held = HeldChild {
		reached: Arc::new(tokio::sync::Notify::new()),
		release: Arc::new(tokio::sync::Notify::new()),
		held: Arc::new(std::sync::Mutex::new(None)),
	};
	cluster
		.node(at)
		.data_server
		.decision_peer_for_test()
		.hold_child_for_test(crate::network::data_server::child_writes::ChildHold {
			sender,
			role,
			reached: held.reached.clone(),
			release: held.release.clone(),
			held: held.held.clone(),
		});
	held
}

type SpawnedWrite = tokio::task::JoinHandle<
	std::io::Result<(
		crate::schema::TableWriteResponseMeta,
		Option<crate::schema::FinalizationMarker>,
	)>,
>;

/// One attempt of a table write at `node`, spawned, never retried.
fn spawn_attempt(
	cluster: &Cluster,
	node: u32,
	meta: TableWriteRequestMeta,
	batch: RecordBatch,
) -> SpawnedWrite {
	let server = cluster.node(node).data_server.clone();
	tokio::spawn(async move { server.table_write_for_test(meta, batch).await })
}

/// Waits until `hold` is reached; a write that answers first, or a hold never
/// reached, fails here.
async fn until_child_reached(what: &str, hold: &HeldChild, write: &mut SpawnedWrite) {
	tokio::select! {
		_ = hold.reached.notified() => {}
		early = &mut *write => panic!("{what}: the write answered before its child frame reached the held peer: {:?}",
			early.map(|result| result.map(|(response, _)| response))),
		_ = tokio::time::sleep(BOUND) => panic!("{what}: the child frame never reached the held peer"),
	}
}

/// The answer of `write` within `bound`, as text for a failure message.
fn first_answer_text(
	answer: &Result<
		Result<
			std::io::Result<(
				crate::schema::TableWriteResponseMeta,
				Option<crate::schema::FinalizationMarker>,
			)>,
			tokio::task::JoinError,
		>,
		tokio::time::error::Elapsed,
	>,
) -> String {
	match answer {
		Ok(Ok(Ok((response, _)))) => format!("{response:?}"),
		other => format!("{other:?}"),
	}
}

/// Ends a timed-out attempt after its hold was released, for the record.
async fn original_after_release(
	answer: &Result<impl Sized, tokio::time::error::Elapsed>,
	write: &mut SpawnedWrite,
) -> Option<String> {
	if answer.is_ok() {
		return None;
	}
	Some(match tokio::time::timeout(BOUND, write).await {
		Ok(Ok(Ok((response, _)))) => format!("{response:?}"),
		other => format!("{other:?}"),
	})
}

/// Two rows whose first-row primary (the coordinator) and second-row primary
/// (a remote holder) differ, from the leader's applied lineages.
async fn rows_with_a_remote_holder(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
) -> (i64, i64, u32, u32) {
	let primary = |id: i64| async move {
		cluster
			.node(leader)
			.data_server
			.primary_for_test(TenantId::ZERO, descriptor, &rows(descriptor, &[id]))
			.await
			.expect("every position is leased")
	};
	let first = 1;
	let coordinator = primary(first).await;
	for second in first + 1..first + 200 {
		let holder = primary(second).await;
		if holder != coordinator {
			return (first, second, coordinator, holder);
		}
	}
	panic!("no row's primary differs from the coordinator's");
}

/// Every node's answer to a full table query.
async fn reads_everywhere(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
) -> Vec<(u32, crate::network::data_server::reader::TestQueryAnswer)> {
	let mut reads = Vec::new();
	for node in NODES {
		reads.push((node, read_at(cluster, node, descriptor).await));
	}
	reads
}

fn assert_reads(
	what: &str,
	reads: Vec<(u32, crate::network::data_server::reader::TestQueryAnswer)>,
	expected: &Expected,
) {
	for (node, answer) in reads {
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::OK),
			"{what}: at {node}: {answer:?}"
		);
		assert_eq!(&answered(&answer), expected, "{what}: at {node}");
	}
}

/// DC-207 (acceptance 1): a write coordinated at one node has a child whose
/// holder is another. That holder's handling of the child write is held once
/// it arrived, so it never answers. The coordinator still answers within
/// [`CHILD_WRITE_ANSWER_BOUND`] with a typed unknown outcome (WRITE_STALLED,
/// INDETERMINATE: Finalize without that child's acknowledgements is pending
/// evidence), never a known non-commit, and the parent stays Reserved.
/// Released, the same parent retried reaches its one recorded outcome, with
/// exactly its rows at every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_remote_holder_answers_unknown_and_its_retry_reaches_the_one_record() {
	use crate::network::data_server::child_writes::ChildRole;
	let (cluster, leader, descriptor) = ready().await;
	let (first_row, second_row, coordinator, holder) =
		rows_with_a_remote_holder(&cluster, leader, &descriptor).await;
	let meta = write_meta(&descriptor, 2, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &[(first_row, "a"), (second_row, "b")]);
	let hold = hold_child(&cluster, holder, coordinator, ChildRole::Holder);
	let mut write = spawn_attempt(&cluster, coordinator, meta, batch.clone());
	until_child_reached("held holder", &hold, &mut write).await;
	let held = hold.meta();

	let first = tokio::time::timeout(CHILD_WRITE_ANSWER_BOUND, &mut write).await;
	let at_answer = records_of(&cluster, key).await;
	drop(hold);
	let original = original_after_release(&first, &mut write).await;
	let retried = write_through(&cluster, coordinator, meta, batch).await;
	let records = records_of(&cluster, key).await;
	let reads = reads_everywhere(&cluster, &descriptor).await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let first_text = first_answer_text(&first);
	let Ok(Ok(Ok((first, _)))) = first else {
		panic!(
			"no answer within {CHILD_WRITE_ANSWER_BOUND:?} while the remote holder held the \
			 child write: {first_text}; after the release the original attempt gave {original:?}"
		);
	};
	assert_ne!(
		first.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"never a known non-commit: {first:?}"
	);
	assert_eq!(
		(first.write.status, first.write.mutation_commit_knowledge()),
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"the first attempt: {first:?}"
	);
	assert_eq!(
		at_answer.len(),
		1,
		"one parent at the answer: {at_answer:?}"
	);
	assert_eq!(
		at_answer[0].status,
		ParentStatus::Reserved,
		"pending evidence leaves the parent Reserved"
	);
	let ordinal = at_answer[0]
		.claim
		.children
		.iter()
		.position(|child| child.holder.node == holder)
		.expect("a child held at the remote holder");
	assert!(
		held.as_ref().is_some_and(
			|held| held.rsv == at_answer[0].rsv && usize::from(held.ordinal) == ordinal
		),
		"the held child write is this parent's child {ordinal}: {held:?}"
	);
	assert_committed(&retried, "the same parent, retried");
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert_eq!(
		records[0].status,
		ParentStatus::Accepted {
			dec: retried.write.log_index
		}
	);
	assert_reads(
		"after the retry",
		reads,
		&[(first_row, "a".to_string()), (second_row, "b".to_string())].into(),
	);
}

/// DC-207 (acceptance 2): a holder's replicate to one plan member is held at
/// that member, so it never answers. The holder's own acknowledgement and the
/// other member's are a majority of the three-member plan including the
/// holder, so the first attempt commits from that evidence within
/// [`CHILD_REPLICATE_ANSWER_BOUND`], and every node reads the row exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_replica_leaves_a_quorum_that_commits_after_its_deadline() {
	use crate::network::data_server::child_writes::ChildRole;
	let (cluster, leader, descriptor) = ready().await;
	let (id, holder) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let held_member = *NODES.iter().find(|node| **node != holder).unwrap();
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &[(id, "a")]);
	let hold = hold_child(&cluster, held_member, holder, ChildRole::Replica);
	let mut write = spawn_attempt(&cluster, holder, meta, batch.clone());
	until_child_reached("held replica", &hold, &mut write).await;
	let held = hold.meta();

	let first = tokio::time::timeout(CHILD_REPLICATE_ANSWER_BOUND, &mut write).await;
	let at_answer = records_of(&cluster, key).await;
	drop(hold);
	let original = original_after_release(&first, &mut write).await;
	let retried = write_through(&cluster, holder, meta, batch).await;
	let reads = reads_everywhere(&cluster, &descriptor).await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let first_text = first_answer_text(&first);
	let Ok(Ok(Ok((first, _)))) = first else {
		panic!(
			"no answer within {CHILD_REPLICATE_ANSWER_BOUND:?} while one replica held the \
			 holder's replicate: {first_text}; after the release the original attempt gave \
			 {original:?}"
		);
	};
	assert_committed(&first, "the first attempt, from the other acknowledgements");
	assert_eq!(at_answer.len(), 1, "one parent: {at_answer:?}");
	assert_eq!(
		at_answer[0].status,
		ParentStatus::Accepted {
			dec: first.write.log_index
		}
	);
	let plan: Vec<u32> = at_answer[0].claim.children[0]
		.plan
		.iter()
		.map(|member| member.node)
		.collect();
	assert_eq!(plan.len(), 3, "a three-member plan: {plan:?}");
	assert!(
		plan.contains(&held_member) && plan.contains(&holder),
		"the held member and the holder are in the plan: {plan:?}"
	);
	assert!(
		held.as_ref()
			.is_some_and(|held| held.rsv == at_answer[0].rsv && held.ordinal == 0),
		"the held replicate is this parent's child: {held:?}"
	);
	assert_committed(&retried, "the same parent, retried");
	assert_eq!(retried.write.log_index, first.write.log_index);
	assert_reads("after the commit", reads, &[(id, "a".to_string())].into());
}

/// DC-207 (acceptance 2, insufficient evidence): both of the holder's
/// replicates are held, so only the holder's acknowledgement arrives, short
/// of a majority. The first attempt answers WRITE_STALLED with INDETERMINATE
/// within [`CHILD_REPLICATE_ANSWER_BOUND`] and the parent stays Reserved:
/// pending evidence, never a terminal refusal. Released, the same parent
/// retried commits, with exactly its row at every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn held_replicas_short_of_a_quorum_answer_unknown_and_never_refuse() {
	use crate::network::data_server::child_writes::ChildRole;
	let (cluster, leader, descriptor) = ready().await;
	let (id, holder) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &[(id, "a")]);
	let holds: Vec<HeldChild> = NODES
		.iter()
		.filter(|node| **node != holder)
		.map(|member| hold_child(&cluster, *member, holder, ChildRole::Replica))
		.collect();
	let mut write = spawn_attempt(&cluster, holder, meta, batch.clone());
	for hold in &holds {
		until_child_reached("held replicas", hold, &mut write).await;
	}

	let first = tokio::time::timeout(CHILD_REPLICATE_ANSWER_BOUND, &mut write).await;
	let at_answer = records_of(&cluster, key).await;
	drop(holds);
	let original = original_after_release(&first, &mut write).await;
	let retried = write_through(&cluster, holder, meta, batch).await;
	let records = records_of(&cluster, key).await;
	let reads = reads_everywhere(&cluster, &descriptor).await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}

	let first_text = first_answer_text(&first);
	let Ok(Ok(Ok((first, _)))) = first else {
		panic!(
			"no answer within {CHILD_REPLICATE_ANSWER_BOUND:?} while both replicas held the \
			 holder's replicates: {first_text}; after the release the original attempt gave \
			 {original:?}"
		);
	};
	assert_eq!(
		(first.write.status, first.write.mutation_commit_knowledge()),
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"the first attempt: {first:?}"
	);
	assert_eq!(
		at_answer.len(),
		1,
		"one parent at the answer: {at_answer:?}"
	);
	assert_eq!(
		at_answer[0].status,
		ParentStatus::Reserved,
		"pending evidence leaves the parent Reserved, never Refused"
	);
	assert_committed(&retried, "the same parent, retried");
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert_eq!(
		records[0].status,
		ParentStatus::Accepted {
			dec: retried.write.log_index
		}
	);
	assert_reads("after the retry", reads, &[(id, "a".to_string())].into());
}

/// DC-207 (acceptance 3, the ordinary response): with nothing held, a write
/// whose second child has a remote holder (a coordinator-to-holder write and
/// both hops' replicates) commits on its first attempt at its one recorded
/// decision, with exactly its rows at every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_write_with_a_remote_holder_answered_in_time_commits_on_its_first_attempt() {
	let (cluster, leader, descriptor) = ready().await;
	let (first_row, second_row, coordinator, _) =
		rows_with_a_remote_holder(&cluster, leader, &descriptor).await;
	let meta = write_meta(&descriptor, 2, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let (response, _) = cluster
		.node(coordinator)
		.data_server
		.table_write_for_test(
			meta,
			named_rows(&descriptor, &[(first_row, "a"), (second_row, "b")]),
		)
		.await
		.unwrap();
	assert_committed(&response, "the first attempt");
	let records = records_of(&cluster, key).await;
	assert_eq!(records.len(), 1, "one parent: {records:?}");
	assert_eq!(records[0].claim.children.len(), 2, "two children");
	assert_eq!(
		records[0].status,
		ParentStatus::Accepted {
			dec: response.write.log_index
		}
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&[(first_row, "a".to_string()), (second_row, "b".to_string())].into(),
		"answered in time",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

#[path = "parent_decisions_cluster_tests/write_deadline.rs"]
mod write_deadline;

#[path = "parent_decisions_cluster_tests/attempt_exhaustion.rs"]
mod attempt_exhaustion;

#[path = "parent_decisions_cluster_tests/offload.rs"]
mod offload;

/// ADR 0726: joins and the root check.
#[path = "parent_decisions_cluster_tests/root_check.rs"]
mod root_check;

// ============================================================
//  ADR 0720: MEMBERSHIP IS FIXED ONCE A WRITE IS ADMITTED
// ============================================================

/// After the first admitted write a fourth node's join starts no membership
/// entry: the cluster keeps exactly its three voters and no learner, the seal
/// names them, and writes go on.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_join_after_the_first_write_is_refused_and_adds_no_member() {
	let (cluster, leader, descriptor) = ready().await;
	let sealed_before = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().sealed().cloned())
		.await;
	assert!(sealed_before.is_none(), "nothing is sealed before a write");
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(1, "a")]),
		)
		.await,
		"the first write",
	);
	let members = |node: &WendelonNode| {
		let metrics = node.raft.metrics().borrow().clone();
		let membership = metrics.membership_config.membership().clone();
		(
			// The membership entry's own log id: an identical entry appended
			// later would change it, where the voters and nodes would not.
			*metrics.membership_config.log_id(),
			membership.voter_ids().collect::<Vec<u32>>(),
			membership.nodes().map(|(id, _)| *id).collect::<Vec<u32>>(),
			membership.get_joint_config().len(),
		)
	};
	let before = members(cluster.node(leader));
	assert!(before.0.is_some(), "the membership is an applied entry");
	assert_eq!(
		(&before.1, &before.2, before.3),
		(&NODES.to_vec(), &NODES.to_vec(), 1)
	);

	const LATE: u32 = 44;
	let (_late_dir, mut cfg) = member(LATE);
	cfg.trusted_lease_signers.insert(
		LATE,
		lease_verifying_key_bytes(dev_lease_signing_secret(LATE)),
	);
	cfg.trusted_transfer_signers.insert(
		LATE,
		transfer_verifying_key_bytes(dev_transfer_signing_secret(LATE)),
	);
	let provider: Arc<dyn crate::network::transport::TransportProvider> =
		Arc::new(SimulatedTransportProvider::new(
			crate::schema::NodeId {
				id: LATE,
				_reserved: 0,
			},
			cluster.network.clone(),
			crate::network::pool::BufPool::new(64, 1024 * 1024),
			TransportTier::QuicIoUring,
		));
	// The late joiner has the cluster's root KEK (ADR 0726), so what refuses
	// it is the sealed membership and nothing earlier.
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	// The joiner retries for five seconds and then reports the last answer it
	// was given. It must report within the bound, and report this refusal: a
	// timeout is a failure, not a refusal.
	let joined = tokio::time::timeout(
		std::time::Duration::from_secs(8),
		WendelonNode::bootstrap_with_provider(cfg, provider),
	)
	.await
	.expect("the joiner reported nothing within its bound");
	let error = match joined {
		Ok(_) => panic!("a node joined a sealed cluster"),
		Err(error) => error,
	};
	assert_eq!(
		error.kind(),
		std::io::ErrorKind::PermissionDenied,
		"refused for another reason: {error}"
	);
	assert!(
		error.to_string().contains(&format!(
			"reason_code={}",
			crate::network::control_server::CONTROL_REASON_MEMBERSHIP_SEALED
		)),
		"refused for another reason: {error}"
	);
	// No membership entry was started: the same entry, by log id, on every member.
	for id in NODES {
		assert_eq!(members(cluster.node(id)), before, "node {id}");
	}
	let sealed = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().sealed().cloned())
		.await
		.expect("the first write sealed");
	assert_eq!(
		sealed.placement().iter().copied().collect::<Vec<_>>(),
		NODES.map(|id| (id, true)).to_vec()
	);
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(2, "b")]),
		)
		.await,
		"a write after the refused join",
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}
