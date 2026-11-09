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
		let network = Arc::new(SimulatedTransportNetwork::default());
		let mut cluster = Self {
			nodes: Vec::new(),
			_dirs: Vec::new(),
			network,
			configs: std::collections::BTreeMap::new(),
		};
		for id in NODES {
			let (dir, cfg) = member(id);
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
		let at = self
			.nodes
			.iter()
			.position(|node| node.local_node.id == id)
			.unwrap();
		let node = self.nodes.remove(at);
		self.network
			.unpublish(crate::schema::NodeId { id, _reserved: 0 });
		tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
			.await
			.expect("a complete close")
			.unwrap();
		self.configs[&id].data_dir.join("CLEAN_SHUTDOWN").exists()
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
	let cluster = Cluster::start().await;
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
/// next start is unclean, proves its certified base, and reads exactly.
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
	assert!(
		!cluster.close(follower).await,
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
