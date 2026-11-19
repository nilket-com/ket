//! Wendelon ADR 0113: the public mutation-outcome resolve under ParentDecisions,
//! three voters. Every outcome answers alike from every node (read linearizably
//! there); answers survive a leader change and a full restart; without a quorum
//! the answer is UNAVAILABLE, never knowledge; and a table operation identity is
//! tenant-wide, so a reuse on another table conflicts with no effect.

use super::*;
use crate::schema::{
	ClientWriteId, MutationOperationKind, MutationOutcomeRequestMeta, MutationOutcomeResponseMeta,
	MutationResolutionStatus,
};

/// A MUTATION_OUTCOME_REQ through the data-frame handler of `node`, from a
/// data-node identity: what a public client's resolve gets from that node.
async fn resolve_at(
	cluster: &Cluster,
	node: u32,
	id: ClientWriteId,
) -> MutationOutcomeResponseMeta {
	use crate::network::router::DataRingServer;
	let meta = MutationOutcomeRequestMeta {
		operation_id: id,
		tenant_id: TenantId::ZERO,
	};
	let meta_bytes = bytemuck::bytes_of(&meta).to_vec();
	let frame = crate::network::core::HrftFrame {
		header: crate::schema::FrameHeader {
			magic: crate::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: crate::schema::MsgType::MUTATION_OUTCOME_REQ.0,
			flags: 0,
			stream_id: 83,
			meta_len: meta_bytes.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: crate::network::pool::PooledBuf::orphan(meta_bytes),
		arrow_payload: None,
		trailing: None,
	};
	let local = crate::schema::NodeId {
		id: node,
		_reserved: 0,
	};
	let context = crate::network::server::RequestContext {
		remote: Some(local),
		actor: crate::network::tls::ActorIdentity::data_node(local),
	};
	let response = tokio::time::timeout(
		BOUND,
		DataRingServer::handle_data_frame(cluster.node(node).data_server.as_ref(), context, frame),
	)
	.await
	.expect("the resolve answers")
	.expect("a response, not a dropped stream")
	.expect("a response frame");
	bytemuck::pod_read_unaligned(
		&response.meta[..std::mem::size_of::<MutationOutcomeResponseMeta>()],
	)
}

/// The answer's (resolution, status, knowledge, kind, counts).
fn shape(
	answer: &MutationOutcomeResponseMeta,
) -> (
	MutationResolutionStatus,
	WriteStatus,
	MutationCommitKnowledge,
	MutationOperationKind,
	[u32; 3],
) {
	(
		answer.resolution,
		answer.write.status,
		answer.write.mutation_commit_knowledge(),
		answer.operation_kind,
		[
			answer.deleted_count,
			answer.already_deleted_count,
			answer.not_found_count,
		],
	)
}

/// Resolves `id` at every live node, asserts they agree on everything, and
/// returns the answer.
async fn alike_everywhere(
	cluster: &Cluster,
	nodes: &[u32],
	id: ClientWriteId,
	what: &str,
) -> MutationOutcomeResponseMeta {
	let first = resolve_at(cluster, nodes[0], id).await;
	assert_eq!(
		first.write.echoed_write_id.canonical_idempotency_bytes(),
		id.canonical_idempotency_bytes(),
		"{what}: the canonical id is echoed"
	);
	for node in &nodes[1..] {
		let answer = resolve_at(cluster, *node, id).await;
		assert_eq!(shape(&answer), shape(&first), "{what}: at {node}");
		assert_eq!(
			answer.write.log_index, first.write.log_index,
			"{what}: at {node}"
		);
		assert_eq!(
			answer.write.commit_hlc, first.write.commit_hlc,
			"{what}: at {node}"
		);
	}
	first
}

fn ids_of(
	descriptor: &TableSchemaDescriptor,
	cluster: &Cluster,
	leader: u32,
	ids: &[i64],
) -> Vec<crate::schema::Identity> {
	let mut rows: Vec<_> = ids
		.iter()
		.map(|id| {
			cluster
				.node(leader)
				.data_server
				.row_identity_and_locality_for_test(
					TenantId::ZERO,
					descriptor,
					&named_rows(descriptor, &[(*id, "x")]),
				)
				.0
		})
		.collect();
	rows.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
	rows
}

fn delete_of(
	descriptor: &TableSchemaDescriptor,
	rows: usize,
) -> crate::schema::TableDeleteRowsRequestMeta {
	let mut meta = delete_meta(descriptor);
	meta.row_count = rows as u32;
	meta
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_outcome_resolves_alike_from_every_node() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let present = MutationResolutionStatus::PRESENT;

	// A committed write: its receipt.
	let write = write_meta(&descriptor, 1, uuid_v7());
	let written = write_through(
		&cluster,
		leader,
		write,
		named_rows(&descriptor, &[(1, "a"), (2, "b")]),
	)
	.await;
	assert_committed(&written, "the write");
	let answer =
		alike_everywhere(&cluster, &NODES, write.parent_write_id, "a committed write").await;
	assert_eq!(
		shape(&answer),
		(
			present,
			WriteStatus::OK,
			MutationCommitKnowledge::COMMITTED,
			MutationOperationKind::WRITE,
			[0, 0, 0]
		)
	);
	assert_eq!(answer.write.log_index, written.write.log_index);
	assert_eq!(answer.write.commit_hlc, written.write.commit_hlc);

	// A committed delete: its frozen counts.
	let rows = ids_of(&descriptor, &cluster, leader, &[1, 2, 99]);
	let delete = delete_of(&descriptor, rows.len());
	let deleted = delete_through(&cluster, leader, delete, rows).await;
	assert_eq!(deleted.write.status, WriteStatus::OK, "{deleted:?}");
	let answer = alike_everywhere(
		&cluster,
		&NODES,
		delete.parent_write_id,
		"a committed delete",
	)
	.await;
	assert_eq!(
		shape(&answer),
		(
			present,
			WriteStatus::OK,
			MutationCommitKnowledge::COMMITTED,
			MutationOperationKind::DELETE,
			[2, 0, 1]
		)
	);
	assert_eq!(answer.write.log_index, deleted.write.log_index);

	// A noop: committed with its counts, and no invented commit HLC.
	let rows = ids_of(&descriptor, &cluster, leader, &[500]);
	let noop = delete_of(&descriptor, 1);
	let nothing = delete_through(&cluster, leader, noop, rows).await;
	assert_eq!(nothing.write.status, WriteStatus::OK, "{nothing:?}");
	let answer = alike_everywhere(&cluster, &NODES, noop.parent_write_id, "a noop").await;
	assert_eq!(
		shape(&answer),
		(
			present,
			WriteStatus::OK,
			MutationCommitKnowledge::COMMITTED,
			MutationOperationKind::DELETE,
			[0, 0, 1]
		)
	);
	assert_eq!(
		answer.write.commit_hlc.physical_ms, 0,
		"a noop has no commit HLC"
	);

	// Pending, then refused: no effect, no counts.
	let batch = named_rows(&descriptor, &[(7, "p")]);
	let coordinator = cluster
		.node(leader)
		.data_server
		.primary_for_test(TenantId::ZERO, &descriptor, &batch)
		.await
		.unwrap();
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let pending_meta = write_meta(&descriptor, 1, uuid_v7());
	let server = cluster.node(coordinator).data_server.clone();
	let pending =
		tokio::spawn(async move { server.table_write_for_test(pending_meta, batch).await });
	reached.notified().await;
	let answer = alike_everywhere(&cluster, &NODES, pending_meta.parent_write_id, "pending").await;
	assert_eq!(
		shape(&answer),
		(
			present,
			WriteStatus::FINALIZATION_PENDING,
			MutationCommitKnowledge::INDETERMINATE,
			MutationOperationKind::WRITE,
			[0, 0, 0]
		)
	);
	let record = record_of(&cluster, key_of(&descriptor, &pending_meta))
		.await
		.expect("reserved");
	abort(&cluster, &record).await;
	release.notify_one();
	let _ = pending.await;
	let answer = alike_everywhere(&cluster, &NODES, pending_meta.parent_write_id, "refused").await;
	assert_eq!(
		shape(&answer),
		(
			present,
			WriteStatus::WRITE_REFUSED,
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			MutationOperationKind::WRITE,
			[0, 0, 0]
		)
	);

	// Never seen, old, and invalid.
	let fresh = write_meta(&descriptor, 1, uuid_v7()).parent_write_id;
	let answer = alike_everywhere(&cluster, &NODES, fresh, "never seen").await;
	assert_eq!(answer.resolution, MutationResolutionStatus::UNKNOWN);
	assert_eq!(answer.write.status, WriteStatus::WRITE_OPERATION_UNKNOWN);
	let old = write_meta(&descriptor, 1, uuid_v7_at(1_000)).parent_write_id;
	let answer = alike_everywhere(&cluster, &NODES, old, "old").await;
	assert_eq!(answer.resolution, MutationResolutionStatus::EXPIRED);
	assert_eq!(answer.write.status, WriteStatus::WRITE_OPERATION_EXPIRED);
	let invalid = write_meta(&descriptor, 1, [0; 16]).parent_write_id;
	let answer = alike_everywhere(&cluster, &NODES, invalid, "invalid").await;
	assert_eq!(answer.resolution, MutationResolutionStatus::INVALID_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_resolve_survives_a_leader_change_and_a_restart_and_is_unavailable_without_quorum() {
	let (mut cluster, leader, descriptor) = ready().await;
	let write = write_meta(&descriptor, 1, uuid_v7());
	let written = write_through(
		&cluster,
		leader,
		write,
		named_rows(&descriptor, &[(3, "c")]),
	)
	.await;
	assert_committed(&written, "the write");
	let id = write.parent_write_id;
	let before = alike_everywhere(&cluster, &NODES, id, "before").await;

	// The leader stops: the others answer the same.
	cluster.stop(leader, false).await;
	let rest: Vec<u32> = NODES.into_iter().filter(|node| *node != leader).collect();
	cluster.leader().await;
	// A new leader serves linearizable cuts once its term commits; until then the
	// honest answer is UNAVAILABLE. Wait for it, bounded.
	until_available(&cluster, &rest, id).await;
	let after = alike_everywhere(&cluster, &rest, id, "after a leader change").await;
	assert_eq!(shape(&after), shape(&before));
	assert_eq!(after.write.log_index, before.write.log_index);

	// Without a quorum: UNAVAILABLE, never UNKNOWN, EXPIRED or not-committed.
	cluster.stop(rest[0], false).await;
	let alone = resolve_at(&cluster, rest[1], id).await;
	assert_eq!(
		alone.resolution,
		MutationResolutionStatus::UNAVAILABLE,
		"{alone:?}"
	);
	assert_eq!(alone.write.status, WriteStatus::WRITE_STALLED);
	assert_eq!(
		alone.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	let fresh = resolve_at(
		&cluster,
		rest[1],
		write_meta(&descriptor, 1, uuid_v7()).parent_write_id,
	)
	.await;
	assert_eq!(
		fresh.resolution,
		MutationResolutionStatus::UNAVAILABLE,
		"never UNKNOWN: {fresh:?}"
	);

	// Everyone back, from their stores: the same answer.
	cluster.reopen(leader).await;
	cluster.reopen(rest[0]).await;
	until_settled(&cluster, &descriptor).await;
	until_available(&cluster, &NODES, id).await;
	let restarted = alike_everywhere(&cluster, &NODES, id, "after a restart").await;
	assert_eq!(shape(&restarted), shape(&before));
	assert_eq!(restarted.write.log_index, before.write.log_index);
}

/// A second table, installed beside the fixture's.
async fn second_table(cluster: &Cluster, leader: u32) -> TableSchemaDescriptor {
	let other = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x52; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"events_other",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		crate::schema::LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::SingleRow,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&other),
		))
		.await
		.unwrap();
	until_settled(cluster, &other).await;
	other
}

/// Wendelon ADR 0113: a table operation identity is tenant-wide across tables. A
/// reuse on another table conflicts and writes nothing there; the first table's
/// receipt is unchanged; in either order; and two concurrent offers on two tables
/// leave exactly one parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reused_identity_on_another_table_conflicts_with_no_effect() {
	let (cluster, leader, table_a) = ready().await;
	let table_b = second_table(&cluster, leader).await;
	let mut expected_a = Expected::new();

	// A, then B.
	let id = uuid_v7();
	let on_a = write_meta(&table_a, 1, id);
	let first = write_through(&cluster, leader, on_a, named_rows(&table_a, &[(1, "a")])).await;
	assert_committed(&first, "A");
	expected_a.insert(1, "a".into());
	let receipt = alike_everywhere(&cluster, &NODES, on_a.parent_write_id, "A's receipt").await;
	let on_b = write_meta(&table_b, 1, id);
	let second = write_through(&cluster, leader, on_b, named_rows(&table_b, &[(1, "b")])).await;
	assert_eq!(
		second.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{second:?}"
	);
	expect_everywhere(&cluster, &table_b, &Expected::new(), "B is untouched").await;
	expect_everywhere(&cluster, &table_a, &expected_a, "A is unchanged").await;
	let again = alike_everywhere(&cluster, &NODES, on_a.parent_write_id, "A's receipt again").await;
	assert_eq!(shape(&again), shape(&receipt));
	assert_eq!(again.write.log_index, receipt.write.log_index);

	// A noop on B first, then a write on A under its identity.
	let rows = ids_of(&table_b, &cluster, leader, &[700]);
	let noop = delete_of(&table_b, 1);
	let nothing = delete_through(&cluster, leader, noop, rows).await;
	assert_eq!(nothing.write.status, WriteStatus::OK, "{nothing:?}");
	let mut reuse = write_meta(&table_a, 1, noop.parent_write_id.client_uuid);
	reuse.parent_write_id.sequence_no = noop.parent_write_id.sequence_no;
	let refused = write_through(&cluster, leader, reuse, named_rows(&table_a, &[(2, "z")])).await;
	assert_eq!(
		refused.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{refused:?}"
	);
	expect_everywhere(&cluster, &table_a, &expected_a, "no row 2 on A").await;
	let answer =
		alike_everywhere(&cluster, &NODES, noop.parent_write_id, "the noop's receipt").await;
	assert_eq!(answer.operation_kind, MutationOperationKind::DELETE);
	assert_eq!(
		[
			answer.deleted_count,
			answer.already_deleted_count,
			answer.not_found_count
		],
		[0, 0, 1]
	);

	// Two concurrent offers on two tables: exactly one parent.
	let id = uuid_v7();
	let (meta_a, meta_b) = (write_meta(&table_a, 1, id), write_meta(&table_b, 1, id));
	let (batch_a, batch_b) = (
		named_rows(&table_a, &[(3, "a")]),
		named_rows(&table_b, &[(3, "b")]),
	);
	let (got_a, got_b) = tokio::join!(
		write_through(&cluster, leader, meta_a, batch_a),
		write_through(
			&cluster,
			NODES[(NODES.iter().position(|n| *n == leader).unwrap() + 1) % 3],
			meta_b,
			batch_b
		),
	);
	let winners = [&got_a, &got_b]
		.iter()
		.filter(|got| got.write.status == WriteStatus::OK)
		.count();
	assert_eq!(winners, 1, "{got_a:?} {got_b:?}");
	let loser = if got_a.write.status == WriteStatus::OK {
		&got_b
	} else {
		&got_a
	};
	assert_eq!(
		loser.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{loser:?}"
	);
	let winner = if got_a.write.status == WriteStatus::OK {
		&got_a
	} else {
		&got_b
	};
	let answer = alike_everywhere(&cluster, &NODES, meta_a.parent_write_id, "the winner").await;
	assert_eq!(answer.write.log_index, winner.write.log_index);
	if got_a.write.status == WriteStatus::OK {
		expected_a.insert(3, "a".into());
		expect_everywhere(&cluster, &table_b, &Expected::new(), "B lost: no row").await;
	} else {
		let mut expected_b = Expected::new();
		expected_b.insert(3, "b".into());
		expect_everywhere(&cluster, &table_b, &expected_b, "B won").await;
	}
	expect_everywhere(&cluster, &table_a, &expected_a, "A as decided").await;
}

/// Waits, within the test bound, until every node in `nodes` answers `id` with
/// something other than UNAVAILABLE (a leader's term committing after a change).
async fn until_available(cluster: &Cluster, nodes: &[u32], id: ClientWriteId) {
	let deadline = tokio::time::Instant::now() + BOUND;
	'wait: loop {
		for node in nodes {
			if resolve_at(cluster, *node, id).await.resolution
				== MutationResolutionStatus::UNAVAILABLE
			{
				assert!(
					tokio::time::Instant::now() < deadline,
					"still unavailable at {node}"
				);
				tokio::time::sleep(std::time::Duration::from_millis(50)).await;
				continue 'wait;
			}
		}
		return;
	}
}

/// Wendelon ADR 0113: under ParentDecisions an outcome pin or acknowledge is a
/// typed UNSUPPORTED answer after write authorization, never a dropped stream;
/// an unknown action is still INVALID_ACTION.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_outcome_pin_is_a_typed_unsupported_answer() {
	use crate::network::router::DataRingServer;
	use crate::schema::{
		MutationOutcomePinAction, MutationOutcomePinRequestMeta, MutationOutcomePinResponseMeta,
		MutationOutcomePinStatus,
	};
	let (cluster, leader, descriptor) = ready().await;
	let id = write_meta(&descriptor, 1, uuid_v7()).parent_write_id;
	for (action, expected) in [
		(
			MutationOutcomePinAction::PIN,
			MutationOutcomePinStatus::UNSUPPORTED,
		),
		(
			MutationOutcomePinAction::ACKNOWLEDGE,
			MutationOutcomePinStatus::UNSUPPORTED,
		),
		(
			MutationOutcomePinAction(9),
			MutationOutcomePinStatus::INVALID_ACTION,
		),
	] {
		let meta = MutationOutcomePinRequestMeta {
			operation_id: id,
			tenant_id: TenantId::ZERO,
			action,
			_pad: [0; 7],
		};
		let meta_bytes = bytemuck::bytes_of(&meta).to_vec();
		let frame = crate::network::core::HrftFrame {
			header: crate::schema::FrameHeader {
				magic: crate::network::core::HRFT_MAGIC,
				version: 1,
				msg_type: crate::schema::MsgType::MUTATION_OUTCOME_PIN_REQ.0,
				flags: 0,
				stream_id: 85,
				meta_len: meta_bytes.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: crate::network::pool::PooledBuf::orphan(meta_bytes),
			arrow_payload: None,
			trailing: None,
		};
		let local = crate::schema::NodeId {
			id: leader,
			_reserved: 0,
		};
		let context = crate::network::server::RequestContext {
			remote: Some(local),
			actor: crate::network::tls::ActorIdentity::data_node(local),
		};
		let response = DataRingServer::handle_data_frame(
			cluster.node(leader).data_server.as_ref(),
			context,
			frame,
		)
		.await
		.expect("a response, not a dropped stream")
		.expect("a response frame");
		let answer: MutationOutcomePinResponseMeta = bytemuck::pod_read_unaligned(
			&response.meta[..std::mem::size_of::<MutationOutcomePinResponseMeta>()],
		);
		assert_eq!(answer.status, expected, "{action:?}");
	}
}

/// Wendelon ADR 0113: at a leader that has lost its quorum, the resolve's read
/// (its own cut included) ends at the server's budget with UNAVAILABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_leader_without_its_quorum_answers_unavailable_within_its_budget() {
	let (mut cluster, leader, descriptor) = ready().await;
	for node in NODES.into_iter().filter(|node| *node != leader) {
		cluster.stop(node, false).await;
	}
	let id = write_meta(&descriptor, 1, uuid_v7()).parent_write_id;
	let started = std::time::Instant::now();
	let answer = resolve_at(&cluster, leader, id).await;
	let elapsed = started.elapsed();
	assert_eq!(
		answer.resolution,
		MutationResolutionStatus::UNAVAILABLE,
		"{answer:?}"
	);
	assert!(
		elapsed
			< crate::network::data_server::PD_RESOLVE_READ_BUDGET
				+ std::time::Duration::from_secs(2),
		"{elapsed:?}"
	);
}

/// Wendelon ADR 0113: the final read of the applied state is inside the server's
/// budget. A follower whose state lock is held (as a long apply would) after a
/// successful cut and apply answers UNAVAILABLE within the budget, not when the
/// lock is released; once released it answers the committed receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_held_state_lock_after_the_cut_answers_unavailable_within_the_budget() {
	let (cluster, leader, descriptor) = ready().await;
	let write = write_meta(&descriptor, 1, uuid_v7());
	let written = write_through(
		&cluster,
		leader,
		write,
		named_rows(&descriptor, &[(5, "e")]),
	)
	.await;
	assert_committed(&written, "the write");
	let follower = NODES.into_iter().find(|node| *node != leader).unwrap();
	until_available(&cluster, &[follower], write.parent_write_id).await;
	let guard = cluster
		.node(follower)
		.state_machine
		.hold_state_for_test()
		.await;
	let started = std::time::Instant::now();
	let held = resolve_at(&cluster, follower, write.parent_write_id).await;
	let elapsed = started.elapsed();
	assert_eq!(
		held.resolution,
		MutationResolutionStatus::UNAVAILABLE,
		"{held:?}"
	);
	assert!(
		elapsed
			< crate::network::data_server::PD_RESOLVE_READ_BUDGET
				+ std::time::Duration::from_secs(1),
		"{elapsed:?}"
	);
	drop(guard);
	let answer = resolve_at(&cluster, follower, write.parent_write_id).await;
	assert_eq!(
		answer.resolution,
		MutationResolutionStatus::PRESENT,
		"{answer:?}"
	);
	assert_eq!(answer.write.log_index, written.write.log_index);
}

/// The lineage holder of `position`, from the leader's applied state.
async fn holder_of(cluster: &Cluster, position: u64) -> u32 {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| {
					decisions
						.lineages()
						.range(..=position)
						.next_back()
						.filter(|(_, lineage)| position <= lineage.range_end)
						.map(|(_, lineage)| lineage.holder.id)
				})
				.expect("every position is in a lineage")
		})
		.await
}

/// Wendelon ADR 0113 (Codex 01a1f424): a pending and then a refused DELETE resolve
/// with NO delete counts at the public SDK's conversion: their selection had no
/// effect yet, or never. A committed delete and a committed noop keep theirs,
/// zeros included.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_pending_or_refused_delete_resolves_without_counts() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let inserted = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_through(
			&cluster,
			leader,
			inserted,
			named_rows(&descriptor, &[(40, "x")]),
		)
		.await,
		"a row",
	);
	let rows = ids_of(&descriptor, &cluster, leader, &[40]);
	let position =
		crate::consensus::decisions::delete_row_position(TenantId::ZERO, &descriptor, rows[0])
			.unwrap();
	let coordinator = holder_of(&cluster, position).await;
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let meta = delete_of(&descriptor, 1);
	let server = cluster.node(coordinator).data_server.clone();
	let pending = {
		let rows = rows.clone();
		tokio::spawn(async move { server.table_delete_for_test(meta, rows).await })
	};
	reached.notified().await;
	let answer = alike_everywhere(&cluster, &NODES, meta.parent_write_id, "a pending delete").await;
	assert_eq!(answer.resolution, MutationResolutionStatus::PRESENT);
	assert_eq!(answer.write.status, WriteStatus::FINALIZATION_PENDING);
	assert_eq!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(
		answer.delete_counts(),
		None,
		"a pending delete has no counts yet: {answer:?}"
	);
	let key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: meta.parent_write_id.sequence_no,
	};
	let record = record_of(&cluster, key).await.expect("reserved");
	abort(&cluster, &record).await;
	release.notify_one();
	let _ = pending.await;
	let answer = alike_everywhere(&cluster, &NODES, meta.parent_write_id, "a refused delete").await;
	assert_eq!(answer.write.status, WriteStatus::WRITE_REFUSED);
	assert_eq!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);
	assert_eq!(
		answer.delete_counts(),
		None,
		"a refused delete deleted nothing: {answer:?}"
	);

	// Positive controls: a committed delete and a committed noop keep their counts.
	let committed = delete_of(&descriptor, 1);
	let done = delete_through(&cluster, leader, committed, rows.clone()).await;
	assert_eq!(done.write.status, WriteStatus::OK, "{done:?}");
	let answer = alike_everywhere(
		&cluster,
		&NODES,
		committed.parent_write_id,
		"a committed delete",
	)
	.await;
	assert_eq!(answer.delete_counts(), Some([1, 0, 0]));
	let noop = delete_of(&descriptor, 1);
	let none = delete_through(
		&cluster,
		leader,
		noop,
		ids_of(&descriptor, &cluster, leader, &[41]),
	)
	.await;
	assert_eq!(none.write.status, WriteStatus::OK, "{none:?}");
	let answer = alike_everywhere(&cluster, &NODES, noop.parent_write_id, "a committed noop").await;
	assert_eq!(
		answer.delete_counts(),
		Some([0, 0, 1]),
		"legitimate zeros are kept"
	);
}
