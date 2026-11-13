//! ADR 0729 (DC-224): a range's coordinating authority moves to a replica when
//! its holder stops renewing. Before it, a new write to a range whose ring
//! primary was stopped did not commit until the primary returned, whoever led
//! the control ring; these tests are that characterization turned positive,
//! and the same boundary with the old holder running but cut off.

use super::*;
use crate::consensus::decisions::{Lineage, ParentStatus, RefusalCause};
use crate::network::data_server::child_writes::PauseStage;
use std::sync::atomic::Ordering::Release;

/// The first `count` row ids whose first-row lineage is the one starting at
/// `range_start`, as `asked` reads its applied lineages. Nothing is written.
async fn rows_in_lineage(
	cluster: &Cluster,
	asked: u32,
	descriptor: &TableSchemaDescriptor,
	range_start: u64,
	count: usize,
) -> Vec<i64> {
	let mut found = Vec::new();
	for id in 1..2_000 {
		let at = cluster
			.node(asked)
			.data_server
			.lineage_for_test(TenantId::ZERO, descriptor, &rows(descriptor, &[id]))
			.await
			.expect("every position is leased");
		if at.0 == range_start {
			found.push(id);
			if found.len() == count {
				return found;
			}
		}
	}
	panic!("fewer than {count} of rows 1..2000 are in the lineage at {range_start}: {found:?}");
}

/// A lineage held by `holder` that is not `not_start`, with its range start.
async fn a_lineage_of(cluster: &Cluster, asked: u32, holder: u32) -> (u64, Lineage) {
	cluster
		.node(asked)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.lineages()
				.iter()
				.find(|(_, lineage)| lineage.holder.id == holder)
				.map(|(start, lineage)| (*start, *lineage))
		})
		.await
		.unwrap_or_else(|| panic!("node {holder} holds no lineage"))
}

async fn lineage_at(cluster: &Cluster, asked: u32, range_start: u64) -> Lineage {
	cluster
		.node(asked)
		.state_machine
		.read_state(move |state| state.decisions.as_ref().unwrap().lineages()[&range_start])
		.await
}

/// The data nodes after `holder` in the replica order of the segment holding
/// `range_start`: the order in which candidates stagger their takeover.
async fn candidates_after(
	cluster: &Cluster,
	asked: u32,
	range_start: u64,
	holder: u32,
) -> Vec<u32> {
	cluster
		.node(asked)
		.state_machine
		.read_state(move |state| {
			let (_, config) = state.epoch_history.last_key_value().unwrap();
			crate::network::ring::RingState::build(
				&crate::network::ring::membership_from_epoch_config(config),
				NODES.len(),
			)
			.replica_segments()
			.iter()
			.find(|segment| segment.start <= range_start && range_start <= segment.end)
			.unwrap()
			.replicas
			.iter()
			.map(|node| node.id)
			.filter(|node| *node != holder)
			.collect()
		})
		.await
}

/// The control leader the running nodes agree on, which must not be `not`.
async fn agreed_leader_other_than(cluster: &Cluster, not: u32) -> u32 {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let mut seen = std::collections::BTreeSet::new();
		for node in &cluster.nodes {
			seen.insert(node.raft.current_leader().await);
		}
		if let [Some(leader)] = seen.iter().copied().collect::<Vec<_>>()[..]
			&& leader != not
			&& cluster
				.nodes
				.iter()
				.any(|node| node.local_node.id == leader)
		{
			return leader;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the running nodes see leaders {seen:?}, wanted one other than {not}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	}
}

/// Waits, bounded, until the lineage at `range_start` is held by a node other
/// than `old`; returns it.
async fn until_taken_over(cluster: &Cluster, asked: u32, range_start: u64, old: u32) -> Lineage {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let lineage = lineage_at(cluster, asked, range_start).await;
		if lineage.holder.id != old {
			return lineage;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the lineage at {range_start} is still {old}'s: {lineage:?}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
}

/// Waits, bounded, until `asked` has applied a lease of `holder` over the range
/// at `range_start`: until then a node asked to redirect to it answers a stall.
async fn until_leased_by(cluster: &Cluster, asked: u32, range_start: u64, holder: u32) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leased = cluster
			.node(asked)
			.state_machine
			.read_state(move |state| {
				state
					.leases
					.get(&range_start)
					.is_some_and(|lease| lease.holder.id == holder)
			})
			.await;
		if leased {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{holder} never leased the range at {range_start}"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
}

/// `node`'s stored intent for the first child of the parent reserved at `rsv`.
async fn intent_of(
	cluster: &Cluster,
	node: u32,
	rsv: u64,
) -> Option<crate::storage::index::parent_intents::ParentIntent> {
	cluster
		.node(node)
		.slab
		.persistent_index()
		.unwrap()
		.parent_intent(rsv, 0)
		.await
		.unwrap()
}

/// One attempt of a write at `entry`, with no redirect followed and no retry.
async fn offer(
	cluster: &Cluster,
	entry: u32,
	meta: TableWriteRequestMeta,
	batch: &RecordBatch,
) -> crate::schema::TableWriteResponseMeta {
	tokio::time::timeout(
		std::time::Duration::from_secs(30),
		cluster
			.node(entry)
			.data_server
			.table_write_for_test(meta, batch.clone()),
	)
	.await
	.expect("a live node answers")
	.unwrap()
	.0
}

/// A primary stopped while another node leads the control ring. A replica
/// takes its range once its lease has lapsed, although the earlier candidate
/// in the range's replica order cannot act; the write that could not be
/// reserved commits under the successor; a range whose primary is live is
/// unaffected throughout; the returning primary holds no lineage for the
/// range, redirects, and serves as a replica; and every node recovers the same
/// authority from its own store.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replica_takes_the_range_of_a_stopped_primary_and_its_write_commits() {
	let (mut cluster, leader, descriptor) = ready().await;
	let (_, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let replica = NODES
		.into_iter()
		.find(|node| *node != leader && *node != primary)
		.unwrap();
	let (range_start, before) = a_lineage_of(&cluster, leader, primary).await;
	let at_primary = rows_in_lineage(&cluster, leader, &descriptor, range_start, 3).await;
	let (row_a, row_b, row_c) = (at_primary[0], at_primary[1], at_primary[2]);
	let (live_start, _) = a_lineage_of(&cluster, leader, leader).await;
	let row_live = rows_in_lineage(&cluster, leader, &descriptor, live_start, 1).await[0];
	let order = candidates_after(&cluster, leader, range_start, primary).await;
	let (first, later) = (order[0], order[1]);

	// Control: a non-primary replica stopped, a majority up: the range takes writes.
	cluster.stop(replica, false).await;
	assert_eq!(agreed_leader_other_than(&cluster, replica).await, leader);
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(row_a, "a")]),
		)
		.await,
		"a replica down, the primary up",
	);
	cluster.reopen(replica).await;
	until_settled(&cluster, &descriptor).await;
	assert_eq!(agreed_leader_other_than(&cluster, primary).await, leader);
	assert_eq!(lineage_at(&cluster, leader, range_start).await, before);

	// The earlier candidate cannot act; the later one is not blocked by it.
	cluster
		.node(first)
		.data_server
		.decision_peer_for_test()
		.pause_takeover
		.store(true, Release);
	cluster.stop(primary, false).await;
	assert_eq!(agreed_leader_other_than(&cluster, primary).await, leader);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let key = key_of(&descriptor, &meta);
	let batch = named_rows(&descriptor, &[(row_b, "b")]);
	// Before the lapse nothing is reserved, and nothing but the stopped
	// holder's own node is ever hinted.
	let early = offer(&cluster, leader, meta, &batch).await;
	assert_ne!(
		early.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED,
		"{early:?}"
	);
	assert!(record_of(&cluster, key).await.is_none());
	// A range whose primary is live takes writes meanwhile.
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(row_live, "l")]),
		)
		.await,
		"a range whose primary is live",
	);
	let after = until_taken_over(&cluster, leader, range_start, primary).await;
	assert_eq!(after.holder.id, later, "the later candidate took it");
	assert!(after.generation > before.generation);
	assert!(after.floor_ms > before.floor_ms);
	assert_eq!(
		(after.range_end, after.epoch),
		(before.range_end, before.epoch)
	);
	// The same identity, offered to the live nodes with redirects followed
	// among them, commits under the successor.
	let committed = write_through(&cluster, leader, meta, batch.clone()).await;
	assert_committed(&committed, "the write the stopped primary could not take");
	let record = record_of(&cluster, key).await.expect("recorded");
	assert!(matches!(record.status, ParentStatus::Accepted { .. }));
	let child = &record.claim.children[usize::from(record.claim.coordinator)];
	assert_eq!(child.holder.node, later);
	assert_eq!(child.lineage.generation, after.generation);
	assert!(record.claim.commit_hlc.physical_ms > after.floor_ms);

	// The primary returns: it holds no lineage for the range, it redirects, a
	// write entering at it commits under the successor, and it reads as a replica.
	cluster
		.node(first)
		.data_server
		.decision_peer_for_test()
		.pause_takeover
		.store(false, Release);
	cluster.reopen(primary).await;
	until_settled(&cluster, &descriptor).await;
	let held = lineage_at(&cluster, primary, range_start).await;
	assert_eq!(held.holder.id, later, "no hand-back");
	until_leased_by(&cluster, primary, range_start, later).await;
	let redirected = offer(
		&cluster,
		primary,
		write_meta(&descriptor, 1, uuid_v7()),
		&named_rows(&descriptor, &[(row_c, "x")]),
	)
	.await;
	assert_eq!(
		(redirected.write.status, redirected.write.leader_hint.id),
		(WriteStatus::NOT_LEADER, later),
		"{redirected:?}"
	);
	assert_committed(
		&write_through(
			&cluster,
			primary,
			write_meta(&descriptor, 1, uuid_v7()),
			named_rows(&descriptor, &[(row_c, "c")]),
		)
		.await,
		"entering at the returned primary",
	);
	// The identity that committed answers its recorded outcome again.
	assert_committed(
		&write_through(&cluster, leader, meta, batch).await,
		"the same identity again",
	);
	let expected: Expected = [
		(row_a, "a".to_string()),
		(row_b, "b".to_string()),
		(row_c, "c".to_string()),
		(row_live, "l".to_string()),
	]
	.into();
	// A reopened member may answer the retryable refusal before its first OK;
	// the bounded read accepts only that, and requires the exact rows.
	assert_eq!(
		cluster.read_once_validated(primary, &descriptor).await,
		expected,
		"the returned primary"
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"after the primary's return",
	)
	.await;

	// Every node restarted on its own store: the range is still the
	// successor's (a new generation, since its lease lapsed while it was down),
	// never the ring primary's again, and every node reads the same rows.
	for id in NODES {
		cluster.stop(id, false).await;
	}
	for id in NODES {
		cluster.reopen(id).await;
	}
	until_settled(&cluster, &descriptor).await;
	let recovered = lineage_at(&cluster, cluster.leader().await, range_start).await;
	assert_eq!(recovered.holder.id, later);
	assert!(recovered.generation >= after.generation);
	assert!(recovered.floor_ms >= after.floor_ms);
	for id in NODES {
		assert_eq!(
			cluster.read_once_validated(id, &descriptor).await,
			expected,
			"restarted node {id}"
		);
	}
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"after every node restarted",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The old holder is running and cut off from its leases, not stopped. One of
/// its writes is held before it builds its claim and another after its
/// children are written and acknowledged, before Finalize; a third was
/// accepted earlier. A replica takes the range. Released, the reserved write
/// is refused once and for good, the unreserved one finds it no longer holds
/// the range and commits under the successor, the accepted one is untouched,
/// and no node ever reads the refused write's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_takeover_refuses_a_cut_off_holders_reserved_write_and_keeps_its_accepted_one() {
	let (cluster, leader, descriptor) = ready().await;
	let (_, holder) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let (range_start, before) = a_lineage_of(&cluster, leader, holder).await;
	let ids = rows_in_lineage(&cluster, leader, &descriptor, range_start, 3).await;
	let (row_accepted, row_reserved, row_unreserved) = (ids[0], ids[1], ids[2]);
	let server = cluster.node(holder).data_server.clone();

	// Accepted before anything moves.
	let accepted_meta = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_through(
			&cluster,
			holder,
			accepted_meta,
			named_rows(&descriptor, &[(row_accepted, "kept")]),
		)
		.await,
		"accepted under the holder",
	);

	// Held before its claim is built: nothing reserved.
	let (reached, release_unreserved) = server.pause_coordinator_for_test(PauseStage::AfterAbsent);
	let unreserved_meta = write_meta(&descriptor, 1, uuid_v7());
	let unreserved_batch = named_rows(&descriptor, &[(row_unreserved, "late")]);
	let unreserved = tokio::spawn({
		let (server, batch) = (server.clone(), unreserved_batch.clone());
		async move { server.table_write_for_test(unreserved_meta, batch).await }
	});
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the unreserved write reached its hold");
	assert!(
		record_of(&cluster, key_of(&descriptor, &unreserved_meta))
			.await
			.is_none()
	);

	// Held with its children written and acknowledged, before Finalize.
	let (reached, release_reserved) = server.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let reserved_meta = write_meta(&descriptor, 1, uuid_v7());
	let reserved_key = key_of(&descriptor, &reserved_meta);
	let reserved_batch = named_rows(&descriptor, &[(row_reserved, "refused")]);
	let reserved = tokio::spawn({
		let (server, batch) = (server.clone(), reserved_batch.clone());
		async move { server.table_write_for_test(reserved_meta, batch).await }
	});
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the reserved write reached its hold");
	let pending = record_of(&cluster, reserved_key).await.expect("reserved");
	assert_eq!(pending.status, ParentStatus::Reserved);
	assert_eq!(
		pending.claim.children[0].lineage.generation,
		before.generation
	);

	// The holder is cut off from its leases; a replica takes the range.
	let peer = server.decision_peer_for_test();
	peer.pause_lease_refresh.store(true, Release);
	peer.pause_takeover.store(true, Release);
	let after = until_taken_over(&cluster, leader, range_start, holder).await;
	assert!(after.generation > before.generation);
	// The holder itself applies the takeover before either write resumes.
	until_taken_over(&cluster, holder, range_start, holder).await;

	// The reserved write: refused, lineage closed, whatever was acknowledged.
	release_reserved.notify_one();
	let (answer, _) = tokio::time::timeout(BOUND, reserved)
		.await
		.expect("the reserved write answered")
		.unwrap()
		.unwrap();
	assert_ne!(
		answer.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED,
		"{answer:?}"
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	let refused = loop {
		let record = record_of(&cluster, reserved_key).await.expect("recorded");
		if record.status != ParentStatus::Reserved {
			break record;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the reserved parent was never resolved"
		);
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	assert!(
		matches!(
			refused.status,
			ParentStatus::Refused {
				cause: RefusalCause::LineageClosed,
				..
			}
		),
		"{:?}",
		refused.status
	);
	// The same identity answers that refusal, at the successor too.
	let again = write_through(&cluster, after.holder.id, reserved_meta, reserved_batch).await;
	assert_eq!(
		again.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"{again:?}"
	);
	assert_eq!(
		record_of(&cluster, reserved_key).await.unwrap().status,
		refused.status
	);

	// The unreserved write: its holder no longer holds the range. Nothing of
	// it was reserved; the same identity commits under the successor.
	until_leased_by(&cluster, holder, range_start, after.holder.id).await;
	release_unreserved.notify_one();
	let (answer, _) = tokio::time::timeout(BOUND, unreserved)
		.await
		.expect("the unreserved write answered")
		.unwrap()
		.unwrap();
	assert_eq!(
		(answer.write.status, answer.write.leader_hint.id),
		(WriteStatus::NOT_LEADER, after.holder.id),
		"{answer:?}"
	);
	assert_committed(
		&write_through(&cluster, after.holder.id, unreserved_meta, unreserved_batch).await,
		"the unreserved identity under the successor",
	);

	// The accepted write is as it was; the refused write's rows are read nowhere.
	let accepted = record_of(&cluster, key_of(&descriptor, &accepted_meta))
		.await
		.expect("still recorded");
	assert!(matches!(accepted.status, ParentStatus::Accepted { .. }));
	assert_eq!(
		accepted.claim.children[0].lineage.generation,
		before.generation
	);
	let expected: Expected = [
		(row_accepted, "kept".to_string()),
		(row_unreserved, "late".to_string()),
	]
	.into();
	expect_everywhere(&cluster, &descriptor, &expected, "after the takeover").await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A successor that never stored an accepted row deletes it. The row was
/// accepted by its primary and one replica while the future successor refused
/// its copy; the primary stops and that replica-less node takes the range. Its
/// delete does not read the missing copy as absence: it repairs the row from
/// the surviving replica, selects it, and publishes a tombstone above the new
/// generation's floor; a second delete records a no-op under the successor's
/// lineage; no node reads the row afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_successor_lacking_an_accepted_row_repairs_it_before_deleting_it() {
	let (mut cluster, leader, descriptor) = ready().await;
	let (_, primary) = row_off_the_leader(&cluster, leader, &descriptor, 1).await;
	let (range_start, before) = a_lineage_of(&cluster, leader, primary).await;
	let row = rows_in_lineage(&cluster, leader, &descriptor, range_start, 1).await[0];
	let order = candidates_after(&cluster, leader, range_start, primary).await;
	let (successor, survivor) = (order[0], order[1]);
	let key_identity = cluster
		.node(leader)
		.data_server
		.row_identity_and_locality_for_test(
			TenantId::ZERO,
			&descriptor,
			&named_rows(&descriptor, &[(row, "x")]),
		)
		.0;

	// Accepted on the primary and the survivor; the future successor has no copy.
	cluster
		.node(successor)
		.data_server
		.decision_peer_for_test()
		.refuse_replicas
		.store(true, Release);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let written = write_through(
		&cluster,
		primary,
		meta,
		named_rows(&descriptor, &[(row, "doomed")]),
	)
	.await;
	assert_committed(&written, "with the future successor refusing its copy");
	cluster
		.node(successor)
		.data_server
		.decision_peer_for_test()
		.refuse_replicas
		.store(false, Release);
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert!(
		intent_of(&cluster, successor, record.rsv).await.is_none(),
		"the successor-to-be holds no copy"
	);
	assert!(intent_of(&cluster, survivor, record.rsv).await.is_some());

	// The primary stops; the node without the row takes its range.
	cluster.stop(primary, false).await;
	let after = until_taken_over(&cluster, leader, range_start, primary).await;
	assert_eq!(after.holder.id, successor);
	until_leased_by(&cluster, leader, range_start, successor).await;
	assert!(
		intent_of(&cluster, successor, record.rsv).await.is_none(),
		"taking the range repaired nothing by itself"
	);

	// Its delete selects the accepted row, having repaired it from the survivor.
	let delete = delete_meta(&descriptor);
	let deleted = delete_through(&cluster, leader, delete, vec![key_identity]).await;
	assert_eq!(deleted.write.status, WriteStatus::OK, "{deleted:?}");
	assert_eq!(
		deleted.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	assert_eq!(counts(&deleted), [1, 0, 0], "matched, not 'not found'");
	assert!(deleted.write.commit_hlc > written.write.commit_hlc);
	assert!(
		deleted.write.commit_hlc.physical_ms > after.floor_ms,
		"the tombstone is above the generation's floor: {:?} vs {}",
		deleted.write.commit_hlc,
		after.floor_ms
	);
	assert_eq!(
		intent_of(&cluster, successor, record.rsv)
			.await
			.expect("repaired by the delete's evaluation")
			.state,
		crate::storage::index::parent_intents::ParentIntentState::Complete
	);
	let tombstone = record_of(
		&cluster,
		ParentKey {
			tenant: TenantId::ZERO,
			namespace: ParentNamespace::Table(descriptor.table_object_id),
			client_uuid: delete.parent_write_id.client_uuid,
			sequence_no: 1,
		},
	)
	.await
	.expect("the delete is a recorded parent");
	assert!(matches!(tombstone.status, ParentStatus::Accepted { .. }));
	assert_eq!(tombstone.claim.children[0].holder.node, successor);
	assert_eq!(
		tombstone.claim.children[0].lineage.generation,
		after.generation
	);
	assert!(after.generation > before.generation);

	// A second delete finds it already deleted: a no-op, recorded by the
	// successor under its lineage.
	let again = delete_meta(&descriptor);
	let already = delete_through(&cluster, leader, again, vec![key_identity]).await;
	assert_eq!(already.write.status, WriteStatus::OK, "{already:?}");
	assert_eq!(counts(&already), [0, 1, 0]);
	let noop_uuid = again.parent_write_id.client_uuid;
	let table = descriptor.table_object_id;
	let noop = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.noops()
				.values()
				.find(|noop| {
					noop.body.key.client_uuid == noop_uuid
						&& noop.body.key.namespace == ParentNamespace::Table(table)
				})
				.map(|noop| (noop.body.coordinator.node, noop.body.lineage.generation))
		})
		.await;
	assert_eq!(noop, Some((successor, after.generation)));

	// No live node reads the row; nor does the primary once it is back.
	let expected = Expected::new();
	for node in [successor, survivor] {
		expect_at(&cluster, node, &descriptor, &expected, "after the delete").await;
	}
	cluster.reopen(primary).await;
	until_settled(&cluster, &descriptor).await;
	// A reopened member may answer the retryable refusal before its first OK;
	// the bounded read accepts only that, and requires the exact rows.
	assert_eq!(
		cluster.read_once_validated(primary, &descriptor).await,
		expected,
		"the returned primary"
	);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"after the primary's return",
	)
	.await;
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}
