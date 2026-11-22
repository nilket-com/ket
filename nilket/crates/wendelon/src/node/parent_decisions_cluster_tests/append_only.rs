//! APPENDONLY-01: an ordinary named AppendOnly table under ParentDecisions, in
//! process on a three-voter cluster. The table has the descriptor Toron gives a
//! table with no PRIMARY KEY: AppendOnly, the default table spread, MultiRow.

use super::*;

pub(super) fn append_only_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0xA0; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"events_append",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(crate::schema::MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// The baseline: one write of two identical-valued rows and a third row to a
/// named AppendOnly table commits, and all three rows are read.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_write_keeps_identical_rows() {
	let (cluster, leader, _) = ready_with(Cluster::start()).await;
	let descriptor = append_only_descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&cluster, &descriptor).await;
	let rows = [(1, "same"), (1, "same"), (2, "other")];
	let response = write_through(
		&cluster,
		leader,
		write_meta(&descriptor, rows.len(), uuid_v7()),
		named_rows(&descriptor, &rows),
	)
	.await;
	eprintln!(
		"APPENDONLY-01 in-process baseline: status {:?} knowledge {:?}",
		response.write.status,
		response.write.mutation_commit_knowledge()
	);
	assert_committed(&response, "the AppendOnly write");
	expect_multiset_everywhere(&cluster, &descriptor, &rows, "the AppendOnly rows").await;
}

/// An answer's rows as a sorted multiset: an AppendOnly table keeps equal rows.
pub(super) fn multiset(
	answer: &crate::network::data_server::reader::TestQueryAnswer,
) -> Vec<(i64, String)> {
	let mut rows = Vec::new();
	for batch in &answer.batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.expect("an id column");
		let values = arrow::compute::cast(
			batch.column_by_name("v").expect("a v column"),
			&DataType::Utf8,
		)
		.unwrap();
		let values = values.as_any().downcast_ref::<StringArray>().unwrap();
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), values.value(row).to_owned()));
		}
	}
	rows.sort();
	rows
}

/// Every node answers OK with exactly `expected`, as a multiset.
async fn expect_multiset_everywhere(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	expected: &[(i64, &str)],
	what: &str,
) {
	let mut expected: Vec<(i64, String)> = expected
		.iter()
		.map(|(id, value)| (*id, (*value).to_owned()))
		.collect();
	expected.sort();
	for node in NODES {
		let answer = read_at(cluster, node, descriptor).await;
		assert_eq!(
			answer.status,
			Some(crate::schema::ReadStatus::OK),
			"{what}: at {node}: {answer:?}"
		);
		assert_eq!(multiset(&answer), expected, "{what}: at {node}");
	}
}

/// A ready cluster with the AppendOnly table applied on every node.
async fn append_only_cluster() -> (Cluster, u32, TableSchemaDescriptor) {
	let (cluster, leader, _) = ready_with(Cluster::start()).await;
	let descriptor = append_only_descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&cluster, &descriptor).await;
	(cluster, leader, descriptor)
}

/// The identity of `meta`'s row at `ordinal` (wendelon ADR 0119 D1).
pub(super) fn identity_of(meta: &TableWriteRequestMeta, ordinal: u32) -> crate::schema::Identity {
	crate::schema::table::parent_decisions_append_only_row_id(
		meta.tenant_id,
		meta.table_object_id,
		meta.parent_write_id.client_uuid,
		meta.parent_write_id.sequence_no,
		ordinal,
	)
}

/// A retry of the same operation and rows commits once (one row set); the same
/// operation with changed rows is a key conflict and writes nothing; identical
/// rows under a distinct operation are kept beside the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn append_only_retries_conflicts_and_distinct_operations() {
	let (cluster, leader, descriptor) = append_only_cluster().await;
	let rows = [(1, "same"), (1, "same")];
	let meta = write_meta(&descriptor, rows.len(), uuid_v7());
	for attempt in ["the write", "its retry"] {
		let response = write_through(&cluster, leader, meta, named_rows(&descriptor, &rows)).await;
		assert!(
			matches!(
				response.write.status,
				WriteStatus::OK | WriteStatus::DUPLICATE
			),
			"{attempt}: {response:?}"
		);
	}
	expect_multiset_everywhere(&cluster, &descriptor, &rows, "after the retry").await;

	let changed = write_through(
		&cluster,
		leader,
		meta,
		named_rows(&descriptor, &[(1, "same"), (1, "changed")]),
	)
	.await;
	assert_eq!(
		changed.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"a changed payload under the same operation: {changed:?}"
	);
	expect_multiset_everywhere(&cluster, &descriptor, &rows, "after the conflict").await;

	let other = write_meta(&descriptor, rows.len(), uuid_v7());
	assert_committed(
		&write_through(&cluster, leader, other, named_rows(&descriptor, &rows)).await,
		"identical rows under a distinct operation",
	);
	expect_multiset_everywhere(
		&cluster,
		&descriptor,
		&[(1, "same"), (1, "same"), (1, "same"), (1, "same")],
		"both operations' rows",
	)
	.await;
}

/// A partially present child is repaired sparsely, as after a holder that began
/// its intent and stored only some rows (a crash mid-append). One store refused
/// the write's replicas; on it, the largest child's intent (its owners) is begun
/// with the manifest a holder computes, and every other row of that child (a
/// non-consecutive set of original ordinals, equal values among them) is stored
/// with its own identity and HLC. A read there repairs from the plan: only the
/// missing rows are appended, and every ordinal is stored exactly once, under its
/// identity and the routing locality that identity determines.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_partially_present_append_only_child_is_repaired_sparsely() {
	use crate::network::data_server::child_writes::{
		RowKeys, child_request_meta, child_versions_keyed, ingest_recovered_rows, manifest_content,
	};
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, descriptor) = append_only_cluster().await;
	let rows: Vec<(i64, &str)> = (0..24)
		.map(|row| {
			if row % 5 == 4 {
				(row, "other")
			} else {
				(1, "same")
			}
		})
		.collect();
	let lacking = *NODES.iter().find(|node| **node != leader).unwrap();
	let peer = |node: u32| cluster.node(node).data_server.decision_peer_for_test();
	// Until the partial state is built, the lacking store takes no replicate (a
	// holder retries the refused one) and the sources serve it no repair fetch, so
	// nothing stores the child before the fixture does (seen under load: two
	// copies of one version, the fixture's and a delivered one).
	let sources: Vec<u32> = NODES
		.iter()
		.copied()
		.filter(|node| *node != lacking)
		.collect();
	for source in &sources {
		peer(*source).refuse_fetches.store(true, Release);
	}
	peer(lacking).refuse_replicas.store(true, Release);
	let meta = write_meta(&descriptor, rows.len(), uuid_v7());
	assert_committed(
		&write_through(&cluster, leader, meta, named_rows(&descriptor, &rows)).await,
		"with one store refusing replicas",
	);
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	let claim = record.claim.clone();
	let hlc = claim.commit_hlc;
	// Each child's original ordinals, by the position of each row's identity.
	let mut by_child = vec![Vec::new(); claim.children.len()];
	for ordinal in 0..rows.len() as u32 {
		let position = crate::consensus::decisions::delete_row_position(
			TenantId::ZERO,
			&descriptor,
			identity_of(&meta, ordinal),
		)
		.unwrap();
		let child = claim
			.children
			.iter()
			.position(|child| child.range_start <= position && position <= child.range_end)
			.expect("every row lies in one child");
		by_child[child].push(ordinal);
	}
	// The largest child the lacking store replicates but does not hold: a holder
	// stores its child through the coordinator's own write, which no replicate
	// refusal stops (the coordinator holds row 0's child).
	let (target, ordinals) = by_child
		.iter()
		.enumerate()
		.filter(|(child, _)| {
			let child = &claim.children[*child];
			child.holder.node != lacking && child.plan.iter().any(|signer| signer.node == lacking)
		})
		.max_by_key(|(_, ordinals)| ordinals.len())
		.map(|(child, ordinals)| (child, ordinals.clone()))
		.unwrap();
	assert!(
		ordinals.len() >= 3,
		"premise: a child of at least 3 rows: {by_child:?}"
	);
	let child_rows: Vec<(i64, &str)> = ordinals.iter().map(|o| rows[*o as usize]).collect();
	let batch = named_rows(&descriptor, &child_rows);
	let request = child_request_meta(
		&claim.key,
		descriptor.table_object_id,
		descriptor.schema_generation,
		batch.num_rows(),
		hlc,
	)
	.unwrap();
	let (_, manifest) = child_versions_keyed(
		&request,
		&batch,
		&descriptor,
		hlc,
		RowKeys::Ordinals(&ordinals),
	)
	.unwrap();
	let child = &claim.children[target];
	assert_eq!(
		manifest_content(&manifest),
		child.content,
		"premise: the holder's manifest"
	);
	let index = cluster.node(lacking).slab.persistent_index().unwrap();
	let begun = index
		.begin_parent_intent(crate::storage::index::parent_intents::ParentIntent {
			tenant: TenantId::ZERO,
			rsv: record.rsv,
			ordinal: target as u16,
			claim_digest: record.claim_digest,
			range_start: child.range_start,
			range_end: child.range_end,
			content: child.content,
			manifest,
			state: crate::storage::index::parent_intents::ParentIntentState::Intent,
		})
		.await
		.unwrap();
	assert!(begun.is_ok(), "premise: the intent begins: {begun:?}");
	// Every other row of the child: non-consecutive original ordinals.
	let present: Vec<u32> = ordinals.iter().step_by(2).copied().collect();
	let subset = named_rows(
		&descriptor,
		&present
			.iter()
			.map(|o| rows[*o as usize])
			.collect::<Vec<_>>(),
	);
	let identities: Vec<_> = present.iter().map(|o| identity_of(&meta, *o)).collect();
	let slab = cluster.node(lacking).slab.clone();
	for ordinal in &ordinals {
		let stored = slab
			.stored_rows_at_version(TenantId::ZERO, identity_of(&meta, *ordinal), hlc, None)
			.await
			.unwrap();
		assert!(
			stored.is_empty(),
			"premise: ordinal {ordinal} absent before the fixture"
		);
	}
	ingest_recovered_rows(
		&slab,
		TenantId::ZERO,
		&descriptor,
		&subset,
		hlc,
		&identities,
	)
	.await
	.unwrap();
	let copies = |ordinal: u32| {
		let slab = slab.clone();
		let identity = identity_of(&meta, ordinal);
		async move {
			slab.stored_rows_at_version(TenantId::ZERO, identity, hlc, None)
				.await
				.unwrap()
		}
	};
	for ordinal in &ordinals {
		let expected = usize::from(present.contains(ordinal));
		assert_eq!(
			copies(*ordinal).await.len(),
			expected,
			"premise: ordinal {ordinal}"
		);
	}
	peer(lacking).refuse_replicas.store(false, Release);
	for source in &sources {
		peer(*source).refuse_fetches.store(false, Release);
	}

	expect_multiset_everywhere(&cluster, &descriptor, &rows, "repaired by the reads").await;
	for ordinal in 0..rows.len() as u32 {
		let identity = identity_of(&meta, ordinal);
		let stored = copies(ordinal).await;
		assert_eq!(stored.len(), 1, "ordinal {ordinal} is stored once");
		let column = |name: &str| stored[0].column_by_name(name).cloned();
		let ids = column(crate::schema::table::STORAGE_ROW_KEY_ID_COLUMN).expect("a key id");
		let ids = ids
			.as_any()
			.downcast_ref::<arrow::array::FixedSizeBinaryArray>()
			.expect("a key id column");
		assert_eq!(
			ids.value(0),
			identity.as_bytes(),
			"ordinal {ordinal}: its identity"
		);
		let locality = column(crate::schema::table::STORAGE_ROW_ROUTING_LOCALITY_COLUMN)
			.expect("a routing locality");
		let locality = locality
			.as_any()
			.downcast_ref::<arrow::array::UInt64Array>()
			.expect("a routing locality column");
		assert_eq!(
			Some(locality.value(0)),
			crate::consensus::decisions::delete_row_locality(TenantId::ZERO, &descriptor, identity)
				.map(|locality| locality.get()),
			"ordinal {ordinal}: placed by its identity"
		);
	}
	eprintln!(
		"APPENDONLY-01 sparse repair: child {target} ordinals {ordinals:?}, present {present:?}"
	);
}

/// Deleting one of two identical-valued rows by its identity removes exactly that
/// one: the delete is placed by the identity (the reducer and every delete child
/// derive its position from it), and its twin and the other row stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deleting_one_of_two_identical_rows_removes_exactly_that_one() {
	let (cluster, leader, descriptor) = append_only_cluster().await;
	let rows = [(1, "same"), (1, "same"), (2, "other")];
	let meta = write_meta(&descriptor, rows.len(), uuid_v7());
	assert_committed(
		&write_through(&cluster, leader, meta, named_rows(&descriptor, &rows)).await,
		"the write",
	);
	let deleted = delete_through(
		&cluster,
		leader,
		delete_meta(&descriptor),
		vec![identity_of(&meta, 0)],
	)
	.await;
	assert_eq!(deleted.write.status, WriteStatus::OK, "{deleted:?}");
	assert_eq!(counts(&deleted), [1, 0, 0], "exactly one row deleted");
	expect_multiset_everywhere(
		&cluster,
		&descriptor,
		&[(1, "same"), (2, "other")],
		"its twin and the other row stay",
	)
	.await;
}

/// An AppendOnly table whose spread factor is 1: every row shares one partition,
/// so a write is one child (the size controls need its whole payload in one).
fn single_child_append_only_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0xA1; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"events_append_wide",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::TableSpreadProjection(
			crate::schema::TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 16,
			},
		),
		RecordLayoutPolicy::MultiRow(crate::schema::MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// Wendelon ADR 0119 D5: the per-child section bound is checked before the
/// Reserve. A write whose one child is near the bound (rows, ordinals, and the
/// repair answer's rows and identities all fit) is accepted; one over it is
/// refused `PAYLOAD_TOO_LARGE` with nothing reserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_write_is_bounded_per_child_before_its_reservation() {
	let (cluster, leader, _) = ready_with(Cluster::start()).await;
	let descriptor = single_child_append_only_descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&cluster, &descriptor).await;
	let wide = "w".repeat(64 * 1024);
	let rows_of =
		|count: i64| -> Vec<(i64, &str)> { (0..count).map(|id| (id, wide.as_str())).collect() };

	let near = rows_of(56);
	let meta = write_meta(&descriptor, near.len(), uuid_v7());
	assert_committed(
		&write_through(&cluster, leader, meta, named_rows(&descriptor, &near)).await,
		"a near-limit child (about 3.5 MiB of rows)",
	);

	let over = rows_of(84);
	let meta = write_meta(&descriptor, over.len(), uuid_v7());
	let refused = write_through(&cluster, leader, meta, named_rows(&descriptor, &over)).await;
	assert_eq!(
		refused.write.status,
		WriteStatus::PAYLOAD_TOO_LARGE,
		"an oversize child (about 5.25 MiB of rows): {refused:?}"
	);
	assert!(
		record_of(&cluster, key_of(&descriptor, &meta))
			.await
			.is_none(),
		"nothing is reserved"
	);
}

/// Wendelon ADR 0119 (acceptance 9): a changed coordinator gives no duplicate
/// and no contradictory decision. The coordinator is the lineage holder of row
/// 0's position, which the operation's identity fixes. While its write is held
/// after the Reserve, the same operation at either other node is redirected to
/// it (`NOT_LEADER`, hinted) and leaves the record unchanged; after the commit,
/// a retry at either other node answers the same commit, and every node reads
/// the rows once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_append_only_retry_at_another_node_is_redirected_and_decided_once() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = append_only_cluster().await;
	let rows = [(1, "same"), (1, "same"), (2, "other")];
	let meta = write_meta(&descriptor, rows.len(), uuid_v7());
	let position = crate::consensus::decisions::delete_row_position(
		TenantId::ZERO,
		&descriptor,
		identity_of(&meta, 0),
	)
	.unwrap();
	let coordinator = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.lineages()
				.range(..=position)
				.next_back()
				.map(|(_, lineage)| lineage.holder.id)
				.unwrap()
		})
		.await;
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::AfterReserve);
	let first = spawn_write(&cluster, coordinator, meta, named_rows(&descriptor, &rows));
	reached.notified().await;
	let reserved = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(reserved.status, ParentStatus::Reserved);
	for other in NODES.into_iter().filter(|node| *node != coordinator) {
		let (retry, _) = cluster
			.node(other)
			.data_server
			.table_write_for_test(meta, named_rows(&descriptor, &rows))
			.await
			.unwrap();
		assert_eq!(retry.write.status, WriteStatus::NOT_LEADER, "{retry:?}");
		assert_eq!(retry.write.leader_hint.id, coordinator);
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
	for other in NODES.into_iter().filter(|node| *node != coordinator) {
		let (retry, _) = cluster
			.node(other)
			.data_server
			.table_write_for_test(meta, named_rows(&descriptor, &rows))
			.await
			.unwrap();
		assert_committed(&retry, "a retry elsewhere after acceptance");
		assert_eq!(retry.write.commit_hlc, reserved.claim.commit_hlc);
	}
	expect_multiset_everywhere(&cluster, &descriptor, &rows, "decided once").await;
}

/// Wendelon ADR 0119 D5 (Codex 01a1f9ca): a near-limit child is repaired over
/// the wire. One child of 79 rows of 64 KiB (its write encoding about 99% of the
/// 5 MiB section, accepted by the pre-Reserve check) is written while one replica
/// refuses replicas; the replica then repairs it from a source, whose answer is
/// encoded, framed and read back through the transport. The answer's encoded
/// rows stay within `repair_rows_bound` of the write and within the section, and
/// every node reads the exact multiset.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_near_limit_append_only_child_is_repaired_over_the_wire() {
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, _) = ready_with(Cluster::start()).await;
	let descriptor = single_child_append_only_descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&cluster, &descriptor).await;
	let wide = "w".repeat(64 * 1024);
	let rows: Vec<(i64, &str)> = (0..79).map(|id| (id, wide.as_str())).collect();
	let meta = write_meta(&descriptor, rows.len(), uuid_v7());
	let position = crate::consensus::decisions::delete_row_position(
		TenantId::ZERO,
		&descriptor,
		identity_of(&meta, 0),
	)
	.unwrap();
	let holder = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.lineages()
				.range(..=position)
				.next_back()
				.map(|(_, lineage)| lineage.holder.id)
				.unwrap()
		})
		.await;
	let lacking = *NODES.iter().find(|node| **node != holder).unwrap();
	let peer = |node: u32| cluster.node(node).data_server.decision_peer_for_test();
	peer(lacking).refuse_replicas.store(true, Release);
	let batch = named_rows(&descriptor, &rows);
	let write_bytes = crate::storage::multirow::encode_batch(&batch)
		.unwrap()
		.len();
	let section = crate::network::core::MAX_SECTION_BYTES as usize;
	let bound = crate::network::data_server::child_writes::repair_rows_bound(
		write_bytes,
		batch.num_columns(),
		rows.len(),
	)
	.unwrap();
	assert!(
		write_bytes * 100 >= section * 98 && bound <= section,
		"premise: a near-limit child ({write_bytes} of {section}; bound {bound})"
	);
	assert_committed(
		&write_through(&cluster, leader, meta, batch).await,
		"a near-limit child with one replica refusing",
	);
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.unwrap();
	assert_eq!(record.claim.children.len(), 1, "premise: one child");
	// Only this child is armed (its claim digest, ordinal 0), before the
	// replica may repair: no other test's fetch can satisfy or perturb the
	// assertion, and the record is taken back below.
	crate::network::data_server::repair::arm_served_fetch(record.claim_digest, 0);
	peer(lacking).refuse_replicas.store(false, Release);
	expect_multiset_everywhere(&cluster, &descriptor, &rows, "repaired over the wire").await;
	let served = crate::network::data_server::repair::take_served_fetch(record.claim_digest, 0);
	eprintln!(
		"near-limit repair: write {write_bytes}, bound {bound}, section {section}, served for this child {served:?}"
	);
	assert!(!served.is_empty(), "the lacking replica fetched this child");
	assert!(
		served
			.iter()
			.all(|bytes| bytes * 100 >= section * 98 && *bytes <= bound && *bytes <= section),
		"every answer for this child is near the limit and within the bound: {served:?}"
	);
}
