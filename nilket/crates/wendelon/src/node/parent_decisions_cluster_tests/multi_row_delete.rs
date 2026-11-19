//! Wendelon ADR 0112: one delete statement is one parent decision. Three voters
//! under ParentDecisions (full replication): one caller identity, one selection
//! at one cut, one parent, one terminal decision, exact counts and reads on
//! every node; the routing rule (absent at the fallback holder, live elsewhere);
//! the visibility of this parent's rows across Finalize and an abort; a lost
//! answer while Reserved; and the whole-statement bounds.

use super::*;
use crate::consensus::decisions::delete_row_position;

/// The ring position of row `id`'s identity.
fn position_of(descriptor: &TableSchemaDescriptor, row: crate::schema::Identity) -> u64 {
	delete_row_position(TenantId::ZERO, descriptor, row).expect("an identity-placed table")
}

/// The lineage (its start) and holder of `position`, from the leader's state.
async fn lineage_of(cluster: &Cluster, position: u64) -> (u64, u32) {
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
						.map(|(start, lineage)| (*start, lineage.holder.id))
				})
				.expect("every position is in a lineage")
		})
		.await
}

fn identity_of(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	id: i64,
) -> crate::schema::Identity {
	cluster
		.node(leader)
		.data_server
		.row_identity_and_locality_for_test(
			TenantId::ZERO,
			descriptor,
			&named_rows(descriptor, &[(id, "x")]),
		)
		.0
}

fn delete_meta_of(
	descriptor: &TableSchemaDescriptor,
	rows: usize,
) -> crate::schema::TableDeleteRowsRequestMeta {
	let mut meta = delete_meta(descriptor);
	meta.row_count = rows as u32;
	meta
}

fn delete_key(
	descriptor: &TableSchemaDescriptor,
	meta: &crate::schema::TableDeleteRowsRequestMeta,
) -> ParentKey {
	ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: meta.parent_write_id.sequence_no,
	}
}

async fn insert(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	id: i64,
	expected: &mut Expected,
) {
	assert_committed(
		&write_through(
			cluster,
			leader,
			write_meta(descriptor, 1, uuid_v7()),
			named_rows(descriptor, &[(id, "v")]),
		)
		.await,
		"a row",
	);
	expected.insert(id, "v".into());
}

/// Whether anything was recorded for `key`: a parent or a noop.
async fn recorded(cluster: &Cluster, key: ParentKey) -> bool {
	if record_of(cluster, key).await.is_some() {
		return true;
	}
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state.decisions.as_ref().is_some_and(|decisions| {
				decisions.noops().values().any(|noop| noop.body.key == key)
			})
		})
		.await
}

fn assert_delete_committed(response: &crate::schema::TableDeleteRowsResponseMeta, what: &str) {
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

/// Rows at two different holders, one already deleted and one never written,
/// in one statement: one parent, exact counts, exact reads on every node, the
/// same receipt from every node, the same record for the reversed order, and a
/// conflict for another set under that identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_multi_row_delete_across_holders_is_one_parent_with_exact_counts_and_reads() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	// Live rows at at least two holders.
	let mut live = Vec::new();
	let mut holders = std::collections::BTreeSet::new();
	for id in 1..200 {
		let (_, holder) = lineage_of(
			&cluster,
			position_of(&descriptor, identity_of(&cluster, leader, &descriptor, id)),
		)
		.await;
		if live.len() < 4 && (holders.insert(holder) || live.len() >= 2) {
			live.push(id);
		}
		if live.len() == 4 && holders.len() >= 2 {
			break;
		}
	}
	assert!(holders.len() >= 2, "rows at two holders");
	for id in &live {
		insert(&cluster, leader, &descriptor, *id, &mut expected).await;
	}
	for id in [500, 501] {
		insert(&cluster, leader, &descriptor, id, &mut expected).await;
	}
	// 500 is deleted on its own first; 999 is never written.
	let single = delete_through(
		&cluster,
		leader,
		delete_meta(&descriptor),
		vec![identity_of(&cluster, leader, &descriptor, 500)],
	)
	.await;
	assert_delete_committed(&single, "the single-row delete");
	expected.remove(&500);

	let mut ids: Vec<i64> = live.clone();
	ids.extend([500, 999]);
	let rows: Vec<_> = ids
		.iter()
		.map(|id| identity_of(&cluster, leader, &descriptor, *id))
		.collect();
	let meta = delete_meta_of(&descriptor, rows.len());
	let deleted = delete_through(&cluster, leader, meta, rows.clone()).await;
	assert_delete_committed(&deleted, "one multi-row delete");
	assert_eq!(
		counts(&deleted),
		[live.len() as u32, 1, 1],
		"the frozen selection"
	);
	for id in &live {
		expected.remove(id);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "exactly the survivors").await;

	// The same receipt from every node, and for the reversed order.
	for node in NODES {
		let replay = delete_through(&cluster, node, meta, rows.clone()).await;
		assert_eq!(counts(&replay), counts(&deleted), "replay at {node}");
		assert_eq!(
			replay.write.log_index, deleted.write.log_index,
			"replay at {node}"
		);
		assert_eq!(
			replay.write.commit_hlc, deleted.write.commit_hlc,
			"replay at {node}"
		);
	}
	let mut reversed = rows.clone();
	reversed.reverse();
	let replay = delete_through(&cluster, leader, meta, reversed).await;
	assert_eq!(
		replay.write.log_index, deleted.write.log_index,
		"the reversed order"
	);
	// Another set under the same identity.
	let mut fewer = meta;
	fewer.row_count -= 1;
	let other = delete_through(&cluster, leader, fewer, rows[..rows.len() - 1].to_vec()).await;
	assert_eq!(
		other.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{other:?}"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "nothing else changed").await;
}

/// The visibility oracle for one delete statement: paused after its children
/// are written and before Finalize, every node still reads every row of the
/// statement; after Finalize, none of them; an aborted parent changes none.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_delete_statements_rows_appear_gone_together_and_an_abort_changes_none() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	for id in 1..=6 {
		insert(&cluster, leader, &descriptor, id, &mut expected).await;
	}
	let statement = |ids: &[i64]| {
		let rows: Vec<_> = ids
			.iter()
			.map(|id| identity_of(&cluster, leader, &descriptor, *id))
			.collect();
		let lowest = rows
			.iter()
			.map(|row| position_of(&descriptor, *row))
			.min()
			.unwrap();
		(rows, lowest)
	};

	// Committed: all rows, then none.
	let (rows, lowest) = statement(&[1, 2, 3]);
	let (_, coordinator) = lineage_of(&cluster, lowest).await;
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let server = cluster.node(coordinator).data_server.clone();
	let meta = delete_meta_of(&descriptor, rows.len());
	let mut pending = {
		let rows = rows.clone();
		tokio::spawn(async move { server.table_delete_for_test(meta, rows).await })
	};
	until_paused(&reached, &mut pending).await;
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"every row, before Finalize",
	)
	.await;
	release.notify_one();
	let finished = pending.await.unwrap();
	assert_delete_committed(&finished, "the paused statement");
	assert_eq!(counts(&finished), [3, 0, 0]);
	for id in [1, 2, 3] {
		expected.remove(&id);
	}
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"none of them, after Finalize",
	)
	.await;

	// Aborted: no row changes, known not committed.
	let (rows, lowest) = statement(&[4, 5]);
	let (_, coordinator) = lineage_of(&cluster, lowest).await;
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let server = cluster.node(coordinator).data_server.clone();
	let meta = delete_meta_of(&descriptor, rows.len());
	let mut pending = tokio::spawn(async move { server.table_delete_for_test(meta, rows).await });
	until_paused(&reached, &mut pending).await;
	let record = record_of(&cluster, delete_key(&descriptor, &meta))
		.await
		.expect("a reserved parent");
	abort(&cluster, &record).await;
	release.notify_one();
	let refused = pending.await.unwrap();
	assert_eq!(
		refused.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
		"{refused:?}"
	);
	expect_everywhere(&cluster, &descriptor, &expected, "an abort changes no row").await;
}

/// The routing rule (ADR 0112 D2), full replication: the lower requested row is
/// absent and held by A, the live row by B. Entering at A, the statement is sent
/// to B without a proposal, and B commits. All absent, entering at a third node,
/// it is sent to A, which records one noop.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_statement_routes_to_its_signer_without_a_loop() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	// a: absent, at A, the lower position; b: live, at B != A.
	let mut pair = None;
	'search: for a in 1000..1400 {
		let ra = identity_of(&cluster, leader, &descriptor, a);
		let (_, holder_a) = lineage_of(&cluster, position_of(&descriptor, ra)).await;
		for b in 2000..2100 {
			let rb = identity_of(&cluster, leader, &descriptor, b);
			let (_, holder_b) = lineage_of(&cluster, position_of(&descriptor, rb)).await;
			if holder_a != holder_b && position_of(&descriptor, ra) < position_of(&descriptor, rb) {
				pair = Some((a, ra, holder_a, b, rb, holder_b));
				break 'search;
			}
		}
	}
	let (_, ra, holder_a, b, rb, holder_b) =
		pair.expect("an absent row at A below a live row at B");
	insert(&cluster, leader, &descriptor, b, &mut expected).await;
	let meta = delete_meta_of(&descriptor, 2);
	let first = cluster
		.node(holder_a)
		.data_server
		.table_delete_for_test(meta, vec![ra, rb])
		.await;
	assert_eq!(first.write.status, WriteStatus::NOT_LEADER, "{first:?}");
	assert_eq!(first.write.leader_hint.id, holder_b, "sent to the signer");
	assert!(
		!recorded(&cluster, delete_key(&descriptor, &meta)).await,
		"no proposal at A"
	);
	let committed = delete_through(&cluster, holder_a, meta, vec![ra, rb]).await;
	assert_delete_committed(&committed, "committed at B");
	assert_eq!(counts(&committed), [1, 0, 1]);
	expected.remove(&b);
	expect_everywhere(&cluster, &descriptor, &expected, "b deleted").await;

	// All absent, entering at a third node: one noop at A.
	let third = NODES
		.into_iter()
		.find(|node| *node != holder_a && *node != holder_b)
		.unwrap();
	let mut absent_b = None;
	for id in 3000..3400 {
		let row = identity_of(&cluster, leader, &descriptor, id);
		if position_of(&descriptor, row) > position_of(&descriptor, ra) {
			absent_b = Some(row);
			break;
		}
	}
	let rows = vec![ra, absent_b.expect("a higher absent row")];
	let meta = delete_meta_of(&descriptor, 2);
	let first = cluster
		.node(third)
		.data_server
		.table_delete_for_test(meta, rows.clone())
		.await;
	assert_eq!(first.write.status, WriteStatus::NOT_LEADER, "{first:?}");
	assert_eq!(
		first.write.leader_hint.id, holder_a,
		"the fallback holder signs the noop"
	);
	let noop = delete_through(&cluster, third, meta, rows).await;
	assert_delete_committed(&noop, "one noop");
	assert_eq!(counts(&noop), [0, 0, 2]);
	assert!(
		record_of(&cluster, delete_key(&descriptor, &meta))
			.await
			.is_none(),
		"no parent"
	);
	assert!(
		recorded(&cluster, delete_key(&descriptor, &meta)).await,
		"one noop record"
	);
}

/// A lost answer while Reserved: a retry elsewhere is sent to the recorded
/// coordinator, the record's claim is unchanged by the retries, and the
/// coordinator's answer is the recorded one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lost_answer_while_reserved_is_answered_from_the_record() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	for id in 1..=4 {
		insert(&cluster, leader, &descriptor, id, &mut expected).await;
	}
	let rows: Vec<_> = (1..=4)
		.map(|id| identity_of(&cluster, leader, &descriptor, id))
		.collect();
	let lowest = rows
		.iter()
		.map(|row| position_of(&descriptor, *row))
		.min()
		.unwrap();
	let (_, coordinator) = lineage_of(&cluster, lowest).await;
	let (reached, release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let server = cluster.node(coordinator).data_server.clone();
	let meta = delete_meta_of(&descriptor, rows.len());
	let mut pending = {
		let rows = rows.clone();
		tokio::spawn(async move { server.table_delete_for_test(meta, rows).await })
	};
	until_paused(&reached, &mut pending).await;
	let before = record_of(&cluster, delete_key(&descriptor, &meta))
		.await
		.expect("reserved");
	let elsewhere = NODES.into_iter().find(|node| *node != coordinator).unwrap();
	let retry = cluster
		.node(elsewhere)
		.data_server
		.table_delete_for_test(meta, rows.clone())
		.await;
	assert_eq!(retry.write.status, WriteStatus::NOT_LEADER, "{retry:?}");
	assert_eq!(
		retry.write.leader_hint.id, coordinator,
		"only the recorded coordinator"
	);
	release.notify_one();
	let finished = pending.await.unwrap();
	assert_delete_committed(&finished, "the first attempt");
	let again = delete_through(&cluster, coordinator, meta, rows).await;
	assert_eq!(counts(&again), counts(&finished));
	assert_eq!(again.write.commit_hlc, finished.write.commit_hlc);
	let after = record_of(&cluster, delete_key(&descriptor, &meta))
		.await
		.expect("recorded");
	assert_eq!(
		after.claim, before.claim,
		"the recorded claim, never rebuilt"
	);
}

/// The whole-statement bounds: more than 512 rows and duplicates are refused
/// typed with no effect; a statement whose matched rows need more than 64
/// children is refused whole, while one with more rows in fewer lineages fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_bounds_are_whole_statement_refusals_and_the_limit_is_the_child_count() {
	let (cluster, leader, descriptor) = ready().await;
	let refused = |response: &crate::schema::TableDeleteRowsResponseMeta, what: &str| {
		assert_ne!(
			response.write.status,
			WriteStatus::OK,
			"{what}: {response:?}"
		);
		assert_ne!(
			response.write.status,
			WriteStatus::NOT_LEADER,
			"{what}: {response:?}"
		);
		assert_ne!(
			response.write.status,
			WriteStatus::WRITE_STALLED,
			"{what}: {response:?}"
		);
		assert_ne!(
			response.reject_code,
			crate::schema::PbcRejectCode::NONE.0,
			"{what}: typed: {response:?}"
		);
	};
	// 513 rows.
	let rows: Vec<_> = (0..513)
		.map(|id| identity_of(&cluster, leader, &descriptor, id))
		.collect();
	let meta = delete_meta_of(&descriptor, rows.len());
	let response = cluster
		.node(leader)
		.data_server
		.table_delete_for_test(meta, rows)
		.await;
	refused(&response, "513 rows");
	assert!(!recorded(&cluster, delete_key(&descriptor, &meta)).await);
	// Duplicates.
	let row = identity_of(&cluster, leader, &descriptor, 1);
	let meta = delete_meta_of(&descriptor, 2);
	let response = cluster
		.node(leader)
		.data_server
		.table_delete_for_test(meta, vec![row, row])
		.await;
	refused(&response, "a duplicate");
	assert!(!recorded(&cluster, delete_key(&descriptor, &meta)).await);

	// 65 live rows in 65 distinct lineages: refused whole, nothing changes.
	let mut expected = Expected::new();
	let mut spread = Vec::new();
	let mut lineages = std::collections::BTreeSet::new();
	for id in 10_000..40_000 {
		let row = identity_of(&cluster, leader, &descriptor, id);
		let (start, _) = lineage_of(&cluster, position_of(&descriptor, row)).await;
		if lineages.insert(start) {
			spread.push(id);
			if spread.len() == 65 {
				break;
			}
		}
	}
	assert_eq!(spread.len(), 65, "the fixture has more than 64 lineages");
	for id in &spread {
		insert(&cluster, leader, &descriptor, *id, &mut expected).await;
	}
	let rows: Vec<_> = spread
		.iter()
		.map(|id| identity_of(&cluster, leader, &descriptor, *id))
		.collect();
	let meta = delete_meta_of(&descriptor, rows.len());
	let response = delete_through(&cluster, leader, meta, rows).await;
	refused(&response, "65 children");
	assert!(!recorded(&cluster, delete_key(&descriptor, &meta)).await);
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"a refused statement changes nothing",
	)
	.await;

	// 100 live rows in at most 8 lineages: one statement.
	let mut concentrated = Vec::new();
	let mut few = std::collections::BTreeSet::new();
	for id in 50_000..200_000 {
		let row = identity_of(&cluster, leader, &descriptor, id);
		let (start, _) = lineage_of(&cluster, position_of(&descriptor, row)).await;
		if few.contains(&start) || (few.len() < 8 && few.insert(start)) {
			concentrated.push(id);
			if concentrated.len() == 100 {
				break;
			}
		}
	}
	assert_eq!(concentrated.len(), 100);
	for id in &concentrated {
		insert(&cluster, leader, &descriptor, *id, &mut expected).await;
	}
	let rows: Vec<_> = concentrated
		.iter()
		.map(|id| identity_of(&cluster, leader, &descriptor, *id))
		.collect();
	let meta = delete_meta_of(&descriptor, rows.len());
	let response = delete_through(&cluster, leader, meta, rows).await;
	assert_delete_committed(&response, "100 rows in 8 lineages");
	assert_eq!(counts(&response), [100, 0, 0]);
	for id in &concentrated {
		expected.remove(id);
	}
	expect_everywhere(
		&cluster,
		&descriptor,
		&expected,
		"the concentrated statement",
	)
	.await;
}

/// Waits until the coordinator pauses before Finalize; fails at once if the
/// statement ends first (for example, refused before it was ever reserved),
/// and within the test bound if neither happens.
async fn until_paused(
	reached: &tokio::sync::Notify,
	pending: &mut tokio::task::JoinHandle<crate::schema::TableDeleteRowsResponseMeta>,
) {
	tokio::select! {
		() = reached.notified() => {}
		done = pending => panic!("the statement ended before Finalize: {done:?}"),
		() = tokio::time::sleep(BOUND) => panic!("never reached Finalize"),
	}
}

/// The highest stored HLC of `row` in `node`'s complete intents.
async fn stored_hlc(
	cluster: &Cluster,
	node: u32,
	row: crate::schema::Identity,
) -> crate::schema::HybridTimestamp {
	let index = cluster.node(node).slab.persistent_index().unwrap();
	let mut found: Option<crate::schema::HybridTimestamp> = None;
	for (rsv, ordinal) in index
		.parent_intents_in(&[crate::storage::index::parent_intents::ParentIntentState::Complete])
		.await
		.unwrap()
	{
		if let Some(intent) = index.parent_intent(rsv, ordinal).await.unwrap() {
			for version in intent.manifest {
				if version.key_identity == *row.as_bytes() {
					found = Some(found.map_or(version.hlc, |hlc| hlc.max(version.hlc)));
				}
			}
		}
	}
	found.expect("a stored version")
}

/// One row the evaluating node cannot prove makes the whole statement
/// indeterminate: no parent, no noop, and the provable live row stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_unprovable_row_stalls_the_whole_statement() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	for id in [1, 99] {
		insert(&cluster, leader, &descriptor, id, &mut expected).await;
	}
	let (live, lost) = (
		identity_of(&cluster, leader, &descriptor, 1),
		identity_of(&cluster, leader, &descriptor, 99),
	);
	// The entry node evaluates (full replication: it covers every row). It
	// loses its copy of 99, and no peer serves a repair.
	let entry = leader;
	let lost_hlc = stored_hlc(&cluster, entry, lost).await;
	cluster
		.node(entry)
		.slab
		.forget_version_for_test(TenantId::ZERO, lost, lost_hlc);
	for node in NODES.into_iter().filter(|node| *node != entry) {
		cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.refuse_fetches
			.store(true, std::sync::atomic::Ordering::Release);
	}
	let mut rows = vec![live, lost];
	rows.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
	let meta = delete_meta_of(&descriptor, 2);
	let stalled = cluster
		.node(entry)
		.data_server
		.table_delete_for_test(meta, rows)
		.await;
	assert_eq!(
		stalled.write.status,
		WriteStatus::WRITE_STALLED,
		"{stalled:?}"
	);
	assert_eq!(
		stalled.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert!(
		!recorded(&cluster, delete_key(&descriptor, &meta)).await,
		"nothing recorded"
	);
	for node in NODES {
		cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.refuse_fetches
			.store(false, std::sync::atomic::Ordering::Release);
	}
	let elsewhere = NODES.into_iter().find(|node| *node != entry).unwrap();
	expect_at(
		&cluster,
		elsewhere,
		&descriptor,
		&expected,
		"both rows stay",
	)
	.await;
}

/// Rows whose winners are in the certified base (their parents erased by a
/// generation) and a row written after it: one statement deletes all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_statement_deletes_certified_base_rows_and_fresh_rows_together() {
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let mut expected = Expected::new();
	for id in 1..=3 {
		insert(&cluster, leader, &descriptor, id, &mut expected).await;
	}
	let floor = now + 60_000;
	jump_floor(&cluster, floor).await;
	until_active(&cluster, 1).await;
	let leader = cluster.leader().await;
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert_eq!(
			index.base_entries(false).await.unwrap().len(),
			3,
			"at {node}"
		);
	}
	// After the floor, identities are minted above it.
	assert_committed(
		&write_through(
			&cluster,
			leader,
			write_meta(&descriptor, 1, uuid_v7_at(floor + 1)),
			named_rows(&descriptor, &[(4, "v")]),
		)
		.await,
		"a row after the floor",
	);
	expected.insert(4, "v".into());
	let mut rows: Vec<_> = [1, 2, 4, 7]
		.iter()
		.map(|id| identity_of(&cluster, leader, &descriptor, *id))
		.collect();
	rows.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
	let meta = delete_meta_of(&descriptor, rows.len());
	let mut meta = meta;
	meta.parent_write_id.client_uuid = uuid_v7_at(floor + 2);
	let deleted = delete_through(&cluster, leader, meta, rows).await;
	assert_delete_committed(&deleted, "base and fresh rows");
	assert_eq!(counts(&deleted), [3, 0, 1]);
	for id in [1, 2, 4] {
		expected.remove(&id);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "row 3 only").await;
}

/// The replica plan (node ids) of `position` under `rf`, from the leader's
/// epoch.
async fn plan_of(cluster: &Cluster, position: u64) -> Vec<u32> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let (_, config) = state.epoch_history.last_key_value().unwrap();
			let rf = state
				.tenants
				.get(&TenantId::ZERO)
				.unwrap()
				.replication_factor as usize;
			let ring = crate::network::ring::RingState::build(
				&crate::network::ring::membership_from_epoch_config(config),
				rf,
			);
			ring.replica_segments()
				.into_iter()
				.find(|segment| segment.start <= position && position <= segment.end)
				.unwrap()
				.replicas
				.iter()
				.map(|node| node.id)
				.collect()
		})
		.await
}

/// The placement of row `id`: (identity, position, holder, plan).
async fn placed(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	id: i64,
) -> (crate::schema::Identity, u64, u32, Vec<u32>) {
	let row = identity_of(cluster, leader, descriptor, id);
	let position = position_of(descriptor, row);
	let (_, holder) = lineage_of(cluster, position).await;
	(row, position, holder, plan_of(cluster, position).await)
}

/// Wendelon ADR 0112 D4, a tenant at rf 2 of three members. Unsupported: the
/// absent row a's plan is {A, F}, the live row b's holder B is not in it, and A
/// covers both. Entering at A, A evaluates, and B cannot sign: unsupported,
/// typed, nothing recorded. Entering at B, which does not cover a, the
/// statement goes to a's holder, which ends the same way. Served: a pair whose
/// signer covers both rows commits, entering at a node that covers neither
/// fully.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn under_partial_replication_a_statement_is_served_or_refused_by_the_one_predicate() {
	let (cluster, leader, descriptor) = ready_with(Cluster::start_with(|_, cfg| {
		cfg.rf = 2;
		None
	}))
	.await;
	let mut expected = Expected::new();
	let unsupported = |response: &crate::schema::TableDeleteRowsResponseMeta, what: &str| {
		assert_eq!(
			response.write.status,
			WriteStatus::INTERNAL_ERROR,
			"{what}: {response:?}"
		);
		assert_eq!(
			response.reject_code,
			crate::schema::PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED.0,
			"{what}: {response:?}"
		);
	};
	let mut rows = Vec::new();
	for id in 1..400 {
		rows.push((id, placed(&cluster, leader, &descriptor, id).await));
	}

	// Unsupported: a's holder F covers both rows, b's holder B is not in a's plan.
	let pair = rows
		.iter()
		.find_map(|(a, (_, position_a, holder_a, plan_a))| {
			rows.iter()
				.find_map(|(b, (_, position_b, holder_b, plan_b))| {
					(a != b
						&& position_a < position_b
						&& !plan_a.contains(holder_b)
						&& plan_b.contains(holder_a))
					.then_some((*a, *b, *holder_a, *holder_b))
				})
		});
	let (a, b, holder_a, holder_b) = pair.expect("an rf-2 pair whose live holder cannot sign");
	insert(&cluster, leader, &descriptor, b, &mut expected).await;
	let mut statement = vec![
		identity_of(&cluster, leader, &descriptor, a),
		identity_of(&cluster, leader, &descriptor, b),
	];
	statement.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
	let meta = delete_meta_of(&descriptor, 2);
	let at_a = cluster
		.node(holder_a)
		.data_server
		.table_delete_for_test(meta, statement.clone())
		.await;
	unsupported(&at_a, "entering at a's holder");
	let at_b = cluster
		.node(holder_b)
		.data_server
		.table_delete_for_test(meta, statement.clone())
		.await;
	assert_eq!(at_b.write.status, WriteStatus::NOT_LEADER, "{at_b:?}");
	assert_eq!(
		at_b.write.leader_hint.id, holder_a,
		"to the fallback holder"
	);
	let through_b = delete_through(&cluster, holder_b, meta, statement).await;
	unsupported(&through_b, "entering at b's holder");
	assert!(
		!recorded(&cluster, delete_key(&descriptor, &meta)).await,
		"no proposal"
	);
	// (A full-table read is not served at rf 2 under ParentDecisions; the stored
	// versions are the oracle.)
	let b_row = identity_of(&cluster, leader, &descriptor, b);
	for node in plan_of(&cluster, position_of(&descriptor, b_row)).await {
		assert_eq!(
			versions_of(&cluster, node, b_row).await,
			1,
			"b stays at {node}"
		);
	}

	// Served: b's holder S covers both rows, a's holder covers both too, and the
	// entry node covers neither fully.
	let served = rows
		.iter()
		.find_map(|(a, (_, position_a, holder_a, plan_a))| {
			rows.iter()
				.find_map(|(b, (_, position_b, holder_b, plan_b))| {
					let entry = NODES.into_iter().find(|node| !plan_a.contains(node))?;
					(a != b
						&& position_a < position_b
						&& plan_a.contains(holder_b)
						&& plan_b.contains(holder_a)
						&& plan_b.contains(holder_b)
						&& *b != pair.unwrap().1)
						.then_some((*a, *b, entry))
				})
		});
	let (a, b, entry) = served.expect("an rf-2 pair its signer covers");
	insert(&cluster, leader, &descriptor, b, &mut expected).await;
	let mut statement = vec![
		identity_of(&cluster, leader, &descriptor, a),
		identity_of(&cluster, leader, &descriptor, b),
	];
	statement.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
	let meta = delete_meta_of(&descriptor, 2);
	let deleted = delete_through(&cluster, entry, meta, statement).await;
	assert_delete_committed(&deleted, "served under rf 2");
	assert_eq!(counts(&deleted), [1, 0, 1]);
	let b_row = identity_of(&cluster, leader, &descriptor, b);
	for node in plan_of(&cluster, position_of(&descriptor, b_row)).await {
		assert_eq!(
			versions_of(&cluster, node, b_row).await,
			2,
			"b and its tombstone at {node}"
		);
	}
	let record = record_of(&cluster, delete_key(&descriptor, &meta))
		.await
		.expect("one parent");
	assert!(
		matches!(record.status, ParentStatus::Accepted { .. }),
		"{record:?}"
	);
}

/// How many versions of `row` `node` holds in complete intents.
async fn versions_of(cluster: &Cluster, node: u32, row: crate::schema::Identity) -> usize {
	let index = cluster.node(node).slab.persistent_index().unwrap();
	let mut count = 0;
	for (rsv, ordinal) in index
		.parent_intents_in(&[crate::storage::index::parent_intents::ParentIntentState::Complete])
		.await
		.unwrap()
	{
		if let Some(intent) = index.parent_intent(rsv, ordinal).await.unwrap() {
			count += intent
				.manifest
				.iter()
				.filter(|version| version.key_identity == *row.as_bytes())
				.count();
		}
	}
	count
}

/// Wendelon ADR 0112 D2, the crossed rf-2 placement (Codex pre-review of
/// f92f9442c): the absent row a holds the lower position, on {A, B} held by A;
/// the live row b is on {B, C} held by B. Only B covers both, so the fallback
/// holder A does not: the statement is unsupported at EVERY entry, before any
/// evaluation, with no durable effect; entering at B commits nothing either.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_crossed_placement_is_refused_alike_at_every_entry() {
	let (cluster, leader, descriptor) = ready_with(Cluster::start_with(|_, cfg| {
		cfg.rf = 2;
		None
	}))
	.await;
	let mut expected = Expected::new();
	let mut rows = Vec::new();
	for id in 1..400 {
		rows.push((id, placed(&cluster, leader, &descriptor, id).await));
	}
	let crossed = rows
		.iter()
		.find_map(|(a, (_, position_a, holder_a, plan_a))| {
			rows.iter()
				.find_map(|(b, (_, position_b, holder_b, plan_b))| {
					let both: Vec<u32> = plan_a
						.iter()
						.copied()
						.filter(|node| plan_b.contains(node))
						.collect();
					(a != b
						&& position_a < position_b
						&& both == vec![*holder_b]
						&& holder_a != holder_b)
						.then_some((*a, *b, *holder_a, *holder_b, plan_a.clone(), plan_b.clone()))
				})
		});
	let (a, b, holder_a, holder_b, plan_a, plan_b) =
		crossed.expect("an rf-2 pair that only the live row's holder covers");
	// The premises, from the placement itself.
	let third = NODES
		.into_iter()
		.find(|node| *node != holder_a && *node != holder_b)
		.unwrap();
	assert!(
		plan_a.contains(&holder_a) && plan_a.contains(&holder_b),
		"a on {{A, B}}"
	);
	assert!(
		plan_b.contains(&holder_b) && plan_b.contains(&third),
		"b on {{B, C}}"
	);
	assert!(!plan_b.contains(&holder_a) && !plan_a.contains(&third));
	let (row_a, row_b) = (
		identity_of(&cluster, leader, &descriptor, a),
		identity_of(&cluster, leader, &descriptor, b),
	);
	assert!(position_of(&descriptor, row_a) < position_of(&descriptor, row_b));
	insert(&cluster, leader, &descriptor, b, &mut expected).await;
	let mut statement = vec![row_a, row_b];
	statement.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
	for entry in [holder_a, holder_b, third] {
		// A fresh, unrecorded identity at each entry.
		let meta = delete_meta_of(&descriptor, 2);
		let answer = cluster
			.node(entry)
			.data_server
			.table_delete_for_test(meta, statement.clone())
			.await;
		assert_eq!(
			answer.write.status,
			WriteStatus::INTERNAL_ERROR,
			"at {entry}: {answer:?}"
		);
		assert_eq!(
			answer.reject_code,
			crate::schema::PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED.0,
			"at {entry}: {answer:?}"
		);
		assert!(
			!recorded(&cluster, delete_key(&descriptor, &meta)).await,
			"nothing recorded at {entry}"
		);
	}
	for node in plan_b {
		assert_eq!(
			versions_of(&cluster, node, row_b).await,
			1,
			"b stays at {node}"
		);
	}
}

/// A TABLE_DELETE frame as the wire delivers it, through the data-frame handler,
/// from a data-node identity; its answer's meta.
async fn wire_delete(
	cluster: &Cluster,
	node: u32,
	meta: &crate::schema::TableDeleteRowsRequestMeta,
	payload: Vec<u8>,
) -> crate::schema::TableDeleteRowsResponseMeta {
	use crate::network::router::DataRingServer;
	let meta_bytes = bytemuck::bytes_of(meta).to_vec();
	let frame = crate::network::core::HrftFrame {
		header: crate::schema::FrameHeader {
			magic: crate::network::core::HRFT_MAGIC,
			version: 1,
			msg_type: crate::schema::MsgType::TABLE_DELETE_REQ.0,
			flags: 0,
			stream_id: 79,
			meta_len: meta_bytes.len() as u32,
			arrow_len: payload.len() as u32,
			trailing_len: 0,
		},
		meta: crate::network::pool::PooledBuf::orphan(meta_bytes),
		arrow_payload: Some(crate::network::pool::PooledBuf::orphan(payload)),
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
	.expect("the delete answers")
	.expect("a response")
	.expect("a response frame");
	bytemuck::pod_read_unaligned(
		&response.meta[..std::mem::size_of::<crate::schema::TableDeleteRowsResponseMeta>()],
	)
}

/// The Arrow payload of a TABLE_DELETE request naming `rows`.
fn delete_payload(rows: &[crate::schema::Identity]) -> Vec<u8> {
	let mut ids = arrow::array::FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
	for row in rows {
		ids.append_value(row.as_bytes()).unwrap();
	}
	let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
		crate::schema::STORAGE_ROW_KEY_ID_COLUMN,
		arrow::datatypes::DataType::FixedSizeBinary(32),
		false,
	)]);
	let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids.finish())]).unwrap();
	crate::storage::multirow::encode_batch(&batch).unwrap()
}

/// Wendelon ADR 0112 at the public ingress: a statement whose DECLARED count is
/// over 512 is refused typed (batch too large) from the meta alone, before its
/// Arrow payload is decoded: a payload that is not Arrow at all gets the same
/// answer as 513 real ids. Nothing is recorded and no row changes. At the bound,
/// the same frame path admits the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_oversized_statement_is_refused_from_its_declared_count_before_decoding() {
	let (cluster, leader, descriptor) = ready().await;
	let mut expected = Expected::new();
	insert(&cluster, leader, &descriptor, 1, &mut expected).await;
	let mut rows: Vec<_> = (1..=513)
		.map(|id| identity_of(&cluster, leader, &descriptor, id))
		.collect();
	rows.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
	let too_large = |response: &crate::schema::TableDeleteRowsResponseMeta, what: &str| {
		assert_eq!(
			response.write.status,
			WriteStatus::PAYLOAD_TOO_LARGE,
			"{what}: {response:?}"
		);
		assert_eq!(
			response.reject_code,
			crate::schema::PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE.0,
			"{what}: {response:?}"
		);
	};
	for (what, payload) in [
		("not Arrow at all", b"not an arrow stream".to_vec()),
		("513 real ids", delete_payload(&rows)),
	] {
		let meta = delete_meta_of(&descriptor, 513);
		let answer = wire_delete(&cluster, leader, &meta, payload).await;
		too_large(&answer, what);
		assert!(
			!recorded(&cluster, delete_key(&descriptor, &meta)).await,
			"{what}: nothing recorded"
		);
	}
	expect_everywhere(&cluster, &descriptor, &expected, "no row changed").await;

	// At the bound the same path admits it: no shape refusal.
	let at_bound = &rows[..512];
	let meta = delete_meta_of(&descriptor, at_bound.len());
	let answer = wire_delete(&cluster, leader, &meta, delete_payload(at_bound)).await;
	assert_ne!(
		answer.write.status,
		WriteStatus::PAYLOAD_TOO_LARGE,
		"{answer:?}"
	);
	assert_eq!(
		answer.reject_code,
		crate::schema::PbcRejectCode::NONE.0,
		"{answer:?}"
	);
}
