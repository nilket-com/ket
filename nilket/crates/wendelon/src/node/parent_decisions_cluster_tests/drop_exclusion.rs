//! Wendelon ADR 0115, the replicated controls, on in-process three-voter
//! ParentDecisions clusters (wendelon ADR 0118 D1: one replicated rule).
//! Tables T and U are certified into generation 1; then T's drop is prepared and
//! logically finalized at F.
//!
//! `the_generation_past_a_finalized_drop_excludes_it_and_keeps_every_survivor`:
//! with uncertified Accepted children of both tables at the drop, the next
//! generation commits at a cut `t >= F` that excludes T: T's inherited entries
//! and children are gone, U's inherited entries are byte-identical and its
//! children are added, and only the chunks those entries touch change. Every
//! member holds the same base (the commit also needs agreeing reports). T's
//! inherited entries become `DisplacedBase` debt and its children
//! `RetiredIntent` debt, which cleanup retires (T's descriptor is still
//! present: no settlement). U reads exactly, and the trigger goes quiet.
//!
//! `a_candidate_rebuilt_after_settlement_reproduces_the_one_staged_before`: the
//! leader and a reporter stage and report the excluding target while the third
//! member's certification step is held. Settlement is then simulated on every
//! member (ADR 0116 refuses a real ParentDecisions settlement until its
//! authority exists): the settlement apply's own removals of T's tombstone,
//! reclaim debt and descriptors, in place, made durable on the reporter by a
//! snapshot. The reporter stops cleanly and its staged rows
//! are then damaged; on reopen, the recovered-candidate path rebuilds without
//! them and must reproduce the staged evidence before restoring its rows. (An
//! unclean reopen at that point cannot validate generation 1's base, which
//! still holds T's entries, without T's descriptor: stage (c) settles only
//! after every member activated the excluding generation.) The held
//! member then builds for the first time after settlement, and the generation
//! commits with one base everywhere. A table re-created under T's name with a
//! fresh id is then included by the next generation, which still excludes T.

use super::*;
use crate::storage::index::certified_base::{BaseEntry, DebtSource};

async fn base_of(cluster: &Cluster, node: u32) -> Vec<BaseEntry> {
	let index = cluster.node(node).slab.persistent_index().unwrap();
	index.base_entries(false).await.unwrap()
}

fn of(base: &[BaseEntry], table: TableObjectId) -> Vec<BaseEntry> {
	base.iter()
		.copied()
		.filter(|entry| entry.table == table)
		.collect()
}

struct Certified {
	cluster: Cluster,
	table_t: TableSchemaDescriptor,
	table_u: TableSchemaDescriptor,
	now: u64,
	expected_u: Expected,
}

/// Writes one row `(id, value)` to each of `tables`, its operation stamped
/// after `after_ms` when given (a write below the floor expires).
async fn write_rows(
	cluster: &Cluster,
	tables: &[&TableSchemaDescriptor],
	ids: std::ops::RangeInclusive<i64>,
	value: &str,
	after_ms: Option<u64>,
) {
	for id in ids {
		for descriptor in tables {
			let leader = cluster.leader().await;
			assert_committed(
				&write_through(
					cluster,
					leader,
					write_meta(
						descriptor,
						1,
						after_ms.map_or_else(uuid_v7, |ms| uuid_v7_at(ms + id as u64)),
					),
					named_rows(descriptor, &[(id, value)]),
				)
				.await,
				"a row",
			);
		}
	}
}

/// A ParentDecisions cluster with `rows` rows in each of T and U, certified into
/// generation 1 (policy 1: no drop yet).
async fn certified(rows: i64) -> Certified {
	let (cluster, leader, table_t) = ready_with(Cluster::start_with(|_, _| None)).await;
	let table_u = super::resolve::second_table(&cluster, leader).await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	write_rows(&cluster, &[&table_t, &table_u], 1..=rows, "a", None).await;
	let expected_u = (1..=rows).map(|id| (id, "a".to_string())).collect();
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	Certified {
		cluster,
		table_t,
		table_u,
		now,
		expected_u,
	}
}

/// T's drop: the real Prepare, then the logical finalize; returns its
/// operation id and F, the finalize's index.
async fn drop_logically(
	cluster: &Cluster,
	table_t: &TableSchemaDescriptor,
) -> (crate::integration::compute::DropOperationId, u64) {
	let drop_id = crate::integration::compute::DropOperationId {
		tenant_id: TenantId::ZERO,
		table_object_id: table_t.table_object_id,
		schema_generation: crate::integration::compute::SchemaGeneration::new(
			table_t.schema_generation,
		),
	};
	let leader = cluster.leader().await;
	let prepared = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::TableDropPrepare(
			crate::consensus::TableDropPrepareRequest::current(drop_id, drop_id.schema_generation),
		))
		.await
		.unwrap();
	assert!(
		matches!(prepared.data, crate::consensus::WendelonResponse::Success),
		"{:?}",
		prepared.data
	);
	let finalized = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::TableDropLogicalFinalize(
			crate::consensus::TableDropLogicalFinalizeRequest {
				operation_id: drop_id,
				fence_log_index: prepared.log_id.index,
			},
		))
		.await
		.unwrap();
	assert!(
		matches!(finalized.data, crate::consensus::WendelonResponse::Success),
		"{:?}",
		finalized.data
	);
	(drop_id, finalized.log_id.index)
}

/// The committed cut, and whether the leader's trigger is quiet.
async fn committed_cut(cluster: &Cluster) -> (u64, bool) {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let t = state.decisions.as_ref().unwrap().generations().fr();
			let applied = state.last_applied_log_id.map_or(0, |id| id.index);
			(
				t,
				crate::consensus::decisions::generations::next_target(state, applied, None)
					.is_none(),
			)
		})
		.await
}

fn pause_cleanup(cluster: &Cluster, paused: bool) {
	for node in NODES {
		cluster
			.node(node)
			.data_server
			.decision_peer_for_test()
			.pause_cleanup
			.store(paused, std::sync::atomic::Ordering::Release);
	}
}

/// Waits until each of `nodes` applied through `at` (`Cluster::until` reads
/// only the leader).
async fn until_applied(cluster: &Cluster, nodes: &[u32], at: u64) {
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in nodes {
		while !cluster
			.node(*node)
			.state_machine
			.read_state(move |state| state.last_applied_log_id.is_some_and(|id| id.index >= at))
			.await
		{
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node} never applied {at}"
			);
			tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_generation_past_a_finalized_drop_excludes_it_and_keeps_every_survivor() {
	const ROWS: i64 = 32;
	const CHILDREN: i64 = 4;
	let Certified {
		cluster,
		table_t,
		table_u,
		now,
		mut expected_u,
	} = certified(ROWS).await;
	let before = base_of(&cluster, NODES[0]).await;
	let (t_before, u_before) = (
		of(&before, table_t.table_object_id),
		of(&before, table_u.table_object_id),
	);
	assert_eq!(
		(t_before.len(), u_before.len()),
		(ROWS as usize, ROWS as usize)
	);
	let t_chunks: std::collections::BTreeSet<usize> =
		t_before.iter().map(BaseEntry::chunk).collect();
	assert!(
		u_before
			.iter()
			.any(|entry| t_chunks.contains(&entry.chunk())),
		"premise: some chunk holds entries of both tables"
	);
	let chunks_before = cluster
		.node(NODES[0])
		.slab
		.persistent_index()
		.unwrap()
		.base_meta()
		.await
		.unwrap()
		.active_chunks;

	// Accepted children of both tables, not yet certified, at the drop.
	write_rows(
		&cluster,
		&[&table_t, &table_u],
		ROWS + 1..=ROWS + CHILDREN,
		"c",
		Some(now + 60_000),
	)
	.await;
	expected_u.extend((ROWS + 1..=ROWS + CHILDREN).map(|id| (id, "c".to_string())));

	// Cleanup is held on every node so activation's debt stays observable.
	pause_cleanup(&cluster, true);
	let (_, f) = drop_logically(&cluster, &table_t).await;

	jump_floor(&cluster, now + 120_000).await;
	until_active(&cluster, 2).await;
	let (t, quiet) = committed_cut(&cluster).await;
	assert!(t >= f, "the excluding cut {t} is at or past F = {f}");
	assert!(
		quiet,
		"the trigger is quiet once a committed cut excludes the drop"
	);

	let after = base_of(&cluster, NODES[0]).await;
	assert!(
		of(&after, table_t.table_object_id).is_empty(),
		"T is excluded"
	);
	let u_after = of(&after, table_u.table_object_id);
	assert_eq!(
		u_after.len(),
		(ROWS + CHILDREN) as usize,
		"U's children are added"
	);
	for entry in &u_before {
		assert!(
			u_after.contains(entry),
			"U's inherited entry is byte-identical"
		);
	}
	let u_added: Vec<BaseEntry> = u_after
		.iter()
		.copied()
		.filter(|entry| !u_before.contains(entry))
		.collect();
	for node in &NODES[1..] {
		assert_eq!(
			base_of(&cluster, *node).await,
			after,
			"node {node}: the same base"
		);
	}
	let chunks_after = cluster
		.node(NODES[0])
		.slab
		.persistent_index()
		.unwrap()
		.base_meta()
		.await
		.unwrap()
		.active_chunks;
	assert_eq!(chunks_before.len(), chunks_after.len());
	for (chunk, (was, now)) in chunks_before.iter().zip(&chunks_after).enumerate() {
		let removed = t_before
			.iter()
			.filter(|entry| entry.chunk() == chunk)
			.count() as u64;
		let added = u_added
			.iter()
			.filter(|entry| entry.chunk() == chunk)
			.count() as u64;
		assert_eq!(
			now.count,
			was.count - removed + added,
			"chunk {chunk}: count"
		);
		if removed == 0 && added == 0 {
			assert_eq!(now.root, was.root, "chunk {chunk}: untouched");
		} else {
			assert_ne!(now.root, was.root, "chunk {chunk}: changed");
		}
	}

	// T's inherited entries are DisplacedBase debt of generation 2 on every
	// node; its children are RetiredIntent debt where owned; U has none.
	let mut retired_children = std::collections::BTreeSet::new();
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let debt = index.debt_page(None, 1 << 20).await.unwrap();
		for entry in &t_before {
			assert!(
				debt.iter().any(|(key, record)| {
					key.table == entry.table
						&& key.key_identity == entry.key_identity
						&& key.hlc == entry.hlc
						&& record.gid == 2 && record.source
						== DebtSource::DisplacedBase { tombstone: false }
				}),
				"node {node}: T's entry is displaced debt"
			);
		}
		for (key, record) in &debt {
			if key.table == table_t.table_object_id
				&& record.gid == 2
				&& matches!(record.source, DebtSource::RetiredIntent { .. })
			{
				assert!(
					!t_before
						.iter()
						.any(|entry| entry.key_identity == key.key_identity),
					"node {node}: a retired intent names only a child key"
				);
				retired_children.insert(key.key_identity);
			}
		}
		assert!(
			debt.iter()
				.all(|(key, _)| key.table != table_u.table_object_id),
			"node {node}: no debt for U"
		);
	}
	assert_eq!(
		retired_children.len(),
		CHILDREN as usize,
		"every T child is RetiredIntent debt on some member"
	);

	// Sealed and released, cleanup (which visits sealed slabs only) retires T's
	// debt: its content evidence needs T's descriptor, still present.
	for node in NODES {
		cluster.node(node).slab.force_seal_active_for_test().await;
	}
	pause_cleanup(&cluster, false);
	let deadline = tokio::time::Instant::now() + BOUND;
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		while index
			.debt_page(None, 1 << 20)
			.await
			.unwrap()
			.iter()
			.any(|(key, _)| key.table == table_t.table_object_id)
		{
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node}: T's debt never retired"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}
	expect_everywhere(&cluster, &table_u, &expected_u, "U after the exclusion").await;
	eprintln!(
		"ADR 0115 replicated: F = {f}; generation 2 committed at t = {t}; {} T entries displaced, {} T children retired, {} U entries kept, {} U children added",
		t_before.len(),
		retired_children.len(),
		u_before.len(),
		u_added.len()
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_candidate_rebuilt_after_settlement_reproduces_the_one_staged_before() {
	let Certified {
		mut cluster,
		table_t,
		table_u,
		now,
		expected_u,
	} = certified(4).await;
	let u_before = of(&base_of(&cluster, NODES[0]).await, table_u.table_object_id);

	// The third member's certification step is held, so the excluding target
	// cannot commit before it builds.
	let leader = cluster.leader().await;
	let followers: Vec<u32> = NODES.into_iter().filter(|node| *node != leader).collect();
	let (reporter, held) = (followers[0], followers[1]);
	let (reached, release) = cluster.node(held).data_server.pause_coordinator_for_test(
		crate::network::data_server::child_writes::PauseStage::CertificationStep,
	);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the held member reached its certification step");
	let (drop_id, f) = drop_logically(&cluster, &table_t).await;
	jump_floor(&cluster, now + 120_000).await;
	cluster
		.until(
			"the leader and the reporter reported the excluding target",
			move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().pending.as_ref())
					.is_some_and(|pending| {
						pending.target.t >= f
							&& pending.target.survivor_policy == 2
							&& pending.reports.contains_key(&leader)
							&& pending.reports.contains_key(&reporter)
					})
			},
		)
		.await;
	until_applied(&cluster, &NODES, f).await;

	// Settlement, simulated (wendelon ADR 0116 refuses a real ParentDecisions
	// settlement until its authority exists): on every member, the settlement
	// apply's own removals of the tombstone, the reclaim debt and T's
	// descriptors, in place under one write guard.
	let leader = cluster.leader().await;
	let debt = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.table_drop_reclaim_debts
				.get(drop_id.tenant_id, drop_id.table_object_id)
				.cloned()
		})
		.await
		.expect("the logical finalize recorded reclaim debt");
	let settlement = crate::consensus::TableDropSettlementRequest {
		operation_id: drop_id,
		finalize_log_index: debt.finalize_log_index,
		ownership_epoch: debt.ownership_epoch,
		attested_participants: debt.participants.clone(),
	};
	for node in NODES {
		let settled = cluster
			.node(node)
			.state_machine
			.simulate_drop_settlement_for_test(settlement.clone())
			.await;
		assert!(
			matches!(settled, crate::consensus::WendelonResponse::Success),
			"node {node}: {settled:?}"
		);
	}
	let settled_state = cluster
		.node(reporter)
		.state_machine
		.read_state(move |state| {
			(
				state
					.table_drop_tombstones
					.get(drop_id.tenant_id, drop_id.table_object_id)
					.is_some(),
				state
					.table_schemas
					.latest_by_id(drop_id.tenant_id, drop_id.table_object_id)
					.is_some(),
				state.drop_excluded_at(drop_id.tenant_id, drop_id.table_object_id, f),
			)
		})
		.await;
	assert_eq!(
		settled_state,
		(false, false, true),
		"tombstone and descriptor gone, the outcome kept"
	);

	// The reporter's settled state is made durable by a snapshot, so its reopen
	// recovers it. It then stops with its staged rows damaged. Reopened, its recovered candidate is rebuilt from the settled state and
	// must reproduce the staged evidence before its rows are restored. The stop
	// is clean: after an unclean stop, start-up validation must find every
	// active-base entry stored, which needs T's descriptor while generation 1
	// (with T's entries) is still active; that is why stage (c) settles only
	// after every member activated the excluding generation.
	{
		let mut sm = cluster.node(reporter).state_machine.clone();
		let mut builder =
			<crate::consensus::raft_storage::WendelonStateMachine as openraft::storage::RaftStateMachine<
				crate::consensus::WendelonTypeConfig,
			>>::get_snapshot_builder(&mut sm)
			.await;
		openraft::storage::RaftSnapshotBuilder::build_snapshot(&mut builder)
			.await
			.unwrap();
	}
	cluster.stop(reporter, false).await;
	cluster.damage_staged(reporter).await;
	cluster.reopen(reporter).await;
	assert!(!cluster.node(reporter).recovered_after_unclean_shutdown());
	assert_eq!(
		cluster
			.node(reporter)
			.state_machine
			.read_state(move |state| state
				.table_schemas
				.latest_by_id(drop_id.tenant_id, drop_id.table_object_id)
				.is_some())
			.await,
		false,
		"premise: the reopened reporter recovered the settled state"
	);
	{
		let index = cluster.node(reporter).slab.persistent_index().unwrap();
		let deadline = tokio::time::Instant::now() + BOUND;
		while !index.staged_rows_match().await.unwrap() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"the recovered candidate was never reproduced after settlement"
			);
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}
	}

	// The held member builds for the first time after settlement; its report
	// completes the commit.
	release.notify_one();
	until_settled(&cluster, &table_u).await;
	until_active(&cluster, 2).await;
	let (t, quiet) = committed_cut(&cluster).await;
	assert!(t >= f);
	assert!(quiet);
	let base = base_of(&cluster, NODES[0]).await;
	assert!(
		of(&base, table_t.table_object_id).is_empty(),
		"T is excluded"
	);
	assert_eq!(of(&base, table_u.table_object_id), u_before, "U is kept");
	for node in &NODES[1..] {
		assert_eq!(
			base_of(&cluster, *node).await,
			base,
			"node {node}: the same base"
		);
	}
	expect_everywhere(&cluster, &table_u, &expected_u, "U after settlement").await;

	// T's name, re-created under a fresh id, is included by the next
	// generation, which still excludes T.
	let table_v = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x7a; 16]),
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
	.unwrap();
	assert_eq!(
		(table_v.namespace.as_str(), table_v.table_name.as_str()),
		(table_t.namespace.as_str(), table_t.table_name.as_str()),
		"premise: T's name"
	);
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&table_v),
		))
		.await
		.unwrap();
	until_settled(&cluster, &table_v).await;
	write_rows(&cluster, &[&table_v], 1..=2, "v", Some(now + 120_000)).await;
	jump_floor(&cluster, now + 180_000).await;
	until_active(&cluster, 3).await;
	let base = base_of(&cluster, NODES[0]).await;
	assert!(
		of(&base, table_t.table_object_id).is_empty(),
		"T stays excluded"
	);
	assert_eq!(
		of(&base, table_v.table_object_id).len(),
		2,
		"the fresh id is included"
	);
	assert_eq!(of(&base, table_u.table_object_id), u_before, "U is kept");
	for node in &NODES[1..] {
		assert_eq!(
			base_of(&cluster, *node).await,
			base,
			"node {node}: the same base"
		);
	}
	let expected_v: Expected = (1..=2).map(|id| (id, "v".to_string())).collect();
	expect_everywhere(&cluster, &table_v, &expected_v, "the fresh id").await;
	eprintln!(
		"ADR 0115 replicated: {leader}/{reporter} reported before the simulated settlement; {reporter} reproduced its staged candidate on reopen; {held} built after; generation 2 at t = {t} (F = {f}); generation 3 includes the fresh id"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Lets `node` run `steps` more certification steps: each pause reached marks
/// the start of a new step, so the step before it completed.
async fn run_steps(cluster: &Cluster, node: u32, steps: usize) {
	use crate::network::data_server::child_writes::PauseStage;
	for _ in 0..steps {
		let (reached, release) = cluster
			.node(node)
			.data_server
			.pause_coordinator_for_test(PauseStage::CertificationStep);
		tokio::time::timeout(BOUND, reached.notified())
			.await
			.expect("the member reached its next certification step");
		release.notify_one();
	}
}

/// Clears `node`'s derived drop index in place, or rebuilds it, under its state
/// machine's one write guard (so it is serialized with apply, and no entry is
/// lost or replayed); returns the state as it then reads.
async fn set_drop_index(
	cluster: &Cluster,
	node: u32,
	rebuilt: bool,
) -> crate::consensus::raft_storage::SmState {
	let state_machine = &cluster.node(node).state_machine;
	state_machine.set_pd_drop_index_for_test(rebuilt).await;
	state_machine.state_snapshot().await
}

/// Acceptance 7, the member-side refusal: one member holds the excluding target
/// with every prerequisite (signer, index, validated base, predecessor, a
/// buildable candidate) but a state whose own policy at the target's cut
/// disagrees (its derived index cleared; the rule is replicated, so nothing else
/// can make a member disagree). It neither stages nor reports across several full
/// steps and the generation does not commit. The same member with its index
/// rebuilt stages, reports, and the generation commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_member_whose_policy_disagrees_neither_stages_nor_reports_an_excluding_target() {
	use crate::network::data_server::child_writes::PauseStage;
	let Certified {
		cluster,
		table_t,
		now,
		..
	} = certified(4).await;
	let leader = cluster.leader().await;
	let held = *NODES.iter().find(|id| **id != leader).unwrap();
	let other = *NODES
		.iter()
		.find(|id| **id != leader && **id != held)
		.unwrap();
	let (reached, release) = cluster
		.node(held)
		.data_server
		.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the held member reached its certification step");

	let (_, f) = drop_logically(&cluster, &table_t).await;
	jump_floor(&cluster, now + 120_000).await;
	cluster
		.until("the leader and the other member reported", move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().pending.as_ref())
				.is_some_and(|pending| {
					pending.target.t >= f
						&& pending.target.survivor_policy == 2
						&& pending.reports.contains_key(&leader)
						&& pending.reports.contains_key(&other)
				})
		})
		.await;
	let index = cluster.node(held).slab.persistent_index().unwrap();
	let reported = |cluster: &Cluster| {
		let leader_sm = cluster.node(leader).state_machine.clone();
		async move {
			leader_sm
				.read_state(move |state| {
					state
						.decisions
						.as_ref()
						.and_then(|decisions| decisions.generations().pending.as_ref())
						.is_some_and(|pending| pending.reports.contains_key(&held))
				})
				.await
		}
	};

	// The held member itself applied the pending target first.
	let deadline = tokio::time::Instant::now() + BOUND;
	while !cluster
		.node(held)
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.generations().pending.as_ref())
				.is_some_and(|pending| pending.target.t >= f && pending.target.survivor_policy == 2)
		})
		.await
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the held member never applied the pending target"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	// Refusal lane: the member's policy disagrees, every prerequisite present.
	let seeded = set_drop_index(&cluster, held, false).await;
	let pending = seeded
		.decisions
		.as_ref()
		.and_then(|decisions| decisions.generations().pending.clone())
		.expect("premise: the held member holds the pending target");
	assert_eq!(pending.target.survivor_policy, 2, "premise: excluding");
	assert_eq!(
		seeded.survivor_policy_at(pending.target.t),
		1,
		"premise: the member's policy disagrees"
	);
	assert_eq!(
		index.base_meta().await.unwrap().active_gid,
		pending.target.predecessor_gid,
		"premise: the predecessor is active"
	);
	assert!(
		cluster
			.node(held)
			.data_server
			.base_validated_for_test()
			.load(std::sync::atomic::Ordering::Acquire),
		"premise: the base is validated"
	);
	release.notify_one();
	run_steps(&cluster, held, 3).await;
	assert_ne!(
		index
			.base_meta()
			.await
			.unwrap()
			.staged
			.map(|staged| staged.gid),
		Some(pending.target.gid),
		"disagreeing: nothing staged"
	);
	assert!(!reported(&cluster).await, "disagreeing: no report");
	assert_eq!(
		cluster
			.node(leader)
			.state_machine
			.read_state(|state| state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current_gid())
			.await,
		1,
		"disagreeing: no commit"
	);

	// Positive lane: the same member, its index rebuilt.
	set_drop_index(&cluster, held, true).await;
	until_active(&cluster, 2).await;
	assert!(
		index.base_meta().await.unwrap().active_gid >= pending.target.gid,
		"rebuilt: staged, reported and activated"
	);
	let base = base_of(&cluster, NODES[0]).await;
	assert!(
		of(&base, table_t.table_object_id).is_empty(),
		"T is excluded"
	);
	for node in &NODES[1..] {
		assert_eq!(
			base_of(&cluster, *node).await,
			base,
			"node {node}: the same base"
		);
	}
	eprintln!(
		"ADR 0115 member refusal: {held} with a disagreeing policy held policy-2 target gid {} (t = {}) through 3 steps: no stage, no report, no commit; rebuilt: committed",
		pending.target.gid, pending.target.t
	);
}

/// Wendelon stage (c), the legacy-slice discriminator: under ParentDecisions the
/// ADR 0095 reclaim slice is refused at its one entry point
/// (`run_drop_reclaim_slice`, reached by the leader's local call and by the
/// remote frame handler) on every node, and no node records legacy progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_legacy_reclaim_slice_is_refused_under_parent_decisions() {
	use crate::network::data_server::{
		DROP_RECLAIM_MAX_EXAMINED_PER_SLICE, DropReclaimSliceRequest,
	};
	let Certified {
		cluster, table_t, ..
	} = certified(2).await;
	let (drop_id, f) = drop_logically(&cluster, &table_t).await;
	until_applied(&cluster, &NODES, f).await;
	let leader = cluster.leader().await;
	let debt = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.table_drop_reclaim_debts
				.get(drop_id.tenant_id, drop_id.table_object_id)
				.cloned()
		})
		.await
		.expect("premise: reclaim debt");
	for node in NODES {
		let slice = cluster
			.node(node)
			.data_server
			.run_drop_reclaim_slice(DropReclaimSliceRequest {
				operation_id: debt.operation_id,
				finalize_log_index: debt.finalize_log_index,
				ownership_epoch: debt.ownership_epoch,
				examined_budget: DROP_RECLAIM_MAX_EXAMINED_PER_SLICE,
				coordinator: node_id(leader),
			})
			.await;
		assert!(
			slice
				.as_ref()
				.is_err_and(|error| error.kind() == std::io::ErrorKind::Unsupported
					&& error.to_string()
						== crate::network::data_server::PD_LEGACY_DROP_RECLAIM_REFUSED),
			"node {node}: the intended refusal: {slice:?}"
		);
	}
	for node in NODES {
		assert!(
			!cluster
				.node(node)
				.data_server
				.drop_reclaim_recorded_for_test(drop_id)
				.await,
			"node {node}: no legacy reclaim progress"
		);
	}
}

/// Wendelon stage (c), logical-finalize progress: under ParentDecisions a
/// prepared drop still reaches LogicalFinalize unattended (the sweeper's
/// roll-forward). After that a reconcile pass does nothing (no legacy slice, no
/// settlement), and the tombstone, reclaim debt and descriptor stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_prepared_drop_reaches_logical_finalize_unattended_without_legacy_reclaim() {
	let Certified {
		cluster, table_t, ..
	} = certified(2).await;
	let drop_id = crate::integration::compute::DropOperationId {
		tenant_id: TenantId::ZERO,
		table_object_id: table_t.table_object_id,
		schema_generation: crate::integration::compute::SchemaGeneration::new(
			table_t.schema_generation,
		),
	};
	let leader = cluster.leader().await;
	let prepared = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::TableDropPrepare(
			crate::consensus::TableDropPrepareRequest::current(drop_id, drop_id.schema_generation),
		))
		.await
		.unwrap();
	assert!(
		matches!(prepared.data, crate::consensus::WendelonResponse::Success),
		"{:?}",
		prepared.data
	);
	// Unattended: nothing but the sweeper proposes the logical finalize.
	cluster
		.until(
			"the prepared drop is logically finalized unattended",
			move |state| {
				state
					.table_drop_outcomes
					.get(drop_id.tenant_id, drop_id.table_object_id)
					.is_some()
			},
		)
		.await;
	// Every member applied the logical finalize before its catalog is read.
	let leader = cluster.leader().await;
	let f = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			state
				.table_drop_outcomes
				.get(drop_id.tenant_id, drop_id.table_object_id)
				.map(|outcome| outcome.finalize_log_index)
		})
		.await
		.expect("the outcome");
	until_applied(&cluster, &NODES, f).await;
	let pass = cluster
		.node(leader)
		.data_server
		.reconcile_drop_reclaim_once()
		.await;
	assert!(
		matches!(pass, Ok(false)),
		"after the roll-forward a reconcile pass has nothing to do: {pass:?}"
	);
	for node in NODES {
		assert!(
			!cluster
				.node(node)
				.data_server
				.drop_reclaim_recorded_for_test(drop_id)
				.await,
			"node {node}: no legacy reclaim progress"
		);
		let kept = cluster
			.node(node)
			.state_machine
			.read_state(move |state| {
				(
					state
						.table_drop_tombstones
						.get(drop_id.tenant_id, drop_id.table_object_id)
						.is_some(),
					state
						.table_drop_reclaim_debts
						.get(drop_id.tenant_id, drop_id.table_object_id)
						.is_some(),
					state
						.table_schemas
						.latest_by_id(drop_id.tenant_id, drop_id.table_object_id)
						.is_some(),
				)
			})
			.await;
		assert_eq!(kept, (true, true, true), "node {node}: nothing settled");
	}
}
