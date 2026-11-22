//! Wendelon ADR 0121 S1: a bound ParentDecisions retention window hides governed rows below one
//! certified floor, on three in-process voters sharing one controlled wall clock (ADR 0110).
//! Logical visibility only: a hidden row stays a durable base entry and owes no cleanup debt.

use super::append_only::{append_only_descriptor, identity_of, multiset};
use super::time_advance::{Record, advance_to};
use super::write_deadline::ReleaseOnDrop;
use super::*;
use crate::network::data_server::child_writes::PauseStage;
use crate::node::test_time::TestTime;
use std::collections::BTreeSet;
use std::time::Duration;

/// The frozen Section 8 window.
const WINDOW: Duration = Duration::from_secs(15);
const WINDOW_MS: u64 = 15_000;

/// A ready three-voter cluster on `time`, every member configured with the window, with the
/// PK table of the shared fixture and an AppendOnly table applied.
async fn windowed(time: &TestTime) -> (Cluster, u32, TableSchemaDescriptor, TableSchemaDescriptor) {
	let (cluster, leader, pk) = ready_with({
		let time = time.clone();
		Cluster::start_with(move |_, cfg| {
			cfg.test_time = Some(time.clone());
			cfg.parent_decisions_retention_window = WINDOW;
			None
		})
	})
	.await;
	let append = append_only_descriptor();
	cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&append),
		))
		.await
		.unwrap();
	until_settled(&cluster, &append).await;
	let bound = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().and_then(|d| d.retention()))
		.await;
	assert_eq!(
		bound,
		Some(crate::consensus::decisions::RetentionBinding {
			window_ms: WINDOW_MS
		}),
		"the leader bound its configured window before the first write"
	);
	(cluster, leader, pk, append)
}

/// One write committed through `leader`; its meta.
async fn write_one(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	row: (i64, &str),
	client_uuid: [u8; 16],
) -> TableWriteRequestMeta {
	let meta = write_meta(descriptor, 1, client_uuid);
	assert_committed(
		&write_through(cluster, leader, meta, named_rows(descriptor, &[row])).await,
		&format!("row {row:?}"),
	);
	meta
}

/// Every node answers OK with exactly `expected` from the AppendOnly table.
async fn append_rows_everywhere(
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

/// The retention cut of the current certificate, identical at every node.
async fn certified_cut(
	cluster: &Cluster,
) -> crate::consensus::decisions::generations::RetentionCut {
	let mut cuts = BTreeSet::new();
	for node in NODES {
		let cut = cluster
			.node(node)
			.state_machine
			.read_state(|state| {
				state
					.decisions
					.as_ref()
					.and_then(|d| d.generations().current.as_ref().map(|cert| cert.retention))
			})
			.await
			.expect("a certificate");
		cuts.insert((cut.window_ms, cut.stamp_ms, cut.floor_ms));
	}
	assert_eq!(
		cuts.len(),
		1,
		"every member holds one certified cut: {cuts:?}"
	);
	let (window_ms, stamp_ms, floor_ms) = cuts.into_iter().next().unwrap();
	crate::consensus::decisions::generations::RetentionCut {
		window_ms,
		stamp_ms,
		floor_ms,
	}
}

/// Whether every node's durable active base holds `row` of `table`, and none owes cleanup debt
/// for it.
async fn stored_and_owing_nothing(
	cluster: &Cluster,
	table: TableObjectId,
	row: crate::schema::Identity,
	what: &str,
) -> crate::schema::HybridTimestamp {
	let mut hlcs = BTreeSet::new();
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let entry = index
			.base_entries(false)
			.await
			.unwrap()
			.into_iter()
			.find(|entry| entry.table == table && entry.key_identity == *row.as_bytes())
			.unwrap_or_else(|| panic!("{what}: a durable base entry at {node}"));
		assert!(!entry.tombstone, "{what}: at {node}");
		hlcs.insert(entry.hlc);
		let debts = index.debt_page(None, usize::MAX).await.unwrap();
		assert!(
			!debts
				.iter()
				.any(|(key, _)| key.table == table && key.key_identity == *row.as_bytes()),
			"{what}: no cleanup debt at {node}"
		);
	}
	assert_eq!(hlcs.len(), 1, "{what}: one version everywhere");
	hlcs.into_iter().next().unwrap()
}

/// Runs one bounded certified cleanup step and one debt sweep page at every node.
async fn cleanup_pass(cluster: &Cluster) {
	for node in NODES {
		let server = &cluster.node(node).data_server;
		let _ = server.certified_cleanup_step().await;
		let _ = server.debt_sweep_step().await;
	}
}

async fn shut_down(mut cluster: Cluster, time: &TestTime) {
	for node in cluster.nodes.drain(..) {
		let id = node.local_node.id;
		tokio::time::timeout(Duration::from_secs(30), node.shutdown())
			.await
			.unwrap_or_else(|_| panic!("node {id} did not shut down within its bound"))
			.unwrap_or_else(|error| panic!("node {id} shutdown: {error}"));
	}
	assert!(
		time.abort.latched().is_empty(),
		"{:?}",
		time.abort.latched()
	);
}

/// D2, D4: an inherited AppendOnly winner (a, certified in g1) and a covered intent winner (e,
/// accepted after g1's cut and covered by g2), both older than the window when g2 is proposed,
/// are not served under g2's floor; a younger row (d), the PK key written before g1 and never
/// updated (p1), and a deleted PK key (p2, a winning tombstone) are as before. Every member
/// holds one certified cut; a and e stay durable base entries owing no cleanup debt, through
/// g2's activation and a cleanup pass; a delete of either finds nothing; the backup roster of a
/// governed table is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certified_floor_hides_aged_rows_it_still_stores_and_owes_no_cleanup() {
	let time = TestTime::new();
	let (cluster, leader, pk, append) = windowed(&time).await;
	let t0 = time.clock.now_ms();
	write_one(&cluster, leader, &pk, (1, "p1"), uuid_v7_at(t0)).await;
	write_one(&cluster, leader, &pk, (2, "p2"), uuid_v7_at(t0 + 1)).await;
	let a = write_one(&cluster, leader, &append, (1, "a"), uuid_v7_at(t0 + 2)).await;
	// p2 deleted: its winner is a tombstone.
	let mut delete = delete_meta(&pk);
	delete.parent_write_id.client_uuid = uuid_v7_at(t0 + 3);
	let p2 = cluster
		.node(leader)
		.data_server
		.row_identity_and_locality_for_test(TenantId::ZERO, &pk, &named_rows(&pk, &[(2, "p2")]))
		.0;
	let deleted = delete_through(&cluster, leader, delete, vec![p2]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "p2 deleted");
	// g1 certifies p1, p2's tombstone and a.
	let floor1 = time.clock.now_ms() + 60_000;
	jump_floor(&cluster, floor1).await;
	until_active(&cluster, 1).await;
	let g1 = certified_cut(&cluster).await;
	assert!(g1.floor_ms < t0, "g1 hides none of these rows: {g1:?}");
	append_rows_everywhere(&cluster, &append, &[(1, "a")], "a, under g1").await;
	// e: accepted after g1's cut, so g2's covered intent winner.
	let e = write_one(&cluster, leader, &append, (5, "e"), uuid_v7_at(floor1 + 1)).await;
	let e_written = time.clock.now_ms();
	// The shared clock passes e's time by the window and a step, in restored steps.
	let full: BTreeSet<u32> = NODES.into_iter().collect();
	let mut record = Record::default();
	let target_ms = e_written
		+ WINDOW_MS
		+ crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS)
		+ 1_000;
	advance_to(
		&cluster,
		&time,
		&full,
		target_ms,
		tokio::time::Instant::now() + Duration::from_secs(120),
		&mut record,
	)
	.await
	.unwrap();
	let leader = cluster.leader().await;
	let d = write_one(&cluster, leader, &append, (4, "d"), uuid_v7_at(floor1 + 2)).await;
	// The generation that covers e and d (an age-only one may already have committed while the
	// clock moved, its cut below e); its floor is due by age (a is old).
	let covering = [key_of(&append, &e), key_of(&append, &d)];
	let mut rsv = 0;
	for key in covering {
		rsv = rsv.max(record_of(&cluster, key).await.expect("recorded").rsv);
	}
	jump_floor(&cluster, floor1 + 60_000).await;
	let gid = until_covered(&cluster, rsv).await;
	assert!(gid >= 2, "g1 did not cover e");
	let cut = certified_cut(&cluster).await;
	assert_eq!(cut.window_ms, WINDOW_MS);
	let floor = crate::schema::HybridTimestamp {
		physical_ms: cut.floor_ms,
		logical: 0,
		node_id: 0,
	};
	let a_hlc =
		stored_and_owing_nothing(&cluster, append.table_object_id, identity_of(&a, 0), "a").await;
	let e_hlc =
		stored_and_owing_nothing(&cluster, append.table_object_id, identity_of(&e, 0), "e").await;
	let d_hlc =
		stored_and_owing_nothing(&cluster, append.table_object_id, identity_of(&d, 0), "d").await;
	assert!(
		a_hlc < floor && e_hlc < floor,
		"a and e below the floor: {a_hlc:?} {e_hlc:?} {floor:?}"
	);
	assert!(d_hlc >= floor, "d at or above it: {d_hlc:?} {floor:?}");
	// Logical: only d is served; the PK table is unchanged.
	append_rows_everywhere(&cluster, &append, &[(4, "d")], "d only, under g2").await;
	let mut pk_expected = Expected::new();
	pk_expected.insert(1, "p1".into());
	expect_everywhere(&cluster, &pk, &pk_expected, "p1 kept, p2 still deleted").await;
	// Physical: nothing reclaimed through a cleanup pass.
	cleanup_pass(&cluster).await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&a, 0),
		"a after cleanup",
	)
	.await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&e, 0),
		"e after cleanup",
	)
	.await;
	append_rows_everywhere(&cluster, &append, &[(4, "d")], "d only, after cleanup").await;
	// A delete evaluates a hidden row as the read does.
	let mut hidden = delete_meta(&append);
	// Minted above the decision floor the covering jump set.
	hidden.parent_write_id.client_uuid = uuid_v7_at(floor1 + 60_000 + 1);
	let mut rows = vec![identity_of(&a, 0), identity_of(&e, 0)];
	rows.sort_unstable_by(|x, y| x.as_bytes().cmp(y.as_bytes()));
	hidden.row_count = rows.len() as u32;
	let answer = delete_through(&cluster, leader, hidden, rows).await;
	assert_eq!(
		counts(&answer),
		[0, 0, 2],
		"hidden rows are not found: {answer:?}"
	);
	// The roster of a governed table is refused: an export cannot carry hidden rows.
	let token = begin_hold_through_raft(&cluster, 0x0121).await;
	let roster = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(move |state| crate::logical_backup::roster::roster_at(state, token))
		.await;
	assert_eq!(
		roster.err(),
		Some(
			crate::logical_backup::roster::RosterRefusal::RetentionActive {
				tenant_id: TenantId::ZERO,
				table_object_id: append.table_object_id,
			}
		),
	);
	release_hold_through_raft(&cluster, token).await;
	eprintln!(
		"ADR0121 floor control: cut {cut:?}, a {a_hlc:?}, e {e_hlc:?}, d {d_hlc:?}, record {record:?}"
	);
	shut_down(cluster, &time).await;
}

/// The current certificate's least visible governed row, at the leader.
async fn certified_oldest(cluster: &Cluster) -> Option<crate::schema::HybridTimestamp> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state.decisions.as_ref().and_then(|d| {
				d.generations()
					.current
					.as_ref()
					.and_then(|cert| cert.oldest_visible)
			})
		})
		.await
}

/// Advances the shared clock to `target_ms` in restored steps.
async fn advance(cluster: &Cluster, time: &TestTime, target_ms: u64) -> Record {
	let full: BTreeSet<u32> = NODES.into_iter().collect();
	let mut record = Record::default();
	advance_to(
		cluster,
		time,
		&full,
		target_ms,
		tokio::time::Instant::now() + Duration::from_secs(120),
		&mut record,
	)
	.await
	.unwrap();
	record
}

/// D3: an idle store. One governed row r is certified in g1 as its least visible row; then only
/// the shared clock moves. An age-only generation commits whose floor passes r: r is no longer
/// served, stays a durable base entry owing no debt, and the new certificate agrees there is no
/// visible governed row, so the trigger proposes nothing more however late it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_idle_store_hides_an_aged_row_by_the_clock_alone() {
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed(&time).await;
	let r = write_one(
		&cluster,
		leader,
		&append,
		(1, "r"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	jump_floor(&cluster, time.clock.now_ms() + 60_000).await;
	until_active(&cluster, 1).await;
	let r_hlc = stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&r, 0),
		"r in g1",
	)
	.await;
	assert_eq!(
		certified_oldest(&cluster).await,
		Some(r_hlc),
		"r is g1's least visible row"
	);
	append_rows_everywhere(&cluster, &append, &[(1, "r")], "r, under g1").await;
	// No write: only the clock passes r's time by the window and a step.
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	let record = advance(
		&cluster,
		&time,
		r_hlc.physical_ms + WINDOW_MS + step + 1_000,
	)
	.await;
	until_active(&cluster, 2).await;
	let cut = certified_cut(&cluster).await;
	assert!(
		cut.stamp_ms > 0 && cut.floor_ms > r_hlc.physical_ms,
		"{cut:?} {r_hlc:?}"
	);
	append_rows_everywhere(&cluster, &append, &[], "r hidden under g2").await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&r, 0),
		"r in g2",
	)
	.await;
	cleanup_pass(&cluster).await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&r, 0),
		"r after cleanup",
	)
	.await;
	assert_eq!(
		certified_oldest(&cluster).await,
		None,
		"no visible governed row"
	);
	let leader = cluster.leader().await;
	let far = time.clock.now_ms() + 1_000 * WINDOW_MS;
	let proposed = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let applied = state.last_applied_log_id.map_or(0, |id| id.index);
			crate::consensus::decisions::generations::next_target(state, applied, Some(far))
		})
		.await;
	assert_eq!(proposed, None, "nothing more is due");
	eprintln!("ADR0121 idle control: cut {cut:?}, r {r_hlc:?}, record {record:?}");
	shut_down(cluster, &time).await;
}

/// D4: a late write. Row x is Reserved with its HLC, held before Finalize while the clock passes
/// it by the window and a step and an age-only generation (g2) commits with a floor above it; its
/// cut stays below x's reservation. Released, x is Accepted strictly after g2's cut with an HLC
/// below g2's floor: it is answered committed, not served before a generation covers it, its
/// retry under the same identity is the same Accepted outcome and writes nothing new, and once
/// g3 covers it, it is a durable base entry owing no debt and still not served.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_late_write_below_the_active_floor_keeps_its_receipt_and_is_not_served() {
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed(&time).await;
	let floor1 = time.clock.now_ms() + 60_000;
	write_one(
		&cluster,
		leader,
		&append,
		(1, "a"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	jump_floor(&cluster, floor1).await;
	until_active(&cluster, 1).await;
	// x, held before Finalize at whichever node coordinates it.
	let mut held = Vec::new();
	for node in NODES {
		held.push(
			cluster
				.node(node)
				.data_server
				.pause_coordinator_for_test(PauseStage::BeforeFinalize),
		);
	}
	let x_meta = write_meta(&append, 1, uuid_v7_at(floor1 + 1));
	let x_rows = named_rows(&append, &[(7, "x")]);
	let mut entry = leader;
	let (mut write, coordinator) = 'sent: loop {
		let mut write = spawn_write(&cluster, entry, x_meta, x_rows.clone());
		loop {
			tokio::select! {
				_ = held[0].0.notified() => break 'sent (write, NODES[0]),
				_ = held[1].0.notified() => break 'sent (write, NODES[1]),
				_ = held[2].0.notified() => break 'sent (write, NODES[2]),
				answer = &mut write => {
					let answer = answer.unwrap();
					assert_eq!(answer.write.status, WriteStatus::NOT_LEADER, "{answer:?}");
					entry = answer.write.leader_hint.id;
					break;
				}
			}
		}
	};
	for node in NODES {
		if node != coordinator {
			cluster.node(node).data_server.clear_pauses_for_test();
		}
	}
	let release = ReleaseOnDrop(
		held[NODES.iter().position(|n| *n == coordinator).unwrap()]
			.1
			.clone(),
	);
	let x_key = key_of(&append, &x_meta);
	let reserved = record_of(&cluster, x_key).await.expect("x reserved");
	assert_eq!(
		reserved.status,
		crate::consensus::decisions::ParentStatus::Reserved
	);
	let x_hlc = reserved.claim.commit_hlc;
	// The clock passes x by the window and a step; g2 commits, its cut below x.
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		x_hlc.physical_ms + WINDOW_MS + step + 1_000,
	)
	.await;
	until_active(&cluster, 2).await;
	let cut = certified_cut(&cluster).await;
	let (t2, floor) = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(|state| {
			let generations = state.decisions.as_ref().unwrap().generations();
			(generations.fr(), generations.retention_floor())
		})
		.await;
	assert!(
		t2 < reserved.rsv,
		"g2's cut {t2} is below x's reservation {}",
		reserved.rsv
	);
	let floor = floor.expect("a positive floor");
	assert!(
		x_hlc < floor,
		"x's HLC {x_hlc:?} below g2's floor {floor:?}"
	);
	// Released: Accepted strictly after g2's cut, answered committed.
	drop(release);
	let answer = tokio::time::timeout(BOUND, &mut write)
		.await
		.unwrap()
		.unwrap();
	assert_committed(&answer, "x, released");
	let accepted = record_of(&cluster, x_key).await.expect("x recorded");
	let crate::consensus::decisions::ParentStatus::Accepted { dec } = accepted.status else {
		panic!("x accepted: {accepted:?}");
	};
	assert!(dec > t2, "accepted after g2's cut");
	// A fresh row above the floor is served; x and a are not.
	let leader = cluster.leader().await;
	write_one(&cluster, leader, &append, (8, "y"), uuid_v7_at(floor1 + 2)).await;
	append_rows_everywhere(
		&cluster,
		&append,
		&[(8, "y")],
		"before a generation covers x",
	)
	.await;
	// The same identity again: the recorded Accepted outcome, nothing new written.
	assert_committed(
		&write_through(&cluster, leader, x_meta, x_rows.clone()).await,
		"x retried",
	);
	assert_eq!(
		record_of(&cluster, x_key).await.map(|r| r.rsv),
		Some(reserved.rsv)
	);
	append_rows_everywhere(&cluster, &append, &[(8, "y")], "after x's retry").await;
	// A later generation covers x: stored, owing no debt, still not served.
	jump_floor(&cluster, floor1 + 60_000).await;
	until_covered(&cluster, reserved.rsv).await;
	assert!(certified_cut(&cluster).await.floor_ms >= cut.floor_ms);
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&x_meta, 0),
		"x in g3",
	)
	.await;
	cleanup_pass(&cluster).await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&x_meta, 0),
		"x after cleanup",
	)
	.await;
	append_rows_everywhere(&cluster, &append, &[(8, "y")], "under g3").await;
	shut_down(cluster, &time).await;
}

/// Waits until a committed generation's cut covers reservation `rsv` and every node activated
/// it; its gid.
async fn until_covered(cluster: &Cluster, rsv: u64) -> u64 {
	cluster
		.until("a generation covering the reservation", move |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().fr() >= rsv)
		})
		.await;
	let gid = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current_gid()
		})
		.await;
	until_active(cluster, gid).await;
	gid
}

/// D1: a member reopened with another window than the store's binding neither serves nor signs:
/// its readiness names the mismatch, its reads are refused, and the all-member generation does
/// not commit without its report, while writes follow the quorum of the others. Reopened with
/// the bound window, it serves again and the generation commits. Nothing is rebound.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_member_installed_with_another_window_neither_serves_nor_signs() {
	let time = TestTime::new();
	let (mut cluster, leader, _, append) = windowed(&time).await;
	// A row written while every member serves.
	let t = time.clock.now_ms();
	write_one(&cluster, leader, &append, (1, "w"), uuid_v7_at(t)).await;
	let member = *NODES.iter().find(|id| **id != leader).unwrap();
	cluster.stop(member, false).await;
	// R3: while it is down the leader snapshots and purges its log, so the member is an
	// installed incarnation: it catches up only by the snapshot, binding included.
	// Entries it never received first (decision floors: no child of the member).
	for step in 1..=3 {
		jump_floor(&cluster, t + step).await;
	}
	let at = cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.last_applied
		.unwrap()
		.index;
	cluster
		.node(leader)
		.raft
		.trigger()
		.snapshot()
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.snapshot
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"no snapshot through {at}"
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	cluster
		.node(leader)
		.raft
		.trigger()
		.purge_log(at)
		.await
		.unwrap();
	cluster
		.configs
		.get_mut(&member)
		.unwrap()
		.parent_decisions_retention_window = Duration::ZERO;
	cluster.reopen(member).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(member)
		.raft
		.metrics()
		.borrow()
		.snapshot
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the member never installed the snapshot"
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	let server = cluster.node(member).data_server.clone();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		if server.decision_readiness_reason().await.as_deref() == Some("RetentionWindowMismatch") {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the mismatch is never reported"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	let answer = read_at(&cluster, member, &append).await;
	assert_ne!(
		answer.status,
		Some(crate::schema::ReadStatus::OK),
		"{answer:?}"
	);
	let leader = cluster.leader().await;
	// The generation that would erase w waits for the mismatched member's report.
	jump_floor(&cluster, t + 60_000).await;
	cluster
		.until("a pending target", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|d| d.generations().pending.is_some())
		})
		.await;
	tokio::time::sleep(Duration::from_secs(3)).await;
	let (gid, reported) = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let generations = state.decisions.as_ref().unwrap().generations();
			(
				generations.current_gid(),
				generations
					.pending
					.as_ref()
					.map(|pending| pending.reports.contains_key(&member)),
			)
		})
		.await;
	assert_eq!(
		gid, 0,
		"no generation commits without every member's report"
	);
	assert_eq!(
		reported,
		Some(false),
		"the mismatched member signed no report"
	);
	let bound = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().retention())
		.await;
	assert_eq!(
		bound,
		Some(crate::consensus::decisions::RetentionBinding {
			window_ms: WINDOW_MS
		}),
		"nothing rebound"
	);
	// Reopened with the bound window: it serves and the generation commits.
	cluster.stop(member, false).await;
	cluster
		.configs
		.get_mut(&member)
		.unwrap()
		.parent_decisions_retention_window = WINDOW;
	cluster.reopen(member).await;
	until_active(&cluster, 1).await;
	append_rows_everywhere(&cluster, &append, &[(1, "w")], "served again").await;
	shut_down(cluster, &time).await;
}

/// R2: a member that staged a candidate and was interrupted before its report (a close past
/// its drain bound) reopens unclean and rebuilds the candidate under the frozen target. With its
/// staged evidence intact it reproduces the least visible row, reports it and the generation
/// commits with it. With only that minimum changed on disk (rows, chunks and digests intact), it
/// refuses: nothing is restaged, overwritten or reported, and the generation does not commit.
async fn reopened_with_its_staged_minimum(tampered: bool) {
	let time = TestTime::new();
	let (mut cluster, leader, _, append) = windowed(&time).await;
	let now = time.clock.now_ms();
	write_one(&cluster, leader, &append, (1, "r"), uuid_v7_at(now)).await;
	let member = *NODES.iter().find(|id| **id != leader).unwrap();
	let (reached, _release) = cluster
		.node(member)
		.data_server
		.pause_coordinator_for_test(PauseStage::CertificationBeforeReport);
	jump_floor(&cluster, now + 60_000).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the member's report is held after durable staging");
	let staged = cluster
		.node(member)
		.slab
		.persistent_index()
		.unwrap()
		.base_meta()
		.await
		.unwrap()
		.staged
		.expect("staged before its report");
	assert!(
		staged.oldest_visible.is_some(),
		"r is the staged least visible row"
	);
	// Never released: the close drains for its bound, then aborts the held step.
	let (closed, clean) = cluster.close_with_result(member).await;
	assert!(closed.is_err() && !clean, "an interrupted close");
	let mut expected = staged.clone();
	if tampered {
		let index = crate::storage::index::PersistentLocalIndex::open(
			crate::storage::index::persistent_index_path(&cluster.configs[&member].data_dir),
		)
		.await
		.unwrap();
		index.set_staged_oldest_for_test(None).await.unwrap();
		drop(index);
		expected.oldest_visible = None;
	}
	cluster.reopen(member).await;
	assert!(cluster.node(member).recovered_after_unclean_shutdown());
	if !tampered {
		until_active(&cluster, staged.gid).await;
		let leader = cluster.leader().await;
		let certified = cluster
			.node(leader)
			.state_machine
			.read_state(|state| {
				state.decisions.as_ref().and_then(|d| {
					d.generations()
						.current
						.as_ref()
						.map(|cert| cert.oldest_visible)
				})
			})
			.await;
		assert_eq!(
			certified,
			Some(staged.oldest_visible),
			"reported and agreed"
		);
	} else {
		tokio::time::sleep(Duration::from_secs(5)).await;
		let leader = cluster.leader().await;
		let (gid, reported) = cluster
			.node(leader)
			.state_machine
			.read_state(move |state| {
				let generations = state.decisions.as_ref().unwrap().generations();
				(
					generations.current_gid(),
					generations
						.pending
						.as_ref()
						.map(|pending| pending.reports.contains_key(&member)),
				)
			})
			.await;
		assert!(gid < staged.gid, "the generation did not commit");
		assert_eq!(reported, Some(false), "the member signed nothing");
		let after = cluster
			.node(member)
			.slab
			.persistent_index()
			.unwrap()
			.base_meta()
			.await
			.unwrap()
			.staged;
		assert_eq!(
			after,
			Some(expected),
			"the staged evidence is not overwritten"
		);
		assert_ne!(
			cluster
				.node(member)
				.data_server
				.decision_peer_for_test()
				.built_gid
				.load(std::sync::atomic::Ordering::Acquire),
			staged.gid,
			"the recovered candidate was not re-proved"
		);
	}
	shut_down(cluster, &time).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reopened_member_reports_its_rebuilt_staged_minimum() {
	reopened_with_its_staged_minimum(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reopened_member_refuses_a_staged_minimum_it_cannot_rebuild() {
	reopened_with_its_staged_minimum(true).await;
}
