//! Wendelon ADR 0121 S1: a bound ParentDecisions retention window hides governed rows below one
//! certified floor, on three in-process voters sharing one controlled wall clock (ADR 0110).
//! Logical visibility only: a hidden row stays a durable base entry and owes no cleanup debt.

use super::append_only::{append_only_descriptor, identity_of, multiset};
use super::time_advance::{Record, advance_to};
use super::write_deadline::ReleaseOnDrop;
use super::*;
use crate::network::data_server::child_writes::PauseStage;
use crate::node::test_time::TestTime;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The frozen Section 8 window.
const WINDOW: Duration = Duration::from_secs(15);
const WINDOW_MS: u64 = 15_000;

/// A ready three-voter cluster on `time`, every member configured with the window, with the
/// PK table of the shared fixture and an AppendOnly table applied.
async fn windowed(time: &TestTime) -> (Cluster, u32, TableSchemaDescriptor, TableSchemaDescriptor) {
	windowed_with(time, WINDOW).await
}

/// As [`windowed`], with `window` (ADR 0123: zero for the zero-window controls).
async fn windowed_with(
	time: &TestTime,
	window: Duration,
) -> (Cluster, u32, TableSchemaDescriptor, TableSchemaDescriptor) {
	let (cluster, leader, pk) = ready_with({
		let time = time.clone();
		Cluster::start_with(move |_, cfg| {
			cfg.test_time = Some(time.clone());
			cfg.parent_decisions_retention_window = window;
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
			window_ms: window.as_millis() as u64
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

/// Wendelon ADR 0122: at every node, `row` of `table` is no base entry and is owed as
/// debt labelled aged, at one HLC everywhere; that HLC.
async fn omitted_as_aged(
	cluster: &Cluster,
	table: TableObjectId,
	row: crate::schema::Identity,
	what: &str,
) -> crate::schema::HybridTimestamp {
	let mut hlcs = BTreeSet::new();
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		assert!(
			!index
				.base_entries(false)
				.await
				.unwrap()
				.iter()
				.any(|entry| entry.table == table && entry.key_identity == *row.as_bytes()),
			"{what}: no base entry at {node}"
		);
		let owed: Vec<_> = index
			.debt_page(None, usize::MAX)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, _)| key.table == table && key.key_identity == *row.as_bytes())
			.collect();
		assert_eq!(owed.len(), 1, "{what}: one debt record at {node}: {owed:?}");
		assert!(
			owed[0].1.aged,
			"{what}: its debt is aged at {node}: {owed:?}"
		);
		hlcs.insert(owed[0].0.hlc);
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
/// holds one certified cut. Wendelon ADR 0122 (superseding S1's "stays stored"): a and e leave
/// the base as aged debt at g2's activation while d stays stored owing nothing through a
/// cleanup pass; a delete of either finds nothing; the backup roster of a governed table is
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certified_floor_omits_aged_rows_as_aged_debt_and_serves_the_rest() {
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
	// Wendelon ADR 0123 D2: read e's reservation now. A generation proposed while the clock
	// moves may cover e (its live key keeps a settled receipt), erasing its body.
	let e_rsv = record_of(&cluster, key_of(&append, &e))
		.await
		.expect("e recorded")
		.rsv;
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
	// The generation that covers e and d (one may already have committed while the clock moved;
	// under wendelon ADR 0123 D2 its cut may cover e); its floor is due by age (a is old).
	let d_rsv = record_of(&cluster, key_of(&append, &d))
		.await
		.expect("d recorded")
		.rsv;
	let rsv = e_rsv.max(d_rsv);
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
	// Wendelon ADR 0122: the inherited and the covered aged winners leave the base as aged debt.
	let a_hlc = omitted_as_aged(&cluster, append.table_object_id, identity_of(&a, 0), "a").await;
	let e_hlc = omitted_as_aged(&cluster, append.table_object_id, identity_of(&e, 0), "e").await;
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
	// A cleanup pass reclaims only debt: d stays stored, owing nothing, and served.
	cleanup_pass(&cluster).await;
	stored_and_owing_nothing(
		&cluster,
		append.table_object_id,
		identity_of(&d, 0),
		"d after cleanup",
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
	omitted_as_aged(
		&cluster,
		append.table_object_id,
		identity_of(&r, 0),
		"r in g2",
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
	// The clock passes x by the window and a step; g2 commits, its cut below x. Generation steps
	// are held meanwhile: a samples the age trigger first (it is older than x), and a target
	// proposed at an intermediate step would fix g2's floor between a and x (8/30 under load,
	// `adr0122-stress-cand.log`), a legal floor that is not this control's premise.
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	pause_generations(&cluster, true);
	advance(
		&cluster,
		&time,
		x_hlc.physical_ms + WINDOW_MS + step + 1_000,
	)
	.await;
	assert_eq!(
		cluster
			.node(cluster.leader().await)
			.state_machine
			.read_state(|state| {
				let generations = state.decisions.as_ref().unwrap().generations();
				(generations.current_gid(), generations.pending.is_some())
			})
			.await,
		(1, false),
		"no target sampled the clock during the advance"
	);
	pause_generations(&cluster, false);
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
	// A later generation covers x (wendelon ADR 0122): omitted as aged debt, still not served.
	jump_floor(&cluster, floor1 + 60_000).await;
	until_covered(&cluster, reserved.rsv).await;
	assert!(certified_cut(&cluster).await.floor_ms >= cut.floor_ms);
	omitted_as_aged(
		&cluster,
		append.table_object_id,
		identity_of(&x_meta, 0),
		"x when covered",
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

/// Wendelon ADR 0122 baseline (test-only on `a9d411d4f`): under a certified floor an aged
/// governed row is reclaimed. The floor generation omits it from the certified base, its
/// activation records it as debt at every member, and one cleanup pass removes every local copy
/// while the table still reads exactly as before. On main S1 keeps aged winners stored, so the
/// first assertion (no base entry) fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_aged_row_leaves_the_certified_base_and_is_reclaimed() {
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed(&time).await;
	let export = retention_series(L1_DATA);
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
	let row = identity_of(&r, 0);
	let r_hlc = stored_and_owing_nothing(&cluster, append.table_object_id, row, "r in g1").await;
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		r_hlc.physical_ms + WINDOW_MS + step + 1_000,
	)
	.await;
	until_active(&cluster, 2).await;
	assert!(certified_cut(&cluster).await.floor_ms > r_hlc.physical_ms);
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let in_base = index
			.base_entries(false)
			.await
			.unwrap()
			.into_iter()
			.any(|entry| {
				entry.table == append.table_object_id && entry.key_identity == *row.as_bytes()
			});
		assert!(!in_base, "the aged row left the certified base at {node}");
		let owed = index
			.debt_page(None, usize::MAX)
			.await
			.unwrap()
			.into_iter()
			.any(|(key, _)| {
				key.table == append.table_object_id && key.key_identity == *row.as_bytes()
			});
		assert!(owed, "its activation recorded it as debt at {node}");
	}
	// Wendelon ADR 0122 D3: that debt is labelled aged at every member.
	omitted_as_aged(&cluster, append.table_object_id, row, "r").await;
	// S5b cleans sealed local slabs only: every member seals its active slab first, so the
	// row's copies are eligible; then the cleanup step's own result is checked.
	for node in NODES {
		let server = &cluster.node(node).data_server;
		let sealed = cluster.node(node).slab.force_seal_active_for_test().await;
		let deadline = tokio::time::Instant::now() + BOUND;
		let mut reclaimed = 0;
		loop {
			reclaimed += server.certified_cleanup_step().await.unwrap();
			let copies = cluster
				.node(node)
				.slab
				.version_locations(TenantId::ZERO, row, r_hlc)
				.unwrap();
			if copies.is_empty() {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"its local copies at {node} were never reclaimed (sealed {sealed:?}): {copies:?}"
			);
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		assert!(reclaimed > 0, "the cleanup step reclaimed it at {node}");
	}
	// Wendelon ADR 0122 D4: each member's one local copy is one aged L1 removal, counted once
	// on its installed rewrite (S5b, or the leader's certified LSM first); L2 counts nothing.
	// Each store's own tally (Codex 01a20151 R3); the export moved by at least as much.
	for node in NODES {
		assert_eq!(
			aged_tally(&cluster, &[node], AgedLevel::L1),
			aged_data(1),
			"one aged L1 removal at {node}"
		);
	}
	assert_eq!(
		aged_tally(&cluster, &NODES, AgedLevel::L2),
		AgedRows::default(),
		"no L2 removal here"
	);
	assert!(
		retention_series(L1_DATA) >= export + NODES.len() as u64,
		"exported"
	);
	append_rows_everywhere(&cluster, &append, &[], "the table reads as before").await;
	shut_down(cluster, &time).await;
}

/// A ready windowed cluster whose members also tier to a local-filesystem L2 (the leader's
/// certified tier step reads and rewrites it).
async fn windowed_with_l2(
	time: &TestTime,
) -> (Cluster, u32, TableSchemaDescriptor, TableSchemaDescriptor) {
	let (cluster, leader, pk) = ready_with({
		let time = time.clone();
		Cluster::start_with(move |id, cfg| {
			cfg.test_time = Some(time.clone());
			cfg.parent_decisions_retention_window = WINDOW;
			let root = crate::test_support::TestDir::new(&format!("aged-l2-{id}"));
			cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs {
				root: root.to_path_buf(),
			});
			cfg.tiering.scan_interval = Duration::from_millis(200);
			Some(root)
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
	(cluster, leader, pk, append)
}

/// The exported value of one retention series, exactly as the Section 8 evaluator reads it from
/// the scrape. Wendelon ADR 0122 (Codex 01a20151 R3): a required series that is missing, repeated
/// or not a whole non-negative number fails; absence is not zero. The registry is the process's,
/// and in-process parallel tests add to it: a control judges its own work by its stores' tallies
/// (`aged_rows_pruned_for_test`, recorded at the same call as the export) and asks of the export
/// only that it moved by at least that much.
fn retention_series(series: &str) -> u64 {
	let text = crate::metrics::global().render_prometheus();
	let values: Vec<&str> = text
		.lines()
		.filter_map(|line| {
			let (name, value) = line.rsplit_once(' ')?;
			(name == series).then_some(value)
		})
		.collect();
	assert_eq!(
		values.len(),
		1,
		"exactly one exported sample of {series}: {values:?}"
	);
	let value: f64 = values[0]
		.parse()
		.unwrap_or_else(|_| panic!("{series} = {:?} is not a number", values[0]));
	assert!(
		value.is_finite() && value >= 0.0 && value.fract() == 0.0,
		"{series} = {value}"
	);
	value as u64
}

const L2_DATA: &str = "wendelon_storage_table_retention_l2_rows_pruned_total{kind=\"data\"}";
const L1_DATA: &str = "wendelon_storage_table_retention_l1_rows_pruned_total{kind=\"data\"}";
const L2_TOMBSTONE: &str =
	"wendelon_storage_table_retention_l2_rows_pruned_total{kind=\"tombstone\"}";

use crate::storage::manager::certified_tier::{
	AgedLevel, AgedRows, CertifiedTierCut, CertifiedTierStep,
};

/// The aged rows `nodes`' current stores recorded at `level`, summed.
fn aged_tally(cluster: &Cluster, nodes: &[u32], level: AgedLevel) -> AgedRows {
	nodes.iter().fold(AgedRows::default(), |sum, node| {
		let rows = cluster.node(*node).slab.aged_rows_pruned_for_test(level);
		AgedRows {
			data: sum.data + rows.data,
			tombstone: sum.tombstone + rows.tombstone,
		}
	})
}

const fn aged_data(data: u64) -> AgedRows {
	AgedRows { data, tombstone: 0 }
}

/// Wendelon ADR 0122 D4: the leader's own tiered slabs, with its cleanup paused so a control drives
/// every certified tier step itself:
/// - S2 holds only aged rows (a2, a3): a step retires it;
/// - S1 holds an aged row (a1), a younger surviving neighbour (n) and a younger row deleted next
///   (d, ordinary debt): a step rewrites it by a certified `Tier` to an object holding n alone.
struct AgedL2 {
	time: TestTime,
	cluster: Cluster,
	leader: u32,
	append: TableSchemaDescriptor,
	s1: crate::schema::SlabId,
	s2: crate::schema::SlabId,
	a1: crate::schema::Identity,
	n: crate::schema::Identity,
	d: crate::schema::Identity,
}

async fn aged_l2() -> AgedL2 {
	use std::sync::atomic::Ordering::Release;
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed_with_l2(&time).await;
	let server = cluster.node(leader).data_server.clone();
	server
		.decision_peer_for_test()
		.pause_cleanup
		.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let floor1 = time.clock.now_ms() + 60_000;
	let now = |offset| uuid_v7_at(time.clock.now_ms() + offset);
	let a2 = write_one(&cluster, leader, &append, (2, "a2"), now(0)).await;
	let a3 = write_one(&cluster, leader, &append, (3, "a3"), now(1)).await;
	let s2 = slab.force_seal_active_for_test().await;
	let a1 = write_one(&cluster, leader, &append, (1, "a1"), now(2)).await;
	// The clock passes the aged rows by the window and a step.
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		time.clock.now_ms() + WINDOW_MS + step + 1_000,
	)
	.await;
	assert_eq!(
		cluster.leader().await,
		leader,
		"the leader stays (its own tiered slabs)"
	);
	let n = write_one(&cluster, leader, &append, (4, "n"), now(0)).await;
	let d = write_one(&cluster, leader, &append, (5, "d"), now(1)).await;
	let s1 = slab.force_seal_active_for_test().await;
	assert!(
		s2 < s1,
		"premise: S2 is sealed first, so a step visits it first"
	);
	let mut delete = delete_meta(&append);
	delete.parent_write_id.client_uuid = now(2);
	assert_eq!(
		counts(&delete_through(&cluster, leader, delete, vec![identity_of(&d, 0)]).await),
		[1, 0, 0],
		"d deleted"
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	for id in [s1, s2] {
		while slab.tier_pointer(id).await.is_none() {
			assert!(
				tokio::time::Instant::now() < deadline,
				"slab {id:?} never tiered"
			);
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
	}
	// The covering generation omits a1, a2 and a3 by age; d's version is ordinary debt.
	let mut rsv = 0;
	for key in [key_of(&append, &a1), key_of(&append, &d)] {
		rsv = rsv.max(record_of(&cluster, key).await.expect("recorded").rsv);
	}
	jump_floor(&cluster, floor1 + 60_000).await;
	until_covered(&cluster, rsv).await;
	for row in [&a1, &a2, &a3] {
		omitted_as_aged(
			&cluster,
			append.table_object_id,
			identity_of(row, 0),
			"an aged row",
		)
		.await;
	}
	let index = slab.persistent_index().unwrap();
	let d_debt = index
		.debt_page(None, usize::MAX)
		.await
		.unwrap()
		.into_iter()
		.find(|(key, _)| key.key_identity == *identity_of(&d, 0).as_bytes())
		.expect("d's version is owed");
	assert!(!d_debt.1.aged, "d's debt is ordinary: {d_debt:?}");
	assert_eq!(
		slab.aged_rows_pruned_for_test(AgedLevel::L2),
		AgedRows::default()
	);
	// No later generation while a control drives its steps: under a slow run the window would
	// otherwise age n too, and its replacement would rightly be retired (one more aged row).
	pause_generations(&cluster, true);
	AgedL2 {
		time,
		cluster,
		leader,
		append,
		s1,
		s2,
		a1: identity_of(&a1, 0),
		n: identity_of(&n, 0),
		d: identity_of(&d, 0),
	}
}

/// One certified tier step on `server`'s store, past the paused loop; retried while it finds
/// nothing to do yet.
async fn tier_step(
	server: Arc<crate::network::data_server::WendelonDataServer>,
	committer: crate::storage::RaftCommitter,
) -> std::io::Result<CertifiedTierStep> {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		match server.certified_tier_step_unpaused(&committer).await {
			Ok(
				CertifiedTierStep::Idle
				| CertifiedTierStep::NothingSelected(_)
				| CertifiedTierStep::GenerationMoved { .. },
			) if tokio::time::Instant::now() < deadline => {
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
			other => return other,
		}
	}
}

impl AgedL2 {
	fn slab(&self) -> Arc<crate::storage::SlabManager> {
		self.cluster.node(self.leader).slab.clone()
	}

	/// As the node's own committer is built, with its owner authority (ADR 0725).
	fn committer(&self) -> crate::storage::RaftCommitter {
		let node = self.cluster.node(self.leader);
		crate::storage::RaftCommitter::new(node.raft.clone())
			.with_state_machine(node.state_machine.clone())
			.with_owner_authority(node.data_server.owner_authority())
	}

	/// One step that visits the lowest eligible slab first: S2 while it is left, then S1.
	async fn step(&self) -> std::io::Result<CertifiedTierStep> {
		self.slab().restart_certified_tier_cursor_for_test();
		tier_step(
			self.cluster.node(self.leader).data_server.clone(),
			self.committer(),
		)
		.await
	}

	/// One step, from the lowest eligible slab, not retried.
	async fn step_once(&self) -> std::io::Result<CertifiedTierStep> {
		self.slab().restart_certified_tier_cursor_for_test();
		self.cluster
			.node(self.leader)
			.data_server
			.certified_tier_step_unpaused(&self.committer())
			.await
	}

	/// What this store's own Applied effects have earned, from what is in force: S2's two aged
	/// rows once it is retired, S1's one once its replacement is installed.
	async fn earned(&self) -> AgedRows {
		aged_data(2 * u64::from(self.retired().await) + u64::from(self.rewritten().await.is_some()))
	}

	/// One step, once: nothing is left to do.
	async fn nothing_left(&self) {
		self.slab().restart_certified_tier_cursor_for_test();
		let answer = self
			.cluster
			.node(self.leader)
			.data_server
			.certified_tier_step_unpaused(&self.committer())
			.await;
		assert!(
			matches!(
				answer,
				Ok(CertifiedTierStep::Idle | CertifiedTierStep::NothingSelected(_))
			),
			"nothing left: {answer:?}"
		);
	}

	fn tally(&self) -> AgedRows {
		self.slab().aged_rows_pruned_for_test(AgedLevel::L2)
	}

	/// The slab's committed tier record in the leader's state.
	async fn committed(&self, slab: crate::schema::SlabId) -> Option<String> {
		let owner = self.slab().owner_ref().unwrap();
		self.cluster
			.node(self.leader)
			.state_machine
			.tiered_slab(owner.slab(slab))
			.await
			.map(|tier| tier.l2_uri)
	}

	/// S2 retired: no local pointer and no committed one.
	async fn retired(&self) -> bool {
		self.slab().tier_pointer(self.s2).await.is_none() && self.committed(self.s2).await.is_none()
	}

	/// S1's local pointer, once it names a certified object: the replacement.
	async fn rewritten(&self) -> Option<crate::storage::TieredSlabPointer> {
		self.slab()
			.tier_pointer(self.s1)
			.await
			.filter(|pointer| pointer.l2_uri.contains("certified/"))
	}

	async fn until(&self, what: &str, done: impl AsyncFn() -> bool) {
		let deadline = tokio::time::Instant::now() + BOUND;
		while !done().await {
			assert!(tokio::time::Instant::now() < deadline, "never: {what}");
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}

	/// The row identities `pointer`'s object physically holds, decoded from L2 (R2).
	async fn object_rows(
		&self,
		pointer: &crate::storage::TieredSlabPointer,
	) -> BTreeSet<crate::schema::Identity> {
		self.slab()
			.tiered_object_identities_for_test(pointer)
			.await
			.unwrap()
			.into_iter()
			.collect()
	}

	/// S1's replacement: its local and committed pointers name the same certified object, which
	/// holds exactly n (a1 and d are gone from the bytes, not only from reads).
	async fn s1_holds_only_n(&self) {
		let replacement = self.rewritten().await.expect("S1 rewritten");
		assert_eq!(
			self.committed(self.s1).await.as_deref(),
			Some(replacement.l2_uri.as_str()),
			"the committed pointer is the replacement"
		);
		assert_eq!(
			self.object_rows(&replacement).await,
			BTreeSet::from([self.n]),
			"the replacement object holds n alone"
		);
	}
}

/// Wendelon ADR 0122 D4, R1/R2 (Codex 01a20151), driven step by step:
/// - an uncommitted rewrite (cut after its upload) earns nothing and installs nothing;
/// - S2's own Applied `Retire` earns its two aged rows; no pointer remains, local or committed;
/// - S1's own Applied `Tier` earns its one aged row, never d's ordinary one; its source object
///   held a1, n and d, and the committed replacement holds n alone;
/// - nothing is left, and nothing more is earned; the export moved by at least the store's own
///   three; a clean reopen of the leader replays the log and its new store earns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn aged_rows_leave_the_leaders_tiered_objects_by_their_own_applied_effects() {
	let mut fx = aged_l2().await;
	let slab = fx.slab();
	let export = retention_series(L2_DATA);
	let _ = retention_series(L2_TOMBSTONE);
	// Read before any step can publish (R2).
	let source = slab.tier_pointer(fx.s1).await.unwrap();
	assert_eq!(
		fx.object_rows(&source).await,
		BTreeSet::from([fx.a1, fx.n, fx.d]),
		"S1's source object"
	);
	// The rewrite cut after its upload; S2 (the lowest slab) is retired first. After every step
	// the tally is exactly what this store's own Applied effects earned so far.
	slab.cut_next_certified_tier_step(CertifiedTierCut::AfterUpload);
	let deadline = tokio::time::Instant::now() + BOUND;
	while slab.certified_tier_cut_pending_for_test().is_some() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"the rewrite never uploaded"
		);
		let _ = fx.step_once().await;
		assert_eq!(fx.tally(), fx.earned().await, "only Applied effects earn");
	}
	assert_eq!(
		slab.tier_pointer(fx.s1).await.as_ref(),
		Some(&source),
		"an uploaded, unsubmitted rewrite installs nothing"
	);
	assert_eq!(fx.tally(), fx.earned().await, "and earns nothing");
	steps_until(&fx, "S2 retired and S1 rewritten", async || {
		assert_eq!(
			fx.tally(),
			fx.earned().await,
			"only Applied effects earn; S2 local {:?} committed {:?}",
			fx.slab()
				.tier_pointer(fx.s2)
				.await
				.map(|pointer| pointer.l2_uri),
			fx.committed(fx.s2).await
		);
		fx.retired().await && fx.rewritten().await.is_some()
	})
	.await;
	assert!(fx.retired().await, "no pointer remains, local or committed");
	assert_eq!(
		fx.tally(),
		aged_data(3),
		"a1, a2, a3 only, never d's ordinary version"
	);
	fx.s1_holds_only_n().await;
	fx.nothing_left().await;
	assert_eq!(fx.tally(), aged_data(3));
	assert!(retention_series(L2_DATA) >= export + 3, "exported");
	append_rows_everywhere(&fx.cluster, &fx.append, &[(4, "n")], "only the neighbour").await;
	// A clean reopen replays the log: the new store earns nothing.
	fx.cluster.stop(fx.leader, false).await;
	fx.cluster.reopen(fx.leader).await;
	pause_generations(&fx.cluster, true);
	until_settled(&fx.cluster, &fx.append).await;
	assert!(fx.retired().await);
	assert_eq!(fx.tally(), AgedRows::default(), "no recount on replay");
	assert_eq!(slab.aged_rows_pruned_for_test(AgedLevel::L2), aged_data(3));
	fx.s1_holds_only_n().await;
	append_rows_everywhere(&fx.cluster, &fx.append, &[(4, "n")], "after the reopen").await;
	shut_down(fx.cluster, &fx.time).await;
}

/// Wendelon ADR 0122 D4, R1 (Codex 01a20151): observer-first success. S2 is retired by its worker
/// (2). S1's rewrite is then held right after the ring decides its `Tier` Applied; the leader's
/// apply observer installs the replacement while the worker waits, and earns nothing; released,
/// the worker verifies its own effect and counts it once (3).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_observer_first_delivery_is_counted_once_by_its_own_worker() {
	let fx = aged_l2().await;
	let slab = fx.slab();
	match fx.step().await {
		Ok(CertifiedTierStep::Retired { slab, .. }) if slab == fx.s2 => {}
		other => panic!("S2's retirement: {other:?}"),
	}
	assert_eq!(fx.tally(), aged_data(2));
	let (reached, release) = slab.hold_next_certified_tier_step(CertifiedTierCut::AfterAcceptance);
	slab.restart_certified_tier_cursor_for_test();
	let worker = tokio::spawn(tier_step(
		fx.cluster.node(fx.leader).data_server.clone(),
		fx.committer(),
	));
	tokio::time::timeout(BOUND, reached.acquire())
		.await
		.expect("S1's step reaches acceptance")
		.unwrap()
		.forget();
	fx.until("the observer installs S1's replacement", async || {
		fx.rewritten().await.is_some()
	})
	.await;
	assert_eq!(fx.tally(), aged_data(2), "the observer earns nothing");
	release.add_permits(1);
	match worker.await.unwrap() {
		Ok(CertifiedTierStep::Rewritten { slab, .. }) if slab == fx.s1 => {}
		other => panic!("the held worker's step: {other:?}"),
	}
	assert_eq!(fx.tally(), aged_data(3), "its own worker counts it, once");
	fx.s1_holds_only_n().await;
	fx.nothing_left().await;
	assert_eq!(fx.tally(), aged_data(3));
	shut_down(fx.cluster, &fx.time).await;
}

/// Wendelon ADR 0122 D4, R2 (Codex 01a20151): an unsubmitted retirement earns nothing. S2's
/// retirement is cut after its intent and certificate are durable and before submission: nothing
/// is earned and nothing changes (S2 stays tiered, locally and committed). That intent is open's
/// to resolve (ADR 0704); in this process S2 is not retried, and S1's own rewrite earns exactly
/// its one aged row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unsubmitted_retirement_earns_nothing() {
	let fx = aged_l2().await;
	let slab = fx.slab();
	let source = slab.tier_pointer(fx.s1).await.unwrap();
	slab.cut_next_certified_tier_step(CertifiedTierCut::BeforeSubmit);
	let cut = fx.step().await;
	assert!(
		cut.is_err(),
		"the retirement is cut before submission: {cut:?}"
	);
	assert_eq!(
		fx.tally(),
		AgedRows::default(),
		"nothing submitted, nothing earned"
	);
	assert!(slab.tier_pointer(fx.s2).await.is_some() && fx.committed(fx.s2).await.is_some());
	assert_eq!(slab.tier_pointer(fx.s1).await.as_ref(), Some(&source));
	steps_until(&fx, "S1 rewritten", async || fx.rewritten().await.is_some()).await;
	assert_eq!(fx.tally(), aged_data(1), "S1's own rewrite alone");
	assert!(slab.tier_pointer(fx.s2).await.is_some() && fx.committed(fx.s2).await.is_some());
	fx.s1_holds_only_n().await;
	shut_down(fx.cluster, &fx.time).await;
}

/// Wendelon ADR 0122 D4 (Codex 01a201b7): only the worker's own Applied op earns its count. With
/// each exact effect installed (S2 retired, S1's replacement published, locally and committed),
/// the production counting entry is called with the worker's real op key but a mismatched digest,
/// and with a key whose outcome is missing: it answers false and the store's tally is unchanged.
/// The same call with the op's real digest answers true and adds its rows, so only the op proof
/// made the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn only_its_own_applied_op_earns_a_count() {
	use crate::storage::manager::certified_tier::AgedL2Effect;
	let fx = aged_l2().await;
	let slab = fx.slab();
	let retire_op = match fx.step().await {
		Ok(CertifiedTierStep::Retired { slab, op }) if slab == fx.s2 => op,
		other => panic!("S2's retirement: {other:?}"),
	};
	let (tier_op, candidate) = match fx.step().await {
		Ok(CertifiedTierStep::Rewritten {
			slab,
			op,
			candidate,
		}) if slab == fx.s1 => (op, candidate),
		other => panic!("S1's rewrite: {other:?}"),
	};
	assert_eq!(fx.tally(), aged_data(3));
	assert!(fx.retired().await);
	fx.s1_holds_only_n().await;
	let tier = AgedL2Effect::Tier(candidate);
	for (slab_id, op, effect, rows) in [
		(fx.s1, tier_op, &tier, aged_data(1)),
		(fx.s2, retire_op, &AgedL2Effect::Retire, aged_data(2)),
	] {
		let digest = slab
			.applied_op_digest_for_test(op)
			.await
			.expect("the op's Applied outcome is retained");
		let mut wrong = digest;
		wrong[0] ^= 0xFF;
		let missing = crate::consensus::slab_ops::OpKey {
			op_seq: op.op_seq + 1_000_000,
			..op
		};
		let before = fx.tally();
		for (key, claimed, what) in [
			(op, wrong, "a mismatched digest"),
			(missing, digest, "a missing outcome"),
		] {
			assert!(
				!slab
					.record_aged_l2_if_applied(slab_id, key, claimed, effect, rows)
					.await,
				"{what} earns nothing for {effect:?}"
			);
			assert_eq!(fx.tally(), before, "{what}: the tally is unchanged");
		}
		assert!(
			slab.record_aged_l2_if_applied(slab_id, op, digest, effect, rows)
				.await,
			"the real op earns it ({effect:?})"
		);
		assert_eq!(
			fx.tally(),
			AgedRows {
				data: before.data + rows.data,
				tombstone: before.tombstone
			}
		);
	}
	shut_down(fx.cluster, &fx.time).await;
}

/// Steps the leader's certified tier work until `done`: later steps recover what an unfinished
/// worker left (or find the observer already did).
async fn steps_until(fx: &AgedL2, what: &str, done: impl AsyncFn() -> bool) {
	let deadline = tokio::time::Instant::now() + BOUND;
	while !done().await {
		assert!(tokio::time::Instant::now() < deadline, "never: {what}");
		let _ = fx.step_once().await;
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
}

/// Wendelon ADR 0122 D4, R1/R2 (Codex 01a20151): an effect whose worker never finished earns
/// nothing, whichever route then completes it (a later step's recovery, or the apply observer):
/// - an error right after acceptance (S2's retirement);
/// - a cancellation right after acceptance: the step's task is aborted while it waits (S1's
///   rewrite).
/// No registration exists to outlive either, and nothing later earns them; the effects are
/// complete (S2 retired; S1's committed replacement holds n alone).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_effect_its_worker_never_finished_earns_nothing() {
	let fx = aged_l2().await;
	let slab = fx.slab();
	slab.cut_next_certified_tier_step(CertifiedTierCut::AfterAcceptance);
	let failed = fx.step().await;
	assert!(
		failed.is_err(),
		"S2's worker fails after acceptance: {failed:?}"
	);
	steps_until(&fx, "S2's decided retirement is delivered", async || {
		fx.retired().await
	})
	.await;
	assert_eq!(fx.tally(), AgedRows::default(), "an error earns nothing");
	let (reached, _release) = slab.hold_next_certified_tier_step(CertifiedTierCut::AfterAcceptance);
	slab.restart_certified_tier_cursor_for_test();
	let worker = tokio::spawn(tier_step(
		fx.cluster.node(fx.leader).data_server.clone(),
		fx.committer(),
	));
	tokio::time::timeout(BOUND, reached.acquire())
		.await
		.expect("S1's step reaches acceptance")
		.unwrap()
		.forget();
	worker.abort();
	assert!(
		worker.await.unwrap_err().is_cancelled(),
		"the worker is cancelled"
	);
	steps_until(&fx, "S1's decided rewrite is installed", async || {
		fx.rewritten().await.is_some()
	})
	.await;
	assert_eq!(
		fx.tally(),
		AgedRows::default(),
		"a cancellation earns nothing"
	);
	fx.s1_holds_only_n().await;
	fx.nothing_left().await;
	assert_eq!(fx.tally(), AgedRows::default(), "nothing earns them later");
	append_rows_everywhere(&fx.cluster, &fx.append, &[(4, "n")], "only the neighbour").await;
	shut_down(fx.cluster, &fx.time).await;
}

/// Wendelon ADR 0122, the idle tombstone bound (S1's trigger kept): when a store's last governed
/// winner is a tombstone, the certificate agrees no visible row, so the clock alone schedules no
/// generation that would omit it; it stays a base entry however late it is. The next generation
/// triggered by something else (an ordinary erasing write) with a covering floor omits it as aged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_last_tombstone_waits_for_a_covering_ordinary_generation() {
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
	let floor1 = time.clock.now_ms() + 60_000;
	jump_floor(&cluster, floor1).await;
	until_active(&cluster, 1).await;
	// r deleted: its tombstone becomes the only governed winner.
	let mut delete = delete_meta(&append);
	delete.parent_write_id.client_uuid = uuid_v7_at(floor1 + 1);
	let delete_key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(append.table_object_id),
		client_uuid: delete.parent_write_id.client_uuid,
		sequence_no: delete.parent_write_id.sequence_no,
	};
	assert_eq!(
		counts(&delete_through(&cluster, leader, delete, vec![identity_of(&r, 0)]).await),
		[1, 0, 0]
	);
	let rsv = record_of(&cluster, delete_key).await.expect("recorded").rsv;
	jump_floor(&cluster, floor1 + 60_000).await;
	let gid = until_covered(&cluster, rsv).await;
	let (table, row) = (append.table_object_id, identity_of(&r, 0));
	assert_eq!(
		tombstone_in_base(&cluster, table, row).await,
		vec![true; NODES.len()]
	);
	assert_eq!(
		certified_oldest(&cluster).await,
		None,
		"a tombstone is never the least visible row"
	);
	// Only the clock passes it by the window and steps: no generation commits.
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		time.clock.now_ms() + WINDOW_MS + 2 * step + 1_000,
	)
	.await;
	let leader = cluster.leader().await;
	let current = cluster
		.node(leader)
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
	assert_eq!(current, gid, "the clock alone schedules nothing");
	assert_eq!(
		tombstone_in_base(&cluster, table, row).await,
		vec![true; NODES.len()]
	);
	// An ordinary erasing write's generation raises the floor and omits it as aged.
	let y = write_one(
		&cluster,
		leader,
		&append,
		(2, "y"),
		uuid_v7_at(floor1 + 60_001),
	)
	.await;
	let y_rsv = record_of(&cluster, key_of(&append, &y))
		.await
		.expect("recorded")
		.rsv;
	jump_floor(&cluster, floor1 + 120_000).await;
	until_covered(&cluster, y_rsv).await;
	assert_eq!(
		tombstone_in_base(&cluster, table, row).await,
		vec![false; NODES.len()]
	);
	// Two debt records at this key: r's data version, displaced by its tombstone (ordinary), and
	// the tombstone, omitted by age (aged).
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let mut owed: Vec<_> = index
			.debt_page(None, usize::MAX)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, _)| key.table == table && key.key_identity == *row.as_bytes())
			.collect();
		owed.sort_by_key(|(key, _)| key.hlc);
		assert_eq!(owed.len(), 2, "at {node}: {owed:?}");
		assert!(
			!owed[0].1.aged
				&& matches!(
					owed[0].1.source,
					crate::storage::index::certified_base::DebtSource::DisplacedBase {
						tombstone: false
					}
				),
			"r's data version is ordinary debt at {node}: {owed:?}"
		);
		assert!(owed[1].1.aged, "the tombstone is aged at {node}: {owed:?}");
	}
	append_rows_everywhere(&cluster, &append, &[(2, "y")], "y served").await;
	shut_down(cluster, &time).await;
}

/// Whether each node's active base holds `row` of `table` as a tombstone.
async fn tombstone_in_base(
	cluster: &Cluster,
	table: TableObjectId,
	row: crate::schema::Identity,
) -> Vec<bool> {
	let mut held = Vec::new();
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		held.push(
			index
				.base_entries(false)
				.await
				.unwrap()
				.into_iter()
				.any(|entry| {
					entry.table == table && entry.key_identity == *row.as_bytes() && entry.tombstone
				}),
		);
	}
	held
}

/// Holds (or releases) every node's generation steps.
fn pause_generations(cluster: &Cluster, paused: bool) {
	for node in NODES {
		cluster
			.node(node)
			.data_server
			.pause_generations_for_test(paused);
	}
}

/// Wendelon ADR 0122 D4 (Codex 01a20151 R2), the certified LSM alone: the leader's cleanup (S5b
/// included) is paused, and a certified LSM job, driven here, elides both an aged row (a) and an
/// ordinary obsolete version (x, overwritten by y in a table no window governs). The leader's L1
/// tally earns exactly a, once; x earns nothing; no S5b and no L2 credit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certified_lsm_job_counts_its_aged_elisions_only() {
	use std::sync::atomic::Ordering::Release;
	let time = TestTime::new();
	let (cluster, leader, pk, append) = windowed(&time).await;
	let server = cluster.node(leader).data_server.clone();
	server
		.decision_peer_for_test()
		.pause_cleanup
		.store(true, Release);
	let slab = cluster.node(leader).slab.clone();
	let floor1 = time.clock.now_ms() + 60_000;
	// Each write in its own L0 run on the leader.
	let a = write_one(
		&cluster,
		leader,
		&append,
		(1, "a"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	slab.force_seal_active_for_test().await;
	let mut last = key_of(&append, &a);
	for value in ["x", "y"] {
		let meta = write_meta(&pk, 1, uuid_v7_at(time.clock.now_ms()));
		assert_committed(
			&write_through(&cluster, leader, meta, named_rows(&pk, &[(1, value)])).await,
			"an overwrite",
		);
		last = key_of(&pk, &meta);
		slab.force_seal_active_for_test().await;
	}
	// Read while their intents are retained (a covering generation retires them).
	let one = server
		.row_identity_and_locality_for_test(TenantId::ZERO, &pk, &named_rows(&pk, &[(1, "x")]))
		.0;
	let ones = owned_versions(&cluster, leader, one).await;
	assert_eq!(ones.len(), 2, "x and y owned");
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		time.clock.now_ms() + WINDOW_MS + step + 1_000,
	)
	.await;
	let rsv = record_of(&cluster, last).await.expect("recorded").rsv;
	jump_floor(&cluster, floor1 + 60_000).await;
	until_covered(&cluster, rsv).await;
	let a_hlc = omitted_as_aged(&cluster, append.table_object_id, identity_of(&a, 0), "a").await;
	let index = slab.persistent_index().unwrap();
	let x_debt = index
		.debt_page(None, usize::MAX)
		.await
		.unwrap()
		.into_iter()
		.find(|(key, _)| key.key_identity == *one.as_bytes() && key.hlc == ones[0])
		.expect("x's version is owed");
	assert!(!x_debt.1.aged, "x's debt is ordinary: {x_debt:?}");
	let worker = crate::storage::LsmCompactionWorker::new(
		crate::storage::LsmConfig {
			max_l0_slab_count: 1,
			lsm_l0_max_age_millis: 0,
			..crate::storage::LsmConfig::default()
		},
		slab.slab_pin_table(),
	)
	.unwrap();
	let committer = crate::storage::RaftCommitter::new(cluster.node(leader).raft.clone())
		.with_state_machine(cluster.node(leader).state_machine.clone())
		.with_owner_authority(server.owner_authority());
	let at = HybridTimestamp {
		physical_ms: time.clock.now_ms(),
		logical: 0,
		node_id: 0,
	};
	let a_id = identity_of(&a, 0);
	let copies = |key, hlc| {
		slab.version_locations(TenantId::ZERO, key, hlc)
			.unwrap()
			.len()
	};
	let deadline = tokio::time::Instant::now() + BOUND;
	let mut jobs = 0;
	while copies(a_id, a_hlc) + copies(one, ones[0]) > 0 {
		assert!(
			tokio::time::Instant::now() < deadline,
			"a {} and x {} copies remain after {jobs} certified jobs",
			copies(a_id, a_hlc),
			copies(one, ones[0])
		);
		match server
			.certified_lsm_step_unpaused(&worker, at, &committer)
			.await
		{
			Ok(Some(_)) => jobs += 1,
			Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
			Err(error) => panic!("a certified LSM job failed: {error}"),
		}
	}
	assert!(copies(one, ones[1]) > 0, "y stays");
	assert_eq!(
		slab.aged_rows_pruned_for_test(AgedLevel::L1),
		aged_data(1),
		"a once; x's ordinary version earns nothing"
	);
	assert_eq!(
		slab.aged_rows_pruned_for_test(AgedLevel::L2),
		AgedRows::default()
	);
	append_rows_everywhere(&cluster, &append, &[], "a is not served").await;
	shut_down(cluster, &time).await;
}

/// Wendelon ADR 0122 D2 (Codex 01a20151), policy 4 live: one generation both excludes a finalized
/// drop (ADR 0115) and omits an aged row. Generation steps are held while the clock passes the
/// aged row and the drop is finalized, so the next target carries both: its cut is past the drop
/// and its floor past the row, and its policy is the composition of the two (4). At every member
/// the dropped table's entries leave the base as ordinary debt, and the aged row as aged debt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_generation_excludes_a_dropped_table_and_omits_an_aged_row() {
	use crate::consensus::decisions::generations::{
		SURVIVOR_POLICY_EXCLUDING_AGED, compose_survivor_policy,
	};
	let time = TestTime::new();
	let (cluster, leader, pk, append) = windowed(&time).await;
	let a = write_one(
		&cluster,
		leader,
		&append,
		(1, "a"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	let p = write_meta(&pk, 1, uuid_v7_at(time.clock.now_ms()));
	assert_committed(
		&write_through(&cluster, leader, p, named_rows(&pk, &[(1, "p")])).await,
		"p",
	);
	let floor1 = time.clock.now_ms() + 60_000;
	jump_floor(&cluster, floor1).await;
	until_active(&cluster, 1).await;
	let dropped = pk.table_object_id;
	for node in NODES {
		let base = cluster
			.node(node)
			.slab
			.persistent_index()
			.unwrap()
			.base_entries(false)
			.await
			.unwrap();
		assert!(
			base.iter().any(|entry| entry.table == dropped),
			"p certified at {node}"
		);
		assert!(
			base.iter()
				.any(|entry| entry.key_identity == *identity_of(&a, 0).as_bytes()),
			"a certified at {node}"
		);
	}
	pause_generations(&cluster, true);
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(
		&cluster,
		&time,
		time.clock.now_ms() + WINDOW_MS + step + 1_000,
	)
	.await;
	let (_, finalized) = super::drop_exclusion::drop_logically(&cluster, &pk).await;
	// A later entry, so the next target's cut can close past the finalize.
	write_one(
		&cluster,
		cluster.leader().await,
		&append,
		(2, "z"),
		// Above the jumped decision floor, or it expires.
		uuid_v7_at(floor1 + 1),
	)
	.await;
	pause_generations(&cluster, false);
	let gid = until_covered(&cluster, finalized).await;
	let (t, floor_ms, policy) = cluster
		.node(cluster.leader().await)
		.state_machine
		.read_state(|state| {
			let certificate = state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current
				.clone()
				.expect("g2");
			(
				certificate.t,
				certificate.retention.floor_ms,
				compose_survivor_policy(
					state.pd_survivor_policy_at(certificate.t),
					certificate.retention.floor_ms,
				),
			)
		})
		.await;
	assert!(
		t >= finalized,
		"g2's cut {t} is past the drop at {finalized}"
	);
	assert_eq!(
		policy, SURVIVOR_POLICY_EXCLUDING_AGED,
		"exclusion and age together"
	);
	let a_hlc = omitted_as_aged(&cluster, append.table_object_id, identity_of(&a, 0), "a").await;
	assert!(a_hlc.physical_ms < floor_ms, "a is below g2's floor");
	for node in NODES {
		let index = cluster.node(node).slab.persistent_index().unwrap();
		let base = index.base_entries(false).await.unwrap();
		assert!(
			!base.iter().any(|entry| entry.table == dropped),
			"the dropped table left the base at {node}"
		);
		let owed: Vec<_> = index
			.debt_page(None, usize::MAX)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, _)| key.table == dropped)
			.collect();
		assert!(
			!owed.is_empty(),
			"the dropped table's entries are owed at {node}"
		);
		assert!(
			owed.iter().all(|(_, record)| !record.aged),
			"excluded, not aged, at {node}: {owed:?}"
		);
		// One build did both: the exclusion and the age omission are the same generation's debt.
		let a_debt: Vec<_> = index
			.debt_page(None, usize::MAX)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, _)| key.key_identity == *identity_of(&a, 0).as_bytes())
			.collect();
		assert!(
			owed.iter()
				.chain(&a_debt)
				.all(|(_, record)| record.gid == gid),
			"both by generation {gid} at {node}: {owed:?} {a_debt:?}"
		);
	}
	append_rows_everywhere(&cluster, &append, &[(2, "z")], "a is not served").await;
	shut_down(cluster, &time).await;
}

// --- The product-semantics discriminator (Codex 01a20253) ---------------------------------------

/// What blocks a generation cut over the leader's applied state, from one read: the applied
/// index, the current generation, whether `key` is live, the blocking point below the highest
/// candidate cut, the decision floor and clock, and whether the production trigger proposes a
/// target at `now_ms`.
async fn cut_blockers(cluster: &Cluster, key: ParentKey, now_ms: u64) -> String {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let applied = state.last_applied_log_id.map_or(0, |id| id.index);
			let decisions = state.decisions.as_ref().unwrap();
			let generations = decisions.generations();
			let next =
				crate::consensus::decisions::generations::next_target(state, applied, Some(now_ms));
			format!(
				"applied {applied}, gid {}, cut {}, key live {}, blocking point below {} = {:?}, \
				 decision floor {} ms, clock {} ms, proposed target {:?}",
				generations.current_gid(),
				generations.fr(),
				decisions.key_is_live_for_test(&key),
				applied.saturating_sub(1),
				crate::consensus::decisions::generations::blocking_point_for_test(
					state,
					applied.saturating_sub(1)
				),
				decisions.floor_ms(),
				decisions.clock_ms(),
				next.map(|t| (t.gid, t.t, t.retention.floor_ms)),
			)
		})
		.await
}

/// The current generation, waiting up to `bound` of real time for it to reach `gid`.
async fn gid_within(cluster: &Cluster, gid: u64, bound: Duration) -> u64 {
	let deadline = tokio::time::Instant::now() + bound;
	loop {
		let leader = cluster.leader().await;
		let current = cluster
			.node(leader)
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
		if current >= gid || tokio::time::Instant::now() >= deadline {
			return current;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

// --- ADR 0123 baselines (Codex 01a202b5) --------------------------------------------------------

/// ADR 0123 D7: the settlement interval S, provisional (a hypothesis the controls measure).
const SETTLE_INTERVAL_MS: u64 = crate::consensus::decisions::generations::SETTLE_INTERVAL_MS;

/// The leader's current certificate.
async fn current_certificate(
	cluster: &Cluster,
) -> Option<crate::consensus::decisions::generations::Certificate> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref()?.generations().current.clone())
		.await
}

/// The leader's settlement watermark.
async fn settled_ms(cluster: &Cluster) -> u64 {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.map_or(0, |decisions| decisions.generations().settled_ms)
		})
		.await
}

/// ADR 0123 L1 (Codex: bound to the actual first T4 certificate). A 15 s window; one governed
/// row r at issue, a young survivor y written S − 5 s later; only the production advancer moves
/// the clock (no floor shortcut, no key retired by hand). Nothing proposes before r is S old
/// (no certificate, no retired key, no structure). The FIRST certificate is then:
/// - proposed by the leader for the settlement reason (T4), carrying its sample;
/// - a cut covering r and y, with the sampled floor between them: r omitted as aged in that
///   same generation, y surviving and served;
/// - the watermark advanced to exactly its stamp.
/// r's key stays live with the typed receipt of its ORIGINAL outcome (its decision, claim
/// digest and commit HLC as recorded in its body): an identical retry is that commit, a
/// changed payload a key conflict, the public resolve PRESENT.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_first_settlement_omits_an_aged_row_and_keeps_its_receipt() {
	use crate::consensus::decisions::generations::TargetReason;
	use crate::consensus::decisions::{AcceptedEffect, RecordKind, Settlement};
	use crate::schema::MutationResolutionStatus;
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed(&time).await;
	let issued = time.clock.now_ms();
	let r = write_one(&cluster, leader, &append, (1, "r"), uuid_v7_at(issued)).await;
	let r_key = key_of(&append, &r);
	let r_body = record_of(&cluster, r_key).await.expect("r recorded");
	let crate::consensus::decisions::ParentStatus::Accepted { dec: r_dec } = r_body.status else {
		panic!("r accepted: {:?}", r_body.status);
	};
	advance(&cluster, &time, issued + SETTLE_INTERVAL_MS - 5_000).await;
	assert_eq!(
		gid_within(&cluster, 1, Duration::from_secs(3)).await,
		0,
		"nothing proposes before r is S old"
	);
	let leader = cluster.leader().await;
	let y = write_one(
		&cluster,
		leader,
		&append,
		(2, "y"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	let y_rsv = record_of(&cluster, key_of(&append, &y))
		.await
		.expect("y recorded")
		.rsv;
	advance(&cluster, &time, issued + SETTLE_INTERVAL_MS + 2_000).await;
	let gid = gid_within(&cluster, 1, Duration::from_secs(10)).await;
	let blockers = cut_blockers(&cluster, r_key, time.clock.now_ms()).await;
	eprintln!(
		"L1 at issue + {} ms: {blockers}",
		time.clock.now_ms() - issued
	);
	assert!(
		gid >= 1,
		"no generation committed by the first settlement (issue + S + 2 s); {blockers}"
	);
	let certificate = current_certificate(&cluster).await.unwrap();
	assert_eq!(certificate.gid, 1, "the first certificate");
	let leader = cluster.leader().await;
	assert_eq!(
		cluster
			.node(leader)
			.data_server
			.proposed_reason_for_test(certificate.gid),
		Some(TargetReason::Settlement),
		"the first target was proposed for the settlement"
	);
	let stamp = certificate.retention.stamp_ms;
	assert!(
		stamp >= issued + SETTLE_INTERVAL_MS,
		"a settlement carries its sample: {stamp}"
	);
	assert!(
		certificate.t >= y_rsv && certificate.t >= r_dec,
		"the cut covers r and y: {certificate:?}"
	);
	assert_eq!(
		certificate.retention.floor_ms,
		stamp - WINDOW_MS,
		"the sampled floor"
	);
	assert!(
		r_body.claim.commit_hlc.physical_ms < certificate.retention.floor_ms,
		"r is below the floor"
	);
	assert_eq!(
		settled_ms(&cluster).await,
		stamp,
		"the watermark is the stamp"
	);
	eprintln!(
		"L1 first certificate: gid {}, t {}, stamp {stamp}, floor {}",
		certificate.gid, certificate.t, certificate.retention.floor_ms
	);
	// Every member activates that generation before its base and debt are read.
	until_active(&cluster, certificate.gid).await;
	// In that same generation: r omitted as aged, y surviving.
	omitted_as_aged(&cluster, append.table_object_id, identity_of(&r, 0), "r").await;
	append_rows_everywhere(&cluster, &append, &[(2, "y")], "y survives").await;
	// r's typed receipt is its original outcome.
	let entry = key_entry_when(&cluster, r_key, |entry| {
		matches!(entry.record, RecordKind::Settled(_))
	})
	.await;
	assert_eq!(entry.reservation, r_body.rsv);
	assert_eq!(
		entry.record,
		RecordKind::Settled(Settlement::Accepted {
			dec: r_dec,
			claim_digest: r_body.claim_digest,
			commit_hlc: r_body.claim.commit_hlc,
			effect: AcceptedEffect::Write,
		}),
		"r's receipt is its original outcome"
	);
	let leader = cluster.leader().await;
	let retry = write_through(&cluster, leader, r, named_rows(&append, &[(1, "r")])).await;
	assert!(
		matches!(retry.write.status, WriteStatus::OK | WriteStatus::DUPLICATE),
		"an identical retry is its recorded commit: {retry:?}"
	);
	let conflict = write_through(&cluster, leader, r, named_rows(&append, &[(1, "changed")])).await;
	assert_eq!(
		conflict.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{conflict:?}"
	);
	let present = super::resolve::resolve_at(&cluster, leader, r.parent_write_id).await;
	assert_eq!(
		present.resolution,
		MutationResolutionStatus::PRESENT,
		"{present:?}"
	);

	// L1b: with no new write, the survivor ages. Past y's window and a step, an age target
	// (attributed by its target digest) omits y as aged in its own generation; then, for two
	// more intervals, nothing is proposed: no record is left to settle and no governed row
	// is visible (quiescence).
	let step = crate::consensus::decisions::generations::retention_step_ms(WINDOW_MS);
	advance(&cluster, &time, stamp + WINDOW_MS + 2 * step).await;
	let gid = gid_within(&cluster, certificate.gid + 1, Duration::from_secs(10)).await;
	assert!(gid > certificate.gid, "y aged out by a later generation");
	let aged = current_certificate(&cluster).await.unwrap();
	assert_eq!(
		reason_of_certificate(&cluster, &aged),
		Some(TargetReason::Age),
		"the survivor ages out by the age trigger: {aged:?}"
	);
	until_active(&cluster, aged.gid).await;
	omitted_as_aged(&cluster, append.table_object_id, identity_of(&y, 0), "y").await;
	eprintln!(
		"L1b: gid {} (age) omits y; stamp {}, floor {}",
		aged.gid, aged.retention.stamp_ms, aged.retention.floor_ms
	);
	let quiet_from = aged.gid;
	advance(
		&cluster,
		&time,
		time.clock.now_ms() + 2 * SETTLE_INTERVAL_MS,
	)
	.await;
	assert_eq!(
		gid_within(&cluster, quiet_from + 1, Duration::from_secs(5)).await,
		quiet_from,
		"quiescence: nothing more is proposed"
	);
	shut_down(cluster, &time).await;
}

/// ADR 0123 Z1, GREEN today and to stay GREEN: a quiet ZERO-window store keeps today's coverage
/// and trigger. One row r; no target while r's key lives (issue + E − 10 s: generation 0, key
/// live); once the key retires, exactly one ordinary retired-key generation (T0) covers r, which
/// stays served (nothing ages at window 0); then, idle for several more minutes of test time,
/// no further generation (quiescence). Crosses the production decision expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "crosses the production decision expiry (~66 min of test time); ADR 0123 Z1 baseline"]
async fn a_quiet_zero_window_store_settles_only_when_its_key_retires() {
	use crate::consensus::decisions::DECISION_EXPIRY_MS;
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed_with(&time, Duration::ZERO).await;
	let issued = time.clock.now_ms();
	let r = write_one(&cluster, leader, &append, (1, "r"), uuid_v7_at(issued)).await;
	let r_key = key_of(&append, &r);
	let r_rsv = record_of(&cluster, r_key).await.expect("r recorded").rsv;
	advance(&cluster, &time, issued + DECISION_EXPIRY_MS - 10_000).await;
	assert_eq!(
		gid_within(&cluster, 1, Duration::from_secs(5)).await,
		0,
		"no target while r's key lives"
	);
	let blockers = cut_blockers(&cluster, r_key, time.clock.now_ms()).await;
	eprintln!("Z1 before expiry: {blockers}");
	assert!(blockers.contains("key live true") && blockers.contains("proposed target None"));
	advance(&cluster, &time, issued + DECISION_EXPIRY_MS + 2_000).await;
	let gid = until_covered(&cluster, r_rsv).await;
	assert_eq!(gid, 1, "exactly one ordinary generation covers r");
	let blockers = cut_blockers(&cluster, r_key, time.clock.now_ms()).await;
	eprintln!("Z1 after expiry: {blockers}");
	assert!(blockers.contains("key live false"), "{blockers}");
	append_rows_everywhere(&cluster, &append, &[(1, "r")], "r stays served at window 0").await;
	advance(
		&cluster,
		&time,
		issued + DECISION_EXPIRY_MS + 5 * SETTLE_INTERVAL_MS,
	)
	.await;
	assert_eq!(
		gid_within(&cluster, 2, Duration::from_secs(5)).await,
		1,
		"quiescence: no further generation"
	);
	shut_down(cluster, &time).await;
}

/// A key's entry in the leader's applied decisions, waiting up to the bound for `ready`.
async fn key_entry_when(
	cluster: &Cluster,
	key: ParentKey,
	ready: impl Fn(&crate::consensus::decisions::KeyEntry) -> bool,
) -> crate::consensus::decisions::KeyEntry {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leader = cluster.leader().await;
		let entry = cluster
			.node(leader)
			.state_machine
			.read_state(move |state| state.decisions.as_ref()?.key_entry_for_test(&key))
			.await;
		if let Some(entry) = entry
			&& ready(&entry)
		{
			return entry;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"key {key:?}: {entry:?}"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

fn delete_key(
	descriptor: &TableSchemaDescriptor,
	meta: &crate::schema::TableDeleteRowsRequestMeta,
) -> ParentKey {
	ParentKey {
		tenant: TenantId::ZERO,
		namespace: crate::consensus::decisions::ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: meta.parent_write_id.client_uuid,
		sequence_no: meta.parent_write_id.sequence_no,
	}
}

/// ADR 0123 slice 1 (Codex 01a202bc item 2), same build, in process: a write, a matched
/// delete and a delete that published nothing are recorded with live keys; an older write's
/// key then retires (T0), so a generation is proposed whose cut also covers the three fresh
/// records and settles them. Each retry through the coordinator then answers its actual
/// recorded outcome from the receipt: the identical write its commit, a changed payload a
/// key conflict, the public resolve PRESENT, and each delete its exact frozen counts. Nothing
/// is admitted afresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn settled_write_and_delete_retries_answer_their_recorded_outcomes() {
	use crate::consensus::decisions::{AcceptedEffect, RecordKind, Settlement};
	use crate::schema::MutationResolutionStatus;
	let time = TestTime::new();
	let (cluster, leader, pk, append) = windowed(&time).await;
	let t0 = time.clock.now_ms();
	write_one(&cluster, leader, &append, (1, "old"), uuid_v7_at(t0)).await;
	write_one(&cluster, leader, &pk, (1, "p1"), uuid_v7_at(t0 + 1)).await;
	let fresh_ms = t0 + 60_000;
	let r = write_one(&cluster, leader, &append, (2, "r"), uuid_v7_at(fresh_ms)).await;
	let p1 = cluster
		.node(leader)
		.data_server
		.row_identity_and_locality_for_test(TenantId::ZERO, &pk, &named_rows(&pk, &[(1, "p1")]))
		.0;
	let mut matched = delete_meta(&pk);
	matched.parent_write_id.client_uuid = uuid_v7_at(fresh_ms + 1);
	let deleted = delete_through(&cluster, leader, matched, vec![p1]).await;
	assert_eq!(counts(&deleted), [1, 0, 0], "p1 deleted");
	let absent = cluster
		.node(leader)
		.data_server
		.row_identity_and_locality_for_test(TenantId::ZERO, &pk, &named_rows(&pk, &[(9, "none")]))
		.0;
	let mut missed = delete_meta(&pk);
	missed.parent_write_id.client_uuid = uuid_v7_at(fresh_ms + 2);
	let published_nothing = delete_through(&cluster, leader, missed, vec![absent]).await;
	let nothing = counts(&published_nothing);
	assert_eq!(nothing[0], 0, "nothing deleted: {nothing:?}");
	let keys = [
		key_of(&append, &r),
		delete_key(&pk, &matched),
		delete_key(&pk, &missed),
	];
	let mut last = 0;
	for key in keys {
		last = last.max(key_entry_when(&cluster, key, |_| true).await.reservation);
	}
	// T0: the older keys retire; the generation proposed for them also covers the fresh ones.
	jump_floor(&cluster, t0 + 30_000).await;
	until_covered(&cluster, last).await;
	let settled = |entry: &crate::consensus::decisions::KeyEntry| {
		matches!(entry.record, RecordKind::Settled(_))
	};
	let r_entry = key_entry_when(&cluster, keys[0], settled).await;
	assert!(
		matches!(
			r_entry.record,
			RecordKind::Settled(Settlement::Accepted {
				effect: AcceptedEffect::Write,
				..
			})
		),
		"{r_entry:?}"
	);
	let matched_entry = key_entry_when(&cluster, keys[1], settled).await;
	assert!(
		matches!(
			matched_entry.record,
			RecordKind::Settled(Settlement::Accepted {
				effect: AcceptedEffect::Delete([1, 0, 0]),
				..
			})
		),
		"{matched_entry:?}"
	);
	let missed_entry = key_entry_when(&cluster, keys[2], settled).await;
	assert_eq!(
		missed_entry.record,
		RecordKind::Settled(Settlement::Noop { counts: nothing }),
		"{missed_entry:?}"
	);
	// Retries through the coordinator, from the receipts.
	let leader = cluster.leader().await;
	let retry = write_through(&cluster, leader, r, named_rows(&append, &[(2, "r")])).await;
	assert!(
		matches!(retry.write.status, WriteStatus::OK | WriteStatus::DUPLICATE),
		"an identical retry is its recorded commit: {retry:?}"
	);
	let changed = write_through(&cluster, leader, r, named_rows(&append, &[(2, "changed")])).await;
	assert_eq!(
		changed.write.status,
		WriteStatus::WRITE_KEY_CONFLICT,
		"{changed:?}"
	);
	let present = super::resolve::resolve_at(&cluster, leader, r.parent_write_id).await;
	assert_eq!(
		present.resolution,
		MutationResolutionStatus::PRESENT,
		"{present:?}"
	);
	let again = delete_through(&cluster, leader, matched, vec![p1]).await;
	assert_eq!(
		counts(&again),
		[1, 0, 0],
		"the matched delete's frozen counts"
	);
	let again = delete_through(&cluster, leader, missed, vec![absent]).await;
	assert_eq!(counts(&again), nothing, "the noop's frozen counts");
	assert!(
		key_entry_when(&cluster, keys[0], settled).await == r_entry,
		"no retry admitted anything afresh"
	);
	shut_down(cluster, &time).await;
}

/// The key's entry on node `id`'s own applied decisions.
async fn key_entry_on(
	cluster: &Cluster,
	id: u32,
	key: ParentKey,
) -> Option<crate::consensus::decisions::KeyEntry> {
	cluster
		.node(id)
		.state_machine
		.read_state(move |state| state.decisions.as_ref()?.key_entry_for_test(&key))
		.await
}

/// Node `id`'s own settlement watermark.
async fn settled_on(cluster: &Cluster, id: u32) -> u64 {
	cluster
		.node(id)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.map_or(0, |decisions| decisions.generations().settled_ms)
		})
		.await
}

/// Waits until node `id` holds exactly `expected` for `key`.
async fn until_entry_on(
	cluster: &Cluster,
	id: u32,
	key: ParentKey,
	expected: crate::consensus::decisions::KeyEntry,
	what: &str,
) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let entry = key_entry_on(cluster, id, key).await;
		if entry == Some(expected) {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: node {id} holds {entry:?}, not {expected:?}"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

/// Wendelon ADR 0123 D6, V3 and V4 (same build, in process): a fresh write's live key is
/// settled by an ordinary retired-key generation. V3: a follower stopped and reopened replays
/// its control log and holds the identical settled receipt. V4: another follower, stopped
/// while the leader snapshots and purges past the settlement, catches up only by installing
/// that snapshot and holds the identical receipt; a retry through the coordinator then answers
/// its recorded commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_settled_receipt_survives_log_replay_and_snapshot_install() {
	use crate::consensus::decisions::RecordKind;
	let time = TestTime::new();
	let (mut cluster, leader, _, append) = windowed(&time).await;
	let t0 = time.clock.now_ms();
	write_one(&cluster, leader, &append, (1, "old"), uuid_v7_at(t0)).await;
	let r = write_one(&cluster, leader, &append, (2, "r"), uuid_v7_at(t0 + 60_000)).await;
	let key = key_of(&append, &r);
	let rsv = key_entry_when(&cluster, key, |_| true).await.reservation;
	jump_floor(&cluster, t0 + 30_000).await;
	until_covered(&cluster, rsv).await;
	let settled = key_entry_when(&cluster, key, |entry| {
		matches!(entry.record, RecordKind::Settled(_))
	})
	.await;
	let followers: Vec<u32> = NODES.into_iter().filter(|id| *id != leader).collect();
	for id in &followers {
		until_entry_on(&cluster, *id, key, settled, "applied").await;
	}

	// V3: log replay.
	let replayed = followers[0];
	cluster.stop(replayed, false).await;
	cluster.reopen(replayed).await;
	until_entry_on(&cluster, replayed, key, settled, "V3 after log replay").await;
	assert_eq!(
		settled_on(&cluster, replayed).await,
		settled_on(&cluster, leader).await,
		"V3: the replayed watermark"
	);
	eprintln!("ADR 0123 V3: node {replayed} replayed its log: {settled:?}");

	// V4: snapshot install past the settlement.
	let installed = followers[1];
	cluster.stop(installed, false).await;
	for step in 1..=3 {
		jump_floor(&cluster, t0 + 30_000 + step).await;
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
	// The purge is asynchronous: the member is reopened only once the leader's log is
	// purged past it, so it can catch up by the snapshot alone.
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.purged
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the leader never purged through {at}"
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	cluster.reopen(installed).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(installed)
		.raft
		.metrics()
		.borrow()
		.snapshot
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"node {installed} never installed the snapshot"
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	until_entry_on(
		&cluster,
		installed,
		key,
		settled,
		"V4 after snapshot install",
	)
	.await;
	eprintln!("ADR 0123 V4: node {installed} installed the snapshot through {at}: {settled:?}");
	let leader = cluster.leader().await;
	assert_eq!(
		settled_on(&cluster, installed).await,
		settled_on(&cluster, leader).await,
		"V4: the installed watermark"
	);
	eprintln!(
		"ADR 0123 V3/V4: the watermark replayed and installed: {}",
		settled_on(&cluster, leader).await
	);
	let leader = cluster.leader().await;
	let retry = write_through(&cluster, leader, r, named_rows(&append, &[(2, "r")])).await;
	assert!(
		matches!(retry.write.status, WriteStatus::OK | WriteStatus::DUPLICATE),
		"a retry after replay and install is its recorded commit: {retry:?}"
	);
	shut_down(cluster, &time).await;
}

/// The committed certificates of a watch, checked contiguous from gid 1 (no commit stepped
/// past), each attributed to its proposal reason by its target digest; also the proposals
/// that never committed (replaced or refused), by reason.
fn attribute(
	cluster: &Cluster,
	seen: &BTreeMap<u64, crate::consensus::decisions::generations::Certificate>,
	what: &str,
) -> (BTreeMap<&'static str, u64>, BTreeMap<&'static str, u64>) {
	let gids: Vec<u64> = seen.keys().copied().collect();
	let expected: Vec<u64> = (1..=gids.last().copied().unwrap_or(0)).collect();
	assert_eq!(gids, expected, "{what}: every commit observed");
	let mut committed = BTreeMap::new();
	for certificate in seen.values() {
		let reason = reason_of_certificate(cluster, certificate)
			.unwrap_or_else(|| panic!("{what}: certificate {} unattributed", certificate.gid));
		*committed.entry(reason.label()).or_insert(0) += 1;
	}
	let mut uncommitted = BTreeMap::new();
	for node in NODES {
		for (gid, digest, reason) in cluster.node(node).data_server.proposed_trace_for_test() {
			if !seen
				.get(&gid)
				.is_some_and(|certificate| certificate.target_digest == digest)
			{
				*uncommitted.entry(reason.label()).or_insert(0) += 1;
			}
		}
	}
	(committed, uncommitted)
}

/// ADR 0123 L2 (measurement, in process): steady load for k·S of test time on a 15 s
/// window, one write every 10 s, the clock moved only by the production advancer. Every
/// committed certificate is observed (contiguous gids) and attributed to its proposal reason
/// by its target DIGEST; proposals that never committed are counted apart; proposal attempts
/// (the exported counter's semantics) are reported apart again. Each member's certification
/// work (builds, base entries loaded, staged rows, activations, activation rows) is
/// reported. Asserted: committed settlements at most k + 1; and every write's identical
/// retry is still its recorded commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn steady_load_settles_at_most_once_per_interval_with_exact_answers() {
	const K: u64 = 3;
	const STEP_MS: u64 = 10_000;
	let time = TestTime::new();
	let (cluster, _, _, append) = windowed(&time).await;
	let watch = watch_certificates(&cluster, NODES[0]);
	let start = time.clock.now_ms();
	let mut written = Vec::new();
	let mut id = 0_i64;
	while time.clock.now_ms() < start + K * SETTLE_INTERVAL_MS {
		id += 1;
		let leader = cluster.leader().await;
		let meta = write_one(
			&cluster,
			leader,
			&append,
			(id, "s"),
			uuid_v7_at(time.clock.now_ms()),
		)
		.await;
		written.push((id, meta));
		let next = time.clock.now_ms() + STEP_MS;
		advance(&cluster, &time, next).await;
	}
	tokio::time::sleep(Duration::from_secs(2)).await;
	let seen = watch.finish().await;
	let (committed, uncommitted) = attribute(&cluster, &seen, "L2");
	let leader = cluster.leader().await;
	let attempts = cluster.node(leader).data_server.proposed_counts_for_test();
	eprintln!(
		"L2: {} writes over {} ms of test time; committed by reason {committed:?}; \
		 proposals never committed {uncommitted:?}; leader proposal attempts {attempts:?}",
		written.len(),
		time.clock.now_ms() - start
	);
	for node in NODES {
		let [builds, loaded, staged, activations, rows] =
			cluster.node(node).data_server.certification_work_for_test();
		eprintln!(
			"L2 node {node}: builds {builds}, base entries loaded {loaded}, staged rows {staged}, \
			 activations {activations}, activation rows {rows}"
		);
	}
	let settlements = committed.get("settlement").copied().unwrap_or(0);
	assert!(
		(1..=K + 1).contains(&settlements),
		"one to k + 1 committed settlements: {committed:?}"
	);
	let leader = cluster.leader().await;
	for (id, meta) in written {
		let retry = write_through(&cluster, leader, meta, named_rows(&append, &[(id, "s")])).await;
		assert!(
			matches!(retry.write.status, WriteStatus::OK | WriteStatus::DUPLICATE),
			"write {id}: an identical retry is its recorded commit: {retry:?}"
		);
	}
	shut_down(cluster, &time).await;
}

/// The reason a committed certificate's target was proposed with, attributed by its target
/// DIGEST in any node's proposal trace (a leader change keeps each proposer's own trace).
fn reason_of_certificate(
	cluster: &Cluster,
	certificate: &crate::consensus::decisions::generations::Certificate,
) -> Option<crate::consensus::decisions::generations::TargetReason> {
	NODES.into_iter().find_map(|node| {
		cluster
			.node(node)
			.data_server
			.proposed_trace_for_test()
			.into_iter()
			.find(|(gid, digest, _)| {
				*gid == certificate.gid && *digest == certificate.target_digest
			})
			.map(|(_, _, reason)| reason)
	})
}

/// A background watch of one voter's applied certificates (every commit applies on every
/// voter), polled every 10 ms. `finish` returns every certificate seen, by gid; a control
/// asserts the gids are contiguous, which shows no commit was stepped past.
struct CertificateWatch {
	seen:
		Arc<std::sync::Mutex<BTreeMap<u64, crate::consensus::decisions::generations::Certificate>>>,
	/// Each pending target first seen: its digest and the claim bytes its records at or
	/// below its cut held then (what its commit releases: no record at or below a cut is
	/// admitted after it).
	pending: Arc<std::sync::Mutex<BTreeMap<u64, ([u8; 32], u64)>>>,
	stop: Arc<std::sync::atomic::AtomicBool>,
	task: tokio::task::JoinHandle<()>,
}

fn watch_certificates(cluster: &Cluster, node: u32) -> CertificateWatch {
	let state_machine = cluster.node(node).state_machine.clone();
	let seen: Arc<std::sync::Mutex<BTreeMap<u64, _>>> = Default::default();
	let pending: Arc<std::sync::Mutex<BTreeMap<u64, ([u8; 32], u64)>>> = Default::default();
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let task = tokio::spawn({
		let (seen, pending, stop) = (seen.clone(), pending.clone(), stop.clone());
		async move {
			while !stop.load(std::sync::atomic::Ordering::Acquire) {
				let target = state_machine
					.read_state(|state| {
						let decisions = state.decisions.as_ref()?;
						let pending = decisions.generations().pending.as_ref()?;
						let t = pending.target.t;
						let bytes = decisions
							.bodies()
							.range(..=t)
							.map(|(_, body)| body.claim_bytes)
							.chain(decisions.noops().range(..=t).map(|(_, noop)| noop.bytes))
							.sum::<u64>();
						Some((pending.target.gid, pending.digest, bytes))
					})
					.await;
				if let Some((gid, digest, bytes)) = target {
					pending
						.lock()
						.unwrap()
						.entry(gid)
						.or_insert((digest, bytes));
				}
				let current = state_machine
					.read_state(|state| state.decisions.as_ref()?.generations().current.clone())
					.await;
				if let Some(certificate) = current {
					seen.lock()
						.unwrap()
						.entry(certificate.gid)
						.or_insert(certificate);
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		}
	});
	CertificateWatch {
		seen,
		pending,
		stop,
		task,
	}
}

impl CertificateWatch {
	async fn finish(self) -> BTreeMap<u64, crate::consensus::decisions::generations::Certificate> {
		self.stop.store(true, std::sync::atomic::Ordering::Release);
		let _ = self.task.await;
		let seen = self.seen.lock().unwrap().clone();
		seen
	}

	/// The pending targets seen so far: gid -> (digest, releasable claim bytes).
	fn pending(&self) -> BTreeMap<u64, ([u8; 32], u64)> {
		self.pending.lock().unwrap().clone()
	}
}

/// ADR 0123 M1: an outstanding read cut across a settlement. A governed row r, then a young
/// row y; a statement through the leader takes its cut and is held right after it
/// (`ReaderAfterCut`). The first settlement then commits (T4) and every member activates it,
/// so K_min (its commit index) rises past the held cut, which is at most the applied index
/// when the statement was held. Released, the statement is refused (`BACKPRESSURED`, the
/// reader's typed answer to `cut expired: below K_min`) and returns no rows; it never reads
/// erased decisions. A fresh statement then reads exactly the survivor y.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_outstanding_read_cut_is_refused_after_a_settlement_and_a_fresh_read_is_exact() {
	use crate::network::data_server::child_writes::PauseStage;
	let time = TestTime::new();
	let (cluster, leader, _, append) = windowed(&time).await;
	let issued = time.clock.now_ms();
	write_one(&cluster, leader, &append, (1, "r"), uuid_v7_at(issued)).await;
	advance(&cluster, &time, issued + SETTLE_INTERVAL_MS - 5_000).await;
	let leader = cluster.leader().await;
	write_one(
		&cluster,
		leader,
		&append,
		(2, "y"),
		uuid_v7_at(time.clock.now_ms()),
	)
	.await;
	let leader = cluster.leader().await;
	let (reached, release) = cluster
		.node(leader)
		.data_server
		.pause_coordinator_for_test(PauseStage::ReaderAfterCut);
	let server = cluster.node(leader).data_server.clone();
	let early = query_meta(&cluster, leader, &append);
	let reading = tokio::spawn(async move { server.table_query_for_test(early).await.unwrap() });
	reached.notified().await;
	let held_at = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.last_applied_log_id.map_or(0, |id| id.index))
		.await;
	advance(&cluster, &time, issued + SETTLE_INTERVAL_MS + 2_000).await;
	assert!(
		gid_within(&cluster, 1, Duration::from_secs(10)).await >= 1,
		"the first settlement committed"
	);
	let certificate = current_certificate(&cluster).await.unwrap();
	until_active(&cluster, certificate.gid).await;
	let k_min = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().generations().k_min())
		.await;
	assert!(
		k_min > held_at,
		"premise: K_min {k_min} passed the held cut (at most {held_at})"
	);
	release.notify_one();
	let answer = reading.await.unwrap();
	eprintln!(
		"M1: held at {held_at}, K_min {k_min}, the held statement: {:?}",
		answer.status
	);
	assert_eq!(
		answer.status,
		Some(crate::schema::ReadStatus::BACKPRESSURED),
		"the held cut is below K_min: {answer:?}"
	);
	assert!(answer.batches.is_empty(), "no erased decision is read");
	// The internal reason, which `BACKPRESSURED` does not carry: this node refused a read at a
	// cut no later than the held one as below K_min.
	let reasons: Vec<(u64, String)> = crate::network::data_server::reader::READ_REFUSALS
		.lock()
		.unwrap()
		.iter()
		.filter(|(node, k, _)| *node == leader && *k <= held_at)
		.map(|(_, k, reason)| (*k, reason.clone()))
		.collect();
	eprintln!("M1: leader {leader} refusals at or below the held cut: {reasons:?}");
	assert!(
		reasons
			.iter()
			.any(|(_, reason)| reason == "cut expired: below K_min"),
		"the held read was refused below K_min: {reasons:?}"
	);
	let leader = cluster.leader().await;
	append_rows_everywhere(&cluster, &append, &[(2, "y")], "a fresh read is exact").await;
	let fresh = cluster
		.node(leader)
		.data_server
		.table_query_for_test(query_meta(&cluster, leader, &append))
		.await
		.unwrap();
	assert_eq!(
		fresh.status,
		Some(crate::schema::ReadStatus::OK),
		"{fresh:?}"
	);
	shut_down(cluster, &time).await;
}

/// ADR 0123 L4 (in process): the watermark across a leadership transfer and a restart. Writes
/// go to the PK table (no governed row, so no age trigger moves the watermark). The first
/// settlement commits at stamp W1 and the watermark is exactly W1. A follower is then elected
/// leader and another member restarts; every member reads W1. A fresh write q follows: no
/// settlement before q is S old; then the next one, attributed to `Settlement` by its target
/// digest, at stamp W2 >= max(W1, q's issue) + S, and the watermark is exactly W2.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_watermark_survives_a_leader_change_and_a_restart_and_spaces_the_next_settlement() {
	use crate::consensus::decisions::generations::TargetReason;
	let time = TestTime::new();
	let (mut cluster, leader, pk, _) = windowed(&time).await;
	let issued = time.clock.now_ms();
	write_one(&cluster, leader, &pk, (1, "p"), uuid_v7_at(issued)).await;
	advance(&cluster, &time, issued + SETTLE_INTERVAL_MS + 2_000).await;
	assert!(gid_within(&cluster, 1, Duration::from_secs(10)).await >= 1);
	let first = current_certificate(&cluster).await.unwrap();
	assert_eq!(
		reason_of_certificate(&cluster, &first),
		Some(TargetReason::Settlement)
	);
	let w1 = first.retention.stamp_ms;
	assert_eq!(settled_ms(&cluster).await, w1);

	// Leadership transfer, then a restart of another member.
	let old = cluster.leader().await;
	let next = *NODES.iter().find(|id| **id != old).unwrap();
	cluster.node(next).raft.trigger().elect().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster.leader().await != next {
		assert!(
			tokio::time::Instant::now() < deadline,
			"node {next} never led"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	let restarted = *NODES.iter().find(|id| **id != next).unwrap();
	cluster.stop(restarted, false).await;
	cluster.reopen(restarted).await;
	for node in NODES {
		let deadline = tokio::time::Instant::now() + BOUND;
		while settled_on(&cluster, node).await != w1 {
			assert!(
				tokio::time::Instant::now() < deadline,
				"node {node} never read the watermark {w1}"
			);
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
	}
	eprintln!("L4: W1 {w1}; leader {old} -> {next}; node {restarted} restarted");

	// The next eligible settlement, exactly spaced.
	let q_issued = time.clock.now_ms();
	let leader = cluster.leader().await;
	write_one(&cluster, leader, &pk, (2, "q"), uuid_v7_at(q_issued)).await;
	let due = w1.max(q_issued) + SETTLE_INTERVAL_MS;
	advance(&cluster, &time, due + 2_000).await;
	assert!(gid_within(&cluster, first.gid + 1, Duration::from_secs(10)).await > first.gid);
	let second = current_certificate(&cluster).await.unwrap();
	assert_eq!(
		reason_of_certificate(&cluster, &second),
		Some(TargetReason::Settlement),
		"{second:?}"
	);
	assert_eq!(
		second.gid,
		first.gid + 1,
		"no other generation in between: the next one is this settlement"
	);
	let w2 = second.retention.stamp_ms;
	assert!(
		w2 >= due,
		"W2 {w2} is S past max(W1, q's issue) = {}",
		due - SETTLE_INTERVAL_MS
	);
	assert_eq!(
		settled_ms(&cluster).await,
		w2,
		"the watermark is exactly W2"
	);
	eprintln!("L4: W2 {w2} (due {due}), gid {}", second.gid);
	shut_down(cluster, &time).await;
}

/// The leader's retained claim bytes.
async fn retained_claim_bytes(cluster: &Cluster) -> u64 {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.map_or(0, |decisions| decisions.retained_claim_bytes())
		})
		.await
}

/// ADR 0123 L3 (in process; the production decision path and real admissions; a fixture-local
/// scaled cap and thresholds set on every voter, documented: cap 16·b, P 6·b, R 2·b, where b
/// is one PK write's claim bytes; the PK table, so no age trigger runs).
/// 1. A backlog of 8 writes with live keys; the fixture is set: retained ≥ P with ≥ R
///    releasable, so PRESSURE targets commit and release the backlog. Each committed
///    pressure generation, attributed by its target digest, released ≥ R (its records at or
///    below its cut when first pending).
/// 2. A parent h is held Reserved (`BeforeFinalize`). Writes above it are admitted until the
///    actual (scaled) admission cap refuses one; meanwhile no pressure target is proposed
///    over the tiny releasable prefix below h.
/// 3. Retries of a released backlog write and of an admitted write are their recorded
///    commits.
/// 4. h is released and concludes; a pressure target then covers everything and the
///    backlog is released.
/// Bound: committed pressure generations ≤ (backlog + admitted) · b / R.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sustained_pressure_releases_by_committed_generations_and_backpressures_at_the_cap() {
	use crate::consensus::decisions::generations::TargetReason;
	use crate::network::data_server::child_writes::PauseStage;
	const BACKLOG: i64 = 8;
	let time = TestTime::new();
	let (cluster, leader, pk, _) = windowed(&time).await;
	let t0 = time.clock.now_ms();
	let mut written = Vec::new();
	for id in 1..=BACKLOG {
		let leader = cluster.leader().await;
		let meta = write_one(&cluster, leader, &pk, (id, "b"), uuid_v7_at(t0 + id as u64)).await;
		written.push((id, meta));
	}
	let b = record_of(&cluster, key_of(&pk, &written[0].1))
		.await
		.expect("recorded")
		.claim_bytes;
	let (cap, mark, release) = (16 * b, 6 * b, 2 * b);
	let watch = watch_certificates(&cluster, NODES[0]);
	for node in NODES {
		cluster
			.node(node)
			.state_machine
			.set_pressure_fixture_for_test(Some((cap, mark, release)))
			.await;
	}
	let before = retained_claim_bytes(&cluster).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while retained_claim_bytes(&cluster).await >= mark {
		assert!(
			tokio::time::Instant::now() < deadline,
			"pressure never released the backlog: retained {}",
			retained_claim_bytes(&cluster).await
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	eprintln!(
		"L3 1: b {b}, cap {cap}, P {mark}, R {release}; retained {before} -> {}",
		retained_claim_bytes(&cluster).await
	);

	// 2. A held Reserved parent (held at its row's primary, which coordinates it), then
	// admissions to the cap.
	let leader = cluster.leader().await;
	let batch = named_rows(&pk, &[(100, "h")]);
	let primary = cluster
		.node(leader)
		.data_server
		.primary_for_test(TenantId::ZERO, &pk, &batch)
		.await
		.expect("every position is leased");
	let (reached, hold) = cluster
		.node(primary)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let held = write_meta(&pk, 1, uuid_v7_at(t0 + 100));
	let server = cluster.node(primary).data_server.clone();
	let holding = tokio::spawn(async move { write_retrying(&server, held, batch).await });
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the parent is held Reserved before Finalize");
	assert_eq!(
		record_of(&cluster, key_of(&pk, &held))
			.await
			.map(|record| record.status),
		Some(crate::consensus::decisions::ParentStatus::Reserved),
		"premise: h is Reserved"
	);
	let pressure_before = cluster
		.node(leader)
		.data_server
		.proposed_counts_for_test()
		.get("pressure")
		.copied()
		.unwrap_or(0);
	let mut admitted = 0;
	let mut refused = None;
	for id in 200..240_i64 {
		let meta = write_meta(&pk, 1, uuid_v7_at(t0 + id as u64));
		// One attempt, following only a redirect: a refusal at the cap stays visible.
		let mut entry = leader;
		let response = loop {
			let (response, _) = cluster
				.node(entry)
				.data_server
				.table_write_for_test(meta, named_rows(&pk, &[(id, "a")]))
				.await
				.unwrap();
			if response.write.status == WriteStatus::NOT_LEADER
				&& response.write.leader_hint.id != entry
			{
				entry = response.write.leader_hint.id;
				continue;
			}
			break response;
		};
		if response.write.status == WriteStatus::OK {
			admitted += 1;
			written.push((id, meta));
		} else {
			// The actual request and response of the refused admission.
			refused = Some((id, meta, response.write.status));
			break;
		}
	}
	let retained = retained_claim_bytes(&cluster).await;
	eprintln!(
		"L3 2: admitted {admitted} above the held parent; refused {refused:?}; retained {retained} (cap {cap})"
	);
	let (refused_id, refused_meta, refused_status) = refused.expect("the cap refuses an admission");
	assert!(retained <= cap, "the cap holds: {retained}");
	// That exact identity: no record and no retry key; and the reducer refused exactly its
	// Reserve for the claim cap (so no other stall can satisfy this premise).
	let refused_key = key_of(&pk, &refused_meta);
	assert!(
		record_of(&cluster, refused_key).await.is_none(),
		"the refused admission (id {refused_id}, {refused_status:?}) recorded nothing"
	);
	let leader_now = cluster.leader().await;
	assert_eq!(
		cluster
			.node(leader_now)
			.state_machine
			.read_state(move |state| state.decisions.as_ref()?.key_entry_for_test(&refused_key))
			.await,
		None,
		"no retry key for the refused identity"
	);
	let reducer_refusals: Vec<_> = crate::network::decision_proposals::RESERVE_REFUSALS
		.lock()
		.unwrap()
		.iter()
		.filter(|(key, _)| *key == refused_key)
		.map(|(_, refusal)| *refusal)
		.collect();
	assert!(
		!reducer_refusals.is_empty()
			&& reducer_refusals
				.iter()
				.all(|refusal| *refusal
					== crate::consensus::decisions::DecisionRefusal::RetainedFull),
		"the reducer refused exactly this Reserve for the cap: {reducer_refusals:?}"
	);
	let before_second_release = retained;
	tokio::time::sleep(Duration::from_secs(2)).await;
	assert_eq!(
		cluster
			.node(leader)
			.data_server
			.proposed_counts_for_test()
			.get("pressure")
			.copied()
			.unwrap_or(0),
		pressure_before,
		"no pressure target over the tiny prefix below the held parent"
	);

	// 3. Exact retries.
	for (id, meta) in [written[0], *written.last().unwrap()] {
		let retry = write_through(
			&cluster,
			leader,
			meta,
			named_rows(&pk, &[(id, if id <= BACKLOG { "b" } else { "a" })]),
		)
		.await;
		assert!(
			matches!(retry.write.status, WriteStatus::OK | WriteStatus::DUPLICATE),
			"write {id}: {retry:?}"
		);
	}

	// 4. Release the held parent; the backlog above it is released.
	hold.notify_one();
	let concluded = holding.await.unwrap();
	assert_eq!(
		concluded.write.status,
		WriteStatus::OK,
		"the held parent concludes once released: {concluded:?}"
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	while retained_claim_bytes(&cluster).await >= mark {
		assert!(
			tokio::time::Instant::now() < deadline,
			"the backlog above the released parent was never released: {}",
			retained_claim_bytes(&cluster).await
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	tokio::time::sleep(Duration::from_secs(1)).await;
	let pending = watch.pending();
	let seen = watch.finish().await;
	let (committed, uncommitted) = attribute(&cluster, &seen, "L3");
	let mut pressure_commits = 0;
	for certificate in seen.values() {
		if reason_of_certificate(&cluster, certificate) == Some(TargetReason::Pressure) {
			pressure_commits += 1;
			let (digest, releasable) = pending
				.get(&certificate.gid)
				.copied()
				.unwrap_or_else(|| panic!("gid {} never seen pending", certificate.gid));
			assert_eq!(digest, certificate.target_digest);
			assert!(
				releasable >= release,
				"pressure gid {} released {releasable} < R {release}",
				certificate.gid
			);
		}
	}
	// The bound from actual claim bytes: the retained totals before each release (no other
	// erasure runs in this fixture: no age, no settlement, no retired key).
	let admitted_bytes = before + before_second_release;
	eprintln!(
		"L3: committed by reason {committed:?}; never committed {uncommitted:?}; pressure commits {pressure_commits} <= ({before} + {before_second_release}) / {release}"
	);
	assert!(
		pressure_commits >= 2,
		"pressure released twice: {committed:?}"
	);
	assert!(pressure_commits as u64 <= admitted_bytes / release);
	for node in NODES {
		let [builds, loaded, staged, activations, rows] =
			cluster.node(node).data_server.certification_work_for_test();
		eprintln!(
			"L3 node {node}: builds {builds}, base entries loaded {loaded}, staged rows {staged}, \
			 activations {activations}, activation rows {rows}"
		);
	}
	shut_down(cluster, &time).await;
}
