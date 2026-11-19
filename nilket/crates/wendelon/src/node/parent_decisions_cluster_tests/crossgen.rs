//! DC-272 regression: a repair in flight across two activations must not append
//! a version after its debt retired. Before the fix (`crossgen-3.log` on
//! `35ec53cc5`) generation 1's activation retired the repair's Complete intent
//! (no debt: its version was the winner), generation 2 displaced that version,
//! cleanup erased it and retired its debt, and the resumed repair appended it:
//! one indexed copy with no debt, owner or base entry.
//!
//! The schedule, on one store S, on production paths:
//! 1. row K is written (P1 Accepted; W1 stored on S under a Complete intent); S's
//!    active slab is sealed;
//! 2. generation 1 stays uncommitted while S has reported (a third member's pass
//!    paused before its report; S's activation held), since a committed
//!    generation erases P1's body and no later repair passes its decision check;
//! 3. S's next pass pauses at its certification step, after its sweep;
//! 4. a repair of P1's child on S passes its decision check, finds the Complete
//!    intent at begin, and pauses right after;
//! 5. the third member reports, generation 1 commits; S's activation is
//!    attempted: fenced by the paused repair's stripe, it waits, and the local
//!    gid and the intent stay;
//! 6. the repair resumes and ends; S activates generation 1;
//! 7. K is written again (W2), sealed; generation 2 commits and activates;
//! 8. W1 is reclaimed exactly (zero indexed copies, no debt, no owner) and W2
//!    wins and survives.

use super::*;
use crate::network::data_server::child_writes::persist_pause;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_repair_in_flight_across_two_generations_leaves_no_copy_without_debt() {
	use crate::network::data_server::child_writes::PauseStage;
	use crate::storage::index::certified_base::{DebtRecord, VersionKey};
	use crate::storage::index::parent_intents::OwnedVersion;
	use std::sync::atomic::Ordering::Release;
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let tenant = TenantId::ZERO;
	let tick = std::time::Duration::from_millis(50);
	// S: a follower holding the row (every node holds every row here).
	let store = *NODES.iter().find(|id| **id != leader).unwrap();
	let server = cluster.node(store).data_server.clone();
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	let named = |debt: Vec<(VersionKey, DebtRecord)>, version: OwnedVersion| {
		debt.into_iter()
			.filter(|(key, _)| {
				key.table == version.table
					&& key.key_identity == version.key_identity
					&& key.hlc == version.hlc
			})
			.collect::<Vec<_>>()
	};

	// 1. P1 writes W1; sealed.
	let first = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_through(
			&cluster,
			leader,
			first,
			named_rows(&descriptor, &[(80, "w1")]),
		)
		.await,
		"the first version",
	);
	let p1 = record_of(&cluster, key_of(&descriptor, &first))
		.await
		.expect("P1");
	let held = index
		.parent_intent(p1.rsv, 0)
		.await
		.unwrap()
		.expect("P1's intent on S");
	assert_eq!(held.manifest.len(), 1, "one version");
	let w1 = held.manifest[0];
	assert_eq!(
		index.owned_version_copies(tenant, vec![w1]).await.unwrap(),
		vec![1]
	);
	slab.force_seal_active_for_test().await;

	// 2. Generation 1 stays uncommitted while S has reported: a third member X's
	//    pass pauses at its certification step (before its report); S's
	//    activation is held. A committed generation erases P1's body, so the
	//    repair must pass its decision check before that commit.
	let third = *NODES
		.iter()
		.find(|id| **id != leader && **id != store)
		.unwrap();
	let (x_reached, x_release) = cluster
		.node(third)
		.data_server
		.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, x_reached.notified())
		.await
		.expect("X's pass paused before its report");
	let hold = server.decision_peer_for_test().hold_activation.clone();
	hold.store(true, Release);
	let floor1 = now + 60_000;
	jump_floor(&cluster, floor1).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let reported = cluster
			.node(leader)
			.state_machine
			.read_state(move |state| {
				let generations = state.decisions.as_ref()?.generations();
				let pending = generations.pending.as_ref()?;
				Some((
					generations.current_gid(),
					pending.target.gid,
					pending.reports.contains_key(&store),
				))
			})
			.await;
		if let Some((current, target, true)) = reported {
			assert_eq!((current, target), (0, 1), "premise: generation 1 pending");
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"S never reported generation 1"
		);
		tokio::time::sleep(tick).await;
	}

	// 3. S's next pass pauses at its certification step, after its sweep.
	let (step_reached, step_release) =
		server.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, step_reached.notified())
		.await
		.expect("S's pass reached its certification step");

	// 4. A repair of P1's child passes its decision check (P1 Accepted, generation
	//    1 uncommitted), begins (Complete intent) and pauses after begin.
	let after = persist_pause::arm_after_begin(&slab, descriptor.table_object_id);
	let repairer = server.clone();
	let generation = descriptor.schema_generation;
	let rsv = p1.rsv;
	let mut repairing =
		tokio::spawn(async move { repairer.repair_child_for_test(rsv, 0, generation).await });
	tokio::select! {
		_ = after.reached.notified() => {}
		ended = &mut repairing => panic!("the repair ended before its begin pause: {ended:?}"),
		_ = tokio::time::sleep(BOUND) => panic!("the repair neither began nor ended"),
	}
	eprintln!(
		"CROSSGEN begun: rsv={rsv} store={store} intent={:?} owner={:?} copies={:?} active_gid={}",
		index
			.parent_intent(rsv, 0)
			.await
			.unwrap()
			.map(|intent| intent.state),
		index.version_owner(tenant, w1).await.unwrap(),
		index.owned_version_copies(tenant, vec![w1]).await.unwrap(),
		index.base_meta().await.unwrap().active_gid,
	);
	// The paused producer is the repair of a Complete intent, owning W1.
	assert_eq!(
		index
			.parent_intent(rsv, 0)
			.await
			.unwrap()
			.map(|intent| intent.state),
		Some(crate::storage::index::parent_intents::ParentIntentState::Complete),
		"premise: the repair re-entered a Complete intent"
	);
	assert_eq!(
		index.version_owner(tenant, w1).await.unwrap(),
		Some((p1.rsv, 0)),
		"premise: W1 owned by (p1.rsv, 0)"
	);
	// X reports; generation 1 commits (P1's body erased).
	x_release.notify_one();
	cluster
		.until("generation 1 committed", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().current_gid() >= 1)
		})
		.await;

	// 5. S's activation of generation 1 is attempted: fenced by the paused
	//    repair's stripe, it waits; the local gid and intent stay in place.
	let waits = &server.decision_peer_for_test().locks.fence_waits;
	let waited_before = waits.load(std::sync::atomic::Ordering::Acquire);
	hold.store(false, Release);
	step_release.notify_one();
	let deadline = tokio::time::Instant::now() + BOUND;
	while waits.load(std::sync::atomic::Ordering::Acquire) == waited_before {
		assert!(
			tokio::time::Instant::now() < deadline,
			"no activation attempt was fenced"
		);
		tokio::time::sleep(tick).await;
	}
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	assert_eq!(
		index.base_meta().await.unwrap().active_gid,
		0,
		"activation waits for the repair"
	);
	assert!(
		index.parent_intent(rsv, 0).await.unwrap().is_some(),
		"the intent stays while its producer runs"
	);
	eprintln!(
		"CROSSGEN fenced: activation waited on rsv {rsv}'s stripe; active_gid=0, intent kept"
	);

	// 6. The repair resumes and ends (its final decision check may refuse: G1
	//    committed); then S activates generation 1.
	after.release.notify_one();
	let repaired = tokio::time::timeout(BOUND, repairing)
		.await
		.expect("the repair returns")
		.unwrap();
	eprintln!("CROSSGEN repair: {repaired:?}");
	let deadline = tokio::time::Instant::now() + BOUND;
	while index.base_meta().await.unwrap().active_gid < 1 {
		assert!(
			tokio::time::Instant::now() < deadline,
			"S never activated generation 1"
		);
		tokio::time::sleep(tick).await;
	}
	assert!(
		index.parent_intent(rsv, 0).await.unwrap().is_none(),
		"P1's intent retired"
	);
	assert_eq!(
		index.owned_version_copies(tenant, vec![w1]).await.unwrap(),
		vec![1]
	);

	// 7. K written again; sealed; generation 2 displaces W1. The expected W2 is
	//    taken from the second write's own intent on S, before the floor moves.
	let second = write_meta(&descriptor, 1, uuid_v7_at(floor1 + 1));
	assert_committed(
		&write_through(
			&cluster,
			leader,
			second,
			named_rows(&descriptor, &[(80, "w2")]),
		)
		.await,
		"the second version",
	);
	let p2 = record_of(&cluster, key_of(&descriptor, &second))
		.await
		.expect("P2");
	let w2 = {
		let intent = index
			.parent_intent(p2.rsv, 0)
			.await
			.unwrap()
			.expect("P2's intent on S");
		assert_eq!(intent.manifest.len(), 1, "one version");
		intent.manifest[0]
	};
	assert_eq!(
		(w2.table, w2.key_identity),
		(w1.table, w1.key_identity),
		"premise: W2 is K's next version"
	);
	assert_ne!(w2.hlc, w1.hlc);
	slab.force_seal_active_for_test().await;
	jump_floor(&cluster, now + 120_000).await;
	until_active(&cluster, 2).await;

	// 8. W1 is reclaimed exactly: zero indexed copies, no debt, no owner; W2 wins
	//    and survives.
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let copies = index.owned_version_copies(tenant, vec![w1]).await.unwrap();
		let debt = named(index.debt_page(None, 1 << 20).await.unwrap(), w1);
		if copies == vec![0] && debt.is_empty() {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"W1 never reclaimed: copies={copies:?} debt={debt:?}"
		);
		tokio::time::sleep(tick).await;
	}
	assert_eq!(index.version_owner(tenant, w1).await.unwrap(), None);
	let winner = index
		.base_entries(false)
		.await
		.unwrap()
		.into_iter()
		.find(|entry| entry.table == w1.table && entry.key_identity == w1.key_identity)
		.expect("K has a base entry");
	assert_eq!(
		(
			winner.table,
			winner.key_identity,
			winner.hlc,
			winner.row_digest,
			winner.tombstone
		),
		(w2.table, w2.key_identity, w2.hlc, w2.row_digest, false),
		"the winner is exactly the expected W2"
	);
	assert_eq!(
		index.owned_version_copies(tenant, vec![w2]).await.unwrap(),
		vec![1],
		"W2 survives"
	);
	eprintln!("CROSSGEN reclaimed: W1 copies=0, no debt, no owner; W2 copies=1 and wins");
}
