//! DC-271 regression: ordinary cleanup must not retire a version's debt while
//! the worker that will append that version can still append it.
//!
//! The guard that already exists: `refuse_recorded_intents` runs at the start of
//! each maintenance pass and takes the reservation stripe of every
//! Intent/Complete intent, so a store's pass waits for a worker paused after its
//! begin. That guard acts only at the sweep point. This schedule begins the
//! intent after a pass's sweep and before the same pass's activation:
//! 1. an unrelated row is overwritten (its first version becomes eligible debt
//!    at generation 1) and the store's active slab sealed;
//! 2. the worker is paused before its begin (decision checked while Reserved);
//! 3. the store's activation is held; the parent is refused by a valid abort,
//!    its key retired, and generation 1 commits;
//! 4. the store's next pass is paused at its certification step (after its
//!    refused-intent sweep, before activation);
//! 5. the worker is released into its begin (the store's installed cut is still
//!    genesis, so DC-269 admits it) and paused again right after;
//! 6. activation runs in that pass, with cleanup paused: the exact debt is
//!    recorded; cleanup resumes: the sweep tries the worker's debt while its
//!    stripe is busy and leaves it, while the unrelated debt is retired;
//! 7. the worker is released: its late copy is appended under that debt;
//! 8. the store's active slab is sealed: cleanup erases the copy, then the debt
//!    retires. No copy remains without debt or owner.

use super::*;
use crate::network::data_server::child_writes::persist_pause;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_worker_begun_after_the_sweep_leaves_no_copy_without_debt() {
	use crate::network::data_server::child_writes::PauseStage;
	use crate::storage::index::certified_base::{DebtRecord, DebtSource, VersionKey};
	use crate::storage::index::parent_intents::OwnedVersion;
	use std::sync::atomic::Ordering::{Acquire, Release};
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let tenant = TenantId::ZERO;
	let batch = named_rows(&descriptor, &[(71, "paused")]);
	let store = cluster
		.node(leader)
		.data_server
		.primary_for_test(tenant, &descriptor, &batch)
		.await
		.unwrap();
	let server = cluster.node(store).data_server.clone();
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	let all_debt = || {
		let index = index.clone();
		async move { index.debt_page(None, 1 << 20).await.unwrap() }
	};
	let named = |debt: &[(VersionKey, DebtRecord)], manifest: &[OwnedVersion]| {
		debt.iter()
			.filter(|(key, _)| {
				manifest.iter().any(|version| {
					key.table == version.table
						&& key.key_identity == version.key_identity
						&& key.hlc == version.hlc
				})
			})
			.cloned()
			.collect::<Vec<_>>()
	};
	let owners_of = |manifest: Vec<OwnedVersion>| {
		let index = index.clone();
		async move {
			let mut found = Vec::new();
			for version in manifest {
				found.push(index.version_owner(TenantId::ZERO, version).await.unwrap());
			}
			found
		}
	};
	let until = |what: &'static str, deadline: std::time::Duration| {
		let start = tokio::time::Instant::now();
		move || {
			assert!(start.elapsed() < deadline, "never: {what}");
		}
	};

	// 1. An unrelated row overwritten; its first version is debt at generation 1.
	for value in ["first", "second"] {
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(90, value)]),
			)
			.await,
			"an unrelated overwrite",
		);
	}
	slab.force_seal_active_for_test().await;

	// 2. The worker passes its decision check while Reserved, then pauses.
	let before = persist_pause::arm(&slab, descriptor.table_object_id);
	let after = persist_pause::arm_after_begin(&slab, descriptor.table_object_id);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let writer = server.clone();
	let writing = tokio::spawn(async move { writer.table_write_for_test(meta, batch).await });
	tokio::time::timeout(BOUND, before.reached.notified())
		.await
		.expect("the worker reached its begin");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("the parent is reserved");
	let rsv = record.rsv;

	// 3. Activation held here; refused, retired, generation 1 committed.
	let hold = server.decision_peer_for_test().hold_activation.clone();
	hold.store(true, Release);
	abort(&cluster, &record).await;
	jump_floor(&cluster, now + 60_000).await;
	cluster
		.until("generation 1 committed", |state| {
			state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.generations().current_gid() >= 1)
		})
		.await;
	let t = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().generations().fr())
		.await;
	assert!(
		t >= rsv,
		"premise: generation 1 covers the reservation ({t} >= {rsv})"
	);
	assert_eq!(
		index.base_meta().await.unwrap().active_gid,
		0,
		"premise: held here"
	);

	// 4. This store's next pass pauses at its certification step, after its sweep.
	let (step_reached, step_release) =
		server.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, step_reached.notified())
		.await
		.expect("the pass reached its certification step");

	// 5. The worker begins (genesis cut here) and pauses right after.
	before.release.notify_one();
	tokio::time::timeout(BOUND, after.reached.notified())
		.await
		.expect("the worker began its intent");
	let manifest = after.manifest();
	assert!(!manifest.is_empty());
	let begun = index.parent_intent(rsv, 0).await.unwrap();
	eprintln!(
		"DC-271 begun after the sweep: rsv={rsv} t={t} store={store} intent={:?} owners={:?} copies={:?} active_gid={}",
		begun.as_ref().map(|intent| intent.state),
		owners_of(manifest.clone()).await,
		index
			.owned_version_copies(tenant, manifest.clone())
			.await
			.unwrap(),
		index.base_meta().await.unwrap().active_gid,
	);
	assert!(begun.is_some(), "premise: the intent is recorded");

	// 6. Activation in this pass, with cleanup paused to read the exact debt.
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	hold.store(false, Release);
	step_release.notify_one();
	let check = until("the store activated", BOUND);
	while index.base_meta().await.unwrap().active_gid < 1 {
		check();
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	let recorded = all_debt().await;
	let mine = named(&recorded, &manifest);
	let unrelated = recorded.len() - mine.len();
	eprintln!(
		"DC-271 activated: intent={:?} owners={:?} mine={mine:?} unrelated={unrelated}",
		index
			.parent_intent(rsv, 0)
			.await
			.unwrap()
			.map(|intent| intent.state),
		owners_of(manifest.clone()).await,
	);
	assert_eq!(
		mine.len(),
		manifest.len(),
		"premise: the exact debt is recorded"
	);
	assert!(
		mine.iter()
			.all(|(_, record)| record.source == DebtSource::RetiredIntent { rsv, ordinal: 0 }),
		"{mine:?}"
	);
	assert!(unrelated > 0, "premise: unrelated eligible debt exists");
	// Cleanup resumes while the worker is paused: the sweep tries the worker's
	// debt with its stripe busy and leaves it; the unrelated debt retires.
	let skips = server.decision_peer_for_test().debt_busy_skips.clone();
	let skipped_before = skips.load(Acquire);
	pause.store(false, Release);
	let check = until(
		"the sweep tried the busy debt and retired the unrelated one",
		BOUND,
	);
	loop {
		let now_debt = all_debt().await;
		if skips.load(Acquire) > skipped_before
			&& now_debt.len() == named(&now_debt, &manifest).len()
		{
			break;
		}
		check();
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	let kept = named(&all_debt().await, &manifest);
	eprintln!(
		"DC-271 swept while busy: busy skips {} -> {}, the worker's debt kept={kept:?}, unrelated retired",
		skipped_before,
		skips.load(Acquire)
	);
	assert_eq!(kept, mine, "the worker's exact debt survives the sweep");

	// 7. The worker resumes and ends: its late copy lands under that debt.
	after.release.notify_one();
	tokio::time::timeout(BOUND, after.ended.notified())
		.await
		.expect("the worker's spawned work ended");
	let caller = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends")
		.unwrap();
	let copies = index
		.owned_version_copies(tenant, manifest.clone())
		.await
		.unwrap();
	let after_append = named(&all_debt().await, &manifest);
	eprintln!(
		"DC-271 worker ended: caller={:?} copies={copies:?} debt={after_append:?}",
		caller.as_ref().map(|(meta, _)| meta.write.status)
	);
	assert!(
		copies.iter().all(|copies| *copies > 0),
		"the late copy was appended"
	);
	assert_eq!(after_append, mine, "the debt still names the late copy");

	// 8. Sealed, cleanup erases the copy, then the debt retires.
	slab.force_seal_active_for_test().await;
	let check = until("the late copy erased and its debt retired", BOUND);
	loop {
		let copies = index
			.owned_version_copies(tenant, manifest.clone())
			.await
			.unwrap();
		let debt = named(&all_debt().await, &manifest);
		if copies.iter().all(|copies| *copies == 0) && debt.is_empty() {
			break;
		}
		check();
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	eprintln!(
		"DC-271 reclaimed: copies={:?} debt=[] owners={:?}",
		index
			.owned_version_copies(tenant, manifest.clone())
			.await
			.unwrap(),
		owners_of(manifest.clone()).await
	);
}
