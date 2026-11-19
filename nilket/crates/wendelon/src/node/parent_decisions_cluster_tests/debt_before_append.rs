//! DC-271 and DC-272 regressions: ordinary cleanup must never leave a copy that
//! no debt, owner or base entry names.
//!
//! `a_worker_begun_after_the_sweep_is_fenced_by_activation` (the DC-271
//! schedule, under DC-272's stronger ordering):
//! 1. the worker is paused before its begin (decision checked while Reserved);
//! 2. the store's activation is held; the parent is refused by a valid abort,
//!    its key retired, and generation 1 commits;
//! 3. the store's next pass is paused at its certification step (after its
//!    refused-intent sweep, before activation);
//! 4. the worker is released into its begin (the store's installed cut is still
//!    genesis, so DC-269 admits it) and paused again right after;
//! 5. activation is attempted: fenced by the worker's stripe, it waits; the
//!    local gid and the intent stay;
//! 6. the worker is released and ends; activation then retires its intent and
//!    records exact debt for the copy it appended;
//! 7. sealed, cleanup erases the copy, then the debt retires.
//!
//! `a_busy_stripe_skips_its_debt_while_unrelated_debt_progresses` (DC-271's
//! sweep guard, directly): with a producer stripe held, the sweep skips that
//! stripe's retired-intent debt and revisits it after release, while unrelated
//! debt retires meanwhile.

use super::*;
use crate::network::data_server::child_writes::persist_pause;
use crate::storage::index::certified_base::{DebtRecord, DebtSource, VersionKey};
use crate::storage::index::parent_intents::OwnedVersion;

fn named(
	debt: &[(VersionKey, DebtRecord)],
	manifest: &[OwnedVersion],
) -> Vec<(VersionKey, DebtRecord)> {
	debt.iter()
		.filter(|(key, _)| {
			manifest.iter().any(|version| {
				key.table == version.table
					&& key.key_identity == version.key_identity
					&& key.hlc == version.hlc
			})
		})
		.cloned()
		.collect()
}

async fn wait_for(what: &str, mut ready: impl AsyncFnMut() -> bool) {
	let deadline = tokio::time::Instant::now() + BOUND;
	while !ready().await {
		assert!(tokio::time::Instant::now() < deadline, "never: {what}");
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_worker_begun_after_the_sweep_is_fenced_by_activation() {
	use crate::network::data_server::child_writes::PauseStage;
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

	// 1. The worker passes its decision check while Reserved, then pauses.
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

	// 2. Activation held here; refused, retired, generation 1 committed.
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

	// 3. This store's next pass pauses at its certification step, after its sweep.
	let (step_reached, step_release) =
		server.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, step_reached.notified())
		.await
		.expect("the pass reached its certification step");

	// 4. The worker begins (genesis cut here) and pauses right after.
	before.release.notify_one();
	tokio::time::timeout(BOUND, after.reached.notified())
		.await
		.expect("the worker began its intent");
	let manifest = after.manifest();
	assert!(!manifest.is_empty());
	assert!(
		index.parent_intent(rsv, 0).await.unwrap().is_some(),
		"premise: intent recorded"
	);
	assert_eq!(
		index
			.owned_version_copies(tenant, manifest.clone())
			.await
			.unwrap(),
		vec![0; manifest.len()],
		"premise: nothing appended yet"
	);

	// 5. Activation is attempted: fenced by the worker's stripe, it waits.
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	let waits = &server.decision_peer_for_test().locks.fence_waits;
	let waited_before = waits.load(Acquire);
	hold.store(false, Release);
	step_release.notify_one();
	wait_for("an activation attempt fenced by the worker", async || {
		waits.load(Acquire) > waited_before
	})
	.await;
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	assert_eq!(
		index.base_meta().await.unwrap().active_gid,
		0,
		"activation waits"
	);
	assert!(
		index.parent_intent(rsv, 0).await.unwrap().is_some(),
		"the intent stays"
	);
	eprintln!("DC-272 fenced the DC-271 worker: rsv={rsv} t={t} store={store} active_gid=0");

	// 6. The worker ends; activation then retires its intent with exact debt
	//    for the copy it appended.
	after.release.notify_one();
	tokio::time::timeout(BOUND, after.ended.notified())
		.await
		.expect("the worker's append block ended");
	let caller = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends")
		.unwrap();
	eprintln!(
		"DC-272 worker caller: {:?}",
		caller.as_ref().map(|(meta, _)| meta.write.status)
	);
	wait_for("the store activated generation 1", async || {
		index.base_meta().await.unwrap().active_gid >= 1
	})
	.await;
	assert!(
		index.parent_intent(rsv, 0).await.unwrap().is_none(),
		"the intent retired"
	);
	let copies = index
		.owned_version_copies(tenant, manifest.clone())
		.await
		.unwrap();
	let debt = named(&index.debt_page(None, 1 << 20).await.unwrap(), &manifest);
	eprintln!("DC-272 after activation: copies={copies:?} debt={debt:?}");
	assert!(
		copies.iter().all(|copies| *copies > 0),
		"the worker's copy was appended"
	);
	assert_eq!(
		debt.len(),
		manifest.len(),
		"every appended version has debt"
	);
	assert!(
		debt.iter()
			.all(|(_, record)| record.source == DebtSource::RetiredIntent { rsv, ordinal: 0 }),
		"{debt:?}"
	);

	// 7. Sealed, cleanup erases the copy, then the debt retires.
	slab.force_seal_active_for_test().await;
	pause.store(false, Release);
	wait_for("the copy erased and its debt retired", async || {
		index
			.owned_version_copies(tenant, manifest.clone())
			.await
			.unwrap()
			.iter()
			.all(|copies| *copies == 0)
			&& named(&index.debt_page(None, 1 << 20).await.unwrap(), &manifest).is_empty()
	})
	.await;
	for version in &manifest {
		assert_eq!(index.version_owner(tenant, *version).await.unwrap(), None);
	}
	eprintln!("DC-272 reclaimed: copies=0, no debt, no owner");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_busy_stripe_skips_its_debt_while_unrelated_debt_progresses() {
	use std::sync::atomic::Ordering::{Acquire, Release};
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let tenant = TenantId::ZERO;
	// Two rows, each overwritten: each first version is retired-intent debt at
	// generation 1, under its own parent's reservation.
	let mut firsts = Vec::new();
	for row in [90, 91] {
		let first = write_meta(&descriptor, 1, uuid_v7());
		assert_committed(
			&write_through(
				&cluster,
				leader,
				first,
				named_rows(&descriptor, &[(row, "first")]),
			)
			.await,
			"a first version",
		);
		firsts.push(
			record_of(&cluster, key_of(&descriptor, &first))
				.await
				.expect("first")
				.rsv,
		);
		assert_committed(
			&write_through(
				&cluster,
				leader,
				write_meta(&descriptor, 1, uuid_v7()),
				named_rows(&descriptor, &[(row, "second")]),
			)
			.await,
			"a second version",
		);
	}
	let store = *NODES.iter().find(|id| **id != leader).unwrap();
	let server = cluster.node(store).data_server.clone();
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	let locks = &server.decision_peer_for_test().locks;
	assert_ne!(
		firsts[0] % locks.stripe_count() as u64,
		firsts[1] % locks.stripe_count() as u64,
		"premise: distinct stripes"
	);
	slab.force_seal_active_for_test().await;
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let debt_of = |rsv: u64| {
		let index = index.clone();
		async move {
			index
				.debt_page(None, 1 << 20)
				.await
				.unwrap()
				.into_iter()
				.filter(|(_, record)| {
					record.source == DebtSource::RetiredIntent { rsv, ordinal: 0 }
				})
				.count()
		}
	};
	assert!(
		debt_of(firsts[0]).await > 0 && debt_of(firsts[1]).await > 0,
		"premise: both debts"
	);
	// The first row's producer stripe is held; cleanup resumes.
	let held = locks.lock_for_test(firsts[0]).await;
	let skips = server.decision_peer_for_test().debt_busy_skips.clone();
	let skipped_before = skips.load(Acquire);
	pause.store(false, Release);
	wait_for(
		"the unrelated debt retired while the busy one was skipped",
		async || debt_of(firsts[1]).await == 0 && skips.load(Acquire) > skipped_before,
	)
	.await;
	assert!(debt_of(firsts[0]).await > 0, "the busy stripe's debt stays");
	eprintln!(
		"DC-271 direct: busy skips {skipped_before} -> {}; unrelated retired; busy kept",
		skips.load(Acquire)
	);
	// Released, the sweep revisits it on wrap.
	drop(held);
	wait_for("the skipped debt retired after release", async || {
		debt_of(firsts[0]).await == 0
	})
	.await;
	let _ = tenant;
}
