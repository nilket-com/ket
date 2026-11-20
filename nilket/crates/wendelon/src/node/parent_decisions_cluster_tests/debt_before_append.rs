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

/// DROP-18 / DC-276 baseline (Codex 01a1f7a5, strengthened per 01a1f7a9),
/// test-only. One refused child version on one store. The worker begins its
/// intent (owners recorded) and pauses; the coordinator's signed abort refuses
/// the parent; then the worker appends. In the failure arm the store's next index
/// apply is armed to fail, so the version's bytes are durable and its record row
/// is never committed (the store is restart-required). Activation and the
/// ordinary debt sweep then run, and the same store reopens.
///
/// Evidence is exact: the same intent `(rsv, 0)`, an owner of exactly that
/// version, or debt at exactly its `VersionKey` carrying its manifest
/// `row_digest`. A recovered copy is identified by its stored row's
/// `stored_row_digest` (computed with the table's descriptor) equal to that
/// manifest digest, at a resolved location and ordinal.
async fn refused_version_across_activation_sweep_and_reopen(fail_publication: bool) {
	use std::sync::atomic::Ordering::Release;
	let (mut cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let tenant = TenantId::ZERO;
	let batch = named_rows(&descriptor, &[(81, "refused")]);
	let store = cluster
		.node(leader)
		.data_server
		.primary_for_test(tenant, &descriptor, &batch)
		.await
		.unwrap();
	let server = cluster.node(store).data_server.clone();
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	let incarnation = index.store_incarnation();

	// The worker begins its intent and pauses before appending.
	let after = persist_pause::arm_after_begin(&slab, descriptor.table_object_id);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let writer = server.clone();
	let writing = tokio::spawn(async move { writer.table_write_for_test(meta, batch).await });
	tokio::time::timeout(BOUND, after.reached.notified())
		.await
		.expect("premise: the worker began its intent");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("premise: the parent is reserved");
	let rsv = record.rsv;
	let manifest = after.manifest();
	assert_eq!(manifest.len(), 1, "premise: one version");
	let version = manifest[0];
	let version_key = VersionKey {
		tenant,
		table: version.table,
		key_identity: version.key_identity,
		hlc: version.hlc,
	};
	assert_eq!(
		version.table, descriptor.table_object_id,
		"premise: the table"
	);
	// Exact cleanup evidence for this version: (intent, owner, debt with its digest).
	let evidence = |index: Arc<crate::storage::index::PersistentLocalIndex>| async move {
		let intent = index.parent_intent(rsv, 0).await.unwrap().is_some();
		let owner = index
			.version_owner(tenant, version)
			.await
			.unwrap()
			.is_some();
		let debt: Vec<_> = index
			.debt_page(None, 1 << 20)
			.await
			.unwrap()
			.into_iter()
			.filter(|(key, record)| *key == version_key && record.row_digest == version.row_digest)
			.collect();
		(intent, owner, debt)
	};
	let (intent, owner, _) = evidence(index.clone()).await;
	assert!(intent && owner, "premise: intent and owner");

	// The coordinator's signed abort refuses the parent; cleanup and the sweep
	// are held on this store so each step is observed.
	abort(&cluster, &record).await;
	let pause = server.decision_peer_for_test().pause_cleanup.clone();
	pause.store(true, Release);
	if fail_publication {
		index.fail_next_delta.store(true, Release);
	}
	after.release.notify_one();
	tokio::time::timeout(BOUND, after.ended.notified())
		.await
		.expect("premise: the worker's append block ended");
	let caller = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends")
		.unwrap();
	let copies = index
		.owned_version_copies(tenant, manifest.clone())
		.await
		.unwrap();
	let readiness = slab.tier_authority_readiness_reason();
	eprintln!(
		"DC-276 fail={fail_publication} store={store} rsv={rsv} version={version:?}: caller={:?} prefix_records={copies:?} readiness={readiness:?}",
		caller.as_ref().map(|(meta, _)| meta.write.status)
	);
	if fail_publication {
		assert_eq!(
			copies,
			vec![0],
			"premise: no record at the version's prefix"
		);
		assert_eq!(
			readiness.as_deref(),
			Some("TierAuthorityPublicationPending(RestartRequired)"),
			"premise: the fault hit this child's row publication"
		);
	} else {
		assert_eq!(copies, vec![1], "premise: the version is indexed");
	}
	let (intent, owner, _) = evidence(index.clone()).await;
	assert!(intent && owner, "premise: intent and owner stay");

	// A generation covering the reservation commits; this store activates it.
	jump_floor(&cluster, now + 60_000).await;
	wait_for(
		"this store activated a generation covering the reservation",
		async || index.base_meta().await.unwrap().active_gid >= 1,
	)
	.await;
	let active_gid = index.base_meta().await.unwrap().active_gid;
	let t = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().generations().fr())
		.await;
	assert!(
		t >= rsv,
		"premise: the generation covers the reservation ({t} >= {rsv})"
	);
	let (intent, owner, debt) = evidence(index.clone()).await;
	eprintln!(
		"DC-276 fail={fail_publication} after activation (store active_gid={active_gid}; leader fr={t}): intent={intent} owner={owner} exact_debt={debt:?}"
	);
	assert!(!intent && !owner, "activation retired the intent and owner");
	assert_eq!(
		debt.len(),
		1,
		"exact RetiredIntent debt with the version's digest"
	);
	assert_eq!(
		debt[0].1.source,
		DebtSource::RetiredIntent { rsv, ordinal: 0 }
	);

	// The ordinary debt sweep: certified cleanup stays held, the sweep runs.
	server
		.decision_peer_for_test()
		.pause_local_cleanup
		.store(true, Release);
	pause.store(false, Release);
	server.debt_sweep_step().await.unwrap();
	let (_, _, debt_after_sweep) = evidence(index.clone()).await;
	let copies_before_reopen = index
		.owned_version_copies(tenant, manifest.clone())
		.await
		.unwrap();
	eprintln!(
		"DC-276 fail={fail_publication} after the sweep: exact_debt={debt_after_sweep:?} prefix_records={copies_before_reopen:?}"
	);
	if !fail_publication {
		// The control: a version with a copy keeps its debt through the sweep.
		assert_eq!(copies_before_reopen, vec![1]);
		assert_eq!(
			debt_after_sweep.len(),
			1,
			"debt survives while a copy exists"
		);
		return;
	}
	assert_eq!(copies_before_reopen, vec![0], "no record before the reopen");
	// DC-276 repair: under an orphaned publication the sweep keeps the exact debt.
	assert_eq!(
		debt_after_sweep.len(),
		1,
		"the exact debt is kept while the publication is uncertain"
	);
	drop(index);
	drop(slab);
	drop(server);

	// The same store reopens (same directory and incarnation), cleanup held.
	cluster
		.configs
		.get_mut(&store)
		.unwrap()
		.hold_cleanup_at_start_for_test = true;
	cluster.stop(store, false).await;
	cluster.reopen(store).await;
	until_settled(&cluster, &descriptor).await;
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	assert_eq!(
		index.store_incarnation(),
		incarnation,
		"premise: the same store"
	);
	let identity = crate::schema::Identity::new(version.key_identity);
	let recovered = {
		let descriptor = descriptor.clone();
		move |slab: Arc<crate::storage::SlabManager>| {
			let descriptor = descriptor.clone();
			async move {
				let locations = slab
					.version_locations(tenant, identity, version.hlc)
					.unwrap();
				let rows = slab
					.stored_rows_at_version(tenant, identity, version.hlc, None)
					.await
					.unwrap();
				locations
					.into_iter()
					.zip(rows)
					.filter(|(_, row)| {
						crate::network::data_server::child_writes::stored_row_digest(
							&descriptor,
							row,
						)
						.is_ok_and(|digest| digest == version.row_digest)
					})
					.map(|(location, _)| location)
					.collect::<Vec<_>>()
			}
		}
	};
	let matching = recovered(slab.clone()).await;
	let (intent, owner, debt) = evidence(index.clone()).await;
	eprintln!(
		"DC-276 after reopen: recovered copy (digest = manifest)={matching:?} exact evidence: intent={intent} owner={owner} debt={debt:?}"
	);
	// The safety invariant: a recoverable physical copy of the refused version
	// (its stored row digest equal to the refused child's manifest digest) never
	// reappears without corresponding exact cleanup evidence.
	assert!(
		matching.is_empty() || intent || owner || !debt.is_empty(),
		"a copy of refused version {version:?} reappeared at {matching:?} with no exact intent, owner or debt"
	);
	// The recovered copy, exactly: one location, one decoded row, and a
	// successfully computed digest equal to the manifest's; one durable record.
	let locations = slab
		.version_locations(tenant, identity, version.hlc)
		.unwrap();
	let rows = slab
		.stored_rows_at_version(tenant, identity, version.hlc, None)
		.await
		.unwrap();
	assert_eq!(
		(locations.len(), rows.len()),
		(1, 1),
		"premise: one recovered copy"
	);
	assert_eq!(
		crate::network::data_server::child_writes::stored_row_digest(&descriptor, &rows[0]),
		Ok(version.row_digest),
		"premise: its digest is the manifest's"
	);
	assert_eq!(
		index
			.owned_version_copies(tenant, vec![version])
			.await
			.unwrap(),
		vec![1],
		"premise: one durable record at the version's prefix"
	);
	assert_eq!(matching.len(), 1, "premise: the recovered copy");
	assert_eq!(debt.len(), 1, "its exact debt survived the reopen");

	// Released: ordinary certified cleanup erases the copy, then the sweep
	// retires the debt.
	let server = cluster.node(store).data_server.clone();
	slab.force_seal_active_for_test().await;
	server
		.decision_peer_for_test()
		.pause_cleanup
		.store(false, Release);
	// Absence is asserted directly, independent of any digest filtering: no
	// location at all for this unique prefix, no durable record, no exact debt.
	wait_for(
		"cleanup erased the copy, then its debt retired",
		async || {
			slab.version_locations(tenant, identity, version.hlc)
				.unwrap()
				.is_empty() && index
				.owned_version_copies(tenant, vec![version])
				.await
				.unwrap() == vec![0]
				&& evidence(index.clone()).await.2.is_empty()
		},
	)
	.await;
	drop(index);
	drop(slab);
	drop(server);

	// A second reopen holds neither; cleanup is held from its start so fresh
	// maintenance cannot mask a reappearance.
	cluster
		.configs
		.get_mut(&store)
		.unwrap()
		.hold_cleanup_at_start_for_test = true;
	cluster.stop(store, false).await;
	cluster.reopen(store).await;
	until_settled(&cluster, &descriptor).await;
	let slab = cluster.node(store).slab.clone();
	let index = slab.persistent_index().unwrap();
	let (_, _, debt) = evidence(index.clone()).await;
	let locations = slab
		.version_locations(tenant, identity, version.hlc)
		.unwrap();
	let durable = index
		.owned_version_copies(tenant, vec![version])
		.await
		.unwrap();
	eprintln!(
		"DC-276 after the second reopen: locations={locations:?} durable={durable:?} exact debt={debt:?}"
	);
	assert!(locations.is_empty(), "no location for the version");
	assert_eq!(durable, vec![0], "no durable record for the version");
	assert!(debt.is_empty(), "no exact debt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_failed_publication_keeps_cleanup_evidence_until_its_copy_is_gone() {
	refused_version_across_activation_sweep_and_reopen(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_indexed_refused_version_keeps_its_debt_through_the_sweep() {
	refused_version_across_activation_sweep_and_reopen(false).await;
}
