//! DC-269 regression: a child persist paused after its last eligibility and
//! authority checks, immediately before it begins its intent, while its
//! still-Reserved parent is refused by a valid abort, its key retired, and a
//! generation covering it certified and activated on the same store. Resumed, its
//! own begin is refused at the installed cut, and it leaves no intent, owner or
//! copy; a newer reservation above the cut is admitted.

use super::*;
use crate::network::data_server::child_writes::persist_pause;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_persist_resumed_after_activation_writes_no_intent_at_or_below_the_cut() {
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let batch = named_rows(&descriptor, &[(70, "late")]);
	let coordinator = cluster
		.node(leader)
		.data_server
		.primary_for_test(TenantId::ZERO, &descriptor, &batch)
		.await
		.unwrap();
	// The store under test: the coordinator, which is also its single child's holder.
	let store = coordinator;
	let probe = persist_pause::arm(&cluster.node(store).slab, descriptor.table_object_id);
	// Keep the parent Reserved: the coordinator never reaches Finalize meanwhile.
	let (_finalize_reached, finalize_release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let server = cluster.node(coordinator).data_server.clone();
	let writing = tokio::spawn(async move { server.table_write_for_test(meta, batch).await });
	tokio::time::timeout(BOUND, probe.reached.notified())
		.await
		.expect("the persist reached the point before its intent");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("the parent is reserved");
	let (rsv, ordinal) = (record.rsv, 0u16);
	let index = cluster.node(store).slab.persistent_index().unwrap();
	assert!(
		index.parent_intent(rsv, ordinal).await.unwrap().is_none(),
		"premise: no intent yet"
	);

	// A valid abort concludes the still-Reserved parent Refused.
	abort(&cluster, &record).await;
	// Its key retires; a generation covering it is certified and activated everywhere.
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let (gid, t) = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let generations = state.decisions.as_ref().unwrap().generations();
			(generations.current_gid(), generations.fr())
		})
		.await;
	let active = index.base_meta().await.unwrap().active_gid;
	eprintln!("DC-269 premises: rsv={rsv} gid={gid} t={t} store={store} active_gid={active}");
	assert!(
		gid >= 1 && t >= rsv,
		"premise: the certificate covers the parent (t={t}, rsv={rsv})"
	);
	assert_eq!(active, gid, "premise: the store activated it");
	assert!(
		index.parent_intent(rsv, ordinal).await.unwrap().is_none(),
		"absent after activation"
	);

	// Resume the paused persist.
	probe.release.notify_one();
	// The paused worker itself finishes its attempted begin, and is refused.
	tokio::time::timeout(BOUND, probe.begun.notified())
		.await
		.expect("the paused worker attempted its begin");
	let begun = probe.outcome().expect("recorded");
	eprintln!("DC-269 worker begin: {:?}", begun.result);
	assert_eq!(
		begun.result,
		Ok(Err(
			crate::storage::index::parent_intents::IntentRefusal::ErasedByInstalledCut
		)),
		"the worker's own begin is refused at the installed cut"
	);
	finalize_release.notify_one();
	let caller = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends")
		.unwrap();
	eprintln!("DC-269 caller result: {caller:?}");
	// No owner and no appended copy of any version it named.
	for version in &begun.manifest {
		assert_eq!(
			index.version_owner(TenantId::ZERO, *version).await.unwrap(),
			None
		);
	}
	assert!(
		index
			.owned_version_copies(TenantId::ZERO, begun.manifest.clone())
			.await
			.unwrap()
			.iter()
			.all(|copies| *copies == 0),
		"no version was appended"
	);
	let late = index.parent_intent(rsv, ordinal).await.unwrap();
	eprintln!("DC-269 intent after resume: {late:?}");
	assert!(
		late.is_none(),
		"no intent may be written for rsv {rsv} at or below the active cut t={t} after activation: {late:?}"
	);
	// A valid newer reservation of the same row, above the cut, is admitted and
	// completed on the same store.
	// Its client id is above the jumped floor, so it is not already expired.
	let newer = write_meta(&descriptor, 1, uuid_v7_at(now + 60_000 + 1));
	let response = cluster
		.node(coordinator)
		.data_server
		.table_write_for_test(newer, named_rows(&descriptor, &[(70, "newer")]))
		.await
		.unwrap();
	assert_eq!(response.0.write.status, WriteStatus::OK, "{response:?}");
	let admitted = record_of(&cluster, key_of(&descriptor, &newer))
		.await
		.expect("reserved");
	assert!(
		admitted.rsv > t,
		"the newer reservation {} is above the cut {t}",
		admitted.rsv
	);
	let intent = index
		.parent_intent(admitted.rsv, 0)
		.await
		.unwrap()
		.expect("its intent");
	assert_eq!(
		intent.state,
		crate::storage::index::parent_intents::ParentIntentState::Complete
	);
}

/// DC-269 r2: normal validation checks the installed cut against the committed
/// certificate. Same gid and chunks with another cut fails closed; an unconfirmed
/// (missing) cut is written from the certificate.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn validation_refuses_another_installed_cut_and_confirms_a_missing_one() {
	use crate::storage::index::certified_base::{CutState, InstalledCut};
	let (cluster, leader, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let meta = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_through(&cluster, leader, meta, named_rows(&descriptor, &[(5, "e")])).await,
		"a write",
	);
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let t = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref().unwrap().generations().fr())
		.await;
	let node = *NODES.iter().find(|id| **id != leader).unwrap();
	let server = &cluster.node(node).data_server;
	let index = cluster.node(node).slab.persistent_index().unwrap();
	assert_eq!(
		index.installed_cut().await.unwrap(),
		CutState::Current(InstalledCut { gid: 1, t })
	);
	let validated = server.base_validated_for_test();
	// Same gid and chunks, another cut: validation fails closed and writes nothing.
	index
		.set_installed_cut_for_test(Some(InstalledCut { gid: 1, t: t + 3 }))
		.await
		.unwrap();
	validated.store(false, std::sync::atomic::Ordering::Release);
	let refused = server.validate_certified_base().await.unwrap_err();
	eprintln!("DC-269 r2 validation with another cut: {refused}");
	assert!(refused.contains("installed cut"), "{refused}");
	assert!(!validated.load(std::sync::atomic::Ordering::Acquire));
	assert_eq!(
		index.installed_cut().await.unwrap(),
		CutState::Current(InstalledCut { gid: 1, t: t + 3 })
	);
	// Missing (an earlier build's store; admission refuses it, see the unit
	// test): validation, or the maintenance step racing it, writes the
	// certificate's cut.
	index.set_installed_cut_for_test(None).await.unwrap();
	server.validate_certified_base().await.unwrap();
	assert_eq!(
		index.installed_cut().await.unwrap(),
		CutState::Current(InstalledCut { gid: 1, t })
	);
	// Same gid, other chunks (and the cut missing): validation, through the
	// committed activation, fails closed; no row changes and nothing is backfilled.
	let chunks = index.base_meta().await.unwrap().active_chunks;
	let rows = index.base_entries(false).await.unwrap();
	let mut other = chunks.clone();
	other[0].count += 1;
	index
		.set_active_chunks_for_test(other.clone())
		.await
		.unwrap();
	index.set_installed_cut_for_test(None).await.unwrap();
	validated.store(false, std::sync::atomic::Ordering::Release);
	let refused = server.validate_certified_base().await.unwrap_err();
	eprintln!("DC-269 r2 validation with other chunks: {refused}");
	assert!(refused.contains("chunks other than"), "{refused}");
	assert!(!validated.load(std::sync::atomic::Ordering::Acquire));
	assert_eq!(
		index.base_entries(false).await.unwrap(),
		rows,
		"rows unchanged"
	);
	assert_eq!(index.base_meta().await.unwrap().active_chunks, other);
	assert_eq!(
		index.installed_cut().await.unwrap(),
		CutState::Unconfirmed,
		"no backfill"
	);
	// Restored, validation confirms and writes the certificate's cut.
	index.set_active_chunks_for_test(chunks).await.unwrap();
	server.validate_certified_base().await.unwrap();
	assert_eq!(
		index.installed_cut().await.unwrap(),
		CutState::Current(InstalledCut { gid: 1, t })
	);
}
