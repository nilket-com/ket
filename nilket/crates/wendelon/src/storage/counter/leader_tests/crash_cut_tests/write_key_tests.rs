//! Write-key laws that need the persistence model: unkeyed records, a failed
//! prepare, recovery of two operations under one write key, cancellation of an
//! add after its prepare, and the in-doubt prepare that became durable.
use super::*;

use tokio::sync::Notify;

fn conflict() -> Result<CounterAddResult, CounterLeaderError> {
	Err(CounterLeaderError::WriteKeyConflict)
}

// T-K2g: an unkeyed applied record cannot show which add it records. Every
// request under its write key is refused: the same counter with the same delta
// (a genuine retry, the documented compatibility limit) and another counter
// with the same delta (which a match on the delta alone would wrongly replay).
#[tokio::test]
async fn unkeyed_record_refuses_every_request_under_its_write_key() {
	let rig = Rig::new(PersistenceModel::new(), 1);
	model_store(&rig.model);
	let bytes = applied_log::encode_unkeyed_frame(&AppliedCounterAdd {
		op_id: 1,
		write_key: wk(81),
		key: None,
		previous: 0,
		current: 5,
	})
	.unwrap();
	rig.model.put_durable(&applied_log_path(), bytes.clone());
	let recovered = rig.restart().await;
	let a = ckey(b"unkeyed-a");
	let b = ckey(b"unkeyed-b");
	assert_eq!(
		recovered.fetch_add(wk(81), &a, 5).await,
		conflict(),
		"same add"
	);
	assert_eq!(
		recovered.fetch_add(wk(81), &b, 5).await,
		conflict(),
		"same delta"
	);
	assert_eq!(recovered.read(&a).await.unwrap(), None);
	assert_eq!(recovered.read(&b).await.unwrap(), None);
	drop(recovered);
	assert_eq!(rig.model.bytes(&applied_log_path()), Some(bytes));
}

// T-K3-rb: a failed prepare rolls back before anything is installed, so its
// write key stays unrecorded and a retry of the same add applies freshly (not
// as a replay of an add that never happened). The gate is reclaimed.
#[tokio::test]
async fn failed_prepare_records_nothing_and_a_retry_applies_freshly() {
	for fault in [
		Fault::TempWriteAfterBytes,
		Fault::TempSync,
		Fault::WitnessRenameAfterMove,
		Fault::PrepareDirectorySync,
	] {
		let rig = Rig::new(PersistenceModel::new(), 3);
		let key = ckey(b"rolled-back");
		let write_key = wk(300 + fault as u64);
		let leader = rig.leader().await;
		rig.model.arm(fault);
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await,
			Err(CounterLeaderError::Internal),
			"{fault:?}"
		);
		assert!(
			leader.leader.idempotency.get(&write_key).is_none(),
			"{fault:?}: nothing recorded"
		);
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await,
			Ok(CounterAddResult::Applied(outcome(0, 5))),
			"{fault:?}: the retry applies freshly"
		);
		leader.reconcile().await;
		assert_eq!(leader.pending_witnesses(), 0, "{fault:?}");
		assert_eq!(leader.read(&key).await.unwrap(), Some(5), "{fault:?}");
		assert_eq!(leader.leader.write_gates.len(), 0, "{fault:?}");
	}
}

/// Recovery must refuse the image with a typed identity conflict that names
/// the write key and both operations, and leave every file as it was.
async fn assert_recovery_refuses_two_operations(rig: &Rig, files: &[PathBuf], context: &str) {
	let before: Vec<_> = files.iter().map(|path| rig.model.bytes(path)).collect();
	assert!(
		before.iter().all(Option::is_some),
		"{context}: fixture files"
	);
	let participants = rig.participants(false).await;
	let Err(error) = LeaderHarness::recover(participants, 1, true, model_store(&rig.model)) else {
		panic!("{context}: recovery must refuse one write key naming two operations");
	};
	assert_eq!(
		fault_of(&error),
		Some(AppliedLogFault::IdentityConflict {
			write_key: wk(91),
			recorded_op_id: 1,
			conflicting_op_id: 2,
		}),
		"{context}"
	);
	let after: Vec<_> = files.iter().map(|path| rig.model.bytes(path)).collect();
	assert_eq!(after, before, "{context}: files kept");
}

// T-REC: two operation ids under one write key, in each mix of applied records
// and witnesses, fail recovery closed and keep the files for repair.
#[tokio::test]
async fn two_operations_under_one_write_key_fail_recovery_closed() {
	let keyed = |op_id, key: &[u8]| {
		applied_log::encode_frame(&AppliedCounterAdd {
			op_id,
			write_key: wk(91),
			key: Some(ckey(key)),
			previous: 0,
			current: 5,
		})
		.unwrap()
	};
	let unkeyed = applied_log::encode_unkeyed_frame(&AppliedCounterAdd {
		op_id: 1,
		write_key: wk(91),
		key: None,
		previous: 0,
		current: 5,
	})
	.unwrap();
	let log = applied_log_path();
	for (context, frames) in [
		(
			"two keyed records",
			[keyed(1, b"a"), keyed(2, b"b")].concat(),
		),
		(
			"unkeyed and keyed records",
			[unkeyed.clone(), keyed(2, b"b")].concat(),
		),
	] {
		let rig = Rig::new(PersistenceModel::new(), 1);
		model_store(&rig.model);
		rig.model.put_durable(&log, frames);
		assert_recovery_refuses_two_operations(&rig, std::slice::from_ref(&log), context).await;
	}

	let rig = Rig::new(PersistenceModel::new(), 1);
	let store = model_store(&rig.model);
	store.write(&fixture_witness(1, wk(91), b"a")).unwrap();
	store.write(&fixture_witness(2, wk(91), b"b")).unwrap();
	let files = [store.origin_path_for(1), store.origin_path_for(2)];
	drop(store);
	assert_recovery_refuses_two_operations(&rig, &files, "two witnesses").await;

	let rig = Rig::new(PersistenceModel::new(), 1);
	let store = model_store(&rig.model);
	rig.model.put_durable(&log, keyed(1, b"a"));
	store.write(&fixture_witness(2, wk(91), b"b")).unwrap();
	let files = [log.clone(), store.origin_path_for(2)];
	drop(store);
	assert_recovery_refuses_two_operations(&rig, &files, "a record and a witness").await;
}

/// Wait until every add the leader started has finished: each holds its write
/// key's gate on its own task until it ends.
async fn wait_for_adds(leader: &LeaderHarness) {
	for _ in 0..10_000 {
		if leader.leader.write_gates.is_empty() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(1)).await;
	}
	panic!("the leader's adds did not finish");
}

const DROPPED: u64 = 800;
const NEXT: u64 = 801;

/// Where T-K5 parks the add `(DROPPED, "parked", +5)` before dropping its caller.
#[derive(Clone, Copy, Debug)]
enum ParkPoint {
	/// Inside the witness write's blocking call (its temp file write).
	WitnessWrite,
	/// Inside `drive_commits`' first progress rewrite.
	ProgressRewrite,
	/// Inside `drive_commits`' applied-log append.
	AppliedAppend,
	/// Inside `drive_commits`' witness removal.
	WitnessRemoval,
	/// Inside the first participant commit.
	ParticipantCommit,
}

/// A parked operation, released through whichever park holds it.
enum Parked {
	File(Arc<BlockingPark>),
	Participant(Arc<CallPark>),
}

impl Parked {
	async fn reached(&self) {
		match self {
			Self::File(park) => park.reached.notified().await,
			Self::Participant(park) => park.reached.notified().await,
		}
	}

	fn release(&self) {
		match self {
			Self::File(park) => park.release(),
			Self::Participant(park) => park.release.notify_one(),
		}
	}
}

fn park(rig: &Rig, leader: &LeaderHarness, point: ParkPoint) -> Parked {
	let file = |prefix, nth| Parked::File(rig.model.park_at(prefix, nth));
	match point {
		ParkPoint::WitnessWrite => file("write_file:", 1),
		ParkPoint::ProgressRewrite => file("write_file:", 2),
		ParkPoint::AppliedAppend => file("append_file:", 1),
		ParkPoint::WitnessRemoval => file("unlink:", 1),
		ParkPoint::ParticipantCommit => Parked::Participant(leader.park_participant_commit(0)),
	}
}

/// Start the parked add, drop its caller once it is parked, then release it.
async fn drop_parked_add(rig: &Rig, leader: &LeaderHarness, point: ParkPoint) {
	let parked = park(rig, leader, point);
	let key = ckey(b"parked");
	{
		let add = leader.fetch_add(wk(DROPPED), &key, 5);
		tokio::pin!(add);
		tokio::select! {
			biased;
			() = parked.reached() => (),
			result = &mut add => panic!("{point:?}: the add finished before it parked: {result:?}"),
		}
	}
	parked.release();
}

// T-K5: the add runs on its own task. Parked inside the witness write, inside
// each of `drive_commits`' store calls, and inside a participant commit, its
// caller is dropped and the park released. The add still completes: a retry of
// it replays, the same write key on another counter conflicts, the next add
// builds on it, no gate is left behind, and after a restart and reconcile the
// counter holds both adds once and recovery does not fail closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_caller_cancels_no_step_of_its_add() {
	let key = ckey(b"parked");
	for point in [
		ParkPoint::WitnessWrite,
		ParkPoint::ProgressRewrite,
		ParkPoint::AppliedAppend,
		ParkPoint::WitnessRemoval,
		ParkPoint::ParticipantCommit,
	] {
		let rig = Rig::new(PersistenceModel::new(), 3);
		{
			let leader = rig.leader().await;
			drop_parked_add(&rig, &leader, point).await;
			assert_eq!(
				leader.fetch_add(wk(DROPPED), &key, 5).await,
				Ok(CounterAddResult::Replayed(outcome(0, 5))),
				"{point:?}: the retry replays"
			);
			assert_eq!(
				leader.fetch_add(wk(DROPPED), &ckey(b"other"), 5).await,
				conflict(),
				"{point:?}: another counter under the write key conflicts"
			);
			assert_eq!(
				leader.fetch_add(wk(NEXT), &key, 1).await,
				Ok(CounterAddResult::Applied(outcome(5, 6))),
				"{point:?}: the next add's base includes the dropped add"
			);
			assert_eq!(leader.leader.write_gates.len(), 0, "{point:?}: gates");
		}
		let recovered = rig.restart().await;
		recovered.reconcile().await;
		assert_eq!(recovered.pending_witnesses(), 0, "{point:?}");
		assert_eq!(recovered.read(&key).await.unwrap(), Some(6), "{point:?}");
		assert_eq!(recovered.read(&ckey(b"other")).await.unwrap(), None);
		drop(recovered);
		for value in rig.participant_values(&key).await {
			assert_eq!(value, Some(6), "{point:?}: participant value");
		}
		assert_eq!(raw_frames_for(&rig.model, wk(DROPPED)), 1, "{point:?}");
		assert_eq!(raw_frames_for(&rig.model, wk(NEXT)), 1, "{point:?}");
	}
}

// T-K5 negative control: with the body run inline in the caller's future,
// dropping the caller inside the witness write abandons the install and
// releases both guards while the write still completes. The durable witness
// is left without its record: a retry of the same add applies a second
// operation under the write key, and recovery then fails closed. (This
// harness's participants read raw tentatives, so the next add's base still
// sees the dropped value; with marker-eligible reads the same drop instead
// loses the delta, since the abandoned add is never published.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_body_separates_a_durable_witness_from_its_record() {
	let key = ckey(b"parked");
	let rig = Rig::new(PersistenceModel::new(), 3);
	{
		let leader = rig.leader().await;
		leader
			.leader
			.write_key_switches
			.inline_body
			.store(true, Ordering::Relaxed);
		drop_parked_add(&rig, &leader, ParkPoint::WitnessWrite).await;
		let retry = leader.fetch_add(wk(DROPPED), &key, 5).await;
		assert!(
			matches!(retry, Ok(CounterAddResult::Applied(_))),
			"the retry found no record and applied again: {retry:?}"
		);
	}
	rig.model.crash();
	let participants = rig.participants(true).await;
	let Err(error) = LeaderHarness::recover(participants, 3 / 2 + 1, true, model_store(&rig.model))
	else {
		panic!("two operations under one write key must fail recovery closed");
	};
	assert_eq!(
		fault_of(&error),
		Some(AppliedLogFault::IdentityConflict {
			write_key: wk(DROPPED),
			recorded_op_id: 2,
			conflicting_op_id: 1,
		})
	);
}

/// Start the add `(DROPPED, "dropped", +5)` and drop its caller at the first
/// await after the `n`-th persistence event or participant marker. Returns
/// whether the caller was dropped before the add completed.
async fn drop_add_after_event(rig: &Rig, leader: &LeaderHarness, n: usize) -> bool {
	let signal = Arc::new(Notify::new());
	rig.model.signal_after(n, signal.clone());
	let key = ckey(b"dropped");
	let add = leader.fetch_add(wk(DROPPED), &key, 5);
	tokio::pin!(add);
	tokio::select! {
		biased;
		() = signal.notified() => true,
		result = &mut add => {
			assert_eq!(result, Ok(CounterAddResult::Applied(outcome(0, 5))), "event {n}");
			false
		}
	}
}

// K5 sweep: the caller is dropped after every persistence event and
// participant marker of the add's run. Then, live or after a restart, the
// retry replays the add, the next same-key add builds on it, and after restart
// and reconcile the counter holds both adds exactly once.
#[tokio::test]
async fn a_dropped_add_never_separates_its_witness_from_its_install() {
	let key = ckey(b"dropped");
	let total = {
		let probe = Rig::new(PersistenceModel::new(), 3);
		let leader = probe.leader().await;
		probe.model.reset_events();
		leader.fetch_add(wk(DROPPED), &key, 5).await.unwrap();
		probe.model.events()
	};
	assert!(
		total > 8,
		"the add has a prepare and a resolution ({total} events)"
	);
	for live in [true, false] {
		for n in 1..=total {
			let context = format!("dropped after event {n}/{total}, live retry {live}");
			let rig = Rig::new(PersistenceModel::new(), 3);
			{
				let leader = rig.leader().await;
				assert!(
					drop_add_after_event(&rig, &leader, n).await,
					"{context}: the caller was dropped before the add completed"
				);
				wait_for_adds(&leader).await;
				if live {
					assert_eq!(
						leader.fetch_add(wk(DROPPED), &key, 5).await,
						Ok(CounterAddResult::Replayed(outcome(0, 5))),
						"{context}: the dropped add is recorded"
					);
					assert_eq!(
						leader.fetch_add(wk(NEXT), &key, 1).await,
						Ok(CounterAddResult::Applied(outcome(5, 6))),
						"{context}: the next add builds on the dropped one"
					);
					leader.reconcile().await;
				}
			}
			let recovered = rig.restart().await;
			if !live {
				assert_eq!(
					recovered.fetch_add(wk(DROPPED), &key, 5).await,
					Ok(CounterAddResult::Replayed(outcome(0, 5))),
					"{context}: the dropped add's witness is durable with its record"
				);
				recovered.reconcile().await;
				assert_eq!(
					recovered.fetch_add(wk(NEXT), &key, 1).await,
					Ok(CounterAddResult::Applied(outcome(5, 6))),
					"{context}: the next add builds on the dropped one"
				);
			}
			recovered.reconcile().await;
			assert_eq!(recovered.pending_witnesses(), 0, "{context}: resolved");
			assert_eq!(recovered.read(&key).await.unwrap(), Some(6), "{context}");
			assert_eq!(
				recovered.fetch_add(wk(DROPPED), &key, 5).await,
				Ok(CounterAddResult::Replayed(outcome(0, 5))),
				"{context}"
			);
			assert_eq!(
				recovered.fetch_add(wk(NEXT), &key, 1).await,
				Ok(CounterAddResult::Replayed(outcome(5, 6))),
				"{context}"
			);
			drop(recovered);
			for value in rig.participant_values(&key).await {
				assert_eq!(value, Some(6), "{context}: participant value");
			}
			assert_eq!(raw_frames_for(&rig.model, wk(DROPPED)), 1, "{context}");
			assert_eq!(raw_frames_for(&rig.model, wk(NEXT)), 1, "{context}");
		}
	}
}

// F-CTR-8 H1, the in-doubt branch: a failed prepare whose withdrawal also fails
// answers `Internal` and latches the store, yet the witness can still become
// durable. Recovery then finds it and completes the add: a different add under
// its write key is refused, the retry replays it, and reconcile commits it on
// every participant exactly once.
#[tokio::test]
async fn in_doubt_prepare_that_became_durable_is_completed_by_recovery() {
	let rig = Rig::new(PersistenceModel::new(), 3);
	let key = ckey(b"in-doubt");
	let write_key = wk(960);
	{
		let leader = rig.leader().await;
		rig.model.arm(Fault::PrepareDirectorySync);
		rig.model.arm(Fault::WitnessUnlink);
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await,
			Err(CounterLeaderError::Internal)
		);
		assert!(leader.leader.durability_unsettled());
		assert_eq!(leader.read(&key).await.unwrap(), None, "nothing visible");
	}
	// The kernel may write the directory back at any time.
	rig.model.writeback_now();
	assert!(
		rig.model.has_durable_witness_name(),
		"the in-doubt witness is durable"
	);
	let recovered = rig.restart().await;
	assert_eq!(
		recovered.pending_witnesses(),
		1,
		"recovery found the witness"
	);
	assert_eq!(
		recovered.fetch_add(write_key, &ckey(b"elsewhere"), 5).await,
		conflict()
	);
	assert_eq!(
		recovered.fetch_add(write_key, &key, 5).await,
		Ok(CounterAddResult::Replayed(outcome(0, 5)))
	);
	recovered.reconcile().await;
	assert_eq!(recovered.pending_witnesses(), 0);
	assert_eq!(recovered.read(&key).await.unwrap(), Some(5));
	drop(recovered);
	for value in rig.participant_values(&key).await {
		assert_eq!(value, Some(5), "participant value");
	}
	assert_eq!(raw_frames_for(&rig.model, write_key), 1);
}
