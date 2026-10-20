//! A write key names exactly one add (see `counter/write_key.rs`).
//!
//! A caller whose `(client_uuid, sequence_no)` restarts from a fixed uuid and
//! seq 1 re-issues an old write key for a NEW logical add. The leader must
//! refuse that add with `WriteKeyConflict`, applying nothing. It must never
//! hand back the recorded outcome of a different add: that silently drops the
//! delta and can report another counter's value.
//!
//! The first fixtures restart the leader from its durable dirs between the two
//! adds, because that is how the reuse arises across processes; the recorded
//! add then lives only in the reloaded applied log. The others cover the
//! pending witness, the window after the witness is written, and concurrent
//! requests under one write key.

use super::*;

/// The write key both lives use: a fresh process that restarts its sequence at 1.
const REUSED: u64 = 1;

struct Dirs {
	wdir: PathBuf,
	path: PathBuf,
	/// Keeps both paths alive across the fixture's restarts.
	_dir: crate::test_support::TestDir,
}

fn dirs() -> Dirs {
	let dir = slab_dir();
	Dirs {
		wdir: dir.join("witnesses"),
		path: dir.join("slab_p0.wnd"),
		_dir: dir,
	}
}

/// First life: one add under the reused write key, fully resolved and fsynced,
/// so its dedup record lives only in the durable applied log.
async fn first_life(dirs: &Dirs, key: &CounterKey, delta: u64) {
	let store = open_store(&dirs.path).await;
	let leader = LeaderHarness::new(
		vec![TestParticipant::over(store.clone())],
		1,
		true,
		DurableCounterWitnessStore::open(&dirs.wdir).unwrap(),
	);
	assert_eq!(
		leader.fetch_add(wk(REUSED), key, delta).await.unwrap(),
		CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: delta
		}),
		"first life applies its add"
	);
	assert_eq!(leader.pending_witnesses(), 0, "first life fully resolved");
	store.fsync().await.unwrap();
}

/// Second life: recover from the same durable dirs and reconcile.
async fn second_life(dirs: &Dirs) -> LeaderHarness {
	let leader = LeaderHarness::recover(
		vec![TestParticipant::over(open_store(&dirs.path).await)],
		1,
		true,
		DurableCounterWitnessStore::open(&dirs.wdir).unwrap(),
	)
	.unwrap();
	leader.reconcile().await;
	leader
}

/// A different add under a known write key is refused, never replayed and
/// never applied.
fn assert_write_key_conflict(what: &str, result: Result<CounterAddResult, CounterLeaderError>) {
	match result {
		Err(CounterLeaderError::WriteKeyConflict) => (),
		Ok(CounterAddResult::Replayed(outcome)) => panic!(
			"{what}: a reused write key with a different (key, delta) REPLAYED the recorded \
			 outcome {outcome:?} of another add (the delta was silently dropped)"
		),
		other => panic!("{what}: expected WriteKeyConflict, got {other:?}"),
	}
}

/// The durable witness files, by name, so a refusal can be shown to leave them.
fn witness_files(wdir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
	let mut files: Vec<_> = std::fs::read_dir(wdir)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| {
			path.file_name()
				.is_some_and(|name| name.to_string_lossy().starts_with("counter-witness-"))
		})
		.map(|path| {
			let bytes = std::fs::read(&path).unwrap();
			(path, bytes)
		})
		.collect();
	files.sort();
	files
}

// T-K2a: repro 1a. A reused write key on another counter after a restart.
#[tokio::test]
async fn reused_write_key_on_another_key_after_restart_is_not_replayed() {
	let dirs = dirs();
	let a = ckey(b"verdict-a");
	let b = ckey(b"verdict-b");
	first_life(&dirs, &a, 5).await;

	let leader = second_life(&dirs).await;
	assert_write_key_conflict(
		"add(B, +7) after add(A, +5) under the same write key",
		leader.fetch_add(wk(REUSED), &b, 7).await,
	);
	assert_eq!(leader.read(&b).await.unwrap(), None, "B is untouched");
	assert_eq!(leader.read(&a).await.unwrap(), Some(5), "A is untouched");
}

// T-K2b: repro 1b. A reused write key on the same counter with another delta.
#[tokio::test]
async fn reused_write_key_on_the_same_key_with_another_delta_after_restart_is_not_replayed() {
	let dirs = dirs();
	let a = ckey(b"verdict-a");
	first_life(&dirs, &a, 5).await;

	let leader = second_life(&dirs).await;
	assert_write_key_conflict(
		"add(A, +3) after add(A, +5) under the same write key",
		leader.fetch_add(wk(REUSED), &a, 3).await,
	);
	assert_eq!(
		leader.read(&a).await.unwrap(),
		Some(5),
		"A carries only the first delta"
	);
}

// T-K2c: a known write key with a zero delta conflicts (the recorded delta is
// at least one) instead of answering `ZeroDelta`; an unknown one still does.
#[tokio::test]
async fn known_write_key_with_zero_delta_conflicts() {
	let dir = slab_dir();
	let (ps, _paths) = participants(&dir, 1).await;
	let leader = LeaderHarness::new(ps, 1, true, wstore(&dir));
	let a = ckey(b"zero");
	leader.fetch_add(wk(1), &a, 5).await.unwrap();
	assert_write_key_conflict("zero delta, same key", leader.fetch_add(wk(1), &a, 0).await);
	assert_eq!(
		leader.fetch_add(wk(2), &a, 0).await,
		Err(CounterLeaderError::ZeroDelta),
		"an unknown write key keeps the zero-delta refusal"
	);
	assert_eq!(leader.read(&a).await.unwrap(), Some(5));
}

// T-K2d: while the witness is pending (a participant still owes its marker), a
// conflicting request under its write key is refused, and the witness, the
// participants and the eventual reconcile are unchanged.
#[tokio::test]
async fn conflicting_request_while_the_witness_is_pending_changes_nothing() {
	let dir = slab_dir();
	let wdir = dir.join("witnesses");
	let (ps, paths) = participants(&dir, 3).await;
	let leader = LeaderHarness::new(
		ps,
		2,
		true,
		DurableCounterWitnessStore::open(&wdir).unwrap(),
	);
	let a = ckey(b"pending-a");
	let b = ckey(b"pending-b");
	leader.arm_participant_commit_failure(2);
	leader.fetch_add(wk(1), &a, 5).await.unwrap();
	assert_eq!(leader.pending_witnesses(), 1, "participant 2 owes a marker");
	let before = witness_files(&wdir);
	assert_eq!(before.len(), 1);

	assert_write_key_conflict("pending, other key", leader.fetch_add(wk(1), &b, 5).await);
	assert_write_key_conflict("pending, other delta", leader.fetch_add(wk(1), &a, 4).await);
	assert_eq!(witness_files(&wdir), before, "the witness is unchanged");
	assert_eq!(leader.pending_witnesses(), 1);
	assert_eq!(leader.participant_read(0, &b).await, None);

	leader.reconcile().await;
	assert_eq!(leader.pending_witnesses(), 0);
	for path in &paths {
		assert_eq!(read_after_restart(path, &a).await, Some(5));
		assert_eq!(read_after_restart(path, &b).await, None);
	}
}

// T-K2e: the same, across `recover`: the recovered pending witness keys its
// write key to its own counter and delta before the reconcile finishes it.
#[tokio::test]
async fn conflicting_request_against_a_recovered_pending_witness_changes_nothing() {
	let dir = slab_dir();
	let wdir = dir.join("witnesses");
	let paths: Vec<PathBuf> = (0..3).map(|i| dir.join(format!("slab_p{i}.wnd"))).collect();
	let a = ckey(b"recovered-a");
	let b = ckey(b"recovered-b");
	{
		let mut ps = Vec::new();
		let mut stores = Vec::new();
		for path in &paths {
			let store = open_store(path).await;
			ps.push(TestParticipant::over(store.clone()));
			stores.push(store);
		}
		let leader = LeaderHarness::new(
			ps,
			2,
			true,
			DurableCounterWitnessStore::open(&wdir).unwrap(),
		);
		leader.arm_participant_commit_failure(2);
		leader.fetch_add(wk(1), &a, 5).await.unwrap();
		assert_eq!(leader.pending_witnesses(), 1);
		for store in &stores {
			store.fsync().await.unwrap();
		}
	}
	let mut ps = Vec::new();
	for path in &paths {
		ps.push(TestParticipant::over(open_store(path).await));
	}
	let leader = LeaderHarness::recover(
		ps,
		2,
		true,
		DurableCounterWitnessStore::open(&wdir).unwrap(),
	)
	.unwrap();
	assert_eq!(leader.pending_witnesses(), 1, "the witness was recovered");
	let before = witness_files(&wdir);

	assert_write_key_conflict("recovered, other key", leader.fetch_add(wk(1), &b, 5).await);
	assert_write_key_conflict(
		"recovered, other delta",
		leader.fetch_add(wk(1), &a, 6).await,
	);
	assert_eq!(witness_files(&wdir), before, "the witness is unchanged");
	assert_eq!(
		leader.fetch_add(wk(1), &a, 5).await.unwrap(),
		CounterAddResult::Replayed(CounterAddOutcome {
			previous: 0,
			current: 5
		}),
		"the add the witness names still replays"
	);

	leader.reconcile().await;
	assert_eq!(leader.pending_witnesses(), 0);
	for path in &paths {
		assert_eq!(read_after_restart(path, &a).await, Some(5));
		assert_eq!(read_after_restart(path, &b).await, None);
	}
}

// T-K2f: a keyed applied record after restart refuses every other add under
// its write key and still replays its own.
#[tokio::test]
async fn keyed_applied_record_after_restart_refuses_other_adds() {
	let dirs = dirs();
	let a = ckey(b"keyed-a");
	first_life(&dirs, &a, 5).await;
	let records = DurableCounterWitnessStore::open(&dirs.wdir)
		.unwrap()
		.load_applied()
		.unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(records[0].key.as_ref(), Some(&a), "the record is keyed");

	let leader = second_life(&dirs).await;
	for (what, key, delta) in [
		("other key", ckey(b"keyed-b"), 5),
		("other delta", a.clone(), 6),
		("zero delta", a.clone(), 0),
	] {
		assert_write_key_conflict(what, leader.fetch_add(wk(REUSED), &key, delta).await);
	}
	assert_eq!(
		leader.fetch_add(wk(REUSED), &a, 5).await.unwrap(),
		CounterAddResult::Replayed(CounterAddOutcome {
			previous: 0,
			current: 5
		})
	);
	assert_eq!(leader.read(&a).await.unwrap(), Some(5));
	assert_eq!(leader.read(&ckey(b"keyed-b")).await.unwrap(), None);
}

// T-K2h: every exit after the witness is written leaves its write key
// recorded. Here a participant's commit poisons the key, so the add returns an
// error after its witness is durable; a same-key request on another counter
// must then conflict rather than apply a second operation under the key.
#[tokio::test]
async fn write_key_is_recorded_when_the_add_fails_after_its_witness() {
	let dir = slab_dir();
	let (ps, _paths) = participants(&dir, 1).await;
	let leader = LeaderHarness::new(ps, 1, true, wstore(&dir));
	let a = ckey(b"window-a");
	let b = ckey(b"window-b");
	leader.arm_participant_commit_visibility_failure(0);
	assert_eq!(
		leader.fetch_add(wk(1), &a, 5).await,
		Err(CounterLeaderError::Visibility(
			CounterVisibilityError::Poisoned
		)),
		"the add fails after its witness is durable"
	);
	assert_eq!(leader.pending_witnesses(), 1, "the witness is kept");
	assert_write_key_conflict(
		"same write key, another counter",
		leader.fetch_add(wk(1), &b, 7).await,
	);
	assert_eq!(leader.read(&b).await.unwrap(), None, "B is untouched");
	assert_eq!(
		leader
			.participant_tentatives(0)
			.iter()
			.filter(|(_, key)| *key == b)
			.count(),
		0,
		"the refused add never reached a participant"
	);
}

/// The concurrent-add fixture: the first add under `wk(1)` parks inside its
/// first `replicate_tentative`, and a second request under the same write key
/// races it. Returns both results and the tentatives the participant saw.
async fn race_one_write_key(
	second_key: &CounterKey,
	second_delta: u64,
	bypass_gate: bool,
) -> (
	crate::test_support::TestDir,
	Result<CounterAddResult, CounterLeaderError>,
	Result<CounterAddResult, CounterLeaderError>,
	Vec<(u64, CounterKey)>,
	Arc<LeaderHarness>,
) {
	let dir = slab_dir();
	let (ps, _paths) = participants(&dir, 1).await;
	let leader = Arc::new(LeaderHarness::new(ps, 1, true, wstore(&dir)));
	leader
		.leader
		.write_key_switches
		.bypass_gate
		.store(bypass_gate, Ordering::Relaxed);
	let a = ckey(b"race-a");
	let park = leader.park_participant_tentative(0);
	let first = tokio::spawn({
		let leader = leader.clone();
		let a = a.clone();
		async move { leader.fetch_add(wk(1), &a, 5).await }
	});
	park.reached.notified().await;
	let second = tokio::spawn({
		let leader = leader.clone();
		let key = second_key.clone();
		async move { leader.fetch_add(wk(1), &key, second_delta).await }
	});
	// While the first add is parked, the second is either held at admission (the
	// gate) or, without it, free to run. Without the gate, wait until it has
	// reached the participant; with it, give it every chance to run first.
	if bypass_gate {
		let reached = |leader: &LeaderHarness| {
			leader
				.participant_tentatives(0)
				.iter()
				.any(|(_, key)| key == second_key)
		};
		for _ in 0..10_000 {
			if reached(&leader) {
				break;
			}
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
		assert!(
			reached(&leader),
			"without the gate the second add reaches the participant"
		);
	} else {
		for _ in 0..64 {
			tokio::task::yield_now().await;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;

		assert!(
			!second.is_finished(),
			"the second request must wait while the first add holds its write key"
		);
		assert!(
			leader
				.participant_tentatives(0)
				.iter()
				.all(|(op_id, key)| *op_id == 1 && *key == a),
			"only the first add reached a participant while it was in flight"
		);
	}
	park.release.notify_one();
	let first = first.await.unwrap();
	let second = second.await.unwrap();
	let tentatives = leader.participant_tentatives(0);
	(dir, first, second, tentatives, leader)
}

// T-K3: one add per write key in flight. The second request under the parked
// first add's write key never reaches a participant: on another counter it
// conflicts, on the same counter with the same delta it replays, and with
// another delta it conflicts. Reconcile leaves nothing pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_under_one_write_key_admit_one_add() {
	let a = ckey(b"race-a");
	let b = ckey(b"race-b");
	let applied = CounterAddOutcome {
		previous: 0,
		current: 5,
	};
	for (what, key, delta, expected) in [
		(
			"other key",
			b.clone(),
			7,
			Err(CounterLeaderError::WriteKeyConflict),
		),
		(
			"same add",
			a.clone(),
			5,
			Ok(CounterAddResult::Replayed(applied)),
		),
		(
			"other delta",
			a.clone(),
			3,
			Err(CounterLeaderError::WriteKeyConflict),
		),
	] {
		let (_dir, first, second, tentatives, leader) =
			race_one_write_key(&key, delta, false).await;
		assert_eq!(first, Ok(CounterAddResult::Applied(applied)), "{what}");
		assert_eq!(second, expected, "{what}");
		assert!(
			tentatives
				.iter()
				.all(|(op_id, key)| *op_id == 1 && *key == a),
			"{what}: the second request never reached a participant: {tentatives:?}"
		);
		leader.reconcile().await;
		assert_eq!(leader.pending_witnesses(), 0, "{what}");
		assert_eq!(leader.read(&a).await.unwrap(), Some(5), "{what}");
		assert_eq!(leader.read(&b).await.unwrap(), None, "{what}");
	}
}

// T-K3 negative control: without the write-key gate, the second request
// misses the record while the first add is in flight, and both adds apply
// under one write key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_write_key_gate_one_write_key_applies_two_adds() {
	let b = ckey(b"race-b");
	let (_dir, first, second, tentatives, leader) = race_one_write_key(&b, 7, true).await;
	assert_eq!(
		first,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 5
		}))
	);
	assert_eq!(
		second,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 7
		})),
		"the bypassed gate lets a second add apply under the write key"
	);
	assert!(tentatives.iter().any(|(_, key)| *key == b));
	assert_eq!(leader.read(&b).await.unwrap(), Some(7));
}

// T-K3-gc: a released gate's entry is reclaimed, whatever the request's fate
// (applied, replayed, refused); with reclamation switched off, every write key
// leaves its entry behind.
#[tokio::test]
async fn write_key_gates_are_reclaimed_after_every_request() {
	const ADDS: u64 = 8;
	for keep in [false, true] {
		let dir = slab_dir();
		let (ps, _paths) = participants(&dir, 1).await;
		let leader = LeaderHarness::new(ps, 1, true, wstore(&dir));
		leader
			.leader
			.write_key_switches
			.keep_gate_entries
			.store(keep, Ordering::Relaxed);
		let key = ckey(b"gates");
		for n in 1..=ADDS {
			leader.fetch_add(wk(n), &key, 1).await.unwrap();
		}
		leader.fetch_add(wk(1), &key, 1).await.unwrap();
		assert_eq!(
			leader.fetch_add(wk(1), &key, 2).await,
			Err(CounterLeaderError::WriteKeyConflict)
		);
		assert_eq!(
			leader.fetch_add(wk(ADDS + 1), &key, 0).await,
			Err(CounterLeaderError::ZeroDelta)
		);
		let left = leader.leader.write_gates.len();
		if keep {
			assert_eq!(
				left as u64,
				ADDS + 1,
				"without reclamation each write key keeps its entry"
			);
		} else {
			assert_eq!(left, 0, "every released gate was reclaimed");
		}
	}
}
