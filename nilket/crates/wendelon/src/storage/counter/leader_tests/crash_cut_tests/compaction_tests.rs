//! Crash-safe compaction (E5) through the real leader and recovery, on the
//! persistence model: every crash cut of a compaction, nested cuts inside
//! recovery, every compaction fault, `CURRENT` and its checksum, publication
//! failure, stale files, a missing `CURRENT`, the downgrade poison, a foreign
//! writer, and bounded state.
//!
//! The model keeps every file in one directory, and a successful directory
//! sync persists every entry in it. So it cannot observe on their own the
//! directory sync that makes a new generation's name durable before `CURRENT`
//! is written, the directory sync after the `CURRENT` rename, or the one after
//! retiring older generations: the next successful directory sync (the
//! witness unlink's, say) persists the same names anyway. They stay (contract
//! Addendum A3): on a real filesystem each name is durable only by its own
//! barrier, and a failed directory sync can lose its dirty entries, which
//! T-PUB-FAIL models.
use super::retention_tests::{T0, compact, expiry, rig_on, served, store_switches};
use super::*;

/// One add on its own counter, issued (and presented) at `issued`.
#[derive(Clone, Copy, Debug)]
struct Add {
	write_key: CounterWriteKey,
	counter: &'static [u8],
	delta: u64,
	issued: u64,
}

impl Add {
	fn new(issued: u64, n: u64, counter: &'static [u8], delta: u64) -> Self {
		Self {
			write_key: wk_at(issued, n),
			counter,
			delta,
			issued,
		}
	}

	fn key(&self) -> CounterKey {
		ckey(self.counter)
	}
}

/// A store with resolved adds (some this compaction deletes), one pending
/// witness, and a leader about to compact at `compact_at`.
struct Scene {
	clock: Arc<TestClock>,
	rig: Rig,
	leader: LeaderHarness,
	/// Every resolved add and whether this compaction deletes it.
	adds: Vec<(Add, bool)>,
	pending: Add,
	/// Appended while the compaction is between its phases.
	tail: Add,
	/// `(generation, cutoff)` served before, and expected after.
	before: (u64, u64),
	after: (u64, u64),
	compact_at: u64,
}

/// The scene for a first compaction (generation 0 to 1) or a second (1 to 2).
async fn scene(second: bool) -> Scene {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	let leader = rig.leader().await;
	let old = Add::new(T0, 1, b"old", 3);
	leader
		.fetch_add(old.write_key, &old.key(), old.delta)
		.await
		.unwrap();
	clock.set(T0 + expiry());
	let young = Add::new(T0 + expiry(), 2, b"young", 4);
	leader
		.fetch_add(young.write_key, &young.key(), young.delta)
		.await
		.unwrap();
	let pending = Add::new(T0 + expiry(), 3, b"pending", 5);
	leader.arm_participant_commit_failure(0);
	leader
		.fetch_add(pending.write_key, &pending.key(), pending.delta)
		.await
		.unwrap();
	assert_eq!(leader.pending_witnesses(), 1);
	let mut scene = Scene {
		clock: clock.clone(),
		rig,
		leader,
		adds: vec![(old, true), (young, false)],
		pending,
		tail: Add::new(T0 + expiry() + 1_000, 6, b"tail", 6),
		before: (0, 0),
		after: (1, T0 + 1_000),
		compact_at: T0 + expiry() + 1_000,
	};
	if second {
		clock.set(scene.compact_at);
		compact(&scene.leader).await;
		scene.adds = vec![(old, true), (young, true)];
		let later = Add::new(T0 + 2 * expiry(), 4, b"later", 7);
		clock.set(later.issued);
		scene
			.leader
			.fetch_add(later.write_key, &later.key(), later.delta)
			.await
			.unwrap();
		scene.adds.push((later, false));
		scene.before = (1, T0 + 1_000);
		scene.compact_at = T0 + 2 * expiry() + 1_000;
		scene.after = (2, T0 + expiry() + 1_000);
		scene.tail = Add::new(scene.compact_at, 6, b"tail", 6);
	}
	clock.set(scene.compact_at);
	scene
}

/// Compact the scene, appending its tail add while the compaction is parked
/// between its phases (at its first sync of the new generation). Returns the
/// compaction's result and whether the tail add ran.
async fn run(scene: &Scene, cut: Option<Cut>, with_tail: bool) -> (std::io::Result<()>, bool) {
	let generation = Path::new(WITNESS_DIR).join(format!("counter-applied.{}", scene.after.0));
	let label: &'static str =
		Box::leak(format!("sync_data:{}", generation.display()).into_boxed_str());
	let park = with_tail.then(|| scene.rig.model.park_at(label, 1));
	if let Some(cut) = cut {
		scene.rig.model.arm_cut(cut);
	}
	let mut task = {
		let leader = scene.leader.leader.clone();
		tokio::spawn(async move { leader.compact().await.map(|_| ()) })
	};
	let Some(park) = park else {
		return (task.await.unwrap(), false);
	};
	let _release = ReleaseOnDrop(park.clone());
	tokio::select! {
		() = park.reached.notified() => {
			let tail = scene.tail;
			let _ = scene.leader.fetch_add(tail.write_key, &tail.key(), tail.delta).await;
			park.release();
			(task.await.unwrap(), true)
		}
		result = &mut task => {
			*scene.rig.model.park.lock().unwrap() = None;
			(result.unwrap(), false)
		}
	}
}

/// The highest operation id the scene's leader sent to its participant: before
/// a compaction, every one a durable record or witness carries.
fn max_op(scene: &Scene) -> u64 {
	scene
		.leader
		.participant_tentatives(0)
		.iter()
		.map(|(op, _)| *op)
		.max()
		.unwrap_or(0)
}

/// After a crash from any cut: recovery serves one whole generation (the old
/// one or the new one, each with its own cutoff), every live record and the
/// pending witness replay, the tail add counts once, no deleted identity is
/// ever applied again, and operation ids stay above every earlier one.
async fn verify(scene: Scene, max_op_before: u64, context: &str) {
	let Scene {
		clock,
		rig,
		leader,
		adds,
		pending,
		tail,
		before,
		after,
		compact_at,
	} = scene;
	drop(leader);
	let recovered = rig.restart().await;
	let (generation, cutoff, high_water, _) = served(&recovered);
	assert!(
		(generation, cutoff) == before || (generation, cutoff) == after,
		"{context}: served ({generation}, {cutoff}), expected {before:?} or {after:?}"
	);
	if generation == after.0 {
		assert!(high_water >= max_op_before, "{context}: header high-water");
		assert_eq!(
			recovered.leader.witness_store.0.cutoff(),
			after.1,
			"{context}: Q_mem"
		);
	}
	for (add, deleted) in &adds {
		if !deleted {
			assert_eq!(
				recovered
					.fetch_add(add.write_key, &add.key(), add.delta)
					.await,
				Ok(CounterAddResult::Replayed(outcome(0, add.delta))),
				"{context}: live record {}",
				String::from_utf8_lossy(add.counter)
			);
		}
	}
	// A retry of the pending add replays, unless the add is older than E by now
	// (T-PEND-E); either way reconcile completes it once.
	let expected = if pending.issued + expiry() <= compact_at {
		Err(CounterLeaderError::OperationExpired)
	} else {
		Ok(CounterAddResult::Replayed(outcome(0, pending.delta)))
	};
	assert_eq!(
		recovered
			.fetch_add(pending.write_key, &pending.key(), pending.delta)
			.await,
		expected,
		"{context}: pending witness"
	);
	let tail_retry = recovered
		.fetch_add(tail.write_key, &tail.key(), tail.delta)
		.await
		.unwrap();
	assert_eq!(
		tail_retry.outcome(),
		outcome(0, tail.delta),
		"{context}: tail"
	);
	recovered.reconcile().await;
	assert_eq!(recovered.pending_witnesses(), 0, "{context}: resolved");
	assert_eq!(
		recovered.read(&tail.key()).await.unwrap(),
		Some(tail.delta),
		"{context}: the tail add counts once"
	);
	assert_eq!(
		recovered.read(&pending.key()).await.unwrap(),
		Some(pending.delta)
	);
	let fresh = Add::new(compact_at, 99, b"fresh", 1);
	recovered
		.fetch_add(fresh.write_key, &fresh.key(), 1)
		.await
		.unwrap();
	let next_op = recovered
		.participant_tentatives(0)
		.iter()
		.map(|(op, _)| *op)
		.max()
		.unwrap();
	assert!(
		next_op > max_op_before,
		"{context}: op id {next_op} reuses one at or below {max_op_before}"
	);
	// A deleted identity is never applied again, even once the clock steps
	// back so that only the cutoff covers it.
	for (add, deleted) in &adds {
		if *deleted {
			clock.set(add.issued + 1);
			let retry = recovered
				.fetch_add(add.write_key, &add.key(), add.delta)
				.await;
			assert!(
				matches!(
					retry,
					Err(CounterLeaderError::OperationExpired) | Ok(CounterAddResult::Replayed(_))
				),
				"{context}: deleted {} answered {retry:?}",
				String::from_utf8_lossy(add.counter)
			);
		}
	}
	clock.set(compact_at);
	drop(recovered);
	let again = rig.restart().await;
	let (generation_again, ..) = served(&again);
	assert_eq!(generation_again, generation, "{context}: a second restart");
}

// T-E5: a crash after every event of a compaction (snapshot, generation write
// and sync, the tail copy and its sync, the directory sync, `CURRENT`'s temp
// write, sync, rename and directory sync, then the retirement of older
// generations and the poison), for a first and a second compaction. Each image
// recovers one whole generation with its cutoff and high-water, losing no live
// record or witness. A crash in the first compaction before the rename serves
// generation 0.
#[tokio::test]
async fn every_crash_cut_of_a_compaction_recovers_one_whole_generation() {
	for second in [false, true] {
		let total = {
			let probe = scene(second).await;
			probe.rig.model.reset_events();
			let (result, tailed) = run(&probe, None, true).await;
			result.unwrap();
			assert!(tailed, "the probe appended a tail");
			probe.rig.model.events()
		};
		assert!(
			total >= 10,
			"the sweep covers the compaction ({total} events)"
		);
		let mut served_old = false;
		for cut in 1..=total {
			let context = format!("second {second}, cut after event {cut}/{total}");
			let scene = scene(second).await;
			let max_op_before = max_op(&scene);
			let _ = run(&scene, Some(Cut::Event(cut)), true).await;
			// Inspect what the crash left before `verify` restarts from it.
			scene.rig.model.crash();
			let current = scene
				.rig
				.model
				.bytes(&Path::new(WITNESS_DIR).join("CURRENT"));
			served_old |= current.is_none() && !second;
			verify(scene, max_op_before, &context).await;
		}
		if !second {
			assert!(served_old, "some first-compaction cut served generation 0");
		}
	}
}

/// What a restarted node does from a crashed image: recover, then reconcile.
async fn recovery_work(rig: &Rig) {
	if let Ok(recovered) = rig.recover_in_place().await {
		recovered.reconcile().await;
	}
}

// T-E5 nested: from every cut of a compaction, followed by a power loss or by
// a process restart without power loss (which keeps every volatile name), a
// crash after every event of recovery itself (its barrier, stale-file cleanup,
// the re-poison, the torn-tail repair, the log sync) and of the reconcile that
// follows. Every doubly crashed image recovers one whole generation without
// losing a record.
#[tokio::test]
async fn crash_cuts_inside_recovery_from_every_compaction_cut_recover() {
	for (second, power_loss) in [(false, true), (true, true), (false, false), (true, false)] {
		let stop = |model: &PersistenceModel| {
			if power_loss {
				model.crash();
			} else {
				model.restart_process();
			}
		};
		let total = {
			let probe = scene(second).await;
			probe.rig.model.reset_events();
			run(&probe, None, true).await.0.unwrap();
			probe.rig.model.events()
		};
		let mut nested = 0;
		for outer in 1..=total {
			let inner_total = {
				let scene = scene(second).await;
				let _ = run(&scene, Some(Cut::Event(outer)), true).await;
				stop(&scene.rig.model);
				scene.rig.model.reset_events();
				recovery_work(&scene.rig).await;
				scene.rig.model.events()
			};
			for inner in 1..=inner_total {
				let context = format!(
					"second {second}, power loss {power_loss}, cut {outer}/{total}, recovery cut {inner}/{inner_total}"
				);
				let scene = scene(second).await;
				let max_op_before = max_op(&scene);
				let _ = run(&scene, Some(Cut::Event(outer)), true).await;
				stop(&scene.rig.model);
				scene.rig.model.arm_cut(Cut::Event(inner));
				recovery_work(&scene.rig).await;
				verify(scene, max_op_before, &context).await;
				nested += 1;
			}
		}
		assert!(nested > total, "the nested sweep ran ({nested} cuts)");
	}
}

// T-E5 control: publishing `CURRENT` before the new generation's data is
// synced, a crash just after `CURRENT` is durable leaves a generation whose
// bytes never reached the disk: recovery cannot read the records and fails
// closed. With the barriers in order, the same cut recovers.
#[tokio::test]
async fn current_published_before_the_generation_sync_loses_the_records() {
	for publish_before_sync in [false, true] {
		let scene = scene(false).await;
		store_switches(&scene.leader)
			.publish_before_sync
			.store(publish_before_sync, Ordering::Relaxed);
		// The second directory sync is the one after the `CURRENT` rename.
		let _ = run(&scene, Some(Cut::Label("sync_dir", 2)), false).await;
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		rig.model.crash();
		let recovered = rig.recover_in_place().await;
		if publish_before_sync {
			let Err(error) = recovered else {
				panic!("control: an unsynced generation behind CURRENT must not recover");
			};
			assert_eq!(
				fault_of(&error),
				Some(AppliedLogFault::GenerationMismatch { generation: 1 }),
				"control: the records are lost"
			);
		} else {
			let recovered = recovered.unwrap();
			assert_eq!(served(&recovered).0, 1);
		}
	}
}

// Every compaction fault: a failure before the `CURRENT` rename abandons the
// generation (the store keeps serving and appending to the old one); a failure
// at or after it latches the store. Either way a crash then recovers one whole
// generation and loses nothing, and the next compaction after recovery runs.
#[tokio::test]
async fn every_compaction_fault_abandons_or_latches_and_recovers() {
	let faults = [
		(Fault::GenerationCreateTorn, false),
		(Fault::GenerationDataSync, false),
		// Addendum A6: every directory-sync failure latches.
		(Fault::GenerationDirectorySync, true),
		(Fault::CurrentTempWrite, false),
		(Fault::CurrentTempSync, false),
		(Fault::CurrentRename, true),
		(Fault::CurrentDirectorySync, true),
		(Fault::PoisonRename, true),
		(Fault::RetireDirectorySync, true),
	];
	for (fault, latches) in faults {
		let context = format!("{fault:?}");
		let scene = scene(false).await;
		let max_op_before = max_op(&scene);
		scene.rig.model.arm(fault);
		let (result, _) = run(&scene, None, false).await;
		assert!(
			result.is_err(),
			"{context}: the compaction reports the failure"
		);
		assert_eq!(
			scene.leader.leader.durability_unsettled(),
			latches,
			"{context}: latched"
		);
		let tail = scene.tail;
		let added = scene
			.leader
			.fetch_add(tail.write_key, &tail.key(), tail.delta)
			.await;
		if latches {
			assert_eq!(
				added,
				Err(CounterLeaderError::Internal),
				"{context}: fails closed"
			);
		} else {
			assert_eq!(added, Ok(CounterAddResult::Applied(outcome(0, tail.delta))));
			assert_eq!(scene.leader.pending_witnesses(), 1, "{context}: served on");
			assert_eq!(served(&scene.leader).0, 0, "{context}: the old generation");
		}
		let Scene { rig, .. } = &scene;
		let rig_model = rig.model.clone();
		verify(scene, max_op_before, &context).await;
		// After recovery, nothing stale blocks the next compaction.
		let clock = TestClock::at(T0 + 3 * expiry());
		let rig = Rig::on_clock(rig_model, 1, clock);
		let recovered = rig.restart().await;
		recovered
			.leader
			.compact()
			.await
			.unwrap_or_else(|error| panic!("{context}: next compaction: {error}"));
	}
}

// The F-CTR-8 append faults on the current generation after a compaction: a
// torn append or a failed sync of a tail frame keeps a retryable outcome, and
// recovery repairs a torn tail without truncating into the generation's base.
#[tokio::test]
async fn append_failures_after_compaction_keep_a_retryable_outcome() {
	for fault in [
		Fault::AppliedAppendTorn,
		Fault::AppliedAppendTornUnnamed,
		Fault::AppliedDataSync,
	] {
		let scene = scene(false).await;
		run(&scene, None, false).await.0.unwrap();
		let (_, _, _, records) = served(&scene.leader);
		let kept = records.len();
		scene.rig.model.arm(fault);
		let tail = scene.tail;
		assert_eq!(
			scene
				.leader
				.fetch_add(tail.write_key, &tail.key(), tail.delta)
				.await,
			Ok(CounterAddResult::Applied(outcome(0, tail.delta))),
			"{fault:?}"
		);
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		let recovered = rig.restart().await;
		let (generation, _, _, records) = served(&recovered);
		assert_eq!(generation, 1, "{fault:?}");
		assert!(
			records.len() >= kept,
			"{fault:?}: the base survives the repair"
		);
		let retry = recovered
			.fetch_add(tail.write_key, &tail.key(), tail.delta)
			.await
			.unwrap();
		assert_eq!(retry.outcome(), outcome(0, tail.delta), "{fault:?}");
		recovered.reconcile().await;
		assert_eq!(recovered.read(&tail.key()).await.unwrap(), Some(tail.delta));
	}
}

// T-CUR: frames appended during and after a compaction form the tail beyond
// `CURRENT`'s base: on restart the base verifies and the tail parses. The
// control checks the digest over the whole file, and recovery fails closed.
#[tokio::test]
async fn appends_after_compaction_form_a_tail_the_base_checksum_excludes() {
	for whole_file in [false, true] {
		let scene = scene(false).await;
		let (result, tailed) = run(&scene, None, true).await;
		result.unwrap();
		assert!(tailed);
		let after = Add::new(scene.compact_at, 7, b"after", 2);
		scene
			.leader
			.fetch_add(after.write_key, &after.key(), after.delta)
			.await
			.unwrap();
		let generation = Path::new(WITNESS_DIR).join("counter-applied.1");
		let bytes = scene.rig.model.bytes(&generation).unwrap();
		let current = scene
			.rig
			.model
			.bytes(&Path::new(WITNESS_DIR).join("CURRENT"))
			.unwrap();
		let base_len = u64::from_le_bytes(current[16..24].try_into().unwrap()) as usize;
		assert!(bytes.len() > base_len, "a tail beyond the base");
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		let store = model_store(&rig.model);
		store
			.switches
			.whole_file_checksum
			.store(whole_file, Ordering::Relaxed);
		rig.model.crash();
		let recovered = rig.recover_with(store).await;
		if whole_file {
			let Err(error) = recovered else {
				panic!("control: a whole-file checksum must fail on a grown generation");
			};
			assert_eq!(
				fault_of(&error),
				Some(AppliedLogFault::GenerationMismatch { generation: 1 })
			);
		} else {
			let recovered = recovered.unwrap();
			let (_, _, _, records) = served(&recovered);
			let names: HashSet<&[u8]> = records
				.iter()
				.filter_map(|record| record.key.as_ref())
				.map(|key| key.0.as_slice())
				.collect();
			for counter in [&b"young"[..], b"tail", b"after"] {
				assert!(
					names.contains(counter),
					"{}",
					String::from_utf8_lossy(counter)
				);
			}
			assert_eq!(
				recovered
					.fetch_add(after.write_key, &after.key(), after.delta)
					.await,
				Ok(CounterAddResult::Replayed(outcome(0, after.delta)))
			);
		}
	}
}

// T-PUB-FAIL: when the directory sync after the `CURRENT` rename fails, the
// store latches unsettled and the scheduled compaction trips the fence; a later
// add is refused, so nothing is appended to a generation `CURRENT` may not
// name. The control does not latch: an add is appended to the unpublished
// generation and, after a crash, lost (its retry applies it a second time).
#[tokio::test]
async fn failed_publication_latches_and_fences_so_no_append_is_lost() {
	for no_latch in [false, true] {
		let scene = scene(false).await;
		store_switches(&scene.leader)
			.no_publish_latch
			.store(no_latch, Ordering::Relaxed);
		scene.rig.model.arm(Fault::CurrentDirectorySync);
		// The control stops at the retire step, so the crash lands before it.
		let retire = no_latch.then(|| {
			scene.rig.model.park_at(
				"write_file:/crash-cut/tenant-7/counter-witness/.applied-poison-tmp",
				1,
			)
		});
		let _release = retire.clone().map(ReleaseOnDrop);
		let fenced = Arc::new(tokio::sync::Notify::new());
		scene
			.leader
			.leader
			.compaction
			.force_due
			.store(true, Ordering::Relaxed);
		scene.leader.leader.compact_when_due({
			let fenced = fenced.clone();
			move || async move { fenced.notify_one() }
		});
		let tail = scene.tail;
		if let Some(retire) = retire {
			// The compaction carried on past the failure and parked at the retire.
			tokio::time::timeout(Duration::from_secs(5), retire.reached.notified())
				.await
				.expect("control: the compaction reaches its retire step");
			assert_eq!(served(&scene.leader).0, 1, "control: the cache moved on");
			assert_eq!(
				scene
					.leader
					.fetch_add(tail.write_key, &tail.key(), tail.delta)
					.await,
				Ok(CounterAddResult::Applied(outcome(0, tail.delta)))
			);
			assert_eq!(
				scene.leader.pending_witnesses(),
				1,
				"only the scene's pending"
			);
			scene.rig.model.halt();
			retire.release();
			for _ in 0..200 {
				if !scene
					.leader
					.leader
					.compaction
					.running
					.load(Ordering::Acquire)
				{
					break;
				}
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		} else {
			tokio::time::timeout(Duration::from_secs(5), fenced.notified())
				.await
				.expect("the fence trips");
			assert!(scene.leader.leader.durability_unsettled());
			assert_eq!(
				scene
					.leader
					.fetch_add(tail.write_key, &tail.key(), tail.delta)
					.await,
				Err(CounterLeaderError::Internal),
				"a latched store takes no add"
			);
		}
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		let recovered = rig.restart().await;
		assert_eq!(served(&recovered).0, 0, "CURRENT never became durable");
		let retry = recovered
			.fetch_add(tail.write_key, &tail.key(), tail.delta)
			.await
			.unwrap();
		recovered.reconcile().await;
		let value = recovered.read(&tail.key()).await.unwrap();
		if no_latch {
			assert_eq!(
				(retry, value),
				(
					CounterAddResult::Applied(outcome(tail.delta, 2 * tail.delta)),
					Some(2 * tail.delta)
				),
				"control: the appended record was lost and the add applied twice"
			);
		} else {
			assert_eq!(retry, CounterAddResult::Applied(outcome(0, tail.delta)));
			assert_eq!(value, Some(tail.delta), "applied exactly once");
		}
	}
}

// T-STALE: recovery deletes generations other than the one `CURRENT` names and
// the `CURRENT` and poison temp files, so the next compaction's `create_new`
// succeeds. The control skips the cleanup, and that `create_new` fails.
#[tokio::test]
async fn recovery_cleans_stale_generations_and_temp_files() {
	for skip_cleanup in [false, true] {
		let scene = scene(false).await;
		run(&scene, None, false).await.0.unwrap();
		let Scene {
			rig, leader, clock, ..
		} = scene;
		drop(leader);
		let dir = Path::new(WITNESS_DIR);
		let stale = [
			dir.join("counter-applied.2"),
			dir.join(compaction::CURRENT_TEMP_FILE),
			dir.join(compaction::POISON_TEMP_FILE),
		];
		for path in &stale {
			rig.model.put_durable(path, b"stale".to_vec());
		}
		let store = model_store(&rig.model);
		store
			.switches
			.skip_recovery_cleanup
			.store(skip_cleanup, Ordering::Relaxed);
		rig.model.crash();
		let recovered = rig.recover_with(store).await.unwrap();
		assert_eq!(served(&recovered).0, 1);
		clock.set(T0 + 3 * expiry());
		let next = recovered.leader.compact().await;
		if skip_cleanup {
			let error = next.expect_err("control: the stale generation blocks create_new");
			assert_eq!(error.kind(), ErrorKind::AlreadyExists);
		} else {
			for path in &stale[1..] {
				assert!(
					rig.model.bytes(path).is_none(),
					"{} removed",
					path.display()
				);
			}
			assert_eq!(next.unwrap().generation, 2);
		}
	}
}

// T-NOCUR: with `CURRENT` absent, recovery fails closed when generation 0 is
// absent but a numbered generation exists, or when generation 0 is the poison;
// it serves a valid generation 0 and deletes every numbered generation; and a
// fresh directory, with or without a pending witness, recovers and serves.
// The control treats a lone numbered generation as an empty store, and a
// deleted identity is applied again.
#[tokio::test]
async fn missing_current_fails_closed_or_serves_generation_zero() {
	let dir = Path::new(WITNESS_DIR);
	// Generation 0 absent, generation 1 present; and generation 0 poisoned.
	for (remove_zero, control) in [(true, false), (false, false), (true, true)] {
		let scene = scene(false).await;
		let old = scene.adds[0].0;
		run(&scene, None, false).await.0.unwrap();
		let Scene {
			rig, leader, clock, ..
		} = scene;
		drop(leader);
		rig.model.remove_durable(&dir.join("CURRENT"));
		if remove_zero {
			rig.model.remove_durable(&dir.join("counter-applied.log"));
		} else {
			assert_eq!(
				rig.model.bytes(&dir.join("counter-applied.log")),
				Some(compaction::POISON.to_vec())
			);
		}
		let store = model_store(&rig.model);
		store
			.switches
			.empty_without_current
			.store(control, Ordering::Relaxed);
		rig.model.crash();
		let recovered = rig.recover_with(store).await;
		if control {
			let recovered = recovered.unwrap();
			clock.set(old.issued + 1);
			assert_eq!(
				recovered
					.fetch_add(old.write_key, &old.key(), old.delta)
					.await,
				Ok(CounterAddResult::Applied(outcome(old.delta, 2 * old.delta))),
				"control: an empty fallback reapplies a deleted identity"
			);
		} else {
			let Err(error) = recovered else {
				panic!("remove_zero {remove_zero}: recovery must fail closed");
			};
			assert_eq!(fault_of(&error), Some(AppliedLogFault::NoAuthority));
			assert!(
				rig.model.bytes(&dir.join("counter-applied.1")).is_some(),
				"untouched"
			);
		}
	}

	// A valid generation 0 with stale numbered generations: generation 0 serves.
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	{
		let leader = rig.leader().await;
		leader
			.fetch_add(wk_at(T0, 1), &ckey(b"zero"), 2)
			.await
			.unwrap();
	}
	for n in [1, 7] {
		rig.model
			.put_durable(&dir.join(format!("counter-applied.{n}")), b"junk".to_vec());
	}
	let recovered = rig.restart().await;
	assert_eq!(served(&recovered).0, 0);
	for n in [1, 7] {
		assert!(
			rig.model
				.bytes(&dir.join(format!("counter-applied.{n}")))
				.is_none()
		);
	}
	assert_eq!(
		recovered.fetch_add(wk_at(T0, 1), &ckey(b"zero"), 2).await,
		Ok(CounterAddResult::Replayed(outcome(0, 2)))
	);

	// A fresh directory, without and with a pending witness.
	for with_witness in [false, true] {
		let clock = TestClock::at(T0);
		let rig = rig_on(&clock);
		if with_witness {
			let store = model_store(&rig.model);
			let mut witness = fixture_witness(1, wk_at(T0, 3), b"fresh");
			witness.committed.clear();
			store.write(&witness).unwrap();
		}
		let recovered = rig.restart().await;
		recovered.reconcile().await;
		assert_eq!(
			recovered.pending_witnesses(),
			0,
			"with witness {with_witness}"
		);
		assert_eq!(
			recovered.fetch_add(wk_at(T0, 4), &ckey(b"fresh"), 1).await,
			Ok(CounterAddResult::Applied(outcome(
				if with_witness { 5 } else { 0 },
				if with_witness { 6 } else { 1 }
			)))
		);
	}
}

// T-POISON: after the first compaction generation 0 holds the poison, which
// the applied-log parser (as an older binary has it) rejects. The control
// poisons generation 0 before `CURRENT` is durable; a crash there leaves no
// authority and recovery fails closed.
#[tokio::test]
async fn generation_zero_is_poisoned_only_after_current_is_durable() {
	let zero = Path::new(WITNESS_DIR).join("counter-applied.log");
	let first = scene(false).await;
	run(&first, None, false).await.0.unwrap();
	let bytes = first.rig.model.bytes(&zero).unwrap();
	assert_eq!(bytes, compaction::POISON.to_vec());
	assert!(applied_log::parse(&bytes).is_err(), "old parsers reject it");

	// Addendum A1: a crash between the `CURRENT` directory sync and the poison
	// leaves generation 0 unpoisoned beside `CURRENT`; recovery poisons it. The
	// control skips the re-poison, and an old-format parser still reads it.
	for skip in [false, true] {
		let scene = scene(false).await;
		let _ = run(&scene, Some(Cut::Label("sync_dir", 2)), false).await;
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		rig.model.crash();
		assert!(
			rig.model
				.bytes(&Path::new(WITNESS_DIR).join("CURRENT"))
				.is_some()
		);
		assert!(applied_log::parse(&rig.model.bytes(&zero).unwrap()).is_ok());
		let store = model_store(&rig.model);
		store
			.switches
			.skip_recovery_poison
			.store(skip, Ordering::Relaxed);
		let recovered = rig.recover_with(store).await.unwrap();
		assert_eq!(served(&recovered).0, 1);
		assert!(!recovered.leader.durability_unsettled());
		drop(recovered);
		rig.model.crash();
		let bytes = rig.model.bytes(&zero).unwrap();
		if skip {
			assert!(
				applied_log::parse(&bytes).is_ok(),
				"control: without the re-poison an old parser reads generation 0"
			);
		} else {
			assert_eq!(bytes, compaction::POISON.to_vec(), "durably poisoned");
		}
	}

	for poison_early in [false, true] {
		let scene = scene(false).await;
		store_switches(&scene.leader)
			.poison_early
			.store(poison_early, Ordering::Relaxed);
		let _ = run(&scene, Some(Cut::Label("sync_dir", 2)), false).await;
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		rig.model.crash();
		let recovered = rig.recover_in_place().await;
		if poison_early {
			let Err(error) = recovered else {
				panic!("control: a poison durable before CURRENT leaves no authority");
			};
			assert_eq!(fault_of(&error), Some(AppliedLogFault::NoAuthority));
		} else {
			assert_eq!(served(&recovered.unwrap()).0, 1);
		}
	}
}

// T-DIV: a foreign write to the current (compacted) generation is a length
// this store did not write: the next resolution fails closed as `Diverged`,
// appends nothing and keeps its witness.
#[tokio::test]
async fn foreign_write_to_the_current_generation_diverges() {
	let scene = scene(false).await;
	run(&scene, None, false).await.0.unwrap();
	let generation = Path::new(WITNESS_DIR).join("counter-applied.1");
	let written = scene.rig.model.bytes(&generation).unwrap();
	let mut grown = written.clone();
	grown.extend_from_slice(b"foreign");
	scene.rig.model.put_durable(&generation, grown.clone());
	let tail = scene.tail;
	assert_eq!(
		scene
			.leader
			.fetch_add(tail.write_key, &tail.key(), tail.delta)
			.await,
		Ok(CounterAddResult::Applied(outcome(0, tail.delta)))
	);
	assert_eq!(scene.leader.pending_witnesses(), 2, "not resolved");
	assert_eq!(scene.rig.model.bytes(&generation), Some(grown.clone()));
	let witness = scene
		.leader
		.leader
		.witnesses
		.iter()
		.find(|witness| witness.write_key == tail.write_key)
		.unwrap()
		.clone();
	let error = scene
		.leader
		.leader
		.witness_store
		.0
		.append_applied(&applied_log::record_for(&witness), &witness)
		.unwrap_err();
	assert_eq!(
		fault_of(&error),
		Some(AppliedLogFault::Diverged {
			expected_len: written.len() as u64,
			observed_len: Some(grown.len() as u64),
		})
	);
}

/// `windows` windows of `per_window` adds, the clock stepping `E / 2` between
/// windows, with or without a compaction after each. Returns the records the
/// restarted store serves and the bytes its recovery read.
async fn bounded_run(windows: u64, per_window: u64, compaction: bool) -> (usize, u64, usize) {
	let step = expiry() / 2;
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	{
		let leader = rig.leader().await;
		for window in 0..windows {
			let now = T0 + window * step;
			clock.set(now);
			for n in 0..per_window {
				leader
					.fetch_add(wk_at(now, window * per_window + n), &ckey(b"bounded"), 1)
					.await
					.unwrap();
			}
			if compaction {
				compact(&leader).await;
			}
		}
	}
	rig.model.take_bytes_read();
	let recovered = rig.restart().await;
	let read = rig.model.take_bytes_read();
	let live = served(&recovered).3.len();
	let map = recovered.leader.idempotency.len();
	assert_eq!(
		recovered.read(&ckey(b"bounded")).await.unwrap(),
		Some(windows * per_window)
	);
	(live, read, map)
}

// T-E4: across many E windows the live records stay O(rate x E) and the
// recovery read stays proportional to them, whatever the total number of adds.
// The control never compacts, and the recovery read grows with the total.
#[tokio::test]
async fn live_state_and_recovery_read_stay_bounded_across_many_windows() {
	let per_window = 10;
	let (live_short, read_short, map_short) = bounded_run(6, per_window, true).await;
	let (live_long, read_long, map_long) = bounded_run(12, per_window, true).await;
	// Live records span at most `E` plus one compaction interval of issuance:
	// with the clock stepping `E / 2`, the two newest windows.
	for (live, map) in [(live_short, map_short), (live_long, map_long)] {
		assert!(live <= 2 * per_window as usize, "live records {live}");
		assert!(
			map <= live,
			"the record map is bounded by the live set ({map})"
		);
	}
	assert_eq!(live_short, live_long, "independent of the number of adds");
	assert!(
		read_long <= read_short + 256,
		"the recovery read does not grow with the adds: {read_short} then {read_long}"
	);
	// The control: no compaction.
	let (live_short, read_short, _) = bounded_run(6, per_window, false).await;
	let (live_long, read_long, _) = bounded_run(12, per_window, false).await;
	assert_eq!(
		(live_short, live_long),
		(6 * per_window as usize, 12 * per_window as usize)
	);
	assert!(
		read_long >= read_short * 18 / 10,
		"control: the recovery read grows with the adds: {read_short} then {read_long}"
	);
}

// Addendum A4, the recovery barrier, against the store's own open-time
// directory sync. A process that died just after the `CURRENT` rename left
// `CURRENT` volatile; after a process restart (no power loss) the store is
// opened and recovers, and a power loss lands in the middle of its cleanup
// with the kernel persisting the unlink at once. Either mechanism alone (the
// open sync, or A4's barrier) makes `CURRENT` durable before the cleanup and
// generation g+1 recovers; with both disabled the unlink of generation g
// persists while `CURRENT` still names it, and nothing recovers.
#[tokio::test]
async fn open_sync_and_recovery_barrier_each_make_a_volatile_current_durable() {
	for (open_sync, barrier) in [(true, true), (true, false), (false, true), (false, false)] {
		let context = format!("open sync {open_sync}, A4 barrier {barrier}");
		let scene = scene(true).await;
		let max_op_before = max_op(&scene);
		// The second compaction's first rename is `CURRENT`'s (generation 0 is
		// already the poison): the process dies before its directory sync.
		let _ = run(&scene, Some(Cut::Label("rename", 1)), false).await;
		scene.rig.model.restart_process();
		scene.rig.model.eager_unlinks();
		let store = if open_sync {
			model_store(&scene.rig.model)
		} else {
			DurableCounterWitnessStore::open_without_open_sync(WITNESS_DIR, scene.rig.model.clone())
		};
		store
			.switches
			.skip_recovery_barrier
			.store(!barrier, Ordering::Relaxed);
		scene.rig.model.arm_cut(Cut::Label("unlink", 1));
		let _ = scene.rig.recover_with(store).await;
		if open_sync || barrier {
			verify(scene, max_op_before, &context).await;
			continue;
		}
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		rig.model.crash();
		let Err(error) = rig.recover_in_place().await else {
			panic!("{context}: with neither mechanism the cleanup must leave no authority");
		};
		assert_eq!(
			fault_of(&error),
			Some(AppliedLogFault::GenerationMismatch { generation: 1 }),
			"{context}: CURRENT still names the deleted generation"
		);
	}
}

/// Addendum A5's order: each new name is followed by a directory sync before
/// the next step that depends on it. The generation file before `CURRENT` is
/// renamed; `CURRENT` before any retirement (an unlink or the poison's temp);
/// the poison's rename before anything else or the end; and recovery's
/// barrier (the named generation's `sync_data`, then a directory sync)
/// before any recovery delete, rename or write.
fn check_new_name_order(trace: &[String], recovery_from: usize) -> Result<(), String> {
	// The sync must fall inside the anchor's own phase: the compaction, or the
	// recovery after it (whose store open syncs the directory too).
	let synced_before =
		|from: usize, end: usize, dependent: &dyn Fn(&str) -> bool| -> Result<(), String> {
			for event in &trace[from + 1..end] {
				if event.starts_with("sync_dir:") {
					return Ok(());
				}
				if dependent(event) {
					return Err(format!(
						"{} precedes a directory sync after {}",
						event, trace[from]
					));
				}
			}
			Err(format!("no directory sync after {}", trace[from]))
		};
	let mutating = |event: &str| {
		[
			"unlink:",
			"rename:",
			"write_file:",
			"append_file:",
			"create_new_file:",
		]
		.iter()
		.any(|op| event.starts_with(op))
	};
	for (at, event) in trace.iter().enumerate().take(recovery_from) {
		if event.starts_with("create_new_file:") && event.contains("counter-applied.") {
			synced_before(at, recovery_from, &|e| {
				e.starts_with("rename:") && e.ends_with("/CURRENT")
			})?;
		}
		if event.starts_with("rename:") && event.ends_with("/CURRENT") {
			synced_before(at, recovery_from, &|e| {
				e.starts_with("unlink:")
					|| (e.starts_with("write_file:") && e.ends_with(".applied-poison-tmp"))
			})?;
		}
		if event.starts_with("rename:") && event.ends_with("/counter-applied.log") {
			synced_before(at, recovery_from, &mutating)?;
		}
	}
	let recovery = &trace[recovery_from..];
	let barrier = recovery
		.iter()
		.position(|e| {
			e.starts_with("sync_data:") && e.contains("counter-applied.") && !e.ends_with(".log")
		})
		.ok_or("recovery has no barrier sync of the named generation")?;
	if let Some(early) = recovery[..barrier].iter().find(|e| mutating(e)) {
		return Err(format!("{early} precedes recovery's barrier"));
	}
	synced_before(recovery_from + barrier, trace.len(), &mutating)?;
	for (at, event) in recovery.iter().enumerate() {
		if event.starts_with("rename:") && event.ends_with("/counter-applied.log") {
			synced_before(recovery_from + at, trace.len(), &mutating)?;
		}
	}
	Ok(())
}

// A5 (required): on the model's operation trace of a first compaction and a
// recovery with `CURRENT` present (with a stale temp to delete and an
// unpoisoned generation 0 to re-poison), every new name is followed by its
// directory sync before the next dependent step. The control removes one
// directory sync from the trace, and the check fails.
#[tokio::test]
async fn every_new_name_is_synced_before_its_dependent_step() {
	let scene = scene(false).await;
	run(&scene, None, false).await.0.unwrap();
	let Scene { rig, leader, .. } = scene;
	drop(leader);
	let dir = Path::new(WITNESS_DIR);
	rig.model
		.put_durable(&dir.join(compaction::CURRENT_TEMP_FILE), b"stale".to_vec());
	rig.model.put_durable(
		&dir.join("counter-applied.log"),
		applied_log::encode_frame(&AppliedCounterAdd {
			op_id: 1,
			write_key: wk_at(T0, 1),
			key: Some(ckey(b"old")),
			previous: 0,
			current: 3,
		})
		.unwrap(),
	);
	rig.model.crash();
	let recovery_from = rig.model.trace().len();
	let recovered = rig.recover_in_place().await.unwrap();
	assert_eq!(served(&recovered).0, 1);
	let trace = rig.model.trace();
	assert!(
		trace[recovery_from..]
			.iter()
			.any(|e| e.starts_with("unlink:") && e.ends_with(".current-tmp")),
		"recovery deleted the stale temp"
	);
	assert!(
		trace[recovery_from..]
			.iter()
			.any(|e| e.starts_with("rename:") && e.ends_with("/counter-applied.log")),
		"recovery re-poisoned generation 0"
	);
	check_new_name_order(&trace, recovery_from).unwrap();

	// The control: drop the directory sync that follows each new name in turn.
	let anchors: Vec<usize> = trace
		.iter()
		.enumerate()
		.filter(|(_, e)| {
			(e.starts_with("rename:")
				&& (e.ends_with("/CURRENT") || e.ends_with("/counter-applied.log")))
				|| (e.starts_with("create_new_file:") && e.contains("counter-applied."))
				|| (e.starts_with("sync_data:")
					&& e.contains("counter-applied.")
					&& !e.ends_with(".log"))
		})
		.map(|(at, _)| at)
		.collect();
	assert!(
		anchors.len() >= 5,
		"every new name and the barrier: {anchors:?}"
	);
	for anchor in anchors {
		let sync = anchor
			+ trace[anchor..]
				.iter()
				.position(|e| e.starts_with("sync_dir:"))
				.unwrap();
		let mut mutated = trace.clone();
		mutated.remove(sync);
		let from = if sync < recovery_from {
			recovery_from - 1
		} else {
			recovery_from
		};
		assert!(
			check_new_name_order(&mutated, from).is_err(),
			"control: removing the sync after {} must fail the order",
			trace[anchor]
		);
	}
}

// Addendum A6: a failed phase-3 directory sync (before `CURRENT` is renamed)
// latches the store and trips the fence, so no later witness is written into
// a directory whose dirty names an fsync error may have dropped. The control
// keeps the abandon-only semantics: the store serves on, and under
// fsync-error semantics the later add's witness never becomes durable, so a
// power loss after its participant commit leaves no representation and the
// retry applies it a second time.
#[tokio::test]
async fn any_compaction_directory_sync_failure_latches_and_fences() {
	for abandon_only in [false, true] {
		let scene = scene(false).await;
		store_switches(&scene.leader)
			.abandon_on_directory_sync
			.store(abandon_only, Ordering::Relaxed);
		scene.rig.model.sticky_directory_errors();
		scene.rig.model.arm(Fault::GenerationDirectorySync);
		let fenced = Arc::new(tokio::sync::Notify::new());
		scene
			.leader
			.leader
			.compaction
			.force_due
			.store(true, Ordering::Relaxed);
		scene.leader.leader.compact_when_due({
			let fenced = fenced.clone();
			move || async move { fenced.notify_one() }
		});
		for _ in 0..400 {
			if !scene
				.leader
				.leader
				.compaction
				.running
				.load(Ordering::Acquire)
			{
				break;
			}
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
		let tail = scene.tail;
		if abandon_only {
			assert!(
				!scene.leader.leader.durability_unsettled(),
				"control: abandoned"
			);
			assert_eq!(served(&scene.leader).0, 0);
			// The later add commits on its participant, then the power fails.
			scene.rig.model.arm_cut(Cut::Label("participant_marker", 1));
			let _ = scene
				.leader
				.fetch_add(tail.write_key, &tail.key(), tail.delta)
				.await;
		} else {
			tokio::time::timeout(Duration::from_secs(5), fenced.notified())
				.await
				.expect("the fence trips");
			assert!(scene.leader.leader.durability_unsettled());
			assert_eq!(
				scene
					.leader
					.fetch_add(tail.write_key, &tail.key(), tail.delta)
					.await,
				Err(CounterLeaderError::Internal),
				"a latched store writes no witness"
			);
		}
		let Scene { rig, leader, .. } = scene;
		drop(leader);
		let recovered = rig.restart().await;
		let retry = recovered
			.fetch_add(tail.write_key, &tail.key(), tail.delta)
			.await
			.unwrap();
		recovered.reconcile().await;
		let value = recovered.read(&tail.key()).await.unwrap();
		if abandon_only {
			assert_eq!(
				(retry, value),
				(
					CounterAddResult::Applied(outcome(tail.delta, 2 * tail.delta)),
					Some(2 * tail.delta)
				),
				"control: the later witness was lost and the add applied twice"
			);
		} else {
			assert_eq!(retry, CounterAddResult::Applied(outcome(0, tail.delta)));
			assert_eq!(value, Some(tail.delta), "applied exactly once");
		}
	}
}
