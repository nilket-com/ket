//! The dedup horizon (E0, E1, E3) through the real leader, its compaction and
//! its recovery, on a controlled wall clock. Each safety law has a negative
//! control: the test switch that disables the rule reproduces the failure it
//! prevents (a reapplied add, a reused op id).
use super::*;

use crate::storage::counter::compaction::CompactionReport;

/// A wall time far from the epoch, so `now - E` never saturates.
pub(super) const T0: u64 = 1_900_000_000_000;

pub(super) fn expiry() -> u64 {
	CounterRetentionConfig::default().expiry_ms
}

pub(super) fn skew() -> u64 {
	CounterRetentionConfig::default().clock_skew_ms
}

pub(super) fn horizon() -> u64 {
	CounterRetentionConfig::default().attempt_horizon_ms
}

/// A write key whose uuid is a UUIDv4 (not presentable under E0).
pub(super) fn v4_key(n: u64) -> CounterWriteKey {
	let mut key = wk_at(T0, n);
	key[6] = 0x40;
	key
}

/// A one-participant rig whose leaders run on `clock`.
pub(super) fn rig_on(clock: &Arc<TestClock>) -> Rig {
	Rig::on_clock(PersistenceModel::new(), 1, clock.clone())
}

pub(super) async fn compact(harness: &LeaderHarness) -> CompactionReport {
	harness.leader.compact().await.expect("compaction")
}

/// The generation the leader's store serves: its number, cutoff, high-water
/// and records.
pub(super) fn served(harness: &LeaderHarness) -> (u64, u64, u64, Vec<AppliedCounterAdd>) {
	let state = harness.leader.witness_store.0.lock_applied().unwrap();
	match &*state {
		AppliedLogState::Loaded(image) => (
			image.generation,
			image.cutoff,
			image.high_water,
			image.records.clone(),
		),
		_ => panic!("the store serves no loaded generation"),
	}
}

pub(super) fn store_switches(harness: &LeaderHarness) -> &compaction::CompactionSwitches {
	&harness.leader.witness_store.0.switches
}

fn applied(previous: u64, current: u64) -> Result<CounterAddResult, CounterLeaderError> {
	Ok(CounterAddResult::Applied(outcome(previous, current)))
}

fn replayed(previous: u64, current: u64) -> Result<CounterAddResult, CounterLeaderError> {
	Ok(CounterAddResult::Replayed(outcome(previous, current)))
}

// T-E0: a v4 identity and one issued beyond `now + S` are refused with nothing
// applied; `now + S` exactly is accepted; a v7 identity applies (the control).
// E0 runs before the write-key gate: a request refused by E0 never waits on
// the gate another request under its write key holds.
#[tokio::test]
async fn e0_refuses_non_v7_and_future_identities_before_the_gate() {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	let leader = rig.leader().await;
	let key = ckey(b"e0");
	assert_eq!(
		leader.fetch_add(v4_key(1), &key, 5).await,
		Err(CounterLeaderError::InvalidWriteIdentity)
	);
	assert_eq!(
		leader.fetch_add(wk_at(T0 + skew() + 1, 2), &key, 5).await,
		Err(CounterLeaderError::InvalidWriteIdentity)
	);
	assert_eq!(leader.read(&key).await.unwrap(), None, "nothing applied");
	assert!(
		!rig.model.has_witness_name(),
		"no witness for a refused add"
	);
	assert_eq!(
		leader.fetch_add(wk_at(T0 + skew(), 3), &key, 5).await,
		applied(0, 5),
		"now + S exactly is presentable"
	);
	// The control: a v7 identity issued now applies.
	assert_eq!(leader.fetch_add(wk_at(T0, 4), &key, 1).await, applied(5, 6));

	// Before the gate: park a valid add under W holding W's gate, then step the
	// clock back so W is issued beyond `now + S`. A second request under W is
	// refused at once, without waiting for the gate.
	let gated = wk_at(T0, 5);
	let park = Arc::new(LookupPark::default());
	*leader.leader.retention_switches.lookup_park.lock().unwrap() = Some(park.clone());
	let parked = {
		let plan = leader.plan();
		let harness_leader = leader.leader.clone();
		let key = key.clone();
		tokio::spawn(async move { harness_leader.fetch_add(plan, gated, &key, 2).await })
	};
	tokio::time::timeout(Duration::from_secs(5), park.reached.notified())
		.await
		.expect("the add parks before its lookup");
	clock.set(T0 - skew() - 1);
	assert_eq!(
		tokio::time::timeout(Duration::from_secs(5), leader.fetch_add(gated, &key, 2)).await,
		Ok(Err(CounterLeaderError::InvalidWriteIdentity)),
		"E0 answers before the write-key gate"
	);
	clock.set(T0);
	park.release.notify_one();
	assert_eq!(parked.await.unwrap(), applied(6, 8));
}

// T-E1: live and after restart, an expired write key is refused with
// `OperationExpired` whether its record is present or absent, and nothing is
// applied. At the boundary `issued = Q` it is refused; issued just after the
// cutoff (the control) it reaches K1/K2: a retry replays, another add conflicts.
#[tokio::test]
async fn e1_refuses_expired_write_keys_live_after_restart_and_at_the_cutoff() {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	let key = ckey(b"e1");
	let old = wk_at(T0, 1);
	let fresh_old = wk_at(T0, 2);
	let after_cutoff = wk_at(T0 + 1, 3);
	{
		let leader = rig.leader().await;
		assert_eq!(leader.fetch_add(old, &key, 5).await, applied(0, 5));
		clock.set(T0 + 1);
		assert_eq!(leader.fetch_add(after_cutoff, &key, 2).await, applied(5, 7));
		clock.set(T0 + expiry());
		for (write_key, what) in [(old, "record present"), (fresh_old, "no record")] {
			assert_eq!(
				leader.fetch_add(write_key, &key, 5).await,
				Err(CounterLeaderError::OperationExpired),
				"live, {what}"
			);
		}
		assert_eq!(leader.read(&key).await.unwrap(), Some(7), "nothing applied");
	}
	let leader = rig.restart().await;
	for (write_key, what) in [(old, "record present"), (fresh_old, "no record")] {
		assert_eq!(
			leader.fetch_add(write_key, &key, 5).await,
			Err(CounterLeaderError::OperationExpired),
			"after restart, {what}"
		);
	}
	// Compact at `T0 + E`: the cutoff is exactly `T0`, the issuance of `old`.
	let report = compact(&leader).await;
	assert_eq!(report.cutoff, T0);
	assert_eq!(leader.leader.witness_store.0.cutoff(), T0);
	// Step the clock back so `now - E` no longer covers either key; only `Q` does.
	clock.set(T0 + 1);
	assert_eq!(
		leader.fetch_add(old, &key, 5).await,
		Err(CounterLeaderError::OperationExpired),
		"issued = Q exactly is refused"
	);
	// The control, issued just after the cutoff: K1 replays, K2 conflicts.
	assert_eq!(
		leader.fetch_add(after_cutoff, &key, 2).await,
		replayed(5, 7)
	);
	assert_eq!(
		leader.fetch_add(after_cutoff, &key, 3).await,
		Err(CounterLeaderError::WriteKeyConflict)
	);
	assert_eq!(leader.read(&key).await.unwrap(), Some(7));
}

/// Park a retry of `write_key` between its gate and its lookup, compact while it
/// is parked (the record is deleted and evicted), step the clock back so only
/// `Q_mem` covers the key, then release it.
async fn retry_across_a_compaction(expiry_before_lookup: bool) -> CounterAddResult {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	let leader = rig.leader().await;
	let key = ckey(b"race");
	let write_key = wk_at(T0, 1);
	assert_eq!(leader.fetch_add(write_key, &key, 5).await, applied(0, 5));
	let switches = &leader.leader.retention_switches;
	switches
		.expiry_before_lookup
		.store(expiry_before_lookup, Ordering::Relaxed);
	let park = Arc::new(LookupPark::default());
	*switches.lookup_park.lock().unwrap() = Some(park.clone());
	let retry = {
		let harness_leader = leader.leader.clone();
		let plan = leader.plan();
		let key = key.clone();
		tokio::spawn(async move { harness_leader.fetch_add(plan, write_key, &key, 5).await })
	};
	tokio::time::timeout(Duration::from_secs(5), park.reached.notified())
		.await
		.expect("the add parks before its lookup");
	clock.set(T0 + expiry());
	let report = compact(&leader).await;
	assert_eq!((report.cutoff, report.deleted), (T0, 1));
	assert!(
		leader.leader.idempotency.get(&write_key).is_none(),
		"evicted"
	);
	clock.set(T0 + 10);
	park.release.notify_one();
	match retry.await.unwrap() {
		Ok(result) => result,
		Err(error) => {
			assert_eq!(error, CounterLeaderError::OperationExpired);
			assert_eq!(leader.read(&key).await.unwrap(), Some(5), "never reapplied");
			CounterAddResult::Replayed(outcome(0, 0))
		}
	}
}

// T-RACE: a retry parked between its write-key gate and its record lookup
// while compaction deletes and evicts its record is refused: the lookup misses,
// and E1, read after it, sees the published cutoff. The control evaluates E1
// before the lookup, and the add is applied a second time.
#[tokio::test]
async fn retry_parked_across_compaction_is_refused_not_reapplied() {
	assert_eq!(
		retry_across_a_compaction(false).await,
		CounterAddResult::Replayed(outcome(0, 0)),
		"refused with OperationExpired"
	);
	assert_eq!(
		retry_across_a_compaction(true).await,
		CounterAddResult::Applied(outcome(5, 10)),
		"control: E1 before the lookup reapplies the add"
	);
}

/// The fixtures T-E3 compacts: old, young, old with a surviving witness file,
/// pending, a non-v7 record, and one between `now - E` and `now - H`.
struct PruneFixture {
	rig: Rig,
	clock: Arc<TestClock>,
	leader: LeaderHarness,
	old: CounterWriteKey,
	witnessed: CounterWriteKey,
	pending: CounterWriteKey,
	young: CounterWriteKey,
	between: CounterWriteKey,
	non_v7: CounterWriteKey,
}

async fn prune_fixture() -> PruneFixture {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	// A record written before E0 existed: a non-v7 write key.
	model_store(&rig.model);
	let non_v7 = v4_key(90);
	rig.model.put_durable(
		&applied_log_path(),
		applied_log::encode_frame(&AppliedCounterAdd {
			op_id: 1,
			write_key: non_v7,
			key: Some(ckey(b"legacy")),
			previous: 0,
			current: 4,
		})
		.unwrap(),
	);
	let leader = rig.restart().await;
	let (old, witnessed, pending) = (wk_at(T0, 1), wk_at(T0, 2), wk_at(T0, 3));
	leader.fetch_add(old, &ckey(b"old"), 1).await.unwrap();
	rig.model.arm(Fault::WitnessUnlink);
	leader
		.fetch_add(witnessed, &ckey(b"witnessed"), 1)
		.await
		.unwrap();
	leader.arm_participant_commit_failure(0);
	leader
		.fetch_add(pending, &ckey(b"pending"), 1)
		.await
		.unwrap();
	assert_eq!(leader.pending_witnesses(), 2, "witnessed and pending");
	let between_issued = T0 + expiry() - horizon() - 2_000;
	clock.set(between_issued);
	let between = wk_at(between_issued, 4);
	leader
		.fetch_add(between, &ckey(b"between"), 1)
		.await
		.unwrap();
	clock.set(T0 + expiry());
	let young = wk_at(T0 + expiry(), 5);
	leader.fetch_add(young, &ckey(b"young"), 1).await.unwrap();
	// Compaction runs at `T0 + E + 1000`: the cutoff is `T0 + 1000`.
	clock.set(T0 + expiry() + 1_000);
	PruneFixture {
		rig,
		clock,
		leader,
		old,
		witnessed,
		pending,
		young,
		between,
		non_v7,
	}
}

// T-E3: compaction keeps exactly the unexpired records, the pending witnesses
// and every record whose witness file survives; it drops the expired and the
// non-v7 records, and every dropped write key is refused (22, or 25 for the
// non-v7 one). The control deletes by `H` instead of the published cutoff,
// and an add between `now - E` and `now - H` is applied again.
#[tokio::test]
async fn compaction_keeps_unexpired_and_witnessed_records_and_drops_the_rest() {
	let f = prune_fixture().await;
	let report = compact(&f.leader).await;
	assert_eq!(report.cutoff, T0 + 1_000);
	let (generation, cutoff, _, records) = served(&f.leader);
	assert_eq!((generation, cutoff), (1, T0 + 1_000));
	let mut kept: Vec<_> = records.iter().map(|record| record.write_key).collect();
	kept.sort();
	let mut expected = vec![f.witnessed, f.between, f.young];
	expected.sort();
	assert_eq!(
		kept, expected,
		"exactly the unexpired and witnessed records"
	);
	assert_eq!(f.leader.pending_witnesses(), 2, "witnesses never pruned");
	assert!(f.leader.leader.idempotency.get(&f.pending).is_some());
	assert!(f.leader.leader.idempotency.get(&f.witnessed).is_some());
	assert!(f.leader.leader.idempotency.get(&f.old).is_none(), "evicted");
	assert_eq!(
		f.leader.fetch_add(f.old, &ckey(b"old"), 1).await,
		Err(CounterLeaderError::OperationExpired)
	);
	assert_eq!(
		f.leader.fetch_add(f.non_v7, &ckey(b"legacy"), 4).await,
		Err(CounterLeaderError::InvalidWriteIdentity)
	);
	assert_eq!(
		f.leader.fetch_add(f.between, &ckey(b"between"), 1).await,
		replayed(0, 1)
	);
	assert_eq!(
		f.leader.fetch_add(f.young, &ckey(b"young"), 1).await,
		replayed(0, 1)
	);
	// After a restart the kept set is what recovery serves.
	let PruneFixture { rig, leader, .. } = f;
	drop(leader);
	let recovered = rig.restart().await;
	let (_, _, _, records) = served(&recovered);
	assert_eq!(records.len(), 3);
	recovered.reconcile().await;
	assert_eq!(recovered.read(&ckey(b"pending")).await.unwrap(), Some(1));

	// The control: delete by `H`.
	let f = prune_fixture().await;
	store_switches(&f.leader)
		.prune_by_horizon
		.store(true, Ordering::Relaxed);
	compact(&f.leader).await;
	assert_eq!(
		f.leader.fetch_add(f.between, &ckey(b"between"), 1).await,
		applied(1, 2),
		"control: deleting by H reapplies an unexpired add"
	);
	f.clock.set(T0 + expiry() + 1_000);
}

// T-UNKEYED-V7: an unkeyed v7 record younger than E survives compaction and
// still refuses every request under its write key (the F-CTR-9 legacy rule).
// The control deletes unkeyed records whatever their age, and a request under
// the write key is applied as a fresh add.
#[tokio::test]
async fn unkeyed_v7_record_younger_than_e_is_kept_and_conflicts() {
	for prune_unkeyed in [false, true] {
		let clock = TestClock::at(T0 + 1_000);
		let rig = rig_on(&clock);
		model_store(&rig.model);
		let unkeyed = wk_at(T0, 9);
		rig.model.put_durable(
			&applied_log_path(),
			applied_log::encode_unkeyed_frame(&AppliedCounterAdd {
				op_id: 1,
				write_key: unkeyed,
				key: None,
				previous: 0,
				current: 5,
			})
			.unwrap(),
		);
		let leader = rig.restart().await;
		store_switches(&leader)
			.prune_unkeyed
			.store(prune_unkeyed, Ordering::Relaxed);
		let report = compact(&leader).await;
		let key = ckey(b"unkeyed");
		if prune_unkeyed {
			assert_eq!(report.deleted, 1);
			assert_eq!(
				leader.fetch_add(unkeyed, &key, 5).await,
				applied(0, 5),
				"control: an unconditionally deleted unkeyed record lets its write key apply"
			);
		} else {
			assert_eq!((report.kept, report.deleted), (1, 0));
			let (_, _, _, records) = served(&leader);
			assert_eq!(records[0].key, None, "kept unkeyed");
			assert_eq!(
				leader.fetch_add(unkeyed, &key, 5).await,
				Err(CounterLeaderError::WriteKeyConflict)
			);
		}
	}
}

// T-OPID: compacting away the record of the highest operation id and
// restarting, the next operation id is above every earlier one, because the
// generation header carries the high-water. The control writes no high-water,
// and recovery reuses an operation id.
#[tokio::test]
async fn compacted_away_high_op_ids_are_never_reused() {
	for omit_high_water in [false, true] {
		let clock = TestClock::at(T0);
		let rig = rig_on(&clock);
		{
			let leader = rig.leader().await;
			leader
				.fetch_add(wk_at(T0, 1), &ckey(b"a"), 1)
				.await
				.unwrap();
			leader
				.fetch_add(wk_at(T0, 2), &ckey(b"b"), 1)
				.await
				.unwrap();
			assert_eq!(
				leader
					.participant_tentatives(0)
					.iter()
					.map(|(op, _)| *op)
					.max(),
				Some(2)
			);
			store_switches(&leader)
				.omit_high_water
				.store(omit_high_water, Ordering::Relaxed);
			clock.set(T0 + expiry());
			let report = compact(&leader).await;
			assert_eq!((report.kept, report.deleted), (0, 2));
		}
		let leader = rig.restart().await;
		leader
			.fetch_add(wk_at(T0 + expiry(), 3), &ckey(b"c"), 1)
			.await
			.unwrap();
		let next = leader.participant_tentatives(0)[0].0;
		if omit_high_water {
			assert!(next <= 2, "control: op id {next} reused");
		} else {
			assert_eq!(next, 3, "the next op id is above every compacted one");
		}
	}
}

/// Recover a leader on `clock` with `config` as its dedup horizon.
async fn recover_with_config(
	rig: &Rig,
	clock: &Arc<TestClock>,
	config: CounterRetentionConfig,
) -> LeaderHarness {
	rig.model.crash();
	let participants: Vec<_> = rig
		.participants(true)
		.await
		.into_iter()
		.enumerate()
		.map(|(i, participant)| (node_id(i), participant))
		.collect();
	let leader = CounterLeader::recover(
		node_id(0),
		participants[0].1.clone(),
		true,
		model_store(&rig.model),
	)
	.unwrap()
	.with_retention(config, clock.clone());
	LeaderHarness {
		leader: Arc::new(leader),
		participants,
		quorum: rig.quorum,
	}
}

// T-CFG: after compaction deletes a record, raising E and restarting does not
// make its write key presentable: the cutoff is in the generation header. The
// control writes no cutoff, and the add is applied again.
#[tokio::test]
async fn raising_e_after_compaction_never_unexpires_a_deleted_identity() {
	for omit_cutoff in [false, true] {
		let clock = TestClock::at(T0);
		let rig = rig_on(&clock);
		let key = ckey(b"cfg");
		let write_key = wk_at(T0, 1);
		{
			let leader = rig.leader().await;
			leader.fetch_add(write_key, &key, 5).await.unwrap();
			store_switches(&leader)
				.omit_cutoff
				.store(omit_cutoff, Ordering::Relaxed);
			clock.set(T0 + expiry());
			assert_eq!(compact(&leader).await.deleted, 1);
		}
		let raised = CounterRetentionConfig {
			expiry_ms: 2 * expiry(),
			..CounterRetentionConfig::default()
		};
		raised.validate().unwrap();
		let leader = recover_with_config(&rig, &clock, raised).await;
		let retry = leader.fetch_add(write_key, &key, 5).await;
		if omit_cutoff {
			assert_eq!(retry, applied(5, 10), "control: an unpersisted Q reapplies");
		} else {
			assert_eq!(retry, Err(CounterLeaderError::OperationExpired));
			assert_eq!(leader.read(&key).await.unwrap(), Some(5));
		}
	}
}

// T-CLOCK: after compaction, a wall clock stepping back does not make a deleted
// write key presentable: E1 refuses by `Q_mem`. The control has E1 ignore
// `Q_mem`, and the add is applied again.
#[tokio::test]
async fn backwards_clock_after_compaction_never_unexpires_a_deleted_identity() {
	for ignore_cutoff in [false, true] {
		let clock = TestClock::at(T0);
		let rig = rig_on(&clock);
		let leader = rig.leader().await;
		let key = ckey(b"clock");
		let write_key = wk_at(T0, 1);
		leader.fetch_add(write_key, &key, 5).await.unwrap();
		clock.set(T0 + expiry());
		assert_eq!(compact(&leader).await.deleted, 1);
		leader
			.leader
			.retention_switches
			.ignore_cutoff
			.store(ignore_cutoff, Ordering::Relaxed);
		clock.set(T0 + 1);
		let retry = leader.fetch_add(write_key, &key, 5).await;
		if ignore_cutoff {
			assert_eq!(
				retry,
				applied(5, 10),
				"control: without Q the add reapplies"
			);
		} else {
			assert_eq!(retry, Err(CounterLeaderError::OperationExpired));
			assert_eq!(leader.read(&key).await.unwrap(), Some(5));
		}
	}
}

// T-PEND-E: a retry of an add whose witness is still pending past E is refused
// with `OperationExpired` (whose wire answer is INDETERMINATE), and reconcile
// still completes the add, so its delta is counted once.
#[tokio::test]
async fn pending_witness_older_than_e_refuses_retries_and_reconciles_once() {
	let clock = TestClock::at(T0);
	let rig = rig_on(&clock);
	let leader = rig.leader().await;
	let key = ckey(b"pending");
	let write_key = wk_at(T0, 1);
	leader.arm_participant_commit_failure(0);
	assert_eq!(leader.fetch_add(write_key, &key, 5).await, applied(0, 5));
	assert_eq!(leader.pending_witnesses(), 1);
	clock.set(T0 + expiry() + 1);
	assert_eq!(
		leader.fetch_add(write_key, &key, 5).await,
		Err(CounterLeaderError::OperationExpired)
	);
	leader.reconcile().await;
	assert_eq!(leader.pending_witnesses(), 0);
	assert_eq!(leader.read(&key).await.unwrap(), Some(5));
	assert_eq!(leader.participant_read(0, &key).await, Some(5));
	assert_eq!(
		leader.fetch_add(write_key, &key, 5).await,
		Err(CounterLeaderError::OperationExpired)
	);
}
