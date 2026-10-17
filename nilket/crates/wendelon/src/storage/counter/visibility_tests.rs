//! Selected visibility and mixed-format regressions over real local storage.
use super::*;
use crate::storage::core::GroupCommitConfig;
use crate::storage::{BlockingFileOps, StorageEngine};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
struct MarkerFault {
	inner: LocalCounterParticipant,
	blocked: Arc<AtomicBool>,
}
impl MarkerFault {
	fn new(f: &Fixture) -> Self {
		Self {
			inner: LocalCounterParticipant::new(f.service.clone(), tenant(7), node(1)),
			blocked: Arc::new(AtomicBool::new(true)),
		}
	}
}
impl CounterParticipant for MarkerFault {
	async fn replicate_tentative(
		&self,
		op: u64,
		key: &CounterKey,
		value: u64,
		stamp: HybridTimestamp,
	) -> Result<(), ParticipantError> {
		self.inner.replicate_tentative(op, key, value, stamp).await
	}
	async fn commit(&self, op: u64, key: &CounterKey) -> Result<(), ParticipantError> {
		if self.blocked.load(Ordering::SeqCst) {
			Err(ParticipantError::Unavailable)
		} else {
			self.inner.commit(op, key).await
		}
	}
	async fn rollback(&self, op: u64, key: &CounterKey) {
		self.inner.rollback(op, key).await;
	}
	fn eligibility_status(&self, key: Identity) -> Result<(), CounterVisibilityError> {
		self.inner.eligibility_status(key)
	}
	async fn read_eligible(
		&self,
		key: Identity,
	) -> Result<Option<CounterVersion>, ParticipantError> {
		self.inner.read_eligible(key).await
	}
	async fn read_committed(&self, key: &CounterKey) -> Result<Option<u64>, ParticipantError> {
		self.inner.read_committed(key).await
	}
}
fn plan(p: &MarkerFault) -> CounterParticipantPlan<MarkerFault> {
	CounterParticipantPlan::new(vec![(node(1), p.clone())], 1)
}
fn witness_store(dir: &TestDir) -> DurableCounterWitnessStore {
	DurableCounterWitnessStore::open(dir.to_path_buf()).unwrap()
}
fn stored(op: u64, value: u64, origin: Option<NodeId>) -> StoredWitness {
	StoredWitness {
		origin,
		body: CounterFinalizationWitness {
			op_id: op,
			write_key: [7; 24],
			key: CounterKey(vec![9]),
			value,
			previous: 3,
			commit_hlc: hlc(op),
			accepted: vec![node(2)],
			committed: HashSet::new(),
		},
	}
}
fn visibility(error: std::io::Error) -> CounterVisibilityError {
	*error
		.get_ref()
		.unwrap()
		.downcast_ref::<CounterVisibilityError>()
		.expect("typed visibility refusal")
}

#[tokio::test]
async fn ctr4_second_add_uses_first_unresolved_witness() {
	let f = Fixture::new("ctr4-second-witness").await;
	let dir = TestDir::new("ctr4-second-witness-log");
	let p = MarkerFault::new(&f);
	let leader = CounterLeader::new(node(1), p.clone(), true, witness_store(&dir));
	let key = CounterKey(b"sum".to_vec());
	assert_eq!(
		leader.fetch_add(plan(&p), [1; 24], &key, 5).await,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 5
		}))
	);
	assert_eq!(
		leader.fetch_add(plan(&p), [2; 24], &key, 7).await,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 5,
			current: 12
		}))
	);
	assert_eq!(leader.pending_witnesses(), 2);
	assert_eq!(leader.read(&key).await, Ok(Some(12)));
	assert_eq!(
		f.service
			.read_committed(tenant(7), counter_identity(&key))
			.await
			.unwrap(),
		None
	);
	p.blocked.store(false, Ordering::SeqCst);
	leader
		.reconcile(&StaticCounterParticipantResolver::new(vec![(
			node(1),
			p.clone(),
		)]))
		.await;
	assert_eq!(leader.pending_witnesses(), 0);
	assert_eq!(
		f.service
			.read_committed(tenant(7), counter_identity(&key))
			.await
			.unwrap(),
		Some(12)
	);
	drop(leader);
	drop(p);
	let f = f.reopened().await;
	let p = MarkerFault::new(&f);
	let leader = CounterLeader::recover(node(1), p.clone(), true, witness_store(&dir)).unwrap();
	assert_eq!(leader.read(&key).await, Ok(Some(12)));
	p.blocked.store(false, Ordering::SeqCst);
	for step in 1..=64 {
		let mut write_key = [0; 24];
		write_key[..8].copy_from_slice(&(step as u64).to_le_bytes());
		write_key[23] = 99;
		assert_eq!(
			leader.fetch_add(plan(&p), write_key, &key, 1).await,
			Ok(CounterAddResult::Applied(CounterAddOutcome {
				previous: 11 + step,
				current: 12 + step
			}))
		);
		assert_eq!(leader.witnessed.seen_len(&counter_identity(&key)), 1);
		assert_eq!(
			f.service
				.marked
				.seen_len(&(tenant(7), counter_identity(&key))),
			1
		);
	}
}

#[tokio::test]
async fn ctr4_orphan_live_value_never_becomes_base() {
	let f = Fixture::new("ctr4-orphan-base").await;
	let key = CounterKey(b"orphan".to_vec());
	let id = counter_identity(&key);
	f.service
		.replicate_tentative(node(2), 100, tenant(7), id, 99, hlc(100))
		.await
		.unwrap();
	let dir = TestDir::new("ctr4-orphan-witness");
	let p = MarkerFault::new(&f);
	p.blocked.store(false, Ordering::SeqCst);
	let leader = CounterLeader::new(node(1), p.clone(), true, witness_store(&dir));
	assert_eq!(
		leader.fetch_add(plan(&p), [1; 24], &key, 3).await,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 3
		}))
	);
	assert_eq!(leader.read(&key).await, Ok(Some(3)));
	let raw = f.slab.counter_lookup(tenant(7), id).unwrap();
	assert_eq!(
		f.slab.counter_read_value(raw).await.unwrap(),
		99,
		"generic raw view remains qualified"
	);
	f.service
		.rollback(node(2), 100, tenant(7), id)
		.await
		.unwrap();
	assert_eq!(leader.read(&key).await, Ok(Some(3)));
}

#[tokio::test]
async fn ctr4_newer_mark_beats_stale_pending_witness() {
	let f = Fixture::new("ctr4-stale-witness").await;
	let dir = TestDir::new("ctr4-stale-witness-log");
	let p = MarkerFault::new(&f);
	let leader = CounterLeader::new(node(1), p.clone(), true, witness_store(&dir));
	let key = CounterKey(b"stale".to_vec());
	assert_eq!(
		leader.fetch_add(plan(&p), [1; 24], &key, 5).await,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 5
		}))
	);
	p.blocked.store(false, Ordering::SeqCst);
	assert_eq!(
		leader.fetch_add(plan(&p), [2; 24], &key, 7).await,
		Ok(CounterAddResult::Applied(CounterAddOutcome {
			previous: 5,
			current: 12
		}))
	);
	assert_eq!(leader.pending_witnesses(), 1);
	assert_eq!(leader.read(&key).await, Ok(Some(12)));
	drop(leader);
	drop(p);
	let f = f.reopened().await;
	let p = MarkerFault::new(&f);
	p.blocked.store(false, Ordering::SeqCst);
	let leader = CounterLeader::recover(node(1), p.clone(), true, witness_store(&dir)).unwrap();
	assert_eq!(leader.pending_witnesses(), 1);
	assert_eq!(leader.read(&key).await, Ok(Some(12)));
	leader
		.reconcile(&StaticCounterParticipantResolver::new(vec![(node(1), p)]))
		.await;
	assert_eq!(leader.pending_witnesses(), 0);
	assert_eq!(leader.read(&key).await, Ok(Some(12)));
}

#[tokio::test]
async fn ctr4_rollback_preserves_newer_eligible_cell() {
	let f = Fixture::new("ctr4-rollback").await;
	let id = identity(b"rollback");
	f.service
		.replicate_tentative(node(1), 1, tenant(7), id, 5, hlc(1))
		.await
		.unwrap();
	f.service
		.replicate_tentative(node(1), 2, tenant(7), id, 12, hlc(2))
		.await
		.unwrap();
	f.service.commit(node(1), 2, tenant(7), id).await.unwrap();
	f.service.rollback(node(1), 1, tenant(7), id).await.unwrap();
	assert_eq!(
		f.service.read_committed(tenant(7), id).await.unwrap(),
		Some(12)
	);
	let f = f.reopened().await;
	assert_eq!(
		f.service.read_committed(tenant(7), id).await.unwrap(),
		Some(12)
	);
}

#[tokio::test]
async fn ctr4_local_reopen_rebuilds_marked_and_pending_witness_winner() {
	let f = Fixture::new("ctr4-rebuild").await;
	let id = counter_identity(&CounterKey(vec![9]));
	f.service
		.replicate_tentative(node(1), 1, tenant(7), id, 10, hlc(1))
		.await
		.unwrap();
	f.service.commit(node(1), 1, tenant(7), id).await.unwrap();
	f.service
		.replicate_tentative(node(1), 3, tenant(7), id, 99, hlc(3))
		.await
		.unwrap();
	let dir = TestDir::new("ctr4-rebuild-witness");
	witness_store(&dir)
		.write(&stored(2, 20, Some(node(1))))
		.unwrap();
	let f = f.reopened().await;
	assert_eq!(
		f.service.read_committed(tenant(7), id).await.unwrap(),
		Some(10)
	);
	let p = MarkerFault::new(&f);
	let leader = CounterLeader::recover(node(1), p, true, witness_store(&dir)).unwrap();
	assert_eq!(leader.read(&CounterKey(vec![9])).await, Ok(Some(20)));
	assert_eq!(leader.pending_witnesses(), 1);
	assert_eq!(
		f.slab
			.counter_read_value(f.slab.counter_lookup(tenant(7), id).unwrap())
			.await
			.unwrap(),
		10,
		"cold raw index excludes unmarked99"
	);
}

#[test]
fn ctr4_equal_hlc_conflict_fails_closed() {
	let a = CounterVersion {
		value: 11,
		hlc: hlc(1),
		origin: Some(node(1)),
	};
	let b = CounterVersion {
		value: 22,
		origin: Some(node(2)),
		..a
	};
	assert_eq!(winner(Some(a), Some(b)), Ok(Some(b)));
	assert_eq!(winner(Some(b), Some(a)), Ok(Some(b)));
	assert_eq!(
		winner(Some(a), Some(CounterVersion { value: 12, ..a })),
		Err(CounterVisibilityError::Conflict)
	);
	let legacy = CounterVersion { origin: None, ..a };
	assert_eq!(winner(Some(legacy), Some(a)), Ok(Some(a)));
	assert_eq!(
		winner(Some(legacy), Some(b)),
		Err(CounterVisibilityError::Conflict)
	);
	// Sequential publication retains only the current stamp, not operation history.
	let bounded = EligibleCells::<u8>::default();
	for op in 1..=1024 {
		bounded
			.prepare(
				&1,
				CounterVersion {
					value: op,
					hlc: hlc(op),
					origin: Some(node(1)),
				},
			)
			.unwrap()
			.publish()
			.unwrap();
		assert_eq!(bounded.seen_len(&1), 1);
	}
	// An older outstanding reservation survives pruning; dropping one of two
	// identical reservations cannot remove the other's collision protection.
	let old = CounterVersion {
		value: 1,
		hlc: hlc(1),
		origin: Some(node(1)),
	};
	let first = bounded.prepare(&1, old).unwrap();
	let second = bounded.prepare(&1, old).unwrap();
	assert_eq!(bounded.seen_len(&1), 2);
	drop(first);
	assert_eq!(bounded.seen_len(&1), 2);
	drop(second);
	assert_eq!(bounded.seen_len(&1), 1);
	// Exercise an actual pre-publication witness-write failure, then unwind
	// its reservation without publishing or retaining a phantom entry.
	let dir = TestDir::new("ctr4-reservation-write-failure");
	let store = witness_store(&dir);
	std::fs::create_dir(store.origin_path_for(999)).unwrap();
	{
		let pending = bounded
			.prepare(
				&2,
				CounterVersion {
					value: 11,
					hlc: hlc(999),
					origin: Some(node(1)),
				},
			)
			.unwrap();
		assert_eq!(bounded.seen_len(&2), 1);
		assert!(store.write(&stored(999, 11, Some(node(1)))).is_err());
		drop(pending);
	}
	assert_eq!(bounded.seen_len(&2), 0);
	assert_eq!(bounded.read(&2), Ok(None));
	let cells = EligibleCells::<u8>::default();
	let reserved = cells.prepare(&1, a).unwrap();
	match cells.prepare(&1, CounterVersion { value: 12, ..a }) {
		Err(e) => assert_eq!(e, CounterVisibilityError::Conflict),
		Ok(_) => panic!("same identity conflict accepted"),
	}
	assert_eq!(cells.read(&1), Err(CounterVisibilityError::Conflict));
	assert_eq!(reserved.publish(), Err(CounterVisibilityError::Poisoned));
	assert_eq!(cells.read(&1), Err(CounterVisibilityError::Poisoned));
}

#[tokio::test]
async fn ctr4_absent_and_unsupported_inputs_are_distinct() {
	let f = Fixture::new("ctr4-unsupported").await;
	let id = identity(b"unsupported");
	assert_eq!(f.service.read_committed(tenant(7), id).await.unwrap(), None);
	f.slab
		.append(tenant(7), id, hlc(1), &11u64.to_le_bytes())
		.await
		.unwrap();
	assert_eq!(
		visibility(f.service.read_committed(tenant(7), id).await.unwrap_err()),
		CounterVisibilityError::Unsupported
	);
	let f = f.reopened().await;
	assert_eq!(
		visibility(f.service.read_committed(tenant(7), id).await.unwrap_err()),
		CounterVisibilityError::Unsupported
	);
	assert_eq!(
		f.service
			.read_committed(tenant(7), identity(b"absent"))
			.await
			.unwrap(),
		None
	);
}

async fn local(path: &std::path::Path) -> Arc<LocalSlabStore> {
	Arc::new(
		LocalSlabStore::open(
			path,
			crate::schema::SlabId::ZERO,
			Arc::new(BlockingFileOps),
			GroupCommitConfig {
				floor: std::time::Duration::ZERO,
				ceiling: std::time::Duration::ZERO,
				per_write_cost_estimate: std::time::Duration::ZERO,
			},
		)
		.await
		.unwrap(),
	)
}
#[test]
fn ctr4_origin_record_bytes_and_independent_goldens() {
	let value = 0x0102030405060708;
	let origin = node(0x0a0b0c0d);
	assert_eq!(
		encode_origin_frame(value, origin).unwrap(),
		[8, 7, 6, 5, 4, 3, 2, 1, 13, 12, 11, 10, 0, 0, 0, 0]
	);
	assert_eq!(
		encode_origin_frame(0x1112131415161718, origin).unwrap(),
		[24, 23, 22, 21, 20, 19, 18, 17, 13, 12, 11, 10, 0, 0, 0, 0]
	);
	assert_eq!(
		decode_origin_frame(&[8, 7, 6, 5, 4, 3, 2, 1, 13, 12, 11, 10, 0, 0, 0, 0]).unwrap(),
		(value, origin)
	);
	for len in [0, 8, 15, 17] {
		assert_eq!(
			decode_origin_frame(&vec![0; len])
				.unwrap_err()
				.get_ref()
				.unwrap()
				.downcast_ref::<CounterFormatError>(),
			Some(&CounterFormatError::InvalidOriginFrame)
		);
	}
	for bytes in [[0; 16], {
		let mut b = [0; 16];
		b[8] = 1;
		b[12] = 1;
		b
	}] {
		assert_eq!(
			decode_origin_frame(&bytes)
				.unwrap_err()
				.get_ref()
				.unwrap()
				.downcast_ref::<CounterFormatError>(),
			Some(&CounterFormatError::InvalidOriginFrame)
		);
	}
}
#[tokio::test]
async fn ctr4_mixed_legacy_origin_records_pair_and_reopen() {
	let dir = TestDir::new("ctr4-mixed");
	let path = dir.join("mixed.slab");
	let s = local(&path).await;
	let old = identity(b"old");
	let new = identity(b"new");
	let a = s
		.write_framed(
			tenant(7),
			old,
			hlc(1),
			record_class::COUNTER_TENTATIVE,
			&11u64.to_le_bytes(),
		)
		.await
		.unwrap();
	s.write_framed(
		tenant(7),
		old,
		hlc(1),
		record_class::COUNTER_COMMIT,
		&a.offset.to_le_bytes(),
	)
	.await
	.unwrap();
	let b = s
		.counter_tentative(tenant(7), new, hlc(2), 22, node(2))
		.await
		.unwrap();
	s.counter_commit_marker(tenant(7), new, hlc(2), b.offset, node(2))
		.await
		.unwrap();
	for (tag, marker, origins) in [
		(
			record_class::COUNTER_TENTATIVE,
			record_class::COUNTER_COMMIT_V2,
			(None, Some(node(1))),
		),
		(
			record_class::COUNTER_TENTATIVE_V2,
			record_class::COUNTER_COMMIT,
			(Some(node(1)), None),
		),
		(
			record_class::COUNTER_TENTATIVE_V2,
			record_class::COUNTER_COMMIT_V2,
			(Some(node(1)), Some(node(2))),
		),
	] {
		let id = identity(&[tag as u8, marker as u8]);
		let frame = match origins.0 {
			None => 33u64.to_le_bytes().to_vec(),
			Some(o) => encode_origin_frame(33, o).unwrap().to_vec(),
		};
		let t = s
			.write_framed(tenant(7), id, hlc(3), tag, &frame)
			.await
			.unwrap();
		let marker_frame = match origins.1 {
			None => t.offset.to_le_bytes().to_vec(),
			Some(o) => encode_origin_frame(t.offset, o).unwrap().to_vec(),
		};
		s.write_framed(tenant(7), id, hlc(3), marker, &marker_frame)
			.await
			.unwrap();
	}
	let standalone_orphan = CounterKey(b"standalone-orphan-marker".to_vec());
	s.counter_commit_marker(
		tenant(7),
		counter_identity(&standalone_orphan),
		hlc(4),
		0,
		node(3),
	)
	.await
	.unwrap();
	s.fsync().await.unwrap();
	drop(s);
	let s = local(&path).await;
	assert_eq!(s.recovered_counter_versions.len(), 2);
	assert!(s.recovered_counter_versions.contains(&(
		tenant(7),
		old,
		CounterVersion {
			value: 11,
			hlc: hlc(1),
			origin: None
		}
	)));
	assert!(s.recovered_counter_versions.contains(&(
		tenant(7),
		new,
		CounterVersion {
			value: 22,
			hlc: hlc(2),
			origin: Some(node(2))
		}
	)));
	assert_eq!(s.recovered_counter_errors.len(), 3);
	assert!(
		s.recovered_counter_errors
			.iter()
			.all(|(_, _, e)| *e == CounterVisibilityError::Conflict)
	);
	let standalone = SlabCounterStore::new(s.clone(), tenant(7), node(3));
	assert!(
		standalone.read_committed(&standalone_orphan).await.is_err(),
		"a standalone slab has no cross-slab join to resolve an orphan marker"
	);
}
#[test]
fn ctr4_origin_witness_legacy_decode_and_versioned_roundtrip() {
	let dir = TestDir::new("ctr4-witness-codecs");
	let store = witness_store(&dir);
	let w = stored(1, 11, Some(node(1)));
	let golden="4354525749543032020000006d0000000100000000000000010000000000000007070707070707070707070707070707070707070707070701000000000000000b0000000000000003000000000000000100000000000000000000000000000001000000000000000902000000".as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect::<Vec<_>>();
	assert_eq!(witness::encode(&w).unwrap(), golden);
	let decoded = witness::decode(&golden).unwrap();
	assert_eq!(decoded.origin, Some(node(1)));
	assert_eq!(decoded.value, 11);
	assert_eq!(decoded.key, CounterKey(vec![9]));
	store.write(&w).unwrap();
	let loaded = store.load_all().unwrap();
	assert_eq!(loaded.len(), 1);
	assert_eq!(loaded[0].origin, Some(node(1)));
	store.remove(1).unwrap();
	let mut legacy = stored(1, 11, None);
	let old = postcard::to_allocvec(&legacy.body).unwrap();
	std::fs::write(store.path_for(1), &old).unwrap();
	let mut loaded = store.load_all().unwrap().remove(0);
	assert_eq!(loaded.origin, None);
	loaded.committed.insert(node(2));
	store.write(&loaded).unwrap();
	assert!(!store.origin_path_for(1).exists());
	assert_eq!(
		postcard::from_bytes::<CounterFinalizationWitness>(
			&std::fs::read(store.path_for(1)).unwrap()
		)
		.unwrap()
		.committed,
		HashSet::from([node(2)])
	);
	legacy.committed.insert(node(2));
	assert_eq!(
		postcard::to_allocvec(&legacy.body).unwrap(),
		std::fs::read(store.path_for(1)).unwrap()
	);
	std::fs::write(store.origin_path_for(1), &golden).unwrap();
	assert_eq!(
		visibility(store.load_all().err().expect("cross generation conflict")),
		CounterVisibilityError::Conflict
	);
	for offset in [10usize, 20, 60, 16] {
		let mut bad = golden.clone();
		if offset == 16 {
			bad[16..20].fill(0)
		} else {
			bad[offset] = 1
		};
		assert_eq!(
			witness::decode(&bad)
				.err()
				.expect("malformed witness")
				.get_ref()
				.unwrap()
				.downcast_ref::<CounterFormatError>(),
			Some(&CounterFormatError::InvalidOriginFrame)
		);
	}
}
