//! Scoped live replica tests; no epoch/incarnation fence or orphan-read fix is claimed.
use super::*;
use crate::crypto::CryptoManager;
use crate::storage::io_uring::spawn_io_thread;
use crate::storage::manager::{SlabManager, SlabManagerConfig};
use crate::test_support::TestDir;
#[path = "scope_assertions.rs"]
mod scope_assertions;
use scope_assertions::{Scope, ScopeAdapter};

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}
fn tenant(n: u64) -> TenantId {
	TenantId::new(n)
}
fn hlc(n: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: n,
		logical: 0,
		node_id: 0,
	}
}
fn identity(key: &[u8]) -> Identity {
	counter_identity(&CounterKey(key.to_vec()))
}
async fn manager(dir: &TestDir, crypto: Arc<CryptoManager>) -> Arc<SlabManager> {
	Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.to_path_buf(),
			max_slab_bytes: 1024 * 1024,
			io: spawn_io_thread(64).unwrap(),
			l2: None,
			rehydration: Default::default(),
			l2_column_fetch: Default::default(),
			row_group_writer: Default::default(),
			l2_row_group_fetch: Default::default(),
			encoding_writer: Default::default(),
			l2_chunk_layout: Default::default(),
			l2_encoding_fetch: Default::default(),
			encoding_compute: Default::default(),
			group_commit: Default::default(),
			crypto,
		})
		.await
		.unwrap(),
	)
}
struct Fixture {
	dir: TestDir,
	crypto: Arc<CryptoManager>,
	slab: Arc<SlabManager>,
	service: Arc<CounterReplicaService>,
}
impl Fixture {
	async fn new(tag: &str) -> Self {
		let dir = TestDir::new(tag);
		let crypto = Arc::new(CryptoManager::new_dev());
		for t in [7, 8] {
			crypto.install_local_only_for_tests(tenant(t)).unwrap();
		}
		let slab = manager(&dir, crypto.clone()).await;
		let service = Arc::new(CounterReplicaService::new(slab.clone()));
		Self {
			dir,
			crypto,
			slab,
			service,
		}
	}
	async fn reopened(self) -> Self {
		self.slab.fsync_all_local_slabs().await.unwrap();
		let Self {
			dir,
			crypto,
			slab,
			service,
		} = self;
		drop(service);
		drop(slab);
		let slab = manager(&dir, crypto.clone()).await;
		let service = Arc::new(CounterReplicaService::new(slab.clone()));
		Self {
			dir,
			crypto,
			slab,
			service,
		}
	}
}
impl ScopeAdapter for Fixture {
	async fn tentative(&self, s: Scope, op: u64, key: &[u8], value: u64, stamp: u64) {
		self.service
			.replicate_tentative(
				node(s.origin),
				op,
				tenant(s.tenant),
				identity(key),
				value,
				hlc(stamp),
			)
			.await
			.unwrap();
	}
	async fn commit(&self, s: Scope, op: u64, key: &[u8]) {
		self.service
			.commit(node(s.origin), op, tenant(s.tenant), identity(key))
			.await
			.unwrap();
	}
	async fn rollback(&self, s: Scope, op: u64, key: &[u8]) {
		self.service
			.rollback(node(s.origin), op, tenant(s.tenant), identity(key))
			.await
			.unwrap();
	}
	async fn read(&self, s: Scope, key: &[u8]) -> Option<u64> {
		// Raw pending-identity evidence, deliberately separate from committed visibility.
		match self.slab.counter_lookup(tenant(s.tenant), identity(key)) {
			Some(location) => Some(self.slab.counter_read_value(location).await.unwrap()),
			None => None,
		}
	}
}
#[tokio::test]
async fn b1d_two_tenants_same_op_id_are_independent() {
	let f = Fixture::new("counter-scope-tenants").await;
	scope_assertions::two_tenants(&f).await;
	let f = f.reopened().await;
	assert_eq!(
		f.read(
			Scope {
				tenant: 7,
				origin: 1
			},
			b"scope-a"
		)
		.await,
		Some(11)
	);
	assert_eq!(
		f.read(
			Scope {
				tenant: 8,
				origin: 1
			},
			b"scope-b"
		)
		.await,
		Some(44)
	);
}
#[tokio::test]
async fn b1d_two_origins_same_op_id_are_independent() {
	let f = Fixture::new("counter-scope-origins").await;
	scope_assertions::two_origins(&f).await;
	let f = f.reopened().await;
	assert_eq!(
		f.read(
			Scope {
				tenant: 7,
				origin: 1
			},
			b"scope-a"
		)
		.await,
		Some(11)
	);
	assert_eq!(
		f.read(
			Scope {
				tenant: 7,
				origin: 2
			},
			b"scope-b"
		)
		.await,
		Some(44)
	);
}
fn zero(error: std::io::Error) {
	assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
	assert!(matches!(
		counter_replica_rejection2(&error),
		Some(CounterReplicaRejection::ZeroExpectedKey)
	));
}
fn conflict(error: std::io::Error) {
	assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
	assert!(matches!(
		counter_replica_rejection2(&error),
		Some(CounterReplicaRejection::TentativeConflict)
	));
}
#[tokio::test]
async fn b1d_pending_live_identical_retry_and_key_value_hlc_conflicts_are_atomic() {
	let f = Fixture::new("counter-scope-content").await;
	let s = &f.service;
	let key = identity(b"original");
	s.replicate_tentative(node(1), 1, tenant(7), key, 11, hlc(1))
		.await
		.unwrap();
	let before = f.slab.local_l1_bytes().await;
	let location = f.slab.counter_lookup(tenant(7), key).unwrap();
	s.replicate_tentative(node(1), 1, tenant(7), key, 11, hlc(1))
		.await
		.unwrap();
	assert_eq!(f.slab.local_l1_bytes().await, before);
	assert_eq!(s.pending_count(), 1);
	for (other, value, stamp) in [
		(identity(b"other"), 11, hlc(1)),
		(key, 12, hlc(1)),
		(key, 11, hlc(2)),
	] {
		conflict(
			s.replicate_tentative(node(1), 1, tenant(7), other, value, stamp)
				.await
				.unwrap_err(),
		);
		assert_eq!(f.slab.local_l1_bytes().await, before);
		assert_eq!(f.slab.counter_lookup(tenant(7), key), Some(location));
		assert_eq!(s.pending_count(), 1);
		assert_eq!(s.read_committed(tenant(7), key).await.unwrap(), None);
		assert_eq!(
			f.slab
				.counter_read_value(f.slab.counter_lookup(tenant(7), key).unwrap())
				.await
				.unwrap(),
			11
		);
	}
	assert_eq!(
		s.read_committed(tenant(7), identity(b"other"))
			.await
			.unwrap(),
		None
	);
}
#[tokio::test]
async fn b1d_commit_rollback_zero_before_missing_then_mismatch_preserve_tentative() {
	let f = Fixture::new("counter-scope-key-gate").await;
	let s = &f.service;
	let key = identity(b"original");
	let absent = identity(b"absent");
	let empty = Identity::new([0; 32]);
	let before = f.slab.local_l1_bytes().await;
	zero(s.commit(node(1), 1, tenant(7), empty).await.unwrap_err());
	zero(s.rollback(node(1), 1, tenant(7), empty).await.unwrap_err());
	s.commit(node(1), 1, tenant(7), absent).await.unwrap();
	s.rollback(node(1), 1, tenant(7), absent).await.unwrap();
	assert_eq!(f.slab.local_l1_bytes().await, before);
	s.replicate_tentative(node(1), 1, tenant(7), key, 11, hlc(1))
		.await
		.unwrap();
	let before = f.slab.local_l1_bytes().await;
	let location = f.slab.counter_lookup(tenant(7), key).unwrap();
	for rollback in [false, true] {
		let e = if rollback {
			s.rollback(node(1), 1, tenant(7), empty).await.unwrap_err()
		} else {
			s.commit(node(1), 1, tenant(7), empty).await.unwrap_err()
		};
		zero(e);
		let e = if rollback {
			s.rollback(node(1), 1, tenant(7), absent).await.unwrap_err()
		} else {
			s.commit(node(1), 1, tenant(7), absent).await.unwrap_err()
		};
		conflict(e);
		assert_eq!(f.slab.local_l1_bytes().await, before);
		assert_eq!(f.slab.counter_lookup(tenant(7), key), Some(location));
		assert_eq!(s.pending_count(), 1);
	}
	s.commit(node(1), 1, tenant(7), key).await.unwrap();
	assert_eq!(s.pending_count(), 0);
}
#[tokio::test]
async fn b1d_postcommit_redrive_reappends_absolute_value_at_original_hlc() {
	let f = Fixture::new("counter-scope-redrive").await;
	let s = &f.service;
	let key = identity(b"original");
	s.replicate_tentative(node(1), 1, tenant(7), key, 11, hlc(1))
		.await
		.unwrap();
	s.commit(node(1), 1, tenant(7), key).await.unwrap();
	let before = f.slab.local_l1_bytes().await;
	s.replicate_tentative(node(1), 1, tenant(7), key, 11, hlc(1))
		.await
		.unwrap();
	assert!(f.slab.local_l1_bytes().await > before);
	assert_eq!(s.pending_count(), 1);
	assert_eq!(f.slab.counter_lookup(tenant(7), key).unwrap().hlc, hlc(1));
	assert_eq!(s.read_committed(tenant(7), key).await.unwrap(), Some(11));
	s.commit(node(1), 1, tenant(7), key).await.unwrap();
	let f = f.reopened().await;
	assert_eq!(
		f.service.read_committed(tenant(7), key).await.unwrap(),
		Some(11)
	);
}
async fn blocked<F: std::future::Future>(future: std::pin::Pin<&mut F>) {
	let mut future = future;
	std::future::poll_fn(|cx| {
		assert!(
			future.as_mut().poll(cx).is_pending(),
			"gate waiter must be polled while its predecessor holds the mutex"
		);
		std::task::Poll::Ready(())
	})
	.await;
}
#[tokio::test]
async fn b1d_gate_waiter_and_new_caller_retain_same_arc_and_serialize() {
	let f = Fixture::new("counter-scope-gate-waiter").await;
	f.service
		.replicate_tentative(node(1), 1, tenant(7), identity(b"gate"), 11, hlc(1))
		.await
		.unwrap();
	let key = ReplicaOpKey2::new(node(1), tenant(7), 1).unwrap();
	let a = f.service.op_locks.entry(key).or_default().clone();
	let a_guard = a.lock().await;
	let w = f.service.op_locks.entry(key).or_default().clone();
	let mut w_wait = Box::pin(w.lock());
	blocked(w_wait.as_mut()).await;
	assert_eq!(Arc::strong_count(&a), 3);
	// Exercise the pending-removing phase under A's held gate, without a runtime hook.
	let (_, removed) = f.service.pending.remove(&key).unwrap();
	f.slab
		.counter_unindex_tentative(removed.tenant_id, removed.key_identity, removed.location);
	assert!(!f.service.pending.contains_key(&key));
	f.service.reclaim_gate2(key, &a);
	assert!(f.service.op_locks.contains_key(&key));
	let n = f.service.op_locks.entry(key).or_default().clone();
	assert!(Arc::ptr_eq(&a, &w));
	assert!(Arc::ptr_eq(&w, &n));
	let mut n_wait = Box::pin(n.lock());
	blocked(n_wait.as_mut()).await;
	drop(a_guard);
	drop(a);
	let w_guard = w_wait.await;
	blocked(n_wait.as_mut()).await;
	f.service.reclaim_gate2(key, &w);
	assert!(f.service.op_locks.contains_key(&key));
	drop(w_guard);
	drop(w);
	let n_guard = n_wait.await;
	f.service.reclaim_gate2(key, &n);
	assert!(!f.service.op_locks.contains_key(&key));
	drop(n_guard);
}
#[tokio::test]
async fn b1d_gate_without_waiters_is_reclaimed_and_next_call_gets_fresh_arc() {
	let f = Fixture::new("counter-scope-gate-empty").await;
	let key = ReplicaOpKey2::new(node(1), tenant(7), 1).unwrap();
	let old = f.service.op_locks.entry(key).or_default().clone();
	let guard = old.lock().await;
	assert_eq!(Arc::strong_count(&old), 2);
	f.service.reclaim_gate2(key, &old);
	assert!(!f.service.op_locks.contains_key(&key));
	drop(guard);
	let new = f.service.op_locks.entry(key).or_default().clone();
	assert!(!Arc::ptr_eq(&old, &new));
	let guard = new.lock().await;
	f.service.reclaim_gate2(key, &new);
	drop(guard);
}
#[tokio::test]
async fn b1d_distinct_tenant_origin_and_op_gates_do_not_share_mutex() {
	let f = Fixture::new("counter-scope-gate-distinct").await;
	let a = ReplicaOpKey2::new(node(1), tenant(7), 1).unwrap();
	let gate = f.service.op_locks.entry(a).or_default().clone();
	let guard = gate.lock().await;
	for key in [
		ReplicaOpKey2::new(node(1), tenant(8), 1).unwrap(),
		ReplicaOpKey2::new(node(2), tenant(7), 1).unwrap(),
		ReplicaOpKey2::new(node(1), tenant(7), 2).unwrap(),
	] {
		let other = f.service.op_locks.entry(key).or_default().clone();
		assert!(!Arc::ptr_eq(&gate, &other));
		let other_guard = other.try_lock().expect("distinct scoped gate is unlocked");
		f.service.reclaim_gate2(key, &other);
		drop(other_guard);
	}
	f.service.reclaim_gate2(a, &gate);
	drop(guard);
}
#[tokio::test]
async fn b1d_service_tentative_waits_for_same_scope_gate_and_rechecks_pending() {
	let f = Fixture::new("counter-scope-gate-recheck").await;
	let key = ReplicaOpKey2::new(node(1), tenant(7), 1).unwrap();
	let gate = f.service.op_locks.entry(key).or_default().clone();
	let guard = gate.lock().await;
	let id = identity(b"pending");
	let mut tentative =
		Box::pin(
			f.service
				.replicate_tentative(node(1), 1, tenant(7), id, 11, hlc(1)),
		);
	blocked(tentative.as_mut()).await;
	assert_eq!(f.service.pending_count(), 0);
	drop(guard);
	tentative.await.unwrap();
	drop(gate);
	let before = f.slab.local_l1_bytes().await;
	let gate = f.service.op_locks.entry(key).or_default().clone();
	let guard = gate.lock().await;
	let mut commit = Box::pin(f.service.commit(node(1), 1, tenant(7), id));
	blocked(commit.as_mut()).await;
	assert_eq!(f.service.pending_count(), 1);
	assert_eq!(f.slab.local_l1_bytes().await, before);
	drop(guard);
	commit.await.unwrap();
	assert_eq!(f.service.pending_count(), 0);
	assert_eq!(
		f.service.read_committed(tenant(7), id).await.unwrap(),
		Some(11)
	);
}
#[derive(Clone, Debug)]
struct Sent {
	peer: NodeId,
	tenant: TenantId,
	key: Identity,
	op: u64,
	value: u64,
	hlc: HybridTimestamp,
	epoch: u64,
	phase: Option<CounterReplicaPhase>,
}
struct Bridge {
	service: Arc<CounterReplicaService>,
	origin: NodeId,
	sent: std::sync::Mutex<Vec<Sent>>,
	fail_commit: std::sync::atomic::AtomicBool,
}
impl CounterReplicaTransport for Bridge {
	fn replicate(
		&self,
		peer: NodeId,
		tenant: TenantId,
		key: Identity,
		op: u64,
		value: u64,
		hlc: HybridTimestamp,
		epoch: u64,
		phase: CounterReplicaPhase,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ()>> + Send + '_>> {
		Box::pin(async move {
			self.sent.lock().unwrap().push(Sent {
				peer,
				tenant,
				key,
				op,
				value,
				hlc,
				epoch,
				phase: Some(phase),
			});
			if phase == CounterReplicaPhase::TENTATIVE {
				self.service
					.replicate_tentative(self.origin, op, tenant, key, value, hlc)
					.await
					.map_err(|_| ())
			} else if self.fail_commit.load(std::sync::atomic::Ordering::SeqCst) {
				Err(())
			} else {
				self.service
					.commit(self.origin, op, tenant, key)
					.await
					.map_err(|_| ())
			}
		})
	}
	fn rollback(
		&self,
		peer: NodeId,
		tenant: TenantId,
		key: Identity,
		op: u64,
		epoch: u64,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			self.sent.lock().unwrap().push(Sent {
				peer,
				tenant,
				key,
				op,
				value: 0,
				hlc: hlc(0),
				epoch,
				phase: None,
			});
			let _ = self.service.rollback(self.origin, op, tenant, key).await;
		})
	}
}
#[tokio::test]
async fn b1d_real_local_remote_enum_and_witness_redrive_carry_counter_key() {
	let local = Fixture::new("counter-scope-local-adapter").await;
	let remote = Fixture::new("counter-scope-remote-adapter").await;
	let original = identity(b"unrelated");
	local
		.service
		.replicate_tentative(node(1), 1, tenant(7), original, 99, hlc(1))
		.await
		.unwrap();
	let bridge = Arc::new(Bridge {
		service: remote.service.clone(),
		origin: node(1),
		sent: Default::default(),
		fail_commit: std::sync::atomic::AtomicBool::new(true),
	});
	let lp = CounterParticipantKind::Local(LocalCounterParticipant::new(
		local.service.clone(),
		tenant(7),
		node(1),
	));
	let rp = CounterParticipantKind::Remote(RemoteCounterParticipant::new(
		bridge.clone(),
		node(2),
		tenant(7),
		19,
	));
	let witness_dir = TestDir::new("counter-scope-witness-key");
	let leader = CounterLeader::new(
		node(1),
		lp.clone(),
		true,
		DurableCounterWitnessStore::open(witness_dir.to_path_buf()).unwrap(),
	);
	let key = CounterKey(b"new-key".to_vec());
	let outcome = leader
		.fetch_add(
			CounterParticipantPlan::new(vec![(node(1), lp.clone()), (node(2), rp.clone())], 1),
			[7; 24],
			&key,
			5,
		)
		.await
		.unwrap();
	assert_eq!(
		outcome,
		CounterAddResult::Applied(CounterAddOutcome {
			previous: 0,
			current: 5
		})
	);
	// A conflicting participant is excluded; the remaining quorum still applies the add.
	let witness = leader.witnesses.get(&1).unwrap();
	assert_eq!(witness.accepted, vec![node(2)]);
	assert_eq!(witness.key, key);
	assert_eq!(witness.commit_hlc, hlc(1));
	assert!(witness.committed.is_empty());
	drop(witness);
	assert_eq!(local.service.pending_count(), 1);
	assert_eq!(
		local
			.service
			.read_committed(tenant(7), original)
			.await
			.unwrap(),
		None
	);
	assert_eq!(
		local
			.slab
			.counter_read_value(local.slab.counter_lookup(tenant(7), original).unwrap())
			.await
			.unwrap(),
		99
	);
	let resolver =
		StaticCounterParticipantResolver::new(vec![(node(1), lp), (node(2), rp.clone())]);
	let before = remote.slab.local_l1_bytes().await;
	bridge
		.fail_commit
		.store(false, std::sync::atomic::Ordering::SeqCst);
	leader.reconcile(&resolver).await;
	assert!(leader.witnesses.is_empty());
	assert!(remote.slab.local_l1_bytes().await > before);
	assert_eq!(remote.service.pending_count(), 0);
	assert_eq!(
		remote
			.service
			.read_committed(tenant(7), counter_identity(&key))
			.await
			.unwrap(),
		Some(5)
	);
	let sent = bridge.sent.lock().unwrap();
	assert!(
		sent.iter()
			.any(|s| s.phase == Some(CounterReplicaPhase::COMMIT))
	);
	for s in sent.iter() {
		assert_eq!(s.peer, node(2));
		assert_eq!(s.tenant, tenant(7));
		assert_eq!(s.key, counter_identity(&key));
		assert_eq!(s.op, 1);
		assert_eq!(s.epoch, 19);
		if s.phase == Some(CounterReplicaPhase::TENTATIVE) {
			assert_eq!(s.value, 5);
			assert_eq!(s.hlc, hlc(1));
		} else {
			assert_eq!(s.value, 0);
			assert_eq!(s.hlc, hlc(0));
		}
	}
	drop(sent);
	// Unit rollback is best effort: conflict remains local, and cannot unindex another key.
	remote
		.service
		.replicate_tentative(node(1), 3, tenant(7), original, 44, hlc(3))
		.await
		.unwrap();
	assert_eq!(rp.commit(3, &key).await, Err(ParticipantError::Unavailable));
	rp.rollback(3, &key).await;
	assert_eq!(remote.service.pending_count(), 1);
	assert_eq!(
		remote
			.service
			.read_committed(tenant(7), original)
			.await
			.unwrap(),
		None
	);
	assert_eq!(
		remote
			.slab
			.counter_read_value(remote.slab.counter_lookup(tenant(7), original).unwrap())
			.await
			.unwrap(),
		44
	);
}
#[tokio::test]
async fn b1d_conflict_below_quorum_rolls_back_only_accepted_scope_with_real_key() {
	let local = Fixture::new("counter-scope-quorum-local").await;
	let remote = Fixture::new("counter-scope-quorum-remote").await;
	let original = identity(b"unrelated");
	local
		.service
		.replicate_tentative(node(1), 1, tenant(7), original, 99, hlc(1))
		.await
		.unwrap();
	let bridge = Arc::new(Bridge {
		service: remote.service.clone(),
		origin: node(1),
		sent: Default::default(),
		fail_commit: std::sync::atomic::AtomicBool::new(false),
	});
	let lp = CounterParticipantKind::Local(LocalCounterParticipant::new(
		local.service.clone(),
		tenant(7),
		node(1),
	));
	let rp = CounterParticipantKind::Remote(RemoteCounterParticipant::new(
		bridge.clone(),
		node(2),
		tenant(7),
		19,
	));
	let dir = TestDir::new("counter-scope-quorum-witness");
	let leader = CounterLeader::new(
		node(1),
		lp.clone(),
		true,
		DurableCounterWitnessStore::open(dir.to_path_buf()).unwrap(),
	);
	let key = CounterKey(b"new-key".to_vec());
	assert_eq!(
		leader
			.fetch_add(
				CounterParticipantPlan::new(vec![(node(1), lp), (node(2), rp)], 2),
				[7; 24],
				&key,
				5
			)
			.await,
		Err(CounterLeaderError::QuorumFailed)
	);
	assert_eq!(local.service.pending_count(), 1);
	assert_eq!(remote.service.pending_count(), 0);
	assert_eq!(
		remote
			.service
			.read_committed(tenant(7), counter_identity(&key))
			.await
			.unwrap(),
		None
	);
	assert!(leader.witnesses.is_empty());
	let sent = bridge.sent.lock().unwrap();
	assert_eq!(sent.iter().filter(|s| s.phase.is_none()).count(), 1);
	assert!(sent.iter().all(|s| s.key == counter_identity(&key)));
}
#[test]
fn b1d_synthetic_hlc_witness_and_commit_marker_bytes_are_unchanged() {
	assert_eq!(
		CounterLeader::<LocalCounterParticipant>::commit_hlc_for(0x0102030405060708),
		hlc(0x0102030405060708)
	);
	let w = CounterFinalizationWitness {
		op_id: 1,
		write_key: [7; 24],
		key: CounterKey(vec![9]),
		value: 11,
		previous: 3,
		commit_hlc: hlc(1),
		accepted: vec![node(2)],
		committed: HashSet::new(),
	};
	let mut golden = vec![1];
	golden.extend([7; 24]);
	golden.extend([1, 9, 11, 3, 1, 0, 0, 1, 2, 0, 0]);
	assert_eq!(postcard::to_stdvec(&w).unwrap(), golden);
	let decoded: CounterFinalizationWitness = postcard::from_bytes(&golden).unwrap();
	assert_eq!(decoded.op_id, 1);
	assert_eq!(decoded.key, CounterKey(vec![9]));
	assert_eq!(decoded.commit_hlc, hlc(1));
	let frame = crate::schema::CounterCommitFrame {
		tentative_op_id: 0x0102030405060708,
	};
	assert_eq!(bytemuck::bytes_of(&frame), &[8, 7, 6, 5, 4, 3, 2, 1]);
}
#[path = "network_scope_tests.rs"]
mod network_scope_tests;

#[tokio::test]
async fn b1d_queued_tentative_and_commit_serialize_while_other_scope_completes() {
	let f = Fixture::new("counter-scope-queued-phases").await;
	let key = ReplicaOpKey2::new(node(1), tenant(7), 1).unwrap();
	let gate = f.service.op_locks.entry(key).or_default().clone();
	let guard = gate.lock().await;
	let a = identity(b"a");
	let b = identity(b"b");
	let mut tentative =
		Box::pin(
			f.service
				.replicate_tentative(node(1), 1, tenant(7), a, 11, hlc(1)),
		);
	blocked(tentative.as_mut()).await;
	let mut commit = Box::pin(f.service.commit(node(1), 1, tenant(7), a));
	blocked(commit.as_mut()).await;
	// B is a distinct scoped key despite identical allocator/op. It need not wait for A.
	f.service
		.replicate_tentative(node(1), 1, tenant(8), b, 22, hlc(1))
		.await
		.unwrap();
	f.service.commit(node(1), 1, tenant(8), b).await.unwrap();
	assert!(f.slab.counter_lookup(tenant(7), a).is_none());
	assert_eq!(
		f.service.read_committed(tenant(8), b).await.unwrap(),
		Some(22)
	);
	drop(guard);
	tentative.await.unwrap();
	commit.await.unwrap();
	assert_eq!(f.service.pending_count(), 0);
	assert_eq!(
		f.service.read_committed(tenant(7), a).await.unwrap(),
		Some(11)
	);
	assert_eq!(
		f.service.read_committed(tenant(8), b).await.unwrap(),
		Some(22)
	);
	drop(gate);
	let f = f.reopened().await;
	assert_eq!(
		f.service.read_committed(tenant(7), a).await.unwrap(),
		Some(11)
	);
	assert_eq!(
		f.service.read_committed(tenant(8), b).await.unwrap(),
		Some(22)
	);
}

#[path = "visibility_tests.rs"]
mod visibility_tests;
