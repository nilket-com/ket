//! T-K5 over a marker-eligible participant: the production local arm
//! (`LocalCounterParticipant` over `CounterReplicaService` on a real slab
//! manager), whose base reads see only marked values, beside the persistence
//! model's witness store for the parks. Here an abandoned add is invisible to
//! the next add's base read, so cancelling the body loses its delta.
use super::*;

use crate::crypto::CryptoManager;
use crate::storage::io_uring::spawn_io_thread;
use crate::storage::manager::{SlabManager, SlabManagerConfig};
use crate::test_support::TestDir;

const TENANT: u64 = 7;

async fn manager(dir: &TestDir, crypto: Arc<CryptoManager>, initialize: bool) -> Arc<SlabManager> {
	let config = SlabManagerConfig {
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
	};
	if initialize {
		SlabManager::initialize_new_store(config).await
	} else {
		SlabManager::open(config).await
	}
	.unwrap()
}

/// One leader whose only participant is its marker-eligible local arm.
struct EligibleRig {
	model: Arc<PersistenceModel>,
	dir: TestDir,
	crypto: Arc<CryptoManager>,
	slab: Arc<SlabManager>,
	service: Arc<CounterReplicaService>,
}

type EligibleLeader = Arc<CounterLeader<LocalCounterParticipant>>;

impl EligibleRig {
	async fn new(tag: &str) -> Self {
		let dir = TestDir::new(tag);
		let crypto = Arc::new(CryptoManager::new_dev());
		crypto
			.install_local_only_for_tests(TenantId::new(TENANT))
			.unwrap();
		let slab = manager(&dir, crypto.clone(), true).await;
		let service = Arc::new(CounterReplicaService::new(slab.clone()));
		Self {
			model: PersistenceModel::new(),
			dir,
			crypto,
			slab,
			service,
		}
	}

	fn participant(&self) -> LocalCounterParticipant {
		LocalCounterParticipant::new(self.service.clone(), TenantId::new(TENANT), node_id(0))
	}

	fn plan(&self) -> CounterParticipantPlan<LocalCounterParticipant> {
		CounterParticipantPlan::new(vec![(node_id(0), self.participant())], 1)
	}

	fn leader(&self) -> EligibleLeader {
		Arc::new(CounterLeader::new(
			node_id(0),
			self.participant(),
			true,
			model_store(&self.model),
		))
	}

	/// Sync the slabs, power-cut the witness store, reopen both, recover the
	/// leader (which must not fail closed) and reconcile it.
	async fn restart(self) -> (Self, EligibleLeader) {
		self.slab.fsync_all_local_slabs().await.unwrap();
		let Self {
			model,
			dir,
			crypto,
			slab,
			service,
		} = self;
		drop(service);
		drop(slab);
		model.crash();
		let slab = manager(&dir, crypto.clone(), false).await;
		let service = Arc::new(CounterReplicaService::new(slab.clone()));
		let rig = Self {
			model,
			dir,
			crypto,
			slab,
			service,
		};
		let leader = Arc::new(
			CounterLeader::recover(node_id(0), rig.participant(), true, model_store(&rig.model))
				.expect("recovery must not fail closed"),
		);
		let resolver = StaticCounterParticipantResolver::new(vec![(node_id(0), rig.participant())]);
		leader.reconcile(&resolver).await;
		(rig, leader)
	}
}

/// Park the add `(wk(800), key, +5)` inside its witness write (point (i)),
/// drop its caller there, then release the park.
async fn drop_inside_the_witness_write(
	rig: &EligibleRig,
	leader: &EligibleLeader,
	key: &CounterKey,
) {
	let park = rig.model.park_at("write_file:", 1);
	{
		let add = leader.fetch_add(rig.plan(), wk(800), key, 5);
		tokio::pin!(add);
		tokio::select! {
			biased;
			() = park.reached.notified() => (),
			result = &mut add => panic!("the add finished before it parked: {result:?}"),
		}
	}
	park.release();
}

// T-K5 at point (i), marker-eligible: the spawned body completes the dropped
// add, so the next same-key add's base includes it and the counter ends at 6.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_body_keeps_the_dropped_delta_for_marker_eligible_reads() {
	let key = ckey(b"eligible");
	let rig = EligibleRig::new("counter-eligible-park-spawned").await;
	let leader = rig.leader();
	drop_inside_the_witness_write(&rig, &leader, &key).await;
	assert_eq!(
		leader.fetch_add(rig.plan(), wk(801), &key, 1).await,
		Ok(CounterAddResult::Applied(outcome(5, 6))),
		"the next add's base includes the dropped delta"
	);
	assert_eq!(
		leader.fetch_add(rig.plan(), wk(800), &key, 5).await,
		Ok(CounterAddResult::Replayed(outcome(0, 5)))
	);
	drop(leader);
	let (_rig, recovered) = rig.restart().await;
	assert_eq!(recovered.pending_witnesses(), 0);
	assert_eq!(recovered.read(&key).await.unwrap(), Some(6));
}

// T-K5 negative control, marker-eligible: with the body inline, dropping the
// caller inside the witness write abandons the install and the publish while
// the write still completes. The next add reads a base without the dropped
// delta (its tentative is unmarked and its value unpublished), and after
// restart and reconcile the durable witness completes BELOW that add: +5 is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_body_loses_the_dropped_delta_for_marker_eligible_reads() {
	let key = ckey(b"eligible");
	let rig = EligibleRig::new("counter-eligible-park-inline").await;
	let leader = rig.leader();
	leader
		.write_key_switches
		.inline_body
		.store(true, Ordering::Relaxed);
	drop_inside_the_witness_write(&rig, &leader, &key).await;
	assert_eq!(
		leader.fetch_add(rig.plan(), wk(801), &key, 1).await,
		Ok(CounterAddResult::Applied(outcome(0, 1))),
		"the next add's base misses the dropped delta"
	);
	drop(leader);
	let (_rig, recovered) = rig.restart().await;
	assert_eq!(
		recovered.pending_witnesses(),
		0,
		"the dropped add's witness resolved"
	);
	assert_eq!(
		recovered.read(&key).await.unwrap(),
		Some(1),
		"the counter lost the dropped +5 (6 expected without the defect)"
	);
}
