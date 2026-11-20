//! ADR 0104 (wendelon) precondition: a control-only member keeps its Raft
//! obligations with no slab manager. A follower's real durable Raft store is
//! reopened by a stack that holds only the owner, the log store, the state
//! machine (with its checkpoint observer, without a tier observer) and a
//! Raft-only router. That stack installs a snapshot through Raft, applies live
//! tier entries, builds its own snapshot and restarts by replay, reaching
//! exactly the leader's state each time.

use super::offload::until_offloaded;
use super::*;

const TICK: std::time::Duration = std::time::Duration::from_millis(50);

/// Every unordered map of `state` in key order, then the rest: equal
/// content gives equal bytes.
fn canonical(state: &crate::consensus::raft_storage::SmState) -> Vec<u8> {
	fn sorted<K: serde::Serialize, V: serde::Serialize>(
		map: &std::collections::HashMap<K, V>,
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		let mut entries: Vec<_> = map
			.iter()
			.map(|(key, value)| {
				(
					postcard::to_allocvec(key).unwrap(),
					postcard::to_allocvec(value).unwrap(),
				)
			})
			.collect();
		entries.sort();
		entries
	}
	let maps = (
		sorted(&state.leases),
		sorted(&state.e_safe_watermarks),
		sorted(&state.tiered_slabs),
		sorted(&state.migrations),
		sorted(&state.tenants),
		sorted(&state.tenant_keysets),
	);
	let mut rest = state.clone();
	rest.leases.clear();
	rest.e_safe_watermarks.clear();
	rest.tiered_slabs.clear();
	rest.migrations.clear();
	rest.tenants.clear();
	rest.tenant_keysets.clear();
	postcard::to_allocvec(&(maps, rest)).unwrap()
}

/// The control-only stack over one member's own data directory.
struct ControlOnly {
	id: u32,
	raft: WendelonRaft,
	state_machine: WendelonStateMachine,
	emitter: tokio::task::JoinHandle<()>,
	owner: DataDirectoryOwner,
}

impl ControlOnly {
	async fn open(cluster: &Cluster, id: u32) -> Self {
		let cfg = &cluster.configs[&id];
		let owner = DataDirectoryOwner::acquire_existing(&cfg.data_dir).unwrap();
		let io = spawn_io_thread_named_for_class(
			cfg.wal_io_depth,
			"control-only-wal",
			IoUringClass::Wal,
		)
		.unwrap()
		.with_data_directory_owner(owner.clone());
		let (wal, vote, snapshot) = control_artifact_paths(&cfg.data_dir);
		let retention = RetentionGuard::new(cfg.control_retention);
		let store = WendelonLogStore::open_with_retention_and_owner(
			&wal,
			&vote,
			&snapshot,
			io,
			retention.clone(),
			owner.clone(),
			cfg.decision_mode,
			cfg.state_admission(),
		)
		.await
		.unwrap();
		let checkpoints = CheckpointCoordinator::new(store.clone());
		let state_machine = store
			.recovered_state_machine()
			.await
			.unwrap()
			.with_checkpoint_observer(checkpoints.clone());
		let emitter = checkpoints.spawn();
		let node = crate::schema::NodeId { id, _reserved: 0 };
		let pool = BufPool::new(64, 1024 * 1024);
		let provider = Arc::new(SimulatedTransportProvider::new(
			node,
			cluster.network.clone(),
			pool.clone(),
			TransportTier::QuicIoUring,
		));
		let network = WendelonRaftNetworkFactory::new(provider.clone(), pool.clone())
			.with_decision_mode(cfg.decision_mode);
		// Elections are off from construction: this member never campaigns.
		let mut config = (*control_raft_config(cfg).unwrap()).clone();
		config.enable_elect = false;
		let raft = WendelonRaft::new(
			id,
			Arc::new(config),
			network,
			store.clone(),
			state_machine.clone(),
		)
		.await
		.unwrap();
		let server = Arc::new(
			WendelonRaftInternalServer::new(raft.clone(), pool, retention)
				.with_decision_mode(cfg.decision_mode),
		);
		let router: Arc<dyn crate::network::server::HrftService> = Arc::new(
			MsgRouterBuilder::default()
				.with_raft_internal(server)
				.build(),
		);
		provider
			.publish_local(node, &cfg.advertise_address, router)
			.unwrap();
		Self {
			id,
			raft,
			state_machine,
			emitter,
			owner,
		}
	}

	async fn close(self, cluster: &Cluster) {
		cluster.network.unpublish(crate::schema::NodeId {
			id: self.id,
			_reserved: 0,
		});
		self.raft.shutdown().await.unwrap();
		self.emitter.abort();
		let _ = self.emitter.await;
		drop(self.state_machine);
		// As the production control-only member does: the directory is
		// released before anything reopens it.
		self.owner
			.close(std::time::Duration::from_secs(10))
			.await
			.unwrap();
	}

	fn applied(&self) -> Option<u64> {
		self.raft.metrics().borrow().last_applied.map(|id| id.index)
	}
}

/// Waits until this member and the leader have applied the same entry, and
/// requires their states to be identical there.
async fn until_same_state(cluster: &Cluster, member: &ControlOnly, what: &str) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leader = cluster.leader().await;
		let ours = member.state_machine.state_snapshot().await;
		let theirs = cluster.node(leader).state_machine.state_snapshot().await;
		if ours.last_applied_log_id.is_some()
			&& ours.last_applied_log_id == theirs.last_applied_log_id
		{
			assert!(
				canonical(&ours) == canonical(&theirs),
				"{what}: the control-only state differs from the leader's at {:?}",
				ours.last_applied_log_id
			);
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: never at the leader's entry (ours {:?}, leader {:?})",
			ours.last_applied_log_id,
			theirs.last_applied_log_id
		);
		tokio::time::sleep(TICK).await;
	}
}

/// Snapshots `raft` and waits until the snapshot covers `at`.
async fn snapshot_through(raft: &WendelonRaft, at: u64, what: &str) {
	raft.trigger().snapshot().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while raft
		.metrics()
		.borrow()
		.snapshot
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: no snapshot through {at}"
		);
		tokio::time::sleep(TICK).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_control_only_member_installs_applies_snapshots_and_replays_without_a_slab_manager() {
	let root = crate::test_support::TestDir::new("control-only-l2");
	let path = root.to_path_buf();
	let (mut cluster, leader, descriptor) = ready_with(Cluster::start_with(move |_, cfg| {
		cfg.l2 = Some(crate::storage::ObjectStoreConfig::LocalFs { root: path.clone() });
		cfg.tiering.scan_interval = std::time::Duration::from_millis(200);
		cfg.tiering.op_driver_interval = std::time::Duration::from_millis(200);
		None
	}))
	.await;
	let followers: Vec<u32> = NODES.into_iter().filter(|id| *id != leader).collect();
	let (member, other) = (followers[0], followers[1]);
	super::offload::write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;

	// The member offloads its own slab as a full node, then stops.
	let own = cluster.node(member).slab.force_seal_active_for_test().await;
	until_offloaded(&cluster, member, own, "the member's own offload").await;
	cluster.stop(member, false).await;

	// While it is down: the leader's slab is tiered, and the leader's log is
	// purged past it, so the member can only catch up by snapshot.
	let sealed = cluster.node(leader).slab.force_seal_active_for_test().await;
	until_offloaded(&cluster, leader, sealed, "the leader's offload").await;
	let at = cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.last_applied
		.unwrap()
		.index;
	snapshot_through(&cluster.node(leader).raft, at, "the leader").await;
	cluster
		.node(leader)
		.raft
		.trigger()
		.purge_log(at)
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.purged
		.is_none_or(|id| id.index < at)
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the leader never purged"
		);
		tokio::time::sleep(TICK).await;
	}

	// 1. Install through Raft, with no slab manager anywhere in the stack.
	let control = ControlOnly::open(&cluster, member).await;
	until_same_state(&cluster, &control, "after the snapshot install").await;
	assert!(
		control
			.raft
			.metrics()
			.borrow()
			.snapshot
			.is_some_and(|id| id.index >= at),
		"the member caught up by snapshot"
	);

	// 2. Live apply: the other follower's slab is tiered while the member follows.
	let sealed = cluster.node(other).slab.force_seal_active_for_test().await;
	until_offloaded(&cluster, other, sealed, "the other follower's offload").await;
	until_same_state(&cluster, &control, "after a live tier entry").await;

	// 3. Its own snapshot, built without a slab manager.
	let applied = control.applied().unwrap();
	snapshot_through(&control.raft, applied, "the member").await;

	// 4. Restart: the same store, replayed from its own snapshot and log.
	control.close(&cluster).await;
	let control = ControlOnly::open(&cluster, member).await;
	assert!(
		control
			.state_machine
			.state_snapshot()
			.await
			.last_applied_log_id
			.is_some_and(|id| id.index >= applied),
		"recovery restored at least the member's own snapshot"
	);
	until_same_state(&cluster, &control, "after the restart").await;
	// It never campaigned: every term was won by a full member.
	assert_ne!(cluster.leader().await, member);

	control.close(&cluster).await;
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}
