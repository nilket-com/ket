//! ADR 0104 (wendelon): a lost control voter is replaced by a new Raft
//! instance. Three ParentDecisions voters; a follower loses its disk; a blank
//! store cannot come back as it; a replacement token admits one fresh,
//! control-only store under a new instance, which the leader's controller
//! adds as a learner, promotes by one swap and records Done; the survivors'
//! data and writes stay exact; the replacement restarts as itself; and the
//! old instance, back on its old disk and reachable at the slot's address,
//! is excluded.

use super::offload::write_rows;
use super::*;
use crate::consensus::decisions::instances::{ALLOCATED_BASE, Origin, ReplacementPhase, Role};
use crate::consensus::decisions::{DecisionCommand, DecisionResult};

const TOKEN: u128 = 0x0104_0104_0104_0104;
const TICK: std::time::Duration = std::time::Duration::from_millis(100);

pub(super) async fn decide(cluster: &Cluster, command: DecisionCommand) -> DecisionResult {
	let leader = cluster.leader().await;
	match cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::Decision(command))
		.await
		.expect("the leader applies the command")
		.data
	{
		crate::consensus::WendelonResponse::Decision(result) => result,
		other => panic!("not a decision answer: {other:?}"),
	}
}

fn provider(cluster: &Cluster, id: u32) -> Arc<dyn crate::network::transport::TransportProvider> {
	Arc::new(SimulatedTransportProvider::new(
		crate::schema::NodeId { id, _reserved: 0 },
		cluster.network.clone(),
		crate::network::pool::BufPool::new(64, 1024 * 1024),
		TransportTier::QuicIoUring,
	))
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), target).unwrap();
		}
	}
}

/// The test's own data directory, emptied: the lost disk.
fn lose_disk(dir: &std::path::Path) {
	std::fs::remove_dir_all(dir).unwrap();
	std::fs::create_dir_all(dir).unwrap();
}

async fn replacement_phase(cluster: &Cluster) -> Option<ReplacementPhase> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()?
				.instances()
				.replacement(TOKEN)
				.map(|record| record.phase)
		})
		.await
}

/// Waits until `member` has applied the leader's entry, and requires their
/// memberships and replacement records to agree there.
async fn until_caught_up(cluster: &Cluster, member: &ControlOnlyNode, what: &str) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leader = cluster.leader().await;
		let ours = member.state_machine.state_snapshot().await;
		let theirs = cluster.node(leader).state_machine.state_snapshot().await;
		if ours.last_applied_log_id.is_some()
			&& ours.last_applied_log_id == theirs.last_applied_log_id
		{
			assert_eq!(ours.last_membership, theirs.last_membership, "{what}");
			assert_eq!(
				ours.decisions.as_ref().unwrap().instances(),
				theirs.decisions.as_ref().unwrap().instances(),
				"{what}"
			);
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{what}: never caught up ({:?} vs {:?})",
			ours.last_applied_log_id,
			theirs.last_applied_log_id
		);
		tokio::time::sleep(TICK).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lost_follower_is_replaced_by_a_new_control_only_instance() {
	let (mut cluster, leader, descriptor) = ready().await;
	// A follower that was not the forming member: its configuration joins.
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let survivors: Vec<u32> = NODES.into_iter().filter(|id| *id != lost).collect();
	let mut expected = write_rows(&cluster, leader, &descriptor, &[(1, "a"), (2, "b")]).await;

	// The follower crashes; a copy of its disk is kept for its return, and the
	// disk itself is lost.
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	let old_disk = crate::test_support::TestDir::new("replacement-old-disk");
	copy_dir(&cfg.data_dir, &old_disk.to_path_buf());
	lose_disk(&cfg.data_dir);

	// A blank store cannot come back as the lost voter, even provisioned as a
	// joiner with the cluster's root: it is born again, and the slot's join
	// grant belongs to the lost store's birth, so it is refused before any
	// Raft object exists.
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	let error = WendelonNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.err()
		.expect("a blank store is not the lost voter");
	assert!(error.to_string().contains("SlotGranted"), "{error}");
	lose_disk(&cfg.data_dir);

	// The operator opens the replacement and provisions its token.
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded {
			slot: lost,
			r_old: lost,
			phase: ReplacementPhase::Begun,
		}
	);
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	// A full start on that directory is refused; only control-only starts it.
	assert!(
		WendelonNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
			.await
			.err()
			.expect("refused")
			.to_string()
			.contains("RaftRoleMismatch")
	);
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	assert_eq!(control.identity.instance, ALLOCATED_BASE);
	assert_eq!(control.identity.origin, Origin::Replacement);
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert!(!cfg.data_dir.join(crate::node::REPLACEMENT_TOKEN).exists());

	// The leader's controller adds, promotes and records Done.
	let deadline = tokio::time::Instant::now() + BOUND;
	while replacement_phase(&cluster).await
		!= Some(ReplacementPhase::Done {
			r_new: ALLOCATED_BASE,
		}) {
		assert!(tokio::time::Instant::now() < deadline, "never Done");
		tokio::time::sleep(TICK).await;
	}
	cluster
		.until("the swap is the applied membership", |state| {
			let membership = state.last_membership.membership();
			let voters: std::collections::BTreeSet<u32> = membership.voter_ids().collect();
			let mut target: std::collections::BTreeSet<u32> = survivors.iter().copied().collect();
			target.insert(ALLOCATED_BASE);
			voters == target
				&& membership.nodes().count() == 3
				&& state
					.decisions
					.as_ref()
					.is_some_and(|decisions| decisions.instances().is_retired(lost))
		})
		.await;
	until_caught_up(&cluster, &control, "the replacement catches up").await;

	// The survivors keep their data and keep committing under the sealed
	// placement; nothing is claimed of the replacement's data.
	let leader = cluster.leader().await;
	assert_ne!(
		leader, ALLOCATED_BASE,
		"the control-only member never leads"
	);
	// The lost slot's ranges are coordinated elsewhere once its leases lapse
	// (ADR 0729): the replacement holds no lineage and serves no data.
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leader = cluster.leader().await;
		let state = cluster.node(leader).state_machine.state_snapshot().await;
		let decisions = state.decisions.as_ref().unwrap();
		if decisions
			.lineages()
			.values()
			.all(|lineage| lineage.holder.id != lost)
		{
			break;
		}
		if tokio::time::Instant::now() >= deadline {
			let now = std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis() as u64;
			panic!(
				"no takeover of the lost slot's lineages: epoch {:?} peers {:?}; sealed {:?}; formed {:?}; lineages {:?}; leases {:?}; candidates {:?}",
				state.epoch_history.keys().last(),
				state.epoch_history.values().last().map(|config| config
					.peers
					.keys()
					.copied()
					.collect::<Vec<_>>()),
				decisions.sealed(),
				crate::consensus::decisions::formed_placement(&state),
				decisions
					.lineages()
					.iter()
					.map(|(start, lineage)| (*start, lineage.holder.id, lineage.epoch))
					.collect::<Vec<_>>(),
				state
					.leases
					.iter()
					.map(|(start, lease)| (*start, lease.lease_expiry.physical_ms))
					.collect::<Vec<_>>(),
				survivors
					.iter()
					.map(|node| crate::consensus::decisions::takeover_candidates(
						&state, *node, now, 1000, 16
					))
					.collect::<Vec<_>>(),
			);
		}
		tokio::time::sleep(TICK).await;
	}
	expected.extend(write_rows(&cluster, leader, &descriptor, &[(3, "c")]).await);
	for node in &survivors {
		expect_at(
			&cluster,
			*node,
			&descriptor,
			&expected,
			"after the replacement",
		)
		.await;
	}

	// The replacement restarts as itself, still control-only after Done.
	control.shutdown().await.unwrap();
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	assert_eq!(control.identity.instance, ALLOCATED_BASE);
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert!(
		WendelonNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
			.await
			.err()
			.is_some(),
		"never a full member before its activation"
	);
	until_caught_up(&cluster, &control, "after its restart").await;
	control.shutdown().await.unwrap();

	// The old instance returns on its old disk, reachable at the slot's
	// address: requests addressed to the replacement reach it and are refused,
	// so the replacement's progress does not move, and the survivors still
	// commit.
	let mut old_cfg = cfg.clone();
	old_cfg.data_dir = old_disk.to_path_buf();
	let old = WendelonNode::bootstrap_with_provider(old_cfg, provider(&cluster, lost))
		.await
		.unwrap();
	let leader = cluster.leader().await;
	let matched_before = cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.replication
		.as_ref()
		.and_then(|replication| replication.get(&ALLOCATED_BASE).cloned().flatten());
	expected.extend(write_rows(&cluster, leader, &descriptor, &[(4, "d")]).await);
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	let leader = cluster.leader().await;
	assert_ne!(leader, lost, "the old instance does not lead");
	let matched_after = cluster
		.node(leader)
		.raft
		.metrics()
		.borrow()
		.replication
		.as_ref()
		.and_then(|replication| replication.get(&ALLOCATED_BASE).cloned().flatten());
	assert_eq!(
		matched_after, matched_before,
		"nothing the old instance answered counts for the replacement"
	);
	for node in &survivors {
		expect_at(
			&cluster,
			*node,
			&descriptor,
			&expected,
			"with the old instance back",
		)
		.await;
	}
	cluster.network.unpublish(crate::schema::NodeId {
		id: lost,
		_reserved: 0,
	});
	let _ = tokio::time::timeout(std::time::Duration::from_secs(30), old.shutdown()).await;

	// The replacement comes back and catches up past what it missed.
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	until_caught_up(&cluster, &control, "after the old instance left").await;
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Control 6: the leader commits the swap's joint configuration and fails
/// before it proposes the uniform one. As it applies the joint configuration
/// it is cut off from the other members and its next append fails, poisoning
/// its log and stopping its Raft. A leader replicates an entry alongside its
/// own write, so the cut is what keeps the uniform step out of every log:
/// while it is down, no member's log goes past the joint entry. With the
/// replaced voter still lost, the joint configuration needs that leader: it
/// returns on its own store (OpenRaft resumes a committed vote of its own
/// under the same term), and its controller resumes from the joint
/// configuration to the recorded target and records Done.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_swap_stopped_after_its_joint_configuration_resumes_to_its_target() {
	let (mut cluster, leader, _descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let mut target: std::collections::BTreeSet<u32> =
		NODES.into_iter().filter(|id| *id != lost).collect();
	target.insert(ALLOCATED_BASE);

	// Armed before anything can reach a joint configuration: at the leader's
	// next one, it is cut off and its log poisoned, so its uniform step is
	// neither appended nor sent.
	let cut: Vec<crate::schema::NodeId> = NODES
		.into_iter()
		.filter(|id| *id != leader)
		.map(|id| crate::schema::NodeId { id, _reserved: 0 })
		.collect();
	let leader_id = crate::schema::NodeId {
		id: leader,
		_reserved: 0,
	};
	let fail_appends = cluster.node(leader).log_store.fail_appends_for_test();
	let network = cluster.network.clone();
	let to_cut = cut.clone();
	let (joint_tx, joint_rx) = tokio::sync::oneshot::channel();
	cluster
		.node(leader)
		.state_machine
		.on_next_joint_membership_for_test(Box::new(move |log_id| {
			for other in to_cut {
				network.block_bidirectional(leader_id, other);
			}
			fail_appends.store(true, std::sync::atomic::Ordering::SeqCst);
			let _ = joint_tx.send(log_id);
		}));

	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded {
			slot: lost,
			r_old: lost,
			phase: ReplacementPhase::Begun,
		}
	);
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	assert_eq!(control.identity.instance, ALLOCATED_BASE);

	let joint = tokio::time::timeout(BOUND, joint_rx)
		.await
		.expect("the swap reached its joint configuration at the armed leader")
		.unwrap();
	// The failed leader is closed (its close may report the poisoned log) and
	// comes back on its own store: the failure was one write, not its disk.
	let _ = cluster.close_with_result(leader).await;
	tokio::time::sleep(std::time::Duration::from_millis(500)).await;
	let survivor = *NODES
		.iter()
		.find(|id| **id != leader && **id != lost)
		.unwrap();
	for (who, last) in [
		(
			"the surviving full member",
			cluster
				.node(survivor)
				.raft
				.metrics()
				.borrow()
				.last_log_index,
		),
		(
			"the replacement",
			control.raft.metrics().borrow().last_log_index,
		),
	] {
		assert!(
			last.is_some_and(|last| last <= joint.index),
			"{who}'s log goes past the joint entry {joint:?}: {last:?}"
		);
	}
	for other in cut {
		cluster.network.heal_bidirectional(leader_id, other);
	}
	cluster.reopen(leader).await;

	let deadline = tokio::time::Instant::now() + BOUND;
	while replacement_phase(&cluster).await
		!= Some(ReplacementPhase::Done {
			r_new: ALLOCATED_BASE,
		}) {
		assert!(tokio::time::Instant::now() < deadline, "never Done");
		tokio::time::sleep(TICK).await;
	}
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let leader = cluster.leader().await;
		let state = cluster.node(leader).state_machine.state_snapshot().await;
		let membership = state.last_membership.membership();
		let voters: std::collections::BTreeSet<u32> = membership.voter_ids().collect();
		let uniform = state.last_membership.log_id();
		if membership.get_joint_config().len() == 1
			&& voters == target
			&& membership.nodes().count() == target.len()
			&& uniform.is_some_and(|uniform| uniform.index > joint.index)
			&& state
				.decisions
				.as_ref()
				.is_some_and(|decisions| decisions.instances().is_retired(lost))
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"not the target swapped after the joint entry: joint {joint:?}, leader {leader}, \
			 membership {:?} at {uniform:?}, retired {:?}",
			membership.get_joint_config(),
			state
				.decisions
				.as_ref()
				.map(|decisions| decisions.instances().is_retired(lost))
		);
		tokio::time::sleep(TICK).await;
	}
	until_caught_up(&cluster, &control, "after the resumed swap").await;
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_store_forms_only_with_a_formation_intent() {
	let (dir, cfg) = member(NODES[0]);
	// `member` provisions the intent for the forming node: remove it.
	std::fs::remove_file(cfg.data_dir.join(crate::node::FORMATION_INTENT)).unwrap();
	let error = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.err()
		.expect("no intent, no formation");
	assert!(
		error.to_string().contains("FormationIntentMissing"),
		"{error}"
	);
	// With the intent, it forms, and the intent is consumed.
	crate::node::provision_formation_intent(&cfg.data_dir).unwrap();
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	assert!(!cfg.data_dir.join(crate::node::FORMATION_INTENT).exists());
	node.shutdown().await.unwrap();
	// A restart needs no intent: the store is itself.
	WendelonNode::bootstrap_single_node(cfg)
		.await
		.unwrap()
		.shutdown()
		.await
		.unwrap();
	drop(dir);
}

/// The applied registry and signer of slot `slot` at the leader.
async fn slot_store(
	cluster: &Cluster,
	slot: u32,
	incarnation: crate::consensus::StoreIncarnation,
) -> (Option<crate::consensus::StoreIncarnation>, Option<[u8; 32]>) {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let current = state
				.incarnations
				.get(&slot)
				.and_then(|registry| registry.current);
			let bound = state.decisions.as_ref().and_then(|decisions| {
				decisions.signer_key(crate::consensus::decisions::Signer {
					node: slot,
					incarnation,
				})
			});
			(current, bound)
		})
		.await
}

/// ADR 0105 (wendelon): after Done, a control-only replacement admits its own
/// store J as its slot's current incarnation and binds J to the slot's
/// configured key, and stays control-only. J is read from its store's own
/// index (feasibility F1), the same J across restarts before promotion, after
/// it and after binding. The returning old process, still on the old store,
/// serves no read. The survivors stay exact. A store lost after promotion is
/// refused, never minted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replacement_admits_its_store_binds_its_key_and_stays_control_only() {
	use crate::node::replacement_preparation::{Stage, halt_for_test};
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let survivors: Vec<u32> = NODES.into_iter().filter(|id| *id != lost).collect();
	let mut expected = write_rows(&cluster, leader, &descriptor, &[(1, "a")]).await;
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	let old_disk = crate::test_support::TestDir::new("promotion-old-disk");
	copy_dir(&cfg.data_dir, &old_disk.to_path_buf());
	lose_disk(&cfg.data_dir);
	// DC-258: since ADR 0106 a prepared replacement hands its store's index to its
	// recovery, whose restricted storage needs the cluster's root; the replacement is given
	// it, as the recovery fixtures do.
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	let (old_incarnation, _) = slot_store(
		&cluster,
		lost,
		crate::consensus::StoreIncarnation::from_raw(0),
	)
	.await;
	let old_incarnation = old_incarnation.expect("the lost slot's store is registered");
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	let start = || ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost));

	// Halted before promotion: J is created once, after Done, and kept.
	halt_for_test(&cfg.data_dir, Some(Stage::Promote));
	let control = start().await.unwrap();
	let index = crate::storage::index::persistent_index_path(&cfg.data_dir);
	let deadline = tokio::time::Instant::now() + BOUND;
	while !index.exists() {
		assert!(tokio::time::Instant::now() < deadline, "J never created");
		tokio::time::sleep(TICK).await;
	}
	assert_eq!(
		replacement_phase(&cluster).await,
		Some(ReplacementPhase::Done {
			r_new: ALLOCATED_BASE
		}),
		"J is created only after Done"
	);
	let j = control.store_incarnation(false).await.unwrap();
	assert_ne!(j, old_incarnation);
	control.shutdown().await.unwrap();
	let control = start().await.unwrap();
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert_eq!(control.store_incarnation(false).await.unwrap(), j);
	assert_eq!(
		slot_store(&cluster, lost, j).await,
		(Some(old_incarnation), None)
	);

	// Promoted (once takeover moved the old store's lineages), halted before
	// binding; a restart there keeps J and the promotion.
	halt_for_test(&cfg.data_dir, Some(Stage::Bind));
	let deadline = tokio::time::Instant::now() + BOUND;
	while slot_store(&cluster, lost, j).await.0 != Some(j) {
		assert!(tokio::time::Instant::now() < deadline, "never promoted");
		tokio::time::sleep(TICK).await;
	}
	let leader = cluster.leader().await;
	let state = cluster.node(leader).state_machine.state_snapshot().await;
	let decisions = state.decisions.as_ref().unwrap();
	assert!(
		decisions
			.lineages()
			.values()
			.all(|lineage| lineage.holder.id != lost),
		"no lineage is held at the replaced slot"
	);
	assert!(state.incarnations[&lost].retired.contains(&old_incarnation));
	assert_eq!(
		decisions.instances().replacement(TOKEN).unwrap().promoted,
		Some(crate::consensus::decisions::instances::Promotion {
			expected: old_incarnation,
			incarnation: j,
		})
	);
	assert!(
		decisions
			.signer_key(crate::consensus::decisions::Signer {
				node: lost,
				incarnation: old_incarnation,
			})
			.is_some(),
		"the old store's binding is kept"
	);
	// Certification counts the slot as its new store, which reports nothing
	// yet: no certificate is committed without it (gate 3 owes its data).
	let members = crate::consensus::decisions::generations::current_members(&state).unwrap();
	assert!(members.contains(&crate::consensus::decisions::Signer {
		node: lost,
		incarnation: j,
	}));
	assert!(
		members
			.iter()
			.all(|member| member.incarnation != old_incarnation)
	);
	let certified = decisions.generations().current_gid();
	control.shutdown().await.unwrap();
	let control = start().await.unwrap();
	assert_eq!(control.store_incarnation(false).await.unwrap(), j);
	assert_eq!(slot_store(&cluster, lost, j).await, (Some(j), None));

	// Bound with the slot's configured key; preparation then ends. Its recovery may open
	// its restricted storage (taking the store's index) but takes no later step here.
	halt_for_test(&cfg.data_dir, Some(Stage::Recover));
	let (secret, _) = crate::node::identity::resolve_decision_keys(&cfg).unwrap();
	let key = ed25519_dalek::SigningKey::from_bytes(&secret)
		.verifying_key()
		.to_bytes();
	let deadline = tokio::time::Instant::now() + BOUND;
	while slot_store(&cluster, lost, j).await != (Some(j), Some(key)) || !control.prepared() {
		assert!(tokio::time::Instant::now() < deadline, "never bound");
		tokio::time::sleep(TICK).await;
	}
	until_caught_up(&cluster, &control, "prepared").await;
	assert_ne!(cluster.leader().await, ALLOCATED_BASE, "it never leads");
	control.shutdown().await.unwrap();
	let control = start().await.unwrap();
	assert_eq!(control.identity.role, Role::ControlOnly);
	// DC-258: once prepared, the store's index belongs to the recovery, never again to the
	// preparation: J is read from the recovery storage's own persistent index, once this
	// lifetime has opened it (bounded).
	let recovery = tokio::time::timeout(BOUND, control.recovery_storage())
		.await
		.expect("the recovery storage within its bound")
		.unwrap();
	assert_eq!(
		recovery
			.persistent_index()
			.expect("the recovery storage's index")
			.store_incarnation(),
		j
	);
	drop(recovery);
	control.shutdown().await.unwrap();
	halt_for_test(&cfg.data_dir, None);
	assert!(
		WendelonNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
			.await
			.err()
			.expect("refused")
			.to_string()
			.contains("RaftRoleMismatch"),
		"never a full member before its activation"
	);

	// The old process returns on the old store: it takes no lease and serves
	// no read; the survivors stay exact.
	let mut old_cfg = cfg.clone();
	old_cfg.data_dir = old_disk.to_path_buf();
	let old = WendelonNode::bootstrap_with_provider(old_cfg, provider(&cluster, lost))
		.await
		.unwrap();
	let leader = cluster.leader().await;
	expected.extend(write_rows(&cluster, leader, &descriptor, &[(2, "b")]).await);
	let answer = old
		.data_server
		.table_query_for_test(query_meta(&cluster, leader, &descriptor))
		.await
		.unwrap();
	assert_ne!(
		answer.status,
		Some(crate::schema::ReadStatus::OK),
		"the old store serves no read: {answer:?}"
	);
	let leader = cluster.leader().await;
	assert!(
		cluster
			.node(leader)
			.state_machine
			.state_snapshot()
			.await
			.decisions
			.unwrap()
			.lineages()
			.values()
			.all(|lineage| lineage.holder.id != lost),
		"the old process acquired no lineage"
	);
	for node in &survivors {
		expect_at(
			&cluster,
			*node,
			&descriptor,
			&expected,
			"with the old store back",
		)
		.await;
	}
	let leader = cluster.leader().await;
	assert_eq!(
		cluster
			.node(leader)
			.state_machine
			.state_snapshot()
			.await
			.decisions
			.unwrap()
			.generations()
			.current_gid(),
		certified,
		"no certificate without the replacement's report"
	);
	cluster.network.unpublish(crate::schema::NodeId {
		id: lost,
		_reserved: 0,
	});
	let _ = tokio::time::timeout(std::time::Duration::from_secs(30), old.shutdown()).await;

	// The store lost after promotion is refused and never minted again.
	std::fs::remove_file(&index).unwrap();
	let control = start().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.preparation_ended() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"preparation never stopped"
		);
		tokio::time::sleep(TICK).await;
	}
	assert!(!index.exists(), "no store is minted after promotion");
	let refused = control.store_incarnation(false).await.unwrap_err();
	assert!(
		refused.to_string().contains("ReplacementStoreLost"),
		"{refused}"
	);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// R2: a definitive refusal ends preparation and nothing is proposed again.
/// The first replacement reaches Done but has not promoted when a second
/// replacement of the same slot is opened against its instance; that instance
/// then restarts and proposes its first promotion once: refused as stale,
/// preparation ends, and the registry and its record are unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stale_promotion_is_refused_once_and_never_proposed_again() {
	use crate::node::replacement_preparation::{Stage, halt_for_test};
	let (mut cluster, leader, _descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	let (old_incarnation, _) = slot_store(
		&cluster,
		lost,
		crate::consensus::StoreIncarnation::from_raw(0),
	)
	.await;
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	halt_for_test(&cfg.data_dir, Some(Stage::Promote));
	let start = || ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost));
	let control = start().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while replacement_phase(&cluster).await
		!= Some(ReplacementPhase::Done {
			r_new: ALLOCATED_BASE,
		}) {
		assert!(tokio::time::Instant::now() < deadline, "never Done");
		tokio::time::sleep(TICK).await;
	}
	control.shutdown().await.unwrap();

	// A second replacement is opened against the first one's instance.
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			..
		}
	));
	halt_for_test(&cfg.data_dir, None);
	let control = start().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.preparation_ended() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"preparation never stopped after {} proposals",
			control.preparation_proposals_for_test()
		);
		tokio::time::sleep(TICK).await;
	}
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert_eq!(
		control.preparation_proposals_for_test(),
		1,
		"one proposal, no retry"
	);
	let leader = cluster.leader().await;
	let state = cluster.node(leader).state_machine.state_snapshot().await;
	assert_eq!(state.incarnations[&lost].current, old_incarnation);
	assert_eq!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.instances()
			.replacement(TOKEN)
			.unwrap()
			.promoted,
		None
	);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// One request frame from survivor `from` to the control-only member of slot
/// `to`, and its answer, or the error that ended the stream.
async fn exchange_with(
	cluster: &Cluster,
	from: u32,
	to: u32,
	msg_type: crate::schema::MsgType,
	meta: &[u8],
) -> std::io::Result<crate::network::core::HrftFrame> {
	use crate::network::core::{HRFT_MAGIC, read_frame, write_frame};
	let provider = provider(cluster, from);
	let connection = provider
		.connect(
			crate::schema::NodeId {
				id: to,
				_reserved: 0,
			},
			&address(to),
		)
		.await?;
	let mut stream = connection.open_bi().await?;
	let header = crate::schema::FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: msg_type.0,
		flags: 0,
		stream_id: stream.stream_id() as u32,
		meta_len: meta.len() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(&mut *stream, &header, meta, None, None).await?;
	stream.finish().await?;
	let pool = crate::network::pool::BufPool::new(4, 1024 * 1024);
	tokio::time::timeout(BOUND, read_frame(&mut *stream, &pool))
		.await
		.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no answer"))?
}

/// ADR 0105 (wendelon), DC-246: a data request that reaches a
/// control-only member gets its family's typed refusal, not a dropped stream:
/// a client's table write and query are told NOT_LEADER with no hint (the
/// write's outcome unknown, its identity echoed, no position, clock or
/// finalization); a survivor's child replicate and child fetch are refused
/// with no acknowledgement or rows; a survivor's pending-witness pull is
/// answered INTERNAL_ERROR with no witnesses, never OK with none.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_control_only_member_refuses_each_data_request_in_its_own_answer() {
	use crate::node::replacement_preparation::{Stage, halt_for_test};
	use crate::schema::{
		MsgType, MutationAnswer, MutationCommitKnowledge, PendingWitnessPullRequestMeta,
		PendingWitnessPullResponseMeta, PendingWitnessPullStatus, QueryResponseMeta, ReadStatus,
		TableWriteResponseMeta, WriteStatus, mutation_answer,
	};
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	halt_for_test(&cfg.data_dir, Some(Stage::Promote));
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while replacement_phase(&cluster).await
		!= Some(ReplacementPhase::Done {
			r_new: ALLOCATED_BASE,
		}) {
		assert!(tokio::time::Instant::now() < deadline, "never Done");
		tokio::time::sleep(TICK).await;
	}
	let survivor = cluster.leader().await;
	let before = cluster.node(survivor).state_machine.state_snapshot().await;

	// A table write: NOT_LEADER, no hint, the outcome unknown, its id echoed.
	let write = write_meta(&descriptor, 1, uuid_v7());
	let answer = exchange_with(
		&cluster,
		survivor,
		lost,
		MsgType::TABLE_WRITE_REQ,
		bytemuck::bytes_of(&write),
	)
	.await
	.expect("a table write is answered");
	assert_eq!(answer.header.msg_type, MsgType::TABLE_WRITE_RESP.0);
	assert_eq!(answer.header.trailing_len, 0, "no finalization");
	let written: TableWriteResponseMeta =
		*bytemuck::from_bytes(&answer.meta[..std::mem::size_of::<TableWriteResponseMeta>()]);
	let response = written.write;
	assert_eq!(response.status, WriteStatus::NOT_LEADER);
	assert_eq!(
		response.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(
		mutation_answer(response.status, response.mutation_commit_knowledge()),
		MutationAnswer::Other
	);
	assert_eq!(response.leader_hint.id, 0, "no hint");
	assert_eq!(response.echoed_write_id, write.parent_write_id);
	assert_eq!((response.log_index, response.term), (0, 0));
	assert_eq!(response.commit_hlc.physical_ms, 0, "no commit clock");
	assert_eq!(response.has_finalization, 0);

	// A table query: NOT_LEADER, no hint, no rows, the stream ended.
	let query = query_meta(&cluster, survivor, &descriptor);
	let answer = exchange_with(
		&cluster,
		survivor,
		lost,
		MsgType::TABLE_QUERY_REQ,
		bytemuck::bytes_of(&query),
	)
	.await
	.expect("a table query is answered");
	assert_eq!(answer.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
	let read: QueryResponseMeta =
		*bytemuck::from_bytes(&answer.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(read.status, ReadStatus::NOT_LEADER);
	assert_eq!((read.leader_hint.id, read.rows, read.arrow_len), (0, 0, 0));
	assert!(answer.arrow_payload.is_none());

	// A child replicate and a child fetch: refused, no acknowledgement or rows.
	for (request, response) in [
		(
			MsgType::TABLE_CHILD_REPLICATE_REQ,
			MsgType::TABLE_CHILD_REPLICATE_RESP,
		),
		(
			MsgType::TABLE_CHILD_WRITE_REQ,
			MsgType::TABLE_CHILD_WRITE_RESP,
		),
	] {
		let answer = exchange_with(&cluster, survivor, lost, request, b"child")
			.await
			.expect("a child request is answered");
		assert_eq!(answer.header.msg_type, response.0);
		assert!(matches!(
			postcard::from_bytes::<crate::network::data_server::child_writes::ChildWriteResponse>(
				&answer.meta
			)
			.unwrap(),
			crate::network::data_server::child_writes::ChildWriteResponse::Refused(_)
		));
	}
	let answer = exchange_with(
		&cluster,
		survivor,
		lost,
		MsgType::TABLE_CHILD_FETCH_REQ,
		b"fetch",
	)
	.await
	.expect("a child fetch is answered");
	assert_eq!(answer.header.msg_type, MsgType::TABLE_CHILD_FETCH_RESP.0);
	assert!(answer.arrow_payload.is_none(), "no rows");
	assert!(
		answer.meta.as_ref()
			!= postcard::to_stdvec(&crate::network::data_server::repair::ChildFetchResponse::Rows)
				.unwrap()
				.as_slice()
	);

	// A pending-witness pull: INTERNAL_ERROR with no witnesses, never OK.
	let pull = PendingWitnessPullRequestMeta {
		requester: crate::schema::NodeId {
			id: survivor,
			_reserved: 0,
		},
		request_epoch: cluster.node(survivor).ring.current_epoch(),
	};
	let answer = exchange_with(
		&cluster,
		survivor,
		lost,
		MsgType::PENDING_WITNESS_REQ,
		bytemuck::bytes_of(&pull),
	)
	.await
	.expect("a pending-witness pull is answered");
	assert_eq!(answer.header.msg_type, MsgType::PENDING_WITNESS_RESP.0);
	let witnesses: PendingWitnessPullResponseMeta = *bytemuck::from_bytes(
		&answer.meta[..std::mem::size_of::<PendingWitnessPullResponseMeta>()],
	);
	assert_eq!(witnesses.status, PendingWitnessPullStatus::INTERNAL_ERROR);
	assert_eq!(witnesses.witness_count, 0);
	assert!(answer.arrow_payload.is_none());

	// Nothing was decided on the member's behalf.
	let after = cluster.node(survivor).state_machine.state_snapshot().await;
	assert_eq!(
		after.decisions.as_ref().unwrap().instances(),
		before.decisions.as_ref().unwrap().instances()
	);
	assert_eq!(control.identity.role, Role::ControlOnly);
	control.shutdown().await.unwrap();
	halt_for_test(&cfg.data_dir, None);
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0106 (wendelon) feasibility F2: a promoted control-only replacement,
/// with the cluster's root enrolled, takes its store's index over from its
/// preparation (one handle at a time) into a restricted slab manager under the
/// owner it already holds; stores a real row and a typed tombstone durably at
/// their exact identity and HLC; installs a recovered base at a generation
/// whose predecessor is nonzero, published only when its entries hash to the
/// certificate's chunks; and after a clean close and reopen the base is active
/// at that generation and every entry passes `entry_is_stored`. It stays
/// control-only throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_promoted_replacement_stores_and_reopens_a_recovered_base() {
	use crate::schema::{HybridTimestamp, TenantId};
	async fn step<T>(what: &str, work: impl std::future::Future<Output = T>) -> T {
		eprintln!("F2 step begins: {what}");
		tokio::time::timeout(std::time::Duration::from_secs(60), work)
			.await
			.unwrap_or_else(|_| panic!("F2 step hung: {what}"))
	}
	use crate::storage::index::certified_base::{
		BaseEntry, InstallRefusal, StagedCandidate, chunk_sums,
	};
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	// A first write seals the placement; takeover (and so promotion, P1) needs
	// it.
	write_rows(&cluster, leader, &descriptor, &[(1, "a")]).await;
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	// The storage opens, but no recovery step runs: this test writes the store
	// itself.
	crate::node::replacement_preparation::halt_for_test(
		&cfg.data_dir,
		Some(crate::node::replacement_preparation::Stage::Recover),
	);
	let start = || ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost));
	let control = step("start", start()).await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.prepared() {
		if tokio::time::Instant::now() >= deadline {
			let leader = cluster.leader().await;
			let state = cluster.node(leader).state_machine.state_snapshot().await;
			let decisions = state.decisions.as_ref().unwrap();
			panic!(
				"never prepared: phase {:?}; registry {:?}; lineages at the lost slot {}; index {}",
				replacement_phase(&cluster).await,
				state.incarnations.get(&lost),
				decisions
					.lineages()
					.values()
					.filter(|lineage| lineage.holder.id == lost)
					.count(),
				crate::storage::index::persistent_index_path(&cfg.data_dir).exists(),
			);
		}
		tokio::time::sleep(TICK).await;
	}
	// The preparation hands its index over as soon as it is prepared: J is
	// read from the applied registry.
	let j = slot_store(
		&cluster,
		lost,
		crate::consensus::StoreIncarnation::from_raw(0),
	)
	.await
	.0
	.expect("promoted");

	// The preparation's handle is handed over; an interrupted hand-over is
	// completed by the next one, and the restricted manager opens the same
	// promoted store. The preparation never opens it again.
	let _ = tokio::time::timeout(std::time::Duration::ZERO, control.recovery_storage()).await;
	let slab = step("recovery storage", control.recovery_storage())
		.await
		.unwrap();
	let refused = control.store_incarnation(false).await.unwrap_err();
	assert!(
		refused.to_string().contains("ReplacementStoreHandedOver"),
		"{refused}"
	);
	// DC-251: the promoted store's own index names it; it is not bound to a node here.
	assert_eq!(
		slab.persistent_index()
			.expect("the promoted store's index")
			.store_incarnation(),
		j
	);

	// One row and one typed tombstone, durably, at their exact versions.
	let tenant = TenantId::ZERO;
	let row_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 1,
		node_id: leader,
	};
	let rows = named_rows(&descriptor, &[(901, "recovered")]);
	crate::network::data_server::child_writes::ingest_recovered_rows(
		&slab,
		tenant,
		&descriptor,
		&rows,
		row_hlc,
	)
	.await
	.unwrap();
	let mut meta = write_meta(&descriptor, 1, uuid_v7());
	meta.tenant_id = tenant;
	let [row] = crate::network::data_server::child_writes::child_manifest(
		&meta,
		&rows,
		&descriptor,
		row_hlc,
	)
	.unwrap()[..] else {
		panic!("one row");
	};
	let gone = crate::network::data_server::child_writes::child_manifest(
		&meta,
		&named_rows(&descriptor, &[(902, "gone")]),
		&descriptor,
		row_hlc,
	)
	.unwrap()[0];
	let tomb_hlc = HybridTimestamp {
		logical: 2,
		..row_hlc
	};
	let delete = crate::consensus::decisions::DeleteRequest {
		table: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		row: crate::schema::Identity::new(gone.key_identity),
	};
	crate::network::data_server::deletes::append_tombstone(
		&slab,
		tenant,
		&descriptor,
		&delete,
		tomb_hlc,
		&[],
	)
	.await
	.unwrap();
	let position = |key: [u8; 32]| {
		crate::consensus::decisions::delete_row_position(
			tenant,
			&descriptor,
			crate::schema::Identity::new(key),
		)
		.unwrap()
	};
	let entries = vec![
		BaseEntry {
			tenant,
			table: descriptor.table_object_id,
			key_identity: row.key_identity,
			hlc: row_hlc,
			row_digest: row.row_digest,
			tombstone: false,
			position: position(row.key_identity),
		},
		BaseEntry {
			tenant,
			table: descriptor.table_object_id,
			key_identity: gone.key_identity,
			hlc: tomb_hlc,
			row_digest: [0; 32],
			tombstone: true,
			position: position(gone.key_identity),
		},
	];
	let certificate = StagedCandidate {
		gid: 5,
		target_digest: [0x06; 32],
		predecessor_gid: 4,
		chunks: chunk_sums(&entries),
	};
	let index = slab.persistent_index().unwrap();
	// Pages staged; a publication whose sums differ is refused and changes
	// nothing; the matching one publishes at gid 5.
	for entry in &entries {
		assert_eq!(
			index
				.stage_recovered_page(certificate.clone(), vec![*entry])
				.await
				.unwrap(),
			Ok(())
		);
	}
	let wrong = StagedCandidate {
		chunks: chunk_sums(&entries[..1]),
		..certificate.clone()
	};
	assert_eq!(
		index.publish_recovered(wrong).await.unwrap(),
		Err(InstallRefusal::OtherCertificate)
	);
	assert_eq!(index.base_meta().await.unwrap().active_gid, 0);
	assert_eq!(
		index.publish_recovered(certificate.clone()).await.unwrap(),
		Ok(())
	);
	assert_eq!(
		index
			.stage_recovered_page(certificate.clone(), entries.clone())
			.await
			.unwrap(),
		Err(InstallRefusal::NotPristine),
		"a published store installs nothing more"
	);
	drop(index);
	drop(slab);
	assert_eq!(control.identity.role, Role::ControlOnly);
	step("shutdown", control.shutdown()).await.unwrap();

	// A clean close, then a reopen: active at gid 5, every entry stored.
	let control = step("start", start()).await.unwrap();
	assert_eq!(control.identity.role, Role::ControlOnly);
	let slab = step("recovery storage", control.recovery_storage())
		.await
		.unwrap();
	let index = slab.persistent_index().unwrap();
	let meta = index.base_meta().await.unwrap();
	assert_eq!(
		(meta.active_gid, &meta.active_chunks, meta.staged.is_none()),
		(5, &certificate.chunks, true)
	);
	let mut stored = index.base_entries(false).await.unwrap();
	stored.sort_by_key(|entry| entry.key_identity);
	let mut wanted = entries.clone();
	wanted.sort_by_key(|entry| entry.key_identity);
	assert_eq!(stored, wanted);
	for entry in &stored {
		crate::network::data_server::certification::entry_is_stored(
			&slab,
			&control.state_machine,
			entry,
		)
		.await
		.unwrap_or_else(|why| panic!("{entry:?}: {why}"));
	}
	drop(index);
	drop(slab);
	step("shutdown", control.shutdown()).await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0106 (wendelon) R3: a certified base that is empty at a nonzero gid
/// (generation 1 commits after only a noop, before any row is written) is
/// installed empty: the
/// staging is bound to the certificate without receiving an entry, its sums are
/// the empty ones, and it is published at gid 1. The write after it is owed as
/// a child and repaired; the recovery is recorded for gid 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_promoted_replacement_installs_an_empty_certified_base() {
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	// A delete of a row never written is recorded as a noop: placement is
	// sealed and lineages run, and no base entry exists.
	let absent = super::delete_through(
		&cluster,
		leader,
		super::delete_meta(&descriptor),
		vec![
			cluster
				.node(leader)
				.data_server
				.row_identity_and_locality_for_test(
					TenantId::ZERO,
					&descriptor,
					&named_rows(&descriptor, &[(99, "x")]),
				)
				.0,
		],
	)
	.await;
	assert_eq!(
		absent.write.status,
		crate::schema::WriteStatus::OK,
		"{absent:?}"
	);
	let first = now + 60_000;
	jump_floor(&cluster, first).await;
	until_active(&cluster, 1).await;
	let certificate = {
		let state = cluster.node(leader).state_machine.state_snapshot().await;
		state
			.decisions
			.unwrap()
			.generations()
			.current
			.clone()
			.unwrap()
	};
	let empty = crate::storage::index::certified_base::chunk_sums(std::iter::empty());
	assert_eq!(
		(certificate.gid, &certificate.chunks),
		(1, &empty),
		"generation 1 certifies an empty base"
	);
	// The first write seals the placement (promotion waits for it).
	let meta = write_meta(&descriptor, 1, uuid_v7_at(first + 1));
	assert_committed(
		&write_through(&cluster, leader, meta, named_rows(&descriptor, &[(1, "a")])).await,
		"a write",
	);

	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND * 2;
	let recorded = loop {
		if let Some(recorded) = recovery_record(&cluster).await {
			break recorded;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never recorded; prepared {}, preparation ended {}",
			control.prepared(),
			control.preparation_ended()
		);
		tokio::time::sleep(TICK).await;
	};
	assert_eq!(
		(recorded.gid, recorded.target_digest),
		(1, Some(certificate.target_digest))
	);
	let slab = control.recovery_storage().await.unwrap();
	let index = slab.persistent_index().unwrap();
	let meta = index.base_meta().await.unwrap();
	assert_eq!((meta.active_gid, &meta.active_chunks), (1, &empty));
	assert!(index.base_entries(false).await.unwrap().is_empty());
	let owed = children_naming(&cluster, lost).await;
	assert!(!owed.is_empty(), "the write's child names the slot");
	for (rsv, ordinal, content) in &owed {
		let intent = index.parent_intent(*rsv, *ordinal).await.unwrap();
		assert!(
			intent.is_some_and(|intent| intent.state
				== crate::storage::index::parent_intents::ParentIntentState::Complete
				&& intent.content == *content),
			"child ({rsv}, {ordinal}) held"
		);
	}
	assert_eq!(control.identity.role, Role::ControlOnly);
	drop(index);
	drop(slab);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0106 (wendelon), Codex 01a1e238 R1: the row pass must cover every
/// verified entry. The first source asked serves entries whole (they verify) but
/// answers its row page with only the first entry, claiming the end. That page
/// is refused before anything is stored or published, the other survivor is
/// asked from the same cursor, and the base is installed complete: recorded for
/// the current certificate, active with its sums, every entry stored. Then the
/// owning workflow's stopped-store reader is checked against this store.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_short_row_pass_is_refused_and_recovery_completes_from_another_source() {
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	let meta = write_meta(&descriptor, 3, uuid_v7_at(now));
	assert_committed(
		&write_through(
			&cluster,
			leader,
			meta,
			named_rows(&descriptor, &[(1, "a"), (2, "b"), (3, "c")]),
		)
		.await,
		"a write",
	);
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let certificate = {
		let state = cluster.node(leader).state_machine.state_snapshot().await;
		state
			.decisions
			.unwrap()
			.generations()
			.current
			.clone()
			.unwrap()
	};

	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	let short = *NODES.iter().filter(|id| **id != lost).min().unwrap();
	crate::network::data_server::base_fetch::TAMPERED_SOURCES
		.lock()
		.unwrap()
		.insert(
			cluster.configs[&short].data_dir.clone(),
			crate::network::data_server::base_fetch::Tamper::ShortRows,
		);
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND * 2;
	let recorded = loop {
		if let Some(recorded) = recovery_record(&cluster).await {
			break recorded;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never recorded; prepared {}, preparation ended {}",
			control.prepared(),
			control.preparation_ended()
		);
		tokio::time::sleep(TICK).await;
	};
	assert!(
		crate::network::data_server::base_fetch::TAMPERED_PAGES
			.load(std::sync::atomic::Ordering::SeqCst)
			> 0,
		"the short source answered a row page"
	);
	assert_eq!(
		(recorded.gid, recorded.target_digest),
		(1, Some(certificate.target_digest))
	);
	let slab = control.recovery_storage().await.unwrap();
	let index = slab.persistent_index().unwrap();
	let meta = index.base_meta().await.unwrap();
	assert_eq!(
		(meta.active_gid, &meta.active_chunks),
		(1, &certificate.chunks)
	);
	let entries = index.base_entries(false).await.unwrap();
	assert_eq!(entries.len(), 3, "every certified entry installed");
	for entry in &entries {
		crate::network::data_server::certification::entry_is_stored(
			&slab,
			&control.state_machine,
			entry,
		)
		.await
		.unwrap_or_else(|why| panic!("{entry:?}: {why}"));
	}
	assert_eq!(control.identity.role, Role::ControlOnly);
	// Where the first row's stored copy lies, for the reader control below.
	let location = slab
		.version_locations(
			entries[0].tenant,
			crate::schema::Identity::new(entries[0].key_identity),
			entries[0].hlc,
		)
		.unwrap()[0];
	// The store's slab file naming (`slab_<id as 16 hex digits>.wnd`).
	let slab_file = slab
		.data_dir()
		.join(format!("slab_{:016x}.wnd", location.slab_id));
	drop(index);
	drop(slab);
	control.shutdown().await.unwrap();

	// The owning workflow's stopped-store reader: the rows read back from the
	// slab are exactly the written ones, every Complete child verifies, and the
	// saved statement is the recorded one. With one stored copy damaged in the
	// slab file (its index and manifest references kept) the reader fails.
	let read_back = |found: &crate::node::RecoveredRowsForTest| {
		let mut held = Vec::new();
		for batch in &found.rows {
			let ids = batch
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			let names = batch
				.column(1)
				.as_any()
				.downcast_ref::<StringArray>()
				.unwrap();
			for row in 0..batch.num_rows() {
				held.push((ids.value(row), names.value(row).to_owned()));
			}
		}
		held.sort();
		held
	};
	let written: Vec<(i64, String)> = [(1, "a"), (2, "b"), (3, "c")]
		.into_iter()
		.map(|(id, name)| (id, name.to_owned()))
		.collect();
	let found = crate::node::recovered_rows_in(&cfg, &descriptor)
		.await
		.unwrap();
	assert!(found.unverified.is_empty(), "{:?}", found.unverified);
	assert_eq!(
		found.saved.map(|saved| (saved.3, saved.4)),
		Some((recorded.gid, recorded.k))
	);
	assert_eq!(
		read_back(&found),
		written,
		"complete {} rows {}",
		found.complete_children,
		found.rows.len()
	);
	{
		use std::io::{Read, Seek, SeekFrom, Write};
		let mut file = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open(&slab_file)
			.unwrap();
		let at = location.offset + u64::from(location.length) / 2;
		let mut byte = [0_u8];
		file.seek(SeekFrom::Start(at)).unwrap();
		file.read_exact(&mut byte).unwrap();
		file.seek(SeekFrom::Start(at)).unwrap();
		file.write_all(&[byte[0] ^ 0xFF]).unwrap();
		file.sync_all().unwrap();
	}
	match crate::node::recovered_rows_in(&cfg, &descriptor).await {
		Err(_) => {}
		Ok(found) => assert!(
			!found.unverified.is_empty() || read_back(&found) != written,
			"a damaged stored copy passes the reader: {found:?}"
		),
	}
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The promoted replacement's applied recovery record at the leader, if any.
async fn recovery_record(
	cluster: &Cluster,
) -> Option<crate::consensus::decisions::instances::Recovery> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()?
				.instances()
				.replacement(TOKEN)?
				.recovered
		})
		.await
}

/// The Accepted children naming `slot` at any incarnation, by (rsv, ordinal),
/// with their content.
async fn children_naming(cluster: &Cluster, slot: u32) -> Vec<(u64, u16, [u8; 32])> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let mut children = Vec::new();
			for body in state.decisions.as_ref().unwrap().bodies().values() {
				if !matches!(
					body.status,
					crate::consensus::decisions::ParentStatus::Accepted { .. }
				) {
					continue;
				}
				for (ordinal, child) in body.claim.children.iter().enumerate() {
					if child.plan.iter().any(|member| member.node == slot) {
						children.push((body.rsv, ordinal as u16, child.content));
					}
				}
			}
			children
		})
		.await
}

/// ADR 0106 (wendelon): a promoted replacement recovers its slot's data.
/// Two generations are certified (the current one's predecessor is nonzero),
/// a further write stays uncertified, then a follower is lost and replaced.
/// With the cluster's root enrolled, the replacement's own task fetches the
/// certified base page by page from the survivors and installs it at the
/// current gid (its sums equal the certificate's; every entry stored), repairs
/// every Accepted child naming the slot (none acknowledged by it), and records
/// its signed recovery statement for the current certificate. A write after
/// the record is repaired as delta. The member stays control-only throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_promoted_replacement_recovers_its_slots_base_and_children() {
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	// After a floor jump an operation's identity must be issued after the
	// floor (its UUIDv7 time).
	async fn write(
		cluster: &Cluster,
		descriptor: &TableSchemaDescriptor,
		at: u64,
		rows: &[(i64, &str)],
	) {
		let meta = write_meta(descriptor, rows.len(), uuid_v7_at(at));
		let leader = cluster.leader().await;
		assert_committed(
			&write_through(cluster, leader, meta, named_rows(descriptor, rows)).await,
			"a write",
		);
	}
	write(&cluster, &descriptor, now, &[(1, "a"), (2, "b")]).await;
	let first = now + 60_000;
	jump_floor(&cluster, first).await;
	until_active(&cluster, 1).await;
	write(&cluster, &descriptor, first + 1, &[(3, "c")]).await;
	let second = now + 120_000;
	jump_floor(&cluster, second).await;
	until_active(&cluster, 2).await;
	// A write the current certificate does not cover.
	write(&cluster, &descriptor, second + 1, &[(4, "d")]).await;
	let certificate = {
		let state = cluster.node(leader).state_machine.state_snapshot().await;
		state
			.decisions
			.unwrap()
			.generations()
			.current
			.clone()
			.unwrap()
	};
	assert_eq!((certificate.gid, certificate.predecessor_gid), (2, 1));

	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	// R4: the first source the replacement asks serves one fabricated extra
	// entry on every page. Its entries fail the certificate whole (discarded,
	// before any row is stored), its row pages are refused, and recovery comes
	// from the other survivor.
	let bad = *NODES.iter().filter(|id| **id != lost).min().unwrap();
	crate::network::data_server::base_fetch::TAMPERED_SOURCES
		.lock()
		.unwrap()
		.insert(
			cluster.configs[&bad].data_dir.clone(),
			crate::network::data_server::base_fetch::Tamper::ExtraEntry,
		);
	let start = || ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost));
	let control = start().await.unwrap();

	let deadline = tokio::time::Instant::now() + BOUND * 2;
	let recorded = loop {
		if let Some(recorded) = recovery_record(&cluster).await {
			break recorded;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never recorded; prepared {}, preparation ended {}",
			control.prepared(),
			control.preparation_ended()
		);
		tokio::time::sleep(TICK).await;
	};
	assert_eq!(
		(recorded.gid, recorded.target_digest),
		(2, Some(certificate.target_digest)),
		"the current certificate"
	);
	// Read before the source controls below ask the same source directly.
	let tampered_before_controls = crate::network::data_server::base_fetch::TAMPERED_PAGES
		.load(std::sync::atomic::Ordering::SeqCst);

	// A survivor serves base pages only to the slot's current replacement, for
	// the current certificate.
	{
		use crate::network::data_server::base_fetch::{
			BaseFetchAnswer, BaseFetchRefusal, BaseFetchRequest,
		};
		let leader = cluster.leader().await;
		let (birth, store) = {
			let state = cluster.node(leader).state_machine.state_snapshot().await;
			let decisions = state.decisions.as_ref().unwrap();
			(
				decisions.instances().record(ALLOCATED_BASE).unwrap().birth,
				state.incarnations[&lost].current.unwrap(),
			)
		};
		let source = *NODES.iter().find(|id| **id != lost).unwrap();
		let good = BaseFetchRequest {
			token: TOKEN,
			r_new: ALLOCATED_BASE,
			birth,
			store,
			gid: 2,
			target_digest: certificate.target_digest,
			after: None,
			max_rows: 4,
			max_bytes: crate::network::data_server::base_fetch::MAX_PAGE_BYTES,
			with_rows: true,
		};
		let ask = |from: u32, request: BaseFetchRequest| {
			let cluster = &cluster;
			async move {
				let answer = exchange_with(
					cluster,
					from,
					source,
					crate::schema::MsgType::TABLE_BASE_FETCH_REQ,
					&postcard::to_stdvec(&request).unwrap(),
				)
				.await
				.expect("a base fetch is answered");
				postcard::from_bytes::<BaseFetchAnswer>(&answer.meta).unwrap()
			}
		};
		assert!(matches!(
			ask(lost, good.clone()).await,
			BaseFetchAnswer::Page { ref entries, .. } if !entries.is_empty()
		));
		let other = *NODES
			.iter()
			.find(|id| **id != lost && **id != source)
			.unwrap();
		for (from, request, refusal) in [
			(
				lost,
				BaseFetchRequest {
					token: TOKEN + 1,
					..good.clone()
				},
				BaseFetchRefusal::NotAuthorized,
			),
			(other, good.clone(), BaseFetchRefusal::NotAuthorized),
			(
				lost,
				BaseFetchRequest {
					gid: 1,
					..good.clone()
				},
				BaseFetchRefusal::WrongGeneration,
			),
			(
				lost,
				BaseFetchRequest {
					max_rows: 0,
					..good.clone()
				},
				BaseFetchRefusal::Malformed,
			),
			(
				lost,
				BaseFetchRequest {
					max_bytes: crate::network::data_server::base_fetch::MAX_ROW_PAGE_BYTES + 1,
					..good.clone()
				},
				BaseFetchRefusal::Malformed,
			),
			// R1: a first row larger than the page's byte bound is refused, never
			// sent over it.
			(
				lost,
				BaseFetchRequest {
					max_bytes: 1,
					..good.clone()
				},
				BaseFetchRefusal::RowTooLarge,
			),
		] {
			assert_eq!(
				ask(from, request).await,
				BaseFetchAnswer::Refused(refusal),
				"from {from}"
			);
		}
	}

	// The base is installed at gid 2 from the survivors and every entry is
	// stored; every owed child is held, Complete.
	let slab = control.recovery_storage().await.unwrap();
	let index = slab.persistent_index().unwrap();
	let meta = index.base_meta().await.unwrap();
	assert_eq!(
		(meta.active_gid, &meta.active_chunks),
		(2, &certificate.chunks)
	);
	let entries = index.base_entries(false).await.unwrap();
	assert!(!entries.is_empty(), "the certified base has entries");
	for entry in &entries {
		crate::network::data_server::certification::entry_is_stored(
			&slab,
			&control.state_machine,
			entry,
		)
		.await
		.unwrap_or_else(|why| panic!("{entry:?}: {why}"));
	}
	// The bad source was asked and answered with its extra entry; nothing of
	// that entry reached this store.
	assert!(
		tampered_before_controls > 0,
		"the tampered source was asked"
	);
	let fabricated = slab
		.version_locations(
			entries[0].tenant,
			crate::schema::Identity::new([0xFF; 32]),
			entries[0].hlc,
		)
		.unwrap();
	assert!(fabricated.is_empty(), "no fabricated version stored");
	assert!(
		entries.iter().all(|entry| entry.key_identity != [0xFF; 32]),
		"no fabricated entry published"
	);
	let owed = children_naming(&cluster, lost).await;
	assert!(
		!owed.is_empty(),
		"the uncertified write's child names the slot"
	);
	for (rsv, ordinal, content) in &owed {
		let intent = index.parent_intent(*rsv, *ordinal).await.unwrap();
		assert!(
			intent.is_some_and(|intent| intent.state
				== crate::storage::index::parent_intents::ParentIntentState::Complete
				&& intent.content == *content),
			"child ({rsv}, {ordinal}) held"
		);
	}

	// A write after the record: its child naming the slot is repaired as delta.
	write(&cluster, &descriptor, second + 2, &[(5, "e")]).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let mut missing = 0;
		for (rsv, ordinal, content) in children_naming(&cluster, lost).await {
			let held = index
				.parent_intent(rsv, ordinal)
				.await
				.unwrap()
				.is_some_and(|intent| {
					intent.state
						== crate::storage::index::parent_intents::ParentIntentState::Complete
						&& intent.content == content
				});
			missing += usize::from(!held);
		}
		if missing == 0 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{missing} delta children never repaired"
		);
		tokio::time::sleep(TICK).await;
	}
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert_ne!(cluster.leader().await, ALLOCATED_BASE, "it never leads");
	drop(index);
	drop(slab);
	control.shutdown().await.unwrap();

	// A restart after the record (also a lost reply: the record and the
	// statement saved before sending agree): the stored data is validated
	// again and recovery continues; the record is unchanged.
	let control = start().await.unwrap();
	let slab = control.recovery_storage().await.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert!(
		!control.preparation_ended(),
		"a sound store keeps recovering"
	);
	assert_eq!(recovery_record(&cluster).await, Some(recorded));
	// One stored base entry is damaged; at the next start the sums recomputed
	// from the stored entries differ, and recovery stops: it never counts as
	// recovered, and nothing changes at the leader.
	slab.persistent_index()
		.unwrap()
		.tamper_base_entry_for_test()
		.await
		.unwrap();
	drop(slab);
	control.shutdown().await.unwrap();
	let control = start().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.preparation_ended() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"a damaged store kept recovering"
		);
		tokio::time::sleep(TICK).await;
	}
	assert_eq!(recovery_record(&cluster).await, Some(recorded));
	assert_eq!(control.identity.role, Role::ControlOnly);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0107 (wendelon): the leader's current certificate, if any.
async fn current_certificate(
	cluster: &Cluster,
) -> Option<crate::consensus::decisions::generations::Certificate> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.decisions.as_ref()?.generations().current.clone())
		.await
}

/// ADR 0107 (wendelon): waits until a certificate past `gid` is committed.
async fn until_certified_past(
	cluster: &Cluster,
	gid: u64,
	control: &ControlOnlyNode,
) -> crate::consensus::decisions::generations::Certificate {
	let deadline = tokio::time::Instant::now() + BOUND * 2;
	loop {
		if let Some(certificate) = current_certificate(cluster).await
			&& certificate.gid > gid
		{
			return certificate;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"no certificate past gid {gid}; preparation ended {}",
			control.preparation_ended()
		);
		tokio::time::sleep(TICK).await;
	}
}

/// ADR 0107 (wendelon): waits until the replacement's store is active at
/// `certificate`, with its chunks.
async fn until_replacement_active(
	slab: &SlabManager,
	certificate: &crate::consensus::decisions::generations::Certificate,
) {
	let index = slab.persistent_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let meta = index.base_meta().await.unwrap();
		if meta.active_gid == certificate.gid {
			assert_eq!(meta.active_chunks, certificate.chunks);
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the replacement never activated gid {} (active {})",
			certificate.gid,
			meta.active_gid
		);
		tokio::time::sleep(TICK).await;
	}
}

/// ADR 0107 (wendelon): the pending target's gid and its reporters, if any.
async fn pending_reports(cluster: &Cluster) -> Option<(u64, Vec<u32>)> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let pending = state.decisions.as_ref()?.generations().pending.as_ref()?;
			Some((
				pending.target.gid,
				pending.reports.keys().copied().collect(),
			))
		})
		.await
}

/// ADR 0107 (wendelon): waits until the replacement holds every Accepted child
/// naming its slot as a Complete intent of the recorded content.
async fn until_children_held(cluster: &Cluster, slab: &SlabManager, slot: u32) {
	let index = slab.persistent_index().unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let mut missing = 0;
		for (rsv, ordinal, content) in children_naming(cluster, slot).await {
			let held = index
				.parent_intent(rsv, ordinal)
				.await
				.unwrap()
				.is_some_and(|intent| {
					intent.state
						== crate::storage::index::parent_intents::ParentIntentState::Complete
						&& intent.content == content
				});
			missing += usize::from(!held);
		}
		if missing == 0 {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"{missing} children naming slot {slot} never held"
		);
		tokio::time::sleep(TICK).await;
	}
}

/// ADR 0107 (wendelon): a lost follower replaced, promoted and (unless the
/// caller blocks it) recording its recovery. The certified base at gid 1 holds
/// rows 1 and 2; row 3 is written before the loss and stays uncertified.
struct Replaced {
	cluster: Cluster,
	descriptor: TableSchemaDescriptor,
	lost: u32,
	cfg: NodeBootstrapConfig,
	now: u64,
}

async fn replaced_after_gid_one(block_recovery: bool) -> (Replaced, ControlOnlyNode) {
	let (mut cluster, leader, descriptor) = ready().await;
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	write_at(&cluster, &descriptor, now, &[(1, "a"), (2, "b")]).await;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	write_at(&cluster, &descriptor, now + 60_001, &[(3, "c")]).await;
	cluster.stop(lost, true).await;
	let cfg = cluster.configs[&lost].clone();
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.unwrap();
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded { .. }
	));
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN).unwrap();
	if block_recovery {
		// Every survivor serves a fabricated extra entry: no source's base
		// verifies, so recovery stays pending and records nothing.
		for source in NODES.iter().filter(|id| **id != lost) {
			crate::network::data_server::base_fetch::TAMPERED_SOURCES
				.lock()
				.unwrap()
				.insert(
					cluster.configs[source].data_dir.clone(),
					crate::network::data_server::base_fetch::Tamper::ExtraEntry,
				);
		}
	}
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	(
		Replaced {
			cluster,
			descriptor,
			lost,
			cfg,
			now,
		},
		control,
	)
}

/// A committed write at `at` (its identity issued then), entering at the leader.
async fn write_at(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	at: u64,
	rows: &[(i64, &str)],
) {
	let meta = write_meta(descriptor, rows.len(), uuid_v7_at(at));
	let leader = cluster.leader().await;
	assert_committed(
		&write_through(cluster, leader, meta, named_rows(descriptor, rows)).await,
		"a write",
	);
}

/// ADR 0107 (wendelon): a committed one-row write at `at`, with the first id
/// from `from` whose range's tenure floor is not ahead of the real clock. The
/// controlled clock's floor jumps move the ring clock about `E` ahead, so a
/// range taken over after the loss begins with a floor that far ahead, and a
/// claim there would be refused `CommitFromFuture` (its commit HLC must be
/// above the floor and within `DECISION_COMMIT_HLC_LEAD_MS` of the stamp): a
/// fixture artifact, avoided by choosing the row.
async fn write_current(
	cluster: &Cluster,
	descriptor: &TableSchemaDescriptor,
	at: u64,
	from: i64,
	name: &'static str,
) -> (i64, String) {
	let leader = cluster.leader().await;
	for id in from..from + 256 {
		let (_, lineage) = cluster
			.node(leader)
			.data_server
			.lineage_for_test(
				TenantId::ZERO,
				descriptor,
				&named_rows(descriptor, &[(id, name)]),
			)
			.await
			.expect("a sealed placement");
		let real = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis() as u64;
		if lineage.floor_ms <= real {
			write_at(cluster, descriptor, at, &[(id, name)]).await;
			return (id, name.to_owned());
		}
	}
	panic!("no row from {from} in a range with a current floor");
}

async fn until_recorded(
	cluster: &Cluster,
	control: &ControlOnlyNode,
) -> crate::consensus::decisions::instances::Recovery {
	let deadline = tokio::time::Instant::now() + BOUND * 2;
	loop {
		if let Some(recorded) = recovery_record(cluster).await {
			return recorded;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never recorded; prepared {}, preparation ended {}",
			control.prepared(),
			control.preparation_ended()
		);
		tokio::time::sleep(TICK).await;
	}
}

/// ADR 0107 (wendelon) controls 1, 2, 4, 5 and 6: a recovered replacement
/// takes part in certification.
/// - Before its recovery is recorded, a target naming the promoted store gets
///   the survivors' reports but none from it, and nothing commits.
/// - Once recorded, it reports its own staged candidate (its first answer
///   lost), the target covering children that name the lost store and the
///   promoted one commits, the covered records are erased, and it activates.
/// - A second generation commits after that activation.
/// - Stopped after a commit before activating, it reopens, activates from its
///   staging and validates at the current certificate; stopped after
///   activating, it reopens and continues. The historical record never changes.
/// - Its stopped store reads back exactly the acknowledged rows.
///
/// Write identities stay within two minutes of the start: a coordinator's
/// commit HLC follows them, and a claim leading the leader's stamp by more
/// than `DECISION_COMMIT_HLC_LEAD_MS` is refused `CommitFromFuture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_recovered_replacement_reports_and_its_generations_commit() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg,
			now,
		},
		control,
	) = replaced_after_gid_one(true).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.prepared() {
		assert!(tokio::time::Instant::now() < deadline, "never promoted");
		tokio::time::sleep(TICK).await;
	}
	let mut written: Vec<(i64, String)> = [(1, "a"), (2, "b"), (3, "c")]
		.into_iter()
		.map(|(id, name)| (id, name.to_owned()))
		.collect();
	// A child naming the promoted store.
	written.push(write_current(&cluster, &descriptor, now + 60_002, 100, "d").await);
	jump_floor(&cluster, now + 75_000).await;
	// Control 2: the survivors report the target; the unrecorded replacement
	// does not, so nothing commits.
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		if let Some((_, reporters)) = pending_reports(&cluster).await
			&& reporters.len() == NODES.len() - 1
		{
			assert!(!reporters.contains(&lost));
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the survivors never reported: {:?}",
			pending_reports(&cluster).await
		);
		tokio::time::sleep(TICK).await;
	}
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert!(recovery_record(&cluster).await.is_none());
	assert_eq!(current_certificate(&cluster).await.unwrap().gid, 1);
	assert!(
		!pending_reports(&cluster).await.unwrap().1.contains(&lost),
		"no report before the record"
	);

	// Recovery is unblocked; the first answer to its report is lost (control 4).
	crate::network::data_server::base_fetch::TAMPERED_SOURCES
		.lock()
		.unwrap()
		.clear();
	crate::node::replacement_preparation::LOSE_REPORT_ANSWERS
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone(), 1);
	let recorded = until_recorded(&cluster, &control).await;
	assert_eq!(recorded.gid, 1);
	// Control 1: the target covering both children commits on its own report.
	let second = until_certified_past(&cluster, 1, &control).await;
	assert_eq!(
		crate::node::replacement_preparation::LOSE_REPORT_ANSWERS
			.lock()
			.unwrap()
			.get(&cfg.data_dir),
		Some(&0),
		"a report answer was lost"
	);
	let slab = control.recovery_storage().await.unwrap();
	until_replacement_active(&slab, &second).await;
	assert!(
		children_naming(&cluster, lost).await.is_empty(),
		"the covered children's records are erased"
	);
	// Control 6: a second generation.
	written.push(write_current(&cluster, &descriptor, now + 75_001, 200, "e").await);
	jump_floor(&cluster, now + 90_000).await;
	let third = until_certified_past(&cluster, second.gid, &control).await;
	until_replacement_active(&slab, &third).await;

	// Control 5: a commit, then a stop before activation.
	let held = slab.data_dir().to_path_buf();
	crate::node::replacement_recovery::HOLD_ACTIVATION
		.lock()
		.unwrap()
		.insert(held.clone());
	written.push(write_current(&cluster, &descriptor, now + 90_001, 300, "f").await);
	jump_floor(&cluster, now + 105_000).await;
	let fourth = until_certified_past(&cluster, third.gid, &control).await;
	tokio::time::sleep(std::time::Duration::from_secs(1)).await;
	assert_eq!(
		slab.persistent_index()
			.unwrap()
			.base_meta()
			.await
			.unwrap()
			.active_gid,
		third.gid,
		"activation held"
	);
	drop(slab);
	control.shutdown().await.unwrap();
	crate::node::replacement_recovery::HOLD_ACTIVATION
		.lock()
		.unwrap()
		.remove(&held);
	let start = || ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost));
	let control = start().await.unwrap();
	let slab = control.recovery_storage().await.unwrap();
	until_replacement_active(&slab, &fourth).await;
	written.push(write_current(&cluster, &descriptor, now + 105_001, 400, "g").await);
	until_children_held(&cluster, &slab, lost).await;
	assert!(!control.preparation_ended(), "it keeps recovering");
	assert_eq!(recovery_record(&cluster).await, Some(recorded));
	drop(slab);
	control.shutdown().await.unwrap();

	// A stop after activation: it reopens and continues.
	let control = start().await.unwrap();
	let slab = control.recovery_storage().await.unwrap();
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert!(!control.preparation_ended(), "it keeps recovering");
	assert_eq!(
		slab.persistent_index()
			.unwrap()
			.base_meta()
			.await
			.unwrap()
			.active_gid,
		fourth.gid
	);
	assert_eq!(recovery_record(&cluster).await, Some(recorded));
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert_ne!(cluster.leader().await, ALLOCATED_BASE, "it never leads");
	drop(slab);
	control.shutdown().await.unwrap();

	// Its stopped store reads back exactly the acknowledged rows.
	let found = crate::node::recovered_rows_in(&cfg, &descriptor)
		.await
		.unwrap();
	assert!(found.unverified.is_empty(), "{:?}", found.unverified);
	assert_eq!(
		found.saved.map(|saved| (saved.3, saved.4)),
		Some((recorded.gid, recorded.k))
	);
	let mut held_rows = Vec::new();
	for batch in &found.rows {
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let names = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		for row in 0..batch.num_rows() {
			held_rows.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	held_rows.sort();
	written.sort();
	assert_eq!(held_rows, written);
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0107 (wendelon) control 3: after its start validation, one row the
/// replacement inherited from its recovered base is damaged in its slab file.
/// It proves inherited winners from its own data before signing, so it reports
/// nothing for the next target: nothing commits and nothing is erased.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replacement_with_a_damaged_inherited_row_does_not_report() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg: _,
			now,
		},
		control,
	) = replaced_after_gid_one(false).await;
	let recorded = until_recorded(&cluster, &control).await;
	assert_eq!(recorded.gid, 1);
	let slab = control.recovery_storage().await.unwrap();
	until_children_held(&cluster, &slab, lost).await;
	let index = slab.persistent_index().unwrap();
	let entries = index.base_entries(false).await.unwrap();
	assert_eq!(entries.len(), 2, "rows 1 and 2 are inherited");
	let entry = entries[0];
	let location = slab
		.version_locations(
			entry.tenant,
			crate::schema::Identity::new(entry.key_identity),
			entry.hlc,
		)
		.unwrap()[0];
	{
		use std::io::{Read, Seek, SeekFrom, Write};
		let path = slab
			.data_dir()
			.join(format!("slab_{:016x}.wnd", location.slab_id));
		let mut file = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.unwrap();
		let at = location.offset + u64::from(location.length) / 2;
		let mut byte = [0_u8];
		file.seek(SeekFrom::Start(at)).unwrap();
		file.read_exact(&mut byte).unwrap();
		file.seek(SeekFrom::Start(at)).unwrap();
		file.write_all(&[byte[0] ^ 0xFF]).unwrap();
		file.sync_all().unwrap();
	}
	// The damage is visible to the store's own proof.
	assert!(
		crate::network::data_server::certification::entry_is_stored(
			&slab,
			&control.state_machine,
			&entry,
		)
		.await
		.is_err(),
		"the damaged row still proves"
	);
	write_current(&cluster, &descriptor, now + 60_002, 100, "d").await;
	until_children_held(&cluster, &slab, lost).await;
	jump_floor(&cluster, now + 120_000).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		if let Some((_, reporters)) = pending_reports(&cluster).await
			&& reporters.len() == NODES.len() - 1
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the survivors never reported: {:?}",
			pending_reports(&cluster).await
		);
		tokio::time::sleep(TICK).await;
	}
	tokio::time::sleep(std::time::Duration::from_secs(3)).await;
	assert!(
		!pending_reports(&cluster).await.unwrap().1.contains(&lost),
		"the damaged replacement reported"
	);
	assert_eq!(current_certificate(&cluster).await.unwrap().gid, 1);
	assert!(
		!children_naming(&cluster, lost).await.is_empty(),
		"nothing is erased"
	);
	assert_eq!(index.base_meta().await.unwrap().active_gid, 1);
	drop(index);
	drop(slab);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0107 (wendelon) control 7: a generation commits on the replacement's
/// report, and before it activates, its staged evidence is damaged. Activation
/// finds no matching candidate: the replacement is behind the boundary and its
/// recovery stops, with its active base unchanged (no reset or copy).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replacement_without_its_committed_staging_stops_without_a_reset() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg: _,
			now,
		},
		control,
	) = replaced_after_gid_one(false).await;
	until_recorded(&cluster, &control).await;
	let slab = control.recovery_storage().await.unwrap();
	let held = slab.data_dir().to_path_buf();
	crate::node::replacement_recovery::HOLD_ACTIVATION
		.lock()
		.unwrap()
		.insert(held.clone());
	write_current(&cluster, &descriptor, now + 60_002, 100, "d").await;
	until_children_held(&cluster, &slab, lost).await;
	jump_floor(&cluster, now + 120_000).await;
	until_certified_past(&cluster, 1, &control).await;
	let index = slab.persistent_index().unwrap();
	assert_eq!(index.base_meta().await.unwrap().active_gid, 1);
	index.drop_staged_row_for_test().await.unwrap();
	crate::node::replacement_recovery::HOLD_ACTIVATION
		.lock()
		.unwrap()
		.remove(&held);
	let deadline = tokio::time::Instant::now() + BOUND;
	while !control.preparation_ended() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"a replacement behind the boundary kept recovering"
		);
		tokio::time::sleep(TICK).await;
	}
	assert_eq!(index.base_meta().await.unwrap().active_gid, 1, "no reset");
	assert_eq!(control.identity.role, Role::ControlOnly);
	drop(index);
	drop(slab);
	control.shutdown().await.unwrap();
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// ADR 0107 (wendelon), Codex 01a1e309 R1: a generation committed while a
/// recovery step is under way. The step reads its view at gid 1; the next
/// generation then commits on the replacement's own (already staged and
/// reported) candidate; the step resumes, activates that exact candidate and
/// continues: no stop, no reset, the record unchanged, a later write held, and
/// the stopped store reads back exactly the acknowledged rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_generation_committed_during_a_recovery_step_is_activated_and_recovery_continues() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg,
			now,
		},
		control,
	) = replaced_after_gid_one(false).await;
	let recorded = until_recorded(&cluster, &control).await;
	let slab = control.recovery_storage().await.unwrap();
	until_children_held(&cluster, &slab, lost).await;
	let mut written: Vec<(i64, String)> = [(1, "a"), (2, "b"), (3, "c")]
		.into_iter()
		.map(|(id, name)| (id, name.to_owned()))
		.collect();
	// The follower survivor's report is held, so the target cannot commit
	// until the test releases it; the leader's own maintenance keeps running.
	let leader = cluster.leader().await;
	let held = *NODES
		.iter()
		.find(|id| **id != lost && **id != leader)
		.unwrap();
	let (held_reached, held_release) = cluster.node(held).data_server.pause_coordinator_for_test(
		crate::network::data_server::child_writes::PauseStage::CertificationBeforeReport,
	);
	written.push(write_current(&cluster, &descriptor, now + 60_002, 100, "d").await);
	until_children_held(&cluster, &slab, lost).await;
	jump_floor(&cluster, now + 75_000).await;
	tokio::time::timeout(BOUND, held_reached.notified())
		.await
		.expect("the follower survivor's report is held after staging");
	// The replacement reports its staged candidate.
	let deadline = tokio::time::Instant::now() + BOUND;
	while !pending_reports(&cluster)
		.await
		.is_some_and(|(_, reporters)| reporters.contains(&lost))
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the replacement never reported: {:?}",
			pending_reports(&cluster).await
		);
		tokio::time::sleep(TICK).await;
	}
	// Its next step pauses after reading its view, at gid 1.
	let (step_reached, step_release) = (
		Arc::new(tokio::sync::Notify::new()),
		Arc::new(tokio::sync::Notify::new()),
	);
	crate::node::replacement_recovery::PAUSE_AFTER_VIEW
		.lock()
		.unwrap()
		.insert(
			slab.data_dir().to_path_buf(),
			(step_reached.clone(), step_release.clone()),
		);
	tokio::time::timeout(BOUND, step_reached.notified())
		.await
		.expect("a recovery step reached its pause");
	assert_eq!(current_certificate(&cluster).await.unwrap().gid, 1);
	// The successor commits while that step is paused, and the replacement's
	// own state applies it.
	held_release.notify_one();
	let second = until_certified_past(&cluster, 1, &control).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	while control
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.generations()
				.current_gid()
		})
		.await < second.gid
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the replacement never applied the commit"
		);
		tokio::time::sleep(TICK).await;
	}
	step_release.notify_one();
	until_replacement_active(&slab, &second).await;
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert!(
		!control.preparation_ended(),
		"a concurrent commit stopped a healthy recovery"
	);
	assert_eq!(recovery_record(&cluster).await, Some(recorded));
	// Recovery continues: a later write is held.
	written.push(write_current(&cluster, &descriptor, now + 75_001, 200, "e").await);
	until_children_held(&cluster, &slab, lost).await;
	assert!(!control.preparation_ended());
	drop(slab);
	control.shutdown().await.unwrap();
	let found = crate::node::recovered_rows_in(&cfg, &descriptor)
		.await
		.unwrap();
	assert!(found.unverified.is_empty(), "{:?}", found.unverified);
	assert_eq!(found.active_gid, second.gid);
	assert_eq!(
		found.saved.map(|saved| (saved.3, saved.4)),
		Some((recorded.gid, recorded.k))
	);
	let mut held_rows = Vec::new();
	for batch in &found.rows {
		let ids = batch
			.column(0)
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		let names = batch
			.column(1)
			.as_any()
			.downcast_ref::<StringArray>()
			.unwrap();
		for row in 0..batch.num_rows() {
			held_rows.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	held_rows.sort();
	written.sort();
	assert_eq!(held_rows, written);
	for node in std::mem::take(&mut cluster.nodes) {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

#[path = "replacement_activation_tests.rs"]
mod activation;
