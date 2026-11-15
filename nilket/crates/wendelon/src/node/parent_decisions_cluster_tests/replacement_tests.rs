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

async fn decide(cluster: &Cluster, command: DecisionCommand) -> DecisionResult {
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

	// Bound with the slot's configured key; preparation then ends.
	halt_for_test(&cfg.data_dir, None);
	let (secret, _) = crate::node::identity::resolve_decision_keys(&cfg).unwrap();
	let key = ed25519_dalek::SigningKey::from_bytes(&secret)
		.verifying_key()
		.to_bytes();
	let deadline = tokio::time::Instant::now() + BOUND;
	while slot_store(&cluster, lost, j).await != (Some(j), Some(key))
		|| !control.preparation_ended()
	{
		assert!(tokio::time::Instant::now() < deadline, "never bound");
		tokio::time::sleep(TICK).await;
	}
	until_caught_up(&cluster, &control, "prepared").await;
	assert_ne!(cluster.leader().await, ALLOCATED_BASE, "it never leads");
	control.shutdown().await.unwrap();
	let control = start().await.unwrap();
	assert_eq!(control.identity.role, Role::ControlOnly);
	assert_eq!(control.store_incarnation(false).await.unwrap(), j);
	control.shutdown().await.unwrap();
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

/// ADR 0105 (wendelon), DC-246: a data-plane request that reaches a
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
