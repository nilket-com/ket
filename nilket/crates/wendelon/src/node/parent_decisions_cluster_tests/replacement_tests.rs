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
