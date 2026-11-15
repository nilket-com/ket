//! ADR 0736: a backup hold in a real three-node ParentDecisions cluster. A write offered
//! while the hold is held gets the existing stalled answer (WRITE_STALLED, INDETERMINATE):
//! nothing is recorded for it, so after the hold is released by its exact token the SAME
//! identity can commit. A delayed release with a stale token changes nothing.

use super::*;
use crate::consensus::decisions::{DecisionCommand, DecisionResult};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_write_offered_during_a_hold_stalls_records_nothing_and_commits_after_release() {
	let (cluster, leader, descriptor) = ready().await;
	let DecisionResult::HoldEntered { id, entered_at } =
		decide(&cluster, DecisionCommand::BeginBackupHold { id: 0x736 }).await
	else {
		panic!("hold not entered");
	};

	// Find a row and write it at its coordinator, under the hold.
	let mut offered = None;
	for row in 1..200i64 {
		let batch = rows(&descriptor, &[row]);
		let meta = write_meta(&descriptor, 1, uuid_v7());
		let (mut response, _) = cluster
			.node(leader)
			.data_server
			.table_write_for_test(meta, batch.clone())
			.await
			.unwrap();
		let mut at = leader;
		if response.write.status == WriteStatus::NOT_LEADER {
			at = response.write.leader_hint.id;
			response = cluster
				.node(at)
				.data_server
				.table_write_for_test(meta, batch.clone())
				.await
				.unwrap()
				.0;
		}
		offered = Some((meta, batch, at, response));
		break;
	}
	let (meta, batch, at, held) = offered.unwrap();
	assert_eq!(
		(held.write.status, held.write.mutation_commit_knowledge()),
		(
			WriteStatus::WRITE_STALLED,
			MutationCommitKnowledge::INDETERMINATE
		),
		"a held offer: {held:?}"
	);
	// Nothing was recorded for the identity.
	assert!(
		record_of(&cluster, key_of(&descriptor, &meta))
			.await
			.is_none()
	);

	// A release with a stale token changes nothing; the exact token ends the hold.
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::ReleaseBackupHold {
				id,
				entered_at: entered_at + 1
			}
		)
		.await,
		DecisionResult::Refused(crate::consensus::decisions::DecisionRefusal::HoldMismatch)
	);
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::ReleaseBackupHold { id, entered_at }
		)
		.await,
		DecisionResult::HoldReleased
	);

	// The SAME identity commits now.
	let (after, _) = cluster
		.node(at)
		.data_server
		.table_write_for_test(meta, batch)
		.await
		.unwrap();
	assert_eq!(
		(after.write.status, after.write.mutation_commit_knowledge()),
		(WriteStatus::OK, MutationCommitKnowledge::COMMITTED),
		"after the release: {after:?}"
	);
	let recorded = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("recorded");
	assert!(matches!(
		recorded.status,
		crate::consensus::decisions::ParentStatus::Accepted { .. }
	));
}

/// The backup's KV check fails closed from both sources the reader pairs: a KV
/// key still retained as a decision body, and the same key after a generation
/// moved it into the certified base (no body left). Without KV it says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_kv_check_finds_a_kv_key_retained_and_after_it_moved_into_the_base() {
	use crate::logical_backup::roster::{RosterRefusal, roster_at};
	let (cluster, leader, _descriptor) = ready().await;
	let check = cluster
		.node(leader)
		.data_server
		.kv_presence()
		.expect("a ParentDecisions store has the check");
	assert_eq!(check.kv_present().await, Ok(false), "no KV yet");
	let at = &cluster;
	let roster = |token| async move {
		let leader = at.leader().await;
		at.node(leader)
			.state_machine
			.read_state(move |state| roster_at(state, token))
			.await
	};

	let (meta, payload) = kv_write_meta(b"backup-kv", b"v");
	let written = kv_write_through(&cluster, leader, meta, payload).await;
	assert_eq!(written.status, WriteStatus::OK, "{written:?}");

	// Retained: the roster itself refuses, and so does the store's check.
	let DecisionResult::HoldEntered { id, entered_at } =
		decide(&cluster, DecisionCommand::BeginBackupHold { id: 0x736 }).await
	else {
		panic!("hold not entered");
	};
	assert_eq!(
		roster((id, entered_at)).await,
		Err(RosterRefusal::KvDataPresent)
	);
	assert_eq!(check.kv_present().await, Ok(true), "a retained KV body");
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::ReleaseBackupHold { id, entered_at }
		)
		.await,
		DecisionResult::HoldReleased
	);

	// A generation erases the body into the certified base.
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(&cluster, now + 60_000).await;
	until_active(&cluster, 1).await;
	let DecisionResult::HoldEntered { id, entered_at } =
		decide(&cluster, DecisionCommand::BeginBackupHold { id: 0x737 }).await
	else {
		panic!("hold not entered");
	};
	let leader = cluster.leader().await;
	let kv_bodies = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.unwrap()
				.bodies()
				.values()
				.filter(|body| {
					body.claim.key.namespace == crate::consensus::decisions::ParentNamespace::Kv
				})
				.count()
		})
		.await;
	assert_eq!(kv_bodies, 0, "the generation erased the KV body");
	assert!(
		roster((id, entered_at)).await.is_ok(),
		"the replicated state alone no longer shows the key"
	);
	let check = cluster.node(leader).data_server.kv_presence().unwrap();
	assert_eq!(check.kv_present().await, Ok(true), "a KV base entry");
}

/// An orphaned hold (its exporter gone) survives the leader's crash and the
/// new leader's election, refuses a stale token there, and ends only by its
/// exact token.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_orphaned_hold_survives_a_leader_crash_until_its_exact_release() {
	let (mut cluster, leader, _descriptor) = ready().await;
	let DecisionResult::HoldEntered { id, entered_at } =
		decide(&cluster, DecisionCommand::BeginBackupHold { id: 0x738 }).await
	else {
		panic!("hold not entered");
	};
	cluster.stop(leader, true).await;
	let deadline = tokio::time::Instant::now() + BOUND;
	let successor = loop {
		let now = cluster.leader().await;
		if now != leader {
			break now;
		}
		assert!(tokio::time::Instant::now() < deadline, "no new leader");
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;
	};
	assert_eq!(
		cluster
			.node(successor)
			.state_machine
			.read_state(|state| state.decisions.as_ref().unwrap().backup_hold())
			.await,
		Some((id, entered_at)),
		"the hold outlives its leader"
	);
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::ReleaseBackupHold {
				id,
				entered_at: entered_at + 1
			}
		)
		.await,
		DecisionResult::Refused(crate::consensus::decisions::DecisionRefusal::HoldMismatch)
	);
	assert_eq!(
		decide(
			&cluster,
			DecisionCommand::ReleaseBackupHold { id, entered_at }
		)
		.await,
		DecisionResult::HoldReleased
	);
	cluster
		.until("the hold is gone", |state| {
			state.decisions.as_ref().unwrap().backup_hold().is_none()
		})
		.await;
}

/// DC-242 mapping control (SYNTHETIC local state, labelled as such): a coordinator whose own
/// applied catalog does not hold a table the cluster has committed answers a table write with
/// the generic pair INTERNAL_ERROR / INDETERMINATE, log index 0, hinting itself, and records
/// nothing. The absence is made by replacing that one follower's catalog through the test seam,
/// so it is stable through the call; it stands for a follower that has not applied the schema
/// update yet, and shows only how such an absence is answered, not that it occurred.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_coordinator_without_the_committed_descriptor_answers_internal_error_indeterminate() {
	let (cluster, leader, descriptor) = ready().await;
	let follower = *NODES.iter().find(|id| **id != leader).unwrap();
	let (table, generation) = (descriptor.table_object_id, descriptor.schema_generation);
	let at = &cluster;
	let holds = |id: u32| async move {
		at.node(id)
			.state_machine
			.read_state(move |state| {
				state
					.table_schemas
					.get(TenantId::ZERO, table, generation)
					.is_some()
			})
			.await
	};
	assert!(holds(leader).await && holds(follower).await);
	cluster
		.node(follower)
		.state_machine
		.seed_catalogs_for_test(Default::default(), Default::default())
		.await;
	assert!(holds(leader).await, "committed and applied at the leader");
	assert!(
		!holds(follower).await,
		"absent at the follower before the call"
	);

	let meta = write_meta(&descriptor, 1, uuid_v7());
	let (response, _) = cluster
		.node(follower)
		.data_server
		.table_write_for_test(meta, rows(&descriptor, &[1]))
		.await
		.unwrap();
	let write = response.write;
	assert_eq!(
		(
			write.status,
			write.mutation_commit_knowledge(),
			write.log_index,
			write.leader_hint.id
		),
		(
			WriteStatus::INTERNAL_ERROR,
			MutationCommitKnowledge::INDETERMINATE,
			0,
			follower
		),
		"{response:?}"
	);
	assert!(!holds(follower).await, "still absent after the call");
	assert!(
		record_of(&cluster, key_of(&descriptor, &meta))
			.await
			.is_none(),
		"nothing was recorded for the write"
	);
}
