//! Wendelon ADR 0114 stage (a), the replicated control: the real
//! `TableDropPrepare`, proposed on the leader of an in-process three-voter
//! cluster whose every replica is admitted for the test
//! (`admit_pd_drop_prepare_for_test`; off by default, no public route), fences
//! the table and concludes a Reserved parent `Refused { TableDropped }` at the
//! Prepare's own committed index, identically on a follower. The cluster is not
//! restarted and installs no snapshot after admission (a machine rebuilt from
//! its log starts unadmitted).

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_replicated_drop_prepare_concludes_a_reserved_parent_on_every_replica() {
	use crate::consensus::decisions::{ParentStatus, RefusalCause};
	use crate::network::data_server::child_writes::PauseStage;
	let (cluster, leader, descriptor) = ready().await;
	for node in NODES {
		cluster
			.node(node)
			.state_machine
			.admit_pd_drop_prepare_for_test()
			.await;
	}
	// A parent kept Reserved: its coordinator pauses before Finalize.
	let batch = named_rows(&descriptor, &[(72, "pending")]);
	let coordinator = cluster
		.node(leader)
		.data_server
		.primary_for_test(TenantId::ZERO, &descriptor, &batch)
		.await
		.unwrap();
	let (finalize_reached, finalize_release) = cluster
		.node(coordinator)
		.data_server
		.pause_coordinator_for_test(PauseStage::BeforeFinalize);
	let meta = write_meta(&descriptor, 1, uuid_v7());
	let server = cluster.node(coordinator).data_server.clone();
	let writing = tokio::spawn(async move { server.table_write_for_test(meta, batch).await });
	tokio::time::timeout(BOUND, finalize_reached.notified())
		.await
		.expect("the coordinator reached Finalize");
	let record = record_of(&cluster, key_of(&descriptor, &meta))
		.await
		.expect("the parent is reserved");
	let rsv = record.rsv;

	// The real Prepare, proposed on the leader.
	let drop_id = crate::integration::compute::DropOperationId {
		tenant_id: TenantId::ZERO,
		table_object_id: descriptor.table_object_id,
		schema_generation: crate::integration::compute::SchemaGeneration::new(
			descriptor.schema_generation,
		),
	};
	let written = cluster
		.node(leader)
		.raft
		.client_write(crate::consensus::WendelonRequest::TableDropPrepare(
			crate::consensus::TableDropPrepareRequest::current(drop_id, drop_id.schema_generation),
		))
		.await
		.unwrap();
	assert!(
		matches!(written.data, crate::consensus::WendelonResponse::Success),
		"{:?}",
		written.data
	);
	let at = written.log_id.index;
	assert!(at > rsv, "premise: the Prepare follows the reservation");

	// On a follower: applied through the Prepare's index, the same fence and the
	// same terminal decision at that index.
	let follower = *NODES.iter().find(|id| **id != leader).unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let applied = cluster
			.node(follower)
			.state_machine
			.read_state(|state| state.last_applied_log_id.map(|id| id.index))
			.await;
		if applied.is_some_and(|index| index >= at) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the follower never applied the Prepare"
		);
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	}
	let expected = Some(ParentStatus::Refused {
		cause: RefusalCause::TableDropped,
		dec: at,
	});
	for node in [leader, follower] {
		let (status, fenced) = cluster
			.node(node)
			.state_machine
			.read_state(move |state| {
				(
					state
						.decisions
						.as_ref()
						.and_then(|decisions| decisions.body(rsv))
						.map(|body| body.status),
					state.table_drop_blocks_admission(TenantId::ZERO, drop_id.table_object_id),
				)
			})
			.await;
		assert!(fenced, "node {node}: the table is fenced");
		assert_eq!(
			status, expected,
			"node {node}: the same terminal decision at {at}"
		);
	}
	eprintln!(
		"ADR 0114 replicated: Prepare committed at {at}; leader {leader} and follower {follower} hold the fence and {expected:?} for rsv {rsv}"
	);
	finalize_release.notify_one();
	let finished = tokio::time::timeout(BOUND, writing)
		.await
		.expect("the write ends")
		.unwrap();
	eprintln!(
		"ADR 0114 replicated: the paused write ends {:?}",
		finished.as_ref().map(|(meta, _)| meta.write.status)
	);
}
